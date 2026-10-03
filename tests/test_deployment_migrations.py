from __future__ import annotations

import runpy
import sys
from pathlib import Path

import metatables
import pytest
import yaml

from valmer_connectors.cli.main import main as cli_main
from valmer_connectors.services.migrations import (
    MIGRATION_JOB_EXECUTION_PATH,
    MIGRATION_JOB_PROVIDERS,
    MIGRATION_WORKFLOW_PATH,
    VALMER_MIGRATION_PROVIDER,
    migration_command_lines,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
JOBS_WORKFLOW_PATH = ".mainsequence/workflows/valmer-control-plane-jobs.yaml"

# Spec fields that workflow API 2.3.0 accepts for these kinds. The backend
# rejects unknown fields, including the retired resource_uid and release_kind.
# Copied by hand from CodeRepositoryWorkflowService.FASTAPI_FIELDS and
# .JOB_FIELDS, plus env_vars (WORKFLOW_ENV_VARS_API_VERSIONS), in tdag-django
# timeseries_orm/tdag/pod_manager/services/code_repository_workflows.py at
# a618e4140 (2026-10-03, WORKFLOW_API_VERSION "2.3.0"). They do not follow
# backend changes: refresh them with every workflow API bump, and validate the
# file through the branch validate-workflow endpoint before pushing.
FASTAPI_FIELDS = {
    "min_scale",
    "source_path",
    "spot",
    "cpu_request",
    "memory_request",
    "gpu_request",
    "gpu_type",
    "automatic_deployment",
    "automatic_redeployment",
    "revision_retention_count",
    "cors_allowed_origins",
    "public_ingress",
    "env_vars",
}
JOB_FIELDS = {
    "name",
    "description",
    "execution_path",
    "scheduled_command_args",
    "task_schedule",
    "cpu_request",
    "memory_request",
    "gpu_request",
    "gpu_type",
    "spot",
    "max_runtime_seconds",
    "automatic_redeployment",
    "env_vars",
}


def _load(path: str) -> dict:
    return yaml.safe_load((REPOSITORY_ROOT / path).read_text(encoding="utf-8"))


def _run_migration_job() -> None:
    script = str(REPOSITORY_ROOT / MIGRATION_JOB_EXECUTION_PATH)
    previous_argv = sys.argv
    sys.argv = [script]
    try:
        runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = previous_argv


def test_api_workflow_declares_the_release_as_an_api_2_3_0_fastapi_resource() -> None:
    workflow = _load(MIGRATION_WORKFLOW_PATH)

    assert workflow["api_version"] == "2.3.0"
    assert workflow["name"] == "valmer-control-plane-api"
    assert {resource["key"]: resource["kind"] for resource in workflow["resources"]} == {
        "valmer-control-plane-api": "fastapi",
        "migrate-valmer": "job",
    }
    api = workflow["resources"][0]["spec"]
    assert set(api) <= FASTAPI_FIELDS
    assert api["source_path"] == "api/valmer_control_plane/main.py"
    assert (REPOSITORY_ROOT / api["source_path"]).is_file()
    assert api["automatic_deployment"] is True
    assert api["revision_retention_count"] == 3


def test_migration_job_shares_the_api_promotion_policy() -> None:
    specs = {
        resource["key"]: resource["spec"]
        for resource in _load(MIGRATION_WORKFLOW_PATH)["resources"]
    }
    job = specs["migrate-valmer"]

    assert set(job) <= JOB_FIELDS
    assert job["execution_path"] == MIGRATION_JOB_EXECUTION_PATH
    assert (REPOSITORY_ROOT / job["execution_path"]).is_file()
    assert job["automatic_redeployment"] == {"enabled": True, "tag_regex": None}
    assert (
        job["automatic_redeployment"] == specs["valmer-control-plane-api"]["automatic_redeployment"]
    )
    assert "task_schedule" not in job
    for field_name in ("name", "description", "cpu_request", "memory_request"):
        assert str(job[field_name]).strip()
    assert job["max_runtime_seconds"] > 0


def test_api_deploys_only_after_the_migration_job_runs_from_its_image() -> None:
    assert _load(MIGRATION_WORKFLOW_PATH)["execution"] == {
        "steps": {
            "image": {"prepare_image": "valmer-control-plane-api"},
            "migrate": {
                "run_job": "migrate-valmer",
                "image_from": "image",
                "needs": ["image"],
            },
            "deploy_api": {
                "deploy": "valmer-control-plane-api",
                "image_from": "image",
                "needs": ["migrate"],
            },
        }
    }


def test_scheduled_jobs_stay_outside_the_deployment_graph() -> None:
    jobs_workflow = _load(JOBS_WORKFLOW_PATH)

    assert "execution" not in jobs_workflow
    assert MIGRATION_JOB_EXECUTION_PATH not in {
        resource["spec"]["execution_path"] for resource in jobs_workflow["resources"]
    }


def test_migration_job_applies_only_the_valmer_provider(monkeypatch, capsys) -> None:
    calls: list[str] = []

    def upgrade_application(provider: str) -> dict[str, object]:
        calls.append(provider)
        return {"revision": "0001", "migrated": False, "data_source_uid": "source-1"}

    monkeypatch.setattr(metatables, "upgrade_application", upgrade_application)

    with pytest.raises(SystemExit) as exit_info:
        _run_migration_job()

    assert exit_info.value.code == 0
    assert MIGRATION_JOB_PROVIDERS == (VALMER_MIGRATION_PROVIDER,)
    assert calls == ["valmer_connectors.migrations:migration"]
    assert capsys.readouterr().out == (
        "valmer_connectors.migrations:migration: 0001 (already current) on source-1\n"
    )


def test_migration_job_failure_fails_the_job(monkeypatch) -> None:
    def upgrade_application(provider: str) -> dict[str, object]:
        raise RuntimeError(f"{provider} could not be finalized")

    monkeypatch.setattr(metatables, "upgrade_application", upgrade_application)

    with pytest.raises(RuntimeError, match="could not be finalized"):
        _run_migration_job()


def test_migrations_commands_print_only_local_valmer_commands(capsys) -> None:
    assert cli_main(["migrations", "commands"]) == 0

    lines = capsys.readouterr().out.splitlines()
    assert lines == migration_command_lines()
    assert not any("msm" in line for line in lines)
    commands = [line for line in lines if line.startswith("metatables ")]
    assert [line for line in commands if " upgrade " in line or " downgrade " in line] == [
        "metatables --local migrations upgrade "
        "--provider valmer_connectors.migrations:migration head"
    ]
    assert any("migration Job" in line and MIGRATION_JOB_EXECUTION_PATH in line for line in lines)
