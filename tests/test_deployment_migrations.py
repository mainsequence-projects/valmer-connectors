from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path

import metatables
import pytest
import yaml

from valmer_connectors.cli.main import main as cli_main
from valmer_connectors.control_plane.catalog import JOB_LAUNCH_PROFILES
from valmer_connectors.services.migrations import (
    MIGRATION_JOB_EXECUTION_PATH,
    MIGRATION_JOB_PROVIDERS,
    MIGRATION_WORKFLOW_PATH,
    VALMER_MIGRATION_PROVIDER,
    migration_command_lines,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIRECTORY = REPOSITORY_ROOT / ".mainsequence" / "workflows"
API_KEY = "valmer-control-plane-api"
DATA_API_KEY = "valmer-data-api"
MIGRATION_JOB_KEY = "migrate-valmer"
# The MetaTables repository's exact platform name; every workload discovers its
# API with reader on its branch (workflow API 3.0.0 names a policy).
METATABLES_BRANCH_READER = [{"repository": "MetaTables", "policy": "reader"}]
# Workloads that reach Markets registries through the Markets API (ms-markets
# ADR 0047) also discover it on the MainSequenceMarkets branch.
MARKETS_BRANCH_READER = [
    *METATABLES_BRANCH_READER,
    {"repository": "MainSequenceMarkets", "policy": "reader"},
]
# The migration Job applies only Valmer's provider, the verification Job only
# reads tables and the time-series backfill only copies table rows; none calls
# the Markets API.
WITHOUT_MARKETS_API = {"migrate-valmer", "pipeline-verification", "time-series-backfill"}
# Resource-level fields this file uses. The workflow API also accepts
# acts_for_requester, which no workload here needs.
RESOURCE_FIELDS = {"key", "kind", "spec", "access"}

# Spec fields that workflow API 2.3.0 accepted for these kinds. The backend
# rejects unknown fields, including the retired resource_uid and release_kind
# and the 2.1.0-only Job fields uid, cluster_uid and namespace. Copied by hand
# from CodeRepositoryWorkflowService.FASTAPI_FIELDS and .JOB_FIELDS, plus
# env_vars (WORKFLOW_ENV_VARS_API_VERSIONS), in tdag-django
# timeseries_orm/tdag/pod_manager/services/code_repository_workflows.py at
# a618e4140, checked again at 122a395fe (2026-10-03) and refreshed at
# f16c42249 (2026-10-11, WORKFLOW_API_VERSION "2.3.0"). For workflow API 3.0.0
# they were not refreshed from the backend source, only compared with the
# branch's 3.0.0 workflow template (2026-10-27), whose fastapi and job fields
# are all here. They do not follow backend changes: refresh them with every
# workflow API bump, and validate the file through the branch validate-workflow
# endpoint before pushing.
FASTAPI_FIELDS = {
    "mcp_enabled",
    "min_scale",
    "max_scale",
    "container_concurrency",
    "concurrency_target",
    "timeout_seconds",
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


def test_the_api_workflow_is_the_repository_s_only_workflow_file() -> None:
    assert sorted(path.name for path in WORKFLOW_DIRECTORY.iterdir()) == [
        "valmer-control-plane-api.yaml"
    ]
    assert REPOSITORY_ROOT / MIGRATION_WORKFLOW_PATH == (
        WORKFLOW_DIRECTORY / "valmer-control-plane-api.yaml"
    )


def test_api_workflow_declares_the_release_as_an_api_3_0_0_fastapi_resource() -> None:
    workflow = _load(MIGRATION_WORKFLOW_PATH)

    assert workflow["api_version"] == "3.0.0"
    assert workflow["name"] == "valmer-control-plane-api"
    assert {resource["key"]: resource["kind"] for resource in workflow["resources"]} == {
        "valmer-control-plane-api": "fastapi",
        "valmer-data-api": "fastapi",
        "migrate-valmer": "job",
        **{profile.key: "job" for profile in JOB_LAUNCH_PROFILES},
    }
    specs = {resource["key"]: resource["spec"] for resource in workflow["resources"]}
    for key, source_path in (
        (API_KEY, "api/valmer_control_plane/main.py"),
        (DATA_API_KEY, "api/valmer_data_api/main.py"),
    ):
        api = specs[key]
        assert set(api) <= FASTAPI_FIELDS, key
        assert api["source_path"] == source_path
        assert (REPOSITORY_ROOT / api["source_path"]).is_file()
        assert api["automatic_deployment"] is True
        assert api["automatic_redeployment"] == {"enabled": True, "tag_regex": None}
        assert api["revision_retention_count"] == 3
    # The data API serves other applications, not the operator console.
    assert "env_vars" not in specs[DATA_API_KEY]
    assert "cors_allowed_origins" not in specs[DATA_API_KEY]


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


def _steps() -> dict[str, dict]:
    return _load(MIGRATION_WORKFLOW_PATH)["execution"]["steps"]


def test_api_deploys_only_after_the_migration_job_runs_from_its_image() -> None:
    assert _steps() == {
        "image": {"prepare_image": API_KEY},
        "migrate": {
            "run_job": MIGRATION_JOB_KEY,
            "image_from": "image",
            "needs": ["image"],
        },
        "deploy_api": {
            "deploy": API_KEY,
            "image_from": "image",
            "needs": ["migrate"],
        },
        "deploy_data_api": {
            "deploy": DATA_API_KEY,
            "image_from": "image",
            "needs": ["migrate"],
        },
    }


def _jobs() -> dict[str, dict]:
    return {
        resource["key"]: resource["spec"]
        for resource in _load(MIGRATION_WORKFLOW_PATH)["resources"]
        if resource["kind"] == "job"
    }


def test_every_job_declares_only_known_job_fields() -> None:
    jobs = _jobs()

    assert len(jobs) == len(JOB_LAUNCH_PROFILES) + 1
    for key, spec in jobs.items():
        assert set(spec) <= JOB_FIELDS, key


def test_operational_jobs_stay_outside_the_deployment_graph() -> None:
    referenced = {
        step[operation]
        for step in _steps().values()
        for operation in ("prepare_image", "run_job", "deploy")
        if operation in step
    }
    jobs = _jobs()
    scheduled = {key for key, spec in jobs.items() if "task_schedule" in spec}
    launched = {profile.key for profile in JOB_LAUNCH_PROFILES}

    # Only the two APIs and their migration Job are in the graph. The backend
    # activates a Job's new image, schedule included, as soon as the commit
    # image is verified, whatever a step's needs say, and a Job in the graph
    # would tie its promotion policy to the API's group.
    assert referenced == {API_KEY, DATA_API_KEY, MIGRATION_JOB_KEY}
    assert scheduled == {"standard-pipeline-refresh"}
    assert scheduled | {"pipeline-verification"} <= launched
    assert not launched & referenced
    assert [
        key for key, spec in jobs.items() if spec["execution_path"] == MIGRATION_JOB_EXECUTION_PATH
    ] == [MIGRATION_JOB_KEY]


def _resources() -> dict[str, dict]:
    return {resource["key"]: resource for resource in _load(MIGRATION_WORKFLOW_PATH)["resources"]}


def test_every_workload_declares_reader_on_the_metatables_branch() -> None:
    for key, resource in _resources().items():
        assert set(resource) <= RESOURCE_FIELDS, key
        expected_branches = (
            METATABLES_BRANCH_READER if key in WITHOUT_MARKETS_API else MARKETS_BRANCH_READER
        )
        assert resource["access"]["branches"] == expected_branches, key
        expected = {"branches", "resources"} if key == API_KEY else {"branches"}
        assert set(resource["access"]) == expected, key


def test_the_api_holds_runner_on_exactly_the_jobs_it_launches() -> None:
    granted = _resources()[API_KEY]["access"]["resources"]

    # The control plane lists and runs a branch Job only when its workload is
    # granted that Job. It lists the Job, runs it and reads its runs, never
    # changes it, which runner covers. The migration Job is not launched.
    assert granted == [{"key": profile.key, "policy": "runner"} for profile in JOB_LAUNCH_PROFILES]
    assert MIGRATION_JOB_KEY not in {entry["key"] for entry in granted}
    assert {entry["key"] for entry in granted} <= set(_jobs())


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
    # Revision autogenerate compares the models with the selected runtime;
    # without --local that would be the Environment's hosted database.
    assert [line for line in commands if " revision " in line] == [
        "metatables --local migrations revision "
        '--provider valmer_connectors.migrations:migration --message "<change>"'
    ]
    assert all(line.startswith("metatables --local ") for line in commands)
    assert any("migration Job" in line and MIGRATION_JOB_EXECUTION_PATH in line for line in lines)


def test_every_metatables_launcher_targets_the_local_runtime() -> None:
    launch = json.loads((REPOSITORY_ROOT / ".vscode" / "launch.json").read_text(encoding="utf-8"))
    launchers = {
        configuration["name"]: configuration["args"]
        for configuration in launch["configurations"]
        if configuration.get("program", "").endswith("/metatables")
    }

    assert sorted(launchers) == [
        "Migrations: Current (Valmer, local runtime)",
        "Migrations: Revision (Valmer, local runtime)",
        "Migrations: Upgrade (Valmer, local runtime)",
    ]
    for name, args in launchers.items():
        assert args[:2] == ["--local", "migrations"], name
        assert args[args.index("--provider") + 1] == VALMER_MIGRATION_PROVIDER, name
