from __future__ import annotations

import sys
from pathlib import Path

import yaml

from valmer_connectors.control_plane.catalog import JOB_ACTIONS
from valmer_connectors.control_plane.pipeline import PIPELINE_SCRIPTS, run_pipeline


def test_control_plane_pipeline_runs_every_stage_in_dependency_order() -> None:
    calls: list[tuple[list[str], bool, Path, bool]] = []

    def runner(command: list[str], *, check: bool, cwd: Path, text: bool) -> None:
        calls.append((command, check, cwd, text))

    scripts_directory = Path(__file__).resolve().parents[1] / "scripts"
    run_pipeline(scripts_directory=scripts_directory, runner=runner)

    assert [Path(command[1]).name for command, *_rest in calls] == list(PIPELINE_SCRIPTS)
    assert all(command[0] == sys.executable for command, *_rest in calls)
    assert all(check and text for _command, check, _cwd, text in calls)
    assert all(cwd == Path(__file__).resolve().parents[1] for _command, _check, cwd, _text in calls)


def test_control_plane_workflow_declares_the_exact_approved_job_catalog() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    workflow_path = (
        repository_root / ".mainsequence" / "workflows" / "valmer-control-plane-jobs.yaml"
    )
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    jobs = [resource for resource in workflow["resources"] if resource["kind"] == "job"]
    jobs_by_key = {resource["key"]: resource["spec"] for resource in jobs}

    assert workflow["api_version"] == "2.1.0"
    assert set(jobs_by_key) == {action.key for action in JOB_ACTIONS}
    for action in JOB_ACTIONS:
        assert jobs_by_key[action.key]["name"] == action.job_name
        assert jobs_by_key[action.key]["execution_path"] == action.execution_path
        assert jobs_by_key[action.key]["automatic_redeployment"] == {
            "enabled": True,
            "tag_regex": None,
        }
    scheduled = [key for key, spec in jobs_by_key.items() if "task_schedule" in spec]
    assert scheduled == ["standard-pipeline-refresh"]
