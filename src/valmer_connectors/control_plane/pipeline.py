from __future__ import annotations

import logging
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger("valmer_control_plane_pipeline")

PIPELINE_SCRIPTS: tuple[str, ...] = (
    "update_vector_valmer.py",
    "update_valmer_irs_mxn_quotes.py",
    "update_valmer_irs_usd_quotes.py",
    "update_banxico_fixings.py",
    "update_fred_reference_rates.py",
    "update_banxico_policy_rates.py",
    "update_valmer_tiie_curve.py",
    "update_valmer_usd_sofr_curve.py",
    "update_valmer_mxn_government_curve.py",
    "update_valmer_usd_mxn_xccy_curve.py",
)

Runner = Callable[..., subprocess.CompletedProcess[str]]


def run_pipeline(
    *,
    scripts_directory: Path,
    runner: Runner = subprocess.run,
) -> None:
    for position, script_name in enumerate(PIPELINE_SCRIPTS, start=1):
        script_path = scripts_directory / script_name
        logger.info(
            "control_plane_pipeline_stage_started",
            extra={
                "pipeline_stage": position,
                "pipeline_stage_count": len(PIPELINE_SCRIPTS),
                "pipeline_script": script_name,
            },
        )
        runner(
            [sys.executable, str(script_path)],
            check=True,
            cwd=scripts_directory.parent,
            text=True,
        )
        logger.info(
            "control_plane_pipeline_stage_completed",
            extra={
                "pipeline_stage": position,
                "pipeline_stage_count": len(PIPELINE_SCRIPTS),
                "pipeline_script": script_name,
            },
        )


__all__ = ["PIPELINE_SCRIPTS", "run_pipeline"]
