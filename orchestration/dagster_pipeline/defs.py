from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    job,
    op,
    run_status_sensor,
    sensor,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def _build_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update(_load_env_file(REPO_ROOT / ".env"))
    return env


def _run_cmd(cmd: list[str], context) -> None:
    context.log.info("Running command: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=_build_env(),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.stdout:
        context.log.info(proc.stdout)
    if proc.stderr:
        context.log.warning(proc.stderr)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")


@op
def validate_env_op(context) -> str:
    _run_cmd(["./scripts/validate_env.sh"], context)
    return "env_ok"


@op
def ingest_raw_op(context, _upstream: str) -> str:
    _run_cmd(["make", "ingest-raw"], context)
    return "raw_loaded"


@op
def dbt_build_dev_op(context, _upstream: str) -> str:
    _run_cmd(["./scripts/dbt.sh", "build", "--target", "dev"], context)
    return "dbt_built_dev"


@op
def dbt_build_prod_op(context, _upstream: str) -> str:
    _run_cmd(["./scripts/dbt.sh", "build", "--target", "prod"], context)
    return "dbt_built_prod"


@op
def export_task1_op(context, _upstream: str) -> str:
    _run_cmd(["./scripts/export_task1.sh"], context)
    return "task1_exported"


@op
def export_task2_op(context, _upstream: str) -> str:
    _run_cmd(["./scripts/export_task2.sh"], context)
    return "task2_exported"


@job
def wolt_daily_dev_pipeline_job():
    env_ok = validate_env_op()
    raw_loaded = ingest_raw_op(env_ok)
    dbt_built_dev = dbt_build_dev_op(raw_loaded)
    task1_exported = export_task1_op(dbt_built_dev)
    export_task2_op(task1_exported)


@job
def wolt_daily_prod_pipeline_job():
    env_ok = validate_env_op()
    dbt_build_prod_op(env_ok)


# Backward-compatible alias for existing commands/scripts that still reference old job name.
@job
def wolt_daily_pipeline_job():
    env_ok = validate_env_op()
    raw_loaded = ingest_raw_op(env_ok)
    dbt_built_dev = dbt_build_dev_op(raw_loaded)
    task1_exported = export_task1_op(dbt_built_dev)
    export_task2_op(task1_exported)


@op
def dbt_full_refresh_dev_op(context, _upstream: str) -> str:
    _run_cmd(["./scripts/dbt.sh", "build", "--target", "dev", "--full-refresh"], context)
    return "dbt_full_refreshed"


@job
def wolt_weekly_full_refresh_job():
    env_ok = validate_env_op()
    raw_loaded = ingest_raw_op(env_ok)
    full_refreshed = dbt_full_refresh_dev_op(raw_loaded)
    task1_exported = export_task1_op(full_refreshed)
    export_task2_op(task1_exported)


daily_schedule = ScheduleDefinition(
    job=wolt_daily_dev_pipeline_job,
    cron_schedule="0 6 * * *",  # 06:00 Europe/Berlin daily
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.RUNNING,
)


weekly_full_refresh_schedule = ScheduleDefinition(
    job=wolt_weekly_full_refresh_job,
    cron_schedule="0 7 * * 0",  # Sunday 07:00 Europe/Berlin
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.STOPPED,
)


@sensor(job=wolt_daily_pipeline_job, default_status=DefaultSensorStatus.STOPPED)
def daily_dedup_sensor(context):
    # Optional safety sensor to prevent duplicate runs for same calendar day if enabled.
    today_utc = datetime.now(timezone.utc).date().isoformat()
    if context.cursor == today_utc:
        yield SkipReason("Daily run already requested for today.")
        return
    context.update_cursor(today_utc)
    yield RunRequest(run_key=f"wolt_daily_pipeline:{today_utc}")


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[wolt_daily_dev_pipeline_job],
    request_job=wolt_daily_prod_pipeline_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def trigger_prod_after_dev_success(context):
    # Trigger prod only after each successful dev run.
    yield RunRequest(
        run_key=f"prod_after_dev_success:{context.dagster_run.run_id}",
        tags={
            "triggered_by": "dev_success",
            "triggered_from_run_id": context.dagster_run.run_id,
        },
    )


defs = Definitions(
    jobs=[
        wolt_daily_dev_pipeline_job,
        wolt_daily_prod_pipeline_job,
        wolt_daily_pipeline_job,
        wolt_weekly_full_refresh_job,
    ],
    schedules=[daily_schedule, weekly_full_refresh_schedule],
    sensors=[daily_dedup_sensor, trigger_prod_after_dev_success],
)
