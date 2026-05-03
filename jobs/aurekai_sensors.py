"""Aurekai Dagster sensors — schedule-driven and event-driven pipeline triggers."""
from __future__ import annotations

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    SensorDefinition,
    SensorEvaluationContext,
    sensor,
)

from aurekai_assets import aurekai_full_pipeline_job


# Daily schedule — run the full pipeline at 02:00 UTC
aurekai_daily_schedule = ScheduleDefinition(
    job=aurekai_full_pipeline_job,
    cron_schedule="0 2 * * *",
    name="aurekai_daily_pipeline",
    description="Run the full Aurekai capability pipeline every day at 02:00 UTC.",
    default_status=DefaultSensorStatus.STOPPED,
)


@sensor(
    job=aurekai_full_pipeline_job,
    name="aurekai_manifest_change_sensor",
    description="Triggers the pipeline when artifact.json changes (mtime-based).",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
)
def manifest_change_sensor(context: SensorEvaluationContext):
    import os

    manifest_path = "artifact.json"
    cursor = context.cursor

    try:
        mtime = str(os.path.getmtime(manifest_path))
    except FileNotFoundError:
        return

    if cursor != mtime:
        context.update_cursor(mtime)
        yield RunRequest(
            run_key=mtime,
            run_config={},
            tags={"trigger": "manifest_change", "manifest_mtime": mtime},
        )


@sensor(
    job=aurekai_full_pipeline_job,
    name="aurekai_release_tag_sensor",
    description="Triggers the pipeline when a new release tag file appears in .aurekai/release-tag.",
    minimum_interval_seconds=120,
    default_status=DefaultSensorStatus.STOPPED,
)
def release_tag_sensor(context: SensorEvaluationContext):
    import os

    tag_path = ".aurekai/release-tag"
    cursor = context.cursor

    try:
        with open(tag_path) as f:
            tag = f.read().strip()
    except FileNotFoundError:
        return

    if tag and tag != cursor:
        context.update_cursor(tag)
        yield RunRequest(
            run_key=tag,
            run_config={},
            tags={"trigger": "release_tag", "release_tag": tag},
        )
