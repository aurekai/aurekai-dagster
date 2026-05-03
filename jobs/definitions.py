"""Aurekai Dagster definitions — registers all assets, jobs, schedules, and sensors."""
from dagster import Definitions, EnvVar

from aurekai_resources import AurékaiResource
from aurekai_assets import (
    runtime_health,
    manifest_integrity,
    model_memory_pack,
    sae_audit,
    semantic_cache_bench,
    proof_bundle,
    release_gate,
    aurekai_full_pipeline_job,
)
from aurekai_sensors import (
    aurekai_daily_schedule,
    manifest_change_sensor,
    release_tag_sensor,
)

defs = Definitions(
    assets=[
        runtime_health,
        manifest_integrity,
        model_memory_pack,
        sae_audit,
        semantic_cache_bench,
        proof_bundle,
        release_gate,
    ],
    jobs=[aurekai_full_pipeline_job],
    schedules=[aurekai_daily_schedule],
    sensors=[manifest_change_sensor, release_tag_sensor],
    resources={
        "aurekai": AurékaiResource(
            version=EnvVar.str("AUREKAI_VERSION") if False else "0.8.0-alpha.5",
            binary="akai",
        )
    },
)
