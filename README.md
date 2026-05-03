<p align="center">
  <img src="https://raw.githubusercontent.com/aurekai/aurekai/main/assets/aurekai-logo.svg" alt="Aurekai" width="520" />
</p>

# `aurekai-dagster` · v0.8.0-alpha.5

Official Dagster integration for Aurekai — software-defined assets, `AurékaiResource`, schedules, and sensors for all core capability operators.

## Assets

| Asset | Group | Upstream deps | Description |
|---|---|---|---|
| `runtime_health` | `aurekai_runtime` | — | Doctor deep — all binaries pass/fail |
| `manifest_integrity` | `aurekai_runtime` | `runtime_health` | Validate `artifact.json` |
| `model_memory_pack` | `aurekai_memory` | `manifest_integrity` | FPQ pack + proof bundle |
| `sae_audit` | `aurekai_memory` | `model_memory_pack` | SAE feature audit |
| `semantic_cache_bench` | `aurekai_memory` | — | Cache hit rate / latency |
| `proof_bundle` | `aurekai_proof` | `model_memory_pack`, `sae_audit` | Export `.akproof` |
| `release_gate` | `aurekai_release` | `proof_bundle`, `manifest_integrity` | Full release gate |

## Quick Start

```bash
pip install dagster dagster-webserver
pip install -r requirements.txt
dagster dev -f jobs/definitions.py
```

## `AurékaiResource`

Configurable resource that wraps the `akai` CLI:

```python
from aurekai_resources import AurékaiResource

result = AurékaiResource(version="0.8.0-alpha.5").doctor(deep=True)
print(result.ok, result.proof_uri)
```

## Jobs

```python
from aurekai_assets import aurekai_full_pipeline_job
# Materialization order: runtime_health → manifest_integrity → model_memory_pack
#                        → sae_audit + semantic_cache_bench → proof_bundle → release_gate
```

## Schedules & Sensors

| Name | Type | Description |
|---|---|---|
| `aurekai_daily_pipeline` | Schedule | `0 2 * * *` — daily at 02:00 UTC |
| `manifest_change_sensor` | Sensor | Triggers on `artifact.json` mtime change |
| `release_tag_sensor` | Sensor | Triggers when `.aurekai/release-tag` changes |

## Layout

```
jobs/
  definitions.py        Dagster Definitions (assets, jobs, schedules, sensors, resources)
  aurekai_assets.py     Software-defined asset graph
  aurekai_resources.py  AurékaiResource (ConfigurableResource)
  aurekai_pipeline.py   Legacy ops/job (kept for reference)
  aurekai_sensors.py    Schedule + event-driven sensors
```

