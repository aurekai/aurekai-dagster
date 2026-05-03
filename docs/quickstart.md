# Quickstart — aurekai-dagster

Aurekai pipeline as a Dagster job.

## Requirements

- Python 3.10+
- `pip install -r requirements.txt`

## Run

```bash
dagster job execute -f jobs/aurekai_pipeline.py -j aurekai_core_pipeline
```

## Validate

```bash
bash tests/validate-schemas.sh
bash tests/validate-scripts.sh
```
