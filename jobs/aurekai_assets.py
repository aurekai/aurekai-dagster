"""Aurekai Dagster assets — capability families as software-defined assets."""
from __future__ import annotations

import json
import uuid
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetOut,
    Output,
    asset,
    multi_asset,
)

from aurekai_resources import AurékaiResource


@asset(
    group_name="aurekai_runtime",
    description="Aurekai runtime deep diagnostics. Confirms all binaries are healthy.",
    required_resource_keys={"aurekai"},
)
def runtime_health(context: AssetExecutionContext) -> dict[str, Any]:
    result = context.resources.aurekai.doctor(deep=True)
    if not result.ok:
        raise Exception(f"doctor --deep failed: {result.stderr}")
    context.add_output_metadata({"proof_uri": result.proof_uri, "exit_code": result.exit_code})
    return result.json_output


@asset(
    group_name="aurekai_runtime",
    deps=[runtime_health],
    description="Verify artifact.json manifest — schema, command bindings, proof references.",
    required_resource_keys={"aurekai"},
)
def manifest_integrity(context: AssetExecutionContext) -> dict[str, Any]:
    result = context.resources.aurekai.verify_manifest("artifact.json")
    if not result.ok:
        raise Exception(f"manifest verify failed: {result.stderr}")
    context.add_output_metadata({"proof_uri": result.proof_uri})
    return result.json_output


@asset(
    group_name="aurekai_memory",
    deps=[manifest_integrity],
    description="Pack model memory — FPQ compress, FNV-key index, proof bundle.",
    required_resource_keys={"aurekai"},
)
def model_memory_pack(context: AssetExecutionContext) -> dict[str, Any]:
    result = context.resources.aurekai.model_memory_pack(tag="latest")
    if not result.ok:
        raise Exception(f"model-memory-pack failed: {result.stderr}")
    context.add_output_metadata({"proof_uri": result.proof_uri})
    return result.json_output


@asset(
    group_name="aurekai_memory",
    deps=[model_memory_pack],
    description="SAE (Sparse Autoencoder) feature audit — active feature ids and score distribution.",
    required_resource_keys={"aurekai"},
)
def sae_audit(context: AssetExecutionContext) -> dict[str, Any]:
    result = context.resources.aurekai.sae_audit()
    if not result.ok:
        raise Exception(f"sae-audit failed: {result.stderr}")
    context.add_output_metadata({"proof_uri": result.proof_uri})
    return result.json_output


@asset(
    group_name="aurekai_memory",
    description="Semantic cache benchmark — hit rate, latency P50/P99, namespace coverage.",
    required_resource_keys={"aurekai"},
)
def semantic_cache_bench(context: AssetExecutionContext) -> dict[str, Any]:
    result = context.resources.aurekai.semantic_cache_bench(namespace="default")
    if not result.ok:
        context.log.warning(f"semantic-cache-bench non-zero: {result.stderr}")
    context.add_output_metadata({"proof_uri": result.proof_uri})
    return result.json_output or {"status": "bench_complete"}


@asset(
    group_name="aurekai_proof",
    deps=[model_memory_pack, sae_audit],
    description="Export a canonical proof bundle (.akproof) for the current pipeline run.",
    required_resource_keys={"aurekai"},
)
def proof_bundle(context: AssetExecutionContext) -> dict[str, Any]:
    run_id = str(context.run_id or uuid.uuid4())
    result = context.resources.aurekai.proof_bundle_export(run_id=run_id)
    if not result.ok:
        raise Exception(f"proof-bundle-export failed: {result.stderr}")
    context.add_output_metadata({
        "run_id": run_id,
        "proof_uri": result.proof_uri,
    })
    return result.json_output


@asset(
    group_name="aurekai_release",
    deps=[proof_bundle, manifest_integrity],
    description="Release gate — validates manifest, proof, SLI checks, and registry presence.",
    required_resource_keys={"aurekai"},
)
def release_gate(context: AssetExecutionContext) -> dict[str, Any]:
    result = context.resources.aurekai.release_gate()
    if not result.ok:
        raise Exception(f"release-gate failed: {result.stderr}")
    context.add_output_metadata({"proof_uri": result.proof_uri, "passed": result.ok})
    return result.json_output


# Convenience job wrapping the full asset graph
from dagster import define_asset_job

aurekai_full_pipeline_job = define_asset_job(
    name="aurekai_full_pipeline",
    selection=[
        runtime_health,
        manifest_integrity,
        model_memory_pack,
        sae_audit,
        semantic_cache_bench,
        proof_bundle,
        release_gate,
    ],
    description="Full Aurekai capability pipeline: runtime → memory → proof → release gate.",
)
