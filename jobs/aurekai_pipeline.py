#!/usr/bin/env python3
"""Aurekai Dagster job — core pipeline."""
import subprocess
from dagster import op, job, Out, Output


@op(out=Out(str))
def doctor_deep(context):
    out = subprocess.run(["akai", "doctor", "--deep", "--json"], capture_output=True, text=True)
    context.log.info(out.stdout)
    return out.stdout


@op(out=Out(str))
def manifest_verify(context, _: str):
    out = subprocess.run(["akai", "verify", "--manifest", "artifact.json", "--json"], capture_output=True, text=True)
    context.log.info(out.stdout)
    return out.stdout


@op(out=Out(str))
def release_gate(context, _: str):
    out = subprocess.run(["akai", "release", "gate", "--version", "0.8.0-alpha.4", "--json"], capture_output=True, text=True)
    context.log.info(out.stdout)
    return out.stdout


@job
def aurekai_core_pipeline():
    result = doctor_deep()
    verified = manifest_verify(result)
    release_gate(verified)
