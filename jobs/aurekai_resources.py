"""Aurekai Dagster resource — wraps the akai CLI as a configurable resource."""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from typing import Any

from dagster import ConfigurableResource, get_dagster_logger


@dataclass
class AurékaiResult:
    operator: str
    exit_code: int
    stdout: str
    stderr: str
    json_output: dict[str, Any] = field(default_factory=dict)
    proof_uri: str = ""

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


class AurékaiResource(ConfigurableResource):
    """Dagster resource for executing Aurekai capability operators via the akai CLI."""

    version: str = "0.8.0-alpha.5"
    binary: str = "akai"
    timeout: int = 120
    json_output: bool = True

    def _run(self, args: list[str]) -> AurékaiResult:
        cmd = [self.binary] + args
        if self.json_output and "--json" not in args:
            cmd.append("--json")
        log = get_dagster_logger()
        log.info(f"Running: {' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        result_json: dict = {}
        try:
            result_json = json.loads(proc.stdout)
        except Exception:
            pass
        proof_uri = result_json.get("proof_uri", "")
        return AurékaiResult(
            operator=" ".join(args[:2]),
            exit_code=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            json_output=result_json,
            proof_uri=proof_uri,
        )

    def doctor(self, deep: bool = True) -> AurékaiResult:
        args = ["doctor"]
        if deep:
            args.append("--deep")
        return self._run(args)

    def verify_manifest(self, manifest_path: str = "artifact.json") -> AurékaiResult:
        return self._run(["verify", "--manifest", manifest_path])

    def model_memory_pack(self, tag: str = "latest") -> AurékaiResult:
        return self._run(["pack", "--tag", tag])

    def sae_audit(self) -> AurékaiResult:
        return self._run(["sae", "audit"])

    def semantic_cache_bench(self, namespace: str = "default") -> AurékaiResult:
        return self._run(["cache", "bench", "--namespace", namespace])

    def proof_bundle_export(self, run_id: str) -> AurékaiResult:
        return self._run(["proof", "export", "--run-id", run_id])

    def release_gate(self, version: str = "") -> AurékaiResult:
        v = version or self.version
        return self._run(["release", "gate", "--version", v])
