# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PTG2 research-branch experiment harness.

This tool is intentionally outside the import runtime. It runs repeatable PTG2
scanner/import experiments, captures evidence, and writes ignored reports under
reports/ptg2-experiments/ so research branches can prove a change before it is
promoted to main.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import datetime as dt
import functools
import gzip
import hashlib
import http.server
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from threading import Thread
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SUITE_PATH = ROOT / "docs" / "research" / "ptg2_benchmark_suite.example.json"
DEFAULT_REPORT_DIR = ROOT / "reports" / "ptg2-experiments"
DEFAULT_SCANNER = ROOT / "support" / "ptg2_scanner" / "target" / "release" / "ptg2_scanner"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
QUALIFIED_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
TERMINAL_IMPORT_STATUSES = {"succeeded", "failed", "canceled", "dead_letter"}
API_LATENCY_PROBE_SCRIPT = r'''
import asyncio
import json
import os
import statistics
import time

from api.endpoint.pagination import PaginationParams
from api.ptg2_serving import search_ptg2_provider_procedures, search_ptg2_serving_table
from api.ptg2_tables import snapshot_serving_tables
from db.connection import db


async def timed_probe(label, factory, *, iterations, warmup):
    samples = []
    payload_total = None
    payload_items = None
    payload_present = False
    for index in range(int(warmup) + int(iterations)):
        started = time.perf_counter()
        payload = await factory()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if index >= int(warmup):
            samples.append(elapsed_ms)
        if payload is not None:
            payload_present = True
            payload_total = (payload.get("pagination") or {}).get("total")
            payload_items = len(payload.get("items") or [])
    ordered = sorted(samples)
    p95_index = min(len(ordered) - 1, max(0, int(round(len(ordered) * 0.95 + 0.499)) - 1))
    return {
        "label": label,
        "payload": payload_present,
        "total": payload_total,
        "items": payload_items,
        "iterations": len(samples),
        "p50_ms": round(float(statistics.median(samples)), 3) if samples else None,
        "p95_ms": round(float(ordered[p95_index]), 3) if samples else None,
        "max_ms": round(float(max(samples)), 3) if samples else None,
        "samples_ms": [round(float(value), 3) for value in samples],
    }


async def main():
    probe_config = json.loads(os.environ["HLTHPRT_PTG2_API_PROBE_CONFIG"])
    snapshot_id = str(probe_config["snapshot_id"])
    pagination = PaginationParams(page=1, limit=int(probe_config["limit"]), offset=0, source="probe")
    async with db.session() as session:
        serving_tables = await snapshot_serving_tables(session, snapshot_id)
        code_args = {
            "plan_id": probe_config["plan_id"],
            "code": probe_config["code"],
            "code_system": probe_config["code_system"],
            "include_details": "true",
        }
        npi_args = {
            "plan_id": probe_config["plan_id"],
            "snapshot_id": snapshot_id,
            "include_details": "true",
        }
        probes = {
            "code_lookup": await timed_probe(
                "code_lookup",
                lambda: search_ptg2_serving_table(
                    session,
                    snapshot_id,
                    code_args,
                    pagination,
                    serving_tables=serving_tables,
                ),
                iterations=probe_config["iterations"],
                warmup=probe_config["warmup"],
            ),
            "npi_reverse": await timed_probe(
                "npi_reverse",
                lambda: search_ptg2_provider_procedures(
                    session,
                    int(probe_config["npi"]),
                    npi_args,
                    pagination,
                ),
                iterations=probe_config["iterations"],
                warmup=probe_config["warmup"],
            ),
        }
    await db.disconnect()
    print(json.dumps({"probes": probes}, sort_keys=True))


asyncio.run(main())
'''


@dataclass(frozen=True)
class RunResult:
    case_id: str
    variant_id: str
    kind: str
    status: str
    command: list[str]
    env_overrides: dict[str, str]
    elapsed_seconds: float | None = None
    returncode: int | None = None
    frames: list[dict[str, Any]] | None = None
    progress: list[dict[str, Any]] | None = None
    scanner_config: dict[str, Any] | None = None
    scanner_summary: dict[str, Any] | None = None
    dedupe_summary: dict[str, Any] | None = None
    copy_outputs: dict[str, Any] | None = None
    memory: dict[str, Any] | None = None
    import_run: dict[str, Any] | None = None
    error: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "variant_id": self.variant_id,
            "kind": self.kind,
            "status": self.status,
            "command": self.command,
            "env_overrides": self.env_overrides,
            "elapsed_seconds": self.elapsed_seconds,
            "returncode": self.returncode,
            "frames": self.frames or [],
            "progress": self.progress or [],
            "scanner_config": self.scanner_config or {},
            "scanner_summary": self.scanner_summary or {},
            "dedupe_summary": self.dedupe_summary or {},
            "copy_outputs": self.copy_outputs or {},
            "memory": self.memory or {},
            "import_run": self.import_run or {},
            "error": self.error,
        }


def load_suite(path: Path | str = DEFAULT_SUITE_PATH) -> dict[str, Any]:
    suite_path = Path(path)
    with suite_path.open("r", encoding="utf-8") as fp:
        suite = json.load(fp)
    if not isinstance(suite, dict):
        raise ValueError("suite must be a JSON object")
    cases = suite.get("cases")
    variants = suite.get("variants")
    if not isinstance(cases, list) or not cases:
        raise ValueError("suite.cases must be a non-empty list")
    if not isinstance(variants, list) or not variants:
        raise ValueError("suite.variants must be a non-empty list")
    variant_ids = set()
    for variant in variants:
        variant_id = str(variant.get("id") or "").strip()
        if not variant_id:
            raise ValueError("every variant needs an id")
        if variant_id in variant_ids:
            raise ValueError(f"duplicate variant id: {variant_id}")
        variant_ids.add(variant_id)
    for case in cases:
        case_id = str(case.get("id") or "").strip()
        if not case_id:
            raise ValueError("every case needs an id")
        selected = case.get("variants") or list(variant_ids)
        missing = [item for item in selected if item not in variant_ids]
        if missing:
            raise ValueError(f"case {case_id} references missing variants: {missing}")
    return suite


def variant_map(suite: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item["id"]): item for item in suite.get("variants") or []}


def resolve_root_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def selected_cases(suite: dict[str, Any], case_ids: set[str] | None = None) -> list[dict[str, Any]]:
    cases = list(suite.get("cases") or [])
    if case_ids:
        cases = [case for case in cases if str(case.get("id")) in case_ids]
    return cases


def env_for_variant(case: dict[str, Any], variant: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for source in (case.get("env") or {}, variant.get("env") or {}):
        for key, value in source.items():
            if value is None:
                continue
            result[str(key)] = str(value)
    if "split_negotiated_rates" in case and "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES" not in result:
        result["HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES"] = str(case["split_negotiated_rates"])
    return result


def parse_sized_frames(stdout: bytes | str) -> list[dict[str, Any]]:
    data = stdout.encode("utf-8") if isinstance(stdout, str) else stdout
    frames: list[dict[str, Any]] = []
    offset = 0
    length = len(data)
    while offset < length:
        line_end = data.find(b"\n", offset)
        if line_end < 0:
            break
        header = data[offset:line_end]
        offset = line_end + 1
        if b"\t" not in header:
            continue
        name_raw, size_raw = header.split(b"\t", 1)
        try:
            size = int(size_raw)
        except ValueError:
            continue
        payload = data[offset : offset + size]
        offset += size
        if offset < length and data[offset : offset + 1] == b"\n":
            offset += 1
        try:
            parsed_payload = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError:
            parsed_payload = {"raw": payload.decode("utf-8", errors="replace")}
        frames.append({"name": name_raw.decode("utf-8", errors="replace"), "payload": parsed_payload})
    return frames


def first_frame_payload(frames: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    for frame in frames:
        if frame.get("name") == name and isinstance(frame.get("payload"), dict):
            return frame["payload"]
    return None


def parse_key_value_line(line: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    parts = line.strip().split("\t")
    for part in parts[1:]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        values[key] = coerce_scalar(value)
    return values


def parse_scanner_progress(stderr: bytes | str) -> list[dict[str, Any]]:
    text = stderr.decode("utf-8", errors="replace") if isinstance(stderr, bytes) else stderr
    return [parse_key_value_line(line) for line in text.splitlines() if line.startswith("PTG2_SCANNER_PROGRESS\t")]


def parse_import_done(text: bytes | str) -> dict[str, Any] | None:
    decoded = text.decode("utf-8", errors="replace") if isinstance(text, bytes) else text
    for line in decoded.splitlines():
        if line.startswith("PTG2_IMPORT_DONE\t"):
            return parse_key_value_line(line)
    return None


def parse_dedupe_summary(text: bytes | str) -> dict[str, Any] | None:
    decoded = text.decode("utf-8", errors="replace") if isinstance(text, bytes) else text
    for line in decoded.splitlines():
        if line.startswith("PTG2_DEDUPE_SUMMARY\t"):
            return parse_key_value_line(line)
    return None


def parse_serving_only_summary(text: bytes | str) -> dict[str, Any] | None:
    decoded = text.decode("utf-8", errors="replace") if isinstance(text, bytes) else text
    prefix = "PTG2 serving-only import summary: "
    for line in decoded.splitlines():
        if not line.startswith(prefix):
            continue
        payload = line[len(prefix) :]
        try:
            parsed = ast.literal_eval(payload)
        except (ValueError, SyntaxError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def coerce_scalar(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    if value in {"None", "null", ""}:
        return None
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def read_proc_status(status_path: Path) -> dict[str, int]:
    metrics: dict[str, int] = {}
    if not status_path.exists():
        return metrics
    for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith(("VmRSS:", "VmHWM:", "VmSize:")):
            continue
        key, raw_value = line.split(":", 1)
        parts = raw_value.strip().split()
        if not parts:
            continue
        try:
            metrics[f"{key.lower()}_kb"] = int(parts[0])
        except ValueError:
            continue
    return metrics


def parse_ps_memory(output: str) -> dict[str, int]:
    parts = output.strip().split()
    if len(parts) < 2:
        return {}
    try:
        return {"vmrss_kb": int(parts[0]), "vmsize_kb": int(parts[1])}
    except ValueError:
        return {}


def read_ps_memory(pid: int) -> dict[str, int]:
    completed = subprocess.run(
        ["ps", "-o", "rss=", "-o", "vsz=", "-p", str(pid)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return {}
    return parse_ps_memory(completed.stdout)


class ProcSampler:
    def __init__(self, proc_root: Path | str = "/proc") -> None:
        self.proc_root = Path(proc_root)
        self.samples = 0
        self.sampler = "proc_status"
        self.peak_rss_kb: int | None = None
        self.peak_hwm_kb: int | None = None
        self.peak_vmsize_kb: int | None = None

    def sample(self, pid: int) -> None:
        status = read_proc_status(self.proc_root / str(pid) / "status")
        if not status:
            status = read_ps_memory(pid)
            if not status:
                return
            self.sampler = "ps"
        self.samples += 1
        self.peak_rss_kb = max_optional(self.peak_rss_kb, status.get("vmrss_kb"))
        self.peak_hwm_kb = max_optional(self.peak_hwm_kb, status.get("vmhwm_kb"))
        self.peak_vmsize_kb = max_optional(self.peak_vmsize_kb, status.get("vmsize_kb"))

    def to_json(self) -> dict[str, Any]:
        return {
            "sampler": self.sampler,
            "samples": self.samples,
            "peak_rss_kb": self.peak_rss_kb,
            "peak_hwm_kb": self.peak_hwm_kb,
            "peak_vmsize_kb": self.peak_vmsize_kb,
        }


def max_optional(current: int | None, candidate: int | None) -> int | None:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return max(current, candidate)


def build_fixture_payload(case: dict[str, Any]) -> dict[str, Any]:
    fixture = str(case.get("fixture") or "large_in_network")
    if fixture == "large_in_network":
        rate_count = int(case.get("negotiated_rates") or 64)
        billing_code_count = max(int(case.get("billing_codes") or 1), 1)
        provider_set_count = max(int(case.get("provider_sets") or 1), 1)
        price_reuse_mod = int(case.get("price_reuse_mod") or 0)
        provider_cycle_divisor = max(price_reuse_mod, 1)
        additional_information = None
        if int(case.get("additional_information_bytes") or 0) > 0:
            additional_information = "x" * int(case.get("additional_information_bytes") or 0)
        in_network_items = []
        for code_index in range(billing_code_count):
            start = (rate_count * code_index) // billing_code_count
            end = (rate_count * (code_index + 1)) // billing_code_count
            negotiated_rates = []
            for index in range(start, end):
                price_index = index % price_reuse_mod if price_reuse_mod > 0 else index
                provider_ref_id = 7 + ((index // provider_cycle_divisor) % provider_set_count)
                negotiated_rates.append(
                    {
                        "provider_references": [provider_ref_id],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 100 + price_index,
                                "service_code": ["11"],
                                "billing_class": "professional",
                                **(
                                    {"additional_information": additional_information}
                                    if additional_information is not None
                                    else {}
                                ),
                            }
                        ],
                    }
                )
            in_network_items.append(
                {
                    "billing_code_type": "CPT",
                    "billing_code": f"{99213 + code_index}",
                    "name": f"Fixture service {code_index + 1}",
                    "negotiated_rates": negotiated_rates,
                }
            )
        return {
            "provider_references": [
                {
                    "provider_group_id": 7 + provider_index,
                    "provider_groups": [
                        {
                            "npi": [] if case.get("omit_provider_npis") else [1234567890 + provider_index],
                            "tin": {"type": "ein", "value": f"12-34567{provider_index % 10}"},
                        }
                    ],
                }
                for provider_index in range(provider_set_count)
            ],
            "in_network": in_network_items,
        }
    if fixture == "duplicate_serving":
        return {
            "provider_references": [
                {
                    "provider_group_id": 7,
                    "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
                }
            ],
            "in_network": [
                {
                    "billing_code_type": "CPT",
                    "billing_code": "99213",
                    "negotiated_rates": [
                        {
                            "provider_references": [7],
                            "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                        },
                        {
                            "provider_references": [7],
                            "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                        },
                    ],
                }
            ],
        }
    raise ValueError(f"unsupported fixture: {fixture}")


def load_json_file(path: Path) -> dict[str, Any]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fp:
        payload = json.load(fp)
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def write_fixture(case: dict[str, Any], output_dir: Path) -> Path:
    artifact = output_dir / f"{case['id']}.json.gz"
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(build_fixture_payload(case), separators=(",", ":")).encode("utf-8"))
    return artifact


def write_ptg_toc_fixture(case: dict[str, Any], output_dir: Path, *, base_url: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    rates_path = output_dir / "rates.json.gz"
    with gzip.open(rates_path, "wb") as fp:
        fp.write(json.dumps(build_fixture_payload(case), separators=(",", ":")).encode("utf-8"))
    plan_id = str(case.get("plan_id") or "LOCAL-PTG2-SMOKE")
    plan_market_type = str(case.get("plan_market_type") or "group")
    index = {
        "reporting_entity_name": case.get("reporting_entity_name") or "HealthPorta Local Fixture",
        "reporting_entity_type": case.get("reporting_entity_type") or "test",
        "last_updated_on": case.get("last_updated_on") or "2026-06-20",
        "version": "1.0",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": case.get("plan_name") or "Local PTG2 Smoke Plan",
                        "plan_id_type": case.get("plan_id_type") or "ein",
                        "plan_id": plan_id,
                        "plan_market_type": plan_market_type,
                        "plan_sponsor_name": case.get("plan_sponsor_name") or "HealthPorta Local",
                    }
                ],
                "in_network_files": [
                    {
                        "description": "local smoke in-network rates",
                        "location": f"{base_url}/rates.json.gz",
                    }
                ],
            }
        ],
    }
    index_path = output_dir / "index.json"
    index_path.write_text(json.dumps(index, separators=(",", ":")), encoding="utf-8")
    return index_path


def expected_original_file_summary(path: Path) -> dict[str, Any]:
    payload = load_json_file(path)
    provider_npis_by_ref: dict[int, set[str]] = {}
    for ref in payload.get("provider_references") or []:
        ref_id = ref.get("provider_group_id")
        if ref_id is None:
            continue
        npis: set[str] = set()
        for group in ref.get("provider_groups") or []:
            for npi in group.get("npi") or []:
                npis.add(str(npi))
        provider_npis_by_ref[int(ref_id)] = npis

    in_network_items = payload.get("in_network") or []
    price_keys: list[str] = []
    serving_keys: set[str] = set()
    used_npis: set[str] = set()
    negotiated_rate_count = 0
    for item in in_network_items:
        code_key = "\t".join([normalize_text(item.get("billing_code_type")), normalize_text(item.get("billing_code"))])
        for rate in item.get("negotiated_rates") or []:
            negotiated_rate_count += 1
            provider_refs = tuple(sorted(str(ref_id) for ref_id in rate.get("provider_references") or []))
            for ref_id in rate.get("provider_references") or []:
                try:
                    used_npis.update(provider_npis_by_ref.get(int(ref_id), set()))
                except (TypeError, ValueError):
                    continue
            for price in rate.get("negotiated_prices") or []:
                price_key = price_atom_original_key(price)
                price_keys.append(price_key)
                serving_keys.add("\t".join([code_key, ",".join(provider_refs), price_key]))

    unique_price_keys = sorted(set(price_keys))
    return {
        "provider_references": len(provider_npis_by_ref),
        "in_network_items": len(in_network_items),
        "negotiated_rates": negotiated_rate_count,
        "negotiated_prices": len(price_keys),
        "unique_serving_rates": len(serving_keys),
        "unique_price_atoms": len(unique_price_keys),
        "unique_provider_npis": len(used_npis),
        "price_atom_digest": digest_text_lines(unique_price_keys),
    }


def price_atom_original_key(price: dict[str, Any]) -> str:
    return "\t".join(
        [
            normalize_text(price.get("negotiated_type")),
            normalize_rate(price.get("negotiated_rate")),
            normalize_text(price.get("expiration_date")),
            ",".join(sorted(normalize_text(item) for item in price.get("service_code") or [])),
            normalize_text(price.get("billing_class")),
            normalize_text(price.get("setting")),
            ",".join(sorted(normalize_text(item) for item in price.get("billing_code_modifier") or [])),
            normalize_text(price.get("additional_information")),
        ]
    )


def normalize_text(value: Any) -> str:
    return "" if value is None else str(value)


def normalize_rate(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return normalize_text(value)


def digest_text_lines(lines: list[str]) -> str:
    digest = hashlib.md5()  # nosec B324 - local non-security parity digest
    for line in sorted(lines):
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def psql_json(env_overrides: dict[str, str], sql: str) -> dict[str, Any]:
    database = env_overrides["HLTHPRT_DB_DATABASE"]
    suffix = env_overrides.get("HLTHPRT_TEST_DATABASE_SUFFIX") or ""
    db_name = f"{database}{suffix}"
    cmd = [
        "psql",
        "-X",
        "-q",
        "-A",
        "-t",
        "-h",
        env_overrides["HLTHPRT_DB_HOST"],
        "-p",
        env_overrides["HLTHPRT_DB_PORT"],
        "-U",
        env_overrides["HLTHPRT_DB_USER"],
        "-d",
        db_name,
        "-c",
        sql,
    ]
    process_env = os.environ.copy()
    if "HLTHPRT_DB_PASSWORD" in env_overrides:
        process_env["PGPASSWORD"] = env_overrides["HLTHPRT_DB_PASSWORD"]
    completed = subprocess.run(cmd, cwd=str(ROOT), env=process_env, check=True, capture_output=True, text=True)
    output = completed.stdout.strip()
    if not output:
        return {}
    return json.loads(output)


def psql_exec(env_overrides: dict[str, str], sql: str) -> None:
    database = env_overrides["HLTHPRT_DB_DATABASE"]
    suffix = env_overrides.get("HLTHPRT_TEST_DATABASE_SUFFIX") or ""
    db_name = f"{database}{suffix}"
    cmd = [
        "psql",
        "-X",
        "-q",
        "-h",
        env_overrides["HLTHPRT_DB_HOST"],
        "-p",
        env_overrides["HLTHPRT_DB_PORT"],
        "-U",
        env_overrides["HLTHPRT_DB_USER"],
        "-d",
        db_name,
        "-c",
        sql,
    ]
    process_env = os.environ.copy()
    if "HLTHPRT_DB_PASSWORD" in env_overrides:
        process_env["PGPASSWORD"] = env_overrides["HLTHPRT_DB_PASSWORD"]
    subprocess.run(cmd, cwd=str(ROOT), env=process_env, check=True, capture_output=True, text=True)


def explain_json(env_overrides: dict[str, str], sql: str) -> list[dict[str, Any]]:
    """Run EXPLAIN ANALYZE in JSON format for a research benchmark query."""
    plan = psql_json(env_overrides, f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
    return plan if isinstance(plan, list) else []


def _explain_execution_ms(plan: list[dict[str, Any]]) -> float | None:
    if not plan or not isinstance(plan[0], dict):
        return None
    execution_time = plan[0].get("Execution Time")
    return round(float(execution_time), 3) if execution_time is not None else None


def is_pg_table_present(env_overrides: dict[str, str], table_name: str | None) -> bool:
    """Return whether a qualified PostgreSQL table name resolves in the target DB."""
    if not table_name:
        return False
    qualified = validate_qualified_table_name(str(table_name))
    payload = psql_json(
        env_overrides,
        "SELECT json_build_object("
        f"'exists', to_regclass('{sql_literal(qualified)}') IS NOT NULL"
        ") AS payload;",
    )
    return bool(payload.get("exists"))


pg_table_exists = is_pg_table_present


def serving_binary_artifact_kinds(env_overrides: dict[str, str], table_name: str | None) -> list[str]:
    """Return sorted artifact kinds present in a published serving-binary table."""
    if not table_name or not pg_table_exists(env_overrides, table_name):
        return []
    qualified = validate_qualified_table_name(str(table_name))
    payload = psql_json(
        env_overrides,
        "SELECT json_build_object("
        "'kinds', COALESCE(array_agg(DISTINCT artifact_kind ORDER BY artifact_kind), ARRAY[]::varchar[])"
        f") AS payload FROM {qualified};",
    )
    return [str(kind) for kind in payload.get("kinds") or []]


def validate_qualified_table_name(value: str) -> str:
    if not QUALIFIED_TABLE_RE.fullmatch(value or ""):
        raise ValueError(f"unsafe or invalid qualified table name: {value!r}")
    return value


def validate_identifier(value: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value or ""):
        raise ValueError(f"unsafe or invalid identifier: {value!r}")
    return value


def short_pg_identifier(seed: str, suffix: str = "") -> str:
    normalized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in seed.lower()).strip("_")
    suffix = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in suffix.lower()).strip("_")
    digest = hashlib.sha1(f"{normalized}:{suffix}".encode("utf-8")).hexdigest()[:10]  # nosec B324
    base_limit = 63 - len(digest) - 1
    if suffix:
        base_limit -= len(suffix) + 1
    base = normalized[: max(base_limit, 1)].strip("_") or "idx"
    parts = [base]
    if suffix:
        parts.append(suffix)
    parts.append(digest)
    return validate_identifier("_".join(parts)[:63])


def split_qualified_table(value: str) -> tuple[str, str]:
    qualified = validate_qualified_table_name(value)
    schema_name, table_name = qualified.split(".", 1)
    return validate_identifier(schema_name), validate_identifier(table_name)


def serving_index_table(serving_index: dict[str, Any], *keys: str) -> str:
    materialized = serving_index.get("materialized_tables")
    materialized_tables = materialized if isinstance(materialized, dict) else {}
    role_aliases = {
        "table": "serving",
        "serving_table": "serving",
        "price_atom_table": "price_atom",
        "provider_group_member_table": "provider_group_member",
        "provider_npi_scope_table": "provider_npi_scope",
    }
    for key in keys:
        value = serving_index.get(key)
        if value:
            return validate_qualified_table_name(str(value))
        role = role_aliases.get(key, key)
        value = materialized_tables.get(role)
        if value:
            return validate_qualified_table_name(str(value))
    raise ValueError(f"serving index does not expose any of: {', '.join(keys)}")


def _serving_index_table_or_none(serving_index: dict[str, Any], *keys: str) -> str | None:
    try:
        return serving_index_table(serving_index, *keys)
    except ValueError:
        return None


def snapshot_storage_table_entries(serving_index: dict[str, Any]) -> list[dict[str, str]]:
    """Return deduped snapshot-owned table names exposed by a serving index."""
    roles_by_table: dict[str, set[str]] = {}

    def add_table_role(table_role: str, raw_table_name: Any) -> None:
        """Track one table role from a serving index when it is fully qualified."""
        if not isinstance(raw_table_name, str) or not raw_table_name:
            return
        if not QUALIFIED_TABLE_RE.fullmatch(raw_table_name):
            return
        roles_by_table.setdefault(validate_qualified_table_name(raw_table_name), set()).add(table_role)

    materialized = serving_index.get("materialized_tables")
    if isinstance(materialized, dict):
        for table_role, raw_table_name in materialized.items():
            add_table_role(str(table_role), raw_table_name)

    for index_key, raw_table_name in serving_index.items():
        if index_key == "materialized_tables":
            continue
        if index_key != "table" and not index_key.endswith("_table"):
            continue
        add_table_role(str(index_key), raw_table_name)

    return [
        {
            "role": ",".join(sorted(roles)),
            "table": table_name,
        }
        for table_name, roles in sorted(
            roles_by_table.items(),
            key=lambda table_roles_entry: (sorted(table_roles_entry[1])[0], table_roles_entry[0]),
        )
    ]


def _values_table_sql(table_entries: list[dict[str, str]]) -> str:
    values_sql_parts = []
    for table_entry in table_entries:
        table_role = sql_literal(str(table_entry.get("role") or "unknown"))
        table_name = sql_literal(str(table_entry.get("table") or ""))
        values_sql_parts.append(f"('{table_role}', '{table_name}')")
    return ", ".join(values_sql_parts)


def _snapshot_table_storage_rows(
    env_overrides: dict[str, str],
    table_entries: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Measure heap and index bytes for snapshot-owned PostgreSQL tables."""
    if not table_entries:
        return []
    values_sql = _values_table_sql(table_entries)
    table_payload = psql_json(
        env_overrides,
        "WITH input(role, table_name) AS (VALUES "
        f"{values_sql}"
        "), measured AS ("
        "SELECT input.role, input.table_name, reg.oid AS relation_oid "
        "FROM input "
        "LEFT JOIN LATERAL (SELECT to_regclass(input.table_name) AS oid) reg ON TRUE"
        ") "
        "SELECT json_build_object("
        "'tables', COALESCE(json_agg("
        "json_build_object("
        "'role', role, "
        "'table', table_name, "
        "'exists', relation_oid IS NOT NULL, "
        "'heap_bytes', CASE WHEN relation_oid IS NULL THEN 0 ELSE pg_relation_size(relation_oid) END, "
        "'index_bytes', CASE WHEN relation_oid IS NULL THEN 0 ELSE pg_indexes_size(relation_oid) END, "
        "'total_bytes', CASE WHEN relation_oid IS NULL THEN 0 ELSE pg_total_relation_size(relation_oid) END"
        ") ORDER BY CASE WHEN relation_oid IS NULL THEN 0 ELSE pg_total_relation_size(relation_oid) END DESC, role"
        "), '[]'::json)) AS payload "
        "FROM measured;",
    )
    measured_tables = table_payload.get("tables")
    return measured_tables if isinstance(measured_tables, list) else []


def _snapshot_top_components(
    table_storage_rows: list[dict[str, Any]],
    artifact_storage: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return the largest table and artifact components in one sorted list."""
    top_components = [
        {
            "kind": "table",
            "name": str(table_storage.get("role") or table_storage.get("table")),
            "bytes": int(table_storage.get("total_bytes") or 0),
            "table": table_storage.get("table"),
        }
        for table_storage in table_storage_rows
        if int(table_storage.get("total_bytes") or 0) > 0
    ]
    for artifact_kind_storage in artifact_storage.get("by_kind") or []:
        top_components.append(
            {
                "kind": "artifact",
                "name": str(artifact_kind_storage.get("artifact_kind") or "unknown"),
                "bytes": int(artifact_kind_storage.get("stored_payload_bytes") or 0),
            }
        )
    top_components.sort(key=lambda component: int(component.get("bytes") or 0), reverse=True)
    return top_components[:8]


def analyze_snapshot_postgres_footprint(
    *,
    env_overrides: dict[str, str],
    serving_index: dict[str, Any],
    snapshot_id: str,
) -> dict[str, Any]:
    """Measure the full PostgreSQL footprint exposed by a published snapshot."""
    table_entries = snapshot_storage_table_entries(serving_index)
    schema_name = validate_identifier(env_overrides["HLTHPRT_DB_SCHEMA"])
    table_rows = _snapshot_table_storage_rows(env_overrides, table_entries)
    artifact_storage = analyze_snapshot_artifact_storage(
        env_overrides=env_overrides,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
    )
    table_total_bytes = sum(int(table_storage.get("total_bytes") or 0) for table_storage in table_rows)
    artifact_stored_bytes = int(artifact_storage.get("stored_payload_bytes") or 0)
    artifact_tuple_bytes = int(artifact_storage.get("tuple_bytes") or 0)
    return {
        "status": "passed",
        "snapshot_id": snapshot_id,
        "table_total_bytes": table_total_bytes,
        "artifact_stored_payload_bytes": artifact_stored_bytes,
        "artifact_tuple_bytes": artifact_tuple_bytes,
        "total_logical_bytes": table_total_bytes + artifact_stored_bytes,
        "total_with_artifact_tuple_bytes": table_total_bytes + artifact_tuple_bytes,
        "tables": table_rows,
        "artifact_storage": artifact_storage,
        "top_components": _snapshot_top_components(table_rows, artifact_storage),
    }


def analyze_snapshot_artifact_storage(
    *,
    env_overrides: dict[str, str],
    schema_name: str,
    snapshot_id: str,
) -> dict[str, Any]:
    """Measure durable artifact blobs owned by a snapshot."""
    if not snapshot_id:
        return {"status": "skipped", "reason": "missing_snapshot_id"}
    manifest_table = f"{schema_name}.ptg2_artifact_manifest"
    chunk_table = f"{schema_name}.ptg2_artifact_blob_chunk"
    if not pg_table_exists(env_overrides, manifest_table):
        return {"status": "skipped", "reason": "missing_artifact_manifest_table"}
    if not pg_table_exists(env_overrides, chunk_table):
        return {"status": "skipped", "reason": "missing_artifact_chunk_table"}
    return psql_json(
        env_overrides,
        "WITH artifacts AS ("
        "SELECT artifact_id, COALESCE(artifact_kind, 'unknown') AS artifact_kind, byte_count, pg_column_size(m.*) AS tuple_bytes "
        f"FROM {manifest_table} m "
        f"WHERE snapshot_id = '{sql_literal(snapshot_id)}'"
        "), chunk_stats AS ("
        "SELECT c.artifact_id, count(*) AS chunks, "
        "COALESCE(sum(c.byte_count), 0) AS stored_payload_bytes, "
        "COALESCE(sum(c.raw_byte_count), 0) AS raw_bytes, "
        "COALESCE(sum(pg_column_size(c.*)), 0) AS tuple_bytes "
        f"FROM {chunk_table} c "
        "JOIN artifacts a ON a.artifact_id = c.artifact_id "
        "GROUP BY c.artifact_id"
        "), by_kind AS ("
        "SELECT a.artifact_kind, count(*) AS artifacts, "
        "COALESCE(sum(c.chunks), 0) AS chunks, "
        "COALESCE(sum(c.stored_payload_bytes), 0) AS stored_payload_bytes, "
        "COALESCE(sum(c.raw_bytes), 0) AS raw_bytes, "
        "COALESCE(sum(a.tuple_bytes), 0) + COALESCE(sum(c.tuple_bytes), 0) AS tuple_bytes "
        "FROM artifacts a "
        "LEFT JOIN chunk_stats c ON c.artifact_id = a.artifact_id "
        "GROUP BY a.artifact_kind"
        ") "
        "SELECT json_build_object("
        "'status', 'passed', "
        "'artifact_count', (SELECT count(*) FROM artifacts), "
        "'chunk_count', COALESCE((SELECT sum(chunks) FROM chunk_stats), 0), "
        "'stored_payload_bytes', COALESCE((SELECT sum(stored_payload_bytes) FROM chunk_stats), 0), "
        "'raw_bytes', COALESCE((SELECT sum(raw_bytes) FROM chunk_stats), 0), "
        "'tuple_bytes', "
        "COALESCE((SELECT sum(tuple_bytes) FROM artifacts), 0) + COALESCE((SELECT sum(tuple_bytes) FROM chunk_stats), 0), "
        "'by_kind', COALESCE((SELECT json_agg("
        "json_build_object("
        "'artifact_kind', artifact_kind, "
        "'artifacts', artifacts, "
        "'chunks', chunks, "
        "'stored_payload_bytes', stored_payload_bytes, "
        "'raw_bytes', raw_bytes, "
        "'tuple_bytes', tuple_bytes"
        ") ORDER BY stored_payload_bytes DESC, artifact_kind"
        ") FROM by_kind), '[]'::json)"
        ") AS payload;",
    )


def _expectation_result(expected: Any, actual: Any) -> dict[str, Any]:
    return {
        "expected": expected,
        "actual": actual,
        "passed": actual == expected,
    }


def has_serving_sidecar_artifacts(serving_index: dict[str, Any]) -> bool:
    """Return whether serving index still references forward/reverse serving sidecars."""
    artifact_manifest = serving_index.get("artifacts")
    serving_artifact_names = {"serving_by_code", "serving_by_provider_set"}
    if isinstance(artifact_manifest, dict):
        return any(bool(artifact_manifest.get(name)) for name in serving_artifact_names)
    if isinstance(artifact_manifest, list):
        return any(
            isinstance(artifact, dict)
            and str(artifact.get("name") or artifact.get("artifact_kind") or "") in serving_artifact_names
            for artifact in artifact_manifest
        )
    return False


def check_serving_index_expectations(
    env_overrides: dict[str, str],
    serving_index: dict[str, Any],
    expectations: dict[str, Any],
) -> dict[str, Any]:
    """Compare a persisted serving_index against declared suite expectations."""
    check_result_by_name: dict[str, dict[str, Any]] = {}
    if "expect_serving_row_strategy" in expectations:
        check_result_by_name["serving_row_strategy"] = _expectation_result(
            expectations["expect_serving_row_strategy"],
            serving_index.get("serving_row_strategy"),
        )
    if "expect_serving_table_retained" in expectations:
        check_result_by_name["serving_table_retained"] = _expectation_result(
            expectations["expect_serving_table_retained"],
            serving_index.get("serving_table_retained"),
        )
    if "expect_serving_table_exists" in expectations:
        serving_table = _serving_index_table_or_none(serving_index, "table", "serving_table")
        check_result_by_name["serving_table_exists"] = _expectation_result(
            expectations["expect_serving_table_exists"],
            pg_table_exists(env_overrides, serving_table),
        )
    if "expect_serving_binary_table_exists" in expectations:
        serving_binary_table = _serving_index_table_or_none(serving_index, "serving_binary_table")
        check_result_by_name["serving_binary_table_exists"] = _expectation_result(
            expectations["expect_serving_binary_table_exists"],
            pg_table_exists(env_overrides, serving_binary_table),
        )
    if "expect_serving_binary_writer" in expectations:
        binary_manifest = serving_index.get("serving_binary")
        binary_manifest = binary_manifest if isinstance(binary_manifest, dict) else {}
        check_result_by_name["serving_binary_writer"] = _expectation_result(
            expectations["expect_serving_binary_writer"],
            binary_manifest.get("writer"),
        )
    if "expect_serving_binary_kinds" in expectations:
        serving_binary_table = _serving_index_table_or_none(serving_index, "serving_binary_table")
        actual_kinds = serving_binary_artifact_kinds(env_overrides, serving_binary_table)
        check_result_by_name["serving_binary_kinds"] = _expectation_result(
            sorted(str(kind) for kind in expectations["expect_serving_binary_kinds"]),
            actual_kinds,
        )
    if "expect_serving_sidecar_artifacts" in expectations:
        check_result_by_name["serving_sidecar_artifacts"] = _expectation_result(
            expectations["expect_serving_sidecar_artifacts"],
            has_serving_sidecar_artifacts(serving_index),
        )
    return {
        "status": "passed" if all(check["passed"] for check in check_result_by_name.values()) else "failed",
        "checks": check_result_by_name,
    }


def serving_index_expectations_for(case: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    """Merge case and variant serving-shape expectations for a local suite run."""
    supported = (
        "expect_serving_row_strategy",
        "expect_serving_table_retained",
        "expect_serving_table_exists",
        "expect_serving_binary_table_exists",
        "expect_serving_binary_writer",
        "expect_serving_binary_kinds",
        "expect_serving_sidecar_artifacts",
    )
    expectation_by_key: dict[str, Any] = {}
    for key in supported:
        if key in case:
            expectation_by_key[key] = case[key]
        if key in variant:
            expectation_by_key[key] = variant[key]
    return expectation_by_key


def _is_api_latency_probe_enabled(case: dict[str, Any], variant: dict[str, Any]) -> bool:
    return bool(case.get("api_latency_probe") or variant.get("api_latency_probe"))


def _is_serving_storage_probe_enabled(case: dict[str, Any], variant: dict[str, Any]) -> bool:
    """Return whether this variant should inspect the retained serving table."""
    if "analyze_serving_sidecar" in variant:
        return bool(variant.get("analyze_serving_sidecar"))
    return bool(case.get("analyze_serving_sidecar"))


def _api_latency_probe_config(
    *,
    snapshot_id: str,
    case: dict[str, Any],
    variant: dict[str, Any],
) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot_id,
        "plan_id": str(case.get("plan_id") or "LOCAL-PTG2-SMOKE"),
        "code": str(case.get("api_probe_code") or "99213"),
        "code_system": str(case.get("api_probe_code_system") or "CPT"),
        "npi": int(case.get("api_probe_npi") or 1234567890),
        "limit": int(case.get("api_probe_limit") or 25),
        "iterations": int(case.get("api_probe_iterations") or variant.get("api_probe_iterations") or 8),
        "warmup": int(case.get("api_probe_warmup") or variant.get("api_probe_warmup") or 2),
        "max_ms": float(case.get("api_latency_max_ms") or variant.get("api_latency_max_ms") or 40.0),
    }


def _api_latency_probe_env(
    env_overrides: dict[str, str],
    probe_config_dict: dict[str, Any],
) -> dict[str, str]:
    probe_env_map = {**os.environ, **env_overrides}
    database_name = env_overrides.get("HLTHPRT_DB_DATABASE", "")
    database_suffix = env_overrides.get("HLTHPRT_TEST_DATABASE_SUFFIX") or ""
    if database_name and database_suffix:
        probe_env_map["HLTHPRT_DB_DATABASE_OVERRIDE"] = f"{database_name}{database_suffix}"
    probe_env_map["HLTHPRT_PTG2_API_PROBE_CONFIG"] = json.dumps(probe_config_dict, sort_keys=True)
    existing_python_path = probe_env_map.get("PYTHONPATH")
    probe_env_map["PYTHONPATH"] = f"{ROOT}{os.pathsep}{existing_python_path}" if existing_python_path else str(ROOT)
    return probe_env_map


def _failed_api_latency_probes(probes_by_name: dict[str, Any], max_latency_ms: float) -> list[str]:
    failed_probes = []
    for name, probe in probes_by_name.items():
        if not probe.get("payload"):
            failed_probes.append(f"{name}:no_payload")
            continue
        p95_latency = probe.get("p95_ms")
        if p95_latency is None or float(p95_latency) > float(max_latency_ms):
            failed_probes.append(f"{name}:p95_ms")
            continue
        max_latency = probe.get("max_ms")
        if max_latency is None or float(max_latency) > float(max_latency_ms):
            failed_probes.append(f"{name}:max_ms")
    return failed_probes


def run_api_latency_probe(
    *,
    env_overrides: dict[str, str],
    snapshot_id: str,
    case: dict[str, Any],
    variant: dict[str, Any],
) -> dict[str, Any]:
    """Run code lookup and NPI reverse lookup through the app API functions."""
    if not _is_api_latency_probe_enabled(case, variant):
        return {}
    if not snapshot_id:
        return {"status": "failed", "error": "missing snapshot_id", "probes": {}}
    probe_config_dict = _api_latency_probe_config(snapshot_id=snapshot_id, case=case, variant=variant)
    completed = subprocess.run(
        [sys.executable, "-c", API_LATENCY_PROBE_SCRIPT],
        cwd=str(ROOT),
        env=_api_latency_probe_env(env_overrides, probe_config_dict),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return {
            "status": "failed",
            "error": completed.stderr[-2000:],
            "stdout": completed.stdout[-1000:],
            "max_ms": probe_config_dict["max_ms"],
            "probes": {},
        }
    try:
        raw_result = json.loads(completed.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        return {
            "status": "failed",
            "error": f"failed to parse API latency probe output: {exc}",
            "stdout": completed.stdout[-1000:],
            "max_ms": probe_config_dict["max_ms"],
            "probes": {},
        }
    probes_by_name = raw_result.get("probes") if isinstance(raw_result.get("probes"), dict) else {}
    failed_probes = _failed_api_latency_probes(probes_by_name, float(probe_config_dict["max_ms"]))
    return {
        "status": "failed" if failed_probes else "passed",
        "max_ms": probe_config_dict["max_ms"],
        "failed": failed_probes,
        "probes": probes_by_name,
    }


def verify_local_import_against_original(
    *,
    env_overrides: dict[str, str],
    original_path: Path,
    import_run_id: str,
) -> dict[str, Any]:
    expected = expected_original_file_summary(original_path)
    schema_name = validate_identifier(env_overrides["HLTHPRT_DB_SCHEMA"])
    run_payload = psql_json(
        env_overrides,
        "SELECT row_to_json(t) FROM ("
        "SELECT import_run_id, status, report "
        f"FROM {schema_name}.ptg2_import_run "
        f"WHERE import_run_id = '{sql_literal(import_run_id)}'"
        ") AS t;",
    )
    report = run_payload.get("report") or {}
    serving_index = report.get("serving_index") or {}
    serving_table = serving_index_table(serving_index, "table", "serving_table")
    serving_table_exists = pg_table_exists(env_overrides, serving_table)
    binary_manifest = serving_index.get("serving_binary") if isinstance(serving_index.get("serving_binary"), dict) else {}
    serving_rows_sql = (
        f"(SELECT count(*) FROM {serving_table})"
        if serving_table_exists
        else str(int((binary_manifest or {}).get("row_count") or 0))
    )
    price_atom_table = serving_index_table(serving_index, "price_atom_table")
    provider_group_member_table = serving_index_table(
        serving_index,
        "provider_group_member_table",
        "provider_npi_scope_table",
    )
    price_atom_schema, price_atom_name = price_atom_table.split(".", 1)
    price_atom_columns_payload = psql_json(
        env_overrides,
        "SELECT json_build_object('columns', COALESCE(array_agg(column_name ORDER BY ordinal_position), ARRAY[]::text[])) "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{sql_literal(price_atom_schema)}' "
        f"AND table_name = '{sql_literal(price_atom_name)}';",
    )
    price_atom_columns = set(price_atom_columns_payload.get("columns") or [])
    digest_columns = {
        "negotiated_type",
        "negotiated_rate",
        "expiration_date",
        "service_code",
        "billing_class",
        "setting",
        "billing_code_modifier",
        "additional_information",
    }
    can_digest_price_atoms = digest_columns.issubset(price_atom_columns)
    price_atom_digest_sql = (
        "'price_atom_digest', (SELECT md5(COALESCE(string_agg(line, E'\\n' ORDER BY line) || E'\\n', '')) "
        "FROM (SELECT "
        "COALESCE(negotiated_type, '') || E'\\t' || "
        "COALESCE(negotiated_rate::text, '') || E'\\t' || "
        "COALESCE(expiration_date, '') || E'\\t' || "
        "COALESCE(array_to_string(ARRAY(SELECT item FROM unnest(service_code) AS item ORDER BY item), ','), '') || E'\\t' || "
        "COALESCE(billing_class, '') || E'\\t' || "
        "COALESCE(setting, '') || E'\\t' || "
        "COALESCE(array_to_string(ARRAY(SELECT item FROM unnest(billing_code_modifier) AS item ORDER BY item), ','), '') || E'\\t' || "
        "COALESCE(additional_information, '') AS line "
        f"FROM {price_atom_table}) AS price_lines)"
        if can_digest_price_atoms
        else "'price_atom_digest', NULL"
    )
    db_counts = psql_json(
        env_overrides,
        "SELECT json_build_object("
        f"'serving_rows', {serving_rows_sql}, "
        f"'price_atom_rows', (SELECT count(*) FROM {price_atom_table}), "
        f"'provider_group_member_rows', (SELECT count(*) FROM {provider_group_member_table}), "
        f"'provider_npis', (SELECT count(DISTINCT npi) FROM {provider_group_member_table}), "
        f"{price_atom_digest_sql}"
        ") AS payload;",
    )
    skipped_checks = []
    if not can_digest_price_atoms:
        skipped_checks.append("price_atom_digest")
    checks = {
        "run_status": run_payload.get("status") == "validated",
        "serving_rows": int(db_counts.get("serving_rows") or 0) == expected["unique_serving_rates"],
        "price_atom_rows": int(db_counts.get("price_atom_rows") or 0) == expected["unique_price_atoms"],
        "provider_group_member_rows": int(db_counts.get("provider_group_member_rows") or 0)
        == expected["unique_provider_npis"],
        "provider_npis": int(db_counts.get("provider_npis") or 0) == expected["unique_provider_npis"],
        "price_atom_digest": True
        if not can_digest_price_atoms
        else db_counts.get("price_atom_digest") == expected["price_atom_digest"],
        "report_serving_rates": int(report.get("serving_rates") or 0) == expected["unique_serving_rates"],
        "report_files_processed": int(report.get("files_processed") or 0) == 1,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "status": "failed" if failed else "passed",
        "failed": failed,
        "expected": expected,
        "db": db_counts,
        "skipped_checks": skipped_checks,
        "tables": {
            "serving": serving_table,
            "serving_exists": serving_table_exists,
            "price_atom": price_atom_table,
            "provider_group_member": provider_group_member_table,
        },
        "checks": checks,
    }


def import_run_serving_index(
    *,
    env_overrides: dict[str, str],
    import_run_id: str,
) -> dict[str, Any]:
    """Load the serving_index persisted on a completed PTG2 import run."""
    if not import_run_id:
        return {}
    schema_name = validate_identifier(env_overrides["HLTHPRT_DB_SCHEMA"])
    run_payload = psql_json(
        env_overrides,
        "SELECT row_to_json(t) FROM ("
        "SELECT report "
        f"FROM {schema_name}.ptg2_import_run "
        f"WHERE import_run_id = '{sql_literal(import_run_id)}'"
        ") AS t;",
    )
    report = run_payload.get("report") if isinstance(run_payload, dict) else {}
    report = report if isinstance(report, dict) else {}
    serving_index = report.get("serving_index")
    return serving_index if isinstance(serving_index, dict) else {}


def psql_copy_lines(env_overrides: dict[str, str], sql: str):
    database = env_overrides["HLTHPRT_DB_DATABASE"]
    suffix = env_overrides.get("HLTHPRT_TEST_DATABASE_SUFFIX") or ""
    db_name = f"{database}{suffix}"
    cmd = [
        "psql",
        "-X",
        "-q",
        "-A",
        "-t",
        "-h",
        env_overrides["HLTHPRT_DB_HOST"],
        "-p",
        env_overrides["HLTHPRT_DB_PORT"],
        "-U",
        env_overrides["HLTHPRT_DB_USER"],
        "-d",
        db_name,
        "-c",
        f"COPY ({sql}) TO STDOUT WITH (FORMAT text, DELIMITER E'\\t', NULL '')",
    ]
    process_env = os.environ.copy()
    if "HLTHPRT_DB_PASSWORD" in env_overrides:
        process_env["PGPASSWORD"] = env_overrides["HLTHPRT_DB_PASSWORD"]
    process = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=process_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    try:
        for line in process.stdout:
            yield line.rstrip("\n").split("\t")
    finally:
        process.stdout.close()
    stderr = process.stderr.read() if process.stderr is not None else ""
    returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(f"psql COPY failed with exit code {returncode}: {stderr.strip()}")


def _write_uvarint(fp, value: int) -> None:
    value = int(value)
    if value < 0:
        raise ValueError("uvarint cannot encode negative values")
    while value >= 0x80:
        fp.write(bytes([(value & 0x7F) | 0x80]))
        value >>= 7
    fp.write(bytes([value]))


def _append_uvarint(buffer: bytearray, value: int) -> None:
    value = int(value)
    if value < 0:
        raise ValueError("uvarint cannot encode negative values")
    while value >= 0x80:
        buffer.append((value & 0x7F) | 0x80)
        value >>= 7
    buffer.append(value)


def _read_uvarint(data: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while True:
        if offset >= len(data):
            raise ValueError("truncated uvarint")
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, offset
        shift += 7
        if shift > 63:
            raise ValueError("uvarint is too large")


def _uuid_bytes(value: str) -> bytes:
    text_value = str(value).strip().lower().replace("-", "")
    if len(text_value) != 32:
        raise ValueError(f"expected uuid value, got {value!r}")
    return bytes.fromhex(text_value)


def _uuid_text(raw: bytes) -> str:
    hex_value = raw.hex()
    return f"{hex_value[:8]}-{hex_value[8:12]}-{hex_value[12:16]}-{hex_value[16:20]}-{hex_value[20:]}"


def _serving_row_digest_update(
    digest: Any,
    code_key: int,
    provider_set_key: int,
    provider_count: int,
    price_set_id: str,
) -> None:
    digest.update(f"{code_key}\t{provider_set_key}\t{provider_count}\t{price_set_id.lower()}\n".encode("utf-8"))


def write_serving_by_code_candidate(rows: Any, output_path: Path) -> dict[str, Any]:
    """Write and round-trip a harness-only compressed serving-by-code artifact."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    body_path = output_path.with_suffix(output_path.suffix + ".body.tmp")
    price_set_to_key: dict[str, int] = {}
    price_set_values: list[str] = []
    blocks: list[dict[str, int]] = []
    row_count = 0
    body_offset = 0
    current_code: int | None = None
    current_block_count = 0
    previous_provider_set_key = 0
    source_digest = hashlib.sha256()

    with body_path.open("wb") as body:
        for raw_row in rows:
            code_key = int(raw_row[0])
            provider_set_key = int(raw_row[1])
            provider_count = int(raw_row[2])
            price_set_id = str(raw_row[3]).strip().lower()
            if current_code != code_key:
                if current_code is not None:
                    blocks[-1]["count"] = current_block_count
                blocks.append({"code_key": code_key, "offset": body_offset, "count": 0})
                current_code = code_key
                current_block_count = 0
                previous_provider_set_key = 0
            price_set_key = price_set_to_key.get(price_set_id)
            if price_set_key is None:
                price_set_key = len(price_set_values)
                price_set_to_key[price_set_id] = price_set_key
                price_set_values.append(price_set_id)
            provider_set_delta = provider_set_key - previous_provider_set_key
            before = body.tell()
            _write_uvarint(body, provider_set_delta)
            _write_uvarint(body, provider_count)
            _write_uvarint(body, price_set_key)
            body_offset += body.tell() - before
            previous_provider_set_key = provider_set_key
            current_block_count += 1
            row_count += 1
            _serving_row_digest_update(source_digest, code_key, provider_set_key, provider_count, price_set_id)
    if blocks:
        blocks[-1]["count"] = current_block_count

    metadata = {
        "format": "research_serving_by_code_v1",
        "row_count": row_count,
        "code_count": len(blocks),
        "price_set_count": len(price_set_values),
        "body_bytes": body_offset,
        "price_dictionary_bytes": len(price_set_values) * 16,
        "block_index_bytes": len(blocks) * 16,
        "source_sha256": source_digest.hexdigest(),
    }
    header = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with output_path.open("wb") as out:
        out.write(b"PTG2SBC1")
        out.write(struct.pack("<I", len(header)))
        out.write(header)
        for price_set_id in price_set_values:
            out.write(_uuid_bytes(price_set_id))
        for block in blocks:
            out.write(struct.pack("<iQI", int(block["code_key"]), int(block["offset"]), int(block["count"])))
        with body_path.open("rb") as body:
            shutil.copyfileobj(body, out)
    body_path.unlink(missing_ok=True)

    gzip_path = output_path.with_suffix(output_path.suffix + ".gz")
    with output_path.open("rb") as source, gzip.open(gzip_path, "wb", compresslevel=6) as compressed:
        shutil.copyfileobj(source, compressed)
    decoded_digest = digest_serving_by_code_candidate(output_path)
    metadata.update(
        {
            "artifact_path": str(output_path),
            "artifact_bytes": output_path.stat().st_size,
            "gzip_path": str(gzip_path),
            "gzip_bytes": gzip_path.stat().st_size,
            "decoded_sha256": decoded_digest,
            "roundtrip": "passed" if decoded_digest == metadata["source_sha256"] else "failed",
        }
    )
    return metadata


def digest_serving_by_code_candidate(path: Path) -> str:
    data = path.read_bytes()
    if data[:8] != b"PTG2SBC1":
        raise ValueError("unexpected serving-by-code artifact magic")
    header_len = struct.unpack("<I", data[8:12])[0]
    header_start = 12
    header_end = header_start + header_len
    metadata = json.loads(data[header_start:header_end].decode("utf-8"))
    price_count = int(metadata["price_set_count"])
    code_count = int(metadata["code_count"])
    price_start = header_end
    index_start = price_start + price_count * 16
    body_start = index_start + code_count * 16
    price_sets = [
        _uuid_text(data[price_start + index * 16 : price_start + (index + 1) * 16])
        for index in range(price_count)
    ]
    digest = hashlib.sha256()
    for index in range(code_count):
        raw = data[index_start + index * 16 : index_start + (index + 1) * 16]
        code_key, body_offset, count = struct.unpack("<iQI", raw)
        cursor = body_start + body_offset
        provider_set_key = 0
        for _ in range(count):
            provider_delta, cursor = _read_uvarint(data, cursor)
            provider_count, cursor = _read_uvarint(data, cursor)
            price_key, cursor = _read_uvarint(data, cursor)
            provider_set_key += provider_delta
            _serving_row_digest_update(digest, code_key, provider_set_key, provider_count, price_sets[price_key])
    return digest.hexdigest()


def _provider_set_pattern_digest_update(
    digest: Any,
    provider_set_key: int,
    code_keys: tuple[int, ...],
    entries: tuple[tuple[int, int], ...],
    price_sets: list[str],
) -> None:
    for code_key in code_keys:
        for provider_count, price_key in entries:
            _serving_row_digest_update(digest, code_key, provider_set_key, provider_count, price_sets[price_key])


def write_serving_by_provider_set_candidate(rows: Any, output_path: Path) -> dict[str, Any]:
    """Write and round-trip a harness-only reverse serving artifact.

    The reverse path is for "all prices for one NPI": NPI -> provider groups ->
    provider sets -> every code/price served by those sets. Rows are grouped by
    repeated price vectors so we do not store a second full copy of the matrix.
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    price_set_to_key: dict[str, int] = {}
    price_set_values: list[str] = []
    code_keys_seen: set[int] = set()
    row_count = 0
    current_provider_set: int | None = None
    current_code: int | None = None
    current_code_entries: list[tuple[int, int]] = []
    patterns_by_provider: list[tuple[int, dict[tuple[tuple[int, int], ...], list[int]]]] = []
    current_patterns: dict[tuple[tuple[int, int], ...], list[int]] = {}

    def price_key_for(value: str) -> int:
        price_set_id = str(value).strip().lower()
        price_set_key = price_set_to_key.get(price_set_id)
        if price_set_key is None:
            price_set_key = len(price_set_values)
            price_set_to_key[price_set_id] = price_set_key
            price_set_values.append(price_set_id)
        return price_set_key

    def flush_code() -> None:
        if current_code is None:
            return
        vector = tuple(current_code_entries)
        current_patterns.setdefault(vector, []).append(current_code)
        current_code_entries.clear()

    def flush_provider() -> None:
        if current_provider_set is None:
            return
        flush_code()
        patterns_by_provider.append((current_provider_set, current_patterns.copy()))
        current_patterns.clear()

    for raw_row in rows:
        provider_set_key = int(raw_row[0])
        code_key = int(raw_row[1])
        provider_count = int(raw_row[2])
        price_set_key = price_key_for(str(raw_row[3]))
        if current_provider_set != provider_set_key:
            flush_provider()
            current_provider_set = provider_set_key
            current_code = None
        if current_code != code_key:
            flush_code()
            current_code = code_key
        current_code_entries.append((provider_count, price_set_key))
        code_keys_seen.add(code_key)
        row_count += 1
    flush_provider()

    body_path = output_path.with_suffix(output_path.suffix + ".body.tmp")
    blocks: list[dict[str, int]] = []
    body_offset = 0
    source_digest = hashlib.sha256()
    pattern_count = 0
    with body_path.open("wb") as body:
        for provider_set_key, pattern_map in patterns_by_provider:
            block_pattern_count = 0
            blocks.append({"provider_set_key": provider_set_key, "offset": body_offset, "count": 0})
            ordered_patterns = sorted(
                pattern_map.items(),
                key=lambda item: (item[1][0] if item[1] else -1, item[0]),
            )
            for entries, code_key_list in ordered_patterns:
                code_keys = tuple(sorted(code_key_list))
                before = body.tell()
                _write_uvarint(body, len(code_keys))
                previous_code_key = 0
                for index, code_key in enumerate(code_keys):
                    if index == 0:
                        _write_uvarint(body, code_key)
                    else:
                        _write_uvarint(body, code_key - previous_code_key)
                    previous_code_key = code_key
                _write_uvarint(body, len(entries))
                for provider_count, price_set_key in entries:
                    _write_uvarint(body, provider_count)
                    _write_uvarint(body, price_set_key)
                body_offset += body.tell() - before
                block_pattern_count += 1
                pattern_count += 1
                _provider_set_pattern_digest_update(
                    source_digest,
                    provider_set_key,
                    code_keys,
                    entries,
                    price_set_values,
                )
            blocks[-1]["count"] = block_pattern_count

    metadata = {
        "format": "research_serving_by_provider_set_v1",
        "row_count": row_count,
        "provider_set_count": len(blocks),
        "code_count": len(code_keys_seen),
        "price_set_count": len(price_set_values),
        "pattern_count": pattern_count,
        "body_bytes": body_offset,
        "price_dictionary_bytes": len(price_set_values) * 16,
        "block_index_bytes": len(blocks) * 16,
        "source_sha256": source_digest.hexdigest(),
    }
    header = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with output_path.open("wb") as out:
        out.write(b"PTG2SBP1")
        out.write(struct.pack("<I", len(header)))
        out.write(header)
        for price_set_id in price_set_values:
            out.write(_uuid_bytes(price_set_id))
        for block in blocks:
            out.write(struct.pack("<iQI", int(block["provider_set_key"]), int(block["offset"]), int(block["count"])))
        with body_path.open("rb") as body:
            shutil.copyfileobj(body, out)
    body_path.unlink(missing_ok=True)

    gzip_path = output_path.with_suffix(output_path.suffix + ".gz")
    with output_path.open("rb") as source, gzip.open(gzip_path, "wb", compresslevel=6) as compressed:
        shutil.copyfileobj(source, compressed)
    decoded_digest = digest_serving_by_provider_set_candidate(output_path)
    metadata.update(
        {
            "artifact_path": str(output_path),
            "artifact_bytes": output_path.stat().st_size,
            "gzip_path": str(gzip_path),
            "gzip_bytes": gzip_path.stat().st_size,
            "decoded_sha256": decoded_digest,
            "roundtrip": "passed" if decoded_digest == metadata["source_sha256"] else "failed",
        }
    )
    return metadata


def digest_serving_by_provider_set_candidate(path: Path) -> str:
    data = path.read_bytes()
    if data[:8] != b"PTG2SBP1":
        raise ValueError("unexpected serving-by-provider-set artifact magic")
    header_len = struct.unpack("<I", data[8:12])[0]
    header_start = 12
    header_end = header_start + header_len
    metadata = json.loads(data[header_start:header_end].decode("utf-8"))
    price_count = int(metadata["price_set_count"])
    provider_set_count = int(metadata["provider_set_count"])
    price_start = header_end
    index_start = price_start + price_count * 16
    body_start = index_start + provider_set_count * 16
    price_sets = [
        _uuid_text(data[price_start + index * 16 : price_start + (index + 1) * 16])
        for index in range(price_count)
    ]
    digest = hashlib.sha256()
    for index in range(provider_set_count):
        raw = data[index_start + index * 16 : index_start + (index + 1) * 16]
        provider_set_key, body_offset, pattern_count = struct.unpack("<iQI", raw)
        cursor = body_start + body_offset
        for _ in range(pattern_count):
            code_count, cursor = _read_uvarint(data, cursor)
            code_keys: list[int] = []
            previous_code_key = 0
            for code_index in range(code_count):
                encoded_code, cursor = _read_uvarint(data, cursor)
                code_key = encoded_code if code_index == 0 else previous_code_key + encoded_code
                code_keys.append(code_key)
                previous_code_key = code_key
            entry_count, cursor = _read_uvarint(data, cursor)
            entries: list[tuple[int, int]] = []
            for _ in range(entry_count):
                provider_count, cursor = _read_uvarint(data, cursor)
                price_key, cursor = _read_uvarint(data, cursor)
                entries.append((provider_count, price_key))
            _provider_set_pattern_digest_update(
                digest,
                provider_set_key,
                tuple(code_keys),
                tuple(entries),
                price_sets,
            )
    return digest.hexdigest()


def _price_dictionary_payload(price_set_values: list[str]) -> bytes:
    return b"".join(_uuid_bytes(price_set_id) for price_set_id in price_set_values)


def build_serving_by_code_db_records(rows: Any) -> dict[str, Any]:
    records: list[tuple[str, int, int, int, bytes]] = []
    price_set_to_key: dict[str, int] = {}
    price_set_values: list[str] = []
    row_count = 0
    current_code: int | None = None
    current_payload = bytearray()
    current_count = 0
    previous_provider_set_key = 0
    source_digest = hashlib.sha256()

    def price_key_for(value: str) -> int:
        price_set_id = str(value).strip().lower()
        price_set_key = price_set_to_key.get(price_set_id)
        if price_set_key is None:
            price_set_key = len(price_set_values)
            price_set_to_key[price_set_id] = price_set_key
            price_set_values.append(price_set_id)
        return price_set_key

    def flush_code(
        code_key: int | None,
        payload: bytearray,
        count: int,
    ) -> tuple[bytearray, int]:
        if code_key is not None:
            records.append(("by_code", code_key, 0, count, bytes(payload)))
        return bytearray(), 0

    for raw_row in rows:
        code_key = int(raw_row[0])
        provider_set_key = int(raw_row[1])
        provider_count = int(raw_row[2])
        price_set_id = str(raw_row[3]).strip().lower()
        price_set_key = price_key_for(price_set_id)
        if current_code != code_key:
            current_payload, current_count = flush_code(
                current_code,
                current_payload,
                current_count,
            )
            current_code = code_key
            previous_provider_set_key = 0
        _append_uvarint(current_payload, provider_set_key - previous_provider_set_key)
        _append_uvarint(current_payload, provider_count)
        _append_uvarint(current_payload, price_set_key)
        previous_provider_set_key = provider_set_key
        current_count += 1
        row_count += 1
        _serving_row_digest_update(source_digest, code_key, provider_set_key, provider_count, price_set_id)
    current_payload, current_count = flush_code(current_code, current_payload, current_count)
    records.insert(
        0,
        (
            "by_code_price_dictionary",
            0,
            0,
            len(price_set_values),
            _price_dictionary_payload(price_set_values),
        ),
    )
    decoded_digest = digest_serving_by_code_db_records(records)
    return {
        "format": "research_postgres_serving_by_code_binary_v1",
        "records": records,
        "row_count": row_count,
        "code_count": len(records) - 1,
        "price_set_count": len(price_set_values),
        "payload_bytes": sum(len(record[4]) for record in records),
        "source_sha256": source_digest.hexdigest(),
        "decoded_sha256": decoded_digest,
        "roundtrip": "passed" if decoded_digest == source_digest.hexdigest() else "failed",
    }


def digest_serving_by_code_db_records(records: list[tuple[str, int, int, int, bytes]]) -> str:
    dictionaries = [record for record in records if record[0] == "by_code_price_dictionary"]
    if not dictionaries:
        raise ValueError("missing by-code price dictionary")
    dictionary_payload = dictionaries[0][4]
    if len(dictionary_payload) % 16:
        raise ValueError("invalid by-code price dictionary payload")
    price_sets = [
        _uuid_text(dictionary_payload[index : index + 16])
        for index in range(0, len(dictionary_payload), 16)
    ]
    digest = hashlib.sha256()
    for _kind, code_key, _block_no, row_count, payload in sorted(
        (record for record in records if record[0] == "by_code"),
        key=lambda record: (record[1], record[2]),
    ):
        cursor = 0
        provider_set_key = 0
        for _ in range(row_count):
            provider_delta, cursor = _read_uvarint(payload, cursor)
            provider_count, cursor = _read_uvarint(payload, cursor)
            price_key, cursor = _read_uvarint(payload, cursor)
            provider_set_key += provider_delta
            _serving_row_digest_update(digest, code_key, provider_set_key, provider_count, price_sets[price_key])
    return digest.hexdigest()


def build_serving_by_provider_set_db_records(rows: Any) -> dict[str, Any]:
    price_set_to_key: dict[str, int] = {}
    price_set_values: list[str] = []
    code_keys_seen: set[int] = set()
    row_count = 0
    current_provider_set: int | None = None
    current_code: int | None = None
    current_code_entries: list[tuple[int, int]] = []
    current_patterns: dict[tuple[tuple[int, int], ...], list[int]] = {}
    records: list[tuple[str, int, int, int, bytes]] = []
    source_digest = hashlib.sha256()

    def price_key_for(value: str) -> int:
        price_set_id = str(value).strip().lower()
        price_set_key = price_set_to_key.get(price_set_id)
        if price_set_key is None:
            price_set_key = len(price_set_values)
            price_set_to_key[price_set_id] = price_set_key
            price_set_values.append(price_set_id)
        return price_set_key

    def flush_code(
        code_key: int | None,
        code_entries: list[tuple[int, int]],
        pattern_map: dict[tuple[tuple[int, int], ...], list[int]],
    ) -> list[tuple[int, int]]:
        if code_key is not None:
            pattern_map.setdefault(tuple(code_entries), []).append(code_key)
        return []

    def append_provider_binary_block(
        provider_set_key: int | None,
        code_key: int | None,
        code_entries: list[tuple[int, int]],
        pattern_map: dict[tuple[tuple[int, int], ...], list[int]],
    ) -> tuple[list[tuple[int, int]], dict[tuple[tuple[int, int], ...], list[int]]]:
        """Append one reverse binary record and reset the current provider state."""
        if provider_set_key is None:
            return code_entries, pattern_map
        remaining_code_entries = flush_code(code_key, code_entries, pattern_map)
        binary_payload = bytearray()
        provider_row_count = 0
        pattern_items = sorted(
            pattern_map.items(),
            key=lambda item: (item[0], item[1]),
        )
        _append_uvarint(binary_payload, len(pattern_items))
        for entries, code_keys in pattern_items:
            sorted_code_keys = sorted(code_keys)
            _append_uvarint(binary_payload, len(sorted_code_keys))
            previous_code_key = 0
            for index, code_key in enumerate(sorted_code_keys):
                _append_uvarint(binary_payload, code_key if index == 0 else code_key - previous_code_key)
                previous_code_key = code_key
            _append_uvarint(binary_payload, len(entries))
            for provider_count, price_key in entries:
                _append_uvarint(binary_payload, provider_count)
                _append_uvarint(binary_payload, price_key)
            provider_row_count += len(sorted_code_keys) * len(entries)
        records.append(("by_provider_set", provider_set_key, 0, provider_row_count, bytes(binary_payload)))
        return remaining_code_entries, {}

    for raw_row in rows:
        provider_set_key = int(raw_row[0])
        code_key = int(raw_row[1])
        provider_count = int(raw_row[2])
        price_set_id = str(raw_row[3]).strip().lower()
        price_set_key = price_key_for(price_set_id)
        if current_provider_set != provider_set_key:
            current_code_entries, current_patterns = append_provider_binary_block(
                current_provider_set,
                current_code,
                current_code_entries,
                current_patterns,
            )
            current_provider_set = provider_set_key
            current_code = None
        if current_code != code_key:
            current_code_entries = flush_code(current_code, current_code_entries, current_patterns)
            current_code = code_key
        current_code_entries.append((provider_count, price_set_key))
        code_keys_seen.add(code_key)
        row_count += 1
        _serving_row_digest_update(source_digest, code_key, provider_set_key, provider_count, price_set_id)
    current_code_entries, current_patterns = append_provider_binary_block(
        current_provider_set,
        current_code,
        current_code_entries,
        current_patterns,
    )
    records.insert(
        0,
        (
            "by_provider_set_price_dictionary",
            0,
            0,
            len(price_set_values),
            _price_dictionary_payload(price_set_values),
        ),
    )
    decoded_digest = digest_serving_by_provider_set_db_records(records)
    source_sha256 = source_digest.hexdigest()
    return {
        "format": "research_postgres_serving_by_provider_set_binary_v1",
        "records": records,
        "row_count": row_count,
        "provider_set_count": len(records) - 1,
        "code_count": len(code_keys_seen),
        "price_set_count": len(price_set_values),
        "payload_bytes": sum(len(record[4]) for record in records),
        "source_sha256": source_sha256,
        "decoded_sha256": decoded_digest,
        "roundtrip": "passed" if decoded_digest == source_sha256 else "failed",
    }


def digest_serving_by_provider_set_db_records(records: list[tuple[str, int, int, int, bytes]]) -> str:
    dictionaries = [record for record in records if record[0] == "by_provider_set_price_dictionary"]
    if not dictionaries:
        raise ValueError("missing by-provider-set price dictionary")
    dictionary_payload = dictionaries[0][4]
    if len(dictionary_payload) % 16:
        raise ValueError("invalid by-provider-set price dictionary payload")
    price_sets = [
        _uuid_text(dictionary_payload[index : index + 16])
        for index in range(0, len(dictionary_payload), 16)
    ]
    digest = hashlib.sha256()
    for _kind, provider_set_key, _block_no, _row_count, payload in sorted(
        (record for record in records if record[0] == "by_provider_set"),
        key=lambda record: (record[1], record[2]),
    ):
        cursor = 0
        pattern_count, cursor = _read_uvarint(payload, cursor)
        for _ in range(pattern_count):
            code_count, cursor = _read_uvarint(payload, cursor)
            code_keys: list[int] = []
            previous_code_key = 0
            for index in range(code_count):
                encoded_code, cursor = _read_uvarint(payload, cursor)
                code_key = encoded_code if index == 0 else previous_code_key + encoded_code
                code_keys.append(code_key)
                previous_code_key = code_key
            entry_count, cursor = _read_uvarint(payload, cursor)
            entries: list[tuple[int, int]] = []
            for _ in range(entry_count):
                provider_count, cursor = _read_uvarint(payload, cursor)
                price_key, cursor = _read_uvarint(payload, cursor)
                entries.append((provider_count, price_key))
            _provider_set_pattern_digest_update(
                digest,
                provider_set_key,
                tuple(code_keys),
                tuple(entries),
                price_sets,
            )
    return digest.hexdigest()


def postgres_binary_candidate_table_names(
    *,
    schema_name: str,
    import_run_id: str,
    variant_id: str,
) -> dict[str, str]:
    schema = validate_identifier(schema_name)
    relation = short_pg_identifier(f"ptg2_research_binary_{variant_id}_{import_run_id}")
    return {"serving_binary": f"{schema}.{relation}"}


def postgres_posting_candidate_table_names(
    *,
    schema_name: str,
    import_run_id: str,
    variant_id: str,
) -> dict[str, str]:
    schema = validate_identifier(schema_name)
    posting_relation = short_pg_identifier(
        f"ptg2_research_posting_{variant_id}_{import_run_id}"
    )
    dictionary_relation = short_pg_identifier(
        f"ptg2_research_price_set_dict_{variant_id}_{import_run_id}"
    )
    return {
        "serving_posting": f"{schema}.{posting_relation}",
        "price_set_dictionary": f"{schema}.{dictionary_relation}",
    }


def postgres_posting_candidate_sql(
    *,
    serving_table: str,
    posting_table: str,
    price_set_dictionary_table: str,
    block_rows: int,
) -> list[str]:
    serving = validate_qualified_table_name(serving_table)
    posting = validate_qualified_table_name(posting_table)
    dictionary = validate_qualified_table_name(price_set_dictionary_table)
    posting_schema, posting_name = split_qualified_table(posting)
    dictionary_schema, dictionary_name = split_qualified_table(dictionary)
    rows_per_block = max(int(block_rows), 1)
    posting_code_idx = short_pg_identifier(posting_name, "code_idx")
    posting_provider_gin = short_pg_identifier(posting_name, "provider_gin")
    posting_price_gin = short_pg_identifier(posting_name, "price_gin")
    dictionary_key_idx = short_pg_identifier(dictionary_name, "key_idx")
    dictionary_value_idx = short_pg_identifier(dictionary_name, "value_idx")
    return [
        f"DROP TABLE IF EXISTS {posting} CASCADE;",
        f"DROP TABLE IF EXISTS {dictionary} CASCADE;",
        f"""
        CREATE UNLOGGED TABLE {dictionary} AS
        SELECT
            (row_number() OVER (ORDER BY price_set_global_id_128) - 1)::integer AS price_set_key,
            price_set_global_id_128
        FROM (
            SELECT DISTINCT price_set_global_id_128
            FROM {serving}
        ) distinct_price_sets;
        """,
        f"""
        CREATE UNIQUE INDEX {dictionary_key_idx}
        ON {dictionary} (price_set_key);
        """,
        f"""
        CREATE UNIQUE INDEX {dictionary_value_idx}
        ON {dictionary} (price_set_global_id_128);
        """,
        f"""
        CREATE UNLOGGED TABLE {posting} AS
        WITH keyed AS (
            SELECT
                serving.code_key,
                serving.provider_set_key,
                serving.provider_count,
                dictionary.price_set_key,
                (
                    row_number() OVER (
                        PARTITION BY serving.code_key
                        ORDER BY serving.provider_set_key, dictionary.price_set_key
                    ) - 1
                ) / {rows_per_block} AS block_no
            FROM {serving} serving
            JOIN {dictionary} dictionary
              ON dictionary.price_set_global_id_128 = serving.price_set_global_id_128
        )
        SELECT
            code_key,
            block_no::integer AS block_no,
            count(*)::integer AS row_count,
            array_agg(provider_set_key ORDER BY provider_set_key, price_set_key)::integer[]
                AS provider_set_keys,
            array_agg(provider_count ORDER BY provider_set_key, price_set_key)::integer[]
                AS provider_counts,
            array_agg(price_set_key ORDER BY provider_set_key, price_set_key)::integer[]
                AS price_set_keys
        FROM keyed
        GROUP BY code_key, block_no;
        """,
        f"""
        CREATE INDEX {posting_code_idx}
        ON {posting} (code_key, block_no);
        """,
        f"""
        CREATE INDEX {posting_provider_gin}
        ON {posting} USING gin (provider_set_keys);
        """,
        f"""
        CREATE INDEX {posting_price_gin}
        ON {posting} USING gin (price_set_keys);
        """,
        f"ANALYZE {posting_schema}.{posting_name};",
        f"ANALYZE {dictionary_schema}.{dictionary_name};",
    ]


async def _write_postgres_binary_records_async(
    *,
    env_overrides: dict[str, str],
    table_name: str,
    records: list[tuple[str, int, int, int, bytes]],
) -> None:
    import asyncpg

    database = env_overrides["HLTHPRT_DB_DATABASE"]
    suffix = env_overrides.get("HLTHPRT_TEST_DATABASE_SUFFIX") or ""
    db_name = f"{database}{suffix}"
    schema_name, relation_name = split_qualified_table(table_name)
    index_name = short_pg_identifier(relation_name, "kind_key_uidx")
    conn = await asyncpg.connect(
        host=env_overrides["HLTHPRT_DB_HOST"],
        port=int(env_overrides["HLTHPRT_DB_PORT"]),
        user=env_overrides["HLTHPRT_DB_USER"],
        password=env_overrides.get("HLTHPRT_DB_PASSWORD"),
        database=db_name,
    )
    try:
        await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        await conn.execute(
            f"""
            CREATE UNLOGGED TABLE {table_name} (
                artifact_kind text NOT NULL,
                block_key integer NOT NULL,
                block_no integer NOT NULL,
                row_count integer NOT NULL,
                payload bytea NOT NULL
            );
            """
        )
        await conn.copy_records_to_table(
            relation_name,
            records=records,
            columns=["artifact_kind", "block_key", "block_no", "row_count", "payload"],
            schema_name=schema_name,
        )
        await conn.execute(f"CREATE UNIQUE INDEX {index_name} ON {table_name} (artifact_kind, block_key, block_no);")
        await conn.execute(f"ANALYZE {table_name};")
    finally:
        await conn.close()


async def _read_postgres_binary_records_async(
    *,
    env_overrides: dict[str, str],
    table_name: str,
) -> list[tuple[str, int, int, int, bytes]]:
    import asyncpg

    database = env_overrides["HLTHPRT_DB_DATABASE"]
    suffix = env_overrides.get("HLTHPRT_TEST_DATABASE_SUFFIX") or ""
    db_name = f"{database}{suffix}"
    conn = await asyncpg.connect(
        host=env_overrides["HLTHPRT_DB_HOST"],
        port=int(env_overrides["HLTHPRT_DB_PORT"]),
        user=env_overrides["HLTHPRT_DB_USER"],
        password=env_overrides.get("HLTHPRT_DB_PASSWORD"),
        database=db_name,
    )
    try:
        rows = await conn.fetch(
            f"""
            SELECT artifact_kind, block_key, block_no, row_count, payload
            FROM {table_name}
            ORDER BY artifact_kind, block_key, block_no
            """
        )
    finally:
        await conn.close()
    return [
        (
            str(row["artifact_kind"]),
            int(row["block_key"]),
            int(row["block_no"]),
            int(row["row_count"]),
            bytes(row["payload"]),
        )
        for row in rows
    ]


def analyze_postgres_binary_candidate(
    *,
    env_overrides: dict[str, str],
    serving_table: str,
    import_run_id: str,
    variant_id: str,
    include_reverse: bool,
) -> dict[str, Any]:
    schema_name, _serving_name = split_qualified_table(serving_table)
    table_names = postgres_binary_candidate_table_names(
        schema_name=schema_name,
        import_run_id=import_run_id,
        variant_id=variant_id,
    )
    table_name = table_names["serving_binary"]
    started = time.monotonic()
    sql_by_code = (
        "SELECT code_key, provider_set_key, provider_count, price_set_global_id_128 "
        f"FROM {serving_table} "
        "ORDER BY code_key, provider_set_key, price_set_global_id_128"
    )
    forward = build_serving_by_code_db_records(psql_copy_lines(env_overrides, sql_by_code))
    records = list(forward["records"])
    reverse = None
    if include_reverse:
        sql_by_provider_set = (
            "SELECT provider_set_key, code_key, provider_count, price_set_global_id_128 "
            f"FROM {serving_table} "
            "ORDER BY provider_set_key, code_key, price_set_global_id_128"
        )
        reverse = build_serving_by_provider_set_db_records(psql_copy_lines(env_overrides, sql_by_provider_set))
        records.extend(reverse["records"])
    asyncio.run(
        _write_postgres_binary_records_async(
            env_overrides=env_overrides,
            table_name=table_name,
            records=records,
        )
    )
    readback_records = asyncio.run(
        _read_postgres_binary_records_async(
            env_overrides=env_overrides,
            table_name=table_name,
        )
    )
    build_elapsed_seconds = time.monotonic() - started
    readback_forward_digest = digest_serving_by_code_db_records(readback_records)
    readback_reverse_digest = None
    if include_reverse:
        readback_reverse_digest = digest_serving_by_provider_set_db_records(readback_records)
    forward_roundtrip = forward.get("source_sha256") == readback_forward_digest
    reverse_roundtrip = True if reverse is None else reverse.get("source_sha256") == readback_reverse_digest
    storage = psql_json(
        env_overrides,
        "SELECT json_build_object("
        f"'source_total_bytes', pg_total_relation_size('{sql_literal(serving_table)}'::regclass), "
        f"'source_heap_bytes', pg_relation_size('{sql_literal(serving_table)}'::regclass), "
        f"'source_rows', (SELECT count(*) FROM {serving_table}), "
        f"'artifact_total_bytes', pg_total_relation_size('{sql_literal(table_name)}'::regclass), "
        f"'artifact_heap_bytes', pg_relation_size('{sql_literal(table_name)}'::regclass), "
        f"'artifact_index_bytes', pg_indexes_size('{sql_literal(table_name)}'::regclass), "
        f"'artifact_rows', (SELECT count(*) FROM {table_name}), "
        f"'artifact_payload_bytes', (SELECT COALESCE(sum(octet_length(payload)), 0) FROM {table_name}), "
        f"'kind_stats', ("
        f"SELECT json_object_agg(artifact_kind, json_build_object('rows', rows, 'payload_bytes', payload_bytes)) "
        f"FROM ("
        f"SELECT artifact_kind, count(*) AS rows, COALESCE(sum(octet_length(payload)), 0) AS payload_bytes "
        f"FROM {table_name} GROUP BY artifact_kind"
        f") stats"
        f")"
        ") AS payload;",
    )
    source_rows = int(storage.get("source_rows") or 0)
    artifact_total_bytes = int(storage.get("artifact_total_bytes") or 0)
    source_total_bytes = int(storage.get("source_total_bytes") or 0)
    sample = psql_json(
        env_overrides,
        "SELECT row_to_json(t) FROM ("
        "SELECT "
        "(SELECT block_key FROM {table} WHERE artifact_kind = 'by_code' ORDER BY row_count DESC, block_key LIMIT 1) "
        "AS code_key, "
        "(SELECT block_key FROM {table} WHERE artifact_kind = 'by_provider_set' ORDER BY row_count DESC, block_key LIMIT 1) "
        "AS provider_set_key"
        ") AS t;".format(table=table_name),
    )
    benchmarks: dict[str, Any] = {}
    if sample.get("code_key") is not None:
        code_key = int(sample["code_key"])
        plan = explain_json(
            env_overrides,
            f"SELECT payload FROM {table_name} WHERE artifact_kind = 'by_code' AND block_key = {code_key}",
        )
        benchmarks["by_code_fetch"] = {
            "execution_ms": _explain_execution_ms(plan),
            "plan": plan[0].get("Plan", {}) if isinstance(plan, list) and plan and isinstance(plan[0], dict) else {},
        }
    if include_reverse and sample.get("provider_set_key") is not None:
        provider_set_key = int(sample["provider_set_key"])
        plan = explain_json(
            env_overrides,
            f"SELECT payload FROM {table_name} "
            f"WHERE artifact_kind = 'by_provider_set' AND block_key = {provider_set_key}",
        )
        benchmarks["by_provider_set_fetch"] = {
            "execution_ms": _explain_execution_ms(plan),
            "plan": plan[0].get("Plan", {}) if isinstance(plan, list) and plan and isinstance(plan[0], dict) else {},
        }
    combined_rows = int(forward.get("row_count") or 0) + int((reverse or {}).get("row_count") or 0)
    roundtrip_passed = forward_roundtrip and reverse_roundtrip and int(forward.get("row_count") or 0) == source_rows
    return {
        "status": "passed" if roundtrip_passed else "failed",
        "layout": "postgres_binary_v1",
        "tables": table_names,
        "storage": storage,
        "forward": {
            key: value for key, value in forward.items() if key != "records"
        },
        "reverse": (
            {key: value for key, value in reverse.items() if key != "records"}
            if isinstance(reverse, dict)
            else None
        ),
        "combined_row_count": combined_rows,
        "build_elapsed_seconds": round(build_elapsed_seconds, 3),
        "benchmarks": benchmarks,
        "candidate": {
            "roundtrip": "passed" if roundtrip_passed else "failed",
            "candidate_total_bytes": artifact_total_bytes,
            "source_total_bytes": source_total_bytes,
        },
        "reduction_ratio_vs_pg_total": (
            round(source_total_bytes / artifact_total_bytes, 3) if artifact_total_bytes else None
        ),
    }


def analyze_postgres_posting_candidate(
    *,
    env_overrides: dict[str, str],
    serving_table: str,
    import_run_id: str,
    variant_id: str,
    block_rows: int,
) -> dict[str, Any]:
    schema_name, _serving_name = split_qualified_table(serving_table)
    table_names = postgres_posting_candidate_table_names(
        schema_name=schema_name,
        import_run_id=import_run_id,
        variant_id=variant_id,
    )
    started = time.monotonic()
    for statement in postgres_posting_candidate_sql(
        serving_table=serving_table,
        posting_table=table_names["serving_posting"],
        price_set_dictionary_table=table_names["price_set_dictionary"],
        block_rows=block_rows,
    ):
        psql_exec(env_overrides, statement)
    build_elapsed_seconds = time.monotonic() - started

    posting_table = table_names["serving_posting"]
    price_set_dictionary_table = table_names["price_set_dictionary"]
    storage = psql_json(
        env_overrides,
        "SELECT json_build_object("
        f"'source_total_bytes', pg_total_relation_size('{sql_literal(serving_table)}'::regclass), "
        f"'source_heap_bytes', pg_relation_size('{sql_literal(serving_table)}'::regclass), "
        f"'source_rows', (SELECT count(*) FROM {serving_table}), "
        f"'posting_total_bytes', pg_total_relation_size('{sql_literal(posting_table)}'::regclass), "
        f"'posting_heap_bytes', pg_relation_size('{sql_literal(posting_table)}'::regclass), "
        f"'posting_index_bytes', pg_indexes_size('{sql_literal(posting_table)}'::regclass), "
        f"'posting_blocks', (SELECT count(*) FROM {posting_table}), "
        f"'posting_rows', (SELECT COALESCE(sum(row_count), 0) FROM {posting_table}), "
        f"'price_set_dictionary_total_bytes', pg_total_relation_size('{sql_literal(price_set_dictionary_table)}'::regclass), "
        f"'price_set_dictionary_rows', (SELECT count(*) FROM {price_set_dictionary_table}), "
        f"'candidate_total_bytes', "
        f"pg_total_relation_size('{sql_literal(posting_table)}'::regclass) + "
        f"pg_total_relation_size('{sql_literal(price_set_dictionary_table)}'::regclass)"
        ") AS payload;",
    )
    source_rows = int(storage.get("source_rows") or 0)
    posting_rows = int(storage.get("posting_rows") or 0)
    candidate_total_bytes = int(storage.get("candidate_total_bytes") or 0)
    source_total_bytes = int(storage.get("source_total_bytes") or 0)
    sample = psql_json(
        env_overrides,
        "SELECT row_to_json(t) FROM ("
        "SELECT code_key, provider_set_keys[1] AS provider_set_key, price_set_keys[1] AS price_set_key "
        f"FROM {posting_table} "
        "WHERE row_count > 0 "
        "ORDER BY row_count DESC, code_key, block_no "
        "LIMIT 1"
        ") AS t;",
    )
    benchmarks: dict[str, Any] = {}
    if sample.get("code_key") is not None and sample.get("provider_set_key") is not None:
        code_key = int(sample["code_key"])
        provider_set_key = int(sample["provider_set_key"])
        price_set_key = int(sample.get("price_set_key") or 0)
        benchmark_sql = {
            "code_lookup": f"SELECT COALESCE(SUM(row_count), 0) FROM {posting_table} WHERE code_key = {code_key}",
            "provider_overlap": (
                f"SELECT COALESCE(SUM(row_count), 0) FROM {posting_table} "
                f"WHERE provider_set_keys && ARRAY[{provider_set_key}]::integer[]"
            ),
            "code_provider_overlap": (
                f"SELECT COALESCE(SUM(row_count), 0) FROM {posting_table} "
                f"WHERE code_key = {code_key} "
                f"AND provider_set_keys && ARRAY[{provider_set_key}]::integer[]"
            ),
            "price_overlap": (
                f"SELECT COALESCE(SUM(row_count), 0) FROM {posting_table} "
                f"WHERE price_set_keys && ARRAY[{price_set_key}]::integer[]"
            ),
        }
        for name, sql in benchmark_sql.items():
            plan = explain_json(env_overrides, sql)
            benchmarks[name] = {
                "execution_ms": _explain_execution_ms(plan),
                "plan": plan[0].get("Plan", {}) if isinstance(plan, list) and plan and isinstance(plan[0], dict) else {},
            }
    roundtrip_passed = source_rows == posting_rows
    return {
        "status": "passed" if roundtrip_passed else "failed",
        "layout": "postgres_posting_v1",
        "block_rows": max(int(block_rows), 1),
        "tables": table_names,
        "storage": storage,
        "build_elapsed_seconds": round(build_elapsed_seconds, 3),
        "benchmarks": benchmarks,
        "candidate": {
            "roundtrip": "passed" if roundtrip_passed else "failed",
            "candidate_total_bytes": candidate_total_bytes,
            "source_total_bytes": source_total_bytes,
        },
        "reduction_ratio_vs_pg_total": (
            round(source_total_bytes / candidate_total_bytes, 3) if candidate_total_bytes else None
        ),
    }


def analyze_local_serving_sidecar_candidate(
    *,
    env_overrides: dict[str, str],
    import_run_id: str,
    variant_id: str,
    output_dir: Path,
    max_rows: int,
) -> dict[str, Any]:
    schema_name = validate_identifier(env_overrides["HLTHPRT_DB_SCHEMA"])
    run_payload = psql_json(
        env_overrides,
        "SELECT row_to_json(t) FROM ("
        "SELECT import_run_id, status, report "
        f"FROM {schema_name}.ptg2_import_run "
        f"WHERE import_run_id = '{sql_literal(import_run_id)}'"
        ") AS t;",
    )
    report = run_payload.get("report") or {}
    serving_index = report.get("serving_index") or {}
    serving_table = serving_index_table(serving_index, "table", "serving_table")
    storage = psql_json(
        env_overrides,
        "SELECT json_build_object("
        f"'row_count', (SELECT count(*) FROM {serving_table}), "
        f"'code_count', (SELECT count(DISTINCT code_key) FROM {serving_table}), "
        f"'provider_set_count', (SELECT count(DISTINCT provider_set_key) FROM {serving_table}), "
        f"'price_set_count', (SELECT count(DISTINCT price_set_global_id_128) FROM {serving_table}), "
        f"'heap_bytes', pg_relation_size(to_regclass('{sql_literal(serving_table)}')), "
        f"'index_bytes', pg_indexes_size(to_regclass('{sql_literal(serving_table)}')), "
        f"'total_bytes', pg_total_relation_size(to_regclass('{sql_literal(serving_table)}'))"
        ") AS payload;",
    )
    row_count = int(storage.get("row_count") or 0)
    if row_count > max_rows:
        return {
            "status": "skipped",
            "reason": "row_limit",
            "max_rows": max_rows,
            "serving_table": serving_table,
            "storage": storage,
        }
    postgres_posting_candidate = None
    if bool(str(env_overrides.get("HLTHPRT_PTG2_RESEARCH_POSTGRES_POSTING") or "").strip()):
        postgres_posting_candidate = analyze_postgres_posting_candidate(
            env_overrides=env_overrides,
            serving_table=serving_table,
            import_run_id=import_run_id,
            variant_id=variant_id,
            block_rows=int(env_overrides.get("HLTHPRT_PTG2_RESEARCH_POSTGRES_POSTING_BLOCK_ROWS") or 512),
        )
    postgres_binary_candidate = None
    if bool(str(env_overrides.get("HLTHPRT_PTG2_RESEARCH_POSTGRES_BINARY") or "").strip()):
        postgres_binary_candidate = analyze_postgres_binary_candidate(
            env_overrides=env_overrides,
            serving_table=serving_table,
            import_run_id=import_run_id,
            variant_id=variant_id,
            include_reverse=bool(
                str(
                    env_overrides.get("HLTHPRT_PTG2_RESEARCH_POSTGRES_BINARY_REVERSE")
                    or env_overrides.get("HLTHPRT_PTG2_RESEARCH_REVERSE_SIDECAR")
                    or ""
                ).strip()
            ),
        )
    sql_by_code = (
        "SELECT code_key, provider_set_key, provider_count, price_set_global_id_128 "
        f"FROM {serving_table} "
        "ORDER BY code_key, provider_set_key, price_set_global_id_128"
    )
    candidate = write_serving_by_code_candidate(
        psql_copy_lines(env_overrides, sql_by_code),
        output_dir / "serving_by_code_v1.ptg2sbc",
    )
    reverse_candidate = None
    if bool(str(env_overrides.get("HLTHPRT_PTG2_RESEARCH_REVERSE_SIDECAR") or "").strip()):
        sql_by_provider_set = (
            "SELECT provider_set_key, code_key, provider_count, price_set_global_id_128 "
            f"FROM {serving_table} "
            "ORDER BY provider_set_key, code_key, price_set_global_id_128"
        )
        reverse_candidate = write_serving_by_provider_set_candidate(
            psql_copy_lines(env_overrides, sql_by_provider_set),
            output_dir / "serving_by_provider_set_v1.ptg2sbp",
        )
    total_bytes = int(storage.get("total_bytes") or 0)
    artifact_bytes = int(candidate.get("artifact_bytes") or 0)
    gzip_bytes = int(candidate.get("gzip_bytes") or 0)
    combined_artifact_bytes = artifact_bytes
    combined_gzip_bytes = gzip_bytes
    combined_roundtrip = candidate.get("roundtrip") == "passed"
    if reverse_candidate is not None:
        combined_artifact_bytes += int(reverse_candidate.get("artifact_bytes") or 0)
        combined_gzip_bytes += int(reverse_candidate.get("gzip_bytes") or 0)
        combined_roundtrip = combined_roundtrip and reverse_candidate.get("roundtrip") == "passed"
    combined_candidate = {
        "format": (
            "research_serving_bidirectional_v1"
            if reverse_candidate is not None
            else candidate.get("format")
        ),
        "artifact_bytes": combined_artifact_bytes,
        "gzip_bytes": combined_gzip_bytes,
        "roundtrip": "passed" if combined_roundtrip else "failed",
    }
    status = "passed" if combined_roundtrip else "failed"
    result = {
        "status": status,
        "serving_table": serving_table,
        "storage": storage,
        "candidate": candidate,
        "combined_candidate": combined_candidate,
        "forward_reduction_ratio_vs_pg_total": round(total_bytes / artifact_bytes, 3) if artifact_bytes else None,
        "forward_gzip_reduction_ratio_vs_pg_total": round(total_bytes / gzip_bytes, 3) if gzip_bytes else None,
        "reduction_ratio_vs_pg_total": round(total_bytes / combined_artifact_bytes, 3) if combined_artifact_bytes else None,
        "gzip_reduction_ratio_vs_pg_total": round(total_bytes / combined_gzip_bytes, 3) if combined_gzip_bytes else None,
    }
    if reverse_candidate is not None:
        reverse_artifact_bytes = int(reverse_candidate.get("artifact_bytes") or 0)
        reverse_gzip_bytes = int(reverse_candidate.get("gzip_bytes") or 0)
        result.update(
            {
                "reverse_candidate": reverse_candidate,
                "reverse_reduction_ratio_vs_pg_total": (
                    round(total_bytes / reverse_artifact_bytes, 3) if reverse_artifact_bytes else None
                ),
                "reverse_gzip_reduction_ratio_vs_pg_total": (
                    round(total_bytes / reverse_gzip_bytes, 3) if reverse_gzip_bytes else None
                ),
            }
        )
    if postgres_posting_candidate is not None:
        result["postgres_posting_candidate"] = postgres_posting_candidate
        result["preferred_candidate"] = "postgres_posting"
        if postgres_posting_candidate.get("status") == "failed":
            result["status"] = "failed"
    if postgres_binary_candidate is not None:
        result["postgres_binary_candidate"] = postgres_binary_candidate
        result["preferred_candidate"] = "postgres_binary"
        if postgres_binary_candidate.get("status") == "failed":
            result["status"] = "failed"
    return {
        **result,
    }


def _published_serving_binary_storage_sql(table_name: str) -> str:
    """Build the storage query for a published PostgreSQL serving-binary table."""
    table_literal = sql_literal(table_name)
    return (
        "SELECT json_build_object("
        f"'total_bytes', pg_total_relation_size('{table_literal}'::regclass), "
        f"'heap_bytes', pg_relation_size('{table_literal}'::regclass), "
        f"'index_bytes', pg_indexes_size('{table_literal}'::regclass), "
        f"'rows', (SELECT count(*) FROM {table_name}), "
        f"'payload_bytes', (SELECT COALESCE(sum(octet_length(payload)), 0) FROM {table_name}), "
        f"'raw_payload_bytes', (SELECT COALESCE(sum(NULLIF((to_jsonb(binary_block)->>'raw_payload_bytes')::integer, 0)), 0) FROM {table_name} binary_block), "
        f"'compressed_saved_bytes', (SELECT COALESCE(sum(CASE "
        f"WHEN COALESCE(to_jsonb(binary_block)->>'payload_compression', 'none') <> 'none' "
        f"THEN COALESCE((to_jsonb(binary_block)->>'raw_payload_bytes')::integer, 0) - octet_length(payload) "
        f"ELSE 0 END), 0) FROM {table_name} binary_block), "
        f"'compressed_rows', (SELECT COALESCE(sum(CASE "
        f"WHEN COALESCE(to_jsonb(binary_block)->>'payload_compression', 'none') <> 'none' THEN 1 ELSE 0 END), 0) "
        f"FROM {table_name} binary_block), "
        f"'kind_stats', (SELECT json_object_agg(artifact_kind, json_build_object("
        f"'rows', rows, 'payload_bytes', payload_bytes, "
        f"'raw_payload_bytes', raw_payload_bytes, 'compressed_saved_bytes', compressed_saved_bytes"
        f")) FROM (SELECT artifact_kind, count(*) AS rows, "
        f"COALESCE(sum(octet_length(payload)), 0) AS payload_bytes, "
        f"COALESCE(sum(NULLIF((to_jsonb(binary_block)->>'raw_payload_bytes')::integer, 0)), 0) AS raw_payload_bytes, "
        f"COALESCE(sum(CASE WHEN COALESCE(to_jsonb(binary_block)->>'payload_compression', 'none') <> 'none' "
        f"THEN COALESCE((to_jsonb(binary_block)->>'raw_payload_bytes')::integer, 0) - octet_length(payload) "
        f"ELSE 0 END), 0) AS compressed_saved_bytes "
        f"FROM {table_name} binary_block GROUP BY artifact_kind) stats)"
        ") AS payload;"
    )


def analyze_published_serving_binary_storage(
    *,
    env_overrides: dict[str, str],
    serving_index: dict[str, Any],
) -> dict[str, Any]:
    """Measure the published PostgreSQL binary serving table for tableless snapshots."""
    table_name = _serving_index_table_or_none(serving_index, "serving_binary_table")
    if not table_name:
        return {"status": "skipped", "reason": "missing_serving_binary_table"}
    if not pg_table_exists(env_overrides, table_name):
        return {
            "status": "failed",
            "reason": "serving_binary_table_missing",
            "serving_binary_table": table_name,
        }
    binary_manifest = serving_index.get("serving_binary")
    binary_manifest = binary_manifest if isinstance(binary_manifest, dict) else {}
    storage = psql_json(env_overrides, _published_serving_binary_storage_sql(table_name))
    return {
        "status": "passed",
        "preferred_candidate": "postgres_binary",
        "postgres_binary_snapshot": {
            "layout": "postgres_binary_v1",
            "table": table_name,
            "writer": binary_manifest.get("writer"),
            "row_count": binary_manifest.get("row_count"),
            "timing": binary_manifest.get("timing") if isinstance(binary_manifest.get("timing"), dict) else {},
            "build_elapsed_seconds": binary_manifest.get("build_elapsed_seconds"),
            "storage": storage,
            "manifest_storage": binary_manifest.get("storage") if isinstance(binary_manifest.get("storage"), dict) else {},
        },
    }


def sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


class QuietHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return


class LocalFixtureServer:
    def __init__(self, directory: Path) -> None:
        handler = functools.partial(QuietHTTPRequestHandler, directory=str(directory))
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> "LocalFixtureServer":
        self.thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def run_scanner_fixture(
    *,
    case: dict[str, Any],
    variant: dict[str, Any],
    suite: dict[str, Any],
    output_root: Path,
    dry_run: bool = False,
) -> RunResult:
    case_id = str(case["id"])
    variant_id = str(variant["id"])
    run_dir = output_root / case_id / variant_id
    run_dir.mkdir(parents=True, exist_ok=True)
    env_overrides = env_for_variant(case, variant)
    scanner = resolve_root_path(case.get("scanner_binary") or suite.get("scanner_binary") or DEFAULT_SCANNER)
    artifact = write_fixture(case, run_dir)
    serving_copy = run_dir / "manifest_serving.copy"
    price_atom_copy = run_dir / "price_atom.copy"
    member_copy = run_dir / "provider_group_member.copy"
    scanner_env = {
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "research-snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "research-plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "research-plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "research-source-trace",
        "HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH": str(serving_copy),
        "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH": str(price_atom_copy),
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH": str(member_copy),
        "HLTHPRT_PTG2_MANIFEST_ONLY": "true",
        **env_overrides,
    }
    command = [str(scanner), "--compact-serving", str(artifact)]
    if dry_run:
        return RunResult(
            case_id=case_id,
            variant_id=variant_id,
            kind="scanner_fixture",
            status="dry_run",
            command=command,
            env_overrides=scanner_env,
        )
    if not scanner.exists():
        return RunResult(
            case_id=case_id,
            variant_id=variant_id,
            kind="scanner_fixture",
            status="skipped",
            command=command,
            env_overrides=scanner_env,
            error=f"scanner binary not found: {scanner}",
        )
    completed, elapsed, memory = run_with_sampling(command, scanner_env, cwd=ROOT)
    frames = parse_sized_frames(completed.stdout)
    progress = parse_scanner_progress(completed.stderr)
    copy_outputs = collect_copy_outputs(run_dir)
    status = "succeeded" if completed.returncode == 0 else "failed"
    return RunResult(
        case_id=case_id,
        variant_id=variant_id,
        kind="scanner_fixture",
        status=status,
        command=command,
        env_overrides=scanner_env,
        elapsed_seconds=elapsed,
        returncode=completed.returncode,
        frames=frames,
        progress=progress,
        scanner_config=first_frame_payload(frames, "scanner_config"),
        scanner_summary=first_frame_payload(frames, "scanner_summary"),
        dedupe_summary=first_frame_payload(frames, "dedupe_summary"),
        copy_outputs=copy_outputs,
        memory=memory,
        error=completed.stderr.decode("utf-8", errors="replace") if completed.returncode else None,
    )


def run_with_sampling(command: list[str], env_overrides: dict[str, str], *, cwd: Path) -> tuple[subprocess.CompletedProcess, float, dict[str, Any]]:
    env = {**os.environ, **env_overrides}
    sampler = ProcSampler()
    started = time.monotonic()
    proc = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    while proc.poll() is None:
        sampler.sample(proc.pid)
        time.sleep(float(os.getenv("HLTHPRT_PTG2_RESEARCH_SAMPLE_SECONDS", "0.1")))
    stdout, stderr = proc.communicate()
    elapsed = time.monotonic() - started
    sampler.sample(proc.pid)
    return (
        subprocess.CompletedProcess(command, proc.returncode, stdout=stdout, stderr=stderr),
        elapsed,
        sampler.to_json(),
    )


def collect_copy_outputs(run_dir: Path) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    for label, pattern in {
        "serving": "manifest_serving.copy*",
        "price_atom": "price_atom.copy*",
        "provider_group_member": "provider_group_member.copy*",
    }.items():
        lines = []
        files = []
        for path in sorted(run_dir.glob(pattern)):
            if path.name.endswith(".ready"):
                data_path = path
            else:
                data_path = path
            if not data_path.is_file():
                continue
            files.append(str(path))
            lines.extend(path.read_text(encoding="utf-8", errors="replace").splitlines())
        outputs[label] = {
            "files": files,
            "rows": len(lines),
            "sha256": digest_lines(lines),
        }
    return outputs


def digest_lines(lines: list[str]) -> str:
    digest = hashlib.sha256()
    for line in sorted(lines):
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def run_import_control_pilot(
    *,
    case: dict[str, Any],
    variant: dict[str, Any],
    dry_run: bool = False,
) -> RunResult:
    params = dict(case.get("params") or {})
    params.update(case.get("variant_params") or {})
    params.update(variant.get("params") or {})
    params.update(env_to_scanner_params(env_for_variant(case, variant)))
    payload = {
        "importer": "ptg",
        "params": params,
        "idempotency_key": case.get("idempotency_key") or f"research-{case['id']}-{variant['id']}",
        "triggered_by": "ptg2-research",
    }
    base_url = str(case.get("control_url") or os.getenv("HLTHPRT_CONTROL_URL") or "").rstrip("/")
    token = str(case.get("control_token") or os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    command = ["POST", f"{base_url}/imports", json.dumps(payload, sort_keys=True)]
    if dry_run:
        return RunResult(
            case_id=str(case["id"]),
            variant_id=str(variant["id"]),
            kind="import_control_pilot",
            status="dry_run",
            command=command,
            env_overrides={},
            import_run={"request_payload": payload},
        )
    if not base_url or not token:
        return RunResult(
            case_id=str(case["id"]),
            variant_id=str(variant["id"]),
            kind="import_control_pilot",
            status="skipped",
            command=command,
            env_overrides={},
            import_run={"request_payload": payload},
            error="control_url and HLTHPRT_CONTROL_API_TOKEN are required to execute pilot imports",
        )
    started = time.monotonic()
    try:
        response_payload = post_json(f"{base_url}/imports", payload, token=token)
        status = "succeeded" if str(response_payload.get("status")) not in {"failed", "error"} else "failed"
        error = None
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        response_payload = {}
        status = "failed"
        error = str(exc)
    return RunResult(
        case_id=str(case["id"]),
        variant_id=str(variant["id"]),
        kind="import_control_pilot",
        status=status,
        command=command,
        env_overrides={},
        elapsed_seconds=time.monotonic() - started,
        import_run=response_payload,
        error=error,
    )


def run_import_control_run(
    *,
    case: dict[str, Any],
    variant: dict[str, Any],
    dry_run: bool = False,
) -> RunResult:
    case_id = str(case["id"])
    variant_id = str(variant["id"])
    params = dict(case.get("params") or {})
    params.update(case.get("variant_params") or {})
    params.update(variant.get("params") or {})
    base_url = str(
        case.get("control_url")
        or os.getenv("HLTHPRT_IMPORT_CONTROL_URL")
        or os.getenv("HLTHPRT_CONTROL_URL")
        or ""
    ).rstrip("/")
    path = str(case.get("runs_path") or "/v1/runs")
    if not path.startswith("/"):
        path = f"/{path}"
    url = f"{base_url}{path}"
    importer = str(case.get("importer") or params.pop("importer", "") or "").strip()
    if not importer:
        raise ValueError(f"case {case_id} must set importer for import_control_run")
    idempotency_key = str(
        case.get("idempotency_key")
        or f"research-{case_id}-{variant_id}-{dt.datetime.now(dt.UTC).strftime('%Y%m%d%H%M%S')}"
    )
    payload = {
        "importer": importer,
        "params": params,
        "idempotency_key": idempotency_key,
        "triggered_by": str(case.get("triggered_by") or "import-research"),
    }
    for key in ("run_id", "node_id", "source_file_import_id", "import_id", "schedule_id", "subscription_id"):
        if case.get(key) is not None:
            payload[key] = case[key]
    actor = str(case.get("actor") or os.getenv("HLTHPRT_IMPORT_CONTROL_ACTOR") or "").strip()
    if actor:
        payload["actor"] = actor
    command = ["POST", url, json.dumps(payload, sort_keys=True)]
    if dry_run:
        return RunResult(
            case_id=case_id,
            variant_id=variant_id,
            kind="import_control_run",
            status="dry_run",
            command=command,
            env_overrides={"control_url": base_url, "runs_path": path},
            import_run={"request_payload": payload},
        )

    token = str(
        case.get("control_token")
        or os.getenv("HLTHPRT_IMPORT_CONTROL_API_TOKEN")
        or os.getenv("HLTHPRT_CONTROL_API_TOKEN")
        or ""
    ).strip()
    if not base_url or not token:
        return RunResult(
            case_id=case_id,
            variant_id=variant_id,
            kind="import_control_run",
            status="skipped",
            command=command,
            env_overrides={"control_url": base_url, "runs_path": path},
            import_run={"request_payload": payload},
            error="control_url and HLTHPRT_IMPORT_CONTROL_API_TOKEN are required to execute import-control runs",
        )

    started = time.monotonic()
    progress_samples: list[dict[str, Any]] = []
    try:
        response_payload = post_json(url, payload, token=token)
        run_id = str(response_payload.get("run_id") or "")
        final_run = response_payload
        progress = response_payload.get("progress")
        if isinstance(progress, dict):
            progress_samples.append(progress)
        if bool(case.get("wait_for_terminal", True)):
            final_run, progress_samples = poll_import_control_run(
                base_url=base_url,
                path=path,
                run_id=run_id,
                token=token,
                poll_seconds=float(case.get("poll_seconds") or 10),
                timeout_seconds=float(case.get("timeout_seconds") or 3600),
                progress_samples=progress_samples,
            )
        run_status = str(final_run.get("status") or response_payload.get("status") or "")
        status = "succeeded" if run_status == "succeeded" else "failed" if run_status in TERMINAL_IMPORT_STATUSES else "succeeded"
        error = None if status == "succeeded" else json.dumps(final_run.get("error") or {}, sort_keys=True)
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        response_payload = {}
        final_run = {}
        status = "failed"
        error = str(exc)
    return RunResult(
        case_id=case_id,
        variant_id=variant_id,
        kind="import_control_run",
        status=status,
        command=command,
        env_overrides={"control_url": base_url, "runs_path": path},
        elapsed_seconds=time.monotonic() - started,
        progress=progress_samples,
        import_run={
            "request_payload": payload,
            "response": response_payload,
            "final_run": final_run,
        },
        error=error,
    )


def poll_import_control_run(
    *,
    base_url: str,
    path: str,
    run_id: str,
    token: str,
    poll_seconds: float,
    timeout_seconds: float,
    progress_samples: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not run_id:
        raise ValueError("import-control response did not include run_id")
    samples = list(progress_samples or [])
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, Any] = {}
    while True:
        last_payload = get_json(f"{base_url}{path}/{urllib.parse.quote(run_id, safe='')}", token=token)
        progress = last_payload.get("progress")
        if isinstance(progress, dict):
            samples.append(progress)
        if str(last_payload.get("status") or "") in TERMINAL_IMPORT_STATUSES:
            return last_payload, samples
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for import run {run_id}")
        time.sleep(max(poll_seconds, 0.0))


def run_local_ptg_cli(
    *,
    case: dict[str, Any],
    variant: dict[str, Any],
    suite: dict[str, Any],
    output_root: Path,
    dry_run: bool = False,
) -> RunResult:
    case_id = str(case["id"])
    variant_id = str(variant["id"])
    run_dir = output_root / case_id / variant_id
    fixture_dir = run_dir / "http_fixture"
    artifact_dir = run_dir / "artifacts"
    run_dir.mkdir(parents=True, exist_ok=True)
    env_overrides = {
        "PYTHONPATH": ".",
        "HLTHPRT_LOG_CFG": "logging.yaml",
        "HLTHPRT_DB_HOST": str(case.get("db_host") or os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1"),
        "HLTHPRT_DB_PORT": str(case.get("db_port") or os.getenv("HLTHPRT_DB_PORT") or "5440"),
        "HLTHPRT_DB_USER": str(case.get("db_user") or os.getenv("HLTHPRT_DB_USER") or "nick"),
        "HLTHPRT_DB_DATABASE": str(case.get("db_database") or os.getenv("HLTHPRT_DB_DATABASE") or "healthporta"),
        "HLTHPRT_TEST_DATABASE_SUFFIX": str(
            case.get("test_database_suffix") or os.getenv("HLTHPRT_TEST_DATABASE_SUFFIX") or "_test"
        ),
        "HLTHPRT_DB_SCHEMA": str(case.get("db_schema") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"),
        "HLTHPRT_FETCH_ALLOW_LOCAL": "true",
        "HLTHPRT_PTG2_ARTIFACT_DIR": str(artifact_dir),
        "HLTHPRT_PTG2_TEST_MAX_BYTES": str(case.get("max_bytes") or 5 * 1024 * 1024),
        "HLTHPRT_FETCH_MAX_BYTES": str(case.get("max_bytes") or 5 * 1024 * 1024),
        "HLTHPRT_PTG2_RUST_SCANNER_BIN": str(
            resolve_root_path(case.get("scanner_binary") or suite.get("scanner_binary") or DEFAULT_SCANNER)
        ),
        "HLTHPRT_PTG2_RUST_REQUIRE_RELEASE": "true",
        "HLTHPRT_PTG2_KEEP_PARTIAL_ARTIFACTS": "true",
        "HLTHPRT_PTG2_DOWNLOAD_TASKS": "1",
        "HLTHPRT_PTG2_RANGE_DOWNLOADS": "false",
    }
    if "HLTHPRT_DB_PASSWORD" in os.environ or case.get("db_password") is not None:
        env_overrides["HLTHPRT_DB_PASSWORD"] = str(case.get("db_password") or os.getenv("HLTHPRT_DB_PASSWORD") or "")
    env_overrides.update(env_for_variant(case, variant))

    plan_id = str(case.get("plan_id") or "LOCAL-PTG2-SMOKE")
    plan_market_type = str(case.get("plan_market_type") or "group")
    import_id = str(case.get("import_id") or f"{case_id}-{variant_id}-{dt.datetime.now(dt.UTC).strftime('%Y%m%d%H%M%S')}")
    source_key = str(case.get("source_key") or f"{case_id}-{variant_id}")
    import_month = str(case.get("import_month") or "2026-06")
    command = [
        sys.executable,
        "main.py",
        "start",
        "ptg",
        "--test",
        "--toc-url",
        "http://127.0.0.1:<auto>/index.json",
        "--source-key",
        source_key,
        "--import-id",
        import_id,
        "--import-month",
        import_month,
        "--max-files",
        str(case.get("max_files") or 1),
        "--plan-id",
        plan_id,
        "--plan-market-type",
        plan_market_type,
    ]
    if case.get("full_file") is not True:
        command.extend(["--max-items", str(case.get("max_items") or 5)])
    elif case.get("max_items") is not None:
        raise ValueError("local_ptg_cli full_file=true cannot be combined with max_items")
    if dry_run:
        write_ptg_toc_fixture(case, fixture_dir, base_url="http://127.0.0.1:<auto>")
        return RunResult(
            case_id=case_id,
            variant_id=variant_id,
            kind="local_ptg_cli",
            status="dry_run",
            command=command,
            env_overrides=env_overrides,
            import_run={"fixture_dir": str(fixture_dir), "artifact_dir": str(artifact_dir)},
        )
    scanner = Path(env_overrides["HLTHPRT_PTG2_RUST_SCANNER_BIN"])
    if not scanner.exists():
        return RunResult(
            case_id=case_id,
            variant_id=variant_id,
            kind="local_ptg_cli",
            status="skipped",
            command=command,
            env_overrides=env_overrides,
            error=f"scanner binary not found: {scanner}",
        )
    with LocalFixtureServer(fixture_dir) as server:
        write_ptg_toc_fixture(case, fixture_dir, base_url=server.base_url)
        command = [part if part != "http://127.0.0.1:<auto>/index.json" else f"{server.base_url}/index.json" for part in command]
        completed, elapsed, memory = run_with_sampling(command, env_overrides, cwd=ROOT)
    combined = completed.stdout + b"\n" + completed.stderr
    import_done = parse_import_done(combined)
    serving_summary = parse_serving_only_summary(combined) or {}
    scanner_summary = serving_summary.get("scanner") if isinstance(serving_summary.get("scanner"), dict) else {}
    status = "succeeded" if completed.returncode == 0 and (import_done or {}).get("status") == "validated" else "failed"
    verification = None
    storage_summary_dict = None
    api_latency = None
    serving_index_checks = None
    import_run_id = str((import_done or {}).get("import_run_id") or "")
    serving_index_payload: dict[str, Any] | None = None
    if status == "succeeded" and case.get("verify_original"):
        verification = verify_local_import_against_original(
            env_overrides=env_overrides,
            original_path=fixture_dir / "rates.json.gz",
            import_run_id=import_run_id,
        )
        if verification.get("status") != "passed":
            status = "failed"
    if status == "succeeded" and _is_serving_storage_probe_enabled(case, variant):
        storage_summary_dict = analyze_local_serving_sidecar_candidate(
            env_overrides=env_overrides,
            import_run_id=import_run_id,
            variant_id=variant_id,
            output_dir=run_dir / "serving_sidecar_candidate",
            max_rows=int(variant.get("serving_sidecar_max_rows") or case.get("serving_sidecar_max_rows") or 2_000_000),
        )
        if storage_summary_dict.get("status") == "failed":
            status = "failed"
    expectations = serving_index_expectations_for(case, variant)
    if status == "succeeded" and expectations:
        serving_index_payload = import_run_serving_index(env_overrides=env_overrides, import_run_id=import_run_id)
        serving_index_checks = check_serving_index_expectations(
            env_overrides,
            serving_index_payload,
            expectations,
        )
        if serving_index_checks.get("status") != "passed":
            status = "failed"
    if status == "succeeded" and storage_summary_dict is None:
        if serving_index_payload is None:
            serving_index_payload = import_run_serving_index(env_overrides=env_overrides, import_run_id=import_run_id)
        published_storage = analyze_published_serving_binary_storage(
            env_overrides=env_overrides,
            serving_index=serving_index_payload,
        )
        if published_storage.get("status") == "passed":
            storage_summary_dict = published_storage
    if status == "succeeded":
        if serving_index_payload is None:
            serving_index_payload = import_run_serving_index(env_overrides=env_overrides, import_run_id=import_run_id)
        if serving_index_payload:
            snapshot_footprint = analyze_snapshot_postgres_footprint(
                env_overrides=env_overrides,
                serving_index=serving_index_payload,
                snapshot_id=str((import_done or {}).get("snapshot_id") or ""),
            )
            if storage_summary_dict is None:
                storage_summary_dict = {"status": "passed"}
            storage_summary_dict["snapshot_footprint"] = snapshot_footprint
    if status == "succeeded" and _is_api_latency_probe_enabled(case, variant):
        api_latency = run_api_latency_probe(
            env_overrides=env_overrides,
            snapshot_id=str((import_done or {}).get("snapshot_id") or ""),
            case=case,
            variant=variant,
        )
        if api_latency.get("status") != "passed":
            status = "failed"
    return RunResult(
        case_id=case_id,
        variant_id=variant_id,
        kind="local_ptg_cli",
        status=status,
        command=command,
        env_overrides=env_overrides,
        elapsed_seconds=elapsed,
        returncode=completed.returncode,
        progress=parse_scanner_progress(combined),
        scanner_config=scanner_summary.get("config") if isinstance(scanner_summary, dict) else None,
        scanner_summary=scanner_summary.get("summary") if isinstance(scanner_summary, dict) else None,
        dedupe_summary=parse_dedupe_summary(combined),
        memory=memory,
        import_run={
            "import_done": import_done or {},
            "fixture_dir": str(fixture_dir),
            "artifact_dir": str(artifact_dir),
            "verification": verification or {},
            "storage": storage_summary_dict or {},
            "serving_index_checks": serving_index_checks or {},
            "api_latency": api_latency or {},
        },
        error=completed.stderr.decode("utf-8", errors="replace") if status != "succeeded" else None,
    )


def env_to_scanner_params(env: dict[str, str]) -> dict[str, Any]:
    mapping = {
        "HLTHPRT_PTG2_RUST_WORKERS": "_scanner_rust_workers",
        "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS": "_scanner_parse_in_workers",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "_scanner_work_queue",
        "HLTHPRT_PTG2_RUST_EVENT_QUEUE": "_scanner_event_queue",
    }
    result: dict[str, Any] = {}
    for env_key, param_key in mapping.items():
        if env_key not in env:
            continue
        value: Any = env[env_key]
        if env_key == "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS":
            value = str(value).lower() in {"1", "true", "yes", "on"}
        result[param_key] = value
    return result


def post_json(url: str, payload: dict[str, Any], *, token: str) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def get_json(url: str, *, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def run_suite(
    suite: dict[str, Any],
    *,
    report_dir: Path = DEFAULT_REPORT_DIR,
    case_ids: set[str] | None = None,
    variant_ids: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    variants = variant_map(suite)
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    output_root = report_dir / f"run-{timestamp}"
    output_root.mkdir(parents=True, exist_ok=True)
    results: list[RunResult] = []
    for case in selected_cases(suite, case_ids):
        selected_variant_ids = list(case.get("variants") or variants.keys())
        if variant_ids:
            selected_variant_ids = [variant_id for variant_id in selected_variant_ids if variant_id in variant_ids]
        for variant_id in selected_variant_ids:
            variant = variants[variant_id]
            kind = str(case.get("kind") or "scanner_fixture")
            if kind == "scanner_fixture":
                results.append(
                    run_scanner_fixture(
                        case=case,
                        variant=variant,
                        suite=suite,
                        output_root=output_root,
                        dry_run=dry_run,
                    )
                )
            elif kind == "import_control_pilot":
                results.append(run_import_control_pilot(case=case, variant=variant, dry_run=dry_run))
            elif kind == "import_control_run":
                results.append(run_import_control_run(case=case, variant=variant, dry_run=dry_run))
            elif kind == "local_ptg_cli":
                results.append(
                    run_local_ptg_cli(
                        case=case,
                        variant=variant,
                        suite=suite,
                        output_root=output_root,
                        dry_run=dry_run,
                    )
                )
            else:
                results.append(
                    RunResult(
                        case_id=str(case["id"]),
                        variant_id=str(variant_id),
                        kind=kind,
                        status="failed",
                        command=[],
                        env_overrides={},
                        error=f"unsupported case kind: {kind}",
                    )
                )
    report = {
        "schema_version": 1,
        "generated_at": timestamp,
        "suite": {
            "title": suite.get("title"),
            "description": suite.get("description"),
            "gates": suite.get("gates") or {},
        },
        "results": [result.to_json() for result in results],
    }
    report["gates"] = evaluate_gates(
        report,
        suite.get("gates") or {},
        case_gates={str(case.get("id")): case.get("gates") or {} for case in suite.get("cases") or []},
    )
    write_report(output_root, report)
    return report


def evaluate_gates(
    report: dict[str, Any],
    gates: dict[str, Any],
    *,
    case_gates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    gate_default_dict = gate_default_options(gates)
    by_case: dict[str, list[dict[str, Any]]] = {}
    for result in report.get("results") or []:
        by_case.setdefault(str(result.get("case_id")), []).append(result)
    case_results = {
        case_id: evaluate_case_gate_results(
            case_id=case_id,
            variant_result_dicts=results,
            gate_options=(case_gates or {}).get(case_id) or {},
            gate_default_dict=gate_default_dict,
        )
        for case_id, results in by_case.items()
    }
    return {"overall": gate_results_overall(case_results), "cases": case_results}


def gate_default_options(gates: dict[str, Any]) -> dict[str, Any]:
    """Return suite-level gate defaults with typed numeric values."""
    return {
        "min_improvement_pct": float(gates.get("min_improvement_pct", 15.0)),
        "max_memory_growth_pct": float(gates.get("max_memory_growth_pct", 20.0)),
        "min_storage_ratio": float(gates.get("min_storage_ratio", 0.0)),
        "min_snapshot_total_ratio": float(gates.get("min_snapshot_total_ratio", 0.0)),
        "min_import_total_improvement_pct": optional_float(gates.get("min_import_total_improvement_pct")),
    }


def evaluate_case_gate_results(
    *,
    case_id: str,
    variant_result_dicts: list[dict[str, Any]],
    gate_options: dict[str, Any],
    gate_default_dict: dict[str, Any],
) -> list[dict[str, Any]]:
    """Evaluate all candidate variants for one harness case."""
    baseline_variant = str(gate_options.get("baseline_variant") or "baseline")
    baseline_result_dict = next(
        (
            variant_result_dict
            for variant_result_dict in variant_result_dicts
            if variant_result_dict.get("variant_id") == baseline_variant
        ),
        None,
    )
    if (
        baseline_result_dict is None
        and gate_options.get("storage", True) is not False
        and any(result_storage(variant_result_dict) for variant_result_dict in variant_result_dicts)
    ):
        return evaluate_storage_only_case_results(
            variant_result_dicts,
            gate_options=gate_options,
            gate_default_dict=gate_default_dict,
        )
    candidate_result_dicts = [
        variant_result_dict
        for variant_result_dict in variant_result_dicts
        if variant_result_dict is not baseline_result_dict
    ]
    return evaluate_candidate_case_results(
        baseline_result_dict,
        candidate_result_dicts,
        gate_options=gate_options,
        gate_default_dict=gate_default_dict,
    )


def evaluate_storage_only_case_results(
    variant_result_dicts: list[dict[str, Any]],
    *,
    gate_options: dict[str, Any],
    gate_default_dict: dict[str, Any],
) -> list[dict[str, Any]]:
    """Evaluate storage-only results when a baseline variant is absent."""
    min_storage_ratio = float(gate_options.get("min_storage_ratio", gate_default_dict["min_storage_ratio"]))
    return [
        evaluate_storage_result(variant_result_dict, min_storage_ratio=min_storage_ratio)
        for variant_result_dict in variant_result_dicts
    ]


def evaluate_candidate_case_results(
    baseline_result_dict: dict[str, Any] | None,
    candidate_result_dicts: list[dict[str, Any]],
    *,
    gate_options: dict[str, Any],
    gate_default_dict: dict[str, Any],
) -> list[dict[str, Any]]:
    """Evaluate candidate variants against the selected baseline."""
    return [
        evaluate_candidate(
            baseline_result_dict,
            candidate_result_dict,
            min_improvement_pct=gate_default_dict["min_improvement_pct"],
            max_memory_growth_pct=gate_default_dict["max_memory_growth_pct"],
            min_storage_ratio=float(
                gate_options.get("min_storage_ratio", gate_default_dict["min_storage_ratio"])
            ),
            min_snapshot_total_ratio=float(
                gate_options.get(
                    "min_snapshot_total_ratio",
                    gate_default_dict["min_snapshot_total_ratio"],
                )
            ),
            min_import_total_improvement_pct=optional_float(
                gate_options.get(
                    "min_import_total_improvement_pct",
                    gate_default_dict["min_import_total_improvement_pct"],
                )
            ),
            gate_options=gate_options,
        )
        for candidate_result_dict in candidate_result_dicts
    ]


def gate_results_overall(case_results: dict[str, list[dict[str, Any]]]) -> str:
    """Return the overall gate status for all cases."""
    failed_results = [
        item
        for case_gate_results in case_results.values()
        for item in case_gate_results
        if item.get("overall") == "failed"
    ]
    not_evaluated_results = [
        item
        for case_gate_results in case_results.values()
        for item in case_gate_results
        if item.get("overall") in {"not_evaluated", "unknown"}
    ]
    return "failed" if failed_results else "not_evaluated" if not_evaluated_results else "passed"


def evaluate_candidate(
    baseline: dict[str, Any] | None,
    candidate: dict[str, Any],
    *,
    min_improvement_pct: float,
    max_memory_growth_pct: float,
    min_storage_ratio: float = 0.0,
    min_snapshot_total_ratio: float = 0.0,
    min_import_total_improvement_pct: float | None = None,
    gate_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    if not baseline:
        return {"variant_id": candidate.get("variant_id"), "overall": "unknown", "checks": {"baseline": "missing"}}
    if candidate.get("status") in {"dry_run", "skipped"} or baseline.get("status") in {"dry_run", "skipped"}:
        return {
            "variant_id": candidate.get("variant_id"),
            "overall": "not_evaluated",
            "checks": {
                "status": {
                    "baseline": baseline.get("status"),
                    "candidate": candidate.get("status"),
                }
            },
        }
    checks["status"] = "passed" if candidate.get("status") == baseline.get("status") == "succeeded" else "failed"
    checks["copy_outputs"] = compare_copy_outputs(baseline.get("copy_outputs") or {}, candidate.get("copy_outputs") or {})
    checks["dedupe"] = compare_dedupe(baseline.get("dedupe_summary") or {}, candidate.get("dedupe_summary") or {})
    gate_options = gate_options or {}
    checks["performance"] = candidate_performance_check(
        baseline,
        candidate,
        gate_options=gate_options,
        min_improvement_pct=min_improvement_pct,
    )
    checks["memory"] = candidate_memory_check(
        baseline,
        candidate,
        gate_options=gate_options,
        max_memory_growth_pct=max_memory_growth_pct,
    )
    checks["import_total"] = candidate_import_total_check(
        baseline,
        candidate,
        gate_options=gate_options,
        min_import_total_improvement_pct=min_import_total_improvement_pct,
    )
    checks["storage"] = candidate_storage_check(
        baseline,
        candidate,
        gate_options=gate_options,
        min_storage_ratio=min_storage_ratio,
        min_snapshot_total_ratio=min_snapshot_total_ratio,
    )
    required = required_candidate_gate_statuses(checks)
    return {
        "variant_id": candidate.get("variant_id"),
        "overall": "failed" if "failed" in required else "passed",
        "checks": checks,
    }


def candidate_performance_check(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    gate_options: dict[str, Any],
    min_improvement_pct: float,
) -> dict[str, Any]:
    """Return wall-clock performance gate result for a candidate."""
    if gate_options.get("performance") is False:
        return {"status": "skipped"}
    return compare_elapsed(
        baseline.get("elapsed_seconds"),
        candidate.get("elapsed_seconds"),
        min_improvement_pct=min_improvement_pct,
    )


def candidate_memory_check(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    gate_options: dict[str, Any],
    max_memory_growth_pct: float,
) -> dict[str, Any]:
    """Return peak RSS gate result for a candidate."""
    if gate_options.get("memory") is False:
        return {"status": "skipped"}
    return compare_memory(
        (baseline.get("memory") or {}).get("peak_rss_kb"),
        (candidate.get("memory") or {}).get("peak_rss_kb"),
        max_memory_growth_pct=max_memory_growth_pct,
    )


def candidate_import_total_check(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    gate_options: dict[str, Any],
    min_import_total_improvement_pct: float | None,
) -> dict[str, Any]:
    """Return importer-reported total time gate result for a candidate."""
    if gate_options.get("import_total") is False or min_import_total_improvement_pct is None:
        return {"status": "skipped"}
    return compare_import_total_seconds(
        baseline,
        candidate,
        min_improvement_pct=min_import_total_improvement_pct,
    )


def candidate_storage_check(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    gate_options: dict[str, Any],
    min_storage_ratio: float,
    min_snapshot_total_ratio: float,
) -> dict[str, Any]:
    """Return the storage gate result for a baseline/candidate pair."""
    if gate_options.get("storage") is False or not result_storage(candidate):
        return {"status": "skipped"}
    return compare_candidate_storage(
        result_storage(baseline),
        result_storage(candidate),
        min_storage_ratio=min_storage_ratio,
        min_snapshot_total_ratio=float(gate_options.get("min_snapshot_total_ratio", min_snapshot_total_ratio)),
    )


def required_candidate_gate_statuses(checks: dict[str, Any]) -> list[str]:
    """Return the gate statuses that determine candidate pass/fail."""
    required_statuses = [checks["status"], checks["copy_outputs"]["status"], checks["dedupe"]["status"]]
    optional_gate_names = ("performance", "memory", "import_total", "storage")
    if any((checks.get(name) or {}).get("status") == "failed" for name in optional_gate_names):
        required_statuses.append("failed")
    return required_statuses


def result_storage(result: dict[str, Any]) -> dict[str, Any]:
    import_run = result.get("import_run") if isinstance(result.get("import_run"), dict) else {}
    storage = import_run.get("storage") if isinstance(import_run, dict) else None
    return storage if isinstance(storage, dict) else {}


def evaluate_storage_result(result: dict[str, Any], *, min_storage_ratio: float) -> dict[str, Any]:
    verification = (result.get("import_run") or {}).get("verification") if isinstance(result.get("import_run"), dict) else {}
    verification_status = str((verification or {}).get("status") or "")
    checks = {
        "status": "passed" if result.get("status") == "succeeded" else "failed",
        "verification": "passed" if not verification_status or verification_status == "passed" else "failed",
        "storage": compare_storage(result_storage(result), min_storage_ratio=min_storage_ratio),
    }
    if checks["status"] == "failed" or checks["verification"] == "failed" or checks["storage"]["status"] == "failed":
        overall = "failed"
    elif checks["storage"]["status"] == "not_evaluated":
        overall = "not_evaluated"
    elif checks["storage"]["status"] == "unknown":
        overall = "unknown"
    else:
        overall = "passed"
    return {"variant_id": result.get("variant_id"), "overall": overall, "checks": checks}


def compare_storage(storage: dict[str, Any], *, min_storage_ratio: float) -> dict[str, Any]:
    if not storage:
        return {"status": "unknown", "ratio": None, "required_ratio": min_storage_ratio}
    if storage.get("status") == "skipped":
        return {
            "status": "not_evaluated",
            "reason": storage.get("reason"),
            "ratio": None,
            "required_ratio": min_storage_ratio,
        }
    if storage.get("status") != "passed":
        return {
            "status": "failed",
            "reason": storage.get("status") or "storage_probe_failed",
            "ratio": storage.get("reduction_ratio_vs_pg_total"),
            "required_ratio": min_storage_ratio,
        }
    preferred_candidate = str(storage.get("preferred_candidate") or "")
    if preferred_candidate == "postgres_binary" and isinstance(storage.get("postgres_binary_candidate"), dict):
        postgres_candidate = storage["postgres_binary_candidate"]
        candidate = postgres_candidate.get("candidate") if isinstance(postgres_candidate.get("candidate"), dict) else {}
        ratio = postgres_candidate.get("reduction_ratio_vs_pg_total")
    elif preferred_candidate == "postgres_posting" and isinstance(storage.get("postgres_posting_candidate"), dict):
        postgres_candidate = storage["postgres_posting_candidate"]
        candidate = postgres_candidate.get("candidate") if isinstance(postgres_candidate.get("candidate"), dict) else {}
        ratio = postgres_candidate.get("reduction_ratio_vs_pg_total")
    else:
        candidate = (
            storage.get("combined_candidate")
            if isinstance(storage.get("combined_candidate"), dict)
            else storage.get("candidate")
            if isinstance(storage.get("candidate"), dict)
            else {}
        )
        ratio = storage.get("reduction_ratio_vs_pg_total")
    if candidate.get("roundtrip") != "passed":
        return {
            "status": "failed",
            "reason": "roundtrip_failed",
            "ratio": ratio,
            "required_ratio": min_storage_ratio,
        }
    if ratio is None:
        return {"status": "unknown", "ratio": None, "required_ratio": min_storage_ratio}
    ratio_float = float(ratio)
    return {
        "status": "passed" if ratio_float >= min_storage_ratio else "failed",
        "ratio": round(ratio_float, 3),
        "required_ratio": min_storage_ratio,
        "gzip_ratio": storage.get("gzip_reduction_ratio_vs_pg_total"),
    }


def compare_candidate_storage(
    baseline_storage: dict[str, Any],
    candidate_storage: dict[str, Any],
    *,
    min_storage_ratio: float,
    min_snapshot_total_ratio: float = 0.0,
) -> dict[str, Any]:
    """Compare candidate serving storage against its own or baseline source size."""
    direct_storage_check = compare_storage(candidate_storage, min_storage_ratio=min_storage_ratio)
    baseline_table = baseline_storage.get("storage") if isinstance(baseline_storage.get("storage"), dict) else {}
    baseline_total_bytes = int(baseline_table.get("total_bytes") or 0)
    binary_snapshot = (
        candidate_storage.get("postgres_binary_snapshot")
        if isinstance(candidate_storage.get("postgres_binary_snapshot"), dict)
        else {}
    )
    binary_storage = binary_snapshot.get("storage") if isinstance(binary_snapshot.get("storage"), dict) else {}
    candidate_total_bytes = int(binary_storage.get("total_bytes") or 0)
    if baseline_total_bytes <= 0 or candidate_total_bytes <= 0:
        return with_snapshot_total_storage_check(
            direct_storage_check,
            baseline_storage,
            candidate_storage,
            min_snapshot_total_ratio=min_snapshot_total_ratio,
        )
    serving_storage_ratio = round(baseline_total_bytes / candidate_total_bytes, 3)
    storage_check_dict = {
        "status": "passed" if serving_storage_ratio >= min_storage_ratio else "failed",
        "ratio": serving_storage_ratio,
        "required_ratio": min_storage_ratio,
        "baseline_total_bytes": baseline_total_bytes,
        "candidate_total_bytes": candidate_total_bytes,
        "candidate": "postgres_binary_snapshot",
    }
    return with_snapshot_total_storage_check(
        storage_check_dict,
        baseline_storage,
        candidate_storage,
        min_snapshot_total_ratio=min_snapshot_total_ratio,
    )


def snapshot_total_logical_bytes(storage: dict[str, Any]) -> int:
    """Return full snapshot logical bytes from a storage payload."""
    footprint = storage.get("snapshot_footprint") if isinstance(storage.get("snapshot_footprint"), dict) else {}
    return int(footprint.get("total_logical_bytes") or 0)


def with_snapshot_total_storage_check(
    storage_check: dict[str, Any],
    baseline_storage: dict[str, Any],
    candidate_storage: dict[str, Any],
    *,
    min_snapshot_total_ratio: float,
) -> dict[str, Any]:
    """Attach and enforce a full snapshot footprint ratio gate when requested."""
    if min_snapshot_total_ratio <= 0:
        return storage_check
    baseline_total_bytes = snapshot_total_logical_bytes(baseline_storage)
    candidate_total_bytes = snapshot_total_logical_bytes(candidate_storage)
    storage_check["required_snapshot_total_ratio"] = min_snapshot_total_ratio
    storage_check["snapshot_baseline_total_bytes"] = baseline_total_bytes
    storage_check["snapshot_candidate_total_bytes"] = candidate_total_bytes
    if baseline_total_bytes <= 0 or candidate_total_bytes <= 0:
        storage_check["status"] = "failed"
        storage_check["snapshot_status"] = "failed"
        storage_check["snapshot_reason"] = "missing_snapshot_footprint"
        storage_check["snapshot_ratio"] = None
        return storage_check
    snapshot_ratio = round(baseline_total_bytes / candidate_total_bytes, 3)
    is_snapshot_ratio_passing = snapshot_ratio >= min_snapshot_total_ratio
    storage_check["snapshot_ratio"] = snapshot_ratio
    storage_check["snapshot_status"] = "passed" if is_snapshot_ratio_passing else "failed"
    if not is_snapshot_ratio_passing:
        storage_check["status"] = "failed"
    return storage_check


def compare_copy_outputs(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    labels = sorted(set(baseline) | set(candidate))
    mismatches = []
    for label in labels:
        base = baseline.get(label) or {}
        cand = candidate.get(label) or {}
        if base.get("rows") != cand.get("rows") or base.get("sha256") != cand.get("sha256"):
            mismatches.append(label)
    return {"status": "failed" if mismatches else "passed", "mismatches": mismatches}


def compare_dedupe(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "negotiated_rates",
        "serving_rate_attempted",
        "serving_rate_unique",
        "serving_rate_duplicate",
        "price_atom_attempted",
        "price_atom_unique",
        "price_atom_duplicate",
    ]
    mismatches = [key for key in keys if baseline.get(key) != candidate.get(key)]
    return {"status": "failed" if mismatches else "passed", "mismatches": mismatches}


def compare_elapsed(base: Any, candidate: Any, *, min_improvement_pct: float) -> dict[str, Any]:
    if not base or not candidate:
        return {"status": "unknown", "improvement_pct": None}
    improvement = ((float(base) - float(candidate)) / float(base)) * 100.0
    return {
        "status": "passed" if improvement >= min_improvement_pct else "failed",
        "improvement_pct": round(improvement, 2),
        "required_pct": min_improvement_pct,
    }


def optional_float(value: Any) -> float | None:
    """Return a float for configured numeric gates, preserving unset values."""
    if value in (None, ""):
        return None
    return float(value)


def import_total_seconds(result: dict[str, Any]) -> float | None:
    """Return PTG2_IMPORT_DONE total_seconds from a harness result."""
    import_run_dict = result.get("import_run") if isinstance(result.get("import_run"), dict) else {}
    import_done_dict = (
        import_run_dict.get("import_done")
        if isinstance(import_run_dict.get("import_done"), dict)
        else {}
    )
    return optional_float(import_done_dict.get("total_seconds"))


def compare_import_total_seconds(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    min_improvement_pct: float,
) -> dict[str, Any]:
    """Compare importer-reported total time, excluding harness overhead."""
    baseline_total_seconds = import_total_seconds(baseline)
    candidate_total_seconds = import_total_seconds(candidate)
    if not baseline_total_seconds or not candidate_total_seconds:
        return {
            "status": "failed",
            "reason": "missing_import_total_seconds",
            "improvement_pct": None,
            "required_pct": min_improvement_pct,
        }
    improvement_pct = ((baseline_total_seconds - candidate_total_seconds) / baseline_total_seconds) * 100.0
    return {
        "status": "passed" if improvement_pct >= min_improvement_pct else "failed",
        "improvement_pct": round(improvement_pct, 2),
        "required_pct": min_improvement_pct,
        "baseline_total_seconds": baseline_total_seconds,
        "candidate_total_seconds": candidate_total_seconds,
    }


def compare_memory(base: Any, candidate: Any, *, max_memory_growth_pct: float) -> dict[str, Any]:
    if not base or not candidate:
        return {"status": "unknown", "growth_pct": None}
    growth = ((float(candidate) - float(base)) / float(base)) * 100.0
    return {
        "status": "passed" if growth <= max_memory_growth_pct else "failed",
        "growth_pct": round(growth, 2),
        "max_growth_pct": max_memory_growth_pct,
    }


def write_report(output_root: Path, report: dict[str, Any]) -> None:
    json_path = output_root / "report.json"
    markdown_path = output_root / "report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(render_markdown_report(report), encoding="utf-8")


def render_markdown_report(report: dict[str, Any]) -> str:
    suite = report.get("suite") if isinstance(report.get("suite"), dict) else {}
    title = suite.get("title") or "PTG2 Experiment Report"
    lines = [
        f"# {title}",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- gate_status: `{(report.get('gates') or {}).get('overall')}`",
        "",
        "## Results",
        "",
        "| Case | Variant | Kind | Status | Scanner | Import | Verification | Storage | Serving Arch | Elapsed | Peak RSS |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: |",
    ]
    for result in report.get("results") or []:
        memory = result.get("memory") or {}
        lines.append(
            "| {case} | {variant} | {kind} | {status} | {scanner} | {import_done} | {verification} | {storage} | {serving_arch} | {elapsed} | {rss} |".format(
                case=result.get("case_id"),
                variant=result.get("variant_id"),
                kind=result.get("kind"),
                status=result.get("status"),
                scanner=format_scanner_summary(result),
                import_done=format_import_done(result),
                verification=format_verification(result),
                storage=format_storage_analysis(result),
                serving_arch=format_serving_arch_checks(result),
                elapsed=format_optional_float(result.get("elapsed_seconds")),
                rss=memory.get("peak_rss_kb") or "",
            )
        )
    lines.append("")
    lines.append("## Gates")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(report.get("gates") or {}, indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def format_optional_float(value: Any) -> str:
    if value is None:
        return ""
    return f"{float(value):.3f}"


def format_scanner_summary(result: dict[str, Any]) -> str:
    config = result.get("scanner_config") or {}
    summary = result.get("scanner_summary") or {}
    if not config and not summary:
        return ""
    parts = []
    if "parse_in_workers" in config:
        parts.append(f"parse_workers={str(config.get('parse_in_workers')).lower()}")
    if config.get("worker_count") is not None:
        parts.append(f"workers={config.get('worker_count')}")
    if summary.get("producer_blocked_micros") is not None:
        parts.append(f"producer_blocked_us={summary.get('producer_blocked_micros')}")
    if summary.get("raw_chunk_count") is not None:
        parts.append(f"raw_chunks={summary.get('raw_chunk_count')}")
    if summary.get("raw_chunk_max_bytes") is not None:
        parts.append(f"max_raw_chunk_bytes={summary.get('raw_chunk_max_bytes')}")
    if summary.get("raw_chunk_max_rates") is not None:
        parts.append(f"max_raw_chunk_rates={summary.get('raw_chunk_max_rates')}")
    return "<br>".join(parts)


def format_import_done(report_result: dict[str, Any]) -> str:
    import_run = report_result.get("import_run") or {}
    import_done_payload = import_run.get("import_done") if isinstance(import_run, dict) else None
    if not isinstance(import_done_payload, dict) or not import_done_payload:
        final_run = import_run.get("final_run") if isinstance(import_run, dict) else None
        if not isinstance(final_run, dict) or not final_run:
            return ""
        parts = []
        if final_run.get("status") is not None:
            parts.append(str(final_run.get("status")))
        if final_run.get("phase_detail") is not None:
            parts.append(str(final_run.get("phase_detail")))
        progress = final_run.get("progress") if isinstance(final_run.get("progress"), dict) else {}
        if progress.get("pct") is not None:
            parts.append(f"pct={progress.get('pct')}")
        metrics = final_run.get("metrics") if isinstance(final_run.get("metrics"), dict) else {}
        if metrics.get("rows") is not None:
            parts.append(f"rows={metrics.get('rows')}")
        return "<br>".join(parts)
    parts = []
    if import_done_payload.get("status") is not None:
        parts.append(str(import_done_payload.get("status")))
    if import_done_payload.get("files_processed") is not None:
        parts.append(f"files={import_done_payload.get('files_processed')}")
    if import_done_payload.get("serving_rates") is not None:
        parts.append(f"rates={import_done_payload.get('serving_rates')}")
    api_latency = import_run.get("api_latency") if isinstance(import_run, dict) else None
    if isinstance(api_latency, dict) and api_latency:
        latency_summary = _format_api_latency(api_latency)
        if latency_summary:
            parts.append(latency_summary)
    return "<br>".join(parts)


def _format_api_latency(api_latency: dict[str, Any]) -> str:
    probes_by_name = api_latency.get("probes") if isinstance(api_latency.get("probes"), dict) else {}
    if not probes_by_name:
        return ""
    parts = [f"api={api_latency.get('status') or 'unknown'}"]
    for name in ("code_lookup", "npi_reverse"):
        probe = probes_by_name.get(name) if isinstance(probes_by_name.get(name), dict) else None
        if not probe:
            continue
        parts.append(
            "{name}:p95={p95}ms,max={max_ms}ms,total={total}".format(
                name=name,
                p95=probe.get("p95_ms"),
                max_ms=probe.get("max_ms"),
                total=probe.get("total"),
            )
        )
    return "<br>".join(parts)


def format_serving_arch_checks(report_result: dict[str, Any]) -> str:
    """Render the snapshot architecture invariants that protect the serving path."""
    import_run = report_result.get("import_run") if isinstance(report_result.get("import_run"), dict) else {}
    checks_payload = import_run.get("serving_index_checks") if isinstance(import_run, dict) else None
    checks = checks_payload.get("checks") if isinstance(checks_payload, dict) else None
    if not isinstance(checks, dict) or not checks:
        return ""
    parts = [str(checks_payload.get("status") or "unknown")]
    preferred_names = (
        "serving_row_strategy",
        "serving_table_retained",
        "serving_table_exists",
        "serving_binary_table_exists",
        "serving_binary_writer",
        "serving_binary_kinds",
        "serving_sidecar_artifacts",
    )
    check_names = [check_name for check_name in preferred_names if check_name in checks]
    check_names.extend(sorted(check_name for check_name in checks if check_name not in preferred_names))
    for check_name in check_names:
        check_payload = checks.get(check_name) if isinstance(checks.get(check_name), dict) else {}
        actual = check_payload.get("actual")
        if isinstance(actual, bool):
            rendered = str(actual).lower()
        elif isinstance(actual, list):
            rendered = ",".join(str(actual_value) for actual_value in actual)
        else:
            rendered = str(actual)
        if check_payload.get("passed") is False:
            rendered = f"{rendered} expected {check_payload.get('expected')}"
        parts.append(f"{check_name}={rendered}")
    return "<br>".join(parts)


def format_verification(result: dict[str, Any]) -> str:
    import_run = result.get("import_run") or {}
    verification = import_run.get("verification") if isinstance(import_run, dict) else None
    parts = []
    if isinstance(verification, dict) and verification:
        parts.append(str(verification.get("status") or "unknown"))
        expected = verification.get("expected") if isinstance(verification.get("expected"), dict) else {}
        db_counts = verification.get("db") if isinstance(verification.get("db"), dict) else {}
        if expected.get("unique_price_atoms") is not None and db_counts.get("price_atom_rows") is not None:
            parts.append(f"prices={db_counts.get('price_atom_rows')}/{expected.get('unique_price_atoms')}")
        if expected.get("unique_provider_npis") is not None and db_counts.get("provider_npis") is not None:
            parts.append(f"npis={db_counts.get('provider_npis')}/{expected.get('unique_provider_npis')}")
    api_latency = import_run.get("api_latency") if isinstance(import_run, dict) else None
    if isinstance(api_latency, dict) and api_latency:
        latency_parts = format_api_latency(api_latency)
        if latency_parts:
            parts.append(latency_parts)
    return "<br>".join(parts)


def format_api_latency(api_latency: dict[str, Any]) -> str:
    """Render compact API latency timings for the HTML/Markdown reports."""
    probes = api_latency.get("probes") if isinstance(api_latency.get("probes"), dict) else {}
    if not probes:
        return ""
    parts = [f"api={api_latency.get('status') or 'unknown'}"]
    for name in ("code_lookup", "npi_reverse"):
        probe = probes.get(name) if isinstance(probes.get(name), dict) else None
        if not probe:
            continue
        parts.append(
            "{name}:p95={p95}ms,max={max_ms}ms,total={total}".format(
                name=name,
                p95=probe.get("p95_ms"),
                max_ms=probe.get("max_ms"),
                total=probe.get("total"),
            )
        )
    return "<br>".join(parts)


def format_bytes(value: Any) -> str:
    if value is None:
        return ""
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024.0 or unit == "TiB":
            return f"{int(size)} B" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024.0
    return str(value)


def format_storage_analysis(result: dict[str, Any]) -> str:
    import_run = result.get("import_run") or {}
    storage = import_run.get("storage") if isinstance(import_run, dict) else None
    if not isinstance(storage, dict) or not storage:
        return ""
    if storage.get("status") == "skipped":
        return f"skipped<br>{storage.get('reason')}<br>rows={((storage.get('storage') or {}).get('row_count'))}"
    table = storage.get("storage") if isinstance(storage.get("storage"), dict) else {}
    candidate = storage.get("candidate") if isinstance(storage.get("candidate"), dict) else {}
    reverse_candidate = storage.get("reverse_candidate") if isinstance(storage.get("reverse_candidate"), dict) else {}
    combined_candidate = (
        storage.get("combined_candidate") if isinstance(storage.get("combined_candidate"), dict) else candidate
    )
    postgres_candidate = (
        storage.get("postgres_posting_candidate")
        if isinstance(storage.get("postgres_posting_candidate"), dict)
        else {}
    )
    postgres_storage = (
        postgres_candidate.get("storage") if isinstance(postgres_candidate.get("storage"), dict) else {}
    )
    postgres_binary_candidate = (
        storage.get("postgres_binary_candidate")
        if isinstance(storage.get("postgres_binary_candidate"), dict)
        else {}
    )
    postgres_binary_storage = (
        postgres_binary_candidate.get("storage")
        if isinstance(postgres_binary_candidate.get("storage"), dict)
        else {}
    )
    postgres_binary_snapshot = (
        storage.get("postgres_binary_snapshot")
        if isinstance(storage.get("postgres_binary_snapshot"), dict)
        else {}
    )
    postgres_binary_snapshot_storage = (
        postgres_binary_snapshot.get("storage")
        if isinstance(postgres_binary_snapshot.get("storage"), dict)
        else {}
    )
    snapshot_footprint = (
        storage.get("snapshot_footprint") if isinstance(storage.get("snapshot_footprint"), dict) else {}
    )
    parts = [str(storage.get("status") or "unknown")]
    if table.get("total_bytes") is not None:
        parts.append(f"pg={format_bytes(table.get('total_bytes'))}")
    if candidate.get("artifact_bytes") is not None:
        label = "fwd" if reverse_candidate else "artifact"
        parts.append(f"{label}={format_bytes(candidate.get('artifact_bytes'))}")
    if reverse_candidate.get("artifact_bytes") is not None:
        parts.append(f"rev={format_bytes(reverse_candidate.get('artifact_bytes'))}")
    if reverse_candidate and combined_candidate.get("artifact_bytes") is not None:
        parts.append(f"combined={format_bytes(combined_candidate.get('artifact_bytes'))}")
    if candidate.get("gzip_bytes") is not None:
        label = "fwd_gzip" if reverse_candidate else "gzip"
        parts.append(f"{label}={format_bytes(candidate.get('gzip_bytes'))}")
    if reverse_candidate.get("gzip_bytes") is not None:
        parts.append(f"rev_gzip={format_bytes(reverse_candidate.get('gzip_bytes'))}")
    if reverse_candidate and combined_candidate.get("gzip_bytes") is not None:
        parts.append(f"combined_gzip={format_bytes(combined_candidate.get('gzip_bytes'))}")
    if storage.get("forward_reduction_ratio_vs_pg_total") is not None:
        parts.append(f"fwd_ratio={storage.get('forward_reduction_ratio_vs_pg_total')}x")
    if storage.get("reverse_reduction_ratio_vs_pg_total") is not None:
        parts.append(f"rev_ratio={storage.get('reverse_reduction_ratio_vs_pg_total')}x")
    if storage.get("reduction_ratio_vs_pg_total") is not None:
        parts.append(f"ratio={storage.get('reduction_ratio_vs_pg_total')}x")
    if storage.get("gzip_reduction_ratio_vs_pg_total") is not None:
        parts.append(f"gzip_ratio={storage.get('gzip_reduction_ratio_vs_pg_total')}x")
    if postgres_storage.get("candidate_total_bytes") is not None:
        parts.append(f"pg_posting={format_bytes(postgres_storage.get('candidate_total_bytes'))}")
    if postgres_candidate.get("reduction_ratio_vs_pg_total") is not None:
        parts.append(f"pg_posting_ratio={postgres_candidate.get('reduction_ratio_vs_pg_total')}x")
    if postgres_candidate.get("build_elapsed_seconds") is not None:
        parts.append(f"pg_posting_build={postgres_candidate.get('build_elapsed_seconds')}s")
    benchmarks = postgres_candidate.get("benchmarks") if isinstance(postgres_candidate.get("benchmarks"), dict) else {}
    if benchmarks:
        timing_parts = []
        for name in ("code_lookup", "provider_overlap", "code_provider_overlap", "price_overlap"):
            item = benchmarks.get(name) if isinstance(benchmarks.get(name), dict) else {}
            if item.get("execution_ms") is not None:
                timing_parts.append(f"{name}={item.get('execution_ms')}ms")
        if timing_parts:
            parts.append("pg_posting_timings=" + ",".join(timing_parts))
    if postgres_binary_storage.get("artifact_total_bytes") is not None:
        parts.append(f"pg_binary={format_bytes(postgres_binary_storage.get('artifact_total_bytes'))}")
    if postgres_binary_storage.get("artifact_payload_bytes") is not None:
        parts.append(f"pg_binary_payload={format_bytes(postgres_binary_storage.get('artifact_payload_bytes'))}")
    if postgres_binary_snapshot_storage.get("total_bytes") is not None:
        parts.append(f"pg_binary_snapshot={format_bytes(postgres_binary_snapshot_storage.get('total_bytes'))}")
    if postgres_binary_snapshot_storage.get("payload_bytes") is not None:
        parts.append(f"pg_binary_snapshot_payload={format_bytes(postgres_binary_snapshot_storage.get('payload_bytes'))}")
    if postgres_binary_snapshot_storage.get("raw_payload_bytes") is not None:
        parts.append(f"pg_binary_snapshot_raw={format_bytes(postgres_binary_snapshot_storage.get('raw_payload_bytes'))}")
    if postgres_binary_snapshot_storage.get("compressed_saved_bytes") is not None:
        parts.append(f"pg_binary_snapshot_saved={format_bytes(postgres_binary_snapshot_storage.get('compressed_saved_bytes'))}")
    if postgres_binary_snapshot.get("writer"):
        parts.append(f"pg_binary_writer={postgres_binary_snapshot.get('writer')}")
    if postgres_binary_snapshot.get("build_elapsed_seconds") is not None:
        parts.append(f"pg_binary_snapshot_build={postgres_binary_snapshot.get('build_elapsed_seconds')}s")
    if postgres_binary_candidate.get("reduction_ratio_vs_pg_total") is not None:
        parts.append(f"pg_binary_ratio={postgres_binary_candidate.get('reduction_ratio_vs_pg_total')}x")
    if postgres_binary_candidate.get("build_elapsed_seconds") is not None:
        parts.append(f"pg_binary_build={postgres_binary_candidate.get('build_elapsed_seconds')}s")
    binary_benchmarks = (
        postgres_binary_candidate.get("benchmarks")
        if isinstance(postgres_binary_candidate.get("benchmarks"), dict)
        else {}
    )
    if binary_benchmarks:
        timing_parts = []
        for name in ("by_code_fetch", "by_provider_set_fetch"):
            item = binary_benchmarks.get(name) if isinstance(binary_benchmarks.get(name), dict) else {}
            if item.get("execution_ms") is not None:
                timing_parts.append(f"{name}={item.get('execution_ms')}ms")
        if timing_parts:
            parts.append("pg_binary_timings=" + ",".join(timing_parts))
    if snapshot_footprint.get("total_logical_bytes") is not None:
        parts.append(f"snapshot_total={format_bytes(snapshot_footprint.get('total_logical_bytes'))}")
    if snapshot_footprint.get("table_total_bytes") is not None:
        parts.append(f"snapshot_tables={format_bytes(snapshot_footprint.get('table_total_bytes'))}")
    if snapshot_footprint.get("artifact_stored_payload_bytes") is not None:
        parts.append(f"snapshot_artifacts={format_bytes(snapshot_footprint.get('artifact_stored_payload_bytes'))}")
    if snapshot_footprint.get("artifact_tuple_bytes") is not None:
        parts.append(f"snapshot_artifact_tuples={format_bytes(snapshot_footprint.get('artifact_tuple_bytes'))}")
    top_components = (
        snapshot_footprint.get("top_components")
        if isinstance(snapshot_footprint.get("top_components"), list)
        else []
    )
    if top_components:
        rendered_components = []
        for component in top_components[:3]:
            name = str(component.get("name") or component.get("table") or "unknown")
            rendered_components.append(f"{name}={format_bytes(component.get('bytes'))}")
        parts.append("snapshot_top=" + ",".join(rendered_components))
    if combined_candidate.get("roundtrip") is not None:
        parts.append(f"roundtrip={combined_candidate.get('roundtrip')}")
    return "<br>".join(parts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run PTG2 research-branch experiments.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    list_parser = subparsers.add_parser("list", help="List cases and variants.")
    list_parser.add_argument("--suite", default=str(DEFAULT_SUITE_PATH))
    run_parser = subparsers.add_parser("run", help="Run the benchmark suite.")
    run_parser.add_argument("--suite", default=str(DEFAULT_SUITE_PATH))
    run_parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    run_parser.add_argument("--case", action="append", dest="cases", default=[])
    run_parser.add_argument("--variant", action="append", dest="variants", default=[])
    run_parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    suite = load_suite(args.suite)
    if args.command == "list":
        print(json.dumps({"cases": suite["cases"], "variants": suite["variants"]}, indent=2, sort_keys=True))
        return 0
    if args.command == "run":
        report = run_suite(
            suite,
            report_dir=Path(args.report_dir),
            case_ids=set(args.cases) or None,
            variant_ids=set(args.variants) or None,
            dry_run=bool(args.dry_run),
        )
        print(json.dumps({"gate_status": report["gates"]["overall"], "results": len(report["results"])}, sort_keys=True))
        return 0 if report["gates"]["overall"] != "failed" else 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
