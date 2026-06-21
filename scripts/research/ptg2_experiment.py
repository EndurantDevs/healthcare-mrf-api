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
import datetime as dt
import functools
import gzip
import hashlib
import http.server
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
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
        additional_information = None
        if int(case.get("additional_information_bytes") or 0) > 0:
            additional_information = "x" * int(case.get("additional_information_bytes") or 0)
        negotiated_rates = [
            {
                "provider_references": [7],
                "negotiated_prices": [
                    {
                        "negotiated_type": "negotiated",
                        "negotiated_rate": 100 + index,
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
            for index in range(rate_count)
        ]
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
                    "name": "Fixture office visit",
                    "negotiated_rates": negotiated_rates,
                }
            ],
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
    used_npis: set[str] = set()
    negotiated_rate_count = 0
    for item in in_network_items:
        for rate in item.get("negotiated_rates") or []:
            negotiated_rate_count += 1
            for ref_id in rate.get("provider_references") or []:
                try:
                    used_npis.update(provider_npis_by_ref.get(int(ref_id), set()))
                except (TypeError, ValueError):
                    continue
            for price in rate.get("negotiated_prices") or []:
                price_keys.append(price_atom_original_key(price))

    unique_price_keys = sorted(set(price_keys))
    return {
        "provider_references": len(provider_npis_by_ref),
        "in_network_items": len(in_network_items),
        "negotiated_rates": negotiated_rate_count,
        "negotiated_prices": len(price_keys),
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


def validate_qualified_table_name(value: str) -> str:
    if not QUALIFIED_TABLE_RE.fullmatch(value or ""):
        raise ValueError(f"unsafe or invalid qualified table name: {value!r}")
    return value


def validate_identifier(value: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value or ""):
        raise ValueError(f"unsafe or invalid identifier: {value!r}")
    return value


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
    serving_table = validate_qualified_table_name(str(serving_index.get("table") or ""))
    price_atom_table = validate_qualified_table_name(str(serving_index.get("price_atom_table") or ""))
    provider_group_member_table = validate_qualified_table_name(str(serving_index.get("provider_group_member_table") or ""))
    db_counts = psql_json(
        env_overrides,
        "SELECT json_build_object("
        f"'serving_rows', (SELECT count(*) FROM {serving_table}), "
        f"'price_atom_rows', (SELECT count(*) FROM {price_atom_table}), "
        f"'provider_group_member_rows', (SELECT count(*) FROM {provider_group_member_table}), "
        f"'provider_npis', (SELECT count(DISTINCT npi) FROM {provider_group_member_table}), "
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
        ") AS payload;",
    )
    checks = {
        "run_status": run_payload.get("status") == "validated",
        "serving_rows": int(db_counts.get("serving_rows") or 0) == expected["unique_price_atoms"],
        "price_atom_rows": int(db_counts.get("price_atom_rows") or 0) == expected["unique_price_atoms"],
        "provider_group_member_rows": int(db_counts.get("provider_group_member_rows") or 0)
        == expected["unique_provider_npis"],
        "provider_npis": int(db_counts.get("provider_npis") or 0) == expected["unique_provider_npis"],
        "price_atom_digest": db_counts.get("price_atom_digest") == expected["price_atom_digest"],
        "report_serving_rates": int(report.get("serving_rates") or 0) == expected["unique_price_atoms"],
        "report_files_processed": int(report.get("files_processed") or 0) == 1,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "status": "failed" if failed else "passed",
        "failed": failed,
        "expected": expected,
        "db": db_counts,
        "tables": {
            "serving": serving_table,
            "price_atom": price_atom_table,
            "provider_group_member": provider_group_member_table,
        },
        "checks": checks,
    }


def sql_literal(value: str) -> str:
    return str(value).replace("'", "''")



class QuietHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib method name
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
    scanner = Path(case.get("scanner_binary") or suite.get("scanner_binary") or DEFAULT_SCANNER)
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
        "HLTHPRT_PTG2_RUST_SCANNER_BIN": str(case.get("scanner_binary") or suite.get("scanner_binary") or DEFAULT_SCANNER),
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
    if status == "succeeded" and case.get("verify_original"):
        verification = verify_local_import_against_original(
            env_overrides=env_overrides,
            original_path=fixture_dir / "rates.json.gz",
            import_run_id=str((import_done or {}).get("import_run_id") or ""),
        )
        if verification.get("status") != "passed":
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
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - operator-controlled URL
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
    min_improvement_pct = float(gates.get("min_improvement_pct", 15.0))
    max_memory_growth_pct = float(gates.get("max_memory_growth_pct", 20.0))
    by_case: dict[str, list[dict[str, Any]]] = {}
    for result in report.get("results") or []:
        by_case.setdefault(str(result.get("case_id")), []).append(result)
    case_results = {}
    for case_id, results in by_case.items():
        gate_options = (case_gates or {}).get(case_id) or {}
        baseline_variant = str(gate_options.get("baseline_variant") or "baseline")
        baseline = next((item for item in results if item.get("variant_id") == baseline_variant), None)
        candidates = [item for item in results if item is not baseline]
        case_results[case_id] = [
            evaluate_candidate(
                baseline,
                candidate,
                min_improvement_pct=min_improvement_pct,
                max_memory_growth_pct=max_memory_growth_pct,
                gate_options=gate_options,
            )
            for candidate in candidates
        ]
    failed = [
        item
        for results in case_results.values()
        for item in results
        if item.get("overall") == "failed"
    ]
    not_evaluated = [
        item
        for results in case_results.values()
        for item in results
        if item.get("overall") in {"not_evaluated", "unknown"}
    ]
    overall = "failed" if failed else "not_evaluated" if not_evaluated else "passed"
    return {"overall": overall, "cases": case_results}


def evaluate_candidate(
    baseline: dict[str, Any] | None,
    candidate: dict[str, Any],
    *,
    min_improvement_pct: float,
    max_memory_growth_pct: float,
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
    if gate_options.get("performance") is False:
        checks["performance"] = {"status": "skipped"}
    else:
        checks["performance"] = compare_elapsed(
            baseline.get("elapsed_seconds"),
            candidate.get("elapsed_seconds"),
            min_improvement_pct=min_improvement_pct,
        )
    if gate_options.get("memory") is False:
        checks["memory"] = {"status": "skipped"}
    else:
        checks["memory"] = compare_memory(
            (baseline.get("memory") or {}).get("peak_rss_kb"),
            (candidate.get("memory") or {}).get("peak_rss_kb"),
            max_memory_growth_pct=max_memory_growth_pct,
        )
    required = [checks["status"], checks["copy_outputs"]["status"], checks["dedupe"]["status"]]
    if checks["performance"]["status"] == "failed" or checks["memory"]["status"] == "failed":
        required.append("failed")
    return {
        "variant_id": candidate.get("variant_id"),
        "overall": "failed" if "failed" in required else "passed",
        "checks": checks,
    }


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
    lines = [
        "# PTG2 Experiment Report",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- gate_status: `{(report.get('gates') or {}).get('overall')}`",
        "",
        "## Results",
        "",
        "| Case | Variant | Kind | Status | Scanner | Import | Verification | Elapsed | Peak RSS |",
        "| --- | --- | --- | --- | --- | --- | --- | ---: | ---: |",
    ]
    for result in report.get("results") or []:
        memory = result.get("memory") or {}
        lines.append(
            "| {case} | {variant} | {kind} | {status} | {scanner} | {import_done} | {verification} | {elapsed} | {rss} |".format(
                case=result.get("case_id"),
                variant=result.get("variant_id"),
                kind=result.get("kind"),
                status=result.get("status"),
                scanner=format_scanner_summary(result),
                import_done=format_import_done(result),
                verification=format_verification(result),
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


def format_import_done(result: dict[str, Any]) -> str:
    import_run = result.get("import_run") or {}
    done = import_run.get("import_done") if isinstance(import_run, dict) else None
    if not isinstance(done, dict) or not done:
        return ""
    parts = []
    if done.get("status") is not None:
        parts.append(str(done.get("status")))
    if done.get("files_processed") is not None:
        parts.append(f"files={done.get('files_processed')}")
    if done.get("serving_rates") is not None:
        parts.append(f"rates={done.get('serving_rates')}")
    return "<br>".join(parts)


def format_verification(result: dict[str, Any]) -> str:
    import_run = result.get("import_run") or {}
    verification = import_run.get("verification") if isinstance(import_run, dict) else None
    if not isinstance(verification, dict) or not verification:
        return ""
    parts = [str(verification.get("status") or "unknown")]
    expected = verification.get("expected") if isinstance(verification.get("expected"), dict) else {}
    db_counts = verification.get("db") if isinstance(verification.get("db"), dict) else {}
    if expected.get("unique_price_atoms") is not None and db_counts.get("price_atom_rows") is not None:
        parts.append(f"prices={db_counts.get('price_atom_rows')}/{expected.get('unique_price_atoms')}")
    if expected.get("unique_provider_npis") is not None and db_counts.get("provider_npis") is not None:
        parts.append(f"npis={db_counts.get('provider_npis')}/{expected.get('unique_provider_npis')}")
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
