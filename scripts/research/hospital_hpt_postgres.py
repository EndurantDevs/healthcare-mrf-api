# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL schema bakeoff for synthetic hospital-price facts."""

from __future__ import annotations

import csv
import hashlib
import json
import platform
import re
import statistics
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scripts.research.hospital_hpt_codecs import READERS
from scripts.research.hospital_hpt_corpus import Hospital, PriceFact
from scripts.research.hospital_hpt_postgres_sql import (
    CANDIDATES,
    FACT_COLUMNS,
    analyze_sql,
    copy_sql,
    create_sql,
    index_sql,
    materialize_sql,
    publish_sql,
)

SAFE_SCHEMA = re.compile(r"^hpt_bench_(?:typed|dictionary|blocks)_[a-z0-9_]+$")
DISPOSABLE_DATABASE = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)


def _psql(dsn: str, sql: str, *, tuples: bool = False) -> str:
    command_args = ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", dsn]
    if tuples:
        command_args.extend(("-A", "-t"))
    completed = subprocess.run(
        command_args, input=sql, text=True, capture_output=True, check=False
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or "psql failed")
    return completed.stdout.strip()


def _validated_database(dsn: str) -> str:
    database = _psql(dsn, "SELECT current_database();", tuples=True)
    if not DISPOSABLE_DATABASE.search(database):
        raise ValueError("benchmark DSN must target an explicit disposable test database")
    return database


def _schema(candidate: str, trial: int) -> str:
    schema = f"hpt_bench_{candidate}_{trial:03d}"
    if candidate not in CANDIDATES or not SAFE_SCHEMA.fullmatch(schema):
        raise ValueError("unsafe benchmark schema")
    return schema


def _write_tsv(path: Path, headers: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)


def write_canonical_tsv(
    output_dir: Path, hospitals: list[Hospital],
    facts: list[PriceFact],
    source_by_hospital: dict[str, tuple[str, str]],
) -> dict[str, Path]:
    """Write COPY-ready canonical facts and identifier provenance."""
    output_dir.mkdir(parents=True, exist_ok=True)
    table_names = (
        "facility_anchor", "hospital_registry", "hospital_mrf_metadata", "hospital_contract_provision",
        "hospital_npi", "hospital_tax_identity", "price_fact",
    )
    path_by_table = {name: output_dir / f"{name}.tsv" for name in table_names}
    _write_tsv(path_by_table["facility_anchor"], ("id", "name"),
               [(hospital.hospital_id, hospital.name) for hospital in hospitals])
    _write_tsv(path_by_table["hospital_registry"], ("hospital_id", "facility_anchor_id"),
               [(hospital.hospital_id, hospital.hospital_id) for hospital in hospitals])
    _write_tsv(
        path_by_table["hospital_mrf_metadata"],
        ("hospital_id", "financial_aid_policy", "source_sha256"),
        [(
            hospital.hospital_id, hospital.financial_aid_policy,
            source_by_hospital[hospital.hospital_id][0],
        ) for hospital in hospitals],
    )
    _write_tsv(
        path_by_table["hospital_contract_provision"],
        (
            "hospital_id", "provision_ordinal", "payer_name", "plan_name",
            "provisions", "source_sha256",
        ),
        [(
            hospital.hospital_id, ordinal, payer_name, plan_name,
            provisions, source_by_hospital[hospital.hospital_id][0],
        )
            for hospital in hospitals
            for ordinal, (payer_name, plan_name, provisions) in enumerate(
                hospital.contract_provisions
            )],
    )
    _write_tsv(
        path_by_table["hospital_npi"],
        ("hospital_id", "npi", "source_sha256", "source_ordinal"),
        [(
            hospital.hospital_id, npi,
            source_by_hospital[hospital.hospital_id][0], ordinal,
        )
            for hospital in hospitals
            for ordinal, npi in enumerate(hospital.npis)],
    )
    _write_tsv(
        path_by_table["hospital_tax_identity"],
        ("hospital_id", "ein", "source_sha256", "source_filename"),
        [(
            hospital.hospital_id, hospital.ein,
            *source_by_hospital[hospital.hospital_id],
        ) for hospital in hospitals],
    )
    _write_tsv(path_by_table["price_fact"], FACT_COLUMNS,
               [tuple(asdict(fact).values()) for fact in facts])
    return path_by_table


def load_manifest_format(
    manifest_path: Path, format_name: str = "json"
) -> tuple[list[Hospital], list[PriceFact], dict[str, tuple[str, str]], dict[str, Any]]:
    """Read one encoded corpus format and retain exact file provenance."""
    if format_name not in READERS:
        raise ValueError(f"unsupported corpus format: {format_name}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    hospitals: list[Hospital] = []
    facts: list[PriceFact] = []
    source_by_hospital = {}
    for entry in manifest["entries"]:
        path = manifest_path.parent / entry[format_name]
        hospital, hospital_facts = READERS[format_name](path, entry["hospital_id"])
        hospitals.append(hospital)
        facts.extend(hospital_facts)
        source_by_hospital[hospital.hospital_id] = (
            hashlib.sha256(path.read_bytes()).hexdigest(),
            path.name,
        )
    return hospitals, facts, source_by_hospital, manifest


def _timed_sql(dsn: str, sql: str) -> float:
    started = time.perf_counter()
    _psql(dsn, sql)
    return time.perf_counter() - started


def _storage(dsn: str, schema: str) -> dict[str, int]:
    sql = f"""SELECT json_build_object(
'heap_bytes', COALESCE(sum(pg_relation_size(c.oid)),0),
'index_bytes', COALESCE(sum(pg_indexes_size(c.oid)),0),
'toast_bytes', COALESCE(sum(pg_total_relation_size(c.oid)-pg_relation_size(c.oid)-pg_indexes_size(c.oid)),0),
'total_bytes', COALESCE(sum(pg_total_relation_size(c.oid)),0))
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='{schema}' AND c.relkind IN ('r','m');"""
    return {key: int(value) for key, value in json.loads(_psql(dsn, sql, tuples=True)).items()}


def _probe_sql(schema: str) -> dict[str, str]:
    base = f"SELECT * FROM {schema}.published_price"
    order = " ORDER BY " + ", ".join(FACT_COLUMNS)
    comparable = (
        "COALESCE(negotiated_dollar, median_amount, gross_amount, "
        "discounted_cash)"
    )
    return {
        "hospital_code": base + " WHERE hospital_id='hospital-00001' AND code_system='MS-DRG' AND code='10000'" + order,
        "payer_plan": base + " WHERE payer_name='Synthetic Payer 001' AND plan_name='Synthetic Plan 001' AND code='10000'" + order,
        "details": base + " WHERE hospital_id='hospital-00001' AND code='10000'" + order,
        "comparison": base + f" WHERE code='10000' ORDER BY {comparable}, hospital_id LIMIT 100",
        "stats": f"SELECT hospital_id, count(*) AS facts, count(DISTINCT (code_system,code)) AS codes, count(DISTINCT (payer_name,plan_name)) AS plans, min({comparable}), max({comparable}) FROM {schema}.published_price WHERE hospital_id='hospital-00001' GROUP BY hospital_id",
        "pagination": base + " WHERE hospital_id='hospital-00001'" + order + " OFFSET 25 LIMIT 25",
        "zero_match": base + " WHERE code='DOES-NOT-EXIST'" + order,
        "high_fanout": base + " WHERE payer_name='Synthetic Payer 001'" + order + " LIMIT 100",
        "ein_exact": base + f" JOIN {schema}.hospital_tax_identity t USING (hospital_id) WHERE t.ein='100000001' AND code='10000'" + order,
    }


def _query_digest(dsn: str, sql: str) -> str:
    output = _psql(dsn, sql + ";", tuples=True)
    return hashlib.sha256(output.encode()).hexdigest()


def _semantic_table_digest(dsn: str, schema: str) -> str:
    columns = ", ".join(FACT_COLUMNS)
    digest = hashlib.sha256()
    queries = (
        ("facts", f"SELECT {columns} FROM {schema}.published_price ORDER BY {columns}"),
        (
            "metadata",
            f"SELECT hospital_id, financial_aid_policy, source_sha256 "
            f"FROM {schema}.hospital_mrf_metadata ORDER BY hospital_id",
        ),
        (
            "contracts",
            f"SELECT hospital_id, provision_ordinal, payer_name, plan_name, "
            f"provisions, source_sha256 FROM {schema}.hospital_contract_provision "
            f"ORDER BY hospital_id, provision_ordinal",
        ),
    )
    for label, query in queries:
        digest.update(label.encode())
        digest.update(b"\n")
        process = subprocess.Popen(
            [
                "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", dsn,
                "-c", f"COPY ({query}) TO STDOUT WITH (FORMAT csv)",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        for chunk in iter(lambda: process.stdout.read(1024 * 1024), b""):
            digest.update(chunk)
        stderr = process.stderr.read() if process.stderr is not None else b""
        if process.wait() != 0:
            raise RuntimeError(stderr.decode(errors="replace").strip() or "psql failed")
    return digest.hexdigest()


def _wal_lsn(dsn: str) -> str:
    return _psql(dsn, "SELECT pg_current_wal_insert_lsn();", tuples=True)


def _wal_bytes(dsn: str, before: str, after: str) -> int:
    return int(
        float(
            _psql(
                dsn,
                f"SELECT pg_wal_lsn_diff('{after}'::pg_lsn, '{before}'::pg_lsn);",
                tuples=True,
            )
        )
    )


def _explain_ms(dsn: str, sql: str) -> float:
    plan = json.loads(_psql(dsn, f"EXPLAIN (ANALYZE, FORMAT JSON) {sql};", tuples=True))
    return float(plan[0]["Execution Time"])


def _percentiles(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    pick = lambda fraction: ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]
    return {"p50_ms": statistics.median(ordered), "p95_ms": pick(0.95), "p99_ms": pick(0.99)}


def run_candidate(
    dsn: str,
    candidate: str,
    trial: int,
    paths: dict[str, Path],
    expected_facts: int,
    query_iterations: int,
) -> dict[str, Any]:
    """Load, validate, probe, and remove one exact candidate schema."""
    schema = _schema(candidate, trial)
    phase_seconds_by_name: dict[str, float] = {}
    wal_before = _wal_lsn(dsn)
    try:
        phase_seconds_by_name["create_seconds"] = _timed_sql(dsn, create_sql(candidate, schema))
        phase_seconds_by_name["copy_seconds"] = _timed_sql(
            dsn, copy_sql(schema, paths)
        )
        phase_seconds_by_name["materialize_seconds"] = _timed_sql(
            dsn, materialize_sql(candidate, schema)
        )
        phase_seconds_by_name["index_seconds"] = _timed_sql(
            dsn, index_sql(candidate, schema)
        )
        phase_seconds_by_name["analyze_seconds"] = _timed_sql(
            dsn, analyze_sql(candidate, schema)
        )
        phase_seconds_by_name["publish_seconds"] = _timed_sql(
            dsn, publish_sql(candidate, schema)
        )
        validation_started = time.perf_counter()
        count = int(_psql(dsn, f"SELECT count(*) FROM {schema}.published_price;", tuples=True))
        if count != expected_facts:
            raise RuntimeError(f"candidate fact count {count} != {expected_facts}")
        semantic_sha256 = _semantic_table_digest(dsn, schema)
        phase_seconds_by_name["validation_seconds"] = time.perf_counter() - validation_started
        wal_after = _wal_lsn(dsn)
        probe_metrics_by_name = {}
        for name, sql in _probe_sql(schema).items():
            cold = _explain_ms(dsn, sql)
            warm_samples = [
                _explain_ms(dsn, sql) for _ in range(query_iterations)
            ]
            probe_metrics_by_name[name] = {
                "cold_ms": cold,
                **_percentiles(warm_samples),
                "digest": _query_digest(dsn, sql),
            }
        phase_seconds_by_name["total_seconds"] = sum(phase_seconds_by_name.values())
        return {
            "candidate": candidate,
            "trial": trial,
            "phases": phase_seconds_by_name,
            "storage": _storage(dsn, schema),
            "wal_bytes": _wal_bytes(dsn, wal_before, wal_after),
            "semantic_sha256": semantic_sha256,
            "probes": probe_metrics_by_name,
        }
    finally:
        if SAFE_SCHEMA.fullmatch(schema):
            _psql(dsn, f"DROP SCHEMA IF EXISTS {schema} CASCADE;")


def _summarize(trials: list[dict[str, Any]]) -> dict[str, Any]:
    summary_by_candidate = {}
    for candidate in CANDIDATES:
        selected_trials = [
            trial for trial in trials if trial["candidate"] == candidate
        ]
        query_latency_by_probe = {
            name: {
                percentile: statistics.median(
                    trial["probes"][name][percentile]
                    for trial in selected_trials
                )
                for percentile in ("p50_ms", "p95_ms", "p99_ms")
            }
            for name in selected_trials[0]["probes"]
        }
        summary_by_candidate[candidate] = {
            "median_import_seconds": statistics.median(
                trial["phases"]["total_seconds"] for trial in selected_trials
            ),
            "median_storage_bytes": int(
                statistics.median(
                    trial["storage"]["total_bytes"] for trial in selected_trials
                )
            ),
            "median_heap_bytes": int(
                statistics.median(
                    trial["storage"]["heap_bytes"] for trial in selected_trials
                )
            ),
            "median_index_bytes": int(
                statistics.median(
                    trial["storage"]["index_bytes"] for trial in selected_trials
                )
            ),
            "median_toast_bytes": int(
                statistics.median(
                    trial["storage"]["toast_bytes"] for trial in selected_trials
                )
            ),
            "median_wal_bytes": int(
                statistics.median(
                    trial["wal_bytes"] for trial in selected_trials
                )
            ),
            "query_latency_ms": query_latency_by_probe,
            "query_p95_ms": {
                name: latency["p95_ms"]
                for name, latency in query_latency_by_probe.items()
            },
        }
    return summary_by_candidate


def _collect_measured_trials(
    dsn: str,
    paths: dict[str, Path],
    expected_facts: int,
    measured_trials: int,
    warmup_trials: int,
    query_iterations: int,
) -> list[dict[str, Any]]:
    """Run rotated candidate orders and discard warmup receipts."""
    measured_receipts = []
    for round_index in range(warmup_trials + measured_trials):
        rotation = round_index % len(CANDIDATES)
        candidate_order = CANDIDATES[rotation:] + CANDIDATES[:rotation]
        for candidate in candidate_order:
            candidate_receipt = run_candidate(
                dsn, candidate, round_index, paths, expected_facts, query_iterations
            )
            if round_index >= warmup_trials:
                measured_receipts.append(candidate_receipt)
    return measured_receipts


def _assert_trial_parity(trials: list[dict[str, Any]]) -> None:
    """Require identical full facts and API responses across layouts."""
    for probe_name in trials[0]["probes"]:
        digests = {trial["probes"][probe_name]["digest"] for trial in trials}
        if len(digests) != 1:
            raise RuntimeError(f"candidate response mismatch: {probe_name}")
    if len({trial["semantic_sha256"] for trial in trials}) != 1:
        raise RuntimeError("candidate semantic fact mismatch")


def _surviving_candidates(summary_by_candidate: dict[str, Any]) -> list[str]:
    """Apply storage and API latency gates relative to typed facts."""
    typed_summary = summary_by_candidate["typed"]
    return [
        candidate
        for candidate, summary in summary_by_candidate.items()
        if summary["median_storage_bytes"] <= typed_summary["median_storage_bytes"]
        and all(
            summary["query_p95_ms"][name]
            <= max(
                typed_summary["query_p95_ms"][name] * 1.25,
                typed_summary["query_p95_ms"][name] + 1.0,
            )
            for name in typed_summary["query_p95_ms"]
        )
    ]


def run_benchmark(
    dsn: str,
    manifest_path: Path,
    work_dir: Path,
    *,
    measured_trials: int = 5,
    warmup_trials: int = 1,
    query_iterations: int = 5,
    format_name: str = "json",
) -> dict[str, Any]:
    """Run alternating fresh-schema trials and return a JSON-ready receipt."""
    if measured_trials < 1 or warmup_trials < 0 or query_iterations < 1:
        raise ValueError("trial and query counts are invalid")
    database = _validated_database(dsn)
    hospitals, facts, source_by_hospital, manifest = load_manifest_format(
        manifest_path, format_name
    )
    paths = write_canonical_tsv(work_dir, hospitals, facts, source_by_hospital)
    trials = _collect_measured_trials(
        dsn, paths, len(facts), measured_trials, warmup_trials, query_iterations
    )
    _assert_trial_parity(trials)
    summaries = _summarize(trials)
    survivors = _surviving_candidates(summaries)
    winner = min(survivors, key=lambda name: summaries[name]["median_import_seconds"])
    return {
        "status": "passed",
        "database": database,
        "environment": {
            "machine": platform.machine(),
            "postgres": _psql(dsn, "SHOW server_version;", tuples=True),
        },
        "corpus": {
            "hospitals": len(hospitals),
            "facts": len(facts),
            "format": format_name,
            "semantic_sha256": manifest["semantic_sha256"],
            "canonical_bytes": sum(path.stat().st_size for path in paths.values()),
            "source_bytes": {
                name: sum(
                    (manifest_path.parent / entry[name]).stat().st_size
                    for entry in manifest["entries"]
                )
                for name in READERS
            },
        },
        "gates": {
            "semantic_parity": "passed",
            "response_parity": "passed",
            "max_storage_ratio_to_typed": 1.0,
            "max_query_p95_ratio_to_typed": 1.25,
            "max_query_p95_additive_ms": 1.0,
            "survivors": survivors,
            "winner": winner,
        },
        "summary": summaries,
        "trials": trials,
    }
