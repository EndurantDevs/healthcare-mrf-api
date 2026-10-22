# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Matched throughput benchmark for the complete CMS NPD intake on PostgreSQL.

Not collected by default. Run it explicitly against a disposable local server:

    HLTHPRT_CMS_NPD_ADMISSION_TEST_DSN=postgresql+asyncpg://user@127.0.0.1:5440/hc_cms_admission_test_<hex>
    HLTHPRT_CMS_NPD_ADMISSION_TEST_ARTIFACT_ROOT=<owned scratch directory>
    HLTHPRT_CMS_NPD_BENCHMARK_RELEASE=<retained release directory with eight .zst files + receipt.json>
    HLTHPRT_CMS_NPD_BENCHMARK_ROWS=200000
    HLTHPRT_CMS_NPD_BENCHMARK_OUT=<result json path>
    HLTHPRT_CMS_NPD_STAGE_WORKERS=<staging worker processes; 0 keeps staging inline; default spare CPUs>
    python -m pytest .agents/cms_npd_admission_benchmark.py -x -s

The sample is a deterministic weighted prefix of every family (the same rows
for the same release and row budget), packaged through the real acquisition
validator, then admitted through ``_run_acquired`` exactly as the intake
worker does. The result records inclusive/exclusive wall and CPU per phase,
database/WAL growth, peak RSS, and content digests of every produced relation
so two source revisions can be compared for both speed and exact output.
"""

from __future__ import annotations

import contextvars
import hashlib
import importlib
import json
import os
import resource
import time
from compression import zstd
from pathlib import Path

import pytest

from process import cms_npd_source as source
from process import provider_directory_cms_identity_candidate as cms_identity
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_npd_relationship as relationships
from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_generic_source_auxiliary as auxiliary
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_proof_store as proof_store
from process import provider_directory_resource_identity_candidate as resource_identity
from tests import cms_npd_admission_postgres_support as support
from tests.cms_npd_admission_postgres_support import cms_admission_template, cms_artifact_root  # noqa: F401
from tests.test_cms_npd_source import _client, _source

# The package attribute is the importer command; the loader module holds the functions.
fhir = importlib.import_module("process.provider_directory_fhir")

_RELEASE_ENV = "HLTHPRT_CMS_NPD_BENCHMARK_RELEASE"
_ROWS_ENV = "HLTHPRT_CMS_NPD_BENCHMARK_ROWS"
_OUT_ENV = "HLTHPRT_CMS_NPD_BENCHMARK_OUT"
_STACK: contextvars.ContextVar[tuple[dict, ...]] = contextvars.ContextVar("cms_benchmark_timer_stack", default=())


def weighted_counts(counts: dict[str, int], total: int) -> dict[str, int]:
    """Split one row budget across families in proportion to the release."""
    denominator = sum(counts.values())
    weights = {kind: max(1, total * count // denominator) for kind, count in counts.items()}
    order = sorted(counts, key=lambda kind: (total * counts[kind]) % denominator, reverse=True)
    for index in range(total - sum(weights.values())):
        weights[order[index % len(order)]] += 1
    assert sum(weights.values()) == total and all(0 < weights[kind] <= counts[kind] for kind in counts)
    return weights


def prefix_rows(release: Path, weights: dict[str, int]) -> dict[str, bytes]:
    """Read the first N decoded lines of every retained family file."""
    rows_by_file = {}
    for name, kind in source.RESOURCE_FILES:
        chunks = []
        with zstd.open(release / f"{name}.zst", "rb") as decoded:
            for _ in range(weights[kind]):
                line = decoded.readline()
                assert line.endswith(b"\n"), name
                chunks.append(line)
        rows_by_file[name] = b"".join(chunks)
    return rows_by_file


class Timers:
    """Inclusive/exclusive wall and process-CPU accounting per wrapped coroutine."""

    def __init__(self):
        self.records: dict[str, dict[str, float | int]] = {}
        self.timeline: list[dict] = []
        self.started = time.monotonic()

    def _record(self, key):
        return self.records.setdefault(key, {"calls": 0, "wall": 0.0, "cpu": 0.0, "wall_excl": 0.0, "cpu_excl": 0.0})

    def wrap(self, monkeypatch, module, name, label, *, family_position=None):
        original = getattr(module, name)

        async def wrapper(*args, **kwargs):
            key = label
            if family_position is not None and len(args) > family_position and isinstance(args[family_position], str):
                key = f"{label}:{args[family_position]}"
            frame = {"child_wall": 0.0, "child_cpu": 0.0}
            token = _STACK.set((*_STACK.get(), frame))
            wall0, cpu0 = time.monotonic(), time.process_time()
            try:
                return await original(*args, **kwargs)
            finally:
                wall, cpu = time.monotonic() - wall0, time.process_time() - cpu0
                _STACK.reset(token)
                parent = _STACK.get()
                if parent:
                    parent[-1]["child_wall"] += wall
                    parent[-1]["child_cpu"] += cpu
                record = self._record(key)
                record["calls"] += 1
                record["wall"] += wall
                record["cpu"] += cpu
                record["wall_excl"] += wall - frame["child_wall"]
                record["cpu_excl"] += cpu - frame["child_cpu"]

        monkeypatch.setattr(module, name, wrapper)

    def observe(self, monkeypatch):
        original = cms._observe_intake

        def wrapper(ctx, *, phase=None, family=None, completed_rows=None, error=None, force=False):
            if phase is not None or error is not None:
                self.timeline.append(
                    {
                        "at": round(time.monotonic() - self.started, 3),
                        "phase": phase,
                        "family": family,
                        "completed_rows": completed_rows,
                        "error": type(error).__name__ if error is not None else None,
                    }
                )
            return original(ctx, phase=phase, family=family, completed_rows=completed_rows, error=error, force=force)

        monkeypatch.setattr(cms, "_observe_intake", wrapper)

    def install(self, monkeypatch):
        self.observe(monkeypatch)
        for module, name, label, family_position in (
            (cms, "_stream_file", "staging_file", 3),
            (cms, "_persist_prepared_batch", "staging_persist_batch", None),
            (storage, "append_generic_source_batch", "append_generic_source_batch", None),
            (storage, "_verify_tables", "custody_verify_tables", None),
            (storage, "_copy", "native_copy", None),
            (proof_store, "persist_built_dataset_proof_shard", "proof_shard_persist", None),
            (completion, "complete_generic_source_candidate", "completion", None),
            (completion, "_complete_payloads", "completion_reduce_copy", None),
            (completion, "_indexes", "completion_indexes", None),
            (completion, "attach_generic_source_candidate", "attach", None),
            (cms, "_assert_counts", "count_validation", None),
            (cms, "_verify_release", "release_file_recheck", None),
            (cms, "_assert_witness_counts", "witness_validation", None),
            (cms, "_materialize_identity_evidence", "identity", None),
            (cms, "_write_identity_batch", "identity_writer_batch", 4),
            (cms, "_seal_identity_evidence", "identity_seal", None),
            (relationships, "materialize", "relationships", None),
            (relationships, "_project_families", "relationships_project_families", None),
            (relationships, "_insert_family", "relationships_insert_family", 4),
            (relationships, "assert_complete", "relationships_verify", None),
            (cms, "_validate_candidate", "candidate_validation", None),
            (fhir, "_assert_candidate_semantic_content_proof", "content_proof_rehash", None),
            (cms, "_prepare_serving_candidate", "coverage", None),
            (coverage, "prepare_cms_candidate_coverage", "coverage_prepare", None),
            (completion, "_finish_derived_indexes", "attach_derived_indexes", None),
            (completion, "_relationships", "attach_relationship_checks", None),
            (completion, "_witness_relationships", "attach_witness_checks", None),
            (completion, "_attach_final_tables", "attach_final_tables", None),
            (auxiliary, "publish_identity_candidate", "attach_publish_identity", None),
            (cms_identity, "publish_identity_stages", "attach_publish_identity_stages", None),
            (cms_identity, "_canonical_authority", "attach_canonical_authority", None),
            (cms_identity, "_global_replay", "attach_identity_global_replay", None),
            (cms_identity, "_publish_canonical_ids", "attach_publish_canonical_ids", None),
            (resource_identity, "publish_resource_identity_stage", "attach_publish_resource_identity", None),
            (resource_identity, "_global_replay", "attach_resource_global_replay", None),
            (storage, "_capture", "custody_capture", None),
        ):
            self.wrap(monkeypatch, module, name, label, family_position=family_position)


async def _capacity(database):
    return {
        "database_bytes": int(await database.scalar("SELECT pg_database_size(current_database())")),
        "wal_lsn": await database.scalar("SELECT pg_current_wal_lsn()::text"),
    }


async def _digest(database, sql, **params):
    row = await database.first(sql, **params)
    return {"count": int(row[0] or 0), "sha256": row[1]}


async def capture_parity(database, dataset_id, release_id):
    """Digest every produced relation so two revisions can be compared exactly.

    shortcut: string_agg digests hold to about 1M rows, switch to a hashed aggregate for full-release runs.
    """
    schema = "mrf"
    agg = "encode(sha256(convert_to(coalesce(string_agg({expr}, E'\\n' ORDER BY {order}), ''), 'UTF8')), 'hex')"

    def query(table, expr, order, where):
        return f"SELECT count(*), {agg.format(expr=expr, order=order)} FROM {schema}.{table} WHERE {where}"

    key_order = 'resource_type COLLATE "C", resource_id COLLATE "C"'
    parity = {
        "resources": await _digest(
            database,
            query(
                "provider_directory_dataset_resource",
                "resource_type||'|'||resource_id||'|'||payload_hash||'|'||coalesce(acquired_resource_sha256,'')",
                key_order,
                "dataset_id=:dataset_id",
            ),
            dataset_id=dataset_id,
        ),
        "witnesses": await _digest(
            database,
            query(
                "provider_directory_cms_npd_resource_witness",
                "resource_type||'|'||resource_id||'|'||raw_payload_sha256||'|'||normalized_payload_hash",
                key_order,
                "dataset_id=:dataset_id",
            ),
            dataset_id=dataset_id,
        ),
        "relationships": await _digest(
            database,
            query(
                "provider_directory_cms_npd_relationship",
                "resource_type||'|'||resource_id||'|'||reference_field||'|'||parent_ordinal||'|'||reference_ordinal"
                "||'|'||source_payload_hash||'|'||raw_payload_sha256||'|'||target_type||'|'||coalesce(target_reference,'')"
                "||'|'||coalesce(target_resource_id,'')||'|'||resolution_status||'|'||coalesce(period_start,'')"
                "||'|'||coalesce(period_end,'')",
                key_order + ", reference_field, parent_ordinal, reference_ordinal",
                "dataset_id=:dataset_id",
            ),
            dataset_id=dataset_id,
        ),
        "entity_evidence": await _digest(
            database,
            query(
                "provider_directory_entity_release_evidence",
                "resource_type||'|'||resource_id||'|'||payload_sha256",
                key_order,
                "source_id='cms-npd' AND release_id=:release_id",
            ),
            release_id=release_id,
        ),
        "entity_bindings": await _digest(
            database,
            query(
                "provider_directory_entity_source_binding",
                "resource_type||'|'||resource_id||'|'||(organization_id IS NOT NULL)::text||'|'||(site_id IS NOT NULL)::text",
                key_order,
                "source_id='cms-npd'",
            ),
        ),
        "resource_identities": await _digest(
            database,
            query(
                "provider_directory_resource_identity",
                "resource_type||'|'||resource_id||'|'||entity_id::text",
                key_order,
                "source_id='cms-npd'",
            ),
        ),
    }
    parity["network_bindings"] = int(
        await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_insurance_network_source_binding WHERE source_id='cms-npd'"
        )
    )
    parity["network_plan_evidence"] = int(
        await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_insurance_network_plan_evidence "
            "WHERE source_id='cms-npd' AND release_id=:release_id",
            release_id=release_id,
        )
    )
    parity["proof_shards"] = int(
        await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_dataset_proof_shard WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    )
    header = await database.first(
        f"SELECT status, dataset_hash, resource_count, candidate_storage_json->>'phase' "
        f"FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id=:dataset_id",
        dataset_id=dataset_id,
    )
    parity["dataset"] = {
        "status": header[0],
        "dataset_hash": header[1],
        "resource_count": int(header[2] or 0),
        "storage_phase": header[3],
    }
    return parity


@pytest.mark.asyncio
async def test_cms_npd_admission_benchmark(monkeypatch, cms_artifact_root):
    """Admit one weighted sample end to end and record speed plus exact-output digests."""
    release_value = os.getenv(_RELEASE_ENV)
    if not release_value:
        pytest.skip(f"set {_RELEASE_ENV} to a retained CMS release directory")
    release = Path(release_value).resolve(strict=True)
    total_rows = int(os.getenv(_ROWS_ENV, "200000"))
    original_receipt = json.loads((release / "receipt.json").read_text(encoding="utf-8"))
    counts = {kind: original_receipt["files"][name]["row_count"] for name, kind in source.RESOURCE_FILES}
    weights = weighted_counts(counts, total_rows)

    extraction_started = time.monotonic()
    rows_by_file = prefix_rows(release, weights)
    selected_sha256 = hashlib.sha256(b"".join(rows_by_file[name] for name, _ in source.RESOURCE_FILES)).hexdigest()
    manifest, payloads = _source(rows_by_file)
    client, _ = _client(manifest, payloads)
    with client:
        directory, receipt = source.acquire_release(cms_artifact_root, client=client)
    del rows_by_file, payloads
    expected_resources = sum(entry["distinct_count"] for entry in receipt["files"].values())
    extraction_seconds = time.monotonic() - extraction_started

    monkeypatch.setattr(cms, "durable_artifact_root", lambda: cms_artifact_root)
    timers = Timers()
    timers.install(monkeypatch)
    async with support.intake_admission_database(monkeypatch) as database:
        before = await _capacity(database)
        rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        wall_started, cpu_started = time.monotonic(), time.process_time()
        with support.release_probe_client(directory) as probe:
            result = await cms._run_acquired({"context": {}}, {}, "benchmark-run", directory, receipt, probe)
        wall = time.monotonic() - wall_started
        cpu = time.process_time() - cpu_started
        out = os.getenv(_OUT_ENV)
        if out:  # keep the timing even if a later capacity or parity query fails
            Path(out).write_text(
                json.dumps(
                    {
                        "partial": True,
                        "admission_wall_seconds": round(wall, 3),
                        "admission_cpu_seconds": round(cpu, 3),
                        "phases": timers.records,
                        "timeline": timers.timeline,
                    },
                    indent=1,
                )
                + "\n",
                encoding="utf-8",
            )
        after = await _capacity(database)
        wal_bytes = int(
            await database.scalar(
                "SELECT pg_wal_lsn_diff(CAST(CAST(:after AS text) AS pg_lsn), CAST(CAST(:before AS text) AS pg_lsn))",
                after=after["wal_lsn"],
                before=before["wal_lsn"],
            )
        )
        parity = await capture_parity(database, result["dataset_id"], receipt["vector_sha256"])

    assert result["status"] == "validated", result
    assert parity["dataset"]["status"] == "validated" and parity["dataset"]["storage_phase"] == "published", parity
    assert parity["resources"]["count"] == expected_resources, parity["resources"]
    assert parity["witnesses"]["count"] == expected_resources, parity["witnesses"]
    assert parity["dataset"]["resource_count"] == expected_resources, parity["dataset"]

    summary = {
        "release_vector_sha256": original_receipt["vector_sha256"],
        "sample_rows": total_rows,
        "weights": weights,
        "selected_bytes_sha256": selected_sha256,
        "sample_vector_sha256": receipt["vector_sha256"],
        "resources": expected_resources,
        "extraction_seconds": round(extraction_seconds, 3),
        "admission_wall_seconds": round(wall, 3),
        "admission_cpu_seconds": round(cpu, 3),
        "resources_per_second": round(expected_resources / wall, 1),
        "database_growth_bytes": after["database_bytes"] - before["database_bytes"],
        "wal_bytes": wal_bytes,
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "rss_before_bytes": rss_before,
        "phases": dict(
            sorted(
                (
                    (key, {name: round(value, 3) for name, value in record.items()})
                    for key, record in timers.records.items()
                ),
                key=lambda item: -item[1]["wall"],
            )
        ),
        "timeline": timers.timeline,
        "parity": parity,
    }
    rendered = json.dumps(summary, indent=1, sort_keys=False)
    if out:
        Path(out).write_text(rendered + "\n", encoding="utf-8")
    print("\nCMS NPD ADMISSION BENCHMARK\n" + rendered)
