# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import struct
import uuid
from pathlib import Path

import pytest

from api import ptg2_serving, ptg2_tables
from api.ptg2_db_sidecars import (
    lookup_serving_binary_by_code_from_db,
    lookup_shared_code_page_from_db,
    lookup_shared_price_atom_memberships_from_db,
    lookup_shared_price_atoms_from_db,
    lookup_shared_provider_code_keys_from_db,
)
from api.ptg2_shared_blocks import fetch_shared_blocks, fetch_shared_graph_members
from db.connection import db
from process.ptg_parts.ptg2_manifest_artifacts import write_global_membership_sidecar
from process.ptg_parts.ptg2_manifest_publish import (
    PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    _copy_price_atom_member_file,
    _copy_ptg2_manifest_price_atom_file,
    _create_ptg2_manifest_serving_stage_table,
    _ptg2_manifest_support_stage_table,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_CANDIDATE_ATTESTATION_CONTRACT,
)
from process.ptg_parts.ptg2_shared_blocks import (
    bind_snapshot_to_shared_layout,
    reserve_shared_layout,
    shared_semantic_fingerprint,
)
from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
)
from process.ptg_parts.ptg2_shared_graph import (
    PTG2_V3_GRAPH_GROUP_TO_NPI,
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
)
from process.ptg_parts.ptg2_shared_reuse import (
    SharedLogicalPlanScope,
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    publish_shared_v3_snapshot_sources,
    publish_strict_shared_v3_layout,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATHS = (
    ROOT / "alembic" / "versions" / "20260712120000_ptg2_v3_shared_schema.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260715120000_ptg2_v3_source_audit_witness.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260716130000_ptg2_v3_multi_plan_scope.py",
)
SCANNER_TEST_PATH = Path(__file__).with_name("test_ptg2_scanner_v3_runs.py")
SERVING_RECORD = struct.Struct(">16s16s16sI")


class _OpRecorder:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, statement) -> None:
        self.executed.append(str(statement))


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _graph_artifacts(
    directory: Path,
    *,
    provider_set_id: bytes,
    provider_group_id: bytes,
    npi: int,
) -> list[dict[str, object]]:
    npi_id = b"\0" * 8 + int(npi).to_bytes(8, "big", signed=False)
    graph_members_by_artifact = {
        "provider_group_npi": {provider_group_id: (npi_id,)},
        "provider_npi_group": {npi_id: (provider_group_id,)},
        "provider_inverted": {provider_group_id: (provider_set_id,)},
        "provider_forward": {provider_set_id: (provider_group_id,)},
    }
    entries: list[dict[str, object]] = []
    directory.mkdir(parents=True)
    for name, mapping in graph_members_by_artifact.items():
        manifest = write_global_membership_sidecar(directory, name, mapping)
        sidecar_metadata_map = dict(manifest["sidecars"][0])
        sidecar_metadata_map.update(
            {
                "name": name,
                "path": str(
                    (directory / str(sidecar_metadata_map["path"])).resolve()
                ),
                "source_shard_id": "shared-smoke-shard",
            }
        )
        entries.append(sidecar_metadata_map)
    return entries


async def _create_shared_schema(schema_name: str) -> None:
    recorder = _OpRecorder()
    for migration_index, migration_path in enumerate(MIGRATION_PATHS):
        migration = _load_module(
            migration_path,
            f"ptg2_shared_schema_{migration_index}_{schema_name}",
        )
        migration.op = recorder
        migration._schema = lambda: schema_name
        migration.upgrade()
    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
    for statement in recorder.executed:
        await db.execute_ddl(statement)
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.code_catalog
            (code_system, code, display_name, short_description)
        VALUES ('CPT', '99213', 'Office visit', 'Office visit')
        """
    )


@pytest.mark.asyncio
async def test_real_postgres_strict_shared_v3_publish_and_cache_free_reads(
    tmp_path,
    monkeypatch,
):
    """Publish a strict shared layout and verify cache-free reads and reuse."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1 for the isolated PostgreSQL test"
        )

    coverage_scope_id = b"\xcc" * 32
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID",
        coverage_scope_id.hex(),
    )
    scanner_tests = _load_module(
        SCANNER_TEST_PATH, "ptg2_shared_publish_scanner_support"
    )
    configured_scanner = os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_SCANNER_BIN")
    scanner_binary = (
        Path(configured_scanner).resolve()
        if configured_scanner
        else scanner_tests._built_scanner_binary()
    )
    assert scanner_binary.is_file() and os.access(scanner_binary, os.X_OK)
    scan = scanner_tests._run_scanner(
        scanner_binary,
        tmp_path,
        "shared-publish-source",
        arch="postgres_binary_v3",
        provider_references_first=True,
        grouped=False,
        multiple_prices=True,
        duplicate_first_price=True,
    )
    serving_records = [
        SERVING_RECORD.unpack_from(scan["partition_bytes"], offset)
        for offset in range(0, len(scan["partition_bytes"]), SERVING_RECORD.size)
    ]
    assert len(serving_records) == 2
    assert all(code_id != b"\0" * 16 for code_id, *_rest in serving_records)
    assert {provider_count for *_ids, provider_count in serving_records} == {2}
    provider_set_ids = {
        provider_set_id for _code, provider_set_id, _price, _count in serving_records
    }
    price_set_ids = {
        price_set_id for _code, _provider, price_set_id, _count in serving_records
    }
    assert len(provider_set_ids) == 1
    assert len(price_set_ids) == 2
    provider_set_id = next(iter(provider_set_ids))
    provider_set_metadata_path = tmp_path / "provider-set-metadata.copy"
    provider_set_metadata_path.write_text(
        f"{provider_set_id.hex()}\t{{}}\n",
        encoding="ascii",
    )
    provider_set_metadata_entries = (
        {"path": str(provider_set_metadata_path), "row_count": 1},
    )
    assert scan["price_atom_frames"]
    assert scan["price_set_atom_frames"]
    assert all(
        Path(frame["path"]).stat().st_size > 0 for frame in scan["price_atom_frames"]
    )
    assert all(
        Path(frame["path"]).stat().st_size > 0
        for frame in scan["price_set_atom_frames"]
    )

    schema_name = f"ptg2_shared_publish_{uuid.uuid4().hex[:16]}"
    quoted_schema = '"' + schema_name + '"'
    snapshot_id = f"shared-smoke-{uuid.uuid4().hex}"
    artifact_digest = hashlib.sha256(scan["artifact"].read_bytes()).hexdigest()
    source_identity = SharedPhysicalArtifactIdentity(
        "in_network",
        "logical_json_sha256_v1",
        "a" * 64,
    )
    source_assignment = SharedSnapshotSourceAssignment(
        source_key=0,
        identity=source_identity,
        source_trace_set_hash="b" * 64,
        source_trace_hashes=("c" * 64,),
        raw_container_sha256=artifact_digest,
        logical_json_sha256="a" * 64,
        logical_hash_deferred=False,
    )
    scanner_summary = scanner_tests._single_frame(scan["frames"], "scanner_summary")
    serving_run_entries = attach_v3_source_run_contract(
        scan["partition_frames"],
        source_identity=source_identity,
        scanner_summary=scanner_summary,
        scanner_config=scanner_tests._single_frame(scan["frames"], "scanner_config"),
    )
    code_dictionary_entries = attach_v3_dictionary_contract(
        scan["code_dictionary_frames"],
        source_identity=source_identity,
        source_run_contract_sha256=serving_run_entries[0][
            "source_run_contract_sha256"
        ],
        scanner_summary=scanner_summary,
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_BINARY_IDS", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(scanner_binary))
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_WORKERS", "1")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES", "67108864"
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES", "16777216"
    )
    monkeypatch.setenv("HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION", "none")
    monkeypatch.setenv("HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES", "65536")

    await db.disconnect()
    await db.connect()
    stage_table: str | None = None
    try:
        await _create_shared_schema(schema_name)
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_snapshot
                (snapshot_id, status, manifest)
            VALUES (:snapshot_id, 'building', '{{}}'::json)
            """,
            snapshot_id=snapshot_id,
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_source_trace_set
                (source_trace_set_hash, source_trace_hashes)
            VALUES (:source_trace_set_hash, CAST(:source_trace_hashes AS varchar[]))
            """,
            source_trace_set_hash=source_assignment.source_trace_set_hash,
            source_trace_hashes=list(source_assignment.source_trace_hashes),
        )
        await publish_shared_v3_snapshot_sources(
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            plan_scopes=[
                SharedLogicalPlanScope("plan-v3-runs", "ein", "group")
            ],
            coverage_scope_id=coverage_scope_id,
            assignments=[source_assignment],
        )
        async with db.transaction() as session:
            reservation = await reserve_shared_layout(
                session,
                schema_name=schema_name,
                semantic_fingerprint=shared_semantic_fingerprint(
                    {"fixture": "strict-shared-publish-v1"}
                ),
                build_token="strict-shared-publish-smoke",
            )
        assert reservation.reused is False

        stage_table = await _create_ptg2_manifest_serving_stage_table(
            f"shared_{reservation.snapshot_key}"
        )
        # Duplicate dictionary rows to exercise atom dedupe. Membership rows are
        # copied once because duplicate physical source files never reach scanning.
        for _duplicate_source in range(2):
            for frame in scan["price_atom_frames"]:
                await _copy_ptg2_manifest_price_atom_file(
                    Path(frame["path"]),
                    target_table=_ptg2_manifest_support_stage_table(
                        stage_table, "price_atom"
                    ),
                )
        for frame in scan["price_set_atom_frames"]:
            await _copy_price_atom_member_file(
                Path(frame["path"]),
                target_table=_ptg2_manifest_support_stage_table(
                    stage_table,
                    "price_set_atom",
                ),
            )

        provider_group_id = bytes.fromhex("00112233445566778899aabbccddeeff")
        npi = 1234567890
        graph_entries = _graph_artifacts(
            tmp_path / "shared-graph",
            provider_set_id=provider_set_id,
            provider_group_id=provider_group_id,
            npi=npi,
        )
        publication = await publish_strict_shared_v3_layout(
            schema_name=schema_name,
            manifest_stage_table=stage_table,
            reserved_snapshot_key=reservation.snapshot_key,
            build_token="strict-shared-publish-smoke",
            expected_coverage_scope_id=coverage_scope_id,
            logical_snapshot_id=snapshot_id,
            expected_source_identities=[source_identity],
            serving_run_entries=serving_run_entries,
            code_dictionary_entries=code_dictionary_entries,
            provider_set_metadata_entries=provider_set_metadata_entries,
            graph_artifact_entries=graph_entries,
            source_audit_witness_entries=(
                scanner_tests._single_frame(
                    scan["frames"],
                    "source_audit_witness_file",
                ),
            ),
            expected_raw_source_sha256=(artifact_digest,),
            provider_identifier_quarantine=scanner_summary[
                "provider_identifier_quarantine"
            ],
            scratch_parent=tmp_path,
        )
        assert publication.snapshot_key == reservation.snapshot_key
        assert publication.layout_reused_at_seal is False
        assert publication.stored_byte_count > 0
        assert publication.serving_index["storage_generation"] == "shared_blocks_v3"
        assert publication.serving_index["shared_block_layout"] == "dense_shared_blocks_v3"
        assert publication.serving_index["source_count"] == 1
        assert publication.serving_index["cold_lookup_contract"] == "ptg_v3_cold_v2"
        assert publication.serving_index["serving_multiplicity_semantics"] == (
            "source_multiset_v1"
        )
        assert publication.serving_index["provider_identifier_quarantine"] == (
            scanner_summary["provider_identifier_quarantine"]
        )
        assert publication.serving_index["coverage_scope_id"] == coverage_scope_id.hex()
        assert publication.serving_index["serving_binary_table"] is None
        assert {
            "finalizer_seconds",
            "serving_block_publish_seconds",
            "dictionary_publish_seconds",
            "provider_set_key_export_seconds",
            "provider_graph_convert_seconds",
            "provider_graph_publish_seconds",
            "independent_publish_wall_seconds",
            "price_publish_seconds",
            "audit_publish_seconds",
            "seal_seconds",
            "shared_publish_total_seconds",
        } <= publication.serving_index["timings"].keys()
        assert all(
            publication.serving_index["timings"][name] >= 0
            for name in (
                "finalizer_seconds",
                "serving_block_publish_seconds",
                "dictionary_publish_seconds",
                "provider_set_key_export_seconds",
                "provider_graph_convert_seconds",
                "provider_graph_publish_seconds",
                "independent_publish_wall_seconds",
                "price_publish_seconds",
                "audit_publish_seconds",
                "seal_seconds",
                "shared_publish_total_seconds",
            )
        )
        assert publication.serving_index["price_stage"]["duplicate_rows_removed"] > 0
        assert publication.serving_index["audit_sample"]["contract"] == (
            "persisted_served_occurrence_sample_v2"
        )
        assert publication.serving_index["audit_sample"]["method"] == (
            "publish_time_stratified_v1"
        )
        assert (
            publication.serving_index["audit_sample"]["serving_multiplicity_semantics"]
            == "source_multiset_v1"
        )
        assert publication.serving_index["audit_sample"]["provider_selection"] == (
            "hash_targeted_owner_ordinals_v1"
        )
        assert (
            publication.serving_index["audit_sample"]["price_membership_block_span"]
            == publication.serving_index["serving_binary"][
                "price_set_atom_memberships_v3"
            ]["block_span"]
        )
        assert publication.serving_index["audit_sample"]["sample_count"] == 3
        assert publication.serving_index["audit_sample"]["candidate_count"] == 2
        assert len(publication.serving_index["audit_sample"]["sample_digest"]) == 64
        assert not any(
            path.name.startswith("ptg2-v3-shared-publish-")
            for path in tmp_path.iterdir()
        )

        await db.status(
            f"""
            UPDATE {quoted_schema}.ptg2_snapshot
               SET status = 'published',
                   manifest = CAST(:manifest AS json)
             WHERE snapshot_id = :snapshot_id
            """,
            snapshot_id=snapshot_id,
            manifest=json.dumps(
                {
                    "serving_index": {
                        **publication.serving_index,
                        "source_key": "synthetic-source",
                    }
                },
                ensure_ascii=True,
                separators=(",", ":"),
            ),
        )
        async with db.transaction() as session:
            await bind_snapshot_to_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
                snapshot_key=publication.snapshot_key,
            )
        source_set_by_field = shared_source_set_metadata(
            [source_assignment.raw_container_sha256]
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_v3_candidate_audit_attestation
                (snapshot_id, snapshot_key, source_key, plan_id,
                 plan_market_type, coverage_scope_id, source_set_digest,
                 audit_sample_digest, contract, tool_name, tool_version,
                 report_digest, report, attested_at, expires_at, activated_at)
            VALUES
                (:snapshot_id, :snapshot_key, 'synthetic-source',
                 'plan-v3-runs', 'group', :coverage_scope_id,
                 :source_set_digest, :audit_sample_digest,
                 :contract, 'integration-test', '1',
                 :report_digest, '{{}}'::jsonb,
                 transaction_timestamp(),
                 transaction_timestamp() + interval '1 hour',
                 transaction_timestamp())
            """,
            snapshot_id=snapshot_id,
            snapshot_key=publication.snapshot_key,
            coverage_scope_id=coverage_scope_id,
            source_set_digest=bytes.fromhex(
                source_set_by_field[
                    "raw_container_sha256_digest"
                ]
            ),
            audit_sample_digest=bytes.fromhex(
                publication.serving_index["audit_sample"]["sample_digest"]
            ),
            contract=PTG2_CANDIDATE_ATTESTATION_CONTRACT,
            report_digest=b"\x11" * 32,
        )

        row_counts = await db.first(
            f"""
            SELECT
                (SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_code),
                (SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_provider_set),
                (SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_provider_group),
                (SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_npi_scope),
                (SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_snapshot_binding),
                (SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_snapshot_scope),
                (SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_audit_occurrence),
                (SELECT COUNT(DISTINCT coverage_scope_id)
                   FROM {quoted_schema}.ptg2_v3_code)
            """
        )
        assert tuple(int(table_count) for table_count in row_counts) == (
            2,
            1,
            1,
            1,
            1,
            1,
            3,
            1,
        )
        provider_set_key = int(
            await db.scalar(
                f"SELECT provider_set_key FROM {quoted_schema}.ptg2_v3_provider_set"
            )
        )
        provider_group_key = int(
            await db.scalar(
                f"SELECT provider_group_key FROM {quoted_schema}.ptg2_v3_provider_group"
            )
        )
        code_key_rows = await db.all(
            f"""
            SELECT reported_code, code_key, negotiation_arrangement
              FROM {quoted_schema}.ptg2_v3_code
             ORDER BY reported_code
            """
        )
        code_key_by_reported_code = {
            str(code_key_record[0]): int(code_key_record[1])
            for code_key_record in code_key_rows
        }
        assert set(code_key_by_reported_code) == {"99213", "99214"}
        assert {str(code_key_record[2]) for code_key_record in code_key_rows} == {
            "FFS"
        }
        audit_rows = await db.all(
            f"""
            SELECT occurrence_id, atom_ordinal, atom_key
              FROM {quoted_schema}.ptg2_v3_audit_occurrence
             WHERE snapshot_key = :snapshot_key
               AND code_key = :code_key
             ORDER BY atom_ordinal
            """,
            snapshot_key=publication.snapshot_key,
            code_key=code_key_by_reported_code["99213"],
        )
        assert [int(audit_row[1]) for audit_row in audit_rows] == [0, 1]
        assert audit_rows[0][2] == audit_rows[1][2]
        assert bytes(audit_rows[0][0]) != bytes(audit_rows[1][0])
        layout_audit_sample = await db.scalar(
            f"""
            SELECT layout_manifest #> '{{serving_index,audit_sample}}'
              FROM {quoted_schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
            """,
            snapshot_key=publication.snapshot_key,
        )
        assert layout_audit_sample == publication.serving_index["audit_sample"]

        async with db.session() as session:
            original_schema = ptg2_tables.PTG2_SCHEMA
            ptg2_tables.PTG2_SCHEMA = schema_name
            try:
                serving_tables = await ptg2_tables.snapshot_serving_tables(
                    session,
                    snapshot_id,
                )
            finally:
                ptg2_tables.PTG2_SCHEMA = original_schema
            assert serving_tables.uses_shared_blocks
            assert serving_tables.shared_snapshot_key == publication.snapshot_key

            class _Pagination:
                limit = 25
                offset = 0

            original_serving_schema = ptg2_serving.PTG2_SCHEMA
            ptg2_serving.PTG2_SCHEMA = schema_name
            try:
                api_payload = await ptg2_serving.search_ptg2_serving_table(
                    session,
                    snapshot_id,
                    {
                        "plan_id": "plan-v3-runs",
                        "code_system": "CPT",
                        "code": "99213",
                    },
                    _Pagination(),
                    serving_tables=serving_tables,
                )
            finally:
                ptg2_serving.PTG2_SCHEMA = original_serving_schema
            assert api_payload is not None
            assert len(api_payload["items"]) == 1
            provenance_map = dict(api_payload["provenance"])
            database_evidence = provenance_map.pop("database_evidence")
            assert provenance_map == {
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
                "database_backend": "postgresql",
                "plan_id": "plan-v3-runs",
                "snapshot_id": snapshot_id,
                "source_key": "synthetic-source",
                "mode": "product_search",
                "pricing_scope": "plan_scoped_ptg",
            }
            assert database_evidence["contract"] == "postgresql_session_v1"
            assert database_evidence["backend_session_active"] is True
            assert database_evidence["database_selected"] is True
            assert database_evidence["transaction_snapshot_observed"] is True
            assert int(database_evidence["server_version_num"]) > 0
            assert api_payload["items"][0]["negotiation_arrangement"] == "FFS"
            assert len(api_payload["items"][0]["prices"]) == 2
            assert (
                api_payload["items"][0]["prices"][0]
                == api_payload["items"][0]["prices"][1]
            )

            forward_rows = await lookup_serving_binary_by_code_from_db(
                session,
                code_key_by_reported_code["99213"],
                shared_snapshot_key=publication.snapshot_key,
                schema_name=schema_name,
                price_dictionary_item_count=2,
                price_dictionary_block_bytes=int(
                    publication.serving_index["serving_binary"]["price_dictionary"][
                        "block_bytes"
                    ]
                ),
            )
            assert len(forward_rows) == 1
            assert forward_rows[0].provider_set_key == provider_set_key
            assert forward_rows[0].provider_count == 2
            assert (
                bytes.fromhex(forward_rows[0].price_set_global_id_128) in price_set_ids
            )
            second_forward_rows = await lookup_serving_binary_by_code_from_db(
                session,
                code_key_by_reported_code["99214"],
                shared_snapshot_key=publication.snapshot_key,
                schema_name=schema_name,
                price_dictionary_item_count=2,
                price_dictionary_block_bytes=int(
                    publication.serving_index["serving_binary"]["price_dictionary"][
                        "block_bytes"
                    ]
                ),
            )
            assert len(second_forward_rows) == 1
            assert (
                bytes.fromhex(second_forward_rows[0].price_set_global_id_128)
                in price_set_ids
            )
            assert second_forward_rows[0].price_key != forward_rows[0].price_key

            assert await lookup_shared_provider_code_keys_from_db(
                session,
                publication.snapshot_key,
                (provider_set_key,),
                schema_name=schema_name,
            ) == {
                provider_set_key: tuple(sorted(code_key_by_reported_code.values()))
            }
            assert (
                await lookup_shared_code_page_from_db(
                    session,
                    publication.snapshot_key,
                    code_key_by_reported_code["99213"],
                    schema_name=schema_name,
                )
                is not None
            )
            price_keys = {
                int(forward_rows[0].price_key),
                int(second_forward_rows[0].price_key),
            }
            memberships = await lookup_shared_price_atom_memberships_from_db(
                session,
                publication.snapshot_key,
                price_keys,
                atom_key_bits=int(publication.serving_index["atom_key_bits"]),
                schema_name=schema_name,
            )
            assert set(memberships) == price_keys
            assert sorted(len(atom_keys) for atom_keys in memberships.values()) == [
                1,
                2,
            ]
            duplicate_atom_keys = memberships[int(forward_rows[0].price_key)]
            assert len(duplicate_atom_keys) == 2
            assert duplicate_atom_keys[0] == duplicate_atom_keys[1]
            atoms = await lookup_shared_price_atoms_from_db(
                session,
                publication.snapshot_key,
                {
                    atom_key
                    for atom_keys in memberships.values()
                    for atom_key in atom_keys
                },
                atom_key_bits=int(publication.serving_index["atom_key_bits"]),
                schema_name=schema_name,
            )
            assert {atom.negotiated_rate for atom in atoms.values()} == {"125.5", "250"}

            assert await fetch_shared_graph_members(
                session,
                schema_name=schema_name,
                snapshot_key=publication.snapshot_key,
                direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
                owner_keys=(npi,),
            ) == {npi: (provider_group_key,)}
            assert await fetch_shared_graph_members(
                session,
                schema_name=schema_name,
                snapshot_key=publication.snapshot_key,
                direction=PTG2_V3_GRAPH_GROUP_TO_NPI,
                owner_keys=(provider_group_key,),
            ) == {provider_group_key: (npi,)}
            assert await fetch_shared_graph_members(
                session,
                schema_name=schema_name,
                snapshot_key=publication.snapshot_key,
                direction=PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
                owner_keys=(provider_group_key,),
            ) == {provider_group_key: (provider_set_key,)}
            assert await fetch_shared_graph_members(
                session,
                schema_name=schema_name,
                snapshot_key=publication.snapshot_key,
                direction=PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
                owner_keys=(provider_set_key,),
            ) == {provider_set_key: (provider_group_key,)}
            assert await fetch_shared_blocks(
                session,
                schema_name=schema_name,
                snapshot_key=publication.snapshot_key,
                object_kind="by_code_provider_shard_v1",
                block_keys=(
                    (code_key_by_reported_code["99213"] << 31)
                    | (provider_set_key // 1024),
                ),
                require_all=True,
            )

        async with db.transaction() as session:
            second_reservation = await reserve_shared_layout(
                session,
                schema_name=schema_name,
                semantic_fingerprint=shared_semantic_fingerprint(
                    {"fixture": "strict-shared-publish-v1-second-logical-source"}
                ),
                build_token="strict-shared-publish-reuse-smoke",
            )
        assert second_reservation.reused is False
        discarded_snapshot_key = second_reservation.snapshot_key
        reused_stage = await _create_ptg2_manifest_serving_stage_table(
            f"shared_{discarded_snapshot_key}"
        )
        for _duplicate_source in range(2):
            for frame in scan["price_atom_frames"]:
                await _copy_ptg2_manifest_price_atom_file(
                    Path(frame["path"]),
                    target_table=_ptg2_manifest_support_stage_table(
                        reused_stage,
                        "price_atom",
                    ),
                )
        for frame in scan["price_set_atom_frames"]:
            await _copy_price_atom_member_file(
                Path(frame["path"]),
                target_table=_ptg2_manifest_support_stage_table(
                    reused_stage,
                    "price_set_atom",
                ),
            )
        reused_snapshot_id = f"shared-reused-{uuid.uuid4().hex}"
        reused_assignment = SharedSnapshotSourceAssignment(
            source_key=0,
            identity=source_identity,
            source_trace_set_hash="e" * 64,
            source_trace_hashes=("f" * 64,),
            raw_container_sha256="1" * 64,
            logical_json_sha256="a" * 64,
            logical_hash_deferred=False,
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_snapshot
                (snapshot_id, status, manifest)
            VALUES (:snapshot_id, 'building', '{{}}'::json)
            """,
            snapshot_id=reused_snapshot_id,
        )
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_source_trace_set
                (source_trace_set_hash, source_trace_hashes)
            VALUES (:source_trace_set_hash, CAST(:source_trace_hashes AS varchar[]))
            """,
            source_trace_set_hash=reused_assignment.source_trace_set_hash,
            source_trace_hashes=list(reused_assignment.source_trace_hashes),
        )
        await publish_shared_v3_snapshot_sources(
            schema_name=schema_name,
            snapshot_id=reused_snapshot_id,
            plan_scopes=[
                SharedLogicalPlanScope(
                    "plan-v3-runs-reused",
                    "ein",
                    "group",
                )
            ],
            coverage_scope_id=coverage_scope_id,
            assignments=[reused_assignment],
        )
        reused_publication = await publish_strict_shared_v3_layout(
            schema_name=schema_name,
            manifest_stage_table=reused_stage,
            reserved_snapshot_key=discarded_snapshot_key,
            build_token="strict-shared-publish-reuse-smoke",
            expected_coverage_scope_id=coverage_scope_id,
            logical_snapshot_id=reused_snapshot_id,
            expected_source_identities=[source_identity],
            serving_run_entries=serving_run_entries,
            code_dictionary_entries=code_dictionary_entries,
            provider_set_metadata_entries=provider_set_metadata_entries,
            graph_artifact_entries=graph_entries,
            source_audit_witness_entries=(
                scanner_tests._single_frame(
                    scan["frames"],
                    "source_audit_witness_file",
                ),
            ),
            expected_raw_source_sha256=(artifact_digest,),
            provider_identifier_quarantine=scanner_summary[
                "provider_identifier_quarantine"
            ],
            scratch_parent=tmp_path,
        )
        assert reused_publication.layout_reused_at_seal is True
        assert reused_publication.snapshot_key == publication.snapshot_key
        assert reused_publication.serving_index["audit_sample"] == (
            publication.serving_index["audit_sample"]
        )
        assert (
            int(
                await db.scalar(
                    f"""
                SELECT COUNT(*)
                  FROM {quoted_schema}.ptg2_v3_snapshot_layout
                 WHERE snapshot_key = :snapshot_key
                """,
                    snapshot_key=discarded_snapshot_key,
                )
                or 0
            )
            == 0
        )
        assert (
            int(
                await db.scalar(
                    f"""
                SELECT COUNT(*)
                  FROM {quoted_schema}.ptg2_v3_audit_occurrence
                 WHERE snapshot_key = :snapshot_key
                """,
                    snapshot_key=discarded_snapshot_key,
                )
                or 0
            )
            == 0
        )
    finally:
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        finally:
            await db.disconnect()
