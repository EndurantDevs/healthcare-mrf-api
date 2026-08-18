# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed evidence alias activation and revocation lifecycle."""

import tempfile
from unittest.mock import AsyncMock

import pytest

from process import address_evidence_alias_native
from process.address_numeric_grid_alias import run_numeric_grid_alias
from process.address_numeric_grid_alias_revoke import revoke_numeric_grid_alias
from process.ext import address_canon
from process.ext.address_canon import resolve_into_archive
from tests.address_evidence_alias_test_support import _insert_visible_address
from tests.test_address_numeric_grid_alias_runtime_db import (
    _connect_runtime_database,
    _create_alias_probe_schema,
    _insert_archive_address,
    _requires_test_database,
    db,
)

_ALIAS_KIND = "evidence_gated_address_match_v1"
_STAGE = "evidence_alias_resolve_stage"
_FIELD_MAP = {
    "first_line": "first_line",
    "second_line": "second_line",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "country": "country",
}
_ARCHIVE_CASE_BY_NAME = {
    "positive_source": ("4007 Clarksville Pike 301", None, "75001", 1),
    "positive_target": ("4007 Clarksville Pike", "Suite 301", "75001", 6),
    "route_source": ("123 US Highway 64", None, "75002", 1),
    "route_target": ("123 US Highway", "Suite 64", "75002", 6),
    "missing_direction_source": ("10 Main St", None, "75003", 1),
    "missing_direction_target": ("10 N Main St", None, "75003", 6),
    "ambiguous_source": ("20 Main", None, "75004", 1),
    "ambiguous_street_target": ("20 Main St", None, "75004", 6),
    "ambiguous_road_target": ("20 Main Rd", None, "75004", 6),
    "conflict_source": ("30 Main St N", None, "75005", 1),
    "conflict_target": ("30 N Main St", None, "75005", 6),
    "conflicting_direction": ("30 S Main St", None, "75005", 2),
    "different_npi_source": ("55 Example Road 9", None, "75006", 1),
    "different_npi_target": ("55 Example Road", "Suite 9", "75006", 6),
    "archive_ambiguous_source": ("77 Archive Road 9", None, "75007", 1),
    "archive_ambiguous_target": ("77 Archive Road", "Suite 9", "75007", 6),
    "archive_room_target": ("77 Archive Road", "Room 9", "75007", 6),
    "drift_source": ("88 Original Source", None, "75008", 1),
    "drift_target": ("99 Original Target", None, "75008", 6),
    "cross_zip_source": ("89 Crosszip Road 9", None, "75008", 1),
    "cross_zip_target": ("89 Crosszip Road", "Suite 9", "75009", 6),
}


async def _corrupt_probe_rows(schema, key_by_case) -> None:
    await db.status(
        f"UPDATE {schema}.address_archive_v2 SET zip5 = '99999' "
        "WHERE address_key = CAST(:address_key AS uuid);",
        address_key=key_by_case["ambiguous_road_target"],
    )
    await db.status(
        f"""
        UPDATE {schema}.address_archive_v2
        SET first_line = CASE address_key
                WHEN CAST(:source_key AS uuid) THEN '88 Drift Road 9'
                ELSE '88 Drift Road'
            END,
            second_line = CASE address_key
                WHEN CAST(:target_key AS uuid) THEN 'Suite 9'
                ELSE NULL
            END
        WHERE address_key = ANY(CAST(:address_keys AS uuid[]));
        """,
        source_key=key_by_case["drift_source"],
        target_key=key_by_case["drift_target"],
        address_keys=[key_by_case["drift_source"], key_by_case["drift_target"]],
    )
    await db.status(
        f"UPDATE {schema}.address_archive_v2 SET zip5 = '75008' "
        "WHERE address_key = CAST(:address_key AS uuid);",
        address_key=key_by_case["cross_zip_target"],
    )


async def _seed_archive_matrix(schema):
    key_by_case = {}
    for case_name, (first_line, second_line, postal_code, source_bits) in (
        _ARCHIVE_CASE_BY_NAME.items()
    ):
        key_by_case[case_name] = await _insert_archive_address(
            schema,
            first_line=first_line,
            second_line=second_line,
            postal_code=postal_code,
            strict_source_bits=source_bits,
        )
    await _corrupt_probe_rows(schema, key_by_case)
    return key_by_case


async def _seed_visible_matrix(schema, key_by_case) -> None:
    visible_groups = (
        ("positive_source", "positive_target"),
        ("route_source", "route_target"),
        ("missing_direction_source", "missing_direction_target"),
        ("ambiguous_source", "ambiguous_street_target", "ambiguous_road_target"),
        ("conflict_source", "conflict_target", "conflicting_direction"),
    )
    for group_index, case_names in enumerate(visible_groups):
        for address_index, case_name in enumerate(case_names):
            await _insert_visible_address(
                schema,
                location_key=f"location-{group_index}-{address_index}",
                npi=1234567893,
                address_key=key_by_case[case_name],
            )
    special_memberships = (
        ("different-npi-source", 1234567893, "different_npi_source"),
        ("different-npi-target", 1000000004, "different_npi_target"),
        ("archive-ambiguous-source", 1234567893, "archive_ambiguous_source"),
        ("archive-ambiguous-target", 1234567893, "archive_ambiguous_target"),
        ("identity-drift-source", 1234567893, "drift_source"),
        ("identity-drift-target", 1234567893, "drift_target"),
        ("cross-zip-source", 1234567893, "cross_zip_source"),
        ("cross-zip-target", 1234567893, "cross_zip_target"),
    )
    for location_key, npi, case_name in special_memberships:
        await _insert_visible_address(
            schema,
            location_key=location_key,
            npi=npi,
            address_key=key_by_case[case_name],
        )


async def _candidate_record(schema, run_id, source_key):
    return await db.first(
        f"""
        SELECT source_address_key::text, target_address_key::text,
               decision, match_rule, match_classification,
               evidence_npi, evidence_npi_count
        FROM {schema}.address_alias_candidate_v1
        WHERE run_id = CAST(:run_id AS uuid)
          AND source_address_key = CAST(:source_address_key AS uuid);
        """,
        run_id=run_id,
        source_address_key=source_key,
    )


async def _candidate_count(schema, run_id, source_key):
    return await db.scalar(
        f"""
        SELECT max(candidate_count)
        FROM {schema}.address_alias_candidate_v1
        WHERE run_id = CAST(:run_id AS uuid)
          AND source_address_key = CAST(:source_address_key AS uuid)
          AND decision = 'ambiguous';
        """,
        run_id=run_id,
        source_address_key=source_key,
    )


async def _candidate_row_count(schema, run_id, source_keys):
    return await db.scalar(
        f"""
        SELECT count(*)
        FROM {schema}.address_alias_candidate_v1
        WHERE run_id = CAST(:run_id AS uuid)
          AND source_address_key = ANY(CAST(:source_keys AS uuid[]));
        """,
        run_id=run_id,
        source_keys=source_keys,
    )


async def _shadow_and_assert_matrix(schema, key_by_case):
    shadow = await run_numeric_grid_alias(
        mode="shadow",
        schema=schema,
        alias_kind=_ALIAS_KIND,
    )
    assert (shadow.eligible, shadow.ambiguous) == (1, 3)
    assert shadow.candidate_digest
    candidate_record = await _candidate_record(
        schema,
        shadow.run_id,
        key_by_case["positive_source"],
    )
    assert dict(candidate_record._mapping) == {
        "source_address_key": key_by_case["positive_source"],
        "target_address_key": key_by_case["positive_target"],
        "decision": "eligible",
        "match_rule": "candidate_confirmed_bare_unit",
        "match_classification": "exact",
        "evidence_npi": 1234567893,
        "evidence_npi_count": 1,
    }
    for case_name in ("ambiguous_source", "archive_ambiguous_source"):
        assert await _candidate_count(
            schema, shadow.run_id, key_by_case[case_name]
        ) == 2
    assert await _candidate_row_count(
        schema,
        shadow.run_id,
        [key_by_case["conflict_source"]],
    ) == 1
    excluded_keys = [
        key_by_case[case_name]
        for case_name in (
            "route_source",
            "missing_direction_source",
            "different_npi_source",
        )
    ]
    assert await _candidate_row_count(schema, shadow.run_id, excluded_keys) == 0
    return shadow


async def _apply_and_assert_alias(schema, key_by_case, shadow) -> None:
    applied = await run_numeric_grid_alias(
        mode="apply",
        schema=schema,
        alias_kind=_ALIAS_KIND,
        alias_run_id=shadow.run_id,
        expected_candidate_sha256=shadow.candidate_digest,
        reviewed_by="ci-reviewer",
    )
    assert applied.promoted == 1
    alias_record = await db.first(
        f"""
        SELECT source_address_key::text, target_address_key::text, alias_kind
        FROM {schema}.address_alias_v1
        WHERE revoked_at IS NULL;
        """
    )
    assert dict(alias_record._mapping) == {
        "source_address_key": key_by_case["positive_source"],
        "target_address_key": key_by_case["positive_target"],
        "alias_kind": _ALIAS_KIND,
    }


async def _identity_snapshot(schema, key_by_case):
    return await db.all(
        f"""
        SELECT address_key::text, identity_key, identity_version,
               premise_key::text, line1_norm, unit_norm, merged_into::text
        FROM {schema}.address_archive_v2
        WHERE address_key = ANY(CAST(:address_keys AS uuid[]))
        ORDER BY address_key;
        """,
        address_keys=[
            key_by_case["positive_source"],
            key_by_case["positive_target"],
        ],
    )


async def _resolve_source(schema, source_key, source_bit):
    await db.status(f"DROP TABLE IF EXISTS {schema}.{_STAGE};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{_STAGE} (
            address_key uuid, first_line text, second_line text,
            city text, state text, zip_code text, country text
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{_STAGE} VALUES (
            CAST(:source_key AS uuid), '4007 Clarksville Pike 301', NULL,
            'Example City', 'TX', '75001', 'US'
        );
        """,
        source_key=source_key,
    )
    return await resolve_into_archive(
        _STAGE,
        _FIELD_MAP,
        source_bit=source_bit,
        priority=4,
        schema=schema,
        strict_source_predicate="TRUE",
    )


async def _source_provenance(schema, address_key):
    return await db.first(
        f"""
        SELECT source_bits, strict_source_bits
        FROM {schema}.address_archive_v2
        WHERE address_key = CAST(:address_key AS uuid);
        """,
        address_key=address_key,
    )


async def _assert_alias_projection(schema, key_by_case) -> dict[str, object]:
    identity_rows_before = await _identity_snapshot(schema, key_by_case)
    resolve_stats = await _resolve_source(schema, key_by_case["positive_source"], 8)
    assert resolve_stats.reason_buckets["persisted_aliases_applied"] == 1
    assert resolve_stats.reason_buckets["address_alias_generation"] == 2
    identity_rows_after = await _identity_snapshot(schema, key_by_case)
    assert [tuple(identity_record) for identity_record in identity_rows_after] == [
        tuple(identity_record) for identity_record in identity_rows_before
    ]
    source_provenance = await _source_provenance(
        schema,
        key_by_case["positive_source"],
    )
    target_provenance = await _source_provenance(
        schema,
        key_by_case["positive_target"],
    )
    assert source_provenance.source_bits & 8
    assert source_provenance.strict_source_bits & 8
    assert target_provenance.source_bits & 8
    assert target_provenance.strict_source_bits == 6
    materializer = (
        "rust"
        if resolve_stats.reason_buckets["canonical_materializer_rust"]
        else "sql"
    )
    return {
        "identity_rows": [tuple(archive_record) for archive_record in identity_rows_after],
        "source_provenance": tuple(source_provenance),
        "target_provenance": tuple(target_provenance),
        "stats": (
            resolve_stats.staged,
            resolve_stats.distinct_keys,
            resolve_stats.inserted,
            resolve_stats.provenance_updates,
            resolve_stats.null_key_rows,
            resolve_stats.eligible_key_rows,
            resolve_stats.eligible_null_key_rows,
            {
                metric_name: metric_count
                for metric_name, metric_count in resolve_stats.reason_buckets.items()
                if not metric_name.startswith("canonical_materializer_")
            },
            resolve_stats.gate_violations,
        ),
        "materializer": materializer,
    }


async def _revoke_and_assert_alias(schema, key_by_case, shadow) -> None:
    with pytest.raises(RuntimeError, match="active address alias was not found"):
        await revoke_numeric_grid_alias(
            source_address_key=key_by_case["positive_source"],
            expected_target_address_key=key_by_case["positive_target"],
            reason="wrong policy probe",
            reviewed_by="ci-reviewer",
            schema=schema,
        )
    revoked = await revoke_numeric_grid_alias(
        source_address_key=key_by_case["positive_source"],
        expected_target_address_key=key_by_case["positive_target"],
        reason="reversible evidence alias probe",
        reviewed_by="ci-reviewer",
        schema=schema,
        alias_kind=_ALIAS_KIND,
    )
    assert (revoked.alias_kind, revoked.generation) == (_ALIAS_KIND, 3)
    assert await db.scalar(
        f"SELECT count(*) FROM {schema}.address_alias_v1 "
        "WHERE revoked_at IS NULL;"
    ) == 0
    with pytest.raises(RuntimeError, match="reviewed shadow contains a revoked alias"):
        await run_numeric_grid_alias(
            mode="apply",
            schema=schema,
            alias_kind=_ALIAS_KIND,
            alias_run_id=shadow.run_id,
            expected_candidate_sha256=shadow.candidate_digest,
            reviewed_by="ci-reviewer",
        )


async def _assert_revoked_projection(schema, key_by_case) -> None:
    await _resolve_source(schema, key_by_case["positive_source"], 16)
    source_provenance = await _source_provenance(
        schema,
        key_by_case["positive_source"],
    )
    target_provenance = await _source_provenance(
        schema,
        key_by_case["positive_target"],
    )
    assert source_provenance.source_bits & 16
    assert target_provenance.source_bits & 16 == 0


async def _materialize_evidence_alias_parity_case(monkeypatch, connection, materializer, schema):
    monkeypatch.setenv(
        "HLTHPRT_ADDRESS_EVIDENCE_ALIAS_NATIVE", str(materializer == "rust").lower()
    )
    monkeypatch.setenv(
        "HLTHPRT_ADDRESS_EVIDENCE_ALIAS_SCRATCH_DIR", tempfile.gettempdir()
    )
    await _create_alias_probe_schema(connection, schema, "evidence")
    key_by_case = await _seed_archive_matrix(schema)
    await _seed_visible_matrix(schema, key_by_case)
    shadow = await _shadow_and_assert_matrix(schema, key_by_case)
    await _apply_and_assert_alias(schema, key_by_case, shadow)
    monkeypatch.setenv(
        address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV,
        str(materializer == "rust").lower(),
    )
    projection = await _assert_alias_projection(schema, key_by_case)
    return shadow, projection


@pytest.mark.asyncio(loop_scope="session")
async def test_evidence_alias_shadow_apply_resolve_and_revoke(monkeypatch):
    """Reviewed evidence aliases preserve identities and reversible projection."""
    _requires_test_database()
    schema = "address_evidence_alias_probe"
    connection = await _connect_runtime_database()
    try:
        await _create_alias_probe_schema(connection, schema, "evidence")
        key_by_case = await _seed_archive_matrix(schema)
        await _seed_visible_matrix(schema, key_by_case)
        shadow = await _shadow_and_assert_matrix(schema, key_by_case)
        await _apply_and_assert_alias(schema, key_by_case, shadow)
        monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE", "false")
        await _assert_alias_projection(schema, key_by_case)
        await _revoke_and_assert_alias(schema, key_by_case, shadow)
        await _assert_revoked_projection(schema, key_by_case)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_evidence_alias_rust_materialization_matches_sql(monkeypatch):
    """Rust-first canonicalization applies reviewed aliases like SQL fallback."""
    _requires_test_database()
    if address_canon._ptg2_rust_scanner_binary() is None:
        pytest.skip("Rust ptg2_scanner binary is required for alias parity")

    rust_run = AsyncMock(wraps=address_canon._run_rust_address_canonicalizer)
    monkeypatch.setattr(address_canon, "_run_rust_address_canonicalizer", rust_run)
    native_evidence_run = AsyncMock(wraps=address_evidence_alias_native._run_scanner)
    monkeypatch.setattr(
        address_evidence_alias_native,
        "_run_scanner",
        native_evidence_run,
    )
    connection = await _connect_runtime_database()
    projection_by_materializer = {}
    shadow_by_materializer = {}
    schema_by_materializer = {
        "sql": "address_evidence_alias_parity_sql",
        "rust": "address_evidence_alias_parity_rust",
    }
    try:
        for materializer, schema in schema_by_materializer.items():
            shadow, projection = await _materialize_evidence_alias_parity_case(
                monkeypatch,
                connection,
                materializer,
                schema,
            )
            shadow_by_materializer[materializer] = shadow
            projection_by_materializer[materializer] = projection
        assert rust_run.await_count == 1
        assert native_evidence_run.await_count == 2
        assert [len(call.args[2]) for call in native_evidence_run.await_args_list] == [6, 6]
        assert projection_by_materializer["rust"]["materializer"] == "rust"
        assert projection_by_materializer["sql"]["materializer"] == "sql"
        for field in (
            "candidate_digest",
            "source_count",
            "candidate_sources",
            "candidate_rows",
            "no_candidate",
            "active_skipped",
            "eligible",
            "ambiguous",
            "insufficient_provenance",
            "sample_rows",
        ):
            assert getattr(shadow_by_materializer["rust"], field) == getattr(
                shadow_by_materializer["sql"], field
            )
        for projection in projection_by_materializer.values():
            projection.pop("materializer")
        assert projection_by_materializer["rust"] == projection_by_materializer["sql"]
    finally:
        for schema in schema_by_materializer.values():
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()
