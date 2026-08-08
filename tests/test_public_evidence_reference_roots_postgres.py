# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for public-evidence reference roots."""

from __future__ import annotations

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from tests.public_evidence_reference_roots_postgres_support import (
    LONG_PROTOCOL_ID,
    REFERENCE_TABLE_NAMES,
    assert_reference_catalog_shape,
    insert_reference_row,
    provider_group_parameters,
    reference_roots_schema,
    source_entity_parameters,
    source_record_parameters,
)
from tests.public_evidence_storage_postgres_support import (
    connect,
    insert_source_release,
    quoted,
    release_parameters,
    run_migration_action,
)


SOURCE_RECORD_KINDS_BY_SOURCE = {
    "tic": ("tic_provider_group_occurrence",),
    "public_provider_directory_fhir": (
        "fhir_insurance_plan",
        "fhir_location",
        "fhir_network",
        "fhir_npi_resource",
        "fhir_organization",
        "fhir_practitioner_role",
    ),
    "nppes_entity_address": ("nppes_registry_record",),
    "public_hpt": ("hpt_hospital_record",),
}


async def _insert_releases(connection, schema_name: str) -> None:
    for source_kind in SOURCE_RECORD_KINDS_BY_SOURCE:
        await insert_source_release(connection, schema_name, source_kind)


async def _sql_ref(
    connection,
    schema_name: str,
    function_name: str,
    parameters: tuple[object, ...],
) -> str:
    placeholders = ", ".join(f"${ordinal}" for ordinal in range(1, len(parameters) + 1))
    return await connection.fetchval(
        f"SELECT {quoted(schema_name)}.{function_name}({placeholders})",
        *parameters,
    )


async def _insert_source_record_vectors(connection, schema_name: str) -> int:
    """Insert every allowed source-record pair after SQL/Python ref parity."""

    record_count = 0
    for source_kind, record_kinds in SOURCE_RECORD_KINDS_BY_SOURCE.items():
        for record_kind in record_kinds:
            record_count += 1
            parameters = source_record_parameters(
                source_kind,
                record_kind,
                identity_contract_id=(
                    LONG_PROTOCOL_ID
                    if record_count == 1
                    else "synthetic_record_hmac_v1"
                ),
                seed=record_count,
            )
            actual_ref = await _sql_ref(
                connection,
                schema_name,
                "public_evidence_source_record_ref",
                (
                    parameters["source_release_ref"],
                    parameters["record_kind"],
                    parameters["identity_contract_id"],
                    parameters["record_hmac_sha256"],
                    parameters["payload_sha256"],
                ),
            )
            assert actual_ref == parameters["source_record_ref"]
            await insert_reference_row(
                connection,
                schema_name,
                "public_evidence_source_record",
                parameters,
            )
    return record_count


async def _insert_provider_group_vector(connection, schema_name: str) -> None:
    """Insert the TiC provider-group vector after SQL/Python ref parity."""

    group_parameters = provider_group_parameters(
        identity_contract_id=LONG_PROTOCOL_ID,
    )
    actual_ref = await _sql_ref(
        connection,
        schema_name,
        "public_evidence_provider_group_ref",
        (
            group_parameters["source_release_ref"],
            group_parameters["identity_contract_id"],
            group_parameters["identity_sha256"],
        ),
    )
    assert actual_ref == group_parameters["provider_group_ref"]
    await insert_reference_row(
        connection,
        schema_name,
        "public_evidence_provider_group",
        group_parameters,
    )


async def _insert_source_entity_vectors(connection, schema_name: str) -> int:
    """Insert both source-entity pairs after SQL/Python ref parity."""

    source_shapes = (
        ("public_provider_directory_fhir", "fhir_organization"),
        ("public_hpt", "hpt_hospital_entity"),
    )
    for ordinal, (source_kind, entity_kind) in enumerate(source_shapes, start=1):
        parameters = source_entity_parameters(
            source_kind,
            entity_kind,
            identity_contract_id=(
                LONG_PROTOCOL_ID if ordinal == 1 else "synthetic_entity_digest_v1"
            ),
            seed=ordinal,
        )
        actual_ref = await _sql_ref(
            connection,
            schema_name,
            "public_evidence_source_entity_ref",
            (
                parameters["source_release_ref"],
                parameters["entity_kind"],
                parameters["identity_contract_id"],
                parameters["identity_sha256"],
            ),
        )
        assert actual_ref == parameters["source_entity_ref"]
        await insert_reference_row(
            connection,
            schema_name,
            "public_evidence_source_entity",
            parameters,
        )
    return len(source_shapes)


async def _assert_reference_counts(
    connection,
    schema_name: str,
    *,
    source_record_count: int,
    source_entity_count: int,
) -> None:
    """Require exact inserted row counts for all three roots."""

    expected_count_by_table = {
        "public_evidence_source_record": source_record_count,
        "public_evidence_provider_group": 1,
        "public_evidence_source_entity": source_entity_count,
    }
    schema = quoted(schema_name)
    for table_name, expected_count in expected_count_by_table.items():
        assert (
            await connection.fetchval(f"SELECT count(*) FROM {schema}.{table_name}")
            == expected_count
        )


@pytest.mark.asyncio
async def test_catalog_and_all_exact_reference_vectors() -> None:
    """Accept every closed source shape and reproduce Python references."""

    async with reference_roots_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            await assert_reference_catalog_shape(connection, schema_name)
            await _insert_releases(connection, schema_name)
            source_record_count = await _insert_source_record_vectors(
                connection, schema_name
            )
            await _insert_provider_group_vector(connection, schema_name)
            source_entity_count = await _insert_source_entity_vectors(
                connection, schema_name
            )
            await _assert_reference_counts(
                connection,
                schema_name,
                source_record_count=source_record_count,
                source_entity_count=source_entity_count,
            )
        finally:
            await connection.close()


async def _assert_parent_spoofs_rejected(connection, schema_name: str) -> None:
    """Reject a child that spoofs either parent kind or contract digest."""

    valid_record_values = source_record_parameters(
        "tic",
        "tic_provider_group_occurrence",
    )
    fhir_release_values = release_parameters("public_provider_directory_fhir")
    source_kind_spoof_values = source_record_parameters(
        "tic",
        "fhir_npi_resource",
        seed=2,
    )
    source_kind_spoof_values["source_kind"] = "public_provider_directory_fhir"
    contract_spoof_by_field = dict(valid_record_values)
    contract_spoof_by_field["source_release_contract_sha256"] = fhir_release_values[
        "contract_sha256"
    ]
    for spoofed_values in (source_kind_spoof_values, contract_spoof_by_field):
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await insert_reference_row(
                connection,
                schema_name,
                "public_evidence_source_record",
                spoofed_values,
            )


async def _assert_source_matrix_rejections(connection, schema_name: str) -> None:
    """Reject otherwise valid refs outside their closed source matrices."""

    invalid_rows = (
        (
            "public_evidence_source_record",
            source_record_parameters("tic", "nppes_registry_record", seed=3),
        ),
        (
            "public_evidence_provider_group",
            provider_group_parameters("public_provider_directory_fhir"),
        ),
        (
            "public_evidence_source_entity",
            source_entity_parameters("public_hpt", "fhir_organization"),
        ),
    )
    for table_name, invalid_values in invalid_rows:
        with pytest.raises(asyncpg.CheckViolationError):
            await insert_reference_row(
                connection,
                schema_name,
                table_name,
                invalid_values,
            )


async def _assert_source_record_shape_rejections(connection, schema_name: str) -> None:
    """Reject malformed record digests and valid-looking wrong refs."""

    valid_record_values = source_record_parameters(
        "tic",
        "tic_provider_group_occurrence",
    )
    invalid_rows = (
        {**valid_record_values, "record_hmac_sha256": b"x" * 31},
        {**valid_record_values, "source_record_ref": "pesr1_" + "A" * 43},
    )
    for invalid_values in invalid_rows:
        with pytest.raises(asyncpg.CheckViolationError):
            await insert_reference_row(
                connection,
                schema_name,
                "public_evidence_source_record",
                invalid_values,
            )


@pytest.mark.asyncio
async def test_rejects_parent_spoof_matrix_digest_and_reference_drift() -> None:
    """Reject rows not owned by one exact release or usable source policy."""

    async with reference_roots_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            await _insert_releases(connection, schema_name)
            await _assert_parent_spoofs_rejected(connection, schema_name)
            await _assert_source_matrix_rejections(connection, schema_name)
            await _assert_source_record_shape_rejections(connection, schema_name)
        finally:
            await connection.close()


async def _assert_table_immutable(
    connection,
    schema_name: str,
    table_name: str,
    parameters: dict[str, object],
) -> None:
    """Insert one row, then reject update, delete, and truncate independently."""

    await insert_reference_row(
        connection,
        schema_name,
        table_name,
        parameters,
    )
    table = f"{quoted(schema_name)}.{table_name}"
    statements = (
        f"UPDATE {table} SET created_at = created_at",
        f"DELETE FROM {table}",
        f"TRUNCATE {table}",
    )
    for statement in statements:
        with pytest.raises(
            asyncpg.ObjectNotInPrerequisiteStateError,
            match="public_evidence_catalog_mutation_forbidden",
        ):
            await connection.execute(statement)


@pytest.mark.asyncio
async def test_immutable_guards_and_populated_downgrade_fail_closed() -> None:
    """Reject every destructive mutation and preserve populated roots."""

    async with reference_roots_schema() as (
        engine,
        database_url,
        schema_name,
        migration,
    ):
        connection = await connect(database_url)
        try:
            await _insert_releases(connection, schema_name)
            rows_by_table = {
                "public_evidence_source_record": source_record_parameters(
                    "tic", "tic_provider_group_occurrence"
                ),
                "public_evidence_provider_group": provider_group_parameters(),
                "public_evidence_source_entity": source_entity_parameters(
                    "public_hpt", "hpt_hospital_entity"
                ),
            }
            assert tuple(rows_by_table) == REFERENCE_TABLE_NAMES
            for table_name, parameters in rows_by_table.items():
                await _assert_table_immutable(
                    connection,
                    schema_name,
                    table_name,
                    parameters,
                )
        finally:
            await connection.close()

        with pytest.raises(
            DBAPIError,
            match="public_evidence_downgrade_requires_empty_reference_roots",
        ):
            await run_migration_action(engine, migration, "downgrade")


@pytest.mark.asyncio
async def test_empty_downgrade_and_reupgrade_preserve_foundation() -> None:
    """Drop and reconstruct only the empty reference-root slice."""

    async with reference_roots_schema() as (
        engine,
        database_url,
        schema_name,
        migration,
    ):
        await run_migration_action(engine, migration, "downgrade")
        connection = await connect(database_url)
        try:
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM pg_tables WHERE schemaname = $1",
                    schema_name,
                )
                == 4
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM pg_constraint AS constraint_record "
                    "JOIN pg_namespace AS namespace "
                    "ON namespace.oid = constraint_record.connamespace "
                    "WHERE namespace.nspname = $1 AND constraint_record.conname = "
                    "'public_evidence_source_release_kind_owner_key'",
                    schema_name,
                )
                == 0
            )
        finally:
            await connection.close()
        await run_migration_action(engine, migration, "upgrade")
