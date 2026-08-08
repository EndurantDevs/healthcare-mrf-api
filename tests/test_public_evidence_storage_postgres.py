# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for the public-evidence catalog roots."""

from __future__ import annotations

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from public_evidence import evidence_record_token_policy as token_policy
from tests.public_evidence_storage_postgres_support import (
    PTG_POLICY_ID,
    PUBLIC_POLICY_ID,
    connect,
    insert_source_release,
    insert_tax_identity,
    insert_token_policies,
    public_evidence_schema,
    quoted,
    release_parameters,
    run_migration_action,
    tax_identity_parameters,
    token_policy_row,
)


TABLE_NAMES = {
    "public_evidence_source_identity",
    "public_evidence_source_release",
    "public_evidence_token_policy",
    "public_evidence_tax_identity",
}


async def _derived_tax_identity_ref(
    connection,
    schema_name: str,
    identity_by_field: dict[str, object],
) -> str:
    return await connection.fetchval(
        f"SELECT {quoted(schema_name)}.public_evidence_tax_identity_ref("
        "$1, $2, $3, $4, $5, $6, $7)",
        identity_by_field["tin_type"],
        identity_by_field["token_policy_contract_id"],
        identity_by_field["token_policy_id"],
        identity_by_field["token_policy_descriptor_sha256"],
        identity_by_field["locator_128"],
        identity_by_field["full_hmac_sha256"],
        identity_by_field["normalization_contract_id"],
    )


def _valid_identity_parameter_sets() -> tuple[dict[str, object], ...]:
    return (
        tax_identity_parameters(
            token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT,
            PTG_POLICY_ID,
            "ein",
            "11" * 32,
        ),
        tax_identity_parameters(
            token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT,
            PUBLIC_POLICY_ID,
            "ein",
            "22" * 32,
        ),
        tax_identity_parameters(
            token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT,
            PUBLIC_POLICY_ID,
            "npi",
            "33" * 32,
        ),
        tax_identity_parameters(
            token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT,
            PUBLIC_POLICY_ID,
            "ein",
            "44" * 16 + "55" * 16,
        ),
        tax_identity_parameters(
            token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT,
            PUBLIC_POLICY_ID,
            "npi",
            "44" * 16 + "66" * 16,
        ),
    )


async def _insert_all_valid_rows(connection, schema_name: str) -> int:
    for source_kind in (
        "tic",
        "public_provider_directory_fhir",
        "nppes_entity_address",
        "public_hpt",
    ):
        await insert_source_release(connection, schema_name, source_kind)
    await insert_token_policies(connection, schema_name)
    identity_parameter_sets = _valid_identity_parameter_sets()
    for identity_parameters in identity_parameter_sets:
        await insert_tax_identity(connection, schema_name, identity_parameters)
    return len(identity_parameter_sets)


@pytest.mark.asyncio
async def test_accepts_all_release_policies_and_typed_identity_candidates() -> None:
    """Accept exact releases plus bounded same-locator HMAC candidates."""

    async with public_evidence_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            identity_count = await _insert_all_valid_rows(connection, schema_name)
            schema = quoted(schema_name)
            assert (
                await connection.fetchval(
                    f"SELECT count(*) FROM {schema}.public_evidence_source_release"
                )
                == 4
            )
            assert (
                await connection.fetchval(
                    f"SELECT count(*) FROM {schema}.public_evidence_tax_identity"
                )
                == identity_count
            )
            assert (
                await connection.fetchval(
                    f"SELECT count(DISTINCT full_hmac_sha256) "
                    f"FROM {schema}.public_evidence_tax_identity "
                    "WHERE locator_128 = decode(repeat('44', 16), 'hex')"
                )
                == 2
            )
        finally:
            await connection.close()


async def _assert_identity_shape_rejections(connection, schema_name: str) -> None:
    public_npi = tax_identity_parameters(
        token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT,
        PUBLIC_POLICY_ID,
        "npi",
        "77" * 32,
    )
    wrong_locator_by_field = dict(public_npi)
    wrong_locator_by_field["locator_128"] = b"z" * 16
    wrong_locator_by_field["tax_identity_ref"] = await _derived_tax_identity_ref(
        connection,
        schema_name,
        wrong_locator_by_field,
    )
    wrong_normalization_by_field = dict(public_npi)
    wrong_normalization_by_field["normalization_contract_id"] = (
        "ein_ascii_digits_or_2_7_hyphen_v1"
    )
    wrong_normalization_by_field["tax_identity_ref"] = await _derived_tax_identity_ref(
        connection,
        schema_name,
        wrong_normalization_by_field,
    )
    for invalid_parameters in (
        wrong_locator_by_field,
        wrong_normalization_by_field,
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await insert_tax_identity(connection, schema_name, invalid_parameters)
    ptg_npi_by_field = dict(public_npi)
    ptg_npi_by_field.update(
        {
            "token_policy_contract_id": token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT,
            "token_policy_id": PTG_POLICY_ID,
            "token_policy_descriptor_sha256": token_policy_row(
                token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT,
                PTG_POLICY_ID,
            )[2],
        }
    )
    ptg_npi_by_field["tax_identity_ref"] = await _derived_tax_identity_ref(
        connection,
        schema_name,
        ptg_npi_by_field,
    )
    with pytest.raises(
        asyncpg.CheckViolationError,
        match="public_evidence_tax_identity_policy_check",
    ):
        await insert_tax_identity(connection, schema_name, ptg_npi_by_field)


async def _assert_cross_type_hmac_rejected(connection, schema_name: str) -> None:
    public_ein = tax_identity_parameters(
        token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT,
        PUBLIC_POLICY_ID,
        "ein",
        "88" * 32,
    )
    await insert_tax_identity(connection, schema_name, public_ein)
    reused_hmac = tax_identity_parameters(
        token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT,
        PUBLIC_POLICY_ID,
        "npi",
        "88" * 32,
    )
    with pytest.raises(asyncpg.UniqueViolationError, match="tax_identity_hmac_key"):
        await insert_tax_identity(connection, schema_name, reused_hmac)


@pytest.mark.asyncio
async def test_rejects_identity_policy_locator_and_type_conflicts() -> None:
    """Reject descriptor, type, normalization, locator, and HMAC conflicts."""

    async with public_evidence_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            schema = quoted(schema_name)
            await insert_token_policies(connection, schema_name)
            with pytest.raises(
                asyncpg.CheckViolationError,
                match="token_policy_shape_check",
            ):
                await connection.execute(
                    f"INSERT INTO {schema}.public_evidence_token_policy "
                    "(token_policy_contract_id, token_policy_id, "
                    "token_policy_descriptor_sha256) VALUES ($1, $2, $3)",
                    token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT,
                    "ptg-tin-hmac-sha256-v1:other-v1",
                    b"x" * 32,
                )

            await _assert_identity_shape_rejections(connection, schema_name)
            await _assert_cross_type_hmac_rejected(connection, schema_name)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_rejects_release_policy_attestation_binding_and_time_drift() -> None:
    """Reject direct SQL rows that depart from the frozen release contract."""

    async with public_evidence_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            invalid_overrides = (
                {"authority_classification": "untrusted_override"},
                {"source_binding_contract_id": None},
                {"observed_record_count": 8},
                {
                    "observed_end_at": release_parameters("tic")[
                        "observed_start_at"
                    ].replace(month=6, day=30)
                },
                {"contract_sha256": b"x" * 31},
            )
            for overrides in invalid_overrides:
                with pytest.raises(asyncpg.CheckViolationError):
                    await insert_source_release(
                        connection,
                        schema_name,
                        "tic",
                        overrides,
                    )
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_immutable_guards_and_populated_downgrade_fail_closed() -> None:
    """Reject all destructive mutations and preserve populated roots."""

    async with public_evidence_schema() as (
        engine,
        database_url,
        schema_name,
        migration,
    ):
        connection = await connect(database_url)
        try:
            await insert_source_release(connection, schema_name, "tic")
            await insert_token_policies(connection, schema_name)
            identity_parameters = tax_identity_parameters(
                token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT,
                PTG_POLICY_ID,
                "ein",
                "99" * 32,
            )
            await insert_tax_identity(connection, schema_name, identity_parameters)
            schema = quoted(schema_name)
            for table_name in TABLE_NAMES:
                with pytest.raises(
                    asyncpg.ObjectNotInPrerequisiteStateError,
                    match="public_evidence_catalog_mutation_forbidden",
                ):
                    await connection.execute(
                        f"UPDATE {schema}.{table_name} SET created_at = created_at"
                    )
                with pytest.raises(
                    asyncpg.ObjectNotInPrerequisiteStateError,
                    match="public_evidence_catalog_mutation_forbidden",
                ):
                    await connection.execute(f"DELETE FROM {schema}.{table_name}")
            with pytest.raises(
                asyncpg.ObjectNotInPrerequisiteStateError,
                match="public_evidence_catalog_mutation_forbidden",
            ):
                await connection.execute(
                    "TRUNCATE "
                    + ", ".join(f"{schema}.{table_name}" for table_name in TABLE_NAMES)
                )
        finally:
            await connection.close()

        with pytest.raises(
            DBAPIError,
            match="public_evidence_downgrade_requires_empty_foundation",
        ):
            await run_migration_action(engine, migration, "downgrade")


@pytest.mark.asyncio
async def test_empty_downgrade_and_reupgrade_are_reversible() -> None:
    """Drop and reconstruct the exact empty catalog without touching other state."""

    async with public_evidence_schema() as (
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
                == 0
            )
        finally:
            await connection.close()
        await run_migration_action(engine, migration, "upgrade")
