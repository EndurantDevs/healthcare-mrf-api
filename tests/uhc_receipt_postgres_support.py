# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixtures and assertions for the disposable UHC receipt proof."""

from __future__ import annotations

from dataclasses import replace

import asyncpg

from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256
from process.formulary_fhir.source_artifact_contract import artifact_sort_key
from process.formulary_fhir.uhc_drug_receipt import uhc_drug_receipt_id
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.uhc_drug_parser_test_support import artifact_set


SOURCE_CONFIGURATION_HASH = "d" * 64
ACQUISITION_CONTRACT_HASH = "e" * 64
COVERAGE_HASH = "1" * 64
MEMBERSHIP_HASH = "2" * 64
ALTERNATIVE_HASH = "3" * 64
TWIN_EVIDENCE_HASH = "4" * 64
SOURCE_OBSERVATION_SHA256 = "5" * 64
SPOOL_CONTENT_SHA256 = "6" * 64
BASELINE_DATASET_ID = "ffd_" + "7" * 48
CANDIDATE_DATASET_ID = "ffd_" + "8" * 48
BASELINE_RUN_ID = "ffua_" + "9" * 48
CANDIDATE_RUN_ID = "ffub_" + "a" * 48


def unicode_artifact_set() -> VerifiedSourceArtifactSet:
    """Build 24+24 rows with exact JSON-escaping and null-size coverage."""

    exact_set, _bodies_by_name = artifact_set()
    first_artifact = exact_set.artifacts[0]
    changed_identity = replace(
        first_artifact.identity,
        file_name='drug-cs-quote-"-é-e\u0301-😀.json',
        source_url='https://example.invalid/quote-"-é-e\u0301-😀-\\.json',
        expected_byte_count=None,
    )
    changed_artifact = replace(first_artifact, identity=changed_identity)
    artifacts = tuple(
        sorted(
            (changed_artifact, *exact_set.artifacts[1:]),
            key=artifact_sort_key,
        )
    )
    return VerifiedSourceArtifactSet(
        source_id=exact_set.source_id,
        source_file_set_sha256=exact_set.source_file_set_sha256,
        raw_listing_projection_sha256=(
            exact_set.raw_listing_projection_sha256
        ),
        artifacts=artifacts,
        artifact_set_sha256=artifact_set_sha256(artifacts),
    )


async def _seed_source(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    await connection.execute(
        f"INSERT INTO {schema}.fhir_formulary_source "
        "(source_id, canonical_base, display_name) VALUES ($1, $2, $3)",
        UHC_FORMULARY_SOURCE_ID,
        "https://providermrf.uhc.com",
        "UHC official formulary MRF",
    )


async def _seed_datasets(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    dataset_parameters = (
        (
            BASELINE_DATASET_ID,
            UHC_FORMULARY_SOURCE_ID,
            BASELINE_RUN_ID,
            False,
            COVERAGE_HASH,
            MEMBERSHIP_HASH,
            ACQUISITION_CONTRACT_HASH,
        ),
        (
            CANDIDATE_DATASET_ID,
            UHC_FORMULARY_SOURCE_ID,
            CANDIDATE_RUN_ID,
            True,
            COVERAGE_HASH,
            MEMBERSHIP_HASH,
            ACQUISITION_CONTRACT_HASH,
        ),
    )
    await connection.executemany(
        f"INSERT INTO {schema}.fhir_formulary_dataset "
        "(dataset_id, source_id, run_id, cutoff_at, status, "
        "publish_requested, seed_eligible, list_count, alias_count, "
        "medication_count, coverage_hash, membership_hash, summary_json, "
        "verified_at) VALUES ($1, $2, $3, transaction_timestamp(), "
        "'verified', $4, false, 2, 2, 5, $5, $6, "
        "jsonb_build_object('acquisition_contract_hash', $7::text), "
        "transaction_timestamp())",
        dataset_parameters,
    )


async def _seed_attempt(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    await connection.execute(
        f"INSERT INTO {schema}.fhir_formulary_twin_attempt "
        "(source_id, baseline_dataset_id, baseline_run_id, "
        "candidate_dataset_id, candidate_run_id, cutoff_at, "
        "source_configuration_hash, acquisition_contract_hash, "
        "baseline_evidence_hash, candidate_evidence_hash, matched, "
        "attempted_at) VALUES ($1, $2, $3, $4, $5, "
        "transaction_timestamp(), $6, $7, $8, $8, true, "
        "transaction_timestamp())",
        UHC_FORMULARY_SOURCE_ID,
        BASELINE_DATASET_ID,
        BASELINE_RUN_ID,
        CANDIDATE_DATASET_ID,
        CANDIDATE_RUN_ID,
        SOURCE_CONFIGURATION_HASH,
        ACQUISITION_CONTRACT_HASH,
        TWIN_EVIDENCE_HASH,
    )


async def _seed_admission(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    await connection.execute(
        f"INSERT INTO {schema}.fhir_formulary_twin_admission "
        "(source_id, baseline_dataset_id, baseline_run_id, "
        "candidate_dataset_id, candidate_run_id, predecessor_dataset_id, "
        "cutoff_at, source_configuration_hash, acquisition_contract_hash, "
        "list_count, alias_count, medication_count, coverage_hash, "
        "membership_hash, alternative_count, alternative_hash, "
        "baseline_verified_at, candidate_verified_at, admitted_at) "
        "VALUES ($1, $2, $3, $4, $5, NULL, transaction_timestamp(), "
        "$6, $7, 2, 2, 5, $8, $9, 0, $10, transaction_timestamp(), "
        "transaction_timestamp(), transaction_timestamp())",
        UHC_FORMULARY_SOURCE_ID,
        BASELINE_DATASET_ID,
        BASELINE_RUN_ID,
        CANDIDATE_DATASET_ID,
        CANDIDATE_RUN_ID,
        SOURCE_CONFIGURATION_HASH,
        ACQUISITION_CONTRACT_HASH,
        COVERAGE_HASH,
        MEMBERSHIP_HASH,
        ALTERNATIVE_HASH,
    )


async def seed_source_and_twins(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Seed one exact matched generic formulary twin admission."""

    schema = quoted(schema_name)
    await _seed_source(connection, schema)
    await _seed_datasets(connection, schema)
    await _seed_attempt(connection, schema)
    await _seed_admission(connection, schema)


async def seed_pending_artifacts(
    connection: asyncpg.Connection,
    schema_name: str,
    exact_set: VerifiedSourceArtifactSet,
) -> None:
    """Insert one exact 48-row file set in reverse physical order."""

    schema = quoted(schema_name)
    await connection.execute(
        f"INSERT INTO {schema}.fhir_formulary_source_artifact_set "
        "(source_id, source_file_set_sha256, "
        "raw_listing_projection_sha256, expected_file_count) "
        "VALUES ($1, $2, $3, 48)",
        exact_set.source_id,
        exact_set.source_file_set_sha256,
        exact_set.raw_listing_projection_sha256,
    )
    await connection.execute(
        f"INSERT INTO {schema}.fhir_formulary_source_artifact_observation "
        "(source_id, source_observation_sha256, source_file_set_sha256, "
        "raw_listing_projection_sha256) VALUES ($1, $2, $3, $4)",
        exact_set.source_id,
        SOURCE_OBSERVATION_SHA256,
        exact_set.source_file_set_sha256,
        exact_set.raw_listing_projection_sha256,
    )
    await connection.executemany(
        f"INSERT INTO {schema}.fhir_formulary_source_artifact "
        "(source_id, source_file_set_sha256, source_file_id, "
        "raw_listing_projection_sha256, family, file_name, source_url, "
        "catalog_modified_at, catalog_entry_sha256, expected_byte_count, "
        "status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "
        "'pending')",
        tuple(
            (
                artifact.identity.source_id,
                artifact.identity.source_file_set_sha256,
                artifact.identity.source_file_id,
                artifact.identity.raw_listing_projection_sha256,
                artifact.identity.family,
                artifact.identity.file_name,
                artifact.identity.source_url,
                artifact.identity.catalog_modified_at,
                artifact.identity.catalog_entry_sha256,
                artifact.identity.expected_byte_count,
            )
            for artifact in reversed(exact_set.artifacts)
        ),
    )


async def verify_artifacts(
    connection: asyncpg.Connection,
    schema_name: str,
    exact_set: VerifiedSourceArtifactSet,
) -> None:
    """Fill every pending row with its exact retained content identity."""

    schema = quoted(schema_name)
    await connection.executemany(
        f"UPDATE {schema}.fhir_formulary_source_artifact SET "
        "artifact_sha256 = $1, artifact_byte_count = $2, "
        "status = 'verified', verified_at = transaction_timestamp() "
        "WHERE source_id = $3 AND source_file_set_sha256 = $4 "
        "AND source_file_id = $5",
        tuple(
            (
                artifact.artifact_sha256,
                artifact.artifact_byte_count,
                artifact.identity.source_id,
                artifact.identity.source_file_set_sha256,
                artifact.identity.source_file_id,
            )
            for artifact in exact_set.artifacts
        ),
    )


def canonical_receipt_id(exact_set: VerifiedSourceArtifactSet) -> str:
    """Return the sole receipt ID admitted for the fixture."""

    return uhc_drug_receipt_id(
        UHC_FORMULARY_SOURCE_ID,
        CANDIDATE_DATASET_ID,
        SOURCE_OBSERVATION_SHA256,
        exact_set.source_file_set_sha256,
        exact_set.artifact_set_sha256,
        SPOOL_CONTENT_SHA256,
    )


def receipt_insert_sql(
    schema_name: str,
    exact_set: VerifiedSourceArtifactSet,
    *,
    receipt_id: str,
    artifact_root: str,
    plan_count: int = 2,
) -> str:
    """Render one fixed aggregate receipt insert without source values."""

    schema = quoted(schema_name)
    return f"""INSERT INTO {schema}.fhir_formulary_uhc_admission_receipt
      (receipt_id, source_id, source_observation_sha256,
       source_file_set_sha256, artifact_set_sha256, candidate_dataset_id,
       spool_content_sha256, file_count, raw_record_count,
       raw_plan_entry_count, plan_count, medication_membership_count,
       duplicate_count, superseded_count, max_last_updated_at)
    VALUES ('{receipt_id}', '{UHC_FORMULARY_SOURCE_ID}',
      '{SOURCE_OBSERVATION_SHA256}', '{exact_set.source_file_set_sha256}',
      '{artifact_root}', '{CANDIDATE_DATASET_ID}', '{SPOOL_CONTENT_SHA256}',
      48, 48, 48, {plan_count}, 5, 46, 0,
      transaction_timestamp() - interval '1 day')"""


async def _assert_invalid_receipts(
    connection: asyncpg.Connection,
    schema_name: str,
    exact_set: VerifiedSourceArtifactSet,
) -> None:
    exact_receipt_id = canonical_receipt_id(exact_set)
    wrong_root = "0" + exact_set.artifact_set_sha256[1:]
    wrong_root_receipt = uhc_drug_receipt_id(
        UHC_FORMULARY_SOURCE_ID,
        CANDIDATE_DATASET_ID,
        SOURCE_OBSERVATION_SHA256,
        exact_set.source_file_set_sha256,
        wrong_root,
        SPOOL_CONTENT_SHA256,
    )
    for statement in (
        receipt_insert_sql(
            schema_name,
            exact_set,
            receipt_id=wrong_root_receipt,
            artifact_root=wrong_root,
        ),
        receipt_insert_sql(
            schema_name,
            exact_set,
            receipt_id="ffur_" + "f" * 48,
            artifact_root=exact_set.artifact_set_sha256,
        ),
        receipt_insert_sql(
            schema_name,
            exact_set,
            receipt_id=exact_receipt_id,
            artifact_root=exact_set.artifact_set_sha256,
            plan_count=3,
        ),
    ):
        await assert_sqlstate(connection, "23514", statement)


async def verify_root_and_insert_receipt(
    connection: asyncpg.Connection,
    schema_name: str,
    exact_set: VerifiedSourceArtifactSet,
) -> str:
    """Prove Python/SQL root parity and insert only the canonical receipt."""

    hash_function = (
        f"{quoted(schema_name)}."
        "fhir_formulary_source_artifact_set_sha256($1, $2)"
    )
    exact_receipt_id = canonical_receipt_id(exact_set)
    assert await connection.fetchval(
        f"SELECT {hash_function}",
        exact_set.source_id,
        exact_set.source_file_set_sha256,
    ) is None
    await assert_sqlstate(
        connection,
        "23514",
        receipt_insert_sql(
            schema_name,
            exact_set,
            receipt_id=exact_receipt_id,
            artifact_root=exact_set.artifact_set_sha256,
        ),
    )
    await verify_artifacts(connection, schema_name, exact_set)
    sql_root = await connection.fetchval(
        f"SELECT {hash_function}",
        exact_set.source_id,
        exact_set.source_file_set_sha256,
    )
    assert sql_root == exact_set.artifact_set_sha256
    await _assert_invalid_receipts(connection, schema_name, exact_set)
    insert_status = await connection.execute(
        receipt_insert_sql(
            schema_name,
            exact_set,
            receipt_id=exact_receipt_id,
            artifact_root=exact_set.artifact_set_sha256,
        )
    )
    assert insert_status == "INSERT 0 1"
    return exact_receipt_id


async def assert_receipt_catalog(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Prove exact hardened functions and always-on receipt triggers."""

    routine_entries = await connection.fetch(
        "SELECT routine.proname, routine.prosecdef, routine.proconfig, "
        "routine.provolatile::text, "
        "has_function_privilege('public', routine.oid, 'EXECUTE') "
        "AS public_execute FROM pg_proc AS routine "
        "JOIN pg_namespace AS namespace "
        "ON namespace.oid = routine.pronamespace "
        "WHERE namespace.nspname = $1 AND routine.proname IN "
        "('guard_fhir_formulary_uhc_admission_receipt', "
        "'fhir_formulary_source_artifact_set_sha256')",
        schema_name,
    )
    routines_by_name = {
        routine_entry["proname"]: routine_entry
        for routine_entry in routine_entries
    }
    assert set(routines_by_name) == {
        "guard_fhir_formulary_uhc_admission_receipt",
        "fhir_formulary_source_artifact_set_sha256",
    }
    assert all(entry["prosecdef"] for entry in routines_by_name.values())
    assert all(
        entry["proconfig"] == ["search_path=pg_catalog"]
        and entry["public_execute"] is False
        for entry in routines_by_name.values()
    )
    assert routines_by_name[
        "fhir_formulary_source_artifact_set_sha256"
    ]["provolatile"] == "s"
    trigger_entries = await connection.fetch(
        "SELECT trigger.tgname, trigger.tgenabled::text FROM pg_trigger "
        "AS trigger JOIN pg_class AS relation "
        "ON relation.oid = trigger.tgrelid JOIN pg_namespace AS namespace "
        "ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = $1 AND relation.relname = "
        "'fhir_formulary_uhc_admission_receipt' "
        "AND NOT trigger.tgisinternal",
        schema_name,
    )
    assert {
        entry["tgname"]: entry["tgenabled"] for entry in trigger_entries
    } == {
        "fhir_formulary_uhc_admission_receipt_guard": "A",
        "fhir_formulary_uhc_admission_receipt_guard_truncate": "A",
    }


async def assert_receipt_immutability(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Reject every receipt mutation and every PUBLIC table privilege."""

    table = f"{quoted(schema_name)}.fhir_formulary_uhc_admission_receipt"
    for statement in (
        f"UPDATE {table} SET plan_count = plan_count",
        f"DELETE FROM {table}",
        f"TRUNCATE TABLE {table}",
    ):
        await assert_sqlstate(connection, "55000", statement)
    privileges = await connection.fetchrow(
        "SELECT "
        "has_table_privilege('public', relation.oid, 'SELECT') AS select_ok, "
        "has_table_privilege('public', relation.oid, 'INSERT') AS insert_ok, "
        "has_table_privilege('public', relation.oid, 'UPDATE') AS update_ok, "
        "has_table_privilege('public', relation.oid, 'DELETE') AS delete_ok, "
        "has_table_privilege('public', relation.oid, 'TRUNCATE') AS truncate_ok "
        "FROM pg_class AS relation JOIN pg_namespace AS namespace "
        "ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = $1 AND relation.relname = "
        "'fhir_formulary_uhc_admission_receipt'",
        schema_name,
    )
    assert not any(dict(privileges).values())


__all__ = (
    "CANDIDATE_DATASET_ID",
    "assert_receipt_catalog",
    "assert_receipt_immutability",
    "canonical_receipt_id",
    "seed_pending_artifacts",
    "seed_source_and_twins",
    "unicode_artifact_set",
    "verify_root_and_insert_receipt",
)
