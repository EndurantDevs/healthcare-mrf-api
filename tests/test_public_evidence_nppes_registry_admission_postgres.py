# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL 18 proof for complete sealed NPPES registry admission."""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from process.nppes_public_evidence_rows import SOURCE_RECORD_COLUMNS
from process.nppes_public_evidence_writer import (
    _create_stages,
    _drop_stages,
    _finalize,
    _insert_single,
    _stage_complete_replay,
)
from tests.public_evidence_nppes_admission_postgres_support import (
    NEW_TABLES,
    admit_chain,
    admit_replay,
    alternate_source_record_values,
    assert_admission_catalog,
    finished_chain_receipt,
    nppes_admission_schema,
    prepared_replay,
)
from tests.public_evidence_storage_postgres_support import (
    connect,
    run_migration_action,
)


EXPECTED_CONSTRAINT_NAMES = {
    "public_evidence_nppes_registry_admission_pkey",
    "public_evidence_nppes_registry_admission_release_key",
    "public_evidence_nppes_registry_admission_owner_key",
    "public_evidence_nppes_registry_admission_chain_owner_key",
    "public_evidence_nppes_registry_admission_release_fkey",
    "public_evidence_nppes_registry_admission_shape_check",
    "public_evidence_nppes_registry_admission_seal_pkey",
    "public_evidence_nppes_registry_admission_seal_parent_fkey",
    "public_evidence_nppes_registry_admission_seal_shape_check",
    "public_evidence_nppes_registry_member_pkey",
    "public_evidence_nppes_registry_member_npi_key",
    "public_evidence_nppes_registry_member_source_key",
    "public_evidence_nppes_registry_member_evidence_key",
    "public_evidence_nppes_registry_member_admission_fkey",
    "public_evidence_nppes_registry_member_source_fkey",
    "public_evidence_nppes_registry_member_evidence_fkey",
    "public_evidence_nppes_registry_member_shape_check",
    "public_evidence_nppes_registry_member_digest_check",
    "public_evidence_nppes_registry_chain_admission_pkey",
    "public_evidence_nppes_registry_chain_admission_owner_key",
    "public_evidence_nppes_registry_chain_listing_key",
    "public_evidence_nppes_registry_chain_shape_check",
    "public_evidence_nppes_registry_chain_admission_seal_pkey",
    "public_evidence_nppes_registry_chain_admission_seal_parent_fkey",
    "public_evidence_nppes_registry_chain_admission_seal_shape_check",
    "public_evidence_nppes_registry_chain_archive_pkey",
    "public_evidence_nppes_registry_chain_archive_admission_key",
    "public_evidence_nppes_registry_chain_archive_release_key",
    "public_evidence_nppes_registry_chain_archive_name_key",
    "public_evidence_nppes_registry_chain_archive_artifact_key",
    "public_evidence_nppes_registry_chain_archive_manifest_key",
    "public_evidence_nppes_registry_chain_archive_parent_fkey",
    "public_evidence_nppes_registry_chain_archive_admission_fkey",
    "public_evidence_nppes_registry_chain_archive_shape_check",
}
ALTERED_TABLES = (
    "public_evidence_source_record",
    "public_evidence_record",
    "public_evidence_record_source_link",
    "public_evidence_npi_enumeration",
)
APPEND_TABLES = (*ALTERED_TABLES, NEW_TABLES[2], NEW_TABLES[5])
FROZEN = {
    "artifact": "18f8d516f2e5968a282a73a82c4f6c1e4a66232e47c05a588f5fcc2769453c68",
    "release": "perel1_thOXFZ-8-p-uFu5YGFWLNDAhZuQ5RYasRRJjpbr7fyQ",
    "release_sha": "686d441a11182c61aa5197d52082f4d3b2e556a03c1d500a24d7c67edeb3ba94",
    "manifest": "497247a67f817db5a014d2a272c150b1c167339d9e3dc025a71607dd1ad0dba7",
    "root": "70edf8081737f9bd597ad4c457d19a47e0cbef39d37854b22d7a13f202ad7385",
    "admission": "penpa1_sZ_9hhZxyiQpWHwx4msF0_g2duVBgQb3bhRPtME91b0",
    "admission_sha": "ea40e980a27fcf94bd8e60773c45d1caf674947c34c7360019c2fc0855f3f1f7",
    "chain": "penpc1_ZQAuLpMS3kgkBDE8ssSqXo0iE6tqPjhiJRRWJMbTzb4",
    "chain_sha": "72605f4d7d53d571253fef93c8be33a36e4e8597cb3905d2bb85b07f4b4d44e5",
}
FROZEN_MEMBERS = (
    (
        "d53a33bc58ed590cee62f4ed16cbefad84aea2a6aaadcfc4545a366aaa61641b",
        "5ec6ad8a264ee27b16afdcb1e98f944c6e142c0f7b0f32275762a6a1c69ca38c",
        "7cad6728795d191454daf4eff28092209cc1b685b152564606a99259bd074034",
        "08d6b58fa9f192df390dacccfe6e96e0bcf0455c948cee9bd49c23224a6eb6af",
    ),
    (
        "49168a7d68de86a7cb9c7ade7d9c370bb62930b2df3c1953dedbdd9da7a03b7d",
        "ece1a38cb404b65e33ab0a19fadea6569e164a0cb3ab8c977875e3e6dbfcb435",
        "712f00eca3391d9378caa5aaaf663a5c10135c8a6a1f9302a538ba3a1e49e8de",
        "e0a5f3899023324862ce0bf0afae0e38c48e36094875460f1a3c0753814ad4af",
    ),
    (
        "1eccbb76a2e7e4d1d213ba7c08557397bbd5bc6960272d617f347b08f34ab067",
        "d219a342162d3f8256db4c225dcdd771d9838555fc8e7ef0001634d6e56ecaec",
        "57e217d2defa8fabbae15b239383ed4a2acf7d8ed2948df0f40f161142fece6e",
        "e273784c81173956e7eb9464b280a87b1d218776362ccfed2f34bdc9718c72b8",
    ),
)


async def _assert_constraints_and_triggers(
    connection: asyncpg.Connection, schema: str
) -> None:
    """Require exact immediate constraints and enabled-always trigger guards."""

    constraint_rows = await connection.fetch(
        "SELECT relation.relname, constraint_record.conname, "
        "constraint_record.contype::text, constraint_record.condeferrable, "
        "constraint_record.condeferred, constraint_record.convalidated, "
        "constraint_record.conenforced FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid=constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
        "AND constraint_record.contype NOT IN ('n','t')",
        schema,
        list(NEW_TABLES),
    )
    assert {
        constraint_row["conname"] for constraint_row in constraint_rows
    } == EXPECTED_CONSTRAINT_NAMES
    assert all(
        constraint_row["convalidated"] and constraint_row["conenforced"]
        for constraint_row in constraint_rows
    )
    assert all(
        not constraint_row["condeferrable"]
        and not constraint_row["condeferred"]
        for constraint_row in constraint_rows
        if constraint_row["contype"] == "f"
    )
    rewired_names = {
        "public_evidence_source_record_admission_fkey",
        "public_evidence_record_admission_fkey",
        "public_evidence_record_source_link_record_fkey",
        "public_evidence_record_source_link_source_fkey",
        "public_evidence_npi_enumeration_record_fkey",
    }
    rewired_rows = await connection.fetch(
        "SELECT constraint_record.conname, constraint_record.condeferrable, "
        "constraint_record.condeferred, constraint_record.convalidated, "
        "constraint_record.conenforced FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid=constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND constraint_record.conname=ANY($2::text[])",
        schema,
        list(rewired_names),
    )
    assert {
        rewired_constraint_row["conname"]
        for rewired_constraint_row in rewired_rows
    } == rewired_names
    assert all(
        not rewired_constraint_row["condeferrable"]
        and not rewired_constraint_row["condeferred"]
        and rewired_constraint_row["convalidated"]
        and rewired_constraint_row["conenforced"]
        for rewired_constraint_row in rewired_rows
    )
    await _assert_triggers(connection, schema)


async def _assert_triggers(connection: asyncpg.Connection, schema: str) -> None:
    """Require exact enabled-always integrity, append, and immutable guards."""

    trigger_rows = await connection.fetch(
        "SELECT relation.relname, trigger_record.tgname, trigger_record.tgtype::integer, "
        "trigger_record.tgenabled::text, trigger_record.tgdeferrable, "
        "trigger_record.tginitdeferred, procedure_namespace.nspname AS function_schema "
        "FROM pg_trigger AS trigger_record "
        "JOIN pg_class AS relation ON relation.oid=trigger_record.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "JOIN pg_proc AS procedure ON procedure.oid=trigger_record.tgfoid "
        "JOIN pg_namespace AS procedure_namespace "
        "ON procedure_namespace.oid=procedure.pronamespace "
        "WHERE namespace.nspname=$1 AND NOT trigger_record.tgisinternal",
        schema,
    )
    task_triggers = [
        trigger_row
        for trigger_row in trigger_rows
        if trigger_row["relname"] in NEW_TABLES
        or trigger_row["tgname"].endswith("_admission_append_guard")
    ]
    assert all(trigger_row["tgenabled"] == "A" for trigger_row in task_triggers)
    assert all(
        trigger_row["function_schema"] == schema
        for trigger_row in task_triggers
    )
    integrity_rows = [
        trigger_row
        for trigger_row in task_triggers
        if "integrity_guard" in trigger_row["tgname"]
    ]
    assert len(integrity_rows) == 2
    assert all(
        trigger_row["tgtype"] == 5
        and trigger_row["tgdeferrable"]
        and trigger_row["tginitdeferred"]
        for trigger_row in integrity_rows
    )
    append_rows = [
        trigger_row
        for trigger_row in task_triggers
        if trigger_row["relname"] in APPEND_TABLES
        and trigger_row["tgtype"] == 4
    ]
    assert {
        trigger_row["relname"] for trigger_row in append_rows
    } == set(APPEND_TABLES)
    assert all(
        trigger_row["tgtype"] == 4 and not trigger_row["tgdeferrable"]
        for trigger_row in append_rows
    )
    assert not any(
        trigger_row["tgname"]
        in {f"{table}_integrity_guard" for table in ALTERED_TABLES[1:]}
        for trigger_row in trigger_rows
    )


async def _assert_private(connection: asyncpg.Connection, schema: str) -> None:
    for table in NEW_TABLES:
        for privilege in (
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "REFERENCES",
            "TRIGGER",
            "MAINTAIN",
        ):
            assert not await connection.fetchval(
                "SELECT has_table_privilege('public', $1, $2)",
                f"{schema}.{table}",
                privilege,
            )
    public_helpers = await connection.fetchval(
        "SELECT count(*) FROM pg_proc AS procedure "
        "JOIN pg_namespace AS namespace ON namespace.oid=procedure.pronamespace "
        "WHERE namespace.nspname=$1 "
        "AND (procedure.proname LIKE 'public_evidence_nppes_%' "
        "OR procedure.proname='nppes_registry_payload_digest' "
        "OR procedure.proname LIKE 'guard_%_admission_append') "
        "AND has_function_privilege('public', procedure.oid, 'EXECUTE')",
        schema,
    )
    assert public_helpers == 0
    for table in APPEND_TABLES[:-1]:
        assert await connection.fetchval(
            "SELECT has_table_privilege(current_user, $1, 'MAINTAIN')",
            f"{schema}.{table}",
        )


@pytest.mark.asyncio
async def test_catalog_is_exact_private_immediate_and_statement_sealed() -> None:
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            await assert_admission_catalog(connection, schema, ALTERED_TABLES)
            await _assert_constraints_and_triggers(connection, schema)
            await _assert_private(connection, schema)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_frozen_archive_and_chain_insert_idempotently(
    tmp_path: Path,
) -> None:
    replay = await prepared_replay(tmp_path)
    assert replay.admission_row.artifact_sha256 == FROZEN["artifact"]
    assert replay.manifest.release.source_release_ref == FROZEN["release"]
    assert replay.manifest.release.contract_sha256 == FROZEN["release_sha"]
    assert replay.manifest.manifest_sha256 == FROZEN["manifest"]
    assert replay.manifest.evidence_root_sha256 == FROZEN["root"]
    assert replay.admission_row.admission_ref == FROZEN["admission"]
    assert replay.admission_row.contract_sha256 == FROZEN["admission_sha"]
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            await connection.execute("SET TIME ZONE 'Pacific/Kiritimati'")
            await connection.execute("SET DateStyle='SQL, DMY'")
            assert (await admit_replay(connection, schema, replay)).write_state == "inserted"
            admitted = await admit_replay(connection, schema, replay)
            assert admitted.write_state == "already_present"
            chain = finished_chain_receipt(replay, admitted)
            assert chain.chain_ref == FROZEN["chain"]
            assert chain.contract_sha256 == FROZEN["chain_sha"]
            await admit_chain(connection, schema, chain)
            await admit_chain(connection, schema, chain)
            member_rows = await connection.fetch(
                f'SELECT encode(payload_sha256,\'hex\') AS payload, '
                "encode(record_hmac_sha256,'hex') AS hmac, "
                "encode(leaf_sha256,'hex') AS leaf, encode(row_sha256,'hex') AS row_sha "
                f'FROM "{schema}"."public_evidence_nppes_registry_member" '
                "ORDER BY source_row_ordinal"
            )
            assert tuple(
                tuple(member_row.values()) for member_row in member_rows
            ) == FROZEN_MEMBERS
            merkle = await connection.fetchval(
                f'SELECT encode("{schema}".public_evidence_nppes_merkle_root('
                "source_row_ordinal, leaf_sha256 ORDER BY source_row_ordinal),'hex') "
                f'FROM "{schema}"."public_evidence_nppes_registry_member"'
            )
            assert merkle == FROZEN["root"]
            counts = await connection.fetchrow(
                f'SELECT (SELECT count(*) FROM "{schema}".'
                '"public_evidence_nppes_registry_admission_seal") AS archive_seals, '
                f'(SELECT count(*) FROM "{schema}".'
                '"public_evidence_nppes_registry_chain_admission_seal") AS chain_seals'
            )
            assert tuple(counts.values()) == (1, 1)
        finally:
            await connection.close()


async def _stage_replay(connection, schema: str, replay) -> None:
    await _create_stages(connection, schema)
    await _stage_complete_replay(
        connection,
        replay,
        cancel_check=None,
        progress=None,
    )


@pytest.mark.asyncio
async def test_cross_family_tamper_rolls_back_completely(tmp_path: Path) -> None:
    replay = await prepared_replay(tmp_path)
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            await _stage_replay(connection, schema, replay)
            await connection.execute("DELETE FROM nppes_stage_typed")
            with pytest.raises(
                asyncpg.CheckViolationError,
                match="public_evidence_nppes_admission_invalid",
            ):
                await _finalize(connection, schema, replay)
            for table in (
                "public_evidence_source_release",
                "public_evidence_source_record",
                *NEW_TABLES,
            ):
                assert await connection.fetchval(
                    f'SELECT count(*) FROM "{schema}"."{table}"'
                ) == 0
        finally:
            await _drop_stages(connection)
            await connection.close()


@pytest.mark.asyncio
async def test_seal_rejects_same_transaction_and_later_append(
    tmp_path: Path,
) -> None:
    replay = await prepared_replay(tmp_path)
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            with pytest.raises(
                asyncpg.CheckViolationError,
                match="public_evidence_nppes_append_outside_admission",
            ):
                async with connection.transaction():
                    await admit_replay(connection, schema, replay)
                    await _insert_single(
                        connection,
                        schema,
                        "public_evidence_source_record",
                        SOURCE_RECORD_COLUMNS,
                        alternate_source_record_values(replay),
                    )
            assert await connection.fetchval(
                f'SELECT count(*) FROM "{schema}"."public_evidence_source_record"'
            ) == 0
            await admit_replay(connection, schema, replay)
            with pytest.raises(
                asyncpg.CheckViolationError,
                match="public_evidence_nppes_append_outside_admission",
            ):
                async with connection.transaction():
                    await connection.execute(
                        "SET LOCAL session_replication_role='replica'"
                    )
                    await _insert_single(
                        connection,
                        schema,
                        "public_evidence_source_record",
                        SOURCE_RECORD_COLUMNS,
                        alternate_source_record_values(replay),
                    )
            assert await connection.fetchval(
                f'SELECT count(*) FROM "{schema}"."public_evidence_source_record"'
            ) == replay.manifest.source_record_count
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_downgrade_is_empty_only_and_restores_legacy_contract(
    tmp_path: Path,
) -> None:
    replay = await prepared_replay(tmp_path)
    async with nppes_admission_schema() as (engine, url, schema, migration):
        connection = await connect(url)
        try:
            await admit_replay(connection, schema, replay)
            with pytest.raises(
                DBAPIError,
                match="nppes_registry_admission_downgrade_requires_empty_slice",
            ):
                await run_migration_action(engine, migration, "downgrade")
            assert await connection.fetchval(
                f'SELECT count(*) FROM "{schema}".'
                '"public_evidence_nppes_registry_admission_seal"'
            ) == 1
        finally:
            await connection.close()

    async with nppes_admission_schema() as (engine, url, schema, migration):
        await run_migration_action(engine, migration, "downgrade")
        connection = await connect(url)
        try:
            for table in NEW_TABLES:
                assert await connection.fetchval(
                    "SELECT to_regclass($1)", f"{schema}.{table}"
                ) is None
            restored_trigger_rows = await connection.fetch(
                "SELECT trigger_record.tgname, trigger_record.tgenabled::text, "
                "trigger_record.tgdeferrable, trigger_record.tginitdeferred "
                "FROM pg_trigger AS trigger_record JOIN pg_class AS relation "
                "ON relation.oid=trigger_record.tgrelid JOIN pg_namespace AS namespace "
                "ON namespace.oid=relation.relnamespace WHERE namespace.nspname=$1 "
                "AND trigger_record.tgname=ANY($2::text[])",
                schema,
                [f"{table}_integrity_guard" for table in ALTERED_TABLES[1:]],
            )
            assert len(restored_trigger_rows) == 3
            assert all(
                restored_trigger_row["tgenabled"] == "A"
                and restored_trigger_row["tgdeferrable"]
                and restored_trigger_row["tginitdeferred"]
                for restored_trigger_row in restored_trigger_rows
            )
        finally:
            await connection.close()
        await run_migration_action(engine, migration, "upgrade")
