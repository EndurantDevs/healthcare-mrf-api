# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native prepared-storage constraints, ACL and migration rollback."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import uuid
from contextlib import asynccontextmanager
from itertools import product
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, insert, inspect, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportSourceBindingRevision
from db.models.custom_import_prepared_source import (
    MAX_PREPARED_PART_BYTES,
    PREPARED_SOURCE_CONTRACT,
    CustomImportPreparedSource,
    CustomImportPreparedSourcePart,
)
from process.custom_import.definition_store import register_definition
from tests.custom_import_postgres_support import _migration, isolated_publication_case
from tests.test_custom_import_snowflake_source_binding import _definition

_VERSIONS = Path(__file__).resolve().parents[1] / "alembic/versions"
_MIGRATION = _VERSIONS / "20261023010000_custom_import_prepared_source.py"
_BINDING_POLICY = _VERSIONS / "20261002020000_custom_import_processing_policy.py"
_MODELS = (CustomImportPreparedSource, CustomImportPreparedSourcePart)
_TABLES = tuple(model.__tablename__ for model in _MODELS)
_BINDING_TABLE = "custom_import_source_binding_revision"
_EXCLUDE_PREPARED = "c.relname NOT IN ('custom_import_prepared_source','custom_import_prepared_source_part')"
_HEADER_POSITIVE_FIELDS = (
    "capture_bundle_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "source_binding_revision_id",
    "version",
    "producer_fence",
)


def _run_migration(connection, schema_name, path, operation):
    migration = _migration(path, "synthetic_prepared_native_" + path.stem)
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(connection))
    getattr(migration, operation)()


@asynccontextmanager
async def _legacy_case():
    async with isolated_publication_case(is_snapshot_storage_enabled=False, is_prepared_source_enabled=False) as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_run_migration, case.schema_name, _BINDING_POLICY, "upgrade")
        yield case


@asynccontextmanager
async def _prepared_case():
    async with _legacy_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "upgrade")
        yield case


def _header_values(**changes):
    return (
        dict(
            execution_id=1,
            capture_bundle_id=1,
            dataset_id=1,
            definition_revision_id=1,
            schema_revision_id=1,
            source_binding_revision_id=1,
            source_binding_sha256=b"b" * 32,
            request_identity_sha256=b"r" * 32,
            raw_manifest_sha256=b"o" * 32,
            canonical_processing_policy="{}",
            processing_policy_sha256=b"p" * 32,
            encoding_contract=PREPARED_SOURCE_CONTRACT,
            state="pending",
            version=1,
            producer_fence=1,
            producer_token_sha256=b"t" * 32,
            preparation_deadline_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC),
            canonical_progress="{}",
            part_count=0,
            record_count=0,
            byte_count=0,
            manifest_byte_count=0,
            canonical_summary=None,
            prepared_sha256=None,
            sealed_at=None,
        )
        | changes
    )


def _sealed_values(**changes):
    return _header_values(
        state="sealed",
        canonical_summary="{}",
        prepared_sha256=hashlib.sha256((PREPARED_SOURCE_CONTRACT + ":{}").encode()).digest(),
        sealed_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC),
        **changes,
    )


def _part_values(**changes):
    payload = b"[1,[]]\n"
    return (
        dict(
            execution_id=1,
            stream_slot=1,
            part_ordinal=1,
            dataset_id=1,
            definition_revision_id=1,
            schema_revision_id=1,
            first_source_ordinal=1,
            record_count=1,
            payload_contract=PREPARED_SOURCE_CONTRACT,
            byte_count=len(payload),
            payload=payload,
            payload_sha256=hashlib.sha256(payload).digest(),
            canonical_manifest="{}",
            manifest_sha256=hashlib.sha256(b"{}").digest(),
        )
        | changes
    )


async def _legacy_catalog(connection, schema_name):
    queries_by_kind = {
        "relations": "SELECT c.relname,c.relkind,c.relowner,c.relacl::text,c.relrowsecurity,c.relforcerowsecurity "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema "
        f"AND c.relkind IN ('r','p','S') AND {_EXCLUDE_PREPARED} ORDER BY c.relname",
        "columns": "SELECT c.relname,a.attname,a.atttypid::regtype::text,a.attnotnull,a.attidentity,"
        "pg_get_expr(d.adbin,d.adrelid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=c.oid LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum "
        f"WHERE n.nspname=:schema AND c.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped AND {_EXCLUDE_PREPARED} "
        "ORDER BY c.relname,a.attnum",
        "constraints": "SELECT c.relname,k.conname,k.contype,pg_get_constraintdef(k.oid),k.convalidated "
        "FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
        f"WHERE n.nspname=:schema AND {_EXCLUDE_PREPARED} "
        "AND k.conname<>'custom_import_source_binding_revision_shape_check' ORDER BY c.relname,k.conname",
        "functions": "SELECT p.oid,p.proname,pg_get_functiondef(p.oid),p.proacl::text,p.proowner "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema ORDER BY p.oid",
        "triggers": "SELECT c.relname,t.tgname,t.tgenabled,t.tgfoid,pg_get_triggerdef(t.oid) "
        "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=:schema ORDER BY c.relname,t.tgname",
        "indexes": "SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname=:schema "
        "AND tablename NOT IN ('custom_import_prepared_source','custom_import_prepared_source_part') "
        "ORDER BY tablename,indexname",
        "default_acl": "SELECT d.defaclobjtype,d.defaclrole,d.defaclacl::text FROM pg_default_acl d "
        "JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname=:schema ORDER BY d.oid",
    }
    catalog_by_kind = {}
    for kind, query in queries_by_kind.items():
        catalog_by_kind[kind] = tuple((await connection.execute(text(query), {"schema": schema_name})).all())
    return catalog_by_kind


def _prepared_catalog(connection, schema_name):
    inspector = inspect(connection)
    return {
        table: dict(
            columns=tuple(
                (column["name"], str(column["type"]), column["nullable"], column["default"])
                for column in inspector.get_columns(table, schema=schema_name)
            ),
            primary_key=tuple(inspector.get_pk_constraint(table, schema=schema_name)["constrained_columns"]),
            checks=tuple(
                sorted(
                    (item["name"], item["sqltext"])
                    for item in inspector.get_check_constraints(table, schema=schema_name)
                )
            ),
            indexes=tuple(
                sorted(
                    (item["name"], tuple(item["column_names"]), item["unique"])
                    for item in inspector.get_indexes(table, schema=schema_name)
                )
            ),
            foreign_keys=inspector.get_foreign_keys(table, schema=schema_name),
        )
        for table in _TABLES
    }


async def test_upgrade_is_empty_additive_and_native_downgrade_upgrade_restores_equal_catalogs():
    async with _legacy_case() as case, case.engine.begin() as connection:
        before = await _legacy_catalog(connection, case.schema_name)
        before_tables = set(
            await connection.scalars(
                text("SELECT tablename FROM pg_tables WHERE schemaname=:schema"), {"schema": case.schema_name}
            )
        )
        await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "upgrade")
        assert await _legacy_catalog(connection, case.schema_name) == before
        after_tables = set(
            await connection.scalars(
                text("SELECT tablename FROM pg_tables WHERE schemaname=:schema"), {"schema": case.schema_name}
            )
        )
        assert after_tables - before_tables == set(_TABLES)
        installed = await connection.run_sync(_prepared_catalog, case.schema_name)
        for model in _MODELS:
            assert await connection.scalar(select(func.count()).select_from(model)) == 0
            assert installed[model.__tablename__]["foreign_keys"] == []
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass(:relation)"),
                    {"relation": f"{case.schema_name}.{model.__tablename__}"},
                )
                == 0
            )
        await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "downgrade")
        assert await _legacy_catalog(connection, case.schema_name) == before
        assert (
            set(
                await connection.scalars(
                    text("SELECT tablename FROM pg_tables WHERE schemaname=:schema"), {"schema": case.schema_name}
                )
            )
            == before_tables
        )
        await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "upgrade")
        assert await connection.run_sync(_prepared_catalog, case.schema_name) == installed
        assert await _legacy_catalog(connection, case.schema_name) == before


async def _assert_rejected(connection, model, values, sqlstate="23514"):
    with pytest.raises(DBAPIError) as caught:
        async with connection.begin_nested():
            await connection.execute(insert(model), values)
    assert caught.value.orig.sqlstate == sqlstate


async def test_native_header_identity_hash_and_sealed_null_checks_reject_corruption():
    """Rejected rows roll back independently while valid preparation states remain."""
    async with _prepared_case() as case, case.engine.begin() as connection:
        await connection.execute(insert(CustomImportPreparedSource), _header_values())
        await connection.execute(insert(CustomImportPreparedSource), _header_values(execution_id=2, state="closed"))
        await connection.execute(insert(CustomImportPreparedSource), _sealed_values(execution_id=3))
        invalid_by_field = (
            [{name: 0} for name in _HEADER_POSITIVE_FIELDS]
            + [
                {name: b"short"}
                for name in (
                    "source_binding_sha256",
                    "request_identity_sha256",
                    "raw_manifest_sha256",
                    "processing_policy_sha256",
                    "producer_token_sha256",
                )
            ]
            + [
                {"encoding_contract": "other/v1"},
                {"part_count": -1},
                {"part_count": 131_073},
                {"record_count": -1},
                {"byte_count": -1},
                {"manifest_byte_count": -1},
                {"state": "other"},
                {"canonical_processing_policy": ""},
                {"canonical_progress": ""},
                {"canonical_progress": "x" * (2 * 1024 * 1024 + 1)},
                {"canonical_processing_policy": "x" * (1024 * 1024 + 1)},
                {"canonical_summary": "{}"},
                {"prepared_sha256": b"s" * 32},
                {"sealed_at": dt.datetime(2030, 1, 1, tzinfo=dt.UTC)},
            ]
        )
        for changes_by_field in invalid_by_field:
            await _assert_rejected(
                connection, CustomImportPreparedSource, _header_values(execution_id=4, **changes_by_field)
            )
        for name in ("canonical_summary", "prepared_sha256", "sealed_at"):
            invalid = _sealed_values(execution_id=4)
            invalid[name] = None
            await _assert_rejected(connection, CustomImportPreparedSource, invalid)
        for digest in (b"short", b"s" * 32):
            invalid = _sealed_values(execution_id=4)
            invalid["prepared_sha256"] = digest
            await _assert_rejected(connection, CustomImportPreparedSource, invalid)
        await _assert_rejected(connection, CustomImportPreparedSource, _header_values(), "23505")
        await _assert_rejected(connection, CustomImportPreparedSource, _header_values(execution_id=0))
        assert await connection.scalar(select(func.count()).select_from(CustomImportPreparedSource)) == 3


async def test_native_parts_reject_positions_payload_manifest_hashes_and_noncanonical_empty_parts():
    async with _prepared_case() as case, case.engine.begin() as connection:
        await connection.execute(insert(CustomImportPreparedSourcePart), _part_values())
        empty = _part_values(
            stream_slot=2, record_count=0, byte_count=0, payload=b"", payload_sha256=hashlib.sha256(b"").digest()
        )
        await connection.execute(insert(CustomImportPreparedSourcePart), empty)
        invalid_changes = [
            {"dataset_id": 0},
            {"definition_revision_id": 0},
            {"schema_revision_id": 0},
            {"stream_slot": 0},
            {"stream_slot": 10},
            {"part_ordinal": 0},
            {"part_ordinal": 131_073},
            {"first_source_ordinal": 0},
            {"record_count": -1},
            {"record_count": 1_000_001},
            {"payload_contract": "other/v1"},
            {"byte_count": -1},
            {"byte_count": MAX_PREPARED_PART_BYTES + 1},
            {"byte_count": 1},
            {"payload": b"changed"},
            {"payload_sha256": b"short"},
            {"payload_sha256": b"p" * 32},
            {"canonical_manifest": ""},
            {"canonical_manifest": "[]"},
            {"manifest_sha256": b"short"},
            {"manifest_sha256": b"m" * 32},
        ]
        for changes_by_field in invalid_changes:
            await _assert_rejected(
                connection, CustomImportPreparedSourcePart, _part_values(execution_id=2, **changes_by_field)
            )
        for changes_by_field in (
            {"part_ordinal": 2},
            {"first_source_ordinal": 2},
            {"byte_count": 1, "payload": b"x", "payload_sha256": hashlib.sha256(b"x").digest()},
        ):
            await _assert_rejected(
                connection, CustomImportPreparedSourcePart, empty | changes_by_field | {"execution_id": 2}
            )
        await _assert_rejected(connection, CustomImportPreparedSourcePart, _part_values(), "23505")
        await _assert_rejected(connection, CustomImportPreparedSourcePart, _part_values(execution_id=0))
        assert await connection.scalar(select(func.count()).select_from(CustomImportPreparedSourcePart)) == 2


async def _binding_seed(case):
    async with case.sessions() as session, session.begin():
        return await register_definition(session, "synthetic_prepared_" + uuid.uuid4().hex[:16], _definition())


def _binding_values(seed, contract="custom-import/source-binding/v3", connector="https", **changes):
    canonical = '{"synthetic":true}'
    definition = _definition()
    return (
        dict(
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            revision_number=1,
            binding_contract=contract,
            connector_kind=connector,
            definition_sha256=bytes.fromhex(definition.digest),
            schema_sha256=bytes.fromhex(definition.schema_digest),
            source_object_fingerprint_sha256=b"f" * 32,
            source_object_version="v1",
            canonical_binding=canonical,
            binding_sha256=hashlib.sha256((contract + ":" + canonical).encode()).digest(),
        )
        | changes
    )


async def _assert_binding_guards(connection, seed, other_seed, contract, connector):
    for name in ("definition_sha256", "schema_sha256", "source_object_fingerprint_sha256", "binding_sha256"):
        await _assert_rejected(
            connection,
            CustomImportSourceBindingRevision,
            _binding_values(seed, contract, connector, **{name: b"short"}),
        )
    for changes_by_field in (
        {"binding_sha256": b"h" * 32},
        {"revision_number": 0},
        {"source_object_version": "bad value"},
        {"canonical_binding": ""},
    ):
        await _assert_rejected(
            connection,
            CustomImportSourceBindingRevision,
            _binding_values(seed, contract, connector, **changes_by_field),
        )
    for name in ("dataset_id", "definition_revision_id", "schema_revision_id"):
        await _assert_rejected(
            connection,
            CustomImportSourceBindingRevision,
            _binding_values(seed, contract, connector, **{name: getattr(other_seed, name)}),
            "23503",
        )


async def test_binding_version_connector_pairing_keeps_digest_and_owner_guards():
    async with _prepared_case() as case, case.engine.begin() as connection:
        seed = await _binding_seed(case)
        other_seed = await _binding_seed(case)
        for version, connector in product(
            ("v1", "v2", "v3", "v4"), ("snowflake_bundle", "https", "s3", "upload", "other")
        ):
            contract = "custom-import/source-binding/" + version
            binding_values = _binding_values(seed, contract, connector)
            is_admitted = (version in ("v1", "v2") and connector == "snowflake_bundle") or (
                version == "v3" and connector in ("https", "s3", "upload")
            )
            async with connection.begin_nested() as trial:
                if is_admitted:
                    await connection.execute(insert(CustomImportSourceBindingRevision), binding_values)
                else:
                    await _assert_rejected(connection, CustomImportSourceBindingRevision, binding_values)
                await trial.rollback()
            if is_admitted:
                await _assert_binding_guards(connection, seed, other_seed, contract, connector)
        for revision, version in enumerate(("v1", "v2"), 1):
            await connection.execute(
                insert(CustomImportSourceBindingRevision),
                _binding_values(
                    seed, "custom-import/source-binding/" + version, "snowflake_bundle", revision_number=revision
                ),
            )
        await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "downgrade")
        assert await connection.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision)) == 2
        await _assert_rejected(connection, CustomImportSourceBindingRevision, _binding_values(seed, revision_number=3))


async def _drop_acl_roles(connection, schema_name, role_names):
    quote = connection.dialect.identifier_preparer.quote
    named_roles = ",".join(quote(role) for role in role_names)
    await connection.execute(
        text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema_name}" REVOKE ALL ON TABLES FROM PUBLIC,{named_roles}')
    )
    for role in reversed(role_names):
        await connection.execute(text(f"DROP OWNED BY {quote(role)}"))
        await connection.execute(text(f"DROP ROLE {quote(role)}"))


async def _assert_default_acl_probe(connection, schema_name, role_names):
    relation = f'"{schema_name}".synthetic_prepared_acl_probe'
    await connection.execute(text(f"CREATE TABLE {relation}(id integer)"))
    grantees = set(
        await connection.scalars(
            text(
                "SELECT acl.grantee FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) acl WHERE c.oid=to_regclass(:relation) AND acl.privilege_type='INSERT'"
            ),
            {"relation": relation},
        )
    )
    assert 0 in grantees
    for role in role_names:
        role_id = await connection.scalar(text("SELECT oid FROM pg_roles WHERE rolname=:role"), {"role": role})
        assert role_id in grantees
    await connection.execute(text(f"DROP TABLE {relation}"))


@asynccontextmanager
async def _hostile_default_acl_case():
    async with _legacy_case() as case:
        created_roles = []
        try:
            role_names = (
                "synthetic_prepared_acl_" + uuid.uuid4().hex[:12],
                'synthetic odd"role_' + uuid.uuid4().hex[:12],
            )
            async with case.engine.begin() as connection:
                quote = connection.dialect.identifier_preparer.quote
                for role in role_names:
                    await connection.execute(text(f"CREATE ROLE {quote(role)} NOLOGIN NOINHERIT"))
            created_roles.extend(role_names)
            async with case.engine.begin() as connection:
                quote = connection.dialect.identifier_preparer.quote
                named_roles = ",".join(quote(role) for role in role_names)
                await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO {named_roles}'))
                await connection.execute(
                    text(
                        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{case.schema_name}" GRANT ALL ON TABLES TO PUBLIC,{named_roles}'
                    )
                )
                await connection.execute(
                    text(f'GRANT SELECT ON "{case.schema_name}".custom_import_dataset TO {quote(role_names[0])}')
                )
                await _assert_default_acl_probe(connection, case.schema_name, role_names)
                before = await _legacy_catalog(connection, case.schema_name)
                await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "upgrade")
                assert await _legacy_catalog(connection, case.schema_name) == before
                await _assert_default_acl_probe(connection, case.schema_name, role_names)
            yield case, role_names
        finally:
            if created_roles:
                async with case.engine.begin() as connection:
                    await _drop_acl_roles(connection, case.schema_name, created_roles)


async def _assert_nonowner_access_denied(connection, role, relation):
    quote = connection.dialect.identifier_preparer.quote
    await connection.execute(text(f"SET LOCAL ROLE {quote(role)}"))
    try:
        for statement in (
            f"SELECT * FROM {relation}",
            f"INSERT INTO {relation} DEFAULT VALUES",
            f"UPDATE {relation} SET byte_count=0",
            f"DELETE FROM {relation}",
            f"TRUNCATE {relation}",
        ):
            with pytest.raises(DBAPIError) as denied:
                async with connection.begin_nested():
                    await connection.execute(text(statement))
            assert denied.value.orig.sqlstate == "42501"
    finally:
        await connection.execute(text("RESET ROLE"))


async def test_upgrade_closes_public_and_named_default_acl_without_changing_legacy_grants():
    async with _hostile_default_acl_case() as (case, role_names), case.engine.begin() as connection:
        for table in _TABLES:
            relation = f'"{case.schema_name}".{table}'
            assert (
                await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) acl WHERE c.oid=to_regclass(:relation) AND acl.grantee<>c.relowner"
                    ),
                    {"relation": relation},
                )
                == 0
            )
            for role in role_names:
                assert not await connection.scalar(
                    text(
                        "SELECT has_table_privilege(:role,:relation,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')"
                    ),
                    {"role": role, "relation": relation},
                )
                await _assert_nonowner_access_denied(connection, role, relation)


def _retained_row(retained, seed=None, connector="https"):
    if retained == "binding":
        return CustomImportSourceBindingRevision, _binding_values(seed, connector=connector)
    if retained == "empty_part":
        return CustomImportPreparedSourcePart, _part_values(
            record_count=0, byte_count=0, payload=b"", payload_sha256=hashlib.sha256(b"").digest()
        )
    if retained == "part":
        return CustomImportPreparedSourcePart, _part_values()
    state = "pending" if retained == "header" else retained
    return CustomImportPreparedSource, _sealed_values() if state == "sealed" else _header_values(state=state)


@pytest.mark.parametrize(
    "retained,connector",
    [
        ("pending", None),
        ("closed", None),
        ("sealed", None),
        ("part", None),
        ("empty_part", None),
        ("binding", "https"),
        ("binding", "s3"),
        ("binding", "upload"),
    ],
)
async def test_downgrade_refuses_every_retained_state_and_preserves_payloads(retained, connector):
    async with _prepared_case() as case:
        seed = await _binding_seed(case) if retained == "binding" else None
        async with case.engine.begin() as connection:
            model, values = _retained_row(retained, seed, connector)
            await connection.execute(insert(model), values)
            with pytest.raises(RuntimeError, match="custom_import_prepared_source_downgrade_blocked"):
                async with connection.begin_nested():
                    await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "downgrade")
            assert await connection.scalar(select(func.count()).select_from(model)) == 1
            for table in _TABLES:
                assert (
                    await connection.scalar(
                        text("SELECT to_regclass(:relation)"), {"relation": f"{case.schema_name}.{table}"}
                    )
                    is not None
                )


async def test_migrations_preserve_statement_budget_and_stricter_lock_waits():
    async with _prepared_case() as case, case.engine.begin() as connection:
        for initial, expected in (("1ms", "1ms"), ("250ms", "250ms"), ("0", "2s"), ("10s", "2s")):
            await connection.execute(text("SELECT set_config('lock_timeout',:timeout,true)"), {"timeout": initial})
            await connection.execute(text("SET LOCAL statement_timeout='5s'"))
            for operation in ("downgrade", "upgrade"):
                await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, operation)
                assert await connection.scalar(text("SHOW lock_timeout")) == expected
                assert await connection.scalar(text("SHOW statement_timeout")) == "5s"
                assert await connection.scalar(text("SHOW transaction_isolation")) == "read committed"


async def test_upgrade_lock_budget_failure_rolls_back_both_new_tables():
    async with _legacy_case() as case, case.engine.begin() as holder:
        await holder.execute(text(f'LOCK TABLE "{case.schema_name}".{_BINDING_TABLE} IN SHARE MODE'))
        async with case.engine.begin() as connection:
            before = await _legacy_catalog(connection, case.schema_name)
            await connection.execute(text("SET LOCAL lock_timeout='250ms'"))
            await connection.execute(text("SET LOCAL statement_timeout='5s'"))
            with pytest.raises(DBAPIError) as rejected:
                async with connection.begin_nested():
                    await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "upgrade")
            assert rejected.value.orig.sqlstate == "55P03"
            assert await connection.scalar(text("SHOW lock_timeout")) == "250ms"
            assert await connection.scalar(text("SHOW statement_timeout")) == "5s"
            assert await _legacy_catalog(connection, case.schema_name) == before
            for table in _TABLES:
                assert (
                    await connection.scalar(
                        text("SELECT to_regclass(:relation)"), {"relation": f"{case.schema_name}.{table}"}
                    )
                    is None
                )


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_downgrade_refuses_stale_isolation_without_removing_empty_tables(isolation):
    async with _prepared_case() as case, case.engine.begin() as connection:
        await connection.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
        with pytest.raises(RuntimeError, match="custom_import_prepared_source_downgrade_isolation"):
            await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "downgrade")
        assert await connection.scalar(text("SHOW transaction_isolation")) == isolation.lower()
        assert await connection.scalar(select(func.count()).select_from(CustomImportPreparedSource)) == 0


async def _attempt_downgrade(case):
    async with case.engine.begin() as connection:
        await connection.run_sync(_run_migration, case.schema_name, _MIGRATION, "downgrade")


async def _wait_for_downgrade_lock(case, table):
    async with asyncio.timeout(1), case.engine.connect() as connection:
        while not await connection.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation=to_regclass(:relation) AND mode='AccessExclusiveLock' AND NOT granted)"
            ),
            {"relation": f"{case.schema_name}.{table}"},
        ):
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("retained", ["header", "part", "binding"])
async def test_downgrade_waits_for_writer_and_refuses_its_newly_committed_retention(retained):
    async with _prepared_case() as case:
        seed = await _binding_seed(case) if retained == "binding" else None
        async with case.engine.connect() as writer:
            transaction = await writer.begin()
            downgrade_task = None
            try:
                model, values = _retained_row(retained, seed)
                await writer.execute(insert(model), values)
                downgrade_task = asyncio.create_task(_attempt_downgrade(case))
                await _wait_for_downgrade_lock(case, model.__tablename__)
                await transaction.commit()
                with pytest.raises(RuntimeError, match="custom_import_prepared_source_downgrade_blocked"):
                    await asyncio.wait_for(downgrade_task, 3)
                assert await writer.scalar(select(func.count()).select_from(model)) == 1
            finally:
                if transaction.is_active:
                    await transaction.rollback()
                if downgrade_task is not None:
                    downgrade_task.cancel()
                    await asyncio.gather(downgrade_task, return_exceptions=True)
