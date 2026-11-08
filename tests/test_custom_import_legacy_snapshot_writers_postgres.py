# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real legacy registration, candidate writes, retained origins and closure."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from db.models import custom_import as models
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.publication import seal_generation
from process.custom_import.runner_codec import pack_hash
from tests import custom_import_grouped_child_support as fixture
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_runner_postgres as runner_fixture
from tests import test_custom_import_snapshot_reads_postgres as base_fixture
from tests.custom_import_postgres_support import (
    _migration,
    isolated_publication_case,
    lease_digest,
    seed_running_generation,
)

pytestmark = pytest.mark.asyncio
_ROOT = Path(__file__).resolve().parents[1]
_MIGRATIONS = (
    "20261005040000_custom_import_bulk_snapshot_writers",
    "20261005050000_custom_import_legacy_snapshot_writers",
    "20261005060000_custom_import_snapshot_finality",
    "20261005070000_custom_import_materialization_storage",
    "20261005080000_custom_import_writer_cutover",
)


def _install(connection, schema):
    for name in _MIGRATIONS:
        migration = _migration(_ROOT / "alembic/versions" / f"{name}.py", f"legacy_native_{name}")
        migration._schema = lambda: schema
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()


async def _sealed_legacy_base(case):
    """Commit a codec-complete canonical BASE before the all-writer cutover."""
    async with case.sessions() as session, session.begin():
        seed = await runner_fixture._seed_identity(session, uuid.uuid4().hex, fixture.definition())
        graph = read_fixture._publication_graph(seed)
        base = await seed_running_generation(
            session,
            graph,
            suffix="legacy_base",
            base_generation_id=None,
            root_count=len(base_fixture._families()),
            family_count=len(base_fixture._families()),
        )
        await base_fixture._seed_material(session, seed, base)
        await seal_generation(
            session,
            dataset_id=graph.dataset_id,
            generation_id=base.generation_id,
            lease_fence=base.fence,
            lease_token=base.token,
        )
    return graph, base


@asynccontextmanager
async def _legacy_case():
    """Seal real canonical fixture data before the actual all-writer cutover."""
    async with isolated_publication_case(migration_through="20261005030000") as case:
        try:
            graph, base = await _sealed_legacy_base(case)
            async with case.engine.begin() as connection:
                await connection.run_sync(_install, case.schema_name)
            async with case.sessions() as session, session.begin():
                attempt = await seed_running_generation(
                    session,
                    graph,
                    suffix="legacy_candidate",
                    base_generation_id=base.generation_id,
                    root_count=1,
                    family_count=1,
                )
                membership = (
                    await session.scalars(
                        select(models.CustomImportGenerationFamily)
                        .where(models.CustomImportGenerationFamily.generation_id == base.generation_id)
                        .order_by(models.CustomImportGenerationFamily.root_record_id)
                        .limit(1)
                    )
                ).one()
                family = await session.get(models.CustomImportFamilyRevision, membership.family_revision_id)
                root = await session.get(models.CustomImportRootRevision, family.root_revision_id)
                root_record = await session.get(models.CustomImportRootRecord, family.root_record_id)
                entity = await session.get(models.CustomImportEntityBinding, family.entity_binding_id)
                children = (
                    await session.scalars(
                        select(models.CustomImportChildRevision)
                        .join(
                            models.CustomImportFamilyChild,
                            models.CustomImportFamilyChild.child_revision_id
                            == models.CustomImportChildRevision.child_revision_id,
                        )
                        .where(models.CustomImportFamilyChild.family_revision_id == family.family_revision_id)
                        .order_by(models.CustomImportChildRevision.child_revision_id)
                    )
                ).all()
            yield SimpleNamespace(
                case=case,
                graph=graph,
                base=base,
                attempt=attempt,
                family=family,
                root=root,
                record=root_record,
                entity=entity,
                children=children,
            )
        finally:
            async with case.engine.begin() as connection:
                ids = (await connection.execute(select(CustomImportSnapshotFamily.family_id))).scalars().all()
                for family_id in ids:
                    await connection.execute(text(f'DROP SCHEMA "ci_snapshot_{family_id}" CASCADE'))


async def _call(session, case, name, arguments):
    await session.execute(text("SET LOCAL statement_timeout='1000ms'"))
    parameter_by_name = {f"p{index}": identity_value for index, (_type, identity_value) in enumerate(arguments)}
    casts = ",".join(f"CAST(:p{index} AS {sql_type})" for index, (sql_type, _value) in enumerate(arguments))
    return await session.scalar(text(f'SELECT "{case.schema_name}".{name}({casts})'), parameter_by_name)


async def _register(session, legacy):
    return await _call(
        session,
        legacy.case,
        "resolve_custom_import_legacy_generation_snapshot",
        (("bigint", legacy.attempt.generation_id),),
    )


async def _authority(session, legacy):
    lease = await session.get(models.CustomImportLease, legacy.attempt.execution_id)
    return tuple(
        ("bigint", identity_value)
        for identity_value in (
            legacy.graph.dataset_id,
            legacy.graph.definition_revision_id,
            legacy.graph.schema_revision_id,
            legacy.attempt.execution_id,
            legacy.graph.capture_bundle_id,
            legacy.attempt.fence,
        )
    ) + (("bytea", lease_digest(legacy.attempt.token)), ("timestamptz", lease.expires_at))


async def _append(session, legacy, retained):
    """Exercise the protected entry points, not owner INSERTs into candidate leaves."""
    family_id = await _register(session, legacy)
    authority = await _authority(session, legacy)
    packs = await _append_packs(session, legacy, authority)
    identity_args = authority + (
        ("bytea[]", [bytes(legacy.record.key_contract_sha256)]),
        ("text[]", [None]),
        ("bytea[]", [bytes(legacy.record.logical_key_sha256)]),
        ("text[]", [legacy.entity.canonical_value]),
        ("bytea[]", [bytes(legacy.entity.value_sha256)]),
        ("bigint[]", [legacy.record.root_record_id if retained else None]),
        ("bigint[]", [legacy.entity.entity_binding_id if retained else None]),
        ("text", legacy.record.canonical_logical_key),
    )
    root_id, entity_id = await _call(session, legacy.case, "persist_custom_import_legacy_identity_set", identity_args)
    root_args = authority + (
        ("bigint[]", [None]),
        ("bigint[]", [None]),
        ("bigint[]", [root_id]),
        ("bigint[]", [entity_id]),
        ("bigint[]", [packs[0]]),
        ("bigint[]", [0]),
        ("text[]", [None]),
        ("bytea[]", [bytes(legacy.root.payload_sha256)]),
        ("bytea[]", [bytes(legacy.family.family_sha256)]),
        ("bigint[]", [len(legacy.children)]),
        ("bigint[]", [legacy.family.family_revision_id if retained else None]),
        ("text", legacy.root.canonical_payload),
    )
    root_revision, family_revision = await _call(
        session, legacy.case, "persist_custom_import_legacy_family_root_set", root_args
    )
    children, child_args = await _append_children(
        session, legacy, retained, authority, packs[1], root_id, family_revision
    )
    root_args = root_args[:8] + (("bigint[]", [root_revision]), ("bigint[]", [family_revision])) + root_args[10:]
    return SimpleNamespace(
        family_id=family_id,
        root_id=root_id,
        root_revision=root_revision,
        family_revision=family_revision,
        children=children,
        identity_args=identity_args,
        root_args=root_args,
        child_args=child_args,
    )


async def _append_packs(session, legacy, authority):
    return await _call(
        session,
        legacy.case,
        "persist_custom_import_legacy_pack_set",
        authority
        + (
            ("smallint[]", [1, 2]),
            ("integer[]", [0, 0]),
            ("bigint[]", [1, len(legacy.children)]),
            (
                "bytea[]",
                [
                    pack_hash("root", [bytes(legacy.root.payload_sha256)]),
                    pack_hash("rates", [bytes(child.payload_sha256) for child in legacy.children]),
                ],
            ),
        ),
    )


async def _append_children(session, legacy, retained, authority, pack_id, root_id, family_revision):
    """Append one real child page and retain its exact replay arguments."""
    count = len(legacy.children)
    child_args = authority + (
        ("bigint[]", [None] * count),
        ("bigint[]", [family_revision] * count),
        ("bigint[]", [root_id] * count),
        ("smallint[]", [child.collection_slot for child in legacy.children]),
        ("bigint[]", [pack_id] * count),
        ("bigint[]", list(range(count))),
        ("text[]", [child.canonical_parent_key for child in legacy.children]),
        ("bytea[]", [bytes(child.parent_key_sha256) for child in legacy.children]),
        ("text[]", [child.canonical_child_key for child in legacy.children]),
        ("bytea[]", [bytes(child.child_key_sha256) for child in legacy.children]),
        ("text[]", [child.canonical_payload for child in legacy.children]),
        ("bytea[]", [bytes(child.payload_sha256) for child in legacy.children]),
        ("bigint[]", [legacy.family.family_revision_id if retained else None] * count),
        ("bigint[]", [child.child_revision_id if retained else None for child in legacy.children]),
        ("text", None),
        ("text", None),
        ("text", None),
    )
    children = await _call(session, legacy.case, "persist_custom_import_legacy_child_set", child_args)
    child_args = child_args[:8] + (("bigint[]", children),) + child_args[9:]
    return children, child_args


def _expected_origins(legacy, copied, retained):
    if not retained:
        return []
    root_origin = (
        "root",
        copied.root_revision,
        copied.family_revision,
        copied.root_id,
        copied.root_revision,
        None,
        legacy.base.generation_id,
        legacy.family.family_revision_id,
        legacy.root.root_revision_id,
        None,
    )
    child_origins = [
        (
            "child",
            fresh,
            copied.family_revision,
            copied.root_id,
            copied.root_revision,
            base.collection_slot,
            legacy.base.generation_id,
            legacy.family.family_revision_id,
            legacy.root.root_revision_id,
            base.child_revision_id,
        )
        for fresh, base in zip(copied.children, legacy.children, strict=True)
    ]
    return [root_origin, *child_origins]


async def _attach_and_freeze(session, legacy, copied):
    generation = await session.get(models.CustomImportGeneration, legacy.attempt.generation_id)
    membership = tuple(
        ("bigint", identity_value)
        for identity_value in (
            generation.generation_id,
            generation.dataset_id,
            generation.definition_revision_id,
            generation.schema_revision_id,
            generation.execution_id,
            generation.capture_bundle_id,
            generation.producing_fence,
        )
    ) + (
        ("bytea", bytes(generation.producing_token_sha256)),
        ("bigint[]", [copied.root_id]),
        ("bigint[]", [copied.family_revision]),
        ("bigint[]", None),
        ("bytea", None),
        ("timestamptz", None),
    )
    assert await _call(session, legacy.case, "persist_custom_import_legacy_generation_family_set", membership) == 1
    authority = await _authority(session, legacy)
    assert (
        await _call(
            session,
            legacy.case,
            "freeze_custom_import_snapshot_family",
            (authority[3], authority[5], authority[6]),
        )
        == copied.family_id
    )


async def test_native_legacy_registration_is_exact_idempotent_and_default_denied():
    async with _legacy_case() as legacy:
        async with legacy.case.sessions() as session, session.begin():
            family_id = await _register(session, legacy)
            assert await _register(session, legacy) == family_id
            family = await session.get(CustomImportSnapshotFamily, family_id)
            assert family.generation_id == legacy.attempt.generation_id and family.frozen_at is None
            assert family.origin_table_oid and family.origin_table_owner and len(family.origin_columns_sha256) == 32
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM pg_constraint WHERE conrelid=CAST(:oid AS oid) AND contype='f'"),
                    {"oid": family.origin_table_oid},
                )
                == 0
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM pg_trigger WHERE tgrelid=CAST(:oid AS oid) AND NOT tgisinternal"),
                    {"oid": family.origin_table_oid},
                )
                == 0
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace,"
                        "LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a "
                        "WHERE n.nspname=:schema AND p.proname='resolve_custom_import_legacy_generation_snapshot' "
                        "AND a.grantee<>p.proowner"
                    ),
                    {"schema": legacy.case.schema_name},
                )
                == 0
            )


@pytest.mark.parametrize("retained", [False, True])
async def test_native_legacy_pages_register_fresh_homes_and_only_exact_retained_origins(retained):
    """Real fresh IDs, provenance, idempotent replay and closed candidate writes."""
    async with _legacy_case() as legacy:
        async with legacy.case.sessions() as session, session.begin():
            copied = await _append(session, legacy, retained)
            assert copied.root_revision != legacy.root.root_revision_id
            assert set(copied.children).isdisjoint(child.child_revision_id for child in legacy.children)
            assert await _call(
                session, legacy.case, "persist_custom_import_legacy_identity_set", copied.identity_args
            ) == [
                copied.root_id,
                legacy.entity.entity_binding_id,
            ]
            assert await _call(
                session, legacy.case, "persist_custom_import_legacy_family_root_set", copied.root_args
            ) == [
                copied.root_revision,
                copied.family_revision,
            ]
            assert (
                await _call(session, legacy.case, "persist_custom_import_legacy_child_set", copied.child_args)
                == copied.children
            )
            origins = (
                await session.execute(
                    text(
                        f'SELECT * FROM "ci_snapshot_{copied.family_id}".legacy_copy_origin ORDER BY kind DESC,revision_id'
                    )
                )
            ).all()
            assert [tuple(origin_row) for origin_row in origins] == _expected_origins(legacy, copied, retained)
            assert await session.scalar(
                text(
                    f'SELECT count(*) FROM "{legacy.case.schema_name}".lookup_custom_import_revision_home'
                    "(CAST(:roots AS bigint[]),CAST(:children AS bigint[])) WHERE family_id=:family"
                ),
                {"roots": [copied.root_revision], "children": copied.children, "family": copied.family_id},
            ) == 1 + len(legacy.children)
            await _attach_and_freeze(session, legacy, copied)
        with pytest.raises(DBAPIError, match="snapshot_binding_mismatch|writes_closed"):
            async with legacy.case.sessions() as session, session.begin():
                await _call(session, legacy.case, "persist_custom_import_legacy_family_root_set", copied.root_args)
        async with legacy.case.sessions() as session, session.begin():
            family = await session.get(CustomImportSnapshotFamily, copied.family_id)
            assert family.frozen_at is not None
            assert await session.get(models.CustomImportGenerationSeal, legacy.attempt.generation_id) is None


async def test_native_legacy_origin_replay_cannot_erase_retained_provenance():
    async with _legacy_case() as legacy:
        async with legacy.case.sessions() as session, session.begin():
            copied = await _append(session, legacy, True)
        altered = copied.root_args[:18] + (("bigint[]", [None]),) + copied.root_args[19:]
        with pytest.raises(DBAPIError, match="origin_replay_mismatch"):
            async with legacy.case.sessions() as session, session.begin():
                await _call(session, legacy.case, "persist_custom_import_legacy_family_root_set", altered)
        async with legacy.case.sessions() as session, session.begin():
            assert await _call(
                session, legacy.case, "persist_custom_import_legacy_family_root_set", copied.root_args
            ) == [
                copied.root_revision,
                copied.family_revision,
            ]


async def test_native_legacy_origin_layout_tampering_and_stale_producer_are_rejected():
    async with _legacy_case() as legacy:
        async with legacy.case.sessions() as session, session.begin():
            family_id = await _register(session, legacy)
        with pytest.raises(DBAPIError, match="origin_mismatch"):
            async with legacy.case.sessions() as session, session.begin():
                await session.execute(
                    text(f'ALTER TABLE "ci_snapshot_{family_id}".legacy_copy_origin ADD COLUMN extra bigint')
                )
                await _register(session, legacy)
        bad = SimpleNamespace(**vars(legacy))
        bad.attempt = replace(legacy.attempt, fence=legacy.attempt.fence + 1)
        with pytest.raises(DBAPIError, match="authority_lost|attempt_lost"):
            async with legacy.case.sessions() as session, session.begin():
                authority = await _authority(session, bad)
                await _call(session, legacy.case, "check_custom_import_materialization_authority", authority)


async def _rejection_receipt(session, legacy, family_id):
    """Read candidate IDs and the exact canonical sequence without advancing it."""

    rejection_ids_by_ordinal = dict(
        (
            await session.execute(
                text(
                    f'SELECT rejection_ordinal,rejection_id FROM "ci_snapshot_{family_id}".custom_import_rejection '
                    "WHERE execution_id=:execution AND producing_fence=:fence ORDER BY rejection_ordinal"
                ),
                {"execution": legacy.attempt.execution_id, "fence": legacy.attempt.fence},
            )
        ).all()
    )
    sequence = (
        await session.execute(
            text(
                f'SELECT last_value,is_called FROM "{legacy.case.schema_name}".custom_import_rejection_rejection_id_seq'
            )
        )
    ).one()
    return rejection_ids_by_ordinal, tuple(sequence)


async def test_native_legacy_rejection_replay_preserves_global_ids_and_sequence_state():
    """Committed replay allocates nothing; a mixed page allocates only its new ordinal."""

    async with _legacy_case() as legacy:
        async with legacy.case.sessions() as session, session.begin():
            family_id = await _register(session, legacy)
            before_ids, (initial_value, initially_called) = await _rejection_receipt(session, legacy, family_id)
            assert before_ids == {}
        receipts = []
        for ordinals in ((1, 0), (1, 0), (2, 0), (2, 0)):
            async with legacy.case.sessions() as session, session.begin():
                arguments = await _authority(session, legacy) + (
                    ("bigint[]", ordinals),
                    ("text[]", (None, None)),
                    ("bytea[]", (None, None)),
                    ("text[]", ("root_key_invalid", "root_key_invalid")),
                    (
                        "text[]",
                        ('{"code":"root_key_invalid","contract":"custom-import-rejection/v1","root_key_sha256":null}',)
                        * 2,
                    ),
                    ("text", None),
                )
                assert await _call(session, legacy.case, "persist_custom_import_legacy_rejection_set", arguments) == 2
                receipts.append(await _rejection_receipt(session, legacy, family_id))
        fresh, replayed, mixed, mixed_replayed = receipts
        first_id = initial_value + int(initially_called)
        assert fresh == ({0: first_id, 1: first_id + 1}, (first_id + 1, True))
        assert replayed == fresh
        assert mixed == ({0: first_id, 1: first_id + 1, 2: first_id + 2}, (first_id + 2, True))
        assert mixed_replayed == mixed
