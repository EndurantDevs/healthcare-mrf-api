# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real read-only snapshot resolution and selected-family read parity.

Canonical fixtures are copied before the protected bind/freeze and real seal.
Fixture-owner inserts are setup, not a test of a protected bulk writer.
Current-chain snapshots install origin metadata and serving indexes before
finality. No canonical mutation guard is disabled or bypassed.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from db.models import custom_import as models
from process.custom_import import materialization as materialize
from process.custom_import import read_core, read_identity, runner_codec
from process.custom_import.family import RootFamily
from process.custom_import.publication import activate_generation, seal_generation
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_schema
from tests import custom_import_grouped_child_support as fixture
from tests import test_custom_import_grouped_child_read_postgres as child_fixture
from tests import test_custom_import_provider_hydration_postgres as native
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_runner_postgres as runner_fixture
from tests.custom_import_postgres_support import (
    FamilyMaterialSpec,
    attach_generation_family,
    isolated_publication_case,
    lease_digest,
    seed_family_material,
    seed_running_generation,
)
from tests.test_custom_import_snapshot_storage_postgres import _call

pytestmark = pytest.mark.asyncio


def _families():
    children = child_fixture._children()
    return tuple(
        RootFamily(
            (root["npi"], root["period"], root["segment"]),
            root,
            {
                "rates": tuple(
                    child
                    for child in children
                    if (child["rate_npi"], child["rate_period"], child["rate_segment"])
                    == (root["npi"], root["period"], root["segment"])
                )
            },
        )
        for root in native._roots()
    )


def _identity_by_column(seed):
    return dict(
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
    )


async def _seed_stream_packs(session, seed, attempt, families):
    """Assign the complete root/child pack hashes before inserting material."""

    packs_by_label = {}
    for slot, label, hashes in (
        (1, "root", [runner_codec.root_payload_hash(seed.definition, family) for family in families]),
        (
            2,
            "rates",
            [
                runner_codec.child_payload_hash(seed.definition, "rates", child)
                for family in families
                for child in family.children["rates"]
            ],
        ),
    ):
        packs_by_label[label] = models.CustomImportPack(
            **_identity_by_column(seed),
            execution_id=attempt.execution_id,
            capture_bundle_id=seed.capture_bundle_id,
            stream_slot=slot,
            pack_ordinal=0,
            record_count=len(hashes),
            pack_sha256=runner_codec.pack_hash(label, hashes),
            producing_fence=attempt.fence,
            producing_token_sha256=lease_digest(attempt.token),
        )
        session.add(packs_by_label[label])
    session.add_all(
        materialize.selection_profile_models(
            seed.definition,
            identity=materialize.DefinitionIdentity(**_identity_by_column(seed)),
            child_collection_slots={"rates": 1},
        )
    )
    await session.flush()
    return packs_by_label


async def _seed_root_identity(session, seed, family, bindings_by_npi):
    npi = family.root["npi"]
    if npi not in bindings_by_npi:
        bindings_by_npi[npi] = models.CustomImportEntityBinding(
            dataset_id=seed.dataset_id,
            adapter_id="npi",
            canonical_value=npi,
            value_sha256=runner_codec.digest_text("entity:npi", npi),
        )
        session.add(bindings_by_npi[npi])
    root = models.CustomImportRootRecord(
        dataset_id=seed.dataset_id,
        key_contract_sha256=runner_codec.root_key_contract_hash(seed.definition),
        canonical_logical_key=runner_codec.root_key_document(seed.definition, family.root),
        logical_key_sha256=runner_codec.root_key_hash(seed.definition, family.root),
    )
    session.add(root)
    await session.flush()
    return root, bindings_by_npi[npi]


def _add_root_projections(session, seed, attempt, family, root, family_model):
    session.add(
        models.CustomImportGenerationFamily(
            **_identity_by_column(seed),
            generation_id=attempt.generation_id,
            root_record_id=root.root_record_id,
            family_revision_id=family_model.family_revision_id,
        )
    )
    root_scalars = materialize.project_root_scalars(
        seed.definition,
        root_target=materialize.RootScalarTarget(
            seed.dataset_id, seed.schema_revision_id, root.root_record_id, family_model.root_revision_id
        ),
        root_values=family.root,
    )
    session.add_all(materialize.scalar_projection_models(seed.definition, root_scalars=root_scalars))


async def _seed_root_family(session, seed, attempt, family, root_pack, source_ordinal, bindings_by_npi):
    root, binding = await _seed_root_identity(session, seed, family, bindings_by_npi)
    revision = models.CustomImportRootRevision(
        **_identity_by_column(seed),
        root_record_id=root.root_record_id,
        pack_id=root_pack.pack_id,
        source_ordinal=source_ordinal,
        canonical_payload=runner_codec.record_payload(seed.definition.root_fields, family.root),
        payload_sha256=runner_codec.root_payload_hash(seed.definition, family),
    )
    session.add(revision)
    await session.flush()
    family_model = models.CustomImportFamilyRevision(
        dataset_id=seed.dataset_id,
        schema_revision_id=seed.schema_revision_id,
        root_record_id=root.root_record_id,
        root_revision_id=revision.root_revision_id,
        entity_binding_id=binding.entity_binding_id,
        family_sha256=runner_codec.new_family_hash(seed.definition, family),
        child_count=len(family.children["rates"]),
        producing_execution_id=attempt.execution_id,
        producing_fence=attempt.fence,
        producing_token_sha256=lease_digest(attempt.token),
    )
    session.add(family_model)
    await session.flush()
    _add_root_projections(session, seed, attempt, family, root, family_model)
    return root, family_model, binding


def _add_child_projections(session, seed, root, family_model, child_revision, child):
    session.add(
        models.CustomImportFamilyChild(
            dataset_id=seed.dataset_id,
            schema_revision_id=seed.schema_revision_id,
            root_record_id=root.root_record_id,
            family_revision_id=family_model.family_revision_id,
            collection_slot=1,
            child_revision_id=child_revision.child_revision_id,
        )
    )
    child_scalars = materialize.project_child_scalars(
        seed.definition,
        collection="rates",
        child_target=materialize.ChildScalarTarget(
            seed.dataset_id, seed.schema_revision_id, root.root_record_id, 1, child_revision.child_revision_id
        ),
        child_values=child,
        child_collection_slots={"rates": 1},
    )
    session.add_all(
        materialize.scalar_projection_models(
            seed.definition, child_scalars=child_scalars, child_collection_slots={"rates": 1}
        )
    )


async def _seed_family_children(session, seed, family, root, family_model, child_pack, first_ordinal):
    """Assign stream ordinals before INSERT; immutable revisions are never updated."""

    for source_ordinal, child in enumerate(family.children["rates"], start=first_ordinal):
        child_revision = models.CustomImportChildRevision(
            **_identity_by_column(seed),
            root_record_id=root.root_record_id,
            collection_slot=1,
            pack_id=child_pack.pack_id,
            source_ordinal=source_ordinal,
            canonical_parent_key=root.canonical_logical_key,
            parent_key_sha256=root.logical_key_sha256,
            canonical_child_key=runner_codec.child_key_document(seed.definition, "rates", child),
            child_key_sha256=runner_codec.child_key_hash(seed.definition, "rates", child),
            canonical_payload=runner_codec.record_payload(seed.definition.child_fields, child),
            payload_sha256=runner_codec.child_payload_hash(seed.definition, "rates", child),
        )
        session.add(child_revision)
        await session.flush()
        _add_child_projections(session, seed, root, family_model, child_revision, child)
    return first_ordinal + len(family.children["rates"])


async def _seed_material(session, seed, attempt):
    """Use production codecs/model builders without invoking a writer under test."""

    families = _families()
    packs_by_label = await _seed_stream_packs(session, seed, attempt, families)
    bindings_by_npi, candidates, roots = {}, [], []
    child_ordinal = 0
    for ordinal, family in enumerate(families):
        root, family_model, binding = await _seed_root_family(
            session, seed, attempt, family, packs_by_label["root"], ordinal, bindings_by_npi
        )
        child_ordinal = await _seed_family_children(
            session, seed, family, root, family_model, packs_by_label["rates"], child_ordinal
        )
        candidates.append(
            materialize.WinnerCandidate(
                entity_binding_id=binding.entity_binding_id,
                family_revision_id=family_model.family_revision_id,
                family_sha256=bytes(family_model.family_sha256),
                context_collection_slot=0,
                context_child_revision_id=None,
                context_child_key_sha256=None,
                values_by_field={field: family.root[field] for field in seed.definition.query.root_fields},
            )
        )
        roots.append(root)
    await session.flush()
    identity = materialize.GenerationIdentity(generation_id=attempt.generation_id, **_identity_by_column(seed))
    winners = materialize.materialize_winners(
        seed.definition,
        generation=identity,
        candidates=materialize.ValidatedWinnerCandidateStream(identity, tuple(candidates)),
        child_collection_slots={"rates": 1},
    )
    session.add_all(materialize.winner_materialization_models(winners))
    await session.flush()
    return roots[0], bindings_by_npi[native._A]


async def _grant_reader_access(connection, case, role):
    read_models = SNAPSHOT_MODELS + (
        models.CustomImportDataset,
        models.CustomImportDefinitionRevision,
        models.CustomImportSchemaRevision,
        models.CustomImportField,
        models.CustomImportChildCollection,
        models.CustomImportSelectionProfile,
        models.CustomImportGeneration,
        models.CustomImportGenerationSeal,
        models.CustomImportPublicationEvent,
    )
    await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{role}"'))
    for model in read_models:
        await connection.execute(text(f'GRANT SELECT ON "{case.schema_name}"."{model.__tablename__}" TO "{role}"'))
    await connection.execute(
        text(
            f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".resolve_custom_import_generation_snapshot'
            f'(bigint,bigint,bigint,bigint) TO "{role}"'
        )
    )


@asynccontextmanager
async def _reader_role(case):
    """Grant existing read rights before creation; registry copies only SELECT."""

    role = f"snapshot_reader_{uuid.uuid4().hex[:16]}"
    is_created = False
    try:
        async with case.engine.begin() as connection:
            await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            is_created = True
            await _grant_reader_access(connection, case, role)
        yield role
    finally:
        if is_created:
            async with case.engine.begin() as connection:
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


async def _copy_snapshot_material(session, case, attempt, family_id, *, install_legacy=False):
    """Copy fixture-owned rows before the protected bind and freeze operations."""

    namespace = snapshot_schema(family_id)
    for model in SNAPSHOT_MODELS:
        table = model.__tablename__
        await session.execute(text(f'INSERT INTO "{namespace}"."{table}" SELECT * FROM "{case.schema_name}"."{table}"'))
    await _call(session, case, attempt, "bind_custom_import_snapshot_generation", generation_id=attempt.generation_id)
    if install_legacy:
        assert (
            await session.scalar(
                text(f'SELECT "{case.schema_name}".install_custom_import_legacy_snapshot_writers(:family)'),
                {"family": family_id},
            )
            == family_id
        )
    await _call(session, case, attempt, "freeze_custom_import_snapshot_family")


async def _prepare_read_indexes(case, family_id):
    """Prepare the ten serving indexes through freshly fenced transactions."""

    for _ in range(10):
        async with case.sessions() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
            is_complete = await session.scalar(
                text(f"SELECT \"{case.schema_name}\".prepare_custom_import_snapshot_indexes(:family,'serving')"),
                {"family": family_id},
            )
            if is_complete is True:
                return
    pytest.fail("snapshot serving indexes did not complete")


async def _activate_read_generation(session, graph, attempt):
    await seal_generation(
        session,
        dataset_id=graph.dataset_id,
        generation_id=attempt.generation_id,
        lease_fence=attempt.fence,
        lease_token=attempt.token,
    )
    await activate_generation(
        session,
        dataset_id=graph.dataset_id,
        target_generation_id=attempt.generation_id,
        expected_generation_id=None,
        expected_pointer_version=0,
    )


async def _seed_canonical_decoy(case, graph, attempt, first_root, first_binding):
    """Keep a same-root/entity sibling from a different producer outside the seal."""

    async with case.sessions() as session, session.begin():
        decoy = await seed_running_generation(
            session,
            graph,
            suffix="canonical_decoy",
            base_generation_id=attempt.generation_id,
            root_count=1,
            family_count=1,
        )
        material = await seed_family_material(
            session,
            graph,
            decoy,
            FamilyMaterialSpec(
                suffix="canonical_decoy",
                child_keys=("foreign-only",),
                include_child_scalars=False,
                root_record_id=first_root.root_record_id,
                entity_binding_id=first_binding.entity_binding_id,
            ),
        )
        await attach_generation_family(session, graph, decoy, material)


@asynccontextmanager
async def _pinned_read_case(*, snapshot=True):
    """Publish complete synthetic material; always clean the leaf schema and role."""

    async with isolated_publication_case() as case, _reader_role(case) as role:
        family_id = None
        try:
            async with case.sessions() as session, session.begin():
                seed = await runner_fixture._seed_identity(session, "snapshot_reads", fixture.definition())
                graph = read_fixture._publication_graph(seed)
                attempt = await seed_running_generation(
                    session,
                    graph,
                    suffix="snapshot_reads",
                    base_generation_id=None,
                    root_count=len(_families()),
                    family_count=len(_families()),
                )
                first_root, first_binding = await _seed_material(session, seed, attempt)
                if snapshot:
                    family_id = await _call(session, case, attempt, "create_custom_import_snapshot_family")
                    await _copy_snapshot_material(session, case, attempt, family_id, install_legacy=True)
            if snapshot:
                await _prepare_read_indexes(case, family_id)
            async with case.sessions() as session, session.begin():
                await _activate_read_generation(session, graph, attempt)
            await _seed_canonical_decoy(case, graph, attempt, first_root, first_binding)
            pinned_target = read_core.PinnedReadTarget(
                graph.dataset_id,
                attempt.generation_id,
                graph.definition_revision_id,
                graph.schema_revision_id,
                "families_by_period",
            )
            yield SimpleNamespace(case=case, role=role, family_id=family_id, target=pinned_target)
        finally:
            if family_id is not None:
                async with case.engine.begin() as connection:
                    await connection.execute(text(f'DROP SCHEMA IF EXISTS "{snapshot_schema(family_id)}" CASCADE'))


@asynccontextmanager
async def _readonly(fixture_case):
    async with fixture_case.case.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text(f'SET LOCAL ROLE "{fixture_case.role}"'))
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"
        assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        yield session


@contextmanager
def _captured_statements(engine):
    statements = []

    def record_statement(_connection, _cursor, statement, _parameters, _context, _many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        yield statements
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record_statement)


async def _assert_child_filter_parity(session, pinned_target):
    split = fixture.query(
        context_filters=(child_fixture._PANEL,),
        filters=(read_core.ReadFilter("amount", "gt", "8"), read_core.ReadFilter("quality", "gt", "8")),
    )
    assert (await native._relation(session, pinned_target, split))[1] == []
    same = replace(
        split, filters=(read_core.ReadFilter("amount", "gt", "6"), read_core.ReadFilter("quality", "gt", "6"))
    )
    assert [row.entity_value for row in (await native._relation(session, pinned_target, same))[1]] == [native._A]
    query = fixture.query(
        context_filters=(child_fixture._PANEL, child_fixture._KEY),
        filters=(read_core.ReadFilter("amount", "gt", "90"),),
    )
    assert (await native._relation(session, pinned_target, query))[1] == []
    older = replace(query, context_filters=query.context_filters + (read_core.ReadFilter("period", "eq", 2023),))
    assert [row.entity_value for row in (await native._relation(session, pinned_target, older))[1]] == [native._A]


async def _assert_grouped_order_parity(session, pinned_target):
    ordered = child_fixture._ordered_query()
    prepared_query, ordered_rows = await native._relation(session, pinned_target, ordered)
    assert set(map(tuple, ordered_rows)) == {(native._A, Decimal("2")), (native._B, Decimal("5"))}
    for offset, expected_entity in enumerate((native._A, native._B, native._C, child_fixture._ABSENT)):
        count, page = await child_fixture._native_rows(session, prepared_query, is_geo=False, offset=offset)
        assert count == 4 and len(page) == 1 and page[0]["npi_code"] == expected_entity


async def _assert_selected_family_details(session, pinned_target):
    grouped = fixture.query(family_entitlement="full_family")
    page_by_entity = await native._page(session, pinned_target, grouped)
    assert set(page_by_entity) == {native._A, native._B}
    assert page_by_entity[native._A].selection_value == 2024
    assert [group for group, _ in page_by_entity[native._A].families] == ["segment_a", "segment_b"]
    request = read_core.RootDetailRequest(
        pinned_target,
        read_core.EntityLocator("npi", native._A),
        "full_family",
        (),
        grouped.grouped_entity_selection,
        grouped.grouped_child_query,
    )
    detail = await native._service(pinned_target).root_detail_for_entity(
        session, authorization=native._AUTHORIZATION, request=request
    )
    assert detail.families == page_by_entity[native._A].families
    children = detail.families[0][1].children
    assert [child.fields[0].value for child in children] == [
        "chosen",
        "sibling",
        "both",
        "both_again",
        "nullable",
    ]
    assert len(detail.families[1][1].children) == 1


def _assert_serving_relations(pinned, statements, *, snapshot):
    namespace = snapshot_schema(pinned.family_id) if snapshot else pinned.case.schema_name
    serving_models = SNAPSHOT_MODELS[1:2] + SNAPSHOT_MODELS[4:12]
    for model in serving_models:
        assert any(
            f"{namespace}.{model.__tablename__}" in sql or f'"{namespace}".{model.__tablename__}' in sql
            for sql in statements
        ), model.__tablename__
    if snapshot:
        for model in SNAPSHOT_MODELS:
            assert not any(
                f"{pinned.case.schema_name}.{model.__tablename__}" in sql
                or f'"{pinned.case.schema_name}".{model.__tablename__}' in sql
                for sql in statements
            )


async def _assert_snapshot_reader_locks(session, pinned):
    namespace = snapshot_schema(pinned.family_id)
    locked_relations = set(
        await session.scalars(
            text(
                "SELECT c.relname FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE l.pid=pg_backend_pid() "
                "AND l.mode='AccessShareLock' AND l.granted AND n.nspname=:namespace "
                "AND c.relkind='r'"
            ),
            {"namespace": namespace},
        )
    )
    assert locked_relations == {model.__tablename__ for model in SNAPSHOT_MODELS} | {"legacy_copy_origin"}
    assert not await session.scalar(
        text(
            "SELECT has_table_privilege(current_user,:leaf,'INSERT') OR "
            "has_table_privilege(current_user,:registry,'SELECT') OR "
            "has_table_privilege(current_user,:origin,'SELECT')"
        ),
        {
            "leaf": f"{namespace}.custom_import_root_record",
            "registry": f"{pinned.case.schema_name}.custom_import_snapshot_family",
            "origin": f"{namespace}.legacy_copy_origin",
        },
    )


@pytest.mark.parametrize("snapshot", [False, True])
async def test_readonly_snapshot_and_legacy_grouped_child_parity(snapshot):
    """Compare complete grouped reads while proving namespace and read-lock isolation."""

    async with _pinned_read_case(snapshot=snapshot) as pinned, _readonly(pinned) as session:
        assert await read_identity.resolve_generation_snapshot(session, pinned.target) == pinned.family_id
        with _captured_statements(pinned.case.engine) as statements:
            await _assert_child_filter_parity(session, pinned.target)
            await _assert_grouped_order_parity(session, pinned.target)
            await _assert_selected_family_details(session, pinned.target)
        _assert_serving_relations(pinned, statements, snapshot=snapshot)
        if snapshot:
            await _assert_snapshot_reader_locks(session, pinned)


@pytest.mark.parametrize("ddl", ["ALTER TABLE {table} ADD COLUMN synthetic_wait integer", "DROP TABLE {table}"])
async def test_readonly_binding_holds_leaf_identity_against_alter_and_drop(ddl):
    async with _pinned_read_case() as pinned:
        table = f'"{snapshot_schema(pinned.family_id)}".custom_import_child_revision'
        async with _readonly(pinned) as reader:
            assert await read_identity.resolve_generation_snapshot(reader, pinned.target) == pinned.family_id
            with pytest.raises(DBAPIError, match="lock timeout"):
                async with pinned.case.sessions() as writer, writer.begin():
                    await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
                    await writer.execute(text(ddl.format(table=table)))
            assert await reader.scalar(text(f"SELECT count(*) FROM {table}")) == len(child_fixture._children())
        # DDL becomes possible only after request completion; roll it back.
        async with pinned.case.sessions() as writer:
            transaction = await writer.begin()
            try:
                await writer.execute(text("SET LOCAL lock_timeout='1000ms'"))
                await writer.execute(text(ddl.format(table=table)))
            finally:
                await transaction.rollback()
                await writer.rollback()
