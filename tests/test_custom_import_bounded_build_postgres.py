# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native direct-SQL proofs for irreversible bounded family builds."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildCandidateContext,
    CustomImportBuildFamily,
    CustomImportBuildOccurrence,
    CustomImportBuildStream,
    CustomImportBuildVerification,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportDataset,
    CustomImportEntityBinding,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportGenerationSeal,
    CustomImportPack,
    CustomImportRejection,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportRootScalar,
    CustomImportWinner,
)
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.definition_store import register_definition
from process.custom_import.family import RootFamily, assemble_root_families
from process.custom_import.publication import activate_generation
from process.custom_import.runner_codec import (
    candidate_hash,
    child_key_document,
    child_key_hash,
    digest_text,
    new_family_hash,
    pack_hash,
    record_payload,
    root_key_document,
    root_key_hash,
)
from tests.custom_import_postgres_support import (
    _migration,
    isolated_publication_case,
    seed_publication_graph,
    seed_running_generation,
)
from tests.test_custom_import_capture_store_postgres import _parquet_definition
from tests.test_custom_import_segmented_capture_postgres import _append, _part, _pending, _seal, _sha

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20261002010000_custom_import_bounded_build.py"
_COUNTS = (
    "root_count",
    "family_count",
    "generation_family_count",
    "family_child_count",
    "winner_count",
    "profile_count",
    "root_scalar_count",
    "child_scalar_count",
)


def _install(connection, schema):
    migration = _migration(_MIGRATION, "bounded_build_native_migration")
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


@asynccontextmanager
async def _case():
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        yield case


@asynccontextmanager
async def _writer_role(case):
    role = "bounded_writer_" + uuid.uuid4().hex[:16]
    is_created = False
    try:
        async with case.engine.begin() as connection:
            await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
        is_created = True
        async with case.engine.begin() as connection:
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{role}"'))
            await connection.execute(
                text(
                    f'GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON ALL TABLES IN SCHEMA "{case.schema_name}" TO "{role}"'
                )
            )
            await connection.execute(text(f'GRANT USAGE ON ALL SEQUENCES IN SCHEMA "{case.schema_name}" TO "{role}"'))
            signatures = (
                (
                    await connection.execute(
                        text(
                            "SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                            "WHERE n.nspname=:schema AND p.proname IN ('begin_custom_import_build','lock_custom_import_build','commit_custom_import_build_source_page',"
                            "'finish_custom_import_build_source_part','freeze_custom_import_build_source','admit_custom_import_build_page')"
                        ),
                        {"schema": case.schema_name},
                    )
                )
                .scalars()
                .all()
            )
            for signature in signatures:
                await connection.execute(text(f'GRANT EXECUTE ON FUNCTION {signature} TO "{role}"'))
        yield role
    finally:
        if is_created:
            async with case.engine.begin() as connection:
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


async def _as_role(session, role):
    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
    await session.execute(text("SET LOCAL statement_timeout='1000ms'"))


async def _call(session, case, expression, **parameters):
    await session.execute(text("SET LOCAL statement_timeout = '1000ms'"))
    return await session.execute(text(f'SELECT * FROM "{case.schema_name}".{expression}'), parameters)


async def _new_build(case, *, empty=True, page_rows=64, parts=1, seed=None, base=None, pointer_version=0):
    attempt = await _pending(case, seed=seed)
    for slot, _ in attempt.streams:
        for ordinal in range(1, parts + 1):
            await _append(case, _part(attempt, slot, ordinal, empty=empty))
    async with case.sessions() as session, session.begin():
        await _seal(session, attempt)
    deadline = dt.datetime.now(dt.UTC) + dt.timedelta(minutes=4)
    async with case.sessions() as session, session.begin():
        build = (
            await _call(
                session,
                case,
                "begin_custom_import_build(:execution,1::bigint,:token,CAST(:base AS bigint),CAST(:pointer AS bigint),true,:rows,1048576::bigint,1000,:deadline)",
                execution=attempt.execution_id,
                token=_sha(attempt.token),
                rows=page_rows,
                deadline=deadline,
                base=base,
                pointer=pointer_version,
            )
        ).scalar_one()
    return attempt, build


def _owner_columns(attempt):
    return dict(
        dataset_id=attempt.seed.dataset_id,
        definition_revision_id=attempt.seed.definition_revision_id,
        schema_revision_id=attempt.seed.schema_revision_id,
    )


def _source_document(kind, key, child_key):
    definition = _parquet_definition()
    values_by_field = (
        {"npi": key, "display_name": key}
        if kind == "root"
        else {"rate_npi": key, "service_code": child_key, "amount": Decimal("1")}
    )
    canonical_payload = record_payload(
        definition.root_fields if kind == "root" else definition.child_fields, values_by_field
    )
    return SimpleNamespace(
        kind=kind,
        key=key,
        values_by_field=values_by_field,
        canonical_key=root_key_document(definition, {"npi": key}),
        logical_hash=root_key_hash(definition, {"npi": key}),
        raw_key=json.dumps({"contract": "custom-import/raw-family-key/v1", "values": [key]}, separators=(",", ":")),
        canonical_payload=canonical_payload,
        payload_hash=digest_text(f"{kind}-payload", canonical_payload),
    )


async def _intern_root(session, attempt, document):
    root = (
        await session.scalars(
            select(CustomImportRootRecord).where(
                CustomImportRootRecord.dataset_id == attempt.seed.dataset_id,
                CustomImportRootRecord.logical_key_sha256 == document.logical_hash,
            )
        )
    ).one_or_none()
    if root is None:
        root = CustomImportRootRecord(
            dataset_id=attempt.seed.dataset_id,
            key_contract_sha256=_sha("synthetic-key-contract"),
            canonical_logical_key=document.canonical_key,
            logical_key_sha256=document.logical_hash,
        )
        session.add(root)
        await session.flush()
    return root


async def _new_pack(session, attempt, stream, hashes, label):
    pack = CustomImportPack(
        **_owner_columns(attempt),
        execution_id=attempt.execution_id,
        capture_bundle_id=attempt.bundle_id,
        stream_slot=stream.stream_slot,
        pack_ordinal=stream.next_pack_ordinal,
        record_count=len(hashes),
        pack_sha256=pack_hash(label, hashes),
        producing_fence=1,
        producing_token_sha256=_sha(attempt.token),
    )
    session.add(pack)
    await session.flush()
    return pack


async def _source_rejection(session, attempt, build, pack, stream, document, code):
    build_attempt = await session.get(CustomImportBuildAttempt, build)
    rejection = CustomImportRejection(
        **_owner_columns(attempt),
        execution_id=attempt.execution_id,
        rejection_ordinal=build_attempt.next_rejection_ordinal,
        pack_id=pack.pack_id,
        canonical_root_key=document.canonical_key,
        root_key_sha256=document.logical_hash,
        source_ordinal=stream.next_source_ordinal,
        collection_slot=None if document.kind == "root" else 1,
        code=code,
        canonical_evidence="{}",
        producing_fence=1,
        producing_token_sha256=_sha(attempt.token),
    )
    session.add(rejection)
    await session.flush()
    return {"rejection_id": rejection.rejection_id}


async def _source_revision(session, attempt, pack, stream, document, root_id):
    revision_columns_by_name = dict(
        **_owner_columns(attempt),
        pack_id=pack.pack_id,
        root_record_id=root_id,
        source_ordinal=stream.next_source_ordinal,
        canonical_payload=document.canonical_payload,
        payload_sha256=document.payload_hash,
    )
    if document.kind == "root":
        revision = CustomImportRootRevision(**revision_columns_by_name)
        session.add(revision)
        await session.flush()
        return {"root_revision_id": revision.root_revision_id}
    definition = _parquet_definition()
    typed_child_hash = child_key_hash(definition, "rates", document.values_by_field)
    revision = CustomImportChildRevision(
        **revision_columns_by_name,
        collection_slot=1,
        canonical_parent_key=document.canonical_key,
        parent_key_sha256=document.logical_hash,
        canonical_child_key=child_key_document(definition, "rates", document.values_by_field),
        child_key_sha256=typed_child_hash,
    )
    session.add(revision)
    await session.flush()
    return {"child_revision_id": revision.child_revision_id, "child_key_sha256": typed_child_hash}


async def _source_row(case, attempt, build, *, kind="root", key="A", child_key="x", part=1, code=None):
    slot = attempt.streams[0 if kind == "root" else 1][0]
    document = _source_document(kind, key, child_key)
    async with case.sessions() as session, session.begin():
        await _call(session, case, "lock_custom_import_build(:build)", build=build)
        stream = await session.get(CustomImportBuildStream, (build, slot))
        root = await _intern_root(session, attempt, document)
        pack = await _new_pack(
            session, attempt, stream, [] if code else [document.payload_hash], "root" if kind == "root" else "rates"
        )
        if code:
            outcome_columns_by_name = await _source_rejection(session, attempt, build, pack, stream, document, code)
        else:
            outcome_columns_by_name = await _source_revision(
                session, attempt, pack, stream, document, root.root_record_id
            )
        occurrence = CustomImportBuildOccurrence(
            build_id=build,
            stream_slot=slot,
            pack_id=pack.pack_id,
            origin="source",
            source_part_ordinal=part,
            part_row_ordinal=0,
            source_ordinal=stream.next_source_ordinal,
            record_kind=kind,
            collection_slot=0 if kind == "root" else 1,
            raw_parent_key_canonical=document.raw_key,
            raw_parent_key_sha256=_sha("custom-import/raw-family-key/v1:" + document.raw_key),
            root_record_id=root.root_record_id,
            **outcome_columns_by_name,
        )
        session.add(occurrence)
        await session.flush()
        for _ in range(2):
            assert (
                await _call(
                    session,
                    case,
                    "commit_custom_import_build_source_page(:build,:pack)",
                    build=build,
                    pack=pack.pack_id,
                )
            ).scalar_one() == part
        await _call(
            session,
            case,
            "finish_custom_import_build_source_part(:build,CAST(:slot AS smallint),:part)",
            build=build,
            slot=slot,
            part=part,
        )
        return occurrence


async def _finish_source(case, attempt, build):
    async with case.sessions() as session, session.begin():
        for slot, _ in attempt.streams:
            await _call(
                session,
                case,
                "finish_custom_import_build_source_part(:build,CAST(:slot AS smallint),1)",
                build=build,
                slot=slot,
            )
        assert (
            await _call(session, case, "freeze_custom_import_build_source(:build)", build=build)
        ).scalar_one() == "admission"
    cursor = 0
    while True:
        async with case.sessions() as session, session.begin():
            progress = (
                await _call(
                    session,
                    case,
                    "admit_custom_import_build_page(:build,CAST(:cursor AS bigint))",
                    build=build,
                    cursor=cursor,
                )
            ).one()
        cursor = progress.after_occurrence_id
        if progress.phase != "admission":
            return progress.phase


async def _plan(case, build):
    sequence = 0
    while True:
        async with case.sessions() as session, session.begin():
            progress = (
                await _call(
                    session,
                    case,
                    "plan_custom_import_build_family_page(:build,CAST(:sequence AS bigint))",
                    build=build,
                    sequence=sequence,
                )
            ).one()
        sequence = progress.page_sequence
        if progress.plan_complete:
            return


async def _open_generation(case, attempt, build):
    async with case.sessions() as session, session.begin():
        await _call(session, case, "lock_custom_import_build(:build)", build=build)
        build_attempt = await session.get(CustomImportBuildAttempt, build)
        keys = (
            await session.scalars(
                select(CustomImportBuildFamily.root_key_sha256)
                .where(CustomImportBuildFamily.build_id == build)
                .order_by(CustomImportBuildFamily.root_key_sha256)
            )
        ).all()
        generation = CustomImportGeneration(
            **_owner_columns(attempt),
            execution_id=attempt.execution_id,
            capture_bundle_id=attempt.bundle_id,
            producing_fence=1,
            producing_token_sha256=_sha(attempt.token),
            base_generation_id=build_attempt.base_generation_id,
            base_dataset_id=attempt.seed.dataset_id if build_attempt.base_generation_id is not None else None,
            root_count=len(keys),
            family_count=len(keys),
            source_bundle_sha256=_sha("synthetic-source"),
            candidate_sha256=candidate_hash(
                execution_id=attempt.execution_id,
                fence=1,
                base_generation_id=build_attempt.base_generation_id,
                root_key_hashes=keys,
            ),
        )
        session.add(generation)
        await session.flush()
        await _call(
            session,
            case,
            "open_custom_import_build_output(:build,:generation)",
            build=build,
            generation=generation.generation_id,
        )
        return generation


async def _output(case, attempt, build, families=(), contexts=()):
    generation = await _open_generation(case, attempt, build)
    for family in families:
        async with case.sessions() as session, session.begin():
            await _call(session, case, "lock_custom_import_build(:build)", build=build)
            session.add(
                CustomImportGenerationFamily(
                    **_owner_columns(attempt),
                    generation_id=generation.generation_id,
                    root_record_id=family.root_record_id,
                    family_revision_id=family.family_revision_id,
                )
            )
    for context in sorted(
        contexts,
        key=lambda candidate: (candidate.profile_slot, candidate.entity_binding_id, candidate.context_key_sha256),
    ):
        async with case.sessions() as session, session.begin():
            await _call(session, case, "lock_custom_import_build(:build)", build=build)
            session.add(
                CustomImportWinner(
                    **_owner_columns(attempt),
                    generation_id=generation.generation_id,
                    profile_slot=context.profile_slot,
                    entity_binding_id=context.entity_binding_id,
                    family_revision_id=context.family_revision_id,
                    context_collection_slot=1,
                    context_child_revision_id=context.context_child_revision_id,
                    context_key_sha256=context.context_key_sha256,
                )
            )
            await session.flush()
            for _ in range(2):
                await _call(
                    session,
                    case,
                    "commit_custom_import_build_winner_group(:build,:context)",
                    build=build,
                    context=context.candidate_context_id,
                )
    async with case.sessions() as session, session.begin():
        await _call(session, case, "freeze_custom_import_build_output(:build)", build=build)
    return generation


async def _verify(case, build):
    for _ in range(100):
        async with case.sessions() as session, session.begin():
            progress = (await _call(session, case, "verify_custom_import_build_structure(:build)", build=build)).one()
        if progress.verification_state == "complete":
            async with case.sessions() as session:
                return await session.get(CustomImportBuildVerification, build)
    pytest.fail("bounded verifier did not reach SQL EOF")


async def _seal_generation(case, attempt, build, generation):
    async with case.sessions() as session, session.begin():
        proof = await session.get(CustomImportBuildVerification, build)
        await session.execute(text("SET LOCAL statement_timeout = '1000ms'"))
        session.add(
            CustomImportGenerationSeal(
                **_owner_columns(attempt),
                execution_id=attempt.execution_id,
                capture_bundle_id=attempt.bundle_id,
                generation_id=generation.generation_id,
                seal_contract="custom-import-generation-seal/v1",
                sealing_fence=1,
                sealing_token_sha256=_sha(attempt.token),
                materialization_sha256=_sha("synthetic-materialization"),
                effective_output_sha256=_sha("synthetic-output"),
                **{name: getattr(proof, name) for name in _COUNTS},
            )
        )


async def test_empty_build_runs_irreversible_sql_verification_and_ordinary_seal():
    async with _case() as case:
        attempt, build = await _new_build(case)
        assert await _finish_source(case, attempt, build) == "graph"
        await _plan(case, build)
        generation = await _output(case, attempt, build)
        await _verify(case, build)
        await _seal_generation(case, attempt, build, generation)
        async with case.sessions() as session:
            assert (await session.get(CustomImportBuildAttempt, build)).phase == "verified"
            assert await session.get(CustomImportGenerationSeal, generation.generation_id)
        with pytest.raises(DBAPIError, match="phase_mismatch"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "freeze_custom_import_build_source(:build)", build=build)


async def test_plan_stale_sequence_and_unfinished_source_fail_closed():
    async with _case() as case:
        attempt, build = await _new_build(case)
        with pytest.raises(DBAPIError, match="incomplete"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "freeze_custom_import_build_source(:build)", build=build)
        assert await _finish_source(case, attempt, build) == "graph"
        async with case.sessions() as session, session.begin():
            await _call(session, case, "plan_custom_import_build_family_page(:build,0::bigint)", build=build)
        with pytest.raises(DBAPIError, match="progress_conflict"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "plan_custom_import_build_family_page(:build,0::bigint)", build=build)


async def test_global_late_root_duplicate_rejects_family_across_packs_and_preserves_counts():
    async with _case() as case:
        attempt, build = await _new_build(case, empty=False, parts=2)
        await _source_row(case, attempt, build, key="A", part=1)
        await _source_row(case, attempt, build, key="A", part=2)
        await _source_row(case, attempt, build, kind="child", key="A", child_key="x", part=1)
        await _source_row(case, attempt, build, kind="child", key="A", child_key="y", part=2)
        assert await _finish_source(case, attempt, build) == "graph"
        await _plan(case, build)
        async with case.sessions() as session:
            build_attempt = await session.get(CustomImportBuildAttempt, build)
            assert build_attempt.source_occurrence_count == 4 and build_attempt.next_rejection_ordinal == 2
            assert build_attempt.selected_family_count == 0
            assert (
                await session.scalars(
                    select(CustomImportRejection.code).where(CustomImportRejection.execution_id == attempt.execution_id)
                )
            ).all() == ["duplicate_root_key", "duplicate_root_key"]


async def test_raw_parent_identity_and_orphan_precedence_are_global():
    async with _case() as case:
        attempt, build = await _new_build(case, empty=False)
        await _source_row(case, attempt, build, key="1.0")
        await _source_row(case, attempt, build, kind="child", key="1.00", code="child_key_missing")
        assert await _finish_source(case, attempt, build) == "rejected"
        async with case.sessions() as session:
            build_attempt = await session.get(CustomImportBuildAttempt, build)
            assert build_attempt.candidate_error_count == 1 and build_attempt.next_rejection_ordinal == 2
            assert (
                await session.scalars(
                    select(CustomImportRejection.code).where(CustomImportRejection.execution_id == attempt.execution_id)
                )
            ).all() == ["child_key_missing", "orphan_child"]


async def _entity_binding(session, attempt, key):
    binding = (
        await session.scalars(
            select(CustomImportEntityBinding).where(
                CustomImportEntityBinding.dataset_id == attempt.seed.dataset_id,
                CustomImportEntityBinding.adapter_id == "synthetic",
                CustomImportEntityBinding.value_sha256 == _sha(key),
            )
        )
    ).one_or_none()
    if binding is None:
        binding = CustomImportEntityBinding(
            dataset_id=attempt.seed.dataset_id, adapter_id="synthetic", canonical_value=key, value_sha256=_sha(key)
        )
        session.add(binding)
        await session.flush()
    return binding


def _add_root_scalars(session, attempt, root, key):
    for field_slot in (1, 2):
        session.add(
            CustomImportRootScalar(
                dataset_id=attempt.seed.dataset_id,
                schema_revision_id=attempt.seed.schema_revision_id,
                root_revision_id=root.root_revision_id,
                root_record_id=root.root_record_id,
                field_slot=field_slot,
                field_collection_slot=0,
                projection_slot=field_slot,
                field_type="string",
                value_state="value",
                string_value=key,
            )
        )


async def _family_header(session, attempt, root, children, key):
    binding = await _entity_binding(session, attempt, key)
    child_documents = []
    for child in children:
        revision = await session.get(CustomImportChildRevision, child.child_revision_id)
        child_documents.append(
            {
                "rate_npi": key,
                "service_code": json.loads(revision.canonical_child_key)["fields"][0]["value"]["value"],
                "amount": Decimal("1"),
            }
        )
    family = CustomImportFamilyRevision(
        dataset_id=attempt.seed.dataset_id,
        schema_revision_id=attempt.seed.schema_revision_id,
        root_record_id=root.root_record_id,
        root_revision_id=root.root_revision_id,
        entity_binding_id=binding.entity_binding_id,
        producing_execution_id=attempt.execution_id,
        producing_fence=1,
        producing_token_sha256=_sha(attempt.token),
        child_count=len(children),
        family_sha256=new_family_hash(
            _parquet_definition(),
            RootFamily((key,), {"npi": key, "display_name": key}, {"rates": tuple(child_documents)}),
        ),
    )
    session.add(family)
    await session.flush()
    _add_root_scalars(session, attempt, root, key)
    await session.flush()
    return family


def _add_child_scalars(session, attempt, child, code):
    for field_slot, value_type, scalar_value in ((4, "string", code), (5, "decimal", Decimal("1"))):
        session.add(
            CustomImportChildScalar(
                dataset_id=attempt.seed.dataset_id,
                schema_revision_id=attempt.seed.schema_revision_id,
                child_revision_id=child.child_revision_id,
                root_record_id=child.root_record_id,
                collection_slot=1,
                field_slot=field_slot,
                field_collection_slot=1,
                projection_slot=field_slot - 1,
                field_type=value_type,
                value_state="value",
                **{f"{value_type}_value": scalar_value},
            )
        )


def _candidate_context(build, family, child, code):
    canonical_context = json.dumps(
        {
            "dimensions": [{"field_slot": 4, "field_type": "string", "value": code, "value_state": "value"}],
            "profile_id": "default",
            "scope": "child",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return CustomImportBuildCandidateContext(
        build_id=build,
        profile_slot=1,
        entity_binding_id=family.entity_binding_id,
        family_revision_id=family.family_revision_id,
        context_collection_slot=1,
        context_child_revision_id=child.child_revision_id,
        canonical_context_key=canonical_context,
        context_key_sha256=_sha(b"custom-import/v1\0winner-context\0" + canonical_context.encode()),
    )


async def _child_family_page(session, attempt, build, family, child):
    revision = await session.get(CustomImportChildRevision, child.child_revision_id)
    code = json.loads(revision.canonical_child_key)["fields"][0]["value"]["value"]
    session.add(
        CustomImportFamilyChild(
            dataset_id=attempt.seed.dataset_id,
            schema_revision_id=attempt.seed.schema_revision_id,
            family_revision_id=family.family_revision_id,
            root_record_id=child.root_record_id,
            collection_slot=1,
            child_revision_id=child.child_revision_id,
        )
    )
    await session.flush()
    _add_child_scalars(session, attempt, child, code)
    context = _candidate_context(build, family, child, code)
    session.add(context)
    await session.flush()
    return context


async def _commit_family(session, case, build, family):
    return (
        await _call(
            session,
            case,
            "commit_custom_import_build_family_page(:build,:root,:family)",
            build=build,
            root=family.root_record_id,
            family=family.family_revision_id,
        )
    ).one()


async def _family(case, attempt, build, root, children, *, key):
    async with case.sessions() as session, session.begin():
        await _call(session, case, "lock_custom_import_build(:build)", build=build)
        family = await _family_header(session, attempt, root, children, key)
        progress = await _commit_family(session, case, build, family)
        assert progress.complete == (not children)
    contexts = []
    for child in sorted(children, key=lambda occurrence: occurrence.child_key_sha256):
        async with case.sessions() as session, session.begin():
            await _call(session, case, "lock_custom_import_build(:build)", build=build)
            contexts.append(await _child_family_page(session, attempt, build, family, child))
            progress = await _commit_family(session, case, build, family)
    assert progress.complete
    return family, contexts


async def test_multiple_source_packs_large_family_pages_contexts_and_protected_counts():
    async with _case() as case:
        attempt, build = await _new_build(case, empty=False, parts=2, page_rows=6)
        # Child-first source interning is not source presence until roots arrive.
        child_x = await _source_row(case, attempt, build, kind="child", key="A", child_key="x", part=1)
        child_y = await _source_row(case, attempt, build, kind="child", key="A", child_key="y", part=2)
        root_b = await _source_row(case, attempt, build, key="B", part=1)
        root_a = await _source_row(case, attempt, build, key="A", part=2)
        assert await _finish_source(case, attempt, build) == "graph"
        await _plan(case, build)
        family_a, contexts = await _family(case, attempt, build, root_a, (child_x, child_y), key="A")
        family_b, _ = await _family(case, attempt, build, root_b, (), key="B")
        await _output(case, attempt, build, (family_a, family_b), contexts)
        proof = await _verify(case, build)
        assert tuple(getattr(proof, name) for name in _COUNTS) == (2, 2, 2, 2, 2, 1, 4, 4)
        async with case.sessions() as session:
            build_attempt = await session.get(CustomImportBuildAttempt, build)
            assert (
                build_attempt.source_occurrence_count,
                build_attempt.selected_family_count,
                build_attempt.completed_family_count,
                build_attempt.candidate_context_count,
                build_attempt.winner_count,
            ) == (4, 2, 2, 2, 2)


async def _mutate_progress(session, case, build, mutation):
    if mutation == "nested_counter":
        await session.execute(text("CREATE TEMP TABLE synthetic_trigger_target (value integer)"))
        await session.execute(
            text(f'''CREATE FUNCTION pg_temp.synthetic_bump() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN UPDATE "{case.schema_name}".custom_import_build_attempt SET source_occurrence_count=99 WHERE build_id={build}; RETURN NEW; END $$''')
        )
        await session.execute(
            text(
                "CREATE TRIGGER synthetic_bump AFTER INSERT ON synthetic_trigger_target FOR EACH ROW EXECUTE FUNCTION pg_temp.synthetic_bump()"
            )
        )
        await session.execute(text("INSERT INTO synthetic_trigger_target VALUES(1)"))
    else:
        sql = {
            "counter": f'UPDATE "{case.schema_name}".custom_import_build_attempt SET source_occurrence_count=99 WHERE build_id=:build',
            "phase": f"UPDATE \"{case.schema_name}\".custom_import_build_attempt SET phase='verified' WHERE build_id=:build",
            "delete": f'DELETE FROM "{case.schema_name}".custom_import_build_attempt WHERE build_id=:build',
            "truncate": f'TRUNCATE "{case.schema_name}".custom_import_build_occurrence CASCADE',
            "proof": f'INSERT INTO "{case.schema_name}".custom_import_build_verification(build_id,generation_id,source_frozen_at,graph_frozen_at,output_frozen_at) VALUES(:build,1,now(),now(),now())',
        }[mutation]
        await session.execute(text(sql), {"build": build})


async def test_writer_role_grant_failure_preserves_error_and_cleans_role(monkeypatch):
    async with _case() as case:
        role_id = uuid.uuid4()
        role = "bounded_writer_" + role_id.hex[:16]
        monkeypatch.setattr(uuid, "uuid4", lambda: role_id)
        missing_schema = SimpleNamespace(engine=case.engine, schema_name=case.schema_name + "_missing")
        with pytest.raises(DBAPIError, match="schema .* does not exist"):
            async with _writer_role(missing_schema):
                pytest.fail("grant to a missing schema unexpectedly succeeded")
        async with case.engine.connect() as connection:
            assert await connection.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = :role"), {"role": role}) is None


@pytest.mark.parametrize("mutation", ["counter", "phase", "delete", "truncate", "proof", "nested_counter"])
async def test_writer_cannot_forge_protected_progress_even_from_unrelated_trigger(mutation):
    async with _case() as case:
        _, build = await _new_build(case)
        async with _writer_role(case) as role:
            with pytest.raises(DBAPIError, match="protected_write|immutable"):
                async with case.sessions() as session, session.begin():
                    await _as_role(session, role)
                    await _mutate_progress(session, case, build, mutation)
            async with case.sessions() as session:
                assert (await session.get(CustomImportBuildAttempt, build)).source_occurrence_count == 0


async def test_supported_begin_retry_works_for_writer_but_changed_bounds_do_not():
    async with _case() as case:
        attempt, build = await _new_build(case)
        async with case.sessions() as session:
            build_attempt = await session.get(CustomImportBuildAttempt, build)
            deadline = build_attempt.build_deadline_at
        async with _writer_role(case) as role:
            async with case.sessions() as session, session.begin():
                await _as_role(session, role)
                result = await _call(
                    session,
                    case,
                    "begin_custom_import_build(:execution,1::bigint,:token,NULL::bigint,0::bigint,true,64,1048576::bigint,1000,:deadline)",
                    execution=attempt.execution_id,
                    token=_sha(attempt.token),
                    deadline=deadline,
                )
                assert result.scalar_one() == build
            with pytest.raises(DBAPIError, match="retry_conflict"):
                async with case.sessions() as session, session.begin():
                    await _as_role(session, role)
                    await _call(
                        session,
                        case,
                        "begin_custom_import_build(:execution,1::bigint,:token,NULL::bigint,0::bigint,true,63,1048576::bigint,1000,:deadline)",
                        execution=attempt.execution_id,
                        token=_sha(attempt.token),
                        deadline=deadline,
                    )


async def test_uncommitted_source_pack_rolls_back_and_cancel_stops_eof():
    async with _case() as case:
        attempt, build = await _new_build(case)
        with pytest.raises(DBAPIError, match="uncommitted pack"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "lock_custom_import_build(:build)", build=build)
                session.add(
                    CustomImportPack(
                        dataset_id=attempt.seed.dataset_id,
                        definition_revision_id=attempt.seed.definition_revision_id,
                        schema_revision_id=attempt.seed.schema_revision_id,
                        execution_id=attempt.execution_id,
                        capture_bundle_id=attempt.bundle_id,
                        stream_slot=attempt.streams[0][0],
                        pack_ordinal=0,
                        record_count=0,
                        pack_sha256=pack_hash("root", []),
                        producing_fence=1,
                        producing_token_sha256=_sha(attempt.token),
                    )
                )
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f"UPDATE \"{case.schema_name}\".custom_import_execution SET state='canceled' WHERE execution_id=:execution"
                ),
                {"execution": attempt.execution_id},
            )
        with pytest.raises(DBAPIError, match="lease_lost"):
            await _finish_source(case, attempt, build)


async def _activate(case, attempt, generation, expected=None, pointer_version=0):
    async with case.sessions() as session, session.begin():
        await session.execute(
            text(
                f"UPDATE \"{case.schema_name}\".custom_import_execution SET state='completed' WHERE execution_id=:execution"
            ),
            {"execution": attempt.execution_id},
        )
    async with case.sessions() as session, session.begin():
        return await activate_generation(
            session,
            dataset_id=attempt.seed.dataset_id,
            target_generation_id=generation.generation_id,
            expected_generation_id=expected,
            expected_pointer_version=pointer_version,
        )


async def _baseline(case):
    attempt, build = await _new_build(case, empty=False)
    root = await _source_row(case, attempt, build)
    child = await _source_row(case, attempt, build, kind="child")
    assert await _finish_source(case, attempt, build) == "graph"
    await _plan(case, build)
    family, contexts = await _family(case, attempt, build, root, (child,), key="A")
    generation = await _output(case, attempt, build, (family,), contexts)
    await _verify(case, build)
    await _seal_generation(case, attempt, build, generation)
    await _activate(case, attempt, generation)
    return SimpleNamespace(attempt=attempt, root=root, child=child, family=family, generation=generation)


async def _copy_revision(session, attempt, pack, source_occurrence):
    model = CustomImportRootRevision if source_occurrence.record_kind == "root" else CustomImportChildRevision
    source_id = (
        source_occurrence.root_revision_id
        if source_occurrence.record_kind == "root"
        else source_occurrence.child_revision_id
    )
    prior = await session.get(model, source_id)
    revision_columns_by_name = dict(
        **_owner_columns(attempt),
        pack_id=pack.pack_id,
        root_record_id=source_occurrence.root_record_id,
        source_ordinal=0,
        canonical_payload=prior.canonical_payload,
        payload_sha256=prior.payload_sha256,
    )
    if source_occurrence.record_kind == "child":
        revision_columns_by_name.update(
            collection_slot=prior.collection_slot,
            canonical_parent_key=prior.canonical_parent_key,
            parent_key_sha256=prior.parent_key_sha256,
            canonical_child_key=prior.canonical_child_key,
            child_key_sha256=prior.child_key_sha256,
        )
    copied_revision = model(**revision_columns_by_name)
    session.add(copied_revision)
    await session.flush()
    if source_occurrence.record_kind == "root":
        return dict(root_revision_id=copied_revision.root_revision_id, base_root_revision_id=source_id)
    return dict(
        child_revision_id=copied_revision.child_revision_id,
        base_child_revision_id=source_id,
        child_key_sha256=copied_revision.child_key_sha256,
    )


async def _copy_row(session, attempt, build, family, source):
    model = CustomImportRootRevision if source.record_kind == "root" else CustomImportChildRevision
    source_id = source.root_revision_id if source.record_kind == "root" else source.child_revision_id
    prior = await session.get(model, source_id)
    stream = await session.get(CustomImportBuildStream, (build, source.stream_slot))
    pack = await _new_pack(
        session, attempt, stream, [prior.payload_sha256], "root" if source.record_kind == "root" else "rates"
    )
    outcome_columns_by_name = await _copy_revision(session, attempt, pack, source)
    occurrence = CustomImportBuildOccurrence(
        build_id=build,
        stream_slot=source.stream_slot,
        pack_id=pack.pack_id,
        origin="retained",
        base_family_revision_id=family.family_revision_id,
        record_kind=source.record_kind,
        collection_slot=source.collection_slot,
        root_record_id=source.root_record_id,
        **outcome_columns_by_name,
    )
    session.add(occurrence)
    await session.flush()
    return occurrence


async def _retained_family(case, attempt, build, baseline):
    async with case.sessions() as session, session.begin():
        await _call(session, case, "lock_custom_import_build(:build)", build=build)
        root_copy = await _copy_row(session, attempt, build, baseline.family, baseline.root)
        family = await _family_header(session, attempt, root_copy, (baseline.child,), "A")
        assert not (await _commit_family(session, case, build, family)).complete
    async with case.sessions() as session, session.begin():
        await _call(session, case, "lock_custom_import_build(:build)", build=build)
        child_copy = await _copy_row(session, attempt, build, baseline.family, baseline.child)
        context = await _child_family_page(session, attempt, build, family, child_copy)
        assert (await _commit_family(session, case, build, family)).complete
    return family, (context,), root_copy, child_copy


async def test_upsert_copies_retained_family_into_current_attempt_and_seals():
    async with _case() as case:
        baseline = await _baseline(case)
        attempt, build = await _new_build(
            case, seed=baseline.attempt.seed, base=baseline.generation.generation_id, pointer_version=1, page_rows=7
        )
        assert await _finish_source(case, attempt, build) == "graph"
        await _plan(case, build)
        family, contexts, root_copy, child_copy = await _retained_family(case, attempt, build, baseline)
        assert root_copy.root_revision_id != baseline.root.root_revision_id
        assert child_copy.child_revision_id != baseline.child.child_revision_id
        generation = await _output(case, attempt, build, (family,), contexts)
        proof = await _verify(case, build)
        assert tuple(getattr(proof, name) for name in _COUNTS) == (1, 1, 1, 1, 1, 1, 2, 2)
        await _seal_generation(case, attempt, build, generation)


async def test_duplicate_child_before_root_admission_rejects_only_its_family():
    async with _case() as case:
        attempt, build = await _new_build(case, empty=False, parts=2, page_rows=1)
        await _source_row(case, attempt, build, kind="child", part=1)
        await _source_row(case, attempt, build, kind="child", part=2)
        await _source_row(case, attempt, build, key="A", part=1)
        await _source_row(case, attempt, build, key="B", part=2)
        assert await _finish_source(case, attempt, build) == "graph"
        await _plan(case, build)
        async with case.sessions() as session:
            build_attempt = await session.get(CustomImportBuildAttempt, build)
            assert build_attempt.selected_family_count == 1
            assert build_attempt.next_rejection_ordinal == 2
            assert build_attempt.candidate_error_count == 0


async def test_malformed_child_remains_candidate_wide_not_orphan_reclassified():
    async with _case() as case:
        attempt, build = await _new_build(case, empty=False)
        await _source_row(case, attempt, build)
        await _source_row(case, attempt, build, kind="child", key="absent", code="child_not_object")
        assert await _finish_source(case, attempt, build) == "rejected"
        async with case.sessions() as session:
            build_attempt = await session.get(CustomImportBuildAttempt, build)
            assert build_attempt.candidate_error_count == 1 and build_attempt.next_rejection_ordinal == 1


async def _snapshot_seed(case, baseline):
    definition_document = json.loads(_parquet_definition().canonical)
    definition_document["refresh_mode"] = "snapshot"
    definition_document["revision"]["definition"] = 2
    async with case.sessions() as session, session.begin():
        dataset = await session.get(CustomImportDataset, baseline.attempt.seed.dataset_id)
        return await register_definition(
            session, dataset.dataset_key, CustomImportDefinition.from_mapping(definition_document)
        )


@pytest.mark.parametrize("rejected", [False, True])
async def test_snapshot_drops_missing_but_retains_rejected_baseline_family(rejected):
    async with _case() as case:
        baseline = await _baseline(case)
        seed = await _snapshot_seed(case, baseline)
        attempt, build = await _new_build(
            case, seed=seed, base=baseline.generation.generation_id, pointer_version=1, empty=not rejected
        )
        if rejected:
            await _source_row(case, attempt, build, code="invalid_type")
            await _source_row(case, attempt, build, kind="child")
        assert await _finish_source(case, attempt, build) == "graph"
        await _plan(case, build)
        async with case.sessions() as session:
            chosen = (
                await session.scalars(select(CustomImportBuildFamily).where(CustomImportBuildFamily.build_id == build))
            ).all()
        assert len(chosen) == int(rejected)
        if rejected:
            assert chosen[0].selection_kind == "retained"
            assert chosen[0].base_family_revision_id == baseline.family.family_revision_id


async def _empty_verifying(case):
    attempt, build = await _new_build(case)
    assert await _finish_source(case, attempt, build) == "graph"
    await _plan(case, build)
    generation = await _output(case, attempt, build)
    async with case.sessions() as session, session.begin():
        await _call(session, case, "verify_custom_import_build_structure(:build)", build=build)
    return attempt, build, generation


async def _verification_call(case, build, started):
    async with case.sessions() as session, session.begin():
        await started.put((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        return (await _call(session, case, "verify_custom_import_build_structure(:build)", build=build)).one()


async def _wait_lock(session, backend_pid):
    for _ in range(60):
        waiting = (
            await session.execute(
                text("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted)"), {"pid": backend_pid}
            )
        ).scalar_one()
        if waiting:
            return
        await asyncio.sleep(0.01)
    pytest.fail("verification did not reach its exact expected lock wait")


async def test_verification_reads_frozen_capture_before_acquiring_dataset_lock():
    async with _case() as case:
        _, build, _ = await _empty_verifying(case)
        started = asyncio.Queue()
        async with case.sessions() as blocker, blocker.begin():
            await blocker.execute(
                text(f'LOCK TABLE "{case.schema_name}".custom_import_capture IN ACCESS EXCLUSIVE MODE')
            )
            worker = asyncio.create_task(_verification_call(case, build, started))
            try:
                backend_pid = await started.get()
                await _wait_lock(blocker, backend_pid)
                held = (
                    await blocker.execute(
                        text(
                            "SELECT count(*) FROM pg_locks WHERE pid=:pid AND granted "
                            "AND relation=CAST(:relation AS regclass) AND mode='RowShareLock'"
                        ),
                        {"pid": backend_pid, "relation": case.schema_name + ".custom_import_dataset"},
                    )
                ).scalar_one()
                assert held == 0
            finally:
                await blocker.commit()
                await worker


async def test_two_verification_pages_cannot_double_charge_same_cursor():
    async with _case() as case:
        attempt, build, _ = await _empty_verifying(case)
        started = asyncio.Queue()
        async with case.sessions() as blocker, blocker.begin():
            await blocker.execute(
                select(CustomImportDataset)
                .where(CustomImportDataset.dataset_id == attempt.seed.dataset_id)
                .with_for_update()
            )
            workers = [asyncio.create_task(_verification_call(case, build, started)) for _ in range(2)]
            try:
                for _ in range(2):
                    await _wait_lock(blocker, await started.get())
            finally:
                await blocker.commit()
                outcomes = await asyncio.gather(*workers, return_exceptions=True)
        assert sum(isinstance(outcome, DBAPIError) for outcome in outcomes) == 1
        assert any(isinstance(outcome, DBAPIError) and "progress_conflict" in str(outcome) for outcome in outcomes)
        proof = await _verify(case, build)
        assert proof.profile_count == 1 and proof.root_count == 0


def _uninstall(connection, schema):
    migration = _migration(_MIGRATION, "bounded_build_native_downgrade")
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.downgrade()


async def test_empty_migration_roundtrip_and_retained_evidence_downgrade_denial():
    async with _case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_uninstall, case.schema_name)
            assert (
                await connection.execute(
                    text("SELECT to_regclass(:name)"), {"name": case.schema_name + ".custom_import_build_attempt"}
                )
            ).scalar_one() is None
            await connection.run_sync(_install, case.schema_name)
        await _new_build(case)
        with pytest.raises(DBAPIError, match="retained build evidence"):
            async with case.engine.begin() as connection:
                await connection.run_sync(_uninstall, case.schema_name)


async def _source_graph(case):
    attempt, build = await _new_build(case, empty=False)
    root = await _source_row(case, attempt, build)
    child = await _source_row(case, attempt, build, kind="child")
    assert await _finish_source(case, attempt, build) == "graph"
    await _plan(case, build)
    return attempt, build, root, child


async def test_family_requires_every_context_and_cannot_append_after_committed_page():
    async with _case() as case:
        attempt, build, root, child = await _source_graph(case)
        async with case.sessions() as session, session.begin():
            await _call(session, case, "lock_custom_import_build(:build)", build=build)
            family = await _family_header(session, attempt, root, (child,), "A")
            assert not (await _commit_family(session, case, build, family)).complete
        with pytest.raises(DBAPIError, match="incomplete"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "lock_custom_import_build(:build)", build=build)
                session.add(
                    CustomImportFamilyChild(
                        dataset_id=attempt.seed.dataset_id,
                        schema_revision_id=attempt.seed.schema_revision_id,
                        family_revision_id=family.family_revision_id,
                        root_record_id=root.root_record_id,
                        collection_slot=1,
                        child_revision_id=child.child_revision_id,
                    )
                )
                await session.flush()
                _add_child_scalars(session, attempt, child, "x")
                await session.flush()
                await _commit_family(session, case, build, family)
        async with case.sessions() as session, session.begin():
            await _call(session, case, "lock_custom_import_build(:build)", build=build)
            context = await _child_family_page(session, attempt, build, family, child)
            assert (await _commit_family(session, case, build, family)).complete
        with pytest.raises(DBAPIError, match="committed child page"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "lock_custom_import_build(:build)", build=build)
                _add_child_scalars(session, attempt, child, "x")
        with pytest.raises(DBAPIError, match="committed root page"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "lock_custom_import_build(:build)", build=build)
                _add_root_scalars(session, attempt, root, "A")
        await _output(case, attempt, build, (family,), (context,))
        with pytest.raises(DBAPIError, match="phase_mismatch"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, "lock_custom_import_build(:build)", build=build)
                session.add(_candidate_context(build, family, child, "other-context"))


async def test_seal_requires_sql_proof_and_live_lease_after_verification():
    async with _case() as case:
        attempt, build, generation = await _empty_verifying(case)
        with pytest.raises(DBAPIError, match="structure_mismatch"):
            await _seal_generation(case, attempt, build, generation)
        await _verify(case, build)
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f"UPDATE \"{case.schema_name}\".custom_import_execution SET state='canceled' WHERE execution_id=:execution"
                ),
                {"execution": attempt.execution_id},
            )
        with pytest.raises(DBAPIError, match="lease_lost"):
            await _seal_generation(case, attempt, build, generation)


async def test_cross_build_occurrence_and_committed_pack_append_are_denied():
    async with _case() as case:
        attempt, build = await _new_build(case, empty=False)
        root = await _source_row(case, attempt, build)
        other_attempt, other_build = await _new_build(case)
        for target_build in (build, other_build):
            with pytest.raises(DBAPIError, match="identity_mismatch|committed pack"):
                async with case.sessions() as session, session.begin():
                    await _call(session, case, "lock_custom_import_build(:build)", build=target_build)
                    session.add(
                        CustomImportBuildOccurrence(
                            build_id=target_build,
                            stream_slot=root.stream_slot,
                            pack_id=root.pack_id,
                            origin="source",
                            source_part_ordinal=1,
                            part_row_ordinal=1,
                            source_ordinal=1,
                            record_kind="root",
                            collection_slot=0,
                            root_record_id=root.root_record_id,
                            root_revision_id=root.root_revision_id,
                            raw_parent_key_canonical=root.raw_parent_key_canonical,
                            raw_parent_key_sha256=root.raw_parent_key_sha256,
                        )
                    )
        assert other_attempt.seed.dataset_id != attempt.seed.dataset_id


async def test_ordinary_unpublished_seal_allows_pointer_drift_after_begin():
    async with _case() as case:
        baseline = await _baseline(case)
        seed = await _snapshot_seed(case, baseline)
        first_attempt, first_build = await _new_build(
            case, seed=seed, base=baseline.generation.generation_id, pointer_version=1
        )
        second_attempt, second_build = await _new_build(
            case, seed=seed, base=baseline.generation.generation_id, pointer_version=1
        )
        generations = []
        for attempt, build in ((first_attempt, first_build), (second_attempt, second_build)):
            assert await _finish_source(case, attempt, build) == "graph"
            await _plan(case, build)
            generations.append(await _output(case, attempt, build))
            await _verify(case, build)
        await _seal_generation(case, second_attempt, second_build, generations[1])
        await _activate(case, second_attempt, generations[1], baseline.generation.generation_id, 1)
        await _seal_generation(case, first_attempt, first_build, generations[0])
        async with case.sessions() as session:
            assert await session.get(CustomImportGenerationSeal, generations[0].generation_id)


async def test_legacy_sealing_remains_available_and_keeps_original_count_guard():
    async with _case() as case:
        async with case.sessions() as session, session.begin():
            graph = await seed_publication_graph(session)
        async with case.sessions() as session:
            assert await session.get(CustomImportGenerationSeal, graph.first_generation_id)
        with pytest.raises(DBAPIError, match="custom_import_generation_seal_count_mismatch"):
            async with case.sessions() as session, session.begin():
                attempt = await seed_running_generation(
                    session, graph, suffix="invalid-counts", base_generation_id=None, root_count=1, family_count=1
                )
                session.add(
                    CustomImportGenerationSeal(
                        dataset_id=graph.dataset_id,
                        definition_revision_id=graph.definition_revision_id,
                        schema_revision_id=graph.schema_revision_id,
                        execution_id=attempt.execution_id,
                        generation_id=attempt.generation_id,
                        capture_bundle_id=graph.capture_bundle_id,
                        seal_contract="custom-import-generation-seal/v1",
                        sealing_fence=attempt.fence,
                        sealing_token_sha256=_sha(attempt.token),
                        materialization_sha256=_sha("synthetic-invalid-count"),
                        effective_output_sha256=_sha("synthetic-output"),
                        **{name: 0 for name in _COUNTS},
                    )
                )


@pytest.mark.parametrize("page_rows", [1, 64])
async def test_child_first_errors_preserve_bounded_legacy_count_evidence(page_rows):
    key = "1234567893"
    roots = [{"npi": key, "display_name": key}, {"npi": key}]
    child_rows = [
        {"rate_npi": key, "service_code": "x", "amount": "invalid"},
        {"rate_npi": key, "service_code": "y", "amount": Decimal("1")},
    ]
    admitted = assemble_root_families(_parquet_definition(), roots, {"rates": child_rows})
    assert not admitted.families
    assert {rejection.code for rejection in admitted.rejections} == {"field_type_invalid", "duplicate_root_key"}
    async with _case() as case:
        attempt, build = await _new_build(case, empty=False, parts=2, page_rows=page_rows)
        await _source_row(case, attempt, build, kind="child", key=key, part=1, code="field_type_invalid")
        await _source_row(case, attempt, build, kind="child", key=key, child_key="y", part=2)
        first_root = await _source_row(case, attempt, build, key=key, part=1)
        await _source_row(case, attempt, build, key=key, part=2, code="required_field_missing")
        assert await _finish_source(case, attempt, build) == "graph"
        async with case.sessions() as session:
            first = await session.get(CustomImportBuildOccurrence, first_root.occurrence_id)
            assert first.rejection_id is None and first.resolved_rejection_id is not None
            duplicate = (
                await session.execute(
                    text(f'''SELECT EXISTS(SELECT 1 FROM "{case.schema_name}".custom_import_build_occurrence
                WHERE build_id=:build AND origin='source' AND record_kind='root'
                AND raw_parent_key_sha256=:raw AND occurrence_id<>:first)'''),
                    {"build": build, "raw": first.raw_parent_key_sha256, "first": first.occurrence_id},
                )
            ).scalar_one()
            child_codes = (
                await session.scalars(
                    select(CustomImportRejection.code)
                    .join(
                        CustomImportBuildOccurrence,
                        CustomImportBuildOccurrence.rejection_id == CustomImportRejection.rejection_id,
                    )
                    .where(
                        CustomImportBuildOccurrence.build_id == build,
                        CustomImportBuildOccurrence.record_kind == "child",
                    )
                    .order_by(CustomImportBuildOccurrence.occurrence_id)
                    .limit(3)
                )
            ).all()
        semantic_codes = set(child_codes)
        if duplicate:
            semantic_codes.add("duplicate_root_key")
        assert semantic_codes == {rejection.code for rejection in admitted.rejections}
        assert len(semantic_codes) == len(admitted.rejections) == 2
