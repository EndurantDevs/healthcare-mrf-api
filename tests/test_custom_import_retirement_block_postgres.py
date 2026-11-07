# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual block candidate retirement, retained audit and transactional DDL rollback."""

import base64
import datetime as dt
import hashlib
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from sqlalchemy import func, select, text, update

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportLease,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout, CustomImportFamilyBlockRelation
from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRetirement
from process.custom_import import family_block_store as store
from process.custom_import import retirement
from process.custom_import.build_source import SourceBuildRequest
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import claim_execution
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_control import begin_shard_build
from process.custom_import.family_shard_retirement_native import read_retired_scope
from process.custom_import.storage_layout import snapshot_schema
from tests.custom_import_build_scratch_postgres import install_current_verification
from tests.custom_import_post_seal_audit_postgres import _refresh_aggregate
from tests.custom_import_postgres_support import _ROOT, _migration, isolated_publication_case
from tests.test_control_owner_handoff import _profile
from tests.test_custom_import_snapshot_storage_postgres import _attempt, _bounded, _external_fixture_check


def _install_blocks(connection, schema):
    install_current_verification(connection, schema)
    for name in (
        "20261102030000_custom_import_build_scratch",
        "20261102040000_custom_import_post_seal_audit",
        "20261104010000_custom_import_unlogged_landing",
        "20261107010000_custom_import_child_reference_aggregate",
        "20261104020000_custom_import_family_blocks",
    ):
        if name == "20261107010000_custom_import_child_reference_aggregate":
            _refresh_aggregate(connection, schema)
            continue
        module = _migration(_ROOT / "alembic/versions" / (name + ".py"), "retirement_block_fixture_" + name)
        module._schema = lambda: schema
        module.op = Operations(MigrationContext.configure(connection))
        module.upgrade()


@asynccontextmanager
async def _block_case():
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_blocks, case.schema_name)
        graph, attempt = await _attempt(case)
        async with case.sessions() as session, session.begin():
            revision = await session.get(CustomImportDefinitionRevision, graph.definition_revision_id)
            definition = CustomImportDefinition.from_json(revision.canonical_definition)
        request = SourceBuildRequest(
            graph.dataset_id,
            graph.definition_revision_id,
            graph.schema_revision_id,
            attempt.execution_id,
            attempt.token,
            attempt.fence,
            definition,
            None,
            0,
            True,
            256,
            1024**2,
            5000,
            dt.datetime.now(dt.UTC) + dt.timedelta(hours=1),
            60,
        )
        layout = FamilyBlockLayout(
            definition, {name: index + 1 for index, name in enumerate(definition.collections_by_name)}
        )
        family = await store.create_candidate(case.sessions, request, layout)
        async with case.sessions() as session, session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == attempt.execution_id)
                .values(expires_at=dt.datetime(2000, 1, 1, tzinfo=dt.UTC))
            )
            grant = await claim_execution(session, execution_id=attempt.execution_id, token=uuid.uuid4().hex)
            assert grant.fence == attempt.fence + 1
        yield case, family


async def _retire(case, family):
    async with case.sessions() as session, session.begin():
        await _bounded(session)
        return await retirement.retire_snapshot_family(
            session, family, engine_id="synthetic", assert_external_unreferenced=_external_fixture_check
        )


async def test_actual_fourteen_role_retirement_preserves_small_immutable_audit():
    async with _block_case() as (case, family):
        async with case.sessions() as session, session.begin():
            before = (
                await session.scalars(
                    select(CustomImportFamilyBlockRelation)
                    .where(CustomImportFamilyBlockRelation.family_id == family)
                    .order_by(CustomImportFamilyBlockRelation.role_slot)
                )
            ).all()
            identities = tuple(
                (relation.role_slot, relation.table_oid, bytes(relation.columns_sha256)) for relation in before
            )
            assert len(identities) == 14
        retirement_receipt = await _retire(case, family)
        assert retirement_receipt.reason == "replaced_fence" and not retirement_receipt.replayed
        async with case.sessions() as session, session.begin():
            assert (
                await session.scalar(text("SELECT to_regnamespace(:name)"), {"name": snapshot_schema(family)}) is None
            )
            assert await session.get(CustomImportSnapshotFamily, family) is not None
            assert await session.get(CustomImportSnapshotRetirement, family) is not None
            assert await session.get(CustomImportFamilyBlockLayout, family) is not None
            after = (
                await session.scalars(
                    select(CustomImportFamilyBlockRelation)
                    .where(CustomImportFamilyBlockRelation.family_id == family)
                    .order_by(CustomImportFamilyBlockRelation.role_slot)
                )
            ).all()
            assert (
                tuple((relation.role_slot, relation.table_oid, bytes(relation.columns_sha256)) for relation in after)
                == identities
            )
        assert (await _retire(case, family)).replayed


async def test_block_retirement_interruption_rolls_back_all_drops_and_tombstone(monkeypatch):
    async with _block_case() as (case, family):
        original = retirement._physical_drop

        async def interrupted(*args):
            await original(*args)
            raise ConnectionError("synthetic interruption after native drops")

        monkeypatch.setattr(retirement, "_physical_drop", interrupted)
        with pytest.raises(ConnectionError):
            await _retire(case, family)
        async with case.sessions() as session, session.begin():
            assert await session.get(CustomImportSnapshotRetirement, family) is None
            assert (
                await session.scalar(text("SELECT to_regnamespace(:name)"), {"name": snapshot_schema(family)})
                is not None
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(CustomImportFamilyBlockRelation)
                    .where(CustomImportFamilyBlockRelation.family_id == family)
                )
                == 14
            )


def _scope_parent(request, capture, binding, idempotency_key, role):
    configuration, parent = _profile()
    configuration["owner_role"] = role
    parent["descriptor"].update(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        engine_execution_id=request.execution_id,
        fence=request.fence,
        capture_bundle_id=capture.capture_bundle_id,
        controller_execution_id=idempotency_key,
        source_binding_revision_id=binding.source_binding_revision_id,
        source_binding_sha256=binding.source_binding_sha256.hex(),
        request_identity_sha256=bytes(capture.request_identity_sha256).hex(),
        capture_manifest_sha256=bytes(capture.manifest_sha256).hex(),
        bearer_sha256=bytes(capture.producing_token_sha256).hex(),
        build_deadline_at=request.build_deadline_at.isoformat().replace("+00:00", "Z"),
    )
    return configuration, parent


async def _bound_owner_case(case):
    """Use the existing real bound-capture writer; never alter a sealed capture."""
    from process.custom_import.capture_pending import seal_pending_parquet_bundle
    from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
    from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
    from tests.custom_import_query_projection_authority_postgres import (
        _bound_receipt,
        _pending_capture,
        _source_definition,
    )
    from tests.test_custom_import_build_source_postgres import _sealed_parts
    from tests.test_custom_import_capture_pending_postgres import _retain
    from tests.test_custom_import_segmented_capture_postgres import _POLICY

    definition = _source_definition()
    policy = ProcessingPolicy(SegmentedCapturePolicy.from_mapping(_POLICY), 17, BuildPolicy(64, 65536, 10000, 120, 300))
    pending, registration, binding, idempotency_key = await _pending_capture(case, definition, policy)
    parts = _sealed_parts(pending, definition, {stream.stream_id: [[]] for stream in definition.source_streams})
    await _retain(case, pending, registration, parts)
    async with case.sessions() as session, session.begin():
        await seal_pending_parquet_bundle(
            session,
            request=pending,
            capture_bundle_id=registration.capture_bundle_id,
            receipts=tuple(_bound_receipt(pending, stream, captured) for stream, captured in parts.items()),
        )
    request = SourceBuildRequest(
        pending.dataset_id,
        pending.definition_revision_id,
        pending.schema_revision_id,
        pending.execution_id,
        pending.token,
        pending.fence,
        definition,
        None,
        0,
        True,
        64,
        65536,
        10000,
        dt.datetime.now(dt.UTC) + dt.timedelta(minutes=5),
        120,
    )
    layout = FamilyBlockLayout(
        definition, {name: index + 1 for index, name in enumerate(definition.collections_by_name)}
    )
    family = await store.create_candidate(case.sessions, request, layout)
    build_id, _registry = await begin_shard_build(case.sessions, request, family, layout)
    async with case.sessions() as session, session.begin():
        capture = await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
        metadata = await session.get(CustomImportFamilyBlockLayout, family)
        # This declared native head exercises terminal observation only, not file adoption.
        metadata.owner_checkpoint_revision, metadata.owner_checkpoint_sha256 = 1, b"h" * 32
        role = await session.scalar(text("SELECT CURRENT_USER"))
        configuration, parent = _scope_parent(request, capture, binding, idempotency_key, role)
    return family, request, configuration, parent, build_id


async def test_native_retired_scope_requires_permanent_drop_and_exact_original_identity():
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_blocks, case.schema_name)
        family, request, configuration, parent, build_id = await _bound_owner_case(case)
        with pytest.raises(ValueError):
            await read_retired_scope(case.sessions, parent, configuration)
        await _replace_owner_fence(case, request)
        await _retire(case, family)
        actual = await read_retired_scope(case.sessions, parent, configuration)
        assert actual.family_id == family and actual.scope[0] == build_id and actual.head.revision == 1
        assert len(actual.sha256) == 64 and not hasattr(actual, "request") and not hasattr(actual, "binding")
        parent["descriptor"]["capture_manifest_sha256"] = "f" * 64
        with pytest.raises(ValueError):
            await read_retired_scope(case.sessions, parent, configuration)


async def test_native_cleanup_entry_replays_exact_sealed_inventory(tmp_path, monkeypatch):
    """Real native retirement plus CLI resume; initial inventory custody is declared."""
    from process.custom_import import family_shard_handoff as handoff
    from process.custom_import import family_shard_owner_cli as owner_cli
    from process.custom_import import family_shard_retirement_cli as cli
    from process.custom_import import family_shard_retirement_files as files
    from process.custom_import import family_shard_retirement_handoff as custody
    from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
    from process.custom_import.family_shard_retirement_policy import RetiredFilePolicy
    from tests import custom_import_query_projection_authority_postgres as authority

    monkeypatch.setattr(authority, "_TOKEN", b"x" * 43)
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_blocks, case.schema_name)
        family, request, configuration, parent, _build_id = await _bound_owner_case(case)
        key = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
        configuration["verifying_key"] = (
            base64.urlsafe_b64encode(key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)).decode().rstrip("=")
        )
        parent["descriptor"]["owner_profile_sha256"] = hashlib.sha256(handoff.canonical(configuration)).hexdigest()
        parent["signature"] = (
            base64.urlsafe_b64encode(key.sign(handoff.DOMAIN + handoff.canonical(parent["descriptor"])))
            .decode()
            .rstrip("=")
        )
        await _replace_owner_fence(case, request)
        await _retire(case, family)
        native = await read_retired_scope(case.sessions, parent, configuration)
        tmp_path.chmod(0o700)
        identifier = handoff.stage(parent, b"x" * 43, parent=tmp_path)
        original = tmp_path / identifier
        _seed_file_inventory(original, parent, native, configuration)
        monkeypatch.setattr(handoff, "OWNER_INPUT", original)
        monkeypatch.setattr(handoff, "OWNER_PARENT", original / "work")
        monkeypatch.setenv(owner_cli.PROFILE_ENV, handoff.canonical(configuration).decode())
        monkeypatch.setenv(owner_cli.NODE_ENV, configuration["node_name"])
        database = SimpleNamespace(session_factory=case.sessions, connect=AsyncMock(), disconnect=AsyncMock())
        document = custody.read(original, custody.INTENT)
        receipt = await cli.run_cleanup(identifier, custody.identity(document), database)
        assert receipt["status"] == "retired_files_cleaned" and not (original / "work/owner").exists()
        database.disconnect.assert_awaited_once()


async def _replace_owner_fence(case, request):
    async with case.sessions() as session, session.begin():
        await session.execute(
            update(CustomImportLease)
            .where(CustomImportLease.execution_id == request.execution_id)
            .values(expires_at=dt.datetime(2000, 1, 1, tzinfo=dt.UTC))
        )
        await claim_execution(session, execution_id=request.execution_id, token=uuid.uuid4().hex)


def _seed_file_inventory(original, parent, native, configuration):
    from process.custom_import import family_shard_retirement_files as files
    from process.custom_import import family_shard_retirement_handoff as custody
    from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
    from process.custom_import.family_shard_retirement_policy import RetiredFilePolicy

    root = original / "work/owner"
    root.mkdir(mode=0o700)
    lock = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
    try:
        (root / "source").mkdir(mode=0o700)
        (root / "source/output.mpk").write_bytes(b"synthetic closed original")
        (root / "source/output.mpk").chmod(0o400)
        document = custody.stage(original, parent, native, configuration)
        policy = RetiredFilePolicy(root)
        policy.work(root / "source")
        files.seal_inventory(custody.context(document, work=original / "work"), policy, lock.check)
    finally:
        lock.close()


async def test_terminal_census_scope_joins_only_actual_later_native_retirement():
    """Actual native scope/closed-lease join; the checkpoint head is declared by this fixture."""
    from process.custom_import.execution import finish_execution
    from process.custom_import.family_shard_retirement_native import read_terminal_scope

    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_blocks, case.schema_name)
        family, request, configuration, parent, _build_id = await _bound_owner_case(case)
        with pytest.raises(ValueError):
            await read_terminal_scope(case.sessions, parent, configuration)
        async with case.sessions() as session, session.begin():
            transition = await finish_execution(
                session,
                execution_id=request.execution_id,
                fence=request.fence,
                token=request.lease_token,
                terminal_state="failed",
            )
            assert transition.state == "failed" and transition.changed
        terminal = await read_terminal_scope(case.sessions, parent, configuration)
        assert not hasattr(terminal, "sha256")
        with pytest.raises(ValueError):
            await read_retired_scope(case.sessions, parent, configuration)
        await _retire(case, family)
        retired = await read_retired_scope(case.sessions, parent, configuration)
        assert (retired.scope, retired.head) == (terminal.scope, terminal.head)
        assert retired.family_id == terminal.family_id == family
        assert len(retired.sha256) == 64
