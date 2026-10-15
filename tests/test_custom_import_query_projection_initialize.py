# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic initialization fencing, replay and restrictive ACL contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import query_projection_initialize as initialize
from process.custom_import.execution import lease_token_sha256
from tests.test_custom_import_derived_query import definition


def _case(**parent_changes):
    parsed = definition()
    request = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        fence=5,
        lease_token="synthetic-token",
        definition=parsed,
    )
    identity_by_name = dict(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        producing_fence=5,
        producing_token_sha256=lease_token_sha256(request.lease_token),
        generation_id=6,
        capture_bundle_id=7,
    )
    parent = SimpleNamespace(**(identity_by_name | dict(family_id=17, frozen_at=None) | parent_changes))
    build = SimpleNamespace(**identity_by_name, phase="output")
    generation = SimpleNamespace(**identity_by_name)
    definition_row = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        definition_sha256=bytes.fromhex(parsed.digest),
        contract_version="custom-import/v1",
    )
    relation = SimpleNamespace(table_oid=100, table_owner=42)
    return request, build, (parent, generation, definition_row, relation)


def _result(value):
    result = Mock()
    result.one_or_none.return_value = value
    result.scalar_one_or_none.return_value = value
    result.first.return_value = value
    result.all.return_value = value
    result.mappings.return_value = result
    return result


def _session(*results):
    return SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(side_effect=results), add=Mock())


def _owner(**changes):
    return (
        dict(current_owner=42, schema_owner=42, table_owner=42, table_oid=100, relkind="r", relpersistence="p")
        | changes
    )


@pytest.fixture
def prepared(monkeypatch):
    prepare = AsyncMock()
    flush = AsyncMock()
    identity = AsyncMock(side_effect=[(101, 42, b"r" * 32), (102, 42, b"c" * 32)])
    lock = AsyncMock()
    monkeypatch.setattr(initialize, "_prepare_statement", prepare)
    monkeypatch.setattr(initialize, "_flush_page", flush)
    monkeypatch.setattr(initialize, "projection_table_identity", identity)
    monkeypatch.setattr(initialize, "lock_projection_tables", lock)
    monkeypatch.setattr(initialize, "capability_sha256", lambda definition: b"k" * 32)
    return SimpleNamespace(prepare=prepare, flush=flush, identity=identity, lock=lock)


@pytest.mark.asyncio
async def test_absent_projection_creates_native_cells_closes_defaults_and_inserts_one_manifest(prepared):
    request, build, parents = _case()
    inherited_readers = [{"grantee": 0, "role_name": None}, {"grantee": 50, "role_name": 'read"only'}]
    defaults = [{"grantee": 0, "role_name": None}, {"grantee": 60, "role_name": "default_writer"}]
    session = _session(
        _result(parents),
        _result(_owner()),
        _result(None),
        _result(None),
        _result(inherited_readers),
        _result(None),
        _result(None),
        _result(defaults),
        _result(None),
        _result(None),
        _result(None),
        _result(defaults),
        _result(None),
        _result(None),
        _result(None),
    )
    manifest = await initialize.initialize_projection(session, request, build)
    session.add.assert_called_once_with(manifest)
    prepared.flush.assert_awaited_once_with(session)
    assert manifest.generation_id == build.generation_id and manifest.execution_id == request.execution_id
    assert manifest.definition_sha256 == bytes.fromhex(request.definition.digest)
    assert manifest.capability_sha256 == b"k" * 32 and manifest.phase == "root"
    assert (manifest.after_entity_binding_id, manifest.root_count, manifest.child_count, manifest.index_count) == (
        0,
        0,
        0,
        0,
    )
    assert manifest.after_selected_value is None and manifest.after_child_key_sha256 is None
    assert (manifest.root_table_oid, manifest.child_table_oid) == (101, 102)
    assert (manifest.root_columns_sha256, manifest.child_columns_sha256) == (b"r" * 32, b"c" * 32)
    queries = [str(call.args[0]) for call in session.execute.call_args_list]
    assert sum(query.lstrip().startswith("CREATE TABLE ci_snapshot_17.") for query in queries) == 2
    assert sum('REVOKE ALL ON TABLE "ci_snapshot_17".' in query and "FROM PUBLIC" in query for query in queries) == 2
    assert sum('FROM "default_writer"' in query for query in queries) == 2
    assert sum('TO "read""only"' in query for query in queries) == 2
    assert not any(
        "GRANT " in query and ("PUBLIC" in query or "INSERT" in query or "UPDATE" in query) for query in queries
    )
    assert prepared.prepare.await_count == session.execute.await_count + 2
    assert [call.args[-1] for call in prepared.identity.await_args_list] == [request.definition, request.definition]
    prepared.lock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_prepare", [False, True])
async def test_existing_manifest_replays_verified_capability_without_schema_or_row_writes(prepared, explicit_prepare):
    request, build, parents = _case()
    manifest = SimpleNamespace(definition_sha256=bytes.fromhex(request.definition.digest), capability_sha256=b"k" * 32)
    if explicit_prepare:
        session = _session(_result(_owner()), _result(manifest))
        actual = await initialize.initialize_projection_parent(
            session, request.definition, parents[0], parents[3], prepare=prepared.prepare, flush=prepared.flush
        )
        prepared.lock.assert_awaited_once_with(
            session, manifest, request.definition, require_complete=False, prepare=prepared.prepare
        )
    else:
        session = _session(_result(parents), _result(_owner()), _result(manifest))
        actual = await initialize.initialize_projection(session, request, build)
        prepared.lock.assert_awaited_once_with(session, manifest, request.definition, require_complete=False)
    assert actual is manifest
    session.add.assert_not_called()
    prepared.flush.assert_not_awaited()
    prepared.identity.assert_not_awaited()
    assert session.execute.await_count == (2 if explicit_prepare else 3)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"frozen_at": "frozen"},
        {"dataset_id": 99},
        {"definition_revision_id": 99},
        {"schema_revision_id": 99},
        {"generation_id": 99},
        {"execution_id": 99},
        {"capture_bundle_id": 99},
        {"producing_fence": 99},
        {"producing_token_sha256": b"x" * 32},
    ],
)
async def test_foreign_or_frozen_parent_cannot_initialize_cells(prepared, changes):
    request, build, parents = _case(**changes)
    session = _session(_result(parents))
    with pytest.raises(initialize.CandidateRunnerError, match="producer"):
        await initialize.initialize_projection(session, request, build)
    assert session.execute.await_count == 1
    session.add.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        None,
        {"current_owner": 51},
        {"schema_owner": 51},
        {"table_owner": 51},
        {"table_oid": 999},
        {"relkind": "v"},
        {"relpersistence": "u"},
    ],
)
async def test_nonowner_or_replaced_root_relation_cannot_initialize(prepared, changes):
    request, build, parents = _case()
    session = _session(_result(parents), _result(None if changes is None else _owner(**changes)))
    with pytest.raises(initialize.CandidateRunnerError, match="existing snapshot owner"):
        await initialize.initialize_projection(session, request, build)
    assert session.execute.await_count == 2
    session.add.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transaction,changes",
    [(False, {}), (True, {"generation_id": True}), (True, {"generation_id": 0}), (True, {"generation_id": 2**63})],
)
async def test_initialization_requires_transaction_and_valid_generation(prepared, transaction, changes):
    request, build, _ = _case()
    vars(build).update(changes)
    session = _session()
    session.in_transaction = lambda: transaction
    with pytest.raises(initialize.CandidateRunnerError, match="live output page"):
        await initialize.initialize_projection(session, request, build)
    session.execute.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["execution_id", "producing_fence", "producing_token_sha256"])
async def test_build_producer_mismatch_is_rejected_before_parent_lookup(prepared, field):
    request, build, _ = _case()
    setattr(build, field, b"x" * 32 if field == "producing_token_sha256" else 99)
    session = _session()
    with pytest.raises(initialize.CandidateRunnerError, match="snapshot producer differs"):
        await initialize.initialize_projection(session, request, build)
    session.execute.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_missing_snapshot_parent_cannot_create_projection_cells(prepared):
    request, build, _ = _case()
    session = _session(_result(None))
    with pytest.raises(initialize.CandidateRunnerError, match="snapshot parent is missing"):
        await initialize.initialize_projection(session, request, build)
    assert session.execute.await_count == 1
    prepared.identity.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("contract_version", "custom-import/v2"),
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("definition_sha256", b"x" * 32),
    ],
)
async def test_saved_definition_must_match_exact_producer_before_table_access(prepared, field, value):
    request, build, parents = _case()
    setattr(parents[2], field, value)
    session = _session(_result(parents))
    with pytest.raises(initialize.CandidateRunnerError, match="definition differs"):
        await initialize.initialize_projection(session, request, build)
    assert session.execute.await_count == 1
    prepared.identity.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_reader", [False, True])
async def test_missing_acl_role_fails_before_granting_access(prepared, is_reader):
    _, _, parents = _case()
    grant_by_name = {"grantee": 51, "role_name": None}
    session = _session(_result([] if is_reader else [grant_by_name]), _result(None))
    with pytest.raises(initialize.CandidateRunnerError, match="ACL role is missing"):
        await initialize._close_and_copy_acl(
            session, parents[0], "custom_import_derived_root", 101, [grant_by_name] if is_reader else []
        )
    queries = [str(call.args[0]) for call in session.execute.call_args_list]
    assert len(queries) == 2 and "REVOKE ALL" in queries[-1]
    assert not any("GRANT" in query for query in queries)


@pytest.mark.asyncio
async def test_existing_unregistered_cell_relation_is_never_adopted(prepared):
    request, build, parents = _case()
    session = _session(_result(parents), _result(_owner()), _result(None), _result((999,)))
    with pytest.raises(initialize.CandidateRunnerError, match="unregistered"):
        await initialize.initialize_projection(session, request, build)
    assert session.execute.await_count == 4
    session.add.assert_not_called()
    prepared.identity.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["definition_sha256", "capability_sha256"])
async def test_replay_rejects_changed_definition_or_capability_before_table_access(prepared, field):
    request, build, parents = _case()
    manifest = SimpleNamespace(definition_sha256=bytes.fromhex(request.definition.digest), capability_sha256=b"k" * 32)
    setattr(manifest, field, b"x" * 32)
    session = _session(_result(parents), _result(_owner()), _result(manifest))
    with pytest.raises(initialize.CandidateRunnerError, match="replay definition or capability"):
        await initialize.initialize_projection(session, request, build)
    prepared.lock.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_nonoutput_and_failed_creation_do_not_insert_manifest(prepared):
    request, build, parents = _case()
    build.phase = "verifying"
    session = _session()
    with pytest.raises(initialize.CandidateRunnerError, match="live output page"):
        await initialize.initialize_projection(session, request, build)
    session.execute.assert_not_awaited()
    build.phase = "output"
    session = _session(
        _result(parents), _result(_owner()), _result(None), _result(None), _result([]), RuntimeError("ddl failed")
    )
    with pytest.raises(RuntimeError, match="ddl failed"):
        await initialize.initialize_projection(session, request, build)
    session.add.assert_not_called()
    prepared.flush.assert_not_awaited()
