# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private workspace checks preserve the original Publisher and frozen inputs."""

import asyncio
import hashlib
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio

from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_generic_source_policy as source
from process.provider_directory_cms_identity_candidate import CMS_IDENTITY_COLUMNS
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_cms_prepared_context import case
from tests.test_provider_directory_cms_source_handoff import _identity


def handoff_for(storage, inventory):
    identity = _identity()
    identity["metadata"]["network_bindings"]["dataset_schema"] = storage.catalog_schema
    roots = (*workspace.storage.RELATIONS, *workspace.storage.HANDOFF_CONTROL_ROOTS)
    prefix = "pdgs_" + "1" * 32
    for inventory_relation, root in zip(inventory["relations"], roots, strict=True):
        inventory_relation["relation_name"] = prefix + "_" + workspace.storage._SUFFIXES[root]
    return FHIRSourceCandidateHandoff(
        workspace.storage._encoded(
            dict(
                contract=workspace.storage.CMS_ISOLATED_CONTRACT,
                phase="ready_for_freeze",
                ordinal=1,
                resets={},
                auxiliary=None,
                relationship_sha256=None,
                identity=identity,
                storage={**asdict(storage), "producer": ["a" * 40, "sha256:" + "b" * 64]},
                admission=dict(
                    dataset_hash="c" * 64,
                    resource_count=16,
                    previous_dataset_id=None,
                    completion_proof_required_version=None,
                    completion_proof_sha256=None,
                    content_proof_admission_sha256="d" * 64,
                ),
                tables={
                    root: dict(
                        name=inventory_relation["relation_name"],
                        oid=inventory_relation["relation_oid"],
                        owner=storage.owner_oid,
                        layout_sha256="e" * 64,
                    )
                    for root, inventory_relation in zip(roots, inventory["relations"], strict=True)
                },
            )
        )
    )


def allocation_state(case):
    """Build detached fixture inputs while preserving the native row aliases."""
    handoff = handoff_for(case.storage, case.inventory)
    for inventory_relation, native_relation in zip(case.inventory["relations"], case.native_rows, strict=True):
        native_relation.update(inventory_relation)
    inputs_by_field = dict(
        source_storage=asdict(case.storage),
        source_policy=asdict(case.source_policy),
        prepared_policy=asdict(case.policy),
        frozen_inventory=case.inventory,
        origin=case.origin,
        native_coordinates=case.coordinates,
    )
    return SimpleNamespace(
        case=case,
        outputs={},
        statements=[],
        inputs=workspace._copy(inputs_by_field),
        handoff=handoff,
        preparation_id=str(uuid4()),
        admission="f" * 64,
        exists=False,
        inherit=True,
        source_closed=True,
    )


@pytest_asyncio.fixture
async def allocated(case, monkeypatch):
    """Emulate owner-scoped SQL and rollback without changing frozen inputs."""
    state = allocation_state(case)
    outputs, statements = state.outputs, state.statements

    class Savepoint:
        async def start(self):
            self.before, self.role = deepcopy(outputs), case.origin["current"]

        async def commit(self):
            return None

        async def rollback(self):
            outputs.clear()
            outputs.update(self.before)
            case.origin["current"] = self.role

    case.driver.transaction = Savepoint

    async def execute(sql):
        statements.append((case.origin["current"], sql))
        if sql.startswith("SET LOCAL ROLE"):
            case.origin["current"] = case.policy.owner_role
        elif sql == "RESET ROLE":
            case.origin["current"] = case.policy.publisher_role
        elif sql.startswith("CREATE TABLE"):
            assert case.origin["current"] == case.policy.owner_role
            name = sql.split('"')[3]
            assert name not in outputs
            outputs[name] = dict(name=name, oid=1000 + len(outputs), owner=76, layout_sha256="2" * 64)
        if sql.startswith(("ALTER TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX")):
            for receipt in outputs.values():
                receipt["layout_sha256"] = hashlib.sha256((receipt["layout_sha256"] + sql).encode()).hexdigest()

    async def fetchval(sql, *args):
        if "quote_literal" in sql:
            return "'" + args[0].replace("'", "''") + "'"
        if "to_regclass" in sql:
            return not state.exists
        if "current_user" in sql:
            return state.inherit
        assert "pg_has_role" in sql
        return state.source_closed

    async def capture(connection, policy, names, **options):
        assert connection is case.driver
        return {name: deepcopy(outputs[name]) for name in names}

    async def copy_rows(connection, session, schema, name, columns, records):
        assert case.origin["current"] == case.policy.owner_role
        assert name in outputs and schema == case.storage.storage_schema
        return len(list(records))

    case.driver.execute = AsyncMock(side_effect=execute)
    case.driver.fetchval = AsyncMock(side_effect=fetchval)
    monkeypatch.setattr(workspace.storage, "_capture", capture)
    monkeypatch.setattr(workspace, "storage_roles", AsyncMock(return_value=(76, [])))
    monkeypatch.setattr(workspace, "copy_identity_rows_native", AsyncMock(side_effect=copy_rows))
    yield state


def scope(state, **changes):
    arguments_by_name = dict(
        source_binding=state.case.storage,
        prepared_inputs=state.inputs,
        handoff=state.handoff,
        preparation_id=state.preparation_id,
        admission_sha256=state.admission,
    )
    arguments_by_name.update(changes)
    return workspace.prepared_cms_workspace(
        state.case.database, state.case.session, state.case.driver, **arguments_by_name
    )


async def test_fixed_new_outputs_keep_publisher_and_never_reopen_inputs(allocated):
    state, case = allocated, allocated.case
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        async with scope(state) as current:
            assert case.origin["current"] == case.policy.publisher_role
            assert len(current.receipts) == len(state.outputs) == 11
            assert current.source_binding is case.storage and case.storage.owner_oid == 74
            assert current.identity == state.handoff.document["identity"]
            assert current.catalog_schema == case.storage.catalog_schema
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            detached = current.receipts
            detached.clear()
            assert len(current.receipts) == 11
            await current.finish_indexes()
            assert current.binding["indexed"]
        with pytest.raises(ValueError, match="scope_ended"):
            await current.verify_current()
    creates = [sql for _, sql in state.statements if sql.startswith("CREATE TABLE")]
    assert len(creates) == 11
    content_indexes = [sql for _, sql in state.statements if "_content_seek" in sql]
    assert len(content_indexes) == 5
    assert all('COLLATE "C"' in sql for sql in content_indexes)
    assert not any(
        "GRANT" in sql or "REVOKE" in sql or "REFERENCES" in sql or '"catalog".' in sql for _, sql in state.statements
    )
    assert not any(row["relation_name"] in sql for row in case.inventory["relations"] for sql in creates)


async def test_payloads_keep_fixed_check_names_for_later_native_attachment(allocated):
    state, case = allocated, allocated.case
    async with case.session.begin(), case.database.bind_existing_session(case.session), scope(state) as current:
        receipts = current.receipts
    for root in workspace.storage.RELATIONS:
        target = workspace.storage_relation(case.policy.schema, receipts[root]["name"])
        statement = next(sql for _, sql in state.statements if sql.startswith("CREATE TABLE " + target + " "))
        for key, kind, definition, _columns in workspace._LAYOUTS[root]["constraints"]:
            if kind == "c":
                expected = (
                    "CHECK (resolution_status IN ('resolved', 'unresolved', 'ambiguous'))"
                    if key == "cms_npd_relationship_status_check"
                    else definition
                )
                assert "CONSTRAINT " + workspace.storage_identifier(key) + " " + expected in statement
        assert 'CONSTRAINT "pd_dataset_bound" CHECK (dataset_id=' in statement
        assert "REFERENCES" not in statement and "FOREIGN KEY" not in statement


@pytest.mark.parametrize("change", ["handoff", "preparation", "admission"])
def test_names_are_deterministic_for_exact_preparation_and_handoff(allocated, change):
    state = allocated
    original = workspace._binding(
        state.case.storage, state.inputs, state.handoff, state.preparation_id, state.admission
    )
    assert workspace._names(original) == workspace._names(deepcopy(original))
    changed = deepcopy(original)
    changed[{"handoff": "handoff_sha256", "preparation": "preparation_id", "admission": "admission_sha256"}[change]] = (
        "different"
    )
    assert not set(workspace._names(original).values()) & set(workspace._names(changed).values())


@pytest.mark.parametrize("failure", ["exists", "inherit", "source_closed"])
async def test_existing_objects_or_incompatible_native_roles_fail_before_create(allocated, failure):
    state, case = allocated, allocated.case
    setattr(state, failure, failure == "exists")
    with pytest.raises(ValueError, match="output_exists|inheritance_required|source_reaches_prepared_owner"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), scope(state):
            pytest.fail("unsafe input reached the workspace")
    assert not state.outputs and not any(sql.startswith("CREATE TABLE") for _, sql in state.statements)


@pytest.mark.parametrize("error", [RuntimeError("caller failure"), asyncio.CancelledError()])
async def test_failure_or_cancel_rolls_back_all_new_outputs_and_restores_publisher(allocated, error):
    state, case = allocated, allocated.case
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(type(error)):
            async with scope(state):
                assert len(state.outputs) == 11
                raise error
        assert not state.outputs and case.origin["current"] == case.policy.publisher_role


async def test_late_frozen_metadata_drift_also_rolls_back_workspace_savepoint(allocated):
    state, case = allocated, allocated.case
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="frozen_inventory_changed"):
            async with scope(state):
                case.native_rows[0]["relfilenode"] = 999
        assert not state.outputs and case.origin["current"] == case.policy.publisher_role


async def test_copy_uses_owner_only_in_method_and_refuses_cross_source_rows(allocated):
    state, case = allocated, allocated.case
    root = workspace.storage.CMS_IDENTITY_ROOTS[0]
    from datetime import datetime, timezone

    row = ("cms-npd", "Organization", "organization", uuid4(), None, datetime.now(timezone.utc))
    async with case.session.begin(), case.database.bind_existing_session(case.session), scope(state) as current:
        assert await current.copy_rows(root, CMS_IDENTITY_COLUMNS[root], [row]) == 1
        assert case.origin["current"] == case.policy.publisher_role
        with pytest.raises(ValueError, match="copy_identity_changed"):
            await current.copy_rows(root, CMS_IDENTITY_COLUMNS[root], [("other-source", *row[1:])])
        assert case.origin["current"] == case.policy.publisher_role
        with pytest.raises(ValueError, match="copy_target_changed"):
            await current.copy_rows(workspace.storage.RESOURCE, (), [])
        await current.finish_indexes()
        with pytest.raises(ValueError, match="already_indexed"):
            await current.copy_rows(root, CMS_IDENTITY_COLUMNS[root], [row])


async def test_receipt_drift_or_child_task_cannot_use_workspace(allocated):
    state, case = allocated, allocated.case
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="output_changed"):
            async with scope(state) as current:
                with pytest.raises(ValueError, match="original_task_required"):
                    await asyncio.create_task(current.verify_current())
                next(iter(state.outputs.values()))["oid"] = 99999
                await current.verify_current()
        assert not state.outputs


@pytest.mark.parametrize("change", ["original_storage", "frozen_name", "handoff_kind"])
async def test_detached_observation_or_handoff_cannot_choose_another_input(allocated, change):
    state, case = allocated, allocated.case
    if change == "original_storage":
        state.inputs["source_storage"]["owner_oid"] = 99
    elif change == "frozen_name":
        state.inputs["frozen_inventory"]["relations"][0]["relation_name"] = "different"
    else:
        state.handoff = object()
    with pytest.raises(ValueError, match="source_binding_changed|frozen_handoff_changed|handoff_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), scope(state):
            pytest.fail("changed input reached mutable work")
    assert not state.outputs


def test_code_owned_layouts_include_payload_snapshots_without_foreign_keys_or_controls():
    layouts = workspace._layouts()
    assert set(layouts) == set(workspace.OUTPUT_ROOTS)
    assert set(workspace.storage.RELATIONS) <= set(layouts)
    assert not set(layouts) & set(workspace.storage.HANDOFF_CONTROL_ROOTS)
    assert not any(
        "REFERENCES" in item or "FOREIGN KEY" in item
        for columns, _, checks in layouts.values()
        for item in (*columns, *checks)
    )
    assert not hasattr(workspace, "finalize_frozen_cms_source_candidate")


@pytest.mark.parametrize("change", ["producer", "files", "network_schema"])
async def test_workspace_rejects_invalid_full_cms_producer_before_any_sql(allocated, change):
    state, case = allocated, allocated.case
    document = state.handoff.document
    metadata = document["identity"]["metadata"]
    if change == "producer":
        metadata["network_bindings"]["producer_id"] = "other-producer"
    elif change == "files":
        metadata["source_release"]["files"].pop(next(iter(metadata["source_release"]["files"])))
    else:
        metadata["network_bindings"]["dataset_schema"] = "private_source"
    handoff = FHIRSourceCandidateHandoff(workspace.storage._encoded(document))
    with pytest.raises(ValueError, match="isolated_cms_"):
        async with (
            case.session.begin(),
            case.database.bind_existing_session(case.session),
            scope(state, handoff=handoff),
        ):
            pytest.fail("invalid CMS producer was accepted")
    assert not state.outputs and not state.statements


async def _retained_binding(state):
    async with scope(state) as current:
        await current.finish_indexes()
        return current.binding


def _reopen(state, binding):
    case = state.case
    return workspace.reopen_prepared_cms_workspace(
        case.database,
        case.session,
        case.driver,
        source_binding=case.storage,
        prepared_inputs=state.inputs,
        handoff=state.handoff,
        expected_workspace=binding,
    )


async def test_retained_reopen_preserves_outputs_and_refuses_mutation(allocated):
    state, case = allocated, allocated.case
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        binding = await _retained_binding(state)
        before = deepcopy(state.outputs)
        state.statements.clear()
        async with _reopen(state, binding) as current:
            assert current.binding == binding
            for operation in (current.finish_indexes(), current.copy_rows("invalid", (), ())):
                with pytest.raises(ValueError, match="readonly_workspace"):
                    await operation
            with pytest.raises(ValueError, match="original_task_required"):
                await asyncio.create_task(current.verify_current())
        assert state.outputs == before
        assert all("LOCK TABLE" in statement for _, statement in state.statements)
        with pytest.raises(ValueError, match="scope_ended"):
            await current.verify_current()


@pytest.mark.parametrize("fault", ["indexed", "identity", "root", "name", "oid", "owner", "layout"])
async def test_retained_reopen_rejects_changed_identity_or_objects(allocated, fault):
    state, case = allocated, allocated.case
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        binding = await _retained_binding(state)
        receipt = next(iter(binding["tables"].values()))
        if fault == "indexed":
            binding["indexed"] = False
        elif fault == "identity":
            binding["admission_sha256"] = "9" * 64
        elif fault == "root":
            binding["tables"].pop(next(iter(binding["tables"])))
        else:
            receipt[{"layout": "layout_sha256"}.get(fault, fault)] = {
                "name": "other",
                "oid": 999999,
                "owner": 8888,
                "layout": "9" * 64,
            }[fault]
        before = deepcopy(state.outputs)
        with pytest.raises(ValueError, match="retained_|output_changed"):
            async with _reopen(state, binding):
                pytest.fail("changed retained workspace was accepted")
        assert state.outputs == before
