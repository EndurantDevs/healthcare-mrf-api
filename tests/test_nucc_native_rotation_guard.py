# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Known mutation guards retire atomically before an immutable NUCC heap rotates."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_registration as registration
from process import provider_directory_generic_source_provision as provision
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_cms_native_input_registration import NativeProtocol


class RotationProtocol(NativeProtocol):
    def __init__(self):
        super().__init__(target_root=("sample", "nucc_taxonomy"))
        self.events = []

    async def fetch(self, sql, *args):
        if sql.startswith("SELECT tgname::text FROM pg_trigger"):
            return [dict(tgname=guard["tgname"]) for guard in self.guards.get(self.target_root, [])]
        return await super().fetch(sql, *args)

    async def execute(self, sql, *args):
        if sql == 'DROP TRIGGER "cms_native_input_mutation" ON "sample"."nucc_taxonomy"':
            self.events.append("drop")
            self.guards[self.target_root] = []
            return "DROP TRIGGER"
        return await super().execute(sql, *args)


async def _registered_rotation(monkeypatch):
    driver = RotationProtocol()
    candidate = registration.NativeInputCandidate("sample", "nucc_taxonomy", "sample", "nucc_taxonomy")
    await registration.register_native_input_candidates(driver, ledger_schema="sample", candidates=(candidate,))
    driver.rows[driver.target_root]["mutation_xid"] = None
    session = SimpleNamespace(begin_nested=driver.transaction)
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=driver))

    async def freeze(*args, **kwargs):
        assert args == (driver, "sample")
        assert kwargs == dict(
            namespace_owner="snapshot_owner",
            reader_roles=("snapshot_owner", "publisher", "reader"),
            application_roles=(),
            relation_names=("nucc_taxonomy",),
            freeze_schema=False,
        )
        driver.events.append("freeze")

    frozen = AsyncMock(side_effect=freeze)
    monkeypatch.setattr(provision, "freeze_fhir_source_snapshot", frozen)
    roles = SimpleNamespace(owner_role="snapshot_owner", publisher_role="publisher", runtime_roles=("reader",))
    return driver, session, roles, frozen


@pytest.mark.asyncio
async def test_known_guard_retires_after_freeze_without_rewriting_revision_history(monkeypatch):
    driver, session, roles, frozen = await _registered_rotation(monkeypatch)
    history = deepcopy(driver.rows)
    await generation._retire_nucc_native_counter(session, "sample", roles)
    assert driver.events == ["freeze", "drop"]
    assert driver.guards[driver.target_root] == [] and driver.rows == history
    frozen.assert_awaited_once()


@pytest.mark.asyncio
async def test_caught_validation_failure_restores_the_original_guard_and_counter(monkeypatch):
    driver, session, roles, _frozen = await _registered_rotation(monkeypatch)
    original = deepcopy((driver.rows, driver.guards))
    monkeypatch.setattr(
        registration, "verify_retired_native_input_adoption", AsyncMock(side_effect=RuntimeError("changed"))
    )
    with pytest.raises(RuntimeError, match="changed"):
        await generation._retire_nucc_native_counter(session, "sample", roles)
    assert driver.events == ["freeze", "drop"]
    assert (driver.rows, driver.guards) == original


@pytest.mark.asyncio
async def test_unknown_additional_hook_cannot_be_retired_or_frozen(monkeypatch):
    driver, session, roles, frozen = await _registered_rotation(monkeypatch)
    driver.guards[driver.target_root].append(dict(tgname="unknown_hook"))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="hooks differ"):
        await generation._retire_nucc_native_counter(session, "sample", roles)
    frozen.assert_not_awaited()
    assert not driver.events


@pytest.mark.asyncio
async def test_unregistered_heap_keeps_the_existing_validation_path(monkeypatch):
    driver, session, roles, frozen = await _registered_rotation(monkeypatch)
    driver.guards[driver.target_root] = []
    await generation._retire_nucc_native_counter(session, "sample", roles)
    frozen.assert_not_awaited()
    assert not driver.events


@pytest.mark.asyncio
async def test_hook_change_during_freeze_refuses_before_retirement(monkeypatch):
    driver, session, roles, frozen = await _registered_rotation(monkeypatch)
    original = deepcopy((driver.rows, driver.guards))

    async def changed(*_args, **_kwargs):
        driver.guards[driver.target_root].append(dict(tgname="unknown_hook"))

    frozen.side_effect = changed
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="hooks differ"):
        await generation._retire_nucc_native_counter(session, "sample", roles)
    assert (driver.rows, driver.guards) == original
    assert "drop" not in driver.events


def _metadata_rotation_session(events):
    """Model the existing savepoint boundary while recording only native DCL order."""
    import asyncio
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield
        except BaseException:
            events.append("rollback-metadata")
            raise
        else:
            events.append("release-metadata")

    session = SimpleNamespace(
        execute=AsyncMock(side_effect=lambda statement: events.append(str(statement))),
        begin_nested=savepoint,
        connection=AsyncMock(),
    )
    return session


def _index_rotation_steps(monkeypatch, session, incoming, dataset, events, rename_failure):
    """Fail only the requested native metadata phase and retain guard/namespace assertions."""
    import asyncio

    async def rename(actual_session, *, schema_name):
        assert actual_session is session and schema_name == incoming
        events.append("rename-indexes")
        if rename_failure == "error":
            raise RuntimeError("native index rename failed")
        if rename_failure == "cancel":
            raise asyncio.CancelledError()

    async def retire(actual_session, schema, roles):
        assert actual_session is session and schema == "sample" and roles is None
        events.append("retire-known-guard")

    async def create_schema(actual_session, importer, schema):
        assert actual_session is session and importer == "nucc"
        assert schema == archive.reference_family_predecessor_schema(dataset)
        events.append("create-predecessor")

    async def freeze(actual_session, schema, roles):
        assert actual_session is session and schema == archive.reference_family_predecessor_schema(dataset)
        assert roles is None
        events.append("freeze-predecessor")

    monkeypatch.setattr(generation, "_freeze_nucc_predecessor", freeze)
    monkeypatch.setattr(generation, "_retire_nucc_native_counter", retire)
    monkeypatch.setattr(archive, "create_model_family_predecessor_schema", create_schema)
    monkeypatch.setattr(archive, "rename_nucc_published_indexes", rename)


@pytest.mark.asyncio
@pytest.mark.parametrize("rename_failure", (None, "error", "cancel"))
@pytest.mark.parametrize("has_incumbent", (False, True))
async def test_incoming_index_names_are_freed_before_generic_nucc_schema_rotation(
    monkeypatch, rename_failure, has_incumbent
):
    """Candidate metadata permission closes before moves; failure rolls the DDL grant back."""
    import asyncio
    from uuid import uuid4

    events = []
    session = _metadata_rotation_session(events)
    monkeypatch.setattr(archive, "model_family_roles", lambda _roles: ("snapshot_owner", ("reader",)))
    dataset = uuid4()
    incoming = archive.reference_family_stage_schema(dataset)
    ownership = SimpleNamespace(schema_name=incoming, dataset_id=dataset)
    incumbent = SimpleNamespace(schema_name="sample", relation_oids=(("nucc_taxonomy", 41 if has_incumbent else None),))

    _index_rotation_steps(monkeypatch, session, incoming, dataset, events, rename_failure)
    expected_events = (["retire-known-guard"] if has_incumbent else []) + [
        "savepoint",
        f'GRANT CREATE ON SCHEMA "{incoming}" TO "snapshot_owner"',
        "rename-indexes",
    ]
    if rename_failure:
        with pytest.raises(asyncio.CancelledError if rename_failure == "cancel" else RuntimeError):
            await generation._rotate_family_relations(
                session, archive.reference_family_spec("nucc"), ownership, incumbent
            )
        assert events == expected_events + ["rollback-metadata"]
        assert session.execute.await_count == 1
    else:
        expected_events += [f'REVOKE CREATE ON SCHEMA "{incoming}" FROM "snapshot_owner"', "release-metadata"]
        predecessor = archive.reference_family_predecessor_schema(dataset) if has_incumbent else None
        assert (
            await generation._rotate_family_relations(
                session, archive.reference_family_spec("nucc"), ownership, incumbent
            )
            == predecessor
        )
        expected_events += (
            ["create-predecessor", f'ALTER TABLE "sample"."nucc_taxonomy" SET SCHEMA "{predecessor}"']
            if has_incumbent
            else []
        )
        expected_events += [f'ALTER TABLE "{incoming}"."nucc_taxonomy" SET SCHEMA "sample"']
        expected_events += ["freeze-predecessor"] if has_incumbent else []
        assert events == expected_events
        assert not any("DROP" in statement or "CREATE INDEX" in statement for statement in events)


@pytest.mark.asyncio
async def test_retired_revision_history_cannot_adopt_a_guardless_heap(monkeypatch):
    """Retirement retains revision rows, while strict mutable adoption still requires its live guard."""
    driver, session, roles, _frozen = await _registered_rotation(monkeypatch)
    original = deepcopy(driver.rows)
    await generation._retire_nucc_native_counter(session, "sample", roles)
    with pytest.raises(RuntimeError, match="cms_native_input_registration_changed"):
        await registration.capture_native_input_adoption(
            driver,
            ledger_schema="sample",
            candidate=registration.NativeInputCandidate("sample", "nucc_taxonomy", "sample", "nucc_taxonomy"),
        )
    assert driver.rows == original and driver.guards[driver.target_root] == []


@pytest.mark.asyncio
async def test_immutable_observer_accepts_closed_clone_with_retired_revision_history(monkeypatch):
    """An immutable replacement has no mutation guard; retained counter rows are historical metadata."""
    driver, session, roles, _frozen = await _registered_rotation(monkeypatch)
    original = deepcopy(driver.rows)
    await generation._retire_nucc_native_counter(session, "sample", roles)
    columns, dependencies = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    monkeypatch.setattr(archive, "require_native_read_catalog", dependencies)
    index_result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))

    async def scalar(statement, _parameters):
        return bool(driver.guards[driver.target_root]) if "SELECT EXISTS" in str(statement) else 42

    observer = SimpleNamespace(scalar=scalar, execute=AsyncMock(return_value=index_result))
    assert await archive._observe_nucc_native_stage(observer, "sample", "nucc_taxonomy", native_revision=True) == {
        "table_name": "nucc_taxonomy",
        "indexes": [],
    }
    assert driver.rows == original and driver.guards[driver.target_root] == []
    columns.assert_awaited_once_with(observer, 42)
    dependencies.assert_awaited_once_with(observer, (42,), allowed_type_oids=())


@pytest.mark.asyncio
async def test_immutable_observer_keeps_live_guard_validation_strict(monkeypatch):
    """Only the known semantic hook can enter mutable adoption before native model reads."""
    driver, _session, _roles, _frozen = await _registered_rotation(monkeypatch)
    driver.guards[driver.target_root][0]["safe"] = False
    columns = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    observer = SimpleNamespace(scalar=AsyncMock(return_value=True), execute=AsyncMock())
    with pytest.raises(RuntimeError, match="cms_native_input_registration_changed"):
        await archive._observe_nucc_native_stage(observer, "sample", "nucc_taxonomy", native_revision=True)
    columns.assert_not_awaited()
    observer.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_namespace_uses_configured_full_freeze_after_schema_transfer(monkeypatch):
    """Regrant native SELECT and schema USAGE only to existing configured readers."""
    events = []
    driver = SimpleNamespace(execute=AsyncMock(side_effect=lambda sql: events.append(sql)))
    session = object()
    roles = SimpleNamespace(owner_role="snapshot_owner", publisher_role="publisher", runtime_roles=("reader",))
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=driver))
    frozen = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("freeze"))
    monkeypatch.setattr(provision, "freeze_fhir_source_snapshot", frozen)
    await generation._freeze_nucc_predecessor(session, "reference_family_predecessor_" + "a" * 32, roles)
    frozen.assert_awaited_once_with(
        driver,
        "reference_family_predecessor_" + "a" * 32,
        namespace_owner="snapshot_owner",
        reader_roles=("snapshot_owner", "publisher", "reader"),
        application_roles=(),
        relation_names=("nucc_taxonomy",),
    )
    assert events == [
        'ALTER SCHEMA "reference_family_predecessor_' + "a" * 32 + '" OWNER TO "snapshot_owner"',
        "freeze",
    ]
