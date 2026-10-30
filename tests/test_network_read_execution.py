# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Executed markers guard buffered results before application or cache exposure."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import select, text

from api import network_read_execution as execution
from api.network_address_scope import network_address_read_scope, scoped_address_relation_sql, scoped_address_statement
from db.models import EntityAddressUnified
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_read_identity import NetworkReadIdentityError
from process.network_serving_read import PinnedNetworkServingManifest


def _manifest(*, compact=False):
    candidate_id = str(UUID(int=1))
    manifest = PinnedNetworkServingManifest(
        1, candidate_id, "network_candidate_" + UUID(candidate_id).hex, 1, {}, 0, "a" * 64, 20
    )
    if compact:
        object.__setattr__(
            manifest,
            "_compact_layout",
            CompactNetworkLayout(
                CompactAddressRelation(manifest.schema_name, "network_slice_ref", 21),
                CompactAddressRelation("address_snapshot", "entity_address_unified", 20),
                (),
            ),
        )
    return manifest


@pytest.mark.parametrize("compact", [False, True])
def test_raw_and_typed_markers_are_stable_and_cover_only_rendered_relations(compact):
    manifest = _manifest(compact=compact)
    with network_address_read_scope(manifest, (42,)):
        first = scoped_address_relation_sql("mrf.entity_address_unified")
        assert first == scoped_address_relation_sql("mrf.entity_address_unified")
        statement = scoped_address_statement(select(EntityAddressUnified.__table__.c.location_key))
        raw_pins = execution.network_read_relations(text(f"SELECT * FROM {first} office"))
        assert execution.network_read_relations(statement) == raw_pins
        assert raw_pins == (
            CompactAddressRelation(
                "address_snapshot" if compact else manifest.schema_name, "entity_address_unified", 20
            ),
        )
        assert execution.network_read_relations(text("SELECT :_canonical_network_ids")) == ()
    with pytest.raises(ServiceUnavailable):
        execution.network_read_relations(statement)


@pytest.mark.parametrize("marker", ["/* network-read:" + "0" * 64 + " */", "/* network-read:bad */"])
def test_unknown_or_malformed_markers_never_execute(marker):
    with network_address_read_scope(_manifest(), (42,)):
        with pytest.raises(ServiceUnavailable):
            execution.network_read_relations(text("SELECT " + marker + " 1"))


def _session(events):
    result = SimpleNamespace(close=Mock(), all=lambda: [("office",)])
    transaction = object()
    driver = SimpleNamespace(get_server_pid=lambda: 42, is_in_transaction=lambda: True)
    raw = SimpleNamespace(driver_connection=driver)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw))

    async def execute(*_arguments):
        events.append("execute")
        return result

    return (
        SimpleNamespace(
            in_transaction=lambda: True,
            get_transaction=lambda: transaction,
            connection=AsyncMock(return_value=connection),
            execute=AsyncMock(side_effect=execute),
        ),
        driver,
        result,
    )


@pytest.mark.parametrize("refuse", [False, True])
async def test_guard_runs_on_original_backend_before_caller_and_cache(monkeypatch, refuse):
    events, cached = [], []
    session, driver, result = _session(events)

    async def verify(actual, relations, *, expected_backend_pid):
        assert actual is driver and expected_backend_pid == 42 and len(relations) == 1
        assert events == ["execute"]
        events.append("guard")
        if refuse:
            raise NetworkReadIdentityError("changed_relation")

    monkeypatch.setattr(execution, "verify_consumed_network_relations", verify)
    with network_address_read_scope(_manifest(), (42,)):
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        operation = execution.execute_network_read(session, text(f"SELECT * FROM {relation} office"))
        if refuse:
            with pytest.raises(ServiceUnavailable):
                await operation
            result.close.assert_called_once()
        else:
            observed = await operation
            events.append("caller")
            cached.extend(observed.all())
    assert events == (["execute", "guard"] if refuse else ["execute", "guard", "caller"])
    assert cached == ([] if refuse else [("office",)])


async def test_unmarked_statement_does_not_open_another_connection(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock(return_value=object()))
    guard = AsyncMock()
    monkeypatch.setattr(execution, "verify_consumed_network_relations", guard)
    assert await execution.execute_network_read(session, text("SELECT 1")) is session.execute.return_value
    guard.assert_not_awaited()


@pytest.mark.parametrize("refusing_probe", [1, 2], ids=["provider", "taxonomy"])
async def test_late_enrichment_identity_failure_refuses_partial_procedure_context(monkeypatch, refusing_probe):
    from api import ptg2_serving as serving

    events = []
    session, driver, buffered = _session(events)
    buffered.scalar = lambda: None
    relation = CompactAddressRelation("snapshot", "entity_address_unified", 23)

    async def address_table(*_args, **_kwargs):
        events.append("address-resolved")
        return "snapshot.entity_address_unified"

    async def verify(actual, relations, *, expected_backend_pid):
        assert actual is driver and relations == (relation,) and expected_backend_pid == 42
        assert events[0] == "address-resolved"
        if session.execute.await_count == refusing_probe:
            raise NetworkReadIdentityError("changed_relation")

    monkeypatch.setattr(serving, "_ptg2_address_serving_table", address_table)
    monkeypatch.setattr(execution, "verify_bound_network_names", verify)
    monkeypatch.setattr(serving, "_hydrate_provider_set_network_names", AsyncMock())
    monkeypatch.setattr(serving, "_procedure_details_for_rows", AsyncMock(return_value={}))
    monkeypatch.setattr(serving, "_logical_source_key", lambda *_args: "synthetic-source")
    request = SimpleNamespace(npi=1234567890, args={}, snapshot_id="synthetic-snapshot", serving_tables=object())
    search = SimpleNamespace(request=request, requested_plan="")
    with execution.network_read_name_inventory(driver, (relation,)):
        with pytest.raises(ServiceUnavailable) as refused:
            await serving._provider_procedure_item_context(session, search, SimpleNamespace(serving_rows=[]))
    assert isinstance(refused.value.__cause__, NetworkReadIdentityError)
    assert refused.value.headers["Cache-Control"] == "private, no-store"
    assert session.execute.await_count == refusing_probe
    buffered.close.assert_called_once()


@pytest.mark.parametrize("refuse", [False, True])
async def test_billing_sidecar_guard_precedes_decoding(monkeypatch, refuse):
    from api import ptg2_billing_associations as billing

    events = []
    session, driver, buffered = _session(events)
    reference = "ab" * billing._PROVIDER_GROUP_REF_BYTES
    relation = CompactAddressRelation("snapshot", "ptg2_provider_group_tax_identity", 23)

    async def verify(actual, relations, *, expected_backend_pid):
        assert actual is driver and relations == (relation,) and expected_backend_pid == 42
        assert events == ["execute"]
        events.append("guard")
        if refuse:
            raise NetworkReadIdentityError("changed_relation")

    def mappings():
        assert events == ["execute", "guard"]
        events.append("decode")
        return [{"provider_group_ref": reference}]

    buffered.mappings = mappings
    monkeypatch.setattr(execution, "verify_bound_network_names", verify)
    monkeypatch.setattr(billing, "_sidecar_state", lambda _row: "legacy")
    with execution.network_read_name_inventory(driver, (relation,)):
        operation = billing.load_provider_group_billing_associations(
            session, schema_name="snapshot", snapshot_key=1, provider_group_refs=(reference,)
        )
        if refuse:
            with pytest.raises(ServiceUnavailable):
                await operation
            buffered.close.assert_called_once()
        else:
            assert set(await operation) == {reference}
    assert events == (["execute", "guard"] if refuse else ["execute", "guard", "decode"])


def test_em_page_accepts_only_the_current_compact_physical_parameters():
    from api.network_address_scope import scoped_address_parameters
    from api.plan_pricing_em_distance import _validate_page_parameters

    manifest = _manifest(compact=True)
    artifact = CompactEdgeArtifact(manifest.candidate_id, manifest.schema_name, 22)
    layout = manifest.compact_layout
    object.__setattr__(manifest, "_compact_layout", CompactNetworkLayout(layout.ref_map, layout.base, (artifact,)))
    with network_address_read_scope(manifest, (42,)):
        parameters = scoped_address_parameters({"allowed_binding_ordinals": [1], "code_index": 0})
        _validate_page_parameters(parameters)
        for changed in (
            {**parameters, "candidate_limit": 100000},
            {**parameters, "_compact_artifact_1000": "untrusted"},
        ):
            with pytest.raises(ValueError):
                _validate_page_parameters(changed)
        for key in parameters:
            if key.startswith("_compact_"):
                with pytest.raises(ValueError, match="network_address_reserved_parameter"):
                    _validate_page_parameters({**parameters, key: "untrusted"})


@pytest.mark.parametrize("method", ["first", "scalar"])
async def test_proxy_single_result_is_guarded_before_exposure(monkeypatch, method):
    events = []
    session, driver, buffered = _session(events)
    decode = Mock(return_value="selected-office")
    setattr(buffered, method, decode)
    verify = AsyncMock()
    monkeypatch.setattr(execution, "verify_consumed_network_relations", verify)
    with network_address_read_scope(_manifest(), (42,)):
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        proxy = execution.NetworkReadConnectionProxy(None, session, driver)
        assert await getattr(proxy, method)(text(f"SELECT * FROM {relation} office")) == "selected-office"
    verify.assert_awaited_once()
    assert verify.await_args.args[0] is driver
    decode.assert_called_once_with()


@pytest.mark.parametrize("damage", ["driver", "unbound_only_relation"])
async def test_inventory_extension_cannot_change_backend_or_claim_unbound_heap(damage):
    from tests.test_network_optional_sql_recovery import _PIN, _Driver, _installed

    session = await _installed()
    previous = session.info[execution._SESSION_INVENTORY_KEY]
    if damage == "driver":
        session.driver = _Driver(session)
        only_relations = (_PIN,)
    else:
        only_relations = (CompactAddressRelation("held", "unbound", 24),)
    with pytest.raises(ServiceUnavailable):
        await execution.install_network_read_inventory(session, (_PIN,), only_relations=only_relations)
    assert session.info[execution._SESSION_INVENTORY_KEY] is previous


def test_empty_inventory_requires_exact_tuple_contract_without_connection():
    before = execution._NAME_INVENTORIES.get()
    with pytest.raises(ServiceUnavailable):
        with execution.network_read_name_inventory(None, []):
            pytest.fail("a mutable empty inventory was admitted")
    assert execution._NAME_INVENTORIES.get() is before


async def test_original_driver_requires_a_real_root_transaction():
    session = SimpleNamespace(in_transaction=lambda: True, get_transaction=lambda: None, connection=AsyncMock())
    with pytest.raises(ServiceUnavailable):
        await execution._original_read_driver(session)
    session.connection.assert_not_awaited()


async def test_native_non_database_failure_cannot_authorize_optional_recovery():
    from tests.test_network_optional_sql_recovery import _installed

    error = RuntimeError("decoder unavailable")
    session = await _installed(error)
    with pytest.raises(ServiceUnavailable) as caught:
        await execution.bounded_network_read_rows(session, 'SELECT id FROM ONLY "held"."parent"')
    assert type(caught.value) is not execution._ReadSQLFailure
    assert caught.value.__cause__ is error


async def test_owned_savepoint_must_actually_finish_rollback_before_recovery():
    from tests.test_network_optional_sql_recovery import _installed, _query

    session = await _installed(asyncpg.DivisionByZeroError("failed query"))
    async with session.begin_nested() as savepoint:
        with pytest.raises(execution._ReadSQLFailure) as failed:
            await _query(session)
        savepoint.rollback = AsyncMock()
        with pytest.raises(ServiceUnavailable):
            await execution._rollback_optional_read(session, failed.value, savepoint, session.transaction, None)
        savepoint.rollback.assert_awaited_once()
        assert failed.value.recovery_started is False


async def test_recovery_refuses_backend_drift_during_name_revalidation():
    from api.provider_profile_snapshot import provider_optional_read
    from tests.test_network_optional_sql_recovery import _installed, _query

    session = await _installed(asyncpg.DivisionByZeroError("failed query"))
    session.after_guard = lambda: setattr(session.driver, "pid", 43)
    with pytest.raises(ServiceUnavailable):
        async with provider_optional_read(session, SimpleNamespace(debug=Mock())):
            await _query(session)
    assert session.events == ["enter", "query", "rollback", "exit", "names"]
