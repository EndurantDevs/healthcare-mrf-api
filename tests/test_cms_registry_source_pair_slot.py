# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed receipt encoding and control-flow regressions; no native authority claim."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import entity_address_snapshot_receipt as address
from process import network_cms_registry_source_pair as retained
from process import network_registry_cms_prepared_cleanup as cleanup
from process import network_registry_cms_prepared_pair as prepared
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving_receipt as receipts
from process.entity_address_snapshot_alias import EntityAddressAliasAuthority
from process.entity_address_snapshot_ownership import EntityAddressArchiveStageOwnership
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_source_epoch import _ORIGIN_FIELDS, _TABLES, RetainedCMSFHIRSourceEpoch
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.provider_directory_cms_capacity_test_support import cms_execution
from tests.test_provider_directory_cms_preflight import _retention_policy
from tests.test_provider_directory_cms_serving_receipt import _payload
from tests.test_provider_directory_cms_source_runtime import registry_publication_case as registry_publication_case


def _prepared():
    """Construct synthetic closed values, without asserting database custody."""
    policy = _retention_policy(cms_execution())
    pin = PinnedFHIRMembershipSource(**policy["source_pin"])
    capture = UUID(policy["capture_id"])
    request = prepared.RegistryCMSRetentionRequest(
        pin,
        RegistryNetworkSourceCoordinates(**policy["binding_coordinates"]),
        policy["selection_proof_id"],
        policy["expected_admission_sha256"],
        policy["expected_metadata_sha256"],
        policy["extra_data_upper_bound_bytes"],
        policy["extra_wal_upper_bound_bytes"],
    )
    epoch = RetainedCMSFHIRSourceEpoch(
        tuple(getattr(pin, field) for field in _ORIGIN_FIELDS),
        capture,
        "registry_cms_epoch_" + capture.hex,
        98,
        tuple((name, index) for index, name in enumerate(sorted(_TABLES), 100)),
        policy["owner_role"],
        tuple(policy["runtime_roles"]),
        request.expected_admission_sha256,
        request.expected_metadata_sha256,
        "a" * 64,
        "b" * 64,
    )
    models = (*address._models(), EntityAddressAliasAuthority)
    tables = tuple(
        address.EntityAddressArchiveTableReceipt(model.__name__, model.__tablename__, "c" * 64, 0) for model in models
    )
    receipt = address.EntityAddressArchiveReceipt(
        tables, address._table_schema_digest(tables), contract=address.CONTRACT
    )
    ownership = EntityAddressArchiveStageOwnership(
        capture,
        "entity_address_archive_" + capture.hex,
        99,
        tuple((name, index) for index, name in enumerate(sorted(model.__tablename__ for model in models), 200)),
    )
    return prepared.PreparedRegistryCMSSourcePair(
        request,
        RegistrySourceMembershipRecipe(replace(pin, retained_epoch=epoch), request.binding_coordinates),
        ownership,
        receipt,
        "d" * 64,
        tuple((name, name + "_candidate", index) for index, name in enumerate(sorted(prepared.RELATION_NAMES), 300)),
        epoch.owner_role,
        epoch.runtime_roles,
        "e" * 64,
        "f" * 64,
        "a" * 64,
    )


def _row(pair):
    payload = _payload()
    pin = pair.request.source_pin
    payload["cms"].update(
        source_id=pin.source_id,
        endpoint_id=pin.endpoint_id,
        dataset_id=pin.dataset_id,
        dataset_hash=pin.dataset_sha256,
        release_id=pin.release_id,
    )
    payload["selection"]["proof_id"] = pair.request.selection_proof_id
    payload["profile"].update(
        generation_id="pdprofile_" + "1" * 32,
        profile_as_of=pin.as_of,
        selection_proof_id=pair.request.selection_proof_id,
    )
    return dict(
        receipt_id="a" * 64,
        payload=payload,
        publication_xid="42",
        profile_generation_id=payload["profile"]["generation_id"],
        registry_source_pair_json=receipts.registry_source_pair_document(pair),
    )


def test_closed_slot_roundtrip_preserves_prepared_capture():
    pair = _prepared()
    document = receipts.registry_source_pair_document(pair)
    assert receipts.decode_registry_source_pair_document(document) == pair
    assert set(document) == {"protocol", "capture_id", "prepared_pair"}
    assert "receipt_id" not in document and "publication_xid" not in document


@pytest.mark.parametrize("damage", ["extra", "protocol", "capture", "zero", "prepared", "bound"])
def test_slot_rejects_changed_or_recursive_envelope(damage):
    document = receipts.registry_source_pair_document(_prepared())
    replacements_by_damage = {
        "extra": ("unbound", True),
        "protocol": ("protocol", True),
        "capture": ("capture_id", str(UUID(int=2))),
        "zero": ("capture_id", str(UUID(int=0))),
        "prepared": ("prepared_pair", {**document["prepared_pair"], "lease_digest": "x"}),
        "bound": ("prepared_pair", {**document["prepared_pair"], "bound_pair": {}}),
    }
    field, value = replacements_by_damage[damage]
    document[field] = value
    with pytest.raises(ValueError):
        receipts.decode_registry_source_pair_document(document)


def test_pair_is_constructed_from_actual_slot_and_native_receipt_fields():
    pair = _prepared()
    row = _row(pair)
    bound = prepared._pair_from_prepared_receipt(pair, row)
    assert bound.proof.serving_receipt_id == row["receipt_id"]
    assert bound.proof.publication_xid == "42" and bound.witness_json is None
    for field, value in (("registry_source_pair_json", None), ("profile_generation_id", "pdprofile_" + "2" * 32)):
        with pytest.raises(ValueError):
            prepared._pair_from_prepared_receipt(pair, {**row, field: value})


@pytest.mark.asyncio
async def test_nonregistry_append_keeps_the_historical_two_column_interface():
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value="receipt"))
    assert await receipts.append_serving_receipt(session, "example", _payload()) == "receipt"
    statement, params = session.scalar.call_args.args
    assert "registry_source_pair_json" not in str(statement) and set(params) == {"payload"}


@pytest.mark.asyncio
async def test_new_append_fails_before_insert_without_genuine_source_owner():
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock())
    with pytest.raises(ValueError, match="publisher_connection_required"):
        await receipts.append_serving_receipt(session, "example", _payload(), registry_source_pair=_prepared())
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_own_receipt_binding_uses_slot_and_never_comments_retained_schema(monkeypatch):
    source_pair = _prepared()
    row = _row(source_pair)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=row), execute=AsyncMock())
    monkeypatch.setattr(receipts, "require_registry_receipt_owner", AsyncMock(return_value=connection))
    monkeypatch.setattr(prepared, "_require_prepared_pair", AsyncMock(return_value=connection))
    monkeypatch.setattr(prepared, "_require_retained_authority", AsyncMock())
    row["payload"]["address"]["relation_oids"] = [
        dict((logical, oid) for logical, _stage, oid in source_pair.address_stages)[name]
        for name in prepared.RELATION_NAMES
    ]
    monkeypatch.setattr(receipts, "capture_native_dependencies", AsyncMock(return_value=row["payload"]))
    row["payload"] = json.dumps(row["payload"])
    bound = await prepared.bind_prepared_registry_cms_source_pair(
        object(), source_pair, receipt_id=row["receipt_id"], receipt_payload=json.loads(row["payload"])
    )
    assert bound.proof.publication_xid == "42"
    assert "publication_xid=pg_current_xact_id()" in connection.fetchrow.call_args.args[0]
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_comment_always_requires_committed_slot_even_without_witness(monkeypatch):
    source_pair = _prepared()
    row = _row(source_pair)
    bound = prepared._pair_from_prepared_receipt(source_pair, row)
    connection = SimpleNamespace(
        fetchval=AsyncMock(
            return_value=prepared._prepared_comment_prefix(source_pair) + retained._json(source_pair.as_dict())
        )
    )
    monkeypatch.setattr(retained, "_normalize_receipt_session", AsyncMock())
    monkeypatch.setattr(retained, "native_driver", AsyncMock(return_value=connection))
    monkeypatch.setattr(retained, "require_retained_cms_fhir_source_epoch", AsyncMock())
    monkeypatch.setattr(
        retained,
        "address_clone_identity",
        AsyncMock(return_value=(bound.address_receipt, bound.address_catalog_sha256)),
    )
    read = AsyncMock(return_value=row)
    monkeypatch.setattr(receipts, "read_registry_source_pair_receipt", read)
    assert await retained.require_registry_cms_source_pair(object(), bound, verify_content=False) == bound
    assert bound.witness_json is None and read.await_count == 1
    read.return_value = None
    with pytest.raises(ValueError, match="source_pair_invalid"):
        await retained.require_registry_cms_source_pair(object(), bound, verify_content=False)


@pytest.mark.asyncio
async def test_finalized_slot_probe_prevents_terminal_cleanup_of_prepared_comment(monkeypatch):
    source_pair = _prepared()
    bound = prepared._pair_from_prepared_receipt(source_pair, _row(source_pair))
    connection = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(cleanup, "_caller_connection", AsyncMock(return_value=connection))
    monkeypatch.setattr(cleanup, "probe_registry_cms_source_capture", AsyncMock(return_value=bound))
    assert await cleanup.cleanup_prepared_registry_cms_source_pair(object(), source_pair) == "finalized"
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_own_xid_slot_cannot_be_classified_as_an_unbound_cleanup_capture(monkeypatch):
    pair = _prepared()
    epoch = pair.recipe.source_pin.retained_epoch
    comment = prepared._prepared_comment_prefix(pair) + retained._json(pair.as_dict())
    connection = SimpleNamespace(
        fetch=AsyncMock(
            return_value=[
                {"nspname": pair.address_ownership.schema_name, "rolname": pair.owner_role, "comment": comment},
                {"nspname": epoch.schema_name, "rolname": pair.owner_role, "comment": "epoch"},
            ]
        ),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(cleanup, "_caller_connection", AsyncMock(return_value=connection))
    monkeypatch.setattr(cleanup, "read_committed_registry_cms_pair", AsyncMock(return_value=None))
    monkeypatch.setattr(receipts, "has_registry_source_pair_receipt", AsyncMock(return_value=True))
    with pytest.raises(ValueError, match="prepared_capture_invalid"):
        await cleanup.probe_registry_cms_source_capture(
            object(), capture_id=epoch.epoch_id, owner_role=pair.owner_role, runtime_roles=pair.runtime_roles
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_presence_read_never_excludes_the_own_transaction():
    session = SimpleNamespace(scalar=AsyncMock(return_value=True))
    assert await receipts.has_registry_source_pair_receipt(session, "example", UUID(int=1))
    sql = str(session.scalar.call_args.args[0])
    assert "EXISTS" in sql and "capture_id" in sql and "IS DISTINCT FROM" not in sql


@pytest.mark.asyncio
async def test_retained_replay_preserves_full_content_verification(monkeypatch):
    source_pair = _prepared()
    bound = prepared._pair_from_prepared_receipt(source_pair, _row(source_pair))
    session = SimpleNamespace(
        scalar=AsyncMock(
            return_value=prepared._prepared_comment_prefix(source_pair) + retained._json(source_pair.as_dict())
        )
    )
    monkeypatch.setattr(prepared, "read_committed_registry_cms_pair", AsyncMock(return_value=bound))
    require = AsyncMock()
    monkeypatch.setattr(retained, "require_registry_cms_source_pair", require)
    assert (
        await retained._retained_pair(
            session, bound.proof, bound.address_ownership.schema_name, bound.owner_role, bound.runtime_roles
        )
        == bound
    )
    require.assert_awaited_once_with(session, bound)


@pytest.mark.parametrize("coordinate", ["capacity_geometry_hash", "lease_digest", "native_address_input_hash"])
def test_registry_preparation_requires_the_actual_admission_coordinates(coordinate):
    from tests.test_provider_directory_cms_source_hooks import _bound_job

    pair = _prepared()
    job = replace(
        _bound_job(),
        retention_request=pair.request,
        capture_id=pair.recipe.source_pin.retained_epoch.epoch_id,
        owner_role=pair.owner_role,
        runtime_roles=pair.runtime_roles,
        source_attempt=pair.source_attempt,
    )
    plan = SimpleNamespace(
        capacity_geometry_hash=pair.capacity_geometry_hash, native_address_input_hash=pair.native_address_input_hash
    )
    lease = SimpleNamespace(lease_digest=pair.lease_digest)
    candidate = SimpleNamespace(
        registry_source_pair=pair, nonprofile_admission=SimpleNamespace(registry_source_job=job, plan=plan, lease=lease)
    )
    assert publication._registry_preparation(candidate) is pair
    setattr(lease if coordinate == "lease_digest" else plan, coordinate, "9" * 64)
    with pytest.raises(RuntimeError, match="runtime_invalid"):
        publication._registry_preparation(candidate)


@pytest.mark.asyncio
async def test_committed_capture_reader_excludes_own_xid_and_bounds_native_json():
    pair = _prepared()
    row = _row(pair)
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    assert (
        await receipts.read_registry_source_pair_receipt(
            session, "example", pair.recipe.source_pin.retained_epoch.epoch_id
        )
        == row
    )
    sql = str(session.execute.call_args.args[0])
    assert "IS DISTINCT FROM pg_current_xact_id_if_assigned()" in sql
    assert "octet_length(registry_source_pair_json::text)<=65536" in sql


def _registry_recovery_session(events):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        yield
        events.append("end")

    async def execute(statement):
        events.append(str(statement))

    session = SimpleNamespace(begin=transaction, execute=AsyncMock(side_effect=execute))

    @asynccontextmanager
    async def factory():
        events.append("fresh-source-session")
        yield session

    return session, factory


def _registry_recovery_case(monkeypatch):
    from process.registry_cms_source_notifications import RegistryCMSSourceNotification
    from tests.test_provider_directory_cms_source_hooks import _bound_job

    job = _bound_job()
    pair = replace(_prepared(), source_attempt=job.source_attempt)
    receipt_row = _row(pair)
    bound = prepared._pair_from_prepared_receipt(pair, receipt_row)
    events = []

    session, factory = _registry_recovery_session(events)

    notice = RegistryCMSSourceNotification(job.source_attempt, bound)

    async def read(actual_session, **coordinates):
        assert actual_session is session
        assert coordinates == dict(
            source_schema=pair.request.source_pin.schema_name, receipt_id=receipt_row["receipt_id"]
        )
        assert events[2] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        events.append("notice")
        return notice

    job = replace(job, source_session_factory=factory, read_receipt=AsyncMock(side_effect=read))
    candidate = SimpleNamespace(nonprofile_admission=SimpleNamespace(registry_source_job=job), metrics={})
    directory = SimpleNamespace(
        _schema=lambda: pair.request.source_pin.schema_name,
        db=SimpleNamespace(session=lambda: pytest.fail("default pool cannot authenticate Source readback")),
    )
    monkeypatch.setattr(publication, "_registry_preparation", lambda _value: pair)
    historical = AsyncMock(return_value=True)
    committed = AsyncMock(return_value=bound)
    monkeypatch.setattr(receipts, "verify_historical_receipt", historical)
    monkeypatch.setattr(prepared, "read_committed_registry_cms_pair", committed)
    return SimpleNamespace(
        job=job,
        prepared=candidate,
        pair=pair,
        bound=bound,
        row=receipt_row,
        session=session,
        directory=directory,
        historical=historical,
        committed=committed,
        events=events,
    )


@pytest.mark.asyncio
async def test_registry_recovery_uses_one_authenticated_source_snapshot(monkeypatch):
    case = _registry_recovery_case(monkeypatch)
    assert await publication._has_registry_commit(
        case.directory, case.prepared, case.row["receipt_id"], case.row["payload"]
    )
    case.job.read_receipt.assert_awaited_once()
    case.historical.assert_awaited_once_with(
        case.session, case.pair.request.source_pin.schema_name, case.row["receipt_id"], case.row["payload"]
    )
    case.committed.assert_awaited_once_with(case.session, case.pair)
    assert case.events[:2] == ["fresh-source-session", "begin"] and case.events[-1] == "end"
    case.job.notify_receipt.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["notice", "attempt", "receipt", "slot", "pair"])
async def test_lostack_cannot_consume_a_preparation_without_exact_committed_evidence(monkeypatch, changed):
    from process.registry_cms_source_notifications import RegistryCMSSourceNotification

    case = _registry_recovery_case(monkeypatch)
    if changed == "notice":
        case.job.read_receipt.side_effect = None
        case.job.read_receipt.return_value = None
    if changed == "attempt":
        case.job.read_receipt.side_effect = None
        case.job.read_receipt.return_value = RegistryCMSSourceNotification(
            replace(case.job.source_attempt, attempt_id="run:" + "b" * 32), case.bound
        )
    if changed == "receipt":
        case.historical.return_value = False
    if changed == "slot":
        case.committed.return_value = None
    if changed == "pair":
        case.committed.return_value = replace(case.bound, cms_receipt_payload=case.bound.cms_receipt_payload + " ")
    consume = AsyncMock()
    monkeypatch.setattr(publication, "_consume_preparations", consume)
    assert (
        await publication._finalize_committed_publication(
            case.directory, case.prepared, None, None, case.row["receipt_id"], case.row["payload"], True
        )
        is None
    )
    consume.assert_not_awaited()
    case.job.notify_receipt.assert_not_awaited()
    assert case.prepared.metrics == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("tagged", [False, True])
async def test_committed_resolver_preserves_prepared_stamp_and_binds_exact_receipt(monkeypatch, tagged):
    from process.network_cms_registry_address_capture import NativeReceiptSession
    from tests.test_provider_directory_cms_source_hooks import _bound_job

    pair = _prepared()
    if tagged:
        pair = replace(pair, source_attempt=_bound_job().source_attempt)
    row = _row(pair)
    comment = prepared._prepared_comment_prefix(pair) + retained._json(pair.as_dict())
    driver = SimpleNamespace(
        fetchrow=AsyncMock(return_value=dict(schema_oid=pair.address_ownership.schema_oid, comment=comment))
    )
    reader = AsyncMock(return_value=row)
    monkeypatch.setattr(receipts, "read_registry_source_pair_receipt", reader)
    actual, oid = await retained._read_committed_pair(driver, pair.address_ownership.schema_name)
    assert actual == prepared._pair_from_prepared_receipt(pair, row) and oid == pair.address_ownership.schema_oid
    receipt_session, schema, capture_id = reader.await_args.args
    assert type(receipt_session) is NativeReceiptSession and receipt_session.native_connection is driver
    assert (
        schema == pair.request.source_pin.schema_name and capture_id == pair.recipe.source_pin.retained_epoch.epoch_id
    )
    assert driver.fetchrow.await_args.args[1] == pair.address_ownership.schema_name
    assert "octet_length" in driver.fetchrow.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["schema", "oid", "missing", "receipt", "capture", "canonical"])
async def test_prepared_committed_resolver_refuses_changed_identity_or_uncommitted_slot(monkeypatch, changed):
    pair = _prepared()
    receipt_row = _row(pair)
    comment = prepared._prepared_comment_prefix(pair) + retained._json(pair.as_dict())
    metadata = dict(schema_oid=pair.address_ownership.schema_oid, comment=comment)
    schema = pair.address_ownership.schema_name
    if changed == "schema":
        schema += "_foreign"
    if changed == "oid":
        metadata["schema_oid"] += 1
    if changed == "missing":
        receipt_row = None
    if changed == "receipt":
        receipt_row["payload"] = {
            **receipt_row["payload"],
            "profile": {**receipt_row["payload"]["profile"], "generation_id": "pdprofile_" + "2" * 32},
        }
    if changed == "capture":
        receipt_row["registry_source_pair_json"] = {
            **receipt_row["registry_source_pair_json"],
            "capture_id": str(UUID(int=2)),
        }
    if changed == "canonical":
        metadata["comment"] += " "
    monkeypatch.setattr(receipts, "read_registry_source_pair_receipt", AsyncMock(return_value=receipt_row))
    expected_error = {
        "schema": "registry_cms_source_pair_invalid",
        "oid": "registry_cms_source_pair_invalid",
        "missing": "registry_cms_source_pair_unavailable",
        "receipt": "registry_cms_prepared_receipt_invalid",
        "capture": "cms_serving_registry_pair_invalid",
        "canonical": "registry_cms_source_pair_invalid",
    }[changed]
    with pytest.raises(ValueError, match="^" + expected_error + "$"):
        await retained._read_prepared_committed_pair(object(), schema, metadata)


def _prepared_pair_native_boundary(case, monkeypatch):
    """Supply native catalog facts while keeping the real scope/own-row authentication."""
    case.session.in_transaction = lambda: True
    case.session.execute = AsyncMock()
    comment = prepared._prepared_comment_prefix(case.pair) + retained._json(case.pair.as_dict())
    fetchval = case.driver.fetchval.side_effect
    comment_state = SimpleNamespace(value=comment)

    async def native_value(query, *arguments):
        if "obj_description" in query:
            return comment_state.value
        return await fetchval(query, *arguments)

    case.driver.fetchval.side_effect = native_value
    epoch = AsyncMock()
    address_catalog = AsyncMock(return_value=(case.pair.address_receipt, case.pair.address_catalog_sha256))
    committed = AsyncMock(return_value=None)
    monkeypatch.setattr(retained, "require_retained_cms_fhir_source_epoch", epoch)
    monkeypatch.setattr(retained, "address_clone_identity", address_catalog)
    monkeypatch.setattr(receipts, "read_registry_source_pair_receipt", committed)
    return SimpleNamespace(comment=comment_state, epoch=epoch, address=address_catalog, committed=committed)


@pytest.mark.asyncio
@pytest.mark.parametrize("verify_content", [False, True])
async def test_original_publication_revalidates_its_exact_own_prepared_pair(
    registry_publication_case, monkeypatch, verify_content
):
    from process import provider_directory_cms_source_runtime as runtime

    async with registry_publication_case() as case:
        boundary = _prepared_pair_native_boundary(case, monkeypatch)
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ) as bound:
            assert (
                await retained.require_registry_cms_source_pair(case.session, bound, verify_content=verify_content)
                is bound
            )
        boundary.committed.assert_not_awaited()
        boundary.epoch.assert_awaited_once_with(
            case.driver,
            case.pair.request.source_pin,
            case.pair.recipe.source_pin.retained_epoch,
            verify_content=verify_content,
        )
        boundary.address.assert_awaited_once_with(
            case.session,
            case.pair.address_ownership,
            case.pair.owner_role,
            case.pair.runtime_roles,
            verify_content=verify_content,
        )


@pytest.mark.asyncio
async def test_unscoped_prepared_pair_keeps_the_committed_reader_boundary(registry_publication_case, monkeypatch):
    async with registry_publication_case() as case:
        boundary = _prepared_pair_native_boundary(case, monkeypatch)
        bound = prepared._pair_from_prepared_receipt(case.pair, case.row)
        with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
            await retained.require_registry_cms_source_pair(case.session, bound, verify_content=False)
        boundary.committed.assert_awaited_once_with(
            case.session, case.pair.request.source_pin.schema_name, case.pair.recipe.source_pin.retained_epoch.epoch_id
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["attempt", "pair", "xid", "task", "session", "stamp"])
async def test_original_prepared_pair_refuses_changed_scope_or_stamp(registry_publication_case, monkeypatch, changed):
    import asyncio

    from process import provider_directory_cms_source_runtime as runtime

    async with registry_publication_case() as case:
        boundary = _prepared_pair_native_boundary(case, monkeypatch)
        async with runtime.registry_source_publication(
            case.session, case.prepared, case.row["receipt_id"], case.row["payload"]
        ) as bound:
            actual_session, supplied = case.session, bound
            if changed == "attempt":
                foreign = replace(
                    case.pair, source_attempt=replace(case.job.source_attempt, attempt_id="run:" + "b" * 32)
                )
                boundary.comment.value = prepared._prepared_comment_prefix(foreign) + retained._json(foreign.as_dict())
            if changed == "pair":
                supplied = replace(bound, cms_receipt_payload=bound.cms_receipt_payload + " ")
            if changed == "xid":
                case.native.xid = "43"
            if changed == "session":
                actual_session = SimpleNamespace(**vars(case.session))
            if changed == "stamp":
                boundary.comment.value += " "
            operation = retained.require_registry_cms_source_pair(actual_session, supplied, verify_content=False)
            if changed == "task":
                operation = asyncio.create_task(operation)
            try:
                with pytest.raises(ValueError, match="^cms_registry_source_publication_invalid$"):
                    await operation
            finally:
                case.native.xid = "42"
        boundary.committed.assert_not_awaited()
