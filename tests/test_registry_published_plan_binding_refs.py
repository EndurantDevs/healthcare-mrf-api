import asyncio
import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import registry_management as routes
from process import network_source_binding_store as writer
from process import registry_published_plan_binding_refs as bindings
from process.ptg_parts.result_archive_published_authority import (
    PtgPublishedResultSourceAuthority,
)
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_ptg_published_plan_contract import AUTHORITY_CONTRACT, OPERATION
from process.registry_ptg_published_plan_scope import _digest, _document
from tests.test_registry_ptg_published_plan import identity, intent
from tests.test_registry_ptg_scope_engine import _actor, _ownership


def reviewed_binding():
    published_identity = identity()
    published_identity["import_month"] = "2026-01-01"
    command_by_field = {
        **intent(),
        "operation": OPERATION,
        "published_identity": published_identity,
        "ownership": _ownership() | {"import_month": "2026-01-01"},
        "source": {
            "snapshot_id": "snapshot_example",
            "binding_source_key": "source_example",
            "ptg_schema_name": "synthetic_ptg",
        },
        "coordinates": {
            "source_system": "ptg",
            "source_id": "source_example",
            "dataset_schema": "synthetic_ptg",
            "dataset_id": "snapshot_example",
            "producer_id": AUTHORITY_CONTRACT,
            "edition_id": "c" * 64,
        },
    }
    evidence_by_field = {
        "source_authority": PtgPublishedResultSourceAuthority(
            "synthetic_review_operation", published_identity
        ).as_dict(),
        "selected_source_keys": [1],
        "selection_mode": bindings.SELECTION_MODE,
    }
    document = _document(command_by_field, _actor(), evidence_by_field)
    scope_by_field = {
        name: command_by_field[name]
        for name in (
            "review_type",
            "scope_id",
            "plan_id",
            "plan_market_type",
            "selection_mode",
        )
    }
    scope_by_field.update(snapshot_id="snapshot_example", approval_sha256=_digest(document))
    binding_by_field = {
        **command_by_field["coordinates"],
        "network_id": 7,
        "source_key": "source_example",
        "source_scope_json": scope_by_field,
        "evidence_id": "published-plan-review:" + scope_by_field["scope_id"],
        "evidence_sha256": scope_by_field["approval_sha256"],
    }
    review_by_field = {
        "scope_id": scope_by_field["scope_id"],
        "approval_sha256": scope_by_field["approval_sha256"],
        "document": document,
    }
    return binding_by_field, review_by_field


class Driver:
    def __init__(self, review_by_field):
        self.fetchval = AsyncMock(
            side_effect=[
                "ordinary,pg_catalog",
                "pg_catalog,pg_temp",
                "repeatable read",
                True,
                True,
                "ordinary,pg_catalog",
            ]
        )
        self.fetchrow = AsyncMock(
            return_value={
                "complete": True,
                "bounded": True,
                "documents_json": json.dumps([review_by_field]),
            }
        )
        self.rollback_count = 0
        self.is_in_transaction = lambda: True

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.rollback_count += 1
            raise


def test_closed_review_uses_original_pure_authority():
    binding_by_field, review_by_field = reviewed_binding()
    command_by_field = bindings._review_commands(json.dumps([review_by_field]))[
        (review_by_field["scope_id"], review_by_field["approval_sha256"])
    ]
    bindings._require_reference(binding_by_field, command_by_field)
    assert "company_key" not in binding_by_field["source_scope_json"]
    assert "cohort_id" not in binding_by_field["source_scope_json"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("network_id", 8),
        ("source_key", "other"),
        ("dataset_schema", "other"),
        ("dataset_id", "other"),
        ("producer_id", "other"),
        ("edition_id", "a" * 64),
        ("evidence_id", "other"),
        ("evidence_sha256", "b" * 64),
    ],
)
def test_reference_cannot_widen_exact_review(field, value):
    binding_by_field, review_by_field = reviewed_binding()
    command_by_field = review_by_field["document"]["command"]
    binding_by_field[field] = value
    with pytest.raises(ValueError):
        bindings._require_reference(binding_by_field, command_by_field)


@pytest.mark.parametrize(
    "mutation",
    [
        {"company_key": "invented"},
        {"cohort_id": "invented"},
        {"plan_id": "other"},
        {"snapshot_id": "other"},
        {"selection_mode": "partial"},
    ],
)
def test_published_namespace_does_not_invent_cohort_or_other_scope(mutation):
    binding_by_field, review_by_field = reviewed_binding()
    binding_by_field["source_scope_json"].update(mutation)
    with pytest.raises(ValueError):
        bindings._require_reference(binding_by_field, review_by_field["document"]["command"])


@pytest.mark.asyncio
async def test_set_read_same_driver_and_canonical_path():
    binding_by_field, review_by_field = reviewed_binding()
    driver = Driver(review_by_field)
    await bindings.require_published_plan_binding_references(
        driver,
        '"synthetic_control"',
        json.dumps([binding_by_field] * 64).encode(),
        RegistryPTGProducerScopeStore("scope_owner", "scope_approver", "synthetic_control"),
    )
    assert driver.fetchrow.await_count == 1
    sql, args, cap = driver.fetchrow.await_args.args
    assert "DISTINCT scope_id" in sql and len(json.loads(args)) == 64 and cap == 8 * 1024 * 1024
    assert driver.fetchval.await_args.args == (
        "SELECT pg_catalog.set_config('search_path',$1,true)",
        "ordinary,pg_catalog",
    )


@pytest.mark.asyncio
async def test_legacy_shape_has_no_new_database_reads():
    driver = Driver({})
    await bindings.require_published_plan_binding_references(
        driver,
        '"synthetic_control"',
        b'[{"source_scope_json":{"company_key":"old","cohort_id":"old","snapshot_id":"old"}}]',
        None,
    )
    driver.fetchval.assert_not_awaited()
    driver.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_protected_store_refuses_before_review_lookup():
    binding_by_field, review_by_field = reviewed_binding()
    driver = Driver(review_by_field)
    with pytest.raises(ValueError, match="store_unprotected"):
        await bindings.require_published_plan_binding_references(
            driver, '"synthetic_control"', json.dumps([binding_by_field]).encode(), None
        )
    driver.fetchrow.assert_not_awaited()
    assert driver.rollback_count == 1


@pytest.mark.asyncio
async def test_cancellation_preserves_first_error_and_rolls_back_path():
    binding_by_field, review_by_field = reviewed_binding()
    driver = Driver(review_by_field)
    error = asyncio.CancelledError()
    driver.fetchrow.side_effect = error
    with pytest.raises(asyncio.CancelledError) as caught:
        await bindings.require_published_plan_binding_references(
            driver,
            '"synthetic_control"',
            json.dumps([binding_by_field]).encode(),
            RegistryPTGProducerScopeStore("scope_owner", "scope_approver", "synthetic_control"),
        )
    assert caught.value is error and driver.rollback_count == 1
    assert driver.fetchval.await_count == 5


@pytest.mark.asyncio
async def test_actual_writer_rechecks_review_before_replay(monkeypatch):
    binding_by_field, review_by_field = reviewed_binding()
    proof = AsyncMock()
    monkeypatch.setattr(bindings, "require_published_plan_binding_references", proof)
    driver = SimpleNamespace(
        fetchrow=AsyncMock(
            side_effect=[
                {"draft_revision": 1},
                {"request_sha256": "hash", "receipt_json": '{"replay":true}'},
            ]
        )
    )
    command_by_field = SimpleNamespace(input_bytes=json.dumps([binding_by_field]).encode(), idempotency_key="same")
    result = await writer._apply(
        driver,
        command_by_field,
        (None, "actor", None, "hash"),
        '"synthetic_control"',
        "trusted_store",
    )
    assert result == {"replay": True}
    proof.assert_awaited_once_with(driver, '"synthetic_control"', command_by_field.input_bytes, "trusted_store")


@pytest.mark.parametrize("source_keys", [[2**63], [True], [1, 1], [], [-1]])
def test_source_keys_preserve_actual_occurrences(source_keys):
    assert not bindings._complete_source_keys(source_keys, 1)


@pytest.mark.asyncio
async def test_approved_reader_consumes_review_proof_on_same_driver(monkeypatch):
    from process import network_approved_source_bindings as approved
    from process.network_approved_membership_source import ApprovedMembershipSource

    binding_by_field, review_by_field = reviewed_binding()
    source_module = ApprovedMembershipSource(3, "a" * 64)
    driver = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={
                "invalid": False,
                "binding_count": 1,
                "published_count": 1,
                "published_json": json.dumps([binding_by_field]),
            }
        )
    )
    monkeypatch.setattr(approved, "pin_approved_membership_source", AsyncMock(return_value=source_module))
    proof = AsyncMock()
    monkeypatch.setattr(bindings, "require_published_plan_binding_references", proof)
    coordinates = approved.RegistryNetworkSourceCoordinates(**review_by_field["document"]["command"]["coordinates"])
    assert (
        await approved.require_approved_network_source_bindings(
            driver,
            source_module,
            coordinates,
            control_schema="synthetic_control",
            published_plan_store="trusted_store",
        )
        == 1
    )
    proof.assert_awaited_once_with(
        driver,
        '"synthetic_control"',
        json.dumps([binding_by_field]).encode(),
        "trusted_store",
    )


def test_catalog_preserves_old_and_new_scope_and_rejects_mixed():
    from process.registry_network_catalog_read import _source_scope

    binding_by_field, _ = reviewed_binding()
    assert _source_scope("ptg", binding_by_field["source_scope_json"]) == binding_by_field["source_scope_json"]
    assert _source_scope("ptg", {"company_key": "old", "cohort_id": "old", "snapshot_id": "old"}) == {
        "company_key": "old",
        "cohort_id": "old",
        "snapshot_id": "old",
    }
    with pytest.raises(ValueError):
        _source_scope("ptg", binding_by_field["source_scope_json"] | {"company_key": "invented"})
    assert bindings._complete_source_keys([0], 1)


@pytest.mark.asyncio
async def test_actual_management_caller_uses_rr_driver_and_server_store(monkeypatch):
    binding_by_field, _ = reviewed_binding()
    driver = object()
    store = object()
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(execute=AsyncMock(), connection=AsyncMock(return_value=connection))

    @asynccontextmanager
    async def transaction():
        yield

    session.begin = transaction
    request = SimpleNamespace(
        args={},
        body=json.dumps(
            {
                "rows": [binding_by_field],
                "reason": "Reviewed source",
                "idempotency_key": "same",
                "actor": _actor(),
            }
        ).encode(),
        ctx=SimpleNamespace(sa_session=session),
        app=SimpleNamespace(ctx=SimpleNamespace(registry_ptg_scope_engine=SimpleNamespace(store=store))),
    )
    monkeypatch.setattr(routes, "require_control_auth", lambda request: None)
    apply = AsyncMock(return_value={"records": []})
    monkeypatch.setattr(routes, "apply_network_source_binding_batch", apply)
    reply = await routes.write_source_binding_batch(request)
    assert reply.status == 200
    assert str(session.execute.await_args.args[0]) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    assert apply.await_args.args[0] is driver and apply.await_args.kwargs["published_plan_store"] is store


@pytest.mark.asyncio
async def test_missing_enforced_size_check_precedes_document_read():
    binding_by_field, review_by_field = reviewed_binding()
    driver = Driver(review_by_field)
    driver.fetchval.side_effect = ["ordinary,pg_catalog", "pg_catalog,pg_temp", "repeatable read", True, False]
    with pytest.raises(ValueError, match="store_unbounded"):
        await bindings.require_published_plan_binding_references(
            driver,
            '"synthetic_control"',
            json.dumps([binding_by_field]).encode(),
            RegistryPTGProducerScopeStore("scope_owner", "scope_approver", "synthetic_control"),
        )
    assert driver.fetchrow.await_count == 0 and driver.rollback_count == 1
