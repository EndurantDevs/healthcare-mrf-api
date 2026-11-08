# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected review rediscovery, bounded pages and installed batch-authority refusal."""

import copy
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_scope_engine as engine
from process import registry_ptg_source_reviews as reviews
from process.ptg_parts.result_archive_published_authority import _authority_from_identity
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError, RegistryPTGProducerScopeStore, _digest
from process.registry_ptg_published_plan_scope import _document, resolved_published_plan_inventory
from process.registry_record_store import _bounded_name, _validated_fields
from tests.test_registry_ptg_published_plan import identity, intent
from tests.test_registry_ptg_scope_engine import _ownership

SCHEMA = "synthetic_ptg"


def sample():
    ownership = _ownership() | {"import_month": "2026-10-01"}
    inventory_by_field = {
        "identity": identity() | {"import_month": "2026-10-01"},
        "plan_scopes": [{"plan_id": intent()["plan_id"], "plan_market_type": "group"}],
        "file_versions": intent()["file_versions"],
        "source_keys": [1],
    }
    authority = _authority_from_identity(
        "registry_ptg_published_review_" + intent()["scope_id"].replace("-", ""), inventory_by_field["identity"]
    )
    inventory_by_field["authority"] = authority.as_dict()
    full, _, evidence = resolved_published_plan_inventory(SCHEMA, intent(), ownership, inventory_by_field)
    actor_by_field = {"kind": "platform_admin", "user_id": intent()["statement_id"], "client_id": "system"}
    document = _document(full, actor_by_field, evidence)
    approval_record_by_field = {
        "scope_id": UUID(full["scope_id"]),
        "approval_json": document,
        "approval_sha256": _digest(document),
        "company_name": "Reviewed company",
        "network_name": "Reviewed network",
        "association_current": True,
    }
    envelope_by_field = {
        "actor": actor_by_field,
        "session_token_sha256": "a" * 64,
        "ownership": ownership,
        "selection": {
            "client_id": full["client_id"],
            "source_file_import_id": full["source_file_import_id"],
            "after": None,
            "limit": 20,
        },
    }
    return approval_record_by_field, inventory_by_field, envelope_by_field


def test_rediscovery_retains_original_scope_command_and_matching_approval():
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    page_by_field, pins = reviews._bounded_reviews(
        [approval_record_by_field],
        SCHEMA,
        envelope_by_field["ownership"],
        envelope_by_field["selection"],
        inventory_by_field,
    )
    item = page_by_field["items"][0]
    assert item["command"] == approval_record_by_field["approval_json"]["command"]
    assert item["approval"]["approval_sha256"] == approval_record_by_field["approval_sha256"]
    assert item["approval"]["command_sha256"] == _digest(item["command"])
    assert item["labels"] == {
        "company_name": "Reviewed company",
        "network_name": "Reviewed network",
        "plan_id": "plan_example",
        "plan_market_type": "group",
    }
    assert page_by_field["source"]["ownership"] == envelope_by_field["ownership"]
    assert page_by_field["next_cursor"] is None and len(pins) == 1


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("a" * 512, "a" * 512),
        ("é" * 256, "é" * 256),
        ("é" * 300, "é" * 254 + "…"),
        ("😀" * 512, "😀" * 127 + "…"),
        ("a" * 508 + "😀" * 4, "a" * 508 + "…"),
        ("a" * 508 + " " + "é" * 3, "a" * 508 + "…"),
        ("Company\u00a0Name", "Company Name"),
        ("Company\u202fName", "Company Name"),
        ("Company\u2028Name", "Company Name"),
        ("Company\u200bName", "Company�Name"),
        ("Company\u2066Name\u2069", "Company�Name�"),
        ("Company\ue000Name", "Company�Name"),
        ("Company\u0378Name", "Company�Name"),
        ("a" * 509 + "\u200b" * 3, "a" * 509 + "…"),
        ("a" * 508 + "\u00a0" + "é" * 3, "a" * 508 + "…"),
    ],
    ids=[
        "ascii-limit",
        "byte-limit",
        "multibyte",
        "character-limit",
        "partial-codepoint",
        "trimmed-prefix",
        "nbsp",
        "narrow-nbsp",
        "line-separator",
        "zero-width-space",
        "bidi-isolates",
        "private-use",
        "unassigned",
        "replacement-byte-boundary",
        "projected-trimmed-prefix",
    ],
)
def test_writer_valid_names_are_bounded_display_labels_without_changing_review_identity(name, expected):
    assert _bounded_name(name, 512, "display_name") == name
    assert (
        _validated_fields("company", {"display_name": name, "roles": ["insurer"], "aliases": []})["display_name"]
        == name
    )
    assert _validated_fields("network", {"display_name": name, "aliases": []})["display_name"] == name
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    approval_record_by_field.update(company_name=name, network_name=name)
    original = copy.deepcopy(approval_record_by_field)
    page_by_field, pins = reviews._bounded_reviews(
        [approval_record_by_field],
        SCHEMA,
        envelope_by_field["ownership"],
        envelope_by_field["selection"],
        inventory_by_field,
    )
    item = page_by_field["items"][0]
    for field in ("company_name", "network_name"):
        label = item["labels"][field]
        assert label == expected and label == label.strip() and label.isprintable()
        assert len(label.encode("utf-8")) <= 512
    assert item["command"] == original["approval_json"]["command"]
    assert item["approval"]["approval_sha256"] == original["approval_sha256"]
    assert item["approval"]["command_sha256"] == _digest(original["approval_json"]["command"])
    assert approval_record_by_field == original
    assert page_by_field["next_cursor"] is None and len(pins) == 1
    assert len(engine._canonical(page_by_field).encode("utf-8")) <= reviews.MAX_PAGE_BYTES


def test_projected_names_keep_unrelated_original_reviews_and_approval_receipts():
    first, inventory_by_field, envelope_by_field = sample()
    first.update(company_name="Company\u00a0Name", network_name="Net\u200bwork")
    second = copy.deepcopy(first)
    second["scope_id"] = UUID("33333333-3333-4333-8333-333333333333")
    second["approval_json"]["command"]["scope_id"] = str(second["scope_id"])
    authority = _authority_from_identity(
        "registry_ptg_published_review_" + second["scope_id"].hex, inventory_by_field["identity"]
    )
    second["approval_json"]["evidence"]["source_authority"] = authority.as_dict()
    second["approval_json"] = _document(
        second["approval_json"]["command"], second["approval_json"]["actor"], second["approval_json"]["evidence"]
    )
    second["approval_sha256"] = _digest(second["approval_json"])
    second.update(company_name="Unrelated company", network_name="Unrelated network")
    approval_records = [first, second]
    original = copy.deepcopy(approval_records)
    page_by_field, pins = reviews._bounded_reviews(
        approval_records, SCHEMA, envelope_by_field["ownership"], envelope_by_field["selection"], inventory_by_field
    )
    assert len(page_by_field["items"]) == len(pins) == 2 and page_by_field["next_cursor"] is None
    assert page_by_field["items"][0]["labels"]["company_name"] == "Company Name"
    assert page_by_field["items"][0]["labels"]["network_name"] == "Net�work"
    assert page_by_field["items"][1]["labels"]["company_name"] == "Unrelated company"
    assert page_by_field["items"][1]["labels"]["network_name"] == "Unrelated network"
    for review_item, retained in zip(page_by_field["items"], original, strict=True):
        assert review_item["command"] == retained["approval_json"]["command"]
        assert review_item["approval"]["command_sha256"] == _digest(retained["approval_json"]["command"])
        assert review_item["approval"]["approval_sha256"] == retained["approval_sha256"]
    assert approval_records == original
    assert len(engine._canonical(page_by_field).encode("utf-8")) <= reviews.MAX_PAGE_BYTES


@pytest.mark.parametrize(
    "name", ["a" * 513, "é" * 513, " leading", "trailing ", "line\nbreak", "a\x7fb", "a\x85b", "\ud800", "", None]
)
def test_display_label_still_refuses_invalid_or_unreadable_names(name):
    with pytest.raises(RegistryPTGProducerScopeError, match="registry_ptg_scope_changed"):
        reviews._label(name)


@pytest.mark.parametrize("mutation", ["digest", "statement", "ownership", "identity", "files", "association", "label"])
def test_rediscovery_refuses_changed_immutable_proof_source_or_target(mutation):
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    mutation_by_name = {
        "digest": (approval_record_by_field, "approval_sha256", "0" * 64),
        "statement": (approval_record_by_field["approval_json"], "producer_statement_sha256", "0" * 64),
        "ownership": (envelope_by_field["ownership"], "content_version", "replacement"),
        "identity": (inventory_by_field["identity"], "map_digest", "0" * 64),
        "files": (inventory_by_field, "file_versions", []),
        "association": (approval_record_by_field, "association_current", False),
        "label": (approval_record_by_field, "company_name", "é" * 513),
    }
    target, name, value = mutation_by_name[mutation]
    target[name] = value
    if mutation == "statement":
        approval_record_by_field["approval_sha256"] = _digest(approval_record_by_field["approval_json"])
    with pytest.raises((ValueError, RegistryPTGProducerScopeError)):
        reviews._bounded_reviews(
            [approval_record_by_field],
            SCHEMA,
            envelope_by_field["ownership"],
            envelope_by_field["selection"],
            inventory_by_field,
        )


def test_absence_is_empty_original_review_page_and_does_not_create_identity():
    _, inventory_by_field, envelope_by_field = sample()
    page_by_field, pins = reviews._bounded_reviews(
        [], SCHEMA, envelope_by_field["ownership"], envelope_by_field["selection"], inventory_by_field
    )
    assert page_by_field["items"] == [] and page_by_field["next_cursor"] is None and pins == []
    assert set(page_by_field) == {"source", "items", "next_cursor"}


def test_response_byte_limit_advances_only_over_returned_original_scopes(monkeypatch):
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    second = copy.deepcopy(approval_record_by_field)
    second["scope_id"] = UUID("33333333-3333-4333-8333-333333333333")
    second["approval_json"]["command"]["scope_id"] = str(second["scope_id"])
    authority = _authority_from_identity(
        "registry_ptg_published_review_" + second["scope_id"].hex, inventory_by_field["identity"]
    )
    second["approval_json"]["evidence"]["source_authority"] = authority.as_dict()
    second["approval_json"] = _document(
        second["approval_json"]["command"], second["approval_json"]["actor"], second["approval_json"]["evidence"]
    )
    second["approval_sha256"] = _digest(second["approval_json"])
    first_page, _ = reviews._bounded_reviews(
        [approval_record_by_field, second],
        SCHEMA,
        envelope_by_field["ownership"],
        envelope_by_field["selection"] | {"limit": 1},
        inventory_by_field,
    )
    maximum = len(engine._canonical(first_page).encode("utf-8"))
    monkeypatch.setattr(reviews, "MAX_PAGE_BYTES", maximum)
    page_by_field, pins = reviews._bounded_reviews(
        [approval_record_by_field, second],
        SCHEMA,
        envelope_by_field["ownership"],
        envelope_by_field["selection"],
        inventory_by_field,
    )
    assert page_by_field == first_page and len(pins) == 1
    monkeypatch.setattr(reviews, "MAX_PAGE_BYTES", maximum - 1)
    with pytest.raises(RegistryPTGProducerScopeError):
        reviews._bounded_reviews(
            [approval_record_by_field],
            SCHEMA,
            envelope_by_field["ownership"],
            envelope_by_field["selection"],
            inventory_by_field,
        )


@pytest.mark.parametrize("mutation", ["extra", "no_session", "cross_client", "limit", "actor"])
def test_source_reviews_closed_envelope_keeps_choices_grammar_unchanged(mutation):
    _, _, envelope_by_field = sample()
    assert engine.validated_registry_ptg_source_reviews_envelope(envelope_by_field) == envelope_by_field
    with pytest.raises(ValueError):
        engine.validated_registry_ptg_choices_envelope(envelope_by_field)
    if mutation == "extra":
        envelope_by_field["approval"] = {}
    elif mutation == "no_session":
        del envelope_by_field["session_token_sha256"]
    elif mutation == "cross_client":
        envelope_by_field["selection"]["client_id"] = "other"
    elif mutation == "limit":
        envelope_by_field["selection"]["limit"] = 21
    else:
        envelope_by_field["actor"]["kind"] = "client_owner"
    with pytest.raises((ValueError, PermissionError)):
        engine.validated_registry_ptg_source_reviews_envelope(envelope_by_field)


def service(batch=None):
    return engine.RegistryPTGScopeEngineService(
        SCHEMA,
        AsyncMock(),
        AsyncMock(),
        RegistryPTGProducerScopeStore("owner_role", "approval_role", "synthetic_control"),
        engine.RegistryPTGScopeAuthorityClient("http://localhost", "synthetic-only"),
        source_reviews_authority=batch,
    )


@pytest.mark.asyncio
async def test_uninstalled_batch_authority_refuses_before_reader():
    _, _, envelope_by_field = sample()
    installed = service()
    with pytest.raises(PermissionError, match="batch_authority_unavailable"):
        await installed.source_reviews(envelope_by_field)
    installed.reader_sessions.assert_not_called()


@asynccontextmanager
async def snapshot(*args, **kwargs):
    yield SimpleNamespace()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 20])
async def test_source_reviews_uses_one_batch_callback_including_empty(monkeypatch, count):
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    item, _ = reviews._verified_review(
        approval_record_by_field, SCHEMA, envelope_by_field["ownership"], inventory_by_field
    )
    page_by_field = {"source": {}, "items": [item] * count, "next_cursor": None}

    async def authorize(request_by_field, *, deadline):
        assert len(request_by_field["commands"]) == count
        return {
            "authorized": True,
            "actor": request_by_field["actor"],
            "request_sha256": _digest(request_by_field),
            "policy_revision": 3,
        }

    batch = AsyncMock(side_effect=authorize)
    monkeypatch.setattr(engine, "registry_company_approval_transaction", snapshot)
    monkeypatch.setattr(reviews, "read_registry_ptg_source_reviews", AsyncMock(return_value=page_by_field))
    installed = service(batch)
    assert await installed.source_reviews(envelope_by_field) == page_by_field
    batch.assert_awaited_once()
    installed.reader_sessions.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", [None, {"authorized": False}, "digest", "actor", "revision"])
async def test_source_reviews_refuses_bad_or_changed_batch_authority(monkeypatch, receipt):
    _, _, envelope_by_field = sample()

    async def authorize(request_by_field, *, deadline):
        receipt_by_field = {
            "authorized": True,
            "actor": request_by_field["actor"],
            "request_sha256": _digest(request_by_field),
            "policy_revision": 3,
        }
        if receipt == "digest":
            return receipt_by_field | {"request_sha256": "0" * 64}
        if receipt == "actor":
            return receipt_by_field | {"actor": {}}
        if receipt == "revision":
            return receipt_by_field | {"policy_revision": True}
        return receipt

    monkeypatch.setattr(engine, "registry_company_approval_transaction", snapshot)
    monkeypatch.setattr(reviews, "read_registry_ptg_source_reviews", AsyncMock(return_value={"items": []}))
    with pytest.raises((ValueError, PermissionError)):
        await service(AsyncMock(side_effect=authorize)).source_reviews(envelope_by_field)


class Result:
    def __init__(self, approval_records):
        self.approval_records = approval_records

    def mappings(self):
        return self

    def all(self):
        return self.approval_records


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 20])
async def test_reader_batches_page_and_exact_pins_with_constant_query_count(monkeypatch, count):
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    approval_records = []
    for index in range(count):
        current = copy.deepcopy(approval_record_by_field)
        current["scope_id"] = UUID(int=index + 1)
        command = current["approval_json"]["command"] | {
            "scope_id": str(current["scope_id"]),
            "idempotency_key": "original-" + str(index),
        }
        authority = _authority_from_identity(
            "registry_ptg_published_review_" + current["scope_id"].hex, inventory_by_field["identity"]
        )
        evidence = current["approval_json"]["evidence"] | {"source_authority": authority.as_dict()}
        current["approval_json"] = _document(command, current["approval_json"]["actor"], evidence)
        current["approval_sha256"] = _digest(current["approval_json"])
        approval_records.append(current)
    expected_authorities = [
        reviews._verified_review(current, SCHEMA, envelope_by_field["ownership"], inventory_by_field)[1]
        for current in approval_records
    ]
    pin_records = [
        {"owner_id": authority.owner_id, "snapshot_id": authority.snapshot_id, "reason": authority.pin_reason}
        for authority in sorted(expected_authorities, key=lambda authority: authority.owner_id)
    ]
    session = SimpleNamespace(execute=AsyncMock(side_effect=[Result(approval_records), Result(pin_records)]))
    protected = AsyncMock(return_value='"synthetic_control"."registry_ptg_published_plan_scope"')
    prepared = AsyncMock(return_value=inventory_by_field)
    monkeypatch.setattr(reviews, "_protected_store", protected)
    monkeypatch.setattr(reviews, "require_published_lock_privileges", AsyncMock())
    monkeypatch.setattr(reviews, "published_plan_inventory", prepared)
    monkeypatch.setattr(reviews, "require_registry_company_approval_fence", AsyncMock())
    monkeypatch.setattr(reviews, "_bounded_clone_lock_reads", snapshot)
    page_by_field = await reviews.read_registry_ptg_source_reviews(
        session, SCHEMA, envelope_by_field["ownership"], envelope_by_field["selection"], None
    )
    assert len(page_by_field["items"]) == count and session.execute.await_count == 2
    protected.assert_awaited_once_with(session, None, write=False, table_name=reviews.TABLE)
    prepared.assert_awaited_once()
    sql = str(session.execute.await_args_list[0].args[0])
    assert "registry_revision_control" in sql and "registry_approved_record" in sql and "LIMIT :maximum" in sql
    assert "review.approval_json->'command'->'ownership'=CAST(:ownership AS jsonb)" in sql
    assert "char_length(company.record_json->>'display_name')<=512" in sql
    assert "char_length(network.record_json->>'display_name')<=512" in sql
    assert "octet_length(review.approval_json::text)<=:byte_limit" in sql
    assert session.execute.await_args_list[0].args[1]["ownership"] == engine._canonical(envelope_by_field["ownership"])
    assert "FOR UPDATE" in str(session.execute.await_args_list[1].args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing", "duplicate", "wrong_snapshot", "wrong_reason"])
async def test_exact_batch_pin_set_refuses_missing_conflicting_or_released_pin(monkeypatch, mutation):
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    _, authority = reviews._verified_review(
        approval_record_by_field, SCHEMA, envelope_by_field["ownership"], inventory_by_field
    )
    pin_by_field = {
        "owner_id": authority.owner_id,
        "snapshot_id": authority.snapshot_id,
        "reason": authority.pin_reason,
    }
    approval_records = [pin_by_field]
    if mutation == "missing":
        approval_records = []
    elif mutation == "duplicate":
        approval_records = [pin_by_field, pin_by_field]
    elif mutation == "wrong_snapshot":
        pin_by_field["snapshot_id"] = "replacement"
    else:
        pin_by_field["reason"] = "replacement"
    session = SimpleNamespace(execute=AsyncMock(return_value=Result(approval_records)))
    monkeypatch.setattr(reviews, "_bounded_clone_lock_reads", snapshot)
    with pytest.raises(RegistryPTGProducerScopeError):
        await reviews._require_review_pins(session, SCHEMA, [authority])


@pytest.mark.asyncio
async def test_deadline_refuses_before_any_reader_or_authority(monkeypatch):
    _, _, envelope_by_field = sample()
    enter = AsyncMock()
    monkeypatch.setattr(engine, "registry_company_approval_transaction", enter)
    batch = AsyncMock()
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired):
        await service(batch).source_reviews(envelope_by_field, deadline=0)
    enter.assert_not_called()
    batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_batch_authority_cannot_return_page(monkeypatch):
    _, _, envelope_by_field = sample()
    monkeypatch.setattr(engine, "registry_company_approval_transaction", snapshot)
    monkeypatch.setattr(reviews, "read_registry_ptg_source_reviews", AsyncMock(return_value={"items": []}))
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired):
        await service(AsyncMock(side_effect=TimeoutError)).source_reviews(envelope_by_field)


@pytest.mark.parametrize("mutation", ["missing_command", "extra_document", "scope_mismatch", "schema_mismatch"])
def test_closed_stored_document_and_exact_source_binding_refuse_substitution(mutation):
    approval_record_by_field, inventory_by_field, envelope_by_field = sample()
    if mutation == "missing_command":
        del approval_record_by_field["approval_json"]["command"]
    elif mutation == "extra_document":
        approval_record_by_field["approval_json"]["asserted_authority"] = True
    elif mutation == "scope_mismatch":
        approval_record_by_field["scope_id"] = UUID(int=5)
    schema_name = "other_ptg" if mutation == "schema_mismatch" else SCHEMA
    approval_record_by_field["approval_sha256"] = _digest(approval_record_by_field["approval_json"])
    with pytest.raises((RegistryPTGProducerScopeError, ValueError)):
        reviews._verified_review(
            approval_record_by_field, schema_name, envelope_by_field["ownership"], inventory_by_field
        )


@pytest.fixture(autouse=True)
def legacy_source_discriminator(monkeypatch):
    """Keep existing negotiated/frozen fixture focus; allowed discovery has separate coverage."""
    from process import registry_ptg_allowed_plan_source

    monkeypatch.setattr(registry_ptg_allowed_plan_source, "is_allowed_snapshot", AsyncMock(return_value=False))
