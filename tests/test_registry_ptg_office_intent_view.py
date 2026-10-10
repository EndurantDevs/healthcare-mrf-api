# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source/actor-scoped discovery and closed wire results without native authority claims."""

from copy import deepcopy
from uuid import UUID

import pytest

from process import registry_ptg_office_intent as intent
from process import registry_ptg_office_intent_view as view
from process.registry_publication_queue import _json
from process.registry_record_store import RegistryActor
from tests.test_registry_ptg_office_approval import _command as office_command
from tests.test_registry_ptg_office_intent import Connection, actor, reserve, selector, source_command


def envelope():
    source = source_command()
    return {
        "actor": {"kind": "platform_admin", "user_id": str(actor().user_id), "client_id": "system"},
        "session_token_sha256": "b" * 64,
        "source_command": source,
        "selector": {
            **{name: source[name] for name in ("scope_id", "client_id")},
            "scope_approval_sha256": selector()["scope_approval_sha256"],
            "after": None,
            "limit": 20,
        },
    }


class Reader:
    def __init__(self, records):
        self.records = records
        self.calls = []
        self.active = True

    def is_in_transaction(self):
        return self.active

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        return deepcopy(self.records)


async def records(count=1, *, large=False, original_actor=None, original_source=None, unicode_reason=False):
    storage = Connection()
    for ordinal in range(count):
        selected = selector()
        selected["reason"] = (
            f"Reviewed café intent {ordinal}" if unicode_reason else f"Reviewed original office intent {ordinal}"
        )
        if large:
            selected["reason"] = "r" * 1000
            selected["selections"] = [
                {
                    "binding_id": str(UUID(int=binding + 1)),
                    "binding_revision": 1,
                    "dense_source_key": 2147483647,
                    "source_record_ordinal": 2**63 - 1,
                }
                for binding in range(100)
            ]
            selected["retained_generation_id"] += ordinal
        await reserve(storage, selection=selected, original_actor=original_actor, source=original_source)
    rows = list(storage.requests_by_key.values())
    for ordinal, row in enumerate(rows):
        row["request_id"] = UUID(int=ordinal + 1)
    return rows


async def listing(reader, *, original_actor=None, source=None, approval=None, after=None, limit=20):
    return await view.list_registry_ptg_office_prepare_intents(
        reader,
        original_actor or actor(),
        source or source_command(),
        scope_approval_sha256=approval or selector()["scope_approval_sha256"],
        after=after,
        limit=limit,
        control_schema="synthetic_registry",
    )


def test_signed_envelope_is_exact_detached_and_renewed_session_is_separate_authority():
    original = envelope()
    frozen = view.validated_office_intent_list_envelope(original)
    assert frozen == original and frozen is not original
    frozen["selector"]["limit"] = 1
    assert original["selector"]["limit"] == 20
    renewed = deepcopy(original)
    renewed["session_token_sha256"] = "c" * 64
    assert view.validated_office_intent_list_envelope(renewed)["selector"] == original["selector"]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda document: document.update(extra=True),
        lambda document: document["actor"].update(impersonator_id=None),
        lambda document: document["actor"].update(kind="client_admin"),
        lambda document: document["actor"].update(client_id="client"),
        lambda document: document["actor"].update(user_id=str(UUID(int=0))),
        lambda document: document.update(session_token_sha256="bad"),
        lambda document: document["selector"].update(extra=True),
        lambda document: document["selector"].update(scope_id=str(UUID(int=4))),
        lambda document: document["selector"].update(client_id="another-client"),
        lambda document: document["selector"].update(scope_approval_sha256="BAD"),
        lambda document: document["selector"].update(after=str(UUID(int=0))),
        lambda document: document["selector"].update(after="00000000000000000000000000000001"),
        lambda document: document["source_command"].update(extra=True),
    ],
)
def test_signed_envelope_rejects_substitutions_and_open_documents(mutation):
    document = envelope()
    mutation(document)
    with pytest.raises(ValueError):
        view.validated_office_intent_list_envelope(document)


@pytest.mark.parametrize("limit", [False, True, 0, 21, 1.0, "20"])
def test_signed_list_limit_is_integer_one_through_twenty(limit):
    document = envelope()
    document["selector"]["limit"] = limit
    with pytest.raises(ValueError):
        view.validated_office_intent_list_envelope(document)


@pytest.mark.asyncio
async def test_empty_page_uses_one_bounded_read_with_exact_authority_filters():
    reader = Reader([])
    page = await listing(reader, limit=1)
    assert page == {
        "source_command_sha256": intent._digest(source_command()),
        "scope_approval_sha256": selector()["scope_approval_sha256"],
        "items": [],
        "next_cursor": None,
    }
    assert len(reader.calls) == 1
    sql, args = reader.calls[0]
    assert sql.startswith("SELECT ") and "ORDER BY request_id LIMIT $9" in sql
    assert "actor_key=$1 AND command_json->'source_command'=$2::jsonb" in sql
    assert "command_json->>'operation'=$3 AND command_json->'version'=$4::jsonb" in sql
    assert all(
        f"command_json->'selector'->>'{name}'=${position}" in sql
        for name, position in (("scope_id", 5), ("client_id", 6), ("scope_approval_sha256", 7))
    )
    assert "octet_length(actor_json::text)<=2048" in sql and "octet_length(command_json::text)<=131072" in sql
    assert args == (
        intent._digest(intent._reactivation_actor(actor())),
        _json(source_command()),
        intent.OPERATION,
        "1",
        source_command()["scope_id"],
        source_command()["client_id"],
        selector()["scope_approval_sha256"],
        None,
        2,
    )


@pytest.mark.asyncio
async def test_transaction_required_before_any_read():
    reader = Reader([])
    reader.active = False
    with pytest.raises(ValueError, match="caller_transaction"):
        await listing(reader)
    assert not reader.calls


@pytest.mark.asyncio
async def test_pagination_preserves_original_handles_and_audit_without_repeating_source():
    rows = await records(3)
    original = deepcopy(rows)
    reader = Reader(rows)
    first = await listing(reader, limit=2)
    assert first["next_cursor"] == str(rows[1]["request_id"])
    assert [item["request_id"] for item in first["items"]] == [str(row["request_id"]) for row in rows[:2]]
    for item, row in zip(first["items"], rows, strict=False):
        assert set(item) == {"request_id", "state", "created_at", "semantic_sha256", "command_sha256", "selector"}
        assert item["selector"] == intent._document(row["command_json"])["selector"]
    reader.records = rows[2:]
    second = await listing(reader, after=first["next_cursor"], limit=2)
    assert second["next_cursor"] is None and len(second["items"]) == 1
    assert reader.calls[-1][1][7] == rows[1]["request_id"] and rows == original
    wire = _json(first)
    assert all(name not in wire for name in ("actor_json", "session_token_sha256", 'source_command"', "replayed"))


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", ["reverse", "duplicate", "before_cursor", "overflow"])
async def test_invalid_native_order_or_fetch_bound_refuses(malformed):
    rows = await records(3)
    after, limit = None, 20
    match malformed:
        case "reverse":
            rows.reverse()
        case "duplicate":
            rows[1]["request_id"] = rows[0]["request_id"]
        case "before_cursor":
            after = str(rows[0]["request_id"])
        case "overflow":
            limit = 1
    with pytest.raises(ValueError):
        await listing(Reader(rows), after=after, limit=limit)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update(actor_key="0" * 64),
        lambda row: row.update(request_sha256="0" * 64),
        lambda row: row.update(actor_json="{}"),
        lambda row: row.update(command_json=None),
        lambda row: row.update(session_token_sha256="bad"),
        lambda row: row.update(state="unknown"),
        lambda row: row.update(idempotency_key="replacement"),
        lambda row: row.update(created_at="2026-01-01"),
    ],
)
async def test_corrupt_stored_audit_fails_closed(mutation):
    rows = await records()
    mutation(rows[0])
    with pytest.raises(ValueError):
        await listing(Reader(rows))


@pytest.mark.asyncio
@pytest.mark.parametrize("substitution", ["actor", "source", "approval"])
async def test_valid_stored_record_from_another_authority_never_projects(substitution):
    original_actor, source, approval = actor(), source_command(), selector()["scope_approval_sha256"]
    match substitution:
        case "actor":
            original_actor = RegistryActor("platform_admin", UUID(int=42), "system")
        case "source":
            source["reason"] = "Different fully valid original source"
        case "approval":
            approval = "e" * 64
    with pytest.raises(ValueError):
        await listing(Reader(await records()), original_actor=original_actor, source=source, approval=approval)


@pytest.mark.asyncio
async def test_wire_byte_bound_truncates_at_last_original_uuid_without_skipping():
    rows = await records(21, large=True)
    page = await listing(Reader(rows))
    assert 0 < len(page["items"]) < 20
    assert len(_json(page).encode()) <= view.MAXIMUM_WIRE_BYTES
    last = len(page["items"]) - 1
    assert page["next_cursor"] == str(rows[last]["request_id"])
    remainder = await listing(Reader(rows[last + 1 :]), after=page["next_cursor"])
    assert remainder["items"][0]["request_id"] == str(rows[last + 1]["request_id"])


async def internal_result(*, completed=False, unicode_reason=False):
    request_record = (await records(unicode_reason=unicode_reason))[0]
    command = intent._document(request_record["command_json"])
    semantic = intent._intent(
        source_command(), {name: command["selector"][name] for name in intent._SELECTOR_FIELDS}, actor()
    )
    receipt = intent._stored_receipt(request_record, semantic, True)
    if not completed:
        return {"intent": receipt, "preparation": None}
    receipt["state"] = "completed"
    selected = command["selector"]
    physical = office_command()
    physical["input_row_count"] = len(selected["selections"])
    physical["office_evidence_kind"] = "reviewed_exact_office"
    physical["source"]["evidence"]["graph_identity"] = view._graph_identity(source_command()["published_identity"])
    physical.update(
        {
            name: selected[name]
            for name in (
                "capture_id",
                "scope_id",
                "client_id",
                "scope_approval_sha256",
                "idempotency_key",
                "reason",
                "retained_generation_id",
            )
        }
    )
    source_command_by_field = source_command()
    physical["source"].update(
        {
            name: source_command_by_field[name]
            for name in ("coordinates", "legal_company_id", "approved_revision", "file_versions", "network_id")
        }
    )
    physical["source"].update(
        {name: source_command_by_field["source"][name] for name in ("binding_source_key", "snapshot_id")}
    )
    physical["source"]["source_scope"].update(
        {
            name: source_command_by_field[name]
            for name in ("review_type", "scope_id", "plan_id", "plan_market_type", "selection_mode")
        }
    )
    physical["source"]["source_scope"].update(
        snapshot_id=source_command_by_field["source"]["snapshot_id"], approval_sha256=selected["scope_approval_sha256"]
    )
    return {
        "intent": receipt,
        "preparation": {
            "capture_id": selected["capture_id"],
            "state": "prepared",
            "command": physical,
            "command_sha256": view.capture_digest(physical),
            "manifest_sha256": "d" * 64,
        },
    }


def project(result, *, original_actor=None):
    return view.project_registry_ptg_office_prepare_result(
        result, original_actor or actor(), source_command(), scope_approval_sha256=selector()["scope_approval_sha256"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [False, True])
async def test_projector_has_closed_wire_and_original_selector(completed):
    result = await internal_result(completed=completed)
    original = deepcopy(result)
    projected = project(result)
    assert set(projected) == {"source_command_sha256", "scope_approval_sha256", "intent", "preparation"}
    assert projected["intent"]["selector"] == result["intent"]["command"]["selector"]
    assert projected["preparation"] == result["preparation"] and result == original
    assert "command" not in projected["intent"] and "replayed" not in projected["intent"]


def _change_prepared_command(result, change):
    command = result["preparation"]["command"]
    change(command)
    result["preparation"]["command_sha256"] = view.capture_digest(command)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result["preparation"].update(capture_id="bad"),
        lambda result: result["preparation"].update(command_sha256="0" * 64),
        lambda result: result["preparation"].update(manifest_sha256="bad"),
        lambda result: _change_prepared_command(result, lambda command: command.update(reason="different reason")),
        lambda result: _change_prepared_command(result, lambda command: command.update(retained_generation_id=42)),
        lambda result: _change_prepared_command(result, lambda command: command["source"].update(network_id=42)),
        lambda result: _change_prepared_command(result, lambda command: command.update(input_row_count=42)),
        lambda result: _change_prepared_command(
            result, lambda command: command.update(office_evidence_kind="payer_exact_office")
        ),
        lambda result: _change_prepared_command(
            result, lambda command: command["source"]["evidence"]["graph_identity"].update(map_sha256="0" * 64)
        ),
        lambda result: result.update(preparation=None),
        lambda result: result["intent"].update(command_sha256="0" * 64),
    ],
)
async def test_projector_refuses_changed_preparation_or_internal_receipt(mutation):
    result = await internal_result(completed=True)
    mutation(result)
    with pytest.raises(ValueError):
        project(result)


@pytest.mark.asyncio
async def test_projector_refuses_another_actor():
    with pytest.raises(ValueError):
        project(await internal_result(), original_actor=RegistryActor("platform_admin", UUID(int=42), "system"))


@pytest.mark.asyncio
async def test_projector_caps_actual_serialized_bytes():
    result = await internal_result(completed=True)
    _change_prepared_command(
        result,
        lambda command: command["source"]["evidence"].update(source_authority={"large_synthetic_field": "é" * 50000}),
    )
    with pytest.raises(ValueError, match="wire_too_large"):
        project(result)


@pytest.mark.asyncio
async def test_renewed_session_listing_keeps_original_stored_audit_and_same_handles():
    reader = Reader(await records())
    original_audit = deepcopy(reader.records)
    original_envelope = envelope()
    renewed = deepcopy(original_envelope)
    renewed["session_token_sha256"] = "c" * 64
    pages = []
    for document in (original_envelope, renewed):
        checked = view.validated_office_intent_list_envelope(document)
        signed_actor = checked["actor"]
        pages.append(
            await listing(
                reader,
                original_actor=RegistryActor(
                    signed_actor["kind"], UUID(signed_actor["user_id"]), signed_actor["client_id"]
                ),
            )
        )
    assert pages[0] == pages[1] and reader.records == original_audit
    assert reader.records[0]["session_token_sha256"] == "a" * 64
    assert "session_token_sha256" not in _json(pages)


@pytest.mark.asyncio
async def test_impersonated_or_non_administrator_actor_never_reaches_query():
    reader = Reader([])
    for invalid_actor in (
        RegistryActor("platform_admin", actor().user_id, "system", UUID(int=42)),
        RegistryActor("client_owner", actor().user_id, "client"),
    ):
        with pytest.raises(ValueError):
            await listing(reader, original_actor=invalid_actor)
    assert not reader.calls


@pytest.mark.asyncio
async def test_corrupt_lookahead_audit_cannot_supply_pagination_hint():
    rows = await records(2)
    rows[1]["request_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        await listing(Reader(rows), limit=1)


@pytest.mark.parametrize("limit", [1, 20])
def test_list_envelope_accepts_exact_bounds_and_original_uuid_cursor(limit):
    document = envelope()
    document["selector"].update(after=str(UUID(int=42)), limit=limit)
    assert view.validated_office_intent_list_envelope(document) == document


@pytest.mark.asyncio
async def test_source_wire_hash_matches_existing_utf8_transport_contract():
    original_source = source_command()
    original_source["reason"] = "Reviewed café source"
    page = await listing(Reader(await records(original_source=original_source)), source=original_source)
    assert page["source_command_sha256"] == view.capture_digest(original_source)
    assert page["source_command_sha256"] != intent._digest(original_source)


@pytest.mark.asyncio
async def test_prepared_unicode_reason_uses_native_capture_hash_and_keeps_selector():
    result = await internal_result(completed=True, unicode_reason=True)
    prepared = result["preparation"]
    assert prepared["command_sha256"] == view.capture_digest(prepared["command"])
    assert prepared["command_sha256"] != intent._digest(prepared["command"])
    assert project(result)["intent"]["selector"]["reason"] == result["intent"]["command"]["selector"]["reason"]
