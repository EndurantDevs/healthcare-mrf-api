# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured billing reads require paired authority and bounded hydration."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import literal, select

from api import billing_search_http as native_http
from api import custom_import_billing_http as billing
from api import custom_import_read_http as transport
from tests import test_billing_search_endpoint_access as native_fixtures
from tests import test_custom_import_read_http as import_fixtures
from tests.custom_import_billing_support import Session, install_pipeline, signed_request


@pytest.mark.parametrize("include_filter", (False, True))
@pytest.mark.asyncio
async def test_billing_filter_inclusion_is_independent_and_uses_one_snapshot(monkeypatch, include_filter):
    session = Session()
    install_pipeline(monkeypatch, session)
    reply = await billing.serve_custom_import_billing_search(signed_request(include_filter=include_filter), session)
    assert reply.status == 200 and reply.headers["Cache-Control"] == "private, no-store"
    payload = json.loads(reply.body)
    assert ("custom_import" in payload["items"][0]) is include_filter
    assert payload.get("data") is None
    assert session.prepared_count == 1
    assert session.events == ["begin", "snapshot", "bounded", "prepare", "search", "shape"] + (
        ["hydrate"] if include_filter else []
    ) + ["finality", "end"]
    assert not session.active


@pytest.mark.parametrize("include_filter", (False, True))
@pytest.mark.asyncio
async def test_billing_native_scope_and_hydration_reuse_latest_prepared_snapshot(monkeypatch, include_filter):
    session = Session()
    install_pipeline(monkeypatch, session)
    read_service = billing.CustomImportReadService
    native_search = billing.search_exact_billing_provider_page
    scoped_relations = []

    class ScopedReadService(read_service):
        async def scope_npi_entity_relation(self, same_session, **kwargs):
            assert same_session is session and session.active
            self.authorizer.authorize(kwargs["authorization"], target=kwargs["pinned_target"])
            scoped = replace(kwargs["prepared"], statement=kwargs["prepared"].statement.where(literal(True)))
            scoped_relations.append(scoped)
            session.events.append("scope")
            return scoped

        async def hydrate_npi_page(self, same_session, **kwargs):
            assert kwargs["prepared"] is scoped_relations[-1]
            return await super().hydrate_npi_page(same_session, **kwargs)

    async def scoped_search(same_session, *, import_context, native_scope_preparer, **kwargs):
        scoped_context = await native_scope_preparer(select(literal(1000000004).label("npi")))
        assert scoped_context is not import_context and scoped_context.import_scope == import_context.import_scope
        assert scoped_context.prepared is scoped_relations[-1]
        return await native_search(same_session, import_context=scoped_context, **kwargs)

    monkeypatch.setattr(billing, "CustomImportReadService", ScopedReadService)
    monkeypatch.setattr(billing, "search_exact_billing_provider_page", scoped_search)
    reply = await billing.serve_custom_import_billing_search(signed_request(include_filter=include_filter), session)

    assert reply.status == 200 and len(scoped_relations) == 1 and session.prepared_count == 1
    assert ("custom_import" in json.loads(reply.body)["items"][0]) is include_filter
    assert session.events == ["begin", "snapshot", "bounded", "prepare", "scope", "search", "shape"] + (
        ["hydrate"] if include_filter else []
    ) + ["finality", "end"]
    assert not session.active


@pytest.mark.asyncio
async def test_optional_unmatched_sort_emits_explicit_null_when_included(monkeypatch):
    session = Session()
    install_pipeline(monkeypatch, session, imported=False)
    reply = await billing.serve_custom_import_billing_search(signed_request(require_match=False), session)
    assert reply.status == 200
    assert json.loads(reply.body)["items"][0]["custom_import"] is None


@pytest.mark.asyncio
async def test_fresh_paired_transport_proofs_keep_stable_import_cursor_coordinates(monkeypatch):
    session = Session()
    install_pipeline(monkeypatch, session)
    first_request = signed_request()
    first_reply = await billing.serve_custom_import_billing_search(first_request, session)
    monkeypatch.setattr(native_fixtures, "REQUEST_ID", "123e4567-e89b-42d3-a456-426614174001")
    monkeypatch.setattr(native_fixtures, "ISSUED_AT", "2031-01-02T03:03:56Z")
    second_request = signed_request()
    second_reply = await billing.serve_custom_import_billing_search(second_request, session)
    assert first_reply.status == second_reply.status == 200
    assert first_request.body != second_request.body
    assert session.contexts[0].endpoint_access_state_sha256 != session.contexts[1].endpoint_access_state_sha256
    assert session.contexts[0].import_scope == session.contexts[1].import_scope


@pytest.mark.parametrize("missing_prefix", ("X-HealthPorta-Billing-Search-", "X-HealthPorta-Extension-Read-"))
@pytest.mark.asyncio
async def test_both_transport_proofs_are_required_before_transaction(monkeypatch, missing_prefix):
    session = Session()
    install_pipeline(monkeypatch, session)
    request = signed_request()
    request.headers = {
        name: value for name, value in request.headers.items() if not name.lower().startswith(missing_prefix.lower())
    }
    reply = await billing.serve_custom_import_billing_search(request, session)
    assert reply.status == 404 and session.events == []
    assert json.loads(reply.body)["error"]["code"] == "resource_not_found"


@pytest.mark.parametrize(
    "body_changes",
    (
        {"billing_transport_context_sha256": "d" * 64},
        {"billing_transport_context_sha256": "invalid"},
        {"unknown_field": True},
        {"native_query": {"billing_entity_ref": "unknown"}},
    ),
)
@pytest.mark.asyncio
async def test_malformed_or_cross_paired_body_fails_closed(monkeypatch, body_changes):
    session = Session()
    install_pipeline(monkeypatch, session)
    reply = await billing.serve_custom_import_billing_search(signed_request(body_changes=body_changes), session)
    assert reply.status == 404 and session.events == []


@pytest.mark.parametrize("changes", ({"method": "GET"}, {"path": "/other"}, {"query_string": "extra=1"}))
@pytest.mark.asyncio
async def test_composition_is_only_the_closed_post_route(monkeypatch, changes):
    session = Session()
    install_pipeline(monkeypatch, session)
    request = signed_request()
    for name, value in changes.items():
        setattr(request, name, value)
    reply = await billing.serve_custom_import_billing_search(request, session)
    assert reply.status == 404 and session.events == []


@pytest.mark.asyncio
async def test_full_billing_page_retains_100_rows_and_bounded_unique_hydration(monkeypatch):
    session = Session()
    rows = [{"npi": 1000000000 + ordinal} for ordinal in range(100)]
    install_pipeline(monkeypatch, session, provider_rows=rows)
    reply = await billing.serve_custom_import_billing_search(signed_request(native_changes={"limit": "100"}), session)
    assert reply.status == 200 and len(json.loads(reply.body)["items"]) == 100
    assert session.prepared_count == 1
    assert [len(page) for page in session.hydration_pages] == [50, 50]


@pytest.mark.asyncio
async def test_duplicate_npis_reuse_selected_family_payload(monkeypatch):
    session = Session()
    rows = [{"npi": 1000000004}, {"npi": 1000000004}]
    install_pipeline(monkeypatch, session, provider_rows=rows)
    reply = await billing.serve_custom_import_billing_search(signed_request(), session)
    assert reply.status == 200 and session.hydration_pages == [("1000000004",)]
    payload = json.loads(reply.body)
    assert payload["items"][0]["custom_import"] == payload["items"][1]["custom_import"]


@pytest.mark.asyncio
async def test_native_byte_bound_fails_before_import_hydration(monkeypatch):
    session = Session()
    install_pipeline(
        monkeypatch,
        session,
        provider_rows=[{"npi": 1000000004, "synthetic_text": "x" * (native_http._MAX_SUCCESS_BODY_BYTES + 1)}],
    )
    reply = await billing.serve_custom_import_billing_search(signed_request(), session)
    assert reply.status == 503 and session.hydration_pages == [] and not session.active


@pytest.mark.asyncio
async def test_generation_finality_failure_does_not_expose_hydrated_rows(monkeypatch):
    session = Session()
    install_pipeline(monkeypatch, session, finality_failure=True)
    reply = await billing.serve_custom_import_billing_search(signed_request(), session)
    assert reply.status == 503 and not session.active
    assert "items" not in json.loads(reply.body)
    assert session.events[-2:] == ["finality", "end"]


@pytest.mark.asyncio
async def test_missing_include_filter_is_rejected_even_with_valid_signature(monkeypatch):
    session = Session()
    install_pipeline(monkeypatch, session)
    request = signed_request()
    document_by_field = json.loads(request.body)
    document_by_field.pop("include_filter")
    request.body = transport._canonical_json_bytes(document_by_field)
    request.headers.update(
        import_fixtures._provider_headers(body=request.body, path=billing.CUSTOM_IMPORT_BILLING_SEARCH_PATH)
    )
    reply = await billing.serve_custom_import_billing_search(request, session)
    assert reply.status == 404 and session.events == []


@pytest.mark.asyncio
async def test_pinned_definition_request_error_retains_redacted_400(monkeypatch):
    session = Session()
    install_pipeline(monkeypatch, session)

    async def unavailable(*_args, **_kwargs):
        raise billing.CustomImportReadRequestError("synthetic field rejection")

    monkeypatch.setattr(billing, "_read_billing_payload", unavailable)
    reply = await billing.serve_custom_import_billing_search(signed_request(), session)
    assert reply.status == 400 and not session.active
    assert json.loads(reply.body) == {
        "error": {
            "code": "custom_import_read_request_invalid",
            "message": "Invalid custom import read request.",
        }
    }
