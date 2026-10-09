# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic dual-authority inputs for configured billing query tests."""

import base64
from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy import Integer, literal, select

from api import billing_search_http as billing_http
from api import billing_search_transport_contract as billing_contract
from api import billing_search_verified_transport as verified_billing
from api import custom_import_billing_http as billing
from api import custom_import_read_http as transport
from api.billing_search_endpoint_access import validate_billing_search_endpoint_access_state
from api.custom_import_billing_query import validate_billing_import_query
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadFieldValue
from tests import test_billing_search_endpoint_access as native_fixtures
from tests import test_custom_import_read_http as import_fixtures


def signed_request(*, include_filter=True, require_match=True, native_changes=None, body_changes=None):
    native_pairs = native_fixtures._query_pairs(**(native_changes or {}))
    native_headers = native_fixtures._signed_headers(native_pairs)
    context_header = native_headers[billing_contract.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER]
    context_bytes = base64.urlsafe_b64decode(context_header + "=" * (-len(context_header) % 4))
    digest = billing_contract._framed_sha256(verified_billing._CONTEXT_DOMAIN, context_bytes)
    document_by_field = {
        "target": import_fixtures._TARGET,
        "native_query": dict(native_pairs),
        "context": [],
        "filters": [],
        "order": None if require_match else [{"field_id": "metric", "direction": "desc"}],
        "require_match": require_match,
        "include_filter": include_filter,
        "billing_transport_context_sha256": digest,
    }
    document_by_field.update(body_changes or {})
    body = transport._canonical_json_bytes(document_by_field)
    return SimpleNamespace(
        method="POST",
        path=billing.CUSTOM_IMPORT_BILLING_SEARCH_PATH,
        query_string="",
        body=body,
        headers={
            **native_headers,
            **import_fixtures._provider_headers(body=body, path=billing.CUSTOM_IMPORT_BILLING_SEARCH_PATH),
        },
    )


class Session:
    """Record transaction and hydration boundaries without native data."""

    def __init__(self):
        self.events = []
        self.hydration_pages = []
        self.contexts = []
        self.prepared_count = 0
        self.active = False

    def in_transaction(self):
        return self.active

    @asynccontextmanager
    async def begin(self):
        self.events.append("begin")
        self.active = True
        try:
            yield self
        finally:
            self.events.append("end")
            self.active = False

    async def execute(self, statement):
        assert str(statement) == str(billing_http._READ_TRANSACTION_SQL)
        assert self.active
        self.events.append("snapshot")


def _read_service_factory(session, *, imported):
    """Create an authorized synthetic read service with bounded hydration."""

    class ReadService:
        def __init__(self, *, authorizer):
            self.authorizer = authorizer

        async def prepare_npi_entity_relation(self, same_session, **kwargs):
            assert same_session is session and session.active
            self.authorizer.authorize(kwargs["authorization"], target=kwargs["target"])
            query = kwargs["query"]
            session.prepared_count += 1
            session.events.append("prepare")
            columns = [literal("1000000004").label("entity_value")]
            for ordinal, _term in enumerate(query.order_terms or ()):
                columns.append(literal(7, type_=Integer).label(f"sort_{ordinal}"))
            return PreparedNpiEntityRelation(select(*columns), query.order_terms or (), "b" * 64, "c" * 64)

        async def hydrate_npi_page(self, same_session, **kwargs):
            assert same_session is session and session.active
            session.hydration_pages.append(kwargs["entity_values"])
            session.events.append("hydrate")
            if not imported:
                return {}
            return {
                entity: SimpleNamespace(
                    root_fields=(ReadFieldValue("metric", "integer", "value", 7),),
                    context_fields=(),
                    children=(),
                )
                for entity in kwargs["entity_values"]
            }

    return ReadService


def install_pipeline(monkeypatch, session, *, provider_rows=None, imported=True, finality_failure=False):
    """Install the signed transport with a synthetic pinned read pipeline."""

    native_rows = [{"npi": 1000000004}] if provider_rows is None else provider_rows
    import_fixtures._install_keyring(monkeypatch)
    monkeypatch.setattr(billing_http, "_transport_keyring", native_fixtures._keyring)
    monkeypatch.setattr(billing_http, "_cursor_keyring", lambda: object())
    monkeypatch.setattr(transport, "_resolve_pinned_target", import_fixtures._resolved_target)

    @asynccontextmanager
    async def bounded(same_session, **_kwargs):
        assert same_session is session and session.active
        session.events.append("bounded")
        yield

    async def search(same_session, *, access, import_context, **_kwargs):
        assert same_session is session and session.active
        _, access_state = validate_billing_search_endpoint_access_state(access, trusted_now=import_fixtures._NOW)
        validate_billing_import_query(import_context, endpoint_access_state_sha256=access_state)
        session.contexts.append(import_context)
        session.events.append("search")
        return object()

    def shape(_access, _result, *, import_scope, **_kwargs):
        assert import_scope == session.contexts[-1].import_scope
        session.events.append("shape")
        return {
            "items": [dict(row) for row in native_rows],
            "pagination": {"limit": len(native_rows), "has_more": False, "next_cursor": None},
        }

    async def finality(same_session, _target):
        assert same_session is session and session.active
        session.events.append("finality")
        if finality_failure:
            raise transport.CustomImportReadUnavailableError("synthetic finality unavailable")

    monkeypatch.setattr(billing, "_bounded_read_window", bounded)
    monkeypatch.setattr(billing, "CustomImportReadService", _read_service_factory(session, imported=imported))
    monkeypatch.setattr(billing, "search_exact_billing_provider_page", search)
    monkeypatch.setattr(billing, "shape_billing_search_response", shape)
    monkeypatch.setattr(billing, "verify_published_generation", finality)
