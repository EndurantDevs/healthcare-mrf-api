# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed legacy service selectors retain complete release and policy identity."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters

from api import custom_import_provider_service_http as service_http
from api import plan_pricing_state_scan as scan
from api import provider_service_legacy_selection as legacy
from api import provider_service_source_policy as policy
from api.billing_search_cursor import BillingSearchCursorError
from api.endpoint import pricing
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from tests.test_custom_import_plan_pricing import _import_context
from tests.test_plan_pricing_state_scan import _keyring
from tests.test_plan_pricing_state_scan_policy import _scope
from tests.test_plan_release_serving import _network_binding, _release_selection
from tests.test_pricing_api import make_request
from tests.test_provider_service_source_policy import _fixture


def _args(**changes):
    native_by_name = {"code_system": "CPT", "code": "27447", "limit": "2", "include_allowed_amounts": "false"}
    return RequestParameters({name: [entry] for name, entry in (native_by_name | changes).items()})


def _selection():
    return _release_selection(
        _network_binding(9, "snapshot-allowed", "source-allowed"),
        _network_binding(27, "snapshot-other", "source-other"),
    )


def _install(monkeypatch, selection=None, release_ids=None):
    selection = selection or _selection()
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(
                scalars=lambda: release_ids if release_ids is not None else [selection.plan_release_id]
            )
        )
    )
    resolver = AsyncMock(return_value=selection)
    monkeypatch.setattr(legacy, "resolve_plan_release_serving", resolver)
    return session, selection, resolver


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selectors",
    [
        {"plan_id": "990000001"},
        {"plan_external_id": "99-0000001", "market_type": "group"},
        {"source_key": "SOURCE-ALLOWED"},
        {"snapshot_id": "snapshot-allowed"},
        {"plan_id": "990000001", "source_key": "source-allowed", "snapshot_id": "snapshot-allowed"},
    ],
)
@pytest.mark.parametrize("imported", [False, True])
@pytest.mark.parametrize("selected", [False, True])
async def test_actual_service_handler_normalizes_and_preserves_full_release(monkeypatch, selectors, imported, selected):
    session, selection, resolver = _install(monkeypatch)
    request = make_request([])
    request.ctx.sa_session = session
    request.path, request.method = "/api/v1/registry/serving/providers/collection", "POST"
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", AsyncMock(return_value=selection))
    monkeypatch.setattr(pricing, "resolve_plan_release_guard_selection", AsyncMock(return_value=selection))
    monkeypatch.setattr(
        pricing,
        "_resolve_ptg_specialty_or_raise",
        AsyncMock(return_value=SimpleNamespace(unresolved_specialty=None, taxonomy_codes=())),
    )
    observed_pages = []

    async def search(same_session, args, pagination, **options):
        assert same_session is session and options["release_selection"] is selection
        assert (options.get("import_context") is not None) is imported
        assert not any(args.get(name) for name in legacy._SELECTOR_FIELDS)
        allowed = legacy.restrict_service_bindings(selection, selection.in_network_bindings)
        expected = (9, 27) if set(selectors) <= {"plan_id", "plan_external_id", "market_type"} else (9,)
        assert tuple(binding.binding_ordinal for binding in allowed) == expected
        observed_pages.append((pagination.offset, legacy.service_legacy_selection_sha256()))
        return {
            "items": [{"npi": 1000000001}],
            "pagination": {"total": 1},
            "query": {"plan_release_id": selection.plan_release_id},
        }

    monkeypatch.setattr(pricing, "search_current_ptg2_index", search)
    original = selection.bindings, selection.binding_set_digest
    with _scope(selected=selected):
        for offset in ("0", "2"):
            reply = await service_http._service_page(
                request,
                native_args=_args(**selectors, offset=offset),
                **({"import_context": _import_context(ordered=False)} if imported else {}),
            )
            assert reply.status == 200 and json.loads(reply.body)["items"][0]["npi"] == 1000000001
            assert legacy.service_legacy_selection_sha256() is None
    assert (selection.bindings, selection.binding_set_digest) == original
    assert observed_pages[0][0] == 0 and observed_pages[1][0] == 2 and observed_pages[0][1] == observed_pages[1][1]
    assert resolver.await_count == 2 and session.execute.await_count == 2
    assert resolver.await_args.kwargs == {"projection_only": True}
    sql, parameters = session.execute.await_args.args
    assert "LIMIT 2" in str(sql) and "ptg2_current" not in str(sql) and "plan_name" not in str(sql)
    assert parameters["source_key"] == selectors.get("source_key", "").lower()


@pytest.mark.asyncio
@pytest.mark.parametrize("release_ids", [[], ["release-one", "release-two"]])
async def test_absent_or_ambiguous_identity_refuses_before_native_query(monkeypatch, release_ids):
    session, _, resolver = _install(monkeypatch, release_ids=release_ids)
    with _scope(), pytest.raises(InvalidUsage, match="one canonical release"):
        async with legacy.legacy_service_selection(session, _args(plan_id="990000001")):
            pytest.fail("ambiguous selector was admitted")
    resolver.assert_not_awaited()
    assert legacy.service_legacy_selection_sha256() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selectors",
    [
        {"plan_id": "990000001", "plan_external_id": "other"},
        {"source_key": "source-allowed", "plan_market_type": "group", "market_type": "individual"},
        {"plan_id": "990000001", "plan_id_type": "unknown"},
        {"source_key": "source-allowed", "plan_release_id": "conflicting-release"},
    ],
)
async def test_unproven_or_conflicting_selectors_fail_before_lookup(monkeypatch, selectors):
    session, _, resolver = _install(monkeypatch)
    with _scope(), pytest.raises(InvalidUsage):
        async with legacy.legacy_service_selection(session, _args(**selectors)):
            pytest.fail("invalid selector was admitted")
    session.execute.assert_not_awaited()
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_denial_intersects_original_ordinals_without_restoring_public(monkeypatch):
    held = _fixture(monkeypatch, excluded=(7,))
    public = replace(
        held.binding, binding_ordinal=27, plan_id="public", snapshot_id="public-snapshot", source_key="public-source"
    )
    held.batches[0].append(
        {
            "snapshot_id": public.snapshot_id,
            "status": "published",
            "manifest": {"serving_index": {"source_key": public.source_key}},
        }
    )
    selection = _release_selection(held.binding, public)
    lookup, _, _ = _install(monkeypatch, selection)
    with _scope(pin=held.scope.manifest):
        async with legacy.legacy_service_selection(lookup, _args(source_key=held.binding.source_key)):
            admitted = await policy.admitted_plan_release_bindings(held.session, selection)
            assert admitted == ()
            assert selection.in_network_bindings == (held.binding, public)
    assert held.driver.fetchrow.await_count == 3


async def _legacy_cursor_scope(session, selector):
    async with legacy.legacy_service_selection(session, _args(source_key=selector)) as args:
        return scan._cursor_scope(_selection(), args, "CPT", "27447", "MI", 2)


@pytest.mark.asyncio
async def test_real_cursor_retains_restriction_and_refuses_changed_selector(monkeypatch):
    session, _, _ = _install(monkeypatch)
    now = 2_000_000_000
    with _scope():
        original = await _legacy_cursor_scope(session, "source-allowed")
        token = scan._seal_position((1000000001, 1, 0, 1), keyring=_keyring(), trusted_now=now, scope=original)
        retained = await _legacy_cursor_scope(session, "source-allowed")
        assert scan._open_position(token, keyring=_keyring(), trusted_now=now + 1, scope=retained) == (
            1000000001,
            1,
            0,
            1,
        )
        changed = await _legacy_cursor_scope(session, "source-other")
        with pytest.raises(BillingSearchCursorError):
            scan._open_position(token, keyring=_keyring(), trusted_now=now + 1, scope=changed)
    assert legacy.service_legacy_selection_sha256() is None


@pytest.mark.asyncio
async def test_cancellation_restores_context_and_changed_release_refuses(monkeypatch):
    session, selection, _ = _install(monkeypatch)
    with _scope(), pytest.raises(asyncio.CancelledError):
        async with legacy.legacy_service_selection(session, _args(source_key="source-allowed")):
            with pytest.raises(PlanPricingProjectionUnavailable, match="identity changed"):
                legacy.restrict_service_bindings(
                    replace(selection, binding_set_digest="f" * 64), selection.in_network_bindings
                )
            with pytest.raises(PlanPricingProjectionUnavailable, match="binding identity changed"):
                legacy.restrict_service_bindings(selection, (replace(selection.bindings[0], source_key="other"),))
            raise asyncio.CancelledError
    assert legacy.service_legacy_selection_sha256() is None


@pytest.mark.asyncio
async def test_unscoped_legacy_arguments_and_direct_parser_remain_unchanged(monkeypatch):
    session, _, resolver = _install(monkeypatch)
    args = _args(source_key="source-allowed")
    async with legacy.legacy_service_selection(session, args) as actual:
        assert actual is args and legacy.service_legacy_selection_sha256() is None
    session.execute.assert_not_awaited()
    resolver.assert_not_awaited()
    with pytest.raises(service_http.CustomImportReadRequestError):
        service_http._parse_service_native_query({"code": "27447", "code_system": "CPT", "plan_id": "990000001"})


@pytest.mark.asyncio
async def test_allowed_role_uses_its_original_identity_without_ordinal_collision(monkeypatch):
    network = _selection().in_network_bindings[0]
    allowed = replace(network, role="allowed_amounts", source_key="allowed-evidence", snapshot_id="allowed-snapshot")
    selection = _release_selection(network, allowed)
    session, _, _ = _install(monkeypatch, selection)
    with _scope():
        async with legacy.legacy_service_selection(session, _args(snapshot_id=allowed.snapshot_id)):
            assert legacy.restrict_service_bindings(selection, selection.in_network_bindings) == ()
            assert legacy.restrict_service_bindings(selection, selection.allowed_amount_bindings) == (allowed,)
    assert selection.bindings == (network, allowed)


def _install_signed_native(monkeypatch, denied_provider):
    from contextlib import asynccontextmanager

    from api import custom_import_provider_collection as managed
    from api import custom_import_read_http as transport
    from tests import test_custom_import_provider_service_http as fixtures
    from tests import test_custom_import_read_http as permits
    from tests.test_network_catalog_routes import v2_envelope

    selector_session, selection, _ = _install(monkeypatch)
    session = fixtures._Session()
    session.execute = selector_session.execute
    session.sql_execute = selector_session.execute
    page = service_http._service_page
    fixtures._install(monkeypatch, session, page_items=[{"npi": "1000000000" if denied_provider else "1104212877"}])
    monkeypatch.setattr(service_http, "_service_page", page)
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", AsyncMock(return_value=selection))
    monkeypatch.setattr(pricing, "resolve_plan_release_guard_selection", AsyncMock(return_value=selection))
    monkeypatch.setattr(
        pricing,
        "_resolve_ptg_specialty_or_raise",
        AsyncMock(return_value=SimpleNamespace(unresolved_specialty=None, taxonomy_codes=())),
    )

    async def search(same_session, args, pagination, **options):
        assert same_session is session and options["import_context"].require_match
        assert legacy.restrict_service_bindings(selection, selection.in_network_bindings) == (selection.bindings[0],)
        return {
            "items": [{"npi": "1000000000" if denied_provider else "1104212877"}],
            "pagination": {"total": 1},
            "query": {"plan_release_id": selection.plan_release_id},
            "plan_release_id": selection.plan_release_id,
            "pricing_scope": "plan_scoped_ptg",
            "custom_import_native_entry_ids": ["a" * 64],
        }

    monkeypatch.setattr(pricing, "search_current_ptg2_index", search)
    reader_entries = []

    @asynccontextmanager
    async def reader_scope(request, parsed, *, temporary_setup):
        assert temporary_setup is not None
        reader_entries.append("reader")
        with _scope() as scope:
            request.ctx = SimpleNamespace(sa_session=session)
            yield session, scope.manifest

    monkeypatch.setattr(managed, "managed_import_collection_scope", reader_scope)
    return session, selection, reader_entries


@pytest.mark.asyncio
@pytest.mark.parametrize("denied_provider", [False, True])
@pytest.mark.parametrize("bad_signature", [False, True])
async def test_signed_managed_selector_uses_actual_handler_and_hydration_guard(
    monkeypatch, denied_provider, bad_signature
):
    from api import custom_import_read_http as transport
    from tests import test_custom_import_provider_service_http as fixtures
    from tests import test_custom_import_read_http as permits
    from tests.test_network_catalog_routes import v2_envelope

    session, selection, reader_entries = _install_signed_native(monkeypatch, denied_provider)
    failures = []
    monkeypatch.setattr(transport, "_log_failure", failures.append)
    document = json.loads(
        fixtures._body(
            native_query={"code_system": "CPT", "code": "99213", "source_key": "source-allowed", "limit": "2"}
        )
    )
    document["managed_collection"] = v2_envelope({"kind": "by-service", "native_query": document["native_query"]})
    body = transport._canonical_json_bytes(document)
    request = fixtures._request(body)
    request.args = {}
    request.headers = permits._headers(
        body=body,
        path=service_http.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
        contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
        body_sha256=transport.managed_provider_body_sha256,
        signature_message=transport.custom_import_provider_signature_message,
    )
    if bad_signature:
        request.headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = "A" * 43
    reply = await service_http.serve_custom_import_provider_service(request, None)
    assert reply.status == (404 if bad_signature else 503 if denied_provider else 200), (reply.body, failures)
    assert reader_entries == ([] if bad_signature else ["reader"])
    if bad_signature:
        session.sql_execute.assert_not_awaited()
    if denied_provider and not bad_signature:
        assert len(failures) == 1 and "provider import match is unavailable" in str(failures[0])
    if denied_provider or bad_signature:
        assert b'"items"' not in reply.body
    else:
        payload_by_field = json.loads(reply.body)
        assert (
            payload_by_field["plan_release_id"] == selection.plan_release_id
            and payload_by_field["items"][0]["custom_import"] is not None
        )
        assert reply.headers["Cache-Control"] == "private, no-store"
    assert legacy.service_legacy_selection_sha256() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes", [{"market_type": "group"}, {"plan_release_id": "canonical", "plan_market_type": "group"}]
)
async def test_market_options_without_legacy_identity_keep_existing_routing(monkeypatch, changes):
    session, _, resolver = _install(monkeypatch)
    args = _args(**changes)
    with _scope():
        async with legacy.legacy_service_selection(session, args) as normalized:
            assert normalized is args and legacy.service_legacy_selection_sha256() is None
    session.execute.assert_not_awaited()
    resolver.assert_not_awaited()


def test_non_ein_identifier_never_gains_digit_or_name_aliases():
    assert legacy._plan_ids("99-0000001") == ["99-0000001", "990000001"]
    assert legacy._plan_ids("990000001") == ["990000001", "99-0000001"]
    assert legacy._plan_ids("label990000001") == ["label990000001"]
    assert legacy._plan_ids("Synthetic Plan") == ["Synthetic Plan"]


def _type_document(selection, kinds=("ein", "hios")):
    import hashlib

    body_by_field = {
        "contract": legacy._TYPE_CONTRACT,
        **{
            name: getattr(selection, name)
            for name in (
                "healthporta_plan_id",
                "plan_version_id",
                "plan_release_id",
                "serving_revision_id",
                "binding_set_digest",
            )
        },
        "bindings": [
            legacy._type_entry(binding) | {"plan_id_type": kind, "source_namespace": "synthetic" if kind else None}
            for binding, kind in zip(selection.bindings, kinds, strict=True)
        ],
    }
    return body_by_field | {
        "sha256": hashlib.sha256(
            json.dumps(body_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
    }


def _typed_install(monkeypatch, kinds=("ein", "hios")):
    import datetime

    from process.ptg_parts.plan_rows import _ptg2_plan_rows

    session, selection, resolver = _install(monkeypatch)
    document = _type_document(selection, kinds)
    session.scalar = AsyncMock(return_value=json.dumps(document))

    async def fetch(_query, encoded):
        rows = []
        for entry in json.loads(encoded):
            plan, _, _ = _ptg2_plan_rows(
                {
                    "plan_id": entry["plan_id"],
                    "plan_id_type": entry["plan_id_type"].upper(),
                    "plan_market_type": entry["plan_market_type"],
                },
                entry["snapshot_id"],
                datetime.date(2026, 7, 1),
            )
            rows.append(
                entry
                | {
                    "snapshot_status": "published",
                    "snapshot_source": entry["source_key"],
                    "witnessed_type": plan["plan_id_type"],
                    "plan_hash": plan["plan_hash"],
                    "canonical_payload": plan["canonical_payload"],
                }
            )
        encoded_rows = json.dumps(rows)
        return {"count": len(rows), "bytes": len(encoded_rows.encode()), "rows_json": encoded_rows}

    driver = SimpleNamespace(fetchrow=AsyncMock(side_effect=fetch))
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session.connection = AsyncMock(return_value=connection)
    return session, selection, resolver, document, driver


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,ordinal", [("ein", 9), ("hios", 27)])
@pytest.mark.parametrize("selected", [False, True])
async def test_typed_commitment_uses_explicit_namespace_and_original_role_ordinal(monkeypatch, kind, ordinal, selected):
    session, selection, _, _, driver = _typed_install(monkeypatch)
    with _scope(selected=selected):
        async with legacy.legacy_service_selection(session, _args(plan_id="99-0000001", plan_id_type=kind)):
            assert tuple(
                binding.binding_ordinal for binding in legacy.restrict_service_bindings(selection, selection.bindings)
            ) == (ordinal,)
    assert driver.fetchrow.await_count == 1
    assert len(selection.bindings) == 2 and legacy.service_legacy_selection_sha256() is None


@pytest.mark.asyncio
async def test_hios_numeric_shape_never_receives_ein_punctuation_alias(monkeypatch):
    session, _, _, _, driver = _typed_install(monkeypatch)
    with _scope(), pytest.raises(PlanPricingProjectionUnavailable):
        async with legacy.legacy_service_selection(session, _args(plan_id="990000001", plan_id_type="hios")):
            pytest.fail("HIOS received an EIN alias")
    driver.fetchrow.assert_not_awaited()
    assert legacy._plan_ids("990000001", "hios") == ["990000001"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["absent", "digest", "revision", "coordinates", "duplicate", "namespace", "unknown"])
async def test_typed_commitment_damage_refuses_before_retained_metadata(monkeypatch, damage):
    session, _, _, document, driver = _typed_install(monkeypatch)
    if damage == "absent":
        document = None
    if damage == "digest":
        document["sha256"] = "f" * 64
    if damage == "revision":
        document["serving_revision_id"] = "other"
    if damage == "coordinates":
        document["bindings"][0]["snapshot_id"] = "foreign"
    if damage == "duplicate":
        document["bindings"][1] = dict(document["bindings"][0])
    if damage == "namespace":
        document["bindings"][0]["source_namespace"] = None
    if damage == "unknown":
        document["bindings"][0]["plan_id_type"] = None
        document["bindings"][0]["source_namespace"] = None
    if document is not None and damage != "digest":
        import hashlib

        body_by_field = {name: entry for name, entry in document.items() if name != "sha256"}
        document["sha256"] = hashlib.sha256(
            json.dumps(body_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
    session.scalar.return_value = json.dumps(document)
    with _scope(), pytest.raises(PlanPricingProjectionUnavailable):
        async with legacy.legacy_service_selection(session, _args(plan_id_type="ein", source_key="source-allowed")):
            pytest.fail("damaged commitment was admitted")
    driver.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_typed_cursor_scope_includes_producer_type_commitment(monkeypatch):
    session, selection, _, document, _ = _typed_install(monkeypatch)
    args = _args(plan_id_type="ein", source_key="source-allowed")
    with _scope():
        async with legacy.legacy_service_selection(session, args):
            original = legacy.service_legacy_selection_sha256()
        document["bindings"][0]["source_namespace"] = "other_explicit_source"
        body_by_field = {name: entry for name, entry in document.items() if name != "sha256"}
        import hashlib

        document["sha256"] = hashlib.sha256(
            json.dumps(body_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        session.scalar.return_value = json.dumps(document)
        async with legacy.legacy_service_selection(session, args):
            assert legacy.service_legacy_selection_sha256() != original
    assert len(selection.bindings) == 2
