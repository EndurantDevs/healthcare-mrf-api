# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed import queries retain exact canonical membership before provider paging."""

import copy
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import custom_import_provider_batch as batch
from api import custom_import_provider_http as providers
from api import custom_import_provider_network as network
from api import custom_import_read_http as transport
from api import provider_batch as native_batch
from api.network_address_scope import current_network_address_scope, scoped_address_relation_sql
from api.provider_list_sql import _provider_import_count_page
from process.custom_import.read_contracts import CustomImportReadRequestError
from process.network_serving_read import NetworkServingReadUnavailable, PinnedNetworkServingManifest
from tests import test_custom_import_provider_batch as batch_fixtures
from tests import test_custom_import_provider_http as page_fixtures
from tests.network_read_execution_support import _ReceiptDriver, mock_approved_control_metadata
from tests.openapi_route_contract_support import load_openapi_document

_MANIFEST = PinnedNetworkServingManifest(
    7,
    "00000000-0000-0000-0000-000000000001",
    "network_candidate_00000000000000000000000000000001",
    1,
    {},
    0,
    "c" * 64,
    1,
)


def _bound_document(*, batch_request=False, **changes):
    document = json.loads(batch_fixtures._body(**changes) if batch_request else page_fixtures._body(**changes))
    document["network_binding"] = {
        "version": 2,
        "client_id": "synthetic-client",
        "policy_revision": 3,
        "generation_id": 7,
        "manifest_sha256": "c" * 64,
        "selector": {"namespace": "canonical", "values": [42, 88]},
        "network_ids": [42, 88],
        "query_sha256": hashlib.sha256(transport._canonical_json_bytes(document)).hexdigest(),
    }
    return document


def _body(document):
    return transport._canonical_json_bytes(document)


class _ScopedSession(page_fixtures._Session):
    def __init__(self):
        super().__init__()
        self._transaction = object()
        self.driver = _ReceiptDriver(self, None)

    def in_transaction(self):
        return self.already_active or "begin" in self.events and "end" not in self.events

    def get_transaction(self):
        return self._transaction if self.in_transaction() else None

    async def connection(self):
        self.events.append("connection")
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.driver))
        )

    async def execute(self, statement, parameters=None):
        if str(statement).startswith("LOCK TABLE"):
            assert str(statement) == f'LOCK TABLE "{_MANIFEST.schema_name}".entity_address_unified IN ACCESS SHARE MODE'
            self.events.append("heap-lock")
        else:
            await super().execute(statement)


def _install_manifest(monkeypatch, session, *, failure=None, manifest=_MANIFEST):
    mock_approved_control_metadata(monkeypatch, events=session.events)

    async def resolve(driver, *, generation_id, api_read):
        assert api_read is True
        assert driver is session.driver and generation_id == 7
        assert session.events[:2] == ["begin", "snapshot"]
        session.events.append("manifest")
        if failure is not None:
            raise failure
        return manifest

    monkeypatch.setattr(network, "resolve_network_serving_manifest", resolve)


def _assert_scope():
    scope = current_network_address_scope()
    assert scope.manifest is _MANIFEST and scope.network_ids == (42, 88)
    assert len(scope.access_scope_sha256) == 64


def test_binding_is_opt_in_and_commits_the_complete_page():
    document = _bound_document()
    with pytest.raises(transport.CustomImportReadTransportError):
        providers._parse_provider_request(_body(document))
    parsed = providers._parse_provider_request(_body(document), allow_network_binding=True)
    assert parsed.network_binding.generation_id == 7 and parsed.network_binding.network_ids == (42, 88)
    document["native_query"]["page"] = "2"
    with pytest.raises(CustomImportReadRequestError, match="binding"):
        providers._parse_provider_request(_body(document), allow_network_binding=True)


@pytest.mark.parametrize("binding", [None, {}, []])
def test_present_invalid_binding_never_becomes_an_unscoped_request(binding):
    document = _bound_document()
    document["network_binding"] = binding
    with pytest.raises(CustomImportReadRequestError, match="binding"):
        providers._parse_provider_request(_body(document), allow_network_binding=True)


@pytest.mark.parametrize("selector", ["network_ids", "network_generation"])
def test_native_query_cannot_smuggle_a_canonical_selector_without_trusted_binding(selector):
    with pytest.raises(CustomImportReadRequestError, match="native provider query"):
        providers._parse_provider_request(
            page_fixtures._body(native_query={selector: "42"}), allow_network_binding=True
        )


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("include_filter", False),
        ("context", []),
        ("filters", [{"field_id": "metric", "operator": "gt", "value": 3}]),
        ("order", [{"field_id": "metric", "direction": "asc"}]),
    ],
)
def test_binding_hash_covers_inclusion_context_filters_and_order(field, replacement):
    document = _bound_document()
    document[field] = replacement
    with pytest.raises(CustomImportReadRequestError, match="binding"):
        providers._parse_provider_request(_body(document), allow_network_binding=True)


def test_same_membership_retains_distinct_authority_cache_identity():
    document = _bound_document()
    first = network.parse_provider_network_binding(document)
    document["network_binding"]["policy_revision"] = 4
    second = network.parse_provider_network_binding(document)
    assert first.network_ids == second.network_ids and first.access_scope_sha256 != second.access_scope_sha256


def test_provider_network_binding_openapi_contract():
    document = load_openapi_document(Path("doc/openapi.yaml"))
    schemas = document["components"]["schemas"]
    binding = schemas["CustomImportProviderNetworkBinding"]
    assert binding["additionalProperties"] is False
    assert set(binding["required"]) == network._BINDING_FIELDS
    assert binding["properties"]["network_ids"]["maxItems"] == 50
    assert binding["properties"]["legacy_checksums"]["maxItems"] == 50
    assert "legacy_checksums" not in binding["required"]
    for name in ("CustomImportProviderRequest", "CustomImportProviderBatchRequest"):
        assert schemas[name]["properties"]["network_binding"] == {
            "$ref": "#/components/schemas/CustomImportProviderNetworkBinding"
        }
    for path in ("/extensions/custom-import/providers", "/extensions/custom-import/providers/batch"):
        assert "X-Network-Generation" in document["paths"][path]["post"]["responses"]["200"]["headers"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("version", 1),
        ("client_id", ""),
        ("client_id", " synthetic"),
        ("client_id", "a\x00b"),
        ("policy_revision", True),
        ("policy_revision", -1),
        ("generation_id", 0),
        ("generation_id", True),
        ("generation_id", 9223372036854775808),
        ("manifest_sha256", "C" * 64),
        ("query_sha256", "a" * 64),
        ("unexpected", 1),
        ("network_ids", []),
        ("network_ids", [88, 42]),
        ("network_ids", [42, 42]),
        ("network_ids", [True]),
        ("network_ids", [-1]),
        ("network_ids", list(range(1, 52))),
        ("selector", {"namespace": "legacy_checksum", "values": [42, 88]}),
        ("selector", {"namespace": "canonical", "values": [42, 99]}),
        ("selector", {"namespace": "canonical", "values": [42, 88], "extra": True}),
    ],
)
def test_binding_rejects_noncanonical_or_unbound_authority(field, value):
    document = _bound_document()
    document["network_binding"][field] = value
    with pytest.raises(CustomImportReadRequestError, match="binding"):
        providers._parse_provider_request(_body(document), allow_network_binding=True)


@pytest.mark.parametrize("legacy", ["plan_network", "plan_release_id"])
def test_binding_cannot_mix_canonical_membership_with_legacy_plan_scope(legacy):
    document = _bound_document(native_query={legacy: "synthetic"})
    with pytest.raises(CustomImportReadRequestError, match="binding"):
        providers._parse_provider_request(_body(document), allow_network_binding=True)


def test_batch_binding_commits_npi_order_and_shape_in_addition_to_query():
    document = _bound_document(batch_request=True)
    assert batch._parse_batch_request(_body(document)).provider.network_binding.network_ids == (42, 88)
    for field, replacement in [("npis", ["9000000001", "9000000000"]), ("address_limit", 0)]:
        changed = copy.deepcopy(document)
        changed["native_batch"][field] = replacement
        with pytest.raises(CustomImportReadRequestError, match="binding"):
            batch._parse_batch_request(_body(changed))


@pytest.mark.asyncio
@pytest.mark.parametrize("include_filter", [False, True])
async def test_signed_page_pins_scope_before_import_preparation_and_keeps_inclusion_independent(
    monkeypatch, include_filter
):
    session = _ScopedSession()
    page_fixtures._install(monkeypatch, session, page_rows=[{"npi": 1104212877}])
    _install_manifest(monkeypatch, session)
    reader = providers._read_provider_payload

    async def read(*args):
        _assert_scope()
        payload = await reader(*args)
        _assert_scope()
        return payload

    monkeypatch.setattr(providers, "_read_provider_payload", read)
    reply = await providers.serve_custom_import_providers(
        page_fixtures._request(_body(_bound_document(include_filter=include_filter))),
        session,
    )
    assert reply.status == 200 and reply.headers["X-Network-Generation"] == "7"
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert ("hydrate" in session.events) is include_filter
    assert ("custom_import" in json.loads(reply.body)["rows"][0]) is include_filter
    assert "heap-lock" not in session.events
    assert session.events.index("approved-pin") < session.events.index("prepare") < session.events.index("page")
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("include_filter", [False, True])
async def test_signed_batch_pins_scope_for_native_eligibility_and_optional_hydration(monkeypatch, include_filter):
    session = _ScopedSession()
    prepared, hydrated = batch_fixtures._install(monkeypatch, session)
    _install_manifest(monkeypatch, session)
    reader = batch._read_batch_payload

    async def read(*args):
        _assert_scope()
        return await reader(*args)

    monkeypatch.setattr(batch, "_read_batch_payload", read)
    reply = await batch.serve_custom_import_provider_batch(
        batch_fixtures._request(_body(_bound_document(batch_request=True, include_filter=include_filter))),
        session,
    )
    assert reply.status == 200 and reply.headers["X-Network-Generation"] == "7"
    assert len(prepared) == 1 and bool(hydrated) is include_filter
    assert all(("custom_import" in item["provider"]) is include_filter for item in json.loads(reply.body)["items"])
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["manifest", "digest", "generation", "native"])
async def test_unavailable_binding_or_native_failure_never_falls_back_and_cleans_scope(monkeypatch, failure):
    session = _ScopedSession()
    page_fixtures._install(
        monkeypatch, session, page_failure=RuntimeError("synthetic native refusal") if failure == "native" else None
    )
    manifest = replace(_MANIFEST, manifest_sha256="d" * 64) if failure == "digest" else _MANIFEST
    if failure == "generation":
        manifest = replace(manifest, generation_id=8)
    _install_manifest(
        monkeypatch,
        session,
        manifest=manifest,
        failure=NetworkServingReadUnavailable("synthetic unavailable") if failure == "manifest" else None,
    )
    reply = await providers.serve_custom_import_providers(page_fixtures._request(_body(_bound_document())), session)
    assert reply.status == 503 and reply.headers["Cache-Control"] == "private, no-store"
    assert "X-Network-Generation" not in reply.headers and session.rolled_back
    assert ("page" in session.events) is (failure == "native")
    assert current_network_address_scope() is None


@pytest.mark.asyncio
async def test_forged_body_binding_is_rejected_before_manifest_or_connection(monkeypatch):
    session = _ScopedSession()
    page_fixtures._install(monkeypatch, session)
    document = _bound_document()
    request = page_fixtures._request(_body(document))
    document["network_binding"]["policy_revision"] = 4
    request.body = _body(document)
    reply = await providers.serve_custom_import_providers(request, session)
    assert reply.status == 404 and session.events == []


@pytest.mark.asyncio
async def test_scoped_matching_relation_is_shared_by_exact_total_and_ordered_page(monkeypatch):
    session = _ScopedSession()
    session.events.extend(["begin", "snapshot"])
    _install_manifest(monkeypatch, session)
    binding = network.parse_provider_network_binding(_bound_document())
    context = SimpleNamespace(
        prepared=SimpleNamespace(normalized_order_terms=[SimpleNamespace(direction="desc")]), require_match=True
    )
    async with network.provider_network_read_scope(session, binding):
        eligible = "SELECT DISTINCT npi FROM " + scoped_address_relation_sql("mrf.entity_address_unified")
        ctes, page = _provider_import_count_page(eligible, context)
        assert _MANIFEST.schema_name in ctes and "canonical_network_ids &&" in ctes
        assert "WHERE imported.entity_value IS NOT NULL" in ctes
        assert "COUNT(*) AS _provider_total FROM matching_npis" in ctes
        assert "FROM matching_npis AS imported ORDER BY" in page and "LIMIT :limit OFFSET :start" in page
        assert "sort_0 DESC NULLS LAST, imported.npi ASC" in page
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("import_order", [False, True])
async def test_canonical_batch_membership_precedes_import_order_and_page_without_false_404(monkeypatch, import_order):
    session = _ScopedSession()
    session.events.extend(["begin", "snapshot"])
    _install_manifest(monkeypatch, session)
    requested_npis = [9000000000, 9000000001, 9000000002]
    options_by_field = {
        "npis": requested_npis,
        "address_limit": 0,
        "address_offset": 0,
        "include_sources": False,
        "include_evidence": False,
    }
    context = SimpleNamespace(prepared=SimpleNamespace(normalized_order_terms=["synthetic"] if import_order else []))

    async def prepare(npis, args, same_session, *, import_context):
        _assert_scope()
        assert npis == requested_npis and same_session is session and import_context is context
        return native_batch._NativeBatchState(
            dict.fromkeys(requested_npis, {"synthetic": True}),
            {requested_npis[0]: [{}], requested_npis[1]: [], requested_npis[2]: [{}]},
        )

    async def eligible(import_context, found, same_session):
        _assert_scope()
        assert import_context is context and same_session is session and found == [requested_npis[0], requested_npis[2]]
        return list(reversed(found))

    async def hydrate(npis, state, options, args, same_session, **kwargs):
        _assert_scope()
        assert npis == [requested_npis[2] if import_order else requested_npis[0]]
        return [{"npi": identity, "status": 200, "provider": {"npi": identity}} for identity in npis]

    monkeypatch.setattr(native_batch, "_prepare_native_batch", prepare)
    monkeypatch.setattr(native_batch, "_imported_batch_npis", eligible)
    monkeypatch.setattr(native_batch, "_hydrate_native_batch", hydrate)
    async with network.provider_network_read_scope(session, network.parse_provider_network_binding(_bound_document())):
        batch_payload = await native_batch.read_native_batch(
            SimpleNamespace(),
            options_by_field,
            native_args=native_batch.parse_native_batch_query({"limit": "1"}),
            session=session,
            import_context=context,
        )
    assert batch_payload["found"] == 2 and batch_payload["not_found"] == 1
    assert batch_payload["pagination"]["total"] == 2 and batch_payload["pagination"]["has_more"] is True
    assert [(provider_item["npi"], provider_item["status"]) for provider_item in batch_payload["items"]] == [
        (requested_npis[2] if import_order else requested_npis[0], 200),
        (requested_npis[1], 404),
    ]
    assert current_network_address_scope() is None


def test_signed_legacy_subset_changes_authority_identity_without_changing_canonical_ids():
    """The complete signed binding includes the original checksum namespace."""
    document = _bound_document()
    canonical = network.parse_provider_network_binding(document)
    document["network_binding"]["legacy_checksums"] = [-17, 42]
    legacy = network.parse_provider_network_binding(document)
    assert legacy.legacy_checksums == (-17, 42)
    assert legacy.network_ids == canonical.network_ids
    assert legacy.access_scope_sha256 != canonical.access_scope_sha256


@pytest.mark.parametrize(
    "checksums", [None, [], (), [True], ["42"], [42, 42], [42, -17], [-2147483649], [2147483648], list(range(51))]
)
def test_signed_legacy_subset_rejects_untyped_or_noncanonical_values(checksums):
    """Presence never falls back to the wider canonical-only contract."""
    document = _bound_document()
    document["network_binding"]["legacy_checksums"] = checksums
    with pytest.raises(CustomImportReadRequestError, match="binding"):
        network.parse_provider_network_binding(document)


@pytest.mark.asyncio
@pytest.mark.parametrize("checksums", [(-2147483648, 0, 2147483647), (-17,), (42,)])
async def test_signed_legacy_subset_is_resolved_pinned_enforced_and_acknowledged(monkeypatch, checksums):
    """The acknowledgement comes from the enforced context after actual mapping."""
    session = _ScopedSession()
    page_fixtures._install(monkeypatch, session, page_rows=[{"npi": 1104212877}])
    session.driver.delegate = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"complete": True, "network_ids": [42, 88]})
    )
    _install_manifest(monkeypatch, session)
    original_reader = providers._read_provider_payload

    async def read(*arguments):
        _assert_scope()
        scope = current_network_address_scope()
        assert scope.legacy_checksums == checksums
        assert "plans_network_array" in str(scope.address_relation)
        assert "canonical_network_ids &&" not in str(scope.address_relation)
        return await original_reader(*arguments)

    monkeypatch.setattr(providers, "_read_provider_payload", read)
    document = _bound_document()
    document["network_binding"]["legacy_checksums"] = list(checksums)
    reply = await providers.serve_custom_import_providers(page_fixtures._request(_body(document)), session)
    assert reply.status == 200
    assert reply.headers["X-Network-Legacy-Checksums"] == ",".join(map(str, checksums))
    assert session.driver.delegate.fetchrow.await_args.args[1:] == (
        0,
        tuple(map(str, checksums)),
        "checksum_network",
        (),
    )
    assert current_network_address_scope() is None
    assert "X-Network-Legacy-Checksums" not in network.provider_network_response({}, _MANIFEST).headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mapping",
    [
        None,
        {"complete": False, "network_ids": [42, 88]},
        {"complete": True, "network_ids": [42]},
        {"complete": True, "network_ids": [88, 42]},
    ],
)
async def test_signed_legacy_subset_refuses_missing_or_different_pinned_mapping(monkeypatch, mapping):
    """Never prepare, read or acknowledge a partially resolved legacy subset."""
    session = _ScopedSession()
    page_fixtures._install(monkeypatch, session)
    session.driver.delegate = SimpleNamespace(fetchrow=AsyncMock(return_value=mapping))
    _install_manifest(monkeypatch, session)
    document = _bound_document()
    document["network_binding"]["legacy_checksums"] = [42]
    reply = await providers.serve_custom_import_providers(page_fixtures._request(_body(document)), session)
    assert reply.status == 503 and "prepare" not in session.events
    assert "X-Network-Legacy-Checksums" not in reply.headers
    assert current_network_address_scope() is None and session.rolled_back


@pytest.mark.asyncio
async def test_changing_signed_checksum_subset_fails_before_database_access(monkeypatch):
    """A correct unsigned-body hash cannot replace the full transport signature."""
    session = _ScopedSession()
    page_fixtures._install(monkeypatch, session)
    document = _bound_document()
    document["network_binding"]["legacy_checksums"] = [42]
    request = page_fixtures._request(_body(document))
    document["network_binding"]["legacy_checksums"] = [-17]
    request.body = _body(document)
    reply = await providers.serve_custom_import_providers(request, session)
    assert reply.status == 404 and session.events == []
