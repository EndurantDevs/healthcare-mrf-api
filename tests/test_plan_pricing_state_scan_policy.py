# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""State scans admit original source ordinals before bounded work and bind real cursors."""

from contextlib import contextmanager, nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import literal, select

from api import plan_pricing_state_scan as scan
from api.billing_search_cursor import BillingSearchCursorError, BillingSearchCursorGenerationExpired
from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_PATH
from api.custom_import_provider_service_sql import ProviderServiceImportQuery
from api.endpoint import pricing
from api.endpoint.pagination import PaginationParams
from api.network_address_scope import current_network_address_scope, network_address_read_scope
from api.plan_pricing_projection_contract import PROJECTION_CONTRACT, PlanPricingProjectionUnavailable
from api.plan_release_serving import PlanReleaseSnapshotBinding
from process.custom_import.read_core import PreparedNpiEntityRelation
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.test_plan_pricing_state_scan import _args, _keyring, _occurrence, _selection, _Session
from tests.test_plan_release_serving import _release_selection
from tests.test_pricing_api import make_request
from tests.test_provider_service_source_policy import _fixture

NOW = 2_000_000_000
FIXED_PATH = "/api/v1/registry/serving/providers/collection"


def _pin(**changes):
    candidate = "11111111-1111-4111-8111-111111111111"
    return replace(
        PinnedNetworkServingManifest(
            17, candidate, "network_candidate_" + candidate.replace("-", ""), 1, {}, 23, "b" * 64, 99
        ),
        **changes,
    )


@contextmanager
def _scope(*, pin=None, digest="a" * 64, selected=False):
    with network_address_read_scope(
        pin or _pin(),
        (7,) if selected else None,
        access_scope_sha256=digest,
        excluded_network_ids=None if selected else (7,),
        organization_policy=RegistryOrganizationVisibility(),
    ) as scope:
        yield scope


def _install_cursor(monkeypatch):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(scan.time, "time", lambda: NOW)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_public_binding", [False, True])
@pytest.mark.parametrize("has_provider_page", [False, True])
async def test_source_admission_precedes_bounded_work_and_metadata(monkeypatch, has_public_binding, has_provider_page):
    held = _fixture(monkeypatch, excluded=(7,))
    public = replace(
        held.binding,
        binding_ordinal=27,
        plan_id="unreviewed-plan",
        snapshot_id="snapshot-public",
        source_key="public-source",
    )
    if has_public_binding:
        held.batches[0].append(
            {
                "snapshot_id": public.snapshot_id,
                "status": "published",
                "manifest": {"serving_index": {"source_key": public.source_key}, "activation": {"state": "activated"}},
            }
        )
    bindings = (held.binding, public) if has_public_binding else (held.binding,)
    selection = replace(
        _release_selection(*bindings), pricing_projection_id="c" * 64, pricing_projection_contract=PROJECTION_CONTRACT
    )
    before = selection.bindings, selection.binding_set_digest
    reader = _Session(
        [1000000001] if has_provider_page else [],
        [_occurrence(1000000001) | {"binding_ordinal": 27}] if has_provider_page else [],
    )

    async def execute(statement, parameters):
        assert held.driver.fetchrow.await_count == 3
        assert parameters["admitted_binding_ordinals"] == [27]
        return await reader.execute(statement, parameters)

    held.session.execute = execute
    _install_cursor(monkeypatch)
    hydrate = AsyncMock(return_value=[{"npi": 1000000001}])
    monkeypatch.setattr(scan, "_hydrate_selected_groups", hydrate)
    with _scope(pin=held.scope.manifest):
        reply = await scan.search_plan_pricing_state_scan(
            held.session,
            selection,
            _args(plan_release_id=selection.plan_release_id),
            PaginationParams(page=1, limit=1, offset=0, source="page"),
        )
    assert (selection.bindings, selection.binding_set_digest) == before
    expected_snapshots = [public.snapshot_id] if has_public_binding else []
    assert reply["in_network_snapshot_ids"] == reply["resolved_snapshot_ids"] == expected_snapshots
    assert reply["query"]["resolved_snapshot_ids"] == expected_snapshots
    assert reply["binding_set_digest"] == selection.binding_set_digest
    assert reply["items"] == ([{"npi": 1000000001}] if has_public_binding and has_provider_page else [])
    expected_queries = (2 if has_provider_page else 1) if has_public_binding else 0
    assert len(reader.calls) == expected_queries
    if has_public_binding and has_provider_page:
        assert hydrate.await_args.args[0] is held.session
        assert hydrate.await_args.args[1].bindings is selection.bindings
        assert hydrate.await_args.args[1].binding_set_digest == selection.binding_set_digest
        assert hydrate.await_args.args[3][0]["binding_ordinal"] == 27
    else:
        hydrate.assert_not_awaited()


def test_managed_sql_admits_ordinals_before_all_limits():
    provider_sql = scan._provider_page_sql((27,))
    occurrence_sql = scan._page_sql((27,))
    assert "EXISTS" in provider_sql and "permitted.npi = provider_state.npi" in provider_sql
    assert provider_sql.index("permitted.binding_ordinal = ANY") < provider_sql.index("LIMIT :npi_sentinel_limit")
    assert occurrence_sql.index("candidate.binding_ordinal = ANY") < occurrence_sql.index(
        "LIMIT :membership_probe_limit"
    )
    assert occurrence_sql.index("candidate.binding_ordinal = ANY") < occurrence_sql.index(
        "LIMIT :membership_sentinel_limit"
    )
    assert "candidate.binding_ordinal = membership.binding_ordinal" in occurrence_sql
    assert "admitted_binding_ordinals" not in scan._provider_page_sql()
    assert "admitted_binding_ordinals" not in scan._page_sql()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["policy", "generation", "revision", "manifest", "expired", "unscoped"])
async def test_real_cursor_refuses_changed_authority_before_source_or_provider_sql(monkeypatch, change):
    _install_cursor(monkeypatch)
    selection, args = _selection(), _args()
    with _scope():
        scope = scan._cursor_scope(selection, args, "CPT", "93320", "MI", 1)
        token = scan._seal_position((1000000001, 1, 0, 1), keyring=_keyring(), trusted_now=NOW, scope=scope)
    pin = _pin(
        **(
            {"generation_id": 18}
            if change == "generation"
            else {"approved_custom_revision": 24}
            if change == "revision"
            else {"manifest_sha256": "c" * 64}
            if change == "manifest"
            else {}
        )
    )
    admission = AsyncMock()
    monkeypatch.setattr(scan, "admitted_plan_release_bindings", admission)
    if change == "expired":
        monkeypatch.setattr(scan.time, "time", lambda: NOW + scan.BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS)
    session = _Session([], [])
    manager = (
        nullcontext() if change == "unscoped" else _scope(pin=pin, digest="d" * 64 if change == "policy" else "a" * 64)
    )
    expected = (
        BillingSearchCursorGenerationExpired
        if change in {"generation", "revision", "manifest"}
        else BillingSearchCursorError
    )
    with manager, pytest.raises(expected):
        await scan.search_plan_pricing_state_scan(
            session, selection, args | {"cursor": token}, PaginationParams(page=1, limit=1, offset=0, source="page")
        )
    admission.assert_not_awaited()
    assert not session.calls


@pytest.mark.asyncio
async def test_retained_cursor_replay_keeps_original_release_and_idle_ttl(monkeypatch):
    _install_cursor(monkeypatch)
    selection, args = _selection(), _args()
    visible = PlanReleaseSnapshotBinding(**vars(selection.in_network_bindings[0]))
    denied = replace(visible, binding_ordinal=5, snapshot_id="snapshot-denied")
    selection = replace(
        _release_selection(denied, visible),
        plan_release_id=args["plan_release_id"],
        pricing_projection_id=selection.pricing_projection_id,
        pricing_projection_contract=selection.pricing_projection_contract,
    )
    admission = AsyncMock(return_value=(visible,))
    monkeypatch.setattr(scan, "admitted_plan_release_bindings", admission)
    with _scope():
        scope = scan._cursor_scope(selection, args, "CPT", "93320", "MI", 1)
        token = scan._seal_position((1000000001, 1, 0, 1), keyring=_keyring(), trusted_now=NOW, scope=scope)
        opened = scan.open_billing_search_cursor(
            token,
            keyring=_keyring(),
            trusted_now=NOW,
            request_fingerprint_sha256=scope[0],
            authorization_context_sha256=scope[1],
            generation_bundle_sha256=scope[2],
            snapshot_set_sha256=scope[3],
        )
        assert opened.expires_at - opened.issued_at == 900
        for _ in range(2):
            session = _Session([1000000002, 1000000003], [])
            reply = await scan.search_plan_pricing_state_scan(
                session, selection, args | {"cursor": token}, PaginationParams(page=1, limit=1, offset=0, source="page")
            )
            assert session.calls[0][1]["after_npi"] == 1000000001
            assert reply["pagination"]["scanned_npi_count"] == 2
            assert reply["pagination"]["has_more"]
            assert reply["in_network_snapshot_ids"] == reply["query"]["resolved_snapshot_ids"] == [visible.snapshot_id]
            assert selection.in_network_bindings == (denied, visible)
            admission.assert_awaited_with(session, selection)
    assert admission.await_count == 2


@pytest.mark.asyncio
async def test_unscoped_scan_never_reads_source_admission(monkeypatch):
    _install_cursor(monkeypatch)
    admission = AsyncMock(side_effect=AssertionError("unexpected policy read"))
    monkeypatch.setattr(scan, "admitted_plan_release_bindings", admission)
    session = _Session([], [])
    await scan.search_plan_pricing_state_scan(
        session, _selection(), _args(), PaginationParams(page=1, limit=1, offset=0, source="page")
    )
    admission.assert_not_awaited()
    assert "admitted_binding_ordinals" not in session.calls[0][1]


def test_unscoped_cursor_digests_retain_original_vector():
    assert scan._cursor_scope(_selection(), _args(), "CPT", "93320", "MI", 1) == (
        "8832b25aade65f8fa6bc550ce548389f3d1e20229e31b56e8bf2e8ff48241e60",
        "963ff07c6103d3c33ad566ad2588a5b2b3ea13e5f625e6d15bb09227b535b12f",
        "322b404721a9e5defdac3491371f5d002b0d6745308fccc960a56303e31a1234",
        "b1039906cdda862ff9aac786031010bb59905990b6cbb362fd18aef22e4391d1",
    )


@pytest.mark.asyncio
async def test_public_cursor_cannot_replay_in_managed_scope(monkeypatch):
    _install_cursor(monkeypatch)
    selection, args = _selection(), _args()
    scope = scan._cursor_scope(selection, args, "CPT", "93320", "MI", 1)
    token = scan._seal_position((1000000001, 1, 0, 1), keyring=_keyring(), trusted_now=NOW, scope=scope)
    admission = AsyncMock()
    monkeypatch.setattr(scan, "admitted_plan_release_bindings", admission)
    session = _Session([], [])
    with _scope(), pytest.raises(BillingSearchCursorError):
        await scan.search_plan_pricing_state_scan(
            session, selection, args | {"cursor": token}, PaginationParams(page=1, limit=1, offset=0, source="page")
        )
    admission.assert_not_awaited()
    assert not session.calls


@pytest.mark.asyncio
async def test_selected_scope_refuses_before_source_or_provider_sql(monkeypatch):
    _install_cursor(monkeypatch)
    admission = AsyncMock()
    monkeypatch.setattr(scan, "admitted_plan_release_bindings", admission)
    session = _Session([], [])
    with _scope(selected=True), pytest.raises(PlanPricingProjectionUnavailable):
        await scan.search_plan_pricing_state_scan(
            session, _selection(), _args(), PaginationParams(page=1, limit=1, offset=0, source="page")
        )
    admission.assert_not_awaited()
    assert not session.calls


@pytest.mark.asyncio
async def test_source_admission_failure_is_terminal_before_provider_sql(monkeypatch):
    _install_cursor(monkeypatch)
    admission = AsyncMock(side_effect=PlanPricingProjectionUnavailable("source evidence unavailable"))
    monkeypatch.setattr(scan, "admitted_plan_release_bindings", admission)
    session = _Session([], [])
    selection = _selection()
    with _scope(), pytest.raises(PlanPricingProjectionUnavailable):
        await scan.search_plan_pricing_state_scan(
            session, selection, _args(), PaginationParams(page=1, limit=1, offset=0, source="page")
        )
    admission.assert_awaited_once_with(session, selection)
    assert not session.calls


@pytest.mark.parametrize(
    "path,method,explicit,scope_kind,allowed",
    [
        (BILLING_SEARCH_TRANSPORT_PATH, "GET", False, None, True),
        (BILLING_SEARCH_TRANSPORT_PATH, "POST", False, None, True),
        (FIXED_PATH, "POST", True, "default", True),
        (FIXED_PATH, "GET", True, "default", False),
        (FIXED_PATH, "POST", False, "default", False),
        (FIXED_PATH, "POST", True, "selected", False),
        (FIXED_PATH, "POST", True, None, False),
        ("/api/v1/pricing/providers/by-service", "GET", True, "default", False),
        ("/api/v1/extensions/custom-import/providers/by-service", "POST", True, "default", False),
    ],
)
def test_state_scan_transport_requires_fixed_trusted_context(path, method, explicit, scope_kind, allowed):
    manager = _scope(selected=scope_kind == "selected") if scope_kind else nullcontext()
    with manager:
        request = SimpleNamespace(path=path, method=method)
        if allowed:
            scan.validate_state_scan_transport(request, {} if explicit else None)
        else:
            with pytest.raises(InvalidUsage, match="canonical"):
                scan.validate_state_scan_transport(request, {} if explicit else None)


@pytest.mark.asyncio
async def test_actual_pricing_native_transport_reaches_scan_without_legacy_fallback(monkeypatch):
    selection = _selection()
    resolver = AsyncMock(return_value=selection)
    reader = AsyncMock(return_value={"items": [], "pagination": {}, "query": {}})
    legacy = AsyncMock()
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", resolver)
    monkeypatch.setattr(pricing, "search_plan_pricing_state_scan", reader)
    monkeypatch.setattr(pricing, "search_current_ptg2_index", legacy)
    monkeypatch.setattr(
        pricing,
        "_resolve_ptg_specialty_or_raise",
        AsyncMock(return_value=SimpleNamespace(unresolved_specialty=None, taxonomy_codes=())),
    )
    request = make_request([])
    request.path, request.method = FIXED_PATH, "POST"
    native_args = RequestParameters({name: [entry] for name, entry in _args().items()})
    with _scope():
        reply = await pricing.list_providers_by_procedure(request, native_args=native_args)
    assert reply.status == 200
    reader.assert_awaited_once()
    assert reader.await_args.args[0] is request.ctx.sa_session and reader.await_args.args[1] is selection
    resolver.assert_awaited_once()
    legacy.assert_not_awaited()
    assert not request.ctx.sa_session.executions and current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [FIXED_PATH, "/api/v1/extensions/custom-import/providers/by-service"])
async def test_imported_service_still_refuses_state_scan_before_resolution(monkeypatch, path):
    resolver = AsyncMock()
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", resolver)
    prepared = PreparedNpiEntityRelation(select(literal("1000000001").label("entity_value")), (), "a" * 64, "b" * 64)
    import_context = ProviderServiceImportQuery(prepared, True)
    request = make_request([])
    request.path, request.method = path, "POST"
    native_args = RequestParameters({name: [entry] for name, entry in _args().items()})
    with _scope(), pytest.raises(InvalidUsage, match="claims lane"):
        await pricing.list_providers_by_procedure(request, native_args=native_args, import_context=import_context)
    resolver.assert_not_awaited()
    assert not request.ctx.sa_session.executions


def _scan_release_rows(bindings):
    from tests.test_plan_release_serving import _binding_row

    return [
        _binding_row(
            **vars(binding),
            expected_binding_count=len(bindings),
            pricing_projection_id="c" * 64,
            pricing_projection_contract=PROJECTION_CONTRACT,
        )
        for binding in bindings
    ]


def _install_scan_readiness(monkeypatch, held, binding, *, ready):
    from api import plan_release_readiness as readiness
    from tests.test_ptg2_price_hydration_cache import _serving_tables

    descriptor = _serving_tables(
        snapshot_id=binding.snapshot_id,
        source_key=binding.source_key,
        plan_id=binding.plan_id,
        plan_market_type=binding.plan_market_type,
        storage_generation="shared_blocks_v4",
        shared_block_layout="packed_snapshot_maps_v4",
    )
    calls = []
    held.scan_order = []

    async def snapshot(_session, **selectors):
        assert held.driver.fetchrow.await_count == 3
        assert selectors == {
            "requested_snapshot_id": binding.snapshot_id,
            "requested_source_key": binding.source_key,
            "requested_plan_id": binding.plan_id,
            "requested_plan_market_type": binding.plan_market_type,
        }
        calls.append(binding.snapshot_id)
        held.scan_order.append(("prepare", binding.snapshot_id))
        return binding.snapshot_id if ready else None

    async def tables(_session, snapshot_id):
        assert snapshot_id == binding.snapshot_id
        return descriptor

    monkeypatch.setattr(readiness, "current_snapshot_id", snapshot)
    monkeypatch.setattr(readiness, "snapshot_serving_tables", tables)
    return descriptor, calls


def _scan_reader(held, bindings, rows):
    from tests.test_plan_pricing_state_scan import _Result
    from tests.test_plan_pricing_state_scan_hydration import _address, _provider_fragment

    provider = _provider_fragment(1000000001, _address("1 Example Street", "Ann Arbor", "48104", "location-1"))
    held.scan_calls = []

    async def execute(statement, parameters=None):
        sql = str(statement)
        if "to_regclass" in sql:
            return SimpleNamespace(scalar_one=lambda: True)
        if "pin_owner_type" in (parameters or {}):
            return _scan_release_rows(bindings)
        assert parameters["admitted_binding_ordinals"] == [27]
        held.scan_order.append(("provider_sql", tuple(parameters["admitted_binding_ordinals"])))
        held.scan_calls.append((sql, parameters))
        if "npi_sentinel_limit" in parameters:
            return _Result([{"npi": 1000000001, "provider_fragment": provider}])
        return _Result(rows)

    held.session.execute = execute


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [False, True])
async def test_metadata_release_real_hydration(monkeypatch, ready):
    from api import plan_release_serving as releases
    from api import ptg2_price_hydration_cache as prices
    from api import ptg2_serving as serving

    held = _fixture(monkeypatch, excluded=(7,))
    public = replace(held.binding, binding_ordinal=27, snapshot_id="snapshot-public", source_key="public-source")
    bindings = (held.binding, public)
    held.batches[0].append(
        {
            "snapshot_id": public.snapshot_id,
            "status": "published",
            "manifest": {"serving_index": {"source_key": public.source_key}, "activation": {"state": "activated"}},
        }
    )
    descriptor, readiness_calls = _install_scan_readiness(monkeypatch, held, public, ready=ready)
    occurrence = _occurrence(1000000001) | {"binding_ordinal": 27}
    _scan_reader(held, bindings, [occurrence])
    cache = prices.PriceHydrationCache(1024 * 1024)
    cache.admit_many(serving._version_three_price_cache_layout(descriptor), {9: [{"negotiated_rate": "10.00"}]})
    monkeypatch.setattr(serving, "PRICE_HYDRATION_CACHE", cache)
    _install_cursor(monkeypatch)
    with _scope(pin=held.scope.manifest):
        selection = await releases.resolve_plan_release_serving(held.session, _release_selection().plan_release_id)
        assert selection is not None and selection._validated_serving_tables == () and readiness_calls == []
        original = selection.bindings, selection.binding_set_digest
        args = _args(plan_release_id=selection.plan_release_id)
        fingerprint = scan._cursor_scope(selection, args, "CPT", "93320", "MI", 1)
        if not ready:
            with pytest.raises(PlanPricingProjectionUnavailable, match="admitted release binding is unavailable"):
                await scan.search_plan_pricing_state_scan(
                    held.session, selection, args, PaginationParams(page=1, limit=1, offset=0, source="page")
                )
            assert held.scan_calls == []
        else:
            reply = await scan.search_plan_pricing_state_scan(
                held.session, selection, args, PaginationParams(page=1, limit=1, offset=0, source="page")
            )
            assert reply["items"][0]["prices"][0]["negotiated_rate"] == 10
            assert reply["items"][0]["address"]["first_line"] == "1 Example Street"
            assert reply["resolved_snapshot_ids"] == [public.snapshot_id]
            assert len(held.scan_calls) == 2
            assert held.scan_order[:2] == [("prepare", public.snapshot_id), ("provider_sql", (27,))]
        assert (selection.bindings, selection.binding_set_digest) == original
        assert selection._validated_serving_tables == ()
        assert scan._cursor_scope(selection, args, "CPT", "93320", "MI", 1) == fingerprint
        assert readiness_calls == [public.snapshot_id]
