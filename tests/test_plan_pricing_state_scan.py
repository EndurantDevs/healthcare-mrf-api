# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed-work cursor traversal for release-bound statewide pricing."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from api import plan_pricing_state_scan as scan
from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
    BillingSearchCursorKeyring,
)
from api.endpoint.pagination import PaginationParams
from api.plan_pricing_projection_contract import (
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
)


PROJECTION_ID = "a" * 64
RELEASE_ID = "hprelease_01J00000000000000000000000"


def _binding(
    ordinal: int = 0,
    snapshot_id: str = "snapshot-1",
):
    return SimpleNamespace(
        binding_ordinal=ordinal,
        snapshot_id=snapshot_id,
        source_key="source-1",
        plan_id="plan-1",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


def _selection(*, contract: str = "plan_pricing_factorized_v4", **overrides):
    binding = _binding()
    selection_by_field = dict(
        plan_release_id=RELEASE_ID,
        serving_revision_id="hpserve_01J00000000000000000000000",
        binding_set_digest="b" * 64,
        pricing_projection_id=PROJECTION_ID,
        pricing_projection_contract=contract,
        in_network_bindings=(binding,),
        serving_tables_for_snapshot=lambda _snapshot_id: SimpleNamespace(),
    )
    selection_by_field.update(overrides)
    selection = SimpleNamespace(**selection_by_field)
    selection.response_metadata = lambda: {
        "resolved_snapshot_ids": sorted(
            {
                str(binding.snapshot_id)
                for binding in selection.in_network_bindings
            }
        )
    }
    return selection


def _args(**overrides):
    return {
        "plan_release_id": RELEASE_ID,
        "code_system": "CPT",
        "code": "93320",
        "state": "MI",
        "order_by": "npi",
        "order": "asc",
        "view": "full",
        "include_providers": "true",
        "include_allowed_amounts": "false",
        **overrides,
    }


class _Values:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _Values(self._rows)

    def mappings(self):
        return _Values(self._rows)


class _Session:
    def __init__(self, state_npis, occurrence_rows):
        state_rows = [
            state_row
            if isinstance(state_row, dict)
            else {
                "npi": state_row,
                "provider_fragment": f"fragment-{state_row}".encode(),
            }
            for state_row in state_npis
        ]
        self.results = [_Result(state_rows), _Result(occurrence_rows)]
        self.calls = []

    async def execute(self, statement, parameters):
        self.calls.append((str(statement), parameters))
        return self.results.pop(0)


def _keyring():
    return BillingSearchCursorKeyring(
        active_key_id="test",
        keys_by_id={"test": bytes(range(32))},
    )


def _occurrence(npi: int, *, multiplicity: int = 1):
    return {
        "npi": npi,
        "binding_ordinal": 0,
        "occurrence_ordinal": 0,
        "provider_set_key": 7,
        "provider_set_ref": "1" * 32,
        "price_key": 9,
        "price_set_ref": "2" * 32,
        "rate_pack_ref": "3" * 32,
        "source_artifact_key": 11,
        "provider_count": 2,
        "group_fragment": {
            "reported_code_system": "CPT",
            "reported_code": "93320",
        },
        "occurrence_multiplicity": multiplicity,
    }


@pytest.mark.asyncio
async def test_state_scan_pages_by_npi_and_mints_bound_cursor(monkeypatch):
    session = _Session(
        [1000000001, 1000000002, 1000000003],
        [_occurrence(1000000001)],
    )
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(scan.time, "time", lambda: 2_000_000_000)
    hydrate = AsyncMock(return_value=[{"npi": 1000000001, "rate_options": [{"rate": 10}]}])
    monkeypatch.setattr(scan, "_hydrate_selected_groups", hydrate)

    page_response = await scan.search_plan_pricing_state_scan(
        session,
        _selection(),
        _args(),
        PaginationParams(page=1, limit=2, offset=0, source="page"),
    )

    assert page_response["items"] == [{"npi": 1000000001, "rate_options": [{"rate": 10}]}]
    assert page_response["pagination"]["has_more"] is True
    assert page_response["pagination"]["total_is_exact"] is False
    assert page_response["pagination"]["total_lower_bound"] == 1
    assert page_response["pagination"]["scanned_npi_count"] == 2
    assert page_response["resolved_snapshot_ids"] == ["snapshot-1"]
    assert page_response["query"]["resolved_snapshot_ids"] == ["snapshot-1"]
    assert page_response["pagination"]["next_cursor"].startswith("bsc1_test_")
    assert len(page_response["pagination"]["next_cursor"]) <= 2048
    assert len(session.calls) == 2
    assert session.calls[0][1]["after_npi"] == 0
    assert "npi > :after_npi" in session.calls[0][0]
    assert "provider_fragment" in session.calls[0][0]
    assert "LIMIT :npi_sentinel_limit" in session.calls[0][0]
    assert "COUNT(" not in session.calls[0][0].upper()
    assert session.calls[1][1]["selected_npis"] == [1000000001, 1000000002]
    hydrate.assert_awaited_once_with(
        session,
        ANY,
        ANY,
        [_occurrence(1000000001)],
        {
            1000000001: b"fragment-1000000001",
            1000000002: b"fragment-1000000002",
        },
    )


@pytest.mark.asyncio
async def test_state_scan_queries_only_taxonomy_eligible_npis(monkeypatch):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(
        scan,
        "_eligible_provider_npis",
        lambda _fragments, _args_by_name: (1000000002,),
    )
    hydrate = AsyncMock(return_value=[{"npi": 1000000002}])
    monkeypatch.setattr(scan, "_hydrate_selected_groups", hydrate)
    session = _Session(
        [1000000001, 1000000002],
        [_occurrence(1000000002)],
    )

    response_document = await scan.search_plan_pricing_state_scan(
        session,
        _selection(),
        _args(),
        PaginationParams(page=1, limit=2, offset=0, source="page"),
    )

    assert session.calls[1][1]["selected_npis"] == [1000000002]
    assert response_document["pagination"]["scanned_npi_count"] == 2
    hydrate.assert_awaited_once_with(
        session,
        ANY,
        ANY,
        [_occurrence(1000000002)],
        {1000000002: b"fragment-1000000002"},
    )


@pytest.mark.asyncio
async def test_state_scan_refuses_complete_group_overflow(monkeypatch):
    monkeypatch.setattr(scan, "STATE_SCAN_RATE_OCCURRENCE_LIMIT", 1)
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    session = _Session([1000000001], [_occurrence(1000000001, multiplicity=2)])

    with pytest.raises(scan.PlanPricingStateScanBudgetExceeded):
        await scan.search_plan_pricing_state_scan(
            session,
            _selection(),
            _args(),
            PaginationParams(page=1, limit=1, offset=0, source="page"),
        )


@pytest.mark.asyncio
async def test_state_scan_refuses_provider_membership_overflow(monkeypatch):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    session = _Session(
        [1000000001],
        [{"membership_budget_exceeded": True}],
    )

    with pytest.raises(
        scan.PlanPricingStateScanBudgetExceeded,
        match="provider-membership",
    ):
        await scan.search_plan_pricing_state_scan(
            session,
            _selection(),
            _args(),
            PaginationParams(page=1, limit=1, offset=0, source="page"),
        )


@pytest.mark.asyncio
async def test_state_scan_refuses_oversized_complete_response(monkeypatch):
    monkeypatch.setattr(scan, "STATE_SCAN_RESPONSE_BYTE_LIMIT", 1)
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(
        scan,
        "_hydrate_selected_groups",
        AsyncMock(return_value=[{"npi": 1000000001}]),
    )

    with pytest.raises(scan.PlanPricingStateScanBudgetExceeded):
        await scan.search_plan_pricing_state_scan(
            _Session([1000000001], [_occurrence(1000000001)]),
            _selection(),
            _args(),
            PaginationParams(page=1, limit=1, offset=0, source="page"),
        )


@pytest.mark.asyncio
async def test_state_scan_empty_page_can_advance_cursor(monkeypatch):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(scan.time, "time", lambda: 2_000_000_000)
    monkeypatch.setattr(scan, "_hydrate_selected_groups", AsyncMock(return_value=[]))
    payload = await scan.search_plan_pricing_state_scan(
        _Session([1000000001, 1000000002], []),
        _selection(),
        _args(),
        PaginationParams(page=1, limit=1, offset=0, source="page"),
    )

    assert payload["items"] == []
    assert payload["pagination"]["has_more"] is True
    assert payload["pagination"]["scanned_npi_count"] == 1
    assert payload["pagination"]["next_cursor"].startswith("bsc1_test_")


@pytest.mark.asyncio
async def test_state_scan_cursor_is_monotonic_repeat_safe_and_accepts_offset_zero(
    monkeypatch,
):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(scan.time, "time", lambda: 2_000_000_000)
    monkeypatch.setattr(
        scan,
        "_hydrate_selected_groups",
        AsyncMock(
            side_effect=lambda _s, _selection, _args, occurrence_rows, _fragments: (
                [{"npi": int(occurrence_rows[0]["npi"])}]
                if occurrence_rows
                else []
            )
        ),
    )
    first_page_response = await scan.search_plan_pricing_state_scan(
        _Session([1000000001, 1000000002], [_occurrence(1000000001)]),
        _selection(),
        _args(),
        PaginationParams(page=1, limit=1, offset=0, source="page"),
    )
    cursor = first_page_response["pagination"]["next_cursor"]
    continuation_args = _args(cursor=cursor, offset="0")
    repeated_payloads = []
    repeated_sessions = []
    for _ in range(2):
        session = _Session([1000000002], [_occurrence(1000000002)])
        repeated_sessions.append(session)
        repeated_payloads.append(
            await scan.search_plan_pricing_state_scan(
                session,
                _selection(),
                continuation_args,
                PaginationParams(page=1, limit=1, offset=0, source="offset"),
            )
        )

    assert [page_response["items"] for page_response in repeated_payloads] == [
        [{"npi": 1000000002}],
        [{"npi": 1000000002}],
    ]
    assert all(session.calls[0][1]["after_npi"] == 1000000001 for session in repeated_sessions)
    assert all(
        page_response["pagination"]["scanned_npi_count"] == 2
        for page_response in repeated_payloads
    )


@pytest.mark.asyncio
async def test_state_scan_cursor_is_bound_to_query_release_snapshot_and_serving(
    monkeypatch,
):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(scan.time, "time", lambda: 2_000_000_000)
    monkeypatch.setattr(scan, "_hydrate_selected_groups", AsyncMock(return_value=[]))
    first = await scan.search_plan_pricing_state_scan(
        _Session([1000000001, 1000000002], []),
        _selection(),
        _args(),
        PaginationParams(page=1, limit=1, offset=0, source="page"),
    )
    cursor_args = _args(cursor=first["pagination"]["next_cursor"])
    pagination = PaginationParams(page=1, limit=1, offset=0, source="page")

    with pytest.raises(BillingSearchCursorError):
        await scan.search_plan_pricing_state_scan(
            _Session([], []),
            _selection(),
            {**cursor_args, "code": "27130"},
            pagination,
        )
    with pytest.raises(BillingSearchCursorGenerationExpired):
        await scan.search_plan_pricing_state_scan(
            _Session([], []),
            _selection(serving_revision_id="hpserve_01J11111111111111111111111"),
            cursor_args,
            pagination,
        )
    changed_binding = SimpleNamespace(**{**vars(_binding()), "snapshot_id": "snapshot-2"})
    with pytest.raises(BillingSearchCursorGenerationExpired):
        await scan.search_plan_pricing_state_scan(
            _Session([], []),
            _selection(in_network_bindings=(changed_binding,)),
            cursor_args,
            pagination,
        )
    changed_release = RELEASE_ID[:-1] + "1"
    with pytest.raises(BillingSearchCursorError):
        await scan.search_plan_pricing_state_scan(
            _Session([], []),
            _selection(plan_release_id=changed_release),
            {**cursor_args, "plan_release_id": changed_release},
            pagination,
        )


def test_state_scan_sql_has_only_keyset_and_bounded_sentinels():
    sql = scan._page_sql()
    assert "UNNEST(CAST(:selected_npis AS bigint[]))" in sql
    assert sql.count("CROSS JOIN LATERAL") == 2
    assert "LIMIT :membership_probe_limit" in sql
    assert "LIMIT :membership_sentinel_limit" in sql
    assert "LIMIT :occurrence_probe_limit" in sql
    assert "LIMIT :occurrence_sentinel_limit" in sql
    assert "LIMIT :page_row_limit" in sql
    assert "COUNT(" not in sql.upper()
    assert "ORDER BY" not in sql.upper()


def test_state_scan_requires_closed_release_shape_and_v4():
    assert scan.is_plan_pricing_state_scan(_args())
    assert not scan.is_plan_pricing_state_scan(_args(order_by="distance"))
    assert scan.validate_plan_pricing_state_scan(
        _args(include_unverified_addresses="true")
    ) == ("CPT", "93320", "MI")
    assert scan.validate_plan_pricing_state_scan(
        _args(include_unverified_addresses=None)
    ) == ("CPT", "93320", "MI")
    with pytest.raises(PlanPricingProjectionUnsupported):
        scan.validate_plan_pricing_state_scan(_args(city="detroit"))
    with pytest.raises(PlanPricingProjectionUnsupported):
        scan.validate_plan_pricing_state_scan(_args(plan_id="not-release-bound"))
    with pytest.raises(
        PlanPricingProjectionUnsupported,
        match="include_allowed_amounts=false",
    ):
        scan.validate_plan_pricing_state_scan(
            _args(include_allowed_amounts=None)
        )
    with pytest.raises(
        PlanPricingProjectionUnsupported,
        match="include_allowed_amounts=false",
    ):
        scan.validate_plan_pricing_state_scan(
            _args(include_allowed_amounts="true")
        )


def test_state_scan_cursor_binds_normalized_unverified_address_default():
    omitted = _args()
    omitted.pop("include_unverified_addresses", None)
    omitted_fingerprint = scan._request_fingerprint(
        omitted, "CPT", "93320", "MI", 25
    )

    assert omitted_fingerprint == scan._request_fingerprint(
        _args(include_unverified_addresses="true"),
        "CPT",
        "93320",
        "MI",
        25,
    )
    assert omitted_fingerprint != scan._request_fingerprint(
        _args(include_unverified_addresses="false"),
        "CPT",
        "93320",
        "MI",
        25,
    )


@pytest.mark.asyncio
async def test_state_scan_rejects_v3_and_cursor_offset(monkeypatch):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    pagination = PaginationParams(page=1, limit=1, offset=0, source="page")
    v3_session = _Session([], [])
    with pytest.raises(PlanPricingProjectionUnavailable):
        await scan.search_plan_pricing_state_scan(
            v3_session,
            _selection(contract="plan_pricing_factorized_v3"),
            _args(),
            pagination,
        )
    assert v3_session.calls == []
    offset_session = _Session([], [])
    with pytest.raises(PlanPricingProjectionUnsupported, match="offset 0"):
        await scan.search_plan_pricing_state_scan(
            offset_session,
            _selection(),
            _args(cursor="bsc1_test_invalid", offset="1"),
            PaginationParams(page=1, limit=1, offset=1, source="offset"),
        )
    assert offset_session.calls == []
