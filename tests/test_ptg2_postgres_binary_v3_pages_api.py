# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving
from api.ptg2_db_serving_v3_pages import PTG2V3PageRecord, PTG2V3ProviderPage


PROVIDER_ID = "00000000000000000000000000000003"
PRICE_ID = "00000000000000000000000000000011"


class ScalarResult:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value

    def scalar(self):
        return self.scalar_value


class RecordingSession:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return ScalarResult(self.scalar_value)


def serving_tables():
    return ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        serving_binary_table="mrf.ptg2_serving_binary_v3",
        code_count_table="mrf.ptg2_code_count_v3",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dictionary_v3",
    )


def reverse_query(limit=2):
    return ptg2_serving._VersionThreeReverseQuery(
        provider_set_ids=(PROVIDER_ID,),
        requested_plan="plan",
        code_value="",
        code_system=None,
        q_text="",
        code_context=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=limit,
        offset=0,
        apply_window=True,
    )


@pytest.mark.asyncio
async def test_forward_page_materializes_without_full_code_lookup(monkeypatch):
    page_entries = (
        PTG2V3PageRecord(code_key=7, provider_set_key=3, provider_count=9, price_key=10),
    )
    monkeypatch.setattr(ptg2_serving, "lookup_code_page_from_db", AsyncMock(return_value=page_entries))
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_ids_for_keys",
        AsyncMock(return_value={3: PROVIDER_ID}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_price_ids_from_db",
        AsyncMock(return_value={10: PRICE_ID}),
    )

    response_rows = await ptg2_serving._version_three_forward_page_rows(
        object(),
        serving_tables(),
        code_metadata={
            "code_key": 7,
            "plan_id": "plan",
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "rate_count": 1,
        },
        source_trace_set_hash=None,
        network_names=[],
        limit=25,
        offset=0,
    )

    assert len(response_rows) == 1
    assert response_rows[0]["provider_set_global_id_128"] == PROVIDER_ID
    assert response_rows[0]["price_set_global_id_128"] == PRICE_ID
    assert response_rows[0]["price_key"] == 10


@pytest.mark.asyncio
async def test_forward_page_resolves_only_requested_window(monkeypatch):
    page_entries = tuple(
        PTG2V3PageRecord(
            code_key=7,
            provider_set_key=provider_set_key,
            provider_count=100 - provider_set_key,
            price_key=provider_set_key + 100,
        )
        for provider_set_key in range(1, 65)
    )
    provider_lookup = AsyncMock(return_value={1: PROVIDER_ID})
    price_lookup = AsyncMock(return_value={101: PRICE_ID})
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_code_page_from_db",
        AsyncMock(return_value=page_entries),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_ids_for_keys",
        provider_lookup,
    )
    monkeypatch.setattr(ptg2_serving, "lookup_price_ids_from_db", price_lookup)

    response_rows = await ptg2_serving._version_three_forward_page_rows(
        object(),
        serving_tables(),
        code_metadata={"code_key": 7, "rate_count": 100},
        source_trace_set_hash=None,
        network_names=[],
        limit=1,
        offset=0,
    )

    assert len(response_rows) == 1
    assert provider_lookup.await_args.args[2] == {1}
    assert price_lookup.await_args.args[2] == {101}


@pytest.mark.asyncio
async def test_forward_page_rejects_underfilled_projection(monkeypatch):
    page_entries = tuple(
        PTG2V3PageRecord(7, provider_set_key, 1, provider_set_key)
        for provider_set_key in range(1, 64)
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_code_page_from_db",
        AsyncMock(return_value=page_entries),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="invalid row count"):
        await ptg2_serving._version_three_forward_page_rows(
            object(),
            serving_tables(),
            code_metadata={"code_key": 7, "rate_count": 100},
            source_trace_set_hash=None,
            network_names=[],
            limit=1,
            offset=0,
        )


def install_reverse_page_stubs(monkeypatch, *, metadata_rows):
    page_record = PTG2V3PageRecord(
        code_key=7,
        provider_set_key=3,
        provider_count=9,
        price_key=10,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "has_provider_pages_in_db",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_has_single_plan_page_order",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_keys_for_ids",
        AsyncMock(return_value={PROVIDER_ID: 3}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_provider_pages_from_db",
        AsyncMock(return_value={3: PTG2V3ProviderPage((page_record,), 100)}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_code_rows_for_provider_reverse",
        AsyncMock(return_value=metadata_rows),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_price_ids_from_db",
        AsyncMock(return_value={10: PRICE_ID}),
    )


@pytest.mark.asyncio
async def test_reverse_page_avoids_full_forward_blocks(monkeypatch):
    install_reverse_page_stubs(
        monkeypatch,
        metadata_rows=[
            {
                "code_key": 7,
                "plan_id": "plan",
                "reported_code_system": "CPT",
                "reported_code": "99213",
            }
        ],
    )
    full_scope = AsyncMock(side_effect=AssertionError("full reverse scope should not run"))
    monkeypatch.setattr(ptg2_serving, "_version_three_reverse_scope", full_scope)

    rows = await ptg2_serving._version_three_reverse_rows(
        object(),
        serving_tables(),
        reverse_query(limit=1),
    )

    assert len(rows) == 1
    assert rows[0]["provider_set_global_id_128"] == PROVIDER_ID
    assert rows[0]["price_set_global_id_128"] == PRICE_ID
    full_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_reverse_page_rejects_projected_code_missing_from_metadata(monkeypatch):
    install_reverse_page_stubs(monkeypatch, metadata_rows=[])
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_price_ids_from_db",
        AsyncMock(return_value={}),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="unknown code"):
        await ptg2_serving._version_three_reverse_page_rows(
            object(),
            serving_tables(),
            reverse_query(limit=1),
        )


@pytest.mark.asyncio
async def test_reverse_page_falls_back_when_truncated_prefix_is_too_short(monkeypatch):
    install_reverse_page_stubs(
        monkeypatch,
        metadata_rows=[
            {
                "code_key": 7,
                "plan_id": "plan",
                "reported_code_system": "CPT",
                "reported_code": "99213",
            }
        ],
    )

    response_rows = await ptg2_serving._version_three_reverse_page_rows(
        object(),
        serving_tables(),
        reverse_query(limit=2),
    )

    assert response_rows is None


@pytest.mark.asyncio
async def test_reverse_page_falls_back_when_snapshot_contains_mixed_plan_order(monkeypatch):
    install_reverse_page_stubs(monkeypatch, metadata_rows=[])
    plan_order = AsyncMock(return_value=False)
    monkeypatch.setattr(
        ptg2_serving,
        "_has_single_plan_page_order",
        plan_order,
    )
    provider_lookup = AsyncMock(side_effect=AssertionError("mixed-plan page must not be read"))
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_keys_for_ids",
        provider_lookup,
    )

    rows = await ptg2_serving._version_three_reverse_page_rows(
        object(),
        serving_tables(),
        reverse_query(limit=1),
    )

    assert rows is None
    plan_order.assert_awaited_once()
    provider_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_old_snapshot_skips_page_plan_and_provider_queries(monkeypatch):
    availability = AsyncMock(return_value=False)
    plan_order = AsyncMock(side_effect=AssertionError("old snapshot must skip plan aggregate"))
    provider_lookup = AsyncMock(side_effect=AssertionError("old snapshot must skip provider lookup"))
    monkeypatch.setattr(ptg2_serving, "has_provider_pages_in_db", availability)
    monkeypatch.setattr(ptg2_serving, "_has_single_plan_page_order", plan_order)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_keys_for_ids",
        provider_lookup,
    )

    response_rows = await ptg2_serving._version_three_reverse_page_rows(
        object(),
        serving_tables(),
        reverse_query(limit=1),
    )

    assert response_rows is None
    availability.assert_awaited_once()
    plan_order.assert_not_awaited()
    provider_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_plan_order_proof_distinguishes_null_and_empty_plan_ids():
    session = RecordingSession(True)

    assert await ptg2_serving._has_single_plan_page_order(
        session,
        serving_tables(),
        "",
    )
    statement, params = session.calls[0]
    assert "BOOL_AND(plan_id IS NULL)" in statement
    assert "BOOL_AND(COALESCE(plan_id = '', FALSE))" in statement
    assert "BOOL_AND(COALESCE(plan_id = :page_plan_id, FALSE))" in statement
    assert params == {"page_plan_id": ""}


@pytest.mark.asyncio
async def test_reverse_page_caps_provider_set_fanout(monkeypatch):
    availability = AsyncMock(return_value=True)
    plan_order = AsyncMock(side_effect=AssertionError("oversized page must skip plan aggregate"))
    provider_lookup = AsyncMock(side_effect=AssertionError("oversized page must skip provider lookup"))
    monkeypatch.setattr(ptg2_serving, "has_provider_pages_in_db", availability)
    monkeypatch.setattr(ptg2_serving, "_has_single_plan_page_order", plan_order)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_keys_for_ids",
        provider_lookup,
    )
    oversized_query = replace(
        reverse_query(limit=1),
        provider_set_ids=tuple(f"{provider_index + 1:032x}" for provider_index in range(65)),
    )

    page_scope = await ptg2_serving._version_three_page_projection_scope(
        object(),
        serving_tables(),
        oversized_query,
    )

    assert page_scope is None
    availability.assert_awaited_once()
    plan_order.assert_not_awaited()
    provider_lookup.assert_not_awaited()
