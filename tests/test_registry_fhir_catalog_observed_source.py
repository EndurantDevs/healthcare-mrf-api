# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Malformed reader and physical evidence fail before catalog acceptance."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from process import registry_fhir_catalog_observed_source as observed
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile


def _profile():
    return FHIRCatalogSourceReadProfile("source_payload", "source_owner", ("source_reader",), "source_catalog")


def _actor(**changes):
    return {
        "login": "source_reader",
        "actor": "source_reader",
        "reader_oid": 101,
        "owner_oid": 102,
        "database_oid": 103,
        "rolcanlogin": True,
        "readonly": "on",
        "isolation": "repeatable read",
        "replication_role": "origin",
        "safe_search_path": True,
        "no_dangerous_role": True,
        "admitted_role": True,
        "runtime_role_count": 1,
        "observed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        **changes,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"actor": "changed-role"},
        {"rolcanlogin": False},
        {"readonly": "off"},
        {"isolation": "read committed"},
        {"replication_role": "replica"},
        {"owner_oid": None},
        {"safe_search_path": False},
        {"no_dangerous_role": False},
        {"admitted_role": False},
        {"runtime_role_count": 0},
        {"observed_at": None},
        {"observed_at": datetime(2026, 1, 1)},
    ],
)
async def test_unsafe_reader_never_reads_source_relations(changes):
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchrow=AsyncMock(return_value=_actor(**changes)),
        execute=AsyncMock(),
        fetch=AsyncMock(),
    )
    with pytest.raises(observed.FHIRCatalogObservedSourceError, match="reader_unsafe"):
        await observed.collect_fhir_catalog_observation(
            connection, _profile(), endpoint_id="endpoint", dataset_id="edition"
        )
    connection.execute.assert_not_awaited()
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_collection_requires_caller_owned_transaction():
    connection = Mock(is_in_transaction=Mock(return_value=False), fetchrow=AsyncMock())
    with pytest.raises(observed.FHIRCatalogObservedSourceError, match="readonly_snapshot_required"):
        await observed.collect_fhir_catalog_observation(
            connection, _profile(), endpoint_id="endpoint", dataset_id="edition"
        )
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("records", [[], [{"document": None}] * 4, [{"document": "{}"}] * 1001])
async def test_native_catalog_evidence_has_row_and_byte_bounds(records, monkeypatch):
    monkeypatch.setattr(observed, "_pin_registration_catalog", AsyncMock())
    connection = MagicMock(
        is_in_transaction=Mock(return_value=True),
        fetchrow=AsyncMock(return_value=_actor()),
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=records),
    )
    with pytest.raises(observed.FHIRCatalogObservedSourceError, match="catalog_bounds"):
        await observed.collect_fhir_catalog_observation(
            connection, _profile(), endpoint_id="endpoint", dataset_id="edition"
        )
    query = connection.fetch.await_args.args[0]
    assert "LIMIT 1001" in query and "octet_length(document)" in query
    assert "ACCESS SHARE MODE NOWAIT" in connection.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_native_catalog_must_include_all_exact_requested_roots(monkeypatch):
    monkeypatch.setattr(observed, "_pin_registration_catalog", AsyncMock())
    catalog_rows = [
        {
            "document": json.dumps(
                {"oid": oid, "nspname": "source_payload", "relname": f"unexpected_{oid}", "parents": []}
            )
        }
        for oid in range(4)
    ]
    connection = MagicMock(
        is_in_transaction=Mock(return_value=True),
        fetchrow=AsyncMock(return_value=_actor()),
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=catalog_rows),
    )
    with pytest.raises(observed.FHIRCatalogObservedSourceError, match="physical_identity_invalid"):
        await observed.collect_fhir_catalog_observation(
            connection, _profile(), endpoint_id="endpoint", dataset_id="edition"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "physical_match,rows,expected_error", [(False, 1, "resource_identity_changed"), (True, 2, "resource_count_changed")]
)
async def test_actual_stream_counts_and_physical_tableoids_must_agree(
    monkeypatch, physical_match, rows, expected_error
):
    connection = Mock(
        fetch=AsyncMock(
            return_value=[{"resource_type": "Organization", "rows": rows, "physical_match": physical_match}]
        )
    )

    async def pages(*_args, **_keywords):
        yield (
            {
                "resource_type": "Organization",
                "resource_id": "network",
                "payload_hash": "a" * 64,
                "payload_json": {"name": "Example"},
            },
        )

    monkeypatch.setattr(observed, "iter_fhir_catalog_pages", pages)
    with pytest.raises(observed.FHIRCatalogObservedSourceError, match=expected_error):
        await observed._facts(
            connection, _profile(), "edition", {"resource_leaf_oids": [104]}, "https://example.test/fhir"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes", [{"profile": {}}, {"require_current": 1}, {"endpoint_id": " endpoint "}, {"dataset_id": "x" * 97}]
)
async def test_invalid_caller_scope_cannot_reach_database(changes):
    connection = Mock(fetchrow=AsyncMock())
    arguments_by_name = {"profile": _profile(), "endpoint_id": "endpoint", "dataset_id": "edition", **changes}
    with pytest.raises(ValueError):
        await observed.collect_fhir_catalog_observation(connection, **arguments_by_name)
    connection.fetchrow.assert_not_awaited()
