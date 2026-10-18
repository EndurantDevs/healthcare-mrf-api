# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Insurance filters must follow the active address and client policy scope."""

import json
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import provider_list_sql
from api.endpoint import npi
from api.network_address_scope import network_address_read_scope, scoped_address_insurance_predicate
from process.network_serving_read import PinnedNetworkServingManifest


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["count", "classification", "page"])
@pytest.mark.parametrize("mode", ["legacy", "default", "selected"])
async def test_real_list_branches_use_same_insurance_scope_before_count_and_limit(monkeypatch, kind, mode):
    """Check membership predicates before every real count and page branch."""
    statements = []

    _record_insurance_statements(monkeypatch, statements)
    manifest = PinnedNetworkServingManifest(
        1,
        "00000000-0000-0000-0000-000000000001",
        "network_candidate_00000000000000000000000000000001",
        1,
        {},
        8,
        "c" * 64,
        1,
    )
    context = nullcontext()
    if mode == "default":
        context = network_address_read_scope(manifest, None, excluded_network_ids=(88,), access_scope_sha256="a" * 64)
    elif mode == "selected":
        context = network_address_read_scope(manifest, (42,), access_scope_sha256="a" * 64)
    request = SimpleNamespace(
        args={
            "has_insurance": "1",
            "city": "Synthetic city",
            "npi": "1111111111",
            "limit": "1",
            "include_total": "0",
            "count_only": "0" if kind == "page" else "1",
            "format": "all" if kind == "classification" else "",
        },
        ctx=SimpleNamespace(sa_session=SimpleNamespace(in_transaction=lambda: True)),
        app=SimpleNamespace(),
    )
    with context:
        predicate = scoped_address_insurance_predicate()
        document = json.loads((await npi.get_all(request)).body)
    assert document["rows"] == ([] if kind == "page" else {"Pharmacy": 1} if kind == "classification" else 1)
    assert statements
    sql, parameters = statements[0]
    assert predicate in sql
    if mode == "legacy":
        assert "NOT (plans_network_array @@ '0'::query_int)" in sql
        assert "_network_approved_revision" not in parameters
    else:
        assert "plans_network_array @@" not in sql
        if mode == "default":
            assert "row_origin IN" not in predicate
            assert "policy_network.record_json->'archived'='false'::jsonb" in sql
            assert parameters["_network_approved_revision"] == 8 and parameters["_network_excluded_ids"] == [88]
        else:
            assert "canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[])" in sql
            assert parameters["_canonical_network_ids"] == [42]
    if kind == "page":
        assert sql.index(predicate) < sql.index("LIMIT :limit OFFSET :start")


def _record_insurance_statements(monkeypatch, statements):
    """Record actual list SQL on the request-owned synthetic connection."""

    async def query(statement, **parameters):
        sql = str(statement)
        statements.append((sql, parameters))
        if "COUNT(DISTINCT ft.npi)" in sql:
            return [("Pharmacy", 1)]
        if "COUNT(DISTINCT" in sql:
            return [(1,)]
        return []

    connection = SimpleNamespace(all=query)

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(provider_list_sql, "ConnectionProxy", Mock(return_value=connection))
    monkeypatch.setattr(npi, "db", SimpleNamespace(acquire=acquire))
    monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value="mrf.entity_address_unified"))
    monkeypatch.setattr(npi, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
