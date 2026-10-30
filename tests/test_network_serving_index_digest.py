# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog deparsing keeps exact index checks independent of caller lookup settings."""

import asyncio

import asyncpg
import pytest

from process.network_membership_serving_indexes import NetworkServingIndexesError, _index_digest


class _IndexConnection:
    def __init__(self, path, *, failure=None, restore_failure=None):
        self.path = path
        self.failure = failure
        self.restore_failure = restore_failure
        self.events = []
        self.records = [
            dict(
                index_oid=101,
                relname="candidate_npi_idx",
                indrelid=100,
                table_oid=100,
                indisvalid=True,
                indisready=True,
                indislive=True,
                index_definition="CREATE INDEX candidate_npi_idx ON synthetic_candidate.entity_address_unified USING btree (npi)",
            )
        ]

    async def fetchval(self, query):
        assert query == "SELECT current_setting('search_path')"
        self.events.append("capture")
        return self.path

    async def execute(self, query, *parameters):
        if query == "SET LOCAL search_path TO pg_catalog,public":
            assert parameters == ()
            self.path = "pg_catalog,public"
            self.events.append("canonical")
        else:
            assert query == "SELECT set_config('search_path',$1,true)"
            self.events.append("restore")
            if self.restore_failure is not None:
                raise self.restore_failure
            self.path = parameters[0]

    async def fetch(self, query, schema):
        assert self.path == "pg_catalog,public" and schema == "synthetic_candidate"
        assert "pg_get_indexdef(index_relation.oid)" in query
        self.events.append("catalog")
        if self.failure is not None:
            raise self.failure
        return self.records


_PLAN = (("candidate_npi_idx", "CREATE INDEX"),)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ['"$user",public', "pg_catalog", '"synthetic_candidate",pg_catalog'])
async def test_digest_restores_each_caller_path(path):
    connection = _IndexConnection(path)
    baseline = await _index_digest(_IndexConnection("pg_catalog,public"), "synthetic_candidate", _PLAN)
    assert await _index_digest(connection, "synthetic_candidate", _PLAN) == baseline
    assert connection.path == path and connection.events == ["capture", "canonical", "catalog", "restore"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [asyncpg.CannotConnectNowError("unavailable"), asyncio.CancelledError()])
@pytest.mark.parametrize("restore_failure", [None, RuntimeError("transaction aborted")])
async def test_digest_preserves_query_failure(failure, restore_failure):
    connection = _IndexConnection("pg_catalog", failure=failure, restore_failure=restore_failure)
    with pytest.raises(type(failure)) as caught:
        await _index_digest(connection, "synthetic_candidate", _PLAN)
    assert caught.value is failure and connection.events[-1] == "restore"
    if restore_failure is None:
        assert connection.path == "pg_catalog"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "indisvalid", "indisready", "indislive", "index_oid", "indrelid"])
async def test_digest_retains_native_index_refusals(damage):
    connection = _IndexConnection("pg_catalog")
    if damage == "missing":
        connection.records.clear()
    else:
        connection.records[0][damage] = False if damage.startswith("indis") else 0
    with pytest.raises(NetworkServingIndexesError, match="missing or invalid"):
        await _index_digest(connection, "synthetic_candidate", _PLAN)
    assert connection.path == "pg_catalog"


@pytest.mark.asyncio
async def test_digest_keeps_complete_index_definitions():
    connection = _IndexConnection("pg_catalog")
    original = await _index_digest(connection, "synthetic_candidate", _PLAN)
    connection.records[0]["index_definition"] += " WHERE (npi IS NOT NULL)"
    assert await _index_digest(connection, "synthetic_candidate", _PLAN) != original
    connection.records[0]["index_definition"] = connection.records[0]["index_definition"].removesuffix(
        " WHERE (npi IS NOT NULL)"
    )
    assert await _index_digest(connection, "synthetic_candidate", _PLAN) == original
    connection.records.append(
        {
            **connection.records[0],
            "index_oid": 102,
            "relname": "extra_idx",
            "index_definition": connection.records[0]["index_definition"].replace("candidate_npi_idx", "extra_idx"),
        }
    )
    assert await _index_digest(connection, "synthetic_candidate", _PLAN) != original
