# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded prepared-stage keysets and admission without a database service."""

import asyncio
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_registry_cms_prepared_address as clone


def _prepared(rows=4):
    return SimpleNamespace(
        address=SimpleNamespace(db_schema="sample"),
        fhir=object(),
        nonprofile_admission=SimpleNamespace(plan=SimpleNamespace(batch_size=rows), assert_ready=AsyncMock()),
    )


class BoundsConnection:
    """Return native aggregate metadata and retain the queries for range assertions."""

    def __init__(self, keys, *, row_bytes=10, oid=42):
        self.keys = keys
        self.row_bytes = row_bytes
        self.oid = oid
        self.bounds = []

    async def fetchrow(self, query, *arguments):
        assert "record_send(batch)" in query and "OFFSET" not in query and "ctid" not in query
        limit = int(re.search(r"LIMIT (\d+)", query)[1])
        keys = [key for key in self.keys if not arguments or key > arguments][:limit]
        self.bounds.append((limit, arguments, keys))
        return {
            "row_count": len(keys),
            "native_bytes": len(keys) * self.row_bytes,
            **{"copy_key_" + str(index): value for index, value in enumerate(keys[-1] if keys else (None,))},
        }

    async def fetchval(self, query, source):
        assert "relkind='r'" in query and "relpersistence='p'" in query
        assert source == '"sample"."stage"'
        return self.oid


def test_exact_native_model_keys_and_compound_range():
    assert clone._COPY_KEYS == {
        "entity_address_unified": ("location_key",),
        "entity_address_evidence": ("evidence_id",),
        "entity_address_plan_bridge": ("location_key", "entity_type", "entity_id", "plan_id"),
        "entity_address_network_bridge": ("location_key", "entity_type", "entity_id", "network_id"),
        "entity_address_procedure_bridge": ("location_key", "npi", "code_system", "code"),
        "entity_address_medication_bridge": ("location_key", "npi", "code_system", "code"),
        "facility_anchor_npi_candidate": ("candidate_id",),
    }
    keys = clone._COPY_KEYS["entity_address_plan_bridge"]
    predicate, arguments = clone._prepared_copy_range(keys, ("a", "npi", "1", "p1"), ("a", "npi", "1", "p2"))
    assert 'ROW("location_key","entity_type","entity_id","plan_id")>ROW($1,$2,$3,$4)' in predicate
    assert "<=ROW($5,$6,$7,$8)" in predicate
    assert arguments == ("a", "npi", "1", "p1", "a", "npi", "1", "p2")
    assert clone._prepared_copy_limits(_prepared(8192)) == (4096, 64 * 1024**2)


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [True, False, 0, -1, 1.5, "4", None])
async def test_invalid_row_limit_precedes_any_schema_work(rows):
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="copy_limits_invalid"):
        await clone.prepare_retained_registry_address(
            session, _prepared(rows), capture_id=None, owner_role="owner", runtime_roles=("reader",)
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("byte_limit", [True, 0, -1, 1.5, "64", None, 64 * 1024**2 + 1])
async def test_invalid_byte_limit_precedes_any_schema_work(monkeypatch, byte_limit):
    monkeypatch.setattr(clone, "_COPY_BATCH_BYTES", byte_limit)
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="copy_limits_invalid"):
        await clone.prepare_retained_registry_address(
            session, _prepared(), capture_id=None, owner_role="owner", runtime_roles=("reader",)
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("indexed", [False, None, 1])
async def test_missing_or_changed_native_key_is_refused(indexed):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=indexed))
    with pytest.raises(ValueError, match="copy_key_invalid"):
        await clone._require_prepared_copy_key(connection, '"sample"."stage"', 42, ("location_key",))
    query, oid, source, keys = connection.fetchval.call_args.args
    assert (oid, source, keys) == (42, '"sample"."stage"', ["location_key"])
    assert all(term in query for term in ("indimmediate", "indislive", "opcdefault", "indcollation", "attnotnull"))


@pytest.mark.asyncio
@pytest.mark.parametrize("overflow", [False, True])
async def test_wide_or_actual_spool_overflow_halves_without_omitting_rows(monkeypatch, overflow):
    keys = [("a", "npi", "1", "p" + str(index)) for index in range(5)]
    connection = BoundsConnection(keys, row_bytes=10 if overflow else 50)
    prepared, imports, attempted = _prepared(), [], []

    async def copy(_connection, query, arguments, schema, table, count, **options):
        attempted.append(count)
        if overflow and len(attempted) == 1:
            raise clone._CopyBatchTooLarge
        assert options["byte_limit"] == 130
        assert schema == "retained" and table == "entity_address_plan_bridge"
        assert "OFFSET" not in query and "ORDER BY" in query
        assert "<=ROW(" in query
        await options["import_admission"]("before_import")
        imports.extend(key for key in keys if (len(arguments) == 4 or key > arguments[:4]) and key <= arguments[-4:])
        await options["import_admission"]("after_import")

    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    await clone._copy_prepared_rows(
        connection, prepared, "retained", ("entity_address_plan_bridge", "stage", 42), 5, (4, 130)
    )
    assert imports == keys
    assert attempted == ([4, 2, 2, 1] if overflow else [2, 2, 1])
    assert prepared.nonprofile_admission.assert_ready.await_count == 6
    assert connection.bounds[0][:2] == (4, ()) and connection.bounds[1][:2] == (2, ())


@pytest.mark.asyncio
@pytest.mark.parametrize("overflow", [False, True])
async def test_one_overwide_row_is_refused_before_admission(monkeypatch, overflow):
    prepared = _prepared()
    connection = BoundsConnection([("a",)], row_bytes=1 if overflow else 130)
    copy = AsyncMock(side_effect=clone._CopyBatchTooLarge)
    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    with pytest.raises(ValueError, match="copy_row_too_large"):
        await clone._copy_prepared_rows(
            connection, prepared, "retained", ("entity_address_unified", "stage", 42), 1, (1, 130)
        )
    assert copy.await_count == int(overflow)
    prepared.nonprofile_admission.assert_ready.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["oid", "before", "after"])
async def test_batch_oid_refusal_and_admission_cancellation_propagate(monkeypatch, failure):
    prepared = _prepared()
    connection = BoundsConnection([("a",)], oid=43 if failure == "oid" else 42)
    phases = []
    if failure != "oid":
        prepared.nonprofile_admission.assert_ready.side_effect = (
            asyncio.CancelledError if failure == "before" else [None, asyncio.CancelledError]
        )

    async def copy(*_arguments, import_admission, **_options):
        await import_admission("before_import")
        phases.append("copied")
        await import_admission("after_import")

    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    with pytest.raises(ValueError if failure == "oid" else asyncio.CancelledError):
        await clone._copy_prepared_rows(
            connection, prepared, "retained", ("entity_address_unified", "stage", 42), 1, (1, 130)
        )
    assert phases == (["copied"] if failure == "after" else [])


@pytest.mark.asyncio
async def test_native_row_accounting_must_reach_exact_receipt(monkeypatch):
    connection = BoundsConnection([("a",)])
    copy = AsyncMock()
    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    with pytest.raises(ValueError, match="copy_accounting_invalid"):
        await clone._copy_prepared_rows(
            connection, _prepared(), "retained", ("entity_address_unified", "stage", 42), 2, (1, 130)
        )
    assert copy.await_count == 1
