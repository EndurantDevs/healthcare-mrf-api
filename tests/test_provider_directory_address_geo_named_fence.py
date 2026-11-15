# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named ZIP fences retain native layout, full data and logical generation checks."""

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from asyncpg import InsufficientPrivilegeError
from sqlalchemy.dialects import postgresql

from db.models import GeoZipLookup
from process import provider_directory_output_address_key_backfill as repair
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows

    def scalars(self):
        return self

    def mappings(self):
        return self.rows


class _GeoSession:
    """Supply native layout and compact full-row chunks without a database transport."""

    def __init__(self):
        self.exists = True
        self.rows = [{"zip_code": "00123", "city": "Example", "state": "FL", "latitude": 29.0}]
        self.columns = [
            (str(column.name), column.type.compile(dialect=postgresql.dialect()).lower(), not column.nullable)
            for column in GeoZipLookup.__table__.columns
        ]
        self.keys = ["zip_code"]
        self.statements = []
        self.failure_at = None
        self.failure = None

    async def scalar(self, statement, params):
        assert str(statement) == "SELECT to_regclass(:relation) IS NOT NULL"
        assert params == {"relation": '"mrf"."geo_zip_lookup"'}
        return self.exists

    async def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if self.failure_at is not None and self.failure_at in sql:
            raise self.failure
        if "FROM pg_attribute WHERE" in sql:
            assert params == {"relation": '"mrf"."geo_zip_lookup"'}
            return _Rows(self.columns)
        if "FROM pg_constraint" in sql:
            assert params == {"relation": '"mrf"."geo_zip_lookup"'}
            return _Rows(self.keys)
        if "WITH row_hashes AS MATERIALIZED" in sql:
            assert 'FROM "mrf"."geo_zip_lookup" AS row_value' in sql
            assert "pg_catalog.to_jsonb(row_value)" in sql
            return self._content_chunks()
        assert "LOCK TABLE ONLY" in sql or "FOR SHARE NOWAIT" in sql
        return _Rows([])

    def _content_chunks(self):
        row_hashes = sorted(
            hashlib.sha256(json.dumps(geo_row, sort_keys=True).encode()).hexdigest() for geo_row in self.rows
        )
        return _Rows(
            []
            if not row_hashes
            else [
                {
                    "chunk_ordinal": 0,
                    "chunk_row_count": len(self.rows),
                    "chunk_sha256": hashlib.sha256("".join(row_hashes).encode()).hexdigest(),
                }
            ]
        )


@pytest.fixture
def geo_fence(monkeypatch):
    session = _GeoSession()
    origin = generation.ReferenceFamilyServingGeneration(
        "00000000-0000-0000-0000-000000000001", 3, datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    authority = generation.ReferenceFamilyResultGenerationAuthority(
        "geo", "00000000-0000-0000-0000-000000000002", 5, origin, (("mrf", repair.GEO),)
    )
    capture = AsyncMock(return_value=origin)
    read = AsyncMock(return_value=authority)
    receipt = archive.ReferenceTableReceipt("GeoZipLookup", repair.GEO, "a" * 64, 1)
    table_receipt = AsyncMock(return_value=receipt)
    monkeypatch.setattr(generation, "capture_reference_family_serving_generation", capture)
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", read)
    monkeypatch.setattr(archive, "_table_receipt", table_receipt)
    return SimpleNamespace(session=session, capture=capture, read=read, receipt=table_receipt, authority=authority)


@pytest.mark.asyncio
async def test_named_geo_fence_uses_native_reads_and_retains_complete_data_receipt(geo_fence):
    snapshot = await repair._geo_snapshot(geo_fence.session, "mrf")
    assert snapshot["relation"] == {"schema_name": "mrf", "relation_name": repair.GEO}
    assert snapshot["shape"] == {
        **geo_fence.receipt.return_value.as_dict(),
        "content_sha256": snapshot["shape"]["content_sha256"],
    }
    assert len(snapshot["shape"]["content_sha256"]) == 64
    assert snapshot["authority"] == geo_fence.authority.as_dict()
    assert geo_fence.session.statements[0] == 'LOCK TABLE ONLY "mrf"."geo_zip_lookup" IN ACCESS SHARE MODE NOWAIT'
    geo_fence.capture.assert_awaited_once_with(geo_fence.session, importer_id="geo", schema_name="mrf")
    geo_fence.receipt.assert_awaited_once_with(
        geo_fence.session, importer_id="geo", schema_name="mrf", model_type=GeoZipLookup
    )
    assert not any(field in json.dumps(snapshot) for field in ("oid", "filenode", "owner", "acl"))
    assert not any("has_table_privilege" in sql or "relowner" in sql for sql in geo_fence.session.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("relations", [None, (("other", repair.GEO),), (("mrf", "other_lookup"),)])
async def test_geo_fence_rejects_a_different_declared_family(geo_fence, relations):
    geo_fence.read.return_value = replace(geo_fence.authority, relations=relations)
    with pytest.raises(RuntimeError, match="address_geo_changed"):
        await repair._geo_snapshot(geo_fence.session, "mrf")
    geo_fence.receipt.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["local_lineage_id", "local_generation", "origin_lineage_id", "origin_generation"])
async def test_final_cut_rejects_changed_local_or_origin_generation(geo_fence, field):
    expected = await repair._geo_snapshot(geo_fence.session, "mrf")
    authority = geo_fence.authority
    if field.startswith("local_"):
        value = 6 if field.endswith("generation") else "00000000-0000-0000-0000-000000000004"
        geo_fence.read.return_value = replace(authority, **{field: value})
    else:
        value = 4 if field.endswith("generation") else "00000000-0000-0000-0000-000000000004"
        geo_fence.read.return_value = replace(
            authority, serving_generation=replace(authority.serving_generation, **{field: value})
        )
    capability = SimpleNamespace(session=geo_fence.session, policy=SimpleNamespace(schema="mrf"))
    with pytest.raises(RuntimeError, match="address_geo_changed"):
        await repair._lock_geo_cut(capability, expected)
    assert any("IN ACCESS SHARE MODE NOWAIT" in sql for sql in geo_fence.session.statements)
    assert any("FOR SHARE NOWAIT" in sql for sql in geo_fence.session.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("city", "Changed"), ("state", "GA"), ("latitude", 30.0)])
async def test_final_cut_rejects_same_count_full_row_content_drift(geo_fence, field, value):
    expected = await repair._geo_snapshot(geo_fence.session, "mrf")
    geo_fence.session.rows[0][field] = value
    capability = SimpleNamespace(session=geo_fence.session, policy=SimpleNamespace(schema="mrf"))
    with pytest.raises(RuntimeError, match="address_geo_changed"):
        await repair._lock_geo_cut(capability, expected)
    assert geo_fence.receipt.return_value.row_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["columns", "primary_key"])
async def test_geo_fence_requires_full_model_layout_and_primary_key(geo_fence, damage):
    if damage == "columns":
        geo_fence.session.columns = geo_fence.session.columns[:-1]
    else:
        geo_fence.session.keys = ["city"]
    with pytest.raises(RuntimeError, match="address_geo_shape_changed"):
        await repair._geo_snapshot(geo_fence.session, "mrf")
    geo_fence.capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_geo_fence_rejects_missing_lookup_before_native_lock(geo_fence):
    geo_fence.session.exists = False
    with pytest.raises(RuntimeError, match="address_geo_snapshot_required"):
        await repair._geo_snapshot(geo_fence.session, "mrf")
    assert not geo_fence.session.statements


@pytest.mark.asyncio
async def test_geo_fence_rejects_inconsistent_full_content_row_accounting(geo_fence):
    geo_fence.session.rows.clear()
    with pytest.raises(RuntimeError, match="address_geo_changed"):
        await repair._geo_snapshot(geo_fence.session, "mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", ["IN ACCESS SHARE MODE", "WITH row_hashes AS MATERIALIZED"])
@pytest.mark.parametrize("error_type", [InsufficientPrivilegeError, asyncio.CancelledError])
async def test_geo_fence_propagates_native_access_denial_and_cancellation(geo_fence, failure_at, error_type):
    error = error_type("native read refused")
    geo_fence.session.failure_at, geo_fence.session.failure = failure_at, error
    with pytest.raises(error_type) as raised:
        await repair._geo_snapshot(geo_fence.session, "mrf")
    assert raised.value is error
