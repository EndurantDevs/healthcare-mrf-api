# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bounded repair semantics; full signed-owner acceptance is a separate pipeline check."""

from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, text

from process import provider_directory_ordinary_scratch_npi as repair
from tests.provider_directory_profile_delta_test_support import _delta_database, importer


def _rows():
    common_by_field = dict(
        npi=None,
        fhir_meta={"versionId": "kept", "nested": [1, {"v": "a"}]},
        last_seen_run_id="root-old",
        updated_at=datetime(2026, 1, 1),
    )
    rows = [dict(common_by_field, source_id="source-a", resource_id=str(1234567890 + ordinal)) for ordinal in range(5)]
    rows.extend(
        [
            dict(common_by_field, source_id="source-b", resource_id="1234567890"),
            dict(common_by_field, source_id=importer.CMS_NPD_SOURCE_ID, resource_id="1234567890"),
            dict(common_by_field, source_id="source-a", resource_id="not-an-npi"),
            dict(common_by_field, source_id="source-a", resource_id="2234567890", npi=9),
        ]
    )
    return rows


async def _read(database, relation):
    return [
        dict(row._mapping) for row in await database.all(f"SELECT * FROM {relation} ORDER BY source_id,resource_id")
    ]


async def _seed(database, schema, root):
    model = repair.npi.output_models()[root]
    scratch = root + "_test_scratch"
    metadata = MetaData()
    tables = [model.__table__.to_metadata(metadata, schema=schema, name=name) for name in (root, scratch)]
    async with database.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        for table in tables:
            await connection.execute(table.insert(), _rows())
        await connection.execute(text(f'ALTER TABLE "{schema}"."{scratch}" SET UNLOGGED'))
    return importer._unscoped_qt(schema, root), importer._unscoped_qt(schema, scratch)


async def _repair_pages(database, schema, root, canonical, scratch, decision_at):
    directory = SimpleNamespace(_schema=lambda: schema, _q=importer._q, CMS_NPD_SOURCE_ID=importer.CMS_NPD_SOURCE_ID)
    inputs, _expected, _count, params = repair.npi.npi_projection(
        directory, root, dict(run_id=None, source_ids=["source-a"])
    )
    statement = repair._batch_sql(inputs.replace(canonical, scratch, 1), scratch)
    params.update(after_source=None, after_resource=None, batch_size=2, update_timestamp=decision_at)
    page_counts = []
    while True:
        batch_by_field = dict((await database.first(statement, **params))._mapping)
        assert batch_by_field["expected"] == batch_by_field["changed"] and 0 <= batch_by_field["changed"] <= 2
        page_counts.append(batch_by_field["changed"])
        if batch_by_field["changed"] == 0:
            return page_counts
        params.update(after_source=batch_by_field["last_source"], after_resource=batch_by_field["last_resource"])


@pytest.mark.asyncio
@pytest.mark.parametrize("root", repair.npi.TARGETS)
@pytest.mark.parametrize("rollback", [False, True])
async def test_native_repair_preserves_unselected_payloads(monkeypatch, root, rollback):
    async with _delta_database(monkeypatch) as (database, schema):
        canonical, scratch = await _seed(database, schema, root)
        before = await _read(database, scratch)
        decision_at = datetime(2026, 2, 1)
        try:
            async with database.transaction():
                assert await _repair_pages(database, schema, root, canonical, scratch, decision_at) == [2, 2, 1, 0]
                assert await _repair_pages(database, schema, root, canonical, scratch, decision_at) == [0]
                if rollback:
                    raise ValueError("late refusal")
        except ValueError:
            assert rollback
        expected_rows = [
            dict(row, npi=int(row["resource_id"]), updated_at=decision_at)
            if not rollback and row["source_id"] == "source-a" and row["npi"] is None and row["resource_id"].isdigit()
            else row
            for row in before
        ]
        assert await _read(database, scratch) == expected_rows
        assert await _read(database, canonical) == before
