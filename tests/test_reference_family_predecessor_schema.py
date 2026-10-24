# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Predecessor creation uses only the fixed family's existing authority."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import cms_doctors_immutable as immutable
from process import native_publication_read_access as access
from process import provider_directory_generic_source_policy as source
from process import reference_family_archive as archive


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["cms-doctors", "places-zcta"])
async def test_predecessor_create_uses_original_publisher_only_for_doctors(monkeypatch, importer):
    session = SimpleNamespace(execute=AsyncMock())
    dataset = UUID(int=1)
    schema = archive.reference_family_predecessor_schema(dataset)
    spec = SimpleNamespace(importer_id=importer, table_names=("a", "b", "c"))
    stage = SimpleNamespace(dataset_id=dataset, schema_name="incoming")
    incumbent = SimpleNamespace(schema_name="current", relation_oids=(("a", 1), ("b", 2), ("c", 3)))
    body_events = []
    reads = AsyncMock()
    writer = AsyncMock(return_value=19)
    driver = object()
    monkeypatch.setattr(access, "preserve_publication_reads", reads)
    monkeypatch.setattr(immutable, "_archive_writer", writer)
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=driver))

    @asynccontextmanager
    async def publisher_body(supplied):
        assert supplied is session and not session.execute.await_count
        body_events.append("entered")
        yield SimpleNamespace(policy=SimpleNamespace(owner_role="physical_owner"))
        assert session.execute.await_count == 1
        body_events.append("restored")

    monkeypatch.setattr(source, "source_publisher_body", publisher_body)
    assert await archive._rotate_family_relations(session, spec, stage, incumbent) == schema
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    if importer == "cms-doctors":
        writer.assert_awaited_once_with(session, importer, "current")
        assert [call.args for call in reads.await_args_list] == [(driver,)] * 3
        assert [call.kwargs for call in reads.await_args_list] == [
            dict(
                schema="current",
                live_table=name,
                stage_schema="incoming",
                stage_table=name,
                expected_stage_owner_oid=19,
            )
            for name in spec.table_names
        ]
    else:
        writer.assert_not_awaited()
        reads.assert_not_awaited()
    authorization = ' AUTHORIZATION "physical_owner"' if importer == "cms-doctors" else ""
    assert statements[0] == f'CREATE SCHEMA "{schema}"' + authorization
    assert body_events == (["entered", "restored"] if importer == "cms-doctors" else [])
    assert statements[1:] == [
        statement
        for name in spec.table_names
        for statement in (
            f'ALTER TABLE "current"."{name}" SET SCHEMA "{schema}"',
            f'ALTER TABLE "incoming"."{name}" SET SCHEMA "current"',
        )
    ]


@pytest.mark.asyncio
async def test_doctors_predecessor_does_not_fall_back_after_publisher_refusal(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    spec = SimpleNamespace(importer_id="cms-doctors", table_names=("a",))
    stage = SimpleNamespace(dataset_id=UUID(int=1), schema_name="incoming")
    incumbent = SimpleNamespace(schema_name="current", relation_oids=(("a", 1),))
    monkeypatch.setattr(access, "preserve_publication_reads", AsyncMock())
    monkeypatch.setattr(immutable, "_archive_writer", AsyncMock(return_value=19))
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=object()))

    @asynccontextmanager
    async def refused(_session):
        raise RuntimeError("genuine publisher required")
        yield

    monkeypatch.setattr(source, "source_publisher_body", refused)
    with pytest.raises(RuntimeError, match="genuine publisher required"):
        await archive._rotate_family_relations(session, spec, stage, incumbent)
    session.execute.assert_not_awaited()
