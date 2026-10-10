# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current approved choices on the existing native migrated, exactly cleaned fixture."""

from dataclasses import replace
from uuid import uuid4

import pytest

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_approved_company_read import read_registry_approved_companies
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import (
    _actor,
    _approve,
    _command,
    _create,
    _draft,
)

pytestmark = pytest.mark.asyncio


async def _read(connection, schema, **query):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await read_registry_approved_companies(connection, schema=schema, **query)


async def test_approved_company_read_never_returns_drafts_and_refuses_stale_page(
    serving_schema,
):
    """Draft corrections stay hidden, later pages pin the current map, and archives disappear."""
    connection, schema, engine = serving_schema
    actor = _actor()
    command = _create("company")
    first = await _draft(engine, schema, command, actor)
    assert (await _read(connection, schema))["items"] == []
    receipt = await _approve(connection, schema, await _command(connection, schema, first), actor)
    revision = receipt["approved_revision"]
    pending = await _draft(
        engine,
        schema,
        replace(
            command,
            operation="correct",
            expected_revision=1,
            fields={**command.fields, "display_name": "Pending name"},
            idempotency_key=uuid4().hex,
        ),
        actor,
    )
    company_page = await _read(connection, schema, approved_revision=revision)
    assert company_page["items"] == [{"company_id": first["record_id"], "display_name": "Example Record"}]
    second = await _draft(engine, schema, _create("company"), actor)
    receipt2 = await _approve(connection, schema, await _command(connection, schema, second), actor)
    with pytest.raises(RegistryApprovalConflict):
        await _read(connection, schema, offset=1, approved_revision=revision)
    await _assert_company_pages(connection, schema, receipt2["approved_revision"], first, second)
    archived = await _draft(
        engine,
        schema,
        replace(
            command,
            operation="archive",
            expected_revision=pending["revision"],
            fields={},
            idempotency_key=uuid4().hex,
        ),
        actor,
    )
    latest = await _approve(connection, schema, await _command(connection, schema, archived), actor)
    assert (await _read(connection, schema, approved_revision=latest["approved_revision"]))["items"] == [
        {"company_id": second["record_id"], "display_name": "Example Record"}
    ]


async def _assert_company_pages(connection, schema, revision, first, second):
    """Verify stable complete paging over one exact immutable revision."""
    first_company_page = await _read(connection, schema, limit=1, approved_revision=revision)
    next_company_page = await _read(
        connection,
        schema,
        limit=1,
        offset=1,
        approved_revision=revision,
    )
    assert first_company_page["has_more"] and not next_company_page["has_more"]
    assert [item["company_id"] for item in first_company_page["items"] + next_company_page["items"]] == sorted(
        [first["record_id"], second["record_id"]]
    )
