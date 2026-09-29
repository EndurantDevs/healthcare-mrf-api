# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Gateway contract, authenticated cursors and sanitized serving failures."""

import base64
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import SQLAlchemyError

from api.endpoint import provider_directory_entities as endpoint
from api.provider_directory_entities_contract import (
    CURSOR_KEY_ENV,
    DirectoryReadError,
    directory_cursor_key,
    issue_directory_cursor,
    parse_directory_read,
    read_directory_cursor,
)

KEY = bytes(range(32))
ENTITY_ID = "00000000-0000-0000-0000-000000000001"


def directory_query(query="source_id=cms-doctors&limit=2", *, shape="entities", entity_id=None):
    return parse_directory_read("medical-groups", entity_id, shape, query)


@pytest.mark.parametrize(
    "query",
    [
        "",
        "source_id=cms-doctors&source_id=cms-doctors",
        "source_id=cms-doctors&extra=1",
        "source_id=cms-doctors&",
        "source_id=%FF",
        "source_id=%0G",
        "source_id=CMS",
        "source_id=cms-doctors&limit=0",
        "source_id=cms-doctors&limit=01",
        "source_id=cms-doctors&limit=101",
        "source_id=cms-doctors&cursor=abc",
        "source_id=cms-doctors&generation_id=",
        "source_id=cms-doctors&generation_id=" + "x" * 129,
        "source_id=" + "x" * 4096,
    ],
)
def test_query_rejects_ambiguous_or_unbounded_selectors(query):
    with pytest.raises(DirectoryReadError) as caught:
        directory_query(query)
    assert caught.value.status == 400


def test_detail_rejects_paging_and_normalizes_uuid():
    query = directory_query("source_id=cms-doctors", shape="entity", entity_id=ENTITY_ID.replace("-", ""))
    assert query.entity_id == ENTITY_ID
    with pytest.raises(DirectoryReadError):
        directory_query(shape="entity", entity_id=ENTITY_ID)


@pytest.mark.parametrize("key_value", [None, "", "a" * 42, "!" * 43, "_" * 43])
def test_key_is_required_canonical_and_never_generated(monkeypatch, key_value):
    monkeypatch.delenv(CURSOR_KEY_ENV, raising=False)
    if key_value is not None:
        monkeypatch.setenv(CURSOR_KEY_ENV, key_value)
    with pytest.raises(DirectoryReadError) as caught:
        directory_cursor_key()
    assert caught.value.status == 503


def test_cursor_roundtrip_expiry_and_tampering():
    query = directory_query()
    cursor = issue_directory_cursor(KEY, query, "gen_test", ENTITY_ID, now=100)
    assert ENTITY_ID.encode() not in base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
    pinned = replace(query, generation_id="gen_test", cursor=cursor)
    assert read_directory_cursor(KEY, pinned, "gen_test", now=999) == ENTITY_ID
    for bad_query, now in ((pinned, 1000), (replace(pinned, cursor=cursor[:-3] + "AAA"), 101)):
        with pytest.raises(DirectoryReadError) as caught:
            read_directory_cursor(KEY, bad_query, "gen_test", now=now)
        assert caught.value.status == 409


@pytest.mark.parametrize(
    "changed",
    [
        {"source_id": "another-source"},
        {"kind": "organizations"},
        {"shape": "relationships"},
        {"entity_id": ENTITY_ID},
        {"limit": 3},
    ],
)
def test_cursor_cannot_cross_query_scope(changed):
    query = directory_query()
    cursor = issue_directory_cursor(KEY, query, "gen_test", ENTITY_ID, now=100)
    with pytest.raises(DirectoryReadError) as caught:
        read_directory_cursor(KEY, replace(query, cursor=cursor, **changed), "gen_test", now=101)
    assert caught.value.status == 409


def test_cursor_cannot_cross_generation_or_key():
    query = directory_query()
    cursor = issue_directory_cursor(KEY, query, "gen_first", ENTITY_ID, now=100)
    pinned = replace(query, cursor=cursor)
    for key, generation in ((KEY, "gen_second"), (bytes(reversed(KEY)), "gen_first")):
        with pytest.raises(DirectoryReadError) as caught:
            read_directory_cursor(key, pinned, generation, now=101)
        assert caught.value.status == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 409, 503])
async def test_boundary_preserves_gateway_error_contract(monkeypatch, status):
    async def reject_read(*_args):
        raise DirectoryReadError(status)

    monkeypatch.setattr(endpoint, "read_medical_groups", reject_read)
    request = SimpleNamespace(query_string="source_id=cms-doctors", ctx=SimpleNamespace(sa_session=None))
    result = await endpoint.list_entities(request, "medical-groups")
    code, message = endpoint._ERRORS[status]
    assert result.status == status
    assert json.loads(result.body) == {"error": {"code": code, "message": message}}
    assert result.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_database_failure_never_exposes_source_values(monkeypatch):
    async def reject_read(*_args):
        raise SQLAlchemyError("synthetic-private-source-value")

    monkeypatch.setattr(endpoint, "read_medical_groups", reject_read)
    request = SimpleNamespace(query_string="source_id=cms-doctors", ctx=SimpleNamespace(sa_session=None))
    result = await endpoint.list_entities(request, "medical-groups")
    assert result.status == 503
    assert b"synthetic-private-source-value" not in result.body
