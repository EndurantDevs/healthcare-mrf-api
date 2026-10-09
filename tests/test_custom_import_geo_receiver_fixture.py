# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the receiver's synthetic hydration aligned with a full native page."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_SIGNED_GEO_READER_SCRIPT = """
import asyncio
import dataclasses
import json
from types import SimpleNamespace

import pytest

from api import custom_import_read_http as transport
from process.custom_import.read_contracts import PinnedReadTarget
from process.custom_import.read_identity import resolve_generation_snapshot
from tests import test_custom_import_read_http as headers
from tests.fixtures import custom_import_geo_receiver as fixture

headers._install_keyring(pytest.MonkeyPatch())
target = {
    "dataset_key": "synthetic_dataset", "definition_revision_id": 11,
    "generation_id": 12, "schema_revision_id": 13, "profile_id": "default",
}
document = {
    "target": target,
    "context": [{"field_id": "region_alias", "operator": "eq", "value": "north"}],
    "filters": [{"field_id": "score_alias", "operator": "gt", "value": 7}],
    "order": [{"field_id": "score_alias", "direction": "desc"}],
    "native_query": {"lat": "40", "long": "-73", "radius": "2.5", "limit": "1", "view": "card"},
    "require_match": True,
}

def read_page(cursor, scope="a" * 64):
    document["native_query"].pop("cursor", None)
    if cursor is not None:
        document["native_query"]["cursor"] = cursor
    body = transport._canonical_json_bytes(document)
    event = {"transport_verified": False, "native_page": False, "hydration": False}
    fixture._FIXTURE_STATE.request_event_by_name = event
    session = fixture.SyntheticSession(target)
    request = SimpleNamespace(
        body=body, method="POST", path=fixture.geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH,
        query_string="", args={}, ctx=SimpleNamespace(sa_session=session),
        headers=headers._resigned_provider_headers(
            body=body, path=fixture.geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH, target=target,
            authorization_scope_sha256=scope,
        ),
    )
    return asyncio.run(fixture.geo.serve_custom_import_provider_geo(request, session)), event

for include in (False, True):
    document["include_filter"] = include
    cursor = None
    for ordinal, npi in enumerate(fixture._NPIS):
        reply, event = read_page(cursor)
        assert reply.status == 200 and event["transport_verified"] and event["native_page"]
        assert event["hydration"] is include
        page = json.loads(reply.body)
        assert page["total_count"] == len(fixture._NPIS) and len(page["items"]) == 1
        assert str(page["items"][0]["npi"]) == npi
        assert ("custom_import" in page["items"][0]) is include
        if include:
            assert page["items"][0]["custom_import"]["root_fields"][0]["value"] == npi
        cursor = page["next_cursor"]
        assert page["has_more"] is (ordinal < len(fixture._NPIS) - 1)
        if ordinal == 0:
            first_cursor = cursor
    assert cursor is None
    rejected, event = read_page(first_cursor, scope="b" * 64)
    assert rejected.status == 400 and event["transport_verified"] and event["cursor_attempt"]
    assert not event["native_page"] and not event["hydration"]

session = fixture.SyntheticSession(target)
pinned = PinnedReadTarget(11, 12, 11, 13, "default")
assert asyncio.run(resolve_generation_snapshot(session, pinned)) is None
for field in ("dataset_id", "generation_id", "definition_revision_id", "schema_revision_id"):
    mismatched = dataclasses.replace(pinned, **{field: getattr(pinned, field) + 1})
    with pytest.raises(AssertionError):
        asyncio.run(resolve_generation_snapshot(session, mismatched))
"""


def test_geo_receiver_preserves_signed_reader_semantics():
    child_process = subprocess.run(
        [sys.executable, "-c", _SIGNED_GEO_READER_SCRIPT],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert child_process.returncode == 0, child_process.stderr


def test_geo_receiver_hydrates_every_npi_in_a_separate_process():
    """Import the fixture only in a child process so its audit hook cannot escape."""

    script = """
import asyncio
from types import SimpleNamespace

from sqlalchemy import select

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportEntityBinding,
    CustomImportFamilyRevision,
    CustomImportRootRevision,
    CustomImportWinner,
)
from process.custom_import import read_core
from tests.fixtures.custom_import_geo_receiver import _DEFINITION, _FIXTURE_STATE, _NPIS, SyntheticSession

_FIXTURE_STATE.request_event_by_name = {"hydration": False}
session = SyntheticSession({})
statement = select(
    CustomImportWinner,
    CustomImportFamilyRevision,
    CustomImportRootRevision,
    CustomImportChildRevision,
    CustomImportEntityBinding.canonical_value,
).where(CustomImportEntityBinding.canonical_value.in_(_NPIS))
rows = asyncio.run(session.execute(statement)).all()
assert tuple(row[-1] for row in rows) == _NPIS
assert len({row[0].entity_binding_id for row in rows}) == len(_NPIS)
assert len({row[1].root_record_id for row in rows}) == len(_NPIS)
assert len({row[2].root_revision_id for row in rows}) == len(_NPIS)
assert len({row[3].child_revision_id for row in rows}) == len(_NPIS)

context = read_core._ReadContext(
    target=SimpleNamespace(dataset_id=11, schema_revision_id=21),
    definition=_DEFINITION,
    profile_slot=1,
    profile_context_slot=1,
    collection_slots_by_name={"rates": 1},
    collection_names_by_slot={1: "rates"},
)
items = asyncio.run(read_core._hydrate_provider_page_items(session, context, tuple(row[:-1] for row in rows), "a" * 64))
assert len(items) == len(_NPIS)
assert tuple(item.root_fields[0].value for item in items) == _NPIS
assert all(item.context_fields for item in items)
assert all(len(item.children) == 1 and item.children[0].collection == "rates" for item in items)
assert _FIXTURE_STATE.request_event_by_name["hydration"]
"""
    child_process = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert child_process.returncode == 0, child_process.stderr
