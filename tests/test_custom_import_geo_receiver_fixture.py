# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the receiver's synthetic hydration aligned with a full native page."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


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

context = SimpleNamespace(
    target=SimpleNamespace(dataset_id=11, schema_revision_id=21),
    definition=_DEFINITION,
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
