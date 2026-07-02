# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_tables
from api.ptg2_types import PTG2ServingTables


class FakeResult:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar(self):
        return self._scalar


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    async def execute(self, *_args, **_kwargs):
        self.calls.append((_args, _kwargs))
        value = self._results.pop(0) if self._results else None
        if isinstance(value, FakeResult):
            return value
        return FakeResult(value)


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_non_manifest_storage():
    session = FakeSession(
        [
            {
                "serving_index": {
                    "type": "db_compact",
                    "storage": "db_compact_snapshot",
                    "snapshot_scoped": True,
                    "source_key": "example_dental",
                    "table": "mrf.ptg2_serving_rate_compact_token",
                    "price_code_set_table": "mrf.ptg2_price_code_set_token",
                    "price_atom_table": "mrf.ptg2_price_atom_token",
                    "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                    "procedure_table": "mrf.ptg2_procedure_token",
                    "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
                    "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
                    "provider_group_rate_scope_table": "mrf.ptg2_provider_group_rate_scope_token",
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-token")

    assert tables.storage == "db_compact_snapshot"
    assert tables.type == "db_compact"
    assert tables.snapshot_scoped is True
    assert tables.source_key == "example_dental"
    assert tables.serving_table == "mrf.ptg2_serving_rate_compact_token"
    assert tables.price_atom_table == "mrf.ptg2_price_atom_token"
    assert tables.provider_group_member_table == "mrf.ptg2_provider_group_member_token"
    assert tables.provider_group_rate_scope_table == "mrf.ptg2_provider_group_rate_scope_token"
    assert tables.is_manifest_backed_snapshot is False


@pytest.mark.asyncio
async def test_snapshot_serving_tables_represents_manifest_snapshot_without_v2_table():
    session = FakeSession(
        [
            {
                "serving_index": {
                    "type": "snapshot_index",
                    "storage": "manifest_snapshot",
                    "snapshot_scoped": True,
                    "source_key": "example_dental",
                    "artifact_uri": "file:///tmp/ptg2/snapshot_index/snap-manifest.json",
                    "table": "ptg2_serving_rate; DROP TABLE ptg2_snapshot",
                    "provider_group_member_table": "mrf.ptg2_provider_group_member; DROP",
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-manifest")

    assert tables.storage == "manifest_snapshot"
    assert tables.type == "snapshot_index"
    assert tables.snapshot_scoped is True
    assert tables.source_key == "example_dental"
    assert tables.artifact_uri == "file:///tmp/ptg2/snapshot_index/snap-manifest.json"
    assert tables.serving_table is None
    assert tables.provider_group_member_table is None
    assert tables.is_manifest_backed_snapshot is True


@pytest.mark.asyncio
async def test_snapshot_serving_tables_does_not_resolve_manifest_snapshot_to_fallback_table():
    session = FakeSession(
        [
            {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "artifact_uri": "file:///tmp/ptg2/snapshot_index/snap-manifest.json",
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-manifest")

    assert tables.serving_table is None


@pytest.mark.asyncio
async def test_snapshot_serving_tables_accepts_json_string_manifest():
    session = FakeSession(
        [
            json.dumps(
                {
                    "serving_index": {
                        "storage": "manifest_snapshot",
                        "storage_uri": "s3://healthporta/ptg2/snap-manifest.json",
                    }
                }
            )
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-manifest")

    assert tables.storage == "manifest_snapshot"
    assert tables.artifact_uri == "s3://healthporta/ptg2/snap-manifest.json"
    assert tables.serving_table is None
    assert tables.is_manifest_backed_snapshot is True


def test_manifest_backed_snapshot_type_defaults_to_false_for_empty_metadata():
    tables = PTG2ServingTables()

    assert tables.is_manifest_backed_snapshot is False
