# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from unittest.mock import AsyncMock

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
async def test_snapshot_serving_tables_requires_published_and_does_not_cache_empty_manifest():
    class RealishFakeSession(FakeSession):
        sync_session = object()

    snapshot_id = "snap-building-then-published"
    ptg2_tables._PTG2_SNAPSHOT_TABLES_CACHE.pop(snapshot_id, None)
    session = RealishFakeSession(
        [
            None,
            {"serving_index": {"table": "mrf.ptg2_serving_rate_compact_published"}},
        ]
    )

    building = await ptg2_tables.snapshot_serving_tables(session, snapshot_id)
    published = await ptg2_tables.snapshot_serving_tables(session, snapshot_id)

    assert building.serving_table is None
    assert published.serving_table == "mrf.ptg2_serving_rate_compact_published"
    assert len(session.calls) == 2
    assert "status = 'published'" in str(session.calls[0][0][0])
    ptg2_tables._PTG2_SNAPSHOT_TABLES_CACHE.pop(snapshot_id, None)


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
                    "arch_version": "materialized_v1",
                    "provider_scope_strategy": "materialized_rate_scope",
                    "materialized_tables": {
                        "provider_group_rate_scope": "mrf.ptg2_provider_group_rate_scope_token"
                    },
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
    assert tables.arch_version == "materialized_v1"
    assert tables.effective_arch_version == "materialized_v1"
    assert tables.provider_scope_strategy == "materialized_rate_scope"
    assert tables.materialized_tables == {"provider_group_rate_scope": "mrf.ptg2_provider_group_rate_scope_token"}
    assert tables.uses_sidecar_provider_scope is False
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
    assert tables.effective_arch_version == "sidecar_scope_v1"
    assert tables.uses_sidecar_provider_scope is True


@pytest.mark.asyncio
async def test_snapshot_serving_tables_reads_postgres_binary_serving_table():
    session = FakeSession(
        [
            {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "arch_version": "postgres_binary_v1",
                    "provider_scope_strategy": "sidecar_provider_scope",
                    "serving_binary_table": "mrf.ptg2_serving_binary_snap",
                    "price_atom_table_layout": "lean_dict_v2",
                    "price_atom_constant_keys": {"service_code_key": 0},
                    "price_atom_constant_values": {"service_code": ["11"]},
                    "materialized_tables": {
                        "serving_binary": "mrf.ptg2_serving_binary_snap",
                    },
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-binary")

    assert tables.serving_binary_table == "mrf.ptg2_serving_binary_snap"
    assert tables.price_atom_table_layout == "lean_dict_v2"
    assert tables.price_atom_constant_keys == {"service_code_key": 0}
    assert tables.price_atom_constant_values == {"service_code": ["11"]}
    assert tables.effective_arch_version == "postgres_binary_v1"
    assert tables.uses_sidecar_provider_scope is True


@pytest.mark.asyncio
async def test_snapshot_serving_tables_reads_v2_membership_scope():
    session = FakeSession(
        [
            {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "arch_version": "postgres_binary_v2",
                    "provider_scope_strategy": "sidecar_provider_scope",
                    "serving_binary_table": "mrf.ptg2_serving_binary_snap",
                    "provider_npi_scope_table": "mrf.ptg2_provider_npi_scope_snap",
                    "materialized_tables": {
                        "serving_binary": "mrf.ptg2_serving_binary_snap",
                        "provider_npi_scope": "mrf.ptg2_provider_npi_scope_snap",
                    },
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-binary-v2")

    assert tables.effective_arch_version == "postgres_binary_v2"
    assert tables.uses_sidecar_provider_scope is True
    assert tables.provider_npi_scope_table == "mrf.ptg2_provider_npi_scope_snap"


@pytest.mark.asyncio
async def test_snapshot_serving_tables_reads_nested_v3_atom_key_width():
    session = FakeSession(
        [
            {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "arch_version": "postgres_binary_v3",
                    "serving_binary_table": "mrf.ptg2_serving_binary_snap",
                    "serving_binary": {
                        "arch_version": "postgres_binary_v3",
                        "dense_atom_keys": {"atom_count": 7, "atom_key_bits": 24},
                        "price_set_atom_memberships_v3": {"block_span": 512},
                        "price_atoms_v3": {"block_span": 512},
                        "price_dictionary": {
                            "price_set_count": 29_000_000,
                            "block_bytes": 65_536,
                            "storage": {"compressed_records": 0},
                        },
                    },
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-binary-v3")

    assert tables.effective_arch_version == "postgres_binary_v3"
    assert tables.atom_key_bits == 24
    assert tables.price_key_block_span == 512
    assert tables.atom_key_block_span == 512
    assert tables.price_dictionary_item_count == 29_000_000
    assert tables.price_dictionary_block_bytes == 65_536
    assert tables.price_dictionary_compressed_records == 0


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


@pytest.mark.asyncio
async def test_snapshot_serving_tables_keeps_db_artifacts_as_metadata_by_default(monkeypatch):
    async def fail_hydrate(*_args, **_kwargs):
        raise AssertionError("db artifacts should not be materialized during snapshot table discovery")

    monkeypatch.delenv("HLTHPRT_PTG2_ARTIFACT_DB_MATERIALIZE_ON_READ", raising=False)
    monkeypatch.setattr(ptg2_tables, "hydrate_ptg2_artifact_entry_from_db", fail_hydrate)
    session = FakeSession(
        [
            {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "artifacts": {
                        "provider_forward": {
                            "name": "provider_forward",
                            "path": "/work/old/provider_forward.ptg2sc",
                            "storage_uri": "db://ptg2_artifact/provider-forward",
                            "byte_count": 123,
                        }
                    },
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-db-artifact")

    assert tables.artifacts["provider_forward"] == {
        "name": "provider_forward",
        "path": "/work/old/provider_forward.ptg2sc",
        "storage_uri": "db://ptg2_artifact/provider-forward",
        "byte_count": 123,
    }


@pytest.mark.asyncio
async def test_snapshot_serving_tables_materializes_legacy_artifacts_when_enabled(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DB_MATERIALIZE_ON_READ", "true")
    monkeypatch.setattr(
        ptg2_tables,
        "hydrate_ptg2_artifact_entry_from_db",
        AsyncMock(
            return_value={
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward",
                "path": "/tmp/materialized/provider_forward.ptg2sc",
                "cache_path": "/tmp/materialized/provider_forward.ptg2sc",
            }
        ),
    )
    session = FakeSession(
        [
            {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "arch_version": "sidecar_scope_v1",
                    "artifacts": {
                        "provider_forward": {
                            "name": "provider_forward",
                            "path": "/work/old/provider_forward.ptg2sc",
                            "storage_uri": "db://ptg2_artifact/provider-forward",
                        }
                    },
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-legacy-artifact")

    assert tables.artifacts["provider_forward"]["path"] == "/tmp/materialized/provider_forward.ptg2sc"
    assert tables.artifacts["provider_forward"]["cache_path"] == "/tmp/materialized/provider_forward.ptg2sc"


@pytest.mark.asyncio
async def test_snapshot_serving_tables_keeps_postgres_binary_artifacts_as_metadata(monkeypatch):
    async def fail_hydrate(*_args, **_kwargs):
        raise AssertionError("postgres_binary_v1 must not materialize db artifacts to local files")

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DB_MATERIALIZE_ON_READ", "true")
    monkeypatch.setattr(ptg2_tables, "hydrate_ptg2_artifact_entry_from_db", fail_hydrate)
    session = FakeSession(
        [
            {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "arch_version": "postgres_binary_v1",
                    "serving_binary_table": "mrf.ptg2_serving_binary_snap",
                    "artifacts": {
                        "provider_forward": {
                            "name": "provider_forward",
                            "path": "/work/old/provider_forward.ptg2sc",
                            "storage_uri": "db://ptg2_artifact/provider-forward",
                            "byte_count": 123,
                        }
                    },
                }
            }
        ]
    )

    tables = await ptg2_tables.snapshot_serving_tables(session, "snap-postgres-binary-artifact")

    assert tables.effective_arch_version == "postgres_binary_v1"
    assert tables.artifacts["provider_forward"] == {
        "name": "provider_forward",
        "path": "/work/old/provider_forward.ptg2sc",
        "storage_uri": "db://ptg2_artifact/provider-forward",
        "byte_count": 123,
    }


def test_manifest_backed_snapshot_type_defaults_to_false_for_empty_metadata():
    tables = PTG2ServingTables()

    assert tables.is_manifest_backed_snapshot is False
