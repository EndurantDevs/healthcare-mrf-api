# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database-free workset, COPY-summary, and settings runtime boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib

import pytest

import process.provider_directory_projection_copy_summary as copy_summary
import process.provider_directory_projection_db as projection_db
import process.provider_directory_projection_native_copy as native_copy
import process.provider_directory_projection_workset as workset
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_native_copy_support import stream_reader


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def test_workset_private_validators_fail_closed_on_direct_misuse() -> None:
    context = synthetic_projection_context("ndjson")
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        workset._validated_block_fields(object())
    malformed_block = replace(context.input_block, summary=())
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        workset._validated_block_fields(malformed_block)
    with pytest.raises(
        ProviderDirectoryProjectionError, match="resource_mismatch"
    ):
        workset._validate_workset_resource_coverage(
            context.claim.recipe_lease.recipe, []
        )
    with pytest.raises(
        ProviderDirectoryProjectionError, match="resource_mismatch"
    ):
        workset._validate_workset_resource_coverage(
            context.claim.recipe_lease.recipe,
            [{"resource_type": "Organization"}],
        )


@pytest.mark.asyncio
async def test_shard_claim_preconditions_need_no_database() -> None:
    lease = synthetic_projection_context("ndjson").claim.recipe_lease
    for options, message in (
        ({"lease_seconds": 29, "admission_id": _digest("admission")}, "lease_seconds"),
        ({"lease_seconds": 30, "admission_id": "bad"}, "admission_id"),
        (
            {
                "lease_seconds": 30,
                "admission_id": _digest("admission"),
                "partition_id": "bad",
            },
            "partition_id",
        ),
    ):
        with pytest.raises(ProviderDirectoryProjectionError, match=message):
            await workset.claim_projection_shard(lease, **options)


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error):
        return False


class _FramingDatabase:
    def __init__(self, block_kind) -> None:
        self.block_kind = block_kind

    def transaction(self):
        return _Transaction()

    async def scalar(self, *_args, **_kwargs):
        return self.block_kind


@pytest.mark.asyncio
async def test_shard_framing_is_limited_to_supported_retained_formats(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")

    async def active_recipe(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(workset, "shared_active_recipe", active_recipe)
    assert await workset.projection_shard_input_framing(
        context.claim, database=_FramingDatabase("bundle")
    ) == "bundle"
    with pytest.raises(ProviderDirectoryProjectionError, match="framing_invalid"):
        await workset.projection_shard_input_framing(
            context.claim, database=_FramingDatabase("xml")
        )


def test_copy_summary_helpers_reject_ambiguous_shapes() -> None:
    claim = synthetic_projection_context("ndjson").claim
    with pytest.raises(ProviderDirectoryProjectionError):
        copy_summary._validated_resource_count_map({}, claim, 1)
    with pytest.raises(ProviderDirectoryProjectionError):
        copy_summary._validated_resource_count_map({"Unknown": 1}, claim, 1)
    for identity in (("Organization", "id"), ["Organization"], ["", "id"]):
        with pytest.raises(ProviderDirectoryProjectionError):
            copy_summary._validated_identity(identity)
    invalid_timing_map = {
        field: -1 for field in copy_summary.NATIVE_COPY_TIMING_FIELDS
    }
    for timings in ([], {}, invalid_timing_map):
        with pytest.raises(ProviderDirectoryProjectionError):
            copy_summary._validate_timing_map(timings)


@pytest.mark.asyncio
async def test_native_copy_source_requires_complete_envelope_and_digest() -> None:
    source = native_copy._NativeCopySource(stream_reader(b"x"), 1)
    assert await source.__anext__() == b"x"
    with pytest.raises(StopAsyncIteration):
        await source.__anext__()
    with pytest.raises(ProviderDirectoryProjectionError):
        source.assert_complete(_digest("not-x"))


class _SettingsDatabase:
    def __init__(self, scalar_result=None) -> None:
        self.scalar_result = scalar_result
        self.statements = []

    async def status(self, statement, **_parameters):
        self.statements.append(statement)

    async def scalar(self, _statement, **parameters):
        if "setting_value" in parameters:
            if self.scalar_result is not None:
                return self.scalar_result
            return parameters["setting_value"]
        return self.scalar_result


def test_database_identifier_and_json_helpers_are_strict() -> None:
    assert projection_db.row_mapping(None) == {}
    assert projection_db.json_value('{"count":1}') == {"count": 1}
    json_marker_map = {"count": 1}
    assert projection_db.json_value(json_marker_map) is json_marker_map
    assert projection_db.table_ref(
        "mrf", "projection_stage"
    ) == '"mrf"."projection_stage"'
    with pytest.raises(ProviderDirectoryProjectionError, match="identifier_invalid"):
        projection_db.quoted_identifier("mrf;drop")


@pytest.mark.parametrize("memory", ("invalid", "63", "1025"))
def test_projection_maintenance_memory_is_bounded(monkeypatch, memory) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_MAINTENANCE_WORK_MEM_MB", memory
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="work_mem_invalid"):
        projection_db.projection_maintenance_work_mem_mb()


@pytest.mark.asyncio
async def test_projection_durability_setting_is_verified() -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="durability_invalid"):
        await projection_db.set_local_projection_synchronous_commit(
            _SettingsDatabase(), "local"
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="durability_invalid"):
        await projection_db.set_local_projection_synchronous_commit(
            _SettingsDatabase("off"), "on"
        )
    database = _SettingsDatabase("on")
    await projection_db.set_local_projection_synchronous_commit(database, "on")
    assert database.statements == ["SET LOCAL synchronous_commit = on;"]


@pytest.mark.asyncio
async def test_projection_action_rejects_identity_and_setting_drift() -> None:
    recipe_id = _digest("recipe")
    with pytest.raises(ProviderDirectoryProjectionError, match="action_invalid"):
        await projection_db.set_local_projection_action(
            _SettingsDatabase(), "unknown", recipe_id=recipe_id, recipe_attempt=1
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_invalid"):
        await projection_db.set_local_projection_action(
            _SettingsDatabase(), "gc", recipe_id="bad", recipe_attempt=1
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="setting_failed"):
        await projection_db.set_local_projection_action(
            _SettingsDatabase("drift"),
            "gc",
            recipe_id=recipe_id,
            recipe_attempt=1,
        )
    await projection_db.set_local_projection_action(
        _SettingsDatabase(), "gc", recipe_id=recipe_id, recipe_attempt=1
    )


def test_wal_compression_mode_is_explicit_and_bounded(monkeypatch) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION", "invalid"
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="compression_invalid"):
        asyncio.run(
            projection_db.set_local_projection_wal_compression(_SettingsDatabase())
        )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION", "off"
    )
    assert asyncio.run(
        projection_db.set_local_projection_wal_compression(_SettingsDatabase())
    ) is None
