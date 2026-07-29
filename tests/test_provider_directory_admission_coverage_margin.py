# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed admission and projection-runtime boundary coverage."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

import process.provider_directory_projection_copy_summary as copy_summary
import process.provider_directory_projection_db as projection_db
import process.provider_directory_projection_native_copy as native_copy
import process.provider_directory_projection_stage as projection_stage
import process.provider_directory_projection_workset as projection_workset
import process.uhc_official_file_acquisition as uhc_acquisition
from process.provider_directory_projection_types import (
    ProjectionProofShard,
    ProjectionStage,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)
from process.provider_directory_refresh_preset import (
    apply_provider_directory_refresh_preset,
)
from process.uhc_provider_file_admission import _requested_source_ids
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_native_copy_support import (
    stream_reader,
)


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error):
        return False


class _StatusDatabase:
    def __init__(self, *, status_result=0, first_result=None) -> None:
        self.status_result = status_result
        self.first_result = first_result
        self.statements: list[str] = []

    def transaction(self):
        return _Transaction()

    async def status(self, statement, **_parameters):
        self.statements.append(statement)
        return self.status_result

    async def first(self, *_args, **_kwargs):
        return self.first_result


def _projection_proof(context, *, resource_count=0) -> ProjectionProofShard:
    claim = context.claim
    return ProjectionProofShard(
        recipe_id=claim.recipe_lease.recipe.recipe_id,
        attempt=claim.recipe_lease.attempt,
        partition_attempt=claim.partition_attempt,
        partition_id=claim.shard.partition_id,
        partition_ordinal=claim.shard.partition_ordinal,
        resource_type=claim.shard.resource_type,
        input_sha256=claim.shard.input_sha256,
        canonical_row_sha256="a" * 64,
        resource_count=resource_count,
        first_identity=("Organization", "first"),
        last_identity=("Organization", "last"),
        proof={"resource_counts": {"Organization": resource_count}},
    )


def test_admission_normalizers_retain_blank_preset_and_scalar_source() -> None:
    task = {"refresh_preset": "  ", "import_resources": True}
    assert apply_provider_directory_refresh_preset(task) is task
    assert _requested_source_ids({"source_id": 123}) == {"123"}


@pytest.mark.asyncio
async def test_projection_stage_creation_rejects_name_and_oid_failures(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")
    database = _StatusDatabase(status_result="CREATE TABLE")

    async def existing_relation(*_args, **_kwargs):
        return 9

    monkeypatch.setattr(projection_stage, "relation_oid", existing_relation)
    with pytest.raises(ProviderDirectoryProjectionError, match="name_conflict"):
        await projection_stage._create_projection_stage(
            context.claim.recipe_lease,
            database,
            "mrf",
        )

    monkeypatch.setattr(
        projection_stage,
        "relation_oid",
        AsyncMock(side_effect=(None, None)),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="create_failed"):
        await projection_stage._create_projection_stage(
            context.claim.recipe_lease,
            database,
            "mrf",
        )


@pytest.mark.asyncio
async def test_projection_stage_binding_and_workset_are_lease_fenced(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")
    database = _StatusDatabase(status_result=0)
    monkeypatch.setattr(
        projection_stage,
        "set_local_projection_action",
        AsyncMock(),
    )
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        await projection_stage._bind_projection_stage(
            context.claim.recipe_lease,
            context.stage,
            database,
            "mrf",
        )

    monkeypatch.setattr(
        projection_stage,
        "locked_active_recipe",
        AsyncMock(return_value={"workset_registered_at": None}),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_missing"):
        await projection_stage.ensure_projection_stage(
            context.claim.recipe_lease,
            database=database,
            schema="mrf",
        )


class _RecordsOnlyDriver:
    async def copy_records_to_table(self, *_args, **_kwargs):
        return "COPY 1"


class _StreamOnlyDriver:
    def __init__(self, status="COPY 1") -> None:
        self.status = status

    async def copy_to_table(self, *_args, **_kwargs):
        return self.status


@pytest.mark.asyncio
async def test_projection_copy_requires_the_exact_driver_and_status() -> None:
    stage = ProjectionStage("mrf", "projection_stage", 1)
    row = (None,) * len(projection_stage.STAGE_COPY_COLUMNS)
    with pytest.raises(ProviderDirectoryProjectionError, match="driver_missing"):
        await projection_stage.copy_projection_stage_records(
            stage,
            (row,),
            transaction=_StreamOnlyDriver(),
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="driver_missing"):
        await projection_stage.copy_projection_stage_binary_stream(
            stage,
            object(),
            transaction=_RecordsOnlyDriver(),
        )
    for status in ("COPY invalid", "COPY -1"):
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="status_invalid",
        ):
            await projection_stage.copy_projection_stage_binary_stream(
                stage,
                object(),
                transaction=_StreamOnlyDriver(status),
            )


@pytest.mark.asyncio
async def test_projection_partition_retries_and_census_fail_closed(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")
    database = _StatusDatabase(status_result="DELETE 0")
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_invalid"):
        await projection_stage.prepare_projection_stage_partition(
            context.stage,
            object(),
            database=database,
        )

    monkeypatch.setattr(
        projection_stage,
        "_validate_bound_stage",
        AsyncMock(),
    )
    retry_claim = replace(context.claim, partition_attempt=2)
    await projection_stage.prepare_projection_stage_partition(
        context.stage,
        retry_claim,
        database=database,
    )
    assert any("DELETE FROM" in statement for statement in database.statements)

    with pytest.raises(ProviderDirectoryProjectionError, match="census_invalid"):
        await projection_stage._stage_partition_census(
            context.stage,
            context.claim,
            _StatusDatabase(first_result=None),
        )
    invalid_count = {
        "resource_count": True,
        "resource_count_map": {},
        "first_identity": None,
        "last_identity": None,
    }
    with pytest.raises(ProviderDirectoryProjectionError, match="census_invalid"):
        await projection_stage._stage_partition_census(
            context.stage,
            context.claim,
            _StatusDatabase(first_result=invalid_count),
        )


@pytest.mark.asyncio
async def test_projection_stage_accepts_an_exact_empty_census(monkeypatch) -> None:
    context = synthetic_projection_context("ndjson")
    proof = replace(
        _projection_proof(context),
        first_identity=None,
        last_identity=None,
        proof={
            "resource_counts": {
                resource: 0
                for resource in context.claim.recipe_lease.recipe.selected_resources
            }
        },
    )
    census = projection_stage._ProjectionStageCensus(
        0,
        dict(proof.proof["resource_counts"]),
        None,
        None,
    )
    monkeypatch.setattr(projection_stage, "_validate_bound_stage", AsyncMock())
    monkeypatch.setattr(
        projection_stage,
        "_stage_partition_census",
        AsyncMock(return_value=census),
    )
    await projection_stage.assert_projection_stage_partition(
        context.stage,
        context.claim,
        proof,
        database=object(),
    )


def test_projection_workset_reconstruction_detects_serializer_drift(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")
    monkeypatch.setattr(
        projection_workset,
        "_block_fields",
        Mock(side_effect=({"block": 1}, {"block": 2})),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        projection_workset._validated_block_fields(context.input_block)


@pytest.mark.asyncio
async def test_projection_shard_checkpoint_and_proof_are_lease_fenced(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")
    proof = _projection_proof(context, resource_count=1)
    database = _StatusDatabase(status_result=0)
    monkeypatch.setattr(
        projection_workset,
        "set_local_projection_action",
        AsyncMock(),
    )
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        await projection_workset._checkpoint_projection_shard(
            context.claim,
            proof,
            database,
            "mrf",
        )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="completion_mismatch",
    ):
        await projection_workset.complete_projection_shard(
            context.claim,
            object(),
            child_lease=context.child_lease,
            database=database,
        )

    monkeypatch.setattr(
        projection_workset,
        "validated_child_shard_claim",
        lambda value: value,
    )
    monkeypatch.setattr(
        projection_workset,
        "validated_child_read_lease",
        lambda value: value,
    )
    monkeypatch.setattr(
        projection_workset,
        "claimed_projection_proof_shard",
        lambda *_args, **_kwargs: replace(proof, resource_count=2),
    )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="completion_mismatch",
    ):
        await projection_workset.complete_projection_shard(
            context.claim,
            proof,
            child_lease=context.child_lease,
            database=database,
        )


def test_native_copy_summary_rejects_specific_resource_mismatch() -> None:
    context = synthetic_projection_context("ndjson")
    specific_claim = replace(
        context.claim,
        shard=replace(context.claim.shard, resource_type="Organization"),
    )
    alternate_resource = next(
        resource
        for resource in context.claim.recipe_lease.recipe.selected_resources
        if resource != "Organization"
    )
    with pytest.raises(ProviderDirectoryProjectionError):
        copy_summary._validated_resource_count_map(
            {alternate_resource: 1},
            specific_claim,
            1,
        )


@pytest.mark.asyncio
async def test_native_copy_source_rejects_truncation_and_tracks_late_chunks() -> None:
    truncated = native_copy._NativeCopySource(stream_reader(b""), 1)
    with pytest.raises(ProviderDirectoryProjectionError):
        await truncated.__anext__()

    source = native_copy._NativeCopySource(stream_reader(b"x"), 1)
    source._prefix_bytes.extend(native_copy._PGCOPY_HEADER)
    assert await source.__anext__() == b"x"


class _WalCompressionDatabase:
    def __init__(self) -> None:
        self.scalar_calls = 0

    async def scalar(self, statement, **_parameters):
        self.scalar_calls += 1
        if "server_version_num" in statement:
            return 150000
        if "has_parameter_privilege" in statement:
            return True
        if "FROM pg_settings" in statement:
            return "invalid"
        raise AssertionError(statement)


@pytest.mark.asyncio
async def test_projection_wal_compression_rejects_unknown_server_algorithm(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION",
        "on",
    )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="compression_unavailable",
    ):
        await projection_db.set_local_projection_wal_compression(
            _WalCompressionDatabase()
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("use_factory", (False, True))
async def test_uhc_download_admission_uses_the_selected_connection_path(
    monkeypatch,
    tmp_path,
    use_factory,
) -> None:
    artifact_path = tmp_path / "provider.json"
    admit_file = AsyncMock()
    monkeypatch.setattr(
        uhc_acquisition,
        "_download_file",
        AsyncMock(return_value=(artifact_path, "a" * 64, 1)),
    )
    monkeypatch.setattr(
        uhc_acquisition,
        "_admit_downloaded_catalog_file",
        admit_file,
    )

    @asynccontextmanager
    async def connection_factory():
        yield "worker-connection"

    context = uhc_acquisition._CatalogAcquisitionContext(
        pipeline_semaphore=asyncio.Semaphore(1),
        admission_semaphore=asyncio.Semaphore(1),
        shared_connection_lock=asyncio.Lock(),
        connection="shared-connection",
        connection_factory=connection_factory if use_factory else None,
        active_session=object(),
        catalog_set_sha256="b" * 64,
        staged_paths=set(),
    )
    result = await uhc_acquisition._acquire_missing_catalog_file(
        context,
        {"file_id": "file-1"},
    )

    assert result[1:] == (artifact_path, "a" * 64, 1)
    expected_connection = (
        "worker-connection" if use_factory else "shared-connection"
    )
    assert admit_file.await_args.args[0] == expected_connection
