# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed runtime-observation tests for Profile capacity preflight."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from process import provider_directory_profile_runtime_observation as runtime
from process.provider_directory_profile_capacity_attestation_contract import (
    capacity_runtime_witness_sha256,
)
from tests.test_provider_directory_profile_capacity_preflight import _geometry
from tests.test_provider_directory_profile_capacity_attestation import (
    _signed_envelope,
    _verify,
)


@pytest.fixture(autouse=True)
def baked_source_commit_file(tmp_path, monkeypatch):
    source_file = tmp_path / "healthcare-source-commit"
    monkeypatch.setattr(
        runtime, "PROFILE_RUNTIME_SOURCE_COMMIT_FILE", source_file
    )
    monkeypatch.setattr(runtime, "_BUILD_IDENTITY_UID", os.getuid())
    monkeypatch.setattr(runtime, "_BUILD_IDENTITY_GID", os.getgid())

    def write_source_file(raw_value: bytes | None = b"b" * 40 + b"\n") -> Path:
        if source_file.is_symlink() or source_file.exists():
            source_file.unlink()
        if raw_value is not None:
            source_file.write_bytes(raw_value)
            source_file.chmod(0o444)
        return source_file

    write_source_file()
    return write_source_file


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_source_commit",
    [
        None,
        b"",
        b"0" * 40 + b"\n",
        b"A" * 40 + b"\n",
        b"a" * 39 + b"\n",
        b"a" * 40,
        b"a" * 41 + b"\n",
    ],
)
async def test_runtime_observation_rejects_unbaked_source_commit(
    monkeypatch,
    baked_source_commit_file,
    raw_source_commit,
):
    baked_source_commit_file(raw_source_commit)
    rows = AsyncMock()
    monkeypatch.setattr(runtime.db, "all", rows)

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="healthcare_source_commit_invalid",
    ):
        await runtime.observe_profile_runtime()
    rows.assert_not_awaited()


def test_runtime_source_commit_ignores_conflicting_environment(monkeypatch):
    monkeypatch.setenv("HLTHPRT_SOURCE_COMMIT", "e" * 40)

    assert runtime.build_baked_healthcare_source_commit() == "b" * 40


@pytest.mark.parametrize("unsafe_shape", ["mode", "hardlink", "symlink"])
def test_runtime_source_commit_rejects_unsafe_file_shape(
    tmp_path,
    baked_source_commit_file,
    unsafe_shape,
):
    source_file = baked_source_commit_file()
    if unsafe_shape == "mode":
        source_file.chmod(0o644)
    elif unsafe_shape == "hardlink":
        os.link(source_file, tmp_path / "second-link")
    else:
        target = tmp_path / "target"
        target.write_bytes(b"b" * 40 + b"\n")
        source_file.unlink()
        source_file.symlink_to(target)

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="healthcare_source_commit_invalid",
    ):
        runtime.build_baked_healthcare_source_commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("migration_rows", [[], [{}, {}]])
async def test_runtime_observation_requires_one_migration_row(
    monkeypatch,
    migration_rows,
):
    monkeypatch.setattr(
        runtime.db,
        "all",
        AsyncMock(return_value=migration_rows),
    )

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="profile_migration_revision_cardinality_invalid",
    ):
        await runtime.observe_profile_runtime()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("migration_row", "reason"),
    [
        (object(), "database_row_invalid"),
        ({}, "database_row_fields_invalid"),
        (
            {
                "profile_migration_revision": "not-a-revision",
                "postgres_server_version_num": 180002,
            },
            "profile_migration_revision_invalid",
        ),
        (
            {
                "profile_migration_revision": (
                    "20260730110000_provider_directory_profile_delta"
                ),
                "postgres_server_version_num": True,
            },
            "postgres_server_version_num_invalid",
        ),
    ],
)
async def test_runtime_observation_rejects_invalid_database_identity(
    monkeypatch,
    migration_row,
    reason,
):
    monkeypatch.setattr(
        runtime.db,
        "all",
        AsyncMock(return_value=[migration_row]),
    )

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match=reason,
    ):
        await runtime.observe_profile_runtime()


@pytest.mark.asyncio
async def test_runtime_observation_maps_database_failure(monkeypatch):
    monkeypatch.setattr(
        runtime.db,
        "all",
        AsyncMock(side_effect=SQLAlchemyError("unavailable")),
    )

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="profile_migration_revision_unavailable",
    ):
        await runtime.observe_profile_runtime()


@pytest.mark.asyncio
async def test_runtime_observation_uses_configured_database_schema(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", 'profile"runtime')
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    runtime_rows = AsyncMock(
        return_value=[
            {
                "profile_migration_revision": (
                    "20260801130000_provider_directory_capacity_lease_v2"
                ),
                "postgres_server_version_num": 180002,
            }
        ]
    )
    monkeypatch.setattr(runtime.db, "all", runtime_rows)

    observation = await runtime.observe_profile_runtime()

    assert observation["profile_migration_revision"] == (
        "20260801130000_provider_directory_capacity_lease_v2"
    )
    runtime_rows.assert_awaited_once()
    assert 'FROM "profile""runtime"."alembic_version"' in (
        runtime_rows.await_args.args[0]
    )


@pytest.mark.asyncio
async def test_runtime_observation_rejects_conflicting_database_schemas(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "profile_runtime")
    monkeypatch.setenv("DB_SCHEMA", "legacy_runtime")
    runtime_rows = AsyncMock()
    monkeypatch.setattr(runtime.db, "all", runtime_rows)

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="database_schema_mismatch",
    ):
        await runtime.observe_profile_runtime()
    runtime_rows.assert_not_awaited()


@pytest.mark.parametrize(
    ("constant_name", "replacement", "reason"),
    [
        ("PROFILE_SCHEMA_VERSION", 0, "profile_schema_version_invalid"),
        (
            "PROFILE_BUILD_STRATEGY_VERSION",
            "legacy-profile-strategy",
            "profile_strategy_version_invalid",
        ),
    ],
)
def test_runtime_observation_rejects_invalid_source_profile_identity(
    monkeypatch,
    constant_name,
    replacement,
    reason,
):
    monkeypatch.setattr(
        runtime.profile_artifact,
        constant_name,
        replacement,
    )

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match=reason,
    ):
        runtime._source_profile_identity()


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("profile_schema_version", 2),
        ("profile_strategy_version", "legacy-profile-strategy"),
        ("postgres_server_version_num", 180099),
    ],
)
def test_runtime_observation_rejects_geometry_disagreement(
    field_name,
    replacement,
):
    geometry = _geometry()
    observation_by_field = {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        "healthcare_source_commit": "c" * 40,
        "profile_migration_revision": (
            "20260730110000_provider_directory_profile_delta"
        ),
        "profile_schema_version": geometry.profile_schema_version,
        "profile_strategy_version": geometry.profile_strategy_version,
        "postgres_server_version_num": (
            geometry.postgres_server_version_num
        ),
    }
    observation_by_field[field_name] = replacement

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match=f"{field_name}_geometry_mismatch",
    ):
        runtime.assert_runtime_observation_matches_geometry(
            observation_by_field,
            geometry,
        )


@pytest.mark.parametrize(
    "observation_by_field",
    [None, {}, {"contract_id": "unexpected"}],
)
def test_runtime_observation_requires_closed_fields(observation_by_field):
    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="fields_invalid",
    ):
        runtime.assert_runtime_observation_matches_geometry(
            observation_by_field,
            _geometry(),
        )


def _lease_runtime_observation(verified_lease):
    witness = verified_lease.runtime_witness
    return {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        "healthcare_source_commit": witness.healthcare_source_commit,
        "profile_migration_revision": witness.profile_migration_revision,
        "profile_schema_version": witness.profile_schema_version,
        "profile_strategy_version": witness.profile_strategy_version,
        "postgres_server_version_num": witness.postgres_server_version_num,
    }


def test_capacity_lease_rejects_cross_runtime_replay():
    verified_lease = _verify()
    observation = _lease_runtime_observation(verified_lease)
    runtime.assert_capacity_lease_matches_runtime_observation(
        verified_lease,
        observation,
    )

    for field_name, drifted_value in (
        ("healthcare_source_commit", "f" * 40),
        ("profile_migration_revision", "next_migration"),
        ("profile_schema_version", 2),
        ("profile_strategy_version", "next_strategy"),
        ("postgres_server_version_num", 180099),
    ):
        drifted_observation_by_field = {
            **observation,
            field_name: drifted_value,
        }
        with pytest.raises(
            runtime.ProviderDirectoryProfileRuntimeObservationError,
            match="capacity_lease_runtime_mismatch",
        ):
            runtime.assert_capacity_lease_matches_runtime_observation(
                verified_lease,
                drifted_observation_by_field,
            )


def test_capacity_lease_deployment_fields_are_explicitly_audit_only():
    def mutate(body):
        body["runtime_witness"]["healthcare_image_digest"] = (
            "sha256:" + "ab" * 32
        )
        body["deployment_witness"]["flux_revision"] = (
            "main@sha1:" + "cd" * 20
        )
        body["runtime_witness_sha256"] = (
            capacity_runtime_witness_sha256(
                body["runtime_witness"],
                body["deployment_witness"],
            )
        )

    verified_lease = _verify(_signed_envelope(body_mutator=mutate))
    runtime.assert_capacity_lease_matches_runtime_observation(
        verified_lease,
        _lease_runtime_observation(verified_lease),
    )


def test_runtime_source_commit_is_baked_into_the_image():
    dockerfile = (
        Path(__file__).resolve().parents[1] / "Dockerfile"
    ).read_text(encoding="utf-8")
    assert "ARG HLTHPRT_SOURCE_COMMIT" in dockerfile
    assert 'test "${#HLTHPRT_SOURCE_COMMIT}" -eq 40' in dockerfile
    assert "grep -Eq '^[0-9a-f]{40}$'" in dockerfile
    assert (
        'test "${HLTHPRT_SOURCE_COMMIT}" != "' + "0" * 40 + '"'
    ) in dockerfile
    assert "/opt/healthporta/build-identity/healthcare-source-commit" in dockerfile
    assert "chown root:root" in dockerfile
    assert "chmod 0444" in dockerfile
    assert "ENV HLTHPRT_SOURCE_COMMIT=" not in dockerfile
