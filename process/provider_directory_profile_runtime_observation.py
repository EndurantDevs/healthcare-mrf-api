# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed runtime identity for Provider Directory Profile capacity proofs."""

from __future__ import annotations

import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping

from sqlalchemy.exc import SQLAlchemyError

from db.models import db
from process import provider_directory_profile as profile_artifact
from process.provider_directory_profile_capacity_attestation_contract import (
    VerifiedDatabaseCapacityLease,
)
from process.provider_directory_profile_capacity_runtime_witness import (
    CAPACITY_RUNTIME_CONTROL_PLANE_IMAGE_DIGEST_FIELD,
    CAPACITY_RUNTIME_CONTROL_PLANE_SOURCE_COMMIT_FIELD,
)
from process.provider_directory_profile_capacity_types import (
    PROFILE_STRATEGY_VERSION,
)


PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-runtime-observation.v1"
)
PROFILE_RUNTIME_SOURCE_COMMIT_FILE = Path(
    "/opt/healthporta/build-identity/healthcare-source-commit"
)
_SOURCE_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_MIGRATION_REVISION_PATTERN = re.compile(r"^[0-9a-z_]{1,128}$")
_BUILD_IDENTITY_UID = 0
_BUILD_IDENTITY_GID = 0
_BUILD_IDENTITY_MODE = 0o444
_BUILD_IDENTITY_SIZE = 41
_RUNTIME_OBSERVATION_FIELDS = frozenset(
    {
        "contract_id",
        "healthcare_source_commit",
        "profile_migration_revision",
        "profile_schema_version",
        "profile_strategy_version",
        "postgres_server_version_num",
    }
)
CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS = (
    "healthcare_source_commit",
    "profile_migration_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "postgres_server_version_num",
)
CAPACITY_LEASE_AUDIT_ONLY_RUNTIME_FIELDS = (
    "healthcare_image_digest",
    CAPACITY_RUNTIME_CONTROL_PLANE_SOURCE_COMMIT_FIELD,
    CAPACITY_RUNTIME_CONTROL_PLANE_IMAGE_DIGEST_FIELD,
)
CAPACITY_LEASE_AUDIT_ONLY_DEPLOYMENT_FIELDS = (
    "flux_revision",
    "bootstrap_config_sha256",
    "kubernetes_snapshot_sha256",
    "preflight_pod_name",
    "preflight_pod_uid",
    "preflight_transport",
)


class ProviderDirectoryProfileRuntimeObservationError(RuntimeError):
    """Raised when the Profile runtime cannot prove one exact identity."""


def _runtime_observation_error(
    reason: str,
) -> ProviderDirectoryProfileRuntimeObservationError:
    return ProviderDirectoryProfileRuntimeObservationError(
        f"provider_directory_profile_runtime_observation_{reason}"
    )


def _database_schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise _runtime_observation_error("database_schema_mismatch")
    return runtime_schema or legacy_schema or "mrf"


def _quoted_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def profile_runtime_observation_sql() -> str:
    """Return the runtime-identity query for the configured schema."""

    schema = _quoted_identifier(_database_schema())
    return f"""
        SELECT migration.version_num::text AS profile_migration_revision,
               current_setting('server_version_num')::integer
                   AS postgres_server_version_num
          FROM {schema}."alembic_version" AS migration
         ORDER BY migration.version_num;
    """


def build_baked_healthcare_source_commit() -> str:
    """Return the exact image-baked source commit or fail closed."""

    descriptor: int | None = None
    try:
        path_stat = PROFILE_RUNTIME_SOURCE_COMMIT_FILE.lstat()
        _validate_build_identity_file_stat(path_stat)
        descriptor = _open_build_identity_file(PROFILE_RUNTIME_SOURCE_COMMIT_FILE)
        opened_stat = os.fstat(descriptor)
        _validate_build_identity_file_stat(opened_stat)
        _require_same_build_identity_file(path_stat, opened_stat)
        raw_source_commit = _read_build_identity_file(descriptor)
        final_path_stat = PROFILE_RUNTIME_SOURCE_COMMIT_FILE.lstat()
        final_opened_stat = os.fstat(descriptor)
        _require_same_build_identity_file(path_stat, final_path_stat)
        _require_same_build_identity_file(path_stat, final_opened_stat)
        source_commit = raw_source_commit.decode("ascii").removesuffix("\n")
    except (OSError, UnicodeDecodeError) as exc:
        raise _runtime_observation_error("healthcare_source_commit_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if (
        len(raw_source_commit) != _BUILD_IDENTITY_SIZE
        or not raw_source_commit.endswith(b"\n")
        or not _SOURCE_COMMIT_PATTERN.fullmatch(source_commit)
        or source_commit == "0" * 40
    ):
        raise _runtime_observation_error("healthcare_source_commit_invalid")
    return source_commit


def _validate_build_identity_file_stat(file_stat: os.stat_result) -> None:
    if (
        not stat.S_ISREG(file_stat.st_mode)
        or stat.S_IMODE(file_stat.st_mode) != _BUILD_IDENTITY_MODE
        or file_stat.st_uid != _BUILD_IDENTITY_UID
        or file_stat.st_gid != _BUILD_IDENTITY_GID
        or file_stat.st_nlink != 1
        or file_stat.st_size != _BUILD_IDENTITY_SIZE
    ):
        raise _runtime_observation_error("healthcare_source_commit_invalid")


def _open_build_identity_file(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    return os.open(path, flags)


def _read_build_identity_file(descriptor: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < _BUILD_IDENTITY_SIZE:
        chunk = os.read(descriptor, _BUILD_IDENTITY_SIZE - len(chunks))
        if not chunk:
            break
        chunks.extend(chunk)
    if os.read(descriptor, 1):
        raise _runtime_observation_error("healthcare_source_commit_invalid")
    return bytes(chunks)


def _require_same_build_identity_file(
    expected: os.stat_result,
    observed: os.stat_result,
) -> None:
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_nlink",
        "st_uid",
        "st_gid",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(
        getattr(expected, field_name) != getattr(observed, field_name)
        for field_name in stable_fields
    ):
        raise _runtime_observation_error("healthcare_source_commit_invalid")


def _runtime_observation_row_mapping(row: Any) -> Mapping[str, Any]:
    if hasattr(row, "_mapping"):
        row = row._mapping
    if not isinstance(row, Mapping):
        raise _runtime_observation_error("database_row_invalid")
    if set(row) != {
        "profile_migration_revision",
        "postgres_server_version_num",
    }:
        raise _runtime_observation_error("database_row_fields_invalid")
    return row


def _validated_migration_revision(value: Any) -> str:
    if not isinstance(value, str) or not _MIGRATION_REVISION_PATTERN.fullmatch(value):
        raise _runtime_observation_error("profile_migration_revision_invalid")
    return value


def _validated_postgres_server_version_num(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise _runtime_observation_error("postgres_server_version_num_invalid")
    return value


def _source_profile_identity() -> tuple[int, str]:
    schema_version = profile_artifact.PROFILE_SCHEMA_VERSION
    strategy_version = profile_artifact.PROFILE_BUILD_STRATEGY_VERSION
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version <= 0
    ):
        raise _runtime_observation_error("profile_schema_version_invalid")
    if strategy_version != PROFILE_STRATEGY_VERSION:
        raise _runtime_observation_error("profile_strategy_version_invalid")
    return schema_version, strategy_version


async def observe_profile_runtime(database: Any = db) -> dict[str, Any]:
    """Observe source and PostgreSQL identity in the caller's transaction."""

    source_commit = build_baked_healthcare_source_commit()
    try:
        migration_rows = await database.all(profile_runtime_observation_sql())
    except SQLAlchemyError as exc:
        raise _runtime_observation_error(
            "profile_migration_revision_unavailable"
        ) from exc
    if not isinstance(migration_rows, (list, tuple)) or len(migration_rows) != 1:
        raise _runtime_observation_error(
            "profile_migration_revision_cardinality_invalid"
        )
    migration_row = _runtime_observation_row_mapping(migration_rows[0])
    schema_version, strategy_version = _source_profile_identity()
    return {
        "contract_id": PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        "healthcare_source_commit": source_commit,
        "profile_migration_revision": _validated_migration_revision(
            migration_row["profile_migration_revision"]
        ),
        "profile_schema_version": schema_version,
        "profile_strategy_version": strategy_version,
        "postgres_server_version_num": (
            _validated_postgres_server_version_num(
                migration_row["postgres_server_version_num"]
            )
        ),
    }


def assert_runtime_observation_matches_geometry(
    observation: Mapping[str, Any],
    geometry: Any,
) -> None:
    """Reject a receipt whose observed runtime and plan disagree."""

    if (
        not isinstance(observation, Mapping)
        or set(observation) != _RUNTIME_OBSERVATION_FIELDS
        or observation.get("contract_id") != PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID
    ):
        raise _runtime_observation_error("fields_invalid")
    expected_by_field = {
        "profile_schema_version": getattr(
            geometry,
            "profile_schema_version",
            None,
        ),
        "profile_strategy_version": getattr(
            geometry,
            "profile_strategy_version",
            None,
        ),
        "postgres_server_version_num": getattr(
            geometry,
            "postgres_server_version_num",
            None,
        ),
    }
    for field_name, expected_value in expected_by_field.items():
        if observation.get(field_name) != expected_value:
            raise _runtime_observation_error(f"{field_name}_geometry_mismatch")


def assert_capacity_lease_matches_runtime_observation(
    lease: VerifiedDatabaseCapacityLease,
    observation: Mapping[str, Any],
) -> None:
    """Reject a signed lease observed on a different local runtime."""

    if not isinstance(lease, VerifiedDatabaseCapacityLease):
        raise _runtime_observation_error("capacity_lease_invalid")
    if (
        not isinstance(observation, Mapping)
        or set(observation) != _RUNTIME_OBSERVATION_FIELDS
        or observation.get("contract_id") != PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID
    ):
        raise _runtime_observation_error("fields_invalid")
    witness = lease.runtime_witness
    expected_by_field = {
        field_name: getattr(witness, field_name)
        for field_name in CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
    }
    observed_by_field = {
        field_name: observation.get(field_name)
        for field_name in CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
    }
    if expected_by_field != observed_by_field:
        raise _runtime_observation_error("capacity_lease_runtime_mismatch")
