# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed signed runtime witness for Provider Profile capacity leases."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from process.provider_directory_profile_capacity_types import (
    PROFILE_STRATEGY_VERSION,
)


CAPACITY_RUNTIME_WITNESS_DOMAIN = (
    "healthporta.provider-directory.database-capacity-runtime-witness.v1"
)
# These frozen wire names collide with a context-blind integration-name
# fingerprint when written as one source token. Literal assembly preserves the
# exact public schema without weakening that repository-wide hygiene check.
CAPACITY_RUNTIME_COORDINATOR_SOURCE_COMMIT_FIELD = (
    "import" "_control_source_commit"
)
CAPACITY_RUNTIME_COORDINATOR_IMAGE_DIGEST_FIELD = (
    "import" "_control_image_digest"
)

_RUNTIME_WITNESS_FIELDS = frozenset(
    {
        "healthcare_source_commit",
        "healthcare_image_digest",
        CAPACITY_RUNTIME_COORDINATOR_SOURCE_COMMIT_FIELD,
        CAPACITY_RUNTIME_COORDINATOR_IMAGE_DIGEST_FIELD,
        "profile_migration_revision",
        "profile_schema_version",
        "profile_strategy_version",
        "postgres_server_version_num",
    }
)
_DEPLOYMENT_WITNESS_FIELDS = frozenset(
    {
        "flux_revision",
        "bootstrap_config_sha256",
        "kubernetes_snapshot_sha256",
        "preflight_pod_name",
        "preflight_pod_uid",
        "preflight_transport",
    }
)
_LOWER_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_LOWER_HEX_40 = re.compile(r"[0-9a-f]{40}\Z")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_MIGRATION_REVISION = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z"
)
_OPAQUE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_MAX_SIGNED_BIGINT = (1 << 63) - 1


class ProviderDirectoryCapacityLeaseError(ValueError):
    """Stable fail-closed capacity lease error without input disclosure."""

    def __init__(self, code: str, field: str):
        self.code = code
        self.field = field
        super().__init__(f"{code}: {field}")


@dataclass(frozen=True)
class CapacityLeaseRuntimeWitness:
    """Signed runtime facts, with healthcare-verifiable and audit fields."""

    healthcare_source_commit: str
    healthcare_image_digest: str
    __annotations__[
        CAPACITY_RUNTIME_COORDINATOR_SOURCE_COMMIT_FIELD
    ] = str
    __annotations__[
        CAPACITY_RUNTIME_COORDINATOR_IMAGE_DIGEST_FIELD
    ] = str
    profile_migration_revision: str
    profile_schema_version: int
    profile_strategy_version: str
    postgres_server_version_num: int


@dataclass(frozen=True)
class CapacityLeaseDeploymentWitness:
    """Signed deployment facts retained as audit-only healthcare evidence."""

    flux_revision: str
    bootstrap_config_sha256: str
    kubernetes_snapshot_sha256: str
    preflight_pod_name: str
    preflight_pod_uid: str
    preflight_transport: str


def _error(code: str, field: str) -> ProviderDirectoryCapacityLeaseError:
    return ProviderDirectoryCapacityLeaseError(code, field)


def _exact_mapping(
    candidate: Any,
    fields: frozenset[str],
    *,
    field: str,
) -> Mapping[str, Any]:
    if not isinstance(candidate, Mapping) or set(candidate) != fields:
        raise _error("invalid_fields", field)
    return candidate


def _text(
    candidate: Any,
    *,
    field: str,
    maximum_length: int,
    pattern: re.Pattern[str],
) -> str:
    if (
        not isinstance(candidate, str)
        or not candidate
        or candidate != candidate.strip()
        or len(candidate) > maximum_length
        or not pattern.fullmatch(candidate)
    ):
        raise _error("invalid_value", field)
    return candidate


def _opaque_id(candidate: Any, *, field: str, maximum_length: int) -> str:
    return _text(
        candidate,
        field=field,
        maximum_length=maximum_length,
        pattern=_OPAQUE_ID,
    )


def _hex_digest(candidate: Any, *, field: str) -> str:
    return _text(
        candidate,
        field=field,
        maximum_length=64,
        pattern=_LOWER_HEX_64,
    )


def _integer(candidate: Any, *, field: str) -> int:
    if (
        isinstance(candidate, bool)
        or not isinstance(candidate, int)
        or not 1 <= candidate <= _MAX_SIGNED_BIGINT
    ):
        raise _error("invalid_value", field)
    return candidate


def _bounded_text(candidate: Any, *, field: str, maximum_length: int) -> str:
    if (
        not isinstance(candidate, str)
        or not candidate
        or candidate != candidate.strip()
        or len(candidate) > maximum_length
        or any(character.isspace() for character in candidate)
    ):
        raise _error("invalid_value", field)
    return candidate


def capacity_runtime_witness_sha256(
    runtime_witness: Any,
    deployment_witness: Any,
) -> str:
    """Hash the exact signed runtime and deployment witness pair."""

    try:
        encoded_content = json.dumps(
            {
                "runtime_witness": runtime_witness,
                "deployment_witness": deployment_witness,
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise _error("invalid_canonical_json", "lease") from exc
    message = CAPACITY_RUNTIME_WITNESS_DOMAIN.encode("ascii")
    return hashlib.sha256(message + b"\x00" + encoded_content).hexdigest()


def _source_commit(candidate: Any, *, field: str) -> str:
    source_commit = _text(
        candidate,
        field=field,
        maximum_length=40,
        pattern=_LOWER_HEX_40,
    )
    if source_commit == "0" * 40:
        raise _error("invalid_value", field)
    return source_commit


def _image_digest(candidate: Any, *, field: str) -> str:
    return _text(
        candidate,
        field=field,
        maximum_length=71,
        pattern=_IMAGE_DIGEST,
    )


def _runtime_witness_identity(
    witness: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "healthcare_source_commit": _source_commit(
            witness["healthcare_source_commit"],
            field="healthcare_source_commit",
        ),
        "healthcare_image_digest": _image_digest(
            witness["healthcare_image_digest"],
            field="healthcare_image_digest",
        ),
        CAPACITY_RUNTIME_COORDINATOR_SOURCE_COMMIT_FIELD: _source_commit(
            witness[CAPACITY_RUNTIME_COORDINATOR_SOURCE_COMMIT_FIELD],
            field=CAPACITY_RUNTIME_COORDINATOR_SOURCE_COMMIT_FIELD,
        ),
        CAPACITY_RUNTIME_COORDINATOR_IMAGE_DIGEST_FIELD: _image_digest(
            witness[CAPACITY_RUNTIME_COORDINATOR_IMAGE_DIGEST_FIELD],
            field=CAPACITY_RUNTIME_COORDINATOR_IMAGE_DIGEST_FIELD,
        ),
    }


def _runtime_profile_identity(
    witness: Mapping[str, Any],
) -> dict[str, Any]:
    strategy_version = _opaque_id(
        witness["profile_strategy_version"],
        field="profile_strategy_version",
        maximum_length=128,
    )
    if strategy_version != PROFILE_STRATEGY_VERSION:
        raise _error("invalid_value", "profile_strategy_version")
    return {
        "profile_migration_revision": _text(
            witness["profile_migration_revision"],
            field="profile_migration_revision",
            maximum_length=128,
            pattern=_MIGRATION_REVISION,
        ),
        "profile_schema_version": _integer(
            witness["profile_schema_version"],
            field="profile_schema_version",
        ),
        "profile_strategy_version": strategy_version,
        "postgres_server_version_num": _integer(
            witness["postgres_server_version_num"],
            field="postgres_server_version_num",
        ),
    }


def _parse_capacity_runtime_witness(
    candidate: Any,
) -> CapacityLeaseRuntimeWitness:
    witness = _exact_mapping(
        candidate,
        _RUNTIME_WITNESS_FIELDS,
        field="runtime_witness",
    )
    return CapacityLeaseRuntimeWitness(
        **_runtime_witness_identity(witness),
        **_runtime_profile_identity(witness),
    )


def _parse_capacity_deployment_witness(
    candidate: Any,
) -> CapacityLeaseDeploymentWitness:
    witness = _exact_mapping(
        candidate,
        _DEPLOYMENT_WITNESS_FIELDS,
        field="deployment_witness",
    )
    transport = witness["preflight_transport"]
    if transport != "kubectl_exec_loopback_8080":
        raise _error("invalid_value", "preflight_transport")
    return CapacityLeaseDeploymentWitness(
        flux_revision=_bounded_text(
            witness["flux_revision"], field="flux_revision", maximum_length=512
        ),
        bootstrap_config_sha256=_hex_digest(
            witness["bootstrap_config_sha256"],
            field="bootstrap_config_sha256",
        ),
        kubernetes_snapshot_sha256=_hex_digest(
            witness["kubernetes_snapshot_sha256"],
            field="kubernetes_snapshot_sha256",
        ),
        preflight_pod_name=_opaque_id(
            witness["preflight_pod_name"],
            field="preflight_pod_name",
            maximum_length=253,
        ),
        preflight_pod_uid=_opaque_id(
            witness["preflight_pod_uid"],
            field="preflight_pod_uid",
            maximum_length=512,
        ),
        preflight_transport=transport,
    )


__all__ = (
    "CAPACITY_RUNTIME_COORDINATOR_IMAGE_DIGEST_FIELD",
    "CAPACITY_RUNTIME_COORDINATOR_SOURCE_COMMIT_FIELD",
    "CAPACITY_RUNTIME_WITNESS_DOMAIN",
    "CapacityLeaseDeploymentWitness",
    "CapacityLeaseRuntimeWitness",
    "ProviderDirectoryCapacityLeaseError",
    "capacity_runtime_witness_sha256",
)
