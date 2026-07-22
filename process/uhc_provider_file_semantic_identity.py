# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Future plan, network, and dataset-build keys for UHC official files."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from process.uhc_provider_file_identity import (
    UHCLogicalScope,
    UHCProviderFileIdentityError,
    UHCSourceFileDescriptor,
    UHCSourceFileIdentity,
    _sha256_json,
    logical_scope_for_file,
)


PLAN_KEY_CONTRACT = "healthporta-uhc-plan-key-v1"
NETWORK_KEY_CONTRACT = "healthporta-uhc-network-key-v1"
DATASET_BUILD_CONTRACT = "healthporta-uhc-dataset-build-identity-v1"
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass(frozen=True)
class UHCPlanKey:
    scope: UHCLogicalScope
    plan_id_type: str
    plan_id: str
    plan_year: int
    plan_key_id: str

    @property
    def logical_scope_id(self) -> str:
        """Return the canonical scope digest retained by this plan key."""

        return self.scope.logical_scope_id

    @property
    def family(self) -> str:
        """Return the source family retained in the canonical scope."""

        return self.scope.family

    @property
    def market(self) -> str:
        """Return the normalized market retained in the canonical scope."""

        return self.scope.market


@dataclass(frozen=True)
class UHCNetworkKey:
    plan_key: UHCPlanKey
    network_tier: str
    network_key_id: str

    @property
    def logical_scope_id(self) -> str:
        """Return the canonical scope digest retained by the plan key."""

        return self.plan_key.logical_scope_id

    @property
    def family(self) -> str:
        """Return the source family retained by the plan key."""

        return self.plan_key.family

    @property
    def market(self) -> str:
        """Return the normalized market retained by the plan key."""

        return self.plan_key.market

    @property
    def plan_id_type(self) -> str:
        """Return the identifier namespace retained by the plan key."""

        return self.plan_key.plan_id_type

    @property
    def plan_id(self) -> str:
        """Return the exact plan identifier retained by the plan key."""

        return self.plan_key.plan_id

    @property
    def plan_year(self) -> int:
        """Return the plan year retained by the plan key."""

        return self.plan_key.plan_year


@dataclass(frozen=True)
class UHCArtifactDigest:
    source_file_id: str
    artifact_sha256: str


@dataclass(frozen=True)
class UHCDatasetBuildIdentity:
    adapter_version: str
    transform_version: str
    catalog_set_sha256: str
    logical_scopes: tuple[UHCLogicalScope, ...]
    artifact_digests: tuple[UHCArtifactDigest, ...]
    as_of: str
    plan_years: tuple[int, ...]
    dataset_build_id: str


def plan_key_for_scope(
    scope: UHCLogicalScope,
    *,
    plan_id_type: str,
    plan_id: str,
    plan_year: int,
) -> UHCPlanKey:
    """Build a plan key bound to the normalized scope, family, and market."""

    _validate_scope_identity(scope)
    _validate_key_text(plan_id_type, "plan_id_type", _VERSION_PATTERN)
    _validate_key_text(plan_id, "plan_id")
    _validate_plan_year(plan_year)
    identity_by_field = _plan_key_fields(scope, plan_id_type, plan_id, plan_year)
    return UHCPlanKey(
        scope=scope,
        plan_id_type=plan_id_type,
        plan_id=plan_id,
        plan_year=plan_year,
        plan_key_id=_sha256_json(identity_by_field),
    )


def network_key_for_plan(plan_key: UHCPlanKey, *, network_tier: str) -> UHCNetworkKey:
    """Build a network key that adds exact tier identity to its plan key."""

    _validate_plan_key_identity(plan_key)
    _validate_key_text(network_tier, "network_tier")
    identity_by_field = _network_key_fields(plan_key, network_tier)
    return UHCNetworkKey(
        plan_key=plan_key,
        network_tier=network_tier,
        network_key_id=_sha256_json(identity_by_field),
    )


def _plan_key_fields(
    scope: UHCLogicalScope,
    plan_id_type: str,
    plan_id: str,
    plan_year: int,
) -> dict[str, str | int]:
    return {
        "contract": PLAN_KEY_CONTRACT,
        "logical_scope_id": scope.logical_scope_id,
        "family": scope.family,
        "market": scope.market,
        "plan_id_type": plan_id_type,
        "plan_id": plan_id,
        "plan_year": plan_year,
    }


def _network_key_fields(
    plan_key: UHCPlanKey,
    network_tier: str,
) -> dict[str, str | int]:
    return {
        "contract": NETWORK_KEY_CONTRACT,
        "logical_scope_id": plan_key.logical_scope_id,
        "family": plan_key.family,
        "market": plan_key.market,
        "plan_id_type": plan_key.plan_id_type,
        "plan_id": plan_key.plan_id,
        "plan_year": plan_key.plan_year,
        "network_tier": network_tier,
    }


def _validate_key_text(
    value: Any,
    label: str,
    pattern: re.Pattern[str] | None = None,
) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or not value.isprintable()
        or len(value) > 256
        or (pattern is not None and not pattern.fullmatch(value))
    ):
        raise UHCProviderFileIdentityError(f"{label} is invalid")


def _validate_plan_year(plan_year: Any) -> None:
    if (
        isinstance(plan_year, bool)
        or not isinstance(plan_year, int)
        or not 2000 <= plan_year <= 9999
    ):
        raise UHCProviderFileIdentityError("plan_year is invalid")


def _validate_scope_identity(scope: Any) -> None:
    if (
        type(scope) is not UHCLogicalScope
        or type(scope.source_file) is not UHCSourceFileIdentity
    ):
        raise UHCProviderFileIdentityError("logical scope type is invalid")
    source_file = scope.source_file
    try:
        expected_scope = logical_scope_for_file(
            UHCSourceFileDescriptor(
                source_file.family,
                source_file.collection_kind,
                source_file.file_name,
            )
        )
    except UHCProviderFileIdentityError as error:
        raise UHCProviderFileIdentityError(
            "logical scope identity is invalid"
        ) from error
    if scope != expected_scope:
        raise UHCProviderFileIdentityError("logical scope identity is invalid")


def _validate_plan_key_identity(plan_key: Any) -> None:
    if type(plan_key) is not UHCPlanKey:
        raise UHCProviderFileIdentityError("plan key type is invalid")
    _validate_scope_identity(plan_key.scope)
    _validate_key_text(plan_key.plan_id_type, "plan_id_type", _VERSION_PATTERN)
    _validate_key_text(plan_key.plan_id, "plan_id")
    _validate_plan_year(plan_key.plan_year)
    _require_digest(plan_key.plan_key_id, "plan_key_id")
    identity_by_field = _plan_key_fields(
        plan_key.scope,
        plan_key.plan_id_type,
        plan_key.plan_id,
        plan_key.plan_year,
    )
    if plan_key.plan_key_id != _sha256_json(identity_by_field):
        raise UHCProviderFileIdentityError("plan key identity is invalid")


def validate_plan_key_identity(plan_key: UHCPlanKey) -> UHCPlanKey:
    """Reject a directly constructed plan key unless every field is canonical."""

    _validate_plan_key_identity(plan_key)
    return plan_key


def validate_network_key_identity(network_key: UHCNetworkKey) -> UHCNetworkKey:
    """Reject a directly constructed network key unless it is fully canonical."""

    if type(network_key) is not UHCNetworkKey:
        raise UHCProviderFileIdentityError("network key type is invalid")
    _validate_plan_key_identity(network_key.plan_key)
    _validate_key_text(network_key.network_tier, "network_tier")
    _require_digest(network_key.network_key_id, "network_key_id")
    identity_by_field = _network_key_fields(
        network_key.plan_key,
        network_key.network_tier,
    )
    if network_key.network_key_id != _sha256_json(identity_by_field):
        raise UHCProviderFileIdentityError("network key identity is invalid")
    return network_key


def dataset_build_identity(
    *,
    adapter_version: str,
    transform_version: str,
    catalog_set_sha256: str,
    logical_scopes: Iterable[UHCLogicalScope],
    artifact_digests: Iterable[UHCArtifactDigest],
    as_of: str,
    plan_years: Iterable[int],
) -> UHCDatasetBuildIdentity:
    """Bind a future build to versions, census, scopes, artifacts, and time."""

    _validate_key_text(adapter_version, "adapter_version", _VERSION_PATTERN)
    _validate_key_text(transform_version, "transform_version", _VERSION_PATTERN)
    _require_digest(catalog_set_sha256, "catalog_set_sha256")
    _validate_as_of_date(as_of)
    ordered_plan_years = _validated_plan_years(plan_years)
    ordered_scopes = _validated_build_scopes(logical_scopes)
    ordered_artifacts = _validated_artifact_digests(artifact_digests, ordered_scopes)
    identity_by_field = {
        "contract": DATASET_BUILD_CONTRACT,
        "adapter_version": adapter_version,
        "transform_version": transform_version,
        "catalog_set_sha256": catalog_set_sha256,
        "logical_scopes": [_scope_build_fields(scope) for scope in ordered_scopes],
        "artifact_digests": [
            {
                "source_file_id": artifact.source_file_id,
                "artifact_sha256": artifact.artifact_sha256,
            }
            for artifact in ordered_artifacts
        ],
        "as_of": as_of,
        "plan_years": list(ordered_plan_years),
    }
    return UHCDatasetBuildIdentity(
        adapter_version=adapter_version,
        transform_version=transform_version,
        catalog_set_sha256=catalog_set_sha256,
        logical_scopes=ordered_scopes,
        artifact_digests=ordered_artifacts,
        as_of=as_of,
        plan_years=ordered_plan_years,
        dataset_build_id=_sha256_json(identity_by_field),
    )


def validate_dataset_build_identity(
    build_identity: UHCDatasetBuildIdentity,
) -> UHCDatasetBuildIdentity:
    """Reject a directly constructed build unless it equals canonical output."""

    if type(build_identity) is not UHCDatasetBuildIdentity:
        raise UHCProviderFileIdentityError("dataset build identity type is invalid")
    _require_digest(build_identity.dataset_build_id, "dataset_build_id")
    canonical_identity = dataset_build_identity(
        adapter_version=build_identity.adapter_version,
        transform_version=build_identity.transform_version,
        catalog_set_sha256=build_identity.catalog_set_sha256,
        logical_scopes=build_identity.logical_scopes,
        artifact_digests=build_identity.artifact_digests,
        as_of=build_identity.as_of,
        plan_years=build_identity.plan_years,
    )
    if build_identity != canonical_identity:
        raise UHCProviderFileIdentityError("dataset build identity is invalid")
    return build_identity


def _validated_plan_years(plan_years: Iterable[int]) -> tuple[int, ...]:
    try:
        years = tuple(plan_years)
    except TypeError as error:
        raise UHCProviderFileIdentityError("plan_years are invalid") from error
    if not years:
        raise UHCProviderFileIdentityError("plan_years are invalid")
    for year in years:
        _validate_plan_year(year)
    if len(years) != len(set(years)):
        raise UHCProviderFileIdentityError("plan_years are invalid")
    return tuple(sorted(years))


def _validated_build_scopes(
    logical_scopes: Iterable[UHCLogicalScope],
) -> tuple[UHCLogicalScope, ...]:
    try:
        scopes = tuple(logical_scopes)
    except TypeError as error:
        raise UHCProviderFileIdentityError(
            "dataset build logical scopes are invalid"
        ) from error
    if not scopes:
        raise UHCProviderFileIdentityError("dataset build has no logical scopes")
    for scope in scopes:
        _validate_scope_identity(scope)
    source_file_ids = [scope.source_file.source_file_id for scope in scopes]
    if len(source_file_ids) != len(set(source_file_ids)):
        raise UHCProviderFileIdentityError("dataset build repeats a source file")
    return tuple(sorted(scopes, key=_scope_build_sort_key))


def _scope_build_sort_key(scope: UHCLogicalScope) -> tuple[str, str, str]:
    return scope.logical_scope_id, scope.collection_kind, scope.source_file.source_file_id


def _scope_build_fields(scope: UHCLogicalScope) -> dict[str, str | None]:
    return {
        "logical_scope_id": scope.logical_scope_id,
        "family": scope.family,
        "market": scope.market,
        "product": scope.product,
        "jurisdiction": scope.jurisdiction,
        "collection_kind": scope.collection_kind,
        "source_file_id": scope.source_file.source_file_id,
    }


def _validated_artifact_digests(
    artifact_digests: Iterable[UHCArtifactDigest],
    scopes: Sequence[UHCLogicalScope],
) -> tuple[UHCArtifactDigest, ...]:
    try:
        artifacts = tuple(artifact_digests)
    except TypeError as error:
        raise UHCProviderFileIdentityError(
            "dataset build artifact digests are invalid"
        ) from error
    if any(type(artifact) is not UHCArtifactDigest for artifact in artifacts):
        raise UHCProviderFileIdentityError("artifact digest type is invalid")
    for artifact in artifacts:
        _require_digest(artifact.source_file_id, "artifact source_file_id")
        _require_digest(artifact.artifact_sha256, "artifact_sha256")
    source_file_ids = [artifact.source_file_id for artifact in artifacts]
    if len(source_file_ids) != len(set(source_file_ids)):
        raise UHCProviderFileIdentityError("dataset build repeats an artifact digest")
    expected_source_file_ids = {scope.source_file.source_file_id for scope in scopes}
    if set(source_file_ids) != expected_source_file_ids:
        raise UHCProviderFileIdentityError("artifact digests do not match source files")
    return tuple(sorted(artifacts, key=lambda artifact: artifact.source_file_id))


def _require_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST_PATTERN.fullmatch(value):
        raise UHCProviderFileIdentityError(
            f"{label} is not a lowercase SHA-256 digest"
        )
    return value


def _validate_as_of_date(value: Any) -> None:
    if not isinstance(value, str):
        raise UHCProviderFileIdentityError("as_of is not an ISO date")
    try:
        parsed_date = dt.date.fromisoformat(value)
    except ValueError as error:
        raise UHCProviderFileIdentityError("as_of is not an ISO date") from error
    if parsed_date.isoformat() != value:
        raise UHCProviderFileIdentityError("as_of is not an ISO date")


__all__ = [
    "UHCArtifactDigest",
    "UHCDatasetBuildIdentity",
    "UHCNetworkKey",
    "UHCPlanKey",
    "dataset_build_identity",
    "network_key_for_plan",
    "plan_key_for_scope",
    "validate_dataset_build_identity",
    "validate_network_key_identity",
    "validate_plan_key_identity",
]
