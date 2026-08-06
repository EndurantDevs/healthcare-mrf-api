# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind an authenticated PTG tax sidecar to its dense rate source key.

This kernel performs no I/O. Its eventual publisher must authenticate the
sidecar bytes first and prove exact-once coverage for the complete source
assignment vector before sealing durable evidence.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
import re
from types import MappingProxyType
from typing import Any

from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    normalized_physical_artifact_identity,
)

PTG2_TAX_IDENTITY_RATE_SOURCE_BINDING_CONTRACT = (
    "ptg2_tax_identity_rate_source_binding_v1"
)
_INVALID = "ptg2_tax_identity_rate_source_binding_invalid"
_IDENTITY_FIELDS = frozenset({"source_type", "identity_kind", "identity_sha256"})
_IDENTITY_KINDS = frozenset({"logical_json_sha256_v1", "raw_container_sha256_v1"})
_SHA256 = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_SOURCE_TYPE = "in_network"
_TAX_SIDECAR_NAME = "provider_group_tax_identity"


class TaxIdentityRateSourceBindingError(RuntimeError):
    """One deliberately value-free source-binding failure."""


def _fail() -> TaxIdentityRateSourceBindingError:
    return TaxIdentityRateSourceBindingError(_INVALID)


def _identity(value: object) -> SharedPhysicalArtifactIdentity:
    try:
        if type(value) is SharedPhysicalArtifactIdentity:
            identity_by_field = value.as_dict()
        elif type(value) is dict:
            identity_by_field = value
        else:
            raise _fail()
        if set(identity_by_field) != _IDENTITY_FIELDS or any(
            type(identity_by_field[field_name]) is not str
            for field_name in _IDENTITY_FIELDS
        ):
            raise _fail()
        if (
            identity_by_field["source_type"] != _SOURCE_TYPE
            or identity_by_field["identity_kind"] not in _IDENTITY_KINDS
            or _SHA256.fullmatch(identity_by_field["identity_sha256"]) is None
        ):
            raise _fail()
        normalized = normalized_physical_artifact_identity(identity_by_field)
        if normalized.as_dict() != identity_by_field:
            raise _fail()
    except Exception:
        raise _fail() from None
    return normalized


@dataclass(frozen=True, slots=True, repr=False)
class TaxIdentityRateSourceBinding:
    """Pathless physical identity bound to the rate row's dense source key."""

    source_type: str
    identity_kind: str
    identity_sha256: str
    source_key: int
    contract: str = field(
        default=PTG2_TAX_IDENTITY_RATE_SOURCE_BINDING_CONTRACT,
        init=False,
    )

    def __post_init__(self) -> None:
        try:
            normalized = _identity(
                {
                    "source_type": self.source_type,
                    "identity_kind": self.identity_kind,
                    "identity_sha256": self.identity_sha256,
                }
            )
        except TaxIdentityRateSourceBindingError:
            raise _fail() from None
        if (
            type(self.source_key) is not int
            or not 0 <= self.source_key < 2**31
            or self.contract != PTG2_TAX_IDENTITY_RATE_SOURCE_BINDING_CONTRACT
        ):
            raise _fail()
        object.__setattr__(self, "source_type", normalized.source_type)
        object.__setattr__(self, "identity_kind", normalized.identity_kind)
        object.__setattr__(self, "identity_sha256", normalized.identity_sha256)

    @classmethod
    def from_assignment(
        cls,
        assignment: SharedSnapshotSourceAssignment,
    ) -> "TaxIdentityRateSourceBinding":
        """Build one binding from an exact published source assignment."""

        if type(assignment) is not SharedSnapshotSourceAssignment:
            raise _fail()
        identity = _identity(assignment.identity)
        return cls(
            source_type=identity.source_type,
            identity_kind=identity.identity_kind,
            identity_sha256=identity.identity_sha256,
            source_key=assignment.source_key,
        )

    @property
    def identity(self) -> SharedPhysicalArtifactIdentity:
        """Return the freshly validated physical identity."""

        return _identity(
            {
                "source_type": self.source_type,
                "identity_kind": self.identity_kind,
                "identity_sha256": self.identity_sha256,
            }
        )

    def as_dict(self) -> dict[str, object]:
        """Return the strict pathless manifest payload."""

        return {
            "contract": self.contract,
            "source_type": self.source_type,
            "identity_kind": self.identity_kind,
            "identity_sha256": self.identity_sha256,
            "source_key": self.source_key,
        }

    def __repr__(self) -> str:
        return "<tax-identity-rate-source-binding source=<redacted>>"


@dataclass(frozen=True, slots=True, repr=False)
class _TaxSourceBindingIndex(
    Mapping[SharedPhysicalArtifactIdentity, TaxIdentityRateSourceBinding]
):
    """Immutable lookup with a deliberately redacted representation."""

    _binding_by_identity: Mapping[
        SharedPhysicalArtifactIdentity,
        TaxIdentityRateSourceBinding,
    ] = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_binding_by_identity",
            MappingProxyType(dict(self._binding_by_identity)),
        )

    def __getitem__(
        self,
        identity: SharedPhysicalArtifactIdentity,
    ) -> TaxIdentityRateSourceBinding:
        return self._binding_by_identity[identity]

    def __iter__(self) -> Iterator[SharedPhysicalArtifactIdentity]:
        return iter(self._binding_by_identity)

    def __len__(self) -> int:
        return len(self._binding_by_identity)

    def __repr__(self) -> str:
        return "<tax-source-binding-index entries=<redacted>>"


def build_tax_source_bindings(
    assignments: Iterable[SharedSnapshotSourceAssignment],
) -> Mapping[SharedPhysicalArtifactIdentity, TaxIdentityRateSourceBinding]:
    """Validate a complete dense source dictionary and index it by identity."""

    if isinstance(assignments, (str, bytes, bytearray, Mapping)):
        raise _fail()
    try:
        raw_assignments = tuple(assignments)
    except Exception:
        raise _fail() from None
    if not raw_assignments:
        raise _fail()
    binding_by_identity: dict[
        SharedPhysicalArtifactIdentity,
        TaxIdentityRateSourceBinding,
    ] = {}
    observed_source_keys: set[int] = set()
    for assignment in raw_assignments:
        binding = TaxIdentityRateSourceBinding.from_assignment(assignment)
        identity = binding.identity
        if (
            binding.source_key in observed_source_keys
            or identity in binding_by_identity
        ):
            raise _fail()
        observed_source_keys.add(binding.source_key)
        binding_by_identity[identity] = binding
    if observed_source_keys != set(range(len(raw_assignments))):
        raise _fail()
    return _TaxSourceBindingIndex(dict(sorted(binding_by_identity.items())))


def _binding_from_index(
    binding_index: object,
    identity: SharedPhysicalArtifactIdentity,
) -> TaxIdentityRateSourceBinding:
    if type(binding_index) is not _TaxSourceBindingIndex:
        raise _fail()
    binding = binding_index.get(identity)
    if type(binding) is not TaxIdentityRateSourceBinding:
        raise _fail()
    try:
        rebuilt = TaxIdentityRateSourceBinding(
            source_type=binding.source_type,
            identity_kind=binding.identity_kind,
            identity_sha256=binding.identity_sha256,
            source_key=binding.source_key,
        )
    except TaxIdentityRateSourceBindingError:
        raise _fail() from None
    if rebuilt != binding or rebuilt.identity != identity:
        raise _fail()
    return rebuilt


def bind_tax_sidecar_source_key(
    sidecar_by_field: dict[str, Any],
    *,
    physical_identity: object,
    binding_index: object,
) -> dict[str, Any]:
    """Copy one preauthenticated sidecar and bind only a tax artifact."""

    if type(sidecar_by_field) is not dict:
        raise _fail()
    raw_sidecar_name = sidecar_by_field.get("name")
    if type(raw_sidecar_name) is not str:
        raise _fail()
    sidecar_name = raw_sidecar_name.strip()
    if not sidecar_name:
        raise _fail()
    bound_by_field = dict(sidecar_by_field)
    if sidecar_name != _TAX_SIDECAR_NAME:
        return bound_by_field
    if "physical_source_binding" in sidecar_by_field:
        raise _fail()
    bound_by_field["name"] = sidecar_name
    identity = _identity(physical_identity)
    binding = _binding_from_index(binding_index, identity)
    bound_by_field["physical_source_binding"] = binding.as_dict()
    return bound_by_field


bind_tax_identity_sidecar_to_rate_source = bind_tax_sidecar_source_key
build_tax_identity_rate_source_binding_index = build_tax_source_bindings


__all__ = [
    "PTG2_TAX_IDENTITY_RATE_SOURCE_BINDING_CONTRACT",
    "TaxIdentityRateSourceBinding",
    "TaxIdentityRateSourceBindingError",
    "bind_tax_identity_sidecar_to_rate_source",
    "bind_tax_sidecar_source_key",
    "build_tax_identity_rate_source_binding_index",
    "build_tax_source_bindings",
]
