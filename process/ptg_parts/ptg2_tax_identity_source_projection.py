# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts and immutable values for source-local tax-identity projection.

The scanner sidecar contains keyed tokens only. This module never accepts or
persists a raw tax identifier, and all externally visible failures are
deliberately value-free.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from process.ptg_parts.ptg2_tax_identity_source_copy import _ProjectionCopyLease

from process.ptg_parts.ptg2_tax_identity_source_binding_vector import (
    PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT,
    TaxIdentitySourceBindingVectorError,
    tax_identity_source_binding_vector_digest,
)

PTG2_TAX_IDENTITY_SOURCE_CONTRACT = "ptg2_provider_group_tax_identity_source_v1"
PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT = (
    "ptg2_provider_group_tax_identity_source_content_v1"
)
PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT = "ptg2_tax_identity_rate_source_binding_v1"
_ERROR = "ptg2_tax_identity_source_projection_invalid"
_MAGIC = b"PTG2TAX1"
_VERSION = 1
_RECORD_BYTES = 65
_RECORD_FORMAT = "ptg2_provider_group_tax_identity_v1"
_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_POLICY_ID = re.compile(r"ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_STATE_COUNT_FIELDS = (
    "matched_ein_count",
    "missing_count",
    "malformed_count",
    "unsupported_type_count",
)
_BINDING_FIELDS = frozenset(
    {"contract", "source_type", "identity_kind", "identity_sha256", "source_key"}
)
_SOURCE_DESCRIPTOR_FIELDS = frozenset(
    {
        "name",
        "path",
        "record_format",
        "sha256",
        "byte_count",
        "row_count",
        "provider_group_count",
        "matched_ein_count",
        "missing_count",
        "malformed_count",
        "unsupported_type_count",
        "version",
        "record_bytes",
        "token_policy_id",
        "normalization_contract",
        "hmac_contract",
        "final",
        "source_shard_id",
        "physical_source_binding",
    }
)
_MAX_INTEGER = 2**31 - 1
_MAX_BIGINT = 2**63 - 1
_DESCRIPTOR_NUMBER_FIELDS = (
    ("version", 1, _MAX_INTEGER),
    ("record_bytes", 1, _MAX_INTEGER),
    ("byte_count", 1, _MAX_BIGINT),
    ("row_count", 0, _MAX_BIGINT),
    ("provider_group_count", 0, _MAX_BIGINT),
    *((name, 0, _MAX_BIGINT) for name in _STATE_COUNT_FIELDS),
)


class TaxIdentitySourceProjectionError(RuntimeError):
    """One pathless, token-free projector failure."""


def _fail() -> TaxIdentitySourceProjectionError:
    return TaxIdentitySourceProjectionError(_ERROR)


def _strict_int(
    value: object,
    *,
    minimum: int = 0,
    maximum: int = _MAX_BIGINT,
) -> int:
    if (
        type(value) is not int
        or not minimum <= value <= maximum
    ):
        raise _fail()
    return value


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_policy(value: object) -> str:
    if (
        type(value) is not str
        or _POLICY_ID.fullmatch(value) is None
        or len(value.encode("ascii")) > 55
    ):
        raise _fail()
    return value


def _digest_bytes(digest: Any, value: bytes) -> None:
    digest.update(len(value).to_bytes(4, "big"))
    digest.update(value)


def _digest_ascii(digest: Any, value: str) -> None:
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise _fail() from None
    _digest_bytes(digest, encoded)


@dataclass(frozen=True, slots=True, repr=False)
class _SourceBinding:
    source_key: int
    source_ordinal: int
    source_type: str
    identity_kind: str
    identity_sha256: str
    source_shard_id: str = field(repr=False)
    artifact_sha256: bytes
    artifact_byte_count: int
    provider_group_count: int
    matched_ein_count: int
    missing_count: int
    malformed_count: int
    unsupported_type_count: int
    path: Path = field(repr=False)

    def persisted_values(
        self,
        *,
        snapshot_key: int,
        token_policy_id: str,
        token_policy_descriptor_sha256: bytes,
    ) -> dict[str, object]:
        """Return pathless values stored for this physical-source binding."""

        return {
            "snapshot_key": _strict_int(snapshot_key),
            "source_key": self.source_key,
            "source_type": self.source_type,
            "identity_kind": self.identity_kind,
            "identity_sha256": self.identity_sha256,
            "token_policy_id": token_policy_id,
            "token_policy_descriptor_sha256": token_policy_descriptor_sha256,
            "record_format": _RECORD_FORMAT,
            "format_version": _VERSION,
            "record_bytes": _RECORD_BYTES,
            "artifact_sha256": self.artifact_sha256,
            "artifact_byte_count": self.artifact_byte_count,
            "provider_group_count": self.provider_group_count,
            "matched_ein_count": self.matched_ein_count,
            "missing_count": self.missing_count,
            "malformed_count": self.malformed_count,
            "unsupported_type_count": self.unsupported_type_count,
        }

    def __repr__(self) -> str:
        return "<tax-identity-source-binding source=<redacted>>"


@dataclass(frozen=True, slots=True, repr=False)
class PreparedTaxIdentitySourceProjection:
    """Authenticated pathless metadata plus one ephemeral PostgreSQL COPY."""

    _copy_owner: _ProjectionCopyLease = field(repr=False, compare=False)
    copy_sha256: str
    copy_byte_count: int
    bindings: tuple[_SourceBinding, ...] = field(repr=False)
    token_policy_id: str
    token_policy_descriptor_sha256: bytes
    source_ordinal_map_digest: bytes
    aggregate_tax_content_digest: bytes
    provider_group_occurrence_count: int
    matched_ein_count: int
    missing_count: int
    malformed_count: int
    unsupported_type_count: int
    content_digest: bytes

    @property
    def source_count(self) -> int:
        """Return the number of exact physical-source bindings."""

        return len(self.bindings)

    @property
    def artifact_byte_count(self) -> int:
        """Return the authenticated sidecar bytes represented here."""

        return sum(binding.artifact_byte_count for binding in self.bindings)

    @property
    def binding_vector_digest(self) -> bytes:
        """Return the digest of every value destined for binding storage."""

        try:
            return tax_identity_source_binding_vector_digest(
                binding.persisted_values(
                    snapshot_key=0,
                    token_policy_id=self.token_policy_id,
                    token_policy_descriptor_sha256=(
                        self.token_policy_descriptor_sha256
                    ),
                )
                for binding in self.bindings
            )
        except TaxIdentitySourceBindingVectorError:
            raise _fail() from None

    def cleanup(self) -> None:
        """Best-effort close only this attempt's anonymous COPY descriptor."""

        from process.ptg_parts.ptg2_tax_identity_source_copy import (
            _cleanup_projection_copy_owner,
        )

        _cleanup_projection_copy_owner(self)

    def __repr__(self) -> str:
        return "<prepared-tax-identity-source-projection evidence=<redacted>>"


@dataclass(frozen=True, slots=True)
class TaxIdentitySourcePublication:
    """Pathless source-local metadata bound into the sealed serving manifest."""

    token_policy_id: str
    token_policy_descriptor_sha256: bytes
    source_ordinal_map_digest: bytes
    source_count: int
    provider_group_occurrence_count: int
    matched_ein_count: int
    missing_count: int
    malformed_count: int
    unsupported_type_count: int
    content_digest: bytes
    artifact_byte_count: int
    binding_vector_digest: bytes

    def as_dict(self) -> dict[str, object]:
        """Return pathless sealed-manifest metadata for this publication."""

        return {
            "contract": PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
            "content_contract": PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT,
            "binding_contract": PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
            "binding_vector_contract": (
                PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT
            ),
            "token_policy_id": self.token_policy_id,
            "token_policy_descriptor_sha256": (
                self.token_policy_descriptor_sha256.hex()
            ),
            "source_ordinal_map_digest": self.source_ordinal_map_digest.hex(),
            "source_count": self.source_count,
            "provider_group_occurrence_count": self.provider_group_occurrence_count,
            "matched_ein_count": self.matched_ein_count,
            "missing_count": self.missing_count,
            "malformed_count": self.malformed_count,
            "unsupported_type_count": self.unsupported_type_count,
            "content_digest": self.content_digest.hex(),
            "artifact_byte_count": self.artifact_byte_count,
            "binding_vector_digest": self.binding_vector_digest.hex(),
        }


def _source_ordinal_by_shard(
    raw_entries: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, int], bytes]:
    entries = tuple(raw_entries)
    if not entries or len(entries) > _MAX_INTEGER:
        raise _fail()
    source_ordinal_by_shard: dict[str, int] = {}
    digest = hashlib.sha256()
    digest.update(b"PTG2V4TAXORD\x01")
    digest.update(len(entries).to_bytes(4, "big"))
    for ordinal, entry in enumerate(entries):
        if type(entry) is not dict or set(entry) != {"shard_id", "ordinal"}:
            raise _fail()
        shard_id = entry.get("shard_id")
        if (
            _strict_int(entry.get("ordinal"), maximum=_MAX_INTEGER) != ordinal
            or type(shard_id) is not str
            or not shard_id
            or shard_id in source_ordinal_by_shard
        ):
            raise _fail()
        source_ordinal_by_shard[shard_id] = ordinal
        shard_bytes = shard_id.encode("utf-8")
        digest.update(len(shard_bytes).to_bytes(4, "big"))
        digest.update(shard_bytes)
        digest.update(ordinal.to_bytes(4, "big"))
    if tuple(source_ordinal_by_shard) != tuple(sorted(source_ordinal_by_shard)):
        raise _fail()
    return source_ordinal_by_shard, digest.digest()


def _descriptor_numbers_by_name(raw: Mapping[str, Any]) -> dict[str, int]:
    return {
        name: _strict_int(raw.get(name), minimum=minimum, maximum=maximum)
        for name, minimum, maximum in _DESCRIPTOR_NUMBER_FIELDS
    }


def _has_valid_descriptor_contract(
    raw: Mapping[str, Any],
    binding_by_field: Mapping[str, Any],
    numbers_by_name: Mapping[str, int],
    *,
    policy_id: str,
    expected_policy_id: str,
    source_shard_id: object,
    source_ordinal_by_shard: Mapping[str, int],
) -> bool:
    provider_group_count = numbers_by_name["provider_group_count"]
    identity_kind = binding_by_field.get("identity_kind")
    return (
        type(raw.get("name")) is str
        and raw.get("name") == "provider_group_tax_identity"
        and type(raw.get("record_format")) is str
        and raw.get("record_format") == _RECORD_FORMAT
        and numbers_by_name["version"] == _VERSION
        and numbers_by_name["record_bytes"] == _RECORD_BYTES
        and type(raw.get("normalization_contract")) is str
        and raw.get("normalization_contract") == _NORMALIZATION_CONTRACT
        and type(raw.get("hmac_contract")) is str
        and raw.get("hmac_contract") == _HMAC_CONTRACT
        and raw.get("final") is True
        and numbers_by_name["row_count"] == provider_group_count
        and policy_id == expected_policy_id
        and type(binding_by_field.get("contract")) is str
        and binding_by_field.get("contract")
        == PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT
        and type(binding_by_field.get("source_type")) is str
        and binding_by_field.get("source_type") == "in_network"
        and type(identity_kind) is str
        and identity_kind in {"logical_json_sha256_v1", "raw_container_sha256_v1"}
        and type(source_shard_id) is str
        and source_shard_id in source_ordinal_by_shard
        and provider_group_count
        == sum(numbers_by_name[name] for name in _STATE_COUNT_FIELDS)
        and numbers_by_name["byte_count"]
        == 13 + len(policy_id.encode("ascii")) + provider_group_count * _RECORD_BYTES
    )


def _descriptor_binding(
    raw: object,
    *,
    source_ordinal_by_shard: Mapping[str, int],
    expected_policy_id: str,
) -> _SourceBinding:
    """Validate one source descriptor and return its pathless binding contract."""
    try:
        if type(raw) is not dict or set(raw) != _SOURCE_DESCRIPTOR_FIELDS:
            raise _fail()
        binding_by_field = raw.get("physical_source_binding")
        if (
            type(binding_by_field) is not dict
            or set(binding_by_field) != _BINDING_FIELDS
        ):
            raise _fail()
        source_key = _strict_int(
            binding_by_field.get("source_key"), maximum=_MAX_INTEGER
        )
        identity_sha256 = _strict_sha256(binding_by_field.get("identity_sha256"))
        source_shard_id = raw.get("source_shard_id")
        policy_id = _strict_policy(raw.get("token_policy_id"))
        numbers_by_name = _descriptor_numbers_by_name(raw)
        path_value = raw.get("path")
        artifact_sha256 = _strict_sha256(raw.get("sha256"))
        if (
            not _has_valid_descriptor_contract(
                raw,
                binding_by_field,
                numbers_by_name,
                policy_id=policy_id,
                expected_policy_id=expected_policy_id,
                source_shard_id=source_shard_id,
                source_ordinal_by_shard=source_ordinal_by_shard,
            )
            or type(path_value) is not str
            or not path_value
        ):
            raise _fail()
        assert type(source_shard_id) is str
        return _SourceBinding(
            source_key=source_key,
            source_ordinal=source_ordinal_by_shard[source_shard_id],
            source_type=binding_by_field["source_type"],
            identity_kind=binding_by_field["identity_kind"],
            identity_sha256=identity_sha256,
            source_shard_id=source_shard_id,
            artifact_sha256=bytes.fromhex(artifact_sha256),
            artifact_byte_count=numbers_by_name["byte_count"],
            provider_group_count=numbers_by_name["provider_group_count"],
            matched_ein_count=numbers_by_name["matched_ein_count"],
            missing_count=numbers_by_name["missing_count"],
            malformed_count=numbers_by_name["malformed_count"],
            unsupported_type_count=numbers_by_name["unsupported_type_count"],
            path=Path(path_value),
        )
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


def _validated_bindings(
    raw_sidecars: Iterable[Mapping[str, Any]],
    *,
    source_ordinal_by_shard: Mapping[str, int],
    token_policy_id: str,
) -> tuple[_SourceBinding, ...]:
    try:
        raw_entries = tuple(raw_sidecars)
    except Exception:
        raise _fail() from None
    bindings = tuple(
        sorted(
            (
                _descriptor_binding(
                    entry,
                    source_ordinal_by_shard=source_ordinal_by_shard,
                    expected_policy_id=token_policy_id,
                )
                for entry in raw_entries
            ),
            key=lambda binding: binding.source_key,
        )
    )
    if (
        not bindings
        or len(bindings) != len(raw_entries)
        or tuple(binding.source_key for binding in bindings)
        != tuple(range(len(bindings)))
        or len({binding.source_ordinal for binding in bindings}) != len(bindings)
        or len({binding.source_shard_id for binding in bindings}) != len(bindings)
        or len(
            {
                (
                    binding.source_type,
                    binding.identity_kind,
                    binding.identity_sha256,
                )
                for binding in bindings
            }
        )
        != len(bindings)
        or {binding.source_ordinal for binding in bindings}
        != set(range(len(source_ordinal_by_shard)))
    ):
        raise _fail()
    return bindings


__all__ = [
    "PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT",
    "PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT",
    "PTG2_TAX_IDENTITY_SOURCE_CONTRACT",
    "PreparedTaxIdentitySourceProjection",
    "TaxIdentitySourceProjectionError",
    "TaxIdentitySourcePublication",
]
