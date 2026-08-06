# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure primitives for dormant public-evidence publication intents."""

from __future__ import annotations

import hashlib
import json
import re
from types import MappingProxyType
from typing import NamedTuple

STAGED_BUNDLE_PUBLICATION_INTENT_CONTRACT = (
    "healthporta.public-evidence-staged-bundle-intent.v1"
)
CATALOG_FINGERPRINT_CONTRACT = (
    "healthporta.public-evidence-name-neutral-catalog-fingerprint.v1"
)
PUBLIC_EVIDENCE_BUILD_RUN_REF_PREFIX = "pebuild1_"
PUBLIC_EVIDENCE_GENERATION_REF_PREFIX = "pegen1_"
CATALOG_FINGERPRINT_EXCLUSIONS = (
    "catalog_object_names",
    "catalog_object_oids",
)
MAX_STAGED_BUNDLE_RELATIONS = 4096
MAX_STAGED_BUNDLE_SOURCE_RELEASES = 4096

_INVALID = "public_evidence_staged_bundle_intent_invalid"
_CONTRACT_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_STAGED_BUNDLE_INTENT_V1\x00"
_SOURCE_VECTOR_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_SOURCE_VECTOR_V1\x00"
_SOURCE_FENCE_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_SOURCE_FENCE_V1\x00"
_POINTER_FENCE_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_POINTER_FENCE_V1\x00"
_OID_FENCE_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_OID_FENCE_V1\x00"
_CATALOG_FENCE_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_CATALOG_FENCE_V1\x00"
_STAGE_NAME_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_STAGE_NAME_V1\x00"
_OLD_NAME_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_OLD_NAME_V1\x00"

_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_PG_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}", flags=re.ASCII)
_SOURCE_KIND_RE = re.compile(r"[a-z][a-z0-9_]{1,95}", flags=re.ASCII)
_BUILD_RUN_REF_RE = re.compile(r"pebuild1_[A-Za-z0-9_-]{43}", flags=re.ASCII)
_GENERATION_REF_RE = re.compile(r"pegen1_[A-Za-z0-9_-]{43}", flags=re.ASCII)
_SOURCE_RELEASE_REF_RE = re.compile(
    r"perel1_[A-Za-z0-9_-]{43}",
    flags=re.ASCII,
)

_FINGERPRINT_FIELDS = frozenset(
    "schema_sha256 columns_sha256 constraints_sha256 indexes_sha256 owner_sha256 "
    "privileges_sha256".split()
)
_RELATION_INPUT_FIELDS = frozenset(
    "role live_relation stage_relation old_relation observed_live_oid "
    "observed_stage_oid observed_old_oid stage_persistence expected_fingerprints "
    "stage_fingerprints live_fingerprints".split()
)
_BUNDLE_INPUT_FIELDS = frozenset(
    "schema build_run_ref generation_ref expected_current_generation_ref "
    "expected_previous_generation_ref "
    "source_releases relations".split()
)
_PUBLICATION_MODES = frozenset({"initial", "replacement"})
_CATALOG_STATES = frozenset({"not_applicable_no_live", "verified_equal"})
_FIXED_DESCRIPTOR_STATE = MappingProxyType(
    {
        "lifecycle_state": "validated_intent_only",
        "serving_authority": "none",
        "current_pointer_authority": "none",
        "executor_authority": "none",
        "publication_authorized": False,
        "publication_enabled": False,
        "cleanup_authorized": False,
        "reverse_swap_authorized": False,
        "database_io_enabled": False,
        "executable_rename_choreography_defined": False,
        "index_rename_choreography_defined": False,
        "retained_old_required": True,
        "automatic_old_deletion_enabled": False,
        "automatic_gc_enabled": False,
    }
)


class StagedBundlePublicationIntentError(RuntimeError):
    """Report invalid publication intent without echoing supplied values."""


def _fail() -> StagedBundlePublicationIntentError:
    return StagedBundlePublicationIntentError(_INVALID)


class CatalogFingerprints(NamedTuple):
    """Exact catalog digests excluding catalog object names and OIDs."""

    schema_sha256: str
    columns_sha256: str
    constraints_sha256: str
    indexes_sha256: str
    owner_sha256: str
    privileges_sha256: str

    def __repr__(self) -> str:
        return "CatalogFingerprints(<redacted>)"


class PublicEvidenceSourceWitness(NamedTuple):
    """Detached immutable witness for one validated public source release."""

    source_kind: str
    source_release_ref: str
    contract_sha256: str

    def __repr__(self) -> str:
        return "PublicEvidenceSourceWitness(<redacted>)"


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _pg_identifier_syntax(value: object) -> str:
    if type(value) is not str or _PG_IDENTIFIER_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_pg_identifier(value: object) -> str:
    return _pg_identifier_syntax(value)


def _strict_build_run_ref(value: object) -> str:
    if type(value) is not str or _BUILD_RUN_REF_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_generation_ref(value: object) -> str:
    if type(value) is not str or _GENERATION_REF_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_source_kind(value: object) -> str:
    if type(value) is not str or _SOURCE_KIND_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_source_release_ref(value: object) -> str:
    if type(value) is not str or _SOURCE_RELEASE_REF_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _optional_generation_ref(value: object) -> str | None:
    return None if value is None else _strict_generation_ref(value)


def _positive_oid(value: object) -> int:
    if type(value) is not int or not 0 < value <= 2**32 - 1:
        raise _fail()
    return value


def _strict_literal(value: object, expected: object) -> object:
    if type(value) is not type(expected) or value != expected:
        raise _fail()
    return expected


def _strict_string_tuple_literal(
    value: object,
    expected: tuple[str, ...],
) -> tuple[str, ...]:
    if (
        type(value) is not tuple
        or len(value) != len(expected)
        or any(type(element) is not str for element in value)
        or value != expected
    ):
        raise _fail()
    return expected


def _require_exact_dict(raw: object, fields: frozenset[str]) -> None:
    if type(raw) is not dict or len(raw) != len(fields):
        raise _fail()
    if any(type(key) is not str or key not in fields for key in raw):
        raise _fail()


def _bounded_tuple(
    value: object,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> tuple[object, ...]:
    if (
        type(value) is not tuple
        or (not allow_empty and not value)
        or len(value) > maximum
    ):
        raise _fail()
    return value


def _canonical_sha256(domain: bytes, payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def derive_stage_relation_name(
    schema: object,
    build_run_ref: object,
    role: object,
    live_relation: object,
) -> str:
    """Return one deterministic, run-scoped PostgreSQL stage identifier."""
    identity_parts = (
        STAGED_BUNDLE_PUBLICATION_INTENT_CONTRACT,
        _strict_pg_identifier(schema),
        _strict_build_run_ref(build_run_ref),
        _strict_pg_identifier(role),
        _strict_pg_identifier(live_relation),
    )
    suffix = f"_stage_{_canonical_sha256(_STAGE_NAME_DOMAIN, identity_parts)[:24]}"
    return f"{identity_parts[-1][: 63 - len(suffix)]}{suffix}"


def derive_old_relation_name(live_relation: object) -> str:
    """Return a deterministic retained relation name ending in ``_old``."""
    live = _strict_pg_identifier(live_relation)
    direct = f"{live}_old"
    if len(direct) <= 63:
        return direct
    digest = _canonical_sha256(
        _OLD_NAME_DOMAIN,
        (STAGED_BUNDLE_PUBLICATION_INTENT_CONTRACT, live),
    )[:24]
    suffix = f"_{digest}_old"
    return f"{live[: 63 - len(suffix)]}{suffix}"


def _fingerprints_tuple(value: object) -> CatalogFingerprints:
    if type(value) is not CatalogFingerprints:
        raise _fail()
    return CatalogFingerprints(*(_strict_sha256(digest) for digest in value))


def _fingerprints_from_raw(raw: object) -> CatalogFingerprints:
    _require_exact_dict(raw, _FINGERPRINT_FIELDS)
    return CatalogFingerprints(
        schema_sha256=_strict_sha256(raw.get("schema_sha256")),
        columns_sha256=_strict_sha256(raw.get("columns_sha256")),
        constraints_sha256=_strict_sha256(raw.get("constraints_sha256")),
        indexes_sha256=_strict_sha256(raw.get("indexes_sha256")),
        owner_sha256=_strict_sha256(raw.get("owner_sha256")),
        privileges_sha256=_strict_sha256(raw.get("privileges_sha256")),
    )


def _fingerprints_payload(value: object) -> dict[str, str]:
    return _fingerprints_tuple(value)._asdict()


def _source_witness_tuple(value: object) -> PublicEvidenceSourceWitness:
    if type(value) is not PublicEvidenceSourceWitness:
        raise _fail()
    return PublicEvidenceSourceWitness(
        source_kind=_strict_source_kind(value.source_kind),
        source_release_ref=_strict_source_release_ref(value.source_release_ref),
        contract_sha256=_strict_sha256(value.contract_sha256),
    )


def _source_witness_payload(value: object) -> dict[str, str]:
    return _source_witness_tuple(value)._asdict()


def _strict_identifier_tuple(
    value: object,
    *,
    maximum: int,
) -> tuple[str, ...]:
    values = _bounded_tuple(value, maximum=maximum, allow_empty=True)
    return tuple(_pg_identifier_syntax(identifier) for identifier in values)
