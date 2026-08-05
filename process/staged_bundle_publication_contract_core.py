# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure validation primitives for the staged-bundle publication contract."""

from __future__ import annotations

import hashlib
import json
import re

STAGED_BUNDLE_PUBLICATION_CONTRACT = "healthporta.staged-bundle-publication.v1"
MAX_STAGED_BUNDLE_RELATIONS = 4096
_INVALID = "staged_bundle_publication_contract_invalid"
_CONTRACT_DOMAIN = b"HEALTHPORTA_STAGED_BUNDLE_PUBLICATION_V1\x00"
_SOURCE_FENCE_DOMAIN = b"HEALTHPORTA_STAGED_BUNDLE_SOURCE_FENCE_V1\x00"
_POINTER_FENCE_DOMAIN = b"HEALTHPORTA_STAGED_BUNDLE_POINTER_FENCE_V1\x00"
_OID_FENCE_DOMAIN = b"HEALTHPORTA_STAGED_BUNDLE_OID_FENCE_V1\x00"
_STAGE_NAME_DOMAIN = b"HEALTHPORTA_STAGED_BUNDLE_STAGE_NAME_V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_STAGE_DIGEST_RE = re.compile(r"[0-9a-f]{24}", flags=re.ASCII)
_STAGE_RELATION_RE = re.compile(
    r"([a-z_][a-z0-9_]{0,31})_stage_[0-9a-f]{24}", flags=re.ASCII
)
_PG_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}", flags=re.ASCII)
_PUBLIC_ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,159}", flags=re.ASCII)
_PRIVATE_ID_SHAPE_RE = re.compile(
    r"(?<![0-9])(?:[0-9][._-]*){8,}[0-9](?![._-]*[0-9])|"
    r"(?:til1_[0-9a-f]{32}|(?:tih1|tip1)_[0-9a-f]{64})(?![0-9a-f])|"
    r"(?:^|[._-])api[._-]+key(?=$|[._-])|"
    r"(?:^|[._-])(?:(?:(?:ghp|gho|ghu|ghs|ghr)_|github_pat_)"
    r"[a-z0-9_]{20,}|(?:sk|rk)(?:[._-]live)?[._-][a-z0-9_-]{20,})",
    flags=re.ASCII,
)
_SENSITIVE_ID_SEGMENTS = frozenset(
    "apikey authorization bearer credential password path private raw secret "
    "token uri url".split()
)
_SENSITIVE_AFFIXES = tuple(
    "apikey authorization bearer credential password private raw secret token".split()
)
_FINGERPRINT_FIELDS = frozenset(
    "schema_sha256 columns_sha256 constraints_sha256 indexes_sha256 owner_sha256 "
    "privileges_sha256".split()
)
_FINGERPRINT_ORDER = tuple(sorted(_FINGERPRINT_FIELDS))
_RELATION_FIELDS = frozenset(
    "role live_relation stage_relation old_relation observed_live_oid "
    "observed_stage_oid observed_old_oid stage_logged "
    "old_relation_expected_absent catalog_parity_verified stage_fingerprints "
    "live_fingerprints".split()
)
_PUBLICATION_MODES = frozenset({"initial", "replacement"})
_FIXED_INPUTS: dict[str, object] = {
    "serving_authority": "none",
    "publication_authorized": False,
    "cleanup_authorized": False,
    "reverse_swap_authorized": False,
    "database_io_enabled": False,
    "retained_old_required": True,
    "automatic_old_deletion_enabled": False,
    "automatic_gc_enabled": False,
}
_BUNDLE_FIELDS = frozenset(
    "schema run_id generation_id expected_predecessor_generation_id "
    "expected_current_generation_id expected_previous_generation_id "
    "source_vector_sha256 source_vector_canonical relations".split()
).union(_FIXED_INPUTS)


class StagedBundlePublicationContractError(RuntimeError):
    """Report invalid preparation state without echoing supplied values."""


def _fail() -> StagedBundlePublicationContractError:
    return StagedBundlePublicationContractError(_INVALID)


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _pg_identifier_syntax(value: object) -> str:
    if type(value) is not str or _PG_IDENTIFIER_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_pg_identifier(value: object) -> str:
    identifier = _pg_identifier_syntax(value)
    if _PRIVATE_ID_SHAPE_RE.search(identifier):
        raise _fail()
    return identifier


def _strict_public_id(value: object) -> str:
    if type(value) is not str or _PUBLIC_ID_RE.fullmatch(value) is None:
        raise _fail()
    segments = frozenset(re.split(r"[._-]+", value))
    if (
        segments.intersection(_SENSITIVE_ID_SEGMENTS)
        or any(
            segment.startswith(_SENSITIVE_AFFIXES)
            or segment.endswith(_SENSITIVE_AFFIXES)
            for segment in segments
        )
        or _PRIVATE_ID_SHAPE_RE.search(value)
    ):
        raise _fail()
    return value


def _strict_stage_relation_name(value: object, live_relation: object) -> str:
    stage = _pg_identifier_syntax(value)
    live = _strict_pg_identifier(live_relation)
    marker = "_stage_"
    live_prefix = live[: 63 - len(marker) - 24]
    expected_prefix = f"{live_prefix}{marker}"
    if (
        not stage.startswith(expected_prefix)
        or _STAGE_DIGEST_RE.fullmatch(stage[len(expected_prefix) :]) is None
    ):
        raise _fail()
    return stage


def _optional_public_id(value: object) -> str | None:
    return None if value is None else _strict_public_id(value)


def _positive_oid(value: object) -> int:
    if type(value) is not int or not 0 < value <= 2**32 - 1:
        raise _fail()
    return value


def _strict_literal(value: object, expected: object) -> object:
    if type(value) is not type(expected) or value != expected:
        raise _fail()
    return expected


def _require_exact_dict(raw: object, fields: frozenset[str]) -> None:
    if type(raw) is not dict or len(raw) != len(fields):
        raise _fail()
    if any(type(key) is not str or key not in fields for key in raw):
        raise _fail()


def _bounded_relations(value: object) -> tuple[object, ...]:
    if (
        type(value) is not tuple
        or not value
        or len(value) > MAX_STAGED_BUNDLE_RELATIONS
    ):
        raise _fail()
    return value


def _canonical_sha256(domain: bytes, payload: object) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def derive_stage_relation_name(
    schema: object, run_id: object, role: object, live_relation: object
) -> str:
    """Return one deterministic, run-scoped PostgreSQL stage identifier."""
    identity_parts = [
        STAGED_BUNDLE_PUBLICATION_CONTRACT,
        _strict_pg_identifier(schema),
        _strict_public_id(run_id),
        _strict_pg_identifier(role),
        _strict_pg_identifier(live_relation),
    ]
    suffix = f"_stage_{_canonical_sha256(_STAGE_NAME_DOMAIN, identity_parts)[:24]}"
    return f"{identity_parts[-1][: 63 - len(suffix)]}{suffix}"


CatalogFingerprints = tuple[str, str, str, str, str, str]


def _fingerprints_tuple(value: object) -> CatalogFingerprints:
    if type(value) is not tuple or len(value) != len(_FINGERPRINT_ORDER):
        raise _fail()
    return tuple(_strict_sha256(digest) for digest in value)


def _fingerprints_from_raw(raw: object) -> CatalogFingerprints:
    _require_exact_dict(raw, _FINGERPRINT_FIELDS)
    return tuple(_strict_sha256(raw[name]) for name in _FINGERPRINT_ORDER)


def _fingerprints_input(value: object) -> dict[str, object]:
    fingerprints = _fingerprints_tuple(value)
    return {
        name: digest
        for name, digest in zip(_FINGERPRINT_ORDER, fingerprints, strict=True)
    }


def _strict_lock_identifier(value: object) -> str:
    identifier = _pg_identifier_syntax(value)
    stage_match = _STAGE_RELATION_RE.fullmatch(identifier)
    if stage_match is None:
        return _strict_pg_identifier(identifier)
    _strict_pg_identifier(stage_match.group(1))
    return identifier


def _strict_identifier_tuple(
    value: object, *, maximum: int, allow_stage_digest: bool = False
) -> tuple[str, ...]:
    if type(value) is not tuple or len(value) > maximum:
        raise _fail()
    validator = _strict_lock_identifier if allow_stage_digest else _strict_pg_identifier
    return tuple(validator(identifier) for identifier in value)
