# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Neutral synthetic fixtures for staged bundle publication intent tests."""

from __future__ import annotations

from public_evidence import staged_bundle_publication_primitives as primitives
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
    build_public_evidence_source_release,
)
from tests.public_evidence_source_release_support import release_input

SCHEMA = "public_evidence"
BUILD_RUN_REF = primitives.PUBLIC_EVIDENCE_BUILD_RUN_REF_PREFIX + "A" * 43
GENERATION_REF = primitives.PUBLIC_EVIDENCE_GENERATION_REF_PREFIX + "B" * 43
CURRENT_GENERATION_REF = primitives.PUBLIC_EVIDENCE_GENERATION_REF_PREFIX + "C" * 43
PREVIOUS_GENERATION_REF = primitives.PUBLIC_EVIDENCE_GENERATION_REF_PREFIX + "D" * 43
ALTERNATE_GENERATION_REF = primitives.PUBLIC_EVIDENCE_GENERATION_REF_PREFIX + "E" * 43
_HEX_CHARACTERS = "0123456789abcdef"
_FINGERPRINT_FIELDS = (
    "schema_sha256",
    "columns_sha256",
    "constraints_sha256",
    "indexes_sha256",
    "owner_sha256",
    "privileges_sha256",
)


def fingerprints(seed: int = 0) -> dict[str, str]:
    """Return six distinct lowercase SHA-256-shaped synthetic digests."""
    return {
        field_name: _HEX_CHARACTERS[(seed + offset) % len(_HEX_CHARACTERS)] * 64
        for offset, field_name in enumerate(_FINGERPRINT_FIELDS)
    }


def source_release(
    source_kind: str = "tic",
) -> PublicEvidenceSourceReleaseDescriptor:
    return build_public_evidence_source_release(release_input(source_kind))


def relation_input(
    *,
    role: str = "source_release",
    live_relation: str = "evidence_source_release",
    build_run_ref: str = BUILD_RUN_REF,
    stage_oid: int = 101,
    live_oid: int | None = None,
    fingerprint_seed: int = 0,
) -> dict[str, object]:
    expected = fingerprints(fingerprint_seed)
    return {
        "role": role,
        "live_relation": live_relation,
        "stage_relation": primitives.derive_stage_relation_name(
            SCHEMA,
            build_run_ref,
            role,
            live_relation,
        ),
        "old_relation": primitives.derive_old_relation_name(live_relation),
        "observed_live_oid": live_oid,
        "observed_stage_oid": stage_oid,
        "observed_old_oid": None,
        "stage_persistence": "logged",
        "expected_fingerprints": expected,
        "stage_fingerprints": dict(expected),
        "live_fingerprints": None if live_oid is None else dict(expected),
    }


def two_relation_inputs(*, replacement: bool = False) -> tuple[dict[str, object], ...]:
    return (
        relation_input(
            role="source_release",
            live_relation="evidence_source_release",
            stage_oid=101,
            live_oid=201 if replacement else None,
            fingerprint_seed=0,
        ),
        relation_input(
            role="address_evidence",
            live_relation="entity_address_evidence",
            stage_oid=102,
            live_oid=202 if replacement else None,
            fingerprint_seed=6,
        ),
    )


def bundle_input(
    *,
    replacement: bool = False,
    previous: str | None = PREVIOUS_GENERATION_REF,
    source_kinds: tuple[str, ...] = ("tic",),
    relations: tuple[dict[str, object], ...] | None = None,
) -> dict[str, object]:
    return {
        "schema": SCHEMA,
        "build_run_ref": BUILD_RUN_REF,
        "generation_ref": GENERATION_REF,
        "expected_current_generation_ref": (
            CURRENT_GENERATION_REF if replacement else None
        ),
        "expected_previous_generation_ref": previous if replacement else None,
        "source_releases": tuple(source_release(kind) for kind in source_kinds),
        "relations": (
            two_relation_inputs(replacement=replacement)
            if relations is None
            else relations
        ),
    }
