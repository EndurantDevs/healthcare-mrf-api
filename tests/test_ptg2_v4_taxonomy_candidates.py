# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import importlib.util
import os
from pathlib import Path
import struct
from types import SimpleNamespace
from typing import Any, Iterable
from unittest.mock import AsyncMock

import pytest

from api.ptg2_code_filters import InferredProviderTaxonomyRule
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api import ptg2_v4_graph as v4_graph
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts import ptg2_v4_taxonomy_candidates as candidates
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.ptg2_v4_coverage_support import (
    _metadata,
    _summary,
    synthetic_adaptive_layout_decision,
)

from tests.ptg2_v4_taxonomy_candidate_test_support import (
    _PreparedCompilerInputSession,
    _PublicationSession,
    _Result,
    _ScriptedSession,
    _assert_candidate_load_rejected,
    _assert_direct_publication_contract,
    _compiler_rules,
    _load_candidate_projection,
    _noop_map_write_lock,
    _observe_projection_row,
    _projection_row,
    _publish_candidate_projection,
    _reader_row,
    _rules,
    _tampered_pattern_projection,
)

def test_candidate_codec_helpers_reject_noncanonical_inputs() -> None:
    rule = _rules()[0]
    assert candidates._row_mapping(SimpleNamespace(_mapping={"key": 1})) == {"key": 1}
    assert candidates._row_mapping({"key": 2}) == {"key": 2}
    assert candidates._row_mapping(None) == {}
    assert candidates._digest_bytes(b"d" * 32, label="digest") == b"d" * 32
    for digest_candidate in (object(), b"short"):
        with pytest.raises(PTG2ManifestArtifactError, match="digest"):
            candidates._digest_bytes(digest_candidate, label="digest")

    for invalid_rule in (
        replace(rule, ranges=()),
        replace(rule, ranges=((-1, 1),)),
        replace(rule, ranges=((2, 1),)),
        replace(rule, taxonomy_codes=()),
        replace(rule, taxonomy_codes=(" ",)),
    ):
        with pytest.raises(ValueError, match="rule is invalid"):
            candidates.inferred_provider_taxonomy_rule_digest(invalid_rule)

    for invalid_keys in ((1, 1), (-1,), (0x1_0000_0000,)):
        with pytest.raises(ValueError, match="strict uint32"):
            candidates.pack_inferred_taxonomy_npi_keys(invalid_keys)
    with pytest.raises(PTG2ManifestArtifactError, match="invalid size"):
        candidates.unpack_inferred_taxonomy_npi_keys(b"", member_count=-1)
    with pytest.raises(PTG2ManifestArtifactError, match="invalid size"):
        candidates.unpack_inferred_taxonomy_npi_keys(b"\0", member_count=1)
    with pytest.raises(PTG2ManifestArtifactError, match="not strict"):
        candidates.unpack_inferred_taxonomy_npi_keys(
            struct.pack("<II", 2, 2), member_count=2
        )


@pytest.mark.parametrize(
    "postings",
    (
        "not-a-map",
        {True: (1,)},
        {"bad": (1,)},
        {-1: (1,)},
        {0x1_0000_0000: (1,)},
        {"1": (1,), 1: (2,)},
        {1: ()},
        {1: (True,)},
        {1: ("bad",)},
        {1: (2, 2)},
        {1: (0x1_0000_0000,)},
    ),
)
def test_pattern_encoder_rejects_noncanonical_postings(postings) -> None:
    with pytest.raises(ValueError, match="pattern"):
        candidates.pack_inferred_taxonomy_pattern_npi_keys(postings)


def _pattern_payload(
    *,
    magic=b"PTG4TXP2",
    version=1,
    pattern_count=1,
    member_count=1,
    body=b"",
) -> bytes:
    return (
        candidates._PATTERN_PAYLOAD_HEADER.pack(
            magic, version, pattern_count, member_count
        )
        + body
    )


@pytest.mark.parametrize(
    ("payload", "pattern_count", "member_count"),
    (
        (b"", -1, 0),
        (b"", 1, 1),
        (b"tiny", 1, 1),
        (_pattern_payload(magic=b"BADMAGIC"), 1, 1),
        (_pattern_payload(version=2), 1, 1),
        (_pattern_payload(pattern_count=2), 1, 1),
        (_pattern_payload(member_count=2), 1, 1),
        (_pattern_payload(), 1, 1),
        (
            _pattern_payload(body=candidates._PATTERN_PAYLOAD_RECORD.pack(1, 0)),
            1,
            1,
        ),
        (
            _pattern_payload(body=candidates._PATTERN_PAYLOAD_RECORD.pack(1, 1)),
            1,
            1,
        ),
        (
            _pattern_payload(
                pattern_count=1,
                member_count=2,
                body=(
                    candidates._PATTERN_PAYLOAD_RECORD.pack(1, 2)
                    + struct.pack("<II", 2, 2)
                ),
            ),
            1,
            2,
        ),
    ),
)
def test_pattern_decoder_rejects_structural_drift(
    payload, pattern_count, member_count
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="pattern"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            payload,
            pattern_count=pattern_count,
            pattern_member_count=member_count,
        )


def test_pattern_decoder_rejects_duplicate_and_trailing_postings() -> None:
    record = candidates._PATTERN_PAYLOAD_RECORD
    duplicate_patterns = _pattern_payload(
        pattern_count=2,
        member_count=2,
        body=(
            record.pack(1, 1)
            + struct.pack("<I", 1)
            + record.pack(1, 1)
            + struct.pack("<I", 2)
        ),
    )
    valid = candidates.pack_inferred_taxonomy_pattern_npi_keys({1: (1,)})
    for payload, pattern_count, member_count in (
        (duplicate_patterns, 2, 2),
        (valid + b"extra", 1, 1),
    ):
        with pytest.raises(PTG2ManifestArtifactError, match="pattern"):
            candidates.unpack_inferred_taxonomy_pattern_npi_keys(
                payload,
                pattern_count=pattern_count,
                pattern_member_count=member_count,
            )


def test_candidate_digests_reject_incompatible_identity_and_payload() -> None:
    rule_digest = b"r" * 32
    for digest, count, member_payload in (
        (b"short", 0, b""),
        (rule_digest, -1, b""),
        (rule_digest, 1, b""),
    ):
        with pytest.raises(ValueError, match="member"):
            candidates.inferred_taxonomy_member_digest(
                digest, member_count=count, payload=member_payload
            )
    for kwargs in (
        {"rule_digest": b"short", "representation": "direct_v1"},
        {"rule_digest": rule_digest, "representation": "bad_v1"},
        {
            "rule_digest": rule_digest,
            "representation": "direct_v1",
            "pattern_count": 1,
        },
        {
            "rule_digest": rule_digest,
            "representation": "pattern_v1",
            "pattern_count": 1,
        },
    ):
        with pytest.raises(ValueError, match="pattern"):
            candidates.inferred_taxonomy_pattern_member_digest(
                pattern_count=kwargs.get("pattern_count", 0),
                pattern_member_count=0,
                packed_pattern_payload=b"",
                **{
                    key: field_value
                    for key, field_value in kwargs.items()
                    if key != "pattern_count"
                },
            )


def test_rule_set_validation_rejects_empty_duplicate_and_malformed_digests() -> None:
    """Fail closed when semantic rule-set identity is absent or non-unique."""

    rule = _rules()[0]
    for invalid_rules in ((), (rule, rule)):
        with pytest.raises(ValueError, match="rules|duplicated"):
            candidates.inferred_provider_taxonomy_rule_set_digest(invalid_rules)
    with pytest.raises(ValueError, match="digest set"):
        candidates._rule_set_digest_from_digests(())


def test_projection_shaper_rejects_root_and_row_drift() -> None:
    row = _projection_row(_rules()[0])
    for npi_count, pattern_count in ((True, 0), (3, True), (-1, 0), (3, -1)):
        with pytest.raises(RuntimeError, match="dictionary bounds"):
            candidates.shape_v4_inferred_taxonomy_projection_manifest(
                (row,), npi_count=npi_count, pattern_count=pattern_count
            )
    with pytest.raises(RuntimeError, match="no rule evidence"):
        candidates.shape_v4_inferred_taxonomy_projection_manifest(
            (), npi_count=3, pattern_count=0
        )
    with pytest.raises(RuntimeError, match="observe witness"):
        candidates.shape_v4_inferred_taxonomy_projection_manifest(
            ({**row, "observe_count_lower_bound": True},),
            npi_count=3,
            pattern_count=0,
        )
    for changed_row in (
        {**row, "rule_digest": b"short"},
        {**row, "member_count": -1},
        {**row, "pattern_member_bytes": 1},
    ):
        with pytest.raises(RuntimeError, match="candidate manifest"):
            candidates.shape_v4_inferred_taxonomy_projection_manifest(
                (changed_row,), npi_count=3, pattern_count=0
            )


def test_projection_shaper_rejects_root_projection_and_digest_drift() -> None:
    rule = _rules()[0]
    direct_row = _projection_row(rule)
    pattern_row = _projection_row(
        rule,
        npi_keys_by_pattern={1: (0,), 2: (2,)},
    )
    projection_cases = (
        ((direct_row,), 2, 0, "NPI root"),
        ((direct_row,), 3, 1, "projection is missing"),
        ((pattern_row,), 3, 2, "pattern root"),
        (
            (_projection_row(rule, npi_keys_by_pattern={1: (0,)}),),
            3,
            2,
            "projection is incomplete",
        ),
        (({**direct_row, "catalog_contract": "bad"},), 3, 0, "manifest changed"),
        (({**direct_row, "vector_format": "bad"},), 3, 0, "manifest changed"),
        (({**direct_row, "member_digest": b"x" * 32},), 3, 0, "manifest changed"),
        (
            ({**direct_row, "pattern_member_digest": b"x" * 32},),
            3,
            0,
            "manifest changed",
        ),
    )
    for projection_rows, npi_count, pattern_count, message in projection_cases:
        with pytest.raises(RuntimeError, match=message):
            candidates.shape_v4_inferred_taxonomy_projection_manifest(
                projection_rows,
                npi_count=npi_count,
                pattern_count=pattern_count,
            )
