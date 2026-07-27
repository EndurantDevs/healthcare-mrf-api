# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import importlib.util
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
from tests.ptg2_v4_coverage_support import _metadata, _summary


def test_candidate_codec_helpers_reject_noncanonical_inputs() -> None:
    rule = _rules()[0]
    assert candidates._row_mapping(SimpleNamespace(_mapping={"key": 1})) == {
        "key": 1
    }
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
    return candidates._PATTERN_PAYLOAD_HEADER.pack(
        magic, version, pattern_count, member_count
    ) + body


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
            _pattern_payload(
                body=candidates._PATTERN_PAYLOAD_RECORD.pack(1, 0)
            ),
            1,
            1,
        ),
        (
            _pattern_payload(
                body=candidates._PATTERN_PAYLOAD_RECORD.pack(1, 1)
            ),
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
    cases = (
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
        (({**direct_row, "pattern_member_digest": b"x" * 32},), 3, 0, "manifest changed"),
    )
    for rows, npi_count, pattern_count, message in cases:
        with pytest.raises(RuntimeError, match=message):
            candidates.shape_v4_inferred_taxonomy_projection_manifest(
                rows,
                npi_count=npi_count,
                pattern_count=pattern_count,
            )


class _Result:
    def __init__(self, rows: Iterable[Any] = ()) -> None:
        self.rows = tuple(rows)

    def __iter__(self):
        return iter(self.rows)


class _ScriptedSession:
    def __init__(self, *results: _Result) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        if not self.results:
            raise AssertionError(f"unexpected SQL: {statement}")
        return self.results.pop(0)


class _PublicationSession:
    def __init__(
        self,
        *,
        catalog_rows_by_codes: dict[
            tuple[str, ...], tuple[dict[str, Any], ...]
        ]
        | None = None,
    ) -> None:
        self.catalog_sql = ""
        self.catalog_calls: list[dict[str, Any]] = []
        self.stored_rows: list[dict[str, Any]] = []
        self.catalog_rows_by_codes = catalog_rows_by_codes

    def _execute_catalog_query(self, sql, parameters):
        self.catalog_sql = sql
        parameters_by_name = dict(parameters)
        self.catalog_calls.append(parameters_by_name)
        taxonomy_codes = tuple(parameters_by_name["taxonomy_codes"])
        default_rows_by_codes = {
            ("AAA",): (
                {
                    "npi_key": 0,
                    "npi": 1_234_567_890,
                    "matched_taxonomy_codes": ["AAA"],
                },
                {
                    "npi_key": 2,
                    "npi": 1_234_567_892,
                    "matched_taxonomy_codes": ["AAA"],
                },
            ),
            ("BBB",): (
                {
                    "npi_key": 2,
                    "npi": 1_234_567_892,
                    "matched_taxonomy_codes": ["BBB"],
                },
            ),
        }
        rows_by_codes = self.catalog_rows_by_codes or default_rows_by_codes
        return _Result(rows_by_codes.get(taxonomy_codes, ()))

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if "ARRAY_AGG" in sql:
            return self._execute_catalog_query(sql, parameters)
        if "INSERT INTO" in sql:
            self.stored_rows = [
                dict(stored_row) for stored_row in parameters
            ]
            return _Result()
        if "SELECT rule_digest" in sql:
            return _Result(self.stored_rows)
        raise AssertionError(f"unexpected SQL: {sql}")


def _rules() -> tuple[InferredProviderTaxonomyRule, ...]:
    return (
        InferredProviderTaxonomyRule(
            ranges=((10, 19),),
            taxonomy_codes=("AAA",),
            display_terms=("first display",),
        ),
        InferredProviderTaxonomyRule(
            ranges=((20, 29),),
            taxonomy_codes=("BBB",),
            display_terms=("second display",),
        ),
    )


async def _noop_map_write_lock(*_args, **_kwargs) -> None:
    return None


async def _publish_candidate_projection(
    session: _PublicationSession,
    *,
    representation: str,
    pattern_count: int,
):
    return await candidates.publish_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation=representation,
        pattern_count=pattern_count,
    )


async def _load_candidate_projection(session, projection_row, manifest):
    return await candidates.load_v4_inferred_taxonomy_candidates(
        session,
        snapshot_key=41,
        rule_digest=projection_row["rule_digest"],
        schema_name="mrf",
        projection_manifest=manifest,
    )


async def _assert_candidate_load_rejected(
    session,
    projection_row,
    manifest,
    message: str,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await _load_candidate_projection(session, projection_row, manifest)


def _tampered_pattern_projection():
    pattern_row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    pattern_manifest = candidates._candidate_projection_manifest(
        (pattern_row,)
    )
    tampered_pattern_payload = bytearray(
        pattern_row["pattern_member_payload"]
    )
    tampered_pattern_payload[-1] ^= 1
    pattern_session = _ScriptedSession(
        _Result(
            (
                _reader_row(
                    pattern_row,
                    pattern_member_payload=bytes(tampered_pattern_payload),
                ),
            )
        )
    )
    return pattern_row, pattern_manifest, pattern_session


def _assert_direct_publication_contract(publication, session) -> None:
    assert "COALESCE(entity.entity_type_code, 0) = 1" in session.catalog_sql
    assert "LIMIT :candidate_limit" in session.catalog_sql
    assert {call["taxonomy_codes"] for call in session.catalog_calls} == {
        ("AAA",),
        ("BBB",),
    }
    assert {call["candidate_limit"] for call in session.catalog_calls} == {
        37_001
    }
    assert publication.rule_count == 2
    assert publication.member_count == 3
    assert publication.packed_byte_count == 12
    assert candidates.validate_v4_inferred_taxonomy_projection_manifest(
        publication.manifest
    ) == publication.manifest
    expected_caps_by_name = {
        "max_online_filtered_reverse_code_sets": 6_600,
        "max_online_filtered_reverse_code_occurrences": 6_700,
        "max_online_inferred_taxonomy_candidates": 37_000,
        "max_online_candidate_pattern_projection_members": 131_072,
        "max_online_inferred_taxonomy_retained_memberships": 65_536,
        "max_online_inferred_taxonomy_graph_pages": 256,
        "max_online_inferred_taxonomy_graph_bytes": 4_194_304,
        "max_online_inferred_taxonomy_graph_batches": 32,
        "pattern_count": 0,
        "pattern_member_count": 0,
        "pattern_member_bytes": 0,
    }
    assert {
        name: publication.manifest[name] for name in expected_caps_by_name
    } == expected_caps_by_name
    assert {
        stored_projection["representation"]
        for stored_projection in session.stored_rows
    } == {"direct_v1"}


def _projection_row(
    rule: InferredProviderTaxonomyRule,
    npi_keys: tuple[int, ...] = (0, 2),
    npi_keys_by_pattern: dict[int, tuple[int, ...]] | None = None,
) -> dict[str, Any]:
    rule_digest = candidates.inferred_provider_taxonomy_rule_digest(rule)
    member_payload = candidates.pack_inferred_taxonomy_npi_keys(npi_keys)
    pattern_members = npi_keys_by_pattern or {}
    pattern_payload = candidates.pack_inferred_taxonomy_pattern_npi_keys(
        pattern_members
    )
    representation = (
        candidates.PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        if pattern_payload
        else candidates.PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
    )
    pattern_member_count = sum(
        len(pattern_npi_keys)
        for pattern_npi_keys in pattern_members.values()
    )
    return {
        "rule_digest": rule_digest,
        "catalog_contract": (
            candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        ),
        "catalog_digest": b"c" * 32,
        "vector_format": candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": candidates.inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=member_payload,
        ),
        "member_keys": member_payload,
        "representation": representation,
        "pattern_count": len(pattern_members),
        "pattern_member_count": pattern_member_count,
        "pattern_member_bytes": len(pattern_payload),
        "pattern_member_digest": (
            candidates.inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=representation,
                pattern_count=len(pattern_members),
                pattern_member_count=pattern_member_count,
                packed_pattern_payload=pattern_payload,
            )
        ),
        "pattern_member_payload": pattern_payload,
    }


def _observe_projection_row(
    rule: InferredProviderTaxonomyRule,
    npi_keys: tuple[int, ...],
) -> dict[str, Any]:
    rule_digest = candidates.inferred_provider_taxonomy_rule_digest(rule)
    member_payload = candidates.pack_inferred_taxonomy_npi_keys(npi_keys)
    representation = (
        candidates.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
    )
    return {
        "rule_digest": rule_digest,
        "catalog_contract": (
            candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        ),
        "catalog_digest": b"o" * 32,
        "vector_format": candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": candidates.inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=member_payload,
        ),
        "member_keys": member_payload,
        "representation": representation,
        "observe_reason": (
            candidates.PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
        ),
        "observe_count_lower_bound": len(npi_keys),
        "pattern_count": 0,
        "pattern_member_count": 0,
        "pattern_member_bytes": 0,
        "pattern_member_digest": (
            candidates.inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=representation,
                pattern_count=0,
                pattern_member_count=0,
                packed_pattern_payload=b"",
            )
        ),
        "pattern_member_payload": b"",
    }


def test_manifest_validator_rejects_invalid_top_level_shapes() -> None:
    """Reject malformed containers, counters, and rule entries before sealing."""

    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        (_projection_row(_rules()[0]),),
        npi_count=3,
        pattern_count=0,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="manifest is invalid"):
        candidates.validate_v4_inferred_taxonomy_projection_manifest([])

    missing_field = deepcopy(manifest)
    missing_field.pop("contract")
    boolean_count = deepcopy(manifest)
    boolean_count["rule_count"] = True
    invalid_rule_container = deepcopy(manifest)
    invalid_rule_container["rules"] = {}
    invalid_rule_entry = deepcopy(manifest)
    invalid_rule_entry["rules"] = [{}]
    invalid_observe_entry = deepcopy(manifest)
    invalid_observe_entry["observe_only_rules"] = [{}]
    malformed_manifests = (
        missing_field,
        boolean_count,
        invalid_rule_container,
        invalid_rule_entry,
        invalid_observe_entry,
    )
    for malformed_manifest in malformed_manifests:
        with pytest.raises(PTG2ManifestArtifactError):
            candidates.validate_v4_inferred_taxonomy_projection_manifest(
                malformed_manifest
            )


def _reader_row(
    projection_row: dict[str, Any],
    *,
    member_keys: bytes | None = None,
    pattern_member_payload: bytes | None = None,
    root_pattern_count: int | None = None,
) -> dict[str, Any]:
    candidate_payload = (
        projection_row["member_keys"]
        if member_keys is None
        else member_keys
    )
    pattern_payload = (
        projection_row["pattern_member_payload"]
        if pattern_member_payload is None
        else pattern_member_payload
    )
    if root_pattern_count is None:
        root_pattern_count = (
            max(projection_row.get("pattern_count", 0), 10)
            if projection_row.get("representation") == "pattern_v1"
            else 0
        )
    return {
        "catalog_contract": projection_row["catalog_contract"],
        "catalog_digest": projection_row["catalog_digest"],
        "vector_format": projection_row["vector_format"],
        "member_count": projection_row["member_count"],
        "member_digest": projection_row["member_digest"],
        "member_keys": candidate_payload,
        "member_bytes": len(candidate_payload),
        "representation": projection_row["representation"],
        "pattern_count": projection_row["pattern_count"],
        "pattern_member_count": projection_row["pattern_member_count"],
        "pattern_member_bytes": projection_row["pattern_member_bytes"],
        "pattern_member_digest": projection_row["pattern_member_digest"],
        "pattern_member_payload": pattern_payload,
        "pattern_payload_bytes": len(pattern_payload),
        "root_state": "complete",
        "npi_count": 3,
        "root_pattern_count": root_pattern_count,
    }


@pytest.mark.parametrize("npi_count", (True, -1))
@pytest.mark.asyncio
async def test_publication_rejects_invalid_npi_dictionary_bounds(npi_count) -> None:
    """Reject boolean and out-of-range NPI dictionary bounds before storage work."""

    with pytest.raises(ValueError, match="NPI count"):
        await candidates.publish_v4_inferred_taxonomy_candidates(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            rules=(_rules()[0],),
            npi_count=npi_count,
            representation="direct_v1",
            pattern_count=0,
        )


@pytest.mark.parametrize(
    ("catalog_row", "message"),
    (
        (
            {
                "npi_key": 3,
                "npi": 1_234_567_890,
                "matched_taxonomy_codes": ["AAA"],
            },
            "NPI key",
        ),
        (
            {
                "npi_key": 0,
                "npi": 999,
                "matched_taxonomy_codes": ["AAA"],
            },
            "NPI is invalid",
        ),
        (
            {
                "npi_key": 0,
                "npi": 1_234_567_890,
                "matched_taxonomy_codes": [],
            },
            "catalog evidence",
        ),
    ),
)
@pytest.mark.asyncio
async def test_publication_rejects_invalid_catalog_evidence(
    monkeypatch,
    catalog_row,
    message,
) -> None:
    """Reject catalog rows that escape the sealed NPI and taxonomy identities."""

    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
    )
    session = _PublicationSession(
        catalog_rows_by_codes={("AAA",): (catalog_row,)}
    )
    with pytest.raises(RuntimeError, match=message):
        await candidates.publish_v4_inferred_taxonomy_candidates(
            session,
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            rules=(_rules()[0],),
            npi_count=3,
            representation="direct_v1",
            pattern_count=0,
        )


@pytest.mark.asyncio
async def test_publication_is_individual_only_packed_and_rule_stable(
    monkeypatch,
) -> None:
    """Publish stable individual candidates and authenticate their rule set."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )
    session = _PublicationSession()
    publication = await _publish_candidate_projection(
        session,
        representation="direct_v1",
        pattern_count=0,
    )

    _assert_direct_publication_contract(publication, session)

    first_rule = _rules()[0]
    renamed_rule = replace(first_rule, display_terms=("renamed",))
    changed_rule = replace(first_rule, ranges=((11, 19),))
    assert candidates.inferred_provider_taxonomy_rule_digest(
        first_rule
    ) == candidates.inferred_provider_taxonomy_rule_digest(renamed_rule)
    assert candidates.inferred_provider_taxonomy_rule_digest(
        first_rule
    ) != candidates.inferred_provider_taxonomy_rule_digest(changed_rule)
    assert candidates.inferred_provider_taxonomy_rule_set_digest(
        _rules()
    ) != candidates.inferred_provider_taxonomy_rule_set_digest(
        (changed_rule, _rules()[1])
    )


@pytest.mark.asyncio
async def test_publication_accepts_rule_scoped_pattern_projection(
    monkeypatch,
) -> None:
    """Publish exact rule-scoped pattern postings under the sealed root."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )
    graph_calls: list[dict[str, Any]] = []

    async def lookup_building_patterns(_session, **kwargs):
        graph_calls.append(dict(kwargs))
        return {
            npi_key: ((2, 9) if npi_key == 0 else (2,))
            for npi_key in kwargs["owner_keys"]
        }

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        lookup_building_patterns,
    )
    first_rule_digest = candidates.inferred_provider_taxonomy_rule_digest(
        _rules()[0]
    )
    session = _PublicationSession()
    publication = await _publish_candidate_projection(
        session,
        representation="pattern_v1",
        pattern_count=10,
    )

    row_by_rule_digest = {
        stored_row["rule_digest"]: stored_row
        for stored_row in session.stored_rows
    }
    factored = row_by_rule_digest[first_rule_digest]
    assert factored["representation"] == "pattern_v1"
    assert factored["pattern_count"] == 2
    assert factored["pattern_member_count"] == 3
    assert candidates.unpack_inferred_taxonomy_pattern_npi_keys(
        factored["pattern_member_payload"],
        pattern_count=2,
        pattern_member_count=3,
    ) == {2: (0, 2), 9: (0,)}
    assert publication.pattern_count == 3
    assert publication.pattern_member_count == 4
    assert {
        stored_row["representation"] for stored_row in session.stored_rows
    } == {"pattern_v1"}
    assert len(graph_calls) == 2
    assert all(call["relation"] == "npi_patterns" for call in graph_calls)
    assert all(call["build_token"] == "build-token" for call in graph_calls)
    assert all(call["snapshot_key"] == 41 for call in graph_calls)
    assert all(
        call["max_members"] == 131_072 for call in graph_calls
    )


@pytest.mark.asyncio
async def test_publication_bounds_rule_catalog_before_materialization(
    monkeypatch,
) -> None:
    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
    )
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    session = _PublicationSession()
    publication = await candidates.publish_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation="direct_v1",
        pattern_count=0,
    )

    assert all(call["candidate_limit"] == 2 for call in session.catalog_calls)
    assert all(
        len(call["taxonomy_codes"]) == 1 for call in session.catalog_calls
    )
    assert publication.rule_count == 1
    assert publication.observe_only_rule_count == 1
    assert {
        stored_row["representation"] for stored_row in session.stored_rows
    } == {"direct_v1", "observe_v1"}
    assert publication.manifest["observe_only_rules"][0]["status"] == (
        "observe_only"
    )
    assert publication.manifest["observe_only_rules"][0]["reason"] == (
        "candidate_cap_exceeded"
    )
    assert publication.manifest["observe_only_rules"][0][
        "observed_count_lower_bound"
    ] == 2
    summarized = await candidates.summarize_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        npi_count=3,
        pattern_count=0,
        rules=_rules(),
    )
    assert summarized == publication.manifest


@pytest.mark.asyncio
async def test_publication_can_seal_an_all_observe_rule_set(monkeypatch) -> None:
    """Seal a rule set whose every member vector exceeds the online cap."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    session = _PublicationSession(
        catalog_rows_by_codes={
            ("AAA",): (
                {
                    "npi_key": 0,
                    "npi": 1_234_567_890,
                    "matched_taxonomy_codes": ["AAA"],
                },
                {
                    "npi_key": 1,
                    "npi": 1_234_567_891,
                    "matched_taxonomy_codes": ["AAA"],
                },
            ),
            ("BBB",): (
                {
                    "npi_key": 1,
                    "npi": 1_234_567_891,
                    "matched_taxonomy_codes": ["BBB"],
                },
                {
                    "npi_key": 2,
                    "npi": 1_234_567_892,
                    "matched_taxonomy_codes": ["BBB"],
                },
            ),
        }
    )
    publication = await _publish_candidate_projection(
        session,
        representation="pattern_v1",
        pattern_count=10,
    )

    assert publication.rule_count == 0
    assert publication.observe_only_rule_count == 2
    assert publication.manifest["rules"] == []
    assert len(publication.manifest["observe_only_rules"]) == 2
    assert {
        stored_row["representation"] for stored_row in session.stored_rows
    } == {"observe_v1"}
    assert candidates.validate_v4_inferred_taxonomy_projection_manifest(
        publication.manifest
    ) == publication.manifest


@pytest.mark.asyncio
async def test_pattern_publication_rejects_root_and_projection_drift(
    monkeypatch,
) -> None:
    """Reject pattern evidence that changes its root or candidate coverage."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )

    async def out_of_root_patterns(_session, **kwargs):
        return {npi_key: (2,) for npi_key in kwargs["owner_keys"]}

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        out_of_root_patterns,
    )
    with pytest.raises(RuntimeError, match="outside its root"):
        await _publish_candidate_projection(
            _PublicationSession(),
            representation="pattern_v1",
            pattern_count=2,
        )

    async def excessive_patterns(_session, **kwargs):
        return {npi_key: (0,) for npi_key in kwargs["owner_keys"]}

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        excessive_patterns,
    )
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS",
        1,
    )
    publication = await _publish_candidate_projection(
        _PublicationSession(),
        representation="pattern_v1",
        pattern_count=2,
    )
    assert publication.rule_count == 1
    assert publication.observe_only_rule_count == 1
    assert {
        rule_manifest["reason"]
        for rule_manifest in publication.manifest["observe_only_rules"]
    } == {"pattern_projection_cap_exceeded"}
    assert {
        rule_manifest["observed_count_lower_bound"]
        for rule_manifest in publication.manifest["observe_only_rules"]
    } == {2}


@pytest.mark.asyncio
async def test_pattern_publication_empty_evidence_is_explicit(
    monkeypatch,
) -> None:
    async def no_op_lock(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_lock,
    )
    pattern_lookup = AsyncMock(
        side_effect=lambda _session, **kwargs: {
            npi_key: () for npi_key in kwargs["owner_keys"]
        }
    )

    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        pattern_lookup,
    )
    with pytest.raises(RuntimeError, match="pattern evidence is incomplete"):
        await candidates.publish_v4_inferred_taxonomy_candidates(
            _PublicationSession(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            rules=_rules(),
            npi_count=3,
            representation="pattern_v1",
            pattern_count=10,
        )
    assert pattern_lookup.await_count == 1

    empty_session = _PublicationSession(
        catalog_rows_by_codes={("AAA",): (), ("BBB",): ()}
    )
    publication = await candidates.publish_v4_inferred_taxonomy_candidates(
        empty_session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        rules=_rules(),
        npi_count=3,
        representation="pattern_v1",
        pattern_count=10,
    )
    assert pattern_lookup.await_count == 1
    assert publication.member_count == 0
    assert publication.pattern_member_count == 0
    assert {
        stored_row["representation"]
        for stored_row in empty_session.stored_rows
    } == {"direct_v1"}


def test_pattern_posting_codec_is_compact_strict_and_deterministic() -> None:
    packed_pattern_payload = candidates.pack_inferred_taxonomy_pattern_npi_keys(
        {9: (2, 5), 2: (0, 2)}
    )
    assert len(packed_pattern_payload) == 24 + 2 * 8 + 4 * 4
    assert candidates.unpack_inferred_taxonomy_pattern_npi_keys(
        packed_pattern_payload,
        pattern_count=2,
        pattern_member_count=4,
    ) == {2: (0, 2), 9: (2, 5)}
    assert packed_pattern_payload == candidates.pack_inferred_taxonomy_pattern_npi_keys(
        {2: (0, 2), 9: (2, 5)}
    )

    with pytest.raises(ValueError, match="strict uint32 order"):
        candidates.pack_inferred_taxonomy_pattern_npi_keys({2: (2, 2)})
    with pytest.raises(PTG2ManifestArtifactError, match="trailing data"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            packed_pattern_payload + b"\x00",
            pattern_count=2,
            pattern_member_count=4,
        )
    with pytest.raises(PTG2ManifestArtifactError, match="truncated"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            packed_pattern_payload[:-1],
            pattern_count=2,
            pattern_member_count=4,
        )
    non_strict = bytearray(
        candidates.pack_inferred_taxonomy_pattern_npi_keys({2: (0, 2)})
    )
    non_strict[36:40] = (0).to_bytes(4, "little")
    with pytest.raises(PTG2ManifestArtifactError, match="NPI keys"):
        candidates.unpack_inferred_taxonomy_pattern_npi_keys(
            bytes(non_strict),
            pattern_count=1,
            pattern_member_count=2,
        )


@pytest.mark.parametrize(
    ("representation", "pattern_count"),
    (
        ("direct_v1", True),
        ("direct_v1", 1),
        ("pattern_v1", 0),
        ("unknown", 0),
    ),
)
def test_publication_rejects_inconsistent_root_identity(
    representation: str,
    pattern_count: int,
) -> None:
    with pytest.raises(ValueError, match="root identity"):
        candidates._normalized_root_identity(representation, pattern_count)


@pytest.mark.asyncio
async def test_pattern_projection_short_circuits_empty_and_rejects_gaps(
    monkeypatch,
) -> None:
    """Avoid graph work for empty candidates and reject incomplete graph replies."""

    assert await candidates._candidate_pattern_postings_for_rule(
        object(),
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        candidate_npi_keys=(),
        root_pattern_count=3,
    ) == {}
    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        AsyncMock(return_value={}),
    )
    with pytest.raises(RuntimeError, match="incomplete"):
        await candidates._candidate_pattern_postings_for_rule(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            candidate_npi_keys=(1,),
            root_pattern_count=3,
        )


@pytest.mark.parametrize(
    ("graph_message", "error_type", "message"),
    (
        (
            "different graph failure",
            PTG2SharedBlockError,
            "different graph failure",
        ),
        (
            "PTG V4 graph selection exceeds max_members",
            candidates._PatternProjectionCapExceeded,
            "pattern projection exceeds the online cap",
        ),
    ),
)
@pytest.mark.asyncio
async def test_pattern_projection_preserves_graph_failure_semantics(
    monkeypatch,
    graph_message: str,
    error_type: type[Exception],
    message: str,
) -> None:
    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        AsyncMock(side_effect=PTG2SharedBlockError(graph_message)),
    )

    with pytest.raises(error_type, match=message):
        await candidates._candidate_pattern_postings_for_rule(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            candidate_npi_keys=(1,),
            root_pattern_count=3,
        )


@pytest.mark.asyncio
async def test_pattern_projection_rejects_boolean_pattern_keys(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        v4_graph,
        "lookup_building_v4_relation_members",
        AsyncMock(return_value={1: (True,)}),
    )

    with pytest.raises(RuntimeError, match="pattern key is invalid"):
        await candidates._candidate_pattern_postings_for_rule(
            object(),
            schema_name="mrf",
            snapshot_key=41,
            build_token="build-token",
            candidate_npi_keys=(1,),
            root_pattern_count=3,
        )


def test_v2_row_shaper_rejects_missing_candidate_and_pattern_bound() -> None:
    missing_candidate = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0,)},
    )
    with pytest.raises(RuntimeError, match="projection is incomplete"):
        candidates.shape_v4_inferred_taxonomy_projection_manifest(
            (missing_candidate,),
            npi_count=3,
            pattern_count=10,
        )

    out_of_range_pattern = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    with pytest.raises(RuntimeError, match="exceeds its pattern root"):
        candidates.shape_v4_inferred_taxonomy_projection_manifest(
            (out_of_range_pattern,),
            npi_count=3,
            pattern_count=9,
        )


@pytest.mark.parametrize(
    "cap_field",
    (
        "max_online_inferred_taxonomy_candidates",
        "max_online_candidate_pattern_projection_members",
    ),
)
def test_manifest_validator_enforces_per_rule_caps(cap_field: str) -> None:
    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        (
            _projection_row(
                _rules()[0],
                npi_keys_by_pattern={9: (0, 2)},
            ),
        ),
        npi_count=3,
        pattern_count=10,
    )
    manifest[cap_field] = 1

    with pytest.raises(PTG2ManifestArtifactError, match="projection rule"):
        candidates.validate_v4_inferred_taxonomy_projection_manifest(
            manifest
        )


def test_observe_rule_is_explicit_fallback_and_status_is_authenticated(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    observe_row = _observe_projection_row(_rules()[0], (0, 2))
    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        (observe_row,),
        npi_count=3,
        pattern_count=0,
    )

    assert candidates.resolve_inferred_taxonomy_projection_rule_manifest(
        manifest,
        observe_row["rule_digest"],
    ) is None
    with pytest.raises(PTG2ManifestArtifactError, match="observe-only"):
        candidates.inferred_taxonomy_projection_rule_manifest(
            manifest,
            observe_row["rule_digest"],
        )

    tampered = deepcopy(manifest)
    tampered["observe_only_rules"][0]["reason"] = "not-the-sealed-reason"
    with pytest.raises(PTG2ManifestArtifactError, match="observe rule"):
        candidates.validate_v4_inferred_taxonomy_projection_manifest(tampered)

    missing = deepcopy(manifest)
    missing["observe_only_rules"] = []
    missing["observe_only_rule_count"] = 0
    with pytest.raises(PTG2ManifestArtifactError):
        candidates.resolve_inferred_taxonomy_projection_rule_manifest(
            missing,
            observe_row["rule_digest"],
        )


def test_projection_rule_resolution_scans_and_rejects_unknown_digest() -> None:
    projection_rows = tuple(
        sorted(
            (_projection_row(rule) for rule in _rules()),
            key=lambda projection_row: projection_row["rule_digest"],
        )
    )
    manifest = candidates.shape_v4_inferred_taxonomy_projection_manifest(
        projection_rows,
        npi_count=3,
        pattern_count=0,
    )

    resolved = candidates.resolve_inferred_taxonomy_projection_rule_manifest(
        manifest,
        projection_rows[1]["rule_digest"],
    )
    assert resolved is not None
    assert resolved.rule_digest == projection_rows[1]["rule_digest"]
    with pytest.raises(PTG2ManifestArtifactError, match="not in the sealed"):
        candidates.resolve_inferred_taxonomy_projection_rule_manifest(
            manifest,
            b"x" * 32,
        )


def test_compatibility_manifest_accepts_an_empty_direct_projection() -> None:
    empty_projection = _projection_row(_rules()[0], npi_keys=())

    manifest = candidates._candidate_projection_manifest((empty_projection,))

    assert manifest["rules"][0]["member_count"] == 0
    assert manifest["pattern_count"] == 0


@pytest.mark.asyncio
async def test_persisted_summary_enforces_global_pattern_bound() -> None:
    projection_row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    session = _ScriptedSession(_Result((projection_row,)))

    with pytest.raises(RuntimeError, match="exceeds its pattern root"):
        await candidates.summarize_v4_inferred_taxonomy_candidates(
            session,
            schema_name="mrf",
            snapshot_key=41,
            npi_count=3,
            pattern_count=9,
            rules=(_rules()[0],),
        )


@pytest.mark.asyncio
async def test_reader_uses_one_bounded_authenticated_round_trip() -> None:
    projection_row = _projection_row(
        _rules()[0],
        npi_keys_by_pattern={9: (0, 2)},
    )
    manifest = candidates._candidate_projection_manifest((projection_row,))
    session = _ScriptedSession(_Result((_reader_row(projection_row),)))

    loaded = await candidates.load_v4_inferred_taxonomy_candidates(
        session,
        snapshot_key=41,
        rule_digest=projection_row["rule_digest"],
        schema_name="mrf",
        projection_manifest=manifest,
    )

    assert loaded.npi_keys == (0, 2)
    assert loaded.representation == "pattern_v1"
    assert loaded.npi_keys_by_pattern == {9: (0, 2)}
    assert len(session.calls) == 1
    assert "candidate.member_keys" in session.calls[0][0]
    assert "candidate.pattern_member_payload" in session.calls[0][0]


@pytest.mark.asyncio
async def test_reader_rejects_manifest_payload_and_cap_tamper() -> None:
    """Reject sealed manifest, payload, root, and cap drift before serving."""

    projection_row = _projection_row(_rules()[0])
    manifest = candidates._candidate_projection_manifest((projection_row,))
    tampered_manifest = deepcopy(manifest)
    tampered_manifest["projection_digest"] = "0" * 64
    no_query_session = _ScriptedSession()
    await _assert_candidate_load_rejected(
        no_query_session, projection_row, tampered_manifest, "digest changed"
    )
    assert no_query_session.calls == []

    tampered_payload = bytearray(projection_row["member_keys"])
    tampered_payload[-1] ^= 1
    payload_session = _ScriptedSession(
        _Result(
            (
                _reader_row(
                    projection_row,
                    member_keys=bytes(tampered_payload),
                ),
            )
        )
    )
    await _assert_candidate_load_rejected(
        payload_session, projection_row, manifest, "digest changed"
    )

    pattern_row, pattern_manifest, pattern_session = (
        _tampered_pattern_projection()
    )
    await _assert_candidate_load_rejected(
        pattern_session,
        pattern_row,
        pattern_manifest,
        "pattern digest changed",
    )

    pattern_bound_session = _ScriptedSession(
        _Result((_reader_row(pattern_row, root_pattern_count=9),))
    )
    await _assert_candidate_load_rejected(
        pattern_bound_session,
        pattern_row,
        pattern_manifest,
        "violates its root",
    )

    capped_manifest = deepcopy(manifest)
    capped_manifest["max_online_inferred_taxonomy_candidates"] = 1
    capped_session = _ScriptedSession()
    await _assert_candidate_load_rejected(
        capped_session, projection_row, capped_manifest, "projection rule"
    )
    assert capped_session.calls == []


@pytest.mark.parametrize(
    ("metadata_updates", "message"),
    (
        (None, "vector is unavailable"),
        ({"root_state": "building"}, "snapshot is not complete"),
        ({"catalog_contract": "incompatible"}, "contract is incompatible"),
        ({"member_bytes": 7}, "metadata is inconsistent"),
        ({"catalog_digest": b"z" * 32}, "metadata changed from its seal"),
    ),
)
@pytest.mark.asyncio
async def test_reader_rejects_unsealed_metadata_states(
    metadata_updates: dict[str, Any] | None,
    message: str,
) -> None:
    projection_row = _projection_row(_rules()[0])
    manifest = candidates._candidate_projection_manifest((projection_row,))
    metadata_rows = ()
    if metadata_updates is not None:
        metadata_by_field = _reader_row(projection_row)
        metadata_by_field.update(metadata_updates)
        metadata_rows = (metadata_by_field,)

    await _assert_candidate_load_rejected(
        _ScriptedSession(_Result(metadata_rows)),
        projection_row,
        manifest,
        message,
    )


def _sealed_taxonomy_reservation_fixture() -> tuple[
    dict[str, Any],
    dict[str, Any],
]:
    projection = candidates._candidate_projection_manifest(
        (_projection_row(_rules()[0]),)
    )
    summary = _summary()
    metadata = replace(
        _metadata(),
        inferred_taxonomy_candidates=projection,
    )
    layout_manifest = snapshot_maps._manifest_with_v4_root(
        {},
        representation="pattern_v1",
        summary=summary,
        metadata=metadata,
    )
    existing_root_map = {
        "snapshot_key": 41,
        "layout_manifest": layout_manifest,
        "layout_mapping_digest": summary.map_digest,
        "root_state": "complete",
        "root_format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "representation": "pattern_v1",
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
        "map_digest": summary.map_digest,
        "object_kind_count": summary.object_kind_count,
        "map_pack_count": summary.map_pack_count,
        "coordinate_count": summary.coordinate_count,
        "entry_count": summary.entry_count,
        "logical_byte_count": summary.logical_byte_count,
        "stored_map_byte_count": summary.stored_map_byte_count,
        "npi_count": metadata.npi_count,
        "component_count": metadata.component_count,
        "pattern_count": metadata.pattern_count,
        "relation_count": metadata.relation_count,
        "heavy_owner_count": metadata.heavy_owner_count,
    }
    return layout_manifest, existing_root_map


def test_seal_and_reuse_validation_reject_projection_manifest_drift() -> None:
    """Sealed reuse rejects a taxonomy projection changed after publication."""

    layout_manifest, existing_root_map = _sealed_taxonomy_reservation_fixture()
    assert snapshot_maps._validate_sealed_reservation(
        existing_root_map
    ) == layout_manifest


    tampered = deepcopy(existing_root_map)
    projection_manifest = tampered["layout_manifest"]["serving_index"][
        "serving_binary"
    ]["provider_graph_v4"]["inferred_taxonomy_candidates"]
    projection_manifest["rules"][0]["member_count"] += 1
    with pytest.raises(PTG2ManifestArtifactError, match="projection rule"):
        snapshot_maps._validate_sealed_reservation(tampered)


def test_sealed_reservation_without_snapshot_map_has_no_object_kinds() -> None:
    """Legacy manifest gaps reconstruct an empty immutable map-kind tuple."""

    _, existing_root_map = _sealed_taxonomy_reservation_fixture()
    absent_snapshot_map = deepcopy(existing_root_map)
    absent_snapshot_map["layout_manifest"]["serving_index"].pop(
        "snapshot_map"
    )
    _, absent_map_summary, _ = snapshot_maps._sealed_root_summaries(
        absent_snapshot_map
    )
    assert absent_map_summary.object_kinds == ()


def test_sealed_reservation_rejects_incomplete_root() -> None:
    """A building root cannot be reused as a sealed V4 reservation."""

    _, existing_root_map = _sealed_taxonomy_reservation_fixture()
    incomplete_root = deepcopy(existing_root_map)
    incomplete_root["root_state"] = "building"
    with pytest.raises(RuntimeError, match="reuse root is inconsistent"):
        snapshot_maps._validate_sealed_reservation(incomplete_root)


def test_migration_installs_guard_and_cascading_snapshot_ownership() -> None:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260724120000_ptg2_v4_taxonomy_candidates.py"
    )
    spec = importlib.util.spec_from_file_location(
        "test_ptg2_v4_taxonomy_candidate_migration",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    class _Recorder:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, statement) -> None:
            self.sql.append(str(statement))

    recorder = _Recorder()
    migration.op = recorder
    migration._schema = lambda: "mrf"
    migration.upgrade()
    upgrade_sql = " ".join(
        " ".join(statement.split()) for statement in recorder.sql
    )
    assert "ptg2_v4_inferred_taxonomy_candidate_root_fkey" in upgrade_sql
    assert "ON DELETE CASCADE" in upgrade_sql
    assert "guard_ptg2_v4_snapshot_metadata" in upgrade_sql
    assert "max_online_filtered_reverse_code_sets" not in upgrade_sql
    assert "representation varchar(16) NOT NULL" in upgrade_sql
    assert "pattern_member_payload bytea NOT NULL" in upgrade_sql
    assert "pattern_member_digest bytea NOT NULL" in upgrade_sql
    assert "'direct_v1', 'pattern_v1', 'observe_v1'" in upgrade_sql
    assert "representation = 'observe_v1'" in upgrade_sql
    assert "member_count = 37001" in upgrade_sql
    assert "pattern_member_bytes = 0" in upgrade_sql
    assert "pattern_member_count >= pattern_count" in upgrade_sql
    assert "pattern_member_bytes = 24" in upgrade_sql

    migration.downgrade()
    assert "DROP TABLE IF EXISTS" in recorder.sql[-1]
