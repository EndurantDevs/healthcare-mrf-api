from __future__ import annotations

from dataclasses import replace

import pytest

from process.ptg_parts.domain import (
    PTG2DownloadedJob,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)
from process.ptg_parts.import_rows import _ptg2_source_trace_rows
from process.ptg_parts.ptg2_shared_reuse import (
    deterministic_source_key_assignments,
    normalized_physical_artifact_identity,
    same_downloaded_physical_input,
    shared_logical_artifact_metadata,
    shared_physical_artifact_identity,
    shared_physical_input_identity,
    shared_snapshot_source_assignments,
)


def _downloaded(
    *,
    raw_sha: str = "a" * 64,
    logical_sha: str = "b" * 64,
    url: str = "https://example.invalid/rates.json.gz",
    plan_id: str = "plan-1",
    sponsor: str = "logical owner one",
    raw_bytes: int = 100,
    logical_bytes: int = 1000,
    logical_hash_deferred: bool = False,
) -> PTG2DownloadedJob:
    raw = PTG2RawArtifact(
        original_url=url,
        canonical_url=url,
        raw_path="/tmp/input.gz",
        raw_storage_uri="file:///tmp/input.gz",
        raw_sha256=raw_sha,
        byte_count=raw_bytes,
    )
    logical = PTG2LogicalArtifact(
        logical_path="/tmp/input.gz",
        logical_sha256=logical_sha,
        byte_count=logical_bytes,
        compression="gzip",
        logical_hash_deferred=logical_hash_deferred,
    )
    return PTG2DownloadedJob(
        job={
            "type": "in_network",
            "url": url,
            "plan_info": [
                {
                    "plan_id": plan_id,
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                    "plan_sponsor_name": sponsor,
                }
            ],
            "source_network_names": ["network b", "network a"],
        },
        raw_artifact=raw,
        logical_artifact=logical,
    )


def _identity(downloaded_jobs):
    return shared_physical_input_identity(
        downloaded_jobs,
        options={
            "snapshot_arch": "postgres_binary_v3",
            "snapshot_arch_variant": "shared_blocks_v3",
            "hash_mode": "checksum64",
            "source_key": "logical-owner-is-excluded",
            "toc_urls": ["https://example.invalid/index.json"],
        },
        scanner_canon_version={"version": 7},
    )


def test_same_files_and_semantics_reuse_across_logical_owners_and_urls():
    first = _downloaded()
    second = _downloaded(
        url="https://mirror.invalid/equal.gz",
        plan_id="plan-2",
        sponsor="another logical owner",
    )

    assert _identity([first]).semantic_fingerprint == _identity([second]).semantic_fingerprint
    assert _identity([first]).coverage_scope_id == _identity([second]).coverage_scope_id
    assert _identity([first]).logical_plan.plan_id == "plan-1"
    assert _identity([second]).logical_plan.plan_id == "plan-2"
    assert same_downloaded_physical_input(first, second)


def test_plan_scope_is_logical_but_content_changes_physical_identity():
    baseline = _identity([_downloaded()]).semantic_fingerprint
    other_plan = _identity([_downloaded(plan_id="plan-2")]).semantic_fingerprint
    repacked_content = _identity([_downloaded(raw_sha="c" * 64)]).semantic_fingerprint
    other_content = _identity([_downloaded(logical_sha="c" * 64)]).semantic_fingerprint

    assert baseline == other_plan
    assert baseline == repacked_content
    assert baseline != other_content


def test_artifact_order_does_not_change_identity_but_duplicate_scope_is_preserved():
    first = _downloaded(raw_sha="a" * 64)
    second = _downloaded(raw_sha="c" * 64, logical_sha="c" * 64)

    forward = _identity([first, second])
    reverse = _identity([second, first])

    assert forward.semantic_fingerprint == reverse.semantic_fingerprint
    assert forward.artifact_count == 2
    assert forward.identity_byte_count == 2000
    assert forward.payload["identity_version"] == 6
    assert forward.payload["price_membership_semantics"] == "multiset_v1"
    assert forward.payload["serving_multiplicity_semantics"] == "source_multiset_v1"


def test_duplicate_bytes_with_different_plan_scope_are_physically_equal():
    first = _downloaded(plan_id="plan-1")
    second = _downloaded(plan_id="plan-2")

    assert same_downloaded_physical_input(first, second)


def test_deferred_logical_hash_reuses_only_byte_identical_containers():
    first = _downloaded(
        raw_sha="a" * 64,
        logical_sha="a" * 64,
        raw_bytes=100,
        logical_bytes=100,
        logical_hash_deferred=True,
    )
    same_container = _downloaded(
        raw_sha="a" * 64,
        logical_sha="a" * 64,
        raw_bytes=100,
        logical_bytes=100,
        logical_hash_deferred=True,
        url="https://mirror.invalid/same.gz",
    )
    repacked_container = _downloaded(
        raw_sha="c" * 64,
        logical_sha="c" * 64,
        raw_bytes=101,
        logical_bytes=101,
        logical_hash_deferred=True,
        url="https://mirror.invalid/repacked.gz",
    )

    assert same_downloaded_physical_input(first, same_container)
    assert _identity([first]).semantic_fingerprint == _identity([same_container]).semantic_fingerprint
    assert not same_downloaded_physical_input(first, repacked_container)
    assert _identity([first]).semantic_fingerprint != _identity([repacked_container]).semantic_fingerprint
    assert _identity([first]).payload["artifacts"][0]["identity_kind"] == "raw_container_sha256_v1"


def test_one_snapshot_cannot_mix_logical_plan_scopes():
    with pytest.raises(ValueError, match="exactly one logical plan scope"):
        _identity([_downloaded(plan_id="plan-1"), _downloaded(plan_id="plan-2")])


def test_multiple_plan_records_can_share_one_logical_scope():
    downloaded = _downloaded()
    downloaded.job["plan_info"] = [
        {
            "plan_name": "first benefit design",
            "plan_id": "plan-1",
            "plan_id_type": "EIN",
            "plan_market_type": "Group",
            "plan_sponsor_name": "logical owner one",
        },
        {
            "plan_name": "second benefit design",
            "plan_id": "plan-1",
            "plan_id_type": "ein",
            "plan_market_type": "group",
            "plan_sponsor_name": "logical owner one",
        },
    ]

    identity = _identity([downloaded])

    assert identity.logical_plan.plan_id == "plan-1"
    assert identity.logical_plan.plan_id_type == "ein"
    assert identity.logical_plan.plan_market_type == "group"
    assert identity.logical_plan_fields["plan_name"] is None
    assert identity.logical_plan_fields["plan_sponsor_name"] == "logical owner one"


def test_multiple_plan_records_with_different_scopes_fail_closed():
    downloaded = _downloaded()
    downloaded.job["plan_info"] = [
        {
            "plan_id": "plan-1",
            "plan_id_type": "ein",
            "plan_market_type": "group",
        },
        {
            "plan_id": "plan-2",
            "plan_id_type": "ein",
            "plan_market_type": "group",
        },
    ]

    with pytest.raises(ValueError, match="unambiguous logical plan id"):
        _identity([downloaded])


def test_plan_descriptive_metadata_is_merged_deterministically():
    missing = _downloaded(sponsor="")
    populated = _downloaded(
        raw_sha="c" * 64,
        logical_sha="c" * 64,
        sponsor="canonical owner",
    )

    identity = _identity([missing, populated])

    assert identity.logical_plan_fields["plan_sponsor_name"] == "canonical owner"


def test_conflicting_plan_descriptive_metadata_fails_closed():
    first = _downloaded(sponsor="owner one")
    second = _downloaded(
        raw_sha="c" * 64,
        logical_sha="c" * 64,
        sponsor="owner two",
    )

    with pytest.raises(ValueError, match="conflicting plan_sponsor_name"):
        _identity([first, second])


def test_missing_or_failed_artifact_fails_closed():
    with pytest.raises(ValueError, match="download failed"):
        _identity([PTG2DownloadedJob(job={"type": "in_network"}, error="network error")])

    invalid = _downloaded()
    invalid = replace(
        invalid,
        raw_artifact=replace(invalid.raw_artifact, raw_sha256="not-a-hash"),
    )
    with pytest.raises(ValueError, match="raw_sha256"):
        _identity([invalid])


def test_dense_source_keys_are_sorted_by_complete_physical_identity():
    first = _downloaded(logical_sha="f" * 64)
    second = _downloaded(logical_sha="1" * 64, raw_sha="2" * 64)

    forward = deterministic_source_key_assignments(
        [
            shared_physical_artifact_identity(first),
            shared_physical_artifact_identity(second),
            shared_physical_artifact_identity(first),
        ]
    )
    reverse = deterministic_source_key_assignments(
        [
            shared_physical_artifact_identity(second),
            shared_physical_artifact_identity(first),
        ]
    )

    assert forward == reverse
    assert [source_key for source_key, _identity_value in forward] == [0, 1]
    assert [identity.identity_sha256 for _key, identity in forward] == ["1" * 64, "f" * 64]


def test_unknown_physical_identity_kind_fails_closed():
    with pytest.raises(ValueError, match="physical artifact identity is incomplete"):
        normalized_physical_artifact_identity(
            {
                "source_type": "in_network",
                "identity_kind": "unknown_sha256_v1",
                "identity_sha256": "a" * 64,
            }
        )


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        (" IN_NETWORK ", "in_network"),
        ("a" + ".-_0" * 15 + "xyz", "a" + ".-_0" * 15 + "xyz"),
    ],
)
def test_physical_source_type_normalizes_to_lowercase_ascii_token(
    source_type,
    expected,
):
    identity = normalized_physical_artifact_identity(
        {
            "source_type": source_type,
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "a" * 64,
        }
    )

    assert identity.source_type == expected
    assert len(identity.source_type.encode("ascii")) <= 64


@pytest.mark.parametrize(
    "source_type",
    [
        "",
        "_in_network",
        "in/network",
        "a" * 65,
        "caf\u00e9",
        "\u212a_network",
    ],
)
def test_physical_source_type_rejects_non_ascii_or_invalid_tokens(source_type):
    with pytest.raises(ValueError, match="source_type"):
        normalized_physical_artifact_identity(
            {
                "source_type": source_type,
                "identity_kind": "raw_container_sha256_v1",
                "identity_sha256": "a" * 64,
            }
        )


def test_duplicate_urls_for_one_physical_artifact_combine_trace_rows():
    downloaded = _downloaded()
    identity = shared_physical_artifact_identity(downloaded)
    artifact_metadata = shared_logical_artifact_metadata(downloaded)

    assignments, trace_sets = shared_snapshot_source_assignments(
        [
            {
                **identity.as_dict(),
                **artifact_metadata,
                "source_trace_hash": "1" * 64,
            },
            {
                **identity.as_dict(),
                **artifact_metadata,
                "source_trace_hash": "2" * 64,
            },
        ],
        expected_identities=[identity],
    )

    assert len(assignments) == 1
    assert assignments[0].source_key == 0
    assert assignments[0].source_trace_hashes == ("1" * 64, "2" * 64)
    assert trace_sets[0]["source_trace_hashes"] == ["1" * 64, "2" * 64]


def test_default_compact_hash_mode_produces_valid_shared_source_trace(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_HASH_MODE", raising=False)
    downloaded = _downloaded(
        logical_hash_deferred=True,
        logical_sha="a" * 64,
        logical_bytes=100,
    )
    identity = shared_physical_artifact_identity(downloaded)
    source_trace, _source_trace_set = _ptg2_source_trace_rows(
        None,
        str(downloaded.job["url"]),
    )

    assignments, _trace_sets = shared_snapshot_source_assignments(
        [
            {
                **identity.as_dict(),
                **shared_logical_artifact_metadata(downloaded),
                "source_trace_hash": source_trace["source_trace_hash"],
            }
        ],
        expected_identities=[identity],
    )

    assert assignments[0].source_trace_hashes == (
        source_trace["source_trace_hash"],
    )
    assert len(assignments[0].source_trace_set_hash) == 64
    assert assignments[0].source_trace_set_hash == _trace_sets[0][
        "source_trace_set_hash"
    ]


def test_cross_wrapper_reuse_keeps_logical_identity_but_records_current_raw_sha():
    first = _downloaded(raw_sha="1" * 64, logical_sha="a" * 64)
    second = _downloaded(
        raw_sha="2" * 64,
        logical_sha="a" * 64,
        url="https://mirror.invalid/repacked.json.gz",
    )

    assert _identity([first]).semantic_fingerprint == _identity([second]).semantic_fingerprint
    assert shared_physical_artifact_identity(first) == shared_physical_artifact_identity(second)
    assert shared_logical_artifact_metadata(first)["raw_container_sha256"] == "1" * 64
    assert shared_logical_artifact_metadata(second)["raw_container_sha256"] == "2" * 64


def test_one_source_key_fails_closed_on_ambiguous_raw_wrappers():
    first = _downloaded(raw_sha="1" * 64, logical_sha="a" * 64)
    second = _downloaded(raw_sha="2" * 64, logical_sha="a" * 64)
    identity = shared_physical_artifact_identity(first)

    with pytest.raises(ValueError, match="ambiguous artifact metadata"):
        shared_snapshot_source_assignments(
            [
                {
                    **identity.as_dict(),
                    **shared_logical_artifact_metadata(first),
                    "source_trace_hash": "1" * 64,
                },
                {
                    **identity.as_dict(),
                    **shared_logical_artifact_metadata(second),
                    "source_trace_hash": "2" * 64,
                },
            ],
            expected_identities=[identity],
        )
