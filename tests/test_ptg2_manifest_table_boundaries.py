# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_tables
from process.ptg_parts.ptg2_shared_source_set import shared_source_set_metadata


def _strict_source_identity_rows():
    return [
        {
            "source_key": 0,
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "2" * 64,
            "raw_container_sha256": "1" * 64,
            "logical_json_sha256": "2" * 64,
            "logical_hash_deferred": False,
            "source_trace_set_hash": "5" * 64,
        },
        {
            "source_key": 1,
            "source_type": "in_network",
            "identity_kind": "raw_container_sha256_v1",
            "identity_sha256": "3" * 64,
            "raw_container_sha256": "3" * 64,
            "logical_json_sha256": None,
            "logical_hash_deferred": True,
            "source_trace_set_hash": "6" * 64,
        },
    ]


def _strict_source_set():
    return shared_source_set_metadata(
        row["raw_container_sha256"] for row in _strict_source_identity_rows()
    )


def _strict_serving_index():
    return {
        "storage": "manifest_snapshot",
        "type": "ptg2_shared_blocks_v3",
        "snapshot_scoped": True,
        "arch_version": "postgres_binary_v3",
        "shared_snapshot_key": 41,
        "coverage_scope_id": "c" * 64,
        "storage_generation": "shared_blocks_v3",
        "source_count": 2,
        "source_set": _strict_source_set(),
        "code_count": 2,
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "serving_multiplicity_semantics": "source_multiset_v1",
        "price_membership_semantics": "multiset_v1",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": "dense_shared_blocks_v3",
        "provider_scope_strategy": "postgres_shared_graph",
        "id_storage": "binary128",
        "materialized_tables": {},
        "audit_sample": {
            "contract": "persisted_served_occurrence_sample_v2",
            "format_version": 2,
            "method": "publish_time_stratified_v1",
            "sample_count": 2,
            "maximum_rows": 2560,
            "complete_population": False,
            "sample_digest": "a" * 64,
            "source_count": 2,
            "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
            "serving_multiplicity_semantics": "source_multiset_v1",
        },
        "serving_binary": {
            "format": "postgres_binary_v3",
            "price_set_atom_memberships_v3": {"block_span": 512},
            "price_atoms_v3": {"block_span": 512},
            "price_dictionary": {
                "artifact_kind": "by_code_price_dictionary",
                "price_set_count": 29_000_000,
                "block_bytes": 65_536,
            },
        },
        "atom_key_bits": 24,
    }


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda value: value.update(price_membership_semantics="set_v1"),
            "price_membership_semantics",
        ),
        (
            lambda value: value.update(serving_multiplicity_semantics="set_v1"),
            "serving_multiplicity_semantics",
        ),
        (lambda value: value.update(snapshot_scoped=False), "snapshot_scoped"),
        (
            lambda value: value.update(artifacts={"legacy": {}}),
            "sidecar artifacts",
        ),
        (
            lambda value: value["serving_binary"].update(price_atoms_v3=None),
            "strict serving_binary metadata",
        ),
        (
            lambda value: value["serving_binary"][
                "price_set_atom_memberships_v3"
            ].update(block_span=0),
            "sparse price metadata",
        ),
        (
            lambda value: value["serving_binary"]["price_dictionary"].update(
                artifact_kind="legacy"
            ),
            "price dictionary kind",
        ),
    ],
)
def test_strict_v3_contract_rejects_remaining_unsafe_markers(mutator, message):
    serving_index = _strict_serving_index()
    mutator(serving_index)

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match=message):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("table_name", "mrf.table_name"),
        ("other.table_name", "other.table_name"),
        ("bad-schema.table_name", None),
        ("mrf.bad-table", None),
    ],
)
def test_safe_table_name_accepts_only_qualified_identifiers(value, expected):
    assert ptg2_tables._safe_table_name(value) == expected


def test_optional_integer_and_nested_serving_metadata_are_fail_closed():
    assert ptg2_tables._optional_integer(True) is None
    assert ptg2_tables._optional_integer(None) is None
    assert ptg2_tables._optional_integer("24") == 24
    assert ptg2_tables._optional_integer("not-an-integer") is None
    assert ptg2_tables._optional_integer(object()) is None

    assert ptg2_tables._serving_index_atom_key_bits({"atom_key_bits": "32"}) == 32
    assert ptg2_tables._serving_index_atom_key_bits({}) is None
    assert ptg2_tables._serving_index_atom_key_bits({"serving_binary": []}) is None
    assert (
        ptg2_tables._serving_index_atom_key_bits(
            {"serving_binary": {"price_atoms_v3": []}}
        )
        is None
    )
    assert (
        ptg2_tables._serving_index_atom_key_bits(
            {"serving_binary": {"price_atoms_v3": {"atom_key_bits": "24"}}}
        )
        == 24
    )
    assert (
        ptg2_tables._serving_binary_section_integer({}, "section", "count")
        is None
    )
    assert (
        ptg2_tables._serving_binary_section_integer(
            {"serving_binary": {"section": []}},
            "section",
            "count",
        )
        is None
    )


def test_source_set_validators_reject_malformed_and_incomplete_evidence():
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="malformed"):
        ptg2_tables._strict_v3_source_set(
            {"source_set": []},
            source_count=2,
        )
    invalid_source_set = _strict_source_set()
    invalid_source_set["source_count"] = 1
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="source_set is invalid",
    ):
        ptg2_tables._strict_v3_source_set(
            {"source_set": invalid_source_set},
            source_count=2,
        )
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="identity row is malformed",
    ):
        ptg2_tables._validated_published_source_identity("not-a-row")
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="rows are malformed",
    ):
        ptg2_tables._validated_published_source_set(
            "{not-json",
            expected_source_count=2,
        )
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="rows are incomplete",
    ):
        ptg2_tables._validated_published_source_set(
            {},
            expected_source_count=2,
        )

    duplicate_identity_rows = _strict_source_identity_rows()
    duplicate_identity_rows[1]["identity_sha256"] = duplicate_identity_rows[0][
        "raw_container_sha256"
    ]
    duplicate_identity_rows[1]["raw_container_sha256"] = duplicate_identity_rows[
        0
    ]["raw_container_sha256"]
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="source-set identity",
    ):
        ptg2_tables._validated_published_source_set(
            duplicate_identity_rows,
            expected_source_count=2,
        )
