# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from process import uhc_canonical_proof as canonical
from process.uhc_canonical_proof import (
    ProviderDirectoryContentProofBuilder,
    UhcCanonicalMaterializationIdentity,
    UhcCanonicalNpiProof,
    UhcCanonicalProofError,
    bind_uhc_canonical_content_proof,
    canonical_materialization_proof,
    validate_uhc_canonical_content_proof,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _row(resource_type: str, resource_id: str) -> tuple[str, ...]:
    payload = {"resource_id": resource_id}
    return (
        resource_type,
        resource_id,
        hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest(),
        json.dumps(payload),
        "source-rank",
    )


def _lineage(range_ordinal: int = 0) -> dict[str, object]:
    return {
        "source_file_id": "file-a",
        "range_ordinal": range_ordinal,
        "input_sha256": _digest(f"input-{range_ordinal}"),
        "artifact_sha256": _digest("artifact-a"),
    }


def _materialization(
    canonical_resources: list[tuple[str, ...]],
) -> dict[str, object]:
    builder = ProviderDirectoryContentProofBuilder(
        source_id="source-a",
        shard_rows=2,
    )
    builder.observe_rows(canonical_resources, input_lineage=[_lineage()])
    content = builder.complete()
    return canonical_materialization_proof(
        content,
        UhcCanonicalMaterializationIdentity(
            catalog_set_sha256=_digest("catalog"),
            semantic_set_sha256=_digest("semantic"),
            semantic_build_ids=(_digest("build"),),
            source_id="source-a",
            semantic_contract_id="semantic-v2",
            semantic_contract_version=2,
            canonical_contract_id="canonical-v1",
        ),
        UhcCanonicalNpiProof(
            evidence_count=2,
            distinct_npis=1,
            proof_sha256=_digest("npi-proof"),
            shards=(
                {
                    "source_id": "source-a",
                    "source_file_id": "file-a",
                    "range_ordinal": 0,
                    "row_count": 2,
                    "input_sha256": _digest("npi-input"),
                    "artifact_sha256": _digest("artifact-a"),
                    "layout_sha256": _digest("layout"),
                },
            ),
        ),
    )


def test_generic_builder_matches_ordered_dataset_hash_without_payload_reread() -> None:
    rows = [
        _row("Practitioner", "b"),
        _row("Location", "a"),
        _row("Practitioner", "a"),
    ]
    proof = _materialization(rows)
    ordered_identities = [
        json.dumps(row[:3], sort_keys=True, separators=(",", ":"))
        for row in sorted(rows, key=lambda row: row[:2])
    ]

    assert proof["dataset_hash"] == hashlib.sha256(
        "\n".join(ordered_identities).encode()
    ).hexdigest()
    assert proof["resource_counts"] == {"Location": 1, "Practitioner": 2}
    assert proof["shard_count"] == 2
    assert proof["shards"][0]["input_lineage"] == [_lineage()]


def test_bound_proof_binds_every_content_and_npi_shard() -> None:
    proof = bind_uhc_canonical_content_proof(
        _materialization([_row("Practitioner", "a"), _row("Location", "a")]),
        dataset_id="dataset-a",
        endpoint_id="endpoint-a",
        acquisition_root_run_id="root-a",
    )

    for shard in [*proof["shards"], *proof["npi_evidence"]["shards"]]:
        assert shard["dataset_id"] == "dataset-a"
        assert shard["endpoint_id"] == "endpoint-a"
        assert shard["acquisition_root_run_id"] == "root-a"
    assert validate_uhc_canonical_content_proof(
        proof,
        dataset_id="dataset-a",
        endpoint_id="endpoint-a",
        acquisition_root_run_id="root-a",
    ) == proof


def test_duplicate_identity_and_bound_shard_tampering_fail_closed() -> None:
    duplicate = _row("Practitioner", "a")
    builder = ProviderDirectoryContentProofBuilder(
        source_id="source-a",
        shard_rows=1,
    )
    builder.observe_rows([duplicate, duplicate], input_lineage=[_lineage()])
    with pytest.raises(UhcCanonicalProofError, match="duplicated"):
        builder.complete()

    proof = bind_uhc_canonical_content_proof(
        _materialization([duplicate]),
        dataset_id="dataset-a",
        endpoint_id="endpoint-a",
        acquisition_root_run_id="root-a",
    )
    proof["shards"][0]["acquisition_root_run_id"] = "root-b"
    with pytest.raises(UhcCanonicalProofError, match="hash changed"):
        validate_uhc_canonical_content_proof(
            proof,
            dataset_id="dataset-a",
            endpoint_id="endpoint-a",
            acquisition_root_run_id="root-a",
        )


def _resign_materialization(proof: dict[str, object]) -> None:
    proof.pop("materialization_sha256", None)
    proof["materialization_sha256"] = canonical._json_sha256(proof)


def _resign_bound_proof(proof: dict[str, object]) -> None:
    proof.pop("proof_sha256", None)
    proof["proof_sha256"] = canonical._json_sha256(proof)


@pytest.mark.parametrize(
    "row",
    [
        (),
        (None, "resource", _digest("payload")),
        ("", "resource", _digest("payload")),
        ("Practitioner", None, _digest("payload")),
        ("Practitioner", "", _digest("payload")),
        ("Practitioner", "resource", None),
        ("Practitioner", "resource", "not-a-hash"),
    ],
)
def test_canonical_identity_rejects_every_incomplete_shape(row):
    with pytest.raises(UhcCanonicalProofError):
        canonical._identity_bytes(row)


@pytest.mark.parametrize(
    ("source_id", "shard_rows"),
    [("source", 0), ("source", -1), ("", 1)],
)
def test_builder_rejects_invalid_configuration(source_id, shard_rows):
    with pytest.raises(ValueError):
        ProviderDirectoryContentProofBuilder(
            source_id=source_id,
            shard_rows=shard_rows,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_file_id", None),
        ("source_file_id", ""),
        ("range_ordinal", True),
        ("range_ordinal", "0"),
        ("range_ordinal", -1),
        ("input_sha256", "bad"),
        ("artifact_sha256", "bad"),
    ],
)
def test_lineage_descriptor_rejects_every_invalid_field(field, value):
    lineage = _lineage()
    lineage[field] = value
    with pytest.raises(UhcCanonicalProofError):
        ProviderDirectoryContentProofBuilder._lineage_descriptor(lineage)


def test_builder_rejects_empty_lineage_and_operations_after_seal():
    builder = ProviderDirectoryContentProofBuilder(
        source_id="source-a",
        shard_rows=2,
    )
    builder._flush_shard()
    with pytest.raises(UhcCanonicalProofError, match="no input lineage"):
        builder.observe_rows([_row("Practitioner", "a")], input_lineage=[])
    builder.close()
    builder.close()
    with pytest.raises(UhcCanonicalProofError, match="is sealed"):
        builder.observe_rows(
            [_row("Practitioner", "a")],
            input_lineage=[_lineage()],
        )
    with pytest.raises(UhcCanonicalProofError, match="is sealed"):
        builder.complete()


def test_builder_rejects_unframed_and_malformed_identity(tmp_path):
    unframed = tmp_path / "unframed"
    unframed.write_bytes(b"not-framed")
    with pytest.raises(UhcCanonicalProofError, match="framing"):
        tuple(ProviderDirectoryContentProofBuilder._lines(unframed))

    for identity in (
        b"\xff",
        b"not-json",
        b"{}",
        b'["Practitioner","resource"]',
        b'["","resource","' + _digest("payload").encode() + b'"]',
        b'["Practitioner","resource","bad"]',
    ):
        with pytest.raises(UhcCanonicalProofError):
            ProviderDirectoryContentProofBuilder._decoded_identity(identity)


def test_builder_performs_bounded_multi_shard_merge():
    builder = ProviderDirectoryContentProofBuilder(
        source_id="source-a",
        shard_rows=1,
    )
    for ordinal in range(canonical._MERGE_FAN_IN + 1):
        builder.observe_rows(
            [_row("Practitioner", f"resource-{ordinal:03d}")],
            input_lineage=[_lineage(ordinal)],
        )
    content = builder.complete()
    assert content.resource_count == canonical._MERGE_FAN_IN + 1
    assert not Path(builder._directory).exists()


def _is_resource_proof_mutated(proof, shard, mutation):
    match mutation:
        case "contract_id":
            proof["contract_id"] = "wrong"
        case "complete":
            proof["complete"] = False
        case "resource_count_bool":
            proof["resource_count"] = True
        case "dataset_hash":
            proof["dataset_hash"] = "bad"
        case "resource_counts_type":
            proof["resource_counts"] = []
        case "resource_hashes_type":
            proof["resource_hashes"] = []
        case "resource_keys":
            proof["resource_hashes"] = {}
        case "resource_total":
            proof["resource_counts"]["Practitioner"] = 2
        case "resource_hash":
            proof["resource_hashes"]["Practitioner"] = "bad"
        case "resource_hash_type":
            proof["resource_hashes"]["Practitioner"] = 7
        case "shards_type":
            proof["shards"] = {}
        case "shard_count":
            proof["shard_count"] = 2
        case "shard_mapping":
            proof["shards"] = ["bad"]
        case "shard_rows":
            shard["resource_count"] = 2
        case "shard_ordinal":
            shard["ordinal"] = 4
        case "shard_source":
            shard["source_id"] = "wrong"
        case "shard_counts_type":
            shard["resource_counts"] = []
        case "shard_counts_total":
            shard["resource_counts"]["Practitioner"] = 2
        case "shard_lineage":
            shard["input_lineage"] = []
        case "shard_hash":
            shard["content_sha256"] = "bad"
        case "shard_set_hash":
            proof["shard_set_sha256"] = "0" * 64
        case _:
            return False
    return True


def _is_npi_proof_mutated(proof, npi, npi_shard, mutation):
    match mutation:
        case "npi_type":
            proof["npi_evidence"] = []
        case "npi_evidence_count":
            npi["evidence_count"] = -1
        case "npi_distinct_count":
            npi["distinct_npis"] = True
        case "npi_proof_hash":
            npi["proof_sha256"] = "bad"
        case "npi_shards_type":
            npi["shards"] = {}
        case "npi_shard_count":
            npi["shard_count"] = 2
        case "npi_rows":
            npi_shard["row_count"] = 1
        case "npi_shard_mapping":
            npi["shards"] = ["bad"]
        case "npi_source":
            npi_shard["source_id"] = "wrong"
        case "npi_file_type":
            npi_shard["source_file_id"] = 7
        case "npi_file_empty":
            npi_shard["source_file_id"] = ""
        case "npi_range_bool":
            npi_shard["range_ordinal"] = True
        case "npi_range_type":
            npi_shard["range_ordinal"] = "0"
        case "npi_range_negative":
            npi_shard["range_ordinal"] = -1
        case "npi_input_hash":
            npi_shard["input_sha256"] = "bad"
        case "npi_artifact_hash":
            npi_shard["artifact_sha256"] = "bad"
        case "npi_layout_hash":
            npi_shard["layout_sha256"] = "bad"
        case "npi_shard_set_hash":
            npi["shard_set_sha256"] = "0" * 64
        case _:
            return False
    return True


def _mutate_materialization(
    mutation: str,
) -> dict[str, object]:
    proof = copy.deepcopy(_materialization([_row("Practitioner", "a")]))
    if mutation == "materialization_hash":
        proof["materialization_sha256"] = "0" * 64
        return proof
    shard = proof["shards"][0]
    npi = proof["npi_evidence"]
    is_mutated = _is_resource_proof_mutated(proof, shard, mutation)
    is_mutated = is_mutated or _is_npi_proof_mutated(
        proof,
        npi,
        npi["shards"][0],
        mutation,
    )
    if not is_mutated:
        raise AssertionError(mutation)
    _resign_materialization(proof)
    return proof


@pytest.mark.parametrize(
    "mutation",
    [
        "materialization_hash",
        "contract_id",
        "complete",
        "resource_count_bool",
        "dataset_hash",
        "resource_counts_type",
        "resource_hashes_type",
        "resource_keys",
        "resource_total",
        "resource_hash",
        "resource_hash_type",
        "shards_type",
        "shard_count",
        "shard_mapping",
        "shard_rows",
        "shard_ordinal",
        "shard_source",
        "shard_counts_type",
        "shard_counts_total",
        "shard_lineage",
        "shard_hash",
        "shard_set_hash",
        "npi_type",
        "npi_evidence_count",
        "npi_distinct_count",
        "npi_proof_hash",
        "npi_shards_type",
        "npi_shard_count",
        "npi_rows",
        "npi_shard_mapping",
        "npi_source",
        "npi_file_type",
        "npi_file_empty",
        "npi_range_bool",
        "npi_range_type",
        "npi_range_negative",
        "npi_input_hash",
        "npi_artifact_hash",
        "npi_layout_hash",
        "npi_shard_set_hash",
    ],
)
def test_materialization_validation_rejects_every_contract_mutation(mutation):
    with pytest.raises(UhcCanonicalProofError):
        canonical._validate_materialization_proof(
            _mutate_materialization(mutation)
        )


def test_bound_proof_rejects_missing_hash_binding_and_nested_binding_drift():
    with pytest.raises(UhcCanonicalProofError, match="is missing"):
        validate_uhc_canonical_content_proof(
            None,
            dataset_id="dataset-a",
            endpoint_id="endpoint-a",
            acquisition_root_run_id="root-a",
        )
    with pytest.raises(UhcCanonicalProofError, match="bound proof hash"):
        validate_uhc_canonical_content_proof(
            {},
            dataset_id="dataset-a",
            endpoint_id="endpoint-a",
            acquisition_root_run_id="root-a",
        )

    original = bind_uhc_canonical_content_proof(
        _materialization([_row("Practitioner", "a")]),
        dataset_id="dataset-a",
        endpoint_id="endpoint-a",
        acquisition_root_run_id="root-a",
    )
    binding_drift = copy.deepcopy(original)
    binding_drift["dataset_id"] = "dataset-b"
    _resign_bound_proof(binding_drift)
    with pytest.raises(UhcCanonicalProofError, match="proof binding changed"):
        validate_uhc_canonical_content_proof(
            binding_drift,
            dataset_id="dataset-a",
            endpoint_id="endpoint-a",
            acquisition_root_run_id="root-a",
        )

    for shard_path, match in (
        (("shards",), "canonical shard binding changed"),
        (("npi_evidence", "shards"), "NPI shard binding changed"),
    ):
        proof = copy.deepcopy(original)
        nested = proof
        for path_component in shard_path:
            nested = nested[path_component]
        nested[0]["endpoint_id"] = "endpoint-b"
        _resign_bound_proof(proof)
        with pytest.raises(UhcCanonicalProofError, match=match):
            validate_uhc_canonical_content_proof(
                proof,
                dataset_id="dataset-a",
                endpoint_id="endpoint-a",
                acquisition_root_run_id="root-a",
            )
