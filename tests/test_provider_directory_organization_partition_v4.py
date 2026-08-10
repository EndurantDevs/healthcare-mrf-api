# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Occurrence-proof boundaries for partitioned semantic-v4 Organizations."""

from __future__ import annotations

import copy
import importlib

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _context() -> importer.PaginationCheckpointContext:
    """Return one lineage-verified partition checkpoint identity."""

    return importer.PaginationCheckpointContext(
        canonical_api_base="https://directory.example.test/fhir",
        source_scope_hash="scope-organization-partition",
        source_ids=("source-organization-partition",),
        owner_run_id="run-organization-partition",
        acquisition_root_run_id="run-organization-partition",
        endpoint_id="endpoint-organization-partition",
        dataset_id="dataset-organization-partition",
        lineage_verified=True,
    )


def _source() -> dict[str, object]:
    """Bind raw observations to the exact v4 root contract."""

    return {
        "source_id": "source-organization-partition",
        "_resource_hash_contract": (
            importer.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        ),
        "_semantic_projection_as_of": "2026-08-10",
    }


def _organization(
    name: str,
    *,
    last_updated: str,
    aliases: list[str] | None = None,
) -> dict[str, object]:
    """Return one raw FHIR Organization observation."""

    return {
        "resourceType": "Organization",
        "id": "organization-a",
        "meta": {
            "versionId": "1",
            "source": "https://directory.example.test/fhir",
            "lastUpdated": last_updated,
        },
        "active": True,
        "identifier": [
            {"system": "urn:example", "value": "organization-a"}
        ],
        "name": name,
        "alias": aliases or [],
    }


def _proof_rows(
    stage: importer.LastUpdatedPartitionStage,
    window_id: str,
) -> list[dict[str, object]]:
    """Project staged occurrence records into durable proof-row shape."""

    return [
        {
            "resource_id": occurrence_id,
            "payload_hash": importer._last_updated_partition_window_proof_hash(
                "Organization",
                window_id,
                2,
            ),
            "payload_json": {
                "window_id": window_id,
                "fingerprint": importer._identity_hash(
                    {"occurrence_id": occurrence_id}
                ),
                "source_fingerprint": (
                    stage.occurrence_source_fingerprints_by_id[occurrence_id]
                ),
                "candidate_resource_id": proof_record[1],
                "candidate_payload_hash": stage.candidate_hashes_by_id[
                    occurrence_id
                ],
                "candidate_proof_record": proof_record,
            },
        }
        for occurrence_id, proof_record in sorted(
            stage.candidate_proof_records_by_id.items()
        )
    ]


def _pass1_rows(
    proof_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Project pass-two rows to the exact pass-one identity fields."""

    return [
        {
            "resource_id": proof_row["resource_id"],
            "payload_hash": importer._last_updated_partition_window_proof_hash(
                "Organization",
                proof_row["payload_json"]["window_id"],
                1,
            ),
            "payload_json": {
                field_name: proof_row["payload_json"][field_name]
                for field_name in (
                    "window_id",
                    "fingerprint",
                    "source_fingerprint",
                )
            },
        }
        for proof_row in proof_rows
    ]


def _bindings(
    resources: tuple[dict[str, object], ...],
    window_id: str,
):
    """Return deterministic v4 partition bindings for test observations."""

    return importer._v4_partition_resource_bindings(
        resources,
        window_id,
    )


async def _stage(
    observations: tuple[dict[str, object], ...],
    window_id: str,
) -> importer.LastUpdatedPartitionStage:
    """Stage raw observations with their exact occurrence commitments."""

    (
        staged_resources,
        _proof_resources,
        occurrence_ids,
        source_fingerprints_by_id,
    ) = _bindings(observations, window_id)
    return await importer._stage_last_updated_partition_window(
        _context(),
        _source(),
        "Organization",
        importer.ProviderDirectoryOrganization,
        staged_resources,
        importer.LastUpdatedPartitionStageOptions(
            run_id="run-organization-partition",
            fetch_url=(
                "https://directory.example.test/fhir/Organization"
            ),
            occurrence_ids=occurrence_ids,
            source_fingerprints_by_id=source_fingerprints_by_id,
        ),
    )


def test_v4_occurrence_ids_are_order_stable() -> None:
    """Bind multiplicity and semantic variants without source-order drift."""

    first = _organization(
        "Community Health Center",
        last_updated="2024-01-01T01:00:00Z",
    )
    second = _organization(
        "COMMUNITY HEALTH SERVICES",
        last_updated="2024-01-01T02:00:00Z",
    )
    forward = _bindings((first, second, first), "window-a")
    reverse = _bindings((first, second, first)[::-1], "window-a")

    assert forward[1:] == reverse[1:]
    assert len(forward[2]) == len(set(forward[2])) == 3
    assert _bindings((first,), "window-a")[2] != _bindings(
        (first,), "window-b"
    )[2]


@pytest.mark.asyncio
async def test_v4_partition_stage_emits_occurrence_proofs() -> None:
    """Collapse retained identity while committing every raw observation."""

    observations = (
        _organization(
            "Community Health Center",
            aliases=["Regional Clinic"],
            last_updated="2024-01-01T01:00:00Z",
        ),
        _organization(
            "COMMUNITY HEALTH SERVICES",
            last_updated="2024-01-01T02:00:00Z",
        ),
    )
    stage = await _stage(observations, "window-a")

    assert len(stage.rows) == 1
    occurrence_ids = set(stage.occurrence_source_fingerprints_by_id)
    assert set(stage.candidate_hashes_by_id) == occurrence_ids
    assert set(stage.candidate_proof_records_by_id) == occurrence_ids
    assert stage.rows[0]["payload_json"]["name_variants"] == [
        "Community Health Center",
        "COMMUNITY HEALTH SERVICES",
    ]
    proof_rows = _proof_rows(stage, "window-a")
    counts = importer._v4_partition_proof_counts(
        list(stage.rows),
        _pass1_rows(proof_rows),
        proof_rows,
        {"window-a": 2},
    )
    assert counts == importer.LastUpdatedPartitionProofCounts(
        leaf_count_sum=0,
        pass1_unique=2,
        pass2_unique=2,
        staged_candidate_count=1,
        invalid_candidate_count=0,
        orphan_proof_count=0,
        candidate_hashes_by_id={
            stage.rows[0]["resource_id"]: stage.rows[0]["payload_hash"]
        },
    )


@pytest.mark.asyncio
async def test_v4_partition_reducer_composes_windows() -> None:
    """Reduce cross-window observations to the exact retained union hash."""

    first = _organization(
        "Community Health Center",
        last_updated="2024-01-01T01:00:00Z",
    )
    second = _organization(
        "COMMUNITY HEALTH SERVICES",
        last_updated="2024-01-02T01:00:00Z",
    )
    combined_stage = await _stage((first, second), "window-combined")
    proof_rows: list[dict[str, object]] = []
    for window_id, observation in (
        ("window-a", first),
        ("window-b", second),
    ):
        window_stage = await _stage((observation,), window_id)
        proof_rows.extend(_proof_rows(window_stage, window_id))

    counts = importer._v4_partition_proof_counts(
        list(combined_stage.rows),
        _pass1_rows(proof_rows),
        proof_rows,
        {"window-a": 1, "window-b": 1},
    )
    assert counts.invalid_candidate_count == 0
    assert counts.orphan_proof_count == 0
    assert counts.staged_candidate_count == 1
    assert counts.pass2_unique == 2

    missing_counts = importer._v4_partition_proof_counts(
        list(combined_stage.rows),
        _pass1_rows(proof_rows),
        proof_rows[:1],
        {"window-a": 1, "window-b": 1},
    )
    assert missing_counts.invalid_candidate_count > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field_name",
    ("resource_id", "window_id", "fingerprint", "source_fingerprint"),
)
async def test_v4_partition_rejects_identity_tamper(field_name) -> None:
    """Reject each independently altered durable occurrence coordinate."""

    stage = await _stage(
        (
            _organization(
                "Community Health Center",
                last_updated="2024-01-01T01:00:00Z",
            ),
        ),
        "window-a",
    )
    proof_rows = _proof_rows(stage, "window-a")
    tampered_rows = copy.deepcopy(proof_rows)
    if field_name == "resource_id":
        tampered_rows[0][field_name] = "f" * 64
    else:
        tampered_rows[0]["payload_json"][field_name] = "f" * 64

    counts = importer._v4_partition_proof_counts(
        list(stage.rows),
        _pass1_rows(proof_rows),
        tampered_rows,
        {"window-a": 1},
    )
    assert counts.invalid_candidate_count > 0


@pytest.mark.asyncio
async def test_v4_partition_rejects_ordinal_gap() -> None:
    """Reject matching pass rows whose duplicate ordinals are not contiguous."""

    observation = _organization(
        "Community Health Center",
        last_updated="2024-01-01T01:00:00Z",
    )
    stage = await _stage((observation, observation), "window-a")
    proof_rows = _proof_rows(stage, "window-a")
    pass1_rows = _pass1_rows(proof_rows)
    source_fingerprint = proof_rows[0]["payload_json"]["source_fingerprint"]
    invalid_occurrence_id = importer._partition_occurrence_id(
        "window-a",
        "organization-a",
        source_fingerprint,
        2,
    )
    proof_rows[1]["resource_id"] = invalid_occurrence_id
    pass1_rows[1]["resource_id"] = invalid_occurrence_id

    counts = importer._v4_partition_proof_counts(
        list(stage.rows),
        pass1_rows,
        proof_rows,
        {"window-a": 2},
    )
    assert counts.invalid_candidate_count > 0


@pytest.mark.asyncio
async def test_v4_partition_binds_planner_window() -> None:
    """Reject jointly rewritten passes that leave the planner's leaf scope."""

    stage = await _stage(
        (
            _organization(
                "Community Health Center",
                last_updated="2024-01-01T01:00:00Z",
            ),
        ),
        "window-a",
    )
    proof_rows = _proof_rows(stage, "window-a")
    pass1_rows = _pass1_rows(proof_rows)
    source_fingerprint = proof_rows[0]["payload_json"]["source_fingerprint"]
    rewritten_id = importer._partition_occurrence_id(
        "window-rewritten",
        "organization-a",
        source_fingerprint,
        0,
    )
    for pass_number, pass_rows in ((1, pass1_rows), (2, proof_rows)):
        pass_rows[0]["resource_id"] = rewritten_id
        pass_rows[0]["payload_json"]["window_id"] = "window-rewritten"
        pass_rows[0]["payload_hash"] = (
            importer._last_updated_partition_window_proof_hash(
                "Organization",
                "window-rewritten",
                pass_number,
            )
        )

    counts = importer._v4_partition_proof_counts(
        list(stage.rows),
        pass1_rows,
        proof_rows,
        {"window-a": 1},
    )
    assert counts.invalid_candidate_count > 0


@pytest.mark.asyncio
async def test_v4_partition_rejects_malformed_digests() -> None:
    """Require both planner and source fingerprints to be exact SHA-256."""

    stage = await _stage(
        (
            _organization(
                "Community Health Center",
                last_updated="2024-01-01T01:00:00Z",
            ),
        ),
        "window-a",
    )
    proof_rows = _proof_rows(stage, "window-a")
    pass1_rows = _pass1_rows(proof_rows)
    malformed_source = "not-a-source-digest"
    rewritten_id = importer._partition_occurrence_id(
        "window-a",
        "organization-a",
        malformed_source,
        0,
    )
    for pass_rows in (pass1_rows, proof_rows):
        pass_rows[0]["resource_id"] = rewritten_id
        pass_rows[0]["payload_json"]["fingerprint"] = "not-a-planner-digest"
        pass_rows[0]["payload_json"]["source_fingerprint"] = malformed_source

    counts = importer._v4_partition_proof_counts(
        list(stage.rows),
        pass1_rows,
        proof_rows,
        {"window-a": 1},
    )
    assert counts.invalid_candidate_count > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper_kind", ("payload", "acquired_hash"))
async def test_v4_partition_rehashes_candidate(tamper_kind) -> None:
    """Reject retained payload or subset-marker drift before streaming."""

    stage = await _stage(
        (
            _organization(
                "Community Health Center",
                last_updated="2024-01-01T01:00:00Z",
            ),
        ),
        "window-a",
    )
    candidate_rows = copy.deepcopy(list(stage.rows))
    if tamper_kind == "payload":
        candidate_rows[0]["payload_json"]["active"] = False
    else:
        candidate_rows[0]["acquired_resource_sha256"] = "f" * 64
    proof_rows = _proof_rows(stage, "window-a")

    counts = importer._v4_partition_proof_counts(
        candidate_rows,
        _pass1_rows(proof_rows),
        proof_rows,
        {"window-a": 1},
    )
    assert counts.invalid_candidate_count > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("field_index", (4, 10))
async def test_v4_partition_binds_proof_summary(field_index) -> None:
    """Bind sealed metrics and union diagnostics to retained payloads."""

    stage = await _stage(
        (
            _organization(
                "Community Health Center",
                last_updated="2024-01-01T01:00:00Z",
            ),
            _organization(
                "COMMUNITY HEALTH SERVICES",
                last_updated="2024-01-01T02:00:00Z",
            ),
        ),
        "window-a",
    )
    proof_rows = _proof_rows(stage, "window-a")
    for proof_row in proof_rows:
        proof_record = proof_row["payload_json"]["candidate_proof_record"]
        proof_record[field_index] = 999 if field_index == 4 else []

    counts = importer._v4_partition_proof_counts(
        list(stage.rows),
        _pass1_rows(proof_rows),
        proof_rows,
        {"window-a": 2},
    )
    assert counts.invalid_candidate_count > 0


def test_v4_partition_accepts_empty_leaf() -> None:
    """Treat a zero-count planner leaf as an exact empty proof."""

    counts = importer._v4_partition_proof_counts(
        [],
        [],
        [],
        {"root": 0},
    )
    assert counts.invalid_candidate_count == 0
    assert counts.staged_candidate_count == 0
    assert counts.pass1_unique == counts.pass2_unique == 0
