# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Grouping prepared merged roles on local disk yields the in-batch merge, whatever the split."""

from __future__ import annotations

import dataclasses
import datetime
import importlib
import itertools
import json
import multiprocessing
import random
from concurrent.futures import ProcessPoolExecutor

import pytest

from process import provider_directory_bulk_grouping as grouping
from process import provider_directory_bulk_prepare as bulk_prepare
from process import provider_directory_role_merge as role_merge
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)

SOURCE_ID = "synthetic_bulk_source"
CONTRACT = SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
CONTEXT = bulk_prepare.PrepareContext(
    source_id=SOURCE_ID,
    resource_type="PractitionerRole",
    run_id="run_synthetic",
    log_identity="https://bulk.synthetic.test/<redacted>:sha256:0000000000000000",
    dataset_id="dataset_synthetic",
    resource_hash_contract=CONTRACT,
    semantic_projection_as_of="2026-01-01",
    merge_roles=True,
    strict=True,
)
NETWORK_URL = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference"


def raw_role(number: int, practitioner: int, network: int) -> bytes:
    return json.dumps(
        {
            "resourceType": "PractitionerRole",
            "id": f"role-{number}",
            "active": True,
            "practitioner": {"reference": f"Practitioner/p{practitioner}"},
            "organization": {"reference": "Organization/o1"},
            "location": [{"reference": f"Location/l{practitioner % 3}"}],
            "specialty": [{"coding": [{"system": "http://nucc.org/provider-taxonomy", "code": "207Q00000X"}]}],
            "extension": [{"url": NETWORK_URL, "valueReference": {"reference": f"Organization/net{network}"}}],
        }
    ).encode()


# Ten practitioners each seen in six networks: sixty source roles that fold into ten merged roles.
LINES = [raw_role(number, number % 10, number // 10) for number in range(60)]


def prepared(lines: list[bytes], ctx: bulk_prepare.PrepareContext = CONTEXT) -> list[dict]:
    result = bulk_prepare.prepare_lines(lines, ctx)
    assert result.error is None
    return result.rows


def folded_in_one_batch() -> list[dict]:
    return prepared(LINES)


def grouped_rows(tmp_path, chunks: list[list[bytes]], partitions: int = grouping.PARTITIONS) -> list[dict]:
    for output_index, chunk in enumerate(chunks):
        writer = grouping.GroupWriter(tmp_path, output_index, partitions)
        for start in range(0, len(chunk), 7):
            writer.write(prepared(chunk[start : start + 7]))
        writer.finish(len(chunk))
    indexes = list(range(len(chunks)))
    random.Random(partitions).shuffle(indexes)
    reduced_rows = [
        reduced_row
        for partition in range(partitions)
        for reduced_row in grouping.reduce_partition(tmp_path, partition, indexes, CONTRACT, partitions)
    ]
    # Each partition is already sorted; partitions interleave role ids.
    return sorted(reduced_rows, key=lambda reduced_row: reduced_row["resource_id"])


def test_one_output_round_trip_equals_in_batch_merge(tmp_path) -> None:
    expected = folded_in_one_batch()

    assert len(expected) == 10
    assert grouped_rows(tmp_path, [LINES]) == expected


@pytest.mark.parametrize("partitions", [1, 7, grouping.PARTITIONS])
def test_shuffled_outputs_and_partition_counts_give_same_roles(tmp_path, partitions) -> None:
    shuffled_lines = random.Random(3).sample(LINES, len(LINES))
    chunks = [shuffled_lines[:13], shuffled_lines[13:40], shuffled_lines[40:]]

    assert grouped_rows(tmp_path, chunks, partitions) == folded_in_one_batch()


def test_sightings_in_far_apart_frames_merge_into_one_role(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(grouping, "FRAME_BYTES", 1)  # every role its own frame
    chunks = [[line] for line in LINES if json.loads(line)["practitioner"]["reference"] == "Practitioner/p4"]

    (merged,) = grouped_rows(tmp_path, chunks, partitions=3)
    payload_by_field = dict(merged["payload_json"])

    assert payload_by_field["network_refs"] == [f"Organization/net{network}" for network in range(6)]
    assert len(payload_by_field.pop(role_merge.NATIVE_ROLES)) == 6
    # Completion drops the source-role commitments and requires the staged hash to still match.
    expected_hash = resource_payload_sha256_for_contract(payload_by_field, CONTRACT, resource_type="PractitionerRole")
    assert merged["payload_hash"] == expected_hash


def test_output_without_done_marker_is_regrouped(tmp_path) -> None:
    crashed = grouping.GroupWriter(tmp_path, 0, 5)
    crashed.write(prepared(LINES))
    crashed._flush_all()

    assert not grouping.is_grouped(tmp_path, 0)
    with pytest.raises(ValueError, match="not_grouped"):
        list(grouping.reduce_partition(tmp_path, 0, [0], CONTRACT, 5))
    assert grouped_rows(tmp_path, [LINES], 5) == folded_in_one_batch()
    with pytest.raises(ValueError, match="partitions_changed"):
        list(grouping.reduce_partition(tmp_path, 0, [0], CONTRACT, 6))


def test_replaying_a_partition_is_idempotent(tmp_path) -> None:
    first = grouped_rows(tmp_path, [LINES[:30], LINES[30:]], 4)
    grouping.mark_loaded(tmp_path, 2)
    replayed_partitions = [
        grouping.reduce_partition(tmp_path, partition, [1, 0], CONTRACT, 4) for partition in range(4)
    ]

    assert sorted(itertools.chain(*replayed_partitions), key=lambda reduced_row: reduced_row["resource_id"]) == first
    assert all(
        [reduced_row["resource_id"] for reduced_row in partition_rows]
        == sorted(reduced_row["resource_id"] for reduced_row in partition_rows)
        for partition_rows in replayed_partitions
    )
    assert grouping.is_loaded(tmp_path, 2) and not grouping.is_loaded(tmp_path, 1)
    grouping.discard(tmp_path)
    assert not tmp_path.exists()


def test_worker_process_prepares_the_same_rows() -> None:
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=context) as executor:
        worker_result = executor.submit(bulk_prepare.prepare_lines, LINES, CONTEXT).result()

    assert worker_result.rows == folded_in_one_batch() and worker_result.rows_fetched == 60


def test_prepared_rows_equal_todays_staged_rows(monkeypatch) -> None:
    fhir = importlib.import_module("process.provider_directory_fhir")
    acquisition = fhir.FHIRAcquisitionContext(
        fetch_url=CONTEXT.log_identity, fetch_mode="bulk_ndjson", semantic_projection_as_of=datetime.date(2026, 1, 1)
    )
    parsed_rows = [
        fhir.parse_fhir_resource(
            SOURCE_ID, json.loads(line), resource_url=CONTEXT.log_identity, acquisition=acquisition
        )[1]
        for line in LINES
    ]
    retained_rows = fhir._endpoint_dataset_resource_rows(
        fhir.ProviderDirectoryPractitionerRole,
        parsed_rows,
        dataset_id=CONTEXT.dataset_id,
        resource_hash_contract=CONTRACT,
    )
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    staged_rows = role_merge.merged_role_rows(retained_rows, parsed_rows, CONTRACT)
    monkeypatch.delenv(role_merge.MERGED_ROLE_SOURCES_ENV)

    assert prepared(LINES, dataclasses.replace(CONTEXT, run_id=None)) == staged_rows
    assert prepared(LINES, dataclasses.replace(CONTEXT, merge_roles=False)) == retained_rows


def test_one_pass_union_equals_pairwise_fold_in_any_order() -> None:
    observations = [dict(prepared([line])[0]["payload_json"]) for line in LINES[4::10]]
    # One source role seen with two contents: both commitments survive, though they share the 24-digit prefix.
    observations.append(
        {**observations[0], role_merge.NATIVE_ROLES: [observations[0][role_merge.NATIVE_ROLES][0][:24] + "f" * 16]}
    )

    for order in itertools.permutations(observations[:4] + observations[-1:]):
        union = role_merge.RoleObservationUnion(order[0])
        pairwise = order[0]
        for observation in order[1:]:
            union.add(observation)
            pairwise = role_merge.merge_semantic_resource_payloads(pairwise, observation)
        assert union.merged_payload() == pairwise
    assert len(union.merged_payload()[role_merge.NATIVE_ROLES]) == 5
    with pytest.raises(ValueError, match="observation_invalid"):
        union.add({**observations[1], "active": False})


def test_invalid_lines_return_todays_error_codes() -> None:
    assert bulk_prepare.prepare_lines([b"{not json"], CONTEXT).error == "invalid_ndjson"
    assert bulk_prepare.prepare_lines([b"[1]"], CONTEXT).error == "invalid_ndjson"
    foreign_line = b'{"resourceType":"Practitioner","id":"p1"}'
    assert (
        bulk_prepare.prepare_lines([LINES[0], foreign_line], CONTEXT).error
        == "bulk_export_output_resource_type_mismatch"
    )
    lenient = bulk_prepare.prepare_lines([LINES[0], foreign_line, b"[1]"], dataclasses.replace(CONTEXT, strict=False))
    assert lenient.error is None and lenient.rows_fetched == 1


def exact_role(number: int, rank: object) -> bytes:
    """A role whose telecom rank and identifier pass through preparation unchanged."""
    return json.dumps(
        {
            "resourceType": "PractitionerRole",
            "id": f"exact-{number}",
            "practitioner": {"reference": "Practitioner/p1"},
            "identifier": [{"system": "urn:role", "value": f"role-{number}-\udc80"}],
            "telecom": [{"system": "phone", "value": "555-0100", "rank": rank}],
            "extension": [{"url": NETWORK_URL, "valueReference": {"reference": f"Organization/net{number}"}}],
        }
    ).encode()


def test_wide_integers_and_lone_surrogates_survive_grouping(tmp_path, monkeypatch) -> None:
    lines = [exact_role(number, 1234567890123456789012) for number in range(2)]
    fhir = importlib.import_module("process.provider_directory_fhir")
    acquisition = fhir.FHIRAcquisitionContext(
        fetch_url=CONTEXT.log_identity, fetch_mode="bulk_ndjson", semantic_projection_as_of=datetime.date(2026, 1, 1)
    )
    parsed_rows = [
        fhir.parse_fhir_resource(
            SOURCE_ID,
            json.loads(line),
            resource_url=CONTEXT.log_identity,
            acquisition=acquisition,
            run_id="run_synthetic",
        )[1]
        for line in lines
    ]
    retained_rows = fhir._endpoint_dataset_resource_rows(
        fhir.ProviderDirectoryPractitionerRole,
        parsed_rows,
        dataset_id=CONTEXT.dataset_id,
        resource_hash_contract=CONTRACT,
    )
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    (staged_row,) = role_merge.merged_role_rows(retained_rows, parsed_rows, CONTRACT)

    (grouped_row,) = grouped_rows(tmp_path, [lines[:1], lines[1:]], partitions=4)

    assert grouped_row == staged_row
    assert grouped_row["payload_json"]["telecom"][0]["rank"] == 1234567890123456789012
    assert grouped_row["payload_json"]["identifiers"][0]["value"] == "role-0-\udc80"


def test_non_finite_number_fails_grouping(tmp_path) -> None:
    writer = grouping.GroupWriter(tmp_path, 0, 4)
    with pytest.raises(ValueError, match="provider_directory_bulk_group_non_finite_number"):
        writer.write(prepared([exact_role(0, float("nan"))]))
