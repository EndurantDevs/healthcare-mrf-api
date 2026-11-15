# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure lossless observation encoding and physical SQL boundaries."""

import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_candidate as candidate


def resource(resource_id="native", **fields):
    return {
        "dataset_id": "dataset",
        "resource_type": "PractitionerRole",
        "resource_id": resource_id,
        "payload_hash": "a" * 64,
        "acquired_resource_sha256": "b" * 64,
        "payload_json": {
            "resource_id": resource_id,
            "network_refs": ["Organization/network"],
            "location_refs": ["Location/office"],
            **fields,
        },
    }


def prepared(rows, **kwargs):
    return compact.prepare_compact_payload_batch(
        rows, dataset_id="dataset", batch_ordinal=1, format_id=compact.FORMAT, **kwargs
    )


@pytest.mark.parametrize(
    "references",
    [
        [],
        None,
        ["HealthcareService/" + "a" * 32] * 2,
        ["HealthcareService/" + "A" * 32, "", "Other/x", "caf\u00e9"],
        [1, None],
        "inline",
    ],
)
def test_reference_shapes_and_native_hashes_are_unchanged(references):
    original = resource(healthcare_service_refs=references)
    saved = copy.deepcopy(original)
    aliases, blocks, sets = prepared([original])
    assert original == saved
    assert aliases[0][4:7] == ("native", "a" * 64, "b" * 64)
    assert aliases[0][10] == (
        1
        if references is None
        else 2
        if isinstance(references, list) and all(type(entry) is str for entry in references)
        else 0
    )
    if aliases[0][10] == 2:
        assert list(sets.values()) == [references]
    elif references is not None:
        assert json.loads(blocks[0][3])[0]["healthcare_service_refs"] == references


def test_observations_keep_duplicates_conflicts_and_original_ordinals():
    observations = [resource("same", active=True), resource("same", active=False), resource("same", active=True)]
    aliases, blocks, _sets = prepared(observations)
    assert [alias[1:3] for alias in aliases] == [(1, 0), (1, 1), (1, 2)]
    assert [alias[4] for alias in aliases] == ["same"] * 3
    assert [body["active"] for body in json.loads(blocks[0][3])] == [True, False, True]


def test_optional_acquisition_hash_defaults_without_mutating_input():
    row_by_field = resource()
    row_by_field.pop("acquired_resource_sha256")
    saved = copy.deepcopy(row_by_field)
    assert prepared([row_by_field]) == prepared([{**row_by_field, "acquired_resource_sha256": None}])
    assert row_by_field == saved
    for missing in ("resource_id", "payload_hash", "payload_json"):
        with pytest.raises(candidate.GenericCandidateError, match="columns"):
            prepared([{key: value for key, value in row_by_field.items() if key != missing}])


def test_body_blocks_split_by_rows_and_shared_copy_bytes(monkeypatch):
    _aliases, blocks, _sets = prepared([resource(str(number)) for number in range(130)])
    assert [len(json.loads(block[3])) for block in blocks] == [64, 64, 2]
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 2000)
    _aliases, blocks, _sets = prepared([resource(str(number), text="x" * 400) for number in range(9)])
    assert len(blocks) > 1
    assert sum(len(json.loads(block[3])) for block in blocks) == 9
    for block in blocks:
        assert candidate.copy_record_bytes((0, *block)) + candidate.COPY_FRAME_BYTES <= candidate.COPY_MAX_BYTES
    with pytest.raises(candidate.GenericCandidateError, match="copy_record_too_large"):
        prepared([resource(text="x" * 2000)])


def test_missing_or_distinct_payload_identity_stays_exact():
    for payload in ({}, {"resource_id": None}, {"resource_id": "different"}):
        aliases, blocks, _sets = prepared([{**resource(), "payload_json": payload}])
        assert not aliases[0][11]
        assert json.loads(blocks[0][3]) == [payload]


def test_decimal_expansion_cannot_exceed_logical_read_bound(monkeypatch):
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 2000)
    with pytest.raises(candidate.GenericCandidateError, match="logical_row_too_large"):
        prepared([resource(numbers=[1e308] * 20)])


def test_reference_hash_collision_is_compared_not_merged(monkeypatch):
    monkeypatch.setattr(compact, "_reference_digest", lambda _refs: b"x" * 32)
    with pytest.raises(candidate.GenericCandidateError, match="reference_hash_collision"):
        prepared([resource(healthcare_service_refs=["one"]), resource(healthcare_service_refs=["two"])])


def test_explicit_format_columns_and_hashes_refuse_before_io():
    for damaged in (
        {**resource(), "payload_hash": "bad"},
        {**resource(), "dataset_id": "other"},
        {**resource(), "extra": True},
    ):
        with pytest.raises(candidate.GenericCandidateError):
            prepared([damaged])
    with pytest.raises(candidate.GenericCandidateError, match="format_or_ordinal"):
        compact.prepare_compact_payload_batch([resource()], dataset_id="dataset", batch_ordinal=1, format_id="unknown")


def test_projection_has_only_logical_names_and_no_stored_identity():
    tables = compact.CompactPayloadTables("edition", "aliases", "sets", "blocks")
    statement = compact.compact_resource_sql(tables)
    assert all(f'"edition"."{name}"' in statement for name in ("aliases", "sets", "blocks"))
    assert "a.batch_ordinal,a.row_ordinal" not in statement
    assert "a.batch_ordinal,a.row_ordinal" in compact.compact_resource_sql(tables, observations=True)
    assert len(compact.compact_payload_ddl(tables)) == 5
    with pytest.raises(ValueError):
        compact.CompactPayloadTables("edition;bad", "aliases", "sets", "blocks")


@pytest.mark.asyncio
async def test_page_selects_physical_npi_and_byte_bounds_before_payload(monkeypatch):
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 256)
    keys = [{"batch_ordinal": 1, "row_ordinal": ordinal, "read_bytes": size} for ordinal, size in enumerate((100, 200))]
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetch=AsyncMock(side_effect=[keys, [{}]]))
    tables = compact.CompactPayloadTables("edition", "aliases", "sets", "blocks")
    assert await compact.read_compact_payload_page(connection, tables, "dataset", npi="1000000000") == [{}]
    first, second = connection.fetch.await_args_list
    assert "npi_text=$7" in first.args[0] and "payload_json" not in first.args[0]
    assert "unnest" in second.args[0] and second.args[1:] == ("dataset", [1], [0])


@pytest.mark.asyncio
async def test_oversized_first_read_is_not_successful_end_of_data(monkeypatch):
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 256)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=[{"batch_ordinal": 1, "row_ordinal": 0, "read_bytes": 256}]),
    )
    tables = compact.CompactPayloadTables("edition", "aliases", "sets", "blocks")
    with pytest.raises(candidate.GenericCandidateError, match="read_row_too_large"):
        await compact.read_compact_payload_page(connection, tables, "dataset")
    connection.fetch.assert_awaited_once()
