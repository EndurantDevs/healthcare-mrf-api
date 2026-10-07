# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Scale regressions for bounded custom-import candidate orchestration."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from sqlalchemy.dialects import postgresql

import process.custom_import.runner_codec as runner_codec
from process.custom_import.definition import MAX_DEFINITION_BYTES, CustomImportDefinition
from process.custom_import.family import RootFamily, assemble_root_families
from process.custom_import.runner_codec import (
    candidate_hash,
    candidate_hash_ordered,
    new_family_hash,
    new_family_hash_ordered,
)
from process.custom_import.runner_graph import previous_children_statement
from process.custom_import.runner_types import CandidateRunnerError, CandidateRunRequest, CurrentGenerationPointer

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"
_CANDIDATE_ROOT_COUNT = 9_995
_FAMILY_CHILD_COUNT = 3_332
_ASYNC_PG_PARAMETER_LIMIT = 32_767


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json(_FIXTURE.read_text())


def _request() -> CandidateRunRequest:
    return CandidateRunRequest(
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        execution_id=14,
        lease_token="synthetic-runner-token",
        definition=_definition(),
        roots=(),
        children_by_collection={"rates": ()},
    )


def _reject_aggregate_canonical_json(monkeypatch) -> None:
    original = runner_codec.canonical_json

    def reject_aggregate(document):
        if "root_keys" in document or "children" in document:
            raise AssertionError("candidate aggregate must be hashed incrementally")
        return original(document)

    monkeypatch.setattr(runner_codec, "canonical_json", reject_aggregate)


def test_candidate_hash_streams_9995_root_keys_in_order_independent_form(monkeypatch):
    _reject_aggregate_canonical_json(monkeypatch)
    root_key_hashes = tuple(hashlib.sha256(ordinal.to_bytes(8)).digest() for ordinal in range(_CANDIDATE_ROOT_COUNT))

    forward = candidate_hash(
        execution_id=14,
        fence=3,
        base_generation_id=8,
        root_key_hashes=root_key_hashes,
    )
    reverse = candidate_hash(
        execution_id=14,
        fence=3,
        base_generation_id=8,
        root_key_hashes=tuple(reversed(root_key_hashes)),
    )

    assert forward == reverse


def test_family_hash_streams_3332_children_in_order_independent_form(monkeypatch):
    definition = _definition()
    _reject_aggregate_canonical_json(monkeypatch)
    child_records = tuple(
        {
            "rate_npi": "1234567893",
            "service_code": f"S{ordinal:04d}",
            "amount": Decimal("12.50"),
        }
        for ordinal in range(_FAMILY_CHILD_COUNT)
    )
    family = RootFamily(
        root_key=("1234567893",),
        root={"npi": "1234567893", "display_name": "Synthetic Clinic"},
        children={"rates": child_records},
    )
    reordered = RootFamily(
        root_key=family.root_key,
        root=family.root,
        children={"rates": tuple(reversed(child_records))},
    )

    assert new_family_hash(definition, family) == new_family_hash(definition, reordered)


def _reference_hash(domain, document):
    serialized = json.dumps(document, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return hashlib.sha256(
        b"custom-import/v1\x00candidate-runner/1\x00" + domain.encode("ascii") + b"\x00" + serialized.encode("utf-8")
    ).digest()


def _child_document(definition, child):
    return (
        runner_codec.child_key_hash(definition, "rates", child),
        runner_codec.child_key_document(definition, "rates", child),
        runner_codec.record_payload(runner_codec.fields_by_collection(definition)["rates"], child),
    )


def _family(child_records=()):
    return RootFamily(
        root_key=("1234567893",),
        root={"npi": "1234567893", "display_name": 'Synthetic "Clinic"\nΔ'},
        children={"rates": child_records},
    )


def _reference_family_hash(definition, family):
    collection_fields = runner_codec.fields_by_collection(definition)
    children_by_collection = {
        collection: [
            {"key": key, "payload": payload}
            for _key_hash, key, payload in sorted(
                (
                    runner_codec.child_key_hash(definition, collection, child),
                    runner_codec.child_key_document(definition, collection, child),
                    runner_codec.record_payload(collection_fields[collection], child),
                )
                for child in children
            )
        ]
        for collection, children in family.children.items()
    }
    return _reference_hash(
        "family",
        {
            "contract": "custom-import-family/v1",
            "children": children_by_collection,
            "root_key": runner_codec.root_key_document(definition, family.root),
            "root_payload": runner_codec.record_payload(definition.root_fields, family.root),
        },
    )


@pytest.mark.parametrize("base_generation_id", [None, 8])
@pytest.mark.parametrize("root_numbers", [(), (4, 0, 4, 2)])
def test_ordered_candidate_hash_preserves_v1_bytes_and_duplicates(base_generation_id, root_numbers):
    root_hashes = [ordinal.to_bytes(32) for ordinal in root_numbers]
    expected = _reference_hash(
        "candidate",
        {
            "contract": "custom-import-candidate/v1",
            "base_generation_id": base_generation_id,
            "execution_id": 14,
            "fence": 3,
            "root_keys": [root_hash.hex() for root_hash in sorted(root_hashes)],
        },
    )
    arguments_by_name = {"execution_id": 14, "fence": 3, "base_generation_id": base_generation_id}

    assert candidate_hash_ordered(**arguments_by_name, root_key_hashes=iter(sorted(root_hashes))) == expected
    assert candidate_hash(**arguments_by_name, root_key_hashes=root_hashes) == expected


@pytest.mark.parametrize("root_hashes", [(b"short",), ("0" * 64,), (b"x" * 33,), (b"b" * 32, b"a" * 32)])
def test_ordered_candidate_hash_rejects_malformed_or_descending_digests(root_hashes):
    with pytest.raises(CandidateRunnerError, match="digest is malformed|not ordered"):
        candidate_hash_ordered(execution_id=14, fence=3, base_generation_id=None, root_key_hashes=iter(root_hashes))


@pytest.mark.parametrize("child_amounts", [(), (Decimal("12.50"), None, "missing", Decimal("12.50"))])
def test_ordered_family_hash_preserves_v1_bytes_null_missing_and_duplicates(child_amounts):
    definition = _definition()
    child_records = tuple(
        {"rate_npi": "1234567893", "service_code": 'A"\\\nΔ', **({"amount": amount} if amount != "missing" else {})}
        for amount in child_amounts
    )
    family = _family(child_records)
    expected = _reference_family_hash(definition, family)
    documents = sorted(_child_document(definition, child) for child in child_records)

    assert new_family_hash_ordered(definition, family.root, {"rates": iter(documents)}) == expected
    assert new_family_hash(definition, family) == expected


@pytest.mark.parametrize("key_type", ["string", "decimal"])
def test_ordered_child_verification_encodes_its_typed_key_once(monkeypatch, key_type):
    document = json.loads(_FIXTURE.read_text())
    if key_type == "decimal":
        document["schema"]["children"][0]["child_key"] = ["amount"]
        document["schema"]["children"][0]["fields"][2]["nullable"] = False
    definition = CustomImportDefinition.from_mapping(document)
    child_values_by_field = {"rate_npi": "1234567893", "service_code": 'A"\\\nΔ', "amount": Decimal("12.500")}
    family = _family((child_values_by_field,))
    expected = _reference_family_hash(definition, family)
    child_document = _child_document(definition, child_values_by_field)
    encoder = Mock(wraps=runner_codec.child_key_document)
    monkeypatch.setattr(runner_codec, "child_key_document", encoder)

    assert new_family_hash_ordered(definition, family.root, {"rates": iter((child_document,))}) == expected
    encoder.assert_called_once_with(definition, "rates", child_values_by_field)


def test_eager_family_hash_preserves_large_unprojected_child_payloads():
    document = json.loads(_FIXTURE.read_text())
    document["schema"]["children"][0]["fields"].append(
        {"id": "details", "slot": 6, "type": "string", "nullable": False}
    )
    definition = CustomImportDefinition.from_mapping(document)
    child_by_field = {
        "rate_npi": "1234567893",
        "service_code": "A",
        "amount": Decimal("1"),
        "details": "a" * (MAX_DEFINITION_BYTES + 1),
    }
    admitted = assemble_root_families(definition, (_family().root,), {"rates": (child_by_field,)})
    assert admitted.rejections == admitted.candidate_errors == ()
    assert len(admitted.families) == 1
    family = admitted.families[0]

    assert new_family_hash(definition, family) == _reference_family_hash(definition, family)
    with pytest.raises(CandidateRunnerError, match="exceeds the text limit"):
        new_family_hash_ordered(definition, family.root, {"rates": iter([_child_document(definition, child_by_field)])})


@pytest.mark.parametrize("has_hash_collision", [False, True])
def test_ordered_family_hash_preserves_full_tuple_ties(monkeypatch, has_hash_collision):
    definition = _definition()
    if has_hash_collision:
        original = runner_codec.digest_text
        monkeypatch.setattr(
            runner_codec,
            "digest_text",
            lambda domain, document: b"0" * 32 if domain == "child-key" else original(domain, document),
        )
    child_records = tuple(
        {"rate_npi": "1234567893", "service_code": service_code, "amount": Decimal(amount)}
        for service_code, amount in (("B", "2"), ("A", "2"), ("A", "1"), ("A", "1"))
    )
    family = _family(child_records)
    documents = sorted(_child_document(definition, child) for child in child_records)
    expected = _reference_family_hash(definition, family)

    assert new_family_hash_ordered(definition, family.root, {"rates": iter(documents)}) == expected
    assert new_family_hash(definition, family) == expected
    for reordered in (reversed(documents), (documents[2], documents[0])):
        with pytest.raises(CandidateRunnerError, match="child documents are not ordered"):
            new_family_hash_ordered(definition, family.root, {"rates": reordered})


def _definition_with_empty_collection():
    document = json.loads(_FIXTURE.read_text())
    document["schema"]["children"].append(
        {
            "name": "annotations",
            "parent_key": [{"child": "annotation_npi", "root": "npi"}],
            "child_key": ["annotation_code"],
            "fields": [
                {"id": "annotation_npi", "slot": 6, "type": "string", "nullable": False},
                {"id": "annotation_code", "slot": 7, "type": "string", "nullable": False},
            ],
        }
    )
    document["streams"].append(
        {
            "id": "annotations",
            "kind": "child",
            "child": "annotations",
            "format": "ndjson",
            "compression": "none",
            "snapshot_token": "snapshot_id",
        }
    )
    return CustomImportDefinition.from_mapping(document)


def test_ordered_family_hash_keeps_lexical_collection_order_and_empty_collections():
    definition = _definition_with_empty_collection()
    child_by_field = {"rate_npi": "1234567893", "service_code": "A", "amount": Decimal("1")}
    family = _family((child_by_field,))
    family = RootFamily(family.root_key, family.root, {"rates": family.children["rates"], "annotations": ()})
    documents_by_collection = {"rates": iter([_child_document(definition, child_by_field)]), "annotations": iter(())}

    assert new_family_hash_ordered(definition, family.root, documents_by_collection) == _reference_family_hash(
        definition, family
    )
    assert new_family_hash(definition, family) == _reference_family_hash(definition, family)
    for collections in ({"rates": ()}, {"rates": (), "annotations": (), "unknown": ()}):
        with pytest.raises(CandidateRunnerError, match="collections do not match"):
            new_family_hash_ordered(definition, family.root, collections)
        with pytest.raises(CandidateRunnerError, match="collections do not match"):
            new_family_hash(definition, RootFamily(family.root_key, family.root, collections))


@pytest.mark.parametrize(
    "corruption",
    [
        "tuple",
        "hash_shape",
        "hash_value",
        "key_type",
        "key_value",
        "key_utf8",
        "key_size",
        "key_byte_size",
        "payload_type",
        "payload_json",
        "payload_canonical",
        "payload_extra",
        "payload_decimal",
        "payload_null",
        "payload_missing",
        "payload_utf8",
        "payload_size",
        "payload_depth",
        "payload_recursion",
    ],
)
def test_ordered_family_hash_rejects_corrupt_fragments(corruption):
    definition = _definition()
    child_by_field = {"rate_npi": "1234567893", "service_code": "A", "amount": Decimal("1")}
    key_hash, canonical_key, canonical_payload = _child_document(definition, child_by_field)
    corrupt_documents_by_name = {
        "tuple": (key_hash, canonical_key),
        "hash_shape": (b"short", canonical_key, canonical_payload),
        "hash_value": (b"0" * 32, canonical_key, canonical_payload),
        "key_type": (key_hash, None, canonical_payload),
        "key_value": (key_hash, canonical_key.replace('"A"', '"B"'), canonical_payload),
        "key_utf8": (key_hash, "\ud800", canonical_payload),
        "key_size": (key_hash, "a" * (MAX_DEFINITION_BYTES + 1), canonical_payload),
        "key_byte_size": (key_hash, "Δ" * MAX_DEFINITION_BYTES, canonical_payload),
        "payload_type": (key_hash, canonical_key, None),
        "payload_json": (key_hash, canonical_key, "not-json"),
        "payload_canonical": (key_hash, canonical_key, canonical_payload + " "),
        "payload_extra": (key_hash, canonical_key, canonical_payload[:-1] + ',"unused":0}'),
        "payload_decimal": (key_hash, canonical_key, canonical_payload.replace('"value":"1"', '"value":"01"')),
        "payload_null": (
            key_hash,
            canonical_key,
            canonical_payload.replace('"state":"value","type":"string","value":"A"', '"state":"null","type":"string"'),
        ),
        "payload_missing": (
            key_hash,
            canonical_key,
            canonical_payload.replace('"state":"value","type":"string","value":"A"', '"state":"missing"'),
        ),
        "payload_utf8": (key_hash, canonical_key, "\ud800"),
        "payload_size": (key_hash, canonical_key, "a" * (MAX_DEFINITION_BYTES + 1)),
        "payload_depth": (key_hash, canonical_key, "[" * 33 + "0" + "]" * 33),
        "payload_recursion": (key_hash, canonical_key, "[" * 2_000 + "0" + "]" * 2_000),
    }

    with pytest.raises(CandidateRunnerError):
        new_family_hash_ordered(definition, _family().root, {"rates": iter([corrupt_documents_by_name[corruption]])})


class _OnePass:
    def __init__(self, values):
        self.values = iter(values)
        self.has_iterated = False

    def __iter__(self):
        assert not self.has_iterated, "ordered inputs must not be restarted"
        self.has_iterated = True
        return self.values

    def __len__(self):
        raise AssertionError("ordered inputs must not be materialized")


def test_ordered_candidate_hash_consumes_one_root_at_a_time(monkeypatch):
    counts_by_stage = {"written": 0}
    original = runner_codec._digest_json_string

    def record_string(digest, value):
        original(digest, value)
        counts_by_stage["written"] += 1

    def root_hashes():
        for ordinal in range(_CANDIDATE_ROOT_COUNT):
            assert counts_by_stage["written"] == ordinal, "hash each root before requesting another"
            yield ordinal.to_bytes(32)
        assert counts_by_stage["written"] == _CANDIDATE_ROOT_COUNT

    expected = candidate_hash(
        execution_id=14,
        fence=3,
        base_generation_id=8,
        root_key_hashes=tuple(ordinal.to_bytes(32) for ordinal in range(_CANDIDATE_ROOT_COUNT)),
    )
    monkeypatch.setattr(runner_codec, "_digest_json_string", record_string)
    _reject_aggregate_canonical_json(monkeypatch)

    assert (
        candidate_hash_ordered(execution_id=14, fence=3, base_generation_id=8, root_key_hashes=_OnePass(root_hashes()))
        == expected
    )


def test_ordered_family_hash_consumes_one_child_at_a_time(monkeypatch):
    definition = _definition()
    child_by_field = {"rate_npi": "1234567893", "service_code": "A", "amount": Decimal("1")}
    document = _child_document(definition, child_by_field)
    counts_by_stage = {"written": 0}
    original = runner_codec._digest_json_string

    def record_string(digest, value):
        original(digest, value)
        if value == document[2]:
            counts_by_stage["written"] += 1

    def child_documents():
        for ordinal in range(_FAMILY_CHILD_COUNT):
            assert counts_by_stage["written"] == ordinal, "hash each child before requesting another"
            yield document
        assert counts_by_stage["written"] == _FAMILY_CHILD_COUNT

    expected = new_family_hash(definition, _family((child_by_field,) * _FAMILY_CHILD_COUNT))
    monkeypatch.setattr(runner_codec, "_digest_json_string", record_string)
    _reject_aggregate_canonical_json(monkeypatch)

    assert new_family_hash_ordered(definition, _family().root, {"rates": _OnePass(child_documents())}) == expected


def test_retained_child_query_uses_fixed_parameters_above_asyncpg_limit():
    family_count = _ASYNC_PG_PARAMETER_LIMIT + 1
    statement = previous_children_statement(
        _request(),
        CurrentGenerationPointer(generation_id=31, definition_revision_id=12, schema_revision_id=13, version=1),
    )
    compiled = statement.compile(dialect=postgresql.dialect())

    assert family_count > _ASYNC_PG_PARAMETER_LIMIT
    assert " IN (" not in str(compiled)
    assert len(compiled.params) <= 3
