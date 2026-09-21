# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Scale regressions for bounded custom-import candidate orchestration."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path

from sqlalchemy.dialects import postgresql

import process.custom_import.runner_codec as runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.runner_codec import candidate_hash, new_family_hash
from process.custom_import.runner_graph import previous_children_statement
from process.custom_import.runner_types import CandidateRunRequest, CurrentGenerationPointer

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
