# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure contracts for dormant source-qualified formulary persistence."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_batch
from process.formulary_fhir import repository_shared
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import CheckpointWrite
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository_batch import medication_version_id
from process.formulary_fhir.repository_shared import aggregate_hash
from process.formulary_fhir.repository_shared import configured_schema
from process.formulary_fhir.repository_shared import flags_intent
from process.formulary_fhir.repository_shared import intent_flags
from process.formulary_fhir.repository_shared import json_object
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.repository_shared import persisted_membership_proof
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.types import FHIRCoding, MedicationRecord


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)
CONTRACT_HASH = "c" * 64


def _dataset(source_id: str = "source-a", **overrides) -> DatasetRef:
    values_by_field = {
        "source_id": source_id,
        "dataset_id": "ffd_" + "a" * 48,
        "run_id": "run-a",
        "previous_dataset_id": None,
        "cutoff_at": CUTOFF,
        "acquisition_contract_hash": CONTRACT_HASH,
        "intent": "none",
        "status": "building",
    }
    values_by_field.update(overrides)
    return DatasetRef(**values_by_field)


def _alias(source_id: str = "source-a", **overrides) -> AliasRef:
    values_by_field = {
        "source_id": source_id,
        "public_id": "fhir_" + "a" * 26,
        "alias_id": "ffa_" + "a" * 48,
        "source_plan_identifier": "SYNTHETIC-PLAN",
    }
    values_by_field.update(overrides)
    return AliasRef(**values_by_field)


def _medication(index: int = 1) -> MedicationRecord:
    return MedicationRecord(
        upstream_medication_id=f"med-{index}",
        upstream_version_id="1",
        upstream_last_updated=CUTOFF,
        status="active",
        drug_name="Synthetic medication",
        rxnorm_id=str(index),
        ndc11=None,
        codings=(FHIRCoding("system", str(index), "Synthetic", None),),
        raw_extensions=(),
        source_plan_identifiers=("SYNTHETIC-PLAN",),
        drug_tier="preferred",
        prior_authorization=False,
        step_therapy=False,
        quantity_limit=False,
        alternative_references=(),
        content_hash=f"{index:064x}",
    )


def test_schema_qualification_matches_the_orm_contract(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_schema")
    monkeypatch.setenv("DB_SCHEMA", "synthetic_schema")
    assert configured_schema() == "synthetic_schema"
    assert table_name('table"name') == '"synthetic_schema"."table""name"'
    monkeypatch.setenv("DB_SCHEMA", "other_schema")
    with pytest.raises(RuntimeError, match="must match"):
        configured_schema()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA")
    monkeypatch.delenv("DB_SCHEMA")
    assert configured_schema() == "mrf"


@pytest.mark.parametrize("value", [None, "", " padded", "line\nbreak", "xxxx"])
def test_strict_text_rejects_noncanonical_values(value):
    with pytest.raises(ValueError, match="label is invalid"):
        strict_text(value, "label", 3)


def test_hash_time_and_stable_identity_are_exact_and_source_qualified():
    assert strict_hash("a" * 64, "hash") == "a" * 64
    for invalid_hash in (None, "A" * 64, "a" * 63):
        with pytest.raises(ValueError, match="hash is invalid"):
            strict_hash(invalid_hash, "hash")
    offset_time = dt.datetime(
        2026,
        8,
        7,
        14,
        tzinfo=dt.timezone(dt.timedelta(hours=2)),
    )
    assert utc_timestamp(offset_time, "time") == CUTOFF
    for invalid_time in (None, dt.datetime(2026, 8, 7, 12)):
        with pytest.raises(ValueError, match="time is invalid"):
            utc_timestamp(invalid_time, "time")
    identity_a = stable_id("id_", "source-a", "part")
    identity_b = stable_id("id_", "source-b", "part")
    assert identity_a != identity_b
    with pytest.raises(ValueError, match="stable identity is empty"):
        stable_id("id_", "source-a")


def test_json_and_publication_intent_contracts_fail_closed():
    assert json_text({"b": 1, "a": [True]}) == '{"a":[true],"b":1}'
    assert json_object('{"a":1}') == {"a": 1}
    assert json_object({"a": 1}) == {"a": 1}
    with pytest.raises(TypeError):
        json_text({"invalid": object()})
    for invalid_json in ("not-json", "[]", None):
        with pytest.raises(RuntimeError, match="stored JSON"):
            json_object(invalid_json)
    assert [intent_flags(value) for value in ("none", "requested", "seed")] == [
        (False, False),
        (True, False),
        (False, True),
    ]
    assert flags_intent(False, False) == "none"
    assert flags_intent(True, False) == "requested"
    assert flags_intent(False, True) == "seed"
    with pytest.raises(ValueError, match="intent is invalid"):
        intent_flags("invalid")
    with pytest.raises(RuntimeError, match="stored publication intent"):
        flags_intent(True, True)


def test_dataset_alias_and_prior_contracts_validate_ownership():
    dataset = _dataset()
    alias = _alias()
    assert dataset.status == "building"
    assert alias.source_id == dataset.source_id
    with pytest.raises(ValueError, match="dataset status"):
        _dataset(status="unknown")
    with pytest.raises(ValueError, match="previous dataset id"):
        _dataset(previous_dataset_id=" bad")
    with pytest.raises(ValueError, match="public id"):
        _alias(public_id="")
    prior = PriorAliasState(
        alias.source_id,
        alias.public_id,
        alias.alias_id,
        alias.source_plan_identifier,
        "ffav_" + "a" * 48,
        0,
        CUTOFF,
        {},
        membership_hash({}),
    )
    assert prior.expected_count == 0
    with pytest.raises(ValueError, match="prior alias count"):
        PriorAliasState(
            alias.source_id,
            alias.public_id,
            alias.alias_id,
            alias.source_plan_identifier,
            "ffav_" + "a" * 48,
            -1,
            CUTOFF,
            {},
            "a" * 64,
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"expected_count": -1}, "expected count"),
        ({"medications": []}, "exact tuple"),
        ({"fence_token": 0}, "fence"),
    ],
)
def test_alias_version_write_rejects_invalid_values(overrides, message):
    values_by_field = {
        "dataset": _dataset(),
        "alias": _alias(),
        "expected_count": 1,
        "medications": (_medication(),),
        "fence_token": 1,
    }
    values_by_field.update(overrides)
    with pytest.raises(ValueError, match=message):
        AliasVersionWrite(**values_by_field)
    with pytest.raises(ValueError, match="source does not match"):
        AliasVersionWrite(
            _dataset(),
            _alias("source-b"),
            1,
            (_medication(),),
            1,
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"fence_token": 0}, "fence"),
        ({"acquisition_mode": "delta"}, "mode"),
        ({"expected_count": -1}, "count"),
        ({"processed_count": -1}, "progress"),
        ({"expected_count": 1, "processed_count": 2}, "exceeds"),
        ({"membership_hash": "bad"}, "membership hash"),
        ({"completed": 1}, "completion"),
        ({"completed": True}, "completed checkpoint"),
    ],
)
def test_checkpoint_contract_rejects_invalid_values(overrides, message):
    values_by_field = {
        "dataset": _dataset(),
        "alias": _alias(),
        "fence_token": 1,
        "acquisition_mode": "full",
        "expected_count": None,
        "processed_count": 0,
        "membership_hash": None,
        "completed": False,
    }
    values_by_field.update(overrides)
    with pytest.raises(ValueError, match=message):
        CheckpointWrite(**values_by_field)
    with pytest.raises(ValueError, match="source is inconsistent"):
        CheckpointWrite(
            _dataset(),
            _alias("source-b"),
            1,
            "full",
            None,
            0,
            None,
            False,
        )


def test_membership_and_medication_id_hashes_are_deterministic():
    variants_by_id_a = {"med-b": "b" * 64, "med-a": "a" * 64}
    variants_by_id_b = {"med-a": "a" * 64, "med-b": "b" * 64}
    assert membership_hash(variants_by_id_a) == membership_hash(variants_by_id_b)
    assert aggregate_hash("domain", ["b", "a"]) == aggregate_hash(
        "domain",
        ["a", "b"],
    )
    medication = _medication()
    assert medication_version_id("source-a", medication) != medication_version_id(
        "source-b",
        medication,
    )
    with pytest.raises(ValueError, match="variant hash"):
        membership_hash({"med-a": "bad"})


@pytest.mark.asyncio
async def test_persisted_membership_proof_uses_bounded_keyset_pages(monkeypatch):
    monkeypatch.setattr(repository_shared, "WRITE_BATCH_SIZE", 2)
    database = SimpleNamespace(
        all=AsyncMock(
            side_effect=[
                [
                    {"upstream_medication_id": "med-a", "variant_hash": "a" * 64},
                    {"upstream_medication_id": "med-b", "variant_hash": "b" * 64},
                ],
                [{"upstream_medication_id": "med-c", "variant_hash": "c" * 64}],
            ]
        )
    )
    count, proof_hash, variants = await persisted_membership_proof(
        database,
        "source-a",
        "alias-version",
    )
    assert count == 3
    assert proof_hash == membership_hash(variants)
    assert database.all.await_args_list[1].kwargs["last_medication_id"] == "med-b"


def test_batch_bound_and_coding_serialization_are_explicit(monkeypatch):
    monkeypatch.setattr(repository_batch, "WRITE_BATCH_SIZE", 2)
    medications = tuple(_medication(index) for index in range(5))
    assert [len(batch) for batch in repository_batch._batches(medications)] == [2, 2, 1]
    values_sql, params = repository_batch._medication_values(
        "source-a",
        medications[:1],
    )
    assert values_sql.count("(") >= 1
    assert params["codings_json_0"] == (
        '[{"code":"0","display":"Synthetic","system":"system","version":null}]'
    )
    assert "source_plan_identifiers" in params["metadata_json_0"]


def test_repository_modules_are_dormant_and_have_no_removed_persistence_fields():
    repository_sources = "\n".join(
        path.read_text()
        for path in sorted(
            (ROOT / "process" / "formulary_fhir").glob("repository*.py")
        )
    )
    for forbidden_text in (
        "next_url",
        "continuation",
        "cursor",
        "reused_from_alias_version_id",
        "fhir-formulary-primary",
        "FHIRFormularyClient",
        "requests.",
        "httpx.",
    ):
        assert forbidden_text not in repository_sources
    repository = FHIRFormularyRepository(source_id="source-a", database=object())
    assert repository.source_id == "source-a"
    with pytest.raises(ValueError, match="source id"):
        FHIRFormularyRepository(source_id="", database=object())
