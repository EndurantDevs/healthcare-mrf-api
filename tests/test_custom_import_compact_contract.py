# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable compatibility checks for versioned materialization evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import Mock

import pytest
from sqlalchemy import CheckConstraint
from sqlalchemy.dialects import postgresql

from db.models.custom_import import CustomImportGenerationSeal
from process.custom_import import operator, publication
from tests.test_custom_import_operator import _generation_row, _Session
from tests.test_custom_import_publication import _generation_seal_fixtures, _no_change_fixtures

_LEGACY = "custom-import/materialization/v1"
_COMPACT = "custom-import/materialization/v2"


def _request(generation, token):
    return publication._GenerationSealRequest(
        generation.dataset_id, generation.generation_id, generation.producing_fence, token
    )


def _evidence(*, root_count=7, child_count=10, winner_count=11, profile_count=12):
    scopes = [("root", 0, root_count), ("child", 1, child_count)]
    scopes.extend(("winner", slot, winner_count if slot == 1 else 0) for slot in range(1, profile_count + 1))
    return {
        "contract": "custom-import/verification-sample/v1",
        "seed_sha256": "a" * 64,
        "selection_sha256": "b" * 64,
        "coverage": [
            {"kind": kind, "slot": slot, "population": count, "sampled": min(count, 1), "capped": count > 1}
            for kind, slot, count in scopes
        ],
    }


def _compact():
    generation, materialization, seal, token = _generation_seal_fixtures()
    evidence = _evidence()
    materialization = replace(materialization, materialization_contract=_COMPACT, verification_evidence=evidence)
    seal.materialization_contract = _COMPACT
    seal.verification_evidence = deepcopy(evidence)
    return generation, materialization, seal, token


@pytest.mark.parametrize("contract", ("absent", None, _LEGACY))
def test_legacy_seal_defaults_and_replay_preserve_receipt_shape(contract):
    generation, materialization, seal, token = _generation_seal_fixtures()
    assert materialization.materialization_contract == _LEGACY
    assert materialization.verification_evidence is None
    if contract != "absent":
        seal.materialization_contract = contract
        seal.verification_evidence = None

    publication._validate_generation_seal(seal, generation, materialization)
    receipt = publication._generation_seal_replay(seal, generation, _request(generation, token))

    assert asdict(receipt) == {
        "generation_id": 9,
        "dataset_id": 1,
        "execution_id": 4,
        "materialization_sha256": (b"m" * 32).hex(),
        "effective_output_sha256": (b"e" * 32).hex(),
        "root_count": 7,
        "family_count": 8,
        "generation_family_count": 9,
        "family_child_count": 10,
        "winner_count": 11,
        "profile_count": 12,
        "root_scalar_count": 13,
        "child_scalar_count": 14,
        "replayed": True,
    }


def test_new_legacy_seal_sets_explicit_algorithm_without_changing_envelope():
    generation, materialization, _seal, token = _generation_seal_fixtures()
    seal = publication._new_generation_seal(generation, materialization, _request(generation, token))
    assert seal.seal_contract == "custom-import-generation-seal/v1"
    assert seal.materialization_contract == _LEGACY
    assert seal.verification_evidence is None
    publication._validate_generation_seal(seal, generation, materialization)


@pytest.mark.parametrize("corruption", ("evidence", "algorithm"))
def test_compact_evidence_round_trips_and_is_bound_to_verified_materialization(corruption):
    generation, materialization, _seal, token = _compact()
    request = _request(generation, token)
    seal = publication._new_generation_seal(generation, materialization, request)
    assert seal.seal_contract == "custom-import-generation-seal/v1"
    assert seal.materialization_contract == _COMPACT
    assert seal.verification_evidence == materialization.verification_evidence
    publication._validate_generation_seal(seal, generation, materialization)
    assert publication._generation_seal_replay(seal, generation, request).replayed is True

    if corruption == "evidence":
        seal.verification_evidence = {**seal.verification_evidence, "selection_sha256": "c" * 64}
    else:
        seal.materialization_contract = _LEGACY
        seal.verification_evidence = None
    with pytest.raises(publication.PublicationConflict):
        publication._validate_generation_seal(seal, generation, materialization)


@pytest.mark.parametrize("evidence", (None, {}, [], "{}", True))
def test_compact_seal_requires_structured_verification_evidence(evidence):
    generation, materialization, seal, token = _compact()
    seal.verification_evidence = evidence
    materialization = replace(materialization, verification_evidence=evidence)
    with pytest.raises(publication.PublicationConflict):
        publication._generation_seal_replay(seal, generation, _request(generation, token))
    with pytest.raises(publication.PublicationConflict):
        publication._new_generation_seal(generation, materialization, _request(generation, token))


@pytest.mark.parametrize("contract", (None, _LEGACY))
def test_legacy_or_missing_algorithm_cannot_hide_compact_evidence(contract):
    generation, _materialization, seal, _token = _compact()
    seal.materialization_contract = contract
    with pytest.raises(publication.PublicationConflict):
        publication._validate_generation_seal_identity(seal, generation)


@pytest.mark.parametrize(
    "field,value",
    (
        ("contract", "custom-import/verification-sample/v2"),
        ("seed_sha256", "g" * 64),
        ("seed_sha256", "a" * 63),
        ("selection_sha256", None),
        ("coverage", []),
        ("coverage", {}),
        ("payload", "synthetic extra field"),
    ),
)
def test_compact_evidence_rejects_unknown_or_malformed_fields(field, value):
    generation, _materialization, seal, _token = _compact()
    seal.verification_evidence[field] = value
    with pytest.raises(publication.PublicationConflict):
        publication._validate_generation_seal_identity(seal, generation)


@pytest.mark.parametrize(
    "field,value",
    (
        ("kind", "unknown"),
        ("kind", []),
        ("slot", 1),
        ("slot", False),
        ("population", -1),
        ("population", True),
        ("sampled", -1),
        ("sampled", 8),
        ("sampled", True),
        ("capped", 0),
        ("payload", "synthetic extra field"),
    ),
)
def test_compact_coverage_rejects_ambiguous_counts_and_unknown_fields(field, value):
    generation, _materialization, seal, _token = _compact()
    seal.verification_evidence["coverage"][0][field] = value
    with pytest.raises(publication.PublicationConflict):
        publication._validate_generation_seal_identity(seal, generation)


@pytest.mark.parametrize("corruption", ("duplicate", "missing_root", "child_zero", "winner_zero", "too_many"))
def test_compact_coverage_requires_unique_bounded_scopes_and_root_even_when_empty(corruption):
    generation, _materialization, seal, _token = _compact()
    coverage = seal.verification_evidence["coverage"]
    if corruption == "duplicate":
        coverage.append(dict(coverage[0]))
    elif corruption == "missing_root":
        coverage.pop(0)
    elif corruption == "child_zero":
        coverage[1]["slot"] = 0
    elif corruption == "winner_zero":
        coverage[2]["slot"] = 0
    else:
        coverage[:] = [coverage[0]] + [
            {"kind": "child", "slot": slot, "population": 0, "sampled": 0, "capped": False} for slot in range(1, 257)
        ]
    with pytest.raises(publication.PublicationConflict):
        publication._validate_generation_seal_identity(seal, generation)


def test_empty_compact_population_still_has_explicit_root_evidence():
    generation, materialization, seal, _token = _compact()
    coverage_entries = [{"kind": "root", "slot": 0, "population": 0, "sampled": 0, "capped": False}]
    seal.verification_evidence["coverage"] = coverage_entries
    counts_by_name = {name: 0 for name in asdict(materialization) if name.endswith("_count")}
    for name, value in counts_by_name.items():
        setattr(seal, name, value)
    generation.root_count = generation.family_count = 0
    materialization = replace(
        materialization, verification_evidence=deepcopy(seal.verification_evidence), **counts_by_name
    )
    publication._validate_generation_seal(seal, generation, materialization)


def test_compact_evidence_accepts_256_complete_scopes():
    generation, _materialization, seal, _token = _compact()
    coverage = seal.verification_evidence["coverage"]
    coverage.extend(
        {"kind": "child", "slot": slot, "population": 0, "sampled": 0, "capped": False} for slot in range(2, 244)
    )
    assert len(coverage) == 256
    publication._validate_generation_seal_identity(seal, generation)


@pytest.mark.parametrize("encoded_bytes", (65536, 65537))
def test_compact_evidence_bounds_canonical_json_bytes(monkeypatch, encoded_bytes):
    generation, _materialization, seal, _token = _compact()
    canonical = publication.canonical_json(seal.verification_evidence)
    encoded = canonical + " " * (encoded_bytes - len(canonical.encode("utf-8")))
    monkeypatch.setattr(publication, "canonical_json", lambda _evidence: encoded)
    if encoded_bytes > 65536:
        with pytest.raises(publication.PublicationConflict, match="exceeds its bound"):
            publication._validate_generation_seal_identity(seal, generation)
    else:
        publication._validate_generation_seal_identity(seal, generation)


@pytest.mark.parametrize("kind", ("child", "winner"))
@pytest.mark.parametrize("slot", (32767, 32768))
def test_compact_coverage_slots_fit_native_small_integer(kind, slot):
    generation, _materialization, seal, _token = _compact()
    scope = next(item for item in seal.verification_evidence["coverage"] if item["kind"] == kind)
    scope["slot"] = slot
    if slot == 32767:
        publication._validate_generation_seal_identity(seal, generation)
    else:
        with pytest.raises(publication.PublicationConflict):
            publication._validate_generation_seal_identity(seal, generation)


@pytest.mark.parametrize("kind", ("root", "child", "winner"))
@pytest.mark.parametrize("delta", (-1, 1))
def test_compact_population_totals_must_match_creation_replay_and_materialization(kind, delta):
    generation, materialization, seal, token = _compact()
    scope = next(item for item in seal.verification_evidence["coverage"] if item["kind"] == kind)
    scope["population"] += delta
    materialization = replace(materialization, verification_evidence=deepcopy(seal.verification_evidence))
    request = _request(generation, token)
    with pytest.raises(publication.PublicationConflict, match="populations differ"):
        publication._new_generation_seal(generation, materialization, request)
    with pytest.raises(publication.PublicationConflict, match="populations differ"):
        publication._generation_seal_replay(seal, generation, request)
    with pytest.raises(publication.PublicationConflict, match="populations differ"):
        publication._validate_generation_seal(seal, generation, materialization)


@pytest.mark.parametrize("extra", (False, True))
def test_compact_coverage_reports_each_profile_including_empty_profiles(extra):
    generation, materialization, seal, token = _compact()
    coverage = seal.verification_evidence["coverage"]
    if extra:
        coverage.append({"kind": "winner", "slot": 13, "population": 0, "sampled": 0, "capped": False})
    else:
        coverage.pop()
    materialization = replace(materialization, verification_evidence=deepcopy(seal.verification_evidence))
    with pytest.raises(publication.PublicationConflict, match="populations differ"):
        publication._new_generation_seal(generation, materialization, _request(generation, token))
    with pytest.raises(publication.PublicationConflict, match="populations differ"):
        publication._generation_seal_replay(seal, generation, _request(generation, token))


@pytest.mark.parametrize("scope_index", (0, 3))
def test_compact_capped_truth_distinguishes_partial_and_complete_samples(scope_index):
    generation, materialization, seal, token = _compact()
    scope = seal.verification_evidence["coverage"][scope_index]
    scope["capped"] = not scope["capped"]
    materialization = replace(materialization, verification_evidence=deepcopy(seal.verification_evidence))
    with pytest.raises(publication.PublicationConflict):
        publication._new_generation_seal(generation, materialization, _request(generation, token))
    with pytest.raises(publication.PublicationConflict):
        publication._generation_seal_replay(seal, generation, _request(generation, token))


def test_compact_populations_are_summed_across_child_and_winner_scopes():
    generation, materialization, seal, token = _compact()
    coverage = seal.verification_evidence["coverage"]
    coverage[1]["population"] = 4
    coverage.append({"kind": "child", "slot": 2, "population": 6, "sampled": 1, "capped": True})
    coverage[2]["population"] = 5
    coverage[3].update(population=6, sampled=1, capped=True)
    materialization = replace(materialization, verification_evidence=deepcopy(seal.verification_evidence))
    publication._validate_generation_seal(seal, generation, materialization)
    publication._new_generation_seal(generation, materialization, _request(generation, token))


@pytest.mark.parametrize(
    "contract", ("", False, 0, "custom-import/materialization/v0", "custom-import/materialization/v3", 7, [])
)
def test_unknown_algorithm_is_rejected_on_validation_replay_and_creation(contract):
    generation, materialization, seal, token = _generation_seal_fixtures()
    seal.materialization_contract = contract
    seal.verification_evidence = None
    materialization = replace(materialization, materialization_contract=contract)
    request = _request(generation, token)

    with pytest.raises(publication.PublicationConflict):
        publication._validate_generation_seal_identity(seal, generation)
    with pytest.raises(publication.PublicationConflict):
        publication._generation_seal_replay(seal, generation, request)
    with pytest.raises(publication.PublicationConflict):
        publication._new_generation_seal(generation, materialization, request)


def test_legacy_no_change_receipt_retains_exact_canonical_bytes_and_domain():
    request, execution, base, candidate, base_seal, candidate_seal, seal = _no_change_fixtures()
    expected = json.dumps(
        {
            "base_generation_id": 3,
            "base_effective_output_sha256": (b"o" * 32).hex(),
            "base_pointer_version": 4,
            "base_source_bundle_sha256": (b"b" * 32).hex(),
            "candidate_generation_id": 5,
            "candidate_source_bundle_sha256": (b"c" * 32).hex(),
            "capture_bundle_id": 9,
            "contract": "custom-import-no-change-seal/v1",
            "dataset_id": 1,
            "definition_revision_id": 7,
            "effective_output_sha256": (b"o" * 32).hex(),
            "execution_id": 2,
            "schema_revision_id": 8,
            "sealing_fence": 6,
            "sealing_token_sha256": (b"t" * 32).hex(),
        },
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    expected_digest = hashlib.sha256(b"custom-import/v1\x00no-change-receipt/v1\x00" + expected.encode()).digest()
    assert seal.canonical_receipt == expected
    assert seal.receipt_sha256 == expected_digest
    base_seal.materialization_contract = candidate_seal.materialization_contract = _LEGACY
    base_seal.verification_evidence = candidate_seal.verification_evidence = None
    publication._validate_no_change_seal(seal, request, execution, base, candidate, base_seal, candidate_seal)


@pytest.mark.parametrize("base_contract,candidate_contract", ((_LEGACY, _COMPACT), (_COMPACT, _LEGACY)))
def test_mixed_algorithms_cannot_replay_no_change_even_with_identical_digest_bytes(base_contract, candidate_contract):
    request, execution, base, candidate, base_seal, candidate_seal, seal = _no_change_fixtures()
    base_seal.materialization_contract = base_contract
    candidate_seal.materialization_contract = candidate_contract
    base_seal.verification_evidence = _evidence() if base_contract == _COMPACT else None
    candidate_seal.verification_evidence = _evidence() if candidate_contract == _COMPACT else None
    assert base_seal.effective_output_sha256 == candidate_seal.effective_output_sha256
    with pytest.raises(publication.PublicationConflict):
        publication._no_change_receipt_document(
            request,
            execution,
            base_generation=base,
            candidate_generation=candidate,
            base_seal=base_seal,
            candidate_seal=candidate_seal,
        )
    with pytest.raises(publication.PublicationConflict):
        publication._validate_no_change_seal(seal, request, execution, base, candidate, base_seal, candidate_seal)


def test_compatible_compact_no_change_receipt_binds_algorithm_without_changing_legacy_bytes():
    request, execution, base, candidate, base_seal, candidate_seal, seal = _no_change_fixtures()
    legacy_receipt, legacy_digest = seal.canonical_receipt, seal.receipt_sha256
    for generation_seal in (base_seal, candidate_seal):
        generation_seal.materialization_contract = _COMPACT
        generation_seal.verification_evidence = _evidence()
    seal.canonical_receipt, seal.receipt_sha256 = publication._no_change_receipt_document(
        request,
        execution,
        base_generation=base,
        candidate_generation=candidate,
        base_seal=base_seal,
        candidate_seal=candidate_seal,
    )
    assert seal.canonical_receipt != legacy_receipt
    assert seal.receipt_sha256 != legacy_digest
    assert json.loads(seal.canonical_receipt)["materialization_contract"] == _COMPACT
    publication._validate_no_change_seal(seal, request, execution, base, candidate, base_seal, candidate_seal)

    base_seal.materialization_contract = candidate_seal.materialization_contract = _LEGACY
    base_seal.verification_evidence = candidate_seal.verification_evidence = None
    with pytest.raises(publication.PublicationConflict):
        publication._validate_no_change_seal(seal, request, execution, base, candidate, base_seal, candidate_seal)


def test_generation_seal_model_declares_native_known_contract_and_evidence_constraints():
    table = CustomImportGenerationSeal.__table__
    assert table.c.materialization_contract.nullable is False
    assert _LEGACY in str(table.c.materialization_contract.server_default.arg)
    assert table.c.verification_evidence.nullable is True
    constraint = next(
        constraint
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
        and constraint.name == "custom_import_generation_seal_materialization_check"
    )
    assert " ".join(str(constraint.sqltext).split()) == (
        "(materialization_contract = 'custom-import/materialization/v1' AND verification_evidence IS NULL) OR "
        "(materialization_contract = 'custom-import/materialization/v2' AND verification_evidence IS NOT NULL "
        "AND jsonb_typeof(verification_evidence) = 'object')"
    )


def test_legacy_absent_evidence_binds_sql_null_not_json_null():
    evidence_type = CustomImportGenerationSeal.__table__.c.verification_evidence.type
    assert evidence_type.none_as_null is True
    assert evidence_type.should_evaluate_none is False
    bind = evidence_type.bind_processor(postgresql.dialect(json_serializer=json.dumps))
    assert bind is not None
    assert bind(None) is None
    assert json.loads(bind(_evidence())) == _evidence()


@pytest.mark.parametrize("contract", ("absent", None, _LEGACY, _COMPACT))
async def test_operator_status_distinguishes_legacy_and_compact_evidence(contract):
    generation_row = _generation_row(sealed=True)
    evidence = _evidence(root_count=2, child_count=3, winner_count=2, profile_count=1) if contract == _COMPACT else None
    if contract != "absent":
        generation_row.update(materialization_contract=contract, verification_evidence=evidence)
    session = _Session(generation_row)

    status = await operator.inspect_generation(session, dataset_id=3, generation_id=19)

    assert status.seal.materialization_contract == (_COMPACT if contract == _COMPACT else _LEGACY)
    assert status.seal.verification_evidence == evidence
    assert len(session.statements) == 1
    selected_columns = session.statements[0].selected_columns.keys()
    assert "materialization_contract" in selected_columns and "verification_evidence" in selected_columns


@pytest.mark.parametrize(
    "contract,evidence",
    (("unknown", None), ("", None), (False, None), (_COMPACT, None), (_COMPACT, {}), (_LEGACY, _evidence())),
)
async def test_operator_rejects_unknown_algorithm_or_incompatible_evidence(contract, evidence):
    generation_row = _generation_row(sealed=True)
    generation_row.update(materialization_contract=contract, verification_evidence=evidence)
    with pytest.raises(operator.OperatorInvariantError):
        await operator.inspect_generation(_Session(generation_row), dataset_id=3, generation_id=19)


@pytest.mark.parametrize("field", ("sealed_root_count", "family_child_count", "winner_count", "profile_count"))
async def test_operator_rejects_coverage_that_disagrees_with_sealed_counts(field):
    generation_row = _generation_row(sealed=True)
    generation_row.update(
        materialization_contract=_COMPACT,
        verification_evidence=_evidence(root_count=2, child_count=3, winner_count=2, profile_count=1),
    )
    generation_row[field] += 1
    with pytest.raises(operator.OperatorInvariantError):
        await operator.inspect_generation(_Session(generation_row), dataset_id=3, generation_id=19)


def _migration():
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20261010010000_custom_import_materialization_contract.py"
    )
    spec = importlib.util.spec_from_file_location("custom_import_compact_contract_migration", path)
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_compact_migration_adds_only_metadata_and_native_constraint(monkeypatch):
    migration = _migration()
    operations = Mock()
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_compact")
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    migration.upgrade()

    assert migration.down_revision == "20261010000000_custom_import_admission_indexes"
    assert [call[0] for call in operations.method_calls] == ["add_column", "add_column", "create_check_constraint"]
    columns = [call.args[1] for call in operations.add_column.call_args_list]
    assert [column.name for column in columns] == ["materialization_contract", "verification_evidence"]
    assert columns[0].nullable is False and _LEGACY in str(columns[0].server_default.arg)
    assert columns[1].nullable is True and columns[1].type.none_as_null is True
    assert all(call.args[0] == "custom_import_generation_seal" for call in operations.add_column.call_args_list)
    assert all(call.kwargs == {"schema": "synthetic_compact"} for call in operations.method_calls)
    constraint = operations.create_check_constraint.call_args
    assert constraint.args[:2] == (
        "custom_import_generation_seal_materialization_check",
        "custom_import_generation_seal",
    )
    model_check = next(
        check for check in CustomImportGenerationSeal.__table__.constraints if check.name == constraint.args[0]
    )
    assert " ".join(constraint.args[2].split()) == " ".join(str(model_check.sqltext).split())


@pytest.mark.parametrize("retained_compact", (False, True))
@pytest.mark.parametrize("schema", ("synthetic_compact", 'synthetic_"quoted'))
def test_compact_migration_checks_retained_algorithms_before_any_downgrade_drop(monkeypatch, retained_compact, schema):
    migration = _migration()
    operations = Mock()
    operations.get_bind.return_value.scalar.return_value = retained_compact
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    if retained_compact:
        with pytest.raises(RuntimeError, match="compact_materialization_downgrade_blocked"):
            migration.downgrade()
        operations.drop_constraint.assert_not_called()
        operations.drop_column.assert_not_called()
    else:
        migration.downgrade()
        assert [call[0] for call in operations.method_calls] == [
            "execute",
            "get_bind",
            "drop_constraint",
            "drop_column",
            "drop_column",
        ]
        assert [call.args[1] for call in operations.drop_column.call_args_list] == [
            "verification_evidence",
            "materialization_contract",
        ]
    assert [call[0] for call in operations.mock_calls[:3]] == ["execute", "get_bind", "get_bind().scalar"]
    operations.execute.assert_called_once()
    quoted_schema = '"synthetic_compact"' if schema == "synthetic_compact" else '"synthetic_""quoted"'
    assert str(operations.execute.call_args.args[0]) == (
        f'LOCK TABLE {quoted_schema}."custom_import_generation_seal" IN SHARE ROW EXCLUSIVE MODE'
    )
    dialect = postgresql.dialect()
    query = operations.get_bind.return_value.scalar.call_args.args[0].compile(dialect=dialect)
    assert (
        f"{dialect.identifier_preparer.quote_schema(schema)}.custom_import_generation_seal.materialization_contract !="
        in str(query)
    )
    assert tuple(query.params.values()) == (_LEGACY,)
