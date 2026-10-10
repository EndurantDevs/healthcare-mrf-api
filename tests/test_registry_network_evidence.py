# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Evidence orchestration tests; native syntax and SQL proof remain separate."""

import copy
import importlib.util
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from db.models.network_registry import NetworkRegistryRecord
from process import registry_record_store as store
from process.registry_management_permissions import _INSERT_COLUMNS, _TABLES, _UPDATE_COLUMNS
from process.registry_manual_undo import RegistryManualUndoCommand, _network_undo_evidence, _prepare_correction


def _evidence(**changes):
    return {"network_id": 7, "expected_record_revision": 1, "pricing_refs": None, "benefit_refs": [], **changes}


def _command(**changes):
    command = store.RegistryRecordCommand(
        "network",
        7,
        "correct",
        1,
        {"display_name": "Example", "aliases": [], "catalog_evidence_json": _evidence()},
        "Reviewed exact references",
        uuid4().hex,
    )
    return replace(command, **changes)


@pytest.fixture
def codec_seam(monkeypatch):
    """Observe input encoding only; this does not implement native validation."""
    inputs = []

    def parse(encoded):
        inputs.append(encoded)
        return encoded

    monkeypatch.setattr(store, "_fast_module", lambda: SimpleNamespace(parse_registry_network_evidence=parse))
    return inputs


def test_optional_field_is_not_required_and_null_remains_unresolved(monkeypatch):
    monkeypatch.setattr(store, "_fast_module", lambda: pytest.fail("Null/omission must not invoke native syntax"))
    omitted = store._validated_command(_command(fields={"display_name": "Example", "aliases": []}))
    assert "catalog_evidence_json" not in omitted
    cleared = store._validated_command(
        _command(fields={"display_name": "Example", "aliases": [], "catalog_evidence_json": None})
    )
    assert cleared["catalog_evidence_json"] is None
    assert store._RECORD_MODELS["network"][2] == {"display_name", "aliases"}


@pytest.mark.parametrize("evidence", [None, {}, _evidence()])
def test_create_refuses_any_evidence_before_native_lookup(monkeypatch, evidence):
    monkeypatch.setattr(store, "_fast_module", lambda: pytest.fail("Creation must refuse before native lookup"))
    command = _command(
        record_id=None,
        operation="create",
        expected_revision=0,
        allocation_key=uuid4(),
        fields={"display_name": "Example", "aliases": [], "catalog_evidence_json": evidence},
    )
    with pytest.raises(ValueError, match="create_evidence_forbidden"):
        store._validated_command(command)


@pytest.mark.parametrize("native", [None, SimpleNamespace()])
def test_supplied_object_requires_native_export(monkeypatch, native):
    monkeypatch.setattr(store, "_fast_module", lambda: native)
    with pytest.raises(store.RegistryAddressUnavailable, match="evidence_native_unavailable"):
        store._validated_command(_command())


@pytest.mark.parametrize("changes", [{"network_id": 8}, {"expected_record_revision": 2}])
def test_command_context_refuses_substitution(codec_seam, changes):
    with pytest.raises(ValueError, match="evidence_context_invalid"):
        store._validated_command(
            _command(fields={"display_name": "Example", "aliases": [], "catalog_evidence_json": _evidence(**changes)})
        )
    assert len(codec_seam) == 1


def test_compact_sorted_utf8_preserves_distinct_null_and_reviewed_none(codec_seam):
    fields = store._validated_command(_command())
    assert fields["catalog_evidence_json"] == _evidence()
    assert codec_seam == [json.dumps(_evidence(), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()]
    assert fields["catalog_evidence_json"]["pricing_refs"] is None
    assert fields["catalog_evidence_json"]["benefit_refs"] == []


def test_input_bound_before_native_call_and_utf8_accounting(codec_seam):
    evidence = _evidence(pricing_refs=[{"snapshot_id": "é" * 9000}])
    with pytest.raises(ValueError, match="evidence_invalid"):
        store._validated_catalog_evidence(evidence)
    assert codec_seam == []


def test_exact_input_bound_is_independent_of_jsonb_text_spacing(codec_seam):
    evidence = _evidence(pricing_refs=[{"snapshot_id": ""}])
    fixed = len(json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode())
    evidence["pricing_refs"][0]["snapshot_id"] = "x" * (16384 - fixed)
    assert len(json.dumps(evidence, separators=(",", ":"), ensure_ascii=False).encode()) == 16384
    assert len(json.dumps(evidence, ensure_ascii=False).encode()) > 16384
    assert store._validated_catalog_evidence(evidence) == evidence
    assert len(codec_seam[0]) == 16384
    evidence["pricing_refs"][0]["snapshot_id"] += "x"
    with pytest.raises(ValueError, match="evidence_invalid"):
        store._validated_catalog_evidence(evidence)
    assert len(codec_seam) == 1


@pytest.mark.parametrize("output", [b"[]", b"{}", "{}", b"x" * 16385])
def test_incompatible_native_output_refuses(monkeypatch, output):
    monkeypatch.setattr(
        store, "_fast_module", lambda: SimpleNamespace(parse_registry_network_evidence=lambda _: output)
    )
    with pytest.raises(ValueError, match="evidence_invalid"):
        store._validated_catalog_evidence(_evidence())


@pytest.mark.parametrize("value", [False, 1, "{}", [], {"pricing_refs": float("nan")}])
def test_invalid_input_refuses_before_native(value, monkeypatch):
    monkeypatch.setattr(store, "_fast_module", lambda: pytest.fail("Invalid input must not reach native"))
    with pytest.raises(ValueError, match="evidence_invalid"):
        store._validated_catalog_evidence(value)


def test_legacy_and_retained_authoring_context_preserve_snapshot_bytes():
    legacy_by_field = {"network_id": 7, "revision": 1}
    store._validate_network_evidence_snapshot(legacy_by_field)
    current_by_field = {"network_id": 7, "revision": 9, "catalog_evidence_json": _evidence()}
    before = copy.deepcopy(current_by_field)
    assert store._snapshot(current_by_field) == before
    assert current_by_field == before


@pytest.mark.parametrize(
    "changes",
    [
        {"network_id": 8},
        {"expected_record_revision": 0},
        {"expected_record_revision": True},
        {"expected_record_revision": 9},
    ],
)
def test_retained_context_refuses_invalid_or_current_authoring_revision(changes):
    with pytest.raises(ValueError, match="evidence_context_invalid"):
        store._validate_network_evidence_snapshot(
            {"network_id": 7, "revision": 9, "catalog_evidence_json": _evidence(**changes)}
        )


def test_undo_restores_historical_refs_and_rebinds_new_review(codec_seam):
    snapshot_by_field = {
        "network_id": 7,
        "display_name": "Historical",
        "aliases": [],
        "archived": False,
        "revision": 2,
        "catalog_evidence_json": _evidence(),
    }
    before = copy.deepcopy(snapshot_by_field)
    command = RegistryManualUndoCommand("network", 7, 5, 2, "Review historical references", "undo-evidence")
    prepared = _prepare_correction(
        command,
        {"record_json": snapshot_by_field, "request_sha256": "a" * 64, "custom_revision": 2, "current_revision": 5},
        "network_id",
    )
    restored = prepared.command.fields["catalog_evidence_json"]
    assert restored == _evidence(expected_record_revision=5)
    assert snapshot_by_field == before
    assert prepared.command.expected_revision == 5 and prepared.provenance.target_revision == 2


@pytest.mark.parametrize(
    "snapshot", [{"network_id": 7, "revision": 1}, {"network_id": 7, "revision": 1, "catalog_evidence_json": None}]
)
def test_undo_legacy_or_null_explicitly_restores_unresolved(snapshot):
    assert _network_undo_evidence(snapshot, RegistryManualUndoCommand("network", 7, 3, 1, "Review", "undo")) is None


def test_request_hash_distinguishes_omission_null_and_reviewed_none():
    actor_by_field = {"kind": "platform_admin", "user_id": str(uuid4()), "client_id": "example"}
    omitted = _command(fields={"display_name": "Example", "aliases": []})
    null = replace(omitted, fields={**omitted.fields, "catalog_evidence_json": None})
    reviewed = replace(omitted, fields={**omitted.fields, "catalog_evidence_json": _evidence(pricing_refs=[])})
    assert len({store._command_request(command, actor_by_field) for command in [omitted, null, reviewed]}) == 3


def test_model_and_permission_column_sets_include_optional_evidence():
    column = NetworkRegistryRecord.__table__.c.catalog_evidence_json
    assert column.nullable and column.type.none_as_null
    for columns in [_TABLES, _INSERT_COLUMNS, _UPDATE_COLUMNS]:
        assert "catalog_evidence_json" in columns["network_registry_record"]
    assert (
        "catalog_evidence_json" not in _UPDATE_COLUMNS["registry_approved_record"]
        if "registry_approved_record" in _UPDATE_COLUMNS
        else True
    )


def test_additive_migration_head_and_storage_bound():
    path = Path(__file__).parents[1] / "alembic/versions/20261009030000_network_catalog_evidence.py"
    spec = importlib.util.spec_from_file_location("network_evidence_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    assert migration.down_revision == "20261009020000_company_registry_assertions"
    ddl = "\n".join(migration._ddl("example_registry"))
    assert "ADD COLUMN catalog_evidence_json JSONB" in ddl and "<=32768" in ddl
    assert "UPDATE" not in ddl and "FUNCTION" not in ddl and "TRIGGER" not in ddl
    with pytest.raises(ValueError):
        migration._ddl('invalid"schema')
