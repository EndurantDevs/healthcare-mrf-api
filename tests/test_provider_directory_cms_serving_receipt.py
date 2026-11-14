# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed public payload shape for a native CMS serving receipt."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving_receipt as receipts


def _payload():
    pin_by_field = {key: key for key in receipts._PIN_FIELDS}
    pin_by_field["source_id"] = "cms-npd"
    return {
        "contract_version": 1,
        "predecessor_receipt_id": None,
        "expected_incumbent": None,
        "cms": {**pin_by_field, "release_id": "a" * 64, "proof_version": 2},
        "desired_datasets": [pin_by_field],
        "selection": {"proof_id": "a" * 64, "fingerprint": "b" * 64, "catalog_digest": "c" * 64},
        "profile": {
            key: ("published" if key == "status" else "publish" if key == "operation" else key)
            for key in receipts._PROFILE_FIELDS
        },
        "address": dict.fromkeys(receipts._AUTHORITY_FIELDS),
        "doctors": dict.fromkeys(receipts._AUTHORITY_FIELDS | {"importer_id"}),
        "alias_generation": 0,
        "overlay_oid": 1,
    }


def _archive_result():
    return {
        "target_oid": 42,
        "from_revision": 0,
        "to_revision": 2,
        "native_input_hash": "a" * 64,
        "delta_rows": 3,
        "delta_sha256": "b" * 64,
    }


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        (None, None),
        ("unbound", True),
        ("to_revision", 3),
        ("native_input_hash", "unbound"),
        ("delta_rows", True),
        ("target_oid", 2**32),
    ],
)
def test_archive_result_is_closed_and_bounded(field_name, replacement):
    payload = _payload()
    payload["archive"] = _archive_result()
    if field_name is None:
        assert receipts.validate_receipt_payload(payload) is payload
        return
    payload["archive"][field_name] = replacement
    with pytest.raises(ValueError, match="archive_result|fields_invalid"):
        receipts.validate_receipt_payload(payload)


def test_archive_history_carries_only_for_the_same_retained_cms_pin():
    previous = _payload()
    previous["archive"] = _archive_result()
    predecessor_by_field = {"receipt_id": "c" * 64, "payload": previous}
    execution = SimpleNamespace(
        attestation=SimpleNamespace(
            pairs=previous["desired_datasets"],
            proof_id="d" * 64,
            selection_fingerprint="e" * 64,
            catalog_digest="f" * 64,
        )
    )
    snapshot_by_field = {
        key: previous[key] for key in ("profile", "address", "doctors", "alias_generation", "overlay_oid")
    }
    result = publication._receipt_payload(execution, previous["cms"], predecessor_by_field, snapshot_by_field)
    assert result["archive"] == previous["archive"]
    with pytest.raises(RuntimeError, match="archive_result_required"):
        publication._receipt_payload(
            execution, {**previous["cms"], "release_id": "f" * 64}, predecessor_by_field, snapshot_by_field
        )


@pytest.mark.parametrize("mutation", ["extra", "pin", "duplicate"])
def test_receipt_shape_rejects_extra_or_ambiguous_fields(mutation):
    payload = _payload()
    if mutation == "extra":
        payload["unreviewed"] = True
    elif mutation == "pin":
        payload["desired_datasets"][0]["name"] = "unbound"
    elif mutation == "duplicate":
        payload["desired_datasets"] *= 2
    with pytest.raises(ValueError):
        receipts.validate_receipt_payload(payload)


def test_profile_selection_can_exclude_independently_retained_cms_source():
    payload = _payload()
    payload["desired_datasets"] = []
    assert receipts.validate_receipt_payload(payload)["cms"]["source_id"] == "cms-npd"


@pytest.mark.asyncio
async def test_receipt_append_requires_caller_transaction():
    session = SimpleNamespace(in_transaction=lambda: False, scalar=AsyncMock())
    with pytest.raises(ValueError, match="requires_transaction"):
        await receipts.append_serving_receipt(session, "mrf", _payload())
    session.scalar.assert_not_awaited()


def _published_payload():
    payload = _payload()
    origin_by_field = {
        "local_lineage_id": "550e8400-e29b-41d4-a716-446655440000",
        "local_generation": 1,
        "origin_lineage_id": "550e8400-e29b-41d4-a716-446655440000",
        "origin_generation": 1,
        "published_at": "2026-01-01T00:00:00.000000Z",
        "relation_oids": [1, 2, 3],
    }
    payload["address"] = dict(origin_by_field)
    payload["doctors"] = {**origin_by_field, "importer_id": "cms-doctors"}
    payload["profile"]["selection_proof_id"] = payload["selection"]["proof_id"]
    return payload


def _snapshot(payload):
    return {key: payload[key] for key in ("profile", "address", "doctors", "alias_generation", "overlay_oid")}


def test_portable_snapshot_ignores_legacy_fields_without_rewriting_metadata():
    payload = _published_payload()
    original = deepcopy(payload)
    normalized = receipts._portable_snapshot(_snapshot(payload))
    assert normalized["address"]["relation_oids"] is normalized["doctors"]["relation_oids"] is None
    assert normalized["profile"]["profile_target_oid"] is normalized["profile"]["evidence_target_oid"] is None
    assert normalized["overlay_oid"] is None
    assert normalized["address"]["origin_generation"] == 1
    assert payload == original


@pytest.mark.asyncio
async def test_named_capture_executes_ordinary_name_and_data_queries_only():
    session = SimpleNamespace(
        in_transaction=lambda: True, scalar=AsyncMock(side_effect=[True, _snapshot(_published_payload())])
    )
    captured = await receipts.capture_named_dependencies(session, "copied")
    assert captured["address"]["relation_oids"] is None
    statements = [str(call.args[0]) for call in session.scalar.await_args_list]
    assert "IS NOT NULL" in statements[0]
    assert all("::oid" not in statement and "cms_serving_native_" not in statement for statement in statements)
    assert "a.relation_oids" not in statements[1] and "p.profile_target_oid" not in statements[1]
    assert '"copied".entity_address_result_generation' in statements[1]


@pytest.mark.asyncio
async def test_named_capture_refuses_missing_logical_relations():
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=False))
    with pytest.raises(RuntimeError, match="named_relations_unavailable"):
        await receipts.capture_named_dependencies(session, "copied")
    assert session.scalar.await_count == 1


@pytest.mark.asyncio
async def test_named_receipt_reads_historical_metadata_unchanged(monkeypatch):
    payload = _published_payload()
    predecessor_by_field = {"receipt_id": "a" * 64, "payload": payload}
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: predecessor_by_field))
        )
    )
    monkeypatch.setattr(
        receipts, "capture_named_dependencies", AsyncMock(return_value=receipts._portable_snapshot(_snapshot(payload)))
    )
    monkeypatch.setattr(receipts, "_current_cms_pin_matches", AsyncMock(return_value=True))
    assert await receipts.read_named_current_receipt(session, "copied") == predecessor_by_field
    assert "cms_serving_current_receipt_matches" not in str(session.execute.await_args.args[0])
    assert predecessor_by_field["payload"]["address"]["relation_oids"] == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "data", "counter", "source"])
async def test_ordinary_append_keeps_predecessor_counter_and_data_checks(monkeypatch, failure):
    prior = _published_payload()
    payload = deepcopy(prior)
    payload.update(
        predecessor_receipt_id="a" * 64, expected_incumbent={key: prior["cms"][key] for key in receipts._PIN_FIELDS}
    )
    payload["address"].update(local_generation=2, origin_generation=2, relation_oids=None)
    if failure == "counter":
        payload["address"]["local_generation"] = 3
    if failure == "source":
        payload["cms"]["dataset_hash"] = "changed"
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(side_effect=[True, "b" * 64])
    )
    monkeypatch.setattr(
        receipts, "capture_named_dependencies", AsyncMock(return_value=receipts._portable_snapshot(_snapshot(payload)))
    )
    monkeypatch.setattr(receipts, "_ordinary_data_matches", AsyncMock(return_value=failure != "data"))
    predecessor_by_field = {"receipt_id": "a" * 64, "payload": prior}
    if failure:
        with pytest.raises(RuntimeError, match="ordinary_.*_mismatch"):
            await receipts.append_ordinary_serving_receipt(session, "copied", payload, predecessor_by_field)
        assert not any("INSERT INTO" in str(call.args[0]) for call in session.scalar.await_args_list)
    else:
        assert (
            await receipts.append_ordinary_serving_receipt(session, "copied", payload, predecessor_by_field) == "b" * 64
        )
