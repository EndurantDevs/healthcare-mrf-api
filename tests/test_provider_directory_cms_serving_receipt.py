# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed public payload shape for a native CMS serving receipt."""

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
