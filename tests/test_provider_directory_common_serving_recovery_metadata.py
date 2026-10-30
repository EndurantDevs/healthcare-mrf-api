# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact metadata plans preserve history while independently restoring Profile selection."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from db.models import AddressAliasArtifactStateV1, ProviderDirectoryProfileServingGeneration
from process import provider_directory_cms_serving_receipt as common
from process import provider_directory_common_serving_recovery_metadata as metadata
from process.provider_directory_output_snapshot_catalog import decoded
from tests.test_provider_directory_common_serving_recovery_document import state


def baseline():
    value = state()
    value["metadata"]["profile"] = {
        column.name: "recorded" for column in ProviderDirectoryProfileServingGeneration.__table__.columns
    }
    value["metadata"]["profile"]["singleton_key"] = "global"
    value["metadata"]["overlay_alias"] = {
        "artifact_name": "provider_directory_address_overlay",
        "generation": 7,
        "updated_at": "2026-07-01T00:00:00+00:00",
    }
    return value


def test_metadata_plan_restores_every_real_head_field_and_original_alias_date():
    before = baseline()
    plans = metadata.metadata_restore_sql("unit", before)
    assert [plan.family for plan in plans] == ["profile", "overlay_alias"]
    assert all(plan.sql.startswith("UPDATE ") and "jsonb_populate_record" in plan.sql for plan in plans)
    for plan in plans:
        assert decoded(dict(plan.parameters)["restore_metadata"]) == before["metadata"][plan.family]
        assert "clock_timestamp" not in plan.sql
    assert len(ProviderDirectoryProfileServingGeneration.__table__.columns) > 20
    assert set(before["metadata"]["overlay_alias"]) == {
        column.name for column in AddressAliasArtifactStateV1.__table__.columns
    }


def test_metadata_plan_rejects_unknown_columns_instead_of_silently_dropping_history():
    before = baseline()
    before["metadata"]["profile"]["unexpected"] = "value"
    with pytest.raises(ValueError, match="metadata_changed"):
        metadata.metadata_restore_sql("unit", before)


def test_absent_original_head_and_alias_are_scoped_deletes_only():
    from tests.test_provider_directory_common_serving_inverse import initial_baseline_and_payload

    before, _payload = initial_baseline_and_payload()
    before["metadata"]["overlay_alias"] = None
    plans = metadata.metadata_restore_sql("unit", before)
    assert all(plan.sql.startswith("DELETE FROM ") for plan in plans)
    assert dict(plans[0].parameters) == {"restore_key": "global"}
    assert dict(plans[1].parameters) == {"restore_key": "provider_directory_address_overlay"}


def receipt_fixture():
    address_by_field = {key: 1 for key in common._AUTHORITY_FIELDS}
    address_by_field.update(
        local_lineage_id="lineage", local_generation=1, origin_lineage_id="lineage", origin_generation=1
    )
    profile_by_field = {key: "old" for key in common._PROFILE_FIELDS}
    pin_by_field = {key: "value" for key in common._PIN_FIELDS}
    pin_by_field["source_id"] = "cms-npd"
    receipt_by_field = dict(
        contract_version=1,
        predecessor_receipt_id=None,
        expected_incumbent=None,
        cms={**pin_by_field, "release_id": "release", "proof_version": 1},
        desired_datasets=[pin_by_field],
        selection=dict(proof_id="old", fingerprint="old", catalog_digest="old"),
        profile=profile_by_field,
        address=address_by_field,
        doctors={**address_by_field, "importer_id": "cms-doctors"},
        alias_generation=7,
        overlay_oid=90,
    )
    forward = deepcopy(receipt_by_field)
    forward["profile"]["generation_id"] = "new"
    forward["selection"]["proof_id"] = "new"
    forward["address"].update(local_generation=2, origin_generation=2)
    restored_by_field = {
        key: deepcopy(receipt_by_field[key])
        for key in ("address", "profile", "doctors", "alias_generation", "overlay_oid")
    }
    restored_by_field["address"].update(local_generation=3, origin_generation=3)
    return (
        {"common_receipt": {"receipt_id": "old-tip", "payload": receipt_by_field}},
        {"common_receipt": {"receipt_id": "forward-tip", "payload": forward}},
        restored_by_field,
    )


def test_combined_receipt_restores_old_profile_selection_and_advances_address():
    before, forward, restored = receipt_fixture()
    result = metadata.common_receipt_payload(before, forward, restored)
    assert result["profile"] == before["common_receipt"]["payload"]["profile"]
    assert result["selection"] == before["common_receipt"]["payload"]["selection"]
    assert result["address"]["local_generation"] == 3
    assert result["predecessor_receipt_id"] == "forward-tip"
    assert result["expected_incumbent"] == {
        key: forward["common_receipt"]["payload"]["cms"][key] for key in common._PIN_FIELDS
    }


@pytest.mark.parametrize("damage", ["cms", "profile", "address", "doctors", "alias"])
def test_combined_receipt_refuses_unrelated_drift_or_nonmonotonic_authority(damage):
    before, forward, restored = receipt_fixture()
    if damage == "cms":
        forward["common_receipt"]["payload"]["cms"]["dataset_id"] = "other"
    elif damage == "profile":
        restored["profile"]["generation_id"] = "wrong"
    elif damage == "address":
        restored["address"]["local_generation"] = 1
    elif damage == "doctors":
        restored["doctors"]["local_generation"] += 1
    else:
        restored["alias_generation"] += 1
    with pytest.raises(ValueError, match="metadata_changed"):
        metadata.common_receipt_payload(before, forward, restored)


@pytest.mark.asyncio
@pytest.mark.parametrize("hops", [1, 2])
async def test_existing_forward_receipt_chain_allows_exact_one_or_two_hops(hops):
    before, forward, _restored = receipt_fixture()
    first, last = forward["common_receipt"], before["common_receipt"]
    first["payload"]["predecessor_receipt_id"] = "old-tip" if hops == 1 else "profile-tip"
    intermediate_by_field = {"receipt_id": "profile-tip", "payload": deepcopy(first["payload"])}
    intermediate_by_field["payload"]["predecessor_receipt_id"] = "old-tip"
    entries = [first, *([intermediate_by_field] if hops == 2 else []), last]
    driver = AsyncMock()
    driver.fetch.return_value = [dict(**entry, depth=index, valid_hash=True) for index, entry in enumerate(entries)]
    await metadata.assert_common_ancestry(driver, "unit", before, forward)
    assert driver.fetch.await_args.args[1:] == ("forward-tip", "old-tip")


@pytest.mark.asyncio
async def test_receipt_ancestry_refuses_a_missing_baseline_or_changed_hash():
    before, forward, _restored = receipt_fixture()
    driver = AsyncMock()
    driver.fetch.return_value = [dict(**forward["common_receipt"], depth=0, valid_hash=True)]
    with pytest.raises(ValueError, match="metadata_changed"):
        await metadata.assert_common_ancestry(driver, "unit", before, forward)
    driver.fetch.return_value = [
        dict(**forward["common_receipt"], depth=0, valid_hash=True),
        dict(**before["common_receipt"], depth=1, valid_hash=False),
    ]
    with pytest.raises(ValueError, match="metadata_changed"):
        await metadata.assert_common_ancestry(driver, "unit", before, forward)
