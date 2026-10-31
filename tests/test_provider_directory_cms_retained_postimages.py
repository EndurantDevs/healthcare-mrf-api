# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline linkage only; authenticated records and native recovery remain caller duties."""

from copy import deepcopy

import pytest
import pytest_asyncio

from process import provider_directory_cms_history_record as history_records
from process import provider_directory_cms_snapshot_readback as readback
from tests.test_provider_directory_cms_history_conversion import _request
from tests.test_provider_directory_cms_sealed_readback import case as case
from tests.test_provider_directory_cms_sealed_readback import retained as retained
from tests.test_provider_directory_cms_sealed_readback import sealed_case as sealed_case


def _attachment_inventory(tables):
    return {
        "storage": [dict(oid=entry["oid"], kind="r", filenode=entry["oid"] + 1) for entry in tables.values()],
        "objects": [],
        "dependencies": [],
        "shared_dependencies": [],
    }


@pytest_asyncio.fixture
async def postimage_data(sealed_case):
    state = sealed_case
    binding = state.case.storage
    request = _request(same_schema=True)
    request.update(
        database_oid=binding.database_oid,
        catalog_schema=binding.catalog_schema,
        catalog_schema_oid=binding.catalog_schema_oid,
        future_schema=binding.catalog_schema,
        future_schema_oid=binding.catalog_schema_oid,
        owner_oid=binding.namespace_owner_oid,
    )
    parents_by_root = {}
    for root, heap in request["heaps"].items():
        for identity in (heap["old"], heap["parent"]):
            identity.update(owner_oid=binding.namespace_owner_oid, schema_oid=binding.catalog_schema_oid)
        parent = heap["parent"]
        parent["oid"] = state.attached["parents"][root]
        parents_by_root[root] = dict(
            oid=parent["oid"],
            relname=parent["name"],
            relowner=parent["owner_oid"],
            native=dict(
                namespace=[parent["schema_oid"]], indexes=[{"class": [oid]} for oid in parent["indexes"].values()]
            ),
        )
    history_by_field = dict(
        transaction="1234",
        request=request,
        dataset_rows={root: dict.fromkeys(heap["dataset_ids"], 0) for root, heap in request["heaps"].items()},
        content={root: dict(rows=0, bytes=0, sha256="a" * 64) for root in request["heaps"]},
    )
    state.allocation = dict(
        contract=history_records.ALLOCATION_CONTRACT, history=history_by_field, parents=parents_by_root
    )
    storage_by_oid = {
        str(entry[kind]["oid"]): {"fixture_only": "opaque-storage-data"}
        for entry in request["heaps"].values()
        for kind in ("old", "parent")
    }
    state.postimages = dict(
        sealed=deepcopy(state.sealed),
        attached=deepcopy(state.attached),
        attachment_objects=_attachment_inventory(state.attached["tables"]),
        history_storage=storage_by_oid,
    )
    state.evidence = dict(
        contract="cms-npd-finalization-evidence-v1",
        workspace=deepcopy(state.binding),
        private={"tables": deepcopy(state.binding["tables"])},
        canonical={"payloads": {"snapshot_postimages": state.postimages}},
    )
    return state


def _extract(state):
    return readback.retained_cms_payload_postimages(state.request, state.evidence, allocation=state.allocation)


async def test_linked_payload_postimages_are_detached_and_issue_no_native_read(postimage_data):
    state = postimage_data
    before = deepcopy(state.evidence)
    calls = state.case.driver.fetch.await_count
    extracted = _extract(state)
    assert extracted == {
        "sealed": state.sealed,
        "attached": state.attached,
        "attachment_objects": state.postimages["attachment_objects"],
    }
    extracted["attached"]["tables"].clear()
    assert state.evidence == before and state.case.driver.fetch.await_count == calls
    state.attachment_read.assert_not_awaited()


@pytest.mark.parametrize("field", ["sealed", "attached", "attachment_objects", "history_storage", "missing_all"])
async def test_absent_postimages_never_fall_back_to_current_observations(postimage_data, field):
    state = postimage_data
    if field == "missing_all":
        del state.evidence["canonical"]["payloads"]["snapshot_postimages"]
    else:
        del state.postimages[field]
    with pytest.raises(ValueError, match="terminal_postimages_required"):
        _extract(state)
    state.attachment_read.assert_not_awaited()


@pytest.mark.parametrize("change", ["seal_owner", "parent", "workspace", "history_storage", "legacy_allocation"])
async def test_postimages_cannot_be_rebound_to_another_allocation_or_workspace(postimage_data, change):
    state = postimage_data
    if change == "seal_owner":
        state.postimages["sealed"]["owner_oid"] += 1
    elif change == "parent":
        root = next(iter(state.postimages["attached"]["parents"]))
        state.postimages["attached"]["parents"][root] = 9999
    elif change == "workspace":
        state.evidence["workspace"]["preparation_id"] = "different"
    elif change == "history_storage":
        state.postimages["history_storage"].pop(next(iter(state.postimages["history_storage"])))
    else:
        state.allocation["contract"] = "cms-prepared-snapshot-allocation-data-v3"
    with pytest.raises(
        ValueError,
        match="seal_required|terminal_parents_changed|terminal_evidence_required|terminal_history_storage_changed|history_allocation_required",
    ):
        _extract(state)
