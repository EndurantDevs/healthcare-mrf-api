# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_network_traversal as walk

directory = importlib.import_module("process.provider_directory_fhir")


def source():
    seed = json.loads(
        (Path(__file__).resolve().parents[1] / "specs/provider_directory_supplemental_sources.json").read_text()
    )
    return directory._source_row_from_seed(next(row for row in seed if row["api_base"] == walk.API_BASE))


def organization(key, *, owner=None):
    organization_by_field = {"resourceType": "Organization", "id": key}
    if owner:
        organization_by_field.update(
            type=[{"coding": [{"code": "ntwk", "system": walk.NETWORK_SYSTEM}]}], partOf={"reference": owner}
        )
    return organization_by_field


def response(resources, *, total=None, next_url=None):
    bundle_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": affiliation_by_field} for affiliation_by_field in resources],
        "total": len(resources) if total is None else total,
    }
    if next_url:
        bundle_by_field["link"] = [{"relation": "next", "url": next_url}]
    return 200, bundle_by_field, None, 1


@pytest.mark.asyncio
async def test_native_network_traversal_discovers_and_reconciles_without_plan_ids(monkeypatch):
    src = source()
    base = walk.API_BASE
    owner_ref = walk.CONFIG["owner_reference"]
    owner = organization(owner_ref.split("/")[1])
    owner["name"] = src["org_name"]
    root = organization("network-1", owner=owner_ref)
    foreign = organization("foreign", owner="Organization/other")
    institution = organization("hospital")
    first_affiliation_by_field = {
        "resourceType": "OrganizationAffiliation",
        "id": "a1",
        "network": [{"reference": "Organization/network-1"}],
        "participatingOrganization": {"reference": base + "/Organization/hospital"},
    }
    second_affiliation_by_field = {**first_affiliation_by_field, "id": "a2"}
    cursor = base + "/OrganizationAffiliation?cursor=value%2B%2f"
    fetch = AsyncMock(
        side_effect=[
            response([owner]),
            response([root, foreign]),
            response([root]),
            response([first_affiliation_by_field], total=2, next_url=cursor),
            response([second_affiliation_by_field], total=2),
            response([institution, foreign]),
        ]
    )
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    owner_work, roots_work = walk.seed_work(src)
    owner_result = await walk.step(directory, src, owner_work, timeout=10, page_count=100)
    assert [entry["resource"] for entry in owner_result["entries"]] == [owner]
    root_result = await walk.step(directory, src, roots_work, timeout=10, page_count=100)
    assert root_result["total"] == 2 and len(root_result["frontier"]) == 1
    (work,) = root_result["frontier"]
    root_read = await walk.read_organizations(directory, src, [work["key"]], timeout=10)
    (net,) = root_read["frontier"]
    page = await walk.step(directory, src, net, timeout=10, page_count=100)
    assert page["memberships"] == [("a1", "Organization/network-1")]
    assert page["entries"] == [] and page["next_url"] == cursor
    last = await walk.step(directory, src, {**net, "url": page["next_url"]}, timeout=10, page_count=100)
    assert last["next_url"] is None and last["memberships"] == [("a2", "Organization/network-1")]
    assert fetch.call_args_list[4].args[1] == cursor
    canon = walk.affiliation_result(
        src, [{"resource": first_affiliation_by_field}, {"resource": second_affiliation_by_field}]
    )
    assert [frontier_work["key"] for frontier_work in canon["frontier"]] == ["hospital", "network-1"]
    org_batch = await walk.read_organizations(directory, src, ["hospital", "foreign", "missing"], timeout=10)
    assert org_batch["missing_ids"] == ["missing"] and [entry["resource"] for entry in org_batch["entries"]] == [
        institution
    ]
    assert not org_batch["frontier"]


@pytest.mark.asyncio
async def test_root_page_filters_foreign_codings_without_losing_query_accounting(monkeypatch):
    owner_ref = walk.CONFIG["owner_reference"]
    owned_network = organization("owned", owner=owner_ref)
    foreign_coding = organization("foreign-code", owner=owner_ref)
    foreign_coding["type"][0]["coding"][0]["system"] = "https://example.org/organization-types"
    text_only = organization("text-only", owner=owner_ref)
    text_only["type"] = [{"text": "Network"}]
    other_owner = organization("other-owner", owner="Organization/other")
    owned_network["type"][0]["coding"].insert(0, {"system": "https://example.org/types", "code": "ntwk"})
    fetch = AsyncMock(return_value=response([owned_network, foreign_coding, text_only, other_owner], total=4))
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    root_result = await walk.step(directory, source(), walk.seed_work(source())[1], timeout=5, page_count=100)
    assert root_result["ids"] == ["owned", "foreign-code", "text-only", "other-owner"]
    assert root_result["total"] == 4 and root_result["next_url"] is None
    assert root_result["frontier"] == [{"kind": "organization", "key": "owned", "url": None, "payload": {}}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad", ["Organization/..", "Organization/.", "Organization/a#x", "https://foreign.example/fhir/Organization/a"]
)
async def test_canonical_affiliation_rejects_unsafe_reference_before_following(bad):
    affiliation_by_field = {"resourceType": "OrganizationAffiliation", "id": "a", "network": [{"reference": bad}]}
    with pytest.raises(ValueError):
        walk.affiliation_result(source(), [{"resource": affiliation_by_field}])


@pytest.mark.asyncio
async def test_organization_results_cannot_hide_truncation_or_widen_identity(monkeypatch):
    src = source()
    for result in [
        response([], total=1),
        response([organization("intruder")]),
        response([organization("known")] * 2),
        (200, {"resourceType": "OperationOutcome"}, None, 0),
    ]:
        monkeypatch.setattr(directory, "_fetch_source_json", AsyncMock(return_value=result))
        with pytest.raises(ValueError):
            await walk.read_organizations(directory, src, ["known"], timeout=5)


@pytest.mark.asyncio
async def test_search_refuses_cross_collection_checkpoint_before_transport(monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    with pytest.raises(ValueError, match="untrusted_continuation"):
        await walk.step(
            directory,
            source(),
            {"kind": "network", "key": "n", "url": walk.API_BASE + "/Patient"},
            timeout=3,
            page_count=10,
        )
    fetch.assert_not_awaited()
