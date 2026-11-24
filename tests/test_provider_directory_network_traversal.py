# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_network_first_store as graph
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
    owner_work, roots_work = walk.seed_work(src)[:2]
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
    assert fetch.await_count == 6


@pytest.mark.asyncio
async def test_qualified_name_search_finds_network_outside_brand_roots(monkeypatch):
    src = source()
    org = organization("independent", owner=walk.CONFIG["owner_reference"])
    org["name"] = "Independent member network"
    fetch = AsyncMock(return_value=response([org]))
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    additional_roots = walk.seed_work(src)[2:]
    assert [item["key"] for item in additional_roots] == list("ABCDEFGHIJLMNOPRSTUVWXY012345789")
    selected = next(item for item in additional_roots if item["key"] == "I")
    result = await walk.step(directory, src, selected, timeout=5, page_count=100)
    assert result["ids"] == ["independent"]
    assert result["frontier"] == [{"kind": "organization", "key": "independent", "url": None, "payload": {}}]
    assert "name=I&type=ntwk" in fetch.await_args.args[1]
    src["metadata_json"][walk.METADATA_KEY] = dict(walk.LEGACY_CONFIG)
    assert [item["kind"] for item in walk.seed_work(src)] == ["owner", "roots"]
    with pytest.raises(ValueError, match="unrecognized_query"):
        await walk.step(directory, src, selected, timeout=5, page_count=100)


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
@pytest.mark.parametrize("shape", ["overfull", "same_page_repeat", "continuation_repeat"])
async def test_ambiguous_organization_search_resolves_requested_ids_by_read(monkeypatch, shape):
    src = source()
    base = walk.API_BASE + "/Organization"
    cursor = base + "?cursor=value%2B%2f"
    first = organization("known", owner="Organization/other-one")
    second = organization("known", owner="Organization/other-two")
    canonical = organization("known", owner=walk.CONFIG["owner_reference"])
    if shape == "continuation_repeat":
        pages = [response([first], total=2, next_url=cursor), response([second], total=2)]
    else:
        total = 3 if shape == "overfull" else 2
        pages = [response([first, second], total=total, next_url=cursor)]
    missing_outcome_by_field = {
        "resourceType": "OperationOutcome",
        "issue": [{"severity": "error", "code": "not-found"}],
    }
    fetch = AsyncMock(side_effect=[*pages, (200, canonical, None, 1), (404, missing_outcome_by_field, None, 1)])
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    result = await walk.read_organizations(directory, src, ["known", "missing"], timeout=5)
    exact = base + "/known"
    assert result["entries"] == [{"resource": canonical, "fullUrl": exact, "_retrieved_from": exact}]
    assert result["ids"] == ["known"] and result["missing_ids"] == ["missing"] and result["total"] == 1
    assert [(item["kind"], item["key"]) for item in result["frontier"]] == [("network", "known")]
    expected = [cursor] if shape == "continuation_repeat" else []
    assert [call.args[1] for call in fetch.await_args_list[1:]] == [*expected, exact, base + "/missing"]


@pytest.mark.asyncio
@pytest.mark.parametrize("unexpected", ["other-id", "invalid/id"])
async def test_ambiguous_organization_search_rejects_invalid_and_foreign_ids(monkeypatch, unexpected):
    rows = [organization("known"), organization("known"), organization(unexpected)]
    fetch = AsyncMock(return_value=response(rows, total=3))
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    with pytest.raises(ValueError, match="invalid_id|unrequested_organization"):
        await walk.read_organizations(directory, source(), ["known", "second"], timeout=5)
    fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_read",
    [
        (200, organization("wrong-id"), None, 1),
        (400, {"resourceType": "OperationOutcome", "issue": [{"code": "processing"}]}, None, 1),
        (200, {"resourceType": "OperationOutcome", "issue": [{"code": "processing"}]}, None, 1),
        (404, {"resourceType": "OperationOutcome", "issue": "invalid"}, None, 1),
        (404, {"resourceType": "OperationOutcome", "issue": [{}]}, None, 1),
        (404, {"resourceType": "OperationOutcome", "issue": [{"severity": "error", "code": "processing"}]}, None, 1),
        (
            404,
            {
                "resourceType": "OperationOutcome",
                "issue": [
                    {"severity": "error", "code": "processing", "diagnostics": "Access denied"},
                ],
            },
            None,
            1,
        ),
        (None, None, "source_fetch_timeout", 1),
        (200, {**organization("missing"), "meta": {"tag": [{"code": "SUBSETTED"}]}}, None, 1),
    ],
)
async def test_ambiguous_organization_search_fails_closed_on_invalid_reads(monkeypatch, bad_read):
    canonical = organization("known")
    pages = [response([canonical, canonical], total=2), (200, canonical, None, 1), bad_read]
    fetch = AsyncMock(side_effect=pages)
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    with pytest.raises(ValueError, match="network_snapshot_(invalid|subsetted)_organization_read"):
        await walk.read_organizations(directory, source(), ["known", "missing"], timeout=5)
    assert fetch.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "search_state", ["exact_zero", "continuation", "match", "unknown_total", "boolean_total", "wrong_id", "short_id"]
)
async def test_excellus_missing_diagnostic_requires_closed_zero_exact_search(monkeypatch, search_state):
    key = "5678" if search_state == "short_id" else "12-34-5678"
    base = walk.API_BASE + "/Organization"
    diagnostic_id = "5679" if search_state == "wrong_id" else key[-4:]
    missing_outcome_by_field = {
        "resourceType": "OperationOutcome",
        "issue": [
            {"severity": "error", "code": "processing", "diagnostics": f"No Networks found using {diagnostic_id}"}
        ],
    }
    has_match = search_state == "match"
    followup = response(
        [organization(key)] if has_match else [],
        total=int(has_match),
        next_url=base + "?cursor=more" if search_state == "continuation" else None,
    )
    if search_state == "unknown_total":
        followup[1].pop("total")
    if search_state == "boolean_total":
        followup[1]["total"] = False
    fetch = AsyncMock(side_effect=[response([], total=2), (404, missing_outcome_by_field, None, 1), followup])
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    if search_state == "exact_zero":
        lookup_by_field = await walk.read_organizations(directory, source(), [key], timeout=5)
        assert lookup_by_field["missing_ids"] == [key] and not lookup_by_field["entries"]
    else:
        with pytest.raises(ValueError):
            await walk.read_organizations(directory, source(), [key], timeout=5)
    urls = [call.args[1] for call in fetch.await_args_list]
    assert urls[1:] == [f"{base}/{key}"] + (
        [] if search_state in {"wrong_id", "short_id"} else [f"{base}?_id={key}&_count=1"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["rest_bundle", "rest_read"])
async def test_organization_provenance_depends_on_retrieval_url(mode):
    source_by_field = {**source(), "_endpoint_dataset_id": "candidate"}
    model = object()
    resource = organization("known")
    exact = walk.API_BASE + "/Organization/known"
    fetch_url = exact if mode == "rest_read" else walk.API_BASE + "/Organization?_id=known"
    parse = Mock(return_value=(model, resource))
    parser = SimpleNamespace(
        parse_fhir_resource=parse,
        RESOURCE_MODELS_BY_TYPE={"Organization": model},
        _rest_bundle_acquisition=directory._rest_bundle_acquisition,
    )
    entry_by_field = {"resource": resource, "fullUrl": exact, "_retrieved_from": fetch_url}
    persist = AsyncMock()
    await graph.GraphStore(parser, source_by_field)._persist_entries(
        {("Organization", "known"): entry_by_field},
        [{"resource_type": "Organization", "resource_id": "known"}],
        persist,
    )
    acquisition = parse.call_args.kwargs["acquisition"]
    assert (acquisition.fetch_mode, acquisition.self_url, acquisition.fetch_url) == (mode, exact, fetch_url)
    persist.assert_awaited_once_with(model, [resource])


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_or_wrong", [None, "missing", "different", "subsetted", "network", "institution"])
async def test_affiliation_verification_requires_a_full_exact_instance(monkeypatch, missing_or_wrong):
    src = source()
    resource_by_field = {
        "resourceType": "OrganizationAffiliation",
        "id": "different" if missing_or_wrong == "different" else "affiliation",
        "network": [{"reference": "Organization/other" if missing_or_wrong == "network" else "Organization/network"}],
        "participatingOrganization": {
            "reference": "Organization/other" if missing_or_wrong == "institution" else "Organization/facility",
        },
    }
    if missing_or_wrong == "subsetted":
        resource_by_field["meta"] = {"tag": [{"code": "SUBSETTED"}]}
    status = 404 if missing_or_wrong == "missing" else 200
    fetch = AsyncMock(return_value=(status, resource_by_field, None, 1))
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    item_by_field = {
        "kind": "verify_affiliation",
        "key": "affiliation",
        "url": None,
        "payload": {"network_refs": ["Organization/network"], "institution_ids": ["facility"]},
    }
    if missing_or_wrong:
        with pytest.raises(ValueError, match="network_snapshot_(?:invalid|subsetted|canonical)_affiliation_"):
            await walk.step(directory, src, item_by_field, timeout=5, page_count=100)
    else:
        result = await walk.step(directory, src, item_by_field, timeout=5, page_count=100)
        assert not result["entries"] and not result["frontier"] and result["next_url"] is None
    fetch.assert_awaited_once_with(src, walk.API_BASE + "/OrganizationAffiliation/affiliation", timeout=5)


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
