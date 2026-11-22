# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical affiliation identity and relationship boundary checks."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from process import provider_directory_affiliation_hydration as hydration
from process.provider_directory_fhir_response import strict_search_bundle_error

BASE = "https://directory.example/fhir"
SOURCE = {"api_base": BASE}


def entry(resource_id, networks=("Organization/n1",)):
    return {
        "resource": {
            "resourceType": "OrganizationAffiliation",
            "id": resource_id,
            "network": [{"reference": ref} for ref in networks],
        }
    }


def host(responses):
    return SimpleNamespace(
        _source_metadata=lambda source: source.get("metadata", {}),
        _fetch_source_json=AsyncMock(side_effect=[(200, payload, None, 0) for payload in responses]),
        _is_bundle_payload=lambda payload: payload.get("resourceType") == "Bundle",
        _resolved_fhir_next_url=lambda source, current, next_link, **kwargs: next_link,
        fhir_response=SimpleNamespace(
            strict_search_next_link=lambda payload: payload.get("next"),
            strict_search_bundle_error=strict_search_bundle_error,
        ),
    )


def bundle(entries, **kwargs):
    return {"resourceType": "Bundle", "type": "searchset", "entry": entries, **kwargs}


def test_bounded_batches_replace_projection_in_original_order():
    projected_entries = [entry(str(i)) for i in range(13)]
    canonical_entries = [entry(str(i), ("Organization/n1", "Organization/n2")) for i in range(13)]
    adapter = host([bundle(list(reversed(canonical_entries[:12]))), bundle(canonical_entries[12:])])
    result = asyncio.run(
        hydration.hydrate(
            adapter, SOURCE, {e["resource"]["id"]: {"Organization/n1"} for e in projected_entries}, timeout=30
        )
    )
    assert [r["resource"] for r in result] == [e["resource"] for e in canonical_entries]
    for call, expected in zip(adapter._fetch_source_json.call_args_list, (12, 1), strict=True):
        query = parse_qs(urlsplit(call.args[1]).query)
        assert len(query["_id"][0].split(",")) == expected
        assert "network" not in query
        assert call.kwargs == {"timeout": 30}


@pytest.mark.parametrize(
    "returned",
    [
        [],
        [entry("other")],
        [entry("a"), entry("a")],
        [entry("a", ("Organization/n2",))],
        [{"resource": {"resourceType": "OperationOutcome", "id": "a"}}],
    ],
)
def test_missing_malformed_conflicting_results_fail_closed(returned):
    adapter = host([bundle(returned)])
    with pytest.raises(ValueError, match="canonical_affiliation_"):
        asyncio.run(hydration.hydrate(adapter, SOURCE, {"a": {"Organization/n1"}}, timeout=30))


def test_opaque_continuation_is_followed_sequentially():
    next_url = BASE + "/OrganizationAffiliation?cursor=opaque%2Bvalue"
    adapter = host([bundle([entry("b")], next=next_url), bundle([entry("a")])])
    result = asyncio.run(
        hydration.hydrate(adapter, SOURCE, {"a": {"Organization/n1"}, "b": {"Organization/n1"}}, timeout=30)
    )
    assert [r["resource"]["id"] for r in result] == ["a", "b"]
    assert result[0]["_retrieved_from"] == next_url
    assert adapter._fetch_source_json.call_args_list[1].args[1] == next_url


def test_untrusted_continuation_never_fetches():
    adapter = host([bundle([entry("a")], next="https://other.example/OrganizationAffiliation?cursor=x")])
    with pytest.raises(ValueError, match="untrusted_continuation"):
        asyncio.run(hydration.hydrate(adapter, SOURCE, {"a": {"Organization/n1"}}, timeout=30))
    assert adapter._fetch_source_json.await_count == 1


def test_union_of_observed_networks_and_absolute_references():
    adapter = host([bundle([entry("a", (BASE + "/Organization/n1", "Organization/n2"))])])
    result = asyncio.run(
        hydration.hydrate(adapter, SOURCE, {"a": {"Organization/n1", BASE + "/Organization/n2"}}, timeout=30)
    )
    assert len(result) == 1
    adapter = host([bundle([entry("a")])])
    with pytest.raises(ValueError, match="network_conflict"):
        asyncio.run(hydration.hydrate(adapter, SOURCE, {"a": {"Organization/n1", "Organization/n2"}}, timeout=30))


def test_partial_canonical_resource_refused():
    item = entry("a")
    item["resource"]["meta"] = {"tag": [{"code": "SUBSETTED"}]}
    with pytest.raises(ValueError, match="subsetted"):
        asyncio.run(hydration.hydrate(host([bundle([item])]), SOURCE, {"a": {"Organization/n1"}}, timeout=30))
