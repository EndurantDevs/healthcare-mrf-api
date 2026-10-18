# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed profile inputs and pagination cannot outlive their authority."""

from copy import deepcopy
from dataclasses import replace

import pytest

from api.provider_profile_network_contract import bind_managed_profile_generation, parse_managed_profile_query
from process.florida_mqa_profile import STANDARD_CATEGORIES
from process.network_serving_read import PinnedNetworkServingManifest


def manifest():
    return PinnedNetworkServingManifest(
        9,
        "11111111-1111-4111-8111-111111111111",
        "network_candidate_" + "1" * 32,
        1,
        {"fhir": "a" * 64},
        4,
        "b" * 64,
        7,
    )


def test_managed_profile_defaults_reuse_existing_public_query_semantics():
    npi, query = parse_managed_profile_query({"npi": "1000000491"})
    assert npi == "1000000491"
    assert query.requested_categories == tuple(STANDARD_CATEGORIES)
    assert not query.include_evidence and not query.include_sensitive
    assert query.page_category is None and query.requested_generation_id is None
    assert (query.page_limit, query.page_offset) == (25, 0)


def test_managed_profile_normalized_wire_preserves_independent_profile_generation():
    npi, query = parse_managed_profile_query(
        {
            "npi": "1000000491",
            "include_evidence": "true",
            "include_sensitive": "false",
            "category": "network_participation",
            "limit": "50",
            "offset": "1000000",
            "generation_id": "c" * 64,
        }
    )
    assert npi == "1000000491" and query.include_evidence and not query.include_sensitive
    assert query.requested_categories == ("network_participation",)
    assert (query.page_limit, query.page_offset, query.requested_generation_id) == (50, 1000000, "c" * 64)


@pytest.mark.parametrize(
    "changes",
    [
        {"npi": "0000000000"},
        {"npi": 1000000491},
        {"include_evidence": True},
        {"include_evidence": "TRUE"},
        {"include_sensitive": None},
        {"network_generation": "9"},
        {"network_ids": "84"},
        {"limit": 25},
        {"limit": "025"},
        {"limit": "0", "category": "services"},
        {"offset": "-1", "category": "services"},
        {"offset": "1000001", "category": "services"},
        {"limit": "25"},
        {"generation_id": "c" * 64},
        {"category": "services", "generation_id": "C" * 64},
        {"category": "services", "categories": "identity"},
        {"categories": "identity,identity"},
        {"categories": "identity, languages"},
        {"categories": ""},
        {"category": "unknown"},
    ],
)
def test_managed_profile_rejects_untrusted_or_noncanonical_fields(changes):
    with pytest.raises(ValueError):
        parse_managed_profile_query({"npi": "1000000491"} | changes)


def test_profile_generation_changes_for_every_authority_and_snapshot_identity():
    profile_by_field = {"generation_id": "c" * 64, "categories": {"languages": {"items": ["English"]}}}
    original = deepcopy(profile_by_field)
    base = manifest()
    first = bind_managed_profile_generation(profile_by_field, base, "d" * 64)
    assert profile_by_field == original and first["categories"] == profile_by_field["categories"]
    assert first == bind_managed_profile_generation(profile_by_field, base, "d" * 64)
    identities = [
        bind_managed_profile_generation(profile_by_field, base, "e" * 64),
        bind_managed_profile_generation(profile_by_field, replace(base, generation_id=10), "d" * 64),
        bind_managed_profile_generation(profile_by_field, replace(base, approved_custom_revision=5), "d" * 64),
        bind_managed_profile_generation(
            profile_by_field, replace(base, source_generations={"fhir": "f" * 64}), "d" * 64
        ),
        bind_managed_profile_generation(profile_by_field, replace(base, manifest_sha256="f" * 64), "d" * 64),
        bind_managed_profile_generation(profile_by_field | {"generation_id": "f" * 64}, base, "d" * 64),
    ]
    assert len({first["generation_id"], *(profile_by_field["generation_id"] for profile_by_field in identities)}) == 7


def test_changed_managed_policy_cannot_reuse_old_category_pagination_generation():
    from api.endpoint.npi import _provider_profile_generation_error

    profile_by_field = {"generation_id": "c" * 64}
    before = bind_managed_profile_generation(profile_by_field, manifest(), "d" * 64)
    after = bind_managed_profile_generation(profile_by_field, manifest(), "e" * 64)
    assert _provider_profile_generation_error(before, before["generation_id"]) is None
    refusal = _provider_profile_generation_error(after, before["generation_id"])
    assert refusal.status == 409
    assert b"provider_profile_generation_changed" in refusal.body


@pytest.mark.parametrize(
    "profile,scope", [({}, "d" * 64), ({"generation_id": "raw"}, "d" * 64), ({"generation_id": "c" * 64}, "D" * 64)]
)
def test_profile_generation_rejects_missing_source_or_policy_identity(profile, scope):
    with pytest.raises(ValueError):
        bind_managed_profile_generation(profile, manifest(), scope)
