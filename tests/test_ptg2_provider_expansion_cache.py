# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Request isolation contracts for sealed provider-expansion cache entries."""

import pytest

from api import ptg2_serving as serving


class _WeightedFallback:
    """Expose a stable string representation for fallback byte accounting."""

    def __str__(self) -> str:
        return "weighted-fallback"


def _selection() -> serving._ProviderExpansionSelection:
    rank_key = ("npi", "1000000001", "CPT", "00001", "FFS", "7")
    return serving._ProviderExpansionSelection(
        row_data=[
            {
                "provider_set_global_id_128": "01" * 16,
                "network_names": ["Original network"],
            }
        ],
        providers_by_set={
            "01" * 16: [
                {
                    "npi": 1000000001,
                    "provider_name": "Synthetic provider",
                    "address_payload": {"first_line": "1 Test Way"},
                    "taxonomy_codes": ["000000000X"],
                }
            ]
        },
        rank_by_key={rank_key: 0},
        exhausted=False,
    )


def _shared_provider_fanout_selection(
    provider_set_count: int,
) -> serving._ProviderExpansionSelection:
    shared_provider_by_field = {
        "npi": 1000000001,
        "provider_name": "Shared synthetic provider",
        "address_payload": {"first_line": "1 Test Way"},
    }
    return serving._ProviderExpansionSelection(
        row_data=[],
        providers_by_set={
            f"{provider_set_key:032x}": [shared_provider_by_field]
            for provider_set_key in range(1, provider_set_count + 1)
        },
        rank_by_key={},
        exhausted=True,
    )


def test_provider_expansion_cache_isolates_mutable_inputs_and_row_hydration():
    """Cache ownership survives caller mutation and per-request row hydration."""

    cache_key = (91, "synthetic-snapshot", 2, False, "sealed-signature")
    source_selection = _selection()
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        serving._cache_provider_expansion_selection(cache_key, source_selection)
        source_selection.row_data[0]["network_names"].append("Caller mutation")
        source_selection.providers_by_set["01" * 16][0]["provider_name"] = (
            "Caller mutation"
        )

        first_hit = serving._provider_expansion_selection_from_cache(cache_key)
        assert first_hit is not None
        first_hit.row_data[0]["network_names"] = ["Request-local hydration"]

        second_hit = serving._provider_expansion_selection_from_cache(cache_key)
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    assert second_hit is not None
    assert second_hit.row_data[0]["network_names"] == ("Original network",)
    assert (
        second_hit.providers_by_set["01" * 16][0]["provider_name"]
        == "Synthetic provider"
    )


def test_provider_expansion_cache_shares_only_deep_read_only_proofs():
    """Provider and rank payloads cannot contaminate a later cache hit."""

    cache_key = (92, "synthetic-snapshot", 2, False, "sealed-signature")
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        serving._cache_provider_expansion_selection(cache_key, _selection())
        cached_selection = serving._provider_expansion_selection_from_cache(
            cache_key
        )
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    assert cached_selection is not None
    provider = cached_selection.providers_by_set["01" * 16][0]
    rank_key = next(iter(cached_selection.rank_by_key))
    with pytest.raises(TypeError):
        provider["provider_name"] = "Mutated"
    with pytest.raises(AttributeError):
        provider["taxonomy_codes"].append("111111111X")
    with pytest.raises(TypeError):
        provider["address_payload"]["first_line"] = "2 Changed Way"
    with pytest.raises(TypeError):
        cached_selection.rank_by_key[rank_key] = 1


def test_provider_expansion_cache_evicts_by_retained_weight(monkeypatch):
    """Bound aggregate retained proof weight independently of entry count."""

    selection = _selection()
    retained_weight = serving._provider_expansion_selection_retained_weight(
        selection
    )
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_EXPANSION_SELECTION_CACHE_MAX_BYTES",
        retained_weight * 2 - 1,
    )
    first_key = (93, "snapshot-one", 2, False, "signature")
    second_key = (94, "snapshot-two", 2, False, "signature")
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        serving._cache_provider_expansion_selection(first_key, selection)
        serving._cache_provider_expansion_selection(second_key, selection)
        assert list(serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE) == [
            second_key
        ]
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()


def test_provider_expansion_cache_rejects_oversized_entry_before_freeze(
    monkeypatch,
):
    """Do not retain an entry that cannot fit the deterministic byte budget."""

    selection = _selection()
    retained_weight = serving._provider_expansion_selection_retained_weight(
        selection
    )
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_EXPANSION_SELECTION_CACHE_MAX_BYTES",
        retained_weight - 1,
    )
    monkeypatch.setattr(
        serving,
        "_freeze_provider_expansion_cache_value",
        lambda *_args: pytest.fail("oversized entry must be rejected before freeze"),
    )
    cache_key = (95, "oversized-snapshot", 2, False, "signature")
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        serving._cache_provider_expansion_selection(cache_key, selection)
        assert cache_key not in serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()


def test_provider_expansion_cache_preserves_shared_identity_across_fanout():
    """Freeze one shared provider once even when many set buckets reference it."""

    cache_key = (97, "fanout-snapshot", 2, False, "signature")
    selection = _shared_provider_fanout_selection(1_000)
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        serving._cache_provider_expansion_selection(cache_key, selection)
        cached_entry = serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE[cache_key]
        cached_providers = [
            providers[0]
            for providers in cached_entry.selection.providers_by_set.values()
        ]
        assert all(
            provider is cached_providers[0]
            for provider in cached_providers[1:]
        )
        assert cached_entry.retained_weight == (
            serving._provider_expansion_selection_retained_weight(
                cached_entry.selection
            )
        )
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()


def test_shared_provider_fanout_respects_oversized_non_retention(monkeypatch):
    """Apply the byte cap to one-provider-many-set cache physics."""

    selection = _shared_provider_fanout_selection(1_000)
    retained_weight = serving._provider_expansion_selection_retained_weight(
        selection
    )
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_EXPANSION_SELECTION_CACHE_MAX_BYTES",
        retained_weight - 1,
    )
    cache_key = (98, "fanout-oversized", 2, False, "signature")
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        serving._cache_provider_expansion_selection(cache_key, selection)
        assert cache_key not in serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()


def test_provider_expansion_cache_hit_does_not_remeasure_weight(monkeypatch):
    """Keep retained-weight accounting entirely off the request hit path."""

    cache_key = (96, "measured-snapshot", 2, False, "signature")
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        serving._cache_provider_expansion_selection(cache_key, _selection())
        monkeypatch.setattr(
            serving,
            "_provider_expansion_selection_retained_weight",
            lambda _selection: pytest.fail("cache hit must not measure weight"),
        )
        assert serving._provider_expansion_selection_from_cache(cache_key)
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()


def test_provider_expansion_cache_accounts_for_supported_leaf_containers():
    """Charge sets, bytes, and fallback values without double-counting aliases."""

    shared_bytes = b"sealed-proof"
    payload = ({"one", "two"}, frozenset({"three"}), shared_bytes, shared_bytes)

    retained_weight = serving._provider_expansion_cache_value_weight(
        payload,
        set(),
    )
    fallback_weight = serving._provider_expansion_cache_value_weight(
        _WeightedFallback(),
        set(),
    )

    assert retained_weight > len(shared_bytes)
    assert fallback_weight >= len("weighted-fallback")


def test_provider_expansion_cache_freezes_set_payloads_once():
    """Freeze set evidence as an immutable shared value with memoized identity."""

    mutable_evidence_set = {"one", "two"}
    frozen_by_identity = {}

    first = serving._freeze_provider_expansion_cache_value(
        mutable_evidence_set,
        frozen_by_identity,
    )
    second = serving._freeze_provider_expansion_cache_value(
        mutable_evidence_set,
        frozen_by_identity,
    )

    assert first == frozenset({"one", "two"})
    assert second is first


def test_provider_expansion_cache_rejects_frozen_graph_weight_growth(
    monkeypatch,
):
    """Recheck the actual frozen graph before retaining an underestimated entry."""

    cache_key = (99, "post-freeze-oversized", 2, False, "signature")
    weight_calls = iter((1, 2))
    monkeypatch.setattr(
        serving,
        "_PTG2_PROVIDER_EXPANSION_SELECTION_CACHE_MAX_BYTES",
        1,
    )
    monkeypatch.setattr(
        serving,
        "_provider_expansion_selection_retained_weight",
        lambda _selection: next(weight_calls),
    )
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        returned = serving._cache_provider_expansion_selection(
            cache_key,
            _selection(),
        )
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    assert cache_key not in serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE
    assert returned.row_data[0]["network_names"] == ["Original network"]


@pytest.mark.parametrize(
    ("raw_npi", "expected"),
    [(None, None), ("", None), ("1000000001", 1000000001)],
)
def test_provider_expansion_npi_normalizes_optional_values(raw_npi, expected):
    """Treat absent NPIs as providerless rows while accepting numeric strings."""

    assert serving._provider_expansion_row_npi({"npi": raw_npi}) == expected


def test_provider_expansion_npi_rejects_malformed_value():
    """Fail closed when a provider graph contains a nonnumeric NPI."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match="invalid NPI"):
        serving._provider_expansion_row_npi({"npi": "not-an-npi"})


def _providerless_rate_row() -> dict[str, object]:
    return {
        "provider_set_global_id_128": "02" * 16,
        "serving_content_hash_128": "03" * 16,
        "reported_code_system": "CPT",
        "reported_code": "00001",
        "negotiation_arrangement": "FFS",
        "source_key": 7,
    }


def test_ranked_materialization_rejects_missing_provider_set_identity():
    """Require every retained rate occurrence to name its provider set."""

    selection = serving._ProviderExpansionSelection(
        row_data=[{"reported_code": "00001"}],
        providers_by_set={},
        rank_by_key={},
        exhausted=True,
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="missing its provider-set identity",
    ):
        serving._ranked_provider_expansion_materialization(selection)


def test_ranked_materialization_handles_providerless_occurrences():
    """Retain only providerless occurrences that belong to the selected page."""

    rate_row = _providerless_rate_row()
    ranked_key = serving._provider_expansion_key(rate_row, npi=None)
    ranked_selection = serving._ProviderExpansionSelection(
        row_data=[rate_row],
        providers_by_set={},
        rank_by_key={ranked_key: 0},
        exhausted=True,
    )
    unranked_selection = serving._ProviderExpansionSelection(
        row_data=[rate_row],
        providers_by_set={},
        rank_by_key={},
        exhausted=True,
    )

    ranked = serving._ranked_provider_expansion_materialization(
        ranked_selection
    )
    unranked = serving._ranked_provider_expansion_materialization(
        unranked_selection
    )

    assert ranked.row_data == [rate_row]
    assert ranked.providers_for(rate_row) == ()
    assert unranked.row_data == []


def test_request_local_provider_fields_normalizes_non_object_address():
    """Keep the public address contract object-shaped for malformed evidence."""

    fields_by_name = serving._request_local_provider_fields(
        {"address_payload": ["not", "an", "object"]}
    )

    assert fields_by_name["address"] == {}


def test_ranked_materialization_prunes_fanout_but_keeps_each_price_occurrence():
    """Retain every rate for ranked providers without shaping unrelated pairs."""

    provider_set_id = "01" * 16
    ranked_npi = 1000000001
    ranked_key = ("npi", str(ranked_npi), "CPT", "00001", "FFS", "7")
    rate_rows = [
        {
            "provider_set_global_id_128": provider_set_id,
            "reported_code_system": "CPT",
            "reported_code": "00001",
            "negotiation_arrangement": "FFS",
            "source_key": source_key,
            "price_set_global_id_128": f"{price_key:032x}",
        }
        for source_key, price_key in ((7, 101), (7, 102), (8, 103))
    ]
    providers = [
        {"npi": ranked_npi, "provider_name": "Ranked provider"},
        *(
            {"npi": ranked_npi + offset, "provider_name": "Unranked provider"}
            for offset in range(1, 101)
        ),
    ]
    selection = serving._ProviderExpansionSelection(
        row_data=rate_rows,
        providers_by_set={provider_set_id: providers},
        rank_by_key={ranked_key: 0},
        exhausted=False,
    )

    materialization = serving._ranked_provider_expansion_materialization(
        selection
    )

    assert [
        rate_row["price_set_global_id_128"]
        for rate_row in materialization.row_data
    ] == [
        f"{101:032x}",
        f"{102:032x}",
    ]
    assert all(
        [
            provider["npi"]
            for provider in materialization.providers_for(rate_row)
        ]
        == [ranked_npi]
        for rate_row in materialization.row_data
    )
    assert len(selection.row_data) == 3
    assert len(selection.providers_by_set[provider_set_id]) == 101
