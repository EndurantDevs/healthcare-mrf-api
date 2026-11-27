# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Network list pages read the search projection only for the filters its indexes can answer."""

from types import SimpleNamespace

from api import network_compact_address, provider_list_sql

PROBE = "JOIN LATERAL (SELECT 1) AS provider_taxonomy_match ON TRUE"
GROUPED = "CROSS JOIN (SELECT ARRAY_AGG(code) AS codes FROM mrf.nucc_taxonomy) AS q " + PROBE
TYPE = "c.type IN ('primary', 'practice')"
CITY_CLAUSES = [TYPE, "city_name = :city", "state_name = :state"]


def _plan(monkeypatch, taxonomy_page, clauses, *, searches=("city",), **filters_by_name):
    relation = object()
    layout = SimpleNamespace(
        city_search=relation if "city" in searches else None,
        network_search=relation if "network" in searches else None,
    )
    manifest = SimpleNamespace(supports_location_lookup=True, compact_layout=layout)
    scope = SimpleNamespace(is_compact_network_selection=True, network_ids=(7,), manifest=manifest)
    monkeypatch.setattr(provider_list_sql, "scoped_address_relation_sql", lambda *_args, **_options: "indexed")
    monkeypatch.setattr(
        network_compact_address, "compact_city_page", lambda *parts: (None, f"CITY {parts[2]} {parts[3]}", "FALSE")
    )
    monkeypatch.setattr(
        network_compact_address,
        "compact_network_search_page",
        lambda *parts: ("search_keys AS (SELECT 1)", f"SEARCH {' AND '.join(parts[2])} {parts[3]}", "NOT small"),
    )
    return provider_list_sql._provider_plain_network_page(
        scope, filters_by_name, clauses, "mrf", 51, is_plain=True, taxonomy_page=taxonomy_page
    )


def test_city_page_keeps_taxonomy_probe(monkeypatch):
    keys, fallback, wrap_page, hydration = _plan(
        monkeypatch, (PROBE, None), CITY_CLAUSES, city="EXAMPLE CITY", state="AA"
    )

    assert (keys, fallback, hydration) == (None, "FALSE", "indexed as c")
    assert wrap_page("GENERIC") == f"(CITY {TYPE} {PROBE}) UNION ALL (GENERIC)"


def test_unsupported_taxonomy_page_stays_generic(monkeypatch):
    city_by_name = {"city": "EXAMPLE CITY", "state": "AA"}

    assert _plan(monkeypatch, (GROUPED, None), CITY_CLAUSES, **city_by_name) is None
    assert _plan(monkeypatch, (PROBE, None), CITY_CLAUSES, searches=(), **city_by_name) is None
    assert _plan(monkeypatch, (PROBE, None), [TYPE, "state_name = :state"], state="AA") is None


def test_small_selection_reads_network_search(monkeypatch):
    clauses = ["c.taxonomy_array && q.int_codes", TYPE]
    keys, fallback, wrap_page, hydration = _plan(
        monkeypatch, (GROUPED, "prior_keys AS (SELECT 2)"), clauses, searches=("network",)
    )

    # A plan chosen earlier keeps its key relations for selections too large to read whole.
    assert keys == "prior_keys AS (SELECT 2),\nsearch_keys AS (SELECT 1)" and fallback == "NOT small"
    assert wrap_page("GENERIC") == f"(SEARCH {' AND '.join(clauses)} {GROUPED}) UNION ALL (GENERIC)"
    assert hydration == f"indexed as c {GROUPED}"
    state_page = _plan(monkeypatch, (PROBE, None), [TYPE, "state_name = :state"], searches=("network",), state="AA")
    assert state_page[0] == "search_keys AS (SELECT 1)"


def test_other_filters_skip_network_search(monkeypatch):
    with_search_by_name = {"searches": ("network",)}

    assert (
        _plan(monkeypatch, (PROBE, None), [TYPE, "plans_network_array && :plan_network_array"], **with_search_by_name)
        is None
    )
    assert (
        _plan(monkeypatch, (PROBE, None), CITY_CLAUSES, city="EXAMPLE CITY", state="AA", **with_search_by_name) is None
    )
    assert _plan(monkeypatch, (PROBE, None), [TYPE, "zip5 = :zip"], zip_code="00001", **with_search_by_name) is None
    assert _plan(monkeypatch, (GROUPED, None), ["c.taxonomy_array && q.int_codes", TYPE]) is None
