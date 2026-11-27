# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A network city page keeps a per-provider taxonomy probe only on the indexed city projection."""

from types import SimpleNamespace

from api import network_compact_address, provider_list_sql

PROBE = "JOIN LATERAL (SELECT 1) AS provider_taxonomy_match ON TRUE"
CLAUSES = ["c.type = 'practice'", "city_name = :city", "state_name = :state"]


def _plan(monkeypatch, taxonomy_join, *, has_city_projection=True, clauses=CLAUSES):
    layout = SimpleNamespace(city_search=object() if has_city_projection else None)
    manifest = SimpleNamespace(supports_location_lookup=True, compact_layout=layout)
    scope = SimpleNamespace(is_compact_network_selection=True, network_ids=(7,), manifest=manifest)
    monkeypatch.setattr(provider_list_sql, "scoped_address_relation_sql", lambda *_args, **_options: "indexed")
    monkeypatch.setattr(
        network_compact_address, "compact_city_page", lambda *parts: (None, f"CITY {parts[2]} {parts[3]}", "FALSE")
    )
    filters_by_name = {"city": "EXAMPLE CITY", "state": "AA"}
    return provider_list_sql._provider_plain_network_page(
        scope, filters_by_name, clauses, "mrf", 51, is_plain=True, taxonomy_join=taxonomy_join
    )


def test_city_page_keeps_taxonomy_probe(monkeypatch):
    keys, fallback, wrap_page, hydration = _plan(monkeypatch, PROBE)

    assert (keys, fallback, hydration) == (None, "FALSE", "indexed as c")
    assert wrap_page("GENERIC") == f"(CITY {CLAUSES[0]} {PROBE}) UNION ALL (GENERIC)"


def test_unsupported_taxonomy_page_stays_generic(monkeypatch):
    grouped_codes = "CROSS JOIN (SELECT ARRAY_AGG(code) AS codes FROM mrf.nucc_taxonomy) AS q " + PROBE

    assert _plan(monkeypatch, grouped_codes) is None
    assert _plan(monkeypatch, PROBE, has_city_projection=False) is None
    assert _plan(monkeypatch, PROBE, clauses=CLAUSES[:1]) is None
