# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure query/cursor regressions; scope spies establish no Source or Reader authority."""

import json
from types import SimpleNamespace

import pytest

from api.endpoint import npi
from tests.test_npi_near_snapshot import near_snapshot as near_snapshot


def _scope(network_ids=None, legacy_checksums=None, excluded_network_ids=None):
    return SimpleNamespace(
        manifest=SimpleNamespace(manifest_sha256="a" * 64),
        access_scope_sha256="b" * 64,
        network_ids=network_ids,
        legacy_checksums=legacy_checksums,
        excluded_network_ids=excluded_network_ids,
    )


def test_trusted_out_of_band_selectors_bind_near_cursor(monkeypatch):
    query_by_name = {"lat": "41", "long": "-87", "count_only": "0", "include_total": "1", "limit": "1"}
    scopes = []
    for selection in [None, (7,), (8,), (7, 8)]:
        monkeypatch.setattr(npi, "current_network_address_scope", lambda selection=selection: _scope(selection))
        scopes.append(npi._nearby_cursor_scope(query_by_name))
    assert len(set(scopes)) == 4


def test_legacy_subset_and_exclusion_are_cursor_bound(monkeypatch):
    values = []
    for scope in [_scope((7,), (19,)), _scope((7,), (20,)), _scope(None, None, (7,)), _scope(None, None, (8,))]:
        monkeypatch.setattr(npi, "current_network_address_scope", lambda scope=scope: scope)
        values.append(npi._nearby_cursor_scope({"lat": "41", "long": "-87"}))
    assert len(set(values)) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["Office", "OFFICE", " office "])
async def test_near_query_matches_lowered_native_name_expression(near_snapshot, name):
    request = SimpleNamespace(
        args={"lat": "41", "long": "-87", "q": name, "include_total": "1", "limit": "1"},
        ctx=SimpleNamespace(sa_session=None),
        app=SimpleNamespace(),
    )
    result = await npi.get_near_npi(request)
    assert json.loads(result.body)["total_count"] == 1
    assert near_snapshot.query_parameters
    assert all(parameters["q"] == "%office%" for parameters in near_snapshot.query_parameters)
