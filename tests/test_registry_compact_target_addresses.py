# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve flat SQL and append bounded compact coordinates without API selector limits."""

import re
from uuid import UUID

import pytest

from process import registry_required_target_coverage as coverage
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_compact_target_addresses import required_target_addresses


def _manifest(*, artifacts=2, compact=True, custom=True):
    manifest = PinnedNetworkServingManifest(7, str(UUID(int=7)), "serving", 1, {}, 0, "a" * 64, 10)
    if compact:
        layout = CompactNetworkLayout(
            CompactAddressRelation("serving", "network_slice_ref", 10),
            CompactAddressRelation("base", "entity_address_unified", 11),
            tuple(
                CompactEdgeArtifact(str(UUID(int=index)), "network_candidate_" + UUID(int=index).hex, 20 + index)
                for index in range(1, artifacts + 1)
            ),
            CompactAddressRelation("custom", "entity_address_unified", 12) if custom else None,
        )
        object.__setattr__(manifest, "_compact_layout", layout)
    return manifest


def test_flat_relation_and_complete_result_sql_remain_the_existing_golden():
    addresses, arguments = required_target_addresses(_manifest(compact=False), first_parameter=6)
    assert addresses == '"serving".entity_address_unified' and arguments == ()
    original_counts = coverage._COUNTS_SQL.replace("__ADDRESSES__", "__SERVING__.entity_address_unified")
    expected = original_counts.replace("__CONTROL__", '"control"').replace("__SERVING__", '"serving"')
    actual = coverage._sql(coverage._COUNTS_SQL, '"control"', '"serving"', addresses=addresses)
    assert actual == expected
    assert coverage._RESULT_SQL.replace("__COUNTS__", actual) == coverage._RESULT_SQL.replace("__COUNTS__", expected)
    assert set(map(int, re.findall(r"\$(\d+)", coverage._RESULT_SQL.replace("__COUNTS__", actual)))) == {1, 2, 3, 4, 5}


def test_compact_edges_drive_complete_native_rows_and_preserve_parameter_positions():
    relation, arguments = required_target_addresses(_manifest(), first_parameter=6)
    assert len(arguments) == 4
    assert arguments[:2] == tuple(str(UUID(int=index)) for index in (1, 2))
    assert arguments[2:] == tuple(UUID(int=index) for index in (1, 2))
    assert set(map(int, re.findall(r"\$(\d+)", relation))) == {6, 7, 8, 9}
    assert "JOIN networks selected ON selected.network_id=ref.network_id" in relation
    assert "array_agg(DISTINCT network_id ORDER BY network_id)" in relation
    assert "LEFT JOIN LATERAL" in relation and "OFFSET 0" in relation
    assert "address.location_key=projected.location_key" in relation
    assert "coalesce(address.location_key,projected.location_key)" in relation
    assert "coalesce(address.canonical_network_ids,projected.canonical_network_ids)" in relation
    assert 'address."entity_name"' in relation and 'address."npi"' in relation
    assert '"serving".entity_address_unified' not in relation
    assert "network_membership" not in relation
    assert "_canonical_network_ids" not in relation


def test_administrative_network_cte_does_not_inherit_the_api_hundred_selector_cap():
    relation, arguments = required_target_addresses(_manifest(), first_parameter=6)
    statement = "WITH networks AS (SELECT generate_series(1,501) AS network_id) SELECT * FROM " + relation
    assert "generate_series(1,501)" in statement and len(arguments) == 4
    assert "ANY(" not in relation and "LIMIT 100" not in relation


def test_empty_edges_produce_no_rows_or_extra_binds():
    relation, arguments = required_target_addresses(_manifest(artifacts=0, custom=False), first_parameter=6)
    assert arguments == ()
    assert "NULL::text AS location_key WHERE FALSE" in relation
    assert not re.findall(r"\$\d+", relation)


@pytest.mark.parametrize("position", [0, -1, True, None, 32768])
def test_native_parameter_positions_are_validated(position):
    with pytest.raises(ValueError):
        required_target_addresses(_manifest(), first_parameter=position)


def test_manifest_shape_and_total_parameter_bound_are_validated():
    with pytest.raises(ValueError):
        required_target_addresses(object(), first_parameter=6)
    with pytest.raises(ValueError):
        required_target_addresses(_manifest(), first_parameter=32767)
