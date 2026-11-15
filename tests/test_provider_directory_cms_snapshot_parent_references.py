# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained parent names remain portable without interpreting numeric identities."""

import pytest

from process import provider_directory_cms_snapshot_attach as snapshots


@pytest.mark.parametrize("fields", [("schema", "name"), ("nspname", "relname"), ("schema_name", "relation_name")])
def test_parent_names_accept_recorded_name_forms_and_ignore_physical_metadata(fields):
    recorded_by_root = {
        root: {fields[0]: "catalog", fields[1]: root, "oid": 10 + index, "relowner": 99}
        for index, root in enumerate(snapshots.ROOTS)
    }
    expected_by_root = {root: {"schema": "catalog", "name": root} for root in snapshots.ROOTS}
    assert snapshots._parent_references(recorded_by_root) == expected_by_root
    for row in recorded_by_root.values():
        row["oid"], row["relowner"] = 777, 888
    assert snapshots._parent_references(recorded_by_root) == expected_by_root


@pytest.mark.parametrize("failure", ["numeric", "missing_name", "conflicting_names", "unsafe_name"])
def test_parent_names_never_invent_a_mapping_from_physical_identity(failure):
    recorded_by_root = {root: {"schema": "catalog", "name": root} for root in snapshots.ROOTS}
    first = snapshots.ROOTS[0]
    if failure == "numeric":
        recorded_by_root[first] = 10
    elif failure == "missing_name":
        recorded_by_root[first] = {"oid": 10}
    elif failure == "conflicting_names":
        recorded_by_root[first].update(nspname="other_catalog", relname=first)
    else:
        recorded_by_root[first]["name"] = "unsafe.name"
    with pytest.raises(ValueError):
        snapshots._parent_references(recorded_by_root)
