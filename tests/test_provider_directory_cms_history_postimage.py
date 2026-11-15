# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full protected fingerprint checks with native IO replaced; no custody authority is supplied."""

from copy import deepcopy

import pytest

from process import provider_directory_cms_history_postimage as execution


@pytest.mark.parametrize("fault", [None, "name", "validated", "kind", "extra", "missing"])
def test_only_independently_proved_history_constraint_is_projected(fault):
    constraint_by_field = dict(
        oid="901",
        conname="cms_history_bound",
        conrelid="100",
        contype="c",
        conbin="fixed tree",
        convalidated=True,
        conkey=[1],
    )
    inventory_by_kind = {
        "canonical": {
            "relations": [
                dict(
                    nspname="catalog",
                    relname="witness",
                    native={
                        "constraints": [[deepcopy(constraint_by_field), "CHECK (fixed)"], [{"oid": "902"}, "unchanged"]]
                    },
                )
            ]
        }
    }
    bounds_by_root = {"witness": {"relation": {"schema": "catalog", "name": "witness"}, "dataset_ids": ["one"]}}
    if fault in {"name", "validated", "kind"}:
        field = {"name": "conname", "validated": "convalidated", "kind": "contype"}[fault]
        changed = {"name": "another_check", "validated": False, "kind": "f"}[fault]
        inventory_by_kind["canonical"]["relations"][0]["native"]["constraints"][0][0][field] = changed
    elif fault == "extra":
        bounds_by_root["other"] = deepcopy(bounds_by_root["witness"])
    elif fault == "missing":
        inventory_by_kind["canonical"]["relations"] = []
    original = deepcopy(inventory_by_kind)
    if fault:
        with pytest.raises(ValueError, match="history_bound_changed"):
            execution._without_history_bounds(inventory_by_kind, bounds_by_root)
    else:
        projected_inventory = execution._without_history_bounds(inventory_by_kind, bounds_by_root)
        assert projected_inventory["canonical"]["relations"][0]["native"]["constraints"] == [
            [{"oid": "902"}, "unchanged"]
        ]
    assert inventory_by_kind == original
