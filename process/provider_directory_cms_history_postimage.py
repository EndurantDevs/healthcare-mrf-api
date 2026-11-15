# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check fixed history additions against a genuine post-owner Source profile."""

from copy import deepcopy


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_postimage_" + code)


def _without_history_bounds(inventory, bounds):
    """Project fixed CHECKs only after the caller validates their named bounds."""
    projected, removed_names = deepcopy(inventory), set()
    for relation in projected["canonical"]["relations"]:
        bound = bounds.get(relation["relname"])
        if bound is None:
            continue
        schema = relation.get("nspname", relation.get("schema"))
        _require(bound["relation"] == {"schema": schema, "name": relation["relname"]}, "history_name_changed")
        constraints = []
        for native, definition in relation["native"]["constraints"]:
            if native.get("conname") != "cms_history_bound":
                constraints.append([native, definition])
                continue
            _require(
                relation["relname"] not in removed_names
                and native["contype"] == "c"
                and native["convalidated"] is True,
                "history_bound_changed",
            )
            removed_names.add(relation["relname"])
        relation["native"]["constraints"] = constraints
    _require(removed_names == set(bounds), "history_bound_changed")
    return projected
