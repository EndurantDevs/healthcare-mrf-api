# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check fixed history additions against a genuine post-owner Source profile."""

import asyncio
import math
from copy import deepcopy

from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_preparation_policy as profiles


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_postimage_" + code)


def _without_history_bounds(inventory, bounds):
    """Project only CHECKs independently proved against the captured dataset commitment."""
    projected, removed = deepcopy(inventory), set()
    for relation in projected["canonical"]["relations"]:
        bound = bounds.get(relation["relname"])
        if bound is None:
            continue
        _require(relation["oid"] == bound["old_oid"], "history_identity_changed")
        proof = bound["native"]["constraint"]
        constraints = []
        for native, definition in relation["native"]["constraints"]:
            if int(native["oid"]) != proof["oid"]:
                constraints.append([native, definition])
                continue
            _require(
                relation["relname"] not in removed
                and native["conname"] == "cms_history_bound"
                and int(native["conrelid"]) == relation["oid"]
                and native["contype"] == "c"
                and native["conbin"] == proof["tree"]
                and native["convalidated"] is proof["validated"]
                and native["conkey"] == (proof["columns"] or None),
                "history_bound_changed",
            )
            removed.add(relation["relname"])
        relation["native"]["constraints"] = constraints
    _require(removed == set(bounds), "history_bound_changed")
    return projected


async def _staged_history_bounds(connection, inventory, binding, datasets, validated):
    """Read only the two fixed additions; never infer their expected dataset membership."""
    from process import provider_directory_cms_history_bounds as bounds
    from process import provider_directory_cms_history_conversion as history

    if datasets is None:
        return {}
    _require(type(datasets) is dict and set(datasets) == set(history.ROOTS), "history_datasets_required")
    roots_by_name = {
        native_bound["relname"]: native_bound
        for native_bound in inventory["canonical"]["relations"]
        if native_bound["relname"] in datasets
    }
    _require(set(roots_by_name) == set(datasets), "history_identity_changed")
    for dataset_ids in datasets.values():
        history._bounds(dataset_ids)
    native_bounds = await connection.fetch(
        bounds._READ_SQL, sorted((native_bound["oid"] for native_bound in roots_by_name.values()))
    )
    _require(len(native_bounds) == len(roots_by_name), "history_identity_changed")
    bounds_by_root = {}
    for native_bound in native_bounds:
        root = roots_by_name.get(native_bound["relname"])
        _require(
            root is not None
            and native_bound["relname"] not in bounds_by_root
            and (native_bound["oid"] == root["oid"])
            and (native_bound["nspname"] == binding.catalog_schema)
            and (native_bound["relkind"] == "r")
            and (native_bound["relowner"] == binding.namespace_owner_oid),
            "history_identity_changed",
        )
        bounds_by_root[native_bound["relname"]] = dict(
            old_oid=native_bound["oid"],
            dataset_ids=deepcopy(datasets[native_bound["relname"]]),
            native=bounds.require_cms_history_bound(
                native_bound["bound"], datasets[native_bound["relname"]], validated=validated
            ),
        )
    return bounds_by_root


async def require_cms_history_prework_postimage(
    connection,
    *,
    storage_binding,
    policy,
    cms_profile,
    deadline,
    history_dataset_ids=None,
    bounds_validated=True,
):
    """Authenticate only the two CHECK additions; all other protected fields remain exact.

    The caller loads the genuine Source1/profile1 after the completed owner cut
    and proves OriginalAdmin, callee/DDL/writer, reader and reverse-hook custody.
    This data comparison neither supplies those scopes nor issues a profile.
    Its finite read budget does not extend the caller's exclusive-lock clock.
    """
    _require(
        type(deadline) in {int, float} and math.isfinite(deadline) and deadline > asyncio.get_running_loop().time(),
        "deadline_required",
    )
    _require(connection.is_in_transaction() is True and not connection.is_closed(), "transaction_required")
    cms_profile, history_dataset_ids = deepcopy((cms_profile, history_dataset_ids))
    protected = profiles._profile(cms_profile, storage_binding, policy)
    async with asyncio.timeout_at(deadline):
        observed = await profiles._collect_profile(connection, storage_binding, policy, protected)
        bounds = await _staged_history_bounds(
            connection, observed, storage_binding, history_dataset_ids, bounds_validated
        )
        projected = _without_history_bounds(observed, bounds)
        _require(
            custody._evidence_digests(projected) == {field: protected[field] for field in custody._DIGEST_FIELDS},
            "protected_profile_changed",
        )
        return observed, bounds
