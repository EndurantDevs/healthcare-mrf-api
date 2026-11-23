# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve ordinary REST ownership across failed retained-only completion attempts."""

import asyncio
from importlib import import_module
from types import SimpleNamespace

from db.models import ProviderDirectoryAPIEndpoint
from db.plan_release_storage import storage_relation
from process import provider_directory_import_completion as completion
from process.import_completion_policy import validate_completion_request
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_storage import _json, _require, _require_rest_transition_checkpoints


def _checkpoint_candidate(directory, candidate, checkpoints):
    metadata = candidate["identity"]["metadata"]
    bases = {checkpoint["canonical_api_base"] for checkpoint in checkpoints}
    _require(
        len(bases) == 1
        and all(_json(checkpoint.get("source_ids")) == metadata["source_ids"] for checkpoint in checkpoints),
        "transition_checkpoint_changed",
    )
    base = next(iter(bases))
    _require(type(base) is str and directory._canonical_base(base) == base, "transition_checkpoint_changed")
    return SimpleNamespace(
        dataset_id=candidate["identity"]["dataset_id"],
        acquisition_root_run_id=candidate["identity"]["acquisition_root_run_id"],
        source_ids=tuple(metadata["source_ids"]),
        selected_resources=tuple(metadata["selected_resources"]),
        checkpoint_context=SimpleNamespace(canonical_api_base=base),
    )


async def _ordinary_checkpoint_owner(connection, directory, candidate, identity, run_evidence, relation):
    parent_id = identity["failed_run_id"]
    observed_ids = set()
    request = None
    for _ in range(64):
        _require(
            parent_id in run_evidence["ancestor_run_ids"] and parent_id not in observed_ids,
            "transition_completion_lineage_changed",
        )
        previous_row = await connection.fetchrow(
            "SELECT run_id,retry_of_run_id,status,finished_at,params,metrics FROM "
            + relation
            + " WHERE run_id=$1 AND engine='healthcare-mrf-api' AND importer='provider-directory-fhir' "
            "FOR SHARE NOWAIT",
            parent_id,
        )
        previous, params, metrics = completion._settled_attempt(directory, candidate, previous_row, parent_id)
        observed_ids.add(parent_id)
        if request is None:
            request = validate_completion_request(params.get("import_completion"))
            _require(
                request["candidate_id"] == candidate.dataset_id
                and request["acquisition_root_run_id"] == candidate.acquisition_root_run_id
                and request["retained_counts"].keys() == set(candidate.selected_resources),
                "transition_completion_lineage_changed",
            )
        if "import_completion" not in params:
            _require(previous["status"] == "failed", "transition_run_changed")
            return parent_id
        _require(not metrics.get("source_candidate_handoffs"), "transition_completion_lineage_changed")
        parent_id = completion._failed_completion_parent(previous, params, metrics, request)
    _require(False, "transition_completion_lineage_changed")


async def checkpoint_owner(connection, candidate_by_field, identity, checkpoints, run_evidence):
    """Resolve only a differing unanimous ordinary owner through exact no-fetch ancestors."""
    owners = {checkpoint["owner_run_id"] for checkpoint in checkpoints}
    if owners == {identity["failed_run_id"]}:
        return identity["failed_run_id"]
    _require(len(owners) == 1 and bool(checkpoints), "transition_checkpoint_changed")
    directory = import_module("process.provider_directory_fhir")
    candidate = _checkpoint_candidate(directory, candidate_by_field, checkpoints)
    catalog_schema = candidate_by_field["storage"]["catalog_schema"]
    base = await connection.fetchval(
        "SELECT canonical_api_base FROM "
        + storage_relation(catalog_schema, ProviderDirectoryAPIEndpoint.__tablename__)
        + " WHERE endpoint_id=$1 FOR SHARE NOWAIT",
        identity["endpoint_id"],
    )
    _require(base == candidate.checkpoint_context.canonical_api_base, "transition_checkpoint_changed")
    try:
        async with asyncio.timeout(10):
            owner = await _ordinary_checkpoint_owner(
                connection,
                directory,
                candidate,
                identity,
                run_evidence,
                storage_relation(catalog_schema, "import_run"),
            )
    except ValueError as error:
        raise GenericCandidateError(
            "provider_directory_generic_source_transition_completion_lineage_changed"
        ) from error
    _require(owners == {owner}, "transition_checkpoint_changed")
    return owner


async def require_rest(connection, candidate_by_field, identity, checkpoints, expected_counts, run_evidence):
    """Check ordinary REST observations using only the proven acquisition owner."""
    owner = await checkpoint_owner(connection, candidate_by_field, identity, checkpoints, run_evidence)
    _require_rest_transition_checkpoints(
        checkpoints, identity, expected_counts, run_evidence["status"], owner_run_id=owner
    )
    return [], []
