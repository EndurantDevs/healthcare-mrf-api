# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded identity observations in the authenticated prepared CMS workspace."""

from datetime import datetime, timezone

from db.plan_release_storage import storage_relation
from process.provider_directory_cms_identity_candidate import (
    CMS_IDENTITY_COLUMNS,
    CMS_IDENTITY_TABLES,
    CMSIdentityReprepareRequired,
    _identity,
    build_entity_batch_rows,
    read_entity_observations,
)
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace
from process.provider_directory_entity_identity import _validated_entity_batch
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_resource_identity import source_resource_uuid
from process.provider_directory_resource_identity_candidate import RESOURCE_IDENTITY_COLUMNS, RESOURCE_IDENTITY_ROOT


async def _workspace(workspace):
    if not isinstance(workspace, CMSPreparedWorkspace):
        raise ValueError("cms_prepared_identity_workspace_required")
    receipts = await workspace.verify_current()
    release_id = _identity(workspace.identity)
    relations_by_root = {
        root: storage_relation(workspace.policy.schema, receipts[root]["name"])
        for root in (*CMS_IDENTITY_TABLES[:2], RESOURCE_IDENTITY_ROOT)
    }
    return release_id, relations_by_root


def _matching_timestamps(staged, published, column):
    for resource_id in staged.keys() & published.keys():
        if staged[resource_id][column] != published[resource_id][column]:
            raise CMSIdentityReprepareRequired("cms_prepared_identity_immutable_timestamp_conflict")


async def stage_prepared_entity_batch(workspace, *, resources):
    """Reuse opaque IDs and immutable release facts; return duplicate input order.

    Both private COPYs share a savepoint, so a failed evidence write cannot leave
    a new binding behind. Accepted rows are observed without being modified.
    """
    release_id, relations = await _workspace(workspace)
    resource_type, ordered_ids, payloads = _validated_entity_batch("cms-npd", release_id, resources)
    async with _savepoint(workspace.connection):
        observations = await read_entity_observations(
            workspace.connection,
            relations,
            workspace.catalog_schema,
            "cms-npd",
            resource_type,
            release_id,
            list(payloads),
        )
        ids, binding_rows, evidence_rows = build_entity_batch_rows(
            "cms-npd", release_id, resource_type, payloads, observations
        )
        _matching_timestamps(observations.staged_bindings, observations.published_bindings, "created_at")
        _matching_timestamps(observations.staged_evidence, observations.published_evidence, "observed_at")
        for root, rows in ((CMS_IDENTITY_TABLES[0], binding_rows), (CMS_IDENTITY_TABLES[1], evidence_rows)):
            if rows:
                await workspace.copy_rows(root, CMS_IDENTITY_COLUMNS[root], rows)
    return [ids[resource_id] for resource_id in ordered_ids]


async def stage_prepared_resource_identity_batch(workspace, *, resource_type, resource_ids):
    """Stage missing deterministic IDs with accepted timestamps and bounded reads.

    A canonical observation appearing after this batch still needs the final
    candidate replay check; these observations confer no publication authority.
    """
    if (
        type(resource_type) is not str
        or resource_type not in {"InsurancePlan", "PractitionerRole"}
        or not isinstance(resource_ids, list)
        or not resource_ids
        or len(resource_ids) > 1_000
    ):
        raise ValueError("provider_directory_resource_identity_batch_invalid")
    identities_by_resource_id = {
        resource_id: source_resource_uuid("cms-npd", resource_type, resource_id) for resource_id in resource_ids
    }
    _, relations = await _workspace(workspace)
    async with _savepoint(workspace.connection):
        observations = []
        for relation in (
            relations[RESOURCE_IDENTITY_ROOT],
            storage_relation(workspace.catalog_schema, RESOURCE_IDENTITY_ROOT),
        ):
            identity_rows = await workspace.connection.fetch(
                f"SELECT resource_id,entity_id,created_at FROM {relation} "
                "WHERE source_id=$1 AND resource_type=$2 AND resource_id=ANY($3::varchar[])",
                "cms-npd",
                resource_type,
                list(identities_by_resource_id),
            )
            if len(identity_rows) > len(identities_by_resource_id) or len(
                {identity_row["resource_id"] for identity_row in identity_rows}
            ) != len(identity_rows):
                raise CMSIdentityReprepareRequired("cms_prepared_identity_lookup_ambiguous")
            if any(
                identity_row["entity_id"] != identities_by_resource_id[identity_row["resource_id"]]
                for identity_row in identity_rows
            ):
                raise CMSIdentityReprepareRequired("provider_directory_resource_identity_immutable_replay_conflict")
            observations.append({identity_row["resource_id"]: identity_row for identity_row in identity_rows})
        staged, published = observations
        _matching_timestamps(staged, published, "created_at")
        now = datetime.now(timezone.utc)
        identity_rows = [
            (
                "cms-npd",
                resource_type,
                resource_id,
                entity_id,
                published[resource_id]["created_at"] if resource_id in published else now,
            )
            for resource_id, entity_id in identities_by_resource_id.items()
            if resource_id not in staged
        ]
        if identity_rows:
            await workspace.copy_rows(RESOURCE_IDENTITY_ROOT, RESOURCE_IDENTITY_COLUMNS, identity_rows)
    return [identities_by_resource_id[resource_id] for resource_id in resource_ids]
