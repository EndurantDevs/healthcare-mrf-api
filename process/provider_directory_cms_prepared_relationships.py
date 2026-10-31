# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare complete CMS references in private outputs, without publication."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_npd_relationship as references
from process import provider_directory_cms_prepared_context as context
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace, _policy
from process.provider_directory_generic_candidate import COPY_MAX_ROWS
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_relationships_" + code)


async def _coordinates(workspace, handoff):
    _require(
        type(workspace) is CMSPreparedWorkspace and type(handoff) is FHIRSourceCandidateHandoff, "workspace_required"
    )
    receipts = await workspace.verify_current()
    binding, document = workspace.binding, handoff.document
    _require(
        binding["handoff_sha256"] == handoff.document_sha256 and workspace.identity == document["identity"],
        "handoff_changed",
    )
    observed = await context._observe(
        workspace.connection,
        workspace.source_binding,
        _policy(workspace._inputs["source_policy"]),
        workspace.policy,
        workspace._inputs["frozen_inventory"],
    )
    _require(observed == workspace._inputs["native_coordinates"], "frozen_coordinates_changed")
    relations_by_root = {
        root: storage_relation(workspace.policy.schema, document["tables"][root]["name"])
        for root in (storage.RESOURCE, storage.WITNESS)
    }
    relations_by_root[references._BINDING] = storage_relation(
        workspace.policy.schema, receipts[references._BINDING]["name"]
    )
    relations_by_root[storage.RELATIONSHIP] = storage_relation(
        workspace.policy.schema, receipts[storage.RELATIONSHIP]["name"]
    )
    return relations_by_root


async def _require_twins(workspace, relations_by_root):
    """Refuse a missing or disqualified witness instead of silently losing refs."""
    resource, witness = relations_by_root[storage.RESOURCE], relations_by_root[storage.WITNESS]
    identity = workspace.identity
    invalid = await workspace.connection.fetchval(
        f"""SELECT EXISTS(
      SELECT FROM {resource} r LEFT JOIN {witness} w USING(dataset_id,resource_type,resource_id)
      WHERE r.dataset_id=$1 AND (w.resource_id IS NULL OR w.source_id<>'cms-npd' OR w.release_id<>$2
        OR w.normalized_payload_hash IS DISTINCT FROM r.payload_hash))
      OR EXISTS(SELECT FROM {witness} w LEFT JOIN {resource} r USING(dataset_id,resource_type,resource_id)
        WHERE w.dataset_id=$1 AND r.resource_id IS NULL)""",
        identity["dataset_id"],
        identity["metadata"]["source_release"]["vector_sha256"],
    )
    _require(invalid is False, "frozen_witness_mismatch")


def _parameters(workspace, kind):
    identity = workspace.identity
    return {
        "dataset_id": identity["dataset_id"],
        "release_id": identity["metadata"]["source_release"]["vector_sha256"],
        "resource_type": kind,
    }


def _report(workspace, handoff, counts_by_family, state):
    binding = workspace.binding
    return {
        "contract": references.CONTRACT,
        "state": state,
        "dataset_id": workspace.identity["dataset_id"],
        "release_id": workspace.identity["metadata"]["source_release"]["vector_sha256"],
        "handoff_sha256": handoff.document_sha256,
        "preparation_id": binding["preparation_id"],
        "admission_sha256": binding["admission_sha256"],
        "family_counts": counts_by_family,
        "relationship_count": sum(counts_by_family.values()),
    }


@asynccontextmanager
async def _cursor(workspace, select_sql, parameters=None):
    """Close our exact native portal before later COPY/index DDL or scope exit."""
    portal = storage_identifier("cms_refs_" + uuid4().hex)
    await workspace.session.execute(text("DECLARE " + portal + " NO SCROLL CURSOR FOR " + select_sql), parameters or {})
    has_failed = False
    try:
        yield portal
    except BaseException:
        has_failed = True
        raise
    finally:
        try:
            await workspace.connection.execute("CLOSE " + portal)
        except Exception, asyncio.CancelledError:
            # Rollback closes a failed/canceled statement's portal; preserve its
            # original error instead of replacing it with a dead-portal error.
            if not has_failed:
                raise


async def _copy_family(workspace, projection, kind):
    """Copy one complete family with the original bounded native portal lifetime."""
    count = 0
    async with _cursor(workspace, projection, _parameters(workspace, kind)) as portal:
        while batch := await workspace.connection.fetch(f"FETCH FORWARD {COPY_MAX_ROWS} FROM {portal}"):
            count += await workspace.copy_rows(
                storage.RELATIONSHIP, references._COLUMNS, [tuple(relationship_row) for relationship_row in batch]
            )
    return count


async def _hash_relationship_rows(workspace, relationship_relation, digest):
    """Hash every row while closing the original bounded native portal."""
    query = (
        f"SELECT {','.join(references._COLUMNS)} FROM {relationship_relation} "
        'ORDER BY dataset_id COLLATE "C",resource_type COLLATE "C",resource_id COLLATE "C",'
        'reference_field COLLATE "C",parent_ordinal,reference_ordinal'
    )
    async with _cursor(workspace, query) as portal:
        while batch := await workspace.connection.fetch(f"FETCH FORWARD {COPY_MAX_ROWS} FROM {portal}"):
            for relationship_row in batch:
                digest.update((storage._encoded(tuple(relationship_row)) + "\n").encode())


async def project_prepared_cms_relationships(workspace, handoff):
    """COPY every explicit reference from exact frozen twins into an empty graph.

    SELECT streams stay on the original Publisher. Only workspace.copy_rows
    temporarily enters the Owner. The caller finishes all workspace indexes
    before assert_prepared_cms_relationships establishes complete row parity.
    """
    relations_by_root = await _coordinates(workspace, handoff)
    _require(not workspace.binding["indexed"], "already_indexed")
    projection = references._projection_sql_for(
        relations_by_root[storage.RESOURCE], relations_by_root[storage.WITNESS], relations_by_root[references._BINDING]
    )
    async with _savepoint(workspace.connection):
        _require(
            await workspace.connection.fetchval(f"SELECT EXISTS(SELECT FROM {relations_by_root[storage.RELATIONSHIP]})")
            is False,
            "graph_not_empty",
        )
        await _require_twins(workspace, relations_by_root)
        previous = await references._raised_work_mem(workspace.session)
        counts_by_family = {}
        with workspace.session.no_autoflush:
            for kind in references.FIELDS:
                counts_by_family[kind] = await _copy_family(workspace, projection, kind)
        await references._restore_work_mem(workspace.session, previous)
        _require(await _coordinates(workspace, handoff) == relations_by_root, "coordinates_changed")
    return _report(workspace, handoff, counts_by_family, "projected")


async def assert_prepared_cms_relationships(workspace, handoff):
    """Use the existing indexed complete-family comparisons and hash every row.

    This is a private projection proof, never a publication receipt or mutation
    fence. Any later identity or input changes require complete revalidation.
    """
    relations_by_root = await _coordinates(workspace, handoff)
    _require(workspace.binding["indexed"], "indexes_required")
    projection = references._projection_sql_for(
        relations_by_root[storage.RESOURCE], relations_by_root[storage.WITNESS], relations_by_root[references._BINDING]
    )
    async with _savepoint(workspace.connection):
        await _require_twins(workspace, relations_by_root)
        previous = await references._raised_work_mem(workspace.session)
        counts_by_family = {}
        with workspace.session.no_autoflush:
            for kind in references.FIELDS:
                counts_by_family[kind] = await references._family_complete_for(
                    workspace.session, projection, relations_by_root[storage.RELATIONSHIP], _parameters(workspace, kind)
                )
            count = await workspace.connection.fetchval(
                f"SELECT count(*) FROM {relations_by_root[storage.RELATIONSHIP]}"
            )
            _require(count == sum(counts_by_family.values()), "projection_incomplete")
            digest = hashlib.sha256()
            await _hash_relationship_rows(workspace, relations_by_root[storage.RELATIONSHIP], digest)
        await references._restore_work_mem(workspace.session, previous)
        _require(await _coordinates(workspace, handoff) == relations_by_root, "coordinates_changed")
    return {
        **_report(workspace, handoff, counts_by_family, "verified"),
        "content_sha256": digest.hexdigest(),
        "content_contract": "sha256-canonical-json-lines-v1",
    }
