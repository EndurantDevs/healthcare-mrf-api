# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose private CMS preparation; canonical publication remains separate."""

import hashlib
import re
from contextlib import asynccontextmanager
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process import cms_npd_source
from process import provider_directory_generic_source_storage as storage
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
from process.provider_directory_cms_prepared_derived import (
    copy_prepared_cms_derived,
    copy_prepared_cms_input_payloads,
    verify_prepared_cms_derived,
    verify_prepared_cms_input_payloads,
)
from process.provider_directory_cms_prepared_identity_stream import (
    _FAMILIES,
    _frozen_coordinates,
    stage_prepared_identities,
)
from process.provider_directory_cms_prepared_identity_validation import assert_prepared_cms_identities
from process.provider_directory_cms_prepared_relationships import (
    assert_prepared_cms_relationships,
    project_prepared_cms_relationships,
)
from process.provider_directory_cms_prepared_workspace import prepared_cms_workspace
from process.provider_directory_generic_candidate import COPY_MAX_BYTES

CONTRACT = "cms-prepared-private-output-evidence-v2"


async def _copy_native_content(connection, session, *, schema, receipt):
    """Read an ordered native commitment after the caller authenticates its table and snapshot."""
    keys = await connection.fetch(
        "SELECT a.attname::text AS name FROM pg_catalog.pg_index i "
        "CROSS JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ordinal) "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
        "WHERE i.indrelid=$1::oid AND i.indisprimary AND i.indisvalid AND i.indisready "
        "AND k.ordinal<=i.indnkeyatts AND NOT a.attisdropped ORDER BY k.ordinal",
        receipt["oid"],
    )
    if not keys:
        raise ValueError("cms_prepared_outputs_frozen_key_required")
    relation = storage_relation(schema, receipt["name"])
    order = ",".join(storage_identifier(key["name"]) for key in keys)
    digest, counts_by_unit = hashlib.sha256(), {"bytes": 0}

    async def _hash_chunk(chunk):
        if len(chunk) > COPY_MAX_BYTES:
            raise ValueError("cms_prepared_outputs_frozen_chunk_unbounded")
        digest.update(chunk)
        counts_by_unit["bytes"] += len(chunk)

    try:
        status = await connection.copy_from_query(
            f"SELECT * FROM ONLY {relation} ORDER BY {order}", output=_hash_chunk, format="binary"
        )
    except BaseException:
        await close_interrupted_copy(connection)
        await invalidate_copy_owner(session, connection)
        raise
    if type(status) is not str or re.fullmatch(r"COPY [0-9]+", status) is None:
        raise ValueError("cms_prepared_outputs_frozen_copy_incomplete")
    return {"rows": int(status.removeprefix("COPY ")), "bytes": counts_by_unit["bytes"], "sha256": digest.hexdigest()}


async def _copy_frozen_content(workspace, receipt):
    return await _copy_native_content(
        workspace.connection, workspace.session, schema=workspace.policy.schema, receipt=receipt
    )


async def _frozen_content(workspace, handoff):
    """Commit all stored values with the existing ordered native COPY pattern.

    This detects net drift during preparation, not provenance or write custody.
    The trusted caller still authenticates the complete frozen content on entry.
    """
    await workspace.verify_current()
    await _frozen_coordinates(workspace)
    names = [receipt["name"] for receipt in handoff.document["tables"].values()]
    layouts_by_name = await storage._capture(workspace.connection, workspace.policy, names)
    commitments_by_root = {}
    for root, receipt in sorted(handoff.document["tables"].items()):
        commitments_by_root[root] = await _copy_frozen_content(workspace, receipt)
    if layouts_by_name != await storage._capture(workspace.connection, workspace.policy, names):
        raise ValueError("cms_prepared_outputs_frozen_layout_changed")
    await _frozen_coordinates(workspace)
    await workspace.verify_current()
    return {
        "contract": "cms-frozen-native-binary-copy-v1",
        "handoff_sha256": handoff.document_sha256,
        "inventory": deepcopy(workspace._inputs["frozen_inventory"]),
        "layouts": layouts_by_name,
        "content": commitments_by_root,
    }


async def read_prepared_cms_outputs(workspace, handoff):
    """Recompute complete private evidence, including after retained reopen.

    The caller authenticates the expected evidence and supplies its content
    custody or snapshot. Native layout equality alone is not immutability.
    """
    frozen = await _frozen_content(workspace, handoff)
    input_payloads = await verify_prepared_cms_input_payloads(workspace, handoff)
    identities = await assert_prepared_cms_identities(workspace, handoff)
    derived = await verify_prepared_cms_derived(workspace, handoff)
    relationships = await assert_prepared_cms_relationships(workspace, handoff)
    witness = storage_relation(workspace.policy.schema, handoff.document["tables"][storage.WITNESS]["name"])
    counts_by_family = {
        count_by_field["resource_type"]: count_by_field["count"]
        for count_by_field in await workspace.connection.fetch(
            f"SELECT resource_type,count(*) AS count FROM ONLY {witness} "
            "WHERE dataset_id=$1 AND resource_type=ANY($2::text[]) GROUP BY resource_type",
            workspace.identity["dataset_id"],
            list(_FAMILIES),
        )
    }
    resource_counts_by_family = {family: counts_by_family.get(family, 0) for family in _FAMILIES}
    release = workspace.identity["metadata"]["source_release"]
    if any(
        resource_counts_by_family[family] != release["files"][filename]["distinct_count"]
        for filename, family in cms_npd_source.RESOURCE_FILES
        if family in _FAMILIES
    ):
        raise ValueError("cms_prepared_outputs_family_count_changed")
    await workspace.verify_current()
    return {
        "contract": CONTRACT,
        "state": "verified_private",
        "preparation_id": workspace.binding["preparation_id"],
        "admission_sha256": workspace.binding["admission_sha256"],
        "handoff_sha256": handoff.document_sha256,
        "tables": workspace.receipts,
        "resource_counts": resource_counts_by_family,
        "input_payload_parity": input_payloads,
        "identity_parity": identities,
        "derived_parity": derived,
        "relationship_parity": relationships,
        "frozen_content": frozen,
    }


@asynccontextmanager
async def prepare_private_cms_outputs(
    database, session, connection, *, source_binding, prepared_inputs, handoff, preparation_id, admission_sha256
):
    """Prepare and verify eleven outputs while keeping all eight inputs unchanged.

    The trusted caller supplies protected profile/claim/content provenance and
    owns the original publisher transaction. Yielded evidence is private output
    parity, never CMS write authority, a serving descriptor or a publication
    receipt. Body failure rolls back the complete workspace for frozen retry.
    """
    async with prepared_cms_workspace(
        database,
        session,
        connection,
        source_binding=source_binding,
        prepared_inputs=prepared_inputs,
        handoff=handoff,
        preparation_id=preparation_id,
        admission_sha256=admission_sha256,
    ) as workspace:
        frozen_content = await _frozen_content(workspace, handoff)
        copied_inputs = await copy_prepared_cms_input_payloads(workspace, handoff)
        staged_identities = await stage_prepared_identities(workspace, handoff)
        copied_derived = await copy_prepared_cms_derived(workspace, handoff)
        projected = await project_prepared_cms_relationships(workspace, handoff)
        await workspace.finish_indexes()
        input_payloads = await verify_prepared_cms_input_payloads(workspace, handoff)
        identities = await assert_prepared_cms_identities(workspace, handoff)
        derived = await verify_prepared_cms_derived(workspace, handoff)
        relationships = await assert_prepared_cms_relationships(workspace, handoff)
        if (
            copied_inputs["row_counts"] != input_payloads["row_counts"]
            or copied_derived["row_counts"] != derived["row_counts"]
            or projected["relationship_count"] != relationships["relationship_count"]
        ):
            raise ValueError("cms_prepared_outputs_incomplete")
        evidence_by_field = {
            "contract": CONTRACT,
            "state": "verified_private",
            "preparation_id": preparation_id,
            "admission_sha256": admission_sha256,
            "handoff_sha256": handoff.document_sha256,
            "tables": workspace.receipts,
            "resource_counts": staged_identities["resource_counts"],
            "input_payload_parity": input_payloads,
            "identity_parity": identities,
            "derived_parity": derived,
            "relationship_parity": relationships,
            "frozen_content": frozen_content,
        }
        yield workspace, deepcopy(evidence_by_field)
        if (
            frozen_content != await _frozen_content(workspace, handoff)
            or input_payloads != await verify_prepared_cms_input_payloads(workspace, handoff)
            or identities != await assert_prepared_cms_identities(workspace, handoff)
            or derived != await verify_prepared_cms_derived(workspace, handoff)
            or relationships != await assert_prepared_cms_relationships(workspace, handoff)
        ):
            raise ValueError("cms_prepared_outputs_content_changed")
