# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic CMS identity copying; authority and the canonical finisher stay separate."""

import hashlib
import re

import asyncpg

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_identity_candidate as entities
from process import provider_directory_cms_prepared_identity_validation as validation
from process import provider_directory_resource_identity_candidate as resources
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
from process.provider_directory_generic_candidate import COPY_MAX_BYTES
from process.provider_directory_generic_source_policy import _savepoint


async def _mint_commitments(workspace, *, expected):
    """Stream UUID/time pairs; existing mint time wins over a binding timestamp."""
    commitments_by_root = {}
    for root, column, stage_root in (
        ("provider_directory_organization_identity", "organization_id", entities.CMS_IDENTITY_TABLES[0]),
        ("provider_directory_site_identity", "site_id", entities.CMS_IDENTITY_TABLES[0]),
        ("provider_directory_insurance_network_identity", "network_id", entities.CMS_IDENTITY_TABLES[2]),
    ):
        field = storage_identifier(column)
        stage = storage_relation(workspace.policy.schema, workspace.receipts[stage_root]["name"])
        canonical = storage_relation(workspace.catalog_schema, root)
        query = (
            f"SELECT DISTINCT candidate.{field},COALESCE(incumbent.created_at,candidate.created_at) AS created_at "
            f"FROM {stage} candidate LEFT JOIN {canonical} incumbent USING({field}) "
            f"WHERE candidate.{field} IS NOT NULL ORDER BY candidate.{field},created_at"
            if expected
            else f"SELECT incumbent.{field},incumbent.created_at FROM {canonical} incumbent "
            f"WHERE EXISTS(SELECT FROM {stage} candidate WHERE candidate.{field}=incumbent.{field}) "
            f"ORDER BY incumbent.{field},incumbent.created_at"
        )
        digest = hashlib.sha256()

        async def _consume(chunk):
            if len(chunk) > COPY_MAX_BYTES:
                raise ValueError("cms_prepared_identity_publication_mint_chunk_unbounded")
            digest.update(chunk)

        try:
            status = await workspace.connection.copy_from_query(query, output=_consume, format="binary")
        except BaseException:
            await close_interrupted_copy(workspace.connection)
            await invalidate_copy_owner(workspace.session, workspace.connection)
            raise
        if type(status) is not str or re.fullmatch(r"COPY [0-9]+", status) is None:
            raise ValueError("cms_prepared_identity_publication_mint_copy_incomplete")
        commitments_by_root[root] = {"rows": int(status.removeprefix("COPY ")), "sha256": digest.hexdigest()}
    return commitments_by_root


async def read_prepared_cms_identity_publication(workspace, handoff):
    """Read complete identity5 equality and actual relevant mint3 UUID/time values.

    The caller authenticates authority and holds its writer fence or pinned
    snapshot. Returned commitments are data evidence, not a publication receipt.
    """
    before = await validation.assert_prepared_cms_identities(workspace, handoff)
    relations_by_root = {
        root: storage_relation(workspace.policy.schema, workspace.receipts[root]["name"])
        for root in entities.CMS_IDENTITY_TABLES
    }
    await entities.verify_identity_replay(
        workspace.connection, relations_by_root, workspace.catalog_schema, complete=True
    )
    resource = storage_relation(workspace.policy.schema, workspace.receipts[resources.RESOURCE_IDENTITY_ROOT]["name"])
    await resources._global_replay(workspace.connection, workspace.catalog_schema, resource, complete=True)
    mints = await _mint_commitments(workspace, expected=False)
    if before != await validation.assert_prepared_cms_identities(workspace, handoff):
        raise ValueError("cms_prepared_identity_publication_private_content_changed")
    return {"identity_parity": before, "canonical_mints": mints}


async def publish_prepared_cms_identities(workspace, handoff):
    """Copy mint3/identity5 in one rollback boundary without changing private data.

    The trusted finisher must separately authenticate issued CMS authority,
    profile, claim and canonical writer/callable custody, holding its qualified
    fence through commit. A workspace and the returned parity report supply
    data checks only. This operation creates no serving descriptor or receipt.
    """
    await workspace.verify_current()
    connection, schema = workspace.connection, workspace.catalog_schema
    try:
        async with _savepoint(connection):
            before = await validation.assert_prepared_cms_identities(workspace, handoff)
            expected_mints = await _mint_commitments(workspace, expected=True)
            receipts = workspace.receipts
            relations_by_root = {
                root: storage_relation(workspace.policy.schema, receipts[root]["name"])
                for root in entities.CMS_IDENTITY_TABLES
            }
            resource = storage_relation(workspace.policy.schema, receipts[resources.RESOURCE_IDENTITY_ROOT]["name"])
            await entities.publish_identity_relations(connection, relations_by_root, schema)
            await validation._resource_replay_coordinates(connection, resource, schema)
            await resources.publish_resource_identity_relation(connection, resource, schema)
            await validation._resource_replay_coordinates(connection, resource, schema)
            if expected_mints != await _mint_commitments(workspace, expected=False):
                raise entities.CMSIdentityReprepareRequired("cms_prepared_identity_publication_mint_replay_conflict")
            if before != await validation.assert_prepared_cms_identities(workspace, handoff):
                raise ValueError("cms_prepared_identity_publication_private_content_changed")
            return before
    except (asyncpg.SerializationError, asyncpg.UniqueViolationError) as error:
        raise entities.CMSIdentityReprepareRequired(
            "cms_prepared_identity_publication_whole_candidate_reprepare_required"
        ) from error
    except entities.GenericCandidateError as error:
        if str(error).endswith("canonical_identity_reuse_forbidden"):
            raise entities.CMSIdentityReprepareRequired(
                "cms_prepared_identity_publication_whole_candidate_reprepare_required"
            ) from error
        raise
