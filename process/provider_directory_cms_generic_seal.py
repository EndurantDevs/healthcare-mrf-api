# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seal genuine CMS generic payloads during protected long prework.

This data primitive does not issue a profile, claim, native receipt or serving
authority. The caller holds its qualified private/canonical/callee writer
closure through the later history/ATTACH/accounting cut and outer commit.
"""

from datetime import timezone
from types import SimpleNamespace

from db.migration_cms_snapshot_allocator import _ADMIN_SQL
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_prepared_candidate as candidate
from process import provider_directory_cms_prepared_payload_publication as payloads
from process.network_cms_registry_address_capture import NativeReceiptSession
from process.provider_directory_cms_prepared_identity_publication import (
    publish_prepared_cms_identities,
    read_prepared_cms_identity_publication,
)
from process.provider_directory_cms_prepared_outputs import read_prepared_cms_outputs
from process.provider_directory_generic_source_policy import _savepoint


async def seal_allocated_cms_generic_candidate(workspace, handoff, *, allocation):
    """Copy/check the four native leaves, then honestly seal their noncurrent header.

    Full private semantics, identity publication, relationship-count receipt and
    binding coverage precede the new seal. Both unchanged generic anti-join
    transitions run in this long transaction. Two witness ATTACHes and complete
    canonical postimage parity remain the protected finisher's responsibility.
    """
    connection = workspace.connection
    candidate._require(await NativeReceiptSession(connection).scalar(_ADMIN_SQL) is True, "original_admin_required")
    actor_by_field = dict(await connection.fetchrow(custody._ACTOR_SQL))
    async with _savepoint(connection):
        private = await read_prepared_cms_outputs(workspace, handoff)
        await publish_prepared_cms_identities(workspace, handoff)
        identities = await read_prepared_cms_identity_publication(workspace, handoff)
        copied = await payloads.copy_allocated_cms_generic_payloads(workspace, handoff, allocation=allocation)
        frozen, expected = await candidate._inputs(workspace, handoff)
        canonical = storage_relation(workspace.catalog_schema, payloads.storage.HEADER)
        await candidate._relationship_receipt(workspace, private["relationship_parity"], create=True)
        changed = await connection.execute(
            f"UPDATE {canonical} c SET status='validated',validated_at=f.validated_at FROM {frozen} f "
            "WHERE c.dataset_id=f.dataset_id AND c.dataset_id=$1 AND c.status='acquiring' AND NOT c.is_current",
            workspace.identity["dataset_id"],
        )
        candidate._require(changed == "UPDATE 1", "header_seal_incomplete")
        coverage = await candidate.coverage.prepare_cms_candidate_coverage_in_session(
            workspace.session,
            SimpleNamespace(dataset_id=workspace.identity["dataset_id"], endpoint_id=workspace.identity["endpoint_id"]),
            workspace.identity["metadata"]["source_release"]["vector_sha256"],
            expected["dataset_hash"],
        )
        published_row = await connection.fetchrow(
            f"UPDATE {canonical} SET status='published',published_at=pg_catalog.statement_timestamp() AT TIME ZONE 'UTC' "
            "WHERE dataset_id=$1 AND status='validated' AND NOT is_current AND published_at IS NULL "
            "AND superseded_at IS NULL AND candidate_storage_json IS NULL RETURNING *",
            workspace.identity["dataset_id"],
        )
        candidate._require(published_row is not None, "header_seal_incomplete")
        header = await candidate._read_header(workspace, expected)
        candidate._require(header == candidate._header(published_row), "canonical_header_changed")
        tables = await payloads.allocations.read_cms_generic_copy_tables(workspace)
        await candidate.coverage.assert_sealed_cms_candidate_coverage(
            workspace.session, candidate.coverage._schema_name(), coverage
        )
        await workspace.verify_current()
        candidate._require(
            await NativeReceiptSession(connection).scalar(_ADMIN_SQL) is True
            and dict(await connection.fetchrow(custody._ACTOR_SQL)) == actor_by_field,
            "original_admin_changed",
        )
    return {
        "header": header,
        "private": private,
        "identities": identities,
        "copy": copied,
        "tables": tables,
        "coverage": coverage,
        "completed_at": header["published_at"].replace(tzinfo=timezone.utc),
    }
