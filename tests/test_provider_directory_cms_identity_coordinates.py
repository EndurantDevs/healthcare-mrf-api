# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private identity workspaces preserve accepted identities in the catalog."""

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from process import provider_directory_cms_identity_candidate as identity
from process import provider_directory_generic_source_policy as source
from process import provider_directory_insurance_network_batch as network
from process import provider_directory_resource_identity_candidate as resource
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.test_provider_directory_cms_identity_candidate import organizations, scope
from tests.test_provider_directory_resource_identity_candidate import scope as resource_scope


def _isolate(publisher, monkeypatch):
    publisher.policy.schema = "candidate_storage"
    publisher.storage_binding = FHIRSourceStorageBinding(
        database_oid=1,
        catalog_schema="accepted_catalog",
        catalog_schema_oid=2,
        storage_schema="candidate_storage",
        storage_schema_oid=3,
        owner_oid=4,
        namespace_owner_oid=5,
        enrollment_sha256="a" * 64,
    )
    monkeypatch.setattr(source, "bound_publisher", lambda: publisher)


@pytest.mark.asyncio
async def test_entity_reuses_catalog_identity_and_only_copies_private_rows(scope, monkeypatch):
    publisher, session, document, receipts = scope
    _isolate(publisher, monkeypatch)
    accepted_id, created = uuid4(), datetime(2026, 1, 1, tzinfo=timezone.utc)

    async def fetch(query, *arguments):
        publisher.connection.reads.append((query, arguments))
        if f'"accepted_catalog"."{identity.CMS_IDENTITY_TABLES[0]}"' in query:
            return [dict(resource_id="organization-0", organization_id=accepted_id, site_id=None, created_at=created)]
        return []

    monkeypatch.setattr(publisher.connection, "fetch", fetch)
    async with identity.bind_cms_identity_candidate(publisher, document, receipts):
        identity_by_resource = await identity.stage_entity_batch(
            session,
            "cms-npd",
            "a" * 64,
            "Organization",
            {"organization-0": ("c" * 64, organizations(1)[0])},
        )
    assert identity_by_resource == {"organization-0": accepted_id}
    assert {copy[1] for copy in publisher.connection.copies} == {"candidate_storage"}
    binding_by_field = dict(
        zip(
            identity.CMS_IDENTITY_COLUMNS[identity.CMS_IDENTITY_TABLES[0]],
            publisher.connection.copies[0][3][0],
            strict=True,
        )
    )
    assert binding_by_field["created_at"] == created and binding_by_field["organization_id"] == accepted_id
    assert '"candidate_storage"."provider_directory_endpoint_dataset"' in publisher.connection.reads[0][0]
    assert publisher.connection.statements == []


@pytest.mark.asyncio
async def test_identity_replay_and_mint_keep_catalog_targets_and_private_inputs(scope, monkeypatch):
    publisher, _, _, receipts = scope
    _isolate(publisher, monkeypatch)
    await identity._global_replay(publisher.connection, publisher.policy, receipts)
    for root, (query, _) in zip(identity.CMS_IDENTITY_TABLES, publisher.connection.reads, strict=True):
        assert f'FROM "candidate_storage"."{receipts[root]["name"]}" candidate' in query
        assert f'JOIN "accepted_catalog"."{root}" incumbent' in query
    await identity._publish_canonical_ids(
        publisher.connection,
        publisher.policy,
        identity._relations(publisher.policy, receipts),
        "provider_directory_organization_identity",
        "organization_id",
        identity.CMS_IDENTITY_TABLES[0],
    )
    assert (
        'INSERT INTO "accepted_catalog"."provider_directory_organization_identity"'
        in publisher.connection.statements[0]
    )
    assert not any('CREATE TABLE "accepted_catalog"' in query for query in publisher.connection.statements)


@pytest.mark.asyncio
async def test_resource_reduce_creates_only_private_storage_from_catalog_template(resource_scope, monkeypatch):
    publisher, _, document, receipt = resource_scope
    _isolate(publisher, monkeypatch)
    document["metadata"]["network_bindings"]["dataset_schema"] = "accepted_catalog"
    await resource.finish_resource_identity_stage(publisher, document, receipt)
    statements = publisher.connection.statements
    assert any(
        query.startswith(
            f'CREATE TABLE "candidate_storage"."{receipt["name"]}_u" '
            '(LIKE "accepted_catalog"."provider_directory_resource_identity"'
        )
        for query in statements
    )
    assert not any(query.startswith('CREATE TABLE "accepted_catalog"') for query in statements)
    assert any('LEFT JOIN "accepted_catalog"."provider_directory_resource_identity"' in query for query in statements)


def test_network_names_separate_private_observations_from_accepted_bindings(scope, monkeypatch):
    publisher, _, _, receipts = scope
    _isolate(publisher, monkeypatch)
    names = network._sql_names(SimpleNamespace(dialect=postgresql.dialect()), publisher, receipts)
    assert names["global_network_binding"] == '"accepted_catalog"."provider_directory_insurance_network_source_binding"'
    assert names["global_plan_evidence"] == '"accepted_catalog"."provider_directory_insurance_network_plan_evidence"'
    for key in ("entity_binding", "entity_evidence", "network_binding", "plan_evidence"):
        assert names[key].startswith('"candidate_storage"."pdgs_')


def test_changed_private_coordinates_and_unbound_policy_refuse_before_io(scope, monkeypatch):
    publisher, _, _, _ = scope
    _isolate(publisher, monkeypatch)
    with pytest.raises(identity.GenericCandidateError, match="publisher_changed"):
        identity._canonical_schema(SimpleNamespace(schema="candidate_storage"))
    publisher.policy.schema = "other_storage"
    with pytest.raises(ValueError, match="schema_changed"):
        identity._canonical_schema(publisher.policy)
    assert publisher.connection.reads == publisher.connection.statements == publisher.connection.copies == []
