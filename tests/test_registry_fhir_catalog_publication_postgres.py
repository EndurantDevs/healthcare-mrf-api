# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native atomic metadata publication with exact leases and standing authority."""

import hashlib
import json
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.network_registry import NetworkRegistryAlias
from db.models.registry_publication_request import RegistryPublicationRequest
from db.registry_fhir_catalog import PROJECTOR_VERSION, table_sql
from process import registry_fhir_catalog_fingerprints as fingerprints
from process import registry_fhir_catalog_publication as publication
from process import registry_publication_consumer as consumer
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_fhir_catalog_grant import (
    FHIRCatalogGrantError,
    FHIRCatalogStandingGrant,
    build_fhir_catalog_publication_runtime,
)
from process.registry_fhir_catalog_registry_reader import (
    fhir_catalog_registry_reads,
    verify_fhir_catalog_registry_reads,
)
from process.registry_fhir_catalog_validation import metadata_generation
from process.registry_publication_queue import (
    claim_registry_publication_request,
    finish_registry_publication_request,
    read_registry_publication_request,
)
from tests.test_registry_fhir_catalog_custody_postgres import expected_snapshot, freeze_metadata
from tests.test_registry_fhir_catalog_custody_postgres import metadata_custody as metadata_custody

GENERATION = "a" * 64
GRANT = "d" * 64
PENDING_BINDING = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
async def catalog_publication(metadata_custody, monkeypatch, request):
    fixture = metadata_custody
    attached = getattr(request, "param", True)
    namespace = '"' + fixture.control_schema + '"'
    await fixture.connection.execute(f"""CREATE TABLE {namespace}.registry_revision_control(id integer PRIMARY KEY,approved_revision bigint NOT NULL);
      INSERT INTO {namespace}.registry_revision_control VALUES(1,6);
      CREATE TABLE {namespace}.network_serving_control(id integer PRIMARY KEY,generation_id bigint,metadata_generation text);
      INSERT INTO {namespace}.network_serving_control VALUES(1,9,NULL)""")
    queue_table = RegistryPublicationRequest.__table__.to_metadata(MetaData(), schema=fixture.control_schema)
    await fixture.connection.execute(str(CreateTable(queue_table).compile(dialect=postgresql.dialect())))
    for statement in table_sql(namespace, "registry_fhir_catalog_snapshot"):
        await fixture.connection.execute(statement)
    selected = expected_snapshot(fixture)
    if not attached:
        await fixture.connection.execute(f"UPDATE {namespace}.network_serving_control SET generation_id=NULL")
        selected.update(network_generation=None, manifest_sha256=None)
        selected["metadata_generation"] = metadata_generation(selected)
    snapshot_by_field = await freeze_metadata(fixture, snapshot=selected)
    fixture.generation = snapshot_by_field["metadata_generation"]
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_snapshot "
        "(metadata_generation,schema_name,custody_json,network_generation,manifest_sha256,metadata_approved_revision,"
        "membership_mapping_sha256,mapping_sha256,standing_grant_sha256,source_vector_json,projector_version,accounting_json,as_of) "
        "VALUES($1,$2,$3::jsonb,$4,$5,6,$6,$7,$8,'[]'::jsonb,$9,$10::jsonb,$11)",
        fixture.generation,
        fixture.target.schema_name,
        json.dumps(snapshot_by_field["custody_json"]),
        snapshot_by_field["network_generation"],
        snapshot_by_field["manifest_sha256"],
        snapshot_by_field["membership_mapping_sha256"],
        snapshot_by_field["mapping_sha256"],
        GRANT,
        PROJECTOR_VERSION,
        json.dumps(snapshot_by_field["accounting_json"]),
        snapshot_by_field["as_of"],
    )
    monkeypatch.setattr(
        publication,
        "resolve_network_serving_manifest",
        AsyncMock(return_value=SimpleNamespace(manifest_sha256="f" * 64, approved_custom_revision=4)),
    )
    fixture.command = {
        "operation": publication.OPERATION,
        "metadata_generation": fixture.generation,
        "expected_metadata_generation": None,
        "expected_serving_generation": 9 if attached else None,
        "expected_approved_revision": 6,
        "standing_grant_sha256": GRANT,
    }
    yield fixture


async def _seed_pending_maps(fixture):
    namespace = '"' + fixture.control_schema + '"'
    alias_table = NetworkRegistryAlias.__table__.to_metadata(MetaData(), schema=fixture.control_schema)
    await fixture.connection.execute(str(CreateTable(alias_table).compile(dialect=postgresql.dialect())))
    await fixture.connection.execute(f"INSERT INTO {namespace}.network_registry_identity VALUES(101),(102)")
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.network_registry_alias "
        "(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) "
        "VALUES('fhir','sample-fhir','fhir_organization','network-pending','fhir-catalog/v1',101,'fixture')"
    )
    binding_by_field = {
        "binding_id": PENDING_BINDING,
        "binding_key": "b" * 64,
        "network_id": 101,
        "archived": False,
        "revision": 1,
        "source_system": "fhir",
        "source_id": "sample-fhir",
        "source_key": "network-pending",
        "producer_id": "registry-source-fhir-witness-v1",
        "source_scope_json": {"organization_id": "network-pending", "capture_sha256": "c" * 64},
    }
    await fixture.connection.copy_records_to_table(
        "registry_approved_record",
        schema_name=fixture.control_schema,
        columns=("approved_revision", "record_kind", "record_key", "record_revision", "custom_revision", "record_json"),
        records=[
            (
                7,
                "network",
                "101",
                1,
                6,
                json.dumps(
                    {
                        "network_id": 101,
                        "display_name": "Pending Network",
                        "aliases": [],
                        "archived": False,
                        "revision": 1,
                    }
                ),
            ),
            (7, "network_binding", PENDING_BINDING, 1, 6, json.dumps(binding_by_field)),
        ],
    )


@pytest.fixture
async def pending_catalog_publication(catalog_publication, monkeypatch):
    fixture = catalog_publication
    await _seed_pending_maps(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_revision_control SET approved_revision=7'
    )
    fixture.command["expected_approved_revision"] = 7
    # Native queue, SQL and custody are real; the serving-manifest resolver remains this fixture's boundary.
    manifest = SimpleNamespace(manifest_sha256="f" * 64, approved_custom_revision=6)
    monkeypatch.setattr(publication, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    monkeypatch.setattr(fingerprints, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    return fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("catalog_publication", [False], indirect=True)
@pytest.mark.parametrize("attach_during_publication", [False, True])
async def test_unattached_publication_preserves_directory_head(
    catalog_publication, monkeypatch, attach_during_publication
):
    fixture = catalog_publication
    receipt = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, receipt)
    if attach_during_publication:
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_control SET generation_id=10'
        )
        with pytest.raises(RegistryApprovalConflict):
            await _complete(fixture, receipt, lease_id)
        assert (await _state(fixture))["metadata_generation"] is None
    else:
        completed = await _complete(fixture, receipt, lease_id)
        assert completed["generation"] is None
        assert completed["memberships_reused"] is False and completed["addresses_reused"] is False
        state = await _state(fixture)
        assert state["generation_id"] is None and state["metadata_generation"] == fixture.generation
    publication.resolve_network_serving_manifest.assert_not_awaited()


async def _authorize(*, command, snapshot=None, connection=None):
    return {
        "authorized": True,
        "operation": publication.OPERATION,
        "command_sha256": hashlib.sha256(publication.publication_json(command).encode()).hexdigest(),
        "standing_grant_sha256": command["standing_grant_sha256"],
    }


@pytest.mark.asyncio
async def test_numeric_authorization_refuses():
    command_by_field = {
        "operation": publication.OPERATION,
        "metadata_generation": GENERATION,
        "expected_metadata_generation": None,
        "expected_serving_generation": 9,
        "expected_approved_revision": 6,
        "standing_grant_sha256": GRANT,
    }

    async def authorize(*, command, snapshot=None, connection=None):
        receipt_by_field = await _authorize(command=command)
        receipt_by_field["authorized"] = 1
        return receipt_by_field

    with pytest.raises(ValueError, match="authority_invalid"):
        await publication._authority(authorize, command_by_field, None, None)


async def _enqueue(fixture, authorize=_authorize):
    async with fhir_catalog_registry_reads(fixture.connection), fixture.connection.transaction():
        receipt = await publication.enqueue_fhir_catalog_publication(
            fixture.connection, fixture.command, authorize_metadata=authorize, control_schema=fixture.control_schema
        )
        await verify_fhir_catalog_registry_reads()
        return receipt


async def _claim_exact(fixture, receipt):
    """Isolate finalization guards from the separately tested family dispatcher."""
    lease_id = uuid4()
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_publication_request '
        "SET state='running',lease_id=$1,lease_expires_at=clock_timestamp()+interval '5 minutes' WHERE request_id=$2",
        lease_id,
        UUID(receipt["request_id"]),
    )
    return lease_id


async def _complete(fixture, receipt, lease_id, *, authorize=_authorize, verify_sources=None):
    return await publication.complete_queued_fhir_catalog_publication(
        fixture.connection,
        UUID(receipt["request_id"]),
        lease_id,
        authorize_metadata=authorize,
        verify_sources=verify_sources or AsyncMock(return_value=True),
        source_fence=_source_fence,
        control_schema=fixture.control_schema,
    )


@asynccontextmanager
async def _source_fence(*, snapshot, connection):
    """Source authority is isolated here; the real factory fence has native coverage."""
    yield


async def _state(fixture):
    return await fixture.connection.fetchrow(f"""SELECT serving.generation_id,serving.metadata_generation,
      snapshot.published_at,(SELECT state FROM "{fixture.control_schema}".registry_publication_request LIMIT 1) AS state
      FROM "{fixture.control_schema}".network_serving_control serving
      CROSS JOIN "{fixture.control_schema}".registry_fhir_catalog_snapshot snapshot WHERE serving.id=1""")


@pytest.mark.asyncio
async def test_service_sentinel_cannot_bypass_standing_grant_or_use_human_authority(catalog_publication, monkeypatch):
    fixture = catalog_publication
    receipt = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, receipt)
    request_record = await fixture.connection.fetchrow(
        f'SELECT * FROM "{fixture.control_schema}".registry_publication_request WHERE request_id=$1',
        UUID(receipt["request_id"]),
    )
    assert request_record["session_token_sha256"] == publication.NO_USER_SESSION
    assert json.loads(request_record["actor_json"])["kind"] == "catalog_source_service"
    grant = FHIRCatalogStandingGrant("different-standing-policy", 1, fixture.control_schema, ())
    runtime = build_fhir_catalog_publication_runtime(grant, None)
    human_authorize = AsyncMock(side_effect=AssertionError("human authorization was invoked"))
    verify_sources = AsyncMock()
    source_fence = AsyncMock(side_effect=AssertionError("source fence was invoked"))
    metadata_runtime = publication.FHIRCatalogPublicationRuntime(
        runtime.authorize_metadata, verify_sources, source_fence
    )

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield fixture.connection

    monkeypatch.setattr(consumer, "_verify_execution", AsyncMock())
    before = await _state(fixture)
    async with AsyncExitStack() as cleanup:
        with pytest.raises(FHIRCatalogGrantError, match="policy_changed"):
            await consumer._execute_publication_request(
                Pool(),
                SimpleNamespace(metadata_runtime=metadata_runtime, control_schema=fixture.control_schema),
                request_record,
                human_authorize,
                None,
                cleanup,
            )
    assert await _state(fixture) == before
    assert before["metadata_generation"] is None and before["published_at"] is None and before["state"] == "running"
    human_authorize.assert_not_awaited()
    verify_sources.assert_not_awaited()
    source_fence.assert_not_called()


@pytest.mark.asyncio
async def test_family_dispatch_claims_metadata(catalog_publication):
    fixture = catalog_publication
    receipt = await _enqueue(fixture)
    async with fixture.connection.transaction():
        assert (
            await claim_registry_publication_request(
                fixture.connection, operation_family="publication", control_schema=fixture.control_schema
            )
            is None
        )
        claimed = await claim_registry_publication_request(
            fixture.connection,
            operation_family="publication",
            control_schema=fixture.control_schema,
            allow_metadata=True,
        )
    assert claimed is not None and str(claimed["request_id"]) == receipt["request_id"]
    status = await read_registry_publication_request(
        fixture.connection, claimed["request_id"], control_schema=fixture.control_schema
    )
    assert status["operation"] == publication.OPERATION


@pytest.mark.asyncio
async def test_atomic_head_and_uncertain_replay(catalog_publication):
    fixture = catalog_publication
    receipt = await _enqueue(fixture)
    assert (
        await fixture.connection.fetchval(
            f'SELECT idempotency_key FROM "{fixture.control_schema}".registry_publication_request'
        )
        == "fhir-catalog:" + fixture.generation
    )
    lease_id = await _claim_exact(fixture, receipt)
    observer = fixture.reader
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"')
    await fixture.connection.execute(
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
    )
    async with observer.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await observer.fetchval(
                f'SELECT metadata_generation FROM "{fixture.control_schema}".network_serving_control'
            )
            is None
        )
        completion = await _complete(fixture, receipt, lease_id)
        assert (
            await observer.fetchval(
                f'SELECT metadata_generation FROM "{fixture.control_schema}".network_serving_control'
            )
            is None
        )
        assert (
            await observer.fetchval(f'SELECT state FROM "{fixture.control_schema}".registry_publication_request')
            == "running"
        )
    after = await _state(fixture)
    assert after["generation_id"] == 9 and after["metadata_generation"] == fixture.generation
    assert after["published_at"] is not None and after["state"] == "completed"
    assert completion["memberships_reused"] is True and completion["addresses_reused"] is True
    replay = await _enqueue(fixture)
    assert replay["request_id"] == receipt["request_id"] and replay["state"] == "completed" and replay["replayed"]
    assert (await _state(fixture))["published_at"] == after["published_at"]


@pytest.mark.asyncio
async def test_pending_additions_publish_with_sealed_serving_maps(pending_catalog_publication):
    fixture = pending_catalog_publication
    namespace = '"' + fixture.control_schema + '"'
    snapshot_before_by_field = dict(
        await fixture.connection.fetchrow(f"SELECT * FROM {namespace}.registry_fhir_catalog_snapshot")
    )
    async with fixture.connection.transaction():
        selected, mapping = await fingerprints.select_fhir_catalog_mapping(
            fixture.connection, 7, SimpleNamespace(approved_custom_revision=6), control_schema=fixture.control_schema
        )
    assert selected == snapshot_before_by_field["metadata_approved_revision"] == 6
    assert mapping == {
        "metadata": snapshot_before_by_field["mapping_sha256"],
        "membership": snapshot_before_by_field["membership_mapping_sha256"],
    }
    receipt = await _enqueue(fixture)
    request_record = await fixture.connection.fetchrow(f"SELECT * FROM {namespace}.registry_publication_request")
    assert request_record["idempotency_key"] == "fhir-catalog:" + fixture.generation + ":approved:7"
    assert publication.validated_fhir_catalog_request_status(request_record)["expected_approved_revision"] == 7
    completed = await _complete(fixture, receipt, await _claim_exact(fixture, receipt))
    assert completed["generation"] == "9"
    assert completed["memberships_reused"] is True and completed["addresses_reused"] is True
    snapshot_after_by_field = dict(
        await fixture.connection.fetchrow(f"SELECT * FROM {namespace}.registry_fhir_catalog_snapshot")
    )
    assert snapshot_after_by_field["published_at"] is not None
    snapshot_after_by_field["published_at"] = snapshot_before_by_field["published_at"]
    assert snapshot_after_by_field == snapshot_before_by_field
    state = await _state(fixture)
    assert state["generation_id"] == 9 and state["metadata_generation"] == fixture.generation
    assert (
        await fixture.connection.fetchval(f"SELECT approved_revision FROM {namespace}.registry_revision_control") == 7
    )
    replay = await _enqueue(fixture)
    assert replay["request_id"] == receipt["request_id"] and replay["replayed"] and replay["state"] == "completed"
    assert (
        await fixture.reader.fetchval(
            f'SELECT count(*) FROM "{fixture.target.schema_name}".registry_fhir_catalog_network'
        )
        == 0
    )


@pytest.mark.asyncio
async def test_changed_approved_head_requires_distinct_split_retry(pending_catalog_publication):
    fixture = pending_catalog_publication
    namespace = '"' + fixture.control_schema + '"'
    stale = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, stale)
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_approved_record "
        f"SELECT 8,record_kind,record_key,record_revision,custom_revision,record_json FROM {namespace}.registry_approved_record "
        "WHERE approved_revision=7"
    )
    await fixture.connection.execute(f"UPDATE {namespace}.registry_revision_control SET approved_revision=8")
    with pytest.raises(RegistryApprovalConflict, match="source_changed"):
        await _complete(fixture, stale, lease_id)
    assert (await _state(fixture))["metadata_generation"] is None
    async with fixture.connection.transaction():
        await finish_registry_publication_request(
            fixture.connection,
            UUID(stale["request_id"]),
            lease_id,
            state="rejected",
            result={"error": {"code": "registry_publication_rejected"}},
            control_schema=fixture.control_schema,
        )
    fixture.command["expected_approved_revision"] = 8
    retry = await _enqueue(fixture)
    assert retry["request_id"] != stale["request_id"] and retry["replayed"] is False
    assert (
        await fixture.connection.fetchval(
            f"SELECT idempotency_key FROM {namespace}.registry_publication_request WHERE request_id=$1",
            UUID(retry["request_id"]),
        )
        == "fhir-catalog:" + fixture.generation + ":approved:8"
    )
    await _complete(fixture, retry, await _claim_exact(fixture, retry))
    state_by_request_id = dict(
        await fixture.connection.fetch(f"SELECT request_id,state FROM {namespace}.registry_publication_request")
    )
    assert state_by_request_id == {UUID(stale["request_id"]): "rejected", UUID(retry["request_id"]): "completed"}


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["generation_id", "metadata_generation"])
async def test_split_publication_keeps_exact_serving_and_metadata_cas(pending_catalog_publication, changed):
    fixture = pending_catalog_publication
    receipt = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, receipt)
    value = 10 if changed == "generation_id" else "e" * 64
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_serving_control SET {changed}=$1', value
    )
    before = await _state(fixture)
    with pytest.raises(RegistryApprovalConflict, match="source_changed"):
        await _complete(fixture, receipt, lease_id)
    assert await _state(fixture) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        "company",
        "group",
        "network",
        "company_links",
        "network_binding",
        "provider",
        "location",
        "site_binding",
        "membership",
    ],
)
@pytest.mark.parametrize("change", ["edit", "remove"])
async def test_pending_proof_preserves_every_old_mapping_family(metadata_custody, kind, change):
    fixture = metadata_custody
    await _seed_pending_maps(fixture)
    table = f'"{fixture.control_schema}".registry_approved_record'
    await fixture.connection.execute(
        f"INSERT INTO {table} "
        "SELECT revision,$1,'existing-record',1,1,'{\"archived\":false}'::jsonb FROM unnest(ARRAY[6,7]) revision",
        kind,
    )
    async with fixture.connection.transaction():
        await fingerprints.require_fhir_catalog_pending_mappings(
            fixture.connection, 6, 7, control_schema=fixture.control_schema
        )
        if change == "edit":
            await fixture.connection.execute(
                f"UPDATE {table} SET record_json=record_json||'{{\"archived\":true}}'::jsonb "
                "WHERE approved_revision=7 AND record_key='existing-record'"
            )
        else:
            await fixture.connection.execute(
                f"DELETE FROM {table} WHERE approved_revision=7 AND record_key='existing-record'"
            )
        with pytest.raises(ValueError, match="membership_mapping_changed"):
            await fingerprints.require_fhir_catalog_pending_mappings(
                fixture.connection, 6, 7, control_schema=fixture.control_schema
            )


@pytest.mark.asyncio
async def test_pending_proof_accepts_retained_normalized_organization(metadata_custody):
    fixture = metadata_custody
    await _seed_pending_maps(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_approved_record '
        "SET record_json=jsonb_set(record_json,'{producer_id}','\"registry-retained-normalized-fhir-v1\"') "
        "WHERE record_kind='network_binding'"
    )
    async with fixture.connection.transaction():
        selected, _ = await fingerprints.select_fhir_catalog_mapping(
            fixture.connection, 7, SimpleNamespace(approved_custom_revision=6), control_schema=fixture.control_schema
        )
    assert selected == 6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("network_id", True),
        ("network_id", "101"),
        ("network_id", 2**31),
        ("network_id", None),
        ("archived", True),
        ("revision", 2),
        ("binding_id", "00000000-0000-0000-0000-000000000002"),
        ("binding_key", "invalid"),
        ("source_system", "other"),
        ("source_key", "../network-pending"),
        ("producer_id", "unapproved-producer"),
        ("source_scope_json", {}),
        ("source_scope_json", {"organization_id": "different", "capture_sha256": "c" * 64}),
        ("source_scope_json", {"organization_id": "network-pending", "capture_sha256": "invalid"}),
        (
            "source_scope_json",
            {"organization_id": "network-pending", "capture_sha256": "c" * 64, "insurance_plan_id": "plan-one"},
        ),
    ],
)
async def test_pending_proof_rejects_malformed_or_nonorganization_bindings(metadata_custody, field, value):
    fixture = metadata_custody
    await _seed_pending_maps(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_approved_record '
        "SET record_json=jsonb_set(record_json,ARRAY[$1::text],$2::jsonb) WHERE record_kind='network_binding'",
        field,
        json.dumps(value),
    )
    async with fixture.connection.transaction():
        with pytest.raises(ValueError, match="membership_mapping_changed"):
            await fingerprints.require_fhir_catalog_pending_mappings(
                fixture.connection, 6, 7, control_schema=fixture.control_schema
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "missing_alias",
        "alias_target",
        "alias_scope",
        "alias_source",
        "missing_network",
        "inactive_network",
        "unpaired_network",
        "provider_addition",
        "duplicate_binding_key",
    ],
)
async def test_pending_proof_requires_exact_aliases_and_only_paired_network_additions(metadata_custody, damage):
    fixture = metadata_custody
    await _seed_pending_maps(fixture)
    namespace = '"' + fixture.control_schema + '"'
    statement_by_damage = {
        "missing_alias": "DELETE FROM {control}.network_registry_alias",
        "alias_target": "UPDATE {control}.network_registry_alias SET network_id=102",
        "alias_scope": "UPDATE {control}.network_registry_alias SET scope_key='different-scope'",
        "alias_source": "UPDATE {control}.network_registry_alias SET source_id='different-source'",
        "missing_network": "DELETE FROM {control}.registry_approved_record WHERE record_kind='network'",
        "inactive_network": "UPDATE {control}.registry_approved_record SET record_json=jsonb_set(record_json,'{{archived}}','true') WHERE record_kind='network'",
        "unpaired_network": "INSERT INTO {control}.registry_approved_record VALUES(7,'network','102',1,6,'{{\"network_id\":102,\"archived\":false}}')",
        "provider_addition": "INSERT INTO {control}.registry_approved_record VALUES(7,'provider','new-provider',1,6,'{{\"archived\":false}}')",
        "duplicate_binding_key": "INSERT INTO {control}.registry_approved_record SELECT approved_revision,record_kind,'00000000-0000-0000-0000-000000000002',record_revision,custom_revision,jsonb_set(record_json,'{{binding_id}}','\"00000000-0000-0000-0000-000000000002\"') FROM {control}.registry_approved_record WHERE record_kind='network_binding'",
    }
    await fixture.connection.execute(statement_by_damage[damage].format(control=namespace))
    async with fixture.connection.transaction():
        with pytest.raises(ValueError, match="membership_mapping_changed"):
            await fingerprints.require_fhir_catalog_pending_mappings(
                fixture.connection, 6, 7, control_schema=fixture.control_schema
            )


@pytest.mark.asyncio
async def test_split_publication_rechecks_alias_after_enqueue(pending_catalog_publication):
    fixture = pending_catalog_publication
    receipt = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, receipt)
    await fixture.connection.execute(f'UPDATE "{fixture.control_schema}".network_registry_alias SET network_id=102')
    before = await _state(fixture)
    with pytest.raises(RegistryApprovalConflict, match="snapshot_changed"):
        await _complete(fixture, receipt, lease_id)
    assert await _state(fixture) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["generation_id", "approved_revision", "metadata_generation"])
async def test_stale_predecessor_cannot_publish(catalog_publication, changed):
    fixture = catalog_publication
    receipt = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, receipt)
    if changed == "approved_revision":
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".registry_revision_control SET approved_revision=7'
        )
    elif changed == "generation_id":
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_control SET generation_id=10'
        )
    else:
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_control SET metadata_generation=$1', "e" * 64
        )
    before = await _state(fixture)
    with pytest.raises(RegistryApprovalConflict, match="source_changed"):
        await _complete(fixture, receipt, lease_id)
    after = await _state(fixture)
    assert tuple(after) == tuple(before) and after["published_at"] is None and after["state"] == "running"


@pytest.mark.asyncio
async def test_revoked_authority_rolls_back(catalog_publication):
    fixture = catalog_publication
    receipt = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, receipt)
    calls = []

    async def authorize(*, command, snapshot=None, connection=None):
        calls.append(command)
        return await _authorize(command=command) if len(calls) == 1 else {"authorized": False}

    with pytest.raises(ValueError, match="authority_invalid"):
        await _complete(fixture, receipt, lease_id, authorize=authorize)
    after = await _state(fixture)
    assert after["metadata_generation"] is None and after["published_at"] is None and after["state"] == "running"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_source_change_and_expired_lease_refuse(catalog_publication):
    fixture = catalog_publication
    receipt = await _enqueue(fixture)
    lease_id = await _claim_exact(fixture, receipt)
    with pytest.raises(RegistryApprovalConflict, match="source_changed"):
        await _complete(fixture, receipt, lease_id, verify_sources=AsyncMock(return_value=False))
    assert (await _state(fixture))["published_at"] is None
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_publication_request '
        "SET lease_expires_at=clock_timestamp()-interval '1 second'"
    )
    with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
        await _complete(fixture, receipt, lease_id)
    assert (await _state(fixture))["metadata_generation"] is None
