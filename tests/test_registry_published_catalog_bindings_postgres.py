# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture-free approval requires exact complete evidence from a genuinely sealed catalog."""

import hashlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import network_approved_source_bindings as approved_bindings
from process import network_source_binding_store as binding_store
from process import registry_source_binding_publication as source_batch
from process.network_approved_membership_source import pin_approved_membership_source
from process.registry_fhir_catalog_extraction import FHIRCatalogOrganization
from process.registry_fhir_catalog_projection import (
    BINDING_COLUMNS,
    NETWORK_COLUMNS,
    SourceCatalogContext,
    format_fhir_catalog_projection,
)
from tests import test_registry_fhir_catalog_custody_postgres as custody
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_network_source_binding_store_postgres import _command, _source_batch_networks
from tests.test_registry_record_store_postgres import _actor
from tests.test_registry_source_batch_approval_postgres import _authority, _complete, _queue_and_claim

pytestmark = pytest.mark.asyncio
metadata_custody = custody.metadata_custody


async def _published_projection(fixture, networks, monkeypatch):
    descriptor = custody.source_descriptor()
    descriptor["coordinates"].update(producer_id="registry-fhir-catalog-v1", edition_id="dataset-one")
    monkeypatch.setattr(custody, "source_descriptor", lambda: descriptor)
    snapshot = await custody.seed_observations(fixture, len(networks))
    await fixture.connection.copy_records_to_table(
        "network_registry_identity",
        schema_name=fixture.control_schema,
        columns=("network_id",),
        records=[(network["record_id"],) for network in networks],
    )
    organizations = tuple(
        FHIRCatalogOrganization(
            f"organization-{index:04d}",
            f"organization-{index:04d}",
            (),
            False,
            "active",
            (),
            hashlib.sha256(f"organization-{index:04d}".encode()).hexdigest(),
        )
        for index in range(len(networks))
    )
    context = SourceCatalogContext(
        approved_bindings.RegistryNetworkSourceCoordinates(**descriptor["coordinates"]),
        snapshot["source_vector_json"][0]["source_generation"],
        snapshot["metadata_generation"],
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        "published_normalized",
    )
    projection = format_fhir_catalog_projection(
        organizations,
        (),
        tuple(
            (organization.resource_id, network["record_id"]) for organization, network in zip(organizations, networks)
        ),
        context,
    )
    for table, columns, projection_rows in (
        ("registry_fhir_catalog_network", NETWORK_COLUMNS, projection.network_rows),
        ("registry_fhir_catalog_binding", BINDING_COLUMNS, projection.binding_rows),
    ):
        await fixture.connection.copy_records_to_table(
            table, schema_name=fixture.target.schema_name, columns=columns, records=projection_rows
        )
    return await custody.freeze_metadata(fixture, snapshot=snapshot), projection


@pytest.fixture
async def published_case(serving_schema, metadata_custody, monkeypatch):
    connection, schema, engine = serving_schema
    actor = _actor()
    networks = await _source_batch_networks(engine, schema, actor, 3)
    snapshot, projection = await _published_projection(metadata_custody, networks, monkeypatch)
    snapshot["published_at"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await connection.execute(
        f'INSERT INTO "{schema}".registry_fhir_catalog_snapshot '
        f'SELECT * FROM jsonb_populate_record(NULL::"{schema}".registry_fhir_catalog_snapshot,$1::jsonb)',
        json.dumps(snapshot, default=str),
    )
    documents = []
    for projected in projection.binding_rows:
        document = json.loads(projected[-1])
        binding_by_field = {
            field: document[field]
            for field in (*snapshot_coordinates(document), "binding_id", "source_key", "evidence_id", "evidence_sha256")
        }
        binding_by_field.update(
            source_scope_json=document["source_scope"],
            network_id=projected[1],
            operation="bind",
            expected_revision=0,
            expected_network_id=None,
        )
        documents.append(binding_by_field)
    return SimpleNamespace(
        connection=connection, schema=schema, actor=actor, engine=engine, snapshot=snapshot, rows=documents
    )


def snapshot_coordinates(document):
    return {
        field: document[field]
        for field in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
    }


async def _save(case, rows):
    batch = _command(rows)
    async with case.connection.transaction():
        receipt = await binding_store.apply_network_source_binding_batch(
            case.connection, batch, case.actor, control_schema=case.schema
        )
    heads = await case.connection.fetchrow(
        f'SELECT draft_revision,approved_revision FROM "{case.schema}".registry_revision_control'
    )
    case.command = {
        "operation": source_batch.APPROVAL_OPERATION,
        "batch_references": [
            {
                "idempotency_key": batch.idempotency_key,
                "request_sha256": binding_store._metadata(batch, case.actor)[3],
                "custom_revision": receipt["custom_revision"],
            }
        ],
        "source_coordinates": snapshot_coordinates(rows[0]),
        "metadata_generation": case.snapshot["metadata_generation"],
        "selection_count": None,
        "selection_sha256": None,
        "expected_draft_revision": heads["draft_revision"],
        "expected_approved_revision": heads["approved_revision"],
        "expected_serving_generation": 0,
        "reason": "Review complete published metadata",
        "idempotency_key": uuid4().hex,
    }


async def _resolve(case):
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        return await source_batch.resolve_source_batch(
            case.connection, case.command, case.actor, control_schema=case.schema, allow_unbound=True
        )


async def test_published_catalog_approval_completes_before_membership_staging(published_case):
    case = published_case
    await _save(case, case.rows)
    await _assert_native_binding_keys(case)
    compact, command, _, lease = await _queue_and_claim(case)
    assert len(command.selection) == compact["selection_count"] == 6
    result = await _complete(case, lease, _authority([]))
    assert result["selected_count"] == 6 and result["serving_generation"] == 0
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        pin = await pin_approved_membership_source(
            case.connection, approved_revision=result["approved_revision"], control_schema=case.schema
        )
        assert (
            await approved_bindings.require_approved_network_source_bindings(
                case.connection,
                pin,
                approved_bindings.RegistryNetworkSourceCoordinates(**compact["source_coordinates"]),
                control_schema=case.schema,
            )
            == 3
        )
    assert tuple(
        await case.connection.fetchrow(f'''SELECT
      (SELECT count(*) FROM "{case.schema}".network_membership_candidate),
      (SELECT count(*) FROM "{case.schema}".network_serving_manifest)''')
    ) == (0, 0)


async def _assert_native_binding_keys(case):
    keys_by_id = {
        str(binding["binding_id"]): binding["binding_key"]
        for binding in await case.connection.fetch(
            f'SELECT binding_id,binding_key FROM "{case.schema}".registry_network_binding'
        )
    }
    for row in case.rows:
        identity_by_field = snapshot_coordinates(row)
        identity_by_field.update(source_key=row["source_key"], source_scope_json=row["source_scope_json"])
        canonical = json.dumps(identity_by_field, sort_keys=True, separators=(",", ":")).encode()
        assert (
            keys_by_id[row["binding_id"]]
            == hashlib.sha256(b"registry_network_source_binding:v1:" + canonical).hexdigest()
        )


@pytest.mark.parametrize("forgery", ["generation", "hash", "dataset", "source", "schema", "partial", "snapshot"])
async def test_published_catalog_refuses_foreign_or_incomplete_evidence(published_case, forgery):
    case = published_case
    rows = json.loads(json.dumps(case.rows))
    for row in rows:
        _alter_catalog_binding(row, forgery)
    if forgery == "partial":
        rows.pop()
    await _save(case, rows)
    if forgery == "snapshot":
        case.command["metadata_generation"] = "e" * 64
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        await _resolve(case)
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_publication_request') == 0
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_approval_history') == 0


def _alter_catalog_binding(row, forgery):
    if forgery == "generation":
        row["source_scope_json"]["source_generation"] = "c" * 64
        row["evidence_id"] = "c" * 64 + ":Organization/" + row["source_key"]
    elif forgery == "hash":
        row["evidence_sha256"] = "d" * 64
    elif forgery == "dataset":
        row.update(dataset_id="other-dataset", edition_id="other-dataset")
    elif forgery in {"source", "schema"}:
        row["source_id" if forgery == "source" else "dataset_schema"] = "other_source"


async def test_published_catalog_refuses_unsealed_custody(published_case):
    case = published_case
    await _save(case, case.rows)
    with pytest.raises(ValueError, match="registry_fhir_catalog_custody_invalid"):
        async with case.connection.transaction(isolation="repeatable_read"):
            await case.connection.execute(
                f'UPDATE "{case.schema}".registry_fhir_catalog_snapshot '
                "SET custody_json=jsonb_set(custody_json,'{component}','\"other\"'::jsonb)",
            )
            await _resolve(case)


async def test_native_published_binding_duplicate_refusal_is_atomic(published_case):
    case = published_case
    with pytest.raises(ValueError, match="registry_source_binding_input_invalid"):
        await _save(case, [*case.rows, case.rows[0]])
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_network_binding') == 0


@pytest.mark.parametrize("field", ["edition_id", "source_key", "evidence_id", "source_scope_json", "evidence_sha256"])
async def test_shared_approved_sql_rejects_malformed_published_metadata(published_case, field):
    case = published_case
    await _save(case, case.rows)
    compact, _, _, lease = await _queue_and_claim(case)
    completion = await _complete(case, lease, _authority([]))
    replacement = (
        {"organization_id": "organization-0000", "source_generation": "A" * 64}
        if field == "source_scope_json"
        else "other"
    )
    async with case.connection.transaction():
        await case.connection.execute(
            f'UPDATE "{case.schema}".registry_approved_record SET record_json=jsonb_set(record_json,ARRAY[$2::text],$3::jsonb) '
            "WHERE approved_revision=$1 AND record_kind='network_binding'",
            completion["approved_revision"],
            field,
            json.dumps(replacement),
        )
        coordinates_by_field = dict(compact["source_coordinates"])
        if field == "edition_id":
            coordinates_by_field[field] = replacement
        parameters = approved_bindings.RegistryNetworkSourceCoordinates(**coordinates_by_field).sql_parameters
        namespace = '"' + case.schema + '"'
        checks = await case.connection.fetchrow(
            approved_bindings._VALIDATION_SQL.format(
                bindings=approved_bindings.APPROVED_NETWORK_BINDINGS_SQL.format(namespace=namespace),
                namespace=namespace,
            ),
            completion["approved_revision"],
            *parameters,
            approved_bindings._TRIM_CHARACTERS,
            approved_bindings.UHC_PROVIDER_FILE_SOURCE_ID,
        )
        assert checks["invalid"]
