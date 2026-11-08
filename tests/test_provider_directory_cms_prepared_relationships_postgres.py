# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed frozen-data/private-graph component proof; no Source enrollment proof."""

import hashlib
import importlib
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateColumn

from db import models
from db.plan_release_storage import storage_relation
from process import cms_npd_source
from process import provider_directory_cms_npd_relationship as legacy
from process import provider_directory_cms_prepared_identity as identity
from process import provider_directory_cms_prepared_relationships as prepared
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_generic_source_policy as source
from process.provider_directory_generic_candidate import RESOURCE_COLUMNS, WITNESS_COLUMNS
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_cms_prepared_context_postgres import native_case, observe, publisher
from tests.test_provider_directory_cms_prepared_workspace import handoff_for
from tests.test_provider_directory_cms_prepared_workspace_postgres import scope

fhir = importlib.import_module("process.provider_directory_fhir")


def _raw_directory_families():
    """Keep the synthetic directory entities and unresolved site reference."""
    return {
        "Organization": [
            {
                "resourceType": "Organization",
                "id": "payer-one",
                "name": "Example Payer",
                "endpoint": [{"reference": "Endpoint/ep-one"}],
            },
            {
                "resourceType": "Organization",
                "id": "network-one",
                "type": [{"text": "ntwk"}],
                "partOf": {"reference": "Organization/payer-one"},
            },
        ],
        "Location": [
            {
                "resourceType": "Location",
                "id": "site-one",
                "managingOrganization": {"reference": "Organization/payer-one"},
                "endpoint": [{"reference": "Endpoint/ep-one"}, {"display": "unknown"}],
                "period": {"start": "2020-01-01", "end": "2030-01-01"},
            }
        ],
        "Endpoint": [
            {
                "resourceType": "Endpoint",
                "id": "ep-one",
                "status": "active",
                "connectionType": {"code": "hl7-fhir-rest"},
                "address": "https://example.test/fhir",
                "managingOrganization": {"reference": "Organization/payer-one"},
            }
        ],
        "HealthcareService": [
            {
                "resourceType": "HealthcareService",
                "id": "service-one",
                "providedBy": {"reference": "Organization/payer-one"},
                "location": [{"reference": "Location/site-one"}],
                "coverageArea": [{"reference": "Location/missing"}],
            }
        ],
    }


def _raw_coverage_families():
    """Keep nested plan periods and the practitioner relationship assertions."""
    return {
        "InsurancePlan": [
            {
                "resourceType": "InsurancePlan",
                "id": "plan-one",
                "status": "active",
                "ownedBy": {"reference": "Organization/payer-one"},
                "administeredBy": {"reference": "Organization/payer-one"},
                "network": [{"reference": "Organization/network-one"}],
                "endpoint": [{"reference": "Endpoint/ep-one"}],
                "period": {"start": "2020-01-01", "end": "2030-01-01"},
                "plan": [
                    {
                        "network": [{"reference": "Organization/network-one"}],
                        "coverageArea": [{"reference": "Location/site-one"}],
                        "period": {"start": "2021-01-01"},
                    }
                ],
                "coverage": [{"network": [{"reference": "Organization/network-one"}]}],
            }
        ],
        "Practitioner": [
            {
                "resourceType": "Practitioner",
                "id": "practitioner-one",
                "qualification": [{"issuer": {"reference": "Organization/payer-one"}}],
            }
        ],
        "PractitionerRole": [
            {
                "resourceType": "PractitionerRole",
                "id": "role-one",
                "practitioner": {"reference": "Practitioner/practitioner-one"},
                "organization": {"reference": "Organization/payer-one"},
                "location": [{"reference": "Location/site-one"}],
                "network": [{"reference": "Organization/network-one"}],
                "insurancePlan": [{"reference": "InsurancePlan/plan-one"}],
                "healthcareService": [{"reference": "HealthcareService/service-one"}],
                "endpoint": [{"reference": "Endpoint/ep-one"}],
            }
        ],
    }


def raw_families():
    """Return the same eight raw families in their original insertion order."""
    return {
        **_raw_directory_families(),
        **_raw_coverage_families(),
        "OrganizationAffiliation": [
            {
                "resourceType": "OrganizationAffiliation",
                "id": "affiliation-one",
                "organization": {"reference": "Organization/payer-one"},
                "participatingOrganization": {"reference": "Organization/network-one"},
                "network": [{"reference": "Organization/network-one"}],
                "location": [{"reference": "Location/site-one"}],
                "healthcareService": [{"reference": "HealthcareService/service-one"}],
                "endpoint": [{"reference": "Endpoint/ep-one"}],
                "insurancePlan": [{"reference": "InsurancePlan/plan-one"}],
            }
        ],
    }


async def input_snapshots(case):
    values = []
    for row in case.inventory["relations"]:
        relation = storage_relation(case.storage.storage_schema, row["relation_name"])
        values.append(
            await case.admin.fetchval(
                f"SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) FROM {relation} r"
            )
        )
    return values


async def _type_frozen_payloads(case, tables):
    """Install the existing modeled payload/witness columns and native keys."""
    for root, model in (
        (workspace.storage.RESOURCE, models.ProviderDirectoryDatasetResource),
        (workspace.storage.WITNESS, models.ProviderDirectoryCMSNPDResourceWitness),
    ):
        relation = storage_relation(case.storage.storage_schema, tables[root]["name"])
        await case.admin.execute(f"ALTER TABLE {relation} DROP COLUMN id")
        for column in model.__table__.columns:
            definition = str(
                CreateColumn(Column(column.name, column.type, nullable=column.nullable)).compile(dialect=dialect())
            )
            await case.admin.execute(f"ALTER TABLE {relation} ADD COLUMN {definition}")
        await case.admin.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY(dataset_id,resource_type,resource_id)")


async def _write_frozen_resources(case, document, tables, release):
    """Write the same materialized twins and canonical raw-witness bytes."""
    case.normalized = []
    for resources in case.raw.values():
        for raw in resources:
            normalized = fhir.materialize_provider_directory_dataset_fhir_resource(
                source_id="cms-npd",
                dataset_id=document["identity"]["dataset_id"],
                resource=raw,
                run_id=document["identity"]["acquisition_root_run_id"],
                semantic_projection_as_of=release["generated_at"],
            )
            case.normalized.append(normalized)
            encoded = workspace.storage._encoded(raw)
            resource_values = [normalized.get(column) for column in RESOURCE_COLUMNS]
            resource_values[RESOURCE_COLUMNS.index("payload_json")] = workspace.storage._encoded(
                normalized["payload_json"]
            )
            await case.admin.execute(
                f"INSERT INTO {storage_relation(case.storage.storage_schema, tables[workspace.storage.RESOURCE]['name'])} "
                f"({','.join(RESOURCE_COLUMNS)}) VALUES({','.join('$' + str(parameter_index) for parameter_index in range(1, len(RESOURCE_COLUMNS) + 1))})",
                *resource_values,
            )
            await case.admin.execute(
                f"INSERT INTO {storage_relation(case.storage.storage_schema, tables[workspace.storage.WITNESS]['name'])} "
                f"({','.join(WITNESS_COLUMNS)}) VALUES({','.join('$' + str(parameter_index) for parameter_index in range(1, len(WITNESS_COLUMNS) + 1))})",
                document["identity"]["dataset_id"],
                "cms-npd",
                release["vector_sha256"],
                raw["resourceType"],
                raw["id"],
                hashlib.sha256(encoded.encode()).hexdigest(),
                normalized["payload_hash"],
                encoded,
            )


async def _create_readonly_identity_roots(case):
    """Leave canonical identity targets read-only to the genuine Publisher."""
    layouts = workspace._layouts()
    for root in workspace.storage.AUXILIARY_ROOTS:
        definitions, _, checks = layouts[root]
        relation = storage_relation(case.storage.catalog_schema, root)
        await case.admin.execute(
            f"CREATE TABLE {relation} ({','.join([*definitions, *(f'CHECK ({c})' for c in checks)])})"
        )
        await case.admin.execute(f'GRANT SELECT ON {relation} TO "{case.policy.publisher_role}"')
        assert not await case.admin.fetchval(
            "SELECT has_table_privilege($1,$2,'INSERT,UPDATE,DELETE,TRUNCATE')", case.policy.publisher_role, relation
        )


@pytest_asyncio.fixture
async def native_frozen_case(native_case, request):
    """Reusable local typed twins/all8 families with Publisher-only canonical reads."""
    case = native_case
    await case.admin.execute(
        f'GRANT "{case.policy.owner_role}" TO "{case.policy.publisher_role}" WITH INHERIT TRUE,SET TRUE'
    )
    await case.admin.execute(f'GRANT USAGE,CREATE ON SCHEMA private_source TO "{case.policy.owner_role}"')
    await case.admin.execute(f'GRANT USAGE ON SCHEMA private_source TO "{case.roles["reader"]}"')
    old_names = [native_row["relation_name"] for native_row in case.inventory["relations"]]
    document = handoff_for(case.storage, case.inventory).document
    case.raw = raw_families()
    if getattr(request, "param", None) == "multi_batch":
        case.raw["Location"][0]["endpoint"] = [{"reference": "Endpoint/ep-one"} for _ in range(4101)]
    release = document["identity"]["metadata"]["source_release"]
    for filename, kind in cms_npd_source.RESOURCE_FILES:
        release["files"][filename].update(row_count=len(case.raw[kind]), distinct_count=len(case.raw[kind]))
    document["admission"]["resource_count"] = sum(map(len, case.raw.values()))
    for old, native_row in zip(old_names, case.inventory["relations"], strict=True):
        await case.admin.execute(f'ALTER TABLE private_source."{old}" RENAME TO "{native_row["relation_name"]}"')
    tables = document["tables"]
    await _type_frozen_payloads(case, tables)
    await _write_frozen_resources(case, document, tables, release)
    await _create_readonly_identity_roots(case)
    case.handoff = FHIRSourceCandidateHandoff(workspace.storage._encoded(document))
    case.preparation_id, case.admission = str(uuid4()), "f" * 64
    case.input_snapshot = await input_snapshots(case)
    yield case
    assert await input_snapshots(case) == case.input_snapshot


async def stage_identities(current, case):
    for kind in ("Organization", "Location"):
        await identity.stage_prepared_entity_batch(current, resources=case.raw[kind])
    for kind in ("InsurancePlan", "PractitionerRole"):
        await identity.stage_prepared_resource_identity_batch(
            current, resource_type=kind, resource_ids=[raw["id"] for raw in case.raw[kind]]
        )


async def test_native_all_families_raw_nested_null_refs_and_periods_match_legacy(native_frozen_case):
    case = native_frozen_case
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stage_identities(current, case)
            projected = await prepared.project_prepared_cms_relationships(current, case.handoff)
            assert set(projected["family_counts"]) == set(legacy.FIELDS)
            assert projected["relationship_count"] > 20 and projected["family_counts"]["Practitioner"] == 1
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            await current.finish_indexes()
            verified = await prepared.assert_prepared_cms_relationships(current, case.handoff)
            assert verified["relationship_count"] == projected["relationship_count"]
            assert verified == await prepared.assert_prepared_cms_relationships(current, case.handoff)
            graph = storage_relation(current.policy.schema, current.receipts[workspace.storage.RELATIONSHIP]["name"])
            statuses = {
                row["resolution_status"]
                for row in await driver.fetch(f"SELECT DISTINCT resolution_status FROM {graph}")
            }
            assert statuses == {"resolved", "unresolved"}
            assert await driver.fetchval(f"SELECT count(*) FROM {graph} WHERE target_reference IS NULL") == 1
            assert dict(
                await driver.fetchrow(
                    f"SELECT parent_ordinal,reference_ordinal,period_start,period_end FROM {graph} "
                    "WHERE resource_type='InsurancePlan' AND reference_field='plan.network'"
                )
            ) == {"parent_ordinal": 1, "reference_ordinal": 1, "period_start": "2021-01-01", "period_end": None}
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient


async def test_native_projection_failure_rolls_back_prior_families_and_allows_retry(native_frozen_case, monkeypatch):
    case = native_frozen_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stage_identities(current, case)
            original = current.copy_rows

            async def fail_family(root, columns, rows):
                await original(root, columns, rows)
                if rows[0][3] == "Location":
                    raise RuntimeError("second family failed")
                return len(rows)

            with monkeypatch.context() as patch:
                patch.setattr(current, "copy_rows", fail_family)
                with pytest.raises(RuntimeError, match="second family failed"):
                    await prepared.project_prepared_cms_relationships(current, case.handoff)
            graph = storage_relation(current.policy.schema, current.receipts[workspace.storage.RELATIONSHIP]["name"])
            assert await driver.fetchval(f"SELECT count(*) FROM {graph}") == 0
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert await prepared.project_prepared_cms_relationships(current, case.handoff)


async def _corrupt_graph(driver, graph, change):
    """Apply one exact graph defect for the existing complete-parity checks."""
    if change == "missing":
        await driver.execute(f"DELETE FROM {graph} WHERE resource_type='Practitioner'")
    elif change == "extra":
        await driver.execute(
            f"INSERT INTO {graph} SELECT dataset_id,source_id,release_id,resource_type,"
            "resource_id,source_payload_hash,raw_payload_sha256,reference_field,parent_ordinal,reference_ordinal+99,"
            f"target_type,target_reference,target_resource_id,resolution_status,period_start,period_end FROM {graph} "
            "WHERE resource_type='Practitioner'"
        )
    elif change == "wrong_target":
        await driver.execute(f"UPDATE {graph} SET target_resource_id='other' WHERE resource_type='Practitioner'")
    else:
        await driver.execute(f"UPDATE {graph} SET target_reference='Endpoint/changed' WHERE target_reference IS NULL")


@pytest.mark.parametrize("change", ["missing", "extra", "wrong_target", "null_reference"])
async def test_native_complete_comparison_rejects_exact_graph_corruption(native_frozen_case, change):
    case = native_frozen_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stage_identities(current, case)
            await prepared.project_prepared_cms_relationships(current, case.handoff)
            await current.finish_indexes()
            graph = storage_relation(current.policy.schema, current.receipts[workspace.storage.RELATIONSHIP]["name"])
            await _corrupt_graph(driver, graph, change)
            with pytest.raises(RuntimeError, match="projection_incomplete"):
                await prepared.assert_prepared_cms_relationships(current, case.handoff)


async def test_native_opaque_entity_aliases_preserve_ambiguous_resolution(native_frozen_case):
    case = native_frozen_case
    root = workspace.storage.CMS_IDENTITY_ROOTS[0]
    canonical = storage_relation(case.storage.catalog_schema, root)
    shared = uuid4()
    for raw in case.raw["Organization"]:
        await case.admin.execute(
            f"INSERT INTO {canonical} VALUES('cms-npd','Organization',$1,$2,NULL,$3)",
            raw["id"],
            shared,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stage_identities(current, case)
            await prepared.project_prepared_cms_relationships(current, case.handoff)
            await current.finish_indexes()
            verified = await prepared.assert_prepared_cms_relationships(current, case.handoff)
            graph = storage_relation(current.policy.schema, current.receipts[workspace.storage.RELATIONSHIP]["name"])
            assert await driver.fetchval(f"SELECT count(*) FROM {graph} WHERE resolution_status='ambiguous'") > 0
            assert verified["relationship_count"] > 20


@pytest.mark.parametrize("native_frozen_case", ["multi_batch"], indirect=True)
async def test_native_cursor_copy_crosses_frame_row_limit_without_losing_refs(native_frozen_case, monkeypatch):
    case = native_frozen_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stage_identities(current, case)
            calls = []
            original = current.copy_rows

            async def copied(root, columns, rows):
                calls.append((rows[0][3], len(rows)))
                return await original(root, columns, rows)

            monkeypatch.setattr(current, "copy_rows", copied)
            result = await prepared.project_prepared_cms_relationships(current, case.handoff)
            assert (
                sum(count for kind, count in calls if kind == "Location") == result["family_counts"]["Location"] == 4102
            )
            assert sum(kind == "Location" for kind, _ in calls) == 2 and max(count for _, count in calls) <= 4096
            await current.finish_indexes()
            assert (await prepared.assert_prepared_cms_relationships(current, case.handoff))["family_counts"][
                "Location"
            ] == 4102


async def test_native_disqualified_frozen_witness_refuses_before_graph_copy(native_frozen_case):
    case = native_frozen_case
    table = storage_relation(
        case.storage.storage_schema, case.handoff.document["tables"][workspace.storage.WITNESS]["name"]
    )
    original = await case.admin.fetchval(
        f"SELECT normalized_payload_hash FROM {table} WHERE resource_type='Practitioner'"
    )
    await case.admin.execute(
        f"UPDATE {table} SET normalized_payload_hash=$1 WHERE resource_type='Practitioner'", "0" * 64
    )
    try:
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            async with scope(case, session, driver, inputs) as current:
                await stage_identities(current, case)
                with pytest.raises(ValueError, match="frozen_witness_mismatch"):
                    await prepared.project_prepared_cms_relationships(current, case.handoff)
                graph = storage_relation(
                    current.policy.schema, current.receipts[workspace.storage.RELATIONSHIP]["name"]
                )
                assert await driver.fetchval(f"SELECT count(*) FROM {graph}") == 0
    finally:
        await case.admin.execute(
            f"UPDATE {table} SET normalized_payload_hash=$1 WHERE resource_type='Practitioner'", original
        )
