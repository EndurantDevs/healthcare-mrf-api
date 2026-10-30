# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained evidence selection and existing request-boundary regression checks."""

import json
import re
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_detail_network_metadata
from api import provider_directory_dataset_sql as dataset_sql
from api import provider_directory_evidence_lineage as lineage
from api.endpoint import npi


def sqlite_sql(sql):
    source_ids = dataset_sql._publication_source_ids_sql()
    sql = sql.replace(
        f"({source_ids}) ? requested.source_id",
        f"EXISTS (SELECT 1 FROM json_each({source_ids}) WHERE value=requested.source_id)",
    )
    sql = re.sub(
        r"FROM unnest\(CAST\(:evidence_source_ids AS varchar\[\]\), CAST\(:evidence_dataset_ids AS varchar\[\]\)\)\s+AS requested\(source_id, dataset_id\)",
        "FROM (SELECT source.value AS source_id, dataset.value AS dataset_id "
        "FROM json_each(:evidence_source_ids_json) source JOIN json_each(:evidence_dataset_ids_json) dataset "
        "ON dataset.key=source.key) AS requested",
        sql,
    )
    sql = sql.replace(
        "= ANY(CAST(:source_record_ids AS varchar[]))", "IN (SELECT value FROM json_each(:source_record_ids_json))"
    )
    sql = re.sub(
        r"FROM unnest\(CAST\(:source_record_ids AS varchar\[\]\), CAST\(:npis AS bigint\[\]\), CAST\(:address_keys AS uuid\[\]\), CAST\(:phone_numbers AS varchar\[\]\)\)\s+AS requested\(source_record_id, npi, address_key, phone_number\)",
        "FROM (SELECT record.value AS source_record_id, provider.value AS npi, address_by_field.value AS address_key, phone.value AS phone_number "
        "FROM json_each(:source_record_ids_json) record JOIN json_each(:npis_json) provider ON provider.key=record.key "
        "JOIN json_each(:address_keys_json) address_by_field ON address_by_field.key=record.key JOIN json_each(:phone_numbers_json) phone ON phone.key=record.key) AS requested",
        sql,
    )
    sql = sql.replace("CROSS JOIN LATERAL jsonb_array_elements_text", "CROSS JOIN json_each")
    sql = sql.replace("AS publication_source(source_id)", "AS publication_source")
    sql = sql.replace("publication_source.source_id", "publication_source.value")
    sql = sql.replace(
        "SELECT DISTINCT publication_source.value,", "SELECT DISTINCT publication_source.value AS source_id,"
    )
    sql = sql.replace("jsonb_typeof", "json_type")
    for cast in ("::varchar", "::jsonb", "::text", "::integer"):
        sql = sql.replace(cast, "")
    return sql


def sqlite_params(params_by_name):
    return {
        **params_by_name,
        **{f"{key}_json": json.dumps(value) for key, value in params_by_name.items() if isinstance(value, list)},
    }


@pytest.fixture
def database():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("ATTACH DATABASE ':memory:' AS mrf")
    connection.create_function("BTRIM", 1, lambda value: value.strip() if value is not None else None)
    connection.create_function(
        "regexp_replace", 4, lambda value, pattern, replacement, _flags: re.sub(pattern, replacement, value)
    )
    connection.executescript("""
      CREATE TABLE mrf.provider_directory_source(source_id TEXT,endpoint_id TEXT,canonical_api_base TEXT);
      CREATE TABLE mrf.provider_directory_api_endpoint(endpoint_id TEXT,canonical_api_base TEXT);
      CREATE TABLE mrf.provider_directory_endpoint_dataset(
        dataset_id TEXT PRIMARY KEY,endpoint_id TEXT,acquisition_root_run_id TEXT,import_run_id TEXT,
        status TEXT,is_current INTEGER,published_at TEXT,superseded_at TEXT,
        publication_metadata_json TEXT,publication_metadata_summary_json TEXT);
      CREATE TABLE mrf.provider_directory_dataset_resource(
        dataset_id TEXT,resource_type TEXT,resource_id TEXT,payload_json TEXT,
        PRIMARY KEY(dataset_id,resource_type,resource_id));
      CREATE TABLE mrf.provider_directory_address_overlay(
        source_record_id TEXT,source_id TEXT,resource_type TEXT,resource_id TEXT,
        last_seen_run_id TEXT,npi INTEGER,address_key TEXT,first_line TEXT,phone_number TEXT);
      CREATE TABLE mrf.provider_directory_network_catalog(
        source_id TEXT,network_resource_id TEXT,provider_directory_network_name TEXT);
      INSERT INTO mrf.provider_directory_source VALUES ('source-a','endpoint-new','https://new.example.test/fhir');
      INSERT INTO mrf.provider_directory_api_endpoint VALUES ('endpoint-old','https://old.example.test/fhir');
      INSERT INTO mrf.provider_directory_api_endpoint VALUES ('endpoint-new','https://new.example.test/fhir');
      INSERT INTO mrf.provider_directory_address_overlay VALUES
        ('provider_directory_fhir:practitioner_role:source-a:role-a:location-a','source-a',
         'PractitionerRole','role-a','root-R1',1234567890,'00000000-0000-0000-0000-000000000001','Office R1','8005550100');
      INSERT INTO mrf.provider_directory_network_catalog VALUES ('source-a','network-a','Network R2');
    """)
    _seed_publications(connection)
    yield connection
    connection.close()


def _seed_publications(connection):
    for suffix, current in (("R1", False), ("R2", True)):
        identifier = f"dataset-{suffix}"
        metadata = {"source_ids": ["source-a"]}
        connection.execute(
            "INSERT INTO mrf.provider_directory_endpoint_dataset VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                identifier,
                "endpoint-old",
                f"root-{suffix}",
                f"attempt-{suffix}",
                "published" if current else "superseded",
                current,
                "2026-01-01",
                None if current else "2026-01-02",
                json.dumps(metadata),
                json.dumps(metadata),
            ),
        )
        for resource_type, resource_id, resource_payload in (
            (
                "PractitionerRole",
                "role-a",
                {
                    "active": True,
                    "insurance_plan_refs": ["InsurancePlan/plan-a"],
                    "network_refs": ["Organization/network-a"],
                    "fhir_meta": {"versionId": suffix},
                },
            ),
            (
                "InsurancePlan",
                "plan-a",
                {"status": "active", "name": f"Plan {suffix}", "network_refs": ["Organization/network-a"]},
            ),
            ("Organization", "network-a", {"active": True, "name": f"Network {suffix}"}),
        ):
            connection.execute(
                "INSERT INTO mrf.provider_directory_dataset_resource VALUES (?,?,?,?)",
                (identifier, resource_type, resource_id, json.dumps(resource_payload)),
            )


def executor(connection):
    async def execute(statement, *, session=None, params=None):
        rows = connection.execute(sqlite_sql(str(statement)), sqlite_params(params or {})).fetchall()
        return SimpleNamespace(all=lambda: [dict(row) for row in rows])

    return execute


def selected_evidence(connection, source_datasets):
    # Execute the maintained selected-dataset + typed projections and the exact
    # query-local catalog; only final requested direct-reference joins use SQLite JSON iteration.
    ctes = dataset_sql._provider_directory_current_resource_ctes_sql("mrf", has_dataset_scope=True)
    ctes += "," + npi._current_typed_resource_ctes_sql(True)
    sql = f"""WITH {ctes}
      SELECT role.last_seen_run_id, role.fhir_meta ->> 'versionId' AS role_version,
             plan.name AS plan_name, network.provider_directory_network_name AS network_name
        FROM current_roles role
        JOIN json_each(role.insurance_plan_refs) plan_ref
        JOIN current_insurance_plans plan
          ON plan.dataset_id=role.dataset_id AND plan.source_id=role.source_id
         AND plan.resource_id=substr(plan_ref.value,instr(plan_ref.value,'/')+1)
        JOIN json_each(role.network_refs) network_ref
        JOIN pinned_network_catalog network
          ON network.source_id=role.source_id
         AND network.network_resource_id=substr(network_ref.value,instr(network_ref.value,'/')+1)
    """
    params_by_name = {
        "evidence_source_ids": [source for source, _dataset in source_datasets],
        "evidence_dataset_ids": [dataset for _source, dataset in source_datasets],
    }
    return [tuple(row) for row in connection.execute(sqlite_sql(sql), sqlite_params(params_by_name)).fetchall()]


@pytest.mark.asyncio
async def test_coherent_evidence_survives_raw_and_catalog_advance(database, monkeypatch):
    monkeypatch.setattr(npi, "_execute_stmt", executor(database))
    address_by_field = {
        "npi": 1234567890,
        "address_key": "00000000-0000-0000-0000-000000000001",
        "source_record_ids": ["provider_directory_fhir:practitioner_role:source-a:role-a:location-a"],
    }
    bindings, role_datasets, affiliations = await lineage.resolve_address_evidence(
        npi, [address_by_field], session=object()
    )
    assert bindings == {"source-a": ("endpoint-old", "https://old.example.test/fhir")}
    assert role_datasets == (("source-a", "dataset-R1"),)
    assert affiliations == ()
    assert (
        database.execute("SELECT first_line FROM mrf.provider_directory_address_overlay").fetchone()[0] == "Office R1"
    )
    assert selected_evidence(database, role_datasets) == [("root-R1", "R1", "Plan R1", "Network R1")]
    database.execute(
        "UPDATE mrf.provider_directory_address_overlay SET last_seen_run_id='root-R2',first_line='Office R2'"
    )
    _, role_datasets, _ = await lineage.resolve_address_evidence(npi, [address_by_field], session=object())
    assert role_datasets == (("source-a", "dataset-R2"),)
    assert selected_evidence(database, role_datasets) == [("root-R2", "R2", "Plan R2", "Network R2")]


@pytest.mark.asyncio
async def test_sealed_header_source_membership_wins_over_current_alias(database, monkeypatch):
    database.execute(
        "UPDATE mrf.provider_directory_endpoint_dataset SET publication_metadata_summary_json=? WHERE dataset_id='dataset-R1'",
        (json.dumps({"source_ids": ["different-source"]}),),
    )
    monkeypatch.setattr(npi, "_execute_stmt", executor(database))
    address_by_field = {
        "npi": 1234567890,
        "address_key": "00000000-0000-0000-0000-000000000001",
        "source_record_ids": ["provider_directory_fhir:practitioner_role:source-a:role-a:location-a"],
    }
    with pytest.raises(RuntimeError, match="lineage_unavailable"):
        await lineage.resolve_address_evidence(npi, [address_by_field], session=object())


@pytest.mark.parametrize("kind", ["role", "affiliation"])
@pytest.mark.parametrize("indexes", [False, True])
def test_scoped_enrichment_has_no_current_pointer_mutable_catalog_or_raw_locator(kind, indexes):
    sql = _scoped_evidence_sql(kind, indexes)
    assert "requested_dataset_scope AS MATERIALIZED" in sql
    assert "mrf.provider_directory_network_catalog" not in sql
    assert "mrf.provider_directory_organization_affiliation AS affiliation_locator" not in sql
    assert "pinned_network_catalog AS NOT MATERIALIZED" in sql
    assert "FROM current_organizations" in sql
    assert "AS ready WHERE ready.dataset_id" not in sql
    assert "dataset.publication_metadata_json::jsonb" in sql
    assert "dataset.is_current IS FALSE" in sql
    assert "endpoint.endpoint_id = dataset.endpoint_id" in sql
    assert "requested.source_id" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["role", "affiliation"])
async def test_direct_enrichment_without_verified_scope_cannot_follow_current(kind):
    with pytest.raises(RuntimeError, match="lineage_unavailable"):
        await getattr(npi, f"_fetch_provider_directory_{kind}_evidence_map")(
            [("source-a", "driver-a")], session=object()
        )


def test_multiple_dataset_scope_for_one_source_fails_closed(database):
    assert selected_evidence(database, (("source-a", "dataset-R1"), ("source-a", "dataset-R2"))) == []


def test_source_root_ambiguity_across_recorded_endpoints_fails_closed(database):
    database.execute(
        "UPDATE mrf.provider_directory_endpoint_dataset SET acquisition_root_run_id='root-R1',endpoint_id='endpoint-new' WHERE dataset_id='dataset-R2'"
    )
    sql = f"WITH {npi._directory_current_dataset_ctes_sql('mrf')} SELECT * FROM current_datasets"
    assert database.execute(sqlite_sql(sql)).fetchall() == []


@pytest.mark.asyncio
async def test_profile_only_fallback_uses_exact_existing_history_vector(database, monkeypatch):
    database.execute("DELETE FROM mrf.provider_directory_address_overlay")
    historical = AsyncMock(return_value=(("source-a", "dataset-R1"),))
    monkeypatch.setattr(lineage, "profile_source_datasets", historical)
    monkeypatch.setattr(npi, "_execute_stmt", executor(database))
    address_by_field = {
        "npi": 1234567890,
        "address_key": "00000000-0000-0000-0000-000000000001",
        "source_record_ids": ["provider_directory_fhir:practitioner_role:source-a:role-a:location-a"],
    }
    generation = "pdprofile_" + "a" * 32
    bindings, role_datasets, _ = await lineage.resolve_address_evidence(
        npi, [address_by_field], profile_generation_id=generation, session=object()
    )
    assert historical.await_args.args == (npi, generation)
    assert bindings["source-a"][0] == "endpoint-old"
    assert selected_evidence(database, role_datasets) == [("root-R1", "R1", "Plan R1", "Network R1")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("address_key", "00000000-0000-0000-0000-000000000003"), ("npi", 9999999999), ("npi", None)]
)
async def test_stable_record_preserves_exact_office_and_npi(database, monkeypatch, field, value):
    monkeypatch.setattr(npi, "_execute_stmt", executor(database))
    address_by_field = {
        "npi": 1234567890,
        "address_key": "00000000-0000-0000-0000-000000000001",
        "source_record_ids": ["provider_directory_fhir:practitioner_role:source-a:role-a:location-a"],
    }
    address_by_field[field] = value
    with pytest.raises(RuntimeError, match="lineage_unavailable"):
        await lineage.resolve_address_evidence(npi, [address_by_field], session=object())


def test_legacy_coalesced_root_collision_fails_closed(database):
    database.execute(
        "UPDATE mrf.provider_directory_endpoint_dataset SET acquisition_root_run_id=NULL,import_run_id='root-R1' WHERE dataset_id='dataset-R2'"
    )
    sql = f"WITH {npi._directory_current_dataset_ctes_sql('mrf')} SELECT * FROM current_datasets"
    assert database.execute(sqlite_sql(sql)).fetchall() == []


@pytest.mark.asyncio
async def test_genuine_profile_history_can_bind_header_without_legacy_source_list(database, monkeypatch):
    database.execute("DELETE FROM mrf.provider_directory_address_overlay")
    database.execute(
        "UPDATE mrf.provider_directory_endpoint_dataset SET publication_metadata_json='{}',publication_metadata_summary_json=NULL WHERE dataset_id='dataset-R1'"
    )
    monkeypatch.setattr(lineage, "profile_source_datasets", AsyncMock(return_value=(("source-a", "dataset-R1"),)))
    monkeypatch.setattr(npi, "_execute_stmt", executor(database))
    address_by_field = {"source_record_ids": ["provider_directory_fhir:practitioner_role:source-a:role-a:location-a"]}
    bindings, source_datasets, _ = await lineage.resolve_address_evidence(
        npi, [address_by_field], profile_generation_id="pdprofile_" + "a" * 32, session=object()
    )
    assert bindings["source-a"][0] == "endpoint-old"
    assert selected_evidence(database, source_datasets) == [("root-R1", "R1", "Plan R1", "Network R1")]


@pytest.mark.asyncio
async def test_managed_network_scope_bypasses_legacy_lineage(monkeypatch):
    attach = provider_detail_network_metadata.attach_provider_source_details
    monkeypatch.setattr(provider_detail_network_metadata, "current_network_address_scope", lambda: object())
    legacy = AsyncMock(side_effect=AssertionError("managed scope cannot use legacy lineage"))
    monkeypatch.setattr(npi, "_attach_provider_directory_source_details", legacy)
    await attach([{}], include_sources=True, include_role_evidence=True, profile_generation_id="managed-hash")
    legacy.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_legacy_lineage_is_explicit_and_never_current_fallback(monkeypatch):
    from sanic.exceptions import ServiceUnavailable

    attach = provider_detail_network_metadata.attach_provider_source_details
    monkeypatch.setattr(provider_detail_network_metadata, "current_network_address_scope", lambda: None)
    legacy = AsyncMock(side_effect=RuntimeError("provider_directory_profile_lineage_unavailable"))
    monkeypatch.setattr(npi, "_attach_provider_directory_source_details", legacy)
    generation = "pdprofile_" + "a" * 32
    with pytest.raises(ServiceUnavailable, match="evidence is unavailable"):
        await attach([{}], include_sources=False, include_role_evidence=True, profile_generation_id=generation)
    assert legacy.await_args.kwargs["profile_generation_id"] == generation


def _scoped_evidence_sql(kind, indexes):
    if kind == "affiliation":
        return npi._provider_directory_affiliation_evidence_sql(
            "mrf", True, has_dataset_scope=True, has_dataset_readiness_indexes=indexes
        )
    from collections import defaultdict

    return dataset_sql._scoped_role_evidence_sql(npi, "mrf", defaultdict(lambda: True))


@pytest.mark.asyncio
async def test_explicit_phone_evidence_keeps_its_exact_other_office_lineage(database, monkeypatch):
    monkeypatch.setattr(npi, "_execute_stmt", executor(database))
    address_by_field = {
        "npi": 1234567890,
        "address_key": "00000000-0000-0000-0000-000000000002",
        "phone_source_record_ids": ["provider_directory_fhir:practitioner_role:source-a:role-a:location-a"],
    }
    bindings, role_datasets, _ = await lineage.resolve_address_evidence(
        npi, [address_by_field], phone_digits="8005550100", session=object()
    )
    assert bindings["source-a"][0] == "endpoint-old"
    assert selected_evidence(database, role_datasets) == [("root-R1", "R1", "Plan R1", "Network R1")]
    with pytest.raises(RuntimeError, match="lineage_unavailable"):
        await lineage.resolve_address_evidence(npi, [address_by_field], phone_digits="8005550101", session=object())
