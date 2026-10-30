# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native common delta projections and inverse preserve actual model types and row history."""

import datetime
import importlib
import json
import uuid
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, insert, text
from sqlalchemy.schema import CreateTable

from process import entity_address_affected_patch as affected
from process import entity_address_affected_support as support
from process import entity_address_snapshot_receipt as row_receipts
from process import provider_directory_common_serving_inverse as inverse
from process import provider_directory_common_serving_postimages as postimages
from process import provider_directory_ordinary_retention_capacity as capacity
from process import provider_directory_ordinary_row_retention as retention
from process import provider_directory_profile as profile
from tests.test_provider_directory_ordinary_row_retention_db import _native_owner, _target

native = importlib.import_module("process.entity_address_unified")
fhir = importlib.import_module("process.provider_directory_fhir")
_NPI, _OTHER_NPI = 1234567890, 2234567890
_JSON = '{ "z": 1, "a": [2, 3], "same": {"x": true} }'


async def _create_real_roots(database, session, schema):
    for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS):
        table = model.__table__.to_metadata(MetaData(), schema=schema)
        await session.execute(CreateTable(table))
    await database.status(profile.profile_table_sql(schema, logged=True))
    await database.status(profile.profile_evidence_table_sql(schema, logged=True))
    await database.status(fhir.provider_directory_address_overlay_table_sql(schema))
    for root in postimages.documents.FAMILIES.values():
        await database.status(f"CREATE TABLE {schema}.stage_{root} (LIKE {schema}.{root} INCLUDING ALL)")
    await database.status(
        f"CREATE TABLE {schema}.reappear_facility (LIKE {schema}.facility_anchor_npi_candidate INCLUDING ALL)"
    )


async def _model_row(session, schema, model, values, *, stage=False):
    name = ("stage_" if stage else "") + model.__tablename__
    table = model.__table__.to_metadata(MetaData(), schema=schema, name=name)
    await session.execute(insert(table), values)


async def _seed_old_main(session, schema, location, ordinal):
    npi = _OTHER_NPI if location == "other" else _NPI
    await _model_row(
        session,
        schema,
        native.EntityAddressUnified,
        dict(
            entity_type="facility_anchor",
            entity_id="facility-" + location,
            npi=npi,
            location_key=location,
            checksum=ordinal,
            type="practice",
        ),
    )
    await _model_row(
        session,
        schema,
        native.EntityAddressEvidence,
        dict(
            evidence_id=ordinal,
            location_key=location,
            entity_type="npi",
            entity_id=str(npi),
            npi=npi,
            source_id=1,
            source_run_id="run-old",
        ),
    )


async def _seed_old_support(session, schema, location):
    npi = _OTHER_NPI if location == "other" else _NPI
    for model, extra in (
        (native.EntityAddressPlanBridge, dict(entity_type="npi", entity_id=str(npi), plan_id="old-plan")),
        (native.EntityAddressNetworkBridge, dict(entity_type="npi", entity_id=str(npi), network_id="old-network")),
        (native.EntityAddressProcedureBridge, dict(npi=npi, code_system="test", code="old-code")),
        (native.EntityAddressMedicationBridge, dict(npi=npi, code_system="test", code="old-code")),
    ):
        await _model_row(session, schema, model, dict(location_key=location, **extra))
    await _model_row(
        session,
        schema,
        native.FacilityAnchorNPICandidate,
        dict(
            candidate_id="review-" + location,
            location_key=location,
            facility_anchor_id="facility-" + location,
            candidate_npi=npi,
            candidate_method="exact",
            candidate_status="single_candidate",
            review_status="approved",
            reviewed_by="reviewer",
            reviewed_at=datetime.datetime(2025, 1, 1),
            source_run_id="run-old",
        ),
    )


async def _seed_candidates(database, session, schema):
    for ordinal, location in enumerate(("same", "new"), 10):
        await _model_row(
            session,
            schema,
            native.EntityAddressUnified,
            dict(
                entity_type="facility_anchor",
                entity_id="facility-" + location,
                npi=_NPI,
                location_key=location,
                checksum=ordinal,
                type="practice",
                aca_plan_array=["new-plan"],
                plans_network_array=[17],
                procedures_array=[18],
                medications_array=[19],
            ),
            stage=True,
        )
    for location in ("same", "new"):
        await _model_row(
            session,
            schema,
            native.FacilityAnchorNPICandidate,
            dict(
                candidate_id="new-" + location,
                location_key=location,
                facility_anchor_id="facility-" + location,
                candidate_npi=_NPI,
                candidate_method="exact",
                candidate_status="single_candidate",
                review_status="needs_review",
                evidence={"new": True},
                source_run_id="run-new",
                updated_at=datetime.datetime(2026, 1, 2),
            ),
            stage=True,
        )
    await database.status(
        f"INSERT INTO {schema}.reappear_facility SELECT * FROM {schema}.stage_facility_anchor_npi_candidate WHERE location_key='same'"
    )
    await database.status(
        f"UPDATE {schema}.reappear_facility SET candidate_id='reappearing',location_key='gone',facility_anchor_id='facility-gone',source_run_id='run-reappear'"
    )


async def _seed_address_rows(database, session, schema):
    for ordinal, location in enumerate(("gone", "same", "other"), 1):
        await _seed_old_main(session, schema, location, ordinal)
        await _seed_old_support(session, schema, location)
    await _model_row(
        session,
        schema,
        native.EntityAddressEvidence,
        dict(
            evidence_id=4,
            location_key="same",
            entity_type="npi",
            entity_id=str(_NPI),
            npi=_NPI,
            source_id=1,
            source_run_id="run-history",
            retired_at=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
        ),
    )
    await database.status(
        f"UPDATE {schema}.facility_anchor_npi_candidate SET evidence=CAST(:payload AS json)", payload=_JSON
    )
    await _seed_candidates(database, session, schema)


async def _seed_profile_overlay(database, schema):
    for npi, table, generation in (
        (_NPI, "provider_directory_profile", "old"),
        (_OTHER_NPI, "provider_directory_profile", "other"),
        (_NPI, "stage_provider_directory_profile", "new"),
    ):
        await database.status(
            f"INSERT INTO {schema}.{table} VALUES(:npi,CAST(:profile AS jsonb),CAST(:evidence AS jsonb),ARRAY['changed']::varchar[],ARRAY['endpoint']::varchar[],ARRAY['dataset']::varchar[],1,1,1,:generation,TIMESTAMP '2026-01-01 00:00:00')",
            npi=npi,
            profile=json.dumps({"generation": generation, "null": None}),
            evidence='[{"fact":true}]',
            generation="pdprofile_" + generation,
        )
    for key, source_id, npi, table in (
        ("a", "changed", _NPI, "provider_directory_profile_evidence"),
        ("b", "sibling", _NPI, "provider_directory_profile_evidence"),
        ("c", "removed", _NPI, "provider_directory_profile_evidence"),
        ("d", "other", _OTHER_NPI, "provider_directory_profile_evidence"),
        ("e", "changed", _NPI, "stage_provider_directory_profile_evidence"),
    ):
        await database.status(
            f"INSERT INTO {schema}.{table}(evidence_key,npi,fact_type,fact_key,value_json,source_id,endpoint_id,dataset_id,resource_type,resource_id) VALUES(:key,:npi,'office',:key,CAST(:payload AS jsonb),:source_id,'endpoint','dataset','Location',:source_id)",
            key=key * 32,
            npi=npi,
            source_id=source_id,
            payload=_JSON,
        )
    for record_id, source_id, npi, table in (
        ("old", "changed", _NPI, "provider_directory_address_overlay"),
        ("removed", "removed", _NPI, "provider_directory_address_overlay"),
        ("other", "other", _OTHER_NPI, "provider_directory_address_overlay"),
        ("new", "changed", _NPI, "stage_provider_directory_address_overlay"),
    ):
        await database.status(
            f"INSERT INTO {schema}.{table}(source_record_id,source_id,last_seen_run_id,resource_type,resource_id,npi,address_key,lat,long) VALUES(:record_id,:source_id,'run','Location',:record_id,:npi,CAST(:address AS uuid),30.125,-97.25)",
            record_id=record_id,
            source_id=source_id,
            npi=npi,
            address=str(uuid.uuid5(uuid.NAMESPACE_URL, record_id)),
        )


async def _retained_copy(prepared, origin, schema, kind, family, source, keys, selection=("TRUE", ())):
    predicate, arguments = selection
    name = "pd_" + family + "_before_" + uuid.uuid4().hex[:20]
    source_oid = await prepared.fhir.db.scalar(
        "SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{source}"
    )
    target = await _target(prepared.fhir.db, schema, source, name)
    columns = await retention._columns(origin[1], target.oid)
    copied = await retention._copy_rows(
        prepared, origin, f'"{schema}"."{source}"', target, columns, keys, predicate, arguments
    )
    await prepared.fhir.db.status(f"ALTER TABLE {schema}.{name} SET LOGGED")
    target = retention.preparation.OwnedRelation(schema, name, target.oid, 0, "p")
    layout = await (
        capacity.retained_scope_layout_sha256(prepared.fhir, family.removesuffix("_scope"), target)
        if kind == "scope"
        else capacity.retention_layout_sha256(prepared.fhir, target)
    )
    count, digest = await row_receipts._row_identity(origin[0], schema, name)
    return retention.RetainedRows(
        kind, family, schema, source, source_oid, name, target.oid, layout, columns, count, digest, copied
    )


async def _retain_beforeimages(prepared, origin, schema):
    database = prepared.fhir.db
    await database.status(f"CREATE TABLE {schema}.npi_scope(npi bigint PRIMARY KEY)")
    await database.status(f"INSERT INTO {schema}.npi_scope VALUES({_NPI})")
    await database.status(f"CREATE TABLE {schema}.location_scope(location_key varchar(64) PRIMARY KEY)")
    await database.status(f"INSERT INTO {schema}.location_scope VALUES('gone'),('same'),('new')")
    await database.status(
        f"CREATE TABLE {schema}.group_scope(entity_type varchar,entity_id varchar,entity_npi bigint,address_key uuid,street_key varchar,city_key varchar,state_key varchar,zip_key varchar,country_key varchar)"
    )
    await database.status(
        f"CREATE INDEX group_scope_idx ON {schema}.group_scope(entity_npi,entity_type,entity_id,address_key,street_key,city_key,state_key,zip_key,country_key)"
    )
    scopes = []
    for kind, keys in (("npi", ("npi",)), ("location", ("location_key",)), ("group", ("ctid",))):
        scopes.append(await _retained_copy(prepared, origin, schema, "scope", kind + "_scope", kind + "_scope", keys))
    families = []
    for family, (source_table, keys) in capacity._FAMILIES.items():
        if family in {"profile", "evidence"}:
            predicate, arguments = f"live.npi={_NPI}", ()
        elif family == "overlay":
            predicate, arguments = "live.source_id=ANY($1::varchar[])", (["changed", "removed"],)
        else:
            predicate, arguments = "live.location_key IN ('gone','same','new')", ()
        families.append(
            await _retained_copy(
                prepared, origin, schema, "family", family, source_table, tuple(keys), (predicate, arguments)
            )
        )
    return dict(
        contract_id="provider-directory-ordinary-retained-rows.v1",
        affected_scope_sha256="a" * 64,
        retention_plan_sha256="b" * 64,
        creator=origin[2],
        scopes=[asdict(entry) for entry in scopes],
        families=[asdict(entry) for entry in families],
    )


async def _native_identity(database, schema):
    return await database.all(
        "SELECT c.relname,c.oid::bigint,ARRAY(SELECT indexrelid::bigint FROM pg_index WHERE indrelid=c.oid ORDER BY indexrelid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND c.relname=ANY(CAST(:roots AS varchar[])) ORDER BY c.relname",
        schema=schema,
        roots=list(postimages.documents.FAMILIES.values()),
    )


async def _complete_identity(session, schema):
    async with retention.receipts.normalized_receipt_scope(fhir.db, session, schema):
        return {
            root: await row_receipts._row_identity(session, schema, root)
            for root in postimages.documents.FAMILIES.values()
        }


def _projection_delta(schema):
    """Synthetic producer coordinates exercise SQL, never publication authority."""
    return fhir.ProviderDirectoryPreparedProfileDelta(
        schema=schema,
        build_id="build",
        resume_lineage_hash="a" * 64,
        owner_run_id=None,
        evidence_stage="stage_provider_directory_profile_evidence",
        evidence_stage_oid=1,
        profile_stage="stage_provider_directory_profile",
        profile_stage_oid=2,
        affected_npi_stage="npi_scope",
        affected_npi_stage_oid=3,
        evidence_target_oid=4,
        profile_target_oid=5,
        from_generation_id="old",
        generation_id="new",
        operation="publish",
        selection_proof_id="proof",
        control_generation=1,
        authority_revision=1,
        profile_schema_version=1,
        profile_strategy_version="strategy",
        executable_plan_hash="a" * 64,
        from_source_vector_hash="b" * 64,
        to_source_vector_hash="c" * 64,
        to_source_vector=(),
        from_source_context_vector_hash="d" * 64,
        to_source_context_vector_hash="e" * 64,
        to_source_context_vector=(),
        refresh_source_ids=("changed",),
        removed_source_ids=("removed",),
        expected_evidence_rows=1,
        expected_profile_rows=1,
        profile_as_of="2026-01-01",
        from_capacity_geometry_status="unknown",
        from_capacity_geometry_hash=None,
        from_capacity_geometry_json=None,
        capacity_geometry_status="bounded",
        capacity_geometry_hash="f" * 64,
        capacity_geometry_json="{}",
    )


def _expected_queries(prepared, schema, payload, descriptor):
    queries = postimages.current_postimage_queries(schema, payload, ["changed", "removed"])
    projected = SimpleNamespace(
        fhir=prepared.fhir, profile_delta=_projection_delta(schema), profile_bundle=SimpleNamespace(stages=[])
    )
    postimages._profile_queries(projected, queries)
    for family, root in postimages.documents.FAMILIES.items():
        if family not in {"profile", "evidence", "address_evidence", "facility_candidate"}:
            queries[family] = postimages._direct_query(schema, root, "stage_" + root)
    queries["address_evidence"] = postimages._address_evidence_query(native, descriptor, queries["address_evidence"])
    queries["facility_candidate"] = postimages._facility_query(native, descriptor, queries["facility_candidate"])
    return queries


async def _apply_common_rows(database, schema, descriptor):
    for root, predicate in (
        ("provider_directory_profile_evidence", "source_id IN ('changed','removed')"),
        ("provider_directory_profile", f"npi={_NPI}"),
        ("provider_directory_address_overlay", "source_id IN ('changed','removed')"),
        ("entity_address_unified", "location_key IN ('gone','same','new')"),
    ):
        await database.status(f"DELETE FROM {schema}.{root} WHERE {predicate}")
        await database.status(f"INSERT INTO {schema}.{root} SELECT * FROM {schema}.stage_{root}")
    for statement in support.apply_statements(native, descriptor, {}):
        await database.status(statement)


async def _prepare_outputs(database, schema):
    descriptor = SimpleNamespace(
        schema=schema,
        stage_table="stage_entity_address_unified",
        affected_location_table="location_scope",
        support_stages=tuple(
            [
                (
                    model.__main_table__,
                    "stage_" + model.__main_table__,
                    await database.scalar(
                        "SELECT to_regclass(:relation)::oid::bigint",
                        relation=f"{schema}.stage_{model.__main_table__}",
                    ),
                )
                for model in native.SUPPORT_TABLE_MODELS
            ]
        ),
        facility_generated=True,
        transition_at=await affected.preparation_timestamp(native),
    )
    await database.status(
        native._evidence_from_stage_sql(
            schema,
            "stage_entity_address_evidence",
            descriptor.stage_table,
            source_run_id="run-new",
            node_id=None,
            evidence_id_floor=4,
        )
    )
    for model, generator in (
        (native.EntityAddressPlanBridge, native._plan_bridge_sql),
        (native.EntityAddressNetworkBridge, native._network_bridge_sql),
        (native.EntityAddressProcedureBridge, native._procedure_bridge_sql),
        (native.EntityAddressMedicationBridge, native._medication_bridge_sql),
    ):
        await database.status(generator(schema, "stage_" + model.__main_table__, descriptor.stage_table))
    await support._reconcile_prepared_facility(native, descriptor, "stage_facility_anchor_npi_candidate")
    return descriptor


async def _assert_review_history(database, session, schema, descriptor, current_queries, expected):
    assert (
        await database.scalar(
            f"SELECT candidate_status FROM {schema}.facility_anchor_npi_candidate WHERE candidate_id='review-gone'"
        )
        == "inactive"
    )
    assert (
        await database.scalar(
            f"SELECT reviewed_by FROM {schema}.facility_anchor_npi_candidate WHERE candidate_id='review-same'"
        )
        == "reviewer"
    )
    await support._reconcile_prepared_facility(native, descriptor, "reappear_facility")
    assert await database.scalar(f"SELECT candidate_id FROM {schema}.reappear_facility") == "review-gone"
    await database.status("SAVEPOINT changed_review")
    await database.status(
        f"UPDATE {schema}.facility_anchor_npi_candidate SET reviewed_by='new-reviewer' WHERE candidate_id='review-same'"
    )
    assert await postimages.capture_postimages(session, current_queries) != expected
    await database.status("ROLLBACK TO SAVEPOINT changed_review")


@pytest.mark.asyncio
async def test_native_common_postimages_inverse_removal_reappearance(monkeypatch):
    # The reused fixture is a component SQL owner; full admission/producer proof is separate.
    async with _native_owner(monkeypatch) as (prepared, database, schema):
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(fhir, "_schema", lambda: schema)
        monkeypatch.setattr(native, "db", database)
        prepared.fhir = fhir
        async with database.transaction() as session:
            await _create_real_roots(database, session, schema)
            await _seed_address_rows(database, session, schema)
            await _seed_profile_overlay(database, schema)
            origin = await retention._origin(prepared)
            retention_payload = await _retain_beforeimages(prepared, origin, schema)
            descriptor = await _prepare_outputs(database, schema)
            expected = await postimages.capture_postimages(
                session, _expected_queries(prepared, schema, retention_payload, descriptor)
            )
            baseline = await _complete_identity(session, schema)
            native_before = await _native_identity(database, schema)
        async with database.transaction() as session:
            await database.status("SET LOCAL TimeZone TO 'Pacific/Honolulu'")
            await _apply_common_rows(database, schema, descriptor)
            current_queries = postimages.current_postimage_queries(schema, retention_payload, ["changed", "removed"])
            assert await postimages.capture_postimages(session, current_queries) == expected
            assert await database.scalar("SHOW TimeZone") == "Pacific/Honolulu"
            await _assert_review_history(database, session, schema, descriptor, current_queries, expected)
            for restore in inverse.restore_rows_sql(schema, retention_payload, ["changed", "removed"]):
                await database.status(restore.delete_sql, **dict(restore.parameters))
            for restore in inverse.restore_rows_sql(schema, retention_payload, ["changed", "removed"]):
                await database.status(restore.insert_sql)
            assert await _complete_identity(session, schema) == baseline
            assert await database.scalar("SHOW TimeZone") == "Pacific/Honolulu"
            assert await _native_identity(database, schema) == native_before
            assert (
                await database.scalar(
                    f"SELECT evidence::text FROM {schema}.facility_anchor_npi_candidate WHERE candidate_id='review-same'"
                )
                == _JSON
            )
