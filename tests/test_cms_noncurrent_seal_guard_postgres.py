# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native header lifecycle/admission component, not full CMS publication proof."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateColumn

from db import migration_cms_noncurrent_seal as migration
from db.models import ProviderDirectoryDatasetResource, ProviderDirectoryEndpointDataset, ProviderDirectorySource
from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_admission_seal import admission_seal_from_validated_metadata
from tests.cms_noncurrent_seal_postgres_support import install_native_header_guards
from tests.test_provider_directory_cms_prepared_context_postgres import native_case as native_case
from tests.test_provider_directory_dataset_selection_bounded_db import _large_metadata_by_field


@pytest_asyncio.fixture
async def header_case(native_case):
    case = native_case
    case.header = storage_relation(case.storage.catalog_schema, "provider_directory_endpoint_dataset")
    columns = ",".join(
        str(CreateColumn(Column(column.name, column.type, nullable=column.nullable)).compile(dialect=dialect()))
        for column in ProviderDirectoryEndpointDataset.__table__.columns
    )
    await case.admin.execute(f"CREATE TABLE {case.header}({columns},PRIMARY KEY(dataset_id))")
    for model in (ProviderDirectorySource, ProviderDirectoryDatasetResource):
        relation = storage_relation(case.storage.catalog_schema, model.__tablename__)
        columns = ",".join(
            str(CreateColumn(Column(column.name, column.type, nullable=column.nullable)).compile(dialect=dialect()))
            for column in model.__table__.columns
        )
        await case.admin.execute(f"CREATE TABLE {relation}({columns})")
        await case.admin.execute(f"ALTER TABLE {relation} OWNER TO {storage_identifier(case.roles['namespace'])}")
    case.guard = await install_native_header_guards(
        case.admin, case.storage.catalog_schema, owner_role=case.roles["namespace"]
    )
    return case


async def _upgrade(case, *, rollback=False, expected=None):
    engine = create_async_engine(case.url.set(drivername="postgresql+asyncpg"))
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql("SET LOCAL search_path=pg_catalog,pg_temp")
            await connection.run_sync(
                lambda sync: migration.upgrade_cms_noncurrent_seal(
                    sync, case.storage.catalog_schema, expected=case.guard if expected is None else expected
                )
            )
            if rollback:
                raise RuntimeError("outer upgrade rollback")
    finally:
        await engine.dispose()


async def _metadata_digest(case, header_by_field):
    return await case.admin.fetchval(
        "SELECT "
        + storage_relation(case.storage.catalog_schema, "provider_directory_endpoint_dataset_admission_metadata_sha256")
        + "($1::jsonb,$2::smallint,$3::text,$4::text,$5::varchar[])",
        header_by_field["publication_metadata_summary_json"],
        header_by_field["content_proof_admission_version"],
        header_by_field["content_proof_admission_kind"],
        header_by_field["content_proof_admission_sha256"],
        header_by_field["content_proof_resource_types"],
    )


async def _seed(case, *, dataset="candidate", metadata_update=None, header_update=None):
    """Supply an admitted synthetic header; full content production is a separate proof."""
    metadata = _large_metadata_by_field(1)
    resources = [
        "Endpoint",
        "HealthcareService",
        "InsurancePlan",
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    ]
    metadata.update(
        source_ids=["cms-npd"],
        selected_resources=resources,
        expected_resources=resources,
        source_release={"source_id": "cms-npd", "vector_sha256": "a" * 64, "manifest_sha256": "b" * 64},
    )
    proof = metadata["provider_directory_content_proof_v1"]
    proof["resource_count"] = len(resources)
    proof["resource_counts"] = {kind: 1 for kind in resources}
    proof["resource_hashes"] = {kind: "a" * 64 for kind in resources}
    proof["shards"][0]["resource_counts"] = proof["resource_counts"]
    proof["shards"][0]["resource_count"] = len(resources)
    if metadata_update:
        metadata_update(metadata)
    seal = admission_seal_from_validated_metadata(metadata)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    header_by_field = dict(
        dataset_id=dataset,
        endpoint_id="synthetic-endpoint",
        status="acquiring",
        is_current=False,
        resource_count=len(resources),
        dataset_hash="e" * 64,
        created_at=now - timedelta(seconds=1),
        publication_metadata_json=json.dumps(metadata),
        publication_metadata_summary_json=json.dumps(seal.metadata_summary),
        publication_metadata_sha256=seal.metadata_sha256,
        content_proof_admission_version=seal.admission_version,
        content_proof_admission_kind=seal.admission_kind,
        content_proof_admission_sha256=seal.proof_sha256,
        content_proof_resource_types=list(seal.resource_types),
    )
    if header_update:
        header_by_field.update(header_update)
    # Recompute through the real native digest function for declared boundary variants.
    header_by_field["publication_metadata_sha256"] = await _metadata_digest(case, header_by_field)
    fields = ",".join(storage_identifier(key) for key in header_by_field)
    parameters = ",".join("$" + str(index) for index in range(1, len(header_by_field) + 1))
    await case.admin.execute(f"INSERT INTO {case.header}({fields}) VALUES({parameters})", *header_by_field.values())
    await case.admin.execute(
        f"UPDATE {case.header} SET status='validated',validated_at=$2 WHERE dataset_id=$1", dataset, now
    )


async def _seal(case, connection=None, suffix="", instant="statement_timestamp() AT TIME ZONE 'UTC'"):
    return await (connection or case.admin).fetchrow(
        f"UPDATE {case.header} AS c SET status='published',published_at={instant}"
        + suffix
        + " WHERE dataset_id='candidate' RETURNING to_jsonb(c) AS header,c.published_at,"
        "statement_timestamp() AT TIME ZONE 'UTC' AS instant"
    )


async def _metadata(case):
    return await case.admin.fetchval(
        "SELECT jsonb_build_object('function',to_jsonb(p)-'prosrc','description',obj_description(p.oid,'pg_proc'),"
        "'hooks',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t WHERE tgfoid=p.oid)) "
        "FROM pg_proc p WHERE p.oid=$1",
        case.guard["oid"],
    )


async def test_actual_old_guard_refuses_and_checked_successor_preserves_identity_and_rollback(header_case):
    case = header_case
    await _seed(case)
    before = await _metadata(case)
    with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
        await _seal(case)
    with pytest.raises(RuntimeError, match="outer upgrade rollback"):
        await _upgrade(case, rollback=True)
    assert await _metadata(case) == before
    assert (
        await case.admin.fetchval("SELECT prosrc FROM pg_proc WHERE oid=$1", case.guard["oid"])
        == (migration.lifecycle_guard_bodies(case.storage.catalog_schema)[0])
    )
    with pytest.raises(RuntimeError, match="function_changed"):
        await _upgrade(case, expected={**case.guard, "owner_oid": case.guard["owner_oid"] + 1})
    await _upgrade(case)
    assert await _metadata(case) == before
    await _upgrade(case)
    assert await _metadata(case) == before
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL timezone='America/Los_Angeles'")
        native = await _seal(case)
        sealed = json.loads(native["header"])
        assert sealed["status"] == "published" and sealed["is_current"] is False
        assert native["published_at"] == native["instant"]
        await case.admin.execute(f"UPDATE {case.header} SET is_current=true WHERE dataset_id='candidate'")
        assert await case.admin.fetchval(f"SELECT is_current FROM {case.header}") is True
    with pytest.raises(RuntimeError, match="outer rollback"):
        async with case.admin.transaction():
            await _seed(case, dataset="rolled-back")
            raise RuntimeError("outer rollback")
    assert await case.admin.fetchval(f"SELECT count(*) FROM {case.header} WHERE dataset_id='rolled-back'") == 0


async def test_new_transitions_refuse_assumed_roles_mutation_and_wrong_state(header_case):
    case = header_case
    await _upgrade(case)
    await _seed(case)
    for suffix in (
        ",is_current=true",
        ",validated_at=validated_at+interval '1 second'",
        ",resource_count=2",
    ):
        with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
            async with case.admin.transaction():
                await _seal(case, suffix=suffix)
    with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
        async with case.admin.transaction():
            await _seal(case, instant="(transaction_timestamp() AT TIME ZONE 'UTC')-interval '1 second'")
    role = storage_identifier(case.roles["prepared_publisher"])
    await case.admin.execute(f"GRANT SELECT,UPDATE(status,published_at,is_current) ON {case.header} TO {role}")
    publisher = await asyncpg.connect(
        case.url.set(username=case.roles["prepared_publisher"], password=case.password).render_as_string(
            hide_password=False
        )
    )
    try:
        with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
            await _seal(case, connection=publisher)
    finally:
        await publisher.close()
    with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
        async with case.admin.transaction():
            actor = await case.admin.fetchval("SELECT session_user::text")
            await case.admin.execute("SET LOCAL ROLE " + storage_identifier(actor))
            await _seal(case)
    await _seal(case)
    for mutation in ("status='superseded'", "published_at=published_at+interval '1 second'", "resource_count=2"):
        with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
            async with case.admin.transaction():
                await case.admin.execute(
                    f"UPDATE {case.header} SET is_current=true,{mutation} WHERE dataset_id='candidate'"
                )


async def test_new_branch_requires_exact_cms_release_and_generic_admission(header_case):
    case = header_case
    await _upgrade(case)
    cases = (
        (lambda metadata: metadata.update(source_ids=["other-source"]), None),
        (lambda metadata: metadata["source_release"].update(source_id="other-source"), None),
        (lambda metadata: metadata["source_release"].update(vector_sha256="invalid"), None),
        (lambda metadata: metadata["source_release"].pop("manifest_sha256"), None),
        (None, {"content_proof_admission_kind": "uhc_canonical"}),
        (None, {"content_proof_resource_types": ["Location"]}),
    )
    for change, header in cases:
        with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
            async with case.admin.transaction():
                await _seed(case, metadata_update=change, header_update=deepcopy(header))
                await _seal(case)


async def test_ordinary_lifecycle_keeps_its_transaction_timestamp_rule(header_case):
    case = header_case
    await _upgrade(case)
    await _seed(case, metadata_update=lambda metadata: metadata.update(source_ids=["ordinary-source"]))
    async with case.admin.transaction():
        await case.admin.execute(
            f"UPDATE {case.header} SET status='published',is_current=true,published_at=transaction_timestamp() "
            "WHERE dataset_id='candidate'"
        )
        await case.admin.execute(
            f"UPDATE {case.header} SET status='superseded',is_current=false,superseded_at=transaction_timestamp() "
            "WHERE dataset_id='candidate'"
        )
    assert await case.admin.fetchval(f"SELECT status FROM {case.header}") == "superseded"


async def test_forward_upgrade_refuses_overloads_and_attribute_drift_and_preserves_origin_hook(header_case):
    case = header_case
    guard = storage_relation(case.storage.catalog_schema, migration.GUARD)
    await case.admin.execute(f"CREATE FUNCTION {guard}(integer) RETURNS integer LANGUAGE sql AS 'SELECT $1'")
    with pytest.raises(RuntimeError, match="overload_changed"):
        await _upgrade(case)
    await case.admin.execute(f"DROP FUNCTION {guard}(integer)")
    await case.admin.execute(f"ALTER FUNCTION {guard}() COST 101")
    with pytest.raises(RuntimeError, match="function_properties_changed"):
        await _upgrade(case)
    await case.admin.execute(f"ALTER FUNCTION {guard}() COST 100")
    await case.admin.execute(f"ALTER TABLE {case.header} ENABLE TRIGGER tin_npi_connector_endpoint_dataset_guard")
    before = await _metadata(case)
    await _upgrade(case)
    assert await _metadata(case) == before
    assert await case.admin.fetchval("SELECT tgenabled::text FROM pg_trigger WHERE tgfoid=$1", case.guard["oid"]) == "O"
