# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed canonical layouts from authentic migrations, without Source adoption."""

import hashlib
import json
import os
from pathlib import Path
from time import monotonic

import asyncpg
import pytest
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Index, MetaData
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import CMSDoctorEducation, DoctorClinicianAddress
from process.provider_directory_cms_canonical_layout import CONTRACT, require_cms_canonical_layout
from tests.cms_npd_admission_postgres_support import _owned_database, _run_migrations
from tests.nppes_candidate_postgres_support import (
    install_source_canonical_functions,
    nppes_source_component_migrations,
)
from tests.provider_directory_source_postgres_support import source_component_migrations

pytestmark = pytest.mark.asyncio(loop_scope="module")


class NoIO:
    def is_in_transaction(self):
        return False

    async def fetchrow(self, *_args):
        raise AssertionError("unexpected catalog read")


async def test_unknown_storage_class_refuses_before_catalog_io():
    with pytest.raises(ValueError, match="storage_class"):
        await require_cms_canonical_layout(NoIO(), catalog_schema="example", storage_class="completed-snapshot")


async def test_explicit_transaction_is_required_before_catalog_io():
    with pytest.raises(ValueError, match="transaction"):
        await require_cms_canonical_layout(NoIO(), catalog_schema="example", storage_class=CONTRACT)


def _migrations():
    """Extend the existing genuine dependency slice in actual ancestry order."""
    root = Path(__file__).parents[1]
    selected = set(nppes_source_component_migrations()) | {
        "20261007150000",
        "20261017210000",
        "20261022120000",
        "20261023100000_provider_directory_retirement_parent_updates",
        "20261024050000",
    }
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    nodes = tuple(reversed(tuple(ScriptDirectory.from_config(config).walk_revisions())))
    nodes_by_prefix = {prefix: [n for n in nodes if Path(n.path).name.startswith(prefix)] for prefix in selected}
    nodes_by_prefix = {
        prefix: ([n for n in found if "provider_directory" in Path(n.path).name] if len(found) > 1 else found)
        for prefix, found in nodes_by_prefix.items()
    }
    assert all(len(found) == 1 for found in nodes_by_prefix.values())
    by_revision = {found[0].revision: prefix for prefix, found in nodes_by_prefix.items()}
    prefixes = tuple(by_revision[n.revision] for n in nodes if n.revision in by_revision)
    baseline = source_component_migrations()
    assert len(prefixes) == len(selected) == 115
    assert tuple(prefix for prefix in prefixes if prefix in baseline) == baseline
    return prefixes


async def _create_native_models(engine):
    async with engine.begin() as native:
        await native.exec_driver_sql("CREATE SCHEMA mrf")
        for model in (DoctorClinicianAddress, CMSDoctorEducation):
            table = model.__table__.to_metadata(MetaData(), schema="mrf")
            await native.run_sync(lambda sync: table.create(sync, checkfirst=False))
            for spec in getattr(model, "__my_additional_indexes__", ()):
                index = Index(table.name + "_idx_" + spec["name"], *(table.c[n] for n in spec["index_elements"]))
                await native.run_sync(lambda sync: index.create(sync, checkfirst=False))


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def native_layout():
    raw = os.getenv("HLTHPRT_CMS_LAYOUT_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_CMS_LAYOUT_TEST_DSN for the native fixed-layout proof")
    url = make_url(raw)
    assert url.host in {"127.0.0.1", "localhost"} and url.port and not url.query
    # Existing lifecycle registers exact cleanup before creating its UUID database.
    async with _owned_database(url) as (owned_url, _admin):
        with pytest.MonkeyPatch.context() as settings:
            settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            settings.setenv("DB_SCHEMA", "mrf")
            engine = create_async_engine(owned_url.set(drivername="postgresql+asyncpg"))
            connection = None
            try:
                await _create_native_models(engine)
                prefixes = _migrations()
                boundary = prefixes.index("20261016040000")
                async with engine.connect() as native:
                    await native.run_sync(_run_migrations, prefixes[:boundary])
                await install_source_canonical_functions(engine, "mrf")
                async with engine.connect() as native:
                    await native.run_sync(_run_migrations, prefixes[boundary:])
                connection = await asyncpg.connect(
                    owned_url.set(drivername="postgresql").render_as_string(hide_password=False)
                )
                await connection.execute("SET search_path=pg_catalog")
                yield connection
            finally:
                if connection is not None:
                    await connection.close()
                await engine.dispose()


async def _verify(connection, schema="mrf"):
    return await require_cms_canonical_layout(connection, catalog_schema=schema, storage_class=CONTRACT)


@pytest.mark.parametrize("search_path", ["pg_catalog", "pg_catalog, pg_temp"])
async def test_genuine_source_built_seventeen_roots_are_read_only_layout_evidence(native_layout, search_path):
    connection = native_layout
    async with connection.transaction():
        await connection.execute("SET LOCAL statement_timeout='5s'")
        await connection.fetchval("SELECT pg_catalog.set_config('search_path',$1,true)", search_path)
        before = await connection.fetchrow("SELECT current_setting('statement_timeout'),current_setting('search_path')")
        started = monotonic()
        result = await _verify(connection)
        elapsed = monotonic() - started
        assert result == {"contract": CONTRACT, "root_count": 17, "evidence": "fixed_layout_only"}
        assert connection.is_in_transaction()
        assert before == await connection.fetchrow(
            "SELECT current_setting('statement_timeout'),current_setting('search_path')"
        )
        print(f"FIXED_LAYOUT_NATIVE_SECONDS={elapsed:.6f}")


async def test_schema_qualified_semantics_support_a_different_quoted_schema(native_layout):
    transaction = native_layout.transaction()
    await transaction.start()
    try:
        await native_layout.execute('ALTER SCHEMA mrf RENAME TO "Example_Layout"')
        assert (await _verify(native_layout, "Example_Layout"))["root_count"] == 17
    finally:
        await transaction.rollback()


@pytest.mark.parametrize(
    ("statements", "reason"),
    [
        (["ALTER TABLE mrf.provider_directory_endpoint_dataset ALTER COLUMN is_current SET DEFAULT true"], "columns"),
        (["ALTER TABLE mrf.provider_directory_organization_identity ALTER COLUMN created_at DROP NOT NULL"], "columns"),
        (
            ["ALTER TABLE mrf.provider_directory_cms_npd_resource_witness ALTER COLUMN release_id TYPE varchar(63)"],
            "columns",
        ),
        (["ALTER TABLE mrf.provider_directory_cms_npd_relationship ADD COLUMN unexpected text"], "columns"),
        (
            [
                "ALTER TABLE mrf.provider_directory_cms_npd_relationship DROP COLUMN period_start",
                "ALTER TABLE mrf.provider_directory_cms_npd_relationship ADD COLUMN period_start text",
            ],
            "root_native",
        ),
        (
            [
                "ALTER TABLE mrf.provider_directory_cms_npd_relationship DROP CONSTRAINT provider_directory_cms_npd_relationship_pkey"
            ],
            "constraints",
        ),
        (
            [
                "ALTER TABLE mrf.provider_directory_endpoint_dataset DROP CONSTRAINT pd_endpoint_dataset_completion_digest_check"
            ],
            "constraints",
        ),
        (
            [
                "ALTER TABLE mrf.provider_directory_cms_npd_resource_witness DROP CONSTRAINT provider_directory_cms_npd_re_dataset_id_resource_type_res_fkey"
            ],
            "constraints",
        ),
        (["ALTER TABLE mrf.provider_directory_cms_npd_resource_witness DISABLE TRIGGER ALL"], "constraint_native"),
        (
            [
                "ALTER TABLE mrf.provider_directory_cms_npd_resource_witness ADD CONSTRAINT unexpected CHECK(raw_payload_sha256<>'')"
            ],
            "constraints",
        ),
        (["CREATE UNIQUE INDEX unexpected ON mrf.provider_directory_site_identity(created_at)"], "operator_class"),
        (
            [
                "DROP INDEX mrf.provider_directory_entity_binding_site_idx",
                "CREATE INDEX provider_directory_entity_binding_site_idx ON mrf.provider_directory_entity_source_binding(site_id) WHERE site_id IS NOT NULL",
            ],
            "indexes",
        ),
        (
            [
                "DROP INDEX mrf.pd_resource_identity_seek_idx",
                "CREATE UNIQUE INDEX pd_resource_identity_seek_idx ON mrf.provider_directory_resource_identity(source_id varchar_pattern_ops,resource_type,entity_id)",
            ],
            "operator_class",
        ),
        (
            [
                "ALTER TABLE mrf.provider_directory_dataset_insurance_plan ALTER COLUMN plan_identifier SET EXPRESSION AS (COALESCE(payload_json->>'plan_identifier','changed'))"
            ],
            "columns",
        ),
        (
            [
                "CREATE DOMAIN mrf.synthetic_hash AS varchar(64)",
                "ALTER TABLE mrf.provider_directory_cms_npd_relationship ALTER COLUMN source_payload_hash TYPE mrf.synthetic_hash USING source_payload_hash::mrf.synthetic_hash",
            ],
            "dependencies",
        ),
        (
            [
                'CREATE COLLATION mrf.synthetic_collation FROM pg_catalog."C"',
                "ALTER TABLE mrf.provider_directory_cms_npd_resource_witness ALTER COLUMN raw_payload_sha256 TYPE varchar(64) COLLATE mrf.synthetic_collation",
            ],
            "dependencies",
        ),
        (
            [
                "CREATE OPERATOR mrf.=== (LEFTARG=text,RIGHTARG=text,PROCEDURE=pg_catalog.texteq)",
                "ALTER TABLE mrf.provider_directory_cms_npd_relationship DROP CONSTRAINT cms_npd_relationship_source_check",
                "ALTER TABLE mrf.provider_directory_cms_npd_relationship ADD CONSTRAINT cms_npd_relationship_source_check CHECK(source_id::text OPERATOR(mrf.===) 'cms-npd')",
            ],
            "dependencies",
        ),
        (["ALTER TABLE mrf.provider_directory_cms_npd_relationship ENABLE ROW LEVEL SECURITY"], "root_native"),
        (["ALTER EXTENSION plpgsql ADD TABLE mrf.provider_directory_site_identity"], "dependencies"),
    ],
    ids=[
        "default",
        "nullability",
        "type-size",
        "extra-column",
        "replacement-order",
        "primary-key",
        "header-check",
        "retained-fk",
        "fk-enforcement",
        "extra-check",
        "unique-index",
        "index-predicate",
        "operator-class",
        "generated-expression",
        "domain",
        "collation",
        "operator",
        "rls",
        "extension-dependency",
    ],
)
async def test_meaningful_native_layout_drift_refuses_and_rolls_back(native_layout, statements, reason):
    transaction = native_layout.transaction()
    await transaction.start()
    try:
        for statement in statements:
            await native_layout.execute(statement)
        with pytest.raises(ValueError, match="cms_canonical_layout_" + reason):
            await _verify(native_layout)
    finally:
        await transaction.rollback()
    async with native_layout.transaction():
        assert (await _verify(native_layout))["root_count"] == 17


@pytest.mark.parametrize("search_path", ["mrf, pg_catalog", "pg_catalog, public", "pg_temp, pg_catalog"])
async def test_untrusted_search_path_is_refused_without_changing_it(native_layout, search_path):
    async with native_layout.transaction():
        await native_layout.fetchval("SELECT pg_catalog.set_config('search_path',$1,true)", search_path)
        with pytest.raises(ValueError, match="environment"):
            await _verify(native_layout)
        assert await native_layout.fetchval("SHOW search_path") == search_path


async def _populate_genuine_allocation(connection, dataset="cms-native-layout-one"):
    """Use the product header hook; never create relaxed expected child tables."""
    release = "a" * 64
    await connection.execute("""INSERT INTO mrf.provider_directory_api_endpoint
      (endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash)
      VALUES('endpoint-native-layout','https://example.test/fhir',repeat('b',64),repeat('c',64))
      ON CONFLICT(endpoint_id) DO NOTHING""")
    await connection.execute(
        """INSERT INTO mrf.provider_directory_endpoint_dataset
      (dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json)
      VALUES($1,'endpoint-native-layout','acquiring',false,0,$2::jsonb)""",
        dataset,
        json.dumps({"source_release": {"source_id": "cms-npd", "vector_sha256": release}}),
    )
    raw = json.dumps({"resourceType": "Organization", "id": "example-org"}, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode()).hexdigest()
    await connection.execute(
        """INSERT INTO mrf.provider_directory_dataset_resource
      (dataset_id,resource_type,resource_id,payload_hash,payload_json)
      VALUES($1,'Organization','example-org',$2,$3::jsonb)""",
        dataset,
        digest,
        raw,
    )
    await connection.execute(
        """INSERT INTO mrf.provider_directory_cms_npd_resource_witness
      (dataset_id,source_id,release_id,resource_type,resource_id,raw_payload_sha256,normalized_payload_hash,raw_payload_json)
      VALUES($1,'cms-npd',$2,'Organization','example-org',$3,$3,$4::jsonb)""",
        dataset,
        release,
        digest,
        raw,
    )
    await connection.execute(
        """INSERT INTO mrf.provider_directory_cms_npd_relationship
      (dataset_id,source_id,release_id,resource_type,resource_id,source_payload_hash,raw_payload_sha256,
       reference_field,parent_ordinal,reference_ordinal,target_type,target_reference,target_resource_id,resolution_status)
      VALUES($1,'cms-npd',$2,'Organization','example-org',$3,$3,'partOf',0,1,'Organization',
       'Organization/example-org','example-org','resolved')""",
        dataset,
        release,
        digest,
    )


async def test_genuine_populated_allocator_clones_preserve_root_layout(native_layout):
    transaction = native_layout.transaction()
    await transaction.start()
    try:
        await _populate_genuine_allocation(native_layout)
        assert (
            await native_layout.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid IN "
                "('mrf.provider_directory_cms_npd_resource_witness'::regclass,"
                "'mrf.provider_directory_cms_npd_relationship'::regclass) AND contype='f' AND conparentid<>0"
            )
            == 2
        )
        assert (await _verify(native_layout))["root_count"] == 17
        await _populate_genuine_allocation(native_layout, "cms-native-layout-two")
        assert (
            await native_layout.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid IN "
                "('mrf.provider_directory_cms_npd_resource_witness'::regclass,"
                "'mrf.provider_directory_cms_npd_relationship'::regclass) AND contype='f' AND conparentid<>0"
            )
            == 4
        )
        assert (await _verify(native_layout))["root_count"] == 17
        await native_layout.execute('ALTER SCHEMA mrf RENAME TO "Example_Layout"')
        assert (await _verify(native_layout, "Example_Layout"))["root_count"] == 17
    finally:
        await transaction.rollback()


async def _drift_native_clone(native_layout, drift):
    leaf = await native_layout.fetchval(
        "SELECT confrelid::regclass::text FROM pg_constraint "
        "WHERE conrelid='mrf.provider_directory_cms_npd_resource_witness'::regclass AND conparentid<>0"
    )
    reason = "fk_clone"
    if drift in {"disabled-action", "replica-action"}:
        trigger = await native_layout.fetchval(
            "SELECT t.tgname FROM pg_trigger t JOIN pg_constraint k "
            "ON k.oid=t.tgconstraint WHERE k.conrelid='mrf.provider_directory_cms_npd_resource_witness'::regclass "
            "AND k.conparentid<>0 AND t.tgtype=9"
        )
        event = "DISABLE" if drift == "disabled-action" else "ENABLE REPLICA"
        await native_layout.execute(f'ALTER TABLE {leaf} {event} TRIGGER "{trigger}"')
    elif drift == "extra-root-fk":
        await native_layout.execute(
            "ALTER TABLE mrf.provider_directory_cms_npd_resource_witness "
            "ADD CONSTRAINT unexpected FOREIGN KEY(dataset_id,resource_type,resource_id) "
            "REFERENCES mrf.provider_directory_dataset_resource(dataset_id,resource_type,resource_id) ON DELETE CASCADE"
        )
    elif drift == "extra-leaf-fk":
        reason = "constraints"
        await native_layout.execute(
            "ALTER TABLE mrf.provider_directory_cms_npd_resource_witness "
            f"ADD CONSTRAINT unexpected FOREIGN KEY(dataset_id,resource_type,resource_id) REFERENCES {leaf} "
            "(dataset_id,resource_type,resource_id) ON DELETE CASCADE"
        )
    else:
        name = "provider_directory_cms_npd_re_dataset_id_resource_type_res_fkey"
        await native_layout.execute(
            f"ALTER TABLE mrf.provider_directory_cms_npd_resource_witness DROP CONSTRAINT {name}"
        )
        await native_layout.execute(
            "ALTER TABLE mrf.provider_directory_cms_npd_resource_witness "
            f"ADD CONSTRAINT {name} FOREIGN KEY(dataset_id,resource_type,resource_id) "
            "REFERENCES mrf.provider_directory_dataset_resource(dataset_id,resource_type,resource_id) ON DELETE RESTRICT"
        )
    return reason


@pytest.mark.parametrize(
    "drift", ["disabled-action", "replica-action", "extra-root-fk", "extra-leaf-fk", "changed-base-action"]
)
async def test_exact_native_clone_drift_refuses_and_restores_populated_layout(native_layout, drift):
    outer = native_layout.transaction()
    await outer.start()
    try:
        await _populate_genuine_allocation(native_layout)
        assert (await _verify(native_layout))["root_count"] == 17
        nested = native_layout.transaction()
        await nested.start()
        try:
            reason = await _drift_native_clone(native_layout, drift)
            with pytest.raises(ValueError, match="cms_canonical_layout_" + reason):
                await _verify(native_layout)
        finally:
            await nested.rollback()
        assert (await _verify(native_layout))["root_count"] == 17
    finally:
        await outer.rollback()
