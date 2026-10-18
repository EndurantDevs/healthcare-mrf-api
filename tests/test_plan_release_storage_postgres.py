# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native legacy parity, custody closure and publication failure proofs."""

import asyncio
import json

import asyncpg
import pytest
from sqlalchemy.exc import ProgrammingError

from db.migration_plan_release_storage import downgrade_plan_release_storage
from process.plan_release_storage_custody import require_plan_release_storage_custody
from tests.plan_release_storage_cohort_support import CAPTURED_TIME, attach_native_cohort, prepare_native_cohort
from tests.plan_release_storage_postgres_support import (
    NEW_REVISION,
    OLD_REVISION,
    RELEASE,
    install_legacy_guards,
    storage_case,
    verify_native_test_profile,
)


@pytest.mark.asyncio
async def test_legacy_enrollment_preserves_rows_heap_and_indexes_without_default():
    async with storage_case(migrated=False) as case:
        admin = await case.connect()
        before = await admin.fetch(
            f"SELECT * FROM {case.relation('plan_release_snapshot_binding')} ORDER BY binding_ordinal"
        )
        old_oid = await admin.fetchval("SELECT to_regclass($1)::oid", case.relation("plan_release_snapshot_binding"))
        indexes = await admin.fetch("SELECT indexrelid FROM pg_index WHERE indrelid=$1 ORDER BY indexrelid", old_oid)
        await case.migrate()
        assert (
            await admin.fetch(
                f"SELECT * FROM {case.relation('plan_release_snapshot_binding')} ORDER BY binding_ordinal"
            )
            == before
        )
        assert (
            await admin.fetchval("SELECT to_regclass($1)::oid", case.relation("plan_release_snapshot_binding_legacy"))
            == old_oid
        )
        assert (
            await admin.fetch("SELECT indexrelid FROM pg_index WHERE indrelid=$1 ORDER BY indexrelid", old_oid)
            == indexes
        )
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=$1::regnamespace AND pg_get_expr(relpartbound,oid)='DEFAULT')",
            case.schema,
        )
        async with case.engine.begin() as connection:
            await connection.run_sync(lambda native: downgrade_plan_release_storage(native, case.schema))
        assert (
            await admin.fetch(
                f"SELECT * FROM {case.relation('plan_release_snapshot_binding')} ORDER BY binding_ordinal"
            )
            == before
        )
        await case.migrate()
        async with case.engine.begin() as connection:
            await connection.run_sync(lambda native: downgrade_plan_release_storage(native, case.schema))
        assert (
            await admin.fetchval("SELECT to_regclass($1)::oid", case.relation("plan_release_snapshot_binding"))
            == old_oid
        )
        assert (
            await admin.fetch("SELECT indexrelid FROM pg_index WHERE indrelid=$1 ORDER BY indexrelid", old_oid)
            == indexes
        )
        assert (
            await admin.fetch(
                f"SELECT * FROM {case.relation('plan_release_snapshot_binding')} ORDER BY binding_ordinal"
            )
            == before
        )


async def _storage_catalog_snapshot(admin, case):
    catalog = await admin.fetchval(
        """WITH roots AS (SELECT oid,reltoastrelid FROM pg_class WHERE relnamespace=$1::regnamespace),
        family AS (SELECT oid FROM roots UNION SELECT reltoastrelid FROM roots WHERE reltoastrelid<>0
          UNION SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT reltoastrelid FROM roots))
        SELECT jsonb_build_object(
          'namespace',(SELECT to_jsonb(n) FROM pg_namespace n WHERE oid=$1::regnamespace),
          'relations',(SELECT jsonb_agg(to_jsonb(c) ORDER BY oid) FROM pg_class c WHERE oid IN(SELECT oid FROM family)),
          'attributes',(SELECT jsonb_agg(to_jsonb(a) ORDER BY attrelid,attnum) FROM pg_attribute a WHERE attrelid IN(SELECT oid FROM family)),
          'constraints',(SELECT jsonb_agg(to_jsonb(c) ORDER BY oid) FROM pg_constraint c WHERE conrelid IN(SELECT oid FROM family)),
          'indexes',(SELECT jsonb_agg(to_jsonb(i) ORDER BY indexrelid) FROM pg_index i WHERE indrelid IN(SELECT oid FROM family)),
          'triggers',(SELECT jsonb_agg(to_jsonb(t) ORDER BY oid) FROM pg_trigger t WHERE tgrelid IN(SELECT oid FROM family)),
          'inheritance',(SELECT jsonb_agg(to_jsonb(i) ORDER BY inhrelid,inhparent) FROM pg_inherits i WHERE inhrelid IN(SELECT oid FROM family) OR inhparent IN(SELECT oid FROM family)),
          'functions',(SELECT jsonb_agg(to_jsonb(p) ORDER BY oid) FROM pg_proc p WHERE pronamespace=$1::regnamespace))""",
        case.schema,
    )
    rows_by_relation = {
        name: await admin.fetch(
            f"SELECT * FROM {case.relation(name)} AS snapshot_row ORDER BY to_jsonb(snapshot_row)::text"
        )
        for name in (
            "plan_release_serving_revision",
            "plan_release_snapshot_binding",
            "plan_release_snapshot_binding_legacy",
            "plan_release_snapshot_pin",
        )
    }
    return catalog, rows_by_relation


@pytest.mark.asyncio
async def test_provisioned_legacy_refuses_downgrade_without_catalog_or_data_changes():
    async with storage_case(migrated=False) as case:
        await case.migrate()
        await case.provision()
        admin = await case.connect()
        before = await _storage_catalog_snapshot(admin, case)
        async with case.engine.begin() as connection:
            with pytest.raises(RuntimeError, match="downgrade_requires_serving_rollback"):
                await connection.run_sync(lambda native: downgrade_plan_release_storage(native, case.schema))
        assert await _storage_catalog_snapshot(admin, case) == before


@pytest.mark.asyncio
async def test_explicit_provision_closes_parent_child_column_and_inherited_bypasses():
    async with storage_case() as case:
        admin = await case.connect()
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        await admin.execute(f'GRANT UPDATE(plan_id) ON {legacy} TO "{case.reader}"')
        await admin.execute(f'GRANT INSERT ON {case.relation("plan_release_snapshot_binding")} TO "{case.reader}"')
        assert await case.provision()
        assert await case.provision()  # Resuming the explicit administrator cut is idempotent.
        publisher = await case.connect(case.publisher)
        async with publisher.transaction():
            proof = await require_plan_release_storage_custody(publisher, case.schema, **case.profile)
            assert len(proof["relations"]) == 4
        reader = await case.connect(case.reader)
        for relation in (
            "plan_release_snapshot_binding",
            "plan_release_snapshot_binding_legacy",
            "plan_release_snapshot_pin",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await reader.execute(f"DELETE FROM {case.relation(relation)}")
        await admin.execute(f'GRANT UPDATE(plan_id) ON {legacy} TO "{case.reader}"')
        async with publisher.transaction():
            with pytest.raises(ValueError, match="custody_open"):
                await require_plan_release_storage_custody(publisher, case.schema, **case.profile)
        await admin.execute(f'REVOKE UPDATE(plan_id) ON {legacy} FROM "{case.reader}"')
        await admin.execute(f'GRANT "{case.owner}" TO "{case.reader}" WITH INHERIT FALSE')
        async with publisher.transaction():
            with pytest.raises(ValueError, match="reader_authority"):
                await require_plan_release_storage_custody(publisher, case.schema, **case.profile)


@pytest.mark.asyncio
async def test_provision_failure_rolls_back_ownership_and_does_not_drop_legacy_fk():
    async with storage_case() as case:
        admin = await case.connect()
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        before = await admin.fetchrow("SELECT relowner,relacl::text FROM pg_class WHERE oid=to_regclass($1)", legacy)
        await admin.execute(f'ALTER ROLE "{case.owner}" LOGIN')
        with pytest.raises(ValueError, match="owner_unprotected"):
            await case.provision()
        assert (
            await admin.fetchrow("SELECT relowner,relacl::text FROM pg_class WHERE oid=to_regclass($1)", legacy)
            == before
        )
        assert await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f')", legacy
        )


@pytest.mark.asyncio
async def test_cross_schema_attach_preserves_reader_revision_through_encoding_and_blocks_drop():
    from api.plan_release_pricing_projection import plan_release_serving_sql

    async with storage_case(provisioned=True) as case:
        binding, pin = await prepare_native_cohort(case)
        admin = await case.connect()
        await admin.execute(
            f"INSERT INTO {case.relation('ptg2_v3_snapshot_plan_scope')} VALUES('old-snapshot','logical-plan','group')"
        )
        # The deployment reader already has source access; provisioning adds only plan storage SELECT.
        await admin.execute(
            f'GRANT SELECT ON {case.relation("ptg2_snapshot")},{case.relation("ptg2_v3_snapshot_plan_scope")},{case.relation("ptg2_snapshot_pin")} TO "{case.reader}"'
        )
        reader = await case.connect(case.reader)
        publisher = await case.connect(case.publisher)
        query = plan_release_serving_sql(case.schema, include_pricing_projection=False)
        query = query.replace(":plan_release_id", "$1").replace(":pin_owner_type", "$2")
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            before = await reader.fetch(query, RELEASE, "plan_release_serving_revision")
            assert len(before) == 1000 and before[0]["serving_revision_id"] == OLD_REVISION
            async with publisher.transaction():
                await require_plan_release_storage_custody(publisher, case.schema, **case.profile)
                await attach_native_cohort(case, publisher, binding, pin)
                await require_plan_release_storage_custody(publisher, case.schema, **case.profile)
            retained = await reader.fetch(query, RELEASE, "plan_release_serving_revision")
            assert retained == before
            encoded = json.dumps([dict(member) for member in retained], default=str)
            assert NEW_REVISION not in encoded and encoded.count(OLD_REVISION) == 1000
            await admin.execute("SET lock_timeout='100ms'")
            with pytest.raises(asyncpg.LockNotAvailableError):
                await admin.execute(f"DROP TABLE {case.relation('plan_release_snapshot_binding_legacy')}")
        current = await reader.fetch(query, RELEASE, "plan_release_serving_revision")
        assert len(current) == 2000 and {member["serving_revision_id"] for member in current} == {NEW_REVISION}
        assert all(member["is_pinned"] for member in current)
        assert not any(member["is_pinned"] for member in await reader.fetch(query, RELEASE, "other-owner"))
        from api import plan_release_serving_resolution as resolution

        billing_query = resolution._PLAN_RELEASE_SERVING_SQL.replace(f"{resolution.PTG2_SCHEMA}.", case.schema + ".")
        billing_query = billing_query.replace(":plan_release_id", "$1").replace(":pin_owner_type", "$2")
        billing_rows = await reader.fetch(billing_query, RELEASE, "plan_release_serving_revision")
        assert billing_rows and all(
            member["is_pinned"] and member["serving_revision_id"] == NEW_REVISION for member in billing_rows
        )
        assert await admin.fetchval(f"SELECT min(created_at) FROM {binding}") == CAPTURED_TIME


@pytest.mark.asyncio
async def test_canceled_attach_rolls_back_both_roots_and_current_headers():
    async with storage_case(provisioned=True) as case:
        binding, pin = await prepare_native_cohort(case)
        publisher = await case.connect(case.publisher)
        attached = asyncio.Event()
        release_commit = asyncio.Event()

        async def publish():
            async with publisher.transaction():
                await attach_native_cohort(case, publisher, binding, pin)
                attached.set()
                await release_commit.wait()

        publication = asyncio.create_task(publish())
        await asyncio.wait_for(attached.wait(), timeout=5)
        publication.cancel()
        with pytest.raises(asyncio.CancelledError):
            await publication
        async with publisher.transaction():
            proof = await require_plan_release_storage_custody(publisher, case.schema, **case.profile)
            assert len(proof["relations"]) == 4
            assert (
                await publisher.fetchval(
                    f"SELECT serving_revision_id FROM {case.relation('plan_release_serving_revision')} WHERE is_current"
                )
                == OLD_REVISION
            )
            assert not await publisher.fetchval(
                "SELECT relispartition FROM pg_class WHERE oid=to_regclass($1)", binding
            )
            assert not await publisher.fetchval("SELECT relispartition FROM pg_class WHERE oid=to_regclass($1)", pin)


@pytest.mark.asyncio
async def test_shared_pin_guards_survive_binding_retirement():
    async with storage_case(migrated=False) as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(lambda native: install_legacy_guards(native, case.schema))
        admin = await case.connect()
        shared = case.relation("ptg2_snapshot_pin")
        query = "SELECT oid,pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid=to_regclass($1) ORDER BY oid"
        original = await admin.fetch(query, shared)
        assert len(original) == 6  # Four unchanged custom guards and two native snapshot FK hooks.
        await case.migrate()
        await case.provision()
        assert await admin.fetch(query, shared) == original
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1))",
            case.relation("plan_release_snapshot_binding_legacy"),
        )
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f')",
            case.relation("plan_release_snapshot_binding_legacy"),
        )


@pytest.mark.asyncio
async def test_migration_ddl_failure_rolls_back_rename_rows_and_original_indexes():
    async with storage_case(migrated=False) as case:
        admin = await case.connect()
        binding = case.relation("plan_release_snapshot_binding")
        oid = await admin.fetchval("SELECT to_regclass($1)::oid", binding)
        await admin.execute(f"CREATE TABLE {case.relation('plan_release_snapshot_pin')}(unrelated_marker integer)")
        with pytest.raises(ProgrammingError, match="already exists"):
            await case.migrate()
        assert await admin.fetchval("SELECT to_regclass($1)::oid", binding) == oid
        assert await admin.fetchval(f"SELECT count(*) FROM {binding}") == 1000
        assert (
            await admin.fetchval("SELECT to_regclass($1)", case.relation("plan_release_snapshot_binding_legacy"))
            is None
        )


@pytest.mark.asyncio
async def test_both_native_gc_sets_retain_a_dedicated_pin_without_a_binding():
    from db.plan_release_storage import storage_table_sql
    from process.ptg_parts import ptg2_source_snapshot_gc, snapshot_cleanup

    async with storage_case(provisioned=True) as case:
        admin = await case.connect()
        detached = case.relation("pin_only")
        async with admin.transaction():
            await admin.execute(storage_table_sql(case.schema, "pin_only", pin=True))
            await admin.execute(
                f"INSERT INTO {detached}(serving_revision_id,snapshot_id) VALUES($1,'pin-only-snapshot')", OLD_REVISION
            )
            await admin.execute(
                f"ALTER TABLE {case.relation('plan_release_snapshot_pin')} ATTACH PARTITION {detached} FOR VALUES IN ('{OLD_REVISION}')"
            )
            await admin.execute(
                f"INSERT INTO {case.relation('ptg2_snapshot')} VALUES('pin-only-snapshot','published','{{}}')"
            )
            await admin.execute(
                f'UPDATE {case.relation("ptg2_snapshot")} SET manifest=\'{{"serving_index":{{"source_key":"synthetic_source"}}}}\''
            )
        ordinary_query = ptg2_source_snapshot_gc._CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", case.schema)
        source_query = snapshot_cleanup._CURRENT_SOURCE_POINTER_SNAPSHOT_IDS_SQL.replace(
            "__SCHEMA__", case.schema
        ).replace(":source_key", "$1")
        assert set(
            await admin.fetchval(
                "SELECT array_agg(snapshot_id ORDER BY snapshot_id) FROM (" + ordinary_query + ") retained"
            )
        ) == {"old-snapshot", "pin-only-snapshot"}
        assert {member["snapshot_id"] for member in await admin.fetch(source_query, "synthetic_source")} == {
            "old-snapshot",
            "pin-only-snapshot",
        }


@pytest.mark.asyncio
async def test_late_custody_rejection_restores_guards_even_when_outer_caller_catches():
    from process.plan_release_storage_provision import provision_plan_release_storage

    async with storage_case() as case:
        admin = await case.connect()
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        owner_before = await admin.fetchval("SELECT relowner FROM pg_class WHERE oid=to_regclass($1)", legacy)
        await admin.execute(f"CREATE INDEX unexpected_index ON {legacy}(plan_id)")
        async with admin.transaction():
            with pytest.raises(ValueError, match="extra_or_unready_index"):
                await provision_plan_release_storage(
                    admin, case.schema, **case.profile, verify_role_profile=verify_native_test_profile
                )
        assert await admin.fetchval("SELECT relowner FROM pg_class WHERE oid=to_regclass($1)", legacy) == owner_before
        assert await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f')", legacy
        )


@pytest.mark.asyncio
async def test_admin_cut_requires_complete_profile_verifier_before_mutation():
    from process.plan_release_storage_provision import provision_plan_release_storage

    async with storage_case() as case:
        admin = await case.connect()
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        before = await admin.fetchrow(
            "SELECT oid,relowner,relacl::text FROM pg_class WHERE oid=to_regclass($1)", legacy
        )
        async with admin.transaction():
            with pytest.raises(ValueError, match="complete_profile_verifier_required"):
                await provision_plan_release_storage(admin, case.schema, **case.profile)
        assert (
            await admin.fetchrow("SELECT oid,relowner,relacl::text FROM pg_class WHERE oid=to_regclass($1)", legacy)
            == before
        )
        assert await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f')", legacy
        )


@pytest.mark.asyncio
async def test_profile_callback_failure_restores_original_guards_and_custody():
    async with storage_case(migrated=False) as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(lambda native: install_legacy_guards(native, case.schema))
        await case.migrate()
        admin = await case.connect()
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        before = await admin.fetchrow(
            "SELECT oid,relowner,relacl::text FROM pg_class WHERE oid=to_regclass($1)", legacy
        )
        guards = await admin.fetch(
            "SELECT oid,pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid=to_regclass($1) ORDER BY oid", legacy
        )

        async def reject_profile(connection, schema_name, **profile):
            assert connection is not None and schema_name == case.schema and profile == case.profile
            await verify_native_test_profile(connection, schema_name, **profile)
            # Legacy protection is still installed after ACL closure, before the callback approves.
            assert await connection.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1)", legacy
            ) == len(guards)
            raise ValueError("complete_profile_rejected")

        with pytest.raises(ValueError, match="complete_profile_rejected"):
            await case.provision(verify_role_profile=reject_profile)
        assert (
            await admin.fetchrow("SELECT oid,relowner,relacl::text FROM pg_class WHERE oid=to_regclass($1)", legacy)
            == before
        )
        assert (
            await admin.fetch(
                "SELECT oid,pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid=to_regclass($1) ORDER BY oid", legacy
            )
            == guards
        )
        assert await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f')", legacy
        )


@pytest.mark.asyncio
async def test_legacy_bounds_capture_building_headers_without_payload_repairs():
    async with storage_case(migrated=False) as case:
        admin = await case.connect()
        header = case.relation("plan_release_serving_revision")
        await admin.execute(
            f"INSERT INTO {header} (serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,expected_binding_count,binding_set_digest) "
            "SELECT $1,plan_release_id,healthporta_plan_id,release_month,release_status,'building',expected_binding_count,binding_set_digest "
            f"FROM {header} WHERE serving_revision_id=$2",
            NEW_REVISION,
            OLD_REVISION,
        )
        headers = await admin.fetch(f"SELECT * FROM {header} ORDER BY serving_revision_id")
        await case.migrate()
        await case.provision()
        assert await admin.fetch(f"SELECT * FROM {header} ORDER BY serving_revision_id") == headers
        assert await admin.fetchval(f"SELECT count(*) FROM {case.relation('plan_release_snapshot_binding')}") == 1000
        bound = await admin.fetchval(
            "SELECT pg_get_expr(relpartbound,oid) FROM pg_class WHERE oid=to_regclass($1)",
            case.relation("plan_release_snapshot_binding_legacy"),
        )
        assert OLD_REVISION in bound and NEW_REVISION in bound
        publisher = await case.connect(case.publisher)
        async with publisher.transaction():
            await require_plan_release_storage_custody(publisher, case.schema, **case.profile)


@pytest.mark.asyncio
async def test_custody_rejects_removed_essential_header_check():
    async with storage_case(provisioned=True) as case:
        admin = await case.connect()
        await admin.execute(
            f"ALTER TABLE {case.relation('plan_release_serving_revision')} DROP CONSTRAINT plan_release_serving_current_state_check"
        )
        publisher = await case.connect(case.publisher)
        async with publisher.transaction():
            with pytest.raises(ValueError, match="header_checks_changed"):
                await require_plan_release_storage_custody(publisher, case.schema, **case.profile)


@pytest.mark.asyncio
async def test_custody_rejects_noncanonical_index_options():
    async with storage_case(provisioned=True) as case:
        admin = await case.connect()
        await admin.execute(f"DROP INDEX {case.relation('plan_release_snapshot_binding_snapshot_idx')}")
        await admin.execute(
            f"CREATE INDEX plan_release_snapshot_binding_snapshot_idx ON {case.relation('plan_release_snapshot_binding')} (snapshot_id DESC)"
        )
        publisher = await case.connect(case.publisher)
        async with publisher.transaction():
            with pytest.raises(ValueError, match="extra_or_unready_index"):
                await require_plan_release_storage_custody(publisher, case.schema, **case.profile)
