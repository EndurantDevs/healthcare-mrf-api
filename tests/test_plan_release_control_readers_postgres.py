# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CREATE cannot replace ownership of closed immutable parents or children."""

import asyncpg
import pytest

from process.plan_release_storage_custody import storage_roles, verify_storage_family
from tests.plan_release_control_reader_postgres_support import control_reader_case


@pytest.mark.asyncio
async def test_control_create_cannot_mutate_protected_storage():
    async with control_reader_case() as prepared:
        case = prepared.case
        actor = await case.connect(prepared.control)
        scratch = case.relation("control_scratch")
        root = case.relation("plan_release_snapshot_binding")
        await actor.execute(f"CREATE TABLE {scratch}(LIKE {root} INCLUDING ALL)")
        assert await actor.fetchval("SELECT has_schema_privilege(current_user,$1,'CREATE')", case.schema)
        async with prepared.admin.transaction():
            before = await verify_storage_family(prepared.admin, case.schema, **prepared.profile)
        protected_relations = [
            case.relation(relation_record["relname"])
            for relation_record in before["relations"]
            if relation_record["nspname"] == case.schema
        ]
        statements = [
            statement
            for protected_relation in protected_relations
            for statement in (
                f"ALTER TABLE {protected_relation} RENAME TO denied_rename",
                f"ALTER TABLE {protected_relation} ADD COLUMN denied_column int",
                f'ALTER TABLE {protected_relation} OWNER TO "{prepared.control}"',
                f"ALTER TABLE {protected_relation} ENABLE ROW LEVEL SECURITY",
                f"DROP TABLE {protected_relation}",
                f"CREATE TRIGGER denied_hook BEFORE UPDATE ON {protected_relation} FOR EACH ROW "
                "EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()",
                f"UPDATE {protected_relation} SET serving_revision_id=serving_revision_id",
                f"DELETE FROM {protected_relation}",
            )
        ]
        statements.append(f"ALTER TABLE {root} ATTACH PARTITION {scratch} FOR VALUES IN ('hpserve_{'9' * 26}')")
        for statement in statements:
            async with actor.transaction():
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await actor.execute(statement)
        await actor.execute(f"GRANT UPDATE ON {root} TO CURRENT_USER")
        assert not await actor.fetchval("SELECT has_table_privilege(current_user,$1,'UPDATE')", root)
        async with prepared.admin.transaction():
            after = await verify_storage_family(prepared.admin, case.schema, **prepared.profile)
        assert before == after
        strict_reader = await case.connect(case.reader)
        async with strict_reader.transaction():
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await strict_reader.execute(f"CREATE TABLE {case.relation('reader_scratch')}(value int)")


@pytest.mark.asyncio
async def test_control_scope_remains_strict_and_schema_owner_protected():
    async with control_reader_case() as prepared:
        case, admin = prepared.case, prepared.admin
        async with admin.transaction():
            with pytest.raises(ValueError, match="reader_namespace_authority"):
                await storage_roles(admin, case.schema, **{**prepared.profile, "control_reader_roles": ()})
        async with admin.transaction():
            with pytest.raises(ValueError, match="control_reader_profile_invalid"):
                await storage_roles(admin, case.schema, **{**prepared.profile, "control_reader_roles": ("unknown",)})
        for corruption in (
            f'GRANT CREATE ON SCHEMA "{case.schema}" TO "{case.reader}"',
            f'GRANT "{prepared.control}" TO "{case.reader}" WITH INHERIT FALSE, SET TRUE',
            f'GRANT "{prepared.namespace_owner}" TO "{case.reader}" WITH INHERIT FALSE, SET TRUE',
            f'ALTER ROLE "{prepared.namespace_owner}" LOGIN',
            f'GRANT "{prepared.namespace_owner}" TO "{prepared.control}" WITH INHERIT FALSE, SET TRUE',
        ):
            transaction = admin.transaction()
            await transaction.start()
            try:
                await admin.execute(corruption)
                with pytest.raises(ValueError, match="namespace.*authority"):
                    await storage_roles(admin, case.schema, **prepared.profile)
            finally:
                await transaction.rollback()


@pytest.mark.asyncio
async def test_control_profile_callback_failure_restores_complete_cut():
    async with control_reader_case(provisioned=False, legacy_guards=True) as prepared:
        case, admin = prepared.case, prepared.admin
        before = await admin.fetch(
            "SELECT oid,relowner,relacl::text,(SELECT array_agg(tgname ORDER BY tgname) FROM pg_trigger WHERE tgrelid=c.oid) AS hooks "
            "FROM pg_class c WHERE relnamespace=$1::regnamespace ORDER BY oid",
            case.schema,
        )

        async def failure(connection, schema_name, **observed):
            await prepared.verify(connection, schema_name, **observed)
            raise ValueError("complete profile refused")

        from process.plan_release_storage_provision import provision_plan_release_storage

        async with admin.transaction():
            with pytest.raises(ValueError, match="complete profile refused"):
                await provision_plan_release_storage(
                    admin, case.schema, **prepared.profile, verify_role_profile=failure
                )
        after = await admin.fetch(
            "SELECT oid,relowner,relacl::text,(SELECT array_agg(tgname ORDER BY tgname) FROM pg_trigger WHERE tgrelid=c.oid) AS hooks "
            "FROM pg_class c WHERE relnamespace=$1::regnamespace ORDER BY oid",
            case.schema,
        )
        assert before == after
