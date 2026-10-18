# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed independent-admin adoption of the existing Geo heap and ledger rights."""

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process.geo_source_storage import ROOT, require, require_geo_storage
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import _GRANTEES_SQL
from process.provider_directory_cms_doctors_provision import (
    GENERATION_SELECT,
    GENERATION_UPDATE,
    _original_admin_driver,
)
from process.provider_directory_generic_source_policy import GenericSourcePolicy


async def _close_geo_access(session, native, policy, oid):
    """Close all direct writers and preserve prior SELECT without adding readers."""
    from process.code_catalog_snapshot import _relation_access

    prior = await _relation_access(session, oid)
    relation = storage_relation(policy.schema, ROOT)
    await native.execute(f"ALTER TABLE {relation} OWNER TO {storage_identifier(policy.owner_role)}")
    for grant in await native.fetch(_GRANTEES_SQL, oid):
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        column = grant["column_name"]
        privileges = f"ALL PRIVILEGES ({storage_identifier(column)})" if column else "ALL PRIVILEGES"
        await native.execute(f"REVOKE {privileges} ON TABLE {relation} FROM {principal} CASCADE")
    for grant in prior.grants:
        if grant.privilege_type == "SELECT" and grant.grantee_name != policy.owner_role:
            principal = "PUBLIC" if grant.grantee_name is None else storage_identifier(grant.grantee_name)
            column = "" if grant.column_name is None else f" ({storage_identifier(grant.column_name)})"
            option = " WITH GRANT OPTION" if grant.is_grantable else ""
            await session.execute(text(f"GRANT SELECT{column} ON TABLE {relation} TO {principal}{option}"))


async def provision_geo_source_storage(
    session, policy, *, verify_role_profile, verify_writer_closure, verify_legacy_guards
):
    """Retire only attested old hooks after authentic full-profile and writer closure."""
    require(type(policy) is GenericSourcePolicy and session.in_transaction(), "explicit_admin_required")
    require(
        all(callable(callback) for callback in (verify_role_profile, verify_writer_closure, verify_legacy_guards)),
        "complete_closure_verifiers_required",
    )
    native = await _original_admin_driver(session, policy)
    async with session.begin_nested():
        await storage_roles(native, policy.schema, **policy.role_arguments())
        await verify_role_profile(native, policy.schema, **policy.role_arguments())
        await native.execute("SET LOCAL lock_timeout='5s'")
        relation = storage_relation(policy.schema, ROOT)
        ledger = storage_relation(policy.schema, "reference_family_result_generation")
        await native.execute(f"LOCK TABLE ONLY {relation},{ledger} IN ACCESS EXCLUSIVE MODE")
        oid = await native.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
        require(type(oid) is int and oid > 0, "relation_changed")
        await verify_legacy_guards(native, policy, oid)
        await _close_geo_access(session, native, policy, oid)
        await require_geo_storage(session, schema_name=policy.schema, legacy=True, native_policy=policy)
        select = ",".join(storage_identifier(name) for name in GENERATION_SELECT)
        update = ",".join(storage_identifier(name) for name in GENERATION_UPDATE)
        await native.execute(
            f"GRANT SELECT({select}),UPDATE({update}) ON {ledger} TO {storage_identifier(policy.owner_role)}"
        )
        await verify_writer_closure(native, policy)
        hooks = await native.fetch("SELECT tgname FROM pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal", oid)
        require(
            all(
                hook_row["tgname"] in {"reference_source_generation_revision_guard", "cms_native_input_mutation"}
                for hook_row in hooks
            ),
            "hooks_changed",
        )
        for hook_row in hooks:
            await native.execute(f"DROP TRIGGER {storage_identifier(hook_row['tgname'])} ON {relation}")
        changed = await native.fetchval(
            f"UPDATE {ledger} SET source_revision_tracked=FALSE WHERE importer_id='geo' RETURNING importer_id"
        )
        require(changed == "geo", "generation_unavailable")
        require(
            await require_geo_storage(session, schema_name=policy.schema, native_policy=policy) == oid,
            "relation_changed",
        )
        await verify_role_profile(native, policy.schema, **policy.role_arguments())
        return oid
