# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One explicit native-admin ownership/ACL cut for the fixed output family."""

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process.mrf_address_publication import require_native_read_catalog
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import _close_relation_grants
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    OUTPUT_ROOTS,
    decoded,
    require_output,
    require_output_shape,
)
from process.provider_directory_output_snapshot_custody import _native_receipt
from process.provider_directory_projection_stage import _copy_driver

_OWNER_RIGHTS = {"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}


async def _capture(session, policy, *, legacy=False):
    driver = await _copy_driver(session)
    owner, readers = await storage_roles(driver, policy.schema, **policy.role_arguments())
    rows = await driver.fetch(OUTPUT_CATALOG_SQL, policy.schema, list(OUTPUT_ROOTS), owner, readers)
    require_output(
        len(rows) == len(OUTPUT_ROOTS) and {row["relname"] for row in rows} == set(OUTPUT_ROOTS), "roots_missing"
    )
    allowed_owners = await driver.fetchval(
        "SELECT array_agg(oid ORDER BY oid) FROM pg_roles WHERE rolname=ANY($1::text[])",
        [policy.owner_role, *policy.control_runtime_roles],
    )
    for row in rows:
        require_output_shape(row, row["relname"])
        require_output(
            row["relowner"] in allowed_owners
            if legacy
            else row["relowner"] == owner and row["acl_closed"] and row["readers_closed"] and row["readers_select"],
            "legacy_owner_changed" if legacy else "custody_changed",
        )
    await require_native_read_catalog(
        session,
        tuple(row["oid"] for row in rows),
        executable_roles=(policy.owner_role, policy.publisher_role),
    )
    return {row["relname"]: dict(row) for row in rows}


def require_output_adoption_delta(before, after, owner, readers):
    """Only ownership and exact fixed Reader ACLs may change; all native OIDs stay."""
    require_output(before.keys() == after.keys() == set(OUTPUT_ROOTS), "adoption_roots_changed")
    for name, previous in before.items():
        current = after[name]
        for key in ("schema_oid", "oid", "relname", "topology", "columns", "constraints", "descriptor"):
            old_value, new_value = previous[key], current[key]
            if key in {"topology", "columns", "constraints"}:
                old_value, new_value = decoded(old_value), decoded(new_value)
            require_output(old_value == new_value, "adoption_layout_changed")
        old_indexes, new_indexes = decoded(previous["indexes"]), decoded(current["indexes"])
        require_output(
            len(old_indexes) == len(new_indexes)
            and all({**old, "owner": owner} == new for old, new in zip(old_indexes, new_indexes, strict=True)),
            "adoption_indexes_changed",
        )
        expected_grants = [[owner, owner, privilege, False] for privilege in _OWNER_RIGHTS]
        expected_grants += [[reader, owner, "SELECT", False] for reader in readers]
        acl = decoded(current["acl"])
        require_output(
            current["relowner"] == owner and sorted(acl["table"]) == sorted(expected_grants) and acl["columns"] == [],
            "adoption_acl_changed",
        )


async def provision_output_snapshot_storage(session, policy, *, verify_role_profile, verify_writer_closure):
    """Run the genuine complete pre/post gate around an exact native-admin savepoint."""
    require_output(
        type(policy) is GenericSourcePolicy
        and session.in_transaction()
        and callable(verify_role_profile)
        and callable(verify_writer_closure),
        "explicit_admin_required",
    )
    actor = await _admin_actor(session)
    require_output(
        actor["session"] == actor["current"]
        and actor["native_admin"]
        and actor["current"] not in (policy.owner_role, policy.publisher_role, *policy.runtime_roles),
        "independent_admin_required",
    )
    driver = await _copy_driver(session)
    async with driver.transaction():
        await verify_role_profile(driver, policy.schema, **policy.role_arguments())
        await driver.execute("SET LOCAL lock_timeout='5s'")
        await driver.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(policy.schema, name) for name in OUTPUT_ROOTS)
            + " IN ACCESS EXCLUSIVE MODE"
        )
        before = await _capture(session, policy, legacy=True)
        for name, catalog_entry in before.items():
            await _close_relation_grants(
                driver,
                {"nspname": policy.schema, "relname": name, "oid": catalog_entry["oid"]},
                policy.owner_role,
                policy.runtime_roles,
            )
            # Include owner-column ACLs; table ownership supplies its native authority.
            columns = await driver.fetch(
                "SELECT DISTINCT a.attname,g.grantee,r.rolname FROM pg_attribute a "
                "CROSS JOIN LATERAL aclexplode(a.attacl) g LEFT JOIN pg_roles r ON r.oid=g.grantee "
                "WHERE a.attrelid=$1::oid ORDER BY a.attname,g.grantee",
                catalog_entry["oid"],
            )
            for column in columns:
                principal = "PUBLIC" if column["grantee"] == 0 else storage_identifier(column["rolname"])
                await driver.execute(
                    f"REVOKE ALL PRIVILEGES ({storage_identifier(column['attname'])}) "
                    f"ON TABLE {storage_relation(policy.schema, name)} FROM {principal} CASCADE"
                )
        after = await _capture(session, policy)
        owner, readers = await storage_roles(driver, policy.schema, **policy.role_arguments())
        require_output_adoption_delta(before, after, owner, readers)
        await verify_writer_closure(driver, policy)
        await verify_role_profile(driver, policy.schema, **policy.role_arguments())
        final_actor = await _admin_actor(session)
        require_output(dict(actor) == dict(final_actor), "admin_transaction_changed")
        return tuple(_native_receipt(policy, after[name], name) for name in OUTPUT_ROOTS)


async def _admin_actor(session):
    """Materialize and identify the supplied native-admin transaction."""
    return (
        (
            await session.execute(
                text(
                    "SELECT session_user::text AS session,current_user::text AS current,"
                    "pg_backend_pid() AS backend,pg_current_xact_id()::text AS xid,"
                    "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS native_admin"
                )
            )
        )
        .mappings()
        .one()
    )
