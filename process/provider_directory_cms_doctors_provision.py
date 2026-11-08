# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit native-admin custody cut for the three immutable Doctors heaps."""

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import _FAMILY_SQL, _close_relation_grants
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_storage import _require
from process.provider_directory_projection_stage import _copy_driver

DOCTORS_ROOTS = ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site")
GENERATION_ROOT = "reference_family_result_generation"
SERVING_RECEIPT_ROOT = "provider_directory_cms_serving_receipt"
SERVING_RECEIPT_SELECT = ("receipt_id", "payload", "publication_xid")
GENERATION_SELECT = (
    "importer_id",
    "local_lineage_id",
    "local_generation",
    "origin_lineage_id",
    "origin_generation",
    "published_at",
    "relation_oids",
    "source_revision_tracked",
)
GENERATION_UPDATE = (
    "local_generation",
    "origin_lineage_id",
    "origin_generation",
    "published_at",
    "relation_oids",
    "source_revision_tracked",
)
_KNOWN_HOOKS = {
    "reference_source_generation_revision_guard",
    "cms_doctors_prepared_write_seal",
    "cms_native_input_mutation",
}


async def provision_doctors_source_storage(
    session, policy, *, verify_role_profile, verify_writer_closure, verify_legacy_guards
):
    """Close the fixed SOURCE heaps using the original independent-admin transaction."""
    _require(type(policy) is GenericSourcePolicy and session.in_transaction(), "explicit_admin_required")
    _require(
        all(callable(verifier) for verifier in (verify_role_profile, verify_writer_closure, verify_legacy_guards)),
        "complete_closure_verifiers_required",
    )
    native = await _original_admin_driver(session, policy)
    async with session.begin_nested():
        await storage_roles(native, policy.schema, **policy.role_arguments())
        await native.execute("SET LOCAL lock_timeout='5s'")
        roots = [storage_relation(policy.schema, root) for root in DOCTORS_ROOTS]
        ledger = storage_relation(policy.schema, GENERATION_ROOT)
        await native.execute("LOCK TABLE " + ",".join((*roots, ledger)) + " IN ACCESS EXCLUSIVE MODE")
        await verify_role_profile(native, policy.schema, **policy.role_arguments())
        family = await native.fetch(_FAMILY_SQL, roots)
        _require(
            len(family) == 3
            and {relation_by_field["relname"] for relation_by_field in family} == set(DOCTORS_ROOTS)
            and all(relation_by_field["nspname"] == policy.schema for relation_by_field in family),
            "doctors_roots_changed",
        )
        await verify_legacy_guards(native, policy, family)
        for relation in family:
            await _close_relation_grants(native, relation, policy.owner_role, policy.runtime_roles)
        await _grant_generation_columns(native, policy)
        await verify_writer_closure(native, policy)
        from process.cms_doctors_immutable import require_doctors_storage

        pairs = await require_doctors_storage(session, schema_name=policy.schema, legacy=True, native_policy=policy)
        _require(tuple(name for name, _ in pairs) == DOCTORS_ROOTS, "doctors_roots_changed")
        hooks = await native.fetch(
            "SELECT tgrelid::bigint AS oid,tgname FROM pg_trigger WHERE tgrelid=ANY($1::oid[]) AND NOT tgisinternal",
            [oid for _, oid in pairs],
        )
        roots_by_oid = {oid: name for name, oid in pairs}
        _require(
            all(hook["tgname"] in _KNOWN_HOOKS and hook["oid"] in roots_by_oid for hook in hooks),
            "doctors_hooks_changed",
        )
        for hook in hooks:
            await native.execute(
                f"DROP TRIGGER {storage_identifier(hook['tgname'])} ON "
                f"{storage_relation(policy.schema, roots_by_oid[hook['oid']])}"
            )
        _require(
            await require_doctors_storage(session, schema_name=policy.schema, native_policy=policy) == pairs,
            "doctors_roots_changed",
        )
        await _require_generation_columns(native, policy)
        await verify_role_profile(native, policy.schema, **policy.role_arguments())
        return pairs


async def _original_admin_driver(session, policy):
    """Materialize only the caller's existing session transaction before native IO."""
    actor = (
        (await session.execute(text("SELECT session_user::text AS session,current_user::text AS current")))
        .mappings()
        .one()
    )
    _require(
        actor["session"] == actor["current"]
        and actor["current"] not in (policy.owner_role, policy.publisher_role, *policy.runtime_roles),
        "independent_admin_required",
    )
    native = await _copy_driver(session)
    _require(native.is_in_transaction(), "explicit_admin_transaction_required")
    return native


async def _grant_generation_columns(native, policy):
    select_columns = ",".join(storage_identifier(name) for name in GENERATION_SELECT)
    update_columns = ",".join(storage_identifier(name) for name in GENERATION_UPDATE)
    await native.execute(
        f"GRANT SELECT({select_columns}),UPDATE({update_columns}) ON "
        f"{storage_relation(policy.schema, GENERATION_ROOT)} TO {storage_identifier(policy.owner_role)}"
    )
    receipt_columns = ",".join(storage_identifier(name) for name in SERVING_RECEIPT_SELECT)
    await native.execute(
        f"GRANT SELECT({receipt_columns}) ON {storage_relation(policy.schema, SERVING_RECEIPT_ROOT)} "
        f"TO {storage_identifier(policy.owner_role)}"
    )


async def _require_generation_columns(native, policy):
    valid = await native.fetchval(
        "SELECT NOT EXISTS(SELECT FROM unnest($3::text[]) a(name) WHERE NOT "
        "has_column_privilege($1,to_regclass($2),a.name,'SELECT')) AND NOT EXISTS("
        "SELECT FROM unnest($4::text[]) a(name) WHERE NOT has_column_privilege($1,to_regclass($2),a.name,'UPDATE'))",
        policy.owner_role,
        storage_relation(policy.schema, GENERATION_ROOT),
        list(GENERATION_SELECT),
        list(GENERATION_UPDATE),
    )
    _require(valid is True, "doctors_generation_privileges_changed")
    receipt_readable = await native.fetchval(
        "SELECT NOT EXISTS(SELECT FROM unnest($3::text[]) a(name) WHERE NOT "
        "has_column_privilege($1,to_regclass($2),a.name,'SELECT'))",
        policy.owner_role,
        storage_relation(policy.schema, SERVING_RECEIPT_ROOT),
        list(SERVING_RECEIPT_SELECT),
    )
    _require(receipt_readable is True, "doctors_serving_receipt_privileges_changed")
