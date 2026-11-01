# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent native custody for one committed, fully prepared candidate.

This foundation does not activate scan-free readers or change legacy publication.
The caller supplies independent operator authority, never worker credentials.
"""

import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_serving_indexes import _index_digest, _matching_inputs, _serving_plan, _source_semantics
from process.network_membership_validation import _json_object
from process.network_membership_writer_closure import (
    _NATIVE_RUNTIME_ROLE_UNSAFE_SQL,
    _catalog,
    _check_closed,
    _check_identity,
    _protected_owner,
    _receipt,
    _remove_default_grants,
    _remove_grants,
)
from process.registry_source_retention import verify_registry_source_retention_rows

MAX_RECEIPT_BYTES = 2 * 1024 * 1024
SOURCE_EVENT_ASSERTION = "source_event_assertion"
_SOURCE_CHECKPOINTS = {
    "cms": "cms_source_notification",
    "ptg": "ptg_source_notification",
    "aca": "aca_source_notification",
}
_STABLE = (
    "dataset_id",
    "schema_id",
    "producer_id",
    "candidate_id",
    "schema_name",
    "schema_revision",
    "source_generations",
    "approved_custom_revision",
    "expected_head",
    "expected_rows",
    "accepted_rows",
    "index_ready",
    "source_recipes_json",
    "validation_json",
    "created_at",
)
_IDENTITY_SQL = """SELECT session_user::text AS session,current_user::text AS current,
 pg_backend_pid() AS backend,pg_current_xact_id()::text AS xid,
 r.oid::bigint AS role_oid,r.rolcanlogin,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls
 FROM pg_roles r WHERE r.rolname=current_user"""
_CONTROL_SQL = """SELECT n.oid::bigint AS schema_oid,s.relowner::bigint AS owner_oid,o.*,
 n.nspowner::bigint AS schema_owner_oid,q.rolname::text AS schema_owner_role,
 c.relowner::bigint AS candidate_owner_oid,q.rolname::text AS candidate_owner_role,
 d.oid::bigint AS database_oid,current_database() AS database_name,
 c.oid::bigint AS candidate_table_oid,s.oid::bigint AS seal_table_oid,
 c.relowner=n.nspowner AND c.relkind='r' AND s.relkind='r'
 AND NOT (q.rolcanlogin OR q.rolsuper OR q.rolcreatedb OR q.rolcreaterole OR q.rolreplication OR q.rolbypassrls)
 AND (s.relowner=n.nspowner OR (NOT pg_has_role(s.relowner,n.nspowner,'MEMBER')
   AND NOT pg_has_role(n.nspowner,s.relowner,'MEMBER')))
 AND c.relpersistence='p' AND s.relpersistence='p' AND NOT s.relrowsecurity AND NOT s.relforcerowsecurity
 AND NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=s.oid AND NOT tgisinternal)
 AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=s.oid) AS owned,
 NOT EXISTS(SELECT FROM (
   SELECT a.* FROM aclexplode(coalesce(s.relacl,acldefault('r',s.relowner))) a
   UNION ALL SELECT a.* FROM pg_attribute att CROSS JOIN LATERAL aclexplode(att.attacl) a
    WHERE att.attrelid=s.oid AND att.attnum>0 AND NOT att.attisdropped
 ) a WHERE a.grantee<>s.relowner AND (a.grantee=0 OR a.privilege_type<>'SELECT' OR a.is_grantable)) AS closed
 FROM pg_namespace n JOIN pg_roles q ON q.oid=n.nspowner
 JOIN pg_database d ON d.datname=current_database()
 JOIN pg_class c ON c.relnamespace=n.oid AND c.relname='network_membership_candidate'
 JOIN pg_class s ON s.relnamespace=n.oid AND s.relname='network_candidate_custody'
 JOIN pg_roles o ON o.oid=s.relowner
 WHERE n.nspname=$1"""


_READER_SQL = """SELECT configured.name FROM unnest($1::text[]) configured(name)
 JOIN pg_roles r ON r.rolname=configured.name
 WHERE has_schema_privilege(r.oid,$2::oid,'USAGE')
 AND NOT EXISTS(SELECT FROM unnest($3::oid[]) heap(oid)
   WHERE NOT has_table_privilege(r.oid,heap.oid,'SELECT')) ORDER BY configured.name"""
_CENSUS_SQL = f"""SELECT NOT EXISTS(
 SELECT FROM unnest($1::text[]) configured(name) LEFT JOIN pg_roles r ON r.rolname=configured.name
 CROSS JOIN (SELECT $2::oid AS owner_oid) s WHERE ({_NATIVE_RUNTIME_ROLE_UNSAFE_SQL})
 OR has_schema_privilege(r.oid,$3::oid,'CREATE')
 OR pg_has_role(r.oid,'pg_write_all_data','MEMBER')
 OR EXISTS(SELECT FROM unnest($4::oid[]) heap(oid)
   WHERE has_table_privilege(r.oid,heap.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   OR has_any_column_privilege(r.oid,heap.oid,'INSERT,UPDATE,REFERENCES')))
"""

_Q_REACH_SQL = """SELECT NOT EXISTS(
 SELECT FROM unnest($1::text[]) configured(name) LEFT JOIN pg_roles r ON r.rolname=configured.name
 WHERE r.oid IS NULL OR pg_has_role(r.oid,$2::oid,'MEMBER')
 OR pg_has_role(r.oid,$2::oid,'USAGE') OR pg_has_role(r.oid,$2::oid,'SET'))"""


class NetworkCandidateCustodyError(ValueError):
    """Native identity, original authority or retained candidate custody changed."""


_API_CONTROL_TABLES = frozenset(
    ("network_membership_candidate", "network_serving_manifest", "registry_approved_record")
)
_API_CONTROL_TABLE_SQL = """SELECT c.relname,c.oid::bigint,c.relowner::bigint,c.relkind::text,c.relpersistence::text,
 c.relrowsecurity,c.relforcerowsecurity,c.relispartition,
 c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap') AS heap,
 EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
 EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) AS rules,
 ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
   AND NOT a.attisdropped ORDER BY a.attnum LIMIT $3) AS columns
 FROM pg_catalog.pg_class c WHERE c.relnamespace=$1::oid AND c.relname=ANY($2::text[]) ORDER BY c.relname"""


def _require(condition):
    if not condition:
        raise NetworkCandidateCustodyError("network_candidate_custody_unavailable")


async def read_network_control_relation_pins(
    connection, relation_names, *, control_schema, expected_candidate_table_oid=None
):
    """Authenticate bounded current control-table identities, without acquiring reader locks.

    These are race pins for subsequent consumed-data checks, not historical seals
    or Source/publication authority. Existing native write guards remain intact.
    """
    from process.network_compact_layout import CompactAddressRelation
    from process.registry_management_permissions import _TABLES

    _require(connection.is_in_transaction() and type(relation_names) is tuple)
    _require(0 < len(relation_names) <= len(_API_CONTROL_TABLES))
    _require(all(type(name) is str and name in _API_CONTROL_TABLES for name in relation_names))
    _require(len(set(relation_names)) == len(relation_names))
    modes = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"
    )
    _require(tuple(modes) in (("repeatable read", "on"), ("serializable", "on")))
    control = await _control(connection, control_schema)
    if expected_candidate_table_oid is not None:
        _require(type(expected_candidate_table_oid) is int and 0 < expected_candidate_table_oid < 2**32)
        _require(control["candidate_table_oid"] == expected_candidate_table_oid)
    tables = await connection.fetch(
        _API_CONTROL_TABLE_SQL,
        control["schema_oid"],
        list(relation_names),
        max(len(_TABLES[name]) for name in relation_names) + 1,
    )
    _require(len(tables) == len(relation_names))
    by_name = {table["relname"]: table for table in tables}
    _require(set(by_name) == set(relation_names))
    pins = []
    for name in relation_names:
        table = by_name[name]
        _require(table["relowner"] == control["schema_owner_oid"])
        _require(table["relkind"] == "r" and table["relpersistence"] == "p" and table["heap"] is True)
        _require(
            not any(
                table[field]
                for field in ("relrowsecurity", "relforcerowsecurity", "relispartition", "inherited", "rules")
            )
        )
        _require(tuple(table["columns"]) == _TABLES[name])
        if name == "network_membership_candidate":
            _require(table["oid"] == control["candidate_table_oid"])
        pins.append(CompactAddressRelation(control_schema, name, table["oid"]))
    _require(await _control(connection, control_schema) == control)
    return tuple(pins)


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _digest(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def _source_event_assertion(report):
    """Validate the fixed source-event assertion retained by a revision-two seal."""
    assertion = report.get(SOURCE_EVENT_ASSERTION)
    _require(type(assertion) is dict and set(assertion) == {"revision", "kind", "checkpoint"})
    _require(type(assertion["revision"]) is int and assertion["revision"] == 1)
    kind = assertion["kind"]
    _require(type(kind) is str and kind in _SOURCE_CHECKPOINTS)
    checkpoint = assertion["checkpoint"]
    _require(type(checkpoint) is dict)
    fields = {"event_id", "attempt_id"}
    if kind == "cms":
        fields |= {"notification_sha256", "pair_sha256", "coordinates", "replaced_coordinates", "recipe_sha256"}
        if checkpoint.get("replaced_coordinates") is None:
            fields |= {"mode", "base_manifest"}
            _require(checkpoint.get("mode") == "initial" and checkpoint.get("base_manifest") is None)
    else:
        fields |= {"header", "reader_identity_sha256"}
    _require(set(checkpoint) == fields)
    try:
        for field in ("event_id", "attempt_id"):
            field_value = checkpoint[field]
            _require(type(field_value) is str and str(UUID(field_value)) == field_value and UUID(field_value).int != 0)
        for field in fields:
            if field.endswith("sha256"):
                _require(type(checkpoint[field]) is str and re.fullmatch("[0-9a-f]{64}", checkpoint[field]) is not None)
        if kind == "cms":
            from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates

            for field in ("coordinates", "replaced_coordinates"):
                field_value = checkpoint[field]
                if field == "replaced_coordinates" and field_value is None:
                    continue
                _require(type(field_value) is dict)
                coordinates = RegistryNetworkSourceCoordinates(**field_value)
                _require(coordinates.source_system == "fhir" and coordinates.source_id == "cms-npd")
        elif kind == "aca":
            from process.registry_aca_source_notifications import validate_registry_aca_source_event

            validate_registry_aca_source_event(checkpoint["header"])
        else:
            from process.registry_ptg_source_notifications import validate_registry_ptg_source_event

            validate_registry_ptg_source_event(checkpoint["header"])
    except ValueError, TypeError, KeyError, AttributeError:
        raise NetworkCandidateCustodyError("network_candidate_custody_unavailable") from None
    key = _SOURCE_CHECKPOINTS[kind]
    _require(not any(other in report for other in _SOURCE_CHECKPOINTS.values() if other != key))
    return key, checkpoint


def _evidence(candidate, *, revision=1):
    _require(type(revision) is int and revision in (1, 2, 3, 4))
    evidence_by_name = {}
    _require(set(candidate) == set(_STABLE) | {"state"})
    for field in _STABLE:
        field_value = candidate[field]
        if field in ("source_generations", "source_recipes_json", "validation_json") and type(field_value) is str:
            field_value = json.loads(field_value)
        if field == "validation_json" and revision in (2, 3, 4):
            _require(type(field_value) is dict)
        if field == "validation_json" and (
            revision == 2 or (revision in (3, 4) and SOURCE_EVENT_ASSERTION in field_value)
        ):
            key, checkpoint = _source_event_assertion(field_value)
            _require(candidate["state"] != "published" or key in field_value)
            report_by_name = dict(field_value)
            if key in report_by_name:
                _require(_encoded(report_by_name.pop(key)) == _encoded(checkpoint))
            field_value = report_by_name
        if isinstance(field_value, datetime):
            field_value = field_value.astimezone(timezone.utc).isoformat()
        evidence_by_name[field] = str(field_value) if isinstance(field_value, UUID) else field_value
    _require(len(_encoded(evidence_by_name)) <= MAX_RECEIPT_BYTES)
    return evidence_by_name


def _final_roles(owner, runtime_roles):
    _identifier(owner)
    _require(
        type(runtime_roles) is tuple
        and len(runtime_roles) <= 128
        and len(set(runtime_roles)) == len(runtime_roles)
        and owner not in runtime_roles
    )
    for role in runtime_roles:
        _identifier(role)
    return {"owner_role": owner, "loader_roles": [], "reader_roles": sorted(runtime_roles)}


async def _existing_readers(connection, catalog, runtime_roles):
    rows = await connection.fetch(
        _READER_SQL,
        list(runtime_roles),
        catalog["namespace"]["oid"],
        list(catalog["heaps"].values()),
    )
    readers = tuple(row["name"] for row in rows)
    _require(len(set(readers)) == len(readers) and set(readers) <= set(runtime_roles))
    return readers


async def _require_reader_enrollment(connection, readers, control_schema):
    """Require the direct owner-issued ledger grants a normal seal would emit."""
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM unnest($1::text[]) configured(name) "
            "LEFT JOIN pg_roles r ON r.rolname=configured.name WHERE NOT EXISTS("
            "SELECT FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a "
            "WHERE c.oid=to_regclass($2) AND a.grantee=r.oid AND a.grantor=c.relowner "
            "AND a.privilege_type='SELECT' AND NOT a.is_grantable))",
            list(readers),
            f"{_identifier(control_schema)}.network_candidate_custody",
        )
        is True
    )


async def _q_reach_closed(connection, control, runtime_roles):
    """Reject every actor's direct, inherited or SET-only schema-owner reach."""
    _require(await connection.fetchval(_Q_REACH_SQL, list(runtime_roles), control["schema_owner_oid"]) is True)


async def _census_closed(connection, control, catalog, runtime_roles):
    await _q_reach_closed(connection, control, runtime_roles)
    _require(
        await connection.fetchval(
            _CENSUS_SQL,
            list(runtime_roles),
            control["owner_oid"],
            catalog["namespace"]["oid"],
            list(catalog["heaps"].values()),
        )
        is True
    )


async def _control(connection, schema):
    _identifier(schema)
    exists = await connection.fetchval(
        "SELECT to_regclass($1)::oid", f"{_identifier(schema)}.network_candidate_custody"
    )
    _require(exists is not None)
    row = await connection.fetchrow(_CONTROL_SQL, schema)
    _require(row is not None and row["owned"] is True and row["closed"] is True and _protected_owner(row))
    return dict(row)


async def _candidate(connection, target, schema, *, lock=False):
    _require(type(target) is MembershipCopyTarget)
    row = await connection.fetchrow(
        f"SELECT * FROM {_identifier(schema)}.network_membership_candidate WHERE candidate_id=$1"
        + (" FOR UPDATE" if lock else ""),
        UUID(target.candidate_id),
    )
    _require(row is not None and all(str(row[key]) == getattr(target, key) for key in _STABLE[:5]))
    return dict(row)


async def _seal(connection, target, schema):
    return await connection.fetchrow(
        f"SELECT seal_sha256,receipt_json::text AS receipt_json FROM {_identifier(schema)}.network_candidate_custody "
        "WHERE candidate_id=$1",
        UUID(target.candidate_id),
    )


async def _heap_guards(connection, catalog):
    _require(
        await connection.fetchval(
            """SELECT count(*)=cardinality($1::oid[]) AND bool_and(
          c.relam=(SELECT oid FROM pg_am WHERE amname='heap') AND NOT c.relrowsecurity
          AND NOT c.relforcerowsecurity AND NOT c.relispartition
          AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
          AND NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
          AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid))
        FROM pg_class c WHERE c.oid=ANY($1::oid[])""",
            list(catalog["heaps"].values()),
        )
        is True
    )


async def _lock_objects(connection, target, candidate, *, exclusive, assume_owner=False):
    before = await _catalog(connection, target, candidate)
    original = await connection.fetchval("SELECT quote_ident(current_user)") if assume_owner else None
    for name in sorted(before["heaps"]):
        has_changed_role = False
        if assume_owner:
            oid = next(row["relowner"] for row in before["relations"] if row["relname"] == name)
            if not await connection.fetchval("SELECT pg_has_role(current_user,$1::oid,'USAGE')", oid):
                owner = await connection.fetchval("SELECT rolname FROM pg_roles WHERE oid=$1::oid", oid)
                _require(owner is not None)
                await connection.execute(f"SET LOCAL ROLE {_identifier(owner)}")
                has_changed_role = True
        try:
            await connection.execute(
                f"LOCK TABLE {_identifier(target.schema_name)}.{_identifier(name)} IN "
                + ("ACCESS EXCLUSIVE" if exclusive else "ACCESS SHARE")
                + " MODE NOWAIT"
            )
        finally:
            if has_changed_role:
                await connection.execute(f"SET LOCAL ROLE {original}")
    _require(await _catalog(connection, target, candidate) == before)
    await _heap_guards(connection, before)
    return before


async def _address_runtime_closure(connection, candidate, runtime_roles):
    """Keep every actual candidate actor unable to alter or assume the retained owner."""
    address_source = PinnedAddressSource(**_json_object(candidate["validation_json"])["address_source"])
    await _retained_address_runtime_closure(connection, address_source, runtime_roles)


async def _retained_address_runtime_closure(connection, address_source, runtime_roles):
    """Apply the same actor refusal to an authenticated address dependency."""
    safe = await connection.fetchval(
        """WITH target AS (
          SELECT c.oid,c.relowner,n.oid AS namespace_oid,n.nspowner,owner.rolcanlogin,owner.rolsuper,
            owner.rolcreaterole,owner.rolcreatedb,owner.rolreplication,owner.rolbypassrls FROM pg_class c
          JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles owner ON owner.oid=c.relowner
          WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='r' AND c.relpersistence='p'
        ), actors AS (SELECT r.* FROM unnest($3::text[]) selected(name)
          LEFT JOIN pg_roles r ON r.rolname=selected.name)
        SELECT (SELECT count(*)=1 AND bool_and(relowner=nspowner AND NOT (rolcanlogin OR rolsuper
          OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls)) FROM target) AND NOT EXISTS(
          SELECT FROM actors r,target t WHERE r.oid IS NULL OR r.rolsuper OR r.rolcreaterole
          OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
          OR pg_has_role(r.oid,t.relowner,'MEMBER') OR pg_has_role(r.oid,t.relowner,'USAGE')
          OR pg_has_role(r.oid,t.relowner,'SET') OR pg_has_role(r.oid,t.nspowner,'MEMBER')
          OR pg_has_role(r.oid,t.nspowner,'USAGE') OR pg_has_role(r.oid,t.nspowner,'SET')
          OR has_schema_privilege(r.oid,t.namespace_oid,'CREATE')
          OR has_parameter_privilege(r.oid,'session_replication_role','SET')
          OR has_table_privilege(r.oid,t.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
          OR has_any_column_privilege(r.oid,t.oid,'INSERT,UPDATE,REFERENCES'))
        AND NOT EXISTS(SELECT FROM pg_proc p,actors r WHERE p.prosecdef
          AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype)
          AND has_schema_privilege(r.oid,p.pronamespace,'USAGE') AND has_function_privilege(r.oid,p.oid,'EXECUTE')
          AND EXISTS(SELECT FROM pg_roles definer,target t WHERE definer.oid=p.proowner
            AND (definer.rolsuper OR definer.rolcreaterole OR pg_has_role(definer.oid,t.relowner,'MEMBER')
              OR pg_has_role(definer.oid,t.relowner,'SET') OR pg_has_role(definer.oid,t.nspowner,'MEMBER')
              OR pg_has_role(definer.oid,t.nspowner,'SET'))))""",
        address_source.schema_name,
        address_source.table_name,
        list(runtime_roles),
    )
    _require(safe is True)


@asynccontextmanager
async def _preservation_scope(
    connection, copy_target, candidate, schema, inventory_entries, *, source_witness_runtime=None
):
    """Keep authenticated carry selections alive through full and compact custody, then drain cleanup."""
    from process.registry_fhir_serving_preservation import (
        cleanup_fhir_serving_preservation,
        rederive_fhir_serving_preservation,
        verify_fhir_preservation_target_rows,
    )

    preservation = await rederive_fhir_serving_preservation(
        connection,
        copy_target,
        candidate,
        control_schema=schema,
        inventory_entries=inventory_entries,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    )
    primary = None
    try:
        if preservation is not None:
            await verify_fhir_preservation_target_rows(
                connection, copy_target, candidate, preservation, control_schema=schema
            )
        yield preservation
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            await cleanup_fhir_serving_preservation(connection, preservation, primary=primary)
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note("FHIR preservation cleanup failed; caller rollback remains required.")
            primary._registry_preservation_cleanup_error = cleanup_error


async def _final_proof(
    connection, copy_target, candidate, schema, *, office_authority=None, inventory_entries=None, **source_options
):
    """Reconstruct the complete closed candidate under independent Source and office context."""
    _require(set(source_options) <= {"source_witness_runtime"})
    from process.network_membership_publication import _publication_readiness, _verify_provider_custody

    readiness = _publication_readiness(candidate)
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    async with _preservation_scope(
        connection, copy_target, candidate, schema, inventory_entries, **source_options
    ) as preservation:
        await verify_registry_source_retention_rows(connection, copy_target, candidate)
        from process.network_source_contribution_custody import verify_independent_source_contributions

        source_preservation = await verify_independent_source_contributions(
            connection,
            copy_target,
            candidate,
            control_schema=schema,
            office_authority=office_authority,
            preservation=preservation,
            **source_options,
            **({"inventory_entries": inventory_entries} if inventory_entries is not None else {}),
        )
        from process.registry_source_office_custody import verify_independent_initial_source_offices

        if inventory_entries is None and source_preservation is None:
            await verify_independent_initial_source_offices(
                connection,
                copy_target,
                candidate,
                control_schema=schema,
                office_authority=office_authority,
                preservation=preservation,
            )
        from process.network_compact_readiness import is_compact_candidate

        if is_compact_candidate(candidate):
            from process.network_compact_validation import verify_compact_final_proof

            proof_options_by_name = {"control_schema": schema, "office_authority": office_authority, **source_options}
            await verify_compact_final_proof(connection, copy_target, candidate, **proof_options_by_name)
            await _verify_provider_custody(connection, candidate, readiness[3])
            return
        # Unlike the READY public replay, this helper reruns complete accounting and parity.
        await _matching_inputs(connection, candidate, copy_target, address_source, schema)
        await _source_semantics(
            connection,
            f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
            f"{_identifier(copy_target.schema_name)}.entity_address_unified",
            replay=True,
        )
        _require(
            await _index_digest(connection, copy_target.schema_name, _serving_plan(copy_target.schema_name))
            == readiness[2]["index_definition_sha256"]
        )
        await _verify_provider_custody(connection, candidate, readiness[3])


async def _original_actor(connection):
    actor_by_name = dict(await connection.fetchrow(_IDENTITY_SQL))
    _require(actor_by_name["session"] == actor_by_name["current"] and actor_by_name["rolcanlogin"] is True)
    _require(
        actor_by_name["rolsuper"] is True
        or not any(actor_by_name[key] for key in ("rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"))
    )
    return actor_by_name


@asynccontextmanager
async def _control_owner(connection, owner):
    original = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"SET LOCAL ROLE {_identifier(owner)}")
    try:
        yield
    finally:
        await connection.execute(f"SET LOCAL ROLE {original}")


async def _operator(connection, actor, control, catalog, runtime_roles):
    await _q_reach_closed(connection, control, runtime_roles)
    _require(actor["current"] not in runtime_roles and actor["role_oid"] != control["owner_oid"])
    if control["schema_owner_oid"] != control["owner_oid"] and actor["rolsuper"] is not True:
        _require(
            not await connection.fetchval("SELECT pg_has_role(current_user,$1::oid,'SET')", control["schema_owner_oid"])
        )
        _require(
            not await connection.fetchval(
                "SELECT pg_has_role(current_user,$1::oid,'USAGE')", control["schema_owner_oid"]
            )
        )
        _require(
            not await connection.fetchval(
                "SELECT pg_has_role(current_user,$1::oid,'MEMBER')", control["schema_owner_oid"]
            )
        )
    for oid in {
        control["owner_oid"],
        catalog["namespace"]["nspowner"],
        *(relation["relowner"] for relation in catalog["relations"]),
    }:
        _require(actor["role_oid"] != oid)
        _require(await connection.fetchval("SELECT pg_has_role(current_user,$1::oid,'SET')", oid))
    # PostgreSQL superusers can transfer schemas without widening the final owner's grants.
    if actor["rolsuper"] is not True:
        _require(
            await connection.fetchval(
                "SELECT has_database_privilege(current_user,current_database(),'CREATE') AND "
                "has_database_privilege($1::oid,current_database(),'CREATE')",
                control["owner_oid"],
            )
        )


async def _transfer(connection, target, roles, catalog):
    namespace, owner = _identifier(target.schema_name), _identifier(roles["owner_role"])
    for oid in {catalog["namespace"]["nspowner"], *(row["relowner"] for row in catalog["relations"])}:
        _require(await connection.fetchval("SELECT pg_has_role(current_user,$1::oid,'USAGE')", oid))
    await _remove_default_grants(connection, namespace, catalog)
    original = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    await connection.execute(f"SET LOCAL ROLE {owner}")
    try:
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {original}")
    finally:
        await connection.execute(f"SET LOCAL ROLE {original}")
    for name in sorted(catalog["heaps"]):
        await connection.execute(f"ALTER TABLE {namespace}.{_identifier(name)} OWNER TO {owner}")
    await connection.execute(f"SET LOCAL ROLE {owner}")
    try:
        owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", roles["owner_role"])
        await _remove_grants(connection, namespace, catalog, owner_oid)
        for role in roles["reader_roles"]:
            await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {_identifier(role)}")
            for name in sorted(catalog["heaps"]):
                await connection.execute(f"GRANT SELECT ON {namespace}.{_identifier(name)} TO {_identifier(role)}")
    finally:
        await connection.execute(f"SET LOCAL ROLE {original}")


def _sealed_receipt(seal):
    document = seal["receipt_json"]
    _require(type(document) is str and len(document.encode()) <= MAX_RECEIPT_BYTES)
    try:
        receipt_by_name = json.loads(document)
    except ValueError:
        raise NetworkCandidateCustodyError("network_candidate_custody_unavailable") from None
    _require(
        type(receipt_by_name) is dict
        and set(receipt_by_name)
        == {
            "component",
            "revision",
            "control",
            "candidate_sha256",
            "closure",
            "index_definition_sha256",
            "runtime_roles",
        }
    )
    _require(
        receipt_by_name["component"] == "network_candidate_custody"
        and type(receipt_by_name["revision"]) is int
        and receipt_by_name["revision"] in (1, 2, 3, 4)
        and _digest(receipt_by_name) == seal["seal_sha256"]
    )
    return receipt_by_name


def _control_identity(control, revision):
    keys = ("database_oid", "database_name", "schema_oid", "owner_oid", "candidate_table_oid", "seal_table_oid")
    if revision in (3, 4):
        if revision == 3:
            _require(control["schema_owner_oid"] != control["owner_oid"])
        keys += ("schema_owner_oid", "schema_owner_role", "candidate_owner_oid", "candidate_owner_role")
    else:
        # Old seals bind the single owner and cannot survive a ledger-only transfer.
        _require(control["schema_owner_oid"] == control["candidate_owner_oid"] == control["owner_oid"])
    identity_by_name = {key: control[key] for key in keys}
    if revision in (3, 4):
        identity_by_name["owner_role"] = control["rolname"]
    return identity_by_name


async def _candidate_index_digest(connection, candidate):
    from process.network_compact_readiness import compact_index_digest, is_compact_candidate

    if is_compact_candidate(candidate):
        return await compact_index_digest(connection, candidate["schema_name"], candidate=candidate)
    return await _index_digest(connection, candidate["schema_name"], _serving_plan(candidate["schema_name"]))


async def _verify(connection, copy_target, schema, control, candidate, seal, *, operator=False, api_read=False):
    """Verify sealed metadata; API metadata checks defer physical pins to actual data reads."""
    _require(type(api_read) is bool and not (api_read and operator))
    _require(candidate["state"] in ("ready", "published") and seal is not None)
    receipt = _sealed_receipt(seal)
    from process.network_compact_readiness import is_compact_candidate

    _require((receipt["revision"] == 4) == is_compact_candidate(candidate))
    control_identity_by_name = _control_identity(control, receipt["revision"])
    _require(
        receipt["control"] == control_identity_by_name
        and receipt["candidate_sha256"] == _digest(_evidence(candidate, revision=receipt["revision"]))
    )
    runtime_roles = receipt["runtime_roles"]
    _require(
        type(runtime_roles) is list and 0 < len(runtime_roles) <= 128 and runtime_roles == sorted(set(runtime_roles))
    )
    for role in runtime_roles:
        _identifier(role)
    closure = receipt["closure"]
    _require(
        type(closure) is dict
        and type(closure.get("reader_roles")) is list
        and closure.get("loader_roles") == []
        and closure.get("owner_role") == control["rolname"]
    )
    _require(set(closure["reader_roles"]) <= set(runtime_roles))
    roles = _final_roles(control["rolname"], tuple(closure["reader_roles"]))
    _require(closure["owner_role"] == control["rolname"] and closure["loader_roles"] == [])
    if api_read:
        catalog = await _catalog(connection, copy_target, candidate)
        await _heap_guards(connection, catalog)
    else:
        catalog = await _lock_objects(connection, copy_target, candidate, exclusive=False, assume_owner=operator)
    _require(_receipt(copy_target, roles, catalog) == closure)
    await _check_closed(connection, roles, catalog)
    await _census_closed(connection, control, catalog, runtime_roles)
    await _address_runtime_closure(connection, candidate, runtime_roles)
    _require(await _candidate_index_digest(connection, candidate) == receipt["index_definition_sha256"])
    return {"seal_sha256": seal["seal_sha256"], "receipt": receipt}


async def verify_network_candidate_read_metadata(connection, copy_target, *, control_schema, required=True):
    """Authenticate API metadata without acquiring locks on candidate data tables.

    This does not authorize exposing data. Every consuming query must verify its
    actual physical identities on this same backend/transaction before rows reach
    application code or a cache. Producers and custodians use the locking verifier.
    """
    _require(connection.is_in_transaction() and type(copy_target) is MembershipCopyTarget and type(required) is bool)
    _identifier(control_schema)
    modes = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"
    )
    _require(tuple(modes) in (("repeatable read", "on"), ("serializable", "on")))
    if not required:
        exists = await connection.fetchval(
            "SELECT to_regclass($1)::oid", f"{_identifier(control_schema)}.network_candidate_custody"
        )
        if exists is None:
            return None
    seal = await _seal(connection, copy_target, control_schema)
    if seal is None and not required:
        return None
    control = await _control(connection, control_schema)
    candidate = await _candidate(connection, copy_target, control_schema)
    result = await _verify(connection, copy_target, control_schema, control, candidate, seal, api_read=True)
    _require(await _control(connection, control_schema) == control)
    return result


async def verify_network_candidate_custody(connection, copy_target, *, control_schema, required=True):
    """Verify a retained seal and hold same-object AccessShare locks in caller RR RO/RW.

    No candidate fact rows are read. Missing legacy seals refuse; callers retain
    their explicit legacy full-validation path. This grants no publication authority.
    """
    _require(connection.is_in_transaction())
    _require(type(required) is bool)
    _require(type(copy_target) is MembershipCopyTarget)
    _identifier(control_schema)
    if not required:
        exists = await connection.fetchval(
            "SELECT to_regclass($1)::oid", f"{_identifier(control_schema)}.network_candidate_custody"
        )
        if exists is None:
            return None
        seal = await _seal(connection, copy_target, control_schema)
        if seal is None:
            return None
    else:
        control = await _control(connection, control_schema)
        seal = await _seal(connection, copy_target, control_schema)
    if not required:
        control = await _control(connection, control_schema)
    candidate = await _candidate(connection, copy_target, control_schema)
    _require(await connection.fetchval("SHOW transaction_isolation") in ("repeatable read", "serializable"))
    result = await _verify(connection, copy_target, control_schema, control, candidate, seal)
    _require(await _control(connection, control_schema) == control)
    return result


async def _store_candidate_seal(
    connection,
    copy_target,
    candidate,
    control,
    control_schema,
    runtime_roles,
    final_closure,
    *,
    pre_enrolled_only=False,
):
    from process.network_compact_readiness import is_compact_candidate

    revision = (
        4
        if is_compact_candidate(candidate)
        else 3
        if control["schema_owner_oid"] != control["owner_oid"]
        else 2
        if SOURCE_EVENT_ASSERTION in _json_object(candidate["validation_json"])
        else 1
    )
    receipt_by_name = {
        "component": "network_candidate_custody",
        "revision": revision,
        "runtime_roles": sorted(runtime_roles),
        "control": _control_identity(control, revision),
        "candidate_sha256": _digest(_evidence(candidate, revision=revision)),
        "closure": final_closure,
        "index_definition_sha256": await _candidate_index_digest(connection, candidate),
    }
    _require(len(_encoded(receipt_by_name)) <= MAX_RECEIPT_BYTES)
    if pre_enrolled_only:
        await _require_reader_enrollment(connection, final_closure["reader_roles"], control_schema)
    async with _control_owner(connection, control["rolname"]):
        await connection.execute(
            f"INSERT INTO {_identifier(control_schema)}.network_candidate_custody "
            "(candidate_id,seal_sha256,receipt_json) VALUES($1,$2,$3::jsonb)",
            UUID(copy_target.candidate_id),
            _digest(receipt_by_name),
            _encoded(receipt_by_name).decode(),
        )
        if not pre_enrolled_only:
            for role in final_closure["reader_roles"]:
                await connection.execute(
                    f"GRANT SELECT ON {_identifier(control_schema)}.network_candidate_custody TO {_identifier(role)}"
                )
        stored = await _seal(connection, copy_target, control_schema)
    return await _verify(
        connection,
        copy_target,
        control_schema,
        control,
        candidate,
        stored,
        operator=True,
    )


def _validated_seal_options(options):
    """Keep the two private seal modes closed without changing public authority checks."""
    _require(set(options) <= {"pre_enrolled_only", "office_authority", "source_witness_runtime"})
    pre_enrolled_only = options.get("pre_enrolled_only", False)
    _require(type(pre_enrolled_only) is bool)
    return pre_enrolled_only, options.get("office_authority")


async def _seal_ready_candidate(
    connection,
    copy_target,
    candidate,
    control,
    control_schema,
    runtime_roles,
    actor,
    **seal_options,
):

    pre_enrolled_only, office_authority = _validated_seal_options(seal_options)
    _require(candidate["state"] == "ready")
    report = _json_object(candidate["validation_json"])
    revision = 2 if SOURCE_EVENT_ASSERTION in report else 1
    if revision == 2:
        checkpoint_key, _ = _source_event_assertion(report)
        _require(checkpoint_key not in report)
    catalog = await _prove_ready_candidate(
        connection,
        copy_target,
        candidate,
        control,
        control_schema,
        (runtime_roles, actor),
        seal_options,
    )
    # Preserve only pre-existing effective reads; the forbidden census grants no fact access.
    readers = await _existing_readers(connection, catalog, runtime_roles)
    roles = _final_roles(control["rolname"], readers)
    await _transfer(connection, copy_target, roles, catalog)
    closed = await _catalog(connection, copy_target, candidate)
    _require(catalog["namespace"]["oid"] == closed["namespace"]["oid"] and catalog["heaps"] == closed["heaps"])
    # Owners/ACLs change, while every heap/index OID and index binding must remain exact.
    before, after = _receipt(copy_target, roles, catalog), _receipt(copy_target, roles, closed)
    _require(before == after)
    await _check_closed(connection, roles, closed)
    await _census_closed(connection, control, closed, runtime_roles)
    await _address_runtime_closure(connection, candidate, runtime_roles)
    return await _store_candidate_seal(
        connection,
        copy_target,
        candidate,
        control,
        control_schema,
        runtime_roles,
        after,
        pre_enrolled_only=pre_enrolled_only,
    )


async def _prove_ready_candidate(
    connection,
    copy_target,
    candidate,
    control,
    schema,
    actor_scope,
    proof_options,
):
    from process.network_membership_publication import _publication_writer_closure
    from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
    from process.registry_source_inventory_reference import source_inventory_heaps
    from process.registry_source_inventory_store import INVENTORY_TABLE, _load_locked_source_inventory
    from process.registry_source_recipe_store import decode_registry_source_recipes, verify_registry_source_recipes

    runtime_roles, actor = actor_scope
    office_authority = proof_options.get("office_authority")
    source_witness_runtime = proof_options.get("source_witness_runtime")
    if source_witness_runtime is not None:
        from process.registry_source_recipe_composition import SourceWitnessRuntime

        _require(type(source_witness_runtime) is SourceWitnessRuntime)
        source_witness_runtime.__post_init__()
        _require(set(runtime_roles) | {actor["current"]} <= set(source_witness_runtime.runtime_roles))
    catalog = await _lock_objects(connection, copy_target, candidate, exclusive=True, assume_owner=True)
    retained = _publication_writer_closure(candidate, _json_object(candidate["validation_json"]))
    prepared_roles_by_group = {name: retained[name] for name in ("owner_role", "loader_roles", "reader_roles")}
    _check_identity(_receipt(copy_target, prepared_roles_by_group, catalog), retained, extending=False)
    await _operator(connection, actor, control, catalog, runtime_roles)
    await _address_runtime_closure(connection, candidate, runtime_roles)
    entries = None
    if source_inventory_heaps(candidate):
        _inventory, entries = await _load_locked_source_inventory(
            connection,
            copy_target,
            candidate,
            expected_table_oid=catalog["heaps"][INVENTORY_TABLE],
            control_schema=schema,
        )
    if office_authority is None:
        recipes = (
            verify_registry_source_recipes(candidate)
            if entries is None
            else (decode_registry_source_recipes("[" + entry.recipe_json + "]")[0] for entry in entries)
        )
        _require(not any(type(recipe.source_pin) is PinnedPTGOfficeMembershipSource for recipe in recipes))
    options_by_name = {}
    if source_witness_runtime is not None:
        options_by_name["source_witness_runtime"] = source_witness_runtime
    if office_authority is not None:
        options_by_name["office_authority"] = office_authority
    if entries is not None:
        options_by_name["inventory_entries"] = entries
    await _final_proof(connection, copy_target, candidate, schema, **options_by_name)
    return catalog


async def _seal_or_verify_candidate(
    connection,
    copy_target,
    candidate,
    control,
    control_schema,
    runtime_roles,
    actor,
    **seal_options,
):

    _require("pre_enrolled_only" in seal_options)
    pre_enrolled_only, office_authority = _validated_seal_options(seal_options)
    async with _control_owner(connection, control["rolname"]):
        existing = await _seal(connection, copy_target, control_schema)
    if existing is not None:
        sealed_result = await _verify(
            connection, copy_target, control_schema, control, candidate, existing, operator=True
        )
        _require(sealed_result["receipt"]["runtime_roles"] == sorted(runtime_roles))
        if pre_enrolled_only:
            await _require_reader_enrollment(
                connection, sealed_result["receipt"]["closure"]["reader_roles"], control_schema
            )
        return sealed_result
    return await _seal_ready_candidate(
        connection,
        copy_target,
        candidate,
        control,
        control_schema,
        runtime_roles,
        actor,
        pre_enrolled_only=pre_enrolled_only,
        office_authority=office_authority,
        **(
            {"source_witness_runtime": seal_options["source_witness_runtime"]}
            if seal_options.get("source_witness_runtime") is not None
            else {}
        ),
    )


def _seal_options(runtime_roles, authorize, pre_enrolled_only):
    _require(callable(authorize) and type(pre_enrolled_only) is bool)
    _require(
        type(runtime_roles) is tuple and 0 < len(runtime_roles) <= 128 and len(set(runtime_roles)) == len(runtime_roles)
    )
    for role in runtime_roles:
        _identifier(role)


async def _seal_in_transaction(
    connection,
    copy_target,
    *,
    control_schema,
    runtime_roles,
    authorize,
    pre_enrolled_only,
    office_authority=None,
    source_witness_runtime=None,
):
    from process.network_membership_publication import _publication_readiness

    _require(connection.is_in_transaction())
    _require(await connection.fetchval("SHOW transaction_isolation") in ("repeatable read", "serializable"))
    task = asyncio.current_task()
    actor = await _original_actor(connection)
    control = await _control(connection, control_schema)
    _require(await authorize(connection, copy_target) is True)
    _require(await connection.fetchval("SELECT pg_has_role(current_user,$1::oid,'SET')", control["owner_oid"]))
    if control["schema_owner_oid"] != control["owner_oid"]:
        candidate = await _candidate(connection, copy_target, control_schema, lock=True)
    else:
        async with _control_owner(connection, control["rolname"]):
            candidate = await _candidate(connection, copy_target, control_schema, lock=True)
    initial_catalog = await _catalog(connection, copy_target, candidate)
    await _operator(connection, actor, control, initial_catalog, runtime_roles)
    loading_owner = _publication_readiness(candidate)[3]["owner_role"]
    _require(await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", loading_owner))
    sealed_result = await _seal_or_verify_candidate(
        connection,
        copy_target,
        candidate,
        control,
        control_schema,
        runtime_roles,
        actor,
        pre_enrolled_only=pre_enrolled_only,
        office_authority=office_authority,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    )
    _require(await authorize(connection, copy_target) is True)
    _require(asyncio.current_task() is task and await _original_actor(connection) == actor)
    await _q_reach_closed(connection, control, runtime_roles)
    _require(await _control(connection, control_schema) == control)
    return sealed_result


async def seal_network_candidate_custody(
    connection,
    copy_target,
    *,
    control_schema,
    runtime_roles,
    authorize,
    pre_enrolled_only=False,
    source_witness_runtime=None,
):
    """Commit exact READY custody on an independent original authorized operator.

    All preparation must already be committed. `authorize(connection, target)`
    authenticates the independent custodian and exact target before and after
    mutation. Trusted callers validate the durable request/source before sealing
    and recheck authority, source selection and expected head at publication.
    The seal closes READY storage; it grants no publication authority.
    `runtime_roles` is the trusted complete ordinary publisher/loader/reader census.
    `pre_enrolled_only` requires existing ledger reader grants and never enrolls them.
    """
    _require(not connection.is_in_transaction())
    _seal_options(runtime_roles, authorize, pre_enrolled_only)
    async with connection.transaction(isolation="repeatable_read"):
        return await _seal_in_transaction(
            connection,
            copy_target,
            control_schema=control_schema,
            runtime_roles=runtime_roles,
            authorize=authorize,
            pre_enrolled_only=pre_enrolled_only,
            **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
        )


async def seal_network_candidate_custody_in_session(
    session,
    copy_target,
    *,
    control_schema,
    runtime_roles,
    authorize,
    office_store,
    office_read_budget,
    **seal_options,
):
    """Seal under the independent custodian's genuine pre-snapshot fenced session.

    The caller owns the company-approval transaction and commits it after this
    returns. Producer sessions and manufactured office authority are refused.
    """
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.registry_ptg_office_membership import require_ptg_office_authority

    _require(set(seal_options) <= {"pre_enrolled_only", "source_witness_runtime"})
    options_by_name = {"pre_enrolled_only": False, **seal_options}
    _seal_options(runtime_roles, authorize, options_by_name["pre_enrolled_only"])
    _require(type(session) is AsyncSession and session.in_transaction())
    connection = (await (await session.connection()).get_raw_connection()).driver_connection
    office_authority = await require_ptg_office_authority(
        connection, control_schema, (session, office_store, office_read_budget)
    )
    async with connection.transaction():
        sealed = await _seal_in_transaction(
            connection,
            copy_target,
            control_schema=control_schema,
            runtime_roles=runtime_roles,
            authorize=authorize,
            office_authority=office_authority,
            **options_by_name,
        )
        await require_ptg_office_authority(connection, control_schema, office_authority)
        return sealed
