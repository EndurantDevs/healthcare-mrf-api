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
_CONTROL_SQL = """SELECT n.oid::bigint AS schema_oid,n.nspowner::bigint AS owner_oid,o.*,
 d.oid::bigint AS database_oid,current_database() AS database_name,
 c.oid::bigint AS candidate_table_oid,s.oid::bigint AS seal_table_oid,
 c.relowner=n.nspowner AND s.relowner=n.nspowner AND c.relkind='r' AND s.relkind='r'
 AND c.relpersistence='p' AND s.relpersistence='p' AND NOT s.relrowsecurity AND NOT s.relforcerowsecurity
 AND NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=s.oid AND NOT tgisinternal)
 AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=s.oid) AS owned,
 NOT EXISTS(SELECT FROM (
   SELECT a.* FROM aclexplode(coalesce(s.relacl,acldefault('r',s.relowner))) a
   UNION ALL SELECT a.* FROM pg_attribute att CROSS JOIN LATERAL aclexplode(att.attacl) a
    WHERE att.attrelid=s.oid AND att.attnum>0 AND NOT att.attisdropped
 ) a WHERE a.grantee<>n.nspowner AND (a.grantee=0 OR a.privilege_type<>'SELECT' OR a.is_grantable)) AS closed
 FROM pg_namespace n JOIN pg_roles o ON o.oid=n.nspowner
 JOIN pg_database d ON d.datname=current_database()
 JOIN pg_class c ON c.relnamespace=n.oid AND c.relname='network_membership_candidate'
 JOIN pg_class s ON s.relnamespace=n.oid AND s.relname='network_candidate_custody'
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


class NetworkCandidateCustodyError(ValueError):
    """Native identity, original authority or retained candidate custody changed."""


def _require(condition):
    if not condition:
        raise NetworkCandidateCustodyError("network_candidate_custody_unavailable")


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
    _require(type(revision) is int and revision in (1, 2))
    evidence_by_name = {}
    _require(set(candidate) == set(_STABLE) | {"state"})
    for field in _STABLE:
        field_value = candidate[field]
        if field in ("source_generations", "source_recipes_json", "validation_json") and type(field_value) is str:
            field_value = json.loads(field_value)
        if field == "validation_json" and revision == 2:
            _require(type(field_value) is dict)
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


async def _census_closed(connection, control, catalog, runtime_roles):
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


async def _final_proof(connection, target, candidate, schema):
    from process.network_membership_publication import _publication_readiness, _verify_provider_custody

    readiness = _publication_readiness(candidate)
    report = _json_object(candidate["validation_json"])
    source = PinnedAddressSource(**report["address_source"])
    await verify_registry_source_retention_rows(connection, target, candidate)
    # Unlike the READY public replay, this helper reruns complete accounting and parity.
    await _matching_inputs(connection, candidate, target, source, schema)
    await _source_semantics(
        connection,
        f"{_identifier(source.schema_name)}.{_identifier(source.table_name)}",
        f"{_identifier(target.schema_name)}.entity_address_unified",
        replay=True,
    )
    _require(
        await _index_digest(connection, target.schema_name, _serving_plan(target.schema_name))
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
    _require(actor["current"] not in runtime_roles and actor["role_oid"] != control["owner_oid"])
    for oid in {
        control["owner_oid"],
        catalog["namespace"]["nspowner"],
        *(row["relowner"] for row in catalog["relations"]),
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
    await _remove_default_grants(connection, namespace, catalog)
    await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    for name in sorted(catalog["heaps"]):
        await connection.execute(f"ALTER TABLE {namespace}.{_identifier(name)} OWNER TO {owner}")
    original = await connection.fetchval("SELECT quote_ident(current_user)")
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
        and receipt_by_name["revision"] in (1, 2)
        and _digest(receipt_by_name) == seal["seal_sha256"]
    )
    return receipt_by_name


async def _verify(connection, copy_target, schema, control, candidate, seal, *, operator=False):
    """Verify exact sealed metadata and hold immutable native relation locks."""
    _require(candidate["state"] in ("ready", "published") and seal is not None)
    receipt = _sealed_receipt(seal)
    control_identity_by_name = {
        key: control[key]
        for key in ("database_oid", "database_name", "schema_oid", "owner_oid", "candidate_table_oid", "seal_table_oid")
    }
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
    catalog = await _lock_objects(connection, copy_target, candidate, exclusive=False, assume_owner=operator)
    _require(_receipt(copy_target, roles, catalog) == closure)
    await _check_closed(connection, roles, catalog)
    await _census_closed(connection, control, catalog, runtime_roles)
    _require(
        await _index_digest(connection, copy_target.schema_name, _serving_plan(copy_target.schema_name))
        == receipt["index_definition_sha256"]
    )
    return {"seal_sha256": seal["seal_sha256"], "receipt": receipt}


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
        await _control(connection, control_schema)
        seal = await _seal(connection, copy_target, control_schema)
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
    revision = 2 if SOURCE_EVENT_ASSERTION in _json_object(candidate["validation_json"]) else 1
    receipt_by_name = {
        "component": "network_candidate_custody",
        "revision": revision,
        "runtime_roles": sorted(runtime_roles),
        "control": {
            key: control[key]
            for key in (
                "database_oid",
                "database_name",
                "schema_oid",
                "owner_oid",
                "candidate_table_oid",
                "seal_table_oid",
            )
        },
        "candidate_sha256": _digest(_evidence(candidate, revision=revision)),
        "closure": final_closure,
        "index_definition_sha256": await _index_digest(
            connection, copy_target.schema_name, _serving_plan(copy_target.schema_name)
        ),
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


async def _seal_ready_candidate(
    connection, copy_target, candidate, control, control_schema, runtime_roles, actor, *, pre_enrolled_only=False
):
    _require(candidate["state"] == "ready")
    report = _json_object(candidate["validation_json"])
    revision = 2 if SOURCE_EVENT_ASSERTION in report else 1
    if revision == 2:
        checkpoint_key, _ = _source_event_assertion(report)
        _require(checkpoint_key not in report)
    catalog = await _lock_objects(connection, copy_target, candidate, exclusive=True, assume_owner=True)
    await _operator(connection, actor, control, catalog, runtime_roles)
    await _final_proof(connection, copy_target, candidate, control_schema)
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


async def seal_network_candidate_custody(
    connection, copy_target, *, control_schema, runtime_roles, authorize, pre_enrolled_only=False
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
    from process.network_membership_publication import _publication_readiness

    _require(not connection.is_in_transaction() and callable(authorize) and type(pre_enrolled_only) is bool)
    _require(
        type(runtime_roles) is tuple and 0 < len(runtime_roles) <= 128 and len(set(runtime_roles)) == len(runtime_roles)
    )
    for role in runtime_roles:
        _identifier(role)
    task = asyncio.current_task()
    async with connection.transaction(isolation="repeatable_read"):
        actor = await _original_actor(connection)
        control = await _control(connection, control_schema)
        _require(await authorize(connection, copy_target) is True)
        _require(await connection.fetchval("SELECT pg_has_role(current_user,$1::oid,'SET')", control["owner_oid"]))
        async with _control_owner(connection, control["rolname"]):
            candidate = await _candidate(connection, copy_target, control_schema, lock=True)
        initial_catalog = await _catalog(connection, copy_target, candidate)
        await _operator(connection, actor, control, initial_catalog, runtime_roles)
        loading_owner = _publication_readiness(candidate)[3]["owner_role"]
        _require(await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", loading_owner))
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
        else:
            sealed_result = await _seal_ready_candidate(
                connection,
                copy_target,
                candidate,
                control,
                control_schema,
                runtime_roles,
                actor,
                pre_enrolled_only=pre_enrolled_only,
            )
        _require(await authorize(connection, copy_target) is True)
        _require(asyncio.current_task() is task and await _original_actor(connection) == actor)
        _require(await _control(connection, control_schema) == control)
        return sealed_result
