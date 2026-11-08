# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Selected exact-OID enrollment through original native rights and catalog-only handoffs."""

from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import asdict, dataclass

from process import npi_result_generation as npi

_TABLE = "cms_native_input_revision"
_TRIGGER = "cms_native_input_mutation"


@dataclass(frozen=True)
class NativeInputCandidate:
    """Bind one fixed input name to its exact unpublished physical heap."""

    target_schema: str
    target_table: str
    stage_schema: str
    stage_table: str
    relation_oid: int
    owner_oid: int
    incumbent_oid: int | None

    def __post_init__(self):
        for name in (self.target_schema, self.target_table, self.stage_schema, self.stage_table):
            if type(name) is not str or npi._schema_name(name) != name:
                raise RuntimeError("cms_native_input_candidate_invalid")
        for oid in (self.relation_oid, self.owner_oid):
            if type(oid) is not int or not 0 < oid < 2**32:
                raise RuntimeError("cms_native_input_candidate_invalid")
        if self.incumbent_oid is not None and (
            type(self.incumbent_oid) is not int or not 0 < self.incumbent_oid < 2**32
        ):
            raise RuntimeError("cms_native_input_candidate_invalid")


def _registration_roots(schema):
    from process.provider_directory_cms_native_inputs import _relations

    excluded_roots = {(schema, name) for name in (*npi.RELATION_NAMES, "doctor_clinician_address", "geo_zip_lookup")}
    return set(_relations(schema)) - excluded_roots


def native_input_registration_roots(schema):
    """Name the fixed generic inputs; native NPI, Doctors and Geo keep their own authority."""
    return tuple(sorted(_registration_roots(npi._schema_name(schema))))


def _selected_candidates(schema, candidates):
    schema = npi._schema_name(schema)
    if type(candidates) is not tuple or not 0 < len(candidates) <= len(_registration_roots(schema)):
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    targets, oids = set(), set()
    for candidate in candidates:
        if type(candidate) is not NativeInputCandidate:
            raise RuntimeError("cms_native_input_candidate_invalid")
        target = candidate.target_schema, candidate.target_table
        if target not in _registration_roots(schema) or target in targets or candidate.relation_oid in oids:
            raise RuntimeError("cms_native_input_candidate_scope_changed")
        targets.add(target)
        oids.add(candidate.relation_oid)
    return schema


def _candidate_relation(schema, table):
    return f'"{npi._schema_name(schema)}"."{npi._schema_name(table)}"'


async def _native_candidate_heap(connection, *, target_schema, target_table, stage_schema, stage_table):
    row = await connection.fetchrow(
        "SELECT c.oid::bigint AS relation_oid,c.relowner::bigint AS owner_oid,"
        "to_regclass($2)::oid::bigint AS incumbent_oid,c.relkind::text,c.relpersistence::text,"
        "c.relrowsecurity,c.relforcerowsecurity,c.relispartition,"
        "EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited "
        "FROM pg_class c WHERE c.oid=to_regclass($1)",
        _candidate_relation(stage_schema, stage_table),
        _candidate_relation(target_schema, target_table),
    )
    if row is None or (
        row["relkind"] != "r"
        or row["relpersistence"] != "p"
        or row["relrowsecurity"]
        or row["relforcerowsecurity"]
        or row["relispartition"]
        or row["inherited"]
    ):
        raise RuntimeError("cms_native_input_candidate_heap_changed")
    return NativeInputCandidate(
        target_schema,
        target_table,
        stage_schema,
        stage_table,
        row["relation_oid"],
        row["owner_oid"],
        row["incumbent_oid"],
    )


async def capture_native_input_candidate(
    connection, *, ledger_schema, target_schema, target_table, stage_schema, stage_table
):
    """Describe an exact existing heap; this does not authorize operator enrollment."""
    ledger_schema = npi._schema_name(ledger_schema)
    if (target_schema, target_table) not in _registration_roots(ledger_schema):
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    return await _native_candidate_heap(
        connection,
        target_schema=target_schema,
        target_table=target_table,
        stage_schema=stage_schema,
        stage_table=stage_table,
    )


async def _current_input_root_records(connection, ledger_schema):
    roots = native_input_registration_roots(ledger_schema)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    root_records = await connection.fetch(
        "SELECT requested.namespace,requested.name,c.oid::bigint AS oid,c.relkind::text AS kind "
        "FROM unnest($1::text[],$2::text[]) requested(namespace,name) LEFT JOIN pg_class c "
        "ON c.oid=to_regclass(format('%I.%I',namespace,name)) ORDER BY namespace,name",
        [schema for schema, _ in roots],
        [name for _, name in roots],
    )
    if [(root_record["namespace"], root_record["name"]) for root_record in root_records] != list(roots):
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    return root_records


async def _current_protected_fhir(connection, ledger_schema, root_records, source_policy):
    from process.provider_directory_generic_source_provision import FHIR_INPUT_ROOTS

    fhir_records = [
        root_record
        for root_record in root_records
        if root_record["namespace"] == ledger_schema and root_record["name"] in FHIR_INPUT_ROOTS
    ]
    protected_fhir = None
    if any(root_record["kind"] == "p" for root_record in fhir_records):
        from process.provider_directory_cms_native_inputs import capture_protected_fhir_input
        from process.provider_directory_generic_source_policy import GenericSourcePolicy

        if type(source_policy) is not GenericSourcePolicy or source_policy.schema != ledger_schema:
            raise RuntimeError("cms_native_input_source_policy_required")
        protected_fhir = await capture_protected_fhir_input(connection, source_policy=source_policy)
        if protected_fhir is None or {
            root_record["name"]: root_record["oid"] for root_record in fhir_records if root_record["kind"] == "p"
        } != dict(protected_fhir["parent_oids"]):
            raise RuntimeError("cms_native_input_candidate_identity_changed")
    return protected_fhir


async def capture_current_native_input_candidates(
    connection, *, ledger_schema, protected_tiger=None, source_policy=None
):
    """Keep every absent initial root explicit; current heaps are their own incumbent."""
    from process.provider_directory_generic_source_provision import FHIR_INPUT_ROOTS

    root_records = await _current_input_root_records(connection, ledger_schema)
    protected_fhir = await _current_protected_fhir(connection, ledger_schema, root_records, source_policy)
    tiger_by_name, tiger_names = {}, ()
    if protected_tiger is not None:
        from process.provider_directory_cms_tiger_inputs import ROOTS, verify_raw_protected_tiger_input

        protected_tiger = await verify_raw_protected_tiger_input(connection, protected_tiger)
        tiger_names = ROOTS
        if protected_tiger["version"] == 1:
            tiger_by_name = dict(protected_tiger["parent_oids"])
    candidates, absent, protected_roots = [], [], []
    for root_record in root_records:
        if (
            protected_fhir is not None
            and root_record["namespace"] == ledger_schema
            and root_record["name"] in FHIR_INPUT_ROOTS
        ):
            protected_roots.append(dict(schema=ledger_schema, table=root_record["name"], proof=protected_fhir))
            continue
        if root_record["namespace"] == "tiger" and root_record["name"] in tiger_names:
            if protected_tiger["version"] == 1 and (
                root_record["oid"] != tiger_by_name[root_record["name"]] or root_record["kind"] != "r"
            ):
                raise RuntimeError("cms_native_input_candidate_identity_changed")
            protected_roots.append(dict(schema="tiger", table=root_record["name"], proof=protected_tiger))
            continue
        if root_record["oid"] is None:
            absent.append((root_record["namespace"], root_record["name"]))
            continue
        if (root_record["namespace"], root_record["name"], root_record["kind"]) == ("mrf", "nucc_taxonomy", "p"):
            from process.provider_directory_cms_native_inputs import capture_protected_nucc_input

            proof = await capture_protected_nucc_input(_NativeNuccSession(connection), schema=ledger_schema)
            if proof["physical"]["parent_oid"] != root_record["oid"]:
                raise RuntimeError("cms_native_input_candidate_identity_changed")
            protected_roots.append(dict(schema="mrf", table="nucc_taxonomy", proof=proof))
            continue
        candidate = await capture_native_input_candidate(
            connection,
            ledger_schema=ledger_schema,
            target_schema=root_record["namespace"],
            target_table=root_record["name"],
            stage_schema=root_record["namespace"],
            stage_table=root_record["name"],
        )
        if candidate.relation_oid != root_record["oid"] or candidate.incumbent_oid != root_record["oid"]:
            raise RuntimeError("cms_native_input_candidate_identity_changed")
        candidates.append(candidate)
    capture_by_field = {"candidates": tuple(candidates), "absent": tuple(absent)}
    if protected_roots:
        capture_by_field["protected_roots"] = tuple(protected_roots)
    if protected_fhir is not None:
        capture_by_field["protected_fhir"] = protected_fhir
    return capture_by_field


async def _closed_candidate_publisher(connection, candidate, pair):
    if pair is None:
        return 0
    if (
        type(pair) is not tuple
        or len(pair) != 2
        or any(type(oid) is not int or not 0 < oid < 2**32 for oid in pair)
        or pair[0] == pair[1]
    ):
        raise RuntimeError("cms_native_input_closed_authority_changed")
    if candidate.owner_oid != pair[0]:
        return 0
    valid = await connection.fetchval(
        "SELECT owner.oid=$1::oid AND publisher.oid=$2::oid AND NOT owner.rolcanlogin "
        "AND NOT owner.rolsuper AND NOT owner.rolcreaterole AND NOT owner.rolcreatedb "
        "AND NOT owner.rolreplication AND NOT owner.rolbypassrls "
        "AND publisher.rolcanlogin AND NOT publisher.rolinherit AND NOT publisher.rolsuper "
        "AND NOT publisher.rolcreaterole AND NOT publisher.rolcreatedb "
        "AND NOT publisher.rolreplication AND NOT publisher.rolbypassrls "
        "AND pg_has_role(publisher.oid,owner.oid,'SET') "
        "AND NOT pg_has_role(publisher.oid,owner.oid,'USAGE') "
        "AND EXISTS(SELECT FROM pg_namespace WHERE nspname='hp_snapshot_retention' AND nspowner=owner.oid) "
        "FROM pg_roles owner,pg_roles publisher WHERE owner.oid=$1::oid AND publisher.oid=$2::oid",
        *pair,
    )
    if valid is not True:
        raise RuntimeError("cms_native_input_closed_authority_changed")
    return pair[1]


async def _require_closed_native_candidate(connection, candidate, closed_owner_publisher):
    from process.entity_address_alias_guard import ORDINARY_PRINCIPAL_SQL

    publisher_oid = await _closed_candidate_publisher(connection, candidate, closed_owner_publisher)
    ordinary = ORDINARY_PRINCIPAL_SQL.replace(":owner_oid", "$2").replace(
        "login.rolcanlogin AND", "login.rolcanlogin AND login.oid<>$3::oid AND"
    )
    unsafe = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_roles principal,pg_class relation WHERE relation.oid=$1::oid AND "
        + ordinary
        + " AND (has_table_privilege(principal.oid,relation.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,REFERENCES') "
        "OR has_any_column_privilege(principal.oid,relation.oid,'INSERT,UPDATE,REFERENCES'))) "
        "OR EXISTS(SELECT FROM pg_rewrite WHERE ev_class=$1::oid)",
        candidate.relation_oid,
        candidate.owner_oid,
        publisher_oid,
    )
    if unsafe is not False:
        raise RuntimeError("cms_native_input_candidate_not_closed")


async def _lock_selected_native_candidates(connection, schema, candidates, *, register, closed_owner_publisher=None):
    names = set()
    for candidate in candidates:
        names.add(_candidate_relation(candidate.stage_schema, candidate.stage_table))
        if candidate.incumbent_oid is not None:
            names.add(_candidate_relation(candidate.target_schema, candidate.target_table))
    mode = "SHARE ROW EXCLUSIVE" if register else "ACCESS SHARE"
    await connection.execute("LOCK TABLE " + ", ".join(sorted(names)) + f" IN {mode} MODE NOWAIT")
    for candidate in candidates:
        actual = await capture_native_input_candidate(
            connection,
            ledger_schema=schema,
            target_schema=candidate.target_schema,
            target_table=candidate.target_table,
            stage_schema=candidate.stage_schema,
            stage_table=candidate.stage_table,
        )
        if actual != candidate:
            raise RuntimeError("cms_native_input_candidate_identity_changed")
        await _require_closed_native_candidate(connection, candidate, closed_owner_publisher)


async def _native_registration_ledger(connection, schema):
    from process.provider_directory_cms_native_inputs import _validate_ledger_guards

    ledger = await connection.fetchrow(
        "SELECT c.oid::bigint AS ledger_oid,c.relowner::bigint AS owner_oid,"
        "c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
        "AND NOT c.relispartition AS safe FROM pg_class c WHERE c.oid=to_regclass($1)",
        _candidate_relation(schema, _TABLE),
    )
    if ledger is None or ledger["safe"] is not True:
        raise RuntimeError("cms_address_native_revision_ledger_unavailable")
    rows = await connection.fetch(
        _LEDGER_GUARDS_NATIVE_SQL,
        _candidate_relation(schema, _TABLE),
        ["cms_native_input_revision_immutable", "cms_native_input_revision_no_truncate"],
        f'"{schema}".cms_native_input_revision_guard()',
    )
    _validate_ledger_guards(rows)
    return dict(ledger)


async def _native_registration_function(connection, schema, ledger):
    from process.provider_directory_cms_native_inputs import _advance_body

    function = await connection.fetchrow(
        "SELECT p.oid::bigint AS function_oid,p.prosrc,p.proowner::bigint AS owner_oid,"
        "p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] "
        "AND l.lanname='plpgsql' AND p.prorettype='trigger'::regtype AND p.prokind='f' "
        "AND p.pronargs=0 AND p.pronargdefaults=0 AND p.provariadic=0 "
        "AND p.proargtypes=''::oidvector AND p.proallargtypes IS NULL AND p.proargmodes IS NULL "
        "AND p.proargnames IS NULL AND NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof "
        "AND p.prosupport=0 AND p.provolatile='v' AND p.proparallel='u' AS safe "
        "FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=to_regprocedure($1)",
        f'"{schema}".cms_native_input_advance()',
    )
    if function is None or (
        function["safe"] is not True
        or function["owner_oid"] != ledger["owner_oid"]
        or function["prosrc"] != _advance_body(schema)
    ):
        raise RuntimeError("cms_address_native_revision_guard_unavailable")
    return dict(function)


async def _native_candidate_registration(connection, schema, candidate, function):
    registered = await connection.fetchrow(
        f'SELECT relation_oid,schema_name::text,table_name::text,revision FROM "{schema}"."{_TABLE}" '
        "WHERE relation_oid=$1",
        candidate.relation_oid,
    )
    guards = await _native_relation_guards(connection, candidate.relation_oid)
    if registered is None and not guards:
        return None
    return _require_candidate_registration(candidate, registered, guards, function["function_oid"])


async def _native_relation_guards(connection, relation_oid):
    return await connection.fetch(
        "SELECT t.oid::bigint AS trigger_oid,t.tgname,t.tgfoid::bigint AS function_oid,"
        "t.tgtype=60 AND t.tgenabled='A' AND t.tgnargs=0 AND t.tgargs=''::bytea "
        "AND t.tgqual IS NULL AND t.tgattr=''::int2vector AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL "
        "AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgconstraint=0 "
        "AND t.tgconstrrelid=0 AND t.tgconstrindid=0 AND t.tgparentid=0 AS safe "
        "FROM pg_trigger t WHERE t.tgrelid=$1::oid AND NOT t.tgisinternal AND t.tgname='cms_native_input_mutation' ORDER BY t.oid",
        relation_oid,
    )


def _require_candidate_registration(candidate, registered, guards, function_oid):
    if registered is None or (
        registered["relation_oid"] != candidate.relation_oid
        or registered["schema_name"] != candidate.target_schema
        or registered["table_name"] != candidate.target_table
        or type(registered["revision"]) is not int
        or registered["revision"] < 0
        or len(guards) != 1
        or guards[0]["tgname"] != _TRIGGER
        or guards[0]["function_oid"] != function_oid
        or guards[0]["safe"] is not True
    ):
        raise RuntimeError("cms_native_input_registration_changed")
    return {**dict(registered), "trigger_oid": guards[0]["trigger_oid"], "function_oid": function_oid}


async def _native_candidate_inventory(connection, candidate, schema):
    inventory = await connection.fetchval(
        "SELECT jsonb_build_object('relation',to_jsonb(c),"
        "'columns',(SELECT jsonb_agg(to_jsonb(a) ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid),"
        "'indexes',(SELECT jsonb_agg(to_jsonb(i) ORDER BY i.indexrelid) FROM pg_index i WHERE i.indrelid=c.oid),"
        "'constraints',(SELECT jsonb_agg(to_jsonb(k) ORDER BY k.oid) FROM pg_constraint k WHERE k.conrelid=c.oid),"
        "'triggers',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t WHERE t.tgrelid=c.oid),"
        "'rules',(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.oid) FROM pg_rewrite r WHERE r.ev_class=c.oid),"
        "'functions',(SELECT jsonb_agg(jsonb_build_object('native',to_jsonb(p),"
        "'definition',pg_get_functiondef(p.oid)) ORDER BY p.oid) FROM pg_proc p "
        "WHERE p.oid IN (to_regprocedure($2),to_regprocedure($3)) "
        "OR p.oid IN (SELECT tgfoid FROM pg_trigger WHERE tgrelid=c.oid))) "
        "FROM pg_class c WHERE c.oid=$1::oid",
        candidate.relation_oid,
        f'"{schema}".cms_native_input_advance()',
        f'"{schema}".cms_native_input_revision_guard()',
    )
    if type(inventory) is str:
        inventory = json.loads(inventory)
    if type(inventory) is not dict:
        raise RuntimeError("cms_native_input_candidate_inventory_changed")
    return inventory


async def _native_enrollment_evidence(
    connection, schema, candidates, *, register, registration_rows=None, closed_owner_publisher=None
):
    await _lock_selected_native_candidates(
        connection,
        schema,
        candidates,
        register=register,
        closed_owner_publisher=closed_owner_publisher,
    )
    if registration_rows is None:
        await connection.fetch(f'SELECT relation_oid FROM "{schema}"."{_TABLE}" WHERE FALSE')
    ledger = await _native_registration_ledger(connection, schema)
    function = await _native_registration_function(connection, schema, ledger)
    registrations, inventory = [], []
    for index, candidate in enumerate(candidates):
        if registration_rows is None:
            enrolled = await _native_candidate_registration(connection, schema, candidate, function)
        else:
            supplied = registration_rows[index]
            guards = await _native_relation_guards(connection, candidate.relation_oid)
            if (
                len(guards) != 1
                or supplied["trigger_oid"] != guards[0]["trigger_oid"]
                or supplied["function_oid"] != function["function_oid"]
            ):
                raise RuntimeError("cms_native_input_registration_changed")
            enrolled = _require_candidate_registration(candidate, supplied, guards, function["function_oid"])
        if enrolled is None:
            raise RuntimeError("cms_address_native_input_not_registered")
        registrations.append(enrolled)
        inventory.append(await _native_candidate_inventory(connection, candidate, schema))
    xid = "pg_current_xact_id()" if register else "pg_current_xact_id_if_assigned()"
    native_context_by_field = dict(
        await connection.fetchrow(
            "SELECT current_user::regrole::oid::bigint AS actor_oid,pg_backend_pid() AS backend_pid,"
            + xid
            + "::text AS transaction_id,"
            "(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid"
        )
    )
    candidate_fields = [asdict(candidate) for candidate in candidates]
    return {
        **native_context_by_field,
        "version": 1,
        "count": len(candidates),
        "ledger_schema": schema,
        "ledger": ledger,
        "ledger_oid": ledger["ledger_oid"],
        "function_oid": function["function_oid"],
        "candidates": candidate_fields,
        "candidates_sha256": _native_enrollment_digest(candidate_fields),
        "registrations": registrations,
        "registrations_sha256": _native_enrollment_digest(registrations),
        "native_inventory": inventory,
        "registration_source": "native-ledger" if registration_rows is None else "provider-linkage",
    }


def _native_enrollment_digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def verify_native_input_candidates(connection, *, ledger_schema, candidates, closed_owner_publisher=None):
    """Verify native enrollment in the owner's original transaction, with no repair writes."""
    schema = _selected_candidates(ledger_schema, candidates)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    return await _native_enrollment_evidence(
        connection, schema, candidates, register=False, closed_owner_publisher=closed_owner_publisher
    )


def _provider_registration_rows(candidates, registrations):
    keys = {"relation_oid", "schema_name", "table_name", "revision", "trigger_oid", "function_oid"}
    if type(registrations) not in (tuple, list) or len(registrations) != len(candidates):
        raise RuntimeError("cms_native_input_registration_changed")
    for candidate, row in zip(candidates, registrations, strict=True):
        if (
            type(row) is not dict
            or set(row) != keys
            or (
                any(
                    type(row[key]) is not int or not 0 < row[key] < 2**32
                    for key in ("relation_oid", "trigger_oid", "function_oid")
                )
                or type(row["revision"]) is not int
                or not 0 <= row["revision"] < 2**63
                or row["relation_oid"] != candidate.relation_oid
                or (row["schema_name"], row["table_name"]) != (candidate.target_schema, candidate.target_table)
            )
        ):
            raise RuntimeError("cms_native_input_registration_changed")


async def verify_native_input_candidate_catalog(
    connection, *, ledger_schema, candidates, registrations, closed_owner_publisher=None
):
    """Link authenticated provider rows to native catalogs, without ledger-data privileges.

    The caller must separately authenticate and reobserve the original provider proof.
    This checks no ledger revision or payload bytes and never confers that authority.
    """
    schema = _selected_candidates(ledger_schema, candidates)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    _provider_registration_rows(candidates, registrations)
    return await _native_enrollment_evidence(
        connection,
        schema,
        candidates,
        register=False,
        registration_rows=registrations,
        closed_owner_publisher=closed_owner_publisher,
    )


def _expected_native_guard(registration):
    return dict(
        oid=str(registration["trigger_oid"]),
        tgrelid=str(registration["relation_oid"]),
        tgparentid="0",
        tgname=_TRIGGER,
        tgfoid=str(registration["function_oid"]),
        tgtype=60,
        tgenabled="A",
        tgisinternal=False,
        tgconstrrelid="0",
        tgconstrindid="0",
        tgconstraint="0",
        tgdeferrable=False,
        tginitdeferred=False,
        tgnargs=0,
        tgattr=[],
        tgargs="\\x",
        tgqual=None,
        tgoldtable=None,
        tgnewtable=None,
    )


def require_native_input_registration_delta(before_inventory, after_inventory, registrations):
    """Allow only an exact existing statement guard and the native relhastriggers flag."""
    if (
        type(registrations) not in (list, tuple)
        or type(before_inventory) is not list
        or type(after_inventory) is not list
        or (len(before_inventory) != len(after_inventory) or len(after_inventory) != len(registrations))
    ):
        raise RuntimeError("cms_native_input_registration_catalog_changed")
    keys = {"relation", "columns", "indexes", "constraints", "triggers", "rules", "functions"}
    for before, after, registration in zip(before_inventory, after_inventory, registrations, strict=True):
        if type(before) is not dict or type(after) is not dict or set(before) != keys or set(after) != keys:
            raise RuntimeError("cms_native_input_registration_catalog_changed")
        if any(
            type(entry["relation"]) is not dict
            or entry["relation"].get("oid") != str(registration["relation_oid"])
            or type(entry["relation"].get("relhastriggers")) is not bool
            for entry in (before, after)
        ):
            raise RuntimeError("cms_native_input_registration_catalog_changed")
        expected = deepcopy(before)
        guard = _expected_native_guard(registration)
        previous = before["triggers"] or []
        current = after["triggers"]
        canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if (
            type(previous) is not list
            or type(current) is not list
            or not any(
                type(entry) is dict and type(entry.get("tgattr")) is list and canonical(entry) == canonical(guard)
                for entry in current
            )
        ):
            raise RuntimeError("cms_native_input_registration_catalog_changed")
        if not any(entry.get("oid") == guard["oid"] for entry in previous):
            expected["triggers"] = sorted([*previous, guard], key=lambda entry: int(entry["oid"]))
        expected["relation"]["relhastriggers"] = True
        if canonical(expected) != canonical(after):
            raise RuntimeError("cms_native_input_registration_catalog_changed")


async def register_native_input_candidates(
    connection, *, ledger_schema, candidates, expected_actor_oid, closed_owner_publisher=None
):
    """Enroll exact closed OIDs through existing native rights; never grant registrar authority."""
    schema = _selected_candidates(ledger_schema, candidates)
    if not connection.is_in_transaction() or type(expected_actor_oid) is not int or expected_actor_oid <= 0:
        raise RuntimeError("cms_native_input_original_transaction_required")
    task = asyncio.current_task()
    actor_sql = (
        "SELECT session_user::regrole::oid::bigint,current_user::regrole::oid::bigint,"
        "pg_backend_pid(),pg_current_xact_id()::text"
    )
    actor_values = tuple(await connection.fetchrow(actor_sql))
    if actor_values[1] != expected_actor_oid:
        raise RuntimeError("cms_native_input_operator_actor_changed")
    async with connection.transaction():
        await _lock_selected_native_candidates(
            connection, schema, candidates, register=True, closed_owner_publisher=closed_owner_publisher
        )
        await connection.fetch(f'SELECT relation_oid FROM "{schema}"."{_TABLE}" WHERE FALSE')
        ledger = await _native_registration_ledger(connection, schema)
        function = await _native_registration_function(connection, schema, ledger)
        before_catalog_entries = [
            await _native_candidate_inventory(connection, candidate, schema) for candidate in candidates
        ]
        for candidate in candidates:
            if await _native_candidate_registration(connection, schema, candidate, function) is not None:
                continue
            await connection.execute(
                f'INSERT INTO "{schema}"."{_TABLE}" (relation_oid,schema_name,table_name) VALUES ($1,$2,$3)',
                candidate.relation_oid,
                candidate.target_schema,
                candidate.target_table,
            )
            physical = _candidate_relation(candidate.stage_schema, candidate.stage_table)
            await connection.execute(
                f"CREATE TRIGGER {_TRIGGER} AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {physical} "
                f'FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".cms_native_input_advance()'
            )
            await connection.execute(f"ALTER TABLE {physical} ENABLE ALWAYS TRIGGER {_TRIGGER}")
        evidence = await _native_enrollment_evidence(
            connection, schema, candidates, register=True, closed_owner_publisher=closed_owner_publisher
        )
        require_native_input_registration_delta(
            before_catalog_entries, evidence["native_inventory"], evidence["registrations"]
        )
        evidence["before_native_inventory"] = before_catalog_entries
        if (
            asyncio.current_task() is not task
            or tuple(await connection.fetchrow(actor_sql)) != actor_values
            or (evidence["actor_oid"], evidence["backend_pid"], evidence["transaction_id"]) != actor_values[1:]
        ):
            raise RuntimeError("cms_native_input_operator_actor_changed")
    return evidence


_LEDGER_GUARDS_NATIVE_SQL = """
        SELECT trigger.tgname, trigger.tgtype, trigger.tgenabled::text AS enabled,
               function.prosrc, function.proconfig, function.prosecdef,
               language.lanname, trigger.tgnargs, trigger.tgqual IS NULL AS unconditional
          FROM pg_trigger trigger JOIN pg_proc function ON function.oid=trigger.tgfoid
          JOIN pg_language language ON language.oid=function.prolang
         WHERE trigger.tgrelid=to_regclass($1) AND trigger.tgname=ANY(CAST($2 AS text[]))
           AND trigger.tgfoid=to_regprocedure($3)
    """


def _local_publication_schema(ledger_schema, relations):
    if type(relations) is not tuple or not relations:
        raise RuntimeError("cms_native_input_local_publication_scope_changed")
    names = set()
    for pair in relations:
        if type(pair) is not tuple or len(pair) != 2:
            raise RuntimeError("cms_native_input_local_publication_scope_changed")
        target_table, stage_table = pair
        if (
            any(type(name) is not str or npi._schema_name(name) != name for name in pair)
            or target_table == stage_table
            or target_table in names
        ):
            raise RuntimeError("cms_native_input_local_publication_scope_changed")
        names.add(target_table)
    return npi._schema_name(ledger_schema)


async def prepare_local_native_input_publication(session, *, ledger_schema, relations):
    """Enroll selected fixed inputs and replay reads in the local owner's final transaction."""
    from sqlalchemy import text

    from process.native_publication_read_access import preserve_publication_reads

    schema = _local_publication_schema(ledger_schema, relations)
    if not session.in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    # Materialize SQLAlchemy's native transaction before using its original driver.
    await session.execute(text("SELECT 1"))
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    if not driver.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    task = asyncio.current_task()
    actor_sql = (
        "SELECT session_user::regrole::oid::bigint AS session_oid,current_user::regrole::oid::bigint AS actor_oid,"
        "pg_backend_pid() AS backend_pid,pg_current_xact_id()::text AS transaction_id"
    )
    identity = await driver.fetchrow(actor_sql)
    if identity["session_oid"] != identity["actor_oid"]:
        raise RuntimeError("cms_native_input_local_owner_required")
    async with driver.transaction():
        selected_candidates = tuple(
            [
                await capture_native_input_candidate(
                    driver,
                    ledger_schema=schema,
                    target_schema=schema,
                    target_table=target_table,
                    stage_schema=schema,
                    stage_table=stage_table,
                )
                for target_table, stage_table in relations
                if (schema, target_table) in _registration_roots(schema)
            ]
        )
        evidence = None
        if selected_candidates:
            if any(candidate.owner_oid != identity["actor_oid"] for candidate in selected_candidates):
                raise RuntimeError("cms_native_input_local_owner_required")
            allowed = await driver.fetchval(
                "SELECT has_table_privilege(current_user,$1::regclass,'INSERT') OR "
                "(has_column_privilege(current_user,$1::regclass,'relation_oid','INSERT') AND "
                "has_column_privilege(current_user,$1::regclass,'schema_name','INSERT') AND "
                "has_column_privilege(current_user,$1::regclass,'table_name','INSERT'))",
                _candidate_relation(schema, _TABLE),
            )
            if allowed is not True:
                raise RuntimeError("cms_native_input_local_insert_required")
            evidence = await register_native_input_candidates(
                driver, ledger_schema=schema, candidates=selected_candidates, expected_actor_oid=identity["actor_oid"]
            )
        for target_table, stage_table in relations:
            await preserve_publication_reads(driver, schema=schema, live_table=target_table, stage_table=stage_table)
        if asyncio.current_task() is not task or await driver.fetchrow(actor_sql) != identity:
            raise RuntimeError("cms_native_input_operator_actor_changed")
    return evidence


async def resolve_installed_native_input_guard(connection, *, ledger_schema, relation_oid):
    """Identify only the installed protected CMS guard, never ledger freshness or authority."""
    schema = npi._schema_name(ledger_schema)
    if type(relation_oid) is not int or not 0 < relation_oid < 2**32:
        raise RuntimeError("cms_native_input_candidate_invalid")
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    ledger = await _native_registration_ledger(connection, schema)
    function = await _native_registration_function(connection, schema, ledger)
    guards = await _native_relation_guards(connection, relation_oid)
    if not guards:
        return None
    if (
        len(guards) != 1
        or guards[0]["tgname"] != _TRIGGER
        or guards[0]["safe"] is not True
        or guards[0]["function_oid"] != function["function_oid"]
    ):
        raise RuntimeError("cms_native_input_registration_changed")
    return {
        "trigger_oid": guards[0]["trigger_oid"],
        "function_oid": function["function_oid"],
        "ledger_oid": ledger["ledger_oid"],
        "owner_oid": ledger["owner_oid"],
    }


_ADOPTION_FIELDS = frozenset(
    {
        "relation_oid",
        "schema_name",
        "table_name",
        "revision",
        "mutation_xid",
        "ledger_oid",
        "ledger_owner_oid",
        "trigger_oid",
        "function_oid",
        "function_sha256",
    }
)


def validate_native_input_adoption(evidence, *, schema, relation_oid):
    """Check the closed retained preimage, without claiming a current installed guard."""
    from process.provider_directory_cms_native_inputs import _advance_body

    if type(evidence) is not dict or set(evidence) != _ADOPTION_FIELDS:
        raise RuntimeError("cms_native_input_adoption_changed")
    identities = ("relation_oid", "ledger_oid", "ledger_owner_oid", "trigger_oid", "function_oid")
    mutation_xid = evidence["mutation_xid"]
    if (
        any(type(evidence[key]) is not int or not 0 < evidence[key] < 2**32 for key in identities)
        or evidence["relation_oid"] != relation_oid
        or (evidence["schema_name"], evidence["table_name"]) != (schema, "nucc_taxonomy")
        or type(evidence["revision"]) is not int
        or not 0 <= evidence["revision"] < 2**63
        or (
            mutation_xid is not None
            and (
                type(mutation_xid) is not str
                or not 1 <= len(mutation_xid) <= 20
                or not mutation_xid.isascii()
                or not mutation_xid.isdecimal()
                or str(int(mutation_xid)) != mutation_xid
                or not 0 < int(mutation_xid) < 2**64
            )
        )
        or evidence["function_sha256"] != hashlib.sha256(_advance_body(schema).encode()).hexdigest()
    ):
        raise RuntimeError("cms_native_input_adoption_changed")
    return evidence


async def _native_adoption_row(connection, schema, relation_oid):
    return await connection.fetchrow(
        f"SELECT relation_oid,schema_name::text,table_name::text,revision,mutation_xid::text AS mutation_xid "
        f'FROM "{schema}"."{_TABLE}" WHERE relation_oid=$1 FOR SHARE NOWAIT',
        relation_oid,
    )


async def _native_adoption_context(connection, schema):
    ledger = await _native_registration_ledger(connection, schema)
    return ledger, await _native_registration_function(connection, schema, ledger)


def _native_adoption_evidence(schema, row, ledger, function, trigger_oid):
    return validate_native_input_adoption(
        {
            **dict(row),
            "ledger_oid": ledger["ledger_oid"],
            "ledger_owner_oid": ledger["owner_oid"],
            "trigger_oid": trigger_oid,
            "function_oid": function["function_oid"],
            "function_sha256": hashlib.sha256(function["prosrc"].encode()).hexdigest(),
        },
        schema=schema,
        relation_oid=row["relation_oid"],
    )


async def capture_native_input_adoption(connection, *, ledger_schema, candidate):
    """Authenticate the locked NUCC predecessor registration before writer custody closes."""
    schema = _selected_candidates(ledger_schema, (candidate,))
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    if (
        candidate.target_table != "nucc_taxonomy"
        or candidate.stage_schema != schema
        or candidate.stage_table != candidate.target_table
        or candidate.incumbent_oid != candidate.relation_oid
    ):
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    guards = await _native_relation_guards(connection, candidate.relation_oid)
    ledger_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", _candidate_relation(schema, _TABLE))
    if ledger_oid is None:
        if guards:
            raise RuntimeError("cms_native_input_registration_changed")
        return None
    await connection.execute(f'LOCK TABLE "{schema}"."{_TABLE}" IN SHARE MODE NOWAIT')
    row = await _native_adoption_row(connection, schema, candidate.relation_oid)
    if row is None and not guards:
        return None
    if row is None or len(guards) != 1:
        raise RuntimeError("cms_native_input_registration_changed")
    ledger, function = await _native_adoption_context(connection, schema)
    _require_candidate_registration(candidate, row, guards, function["function_oid"])
    return _native_adoption_evidence(schema, row, ledger, function, guards[0]["trigger_oid"])


async def verify_retired_native_input_adoption(connection, *, evidence):
    """Require the same immutable registration after only its heap guard was retired."""
    schema = npi._schema_name(evidence["schema_name"])
    validate_native_input_adoption(evidence, schema=schema, relation_oid=evidence["relation_oid"])
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    row = await _native_adoption_row(connection, schema, evidence["relation_oid"])
    if row is None or await _native_relation_guards(connection, evidence["relation_oid"]):
        raise RuntimeError("cms_native_input_adoption_changed")
    ledger, function = await _native_adoption_context(connection, schema)
    if _native_adoption_evidence(schema, row, ledger, function, evidence["trigger_oid"]) != evidence:
        raise RuntimeError("cms_native_input_adoption_changed")


class _NativeNuccSession:
    """Expose fixed native NUCC read/lock statements on the caller's exact driver."""

    def __init__(self, connection):
        self.connection = connection
        self._task = asyncio.current_task()

    def in_transaction(self):
        """Report only the original task's still-active native transaction."""
        return asyncio.current_task() is self._task and self.connection.is_in_transaction()

    def _compiled(self, statement, parameters):
        from sqlalchemy.dialects.postgresql.asyncpg import dialect

        if not self.in_transaction():
            raise RuntimeError("cms_native_input_original_transaction_required")
        compiled = statement.bindparams(**parameters).compile(dialect=dialect())
        return str(compiled), tuple(compiled.params[name] for name in compiled.positiontup or ())

    async def scalar(self, statement, parameters=None):
        """Return the first field from the same driver's fixed native query."""
        query, arguments = self._compiled(statement, parameters or {})
        return await self.connection.fetchval(query, *arguments)

    async def execute(self, statement, parameters=None):
        """Execute fixed lock/search-path statements without opening a transaction."""
        query, arguments = self._compiled(statement, parameters or {})
        return await self.connection.execute(query, *arguments)

    async def first(self, statement, **parameters):
        """Expose the native record mapping used by the existing generation validator."""
        query, arguments = self._compiled(statement, parameters)
        row = await self.connection.fetchrow(query, *arguments)
        return None if row is None else dict(row)
