# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named native input registration with existing statement revision guards."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import asdict, dataclass

from process import npi_result_generation as npi

_TABLE = "cms_native_input_revision"
_TRIGGER = "cms_native_input_mutation"


@dataclass(frozen=True)
class NativeInputCandidate:
    """Declare one input and its completed candidate by qualified name."""

    target_schema: str
    target_table: str
    stage_schema: str
    stage_table: str

    def __post_init__(self):
        for name in (self.target_schema, self.target_table, self.stage_schema, self.stage_table):
            if type(name) is not str or npi._schema_name(name) != name:
                raise RuntimeError("cms_native_input_candidate_invalid")


def _registration_roots(schema):
    from process.provider_directory_cms_native_inputs import _relations

    excluded_roots = {(schema, name) for name in (*npi.RELATION_NAMES, "doctor_clinician_address", "geo_zip_lookup")}
    return set(_relations(schema)) - excluded_roots


def native_input_registration_roots(schema):
    """Return the finite ordinary inputs supported by the existing registration protocol."""
    return tuple(sorted(_registration_roots(npi._schema_name(schema))))


def _selected_candidates(schema, candidates):
    schema = npi._schema_name(schema)
    if type(candidates) is not tuple or not 0 < len(candidates) <= len(_registration_roots(schema)):
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    targets, stages = set(), set()
    for candidate in candidates:
        if type(candidate) is not NativeInputCandidate:
            raise RuntimeError("cms_native_input_candidate_invalid")
        target = candidate.target_schema, candidate.target_table
        stage = candidate.stage_schema, candidate.stage_table
        if target not in _registration_roots(schema) or target in targets or stage in stages:
            raise RuntimeError("cms_native_input_candidate_scope_changed")
        targets.add(target)
        stages.add(stage)
    return schema


def _candidate_relation(schema, table):
    return f'"{npi._schema_name(schema)}"."{npi._schema_name(table)}"'


async def _native_candidate_heap(connection, *, target_schema, target_table, stage_schema, stage_table):
    row = await connection.fetchrow(
        "SELECT c.relkind::text,c.relpersistence::text,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,"
        "EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited "
        "FROM pg_class c WHERE c.oid=to_regclass($1)",
        _candidate_relation(stage_schema, stage_table),
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
    return NativeInputCandidate(target_schema, target_table, stage_schema, stage_table)


async def capture_native_input_candidate(
    connection, *, ledger_schema, target_schema, target_table, stage_schema, stage_table
):
    """Capture declared target and candidate names after validating native heap shape."""
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
    rows = await connection.fetch(
        "SELECT requested.namespace,requested.name,c.oid IS NOT NULL AS present,c.relkind::text AS kind "
        "FROM unnest($1::text[],$2::text[]) requested(namespace,name) LEFT JOIN pg_class c "
        "ON c.oid=to_regclass(format('%I.%I',namespace,name)) ORDER BY namespace,name",
        [schema for schema, _ in roots],
        [name for _, name in roots],
    )
    if [(row["namespace"], row["name"]) for row in rows] != list(roots):
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    return rows


async def _current_protected_fhir(connection, ledger_schema, root_records, source_policy):
    from process.provider_directory_generic_source_provision import FHIR_INPUT_ROOTS

    rows = [row for row in root_records if row["namespace"] == ledger_schema and row["name"] in FHIR_INPUT_ROOTS]
    if not any(row["kind"] == "p" for row in rows):
        return None
    from process.provider_directory_cms_native_inputs import capture_protected_fhir_input
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    if type(source_policy) is not GenericSourcePolicy or source_policy.schema != ledger_schema:
        raise RuntimeError("cms_native_input_source_policy_required")
    proof = await capture_protected_fhir_input(connection, source_policy=source_policy)
    if proof is None or {(row["namespace"], row["name"]) for row in rows if row["kind"] == "p"} != {
        tuple(pair) for pair in proof["parents"]
    }:
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    return proof


def _protected_tiger_root(proof, root_record):
    if proof["version"] == 1 and root_record["kind"] != "r":
        raise RuntimeError("cms_native_input_candidate_heap_changed")
    return proof


async def capture_current_native_input_candidates(
    connection, *, ledger_schema, protected_tiger=None, source_policy=None
):
    """Classify current declared inputs as ordinary, absent or protected native families."""
    from process.provider_directory_generic_source_provision import FHIR_INPUT_ROOTS

    root_records = await _current_input_root_records(connection, ledger_schema)
    fhir = await _current_protected_fhir(connection, ledger_schema, root_records, source_policy)
    tiger_names = ()
    if protected_tiger is not None:
        from process.provider_directory_cms_tiger_inputs import ROOTS, verify_raw_protected_tiger_input

        protected_tiger = await verify_raw_protected_tiger_input(connection, protected_tiger)
        tiger_names = ROOTS
    candidates, absent, protected = [], [], []
    for root_record in root_records:
        namespace, name = root_record["namespace"], root_record["name"]
        if fhir is not None and namespace == ledger_schema and name in FHIR_INPUT_ROOTS:
            protected.append(dict(schema=namespace, table=name, proof=fhir))
            continue
        if namespace == "tiger" and name in tiger_names:
            protected.append(
                dict(schema=namespace, table=name, proof=_protected_tiger_root(protected_tiger, root_record))
            )
            continue
        if not root_record["present"]:
            absent.append((namespace, name))
            continue
        if (namespace, name, root_record["kind"]) == ("mrf", "nucc_taxonomy", "p"):
            from process.provider_directory_cms_native_inputs import capture_protected_nucc_input

            proof = await capture_protected_nucc_input(_NativeNuccSession(connection), schema=ledger_schema)
            protected.append(dict(schema=namespace, table=name, proof=proof))
            continue
        candidates.append(
            await capture_native_input_candidate(
                connection,
                ledger_schema=ledger_schema,
                target_schema=namespace,
                target_table=name,
                stage_schema=namespace,
                stage_table=name,
            )
        )
    capture_by_field = dict(candidates=tuple(candidates), absent=tuple(absent))
    if protected:
        capture_by_field["protected_roots"] = tuple(protected)
    if fhir is not None:
        capture_by_field["protected_fhir"] = fhir
    return capture_by_field


async def _lock_selected_native_candidates(connection, schema, candidates, *, register, closed_owner_publisher=None):
    names = {_candidate_relation(candidate.stage_schema, candidate.stage_table) for candidate in candidates}
    for candidate in candidates:
        target = _candidate_relation(candidate.target_schema, candidate.target_table)
        if await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", target):
            names.add(target)
    await connection.execute("LOCK TABLE " + ", ".join(sorted(names)) + " IN ACCESS SHARE MODE NOWAIT")
    for candidate in candidates:
        await capture_native_input_candidate(connection, ledger_schema=schema, **asdict(candidate))


async def _native_registration_ledger(connection, schema):
    from process.provider_directory_cms_native_inputs import _validate_ledger_guards

    row = await connection.fetchrow(
        "SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
        "AND NOT c.relispartition AS safe FROM pg_class c WHERE c.oid=to_regclass($1)",
        _candidate_relation(schema, _TABLE),
    )
    if row is None or row["safe"] is not True:
        raise RuntimeError("cms_address_native_revision_ledger_unavailable")
    _validate_ledger_guards(
        await connection.fetch(
            _LEDGER_GUARDS_NATIVE_SQL,
            _candidate_relation(schema, _TABLE),
            ["cms_native_input_revision_immutable", "cms_native_input_revision_no_truncate"],
            f'"{schema}".cms_native_input_revision_guard()',
        )
    )
    return dict(schema_name=schema, table_name=_TABLE)


async def _native_registration_function(connection, schema, ledger):
    from process.provider_directory_cms_native_inputs import _advance_body

    row = await connection.fetchrow(
        "SELECT p.prosrc,p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] "
        "AND l.lanname='plpgsql' AND p.prorettype='trigger'::regtype AND p.prokind='f' "
        "AND p.pronargs=0 AND p.pronargdefaults=0 AND p.provariadic=0 "
        "AND p.proargtypes=''::oidvector AND p.proallargtypes IS NULL AND p.proargmodes IS NULL "
        "AND p.proargnames IS NULL AND NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof "
        "AND p.prosupport=0 AND p.provolatile='v' AND p.proparallel='u' AS safe "
        "FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=to_regprocedure($1)",
        f'"{schema}".cms_native_input_advance()',
    )
    if row is None or row["safe"] is not True or row["prosrc"] != _advance_body(schema):
        raise RuntimeError("cms_address_native_revision_guard_unavailable")
    return dict(schema_name=schema, function_name="cms_native_input_advance", prosrc=row["prosrc"])


async def _native_relation_guards(connection, schema_name, table_name, ledger_schema):
    return await connection.fetch(
        "SELECT t.tgname,t.tgtype,t.tgenabled::text AS enabled,n.nspname AS function_schema,p.proname AS function_name,"
        "t.tgtype=60 AND t.tgenabled='A' AND t.tgnargs=0 AND t.tgargs=''::bytea "
        "AND t.tgqual IS NULL AND t.tgattr=''::int2vector AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL "
        "AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgconstraint=0 "
        "AND t.tgconstrrelid=0 AND t.tgconstrindid=0 AND t.tgparentid=0 "
        "AND t.tgfoid=to_regprocedure($2) AS safe FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
        "JOIN pg_namespace n ON n.oid=p.pronamespace WHERE t.tgrelid=to_regclass($1) "
        "AND NOT t.tgisinternal AND t.tgname='cms_native_input_mutation' ORDER BY t.tgname",
        _candidate_relation(schema_name, table_name),
        f'"{ledger_schema}".cms_native_input_advance()',
    )


def _require_candidate_registration(candidate, registered, guards, function):
    if (
        registered is None
        or (registered["schema_name"], registered["table_name"]) != (candidate.target_schema, candidate.target_table)
        or type(registered["revision"]) is not int
        or not 0 <= registered["revision"] < 2**63
        or len(guards) != 1
        or guards[0]["tgname"] != _TRIGGER
        or guards[0]["safe"] is not True
        or (guards[0]["function_schema"], guards[0]["function_name"])
        != (function["schema_name"], function["function_name"])
    ):
        raise RuntimeError("cms_native_input_registration_changed")
    return dict(
        schema_name=registered["schema_name"],
        table_name=registered["table_name"],
        revision=registered["revision"],
        ledger_schema=function["schema_name"],
    )


async def _native_candidate_registration(connection, schema, candidate, function):
    row = await connection.fetchrow(
        f'SELECT schema_name::text,table_name::text,revision FROM "{schema}"."{_TABLE}" WHERE schema_name=$1 AND table_name=$2',
        candidate.target_schema,
        candidate.target_table,
    )
    guards = await _native_relation_guards(connection, candidate.stage_schema, candidate.stage_table, schema)
    if not guards and (
        row is None
        or (candidate.stage_schema, candidate.stage_table) != (candidate.target_schema, candidate.target_table)
    ):
        return None
    return _require_candidate_registration(candidate, row, guards, function)


async def _native_candidate_inventory(connection, candidate, schema):
    inventory = await connection.fetchval(
        """SELECT jsonb_build_object(
      'relation',jsonb_build_object('schema_name',n.nspname,'table_name',c.relname,'kind',c.relkind::text,'persistence',c.relpersistence::text,'rls',c.relrowsecurity,'force_rls',c.relforcerowsecurity,'partition',c.relispartition),
      'columns',(SELECT jsonb_agg(jsonb_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),'required',a.attnotnull,'identity',a.attidentity,'generated',a.attgenerated,'default',pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),
      'indexes',(SELECT jsonb_agg(jsonb_build_object('definition',pg_get_indexdef(i.indexrelid),'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive) ORDER BY ic.relname) FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid WHERE i.indrelid=c.oid),
      'constraints',(SELECT jsonb_agg(jsonb_build_object('name',k.conname,'type',k.contype,'definition',pg_get_constraintdef(k.oid),'validated',k.convalidated) ORDER BY k.conname) FROM pg_constraint k WHERE k.conrelid=c.oid),
      'triggers',(SELECT jsonb_agg(jsonb_build_object('name',t.tgname,'type',t.tgtype,'enabled',t.tgenabled::text,'definition',pg_get_triggerdef(t.oid)) ORDER BY t.tgname) FROM pg_trigger t WHERE t.tgrelid=c.oid),
      'rules',(SELECT jsonb_agg(pg_get_ruledef(r.oid) ORDER BY r.rulename) FROM pg_rewrite r WHERE r.ev_class=c.oid),
      'functions',(SELECT jsonb_agg(jsonb_build_object('schema_name',fn.nspname,'function_name',p.proname,'definition',pg_get_functiondef(p.oid)) ORDER BY fn.nspname,p.proname) FROM pg_proc p JOIN pg_namespace fn ON fn.oid=p.pronamespace WHERE p.oid IN(SELECT tgfoid FROM pg_trigger WHERE tgrelid=c.oid))) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)""",
        _candidate_relation(candidate.stage_schema, candidate.stage_table),
    )
    if type(inventory) is str:
        inventory = json.loads(inventory)
    if type(inventory) is not dict:
        raise RuntimeError("cms_native_input_candidate_inventory_changed")
    return inventory


def _native_enrollment_digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


async def _native_enrollment_evidence(
    connection, schema, candidates, *, register, registration_rows=None, closed_owner_publisher=None
):
    await _lock_selected_native_candidates(connection, schema, candidates, register=register)
    if registration_rows is None:
        await connection.fetch(f'SELECT schema_name,table_name FROM "{schema}"."{_TABLE}" WHERE FALSE')
    ledger = await _native_registration_ledger(connection, schema)
    function = await _native_registration_function(connection, schema, ledger)
    registrations, inventory = [], []
    for index, candidate in enumerate(candidates):
        if registration_rows is None:
            enrolled = await _native_candidate_registration(connection, schema, candidate, function)
        else:
            guards = await _native_relation_guards(connection, candidate.stage_schema, candidate.stage_table, schema)
            enrolled = _require_candidate_registration(candidate, registration_rows[index], guards, function)
        if enrolled is None:
            raise RuntimeError("cms_address_native_input_not_registered")
        registrations.append(enrolled)
        inventory.append(await _native_candidate_inventory(connection, candidate, schema))
    fields = [asdict(candidate) for candidate in candidates]
    return dict(
        version=2,
        count=len(candidates),
        ledger_schema=schema,
        candidates=fields,
        candidates_sha256=_native_enrollment_digest(fields),
        registrations=registrations,
        registrations_sha256=_native_enrollment_digest(registrations),
        native_inventory=inventory,
        registration_source="native-ledger" if registration_rows is None else "provider-linkage",
    )


async def verify_native_input_candidates(connection, *, ledger_schema, candidates, closed_owner_publisher=None):
    """Recheck existing named registrations and complete native schema inventories."""
    schema = _selected_candidates(ledger_schema, candidates)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    return await _native_enrollment_evidence(connection, schema, candidates, register=False)


def _provider_registration_rows(candidates, registrations):
    keys = {"schema_name", "table_name", "revision", "ledger_schema"}
    if type(registrations) not in (tuple, list) or len(registrations) != len(candidates):
        raise RuntimeError("cms_native_input_registration_changed")
    for candidate, row in zip(candidates, registrations, strict=True):
        if (
            type(row) is not dict
            or set(row) != keys
            or (row["schema_name"], row["table_name"]) != (candidate.target_schema, candidate.target_table)
            or type(row["revision"]) is not int
            or not 0 <= row["revision"] < 2**63
        ):
            raise RuntimeError("cms_native_input_registration_changed")


async def verify_native_input_candidate_catalog(
    connection, *, ledger_schema, candidates, registrations, closed_owner_publisher=None
):
    """Recheck catalog shape against supplied logical registration revisions."""
    schema = _selected_candidates(ledger_schema, candidates)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    _provider_registration_rows(candidates, registrations)
    if any(row["ledger_schema"] != schema for row in registrations):
        raise RuntimeError("cms_native_input_registration_changed")
    return await _native_enrollment_evidence(
        connection, schema, candidates, register=False, registration_rows=registrations
    )


def _native_guard_forms(candidate, ledger_schema):
    """pg_get_triggerdef omits a schema on the search path; the functions inventory still names the schema."""
    from sqlalchemy.dialects.postgresql import dialect

    quote = dialect().identifier_preparer.quote
    tables = (quote(candidate.stage_schema) + "." + quote(candidate.stage_table), quote(candidate.stage_table))
    functions = (quote(ledger_schema) + ".cms_native_input_advance", "cms_native_input_advance")
    return [
        dict(
            name=_TRIGGER,
            type=60,
            enabled="A",
            definition=f"CREATE TRIGGER {_TRIGGER} AFTER INSERT OR DELETE OR UPDATE OR TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {function}()",
        )
        for table in tables
        for function in functions
    ]


def _expected_native_guard(candidate, ledger_schema):
    return _native_guard_forms(candidate, ledger_schema)[0]


def require_native_input_registration_delta(before_inventory, after_inventory, registrations):
    """Permit only the existing statement guard and its exact counter body to be added."""
    if (
        type(registrations) not in (list, tuple)
        or type(before_inventory) is not list
        or type(after_inventory) is not list
        or len(before_inventory) != len(after_inventory)
        or len(after_inventory) != len(registrations)
    ):
        raise RuntimeError("cms_native_input_registration_catalog_changed")
    keys = {"relation", "columns", "indexes", "constraints", "triggers", "rules", "functions"}
    for before, after, registration in zip(before_inventory, after_inventory, registrations, strict=True):
        if type(before) is not dict or type(after) is not dict or set(before) != keys or set(after) != keys:
            raise RuntimeError("cms_native_input_registration_catalog_changed")
        relation = before["relation"]
        candidate = NativeInputCandidate(
            registration["schema_name"], registration["table_name"], relation["schema_name"], relation["table_name"]
        )
        expected = deepcopy(before)
        previous = expected["triggers"] or []
        forms = _native_guard_forms(candidate, registration["ledger_schema"])
        guard = next((entry for entry in after["triggers"] or [] if entry in forms), forms[0])
        if not any(entry == guard for entry in previous):
            previous = [*previous, guard]
        expected["triggers"] = sorted(previous, key=lambda entry: entry["name"])
        expected_functions = expected["functions"] or []
        current_functions = after["functions"] or []
        added_functions = [entry for entry in current_functions if entry not in expected_functions]
        if added_functions:
            if (
                len(added_functions) != 1
                or type(added_functions[0]) is not dict
                or set(added_functions[0]) != {"schema_name", "function_name", "definition"}
                or added_functions[0]["schema_name"] != registration["ledger_schema"]
                or added_functions[0]["function_name"] != "cms_native_input_advance"
            ):
                raise RuntimeError("cms_native_input_registration_catalog_changed")
            from process.provider_directory_cms_native_inputs import _advance_body

            definition = added_functions[0]["definition"]
            if type(definition) is not str or _advance_body(registration["ledger_schema"]) not in definition:
                raise RuntimeError("cms_native_input_registration_catalog_changed")
            expected_functions = [*expected_functions, *added_functions]
        expected["functions"] = (
            sorted(expected_functions, key=lambda entry: (entry["schema_name"], entry["function_name"])) or None
        )
        if expected != after:
            raise RuntimeError("cms_native_input_registration_catalog_changed")


async def register_native_input_candidates(
    connection, *, ledger_schema, candidates, expected_actor_oid=None, closed_owner_publisher=None
):
    """Use native INSERT/DDL privileges; the caller's savepoint rolls back partial registration."""
    schema = _selected_candidates(ledger_schema, candidates)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    async with connection.transaction():
        await _lock_selected_native_candidates(connection, schema, candidates, register=True)
        await connection.fetch(f'SELECT schema_name,table_name FROM "{schema}"."{_TABLE}" WHERE FALSE')
        ledger = await _native_registration_ledger(connection, schema)
        function = await _native_registration_function(connection, schema, ledger)
        before_inventories = [
            await _native_candidate_inventory(connection, candidate, schema) for candidate in candidates
        ]
        for candidate in candidates:
            if await _native_candidate_registration(connection, schema, candidate, function) is not None:
                continue
            await connection.execute(
                f'INSERT INTO "{schema}"."{_TABLE}" (schema_name,table_name) VALUES ($1,$2) ON CONFLICT (schema_name,table_name) DO NOTHING',
                candidate.target_schema,
                candidate.target_table,
            )
            relation = _candidate_relation(candidate.stage_schema, candidate.stage_table)
            await connection.execute(
                f'CREATE TRIGGER {_TRIGGER} AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".cms_native_input_advance()'
            )
            await connection.execute(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_TRIGGER}")
        evidence = await _native_enrollment_evidence(connection, schema, candidates, register=True)
        require_native_input_registration_delta(
            before_inventories, evidence["native_inventory"], evidence["registrations"]
        )
        evidence["before_native_inventory"] = before_inventories
        return evidence


_LEDGER_GUARDS_NATIVE_SQL = """
 SELECT trigger.tgname,trigger.tgtype,trigger.tgenabled::text AS enabled,function.prosrc,function.proconfig,function.prosecdef,
 language.lanname,trigger.tgnargs,trigger.tgqual IS NULL AS unconditional
 FROM pg_trigger trigger JOIN pg_proc function ON function.oid=trigger.tgfoid JOIN pg_language language ON language.oid=function.prolang
 WHERE trigger.tgrelid=to_regclass($1) AND trigger.tgname=ANY(CAST($2 AS text[])) AND trigger.tgfoid=to_regprocedure($3)
"""


def _local_publication_schema(ledger_schema, relations):
    if type(relations) is not tuple or not relations:
        raise RuntimeError("cms_native_input_local_publication_scope_changed")
    names = set()
    for pair in relations:
        if type(pair) is not tuple or len(pair) != 2:
            raise RuntimeError("cms_native_input_local_publication_scope_changed")
        target, stage = pair
        if (
            any(type(name) is not str or npi._schema_name(name) != name for name in pair)
            or target == stage
            or target in names
        ):
            raise RuntimeError("cms_native_input_local_publication_scope_changed")
        names.add(target)
    return npi._schema_name(ledger_schema)


async def prepare_local_native_input_publication(session, *, ledger_schema, relations):
    """Register mutable replacement candidates and retain the current reader grants."""
    from process.native_publication_read_access import preserve_publication_reads
    from process.provider_directory_generic_source_policy import _require_active_source_session
    from process.provider_directory_projection_stage import _copy_driver

    schema = _local_publication_schema(ledger_schema, relations)
    _require_active_source_session(session)
    driver = await _copy_driver(session)
    async with driver.transaction():
        candidates = tuple(
            [
                await capture_native_input_candidate(
                    driver,
                    ledger_schema=schema,
                    target_schema=schema,
                    target_table=target_table,
                    stage_schema=schema,
                    stage_table=stage,
                )
                for target_table, stage in relations
                if (schema, target_table) in _registration_roots(schema)
            ]
        )
        evidence = (
            await register_native_input_candidates(driver, ledger_schema=schema, candidates=candidates)
            if candidates
            else None
        )
        for target_table, stage in relations:
            await preserve_publication_reads(driver, schema=schema, live_table=target_table, stage_table=stage)
    return evidence


async def resolve_installed_native_input_guard(connection, *, ledger_schema, schema_name, table_name):
    """Resolve the existing guard by its declared relation and function names."""
    schema = npi._schema_name(ledger_schema)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    ledger = await _native_registration_ledger(connection, schema)
    function = await _native_registration_function(connection, schema, ledger)
    guards = await _native_relation_guards(connection, schema_name, table_name, schema)
    if not guards:
        return None
    candidate = NativeInputCandidate(schema_name, table_name, schema_name, table_name)
    _require_candidate_registration(
        candidate, dict(schema_name=schema_name, table_name=table_name, revision=0), guards, function
    )
    return dict(
        schema_name=schema_name,
        table_name=table_name,
        ledger_schema=schema,
        trigger_name=_TRIGGER,
        function_name="cms_native_input_advance",
    )


_ADOPTION_FIELDS = frozenset({"schema_name", "table_name", "revision", "mutation_xid", "function_sha256"})
_LEGACY_ADOPTION_FIELDS = _ADOPTION_FIELDS | {
    "relation_oid",
    "ledger_oid",
    "ledger_owner_oid",
    "trigger_oid",
    "function_oid",
}


def validate_native_input_adoption(evidence, *, schema, table_name="nucc_taxonomy"):
    """Decode named revision state while preserving old opaque receipt editions."""
    from process.provider_directory_cms_native_inputs import _advance_body

    if type(evidence) is not dict or set(evidence) not in (_ADOPTION_FIELDS, _LEGACY_ADOPTION_FIELDS):
        raise RuntimeError("cms_native_input_adoption_changed")
    xid = evidence["mutation_xid"]
    if (
        (evidence["schema_name"], evidence["table_name"]) != (schema, table_name)
        or table_name != "nucc_taxonomy"
        or type(evidence["revision"]) is not int
        or not 0 <= evidence["revision"] < 2**63
        or (
            xid is not None
            and (
                type(xid) is not str
                or not 1 <= len(xid) <= 20
                or not xid.isascii()
                or not xid.isdecimal()
                or str(int(xid)) != xid
                or not 0 < int(xid) < 2**64
            )
        )
    ):
        raise RuntimeError("cms_native_input_adoption_changed")
    if (
        set(evidence) == _ADOPTION_FIELDS
        and evidence["function_sha256"] != hashlib.sha256(_advance_body(schema).encode()).hexdigest()
    ):
        raise RuntimeError("cms_native_input_adoption_changed")
    return evidence


async def _native_adoption_row(connection, schema, table_name):
    return await connection.fetchrow(
        f'SELECT schema_name::text,table_name::text,revision,mutation_xid::text AS mutation_xid FROM "{schema}"."{_TABLE}" WHERE schema_name=$1 AND table_name=$2 FOR SHARE NOWAIT',
        schema,
        table_name,
    )


async def _native_adoption_context(connection, schema):
    ledger = await _native_registration_ledger(connection, schema)
    return ledger, await _native_registration_function(connection, schema, ledger)


def _native_adoption_evidence(schema, row, ledger, function):
    return validate_native_input_adoption(
        {**dict(row), "function_sha256": hashlib.sha256(function["prosrc"].encode()).hexdigest()}, schema=schema
    )


async def capture_native_input_adoption(connection, *, ledger_schema, candidate):
    """Capture the registered ordinary NUCC revision and exact existing guard body."""
    schema = _selected_candidates(ledger_schema, (candidate,))
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    if (candidate.target_schema, candidate.target_table, candidate.stage_schema, candidate.stage_table) != (
        schema,
        "nucc_taxonomy",
        schema,
        "nucc_taxonomy",
    ):
        raise RuntimeError("cms_native_input_candidate_scope_changed")
    guards = await _native_relation_guards(connection, schema, "nucc_taxonomy", schema)
    if not await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", _candidate_relation(schema, _TABLE)):
        if guards:
            raise RuntimeError("cms_native_input_registration_changed")
        return None
    row = await _native_adoption_row(connection, schema, "nucc_taxonomy")
    if row is None and not guards:
        return None
    ledger, function = await _native_adoption_context(connection, schema)
    _require_candidate_registration(candidate, row, guards, function)
    return _native_adoption_evidence(schema, row, ledger, function)


async def verify_retired_native_input_adoption(connection, *, evidence):
    """Verify named revision state after the ordinary guard has been retired."""
    schema = npi._schema_name(evidence["schema_name"])
    validate_native_input_adoption(evidence, schema=schema)
    if not connection.is_in_transaction():
        raise RuntimeError("cms_native_input_original_transaction_required")
    row = await _native_adoption_row(connection, schema, evidence["table_name"])
    if row is None or await _native_relation_guards(connection, schema, evidence["table_name"], schema):
        raise RuntimeError("cms_native_input_adoption_changed")
    ledger, function = await _native_adoption_context(connection, schema)
    current = _native_adoption_evidence(schema, row, ledger, function)
    if any(current[field] != evidence[field] for field in ("schema_name", "table_name", "revision", "mutation_xid")):
        raise RuntimeError("cms_native_input_adoption_changed")


class _NativeNuccSession:
    """Expose fixed native NUCC read/lock statements on the caller's exact driver."""

    def __init__(self, connection):
        self.connection = connection

    def in_transaction(self):
        """Keep all statements inside the caller's active transaction."""
        return self.connection.is_in_transaction()

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

    async def all(self, statement, **parameters):
        """Read metadata rows through the caller's active native transaction."""
        query, arguments = self._compiled(statement, parameters)
        return await self.connection.fetch(query, *arguments)

    async def first(self, statement, **parameters):
        """Expose the native record mapping used by the existing generation validator."""
        query, arguments = self._compiled(statement, parameters)
        row = await self.connection.fetchrow(query, *arguments)
        return None if row is None else dict(row)


async def finish_local_native_input_publication(session, *, prepared):
    """Advance existing counters once after the candidate takes its declared live name."""
    from process.provider_directory_generic_source_policy import _require_active_source_session
    from process.provider_directory_projection_stage import _copy_driver

    if prepared is None:
        return
    _require_active_source_session(session)
    if type(prepared) is not dict or prepared.get("version") != 2:
        raise RuntimeError("cms_native_input_local_publication_scope_changed")
    schema = npi._schema_name(prepared["ledger_schema"])
    candidates = tuple(NativeInputCandidate(**fields) for fields in prepared["candidates"])
    _selected_candidates(schema, candidates)
    registration_rows = prepared["registrations"]
    _provider_registration_rows(candidates, registration_rows)
    driver = await _copy_driver(session)
    async with driver.transaction():
        for candidate, previous in zip(candidates, registration_rows, strict=True):
            if previous["ledger_schema"] != schema:
                raise RuntimeError("cms_native_input_registration_changed")
            live = NativeInputCandidate(
                candidate.target_schema, candidate.target_table, candidate.target_schema, candidate.target_table
            )
            ledger = await _native_registration_ledger(driver, schema)
            function = await _native_registration_function(driver, schema, ledger)
            current = await _native_candidate_registration(driver, schema, live, function)
            if current != previous:
                raise RuntimeError("cms_native_input_publication_revision_changed")
            column = await driver.fetchval(
                "SELECT attname::text FROM pg_attribute WHERE attrelid=to_regclass($1) "
                "AND attnum>0 AND NOT attisdropped AND attgenerated='' AND attidentity='' ORDER BY attnum LIMIT 1",
                _candidate_relation(live.stage_schema, live.stage_table),
            )
            if type(column) is not str:
                raise RuntimeError("cms_native_input_publication_column_unavailable")
            column = npi._schema_name(column)
            await driver.execute(
                f'UPDATE {_candidate_relation(live.stage_schema, live.stage_table)} SET "{column}"="{column}" WHERE FALSE'
            )
            updated = await _native_candidate_registration(driver, schema, live, function)
            if updated != {**previous, "revision": previous["revision"] + 1}:
                raise RuntimeError("cms_native_input_publication_revision_changed")
