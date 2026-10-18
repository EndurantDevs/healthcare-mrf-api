# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure protocol tests with declared catalog rows; these do not prove native authority."""

import asyncio
import datetime
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_inputs as inputs

_ADVANCE = """
BEGIN
    UPDATE "sample"."cms_native_input_revision" SET revision=revision+1,mutation_xid=pg_current_xact_id() WHERE relation_oid=TG_RELID::bigint;
    IF NOT FOUND THEN RAISE EXCEPTION 'cms_native_input_not_registered'; END IF;
    RETURN NULL;
END;
"""
_LEDGER_GUARD = """
BEGIN
    IF TG_OP='UPDATE' AND pg_trigger_depth()=2
       AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name
       AND NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1
       AND NEW.mutation_xid=pg_current_xact_id() THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'cms_native_input_revision_immutable';
END;
"""
_ROOTS = (
    ("sample", "address_alias_v1"),
    ("sample", "address_archive_v2"),
    ("sample", "facility_anchor"),
    ("sample", "facility_anchor_npi_override"),
    ("sample", "mrf_address"),
    ("sample", "nucc_taxonomy"),
    ("sample", "openaddresses_geocode"),
    ("sample", "provider_directory_dataset_affiliation_organization"),
    ("sample", "provider_directory_dataset_insurance_plan"),
    ("sample", "provider_directory_dataset_network_plan"),
    ("sample", "provider_enrichment_summary"),
    ("sample", "provider_enrollment_ffs"),
    ("sample", "provider_enrollment_ffs_additional_npi"),
    ("sample", "provider_enrollment_ffs_address"),
    ("sample", "provider_enrollment_fqhc"),
    ("sample", "provider_enrollment_hospital"),
    ("tiger", "zcta5"),
    ("tiger", "zip_state"),
)


class NativeProtocol:
    """Model only the API's declared native catalog dependency and savepoint outcomes."""

    def __init__(self, *, target_root=("sample", "mrf_address")):
        self.target_root = target_root
        self.calls = []
        self.rows = {}
        self.guards = {}
        self.other_native_guards = []
        self.unsafe = False
        self.publisher_profile_valid = True
        self.ordinary_writer = False
        self.publisher_requires_set = False
        self.can_insert = True
        self.can_read_ledger = True
        self.can_lock_ledger = True
        self.current_roots = {target_root: 41}
        self.fail_trigger = False
        self.actor_changed = False
        self.actor_reads = 0
        self.active = True
        self.heap = dict(
            relation_oid=41,
            owner_oid=19,
            incumbent_oid=31,
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
            relispartition=False,
            inherited=False,
        )
        self.ledger = dict(ledger_oid=21, owner_oid=17, safe=True)
        self.function = dict(function_oid=51, owner_oid=17, safe=True, prosrc=_ADVANCE)
        self.ledger_guards = [
            dict(
                tgname=name,
                tgtype=kind,
                enabled="A",
                prosrc=_LEDGER_GUARD,
                proconfig=["search_path=pg_catalog"],
                prosecdef=False,
                lanname="plpgsql",
                tgnargs=0,
                unconditional=True,
            )
            for name, kind in (
                ("cms_native_input_revision_immutable", 27),
                ("cms_native_input_revision_no_truncate", 34),
            )
        ]

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self):
        before = deepcopy((self.rows, self.guards))
        try:
            yield
        except BaseException:
            self.rows, self.guards = before
            raise

    async def fetchrow(self, statement, *arguments):
        self.calls.append(("fetchrow", statement, arguments))
        if statement.startswith("SELECT session_user"):
            self.actor_reads += 1
            return 17, 99 if self.actor_changed and self.actor_reads > 1 else 17, 7101, "101"
        if statement.startswith("SELECT current_user"):
            return dict(actor_oid=17, backend_pid=7101, transaction_id="101", database_oid=81)
        if "AS relation_oid" in statement:
            heap = deepcopy(self.heap)
            if arguments[0] == arguments[1]:
                heap["relation_oid"] = heap["incumbent_oid"] = next(
                    oid
                    for (schema, table), oid in self.current_roots.items()
                    if arguments[0] == f'"{schema}"."{table}"'
                )
            return heap
        if "AS ledger_oid" in statement:
            return deepcopy(self.ledger)
        if "AS function_oid" in statement:
            return deepcopy(self.function)
        if statement.startswith("SELECT relation_oid,"):
            if not self.can_read_ledger:
                raise PermissionError("native ledger SELECT denied")
            return deepcopy(self.rows.get(arguments[0]))
        raise AssertionError(statement)

    async def fetch(self, statement, *arguments):
        self.calls.append(("fetch", statement, arguments))
        if statement.startswith("SELECT relation_oid FROM "):
            if not self.can_read_ledger:
                raise PermissionError("native ledger SELECT denied")
            return []
        if "FROM unnest" in statement:
            return [
                dict(namespace=schema, name=table, oid=self.current_roots.get((schema, table)), kind="r")
                for schema, table in zip(*arguments, strict=True)
            ]
        if "FROM pg_trigger trigger" in statement:
            return deepcopy(self.ledger_guards)
        if "FROM pg_trigger t WHERE" in statement:
            guards = deepcopy(self.guards.get(arguments[0], []))
            if "t.tgname=" not in statement:
                guards.extend(
                    dict(
                        trigger_oid=int(guard_record["oid"]),
                        tgname=guard_record["tgname"],
                        function_oid=int(guard_record["tgfoid"]),
                        safe=False,
                    )
                    for guard_record in self.other_native_guards
                )
            return guards
        raise AssertionError(statement)

    async def fetchval(self, statement, *arguments):
        self.calls.append(("fetchval", statement, arguments))
        if "FROM pg_roles owner,pg_roles publisher" in statement:
            assert (
                "nspname='hp_snapshot_retention'" in statement
                and "'SET'" in statement
                and "NOT publisher.rolinherit" in statement
            )
            return self.publisher_profile_valid
        if "pg_roles principal" in statement:
            return self.unsafe or self.ordinary_writer or (self.publisher_requires_set and arguments[2] != 29)
        if "jsonb_build_object" in statement:
            guards = self.guards.get(arguments[0], [])
            return dict(
                relation={
                    "oid": str(arguments[0]),
                    "relhastriggers": bool(guards or self.other_native_guards),
                    "relowner": str(self.heap["owner_oid"]),
                    "relacl": None,
                },
                columns=[],
                indexes=[],
                constraints=[],
                triggers=sorted(
                    [*_map_native_guards(guards), *deepcopy(self.other_native_guards)], key=lambda row: int(row["oid"])
                )
                or None,
                rules=[],
                functions=[
                    dict(native=dict(oid="51", proowner="17"), definition=_ADVANCE),
                    dict(native=dict(oid="52", proowner="17"), definition=_LEDGER_GUARD),
                ],
            )
        raise AssertionError(statement)

    async def execute(self, statement, *arguments):
        self.calls.append(("execute", statement, arguments))
        if statement.startswith('LOCK TABLE "sample"."cms_native_input_revision"') and (
            not self.can_read_ledger or not self.can_lock_ledger
        ):
            raise PermissionError("native ledger lock denied")
        if statement.startswith("INSERT INTO"):
            if not self.can_insert:
                raise PermissionError("native ledger INSERT denied")
            oid, schema, table = arguments
            assert oid not in self.rows
            self.rows[oid] = dict(relation_oid=oid, schema_name=schema, table_name=table, revision=0)
        elif statement.startswith("CREATE TRIGGER"):
            self.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=False)]
            if self.fail_trigger:
                raise asyncio.CancelledError("cancelled during enrollment")
        elif statement.startswith("ALTER TABLE"):
            self.guards[41][0]["safe"] = True
        else:
            assert statement.startswith("LOCK TABLE"), statement

    def candidate(self):
        return inputs.NativeInputCandidate(*self.target_root, "stage", "incoming", 41, 19, 31)


def _writes(native):
    return [
        statement
        for method, statement, _ in native.calls
        if method == "execute" and not statement.startswith("LOCK TABLE")
    ]


async def _enroll(native):
    return await inputs.register_native_input_candidates(
        native, ledger_schema="sample", candidates=(native.candidate(),), expected_actor_oid=17
    )


def _map_native_guards(guards):
    return [_native_trigger(guard) for guard in guards]


def _native_trigger(guard):
    return dict(
        oid=str(guard["trigger_oid"]),
        tgrelid="41",
        tgparentid="0",
        tgname="cms_native_input_mutation",
        tgfoid=str(guard["function_oid"]),
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


def test_fixed_roots_and_candidate_contract_are_closed():
    assert inputs.native_input_registration_roots("sample") == _ROOTS
    candidate = NativeProtocol().candidate()
    with pytest.raises(FrozenInstanceError):
        candidate.relation_oid = 99
    for field, value in (
        ("relation_oid", True),
        ("owner_oid", 0),
        ("incumbent_oid", "31"),
        ("stage_schema", True),
        ("stage_table", " incoming "),
    ):
        with pytest.raises(RuntimeError, match="candidate_invalid"):
            replace(candidate, **{field: value})


@pytest.mark.asyncio
@pytest.mark.parametrize("target_root", _ROOTS)
async def test_selected_registration_never_expands_into_unselected_inputs(target_root):
    native = NativeProtocol(target_root=target_root)
    evidence = await _enroll(native)
    assert native.rows == {41: dict(relation_oid=41, schema_name=target_root[0], table_name=target_root[1], revision=0)}
    assert evidence["ledger"] == dict(ledger_oid=21, owner_oid=17, safe=True)
    assert evidence["function_oid"] == 51
    assert evidence["registrations"] == [dict(native.rows[41], trigger_oid=61, function_oid=51)]
    assert evidence["candidates"] == [
        dict(
            target_schema=target_root[0],
            target_table=target_root[1],
            stage_schema="stage",
            stage_table="incoming",
            relation_oid=41,
            owner_oid=19,
            incumbent_oid=31,
        )
    ]
    assert evidence["native_inventory"][0]["relation"] == dict(
        oid="41", relhastriggers=True, relowner="19", relacl=None
    )
    assert evidence["before_native_inventory"][0]["relation"]["relhastriggers"] is False
    assert {
        key: evidence[key] for key in ("actor_oid", "backend_pid", "transaction_id", "database_oid", "count")
    } == dict(actor_oid=17, backend_pid=7101, transaction_id="101", database_oid=81, count=1)
    locks = [statement for method, statement, _ in native.calls if method == "execute" and statement.startswith("LOCK")]
    assert all('"npi"' not in statement and '"doctor_clinician_address"' not in statement for statement in locks)
    assert _writes(native) == [
        'INSERT INTO "sample"."cms_native_input_revision" (relation_oid,schema_name,table_name) VALUES ($1,$2,$3)',
        'CREATE TRIGGER cms_native_input_mutation AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON "stage"."incoming" '
        'FOR EACH STATEMENT EXECUTE FUNCTION "sample".cms_native_input_advance()',
        'ALTER TABLE "stage"."incoming" ENABLE ALWAYS TRIGGER cms_native_input_mutation',
    ]


@pytest.mark.asyncio
async def test_retry_and_owner_verification_never_repair_or_repeat_enrollment():
    native = NativeProtocol()
    first = await _enroll(native)
    native.calls.clear()
    retry = await _enroll(native)
    assert retry["before_native_inventory"] == retry["native_inventory"]
    assert {key: value for key, value in retry.items() if key != "before_native_inventory"} == {
        key: value for key, value in first.items() if key != "before_native_inventory"
    }
    assert _writes(native) == []
    native.calls.clear()
    assert await inputs.verify_native_input_candidates(
        native, ledger_schema="sample", candidates=(native.candidate(),)
    ) == {key: value for key, value in first.items() if key != "before_native_inventory"}
    assert _writes(native) == []
    assert all("SHARE ROW EXCLUSIVE" not in statement for _, statement, _ in native.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["owner", "oid", "incumbent", "kind", "rls", "inherited", "unsafe"])
async def test_changed_or_open_candidate_cannot_be_registered(change):
    native = NativeProtocol()
    candidate = native.candidate()
    if change in {"owner", "oid", "incumbent"}:
        native.heap[{"owner": "owner_oid", "oid": "relation_oid", "incumbent": "incumbent_oid"}[change]] = 99
    elif change == "kind":
        native.heap["relkind"] = "v"
    elif change == "rls":
        native.heap["relrowsecurity"] = True
    elif change == "inherited":
        native.heap["inherited"] = True
    else:
        native.unsafe = True
    with pytest.raises(RuntimeError, match="cms_native_input_candidate"):
        await inputs.register_native_input_candidates(
            native, ledger_schema="sample", candidates=(candidate,), expected_actor_oid=17
        )
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["body", "function_owner", "function_shape", "ledger_shape", "ledger_guard"])
async def test_changed_native_ledger_or_function_denies_before_insert(change):
    native = NativeProtocol()
    if change == "body":
        native.function["prosrc"] += "-- changed"
    elif change == "function_owner":
        native.function["owner_oid"] = 99
    elif change == "function_shape":
        native.function["safe"] = False
    elif change == "ledger_shape":
        native.ledger["safe"] = False
    else:
        native.ledger_guards[0]["enabled"] = "O"
    with pytest.raises(RuntimeError, match="cms_address_native_revision"):
        await _enroll(native)
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["schema", "table", "revision", "missing_guard", "wrong_guard", "extra_guard"])
async def test_existing_registration_drift_is_never_repaired(change):
    native = NativeProtocol()
    await _enroll(native)
    match change:
        case "schema":
            native.rows[41]["schema_name"] = "other"
        case "table":
            native.rows[41]["table_name"] = "other"
        case "revision":
            native.rows[41]["revision"] = -1
        case "missing_guard":
            native.guards.clear()
        case "wrong_guard":
            native.guards[41][0]["function_oid"] = 99
        case _:
            native.guards[41].append(dict(native.guards[41][0], trigger_oid=62))
    native.calls.clear()
    with pytest.raises(RuntimeError, match="registration_changed"):
        await _enroll(native)
    assert _writes(native) == []


@pytest.mark.asyncio
async def test_actor_integer_never_grants_native_insert_permission():
    native = NativeProtocol()
    native.can_insert = False
    with pytest.raises(PermissionError, match="INSERT denied"):
        await _enroll(native)
    assert native.rows == native.guards == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["cancel", "actor_change"])
async def test_caller_caught_failure_rolls_back_complete_enrollment(failure):
    native = NativeProtocol()
    native.fail_trigger = failure == "cancel"
    native.actor_changed = failure == "actor_change"
    error_type = asyncio.CancelledError if failure == "cancel" else RuntimeError
    with pytest.raises(error_type):
        await _enroll(native)
    assert native.rows == native.guards == {}
    assert _writes(native)


@pytest.mark.asyncio
async def test_missing_enrollment_is_read_only_and_requires_operator():
    native = NativeProtocol()
    with pytest.raises(RuntimeError, match="input_not_registered"):
        await inputs.verify_native_input_candidates(native, ledger_schema="sample", candidates=(native.candidate(),))
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["unknown", "duplicate", "list", "empty", "overflow", "outside_transaction"])
async def test_scope_errors_cannot_reach_native_mutation(scope):
    native = NativeProtocol()
    candidate = native.candidate()
    candidates = (candidate,)
    match scope:
        case "unknown":
            candidates = (replace(candidate, target_table="npi"),)
        case "duplicate":
            candidates *= 2
        case "list":
            candidates = [candidate]
        case "empty":
            candidates = ()
        case "overflow":
            candidates *= 19
        case _:
            native.active = False
    with pytest.raises(RuntimeError, match="cms_native_input_"):
        await inputs.register_native_input_candidates(
            native, ledger_schema="sample", candidates=candidates, expected_actor_oid=17
        )
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("presence", ["all", "one", "none"])
async def test_initial_roster_keeps_present_and_absent_roots_explicit(presence):
    native = NativeProtocol()
    if presence == "all":
        native.current_roots = {root: 41 + index for index, root in enumerate(_ROOTS)}
    elif presence == "none":
        native.current_roots = {}
    result = await inputs.capture_current_native_input_candidates(native, ledger_schema="sample")
    assert {(c.target_schema, c.target_table) for c in result["candidates"]} == set(native.current_roots)
    assert set(result["absent"]) == set(_ROOTS) - set(native.current_roots)
    assert all(
        (c.stage_schema, c.stage_table) == (c.target_schema, c.target_table) and c.incumbent_oid == c.relation_oid
        for c in result["candidates"]
    )
    assert _writes(native) == []


@pytest.mark.asyncio
async def test_catalog_linkage_needs_no_ledger_access():
    native = NativeProtocol()
    registered = await _enroll(native)
    native.can_read_ledger = False
    native.calls.clear()
    result = await inputs.verify_native_input_candidate_catalog(
        native, ledger_schema="sample", candidates=(native.candidate(),), registrations=registered["registrations"]
    )
    assert result["registration_source"] == "provider-linkage"
    assert result["registrations"] == registered["registrations"]
    assert result["registrations_sha256"] == registered["registrations_sha256"]
    assert _writes(native) == []
    assert not any(
        statement.startswith("SELECT relation_oid,") or 'LOCK TABLE "sample"."cms_native_input_revision"' in statement
        for _, statement, _ in native.calls
    )
    with pytest.raises(PermissionError, match="ledger SELECT denied"):
        await inputs.verify_native_input_candidates(native, ledger_schema="sample", candidates=(native.candidate(),))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [("trigger_oid", 99), ("function_oid", 99), ("schema_name", "other"), ("relation_oid", True), ("revision", -1)],
)
async def test_provider_linkage_cannot_substitute_native_objects(field, value):
    native = NativeProtocol()
    evidence = await _enroll(native)
    rows = deepcopy(evidence["registrations"])
    rows[0][field] = value
    native.can_read_ledger = False
    with pytest.raises(RuntimeError, match="registration_changed"):
        await inputs.verify_native_input_candidate_catalog(
            native, ledger_schema="sample", candidates=(native.candidate(),), registrations=rows
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["owner", "acl", "column", "index", "constraint", "rule", "function", "guard", "extra_guard"]
)
async def test_admin_delta_refuses_every_change_beyond_the_existing_guard(mutation):
    native = NativeProtocol()
    evidence = await _enroll(native)
    before, after = deepcopy(evidence["before_native_inventory"]), deepcopy(evidence["native_inventory"])
    if mutation in {"owner", "acl"}:
        after[0]["relation"]["relowner" if mutation == "owner" else "relacl"] = 99
    elif mutation in {"column", "index", "constraint", "rule"}:
        after[0][
            {"column": "columns", "index": "indexes", "constraint": "constraints", "rule": "rules"}[mutation]
        ].append({"changed": True})
    elif mutation == "function":
        after[0]["functions"][0]["definition"] += "-- changed"
    elif mutation == "guard":
        after[0]["triggers"][0]["tgtype"] = 4
    else:
        after[0]["triggers"].append(dict(after[0]["triggers"][0], oid="99"))
    with pytest.raises(RuntimeError, match="registration_catalog_changed"):
        inputs.require_native_input_registration_delta(before, after, evidence["registrations"])


@pytest.mark.asyncio
async def test_native_catalog_oid_strings_keep_numeric_guard_order_and_pg_types():
    native = NativeProtocol()
    native.other_native_guards = [
        dict(_native_trigger(dict(trigger_oid=oid, function_oid=12)), tgname=f"essential_{oid}", tgtype=27)
        for oid in (9, 100)
    ]
    evidence = await _enroll(native)
    before, after = evidence["before_native_inventory"], evidence["native_inventory"]
    assert [guard["oid"] for guard in after[0]["triggers"]] == ["9", "61", "100"]
    assert before[0]["triggers"] == [native.other_native_guards[0], native.other_native_guards[1]]
    guard = after[0]["triggers"][1]
    assert {
        key: guard[key]
        for key in ("oid", "tgrelid", "tgparentid", "tgfoid", "tgconstrrelid", "tgconstrindid", "tgconstraint")
    } == dict(
        oid="61", tgrelid="41", tgparentid="0", tgfoid="51", tgconstrrelid="0", tgconstrindid="0", tgconstraint="0"
    )
    assert type(guard["tgtype"]) is int and guard["tgtype"] == 60
    assert type(guard["tgnargs"]) is int and guard["tgnargs"] == 0
    assert type(guard["tgattr"]) is list and guard["tgattr"] == []
    assert type(guard["tgisinternal"]) is bool and guard["tgisinternal"] is False
    assert type(after[0]["relation"]["relhastriggers"]) is bool
    saved = deepcopy((before, after))
    inputs.require_native_input_registration_delta(before, after, evidence["registrations"])
    inputs.require_native_input_registration_delta(after, after, evidence["registrations"])
    assert (before, after) == saved


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["oid", "tgrelid", "tgparentid", "tgfoid", "tgconstrrelid", "tgconstrindid", "tgconstraint"]
)
@pytest.mark.parametrize("mutation", ["integer", "padded", "different", "malformed"])
async def test_native_guard_oid_json_requires_exact_canonical_strings(field, mutation):
    evidence = await _enroll(NativeProtocol())
    changed = deepcopy(evidence["native_inventory"])
    guard = changed[0]["triggers"][0]
    oid = guard[field]
    guard[field] = {"integer": int(oid), "padded": "0" + oid, "different": str(int(oid) + 1), "malformed": "not_oid"}[
        mutation
    ]
    with pytest.raises(RuntimeError, match="registration_catalog_changed"):
        inputs.require_native_input_registration_delta(
            evidence["before_native_inventory"], changed, evidence["registrations"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("oid", [41, True, 41.0, "041", "+41", " 41", "not_oid", "42"])
async def test_native_relation_oid_json_never_coerces_supplied_inventory(oid):
    evidence = await _enroll(NativeProtocol())
    before, after = deepcopy(evidence["before_native_inventory"]), deepcopy(evidence["native_inventory"])
    for inventory in (before, after):
        inventory[0]["relation"]["oid"] = oid
    with pytest.raises(RuntimeError, match="registration_catalog_changed"):
        inputs.require_native_input_registration_delta(before, after, evidence["registrations"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "pg_integer",
        "pg_bool",
        "missing",
        "extra",
        "name",
        "vector_string",
        "vector_tuple",
        "vector_values",
        "vector_null",
    ],
)
async def test_native_guard_json_keeps_exact_non_oid_types_and_shape(mutation):
    evidence = await _enroll(NativeProtocol())
    changed = deepcopy(evidence["native_inventory"])
    guard = changed[0]["triggers"][0]
    if mutation == "missing":
        guard.pop("tgqual")
    else:
        field, value = {
            "pg_integer": ("tgtype", "60"),
            "pg_bool": ("tgisinternal", 0),
            "extra": ("unexpected", False),
            "name": ("tgname", "different_guard"),
            "vector_string": ("tgattr", ""),
            "vector_tuple": ("tgattr", ()),
            "vector_values": ("tgattr", [1]),
            "vector_null": ("tgattr", None),
        }[mutation]
        guard[field] = value
    with pytest.raises(RuntimeError, match="registration_catalog_changed"):
        inputs.require_native_input_registration_delta(
            evidence["before_native_inventory"], changed, evidence["registrations"]
        )


@pytest.mark.asyncio
async def test_existing_essential_guards_survive_selected_enrollment_and_catalog_linkage():
    native = NativeProtocol(target_root=("sample", "address_alias_v1"))
    essential_by_field = dict(
        _native_trigger(dict(trigger_oid=11, function_oid=12)), tgname="address_alias_v1_immutable_trg", tgtype=27
    )
    native.other_native_guards = [essential_by_field]
    evidence = await _enroll(native)
    assert evidence["before_native_inventory"][0]["triggers"] == [essential_by_field]
    assert evidence["native_inventory"][0]["triggers"][0] == essential_by_field
    assert native.other_native_guards == [essential_by_field]
    native.can_read_ledger = False
    result = await inputs.verify_native_input_candidate_catalog(
        native, ledger_schema="sample", candidates=(native.candidate(),), registrations=evidence["registrations"]
    )
    assert result["native_inventory"] == evidence["native_inventory"]
    native.can_read_ledger = True
    assert (await _enroll(native))["native_inventory"] == evidence["native_inventory"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["removed", "changed", "added"])
async def test_existing_essential_guard_catalog_cannot_change_during_enrollment(mutation):
    native = NativeProtocol()
    essential_by_field = dict(
        _native_trigger(dict(trigger_oid=11, function_oid=12)), tgname="address_alias_v1_immutable_trg", tgtype=27
    )
    native.other_native_guards = [essential_by_field]
    evidence = await _enroll(native)
    changed = deepcopy(evidence["native_inventory"])
    if mutation == "removed":
        changed[0]["triggers"].pop(0)
    elif mutation == "changed":
        changed[0]["triggers"][0]["tgenabled"] = "D"
    else:
        changed[0]["triggers"].append(dict(essential_by_field, oid="99"))
    with pytest.raises(RuntimeError, match="registration_catalog_changed"):
        inputs.require_native_input_registration_delta(
            evidence["before_native_inventory"], changed, evidence["registrations"]
        )


@pytest.mark.asyncio
async def test_declared_retained_publisher_set_role_is_not_an_ordinary_writer():
    native = NativeProtocol()
    native.publisher_requires_set = True
    with pytest.raises(RuntimeError, match="candidate_not_closed"):
        await _enroll(native)
    result = await inputs.register_native_input_candidates(
        native,
        ledger_schema="sample",
        candidates=(native.candidate(),),
        expected_actor_oid=17,
        closed_owner_publisher=(19, 29),
    )
    assert result["count"] == 1
    native.can_read_ledger = False
    assert (
        await inputs.verify_native_input_candidate_catalog(
            native,
            ledger_schema="sample",
            candidates=(native.candidate(),),
            registrations=result["registrations"],
            closed_owner_publisher=(19, 29),
        )
    )["count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["wrong_publisher", "invalid_profile", "ordinary_writer", "bad_pair"])
async def test_declared_publisher_never_exempts_builder_or_untrusted_set_member(failure):
    native = NativeProtocol()
    native.publisher_requires_set = True
    role_oids = (19, 29)
    if failure == "wrong_publisher":
        role_oids = (19, 30)
    elif failure == "invalid_profile":
        native.publisher_profile_valid = False
    elif failure == "ordinary_writer":
        native.ordinary_writer = True
    else:
        role_oids = [19, 29]
    with pytest.raises(RuntimeError, match="cms_native_input_"):
        await inputs.register_native_input_candidates(
            native,
            ledger_schema="sample",
            candidates=(native.candidate(),),
            expected_actor_oid=17,
            closed_owner_publisher=role_oids,
        )
    assert _writes(native) == []


class LocalPublicationSession:
    """Expose only the original driver's unit boundary; no external engine exists."""

    def __init__(self, native):
        self.native = native
        self.executed = []

    def in_transaction(self):
        return self.native.active

    async def execute(self, statement):
        self.executed.append(str(statement))

    async def connection(self):
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.native))
        )


@pytest.mark.asyncio
async def test_local_owner_enrolls_selected_fixed_roots_then_replays_all_reads(monkeypatch):
    from process import native_publication_read_access

    native = NativeProtocol()
    native.heap["owner_oid"] = 17
    session = LocalPublicationSession(native)
    native_fetchrow = native.fetchrow
    native_fetchval = native.fetchval

    async def fetchrow(sql, *args):
        if "AS session_oid" in sql:
            return dict(session_oid=17, actor_oid=17)
        return await native_fetchrow(sql, *args)

    async def fetchval(sql, *args):
        if "has_table_privilege(current_user" in sql:
            assert "has_column_privilege" in sql and args == ('"sample"."cms_native_input_revision"',)
            return True
        return await native_fetchval(sql, *args)

    native.fetchrow, native.fetchval = fetchrow, fetchval

    async def replay(driver, **scope):
        assert driver is native and native.rows[41]["table_name"] == "mrf_address"
        assert scope["schema"] == "sample"

    preserved = AsyncMock(side_effect=replay)
    monkeypatch.setattr(native_publication_read_access, "preserve_publication_reads", preserved)
    evidence = await inputs.prepare_local_native_input_publication(
        session, ledger_schema="sample", relations=(("mrf_address", "incoming"), ("unselected_model", "other_stage"))
    )
    assert evidence["count"] == 1 and session.executed == ["SELECT 1"]
    assert [call.kwargs for call in preserved.await_args_list] == [
        dict(schema="sample", live_table="mrf_address", stage_table="incoming"),
        dict(schema="sample", live_table="unselected_model", stage_table="other_stage"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["wrong_session", "wrong_owner", "no_insert", "acl_replay"])
async def test_local_publication_denies_authority_drift_and_rolls_back_enrollment(monkeypatch, failure):
    from process import native_publication_read_access

    native = NativeProtocol()
    native.heap["owner_oid"] = 19 if failure == "wrong_owner" else 17
    session = LocalPublicationSession(native)
    native_fetchrow, native_fetchval = native.fetchrow, native.fetchval

    async def fetchrow(sql, *args):
        if "AS session_oid" in sql:
            return dict(session_oid=99 if failure == "wrong_session" else 17, actor_oid=17)
        return await native_fetchrow(sql, *args)

    async def fetchval(sql, *args):
        if "has_table_privilege(current_user" in sql:
            return failure != "no_insert"
        return await native_fetchval(sql, *args)

    native.fetchrow, native.fetchval = fetchrow, fetchval
    preserved = AsyncMock(side_effect=RuntimeError("read ACL changed"))
    monkeypatch.setattr(native_publication_read_access, "preserve_publication_reads", preserved)
    with pytest.raises(RuntimeError):
        await inputs.prepare_local_native_input_publication(
            session, ledger_schema="sample", relations=(("mrf_address", "incoming"),)
        )
    assert native.rows == native.guards == {}
    if failure != "acl_replay":
        preserved.assert_not_awaited()
    else:
        preserved.assert_awaited_once()


@pytest.mark.asyncio
async def test_local_publication_requires_the_existing_original_session_transaction():
    native = NativeProtocol()
    native.active = False
    session = LocalPublicationSession(native)
    with pytest.raises(RuntimeError, match="original_transaction_required"):
        await inputs.prepare_local_native_input_publication(
            session, ledger_schema="sample", relations=(("mrf_address", "incoming"),)
        )
    assert session.executed == native.calls == []


@pytest.mark.asyncio
async def test_installed_guard_resolver_needs_no_ledger_data():
    native = NativeProtocol()
    native.can_read_ledger = False
    assert await inputs.resolve_installed_native_input_guard(native, ledger_schema="sample", relation_oid=41) is None
    native.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=True)]
    native.other_native_guards = [
        dict(_native_trigger(dict(trigger_oid=11, function_oid=12)), tgname="existing_essential_guard")
    ]
    assert await inputs.resolve_installed_native_input_guard(native, ledger_schema="sample", relation_oid=41) == dict(
        trigger_oid=61, function_oid=51, ledger_oid=21, owner_oid=17
    )
    assert _writes(native) == []
    assert not any("LOCK TABLE" in sql or sql.startswith("SELECT relation_oid,") for _, sql, _ in native.calls)
    assert native.rows == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["guard", "function", "function_owner", "ledger", "duplicate"])
async def test_installed_guard_resolver_rejects_changed_native_contract(fault):
    native = NativeProtocol()
    native.can_read_ledger = False
    native.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=True)]
    if fault == "guard":
        native.guards[41][0]["safe"] = False
    elif fault == "function":
        native.function["prosrc"] += "changed"
    elif fault == "function_owner":
        native.function["owner_oid"] = 99
    elif fault == "ledger":
        native.ledger["safe"] = False
    else:
        native.guards[41] *= 2
    with pytest.raises(RuntimeError, match="cms_"):
        await inputs.resolve_installed_native_input_guard(native, ledger_schema="sample", relation_oid=41)
    assert native.rows == {} and _writes(native) == []


class ProtectedNuccProtocol(NativeProtocol):
    """Run the actual NUCC validator against the existing shared native-shaped fixture."""

    def __init__(self):
        from tests.test_nucc_snapshot_partition import parent_catalog

        super().__init__(target_root=("mrf", "nucc_taxonomy"))
        self.catalog = parent_catalog()
        self.current_roots = {("mrf", "nucc_taxonomy"): 201}
        self.generation = dict(
            importer_id="nucc",
            local_lineage_id="11111111-1111-1111-1111-111111111111",
            local_generation=3,
            origin_lineage_id="11111111-1111-1111-1111-111111111111",
            origin_generation=3,
            published_at=datetime.datetime(2026, 10, 18, tzinfo=datetime.UTC),
            relation_oids=[202],
        )
        self.catalog_reads = 0
        self.change_after_lock = False
        self.select_only_nucc = False

    async def fetch(self, sql, *arguments):
        if "FROM unnest" in sql:
            self.calls.append(("fetch", sql, arguments))
            return [
                dict(
                    namespace=schema,
                    name=table,
                    oid=self.current_roots.get((schema, table)),
                    kind="p" if (schema, table) == ("mrf", "nucc_taxonomy") else None,
                )
                for schema, table in zip(*arguments, strict=True)
            ]
        return await super().fetch(sql, *arguments)

    async def fetchval(self, sql, *arguments):
        from process import nucc_snapshot_partition as partition

        if sql == partition.CATALOG_SQL:
            self.calls.append(("fetchval", sql, arguments))
            self.catalog_reads += 1
            captured = deepcopy(self.catalog)
            if self.change_after_lock and self.catalog_reads > 1:
                captured["children"][0]["relation"]["filenode"] += 1
            return captured
        if sql == "SHOW search_path":
            self.calls.append(("fetchval", sql, arguments))
            return '"custom", public'
        return await super().fetchval(sql, *arguments)

    async def fetchrow(self, sql, *arguments):
        if "AS relation_oid" in sql and arguments[0] == '"mrf"."nucc_taxonomy"':
            self.calls.append(("fetchrow", sql, arguments))
            return dict(
                relation_oid=201,
                owner_oid=101,
                incumbent_oid=201,
                relkind="p",
                relpersistence="p",
                relrowsecurity=False,
                relforcerowsecurity=False,
                relispartition=False,
                inherited=True,
            )
        if sql.startswith("SELECT importer_id, local_lineage_id"):
            assert arguments == ("nucc",) and "WHERE importer_id=$1" in sql
            self.calls.append(("fetchrow", sql, arguments))
            return deepcopy(self.generation)
        return await super().fetchrow(sql, *arguments)

    async def execute(self, sql, *arguments):
        if (
            self.select_only_nucc
            and sql.startswith("LOCK TABLE")
            and '"mrf"."nucc_taxonomy"' in sql
            and " IN ACCESS SHARE MODE" not in sql
        ):
            raise PermissionError("protected NUCC permits SELECT locks only")
        if sql.startswith(("SET LOCAL search_path", "SELECT set_config", "LOCK TABLE ONLY")):
            self.calls.append(("execute", sql, arguments))
            return "OK"
        return await super().execute(sql, *arguments)


@pytest.mark.asyncio
async def test_initial_protected_nucc_uses_native_parent_and_generation_without_cms_guard():
    native = ProtectedNuccProtocol()
    observed = await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf")
    assert observed["candidates"] == ()
    assert observed["protected_roots"][0]["schema"] == "mrf"
    assert observed["protected_roots"][0]["table"] == "nucc_taxonomy"
    proof = observed["protected_roots"][0]["proof"]
    assert proof["physical"]["parent_oid"] == 201 and proof["physical"]["child_oid"] == 202
    assert proof["generation"]["relation_oids"] == [202]
    assert len(observed["absent"]) == 17 and native.rows == native.guards == {}
    assert native.catalog_reads == 2
    assert [(sql, args) for method, sql, args in native.calls if method == "execute"] == [
        ("SET LOCAL search_path=pg_catalog,pg_temp", ()),
        ('LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT', ()),
        ('LOCK TABLE ONLY "reference_family_archive_' + "a" * 32 + '"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT', ()),
        ("SELECT set_config('search_path',$1::VARCHAR,true)", ('"custom", public',)),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["mutable", "wrong_generation", "changed_child", "extra_child", "no_transaction"])
async def test_protected_nucc_observation_never_exempts_changed_native_authority(fault):
    native = ProtectedNuccProtocol()
    if fault == "mutable":
        native.catalog["children"][0]["relation"]["ordinary_write"] = True
    elif fault == "wrong_generation":
        native.generation["relation_oids"] = [999]
    elif fault == "changed_child":
        native.change_after_lock = True
    elif fault == "extra_child":
        native.catalog["children"] *= 2
    else:
        native.active = False
    with pytest.raises((RuntimeError, ValueError)):
        await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf")
    assert native.rows == native.guards == {}


@pytest.mark.asyncio
async def test_native_nucc_facade_preserves_parameter_order_and_original_transaction():
    from sqlalchemy import text

    from process.provider_directory_cms_native_registration import _NativeNuccSession

    native = ProtectedNuccProtocol()
    session = _NativeNuccSession(native)
    assert session.connection is native and session.in_transaction() is True
    assert session._compiled(text("SELECT :second,:first,:second"), dict(first=1, second=2)) == (
        "SELECT $1::INTEGER,$2::INTEGER,$1::INTEGER",
        (2, 1),
    )
    native.active = False
    assert session.in_transaction() is False
    assert not hasattr(session, "transaction") and not hasattr(session, "commit") and not hasattr(session, "close")


def _source_fence_physical_relations(protected):
    physical_by_name = {
        f'"{schema}"."{name}"': dict(
            relation_oid=None, relfilenode=None, relkind=None, relpersistence=None, inherited=False
        )
        for schema, name in inputs._relations("mrf")
    }
    physical_by_name['"mrf"."nucc_taxonomy"'] = dict(
        relation_oid=201,
        relfilenode=None if protected else 501,
        relkind="p" if protected else "r",
        relpersistence="p",
        inherited=protected,
    )
    return physical_by_name


def _assert_nucc_lock_modes(native, protected):
    nucc_locks = [
        sql
        for method, sql, _ in native.calls
        if method == "execute" and sql.startswith("LOCK TABLE") and '"mrf"."nucc_taxonomy"' in sql
    ]
    assert nucc_locks == (
        ['LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT']
        if protected
        else ['LOCK TABLE "mrf"."nucc_taxonomy" IN SHARE MODE NOWAIT']
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_source_fence_versions_plain_and_protected_nucc_without_changing_plain_pins(monkeypatch, protected):
    from process.provider_directory_cms_native_registration import _NativeNuccSession
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    native = ProtectedNuccProtocol()
    native.select_only_nucc = protected

    class SourceFenceSession(_NativeNuccSession):
        async def execute(self, statement, parameters=None):
            if str(statement) == "SELECT to_regclass(:name)":
                return SimpleNamespace(scalar_one=lambda: 21)
            if "current_database() AS database_name" in str(statement):
                return SimpleNamespace(
                    mappings=lambda: SimpleNamespace(
                        one=lambda: dict(database_name="synthetic", database_oid=81, system_identifier="sample")
                    )
                )
            return await super().execute(statement, parameters)

    session = SourceFenceSession(native)
    physical_by_name = _source_fence_physical_relations(protected)
    monkeypatch.setattr(inputs, "_physical_relations", AsyncMock(return_value=physical_by_name))
    monkeypatch.setattr(inputs, "_npi_revision", AsyncMock(return_value={"native": "npi"}))
    monkeypatch.setattr(
        inputs,
        "_reference_authorities",
        AsyncMock(return_value={key: {} for key in ("cms-doctors", "geo", "tiger", "mrf")}),
    )
    monkeypatch.setattr(inputs, "_geo_bindings", lambda *args: {"native": "geo"})
    revisions = AsyncMock(return_value=[{"plain_revision": 1}])
    monkeypatch.setattr(inputs, "_revision_guards", revisions)
    monkeypatch.setattr(
        inputs, "_cms_office_read_relations", AsyncMock(return_value={key: {} for key in CMS_OFFICE_READ_TABLES})
    )
    fence = await inputs.capture_native_address_input_fence(session, "mrf", cutover=True)
    assert fence["relations"] == physical_by_name and fence["npi"] == {"native": "npi"}
    assert fence["revisions"] == [{"plain_revision": 1}]
    if protected:
        assert fence["version"] == 2 and fence["nucc_protected"]["physical"]["child_oid"] == 202
        assert revisions.await_args.kwargs == dict(protected_nucc=fence["nucc_protected"])
        assert native.catalog_reads == 2
    else:
        assert fence == dict(
            version=1,
            schema="mrf",
            database=dict(database_name="synthetic", database_oid=81, system_identifier="sample"),
            relations=physical_by_name,
            npi={"native": "npi"},
            reference_authorities={key: {} for key in ("cms-doctors", "geo", "tiger", "mrf")},
            geo_bindings={"native": "geo"},
            revisions=[{"plain_revision": 1}],
            cms_office_read_relations={key: {} for key in CMS_OFFICE_READ_TABLES},
        )
        assert revisions.await_args.kwargs == {} and native.catalog_reads == 0
    _assert_nucc_lock_modes(native, protected)
    inputs.require_supported_native_address_inputs(fence)


@pytest.mark.asyncio
async def test_only_verified_fixed_nucc_parent_can_leave_generic_revision_guard_set(monkeypatch):
    native = ProtectedNuccProtocol()
    from process.provider_directory_cms_native_registration import _NativeNuccSession

    proof = await inputs.capture_protected_nucc_input(_NativeNuccSession(native), schema="mrf")
    parent_by_field = dict(relation_oid=201, relkind="p", relpersistence="p", inherited=True)
    inputs._require_heaps({'"mrf"."nucc_taxonomy"': parent_by_field}, protected_nucc=proof)
    for relation in ['"other"."nucc_taxonomy"', '"tiger"."zcta5"', '"mrf"."other"']:
        with pytest.raises(RuntimeError, match="requires_persistent_heap"):
            inputs._require_heaps({relation: parent_by_field}, protected_nucc=proof)
    with pytest.raises(RuntimeError, match="requires_persistent_heap"):
        inputs._require_heaps({'"mrf"."nucc_taxonomy"': dict(parent_by_field, relation_oid=999)}, protected_nucc=proof)
    statements = []

    async def execute(sql, parameters=None):
        statements.append((str(sql), parameters))
        if "FROM pg_trigger trigger" in str(sql):
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))

    monkeypatch.setattr(inputs, "_require_ledger_guards", AsyncMock())
    assert (
        await inputs._revision_guards(
            SimpleNamespace(execute=execute), "mrf", {'"mrf"."nucc_taxonomy"': parent_by_field}, protected_nucc=proof
        )
        == []
    )
    assert all(params["oids"] == [] for _, params in statements if params is not None)
    assert statements[0] == ('SELECT relation_oid FROM "mrf"."cms_native_input_revision" WHERE FALSE', None)
    assert native.rows == native.guards == {}


@pytest.mark.asyncio
async def test_native_nucc_facade_refuses_a_different_task_or_closed_transaction():
    from sqlalchemy import text

    from process.provider_directory_cms_native_registration import _NativeNuccSession

    native = ProtectedNuccProtocol()
    session = _NativeNuccSession(native)

    async def child():
        assert session.in_transaction() is False
        with pytest.raises(RuntimeError, match="original_transaction_required"):
            await session.scalar(text("SHOW search_path"))

    await asyncio.create_task(child())
    native.active = False
    with pytest.raises(RuntimeError, match="original_transaction_required"):
        await session.scalar(text("SHOW search_path"))
    assert native.calls == []


@pytest.mark.asyncio
async def test_initial_capture_keeps_plain_selected_input_beside_protected_nucc():
    native = ProtectedNuccProtocol()
    native.current_roots[("mrf", "mrf_address")] = 41
    observed = await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf")
    assert len(observed["candidates"]) == 1
    assert observed["candidates"][0].relation_oid == 41 and observed["candidates"][0].target_table == "mrf_address"
    assert observed["protected_roots"][0]["proof"]["physical"]["child_oid"] == 202
    assert len(observed["absent"]) == 16 and native.rows == native.guards == {}


@pytest.mark.asyncio
async def test_registration_needs_select_and_insert_without_ledger_lock_privilege():
    native = NativeProtocol()
    native.can_lock_ledger = False
    evidence = await _enroll(native)
    assert evidence["count"] == 1
    assert not any(sql.startswith('LOCK TABLE "sample"."cms_native_input_revision"') for _, sql, _ in native.calls)
    pins = [
        i
        for i, (method, sql, _) in enumerate(native.calls)
        if method == "fetch" and sql == 'SELECT relation_oid FROM "sample"."cms_native_input_revision" WHERE FALSE'
    ]
    catalog = next(i for i, (_, sql, _) in enumerate(native.calls) if "AS ledger_oid" in sql)
    assert pins and pins[0] < catalog
    assert any(
        sql == 'LOCK TABLE "sample"."mrf_address", "stage"."incoming" IN SHARE ROW EXCLUSIVE MODE NOWAIT'
        for _, sql, _ in native.calls
    )
    native.calls.clear()
    await _enroll(native)
    assert _writes(native) == []
    await inputs.verify_native_input_candidates(native, ledger_schema="sample", candidates=(native.candidate(),))
    assert _writes(native) == []


@pytest.mark.asyncio
async def test_ledger_select_failure_precedes_catalog_or_registration():
    native = NativeProtocol()
    native.can_read_ledger = False
    with pytest.raises(PermissionError, match="ledger SELECT denied"):
        await _enroll(native)
    assert not any("AS ledger_oid" in sql or "AS function_oid" in sql for _, sql, _ in native.calls)
    assert native.rows == native.guards == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("cutover,register", [(False, False), (True, False), (False, True)])
async def test_source_locks_preserve_heap_and_other_authorities_without_ledger_lock(cutover, register):
    statements = []

    async def execute(statement, parameters=None):
        statements.append((str(statement), parameters))
        return SimpleNamespace(scalar_one=lambda: 21)

    relation_by_field = dict(relation_oid=41)
    await inputs._lock_inputs(
        SimpleNamespace(execute=execute),
        "sample",
        {'"sample"."mrf_address"': relation_by_field},
        cutover=cutover,
        register=register,
    )
    locks = [sql for sql, _ in statements if sql.startswith("LOCK TABLE")]
    mode = "SHARE ROW EXCLUSIVE" if register else "SHARE" if cutover else "ACCESS SHARE"
    authority_mode = "SHARE" if register or cutover else "ACCESS SHARE"
    assert locks == [
        f'LOCK TABLE "sample"."mrf_address" IN {mode} MODE NOWAIT',
        f'LOCK TABLE "sample"."npi_result_generation" IN {authority_mode} MODE NOWAIT',
        f'LOCK TABLE "sample"."reference_family_result_generation" IN {authority_mode} MODE NOWAIT',
    ]
    assert all(
        "cms_native_input_revision" not in sql and (params is None or "cms_native_input_revision" not in str(params))
        for sql, params in statements
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    ["missing", "parent_oid", "schema", "plain_heap", "not_inherited", "filenode", "zero_filenode", "registration"],
)
async def test_protected_nucc_lock_exclusion_requires_exact_fresh_native_identity(fault):
    from process.provider_directory_cms_native_registration import _NativeNuccSession

    native = ProtectedNuccProtocol()
    proof = await inputs.capture_protected_nucc_input(_NativeNuccSession(native), schema="mrf")
    relation_by_field = dict(relation_oid=201, relkind="p", relpersistence="p", inherited=True, relfilenode=None)
    schema = "other" if fault == "schema" else "mrf"
    is_registration = fault == "registration"
    if fault == "missing":
        proof = None
    changes_by_fault = {
        "parent_oid": ("relation_oid", 999),
        "plain_heap": ("relkind", "r"),
        "not_inherited": ("inherited", False),
        "filenode": ("relfilenode", 99),
        "zero_filenode": ("relfilenode", 0),
    }
    if fault in changes_by_fault:
        field, changed_value = changes_by_fault[fault]
        relation_by_field[field] = changed_value
    execute = AsyncMock()
    with pytest.raises(RuntimeError, match="nucc_proof_changed"):
        await inputs._lock_inputs(
            SimpleNamespace(execute=execute),
            schema,
            {'"mrf"."nucc_taxonomy"': relation_by_field},
            cutover=True,
            register=is_registration,
            protected_nucc=proof,
        )
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_nucc_read_locks_leave_mutable_heap_and_authority_cutover_locks_unchanged():
    from process.provider_directory_cms_native_registration import _NativeNuccSession

    native = ProtectedNuccProtocol()
    native.select_only_nucc = True
    proof = await inputs.capture_protected_nucc_input(_NativeNuccSession(native), schema="mrf")
    relations_by_name = {
        '"mrf"."nucc_taxonomy"': dict(
            relation_oid=201, relkind="p", relpersistence="p", inherited=True, relfilenode=None
        ),
        '"mrf"."mrf_address"': dict(
            relation_oid=301, relkind="r", relpersistence="p", inherited=False, relfilenode=401
        ),
    }
    statements = []

    async def execute(statement, parameters=None):
        sql = str(statement)
        statements.append(sql)
        if sql.startswith("LOCK TABLE"):
            await native.execute(sql)
        return SimpleNamespace(scalar_one=lambda: 21)

    await inputs._lock_inputs(
        SimpleNamespace(execute=execute), "mrf", relations_by_name, cutover=True, protected_nucc=proof
    )
    assert [sql for sql in statements if sql.startswith("LOCK TABLE")] == [
        'LOCK TABLE "mrf"."mrf_address" IN SHARE MODE NOWAIT',
        'LOCK TABLE "mrf"."npi_result_generation" IN SHARE MODE NOWAIT',
        'LOCK TABLE "mrf"."reference_family_result_generation" IN SHARE MODE NOWAIT',
    ]
    assert [sql for method, sql, _ in native.calls if method == "execute" and "nucc_taxonomy" in sql] == [
        'LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        'LOCK TABLE ONLY "reference_family_archive_' + "a" * 32 + '"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
    ]


class CapturedTigerRosterProtocol(NativeProtocol):
    """Use real shared proof validation over declared catalog-only protocol rows."""

    def __init__(self):
        from tests.test_cms_tiger_native_inputs import _captured_catalog, _captured_proof

        super().__init__(target_root=("mrf", "mrf_address"))
        self.proof = _captured_proof()
        self.catalog = _captured_catalog(self.proof)
        self.current_roots.update({("tiger", "zcta5"): 901, ("tiger", "zip_state"): 902})
        self.catalog_error = None

    async def fetch(self, statement, *arguments):
        if "AS protected" in statement:
            from process.provider_directory_cms_tiger_inputs import captured_tiger_names

            self.calls.append(("fetch", statement, arguments))
            assert arguments == (captured_tiger_names(self.proof),)
            if self.catalog_error is not None:
                raise self.catalog_error
            return deepcopy(self.catalog)
        return await super().fetch(statement, *arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical", ["present", "absent"])
async def test_captured_tiger_roster_skips_only_logical_roots(canonical):
    from process.provider_directory_cms_tiger_inputs import ROOTS

    native = CapturedTigerRosterProtocol()
    if canonical == "absent":
        native.current_roots = {native.target_root: 41}
    capture_by_field = await inputs.capture_current_native_input_candidates(
        native, ledger_schema="mrf", protected_tiger=native.proof
    )
    assert [
        (candidate.target_schema, candidate.target_table, candidate.relation_oid)
        for candidate in capture_by_field["candidates"]
    ] == [("mrf", "mrf_address", 41)]
    assert capture_by_field["protected_roots"] == tuple(
        dict(schema="tiger", table=name, proof=native.proof) for name in ROOTS
    )
    assert len(capture_by_field["absent"]) == 15 and not any(
        schema == "tiger" for schema, _ in capture_by_field["absent"]
    )
    assert "parent_oids" not in native.proof
    proof_index = next(index for index, call in enumerate(native.calls) if "AS protected" in call[1])
    heap_index = next(
        index for index, call in enumerate(native.calls) if call[0] == "fetchrow" and "AS relation_oid" in call[1]
    )
    assert proof_index < heap_index and _writes(native) == []
    assert all(
        candidate.stage_schema != native.proof["inventory"]["relations"][0]["schema_name"]
        for candidate in capture_by_field["candidates"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "lock",
        "writable",
        "inherited",
        "oid",
        "file",
        "database",
        "missing",
        "extra",
        "digest",
        "shape",
        "transaction",
        "transport",
    ],
)
async def test_captured_roster_refuses_unverified_heaps(fault):
    native = CapturedTigerRosterProtocol()
    match fault:
        case "lock":
            native.catalog[0]["read_lock"] = False
        case "writable":
            native.catalog[0]["protected"] = False
        case "inherited":
            native.catalog[0]["inherited"] = True
        case "oid":
            native.catalog[0]["relation_oid"] += 1
        case "file":
            native.catalog[0]["relfilenode"] += 1
        case "database":
            native.catalog[0]["database_oid"] += 1
        case "missing":
            native.catalog.pop()
        case "extra":
            native.catalog.append(deepcopy(native.catalog[0]))
        case "digest":
            native.proof["selection_sha256"] = "f" * 64
        case "shape":
            native.proof["parent_oids"] = [["zcta5", 901], ["zip_state", 902]]
        case "transaction":
            native.active = False
        case "transport":
            native.catalog_error = OSError("synthetic catalog transport failure")
    with pytest.raises((RuntimeError, OSError)):
        await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf", protected_tiger=native.proof)
    assert _writes(native) == []
    assert not any(call[0] == "fetchrow" and "AS relation_oid" in call[1] for call in native.calls)


@pytest.mark.asyncio
async def test_native_adoption_captures_nullable_revision_without_mutation():
    from process import provider_directory_cms_native_registration as registration

    protocol = AdoptionProtocol(target_root=("sample", "nucc_taxonomy"))
    protocol.rows[41] = dict(
        relation_oid=41, schema_name="sample", table_name="nucc_taxonomy", revision=7, mutation_xid=None
    )
    protocol.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=True)]
    before = deepcopy(protocol.rows)
    captured = await registration.capture_native_input_adoption(
        protocol,
        ledger_schema="sample",
        candidate=replace(protocol.candidate(), stage_schema="sample", stage_table="nucc_taxonomy", incumbent_oid=41),
    )
    assert captured["revision"] == 7 and captured["mutation_xid"] is None
    assert captured["trigger_oid"] == 61 and captured["relation_oid"] == 41
    assert protocol.rows == before and not _writes(protocol)


@pytest.mark.asyncio
async def test_native_adoption_refuses_registered_heap_without_guard():
    from process import provider_directory_cms_native_registration as registration

    protocol = AdoptionProtocol(target_root=("sample", "nucc_taxonomy"))
    protocol.rows[41] = dict(
        relation_oid=41, schema_name="sample", table_name="nucc_taxonomy", revision=7, mutation_xid="101"
    )
    with pytest.raises(RuntimeError, match="registration_changed"):
        await registration.capture_native_input_adoption(
            protocol,
            ledger_schema="sample",
            candidate=replace(
                protocol.candidate(), stage_schema="sample", stage_table="nucc_taxonomy", incumbent_oid=41
            ),
        )
    assert not _writes(protocol)


class AdoptionProtocol(NativeProtocol):
    """Supply only the declared ledger lookup for adoption protocol host checks."""

    async def fetchval(self, statement, *arguments):
        if statement == "SELECT to_regclass($1)::oid::bigint":
            self.calls.append(("fetchval", statement, arguments))
            return 21
        return await super().fetchval(statement, *arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["target", "revision", "mutation", "function", "ledger", "duplicate", "missing_row"])
async def test_native_adoption_refuses_changed_custody_before_writes(fault):
    from process import provider_directory_cms_native_registration as registration

    protocol = AdoptionProtocol(target_root=("sample", "nucc_taxonomy"))
    protocol.rows[41] = dict(
        relation_oid=41, schema_name="sample", table_name="nucc_taxonomy", revision=7, mutation_xid="101"
    )
    protocol.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=True)]
    match fault:
        case "target":
            protocol.rows[41]["schema_name"] = "foreign"
        case "revision":
            protocol.rows[41]["revision"] = True
        case "mutation":
            protocol.rows[41]["mutation_xid"] = "01"
        case "function":
            protocol.function["prosrc"] += "-- changed"
        case "ledger":
            protocol.ledger_guards[0]["enabled"] = "D"
        case "duplicate":
            protocol.guards[41] *= 2
        case "missing_row":
            protocol.rows.clear()
    candidate = replace(protocol.candidate(), stage_schema="sample", stage_table="nucc_taxonomy", incumbent_oid=41)
    with pytest.raises(RuntimeError):
        await registration.capture_native_input_adoption(protocol, ledger_schema="sample", candidate=candidate)
    assert not _writes(protocol)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "revision", "mutation", "guard", "function"])
async def test_retired_adoption_requires_exact_unchanged_registration(fault):
    from process import provider_directory_cms_native_registration as registration

    protocol = AdoptionProtocol(target_root=("sample", "nucc_taxonomy"))
    protocol.rows[41] = dict(
        relation_oid=41, schema_name="sample", table_name="nucc_taxonomy", revision=7, mutation_xid=None
    )
    protocol.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=True)]
    candidate = replace(protocol.candidate(), stage_schema="sample", stage_table="nucc_taxonomy", incumbent_oid=41)
    evidence = await registration.capture_native_input_adoption(protocol, ledger_schema="sample", candidate=candidate)
    protocol.guards.clear()
    if fault == "revision":
        protocol.rows[41]["revision"] += 1
    elif fault == "mutation":
        protocol.rows[41]["mutation_xid"] = "102"
    elif fault == "guard":
        protocol.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=True)]
    elif fault == "function":
        protocol.function["owner_oid"] += 1
    before = deepcopy(protocol.rows)
    if fault is None:
        await registration.verify_retired_native_input_adoption(protocol, evidence=evidence)
    else:
        with pytest.raises(RuntimeError):
            await registration.verify_retired_native_input_adoption(protocol, evidence=evidence)
    assert protocol.rows == before and not _writes(protocol)


@pytest.mark.asyncio
async def test_unregistered_legacy_heap_remains_eligible_without_native_authority():
    from process import provider_directory_cms_native_registration as registration

    protocol = AdoptionProtocol(target_root=("sample", "nucc_taxonomy"))
    candidate = replace(protocol.candidate(), stage_schema="sample", stage_table="nucc_taxonomy", incumbent_oid=41)
    assert (
        await registration.capture_native_input_adoption(protocol, ledger_schema="sample", candidate=candidate) is None
    )
    assert not _writes(protocol)
    assert not any(method == "fetchrow" and "AS function_oid" in statement for method, statement, _ in protocol.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_guard", [False, True])
async def test_missing_native_ledger_only_permits_unregistered_legacy_heap(has_guard):
    from process import provider_directory_cms_native_registration as registration

    class MissingLedger(AdoptionProtocol):
        async def fetchval(self, statement, *arguments):
            if statement == "SELECT to_regclass($1)::oid::bigint":
                return None
            return await super().fetchval(statement, *arguments)

    protocol = MissingLedger(target_root=("sample", "nucc_taxonomy"))
    candidate = replace(protocol.candidate(), stage_schema="sample", stage_table="nucc_taxonomy", incumbent_oid=41)
    if has_guard:
        protocol.guards[41] = [dict(trigger_oid=61, tgname="cms_native_input_mutation", function_oid=51, safe=True)]
        with pytest.raises(RuntimeError, match="registration_changed"):
            await registration.capture_native_input_adoption(protocol, ledger_schema="sample", candidate=candidate)
    else:
        assert (
            await registration.capture_native_input_adoption(protocol, ledger_schema="sample", candidate=candidate)
            is None
        )
    assert not _writes(protocol)


@pytest.mark.parametrize(
    "transaction_id,is_valid",
    [
        (None, True),
        ("1", True),
        ("18446744073709551615", True),
        ("", False),
        ("0", False),
        ("18446744073709551616", False),
        pytest.param("1" * 21, False, id="over_uint64_length"),
        pytest.param("1" * 5000, False, id="oversized"),
    ],
)
def test_native_adoption_transaction_id_has_bounded_uint64_encoding(transaction_id, is_valid):
    from process import provider_directory_cms_native_registration as registration
    from tests.test_nucc_protected_bootstrap import native_registration

    evidence = native_registration()
    evidence["mutation_xid"] = transaction_id
    if is_valid:
        assert registration.validate_native_input_adoption(evidence, schema="mrf", relation_oid=202) == evidence
    else:
        with pytest.raises(RuntimeError, match="adoption_changed"):
            registration.validate_native_input_adoption(evidence, schema="mrf", relation_oid=202)
