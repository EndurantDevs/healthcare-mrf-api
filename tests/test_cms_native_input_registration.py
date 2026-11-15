# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named registration protocol, semantic drift and atomic revision checks."""

import asyncio
import datetime
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_native_registration as registration

_ADVANCE = inputs._advance_body("sample")
_NUCC_GENERATION_LOCK_SQL = 'SELECT local_generation FROM "mrf"."reference_family_result_generation" WHERE FALSE'
_NUCC_GENERATION_COLUMNS_SQL = (
    "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid=to_regclass($1::VARCHAR) "
    "AND attnum>0 AND NOT attisdropped AND attname IN ('serving_schema','serving_relation_names')"
)
_LEDGER_GUARD = """
BEGIN
    IF TG_OP='UPDATE' AND pg_trigger_depth()=2
       AND NEW.schema_name=OLD.schema_name
       AND NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1
       AND NEW.mutation_xid=pg_current_xact_id() THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'cms_native_input_revision_immutable';
END;
"""


class NativeProtocol:
    """A declared SQL protocol; real grants and native counters qualify separately."""

    def __init__(self, *, target_root=("sample", "mrf_address")):
        self.target_root = target_root
        self.calls = []
        self.rows = {}
        self.guards = {}
        self.other_native_guards = []
        self.active = True
        self.can_insert = True
        self.can_read_ledger = True
        self.can_lock_ledger = True
        self.fail_trigger = False
        self.fail_update = False
        self.current_roots = {target_root: True}
        self.heap = dict(
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
            relispartition=False,
            inherited=False,
        )
        self.ledger = dict(safe=True)
        self.function = dict(safe=True, prosrc=_ADVANCE)
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

    async def fetchrow(self, sql, *args):
        self.calls.append(("fetchrow", sql, args))
        if sql.startswith("SELECT c.relkind::text"):
            return deepcopy(self.heap)
        if sql.startswith("SELECT c.relkind='r'"):
            return deepcopy(self.ledger)
        if sql.startswith("SELECT p.prosrc"):
            return deepcopy(self.function)
        if sql.startswith("SELECT schema_name::text"):
            if not self.can_read_ledger:
                raise PermissionError("native ledger SELECT denied")
            return deepcopy(self.rows.get(tuple(args)))
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        self.calls.append(("fetch", sql, args))
        if "WHERE FALSE" in sql:
            if not self.can_read_ledger:
                raise PermissionError("native ledger SELECT denied")
            return []
        if "FROM unnest" in sql:
            return [
                dict(
                    namespace=schema,
                    name=name,
                    present=bool(self.current_roots.get((schema, name))),
                    kind="r" if self.current_roots.get((schema, name)) else None,
                )
                for schema, name in zip(*args, strict=True)
            ]
        if "FROM pg_trigger trigger" in sql:
            return deepcopy(self.ledger_guards)
        if "FROM pg_trigger t JOIN" in sql:
            return deepcopy(self.guards.get(tuple(re.findall(r'"([^"]+)"', args[0])), []))
        raise AssertionError(sql)

    async def fetchval(self, sql, *args):
        self.calls.append(("fetchval", sql, args))
        if "jsonb_build_object" in sql:
            schema, name = re.findall(r'"([^"]+)"', args[0])
            guards = self.guards.get((schema, name), [])
            candidate = inputs.NativeInputCandidate(*self.target_root, schema, name)
            functions = (
                [
                    dict(
                        schema_name="sample",
                        function_name="cms_native_input_advance",
                        definition="CREATE FUNCTION sample.cms_native_input_advance() RETURNS trigger AS $$"
                        + _ADVANCE
                        + "$$ LANGUAGE plpgsql SECURITY DEFINER",
                    )
                ]
                if guards
                else None
            )
            return dict(
                relation=dict(
                    schema_name=schema,
                    table_name=name,
                    kind="r",
                    persistence="p",
                    rls=False,
                    force_rls=False,
                    partition=False,
                ),
                columns=[dict(name="value", type="text")],
                indexes=[],
                constraints=[],
                triggers=([registration._expected_native_guard(candidate, "sample")] if guards else None),
                rules=[],
                functions=functions,
            )
        if "SELECT attname::text" in sql:
            return "value"
        if "to_regclass($1) IS NOT NULL" in sql:
            return True
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.calls.append(("execute", sql, args))
        if sql.startswith("INSERT INTO"):
            if not self.can_insert:
                raise PermissionError("native ledger INSERT denied")
            self.rows.setdefault(tuple(args), dict(schema_name=args[0], table_name=args[1], revision=0))
            return "OK"
        if sql.startswith("CREATE TRIGGER"):
            self._record_statement_guard(sql)
            return "OK"
        if sql.startswith("ALTER TABLE"):
            schema, name = re.findall(r'"([^"]+)"', sql)[:2]
            self.guards[(schema, name)][0].update(enabled="A", safe=True)
            return "OK"
        if sql.startswith("UPDATE "):
            schema, name = re.findall(r'"([^"]+)"', sql)[:2]
            self.rows[(schema, name)]["revision"] += 2 if self.fail_update else 1
            return "OK"
        if not sql.startswith("LOCK TABLE"):
            raise AssertionError(sql)
        return "OK"

    def _record_statement_guard(self, sql):
        """Record the exact supported guard before an injected DDL failure."""
        schema, name = re.search(r' ON "([^"]+)"\."([^"]+)"', sql).groups()
        self.guards[(schema, name)] = [
            dict(
                tgname="cms_native_input_mutation",
                tgtype=60,
                enabled="O",
                function_schema="sample",
                function_name="cms_native_input_advance",
                safe=False,
            )
        ]
        if self.fail_trigger:
            raise asyncio.CancelledError("registration interrupted")

    def candidate(self):
        return inputs.NativeInputCandidate(*self.target_root, "stage", "incoming")


def _writes(native):
    return [call for call in native.calls if call[0] == "execute" and not call[1].startswith("LOCK TABLE")]


async def _enroll(native):
    return await inputs.register_native_input_candidates(
        native, ledger_schema="sample", candidates=(native.candidate(),)
    )


def test_candidate_uses_only_declared_names_and_closed_scope():
    candidate = NativeProtocol().candidate()
    assert vars(candidate) == dict(
        target_schema="sample", target_table="mrf_address", stage_schema="stage", stage_table="incoming"
    )
    with pytest.raises(FrozenInstanceError):
        candidate.stage_table = "other"
    with pytest.raises((RuntimeError, ValueError)):
        inputs.NativeInputCandidate("unsafe.name", "input", "stage", "incoming")
    assert ("sample", "doctor_clinician_address") not in inputs.native_input_registration_roots("sample")
    assert ("sample", "geo_zip_lookup") not in inputs.native_input_registration_roots("sample")


@pytest.mark.asyncio
@pytest.mark.parametrize("target", [("sample", "mrf_address"), ("sample", "address_alias_v1"), ("tiger", "zcta5")])
async def test_selected_registration_is_named_additive_and_retry_is_read_only(target):
    native = NativeProtocol(target_root=target)
    evidence = await _enroll(native)
    assert evidence["version"] == 2 and evidence["count"] == 1
    assert evidence["registrations"] == [
        dict(schema_name=target[0], table_name=target[1], revision=0, ledger_schema="sample")
    ]
    assert (
        not {"actor_oid", "database_oid", "backend_pid", "transaction_id", "ledger_oid", "function_oid"}
        & evidence.keys()
    )
    assert len(_writes(native)) == 3
    native.calls.clear()
    retry = await _enroll(native)
    assert _writes(native) == []
    assert retry["registrations"] == evidence["registrations"]
    native.calls.clear()
    assert (
        await inputs.verify_native_input_candidates(native, ledger_schema="sample", candidates=(native.candidate(),))
    )["registrations"] == evidence["registrations"]
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("relkind", "v"),
        ("relpersistence", "u"),
        ("relrowsecurity", True),
        ("relforcerowsecurity", True),
        ("relispartition", True),
        ("inherited", True),
    ],
)
async def test_nonordinary_or_unisolated_candidate_is_refused_before_writes(field, value):
    native = NativeProtocol()
    native.heap[field] = value
    with pytest.raises(RuntimeError, match="heap_changed"):
        await _enroll(native)
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["ledger", "function", "counter_body", "ledger_guard", "missing_guard", "wrong_guard"]
)
async def test_existing_semantic_guards_remain_required_and_drift_is_not_repaired(fault):
    native = NativeProtocol()
    match fault:
        case "ledger":
            native.ledger["safe"] = False
        case "function":
            native.function["safe"] = False
        case "counter_body":
            native.function["prosrc"] = "BEGIN RETURN NULL; END;"
        case "ledger_guard":
            native.ledger_guards[0]["prosrc"] = "BEGIN RETURN NEW; END;"
        case _:
            await _enroll(native)
            native.candidate = lambda: inputs.NativeInputCandidate(*native.target_root, *native.target_root)
            native.guards[native.target_root] = deepcopy(native.guards[("stage", "incoming")])
            if fault == "missing_guard":
                native.guards[native.target_root] = []
            else:
                native.guards[native.target_root][0]["safe"] = False
            native.calls.clear()
    with pytest.raises(RuntimeError):
        await _enroll(native)
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["select", "insert", "cancel"])
async def test_native_privilege_or_mid_registration_failure_rolls_back(fault):
    native = NativeProtocol()
    if fault == "select":
        native.can_read_ledger = False
    elif fault == "insert":
        native.can_insert = False
    else:
        native.fail_trigger = True
    with pytest.raises((PermissionError, asyncio.CancelledError)):
        await _enroll(native)
    assert native.rows == {} and native.guards == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["empty", "duplicate", "stage_duplicate", "not_fixed", "transaction"])
async def test_candidate_scope_errors_never_mutate(scope):
    native = NativeProtocol()
    candidate = native.candidate()
    candidates = (candidate,)
    if scope == "empty":
        candidates = ()
    elif scope == "duplicate":
        candidates = (candidate, candidate)
    elif scope == "stage_duplicate":
        candidates = (candidate, replace(candidate, target_table="address_alias_v1"))
    elif scope == "not_fixed":
        candidates = (replace(candidate, target_table="unreviewed"),)
    else:
        native.active = False
    with pytest.raises(RuntimeError):
        await inputs.register_native_input_candidates(native, ledger_schema="sample", candidates=candidates)
    assert _writes(native) == []


@pytest.mark.asyncio
async def test_catalog_linkage_checks_names_revisions_and_current_guards_without_ledger_read():
    native = NativeProtocol()
    evidence = await _enroll(native)
    native.can_read_ledger = False
    native.calls.clear()
    linked = await inputs.verify_native_input_candidate_catalog(
        native, ledger_schema="sample", candidates=(native.candidate(),), registrations=evidence["registrations"]
    )
    assert linked["registrations"] == evidence["registrations"] and linked["registration_source"] == "provider-linkage"
    assert _writes(native) == []
    changed_entries = [{**evidence["registrations"][0], "table_name": "other"}]
    with pytest.raises(RuntimeError):
        await inputs.verify_native_input_candidate_catalog(
            native, ledger_schema="sample", candidates=(native.candidate(),), registrations=changed_entries
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["columns", "indexes", "constraints", "rules", "functions", "relation", "triggers"])
async def test_registration_delta_preserves_every_semantic_catalog(field):
    native = NativeProtocol()
    evidence = await _enroll(native)
    changed = deepcopy(evidence["native_inventory"])
    if field == "relation":
        changed[0][field]["table_name"] = "other"
    elif field == "triggers":
        changed[0][field][0]["enabled"] = "O"
    else:
        changed[0][field] = [{"unexpected": "changed"}]
    with pytest.raises(RuntimeError, match="catalog_changed"):
        inputs.require_native_input_registration_delta(
            evidence["before_native_inventory"], changed, evidence["registrations"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "prior_revision", "increment"])
async def test_finished_rename_advances_existing_counter_once_without_row_updates(monkeypatch, fault):
    from process import provider_directory_projection_stage as projection

    native = NativeProtocol()
    evidence = await _enroll(native)
    native.guards[native.target_root] = native.guards.pop(("stage", "incoming"))
    monkeypatch.setattr(projection, "_copy_driver", AsyncMock(return_value=native))
    session = SimpleNamespace(in_transaction=lambda: True)
    if fault == "prior_revision":
        native.rows[native.target_root]["revision"] = 5
    if fault == "increment":
        native.fail_update = True
    before = deepcopy(native.rows)
    if fault:
        with pytest.raises(RuntimeError, match="publication_revision_changed"):
            await registration.finish_local_native_input_publication(session, prepared=evidence)
        assert native.rows == before
    else:
        await registration.finish_local_native_input_publication(session, prepared=evidence)
        assert native.rows[native.target_root]["revision"] == 1
        updates = [call[1] for call in _writes(native) if call[1].startswith("UPDATE ")]
        assert updates == ['UPDATE "sample"."mrf_address" SET "value"="value" WHERE FALSE']


@pytest.mark.asyncio
async def test_initial_roster_keeps_present_and_absent_names_explicit():
    native = NativeProtocol()
    observed = await inputs.capture_current_native_input_candidates(native, ledger_schema="sample")
    assert observed["candidates"] == (inputs.NativeInputCandidate(*native.target_root, *native.target_root),)
    assert native.target_root not in observed["absent"]
    assert len(observed["absent"]) + len(observed["candidates"]) == len(
        inputs.native_input_registration_roots("sample")
    )
    assert _writes(native) == []


class ProtectedNuccProtocol(NativeProtocol):
    """Run the real named partition decoder on a native-shaped logical catalogue."""

    def __init__(self):
        from tests.test_nucc_snapshot_partition import parent_catalog

        super().__init__(target_root=("mrf", "nucc_taxonomy"))
        self.catalog = parent_catalog()
        self.current_roots = {("mrf", "nucc_taxonomy"): True}
        self.generation = dict(
            importer_id="nucc",
            local_lineage_id="11111111-1111-1111-1111-111111111111",
            local_generation=3,
            origin_lineage_id="11111111-1111-1111-1111-111111111111",
            origin_generation=3,
            published_at=datetime.datetime(2026, 10, 18, tzinfo=datetime.UTC),
            serving_schema="reference_family_archive_" + "a" * 32,
            serving_relation_names=["nucc_taxonomy"],
        )
        self.catalog_reads = 0
        self.change_after_lock = False
        self.select_only_nucc = False

    async def fetch(self, sql, *arguments):
        if sql == _NUCC_GENERATION_LOCK_SQL:
            assert arguments == ()
            self.calls.append(("fetch", sql, arguments))
            return []
        if sql == _NUCC_GENERATION_COLUMNS_SQL:
            assert arguments == ('"mrf"."reference_family_result_generation"',)
            self.calls.append(("fetch", sql, arguments))
            return [
                (column_name,)
                for column_name in ("serving_schema", "serving_relation_names")
                if column_name in self.generation
            ]
        if "FROM unnest" in sql:
            self.calls.append(("fetch", sql, arguments))
            return [
                dict(
                    namespace=schema,
                    name=table,
                    present=bool(self.current_roots.get((schema, table))),
                    kind=("p" if (schema, table) == ("mrf", "nucc_taxonomy") else "r")
                    if self.current_roots.get((schema, table))
                    else None,
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
                captured["children"][0]["relation"]["schema"] = "reference_family_archive_" + "b" * 32
            return captured
        if sql == "SHOW search_path":
            self.calls.append(("fetchval", sql, arguments))
            return '"custom", public'
        return await super().fetchval(sql, *arguments)

    async def fetchrow(self, sql, *arguments):
        if sql.startswith("SELECT importer_id, local_lineage_id"):
            assert arguments == ("nucc",)
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
async def test_initial_protected_nucc_binds_named_member_and_generation():
    native = ProtectedNuccProtocol()
    observed = await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf")
    assert observed["candidates"] == ()
    proof = observed["protected_roots"][0]["proof"]
    assert proof["physical"] == dict(
        contract="nucc-protected-parent.postgres.v1",
        parent=("mrf", "nucc_taxonomy"),
        parent_index=("mrf", "index_parent_pk"),
        child=("reference_family_archive_" + "a" * 32, "nucc_taxonomy"),
    )
    assert proof["generation"]["relations"] == [
        {"schema_name": proof["physical"]["child"][0], "relation_name": "nucc_taxonomy"}
    ]
    assert len(observed["absent"]) == 17 and native.rows == native.guards == {} and native.catalog_reads == 2
    locks = [sql for method, sql, _ in native.calls if method == "execute" and sql.startswith("LOCK")]
    assert locks == [
        'LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
        'LOCK TABLE ONLY "reference_family_archive_' + "a" * 32 + '"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
    ]
    assert [
        sql
        for method, sql, _ in native.calls
        if method == "fetch" and sql in (_NUCC_GENERATION_LOCK_SQL, _NUCC_GENERATION_COLUMNS_SQL)
    ] == [_NUCC_GENERATION_LOCK_SQL, _NUCC_GENERATION_COLUMNS_SQL]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("layout", "wrong_generation", "changed_child", "extra_child", "no_transaction"))
async def test_protected_nucc_observation_rejects_changed_names_generation_or_layout(fault):
    native = ProtectedNuccProtocol()
    if fault == "layout":
        native.catalog["children"][0]["relation"]["safe"] = False
    elif fault == "wrong_generation":
        native.generation["serving_relation_names"] = ["other"]
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
async def test_native_nucc_facade_preserves_parameter_order_and_native_transaction_lifecycle():
    from sqlalchemy import text

    from process.provider_directory_cms_native_registration import _NativeNuccSession

    native = ProtectedNuccProtocol()
    session = _NativeNuccSession(native)
    assert session._compiled(text("SELECT :second,:first,:second"), dict(first=1, second=2)) == (
        "SELECT $1::INTEGER,$2::INTEGER,$1::INTEGER",
        (2, 1),
    )
    assert await asyncio.create_task(session.scalar(text("SHOW search_path"))) == '"custom", public'
    assert await session.all(text(_NUCC_GENERATION_LOCK_SQL)) == []
    assert await session.all(
        text(_NUCC_GENERATION_COLUMNS_SQL.replace("$1::VARCHAR", ":relation")),
        relation='"mrf"."reference_family_result_generation"',
    ) == [("serving_schema",), ("serving_relation_names",)]
    assert native.calls[-1] == (
        "fetch",
        _NUCC_GENERATION_COLUMNS_SQL,
        ('"mrf"."reference_family_result_generation"',),
    )
    calls_before_close = len(native.calls)
    native.active = False
    with pytest.raises(RuntimeError, match="original_transaction_required"):
        await session.scalar(text("SHOW search_path"))
    with pytest.raises(RuntimeError, match="original_transaction_required"):
        await session.all(text(_NUCC_GENERATION_LOCK_SQL))
    assert len(native.calls) == calls_before_close
    assert not hasattr(session, "commit") and not hasattr(session, "close")


def _source_fence_physical_relations(protected):
    result_by_field = {
        f'"{schema}"."{name}"': dict(
            name=f'"{schema}"."{name}"',
            schema_name=schema,
            table_name=name,
            present=False,
            relkind=None,
            relpersistence=None,
            inherited=False,
        )
        for schema, name in inputs._relations("mrf")
    }
    result_by_field['"mrf"."nucc_taxonomy"'].update(
        present=True, relkind="p" if protected else "r", relpersistence="p", inherited=protected
    )
    return result_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", (False, True))
async def test_source_fence_names_protected_nucc_and_preserves_plain_revisions(monkeypatch, protected):
    from process import provider_directory_cms_archive as archive
    from process.provider_directory_cms_native_registration import _NativeNuccSession
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    native = ProtectedNuccProtocol()
    native.select_only_nucc = protected

    class Session(_NativeNuccSession):
        async def execute(self, statement, parameters=None):
            if str(statement) == "SELECT to_regclass(:name)":
                return SimpleNamespace(scalar_one=lambda: True)
            return await super().execute(statement, parameters)

    monkeypatch.setattr(archive, "require_source_archive_owner", AsyncMock())
    relations = _source_fence_physical_relations(protected)
    monkeypatch.setattr(inputs, "_physical_relations", AsyncMock(return_value=relations))
    monkeypatch.setattr(inputs, "_npi_revision", AsyncMock(return_value={"native": "npi"}))
    monkeypatch.setattr(
        inputs,
        "_reference_authorities",
        AsyncMock(return_value={key: {} for key in ("cms-doctors", "geo", "tiger", "mrf")}),
    )
    monkeypatch.setattr(inputs, "_geo_bindings", lambda *_args, **_kwargs: {"native": "geo"})
    revisions = AsyncMock(return_value=[{"plain_revision": 1}])
    monkeypatch.setattr(inputs, "_revision_guards", revisions)
    monkeypatch.setattr(
        inputs, "_cms_office_read_relations", AsyncMock(return_value={key: {} for key in CMS_OFFICE_READ_TABLES})
    )
    fence = await inputs.capture_native_address_input_fence(Session(native), "mrf", cutover=True)
    assert fence["relations"] == relations and fence["revisions"] == [{"plain_revision": 1}] and "database" not in fence
    assert fence["version"] == (2 if protected else 1)
    if protected:
        assert fence["nucc_protected"]["physical"]["child"] == ("reference_family_archive_" + "a" * 32, "nucc_taxonomy")
    assert native.catalog_reads == (2 if protected else 0)
    inputs.require_supported_native_address_inputs(fence)


@pytest.mark.asyncio
async def test_named_nucc_proof_excludes_only_fixed_parent_from_generic_revision_set(monkeypatch):
    from process.provider_directory_cms_native_registration import _NativeNuccSession

    native = ProtectedNuccProtocol()
    proof = await inputs.capture_protected_nucc_input(_NativeNuccSession(native), schema="mrf")
    relation_by_field = dict(
        name='"mrf"."nucc_taxonomy"',
        schema_name="mrf",
        table_name="nucc_taxonomy",
        present=True,
        relkind="p",
        relpersistence="p",
        inherited=True,
    )
    inputs._require_heaps({'"mrf"."nucc_taxonomy"': relation_by_field}, protected_nucc=proof)
    for name in ('"other"."nucc_taxonomy"', '"mrf"."other"'):
        with pytest.raises(RuntimeError, match="requires_persistent_heap"):
            inputs._require_heaps({name: relation_by_field}, protected_nucc=proof)
    statements = []

    async def execute(statement, parameters=None):
        statements.append((str(statement), parameters))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))

    monkeypatch.setattr(inputs, "_require_ledger_guards", AsyncMock())
    assert (
        await inputs._revision_guards(
            SimpleNamespace(execute=execute), "mrf", {'"mrf"."nucc_taxonomy"': relation_by_field}, protected_nucc=proof
        )
        == []
    )
    assert statements[0][0] == 'SELECT schema_name,table_name FROM "mrf"."cms_native_input_revision" WHERE FALSE'
    assert all(params["names"] == [] for _sql, params in statements if params is not None)


@pytest.mark.asyncio
async def test_initial_capture_keeps_plain_named_input_beside_protected_nucc():
    native = ProtectedNuccProtocol()
    native.current_roots[("mrf", "mrf_address")] = True
    observed = await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf")
    assert observed["candidates"] == (inputs.NativeInputCandidate("mrf", "mrf_address", "mrf", "mrf_address"),)
    assert observed["protected_roots"][0]["proof"]["physical"]["parent"] == ("mrf", "nucc_taxonomy")
    assert len(observed["absent"]) == 16 and native.rows == native.guards == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("missing", "schema", "plain_heap", "not_inherited", "registration"))
async def test_protected_nucc_lock_exclusion_requires_named_scope_and_partition_compatibility(fault):
    from process.provider_directory_cms_native_registration import _NativeNuccSession

    proof = await inputs.capture_protected_nucc_input(_NativeNuccSession(ProtectedNuccProtocol()), schema="mrf")
    relation = _source_fence_physical_relations(True)['"mrf"."nucc_taxonomy"']
    if fault == "missing":
        proof = None
    if fault == "plain_heap":
        relation["relkind"] = "r"
    if fault == "not_inherited":
        relation["inherited"] = False
    execute = AsyncMock()
    with pytest.raises(RuntimeError, match="nucc_proof_changed"):
        await inputs._lock_inputs(
            SimpleNamespace(execute=execute),
            "other" if fault == "schema" else "mrf",
            {'"mrf"."nucc_taxonomy"': relation},
            cutover=True,
            register=fault == "registration",
            protected_nucc=proof,
        )
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_nucc_read_locks_preserve_mutable_heap_and_authority_cutover_locks():
    from process.provider_directory_cms_native_registration import _NativeNuccSession

    native = ProtectedNuccProtocol()
    native.select_only_nucc = True
    proof = await inputs.capture_protected_nucc_input(_NativeNuccSession(native), schema="mrf")
    relations_by_field = _source_fence_physical_relations(True)
    relations_by_field = {key: row for key, row in relations_by_field.items() if row["present"]}
    relations_by_field['"mrf"."mrf_address"'] = dict(
        schema_name="mrf", table_name="mrf_address", present=True, relkind="r", relpersistence="p", inherited=False
    )
    statements = []

    async def execute(statement, parameters=None):
        statements.append(str(statement))
        return SimpleNamespace(scalar_one=lambda: True)

    await inputs._lock_inputs(
        SimpleNamespace(execute=execute), "mrf", relations_by_field, cutover=True, protected_nucc=proof
    )
    assert [sql for sql in statements if sql.startswith("LOCK")] == [
        'LOCK TABLE "mrf"."mrf_address" IN SHARE MODE NOWAIT',
        'LOCK TABLE "mrf"."npi_result_generation" IN SHARE MODE NOWAIT',
        'LOCK TABLE "mrf"."reference_family_result_generation" IN SHARE MODE NOWAIT',
    ]


class CapturedTigerRosterProtocol(NativeProtocol):
    """The selected Tiger proof names exactly the two already validated retained heaps."""

    def __init__(self):
        from tests.test_cms_tiger_native_inputs import _captured_catalog, _captured_proof

        super().__init__(target_root=("mrf", "mrf_address"))
        self.proof = _captured_proof()
        self.catalog = _captured_catalog(self.proof)
        self.current_roots.update({("tiger", "zcta5"): True, ("tiger", "zip_state"): True})
        self.catalog_error = None

    async def fetch(self, sql, *arguments):
        if "AS valid" in sql:
            from process.provider_directory_cms_tiger_inputs import captured_tiger_names

            self.calls.append(("fetch", sql, arguments))
            assert arguments == (captured_tiger_names(self.proof),)
            if self.catalog_error is not None:
                raise self.catalog_error
            return deepcopy(self.catalog)
        return await super().fetch(sql, *arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical", ["present", "absent"])
async def test_captured_tiger_roster_skips_only_logical_roots(canonical):
    from process.provider_directory_cms_tiger_inputs import ROOTS

    native = CapturedTigerRosterProtocol()
    if canonical == "absent":
        native.current_roots = {native.target_root: True}
    observed = await inputs.capture_current_native_input_candidates(
        native, ledger_schema="mrf", protected_tiger=native.proof
    )
    assert observed["candidates"] == (inputs.NativeInputCandidate("mrf", "mrf_address", "mrf", "mrf_address"),)
    assert observed["protected_roots"] == tuple(dict(schema="tiger", table=name, proof=native.proof) for name in ROOTS)
    assert len(observed["absent"]) == 15 and all(schema != "tiger" for schema, _ in observed["absent"])
    catalog_index = next(index for index, call in enumerate(native.calls) if "AS valid" in call[1])
    heap_index = next(
        index
        for index, call in enumerate(native.calls)
        if call[0] == "fetchrow" and call[1].startswith("SELECT c.relkind::text")
    )
    assert catalog_index < heap_index and _writes(native) == []
    assert all(
        candidate.stage_schema != native.proof["inventory"]["relations"][0]["schema_name"]
        for candidate in observed["candidates"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "invalid_layout",
        "inherited",
        "schema",
        "table",
        "missing",
        "extra",
        "digest_format",
        "shape",
        "transaction",
        "transport",
    ],
)
async def test_captured_roster_refuses_changed_names_layout_or_declaration(fault):
    native = CapturedTigerRosterProtocol()
    match fault:
        case "invalid_layout":
            native.catalog[0]["valid"] = False
        case "inherited":
            native.catalog[0]["inherited"] = True
        case "schema":
            native.catalog[0]["schema_name"] = "other"
        case "table":
            native.catalog[0]["relation_name"] = "other"
        case "missing":
            native.catalog.pop()
        case "extra":
            native.catalog.append(deepcopy(native.catalog[0]))
        case "digest_format":
            native.proof["selection_sha256"] = "invalid"
        case "shape":
            native.proof["parent_oids"] = [["zcta5", None], ["zip_state", None]]
        case "transaction":
            native.active = False
        case _:
            native.catalog_error = OSError("synthetic transport failure")
    with pytest.raises((RuntimeError, OSError)):
        await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf", protected_tiger=native.proof)
    assert _writes(native) == []
    assert not any(call[0] == "fetchrow" and call[1].startswith("SELECT c.relkind::text") for call in native.calls)


class AdoptionProtocol(NativeProtocol):
    """Named preimages for the existing NUCC adoption/retirement protocol."""


def _adoption_protocol():
    native = AdoptionProtocol(target_root=("sample", "nucc_taxonomy"))
    key = ("sample", "nucc_taxonomy")
    native.rows[key] = dict(schema_name="sample", table_name="nucc_taxonomy", revision=7, mutation_xid=None)
    native.guards[key] = [
        dict(
            tgname="cms_native_input_mutation",
            tgtype=60,
            enabled="A",
            function_schema="sample",
            function_name="cms_native_input_advance",
            safe=True,
        )
    ]
    candidate = inputs.NativeInputCandidate("sample", "nucc_taxonomy", "sample", "nucc_taxonomy")
    return native, key, candidate


@pytest.mark.asyncio
async def test_native_adoption_captures_named_nullable_revision_without_mutation():
    native, key, candidate = _adoption_protocol()
    before = deepcopy(native.rows)
    evidence = await registration.capture_native_input_adoption(native, ledger_schema="sample", candidate=candidate)
    assert set(evidence) == {"schema_name", "table_name", "revision", "mutation_xid", "function_sha256"}
    assert evidence["revision"] == 7 and evidence["mutation_xid"] is None
    assert (evidence["schema_name"], evidence["table_name"]) == key
    assert native.rows == before and _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ("target", "revision", "mutation", "function", "ledger", "duplicate", "missing-row", "missing-guard")
)
async def test_native_adoption_refuses_changed_named_revision_or_guard_semantics_before_writes(fault):
    native, key, candidate = _adoption_protocol()
    match fault:
        case "target":
            native.rows[key]["schema_name"] = "other"
        case "revision":
            native.rows[key]["revision"] = True
        case "mutation":
            native.rows[key]["mutation_xid"] = "01"
        case "function":
            native.function["prosrc"] += "changed"
        case "ledger":
            native.ledger_guards[0]["enabled"] = "D"
        case "duplicate":
            native.guards[key] *= 2
        case "missing-row":
            native.rows.clear()
        case _:
            native.guards.clear()
    with pytest.raises(RuntimeError):
        await registration.capture_native_input_adoption(native, ledger_schema="sample", candidate=candidate)
    assert _writes(native) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", (False, True))
@pytest.mark.parametrize("fault", (None, "revision", "mutation", "guard", "function"))
async def test_retired_adoption_preserves_raw_editions_and_requires_unchanged_named_revision(fault, legacy):
    native, key, candidate = _adoption_protocol()
    evidence = await registration.capture_native_input_adoption(native, ledger_schema="sample", candidate=candidate)
    if legacy:
        evidence.update(
            {
                field: "opaque original " + field
                for field in registration._LEGACY_ADOPTION_FIELDS - registration._ADOPTION_FIELDS
            }
        )
        evidence["function_sha256"] = "opaque original semantic token"
    saved = deepcopy(evidence)
    native.guards.clear()
    if fault == "revision":
        native.rows[key]["revision"] += 1
    elif fault == "mutation":
        native.rows[key]["mutation_xid"] = "102"
    elif fault == "guard":
        native.guards[key] = [dict(tgname="cms_native_input_mutation", safe=True)]
    elif fault == "function":
        native.function["prosrc"] += "changed"
    before = deepcopy(native.rows)
    if fault:
        with pytest.raises(RuntimeError):
            await registration.verify_retired_native_input_adoption(native, evidence=evidence)
    else:
        await registration.verify_retired_native_input_adoption(native, evidence=evidence)
    assert evidence == saved and native.rows == before and _writes(native) == []


@pytest.mark.asyncio
async def test_unregistered_named_legacy_heap_remains_eligible_without_invented_revision():
    native, key, candidate = _adoption_protocol()
    native.rows.clear()
    native.guards.clear()
    assert await registration.capture_native_input_adoption(native, ledger_schema="sample", candidate=candidate) is None
    assert _writes(native) == []


@pytest.mark.parametrize("visible", [False, True])
def test_registration_delta_accepts_guard_printed_without_search_path_schema(visible):
    candidate = registration.NativeInputCandidate("mrf", "address_archive_v2", "mrf", "address_archive_v2")
    relation_by_field = dict(schema_name="mrf", table_name="address_archive_v2")
    before_by_field = dict(
        relation=relation_by_field, columns=[], indexes=[], constraints=[], triggers=None, rules=[], functions=None
    )
    guard_by_field = registration._expected_native_guard(candidate, "sample")
    if visible:
        # The ledger schema on the search path makes pg_get_triggerdef print the function unqualified.
        guard_by_field = {
            **guard_by_field,
            "definition": guard_by_field["definition"].replace(
                "sample.cms_native_input_advance", "cms_native_input_advance"
            ),
        }
    function_by_field = dict(
        schema_name="sample", function_name="cms_native_input_advance", definition="AS $$" + _ADVANCE + "$$"
    )
    after_by_field = {**before_by_field, "triggers": [guard_by_field], "functions": [function_by_field]}
    registrations = [dict(schema_name="mrf", table_name="address_archive_v2", ledger_schema="sample")]
    registration.require_native_input_registration_delta([before_by_field], [after_by_field], registrations)
    foreign_by_field = {**after_by_field, "functions": [{**function_by_field, "schema_name": "other"}]}
    with pytest.raises(RuntimeError, match="catalog_changed"):
        registration.require_native_input_registration_delta([before_by_field], [foreign_by_field], registrations)
