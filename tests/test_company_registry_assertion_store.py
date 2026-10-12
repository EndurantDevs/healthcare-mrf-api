# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected draft flow with fake SQL results; native transaction proof is separate."""

import asyncio
import copy
import importlib.util
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateColumn, CreateTable

from db.models.company_registry import CompanyRegistry
from db.models.company_registry_assertions import CompanyRegistryIdentifierAssertion, CompanyRegistryRoleAssertion
from process import registry_approval_preview as preview
from process import registry_approval_store as approval
from process import registry_management_permissions as permissions
from process import registry_manual_undo as undo
from process import registry_record_store as store
from process.registry_manual_undo import RegistryManualUndoCommand, _prepare_correction
from tests.test_company_registry_assertion_values import COMPANY, _command, _group, _identifier, _role

ACTOR = store.RegistryActor("platform_admin", UUID(int=100), "example-client")


def _edit(*, assertions=True, **changes):
    fields_by_name = {"display_name": "Example Company", "roles": ["employer"], "aliases": []}
    if assertions:
        fields_by_name["assertions"] = _command(identifier_assertions=[_identifier()])
    return replace(
        store.RegistryRecordCommand(
            "company", UUID(COMPANY), "create", 0, fields_by_name, "Reviewed company values", "example-key"
        ),
        **changes,
    )


class Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def one_or_none(self):
        assert len(self.rows) <= 1
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class Session:
    new = dirty = deleted = ()

    def __init__(self):
        self.active = True
        self.head = None
        self.history = []
        self.assertions = {}
        self.events = []
        self.valid = True
        self.fail_history = False
        self.draft = 0

    def in_transaction(self):
        return self.active

    @asynccontextmanager
    async def begin_nested(self):
        previous = copy.deepcopy((self.head, self.history, self.assertions, self.draft))
        try:
            yield
        except BaseException:
            self.head, self.history, self.assertions, self.draft = previous
            self.events.append("rollback")
            raise

    async def scalar(self, statement, parameters=None):
        sql = str(statement.compile(dialect=postgresql.dialect()))
        if isinstance(statement, store.text("").__class__):
            self.events.append("validate-set")
            assert parameters["company_id"] == UUID(COMPANY)
            return self.valid
        if sql.startswith("UPDATE"):
            self.draft += 1
            return self.draft
        if "record_json" in sql:
            return copy.deepcopy(self.history[-1]["record_json"]) if self.history else None
        self.events.append("control-lock")
        assert "FOR UPDATE" in sql
        return 1

    async def execute(self, statement, rows=None):
        compiled = statement.compile(dialect=postgresql.dialect())
        params = compiled.params
        table = getattr(statement, "table", None)
        if table is None:
            if "undo_target" in str(compiled):
                return self._undo_history_result(params)
            self.events.append("replay")
            matching_rows = [row for row in self.history if row["idempotency_key"] == params["idempotency_key_1"]]
            return Result(matching_rows)
        name = table.name
        if name == "company_registry":
            self.events.append("head")
            if self.head is not None and self.head["revision"] != params.get("revision_1", 0):
                return Result([])
            self._write_head(params)
            return Result([copy.deepcopy(self.head)])
        if name == "registry_record_history":
            if self.fail_history:
                raise RuntimeError("synthetic-history-failure")
            self.events.append("history")
            self.history.append(copy.deepcopy(params))
        else:
            self.events.append("insert-set")
            self.assertions.setdefault(name, []).extend(copy.deepcopy(rows))
        return Result([])

    def _undo_history_result(self, params):
        target_history = next(entry for entry in self.history if entry["revision"] == params["revision_1"])
        expected_history = next(entry for entry in self.history if entry["revision"] == params["revision_2"])
        snapshot = expected_history["record_json"]
        predecessor = snapshot.get("correction_predecessors", {}).get("company_revision")
        return Result(
            [
                {
                    "current_revision": self.head["revision"],
                    "record_json": copy.deepcopy(target_history["record_json"]),
                    "custom_revision": target_history["custom_revision"],
                    "request_sha256": target_history["request_sha256"],
                    "expected_custom_revision": expected_history["custom_revision"],
                    "expected_request_sha256": expected_history["request_sha256"],
                    "predecessor_revision": str(predecessor) if predecessor is not None else None,
                    "predecessor_revision_type": "number" if type(predecessor) is int else "string",
                    "same_actor": target_history["actor_json"] == expected_history["actor_json"],
                    "target_reason": target_history["reason"],
                    "target_idempotency_key": target_history["idempotency_key"],
                    "expected_identity": snapshot["company_id"],
                    "expected_identity_type": "string",
                    "expected_record_revision": str(snapshot["revision"]),
                    "expected_revision_type": "number",
                }
            ]
        )

    def _write_head(self, params):
        self.head = (
            self.head
            or {
                "company_id": UUID(COMPANY),
                "archived": False,
                "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        ) | params
        self.head.pop("revision_1", None)
        self.head.pop("company_id_1", None)


async def _apply(session, command):
    return await store.apply_registry_record_command(session, command, ACTOR, schema="example_registry")


@pytest.mark.asyncio
async def test_whole_assertion_command_uses_existing_lock_and_history():
    session = Session()
    result = await _apply(session, _edit())
    assert result["revision"] == result["custom_revision"] == 1
    assert result["record"]["identifier_assertions"] == [_identifier()]
    assert session.events == ["control-lock", "replay", "validate-set", "head", "insert-set", "insert-set", "history"]
    assert len(session.assertions["company_registry_role_assertion"]) == 1
    assert len(session.assertions["company_registry_identifier_assertion"]) == 1
    assert "assertions" not in session.head
    assert session.history[0]["actor_json"]["user_id"] == str(ACTOR.user_id)


@pytest.mark.asyncio
async def test_manual_creation_needs_no_identifier_period_or_source_query():
    session = Session()
    result = await _apply(session, _edit(assertions=False))
    assert result["record"]["role_assertions"] == result["record"]["identifier_assertions"] == []
    assert "validate-set" not in session.events
    assert not session.assertions


@pytest.mark.asyncio
async def test_legacy_correction_archive_restore_preserve_revision_assertions():
    session = Session()
    initial = await _apply(session, _edit())
    for revision, operation in enumerate(("correct", "archive", "restore"), 1):
        fields = _edit(assertions=False).fields if operation == "correct" else {}
        result = await _apply(
            session, _edit(operation=operation, expected_revision=revision, fields=fields, idempotency_key=operation)
        )
        assert result["record"]["role_assertions"] == initial["record"]["role_assertions"]
        assert result["record"]["identifier_assertions"] == initial["record"]["identifier_assertions"]
    assert [row["company_revision"] for row in session.assertions["company_registry_role_assertion"]] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_exact_retained_replay_precedes_new_validation():
    session = Session()
    command = _edit()
    first = await _apply(session, command)
    session.valid = False
    assert await _apply(session, command) == first
    assert session.events[-2:] == ["control-lock", "replay"]
    with pytest.raises(store.RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(session, replace(command, reason="changed"))
    assert len(session.history) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["validation", "history"])
async def test_failure_rolls_back_whole_draft_and_assertion_sets(failure):
    session = Session()
    session.valid = failure != "validation"
    session.fail_history = failure == "history"
    with pytest.raises((store.RegistryRecordConflict, RuntimeError)):
        await _apply(session, _edit())
    assert session.head is None and session.history == [] and session.assertions == {} and session.draft == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"company_id": str(UUID(int=23))}, {"expected_revision": 1}])
async def test_context_mismatch_refuses_before_transaction(change):
    session = Session()
    command = _edit()
    command.fields["assertions"].update(change)
    with pytest.raises(ValueError, match="context_invalid"):
        await _apply(session, command)
    assert session.events == []


@pytest.mark.asyncio
async def test_invalid_actor_and_missing_caller_transaction_never_query():
    session = Session()
    with pytest.raises(ValueError, match="actor_invalid"):
        await store.apply_registry_record_command(session, _edit(), object())
    session.active = False
    with pytest.raises(ValueError, match="caller_transaction"):
        await _apply(session, _edit())
    assert session.events == []


@pytest.mark.asyncio
async def test_company_read_batches_three_families_for_multiple_revisions():
    records = [{"company_id": COMPANY, "revision": 2}, {"company_id": str(UUID(int=5)), "revision": 3}]
    row = store._company_assertion_row(_identifier(), COMPANY, 2)
    queries = []

    class Reader:
        async def execute(self, statement):
            sql = str(statement.compile(dialect=postgresql.dialect()))
            queries.append(sql)
            assert "company_id" in sql and "company_revision" in sql and " IN " in sql
            return Result([row] if "company_registry_identifier_assertion" in sql else [])

    await store._attach_company_assertions(Reader(), records, "example_registry")
    assert len(queries) == 3
    assert records[0]["identifier_assertions"] == [_identifier()]
    assert records[1]["identifier_assertions"] == []


def test_source_and_conflict_validation_is_one_complete_set_query():
    sql = store._COMPANY_ASSERTION_TARGETS_SQL
    assert "registry_source_snapshot" in sql and "registry_source_observation" in sql
    assert "registry_identifier_observation" in sql and "registry_identifier_binding" in sql
    assert "resolution_status<>'conflicting'" in sql and "observation.entity_id IS NULL" in sql
    assert "company.revision)=(retained.company_id,retained.company_revision)" in sql
    assert "NOT company.archived" in sql and "'infinity'::date" in sql
    assert "control.approved_revision=approved.approved_revision" in sql
    assert "approved.record_revision=retained.company_revision" in sql
    assert "retained.identifier_scope" in sql and "retained.valid_from<=" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("valid", [True, False])
async def test_approval_compares_exact_assertion_revision_without_legacy_bypass(valid):
    class Connection:
        async def fetchval(self, sql, revision):
            assert revision == 5
            assert "assertion.company_revision=head.revision" in sql
            assert "ORDER BY assertion.assertion_id" in sql
            assert (
                "jsonb_build_object('role_assertions','[]'::jsonb,'identifier_assertions','[]'::jsonb,"
                "'group_assertions','[]'::jsonb,'display_name_origin','unknown')||(history.record_json-'correction_predecessors')"
                in "".join(sql.split())
            )
            assert "heads.record_json-'created_at'" in sql
            return valid

    if valid:
        await approval._validate_selected_history(Connection(), '"example_registry"', '"selected"', 5)
    else:
        with pytest.raises(approval.RegistryApprovalConflict):
            await approval._validate_selected_history(Connection(), '"example_registry"', '"selected"', 5)


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261009020000_company_registry_assertions.py"
    spec = importlib.util.spec_from_file_location("company_assertion_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_additive_migration_matches_metadata_and_native_constraints():
    migration = _migration()
    ddl = migration._ddl("example_registry")
    for model, statement in zip((CompanyRegistryRoleAssertion, CompanyRegistryIdentifierAssertion), ddl):
        normalized = "".join(statement.split())
        assert model.__tablename__ in statement
        for column in model.__table__.columns:
            expected = str(CreateColumn(column).compile(dialect=postgresql.dialect()))
            assert "".join(expected.split()) in normalized
        for constraint in model.__table__.constraints:
            if isinstance(constraint, CheckConstraint):
                expected = f"CONSTRAINT{constraint.name}CHECK({constraint.sqltext})"
                assert "".join(expected.split()) in normalized
            if isinstance(constraint, UniqueConstraint):
                expected = f"CONSTRAINT{constraint.name}UNIQUE({','.join(constraint.columns.keys())})"
                assert expected in normalized
    assert len(ddl) == 4 and all("REVOKE ALL" in sql for sql in ddl[2:])
    assert not any(word in " ".join(ddl).upper() for word in ("CREATE FUNCTION", "CREATE TRIGGER", "REFERENCES"))
    with pytest.raises(RuntimeError):
        migration.downgrade()


@pytest.mark.parametrize("schema", ['bad"name', "a.b", "a" * 64, "x\n"])
def test_migration_namespace_refuses_before_sql(schema):
    with pytest.raises(ValueError):
        _migration()._ddl(schema)


def test_new_assertion_privileges_are_select_and_insert_only():
    for model in (CompanyRegistryRoleAssertion, CompanyRegistryIdentifierAssertion):
        name = model.__tablename__
        assert name in permissions._TABLES and name not in permissions._UPDATE_COLUMNS
        assert set(permissions._INSERT_COLUMNS[name]) == set(model.__table__.columns.keys()) - {"created_at"}
        assert model in permissions._MODELS and model not in permissions._HEAD_MODELS


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_manual_undo_restores_target_assertion_families_and_evidence(legacy):
    session = Session()
    original = _edit(assertions=not legacy)
    first = await _apply(session, original)
    target_history = copy.deepcopy(session.history[0])
    if legacy:
        target_history["record_json"].pop("role_assertions")
        target_history["record_json"].pop("identifier_assertions")
    later = _command(
        expected_revision=1,
        role_assertions=[
            _role(2, role="network_operator", provenance={**_role()["provenance"], "evidence_ref": "later-role"})
        ],
        identifier_assertions=[
            _identifier(
                21,
                identifier_value="54321",
                provenance={**_identifier()["provenance"], "evidence_ref": "later-identifier"},
            )
        ],
    )
    await _apply(
        session,
        _edit(
            operation="correct",
            expected_revision=1,
            fields={"display_name": "Later Company", "aliases": [], "roles": ["network_operator"], "assertions": later},
            idempotency_key="later",
        ),
    )
    undo = RegistryManualUndoCommand("company", UUID(COMPANY), 2, 1, "Reviewed target_history", "undo")
    preparation = _prepare_correction(undo, target_history | {"current_revision": 2}, "company_id")
    assert preparation.command.fields["assertions"]["expected_revision"] == 2
    if not legacy:
        session.valid = False
        before = copy.deepcopy((session.head, session.history, session.assertions))
        with pytest.raises(store.RegistryRecordConflict, match="assertion_target_conflict"):
            await _apply(session, preparation.command)
        assert (session.head, session.history, session.assertions) == before
        session.valid = True
    restored = await _apply(session, preparation.command)
    for key in ("display_name", "roles", "aliases", "role_assertions", "identifier_assertions"):
        assert restored["record"][key] == first["record"][key]
    assert session.history[0]["record_json"] == first["record"]


def _group_edit(**changes):
    command = _edit(**changes)
    command.fields["assertions"]["group_assertions"] = [_group()]
    return command


@pytest.mark.asyncio
async def test_groups_use_one_batch_and_preserve_exact_replay(monkeypatch):
    import ptg2_address_canon

    genuine = ptg2_address_canon.validate_company_group_assertions
    calls = []

    def observed(encoded):
        calls.append(encoded)
        return genuine(encoded)

    monkeypatch.setattr(ptg2_address_canon, "validate_company_group_assertions", observed)
    session = Session()
    command = _group_edit()
    created = await _apply(session, command)
    assert created["record"]["group_assertions"] == [_group()]
    assert len(calls) == 1
    assert session.events.count("validate-set") == 1 and session.events.count("insert-set") == 3
    stored = session.assertions["company_registry_group_assertion"][0]
    assert stored["company_id"] == UUID(COMPANY) and stored["group_id"] == UUID(_group()["group_id"])
    assert store._company_assertion_document(stored) == _group()
    correction = _edit(operation="correct", expected_revision=1, idempotency_key="omitted-group")
    correction.fields["assertions"]["expected_revision"] = 1
    later = await _apply(session, correction)
    assert later["record"]["group_assertions"] == created["record"]["group_assertions"]
    assert len(calls) == 2
    session.valid = False
    assert await _apply(session, command) == created
    assert len(calls) == 3  # Submitted DTO validation remains before exact replay.
    assert session.events[-2:] == ["control-lock", "replay"]


@pytest.mark.asyncio
async def test_group_omission_clear_archive_and_restore_preserve_history():
    session = Session()
    command = _group_edit()
    created = await _apply(session, command)
    for revision, operation in enumerate(("correct", "archive", "restore"), 1):
        fields = _edit(assertions=False).fields if operation == "correct" else {}
        result = await _apply(
            session, _edit(operation=operation, expected_revision=revision, fields=fields, idempotency_key=operation)
        )
        assert result["record"]["group_assertions"] == created["record"]["group_assertions"]
    clear = _group_edit(operation="correct", expected_revision=4, idempotency_key="clear-groups")
    clear.fields["assertions"].update(expected_revision=4, group_assertions=[])
    assert (await _apply(session, clear))["record"]["group_assertions"] == []
    assert session.history[-1]["record_json"]["correction_predecessors"]["group_assertions"] == [
        _group()["assertion_id"]
    ]
    assert session.history[0]["record_json"]["group_assertions"] == [_group()]
    omitted = copy.deepcopy(clear)
    del omitted.fields["assertions"]["group_assertions"]
    with pytest.raises(store.RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(session, omitted)
    with pytest.raises(store.RegistryRecordConflict, match="revision_conflict"):
        await _apply(session, replace(clear, idempotency_key="stale-clear"))
    assert len(session.history) == 5
    assert (await _apply(Session(), _edit(assertions=False)))["record"]["group_assertions"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["role_assertions", "identifier_assertions"])
async def test_preserved_groups_recheck_cross_family_ids(family):
    session = Session()
    await _apply(session, _group_edit())
    before = copy.deepcopy((session.head, session.history, session.assertions, session.draft))
    correction = _edit(operation="correct", expected_revision=1, idempotency_key="collision")
    correction.fields["assertions"]["expected_revision"] = 1
    correction.fields["assertions"][family][0]["assertion_id"] = _group()["assertion_id"]
    with pytest.raises(ValueError):
        await _apply(session, correction)
    assert (session.head, session.history, session.assertions, session.draft) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["validation", "history", "group_insert", "cancel_group_insert"])
async def test_group_failures_roll_back_every_family(failure):
    class FailingSession(Session):
        async def execute(self, statement, rows=None):
            table = getattr(statement, "table", None)
            if table is not None and table.name == "company_registry_group_assertion":
                if failure == "group_insert":
                    raise RuntimeError("synthetic-group-insert-failure")
                if failure == "cancel_group_insert":
                    raise asyncio.CancelledError
            return await super().execute(statement, rows)

    session = FailingSession()
    session.valid = failure != "validation"
    session.fail_history = failure == "history"
    with pytest.raises((store.RegistryRecordConflict, RuntimeError, asyncio.CancelledError)):
        await _apply(session, _group_edit())
    assert session.head is None and session.history == [] and session.assertions == {} and session.draft == 0


@pytest.mark.asyncio
async def test_group_reads_are_bounded_and_keep_nullable_identity():
    document = _group(
        group_id=None,
        valid_from=None,
        provenance={
            "kind": "source_reference",
            "evidence_ref": "source-reference",
            "snapshot_id": str(UUID(int=99)),
            "source_record_key": "record-99",
        },
    )
    group_row = store._company_assertion_row(document, COMPANY, 2)
    assert group_row["group_id"] is group_row["valid_from"] is group_row["valid_to"] is None
    queries = []

    class Reader:
        async def execute(self, statement):
            queries.append(str(statement.compile(dialect=postgresql.dialect())))
            return Result([group_row] if "company_registry_group_assertion" in queries[-1] else [])

    company_records = [{"company_id": COMPANY, "revision": 2}]
    await store._attach_company_assertions(Reader(), company_records, "example_registry")
    assert len(queries) == 3 and " LIMIT " in queries[-1]
    assert company_records[0]["group_assertions"] == [document]

    class OversizedReader(Reader):
        async def execute(self, statement):
            result = await super().execute(statement)
            return Result([group_row] * 129) if result.rows else result

    with pytest.raises(store.RegistryAddressUnavailable):
        await store._attach_company_assertions(OversizedReader(), company_records, "example_registry")


def test_group_targets_use_exact_source_edge_and_actual_kind():
    sql = store._COMPANY_ASSERTION_TARGETS_SQL
    for predicate in (
        "edge.company_id=CAST(:company_id AS uuid)",
        "edge.group_id IS NOT DISTINCT FROM",
        "edge.valid_from IS NOT DISTINCT FROM",
        "edge.valid_to IS NOT DISTINCT FROM",
        "edge.relationship_kind=",
        "edge.resolution_status IN ('resolved','unresolved')",
        "target.group_kind=",
        "NOT target.archived",
        "observation.status IN ('accepted','unresolved')",
    ):
        assert predicate in sql
    current = store._company_record_json_sql('"example_registry"')
    assert "company_registry_group_assertion" in current and "LIMIT 129" in current


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [None, {}, [_group()] * 129])
async def test_malformed_retained_groups_refuse_before_any_write(invalid):
    session = Session()
    await _apply(session, _group_edit())
    session.history[0]["record_json"]["group_assertions"] = invalid
    before = copy.deepcopy((session.head, session.history, session.assertions, session.draft))
    correction = _edit(operation="correct", expected_revision=1, idempotency_key="malformed-history")
    correction.fields["assertions"]["expected_revision"] = 1
    with pytest.raises(store.RegistryRecordConflict, match="group_assertions_invalid"):
        await _apply(session, correction)
    assert (session.head, session.history, session.assertions, session.draft) == before


def test_company_name_origin_is_bounded_native_metadata_with_unknown_legacy_default():
    table = CompanyRegistry.__table__
    column = table.c.display_name_origin
    assert not column.nullable and column.type.length == 32
    assert str(column.server_default.arg) == "unknown"
    checks_by_name = {c.name: str(c.sqltext) for c in table.constraints if isinstance(c, CheckConstraint)}
    assert checks_by_name["registry_company_name_origin"] == (
        "display_name_origin IN ('manual','source_company','source_issuer','identifier','unknown')"
    )
    sql = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "display_name_origin VARCHAR(32) DEFAULT 'unknown' NOT NULL" in sql
    assert "CONSTRAINT registry_company_name_origin CHECK" in sql


@pytest.mark.asyncio
async def test_company_manual_creation_retains_origin_in_history_and_exact_replay():
    session = Session()
    command = _edit(assertions=False)
    created = await _apply(session, command)
    assert created["record"]["display_name_origin"] == "manual"
    assert session.history[0]["record_json"]["display_name_origin"] == "manual"
    assert await _apply(session, command) == created
    assert len(session.history) == session.draft == 1
    assert session.events[-2:] == ["control-lock", "replay"]


class HeadStatementSession:
    """Capture real PostgreSQL statements without claiming native SQL execution."""

    def __init__(self, origin):
        self.origin = origin
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return Result([{"company_id": UUID(COMPANY), "revision": 2, "display_name_origin": self.origin}])


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["manual", "source_company", "source_issuer", "identifier", "unknown"])
@pytest.mark.parametrize("name", [" Example Company ", "New company name"])
async def test_company_name_correction_uses_same_cas_row_for_origin(origin, name):
    command = _edit(
        assertions=False,
        operation="correct",
        expected_revision=1,
        fields={"display_name": name, "roles": ["network_operator"], "aliases": ["Alias"]},
    )
    session = HeadStatementSession(origin)
    table, identity = store._record_table("company", "example_registry")
    result = await store._write_record_head(
        session,
        command,
        table,
        identity,
        command.record_id,
        store._validated_command(command),
    )
    compiled = session.statements[0].compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert len(session.statements) == 1 and result["display_name_origin"] == origin
    assert "display_name_origin=CASE WHEN" in sql and "display_name IS DISTINCT FROM" in sql
    assert "ELSE example_registry.company_registry.display_name_origin END" in sql
    assert "company_id =" in sql and "revision =" in sql
    assert compiled.params["display_name"] == name.strip()
    assert compiled.params["company_id_1"] == command.record_id
    assert compiled.params["revision_1"] == command.expected_revision
    assert compiled.params["revision"] == command.expected_revision + 1
    assert list(compiled.params.values()).count("manual") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["archive", "restore"])
async def test_company_archival_does_not_assign_name_origin(operation):
    command = _edit(assertions=False, operation=operation, expected_revision=1, fields={})
    session = HeadStatementSession("source_issuer")
    table, identity = store._record_table("company", "example_registry")
    await store._write_record_head(session, command, table, identity, command.record_id, {})
    sql = str(session.statements[0].compile(dialect=postgresql.dialect())).split(" WHERE ")[0]
    assert " SET " in sql and "display_name_origin=" not in sql


@pytest.mark.asyncio
async def test_correction_retains_predecessor_families_without_changing_wire_replay():
    session = Session()
    original = _edit()
    first = await _apply(session, original)
    retained = copy.deepcopy(session.history[0])
    correction = _edit(operation="correct", expected_revision=1, idempotency_key="replace-assertions")
    correction.fields["assertions"].update(expected_revision=1)
    correction.fields["assertions"]["role_assertions"][0]["assertion_id"] = str(UUID(int=201))
    correction.fields["assertions"]["identifier_assertions"][0]["assertion_id"] = str(UUID(int=202))
    correction_result = await _apply(session, correction)
    predecessors = session.history[1]["record_json"]["correction_predecessors"]
    assert predecessors == {
        "company_revision": 1,
        "role_assertions": [first["record"]["role_assertions"][0]["assertion_id"]],
        "identifier_assertions": [first["record"]["identifier_assertions"][0]["assertion_id"]],
        "group_assertions": [],
    }
    assert session.history[0] == retained
    assert session.history[1]["reason"] == correction.reason
    assert "correction_predecessors" not in correction_result["record"]
    assert await _apply(session, correction) == correction_result
    assert await _apply(session, original) == first
    assert len(session.history) == 2
    assert session.history[0] == retained


@pytest.mark.asyncio
async def test_identifier_free_correction_records_empty_predecessor_families():
    session = Session()
    await _apply(session, _edit(assertions=False))
    correction = _edit(assertions=False, operation="correct", expected_revision=1, idempotency_key="name-only")
    await _apply(session, correction)
    assert session.history[1]["record_json"]["correction_predecessors"] == {
        "company_revision": 1,
        "role_assertions": [],
        "identifier_assertions": [],
        "group_assertions": [],
    }
    assert "correction_predecessors" not in session.history[0]["record_json"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "cas", "history"])
async def test_materialized_predecessor_retention_is_atomic(monkeypatch, failure):
    class MaterializedSession(Session):
        async def execute(self, statement, rows=None):
            if getattr(getattr(statement, "table", None), "name", None) == "registry_record_history":
                params = statement.compile(dialect=postgresql.dialect()).params
                if failure == "history" and params["revision"] == 2:
                    assert self.history[0]["revision"] == 1
                    raise RuntimeError("synthetic-correction-history-failure")
            return await super().execute(statement, rows)

    session = MaterializedSession()
    original = await _apply(session, _edit())
    snapshot = copy.deepcopy(original["record"])
    session.history.clear()
    session.draft = 0
    if failure == "cas":
        session.head["revision"] = 2

    async def materialized_read(*args, **kwargs):
        return copy.deepcopy(snapshot)

    monkeypatch.setattr(store, "get_registry_record", materialized_read)
    correction = _edit(
        assertions=False,
        operation="correct",
        expected_revision=1,
        fields={"display_name": "Corrected Company", "roles": ["network_operator"], "aliases": ["Corrected"]},
        idempotency_key="materialized",
    )
    before = copy.deepcopy((session.head, session.assertions))
    if failure is not None:
        with pytest.raises((store.RegistryRecordConflict, RuntimeError)):
            await _apply(session, correction)
        assert (session.head, session.assertions) == before
        assert session.history == [] and session.draft == 0
        return
    correction_result = await _apply(session, correction)
    baseline, corrected = session.history
    assert baseline["record_json"] == snapshot
    assert baseline["actor_json"] == corrected["actor_json"]
    assert baseline["reason"] == "Preserved materialized company before manual correction"
    assert baseline["idempotency_key"] != corrected["idempotency_key"]
    assert baseline["request_sha256"] != corrected["request_sha256"]
    assert baseline["custom_revision"] == corrected["custom_revision"] == 1
    assert await _apply(session, correction) == correction_result
    assert len(session.history) == 2
    await _verify_materialized_undo_replay(session, snapshot)


async def _verify_materialized_undo_replay(session, snapshot):
    retained = copy.deepcopy(session.history)
    command = RegistryManualUndoCommand("company", UUID(COMPANY), 2, 1, "Restore materialized values", "first-undo")
    preparation = await undo.prepare_registry_manual_undo(session, command, ACTOR, schema="example_registry")
    restored = await _apply(session, preparation.command)
    assert restored["revision"] == 3
    for field in ("display_name", "roles", "aliases", "role_assertions", "identifier_assertions", "group_assertions"):
        assert restored["record"][field] == snapshot[field]
    assert session.history[:2] == retained
    assert session.history[2]["actor_json"] == retained[0]["actor_json"]
    assert session.history[2]["reason"] == preparation.command.reason
    replay = await undo.prepare_registry_manual_undo(session, command, ACTOR, schema="example_registry")
    assert replay.command == preparation.command
    assert await _apply(session, replay.command) == restored
    assert session.history[:2] == retained and len(session.history) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "history_index,field_path,forged_value",
    [
        (1, ("record_json", "correction_predecessors"), {}),
        (1, ("record_json", "correction_predecessors", "company_revision"), 2),
        (1, ("record_json", "correction_predecessors", "company_revision"), "1"),
        (0, ("request_sha256",), "0" * 64),
        (0, ("reason",), "Unrelated history"),
        (0, ("idempotency_key",), "unrelated-key"),
        (0, ("actor_json", "user_id"), str(UUID(int=101))),
        (0, ("record_json", "company_id"), str(UUID(int=102))),
        (0, ("record_json", "revision"), 2),
        (1, ("request_sha256",), "invalid"),
        (0, ("custom_revision",), 2),
    ],
    ids=[
        "missing-predecessor",
        "wrong-predecessor",
        "string-predecessor",
        "digest",
        "reason",
        "key",
        "actor",
        "entity",
        "revision",
        "correction-digest",
        "reversed-order",
    ],
)
async def test_materialized_undo_refuses_forged_predecessor(monkeypatch, history_index, field_path, forged_value):
    session = Session()
    original = await _apply(session, _edit())
    snapshot = copy.deepcopy(original["record"])
    session.history.clear()
    session.draft = 0

    async def materialized_read(*args, **kwargs):
        return copy.deepcopy(snapshot)

    monkeypatch.setattr(store, "get_registry_record", materialized_read)
    await _apply(
        session, _edit(assertions=False, operation="correct", expected_revision=1, idempotency_key="materialized")
    )
    history_entry = session.history[history_index]
    for field in field_path[:-1]:
        history_entry = history_entry[field]
    history_entry[field_path[-1]] = forged_value
    before = copy.deepcopy((session.head, session.history, session.assertions, session.draft))
    command = RegistryManualUndoCommand("company", UUID(COMPANY), 2, 1, "Restore materialized values", "denied-undo")
    with pytest.raises(store.RegistryRecordConflict, match="undo_history_conflict"):
        await undo.prepare_registry_manual_undo(session, command, ACTOR, schema="example_registry")
    assert (session.head, session.history, session.assertions, session.draft) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value,error",
    [
        ("company_id", None, "predecessor_invalid"),
        ("company_id", UUID(COMPANY), "predecessor_invalid"),
        ("company_id", str(UUID(int=23)), "predecessor_invalid"),
        ("revision", None, "revision_conflict"),
        ("revision", True, "revision_conflict"),
        ("revision", "1", "revision_conflict"),
        ("revision", 2, "revision_conflict"),
    ],
)
async def test_correction_refuses_retained_identity_or_revision_before_any_write(field, value, error):
    session = Session()
    await _apply(session, _edit())
    session.history[0]["record_json"][field] = value
    before = copy.deepcopy((session.head, session.history, session.assertions, session.draft))
    correction = _edit(operation="correct", expected_revision=1, idempotency_key="invalid-predecessor-identity")
    correction.fields["assertions"]["expected_revision"] = 1
    with pytest.raises(store.RegistryRecordConflict, match=error):
        await _apply(session, correction)
    assert (session.head, session.history, session.assertions, session.draft) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "family,entries",
    [
        ("role_assertions", [None]),
        ("role_assertions", [{"assertion_id": "invalid"}]),
        ("identifier_assertions", [{"assertion_id": str(UUID(int=0))}]),
        ("identifier_assertions", [{"assertion_id": str(UUID(int=23))}] * 2),
        ("role_assertions", [{"assertion_id": str(UUID(int=23))}] * 33),
    ],
)
async def test_invalid_predecessor_evidence_refuses_before_retaining_correction(family, entries):
    session = Session()
    await _apply(session, _edit())
    session.history[0]["record_json"][family] = entries
    before = copy.deepcopy((session.head, session.history, session.assertions, session.draft))
    correction = _edit(operation="correct", expected_revision=1, idempotency_key="invalid-predecessor")
    correction.fields["assertions"]["expected_revision"] = 1
    with pytest.raises(store.RegistryRecordConflict, match="predecessor_invalid"):
        await _apply(session, correction)
    assert (session.head, session.history, session.assertions, session.draft) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("nested", [False, True])
async def test_callers_cannot_supply_predecessor_metadata(nested):
    session = Session()
    command = _edit()
    target = command.fields["assertions"] if nested else command.fields
    target["correction_predecessors"] = {"company_revision": 1}
    with pytest.raises(ValueError):
        await _apply(session, command)
    assert session.events == []


@pytest.mark.asyncio
async def test_history_and_preview_project_metadata_without_changing_retained_evidence():
    statements = []

    class HistoryReader:
        async def execute(self, statement):
            statements.append(statement.compile(dialect=postgresql.dialect()))

            class Rows:
                def mappings(self):
                    return self

                def all(self):
                    return []

            return Rows()

    await undo._read_history_page(HistoryReader(), "company", UUID(COMPANY), 50, 0, "example_registry")
    assert "correction_predecessors" in statements[0].params.values()
    assert "record_json - " in str(statements[0])

    class PreviewReader:
        async def fetchval(self, sql, *_args):
            assert "previous.record_json-'correction_predecessors'" in sql
            assert "history.record_json-'correction_predecessors'" in sql
            return "{}"

    command = approval.RegistryApprovalCommand(1, 0, (), "Reviewed selection", "preview-key")
    assert await preview._preview_document(PreviewReader(), '"example_registry"', '"selection"', command) == "{}"
