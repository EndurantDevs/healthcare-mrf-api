# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected draft flow with fake SQL results; native transaction proof is separate."""

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
from sqlalchemy.schema import CreateColumn

from db.models.company_registry_assertions import CompanyRegistryIdentifierAssertion, CompanyRegistryRoleAssertion
from process import registry_approval_store as approval
from process import registry_management_permissions as permissions
from process import registry_record_store as store
from process.registry_manual_undo import RegistryManualUndoCommand, _prepare_correction
from tests.test_company_registry_assertion_values import COMPANY, _command, _identifier, _role

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
        params = statement.compile(dialect=postgresql.dialect()).params
        table = getattr(statement, "table", None)
        if table is None:
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
async def test_company_read_batches_two_families_for_multiple_revisions():
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
    assert len(queries) == 2
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
                "jsonb_build_object('role_assertions','[]'::jsonb,'identifier_assertions','[]'::jsonb)||history.record_json"
                in sql
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
