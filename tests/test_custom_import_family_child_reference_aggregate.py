# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Membership witnesses preserve exact refresh, scope and setting boundaries."""

import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import post_seal_audit as audit
from tests.test_custom_import_post_seal_audit_ranges import _range_context


def _migration():
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20261108010000_custom_import_family_child_reference_aggregate.py"
    )
    spec = importlib.util.spec_from_file_location("family_child_reference_unit", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exact_query_and_planner_refresh_preserve_every_other_arm():
    migration = _migration()
    previous, updated = migration._bodies("synthetic_control")
    parent = migration._parent()
    assert previous == parent._bodies("synthetic_control")[1]
    assert migration.down_revision == parent.revision
    assert audit._sql_resource()[0]["arms"][20]["sql"] == migration._UPDATED_QUERY.replace("__CANDIDATE__", "__LEAF__")
    assert hashlib.sha256(migration._UPDATED_QUERY.encode()).hexdigest() == migration._QUERY_SHA256
    assert "IF stage=21 THEN" in updated and "previous_enable_nestloop text;" in updated
    for predicate in (
        "f.family_revision_id=e.family_revision_id",
        "ROW(f.dataset_id,f.schema_revision_id,f.root_record_id)",
        "ROW(e.dataset_id,e.schema_revision_id,e.root_record_id)",
        "c.child_revision_id=e.child_revision_id",
        "ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot)",
        "ROW(e.dataset_id,$3,e.schema_revision_id,e.root_record_id,e.collection_slot)",
    ):
        assert predicate in migration._PREVIOUS_QUERY and predicate in migration._UPDATED_QUERY
    assert "min(e.child_revision_id)" in migration._UPDATED_QUERY and "HAVING count(*)>0" in migration._UPDATED_QUERY
    assert "OR NOT EXISTS" not in migration._UPDATED_QUERY


@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.parametrize("installed", ["old", "new", "unknown", None])
def test_refresh_keeps_existing_identity_guard_and_checked_refresher(monkeypatch, rollback, installed):
    migration = _migration()
    calls, statements = [], []
    continuation = SimpleNamespace(_refresh=lambda *args: calls.append(args))
    staged = SimpleNamespace(
        _previous=lambda _name: continuation,
        _finality=lambda: SimpleNamespace(
            _bulk=lambda: SimpleNamespace(_storage=lambda: SimpleNamespace(_quote=lambda s: f'"{s}"'))
        ),
    )
    ranges = SimpleNamespace(_schema=lambda: "synthetic_control", _arms=lambda: SimpleNamespace(_staged=lambda: staged))
    monkeypatch.setattr(migration, "_parent", lambda: SimpleNamespace(_ranges=lambda: ranges))
    monkeypatch.setattr(migration, "_bodies", lambda _schema: ("old", "new"))
    monkeypatch.setattr(
        migration.op,
        "get_bind",
        lambda: SimpleNamespace(execute=lambda *args: SimpleNamespace(scalar_one_or_none=lambda: installed)),
    )
    monkeypatch.setattr(migration.op, "execute", lambda statement: statements.append(str(statement)))
    if installed not in ({"new"} if rollback else {"old", "new"}):
        with pytest.raises(RuntimeError, match="function_identity_mismatch"):
            migration._refresh(rollback=rollback)
        assert not calls and not statements
    else:
        migration._refresh(rollback=rollback)
        assert calls == [
            (
                '"synthetic_control".verify_custom_import_build_structure(bigint)',
                installed,
                "old" if rollback else "new",
            )
        ]
        assert statements == [
            'LOCK TABLE "synthetic_control".custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE'
        ]


@pytest.mark.parametrize("setting", ["on", "off"])
@pytest.mark.parametrize("failure", [False, True])
async def test_audit_planner_setting_is_scoped_to_success_or_savepoint_rollback(setting, failure):
    class Session:
        current = setting
        events = []

        def begin_nested(self):
            session = self

            class Savepoint:
                async def __aenter__(self):
                    self.before = session.current

                async def __aexit__(self, kind, value, traceback):
                    if kind is not None:
                        session.current = self.before

            return Savepoint()

        async def scalar(self, statement):
            assert "current_setting('enable_nestloop')" in str(statement)
            return self.current

        async def execute(self, statement, parameters=None):
            if "set_config" in str(statement):
                self.current = parameters["previous_nestloop"] if parameters else "off"
            else:
                assert self.current == "off"
                if failure:
                    raise ConnectionError("synthetic query failure")
            return Mock(first=Mock(return_value=None))

    session = Session()
    previous = SimpleNamespace(next_ordinal=21, checkpoint=20)
    if failure:
        with pytest.raises(ConnectionError, match="query failure"):
            await audit._run_arm(session, _range_context(), previous)
    else:
        progress = await audit._run_arm(session, _range_context(), previous)
        assert progress["checkpoint"] == 21 and progress["outcome"] == "passed"
    assert session.current == setting


async def test_other_arm_does_not_change_planner_settings():
    session = SimpleNamespace(execute=AsyncMock(return_value=Mock(first=Mock(return_value=None))))
    await audit._run_arm(session, _range_context(), SimpleNamespace(next_ordinal=20, checkpoint=19))
    assert session.execute.await_count == 1 and "set_config" not in str(session.execute.await_args.args[0])
