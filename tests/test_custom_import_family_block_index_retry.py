# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact index observation and transactional retries; only SQL transport is fake."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from process.custom_import import family_block_binding as binding_api
from process.custom_import import family_block_store as store
from process.custom_import.family_block_layout import index_specifications, index_statements
from tests import test_custom_import_family_block_raw_integration as catalog_fixture
from tests import test_custom_import_family_block_store as store_fixture

_DAMAGES = (
    ("table_oid", 999),
    ("index_owner", 999),
    ("index_kind", "I"),
    ("access_method", "hash"),
    ("key_names", ["wrong"]),
    ("key_count", 999),
    ("attribute_count", 999),
    ("collation_names", ["pg_catalog.default"]),
    ("uncollated", False),
    ("unique_index", True),
    ("primary_index", True),
    ("exclusion_index", True),
    *((name, False) for name in ("valid", "ready", "live", "plain", "complete", "default_options", "default_ops")),
)


def _catalog_case(monkeypatch, change):
    session = catalog_fixture._Session(store_fixture._layout())
    binding = catalog_fixture._resolve(session, persistence="candidate")
    original = session.execute

    async def execute(statement, parameters):
        observed = await original(statement, parameters)
        assert "index_names" in parameters
        change(observed.rows)
        return observed

    monkeypatch.setattr(session, "execute", execute)
    return session, binding


def test_zero_index_observations_are_optional_only_at_the_creation_boundary(monkeypatch):
    session, binding = _catalog_case(monkeypatch, lambda rows: rows.clear())
    prepared_sessions = []

    async def prepare(current):
        prepared_sessions.append(current)

    assert not asyncio.run(binding_api.has_family_block_indexes(session, binding, prepare=prepare))
    with pytest.raises(binding_api.FamilyBlockUnavailable, match="census"):
        asyncio.run(binding_api.validate_family_block_indexes(session, binding, prepare=prepare))
    assert prepared_sessions == [session, session]
    assert len(session.calls) == 4


def test_complete_exact_indexes_are_reobserved_without_a_cache(monkeypatch):
    session, binding = _catalog_case(monkeypatch, lambda _rows: None)
    assert asyncio.run(binding_api.has_family_block_indexes(session, binding))
    assert asyncio.run(binding_api.validate_family_block_indexes(session, binding)) is None
    assert len(session.calls) == 4


@pytest.mark.parametrize("damage", ("partial", "duplicate", "unknown"))
def test_partial_duplicate_or_unknown_index_set_never_means_absent(monkeypatch, damage):
    def change(rows):
        if damage == "partial":
            rows.pop()
        elif damage == "duplicate":
            rows[-1] = rows[0]
        else:
            rows[-1]["index_name"] = "unknown"

    session, binding = _catalog_case(monkeypatch, change)
    with pytest.raises(binding_api.FamilyBlockUnavailable):
        asyncio.run(binding_api.has_family_block_indexes(session, binding))


@pytest.mark.parametrize("field,value", _DAMAGES)
def test_every_existing_native_index_custody_cell_is_still_required(monkeypatch, field, value):
    def change(rows):
        rows[0][field] = value

    session, binding = _catalog_case(monkeypatch, change)
    with pytest.raises(binding_api.FamilyBlockUnavailable, match="custody"):
        asyncio.run(binding_api.has_family_block_indexes(session, binding))


class _IndexTransaction(store_fixture._Transaction):
    """Use the real transaction guard and exact catalog/DDL shapes with fake IO."""

    def __init__(self):
        super().__init__(phase="closed")
        self.layout = store_fixture._layout()
        self.catalog = catalog_fixture._Session(self.layout)
        self.statements = dict(
            zip(
                index_statements(11, self.layout),
                (name for _role, name, _keys in index_specifications(11, self.layout)),
                strict=True,
            )
        )
        self.primed, self.damage = set(), None
        self.fail_statement = None
        self.is_reply_lost = False
        self.has_post_create_damage = False

    @property
    def index_names(self):
        return self.primed | {
            self.statements[statement] for kind, statement in (*self.committed, *self.pending) if kind == "ddl"
        }

    async def execute(self, statement, parameters):
        assert "index_names" in parameters
        observed = await self.catalog.execute(statement, parameters)
        visible_rows = [row for row in observed.rows if row["index_name"] in self.index_names]
        if self.damage is not None and visible_rows:
            visible_rows[0][self.damage[0]] = self.damage[1]
        if self.has_post_create_damage and self.pending and visible_rows:
            visible_rows[0]["ready"] = False
        self.events.append("catalog-indexes")
        return catalog_fixture._Result(visible_rows)

    @asynccontextmanager
    async def begin(self):
        async with super().begin():
            yield
        self.pending.clear()
        if self.is_reply_lost:
            self.is_reply_lost = False
            raise ConnectionError("synthetic commit reply lost after the real guard committed")


def _install(monkeypatch, transaction):
    store_fixture._install(monkeypatch, transaction, transaction.layout)
    transaction.binding = catalog_fixture._resolve(transaction.catalog, persistence="candidate")
    previous = store._execute

    async def execute(session, statement, parameters=None):
        if isinstance(statement, str) and statement in transaction.statements:
            assert statement.startswith("CREATE INDEX ") and "IF NOT EXISTS" not in statement
            if transaction.statements[statement] in transaction.index_names:
                raise RuntimeError("synthetic native duplicate relation")
            if statement == transaction.fail_statement:
                transaction.fail_statement = None
                raise RuntimeError("synthetic native DDL failure")
        return await previous(session, statement, parameters)

    monkeypatch.setattr(store, "_execute", execute)


def _prepare(transaction):
    request = store_fixture._request(definition=transaction.layout.definition)
    asyncio.run(store.prepare_indexes(transaction, request, 11, transaction.layout))


def test_successful_index_preparation_replays_without_issuing_ddl(monkeypatch):
    transaction = _IndexTransaction()
    _install(monkeypatch, transaction)
    _prepare(transaction)
    expected_statements = [("ddl", sql) for sql in transaction.statements]
    assert transaction.committed == expected_statements
    assert transaction.events.count("catalog-indexes") == 2
    _prepare(transaction)
    assert transaction.committed == expected_statements
    assert transaction.events.count("catalog-indexes") == 3
    assert transaction.events.count("fresh-authority-before-commit") == 2


def test_lost_index_commit_reply_replays_the_exact_committed_set(monkeypatch):
    transaction = _IndexTransaction()
    _install(monkeypatch, transaction)
    transaction.is_reply_lost = True
    with pytest.raises(ConnectionError, match="reply lost"):
        _prepare(transaction)
    expected_statements = list(transaction.committed)
    assert expected_statements == [("ddl", sql) for sql in transaction.statements]
    _prepare(transaction)
    assert transaction.committed == expected_statements
    assert transaction.events.count("fresh-authority-before-commit") == 2


@pytest.mark.parametrize("failure", ("ddl", "post_create_catalog", "expired", "canceled"))
def test_failed_preparation_rolls_back_all_indexes_then_retries(monkeypatch, failure):
    transaction = _IndexTransaction()
    _install(monkeypatch, transaction)
    transaction.fail_statement = tuple(transaction.statements)[1] if failure == "ddl" else None
    transaction.has_post_create_damage = failure == "post_create_catalog"
    transaction.expired, transaction.cancelled = failure == "expired", failure == "canceled"
    with pytest.raises((RuntimeError, binding_api.FamilyBlockUnavailable, asyncio.CancelledError)):
        _prepare(transaction)
    assert not transaction.pending and not transaction.committed and not transaction.index_names
    assert transaction.events[-1] == "rollback"
    transaction.has_post_create_damage = transaction.expired = transaction.cancelled = False
    _prepare(transaction)
    assert transaction.committed == [("ddl", sql) for sql in transaction.statements]


@pytest.mark.parametrize("damage", ("partial", "mismatched"))
def test_existing_partial_or_mismatched_set_is_preserved_and_refused(monkeypatch, damage):
    transaction = _IndexTransaction()
    _install(monkeypatch, transaction)
    transaction.primed = set(transaction.statements.values())
    if damage == "partial":
        transaction.primed.pop()
    else:
        transaction.damage = "index_owner", 999
    expected_names = set(transaction.primed)
    with pytest.raises(binding_api.FamilyBlockUnavailable):
        _prepare(transaction)
    assert transaction.index_names == expected_names and not transaction.committed and not transaction.pending
    assert "ddl" not in transaction.events and transaction.events[-1] == "rollback"


def test_same_name_nonindex_collision_keeps_native_failure_and_rollback(monkeypatch):
    transaction = _IndexTransaction()
    _install(monkeypatch, transaction)
    previous = store._execute

    async def collided(session, statement, parameters=None):
        if isinstance(statement, str) and statement.startswith("CREATE INDEX "):
            raise RuntimeError("synthetic native duplicate relation")
        return await previous(session, statement, parameters)

    monkeypatch.setattr(store, "_execute", collided)
    with pytest.raises(RuntimeError, match="duplicate relation"):
        _prepare(transaction)
    assert not transaction.committed and not transaction.pending
    assert transaction.events[-1] == "rollback"
