# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from sqlalchemy.dialects import postgresql

from db import provider_directory_schema as schema_sync


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class _SyncConnection:
    dialect = postgresql.dialect()

    def __init__(self, mismatch_count=0):
        self.mismatch_count = mismatch_count
        self.executed = []

    def execute(self, statement, params=None):
        rendered_statement = str(statement)
        self.executed.append((rendered_statement, params))
        if "SELECT count(*)" in rendered_statement:
            return _ScalarResult(self.mismatch_count)
        return _ScalarResult(None)


class _Inspector:
    def __init__(
        self,
        primary_key_columns,
        missing_columns=(),
        missing_tables=(),
    ):
        self.primary_key_columns = primary_key_columns
        self.missing_columns = set(missing_columns)
        self.missing_tables = set(missing_tables)

    def has_table(self, table_name, schema=None):
        return schema == "mrf" and table_name not in self.missing_tables

    def get_pk_constraint(self, _table_name, schema=None):
        assert schema == "mrf"
        return {"constrained_columns": list(self.primary_key_columns)}

    def get_columns(self, table_name, schema=None):
        assert schema == "mrf"
        required_columns = (
            schema_sync.CHECKPOINT_REQUIRED_COLUMNS
            if table_name == schema_sync.CHECKPOINT_TABLE
            else schema_sync.DATASET_REQUIRED_COLUMNS
        )
        return [
            {"name": column_name}
            for column_name in required_columns - self.missing_columns
        ]


def _results():
    return {"indexes": [], "constraints": []}


def test_sync_rekeys_legacy_checkpoint_identity(monkeypatch):
    """Legacy sync-managed databases get the root-scoped key."""
    inspector = _Inspector(schema_sync.LEGACY_PRIMARY_KEY_COLUMNS)
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: inspector)
    connection = _SyncConnection()
    results = _results()

    schema_sync.ensure_provider_directory_pagination_root_identity(
        connection,
        "mrf",
        results,
    )

    executed_sql = "\n".join(statement for statement, _params in connection.executed)
    assert "SET LOCAL lock_timeout = '5s'" in executed_sql
    assert "SHARE ROW EXCLUSIVE MODE" in executed_sql
    assert "ACCESS EXCLUSIVE MODE" in executed_sql
    assert "UPDATE mrf.provider_directory_endpoint_dataset" in executed_sql
    assert "retry_of_run_id IS NOT NULL" in executed_sql
    assert "ALTER COLUMN acquisition_root_run_id SET NOT NULL" in executed_sql
    assert "PRIMARY KEY (canonical_api_base, resource_type, source_scope_hash" in (
        executed_sql
    )
    assert "CREATE INDEX" not in executed_sql
    assert results["constraints"] == [
        "mrf.provider_directory_pagination_checkpoint_pkey"
    ]
    assert results["indexes"] == []


def test_sync_accepts_current_identity_without_rekeying(monkeypatch):
    """Repeated sync verifies lineage and leaves the current key unchanged."""
    inspector = _Inspector(schema_sync.ROOT_PRIMARY_KEY_COLUMNS)
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: inspector)
    connection = _SyncConnection()
    results = _results()

    schema_sync.ensure_provider_directory_pagination_root_identity(
        connection,
        "mrf",
        results,
    )

    executed_sql = "\n".join(statement for statement, _params in connection.executed)
    assert "SELECT count(*)" in executed_sql
    assert "checkpoint.dataset_id IS NOT NULL" in executed_sql
    assert "LOCK TABLE" not in executed_sql
    assert "SET LOCAL lock_timeout" not in executed_sql
    assert "ALTER TABLE" not in executed_sql
    assert results == _results()


def test_sync_rejects_current_identity_mismatch_without_locking(monkeypatch):
    """A current key is checked read-only even when its lineage is invalid."""
    inspector = _Inspector(schema_sync.ROOT_PRIMARY_KEY_COLUMNS)
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: inspector)
    connection = _SyncConnection(mismatch_count=1)

    try:
        schema_sync.ensure_provider_directory_pagination_root_identity(
            connection,
            "mrf",
            _results(),
        )
    except RuntimeError as exc:
        assert "root_mismatch" in str(exc)
    else:
        raise AssertionError("mismatched current identity was accepted")

    executed_sql = "\n".join(statement for statement, _params in connection.executed)
    assert "SELECT count(*)" in executed_sql
    assert "LOCK TABLE" not in executed_sql


def test_sync_accepts_concurrent_rekey_after_locking(monkeypatch):
    """A waiter rechecks schema state after another process completes repair."""
    inspectors = iter(
        (
            _Inspector(schema_sync.LEGACY_PRIMARY_KEY_COLUMNS),
            _Inspector(schema_sync.ROOT_PRIMARY_KEY_COLUMNS),
        )
    )
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: next(inspectors))
    connection = _SyncConnection()
    results = _results()

    schema_sync.ensure_provider_directory_pagination_root_identity(
        connection,
        "mrf",
        results,
    )

    executed_sql = "\n".join(statement for statement, _params in connection.executed)
    assert "SHARE ROW EXCLUSIVE MODE" in executed_sql
    assert "ACCESS EXCLUSIVE MODE" in executed_sql
    assert "SELECT count(*)" in executed_sql
    assert "ALTER TABLE" not in executed_sql
    assert results == _results()


def test_sync_fails_closed_for_unknown_primary_key(monkeypatch):
    """Unknown deployed key shapes are never rewritten automatically."""
    inspector = _Inspector(("canonical_api_base", "resource_type"))
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: inspector)
    connection = _SyncConnection()

    try:
        schema_sync.ensure_provider_directory_pagination_root_identity(
            connection,
            "mrf",
            _results(),
        )
    except RuntimeError as exc:
        assert "primary_key_unknown" in str(exc)
    else:
        raise AssertionError("unknown primary key was accepted")

    assert connection.executed == []


def test_sync_does_not_rekey_unresolved_lineage(monkeypatch):
    """A failed root backfill aborts before the primary key is changed."""
    inspector = _Inspector(schema_sync.LEGACY_PRIMARY_KEY_COLUMNS)
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: inspector)
    connection = _SyncConnection(mismatch_count=1)

    try:
        schema_sync.ensure_provider_directory_pagination_root_identity(
            connection,
            "mrf",
            _results(),
        )
    except RuntimeError as exc:
        assert "root_backfill_failed" in str(exc)
    else:
        raise AssertionError("unresolved lineage was rekeyed")

    executed_sql = "\n".join(statement for statement, _params in connection.executed)
    assert "ALTER TABLE" not in executed_sql


def test_sync_fails_before_lock_when_required_columns_are_missing(monkeypatch):
    """A partially synced schema is reported instead of being guessed."""
    inspector = _Inspector(
        schema_sync.LEGACY_PRIMARY_KEY_COLUMNS,
        missing_columns=("retry_of_run_id",),
    )
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: inspector)
    connection = _SyncConnection()

    try:
        schema_sync.ensure_provider_directory_pagination_root_identity(
            connection,
            "mrf",
            _results(),
        )
    except RuntimeError as exc:
        assert "columns_missing:retry_of_run_id" in str(exc)
    else:
        raise AssertionError("missing checkpoint columns were accepted")

    assert connection.executed == []


def test_sync_fails_before_lock_when_dataset_table_is_missing(monkeypatch):
    """The checkpoint repair requires its foreign-key parent table."""
    inspector = _Inspector(
        schema_sync.LEGACY_PRIMARY_KEY_COLUMNS,
        missing_tables=(schema_sync.DATASET_TABLE,),
    )
    monkeypatch.setattr(schema_sync, "inspect", lambda _connection: inspector)
    connection = _SyncConnection()

    try:
        schema_sync.ensure_provider_directory_pagination_root_identity(
            connection,
            "mrf",
            _results(),
        )
    except RuntimeError as exc:
        assert "endpoint_dataset_missing" in str(exc)
    else:
        raise AssertionError("missing endpoint dataset table was accepted")

    assert connection.executed == []
