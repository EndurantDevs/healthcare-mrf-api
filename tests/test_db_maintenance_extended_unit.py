# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest
from db import maintenance
from sqlalchemy import Column, Computed, Integer, MetaData, String, Table
from sqlalchemy.dialects import postgresql


class _AsyncBeginContext:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        return None


class _AsyncConnection:
    def __init__(self) -> None:
        self.run_sync_calls = []

    async def run_sync(self, operation, *args) -> None:
        self.run_sync_calls.append((operation, args))


class _AsyncDatabase:
    def __init__(self) -> None:
        self.connection = _AsyncConnection()
        self.engine = SimpleNamespace(
            begin=lambda: _AsyncBeginContext(self.connection)
        )
        self.connect_count = 0

    async def connect(self) -> None:
        self.connect_count += 1


@pytest.mark.asyncio
async def test_sync_structure_connects_and_passes_flags_to_sync_callback(
    monkeypatch,
) -> None:
    fake_database = _AsyncDatabase()
    monkeypatch.setattr(maintenance, "db", fake_database)

    summary = await maintenance.sync_structure(add_columns=False, add_indexes=True)

    assert fake_database.connect_count == 1
    operation, arguments = fake_database.connection.run_sync_calls[0]
    assert operation is maintenance._sync_structure
    assert arguments == (summary, False, True)
    assert summary == {
        "tables": [],
        "columns": [],
        "indexes": [],
        "constraints": [],
        "skipped_columns": [],
        "retired": [],
    }


class _SyncTable:
    def __init__(self, schema: str, name: str) -> None:
        self.schema = schema
        self.name = name
        self.fullname = f"{schema}.{name}"
        self.create_calls = []

    def create(self, **kwargs) -> None:
        self.create_calls.append(kwargs)


class _SyncInspector:
    def __init__(self, existing_tables: set[str]) -> None:
        self.existing_tables = existing_tables

    def has_table(self, table_name: str, *, schema: str) -> bool:
        return f"{schema}.{table_name}" in self.existing_tables


def _empty_sync_summary() -> dict[str, list[str]]:
    return {
        "tables": [],
        "columns": [],
        "indexes": [],
        "constraints": [],
        "skipped_columns": [],
        "retired": [],
    }


class _SyncStructureFixture:
    """Record table routing without a live database."""

    def __init__(self) -> None:
        self.unmanaged_table = _SyncTable("tiger", "place")
        self.new_table = _SyncTable("mrf", "new_table")
        self.existing_without_model = _SyncTable("mrf", "without_model")
        self.existing_with_model = _SyncTable("mrf", "with_model")
        self.tables = (
            self.unmanaged_table,
            self.new_table,
            self.existing_without_model,
            self.existing_with_model,
        )
        self.inspector = _SyncInspector(
            {
                self.existing_without_model.fullname,
                self.existing_with_model.fullname,
            }
        )
        self.column_tables = []
        self.index_tables = []
        self.pagination_calls = []
        self.model = object()

    def install(self, monkeypatch) -> None:
        monkeypatch.setattr(
            maintenance.Base,
            "metadata",
            SimpleNamespace(sorted_tables=self.tables),
        )
        monkeypatch.setattr(maintenance, "inspect", lambda connection: self.inspector)
        monkeypatch.setattr(
            maintenance,
            "_model_by_table_fullname",
            lambda: {self.existing_with_model.fullname: self.model},
        )
        monkeypatch.setattr(maintenance, "_managed_schemas", lambda: ("mrf",))
        monkeypatch.setattr(
            maintenance,
            "_ensure_columns",
            lambda connection, current_inspector, table, summary: (
                self.column_tables.append(table)
            ),
        )
        monkeypatch.setattr(
            maintenance,
            "_ensure_indexes",
            lambda connection, current_inspector, table, current_model, summary: (
                self.index_tables.append((table, current_model))
            ),
        )
        monkeypatch.setattr(
            maintenance.ProviderDirectoryPaginationCheckpoint,
            "__table__",
            SimpleNamespace(schema="mrf"),
        )
        monkeypatch.setattr(
            maintenance,
            "ensure_provider_directory_pagination_root_identity",
            lambda *args: self.pagination_calls.append(args),
        )


def test_sync_structure_routes_new_existing_and_unmanaged_tables(monkeypatch) -> None:
    structure_fixture = _SyncStructureFixture()
    structure_fixture.install(monkeypatch)
    summary = _empty_sync_summary()
    sync_connection = object()

    maintenance._sync_structure(sync_connection, summary, True, True)

    assert summary["tables"] == [structure_fixture.new_table.fullname]
    assert structure_fixture.new_table.create_calls == [
        {"bind": sync_connection, "checkfirst": True}
    ]
    assert structure_fixture.column_tables == [
        structure_fixture.existing_without_model,
        structure_fixture.existing_with_model,
    ]
    assert structure_fixture.index_tables == [
        (structure_fixture.existing_with_model, structure_fixture.model)
    ]
    assert len(structure_fixture.pagination_calls) == 1


def test_sync_structure_can_skip_optional_work_and_pagination(monkeypatch) -> None:
    existing_table = _SyncTable("mrf", "existing")
    monkeypatch.setattr(
        maintenance.Base,
        "metadata",
        SimpleNamespace(sorted_tables=(existing_table,)),
    )
    monkeypatch.setattr(
        maintenance,
        "inspect",
        lambda connection: _SyncInspector({existing_table.fullname}),
    )
    monkeypatch.setattr(maintenance, "_model_by_table_fullname", lambda: {})
    monkeypatch.setattr(maintenance, "_managed_schemas", lambda: ("mrf",))
    monkeypatch.setattr(
        maintenance.ProviderDirectoryPaginationCheckpoint,
        "__table__",
        SimpleNamespace(schema="other"),
    )
    monkeypatch.setattr(
        maintenance,
        "_ensure_columns",
        lambda *args: pytest.fail("columns should be skipped"),
    )
    monkeypatch.setattr(
        maintenance,
        "_ensure_indexes",
        lambda *args: pytest.fail("indexes should be skipped"),
    )
    monkeypatch.setattr(
        maintenance,
        "ensure_provider_directory_pagination_root_identity",
        lambda *args: pytest.fail("pagination should be skipped"),
    )

    maintenance._sync_structure(object(), _empty_sync_summary(), False, False)


def test_model_map_ignores_mappers_without_tables(monkeypatch) -> None:
    mapped_table = SimpleNamespace(fullname="mrf.mapped")
    mapped_class = type("Mapped", (), {"__table__": mapped_table})
    unmapped_class = type("Unmapped", (), {})
    monkeypatch.setattr(
        maintenance.Base,
        "registry",
        SimpleNamespace(
            mappers=(
                SimpleNamespace(class_=mapped_class),
                SimpleNamespace(class_=unmapped_class),
            )
        ),
    )

    assert maintenance._model_by_table_fullname() == {"mrf.mapped": mapped_class}


class _ColumnInspector:
    def get_columns(self, table_name: str, *, schema: str):
        return ({"name": "existing"},)


class _RecordingSyncConnection:
    dialect = postgresql.dialect()

    def __init__(self) -> None:
        self.statements = []

    def execute(self, statement) -> None:
        self.statements.append(str(statement))


def test_ensure_columns_adds_safe_columns_and_skips_unsafe_required_column() -> None:
    table = Table(
        "column_fixture",
        MetaData(),
        Column("existing", Integer),
        Column("nullable_value", String),
        Column("required_without_default", String, nullable=False),
        Column("required_with_default", Integer, nullable=False, server_default="7"),
        schema="mrf",
    )
    connection = _RecordingSyncConnection()
    summary = _empty_sync_summary()

    maintenance._ensure_columns(connection, _ColumnInspector(), table, summary)

    assert summary["columns"] == [
        "mrf.column_fixture.nullable_value",
        "mrf.column_fixture.required_with_default",
    ]
    assert summary["skipped_columns"] == [
        "mrf.column_fixture.required_without_default"
    ]
    assert len(connection.statements) == 2


def test_compile_column_definition_covers_scalar_and_virtual_defaults() -> None:
    scalar_default = Column("scalar_default", Integer, default=3, nullable=False)
    virtual_column = Column(
        "virtual_value",
        Integer,
        Computed("source_value + 1", persisted=False),
    )
    callable_default = Column(
        "callable_default",
        Integer,
        default=lambda: 3,
        nullable=False,
    )
    dialect = postgresql.dialect()

    assert maintenance._compile_column_definition(
        scalar_default,
        dialect,
    ) == '"scalar_default" INTEGER DEFAULT 3 NOT NULL'
    assert maintenance._compile_column_definition(
        virtual_column,
        dialect,
    ) == '"virtual_value" INTEGER GENERATED ALWAYS AS (source_value + 1)'
    assert maintenance._compile_column_definition(callable_default, dialect) is None


class _IndexInspector:
    def __init__(self, names: tuple[str, ...]) -> None:
        self.names = names

    def get_indexes(self, table_name: str, *, schema: str):
        return tuple({"name": name} for name in self.names)


def test_ensure_indexes_validates_exact_existing_and_creates_missing(monkeypatch) -> None:
    exact_name = next(iter(maintenance.EXACT_RUNTIME_INDEXES))
    ordinary_name = "ordinary_existing"
    new_name = "new_index"
    specs = (
        {
            "name": exact_name,
            "columns": ("active",),
            "unique": True,
            "where": "active",
            "include": ("dataset_id",),
            "using": "btree",
        },
        {"name": ordinary_name, "columns": ("ordinary",)},
        {"name": new_name, "columns": ("new_value",)},
    )
    matching_calls = []
    monkeypatch.setattr(maintenance, "_iter_index_specs", lambda model: specs)
    monkeypatch.setattr(
        maintenance,
        "has_matching_index",
        lambda *args, **kwargs: matching_calls.append((args, kwargs)),
    )
    table = SimpleNamespace(schema=None, name="sample", fullname="sample")
    connection = _RecordingSyncConnection()
    summary = _empty_sync_summary()

    maintenance._ensure_indexes(
        connection,
        _IndexInspector((exact_name, ordinary_name)),
        table,
        object(),
        summary,
    )

    assert len(matching_calls) == 1
    assert matching_calls[0][1]["schema"] == "public"
    assert summary["indexes"] == [f"sample.{new_name}"]
    assert len(connection.statements) == 1


def test_iter_index_specs_covers_primary_and_skips_empty_additional_spec() -> None:
    model = type(
        "IndexFixture",
        (),
        {
            "__tablename__": "fixture",
            "__my_index_elements__": ("lookup",),
            "__primary_index_name__": "fixture_lookup_unique",
            "__my_additional_indexes__": (
                {"index_elements": ()},
                {"index_elements": ("other",)},
            ),
        },
    )

    specs = list(maintenance._iter_index_specs(model))

    assert [spec["name"] for spec in specs] == [
        "fixture_lookup_unique",
        "fixture_other_idx",
    ]
    assert maintenance._primary_key_columns(object()) == ()


@pytest.mark.parametrize(
    ("raw_columns", "expected"),
    (
        (None, ()),
        ("  ", ()),
        ((" one ", "", " two"), ("one", "two")),
        ((value for value in (" one ", "two")), ("one", "two")),
        (7, ("7",)),
    ),
)
def test_normalize_index_columns_handles_supported_shapes(raw_columns, expected) -> None:
    assert maintenance._normalize_index_columns(raw_columns) == expected


def test_build_index_sql_supports_unique_index_without_optional_clauses() -> None:
    index_sql = maintenance._build_index_sql(
        "sample_unique",
        "",
        "sample",
        {"columns": ("value",), "unique": True},
    )
    assert index_sql == (
        "CREATE UNIQUE INDEX IF NOT EXISTS sample_unique "
        'ON "sample" (value);'
    )


def test_managed_schemas_and_public_schema_variants(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_MANAGED_SCHEMAS", " mrf, public ,, ")
    assert maintenance._managed_schemas() == ("mrf", "public")
    monkeypatch.setenv("HLTHPRT_MANAGED_SCHEMAS", ",,")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "   ")
    assert maintenance._managed_schemas() == ("mrf",)
    assert maintenance._should_manage_table_schema(None, {"public"})
    assert not maintenance._should_manage_table_schema(None, {"mrf"})


def test_render_sync_summary_reports_each_nonempty_category(monkeypatch) -> None:
    messages = []
    monkeypatch.setattr(maintenance.click, "echo", messages.append)
    summary_by_category = {
        "retired": ["old_table"],
        "tables": ["new_table"],
        "columns": ["table.new_column"],
        "indexes": ["table.new_index"],
        "constraints": ["table.constraint"],
        "skipped_columns": ["table.required"],
    }

    maintenance.render_sync_summary(summary_by_category)
    maintenance.render_sync_summary(
        {category: [] for category in summary_by_category}
    )

    assert len(messages) == 6
