# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
from pathlib import Path
import subprocess
import sys

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db import migration_adoption
from db import migration_index_adoption


ROOT = Path(__file__).resolve().parents[1]


class _OperationRecorder:
    def __init__(self):
        self.altered_columns = []
        self.created_indexes = []
        self.dropped_indexes = []

    def alter_column(self, table_name, column_name, **options):
        self.altered_columns.append((table_name, column_name, options))

    def create_index(self, index_name, table_name, columns, **options):
        self.created_indexes.append((index_name, table_name, columns, options))

    def drop_index(self, index_name, **options):
        self.dropped_indexes.append((index_name, options))


class _TableInspector:
    def __init__(self, columns, primary_key=("record_id",)):
        self.columns = columns
        self.primary_key = primary_key

    def has_table(self, table_name, schema=None):
        return table_name == "adopted_record" and schema == "mrf"

    def get_columns(self, table_name, schema=None):
        assert table_name == "adopted_record" and schema == "mrf"
        return self.columns

    def get_pk_constraint(self, table_name, schema=None):
        assert table_name == "adopted_record" and schema == "mrf"
        return {"constrained_columns": list(self.primary_key)}

    def get_unique_constraints(self, _table_name, schema=None):
        assert schema == "mrf"
        return []

    def get_foreign_keys(self, _table_name, schema=None):
        assert schema == "mrf"
        return []


def _column_record(name, column_type, *, nullable, default=None, computed=None):
    return {
        "name": name,
        "type": column_type,
        "nullable": nullable,
        "default": default,
        "computed": computed,
    }


def _mock_existing_index(
    monkeypatch,
    index_name,
    key_columns,
    *,
    include_columns=(),
    predicate=None,
    catalog_overrides=None,
):
    """Install matching inspector and catalog records for one named index."""
    key_count = len(key_columns)
    catalog_record_dict = {
        "indisvalid": True,
        "indisready": True,
        "key_columns": list(key_columns),
        "include_columns": list(include_columns),
        "predicate": predicate,
        "indisunique": False,
        "access_method": "btree",
        "operator_classes": [""] * key_count,
        "collations": [""] * key_count,
        "sort_options": [0] * key_count,
        "indnullsnotdistinct": False,
        "indisexclusion": False,
        "indimmediate": True,
        "expression_keys": False,
        "relation_options": [],
    }
    catalog_record_dict.update(catalog_overrides or {})
    monkeypatch.setattr(
        migration_index_adoption,
        "_index_catalog_record",
        lambda *_args: catalog_record_dict,
    )


def test_existing_table_adoption_accepts_json_and_restores_server_default(monkeypatch):
    inspector = _TableInspector(
        [
            _column_record("record_id", sa.String(64), nullable=False),
            _column_record("payload_json", postgresql.JSON(), nullable=False),
            _column_record("row_count", sa.BigInteger(), nullable=False),
        ]
    )
    operations = _OperationRecorder()
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )

    migration_adoption.create_table_or_validate(
        operations,
        "adopted_record",
        sa.Column("record_id", sa.String(64), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(), nullable=False),
        sa.Column(
            "row_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.PrimaryKeyConstraint("record_id"),
        schema="mrf",
    )

    assert len(operations.altered_columns) == 1
    assert operations.altered_columns[0][:2] == ("adopted_record", "row_count")
    assert str(operations.altered_columns[0][2]["server_default"]) == "0"


def test_existing_table_adoption_rejects_unknown_primary_key(monkeypatch):
    inspector = _TableInspector(
        [_column_record("record_id", sa.String(64), nullable=False)],
        primary_key=("unexpected_id",),
    )
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )

    with pytest.raises(RuntimeError, match="primary_key_mismatch"):
        migration_adoption.create_table_or_validate(
            _OperationRecorder(),
            "adopted_record",
            sa.Column("record_id", sa.String(64), nullable=False),
            sa.PrimaryKeyConstraint("record_id"),
            schema="mrf",
        )


def test_existing_table_adoption_rejects_wrong_server_default(monkeypatch):
    inspector = _TableInspector(
        [
            _column_record("record_id", sa.String(64), nullable=False),
            _column_record(
                "row_count",
                sa.BigInteger(),
                nullable=False,
                default="1",
            ),
        ]
    )
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )

    with pytest.raises(RuntimeError, match="column_default_mismatch"):
        migration_adoption.create_table_or_validate(
            _OperationRecorder(),
            "adopted_record",
            sa.Column("record_id", sa.String(64), nullable=False),
            sa.Column(
                "row_count",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.PrimaryKeyConstraint("record_id"),
            schema="mrf",
        )


@pytest.mark.parametrize(
    ("column_record", "expected_column"),
    (
        (
            _column_record("value", sa.String(32), nullable=True),
            sa.Column("value", sa.Text(), nullable=True),
        ),
        (
            _column_record("value", sa.Integer(), nullable=True),
            sa.Column("value", sa.BigInteger(), nullable=True),
        ),
        (
            _column_record(
                "value",
                sa.TIMESTAMP(timezone=True),
                nullable=True,
            ),
            sa.Column("value", sa.TIMESTAMP(timezone=False), nullable=True),
        ),
        (
            _column_record("value", sa.Text(), nullable=False),
            sa.Column("value", sa.Text(), nullable=True),
        ),
        (
            _column_record("value", sa.Text(), nullable=True, default="'forced'"),
            sa.Column("value", sa.Text(), nullable=True),
        ),
    ),
)
def test_existing_column_adoption_rejects_unexpected_shape(
    monkeypatch,
    column_record,
    expected_column,
):
    inspector = _TableInspector([column_record])
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )

    with pytest.raises(RuntimeError, match="existing_schema_"):
        migration_adoption.add_column_if_missing(
            _OperationRecorder(),
            "adopted_record",
            expected_column,
            schema="mrf",
        )


def test_existing_column_adoption_allows_explicit_forward_nullability(monkeypatch):
    inspector = _TableInspector(
        [_column_record("value", sa.Text(), nullable=False)]
    )
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )

    migration_adoption.add_column_if_missing(
        _OperationRecorder(),
        "adopted_record",
        sa.Column("value", sa.Text(), nullable=True),
        schema="mrf",
        accepted_nullabilities=(True, False),
    )


def test_existing_generated_column_requires_matching_persisted_expression(monkeypatch):
    inspector = _TableInspector(
        [
            _column_record("record_id", sa.String(64), nullable=False),
            _column_record(
                "is_active",
                sa.Boolean(),
                nullable=True,
                computed={
                    "persisted": True,
                    "sqltext": "((record_id)::text <> ''::text)",
                },
            ),
        ]
    )
    monkeypatch.setattr(migration_adoption, "_schema_inspector", lambda _operations: inspector)
    monkeypatch.setattr(
        migration_adoption,
        "_canonical_column_expression",
        lambda *_args: "((record_id)::text <> ''::text)",
    )

    migration_adoption.add_column_if_missing(
        _OperationRecorder(),
        "adopted_record",
        sa.Column(
            "is_active",
            sa.Boolean(),
            sa.Computed("record_id <> ''", persisted=True),
        ),
        schema="mrf",
    )


def test_generated_expression_literal_case_mismatch_fails_closed(monkeypatch):
    inspector = _TableInspector(
        [
            _column_record(
                "is_active",
                sa.Boolean(),
                nullable=True,
                computed={
                    "persisted": True,
                    "sqltext": "record_id = 'ACTIVE'",
                },
            ),
        ]
    )
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )

    with pytest.raises(RuntimeError, match="generated_expression_mismatch"):
        migration_adoption.add_column_if_missing(
            _OperationRecorder(),
            "adopted_record",
            sa.Column(
                "is_active",
                sa.Boolean(),
                sa.Computed("record_id = 'active'", persisted=True),
            ),
            schema="mrf",
        )


def test_expression_normalization_preserves_grouping_and_constraining_casts():
    assert migration_adoption._normalized_expression(
        "enabled AND (primary_match OR fallback_match)"
    ) != migration_adoption._normalized_expression(
        "(enabled AND primary_match) OR fallback_match"
    )
    assert migration_adoption._normalized_expression(
        "record_id::character varying(8)"
    ) != migration_adoption._normalized_expression("record_id")


def test_existing_index_requires_valid_complete_shape(monkeypatch):
    _mock_existing_index(
        monkeypatch,
        "adopted_record_active_idx",
        ("record_id",),
        include_columns=("payload_hash",),
        predicate="record_id <> ''",
    )

    assert migration_index_adoption.has_matching_index(
        _OperationRecorder(),
        "adopted_record_active_idx",
        "adopted_record",
        ("record_id",),
        schema="mrf",
        postgresql_include=("payload_hash",),
        postgresql_where=sa.text("record_id <> ''"),
    )


def test_index_predicate_literal_case_mismatch_fails_closed(monkeypatch):
    _mock_existing_index(
        monkeypatch,
        "adopted_record_type_idx",
        ("record_id",),
        predicate="record_type = 'GROUP'",
    )

    with pytest.raises(RuntimeError, match="index_mismatch"):
        migration_index_adoption.has_matching_index(
            _OperationRecorder(),
            "adopted_record_type_idx",
            "adopted_record",
            ("record_id",),
            schema="mrf",
            postgresql_where=sa.text("record_type = 'group'"),
        )


@pytest.mark.parametrize(
    ("catalog_override", "override_value"),
    (
        ("access_method", "hash"),
        ("operator_classes", ("pg_catalog.text_pattern_ops",)),
        ("collations", ("pg_catalog.C",)),
        ("sort_options", (1,)),
        ("indnullsnotdistinct", True),
        ("indisexclusion", True),
        ("indimmediate", False),
        ("expression_keys", True),
        ("relation_options", ("fillfactor=70",)),
    ),
)
def test_existing_index_rejects_nondefault_catalog_identity(
    monkeypatch,
    catalog_override,
    override_value,
):
    _mock_existing_index(
        monkeypatch,
        "adopted_record_idx",
        ("record_id",),
        catalog_overrides={catalog_override: override_value},
    )

    with pytest.raises(RuntimeError, match="index_mismatch"):
        migration_index_adoption.has_matching_index(
            _OperationRecorder(),
            "adopted_record_idx",
            "adopted_record",
            ("record_id",),
            schema="mrf",
        )


def test_existing_invalid_index_fails_closed(monkeypatch):
    _mock_existing_index(
        monkeypatch,
        "adopted_record_idx",
        ("record_id",),
        catalog_overrides={"indisvalid": False},
    )

    with pytest.raises(RuntimeError, match="index_invalid"):
        migration_index_adoption.has_matching_index(
            _OperationRecorder(),
            "adopted_record_idx",
            "adopted_record",
            ("record_id",),
            schema="mrf",
        )


def test_known_legacy_index_shape_is_replaced(monkeypatch):
    """A named legacy index is replaced only when its full shape is recognized."""
    _mock_existing_index(
        monkeypatch,
        "adopted_record_active_idx",
        ("record_id", "payload_hash"),
        predicate="record_id <> ''",
    )
    operations = _OperationRecorder()

    migration_index_adoption.create_index_if_missing(
        operations,
        "adopted_record_active_idx",
        "adopted_record",
        ("record_id",),
        schema="mrf",
        legacy_shapes=(
            migration_index_adoption.IndexDefinition(
                key_columns=("record_id", "payload_hash"),
                predicate="record_id <> ''",
            ),
        ),
        postgresql_include=("payload_hash",),
        postgresql_where=sa.text("record_id <> ''"),
    )

    assert operations.dropped_indexes == [
        (
            "adopted_record_active_idx",
            {"table_name": "adopted_record", "schema": "mrf"},
        )
    ]
    assert len(operations.created_indexes) == 1
    created_index = operations.created_indexes[0]
    assert created_index[:3] == (
        "adopted_record_active_idx",
        "adopted_record",
        ["record_id"],
    )
    assert created_index[3]["schema"] == "mrf"
    assert created_index[3]["postgresql_include"] == ("payload_hash",)
    assert str(created_index[3]["postgresql_where"]) == "record_id <> ''"


def test_provider_directory_adoption_migrations_compile_offline_sql():
    environment = os.environ.copy()
    environment.update(
        {
            "HLTHPRT_DB_HOST": "offline.invalid",
            "HLTHPRT_DB_PORT": "5432",
            "HLTHPRT_DB_USER": "offline",
            "HLTHPRT_DB_PASSWORD": "offline",
            "HLTHPRT_DB_DATABASE": "offline",
            "HLTHPRT_DB_SCHEMA": "mrf",
            "DB_SCHEMA": "mrf",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "20260713233000_provider_directory_resource_identifiers:head",
            "--sql",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "provider_directory_dataset_resource_plan_lookup_idx" in result.stdout
    assert "import_run_provider_directory_retry_child_idx" in result.stdout
