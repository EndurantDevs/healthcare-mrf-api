# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundary coverage for runtime-created schema adoption."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from db import migration_adoption
from tests.test_migration_adoption import (
    _OperationRecorder,
    _TableInspector,
    _column_record,
)


class _AdoptionRecorder(_OperationRecorder):
    def __init__(self):
        super().__init__()
        self.created_foreign_keys = []

    def create_foreign_key(self, *args, **kwargs):
        self.created_foreign_keys.append((args, kwargs))


def test_schema_inspection_and_type_affinity_fail_closed(monkeypatch):
    monkeypatch.setattr(
        migration_adoption,
        "_database_connection",
        lambda _operations: object(),
    )

    def reject_inspection(_connection):
        raise sa.exc.NoInspectionAvailable("synthetic recorder")

    monkeypatch.setattr(migration_adoption.sa, "inspect", reject_inspection)

    assert migration_adoption._schema_inspector(object()) is None
    assert migration_adoption._expected_primary_keys([]) is None
    assert migration_adoption._is_type_compatible(sa.Text(), sa.Integer()) is False


def test_generated_column_contract_rejects_unexpected_or_missing_shape():
    operations = _AdoptionRecorder()
    plain_column = sa.Column("value", sa.Text(), nullable=True)
    generated_column = sa.Column(
        "value",
        sa.Text(),
        sa.Computed("record_id", persisted=True),
    )

    with pytest.raises(RuntimeError, match="unexpected_generated_column"):
        migration_adoption._validate_computed_expression(
            operations,
            {"computed": {"persisted": True}},
            plain_column,
            "mrf",
            "adopted_record",
            "mrf.adopted_record.value",
        )
    with pytest.raises(RuntimeError, match="generated_column_missing"):
        migration_adoption._validate_computed_expression(
            operations,
            {"computed": None},
            generated_column,
            "mrf",
            "adopted_record",
            "mrf.adopted_record.value",
        )


def test_table_contract_rejects_missing_column_unique_and_foreign_key():
    inspector = _TableInspector([])
    expected_column = sa.Column("record_id", sa.String(64), nullable=False)
    with pytest.raises(RuntimeError, match="missing_columns"):
        migration_adoption._validate_columns(
            _AdoptionRecorder(),
            inspector,
            "mrf",
            "adopted_record",
            (expected_column,),
        )

    migration_adoption._validate_primary_key(
        inspector,
        "mrf",
        "adopted_record",
        (expected_column,),
        None,
    )
    with pytest.raises(RuntimeError, match="unique_constraint_mismatch"):
        migration_adoption._validate_unique_constraints(
            inspector,
            "mrf",
            "adopted_record",
            (sa.UniqueConstraint("record_id", name="uq_record"),),
        )

    foreign_key = sa.ForeignKeyConstraint(
        ["source_id"],
        ["mrf.source.source_id"],
        name="fk_source",
    )
    with pytest.raises(RuntimeError, match="foreign_key_mismatch"):
        migration_adoption._validate_foreign_keys(
            inspector,
            "mrf",
            "adopted_record",
            (foreign_key,),
        )


def test_foreign_key_target_rejects_mixed_remote_tables():
    constraint = sa.ForeignKeyConstraint(
        ["source_id", "other_id"],
        ["mrf.source.source_id", "mrf.other.other_id"],
        name="fk_mixed",
    )

    with pytest.raises(RuntimeError, match="foreign_key_target_invalid"):
        migration_adoption._expected_foreign_key_identity(constraint, "mrf")


def _foreign_key_record():
    return {
        "name": "fk_source",
        "constrained_columns": ["source_id"],
        "referred_schema": "mrf",
        "referred_table": "source",
        "referred_columns": ["source_id"],
        "options": {"ondelete": "CASCADE"},
    }


def _foreign_key_constraint():
    return sa.ForeignKeyConstraint(
        ["source_id"],
        ["mrf.source.source_id"],
        name="fk_source",
        ondelete="CASCADE",
    )


def test_foreign_key_adoption_matches_rejects_and_creates(monkeypatch):
    inspector = _TableInspector([])
    inspector.get_foreign_keys = lambda *_args, **_kwargs: [_foreign_key_record()]
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )
    operations = _AdoptionRecorder()
    migration_adoption.create_foreign_key_if_missing(
        operations,
        "adopted_record",
        _foreign_key_constraint(),
        schema="mrf",
    )
    assert operations.created_foreign_keys == []

    inspector.get_foreign_keys = lambda *_args, **_kwargs: [
        {**_foreign_key_record(), "referred_table": "wrong"}
    ]
    with pytest.raises(RuntimeError, match="foreign_key_mismatch"):
        migration_adoption.create_foreign_key_if_missing(
            operations,
            "adopted_record",
            _foreign_key_constraint(),
            schema="mrf",
        )

    inspector.get_foreign_keys = lambda *_args, **_kwargs: []
    migration_adoption.create_foreign_key_if_missing(
        operations,
        "adopted_record",
        _foreign_key_constraint(),
        schema="mrf",
    )
    assert len(operations.created_foreign_keys) == 1


def test_live_primary_key_and_column_nullability_are_exact(monkeypatch):
    inspector = _TableInspector(
        [_column_record("value", sa.Text(), nullable=False)],
        primary_key=("record_id",),
    )
    monkeypatch.setattr(
        migration_adoption,
        "_schema_inspector",
        lambda _operations: inspector,
    )

    assert migration_adoption.primary_key_columns(
        object(),
        "adopted_record",
        schema="mrf",
    ) == ("record_id",)
    assert migration_adoption.column_is_nullable(
        object(),
        "adopted_record",
        "value",
        schema="mrf",
    ) is False
    with pytest.raises(RuntimeError, match="missing_column"):
        migration_adoption.column_is_nullable(
            object(),
            "adopted_record",
            "missing",
            schema="mrf",
        )
