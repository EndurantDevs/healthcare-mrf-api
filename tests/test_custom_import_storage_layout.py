# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Host-only fixed-layout checks; these do not establish native DB proof."""

from __future__ import annotations

import pytest
from sqlalchemy import ForeignKeyConstraint, UniqueConstraint, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import AddConstraint

from db.models.custom_import import CustomImportGeneration, CustomImportRootRecord, CustomImportWinner
from process.custom_import import storage_layout as storage


@pytest.mark.parametrize("family_id", [True, False, None, "1", 0, -1, 2**63])
def test_snapshot_identity_never_accepts_caller_names(family_id):
    with pytest.raises(ValueError, match="positive bigint"):
        storage.snapshot_schema(family_id)


def test_snapshot_models_keep_shapes_ids_and_canonical_metadata():
    """A candidate has identical columns but no copied hot FK constraints."""

    canonical_fk_counts_by_model = {model: len(model.__table__.foreign_keys) for model in storage.SNAPSHOT_MODELS}
    tables_by_name = storage.snapshot_tables(17)
    assert len(tables_by_name) == 15
    for model in storage.SNAPSHOT_MODELS:
        original_table = model.__table__
        snapshot_table = tables_by_name[model.__tablename__]
        assert snapshot_table is not original_table
        assert snapshot_table.schema == "ci_snapshot_17"
        assert tuple(snapshot_table.c.keys()) == tuple(original_table.c.keys())
        assert tuple(column.nullable for column in snapshot_table.c) == tuple(
            column.nullable for column in original_table.c
        )
        assert not snapshot_table.foreign_keys
        assert not any(isinstance(constraint, ForeignKeyConstraint) for constraint in snapshot_table.constraints)
        assert len(original_table.foreign_keys) == canonical_fk_counts_by_model[model]
    with pytest.raises(TypeError):
        tables_by_name["untrusted_table"] = None


def test_load_plan_has_only_native_constraints_and_essential_unique_indexes():
    """LIKE keeps canonical sequence defaults instead of creating local IDs."""

    statements = storage.snapshot_load_statements(17)
    assert statements[0] == 'CREATE SCHEMA "ci_snapshot_17"'
    creates = [statement for statement in statements if statement.startswith("CREATE TABLE")]
    assert len(creates) == 15
    assert all("INCLUDING DEFAULTS INCLUDING CONSTRAINTS" in statement for statement in creates)
    assert all("INCLUDING INDEXES" not in statement for statement in creates)
    assert not any(
        "FOREIGN KEY" in statement or "TRIGGER" in statement or "BIGSERIAL" in statement for statement in statements
    )
    index_statements = [
        statement for statement in statements if statement.startswith("CREATE") and "INDEX" in statement
    ]
    assert len(index_statements) == 4
    assert all(statement.startswith("CREATE UNIQUE INDEX") for statement in index_statements)
    assert not any("scalar_text_idx" in statement or "family_pending_idx" in statement for statement in statements)


def test_load_omits_only_unique_constraints_already_proved_by_primary_key():
    """Do not maintain redundant FK-target indexes on an FK-free snapshot."""

    statements = storage.snapshot_load_statements(17)
    redundant_names = set()
    for snapshot_table in storage.snapshot_tables(17).values():
        primary_key_columns = frozenset(snapshot_table.primary_key.columns.keys())
        assert primary_key_columns
        for constraint in snapshot_table.constraints:
            if not isinstance(constraint, UniqueConstraint):
                continue
            is_redundant = primary_key_columns.issubset(constraint.columns.keys())
            is_installed = str(AddConstraint(constraint).compile(dialect=postgresql.dialect())) in statements
            assert is_installed is not is_redundant
            if is_redundant:
                redundant_names.add(constraint.name)
    assert len(redundant_names) == 7


def test_phase_and_serving_indexes_never_target_canonical_or_another_snapshot():
    phase_statements = tuple(
        statement
        for phase in ("admission", "graph", "output")
        for statement in storage.snapshot_phase_index_statements(17, phase)
    )
    serving_statements = storage.snapshot_serving_index_statements(17)
    assert len(phase_statements) == 11
    assert len(serving_statements) == 10
    assert all(
        "ON ci_snapshot_17.custom_import_" in statement for statement in (*phase_statements, *serving_statements)
    )
    assert all("WHERE value_state = 'value'" in statement for statement in serving_statements)
    assert storage.snapshot_serving_index_statements(17) == serving_statements
    with pytest.raises(ValueError, match="unsupported"):
        storage.snapshot_phase_index_statements(17, "caller_sql")


def test_old_request_tables_remain_pinned_after_another_family_is_prepared():
    """No global rename or mutable table alias can redirect an in-flight read."""

    old_table = storage.snapshot_tables(17)["custom_import_root_revision"]
    next_table = storage.snapshot_tables(18)["custom_import_root_revision"]
    old_query = str(select(old_table).compile(dialect=postgresql.dialect()))
    next_query = str(select(next_table).compile(dialect=postgresql.dialect()))
    assert "ci_snapshot_17.custom_import_root_revision" in old_query
    assert "ci_snapshot_18.custom_import_root_revision" not in old_query
    assert "ci_snapshot_18.custom_import_root_revision" in next_query


def test_hot_model_aliases_never_redirect_control_or_global_interner_tables():
    """Use an immutable request binding, not a session-wide schema rewrite."""

    models_by_canonical = storage.snapshot_models(17)
    winner = models_by_canonical[CustomImportWinner]
    statement = select(winner).join(
        CustomImportGeneration, CustomImportGeneration.generation_id == winner.generation_id
    )
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    assert "ci_snapshot_17.custom_import_winner" in compiled
    assert "mrf.custom_import_generation" in compiled
    assert CustomImportRootRecord.__table__.schema == CustomImportWinner.__table__.schema == "mrf"
    assert CustomImportGeneration not in models_by_canonical
    storage.snapshot_models(18)
    assert "ci_snapshot_17.custom_import_winner" in str(statement.compile(dialect=postgresql.dialect()))
    with pytest.raises(TypeError):
        models_by_canonical[CustomImportGeneration] = None
