# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Installed SQL finds history and PTG snapshot tables by name, never by a stored object id."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from db.migration_provider_directory_dataset_candidates import candidate_functions

_ROOT = Path(__file__).resolve().parents[1]


def _migration():
    path = _ROOT / "alembic/versions/20261126120000_stored_id_lookups_by_name.py"
    spec = importlib.util.spec_from_file_location("stored_id_lookups_by_name", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sources_never_compare_stored_ids_with_the_catalog():
    dataset = candidate_functions("mrf")
    assert "'original_oid'])::oid" not in dataset
    assert dataset.count("'pd_dataset_history_' || (") == 2
    for name in ("ptg_snapshot_candidates.sql", "ptg_snapshot_gc.sql"):
        source = (_ROOT / "db/sql" / name).read_text()
        assert "_identity' USING ERRCODE" not in source and "WHERE table_oid=p_relation" not in source


def test_upgrade_rewrites_old_bodies_and_refuses_unknown_ones():
    migration = _migration()
    old_guard = "BEGIN\n  SELECT 1 WHERE conrelid=(plan#>>ARRAY['tables',parent_name,'original_oid'])::oid;\nEND"
    unknown = "BEGIN\n  RAISE EXCEPTION 'ptg_snapshot_gc_identity' USING ERRCODE='55000';\nEND"
    rows = [
        {"oid": 1, "nspname": "mrf", "proname": "guard_pd_dataset_candidate_parent", "definition": old_guard},
        {"oid": 2, "nspname": "mrf", "proname": "delete_ptg_snapshot_history", "definition": unknown},
    ]
    connection = MagicMock()
    connection.execute.return_value = SimpleNamespace(mappings=lambda: rows)
    migration.op = SimpleNamespace(get_bind=lambda: connection)
    with pytest.raises(RuntimeError, match="stored_id_lookup_unrecognized:mrf.delete_ptg_snapshot_history"):
        migration.upgrade()
    rewritten = connection.exec_driver_sql.call_args.args[0]
    assert "to_regclass(format('%I.%I','mrf'," in rewritten and "::oid" not in rewritten
