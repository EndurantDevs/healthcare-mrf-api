# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts" / "devops"


def _load_script(module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_DIR / f"{module_name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ptg_compare_sql_helper_returns_text_clause():
    module = _load_script("ptg2_compare_snapshots")

    clause = module._sql_text("SELECT 1")

    assert str(clause) == "SELECT 1"


def test_ptg_devops_scripts_import_without_database_connection():
    for module_name in (
        "ptg2_compare_snapshots",
        "ptg2_rebuild_snapshot_from_options",
        "ptg2_remove_source_snapshot",
    ):
        assert _load_script(module_name).__name__ == module_name
