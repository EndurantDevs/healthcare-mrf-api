# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static dormancy proof for prospective persistence-candidate modules."""

from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys

from public_evidence.record_persistence_candidate_primitives import (
    fixed_persistence_candidate_authority,
)


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATHS = (
    ROOT / "public_evidence" / "record_persistence_candidate_primitives.py",
    ROOT / "public_evidence" / "record_persistence_candidate_contract.py",
)


def _import_roots(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Import):
        return tuple(alias.name.split(".", 1)[0] for alias in node.names)
    if isinstance(node, ast.ImportFrom) and node.module:
        return (node.module.split(".", 1)[0],)
    return ()


def _called_name(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _module_surface(path: Path) -> tuple[str, ast.Module, frozenset[str], frozenset[str]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    nodes = tuple(ast.walk(tree))
    roots = frozenset(root for node in nodes for root in _import_roots(node))
    calls = frozenset(
        name for node in nodes if (name := _called_name(node)) is not None
    )
    return source, tree, roots, calls


def test_candidate_modules_have_no_io_sql_or_runtime_import_surface() -> None:
    forbidden_roots = {
        "api",
        "asyncpg",
        "db",
        "os",
        "pathlib",
        "process",
        "psycopg",
        "requests",
        "socket",
        "sqlalchemy",
        "subprocess",
        "urllib",
    }
    forbidden_calls = {
        "connect",
        "delete",
        "execute",
        "executemany",
        "insert",
        "open",
        "publish",
        "remove",
        "rename",
        "run",
        "unlink",
        "write",
        "write_bytes",
        "write_text",
    }
    allowed_roots = set(sys.stdlib_module_names) | {"__future__", "public_evidence"}

    for path in MODULE_PATHS:
        module_source, tree, roots, calls = _module_surface(path)
        assert len(module_source.splitlines()) < 500
        assert not roots & forbidden_roots
        assert not roots - allowed_roots
        assert not calls & forbidden_calls
        assert not any(
            isinstance(node, (ast.AsyncFunctionDef, ast.Await))
            for node in ast.walk(tree)
        )
        lowered = module_source.casefold()
        assert "insert into" not in lowered
        assert "create table" not in lowered
        assert "select * from" not in lowered
        assert "alter table" not in lowered
        assert "encrypt" not in lowered


def test_candidate_modules_expose_no_writer_or_publisher_entrypoint() -> None:
    public_functions = set()
    for path in MODULE_PATHS:
        _source, tree, _roots, _calls = _module_surface(path)
        public_functions.update(
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not node.name.startswith("_")
        )
    assert not {
        "execute_adapter",
        "insert",
        "materialize",
        "migrate",
        "persist",
        "publish",
        "serve",
        "swap",
        "write",
    } & public_functions


def test_importing_candidate_contract_does_not_load_runtime_packages() -> None:
    script = """
import json
import sys
before = set(sys.modules)
from public_evidence import record_persistence_candidate_contract
introduced = set(sys.modules) - before
forbidden = sorted(
    name for name in introduced
    if name in {"api", "db", "process", "service"}
    or name.startswith(("api.", "db.", "process.", "service."))
)
print(json.dumps(forbidden))
raise SystemExit(bool(forbidden))
"""
    completed = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "[]"


def test_runtime_and_container_do_not_wire_candidate_contract() -> None:
    runtime_paths = [ROOT / "main.py"]
    for package_name in ("api", "db", "process", "service"):
        runtime_paths.extend((ROOT / package_name).rglob("*.py"))
    for path in runtime_paths:
        assert "record_persistence_candidate" not in path.read_text(encoding="utf-8")
    assert "record_persistence_candidate" not in (ROOT / "Dockerfile").read_text(
        encoding="utf-8"
    )


def test_candidate_authority_explicitly_denies_storage_and_claim_promotion() -> None:
    authority = fixed_persistence_candidate_authority()
    assert authority.lifecycle_state == "prospective_row_shape_only"
    assert authority.normalized_record_validated is True
    assert authority.row_shape_frozen is True
    assert authority.source_link_order_verified is True
    assert authority.exactly_one_typed_row_verified is True
    assert authority.row_digests_recomputed is True
    assert authority.positive_evidence_only is True
    assert authority.storage_schema_state == "not_defined"
    assert authority.database_write_state == "not_executed"
    assert authority.database_io_authority == "none"
    assert authority.writer_authority == "none"
    assert authority.migration_authority == "none"
    assert authority.adapter_execution_authority == "none"
    assert authority.serving_authority == "none"
    assert authority.current_pointer_authority == "none"
    denied_claims = (
        authority.database_row_presence_verified,
        authority.database_constraint_parity_verified,
        authority.source_bytes_authenticated,
        authority.complete_inventory_scan_verified,
        authority.source_authenticity_claimed,
        authority.legal_ownership_claimed,
        authority.employment_claimed,
        authority.facility_ownership_claimed,
        authority.exact_rate_site_claimed,
        authority.payer_confirmed_site_claimed,
        authority.site_match_claimed,
        authority.confidence_claimed,
        authority.independence_claimed,
        authority.publication_enabled,
        authority.replacement_enabled,
        authority.deletion_enabled,
        authority.retirement_enabled,
        authority.supersession_enabled,
    )
    assert denied_claims == (False,) * len(denied_claims)
