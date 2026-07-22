# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Enforce the one-way dependency boundary around projection core."""

from __future__ import annotations

import ast
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
GENERIC_PROJECTION_FILES = (
    REPOSITORY_ROOT
    / "alembic/versions/20260721170000_provider_directory_physical_projection.py",
    REPOSITORY_ROOT / "process/provider_directory_physical_projection.py",
    *sorted((REPOSITORY_ROOT / "process").glob("provider_directory_projection_*.py")),
)
GENERIC_PROCESS_MODULE_PREFIX = "process.provider_directory_projection_"
FORBIDDEN_FILE_MODULE_PREFIXES = (
    "aiofiles",
    "pathlib",
    "urllib",
    "process.provider_directory_retained_private",
)
ADAPTER_IDENTITY_TOKENS = (
    "aetna",
    "devoted",
    "flex.optum.com",
    "healthlx.com",
    "iowa medicaid",
    "netsmart",
    "optum",
    "pennsylvania medicaid",
    "scan health",
    "sbcounty",
    "smchealth",
    "simpra",
    "uhc",
    "unitedhealth",
)
FORBIDDEN_STORAGE_TOKENS = (
    "artifact_path",
    "manifest_path",
    "private_item_locator",
    "source_locator",
    "storage_locator",
)
FORBIDDEN_OS_CALLS = {"open", "pread", "read", "readv"}


def _imports(source: str) -> tuple[str, ...]:
    tree = ast.parse(source)
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)
    return tuple(imported_modules)


def _forbidden_file_calls(source: str) -> tuple[str, ...]:
    forbidden_calls: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id == "open":
            forbidden_calls.append("open")
        elif (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
            and node.func.attr in FORBIDDEN_OS_CALLS
        ):
            forbidden_calls.append(f"os.{node.func.attr}")
    return tuple(forbidden_calls)


def test_generic_projection_core_has_no_adapter_specific_dependencies() -> None:
    """Adapters may depend inward; generic projection must never depend outward."""

    assert GENERIC_PROJECTION_FILES
    for source_path in GENERIC_PROJECTION_FILES:
        generic_source = source_path.read_text(encoding="utf-8")
        imported_modules = _imports(generic_source)
        adapter_imports = tuple(
            module_name
            for module_name in imported_modules
            if module_name.startswith("process.")
            and not module_name.startswith(GENERIC_PROCESS_MODULE_PREFIX)
        )
        forbidden_file_imports = tuple(
            module_name
            for module_name in imported_modules
            if module_name.startswith(FORBIDDEN_FILE_MODULE_PREFIXES)
        )
        assert adapter_imports == (), (
            source_path.relative_to(REPOSITORY_ROOT),
            adapter_imports,
        )
        assert forbidden_file_imports == (), (
            source_path.relative_to(REPOSITORY_ROOT),
            forbidden_file_imports,
        )
        assert _forbidden_file_calls(generic_source) == (), (
            source_path.relative_to(REPOSITORY_ROOT),
            _forbidden_file_calls(generic_source),
        )
        lowered_source = generic_source.lower()
        forbidden_tokens = tuple(
            token for token in ADAPTER_IDENTITY_TOKENS if token in lowered_source
        )
        assert forbidden_tokens == (), (
            source_path.relative_to(REPOSITORY_ROOT),
            forbidden_tokens,
        )
        forbidden_storage_tokens = tuple(
            token for token in FORBIDDEN_STORAGE_TOKENS if token in lowered_source
        )
        assert forbidden_storage_tokens == (), (
            source_path.relative_to(REPOSITORY_ROOT),
            forbidden_storage_tokens,
        )
