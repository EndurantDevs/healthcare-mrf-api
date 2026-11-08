# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native lifecycle proofs retain one CI entrypoint and their original fixtures."""

import ast
import importlib
from collections import defaultdict
from fnmatch import fnmatch
from pathlib import Path

from tests.custom_import_native_enrollment import NATIVE_SUPPORT_ENTRYPOINTS

ROOT = Path(__file__).resolve().parents[1]
ENROLLED_ENTRYPOINTS = {
    "tests/test_custom_import_build_source_postgres.py",
    "tests/test_custom_import_verification_root_ranges_postgres.py",
}


def _module(path):
    return ast.parse((ROOT / path).read_text())


def _proof_functions(tree):
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")
    }


def test_native_support_is_not_collected_by_main(pytestconfig):
    assert set(NATIVE_SUPPORT_ENTRYPOINTS) == {
        "tests.custom_import_retirement_postgres",
        "tests.custom_import_retirement_inventory_postgres",
        "tests.custom_import_post_seal_audit_postgres",
    }
    assert set(NATIVE_SUPPORT_ENTRYPOINTS.values()) == ENROLLED_ENTRYPOINTS
    for module in NATIVE_SUPPORT_ENTRYPOINTS:
        filename = module.rsplit(".", 1)[1] + ".py"
        assert not any(fnmatch(filename, pattern) for pattern in pytestconfig.getini("python_files"))


def _add_direct_exports(tree, relative, exports):
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in NATIVE_SUPPORT_ENTRYPOINTS:
            for alias in node.names:
                if (alias.asname or alias.name).startswith("test_"):
                    exports[node.module, alias.name].append((relative, node))


def _collect_entrypoints():
    exports = defaultdict(list)
    for path in (ROOT / "tests").glob("test_*.py"):
        relative = path.relative_to(ROOT).as_posix()
        tree = _module(relative)
        _add_direct_exports(tree, relative, exports)
        for function in _proof_functions(tree).values():
            for node in function.body:
                if isinstance(node, ast.ImportFrom) and node.module in NATIVE_SUPPORT_ENTRYPOINTS:
                    assert len(node.names) == 1 and node.names[0].asname == "verify"
                    exports[node.module, node.names[0].name].append((relative, function))
    return exports


def test_native_proofs_execute_once_with_exact_inputs():
    exports = _collect_entrypoints()
    expected_functions = set()
    for module, entrypoint in NATIVE_SUPPORT_ENTRYPOINTS.items():
        tree = _module(module.replace(".", "/") + ".py")
        functions = _proof_functions(tree)
        assert functions
        for name, original in functions.items():
            expected_functions.add((module, name))
            assert len(exports[module, name]) == 1
            actual_entrypoint, wrapper = exports[module, name][0]
            assert actual_entrypoint == entrypoint and type(wrapper) is type(original)
            assert wrapper.name == name and ast.dump(wrapper.args) == ast.dump(original.args)
            assert [ast.dump(mark) for mark in wrapper.decorator_list] == [
                ast.dump(mark) for mark in original.decorator_list
            ]
            assert len(wrapper.body) == 2 and isinstance(wrapper.body[1], ast.Expr)
            invocation = wrapper.body[1].value
            if isinstance(original, ast.AsyncFunctionDef):
                assert isinstance(invocation, ast.Await)
                invocation = invocation.value
            assert isinstance(invocation, ast.Call)
            call = invocation
            assert isinstance(call.func, ast.Name) and call.func.id == "verify" and not call.keywords
            assert [ast.unparse(argument) for argument in call.args] == [
                argument.arg for argument in original.args.args
            ]
    assert set(exports) == expected_functions


def test_native_support_keeps_original_fixture_imports():
    for module in NATIVE_SUPPORT_ENTRYPOINTS:
        assert importlib.import_module(module).__name__ == module
