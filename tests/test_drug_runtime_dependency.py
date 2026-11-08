"""The ordinary build copies only the fixed, verified public SDK source."""

import ast
import importlib.util
import json
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/drug_runtime_dependency.py"
SPEC = importlib.util.spec_from_file_location("dependency", SCRIPT)
dependency = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dependency)


def selected():
    return {
        "schema": "drug_snapshot_runtime_source_v1",
        "repository": dependency.REPOSITORY,
        "source_sha": "a" * 40,
        "source_tree_sha": "b" * 40,
        "base_sha": "c" * 40,
    }


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("schema", "foreign"),
        ("repository", "foreign/repository"),
        ("source_sha", "0" * 40),
        ("source_tree_sha", True),
        ("base_sha", "short"),
        ("image", "unselected"),
    ],
)
def test_public_source_descriptor_is_closed(field, replacement):
    value = selected()
    value[field] = replacement
    with pytest.raises(ValueError):
        dependency.parse(json.dumps(value).encode())


def test_fixed_arguments_and_real_descriptor_use_only_public_source_identity():
    value = selected()
    assert dependency.parse(json.dumps(value).encode()) == value
    assert dependency.build_arguments(value) == (
        "--build-arg",
        "DRUG_SOURCE_COMMIT=" + value["source_sha"],
        "--build-arg",
        "DRUG_SOURCE_TREE=" + value["source_tree_sha"],
        "--build-arg",
        "DRUG_SOURCE_BASE=" + value["base_sha"],
    )
    real = dependency.parse((ROOT / dependency.DESCRIPTOR).read_bytes())
    assert real == {
        "schema": "drug_snapshot_runtime_source_v1",
        "repository": dependency.REPOSITORY,
        "source_sha": "aa7e6f7b882f12f070d3f7f648d151b81b56436d",
        "source_tree_sha": "d90ace1c7063eb958eb371a697e23d0facd1af16",
        "base_sha": "0b2c50fb8e11d457453ebb733cf494842dd6068a",
    }


@pytest.mark.parametrize("content", [b"{}", b"[]", b'{"same":1,"same":2}', b" " * 4097])
def test_descriptor_refuses_missing_duplicate_or_unbounded_fields(content):
    with pytest.raises(ValueError):
        dependency.parse(content)


def test_fixed_selector_refuses_absent_tuple_and_arbitrary_path(tmp_path):
    script = tmp_path / "scripts/drug_runtime_dependency.py"
    script.parent.mkdir()
    script.write_bytes(SCRIPT.read_bytes())
    result = subprocess.run([sys.executable, str(script)], capture_output=True)
    assert result.returncode != 0 and not result.stdout
    result = subprocess.run([sys.executable, str(script), "other.json"], capture_output=True)
    assert result.returncode != 0 and not result.stdout
    assert b"caller arguments" in result.stderr
    (tmp_path / "config").mkdir()
    (tmp_path / dependency.DESCRIPTOR).write_text(json.dumps(selected()))
    result = subprocess.run([sys.executable, str(script)], capture_output=True, check=True)
    assert result.stdout.decode().splitlines() == list(dependency.build_arguments(selected()))


def fake_git(source_descriptor, *, changed=None):
    blob_oid_by_path = {path: str(index).zfill(40) for index, path in enumerate(sorted(dependency.SDK_FILES), 1)}
    blob_oid_by_path["LICENSE"] = "f" * 40
    by_oid = {oid: path.encode() for path, oid in blob_oid_by_path.items()}

    def run(_directory, *arguments):
        if arguments[0] in {"init", "fetch"}:
            if changed == "fetch" and arguments[0] == "fetch":
                raise ValueError("public SDK Git verification failed")
            return b""
        if arguments[0] == "rev-parse":
            if "commit" in arguments[2]:
                return ("d" * 40 if changed == "commit" else source_descriptor["source_sha"]).encode() + b"\n"
            return ("d" * 40 if changed == "tree" else source_descriptor["source_tree_sha"]).encode() + b"\n"
        if arguments[:2] == ("cat-file", "-p"):
            parent = "d" * 40 if changed == "parent" else source_descriptor["base_sha"]
            return f"tree {source_descriptor['source_tree_sha']}\nparent {parent}\n\nauthored\n".encode()
        if arguments[0] == "ls-tree":
            paths = sorted(blob_oid_by_path) if arguments[-1] == "db/drug_snapshot_runtime" else ["LICENSE"]
            if arguments[-1] == "db/drug_snapshot_runtime":
                paths = [path for path in paths if path.startswith(dependency.SDK_PREFIX)]
                if changed == "unrelated":
                    paths.append("db/drug_snapshot_runtime/extra.py")
                    blob_oid_by_path[paths[-1]] = "e" * 40
                if changed == "missing":
                    paths.pop()
            mode = "120000" if changed == "symlink" else "100644"
            return b"".join(f"{mode} blob {blob_oid_by_path[path]}\t{path}\0".encode() for path in paths)
        if arguments[:2] == ("cat-file", "blob"):
            return by_oid[arguments[2]]
        raise AssertionError(arguments)

    return run


def prepared_root(tmp_path, value):
    root = tmp_path / "image"
    identity = root / dependency.BAKED_DESCRIPTOR
    identity.parent.mkdir(parents=True)
    identity.write_text(json.dumps(value))
    return root


@pytest.mark.parametrize("explicit_arguments", [True, False])
def test_sdk_blobs_license_and_temp_cleanup(tmp_path, monkeypatch, explicit_arguments):
    value = selected()
    root = prepared_root(tmp_path, value)
    monkeypatch.setattr(dependency, "_git", fake_git(value))
    arguments = tuple(value[key] for key in dependency.IDENTITY_FILES) if explicit_arguments else ("", "", "")
    dependency.compose(*arguments, root=root, temporary_parent=tmp_path)
    assert {path.name for path in (root / "drug_snapshot_runtime").iterdir()} == {
        path.removeprefix(dependency.SDK_PREFIX) for path in dependency.SDK_FILES
    }
    assert (root / "licenses/drug_snapshot_runtime-LICENSE").read_bytes() == b"LICENSE"
    assert not (root / "db").exists() and not (root / "api").exists()
    assert not list(tmp_path.glob("drug-sdk-source-*"))
    for marker in (
        root / dependency.BAKED_DESCRIPTOR,
        *(root / "healthporta/build-identity" / name for name in dependency.IDENTITY_FILES.values()),
    ):
        assert stat.S_IMODE(marker.stat().st_mode) == 0o444


@pytest.mark.parametrize("changed", ["fetch", "commit", "tree", "parent", "missing", "unrelated", "symlink"])
def test_wrong_source_or_sdk_inventory_fails_closed_and_cleans_temp(tmp_path, monkeypatch, changed):
    value = selected()
    root = prepared_root(tmp_path, value)
    monkeypatch.setattr(dependency, "_git", fake_git(value, changed=changed))
    with pytest.raises(ValueError):
        dependency.compose(
            value["source_sha"], value["source_tree_sha"], value["base_sha"], root=root, temporary_parent=tmp_path
        )
    assert not list(tmp_path.glob("drug-sdk-source-*"))
    assert not (root / "drug_snapshot_runtime").exists()


@pytest.mark.parametrize("field", ["source_sha", "source_tree_sha", "base_sha"])
@pytest.mark.parametrize("replacement", ["foreign", ""])
def test_conflicting_build_argument_refuses_before_git(tmp_path, monkeypatch, field, replacement):
    value = selected()
    root = prepared_root(tmp_path, value)
    monkeypatch.setattr(dependency, "_git", lambda *_args: pytest.fail("Git must not run"))
    arguments_by_field = {key: value[key] for key in dependency.IDENTITY_FILES}
    arguments_by_field[field] = replacement
    with pytest.raises(ValueError, match="build arguments differ"):
        dependency.compose(
            arguments_by_field["source_sha"],
            arguments_by_field["source_tree_sha"],
            arguments_by_field["base_sha"],
            root=root,
        )


def test_ordinary_default_build_composes_source_in_final_venv_without_private_parent():
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert "COPY config/drug_snapshot_runtime.json" in dockerfile
    assert "drug_runtime_dependency.py compose" in dockerfile
    assert "drug_runtime_dependency.py verify-baked" in dockerfile
    assert "FROM ${DRUG_RUNTIME_IMAGE}" not in dockerfile
    assert "COPY --from=drug-runtime" not in dockerfile
    assert "--target" not in dockerfile
    baked_check = next(
        node
        for node in ast.parse(SCRIPT.read_text()).body
        if isinstance(node, ast.FunctionDef) and node.name == "verify_baked"
    )
    imported_modules = {
        name.name
        for node in ast.walk(baked_check)
        if isinstance(node, ast.ImportFrom) and node.module == "drug_snapshot_runtime"
        for name in node.names
    }
    assert {"label", "model_copy", "model_indexes", "ndc_publication", "publication"} <= imported_modules
