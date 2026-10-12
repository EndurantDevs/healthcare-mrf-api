"""Compose the fixed, independently accepted public SDK source in ordinary builds."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

DESCRIPTOR = "config/drug_snapshot_runtime.json"
BAKED_DESCRIPTOR = "healthporta/build-identity/drug-runtime-source.json"
REPOSITORY = "EndurantDevs/drug-api"
SOURCE_URL = f"https://github.com/{REPOSITORY}.git"
SOURCE = re.compile(r"[0-9a-f]{40}\Z")
SDK_PREFIX = "db/drug_snapshot_runtime/"
SDK_FILES = frozenset(
    SDK_PREFIX + name
    for name in (
        "__init__.py",
        "label.py",
        "model_copy.py",
        "model_indexes.py",
        "ndc.py",
        "ndc_publication.py",
        "projection_catalog.py",
        "publication.py",
    )
)
IDENTITY_FILES = {
    "source_sha": "drug-source-commit",
    "source_tree_sha": "drug-source-tree",
    "base_sha": "drug-source-base",
}


def _object(pairs):
    fields_by_name = {}
    for key, value in pairs:
        if key in fields_by_name:
            raise ValueError("SDK source contains duplicate fields")
        fields_by_name[key] = value
    return fields_by_name


def parse(content):
    """Accept only the bounded fixed public repository and source identity."""
    if len(content) > 4096:
        raise ValueError("SDK source descriptor exceeds its bound")
    value = json.loads(content, object_pairs_hook=_object)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema", "repository", "source_sha", "source_tree_sha", "base_sha"}
        or value["schema"] != "drug_snapshot_runtime_source_v1"
        or value["repository"] != REPOSITORY
    ):
        raise ValueError("SDK source descriptor differs")
    for key in IDENTITY_FILES:
        if not isinstance(value[key], str) or not SOURCE.fullmatch(value[key]) or value[key] == "0" * 40:
            raise ValueError("SDK source identity differs")
    return value


def build_arguments(value):
    """Pass the selected public commit, tree, and parent to an ordinary build."""
    return tuple(
        item
        for field, key in (
            ("DRUG_SOURCE_COMMIT", "source_sha"),
            ("DRUG_SOURCE_TREE", "source_tree_sha"),
            ("DRUG_SOURCE_BASE", "base_sha"),
        )
        for item in ("--build-arg", field + "=" + value[key])
    )


def _git(directory, *arguments):
    environment_by_name = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(directory),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
    }
    try:
        return subprocess.run(
            ["git", "-C", str(directory), "-c", "credential.helper=", *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=180,
            env=environment_by_name,
        ).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise ValueError("public SDK Git verification failed") from error


def _entries(directory, source_sha, path):
    entries_by_path = {}
    for record in _git(directory, "ls-tree", "-rz", source_sha, "--", path).split(b"\0"):
        if not record:
            continue
        header, separator, name = record.partition(b"\t")
        if not separator:
            raise ValueError("SDK source inventory differs")
        fields = header.decode("ascii").split(" ")
        decoded_name = name.decode("utf-8")
        if len(fields) != 3 or decoded_name in entries_by_path:
            raise ValueError("SDK source inventory differs")
        mode, kind, oid = fields
        if mode != "100644" or kind != "blob" or not SOURCE.fullmatch(oid):
            raise ValueError("SDK source is not an ordinary tracked file")
        entries_by_path[decoded_name] = oid
    return entries_by_path


def _source_blobs(directory, value):
    source_sha = value["source_sha"]
    _git(directory, "init", "--quiet")
    _git(directory, "fetch", "--quiet", "--no-tags", "--depth=2", SOURCE_URL, source_sha)
    fetched = _git(directory, "rev-parse", "--verify", "FETCH_HEAD^{commit}").decode("ascii").strip()
    tree = _git(directory, "rev-parse", "--verify", "FETCH_HEAD^{tree}").decode("ascii").strip()
    # Read the commit object: rev-list may omit a parent at a shallow boundary.
    header = _git(directory, "cat-file", "-p", fetched).split(b"\n\n", 1)[0].splitlines()
    parents = [line.removeprefix(b"parent ").decode("ascii") for line in header if line.startswith(b"parent ")]
    if (fetched, tree, parents) != (source_sha, value["source_tree_sha"], [value["base_sha"]]):
        raise ValueError("SDK source commit, tree, or sole parent differs")
    sdk = _entries(directory, source_sha, "db/drug_snapshot_runtime")
    license_files = _entries(directory, source_sha, "LICENSE")
    if set(sdk) != SDK_FILES or set(license_files) != {"LICENSE"}:
        raise ValueError("SDK source or license inventory differs")
    return {name.removeprefix(SDK_PREFIX): _git(directory, "cat-file", "blob", oid) for name, oid in sdk.items()}, _git(
        directory, "cat-file", "blob", license_files["LICENSE"]
    )


def compose(source_sha, tree_sha, base_sha, *, root=Path("/opt"), temporary_parent=None):
    """Verify Git objects, install only SDK blobs and license, then drop the checkout."""
    identity = root / BAKED_DESCRIPTOR
    source_descriptor = parse(identity.read_bytes())
    if (source_sha, tree_sha, base_sha) not in (
        (source_descriptor["source_sha"], source_descriptor["source_tree_sha"], source_descriptor["base_sha"]),
        ("", "", ""),
    ):
        raise ValueError("SDK source build arguments differ")
    with tempfile.TemporaryDirectory(prefix="drug-sdk-source-", dir=temporary_parent) as temporary:
        sdk, license_text = _source_blobs(Path(temporary), source_descriptor)
    package = root / "drug_snapshot_runtime"
    license_path = root / "licenses/drug_snapshot_runtime-LICENSE"
    if package.exists() or license_path.exists():
        raise ValueError("SDK destination is already populated")
    package.mkdir(mode=0o755)
    for name, content in sdk.items():
        sdk_path = package / name
        with sdk_path.open("xb") as output:
            output.write(content)
        sdk_path.chmod(0o444)
    license_path.parent.mkdir(mode=0o755, exist_ok=True)
    with license_path.open("xb") as output:
        output.write(license_text)
    license_path.chmod(0o444)
    for field, filename in IDENTITY_FILES.items():
        marker = identity.parent / filename
        with marker.open("x") as output:
            output.write(source_descriptor[field] + "\n")
        marker.chmod(0o444)
    identity.chmod(0o444)


def verify_baked(source_sha, tree_sha, base_sha, *, root=Path("/opt")):
    """Check read-only source markers and import the SDK without application startup."""
    identity = root / BAKED_DESCRIPTOR
    value = parse(identity.read_bytes())
    if (source_sha, tree_sha, base_sha) not in (
        (value["source_sha"], value["source_tree_sha"], value["base_sha"]),
        ("", "", ""),
    ):
        raise ValueError("SDK source build arguments differ")
    for path, content in (
        (identity, identity.read_bytes()),
        *((identity.parent / filename, (value[field] + "\n").encode()) for field, filename in IDENTITY_FILES.items()),
    ):
        info = os.lstat(path)
        if (
            not stat.S_ISREG(info.st_mode)
            or (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode), info.st_nlink) != (0, 0, 0o444, 1)
            or path.read_bytes() != content
        ):
            raise ValueError("SDK baked source identity differs")
    if not (root / "licenses/drug_snapshot_runtime-LICENSE").is_file():
        raise ValueError("SDK license is absent")
    sys.path.insert(0, str(root))
    from drug_snapshot_runtime import label, model_copy, model_indexes, ndc_publication, publication

    if "sanic" in sys.modules or "db.connection" in sys.modules:
        raise ValueError("SDK import initializes an application")
    assert label and model_copy and model_indexes and ndc_publication and publication


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "compose":
        compose(*sys.argv[2:])
    elif len(sys.argv) == 5 and sys.argv[1] == "verify-baked":
        verify_baked(*sys.argv[2:])
    elif len(sys.argv) != 1:
        raise SystemExit("SDK source selector does not accept caller arguments")
    else:
        dependency = parse((Path(__file__).resolve().parents[1] / DESCRIPTOR).read_bytes())
        print("\n".join(build_arguments(dependency)))
