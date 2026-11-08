"""Accept only provenance-complete PostgreSQL coverage unions without policy changes."""

from __future__ import annotations

import json
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import coverage
import pytest

from tests.test_coverage_forecast import (
    BASE_SHA,
    HEAD_SHA,
    _artifact_baseline,
    artifacts,
    combine,
)


def _write_json(path: Path, document: dict) -> None:
    """Write deterministic synthetic metadata for digest-bound fixtures."""

    path.write_text(json.dumps(document, sort_keys=True) + "\n", encoding="utf-8")


def _cohort(root: Path, originals: Path, destination: Path) -> dict:
    """Union original databases while retaining every original sidecar byte."""

    destination.mkdir()
    combined = coverage.CoverageData(basename=str(destination / artifacts.POSTGRES_COVERAGE_NAME))
    inputs_by_shard = {}
    for shard in artifacts.SHARD_SPEC_BY_KIND["postgres"]["shards"]:
        coverage_name, provenance_name = artifacts.shard_file_names("postgres", shard)
        original = coverage.CoverageData(basename=str(originals / coverage_name))
        original.read()
        combined.update(original)
        shutil.copyfile(originals / provenance_name, destination / provenance_name)
        inputs_by_shard[shard] = {
            "coverage_sha256": artifacts._sha256_file(originals / coverage_name),
            "provenance_sha256": artifacts._sha256_file(originals / provenance_name),
        }
    combined.write()
    manifest_by_field = {
        "schema": "python-coverage-cohort/v1",
        "kind": "postgres",
        "head_sha": HEAD_SHA,
        "base_sha": BASE_SHA,
        "baseline_sha256": artifacts._sha256_file(root / artifacts.BASELINE_NAME),
        "coverage_version": artifacts._coverage_version(),
        "coverage_file": artifacts.POSTGRES_COVERAGE_NAME,
        "coverage_sha256": artifacts._sha256_file(destination / artifacts.POSTGRES_COVERAGE_NAME),
        "inputs": inputs_by_shard,
    }
    _write_json(destination / artifacts.POSTGRES_COHORT_NAME, manifest_by_field)
    return manifest_by_field


@pytest.fixture(scope="module")
def cohort_template(tmp_path_factory) -> Path:
    """Build the full fixed topology once without PostgreSQL or subprocesses."""

    root = tmp_path_factory.mktemp("coverage-cohort")
    _write_json(root / artifacts.BASELINE_NAME, _artifact_baseline())
    originals = root / "originals"
    _write_branch_family(root, originals, "postgres")
    _cohort(root, originals, root / "cohort")
    return root


@pytest.fixture
def cohort_input(tmp_path: Path, cohort_template: Path) -> tuple[Path, Path, dict]:
    """Give each rejection case an isolated copy of the small transport payload."""

    shutil.copyfile(cohort_template / artifacts.BASELINE_NAME, tmp_path / artifacts.BASELINE_NAME)
    directory = tmp_path / "cohort"
    shutil.copytree(cohort_template / "cohort", directory)
    manifest = json.loads((directory / artifacts.POSTGRES_COHORT_NAME).read_text())
    return tmp_path, directory, manifest


def _verify(fixture: tuple[Path, Path, dict]) -> list[Path]:
    """Use the same entry point as ordinary and recovery forecast aggregation."""

    root, directory, _ = fixture
    return artifacts.verify_shard_artifacts(root, directory, "postgres", BASE_SHA, HEAD_SHA)


def _rewrite_sidecar(fixture: tuple[Path, Path, dict], changed: dict) -> None:
    """Bind deliberate header mutations to their new sidecar bytes."""

    _, directory, manifest = fixture
    shard = artifacts.SHARD_SPEC_BY_KIND["postgres"]["shards"][0]
    _, sidecar_name = artifacts.shard_file_names("postgres", shard)
    sidecar = directory / sidecar_name
    _write_json(sidecar, changed)
    manifest["inputs"][shard]["provenance_sha256"] = artifacts._sha256_file(sidecar)
    _write_json(directory / artifacts.POSTGRES_COHORT_NAME, manifest)


def test_complete_cohort_retains_all_sidecars_and_accepts_only_the_union(cohort_input, cohort_template: Path) -> None:
    """Every producer identity survives while transporting one combined database."""

    _, directory, _ = cohort_input
    bytes_by_name = {path.name: path.read_bytes() for path in directory.iterdir()}
    assert len(bytes_by_name) == 64
    assert _verify(cohort_input) == [directory / artifacts.POSTGRES_COVERAGE_NAME]
    assert bytes_by_name == {path.name: path.read_bytes() for path in directory.iterdir()}
    for path in (cohort_template / "originals").glob("*.json"):
        assert (directory / path.name).read_bytes() == path.read_bytes()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "python-coverage-cohort/v2"),
        ("kind", "main"),
        ("head_sha", "c" * 40),
        ("base_sha", "c" * 40),
        ("baseline_sha256", "0" * 64),
        ("coverage_version", "0.0"),
        ("coverage_file", ".coverage.postgres.registry-0"),
        ("coverage_sha256", "0" * 64),
        ("coverage_sha256", True),
        ("inputs", []),
        ("unknown", "field"),
    ],
)
def test_cohort_rejects_stale_or_unknown_manifest_fields(cohort_input, field, value) -> None:
    """No manifest header, result digest, shape, or extra field is advisory."""

    _, directory, manifest = cohort_input
    manifest[field] = value
    _write_json(directory / artifacts.POSTGRES_COHORT_NAME, manifest)
    with pytest.raises(artifacts.CoverageForecastError, match="cohort"):
        _verify(cohort_input)


@pytest.mark.parametrize("mutation", ["missing", "extra", "empty", "list", "bool", "uppercase", "short", "unknown"])
def test_cohort_requires_exact_original_producer_hash_map(cohort_input, mutation) -> None:
    """Every original report and sidecar digest must have one exact typed identity."""

    _, directory, manifest = cohort_input
    shard = artifacts.SHARD_SPEC_BY_KIND["postgres"]["shards"][0]
    inputs_by_shard = manifest["inputs"]
    hashes_by_field = inputs_by_shard[shard]
    inputs_by_mutation = {
        "missing": {name: hashes for name, hashes in inputs_by_shard.items() if name != shard},
        "extra": {**inputs_by_shard, "unexpected": hashes_by_field},
        "empty": {**inputs_by_shard, shard: {}},
        "list": {**inputs_by_shard, shard: []},
        "bool": {**inputs_by_shard, shard: {**hashes_by_field, "coverage_sha256": True}},
        "uppercase": {**inputs_by_shard, shard: {**hashes_by_field, "coverage_sha256": "A" * 64}},
        "short": {**inputs_by_shard, shard: {**hashes_by_field, "provenance_sha256": "a" * 63}},
        "unknown": {**inputs_by_shard, shard: {**hashes_by_field, "other": "a" * 64}},
    }
    manifest["inputs"] = inputs_by_mutation[mutation]
    _write_json(directory / artifacts.POSTGRES_COHORT_NAME, manifest)
    with pytest.raises(artifacts.CoverageForecastError, match="cohort"):
        _verify(cohort_input)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", True),
        ("schema_version", 1.0),
        ("kind", "main"),
        ("shard", "registry-0"),
        ("report_name", "rust"),
        ("head_sha", "c" * 40),
        ("base_sha", "c" * 40),
        ("baseline_sha256", "0" * 64),
        ("coverage_version", "0.0"),
        ("coverage_file", artifacts.POSTGRES_COVERAGE_NAME),
        ("coverage_sha256", "0" * 64),
        ("unknown", "field"),
        ("common_test_execution", None),
        ("common_test_execution", []),
        ("common_test_execution", False),
    ],
)
def test_cohort_rejects_rehashed_invalid_original_sidecars(cohort_input, field, value) -> None:
    """A matching sidecar digest cannot replace its full producer/header validation."""

    _, directory, _ = cohort_input
    shard = artifacts.SHARD_SPEC_BY_KIND["postgres"]["shards"][0]
    _, sidecar_name = artifacts.shard_file_names("postgres", shard)
    sidecar = json.loads((directory / sidecar_name).read_text())
    sidecar[field] = value
    _rewrite_sidecar(cohort_input, sidecar)
    with pytest.raises(artifacts.CoverageForecastError, match="cohort|common test execution"):
        _verify(cohort_input)


def test_optional_execution_proof_is_preserved_and_digest_bound(cohort_input) -> None:
    """The consumer preserves optional origin proof rather than interpreting its contents."""

    _, directory, _ = cohort_input
    _, sidecar_name = artifacts.shard_file_names("postgres", "core-services")
    sidecar_path = directory / sidecar_name
    sidecar = json.loads(sidecar_path.read_text())
    sidecar["common_test_execution"] = {"schema": "synthetic-execution/v1", "attempt": 2}
    _rewrite_sidecar(cohort_input, sidecar)
    original = sidecar_path.read_bytes()
    assert _verify(cohort_input) == [directory / artifacts.POSTGRES_COVERAGE_NAME]
    assert sidecar_path.read_bytes() == original
    sidecar["common_test_execution"]["attempt"] = 3
    _write_json(sidecar_path, sidecar)
    with pytest.raises(artifacts.CoverageForecastError, match="provenance digest differs"):
        _verify(cohort_input)


@pytest.mark.parametrize("member", [artifacts.POSTGRES_COVERAGE_NAME, artifacts.POSTGRES_COHORT_NAME, "sidecar"])
@pytest.mark.parametrize("replacement", ["missing", "directory", "symlink", "empty"])
def test_cohort_rejects_missing_nonregular_or_empty_members(cohort_input, member, replacement) -> None:
    """Manifest, result, and every original sidecar use the same closed-file boundary."""

    root, directory, _ = cohort_input
    name = artifacts.shard_file_names("postgres", "core-services")[1] if member == "sidecar" else member
    path = directory / name
    original = path.read_bytes()
    path.unlink()
    if replacement == "directory":
        path.mkdir()
    elif replacement == "symlink":
        target = root / "symlink-target"
        target.write_bytes(original)
        path.symlink_to(target)
    elif replacement == "empty":
        path.touch()
    with pytest.raises(artifacts.CoverageForecastError, match="cohort|exact producer set|provenance"):
        _verify(cohort_input)


@pytest.mark.parametrize("extra", ["raw", "unexpected", "subdirectory"])
def test_cohort_rejects_mixed_layout_and_extra_members(cohort_input, extra) -> None:
    """The explicit manifest cannot silently coexist with raw or unrelated inputs."""

    _, directory, _ = cohort_input
    name = artifacts.shard_file_names("postgres", "core-services")[0] if extra == "raw" else extra
    path = directory / name
    if extra == "subdirectory":
        path.mkdir()
    else:
        path.write_bytes(b"unexpected")
    with pytest.raises(artifacts.CoverageForecastError, match="exact producer set"):
        _verify(cohort_input)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("member", ["manifest", "sidecar"])
def test_cohort_rejects_non_json_constants(cohort_input, constant, member) -> None:
    """Nonstandard numeric constants cannot hide in optional nested proof objects."""

    _, directory, _ = cohort_input
    name = (
        artifacts.POSTGRES_COHORT_NAME
        if member == "manifest"
        else artifacts.shard_file_names("postgres", "core-services")[1]
    )
    path = directory / name
    path.write_text('{"common_test_execution": {"value": ' + constant + "}}", encoding="utf-8")
    with pytest.raises(artifacts.CoverageForecastError, match="invalid cohort JSON"):
        _verify(cohort_input)


@pytest.mark.parametrize("member", ["manifest", "nested-manifest", "sidecar"])
def test_cohort_rejects_duplicate_json_keys(cohort_input, member) -> None:
    """Even identical duplicate values must not rely on a parser's last-key behavior."""

    _, directory, manifest = cohort_input
    if member == "sidecar":
        _, name = artifacts.shard_file_names("postgres", "core-services")
        path = directory / name
        encoded = path.read_text().replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1')
    else:
        path = directory / artifacts.POSTGRES_COHORT_NAME
        encoded = json.dumps(manifest)
        field = "schema" if member == "manifest" else "provenance_sha256"
        encoded = encoded.replace(f'"{field}": ', f'"{field}": null, "{field}": ', 1)
    path.write_text(encoded, encoding="utf-8")
    with pytest.raises(artifacts.CoverageForecastError, match="invalid cohort JSON"):
        _verify(cohort_input)


@pytest.mark.parametrize("member", ["manifest", "sidecar"])
@pytest.mark.parametrize("document", [None, [], 1, "not-an-object", True])
def test_cohort_requires_json_objects_at_each_document_boundary(cohort_input, member, document) -> None:
    """Valid JSON scalars and arrays cannot stand in for a manifest or producer identity."""

    _, directory, _ = cohort_input
    name = (
        artifacts.POSTGRES_COHORT_NAME
        if member == "manifest"
        else artifacts.shard_file_names("postgres", "core-services")[1]
    )
    (directory / name).write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(artifacts.CoverageForecastError, match="cohort JSON must be an object"):
        _verify(cohort_input)


def test_disappearing_cohort_member_has_a_controlled_error(cohort_input, monkeypatch) -> None:
    """A missing file after enumeration cannot escape as an unclassified filesystem error."""

    _, directory, _ = cohort_input
    member = directory / artifacts.shard_file_names("postgres", "core-services")[1]
    original_lstat = Path.lstat

    def vanishing_member(path, *args, **kwargs):
        """Model an artifact disappearing between directory enumeration and its first stat."""
        if path == member:
            raise FileNotFoundError("downloaded member disappeared")
        return original_lstat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", vanishing_member)
    with pytest.raises(artifacts.CoverageForecastError, match="cohort files are unavailable") as error:
        _verify(cohort_input)
    assert isinstance(error.value.__cause__, FileNotFoundError)


def test_cohort_enforces_per_file_and_total_bounds(cohort_input, monkeypatch) -> None:
    """No larger packaging limit is introduced for the compact representation."""

    _, directory, _ = cohort_input
    sizes = [path.stat().st_size for path in directory.iterdir()]
    monkeypatch.setattr(artifacts, "MAX_COHORT_FILE_BYTES", max(sizes) - 1)
    with pytest.raises(artifacts.CoverageForecastError, match="bounded regular files"):
        _verify(cohort_input)
    monkeypatch.setattr(artifacts, "MAX_COHORT_FILE_BYTES", max(sizes))
    monkeypatch.setattr(artifacts, "MAX_COHORT_TOTAL_BYTES", sum(sizes) - 1)
    with pytest.raises(artifacts.CoverageForecastError, match="aggregate limit"):
        _verify(cohort_input)


def test_cohort_is_not_accepted_for_main_or_capacity(cohort_input) -> None:
    """Other producer families retain their exact legacy inventory and validation."""

    root, directory, _ = cohort_input
    for kind in ("main", "capacity"):
        with pytest.raises(artifacts.CoverageForecastError, match="exact producer set"):
            artifacts.verify_shard_artifacts(root, directory, kind, BASE_SHA, HEAD_SHA)


def test_dangling_manifest_cannot_select_the_legacy_reader(cohort_input, cohort_template: Path) -> None:
    """An explicit reserved marker fails closed even when its symlink target is absent."""

    root, _, _ = cohort_input
    raw = root / "raw"
    shutil.copytree(cohort_template / "originals", raw)
    (raw / artifacts.POSTGRES_COHORT_NAME).symlink_to(root / "absent")
    with pytest.raises(artifacts.CoverageForecastError, match="cohort files differ"):
        artifacts.verify_shard_artifacts(root, raw, "postgres", BASE_SHA, HEAD_SHA)


def test_cohort_directory_cannot_be_a_symlink(cohort_input) -> None:
    """The closed transport directory itself must not redirect validation elsewhere."""

    root, directory, _ = cohort_input
    alias = root / "alias"
    alias.symlink_to(directory, target_is_directory=True)
    with pytest.raises(artifacts.CoverageForecastError, match="cohort files differ"):
        artifacts.verify_shard_artifacts(root, alias, "postgres", BASE_SHA, HEAD_SHA)


def _write_branch_family(root: Path, directory: Path, kind: str) -> None:
    """Supply overlapping arcs and distinct contexts to every synthetic producer."""

    directory.mkdir()
    for index, shard in enumerate(artifacts.SHARD_SPEC_BY_KIND[kind]["shards"]):
        coverage_name, provenance_name = artifacts.shard_file_names(kind, shard)
        path = directory / coverage_name
        coverage_data = coverage.CoverageData(basename=str(path))
        coverage_data.set_context(f"{kind}-{index % 3}")
        coverage_data.add_arcs({"api/sample.py": {(-1, 1), (1, -1), (-1, 2), (2, 3 + index % 2), (3 + index % 2, -1)}})
        if index == 0:
            coverage_data.touch_file("api/untouched.py")
        coverage_data.write()
        _write_json(
            path.with_name(provenance_name),
            artifacts._expected_shard_provenance(root, kind, shard, path, BASE_SHA, HEAD_SHA),
        )


def _coverage_contents(path: Path) -> dict:
    """Expose every context, including unexecuted files, independently of JSON totals."""

    data = coverage.CoverageData(basename=str(path))
    data.read()
    contents_by_context = {}
    for context in sorted(data.measured_contexts()):
        data.set_query_context(context)
        contents_by_context[context] = {name: sorted(data.arcs(name) or []) for name in sorted(data.measured_files())}
    return contents_by_context


def _prepare_branch_inputs(root: Path) -> tuple[Path, Path, dict]:
    """Create two transport layouts containing the same independent branch evidence."""

    _write_json(root / artifacts.BASELINE_NAME, _artifact_baseline())
    (root / "api").mkdir()
    (root / "api" / "sample.py").write_text("def choose(flag):\n    if flag:\n        return 1\n    return 2\n")
    (root / "api" / "additional.py").write_text("value = 2\n")
    (root / "api" / "untouched.py").write_text("value = 3\n")
    (root / "test-coverage.ini").write_text(
        "[run]\nbranch = True\nrelative_files = True\nsource_dirs =\n    api\n\n[report]\ninclude =\n    api/*.py\n"
    )
    raw = root / "raw"
    raw.mkdir()
    for kind in artifacts.SHARD_SPEC_BY_KIND:
        _write_branch_family(root, raw / kind, kind)
    compact = root / "compact"
    compact.mkdir()
    for kind in ("main", "capacity"):
        shutil.copytree(raw / kind, compact / kind)
    return raw, compact, _cohort(root, raw / "postgres", compact / "postgres")


def test_combined_forecast_preserves_union_and_companion_additions(tmp_path: Path) -> None:
    """The unchanged final combiner yields identical coverage from raw or compact inputs."""

    raw, compact, manifest = _prepare_branch_inputs(tmp_path)
    original_inputs = json.loads(json.dumps(manifest["inputs"]))
    for path in (
        raw / "postgres" / artifacts.shard_file_names("postgres", "core-services")[0],
        compact / "postgres" / artifacts.POSTGRES_COVERAGE_NAME,
    ):
        coverage_data = coverage.CoverageData(basename=str(path))
        coverage_data.read()
        coverage_data.set_context("additional-tests")
        coverage_data.add_arcs({"api/additional.py": {(-1, 1), (1, -1)}})
        coverage_data.write()
    raw_path = raw / "postgres" / artifacts.shard_file_names("postgres", "core-services")[0]
    _write_json(
        raw_path.with_name(artifacts.shard_file_names("postgres", "core-services")[1]),
        artifacts._expected_shard_provenance(tmp_path, "postgres", "core-services", raw_path, BASE_SHA, HEAD_SHA),
    )
    manifest["coverage_sha256"] = artifacts._sha256_file(compact / "postgres" / artifacts.POSTGRES_COVERAGE_NAME)
    _write_json(compact / "postgres" / artifacts.POSTGRES_COHORT_NAME, manifest)
    reports = []
    for inputs in (raw, compact):
        report, names = combine.combine_python_coverage(
            tmp_path,
            tmp_path / (inputs.name + "-result"),
            *(inputs / kind for kind in ("main", "capacity", "postgres")),
            BASE_SHA,
            HEAD_SHA,
        )
        reports.append(json.loads(report.read_text()))
        assert len(names["main"]) == 6 and len(names["capacity"]) == 1
    assert reports[0]["files"] == reports[1]["files"]
    assert reports[0]["totals"] == reports[1]["totals"]
    assert reports[1]["files"]["api/additional.py"]["executed_lines"] == [1]
    assert reports[1]["files"]["api/untouched.py"]["missing_lines"] == [1]
    assert _coverage_contents(tmp_path / "raw-result" / ".coverage") == _coverage_contents(
        tmp_path / "compact-result" / ".coverage"
    )
    assert manifest["inputs"] == original_inputs
    assert len(names["postgres"]) == 1


def test_changed_cohort_database_is_rejected_before_combining(cohort_input) -> None:
    """The result digest authenticates the actual union, not an original producer hash."""

    _, directory, _ = cohort_input
    path = directory / artifacts.POSTGRES_COVERAGE_NAME
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(artifacts.CoverageForecastError, match="manifest differs"):
        _verify(cohort_input)


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE coverage_schema SET version = 99",
        "UPDATE meta SET value = '0' WHERE key = 'has_arcs'",
        "INSERT INTO tracer VALUES (1, 'unknown.plugin')",
        "ALTER TABLE arc ADD COLUMN extra TEXT",
        "CREATE TABLE unexpected (value INTEGER)",
        "CREATE VIEW unexpected AS SELECT * FROM arc",
        "CREATE TRIGGER unexpected AFTER INSERT ON file BEGIN SELECT 1; END",
        "UPDATE file SET path = '../external.py' WHERE id = 1",
        "UPDATE file SET path = '/external.py' WHERE id = 1",
    ],
)
def test_rehashed_cohort_database_rejects_unsupported_schema_or_plugins(cohort_input, statement) -> None:
    """A correct digest cannot make unsupported or executable SQLite content acceptable."""

    _, directory, manifest = cohort_input
    path = directory / artifacts.POSTGRES_COVERAGE_NAME
    with closing(sqlite3.connect(path)) as database, database:
        database.execute(statement)
    manifest["coverage_sha256"] = artifacts._sha256_file(path)
    _write_json(directory / artifacts.POSTGRES_COHORT_NAME, manifest)
    original = path.read_bytes()
    with pytest.raises(artifacts.CoverageForecastError, match="invalid cohort coverage data"):
        _verify(cohort_input)
    assert path.read_bytes() == original


@pytest.mark.parametrize("replacement", [b"not SQLite coverage data", b"SQLite format 3\0" + b"\0" * 100])
def test_rehashed_malformed_database_is_not_silently_skipped(cohort_input, replacement) -> None:
    """Coverage aggregation never gets a chance to ignore a corrupt compact input."""

    _, directory, manifest = cohort_input
    path = directory / artifacts.POSTGRES_COVERAGE_NAME
    path.write_bytes(replacement)
    manifest["coverage_sha256"] = artifacts._sha256_file(path)
    _write_json(directory / artifacts.POSTGRES_COHORT_NAME, manifest)
    with pytest.raises(artifacts.CoverageForecastError, match="invalid cohort coverage data"):
        _verify(cohort_input)
    assert path.read_bytes() == replacement


def test_corrupt_arc_pages_are_rejected_before_coverage_can_skip_them(cohort_input) -> None:
    """Valid headers and metadata cannot conceal unreadable branch data pages."""

    _, directory, manifest = cohort_input
    path = directory / artifacts.POSTGRES_COVERAGE_NAME
    with closing(sqlite3.connect(path)) as database:
        page_size = database.execute("PRAGMA page_size").fetchone()[0]
        root_page = database.execute("SELECT rootpage FROM sqlite_master WHERE name = 'arc'").fetchone()[0]
    damaged = bytearray(path.read_bytes())
    damaged[(root_page - 1) * page_size] = 0
    path.write_bytes(damaged)
    manifest["coverage_sha256"] = artifacts._sha256_file(path)
    _write_json(directory / artifacts.POSTGRES_COHORT_NAME, manifest)
    with pytest.raises(artifacts.CoverageForecastError, match="invalid cohort coverage data"):
        _verify(cohort_input)


def test_readable_but_inconsistent_database_is_rejected(cohort_input) -> None:
    """SQLite integrity diagnostics must fail even when metadata and tables remain readable."""

    _, directory, manifest = cohort_input
    path = directory / artifacts.POSTGRES_COVERAGE_NAME
    damaged = bytearray(path.read_bytes())
    # Declare one extra free page without damaging any readable table or metadata page.
    free_pages = int.from_bytes(damaged[36:40], "big")
    damaged[36:40] = (free_pages + 1).to_bytes(4, "big")
    path.write_bytes(damaged)
    manifest["coverage_sha256"] = artifacts._sha256_file(path)
    _write_json(directory / artifacts.POSTGRES_COHORT_NAME, manifest)
    with pytest.raises(artifacts.CoverageForecastError, match="invalid cohort coverage data") as error:
        _verify(cohort_input)
    assert str(error.value.__cause__) == "corrupt coverage SQLite data"


def test_cohort_reader_never_loads_source_coverage_configuration(cohort_input) -> None:
    """Data-only validation neither imports configured plugins nor changes input bytes."""

    root, directory, _ = cohort_input
    (root / ".coveragerc").write_text("[run]\nplugins = nonexistent.synthetic_plugin\n")
    path = directory / artifacts.POSTGRES_COVERAGE_NAME
    original = path.read_bytes()
    assert _verify(cohort_input) == [path]
    assert path.read_bytes() == original
