import json
import subprocess
import sys
import textwrap
from importlib import util
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "readability_budget.py"
SPEC = util.spec_from_file_location("readability_budget", SCRIPT_PATH)
readability_budget = util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = readability_budget
SPEC.loader.exec_module(readability_budget)
readability_cli = sys.modules["readability.cli"]

NOQA_FIXTURE = "# no" + "qa: E123"
COMMENT_NOISE_FIXTURE = "# return" + " result"


def test_readability_cli_has_no_per_pr_debt_tax_or_reset():
    cli_text = (Path(__file__).resolve().parents[1] / "scripts" / "readability" / "cli.py").read_text(encoding="utf-8")

    assert "--base" in cli_text
    assert "--required-reduction-percent" not in cli_text
    assert "--ratchet-baseline" not in cli_text
    assert "one_time_debt_reset" not in cli_text


def _write_config(repo_root: Path) -> None:
    config_dict = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 8,
            "max_rust_file_lines": 5,
            "huge_file_lines": 20,
            "max_function_lines": 4,
            "max_nesting_depth": 1,
            "max_function_name_tokens": 6,
            "max_class_name_tokens": 6,
            "min_generic_function_lines": 4,
            "min_ambiguous_variable_scope_lines": 4,
            "max_single_letter_scope_lines": 2,
            "min_docstring_function_lines": 4,
            "max_parameters": 3,
            "max_locals": 3,
        },
        "readability": {
            "file_length_roots": ["pkg"],
            "module_attribute_injection_allowlist": [],
            "ambiguous_function_names": ["process_data"],
            "ambiguous_class_names": ["Manager"],
            "ambiguous_variable_names": ["data", "row", "result"],
            "allowed_short_names": ["_", "i"],
            "always_bad_short_names": ["l", "O"],
            "boolean_prefixes": ["is_", "has_", "should_"],
            "dict_name_markers": ["_by_", "_map"],
            "collection_singular_exceptions": ["data"],
            "comment_noise_patterns": ["^return\\b"],
        },
        "inline_suppression_patterns": [
            {"name": "python_noqa", "pattern": "#\\s*noqa\\b"},
        ],
    }
    (repo_root / "readability-budget.json").write_text(json.dumps(config_dict), encoding="utf-8")


def _commit_paths(repo_root: Path, message: str, *paths: str) -> None:
    subprocess.run(["git", "add", *paths], cwd=repo_root, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Readability Test",
            "-c",
            "user.email=readability@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
            "commit",
            "-qm",
            message,
        ],
        cwd=repo_root,
        check=True,
    )


def test_readability_budget_allows_existing_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def existing():
                return 1  {noqa_fixture}
            """
        ).format(noqa_fixture=NOQA_FIXTURE),
        encoding="utf-8",
    )
    _write_config(repo_root)

    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    assert readability_budget.main(["--repo-root", str(repo_root)]) == 0


def test_readability_budget_rejects_new_inline_suppression(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text("def clean():\n    return 1\n", encoding="utf-8")
    _write_config(repo_root)
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0

    module.write_text(
        textwrap.dedent(
            """
            def clean():
                return 1

            def new_debt():
                return 2  {noqa_fixture}
            """
        ).format(noqa_fixture=NOQA_FIXTURE),
        encoding="utf-8",
    )

    assert readability_budget.main(["--repo-root", str(repo_root)]) == 1


def test_readability_budget_reports_long_functions(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def too_long():
                first = 1
                second = 2
                third = 3
                fourth = 4
                return first + second + third + fourth
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["long_functions"] == 1
    assert snapshot["issues"]["long_functions"][0]["function"] == "too_long"


def test_readability_budget_attributes_nonlocal_to_nested_scope_only(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def outer():
                count = 0

                def inner():
                    nonlocal count
                    count += 1
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["global_state_usage"] == 1
    assert snapshot["issues"]["global_state_usage"][0]["function"] == "outer.inner"


def test_readability_budget_ignores_response_factory_calls(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def _route_response(response):
                response_headers_by_name = {"Allow": "OPTIONS"}
                preflight_response = response.empty(status=204, headers=response_headers_by_name)
                return preflight_response
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["boolean_name_mismatch"] == 0
    assert snapshot["issue_counts"]["collection_name_mismatch"] == 0


def test_readability_budget_does_not_parse_non_python_files(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "route.rs").write_text(
        'fn main() {\n    println!("not python");\n}\n',
        encoding="utf-8",
    )
    config_dict = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py", ".rs"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 8,
            "max_function_lines": 4,
            "max_nesting_depth": 1,
        },
        "inline_suppression_patterns": [],
    }
    (repo_root / "readability-budget.json").write_text(json.dumps(config_dict), encoding="utf-8")

    snapshot = readability_budget.build_snapshot(repo_root, config_dict)

    assert snapshot["issue_counts"]["syntax_errors"] == 0


def test_readability_budget_reports_naming_and_contract_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            class Manager:
                pass

            def process_data(a, b, c, d):
                {comment_noise_fixture}
                data = [1, 2, 3]
                row = {{"a": 1}}
                result = a == b
                l = 1
                extra = 2
                another = 3
                return result
            """
        ).format(comment_noise_fixture=COMMENT_NOISE_FIXTURE),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["ambiguous_function_names"] == 1
    assert snapshot["issue_counts"]["ambiguous_variable_names"] == 3
    assert snapshot["issue_counts"]["boolean_name_mismatch"] == 1
    assert snapshot["issue_counts"]["class_name_shape"] == 1
    assert snapshot["issue_counts"]["comment_noise"] == 1
    assert snapshot["issue_counts"]["missing_contract_docstrings"] == 1
    assert snapshot["issue_counts"]["single_letter_names"] == 5
    assert snapshot["issue_counts"]["too_many_locals"] == 1
    assert snapshot["issue_counts"]["too_many_parameters"] == 1


def test_readability_budget_reports_collection_and_global_state_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def build_lookup():
                global CACHE
                names = {"a": 1}
                thing = []
                ...
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["collection_name_mismatch"] == 2
    assert snapshot["issue_counts"]["global_state_usage"] == 1
    assert snapshot["issue_counts"]["pass_placeholders"] == 1


def test_readability_budget_blocks_scaffolding_but_softens_file_length(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "legacy_part_01.py").write_text(
        textwrap.dedent(
            """
            import sys as _module_sys
            from . import facade as _service

            _service.answer = (
                1
            )
            _SPLIT_IMPLEMENTATION_MODULES = ()
            for module_name in _SPLIT_IMPLEMENTATION_MODULES:
                _module_sys.modules[module_name].__dict__.update({})
            """
        ),
        encoding="utf-8",
    )
    _write_config(tmp_path)

    snapshot = readability_budget.build_snapshot(
        tmp_path,
        json.loads((tmp_path / "readability-budget.json").read_text(encoding="utf-8")),
    )
    assert snapshot["issue_counts"]["module_attribute_injection"] == 3
    assert snapshot["issue_counts"]["split_module_name"] == 1
    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0

    (package / "long_but_clean.py").write_text("answer = 1\n" * 9, encoding="utf-8")
    assert readability_budget.main(["--repo-root", str(tmp_path)]) == 0
    (package / "new_part_02.py").write_text("answer = 1\n", encoding="utf-8")
    assert readability_budget.main(["--repo-root", str(tmp_path)]) == 1

    duplicate_snapshot_by_category = {
        "issues": {
            "module_attribute_injection": [
                {"id": "same"},
                {"id": "same"},
            ]
        }
    }
    duplicate_baseline_by_category = {"issue_ids": {"module_attribute_injection": ["same"]}}
    assert (
        len(
            readability_cli._new_issues(duplicate_snapshot_by_category, duplicate_baseline_by_category)[
                "module_attribute_injection"
            ]
        )
        == 1
    )


def test_readability_budget_blocks_growth_of_an_existing_huge_file(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text("answer = 1\n" * 6, encoding="utf-8")
    _write_config(tmp_path)
    config_path = tmp_path / "readability-budget.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["thresholds"]["huge_file_lines"] = 5
    config_path.write_text(json.dumps(config), encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    _commit_paths(tmp_path, "baseline", "pkg/module.py")
    base_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0

    module.write_text("answer = 1\n" * 7, encoding="utf-8")
    assert readability_budget.main(["--repo-root", str(tmp_path), "--base", base_revision]) == 1

    renamed_module = package / "renamed.py"
    module.rename(renamed_module)
    _commit_paths(tmp_path, "rename module", "pkg")
    assert readability_budget.main(["--repo-root", str(tmp_path), "--base", base_revision]) == 1


def test_readability_budget_fails_closed_for_an_unresolvable_base(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "module.py").write_text("answer = 1\n" * 6, encoding="utf-8")
    _write_config(tmp_path)
    config_path = tmp_path / "readability-budget.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["thresholds"]["huge_file_lines"] = 5
    config_path.write_text(json.dumps(config), encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    _commit_paths(tmp_path, "baseline", "pkg/module.py")
    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0

    unknown_revision = "0" * 40
    snapshot = readability_budget.build_snapshot(tmp_path, config, unknown_revision)

    assert [issue["id"] for issue in snapshot["issues"]["huge_file_growth"]] == [
        "huge_file_growth:git:base revision unavailable:."
    ]
    assert readability_budget.main(["--repo-root", str(tmp_path), "--base", unknown_revision]) == 1


def test_readability_budget_allows_only_explicit_module_rewrites(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    module = package / "module.py"
    allowed_text = "from .models import InternalRecord, PublicReceipt\nPublicReceipt.__module__ = __name__\n"
    module.write_text(allowed_text, encoding="utf-8")
    _write_config(tmp_path)
    config_path = tmp_path / "readability-budget.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["readability"]["module_attribute_injection_allowlist"] = ["pkg/module.py:PublicReceipt"]
    config_path.write_text(json.dumps(config), encoding="utf-8")
    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0

    module.write_text(allowed_text + "InternalRecord.__module__ = __name__\n", encoding="utf-8")
    assert readability_budget.main(["--repo-root", str(tmp_path)]) == 1


def test_file_length_exclusions_keep_function_rules(tmp_path):
    _write_config(tmp_path)
    config_path = tmp_path / "readability-budget.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["source_roots"] = ["pkg", "tests", "scripts", "alembic"]
    config_path.write_text(json.dumps(config), encoding="utf-8")
    long_function_text = textwrap.dedent(
        """
        def calculate_total():
            total = 0
            total += 1
            total += 1
            total += 1
            total += 1
            total += 1
            return total
        """
    )
    expected_paths = {
        "tests/long_test.py",
        "scripts/long_script.py",
        "alembic/versions/long_migration.py",
    }
    for relative in expected_paths:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(long_function_text, encoding="utf-8")

    snapshot = readability_budget.build_snapshot(tmp_path, config)
    assert snapshot["issue_counts"]["long_files"] == 0
    assert {issue["path"] for issue in snapshot["issues"]["long_functions"]} == expected_paths


def test_rust_file_budget_excludes_cfg_test_modules_only(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    module = package / "module.rs"
    module.write_text(
        textwrap.dedent(
            """\
            const FIRST: usize = 1;
            const SECOND: usize = 2;
            #[cfg(test)]
            #[path = "module_tests.rs"]
            mod external_tests;
            #[cfg(test)]
            mod tests {
                const JSON: &str = r#"{"brace": "}"}"#;
                fn example() {}
            }
            #[cfg(all(test, unix))]
            mod unix_tests {
                fn example() {}
            }
            const THIRD: usize = 3;
            """
        ),
        encoding="utf-8",
    )
    _write_config(tmp_path)
    config_path = tmp_path / "readability-budget.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["include_suffixes"] = [".rs"]
    config["thresholds"]["max_rust_file_lines"] = 3
    config_path.write_text(json.dumps(config), encoding="utf-8")

    snapshot = readability_budget.build_snapshot(tmp_path, config)
    assert snapshot["issue_counts"]["long_files"] == 0

    module.write_text(
        module.read_text(encoding="utf-8") + "#[cfg(test)]\nfn test_only_helper() {}\n",
        encoding="utf-8",
    )
    snapshot = readability_budget.build_snapshot(tmp_path, config)
    assert snapshot["issue_counts"]["long_files"] == 1
    assert snapshot["issues"]["long_files"][0]["lines"] == 5
    assert snapshot["issues"]["long_files"][0]["limit"] == 3


def test_readability_budget_rejects_rust_split_test_halves(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "query_a.rs").write_text("const A: usize = 1;\n", encoding="utf-8")
    (package / "query_b.rs").write_text("const B: usize = 2;\n", encoding="utf-8")
    (package / "owner.rs").write_text('include!("query/tests/query_a.rs");\n', encoding="utf-8")
    (package / "root_owner.rs").write_text('include!("tests/query_b.rs");\n', encoding="utf-8")
    _write_config(tmp_path)
    config_path = tmp_path / "readability-budget.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["include_suffixes"] = [".rs"]
    config_path.write_text(json.dumps(config), encoding="utf-8")

    snapshot = readability_budget.build_snapshot(tmp_path, config)

    assert snapshot["issue_counts"]["split_module_name"] == 4


def test_repository_huge_file_inventory_matches_current_product_tree():
    repo_root = Path(__file__).resolve().parents[1]
    config = json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8"))
    source_files = sys.modules["readability.source_files"]
    huge_file_lines = config["thresholds"]["huge_file_lines"]
    huge_paths = {
        path.relative_to(repo_root).as_posix()
        for path in source_files._iter_source_files(repo_root, config)
        if source_files._is_file_length_path(path.relative_to(repo_root).as_posix(), config)
        and source_files._line_count(path) > huge_file_lines
    }

    assert huge_paths == {
        "api/endpoint/npi.py",
        "api/endpoint/pricing.py",
        "api/ptg2_db_sidecars.py",
        "api/ptg2_serving.py",
        "db/models/_legacy.py",
        "process/entity_address_unified.py",
        "process/florida_mqa_profile.py",
        "process/mrf_source_discovery.py",
        "process/provider_directory_fhir.py",
        "process/ptg.py",
        "process/ptg_parts/ptg2_shared_snapshot_publish.py",
        "process/reference_family_archive.py",
        "support/ptg2_scanner/src/main.rs",
        "support/ptg2_scanner/src/provider_graph_v4.rs",
    }


def test_repository_config_checks_migrations_but_excludes_their_file_lengths():
    repo_root = Path(__file__).resolve().parents[1]
    config = json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8"))
    source_files = sys.modules["readability.source_files"]
    excluded_paths = set(config["exclude_globs"])

    assert {"alembic", "service"} <= set(config["source_roots"])
    assert "alembic" not in config["readability"]["file_length_roots"]
    assert "alembic/versions/**" not in excluded_paths
    assert "service/**" not in excluded_paths

    migration = next((repo_root / "alembic" / "versions").glob("*.py"))
    assert migration in source_files._iter_source_files(repo_root, config)
    assert not source_files._is_file_length_path(migration.relative_to(repo_root).as_posix(), config)


def _configure_importer_guard_repo(repo_root: Path) -> dict:
    _write_config(repo_root)
    config_by_field = json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8"))
    config_by_field["source_roots"] = ["pkg", "process"]
    config_by_field["readability"]["importer_guard_roots"] = ["process"]
    config_by_field["readability"]["importer_guard_exclude_globs"] = ["process/excluded_*.py"]
    (repo_root / "readability-budget.json").write_text(json.dumps(config_by_field), encoding="utf-8")
    return config_by_field


def test_importer_guards_allow_set_based_sql_and_line_shifts(tmp_path):
    process_dir = tmp_path / "process"
    process_dir.mkdir()
    (process_dir / "clean_importer.py").write_text(
        textwrap.dedent(
            '''
            import hashlib

            async def copy_clean(connection):
                """Copy validated rows with one set-based SQL statement."""
                await connection.execute("INSERT INTO dst (id) SELECT id FROM src ON CONFLICT DO NOTHING")
                return await connection.fetchval("SELECT count(*)::bigint FROM dst")

            async def export_with_metadata(connection, metadata, destination):
                """Write rows and fingerprint their separate metadata."""
                metadata_digest = hashlib.sha256(metadata).hexdigest()
                await connection.copy_from_query("SELECT id FROM src", output=destination.write, format="binary")
                return metadata_digest

            async def page_source_rows(connection, cursor_json):
                """Read source pages without copying a prior snapshot."""
                while True:
                    rows = await connection.fetch("SELECT id FROM src WHERE id > $1 ORDER BY id LIMIT $2", cursor_json, 10)
                    if not rows:
                        break
                    cursor_json = rows[-1]["id"]

                    async def write_later():
                        await connection.execute("INSERT INTO dst (id) VALUES ($1)", cursor_json)
            '''
        ),
        encoding="utf-8",
    )
    (process_dir / "excluded_legacy.py").write_text(
        'LEGACY_SQL = "SELECT relfilenode FROM pg_class"\n',
        encoding="utf-8",
    )
    grandfathered = process_dir / "grandfathered.py"
    grandfathered.write_text(
        textwrap.dedent(
            '''
            def existing_check(connection):
                """Baseline grandfathered check stays scope-keyed across line shifts."""
                return connection.fetchval("SELECT pg_catalog.row_security_active(oid) FROM pg_class")
            '''
        ),
        encoding="utf-8",
    )
    config_by_field = _configure_importer_guard_repo(tmp_path)
    snapshot = readability_budget.build_snapshot(tmp_path, config_by_field)
    assert snapshot["issue_counts"]["importer_paged_copy_loops"] == 0
    assert [issue["path"] for issue in snapshot["issues"]["importer_custody_checks"]] == ["process/grandfathered.py"]
    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0

    grandfathered.write_text("\n\n" + grandfathered.read_text(encoding="utf-8"), encoding="utf-8")
    assert readability_budget.main(["--repo-root", str(tmp_path)]) == 0


_BAD_IMPORTER_FIXTURE = textwrap.dedent(
    '''
    import hashlib

    def check_filenode():
        """Probe physical filenode."""
        return "SELECT relfilenode FROM pg_class"

    def check_rls():
        """Probe row security."""
        return "SELECT row_security_active(oid) FROM pg_class"

    def check_owner():
        """Probe relation owner."""
        return "SELECT relowner FROM pg_class"

    def check_triggers():
        """Probe pg_trigger catalog."""
        return "SELECT EXISTS(SELECT FROM pg_trigger)"

    def verify_independent_source_contributions():
        """Run forbidden multiset verifier."""
        return True

    async def rehash_copy(connection):
        """Re-hash copied query output."""
        folded = hashlib.sha256()
        await connection.copy_from_query("SELECT id FROM src", output=folded.update, format="binary")

    def paged_sql_limit():
        """Run paged LIMIT copy SQL."""
        return "INSERT INTO dst SELECT id FROM src ORDER BY id LIMIT $1"

    def paged_sql_running_bytes():
        """Run byte-windowed copy SQL."""
        return "INSERT INTO dst SELECT id, running_bytes FROM windowed"

    async def paged_loop(connection):
        """Page copy loop with cursor state."""
        cursor_json = None
        while True:
            batch = await connection.fetch("SELECT id FROM src ORDER BY id LIMIT $1", cursor_json)
            if not batch:
                break
            await connection.execute("INSERT INTO dst (id) VALUES ($1)", batch[0]["id"])
    async def paged_for(connection, cursors):
        """Copy rows in a for loop over cursor values."""
        for cursor_json in cursors:
            batch = await connection.fetch("SELECT id FROM src ORDER BY id LIMIT $1", cursor_json)
            await connection.executemany("INSERT INTO dst (id) VALUES ($1)", batch)

    async def paged_async_for(connection, cursors):
        """Copy rows in an async for loop over cursor values."""
        async for cursor_json in cursors:
            batch = await connection.fetch("SELECT id FROM src ORDER BY id LIMIT $1", cursor_json)
            await connection.executemany("INSERT INTO dst (id) VALUES ($1)", batch)

    async def paged_nested_loop(connection):
        """Write individual rows inside an outer paging loop."""
        cursor_json = None
        while True:
            batch = await connection.fetch("SELECT id FROM src ORDER BY id LIMIT $1", cursor_json)
            if not batch:
                break
            for row in batch:
                await connection.execute("INSERT INTO dst (id) VALUES ($1)", row["id"])
            cursor_json = batch[-1]["id"]
    '''
)


def test_importer_guards_block_custody_rehash_and_paged_copy_loops(tmp_path):
    process_dir = tmp_path / "process"
    process_dir.mkdir()
    config_by_field = _configure_importer_guard_repo(tmp_path)
    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0

    (process_dir / "bad_importers.py").write_text(_BAD_IMPORTER_FIXTURE, encoding="utf-8")
    snapshot = readability_budget.build_snapshot(tmp_path, config_by_field)
    custody_reasons = {issue["reason"] for issue in snapshot["issues"]["importer_custody_checks"]}
    paged_reasons = {issue["reason"] for issue in snapshot["issues"]["importer_paged_copy_loops"]}
    assert custody_reasons == {
        "physical_filenode_check",
        "row_security_custody_check",
        "owner_or_acl_custody_check",
        "catalog_hook_custody_check",
        "forbidden_seal_or_multiset_verifier",
        "copy_from_query_rehash",
    }
    assert paged_reasons == {"paged_copy_forward_sql", "python_paged_copy_loop"}
    assert {
        issue["function"]
        for issue in snapshot["issues"]["importer_paged_copy_loops"]
        if issue["reason"] == "python_paged_copy_loop"
    } == {"paged_loop", "paged_for", "paged_async_for", "paged_nested_loop"}
    assert readability_budget.main(["--repo-root", str(tmp_path)]) == 1
