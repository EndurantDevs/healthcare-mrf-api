"""Contracts for the shared local and GitHub Actions pre-push gate."""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PREPUSH = ROOT / "scripts" / "ci" / "prepush"
PYTHON_LOCK_INPUT = ROOT / "requirements-ci.in"
PYTHON_LOCK = ROOT / "requirements-ci.lock"
DOCKERFILE = ROOT / "Dockerfile"
PYTHON_LOCK_INSTALLER = ROOT / "scripts" / "ci" / "install_python_lock"
PYTHON_LOCK_GENERATOR = ROOT / "scripts" / "ci" / "compile_python_lock"
PYTHON_LOCK_VALIDATOR = ROOT / "scripts" / "ci" / "validate_python_lock_inputs"


def _job(workflow: str, name: str) -> str:
    match = re.search(rf"^  {re.escape(name)}:\n(.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)", workflow, re.M | re.S)
    assert match is not None, name
    return match.group(1)


def test_every_required_ci_family_delegates_to_prepush() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    expected_command_by_job = {
        "public-hygiene": "scripts/ci/prepush hygiene",
        "python-quality": "scripts/ci/prepush quality",
        "python-tests": 'scripts/ci/prepush python-main "${{ matrix.shard-index }}"',
        "capacity-evidence": "scripts/ci/prepush capacity",
        "test-coverage": "scripts/ci/prepush python-coverage",
        "api-contract": "scripts/ci/prepush api-contract",
        "rust-scanner": "scripts/ci/prepush rust",
        "container-package": "scripts/ci/prepush container-package",
        "security": "scripts/ci/prepush security",
        "worker-queue-smoke": "scripts/ci/prepush redis",
        "address-canonical-db-tests": 'scripts/ci/prepush postgres "${{ matrix.shard }}"',
    }
    for job, command in expected_command_by_job.items():
        job_body = _job(workflow, job)
        assert command in job_body
        assert len(re.findall(r"^        run:", job_body, re.M)) == 1


def test_service_modes_wait_for_ready_redis_and_postgres() -> None:
    script = PREPUSH.read_text(encoding="utf-8")
    helper = re.search(r"^wait_for_service\(\) \{\n.*?^\}", script, re.M | re.S)
    assert helper is not None

    subprocess.run(
        [
            "bash",
            "-eu",
            "-o",
            "pipefail",
            "-c",
            f"""
{helper.group(0)}
attempt=0
ready() {{ return 0; }}
delayed() {{ attempt=$((attempt + 1)); [ "$attempt" -eq 2 ]; }}
never() {{ return 1; }}
wait_for_service Ready 0 ready
wait_for_service Delayed 1 delayed
if output=$(wait_for_service Missing 0 never 2>&1); then exit 1; fi
[ "$attempt" -eq 2 ]
[ "$output" = "Missing service was not ready within 0 seconds" ]
""",
        ],
        check=True,
    )

    redis = re.search(r"^run_redis\(\) \{\n(.*?)(?=^\}\n)", script, re.M | re.S)
    postgres = re.search(
        r"^run_postgres\(\) \{\n(.*?)(?=^\}\n)", script, re.M | re.S
    )
    assert redis is not None
    assert postgres is not None
    assert redis.group(1).index("wait_for_service Redis 30") < redis.group(1).index(
        'await redis.rpush("arq:ci-smoke", "healthporta-ci")'
    )
    assert "redis://127.0.0.1:6379" in redis.group(1)
    assert "socket_connect_timeout=1, socket_timeout=1" in redis.group(1)
    assert postgres.group(1).index(
        "wait_for_service PostgreSQL 30"
    ) < postgres.group(1).index("prepare_postgres")
    assert "pg_isready" in postgres.group(1)
    assert "HLTHPRT_DB_HOST:-127.0.0.1" in postgres.group(1)
    assert "--timeout=1" in postgres.group(1)


def test_local_all_pins_ci_images_inputs_and_safety_margins() -> None:
    script = PREPUSH.read_text(encoding="utf-8")
    assert "ghcr.io/endurantdevs/healthcare-mrf-api-arc-ci@sha256:12320cf489cc218f00d04e9df21d12f60bacd883e5120bd8dabb7ba0378c972c" in script
    assert "postgis/postgis:18-3.6@sha256:0aacdfb9dda40942d424785c3ccabeffe31da9cc9e26b0dfd93b222b31462871" in script
    assert "redis:7@sha256:e9b2e45ecd47fbb69b877cf8d045d5cccaaaed52524b6e098b4abe8212994f73" in script
    assert "--platform linux/amd64" in script
    assert "for shard in 0 1 2 3" in script
    assert "for shard in core provider-directory provider-profile" in script
    assert 'require_margin "$(artifact_path test-coverage-forecast-python.json)" python lines 5' in script
    assert 'require_margin "$(artifact_path test-coverage-forecast-python.json)" python branches 5' in script
    assert 'require_margin "$(artifact_path test-coverage-forecast-rust.json)" rust functions 2' in script
    assert script.count("require_margin \"") == 3
    assert 'all requires a clean, committed candidate' in script
    assert 'status=passed' in script


def test_local_all_matches_hosted_family_environment_boundaries() -> None:
    script = PREPUSH.read_text(encoding="utf-8")
    run_all = script.split("run_all() {", 1)[1]
    container_arguments = run_all.split(
        "docker run --rm --platform linux/amd64", 1
    )[1].split('    "$arc_image"', 1)[0]

    assert "export HOME=/artifacts/home" in script
    for environment_name in (
        "HLTHPRT_DB_DRIVER",
        "HLTHPRT_DB_HOST",
        "HLTHPRT_DB_PORT",
        "HLTHPRT_DB_USER",
        "HLTHPRT_DB_PASSWORD",
        "HLTHPRT_DB_DATABASE",
        "HLTHPRT_DB_SCHEMA",
        "DB_SCHEMA",
        "HLTHPRT_REDIS_ADDRESS",
    ):
        assert environment_name not in container_arguments
    assert "HLTHPRT_REDIS_ADDRESS=redis://redis:6379 gate redis redis" in script
    assert "HLTHPRT_DB_HOST=postgres" in script
    assert "HLTHPRT_REDIS_ADDRESS=redis://127.0.0.1" in script


def test_dispatcher_exposes_only_repository_ci_modes() -> None:
    script = PREPUSH.read_text(encoding="utf-8")
    for mode in (
        "all",
        "hygiene",
        "quality",
        "python-main",
        "capacity",
        "python-coverage",
        "api-contract",
        "rust",
        "container-package",
        "security",
        "redis",
        "postgres",
    ):
        assert f"{mode})" in script


def test_local_all_freezes_candidate_and_preserves_external_receipt() -> None:
    script = PREPUSH.read_text(encoding="utf-8")

    assert 'candidate_sha=$(git rev-parse HEAD)' in script
    assert 'all requires a clean, committed candidate' in script
    assert 'PREPUSH_RECEIPT_DIR must be outside the repository' in script
    assert 'assert_candidate_unchanged "$candidate_sha"' in script
    assert 'printf "gate=%s passed\\n" "$name" >> /artifacts/receipt.txt' in script
    assert 'status=passed' in script
    assert 'rm -rf -- "$artifacts"' not in script
    assert 'linux/x86_64' in script


def test_local_all_exit_cleanup_state_outlives_run_all() -> None:
    script = PREPUSH.read_text(encoding="utf-8")
    for variable in ("network", "postgres_container", "redis_container", "artifacts"):
        assert re.search(rf"^  {variable}=", script, re.M)
        assert not re.search(rf"^  local {variable}(?:=|$)", script, re.M)


def test_local_all_propagates_reviewed_readability_approval() -> None:
    script = PREPUSH.read_text(encoding="utf-8")

    assert "readability_zero_growth_approved=${READABILITY_ZERO_GROWTH_APPROVED:-false}" in script
    assert "readability_zero_growth_approved=$readability_zero_growth_approved" in script
    assert '--env READABILITY_ZERO_GROWTH_APPROVED="$readability_zero_growth_approved"' in script


def test_local_all_records_the_native_libc_version() -> None:
    script = PREPUSH.read_text(encoding="utf-8")

    assert "platform.libc_ver()" in script
    assert "platform.libc()" not in script


def test_local_all_records_redis_version_inside_the_quoted_runner() -> None:
    script = PREPUSH.read_text(encoding="utf-8")

    assert "python - <<PY" in script
    assert 'print("redis " + info["redis_version"])' in script
    assert "python - <<'PY'\nimport asyncio\nfrom redis.asyncio" not in script


def test_native_python_lock_is_complete_and_hashed() -> None:
    lock_input = PYTHON_LOCK_INPUT.read_text(encoding="utf-8")
    lock = PYTHON_LOCK.read_text(encoding="utf-8")
    lock_lines = lock.splitlines()
    requirement_starts = [
        index
        for index, line in enumerate(lock_lines)
        if line and not line[0].isspace() and not line.startswith(("#", "--"))
    ]

    assert lock_input == "-r requirements-dev.txt\npip==26.2.1\npip-audit==2.10.1\n"
    for input_name in ("requirements.txt", "requirements-dev.txt", "requirements-ci.in"):
        input_hash = hashlib.sha256((ROOT / input_name).read_bytes()).hexdigest()
        assert f"# Input: {input_name} ({input_hash})" in lock
    assert "# Python: 3.14.6 (Linux x86_64)" in lock
    assert "# Resolver: pip 26.1.2" in lock
    assert requirement_starts
    assert any(lock_lines[index].startswith("pip==") for index in requirement_starts)
    for offset, start in enumerate(requirement_starts):
        end = (
            requirement_starts[offset + 1]
            if offset + 1 < len(requirement_starts)
            else len(lock_lines)
        )
        requirement = lock_lines[start]
        block = "\n".join(lock_lines[start:end])
        assert re.fullmatch(
            r"[A-Za-z0-9_.-]+==[^ ]+ --hash=sha256:[0-9a-f]{64}",
            requirement,
        )
        assert re.search(r"--hash=sha256:[0-9a-f]{64}", block)


def test_every_python_mode_uses_the_exact_lock() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    script = PREPUSH.read_text(encoding="utf-8")
    installer = PYTHON_LOCK_INSTALLER.read_text(encoding="utf-8")
    validator = PYTHON_LOCK_VALIDATOR.read_text(encoding="utf-8")
    for function in (
        "run_quality",
        "run_python_main",
        "run_capacity",
        "run_python_coverage",
        "run_api_contract",
        "run_rust",
        "run_security",
        "run_redis",
        "run_postgres",
    ):
        body = re.search(rf"^{function}\(\) \{{\n(.*?)(?=^\}}\n)", script, re.M | re.S)
        assert body is not None
        assert "install_python_dependencies" in body.group(1)
    for function in ("run_hygiene", "run_container_package"):
        body = re.search(rf"^{function}\(\) \{{\n(.*?)(?=^\}}\n)", script, re.M | re.S)
        assert body is not None
        assert "install_python_dependencies" not in body.group(1)
    assert "scripts/ci/install_python_lock" in script
    assert len(
        re.findall(r"^\s+scripts/ci/install_python_lock$", script, re.M)
    ) == 2
    assert "--require-hashes" in installer
    assert "--only-binary=:all:" in installer
    assert "--force-reinstall" in installer
    assert "-r requirements-ci.lock" in installer
    assert "python -m pip check" in installer
    assert "validate_python_lock_inputs" in installer
    assert "install report does not match the complete lock" in installer
    for input_name in ("requirements.txt", "requirements-dev.txt", "requirements-ci.in"):
        assert input_name in validator
    assert "pip install --upgrade pip" not in script
    assert "pip install -r requirements-dev.txt" not in script
    assert "pip install coverage==" not in script
    assert "pip install pip-audit" not in script
    assert "sha256sum requirements-ci.in" in script
    assert "requirements-ci.lock scripts/ci/install_python_lock" in script
    assert "python -m pip freeze --all" in script
    assert 'PYTHON_VERSION: "3.14.6"' in workflow
    assert workflow.count("cache-dependency-path: requirements-ci.lock") == 5
    assert "actions/setup-python@" not in _job(workflow, "container-package")


def test_rust_gate_reuses_only_the_exact_baked_cargo_tools() -> None:
    script = PREPUSH.read_text(encoding="utf-8")

    assert (
        '"$(cargo llvm-cov --version 2>/dev/null || true)" '
        '!= "cargo-llvm-cov 0.8.7"'
    ) in script
    assert "cargo install cargo-llvm-cov --version 0.8.7 --locked" in script
    assert (
        '"$(cargo audit --version 2>/dev/null || true)" '
        '!= "cargo-audit-audit 0.22.2"'
    ) in script
    assert "cargo install cargo-audit --version 0.22.2 --locked" in script


def test_container_and_lock_generator_use_exact_inputs() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    generator = PYTHON_LOCK_GENERATOR.read_text(encoding="utf-8")
    assert (
        "rust:1.97.1-slim-trixie@sha256:"
        "fc0648ac2962539be80bd424729a20fd80f7b64bfba7e90bbd642aed6c697c5a"
    ) in dockerfile
    assert (
        "python:3.14.6-slim-trixie@sha256:"
        "b921fe7e7522f828d45197a47656ec465a9b15689b27fa8e1fba2864fca5b967"
    ) in dockerfile
    assert "PREPUSH_PIP_REPORT=/tmp/python-lock-install-report.json /wheels/scripts/ci/install_python_lock" in dockerfile
    assert "requirements.txt requirements-dev.txt requirements-ci.in" in dockerfile
    assert "scripts/ci/install_python_lock" in dockerfile
    assert "scripts/ci/validate_python_lock_inputs" in dockerfile
    assert "--require-hashes" in dockerfile
    assert "--only-binary=:all:" in dockerfile
    assert "maturin==1\\.14\\.1 --hash=sha256:" in dockerfile
    assert "pip install --no-compile --upgrade pip" not in dockerfile
    assert "-r /wheels/requirements-dev.txt" not in dockerfile
    assert "test \"$(python -m pip --version | awk '{print $2}')\" = 26.1.2" in generator
    assert "pip-tools" not in generator
    assert "--report \"$temporary_directory/selected.json\"" in generator
    assert "non-wheel or malformed selected artifact" in generator
    assert "input-sha256.before" in generator
    assert "input-sha256.after" in generator
    assert "requirements-ci.lock.candidate" in generator
    assert "Python requirement inputs changed during lock generation" in generator
    assert "mv -- \"$candidate_lock\" requirements-ci.lock" in generator
