# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from click.testing import CliRunner

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process as process_cli


def _assert_cli_forwards(
    monkeypatch,
    *,
    command,
    target_name: str,
    args: list[str],
    expected: dict,
) -> None:
    """Assert that Click parsing reaches exactly one importer invocation."""

    coroutine_token = object()
    target_calls: list[dict] = []
    run_calls: list[object] = []

    def fake_target(**kwargs):
        target_calls.append(kwargs)
        return coroutine_token

    monkeypatch.setattr(process_cli, target_name, fake_target)
    monkeypatch.setattr(process_cli, "_run", run_calls.append)

    result = CliRunner().invoke(command, args)

    assert result.exit_code == 0, result.output
    assert result.exception is None
    assert target_calls == [expected]
    assert run_calls == [coroutine_token]


@pytest.mark.parametrize(
    ("command", "target_name", "args", "expected"),
    [
        (
            process_cli.mrf,
            "initiate_mrf",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.plan_attributes,
            "initiate_plan_attributes",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.nucc,
            "initiate_nucc",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.code_sets,
            "initiate_code_sets",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.terminology_synonyms,
            "initiate_terminology_synonyms",
            ["--test", "--import-id", "terms-1"],
            {"test_mode": True, "import_id": "terms-1"},
        ),
        (
            process_cli.claims_procedures,
            "initiate_claims_pricing",
            ["--test", "--import-id", "procedures-1"],
            {"test_mode": True, "import_id": "procedures-1"},
        ),
        (
            process_cli.drug_claims,
            "initiate_drug_claims",
            ["--test", "--import-id", "drugs-1"],
            {"test_mode": True, "import_id": "drugs-1"},
        ),
        (
            process_cli.provider_quality,
            "initiate_provider_quality",
            ["--test", "--import-id", "quality-1"],
            {"test_mode": True, "import_id": "quality-1"},
        ),
        (
            process_cli.provider_enrichment,
            "initiate_provider_enrichment",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.partd_formulary_network,
            "initiate_partd_formulary_network",
            ["--test", "--import-id", "partd-1"],
            {"test_mode": True, "import_id": "partd-1"},
        ),
        (
            process_cli.pharmacy_license,
            "initiate_pharmacy_license",
            ["--test", "--import-id", "licenses-1"],
            {"test_mode": True, "import_id": "licenses-1"},
        ),
        (
            process_cli.places_zcta,
            "initiate_places_zcta",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.lodes,
            "initiate_lodes",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.medicare_enrollment,
            "initiate_medicare_enrollment",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.cms_doctors,
            "initiate_cms_doctors",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.facility_anchors,
            "initiate_facility_anchors",
            ["--test"],
            {"test_mode": True},
        ),
        (
            process_cli.pharmacy_economics,
            "initiate_pharmacy_economics",
            ["--test"],
            {"test_mode": True},
        ),
    ],
)
def test_simple_start_commands_preserve_importer_arguments(
    monkeypatch,
    command,
    target_name: str,
    args: list[str],
    expected: dict,
) -> None:
    _assert_cli_forwards(
        monkeypatch,
        command=command,
        target_name=target_name,
        args=args,
        expected=expected,
    )


def test_npi_cli_has_no_live_test_mode_and_dispatches_without_parameters(
    monkeypatch,
) -> None:
    _assert_cli_forwards(
        monkeypatch,
        command=process_cli.npi,
        target_name="initiate_npi",
        args=[],
        expected={},
    )
    rejected = CliRunner().invoke(process_cli.npi, ["--test"])
    assert rejected.exit_code == 2
    assert "No such option '--test'" in rejected.output


def test_npi_worker_registers_only_the_control_wrapper():
    assert process_cli.NPI.functions == [process_cli.control_single_job_start]
    assert not hasattr(process_cli, "NPI_finish")


@pytest.mark.parametrize(
    ("command", "target_name", "args", "expected"),
    [
        (
            process_cli.mrf_end,
            "finish_mrf",
            ["--test", "--import-id", "mrf-1"],
            {"test_mode": True, "import_id": "mrf-1"},
        ),
        (
            process_cli.claims_pricing_end,
            "finish_claims_pricing",
            [
                "--test",
                "--import-id",
                "claims-1",
                "--run-id",
                "run-1",
                "--manifest-path",
                "/tmp/claims.json",
            ],
            {
                "import_id": "claims-1",
                "run_id": "run-1",
                "test_mode": True,
                "manifest_path": "/tmp/claims.json",
            },
        ),
        (
            process_cli.drug_claims_end,
            "finish_drug_claims",
            ["--import-id", "drugs-1", "--run-id", "run-2"],
            {
                "import_id": "drugs-1",
                "run_id": "run-2",
                "test_mode": False,
                "manifest_path": None,
            },
        ),
        (
            process_cli.provider_quality_end,
            "finish_provider_quality",
            ["--import-id", "quality-1", "--run-id", "run-3"],
            {
                "import_id": "quality-1",
                "run_id": "run-3",
                "test_mode": False,
                "manifest_path": None,
            },
        ),
        (
            process_cli.partd_formulary_network_end,
            "finish_partd_formulary_network",
            ["--import-id", "partd-1", "--run-id", "run-4"],
            {
                "import_id": "partd-1",
                "run_id": "run-4",
                "test_mode": False,
                "manifest_path": None,
            },
        ),
        (
            process_cli.pharmacy_license_end,
            "finish_pharmacy_license",
            ["--import-id", "licenses-1", "--run-id", "run-5"],
            {
                "import_id": "licenses-1",
                "run_id": "run-5",
                "test_mode": False,
                "manifest_path": None,
            },
        ),
    ],
)
def test_finalize_commands_preserve_run_identity(
    monkeypatch,
    command,
    target_name: str,
    args: list[str],
    expected: dict,
) -> None:
    _assert_cli_forwards(
        monkeypatch,
        command=command,
        target_name=target_name,
        args=args,
        expected=expected,
    )


def test_run_uses_uvloop_only_when_asyncio_runner_is_unmodified(monkeypatch) -> None:
    coroutine_token = object()
    uvloop_run = Mock(return_value="uvloop-result")
    monkeypatch.setattr(
        process_cli,
        "uvloop",
        SimpleNamespace(run=uvloop_run),
    )
    monkeypatch.setattr(process_cli.asyncio, "run", process_cli._ASYNCIO_RUN)

    assert process_cli._run(coroutine_token) == "uvloop-result"
    uvloop_run.assert_called_once_with(coroutine_token)

    asyncio_run = Mock(return_value="asyncio-result")
    monkeypatch.setattr(process_cli.asyncio, "run", asyncio_run)

    assert process_cli._run(coroutine_token) == "asyncio-result"
    asyncio_run.assert_called_once_with(coroutine_token)
    assert uvloop_run.call_count == 1


@pytest.mark.asyncio
async def test_candidate_audit_startup_requires_and_accepts_uvloop(monkeypatch) -> None:
    ctx_by_key = {"worker": "audit"}
    startup = AsyncMock()
    monkeypatch.setattr(process_cli, "db_startup", startup)
    monkeypatch.setattr(process_cli, "uvloop", None)

    with pytest.raises(RuntimeError, match="requires uvloop"):
        await process_cli._ptg_candidate_audit_startup(ctx_by_key)

    class FakeUVLoop:
        pass

    monkeypatch.setattr(
        process_cli,
        "uvloop",
        SimpleNamespace(Loop=FakeUVLoop),
    )
    monkeypatch.setattr(
        process_cli.asyncio,
        "get_running_loop",
        lambda: FakeUVLoop(),
    )

    await process_cli._ptg_candidate_audit_startup(ctx_by_key)

    startup.assert_awaited_once_with(ctx_by_key)


@pytest.mark.parametrize(
    ("raw", "default", "expected"),
    [
        (None, 7, 7),
        (" 12 ", 7, 12),
        ("not-an-integer", 7, 7),
    ],
)
def test_worker_integer_environment_has_safe_fallbacks(
    monkeypatch,
    raw: str | None,
    default: int,
    expected: int,
) -> None:
    name = "HLTHPRT_TEST_WORKER_INTEGER"
    if raw is None:
        monkeypatch.delenv(name, raising=False)
    else:
        monkeypatch.setenv(name, raw)

    assert process_cli._worker_int_env(name, default) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, 86400),
        ("518400", 518400),
        ("invalid", 86400),
        ("0", 1),
    ],
)
def test_npi_job_timeout_is_configurable_and_positive(
    monkeypatch,
    raw: str | None,
    expected: int,
) -> None:
    if raw is None:
        monkeypatch.delenv("HLTHPRT_NPI_JOB_TIMEOUT", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_NPI_JOB_TIMEOUT", raw)

    assert process_cli._npi_job_timeout() == expected


@pytest.mark.parametrize(
    ("raw", "max_jobs", "expected"),
    [
        (None, 2, 16),
        ("0", 2, 1),
        ("not-an-integer", 5, 20),
    ],
)
def test_ptg_queue_read_limit_is_bounded_and_recovers_from_invalid_env(
    monkeypatch,
    raw: str | None,
    max_jobs: int,
    expected: int,
) -> None:
    name = "HLTHPRT_TEST_QUEUE_READ_LIMIT"
    if raw is None:
        monkeypatch.delenv(name, raising=False)
    else:
        monkeypatch.setenv(name, raw)

    assert process_cli._ptg_queue_read_limit(name, max_jobs) == expected


def test_ptg_job_timeout_never_allows_nonpositive_lane_values(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_PTG_JOB_TIMEOUT", "invalid")
    monkeypatch.setenv("HLTHPRT_TEST_JOB_TIMEOUT", "0")

    assert process_cli._ptg_job_timeout() == 172800
    assert process_cli._ptg_job_timeout("HLTHPRT_TEST_JOB_TIMEOUT") == 1
    assert process_cli._ptg_max_jobs("HLTHPRT_TEST_JOB_TIMEOUT", 8) == 1


def test_florida_profile_command_and_worker_are_registered_together() -> None:
    command = process_cli.process_group.commands["florida-mqa-profile"]
    worker = process_cli.FloridaMQAProfile

    assert command is process_cli.florida_mqa_profile
    assert worker.functions == [process_cli.control_single_job_start]
    assert worker.on_startup is process_cli.db_startup
    assert worker.queue_name == "arq:FloridaMQAProfile"
    assert worker.max_jobs == 1
    assert worker.queue_read_limit == 1
    assert worker.job_timeout == 24 * 60 * 60
