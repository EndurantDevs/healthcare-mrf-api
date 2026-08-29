# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Privacy-safe progress and failure receipts for the projection-v3 census."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as code_stage
from api import ptg2_serving as serving
from scripts.research import plan_pricing_projection_v3_census as census
from scripts.research import (
    plan_pricing_projection_v3_census_diagnostics as diagnostics,
)
from scripts.research import (
    plan_pricing_projection_v3_census_transaction as transaction,
)
from tests.test_plan_pricing_projection_v3_census import TARGET_CLI_ARGS
from tests.test_plan_pricing_projection_v3_code_bounds import (
    _numeric_alias_binding,
    _numeric_alias_code_rows,
)
from tests.test_plan_pricing_projection_v3 import _ExecuteSession


def _set_census_argv(monkeypatch, receipt_path) -> None:
    """Bind one synthetic census CLI receipt target."""

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "plan-pricing-v3-census",
            "--plan-release-id",
            "hprelease_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            *TARGET_CLI_ARGS,
            "--expected-source-sha",
            "0" * 40,
            "--expected-source-manifest-sha256",
            "f" * 64,
            "--receipt",
            str(receipt_path),
        ],
    )


@pytest.mark.asyncio
async def test_census_diagnostic_stages_identify_price_substages(
    monkeypatch,
) -> None:
    monkeypatch.setattr(serving, "_declared_geo_rate_count", lambda _code_rows: 1)
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(return_value={1: [{"negotiated_rate": "10"}]}),
    )
    observed_stages = []

    async def diagnostic_stage(stage: str) -> None:
        observed_stages.append(stage)

    assert await code_stage._has_staged_code_inputs(
        _ExecuteSession(),
        projection._BuildState(hashlib.sha256()),
        ("CPT", "27447"),
        [_numeric_alias_binding()],
        binding_code_rows=_numeric_alias_code_rows,
        stage_code_provider_sets=AsyncMock(),
        preflight_price_membership_aliases=AsyncMock(),
        diagnostic_stage=diagnostic_stage,
    )

    assert observed_stages == [
        "reset_code_inputs",
        "code_layout",
        "price_membership_metadata",
        "price_hydration",
        "provider_set_staging",
        "code_occurrence_staging",
    ]


@pytest.mark.asyncio
async def test_checkpoint_precedes_release_context_loading(
    monkeypatch,
    tmp_path,
) -> None:
    checkpoints = []
    stages = []
    checkpoint_path = tmp_path / "initial-checkpoint.json"

    def checkpoint(progress_by_field):
        checkpoints.append(progress_by_field)
        census.write_json(checkpoint_path, {"progress": dict(progress_by_field)})

    async def fail_context(*_args):
        persisted = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        assert persisted["progress"]["stage"] == "preparing_release_context"
        raise RuntimeError("private-context-detail")

    monkeypatch.setattr(census, "_prepare_context", fail_context)

    async def set_stage(stage):
        stages.append(stage)

    with pytest.raises(RuntimeError, match="private-context-detail"):
        await census._measure_release(
            object(),
            SimpleNamespace(),
            checkpoint,
            set_stage,
        )

    assert stages == ["preparing_release_context"]
    assert checkpoints == [
        {
            "stage": "preparing_release_context",
            "code_identities_processed": 0,
            "codes_with_rates_measured": 0,
        }
    ]


@pytest.mark.asyncio
async def test_checkpoint_precedes_the_first_code_failure(monkeypatch) -> None:
    context = SimpleNamespace(
        state=census._BuildState(hashlib.sha256()),
        code_identities=[("CPT", "private-code")],
    )
    checkpoints = []

    async def fail_first_code(*_args, **_kwargs):
        raise RuntimeError("private failure detail")

    monkeypatch.setattr(census, "_has_measured_code", fail_first_code)

    with pytest.raises(RuntimeError, match="private failure detail"):
        await census._measure_codes(object(), context, checkpoints.append)

    assert len(checkpoints) == 1
    assert checkpoints[0]["code_identities_processed"] == 0
    assert checkpoints[0]["codes_with_rates_measured"] == 0
    assert checkpoints[0]["price_membership_metadata"] == {
        "price_membership_cached_block_count": 0,
        "price_membership_identity_retained_bytes": 0,
        "price_membership_metadata_fragment_count": 0,
        "price_membership_maximum_fragments_per_block": 0,
        "price_membership_singleton_peak_bytes": 0,
    }
    assert "private-code" not in json.dumps(checkpoints[0])


def test_failure_retains_only_privacy_safe_stage_and_error_type(
    monkeypatch,
    tmp_path,
) -> None:
    receipt_path = tmp_path / "failed-census.json"
    _set_census_argv(monkeypatch, receipt_path)
    monkeypatch.setattr(census, "_source_identity", lambda _args: {})

    async def fail_with_stage(_args, receipt_by_field):
        receipt_by_field["phase"] = "measuring code inputs"
        receipt_by_field["message"] = "measuring bounded code inputs"
        receipt_by_field["cap_calibration_admissible"] = True
        receipt_by_field["resource_proof_admissible"] = True
        raise RuntimeError("private-price-identity")

    monkeypatch.setattr(census, "run_census", fail_with_stage)

    assert census.census_main() == 1
    receipt_text = receipt_path.read_text(encoding="utf-8")
    receipt_by_field = json.loads(receipt_text)
    assert receipt_by_field["phase"] == "measuring code inputs"
    assert receipt_by_field["message"] == "measuring bounded code inputs"
    assert receipt_by_field["cap_calibration_admissible"] is False
    assert receipt_by_field["resource_proof_admissible"] is False
    assert receipt_by_field["error"] == {"type": "RuntimeError"}
    assert "private-price-identity" not in receipt_text


@pytest.mark.asyncio
async def test_database_stage_is_closed_and_bound_to_application_name() -> None:
    """Reject a database stage outside the closed attribution contract."""

    session = AsyncMock()
    run_token = "a" * 12
    with pytest.raises(ValueError, match="database stage is invalid"):
        await transaction.set_census_database_stage(
            session,
            run_token,
            "private-price-id",
            transaction.census_database_application_name(run_token, "setup"),
        )
    session.execute.assert_not_awaited()


def test_database_identity_binds_exact_runtime_and_postgresql_limit() -> None:
    runtime_by_field = {
        "job_name": "census-job",
        "pod_uid": "pod-uid",
        "image_digest": "sha256:" + "c" * 64,
    }
    run_token = transaction.census_database_run_token(runtime_by_field)
    changed_token = transaction.census_database_run_token(
        {**runtime_by_field, "pod_uid": "changed"}
    )

    assert run_token != changed_token
    assert (
        max(
            len(transaction.census_database_application_name(run_token, stage).encode())
            for stage in transaction.CENSUS_DATABASE_STAGE_KEYS
        )
        <= 63
    )
    with pytest.raises(ValueError, match="identity is incomplete"):
        transaction.census_database_run_token({})


@pytest.mark.parametrize(
    ("error_type", "error_dimension"),
    (
        (
            census._PriceMembershipMetadataReadLimitError,
            "price_membership_metadata",
        ),
        (census._PriceHydrationReadLimitError, "price_hydration"),
    ),
)
def test_failure_retains_allowlisted_limit_dimension(
    monkeypatch,
    tmp_path,
    error_type,
    error_dimension,
) -> None:
    receipt_path = tmp_path / f"{error_dimension}.json"
    _set_census_argv(monkeypatch, receipt_path)
    monkeypatch.setattr(census, "_source_identity", lambda _args: {})

    async def fail_with_limit(_args, _receipt_by_field):
        raise error_type("private-limit-detail")

    monkeypatch.setattr(census, "run_census", fail_with_limit)

    assert census.census_main() == 1
    receipt_text = receipt_path.read_text(encoding="utf-8")
    assert json.loads(receipt_text)["error"] == {
        "type": error_type.__name__,
        "dimension": error_dimension,
    }
    assert "private-limit-detail" not in receipt_text


@pytest.mark.parametrize("signal_number", (signal.SIGINT, signal.SIGTERM))
def test_process_signal_cancels_then_seals_after_cleanup(
    monkeypatch,
    tmp_path,
    signal_number,
) -> None:
    receipt_path = tmp_path / f"signal-{signal_number}.json"
    _set_census_argv(monkeypatch, receipt_path)
    monkeypatch.setattr(census, "_source_identity", lambda _args: {})

    async def wait_for_signal(_args, receipt_by_field):
        census.asyncio.get_running_loop().call_soon(
            os.kill,
            os.getpid(),
            signal_number,
        )
        try:
            await census.asyncio.Event().wait()
        finally:
            os.kill(os.getpid(), signal_number)
            receipt_by_field["rollback_complete"] = True
            receipt_by_field["temporary_relations_after_rollback"] = []

    monkeypatch.setattr(census, "run_census", wait_for_signal)
    previous_handler = signal.getsignal(signal_number)

    assert census.census_main() == 128 + signal_number
    assert signal.getsignal(signal_number) == previous_handler
    receipt_by_field = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt_by_field["rollback_complete"] is True
    assert receipt_by_field["temporary_relations_after_rollback"] == []
    assert receipt_by_field["error"] == {
        "type": "_CensusInterrupted",
        "signal": signal.Signals(signal_number).name,
    }


def test_process_signal_during_final_seal_rewrites_failed_receipt(
    monkeypatch,
    tmp_path,
) -> None:
    receipt_path = tmp_path / "late-signal.json"
    _set_census_argv(monkeypatch, receipt_path)

    async def succeed(_args, receipt_by_field):
        receipt_by_field.update(
            status="complete",
            accepted=True,
            cap_calibration_admissible=True,
            resource_proof_admissible=True,
        )
        return 0

    writes = []

    def signal_during_first_write(path, value):
        writes.append(json.loads(json.dumps(value)))
        if len(writes) == 1:
            os.kill(os.getpid(), signal.SIGTERM)
        path.write_text(json.dumps(value), encoding="utf-8")

    monkeypatch.setattr(census, "run_census", succeed)
    monkeypatch.setattr(diagnostics, "write_json", signal_during_first_write)

    assert census.census_main() == 128 + signal.SIGTERM
    assert len(writes) == 2
    final_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert final_receipt["cap_calibration_admissible"] is False
    assert final_receipt["resource_proof_admissible"] is False
    assert final_receipt["error"] == {
        "type": "_CensusInterrupted",
        "signal": "SIGTERM",
    }
