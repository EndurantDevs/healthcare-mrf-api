# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Privacy-safe progress and failure receipts for the projection-v3 census."""

from __future__ import annotations

import hashlib
import json
import sys
from types import SimpleNamespace

import pytest

from scripts.research import plan_pricing_projection_v3_census as census
from tests.test_plan_pricing_projection_v3_census import TARGET_CLI_ARGS


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

    with pytest.raises(RuntimeError, match="private-context-detail"):
        await census._measure_release(
            object(),
            SimpleNamespace(),
            checkpoint,
            lambda phase, message: stages.append((phase, message)),
        )

    assert stages == [
        ("preparing release context", "preparing bounded release context")
    ]
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
    monkeypatch.setattr(census, "_source_identity", lambda _args: {})

    async def fail_with_stage(_args, receipt_by_field):
        receipt_by_field["phase"] = "measuring code inputs"
        receipt_by_field["message"] = "measuring bounded code inputs"
        raise RuntimeError("private-price-identity")

    monkeypatch.setattr(census, "run_census", fail_with_stage)

    assert census.census_main() == 1
    receipt_text = receipt_path.read_text(encoding="utf-8")
    receipt_by_field = json.loads(receipt_text)
    assert receipt_by_field["phase"] == "measuring code inputs"
    assert receipt_by_field["message"] == "measuring bounded code inputs"
    assert receipt_by_field["error"] == {"type": "RuntimeError"}
    assert "private-price-identity" not in receipt_text


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
