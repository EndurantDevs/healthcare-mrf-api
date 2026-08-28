# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cross-boundary proofs for exact invalid-price recovery policy."""

from __future__ import annotations

import datetime as dt
import importlib

import pytest

from api import control, control_imports
from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    invalid_price_exclusion_evidence,
    invalid_price_exclusion_policy,
    invalid_price_exclusion_source,
)
from tests import test_ptg2_prepared_shared_layout as prepared_layout_tests
from tests import test_ptg2_shared_reuse as shared_reuse_tests


ptg = importlib.import_module("process.ptg")


def _policy() -> dict[str, object]:
    return invalid_price_exclusion_policy(
        [
            invalid_price_exclusion_source(
                raw_source_sha256="a" * 64,
                entries=[
                    {
                        "object_ordinal": 1,
                        "rate_ordinal": 2,
                        "price_ordinal": 3,
                        "invalid_value_sha256": "b" * 64,
                    }
                ],
                emptied_rate_count=0,
            )
        ]
    )


@pytest.mark.asyncio
async def test_prepared_layout_seals_invalid_price_exclusion(monkeypatch, tmp_path):
    mocks = prepared_layout_tests._prepared_layout_mocks(monkeypatch, tmp_path)
    policy = _policy()
    evidence = invalid_price_exclusion_evidence(policy)
    support_payloads = []

    def support_digest(support_by_name):
        support_payloads.append(support_by_name)
        return b"s" * 32

    monkeypatch.setattr(snapshot_publish, "shared_support_digest", support_digest)
    publication = await prepared_layout_tests._publish_prepared_layout(
        mocks,
        tmp_path,
        invalid_price_exclusion=evidence,
    )

    assert publication.serving_index["invalid_price_exclusion"] == evidence
    assert mocks.seal.await_args.kwargs["layout_manifest"]["serving_index"]["invalid_price_exclusion"] == evidence
    assert any(
        support_by_name.get("invalid_price_exclusion") == policy["sha256"] for support_by_name in support_payloads
    )


def test_invalid_price_exclusion_isolates_physical_layout_identity():
    downloaded = shared_reuse_tests._downloaded()
    policy = _policy()
    baseline = shared_reuse_tests._identity([downloaded])
    recovered = shared_reuse_tests._identity(
        [downloaded],
        invalid_price_exclusion=policy,
    )

    assert baseline.payload["physical_options"] == {}
    assert recovered.payload["physical_options"] == {"invalid_price_exclusion_policy": policy["sha256"]}
    assert recovered.semantic_fingerprint != baseline.semantic_fingerprint
    assert recovered.coverage_scope_id == baseline.coverage_scope_id


def test_invalid_price_exclusion_isolates_snapshot_identity():
    policy = _policy()
    baseline_options = ptg._ptg2_snapshot_content_options({})
    explicit_none_options = ptg._ptg2_snapshot_content_options(
        {"invalid_price_exclusion_policy": None}
    )
    recovered_options = ptg._ptg2_snapshot_content_options(
        {"invalid_price_exclusion_policy": policy}
    )
    identity_by_option = {
        option_name: ptg._ptg2_deterministic_snapshot_id(
            import_month=dt.date(2026, 8, 1),
            import_id="same-import",
            option_by_name=option_by_name,
        )
        for option_name, option_by_name in {
            "baseline": {},
            "explicit_none": {"invalid_price_exclusion_policy": None},
            "recovered": {"invalid_price_exclusion_policy": policy},
        }.items()
    }

    assert baseline_options == explicit_none_options
    assert recovered_options["invalid_price_exclusion_policy"] == policy["sha256"]
    assert identity_by_option["baseline"] == identity_by_option["explicit_none"]
    assert identity_by_option["recovered"] != identity_by_option["baseline"]


def test_control_api_rejects_policy_without_frozen_source_set():
    request_by_field = {
        "importer": "ptg",
        "params": {"invalid_price_exclusion_policy": {}},
    }

    with pytest.raises(ValueError, match="requires a frozen rate file set"):
        control._validated_control_import_payload(request_by_field)


def test_control_response_redacts_orphaned_private_policy():
    assert control_imports._params_for_import_run_response(
        "ptg",
        {
            "source_key": "source-a",
            "invalid_price_exclusion_policy": {"private": "value"},
        },
    ) == {"source_key": "source-a"}
