# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import process.formulary_fhir.uhc_drug_acquisition as acquisition


def install_postflight_drift_mocks(
    monkeypatch: Any,
    *,
    binding: Any,
    changed_binding: Any,
    registration: Any,
    exact_artifacts: Any,
) -> None:
    """Install a claimed acquisition whose final source binding drifts."""

    claim = acquisition.UHCDrugSourceAcquisitionClaim(
        source_id=binding.source_id,
        lease_generation=1,
        lease_token="7" * 64,
    )

    async def run_claimed(_source_id: str, operation: Any, *, database: Any):
        del database
        return await operation(claim)

    monkeypatch.setattr(
        acquisition,
        "register_uhc_formulary_source",
        AsyncMock(side_effect=[binding, changed_binding]),
    )
    monkeypatch.setattr(
        acquisition,
        "register_uhc_source_file_set",
        AsyncMock(return_value=registration),
    )
    monkeypatch.setattr(acquisition, "require_source_unchanged", AsyncMock())
    monkeypatch.setattr(
        acquisition,
        "require_active_uhc_drug_source_acquisition",
        AsyncMock(),
    )
    monkeypatch.setattr(
        acquisition,
        "run_with_uhc_drug_source_acquisition_lease",
        run_claimed,
    )
    monkeypatch.setattr(acquisition, "pending_source_files", AsyncMock(return_value=()))
    monkeypatch.setattr(
        acquisition,
        "acquire_pending_uhc_drug_artifacts",
        AsyncMock(return_value=(0, ())),
    )
    monkeypatch.setattr(
        acquisition,
        "load_complete_source_artifact_set",
        AsyncMock(return_value=exact_artifacts),
    )
    monkeypatch.setattr(
        acquisition,
        "validate_retained_uhc_drug_artifact",
        lambda *_arguments, **_keywords: 1,
    )
