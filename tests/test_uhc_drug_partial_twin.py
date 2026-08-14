# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_twin as twin
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256
from tests.test_uhc_drug_twin import CUTOFF
from tests.uhc_drug_parser_test_support import artifact_set


@pytest.mark.asyncio
async def test_artifact_recheck_reopens_the_exact_selected_ids(monkeypatch) -> None:
    artifacts, _bodies = artifact_set()
    selected_artifacts = artifacts.artifacts[:-1]
    selected = replace(
        artifacts,
        artifacts=selected_artifacts,
        artifact_set_sha256=artifact_set_sha256(selected_artifacts),
    )
    request = twin._TwinRequest(
        artifacts=selected,
        baseline_run_id="uhc-baseline",
        candidate_run_id="uhc-candidate",
        cutoff_at=CUTOFF,
        work_directory=Path("/unused"),
        database=object(),
        repository=None,
    )
    identities = tuple(artifact.identity for artifact in artifacts.artifacts)
    load_identities = AsyncMock(return_value=identities)
    load_selected = AsyncMock(return_value=selected)
    monkeypatch.setattr(twin, "load_source_artifact_identities", load_identities)
    monkeypatch.setattr(twin, "load_selected_source_artifact_set", load_selected)

    await twin._require_artifacts_unchanged(request)

    load_identities.assert_awaited_once_with(
        selected.source_id, selected.source_file_set_sha256, database=request.database
    )
    load_selected.assert_awaited_once_with(
        identities,
        selected_source_file_ids=tuple(
            artifact.identity.source_file_id for artifact in selected.artifacts
        ),
        database=request.database,
    )
