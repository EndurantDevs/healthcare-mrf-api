# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission-lock boundaries for complete and partial Flex publication."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import uhc_flex_practitioner_publication as publication
from process import uhc_flex_practitioner_publication_store as store
from tests.test_uhc_flex_practitioner_publication import _single_root_admission
from tests.test_uhc_flex_practitioner_publication_store_boundaries import (
    _identity_and_admission,
)


@pytest.mark.asyncio
async def test_admission_identity_and_source_fail_closed(monkeypatch) -> None:
    identity, admission = _identity_and_admission()
    assert store._is_expected_admission(
        admission,
        admission.candidate_acquisition_id,
    )
    assert not store._is_expected_admission(
        object(),
        admission.candidate_acquisition_id,
    )
    assert not store._is_expected_admission(admission, "pdufpa_" + "0" * 48)

    missing_source_database = SimpleNamespace(
        scalar=AsyncMock(),
        first=AsyncMock(return_value=None),
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="source has drifted",
    ):
        await store._lock_admission(
            missing_source_database,
            admission.candidate_acquisition_id,
            identity.endpoint_id,
        )

    source_database = SimpleNamespace(
        scalar=AsyncMock(),
        first=AsyncMock(side_effect=[{"source_id": admission.source_id}]),
    )
    monkeypatch.setattr(
        store,
        "require_uhc_flex_practitioner_admission",
        AsyncMock(return_value=object()),
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="admission is invalid",
    ):
        await store._lock_admission(
            source_database,
            admission.candidate_acquisition_id,
            identity.endpoint_id,
        )


@pytest.mark.asyncio
async def test_admission_lock_accepts_complete_and_partial_roots(monkeypatch) -> None:
    identity, admission = _identity_and_admission()
    source_by_field = {"source_id": admission.source_id}
    database = SimpleNamespace(scalar=AsyncMock(), first=AsyncMock())
    admission_loader = AsyncMock(return_value=admission)
    monkeypatch.setattr(
        store,
        "require_uhc_flex_practitioner_admission",
        admission_loader,
    )
    database.first.side_effect = [
        source_by_field,
        {
            "status": "sealed",
            "cohort_complete": True,
            "error_count": 0,
            "terminal_set_sha256": admission.terminal_set_sha256,
            "resource_count": admission.resource_count,
        },
    ]
    assert await store._lock_admission(
        database,
        admission.candidate_acquisition_id,
        identity.endpoint_id,
    ) == (admission, 0)

    partial = _single_root_admission(error_count=1)
    admission_loader.return_value = partial
    database.first.side_effect = [
        source_by_field,
        {
            "status": "sealed",
            "cohort_complete": False,
            "error_count": 1,
            "terminal_set_sha256": partial.terminal_set_sha256,
            "resource_count": partial.resource_count,
        },
    ]
    assert await store._lock_admission(
        database,
        partial.candidate_acquisition_id,
        identity.endpoint_id,
    ) == (partial, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    (
        {"status": "building"},
        {"cohort_complete": True},
        {"error_count": -1},
        {"terminal_set_sha256": "d" * 64},
        {"resource_count": 2},
    ),
)
async def test_admission_lock_rejects_candidate_state_drift(
    monkeypatch,
    drift,
) -> None:
    admission = _single_root_admission(error_count=1)
    candidate_by_field = {
        "status": "sealed",
        "cohort_complete": False,
        "error_count": 1,
        "terminal_set_sha256": admission.terminal_set_sha256,
        "resource_count": admission.resource_count,
        **drift,
    }
    database = SimpleNamespace(
        scalar=AsyncMock(),
        first=AsyncMock(
            side_effect=[{"source_id": admission.source_id}, candidate_by_field]
        ),
    )
    monkeypatch.setattr(
        store,
        "require_uhc_flex_practitioner_admission",
        AsyncMock(return_value=admission),
    )

    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="admission is invalid",
    ):
        await store._lock_admission(
            database,
            admission.candidate_acquisition_id,
            publication.uhc_flex_practitioner_endpoint_identity().endpoint_id,
        )
