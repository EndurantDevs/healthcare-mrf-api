# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Publication-lock ordering for exact Profile dataset fences."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_artifact_fence_takes_publication_lock_before_row_locks(
    monkeypatch,
):
    events: list[str] = []
    fence = SimpleNamespace(
        datasets=(SimpleNamespace(source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID),)
    )

    def async_step(name: str, return_value: object = None) -> AsyncMock:
        return AsyncMock(
            side_effect=lambda *_args, **_kwargs: (
                events.append(name),
                return_value,
            )[1]
        )

    monkeypatch.setattr(
        importer,
        "lock_uhc_flex_profile_publication",
        async_step("publication"),
    )
    for function_name, event_name, return_value in (
        ("_lock_artifact_fence_endpoint_advisories", "endpoint_advisory", None),
        ("_lock_artifact_fence_endpoints", "endpoint_rows", None),
        ("_lock_artifact_fence_aliases", "source_rows", []),
        ("_artifact_fence_dataset_rows", "dataset_rows", []),
        ("_artifact_eligible_validated_ids", "eligible_rows", {}),
        ("_assert_uhc_flex_profile_fence_ready", "readiness", None),
    ):
        monkeypatch.setattr(
            importer,
            function_name,
            async_step(event_name, return_value),
        )
    monkeypatch.setattr(
        importer,
        "_assert_locked_artifact_fence_aliases",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        importer,
        "_assert_locked_artifact_fence_datasets",
        lambda *_args: None,
    )

    await importer._lock_and_verify_artifact_dataset_fence(fence, object())
    assert events[0] == "publication"
    assert events[-1] == "readiness"
