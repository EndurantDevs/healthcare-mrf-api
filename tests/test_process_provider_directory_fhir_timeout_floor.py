# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from typing import Any

import pytest

importer = importlib.import_module("process.provider_directory_fhir")


def _reviewed_timeout_source() -> dict[str, str]:
    reviewed_base = importer.REVIEWED_PRACTITIONER_ROLE_TIMEOUT_BASE
    return {
        "source_id": "source_reviewed_timeout",
        "api_base": f"{reviewed_base}/",
        "canonical_api_base": reviewed_base,
    }


def _reviewed_timeout_context():
    reviewed_base = importer.REVIEWED_PRACTITIONER_ROLE_TIMEOUT_BASE
    return importer.PaginationCheckpointContext(
        canonical_api_base=reviewed_base,
        source_scope_hash="scope_reviewed_timeout",
        source_ids=("source_reviewed_timeout",),
        owner_run_id="run_retry",
        acquisition_root_run_id="run_original",
        retry_of_run_id="run_original",
        dataset_id="dataset_reviewed_timeout",
        lineage_verified=True,
    )


def _reviewed_role_bundle(reviewed_base: str) -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [
            {
                "fullUrl": f"{reviewed_base}/PractitionerRole/role-2001",
                "resource": {
                    "resourceType": "PractitionerRole",
                    "id": "role-2001",
                },
            }
        ],
    }


def test_source_resource_timeout_isolates_reviewed_pair_floor():
    reviewed_source = _reviewed_timeout_source()
    target_timeout = importer._source_resource_timeout(
        reviewed_source,
        "PractitionerRole",
        60,
    )
    other_resource_timeout = importer._source_resource_timeout(
        reviewed_source,
        "Location",
        60,
    )
    other_source_timeout = importer._source_resource_timeout(
        {"api_base": "https://synthetic.example/fhir"},
        "PractitionerRole",
        60,
    )
    caller_timeout = importer._source_resource_timeout(
        reviewed_source,
        "PractitionerRole",
        360,
    )

    assert target_timeout == importer.REVIEWED_PRACTITIONER_ROLE_TIMEOUT_MIN_SECONDS
    assert other_resource_timeout == 60
    assert other_source_timeout == 60
    assert caller_timeout == 360


class _ReviewedTimeoutResumeHarness:
    def __init__(self, monkeypatch) -> None:
        self.reviewed_base = importer.REVIEWED_PRACTITIONER_ROLE_TIMEOUT_BASE
        self.source_record = _reviewed_timeout_source()
        self.checkpoint_context = _reviewed_timeout_context()
        self.start_url = f"{self.reviewed_base}/PractitionerRole?_count=100"
        self.resume_url = (
            f"{self.reviewed_base}/PractitionerRole?"
            "_getpages=opaque-review&_getpagesoffset=2000&_count=100"
        )
        self.checkpoint_loads: list[tuple[Any, str, str]] = []
        self.fetches: list[tuple[str, int]] = []
        self.checkpoint_saves: list[tuple[Any, str, dict[str, Any]]] = []
        monkeypatch.setattr(
            importer,
            "_load_or_initialize_pagination_checkpoint",
            self.load_checkpoint,
        )
        monkeypatch.setattr(importer, "_fetch_source_json", self.fetch_source_json)
        monkeypatch.setattr(
            importer,
            "_save_pagination_checkpoint",
            self.save_checkpoint,
        )

    async def load_checkpoint(self, context, resource_type, requested_start_url):
        self.checkpoint_loads.append((context, resource_type, requested_start_url))
        return importer.PaginationResumeState(
            next_url=self.resume_url,
            pages_processed=20,
            rows_processed=2000,
            recent_url_hashes=("prior_hash",),
            resumed=True,
        )

    async def fetch_source_json(self, _source, request_url, *, timeout):
        self.fetches.append((request_url, timeout))
        return 200, _reviewed_role_bundle(self.reviewed_base), None, 5

    async def save_checkpoint(self, context, resource_type, **checkpoint):
        self.checkpoint_saves.append((context, resource_type, checkpoint))

    async def write_rows(self, _model, resource_rows):
        return len(resource_rows)

    async def fetch_resumed_page(self):
        return await importer._fetch_resource_rows(
            self.source_record,
            "PractitionerRole",
            per_resource_limit=0,
            page_limit=0,
            page_count=100,
            timeout=60,
            run_id="run_retry",
            row_batch_handler=self.write_rows,
            row_batch_size=1000,
            retain_rows=False,
            pagination_checkpoint=self.checkpoint_context,
        )

    def assert_identity_and_timeout(self, fetch_result) -> None:
        assert fetch_result is not None
        assert fetch_result.complete is True
        assert fetch_result.pages_fetched == 21
        assert fetch_result.rows_fetched == 2001
        assert self.checkpoint_loads == [
            (self.checkpoint_context, "PractitionerRole", self.start_url)
        ]
        assert self.fetches == [
            (
                self.resume_url,
                importer.REVIEWED_PRACTITIONER_ROLE_TIMEOUT_MIN_SECONDS,
            )
        ]
        context, resource_type, checkpoint = self.checkpoint_saves[0]
        assert context is self.checkpoint_context
        assert resource_type == "PractitionerRole"
        assert checkpoint["next_url"] is None
        assert checkpoint["pages_processed"] == 21
        assert checkpoint["rows_processed"] == 2001


@pytest.mark.asyncio
async def test_reviewed_pair_resume_preserves_identity_with_timeout_floor(monkeypatch):
    resume_harness = _ReviewedTimeoutResumeHarness(monkeypatch)
    fetch_result = await resume_harness.fetch_resumed_page()
    resume_harness.assert_identity_and_timeout(fetch_result)
