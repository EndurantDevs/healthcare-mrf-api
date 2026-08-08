# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contracts for the checked-in library-only formulary source."""

from __future__ import annotations

import copy
import datetime as dt
import json
from contextlib import asynccontextmanager
from unittest.mock import Mock

import pytest

import process.formulary_fhir.reviewed_source as reviewed_module
from process.formulary_fhir.reviewed_source import ReviewedSourceError
from process.formulary_fhir.reviewed_source import register_reviewed_source
from process.formulary_fhir.reviewed_source import reviewed_source_manifest
from process.formulary_fhir.reviewed_source import (
    verify_reviewed_source_candidate,
)
from process.formulary_fhir.synchronizer import SynchronizationResult


def _manifest_document() -> dict[str, object]:
    return json.loads(
        reviewed_module.DEFAULT_REVIEWED_SOURCE_MANIFEST.read_text(
            encoding="utf-8"
        )
    )


class _Database:
    def __init__(self, source_rows: list[dict[str, object]] | None = None) -> None:
        self.source_rows = list(source_rows or [])
        self.statements: list[str] = []

    @asynccontextmanager
    async def transaction(self):
        yield

    async def all(self, statement: str, **params: object):
        self.statements.append(statement)
        return [
            source_by_field
            for source_by_field in self.source_rows
            if source_by_field["source_id"] == params["source_id"]
            or source_by_field["canonical_base"] == params["canonical_base"]
        ]

    async def first(self, statement: str, **params: object):
        self.statements.append(statement)
        return next(
            (
                source_by_field
                for source_by_field in self.source_rows
                if source_by_field["source_id"] == params["source_id"]
            ),
            None,
        )

    async def status(self, statement: str, **params: object):
        self.statements.append(statement)
        if not statement.startswith("INSERT INTO"):
            return None
        self.source_rows.append(
            {
                "source_id": params["source_id"],
                "canonical_base": params["canonical_base"],
                "display_name": params["display_name"],
                "enabled": True,
                "runtime_config_json": json.loads(
                    params["runtime_config_json"]
                ),
                "metadata_json": json.loads(params["metadata_json"]),
            }
        )
        return 1


class _PostflightDatabase(_Database):
    async def first(self, statement: str, **params: object):
        self.statements.append(statement)
        if "fhir_formulary_current" in statement:
            return None
        if "fhir_formulary_dataset" in statement:
            return {
                "status": "verified",
                "publish_requested": False,
                "seed_eligible": False,
            }
        raise AssertionError(f"unexpected query: {params!r}")


def _synchronization_result() -> SynchronizationResult:
    return SynchronizationResult(
        dataset_id="ffd_" + "1" * 48,
        acquisition_contract_hash="a" * 64,
        list_count=1,
        alias_count=1,
        medication_membership_count=1,
        coverage_hash="b" * 64,
        membership_hash="c" * 64,
        full_aliases=1,
        reused_aliases=0,
        resumed_aliases=0,
        request_count=6,
        transient_retry_count=0,
        throttle_count=0,
    )


def test_reviewed_manifest_is_exact_nonpublishing_and_redacted():
    manifest_document = _manifest_document()
    source_by_field = manifest_document["source"]
    metadata_by_field = source_by_field["metadata_json"]

    manifest = reviewed_source_manifest()

    assert manifest.source_id == source_by_field["source_id"]
    assert manifest.config.canonical_base == source_by_field["canonical_base"]
    assert manifest.config.page_size == 100
    assert manifest.config.max_total_resources == 10_000_000
    assert metadata_by_field["publication_intent"] == "none"
    assert metadata_by_field["launch_mode"] == "manual-library"
    assert metadata_by_field["resource_types"] == [
        "List",
        "MedicationKnowledge",
    ]
    assert manifest.alternative_correction.prefix == (
        metadata_by_field["alternative_reference_correction"]["prefix"]
    )
    assert manifest.canonical_base not in repr(manifest)
    assert manifest.display_name not in repr(manifest)
    assert manifest.alternative_correction.prefix not in repr(manifest)


@pytest.mark.parametrize(
    "mutation_name",
    [
        "schema_bool",
        "publication",
        "cutoff",
        "resources",
        "correction",
    ],
)
def test_reviewed_manifest_rejects_contract_drift(mutation_name):
    manifest_document = copy.deepcopy(_manifest_document())
    source_by_field = manifest_document["source"]
    metadata_by_field = source_by_field["metadata_json"]
    if mutation_name == "schema_bool":
        manifest_document["schema_version"] = True
    elif mutation_name == "publication":
        metadata_by_field["publication_intent"] = "requested"
    elif mutation_name == "cutoff":
        metadata_by_field["cutoff"] = "2026-08-08T00:00:00Z"
    elif mutation_name == "resources":
        metadata_by_field["resource_types"] = ["MedicationKnowledge"]
    else:
        metadata_by_field["alternative_reference_correction"]["prefix"] = (
            "bad prefix"
        )

    with pytest.raises(ReviewedSourceError) as caught:
        reviewed_module._validated_manifest_document(manifest_document)

    assert caught.value.code == "manifest"


@pytest.mark.asyncio
async def test_registration_is_idempotent_and_never_rewrites_source():
    database = _Database()

    first_binding = await register_reviewed_source(database=database)
    second_binding = await register_reviewed_source(database=database)

    assert len(database.source_rows) == 1
    assert first_binding == second_binding
    assert first_binding.alternative_correction is not None
    assert sum(
        statement.startswith("INSERT INTO") for statement in database.statements
    ) == 1
    assert all(
        not statement.lstrip().startswith("UPDATE ")
        for statement in database.statements
    )


@pytest.mark.asyncio
async def test_registration_rejects_identity_collision_without_mutation():
    manifest = reviewed_source_manifest()
    conflicting_source_by_field = reviewed_module._source_values(manifest)
    conflicting_source_by_field["display_name"] = "Conflicting source"
    database = _Database([conflicting_source_by_field])

    with pytest.raises(ReviewedSourceError) as caught:
        await register_reviewed_source(database=database)

    assert caught.value.code == "catalog"
    assert database.source_rows == [conflicting_source_by_field]
    assert all(
        not statement.startswith("INSERT INTO")
        for statement in database.statements
    )


@pytest.mark.asyncio
async def test_candidate_rejects_future_cutoff_before_lock(monkeypatch):
    lock_constructor = Mock(side_effect=AssertionError("lock must not open"))

    monkeypatch.setattr(
        reviewed_module.manual_lock,
        "manual_source_lease",
        lock_constructor,
    )

    with pytest.raises(ReviewedSourceError) as caught:
        await verify_reviewed_source_candidate(
            run_id="reviewed-candidate-run",
            cutoff=dt.datetime.now(dt.UTC) + dt.timedelta(days=1),
            database=object(),
        )

    assert caught.value.code == "invalid_request"
    lock_constructor.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift_mode", ["deleted", "changed"])
async def test_candidate_postflight_rejects_source_drift_without_repair(
    monkeypatch,
    drift_mode,
):
    manifest = reviewed_source_manifest()
    database = _PostflightDatabase(
        [reviewed_module._source_values(manifest)]
    )

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def verify_candidate(*_args, **_kwargs):
        if drift_mode == "deleted":
            database.source_rows.clear()
        else:
            database.source_rows[0]["display_name"] = "Changed source"
        return _synchronization_result()

    monkeypatch.setattr(
        reviewed_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        reviewed_module,
        "_verify_registered_candidate",
        verify_candidate,
    )

    with pytest.raises(ReviewedSourceError) as caught:
        await verify_reviewed_source_candidate(
            run_id="reviewed-candidate-run",
            cutoff=dt.datetime(2026, 8, 8, tzinfo=dt.UTC),
            database=database,
        )

    assert caught.value.code == "catalog"
    assert all(
        not statement.lstrip().startswith(("INSERT ", "UPDATE "))
        for statement in database.statements
    )
