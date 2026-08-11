# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Direct-v5 HTTP-410 coverage for the shared disposition transaction."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from process import provider_directory_fhir_subset_terminal_disposition as facade
from process import (
    provider_directory_fhir_subset_terminal_disposition_v5_selection
    as v5_selection,
)
from process.provider_directory_fhir_subset_terminal_disposition import (
    dispose_v5_terminal_root,
    require_v5_disposition_gate,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_METADATA_KEY,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
    canonical_evidence_sha256,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V5_CONTRACT_VERSION,
    DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE,
    DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
    DIRECT_V5_TERMINAL_MARKER_SHA256,
    TERMINAL_HTTP_410_DISPOSITION,
    VERIFIED_COMPLETE_DISPOSITION,
)
from process.provider_directory_fhir_subset_terminal_disposition_source import (
    expected_terminal_start_hashes,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_v5_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_v5_contract import (
    direct_v5_terminal_marker,
    validated_direct_v5_terminal_marker,
)
from process.provider_directory_fhir_subset_terminal_disposition_v5_evidence import (
    validated_v5_resource_dispositions,
)
from process.provider_directory_fhir_subset_terminal_disposition_v5_selection import (
    selected_direct_v5_terminal_disposition,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v4_support import (
    CHECKPOINT_SCOPE_SHA256,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v5_support import (
    DirectV5TerminalDatabase,
    direct_v5_inputs,
)


_SYNTHETIC_MARKER_SHA256 = (
    "f419b51652df88f110e7f1f8ea61298c7342d19427e9f6209d839ce54434eb83"
)
_FROZEN_MARKER_PATH = Path(__file__).with_name("fixtures") / (
    "provider_directory_v5_http410_terminal_marker.json"
)


def _synthetic_marker() -> dict:
    source_by_field, candidate_by_field, checkpoint_rows = direct_v5_inputs()
    candidate_metadata = candidate_by_field["publication_metadata_json"]
    diagnostics_by_type = candidate_metadata["resource_diagnostics"]
    source_import = source_by_field["metadata_json"]["last_resource_import"]
    expected_start_hash_by_type = expected_terminal_start_hashes(
        source_by_field,
        candidate_metadata,
        diagnostics_by_type,
    )
    resources_by_type = validated_v5_resource_dispositions(
        diagnostics_by_type,
        checkpoint_rows,
        candidate_metadata,
        expected_start_hash_by_type=expected_start_hash_by_type,
    )
    return direct_v5_terminal_marker(
        source_scope_sha256=CHECKPOINT_SCOPE_SHA256,
        resource_dispositions=resources_by_type,
        proof_shard_count=len(checkpoint_rows),
        source_diagnostics=diagnostics_by_type,
        source_import=source_import,
        candidate_metadata=candidate_metadata,
        direct_lineage={
            "checkpoint_retry_count": 0,
            "competing_candidate_count": 0,
            "current_dataset_count": 0,
            "import_run_row_count": 0,
            "owner_equals_root": True,
            "previous_dataset_present": False,
            "previous_reference_count": 0,
        },
    )


def _frozen_live_marker() -> dict:
    """Load the independently reconstructed identifier-free live marker."""

    return json.loads(
        _FROZEN_MARKER_PATH.read_text(encoding="utf-8")
    )


@pytest.fixture(autouse=True)
def _bind_synthetic_marker(monkeypatch):
    """Bind neutral fixtures while production retains the live digest."""

    monkeypatch.setattr(
        v5_selection,
        "DIRECT_V5_TERMINAL_MARKER_SHA256",
        _SYNTHETIC_MARKER_SHA256,
    )


def test_v5_marker_hashes_are_frozen():
    assert canonical_evidence_sha256(_synthetic_marker()) == (
        _SYNTHETIC_MARKER_SHA256
    )
    assert DIRECT_V5_TERMINAL_MARKER_SHA256 == (
        "87f1c25625562037f9544b30a62e8b1bbf625018c73076bb083b8680225b23d9"
    )
    frozen_live_marker = validated_direct_v5_terminal_marker(
        _frozen_live_marker()
    )
    assert canonical_evidence_sha256(frozen_live_marker) == (
        DIRECT_V5_TERMINAL_MARKER_SHA256
    )


def test_v5_profile_is_exactly_six_complete_and_one_http410():
    assert DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE["HealthcareService"] == (
        TERMINAL_HTTP_410_DISPOSITION
    )
    assert set(DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE.values()) == {
        TERMINAL_HTTP_410_DISPOSITION,
        VERIFIED_COMPLETE_DISPOSITION,
    }
    assert list(DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE.values()).count(
        VERIFIED_COMPLETE_DISPOSITION
    ) == 6


def test_v5_gate_is_exact_and_default_off(monkeypatch):
    for value in (None, "", "1", "TRUE"):
        if value is None:
            monkeypatch.delenv(
                DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
                raising=False,
            )
        else:
            monkeypatch.setenv(
                DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
                value,
            )
        with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
            require_v5_disposition_gate()
        assert error.value.code == "disabled"
    monkeypatch.setenv(
        DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
        "true",
    )
    require_v5_disposition_gate()


@pytest.mark.asyncio
async def test_selector_builds_exact_v5_marker():
    database = DirectV5TerminalDatabase()

    selection, checkpoint_rows = await selected_direct_v5_terminal_disposition(
        database,
        "source-a",
    )

    marker_by_field = validated_direct_v5_terminal_marker(
        selection.marker_by_field
    )
    assert marker_by_field["contract_version"] == DIRECT_V5_CONTRACT_VERSION
    assert marker_by_field["resource_dispositions"]["HealthcareService"][
        "advertised_post"
    ] is None
    assert marker_by_field["terminal_page_delta"] == 0
    assert len(checkpoint_rows) == 7


@pytest.mark.asyncio
async def test_store_seals_once_and_replays(monkeypatch):
    from process import provider_directory_fhir_manual_catalog as catalog

    database = DirectV5TerminalDatabase()

    first = await sync_v5_terminal_disposition(database, "source-a")
    database.checkpoint_rows = tuple(
        {
            **checkpoint,
            "state": "acquisition_abandoned",
            "completed_at": checkpoint.get("updated_at"),
        }
        for checkpoint in database.checkpoint_rows
    )
    second = await sync_v5_terminal_disposition(database, "source-a")

    assert first.disposed is True
    assert second.is_already_applied is True
    assert TERMINAL_DISPOSITION_METADATA_KEY in (
        database.candidate_row["publication_metadata_json"]
    )
    monkeypatch.setenv(
        DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
        "true",
    )
    monkeypatch.setattr(catalog, "reviewed_manual_census_source_id", lambda: "source-a")
    result = await dispose_v5_terminal_root(database=database)
    assert result.is_already_applied is True


@pytest.mark.parametrize(
    "field_name,field_value",
    (
        ("reason_code", "unexpected"),
        ("checkpoint_count", 6),
        ("terminal_page_delta", 1),
        ("proof_shard_count", 0),
    ),
)
def test_v5_marker_rejects_top_level_tamper(field_name, field_value):
    marker_by_field = deepcopy(_synthetic_marker())
    marker_by_field[field_name] = field_value

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        validated_direct_v5_terminal_marker(marker_by_field)


@pytest.mark.asyncio
async def test_duplicate_v5_candidate_fails_closed():
    database = DirectV5TerminalDatabase()
    duplicate = deepcopy(database.candidate_row)
    duplicate["dataset_id"] = "duplicate-dataset"
    database.candidate_rows.append(duplicate)

    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await selected_direct_v5_terminal_disposition(database, "source-a")

    assert error.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation_name",
    ("row_count", "proof_shard", "marker_hash", "candidate_shape"),
)
async def test_v5_selection_rejects_independent_evidence_drift(
    monkeypatch,
    mutation_name,
):
    database = DirectV5TerminalDatabase()
    if mutation_name == "row_count":
        database.candidate_row["resource_count"] += 1
    elif mutation_name == "proof_shard":
        database.invalid_proof_shard_count = 1
    elif mutation_name == "marker_hash":
        monkeypatch.setattr(
            v5_selection,
            "DIRECT_V5_TERMINAL_MARKER_SHA256",
            "f" * 64,
        )
    else:
        database.candidate_row["dataset_hash"] = "f" * 64

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v5_terminal_disposition(database, "source-a")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation_name", ("error", "continuation"))
async def test_v5_http410_evidence_is_exact(mutation_name):
    database = DirectV5TerminalDatabase()
    if mutation_name == "continuation":
        checkpoint = next(
            row
            for row in database.checkpoint_rows
            if row["resource_type"] == "HealthcareService"
        )
        checkpoint["next_url"] = None
    else:
        candidate_metadata = database.candidate_row["publication_metadata_json"]
        diagnostic_copies = (
            candidate_metadata["resource_diagnostics"],
            candidate_metadata["completion_proof_v1"]["resource_diagnostics"],
            database.source_row["metadata_json"]["last_resource_import"][
                "resources"
            ],
        )
        for diagnostics_by_type in diagnostic_copies:
            diagnostics_by_type["HealthcareService"]["error"] = (
                "provider_directory_current_version_census_completeness_blocked:"
                "http_404"
            )

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v5_terminal_disposition(database, "source-a")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation_name", ("marker_hash", "candidate_metadata"))
async def test_v5_replay_rejects_marker_or_candidate_tamper(
    monkeypatch,
    mutation_name,
):
    database = DirectV5TerminalDatabase()
    await sync_v5_terminal_disposition(database, "source-a")
    if mutation_name == "marker_hash":
        monkeypatch.setattr(
            v5_selection,
            "DIRECT_V5_TERMINAL_MARKER_SHA256",
            "f" * 64,
        )
    else:
        database.candidate_row["publication_metadata_json"]["error"] = "changed"

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v5_terminal_disposition(database, "source-a")


@pytest.mark.asyncio
async def test_v5_facade_uses_catalog_and_runtime_database(monkeypatch):
    from db import connection as connection_module
    from process import provider_directory_fhir_manual_catalog as catalog

    selected_database = object()
    expected_result = ReviewedSubsetTerminalDispositionResult(disposed=True)
    monkeypatch.setenv(
        DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
        "true",
    )
    monkeypatch.setattr(connection_module, "db", selected_database)
    monkeypatch.setattr(catalog, "reviewed_manual_census_source_id", lambda: "source-a")

    async def sync(database, source_id):
        assert database is selected_database
        assert source_id == "source-a"
        return expected_result

    monkeypatch.setattr(facade, "sync_v5_terminal_disposition", sync)
    assert await dispose_v5_terminal_root() == expected_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised_error", "expected_error"),
    (
        (TimeoutError(), TimeoutError),
        (
            ReviewedSubsetTerminalDispositionError("evidence"),
            ReviewedSubsetTerminalDispositionError,
        ),
        (RuntimeError("private"), ReviewedSubsetTerminalDispositionError),
    ),
)
async def test_v5_facade_preserves_closed_error_boundary(
    monkeypatch,
    raised_error,
    expected_error,
):
    from process import provider_directory_fhir_manual_catalog as catalog

    monkeypatch.setenv(
        DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
        "true",
    )
    monkeypatch.setattr(catalog, "reviewed_manual_census_source_id", lambda: "source-a")

    async def fail(_database, _source_id):
        raise raised_error

    monkeypatch.setattr(facade, "sync_v5_terminal_disposition", fail)
    with pytest.raises(expected_error):
        await dispose_v5_terminal_root(database=object())
