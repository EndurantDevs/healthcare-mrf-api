# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Runtime boundaries for terminal checkpoint and verification scopes."""

from __future__ import annotations

import pytest

from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    EXPECTED_RESOURCE_TYPES,
    ReviewedSubsetTerminalDispositionError,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    selected_reviewed_subset_terminal_disposition,
)
from tests.provider_directory_fhir_subset_terminal_disposition_support import (
    CHECKPOINT_SCOPE_SHA256,
    TerminalDispositionDatabase,
    VERIFICATION_SCOPE_SHA256,
)


_MISSING = object()
_SERIAL_CONCURRENCY_FIELDS = (
    "resource_scan_concurrency_requested",
    "resource_scan_concurrency_effective",
)


def _mutate_all_diagnostic_copies(database, mutator):
    candidate_metadata = database.candidate_row["publication_metadata_json"]
    diagnostic_copies = (
        candidate_metadata["resource_diagnostics"],
        candidate_metadata["completion_proof_v1"]["resource_diagnostics"],
        database.source_row["metadata_json"]["last_resource_import"][
            "resources"
        ],
    )
    for diagnostics_by_type in diagnostic_copies:
        mutator(diagnostics_by_type)


def _mutate_serial_concurrency(database, field_name, value):
    def mutate_diagnostics(diagnostics_by_type):
        diagnostic = diagnostics_by_type[EXPECTED_RESOURCE_TYPES[0]]
        if value is _MISSING:
            diagnostic.pop(field_name)
        else:
            diagnostic[field_name] = value

    _mutate_all_diagnostic_copies(database, mutate_diagnostics)


@pytest.mark.asyncio
async def test_selector_binds_distinct_checkpoint_and_verification_scopes():
    selection, _checkpoint_rows = (
        await selected_reviewed_subset_terminal_disposition(
            TerminalDispositionDatabase(),
            "source-a",
        )
    )

    assert selection.source_scope_sha256 == CHECKPOINT_SCOPE_SHA256
    assert (
        selection.observed_candidate_metadata[
            "verification_source_scope_hash"
        ]
        == VERIFICATION_SCOPE_SHA256
    )
    assert selection.source_scope_sha256 != VERIFICATION_SCOPE_SHA256


@pytest.mark.parametrize("field_name", _SERIAL_CONCURRENCY_FIELDS)
@pytest.mark.parametrize("value", (_MISSING, "1", 1.0, 2))
@pytest.mark.asyncio
async def test_selector_requires_exact_serial_concurrency(field_name, value):
    database = TerminalDispositionDatabase()
    _mutate_serial_concurrency(database, field_name, value)

    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await selected_reviewed_subset_terminal_disposition(
            database,
            "source-a",
        )
    assert error.value.code == "evidence"


@pytest.mark.parametrize("mutation", ("verification", "checkpoint"))
@pytest.mark.asyncio
async def test_selector_keeps_scope_domains_distinct(mutation):
    database = TerminalDispositionDatabase()
    candidate_metadata = database.candidate_row["publication_metadata_json"]
    if mutation == "verification":
        candidate_metadata["verification_source_scope_hash"] = "7" * 64
        candidate_metadata["completion_proof_v1"][
            "verification_source_scope_hash"
        ] = "7" * 64
    else:
        for checkpoint in database.checkpoint_rows:
            checkpoint["source_scope_hash"] = VERIFICATION_SCOPE_SHA256

    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await selected_reviewed_subset_terminal_disposition(
            database,
            "source-a",
        )
    assert error.value.code == "evidence"
