# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL publication proof for a retry-exhausted Flex cohort."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy.exc import DBAPIError
import pytest

import process.uhc_flex_practitioner_publication as publication
from process.uhc_flex_practitioner_acquisition_contract import (
    UHC_FLEX_PRACTITIONER_RETRY_EXHAUSTED_ERROR_CODE,
)
from process.uhc_flex_practitioner_single_root_contract import (
    single_root_dataset_intent_id,
    single_root_run_id,
)
from process.uhc_flex_practitioner_store import (
    build_uhc_flex_practitioner_acquisition_identity,
    claim_uhc_flex_practitioner_work,
    complete_uhc_flex_practitioner_error,
    complete_uhc_flex_practitioner_result,
    initialize_uhc_flex_practitioner_acquisition,
    release_uhc_flex_practitioner_work,
    seal_uhc_flex_practitioner_acquisition,
)
from process.uhc_flex_practitioner_twin_store import (
    admit_uhc_flex_practitioner_single_root,
)
from tests import provider_directory_uhc_flex_npi_cohort_pg_support as cohort_support
from tests.formulary_fhir_twin_admission_pg_support import connect, run_migration
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    cohort_fixture,
    MEMBER_NPIS,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    ENDPOINT_ID,
    PROJECTION_DATE,
    _publication_test_scope,
    _query_result,
)
from tests.test_provider_directory_uhc_flex_practitioner_twin_postgres import (
    _bound_official_content_proof,
)


async def _terminalize_retry_exhaustion(database, identity, exhausted_npi) -> None:
    for expected_attempt in range(1, 8):
        claim = await claim_uhc_flex_practitioner_work(
            identity.acquisition_id,
            requested_npi=exhausted_npi,
            database=database,
        )
        assert claim is not None and claim.attempt == expected_attempt
        await release_uhc_flex_practitioner_work(claim, database=database)
    exhausted = await claim_uhc_flex_practitioner_work(
        identity.acquisition_id,
        requested_npi=exhausted_npi,
        database=database,
    )
    assert exhausted is not None and exhausted.attempt == 8
    await complete_uhc_flex_practitioner_error(
        exhausted,
        error_code=UHC_FLEX_PRACTITIONER_RETRY_EXHAUSTED_ERROR_CODE,
        database=database,
    )


async def _partial_single_root(database):
    operation_key = "c" * 64
    cohort = cohort_fixture()
    intent_id = single_root_dataset_intent_id(
        cohort.cohort_id,
        PROJECTION_DATE,
        operation_key,
    )
    identity = build_uhc_flex_practitioner_acquisition_identity(
        cohort,
        acquisition_role="candidate",
        run_id=single_root_run_id(intent_id),
        dataset_intent_id=intent_id,
    )
    assert await initialize_uhc_flex_practitioner_acquisition(
        identity,
        database=database,
    ) == 1
    matched_npi, exhausted_npi = MEMBER_NPIS
    await _terminalize_retry_exhaustion(database, identity, exhausted_npi)
    matched = await claim_uhc_flex_practitioner_work(
        identity.acquisition_id,
        requested_npi=matched_npi,
        database=database,
    )
    assert matched is not None
    await complete_uhc_flex_practitioner_result(
        matched,
        _query_result(matched_npi, True),
        database=database,
    )
    summary = await seal_uhc_flex_practitioner_acquisition(
        identity,
        database=database,
    )
    assert (
        summary.matched_count,
        summary.unmatched_count,
        summary.error_count,
        summary.cohort_complete,
    ) == (1, 0, 1, False)
    return await admit_uhc_flex_practitioner_single_root(
        identity.acquisition_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=operation_key,
        database=database,
    )


@pytest.mark.asyncio
async def test_retry_exhausted_single_root_publishes_explicit_partial_dataset(
    monkeypatch,
) -> None:
    content_proof = _bound_official_content_proof()
    monkeypatch.setattr(cohort_support, "DATASET_HASH", content_proof["dataset_hash"])
    monkeypatch.setattr(
        cohort_support,
        "CONTENT_PROOF_SHA256",
        content_proof["proof_sha256"],
    )
    monkeypatch.setattr(cohort_support, "_content_proof", lambda: content_proof)
    async with _publication_test_scope(monkeypatch) as test_scope:
        (
            url,
            schema,
            database,
            engine,
            _publication_migration,
            retry_exhaustion_migration,
        ) = test_scope
        monkeypatch.setattr(
            publication,
            "register_uhc_flex_practitioner_source",
            AsyncMock(return_value=SimpleNamespace(endpoint_id=ENDPOINT_ID)),
        )
        admission = await _partial_single_root(database)
        publication_result = await publication.publish_uhc_flex_practitioner_dataset(
            admission.candidate_acquisition_id,
            database=database,
            batch_size=1,
        )
        assert publication_result.replayed is False
        assert publication_result.readiness.retry_exhausted_count == 1
        assert publication_result.readiness.cohort_complete is False
        assert publication_result.readiness.resource_count == 1
        connection = await connect(url)
        try:
            metadata = await connection.fetchval(
                f"SELECT publication_metadata_json FROM {schema}."
                "provider_directory_endpoint_dataset WHERE dataset_id = $1",
                publication_result.readiness.dataset_id,
            )
        finally:
            await connection.close()
        metadata = json.loads(metadata)
        assert metadata["retry_exhausted_count"] == 1
        assert metadata["cohort_complete"] is False
        with pytest.raises(DBAPIError, match="retry_exhaustion_downgrade_blocked"):
            await run_migration(engine, retry_exhaustion_migration, "downgrade")
