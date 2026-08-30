# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable-PostgreSQL proof for partial Flex-to-rooted lineage."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.uhc_flex_practitioner_publication as flex_publication
from process.provider_directory_dataset_scoped_publication import (
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
)
from process.provider_directory_rooted_graph_publication import (
    publish_provider_directory_rooted_graph_dataset,
)
from process.provider_directory_rooted_graph_single_root_contract import (
    derive_single_root_identity,
)
from process.provider_directory_rooted_graph_twin_store import (
    admit_rooted_graph_single_root,
)
from tests.formulary_fhir_twin_admission_pg_support import (
    connect,
    load_migration,
    run_migration,
)
from tests import provider_directory_uhc_flex_npi_cohort_pg_support as cohort_support
from tests.test_provider_directory_rooted_graph_acquisition_postgres import (
    _complete_success,
)
from tests.test_provider_directory_uhc_flex_partial_publication_postgres import (
    _partial_single_root,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    _publication_test_scope,
    ENDPOINT_ID,
)
from tests.test_provider_directory_uhc_flex_practitioner_twin_postgres import (
    _bound_official_content_proof,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / ("20260830100000_provider_directory_rooted_partial_lineage.py")
)
CANONICAL_JSON_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / ("20260810110000_ptg_wave_receipt_authority.py")
)


async def _publish_and_verify_partial_rooted_graph(
    url,
    schema,
    database,
    current,
) -> None:
    """Publish one partial rooted graph and verify its stored readiness."""

    operation_key = "d" * 64
    identity = derive_single_root_identity(current, operation_key=operation_key)
    await _complete_success(database, identity.candidate)
    admission = await admit_rooted_graph_single_root(
        identity.candidate.acquisition_id,
        acquisition_operation_key=operation_key,
        database=database,
    )
    rooted = await publish_provider_directory_rooted_graph_dataset(
        admission.publication_acquisition_id,
        database=database,
        batch_size=4,
    )
    assert rooted.readiness.cohort_complete is False
    assert rooted.readiness.retry_exhausted_count == 1
    assert rooted.readiness.rooted_graph_complete is True
    assert rooted.readiness.endpoint_collection_complete is False
    assert rooted.readiness.endpoint_complete is False
    assert (
        await database.scalar(
            f"SELECT {schema}.provider_directory_rooted_graph_dataset_ready("
            ":dataset_id)",
            dataset_id=rooted.readiness.dataset_id,
        )
        is True
    )

    connection = await connect(url)
    try:
        metadata_json = await connection.fetchval(
            f"SELECT publication_metadata_json FROM {schema}."
            "provider_directory_endpoint_dataset WHERE dataset_id = $1",
            rooted.readiness.dataset_id,
        )
    finally:
        await connection.close()
    metadata_by_field = json.loads(metadata_json)
    assert metadata_by_field["cohort_complete"] is False
    assert metadata_by_field["retry_exhausted_count"] == 1


@pytest.mark.asyncio
async def test_partial_flex_root_admits_and_publishes_exact_rooted_readiness(
    monkeypatch,
) -> None:
    """Carry one exhausted Flex member through exact rooted publication."""

    content_proof = _bound_official_content_proof()
    monkeypatch.setattr(cohort_support, "DATASET_HASH", content_proof["dataset_hash"])
    monkeypatch.setattr(
        cohort_support,
        "CONTENT_PROOF_SHA256",
        content_proof["proof_sha256"],
    )
    monkeypatch.setattr(cohort_support, "_content_proof", lambda: content_proof)
    async with _publication_test_scope(monkeypatch) as test_scope:
        url, schema, database, engine, _, _ = test_scope
        monkeypatch.setattr(
            flex_publication,
            "register_uhc_flex_practitioner_source",
            AsyncMock(return_value=SimpleNamespace(endpoint_id=ENDPOINT_ID)),
        )
        flex_admission = await _partial_single_root(database)
        flex_result = await flex_publication.publish_uhc_flex_practitioner_dataset(
            flex_admission.candidate_acquisition_id,
            database=database,
            batch_size=1,
        )
        migration = load_migration(MIGRATION_PATH, "rooted_partial_lineage")
        await run_migration(engine, migration, "upgrade")
        canonical = load_migration(CANONICAL_JSON_PATH, "rooted_partial_json")
        canonical.install = lambda: canonical._install_receipt_verification_functions(
            schema.strip('"')
        )
        await run_migration(engine, canonical, "install")

        async with database.transaction():
            current = await lock_exact_current_dataset(
                database,
                pair=exact_uhc_dataset_pair(),
            )
        assert current is not None
        assert current.dataset_id == flex_result.readiness.dataset_id
        assert current.cohort_complete is False
        assert current.retry_exhausted_count == 1

        await _publish_and_verify_partial_rooted_graph(
            url,
            schema,
            database,
            current,
        )
