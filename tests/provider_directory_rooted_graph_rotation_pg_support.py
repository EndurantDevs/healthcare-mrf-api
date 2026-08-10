# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Official-rotation assertions for rooted publication PostgreSQL proofs."""

from __future__ import annotations

import json
from typing import Any, Awaitable, Callable

import pytest

from db.connection import Database
from process.provider_directory_dataset_scoped_publication import (
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
    ProviderDirectoryDatasetScopedPublicationError,
)
from process.provider_directory_profile_selection_dataset import (
    _dataset_selection_by_group,
)
from process.provider_directory_profile_uhc_flex_store import (
    load_profile_selection_dataset_rows,
)
from process.uhc_flex_official_cohort_contract import (
    build_uhc_flex_official_cohort,
)
from process.uhc_flex_practitioner_publication_store import (
    publish_registered_uhc_flex_dataset,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    DATASET_ID as OFFICIAL_DATASET_ID,
    ENDPOINT_ID as OFFICIAL_ENDPOINT_ID,
    MEMBER_NPIS,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    _sealed_pair as _sealed_legacy_pair,
)


_ROTATED_OFFICIAL_DATASET_ID = "dataset-2026-rotated"
_ROTATED_OFFICIAL_ROOT_RUN_ID = "s" * 64
_ROTATED_OFFICIAL_DATASET_HASH = "e" * 64
_ROTATED_OFFICIAL_CONTENT_PROOF = "f" * 64
_ROOTED_FUNCTION_NAMES = (
    "provider_directory_rooted_graph_dataset_intrinsic_valid",
    "provider_directory_rooted_graph_official_lineage_current",
    "provider_directory_rooted_graph_dataset_valid",
    "provider_directory_rooted_graph_dataset_ready",
)
_PublishGeneration = Callable[[Any, Any, str, str], Awaitable[tuple[Any, ...]]]


async def publish_legacy_root(
    database: Database,
    operation_key: str = "1" * 64,
    *,
    cohort=None,
):
    """Publish and return one exact legacy current root."""

    admission = await _sealed_legacy_pair(
        database,
        operation_key=operation_key,
        matched=True,
        cohort=cohort,
    )
    published = await publish_registered_uhc_flex_dataset(
        admission.candidate_acquisition_id,
        uhc_flex_practitioner_endpoint_identity().endpoint_id,
        1,
        database=database,
    )
    assert published.replayed is False
    current = await locked_exact_current(database)
    assert current is not None
    assert current.dataset_id == published.readiness.dataset_id
    return current


async def locked_exact_current(database: Database):
    """Load one ready exact logical current under its publication lock."""

    async with database.transaction():
        return await lock_exact_current_dataset(database, pair=exact_uhc_dataset_pair())


def _rotated_official_cohort():
    return build_uhc_flex_official_cohort(
        official_endpoint_id=OFFICIAL_ENDPOINT_ID,
        official_dataset_id=_ROTATED_OFFICIAL_DATASET_ID,
        official_acquisition_root_run_id=_ROTATED_OFFICIAL_ROOT_RUN_ID,
        official_dataset_hash=_ROTATED_OFFICIAL_DATASET_HASH,
        official_content_proof_sha256=_ROTATED_OFFICIAL_CONTENT_PROOF,
        practitioner_resource_count=3,
        npi_count=len(MEMBER_NPIS),
    )


def _rotated_official_metadata_json() -> str:
    proof_by_field = {
        "contract_id": "healthporta.uhc.canonical-content-proof.v1",
        "complete": True,
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "dataset_id": _ROTATED_OFFICIAL_DATASET_ID,
        "endpoint_id": OFFICIAL_ENDPOINT_ID,
        "acquisition_root_run_id": _ROTATED_OFFICIAL_ROOT_RUN_ID,
        "dataset_hash": _ROTATED_OFFICIAL_DATASET_HASH,
        "resource_count": 4,
        "resource_counts": {"Practitioner": 3, "Organization": 1},
        "proof_sha256": _ROTATED_OFFICIAL_CONTENT_PROOF,
    }
    return json.dumps({"uhc_canonical_content_proof_v1": proof_by_field})


async def _replace_official_parent(context) -> None:
    await context.connection.execute(
        f"UPDATE {context.schema}.provider_directory_endpoint_dataset "
        "SET status = 'superseded', is_current = false, "
        "superseded_at = transaction_timestamp() WHERE dataset_id = $1",
        OFFICIAL_DATASET_ID,
    )
    await context.connection.execute(
        f"INSERT INTO {context.schema}.provider_directory_endpoint_dataset "
        "(dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash, "
        "status, is_current, resource_count, publication_metadata_json) "
        "VALUES ($1, $2, $3, $4, 'published', true, 4, $5::jsonb)",
        _ROTATED_OFFICIAL_DATASET_ID,
        OFFICIAL_ENDPOINT_ID,
        _ROTATED_OFFICIAL_ROOT_RUN_ID,
        _ROTATED_OFFICIAL_DATASET_HASH,
        _rotated_official_metadata_json(),
    )
    await context.connection.execute(
        f"INSERT INTO {context.schema}.provider_directory_dataset_resource "
        "(dataset_id, resource_type, resource_id, payload_hash, payload_json, "
        "acquired_resource_sha256) SELECT $1, resource_type, resource_id, "
        "payload_hash, payload_json, acquired_resource_sha256 FROM "
        f"{context.schema}.provider_directory_dataset_resource "
        "WHERE dataset_id = $2",
        _ROTATED_OFFICIAL_DATASET_ID,
        OFFICIAL_DATASET_ID,
    )


async def _insert_rotated_cohort(context, cohort) -> None:
    await context.connection.executemany(
        f"INSERT INTO {context.schema}.provider_directory_uhc_flex_npi_member "
        "(cohort_id, npi) VALUES ($1, $2)",
        tuple((cohort.cohort_id, npi) for npi in MEMBER_NPIS),
    )
    await context.connection.execute(
        f"INSERT INTO {context.schema}.provider_directory_uhc_flex_npi_cohort "
        "(cohort_id, contract_id, authority_id, official_source_id, "
        "official_endpoint_id, official_dataset_id, "
        "official_acquisition_root_run_id, official_dataset_hash, "
        "official_content_proof_sha256, resource_type, "
        "practitioner_resource_count, npi_count, cohort_complete, "
        "endpoint_collection_complete, endpoint_complete) VALUES "
        "($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, "
        "$13, $14, $15)",
        cohort.cohort_id,
        cohort.contract_id,
        cohort.authority_id,
        cohort.official_source_id,
        cohort.official_endpoint_id,
        cohort.official_dataset_id,
        cohort.official_acquisition_root_run_id,
        cohort.official_dataset_hash,
        cohort.official_content_proof_sha256,
        cohort.resource_type,
        cohort.practitioner_resource_count,
        cohort.npi_count,
        cohort.cohort_complete,
        cohort.endpoint_collection_complete,
        cohort.endpoint_complete,
    )


async def rotate_official_dataset(context):
    """Publish a second official dataset and return its exact cohort."""

    cohort = _rotated_official_cohort()
    async with context.connection.transaction():
        await _replace_official_parent(context)
        await _insert_rotated_cohort(context, cohort)
    return cohort


async def profile_exact_selection(context):
    """Return the visible exact row and selected ready source group."""

    pair = exact_uhc_dataset_pair()
    rows = await load_profile_selection_dataset_rows(
        database=context.database,
        endpoint_dataset_ref=(
            f'{context.schema}."provider_directory_endpoint_dataset"'
        ),
        schema_ref=context.schema,
        row_mapping=lambda row: dict(row._mapping),
    )
    selected = _dataset_selection_by_group(
        rows,
        ((pair.legacy_source_id,), (pair.rooted_source_id,)),
        {
            pair.legacy_endpoint_id: {pair.legacy_source_id},
            pair.rooted_endpoint_id: {pair.rooted_source_id},
        },
        ((pair.legacy_source_id, pair.rooted_source_id),),
    )
    exact_rows = tuple(
        row
        for row in rows
        if row.get("endpoint_id") in {pair.legacy_endpoint_id, pair.rooted_endpoint_id}
    )
    assert len(exact_rows) == 1
    return exact_rows[0], selected


async def is_rooted_predicate_true(
    context,
    function_name: str,
    dataset_id: str,
) -> bool:
    """Evaluate one rooted validity/readiness predicate."""

    return await context.connection.fetchval(
        f"SELECT {context.schema}.{function_name}($1)",
        dataset_id,
    )


async def prove_stale_legacy_replacement(context) -> None:
    """Prove official rotation permits an exact successor legacy publish."""

    stale_legacy = await publish_legacy_root(context.database)
    rotated_cohort = await rotate_official_dataset(context)
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as stale:
        await locked_exact_current(context.database)
    assert stale.value.code == "foreign_current"
    stale_row, stale_selection = await profile_exact_selection(context)
    assert stale_row["dataset_id"] == stale_legacy.dataset_id
    assert stale_row["dataset_scoped_ready"] is False
    assert stale_selection == {}
    replacement = await publish_legacy_root(
        context.database,
        "2" * 64,
        cohort=rotated_cohort,
    )
    assert replacement.dataset_id != stale_legacy.dataset_id
    assert replacement.variant == "uhc_flex_practitioner"
    ready_row, ready_selection = await profile_exact_selection(context)
    assert ready_row["dataset_id"] == replacement.dataset_id
    assert ready_row["dataset_scoped_ready"] is True
    assert ready_selection == {(exact_uhc_dataset_pair().legacy_source_id,): ready_row}


async def _assert_rooted_functions_current(context, dataset_id: str) -> None:
    for function_name in _ROOTED_FUNCTION_NAMES:
        assert (
            await context.connection.fetchval(
                "SELECT pg_catalog.to_regprocedure($1)::text",
                f"{context.schema_name}.{function_name}(text)",
            )
            is not None
        )
        assert await is_rooted_predicate_true(context, function_name, dataset_id)


async def _assert_rooted_functions_stale(context, dataset_id: str) -> None:
    assert await is_rooted_predicate_true(
        context,
        _ROOTED_FUNCTION_NAMES[0],
        dataset_id,
    )
    for function_name in _ROOTED_FUNCTION_NAMES[1:]:
        assert not await is_rooted_predicate_true(context, function_name, dataset_id)


async def _assert_stale_root_profile(context, stale_rooted) -> None:
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as stale:
        await locked_exact_current(context.database)
    assert stale.value.code == "foreign_current"
    stale_row, stale_selection = await profile_exact_selection(context)
    assert stale_row["dataset_id"] == stale_rooted.dataset_id
    assert stale_row["dataset_scoped_ready"] is False
    assert stale_selection == {}


async def _publish_recovered_generations(
    context,
    replacement_legacy,
    publish_generation: _PublishGeneration,
):
    _, _, _, recovered_first = await publish_generation(
        context,
        replacement_legacy,
        "89",
        "a",
    )
    recovered_current = await locked_exact_current(context.database)
    assert recovered_current is not None
    _, _, _, recovered_second = await publish_generation(
        context,
        recovered_current,
        "bc",
        "d",
    )
    return recovered_first, recovered_second


async def _assert_recovered_profile(
    context,
    recovered_first,
    recovered_second,
) -> None:
    current = await locked_exact_current(context.database)
    assert current is not None
    assert current.dataset_id == recovered_second.readiness.dataset_id
    for dataset_id in (
        recovered_first.readiness.dataset_id,
        recovered_second.readiness.dataset_id,
    ):
        assert await is_rooted_predicate_true(
            context,
            "provider_directory_rooted_graph_dataset_valid",
            dataset_id,
        )
    final_row, final_selection = await profile_exact_selection(context)
    assert final_row["dataset_id"] == current.dataset_id
    assert final_row["dataset_scoped_ready"] is True
    assert final_selection == {(exact_uhc_dataset_pair().rooted_source_id,): final_row}


async def prove_recursive_rooted_rotation(
    context,
    publish_generation: _PublishGeneration,
) -> None:
    """Prove recursive rooted generations revoke and recover on rotation."""

    legacy = await publish_legacy_root(context.database)
    _, _, _, first = await publish_generation(context, legacy, "12", "3")
    first_current = await locked_exact_current(context.database)
    assert first_current is not None
    _, _, _, second = await publish_generation(context, first_current, "45", "6")
    stale_rooted = await locked_exact_current(context.database)
    assert stale_rooted is not None
    assert stale_rooted.dataset_id == second.readiness.dataset_id
    await _assert_rooted_functions_current(context, stale_rooted.dataset_id)
    rotated_cohort = await rotate_official_dataset(context)
    await _assert_rooted_functions_stale(context, stale_rooted.dataset_id)
    await _assert_stale_root_profile(context, stale_rooted)
    replacement_legacy = await publish_legacy_root(
        context.database,
        "7" * 64,
        cohort=rotated_cohort,
    )
    legacy_row, legacy_selection = await profile_exact_selection(context)
    assert legacy_row["dataset_id"] == replacement_legacy.dataset_id
    assert legacy_selection == {
        (exact_uhc_dataset_pair().legacy_source_id,): legacy_row
    }
    recovered = await _publish_recovered_generations(
        context,
        replacement_legacy,
        publish_generation,
    )
    await _assert_recovered_profile(context, *recovered)
