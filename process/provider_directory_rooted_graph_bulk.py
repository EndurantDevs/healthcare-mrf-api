# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native landing for one rooted-graph query_result transaction."""

from __future__ import annotations

from itertools import islice
from typing import Any

from sqlalchemy.exc import DBAPIError

from process.provider_directory_dataset_candidate import _bounded_copy_batches
from process.provider_directory_projection_stage import _copy_driver
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
)
from process.provider_directory_rooted_graph_store_support import function_ref, row_fields

_RESOURCE_COLUMNS = (
    "acquisition_id",
    "scope_id",
    "query_id",
    "attempt",
    "resource_type",
    "resource_id",
    "payload_sha256",
    "payload_json_text",
    "closure_scope",
)
_EDGE_COLUMNS = (
    "acquisition_id",
    "scope_id",
    "query_id",
    "attempt",
    "source_resource_type",
    "source_resource_id",
    "field_path",
    "target_resource_type",
    "target_resource_id",
    "edge_sha256",
    "closure_scope",
)
_COPY_ROWS = 1024


async def _copy_stage(driver, name, columns, records):
    iterator = iter(records)
    while batch_records := tuple(islice(iterator, _COPY_ROWS)):
        for bounded_records in _bounded_copy_batches(batch_records):
            status = await driver.copy_records_to_table(
                name,
                schema_name="pg_temp",
                columns=columns,
                records=bounded_records,
            )
            if status != f"COPY {len(bounded_records)}":
                raise ProviderDirectoryRootedGraphStoreError("state")


async def admit_result_witnesses(database: Any, transaction: Any, claim: Any, query_result: Any) -> None:
    """Land an isolated pair, then admit the complete pair with one authority check."""

    driver = await _copy_driver(transaction)
    await database.scalar(f"SELECT {function_ref('prepare_provider_directory_rooted_graph_witness_stage')}();")
    resource_stage, edge_stage = "pdrg_resource_stage", "pdrg_edge_stage"
    prefix = (claim.acquisition_id, claim.scope_id, claim.query_id, claim.attempt)
    await _copy_stage(
        driver,
        resource_stage,
        _RESOURCE_COLUMNS,
        (
            prefix + tuple(getattr(witness, field) for field in _RESOURCE_COLUMNS[4:])
            for witness in query_result.resources
        ),
    )
    await _copy_stage(
        driver,
        edge_stage,
        _EDGE_COLUMNS,
        (prefix + tuple(getattr(witness, field) for field in _EDGE_COLUMNS[4:]) for witness in query_result.edges),
    )
    try:
        counts = await database.scalar(
            f"SELECT {function_ref('admit_provider_directory_rooted_graph_witnesses')}("
            ":acquisition_id, :query_id, :attempt, :lease_token, "
            "CAST(:resource_stage AS regclass), CAST(:edge_stage AS regclass));",
            acquisition_id=claim.acquisition_id,
            query_id=claim.query_id,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
            resource_stage=f'pg_temp."{resource_stage}"',
            edge_stage=f'pg_temp."{edge_stage}"',
        )
    except DBAPIError as error:
        if "provider_directory_rooted_graph_lease_lost" in str(error.orig):
            raise ProviderDirectoryRootedGraphStoreError("lease_lost") from None
        raise
    if list(counts or ()) != [len(query_result.resources), len(query_result.edges)]:
        raise ProviderDirectoryRootedGraphStoreError("state")


_WORK_COLUMNS = (
    "acquisition_id",
    "scope_id",
    "query_id",
    "query_identity_sha256",
    "query_identity_json_text",
    "kind",
    "resource_type",
    "search_parameter",
    "reference_type",
    "reference_id",
    "closure_scope",
    "discovered_by_query_id",
    "discovered_source_type",
    "discovered_source_id",
    "discovered_edge_sha256",
    "status",
    "attempt_count",
    "pagination_terminal",
)


async def admit_work_specs(database, acquisition_id, work_specs, *, action):
    """Copy and validate a complete derived frontier within the current transaction."""
    async with database.transaction() as transaction:
        driver = await _copy_driver(transaction)
        await database.scalar(f"SELECT {function_ref('prepare_provider_directory_rooted_graph_work_stage')}()")
        await _copy_stage(
            driver,
            "pdrg_work_stage",
            _WORK_COLUMNS,
            (
                (acquisition_id,)
                + tuple(getattr(spec, column) for column in _WORK_COLUMNS[1:-3])
                + ("pending", 0, False)
                for spec in work_specs
            ),
        )
        inserted_count = await database.scalar(
            f"SELECT {function_ref('admit_provider_directory_rooted_graph_work')}(:acquisition_id,:action)",
            acquisition_id=acquisition_id,
            action=action,
        )
        if type(inserted_count) is not int or not 0 <= inserted_count <= len(work_specs):
            raise ProviderDirectoryRootedGraphStoreError("state")
        return inserted_count


async def copy_initial_root_work(database, acquisition_id):
    """Land bounded canonical source pages before one set admission and index build."""
    async with database.transaction() as transaction:
        prepared = await database.scalar(
            f"SELECT {function_ref('initialize_provider_directory_rooted_graph_work')}(:acquisition_id)",
            acquisition_id=acquisition_id,
        )
        if prepared == 0:
            return 0
        driver = await _copy_driver(transaction)
        after_id = ""
        while True:
            page_rows = await database.all(
                f"SELECT * FROM {function_ref('read_provider_directory_rooted_graph_initial_work')}(:acquisition_id,:after_id,:limit)",
                acquisition_id=acquisition_id,
                after_id=after_id,
                limit=_COPY_ROWS,
            )
            if not page_rows:
                break
            page_fields = [row_fields(source_row) for source_row in page_rows]
            await _copy_stage(
                driver,
                "pdrg_work_stage",
                _WORK_COLUMNS,
                (tuple(fields[column] for column in _WORK_COLUMNS) for fields in page_fields),
            )
            after_id = page_fields[-1]["reference_id"]
        return await database.scalar(
            f"SELECT {function_ref('admit_provider_directory_rooted_graph_work')}(:acquisition_id,'initialize')",
            acquisition_id=acquisition_id,
        )
