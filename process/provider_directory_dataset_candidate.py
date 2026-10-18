# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare indexed dataset partitions inside the publication transaction."""

import json
from contextlib import asynccontextmanager
from importlib import import_module

from process.provider_directory_projection_stage import _copy_driver

COPY_MAX_BYTES = 32 * 1024 * 1024
COPY_FRAME_BYTES = 21


def _bounded_copy_batches(records):
    batch_records = []
    batch_bytes = COPY_FRAME_BYTES
    for record in records:
        # Reserve the JSONB version byte for every text field; fixed-width fields use at most eight bytes.
        record_bytes = 2 + sum(
            4 + (1 + len(value.encode("utf-8")) if isinstance(value, str) else 8) for value in record
        )
        if COPY_FRAME_BYTES + record_bytes > COPY_MAX_BYTES:
            raise RuntimeError("provider_directory_candidate_record_too_large")
        if batch_records and batch_bytes + record_bytes > COPY_MAX_BYTES:
            yield batch_records
            batch_records = []
            batch_bytes = COPY_FRAME_BYTES
        batch_records.append(record)
        batch_bytes += record_bytes
    if batch_records:
        yield batch_records


@asynccontextmanager
async def prepare_dataset_candidate(database, dataset_id, *, kind):
    """Use the explicit native publisher; no custom database prepare/finish functions."""
    from process.provider_directory_generic_source_kind_candidate import native_kind_candidate

    directory = import_module("process.provider_directory_fhir")
    async with native_kind_candidate(database, directory, dataset_id, kind=kind) as relations:
        yield relations


async def copy_dataset_candidate_rows(database, table_name, columns, records):
    """COPY one complete bounded page using the original native publisher owner."""
    from process.provider_directory_generic_source_policy import bound_publisher
    from process.provider_directory_generic_source_storage import _copy

    directory = import_module("process.provider_directory_fhir")
    publisher = bound_publisher()
    binding = database._transaction_binding()
    relation = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get().get(table_name)
    if binding is None or not relation or not 1 <= len(records) <= 4096:
        raise RuntimeError("provider_directory_candidate_copy_invalid")
    driver = await _copy_driver(binding.session)
    if driver is not publisher.connection or publisher.policy.schema != directory._schema():
        raise RuntimeError("provider_directory_generic_source_physical_transaction_changed")
    return await _copy(driver, publisher.policy, relation, columns, records)
