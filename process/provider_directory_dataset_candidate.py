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
    """Route materialization to private heaps, then attach the completed group."""
    directory = import_module("process.provider_directory_fhir")

    if kind not in {"rooted", "practitioner"}:
        raise ValueError("provider_directory_candidate_kind_invalid")
    if database._transaction_binding() is None:
        raise RuntimeError("provider_directory_candidate_transaction_required")
    relations = await database.scalar(
        f"SELECT {directory._qt(directory._schema(), f'prepare_pd_{kind}_dataset_candidate')}(:dataset_id);",
        dataset_id=dataset_id,
    )
    if isinstance(relations, str):
        relations = json.loads(relations)
    if not isinstance(relations, dict) or not relations:
        raise RuntimeError("provider_directory_candidate_invalid")
    with directory._provider_directory_artifact_relation_scope(relations):
        yield relations
    await database.scalar(
        f"SELECT {directory._qt(directory._schema(), f'finish_pd_{kind}_dataset_candidate')}(:dataset_id);",
        dataset_id=dataset_id,
    )


async def copy_dataset_candidate_rows(database, table_name, columns, records):
    """Native COPY of one bounded materialization page into its isolated heap."""
    directory = import_module("process.provider_directory_fhir")

    binding = database._transaction_binding()
    relation = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get().get(table_name)
    if binding is None or not relation or not 1 <= len(records) <= 4096:
        raise RuntimeError("provider_directory_candidate_copy_invalid")
    driver = await _copy_driver(binding.session)
    for batch in _bounded_copy_batches(records):
        status = await driver.copy_records_to_table(
            relation,
            schema_name=directory._schema(),
            columns=columns,
            records=batch,
        )
        if status != f"COPY {len(batch)}":
            raise RuntimeError("provider_directory_candidate_copy_incomplete")
    return len(records)
