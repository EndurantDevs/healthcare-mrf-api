# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real predecessor publication APIs for explicit historical migration fixtures."""

import json
from contextlib import asynccontextmanager, contextmanager
from importlib import import_module

from process import provider_directory_dataset_candidate as candidates
from process import provider_directory_rooted_graph_publication_store as rooted
from process import uhc_flex_practitioner_publication_materialization as materialization
from process import uhc_flex_practitioner_publication_store as practitioner
from process.provider_directory_projection_stage import _copy_driver


@asynccontextmanager
async def _historical_candidate(database, dataset_id, *, kind):
    directory = import_module("process.provider_directory_fhir")
    if kind not in {"rooted", "practitioner"}:
        raise ValueError("provider_directory_candidate_kind_invalid")
    if database._transaction_binding() is None:
        raise RuntimeError("provider_directory_candidate_transaction_required")
    prepare = directory._qt(directory._schema(), f"prepare_pd_{kind}_dataset_candidate")
    if not await database.scalar("SELECT to_regprocedure(:signature) IS NOT NULL", signature=prepare + "(text)"):
        yield
        return
    relations = await database.scalar(f"SELECT {prepare}(:dataset_id)", dataset_id=dataset_id)
    if isinstance(relations, str):
        relations = json.loads(relations)
    if not isinstance(relations, dict) or not relations:
        raise RuntimeError("provider_directory_candidate_invalid")
    with directory._provider_directory_artifact_relation_scope(relations):
        yield relations
    finish = directory._qt(directory._schema(), f"finish_pd_{kind}_dataset_candidate")
    await database.scalar(f"SELECT {finish}(:dataset_id)", dataset_id=dataset_id)


async def _copy_historical_candidate(database, table, columns, records):
    directory = import_module("process.provider_directory_fhir")
    binding = database._transaction_binding()
    if binding is None or not 1 <= len(records) <= 4096:
        raise RuntimeError("provider_directory_candidate_copy_invalid")
    relation = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get().get(table, table)
    driver = await _copy_driver(binding.session)
    for batch in candidates._bounded_copy_batches(records):
        status = await driver.copy_records_to_table(
            relation, schema_name=directory._schema(), columns=columns, records=batch
        )
        if status != f"COPY {len(batch)}":
            raise RuntimeError("provider_directory_candidate_copy_incomplete")
    return len(records)


async def _publish_historical_practitioner(candidate_acquisition_id, endpoint_id, batch_size, *, database):
    async with database.transaction():
        admission, retry_exhausted_count = await practitioner._lock_admission(
            database, candidate_acquisition_id, endpoint_id
        )
        return await practitioner._publish_admitted_dataset(
            database, admission, endpoint_id, batch_size, retry_exhausted_count
        )


@contextmanager
def historical_publication_scope(monkeypatch):
    """Keep old storage and its real validators confined to migration proofs."""
    from process import provider_directory_rooted_graph_terminal as terminal
    from tests import provider_directory_rooted_graph_rotation_pg_support as rotation

    with monkeypatch.context() as patch:
        patch.setattr(terminal, "PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID", "")
        patch.setattr(practitioner, "publish_registered_uhc_flex_dataset", _publish_historical_practitioner)
        patch.setattr(rotation, "publish_registered_uhc_flex_dataset", _publish_historical_practitioner)
        for store in (practitioner, rooted):
            patch.setattr(store, "prepare_dataset_candidate", _historical_candidate)
        patch.setattr(candidates, "copy_dataset_candidate_rows", _copy_historical_candidate)
        patch.setattr(materialization, "copy_dataset_candidate_rows", _copy_historical_candidate)
        yield
