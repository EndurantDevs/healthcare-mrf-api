# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Candidate COPY, completed indexes and cutover remain transactionally isolated."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import uhc_flex_practitioner_publication_materialization as materialization
from process import uhc_flex_practitioner_publication_store as store
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    ENDPOINT_ID,
    _publication_test_scope,
    _sealed_pair,
    connect,
    publication,
)


async def _publish_with_failure_probes(monkeypatch, database, admission, reader, assert_unpublished):
    copier = materialization.copy_dataset_candidate_rows
    validator = store._validate_candidate
    candidate_relations = set()
    fail_at = "copy"

    async def observe_copy(*args):
        count = await copier(*args)
        candidate_relations.update(
            publication._table(name)
            for name in (
                publication._DATASET_RESOURCE,
                publication._PROVENANCE,
            )
        )
        await assert_unpublished()
        if fail_at == "copy":
            raise RuntimeError("synthetic copy failure")
        return count

    async def observe_validation(*args):
        validation = await validator(*args)
        await assert_unpublished()
        if fail_at == "validate":
            raise RuntimeError("synthetic validate failure")
        return validation

    monkeypatch.setattr(materialization, "copy_dataset_candidate_rows", observe_copy)
    monkeypatch.setattr(store, "_validate_candidate", observe_validation)
    for fail_at in ("copy", "validate"):
        with pytest.raises(RuntimeError, match=f"synthetic {fail_at} failure"):
            await publication.publish_uhc_flex_practitioner_dataset(
                admission.candidate_acquisition_id,
                database=database,
                batch_size=1,
            )
        await assert_unpublished()
        for relation in candidate_relations:
            assert await reader.fetchval("SELECT to_regclass($1)", relation) is None
    fail_at = None
    return await publication.publish_uhc_flex_practitioner_dataset(
        admission.candidate_acquisition_id,
        database=database,
        batch_size=1,
    )


async def _assert_published_resource_immutable(reader, schema, dataset_id):
    resources = f"{schema}.provider_directory_dataset_resource"
    original_rows = await reader.fetch(f"SELECT * FROM {resources} WHERE dataset_id=$1", dataset_id)
    statements = (
        ({"55000"}, f"UPDATE {resources} SET payload_hash=repeat('0',64) WHERE dataset_id=$1"),
        ({"55000", "23503"}, f"DELETE FROM {resources} WHERE dataset_id=$1"),
        (
            {"55000"},
            f"INSERT INTO {resources}(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
            f"SELECT dataset_id,resource_type,resource_id||'-tamper',payload_hash,payload_json FROM {resources} WHERE dataset_id=$1",
        ),
    )
    for allowed_states, statement in statements:
        with pytest.raises(asyncpg.PostgresError) as failure:
            async with reader.transaction():
                await reader.execute(statement, dataset_id)
        assert failure.value.sqlstate in allowed_states
    candidate = await reader.fetchval(
        f"SELECT tableoid::regclass::text FROM {resources} WHERE dataset_id=$1", dataset_id
    )
    await assert_sqlstate(reader, "55000", f"DELETE FROM {candidate}")
    assert await reader.fetch(f"SELECT * FROM {resources} WHERE dataset_id=$1", dataset_id) == original_rows


async def _assert_unpublished_snapshot(reader, schema, pinned_id, pinned_rows, candidate_id):
    header = f"{schema}.provider_directory_uhc_flex_practitioner_dataset"
    resources = f"{schema}.provider_directory_dataset_resource"
    assert await reader.fetchval(f"SELECT dataset_id FROM {header} WHERE is_current") == pinned_id
    assert await reader.fetchval(f"SELECT count(*) FROM {header} WHERE dataset_id=$1", candidate_id) == 0
    assert await reader.fetch(f"SELECT tableoid,* FROM {resources} WHERE dataset_id=$1", pinned_id) == pinned_rows
    assert await reader.fetchval(f"SELECT count(*) FROM {resources} WHERE dataset_id=$1", candidate_id) == 0


@pytest.mark.asyncio
async def test_practitioner_candidate_failure_retry_and_pinned_read(monkeypatch):
    async with _publication_test_scope(monkeypatch) as (url, schema, database, *_):
        monkeypatch.setattr(
            publication,
            "register_uhc_flex_practitioner_source",
            AsyncMock(return_value=SimpleNamespace(endpoint_id=ENDPOINT_ID)),
        )
        first = await _sealed_pair(database, operation_key="c" * 64, matched=True)
        previous = await publication.publish_uhc_flex_practitioner_dataset(
            first.candidate_acquisition_id,
            database=database,
            batch_size=1,
        )
        second = await _sealed_pair(database, operation_key="d" * 64, matched=True)
        identity = publication.build_uhc_flex_practitioner_dataset_identity(second, endpoint_id=ENDPOINT_ID)
        reader = await connect(url)
        try:
            await reader.execute("SET statement_timeout='2s'")
            header = f"{schema}.provider_directory_uhc_flex_practitioner_dataset"
            resources = f"{schema}.provider_directory_dataset_resource"
            pinned_id = await reader.fetchval(f"SELECT dataset_id FROM {header} WHERE is_current")
            assert pinned_id == previous.readiness.dataset_id
            pinned_rows = await reader.fetch(f"SELECT tableoid,* FROM {resources} WHERE dataset_id=$1", pinned_id)

            async def assert_unpublished():
                await _assert_unpublished_snapshot(reader, schema, pinned_id, pinned_rows, identity.dataset_id)

            current = await _publish_with_failure_probes(
                monkeypatch,
                database,
                second,
                reader,
                assert_unpublished,
            )
            assert current.readiness.previous_dataset_id == pinned_id
            assert (
                await reader.fetchval(f"SELECT dataset_id FROM {header} WHERE is_current")
                == current.readiness.dataset_id
            )
            assert (
                await reader.fetch(f"SELECT tableoid,* FROM {resources} WHERE dataset_id=$1", pinned_id) == pinned_rows
            )
            assert (
                await reader.fetchval(
                    f"SELECT count(*) FROM {resources} WHERE dataset_id=$1", current.readiness.dataset_id
                )
                == 1
            )
            await _assert_published_resource_immutable(reader, schema, current.readiness.dataset_id)
        finally:
            await reader.close()
