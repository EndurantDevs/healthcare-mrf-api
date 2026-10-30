# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native private-header CAS and terminal controls; runtime issuance is tested separately."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_source_checkpoint_control as checkpoints
from process import provider_directory_source_failure_control as control
from process import provider_directory_source_proof_control as proofs
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_isolated_source_provision import _dsn, _provision, native_bootstrap, quote


@asynccontextmanager
async def _native_parent(case, *, metadata_type):
    parent = storage_relation(case.policy.schema, control.storage.HEADER)
    frozen = storage_relation(case.policy.schema, "frozen_header")
    if metadata_type == "json":
        await case.connection.execute(
            f"ALTER TABLE {storage_relation('catalog', control.storage.HEADER)} "
            "ALTER COLUMN publication_metadata_json TYPE json USING publication_metadata_json::json"
        )
    async with case.connection.transaction():
        await _provision(case)
    writer = await asyncpg.connect(_dsn(case.url.set(username=case.roles["publisher"])))
    try:
        await writer.execute(
            f"INSERT INTO {parent}(dataset_id,endpoint_id,acquisition_root_run_id,import_run_id,status,is_current,"
            "resource_count,dataset_hash,created_at,validated_at,publication_metadata_json,candidate_storage_json) "
            "VALUES('selected','endpoint','root','attempt','validated',false,1,repeat('a',64),"
            "TIMESTAMP '2026-01-02 03:04:05',TIMESTAMP '2026-01-02 04:05:06',"
            '\'{"nested":{"value":1},"source_ids":["source"]}\', \'{"sealed":true}\')'
        )
        await case.connection.execute(f"CREATE TABLE {frozen} (LIKE {parent} INCLUDING ALL)")
        await case.connection.execute(f"INSERT INTO {frozen} SELECT * FROM {parent}")
        await case.connection.execute(f"ALTER TABLE {frozen} OWNER TO {quote(case.roles['namespace'])}")
        assert not await case.connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')", case.roles["owner"], frozen
        )
        encoded = await case.connection.fetchval(f"SELECT to_jsonb(snapshot)::text FROM {frozen} snapshot")
        yield SimpleNamespace(
            connection=writer,
            policy=case.policy,
            header=parent,
            frozen=frozen,
            expected=json.loads(encoded),
            encoded=encoded,
        )
    finally:
        await writer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_type", ["json", "jsonb"])
async def test_native_acknowledgement_changes_only_status_and_closes_all_parent_writers(
    native_bootstrap, metadata_type
):
    case = native_bootstrap
    async with _native_parent(case, metadata_type=metadata_type) as publisher:
        async with publisher.connection.transaction():
            await publisher.connection.execute("SET LOCAL ROLE " + quote(case.roles["owner"]))
            await publisher.connection.execute("SET LOCAL TimeZone='Pacific/Honolulu'")
            await publisher.connection.execute("SET LOCAL DateStyle='SQL, DMY'")
            assert await publisher.connection.fetchval("SELECT current_user::text") == case.roles["owner"]
            before = json.loads(
                await publisher.connection.fetchval(f"SELECT to_jsonb(parent)::text FROM {publisher.header} parent")
            )
            await control._acknowledge_header(publisher, "selected", publisher.expected)
            await control._acknowledge_header(publisher, "selected", publisher.expected)
            after = json.loads(
                await publisher.connection.fetchval(f"SELECT to_jsonb(parent)::text FROM {publisher.header} parent")
            )
            assert after == {**before, "status": "publication_acknowledged"}
            assert await publisher.connection.fetchval("SHOW TimeZone") == "Pacific/Honolulu"
            assert await publisher.connection.fetchval("SHOW DateStyle") == "SQL, DMY"
            with pytest.raises(GenericCandidateError, match="checkpoint_parent_abandoned"):
                await checkpoints.fence_checkpoint_dataset_parents(publisher, ["selected"], ["root"])
            with pytest.raises(GenericCandidateError, match="proof_parent_immutable"):
                await proofs.lock_proof_parent(publisher, "selected", acquiring=False)
        assert (
            await case.connection.fetchval(f"SELECT to_jsonb(snapshot)::text FROM {publisher.frozen} snapshot")
            == publisher.encoded
        )
        assert await case.connection.fetchval(f"SELECT status FROM {publisher.header}") == "publication_acknowledged"
        assert (
            await case.connection.fetchval(
                f"SELECT count(*) FROM {storage_relation('catalog', control.storage.HEADER)}"
            )
            == 0
        )


@pytest.mark.asyncio
async def test_native_complete_row_comparison_rejects_changed_or_incomplete_acknowledgements(native_bootstrap):
    case = native_bootstrap
    async with _native_parent(case, metadata_type="json") as publisher:
        async with publisher.connection.transaction():
            await publisher.connection.execute("SET LOCAL ROLE " + quote(case.roles["owner"]))
            for expected in _changed_headers(publisher.expected):
                with pytest.raises(GenericCandidateError, match="publication_acknowledgement_parent_changed"):
                    await control._acknowledge_header(publisher, "selected", expected)
                assert await publisher.connection.fetchval(f"SELECT status FROM {publisher.header}") == "validated"
        assert (
            await case.connection.fetchval(f"SELECT to_jsonb(snapshot)::text FROM {publisher.frozen} snapshot")
            == publisher.encoded
        )


def _changed_headers(expected):
    return [
        {**expected, "publication_metadata_json": {**expected["publication_metadata_json"], "nested": {"value": 2}}},
        {**expected, "content_proof_admission_sha256": "b" * 64},
        {**expected, "created_at": "2026-01-03T03:04:05"},
        {key: value for key, value in expected.items() if key != "superseded_at"},
        {**expected, "unrecognized": None},
    ]
