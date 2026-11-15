# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Memory-only proof semantics with the normal native batch statement interface."""

from types import SimpleNamespace

import pytest

from process import provider_directory_proof_store as proof_store
from process import provider_directory_source_proof_control as control


@pytest.fixture(autouse=True)
def proof_memory_authority(monkeypatch):
    """Map native batch statements onto the existing in-memory proof storage."""

    async def memory_executor(executor, schema):
        async def execute(query, *arguments):
            if "INSERT INTO" in query:
                fields = (
                    "dataset_id",
                    "shard_id",
                    "endpoint_id",
                    "acquisition_root_run_id",
                    "source_ids_json",
                    "resource_count",
                    "resource_counts_json",
                    "first_identity_json",
                    "last_identity_json",
                    "input_sha256",
                    "artifact_sha256",
                    "artifact_byte_count",
                    "payload_bytes",
                )
            else:
                fields = ("dataset_id", "resource_type") if "resource_counts_json ?" in query else ("dataset_id",)
            return await executor.status(query, **dict(zip(fields, arguments, strict=True)))

        async def fetchrow(query, dataset_id, shard_id):
            return await executor.first(query, dataset_id=dataset_id, shard_id=shard_id)

        async def first(query, **parameters):
            return await executor.first(query, **parameters)

        connection = SimpleNamespace(execute=execute, fetchrow=fetchrow, first=first, is_in_transaction=lambda: True)
        return SimpleNamespace(connection=connection, schema=schema)

    async def memory_parent(publisher, dataset_id, *, acquiring):
        return await publisher.connection.first(
            f"SELECT endpoint_id,acquisition_root_run_id,publication_metadata_json "
            f'FROM "{publisher.schema}"."provider_directory_endpoint_dataset" '
            "WHERE dataset_id=:dataset_id FOR SHARE",
            dataset_id=dataset_id,
        )

    monkeypatch.setattr(control, "require_proof_executor", memory_executor)
    monkeypatch.setattr(control, "lock_proof_parent", memory_parent)
    monkeypatch.setattr(proof_store, "source_copy_executor", lambda executor: executor)
