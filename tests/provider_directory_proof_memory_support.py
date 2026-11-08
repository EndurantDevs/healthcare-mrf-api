# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Memory-only proof semantic tests keep native authority in its own real tests."""

from types import SimpleNamespace

import pytest

from process import provider_directory_source_proof_control as control


@pytest.fixture(autouse=True)
def proof_memory_authority(monkeypatch):
    """Substitute only the new native boundary around existing in-memory proofs."""

    async def memory_executor(executor, schema):
        return SimpleNamespace(connection=executor, schema=schema)

    async def memory_parent(publisher, dataset_id, *, acquiring):
        return await publisher.connection.first(
            f"SELECT endpoint_id,acquisition_root_run_id,publication_metadata_json "
            f'FROM "{publisher.schema}"."provider_directory_endpoint_dataset" '
            "WHERE dataset_id=:dataset_id FOR SHARE",
            dataset_id=dataset_id,
        )

    monkeypatch.setattr(control, "require_proof_executor", memory_executor)
    monkeypatch.setattr(control, "lock_proof_parent", memory_parent)
