# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import ProviderDirectoryDatasetRehydrationCheckpoint


def test_rehydration_checkpoint_fences_dataset_lineage_and_counts():
    table = ProviderDirectoryDatasetRehydrationCheckpoint.__table__
    assert tuple(column.name for column in table.primary_key.columns) == (
        "source_id", "dataset_id", "acquisition_root_run_id", "resource_type"
    )
    assert isinstance(table.c.expected_input_count.type, sa.BigInteger)
    assert table.c.endpoint_id.nullable is False
    assert table.c.dataset_hash.nullable is False
    assert table.c.evidence_json.nullable is False


def test_rehydration_checkpoint_compiles_for_postgresql():
    compiled = str(CreateTable(ProviderDirectoryDatasetRehydrationCheckpoint.__table__).compile(dialect=postgresql.dialect()))
    assert "expected_input_count BIGINT NOT NULL" in compiled
    assert "PRIMARY KEY (source_id, dataset_id, acquisition_root_run_id, resource_type)" in compiled
