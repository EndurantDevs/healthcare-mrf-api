# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Runtime schema maintenance cannot recreate controlled serving storage."""

from types import SimpleNamespace

from db import maintenance
from db.models import PlanReleaseServingRevision, PlanReleaseSnapshotBinding, PlanReleaseSnapshotPin


def test_runtime_schema_sync_skips_every_controlled_serving_table(monkeypatch):
    models = (PlanReleaseServingRevision, PlanReleaseSnapshotBinding, PlanReleaseSnapshotPin)
    tables = [model.__table__ for model in models]
    models_by_name = {model.__table__.fullname: model for model in models}

    class Inspector:
        def has_table(self, *_args, **_kwargs):
            raise AssertionError("runtime maintenance reached migration-owned storage")

    monkeypatch.setattr(maintenance, "Base", SimpleNamespace(metadata=SimpleNamespace(sorted_tables=tables)))
    monkeypatch.setattr(maintenance, "inspect", lambda _connection: Inspector())
    monkeypatch.setattr(maintenance, "_model_by_table_fullname", lambda: models_by_name)
    monkeypatch.setattr(maintenance, "_managed_schemas", lambda: [table.schema for table in tables])
    monkeypatch.setattr(
        maintenance,
        "ProviderDirectoryPaginationCheckpoint",
        SimpleNamespace(__table__=SimpleNamespace(schema="unmanaged_fixture")),
    )
    summary_by_kind = {}
    maintenance._sync_structure(object(), summary_by_kind, add_columns=True, add_indexes=True)
    assert summary_by_kind == {}
