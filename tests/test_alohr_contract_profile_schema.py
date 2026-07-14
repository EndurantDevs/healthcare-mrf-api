import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260714161000_alohr_contract_profile.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "alohr_contract_profile_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_alohr_contract_profile_repair_is_current_dataset_scoped(monkeypatch):
    migration = _load_migration()
    executed_statements = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "provider_test")
    monkeypatch.setattr(
        migration.op,
        "execute",
        lambda statement: executed_statements.append(str(statement)),
    )

    migration.upgrade()

    assert migration.down_revision == (
        "20260714150000_provider_directory_pagination_census"
    )
    assert len(executed_statements) == 1
    repair_sql = executed_statements[0]
    assert "UPDATE provider_test.provider_directory_endpoint_dataset" in repair_sql
    assert migration.ALOHR_SOURCE_ID in repair_sql
    assert "dataset.is_current IS TRUE" in repair_sql
    assert "dataset.status = 'published'" in repair_sql
    assert "provider_directory_dataset_resource" in repair_sql
    assert "array_agg(DISTINCT resource.resource_type" in repair_sql
    assert repair_sql.count("publication_metadata_json::jsonb") == 4
    assert "#- '{resource_diagnostics,OrganizationAffiliation}'" in repair_sql
    assert "#- '{completion_proof_v1,resource_diagnostics,OrganizationAffiliation}'" in repair_sql
    assert "'{completion_proof_v1,selected_resources}'" in repair_sql
    assert repair_sql.count("OrganizationAffiliation") == 5


def test_alohr_contract_profile_downgrade_does_not_restore_false_claim(monkeypatch):
    migration = _load_migration()
    monkeypatch.setattr(
        migration.op,
        "execute",
        lambda _statement: (_ for _ in ()).throw(AssertionError("unexpected SQL")),
    )

    assert migration.downgrade() is None
