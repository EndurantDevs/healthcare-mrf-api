# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sys
from contextlib import asynccontextmanager
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location("drug_claims_publish_contracts", MODULE_PATH)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_publish_contracts"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


class _PublishDatabase:
    def __init__(self):
        self.statements = []
        self.transaction_entries = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_entries += 1
        yield

    async def status(self, statement):
        self.statements.append(str(statement))
        return 1

    async def scalar(self, statement):
        assert "extension_record.extname = 'pg_trgm'" in str(statement)
        return "mrf"


@pytest.mark.asyncio
async def test_publish_renames_tables_and_declared_indexes(monkeypatch):
    prescription_model = type(
        "PricingPrescription",
        (),
        {
            "__main_table__": "pricing_prescription",
            "__my_initial_indexes__": (
                {"index_elements": ("rx_code",)},
                {},
            ),
            "__my_additional_indexes__": (
                {"name": "year_idx", "index_elements": ("source_year",)},
            ),
        },
    )
    provider_model = type(
        "PricingProviderPrescription",
        (),
        {
            "__main_table__": "pricing_provider_prescription",
            "__my_initial_indexes__": (),
            "__my_additional_indexes__": (
                {
                    "name": "pricing_provider_rx_autocomplete_trgm_idx",
                    "index_elements": ("lower(rx_name) gin_trgm_ops",),
                    "staging_name": "rx_ac_gin",
                },
            ),
        },
    )
    staged_class_by_name = {
        "PricingPrescription": SimpleNamespace(__tablename__="prescription_stage"),
        "PricingProviderPrescription": SimpleNamespace(__tablename__="provider_stage"),
    }
    publish_database = _PublishDatabase()
    monkeypatch.setattr(drug_claims, "PricingPrescription", prescription_model)
    monkeypatch.setattr(drug_claims, "PricingProviderPrescription", provider_model)
    monkeypatch.setattr(drug_claims, "db", publish_database)

    await drug_claims._publish_by_table_rename(staged_class_by_name, "mrf")
    sql_statements = "\n".join(publish_database.statements)
    assert publish_database.transaction_entries == 1
    assert "DROP TABLE IF EXISTS mrf.pricing_prescription" in sql_statements
    assert (
        "ALTER TABLE mrf.prescription_stage "
        "RENAME TO pricing_prescription"
    ) in sql_statements
    assert "ALTER TABLE IF EXISTS mrf.prescription_stage" not in sql_statements
    assert "prescription_stage_pricing_prescription_rx_code_idx" in sql_statements
    assert "prescription_stage_year_idx RENAME TO year_idx" in sql_statements
    assert "provider_stage RENAME TO pricing_provider_prescription" in sql_statements
    assert (
        "provider_stage_rx_ac_gin "
        "RENAME TO pricing_provider_rx_autocomplete_trgm_idx"
    ) in sql_statements


@pytest.mark.asyncio
async def test_staging_build_declares_autocomplete_trigram_index(monkeypatch):
    staging_model = drug_claims.make_class(
        drug_claims.PricingProviderPrescription,
        "autocomplete_contract",
        schema_override="mrf",
    )
    publish_database = _PublishDatabase()
    monkeypatch.setattr(drug_claims, "db", publish_database)

    await drug_claims._ensure_indexes(staging_model, "mrf")

    assert len(f"{staging_model.__tablename__}_rx_ac_gin") < 64
    autocomplete_statements = [
        statement
        for statement in publish_database.statements
        if "rx_ac_gin" in statement
    ]
    assert autocomplete_statements == [
        "CREATE INDEX IF NOT EXISTS "
        "pricing_provider_prescription_autocomplete_contract_rx_ac_gin "
        "ON mrf.pricing_provider_prescription_autocomplete_contract USING gin "
        "(lower(COALESCE(rx_name, '')) mrf.gin_trgm_ops, "
        "lower(COALESCE(generic_name, '')) mrf.gin_trgm_ops, "
        "lower(COALESCE(brand_name, '')) mrf.gin_trgm_ops, "
        "lower(COALESCE(rx_code, '')) mrf.gin_trgm_ops) "
        "WHERE rx_code_system = 'HP_RX_CODE';"
    ]


@pytest.mark.asyncio
async def test_staging_build_requires_pg_trgm_extension(monkeypatch):
    staging_model = drug_claims.make_class(
        drug_claims.PricingProviderPrescription,
        "autocomplete_missing_extension",
        schema_override="mrf",
    )
    publish_database = _PublishDatabase()
    publish_database.scalar = AsyncMock(return_value=None)
    monkeypatch.setattr(drug_claims, "db", publish_database)

    with pytest.raises(RuntimeError, match="pg_trgm extension is required"):
        await drug_claims._ensure_indexes(staging_model, "mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize("should_defer_indexes", [True, False])
async def test_finalize_materialization_orders_publish_steps(
    monkeypatch,
    should_defer_indexes,
):
    request = drug_claims.DrugClaimsFinalizeRequest(
        test_mode=False,
        import_id="import-one",
        run_id="run-one",
        stage_suffix="stage-one",
        schema="mrf",
        redis=None,
        manifest={},
        expected_chunks=0,
    )
    calls = []
    monkeypatch.setattr(
        drug_claims,
        "_staging_classes",
        lambda suffix, schema: {"PricingPrescription": "stage"},
    )
    monkeypatch.setattr(
        drug_claims,
        "_ensure_live_code_tables",
        AsyncMock(side_effect=lambda schema: calls.append("live")),
    )
    monkeypatch.setattr(
        drug_claims,
        "_materialize_prescription_and_code_rows",
        AsyncMock(side_effect=lambda classes, schema: calls.append("materialize")),
    )
    monkeypatch.setattr(
        drug_claims,
        "_build_staging_indexes",
        AsyncMock(side_effect=lambda classes, schema: calls.append("indexes")),
    )
    monkeypatch.setattr(
        drug_claims,
        "_publish_by_table_rename",
        AsyncMock(side_effect=lambda classes, schema: calls.append("publish")),
    )
    monkeypatch.setattr(drug_claims, "_step_start", lambda label: 0.0)
    monkeypatch.setattr(drug_claims, "_step_end", lambda label, started_at: None)
    monkeypatch.setattr(
        drug_claims,
        "DRUG_CLAIMS_DEFER_STAGE_INDEXES",
        should_defer_indexes,
    )

    await drug_claims._materialize_and_publish_drug_claims(request)
    expected_calls = ["live", "materialize"]
    if should_defer_indexes:
        expected_calls.append("indexes")
    expected_calls.append("publish")
    assert calls == expected_calls


def _drug_cleanup_request(manifest=None):
    return drug_claims.DrugClaimsFinalizeRequest(
        False,
        "import-one",
        "run-one",
        "stage-one",
        "mrf",
        None,
        manifest or {},
        0,
    )


def test_cleanup_requires_explicit_owned_work_directory(
    tmp_path,
    monkeypatch,
):
    empty_request = _drug_cleanup_request()
    drug_claims._cleanup_drug_claims_workdir(empty_request)

    work_dir_root = tmp_path / "drug-claims"
    monkeypatch.setattr(
        drug_claims,
        "DRUG_CLAIMS_WORKDIR",
        str(work_dir_root),
    )
    remove_tree = Mock(wraps=drug_claims.shutil.rmtree)
    monkeypatch.setattr(drug_claims.shutil, "rmtree", remove_tree)
    drug_claims._cleanup_drug_claims_workdir(
        _drug_cleanup_request({"status": "ready"})
    )
    remove_tree.assert_not_called()

    outside_directory = tmp_path / "outside"
    outside_directory.mkdir()
    drug_claims._cleanup_drug_claims_workdir(
        _drug_cleanup_request({"work_dir": str(outside_directory)})
    )
    assert outside_directory.is_dir()
    remove_tree.assert_not_called()


def test_cleanup_removes_completed_owned_work_directory(
    tmp_path,
    monkeypatch,
):
    work_dir_root = tmp_path / "drug-claims"
    completed_directory = work_dir_root / "import-one" / "run-one"
    completed_directory.mkdir(parents=True)
    monkeypatch.setattr(
        drug_claims,
        "DRUG_CLAIMS_WORKDIR",
        str(work_dir_root),
    )
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_KEEP_WORKDIR", False)
    drug_claims._cleanup_drug_claims_workdir(
        _drug_cleanup_request({"work_dir": str(completed_directory)})
    )
    assert not completed_directory.exists()


def test_cleanup_preserves_owned_work_directory_when_retained(
    tmp_path,
    monkeypatch,
):
    work_dir_root = tmp_path / "drug-claims"
    run_directory = work_dir_root / "import-one" / "run-one"
    run_directory.mkdir(parents=True)
    monkeypatch.setattr(
        drug_claims,
        "DRUG_CLAIMS_WORKDIR",
        str(work_dir_root),
    )
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_KEEP_WORKDIR", True)
    drug_claims._cleanup_drug_claims_workdir(
        _drug_cleanup_request({"work_dir": str(run_directory)})
    )
    assert run_directory.is_dir()


@pytest.mark.asyncio
async def test_finish_main_includes_explicit_manifest(monkeypatch):
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(drug_claims, "create_pool", AsyncMock(return_value=redis))
    monkeypatch.setattr(drug_claims.secrets, "token_hex", lambda size: "abcd")

    finish_result = await drug_claims.finish_main(
        import_id="import-one",
        run_id="run-one",
        test_mode=True,
        manifest_path="/tmp/synthetic-manifest.json",
    )
    enqueue_call = redis.enqueue_job.await_args
    assert finish_result["queued"] is True
    assert enqueue_call.args[1]["manifest_path"] == "/tmp/synthetic-manifest.json"
    assert enqueue_call.kwargs["_job_id"] == "drug_claims_finalize_run-one_abcd"
