# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""DDL contract for specialized reviewed single-root admissions."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260812030000_provider_directory_specialized_single_root_admission.py"
)
SCHEMA = "specialized_single_root_test"


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        rendered = str(statement)
        assert ":1" not in rendered.replace(r"\:1", "")
        self.statements.append(rendered)


def _normalized(value: str) -> str:
    return " ".join(value.split())


def _load(name: str):
    module_spec = importlib.util.spec_from_file_location(name, MIGRATION_PATH)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _capture(monkeypatch, operation: str):
    migration = _load(f"specialized_single_root_{operation}")
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", SCHEMA)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = recorder
    getattr(migration, operation)()
    statements = [_normalized(statement) for statement in recorder.statements]
    return migration, statements, " ".join(statements)


def test_upgrade_is_one_disjoint_revision_and_keeps_legacy_guards(
    monkeypatch,
) -> None:
    migration, statements, sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260812030000_provider_directory_specialized_single_root_admission"
    )
    assert migration.down_revision == (
        "20260812020000_provider_directory_endpoint_dataset_admission_seal"
    )
    assert len(statements) == 35
    assert statements[0].startswith("LOCK TABLE")
    assert statements[0].count(
        '"provider_directory_uhc_flex_practitioner_dataset"'
    ) == 1
    assert sql.count("ADD COLUMN reviewed_root_policy_json jsonb") == 2
    assert "ADD COLUMN acquisition_operation_key varchar(64)" in sql
    for column in (
        "attempt_id",
        "baseline_acquisition_id",
        "baseline_run_id",
        "comparison_acquisition_id",
    ):
        assert f'ALTER COLUMN "{column}" DROP NOT NULL' in sql

    flex_check = _normalized(migration._flex_check_sql(SCHEMA, historical=False))
    assert "baseline_acquisition_id <> candidate_acquisition_id" in flex_check
    assert "baseline_run_id <> candidate_run_id" in flex_check
    assert "attempt_id IS NOT NULL" in flex_check
    assert "attempt_id IS NULL" in flex_check
    assert "reviewed_root_policy_json IS NOT NULL" in flex_check
    rooted_check = _normalized(migration._rooted_check_sql(SCHEMA, historical=False))
    assert "comparison_acquisition_id IS NOT NULL" in rooted_check
    assert "comparison_acquisition_id IS NULL" in rooted_check
    assert "acquisition_operation_key IS NOT NULL" in rooted_check
    assert migration._POLICY_SQL_JSON in flex_check
    assert migration._POLICY_SQL_JSON in rooted_check

    assert sql.count("CREATE FUNCTION") == 2
    assert sql.count("CREATE OR REPLACE FUNCTION") == 2
    assert (
        f'CREATE OR REPLACE FUNCTION "{SCHEMA}".'
        '"guard_pd_uhc_flex_practitioner_admission_insert"'
    ) not in sql
    assert (
        f'CREATE OR REPLACE FUNCTION "{SCHEMA}".'
        '"guard_provider_directory_rooted_graph_twin_admission"'
    ) not in sql
    assert "WHEN (NEW.admission_contract_id =" in sql
    assert sql.count("ENABLE ALWAYS TRIGGER") == 5
    assert "BEFORE UPDATE OR DELETE" in sql


def test_new_guards_rederive_exact_identities() -> None:
    """Bind every new admission to its sealed root identity."""

    migration = _load("specialized_single_root_contracts")
    flex_guard = _normalized(migration._flex_single_guard_sql(SCHEMA))
    rooted_guard = _normalized(migration._rooted_single_guard_sql(SCHEMA))

    for guard in (flex_guard, rooted_guard):
        assert "SECURITY DEFINER SET search_path = pg_catalog" in guard
        assert "expected_admission_id" in guard
        assert "expected_acquisition_id" in guard
        assert "admitted_at IS DISTINCT FROM transaction_timestamp()" in guard
    for literal in (
        migration._FLEX_SINGLE_CONTRACT,
        migration._FLEX_INTENT_DOMAIN,
        migration._FLEX_RUN_DOMAIN,
        "FOR SHARE OF official_cohort, official_source, official_dataset",
        "official_dataset.is_current IS TRUE",
        (
            "current_official.content_proof ->> 'proof_sha256' IS DISTINCT FROM "
            "current_official.official_content_proof_sha256"
        ),
        "candidate.storage_contract_id, candidate.cohort_id, 'candidate'",
        "candidate.query_contract_id, expected_run_id, expected_intent_id",
        "candidate.acquisition_id IS DISTINCT FROM expected_acquisition_id",
    ):
        assert literal in flex_guard
    for literal in (
        migration._ROOTED_SINGLE_CONTRACT,
        migration._ROOTED_SINGLE_OPERATOR_SHA256,
        "ptg_wave_canonical_json_ascii_v1",
        "'reviewed_root_policy'",
        "'acquisition_role', 'candidate'",
        "'None'",
        "'True'",
        "current_root.dataset_id IS NULL",
        "candidate.scope_id IS DISTINCT FROM expected_scope_id",
        "candidate.acquisition_id IS DISTINCT FROM expected_acquisition_id",
        ):
        assert literal in rooted_guard
    admission_digest = rooted_guard.split("expected_admission_id :=", 1)[1].split(
        "IF candidate", 1
    )[0]
    assert admission_digest.index("NEW.rooted_graph_sha256") < admission_digest.index(
        migration._POLICY_SQL_JSON
    ) < admission_digest.index("NEW.acquisition_operation_key")


def test_validators_preserve_historical_metadata_and_bind_new_policy() -> None:
    """Keep historical metadata exact and bind the reviewed-root policy."""

    migration = _load("specialized_single_root_metadata")
    flex_valid = _normalized(migration._flex_valid_function_sql(SCHEMA))
    rooted_valid = _normalized(
        migration._rooted_intrinsic_valid_function_sql(SCHEMA)
    )
    assert migration._FLEX_SINGLE_CONTRACT in flex_valid
    assert "provider_directory_reviewed_root_policy_v1" in flex_valid
    assert "- ARRAY[ 'baseline_acquisition_id', 'baseline_run_id' ]::text[]" in (
        flex_valid
    )
    assert "LEFT JOIN" in rooted_valid
    assert "attempt_id IS NOT DISTINCT FROM header.attempt_id" in rooted_valid
    assert (
        "comparison_acquisition_id IS NOT DISTINCT FROM "
        "header.comparison_acquisition_id"
    ) in rooted_valid
    assert "comparison.matched IS TRUE" in rooted_valid
    assert "provider_directory_reviewed_root_policy_v1" in rooted_valid
    assert "acquisition_operation_key" in rooted_valid

    flex_legacy = migration._flex_publication()._metadata_sql("header", "admission")
    rooted_legacy = migration._rooted()._rooted_expected_metadata_sql(
        "header", "admitted"
    )
    assert _normalized(
        f"THEN ({flex_legacy}) WHEN"
    ) in flex_valid
    assert _normalized(f"THEN ({rooted_legacy}) WHEN") in rooted_valid


def test_downgrade_is_fail_closed_and_restores_predecessor_contracts(
    monkeypatch,
) -> None:
    migration, statements, sql = _capture(monkeypatch, "downgrade")

    assert len(statements) == 31
    assert "provider_directory_specialized_single_root_downgrade_blocked" in (
        statements[1]
    )
    assert migration._FLEX_SINGLE_CONTRACT in statements[1]
    assert migration._ROOTED_SINGLE_CONTRACT in statements[1]
    assert "attempt_id IS NULL OR comparison_acquisition_id IS NULL" in statements[1]
    assert "BEFORE INSERT OR UPDATE OR DELETE" in sql
    assert sql.count("DROP FUNCTION") == 2
    assert sql.count("DROP COLUMN") == 3

    flex_check = _normalized(migration._flex_check_sql(SCHEMA, historical=True))
    assert "baseline_acquisition_id <> candidate_acquisition_id" in flex_check
    assert "baseline_run_id <> candidate_run_id" in flex_check
    assert "reviewed_root_policy_json" not in flex_check
    rooted_check = _normalized(migration._rooted_check_sql(SCHEMA, historical=True))
    assert migration._rooted()._TWIN_ADMISSION_CONTRACT in rooted_check
    assert "reviewed_root_policy_json" not in rooted_check
    assert "acquisition_operation_key" not in rooted_check

    for column in (
        "attempt_id",
        "baseline_acquisition_id",
        "baseline_run_id",
        "comparison_acquisition_id",
    ):
        assert f'ALTER COLUMN "{column}" SET NOT NULL' in sql
    assert "DROP CONSTRAINT" not in " ".join(
        statement for statement in statements if "candidate" in statement
    )
