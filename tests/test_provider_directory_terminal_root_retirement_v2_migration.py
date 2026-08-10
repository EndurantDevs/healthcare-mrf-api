# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Static contracts for additive terminal-root retirement v2."""

from __future__ import annotations

from hashlib import sha256
import importlib.util
from pathlib import Path

import pytest

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as legacy_evidence,
)
from db import (
    migration_provider_directory_terminal_root_retirement_guards as legacy_guards,
)
from db import migration_provider_directory_terminal_root_retirement_v2 as retirement_v2


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / "20260810120000_provider_directory_terminal_root_retirement_v2.py"
)
_LEGACY_SQL_SHA256 = {
    "relation": "3e82da8a9fffc3ef64f97ea91e0717c939ec886db43798f9eaeb3f68f030c4d6",
    "evidence": "d99685d44848d3f8d5d00c2db3b50fd5d5ac5cd397a6e3a5a269e1ae5e8aeefa",
    "eligible": "229f40083f01562d30332d7c222d668576ab297c6970e5ae4afc4ec673733696",
    "marker": "3518d5af21e13d86c8956858b3e2b314f948acc30d1b8b60cc4850282849f769",
    "valid": "a19a17804a39f93dc193676736bdf836c4204c6dc0cf20e2302da24cf856e1a6",
    "run": "78a1cb7212328fee722c203ddbde50db210339ef741ac7653a3abe7773afdeac",
    "parent": "0f104e7ec75583e392e6d742c30d1c3093ecc6856e9e0388d2148cf7fa24544f",
    "child": "3c22a2c483fe2fc01f8e8189c972216c993b382c9f06bac431d5d67a282edbc7",
    "import": "f4d8afa04f0ff261511083ac057cbf9abf63fcec8b6a2a49b7b4a556566a2e6b",
}
_V2_SQL_SHA256 = {
    "evidence": "56cd513884c07a9105dbe87b98625f86fe16b9ffc501b0341e892ba239cb18fc",
    "eligible": "b4a034810993a58b0c35c49af78fe4b750253276fa99472d3baa8143e391f34f",
    "marker": "56c8d306c4140be9374c0b6ff3784987c7f8cf07941af60577a7102f7eaa307e",
    "valid": "dbf5fc6f4e32c0f12c8c92e79e27d6457d7f306fdb6ed8f39cd7f35bad55018e",
    "parent": "1eeb892a667156ca10229d344d73fea963cc731c2cd45405f9adc4281fc2d4d6",
}


class _OperationsRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object) -> None:
        self.statements.append(str(statement))


def _migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_terminal_root_retirement_v2_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _sha256(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _normalized(value: str) -> str:
    return " ".join(value.split())


def _statement_index(statements: list[str], marker: str) -> int:
    """Return the first migration statement containing one marker."""

    return next(
        index for index, statement in enumerate(statements) if marker in statement
    )


def _assert_upgrade_catalog_fences(normalized_sql: str) -> None:
    """Require every frozen function and trigger catalog attribute."""

    required_fragments = (
        "trigger_row.tgenabled = 'A'",
        "trigger_row.tgconstraint = 0",
        "trigger_row.tgparentid = 0",
        "trigger_row.tgconstrrelid = 0",
        "trigger_row.tgconstrindid = 0",
        "function_row.proowner",
        "function_row.pronargdefaults = 0",
        "function_row.proargnames IS NOT DISTINCT FROM",
        "function_row.proleakproof IS FALSE",
        "function_row.protrftypes IS NULL",
        "function_row.probin IS NULL",
        "function_row.prosqlbody IS NULL",
        "function_row.proname::text = ANY",
        "pg_catalog.left(trigger_row.tgname, 7) = 'pd_trr_'",
        "function_acl.grantee <> function_row.proowner",
    )
    for required_fragment in required_fragments:
        assert required_fragment in normalized_sql


def test_v1_generator_output_remains_byte_frozen() -> None:
    schema = "terminal_test"
    rendered_by_name = {
        "relation": legacy_evidence.relation_evidence_function_sql(schema),
        "evidence": legacy_evidence.evidence_function_sql(schema),
        "eligible": legacy_guards.eligible_function_sql(schema),
        "marker": legacy_guards.marker_function_sql(schema),
        "valid": legacy_guards.valid_function_sql(schema),
        "run": legacy_guards.run_retired_function_sql(schema),
        "parent": legacy_guards.parent_guard_function_sql(schema),
        "child": legacy_guards.child_guard_function_sql(schema),
        "import": legacy_guards.import_run_guard_function_sql(schema),
    }
    assert {
        name: _sha256(rendered_sql) for name, rendered_sql in rendered_by_name.items()
    } == _LEGACY_SQL_SHA256


def test_v2_generator_output_is_byte_frozen() -> None:
    schema = "terminal_test"
    rendered_by_name = {
        "evidence": retirement_v2.evidence_function_sql(schema),
        "eligible": retirement_v2.eligible_function_sql(schema),
        "marker": retirement_v2.marker_function_sql(schema),
        "valid": retirement_v2.valid_function_sql(schema),
        "parent": retirement_v2.parent_guard_function_sql(schema),
    }
    assert {
        name: _sha256(rendered_sql) for name, rendered_sql in rendered_by_name.items()
    } == _V2_SQL_SHA256


def test_v2_evidence_has_distinct_parent_marker_identity() -> None:
    sql = retirement_v2.evidence_function_sql("terminal_test")
    assert retirement_v2.EVIDENCE_FUNCTION in sql
    assert retirement_v2.MARKER in sql
    assert legacy_evidence.MARKER not in sql
    assert "pg_catalog.sum(grouped.row_count)" in sql
    assert "SET TimeZone = 'UTC'" in sql
    assert "provider_directory_terminal_root_retirement_relation_evidence" in sql


def test_v2_eligibility_is_explicit_semantic_content_v4_only() -> None:
    sql = _normalized(retirement_v2.eligible_function_sql("terminal_test"))
    assert "parent.status IN ('acquiring', 'acquisition_retired')" in sql
    assert "? 'resource_hash_contract'" in sql
    assert "resource_hash_contract' = 'semantic_content_v4'" in sql
    assert "transport_bound_v1" not in sql
    assert "completion_proof_required_version IS NULL" in sql
    assert "lineage_shape.is_linear" in sql
    assert "minimum_age BETWEEN 900 AND 604800" in sql


def test_v2_marker_and_valid_functions_are_version_closed() -> None:
    marker_sql = retirement_v2.marker_function_sql("terminal_test")
    valid_sql = retirement_v2.valid_function_sql("terminal_test")
    assert retirement_v2.CONTRACT in marker_sql
    assert legacy_guards.CONTRACT not in marker_sql
    assert retirement_v2.EVIDENCE_FUNCTION in marker_sql
    assert legacy_evidence.EVIDENCE_FUNCTION not in marker_sql
    assert retirement_v2.MARKER in valid_sql
    assert retirement_v2.MARKER_FUNCTION in valid_sql
    assert retirement_v2.ELIGIBLE_FUNCTION in valid_sql


def test_dual_parent_guard_requires_exactly_one_profile() -> None:
    sql = _normalized(retirement_v2.parent_guard_function_sql("terminal_test"))
    assert legacy_evidence.MARKER in sql
    assert retirement_v2.MARKER in sql
    assert "legacy_marker_present = v2_marker_present" in sql
    assert legacy_guards.ELIGIBLE_FUNCTION in sql
    assert retirement_v2.ELIGIBLE_FUNCTION in sql
    assert legacy_evidence.EVIDENCE_FUNCTION in sql
    assert retirement_v2.EVIDENCE_FUNCTION in sql
    assert "OLD.status = 'acquisition_retired'" in sql
    assert "terminal_root_retirement_marker_forbidden" in sql


def test_migration_is_additive_and_fenced(monkeypatch) -> None:
    """Install v2 only after exact legacy function and trigger fences."""

    migration = _migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    migration.upgrade()

    sql = "\n".join(recorder.statements)
    normalized_sql = _normalized(sql)
    assert migration.revision == (
        "20260810120000_provider_directory_terminal_root_retirement_v2"
    )
    assert migration.down_revision == (
        "20260810110000_provider_directory_reviewed_subset_direct_v4_" "disposition"
    )
    assert recorder.statements[0] == (
        'LOCK TABLE "terminal_test"."provider_directory_endpoint_dataset" '
        "IN SHARE ROW EXCLUSIVE MODE;"
    )
    assert "terminal_root_retirement_v2_adoption_blocked" in sql
    assert "terminal_root_retirement_v1_state_invalid" in sql
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 1
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 5
    assert normalized_sql.count("v2_function_changed") == 24
    assert normalized_sql.count("v2_trigger_changed") == 2
    _assert_upgrade_catalog_fences(normalized_sql)
    adoption_index = _statement_index(recorder.statements, "v2_adoption_blocked")
    first_function_fence = _statement_index(recorder.statements, "v2_function_changed")
    first_trigger_fence = _statement_index(recorder.statements, "v2_trigger_changed")
    assert first_function_fence < first_trigger_fence < adoption_index
    assert "DROP FUNCTION" not in sql


def test_downgrade_restores_v1_only_when_v2_is_unused(monkeypatch) -> None:
    migration = _migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    sql = "\n".join(recorder.statements)
    normalized_sql = _normalized(sql)
    assert "terminal_root_retirement_v2_downgrade_blocked" in sql
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 1
    assert normalized_sql.count("DROP FUNCTION") == 4
    assert normalized_sql.count("v2_trigger_changed") == 2
    assert "function_row.proname::text = ANY" in normalized_sql
    unused_index = _statement_index(recorder.statements, "v2_downgrade_blocked")
    first_function_fence = _statement_index(recorder.statements, "v2_function_changed")
    first_trigger_fence = _statement_index(recorder.statements, "v2_trigger_changed")
    assert first_function_fence < first_trigger_fence < unused_index
    assert legacy_evidence.MARKER in sql
    assert retirement_v2.MARKER in sql


def test_migration_rejects_schema_and_generator_drift(monkeypatch) -> None:
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    with pytest.raises(RuntimeError, match="must match"):
        migration._schema()
    with pytest.raises(RuntimeError, match="function body changed"):
        migration._function_body("SELECT 1")
    with pytest.raises(RuntimeError, match="replacement SQL changed"):
        migration._replacement_sql("SELECT 1")
    duplicate_spec = migration._legacy_function_specs("terminal_test")[0]
    with pytest.raises(RuntimeError, match="function names changed"):
        migration._function_topology_fence_sql(
            "terminal_test", (duplicate_spec, duplicate_spec)
        )
    assert len(migration._trigger_specs("terminal_test")) == 28


def test_v2_clone_rejects_legacy_generator_drift(monkeypatch) -> None:
    monkeypatch.setattr(
        legacy_evidence,
        "evidence_function_sql",
        lambda _schema: "CREATE FUNCTION unknown() RETURNS jsonb AS 'x'",
    )
    with pytest.raises(RuntimeError, match="v1 SQL shape changed"):
        retirement_v2.evidence_function_sql("terminal_test")


def test_migration_rejects_frozen_v2_generator_drift(monkeypatch) -> None:
    migration = _migration()
    monkeypatch.setattr(
        retirement_v2,
        "valid_function_sql",
        lambda _schema: "CREATE FUNCTION drifted() RETURNS boolean",
    )
    with pytest.raises(RuntimeError, match="frozen SQL changed"):
        migration._assert_frozen_generators()
