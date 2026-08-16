# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Evidence-specific reviewed address alias contracts."""

from db.models import AddressAliasCandidateV1
from process.ext import address_evidence_alias_sql
from tests.test_address_numeric_grid_alias import _Recorder, _load_evidence_migration


def test_evidence_alias_sql_requires_visible_same_npi_exact_matches():
    sql = address_evidence_alias_sql.evidence_candidate_insert_sql(
        schema="mrf",
        archive='"mrf"."address_archive_v2"',
    )
    normalized = " ".join(sql.split()).lower()

    assert '"mrf"."entity_address_unified"' in sql
    assert "public_evidence_npi_valid" in sql
    assert "target.npi = source.npi" in sql
    assert "target.state_code = source.state_code" in sql
    assert "target.zip5 = source.zip5" in sql
    assert "target.country_code = source.country_code" in sql
    assert "count(distinct target_address_key)" in normalized
    assert "global_related_targets" in normalized
    assert "join \"mrf\".\"address_archive_v2\" as target" in normalized
    assert "target_strict_source_count < 2" in normalized
    assert "match_classification" in sql
    assert "'exact'" in sql
    assert "premise_only" not in normalized
    assert "similarity(" not in normalized
    assert "levenshtein" not in normalized
    fence_sql = address_evidence_alias_sql.evidence_input_stale_count_sql(
        schema="mrf"
    )
    assert "base_address_version" in fence_sql
    assert "alias-v1:g" in fence_sql


def test_evidence_migration_adds_auditable_exact_match_contract(monkeypatch):
    migration = _load_evidence_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "alias_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()
    normalized = " ".join(" ".join(sql.split()) for sql in recorder.statements)

    assert migration.revision == "20260816020000_address_evidence_alias"
    assert migration.down_revision == (
        "20260816010000_provider_directory_terminal_publication_guard"
    )
    assert "evidence_gated_address_match_v1" in normalized
    assert "match_rule varchar(64)" in normalized
    assert "match_classification varchar(16)" in normalized
    assert "evidence_npi bigint" in normalized
    assert "evidence_npi_count integer" in normalized
    assert "schema_version = 2" in normalized
    assert "generation = generation + 1" in normalized
    assert "num_nonnulls" in normalized
    assert "public_evidence_npi_valid(evidence_npi::text)" in normalized
    assert "match_classification = 'exact'" in normalized
    assert "addr_evidence_alias_match_v1" in normalized
    assert "candidate_confirmed_bare_unit" in normalized
    assert "unit_designator_punctuation" in normalized
    assert "candidate_confirmed_spaced_unit" in normalized
    assert "formatted_address_omits_descriptor" not in normalized
    assert "direction_relocation" in normalized
    assert "terminal_suffix_omission" in normalized
    assert "premise_only" not in normalized


def test_evidence_candidate_model_exposes_migrated_audit_columns():
    assert {
        "match_rule",
        "match_classification",
        "evidence_npi",
        "evidence_npi_count",
    } <= set(AddressAliasCandidateV1.__table__.columns.keys())
