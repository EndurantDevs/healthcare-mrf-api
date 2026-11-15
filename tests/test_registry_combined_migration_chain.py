# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog and compact membership migrations have one ordered successor chain."""

from pathlib import Path

from alembic.script import ScriptDirectory


def test_catalog_membership_and_inventory_extend_the_existing_target_head():
    scripts = ScriptDirectory(str(Path(__file__).resolve().parents[1] / "alembic"))
    chain = (
        "20261025020000_company_affiliation_join",
        "20261028010000_registry_fhir_catalog",
        "20261028020000_registry_membership_refresh",
        "20261029010000_registry_source_inventory",
        "20261102010000_custom_import_snapshot_retirement",
        "20261102030000_custom_import_build_scratch",
        "20261102040000_custom_import_post_seal_audit",
        "20261104010000_custom_import_unlogged_landing",
        "20261107010000_custom_import_child_reference_aggregate",
        "20261108010000_custom_import_family_child_reference_aggregate",
        "20261111010000_code_sets_publication_idempotency",
        "20261113010000_address_canonical_space_normalization",
        "20261115010000_ms_drg_companion_tables",
        "20261114010000_custom_import_snapshot_names",
        "20261114020000_custom_import_snapshot_metadata",
        "20261114130000_named_preparation_catalog",
        "20261114160000_npi_named_generation",
        "20261114170000_source_profile_named_attachment",
        "20261114180000_reference_family_named_generation",
        "20261114190000_cms_native_input_named_revision",
        "20261115100000_prescription_rollup_named_fingerprint",
        "20261115110000_network_retained_read_grant_ledger",
        "20261115120000_named_output_recovery_document",
        "20261116090000_profile_capacity_logical_names",
    )
    assert scripts.get_heads() == [chain[-1]]
    for predecessor, successor in zip(chain, chain[1:]):
        assert scripts.get_revision(successor).down_revision == predecessor
