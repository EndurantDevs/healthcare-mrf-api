# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native lifecycle proofs execute once through existing PostgreSQL entrypoints."""

NATIVE_SUPPORT_ENTRYPOINTS = {
    "tests.custom_import_retirement_postgres": "tests/test_custom_import_build_source_postgres.py",
    "tests.custom_import_retirement_inventory_postgres": "tests/test_custom_import_verification_root_ranges_postgres.py",
    "tests.custom_import_post_seal_audit_postgres": "tests/test_custom_import_verification_root_ranges_postgres.py",
}
