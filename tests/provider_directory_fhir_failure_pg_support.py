# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install the current rooted request-budget contract in disposable schemas."""

from pathlib import Path

from tests.formulary_fhir_twin_admission_pg_support import load_migration, run_migration


async def install_request_failure_budget(engine, schema_name):
    versions = Path(__file__).resolve().parents[1] / "alembic/versions"
    for filename in (
        "20260830090000_uhc_flex_retry_exhaustion.py",
        "20260830100000_provider_directory_rooted_partial_lineage.py",
    ):
        await run_migration(engine, load_migration(versions / filename, "fhir_budget_predecessor"), "upgrade")
    canonical = load_migration(versions / "20260810110000_ptg_wave_receipt_authority.py", "fhir_budget_canonical")
    canonical.install = lambda: canonical._install_receipt_verification_functions(schema_name)
    await run_migration(engine, canonical, "install")
    current = load_migration(versions / "20260909120000_fhir_request_failure_budget.py", "fhir_budget_current")
    await run_migration(engine, current, "upgrade")
    return current
