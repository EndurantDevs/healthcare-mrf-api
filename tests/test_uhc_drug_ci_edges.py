# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_uhc_drug_postgres_lease_and_vertical_proofs_run_exactly_once() -> None:
    workflow = (REPOSITORY_ROOT / ".github/workflows/ci.yml").read_text(
        encoding="utf-8"
    )
    migration_step = workflow.split(
        "      - name: Run migration adoption and storage integrity gates\n",
        1,
    )[1].split("      - name:", 1)[0]
    proof_paths = (
        "tests/test_uhc_drug_acquisition_lease_postgres.py",
        "tests/test_uhc_drug_vertical_postgres.py",
    )

    assert "if: matrix.shard == 'core'" in migration_step
    assert "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN:" in migration_step
    for proof_path in proof_paths:
        assert proof_path in migration_step
        assert workflow.count(proof_path) == 1
