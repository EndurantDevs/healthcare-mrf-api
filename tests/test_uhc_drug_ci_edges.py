# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_uhc_drug_postgres_lease_and_vertical_proofs_run_exactly_once() -> None:
    workflow = (REPOSITORY_ROOT / ".github/workflows/ci.yml").read_text(
        encoding="utf-8"
    )
    prepush = (REPOSITORY_ROOT / "scripts/ci/prepush").read_text(encoding="utf-8")
    proof_paths = (
        "tests/test_uhc_drug_acquisition_lease_postgres.py",
        "tests/test_uhc_drug_vertical_postgres.py",
    )

    assert 'scripts/ci/prepush postgres "${{ matrix.shard }}"' in workflow
    assert "run_core_postgres()" in prepush
    assert "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN=$dsn" in prepush
    for proof_path in proof_paths:
        assert prepush.count(proof_path) == 1
