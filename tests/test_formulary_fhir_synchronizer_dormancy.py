# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static reachability guard for the dormant formulary synchronizer."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_synchronizer_has_no_publication_or_partial_checkpoint_surface():
    synchronizer_source = (
        ROOT / "process" / "formulary_fhir" / "synchronizer.py"
    ).read_text(encoding="utf-8")
    assert "publish_dataset" not in synchronizer_source
    assert "publish_verified_seed" not in synchronizer_source
    assert "save_checkpoint" not in synchronizer_source


def test_synchronizer_is_absent_from_runtime_entrypoints():
    for relative_path in (
        "main.py",
        "process/__init__.py",
        "api/control_imports.py",
        "api/control_workers.py",
    ):
        runtime_source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "synchronize_verified_dataset" not in runtime_source
