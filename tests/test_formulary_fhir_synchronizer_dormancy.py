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
        "process/__init__.py",
        "api/control_imports.py",
        "api/control_workers.py",
    ):
        runtime_source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "synchronize_verified_dataset" not in runtime_source


def test_manual_adapter_is_manage_only_and_never_publishes():
    main_source = (ROOT / "main.py").read_text(encoding="utf-8")
    worker_source = (
        ROOT / "process" / "formulary_fhir" / "manual_worker.py"
    ).read_text(encoding="utf-8")
    assert '@manage.command("verify-formulary-fhir")' in main_source
    assert "synchronize_verified_dataset_manually" in main_source
    assert "synchronize_verified_dataset(" not in main_source
    assert "publish_dataset" not in worker_source
    assert "publish_verified_seed" not in worker_source
    for relative_path in (
        "process/__init__.py",
        "api/control_imports.py",
        "api/control_workers.py",
    ):
        runtime_source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "verify-formulary-fhir" not in runtime_source
