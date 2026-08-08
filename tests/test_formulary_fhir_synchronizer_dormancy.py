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
    assert 'intent="none"' in synchronizer_source


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


def test_reviewed_candidate_is_library_only_and_never_publishes():
    candidate_source = (
        ROOT / "process" / "formulary_fhir" / "reviewed_source.py"
    ).read_text(encoding="utf-8")
    assert 'intent="none"' in candidate_source
    assert "publish_dataset" not in candidate_source
    assert "publish_verified_seed" not in candidate_source
    synchronizer_source = (
        ROOT / "process" / "formulary_fhir" / "synchronizer.py"
    ).read_text(encoding="utf-8")
    assert "LIBRARY_ONLY_LAUNCH_MODE" in synchronizer_source
    assert "reviewed synchronization" in synchronizer_source
    runtime_paths = [
        ROOT / "main.py",
        ROOT / "process" / "__init__.py",
        ROOT / "process" / "formulary_fhir" / "__init__.py",
        ROOT / "api" / "control_imports.py",
        ROOT / "api" / "control_workers.py",
    ]
    runtime_paths.extend((ROOT / "api" / "endpoint").glob("*.py"))
    for runtime_path in runtime_paths:
        runtime_source = runtime_path.read_text(encoding="utf-8")
        assert "verify_reviewed_source_candidate" not in runtime_source
        assert "register_reviewed_source" not in runtime_source


def test_synthetic_canary_is_smoke_only_socket_free_and_never_publishes():
    canary_source = (
        ROOT / "process" / "formulary_fhir" / "synthetic_canary.py"
    ).read_text(encoding="utf-8")
    transport_source = (
        ROOT / "process" / "formulary_fhir" / "synthetic_canary_transport.py"
    ).read_text(encoding="utf-8")
    script_source = (
        ROOT / "scripts" / "smoke" / "formulary_fhir_synthetic_canary.py"
    ).read_text(encoding="utf-8")
    for source_text in (canary_source, transport_source, script_source):
        assert "publish_dataset" not in source_text
        assert "publish_verified_seed" not in source_text
    for network_constructor in (
        "aiohttp.ClientSession(",
        "aiohttp.TCPConnector(",
        "asyncio.open_connection(",
        "socket.socket(",
    ):
        assert network_constructor not in transport_source
    assert 'choices=("verify-seed",)' in script_source
    assert "intent=\"seed\"" in canary_source


def test_synthetic_canary_is_absent_from_all_runtime_entrypoints():
    runtime_paths = [
        ROOT / "main.py",
        ROOT / "process" / "__init__.py",
        ROOT / "api" / "control_imports.py",
        ROOT / "api" / "control_workers.py",
    ]
    runtime_paths.extend((ROOT / "api" / "endpoint").glob("*.py"))
    for runtime_path in runtime_paths:
        runtime_source = runtime_path.read_text(encoding="utf-8")
        assert "verify_synthetic_seed_candidate" not in runtime_source
        assert "formulary_fhir_synthetic_canary" not in runtime_source


def test_synthetic_canary_postgres_proof_and_fixtures_are_packaged():
    workflow_source = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    dockerfile_source = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    fixture_directory = ROOT / "scripts" / "smoke" / "fixtures" / "formulary_fhir"

    assert "tests/test_formulary_fhir_synthetic_canary_postgres.py" in workflow_source
    assert "COPY scripts/ /opt/scripts/" in dockerfile_source
    assert (fixture_directory / "coverage_plan.json").is_file()
    assert (fixture_directory / "medication_a.json").is_file()
    assert (fixture_directory / "medication_b.json").is_file()
    assert (fixture_directory / "canary_expected_v1.json").is_file()


def test_synthetic_seed_publisher_is_fixed_smoke_only_and_dormant():
    publisher_source = (
        ROOT
        / "process"
        / "formulary_fhir"
        / "synthetic_seed_publisher.py"
    ).read_text(encoding="utf-8")
    script_source = (
        ROOT
        / "scripts"
        / "smoke"
        / "formulary_fhir_synthetic_seed_publisher.py"
    ).read_text(encoding="utf-8")
    assert 'choices=("publish-seed",)' in script_source
    assert "publish_synthetic_seed" in script_source
    for forbidden_import in (
        "FHIRFormularyClient",
        "SyntheticCanaryClient",
        "synchronize_verified_dataset",
        "_run_verified_sync",
        "aiohttp",
        "socket",
        "publish_dataset",
    ):
        assert forbidden_import not in publisher_source
    for forbidden_selector in (
        "--source-id",
        "--run-id",
        "--dataset-id",
        "--cutoff",
        "--generation",
        "--intent",
    ):
        assert forbidden_selector not in script_source


def test_synthetic_seed_publisher_has_no_runtime_or_deployment_reachability():
    runtime_paths = [
        ROOT / "main.py",
        ROOT / "process" / "__init__.py",
        ROOT / "api" / "control_imports.py",
        ROOT / "api" / "control_workers.py",
    ]
    runtime_paths.extend((ROOT / "api" / "endpoint").glob("*.py"))
    for runtime_path in runtime_paths:
        runtime_source = runtime_path.read_text(encoding="utf-8")
        assert "publish_synthetic_seed" not in runtime_source
        assert (
            "HLTHPRT_FHIR_FORMULARY_SYNTHETIC_SEED_PUBLICATION_ENABLED"
            not in runtime_source
        )

    workflow_source = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    dockerfile_source = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert (
        "tests/test_formulary_fhir_synthetic_seed_publisher_postgres.py"
        in workflow_source
    )
    assert "COPY process/ /opt/process/" in dockerfile_source
    assert "COPY scripts/ /opt/scripts/" in dockerfile_source
