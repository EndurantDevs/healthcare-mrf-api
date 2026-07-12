import json
import subprocess
import sys

from scripts.research import provider_directory_endpoint_acquisition_cli as acquisition_cli


def test_acquisition_cli_prefers_current_import_control_environment(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_CONTROL_URL", "http://current-import-control:8095")
    monkeypatch.setenv("HLTHPRT_IMPORT_CONTROL_TOKEN", "configured")
    monkeypatch.setenv("HP_IMPORT_CONTROL_URL", "http://legacy-import-control:8095")
    monkeypatch.setenv("HP_IMPORT_CONTROL_TOKEN", "configured")

    args = acquisition_cli.parse_acquisition_arguments([])

    assert args.control_url == "http://current-import-control:8095"
    assert args.token_env == "HLTHPRT_IMPORT_CONTROL_TOKEN"


def test_acquisition_cli_retains_legacy_import_control_environment(monkeypatch):
    monkeypatch.delenv("HLTHPRT_IMPORT_CONTROL_URL", raising=False)
    monkeypatch.delenv("HLTHPRT_IMPORT_CONTROL_TOKEN", raising=False)
    monkeypatch.setenv("HP_IMPORT_CONTROL_URL", "http://legacy-import-control:8095")
    monkeypatch.setenv("HP_IMPORT_CONTROL_TOKEN", "configured")

    args = acquisition_cli.parse_acquisition_arguments([])

    assert args.control_url == "http://legacy-import-control:8095"
    assert args.token_env == "HP_IMPORT_CONTROL_TOKEN"


def test_acquisition_cli_is_directly_executable():
    result = subprocess.run(
        [sys.executable, acquisition_cli.__file__, "--validate-only"],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["valid"] is True
    assert payload["entries"] > 0
