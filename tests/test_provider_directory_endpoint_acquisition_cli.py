import json
import subprocess
import sys

from scripts.research import (
    provider_directory_endpoint_acquisition_cli as acquisition_cli,
)


def test_acquisition_cli_defaults_to_local_inputs():
    args = acquisition_cli.parse_acquisition_arguments([])

    assert args.manifest == acquisition_cli.harness.DEFAULT_MANIFEST
    assert args.entry == []
    assert args.operator_input is None
    assert args.output is None
    assert args.validate_only is False


def test_acquisition_cli_emits_selected_operator_plan(capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = manifest["entries"][0]

    exit_code = acquisition_cli.run_acquisition_cli(["--entry", entry["entry_id"]])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [item["entry_id"] for item in payload["entries"]] == [entry["entry_id"]]
    assert payload["entries"][0]["params"] == (
        acquisition_cli.harness.entry_params(manifest, entry)
    )


def test_acquisition_cli_verifies_local_operator_input(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = manifest["entries"][0]
    source_ids = list(entry["source_ids"])
    metrics = {
        "source_ids": source_ids,
        "source_import_sources_selected": len(source_ids),
        "source_import_groups_attempted": 1,
        "resource_fetch_completed_source_ids": {
            resource_type: source_ids for resource_type in entry["resources"]
        },
        "resource_fetch_stats": {
            resource_type: {
                "sources_completed": 1,
                "sources_bounded": 0,
                "sources_failed": 0,
            }
            for resource_type in entry["resources"]
        },
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
    }
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "results": {
                    entry["entry_id"]: {
                        "status": "succeeded",
                        "importer": manifest["importer"],
                        "params": acquisition_cli.harness.entry_params(manifest, entry),
                        "metrics": metrics,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "report.json"

    exit_code = acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(report_path),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["entries"][entry["entry_id"]]["status"] == "passed"
    assert json.loads(report_path.read_text(encoding="utf-8")) == payload


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
