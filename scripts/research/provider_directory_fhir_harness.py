# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Provider Directory FHIR self-harness.

This tool is intentionally outside the import runtime. It runs repeatable
Provider Directory FHIR parser, local CLI, and control-API smoke cases, then
writes ignored reports under reports/provider-directory-fhir/.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPORT_DIR = ROOT / "reports" / "provider-directory-fhir"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@dataclass(frozen=True)
class CaseResult:
    case_id: str
    kind: str
    status: str
    elapsed_seconds: float
    command: list[str] | None = None
    metrics: dict[str, Any] | None = None
    error: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "kind": self.kind,
            "status": self.status,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "command": self.command or [],
            "metrics": self.metrics or {},
            "error": self.error,
        }


def _fixture_resources() -> list[dict[str, Any]]:
    return [
        {
            "resourceType": "InsurancePlan",
            "id": "plan-1",
            "identifier": [{"system": "https://example.test/plan-id", "value": "H1234-001"}],
            "status": "active",
            "name": "Fixture Gold HMO",
            "network": [{"reference": "Organization/net-1"}],
        },
        {
            "resourceType": "Practitioner",
            "id": "prac-1",
            "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"}],
            "active": True,
            "name": [{"family": "Rivera", "given": ["Alex"]}],
        },
        {
            "resourceType": "Location",
            "id": "loc-1",
            "name": "Fixture Clinic",
            "telecom": [{"system": "phone", "value": "312-555-0100"}],
            "address": {
                "line": ["100 Main St", "Suite 2"],
                "city": "Chicago",
                "state": "IL",
                "postalCode": "60601-1234",
                "country": "US",
            },
        },
        {
            "resourceType": "PractitionerRole",
            "id": "role-1",
            "active": True,
            "practitioner": {"reference": "Practitioner/prac-1"},
            "organization": {"reference": "Organization/org-1"},
            "location": [{"reference": "Location/loc-1"}],
            "healthcareService": [{"reference": "HealthcareService/service-1"}],
            "insurancePlan": [{"reference": "InsurancePlan/plan-1"}],
            "endpoint": [{"reference": "Endpoint/endpoint-1"}],
        },
        {
            "resourceType": "HealthcareService",
            "id": "service-1",
            "active": True,
            "name": "Fixture Primary Care",
            "type": [{"coding": [{"system": "http://snomed.info/sct", "code": "408443003"}]}],
            "location": [{"reference": "Location/loc-1"}],
            "endpoint": [{"reference": "Endpoint/endpoint-1"}],
        },
        {
            "resourceType": "Endpoint",
            "id": "endpoint-1",
            "status": "active",
            "connectionType": {
                "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                "code": "hl7-fhir-rest",
                "display": "HL7 FHIR",
            },
            "name": "Fixture Provider Directory Endpoint",
            "address": "https://fixture.example/fhir",
            "payloadType": [{"coding": [{"system": "http://hl7.org/fhir/resource-types", "code": "Practitioner"}]}],
            "payloadMimeType": ["application/fhir+json"],
        },
    ]


def _run_fixture_case() -> CaseResult:
    started = time.monotonic()
    try:
        importer = importlib.import_module("process.provider_directory_fhir")

        source = {"source_id": "fixture_source", "api_base": "https://fixture.example/fhir"}
        capability = importer.parse_capability(
            source,
            {
                "resourceType": "CapabilityStatement",
                "fhirVersion": "4.0.1",
                "rest": [
                    {
                        "resource": [
                            {"type": "InsurancePlan", "searchParam": [{"name": "name"}]},
                            {"type": "PractitionerRole", "searchParam": [{"name": "practitioner"}]},
                            {"type": "Endpoint", "searchParam": [{"name": "connection-type"}]},
                        ]
                    }
                ],
            },
            {"status": "valid", "http_status": 200, "response_time_ms": 12, "url": "https://fixture.example/fhir/metadata"},
        )
        parsed = [importer.parse_fhir_resource("fixture_source", item) for item in _fixture_resources()]
        resource_counts: dict[str, int] = {}
        for model, _row in (item for item in parsed if item):
            resource_counts[model.__tablename__] = resource_counts.get(model.__tablename__, 0) + 1
        status = "succeeded" if capability["fhir_version"] == "4.0.1" and len(parsed) == 6 else "failed"
        return CaseResult(
            case_id="fixture-parser",
            kind="fixture",
            status=status,
            elapsed_seconds=time.monotonic() - started,
            metrics={"supported_resources": capability["supported_resources"], "resource_counts": resource_counts},
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return CaseResult(
            case_id="fixture-parser",
            kind="fixture",
            status="failed",
            elapsed_seconds=time.monotonic() - started,
            error=str(exc),
        )


def _validate_identifier(value: str, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not IDENTIFIER_RE.fullmatch(cleaned):
        raise ValueError(f"{label} must be a PostgreSQL identifier, got {value!r}")
    return cleaned


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def _sql_typing_schema() -> str:
    return "provider_directory_sql_typing_" + dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")


def _sql_typing_ddl(schema: str) -> str:
    return f"""
DROP SCHEMA IF EXISTS {schema} CASCADE;
CREATE SCHEMA {schema};
CREATE FUNCTION {schema}.addr_zip5_norm_v1(value text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(substring(regexp_replace(coalesce(value, ''), '[^0-9]', '', 'g') from 1 for 5), '')
$$;
CREATE FUNCTION {schema}.addr_state_code_v1(value text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT upper(NULLIF(btrim(value), ''))
$$;
CREATE FUNCTION {schema}.addr_city_norm_v1(value text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT lower(NULLIF(btrim(value), ''))
$$;
CREATE FUNCTION {schema}.addr_key_v1(a text, b text, c text, d text, e text, f text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT md5(coalesce(a,'') || '|' || coalesce(b,'') || '|' || coalesce(c,'') || '|' || coalesce(d,'') || '|' || coalesce(e,'') || '|' || coalesce(f,''))
$$;
CREATE TABLE {schema}.geo_zip_lookup (
    zip_code text PRIMARY KEY,
    state text
);
CREATE TABLE {schema}.provider_directory_location (
    source_id varchar NOT NULL,
    resource_id varchar NOT NULL,
    first_line text,
    second_line text,
    city_name text,
    postal_code text,
    country_code text,
    state_name text,
    state_code text,
    address_key text,
    zip5 varchar,
    city_norm varchar,
    last_seen_run_id varchar,
    updated_at timestamp,
    PRIMARY KEY (source_id, resource_id)
);
CREATE TABLE {schema}.provider_directory_import_seen_stage_test (
    run_id varchar,
    resource_type varchar,
    source_id varchar,
    resource_id varchar
);
"""


async def _run_sql_typing_case_async(args: argparse.Namespace) -> CaseResult:
    started = time.monotonic()
    schema = _validate_identifier(args.sql_schema or _sql_typing_schema(), label="sql schema")
    user = urllib.parse.quote(str(args.db_user), safe="")
    password = urllib.parse.quote(str(args.db_password), safe="")
    auth = f"{user}:{password}" if password else user
    asyncpg_dsn = f"postgresql://{auth}@{args.db_host}:{args.db_port}/{args.db_database}"
    sqlalchemy_url = f"postgresql+asyncpg://{auth}@{args.db_host}:{args.db_port}/{args.db_database}"
    engine = None
    raw_execute = None
    schema_created = False
    status = "succeeded"
    error: str | None = None
    try:
        import asyncpg  # pylint: disable=import-outside-toplevel
        from sqlalchemy import text  # pylint: disable=import-outside-toplevel
        from sqlalchemy.ext.asyncio import create_async_engine  # pylint: disable=import-outside-toplevel

        importer = importlib.import_module("process.provider_directory_fhir")

        async def raw(sql: str) -> None:
            conn = await asyncpg.connect(asyncpg_dsn)
            try:
                await conn.execute(sql)
            finally:
                await conn.close()

        raw_execute = raw
        await raw(_sql_typing_ddl(schema))
        schema_created = True
        engine = create_async_engine(sqlalchemy_url)
        async with engine.begin() as conn:
            params = {
                "run_id": "run_sql_typing",
                "after_source_id": None,
                "after_resource_id": None,
                "batch_size": 10,
            }
            sql = importer.provider_directory_location_address_key_batch_sql(
                schema,
                run_id="run_sql_typing",
            )
            row = (await conn.execute(text(sql), params)).fetchone()
            assert row is not None and row.candidate_rows == 0

            seen_sql = importer.provider_directory_location_address_key_batch_sql(
                schema,
                run_id="run_sql_typing",
                seen_table="provider_directory_import_seen_stage_test",
            )
            seen_row = (await conn.execute(text(seen_sql), params)).fetchone()
            assert seen_row is not None and seen_row.candidate_rows == 0
    except Exception as exc:  # pylint: disable=broad-exception-caught
        status = "failed"
        error = str(exc)
    finally:
        if engine is not None:
            await engine.dispose()
        if schema_created and raw_execute is not None and not args.keep_sql_schema:
            try:
                await raw_execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception as exc:  # pylint: disable=broad-exception-caught
                cleanup_error = f"cleanup failed: {exc}"
                status = "failed"
                error = f"{error}; {cleanup_error}" if error else cleanup_error
    return CaseResult(
        case_id="sql-typing",
        kind="sql",
        status=status,
        elapsed_seconds=time.monotonic() - started,
        metrics={"schema": schema, "database": args.db_database},
        error=error,
    )


def _run_sql_typing_case(args: argparse.Namespace) -> CaseResult:
    return asyncio.run(_run_sql_typing_case_async(args))


def _parse_import_metrics(output: str) -> dict[str, Any]:
    json_prefix = "PROVIDER_DIRECTORY_FHIR_IMPORT_DONE\t"
    for line in reversed(output.splitlines()):
        if line.startswith(json_prefix):
            try:
                parsed = json.loads(line[len(json_prefix) :])
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
    prefix = "Provider Directory FHIR import done:"
    for line in reversed(output.splitlines()):
        if not line.startswith(prefix):
            continue
        metrics: dict[str, Any] = {}
        for token in line[len(prefix) :].strip().split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            try:
                metrics[key] = int(value)
            except ValueError:
                metrics[key] = value
        return metrics
    return {}


def _run_cli_case(case_id: str, args: argparse.Namespace) -> CaseResult:
    started = time.monotonic()
    python = args.python or sys.executable
    command = [
        python,
        "main.py",
        "start",
        "provider-directory-fhir",
        "--test",
        "--seed-db-path",
        args.seed_db_path,
        "--limit",
        str(args.limit),
        "--timeout",
        str(args.timeout),
        "--concurrency",
        str(args.concurrency),
    ]
    if args.retest_results_path:
        command.extend(["--retest-results-path", args.retest_results_path])
    if args.retest_results_url:
        command.extend(["--retest-results-url", args.retest_results_url])
    if args.credential_config_file:
        command.extend(["--credential-config-file", args.credential_config_file])
    if args.source_query:
        command.extend(["--source-query", args.source_query])
    if args.import_resources:
        command.append("--import-resources")
        if args.full_refresh:
            command.append("--full-refresh")
        if args.stale_cleanup is True:
            command.append("--stale-cleanup")
        elif args.stale_cleanup is False:
            command.append("--no-stale-cleanup")
        if args.resource_limit is not None:
            command.extend(["--resource-limit", str(args.resource_limit)])
        if args.linked_resource_limit is not None:
            command.extend(["--linked-resource-limit", str(args.linked_resource_limit)])
        if args.linked_resource_deadline_seconds is not None:
            command.extend(["--linked-resource-deadline-seconds", str(args.linked_resource_deadline_seconds)])
        if args.page_limit is not None:
            command.extend(["--page-limit", str(args.page_limit)])
        if args.page_count is not None:
            command.extend(["--page-count", str(args.page_count)])
        if args.stream_batch_size is not None:
            command.extend(["--stream-batch-size", str(args.stream_batch_size)])
        if args.source_concurrency is not None:
            command.extend(["--source-concurrency", str(args.source_concurrency)])
        if args.publish_artifacts is True:
            command.append("--publish-artifacts")
        elif args.publish_artifacts is False:
            command.append("--no-publish-artifacts")
        if args.resources:
            command.extend(["--resources", args.resources])
        if args.include_credentialed:
            command.append("--include-credentialed")
    if args.seed_only:
        command.append("--seed-only")
    if args.no_probe:
        command.append("--no-probe")
    env = os.environ.copy()
    env.update({key: str(value) for key, value in (args.env or {}).items()})
    try:
        proc = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=args.command_timeout,
            check=False,
        )
        elapsed = time.monotonic() - started
        metrics = _parse_import_metrics(proc.stdout)
        if proc.returncode == 0:
            status = "succeeded"
            error = None
        else:
            status = "failed"
            error = proc.stdout[-4000:]
        metrics["returncode"] = proc.returncode
        return CaseResult(case_id=case_id, kind="cli", status=status, elapsed_seconds=elapsed, command=command, metrics=metrics, error=error)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return CaseResult(case_id=case_id, kind="cli", status="failed", elapsed_seconds=time.monotonic() - started, command=command, error=str(exc))


def _run_control_case(case_id: str, args: argparse.Namespace) -> CaseResult:
    started = time.monotonic()
    control_url = str(args.control_url or "").rstrip("/")
    token = args.control_token or os.getenv("HLTHPRT_CONTROL_API_TOKEN")
    if not control_url or not token:
        return CaseResult(case_id=case_id, kind="control", status="skipped", elapsed_seconds=0, error="control URL/token not supplied")
    payload = {
        "importer": "provider-directory-fhir",
        "idempotency_key": f"provider-directory-fhir-harness-{dt.datetime.now(dt.UTC).strftime('%Y%m%d%H%M%S')}",
        "params": {
            "import_resources": True,
            "full_refresh": args.full_refresh,
            "concurrency": args.concurrency,
            "timeout": args.timeout,
        },
    }
    if args.stale_cleanup is not None:
        payload["params"]["stale_cleanup"] = args.stale_cleanup
    if args.publish_artifacts is not None:
        payload["params"]["publish_artifacts"] = args.publish_artifacts
    for key in (
        "resource_limit",
        "linked_resource_limit",
        "linked_resource_deadline_seconds",
        "page_limit",
        "page_count",
        "stream_batch_size",
        "source_concurrency",
    ):
        value = getattr(args, key)
        if value is not None:
            payload["params"][key] = value
    if args.limit:
        payload["params"]["limit"] = args.limit
    if args.source_query:
        payload["params"]["source_query"] = args.source_query
    if args.retest_results_path:
        payload["params"]["retest_results_path"] = args.retest_results_path
    if args.retest_results_url:
        payload["params"]["retest_results_url"] = args.retest_results_url
    request = urllib.request.Request(
        f"{control_url}/imports",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            body = response.read(1024 * 1024)
        parsed = json.loads(body.decode("utf-8"))
        return CaseResult(
            case_id=case_id,
            kind="control",
            status="queued" if 200 <= response.status < 300 else "failed",
            elapsed_seconds=time.monotonic() - started,
            metrics={"http_status": response.status, "response": parsed},
        )
    except urllib.error.HTTPError as exc:
        body = exc.read(1024 * 1024).decode("utf-8", errors="replace")
        return CaseResult(
            case_id=case_id,
            kind="control",
            status="failed",
            elapsed_seconds=time.monotonic() - started,
            metrics={"http_status": exc.code},
            error=body,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return CaseResult(case_id=case_id, kind="control", status="failed", elapsed_seconds=time.monotonic() - started, error=str(exc))


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Provider Directory FHIR Harness Report",
        "",
        f"- generated_at: `{report['generated_at']}`",
        f"- overall_status: `{report['overall_status']}`",
        "",
        "| Case | Kind | Status | Elapsed | Key metrics |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for result in report["results"]:
        metrics = result.get("metrics") or {}
        key_metrics = ", ".join(f"{key}={value}" for key, value in metrics.items() if key != "response")[:200]
        lines.append(
            f"| {result['case_id']} | {result['kind']} | {result['status']} | {result['elapsed_seconds']} | {key_metrics} |"
        )
    lines.extend(["", "## Raw Results", "", "```json", json.dumps(report["results"], indent=2, sort_keys=True), "```", ""])
    return "\n".join(lines)


def write_report(report: dict[str, Any], output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_root / "report.md").write_text(render_markdown(report), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    results = [_run_fixture_case()]
    if args.sql_typing:
        results.append(_run_sql_typing_case(args))
    if args.local_cli:
        if not args.seed_db_path:
            results.append(CaseResult("local-cli", "cli", "skipped", 0, error="--seed-db-path is required for --local-cli"))
        else:
            results.append(_run_cli_case("local-cli", args))
    if args.control_url:
        results.append(_run_control_case("control-api", args))
    serialized = [result.to_json() for result in results]
    overall_status = "failed" if any(item["status"] == "failed" for item in serialized) else "succeeded"
    report = {
        "generated_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "overall_status": overall_status,
        "results": serialized,
    }
    output_root = (
        Path(args.output_dir)
        if args.output_dir
        else DEFAULT_REPORT_DIR / f"run-{dt.datetime.now(dt.UTC).strftime('%Y%m%d%H%M%S')}"
    )
    write_report(report, output_root)
    report["output_dir"] = str(output_root)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Provider Directory FHIR self-harness cases.")
    parser.add_argument("--output-dir", help="Report output directory. Defaults to reports/provider-directory-fhir/run-<timestamp>.")
    parser.add_argument("--python", help="Python executable for local CLI case. Defaults to current interpreter.")
    parser.add_argument("--sql-typing", action="store_true", help="Run disposable DB-backed SQL typing checks through SQLAlchemy asyncpg.")
    parser.add_argument("--sql-schema", default=os.getenv("HLTHPRT_PROVIDER_DIRECTORY_SQL_HARNESS_SCHEMA") or _sql_typing_schema())
    parser.add_argument("--keep-sql-schema", action="store_true", help="Keep the disposable SQL typing schema after the run.")
    parser.add_argument("--db-host", default=os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1")
    parser.add_argument("--db-port", type=int, default=_env_int("HLTHPRT_DB_PORT", 5440))
    parser.add_argument("--db-database", default=os.getenv("HLTHPRT_DB_DATABASE") or "healthporta_test")
    parser.add_argument("--db-user", default=os.getenv("HLTHPRT_DB_USER") or os.getenv("USER") or "nick")
    parser.add_argument("--db-password", default=os.getenv("HLTHPRT_DB_PASSWORD") or "")
    parser.add_argument("--local-cli", action="store_true", help="Run the local DB-backed CLI importer case.")
    parser.add_argument("--control-url", help="Control API base URL, for example https://app-dev.healthporta.com/control/v1.")
    parser.add_argument("--control-token", help="Control API bearer token. Defaults to HLTHPRT_CONTROL_API_TOKEN.")
    parser.add_argument("--seed-db-path", help="SQLite seed database path for local CLI runs.")
    parser.add_argument("--retest-results-path", help="Optional provider-directory-db retest_results.json path for local/control runs.")
    parser.add_argument("--retest-results-url", help="Optional provider-directory-db retest_results.json URL for local/control runs.")
    parser.add_argument("--credential-config-file", help="Optional Provider Directory credentials JSON file for local CLI runs.")
    parser.add_argument("--limit", type=int, default=10, help="Source limit for bounded runs.")
    parser.add_argument("--source-query", help="Optional source org/plan filter.")
    parser.add_argument("--seed-only", action="store_true", help="Local CLI: only load source rows.")
    parser.add_argument("--no-probe", action="store_true", help="Local CLI: skip metadata probing.")
    parser.add_argument("--import-resources", action="store_true", help="Local CLI: fetch FHIR resources.")
    parser.add_argument("--full-refresh", action="store_true", help="Use full-refresh importer defaults instead of sample defaults.")
    stale_group = parser.add_mutually_exclusive_group()
    stale_group.add_argument("--stale-cleanup", dest="stale_cleanup", action="store_true", help="Explicitly enable stale-row cleanup.")
    stale_group.add_argument("--no-stale-cleanup", dest="stale_cleanup", action="store_false", help="Explicitly disable stale-row cleanup.")
    parser.set_defaults(stale_cleanup=None)
    publish_group = parser.add_mutually_exclusive_group()
    publish_group.add_argument("--publish-artifacts", dest="publish_artifacts", action="store_true", help="Refresh derived Provider Directory address/search artifacts.")
    publish_group.add_argument("--no-publish-artifacts", dest="publish_artifacts", action="store_false", help="Skip derived Provider Directory artifact refresh.")
    parser.set_defaults(publish_artifacts=None)
    parser.add_argument("--include-credentialed", action="store_true", help="Local CLI: include sources not marked open/none.")
    parser.add_argument("--resources", help="Comma-separated resource list for local CLI imports.")
    parser.add_argument("--resource-limit", type=int, default=None, help="Rows per source/resource; 0 means unbounded.")
    parser.add_argument("--linked-resource-limit", type=int, default=None, help="Referenced FHIR resources per source.")
    parser.add_argument("--linked-resource-deadline-seconds", type=int, default=None, help="Seconds to spend fetching linked resources per source; 0 disables the deadline.")
    parser.add_argument("--page-limit", type=int, default=None, help="FHIR pages per source/resource; 0 means unbounded.")
    parser.add_argument("--page-count", type=int, default=None, help="FHIR _count page size.")
    parser.add_argument("--stream-batch-size", type=int, default=None, help="Rows per streaming upsert batch; 0 disables streaming.")
    parser.add_argument("--source-concurrency", type=int, default=None, help="Concurrent source resource imports.")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent metadata probes.")
    parser.add_argument("--timeout", type=int, default=15, help="Per-request timeout.")
    parser.add_argument("--command-timeout", type=int, default=900, help="Local CLI command timeout.")
    parser.add_argument("--env", action="append", default=[], help="Environment override KEY=VALUE for local CLI; repeatable.")
    args = parser.parse_args(argv)
    env: dict[str, str] = {}
    for item in args.env:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise SystemExit(f"invalid --env value: {item}")
        env[key] = value
    args.env = env
    return args


def main(argv: list[str] | None = None) -> int:
    report = run(parse_args(argv))
    print(json.dumps({"overall_status": report["overall_status"], "output_dir": report["output_dir"]}, sort_keys=True))
    return 0 if report["overall_status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
