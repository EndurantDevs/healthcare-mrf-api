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
            "meta": {"versionId": "3", "lastUpdated": "2026-07-13T12:00:00Z"},
            "identifier": [
                {"system": "https://example.test/product-id", "value": "product-1"},
                {"system": "https://example.test/issuer-id", "value": "issuer-1"},
            ],
            "status": "active",
            "name": "Fixture Gold HMO",
            "network": [{"reference": "Organization/net-1"}],
            "plan": [
                {
                    "identifier": [{"value": "H1234-001"}],
                    "type": {"coding": [{"code": "gold"}]},
                    "network": [{"reference": "Organization/net-1"}],
                }
            ],
            "coverage": [
                {
                    "type": {"coding": [{"code": "medical"}]},
                    "network": [{"reference": "Organization/net-1"}],
                    "benefit": [{"type": {"coding": [{"code": "pcp"}]}}],
                }
            ],
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
            "availableTime": [{"daysOfWeek": ["mon"], "availableStartTime": "09:00:00"}],
            "notAvailable": [{"description": "Fixture holiday"}],
            "availabilityExceptions": "Call for holiday hours",
            "extension": [
                {
                    "url": (
                        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                        "StructureDefinition/newpatients"
                    ),
                    "extension": [
                        {
                            "url": "acceptingPatients",
                            "valueCodeableConcept": {"coding": [{"code": "newpt"}]},
                        }
                    ],
                },
                {
                    "url": "https://healthporta.com/fhir/provider-directory/telehealth",
                    "valueBoolean": True,
                },
            ],
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


def _parse_fixture_resources(importer: Any) -> list[tuple[type, dict[str, Any]] | None]:
    parsed_resources = []
    for resource in _fixture_resources():
        resource_type = resource["resourceType"]
        resource_id = resource["id"]
        parsed_resources.append(
            importer.parse_fhir_resource(
                "fixture_source",
                resource,
                resource_url=f"https://fixture.example/fhir/{resource_type}/{resource_id}",
                acquisition=importer.FHIRAcquisitionContext(
                    self_url=f"https://fixture.example/fhir/{resource_type}/{resource_id}",
                    fetch_url=(
                        f"https://fixture.example/fhir/{resource_type}"
                        "?_page_token=fixture-secret"
                    ),
                    fetch_mode="rest_bundle",
                ),
                run_id="fixture-run",
            )
        )
    return parsed_resources


def _fixture_completeness_checks(
    parsed_resources: list[tuple[type, dict[str, Any]] | None],
) -> dict[str, bool]:
    rows_by_table = {
        model.__tablename__: row
        for parsed_resource in parsed_resources
        if parsed_resource is not None
        for model, row in [parsed_resource]
    }
    plan_row = rows_by_table.get("provider_directory_insurance_plan", {})
    role_row = rows_by_table.get("provider_directory_practitioner_role", {})
    return {
        "plan_products": len(plan_row.get("product_identifiers") or []) == 2,
        "plan_backbones": len(plan_row.get("plan_backbones") or []) == 1,
        "plan_coverage": len(plan_row.get("coverage") or []) == 1,
        "role_availability": len(role_row.get("available_time") or []) == 1,
        "role_new_patients": role_row.get("new_patient_acceptance") == [{"code": "newpt"}],
        "role_telehealth": role_row.get("telehealth") == [{"supported": True}],
        "meta": plan_row.get("fhir_meta", {}).get("versionId") == "3",
        "provenance": plan_row.get("fhir_fetch_url") == "https://fixture.example/fhir/InsurancePlan",
    }


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
        parsed = _parse_fixture_resources(importer)
        resource_counts: dict[str, int] = {}
        for model, _row in (item for item in parsed if item):
            resource_counts[model.__tablename__] = resource_counts.get(model.__tablename__, 0) + 1
        completeness_checks = _fixture_completeness_checks(parsed)
        is_complete = (
            capability["fhir_version"] == "4.0.1"
            and len(parsed) == 6
            and all(completeness_checks.values())
        )
        status = "succeeded" if is_complete else "failed"
        return CaseResult(
            case_id="fixture-parser",
            kind="fixture",
            status=status,
            elapsed_seconds=time.monotonic() - started,
            metrics={
                "supported_resources": capability["supported_resources"],
                "resource_counts": resource_counts,
                "completeness_checks": completeness_checks,
            },
        )
    except Exception as exc:
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
    state text,
    city text
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
        import asyncpg
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

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
    except Exception as exc:
        status = "failed"
        error = str(exc)
    finally:
        if engine is not None:
            await engine.dispose()
        if schema_created and raw_execute is not None and not args.keep_sql_schema:
            try:
                await raw_execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except Exception as exc:
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
        "--limit",
        str(args.limit),
        "--timeout",
        str(args.timeout),
        "--concurrency",
        str(args.concurrency),
    ]
    if args.cli_test_mode:
        command.append("--test")
    if args.seed_db_path:
        command.extend(["--seed-db-path", args.seed_db_path])
    elif args.seed_db_url:
        command.extend(["--seed-db-url", args.seed_db_url])
    if args.retest_results_path:
        command.extend(["--retest-results-path", args.retest_results_path])
    if args.retest_results_url:
        command.extend(["--retest-results-url", args.retest_results_url])
    if args.credential_config_file:
        command.extend(["--credential-config-file", args.credential_config_file])
    if args.source_query:
        command.extend(["--source-query", args.source_query])
    if args.refresh_preset:
        command.extend(["--refresh-preset", args.refresh_preset])
    if args.include_supplemental_catalogs is True:
        command.append("--include-supplemental-catalogs")
    elif args.include_supplemental_catalogs is False:
        command.append("--no-include-supplemental-catalogs")
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
        if args.resource_deadline_seconds is not None:
            command.extend(["--resource-deadline-seconds", str(args.resource_deadline_seconds)])
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
    except Exception as exc:
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
    if args.refresh_preset:
        payload["params"]["refresh_preset"] = args.refresh_preset
    if args.include_supplemental_catalogs is not None:
        payload["params"]["include_supplemental_catalogs"] = args.include_supplemental_catalogs
    if args.stale_cleanup is not None:
        payload["params"]["stale_cleanup"] = args.stale_cleanup
    if args.publish_artifacts is not None:
        payload["params"]["publish_artifacts"] = args.publish_artifacts
    for key in (
        "resource_limit",
        "resource_deadline_seconds",
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
    except Exception as exc:
        return CaseResult(case_id=case_id, kind="control", status="failed", elapsed_seconds=time.monotonic() - started, error=str(exc))


def _coverage_audit_metrics(report: dict[str, Any]) -> dict[str, Any]:
    source_summary = report.get("source_summary") if isinstance(report, dict) else {}
    resource_summary = report.get("resource_summary") if isinstance(report, dict) else {}
    serving_readiness = report.get("serving_readiness") if isinstance(report, dict) else {}
    readiness_checks = [
        check
        for check in (serving_readiness or {}).get("checks") or []
        if isinstance(check, dict)
    ]
    required_check_names = [
        str(check.get("name") or "")
        for check in readiness_checks
        if check.get("required") is True and check.get("name")
    ]
    failed_required_check_names = [
        str(check.get("name") or "")
        for check in readiness_checks
        if check.get("required") is True and check.get("status") != "pass" and check.get("name")
    ]
    phone_readiness_metrics = next(
        (
            check.get("metrics") or {}
            for check in readiness_checks
            if check.get("name") == "searchable_phone_overlay"
        ),
        {},
    )
    resource_row_count_by_table = {
        table_name: int((table_summary or {}).get("row_count") or 0)
        for table_name, table_summary in sorted((resource_summary or {}).items())
        if isinstance(table_summary, dict)
    }
    return {
        "source_count": int((source_summary or {}).get("source_count") or 0),
        "live_valid_count": int((source_summary or {}).get("live_valid_count") or 0),
        "live_auth_required_count": int((source_summary or {}).get("live_auth_required_count") or 0),
        "never_probed_count": int((source_summary or {}).get("never_probed_count") or 0),
        "resource_rows": resource_row_count_by_table,
        "serving_readiness_status": (serving_readiness or {}).get("status"),
        "serving_required_fail_count": int((serving_readiness or {}).get("required_fail_count") or 0),
        "serving_required_checks": required_check_names,
        "serving_failed_required_checks": failed_required_check_names,
        "serving_phone_rows": int(phone_readiness_metrics.get("provider_directory_phone_rows") or 0),
        "serving_phone_pct": phone_readiness_metrics.get("provider_directory_phone_pct"),
    }


def _effective_db_value(args: argparse.Namespace, attribute_name: str, env_name: str) -> Any:
    env_map = getattr(args, "env", {}) or {}
    if env_name in env_map:
        return env_map[env_name]
    return getattr(args, attribute_name)


def _effective_db_schema(args: argparse.Namespace) -> str:
    return str(_effective_db_value(args, "db_schema", "HLTHPRT_DB_SCHEMA"))


def _coverage_audit_command(args: argparse.Namespace) -> list[str]:
    """Build the coverage audit subprocess command."""
    python = args.python or sys.executable
    audit_command_parts = [
        python,
        "scripts/research/provider_directory_coverage_audit.py",
        "--host",
        str(_effective_db_value(args, "db_host", "HLTHPRT_DB_HOST")),
        "--port",
        str(_effective_db_value(args, "db_port", "HLTHPRT_DB_PORT")),
        "--database",
        str(_effective_db_value(args, "db_database", "HLTHPRT_DB_DATABASE")),
        "--user",
        str(_effective_db_value(args, "db_user", "HLTHPRT_DB_USER")),
        "--password",
        str(_effective_db_value(args, "db_password", "HLTHPRT_DB_PASSWORD")),
        "--schema",
        _effective_db_schema(args),
        "--format",
        "json",
    ]
    if args.coverage_audit_pod_safe:
        audit_command_parts.append("--pod-safe")
    if args.coverage_audit_fast_serving_readiness:
        audit_command_parts.append("--fast-serving-readiness")
    if args.coverage_audit_require_serving_ready:
        audit_command_parts.append("--require-serving-ready")
    if args.retest_results_path:
        audit_command_parts.extend(["--retest-results-path", args.retest_results_path])
    if args.coverage_audit_statement_timeout_ms:
        audit_command_parts.extend(["--statement-timeout-ms", str(args.coverage_audit_statement_timeout_ms)])
    return audit_command_parts


def _parse_coverage_audit_output(output: str, returncode: int) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return compact audit metrics plus the decoded audit report."""
    audit_metrics_map: dict[str, Any] = {"returncode": returncode}
    try:
        audit_report_map = json.loads(output)
    except json.JSONDecodeError:
        return audit_metrics_map, {}
    if isinstance(audit_report_map, dict):
        audit_metrics_map.update(_coverage_audit_metrics(audit_report_map))
        return audit_metrics_map, audit_report_map
    return audit_metrics_map, {}


def _run_coverage_audit_case(args: argparse.Namespace) -> CaseResult:
    """Run the pod-safe coverage audit as a harness report case."""
    started = time.monotonic()
    audit_command_parts = _coverage_audit_command(args)
    try:
        proc = subprocess.run(
            audit_command_parts,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=args.coverage_audit_timeout,
            check=False,
        )
        elapsed = time.monotonic() - started
        audit_metrics_map, audit_report_map = _parse_coverage_audit_output(proc.stdout, proc.returncode)
        if proc.returncode == 0 and audit_report_map:
            return CaseResult(
                case_id="coverage-audit",
                kind="audit",
                status="succeeded",
                elapsed_seconds=elapsed,
                command=audit_command_parts,
                metrics=audit_metrics_map,
            )
        return CaseResult(
            case_id="coverage-audit",
            kind="audit",
            status="failed",
            elapsed_seconds=elapsed,
            command=audit_command_parts,
            metrics=audit_metrics_map,
            error=proc.stdout[-4000:],
        )
    except Exception as exc:
        return CaseResult(
            case_id="coverage-audit",
            kind="audit",
            status="failed",
            elapsed_seconds=time.monotonic() - started,
            command=audit_command_parts,
            error=str(exc),
        )


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
    for case_result in report["results"]:
        metrics = case_result.get("metrics") or {}
        key_metrics = ", ".join(
            f"{metric_name}={metric_value}"
            for metric_name, metric_value in metrics.items()
            if metric_name != "response"
        )[:200]
        lines.append(
            f"| {case_result['case_id']} | {case_result['kind']} | {case_result['status']} | {case_result['elapsed_seconds']} | {key_metrics} |"
        )
    audit_results = [
        case_result
        for case_result in report["results"]
        if case_result.get("case_id") == "coverage-audit" and isinstance(case_result.get("metrics"), dict)
    ]
    if audit_results:
        lines.extend(["", "## Coverage Audit Readiness", ""])
        for audit_result in audit_results:
            metrics = audit_result.get("metrics") or {}
            failed_checks = metrics.get("serving_failed_required_checks") or []
            failed_text = ", ".join(f"`{check_name}`" for check_name in failed_checks) or "`none`"
            lines.extend(
                [
                    f"- status: `{metrics.get('serving_readiness_status')}`",
                    f"- required failures: `{metrics.get('serving_required_fail_count')}`",
                    f"- failed required checks: {failed_text}",
                    f"- phone rows: `{metrics.get('serving_phone_rows')}` ({metrics.get('serving_phone_pct')}%)",
                ]
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
        results.append(_run_cli_case("local-cli", args))
    if getattr(args, "coverage_audit", False):
        results.append(_run_coverage_audit_case(args))
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


def _add_harness_case_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dir", help="Report output directory. Defaults to reports/provider-directory-fhir/run-<timestamp>.")
    parser.add_argument("--python", help="Python executable for local CLI case. Defaults to current interpreter.")
    parser.add_argument("--sql-typing", action="store_true", help="Run disposable DB-backed SQL typing checks through SQLAlchemy asyncpg.")
    parser.add_argument("--sql-schema", default=os.getenv("HLTHPRT_PROVIDER_DIRECTORY_SQL_HARNESS_SCHEMA") or _sql_typing_schema())
    parser.add_argument("--keep-sql-schema", action="store_true", help="Keep the disposable SQL typing schema after the run.")
    parser.add_argument("--local-cli", action="store_true", help="Run the local DB-backed CLI importer case.")
    parser.add_argument("--control-url", help="Control API base URL, for example https://app-dev.healthporta.com/control/v1.")
    parser.add_argument("--control-token", help="Control API bearer token. Defaults to HLTHPRT_CONTROL_API_TOKEN.")
    parser.add_argument("--coverage-audit", action="store_true", help="Run a pod-safe Provider Directory coverage audit against the selected DB.")
    parser.add_argument("--coverage-audit-timeout", type=int, default=300, help="Coverage audit subprocess timeout in seconds.")
    parser.add_argument("--coverage-audit-statement-timeout-ms", type=int, default=0, help="Optional PostgreSQL statement_timeout for coverage audit queries.")
    parser.add_argument("--coverage-audit-full", dest="coverage_audit_pod_safe", action="store_false", help="Run the full coverage audit instead of pod-safe source/resource checks.")
    parser.add_argument("--coverage-audit-require-serving-ready", action="store_true", help="Fail the harness audit case unless serving_readiness is ready.")
    parser.add_argument("--coverage-audit-fast-serving-readiness", action="store_true", help="Use bounded serving-readiness existence probes in full audit mode.")
    parser.set_defaults(coverage_audit_pod_safe=True)
    cli_test_group = parser.add_mutually_exclusive_group()
    cli_test_group.add_argument("--cli-test-mode", dest="cli_test_mode", action="store_true", help="Pass --test to local CLI imports for bounded smoke defaults.")
    cli_test_group.add_argument("--no-cli-test-mode", dest="cli_test_mode", action="store_false", help="Run the local CLI without --test, useful for scheduled monthly command-shape checks.")
    parser.set_defaults(cli_test_mode=True)


def _add_database_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db-host", default=os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1")
    parser.add_argument("--db-port", type=int, default=_env_int("HLTHPRT_DB_PORT", 5440))
    parser.add_argument("--db-database", default=os.getenv("HLTHPRT_DB_DATABASE") or "healthporta_test")
    parser.add_argument("--db-user", default=os.getenv("HLTHPRT_DB_USER") or os.getenv("USER") or "nick")
    parser.add_argument("--db-password", default=os.getenv("HLTHPRT_DB_PASSWORD") or "")
    parser.add_argument("--db-schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")


def _add_source_catalog_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seed-db-path", help="SQLite seed database path for local CLI runs.")
    parser.add_argument("--seed-db-url", help="SQLite seed database URL for local CLI runs. Omit path and URL to use the importer default.")
    parser.add_argument("--retest-results-path", help="Optional provider-directory-db retest_results.json path for local/control runs.")
    parser.add_argument("--retest-results-url", help="Optional provider-directory-db retest_results.json URL for local/control runs.")
    parser.add_argument("--credential-config-file", help="Optional Provider Directory credentials JSON file for local CLI runs.")
    parser.add_argument("--limit", type=int, default=10, help="Source limit for bounded runs.")
    parser.add_argument("--source-query", help="Optional source org/plan filter.")
    parser.add_argument("--refresh-preset", choices=("monthly-full",), help="Named importer defaults, matching scheduled Provider Directory runs.")
    supplemental_group = parser.add_mutually_exclusive_group()
    supplemental_group.add_argument("--include-supplemental-catalogs", dest="include_supplemental_catalogs", action="store_true", help="Include payer-specific supplemental Provider Directory catalog pages.")
    supplemental_group.add_argument("--no-include-supplemental-catalogs", dest="include_supplemental_catalogs", action="store_false", help="Skip payer-specific supplemental Provider Directory catalog pages.")
    parser.set_defaults(include_supplemental_catalogs=None)


def _add_import_behavior_args(parser: argparse.ArgumentParser) -> None:
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
    parser.add_argument("--resource-deadline-seconds", type=int, default=None, help="Seconds to spend fetching one regular resource endpoint; 0 disables the deadline.")
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Provider Directory FHIR self-harness cases.")
    _add_harness_case_args(parser)
    _add_database_args(parser)
    _add_source_catalog_args(parser)
    _add_import_behavior_args(parser)
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
