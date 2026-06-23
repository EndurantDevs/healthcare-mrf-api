# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable promote-plus-refresh smoke for PTG address/entity refresh.

The smoke seeds a temporary schema, calls the PTG source-snapshot promotion
control handler with ``refresh_addresses=true``, and executes the generated
``ptg-address-entity-refresh`` payload synchronously inside the same process.
That keeps the test isolated from shared Redis workers while still exercising
the deployed promotion, payload mapping, and refresh code paths against a real
PostgreSQL database.

The ``auto-import-hook`` scenario seeds the same disposable HealthJoy-style
source after source-pointer publication and invokes the post-PTG-import refresh
enqueue hook directly with the control enqueue patched to run synchronously.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sys
import time
import types
from pathlib import Path
from typing import Any


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_PROVIDER_GROUP_LOCATION_TABLE = "ptg2_provider_group_location_codex_new"
DEFAULT_IMPORT_MONTH = dt.date(2026, 6, 1)


def _repo_root() -> Path:
    override = os.getenv("HLTHPRT_REPO_ROOT")
    if override:
        return Path(override).resolve()
    path = Path(__file__).resolve()
    if len(path.parents) >= 3:
        return path.parents[2]
    return Path.cwd()


ROOT = _repo_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _validate_identifier(value: str, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not IDENTIFIER_RE.fullmatch(cleaned):
        raise ValueError(f"{label} must be a PostgreSQL identifier, got {value!r}")
    return cleaned


def _default_schema() -> str:
    return "codex_dev_ptg_refresh_" + dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_SMOKE_SCHEMA") or _default_schema())
    parser.add_argument("--base-schema", default=os.getenv("HLTHPRT_SMOKE_BASE_SCHEMA") or "mrf")
    parser.add_argument(
        "--scenario",
        choices=("promote-plus-refresh", "auto-import-hook"),
        default=os.getenv("HLTHPRT_SMOKE_SCENARIO") or "promote-plus-refresh",
    )
    parser.add_argument("--keep-schema", action="store_true", help="keep the disposable schema after a successful run")
    return parser.parse_args()


async def run_smoke(args: argparse.Namespace) -> dict[str, Any]:
    schema = _validate_identifier(args.schema, label="schema")
    base_schema = _validate_identifier(args.base_schema, label="base-schema")
    pgl_table = _validate_identifier(DEFAULT_PROVIDER_GROUP_LOCATION_TABLE, label="provider-group-location table")
    source_key = "healthjoy"
    unchanged_source_key = "source-b"
    promoted_seed = args.scenario == "auto-import-hook"

    os.environ["HLTHPRT_DB_SCHEMA"] = schema
    os.environ.setdefault("HLTHPRT_CONTROL_API_TOKEN", "codex-smoke-token")
    os.environ.setdefault("HLTHPRT_IMPORT_ID_OVERRIDE", "codex_dev_smoke_20260623")
    os.environ.setdefault("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    os.environ.setdefault("HLTHPRT_DB_POOL_MAX_SIZE", "4")
    os.environ.setdefault("HLTHPRT_PTG_ADDRESS_SQL_WORK_MEM", "64MB")
    os.environ.setdefault("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SQL_WORK_MEM", "64MB")

    # Import after HLTHPRT_DB_SCHEMA is set so SQLAlchemy model schemas bind to
    # the disposable schema.
    from api import control, control_imports  # pylint: disable=import-outside-toplevel
    from db.models import db  # pylint: disable=import-outside-toplevel
    from process.ext.utils import my_init_db  # pylint: disable=import-outside-toplevel
    from process import ptg as process_ptg  # pylint: disable=import-outside-toplevel
    from process.ptg_address_entity_refresh import process_data as refresh_process_data  # pylint: disable=import-outside-toplevel

    await my_init_db(db)

    async def status(sql: str, **params: Any) -> int:
        return int(await db.status(sql, **params) or 0)

    async def scalar(sql: str, **params: Any) -> Any:
        return await db.scalar(sql, **params)

    async def seed() -> None:
        await db.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await db.status(f"CREATE SCHEMA {schema};")
        clone_tables = [
            "ptg_address",
            "entity_address_unified",
            "entity_address_evidence",
            "entity_address_plan_bridge",
            "entity_address_network_bridge",
            "entity_address_ptg_bridge",
            "entity_address_procedure_bridge",
            "entity_address_medication_bridge",
            "ptg2_snapshot",
            "ptg2_plan",
            "ptg2_plan_month",
            "ptg2_current_source_snapshot",
            "ptg2_current_plan_source",
        ]
        for table in clone_tables:
            await db.status(f"CREATE TABLE {schema}.{table} (LIKE {base_schema}.{table} INCLUDING ALL);")
        await db.status(
            f"""
            CREATE TABLE {schema}.{pgl_table} (
                provider_group_hash varchar,
                npi bigint,
                address_checksum varchar,
                first_line varchar,
                second_line varchar,
                city_name varchar,
                state_name varchar,
                postal_code varchar,
                zip5 varchar,
                country_code varchar,
                lat numeric,
                "long" numeric
            );
            """
        )
        await db.status(
            f"""
            INSERT INTO {schema}.{pgl_table} (
                provider_group_hash, npi, address_checksum, first_line, second_line,
                city_name, state_name, postal_code, zip5, country_code, lat, "long"
            ) VALUES (
                :provider_group_hash, :npi, :address_checksum, :first_line, NULL,
                :city_name, :state_name, :postal_code, :zip5, :country_code, :lat, :long
            );
            """,
            provider_group_hash="pg-new",
            npi=1234567890,
            address_checksum="new-address-checksum",
            first_line="123 New St",
            city_name="Boston",
            state_name="MA",
            postal_code="02110",
            zip5="02110",
            country_code="US",
            lat=42.3584,
            long=-71.0598,
        )
        manifest = json.dumps(
            {
                "serving_index": {
                    "source_key": source_key,
                    "provider_group_location_table": f"{schema}.{pgl_table}",
                }
            }
        )
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_snapshot (
                snapshot_id, import_run_id, import_month, status, created_at, validated_at,
                published_at, previous_snapshot_id, manifest
            ) VALUES (
                :snapshot_id, :import_run_id, :import_month, :status, NOW(), NOW(), NOW(),
                :previous_snapshot_id, CAST(:manifest AS json)
            );
            """,
            snapshot_id="snap-new",
            import_run_id="run-new",
            import_month=DEFAULT_IMPORT_MONTH,
            status="published",
            previous_snapshot_id="snap-old",
            manifest=manifest,
        )
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_plan (
                plan_hash, hash_prefix, plan_id, plan_id_type, plan_name, plan_market_type,
                issuer_name, plan_sponsor_name, canonical_payload, created_at
            ) VALUES (
                :plan_hash, :hash_prefix, :plan_id, :plan_id_type, :plan_name, :plan_market_type,
                :issuer_name, :plan_sponsor_name, CAST(:canonical_payload AS json), NOW()
            );
            """,
            plan_hash="plan-hash-new",
            hash_prefix="plan-hash-new",
            plan_id="plan-new",
            plan_id_type="ein",
            plan_name="Plan New",
            plan_market_type="group",
            issuer_name="Issuer New",
            plan_sponsor_name="Sponsor New",
            canonical_payload="{}",
        )
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_plan_month (
                plan_month_id, snapshot_id, plan_hash, import_month, created_at
            ) VALUES (
                :plan_month_id, :snapshot_id, :plan_hash, :import_month, NOW()
            );
            """,
            plan_month_id="plan-month-new",
            snapshot_id="snap-new",
            plan_hash="plan-hash-new",
            import_month=DEFAULT_IMPORT_MONTH,
        )
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_current_source_snapshot (
                source_key, snapshot_id, previous_snapshot_id, import_month, updated_at
            ) VALUES (:source_key, :snapshot_id, :previous_snapshot_id, :import_month, NOW());
            """,
            source_key=source_key,
            snapshot_id="snap-new" if promoted_seed else "snap-old",
            previous_snapshot_id="snap-old" if promoted_seed else None,
            import_month=DEFAULT_IMPORT_MONTH,
        )
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_current_plan_source (
                plan_source_key, plan_id, plan_market_type, import_month, source_key,
                snapshot_id, previous_snapshot_id, updated_at
            ) VALUES (
                :plan_source_key, :plan_id, :plan_market_type, :import_month, :source_key,
                :snapshot_id, NULL, NOW()
            );
            """,
            plan_source_key=("healthjoy-plan-new" if promoted_seed else "old-healthjoy-plan"),
            plan_id=("plan-new" if promoted_seed else "plan-old"),
            plan_market_type="group",
            import_month=DEFAULT_IMPORT_MONTH,
            source_key=source_key,
            snapshot_id=("snap-new" if promoted_seed else "snap-old"),
        )
        await db.status(
            f"""
            INSERT INTO {schema}.ptg_address (
                source_key, snapshot_id, plan_id, ptg_plan_id, provider_group_id, npi,
                location_key, address_source_record_key, zip5, state_code, city_norm,
                lat, "long", ptg_plan_array, ptg_source_array, group_plan_array,
                base_address_version, observed_at, updated_at
            ) VALUES
            (
                :source_key, 'snap-old', 'plan-old', 'plan-old', 'pg-old', 1234567890,
                'old-healthjoy-location', 'old-healthjoy-record', '02109', 'MA', 'boston',
                42.36, -71.06, ARRAY['plan-old']::varchar[], ARRAY[CAST(:source_key AS varchar)]::varchar[], ARRAY['plan-old']::varchar[],
                'address_archive_v2:v2', NOW(), NOW()
            ),
            (
                :unchanged_source_key, 'snap-keep', 'plan-keep', 'plan-keep', 'pg-keep', 2222222222,
                'keep-source-b-location', 'keep-source-b-record', '10001', 'NY', 'newyork',
                40.75, -73.99, ARRAY['plan-keep']::varchar[], ARRAY[CAST(:unchanged_source_key AS varchar)]::varchar[], ARRAY['plan-keep']::varchar[],
                'address_archive_v2:v2', NOW(), NOW()
            );
            """,
            source_key=source_key,
            unchanged_source_key=unchanged_source_key,
        )
        await db.status(
            f"""
            INSERT INTO {schema}.entity_address_unified (
                entity_type, entity_id, npi, location_key, checksum, type,
                ptg_plan_array, ptg_source_array, group_plan_array, base_address_version,
                ptg_address_version, city_name, state_name, postal_code, country_code,
                lat, "long", updated_at
            ) VALUES
            (
                'npi', '1234567890', 1234567890, 'old-healthjoy-location', 1, 'practice',
                ARRAY['plan-old']::varchar[], ARRAY[CAST(:source_key AS varchar)]::varchar[], ARRAY['plan-old']::varchar[],
                'address_archive_v2:v2', 'ptg:snap-old', 'boston', 'MA', '02109', 'US',
                42.36, -71.06, NOW()
            ),
            (
                'npi', '2222222222', 2222222222, 'keep-source-b-location', 2, 'practice',
                ARRAY['plan-keep']::varchar[], ARRAY[CAST(:unchanged_source_key AS varchar)]::varchar[], ARRAY['plan-keep']::varchar[],
                'address_archive_v2:v2', 'ptg:snap-keep', 'newyork', 'NY', '10001', 'US',
                40.75, -73.99, NOW()
            );
            """,
            source_key=source_key,
            unchanged_source_key=unchanged_source_key,
        )

    async def checks() -> dict[str, Any]:
        return {
            "current_source_snapshot": await scalar(
                f"SELECT snapshot_id FROM {schema}.ptg2_current_source_snapshot WHERE source_key=:source_key;",
                source_key=source_key,
            ),
            "current_plan_snapshot": await scalar(
                f"SELECT snapshot_id FROM {schema}.ptg2_current_plan_source WHERE source_key=:source_key;",
                source_key=source_key,
            ),
            "current_plan_id": await scalar(
                f"SELECT plan_id FROM {schema}.ptg2_current_plan_source WHERE source_key=:source_key;",
                source_key=source_key,
            ),
            "ptg_old_source_rows": await scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg_address WHERE source_key=:source_key AND snapshot_id='snap-old';",
                source_key=source_key,
            ),
            "ptg_new_source_rows": await scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg_address WHERE source_key=:source_key AND snapshot_id='snap-new';",
                source_key=source_key,
            ),
            "ptg_unchanged_rows": await scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg_address WHERE source_key=:unchanged_source_key AND snapshot_id='snap-keep';",
                unchanged_source_key=unchanged_source_key,
            ),
            "entity_old_rows": await scalar(
                f"SELECT COUNT(*) FROM {schema}.entity_address_unified WHERE ptg_address_version='ptg:snap-old';"
            ),
            "entity_new_rows": await scalar(
                f"SELECT COUNT(*) FROM {schema}.entity_address_unified WHERE ptg_address_version='ptg:snap-new';"
            ),
            "entity_unchanged_rows": await scalar(
                f"SELECT COUNT(*) FROM {schema}.entity_address_unified WHERE ptg_address_version='ptg:snap-keep';"
            ),
            "stage_tables_left": await scalar(
                """
                SELECT COUNT(*)
                  FROM information_schema.tables
                 WHERE table_schema = :schema
                   AND table_name NOT IN (
                        'ptg_address', 'entity_address_unified', 'entity_address_evidence',
                        'entity_address_plan_bridge', 'entity_address_network_bridge',
                        'entity_address_ptg_bridge', 'entity_address_procedure_bridge',
                        'entity_address_medication_bridge', 'ptg2_snapshot', 'ptg2_plan',
                        'ptg2_plan_month', 'ptg2_current_source_snapshot', 'ptg2_current_plan_source',
                        :pgl_table
                   );
                """,
                schema=schema,
                pgl_table=pgl_table,
            ),
        }

    await seed()
    timings: dict[str, float] = {}
    captured_payloads: list[dict[str, Any]] = []
    refresh_results: list[dict[str, Any]] = []

    async def fake_create_import_run(run_payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        captured_payloads.append(run_payload)
        start = time.perf_counter()
        refresh_result = await refresh_process_data({}, run_payload["params"])
        refresh_results.append(refresh_result)
        timings["refresh_seconds"] = round(time.perf_counter() - start, 6)
        return (
            {
                "run_id": run_payload.get("run_id") or "sync-dev-smoke",
                "status": "succeeded",
                "importer": run_payload["importer"],
                "params": run_payload["params"],
                "result": refresh_result,
            },
            True,
        )

    async def fake_ensure_import_run_table() -> None:
        return None

    control.create_import_run = fake_create_import_run
    control_imports.create_import_run = fake_create_import_run
    control_imports.ensure_import_run_table = fake_ensure_import_run_table
    token = os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "codex-smoke-token"

    try:
        body: dict[str, Any] = {}
        response_status: int | None = None
        hook_result: dict[str, Any] | None = None
        if args.scenario == "promote-plus-refresh":
            request = types.SimpleNamespace(
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "source_key": source_key,
                    "snapshot_id": "snap-new",
                    "expected_current_snapshot_id": "snap-old",
                    "refresh_addresses": True,
                    "address_refresh": {
                        "publish": True,
                        "limit_per_source": 25,
                        "idempotency_key": "codex-dev-refresh-healthjoy-snap-new",
                    },
                },
                id="codex-dev-promote-refresh-smoke",
            )
            start = time.perf_counter()
            response = await control.control_ptg_source_snapshot_promote(request)
            timings["promote_plus_refresh_seconds"] = round(time.perf_counter() - start, 6)
            response_status = response.status
            body = json.loads(response.body)
        else:
            start = time.perf_counter()
            hook_result = await process_ptg._enqueue_ptg2_auto_address_refresh_after_import(
                source_key=source_key,
                snapshot_id="snap-new",
                import_run_id="healthjoy-run-new",
                has_serving_files=True,
                source_scoped_compact=True,
                test_mode=False,
            )
            timings["auto_import_hook_seconds"] = round(time.perf_counter() - start, 6)
            body = {"address_refresh": hook_result}
        check_values = await checks()
        expected = {
            "current_source_snapshot": "snap-new",
            "current_plan_snapshot": "snap-new",
            "current_plan_id": "plan-new",
            "ptg_old_source_rows": 0,
            "ptg_new_source_rows": 1,
            "ptg_unchanged_rows": 1,
            "entity_old_rows": 0,
            "entity_new_rows": 1,
            "entity_unchanged_rows": 1,
            "stage_tables_left": 0,
        }
        if response_status not in (None, 200) or check_values != expected:
            raise AssertionError(
                {
                    "status": response_status,
                    "expected": expected,
                    "actual": check_values,
                    "body": body,
                }
            )
        if not captured_payloads:
            raise AssertionError({"error": "refresh payload was not captured", "body": body})
        run_payload = captured_payloads[0]
        payload_checks = {
            "importer": run_payload["importer"],
            "idempotency_key": run_payload["idempotency_key"],
            "triggered_by": run_payload["triggered_by"],
            "ptg_refresh_mode": run_payload["params"].get("ptg_refresh_mode"),
            "entity_refresh_mode": run_payload["params"].get("entity_refresh_mode"),
            "publish": run_payload["params"].get("publish"),
            "limit_per_source": run_payload["params"].get("limit_per_source"),
        }
        if not args.keep_schema:
            await status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        schema_exists = bool(
            await scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name=:schema);",
                schema=schema,
            )
        )
        return {
            "schema": schema,
            "scenario": args.scenario,
            "schema_exists_after_cleanup": schema_exists,
            "response_status": response_status,
            "timings": timings,
            "checks": check_values,
            "payload_checks": payload_checks,
            "address_refresh": body["address_refresh"],
            "refresh_result": refresh_results[0],
        }
    except Exception:
        print(json.dumps({"schema_retained_for_debug": schema}, sort_keys=True), flush=True)
        raise


def main() -> None:
    args = parse_args()
    result = asyncio.run(run_smoke(args))
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
