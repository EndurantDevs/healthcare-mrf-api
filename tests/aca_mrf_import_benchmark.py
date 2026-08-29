# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Matched synthetic ACA MRF import benchmark for a disposable PostgreSQL DB."""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import os
import resource
import tempfile
import time
from pathlib import Path


DATABASE = os.getenv("HLTHPRT_DB_DATABASE", "")
PORT = os.getenv("HLTHPRT_DB_PORT", "")
SCHEMA = "mrf_speed_test"
SUFFIX = "aca_speed_20260829"

if "test" not in DATABASE or PORT != "5440":
    raise RuntimeError("benchmark requires a disposable test database on PostgreSQL port 5440")

os.environ.setdefault("HLTHPRT_DB_DRIVER", "asyncpg")
os.environ.setdefault("HLTHPRT_DB_HOST", "127.0.0.1")
os.environ.setdefault("HLTHPRT_DB_USER", os.getenv("USER", "postgres"))
os.environ.setdefault("HLTHPRT_DB_PASSWORD", "")
os.environ.setdefault("HLTHPRT_DB_SCHEMA", SCHEMA)
os.environ.setdefault("HLTHPRT_DB_POOL_MIN_SIZE", "1")
os.environ.setdefault("HLTHPRT_DB_POOL_MAX_SIZE", "4")
os.environ.setdefault("HLTHPRT_MRF_PROVIDER_FLUSH_ROWS", "5000")
os.environ.setdefault("HLTHPRT_MRF_PLAN_FLUSH_ROWS", "2000")
os.environ.setdefault("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", "0")
os.environ.setdefault("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "64MB")

from db.models import (  # noqa: E402
    Issuer,
    MRFAddress,
    MRFAddressEvidence,
    Plan,
    PlanBenefitsMarketplace,
    PlanFormulary,
    PlanNetworkTierRaw,
    PlanNPIRaw,
    db,
)
from process import initial  # noqa: E402
from process.ext.utils import make_class, push_objects  # noqa: E402


MODELS = (
    Issuer,
    Plan,
    PlanFormulary,
    PlanBenefitsMarketplace,
    PlanNPIRaw,
    PlanNetworkTierRaw,
    MRFAddress,
    MRFAddressEvidence,
)


def _write_fixture(path: Path, rows: list[dict]) -> str:
    path.write_text(json.dumps(rows, separators=(",", ":")), encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixtures(root: Path, plan_count: int, provider_count: int) -> dict:
    year = datetime.datetime.now().year
    issuer_ids = (12345, 23456, 34567)
    plan_ids = [f"{issuer_id:05d}AA{idx:07d}" for idx, issuer_id in enumerate(issuer_ids)]
    plans = []
    for idx in range(plan_count):
        issuer_id = issuer_ids[idx % len(issuer_ids)]
        plan_id = f"{issuer_id:05d}AA{idx:07d}"
        plans.append(
            {
                "plan_id": plan_id,
                "plan_id_type": "CMS-HIOS-PLAN-ID",
                "years": [year],
                "marketing_name": f"Synthetic ACA Plan {idx}",
                "summary_url": f"https://synthetic.example/plan/{idx}",
                "plan_contact": "synthetic@example.invalid",
                "network": [{"network_tier": "PREFERRED"}],
                "formulary": [
                    {
                        "drug_tier": "GENERIC",
                        "mail_order": False,
                        "cost_sharing": [
                            {
                                "pharmacy_type": "RETAIL",
                                "copay_amount": 10.0,
                                "copay_opt": "AFTER_DEDUCTIBLE",
                                "coinsurance_rate": 0.1,
                                "coinsurance_opt": "NONE",
                            }
                        ],
                    }
                ],
                "benefits": [{"name": "virtual_visit", "value": True}],
                "last_updated_on": f"{year}-01-01",
            }
        )

    providers = []
    for idx in range(provider_count):
        providers.append(
            {
                "npi": str(1000000000 + idx),
                "type": "INDIVIDUAL",
                "name": {"first": "Synthetic", "last": f"Provider {idx}"},
                "specialty": ["Synthetic Specialty"],
                "languages": ["en"],
                "addresses": [
                    {
                        "address": f"{idx + 1} Main Street",
                        "city": "Austin",
                        "state": "TX",
                        "zip": f"{78700 + (idx % 100):05d}",
                        "phone": f"512555{idx % 10000:04d}",
                    },
                    {
                        "address": f"{idx + 1} Second Street",
                        "city": "Austin",
                        "state": "TX",
                        "zip": f"{78700 + (idx % 100):05d}",
                    },
                ],
                "accepting": "true",
                "gender": "X",
                "plans": [
                    {
                        "plan_id": plan_id,
                        "network_tier": network_tier,
                        "years": [year],
                    }
                    for plan_id, network_tier in zip(
                        plan_ids, ("PREFERRED", "STANDARD", "VALUE")
                    )
                ],
                "last_updated_on": f"{year}-01-01",
            }
        )

    plan_path = root / "plans.json"
    provider_path = root / "providers.json"
    hashes = {
        "plans_sha256": _write_fixture(plan_path, plans),
        "providers_sha256": _write_fixture(provider_path, providers),
    }
    return {
        "year": year,
        "issuer_ids": issuer_ids,
        "plan_path": plan_path,
        "provider_path": provider_path,
        "hashes": hashes,
    }


async def _drop_tables(stages: dict) -> None:
    for model in reversed(MODELS):
        stage = stages[model]
        await db.status(f"DROP TABLE IF EXISTS {SCHEMA}.{stage.__tablename__};")


async def _prepare_tables(stages: dict) -> None:
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    await _drop_tables(stages)
    for model in MODELS:
        stage = stages[model]
        await db.create_table(stage.__table__, checkfirst=True)
        if model in {PlanBenefitsMarketplace, MRFAddress, MRFAddressEvidence}:
            await initial._create_named_indexes(stage, SCHEMA)


async def _table_receipt(stage, order_by: str) -> dict:
    qualified_name = f"{SCHEMA}.{stage.__tablename__}"
    row = await db.all(
        f"""
        SELECT COUNT(*)::bigint AS row_count,
               COALESCE(
                   md5(string_agg(md5(to_jsonb(t)::text), '' ORDER BY {order_by})),
                   md5('')
               ) AS digest
          FROM {qualified_name} AS t;
        """
    )
    table_bytes = await db.scalar(
        "SELECT pg_total_relation_size(CAST(:qualified_name AS regclass))",
        qualified_name=qualified_name,
    )
    return {
        "row_count": int(row[0].row_count),
        "digest": row[0].digest,
        "table_bytes": int(table_bytes),
    }


async def _run() -> dict:
    plan_count = int(os.getenv("HLTHPRT_ACA_MRF_BENCHMARK_PLANS", "1500"))
    provider_count = int(os.getenv("HLTHPRT_ACA_MRF_BENCHMARK_PROVIDERS", "4000"))
    await db.connect()
    stages = {
        model: make_class(model, SUFFIX, schema_override=SCHEMA)
        for model in MODELS
    }
    cleaned = False
    try:
        await _prepare_tables(stages)
        with tempfile.TemporaryDirectory(prefix="aca_mrf_speed_") as tmpdirname:
            fixtures = _fixtures(Path(tmpdirname), plan_count, provider_count)
            await push_objects(
                [
                    {
                        "state": "AA",
                        "issuer_id": issuer_id,
                        "issuer_name": f"Synthetic Issuer {issuer_id}",
                        "issuer_marketing_name": "",
                        "mrf_url": f"https://synthetic.example/{issuer_id}/index.json",
                        "data_contact_email": "synthetic@example.invalid",
                    }
                    for issuer_id in fixtures["issuer_ids"]
                ],
                stages[Issuer],
            )
            ctx = {
                "context": {"import_date": SUFFIX, "test_mode": False},
                "redis": None,
            }

            plan_started = time.perf_counter()
            await initial.process_plan(
                ctx,
                {
                    "url": "https://synthetic.example/plans.json",
                    "source_url": "https://synthetic.example/plans.json",
                    "input_url": fixtures["plan_path"].as_uri(),
                    "issuer_array": list(fixtures["issuer_ids"]),
                },
            )
            plan_seconds = time.perf_counter() - plan_started

            provider_started = time.perf_counter()
            await initial.process_provider(
                ctx,
                {
                    "url": "https://synthetic.example/providers.json",
                    "source_url": "https://synthetic.example/providers.json",
                    "input_url": fixtures["provider_path"].as_uri(),
                    "issuer_array": list(fixtures["issuer_ids"]),
                },
            )
            provider_seconds = time.perf_counter() - provider_started

            summary_started = time.perf_counter()
            await initial._refresh_mrf_address_summary(SUFFIX, SCHEMA)
            summary_seconds = time.perf_counter() - summary_started

            receipts = {
                "plan": await _table_receipt(stages[Plan], "plan_id, year"),
                "plan_formulary": await _table_receipt(
                    stages[PlanFormulary], "plan_id, year, drug_tier, pharmacy_type"
                ),
                "plan_benefits_marketplace": await _table_receipt(
                    stages[PlanBenefitsMarketplace], "plan_id, year, checksum"
                ),
                "plan_npi_raw": await _table_receipt(
                    stages[PlanNPIRaw], "npi, checksum_network"
                ),
                "plan_networktier": await _table_receipt(
                    stages[PlanNetworkTierRaw], "plan_id, checksum_network"
                ),
                "mrf_address_evidence": await _table_receipt(
                    stages[MRFAddressEvidence], "evidence_checksum"
                ),
                "mrf_address": await _table_receipt(
                    stages[MRFAddress], "npi, type, checksum"
                ),
            }
            expected_counts = {
                "plan": plan_count,
                "plan_formulary": plan_count,
                "plan_benefits_marketplace": plan_count,
                "plan_npi_raw": provider_count * 3,
                "plan_networktier": 3,
                "mrf_address_evidence": provider_count * 2 * 3,
                "mrf_address": provider_count * 2,
            }
            actual_counts = {
                name: receipt["row_count"] for name, receipt in receipts.items()
            }
            return {
                "database": DATABASE,
                "postgres_port": int(PORT),
                "schema": SCHEMA,
                "source_revision": os.getenv("HLTHPRT_ACA_MRF_BENCHMARK_REVISION", "unknown"),
                "settings": {
                    "plan_count": plan_count,
                    "provider_count": provider_count,
                    "plan_flush_rows": int(os.environ["HLTHPRT_MRF_PLAN_FLUSH_ROWS"]),
                    "provider_flush_rows": int(os.environ["HLTHPRT_MRF_PROVIDER_FLUSH_ROWS"]),
                    "address_aggregate_during_ingest": os.environ[
                        "HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST"
                    ],
                },
                "fixture_hashes": fixtures["hashes"],
                "network_seconds": 0.0,
                "phase_seconds": {
                    "plan_ingest": round(plan_seconds, 6),
                    "provider_ingest": round(provider_seconds, 6),
                    "address_summary": round(summary_seconds, 6),
                },
                "whole_seconds": round(
                    plan_seconds + provider_seconds + summary_seconds, 6
                ),
                "max_rss_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
                "expected_counts": expected_counts,
                "actual_counts": actual_counts,
                "correctness_ok": actual_counts == expected_counts,
                "receipts": receipts,
            }
    finally:
        await _drop_tables(stages)
        cleaned = not bool(
            await db.scalar(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM information_schema.tables
                     WHERE table_schema = :schema
                       AND table_name LIKE :prefix
                )
                """,
                schema=SCHEMA,
                prefix=f"%{SUFFIX}%",
            )
        )
        await db.disconnect()
        if not cleaned:
            raise RuntimeError("benchmark staging tables were not cleaned")


if __name__ == "__main__":
    print(json.dumps(asyncio.run(_run()), sort_keys=True))
