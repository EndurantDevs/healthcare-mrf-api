# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Quiescent physical-storage evidence for a hospital-price canary."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping

from process.hospital_price_native import hospital_price_version_id


_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_BASELINE_CONTRACT = "hospital_price_quiescent_storage_baseline_v1"


class CanaryError(RuntimeError):
    """Reject an unsafe canary configuration or invalid observation."""


async def _read_storage_evidence(
    dsn: str,
    schema: str,
    version_id: str,
    timeout_seconds: float,
) -> tuple[Mapping[str, Any] | None, list[Mapping[str, Any]], Mapping[str, Any] | None]:
    import asyncpg

    normalized_dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    normalized_dsn = normalized_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    connection = await asyncpg.connect(
        normalized_dsn,
        timeout=timeout_seconds,
        command_timeout=timeout_seconds,
    )
    try:
        version = await connection.fetchrow(
            f"""SELECT version.version_id, content.content_sha256,
                       content.byte_count, root.service_count,
                       root.charge_count, root.fact_count,
                       root.service_block_count, root.fact_block_count,
                       root.code_selector_page_count,
                       root.payer_plan_selector_page_count
                  FROM {quoted_schema}.hospital_price_version version
                  JOIN {quoted_schema}.hospital_price_content content
                    ON content.content_sha256=version.content_sha256
                  JOIN {quoted_schema}.hospital_price_packed_root root
                    ON root.version_id=version.version_id
                 WHERE version.version_id=$1""",
            version_id,
        )
        blocks = await connection.fetch(
            f"""SELECT block_kind, count(*)::bigint AS block_count,
                       sum(octet_length(payload))::bigint AS payload_bytes
                  FROM {quoted_schema}.hospital_price_data_block
                 WHERE version_id=$1 GROUP BY block_kind ORDER BY block_kind""",
            version_id,
        )
        physical = await _read_physical_evidence(
            connection, quoted_schema, schema, version_id
        )
    finally:
        await connection.close()
    return version, list(blocks), physical


async def _read_physical_evidence(
    connection: Any,
    quoted_schema: str,
    schema: str,
    version_id: str,
) -> Mapping[str, Any] | None:
    return await connection.fetchrow(
            f"""SELECT current_database() AS database_name,
                (SELECT oid::bigint FROM pg_database
                  WHERE datname=current_database()) AS database_oid,
                pg_database_size(current_database())::bigint AS database_bytes,
                (SELECT coalesce(sum(pg_total_relation_size(relation.oid)), 0)::bigint
                   FROM pg_class relation
                   JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
                  WHERE namespace.nspname=$1
                    AND relation.relkind IN ('r', 'p', 'm')
                    AND relation.relname LIKE 'hospital_price_%')
                    AS hospital_relation_bytes,
                (SELECT count(*)::bigint
                   FROM {quoted_schema}.hospital_price_packed_root
                  WHERE version_id=$2) AS target_root_count,
                (SELECT count(*)::bigint
                   FROM {quoted_schema}.hospital_price_packed_root
                  WHERE version_id<>$2) AS comparison_root_count,
                (SELECT md5(coalesce(string_agg(version_id, ',' ORDER BY version_id), ''))
                   FROM {quoted_schema}.hospital_price_packed_root
                  WHERE version_id<>$2) AS comparison_root_digest,
                (SELECT count(*)::bigint FROM {quoted_schema}.import_run
                  WHERE importer='hospital-prices'
                    AND status IN ('queued', 'starting', 'running',
                                   'finalizing', 'canceling')) AS active_runs,
                (SELECT count(*)::bigint
                   FROM {quoted_schema}.hospital_price_import_attempt
                  WHERE status IN ('queued', 'running', 'verified'))
                    AS active_attempts""",
            schema,
            version_id,
        )


def _expected_block_counts(version: Mapping[str, Any]) -> dict[int, int]:
    expected_by_kind = {
        1: int(version["service_block_count"]),
        2: int(version["fact_block_count"]),
        3: int(version["code_selector_page_count"]),
        4: int(version["payer_plan_selector_page_count"]),
    }
    return {kind: count for kind, count in expected_by_kind.items() if count > 0}


def _validated_baseline(
    baseline: Mapping[str, Any],
    schema: str,
    version: Mapping[str, Any],
    physical: Mapping[str, Any],
    maximum_age_seconds: float,
) -> tuple[int, int]:
    try:
        captured_at = dt.datetime.fromisoformat(
            str(baseline["captured_at"]).replace("Z", "+00:00")
        )
        expected_source_bytes = int(baseline["expected_source_bytes"])
        baseline_relation_bytes = int(baseline["hospital_relation_bytes"])
        expected_source_sha256 = str(baseline["expected_source_sha256"])
        if captured_at.tzinfo is None:
            raise ValueError("timestamp is not timezone-aware")
        age_seconds = (dt.datetime.now(dt.UTC) - captured_at).total_seconds()
    except (KeyError, TypeError, ValueError):
        raise CanaryError("pre-import storage receipt is invalid") from None
    if (
        baseline.get("schema_version") != 1
        or baseline.get("contract") != _BASELINE_CONTRACT
        or baseline.get("database_schema") != schema
        or baseline.get("database_name") != physical.get("database_name")
        or baseline.get("database_oid") != physical.get("database_oid")
        or baseline.get("expected_version_id") != version.get("version_id")
        or expected_source_sha256 != version.get("content_sha256")
        or _SHA256_PATTERN.fullmatch(expected_source_sha256) is None
        or hospital_price_version_id(expected_source_sha256)
        != baseline.get("expected_version_id")
        or expected_source_bytes != int(version.get("byte_count") or 0)
        or baseline.get("comparison_root_count")
        != physical.get("comparison_root_count")
        or baseline.get("comparison_root_digest")
        != physical.get("comparison_root_digest")
        or baseline.get("active_runs") != 0
        or baseline.get("active_attempts") != 0
        or int(physical.get("target_root_count") or 0) != 1
        or not 0 <= age_seconds <= maximum_age_seconds
        or expected_source_bytes <= 0
        or baseline_relation_bytes < 0
    ):
        raise CanaryError("pre-import storage receipt is not bound to this import")
    return baseline_relation_bytes, expected_source_bytes


def _validated_storage_receipt(
    version: Mapping[str, Any] | None,
    blocks: list[Mapping[str, Any]],
    physical: Mapping[str, Any] | None,
    baseline: Mapping[str, Any],
    schema: str,
    maximum_baseline_age_seconds: float,
) -> dict[str, object]:
    """Validate one exact packed version and its attributable physical growth."""

    if version is None or not blocks:
        raise CanaryError("packed storage evidence is unavailable")
    try:
        source_bytes = int(version["byte_count"])
        packed_payload_bytes = sum(int(block["payload_bytes"]) for block in blocks)
        block_count_by_kind = {
            int(block["block_kind"]): int(block["block_count"])
            for block in blocks
        }
        expected_block_counts = _expected_block_counts(version)
    except (KeyError, TypeError, ValueError):
        raise CanaryError("packed storage evidence is invalid") from None
    if source_bytes <= 0 or packed_payload_bytes <= 0:
        raise CanaryError("packed storage byte counts are invalid")
    if physical is None:
        raise CanaryError("physical hospital storage evidence is unavailable")
    if block_count_by_kind != expected_block_counts:
        raise CanaryError("packed storage block counts are incomplete")
    baseline_relation_bytes, unique_source_bytes = _validated_baseline(
        baseline, schema, version, physical, maximum_baseline_age_seconds
    )
    current_relation_bytes = int(physical["hospital_relation_bytes"])
    if (
        int(physical["active_runs"]) != 0
        or int(physical["active_attempts"]) != 0
        or current_relation_bytes < baseline_relation_bytes
        or unique_source_bytes <= 0
    ):
        raise CanaryError("physical hospital storage evidence is not quiescent")
    physical_growth_bytes = current_relation_bytes - baseline_relation_bytes
    if physical_growth_bytes <= 0:
        raise CanaryError("physical hospital storage growth is not attributable")
    return _storage_receipt_payload(
        version,
        blocks,
        physical,
        baseline_relation_bytes,
        unique_source_bytes,
        packed_payload_bytes,
        physical_growth_bytes,
    )


def _storage_receipt_payload(
    version: Mapping[str, Any],
    blocks: list[Mapping[str, Any]],
    physical: Mapping[str, Any],
    baseline_relation_bytes: int,
    unique_source_bytes: int,
    packed_payload_bytes: int,
    physical_growth_bytes: int,
) -> dict[str, object]:
    """Return the stable source, packed-byte, and physical-byte receipt fields."""

    source_bytes = int(version["byte_count"])
    return {
        "content_sha256": str(version["content_sha256"]),
        "measurement": "quiescent_pre_post_hospital_relations_including_heap_toast_and_indexes",
        "database_bytes": int(physical["database_bytes"]),
        "baseline_hospital_relation_bytes": baseline_relation_bytes,
        "current_hospital_relation_bytes": int(physical["hospital_relation_bytes"]),
        "physical_growth_bytes": physical_growth_bytes,
        "unique_downloaded_source_bytes": unique_source_bytes,
        "physical_storage_ratio_to_unique_source": round(
            physical_growth_bytes / unique_source_bytes, 6
        ),
        "source_content_bytes": source_bytes,
        "packed_payload_bytes": packed_payload_bytes,
        "packed_payload_ratio_to_source": round(packed_payload_bytes / source_bytes, 6),
        "service_count": int(version["service_count"]),
        "charge_count": int(version["charge_count"]),
        "fact_count": int(version["fact_count"]),
        "blocks": [
            {
                "block_kind": int(block["block_kind"]),
                "block_count": int(block["block_count"]),
                "payload_bytes": int(block["payload_bytes"]),
            }
            for block in blocks
        ],
    }


async def capture_storage_receipt(
    dsn: str,
    schema: str,
    version_id: str,
    baseline: Mapping[str, Any],
    timeout_seconds: float,
    maximum_baseline_age_seconds: float,
) -> dict[str, object]:
    """Capture exact-version packed bytes and quiescent physical relation growth."""

    version, blocks, physical = await _read_storage_evidence(
        dsn, schema, version_id, timeout_seconds
    )
    receipt = _validated_storage_receipt(
        version, blocks, physical, baseline, schema, maximum_baseline_age_seconds
    )
    return {"version_id": version_id, **receipt}


async def capture_storage_baseline(
    dsn: str,
    schema: str,
    expected_source_sha256: str,
    expected_source_bytes: int,
    timeout_seconds: float,
) -> dict[str, object]:
    """Capture one source-bound quiescent baseline before its first import."""

    if (
        _IDENTIFIER_PATTERN.fullmatch(schema) is None
        or _SHA256_PATTERN.fullmatch(expected_source_sha256) is None
        or expected_source_bytes <= 0
        or timeout_seconds <= 0
    ):
        raise CanaryError("storage baseline inputs are invalid")
    expected_version_id = hospital_price_version_id(expected_source_sha256)
    import asyncpg

    normalized_dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    normalized_dsn = normalized_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    connection = await asyncpg.connect(
        normalized_dsn,
        timeout=timeout_seconds,
        command_timeout=timeout_seconds,
    )
    try:
        physical = await _read_physical_evidence(
            connection,
            '"' + schema.replace('"', '""') + '"',
            schema,
            expected_version_id,
        )
    finally:
        await connection.close()
    if (
        physical is None
        or int(physical.get("active_runs") or 0) != 0
        or int(physical.get("active_attempts") or 0) != 0
        or int(physical.get("target_root_count") or 0) != 0
    ):
        raise CanaryError("storage baseline is not quiescent or source-new")
    return {
        "schema_version": 1,
        "contract": _BASELINE_CONTRACT,
        "captured_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "database_name": physical["database_name"],
        "database_oid": physical["database_oid"],
        "database_schema": schema,
        "database_bytes": int(physical["database_bytes"]),
        "hospital_relation_bytes": int(physical["hospital_relation_bytes"]),
        "comparison_root_count": int(physical["comparison_root_count"]),
        "comparison_root_digest": str(physical["comparison_root_digest"]),
        "active_runs": 0,
        "active_attempts": 0,
        "expected_source_sha256": expected_source_sha256,
        "expected_source_bytes": expected_source_bytes,
        "expected_version_id": expected_version_id,
    }


def capture_storage_baseline_cli(argv: list[str] | None = None) -> int:
    """Write one pre-import baseline bound to an exact source artifact."""

    parser = argparse.ArgumentParser(description=capture_storage_baseline_cli.__doc__)
    parser.add_argument("--database-url-env", default="HOSPITAL_PRICE_CANARY_DATABASE_URL")
    parser.add_argument("--database-schema", default="mrf")
    parser.add_argument("--expected-source-sha256", required=True)
    parser.add_argument("--expected-source-bytes", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    dsn = os.getenv(args.database_url_env)
    if not dsn:
        raise SystemExit(f"database URL environment is unset: {args.database_url_env}")
    try:
        receipt = asyncio.run(capture_storage_baseline(
            dsn,
            args.database_schema,
            args.expected_source_sha256,
            args.expected_source_bytes,
            args.timeout_seconds,
        ))
    except CanaryError as error:
        raise SystemExit(str(error)) from None
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(capture_storage_baseline_cli())
