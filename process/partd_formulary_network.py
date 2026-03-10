# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import hashlib
import json
import os
import re
import secrets
import shutil
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from arq import create_pool

from db.connection import db
from db.models import (PartDFormularySnapshot, PartDImportRun, PartDMedicationCost,
                       PartDMedicationCostStage, PartDPharmacyActivity,
                       PartDPharmacyActivityStage)
from process.ext.utils import db_startup, download_it, download_it_and_save, push_objects
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

# CMS rows can exceed Python's default field limit.
_csv_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_csv_limit)
        break
    except OverflowError:
        _csv_limit = _csv_limit // 10


CATALOG_URL = "https://data.cms.gov/data.json"
MONTHLY_DATASET_TITLE = (
    "Monthly Prescription Drug Plan Formulary and Pharmacy Network Information"
)
QUARTERLY_DATASET_TITLE = (
    "Quarterly Prescription Drug Plan Formulary, Pharmacy Network, and Pricing Information"
)

PARTD_QUEUE_NAME = "arq:PartDFormularyNetwork"
PARTD_FINISH_QUEUE_NAME = "arq:PartDFormularyNetwork_finish"
PARTD_WORKDIR = os.getenv("HLTHPRT_PARTD_WORKDIR", "/tmp/healthporta_partd")
# Keep default near NPI-scale chunking while allowing explicit per-import override.
PARTD_BATCH_SIZE = max(
    int(
        os.getenv(
            "HLTHPRT_PARTD_BATCH_SIZE",
            os.getenv("HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE", "300000"),
        )
    ),
    1000,
)
PARTD_TEST_MAX_ROWS_PER_FILE = max(
    int(os.getenv("HLTHPRT_PARTD_TEST_MAX_ROWS_PER_FILE", "5000")),
    100,
)

_DATE_PATTERN = re.compile(r"(20\d{2})(\d{2})(\d{2})")
_NON_DIGIT = re.compile(r"[^0-9]+")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_SAFE_FILE_CHARS = re.compile(r"[^a-zA-Z0-9._-]+")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


PARTD_DEFER_ADDITIONAL_INDEXES = _env_bool("HLTHPRT_PARTD_DEFER_ADDITIONAL_INDEXES", default=True)
PARTD_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT = _env_bool(
    "HLTHPRT_PARTD_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT",
    default=True,
)


@dataclass(frozen=True)
class SourceArtifact:
    source_type: str
    url: str
    artifact_name: str
    release_date: datetime.date
    cutoff_month: datetime.date


LEGACY_PARTD_TABLES = (
    "partd_import_run",
    "partd_formulary_snapshot",
    "partd_pharmacy_activity",
    "partd_medication_cost",
    "partd_pharmacy_activity_stage",
    "partd_medication_cost_stage",
)


def _normalize_run_id(run_id: str | None) -> str:
    if run_id:
        normalized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(run_id))
        normalized = normalized.strip("_")
        if normalized:
            return normalized[:64]
    token = secrets.token_hex(4)
    return f"{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{token}"


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    return normalized[:32] or datetime.date.today().strftime("%Y%m%d")


def _artifact_name(url: str) -> str:
    path = urlparse(url).path
    name = Path(path).name.strip()
    return name or "artifact.zip"


def _month_floor(value: datetime.date) -> datetime.date:
    return datetime.date(value.year, value.month, 1)


@lru_cache(maxsize=2048)
def _normalize_key(key: str) -> str:
    return _NON_ALNUM.sub("", str(key or "").strip().lower())


def _parse_date(value: Any) -> datetime.date | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.split("T", 1)[0], text.split(" ", 1)[0]):
        try:
            return datetime.date.fromisoformat(candidate)
        except ValueError:
            pass
    match = _DATE_PATTERN.search(text)
    if match:
        year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        try:
            return datetime.date(year, month, day)
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text.replace(",", "")))
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    if value in (None, "", "*", "NA", "N/A", "null"):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "active", "in", "innetwork", "participating"}:
        return True
    if text in {"0", "false", "no", "n", "inactive", "out", "outofnetwork"}:
        return False
    return None


def _to_npi(value: Any) -> int | None:
    digits = _normalize_code_digits(value)
    if not digits:
        return None
    if len(digits) == 12 and digits.startswith("1") and digits.endswith("0"):
        digits = digits[1:-1]
    elif len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    numeric = _to_int(digits)
    if numeric is None or numeric <= 0:
        return None
    return numeric


def _normalize_code_digits(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    digits = _NON_DIGIT.sub("", text)
    return digits or None


def _compose_plan_id(contract_id: str, plan_component: str, segment_id: str) -> str:
    return f"{contract_id}-{plan_component}-{segment_id}"[:32]


def _row_value(values: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in values:
            return values.get(key)
    return None


def _row_index(row: dict[str, Any]) -> dict[str, Any]:
    indexed: dict[str, Any] = {}
    for key, value in row.items():
        indexed[_normalize_key(key)] = value
    return indexed


def _match_cost_fields(row: dict[str, Any]) -> list[tuple[str, float]]:
    entries: list[tuple[str, float]] = []
    seen: set[str] = set()
    for raw_key, raw_value in row.items():
        key = _normalize_key(raw_key)
        if not key:
            continue
        if not any(token in key for token in ("copay", "coinsurance", "cost", "price", "amount")):
            continue
        amount = _to_float(raw_value)
        if amount is None:
            continue
        cost_type = key[:64]
        if cost_type in seen:
            continue
        entries.append((cost_type, amount))
        seen.add(cost_type)
        if len(entries) >= 5:
            break
    return entries


def _detect_delimiter(file_path: Path) -> str:
    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        sample = handle.read(32768)
    first_line = sample.splitlines()[0] if sample else ""
    if first_line.count("|") > first_line.count(","):
        return "|"
    if first_line.count("\t") > first_line.count(","):
        return "\t"
    return ","


def _extract_distribution_release_date(distribution: dict[str, Any]) -> datetime.date | None:
    for key in ("modified", "issued"):
        parsed = _parse_date(distribution.get(key))
        if parsed is not None:
            return parsed
    url = str(distribution.get("downloadURL") or "")
    match = _DATE_PATTERN.search(url)
    if match:
        year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        try:
            return datetime.date(year, month, day)
        except ValueError:
            return None
    return None


def _resolve_dataset(catalog: dict[str, Any], title: str) -> dict[str, Any]:
    wanted = title.strip().lower()
    for dataset in catalog.get("dataset", []):
        candidate = str(dataset.get("title") or "").strip().lower()
        if candidate == wanted:
            return dataset
    raise LookupError(f"CMS dataset not found: {title}")


def _zip_distributions(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for distribution in dataset.get("distribution", []):
        url = str(distribution.get("downloadURL") or "").strip()
        if not url or not url.lower().endswith(".zip"):
            continue
        release_date = _extract_distribution_release_date(distribution)
        if release_date is None:
            continue
        selected.append(
            {
                "url": url,
                "release_date": release_date,
                "artifact_name": _artifact_name(url),
            }
        )
    selected.sort(key=lambda item: (item["release_date"], item["url"]), reverse=True)
    return selected


def _resolve_artifacts(catalog: dict[str, Any], test_mode: bool) -> list[SourceArtifact]:
    monthly_dataset = _resolve_dataset(catalog, MONTHLY_DATASET_TITLE)
    quarterly_dataset = _resolve_dataset(catalog, QUARTERLY_DATASET_TITLE)

    quarterly_dist = _zip_distributions(quarterly_dataset)
    if not quarterly_dist:
        raise LookupError("Quarterly Part D dataset does not expose ZIP distributions")
    latest_quarterly = quarterly_dist[0]
    quarterly_release_date = latest_quarterly["release_date"]
    cutoff_month = _month_floor(quarterly_release_date)

    artifacts: list[SourceArtifact] = [
        SourceArtifact(
            source_type="quarterly",
            url=latest_quarterly["url"],
            artifact_name=latest_quarterly["artifact_name"],
            release_date=quarterly_release_date,
            cutoff_month=cutoff_month,
        )
    ]

    monthly_dist = _zip_distributions(monthly_dataset)
    monthly_after_cutoff = [
        dist
        for dist in monthly_dist
        if _month_floor(dist["release_date"]) > cutoff_month
    ]
    monthly_after_cutoff.sort(key=lambda item: item["release_date"])
    if test_mode:
        monthly_after_cutoff = monthly_after_cutoff[:2]

    for distribution in monthly_after_cutoff:
        artifacts.append(
            SourceArtifact(
                source_type="monthly",
                url=distribution["url"],
                artifact_name=distribution["artifact_name"],
                release_date=distribution["release_date"],
                cutoff_month=cutoff_month,
            )
        )
    return artifacts


async def _fetch_catalog() -> dict[str, Any]:
    raw = await download_it(CATALOG_URL, local_timeout=180)
    return json.loads(raw)


def _iter_additional_indexes(obj: type) -> list[dict[str, Any]]:
    if hasattr(obj, "__my_additional_indexes__") and obj.__my_additional_indexes__:
        return list(obj.__my_additional_indexes__)
    return []


async def _ensure_indexes(obj: type, schema: str, *, include_additional: bool = True) -> None:
    if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
        cols = ", ".join(obj.__my_index_elements__)
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {obj.__tablename__}_idx_primary "
            f"ON {schema}.{obj.__tablename__} ({cols});"
        )

    if not include_additional:
        return

    for index_data in _iter_additional_indexes(obj):
        elements = index_data.get("index_elements")
        if not elements:
            continue
        index_name = index_data.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
        using = index_data.get("using")
        where = index_data.get("where")
        stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{obj.__tablename__}"
        if using:
            stmt += f" USING {using}"
        stmt += f" ({', '.join(elements)})"
        if where:
            stmt += f" WHERE {where}"
        stmt += ";"
        await db.status(stmt)


async def _drop_additional_indexes(obj: type, schema: str) -> None:
    for index_data in _iter_additional_indexes(obj):
        elements = index_data.get("index_elements")
        if not elements:
            continue
        index_name = index_data.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
        await db.status(f"DROP INDEX IF EXISTS {schema}.{index_name};")


async def _ensure_tables() -> str:
    schema = PartDImportRun.__table__.schema or "mrf"
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    for cls in (
        PartDImportRun,
        PartDFormularySnapshot,
        PartDPharmacyActivity,
        PartDMedicationCost,
        PartDPharmacyActivityStage,
        PartDMedicationCostStage,
    ):
        await db.create_table(cls.__table__, checkfirst=True)
        include_additional = True
        if PARTD_DEFER_ADDITIONAL_INDEXES and cls in (PartDPharmacyActivity, PartDMedicationCost):
            include_additional = False
        await _ensure_indexes(cls, schema, include_additional=include_additional)
    return schema


async def _drop_partd_secondary_indexes(schema: str) -> None:
    for cls in (PartDPharmacyActivity, PartDMedicationCost):
        await _drop_additional_indexes(cls, schema)


async def _ensure_partd_secondary_indexes(schema: str) -> None:
    for cls in (PartDPharmacyActivity, PartDMedicationCost):
        await _ensure_indexes(cls, schema, include_additional=True)


async def _analyze_partd_tables(schema: str) -> None:
    await db.status(f"ANALYZE {schema}.{PartDPharmacyActivity.__tablename__};")
    await db.status(f"ANALYZE {schema}.{PartDMedicationCost.__tablename__};")
    await db.status(f"ANALYZE {schema}.{PartDPharmacyActivityStage.__tablename__};")
    await db.status(f"ANALYZE {schema}.{PartDMedicationCostStage.__tablename__};")


def _snapshot_id(artifact: SourceArtifact) -> str:
    digest = hashlib.sha1(f"{artifact.source_type}|{artifact.url}".encode("utf-8")).hexdigest()[:12]
    return f"{artifact.source_type}:{artifact.release_date.strftime('%Y%m%d')}:{digest}"


async def _truncate_stage_tables(schema: str) -> None:
    await db.status(
        f"TRUNCATE TABLE "
        f"{schema}.{PartDPharmacyActivityStage.__tablename__}, "
        f"{schema}.{PartDMedicationCostStage.__tablename__};"
    )


async def _drop_legacy_partd_tables(schema: str) -> None:
    for table_name in LEGACY_PARTD_TABLES:
        await db.status(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")


async def _materialize_activity_snapshot(schema: str, snapshot_id: str) -> None:
    canonical_table = f"{schema}.{PartDPharmacyActivity.__tablename__}"
    stage_table = f"{schema}.{PartDPharmacyActivityStage.__tablename__}"
    await db.status(
        f"DELETE FROM {canonical_table} WHERE snapshot_id = :snapshot_id;",
        snapshot_id=snapshot_id,
    )
    await db.status(
        f"""
        INSERT INTO {canonical_table} (
            canonical_id,
            snapshot_id,
            npi,
            year,
            medicare_active,
            pharmacy_name,
            address_line1,
            address_line2,
            city,
            state,
            zip_code,
            pharmacy_type,
            mail_order,
            effective_from,
            effective_to,
            source_type,
            plan_ids,
            contract_ids,
            segment_ids
        )
        WITH base AS (
            SELECT
                snapshot_id,
                npi,
                year,
                medicare_active,
                pharmacy_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                pharmacy_type,
                mail_order,
                effective_from,
                effective_to,
                source_type,
                plan_id,
                contract_id,
                segment_id
            FROM {stage_table}
            WHERE snapshot_id = :snapshot_id
        ),
        dedup AS (
            SELECT DISTINCT
                snapshot_id,
                npi,
                year,
                medicare_active,
                pharmacy_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                pharmacy_type,
                mail_order,
                effective_from,
                effective_to,
                source_type,
                plan_id,
                contract_id,
                segment_id
            FROM base
        ),
        grouped AS (
            SELECT
                snapshot_id,
                npi,
                year,
                medicare_active,
                pharmacy_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                pharmacy_type,
                mail_order,
                effective_from,
                effective_to,
                source_type,
                array_agg(plan_id ORDER BY plan_id) AS plan_ids,
                array_agg(contract_id ORDER BY plan_id) AS contract_ids,
                array_agg(segment_id ORDER BY plan_id) AS segment_ids
            FROM dedup
            GROUP BY
                snapshot_id,
                npi,
                year,
                medicare_active,
                pharmacy_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                pharmacy_type,
                mail_order,
                effective_from,
                effective_to,
                source_type
        )
        SELECT
            md5(
                jsonb_build_array(
                    snapshot_id,
                    npi,
                    year,
                    medicare_active,
                    pharmacy_name,
                    address_line1,
                    address_line2,
                    city,
                    state,
                    zip_code,
                    pharmacy_type,
                    mail_order,
                    effective_from,
                    effective_to,
                    source_type
                )::text
            ) AS canonical_id,
            snapshot_id,
            npi,
            year,
            medicare_active,
            pharmacy_name,
            address_line1,
            address_line2,
            city,
            state,
            zip_code,
            pharmacy_type,
            mail_order,
            effective_from,
            effective_to,
            source_type,
            plan_ids,
            contract_ids,
            segment_ids
        FROM grouped;
        """,
        snapshot_id=snapshot_id,
    )
    await db.status(
        f"DELETE FROM {stage_table} WHERE snapshot_id = :snapshot_id;",
        snapshot_id=snapshot_id,
    )


async def _materialize_pricing_snapshot(schema: str, snapshot_id: str) -> None:
    canonical_table = f"{schema}.{PartDMedicationCost.__tablename__}"
    stage_table = f"{schema}.{PartDMedicationCostStage.__tablename__}"
    await db.status(
        f"DELETE FROM {canonical_table} WHERE snapshot_id = :snapshot_id;",
        snapshot_id=snapshot_id,
    )
    await db.status(
        f"""
        INSERT INTO {canonical_table} (
            canonical_id,
            snapshot_id,
            year,
            code_system,
            code,
            normalized_code,
            rxnorm_id,
            ndc11,
            days_supply,
            drug_name,
            tier,
            pharmacy_type,
            mail_order,
            cost_type,
            cost_amount,
            effective_from,
            effective_to,
            source_type,
            plan_ids,
            contract_ids,
            segment_ids
        )
        WITH base AS (
            SELECT
                snapshot_id,
                year,
                code_system,
                code,
                normalized_code,
                rxnorm_id,
                ndc11,
                days_supply,
                drug_name,
                tier,
                pharmacy_type,
                mail_order,
                cost_type,
                cost_amount,
                effective_from,
                effective_to,
                source_type,
                plan_id,
                contract_id,
                segment_id
            FROM {stage_table}
            WHERE snapshot_id = :snapshot_id
        ),
        dedup AS (
            SELECT DISTINCT
                snapshot_id,
                year,
                code_system,
                code,
                normalized_code,
                rxnorm_id,
                ndc11,
                days_supply,
                drug_name,
                tier,
                pharmacy_type,
                mail_order,
                cost_type,
                cost_amount,
                effective_from,
                effective_to,
                source_type,
                plan_id,
                contract_id,
                segment_id
            FROM base
        ),
        grouped AS (
            SELECT
                snapshot_id,
                year,
                code_system,
                code,
                normalized_code,
                rxnorm_id,
                ndc11,
                days_supply,
                drug_name,
                tier,
                pharmacy_type,
                mail_order,
                cost_type,
                cost_amount,
                effective_from,
                effective_to,
                source_type,
                array_agg(plan_id ORDER BY plan_id) AS plan_ids,
                array_agg(contract_id ORDER BY plan_id) AS contract_ids,
                array_agg(segment_id ORDER BY plan_id) AS segment_ids
            FROM dedup
            GROUP BY
                snapshot_id,
                year,
                code_system,
                code,
                normalized_code,
                rxnorm_id,
                ndc11,
                days_supply,
                drug_name,
                tier,
                pharmacy_type,
                mail_order,
                cost_type,
                cost_amount,
                effective_from,
                effective_to,
                source_type
        )
        SELECT
            md5(
                jsonb_build_array(
                    snapshot_id,
                    year,
                    code_system,
                    code,
                    normalized_code,
                    rxnorm_id,
                    ndc11,
                    days_supply,
                    drug_name,
                    tier,
                    pharmacy_type,
                    mail_order,
                    cost_type,
                    cost_amount,
                    effective_from,
                    effective_to,
                    source_type
                )::text
            ) AS canonical_id,
            snapshot_id,
            year,
            code_system,
            code,
            normalized_code,
            rxnorm_id,
            ndc11,
            days_supply,
            drug_name,
            tier,
            pharmacy_type,
            mail_order,
            cost_type,
            cost_amount,
            effective_from,
            effective_to,
            source_type,
            plan_ids,
            contract_ids,
            segment_ids
        FROM grouped;
        """,
        snapshot_id=snapshot_id,
    )
    await db.status(
        f"DELETE FROM {stage_table} WHERE snapshot_id = :snapshot_id;",
        snapshot_id=snapshot_id,
    )


def _entry_kind(name: str) -> str:
    lower = name.lower()
    if not (lower.endswith(".csv") or lower.endswith(".txt")):
        return "skip"
    if "plan information file" in lower:
        return "plan_info"
    if "basic drugs formulary file" in lower:
        return "formulary_map"
    if "pharmacy networks file" in lower:
        return "activity"
    if "pricing file" in lower:
        return "pricing"
    if any(token in lower for token in ("pharmacy", "network")):
        return "activity"
    return "unknown"


def _safe_file_name(name: str) -> str:
    safe = _SAFE_FILE_CHARS.sub("_", name.strip())
    safe = safe.strip("._")
    return safe or "partd_file"


def _extract_data_files(zip_path: Path, workdir: Path) -> list[tuple[Path, str]]:
    extracted_files: list[tuple[Path, str]] = []
    queue: list[tuple[Path, str]] = [(zip_path, zip_path.name)]
    counter = 0

    while queue:
        current_zip_path, zip_label = queue.pop(0)
        with zipfile.ZipFile(current_zip_path) as archive:
            for member_name in archive.namelist():
                if member_name.endswith("/"):
                    continue
                member_basename = Path(member_name).name
                if not member_basename:
                    continue
                logical_name = f"{zip_label}/{member_name}"
                suffix = Path(member_basename).suffix.lower()
                member_lower = member_name.lower()
                is_relevant = any(
                    token in member_lower
                    for token in (
                        "pharmacy network",
                        "pricing file",
                        "basic drugs formulary",
                        "plan information file",
                    )
                )
                if suffix in {".zip", ".txt", ".csv"} and not is_relevant:
                    continue
                counter += 1
                out_name = f"{counter:08d}_{_safe_file_name(member_basename)}"
                out_path = workdir / out_name
                with archive.open(member_name, "r") as src, out_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                if suffix == ".zip":
                    queue.append((out_path, logical_name))
                elif suffix in {".txt", ".csv"}:
                    extracted_files.append((out_path, logical_name))
                else:
                    out_path.unlink(missing_ok=True)
        if current_zip_path != zip_path:
            current_zip_path.unlink(missing_ok=True)

    return extracted_files


def _extract_plan_fields(values: dict[str, Any]) -> tuple[str, str, str, str]:
    contract_id = (str(_row_value(values, "contractid", "contract") or "UNKNOWN").strip() or "UNKNOWN")[:32]
    plan_component = (str(_row_value(values, "planid", "plan", "pbpid", "pbp") or "000").strip() or "000")[:32]
    segment_id = (str(_row_value(values, "segmentid", "segment", "pbpid", "pbpsegmentid") or "000").strip() or "000")[:32]
    plan_id = _compose_plan_id(contract_id, plan_component, segment_id)
    return contract_id, plan_component, segment_id, plan_id


def _load_plan_formulary_map(file_path: Path) -> dict[tuple[str, str, str], str]:
    mapping: dict[tuple[str, str, str], str] = {}
    delimiter = _detect_delimiter(file_path)
    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            values = _row_index(row)
            contract_id, plan_component, segment_id, _ = _extract_plan_fields(values)
            formulary_id = str(_row_value(values, "formularyid", "formulary") or "").strip()
            if not formulary_id:
                continue
            mapping[(contract_id, plan_component, segment_id)] = formulary_id[:32]
    return mapping


def _load_formulary_ndc_map(file_path: Path) -> dict[tuple[str, str], str]:
    mapping: dict[tuple[str, str], str] = {}
    delimiter = _detect_delimiter(file_path)
    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            values = _row_index(row)
            formulary_id = str(_row_value(values, "formularyid", "formulary") or "").strip()
            ndc11 = _normalize_code_digits(_row_value(values, "ndc11", "ndc", "ndccode"))
            rxnorm_id = _normalize_code_digits(_row_value(values, "rxcui", "rxnorm", "rxnormid"))
            if not formulary_id or not ndc11 or not rxnorm_id:
                continue
            key = (formulary_id[:32], ndc11[:16])
            if key not in mapping:
                mapping[key] = rxnorm_id[:32]
            fallback_key = ("*", ndc11[:16])
            if fallback_key not in mapping:
                mapping[fallback_key] = rxnorm_id[:32]
    return mapping


def _activity_row_from_source(
    row: dict[str, Any],
    *,
    snapshot_id: str,
    source_type: str,
    default_date: datetime.date,
) -> dict[str, Any] | None:
    values = _row_index(row)
    npi = _to_npi(_row_value(values, "npi", "pharmacynpi", "pharmacynumber", "pharmacyid", "providernpi"))
    if npi is None:
        return None

    contract_id, _plan_component, segment_id, plan_id = _extract_plan_fields(values)

    year = _to_int(_row_value(values, "year", "contractyear", "planyear"))
    retail_flag = _to_bool(_row_value(values, "pharmacyretail", "retail", "isretail"))
    mail_flag = _to_bool(_row_value(values, "pharmacymail", "mailorder", "mail"))
    in_area_flag = _to_bool(_row_value(values, "inareaflag", "inarea", "insvcarea"))
    active = _to_bool(_row_value(values, "medicareactive", "active", "isactive", "innetwork", "status"))
    if active is None:
        active = any(flag is True for flag in (retail_flag, mail_flag, in_area_flag))
    if active is None:
        active = True

    effective_from = _parse_date(
        _row_value(
            values,
            "effectivefrom",
            "effectivedate",
            "startdate",
            "coveragefrom",
            "month",
            "snapshotmonth",
        )
    ) or default_date
    effective_to = _parse_date(
        _row_value(values, "effectiveto", "enddate", "coverageto")
    )
    pharmacy_type = _row_value(values, "pharmacytype", "networktype", "pharmacynetworktype")
    if not pharmacy_type:
        if retail_flag and mail_flag:
            pharmacy_type = "retail_mail"
        elif retail_flag:
            pharmacy_type = "retail"
        elif mail_flag:
            pharmacy_type = "mail_order"
        else:
            pharmacy_type = "unknown"

    return {
        "snapshot_id": snapshot_id,
        "npi": npi,
        "plan_id": plan_id[:32],
        "contract_id": contract_id[:32],
        "segment_id": segment_id[:32],
        "year": year or effective_from.year,
        "medicare_active": bool(active),
        "pharmacy_name": _row_value(values, "pharmacyname", "name", "providername"),
        "address_line1": _row_value(values, "address1", "addressline1", "firstline"),
        "address_line2": _row_value(values, "address2", "addressline2", "secondline"),
        "city": _row_value(values, "city", "cityname"),
        "state": _row_value(values, "state", "statecode", "statename"),
        "zip_code": _row_value(values, "pharmacyzipcode", "zip", "zipcode", "postalcode"),
        "pharmacy_type": pharmacy_type,
        "mail_order": mail_flag if mail_flag is not None else _to_bool(_row_value(values, "mailorder", "ismailorder", "mailorderflag")),
        "effective_from": effective_from,
        "effective_to": effective_to,
        "source_type": source_type,
    }


def _pricing_rows_from_source(
    row: dict[str, Any],
    *,
    snapshot_id: str,
    source_type: str,
    default_date: datetime.date,
    plan_to_formulary: dict[tuple[str, str, str], str],
    formulary_ndc_to_rxnorm: dict[tuple[str, str], str],
) -> list[dict[str, Any]]:
    values = _row_index(row)
    contract_id, plan_component, segment_id, plan_id = _extract_plan_fields(values)

    year = _to_int(_row_value(values, "year", "contractyear", "planyear"))
    effective_from = _parse_date(
        _row_value(values, "effectivefrom", "effectivedate", "startdate", "month", "snapshotmonth")
    ) or default_date
    effective_to = _parse_date(_row_value(values, "effectiveto", "enddate"))
    days_supply = _to_int(_row_value(values, "dayssupply", "supplydays", "days")) or 0

    rxnorm_id = _normalize_code_digits(_row_value(values, "rxnorm", "rxnormid", "rxcui"))
    ndc11 = _normalize_code_digits(_row_value(values, "ndc11", "ndc", "ndccode", "packagecode", "productcode"))
    if not rxnorm_id and ndc11:
        formulary_id = plan_to_formulary.get((contract_id, plan_component, segment_id))
        if formulary_id:
            rxnorm_id = formulary_ndc_to_rxnorm.get((formulary_id, ndc11))
        if not rxnorm_id:
            rxnorm_id = formulary_ndc_to_rxnorm.get(("*", ndc11))
    code_system = None
    code = None
    normalized_code = None
    if ndc11:
        code_system = "NDC"
        code = ndc11
        normalized_code = ndc11
    elif rxnorm_id:
        code_system = "RXNORM"
        code = rxnorm_id
        normalized_code = rxnorm_id
    else:
        return []

    cost_fields = _match_cost_fields(row)
    if not cost_fields:
        unit_cost = _to_float(_row_value(values, "unitcost"))
        if unit_cost is not None:
            cost_fields = [("unit_cost", unit_cost)]
    if not cost_fields:
        return []

    rows: list[dict[str, Any]] = []
    for cost_type, amount in cost_fields:
        normalized_cost_type = cost_type
        if days_supply:
            normalized_cost_type = f"{cost_type}_days_{days_supply}"
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "plan_id": plan_id[:32],
                "contract_id": contract_id[:32],
                "segment_id": segment_id[:32],
                "year": year or effective_from.year,
                "code_system": code_system,
                "code": code[:64],
                "normalized_code": normalized_code[:64] if normalized_code else None,
                "rxnorm_id": rxnorm_id[:32] if rxnorm_id else None,
                "ndc11": ndc11[:16] if ndc11 else None,
                "days_supply": days_supply,
                "drug_name": _row_value(values, "drugname", "rxname", "genericname", "brandname"),
                "tier": _row_value(values, "tier", "drugtier"),
                "pharmacy_type": _row_value(values, "pharmacytype", "networktype"),
                "mail_order": _to_bool(_row_value(values, "mailorder", "ismailorder", "mailorderflag")),
                "cost_type": normalized_cost_type[:64],
                "cost_amount": amount,
                "effective_from": effective_from,
                "effective_to": effective_to,
                "source_type": source_type,
            }
        )
    return rows


async def _flush_batches(
    activity_batch: list[dict[str, Any]],
    pricing_batch: list[dict[str, Any]],
) -> None:
    tasks: list[Any] = []
    if activity_batch:
        activity_rows = list(activity_batch)
        activity_batch.clear()
        tasks.append(push_objects(activity_rows, PartDPharmacyActivityStage, rewrite=False, use_copy=True))
    if pricing_batch:
        pricing_rows = list(pricing_batch)
        pricing_batch.clear()
        tasks.append(push_objects(pricing_rows, PartDMedicationCostStage, rewrite=False, use_copy=True))
    if tasks:
        await asyncio.gather(*tasks)


async def _import_artifact(
    artifact: SourceArtifact,
    snapshot_id: str,
    *,
    schema: str,
    test_mode: bool,
) -> tuple[int, int]:
    Path(PARTD_WORKDIR).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="partd_", dir=PARTD_WORKDIR) as tmpdir:
        zip_path = str(Path(tmpdir) / artifact.artifact_name)
        await download_it_and_save(artifact.url, zip_path)

        extraction_root = Path(tmpdir) / "expanded"
        extraction_root.mkdir(parents=True, exist_ok=True)
        extracted_files = _extract_data_files(Path(zip_path), extraction_root)
        classified_files = [(path, logical_name, _entry_kind(logical_name)) for path, logical_name in extracted_files]

        activity_count = 0
        pricing_count = 0
        activity_batch: list[dict[str, Any]] = []
        pricing_batch: list[dict[str, Any]] = []
        plan_to_formulary: dict[tuple[str, str, str], str] = {}
        formulary_ndc_to_rxnorm: dict[tuple[str, str], str] = {}
        for file_path, _logical_name, kind in classified_files:
            if kind == "plan_info":
                plan_to_formulary.update(_load_plan_formulary_map(file_path))
            elif kind == "formulary_map":
                formulary_ndc_to_rxnorm.update(_load_formulary_ndc_map(file_path))

        activity_members = [(path, logical_name) for path, logical_name, kind in classified_files if kind == "activity"]
        pricing_members = [(path, logical_name) for path, logical_name, kind in classified_files if kind == "pricing"]
        unknown_members = [(path, logical_name) for path, logical_name, kind in classified_files if kind == "unknown"]
        if not activity_members and unknown_members:
            activity_members = unknown_members[:3]
        if artifact.source_type == "quarterly" and not pricing_members and unknown_members:
            pricing_members = unknown_members[:3]

        processed_rows = 0
        for file_path, _logical_name in activity_members:
            delimiter = _detect_delimiter(file_path)
            with file_path.open("r", encoding="utf-8", errors="replace") as handle:
                reader = csv.DictReader(handle, delimiter=delimiter)
                for row in reader:
                    activity_row = _activity_row_from_source(
                        row,
                        snapshot_id=snapshot_id,
                        source_type=artifact.source_type,
                        default_date=artifact.cutoff_month,
                    )
                    if activity_row is None:
                        continue
                    activity_batch.append(activity_row)
                    activity_count += 1
                    processed_rows += 1
                    if len(activity_batch) >= PARTD_BATCH_SIZE:
                        await _flush_batches(activity_batch, pricing_batch)
                    if test_mode and processed_rows >= PARTD_TEST_MAX_ROWS_PER_FILE:
                        break
            if test_mode and processed_rows >= PARTD_TEST_MAX_ROWS_PER_FILE:
                break

        processed_rows = 0
        for file_path, _logical_name in pricing_members:
            delimiter = _detect_delimiter(file_path)
            with file_path.open("r", encoding="utf-8", errors="replace") as handle:
                reader = csv.DictReader(handle, delimiter=delimiter)
                for row in reader:
                    pricing_rows = _pricing_rows_from_source(
                        row,
                        snapshot_id=snapshot_id,
                        source_type=artifact.source_type,
                        default_date=artifact.cutoff_month,
                        plan_to_formulary=plan_to_formulary,
                        formulary_ndc_to_rxnorm=formulary_ndc_to_rxnorm,
                    )
                    if not pricing_rows:
                        continue
                    pricing_batch.extend(pricing_rows)
                    pricing_count += len(pricing_rows)
                    processed_rows += 1
                    if len(pricing_batch) >= PARTD_BATCH_SIZE:
                        await _flush_batches(activity_batch, pricing_batch)
                    if test_mode and processed_rows >= PARTD_TEST_MAX_ROWS_PER_FILE:
                        break
            if test_mode and processed_rows >= PARTD_TEST_MAX_ROWS_PER_FILE:
                break

        await _flush_batches(activity_batch, pricing_batch)
        await _materialize_activity_snapshot(schema, snapshot_id)
        await _materialize_pricing_snapshot(schema, snapshot_id)
        return activity_count, pricing_count


async def _upsert_run(payload: dict[str, Any]) -> None:
    await push_objects([payload], PartDImportRun, rewrite=True, use_copy=False)


async def _upsert_snapshot(payload: dict[str, Any]) -> None:
    await push_objects([payload], PartDFormularySnapshot, rewrite=True, use_copy=False)


async def partd_formulary_network_start(ctx, task=None):  # pragma: no cover
    task = task or {}
    run_id = _normalize_run_id(task.get("run_id"))
    import_id = _normalize_import_id(task.get("import_id"))
    test_mode = bool(task.get("test_mode"))

    schema = await _ensure_tables()
    await _truncate_stage_tables(schema)
    if PARTD_DEFER_ADDITIONAL_INDEXES and PARTD_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT:
        await _drop_partd_secondary_indexes(schema)

    now = datetime.datetime.utcnow()
    await _upsert_run(
        {
            "run_id": run_id,
            "import_id": import_id,
            "status": "running",
            "started_at": now,
            "finished_at": None,
            "source_summary": {"test_mode": test_mode, "datasets": []},
            "error_text": None,
        }
    )

    try:
        catalog = await _fetch_catalog()
        artifacts = _resolve_artifacts(catalog, test_mode=test_mode)
        await _upsert_run(
            {
                "run_id": run_id,
                "import_id": import_id,
                "status": "running",
                "started_at": now,
                "finished_at": None,
                "source_summary": {
                    "test_mode": test_mode,
                    "datasets": [
                        {
                            "source_type": artifact.source_type,
                            "url": artifact.url,
                            "artifact_name": artifact.artifact_name,
                            "release_date": artifact.release_date.isoformat(),
                            "cutoff_month": artifact.cutoff_month.isoformat(),
                        }
                        for artifact in artifacts
                    ],
                },
                "error_text": None,
            }
        )

        activity_total = 0
        pricing_total = 0
        for artifact in artifacts:
            snapshot_id = _snapshot_id(artifact)
            await _upsert_snapshot(
                {
                    "snapshot_id": snapshot_id,
                    "run_id": run_id,
                    "source_type": artifact.source_type,
                    "source_url": artifact.url,
                    "artifact_name": artifact.artifact_name,
                    "release_date": artifact.release_date,
                    "cutoff_month": artifact.cutoff_month,
                    "status": "running",
                    "row_count_activity": 0,
                    "row_count_pricing": 0,
                    "imported_at": None,
                    "metadata_json": {
                        "release_date": artifact.release_date.isoformat(),
                        "cutoff_month": artifact.cutoff_month.isoformat(),
                    },
                }
            )

            activity_count, pricing_count = await _import_artifact(
                artifact,
                snapshot_id,
                schema=schema,
                test_mode=test_mode,
            )
            activity_total += activity_count
            pricing_total += pricing_count
            await _upsert_snapshot(
                {
                    "snapshot_id": snapshot_id,
                    "run_id": run_id,
                    "source_type": artifact.source_type,
                    "source_url": artifact.url,
                    "artifact_name": artifact.artifact_name,
                    "release_date": artifact.release_date,
                    "cutoff_month": artifact.cutoff_month,
                    "status": "completed",
                    "row_count_activity": activity_count,
                    "row_count_pricing": pricing_count,
                    "imported_at": datetime.datetime.utcnow(),
                    "metadata_json": {
                        "release_date": artifact.release_date.isoformat(),
                        "cutoff_month": artifact.cutoff_month.isoformat(),
                        "activity_rows": activity_count,
                        "pricing_rows": pricing_count,
                    },
                }
            )

        if PARTD_DEFER_ADDITIONAL_INDEXES:
            await _ensure_partd_secondary_indexes(schema)
            await _analyze_partd_tables(schema)
        await _truncate_stage_tables(schema)
        await _drop_legacy_partd_tables(schema)

        await _upsert_run(
            {
                "run_id": run_id,
                "import_id": import_id,
                "status": "completed",
                "started_at": now,
                "finished_at": datetime.datetime.utcnow(),
                "source_summary": {
                    "test_mode": test_mode,
                    "activity_rows": activity_total,
                    "pricing_rows": pricing_total,
                    "datasets": len(artifacts),
                },
                "error_text": None,
            }
        )
        print(
            f"partd_formulary_network completed run_id={run_id} "
            f"activity_rows={activity_total} pricing_rows={pricing_total}",
            flush=True,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        await _truncate_stage_tables(schema)
        if PARTD_DEFER_ADDITIONAL_INDEXES and PARTD_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT:
            try:
                await _ensure_partd_secondary_indexes(schema)
                await _analyze_partd_tables(schema)
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        await _upsert_run(
            {
                "run_id": run_id,
                "import_id": import_id,
                "status": "failed",
                "started_at": now,
                "finished_at": datetime.datetime.utcnow(),
                "source_summary": {"test_mode": test_mode},
                "error_text": str(exc),
            }
        )
        raise


async def partd_formulary_network_finalize(_ctx, task=None):  # pragma: no cover
    task = task or {}
    run_id = _normalize_run_id(task.get("run_id"))
    import_id = _normalize_import_id(task.get("import_id"))
    now = datetime.datetime.utcnow()
    await _upsert_run(
        {
            "run_id": run_id,
            "import_id": import_id,
            "status": "completed",
            "started_at": now,
            "finished_at": now,
            "source_summary": {"finalized_via_queue": True},
            "error_text": None,
        }
    )


async def main(test_mode: bool = False, import_id: str | None = None):  # pragma: no cover
    run_id = _normalize_run_id(None)
    normalized_import_id = _normalize_import_id(import_id)
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    await redis.enqueue_job(
        "partd_formulary_network_start",
        {
            "run_id": run_id,
            "import_id": normalized_import_id,
            "test_mode": bool(test_mode),
        },
        _queue_name=PARTD_QUEUE_NAME,
    )
    print(
        json.dumps(
            {
                "status": "queued",
                "run_id": run_id,
                "import_id": normalized_import_id,
                "queue_name": PARTD_QUEUE_NAME,
                "test_mode": bool(test_mode),
            },
            ensure_ascii=True,
        )
    )
    return run_id


async def finish_main(
    import_id: str,
    run_id: str,
    test_mode: bool = False,
    manifest_path: str | None = None,
):  # pragma: no cover
    del manifest_path
    normalized_run_id = _normalize_run_id(run_id)
    normalized_import_id = _normalize_import_id(import_id)
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    await redis.enqueue_job(
        "partd_formulary_network_finalize",
        {
            "run_id": normalized_run_id,
            "import_id": normalized_import_id,
            "test_mode": bool(test_mode),
        },
        _queue_name=PARTD_FINISH_QUEUE_NAME,
    )
    print(
        json.dumps(
            {
                "status": "queued",
                "run_id": normalized_run_id,
                "import_id": normalized_import_id,
                "queue_name": PARTD_FINISH_QUEUE_NAME,
                "test_mode": bool(test_mode),
            },
            ensure_ascii=True,
        )
    )


async def startup(_ctx):  # pragma: no cover
    await db_startup(_ctx)


async def shutdown(_ctx):  # pragma: no cover
    return None
