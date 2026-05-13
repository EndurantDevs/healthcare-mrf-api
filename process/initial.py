# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=too-many-branches,too-many-locals,too-many-statements,too-many-nested-blocks,broad-exception-caught,too-many-return-statements,not-callable

import asyncio
import datetime
import glob
import json
import logging
import os
import sys
import tempfile
import zipfile
from pathlib import Path, PurePath
from urllib.parse import urlparse

import ijson
import pylightxl as xl
from aiocsv import AsyncDictReader
from aiofile import async_open
from arq import create_pool
from async_unzip.unzipper import unzip
from asyncpg import DuplicateTableError
from dateutil.parser import parse as parse_date
from sqlalchemy import func, literal, or_, select
from sqlalchemy.exc import IntegrityError, ProgrammingError

from db.models import (ImportHistory, ImportLog, Issuer, NPIAddress,
                       NPIDataOtherIdentifier, NPIDataTaxonomyGroup, MRFAddress,
                       MRFAddressEvidence, Plan, PlanBenefitsMarketplace,
                       PlanDrugRaw, PlanDrugStats, PlanDrugTierStats,
                       PlanFormulary, PlanNetworkTierRaw, PlanNPIRaw,
                       PlanTransparency, db)
from process.ext.utils import (download_it_and_save, ensure_database,
                               flush_error_log, get_import_schema, log_error,
                               make_class, my_init_db, print_time_info,
                               push_objects, return_checksum)
from process.plan_summary import rebuild_plan_search_summary
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

TEST_PLAN_TRANSPARENCY_ROWS = 25
TEST_UNKNOWN_STATE_ROWS = 50
TEST_PLAN_URLS = 2
TEST_PROVIDER_URLS = 2
TEST_FORMULARY_URLS = 2
TEST_PLAN_RECORDS = 30
TEST_PROVIDER_RECORDS = 60
TEST_DRUG_RECORDS = 60
TEST_MIN_PLAN_COUNT = 1

PLAN_ID_MAX_LENGTH = getattr(Plan.__table__.c.plan_id.type, "length", 14)

logger = logging.getLogger(__name__)
MRF_QUEUE_NAME = "arq:MRF"
MRF_FINISH_QUEUE_NAME = "arq:MRF_finish"


def is_test_mode(ctx: dict) -> bool:
    return bool(ctx.get("context", {}).get("test_mode"))


def _truthy(value, truthy=("yes", "y", "true")) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in truthy
    return bool(value)


def _clean_name_part(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _extract_plan_years(plan_obj: dict) -> list[int]:
    raw_years = plan_obj.get("years")
    if raw_years is None:
        raw_years = plan_obj.get("year")
    if raw_years is None:
        return []

    if isinstance(raw_years, (list, tuple, set)):
        candidates = list(raw_years)
    else:
        candidates = [raw_years]

    years: list[int] = []
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            if isinstance(candidate, int):
                year_val = candidate
            elif isinstance(candidate, float):
                if not candidate.is_integer():
                    continue
                year_val = int(candidate)
            else:
                text = str(candidate).strip()
                if not text:
                    continue
                if text.endswith(".0"):
                    text = text[:-2]
                year_val = int(text)
        except (TypeError, ValueError):
            continue
        if 1900 <= year_val <= 2200 and year_val not in years:
            years.append(year_val)

    return years


def _normalize_upper(value):
    cleaned = _clean_name_part(value)
    return cleaned.upper() if cleaned else None


def _coerce_text(value):
    cleaned = _clean_name_part(value)
    return cleaned or None


def _parse_timestamp(value):
    if not value:
        return None
    return datetime.datetime.combine(parse_date(value, fuzzy=True), datetime.datetime.min.time())


def _create_index_sql(table_name: str, index: dict, db_schema: str) -> str:
    index_name = index.get("name", "_".join(index.get("index_elements")))
    using = f"USING {index.get('using')} " if index.get("using") else ""
    unique = "UNIQUE " if index.get("unique") else ""
    where = f" WHERE {index.get('where')}" if index.get("where") else ""
    return (
        f"CREATE {unique}INDEX IF NOT EXISTS {table_name}_idx_{index_name} "
        f"ON {db_schema}.{table_name} {using}({', '.join(index.get('index_elements'))}){where};"
    )


async def _create_named_indexes(stage_cls, db_schema: str) -> None:
    for attr_name in ("__my_initial_indexes__", "__my_additional_indexes__"):
        for index in getattr(stage_cls, attr_name, []) or []:
            await db.status(_create_index_sql(stage_cls.__tablename__, index, db_schema))


def _benefit_label_from_key(key):
    cleaned = _coerce_text(key) or "unknown"
    return cleaned.replace("_", " ").replace("-", " ").strip().title()


def _serialize_jsonable(value):
    try:
        return json.loads(json.dumps(value))
    except (TypeError, ValueError):
        return {"value": str(value)}


def _issuer_display_name(issuer_id, issuer_name=None, issuer_marketing_name=None, issuer_url=None):
    for candidate in (issuer_name, issuer_marketing_name):
        value = _coerce_text(candidate)
        if value and value.upper() != "N/A":
            return value

    host = urlparse(_coerce_text(issuer_url) or "").hostname
    if host:
        return host.lower()

    return str(issuer_id)


def _normalize_benefit_value(value):
    if isinstance(value, bool):
        return ("true" if value else "false", value, None, None)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (str(value), None, float(value), None)
    if isinstance(value, str):
        cleaned = value.strip()
        return (cleaned or None, None, None, None)
    if value is None:
        return (None, None, None, None)
    return (json.dumps(value, sort_keys=True), None, None, _serialize_jsonable(value))


def _iter_marketplace_benefit_entries(item, position):
    if isinstance(item, dict):
        named_key = _coerce_text(item.get("benefit_name") or item.get("name") or item.get("key"))
        has_value_field = "value" in item
        extra_keys = {
            key for key in item.keys()
            if key not in {"benefit_name", "name", "key", "label", "display_name", "value"}
        }
        if named_key and has_value_field and not extra_keys:
            yield {
                "benefit_name": named_key,
                "benefit_label": _coerce_text(item.get("label") or item.get("display_name")) or _benefit_label_from_key(named_key),
                "benefit_value": item.get("value"),
                "benefit_position": position,
                "benefit_item_json": _serialize_jsonable(item),
            }
            return
        for key, value in item.items():
            yield {
                "benefit_name": str(key).strip(),
                "benefit_label": _benefit_label_from_key(key),
                "benefit_value": value,
                "benefit_position": position,
                "benefit_item_json": _serialize_jsonable(item),
            }
        return

    yield {
        "benefit_name": f"benefit_{position}",
        "benefit_label": f"Benefit {position + 1}",
        "benefit_value": item,
        "benefit_position": position,
        "benefit_item_json": _serialize_jsonable(item),
    }


def _normalize_marketplace_benefits(plan_id, year, issuer_id, benefits, last_updated_on):
    if isinstance(benefits, dict):
        benefits = [benefits]
    if not isinstance(benefits, list):
        return []

    rows = []
    for position, item in enumerate(benefits):
        for entry in _iter_marketplace_benefit_entries(item, position):
            benefit_value_text, benefit_value_bool, benefit_value_number, benefit_value_json = _normalize_benefit_value(
                entry["benefit_value"]
            )
            checksum = return_checksum(
                [
                    plan_id,
                    year,
                    entry["benefit_position"],
                    entry["benefit_name"],
                    json.dumps(entry["benefit_item_json"], sort_keys=True),
                ]
            )
            rows.append(
                {
                    "plan_id": plan_id,
                    "year": int(year),
                    "issuer_id": issuer_id,
                    "benefit_position": entry["benefit_position"],
                    "benefit_name": entry["benefit_name"],
                    "benefit_label": entry["benefit_label"],
                    "benefit_value_text": benefit_value_text,
                    "benefit_value_bool": benefit_value_bool,
                    "benefit_value_number": benefit_value_number,
                    "benefit_value_json": benefit_value_json,
                    "benefit_item_json": entry["benefit_item_json"],
                    "last_updated_on": last_updated_on,
                    "checksum": checksum,
                }
            )
    return rows


def _normalize_marketplace_address_entry(address):
    if not isinstance(address, dict):
        return None

    first_line = _coerce_text(address.get("address") or address.get("address1") or address.get("line1"))
    city_name = _normalize_upper(address.get("city"))
    state_name = _normalize_upper(address.get("state"))
    postal_code = _coerce_text(address.get("zip") or address.get("postal_code"))
    if not first_line or not city_name or not state_name or not postal_code:
        return None

    second_line = _coerce_text(
        address.get("address_2") or address.get("address2") or address.get("line2")
    )
    country_code = _normalize_upper(address.get("country") or address.get("country_code"))
    telephone_number = _coerce_text(address.get("phone") or address.get("telephone"))
    checksum = return_checksum(
        [
            first_line or "",
            second_line or "",
            city_name or "",
            state_name or "",
            postal_code or "",
            country_code or "",
        ]
    )
    return {
        "checksum": checksum,
        "first_line": first_line,
        "second_line": second_line,
        "city_name": city_name,
        "state_name": state_name,
        "postal_code": postal_code,
        "country_code": country_code,
        "telephone_number": telephone_number,
        "formatted_address": ", ".join(
            [
                part
                for part in (
                    " ".join(part for part in (first_line, second_line) if part).strip(),
                    " ".join(part for part in (city_name, state_name, postal_code) if part).strip(),
                )
                if part
            ]
        ),
    }
def _build_mrf_address_rows(res, network_tiers, import_id, source_url, last_updated_on, issuer_lookup=None):
    addresses = res.get("addresses", []) or []
    if not isinstance(addresses, list):
        return [], []

    issuer_lookup = issuer_lookup or {}
    npi = int(res["npi"])
    address_type = "practice"
    address_rows = {}
    evidence_rows = {}
    try:
        import_date_value = datetime.datetime.strptime(str(import_id)[:8], "%Y%m%d").date()
    except (TypeError, ValueError):
        import_date_value = None

    for address in addresses:
        normalized = _normalize_marketplace_address_entry(address)
        if not normalized:
            continue
        address_key = (npi, address_type, normalized["checksum"])
        address_record_id_values = []
        issuer_ids = []
        issuer_names = []
        import_ids = []
        import_dates = []
        sources = []
        source_urls = []
        for network in network_tiers.values():
            issuer_id = network["issuer_id"]
            issuer_name = issuer_lookup.get(issuer_id) or str(issuer_id)
            source_record_id = ":".join(
                [
                    str(npi),
                    address_type,
                    str(normalized["checksum"]),
                    str(network["checksum_network"]),
                ]
            )
            evidence_checksum = return_checksum(
                [
                    npi,
                    address_type,
                    normalized["checksum"],
                    network["issuer_id"],
                    network["year"],
                    network["checksum_network"],
                    import_id,
                    source_url,
                    source_record_id,
                ]
            )
            evidence_rows[evidence_checksum] = {
                "evidence_checksum": evidence_checksum,
                "npi": npi,
                "type": address_type,
                "checksum": normalized["checksum"],
                "issuer_id": issuer_id,
                "issuer_name": issuer_name,
                "year": network["year"],
                "checksum_network": network["checksum_network"],
                "network_tier": network["network_tier"],
                "import_id": str(import_id),
                "import_date": import_date_value,
                "address_source": "marketplace_provider",
                "source_table": "plan_npi_raw",
                "source_url": source_url,
                "source_record_id": source_record_id,
                "first_line": normalized["first_line"],
                "second_line": normalized["second_line"],
                "city_name": normalized["city_name"],
                "state_name": normalized["state_name"],
                "postal_code": normalized["postal_code"],
                "country_code": normalized["country_code"],
                "telephone_number": normalized["telephone_number"],
                "observed_at": last_updated_on,
            }
            address_record_id_values.append(source_record_id)
            issuer_ids.append(issuer_id)
            issuer_names.append(issuer_name)
            import_ids.append(str(import_id))
            if import_date_value:
                import_dates.append(import_date_value)
            sources.append("marketplace_provider")
            if source_url:
                source_urls.append(str(source_url))

        address_rows[address_key] = {
            "npi": npi,
            "type": address_type,
            "checksum": normalized["checksum"],
            "first_line": normalized["first_line"],
            "second_line": normalized["second_line"],
            "city_name": normalized["city_name"],
            "state_name": normalized["state_name"],
            "postal_code": normalized["postal_code"],
            "country_code": normalized["country_code"],
            "telephone_number": normalized["telephone_number"],
            "formatted_address": normalized["formatted_address"],
            "date_added": last_updated_on.date() if last_updated_on else None,
            "source_count": len(set(address_record_id_values)),
            "address_sources": sorted(set(sources)),
            "source_record_ids": sorted(set(address_record_id_values)),
            "source_import_ids": sorted(set(import_ids)),
            "source_import_dates": sorted(set(import_dates)),
            "source_issuer_ids": sorted(set(issuer_ids)),
            "source_issuer_names": sorted(set(issuer_names)),
            "source_urls": sorted(set(source_urls)),
        }

    return list(address_rows.values()), list(evidence_rows.values())


async def _refresh_mrf_address_summary(import_date: str, db_schema: str) -> None:
    address_cls = make_class(MRFAddress, import_date, schema_override=db_schema)
    evidence_cls = make_class(MRFAddressEvidence, import_date, schema_override=db_schema)
    await db.status(
        f"""
        UPDATE {db_schema}.{address_cls.__tablename__} AS a
           SET address_sources = COALESCE(src.address_sources, ARRAY[]::varchar[]),
               source_record_ids = COALESCE(src.source_record_ids, ARRAY[]::varchar[]),
               source_import_ids = COALESCE(src.source_import_ids, ARRAY[]::varchar[]),
               source_import_dates = COALESCE(src.source_import_dates, ARRAY[]::date[]),
               source_issuer_ids = COALESCE(src.source_issuer_ids, ARRAY[]::integer[]),
               source_issuer_names = COALESCE(src.source_issuer_names, ARRAY[]::varchar[]),
               source_urls = COALESCE(src.source_urls, ARRAY[]::varchar[]),
               source_count = COALESCE(src.source_count, 0)
          FROM (
                SELECT
                    npi,
                    type,
                    checksum,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT import_id ORDER BY import_id), NULL)::varchar[] AS source_import_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT import_date ORDER BY import_date), NULL)::date[] AS source_import_dates,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT issuer_id ORDER BY issuer_id), NULL)::integer[] AS source_issuer_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT issuer_name ORDER BY issuer_name), NULL)::varchar[] AS source_issuer_names,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_url ORDER BY source_url), NULL)::varchar[] AS source_urls,
                    COUNT(DISTINCT evidence_checksum)::int AS source_count
                FROM {db_schema}.{evidence_cls.__tablename__}
                GROUP BY npi, type, checksum
          ) AS src
         WHERE a.npi = src.npi
           AND a.type = src.type
           AND a.checksum = src.checksum;
        """
    )


async def _prepare_import_tables(import_date: str, test_mode: bool) -> None:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)

    await db.create_table(ImportHistory.__table__, checkfirst=True)
    if hasattr(ImportHistory, "__my_index_elements__") and ImportHistory.__my_index_elements__:
        cols = ", ".join(ImportHistory.__my_index_elements__)
        try:
            await db.status(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                + f"{ImportHistory.__tablename__}_idx_primary ON "
                + f"{db_schema}.{ImportHistory.__tablename__} ({cols});"
            )
        except IntegrityError:
            pass

    for cls in (
        Issuer,
        Plan,
        PlanFormulary,
        PlanBenefitsMarketplace,
        PlanTransparency,
        PlanDrugRaw,
        PlanDrugStats,
        PlanDrugTierStats,
        ImportLog,
        PlanNPIRaw,
        PlanNetworkTierRaw,
        MRFAddress,
        MRFAddressEvidence,
    ):
        obj = make_class(cls, import_date, schema_override=db_schema)
        try:
            await db.status("DROP TABLE IF EXISTS " + f"{db_schema}.{obj.__tablename__};")
        except ProgrammingError:
            pass
        try:
            await db.create_table(obj.__table__, checkfirst=True)
        except (ProgrammingError, DuplicateTableError, IntegrityError):
            pass
        if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
            cols = ", ".join(obj.__my_index_elements__)
            try:
                await db.status(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    + f"{obj.__tablename__}_idx_primary ON "
                    + f"{db_schema}.{obj.__tablename__} ({cols});"
                )
            except IntegrityError:
                pass
        if cls in {PlanBenefitsMarketplace, MRFAddress, MRFAddressEvidence}:
            await _create_named_indexes(obj, db_schema)

    print("Preparing done")


async def process_plan(ctx, task):
    """
    The process_plan function is responsible for downloading the plan data from the CMS PUF,
        parsing it and saving to a database.

    :param ctx: Pass the import_date to the function
    :param task: Pass the task object to the function
    :return: 1 if there is no error
    """
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    test_mode = is_test_mode(ctx)
    plan_limit = TEST_PLAN_RECORDS if test_mode else None
    await ensure_database(test_mode)

    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplan = make_class(Plan, import_date, schema_override=db_schema)
    myplanformulary = make_class(PlanFormulary, import_date, schema_override=db_schema)
    myplanbenefitsmarketplace = make_class(PlanBenefitsMarketplace, import_date, schema_override=db_schema)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)

    print("Starting Plan data download: ", task.get("url"))
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get("url"))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        try:
            await download_it_and_save(
                task.get("url"),
                tmp_filename,
                context={"issuer_array": task["issuer_array"], "source": "plans"},
                logger=myimportlog,
            )
        except Exception as exc:
            logger.warning("Failed to download plan data from %s: %s", task.get("url"), exc)
            return

        async with async_open(tmp_filename, "rb") as afp:
            plan_obj = []
            planformulary_obj = []
            planbenefitsmarketplace_obj = []
            count = 0
            processed_plans = 0
            stop_processing = False
            try:
                async for res in ijson.items(afp, "item", use_float=True):
                    if stop_processing:
                        break
                    if not isinstance(res, dict):
                        await log_error(
                            "err",
                            f"Malformed plan entry type: {type(res).__name__}. Expected object.",
                            task.get("issuer_array"),
                            task.get("url"),
                            "plans",
                            "json",
                            myimportlog,
                        )
                        continue
                    plan_id_raw = res.get("plan_id")
                    plan_id_value = str(plan_id_raw).strip() if plan_id_raw is not None else ""
                    years = _extract_plan_years(res)
                    if not years:
                        await log_error(
                            "err",
                            f"Mandatory field `years` is not present or incorrect. Plan ID: "
                            f"{plan_id_value or 'UNKNOWN'}",
                            task.get("issuer_array"),
                            task.get("url"),
                            "plans",
                            "json",
                            myimportlog,
                        )
                        continue
                    if PLAN_ID_MAX_LENGTH and len(plan_id_value) > PLAN_ID_MAX_LENGTH:
                        for year in years:
                            await log_error(
                                "err",
                                f"Plan ID length exceeds {PLAN_ID_MAX_LENGTH} characters. "
                                f"Plan ID: {plan_id_value}, year: {year}",
                                task.get("issuer_array"),
                                task.get("url"),
                                "plans",
                                "json",
                                myimportlog,
                            )
                        continue
                    for year in years:
                        if stop_processing:
                            break
                        try:
                            for k in (
                                "plan_id",
                                "plan_id_type",
                                "marketing_name",
                                "summary_url",
                                "plan_contact",
                                "network",
                                "formulary",
                                "last_updated_on",
                            ):
                                if k not in res or res[k] is None:
                                    await log_error(
                                        "err",
                                        f"Mandatory field `{k}` is not present or incorrect. Plan ID: "
                                        f"{plan_id_value or 'UNKNOWN'}, year: {year}",
                                        task.get("issuer_array"),
                                        task.get("url"),
                                        "plans",
                                        "json",
                                        myimportlog,
                                    )

                            if int(plan_id_value[:5]) not in task.get("issuer_array"):
                                await log_error(
                                    "err",
                                    f"File describes the issuer that is not defined/allowed by the index "
                                    f"CMS PUF."
                                    f"Issuer of Plan: {int(plan_id_value[:5])}. Allowed issuer list: "
                                    f"{', '.join([str(x) for x in task.get('issuer_array')])}"
                                    f"Plan ID: {plan_id_value}, year: {year}",
                                    task.get("issuer_array"),
                                    task.get("url"),
                                    "plans",
                                    "json",
                                    myimportlog,
                                )

                            network_entries = res.get("network", [])
                            if isinstance(network_entries, dict):
                                network_entries = [network_entries]
                            formulary_entries = res.get("formulary", [])
                            if isinstance(formulary_entries, dict):
                                formulary_entries = [formulary_entries]
                            if not isinstance(formulary_entries, list):
                                formulary_entries = []
                            benefits_entries = res.get("benefits", [])
                            if isinstance(benefits_entries, dict):
                                benefits_entries = [benefits_entries]
                            if not isinstance(benefits_entries, list):
                                benefits_entries = []
                            last_updated_on = _parse_timestamp(res["last_updated_on"])

                            obj = {
                                "plan_id": plan_id_value,
                                "plan_id_type": res["plan_id_type"],
                                "year": int(year),
                                "issuer_id": int(plan_id_value[:5]),
                                "state": str(plan_id_value[5:7]).upper(),
                                "marketing_name": res["marketing_name"],
                                "summary_url": res["summary_url"],
                                "marketing_url": res.get("marketing_url", ""),
                                "formulary_url": res.get("formulary_url", ""),
                                "plan_contact": res["plan_contact"],
                                "network": [(k["network_tier"]) for k in network_entries if isinstance(k, dict)],
                                "benefits": [json.dumps(x) for x in benefits_entries],
                                "last_updated_on": last_updated_on,
                                "checksum": return_checksum([plan_id_value.lower(), year], crc=32),
                            }
                            plan_obj.append(obj)
                            planbenefitsmarketplace_obj.extend(
                                _normalize_marketplace_benefits(
                                    plan_id_value,
                                    year,
                                    int(plan_id_value[:5]),
                                    benefits_entries,
                                    last_updated_on,
                                )
                            )
                            processed_plans += 1
                            if plan_limit and processed_plans >= plan_limit:
                                stop_processing = True
                                break
                            if count > int(os.environ.get("HLTHPRT_SAVE_PER_PACK", 50)):
                                await asyncio.gather(
                                    push_objects(plan_obj, myplan),
                                    push_objects(planbenefitsmarketplace_obj, myplanbenefitsmarketplace),
                                )
                                plan_obj.clear()
                                planbenefitsmarketplace_obj.clear()
                                count = 0
                            else:
                                count += 1
                        except Exception as exc:
                            logger.debug(
                                "Skipping malformed plan entry plan_id=%s year=%s: %s",
                                res.get("plan_id"),
                                year,
                                exc,
                            )

                    count = 0
                    for year in years:
                        if stop_processing:
                            break
                        if formulary_entries:
                            for formulary in formulary_entries:
                                if (
                                    isinstance(formulary, dict)
                                    and ("cost_sharing" in formulary)
                                    and formulary["cost_sharing"]
                                ):
                                    try:
                                        for k in ("drug_tier", "mail_order"):
                                            if k not in formulary or formulary[k] is None:
                                                await log_error(
                                                    "err",
                                                    f"Mandatory field `{k}` in Formulary (`formulary`) "
                                                    f"sub-type is "
                                                    f"not present or "
                                                    f"incorrect. Plan ID: "
                                                    f"{plan_id_value}, year: {year}",
                                                    task.get("issuer_array"),
                                                    task.get("url"),
                                                    "plans",
                                                    "json",
                                                    myimportlog,
                                                )
                                        for cost_sharing in formulary["cost_sharing"]:
                                            for k in (
                                                "pharmacy_type",
                                                "copay_amount",
                                                "copay_opt",
                                                "coinsurance_rate",
                                                "coinsurance_opt",
                                            ):
                                                if k not in cost_sharing:
                                                    await log_error(
                                                        "err",
                                                        f"Mandatory field `{k}` in Cost Sharing ("
                                                        f"`cost_sharing`) "
                                                        f"sub-type is not present or "
                                                        f"incorrect. Plan ID: "
                                                        f"{plan_id_value}, year: {year}",
                                                        task.get("issuer_array"),
                                                        task.get("url"),
                                                        "plans",
                                                        "json",
                                                        myimportlog,
                                                    )
                                            obj = {
                                                "plan_id": plan_id_value,
                                                "year": int(year),
                                                "drug_tier": formulary.get("drug_tier", ""),
                                                "mail_order": bool(formulary.get("mail_order")),
                                                "pharmacy_type": cost_sharing.get("pharmacy_type", ""),
                                                "copay_amount": (
                                                    float(cost_sharing.get("copay_amount"))
                                                    if cost_sharing.get("copay_amount", None) is not None
                                                    else None
                                                ),
                                                "copay_opt": cost_sharing.get("copay_opt", ""),
                                                "coinsurance_rate": (
                                                    float(cost_sharing.get("coinsurance_rate"))
                                                    if cost_sharing.get("coinsurance_rate", None) is not None
                                                    else None
                                                ),
                                                "coinsurance_opt": cost_sharing.get("coinsurance_opt", ""),
                                            }
                                            planformulary_obj.append(obj)
                                            if count > int(os.environ.get("HLTHPRT_SAVE_PER_PACK", 50)):
                                                await push_objects(planformulary_obj, myplanformulary)
                                                planformulary_obj.clear()
                                                count = 0
                                            else:
                                                count += 1
                                    except Exception as exc:
                                        logger.debug(
                                            "Skipping cost sharing entry for plan %s year=%s: %s",
                                            res.get("plan_id"),
                                            year,
                                            exc,
                                        )

                                    planformulary_obj.clear()
                                    count = 0
                                else:
                                    await log_error(
                                        "warn",
                                        f"Recommended field 'cost_sharing' is not present or incorrect. "
                                        f"Plan ID: {plan_id_value}, year: {year}",
                                        task.get("issuer_array"),
                                        task.get("url"),
                                        "plans",
                                        "json",
                                        myimportlog,
                                    )
                        else:
                            await log_error(
                                "err",
                                f"Mandatory field 'formulary' is not present or incorrect. Plan ID: "
                                f"{plan_id_value}, year: {year}",
                                task.get("issuer_array"),
                                task.get("url"),
                                "plans",
                                "json",
                                myimportlog,
                            )
                    if stop_processing:
                        break

                await asyncio.gather(
                    push_objects(plan_obj, myplan),
                    push_objects(planformulary_obj, myplanformulary),
                    push_objects(planbenefitsmarketplace_obj, myplanbenefitsmarketplace),
                )
            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "plans",
                    "json",
                    myimportlog,
                )
                return
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "plans",
                    "json",
                    myimportlog,
                )
                return
    await flush_error_log(myimportlog)
    return 1


async def process_provider(ctx, task):
    """
    The process_provider function is responsible for downloading the provider data from the CMS PUF website,
        parsing it into a JSON object, and then inserting that data into our database.

        The function takes in two arguments: ctx and task. Ctx is a dictionary containing information about the
        current import date (the date of which we are importing data). Task contains information about what URL to
        download from as well as what issuers are allowed to be imported based on our index file.

    :param ctx: Pass the import_date value to the function
    :param task: Pass the url of the file to be downloaded
    :return: 1 if the file is successfully processed
    """
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    test_mode = is_test_mode(ctx)
    provider_limit = TEST_PROVIDER_RECORDS if test_mode else None
    await ensure_database(test_mode)

    current_year = datetime.datetime.now().year
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    myissuer = make_class(Issuer, import_date, schema_override=db_schema)
    myplan_npi = make_class(PlanNPIRaw, import_date, schema_override=db_schema)
    myplan_networktier = make_class(PlanNetworkTierRaw, import_date, schema_override=db_schema)
    mymrfaddress = make_class(MRFAddress, import_date, schema_override=db_schema)
    mymrfaddressevidence = make_class(MRFAddressEvidence, import_date, schema_override=db_schema)
    issuer_lookup = {}
    if task.get("issuer_array"):
        issuer_rows = await (
            db.select(myissuer.issuer_id, myissuer.issuer_name, myissuer.issuer_marketing_name, myissuer.mrf_url)
            .where(myissuer.issuer_id.in_(task["issuer_array"]))
            .all()
        )
        issuer_lookup = {
            int(row.issuer_id): _issuer_display_name(
                row.issuer_id,
                issuer_name=row.issuer_name,
                issuer_marketing_name=row.issuer_marketing_name,
                issuer_url=row.mrf_url,
            )
            for row in issuer_rows
            if row.issuer_id is not None
        }

    print("Starting Provider file data download: ", task.get("url"))
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get("url"))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        try:
            await download_it_and_save(
                task.get("url"),
                tmp_filename,
                context={"issuer_array": task["issuer_array"], "source": "providers"},
                logger=myimportlog,
            )
        except Exception as exc:
            logger.warning("Failed to download provider data from %s: %s", task.get("url"), exc)
            return
        async with async_open(tmp_filename, "rb") as afp:
            plan_npi_obj_dict = {}
            plan_network_year = {}
            mrf_address_obj_dict = {}
            mrf_address_evidence_dict = {}
            count = 0
            processed_providers = 0
            try:
                async for res in ijson.items(afp, "item", use_float=True):
                    if provider_limit and processed_providers >= provider_limit:
                        break
                    my_network_tiers = {}
                    not_good = False
                    my_years = set()
                    if not res or not res.get("plans"):
                        continue
                    for plan in res["plans"]:
                        # try:
                        #     for k in (
                        #             'npi', 'type', 'plans', 'addresses', 'last_updated_on'):
                        #         if not (k in res and res[k] is not None):
                        #             await log_error('err',
                        #                             f"Mandatory field `{k}` for providers data is not present or "
                        #                             f"incorrect. Plan ID: "
                        #                             f"{plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                             task.get('issuer_array'), task.get('url'), 'plans', 'json',
                        #                             myimportlog)

                        # if not int(plan['plan_id'][:5]) in task.get('issuer_array'):
                        #     await log_error('err',
                        #                     f"File describes the issuer that is not defined/allowed by the index "
                        #                     f"CMS PUF."
                        #                     f"Issuer of Plan: {int(plan['plan_id'][:5])}. Allowed issuer list: "
                        #                     f"{''.join([str(x) for x in task.get('issuer_array')])}"
                        #                     f"Plan ID: {plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                     task.get('issuer_array'), task.get('url'), 'providers', 'json',
                        #                     myimportlog)
                        npi_raw = res.get("npi", "")
                        has_valid_npi = npi_raw and npi_raw.isdigit() and 0 < int(npi_raw) < 4294967295
                        has_plan_id = bool(plan.get("plan_id"))
                        has_years = bool(plan.get("years"))
                        if not has_valid_npi or not has_plan_id or not has_years:
                            not_good = True
                            break
                        if len(plan["plan_id"]) <= 12 or len(plan["plan_id"]) > 14:
                            continue

                        for x in plan.get("years", []):
                            if x and (current_year + 1 >= int(x) >= current_year):
                                my_years.add(int(x))

                        issuer_id = int(plan["plan_id"][0:5])
                        for year in my_years:
                            checksum_plan = return_checksum([plan["plan_id"], plan["network_tier"], issuer_id, year])
                            checksum_network = return_checksum([plan["network_tier"], issuer_id, year])
                            plan_network_year[checksum_plan] = {
                                "plan_id": plan["plan_id"],
                                "network_tier": plan["network_tier"],
                                "issuer_id": issuer_id,
                                "year": year,
                                "checksum_network": checksum_network,
                            }
                            my_network_tiers[checksum_network] = {
                                "network_tier": plan["network_tier"],
                                "issuer_id": issuer_id,
                                "year": year,
                                "checksum_network": checksum_network,
                            }
                    if not_good:
                        continue

                    name = res.get("name", {})
                    if not name:
                        name = {}
                    languages = res.get("languages", [])
                    if not languages:
                        languages = []
                    addresses = res.get("addresses", [])
                    if not addresses:
                        addresses = []
                    last_updated_on = _parse_timestamp(res["last_updated_on"])

                    obj = {
                        "npi": int(res["npi"]),
                        "network_tier": "",
                        "checksum_network": "",
                        "year": 0,
                        "issuer_id": 0,
                        "name_or_facility_name": "",
                        "specialty_or_facility_type": [],
                        "type": str(res.get("type", "")),
                        "prefix": name.get("prefix", None),
                        "first_name": name.get("first", None),
                        "middle_name": name.get("middle", None),
                        "last_name": name.get("last", None),
                        "suffix": name.get("suffix", None),
                        "addresses": [json.dumps(x) for x in addresses],
                        "accepting": res.get("accepting", None),
                        "gender": res.get("gender", None),
                        "languages": [str(x) for x in languages],
                        "last_updated_on": last_updated_on,
                    }

                    if (
                        ("facility_name" in res)
                        and res.get("facility_name", None)
                        and str(res.get("facility_name", "")).strip()
                    ):
                        # for k in (
                        #         'facility_name', 'facility_type'):
                        #     if not (k in res and res[k] is not None):
                        #         await log_error('err',
                        #                         f"Mandatory field `{k}` for providers data is not present or "
                        #                         f"incorrect. Plan ID: "
                        #                         f"{plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                         task.get('issuer_array'), task.get('url'), 'providers', 'json',
                        #                         myimportlog)

                        obj["name_or_facility_name"] = str(res.get("facility_name", "").strip())
                        obj["specialty_or_facility_type"] = [str(x) for x in res.get("facility_type", [])]
                    else:
                        # for k in (
                        #         'name', 'first', 'last', 'speciality', 'accepting'):
                        #     if not (k in res and res[k] is not None):
                        #         await log_error('err',
                        #                         f"Mandatory field `{k}` for providers data is not present or "
                        #                         f"incorrect. Plan ID: "
                        #                         f"{plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                         task.get('issuer_array'), task.get('url'), 'providers', 'json',
                        #                         myimportlog)

                        obj["name_or_facility_name"] = ""
                        for k in ("prefix", "first", "middle", "last", "suffix"):
                            if (k in name) and (name.get(k, None)):
                                cleaned = _clean_name_part(name.get(k))
                                if cleaned:
                                    obj["name_or_facility_name"] += f"{cleaned} "
                        obj["name_or_facility_name"] = obj["name_or_facility_name"].strip()
                        obj["specialty_or_facility_type"] = [str(x) for x in res.get("specialty", [])]

                    for x in my_network_tiers.values():
                        obj["network_tier"] = x["network_tier"]
                        obj["checksum_network"] = x["checksum_network"]
                        obj["issuer_id"] = x["issuer_id"]
                        obj["year"] = x["year"]
                        plan_npi_obj_dict["_".join([str(obj["npi"]), str(x["checksum_network"])])] = obj.copy()

                        # if count > 10 * int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 50)):
                        #     await push_objects(list(plan_npi_obj_dict.values()), myplan_npi)
                        #     plan_npi_obj_dict = {}
                        #     count = 0
                        # else:
                        #     count += 1
                        #     # except Exception as e:
                        #     #     print(repr(e))
                        #     #     # print('res: ', res)
                        #     #     # print('plan: ', plan)
                        #     #     print('WTF>', obj)
                        #     #     pass
                    address_rows, evidence_rows = _build_mrf_address_rows(
                        res,
                        my_network_tiers,
                        import_date,
                        task.get("url"),
                        last_updated_on,
                        issuer_lookup=issuer_lookup,
                    )
                    for address_row in address_rows:
                        mrf_address_obj_dict[
                            "_".join(
                                [
                                    str(address_row["npi"]),
                                    str(address_row["checksum"]),
                                    address_row["type"],
                                ]
                            )
                        ] = address_row
                    for evidence_row in evidence_rows:
                        mrf_address_evidence_dict[str(evidence_row["evidence_checksum"])] = evidence_row

                    processed_providers += 1
                    count += 1
                    if count > 10000:
                        await asyncio.gather(
                            push_objects(list(plan_npi_obj_dict.values()), myplan_npi),
                            push_objects(list(plan_network_year.values()), myplan_networktier),
                            push_objects(list(mrf_address_obj_dict.values()), mymrfaddress, rewrite=True),
                            push_objects(list(mrf_address_evidence_dict.values()), mymrfaddressevidence),
                        )
                        count = 0
                        plan_npi_obj_dict.clear()
                        plan_network_year.clear()
                        mrf_address_obj_dict.clear()
                        mrf_address_evidence_dict.clear()

                await asyncio.gather(
                    push_objects(list(plan_npi_obj_dict.values()), myplan_npi),
                    push_objects(list(plan_network_year.values()), myplan_networktier),
                    push_objects(list(mrf_address_obj_dict.values()), mymrfaddress, rewrite=True),
                    push_objects(list(mrf_address_evidence_dict.values()), mymrfaddressevidence),
                )
                plan_npi_obj_dict.clear()
                plan_network_year.clear()
                mrf_address_obj_dict.clear()
                mrf_address_evidence_dict.clear()

            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "providers",
                    "json",
                    myimportlog,
                )
                return
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "providers",
                    "json",
                    myimportlog,
                )
                return
    await flush_error_log(myimportlog)
    return 1


def _parse_optional_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "y", "1"}:
            return True
        if lowered in {"false", "no", "n", "0"}:
            return False
    return bool(value)


def _chunked(values, chunk_size=500):
    items = [value for value in set(values) if value]
    for idx in range(0, len(items), chunk_size):
        yield items[idx: idx + chunk_size]


async def _refresh_plan_drug_statistics(plan_ids, import_date, db_schema):
    plan_ids = [value for value in set(plan_ids) if value]
    if not plan_ids:
        return

    plan_drug_cls = make_class(PlanDrugRaw, import_date, schema_override=db_schema)
    stats_cls = make_class(PlanDrugStats, import_date, schema_override=db_schema)
    tier_stats_cls = make_class(PlanDrugTierStats, import_date, schema_override=db_schema)

    plan_drug_table = plan_drug_cls.__table__
    stats_table = stats_cls.__table__
    tier_table = tier_stats_cls.__table__
    tier_label = func.coalesce(plan_drug_table.c.drug_tier, literal("UNKNOWN"))

    for chunk in _chunked(plan_ids):
        delete_stats_stmt = db.delete(stats_table).where(stats_table.c.plan_id.in_(chunk))
        await delete_stats_stmt.status()
        delete_tier_stmt = db.delete(tier_table).where(tier_table.c.plan_id.in_(chunk))
        await delete_tier_stmt.status()

        stats_select = (
            select(
                plan_drug_table.c.plan_id.label("plan_id"),
                func.count().label("total_drugs"),
                func.count().filter(plan_drug_table.c.prior_authorization.is_(True)).label("auth_required"),
                func.count()
                .filter(
                    or_(
                        plan_drug_table.c.prior_authorization.is_(False),
                        plan_drug_table.c.prior_authorization.is_(None),
                    )
                )
                .label("auth_not_required"),
                func.count().filter(plan_drug_table.c.step_therapy.is_(True)).label("step_required"),
                func.count()
                .filter(
                    or_(
                        plan_drug_table.c.step_therapy.is_(False),
                        plan_drug_table.c.step_therapy.is_(None),
                    )
                )
                .label("step_not_required"),
                func.count().filter(plan_drug_table.c.quantity_limit.is_(True)).label("quantity_limit"),
                func.count()
                .filter(
                    or_(
                        plan_drug_table.c.quantity_limit.is_(False),
                        plan_drug_table.c.quantity_limit.is_(None),
                    )
                )
                .label("quantity_no_limit"),
                func.max(plan_drug_table.c.last_updated_on).label("last_updated_on"),
            )
            .where(plan_drug_table.c.plan_id.in_(chunk))
            .group_by(plan_drug_table.c.plan_id)
        )

        stats_insert = (
            db.insert(stats_table)
            .from_select(
                [
                    "plan_id",
                    "total_drugs",
                    "auth_required",
                    "auth_not_required",
                    "step_required",
                    "step_not_required",
                    "quantity_limit",
                    "quantity_no_limit",
                    "last_updated_on",
                ],
                stats_select,
            )
        )
        await stats_insert.status()

        tier_select = (
            select(
                plan_drug_table.c.plan_id.label("plan_id"),
                tier_label.label("drug_tier"),
                func.count().label("drug_count"),
            )
            .where(plan_drug_table.c.plan_id.in_(chunk))
            .group_by(plan_drug_table.c.plan_id, tier_label)
        )
        tier_insert = (
            db.insert(tier_table)
            .from_select(
                [
                    "plan_id",
                    "drug_tier",
                    "drug_count",
                ],
                tier_select,
            )
        )
        await tier_insert.status()


async def _plan_summary_dependencies_ready(db_schema: str) -> tuple[bool, list[str]]:
    missing = []
    for table_name in ("plan_attributes", "plan_benefits", "plan_prices"):
        qualified_name = f"{db_schema}.{table_name}"
        exists = await db.scalar("SELECT to_regclass(:qualified_name)", qualified_name=qualified_name)
        if not exists:
            missing.append(table_name)
    return (len(missing) == 0, missing)


async def process_formulary(ctx, task):
    """
    Download and store formulary (drugs.json) data for an issuer.
    """
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    test_mode = is_test_mode(ctx)
    drug_limit = TEST_DRUG_RECORDS if test_mode else None
    await ensure_database(test_mode)

    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    myplan_drug = make_class(PlanDrugRaw, import_date, schema_override=db_schema)
    touched_plan_ids = set()

    print("Starting Formulary file data download: ", task.get("url"))
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get("url"))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        try:
            await download_it_and_save(
                task.get("url"),
                tmp_filename,
                context={"issuer_array": task["issuer_array"], "source": "formulary"},
                logger=myimportlog,
            )
        except Exception as exc:
            logger.warning("Failed to download formulary data from %s: %s", task.get("url"), exc)
            return

        batch = []
        processed = 0
        try:
            async with async_open(tmp_filename, "rb") as afp:
                async for res in ijson.items(afp, "item", use_float=True):
                    rxnorm_id = str(res.get("rxnorm_id", "")).strip()
                    drug_name = str(res.get("drug_name", "")).strip()
                    plans = res.get("plans") or []
                    if not rxnorm_id or not drug_name or not isinstance(plans, list) or not plans:
                        await log_error(
                            "err",
                            f"Missing required drug fields. rxnorm_id={rxnorm_id!r}",
                            task.get("issuer_array"),
                            task.get("url"),
                            "formulary",
                            "json",
                            myimportlog,
                        )
                        continue

                    for plan_entry in plans:
                        plan_id = str(plan_entry.get("plan_id", "")).strip()
                        plan_id_type = str(plan_entry.get("plan_id_type", "")).strip()
                        if not plan_id or not plan_id_type:
                            await log_error(
                                "err",
                                f"Plan entry missing identifiers for rxnorm_id={rxnorm_id}",
                                task.get("issuer_array"),
                                task.get("url"),
                                "formulary",
                                "json",
                                myimportlog,
                            )
                            continue
                        drug_tier = plan_entry.get("drug_tier")
                        if isinstance(drug_tier, str):
                            drug_tier = drug_tier.strip().upper()
                        record = {
                            "plan_id": plan_id,
                            "plan_id_type": plan_id_type,
                            "rxnorm_id": rxnorm_id,
                            "drug_name": drug_name,
                            "drug_tier": drug_tier,
                            "prior_authorization": _parse_optional_bool(plan_entry.get("prior_authorization")),
                            "step_therapy": _parse_optional_bool(plan_entry.get("step_therapy")),
                            "quantity_limit": _parse_optional_bool(plan_entry.get("quantity_limit")),
                            "last_updated_on": None,
                        }
                        if res.get("last_updated_on"):
                            try:
                                record["last_updated_on"] = datetime.datetime.combine(
                                    parse_date(res["last_updated_on"], fuzzy=True),
                                    datetime.datetime.min.time(),
                                )
                            except (ValueError, TypeError):
                                record["last_updated_on"] = None
                        batch.append(record)
                        touched_plan_ids.add(plan_id)

                    processed += 1
                    if drug_limit and processed >= drug_limit:
                        break
                    if len(batch) > 10000:
                        await push_objects(batch, myplan_drug)
                        batch.clear()

        except ijson.IncompleteJSONError as exc:
            await log_error(
                "err",
                f"Incomplete JSON: can't read expected data. {exc}",
                task.get("issuer_array"),
                task.get("url"),
                "formulary",
                "json",
                myimportlog,
            )
            return
        except ijson.JSONError as exc:
            await log_error(
                "err",
                f"JSON Parsing Error: {exc}",
                task.get("issuer_array"),
                task.get("url"),
                "formulary",
                "json",
                myimportlog,
            )
            return

        if batch:
            await push_objects(batch, myplan_drug)

    if touched_plan_ids:
        await _refresh_plan_drug_statistics(touched_plan_ids, import_date, db_schema)

    await flush_error_log(myimportlog)
    return 1


async def save_mrf_data(ctx, task):
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    x = []
    print("Got task for saving MRF data")
    for key in task:
        match key:
            case "plan_npi":
                myplan_npi = make_class(PlanNPIRaw, import_date, schema_override=db_schema)
                x.append(push_objects(task["plan_npi"], myplan_npi, rewrite=True))
            case "plan_networktier":
                myplan_networktier = make_class(PlanNetworkTierRaw, import_date, schema_override=db_schema)
                x.append(push_objects(task["plan_networktier"], myplan_networktier, rewrite=True))
            case "plan_drugs":
                myplan_drugs = make_class(PlanDrugRaw, import_date, schema_override=db_schema)
                await push_objects(task["plan_drugs"], myplan_drugs, rewrite=True)
                plan_ids = {entry.get("plan_id") for entry in task["plan_drugs"] if entry.get("plan_id")}
                if plan_ids:
                    await _refresh_plan_drug_statistics(plan_ids, import_date, db_schema)
            case "plan_benefits_marketplace":
                myplanbenefitsmarketplace = make_class(
                    PlanBenefitsMarketplace, import_date, schema_override=db_schema
                )
                x.append(push_objects(task["plan_benefits_marketplace"], myplanbenefitsmarketplace))
            case "mrf_address":
                mymrfaddress = make_class(MRFAddress, import_date, schema_override=db_schema)
                x.append(push_objects(task["mrf_address"], mymrfaddress, rewrite=True))
            case "mrf_address_evidence":
                mymrfaddressevidence = make_class(MRFAddressEvidence, import_date, schema_override=db_schema)
                x.append(push_objects(task["mrf_address_evidence"], mymrfaddressevidence))
            case "npi_other_id_list":
                mynpidataotheridentifier = make_class(
                    NPIDataOtherIdentifier, import_date, schema_override=db_schema
                )
                x.append(push_objects(task["npi_other_id_list"], mynpidataotheridentifier, rewrite=True))
            case "npi_taxonomy_group_list":
                mynpidatataxonomygroup = make_class(
                    NPIDataTaxonomyGroup, import_date, schema_override=db_schema
                )
                x.append(push_objects(task["npi_taxonomy_group_list"], mynpidatataxonomygroup, rewrite=True))
            case "npi_address_list":
                mynpiaddress = make_class(NPIAddress, import_date, schema_override=db_schema)
                x.append(push_objects(task["npi_address_list"], mynpiaddress, rewrite=True))
            case "context":
                pass
            case _:
                print("Some wrong key passed")
    await asyncio.gather(*x)


async def process_json_index(ctx, task):
    """
    The process_json_index function is called by the process_index function.
    It downloads a JSON file containing URLs to other files, and then queues up jobs for those files.
    The JSON file contains two arrays: plan_urls and provider_urls.  The plan URLs are queued as 'process_plan' jobs,
    and the provider URLs are queued as 'process_provider' jobs.

    :param ctx: Pass the redis connection to the function
    :param task: Pass the url to download and the issuer_array
    :return: A list of urls to the plan and provider json files
    """
    redis = ctx["redis"]
    issuer_array = task["issuer_array"]
    print(f"CTX: {ctx} \n TASK: {task}")
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    test_mode = is_test_mode(ctx)
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)

    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get("url"))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        await download_it_and_save(
            task.get("url"),
            tmp_filename,
            context={"issuer_array": task["issuer_array"], "source": "json_index"},
            logger=myimportlog,
        )
        plan_limit = TEST_PLAN_URLS if test_mode else None
        provider_limit = TEST_PROVIDER_URLS if test_mode else None
        formulary_limit = TEST_FORMULARY_URLS if test_mode else None
        enqueued_plans = 0
        enqueued_providers = 0
        enqueued_formularies = 0

        async with async_open(tmp_filename, "rb") as afp:
            try:
                async for url in ijson.items(
                    afp, "plan_urls.item", use_float=True
                ):  # , 'formulary_urls', 'provider_urls'
                    print(f"Plan URL: {url}")
                    await redis.enqueue_job(
                        "process_plan",
                        {"url": url, "issuer_array": issuer_array, "context": ctx["context"]},
                        _queue_name=MRF_QUEUE_NAME,
                    )
                    # break
                    enqueued_plans += 1
                    if plan_limit and enqueued_plans >= plan_limit:
                        break
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "json_index",
                    "json",
                    myimportlog,
                )
                return
        async with async_open(tmp_filename, "rb") as afp:
            try:
                async for url in ijson.items(
                    afp, "formulary_urls.item", use_float=True
                ):
                    print(f"Formulary URL: {url}")
                    await redis.enqueue_job(
                        "process_formulary",
                        {"url": url, "issuer_array": issuer_array, "context": ctx["context"]},
                        _queue_name=MRF_QUEUE_NAME,
                    )
                    enqueued_formularies += 1
                    if formulary_limit and enqueued_formularies >= formulary_limit:
                        break
            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "index",
                    "json",
                    myimportlog,
                )
                return
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "json_index",
                    "json",
                    myimportlog,
                )
                return

        async with async_open(tmp_filename, "rb") as afp:
            try:
                async for url in ijson.items(
                    afp, "provider_urls.item", use_float=True
                ):  # , 'formulary_urls', 'provider_urls'
                    print(f"Provider URL: {url}")
                    await redis.enqueue_job(
                        "process_provider",
                        {"url": url, "issuer_array": issuer_array, "context": ctx["context"]},
                        _queue_name=MRF_QUEUE_NAME,
                    )
                    # break
                    enqueued_providers += 1
                    if provider_limit and enqueued_providers >= provider_limit:
                        break
            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "index",
                    "json",
                    myimportlog,
                )
                return
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "json_index",
                    "json",
                    myimportlog,
                )
                return


async def import_unknown_state_issuers_data(test_mode: bool = False):
    plan_list = {}
    issuer_list = {}

    attribute_files = json.loads(os.environ["HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF"])
    processed_rows = 0
    row_limit = TEST_UNKNOWN_STATE_ROWS if test_mode else None
    for file in attribute_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            p = "attr.csv"
            tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
            await download_it_and_save(file["url"], tmp_filename)
            try:
                await unzip(tmp_filename, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for %s: %s", tmp_filename, exc)
                with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]

            async with async_open(tmp_filename, "r", encoding="utf-8-sig") as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row["StandardComponentId"] or not row["PlanId"]:
                        continue
                    plan_key = f"{row['StandardComponentId']}_{row['BusinessYear']}"
                    if plan_key in plan_list:
                        continue
                    plan_list[plan_key] = {
                        "plan_id": row["StandardComponentId"],
                        "plan_id_type": "CMS-HIOS-PLAN-ID",
                        "year": int(row["BusinessYear"]),
                        "issuer_id": int(row["IssuerId"]),
                        "state": str(row["StateCode"]).upper(),
                        "marketing_name": row["PlanMarketingName"],
                        "summary_url": row["URLForSummaryofBenefitsCoverage"],
                        "marketing_url": row["PlanBrochure"],
                        "formulary_url": row["FormularyURL"],
                        "plan_contact": "",
                        "network": [row["NetworkId"]],
                        "benefits": [],
                        "last_updated_on": datetime.datetime.combine(
                            parse_date(row["ImportDate"], fuzzy=True), datetime.datetime.min.time()
                        ),
                        "checksum": return_checksum(
                            [row["StandardComponentId"].lower(), int(row["BusinessYear"])], crc=32
                        ),
                    }

                    issuer_list[int(row["IssuerId"])] = {
                        "state": str(row["StateCode"]).upper(),
                        "issuer_id": int(row["IssuerId"]),
                        "mrf_url": "",
                        "data_contact_email": "",
                        "issuer_marketing_name": "",
                        "issuer_name": (
                            row["IssuerMarketPlaceMarketingName"].strip()
                            if row["IssuerMarketPlaceMarketingName"].strip()
                            else row["IssuerId"]
                        ),
                    }
                    # except:
                    #     from pprint import pprint
                    #     pprint(row)

    state_attribute_files = json.loads(os.environ["HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF"])
    for file in state_attribute_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            p = "attr.csv"
            tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
            await download_it_and_save(file["url"], tmp_filename)
            try:
                await unzip(tmp_filename, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for state attributes %s: %s", tmp_filename, exc)
                with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            csv_files = glob.glob(f"{tmpdirname}/*Plans*.csv")
            tmp_filename = csv_files[0] if csv_files else glob.glob(f"{tmpdirname}/*.csv")[0]

            def to_camel_case(s):
                parts = s.split()
                return "".join(word.capitalize() for word in parts)

            unique_keys = {
                "STANDARD COMPONENT ID": "STANDARD COMPONENT ID",
                "PLAN ID": "PLAN ID",
                "BUSINESS YEAR": "BUSINESS YEAR",
                "ISSUER ID": "ISSUER ID",
                "STATE CODE": "STATE CODE",
                "PLAN MARKETING NAME": "PLAN MARKETING NAME",
                "URL FOR SUMMARY OF BENEFITS COVERAGE": "URL FOR SUMMARY OF BENEFITS COVERAGE",
                "PLAN BROCHURE": "PLAN BROCHURE",
                "FORMULARY URL": "FORMULARY URL",
                "IMPORT DATE": "IMPORT DATE",
                "NETWORK ID": "NETWORK ID",
                "ISSUER NAME": "ISSUER NAME",
            }

            async with async_open(tmp_filename, "r", encoding="utf-8-sig") as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if row.get("STANDARD COMPONENT ID") and row.get("PLAN ID"):
                        continue
                    for key in unique_keys:
                        unique_keys[key] = to_camel_case(unique_keys[key])
                    break

            async with async_open(tmp_filename, "r", encoding="utf-8-sig") as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    standard_component_id = row.get(unique_keys["STANDARD COMPONENT ID"])
                    plan_identifier = row.get(unique_keys["PLAN ID"])
                    business_year = row.get(unique_keys["BUSINESS YEAR"])
                    if standard_component_id and plan_identifier:
                        continue
                    if not standard_component_id or business_year is None:
                        continue

                    plan_key = f"{standard_component_id.upper()}_{business_year}"
                    if plan_key in plan_list:
                        continue

                    issuer_id_value = row.get(unique_keys["ISSUER ID"])
                    plan_list[plan_key] = {
                        "plan_id": standard_component_id,
                        "plan_id_type": "STATE-HIOS-PLAN-ID",
                        "year": int(business_year),
                        "issuer_id": int(issuer_id_value),
                        "state": str(row.get(unique_keys["STATE CODE"])).upper(),
                        "marketing_name": row.get(unique_keys["PLAN MARKETING NAME"]),
                        "summary_url": row.get(unique_keys["URL FOR SUMMARY OF BENEFITS COVERAGE"]),
                        "marketing_url": row.get(unique_keys["PLAN BROCHURE"]),
                        "formulary_url": row.get(unique_keys["FORMULARY URL"]),
                        "plan_contact": "",
                        "network": [row.get(unique_keys["NETWORK ID"])],
                        "benefits": [],
                        "last_updated_on": datetime.datetime.combine(
                            parse_date(row.get(unique_keys["IMPORT DATE"]), fuzzy=True),
                            datetime.datetime.min.time(),
                        ),
                        "checksum": return_checksum(
                            [
                                standard_component_id.lower(),
                                int(business_year),
                            ],
                            crc=32,
                        ),
                    }

                    issuer_name_value = (row.get(unique_keys["ISSUER NAME"]) or "").strip()
                    issuer_list[int(issuer_id_value)] = {
                        "state": str(row.get(unique_keys["STATE CODE"])).upper(),
                        "issuer_id": int(issuer_id_value),
                        "mrf_url": "",
                        "data_contact_email": "",
                        "issuer_marketing_name": "",
                        "issuer_name": issuer_name_value or issuer_id_value,
                    }

                    processed_rows += 1
                    if row_limit and processed_rows >= row_limit:
                        break
                if row_limit and processed_rows >= row_limit:
                    break
        if row_limit and processed_rows >= row_limit:
            break

    return (issuer_list, plan_list)


async def update_issuer_names_data(test_mode: bool = False):
    issuer_list = {}
    my_files = json.loads(os.environ["HLTHPRT_CMSGOV_RATE_REVIEW_URL_PUF"])
    processed_rows = 0
    row_limit = TEST_UNKNOWN_STATE_ROWS if test_mode else None
    for file in my_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            p = "some_file"
            tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
            await download_it_and_save(file["url"], tmp_filename)
            print(f"Trying to unpack1: {tmp_filename}")

            # temp solution
            with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                zip_ref.extractall(tmpdirname)

            # tmp_filename = glob.glob(f"{tmpdirname}/*PUF*.zip")[0]
            # print(f"Trying to unpack: {tmp_filename}")
            # tmpdirname = str(PurePath(str(tmpdirname), 'PUF_FILES'))
            # # temp solution
            # with zipfile.ZipFile(tmp_filename, 'r') as zip_ref:
            #     zip_ref.extractall(tmpdirname)
            print(glob.glob(f"{tmpdirname}/*PUF*.csv"))

            csv_files = glob.glob(f"{tmpdirname}/*PUF*.csv")
            for tmp_filename in csv_files:
                async with async_open(tmp_filename, "r", encoding="utf-8-sig") as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        issuer_list[int(row["ISSUER_ID"])] = {
                            "state": str(row["STATE"]).upper(),
                            "issuer_id": int(row["ISSUER_ID"]),
                            "mrf_url": "",
                            "data_contact_email": "",
                            "issuer_marketing_name": "",
                            "issuer_name": row["COMPANY"].strip() if row["COMPANY"].strip() else row["ISSUER_ID"],
                        }
                        processed_rows += 1
                        if row_limit and processed_rows >= row_limit:
                            break
                if row_limit and processed_rows >= row_limit:
                    break
        if row_limit and processed_rows >= row_limit:
            break

    return issuer_list


async def init_file(ctx, task=None):
    """
    The init_file function is the first function called in this file.
    It downloads a zip file from the CMS website, unzips it, and then parses through each worksheet to create an
    object for each row of data.
    The objects are then pushed into a database using GINO ORM.

    :param ctx: Pass information between functions
    :return: The following:

    """
    task = task or {}
    test_mode = bool(task.get("test_mode"))
    redis = ctx["redis"]
    ctx.setdefault("context", {})
    ctx["context"]["test_mode"] = test_mode
    await ensure_database(test_mode)

    mrf_source = os.environ["HLTHPRT_CMSGOV_MRF_URL_PUF"]
    try:
        if mrf_source.strip().startswith("["):
            parsed_urls = json.loads(mrf_source)
        else:
            parsed_urls = [mrf_source]
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid HLTHPRT_CMSGOV_MRF_URL_PUF; must be JSON array or single URL") from exc
    mrf_urls = [str(url).strip() for url in parsed_urls if str(url).strip()]
    if not mrf_urls:
        raise RuntimeError("HLTHPRT_CMSGOV_MRF_URL_PUF did not provide any usable URLs")
    print("Downloading data from: ", ", ".join(mrf_urls))

    import_date = ctx["context"]["import_date"]
    await _prepare_import_tables(import_date, test_mode)
    ctx["context"]["run"] += 1
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myissuer = make_class(Issuer, import_date, schema_override=db_schema)
    myplan = make_class(Plan, import_date, schema_override=db_schema)
    myplantransparency = make_class(PlanTransparency, import_date, schema_override=db_schema)

    with tempfile.TemporaryDirectory() as tmpdirname:
        transparent_files = json.loads(os.environ["HLTHPRT_CMSGOV_PLAN_TRANSPARENCY_URL_PUF"])
        for file_idx, file in enumerate(transparent_files):
            if test_mode and file_idx >= 1:
                break
            p = "transp.xlsx"
            tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
            await download_it_and_save(file["url"], tmp_filename)

            try:
                await unzip(tmp_filename, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for transparency file %s: %s", tmp_filename, exc)
                with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            tmp_filename = glob.glob(f"{tmpdirname}/*.xlsx")[0]
            xls_file = xl.readxl(tmp_filename)
            os.unlink(tmp_filename)

            obj_list = []
            for ws_name in xls_file.ws_names:
                if not ws_name.startswith("Transparency"):
                    continue
                count = 0
                template = {}
                convert = {
                    "State": "state",
                    "Issuer_Name": "issuer_name",
                    "Issuer_ID": "issuer_id",
                    "Is_Issuer_New_to_Exchange? (Yes_or_No)": "new_issuer_to_exchange",
                    "SADP_Only?": "sadp_only",
                    "Plan_ID": "plan_id",
                    "QHP/SADP": "qhp_sadp",
                    "Plan_Type": "plan_type",
                    "Metal_Level": "metal",
                    "URL_Claims_Payment_Policies": "claims_payment_policies_url",
                }
                for _, v in convert.items():
                    template[v] = -1

                for row in xls_file.ws(ws=ws_name).rows:
                    if count > 2:
                        obj = {}
                        obj["state"] = str(row[template["state"]].upper())
                        obj["issuer_name"] = str(row[template["issuer_name"]])
                        obj["issuer_id"] = int(row[template["issuer_id"]])
                        obj["new_issuer_to_exchange"] = _truthy(row[template["new_issuer_to_exchange"]], ("yes", "y"))
                        obj["sadp_only"] = _truthy(row[template["sadp_only"]], ("yes", "y"))
                        obj["plan_id"] = str(row[template["plan_id"]])
                        obj["year"] = int(file["year"])
                        obj["qhp_sadp"] = str(row[template["qhp_sadp"]])
                        obj["plan_type"] = str(row[template["plan_type"]])
                        obj["metal"] = str(row[template["metal"]])
                        obj["claims_payment_policies_url"] = str(row[template["claims_payment_policies_url"]])

                        obj_list.append(obj)
                        if count > int(os.environ.get("HLTHPRT_SAVE_PER_PACK", 50)):
                            count = 3
                            await push_objects(obj_list, myplantransparency)
                            obj_list = []
                        if test_mode and len(obj_list) >= TEST_PLAN_TRANSPARENCY_ROWS:
                            break
                    elif count == 2:
                        i = 0
                        for name in row:
                            if name in convert:
                                template[convert[name]] = i
                            i += 1
                    count += 1

                await push_objects(obj_list, myplantransparency)
                if test_mode and len(obj_list) >= TEST_PLAN_TRANSPARENCY_ROWS:
                    break

        (issuer_list, plan_list) = await import_unknown_state_issuers_data(test_mode=test_mode)
        issuer_list.update(await update_issuer_names_data(test_mode=test_mode))
        if test_mode:
            issuer_list = dict(list(issuer_list.items())[:TEST_UNKNOWN_STATE_ROWS])
            plan_list = dict(list(plan_list.items())[:TEST_UNKNOWN_STATE_ROWS])

        url_list: set[str] = set()
        url2issuer = {}

        for url_idx, source_url in enumerate(mrf_urls):
            zip_name = f"mrf_puf_{url_idx}.zip"
            zip_path = str(PurePath(str(tmpdirname), zip_name))
            await download_it_and_save(source_url, zip_path)
            try:
                await unzip(zip_path, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for MRF file %s: %s", zip_path, exc)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            extracted_files = glob.glob(f"{tmpdirname}/*.xlsx")
            if not extracted_files:
                continue

            for workbook_path in extracted_files:
                xls_file = xl.readxl(workbook_path)
                ws_name = xls_file.ws_names[-1]
                os.unlink(workbook_path)

                count = 0
                obj_list = []

                for row in xls_file.ws(ws=ws_name).rows:
                    if count != 0:
                        row_urls = []
                        raw_url = row[2]
                        if raw_url:
                            raw_url = str(raw_url).strip()
                            if raw_url.startswith("["):
                                try:
                                    row_urls = json.loads(raw_url)
                                except json.JSONDecodeError:
                                    row_urls = [raw_url]
                            else:
                                row_urls = [raw_url]
                        row_urls = [str(item).strip() for item in row_urls if str(item).strip()]
                        if not row_urls:
                            count += 1
                            continue

                        obj = {
                            "state": row[0].upper(),
                            "issuer_id": int(row[1]),
                            "issuer_marketing_name": "",
                            "data_contact_email": (row[3] or "").strip() if row[3] else "",
                        }
                        issuer_stmt = select(myplantransparency.issuer_name).where(
                            myplantransparency.issuer_id == obj["issuer_id"]
                        )
                        issuer_name = await db.scalar(issuer_stmt)
                        obj["issuer_name"] = issuer_name if issuer_name else "N/A"
                        for single_url in row_urls:
                            obj["mrf_url"] = single_url
                            obj_list.append(obj.copy())
                            url2issuer.setdefault(single_url, []).append(obj["issuer_id"])
                            url_list.add(single_url)
                            existing_issuer = issuer_list.get(obj["issuer_id"])
                            if existing_issuer:
                                if not existing_issuer.get("mrf_url"):
                                    existing_issuer["mrf_url"] = single_url
                                if obj["data_contact_email"] and not existing_issuer.get("data_contact_email"):
                                    existing_issuer["data_contact_email"] = obj["data_contact_email"]
                                if obj["issuer_name"] and not existing_issuer.get("issuer_name"):
                                    existing_issuer["issuer_name"] = obj["issuer_name"]
                                if obj["issuer_marketing_name"] and not existing_issuer.get("issuer_marketing_name"):
                                    existing_issuer["issuer_marketing_name"] = obj["issuer_marketing_name"]
                            else:
                                issuer_list[obj["issuer_id"]] = obj.copy()
                    count += 1

                # obj_list mirrors legacy behaviour (kept for potential reuse), but inserts are handled via issuer_list.

            try:
                os.unlink(zip_path)
            except FileNotFoundError:
                pass

        await asyncio.gather(
            push_objects(list(issuer_list.values()), myissuer), push_objects(list(plan_list.values()), myplan)
        )

        max_urls = TEST_PLAN_URLS if test_mode else None

        for idx, url in enumerate(sorted(url_list)):
            await redis.enqueue_job(
                "process_json_index",
                {"url": url, "issuer_array": url2issuer[url], "context": ctx["context"]},
                _queue_name=MRF_QUEUE_NAME,
            )
            if max_urls and idx + 1 >= max_urls:
                break

        shutdown_job_id = f"shutdown_mrf_{ctx['context']['import_date']}"
        await redis.enqueue_job(
            "shutdown",
            {"context": ctx["context"], "test_mode": test_mode},
            _job_id=shutdown_job_id,
            _queue_name=MRF_FINISH_QUEUE_NAME,
        )
        # break


async def startup(ctx):
    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    override_import_id = os.environ.get("HLTHPRT_IMPORT_ID_OVERRIDE")
    if override_import_id:
        ctx["context"]["import_date"] = override_import_id
    else:
        ctx["context"]["import_date"] = datetime.datetime.utcnow().strftime("%Y%m%d")


async def shutdown(ctx, task):
    """
    The shutdown function is called after the import process has completed.
    It should be used to clean up any temporary tables or files that were created during the import process.


    :param ctx: Pass the context of the import process to other functions
    :return: A coroutine
    """
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    test_mode = is_test_mode(ctx)
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    await flush_error_log(myimportlog)
    await db.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    await db.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")

    test = make_class(Plan, import_date, schema_override=db_schema)
    plans_count = await db.scalar(select(func.count(test.plan_id)))
    if test_mode:
        print(f"Test mode: imported {plans_count} plan rows (no minimum enforced).")
    else:
        if not plans_count or plans_count < 500:
            print(f"Failed Import: Plans number:{plans_count}")
            sys.exit(1)

    await _refresh_mrf_address_summary(import_date, db_schema)

    tables = {}
    async with db.transaction():
        for cls in (
            Issuer,
            Plan,
            PlanFormulary,
            PlanBenefitsMarketplace,
            PlanTransparency,
            PlanDrugRaw,
            PlanDrugStats,
            PlanDrugTierStats,
            ImportLog,
            PlanNPIRaw,
            PlanNetworkTierRaw,
            MRFAddress,
            MRFAddressEvidence,
        ):
            tables[cls.__main_table__] = make_class(cls, import_date, schema_override=db_schema)
            obj = tables[cls.__main_table__]
            table = obj.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{obj.__tablename__} RENAME TO {table};")

            await db.status(
                f"ALTER INDEX IF EXISTS " f"{db_schema}.{table}_idx_primary RENAME TO " f"{table}_idx_primary_old;"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS "
                f"{db_schema}.{obj.__tablename__}_idx_primary RENAME TO "
                f"{table}_idx_primary;"
            )

            if cls in {PlanBenefitsMarketplace, MRFAddress, MRFAddressEvidence}:
                move_indexes = []
                if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                    move_indexes += list(cls.__my_initial_indexes__)
                if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
                    move_indexes += list(cls.__my_additional_indexes__)
                for index in move_indexes:
                    index_name = index.get("name", "_".join(index.get("index_elements")))
                    await db.status(
                        f"ALTER INDEX IF EXISTS "
                        f"{db_schema}.{table}_idx_{index_name} RENAME TO "
                        f"{table}_idx_{index_name}_old;"
                    )
                    await db.status(
                        f"ALTER INDEX IF EXISTS "
                        f"{db_schema}.{obj.__tablename__}_idx_{index_name} RENAME TO "
                        f"{table}_idx_{index_name};"
                    )

    upsert_history = (
        db.insert(ImportHistory)
        .values(import_id=import_date, when=db.func.now())
        .on_conflict_do_update(
            index_elements=ImportHistory.__my_index_elements__,
            index_where=ImportHistory.import_id == import_date,
            set_={"when": db.func.now()},
        )
    )
    await upsert_history.status()
    print("Plans in DB: ", await db.scalar(select(func.count(Plan.plan_id))))  # pylint: disable=E1101
    if test_mode:
        summary_ready, missing_summary_tables = await _plan_summary_dependencies_ready(db_schema)
        if not summary_ready:
            print(
                "Skipping plan search summary rebuild in test mode; missing tables: "
                + ", ".join(missing_summary_tables)
            )
            summary_rows = 0
        else:
            summary_rows = await rebuild_plan_search_summary(test_mode=test_mode)
    else:
        summary_rows = await rebuild_plan_search_summary(test_mode=test_mode)
    print("Plan search summary rows: ", summary_rows)
    print_time_info(ctx["context"]["start"])


async def main(test_mode: bool = False):
    """
    The main function is the entry point of the application.

    :return: A coroutine
    """
    redis = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    await redis.enqueue_job("init_file", {"test_mode": test_mode}, _queue_name=MRF_QUEUE_NAME)


async def finish_main(test_mode: bool = False, import_id: str | None = None):
    redis = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    resolved_import_id = import_id or os.environ.get("HLTHPRT_IMPORT_ID_OVERRIDE") or datetime.datetime.utcnow().strftime(
        "%Y%m%d"
    )
    context = {
        "import_date": resolved_import_id,
        "test_mode": bool(test_mode),
    }
    await redis.enqueue_job(
        "shutdown",
        {"context": context, "test_mode": bool(test_mode)},
        _queue_name=MRF_FINISH_QUEUE_NAME,
        _job_id=f"shutdown_mrf_{resolved_import_id}",
    )
