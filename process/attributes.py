# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import glob
import json
import os
import re
import sys
import tempfile
import zipfile
from pathlib import Path
from pathlib import PurePath

import pytz
from aiocsv import AsyncDictReader
from aiofile import async_open
from arq import create_pool
from dateutil.parser import parse as parse_date
from sqlalchemy.exc import IntegrityError

from api.for_human import plan_attributes_labels_to_key
from db.connection import init_db
from db.models import (PlanAttributes, PlanBenefits, PlanPrices,
                       PlanRatingAreas, db)
from process.ext.archive import unzip
from process.ext.utils import (download_it_and_save, ensure_database,
                               get_import_schema, make_class, print_time_info,
                               push_objects, return_checksum)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

latin_pattern = re.compile(r"[^\x00-\x7f]")
ATTRIBUTES_QUEUE_NAME = "arq:Attributes"

_TABLE_STATE_BY_KEY = {"is_prepared": False}
_TABLES_LOCK = asyncio.Lock()
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_FILE_LIMIT = 1
DEFAULT_TEST_ROW_LIMIT = 500


class _InlineAttributeRedis:
    def __init__(self, ctx):
        self.ctx = ctx
        self.count = 0

    async def enqueue_job(self, function_name, payload, **_kwargs):
        """Execute a supported attribute job in the current process."""

        if function_name != "save_attributes":
            raise RuntimeError(f"Unsupported inline attributes job: {function_name}")
        self.count += 1
        await save_attributes(self.ctx, payload)
        return type("InlineJob", (), {"job_id": f"inline_save_attributes_{self.count}"})()


async def _is_table_available(schema: str, table_name: str) -> bool:
    exists = await db.scalar(
        "SELECT to_regclass(:qualified_name) IS NOT NULL",
        qualified_name=f"{schema}.{table_name}",
    )
    return bool(exists)


async def _prepare_attribute_tables(ctx):
    if _TABLE_STATE_BY_KEY["is_prepared"]:
        return

    async with _TABLES_LOCK:
        if _TABLE_STATE_BY_KEY["is_prepared"]:
            return

        context = ctx.setdefault("context", {})
        test_mode = bool(context.get("test_mode"))
        await ensure_database(test_mode)

        import_date = ctx["import_date"]
        db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)

        await db.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        await db.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")

        for cls in (
            PlanAttributes,
            PlanPrices,
            PlanRatingAreas,
            PlanBenefits,
        ):
            table_model = make_class(cls, import_date, schema_override=db_schema)
            await db.status(
                f"DROP TABLE IF EXISTS {db_schema}.{table_model.__main_table__}_{import_date};"
            )
            try:
                await db.create_table(table_model.__table__, checkfirst=True)
            except IntegrityError as exc:  # pragma: no cover - rare race; ignore if table/type already exists
                if "pg_type_typname_nsp_index" not in str(exc):
                    raise
            if hasattr(table_model, "__my_index_elements__"):
                await db.status(
                    f"CREATE UNIQUE INDEX {table_model.__tablename__}_idx_primary ON "
                    f"{db_schema}.{table_model.__tablename__} "
                    f"({', '.join(table_model.__my_index_elements__)});"
                )

        context["tables_prepared"] = True
        _TABLE_STATE_BY_KEY["is_prepared"] = True
        print("Preparing done")


async def _safe_unzip(zip_path: str, destination: str) -> None:
    try:
        await unzip(zip_path, destination)
    except (zipfile.BadZipFile, RuntimeError, ValueError) as exc:
        print(f"Falling back to zipfile extraction for {zip_path}: {exc}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(destination)


def _parse_flag(value, truthy: tuple[str, ...], falsy: tuple[str, ...]) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False
    return None


def _test_file_limit() -> int:
    return max(1, int(os.environ.get("HLTHPRT_ATTRIBUTES_TEST_FILE_LIMIT", DEFAULT_TEST_FILE_LIMIT)))


def _test_row_limit() -> int:
    return max(1, int(os.environ.get("HLTHPRT_ATTRIBUTES_TEST_ROW_LIMIT", DEFAULT_TEST_ROW_LIMIT)))


def _bounded_test_files(files, test_mode: bool):
    return list(files)[: _test_file_limit()] if test_mode else list(files)


def _normalize_plan_ids(standard_id, full_id):
    standard = (standard_id or "").strip()
    full = (full_id or "").strip()
    if not full:
        return None, None
    if not standard:
        base = full.split("-", 1)[0].strip()
        standard = base[:14] if base else ""
    return (standard or None, full)


def _attribute_objects_from_row(
    attribute_row,
    *,
    plan_id: str,
    full_plan_id: str,
    year: int,
    attribute_name_by_label=None,
):
    """Expand one wide attributes row into normalized key/value records."""
    attribute_objects = []
    for key, raw_value in attribute_row.items():
        if key == "StandardComponentId" and raw_value is None:
            continue
        text_value = str(raw_value).strip()
        if not text_value:
            continue
        attribute_name = (
            attribute_name_by_label[key]
            if attribute_name_by_label is not None
            else key
        )
        attribute_objects.append(
            {
                "plan_id": plan_id,
                "full_plan_id": full_plan_id,
                "year": year,
                "attr_name": re.sub(latin_pattern, r"", attribute_name),
                "attr_value": text_value,
            }
        )
    return attribute_objects


def _benefit_object_from_row(benefit_row, plan_id: str, full_plan_id: str):
    """Normalize one plan-benefit row, returning None for an invalid year."""
    benefit_by_field = {
        "year": None,
        "plan_id": plan_id,
        "full_plan_id": full_plan_id,
        "benefit_name": benefit_row["BenefitName"],
        "copay_inn_tier1": benefit_row["CopayInnTier1"],
        "copay_inn_tier2": benefit_row["CopayInnTier2"],
        "copay_outof_net": benefit_row["CopayOutofNet"],
        "coins_inn_tier1": benefit_row["CoinsInnTier1"],
        "coins_inn_tier2": benefit_row["CoinsInnTier2"],
        "coins_outof_net": benefit_row["CoinsOutofNet"],
        "is_ehb": _parse_flag(benefit_row.get("IsEHB"), ("yes", "y"), ("no", "n")),
        "is_covered": _parse_flag(
            benefit_row.get("IsCovered"), ("covered",), ("not covered",)
        ),
        "quant_limit_on_svc": _parse_flag(
            benefit_row.get("QuantLimitOnSvc"), ("yes", "y"), ("no", "n")
        ),
        "limit_qty": None,
        "limit_unit": benefit_row["LimitUnit"],
        "exclusions": benefit_row["Exclusions"],
        "explanation": benefit_row["Explanation"],
        "ehb_var_reason": benefit_row["EHBVarReason"],
        "is_excl_from_inn_mo": _parse_flag(
            benefit_row.get("IsExclFromInnMOOP"), ("yes", "y"), ("no", "n")
        ),
        "is_excl_from_oon_mo": _parse_flag(
            benefit_row.get("IsExclFromOonMOOP"), ("yes", "y"), ("no", "n")
        ),
    }
    if benefit_row["LimitQty"]:
        try:
            benefit_by_field["limit_qty"] = float(benefit_row["LimitQty"])
        except ValueError:
            benefit_by_field["limit_qty"] = None
    try:
        if benefit_row["BusinessYear"]:
            benefit_by_field["year"] = int(benefit_row["BusinessYear"])
    except ValueError:
        return None
    except KeyError:
        print(benefit_row)
        sys.exit(1)
    return benefit_by_field


async def _enqueue_attribute_batch(
    redis,
    attribute_objects,
    *,
    test_mode: bool,
    record_type: str | None = None,
) -> None:
    """Queue one normalized attribute batch and release its local storage."""
    payload = {
        "attr_obj_list": attribute_objects,
        "context": {"test_mode": test_mode},
    }
    if record_type is not None:
        payload["type"] = record_type
    await redis.enqueue_job(
        "save_attributes",
        payload,
        _queue_name=ATTRIBUTES_QUEUE_NAME,
    )
    attribute_objects.clear()


async def startup(ctx):
    """Initialize attribute-worker context and database access."""

    loop = asyncio.get_event_loop()
    ctx.setdefault("context", {})
    ctx["context"]["start"] = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = bool(ctx["context"].get("test_mode", False))
    ctx["import_date"] = datetime.datetime.now().strftime("%Y%m%d")
    await init_db(db, loop)


async def finalize_attribute_tables(ctx):
    """Finalize staged attribute tables after worker shutdown."""

    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    tables_by_name = {}

    processing_classes_array = (
        PlanAttributes,
        PlanPrices,
        PlanRatingAreas,
        PlanBenefits,
    )

    for cls in processing_classes_array:
        tables_by_name[cls.__main_table__] = make_class(
            cls, import_date, schema_override=db_schema
        )
        table_model = tables_by_name[cls.__main_table__]
        table_name = f"{db_schema}.{table_model.__tablename__}"

        if not await _is_table_available(db_schema, table_model.__tablename__):
            print(f"Skipping post-processing for missing table {table_name}")
            continue

        if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
            for index in cls.__my_additional_indexes__:
                index_name = index.get("name", "_".join(index.get("index_elements")))
                using = ""
                if index_method := index.get("using"):
                    using = f"USING {index_method} "

                unique = ' '
                if index.get('unique'):
                    unique = ' UNIQUE '
                where = ''
                if index.get('where'):
                    where = f' WHERE {index.get("where")} '
                create_index_sql = (
                    f"CREATE{unique}INDEX IF NOT EXISTS "
                    f"{table_model.__tablename__}_idx_{index_name} "
                    f"ON {db_schema}.{table_model.__tablename__}  {using}"
                    f"({', '.join(index.get('index_elements'))}){where};"
                )
                print(create_index_sql)
                await db.status(create_index_sql)

        print(f"Post-Index VACUUM FULL ANALYZE {table_name};")
        await db.execute_ddl(f"VACUUM FULL ANALYZE {table_name};")

    async with db.transaction():
        for cls in processing_classes_array:
            tables_by_name[cls.__main_table__] = make_class(
                cls, import_date, schema_override=db_schema
            )
            table_model = tables_by_name[cls.__main_table__]
            table_name = f"{db_schema}.{table_model.__tablename__}"

            if not await _is_table_available(db_schema, table_model.__tablename__):
                print(f"Skipping swap for missing table {table_name}")
                continue

            table = table_model.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(
                f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;"
            )
            await db.status(
                f"ALTER TABLE IF EXISTS {db_schema}.{table_model.__tablename__} RENAME TO {table};"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS "
                f"{db_schema}.{table}_idx_primary RENAME TO "
                f"{table}_idx_primary_old;"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS "
                f"{db_schema}.{table_model.__tablename__}_idx_primary RENAME TO "
                f"{table}_idx_primary;"
            )

            if (
                hasattr(cls, "__my_additional_indexes__")
                and table_model.__my_additional_indexes__
            ):
                for index in table_model.__my_additional_indexes__:
                    index_name = index.get(
                        "name", "_".join(index.get("index_elements"))
                    )
                    await db.status(
                        f"ALTER INDEX IF EXISTS "
                        f"{db_schema}.{table}_idx_{index_name} RENAME TO "
                        f"{table}_idx_{index_name}_old;"
                    )
                    await db.status(
                        f"ALTER INDEX IF EXISTS "
                        f"{db_schema}.{table_model.__tablename__}_idx_{index_name} RENAME TO "
                        f"{table}_idx_{index_name};"
                    )

    print_time_info(ctx["context"]["start"])


shutdown = finalize_attribute_tables


async def save_attributes(ctx, task):
    """Persist one queued batch of plan attributes or benefits."""

    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    if ("type" in task) and task["type"] == "PlanPrices":
        myplanattributes = make_class(PlanPrices, import_date, schema_override=db_schema)
    else:
        if ("type" in task) and task["type"] == "PlanBenefits":
            myplanattributes = make_class(PlanBenefits, import_date, schema_override=db_schema)
        else:
            myplanattributes = make_class(PlanAttributes, import_date, schema_override=db_schema)
    await push_objects(task["attr_obj_list"], myplanattributes)


async def process_attributes(ctx, task):
    """Download and stream plan attributes into dated staging tables."""

    redis = ctx["redis"]

    print("Downloading data from: ", task["url"])

    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanattributes = make_class(PlanAttributes, import_date, schema_override=db_schema)
    test_row_limit = _test_row_limit()

    with tempfile.TemporaryDirectory() as tmpdirname:
        archive_basename = "attr.csv"
        tmp_filename = str(PurePath(str(tmpdirname), archive_basename + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        attr_obj_list = []

        count = 0
        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for attribute_row in AsyncDictReader(afp, delimiter=","):
                plan_id, full_plan_id = _normalize_plan_ids(
                    attribute_row.get("StandardComponentId"),
                    attribute_row.get("PlanId"),
                )
                if not plan_id or not full_plan_id:
                    continue
                count += 1
                attr_obj_list.extend(
                    _attribute_objects_from_row(
                        attribute_row,
                        plan_id=plan_id,
                        full_plan_id=full_plan_id,
                        year=int(task["year"]),
                    )
                )

                if count > 10000:
                    await _enqueue_attribute_batch(
                        redis,
                        attr_obj_list,
                        test_mode=test_mode,
                    )
                    count = 0
                else:
                    count += 1
                if test_mode and count >= test_row_limit:
                    break

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanattributes)


async def process_benefits(ctx, task):
    """Download and stream plan benefits into dated staging tables."""

    redis = ctx["redis"]
    print("Downloading data from: ", task["url"])

    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanbenefits = make_class(PlanBenefits, import_date, schema_override=db_schema)
    test_row_limit = _test_row_limit()

    with tempfile.TemporaryDirectory() as tmpdirname:
        archive_basename = "benefits.csv"
        tmp_filename = str(PurePath(str(tmpdirname), archive_basename + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        attr_obj_list = []

        count = 0
        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for benefit_row in AsyncDictReader(afp, delimiter=","):
                plan_id, full_plan_id = _normalize_plan_ids(
                    benefit_row.get("StandardComponentId"), benefit_row.get("PlanId")
                )
                if not plan_id or not full_plan_id:
                    continue

                benefit_dict = _benefit_object_from_row(
                    benefit_row,
                    plan_id,
                    full_plan_id,
                )
                if benefit_dict is None:
                    continue

                attr_obj_list.append(benefit_dict)

                if count > 50000:
                    await _enqueue_attribute_batch(
                        redis,
                        attr_obj_list,
                        test_mode=test_mode,
                        record_type="PlanBenefits",
                    )
                    count = 0
                else:
                    count += 1
                if test_mode and count >= test_row_limit:
                    break

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanbenefits)


async def process_rating_areas(ctx):
    """Load the bundled rating-area reference rows into staging."""

    print("Importing Rating Areas")
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanrating = make_class(PlanRatingAreas, import_date, schema_override=db_schema)
    attr_obj_list = []
    rating_areas_path = _PROJECT_ROOT / "data" / "rating_areas.csv"
    if not rating_areas_path.exists():
        rating_areas_path = _PROJECT_ROOT / "restore" / "data" / "rating_areas.csv"
    async with async_open(rating_areas_path, "r", encoding='utf-8-sig') as afp:
        async for row in AsyncDictReader(afp, delimiter=";"):
            rating_area_dict = {
                "state": row["STATE CODE"].upper(),
                "county": row["COUNTY"],
                "zip3": row["ZIP3"],
                "rating_area_id": row["RATING AREA ID"],
                "market": row["MARKET"],
            }
            attr_obj_list.append(rating_area_dict)

    if attr_obj_list:
        await push_objects(attr_obj_list, myplanrating)


async def process_prices(ctx, task):
    """Download and stage one federal plan-pricing CSV source."""

    redis = ctx["redis"]
    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    await process_rating_areas(ctx)
    print("Downloading data from: ", task["url"])

    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanprices = make_class(PlanPrices, import_date, schema_override=db_schema)
    test_row_limit = _test_row_limit()

    with tempfile.TemporaryDirectory() as tmpdirname:
        archive_basename = "rate.csv"
        tmp_filename = str(PurePath(str(tmpdirname), archive_basename + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        attr_obj_list = []

        count = 0

        range_regex = re.compile(r"^(\d+)-(\d+)$")
        int_more_regex = re.compile(r"^(\d+) and over$")
        clean_int = re.compile(r"^(\d+)$")
        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for price_row in AsyncDictReader(afp, delimiter=","):
                if not price_row["PlanId"]:
                    continue
                count += 1

                price_dict = {
                    "plan_id": price_row["PlanId"],
                    "state": price_row["StateCode"].upper(),
                    "year": int(task["year"]),
                    "rate_effective_date": pytz.utc.localize(
                        parse_date(price_row["RateEffectiveDate"], fuzzy=True)
                    )
                    if price_row["RateEffectiveDate"]
                    else None,
                    "rate_expiration_date": pytz.utc.localize(
                        parse_date(price_row["RateExpirationDate"], fuzzy=True)
                    )
                    if price_row["RateExpirationDate"]
                    else None,
                    "rating_area_id": price_row["RatingAreaId"],
                    "tobacco": price_row["Tobacco"],
                    "min_age": 0,
                    "max_age": 125,
                    "individual_rate": float(price_row["IndividualRate"])
                    if price_row["IndividualRate"]
                    else None,
                    "individual_tobacco_rate": float(price_row["IndividualTobaccoRate"])
                    if price_row["IndividualTobaccoRate"]
                    else None,
                    "couple": float(price_row["Couple"])
                    if price_row["Couple"]
                    else None,
                    "primary_subscriber_and_one_dependent": float(
                        price_row["PrimarySubscriberAndOneDependent"]
                    )
                    if price_row["PrimarySubscriberAndOneDependent"]
                    else None,
                    "primary_subscriber_and_two_dependents": float(
                        price_row["PrimarySubscriberAndTwoDependents"]
                    )
                    if price_row["PrimarySubscriberAndTwoDependents"]
                    else None,
                    "primary_subscriber_and_three_or_more_dependents": float(
                        price_row["PrimarySubscriberAndThreeOrMoreDependents"]
                    )
                    if price_row["PrimarySubscriberAndThreeOrMoreDependents"]
                    else None,
                    "couple_and_one_dependent": float(
                        price_row["CoupleAndOneDependent"]
                    )
                    if price_row["CoupleAndOneDependent"]
                    else None,
                    "couple_and_two_dependents": float(
                        price_row["CoupleAndTwoDependents"]
                    )
                    if price_row["CoupleAndTwoDependents"]
                    else None,
                    "couple_and_three_or_more_dependents": float(
                        price_row["CoupleAndThreeOrMoreDependents"]
                    )
                    if price_row["CoupleAndThreeOrMoreDependents"]
                    else None,
                }

                match price_row["Age"].strip():
                    case age_text if regex_match := clean_int.search(age_text):
                        price_dict["min_age"] = int(regex_match.group(1))
                        price_dict["max_age"] = price_dict["min_age"]
                    case age_text if regex_match := range_regex.search(age_text):
                        price_dict["min_age"] = int(regex_match.group(1))
                        price_dict["max_age"] = int(regex_match.group(2))
                    case age_text if regex_match := int_more_regex.search(age_text):
                        price_dict["min_age"] = int(regex_match.group(1))

                price_dict["checksum"] = return_checksum(
                    [
                        price_dict["plan_id"],
                        price_dict["year"],
                        price_dict["rate_effective_date"],
                        price_dict["rate_expiration_date"],
                        price_dict["rating_area_id"],
                        price_dict["min_age"],
                        price_dict["max_age"],
                    ]
                )

                attr_obj_list.append(price_dict)

                if count > 1000000:
                    total_count += count
                    await redis.enqueue_job(
                        "save_attributes",
                        {
                            "type": "PlanPrices",
                            "attr_obj_list": attr_obj_list,
                            "context": {"test_mode": test_mode},
                        },
                        _queue_name=ATTRIBUTES_QUEUE_NAME,
                    )
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1
                if test_mode and count >= test_row_limit:
                    break

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanprices)

        #     obj_list = []
        #     for ws_name in xls_file.ws_names:
        #         if not ws_name.startswith('Transparency'):
        #             continue
        #         count = 0
        #         template = {}
        #         convert = {
        #             'State': 'state',
        #             'Issuer_Name': 'issuer_name',
        #             'Issuer_ID': 'issuer_id',
        #             'Is_Issuer_New_to_Exchange? (Yes_or_No)': 'new_issuer_to_exchange',
        #             'SADP_Only?': 'sadp_only',
        #             'Plan_ID': 'plan_id',
        #             'QHP/SADP': 'qhp_sadp',
        #             'Plan_Type': 'plan_type',
        #             'Metal_Level': 'metal',
        #             'URL_Claims_Payment_Policies': 'claims_payment_policies_url'
        #         }
        #         for k, v in convert.items():
        #             template[v] = -1
        #
        #         for row in xls_file.ws(ws=ws_name).rows:
        #             if count > 2:
        #                 obj = {}
        #                 obj['state'] = row[template['state']].upper()
        #                 obj['issuer_name'] = row[template['issuer_name']]
        #                 obj['issuer_id'] = int(row[template['issuer_id']])
        #                 obj['new_issuer_to_exchange'] = True if row[template['new_issuer_to_exchange']] in (
        #                     'Yes', 'yes', 'y') else False
        #                 obj['sadp_only'] = True if row[template['sadp_only']] in ('Yes', 'yes', 'y') else False
        #                 obj['plan_id'] = row[template['plan_id']]
        #                 obj['year'] = int(file['year'])
        #                 obj['qhp_sadp'] = row[template['qhp_sadp']]
        #                 obj['plan_type'] = row[template['plan_type']]
        #                 obj['metal'] = row[template['metal']]
        #                 obj['claims_payment_policies_url'] = row[template['claims_payment_policies_url']]
        #
        #                 obj_list.append(obj)
        #                 if count > int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 50)):
        #                     count = 3
        #                     await push_objects(obj_list, myplantransparency)
        #                     obj_list = []
        #             elif count == 2:
        #                 i = 0
        #                 for name in row:
        #                     if name in convert:
        #                         template[convert[name]] = i
        #                     i += 1
        #             count += 1
        #
        #         await push_objects(obj_list, myplantransparency)
        #
        # p = 'mrf_puf.xlsx'
        # tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
        # await download_it_and_save(os.environ['HLTHPRT_CMSGOV_MRF_URL_PUF'], tmp_filename)
        # await unzip(tmp_filename, tmpdirname)
        #
        # tmp_filename = glob.glob(f"{tmpdirname}/*.xlsx")[0]
        # xls_file = xl.readxl(tmp_filename)
        # ws_name = xls_file.ws_names[1]
        # os.unlink(tmp_filename)
        #
        # count = 0
        # url_list = []
        # obj_list = []
        # url2issuer = {}
        #
        # for row in xls_file.ws(ws=ws_name).rows:
        #     if count != 0:
        #         url_list.append(row[2])
        #         obj = {}
        #         obj['state'] = row[0].upper()
        #         obj['issuer_id'] = int(row[1])
        #         obj['mrf_url'] = row[2]
        #         issuer_name = await myplantransparency.select('issuer_name').where(
        #             myplantransparency.issuer_id == obj['issuer_id'])
        #         obj['issuer_name'] = issuer_name if issuer_name else 'N/A'
        #         obj['data_contact_email'] = row[3]
        #         obj_list.append(obj)
        #         if obj['mrf_url'] in url2issuer:
        #             url2issuer[obj['mrf_url']].append(obj['issuer_id'])
        #         else:
        #             url2issuer[obj['mrf_url']] = [obj['issuer_id'], ]
        #     count += 1
        #     if not (count % 100):
        #         await push_objects(obj_list, myissuer)
        #         obj_list.clear()
        #
        # url_list = list(set(url_list))
        # await push_objects(obj_list, myissuer)
        #
        # for url in url_list:
        #     await redis.enqueue_job('process_json_index', {'url': url, 'issuer_array': url2issuer[url]})
#     # break


async def process_state_attributes(ctx, task):
    """Download and stage one state-marketplace plan-attributes source."""

    redis = ctx["redis"]

    print("Downloading data from: ", task["url"])

    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanattributes = make_class(PlanAttributes, import_date, schema_override=db_schema)
    test_row_limit = _test_row_limit()

    with tempfile.TemporaryDirectory() as tmpdirname:
        archive_basename = "attr.csv"
        tmp_filename = str(PurePath(str(tmpdirname), archive_basename + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*Plans*.csv")[0]
        total_count = 0
        attr_obj_list = []

        count = 0
        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for attribute_row in AsyncDictReader(afp, delimiter=","):
                plan_id, full_plan_id = _normalize_plan_ids(
                    attribute_row.get("STANDARD COMPONENT ID"),
                    attribute_row.get("PLAN ID"),
                )
                if not plan_id or not full_plan_id:
                    continue
                count += 1
                attr_obj_list.extend(
                    _attribute_objects_from_row(
                        attribute_row,
                        plan_id=plan_id,
                        full_plan_id=full_plan_id,
                        year=int(task["year"]),
                        attribute_name_by_label=plan_attributes_labels_to_key,
                    )
                )

                if count > 10000:
                    await _enqueue_attribute_batch(
                        redis,
                        attr_obj_list,
                        test_mode=test_mode,
                    )
                    count = 0
                else:
                    count += 1
                if test_mode and count >= test_row_limit:
                    break

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanattributes)


async def enqueue_attribute_sources(test_mode: bool = False):
    """Queue all configured plan attribute, benefit, and pricing sources."""

    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
        default_queue_name=ATTRIBUTES_QUEUE_NAME,
    )
    source_groups = _attribute_source_groups()
    attribute_files = _bounded_test_files(source_groups["attributes"], test_mode)
    state_attribute_files = _bounded_test_files(source_groups["state_attributes"], test_mode)
    price_files = _bounded_test_files(source_groups["prices"], test_mode)
    benefits_files = _bounded_test_files(source_groups["benefits"], test_mode)

    print("Starting to process STATE Plan Attribute files..")
    for file in state_attribute_files:
        print("Adding: ", file)
        await redis.enqueue_job(
            "process_state_attributes",
            {
                "url": file["url"],
                "year": file["year"],
                "context": {"test_mode": test_mode},
            },
            _queue_name=ATTRIBUTES_QUEUE_NAME,
        )

    print("Starting to process Plan Attribute files..")
    for file in attribute_files:
        print("Adding: ", file)
        await redis.enqueue_job(
            "process_attributes",
            {
                "url": file["url"],
                "year": file["year"],
                "context": {"test_mode": test_mode},
            },
            _queue_name=ATTRIBUTES_QUEUE_NAME,
        )

    print("Starting to process Plan Prices files..")
    for file in price_files:
        print("Adding: ", file)
        await redis.enqueue_job(
            "process_prices",
            {
                "url": file["url"],
                "year": file["year"],
                "context": {"test_mode": test_mode},
            },
            _queue_name=ATTRIBUTES_QUEUE_NAME,
        )

    print("Starting to process Plan Benefits files..")
    for file in benefits_files:
        print("Adding: ", file)
        await redis.enqueue_job(
            "process_benefits",
            {
                "url": file["url"],
                "year": file["year"],
                "context": {"test_mode": test_mode},
            },
            _queue_name=ATTRIBUTES_QUEUE_NAME,
        )


main = enqueue_attribute_sources


def _attribute_source_groups():
    return {
        "attributes": json.loads(os.environ["HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF"]),
        "state_attributes": json.loads(os.environ["HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF"]),
        "prices": json.loads(os.environ["HLTHPRT_CMSGOV_PRICE_PLAN_URL_PUF"]),
        "benefits": json.loads(os.environ["HLTHPRT_CMSGOV_BENEFITS_URL_PUF"]),
    }


async def plan_attributes_control_start(ctx, task=None):
    """Run bounded plan-attribute ingestion under the control-plane adapter."""

    task = task or {}
    ctx.setdefault("context", {})
    ctx["context"]["test_mode"] = bool(task.get("test_mode", task.get("test", False)))
    ctx["context"].setdefault("start", datetime.datetime.utcnow().replace(tzinfo=pytz.utc))
    ctx["import_date"] = datetime.datetime.now().strftime("%Y%m%d")
    ctx["redis"] = _InlineAttributeRedis(ctx)

    source_groups = _attribute_source_groups()
    state_attribute_files = _bounded_test_files(source_groups["state_attributes"], ctx["context"]["test_mode"])
    attribute_files = _bounded_test_files(source_groups["attributes"], ctx["context"]["test_mode"])
    price_files = _bounded_test_files(source_groups["prices"], ctx["context"]["test_mode"])
    benefits_files = _bounded_test_files(source_groups["benefits"], ctx["context"]["test_mode"])
    for file in state_attribute_files:
        await process_state_attributes(
            ctx,
            {"url": file["url"], "year": file["year"], "context": {"test_mode": ctx["context"]["test_mode"]}},
        )
    for file in attribute_files:
        await process_attributes(
            ctx,
            {"url": file["url"], "year": file["year"], "context": {"test_mode": ctx["context"]["test_mode"]}},
        )
    for file in price_files:
        await process_prices(
            ctx,
            {"url": file["url"], "year": file["year"], "context": {"test_mode": ctx["context"]["test_mode"]}},
        )
    for file in benefits_files:
        await process_benefits(
            ctx,
            {"url": file["url"], "year": file["year"], "context": {"test_mode": ctx["context"]["test_mode"]}},
        )

    await shutdown(ctx)
    return {
        "state_attribute_files": len(state_attribute_files),
        "attribute_files": len(attribute_files),
        "price_files": len(price_files),
        "benefit_files": len(benefits_files),
        "inline_save_jobs": ctx["redis"].count,
        "test_mode": ctx["context"]["test_mode"],
    }
