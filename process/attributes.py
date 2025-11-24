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
from pathlib import PurePath

import msgpack
import pytz
from aiocsv import AsyncDictReader
from aiofile import async_open
from arq import create_pool
from arq.connections import RedisSettings
from async_unzip.unzipper import unzip
from dateutil.parser import parse as parse_date
from sqlalchemy.exc import IntegrityError

from api.for_human import plan_attributes_labels_to_key
from db.connection import init_db
from db.models import (PlanAttributes, PlanBenefits, PlanPrices,
                       PlanRatingAreas, db)
from process.ext.utils import (download_it_and_save, ensure_database,
                               get_import_schema, make_class, print_time_info,
                               push_objects, return_checksum)

latin_pattern = re.compile(r"[^\x00-\x7f]")

_TABLES_PREPARED = False
_TABLES_LOCK = asyncio.Lock()


async def _prepare_attribute_tables(ctx):
    global _TABLES_PREPARED  # pylint: disable=global-statement
    if _TABLES_PREPARED:
        return

    async with _TABLES_LOCK:
        if _TABLES_PREPARED:
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
            obj = make_class(cls, import_date, schema_override=db_schema)
            await db.status(
                f"DROP TABLE IF EXISTS {db_schema}.{obj.__main_table__}_{import_date};"
            )
            try:
                await db.create_table(obj.__table__, checkfirst=True)
            except IntegrityError as exc:  # pragma: no cover - rare race; ignore if table/type already exists
                if "pg_type_typname_nsp_index" not in str(exc):
                    raise
            if hasattr(obj, "__my_index_elements__"):
                await db.status(
                    f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary ON "
                    f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});"
                )

        context["tables_prepared"] = True
        _TABLES_PREPARED = True
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


def _normalize_plan_ids(standard_id, full_id):
    standard = (standard_id or "").strip()
    full = (full_id or "").strip()
    if not full:
        return None, None
    if not standard:
        base = full.split("-", 1)[0].strip()
        standard = base[:14] if base else ""
    return (standard or None, full)


async def startup(ctx):
    loop = asyncio.get_event_loop()
    ctx.setdefault("context", {})
    ctx["context"]["start"] = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = bool(ctx["context"].get("test_mode", False))
    ctx["import_date"] = datetime.datetime.now().strftime("%Y%m%d")
    await init_db(db, loop)


async def shutdown(ctx):
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    tables = {}

    processing_classes_array = (
        PlanAttributes,
        PlanPrices,
        PlanRatingAreas,
        PlanBenefits,
    )

    for cls in processing_classes_array:
        tables[cls.__main_table__] = make_class(cls, import_date, schema_override=db_schema)
        obj = tables[cls.__main_table__]

        if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
            for index in cls.__my_additional_indexes__:
                index_name = index.get("name", "_".join(index.get("index_elements")))
                using = ""
                if t := index.get("using"):
                    using = f"USING {t} "

                unique = ' '
                if index.get('unique'):
                    unique = ' UNIQUE '
                where = ''
                if index.get('where'):
                    where = f' WHERE {index.get("where")} '
                create_index_sql = (
                    f"CREATE{unique}INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} "
                    f"ON {db_schema}.{obj.__tablename__}  {using}"
                    f"({', '.join(index.get('index_elements'))}){where};"
                )
                print(create_index_sql)
                await db.status(create_index_sql)

        print(f"Post-Index VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")
        await db.execute_ddl(f"VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")

    async with db.transaction():
        for cls in processing_classes_array:
            tables[cls.__main_table__] = make_class(cls, import_date, schema_override=db_schema)
            obj = tables[cls.__main_table__]

            table = obj.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(
                f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;"
            )
            await db.status(
                f"ALTER TABLE IF EXISTS {db_schema}.{obj.__tablename__} RENAME TO {table};"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS "
                f"{db_schema}.{table}_idx_primary RENAME TO "
                f"{table}_idx_primary_old;"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS "
                f"{db_schema}.{obj.__tablename__}_idx_primary RENAME TO "
                f"{table}_idx_primary;"
            )

            if (
                hasattr(cls, "__my_additional_indexes__")
                and obj.__my_additional_indexes__
            ):
                for index in obj.__my_additional_indexes__:
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
                        f"{db_schema}.{obj.__tablename__}_idx_{index_name} RENAME TO "
                        f"{table}_idx_{index_name};"
                    )

    print_time_info(ctx["context"]["start"])


async def save_attributes(ctx, task):
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
    redis = ctx["redis"]

    print("Downloading data from: ", task["url"])

    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanattributes = make_class(PlanAttributes, import_date, schema_override=db_schema)

    with tempfile.TemporaryDirectory() as tmpdirname:
        p = "attr.csv"
        tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        total_count = 0
        attr_obj_list = []

        count = 0
        # return 1
        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for row in AsyncDictReader(afp, delimiter=","):
                plan_id, full_plan_id = _normalize_plan_ids(
                    row.get("StandardComponentId"), row.get("PlanId")
                )
                if not plan_id or not full_plan_id:
                    continue
                count += 1
                for key in row:
                    if not (
                        (key in ("StandardComponentId",)) and (row[key] is None)
                    ) and (t := str(row[key]).strip()):
                        obj = {
                            "plan_id": plan_id,
                            "full_plan_id": full_plan_id,
                            "year": int(task["year"]),  # int(row['\ufeffBusinessYear'])
                            "attr_name": re.sub(latin_pattern, r"", key),
                            "attr_value": t,
                        }

                        attr_obj_list.append(obj)

                if count > 10000:
                    # int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 100)):
                    total_count += count
                    await redis.enqueue_job(
                        "save_attributes",
                        {
                            "attr_obj_list": attr_obj_list,
                            "context": {"test_mode": test_mode},
                        },
                    )
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanattributes)


async def process_benefits(ctx, task):
    redis = ctx["redis"]
    print("Downloading data from: ", task["url"])

    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanbenefits = make_class(PlanBenefits, import_date, schema_override=db_schema)

    with tempfile.TemporaryDirectory() as tmpdirname:
        p = "benefits.csv"
        tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        total_count = 0
        attr_obj_list = []

        count = 0
        # return 1
        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for row in AsyncDictReader(afp, delimiter=","):
                plan_id, full_plan_id = _normalize_plan_ids(
                    row.get("StandardComponentId"), row.get("PlanId")
                )
                if not plan_id or not full_plan_id:
                    continue

                obj = {
                    "year": None,
                    "plan_id": plan_id,
                    "full_plan_id": full_plan_id,
                    "benefit_name": row["BenefitName"],
                    "copay_inn_tier1": row["CopayInnTier1"],
                    "copay_inn_tier2": row["CopayInnTier2"],
                    "copay_outof_net": row["CopayOutofNet"],
                    "coins_inn_tier1": row["CoinsInnTier1"],
                    "coins_inn_tier2": row["CoinsInnTier2"],
                    "coins_outof_net": row["CoinsOutofNet"],
                    "is_ehb": None,
                    "is_covered": None,
                    "quant_limit_on_svc": None,
                    "limit_qty": None,
                    "limit_unit": row["LimitUnit"],
                    "exclusions": row["Exclusions"],
                    "explanation": row["Explanation"],
                    "ehb_var_reason": row["EHBVarReason"],
                    "is_excl_from_inn_mo": None,
                    "is_excl_from_oon_mo": None,
                }

                obj["is_ehb"] = _parse_flag(row.get("IsEHB"), ("yes", "y"), ("no", "n"))
                obj["is_covered"] = _parse_flag(row.get("IsCovered"), ("covered",), ("not covered",))
                obj["quant_limit_on_svc"] = _parse_flag(
                    row.get("QuantLimitOnSvc"), ("yes", "y"), ("no", "n")
                )
                obj["is_excl_from_inn_mo"] = _parse_flag(
                    row.get("IsExclFromInnMOOP"), ("yes", "y"), ("no", "n")
                )
                obj["is_excl_from_oon_mo"] = _parse_flag(
                    row.get("IsExclFromOonMOOP"), ("yes", "y"), ("no", "n")
                )

                if row["LimitQty"]:
                    try:
                        obj["limit_qty"] = float(row["LimitQty"])
                    except ValueError:
                        pass

                try:
                    if row["BusinessYear"]:
                        try:
                            obj["year"] = int(row["BusinessYear"])
                        except ValueError:
                            continue
                except KeyError:
                    print(row)
                    sys.exit(1)

                attr_obj_list.append(obj)

                if count > 50000:
                    total_count += count
                    await redis.enqueue_job(
                        "save_attributes",
                        {
                            "type": "PlanBenefits",
                            "attr_obj_list": attr_obj_list,
                            "context": {"test_mode": test_mode},
                        },
                    )
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanbenefits)


async def process_rating_areas(ctx):
    print("Importing Rating Areas")
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanrating = make_class(PlanRatingAreas, import_date, schema_override=db_schema)
    attr_obj_list = []
    async with async_open("data/rating_areas.csv", "r", encoding='utf-8-sig') as afp:
        async for row in AsyncDictReader(afp, delimiter=";"):
            obj = {
                "state": row["STATE CODE"].upper(),
                "county": row["COUNTY"],
                "zip3": row["ZIP3"],
                "rating_area_id": row["RATING AREA ID"],
                "market": row["MARKET"],
            }
            attr_obj_list.append(obj)

    if attr_obj_list:
        await push_objects(attr_obj_list, myplanrating)


async def process_prices(ctx, task):
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

    with tempfile.TemporaryDirectory() as tmpdirname:
        p = "rate.csv"
        tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        total_count = 0
        attr_obj_list = []

        count = 0

        range_regex = re.compile(r"^(\d+)-(\d+)$")
        int_more_regex = re.compile(r"^(\d+) and over$")
        clean_int = re.compile(r"^(\d+)$")
        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for row in AsyncDictReader(afp, delimiter=","):
                if not row["PlanId"]:
                    continue
                count += 1

                obj = {
                    "plan_id": row["PlanId"],
                    "state": row["StateCode"].upper(),
                    "year": int(task["year"]),
                    "rate_effective_date": pytz.utc.localize(
                        parse_date(row["RateEffectiveDate"], fuzzy=True)
                    )
                    if row["RateEffectiveDate"]
                    else None,
                    "rate_expiration_date": pytz.utc.localize(
                        parse_date(row["RateExpirationDate"], fuzzy=True)
                    )
                    if row["RateExpirationDate"]
                    else None,
                    "rating_area_id": row["RatingAreaId"],
                    "tobacco": row["Tobacco"],
                    "min_age": 0,
                    "max_age": 125,
                    "individual_rate": float(row["IndividualRate"])
                    if row["IndividualRate"]
                    else None,
                    "individual_tobacco_rate": float(row["IndividualTobaccoRate"])
                    if row["IndividualTobaccoRate"]
                    else None,
                    "couple": float(row["Couple"]) if row["Couple"] else None,
                    "primary_subscriber_and_one_dependent": float(
                        row["PrimarySubscriberAndOneDependent"]
                    )
                    if row["PrimarySubscriberAndOneDependent"]
                    else None,
                    "primary_subscriber_and_two_dependents": float(
                        row["PrimarySubscriberAndTwoDependents"]
                    )
                    if row["PrimarySubscriberAndTwoDependents"]
                    else None,
                    "primary_subscriber_and_three_or_more_dependents": float(
                        row["PrimarySubscriberAndThreeOrMoreDependents"]
                    )
                    if row["PrimarySubscriberAndThreeOrMoreDependents"]
                    else None,
                    "couple_and_one_dependent": float(row["CoupleAndOneDependent"])
                    if row["CoupleAndOneDependent"]
                    else None,
                    "couple_and_two_dependents": float(row["CoupleAndTwoDependents"])
                    if row["CoupleAndTwoDependents"]
                    else None,
                    "couple_and_three_or_more_dependents": float(
                        row["CoupleAndThreeOrMoreDependents"]
                    )
                    if row["CoupleAndThreeOrMoreDependents"]
                    else None,
                }

                match row["Age"].strip():
                    case x if t := clean_int.search(x):
                        obj["min_age"] = int(t.group(1))
                        obj["max_age"] = obj["min_age"]
                    case x if t := range_regex.search(x):
                        obj["min_age"] = int(t.group(1))
                        obj["max_age"] = int(t.group(2))
                    case x if t := int_more_regex.search(x):
                        obj["min_age"] = int(t.group(1))

                obj["checksum"] = return_checksum(
                    [
                        obj["plan_id"],
                        obj["year"],
                        obj["rate_effective_date"],
                        obj["rate_expiration_date"],
                        obj["rating_area_id"],
                        obj["min_age"],
                        obj["max_age"],
                    ]
                )

                attr_obj_list.append(obj)

                if count > 1000000:
                    total_count += count
                    await redis.enqueue_job(
                        "save_attributes",
                        {
                            "type": "PlanPrices",
                            "attr_obj_list": attr_obj_list,
                            "context": {"test_mode": test_mode},
                        },
                    )
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanprices)

        #     obj_list = []
        #     for ws_name in xls_file.ws_names:
        #         print(ws_name)
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
    redis = ctx["redis"]

    print("Downloading data from: ", task["url"])

    if "context" in task:
        ctx.setdefault("context", {}).update(task["context"])
    await _prepare_attribute_tables(ctx)
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    myplanattributes = make_class(PlanAttributes, import_date, schema_override=db_schema)

    with tempfile.TemporaryDirectory() as tmpdirname:
        p = "attr.csv"
        tmp_filename = str(PurePath(str(tmpdirname), p + ".zip"))
        await download_it_and_save(task["url"], tmp_filename)
        await _safe_unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*Plans*.csv")[0]
        total_count = 0
        attr_obj_list = []

        count = 0
        # return 1

        async with async_open(tmp_filename, "r", encoding='utf-8-sig') as afp:
            async for row in AsyncDictReader(afp, delimiter=","):
                plan_id, full_plan_id = _normalize_plan_ids(
                    row.get("STANDARD COMPONENT ID"), row.get("PLAN ID")
                )
                if not plan_id or not full_plan_id:
                    continue
                count += 1
                for key in row:
                    if not (
                        (key in ("StandardComponentId",)) and (row[key] is None)
                    ) and (t := str(row[key]).strip()):
                        obj = {
                            "plan_id": plan_id,
                            "full_plan_id": full_plan_id,
                            "year": int(task["year"]),  # int(row['\ufeffBusinessYear'])
                            "attr_name": re.sub(
                                latin_pattern, r"", plan_attributes_labels_to_key[key]
                            ),
                            "attr_value": t,
                        }

                        attr_obj_list.append(obj)

                if count > 10000:
                    # int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 100)):
                    total_count += count
                    await redis.enqueue_job(
                        "save_attributes",
                        {
                            "attr_obj_list": attr_obj_list,
                            "context": {"test_mode": test_mode},
                        },
                    )
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanattributes)


async def main(test_mode: bool = False):
    redis = await create_pool(
        RedisSettings.from_dsn(os.environ.get("HLTHPRT_REDIS_ADDRESS")),
        job_serializer=msgpack.packb,
        job_deserializer=lambda b: msgpack.unpackb(b, raw=False),
    )
    attribute_files = json.loads(os.environ["HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF"])
    state_attribute_files = json.loads(
        os.environ["HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF"]
    )

    price_files = json.loads(os.environ["HLTHPRT_CMSGOV_PRICE_PLAN_URL_PUF"])

    benefits_files = json.loads(os.environ["HLTHPRT_CMSGOV_BENEFITS_URL_PUF"])

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
        )
