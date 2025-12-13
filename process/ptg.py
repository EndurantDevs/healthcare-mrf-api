# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=broad-exception-caught,too-many-branches,too-many-locals,too-many-statements

import datetime
import gzip
import json
import logging
import os
import tempfile
import zipfile
from pathlib import Path, PurePath
from typing import Any

import ijson
from aiofile import async_open
from dateutil.parser import parse as parse_date

from db.connection import db
from db.models import (
    ImportLog,
    PTGAllowedItem,
    PTGAllowedPayment,
    PTGAllowedProviderPayment,
    PTGBillingCode,
    PTGFile,
    PTGInNetworkItem,
    PTGNegotiatedPrice,
    PTGNegotiatedRate,
    PTGProviderGroup,
)
from process.ext.utils import (
    download_it_and_save,
    ensure_database,
    flush_error_log,
    get_import_schema,
    log_error,
    make_class,
    push_objects,
    return_checksum,
)

logger = logging.getLogger(__name__)

TEST_TOC_FILES = 2
TEST_TOC_JOBS = 2
TEST_PROVIDER_GROUPS = 50
TEST_IN_NETWORK_ITEMS = 25
TEST_NEGOTIATED_PRICES = 250
TEST_ALLOWED_ITEMS = 25


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    if not normalized:
        return datetime.date.today().strftime("%Y%m%d")
    return normalized


def _make_checksum(*values: Any) -> int:
    return return_checksum(list(values))


def _coerce_date(value: Any) -> datetime.date | None:
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    try:
        parsed = parse_date(str(value))
    except (ValueError, TypeError):
        return None
    if isinstance(parsed, datetime.datetime):
        return parsed.date()
    return parsed


def _as_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]

def _normalize_provider_ref(value: Any) -> Any:
    if isinstance(value, str) and value.isdigit():
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _dedupe_preserve(seq: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in seq:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _load_toc_urls_from_file(path: str) -> list[str]:
    urls: list[str] = []
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return urls
    text_strip = text.strip()
    if not text_strip:
        return urls
    if text_strip.startswith("["):
        try:
            data = json.loads(text_strip)
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, str) and entry.strip():
                        urls.append(entry.strip())
            elif isinstance(data, dict):
                for entry in data.values():
                    if isinstance(entry, str) and entry.strip():
                        urls.append(entry.strip())
                    elif isinstance(entry, list):
                        urls.extend([str(v).strip() for v in entry if str(v).strip()])
        except json.JSONDecodeError:
            pass
    else:
        for line in text.splitlines():
            line = line.strip()
            if line:
                urls.append(line)
    return _dedupe_preserve(urls)


async def _ensure_indexes(obj, db_schema: str) -> None:
    if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
        cols = ", ".join(obj.__my_index_elements__)
        await db.status(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            + f"{obj.__tablename__}_idx_primary ON {db_schema}.{obj.__tablename__} ({cols});"
        )
    if hasattr(obj, "__my_additional_indexes__") and obj.__my_additional_indexes__:
        for idx in obj.__my_additional_indexes__:
            elements = idx.get("index_elements")
            if not elements:
                continue
            name = idx.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
            using = idx.get("using")
            where = idx.get("where")
            cols = ", ".join(elements)
            statement = f"CREATE INDEX IF NOT EXISTS {name} ON {db_schema}.{obj.__tablename__}"
            if using:
                statement += f" USING {using}"
            statement += f" ({cols})"
            if where:
                statement += f" WHERE {where}"
            statement += ";"
            await db.status(statement)


async def _prepare_ptg_tables(import_id: str, test_mode: bool) -> dict[str, type]:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        logger.warning("Failed to ensure schema %s exists (%s); falling back to public schema", db_schema, exc)
        db_schema = "public"
    dynamic: dict[str, type] = {}
    for cls in (
        PTGFile,
        PTGProviderGroup,
        PTGInNetworkItem,
        PTGBillingCode,
        PTGNegotiatedRate,
        PTGNegotiatedPrice,
        PTGAllowedItem,
        PTGAllowedPayment,
        PTGAllowedProviderPayment,
        ImportLog,
    ):
        obj = make_class(cls, import_id, schema_override=db_schema)
        dynamic[cls.__name__] = obj
        try:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        except Exception as exc:
            logger.debug("PTG drop table %s failed: %s", obj.__tablename__, exc)
        try:
            await db.create_table(obj.__table__, checkfirst=True)
        except Exception as exc:
            logger.warning("PTG create table %s failed: %s", obj.__tablename__, exc)
        await _ensure_indexes(obj, db_schema)
    return dynamic


def _maybe_unzip(path: str) -> str:
    if path.endswith(".gz"):
        target = f"{path[:-3]}_unzipped.json"
        with gzip.open(path, "rb") as gz_file, open(target, "wb") as out_file:
            out_file.write(gz_file.read())
        return target

    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path, "r") as zip_ref:
            for name in zip_ref.namelist():
                if not name.endswith("/"):
                    target = os.path.join(os.path.dirname(path), name)
                    zip_ref.extract(name, os.path.dirname(path))
                    return target
    return path


async def _extract_metadata_fields(file_path: str) -> dict[str, Any]:
    """
    Extract top-level metadata without loading the full file.
    """
    fields = {
        "reporting_entity_name",
        "reporting_entity_type",
        "plan_name",
        "plan_id_type",
        "plan_id",
        "plan_market_type",
        "issuer_name",
        "plan_sponsor_name",
        "last_updated_on",
        "version",
    }
    meta: dict[str, Any] = {}
    async with async_open(file_path, "rb") as afp:
        async for prefix, event, value in ijson.parse(afp):
            if event in ("string", "number") and prefix in fields:
                meta[prefix] = value
                if len(meta) == len(fields):
                    break
    return meta


def _derive_plan_fields(meta: dict[str, Any], plan_info: list[dict[str, Any]] | None) -> dict[str, Any]:
    if any(meta.get(k) for k in ("plan_name", "plan_id", "plan_market_type", "plan_sponsor_name", "issuer_name")):
        return {
            "plan_name": meta.get("plan_name"),
            "plan_id_type": meta.get("plan_id_type"),
            "plan_id": meta.get("plan_id"),
            "plan_market_type": meta.get("plan_market_type"),
            "issuer_name": meta.get("issuer_name"),
            "plan_sponsor_name": meta.get("plan_sponsor_name"),
        }
    if plan_info:
        if len(plan_info) == 1:
            single = plan_info[0]
            return {
                "plan_name": single.get("plan_name"),
                "plan_id_type": single.get("plan_id_type"),
                "plan_id": single.get("plan_id"),
                "plan_market_type": single.get("plan_market_type"),
                "issuer_name": single.get("issuer_name"),
                "plan_sponsor_name": single.get("plan_sponsor_name"),
            }
    return {}


def _build_file_row(
    url: str,
    file_type: str,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    description: str | None,
    from_index_url: str | None,
) -> dict[str, Any]:
    plan_fields = _derive_plan_fields(meta, plan_info)
    file_id = _make_checksum(url, file_type, plan_fields.get("plan_id") or plan_fields.get("plan_name") or "")
    return {
        "file_id": file_id,
        "file_type": file_type,
        "url": url,
        "description": description,
        "reporting_entity_name": meta.get("reporting_entity_name"),
        "reporting_entity_type": meta.get("reporting_entity_type"),
        "last_updated_on": _coerce_date(meta.get("last_updated_on")),
        "version": meta.get("version"),
        "plan_name": plan_fields.get("plan_name"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_id": plan_fields.get("plan_id"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
        "from_index_url": from_index_url,
    }


async def _load_provider_references_from_file(
    file_path: str,
    file_id: int,
    provider_cls,
    provider_map: dict[int, list[dict[str, Any]]],
    test_mode: bool,
    log_cls,
    source_url: str,
) -> None:
    rows: list[dict[str, Any]] = []
    seen_hashes: set[int] = set()
    async with async_open(file_path, "rb") as afp:
        async for ref in ijson.items(afp, "provider_references.item"):
            provider_group_id = _normalize_provider_ref(ref.get("provider_group_id"))
            network_names = ref.get("network_name") or ref.get("network_names") or []
            for group in ref.get("provider_groups", []):
                tin_info = group.get("tin") or {}
                npi_list = group.get("npi") or []
                provider_hash = _make_checksum(
                    file_id,
                    provider_group_id or "",
                    tin_info.get("value") or "",
                    "|".join(str(n) for n in npi_list),
                )
                if provider_hash in seen_hashes:
                    continue
                seen_hashes.add(provider_hash)
                rows.append(
                    {
                        "provider_group_hash": provider_hash,
                        "provider_group_ref": provider_group_id,
                        "file_id": file_id,
                        "network_names": network_names,
                        "tin_type": tin_info.get("type"),
                        "tin_value": tin_info.get("value"),
                        "tin_business_name": tin_info.get("business_name"),
                        "npi": npi_list,
                    }
                )
                provider_entry = dict(group)
                provider_entry["network_name"] = network_names
                provider_entry["__hash__"] = provider_hash
                provider_entry["provider_group_id"] = provider_group_id
                provider_map.setdefault(provider_group_id, []).append(provider_entry)
            if test_mode and len(rows) >= TEST_PROVIDER_GROUPS:
                break
    if rows:
        await push_objects(rows, provider_cls)
    await flush_error_log(log_cls)
    return None


async def _parse_in_network_items(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map: dict[int, list[dict[str, Any]]],
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
) -> None:
    provider_cls = classes["PTGProviderGroup"]
    item_cls = classes["PTGInNetworkItem"]
    billing_cls = classes["PTGBillingCode"]
    rate_cls = classes["PTGNegotiatedRate"]
    price_cls = classes["PTGNegotiatedPrice"]

    item_rows: list[dict[str, Any]] = []
    billing_rows: list[dict[str, Any]] = []
    rate_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    provider_rows: list[dict[str, Any]] = []
    provider_hash_seen: set[int] = set(
        entry["__hash__"]
        for groups in provider_map.values()
        for entry in groups
        if isinstance(entry, dict) and "__hash__" in entry
    )

    plan_fields = _derive_plan_fields(meta, plan_info)

    async with async_open(file_path, "rb") as afp:
        count = 0
        async for in_item in ijson.items(afp, "in_network.item", use_float=True):
            count += 1
            item_hash = _make_checksum(
                file_id,
                in_item.get("billing_code_type"),
                in_item.get("billing_code"),
                in_item.get("negotiation_arrangement"),
                in_item.get("name"),
            )
            item_rows.append(
                {
                    "item_hash": item_hash,
                    "file_id": file_id,
                    "negotiation_arrangement": in_item.get("negotiation_arrangement"),
                    "name": in_item.get("name"),
                    "billing_code_type": in_item.get("billing_code_type"),
                    "billing_code_type_version": in_item.get("billing_code_type_version"),
                    "billing_code": in_item.get("billing_code"),
                    "description": in_item.get("description"),
                    "severity_of_illness": in_item.get("severity_of_illness"),
                    "plan_name": plan_fields.get("plan_name"),
                    "plan_id_type": plan_fields.get("plan_id_type"),
                    "plan_id": plan_fields.get("plan_id"),
                    "plan_market_type": plan_fields.get("plan_market_type"),
                    "issuer_name": plan_fields.get("issuer_name"),
                    "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                }
            )
            for code in in_item.get("bundled_codes", []):
                billing_rows.append(
                    {
                        "code_hash": _make_checksum(item_hash, "bundle", code.get("billing_code")),
                        "item_hash": item_hash,
                        "code_role": "bundle",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )
            for code in in_item.get("covered_services", []):
                billing_rows.append(
                    {
                        "code_hash": _make_checksum(item_hash, "covered", code.get("billing_code")),
                        "item_hash": item_hash,
                        "code_role": "covered",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )

            for negotiated_rate in in_item.get("negotiated_rates", []):
                provider_refs = negotiated_rate.get("provider_references") or []
                provider_groups_inline = negotiated_rate.get("provider_groups") or []
                groups_to_use: list[dict[str, Any]] = []
                for provider_ref in provider_refs:
                    provider_key = _normalize_provider_ref(provider_ref)
                    groups = provider_map.get(provider_key) or provider_map.get(provider_ref)
                    if not groups:
                        await log_error(
                            "err",
                            f"Provider reference {provider_ref} not found for file {source_url}",
                            [0],
                            source_url,
                            "ptg-in-network",
                            "json",
                            import_log_cls,
                        )
                        continue
                    groups_to_use.extend(groups)
                for inline_group in provider_groups_inline:
                    inline_tin = inline_group.get("tin") or {}
                    inline_npi = inline_group.get("npi") or []
                    inline_hash = _make_checksum(
                        file_id, inline_tin.get("value") or "", "|".join(str(n) for n in inline_npi)
                    )
                    if inline_hash not in provider_hash_seen:
                        provider_hash_seen.add(inline_hash)
                        provider_rows.append(
                            {
                                "provider_group_hash": inline_hash,
                                "provider_group_ref": _normalize_provider_ref(inline_group.get("provider_group_id")),
                                "file_id": file_id,
                                "network_names": negotiated_rate.get("network_name") or [],
                                "tin_type": inline_tin.get("type"),
                                "tin_value": inline_tin.get("value"),
                                "tin_business_name": inline_tin.get("business_name"),
                                "npi": inline_npi,
                            }
                        )
                    groups_to_use.append(
                        {
                            **inline_group,
                            "network_name": negotiated_rate.get("network_name") or [],
                            "__hash__": inline_hash,
                        }
                    )
                for group in groups_to_use:
                    rate_hash = _make_checksum(item_hash, group.get("__hash__") or "")
                    rate_rows.append(
                        {
                            "rate_hash": rate_hash,
                            "item_hash": item_hash,
                            "provider_group_ref": group.get("provider_group_id"),
                            "provider_group_hash": group.get("__hash__"),
                        }
                    )
                    for negotiated_price in negotiated_rate.get("negotiated_prices", []):
                        price_rows.append(
                            {
                                "price_hash": _make_checksum(
                                    rate_hash,
                                    negotiated_price.get("negotiated_type"),
                                    negotiated_price.get("negotiated_rate"),
                                    negotiated_price.get("expiration_date") or "",
                                    negotiated_price.get("billing_class"),
                                    negotiated_price.get("setting"),
                                    "|".join(_as_list(negotiated_price.get("service_code"))),
                                    "|".join(_as_list(negotiated_price.get("billing_code_modifier"))),
                                ),
                                "rate_hash": rate_hash,
                                "negotiated_type": negotiated_price.get("negotiated_type"),
                                "negotiated_rate": negotiated_price.get("negotiated_rate"),
                                "expiration_date": _coerce_date(negotiated_price.get("expiration_date")),
                                "service_code": _as_list(negotiated_price.get("service_code")),
                                "billing_class": negotiated_price.get("billing_class"),
                                "setting": negotiated_price.get("setting"),
                                "billing_code_modifier": _as_list(negotiated_price.get("billing_code_modifier")),
                                "additional_information": negotiated_price.get("additional_information"),
                            }
                        )
                        if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                            break
                if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                    break

            if len(item_rows) >= 100:
                await push_objects(item_rows, item_cls)
                item_rows = []
            if len(billing_rows) >= 200:
                await push_objects(billing_rows, billing_cls)
                billing_rows = []
            if len(rate_rows) >= 200:
                await push_objects(rate_rows, rate_cls)
                rate_rows = []
            if len(price_rows) >= 200:
                await push_objects(price_rows, price_cls)
                price_rows = []

            if test_mode and count >= TEST_IN_NETWORK_ITEMS:
                break

    if item_rows:
        await push_objects(item_rows, item_cls)
    if billing_rows:
        await push_objects(billing_rows, billing_cls)
    if rate_rows:
        await push_objects(rate_rows, rate_cls)
    if price_rows:
        await push_objects(price_rows, price_cls)
    if provider_rows:
        await push_objects(provider_rows, provider_cls)
    await flush_error_log(import_log_cls)


async def _parse_allowed_amounts(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
) -> None:
    item_cls = classes["PTGAllowedItem"]
    payment_cls = classes["PTGAllowedPayment"]
    provider_payment_cls = classes["PTGAllowedProviderPayment"]

    plan_fields = _derive_plan_fields(meta, plan_info)

    item_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    provider_payment_rows: list[dict[str, Any]] = []

    async with async_open(file_path, "rb") as afp:
        count = 0
        async for out_item in ijson.items(afp, "out_of_network.item", use_float=True):
            count += 1
            item_hash = _make_checksum(
                file_id,
                out_item.get("billing_code_type"),
                out_item.get("billing_code"),
                out_item.get("name"),
            )
            item_rows.append(
                {
                    "allowed_item_hash": item_hash,
                    "file_id": file_id,
                    "name": out_item.get("name"),
                    "billing_code_type": out_item.get("billing_code_type"),
                    "billing_code_type_version": out_item.get("billing_code_type_version"),
                    "billing_code": out_item.get("billing_code"),
                    "description": out_item.get("description"),
                    "plan_name": plan_fields.get("plan_name"),
                    "plan_id_type": plan_fields.get("plan_id_type"),
                    "plan_id": plan_fields.get("plan_id"),
                    "plan_market_type": plan_fields.get("plan_market_type"),
                    "issuer_name": plan_fields.get("issuer_name"),
                    "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                }
            )
            for allowed_amount in out_item.get("allowed_amounts", []):
                tin_info = allowed_amount.get("tin") or {}
                for payment in allowed_amount.get("payments", []):
                    payment_hash = _make_checksum(
                        item_hash,
                        tin_info.get("value") or "",
                        "|".join(_as_list(allowed_amount.get("service_code"))),
                        allowed_amount.get("billing_class") or "",
                        allowed_amount.get("setting") or "",
                        payment.get("allowed_amount"),
                        "|".join(_as_list(payment.get("billing_code_modifier"))),
                    )
                    payment_rows.append(
                        {
                            "payment_hash": payment_hash,
                            "allowed_item_hash": item_hash,
                            "tin_type": tin_info.get("type"),
                            "tin_value": tin_info.get("value"),
                            "service_code": _as_list(allowed_amount.get("service_code")),
                            "billing_class": allowed_amount.get("billing_class"),
                            "setting": allowed_amount.get("setting"),
                            "allowed_amount": payment.get("allowed_amount"),
                            "billing_code_modifier": _as_list(payment.get("billing_code_modifier")),
                        }
                    )
                    for provider in payment.get("providers", []):
                        provider_payment_rows.append(
                            {
                                "provider_payment_hash": _make_checksum(
                                    payment_hash,
                                    provider.get("billed_charge"),
                                    "|".join(str(n) for n in _as_list(provider.get("npi"))),
                                ),
                                "payment_hash": payment_hash,
                                "billed_charge": provider.get("billed_charge"),
                                "npi": _as_list(provider.get("npi")),
                            }
                        )
            if len(item_rows) >= 100:
                await push_objects(item_rows, item_cls)
                item_rows = []
            if len(payment_rows) >= 200:
                await push_objects(payment_rows, payment_cls)
                payment_rows = []
            if len(provider_payment_rows) >= 200:
                await push_objects(provider_payment_rows, provider_payment_cls)
                provider_payment_rows = []
            if test_mode and count >= TEST_ALLOWED_ITEMS:
                break

    if item_rows:
        await push_objects(item_rows, item_cls)
    if payment_rows:
        await push_objects(payment_rows, payment_cls)
    if provider_payment_rows:
        await push_objects(provider_payment_rows, provider_payment_cls)
    await flush_error_log(import_log_cls)


async def _process_table_of_contents(
    toc_url: str,
    classes: dict[str, type],
    test_mode: bool,
) -> list[dict[str, Any]]:
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    jobs: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    seen_files: set[int] = set()

    with tempfile.TemporaryDirectory() as tmpdir:
        target = str(PurePath(tmpdir, Path(toc_url).name or "table-of-contents.json"))
        try:
            await download_it_and_save(
                toc_url,
                target,
                context={"issuer_array": [0], "source": "ptg-toc"},
                logger=import_log_cls,
            )
        except Exception as exc:
            logger.warning("Failed to download table-of-contents from %s: %s", toc_url, exc)
            return []
        extracted = _maybe_unzip(target)
        async with async_open(extracted, "r") as afp:
            toc_content = json.loads(await afp.read())

    toc_meta = {
        "reporting_entity_name": toc_content.get("reporting_entity_name"),
        "reporting_entity_type": toc_content.get("reporting_entity_type"),
        "last_updated_on": toc_content.get("last_updated_on"),
        "version": toc_content.get("version"),
    }
    file_rows.append(
        _build_file_row(
            toc_url,
            "table-of-contents",
            toc_meta,
            None,
            toc_content.get("description"),
            None,
        )
    )

    for structure in toc_content.get("reporting_structure", []):
        plans = structure.get("reporting_plans") or []
        in_network_files = structure.get("in_network_files") or []
        allowed_amount_file = structure.get("allowed_amount_file")

        for entry in in_network_files:
            location = entry.get("location")
            if not location:
                continue
            meta = dict(toc_meta)
            file_row = _build_file_row(location, "in-network", meta, plans, entry.get("description"), toc_url)
            if file_row["file_id"] not in seen_files:
                file_rows.append(file_row)
                seen_files.add(file_row["file_id"])
            jobs.append(
                {
                    "type": "in_network",
                    "url": location,
                    "description": entry.get("description"),
                    "plan_info": plans,
                    "from_index_url": toc_url,
                    "meta": meta,
                }
            )
            if test_mode and len(jobs) >= TEST_TOC_JOBS:
                break

        if allowed_amount_file:
            location = allowed_amount_file.get("location")
            if location:
                meta = dict(toc_meta)
                file_row = _build_file_row(
                    location, "allowed-amounts", meta, plans, allowed_amount_file.get("description"), toc_url
                )
                if file_row["file_id"] not in seen_files:
                    file_rows.append(file_row)
                    seen_files.add(file_row["file_id"])
                jobs.append(
                    {
                        "type": "allowed_amounts",
                        "url": location,
                        "description": allowed_amount_file.get("description"),
                        "plan_info": plans,
                        "from_index_url": toc_url,
                        "meta": meta,
                    }
                )
        if test_mode and len(jobs) >= TEST_TOC_JOBS:
            break

    if file_rows:
        await push_objects(file_rows, file_cls, rewrite=True)
    await flush_error_log(import_log_cls)
    return jobs


async def _process_provider_reference_file(
    url: str,
    classes: dict[str, type],
    test_mode: bool,
) -> dict[int, list[dict[str, Any]]]:
    provider_cls = classes["PTGProviderGroup"]
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    provider_map: dict[int, list[dict[str, Any]]] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        target = str(PurePath(tmpdir, Path(url).name or "provider-reference.json"))
        try:
            await download_it_and_save(
                url,
                target,
                context={"issuer_array": [0], "source": "ptg-provider-reference"},
                logger=import_log_cls,
            )
        except Exception as exc:
            logger.warning("Failed to download provider-reference from %s: %s", url, exc)
            return provider_map
        extracted = _maybe_unzip(target)
        async with async_open(extracted, "r") as afp:
            provider_content = json.loads(await afp.read())

    meta = {
        "version": provider_content.get("version"),
    }
    file_row = _build_file_row(url, "provider-reference", meta, None, None, None)
    await push_objects([file_row], file_cls, rewrite=True)

    provider_groups = provider_content.get("provider_groups") or []
    rows: list[dict[str, Any]] = []
    for idx, group in enumerate(provider_groups):
        tin_info = group.get("tin") or {}
        npi_list = group.get("npi") or []
        provider_group_ref = _normalize_provider_ref(
            group.get("provider_group_id") or group.get("provider_group_ref") or (idx + 1)
        )
        provider_hash = _make_checksum(file_row["file_id"], provider_group_ref, tin_info.get("value") or "")
        rows.append(
            {
                "provider_group_hash": provider_hash,
                "provider_group_ref": provider_group_ref,
                "file_id": file_row["file_id"],
                "network_names": group.get("network_name") or group.get("network_names") or [],
                "tin_type": tin_info.get("type"),
                "tin_value": tin_info.get("value"),
                "tin_business_name": tin_info.get("business_name"),
                "npi": npi_list,
            }
        )
        provider_map.setdefault(provider_group_ref, []).append(
            {
                **group,
                "network_name": group.get("network_name") or [],
                "__hash__": provider_hash,
                "provider_group_id": provider_group_ref,
            }
        )
        if test_mode and len(rows) >= TEST_PROVIDER_GROUPS:
            break

    if rows:
        await push_objects(rows, provider_cls)
    await flush_error_log(import_log_cls)
    return provider_map


async def _process_in_network_file(
    job: dict[str, Any],
    classes: dict[str, type],
    provider_ref_cache: dict[int, list[dict[str, Any]]],
    test_mode: bool,
) -> None:
    url = job["url"]
    description = job.get("description")
    plan_info = job.get("plan_info")
    from_index_url = job.get("from_index_url")
    provided_meta = job.get("meta") or {}

    file_cls = classes["PTGFile"]
    provider_cls = classes["PTGProviderGroup"]
    import_log_cls = classes["ImportLog"]

    with tempfile.TemporaryDirectory() as tmpdir:
        target = str(PurePath(tmpdir, Path(url).name or "in-network.json"))
        try:
            await download_it_and_save(
                url,
                target,
                context={"issuer_array": [0], "source": "ptg-in-network"},
                logger=import_log_cls,
            )
        except Exception as exc:
            logger.warning("Failed to download in-network file from %s: %s", url, exc)
            return
        extracted = _maybe_unzip(target)
        meta = provided_meta or await _extract_metadata_fields(extracted)
        file_row = _build_file_row(url, "in-network", meta, plan_info, description, from_index_url)
        await push_objects([file_row], file_cls, rewrite=True)
        provider_map = {k: [dict(x) for x in v] for k, v in provider_ref_cache.items()}
        await _load_provider_references_from_file(
            extracted, file_row["file_id"], provider_cls, provider_map, test_mode, import_log_cls, url
        )
        await _parse_in_network_items(
            extracted, file_row["file_id"], meta, plan_info, provider_map, classes, test_mode, import_log_cls, url
        )


async def _process_allowed_amounts_file(
    job: dict[str, Any],
    classes: dict[str, type],
    test_mode: bool,
) -> None:
    url = job["url"]
    description = job.get("description")
    plan_info = job.get("plan_info")
    from_index_url = job.get("from_index_url")
    provided_meta = job.get("meta") or {}

    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]

    with tempfile.TemporaryDirectory() as tmpdir:
        target = str(PurePath(tmpdir, Path(url).name or "allowed-amounts.json"))
        try:
            await download_it_and_save(
                url,
                target,
                context={"issuer_array": [0], "source": "ptg-allowed-amounts"},
                logger=import_log_cls,
            )
        except Exception as exc:
            logger.warning("Failed to download allowed-amounts file from %s: %s", url, exc)
            return
        extracted = _maybe_unzip(target)
        meta = provided_meta or await _extract_metadata_fields(extracted)
        file_row = _build_file_row(url, "allowed-amounts", meta, plan_info, description, from_index_url)
        await push_objects([file_row], file_cls, rewrite=True)
        await _parse_allowed_amounts(
            extracted, file_row["file_id"], meta, plan_info, classes, test_mode, import_log_cls, url
        )


async def main(
    test_mode: bool = False,
    toc_urls: list[str] | None = None,
    toc_list: str | None = None,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    provider_ref_url: str | None = None,
    import_id: str | None = None,
) -> None:
    """
    Entry point for the Transparency in Coverage importer.
    """
    import_id_val = _normalize_import_id(import_id)
    await ensure_database(test_mode)
    classes = await _prepare_ptg_tables(import_id_val, test_mode)

    provider_ref_cache: dict[int, list[dict[str, Any]]] = {}
    jobs: list[dict[str, Any]] = []

    toc_candidates: list[str] = []
    if toc_urls:
        toc_candidates.extend([u for u in toc_urls if u])
    if toc_list:
        toc_candidates.extend(_load_toc_urls_from_file(toc_list))
    toc_candidates = _dedupe_preserve([u.strip() for u in toc_candidates if u.strip()])

    for idx, toc_url in enumerate(toc_candidates):
        if test_mode and idx >= TEST_TOC_FILES:
            break
        toc_jobs = await _process_table_of_contents(toc_url, classes, test_mode)
        jobs.extend(toc_jobs)

    if provider_ref_url:
        provider_ref_cache.update(await _process_provider_reference_file(provider_ref_url, classes, test_mode))

    if in_network_url:
        jobs.append({"type": "in_network", "url": in_network_url})
    if allowed_url:
        jobs.append({"type": "allowed_amounts", "url": allowed_url})

    seen_jobs: set[tuple[str, str]] = set()
    for job in jobs:
        job_key = (job.get("type"), job.get("url"))
        if job_key in seen_jobs:
            continue
        seen_jobs.add(job_key)
        if job.get("type") == "in_network":
            await _process_in_network_file(job, classes, provider_ref_cache, test_mode)
        elif job.get("type") == "allowed_amounts":
            await _process_allowed_amounts_file(job, classes, test_mode)

    await flush_error_log(classes["ImportLog"])


__all__ = ["main"]
