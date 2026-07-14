# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import glob
import hashlib
import os
import re
import sys
import tempfile
import time
import uuid
import zipfile
from pathlib import PurePath

import pytz
from aiocsv import AsyncDictReader
from aiofile import async_open
from arq import create_pool
from arq.connections import RedisSettings
from asyncpg import DuplicateTableError
from dateutil.parser import parse as parse_date
from sqlalchemy import func, select

from db.models import (AddressArchive, NPIAddress, NPIData,
                       NPIDataOtherIdentifier, NPIDataTaxonomy,
                       NPIDataTaxonomyGroup, NPIPhoneStaffing, db)
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import mark_control_run
from process.ext.archive import unzip
from process.ext.address_canon import (
    address_key_v1,
    archive_table_name,
    resolve_into_archive,
    source_enabled,
    stamp_address_keys,
)
from process.ext.address_fast import canonicalize_batch as canonicalize_address_batch
from process.ext.contact_canon import canonicalize_batch as canonicalize_contact_batch
from process.ext.utils import (download_it, download_it_and_save,
                               ensure_database, make_class, my_init_db,
                               print_time_info, push_objects, return_checksum)
from process.live_progress import enqueue_live_progress
from process.openaddresses import refresh_archive_geocodes_from_openaddresses_sharded
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

latin_pattern= re.compile(r'[^\x00-\x7f]')

TEST_NPI_MAX_FILES = 1
TEST_NPI_ROWS = 1000
TEST_NPI_OTHER_ROWS = 500
TEST_NPI_SECONDARY_ROWS = 1000
NPI_QUEUE_NAME = "arq:NPI"
DEFAULT_NPI_MAX_PENDING_SAVE_TASKS = 4
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
ADDRESS_KEY_MISMATCH_MESSAGE = "Stamped canonical address key does not match identity_key"


class NPIPrerequisiteError(RuntimeError):
    """Raised when a full NPI import would publish incomplete derived data."""


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _index_requires_postgis(index: dict) -> bool:
    name = str(index.get("name") or "").lower()
    if name.startswith("geo_index_") or name.endswith("_geo_idx") or name == "geo_idx":
        return True
    elements = " ".join(str(element).lower() for element in index.get("index_elements") or ())
    return "st_makepoint" in elements or "geography" in elements


def _npi_requires_nucc(context: dict | None = None) -> bool:
    context = context or {}
    if context.get("test_mode"):
        return _env_bool("HLTHPRT_NPI_REQUIRE_NUCC_IN_TEST", False)
    return _env_bool("HLTHPRT_NPI_REQUIRE_NUCC", True)


async def _assert_nucc_ready(schema: str) -> None:
    exists = await db.scalar(f"SELECT to_regclass('{schema}.nucc_taxonomy');")
    if not exists:
        raise NPIPrerequisiteError(
            f"NPI import requires {schema}.nucc_taxonomy. Run the NUCC importer before NPI "
            "or set HLTHPRT_NPI_REQUIRE_NUCC=0 to explicitly skip NPI phone-staffing output."
        )
    total_rows = int(await db.scalar(f"SELECT count(*) FROM {schema}.nucc_taxonomy;") or 0)
    pharmacist_rows = int(
        await db.scalar(
            f"""
            SELECT count(*)
             FROM {schema}.nucc_taxonomy
             WHERE lower(classification) = 'pharmacist'
               AND int_code IS NOT NULL;
            """
        )
        or 0
    )
    if total_rows <= 0 or pharmacist_rows <= 0:
        raise NPIPrerequisiteError(
            f"NPI import requires populated {schema}.nucc_taxonomy with pharmacist taxonomy codes. "
            f"Found rows={total_rows}, pharmacist_rows={pharmacist_rows}. Run `python main.py start nucc` "
            "and its worker before NPI, or set HLTHPRT_NPI_REQUIRE_NUCC=0 to explicitly skip "
            "NPI phone-staffing output."
        )


async def _assert_nppes_canonical_ready(schema: str) -> None:
    if not source_enabled("nppes"):
        return
    function_exists = await db.scalar(
        f"SELECT to_regprocedure('{schema}.addr_key_v1(text,text,text,text,text,text)');"
    )
    if not function_exists:
        raise NPIPrerequisiteError(
            f"NPI canonical address mode requires {schema}.addr_key_v1(...). "
            "Run the address-canonical migration before enabling HLTHPRT_ADDRESS_CANON_SOURCES=nppes."
        )
    archive_table = archive_table_name()
    archive_exists = await db.scalar(f"SELECT to_regclass('{schema}.{archive_table}');")
    if not archive_exists:
        raise NPIPrerequisiteError(
            f"NPI canonical address mode requires {schema}.{archive_table}. "
            "Run address-archive-v2 migration/backfill or set HLTHPRT_ADDRESS_ARCHIVE_TABLE correctly."
        )
    archive_has_key = await db.scalar(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM information_schema.columns
             WHERE table_schema = '{schema}'
               AND table_name = '{archive_table}'
               AND column_name = 'address_key'
        );
        """
    )
    if not archive_has_key:
        raise NPIPrerequisiteError(
            f"NPI canonical address mode requires {schema}.{archive_table}.address_key."
        )


async def _ensure_required_extensions() -> None:
    for extension in ("pg_trgm", "intarray", "btree_gin"):
        await db.status(f"CREATE EXTENSION IF NOT EXISTS {extension};")


def is_test_mode(ctx: dict) -> bool:
    """Return whether the active NPI worker context is in test mode."""
    return bool(ctx.get("context", {}).get("test_mode"))


def _attach_npi_address_key(address: dict, *, canonical_enabled: bool) -> dict:
    return _attach_npi_address_keys([address], canonical_enabled=canonical_enabled)[0]


def _npi_address_canon_row(address: dict) -> tuple[object, object, object, object, object, object]:
    return (
        address.get("first_line"),
        address.get("second_line"),
        address.get("city_name"),
        address.get("state_name"),
        address.get("postal_code"),
        address.get("country_code") or "US",
    )


def _npi_contact_canon_row(address: dict) -> tuple[object, object, object]:
    return (
        address.get("telephone_number"),
        address.get("fax_number"),
        address.get("country_code") or "US",
    )


def _attach_npi_contact_fields(addresses) -> list[dict]:
    address_list = list(addresses)
    if not address_list:
        return address_list
    canonical_rows = canonicalize_contact_batch(_npi_contact_canon_row(address) for address in address_list)
    for address, canonical in zip(address_list, canonical_rows):
        address["phone_number"] = canonical.get("phone_number")
        address["phone_extension"] = canonical.get("phone_extension")
        address["fax_number_digits"] = canonical.get("fax_number_digits") or canonical.get("fax_number")
        address["fax_extension"] = canonical.get("fax_extension")
    return address_list


def _attach_npi_address_keys(addresses, *, canonical_enabled: bool) -> list[dict]:
    address_list = list(addresses)
    if not canonical_enabled:
        return address_list
    canonical_rows = canonicalize_address_batch(_npi_address_canon_row(address) for address in address_list)
    for address, canonical in zip(address_list, canonical_rows):
        address_key = canonical.get("address_key")
        address["address_key"] = uuid.UUID(address_key) if isinstance(address_key, str) else address_key
    return address_list


def _prepare_npi_address_rows(addresses, *, canonical_enabled: bool) -> list[dict]:
    return _attach_npi_address_keys(
        _attach_npi_contact_fields(addresses),
        canonical_enabled=canonical_enabled,
    )


def _is_address_key_mismatch_error(exc: BaseException) -> bool:
    return ADDRESS_KEY_MISMATCH_MESSAGE in str(exc)


async def _load_nucc_taxonomy_int_code_map(schema: str) -> dict[str, int]:
    if not await db.scalar(f"SELECT to_regclass('{schema}.nucc_taxonomy');"):
        return {}
    rows = await db.all(
        f"""
        SELECT code, int_code
          FROM {schema}.nucc_taxonomy
         WHERE code IS NOT NULL
           AND int_code IS NOT NULL;
        """
    )
    return {str(row[0]): int(row[1]) for row in rows if row and row[0] is not None and row[1] is not None}


def _taxonomy_array_from_npi_row(row: dict, taxonomy_int_code_map: dict[str, int] | None) -> list[int]:
    if not taxonomy_int_code_map:
        return [0]
    int_codes: set[int] = set()
    for i in range(1, 16):
        taxonomy_code = row.get(f'Healthcare Provider Taxonomy Code_{i}')
        if not taxonomy_code:
            break
        int_code = taxonomy_int_code_map.get(taxonomy_code)
        if int_code is not None:
            int_codes.add(int(int_code))
    return sorted(int_codes) if int_codes else [0]


async def process_npi_chunk(ctx, task):
    """Parse one NPPES CSV chunk and enqueue or persist normalized rows."""
    redis = ctx['redis']
    canonical_addresses_enabled = source_enabled("nppes")
    taxonomy_int_code_map = task.get("taxonomy_int_code_map") or {}

    npi_obj_list = []
    npi_taxonomy_list_dict = {}
    npi_other_id_list_dict = {}
    npi_taxonomy_group_list_dict = {}
    npi_address_list_dict = {}

    npi_csv_map = task['npi_csv_map']
    npi_csv_map_reverse = task['npi_csv_map_reverse']
    for row in task['row_list']:
        obj = {}

        for key in npi_csv_map:
            t = row[key]
            if not t or str(t).upper() == '<UNAVAIL>':
                obj[npi_csv_map[key]] = None
                continue
            if npi_csv_map[key] in ('replacement_npi', 'entity_type_code', 'npi',):
                t = int(t)
            elif npi_csv_map[key].endswith('_date'):
                t = pytz.utc.localize(parse_date(t, fuzzy=True))

            obj[npi_csv_map[key]] = t

        npi_obj_list.append(obj)
        taxonomy_array = _taxonomy_array_from_npi_row(row, taxonomy_int_code_map)

        if row['Provider First Line Business Practice Location Address']:
            obj = {
                'first_line': row['Provider First Line Business Practice Location Address'],
                'second_line': row['Provider Second Line Business Practice Location Address'],
                'city_name': row.get('Provider Business Practice Location Address City Name', '').upper(),
                'state_name': row.get('Provider Business Practice Location Address State Name', '').upper(),
                'postal_code': row['Provider Business Practice Location Address Postal Code'],
                'country_code': row['Provider Business Practice Location Address Country Code (If outside U.S.)'],
            }

            obj.update({
                'checksum': return_checksum(list(obj.values())),  # addresses have blank symbols
                'npi': int(row['NPI']),
                'type': 'primary',
                'telephone_number': row['Provider Business Practice Location Address Telephone Number'],
                'fax_number': row['Provider Business Practice Location Address Fax Number'],
                'taxonomy_array': taxonomy_array,
                'date_added': pytz.utc.localize(parse_date(row['Last Update Date'], fuzzy=True))
                if row['Last Update Date']
                else None,
            })
            npi_address_list_dict['_'.join([str(obj['npi']), str(obj['checksum']), obj['type'],])] = obj

        if row['Provider First Line Business Mailing Address']:
            obj = {
                'first_line': row['Provider First Line Business Mailing Address'],
                'second_line': row['Provider Second Line Business Mailing Address'],
                'city_name': row.get('Provider Business Mailing Address City Name', '').upper(),
                'state_name': row.get('Provider Business Mailing Address State Name', '').upper(),
                'postal_code': row['Provider Business Mailing Address Postal Code'],
                'country_code': row['Provider Business Mailing Address Country Code (If outside U.S.)'],
            }

            obj.update({
                'checksum': return_checksum(list(obj.values())),  # addresses have blank symbols
                'npi': int(row['NPI']),
                'type': 'mail',
                'telephone_number': row['Provider Business Mailing Address Telephone Number'],
                'fax_number': row['Provider Business Mailing Address Fax Number'],
                'taxonomy_array': taxonomy_array,
                'date_added': pytz.utc.localize(parse_date(row['Last Update Date'], fuzzy=True))
                if row['Last Update Date']
                else None,
            })
            npi_address_list_dict['_'.join([str(obj['npi']), str(obj['checksum']), obj['type'],])] = obj

        for i in range(1, 16):
            if row[f'Healthcare Provider Taxonomy Code_{i}']:
                t = {
                    'npi': int(row[npi_csv_map_reverse['npi']]),
                    'healthcare_provider_taxonomy_code': row[f'Healthcare Provider Taxonomy Code_{i}'],
                    'provider_license_number': row[f'Provider License Number_{i}'],
                    'provider_license_number_state_code': row[f'Provider License Number State Code_{i}'],
                    'healthcare_provider_primary_taxonomy_switch': row[
                        f'Healthcare Provider Primary Taxonomy Switch_{i}']
                }
                checksum = return_checksum(list(t.values()))
                t['checksum'] = checksum
                npi_taxonomy_list_dict[checksum] = t
            else:
                break

        for i in range(1, 51):
            if row[f'Other Provider Identifier_{i}']:
                t = {
                    'npi': int(row[npi_csv_map_reverse['npi']]),
                    'other_provider_identifier': row[f'Other Provider Identifier_{i}'],
                    'other_provider_identifier_type_code': row[f'Other Provider Identifier Type Code_{i}'],
                    'other_provider_identifier_state': row[f'Other Provider Identifier State_{i}'],
                    'other_provider_identifier_issuer': row[f'Other Provider Identifier Issuer_{i}']
                }
                checksum = return_checksum(list(t.values()))
                t['checksum'] = checksum
                npi_other_id_list_dict[checksum] = t
            else:
                break

        for i in range(1, 16):
            if row[f'Healthcare Provider Taxonomy Group_{i}']:
                t = {
                    'npi': int(row[npi_csv_map_reverse['npi']]),
                    'healthcare_provider_taxonomy_group': row[f'Healthcare Provider Taxonomy Group_{i}'],
                }
                checksum = return_checksum(list(t.values()))
                t['checksum'] = checksum
                npi_taxonomy_group_list_dict[checksum] = t
            else:
                break

    payload = {
        'npi_obj_list': npi_obj_list,
        'npi_taxonomy_list': list(npi_taxonomy_list_dict.values()),
        'npi_other_id_list': list(npi_other_id_list_dict.values()),
        'npi_taxonomy_group_list': list(npi_taxonomy_group_list_dict.values()),
        'npi_address_list': _prepare_npi_address_rows(
            npi_address_list_dict.values(),
            canonical_enabled=canonical_addresses_enabled,
        ),
    }
    if task.get('direct'):
        await save_npi_data(ctx, payload)
        print(f'Processing.. {len(npi_obj_list)} rows directly')
    else:
        await redis.enqueue_job('save_npi_data', payload, _queue_name=NPI_QUEUE_NAME)



async def process_data(ctx, task=None):  # pragma: no cover
    """Download and process the configured NPPES dissemination files."""
    # Track whether any work actually ran so shutdown can distinguish "no jobs" from a bad import
    task = task or {}
    await raise_if_cancelled(ctx, task)
    ctx.setdefault('context', {})
    context = ctx['context']
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    if 'test_mode' in task:
        context['test_mode'] = bool(task.get('test_mode'))
    test_mode = bool(context.get('test_mode', False))
    canonical_addresses_enabled = source_enabled("nppes")
    await ensure_database(test_mode)
    await _ensure_required_extensions()

    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    if _npi_requires_nucc(context):
        await _assert_nucc_ready(db_schema)
    if canonical_addresses_enabled:
        await _assert_nppes_canonical_ready(db_schema)
    taxonomy_int_code_map = await _load_nucc_taxonomy_int_code_map(db_schema)
    if taxonomy_int_code_map:
        print(f"Loaded {len(taxonomy_int_code_map)} NUCC taxonomy integer codes for NPI address load.")
    print(os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    html_source = await download_it(
        os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    # re./NPPES_Data_Dissemination_110722_111322_Weekly.zip">NPPES Data Dissemination - Weekly Update -
    # 110722_111322</a>
    count_files = 0
    sql_chunk_size = 299999
    max_pending_save_tasks = _env_positive_int(
        "HLTHPRT_NPI_MAX_PENDING_SAVE_TASKS",
        DEFAULT_NPI_MAX_PENDING_SAVE_TASKS,
    )

    async def enqueue_or_flush(coros: list, coro) -> None:
        """Bound pending save tasks by flushing the active task batch."""
        coros.append(asyncio.create_task(coro))
        if len(coros) >= max_pending_save_tasks:
            await asyncio.gather(*coros)
            coros.clear()

    file_limit = TEST_NPI_MAX_FILES if test_mode else None
    source_files = re.findall(r'(NPPES_Data_Dissemination.*_V2.zip)', html_source)
    selected_files = source_files[:file_limit] if file_limit else source_files
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="npi",
            status="running",
            phase="npi sources discovered",
            unit="files",
            done=0,
            total=len(selected_files),
            message=f"{len(selected_files)} source files discovered",
        )
    for file_idx, p in enumerate(selected_files):
        await raise_if_cancelled(ctx, task)
        count_files = count_files + 1
        current_sql_chunk_size = sql_chunk_size
        if test_mode:
            current_sql_chunk_size = min(current_sql_chunk_size, TEST_NPI_ROWS)
        print(f"Round {count_files} for {p}")
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="npi",
                status="running",
                phase="npi downloading source",
                unit="files",
                done=file_idx,
                total=len(selected_files),
                message=f"downloading file {file_idx + 1}/{len(selected_files)}",
                label=p,
            )
        with tempfile.TemporaryDirectory() as tmpdirname:
            print(f"Found: {p}")
            # await unzip('/users/nick/downloads/NPPES_Data_Dissemination_November_2022.zip', tmpdirname, __debug=True)

            tmp_filename = str(PurePath(str(tmpdirname), p))
            await download_it_and_save(os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + p, tmp_filename,
                                       chunk_size=10 * 1024 * 1024, cache_dir='/tmp')
            print(f"Downloaded: {p}")
            if os.environ.get("DEBUG"):
                print(f"DEBUG: Downloaded file {tmp_filename}, size: {os.path.getsize(tmp_filename)} bytes")
                if os.path.getsize(tmp_filename) > 100 * 1024 * 1024:
                    print(f"File {tmp_filename} is too big, skipping")
                    continue
            else:
                print(f"Downloaded file size: {os.path.getsize(tmp_filename)} bytes")

            try:
                await unzip(tmp_filename, tmpdirname, buffer_size=10 * 1024 * 1024)
            except Exception:
                print(f"Failed to unzip {tmp_filename}, trying with zipfile")
                with zipfile.ZipFile(tmp_filename, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)

            npi_file = [fn for fn in glob.glob(f"{tmpdirname}/npi*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            pl_file = [fn for fn in glob.glob(f"{tmpdirname}/pl_pfile*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            other_file = [fn for fn in glob.glob(f"{tmpdirname}/other*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            if count_files > 1:
                # Collect all NPIs from npi_file and pl_file
                current_sql_chunk_size = sql_chunk_size // 26
                npi_set: set[int] = set()
                async with async_open(npi_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))

                for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataTaxonomy, NPIAddress):
                    table = make_class(cls, import_date)
                    npi_list = list(npi_set)
                    chunk_size = 1000
                    npi_column = getattr(table, 'npi')
                    for index in range(0, len(npi_list), chunk_size):
                        await raise_if_cancelled(ctx, task)
                        chunk = npi_list[index:index + chunk_size]
                        delete_stmt = db.delete(table.__table__).where(npi_column.in_(chunk))
                        if cls is NPIAddress:
                            delete_stmt = delete_stmt.where((table.type == 'primary') | (table.type == 'mail'))
                        await delete_stmt.status()
                print(f"Cleaned up models for {len(npi_set)} NPIs due to multiple files.")

                npi_set.clear()

                async with async_open(pl_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))

                table = make_class(NPIAddress, import_date)
                npi_list = list(npi_set)
                chunk_size = 10000
                npi_column = getattr(table, 'npi')
                for index in range(0, len(npi_list), chunk_size):
                    await raise_if_cancelled(ctx, task)
                    chunk = npi_list[index:index + chunk_size]
                    delete_stmt = db.delete(table.__table__).where(npi_column.in_(chunk))
                    delete_stmt = delete_stmt.where(table.type == 'secondary')
                    await delete_stmt.status()

                npi_set.clear()

                async with async_open(other_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))

                table = make_class(NPIDataOtherIdentifier, import_date)
                npi_list = list(npi_set)
                chunk_size = 10000
                npi_column = getattr(table, 'npi')
                for index in range(0, len(npi_list), chunk_size):
                    await raise_if_cancelled(ctx, task)
                    chunk = npi_list[index:index + chunk_size]
                    delete_stmt = db.delete(table.__table__).where(npi_column.in_(chunk))
                    await delete_stmt.status()

                print(f"Cleaned up models for {len(npi_set)} NPIs due to multiple files.")

            endpoint_file = [fn for fn in glob.glob(f"{tmpdirname}/endpoint*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            for t in (endpoint_file, other_file, pl_file, npi_file):
                print(f"Files: {t}")


            npi_csv_map = {}
            npi_csv_map_reverse = {}

            int_key_re = re.compile(r'.*_\d+$')

            async with async_open(npi_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    for key in row:
                        if int_key_re.match(key) or ' Address' in key:
                            continue
                        t = re.sub(r"\(.*\)", r"", key.lower()).strip().replace(' ', '_')
                        npi_csv_map[key] = t
                        npi_csv_map_reverse[t] = key
                    break
            count = 0


            row_list = []
            coros = []
            processed_rows = 0
            async with async_open(npi_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row['NPI']:
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Processed: {count}")
                    row_list.append(row)
                    processed_rows += 1
                    if run_id and processed_rows and processed_rows % (100 if test_mode else 100_000) == 0:
                        enqueue_live_progress(
                            run_id=run_id,
                            importer="npi",
                            status="running",
                            phase="npi parsing primary rows",
                            unit="rows",
                            done=processed_rows,
                            total=TEST_NPI_ROWS if test_mode else None,
                            message=f"parsed {processed_rows} primary rows",
                            label=p,
                        )
                    if count > current_sql_chunk_size:
                        print(f"Sending to DB: {count}")
                        await raise_if_cancelled(ctx, task)
                        payload = {
                            'row_list': row_list.copy(),
                            'npi_csv_map': npi_csv_map,
                            'npi_csv_map_reverse': npi_csv_map_reverse,
                            'taxonomy_int_code_map': taxonomy_int_code_map,
                            'direct': True,
                        }
                        await enqueue_or_flush(coros, process_npi_chunk(ctx, payload))
                        row_list.clear()
                        count = 0
                    else:
                        count += 1
                    if test_mode and processed_rows >= TEST_NPI_ROWS:
                        break

            payload = {
                'row_list': row_list.copy(),
                'npi_csv_map': npi_csv_map,
                'npi_csv_map_reverse': npi_csv_map_reverse,
                'taxonomy_int_code_map': taxonomy_int_code_map,
                'direct': True,
            }
            await raise_if_cancelled(ctx, task)
            await enqueue_or_flush(coros, process_npi_chunk(ctx, payload))
            row_list.clear()

            npi_other_org_list_dict = {}

            async with async_open(other_file, 'r') as afp:
                processed_other = 0
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row['NPI']:
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Other Names Processed: {count}")
                    obj = {
                        'npi': int(row['NPI']),
                        'other_provider_identifier': row['Provider Other Organization Name'],
                        'other_provider_identifier_type_code': row['Provider Other Organization Name Type Code'],
                        'other_provider_identifier_state': None,
                        'other_provider_identifier_issuer': None,
                    }
                    checksum = return_checksum(list(obj.values()))
                    obj['checksum'] = checksum
                    npi_other_org_list_dict[checksum] = obj

                    if count > current_sql_chunk_size:
                        print(f"Sending to DB: {count}")
                        await raise_if_cancelled(ctx, task)
                        other_payload = {
                            'npi_other_id_list': list(npi_other_org_list_dict.copy().values())
                        }
                        await enqueue_or_flush(coros, save_npi_data(ctx, other_payload))
                        npi_other_org_list_dict.clear()
                        count = 0
                    else:
                        count += 1
                    processed_other += 1
                    if run_id and processed_other and processed_other % (100 if test_mode else 100_000) == 0:
                        enqueue_live_progress(
                            run_id=run_id,
                            importer="npi",
                            status="running",
                            phase="npi parsing other identifiers",
                            unit="rows",
                            done=processed_other,
                            total=TEST_NPI_OTHER_ROWS if test_mode else None,
                            message=f"parsed {processed_other} other identifier rows",
                            label=p,
                        )
                    if test_mode and processed_other >= TEST_NPI_OTHER_ROWS:
                        break

            other_payload = {'npi_other_id_list': list(npi_other_org_list_dict.copy().values())}
            await enqueue_or_flush(coros, save_npi_data(ctx, other_payload))
            npi_other_org_list_dict.clear()


            npi_address_list_dict = {}
            async with async_open(pl_file, 'r') as afp:
                processed_secondary = 0
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row['NPI'] and not row['Provider Secondary Practice Location Address- Address Line 1']:
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Secondary Addresses Processed: {count}")
                    obj = {
                        'first_line': row['Provider Secondary Practice Location Address- Address Line 1'],
                        'second_line': row['Provider Secondary Practice Location Address-  Address Line 2'],
                        'city_name': row.get('Provider Secondary Practice Location Address - City Name', '').upper(),
                        'state_name': row.get('Provider Secondary Practice Location Address - State Name', '').upper(),
                        'postal_code': row['Provider Secondary Practice Location Address - Postal Code'],
                        'country_code': row['Provider Secondary Practice Location Address - Country Code (If outside U.S.)'],
                    }

                    obj.update({
                        'checksum': return_checksum(list(obj.values())),  # addresses have blank symbols
                        'npi': int(row['NPI']),
                        'type': 'secondary',
                        'telephone_number': row['Provider Secondary Practice Location Address - Telephone Number'],
                        'fax_number': row['Provider Practice Location Address - Fax Number'],
                        'date_added': pytz.utc.localize(datetime.datetime.now())
                    })
                    npi_address_list_dict['_'.join([str(obj['npi']), str(obj['checksum']), obj['type'], ])] = obj

                    if count > current_sql_chunk_size:
                        print(f"Sending Secondary to DB: {count}")
                        await raise_if_cancelled(ctx, task)
                        await enqueue_or_flush(
                            coros,
                            save_npi_data(
                                ctx,
                                {
                                    'npi_address_list': _prepare_npi_address_rows(
                                        npi_address_list_dict.copy().values(),
                                        canonical_enabled=canonical_addresses_enabled,
                                    )
                                },
                            ),
                        )
                        # await redis.enqueue_job('save_npi_data', {
                        #     'npi_address_list': list(npi_address_list_dict.values()),
                        # })
                        npi_address_list_dict.clear()
                        count = 0
                    else:
                        count += 1
                    processed_secondary += 1
                    if run_id and processed_secondary and processed_secondary % (100 if test_mode else 100_000) == 0:
                        enqueue_live_progress(
                            run_id=run_id,
                            importer="npi",
                            status="running",
                            phase="npi parsing secondary addresses",
                            unit="rows",
                            done=processed_secondary,
                            total=TEST_NPI_SECONDARY_ROWS if test_mode else None,
                            message=f"parsed {processed_secondary} secondary address rows",
                            label=p,
                        )
                    if test_mode and processed_secondary >= TEST_NPI_SECONDARY_ROWS:
                        print(f"Test mode: stopping secondary address scan at {processed_secondary} rows.")
                        break
            # await redis.enqueue_job('save_npi_data', {
            #     'npi_address_list': list(npi_address_list_dict.values()),
            # })
            await enqueue_or_flush(
                coros,
                save_npi_data(
                    ctx,
                    {
                        'npi_address_list': _prepare_npi_address_rows(
                            npi_address_list_dict.copy().values(),
                            canonical_enabled=canonical_addresses_enabled,
                        )
                    },
                ),
            )
            await raise_if_cancelled(ctx, task)
            await asyncio.gather(*coros)
            npi_address_list_dict.clear()

            print(f"Processed: {count}")
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="npi",
                    status="running",
                    phase="npi source processed",
                    unit="files",
                    done=file_idx + 1,
                    total=len(selected_files),
                    message=f"processed file {file_idx + 1}/{len(selected_files)}",
                    label=p,
                )

    # Mark this job as successfully completed; shutdown finalization depends on this.
    context['run'] = context.get('run', 0) + 1


async def startup(ctx):  # pragma: no cover
    """Initialize NPI worker state and import staging tables."""
    await my_init_db(db)
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.utcnow()
    ctx['context']['run'] = 0
    ctx['context']['test_mode'] = False
    await ensure_database(False)
    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    if override_import_id:
        ctx['import_date'] = override_import_id
    else:
        ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    tables = {}  # for the future complex usage

    await _ensure_required_extensions()

    try:
        obj = AddressArchive
        await db.create_table(AddressArchive.__table__, checkfirst=True)
        if hasattr(AddressArchive, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")
    except DuplicateTableError:
        print(f"Address archive table {db_schema}.{obj.__tablename__} already exists.")

    for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIAddress, NPIPhoneStaffing):
        tables[cls.__main_table__] = make_class(cls, import_date)
        obj = tables[cls.__main_table__]
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__main_table__}_{import_date};")
        await db.create_table(obj.__table__, checkfirst=True)
        if hasattr(obj, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")

        if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
            for index in cls.__my_initial_indexes__:
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

    print("Preparing done")

async def refresh_do_business_as(
    target_table: str | None = None,
    source_table: str | None = None,
    test_mode: bool | None = None,
) -> tuple[int, int] | None:
    """
    Populate the NPI.do_business_as array from other identifier entries (type code 3).
    """
    await ensure_database(bool(test_mode))
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    table = target_table or NPIData.__tablename__
    source = source_table or NPIDataOtherIdentifier.__tablename__

    source_exists = await db.scalar(
        f"SELECT to_regclass('{db_schema}.{source}');"
    )
    if not source_exists:
        print(f"Skipping do_business_as refresh: source table {db_schema}.{source} does not exist.")
        return

    update_sql = f"""
        WITH sub AS (
            SELECT
                npi,
                ARRAY_AGG(DISTINCT other_provider_identifier ORDER BY other_provider_identifier) AS names,
                STRING_AGG(DISTINCT other_provider_identifier, ' ' ORDER BY other_provider_identifier) AS search_text
            FROM {db_schema}.{source}
            WHERE other_provider_identifier_type_code = '3'
              AND NULLIF(other_provider_identifier, '') IS NOT NULL
            GROUP BY npi
        ),
        updated AS (
            UPDATE {db_schema}.{table} AS n
            SET
                do_business_as = sub.names,
                do_business_as_text = COALESCE(sub.search_text, '')
            FROM sub
            WHERE n.npi = sub.npi
              AND (
                    n.do_business_as IS DISTINCT FROM sub.names
                 OR COALESCE(n.do_business_as_text, '') IS DISTINCT FROM COALESCE(sub.search_text, '')
              )
            RETURNING 1
        )
        SELECT count(*) FROM updated;
    """
    updated = int(await db.scalar(update_sql) or 0)

    clear_sql = f"""
        WITH cleared AS (
            UPDATE {db_schema}.{table} AS n
            SET
                do_business_as = ARRAY[]::varchar[],
                do_business_as_text = ''
            WHERE (
                    COALESCE(array_length(n.do_business_as, 1), 0) > 0
                 OR COALESCE(n.do_business_as_text, '') <> ''
            )
              AND NOT EXISTS (
                    SELECT 1
                    FROM {db_schema}.{source} AS s
                    WHERE s.npi = n.npi
                      AND s.other_provider_identifier_type_code = '3'
                      AND NULLIF(s.other_provider_identifier, '') IS NOT NULL
              )
            RETURNING 1
        )
        SELECT count(*) FROM cleared;
    """
    cleared = int(await db.scalar(clear_sql) or 0)
    print(f"do_business_as refresh complete: updated={updated}, cleared={cleared}")
    return updated, cleared


async def refresh_taxonomy_arrays(
    *,
    address_table: str,
    taxonomy_table: str,
    schema: str,
) -> int:
    """Refresh denormalized taxonomy integer arrays on NPI addresses."""
    update_sql = f"""
        WITH sub AS (
            SELECT
                tax.npi,
                ARRAY_AGG(DISTINCT nucc.int_code ORDER BY nucc.int_code)::int[] AS res
            FROM {schema}.{taxonomy_table} AS tax
            INNER JOIN {schema}.nucc_taxonomy AS nucc
                ON tax.healthcare_provider_taxonomy_code = nucc.code
            WHERE nucc.int_code IS NOT NULL
            GROUP BY tax.npi
        ),
        updated AS (
            UPDATE {schema}.{address_table} AS addr
            SET taxonomy_array = sub.res
            FROM sub
            WHERE addr.npi = sub.npi
              AND addr.taxonomy_array IS DISTINCT FROM sub.res
            RETURNING 1
        )
        SELECT count(*) FROM updated;
    """
    updated = int(await db.scalar(update_sql) or 0)
    print(f"taxonomy_array refresh complete: updated={updated}")
    return updated


async def resolve_npi_address_archive(
    *,
    staging_table: str,
    field_map: dict[str, str],
    schema: str,
    cancel_check,
):
    """Stamp and resolve NPI staging addresses into the canonical archive."""
    missing_key_rows = int(
        await db.scalar(
            f"SELECT count(*) FROM {schema}.{staging_table} WHERE address_key IS NULL;"
        )
        or 0
    )
    if missing_key_rows:
        configured_shards = int(os.getenv("HLTHPRT_ADDRESS_CANON_NPI_SHARDS", "16"))
        stamp_shards = 1 if missing_key_rows < 1_000_000 else configured_shards
        print(
            "NPI canonical address keys missing after load: "
            f"{missing_key_rows}; stamping missing keys only with {stamp_shards} shard(s)"
        )
        await stamp_address_keys(
            staging_table,
            field_map,
            schema=schema,
            shards=stamp_shards,
            cancel_check=cancel_check,
            update_existing=False,
            honor_env_override=stamp_shards != 1,
        )
    else:
        print("NPI canonical address keys were populated during load; skipping SQL restamp")

    try:
        stats = await resolve_into_archive(
            staging_table,
            field_map,
            source_bit=1,
            priority=0,
            schema=schema,
            cancel_check=cancel_check,
        )
    except RuntimeError as exc:
        if not _is_address_key_mismatch_error(exc):
            raise
        print("NPI canonical address key mismatch detected; repairing staged keys with SQL canonical stamp")
        repaired = await stamp_address_keys(
            staging_table,
            field_map,
            schema=schema,
            shards=int(os.getenv("HLTHPRT_ADDRESS_CANON_NPI_SHARDS", "16")),
            cancel_check=cancel_check,
            update_existing=True,
        )
        print(f"NPI canonical address SQL repair complete: updated={repaired}")
        stats = await resolve_into_archive(
            staging_table,
            field_map,
            source_bit=1,
            priority=0,
            schema=schema,
            cancel_check=cancel_check,
        )
    print(f"NPI canonical address resolve complete: {stats}")
    return stats


async def rebuild_phone_staffing_table(
    *,
    target_table: str,
    address_table: str,
    schema: str,
) -> None:
    """Rebuild the phone-level provider staffing materialization."""
    if not await db.scalar(f"SELECT to_regclass('{schema}.{target_table}');"):
        print(
            f"Skipping phone staffing materialization for {schema}.{target_table}: "
            "target staging table is missing."
        )
        return
    if not await db.scalar(f"SELECT to_regclass('{schema}.{address_table}');"):
        print(
            f"Skipping phone staffing materialization for {schema}.{target_table}: "
            f"address staging table {schema}.{address_table} is missing."
        )
        return
    if not await db.scalar(f"SELECT to_regclass('{schema}.nucc_taxonomy');"):
        raise NPIPrerequisiteError(
            f"Cannot materialize {schema}.{target_table}: {schema}.nucc_taxonomy is missing. "
            "Run the NUCC importer before NPI."
        )
    pharmacist_rows = int(
        await db.scalar(
            f"""
            SELECT count(*)
             FROM {schema}.nucc_taxonomy
             WHERE lower(classification) = 'pharmacist'
               AND int_code IS NOT NULL;
            """
        )
        or 0
    )
    if pharmacist_rows <= 0:
        raise NPIPrerequisiteError(
            f"Cannot materialize {schema}.{target_table}: {schema}.nucc_taxonomy has no "
            "Pharmacist rows with int_code. Run the NUCC importer before NPI."
        )

    print(f"Materializing phone staffing table {schema}.{target_table} from {schema}.{address_table}...")
    await db.status(f"TRUNCATE TABLE {schema}.{target_table};")
    await db.status(
        f"""
        INSERT INTO {schema}.{target_table} (
            state_name,
            telephone_number,
            pharmacist_count,
            updated_at
        )
        WITH pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM {schema}.nucc_taxonomy
            WHERE lower(classification) = 'pharmacist'
        )
        SELECT
            a.state_name,
            REGEXP_REPLACE(a.telephone_number, '[^0-9]', '', 'g') AS telephone_number,
            COUNT(DISTINCT a.npi)::int AS pharmacist_count,
            now()::timestamp AS updated_at
        FROM {schema}.{address_table} AS a
        CROSS JOIN pharmacist_taxonomy AS pt
        WHERE a.type = 'primary'
          AND a.state_name IS NOT NULL
          AND a.state_name <> ''
          AND a.telephone_number IS NOT NULL
          AND a.telephone_number <> ''
          AND a.taxonomy_array && pt.codes
        GROUP BY a.state_name, REGEXP_REPLACE(a.telephone_number, '[^0-9]', '', 'g');
        """
    )




async def shutdown(ctx):  # pragma: no cover
    """Finalize, index, validate, and atomically publish an NPI import."""
    import_date = ctx['import_date']
    context = ctx.get('context') or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    shutdown_phase_timings: list[dict[str, object]] = []

    async def timed_shutdown_phase(phase: str, awaitable):
        """Run one finalization phase while recording progress and timing."""
        started = time.monotonic()
        started_at = datetime.datetime.now(datetime.UTC).isoformat()
        print(f"NPI_SHUTDOWN_PHASE_START phase={phase} started_at={started_at}")
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="npi",
                status="running",
                phase=f"npi shutdown {phase}",
                unit="phase",
                total=1,
                done=0,
                pct=0,
                message=f"npi shutdown {phase} started",
            )
        try:
            result = await awaitable
        except Exception:
            elapsed = round(time.monotonic() - started, 3)
            shutdown_phase_timings.append(
                {
                    "phase": phase,
                    "status": "failed",
                    "elapsed_seconds": elapsed,
                    "started_at": started_at,
                    "finished_at": datetime.datetime.now(datetime.UTC).isoformat(),
                }
            )
            print(f"NPI_SHUTDOWN_PHASE_FAILED phase={phase} elapsed_seconds={elapsed}")
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="npi",
                    status="failed",
                    phase=f"npi shutdown {phase}",
                    unit="phase",
                    total=1,
                    done=0,
                    pct=0,
                    message=f"npi shutdown {phase} failed",
                )
            raise
        elapsed = round(time.monotonic() - started, 3)
        shutdown_phase_timings.append(
            {
                "phase": phase,
                "status": "succeeded",
                "elapsed_seconds": elapsed,
                "started_at": started_at,
                "finished_at": datetime.datetime.now(datetime.UTC).isoformat(),
            }
        )
        print(f"NPI_SHUTDOWN_PHASE_DONE phase={phase} elapsed_seconds={elapsed}")
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="npi",
                status="running",
                phase=f"npi shutdown {phase}",
                unit="phase",
                total=1,
                done=1,
                pct=100,
                message=f"npi shutdown {phase} completed",
            )
        return result

    if not context.get('run'):
        print("No NPI jobs ran in this worker session; skipping shutdown validation.")
        return
    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    if _npi_requires_nucc(context):
        await _assert_nucc_ready(db_schema)
    if source_enabled("nppes"):
        await _assert_nppes_canonical_ready(db_schema)
    tables = {}

    npi_address_stage_table = f"{NPIAddress.__tablename__}_{import_date}"
    npi_address_stage_exists = await db.scalar(
        f"SELECT to_regclass('{db_schema}.{npi_address_stage_table}');"
    )
    if not npi_address_stage_exists:
        print(
            f"Staging table {db_schema}.{npi_address_stage_table} is missing; "
            "skipping NPI shutdown finalization."
        )
        return

    test = make_class(NPIAddress, import_date)
    npi_address_count = await db.scalar(select(func.count(test.npi)))
    if context.get("test_mode"):
        print(f"Test mode: imported {npi_address_count} NPI addresses (no minimum enforced).")
    else:
        if not npi_address_count or npi_address_count < 5_000_000:
            print(f"Failed Import: Address number:{npi_address_count}")
            sys.exit(1)

    processing_classes_array = (
        NPIData,
        NPIDataTaxonomyGroup,
        NPIDataOtherIdentifier,
        NPIDataTaxonomy,
        NPIAddress,
        NPIPhoneStaffing,
    )

    async def table_exists(table_name: str) -> bool:
        """Check whether a table exists in the active import schema."""
        exists = await db.scalar(f"SELECT to_regclass('{db_schema}.{table_name}');")
        return bool(exists)

    async def table_has_column(table_name: str, column_name: str) -> bool:
        """Check whether an import table exposes a named column."""
        return bool(await db.scalar(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = '{db_schema}'
                   AND table_name = '{table_name}'
                   AND column_name = '{column_name}'
            );
            """
        ))

    stage_counts: dict[str, int] = {}
    for cls in processing_classes_array:
        stage_obj = make_class(cls, import_date)
        if await table_exists(stage_obj.__tablename__):
            stage_counts[cls.__main_table__] = int(
                await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_obj.__tablename__};") or 0
            )

    postgis_cache: dict[str, bool | None] = {"available": None}

    async def has_postgis() -> bool:
        """Cache whether PostGIS types and constructors are available."""
        if postgis_cache["available"] is None:
            geography_type = await db.scalar("SELECT to_regtype('geography');")
            st_makepoint = await db.scalar("SELECT to_regprocedure('st_makepoint(double precision, double precision)');")
            postgis_cache["available"] = bool(geography_type and st_makepoint)
            if not postgis_cache["available"]:
                print("PostGIS is unavailable; geo GIST index creation will be skipped.")
        return bool(postgis_cache["available"])

    async def archive_index(index_name: str) -> str:
        """Move a live index name aside before staged table promotion."""
        archived_name = _archived_identifier(index_name)
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_name};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{index_name} RENAME TO {archived_name};"
        )
        return archived_name

    address_stats = None
    if source_enabled("nppes"):
        async def _cancel_check():
            await raise_if_cancelled(ctx, {})

        npi_address_field_map = {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city_name",
            "state": "state_name",
            "zip": "postal_code",
            "country": "COALESCE(NULLIF(country_code, ''), 'US')",
        }

        async def _canonical_address_resolve():
            return await resolve_npi_address_archive(
                staging_table=test.__tablename__,
                field_map=npi_address_field_map,
                schema=db_schema,
                cancel_check=_cancel_check,
            )

        address_stats = await timed_shutdown_phase(
            "canonical_address_resolve",
            _canonical_address_resolve(),
        )
        if _env_bool("HLTHPRT_NPI_OPENADDRESSES_BACKFILL", False):
            oa_stats = await timed_shutdown_phase(
                "openaddresses_archive_backfill",
                refresh_archive_geocodes_from_openaddresses_sharded(schema=db_schema, run_id=run_id or None),
            )
            print(
                "OpenAddresses archive backfill after NPI canonical resolve: "
                f"exact={oa_stats.exact_updates} fuzzy={oa_stats.fuzzy_updates} "
                f"relaxed={oa_stats.relaxed_updates}"
            )
        else:
            print(
                "Skipping OpenAddresses archive backfill after NPI canonical resolve; "
                "set HLTHPRT_NPI_OPENADDRESSES_BACKFILL=1 to run it."
            )

    async def _timed_geo_update(obj, archive_source: str, use_canonical_archive: bool):
        async def _run_geo_update():
            if use_canonical_archive and await table_has_column(archive_source, "address_key"):
                await db.status(
                    f"UPDATE {db_schema}.{obj.__tablename__} as a SET formatted_address = b.formatted_address, "
                    f"lat = b.lat, long = b.long, place_id = b.place_id "
                    f"FROM {db_schema}.{archive_source} as b "
                    f"WHERE a.address_key IS NOT NULL AND a.address_key = b.address_key"
                    f" AND b.lat IS NOT NULL AND b.long IS NOT NULL"
                )
                if await table_exists("address_checksum_map"):
                    await db.status(
                        f"UPDATE {db_schema}.{obj.__tablename__} as a SET formatted_address = b.formatted_address, "
                        f"lat = b.lat, long = b.long, place_id = b.place_id "
                        f"FROM {db_schema}.address_checksum_map as m "
                        f"JOIN {db_schema}.{archive_source} as b ON b.address_key = m.address_key "
                        f"WHERE a.address_key IS NULL AND a.checksum = m.checksum"
                        f" AND b.lat IS NOT NULL AND b.long IS NOT NULL"
                    )
            else:
                await db.status(
                    f"UPDATE {db_schema}.{obj.__tablename__} as a SET formatted_address = b.formatted_address, "
                    f"lat = b.lat, long = b.long, place_id = b.place_id "
                    f"FROM {db_schema}.{archive_source} as b WHERE a.checksum = b.checksum"
                )

        return await timed_shutdown_phase("geocode_archive_enrichment", _run_geo_update())

    async def create_additional_indexes(cls, obj) -> None:
        """Create model-declared secondary indexes on a staged table."""
        if not hasattr(cls, '__my_additional_indexes__') or not cls.__my_additional_indexes__:
            return
        for index in cls.__my_additional_indexes__:
            index_name = index.get('name', '_'.join(index.get('index_elements')))
            if _index_requires_postgis(index) and not await has_postgis():
                print(
                    f"Skipping index {obj.__tablename__}_idx_{index_name}: "
                    "requires PostGIS (geography + ST_MakePoint)."
                )
                continue
            using = ""
            if t := index.get('using'):
                using = f"USING {t} "
            where_clause = ""
            if where := index.get('where'):
                where_clause = f" WHERE {where}"
            index_elements = [
                str(element)
                .replace("Geography(ST_MakePoint", "public.Geography(public.ST_MakePoint")
                .replace("geography(st_makepoint", "public.geography(public.st_makepoint")
                for element in index.get('index_elements')
            ]
            create_index_sql = (
                f"CREATE INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} "
                f"ON {db_schema}.{obj.__tablename__}  {using}"
                f"({', '.join(index_elements)}){where_clause};"
            )
            print(create_index_sql)
            await timed_shutdown_phase(
                f"index_creation:{obj.__tablename__}:{index_name}",
                db.status(create_index_sql),
            )

    deferred_npi_indexes_obj = None

    async with db.transaction():
        for cls in processing_classes_array:
            tables[cls.__main_table__] = make_class(cls, import_date)
            obj = tables[cls.__main_table__]
            if cls is NPIData:
                deferred_npi_indexes_obj = obj
            if cls is NPIDataOtherIdentifier:
                print('Updating NPI do_business_as arrays from other identifiers...')
                target_npi_cls = tables.get(NPIData.__main_table__)
                target_table_name = target_npi_cls.__tablename__ if target_npi_cls else NPIData.__tablename__
                source_table_name = obj.__tablename__
                await timed_shutdown_phase(
                    "do_business_as_enrichment",
                    refresh_do_business_as(
                        target_table=target_table_name,
                        source_table=source_table_name,
                        test_mode=bool(context.get("test_mode")),
                    ),
                )
                if deferred_npi_indexes_obj is not None:
                    await create_additional_indexes(NPIData, deferred_npi_indexes_obj)
                    deferred_npi_indexes_obj = None
            if cls is NPIAddress:
                npi_taxonomy_table = f"npi_taxonomy_{import_date}"
                if await table_exists(npi_taxonomy_table) and await table_exists("nucc_taxonomy"):
                    print("Updating NUCC Taxonomy for NPI Addresses...")
                    await timed_shutdown_phase(
                        "taxonomy_array_enrichment",
                        refresh_taxonomy_arrays(
                            address_table=obj.__tablename__,
                            taxonomy_table=npi_taxonomy_table,
                            schema=db_schema,
                        ),
                    )
                else:
                    print(
                        f"Skipping NUCC taxonomy update: "
                        f"required tables missing ({db_schema}.{npi_taxonomy_table} and/or {db_schema}.nucc_taxonomy)."
                    )

                preferred_archive = archive_table_name()
                use_canonical_archive = _env_bool("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER")
                archive_source = (
                    preferred_archive
                    if use_canonical_archive and await table_exists(preferred_archive)
                    else "address_archive"
                )
                if await table_exists(archive_source):
                    print(f"Updating NPI Addresses Geo from Archive {archive_source}...")
                    await _timed_geo_update(obj, archive_source, use_canonical_archive)
                else:
                    print(f"Skipping NPI geo update: no address archive table is available in {db_schema}.")

                if await table_exists("plan_npi_raw"):
                    print("Updating NPI Plan-Network Array from Plans Import Data...")
                    await timed_shutdown_phase(
                        "plan_network_array_enrichment",
                        db.status(
                        f"""UPDATE {db_schema}.{obj.__tablename__} as a
SET
    plans_network_array = n_list
FROM (
    SELECT
        npi,
        ARRAY_AGG(DISTINCT checksum_network) as n_list
    FROM {db_schema}.plan_npi_raw
    GROUP BY npi
) as b
WHERE
    a.npi = b.npi;"""
                        ),
                    )
                else:
                    print(f"Skipping NPI plan-network update: source table {db_schema}.plan_npi_raw is missing.")

                if await table_exists("pricing_provider_procedure"):
                    print("Updating NPI procedures_array from pricing provider procedures...")
                    await timed_shutdown_phase(
                        "procedures_array_enrichment",
                        db.status(
                        f"""UPDATE {db_schema}.{obj.__tablename__} AS a
SET
    procedures_array = b.codes
FROM (
    SELECT
        npi,
        ARRAY_AGG(DISTINCT procedure_code ORDER BY procedure_code) AS codes
    FROM {db_schema}.pricing_provider_procedure
    GROUP BY npi
) AS b
WHERE
    a.npi = b.npi;"""
                        ),
                    )
                else:
                    print(
                        f"Skipping NPI procedures_array update: source table "
                        f"{db_schema}.pricing_provider_procedure is missing."
                    )

                if await table_exists("pricing_provider_prescription"):
                    print("Updating NPI medications_array from pricing provider prescriptions...")
                    await timed_shutdown_phase(
                        "medications_array_enrichment",
                        db.status(
                        f"""UPDATE {db_schema}.{obj.__tablename__} AS a
SET
    medications_array = b.codes
FROM (
    SELECT
        npi,
        ARRAY_AGG(DISTINCT rx_code::INTEGER ORDER BY rx_code::INTEGER) AS codes
    FROM {db_schema}.pricing_provider_prescription
    WHERE
        rx_code_system = 'HP_RX_CODE'
        AND rx_code ~ '^-?[0-9]+$'
    GROUP BY npi
) AS b
WHERE
    a.npi = b.npi;"""
                        ),
                    )
                else:
                    print(
                        f"Skipping NPI medications_array update: source table "
                        f"{db_schema}.pricing_provider_prescription is missing."
                    )

            if cls is NPIPhoneStaffing:
                address_stage = tables.get(NPIAddress.__main_table__)
                if address_stage is None:
                    print(
                        f"Skipping phone staffing materialization: "
                        f"staging table for {NPIAddress.__tablename__} is unavailable."
                    )
                else:
                    await timed_shutdown_phase(
                        "phone_staffing_rebuild",
                        rebuild_phone_staffing_table(
                            target_table=obj.__tablename__,
                            address_table=address_stage.__tablename__,
                            schema=db_schema,
                        ),
                    )

            if cls is not NPIData:
                await create_additional_indexes(cls, obj)

        if deferred_npi_indexes_obj is not None:
            await create_additional_indexes(NPIData, deferred_npi_indexes_obj)

    # Run VACUUM FULL ANALYZE in parallel for all tables
    async def vacuum_table(obj):
        """Run the post-index full vacuum for one promoted NPI table."""
        if not await table_exists(obj.__tablename__):
            print(f"Skipping VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__}: table is missing.")
            return
        print(f"Post-Index VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")
        await timed_shutdown_phase(
            f"vacuum_analyze:{obj.__tablename__}",
            db.execute_ddl(f"VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};"),
        )

    vacuum_tasks = [
        vacuum_table(tables[cls.__main_table__])
        for cls in processing_classes_array
    ]
    await asyncio.gather(*vacuum_tasks)

    async with db.transaction():
        for cls in processing_classes_array:
            obj = tables[cls.__main_table__]
            table = obj.__main_table__
            if not await table_exists(obj.__tablename__):
                print(f"Skipping publish rotation for {db_schema}.{obj.__tablename__}: staging table is missing.")
                continue

            async def _publish_table_rotation(cls=cls, obj=obj, table=table):
                await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
                await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
                await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{obj.__tablename__} RENAME TO {table};")

                await archive_index(f"{table}_idx_primary")

                await db.status(f"ALTER INDEX IF EXISTS "
                                f"{db_schema}.{obj.__tablename__}_idx_primary RENAME TO "
                                f"{table}_idx_primary;")

                move_indexes = []
                if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                    move_indexes += cls.__my_initial_indexes__
                if hasattr(cls, '__my_additional_indexes__') and obj.__my_additional_indexes__:
                    move_indexes += obj.__my_additional_indexes__

                for index in move_indexes:
                    index_name = index.get('name', '_'.join(index.get('index_elements')))
                    await archive_index(f"{table}_idx_{index_name}")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.{obj.__tablename__}_idx_{index_name} RENAME TO "
                                    f"{table}_idx_{index_name};")

            await timed_shutdown_phase(
                f"publish_swap:{table}",
                _publish_table_rotation(),
            )

    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="npi published",
        progress_message="succeeded",
        progress={
            "unit": "rows",
            "done": int(npi_address_count or 0),
            "total": int(npi_address_count or 0),
            "pct": 100,
            "message": "succeeded",
            "phase": "npi published",
        },
        metrics={
            "stage_rows": stage_counts,
            "npi_address_rows": int(npi_address_count or 0),
            "npi_shutdown_phase_timings": shutdown_phase_timings,
            **({"address_resolve": address_stats.__dict__} if address_stats else {}),
            "openaddresses_backfill_enabled": _env_bool("HLTHPRT_NPI_OPENADDRESSES_BACKFILL", False),
        },
    )
    print_time_info(ctx['context']['start'])


async def save_npi_data(ctx, task):
    """Persist one normalized NPI payload into its staging models."""
    import_date = ctx['import_date']
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    x = []
    for key in task:
        match key:
            case 'npi_obj_list':
                mynpidata = make_class(NPIData, import_date)
                x.append(push_objects(task['npi_obj_list'], mynpidata, rewrite=True))
            case 'npi_taxonomy_list':
                mynpidatataxonomy = make_class(NPIDataTaxonomy, import_date)
                x.append(push_objects(task['npi_taxonomy_list'], mynpidatataxonomy, rewrite=True))
            case 'npi_other_id_list':
                mynpidataotheridentifier = make_class(NPIDataOtherIdentifier, import_date)
                unique = list({item['checksum']: item for item in task['npi_other_id_list']}.values())
                x.append(push_objects(unique, mynpidataotheridentifier))
            case 'npi_taxonomy_group_list':
                mynpidatataxonomygroup = make_class(NPIDataTaxonomyGroup, import_date)
                x.append(push_objects(task['npi_taxonomy_group_list'], mynpidatataxonomygroup, rewrite=True))
            case 'npi_address_list':
                mynpiaddress = make_class(NPIAddress, import_date)
                x.append(push_objects(task['npi_address_list'], mynpiaddress, rewrite=True))
            case _:
                print('Some wrong key passed')
    for coro in x:
        await coro


async def main(test_mode: bool = False):  # pragma: no cover
    """Enqueue an NPI import through the configured worker queue."""
    redis = await create_pool(build_redis_settings(),
                              job_serializer=serialize_job,
                              job_deserializer=deserialize_job)
    payload = {'test_mode': bool(test_mode)}
    await redis.enqueue_job('process_data', payload, _queue_name=NPI_QUEUE_NAME)
