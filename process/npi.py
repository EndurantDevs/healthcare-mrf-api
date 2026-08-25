# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import glob
import hashlib
import os
import re
import tempfile
import time
import uuid
import zipfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import wraps
from pathlib import Path, PurePath

import pytz
from aiocsv import AsyncDictReader
from aiofile import async_open
from arq.connections import RedisSettings
from asyncpg import DuplicateTableError
from dateutil.parser import parse as parse_date
from sqlalchemy import func, select

from db.models import (AddressArchive, NPIAddress, NPIData,
                       NPIDataOtherIdentifier, NPIDataTaxonomy,
                       NPIDataTaxonomyGroup, NPIPhoneStaffing, db)
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import suppress_control_run_heartbeat_persistence
from process.ext.archive import unzip
from process.ext.address_canon import (
    address_key_v1,
    archive_table_name,
    resolve_into_archive,
    source_enabled,
    stamp_address_keys,
)
from process.ext.address_fast import canonicalize_batch as canonicalize_address_batch
from process.ext.address_format import ADDRESS_FORMAT_FUNCTION
from process.ext.contact_canon import canonicalize_batch as canonicalize_contact_batch
from process.ext.utils import (download_it, download_it_and_save,
                               ensure_database, make_class, my_init_db,
                               print_time_info, push_objects, return_checksum)
from process.live_progress import enqueue_live_progress
from process.npi_canonical_publication import (
    NPI_CANONICAL_TABLES,
    NpiCanonicalPublicationInput,
    publication_error,
    receipt_metrics as npi_publication_metrics,
)
from process.npi_canonical_publication_store import (
    NpiCanonicalPublicationCommit,
    canonical_relation_oids,
    insert_npi_publication_receipt,
    load_committed_npi_publication,
    lock_npi_publication_attempt,
    has_settled_npi_publication,
    mark_npi_publication_succeeded,
)
from process.nppes_public_evidence_catalog import assert_nppes_admission_catalog
from process.nppes_public_evidence_import import (
    NppesEvidenceRuntimeConfig,
    import_nppes_public_evidence_chain,
    materialize_prepared_nppes_archive,
    open_verified_nppes_legacy_text,
    prepare_nppes_release_chain,
    resolve_nppes_evidence_runtime_config,
    validate_nppes_public_evidence_chain_receipt,
)
from process.nppes_public_evidence_scratch import (
    assert_nppes_scratch_capacity,
    resolve_nppes_scratch_root,
)
from process.openaddresses import refresh_archive_geocodes_from_openaddresses_sharded

latin_pattern= re.compile(r'[^\x00-\x7f]')

TEST_NPI_MAX_FILES = 1
TEST_NPI_ROWS = 1000
TEST_NPI_OTHER_ROWS = 500
TEST_NPI_SECONDARY_ROWS = 1000
NPI_QUEUE_NAME = "arq:NPI"
DEFAULT_NPI_MAX_PENDING_SAVE_TASKS = 4
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
POSTGRES_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}", flags=re.ASCII)
ADDRESS_KEY_MISMATCH_MESSAGE = "Stamped canonical address key does not match identity_key"
_NPPES_EVIDENCE_CONFIG_KEY = "_nppes_evidence_runtime_config"
_NPPES_EVIDENCE_RECEIPT_KEY = "_nppes_public_evidence_chain_receipt"
_NPPES_EVIDENCE_METRICS_KEY = "nppes_public_evidence"
_NPI_IMPORT_LEASE_KEY = "_npi_import_lease"
_NPI_IMPORT_LEASE_DOMAIN = "healthporta.npi-import-publish.v1"
_NPI_PENDING_SAVE_TASKS_KEY = "_npi_pending_save_tasks"
_NPI_CONTROL_ATTEMPT_ID_KEY = "_control_attempt_id"
_NPI_CONTROL_ATTEMPT_STARTED_AT_KEY = "_control_attempt_started_at"
_NPI_CONTROL_TERMINAL_COMMITTED_KEY = "control_run_terminal_committed"
_NPI_CONTROL_COMMITTED_HEARTBEAT_AT_KEY = "_control_committed_heartbeat_at"
_NPI_CONTROL_COMMITTED_FINISHED_AT_KEY = "_control_committed_finished_at"
_NPI_CONTROL_COMMITTED_RESULT_KEY = "_control_committed_result"
_NPI_PRIVATE_SOURCE_FIELDS = frozenset(
    {"employer_identification_number", "parent_organization_tin"}
)
_NPI_STAGING_CLASSES = (
    NPIData,
    NPIDataTaxonomyGroup,
    NPIDataOtherIdentifier,
    NPIDataTaxonomy,
    NPIAddress,
    NPIPhoneStaffing,
)


class NPIPrerequisiteError(RuntimeError):
    """Raised when a full NPI import would publish incomplete derived data."""


@dataclass(slots=True, repr=False)
class _NpiImportLease:
    manager: object
    connection: object
    backend_pid: int

    def __repr__(self) -> str:
        return "<npi-import-lease>"


def _release_lease_after_process_failure(process_attempt):
    """Decorate an NPI process attempt with failure-only lease cleanup."""

    @wraps(process_attempt)
    async def process_attempt_with_cleanup(ctx, task=None):
        """Release attempt-owned resources only when processing fails."""

        try:
            return await process_attempt(ctx, task)
        except BaseException:
            context = ctx.get("context") if isinstance(ctx, dict) else None
            if isinstance(context, dict):
                pending_tasks = context.pop(_NPI_PENDING_SAVE_TASKS_KEY, None)
                await _cancel_npi_save_tasks(pending_tasks)
                await _release_npi_import_lease(context, suppress_errors=True)
            raise

    return process_attempt_with_cleanup


def _release_lease_after_shutdown(finalize_attempt):
    """Decorate NPI shutdown with unconditional lease cleanup."""

    @wraps(finalize_attempt)
    async def finalize_attempt_with_cleanup(ctx):
        """Release the attempt lease after shutdown succeeds or fails."""

        context = ctx.get("context") if isinstance(ctx, dict) else None
        try:
            result = await finalize_attempt(ctx)
        except BaseException:
            if isinstance(context, dict):
                await _release_npi_import_lease(context, suppress_errors=True)
            raise
        if isinstance(context, dict):
            is_committed = bool(
                context.get(_NPI_CONTROL_TERMINAL_COMMITTED_KEY)
            )
            try:
                await _release_npi_import_lease(
                    context,
                    suppress_errors=is_committed,
                )
            except BaseException:
                if not is_committed:
                    raise
        return result

    return finalize_attempt_with_cleanup


async def _suppress_lease_manager_exit(manager: object, error: BaseException) -> None:
    """Best-effort close a failed lease acquisition without masking its error."""

    try:
        await manager.__aexit__(type(error), error, error.__traceback__)
    except BaseException:
        return


def _nppes_evidence_runtime_config(context: dict) -> NppesEvidenceRuntimeConfig:
    """Resolve once per worker and reject configuration drift within a run."""

    resolved = resolve_nppes_evidence_runtime_config()
    remembered = context.get(_NPPES_EVIDENCE_CONFIG_KEY)
    if remembered is not None and remembered != resolved:
        raise NPIPrerequisiteError("NPPES evidence runtime configuration changed")
    context[_NPPES_EVIDENCE_CONFIG_KEY] = resolved
    return resolved


def _nppes_evidence_metrics(receipt: object) -> dict[str, object]:
    """Return value-safe ordered-chain evidence metrics for control status."""

    validated = validate_nppes_public_evidence_chain_receipt(receipt)
    return {
        "mode": "required",
        "status": "admitted",
        "archive_count": len(validated.archives),
        "chain_ref": validated.chain_ref,
        "listing_sha256": validated.listing_sha256,
        "listing_candidate_count": len(validated.listing_candidate_names),
        "source_record_count": validated.source_record_count,
        "projected_record_count": validated.projected_record_count,
        "excluded_record_count": validated.excluded_record_count,
        "chain_contract_sha256": validated.contract_sha256,
        "archives": [
            {
                "source_release_ref": archive.source_release_ref,
                "artifact_sha256": archive.artifact_sha256,
                "manifest_sha256": archive.manifest_sha256,
                "source_record_count": archive.source_record_count,
                "projected_record_count": archive.projected_record_count,
                "excluded_record_count": archive.excluded_record_count,
                "write_state": archive.write_state,
            }
            for archive in validated.archives
        ],
    }


def _required_nppes_evidence_receipt(context: dict):
    """Normalize a missing or malformed required-mode receipt at the NPI boundary."""

    try:
        validated = validate_nppes_public_evidence_chain_receipt(
            context.get(_NPPES_EVIDENCE_RECEIPT_KEY)
        )
    except Exception:
        normalized_error = NPIPrerequisiteError(
            "NPPES public-evidence admission receipt is invalid"
        )
    else:
        return validated
    raise normalized_error


def _open_nppes_source(
    legacy_layout: object | None,
    member_kind: str,
    source_path: str,
):
    """Use a sealed descriptor in required mode and the legacy path otherwise."""

    if legacy_layout is not None:
        return open_verified_nppes_legacy_text(legacy_layout, member_kind)
    return async_open(source_path, "r")


def _postgres_identifier(value: object) -> str:
    if type(value) is not str or POSTGRES_IDENTIFIER_RE.fullmatch(value) is None:
        raise NPIPrerequisiteError("NPI database identifier is invalid")
    return value


def _runtime_db_schema() -> str:
    """Resolve the one schema shared by migrations and the NPI runtime."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise NPIPrerequisiteError("NPI database schema configuration conflicts")
    return _postgres_identifier(runtime_schema or legacy_schema or "mrf")


def _required_npi_control_attempt(
    context: dict,
    run_id: str,
) -> tuple[str, str]:
    """Return the exact wrapper attempt authorized to publish live tables."""

    attempt_id = str(context.get(_NPI_CONTROL_ATTEMPT_ID_KEY) or "").strip()
    attempt_started_at = str(
        context.get(_NPI_CONTROL_ATTEMPT_STARTED_AT_KEY) or ""
    ).strip()
    if (
        not run_id
        or not attempt_id
        or not attempt_started_at
        or not attempt_id.startswith(run_id + ":")
    ):
        raise NPIPrerequisiteError("NPI control attempt identity is missing")
    return attempt_id, attempt_started_at


def _canonical_publication_import_date(import_date: str) -> str:
    """Convert the staging suffix to the receipt's canonical date."""

    try:
        return datetime.datetime.strptime(import_date, "%Y%m%d").date().isoformat()
    except (TypeError, ValueError):
        raise NPIPrerequisiteError("NPI import date is invalid") from None


def _canonical_publication_row_counts(
    counts_by_table: dict[str, int],
) -> tuple[int, ...]:
    """Order the exact six live-table censuses for the receipt contract."""

    try:
        counts = tuple(counts_by_table[table] for table in NPI_CANONICAL_TABLES)
    except (KeyError, TypeError):
        raise NPIPrerequisiteError("NPI staging census is incomplete") from None
    if any(type(count) is not int or count < 0 for count in counts):
        raise NPIPrerequisiteError("NPI staging census is invalid")
    return counts


async def _install_npi_postseal_guards(
    connection: object,
    *,
    schema: str,
    stage_tables: tuple[str, ...],
) -> None:
    """Carry the transaction-local post-seal write fence into the new live tables."""

    guard_function = f"{schema}.guard_npi_canonical_publication_after_seal"
    for stage_table in stage_tables:
        fixed_stage = _postgres_identifier(stage_table)
        qualified_stage = f"{schema}.{fixed_stage}"
        await connection.execute(
            "DROP TRIGGER IF EXISTS "
            "npi_canonical_publication_postseal_write_guard "
            f"ON {qualified_stage}; "
            "CREATE TRIGGER npi_canonical_publication_postseal_write_guard "
            f"BEFORE INSERT OR UPDATE OR DELETE ON {qualified_stage} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {guard_function}(); "
            f"ALTER TABLE {qualified_stage} ENABLE ALWAYS TRIGGER "
            "npi_canonical_publication_postseal_write_guard; "
            "DROP TRIGGER IF EXISTS "
            "npi_canonical_publication_postseal_truncate_guard "
            f"ON {qualified_stage}; "
            "CREATE TRIGGER npi_canonical_publication_postseal_truncate_guard "
            f"BEFORE TRUNCATE ON {qualified_stage} FOR EACH STATEMENT "
            f"EXECUTE FUNCTION {guard_function}(); "
            f"ALTER TABLE {qualified_stage} ENABLE ALWAYS TRIGGER "
            "npi_canonical_publication_postseal_truncate_guard;"
        )


async def _archive_npi_index(
    connection: object,
    *,
    schema: str,
    index_name: str,
) -> None:
    """Move one live index aside on the publication transaction."""

    fixed_schema = _postgres_identifier(schema)
    fixed_index = _postgres_identifier(index_name)
    archived_name = _postgres_identifier(_archived_identifier(fixed_index))
    await connection.execute(
        f"DROP INDEX IF EXISTS {fixed_schema}.{archived_name};"
    )
    await connection.execute(
        f"ALTER INDEX IF EXISTS {fixed_schema}.{fixed_index} "
        f"RENAME TO {archived_name};"
    )


async def _rotate_npi_canonical_table(
    connection: object,
    *,
    schema: str,
    live_table: str,
    stage_table: str,
    index_suffixes: tuple[str, ...],
) -> None:
    """Atomically promote one staged NPI relation and its named indexes."""

    fixed_schema = _postgres_identifier(schema)
    fixed_live = _postgres_identifier(live_table)
    fixed_stage = _postgres_identifier(stage_table)
    await connection.execute(
        f"DROP TABLE IF EXISTS {fixed_schema}.{fixed_live}_old;"
    )
    await connection.execute(
        f"ALTER TABLE IF EXISTS {fixed_schema}.{fixed_live} "
        f"RENAME TO {fixed_live}_old;"
    )
    await connection.execute(
        f"ALTER TABLE {fixed_schema}.{fixed_stage} RENAME TO {fixed_live};"
    )
    await _archive_npi_index(
        connection,
        schema=fixed_schema,
        index_name=f"{fixed_live}_idx_primary",
    )
    await connection.execute(
        f"ALTER INDEX IF EXISTS {fixed_schema}.{fixed_stage}_idx_primary "
        f"RENAME TO {fixed_live}_idx_primary;"
    )
    for index_suffix in index_suffixes:
        fixed_suffix = _postgres_identifier(index_suffix)
        await _archive_npi_index(
            connection,
            schema=fixed_schema,
            index_name=f"{fixed_live}_idx_{fixed_suffix}",
        )
        await connection.execute(
            f"ALTER INDEX IF EXISTS {fixed_schema}.{fixed_stage}_idx_{fixed_suffix} "
            f"RENAME TO {fixed_live}_idx_{fixed_suffix};"
        )


async def _assert_nppes_canonical_stage_parity(
    receipt: object,
    import_date: str,
    db_schema: str,
) -> None:
    """Prove the canonical NPI stage matches each latest admitted source row."""

    fixed_receipt = validate_nppes_public_evidence_chain_receipt(receipt)
    schema = _postgres_identifier(db_schema)
    stage_table = _postgres_identifier(make_class(NPIData, import_date).__tablename__)
    query = _nppes_canonical_parity_query(schema, stage_table)
    async with db.acquire_driver() as connection:
        parity = await connection.fetchrow(query, fixed_receipt.chain_ref)
    if not _is_nppes_parity_zero(parity):
        raise NPIPrerequisiteError(
            "Canonical NPI staging does not match admitted NPPES evidence"
        )


def _nppes_canonical_parity_query(schema: str, stage_table: str) -> str:
    return f"""
        WITH selected_members AS (
            SELECT member.npi,
                   member.payload_sha256,
                   row_number() OVER (
                       PARTITION BY member.npi
                       ORDER BY archive.archive_ordinal DESC
                   ) AS newest
              FROM {schema}.public_evidence_nppes_registry_chain_archive AS archive
              JOIN {schema}.public_evidence_nppes_registry_member AS member
                ON member.admission_ref = archive.admission_ref
             WHERE archive.chain_ref = $1
        ), latest_members AS (
            SELECT npi, payload_sha256
              FROM selected_members
             WHERE newest = 1
        ), comparison AS (
            SELECT member.npi,
                   stage.npi IS NULL AS missing_stage,
                   CASE WHEN stage.npi IS NULL THEN false ELSE
                       member.payload_sha256 IS DISTINCT FROM
                       {schema}.nppes_registry_payload_digest(
                           stage.npi::text,
                           stage.entity_type_code::text,
                           stage.provider_enumeration_date,
                           stage.last_update_date,
                           stage.npi_deactivation_date,
                           stage.npi_reactivation_date
                       )
                   END AS payload_mismatch
              FROM latest_members AS member
              LEFT JOIN {schema}.{stage_table} AS stage
                ON stage.npi::text = member.npi
        )
        SELECT count(*) FILTER (WHERE missing_stage) AS missing_count,
               count(*) FILTER (WHERE payload_mismatch) AS mismatch_count,
               (SELECT count(*)
                  FROM {schema}.{stage_table} AS stage
                 WHERE NOT EXISTS (
                    SELECT 1 FROM latest_members AS member
                     WHERE member.npi = stage.npi::text
                 )) AS unexpected_count,
               (SELECT count(*)
                  FROM {schema}.{stage_table}
                 WHERE employer_identification_number IS NOT NULL
                    OR parent_organization_tin IS NOT NULL) AS private_count
          FROM comparison
    """


def _is_nppes_parity_zero(parity: object) -> bool:
    return not (
        parity is None
        or type(parity["missing_count"]) is not int
        or type(parity["mismatch_count"]) is not int
        or type(parity["unexpected_count"]) is not int
        or type(parity["private_count"]) is not int
        or any(
            parity[count_name] != 0
            for count_name in (
                "missing_count",
                "mismatch_count",
                "unexpected_count",
                "private_count",
            )
        )
    )


async def _acquire_npi_import_lease(context: dict) -> None:
    """Hold one cross-process PostgreSQL session lock through publication."""

    if context.get(_NPI_IMPORT_LEASE_KEY) is not None:
        raise NPIPrerequisiteError("NPI import lease is already held")
    manager = db.acquire_driver()
    connection = await manager.__aenter__()
    try:
        locked = await connection.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            _NPI_IMPORT_LEASE_DOMAIN,
        )
        backend_pid = await connection.fetchval("SELECT pg_backend_pid()")
        if locked is not True or type(backend_pid) is not int:
            raise NPIPrerequisiteError("Another NPI import is already active")
    except BaseException as error:
        await _suppress_lease_manager_exit(manager, error)
        raise
    context[_NPI_IMPORT_LEASE_KEY] = _NpiImportLease(
        manager=manager,
        connection=connection,
        backend_pid=backend_pid,
    )


async def _assert_npi_import_lease(context: dict) -> None:
    """Require the exact PostgreSQL session that owns the import lease."""

    lease = context.get(_NPI_IMPORT_LEASE_KEY)
    if type(lease) is not _NpiImportLease:
        raise NPIPrerequisiteError("NPI import lease is missing")
    backend_pid = await lease.connection.fetchval("SELECT pg_backend_pid()")
    lock_is_granted = await lease.connection.fetchval(
        """
        WITH lock_key AS (
            SELECT hashtextextended($1, 0)::bigint AS value
        )
        SELECT EXISTS (
            SELECT 1
              FROM pg_catalog.pg_locks AS held_lock, lock_key
             WHERE held_lock.pid = pg_backend_pid()
               AND held_lock.locktype = 'advisory'
               AND held_lock.mode = 'ExclusiveLock'
               AND held_lock.granted
               AND held_lock.database = (
                   SELECT database_record.oid
                     FROM pg_catalog.pg_database AS database_record
                    WHERE database_record.datname = current_database()
               )
               AND held_lock.classid::bigint =
                   ((lock_key.value >> 32) & 4294967295::bigint)
               AND held_lock.objid::bigint =
                   (lock_key.value & 4294967295::bigint)
               AND held_lock.objsubid = 1
        )
        """,
        _NPI_IMPORT_LEASE_DOMAIN,
    )
    if backend_pid != lease.backend_pid or lock_is_granted is not True:
        raise NPIPrerequisiteError("NPI import lease was lost")


async def _assert_nppes_postgres_runtime(context: dict) -> None:
    """Require the durable PostgreSQL 18 settings proved by the scale gate."""

    lease = context.get(_NPI_IMPORT_LEASE_KEY)
    if type(lease) is not _NpiImportLease:
        raise NPIPrerequisiteError("NPI import lease is missing")
    try:
        settings = tuple(
            await lease.connection.fetchrow(
                "SELECT current_setting('server_version_num')::integer, "
                "current_setting('fsync'), current_setting('full_page_writes'), "
                "current_setting('synchronous_commit'), "
                "current_setting('wal_compression')"
            )
        )
    except Exception:
        settings = ()
    if (
        len(settings) != 5
        or type(settings[0]) is not int
        or not 180_000 <= settings[0] < 190_000
        or settings[1:] != ("on", "on", "on", "pglz")
    ):
        raise NPIPrerequisiteError(
            "NPPES PostgreSQL durability configuration is invalid"
        )


async def _assert_nppes_storage_catalog(context: dict, schema: str) -> None:
    """Require the complete writer catalog before acquisition or staging."""

    lease = context.get(_NPI_IMPORT_LEASE_KEY)
    if type(lease) is not _NpiImportLease:
        raise NPIPrerequisiteError("NPI import lease is missing")
    await assert_nppes_admission_catalog(lease.connection, schema)


async def _release_npi_import_lease(
    context: dict,
    *,
    suppress_errors: bool,
) -> None:
    """Unlock and return the dedicated lease connection without pool leakage."""

    lease = context.pop(_NPI_IMPORT_LEASE_KEY, None)
    if lease is None:
        return
    if type(lease) is not _NpiImportLease:
        if suppress_errors:
            return
        raise NPIPrerequisiteError("NPI import lease is invalid")
    try:
        unlocked = await lease.connection.fetchval(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            _NPI_IMPORT_LEASE_DOMAIN,
        )
        if unlocked is not True:
            raise NPIPrerequisiteError("NPI import lease could not be released")
    except BaseException as error:
        await _suppress_lease_manager_exit(lease.manager, error)
        if not suppress_errors:
            raise NPIPrerequisiteError("NPI import lease release failed") from None
    else:
        await lease.manager.__aexit__(None, None, None)


async def _drain_npi_save_tasks(tasks: list[asyncio.Task]) -> None:
    """Wait for all staged writes and cancel every sibling on one failure."""

    if not tasks:
        return
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for pending_task in tasks:
            if not pending_task.done():
                pending_task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        tasks.clear()


async def _cancel_npi_save_tasks(tasks: object) -> None:
    """Cancel and drain a failed attempt's exact owned write tasks."""

    if type(tasks) is not list:
        return
    for pending_task in tasks:
        if isinstance(pending_task, asyncio.Task) and not pending_task.done():
            pending_task.cancel()
    await asyncio.gather(
        *(task for task in tasks if isinstance(task, asyncio.Task)),
        return_exceptions=True,
    )
    tasks.clear()


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _is_environment_enabled(name: str, default: bool = False) -> bool:
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


def _is_postgis_required_for_index(index: dict) -> bool:
    name = str(index.get("name") or "").lower()
    if name.startswith("geo_index_") or name.endswith("_geo_idx") or name == "geo_idx":
        return True
    elements = " ".join(str(element).lower() for element in index.get("index_elements") or ())
    return "st_makepoint" in elements or "geography" in elements


def _is_nucc_required_for_npi(context: dict | None = None) -> bool:
    context = context or {}
    if context.get("test_mode"):
        return _is_environment_enabled("HLTHPRT_NPI_REQUIRE_NUCC_IN_TEST", False)
    return _is_environment_enabled("HLTHPRT_NPI_REQUIRE_NUCC", True)


_index_requires_postgis = _is_postgis_required_for_index
_npi_requires_nucc = _is_nucc_required_for_npi


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
    return _attach_all_npi_address_keys([address], canonical_enabled=canonical_enabled)[0]


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


def _attach_all_npi_address_keys(addresses, *, canonical_enabled: bool) -> list[dict]:
    address_list = list(addresses)
    if not canonical_enabled:
        return address_list
    canonical_rows = canonicalize_address_batch(_npi_address_canon_row(address) for address in address_list)
    for address, canonical in zip(address_list, canonical_rows):
        address_key = canonical.get("address_key")
        address["address_key"] = uuid.UUID(address_key) if isinstance(address_key, str) else address_key
    return address_list


def _prepare_npi_address_rows(addresses, *, canonical_enabled: bool) -> list[dict]:
    return _attach_all_npi_address_keys(
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


def _npi_record_from_source_row(
    npi_source_row: dict,
    npi_csv_map: dict,
) -> dict:
    """Normalize one public NPPES row while discarding private tax fields."""

    npi_record_by_field = {}
    for source_field_name, normalized_field_name in npi_csv_map.items():
        normalized_field_value = npi_source_row[source_field_name]
        if normalized_field_name in _NPI_PRIVATE_SOURCE_FIELDS:
            npi_record_by_field[normalized_field_name] = None
            continue
        if not normalized_field_value or str(normalized_field_value).upper() == '<UNAVAIL>':
            npi_record_by_field[normalized_field_name] = None
            continue
        if normalized_field_name in ('replacement_npi', 'entity_type_code', 'npi',):
            normalized_field_value = int(normalized_field_value)
        elif normalized_field_name.endswith('_date'):
            normalized_field_value = pytz.utc.localize(
                parse_date(normalized_field_value, fuzzy=True)
            )
        npi_record_by_field[normalized_field_name] = normalized_field_value
    return npi_record_by_field


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
    for npi_source_row in task['row_list']:
        npi_obj_list.append(_npi_record_from_source_row(npi_source_row, npi_csv_map))
        taxonomy_array = _taxonomy_array_from_npi_row(npi_source_row, taxonomy_int_code_map)

        if npi_source_row['Provider First Line Business Practice Location Address']:
            address_record_by_field = {
                'first_line': npi_source_row['Provider First Line Business Practice Location Address'],
                'second_line': npi_source_row['Provider Second Line Business Practice Location Address'],
                'city_name': npi_source_row.get('Provider Business Practice Location Address City Name', '').upper(),
                'state_name': npi_source_row.get('Provider Business Practice Location Address State Name', '').upper(),
                'postal_code': npi_source_row['Provider Business Practice Location Address Postal Code'],
                'country_code': npi_source_row[
                    'Provider Business Practice Location Address Country Code (If outside U.S.)'
                ],
            }

            address_record_by_field.update({
                'checksum': return_checksum(list(address_record_by_field.values())),  # addresses have blank symbols
                'npi': int(npi_source_row['NPI']),
                'type': 'primary',
                'telephone_number': npi_source_row['Provider Business Practice Location Address Telephone Number'],
                'fax_number': npi_source_row['Provider Business Practice Location Address Fax Number'],
                'taxonomy_array': taxonomy_array,
                'date_added': pytz.utc.localize(parse_date(npi_source_row['Last Update Date'], fuzzy=True))
                if npi_source_row['Last Update Date']
                else None,
            })
            address_key = '_'.join([
                str(address_record_by_field['npi']),
                str(address_record_by_field['checksum']),
                address_record_by_field['type'],
            ])
            npi_address_list_dict[address_key] = address_record_by_field

        if npi_source_row['Provider First Line Business Mailing Address']:
            address_record_by_field = {
                'first_line': npi_source_row['Provider First Line Business Mailing Address'],
                'second_line': npi_source_row['Provider Second Line Business Mailing Address'],
                'city_name': npi_source_row.get('Provider Business Mailing Address City Name', '').upper(),
                'state_name': npi_source_row.get('Provider Business Mailing Address State Name', '').upper(),
                'postal_code': npi_source_row['Provider Business Mailing Address Postal Code'],
                'country_code': npi_source_row[
                    'Provider Business Mailing Address Country Code (If outside U.S.)'
                ],
            }

            address_record_by_field.update({
                'checksum': return_checksum(list(address_record_by_field.values())),  # addresses have blank symbols
                'npi': int(npi_source_row['NPI']),
                'type': 'mail',
                'telephone_number': npi_source_row['Provider Business Mailing Address Telephone Number'],
                'fax_number': npi_source_row['Provider Business Mailing Address Fax Number'],
                'taxonomy_array': taxonomy_array,
                'date_added': pytz.utc.localize(parse_date(npi_source_row['Last Update Date'], fuzzy=True))
                if npi_source_row['Last Update Date']
                else None,
            })
            address_key = '_'.join([
                str(address_record_by_field['npi']),
                str(address_record_by_field['checksum']),
                address_record_by_field['type'],
            ])
            npi_address_list_dict[address_key] = address_record_by_field

        for i in range(1, 16):
            if npi_source_row[f'Healthcare Provider Taxonomy Code_{i}']:
                taxonomy_record_by_field = {
                    'npi': int(npi_source_row[npi_csv_map_reverse['npi']]),
                    'healthcare_provider_taxonomy_code': npi_source_row[f'Healthcare Provider Taxonomy Code_{i}'],
                    'provider_license_number': npi_source_row[f'Provider License Number_{i}'],
                    'provider_license_number_state_code': npi_source_row[f'Provider License Number State Code_{i}'],
                    'healthcare_provider_primary_taxonomy_switch': npi_source_row[
                        f'Healthcare Provider Primary Taxonomy Switch_{i}']
                }
                checksum = return_checksum(list(taxonomy_record_by_field.values()))
                taxonomy_record_by_field['checksum'] = checksum
                npi_taxonomy_list_dict[checksum] = taxonomy_record_by_field
            else:
                break

        for i in range(1, 51):
            if npi_source_row[f'Other Provider Identifier_{i}']:
                identifier_record_by_field = {
                    'npi': int(npi_source_row[npi_csv_map_reverse['npi']]),
                    'other_provider_identifier': npi_source_row[f'Other Provider Identifier_{i}'],
                    'other_provider_identifier_type_code': npi_source_row[f'Other Provider Identifier Type Code_{i}'],
                    'other_provider_identifier_state': npi_source_row[f'Other Provider Identifier State_{i}'],
                    'other_provider_identifier_issuer': npi_source_row[f'Other Provider Identifier Issuer_{i}']
                }
                checksum = return_checksum(list(identifier_record_by_field.values()))
                identifier_record_by_field['checksum'] = checksum
                npi_other_id_list_dict[checksum] = identifier_record_by_field
            else:
                break

        for i in range(1, 16):
            if npi_source_row[f'Healthcare Provider Taxonomy Group_{i}']:
                taxonomy_group_record_by_field = {
                    'npi': int(npi_source_row[npi_csv_map_reverse['npi']]),
                    'healthcare_provider_taxonomy_group': npi_source_row[
                        f'Healthcare Provider Taxonomy Group_{i}'
                    ],
                }
                checksum = return_checksum(list(taxonomy_group_record_by_field.values()))
                taxonomy_group_record_by_field['checksum'] = checksum
                npi_taxonomy_group_list_dict[checksum] = taxonomy_group_record_by_field
            else:
                break

    normalized_rows_by_kind = {
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
        await save_npi_data(ctx, normalized_rows_by_kind)
        print(f'Processing.. {len(npi_obj_list)} rows directly')
    else:
        await redis.enqueue_job('save_npi_data', normalized_rows_by_kind, _queue_name=NPI_QUEUE_NAME)


async def _prepare_npi_staging(import_date: str, db_schema: str) -> None:
    """Recreate every canonical NPI staging table for one import attempt."""

    for model_class in _NPI_STAGING_CLASSES:
        staged_model = make_class(model_class, import_date)
        await db.status(
            f"DROP TABLE IF EXISTS {db_schema}.{staged_model.__main_table__}_{import_date};"
        )
        await db.create_table(staged_model.__table__, checkfirst=True)
        if hasattr(staged_model, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {staged_model.__tablename__}_idx_primary ON "
                f"{db_schema}.{staged_model.__tablename__} "
                f"({', '.join(staged_model.__my_index_elements__)});"
            )
        for index in getattr(model_class, "__my_initial_indexes__", ()) or ():
            index_name = index.get("name", "_".join(index.get("index_elements")))
            using = f"USING {index['using']} " if index.get("using") else ""
            unique = " UNIQUE " if index.get("unique") else " "
            where = f" WHERE {index['where']} " if index.get("where") else ""
            await db.status(
                f"CREATE{unique}INDEX IF NOT EXISTS "
                f"{staged_model.__tablename__}_idx_{index_name} ON "
                f"{db_schema}.{staged_model.__tablename__} {using}"
                f"({', '.join(index.get('index_elements'))}){where};"
            )



@_release_lease_after_process_failure
async def execute_npi_import_attempt(ctx, task=None):  # pragma: no cover
    """Download and process the configured NPPES dissemination files."""
    # Track whether any work actually ran so shutdown can distinguish "no jobs" from a bad import
    task = task or {}
    ctx.setdefault('context', {})
    context = ctx['context']
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    context['run'] = 0
    context.pop(_NPPES_EVIDENCE_RECEIPT_KEY, None)
    context.pop(_NPPES_EVIDENCE_METRICS_KEY, None)
    await raise_if_cancelled(ctx, task)
    if 'test_mode' in task:
        context['test_mode'] = bool(task.get('test_mode'))
    test_mode = bool(context.get('test_mode', False))
    if test_mode and run_id:
        raise NPIPrerequisiteError(
            "Controlled NPI test mode is unavailable without isolated publication"
        )
    evidence_config = _nppes_evidence_runtime_config(context)
    if test_mode and evidence_config.required:
        raise NPIPrerequisiteError(
            "NPI test mode cannot admit immutable public evidence"
        )
    if run_id and not evidence_config.required:
        raise NPIPrerequisiteError(
            "Controlled NPI publication requires NPPES public evidence"
        )
    scratch_root = (
        resolve_nppes_scratch_root() if evidence_config.required else None
    )
    canonical_addresses_enabled = source_enabled("nppes")
    await ensure_database(test_mode)
    await _acquire_npi_import_lease(context)
    await _assert_npi_import_lease(context)
    if evidence_config.required:
        await _assert_nppes_postgres_runtime(context)
    await _ensure_required_extensions()

    import_date = ctx['import_date']
    db_schema = _runtime_db_schema()
    if evidence_config.required:
        await _assert_nppes_storage_catalog(context, db_schema)
    if _npi_requires_nucc(context):
        await _assert_nucc_ready(db_schema)
    if canonical_addresses_enabled:
        await _assert_nppes_canonical_ready(db_schema)
    taxonomy_int_code_map = await _load_nucc_taxonomy_int_code_map(db_schema)
    if taxonomy_int_code_map:
        print(f"Loaded {len(taxonomy_int_code_map)} NUCC taxonomy integer codes for NPI address load.")
    count_files = 0
    sql_chunk_size = 299999
    max_pending_save_tasks = _env_positive_int(
        "HLTHPRT_NPI_MAX_PENDING_SAVE_TASKS",
        DEFAULT_NPI_MAX_PENDING_SAVE_TASKS,
    )
    pending_save_tasks: list[asyncio.Task] = []
    context[_NPI_PENDING_SAVE_TASKS_KEY] = pending_save_tasks
    staged_source_record_counts: list[tuple[str, int]] = []

    async def evidence_cancel_check() -> None:
        """Propagate the control-run cancellation state into evidence work."""

        await raise_if_cancelled(ctx, task)

    async def enqueue_or_flush(coros: list, coro) -> None:
        """Bound pending save tasks by flushing the active task batch."""
        coros.append(asyncio.create_task(coro))
        if len(coros) >= max_pending_save_tasks:
            await _drain_npi_save_tasks(coros)

    prepared_chain = None
    if evidence_config.required:
        prepared_chain = await prepare_nppes_release_chain(
            evidence_config,
            cancel_check=evidence_cancel_check,
        )
        assert_nppes_scratch_capacity(prepared_chain, scratch_root)
        selected_sources = prepared_chain.archives
    else:
        listing_url = (
            os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR']
            + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE']
        )
        print(listing_url)
        html_source = await download_it(listing_url)
        source_files = re.findall(
            r'(NPPES_Data_Dissemination.*_V2.zip)', html_source
        )
        file_limit = TEST_NPI_MAX_FILES if test_mode else None
        selected_sources = source_files[:file_limit] if file_limit else source_files
    if not selected_sources:
        raise NPIPrerequisiteError("No NPPES source archives were discovered")
    await raise_if_cancelled(ctx, task)
    await _assert_npi_import_lease(context)
    await _prepare_npi_staging(import_date, db_schema)
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="npi",
            status="running",
            phase="npi sources discovered",
            unit="files",
            done=0,
            total=len(selected_sources),
            message=f"{len(selected_sources)} source files discovered",
        )
    for file_idx, source_item in enumerate(selected_sources):
        await raise_if_cancelled(ctx, task)
        prepared_archive = source_item if evidence_config.required else None
        archive_filename = (
            prepared_archive.archive_name
            if prepared_archive is not None
            else source_item
        )
        count_files = count_files + 1
        current_sql_chunk_size = sql_chunk_size
        if test_mode:
            current_sql_chunk_size = min(current_sql_chunk_size, TEST_NPI_ROWS)
        print(f"Round {count_files} for {archive_filename}")
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="npi",
                status="running",
                phase="npi downloading source",
                unit="files",
                done=file_idx,
                total=len(selected_sources),
                message=f"downloading file {file_idx + 1}/{len(selected_sources)}",
                label=archive_filename,
            )
        with tempfile.TemporaryDirectory(dir=scratch_root) as tmpdirname:
            print(f"Found: {archive_filename}")
            # await unzip('/users/nick/downloads/NPPES_Data_Dissemination_November_2022.zip', tmpdirname, __debug=True)

            if prepared_archive is not None:
                legacy_layout = await materialize_prepared_nppes_archive(
                    prepared_archive,
                    Path(tmpdirname),
                )
                npi_file = str(legacy_layout.primary_path)
                pl_file = str(legacy_layout.practice_location_path)
                other_file = str(legacy_layout.other_name_path)
                endpoint_file = str(legacy_layout.endpoint_path)
            else:
                legacy_layout = None
                tmp_filename = str(PurePath(str(tmpdirname), archive_filename))
                await download_it_and_save(
                    os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + archive_filename,
                    tmp_filename,
                    chunk_size=10 * 1024 * 1024,
                    cache_dir='/tmp',
                )
                print(f"Downloaded: {archive_filename}")
                if os.environ.get("DEBUG"):
                    print(
                        f"DEBUG: Downloaded file {tmp_filename}, "
                        f"size: {os.path.getsize(tmp_filename)} bytes"
                    )
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
                npi_file = [
                    filename
                    for filename in glob.glob(f"{tmpdirname}/npi*.csv")
                    if not os.path.basename(filename).endswith('_fileheader.csv')
                ][0]
                pl_file = [
                    filename
                    for filename in glob.glob(f"{tmpdirname}/pl_pfile*.csv")
                    if not os.path.basename(filename).endswith('_fileheader.csv')
                ][0]
                other_file = [
                    filename
                    for filename in glob.glob(f"{tmpdirname}/other*.csv")
                    if not os.path.basename(filename).endswith('_fileheader.csv')
                ][0]
                endpoint_file = [
                    filename
                    for filename in glob.glob(f"{tmpdirname}/endpoint*.csv")
                    if not os.path.basename(filename).endswith('_fileheader.csv')
                ][0]
            if count_files > 1:
                # Collect all NPIs from npi_file and pl_file
                current_sql_chunk_size = sql_chunk_size // 26
                npi_set: set[int] = set()
                async with _open_nppes_source(
                    legacy_layout, "primary", npi_file
                ) as afp:
                    async for source_csv_row in AsyncDictReader(afp, delimiter=","):
                        if source_csv_row.get('NPI'):
                            npi_set.add(int(source_csv_row['NPI']))

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

                async with _open_nppes_source(
                    legacy_layout, "practice_location", pl_file
                ) as afp:
                    async for source_csv_row in AsyncDictReader(afp, delimiter=","):
                        if source_csv_row.get('NPI'):
                            npi_set.add(int(source_csv_row['NPI']))

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

                async with _open_nppes_source(
                    legacy_layout, "other_name", other_file
                ) as afp:
                    async for source_csv_row in AsyncDictReader(afp, delimiter=","):
                        if source_csv_row.get('NPI'):
                            npi_set.add(int(source_csv_row['NPI']))

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

            for source_file_path in (endpoint_file, other_file, pl_file, npi_file):
                print(f"Files: {source_file_path}")


            npi_csv_map = {}
            npi_csv_map_reverse = {}

            int_key_re = re.compile(r'.*_\d+$')

            async with _open_nppes_source(
                legacy_layout, "primary", npi_file
            ) as afp:
                async for source_csv_row in AsyncDictReader(afp, delimiter=","):
                    for key in source_csv_row:
                        if int_key_re.match(key) or ' Address' in key:
                            continue
                        normalized_column_name = re.sub(r"\(.*\)", r"", key.lower()).strip().replace(' ', '_')
                        npi_csv_map[key] = normalized_column_name
                        npi_csv_map_reverse[normalized_column_name] = key
                    break
            count = 0


            row_list = []
            coros = pending_save_tasks
            processed_rows = 0
            async with _open_nppes_source(
                legacy_layout, "primary", npi_file
            ) as afp:
                async for source_csv_row in AsyncDictReader(afp, delimiter=","):
                    if not source_csv_row['NPI']:
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Processed: {count}")
                    row_list.append(source_csv_row)
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
                            label=archive_filename,
                        )
                    if count > current_sql_chunk_size:
                        print(f"Sending to DB: {count}")
                        await raise_if_cancelled(ctx, task)
                        npi_chunk_by_field = {
                            'row_list': row_list.copy(),
                            'npi_csv_map': npi_csv_map,
                            'npi_csv_map_reverse': npi_csv_map_reverse,
                            'taxonomy_int_code_map': taxonomy_int_code_map,
                            'direct': True,
                        }
                        await enqueue_or_flush(coros, process_npi_chunk(ctx, npi_chunk_by_field))
                        row_list.clear()
                        count = 0
                    else:
                        count += 1
                    if test_mode and processed_rows >= TEST_NPI_ROWS:
                        break

            npi_chunk_by_field = {
                'row_list': row_list.copy(),
                'npi_csv_map': npi_csv_map,
                'npi_csv_map_reverse': npi_csv_map_reverse,
                'taxonomy_int_code_map': taxonomy_int_code_map,
                'direct': True,
            }
            await raise_if_cancelled(ctx, task)
            await enqueue_or_flush(coros, process_npi_chunk(ctx, npi_chunk_by_field))
            row_list.clear()

            npi_other_org_list_dict = {}

            async with _open_nppes_source(
                legacy_layout, "other_name", other_file
            ) as afp:
                processed_other = 0
                async for source_csv_row in AsyncDictReader(afp, delimiter=","):
                    if not source_csv_row['NPI']:
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Other Names Processed: {count}")
                    other_identifier_by_field = {
                        'npi': int(source_csv_row['NPI']),
                        'other_provider_identifier': source_csv_row['Provider Other Organization Name'],
                        'other_provider_identifier_type_code': source_csv_row[
                            'Provider Other Organization Name Type Code'
                        ],
                        'other_provider_identifier_state': None,
                        'other_provider_identifier_issuer': None,
                    }
                    checksum = return_checksum(list(other_identifier_by_field.values()))
                    other_identifier_by_field['checksum'] = checksum
                    npi_other_org_list_dict[checksum] = other_identifier_by_field

                    if count > current_sql_chunk_size:
                        print(f"Sending to DB: {count}")
                        await raise_if_cancelled(ctx, task)
                        other_identifier_rows_by_key = {
                            'npi_other_id_list': list(npi_other_org_list_dict.copy().values())
                        }
                        await enqueue_or_flush(coros, save_npi_data(ctx, other_identifier_rows_by_key))
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
                            label=archive_filename,
                        )
                    if test_mode and processed_other >= TEST_NPI_OTHER_ROWS:
                        break

            other_identifier_rows_by_key = {
                'npi_other_id_list': list(npi_other_org_list_dict.copy().values())
            }
            await enqueue_or_flush(coros, save_npi_data(ctx, other_identifier_rows_by_key))
            npi_other_org_list_dict.clear()


            npi_address_list_dict = {}
            async with _open_nppes_source(
                legacy_layout, "practice_location", pl_file
            ) as afp:
                processed_secondary = 0
                async for source_csv_row in AsyncDictReader(afp, delimiter=","):
                    if (
                        not source_csv_row['NPI']
                        and not source_csv_row['Provider Secondary Practice Location Address- Address Line 1']
                    ):
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Secondary Addresses Processed: {count}")
                    secondary_address_by_field = {
                        'first_line': source_csv_row[
                            'Provider Secondary Practice Location Address- Address Line 1'
                        ],
                        'second_line': source_csv_row[
                            'Provider Secondary Practice Location Address-  Address Line 2'
                        ],
                        'city_name': source_csv_row.get(
                            'Provider Secondary Practice Location Address - City Name',
                            '',
                        ).upper(),
                        'state_name': source_csv_row.get(
                            'Provider Secondary Practice Location Address - State Name',
                            '',
                        ).upper(),
                        'postal_code': source_csv_row[
                            'Provider Secondary Practice Location Address - Postal Code'
                        ],
                        'country_code': source_csv_row[
                            'Provider Secondary Practice Location Address - Country Code (If outside U.S.)'
                        ],
                    }

                    secondary_address_by_field.update({
                        'checksum': return_checksum(
                            list(secondary_address_by_field.values())
                        ),  # addresses have blank symbols
                        'npi': int(source_csv_row['NPI']),
                        'type': 'secondary',
                        'telephone_number': source_csv_row[
                            'Provider Secondary Practice Location Address - Telephone Number'
                        ],
                        'fax_number': source_csv_row['Provider Practice Location Address - Fax Number'],
                        'date_added': pytz.utc.localize(datetime.datetime.now())
                    })
                    address_key = '_'.join([
                        str(secondary_address_by_field['npi']),
                        str(secondary_address_by_field['checksum']),
                        secondary_address_by_field['type'],
                    ])
                    npi_address_list_dict[address_key] = secondary_address_by_field

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
                            label=archive_filename,
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
            await _drain_npi_save_tasks(coros)
            npi_address_list_dict.clear()

            if evidence_config.required:
                if processed_rows <= 0:
                    raise NPIPrerequisiteError(
                        "NPPES primary member did not contain source rows"
                    )
                staged_source_record_counts.append(
                    (archive_filename, processed_rows)
                )

            print(f"Processed: {count}")
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="npi",
                    status="running",
                    phase="npi source processed",
                    unit="files",
                    done=file_idx + 1,
                    total=len(selected_sources),
                    message=f"processed file {file_idx + 1}/{len(selected_sources)}",
                    label=archive_filename,
                )

    if evidence_config.required:
        chain_receipt = await import_nppes_public_evidence_chain(
            prepared_chain,
            evidence_config,
            expected_source_record_counts=tuple(staged_source_record_counts),
            schema=db_schema,
            cancel_check=evidence_cancel_check,
        )
        validated_receipt = validate_nppes_public_evidence_chain_receipt(
            chain_receipt
        )
        await _assert_nppes_canonical_stage_parity(
            validated_receipt,
            import_date,
            db_schema,
        )
        context[_NPPES_EVIDENCE_RECEIPT_KEY] = validated_receipt
        context[_NPPES_EVIDENCE_METRICS_KEY] = _nppes_evidence_metrics(
            validated_receipt
        )

    # Mark this job as successfully completed; shutdown finalization depends on this.
    if pending_save_tasks:
        raise NPIPrerequisiteError("NPI staged writes did not drain")
    context.pop(_NPI_PENDING_SAVE_TASKS_KEY, None)
    context['run'] = context.get('run', 0) + 1


async def process_data(ctx, task=None):  # pragma: no cover
    """Run the NPI worker entry point."""
    return await execute_npi_import_attempt(ctx, task)


async def startup(ctx):  # pragma: no cover
    """Initialize NPI worker state and import staging tables."""

    evidence_config = resolve_nppes_evidence_runtime_config()
    await my_init_db(db)
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.utcnow()
    ctx['context']['run'] = 0
    ctx['context']['test_mode'] = False
    ctx['context'][_NPPES_EVIDENCE_CONFIG_KEY] = evidence_config
    await ensure_database(False)
    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    if override_import_id:
        ctx['import_date'] = override_import_id
    else:
        ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    import_date = ctx['import_date']
    db_schema = _runtime_db_schema()

    await _ensure_required_extensions()
    try:
        archive_model = AddressArchive
        await db.create_table(AddressArchive.__table__, checkfirst=True)
        if hasattr(AddressArchive, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {archive_model.__tablename__}_idx_primary ON "
                f"{db_schema}.{archive_model.__tablename__} "
                f"({', '.join(archive_model.__my_index_elements__)});"
            )
    except DuplicateTableError:
        print(f"Address archive table {db_schema}.{archive_model.__tablename__} already exists.")
    print("Preparing done")

def _do_business_as_update_sql(
    db_schema: str,
    target_table: str,
    source_table: str,
) -> str:
    """Return the set-based business-name enrichment statement."""
    return f"""
        WITH sub AS (
            SELECT
                npi,
                ARRAY_AGG(DISTINCT other_provider_identifier ORDER BY other_provider_identifier) AS names,
                STRING_AGG(DISTINCT other_provider_identifier, ' ' ORDER BY other_provider_identifier) AS search_text
            FROM {db_schema}.{source_table}
            WHERE other_provider_identifier_type_code = '3'
              AND NULLIF(other_provider_identifier, '') IS NOT NULL
            GROUP BY npi
        ),
        updated AS (
            UPDATE {db_schema}.{target_table} AS n
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


def _do_business_as_clear_sql(
    db_schema: str,
    target_table: str,
    source_table: str,
) -> str:
    """Return the statement that clears business names absent from the source."""
    return f"""
        WITH cleared AS (
            UPDATE {db_schema}.{target_table} AS n
            SET
                do_business_as = ARRAY[]::varchar[],
                do_business_as_text = ''
            WHERE (
                    COALESCE(array_length(n.do_business_as, 1), 0) > 0
                 OR COALESCE(n.do_business_as_text, '') <> ''
            )
              AND NOT EXISTS (
                    SELECT 1
                    FROM {db_schema}.{source_table} AS s
                    WHERE s.npi = n.npi
                      AND s.other_provider_identifier_type_code = '3'
                      AND NULLIF(s.other_provider_identifier, '') IS NOT NULL
              )
            RETURNING 1
        )
        SELECT count(*) FROM cleared;
    """


async def refresh_do_business_as(
    target_table: str | None = None,
    source_table: str | None = None,
    test_mode: bool | None = None,
) -> tuple[int, int] | None:
    """Populate NPI business names from other identifier entries."""
    await ensure_database(bool(test_mode))
    db_schema = _runtime_db_schema()
    target_table_name = target_table or NPIData.__tablename__
    source_table_name = source_table or NPIDataOtherIdentifier.__tablename__
    if not await db.scalar(
        f"SELECT to_regclass('{db_schema}.{source_table_name}');"
    ):
        print(
            "Skipping do_business_as refresh: source table "
            f"{db_schema}.{source_table_name} does not exist."
        )
        return

    updated = int(
        await db.scalar(
            _do_business_as_update_sql(
                db_schema,
                target_table_name,
                source_table_name,
            )
        )
        or 0
    )
    cleared = int(
        await db.scalar(
            _do_business_as_clear_sql(
                db_schema,
                target_table_name,
                source_table_name,
            )
        )
        or 0
    )
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


async def _resolve_npi_archive_once(
    staging_table: str,
    field_map: dict[str, str],
    schema: str,
    cancel_check,
):
    """Resolve the staged NPI addresses through the canonical archive."""
    return await resolve_into_archive(
        staging_table,
        field_map,
        source_bit=1,
        priority=0,
        schema=schema,
        cancel_check=cancel_check,
    )


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
        stats = await _resolve_npi_archive_once(
            staging_table,
            field_map,
            schema,
            cancel_check,
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
        stats = await _resolve_npi_archive_once(
            staging_table,
            field_map,
            schema,
            cancel_check,
        )
    print(f"NPI canonical address resolve complete: {stats}")
    return stats


async def _is_phone_staffing_ready(
    target_table: str,
    address_table: str,
    schema: str,
) -> bool:
    """Validate the tables required by phone staffing materialization."""
    if not await db.scalar(f"SELECT to_regclass('{schema}.{target_table}');"):
        print(
            f"Skipping phone staffing materialization for {schema}.{target_table}: "
            "target staging table is missing."
        )
        return False
    if not await db.scalar(f"SELECT to_regclass('{schema}.{address_table}');"):
        print(
            f"Skipping phone staffing materialization for {schema}.{target_table}: "
            f"address staging table {schema}.{address_table} is missing."
        )
        return False
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
    return True


async def rebuild_phone_staffing_table(
    *,
    target_table: str,
    address_table: str,
    schema: str,
) -> None:
    """Rebuild the phone-level provider staffing materialization."""
    if not await _is_phone_staffing_ready(target_table, address_table, schema):
        return

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




def _npi_shutdown_timing(
    phase: str,
    status: str,
    started: float,
    started_at: str,
) -> tuple[dict[str, object], float]:
    """Shape one terminal NPI shutdown timing record."""
    elapsed = round(time.monotonic() - started, 3)
    return (
        {
            "phase": phase,
            "status": status,
            "elapsed_seconds": elapsed,
            "started_at": started_at,
            "finished_at": datetime.datetime.now(datetime.UTC).isoformat(),
        },
        elapsed,
    )


def _emit_npi_shutdown_progress(
    run_id: str,
    phase: str,
    status: str,
    done: int,
) -> None:
    """Emit one shutdown phase transition when control is attached."""
    if not run_id:
        return
    action = "completed" if done else ("failed" if status == "failed" else "started")
    enqueue_live_progress(
        run_id=run_id,
        importer="npi",
        status=status,
        phase=f"npi shutdown {phase}",
        unit="phase",
        total=1,
        done=done,
        pct=100 if done else 0,
        message=f"npi shutdown {phase} {action}",
    )


async def _load_npi_commit_on_fresh_connection(
    *,
    schema: str,
    commit: NpiCanonicalPublicationCommit,
    progress_by_name: dict[str, object],
    metrics_by_name: dict[str, object],
) -> NpiCanonicalPublicationCommit | None:
    """Read one exact sealed commit without reusing an uncertain connection."""

    try:
        async with db.acquire_driver() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL lock_timeout='30s'")
                await connection.execute("SET LOCAL statement_timeout='35s'")
                is_settled = await has_settled_npi_publication(
                    connection,
                    schema=schema,
                    run_id=commit.receipt.run_id,
                )
                if not is_settled:
                    return None
                return await load_committed_npi_publication(
                    connection,
                    schema=schema,
                    receipt=commit.receipt,
                    progress_by_name=progress_by_name,
                    metrics_by_name=metrics_by_name,
                )
    except BaseException:
        return None


async def _reconcile_npi_commit_after_error(
    *,
    schema: str,
    commit: NpiCanonicalPublicationCommit,
    progress_by_name: dict[str, object],
    metrics_by_name: dict[str, object],
) -> NpiCanonicalPublicationCommit | None:
    """Shield one fresh read while the publishing task may be canceled."""

    reconcile_task = asyncio.create_task(
        _load_npi_commit_on_fresh_connection(
            schema=schema,
            commit=commit,
            progress_by_name=progress_by_name,
            metrics_by_name=metrics_by_name,
        )
    )
    while True:
        try:
            return await asyncio.shield(reconcile_task)
        except asyncio.CancelledError:
            continue


def _install_npi_committed_control_state(
    context: dict[str, object],
    publication_state_by_name: dict[str, object],
) -> None:
    """Expose one exact durable terminal projection to the control wrapper."""

    publication_commit = publication_state_by_name.get("commit")
    terminal_progress_by_name = publication_state_by_name.get("progress")
    terminal_metrics_by_name = publication_state_by_name.get("metrics")
    if (
        type(publication_commit) is not NpiCanonicalPublicationCommit
        or type(terminal_progress_by_name) is not dict
        or type(terminal_metrics_by_name) is not dict
    ):
        raise NPIPrerequisiteError("NPI publication commit is invalid")
    context["preserve_control_run_finished_at"] = True
    context[_NPI_CONTROL_COMMITTED_HEARTBEAT_AT_KEY] = (
        publication_commit.heartbeat_at
    )
    context[_NPI_CONTROL_COMMITTED_FINISHED_AT_KEY] = (
        publication_commit.finished_at
    )
    context[_NPI_CONTROL_COMMITTED_RESULT_KEY] = {
        **terminal_metrics_by_name,
        "terminal_progress": terminal_progress_by_name,
    }
    context[_NPI_CONTROL_TERMINAL_COMMITTED_KEY] = True


@asynccontextmanager
async def _npi_publication_transaction(
    *,
    lease: _NpiImportLease,
    schema: str,
    context: dict[str, object],
    publication_state_by_name: dict[str, object],
):
    """Converge an uncertain transaction exit onto its durable receipt seal."""

    try:
        async with lease.connection.transaction():
            yield
    except BaseException as transaction_error:
        commit = publication_state_by_name.get("commit")
        progress = publication_state_by_name.get("progress")
        metrics = publication_state_by_name.get("metrics")
        if (
            type(commit) is not NpiCanonicalPublicationCommit
            or type(progress) is not dict
            or type(metrics) is not dict
        ):
            raise
        reconciled = await _reconcile_npi_commit_after_error(
            schema=schema,
            commit=commit,
            progress_by_name=progress,
            metrics_by_name=metrics,
        )
        if reconciled is None:
            if isinstance(transaction_error, asyncio.CancelledError):
                raise
            raise publication_error() from None
        publication_state_by_name["commit"] = reconciled
        current_task = asyncio.current_task()
        while current_task is not None and current_task.cancelling():
            current_task.uncancel()
    _install_npi_committed_control_state(context, publication_state_by_name)


def _print_time_info_best_effort(started_at: object) -> None:
    """Report elapsed import time without invalidating an already-committed run."""

    try:
        print_time_info(started_at)
    except Exception:
        return


@_release_lease_after_shutdown
async def finalize_npi_import_attempt(ctx):  # pragma: no cover
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
        _emit_npi_shutdown_progress(run_id, phase, "running", 0)
        try:
            phase_result = await awaitable
        except Exception:
            timing, elapsed = _npi_shutdown_timing(
                phase,
                "failed",
                started,
                started_at,
            )
            shutdown_phase_timings.append(timing)
            print(f"NPI_SHUTDOWN_PHASE_FAILED phase={phase} elapsed_seconds={elapsed}")
            _emit_npi_shutdown_progress(run_id, phase, "failed", 0)
            raise
        timing, elapsed = _npi_shutdown_timing(
            phase,
            "succeeded",
            started,
            started_at,
        )
        shutdown_phase_timings.append(timing)
        print(f"NPI_SHUTDOWN_PHASE_DONE phase={phase} elapsed_seconds={elapsed}")
        _emit_npi_shutdown_progress(run_id, phase, "running", 1)
        return phase_result

    if not context.get('run'):
        print("No NPI jobs ran in this worker session; skipping shutdown validation.")
        return
    attempt_id, attempt_started_at = _required_npi_control_attempt(context, run_id)
    await _assert_npi_import_lease(context)
    if context.get("test_mode"):
        raise NPIPrerequisiteError("NPI test mode cannot publish live tables")
    evidence_config = _nppes_evidence_runtime_config(context)
    if not evidence_config.required:
        raise NPIPrerequisiteError(
            "Controlled NPI publication requires NPPES public evidence"
        )
    evidence_receipt = _required_nppes_evidence_receipt(context)
    evidence_metrics = _nppes_evidence_metrics(evidence_receipt)
    await ensure_database(bool(context.get("test_mode")))

    db_schema = _runtime_db_schema()
    if _npi_requires_nucc(context):
        await _assert_nucc_ready(db_schema)
    if source_enabled("nppes"):
        await _assert_nppes_canonical_ready(db_schema)
    staged_models_by_table = {}
    has_missing_stage = False
    for model_class in _NPI_STAGING_CLASSES:
        staged_model = make_class(model_class, import_date)
        stage_exists = await db.scalar(
            f"SELECT to_regclass('{db_schema}.{staged_model.__tablename__}');"
        )
        has_missing_stage = has_missing_stage or not bool(stage_exists)
    if has_missing_stage:
        raise NPIPrerequisiteError("NPI staging table set is incomplete")

    test = make_class(NPIAddress, import_date)
    npi_address_count = await db.scalar(select(func.count(test.npi)))
    if context.get("test_mode"):
        print(f"Test mode: imported {npi_address_count} NPI addresses (no minimum enforced).")
    else:
        if not npi_address_count or npi_address_count < 5_000_000:
            raise NPIPrerequisiteError("NPI staging address census is incomplete")

    processing_classes_array = _NPI_STAGING_CLASSES

    async def has_table(table_name: str) -> bool:
        """Check whether a table exists in the active import schema."""
        exists = await db.scalar(f"SELECT to_regclass('{db_schema}.{table_name}');")
        return bool(exists)

    async def has_table_column(table_name: str, column_name: str) -> bool:
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

    postgis_availability_by_key: dict[str, bool | None] = {"available": None}

    async def has_postgis() -> bool:
        """Cache whether PostGIS types and constructors are available."""
        if postgis_availability_by_key["available"] is None:
            geography_type = await db.scalar("SELECT to_regtype('geography');")
            st_makepoint = await db.scalar("SELECT to_regprocedure('st_makepoint(double precision, double precision)');")
            postgis_availability_by_key["available"] = bool(geography_type and st_makepoint)
            if not postgis_availability_by_key["available"]:
                print("PostGIS is unavailable; geo GIST index creation will be skipped.")
        return bool(postgis_availability_by_key["available"])

    address_stats = None
    if source_enabled("nppes"):
        async def _cancel_check():
            await raise_if_cancelled(ctx, {"run_id": run_id})

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
        if _is_environment_enabled("HLTHPRT_NPI_OPENADDRESSES_BACKFILL", False):
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

    async def _timed_geo_update(
        staged_address_model,
        archive_source: str,
        use_canonical_archive: bool,
    ):
        async def _run_geo_update():
            formatted_address_sql = (
                f"{db_schema}.{ADDRESS_FORMAT_FUNCTION}("
                "a.first_line, a.second_line, a.city_name, a.state_name, "
                "a.postal_code, a.country_code)"
            )
            if use_canonical_archive and await has_table_column(
                archive_source,
                "address_key",
            ):
                await db.status(
                    f"UPDATE {db_schema}.{staged_address_model.__tablename__} as a SET formatted_address = {formatted_address_sql}, "
                    f"lat = b.lat, long = b.long, place_id = b.place_id "
                    f"FROM {db_schema}.{archive_source} as b "
                    f"WHERE a.address_key IS NOT NULL AND a.address_key = b.address_key"
                    f" AND b.lat IS NOT NULL AND b.long IS NOT NULL"
                )
                if await has_table("address_checksum_map"):
                    await db.status(
                        f"UPDATE {db_schema}.{staged_address_model.__tablename__} as a SET formatted_address = {formatted_address_sql}, "
                        f"lat = b.lat, long = b.long, place_id = b.place_id "
                        f"FROM {db_schema}.address_checksum_map as m "
                        f"JOIN {db_schema}.{archive_source} as b ON b.address_key = m.address_key "
                        f"WHERE a.address_key IS NULL AND a.checksum = m.checksum"
                        f" AND b.lat IS NOT NULL AND b.long IS NOT NULL"
                    )
            else:
                await db.status(
                    f"UPDATE {db_schema}.{staged_address_model.__tablename__} as a SET formatted_address = {formatted_address_sql}, "
                    f"lat = b.lat, long = b.long, place_id = b.place_id "
                    f"FROM {db_schema}.{archive_source} as b WHERE a.checksum = b.checksum"
                )

        return await timed_shutdown_phase("geocode_archive_enrichment", _run_geo_update())

    async def create_additional_indexes(cls, staged_model) -> None:
        """Create model-declared secondary indexes on a staged table."""
        if not hasattr(cls, '__my_additional_indexes__') or not cls.__my_additional_indexes__:
            return
        for index in cls.__my_additional_indexes__:
            index_name = index.get('name', '_'.join(index.get('index_elements')))
            if _index_requires_postgis(index) and not await has_postgis():
                print(
                    f"Skipping index {staged_model.__tablename__}_idx_{index_name}: "
                    "requires PostGIS (geography + ST_MakePoint)."
                )
                continue
            using = ""
            if index_method := index.get('using'):
                using = f"USING {index_method} "
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
                f"CREATE INDEX IF NOT EXISTS {staged_model.__tablename__}_idx_{index_name} "
                f"ON {db_schema}.{staged_model.__tablename__}  {using}"
                f"({', '.join(index_elements)}){where_clause};"
            )
            print(create_index_sql)
            await timed_shutdown_phase(
                f"index_creation:{staged_model.__tablename__}:{index_name}",
                db.status(create_index_sql),
            )

    deferred_npi_indexes_obj = None

    async with db.transaction():
        for cls in processing_classes_array:
            staged_models_by_table[cls.__main_table__] = make_class(cls, import_date)
            staged_model = staged_models_by_table[cls.__main_table__]
            if cls is NPIData:
                deferred_npi_indexes_obj = staged_model
            if cls is NPIDataOtherIdentifier:
                print('Updating NPI do_business_as arrays from other identifiers...')
                target_npi_cls = staged_models_by_table.get(NPIData.__main_table__)
                target_table_name = target_npi_cls.__tablename__ if target_npi_cls else NPIData.__tablename__
                source_table_name = staged_model.__tablename__
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
                if await has_table(npi_taxonomy_table) and await has_table(
                    "nucc_taxonomy"
                ):
                    print("Updating NUCC Taxonomy for NPI Addresses...")
                    await timed_shutdown_phase(
                        "taxonomy_array_enrichment",
                        refresh_taxonomy_arrays(
                            address_table=staged_model.__tablename__,
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
                use_canonical_archive = _is_environment_enabled("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER")
                archive_source = (
                    preferred_archive
                    if use_canonical_archive and await has_table(preferred_archive)
                    else "address_archive"
                )
                if await has_table(archive_source):
                    print(f"Updating NPI Addresses Geo from Archive {archive_source}...")
                    await _timed_geo_update(staged_model, archive_source, use_canonical_archive)
                else:
                    print(f"Skipping NPI geo update: no address archive table is available in {db_schema}.")

                if await has_table("plan_npi_raw"):
                    print("Updating NPI Plan-Network Array from Plans Import Data...")
                    await timed_shutdown_phase(
                        "plan_network_array_enrichment",
                        db.status(
                        f"""UPDATE {db_schema}.{staged_model.__tablename__} as a
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

                if await has_table("pricing_provider_procedure"):
                    print("Updating NPI procedures_array from pricing provider procedures...")
                    await timed_shutdown_phase(
                        "procedures_array_enrichment",
                        db.status(
                        f"""UPDATE {db_schema}.{staged_model.__tablename__} AS a
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

                if await has_table("pricing_provider_prescription"):
                    print("Updating NPI medications_array from pricing provider prescriptions...")
                    await timed_shutdown_phase(
                        "medications_array_enrichment",
                        db.status(
                        f"""UPDATE {db_schema}.{staged_model.__tablename__} AS a
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
                address_stage = staged_models_by_table.get(NPIAddress.__main_table__)
                if address_stage is None:
                    raise NPIPrerequisiteError("NPI address staging model is unavailable")
                await timed_shutdown_phase(
                    "phone_staffing_rebuild",
                    rebuild_phone_staffing_table(
                        target_table=staged_model.__tablename__,
                        address_table=address_stage.__tablename__,
                        schema=db_schema,
                    ),
                )

            if cls is not NPIData:
                await create_additional_indexes(cls, staged_model)

        if deferred_npi_indexes_obj is not None:
            await create_additional_indexes(NPIData, deferred_npi_indexes_obj)

    # Run VACUUM FULL ANALYZE in parallel for all tables
    async def vacuum_table(obj):
        """Run the post-index full vacuum for one promoted NPI table."""
        if not await has_table(obj.__tablename__):
            raise NPIPrerequisiteError("NPI staging table disappeared before vacuum")
        print(f"Post-Index VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")
        await timed_shutdown_phase(
            f"vacuum_analyze:{obj.__tablename__}",
            db.execute_ddl(f"VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};"),
        )

    vacuum_tasks = [
        asyncio.create_task(
            vacuum_table(staged_models_by_table[cls.__main_table__])
        )
        for cls in processing_classes_array
    ]
    await _drain_npi_save_tasks(vacuum_tasks)

    terminal_metrics_by_name = {
        "npi_shutdown_phase_timings": shutdown_phase_timings,
        **({"address_resolve": address_stats.__dict__} if address_stats else {}),
        "openaddresses_backfill_enabled": _is_environment_enabled(
            "HLTHPRT_NPI_OPENADDRESSES_BACKFILL", False
        ),
        _NPPES_EVIDENCE_METRICS_KEY: evidence_metrics,
    }
    await raise_if_cancelled(ctx, {"run_id": run_id})
    await _assert_npi_import_lease(context)
    lease = context.get(_NPI_IMPORT_LEASE_KEY)
    if type(lease) is not _NpiImportLease:
        raise NPIPrerequisiteError("NPI import lease is missing")
    schema = _postgres_identifier(db_schema)
    publication_import_date = _canonical_publication_import_date(import_date)
    publication_state_by_name: dict[str, object] = {}

    async with (
        suppress_control_run_heartbeat_persistence(run_id),
        _npi_publication_transaction(
            lease=lease,
            schema=schema,
            context=context,
            publication_state_by_name=publication_state_by_name,
        ),
    ):
        await lock_npi_publication_attempt(
            lease.connection,
            schema=schema,
            run_id=run_id,
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at,
        )
        await raise_if_cancelled(ctx, {"run_id": run_id})
        await _assert_npi_import_lease(context)

        stage_table_by_live_table = {
            _postgres_identifier(cls.__main_table__): _postgres_identifier(
                staged_models_by_table[cls.__main_table__].__tablename__
            )
            for cls in processing_classes_array
        }
        await lease.connection.execute(
            "LOCK TABLE "
            + ", ".join(
                f"{schema}.{stage_table}"
                for stage_table in stage_table_by_live_table.values()
            )
            + " IN ACCESS EXCLUSIVE MODE"
        )
        await _install_npi_postseal_guards(
            lease.connection,
            schema=schema,
            stage_tables=tuple(stage_table_by_live_table.values()),
        )
        stage_row_counts_by_table: dict[str, int] = {}
        for table, stage_table in stage_table_by_live_table.items():
            row_count = await lease.connection.fetchval(
                f"SELECT count(*)::bigint FROM {schema}.{stage_table}"
            )
            if type(row_count) is not int or row_count < 0:
                raise NPIPrerequisiteError("NPI staging census is invalid")
            stage_row_counts_by_table[table] = row_count
        publication_row_counts = _canonical_publication_row_counts(
            stage_row_counts_by_table
        )
        published_address_count = stage_row_counts_by_table["npi_address"]
        terminal_progress_by_name = {
            "unit": "rows",
            "done": published_address_count,
            "total": published_address_count,
            "pct": 100,
            "message": "succeeded",
            "phase": "npi published",
        }
        terminal_metrics_by_name.update(
            {
                "stage_rows": stage_row_counts_by_table,
                "npi_address_rows": published_address_count,
            }
        )
        await raise_if_cancelled(ctx, {"run_id": run_id})
        await _assert_npi_import_lease(context)

        for cls in processing_classes_array:
            staged_model = staged_models_by_table[cls.__main_table__]
            table = _postgres_identifier(staged_model.__main_table__)
            stage_table = _postgres_identifier(staged_model.__tablename__)
            index_definitions = tuple(
                getattr(cls, "__my_initial_indexes__", ()) or ()
            ) + tuple(getattr(cls, "__my_additional_indexes__", ()) or ())
            index_suffixes = tuple(
                index_definition.get(
                    "name",
                    "_".join(index_definition.get("index_elements")),
                )
                for index_definition in index_definitions
            )

            await timed_shutdown_phase(
                f"publish_swap:{table}",
                _rotate_npi_canonical_table(
                    lease.connection,
                    schema=schema,
                    live_table=table,
                    stage_table=stage_table,
                    index_suffixes=index_suffixes,
                ),
            )
        relation_oids = await canonical_relation_oids(
            lease.connection,
            schema=schema,
        )
        publication_receipt = await insert_npi_publication_receipt(
            lease.connection,
            schema=schema,
            publication_input=NpiCanonicalPublicationInput(
                run_id,
                attempt_id,
                attempt_started_at,
                evidence_receipt.chain_ref,
                publication_import_date,
                relation_oids,
                publication_row_counts,
            ),
        )
        terminal_metrics_by_name["npi_canonical_publication"] = (
            npi_publication_metrics(publication_receipt)
        )
        publication_state_by_name["progress"] = terminal_progress_by_name
        publication_state_by_name["metrics"] = terminal_metrics_by_name
        publication_state_by_name["commit"] = await mark_npi_publication_succeeded(
            lease.connection,
            schema=schema,
            receipt=publication_receipt,
            progress_by_name=terminal_progress_by_name,
            metrics_by_name=terminal_metrics_by_name,
        )

    committed_result_by_name = context.get(_NPI_CONTROL_COMMITTED_RESULT_KEY)
    if type(committed_result_by_name) is not dict:
        raise NPIPrerequisiteError("NPI publication result is invalid")
    _print_time_info_best_effort(ctx['context']['start'])
    return committed_result_by_name


async def shutdown(ctx):  # pragma: no cover
    """Run the NPI worker shutdown entry point."""
    return await finalize_npi_import_attempt(ctx)


async def save_npi_data(ctx, task):
    """Persist one normalized NPI payload into its staging models."""
    import_date = ctx['import_date']
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    write_tasks = []
    for key in task:
        match key:
            case 'npi_obj_list':
                mynpidata = make_class(NPIData, import_date)
                write_tasks.append(push_objects(task['npi_obj_list'], mynpidata, rewrite=True))
            case 'npi_taxonomy_list':
                mynpidatataxonomy = make_class(NPIDataTaxonomy, import_date)
                write_tasks.append(push_objects(task['npi_taxonomy_list'], mynpidatataxonomy, rewrite=True))
            case 'npi_other_id_list':
                mynpidataotheridentifier = make_class(NPIDataOtherIdentifier, import_date)
                unique_rows = list({item['checksum']: item for item in task['npi_other_id_list']}.values())
                write_tasks.append(push_objects(unique_rows, mynpidataotheridentifier))
            case 'npi_taxonomy_group_list':
                mynpidatataxonomygroup = make_class(NPIDataTaxonomyGroup, import_date)
                write_tasks.append(push_objects(task['npi_taxonomy_group_list'], mynpidatataxonomygroup, rewrite=True))
            case 'npi_address_list':
                mynpiaddress = make_class(NPIAddress, import_date)
                write_tasks.append(push_objects(task['npi_address_list'], mynpiaddress, rewrite=True))
            case _:
                print('Some wrong key passed')
    for coro in write_tasks:
        await coro


async def main(test_mode: bool = False):  # pragma: no cover
    """Create and enqueue one controlled NPI import run."""
    if test_mode:
        raise ValueError(
            "NPI test mode requires an isolated database and cannot use the live queue"
        )
    from api.control_imports import create_import_run, ensure_import_run_table

    await ensure_import_run_table()
    run, _ = await create_import_run({
        "importer": "npi",
        "params": {},
        "triggered_by": "manual",
    })
    return run
