# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical address identity helpers.

PostgreSQL functions are the set-based identity producer for live staging
tables. This Python implementation is also production code for migration and
large-batch materialization paths; frozen corpus parity tests and runtime stamp
guards keep it byte-identical to the SQL functions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

from sqlalchemy import text

from db.models import db
from process.ext.address_pub28 import (
    PUB28_DIRECTIONAL_MAP,
    PUB28_INVALID_UNIT_VALUES,
    PUB28_STATE_MAP,
    PUB28_STREET_SUFFIX_MAP,
    PUB28_UNIT_DESIGNATOR_MAP,
    PUB28_UNIT_NO_RANGE,
)
from process.live_progress import enqueue_live_progress
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary


logger = logging.getLogger(__name__)

ADDRESS_CANON_RUST_MATERIALIZE_ENV = "HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE"
CURRENT_ADDRESS_IDENTITY_VERSION = 2
CURRENT_ADDRESS_IDENTITY_PREFIX = f"v{CURRENT_ADDRESS_IDENTITY_VERSION}"
_RUST_CANON_VERSION_CACHE: dict[str, bool] = {}


UNIT_DESIGNATOR_PATTERN = "|".join(
    re.escape(value)
    for value in sorted(PUB28_UNIT_DESIGNATOR_MAP, key=len, reverse=True)
)
UNIT_RE = re.compile(
    rf"^\s*({UNIT_DESIGNATOR_PATTERN})\.?\s*(?:#\s*)?([a-z0-9][a-z0-9-]*)?[\.,;:]*\s*$",
    re.IGNORECASE,
)
UNIT_TAIL_RE = re.compile(
    rf"(^|[\s,])({UNIT_DESIGNATOR_PATTERN})\.?\s*(?:#\s*)?([a-z0-9][a-z0-9-]*)?[\.,;:]*\s*$",
    re.IGNORECASE,
)
NUMERIC_ORDINAL_RE = re.compile(r"^0*([1-9][0-9]*)(?:st|nd|rd|th)$")
ORDINAL_H_TYPO_RE = re.compile(r"^0*([1-9][0-9]*)h$")
ORDINAL_WORD_MAP = {
    "first": "1",
    "second": "2",
    "third": "3",
    "fourth": "4",
    "fifth": "5",
    "sixth": "6",
    "seventh": "7",
    "eighth": "8",
    "ninth": "9",
    "tenth": "10",
    "eleventh": "11",
    "twelfth": "12",
    "thirteenth": "13",
    "fourteenth": "14",
    "fifteenth": "15",
    "sixteenth": "16",
    "seventeenth": "17",
    "eighteenth": "18",
    "nineteenth": "19",
    "twentieth": "20",
    "thirtieth": "30",
}
STREET_SUFFIX_TOKENS = frozenset(PUB28_STREET_SUFFIX_MAP)
DIRECTIONAL_TOKENS = frozenset(PUB28_DIRECTIONAL_MAP)


@dataclass(frozen=True)
class ResolveStats:
    staged: int
    distinct_keys: int
    inserted: int
    provenance_updates: int
    null_key_rows: int
    eligible_key_rows: int = 0
    eligible_null_key_rows: int = 0
    reason_buckets: dict[str, int] = field(default_factory=dict)
    gate_violations: tuple[str, ...] = ()
    gate_sample_rows: list[dict[str, Any]] = field(default_factory=list)
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class ArchiveV2MigrationStats:
    legacy_rows: int
    keyable_rows: int
    non_keyable_rows: int
    upserted_rows: int
    inserted_rows: int
    updated_rows: int
    checksum_map_rows: int
    checksum_collision_rows: int
    checksum_collision_checksums: int
    represented_missing_keys: int
    legacy_geocoded_keys: int
    geocoded_missing_keys: int
    runtime_seconds: float
    dry_run: bool = False
    sample_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ArchiveSwapStats:
    current_table: str
    archive_table: str
    backup_table: str
    legacy_rows_before: int
    legacy_rows_after: int
    legacy_checksum_min: int | None
    legacy_checksum_max: int | None
    legacy_checksum_sum: str | None
    archive_rows_before: int
    current_rows_after: int
    checksum_map_rows: int
    checksum_collision_rows: int
    missing_map_targets: int
    swapped: bool
    runtime_seconds: float
    dry_run: bool = False


@dataclass(frozen=True)
class _UnitDecision:
    unit: str
    street_text: str


def _unit_prefix(value: str | None) -> str | None:
    raw = (value or "").strip().lower()
    if raw == "#":
        return "ste"
    cleaned = re.sub(r"[^a-z0-9]", "", raw)
    return PUB28_UNIT_DESIGNATOR_MAP.get(cleaned)


def _valid_unit_value(value: str | None) -> bool:
    cleaned = re.sub(r"[^a-z0-9]", "", (value or "").lower())
    return bool(cleaned and cleaned not in PUB28_INVALID_UNIT_VALUES)


def _unit_from_match(match: re.Match[str], prefix_group: int, value_group: int) -> str:
    prefix = _unit_prefix(match.group(prefix_group))
    unit_value = match.group(value_group) or ""
    if not prefix:
        return ""
    cleaned = re.sub(r"[^a-z0-9]", "", unit_value.lower())
    if cleaned:
        if not _valid_unit_value(cleaned):
            return ""
        return f"{prefix}{cleaned}"
    if prefix in PUB28_UNIT_NO_RANGE:
        return prefix
    return ""


def _unit_decision(line1: str | None, line2: str | None) -> _UnitDecision:
    l1 = (line1 or "").lower()
    l2 = (line2 or "").lower()
    line2_match = UNIT_RE.match(l2)
    if line2_match:
        unit = _unit_from_match(line2_match, 1, 2)
        if unit:
            return _UnitDecision(unit=unit, street_text=f" {l1} ")
        return _UnitDecision(unit="", street_text=f" {l1} {l2} ")

    joined = f" {l1} {l2} "
    tail = UNIT_TAIL_RE.search(joined)
    if tail:
        unit = _unit_from_match(tail, 2, 3)
        if unit and (tail.group(3) or not l2.strip()):
            return _UnitDecision(unit=unit, street_text=joined[: tail.start()])
    return _UnitDecision(unit="", street_text=joined)


def _street_token_norm(value: str) -> str:
    token = re.sub(r"[^a-z0-9]", "", value.lower())
    if not token:
        return ""
    ordinal = NUMERIC_ORDINAL_RE.match(token)
    if ordinal:
        return ordinal.group(1)
    if token == "saint":
        return "st"
    mapped = PUB28_DIRECTIONAL_MAP.get(token) or PUB28_STREET_SUFFIX_MAP.get(token) or token
    return PUB28_DIRECTIONAL_MAP.get(mapped) or PUB28_STREET_SUFFIX_MAP.get(mapped) or mapped


def _street_token_is_suffix(value: str | None) -> bool:
    token = re.sub(r"[^a-z0-9]", "", (value or "").lower())
    return token in STREET_SUFFIX_TOKENS


def _street_token_is_directional(value: str | None) -> bool:
    token = re.sub(r"[^a-z0-9]", "", (value or "").lower())
    return token in DIRECTIONAL_TOKENS


def _street_token_norm_context(token: str, index: int, tokens: list[str]) -> str:
    cleaned = re.sub(r"[^a-z0-9]", "", token.lower())
    if not cleaned:
        return ""
    next_token = tokens[index + 1] if index + 1 < len(tokens) else ""
    if _street_token_is_suffix(next_token):
        if cleaned in ORDINAL_WORD_MAP:
            return ORDINAL_WORD_MAP[cleaned]
        ordinal_typo = ORDINAL_H_TYPO_RE.match(cleaned)
        if ordinal_typo:
            return ordinal_typo.group(1)
    return _street_token_norm(cleaned)


def _street_raw_text(line1: str | None, line2: str | None) -> str:
    raw = _unit_decision(line1, line2).street_text
    raw = re.sub(r"\bp\s*\.?\s*o\s*\.?\s*box\b", " pobox ", raw, flags=re.IGNORECASE)
    return re.sub(r"\bpob\b", " pobox ", raw, flags=re.IGNORECASE)


def _street_tokens(line1: str | None, line2: str | None) -> list[str]:
    return re.findall(r"[a-z0-9]+", _street_raw_text(line1, line2).lower())


def _normalized_street_tokens(tokens: list[str]) -> list[str]:
    return [_street_token_norm_context(token, index, tokens) for index, token in enumerate(tokens)]


def _edge_direction_index(tokens: list[str]) -> int | None:
    if tokens and _street_token_is_directional(tokens[0]):
        return 0
    if (
        len(tokens) >= 2
        and re.match(r"^[0-9]+[a-z]?$", re.sub(r"[^a-z0-9]", "", tokens[0].lower()))
        and _street_token_is_directional(tokens[1])
    ):
        return 1
    if tokens and _street_token_is_directional(tokens[-1]):
        return len(tokens) - 1
    return None


def city_norm(value: str | None) -> str | None:
    cleaned = re.sub(r"[^a-z0-9]", "", (value or "").lower())
    return cleaned or None


def state_code(value: str | None) -> str | None:
    cleaned = re.sub(r"[^A-Za-z]", "", value or "").upper()
    if not cleaned:
        return None
    return PUB28_STATE_MAP.get(cleaned)


def zip5_norm(value: str | None) -> str | None:
    digits = re.sub(r"[^0-9]", "", value or "")
    if len(digits) >= 5:
        return digits[:5]
    if len(digits) in {3, 4}:
        return digits.zfill(5)
    return None


def country_code(value: str | None) -> str:
    cleaned = re.sub(r"[^A-Za-z]", "", value or "").upper()
    if not cleaned or cleaned in {"US", "USA", "UNITEDSTATES", "UNITEDSTATESOFAMERICA"}:
        return "US"
    return cleaned


def _clamp_text(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    return value[:limit]


def unit_norm(line1: str | None, line2: str | None) -> str:
    return _unit_decision(line1, line2).unit


def street_norm(line1: str | None, line2: str | None) -> str | None:
    tokens = _street_tokens(line1, line2)
    cleaned = "".join(_normalized_street_tokens(tokens))
    return cleaned or None


def street_suffix_token(line1: str | None, line2: str | None) -> str | None:
    tokens = _street_tokens(line1, line2)
    if len(tokens) < 2 or not _street_token_is_suffix(tokens[-1]):
        return None
    return _street_token_norm(tokens[-1])


def street_suffixless_norm(line1: str | None, line2: str | None) -> str | None:
    tokens = _street_tokens(line1, line2)
    stop = len(tokens)
    if len(tokens) >= 2 and _street_token_is_suffix(tokens[-1]):
        stop -= 1
    cleaned = "".join(
        _street_token_norm_context(token, index, tokens)
        for index, token in enumerate(tokens[:stop])
    )
    return cleaned or None


def street_direction_token(line1: str | None, line2: str | None) -> str | None:
    tokens = _street_tokens(line1, line2)
    direction_index = _edge_direction_index(tokens)
    if direction_index is None:
        return None
    return _street_token_norm(tokens[direction_index])


def street_directionless_norm(line1: str | None, line2: str | None) -> str | None:
    tokens = _street_tokens(line1, line2)
    direction_index = _edge_direction_index(tokens)
    retained = [
        token
        for index, token in enumerate(tokens)
        if index != direction_index
    ]
    cleaned = "".join(
        _street_token_norm_context(token, index, retained)
        for index, token in enumerate(retained)
    )
    return cleaned or None


def street_completion_norm(line1: str | None, line2: str | None) -> str | None:
    tokens = _street_tokens(line1, line2)
    direction_index = _edge_direction_index(tokens)
    drop_indexes = {direction_index} if direction_index is not None else set()
    retained_indexes = [index for index in range(len(tokens)) if index not in drop_indexes]
    if len(retained_indexes) >= 2 and _street_token_is_suffix(tokens[retained_indexes[-1]]):
        drop_indexes.add(retained_indexes[-1])
    normalized = _normalized_street_tokens(tokens)
    cleaned = "".join(token for index, token in enumerate(normalized) if index not in drop_indexes)
    return cleaned or None


def identity_key_v1(
    first_line: str | None,
    second_line: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
    country: str | None = "US",
) -> str | None:
    street = street_norm(first_line, second_line)
    unit = unit_norm(first_line, second_line)
    city_value = city_norm(city)
    state_value = state_code(state)
    zip_value = zip5_norm(zip_code)
    country_value = country_code(country)
    if country_value != "US" or not state_value or not zip_value:
        return None
    if street:
        precision = "street"
        identity_city = ""
    elif city_value:
        precision = "city_zip"
        identity_city = city_value
    else:
        return None
    return "|".join([
        CURRENT_ADDRESS_IDENTITY_PREFIX,
        street or "",
        unit,
        identity_city,
        state_value,
        zip_value,
        country_value,
        precision,
    ])


def premise_identity_key_v1(
    first_line: str | None,
    second_line: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
    country: str | None = "US",
) -> str | None:
    street = street_norm(first_line, second_line)
    state_value = state_code(state)
    zip_value = zip5_norm(zip_code)
    country_value = country_code(country)
    if country_value != "US" or not street or not state_value or not zip_value:
        return None
    return "|".join([
        CURRENT_ADDRESS_IDENTITY_PREFIX,
        street,
        "",
        "",
        state_value,
        zip_value,
        country_value,
        "street",
    ])


def key_from_identity(identity: str | None) -> uuid.UUID | None:
    if identity is None:
        return None
    return uuid.UUID(bytes=hashlib.sha256(identity.encode("utf-8")).digest()[:16])


def address_key_v1(
    first_line: str | None,
    second_line: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
    country: str | None = "US",
) -> uuid.UUID | None:
    return key_from_identity(identity_key_v1(first_line, second_line, city, state, zip_code, country))


def source_enabled(source_name: str) -> bool:
    raw = os.getenv("HLTHPRT_ADDRESS_CANON_SOURCES", "").strip()
    if not raw:
        return False
    enabled = {part.strip().lower() for part in raw.split(",") if part.strip()}
    return "all" in enabled or source_name.lower() in enabled


def archive_table_name() -> str:
    value = os.getenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2").strip()
    return value or "address_archive_v2"


def _schema_name() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quote_ident(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value or ""):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def _qtable(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _setting_value(value: str) -> str:
    if not re.match(r"^[A-Za-z0-9_. -]+$", value or ""):
        raise ValueError(f"Unsafe PostgreSQL setting value: {value!r}")
    return value.replace("'", "''")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def pub28_source_sha256() -> str:
    return hashlib.sha256(Path(__file__).with_name("address_pub28.py").read_bytes()).hexdigest()


def current_canon_version() -> dict[str, Any]:
    return {
        "identity_version": CURRENT_ADDRESS_IDENTITY_VERSION,
        "identity_prefix": CURRENT_ADDRESS_IDENTITY_PREFIX,
        "pub28_sha256": pub28_source_sha256(),
    }


def _archive_lock_key(schema: str, table: str, purpose: str) -> str:
    return f"address_archive:{schema}:{table}:{purpose}"


def _expr(fields: Mapping[str, str], key: str, fallback: str = "NULL") -> str:
    return fields.get(key) or fallback


def _address_sql(schema: str, fields: Mapping[str, str]) -> tuple[str, str, str, str, str, str]:
    first = _expr(fields, "first_line")
    second = _expr(fields, "second_line")
    city = _expr(fields, "city")
    state = _expr(fields, "state")
    zip_code = _expr(fields, "zip")
    country = _expr(fields, "country", "'US'")
    return first, second, city, state, zip_code, country


def _key_from_identity_sql(schema: str, identity_expr: str) -> str:
    return f"{_quote_ident(schema)}.addr_key_from_identity_v1({identity_expr})"


KEYED_COPY_COLUMNS = (
    "rn",
    "source_ctid",
    "staged_address_key",
    "address_key",
    "computed_address_key",
    "identity_key",
    "premise_key",
    "line1_norm",
    "unit_norm",
    "city_norm",
    "state_code",
    "zip5",
    "zip4",
    "country_code",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
)


def _keyed_temp_table_ddl(keyed_table: str) -> str:
    return f"""
        CREATE TEMP TABLE {keyed_table} (
            rn bigint,
            source_ctid text,
            staged_address_key uuid,
            address_key uuid,
            computed_address_key uuid,
            identity_key text,
            premise_key uuid,
            line1_norm text,
            unit_norm text,
            city_norm text,
            state_code text,
            zip5 text,
            zip4 text,
            country_code text,
            first_line text,
            second_line text,
            city_name text,
            state_name text,
            postal_code text
        ) ON COMMIT DROP;
    """


def _keyed_raw_copy_sql(
    *,
    staging: str,
    first: str,
    second: str,
    city: str,
    state: str,
    zip_code: str,
    country: str,
) -> str:
    return f"""
        WITH raw AS (
            SELECT
                row_number() OVER (ORDER BY ctid) AS rn,
                ctid::text AS source_ctid,
                address_key AS staged_address_key,
                NULLIF(trim(COALESCE({first}, '')), '') AS first_line,
                NULLIF(trim(COALESCE({second}, '')), '') AS second_line,
                NULLIF(trim(COALESCE({city}, '')), '') AS city_name,
                NULLIF(trim(COALESCE({state}, '')), '') AS state_name,
                NULLIF(trim(COALESCE({zip_code}, '')), '') AS postal_code,
                {country} AS raw_country_code
            FROM {staging}
        ),
        ranked_stamped AS (
            SELECT
                raw.*,
                row_number() OVER (
                    PARTITION BY staged_address_key
                    ORDER BY
                        first_line IS NULL,
                        length(COALESCE(first_line, '')) DESC,
                        second_line IS NULL,
                        length(COALESCE(second_line, '')) DESC,
                        city_name IS NULL,
                        length(COALESCE(city_name, '')) DESC,
                        COALESCE(first_line, ''),
                        COALESCE(second_line, ''),
                        COALESCE(city_name, ''),
                        COALESCE(state_name, ''),
                        COALESCE(postal_code, ''),
                        rn
                ) AS key_rank
            FROM raw
            WHERE staged_address_key IS NOT NULL
        ),
        raw_to_normalize AS (
            SELECT
                rn,
                source_ctid,
                staged_address_key,
                first_line,
                second_line,
                city_name,
                state_name,
                postal_code,
                raw_country_code
            FROM ranked_stamped
            WHERE key_rank = 1
            UNION ALL
            SELECT
                rn,
                source_ctid,
                staged_address_key,
                first_line,
                second_line,
                city_name,
                state_name,
                postal_code,
                raw_country_code
            FROM raw
            WHERE staged_address_key IS NULL
        )
        SELECT
            rn,
            source_ctid,
            staged_address_key,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            raw_country_code
        FROM raw_to_normalize
    """


def _canon_version_matches(payload: Mapping[str, Any]) -> bool:
    expected = current_canon_version()
    return (
        int(payload.get("identity_version") or 0) == expected["identity_version"]
        and str(payload.get("identity_prefix") or "") == expected["identity_prefix"]
        and str(payload.get("pub28_sha256") or "") == expected["pub28_sha256"]
    )


async def _rust_canon_version(binary: Path) -> dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--canon-version",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        stderr_text = stderr.decode("utf-8", errors="replace")[-1000:]
        stdout_text = stdout.decode("utf-8", errors="replace")[-1000:]
        raise RuntimeError(
            f"Rust address canonicalizer version check failed with exit code {process.returncode}: "
            f"{stderr_text or stdout_text}"
        )
    return json.loads(stdout.decode("utf-8"))


async def _rust_canon_version_is_current(binary: Path) -> bool:
    cache_key = str(binary.resolve())
    cached = _RUST_CANON_VERSION_CACHE.get(cache_key)
    if cached is not None:
        return cached
    try:
        payload = await _rust_canon_version(binary)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("Rust address canonicalizer version check failed; falling back to SQL: %s", exc)
        _RUST_CANON_VERSION_CACHE[cache_key] = False
        return False
    ok = _canon_version_matches(payload)
    if not ok:
        logger.warning(
            "Rust address canonicalizer version mismatch; falling back to SQL "
            "(rust=%s python=%s)",
            payload,
            current_canon_version(),
        )
    _RUST_CANON_VERSION_CACHE[cache_key] = ok
    return ok


async def _run_rust_address_canonicalizer(
    input_path: Path,
    output_path: Path,
    *,
    binary: Path | None = None,
) -> None:
    binary = binary or _ptg2_rust_scanner_binary()
    if binary is None:
        raise FileNotFoundError("ptg2_scanner binary was not found")
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--address-canonicalize-copy",
        str(input_path),
        str(output_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        stderr_text = stderr.decode("utf-8", errors="replace")[-2000:]
        stdout_text = stdout.decode("utf-8", errors="replace")[-1000:]
        raise RuntimeError(
            "Rust address canonicalizer failed with exit code "
            f"{process.returncode}: {stderr_text or stdout_text}"
        )


async def _try_materialize_keyed_with_rust(
    session: Any,
    *,
    keyed_table: str,
    keyed_table_name: str,
    raw_copy_sql: str,
) -> bool:
    if not _env_bool(ADDRESS_CANON_RUST_MATERIALIZE_ENV, default=True):
        return False
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        logger.warning(
            "%s is enabled but ptg2_scanner was not found; falling back to SQL address materialization",
            ADDRESS_CANON_RUST_MATERIALIZE_ENV,
        )
        return False
    if not await _rust_canon_version_is_current(binary):
        return False

    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    driver_connection = getattr(raw_connection, "driver_connection", raw_connection)
    copy_from_query = getattr(driver_connection, "copy_from_query", None)
    copy_to_table = getattr(driver_connection, "copy_to_table", None)
    if copy_from_query is None or copy_to_table is None:
        logger.warning(
            "%s is enabled but the active database driver lacks COPY support; "
            "falling back to SQL address materialization",
            ADDRESS_CANON_RUST_MATERIALIZE_ENV,
        )
        return False

    with tempfile.TemporaryDirectory(prefix="address-canon-rust-") as tmpdir:
        input_path = Path(tmpdir) / "raw.copy"
        output_path = Path(tmpdir) / "keyed.copy"
        with input_path.open("wb") as copy_output:
            await copy_from_query(
                raw_copy_sql,
                output=copy_output,
                format="text",
                delimiter="\t",
                null="\\N",
            )
        await _run_rust_address_canonicalizer(input_path, output_path, binary=binary)
        await session.execute(text(_keyed_temp_table_ddl(keyed_table)))
        with output_path.open("rb") as copy_input:
            await copy_to_table(
                keyed_table_name,
                source=copy_input,
                columns=KEYED_COPY_COLUMNS,
                format="text",
                delimiter="\t",
                null="\\N",
            )
    return True


def _emit_progress(**payload: Any) -> None:
    try:
        enqueue_live_progress(source="address-canonical", **payload)
    except Exception:
        return


def _keyable_null_address_key_exists_sql(
    *,
    schema: str,
    table: str,
    first: str,
    second: str,
    city: str,
    state: str,
    zip_code: str,
    country: str,
) -> str:
    return f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_qtable(schema, table)}
             WHERE address_key IS NULL
               AND {_quote_ident(schema)}.addr_key_v1(
                    {first}, {second}, {city}, {state}, {zip_code}, {country}
               ) IS NOT NULL
             LIMIT 1
        );
    """


def _resolve_gate_violations(
    *,
    staged: int,
    distinct_keys: int,
    inserted: int,
    eligible_null_key_rows: int,
) -> tuple[str, ...]:
    violations: list[str] = []
    if eligible_null_key_rows:
        violations.append("eligible_rows_missing_address_key")
    if distinct_keys > staged:
        violations.append("distinct_keys_exceed_staged_rows")
    if inserted > distinct_keys:
        violations.append("inserted_rows_exceed_distinct_keys")
    return tuple(violations)


async def stamp_address_keys(
    staging_table: str,
    field_map: Mapping[str, str],
    *,
    schema: str | None = None,
    shards: int = 8,
    cancel_check: Callable[[], Awaitable[None]] | None = None,
    update_existing: bool = True,
    honor_env_override: bool = True,
) -> int:
    schema = schema or _schema_name()
    override_shards = os.getenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS")
    if honor_env_override and override_shards:
        shards = int(override_shards)
        if shards <= 0:
            raise ValueError("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS must be a positive integer")
    shards = max(int(shards or 1), 1)
    stamp_concurrency = int(os.getenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "1"))
    if stamp_concurrency <= 0:
        raise ValueError("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY must be a positive integer")
    db_pool_max = int(os.getenv("HLTHPRT_DB_POOL_MAX_SIZE", "5"))
    if db_pool_max <= 0:
        raise ValueError("HLTHPRT_DB_POOL_MAX_SIZE must be a positive integer")
    stamp_concurrency = min(stamp_concurrency, shards, db_pool_max)
    first, second, city, state, zip_code, country = _address_sql(schema, field_map)
    total = 0
    completed = 0
    semaphore = asyncio.Semaphore(stamp_concurrency)
    progress_lock = asyncio.Lock()
    _emit_progress(
        phase="address key stamping",
        unit="shard",
        total=shards,
        done=0,
        pct=0,
        message="stamping canonical address keys",
    )
    if not update_existing:
        has_keyable_null_address_key = bool(
            await db.scalar(
                _keyable_null_address_key_exists_sql(
                    schema=schema,
                    table=staging_table,
                    first=first,
                    second=second,
                    city=city,
                    state=state,
                    zip_code=zip_code,
                    country=country,
                )
            )
        )
        if not has_keyable_null_address_key:
            _emit_progress(
                phase="address key stamping",
                unit="shard",
                total=shards,
                done=shards,
                pct=100,
                message="no keyable address rows need stamping",
            )
            return 0

    async def _stamp_shard(shard: int) -> None:
        nonlocal completed, total
        async with semaphore:
            if cancel_check:
                await cancel_check()
            source_filters = []
            if not update_existing:
                source_filters.append("address_key IS NULL")
            if shards > 1:
                source_filters.append(
                    "mod(abs(hashtext(ctid::text)::bigint), :shards) = :shard"
                )
            source_filter = f"WHERE {' AND '.join(source_filters)}" if source_filters else ""
            update_filter = (
                "target.address_key IS DISTINCT FROM stamped.computed_address_key"
                if update_existing
                else "target.address_key IS NULL AND stamped.computed_address_key IS NOT NULL"
            )
            rowcount = await db.status(
                f"""
                WITH stamped AS (
                    SELECT ctid,
                           {_quote_ident(schema)}.addr_key_v1(
                                {first}, {second}, {city}, {state}, {zip_code}, {country}
                           ) AS computed_address_key
                      FROM {_qtable(schema, staging_table)}
                     {source_filter}
                )
                UPDATE {_qtable(schema, staging_table)} AS target
                   SET address_key = stamped.computed_address_key
                  FROM stamped
                 WHERE target.ctid = stamped.ctid
                   AND {update_filter};
                """,
                shards=shards,
                shard=shard,
            )
            async with progress_lock:
                total += int(rowcount or 0)
                completed += 1
                _emit_progress(
                    phase="address key stamping",
                    unit="shard",
                    total=shards,
                    done=completed,
                    pct=(completed / shards) * 100.0,
                    message="stamping canonical address keys",
                )
            await asyncio.sleep(0)

    await asyncio.gather(*(_stamp_shard(shard) for shard in range(shards)))
    return total


async def propagate_child_address_keys(
    child_table: str,
    parent_table: str,
    *,
    schema: str | None = None,
    shards: int = 8,
    cancel_check: Callable[[], Awaitable[None]] | None = None,
    skip_when_child_fully_keyed: bool = False,
) -> int:
    """Copy address keys from an aggregate address table to child evidence rows."""
    schema = schema or _schema_name()
    override_shards = os.getenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS")
    if override_shards:
        shards = int(override_shards)
        if shards <= 0:
            raise ValueError("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS must be a positive integer")
    shards = max(int(shards or 1), 1)
    stamp_concurrency = int(os.getenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "1"))
    if stamp_concurrency <= 0:
        raise ValueError("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY must be a positive integer")
    stamp_concurrency = min(stamp_concurrency, shards)
    total = 0
    completed = 0
    semaphore = asyncio.Semaphore(stamp_concurrency)
    progress_lock = asyncio.Lock()
    same_address = """
                   child.first_line IS NOT DISTINCT FROM parent.first_line
               AND child.second_line IS NOT DISTINCT FROM parent.second_line
               AND child.city_name IS NOT DISTINCT FROM parent.city_name
               AND child.state_name IS NOT DISTINCT FROM parent.state_name
               AND child.postal_code IS NOT DISTINCT FROM parent.postal_code
               AND COALESCE(NULLIF(child.country_code, ''), 'US')
                   IS NOT DISTINCT FROM COALESCE(NULLIF(parent.country_code, ''), 'US')
    """
    _emit_progress(
        phase="address key propagation",
        unit="shard",
        total=shards,
        done=0,
        pct=0,
        message="propagating canonical address keys",
    )
    child = _qtable(schema, child_table)
    parent = _qtable(schema, parent_table)
    if skip_when_child_fully_keyed:
        has_child_key_to_propagate = bool(
            await db.scalar(
                f"""
                SELECT EXISTS (
                    SELECT 1
                      FROM {child} AS child
                      JOIN {parent} AS parent
                        ON child.npi = parent.npi
                       AND child.type = parent.type
                       AND child.checksum = parent.checksum
                     WHERE child.address_key IS NULL
                       AND parent.address_key IS NOT NULL
                       AND ({same_address})
                     LIMIT 1
                );
                """
            )
        )
        if not has_child_key_to_propagate:
            _emit_progress(
                phase="address key propagation",
                unit="shard",
                total=shards,
                done=shards,
                pct=100,
                message="no child canonical address keys need propagation",
            )
            return 0

    async def _propagate_shard(shard: int) -> None:
        nonlocal completed, total
        async with semaphore:
            if cancel_check:
                await cancel_check()
            shard_filter = ""
            if shards > 1:
                shard_filter = (
                    "AND mod(abs(hashtext(child.ctid::text)::bigint), :shards) = :shard"
                )
            pending_table = _quote_ident("address_key_propagation_pending")
            params = {"shards": shards, "shard": shard}
            async with db.transaction() as session:
                await session.execute(text(f"DROP TABLE IF EXISTS pg_temp.{pending_table};"))
                await session.execute(
                    text(
                        f"""
                        CREATE TEMP TABLE {pending_table} ON COMMIT DROP AS
                        SELECT child.ctid AS child_ctid,
                               NULL::uuid AS address_key,
                               TRUE AS clear_key
                          FROM {child} AS child
                          JOIN {parent} AS parent
                            ON child.npi = parent.npi
                           AND child.type = parent.type
                           AND child.checksum = parent.checksum
                         WHERE child.address_key IS NOT NULL
                           AND NOT ({same_address})
                         {shard_filter}
                        UNION ALL
                        SELECT child.ctid AS child_ctid,
                               parent.address_key AS address_key,
                               FALSE AS clear_key
                          FROM {child} AS child
                          JOIN {parent} AS parent
                            ON child.npi = parent.npi
                           AND child.type = parent.type
                           AND child.checksum = parent.checksum
                         WHERE parent.address_key IS NOT NULL
                           AND (
                                child.address_key IS NULL
                                OR child.address_key IS DISTINCT FROM parent.address_key
                           )
                           AND {same_address}
                         {shard_filter};
                        """
                    ),
                    params,
                )
                await session.execute(text(f"CREATE INDEX ON {pending_table} (child_ctid);"))
                cleared = (
                    await session.execute(
                        text(
                            f"""
                            WITH cleared AS (
                                UPDATE {child} AS child
                                   SET address_key = NULL
                                  FROM {pending_table} AS pending
                                 WHERE pending.clear_key
                                   AND child.ctid = pending.child_ctid
                                RETURNING 1
                            )
                            SELECT count(*) FROM cleared;
                            """
                        )
                    )
                ).scalar()
                propagated = (
                    await session.execute(
                        text(
                            f"""
                            WITH propagated AS (
                                UPDATE {child} AS child
                                   SET address_key = pending.address_key
                                  FROM {pending_table} AS pending
                                 WHERE NOT pending.clear_key
                                   AND pending.address_key IS NOT NULL
                                   AND child.ctid = pending.child_ctid
                                RETURNING 1
                            )
                            SELECT count(*) FROM propagated;
                            """
                        )
                    )
                ).scalar()
            async with progress_lock:
                total += int(cleared or 0) + int(propagated or 0)
                completed += 1
                _emit_progress(
                    phase="address key propagation",
                    unit="shard",
                    total=shards,
                    done=completed,
                    pct=(completed / shards) * 100.0,
                    message="propagating canonical address keys",
                )
            await asyncio.sleep(0)

    await asyncio.gather(*(_propagate_shard(shard) for shard in range(shards)))
    return total


async def _table_exists_in_session(session: Any, schema: str, table: str) -> bool:
    value = (
        await session.execute(
            text("SELECT to_regclass(:table_name);"),
            {"table_name": f"{schema}.{table}"},
        )
    ).scalar()
    return bool(value)


async def _table_has_column_in_session(session: Any, schema: str, table: str, column: str) -> bool:
    return bool(
        (
            await session.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                          FROM information_schema.columns
                         WHERE table_schema = :schema
                           AND table_name = :table
                           AND column_name = :column
                    );
                    """
                ),
                {"schema": schema, "table": table, "column": column},
            )
        ).scalar()
    )


async def _select_canonical_archive_table(schema: str, requested: str) -> str:
    async with db.transaction() as session:
        if await _table_exists_in_session(session, schema, requested) and await _table_has_column_in_session(
            session, schema, requested, "address_key"
        ):
            return requested
    return requested


def _completion_alias_sql(
    *,
    schema: str,
    keyed_table: str,
    archive: str,
) -> str:
    qschema = _quote_ident(schema)
    return f"""
        CREATE TEMP TABLE address_completion_aliases ON COMMIT DROP AS
        WITH source_rows AS MATERIALIZED (
            SELECT
                rn,
                source_ctid,
                address_key AS source_address_key,
                COALESCE(unit_norm, '') AS unit_norm,
                state_code,
                zip5,
                COALESCE(country_code, 'US') AS country_code,
                {qschema}.addr_street_completion_norm_v1(first_line, second_line) AS completion_norm,
                {qschema}.addr_street_suffix_token_v1(first_line, second_line) AS suffix_token,
                {qschema}.addr_street_direction_token_v1(first_line, second_line) AS direction_token
            FROM {keyed_table}
            WHERE address_key IS NOT NULL
              AND identity_key IS NOT NULL
              AND split_part(identity_key, '|', 8) = 'street'
              AND state_code IS NOT NULL
              AND zip5 IS NOT NULL
        ),
        source_ranked AS MATERIALIZED (
            SELECT
                *,
                CASE WHEN suffix_token IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN direction_token IS NOT NULL THEN 1 ELSE 0 END AS completion_rank
            FROM source_rows
            WHERE completion_norm IS NOT NULL
              AND (suffix_token IS NULL OR direction_token IS NULL)
        ),
        source_keys AS MATERIALIZED (
            SELECT DISTINCT completion_norm, unit_norm, state_code, zip5, country_code
            FROM source_ranked
        ),
        archive_key_scope AS MATERIALIZED (
            SELECT DISTINCT unit_norm, state_code, zip5, country_code
            FROM source_keys
        ),
        archive_prefilter AS MATERIALIZED (
            SELECT
                archived.address_key,
                archived.identity_key,
                archived.premise_key,
                archived.line1_norm,
                archived.unit_norm,
                archived.city_norm,
                archived.state_code,
                archived.zip5,
                archived.zip4,
                archived.country_code,
                archived.first_line,
                archived.second_line,
                archived.city_name,
                archived.state_name,
                archived.postal_code
            FROM {archive} AS archived
            JOIN archive_key_scope
              ON COALESCE(archived.unit_norm, '') = archive_key_scope.unit_norm
             AND archived.state_code = archive_key_scope.state_code
             AND archived.zip5 = archive_key_scope.zip5
             AND COALESCE(archived.country_code, 'US') = archive_key_scope.country_code
            WHERE archived.address_key IS NOT NULL
              AND archived.identity_key IS NOT NULL
              AND COALESCE(archived.precision, split_part(archived.identity_key, '|', 8)) = 'street'
              AND archived.merged_into IS NULL
              AND archived.state_code IS NOT NULL
              AND archived.zip5 IS NOT NULL
        ),
        target_rows AS MATERIALIZED (
            SELECT
                0 AS target_source_rank,
                address_key AS target_address_key,
                identity_key AS target_identity_key,
                premise_key AS target_premise_key,
                line1_norm AS target_line1_norm,
                COALESCE(unit_norm, '') AS target_unit_norm,
                city_norm AS target_city_norm,
                state_code AS target_state_code,
                zip5 AS target_zip5,
                zip4 AS target_zip4,
                COALESCE(country_code, 'US') AS target_country_code,
                first_line AS target_first_line,
                second_line AS target_second_line,
                city_name AS target_city_name,
                state_name AS target_state_name,
                postal_code AS target_postal_code,
                COALESCE(unit_norm, '') AS unit_norm,
                state_code,
                zip5,
                COALESCE(country_code, 'US') AS country_code,
                {qschema}.addr_street_completion_norm_v1(first_line, second_line) AS completion_norm,
                {qschema}.addr_street_suffix_token_v1(first_line, second_line) AS suffix_token,
                {qschema}.addr_street_direction_token_v1(first_line, second_line) AS direction_token
            FROM {keyed_table}
            WHERE address_key IS NOT NULL
              AND identity_key IS NOT NULL
              AND split_part(identity_key, '|', 8) = 'street'
              AND state_code IS NOT NULL
              AND zip5 IS NOT NULL
            UNION ALL
            SELECT
                1 AS target_source_rank,
                archived.address_key AS target_address_key,
                archived.identity_key AS target_identity_key,
                archived.premise_key AS target_premise_key,
                archived.line1_norm AS target_line1_norm,
                COALESCE(archived.unit_norm, '') AS target_unit_norm,
                archived.city_norm AS target_city_norm,
                archived.state_code AS target_state_code,
                archived.zip5 AS target_zip5,
                archived.zip4 AS target_zip4,
                COALESCE(archived.country_code, 'US') AS target_country_code,
                archived.first_line AS target_first_line,
                archived.second_line AS target_second_line,
                archived.city_name AS target_city_name,
                archived.state_name AS target_state_name,
                archived.postal_code AS target_postal_code,
                COALESCE(archived.unit_norm, '') AS unit_norm,
                archived.state_code,
                archived.zip5,
                COALESCE(archived.country_code, 'US') AS country_code,
                source_keys.completion_norm,
                {qschema}.addr_street_suffix_token_v1(archived.first_line, archived.second_line) AS suffix_token,
                {qschema}.addr_street_direction_token_v1(archived.first_line, archived.second_line) AS direction_token
            FROM archive_prefilter AS archived
            JOIN source_keys
              ON COALESCE(archived.unit_norm, '') = source_keys.unit_norm
             AND archived.state_code = source_keys.state_code
             AND archived.zip5 = source_keys.zip5
             AND COALESCE(archived.country_code, 'US') = source_keys.country_code
             AND {qschema}.addr_street_completion_norm_v1(archived.first_line, archived.second_line)
                 = source_keys.completion_norm
        ),
        target_ranked AS MATERIALIZED (
            SELECT
                *,
                CASE WHEN suffix_token IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN direction_token IS NOT NULL THEN 1 ELSE 0 END AS completion_rank
            FROM target_rows
            WHERE completion_norm IS NOT NULL
        ),
        candidates AS (
            SELECT
                s.rn,
                s.source_ctid,
                s.source_address_key,
                t.target_address_key,
                t.target_identity_key,
                t.target_premise_key,
                t.target_line1_norm,
                t.target_unit_norm,
                t.target_city_norm,
                t.target_state_code,
                t.target_zip5,
                t.target_zip4,
                t.target_country_code,
                t.target_first_line,
                t.target_second_line,
                t.target_city_name,
                t.target_state_name,
                t.target_postal_code,
                t.completion_rank AS target_completion_rank,
                t.target_source_rank,
                (s.suffix_token IS NULL AND t.suffix_token IS NOT NULL) AS filled_suffix,
                (s.direction_token IS NULL AND t.direction_token IS NOT NULL) AS filled_direction
            FROM source_ranked AS s
            JOIN target_ranked AS t
              ON t.completion_norm = s.completion_norm
             AND t.unit_norm = s.unit_norm
             AND t.state_code = s.state_code
             AND t.zip5 = s.zip5
             AND t.country_code = s.country_code
             AND t.target_address_key IS DISTINCT FROM s.source_address_key
             AND t.completion_rank > s.completion_rank
            WHERE (s.suffix_token IS NULL AND t.suffix_token IS NOT NULL)
               OR (s.direction_token IS NULL AND t.direction_token IS NOT NULL)
        ),
        unique_candidates AS (
            SELECT
                rn,
                bool_or(filled_suffix) AS filled_suffix,
                bool_or(filled_direction) AS filled_direction
            FROM candidates
            GROUP BY rn
            HAVING count(DISTINCT target_address_key) = 1
        )
        SELECT DISTINCT ON (c.rn)
            c.*
        FROM candidates AS c
        JOIN unique_candidates AS u USING (rn)
        ORDER BY c.rn, c.target_completion_rank DESC, c.target_source_rank, c.target_address_key;
    """


def _is_statement_timeout_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "statement timeout" in message or "querycancelederror" in message


def _zip_alias_sql(
    *,
    keyed_table: str,
    archive: str,
) -> str:
    return f"""
        CREATE TEMP TABLE address_zip_aliases ON COMMIT DROP AS
        WITH source_rows AS (
            SELECT
                rn,
                source_ctid,
                address_key AS source_address_key,
                line1_norm,
                COALESCE(unit_norm, '') AS unit_norm,
                city_norm,
                state_code,
                COALESCE(country_code, 'US') AS country_code
            FROM {keyed_table}
            WHERE address_key IS NULL
              AND line1_norm IS NOT NULL
              AND city_norm IS NOT NULL
              AND state_code IS NOT NULL
              AND zip5 IS NULL
              AND COALESCE(country_code, 'US') = 'US'
        ),
        source_keys AS (
            SELECT DISTINCT line1_norm, unit_norm, city_norm, state_code, country_code
            FROM source_rows
        ),
        target_rows AS (
            SELECT
                0 AS target_source_rank,
                address_key AS target_address_key,
                identity_key AS target_identity_key,
                premise_key AS target_premise_key,
                line1_norm AS target_line1_norm,
                COALESCE(unit_norm, '') AS target_unit_norm,
                city_norm AS target_city_norm,
                state_code AS target_state_code,
                zip5 AS target_zip5,
                zip4 AS target_zip4,
                COALESCE(country_code, 'US') AS target_country_code,
                first_line AS target_first_line,
                second_line AS target_second_line,
                city_name AS target_city_name,
                state_name AS target_state_name,
                postal_code AS target_postal_code,
                line1_norm,
                COALESCE(unit_norm, '') AS unit_norm,
                city_norm,
                state_code,
                COALESCE(country_code, 'US') AS country_code
            FROM {keyed_table}
            WHERE address_key IS NOT NULL
              AND identity_key IS NOT NULL
              AND split_part(identity_key, '|', 8) = 'street'
              AND line1_norm IS NOT NULL
              AND city_norm IS NOT NULL
              AND state_code IS NOT NULL
              AND zip5 IS NOT NULL
            UNION ALL
            SELECT
                1 AS target_source_rank,
                archived.address_key AS target_address_key,
                archived.identity_key AS target_identity_key,
                archived.premise_key AS target_premise_key,
                archived.line1_norm AS target_line1_norm,
                COALESCE(archived.unit_norm, '') AS target_unit_norm,
                archived.city_norm AS target_city_norm,
                archived.state_code AS target_state_code,
                archived.zip5 AS target_zip5,
                archived.zip4 AS target_zip4,
                COALESCE(archived.country_code, 'US') AS target_country_code,
                archived.first_line AS target_first_line,
                archived.second_line AS target_second_line,
                archived.city_name AS target_city_name,
                archived.state_name AS target_state_name,
                archived.postal_code AS target_postal_code,
                archived.line1_norm,
                COALESCE(archived.unit_norm, '') AS unit_norm,
                archived.city_norm,
                archived.state_code,
                COALESCE(archived.country_code, 'US') AS country_code
            FROM {archive} AS archived
            JOIN source_keys
              ON archived.line1_norm = source_keys.line1_norm
             AND COALESCE(archived.unit_norm, '') = source_keys.unit_norm
             AND archived.city_norm = source_keys.city_norm
             AND archived.state_code = source_keys.state_code
             AND COALESCE(archived.country_code, 'US') = source_keys.country_code
            WHERE archived.address_key IS NOT NULL
              AND archived.identity_key IS NOT NULL
              AND COALESCE(archived.precision, split_part(archived.identity_key, '|', 8)) = 'street'
              AND archived.merged_into IS NULL
              AND archived.line1_norm IS NOT NULL
              AND archived.city_norm IS NOT NULL
              AND archived.state_code IS NOT NULL
              AND archived.zip5 IS NOT NULL
        ),
        candidates AS (
            SELECT
                s.rn,
                s.source_ctid,
                s.source_address_key,
                t.target_address_key,
                t.target_identity_key,
                t.target_premise_key,
                t.target_line1_norm,
                t.target_unit_norm,
                t.target_city_norm,
                t.target_state_code,
                t.target_zip5,
                t.target_zip4,
                t.target_country_code,
                t.target_first_line,
                t.target_second_line,
                t.target_city_name,
                t.target_state_name,
                t.target_postal_code,
                t.target_source_rank
            FROM source_rows AS s
            JOIN target_rows AS t
              ON t.line1_norm = s.line1_norm
             AND t.unit_norm = s.unit_norm
             AND t.city_norm = s.city_norm
             AND t.state_code = s.state_code
             AND t.country_code = s.country_code
        ),
        unique_candidates AS (
            SELECT rn
            FROM candidates
            GROUP BY rn
            HAVING count(DISTINCT target_address_key) = 1
               AND count(DISTINCT target_zip5) = 1
        )
        SELECT DISTINCT ON (c.rn)
            c.*
        FROM candidates AS c
        JOIN unique_candidates AS u USING (rn)
        ORDER BY c.rn, c.target_source_rank, c.target_address_key;
    """


async def resolve_into_archive(
    staging_table: str,
    field_map: Mapping[str, str],
    *,
    source_bit: int,
    priority: int,
    schema: str | None = None,
    archive_table: str | None = None,
    work_mem: str = "256MB",
    timeout: str = "10min",
    gate_sample_limit: int = 20,
    cancel_check: Callable[[], Awaitable[None]] | None = None,
) -> ResolveStats:
    started = time.monotonic()
    schema = schema or _schema_name()
    archive_table = await _select_canonical_archive_table(schema, archive_table or archive_table_name())
    first, second, city, state, zip_code, country = _address_sql(schema, field_map)
    staging = _qtable(schema, staging_table)
    archive = _qtable(schema, archive_table)
    qschema = _quote_ident(schema)
    keyed_table_name = "address_archive_resolve_keyed"
    keyed_table = _quote_ident("address_archive_resolve_keyed")
    keyed_temp_table = f"pg_temp.{keyed_table}"
    address_key_expr = _key_from_identity_sql(schema, "identity_key")
    premise_key_expr = _key_from_identity_sql(schema, "premise_identity_key")
    keyed_raw_copy_sql = _keyed_raw_copy_sql(
        staging=staging,
        first=first,
        second=second,
        city=city,
        state=state,
        zip_code=zip_code,
        country=country,
    )
    keyed_materialize_sql = f"""
        CREATE TEMP TABLE {keyed_table} ON COMMIT DROP AS
        WITH raw AS (
            SELECT
                row_number() OVER (ORDER BY ctid) AS rn,
                ctid::text AS source_ctid,
                address_key AS staged_address_key,
                NULLIF(trim(COALESCE({first}, '')), '') AS first_line,
                NULLIF(trim(COALESCE({second}, '')), '') AS second_line,
                NULLIF(trim(COALESCE({city}, '')), '') AS city_name,
                NULLIF(trim(COALESCE({state}, '')), '') AS state_name,
                NULLIF(trim(COALESCE({zip_code}, '')), '') AS postal_code,
                {country} AS raw_country_code
            FROM {staging}
        ),
        ranked_stamped AS (
            SELECT
                raw.*,
                row_number() OVER (
                    PARTITION BY staged_address_key
                    ORDER BY
                        first_line IS NULL,
                        length(COALESCE(first_line, '')) DESC,
                        second_line IS NULL,
                        length(COALESCE(second_line, '')) DESC,
                        city_name IS NULL,
                        length(COALESCE(city_name, '')) DESC,
                        COALESCE(first_line, ''),
                        COALESCE(second_line, ''),
                        COALESCE(city_name, ''),
                        COALESCE(state_name, ''),
                        COALESCE(postal_code, ''),
                        rn
                ) AS key_rank
            FROM raw
            WHERE staged_address_key IS NOT NULL
        ),
        raw_to_normalize AS (
            SELECT
                rn,
                source_ctid,
                staged_address_key,
                first_line,
                second_line,
                city_name,
                state_name,
                postal_code,
                raw_country_code
            FROM ranked_stamped
            WHERE key_rank = 1
            UNION ALL
            SELECT
                rn,
                source_ctid,
                staged_address_key,
                first_line,
                second_line,
                city_name,
                state_name,
                postal_code,
                raw_country_code
            FROM raw
            WHERE staged_address_key IS NULL
        ),
        normalized AS (
            SELECT
                rn,
                source_ctid,
                staged_address_key,
                {qschema}.addr_identity_key_v1(
                    first_line, second_line, city_name, state_name, postal_code, raw_country_code
                ) AS identity_key,
                {qschema}.addr_premise_identity_key_v1(
                    first_line, second_line, city_name, state_name, postal_code, raw_country_code
                ) AS premise_identity_key,
                {qschema}.addr_street_norm_v1(first_line, second_line) AS line1_norm,
                {qschema}.addr_unit_norm_v1(first_line, second_line) AS unit_norm,
                {qschema}.addr_city_norm_v1(city_name) AS city_norm,
                LEFT({qschema}.addr_state_code_v1(state_name), 32) AS state_code,
                {qschema}.addr_zip5_norm_v1(postal_code) AS zip5,
                NULLIF(substr(regexp_replace(COALESCE(postal_code, ''), '[^0-9]', '', 'g'), 6, 4), '') AS zip4,
                {qschema}.addr_country_code_v1(raw_country_code) AS country_code,
                first_line,
                second_line,
                city_name,
                state_name,
                postal_code
            FROM raw_to_normalize
        ),
        computed AS (
            SELECT
                *,
                {address_key_expr} AS computed_address_key,
                {premise_key_expr} AS premise_key
            FROM normalized
        )
        SELECT
            rn,
            source_ctid,
            staged_address_key,
            COALESCE(staged_address_key, computed_address_key) AS address_key,
            computed_address_key,
            identity_key,
            premise_key,
            line1_norm,
            unit_norm,
            city_norm,
            state_code,
            zip5,
            zip4,
            country_code,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code
        FROM computed;
    """
    valid_source = f"""
        SELECT *
        FROM {keyed_table}
        WHERE address_key IS NOT NULL AND identity_key IS NOT NULL
    """
    dedup_cte = f"""
        SELECT DISTINCT ON (address_key) *
        FROM ({valid_source}) keyed
        ORDER BY
            address_key,
            first_line IS NULL,
            length(COALESCE(first_line, '')) DESC,
            second_line IS NULL,
            length(COALESCE(second_line, '')) DESC,
            city_name IS NULL,
            length(COALESCE(city_name, '')) DESC,
            COALESCE(first_line, ''),
            COALESCE(second_line, ''),
            COALESCE(city_name, ''),
            COALESCE(state_name, ''),
            COALESCE(postal_code, ''),
            identity_key
    """
    reason_sql = f"""
        WITH unit_conflict_premises AS (
            SELECT premise_key
            FROM {keyed_table}
            WHERE premise_key IS NOT NULL AND COALESCE(unit_norm, '') <> ''
            GROUP BY premise_key
            HAVING count(DISTINCT unit_norm) > 1
        )
        SELECT jsonb_build_object(
            'missing_zip', count(*) FILTER (WHERE country_code = 'US' AND zip5 IS NULL),
            'missing_state', count(*) FILTER (WHERE country_code = 'US' AND state_code IS NULL),
            'missing_street', count(*) FILTER (
                WHERE country_code = 'US'
                  AND line1_norm IS NULL
                  AND city_norm IS NOT NULL
                  AND state_code IS NOT NULL
                  AND zip5 IS NOT NULL
            ),
            'unsupported_country', count(*) FILTER (WHERE country_code <> 'US'),
            'ambiguous_unit', count(*) FILTER (
                WHERE second_line IS NOT NULL
                  AND COALESCE(unit_norm, '') = ''
                  AND line1_norm IS NOT NULL
            ),
            'unit_conflicts', (SELECT count(*) FROM unit_conflict_premises)
        )
        FROM {keyed_table};
    """
    coverage_sql = f"""
        WITH raw AS (
            SELECT
                address_key,
                NULLIF(trim(COALESCE({first}, '')), '') AS first_line,
                NULLIF(trim(COALESCE({second}, '')), '') AS second_line,
                NULLIF(trim(COALESCE({state}, '')), '') AS state_name,
                NULLIF(trim(COALESCE({zip_code}, '')), '') AS postal_code,
                {country} AS raw_country_code
            FROM {staging}
        ),
        eligible AS (
            SELECT address_key
            FROM raw
            WHERE address_key IS NOT NULL
            UNION ALL
            SELECT address_key
            FROM raw
            WHERE address_key IS NULL
              AND {qschema}.addr_street_norm_v1(first_line, second_line) IS NOT NULL
              AND {qschema}.addr_state_code_v1(state_name) IS NOT NULL
              AND {qschema}.addr_zip5_norm_v1(postal_code) IS NOT NULL
              AND {qschema}.addr_country_code_v1(raw_country_code) = 'US'
        )
        SELECT jsonb_build_object(
            'eligible_key_rows', count(*),
            'eligible_null_key_rows', count(*) FILTER (WHERE address_key IS NULL)
        )
        FROM eligible;
    """

    async with db.transaction() as session:
        _emit_progress(
            phase="address archive resolve",
            unit="phase",
            total=5,
            done=0,
            pct=0,
            message="resolving canonical addresses",
        )
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key));"),
            {"lock_key": _archive_lock_key(schema, archive_table, "resolve")},
        )
        await session.execute(text(f"SET LOCAL work_mem = '{_setting_value(work_mem)}';"))
        await session.execute(text(f"SET LOCAL statement_timeout = '{_setting_value(timeout)}';"))

        if cancel_check:
            await cancel_check()

        await session.execute(text(f"DROP TABLE IF EXISTS {keyed_temp_table};"))
        rust_materialized = await _try_materialize_keyed_with_rust(
            session,
            keyed_table=keyed_table,
            keyed_table_name=keyed_table_name,
            raw_copy_sql=keyed_raw_copy_sql,
        )
        if not rust_materialized:
            await session.execute(text(keyed_materialize_sql))
        staged = int((await session.execute(text(f"SELECT count(*) FROM {staging};"))).scalar() or 0)
        keyed_rows = int((await session.execute(text(f"SELECT count(*) FROM {keyed_table};"))).scalar() or 0)
        mismatch = (
            await session.execute(text(f"""
                SELECT rn, staged_address_key, computed_address_key, identity_key
                  FROM {keyed_table}
                 WHERE staged_address_key IS NOT NULL
                   AND staged_address_key IS DISTINCT FROM computed_address_key
                 ORDER BY rn
                 LIMIT 1;
            """))
        ).first()
        if mismatch:
            _emit_progress(
                status="failed",
                phase="address archive resolve",
                unit="row",
                total=staged,
                done=0,
                pct=0,
                message="stamped canonical address key mismatch",
            )
            raise RuntimeError(
                "Stamped canonical address key does not match identity_key: "
                f"rn={mismatch.rn} "
                f"stamped={mismatch.staged_address_key} expected={mismatch.computed_address_key} "
                f"identity={mismatch.identity_key!r}"
            )
        await session.execute(text("DROP TABLE IF EXISTS pg_temp.address_zip_aliases;"))
        await session.execute(text(_zip_alias_sql(keyed_table=keyed_table, archive=archive)))
        zip_alias_stats_raw = (
            await session.execute(text("""
                SELECT jsonb_build_object(
                    'zip_aliases', count(*),
                    'missing_zip_recovered', count(*)
                )
                FROM address_zip_aliases;
            """))
        ).scalar() or {}
        if isinstance(zip_alias_stats_raw, str):
            zip_alias_stats = json.loads(zip_alias_stats_raw)
        else:
            zip_alias_stats = dict(zip_alias_stats_raw)
        zip_alias_rows = int(zip_alias_stats.get("zip_aliases") or 0)
        if zip_alias_rows:
            await session.execute(text(f"""
                UPDATE {keyed_table} AS keyed
                   SET address_key = aliases.target_address_key,
                       computed_address_key = aliases.target_address_key,
                       identity_key = aliases.target_identity_key,
                       premise_key = aliases.target_premise_key,
                       line1_norm = aliases.target_line1_norm,
                       unit_norm = aliases.target_unit_norm,
                       city_norm = aliases.target_city_norm,
                       state_code = aliases.target_state_code,
                       zip5 = aliases.target_zip5,
                       zip4 = aliases.target_zip4,
                       country_code = aliases.target_country_code,
                       first_line = aliases.target_first_line,
                       second_line = aliases.target_second_line,
                       city_name = aliases.target_city_name,
                       state_name = aliases.target_state_name,
                       postal_code = aliases.target_postal_code
                  FROM address_zip_aliases AS aliases
                 WHERE keyed.rn = aliases.rn;
            """))
            await session.execute(text(f"""
                UPDATE {staging} AS target
                   SET address_key = aliases.target_address_key
                  FROM address_zip_aliases AS aliases
                 WHERE target.ctid::text = aliases.source_ctid
                   AND target.address_key IS DISTINCT FROM aliases.target_address_key;
            """))
        alias_stats: dict[str, int] = {}
        completion_alias_rows = 0
        await session.execute(text("SAVEPOINT address_completion_alias_repair;"))
        try:
            await session.execute(text("DROP TABLE IF EXISTS pg_temp.address_completion_aliases;"))
            await session.execute(text(_completion_alias_sql(schema=schema, keyed_table=keyed_table, archive=archive)))
            alias_stats_raw = (
                await session.execute(text("""
                    SELECT jsonb_build_object(
                        'completion_aliases', count(*),
                        'completion_suffix_aliases', count(*) FILTER (WHERE filled_suffix),
                        'completion_directional_aliases', count(*) FILTER (WHERE filled_direction)
                    )
                    FROM address_completion_aliases;
                """))
            ).scalar() or {}
            if isinstance(alias_stats_raw, str):
                alias_stats = json.loads(alias_stats_raw)
            else:
                alias_stats = dict(alias_stats_raw)
            completion_alias_rows = int(alias_stats.get("completion_aliases") or 0)
            if completion_alias_rows:
                await session.execute(text(f"""
                    UPDATE {keyed_table} AS keyed
                       SET address_key = aliases.target_address_key,
                           computed_address_key = aliases.target_address_key,
                           identity_key = aliases.target_identity_key,
                           premise_key = aliases.target_premise_key,
                           line1_norm = aliases.target_line1_norm,
                           unit_norm = aliases.target_unit_norm,
                           city_norm = aliases.target_city_norm,
                           state_code = aliases.target_state_code,
                           zip5 = aliases.target_zip5,
                           zip4 = aliases.target_zip4,
                           country_code = aliases.target_country_code,
                           first_line = aliases.target_first_line,
                           second_line = aliases.target_second_line,
                           city_name = aliases.target_city_name,
                           state_name = aliases.target_state_name,
                           postal_code = aliases.target_postal_code
                      FROM address_completion_aliases AS aliases
                     WHERE keyed.rn = aliases.rn;
                """))
                await session.execute(text(f"""
                    UPDATE {staging} AS target
                       SET address_key = aliases.target_address_key
                      FROM address_completion_aliases AS aliases
                     WHERE (
                               target.ctid::text = aliases.source_ctid
                               OR (
                                   aliases.source_address_key IS NOT NULL
                                   AND target.address_key = aliases.source_address_key
                               )
                           )
                       AND target.address_key IS DISTINCT FROM aliases.target_address_key;
                """))
            await session.execute(text("RELEASE SAVEPOINT address_completion_alias_repair;"))
        except Exception as exc:
            try:
                await session.execute(text("ROLLBACK TO SAVEPOINT address_completion_alias_repair;"))
                await session.execute(text("RELEASE SAVEPOINT address_completion_alias_repair;"))
            except Exception:
                logger.exception("Failed to roll back address completion alias savepoint")
                raise
            if not _is_statement_timeout_error(exc):
                raise
            alias_stats = {"completion_aliases": 0, "completion_alias_repair_skipped": 1}
            logger.warning("Skipping address completion alias repair after statement timeout: %s", exc)
            _emit_progress(
                phase="address archive resolve",
                unit="row",
                total=staged,
                done=0,
                pct=15,
                message="skipped slow completion alias repair",
            )
        _emit_progress(
            phase="address archive resolve",
            unit="row",
            total=staged,
            done=staged,
            pct=20,
            message="materialized deduplicated canonical address keys",
            keyed_rows=keyed_rows,
            zip_aliases=zip_alias_rows,
            completion_aliases=completion_alias_rows,
        )
        await session.execute(text(f"CREATE INDEX ON {keyed_table} (address_key) WHERE address_key IS NOT NULL;"))
        await session.execute(text(f"CREATE INDEX ON {keyed_table} (premise_key) WHERE premise_key IS NOT NULL;"))
        await session.execute(text(f"ANALYZE {keyed_table};"))

        null_key_rows = int((
            await session.execute(text(f"SELECT count(*) FROM {keyed_table} WHERE address_key IS NULL;"))
        ).scalar() or 0)
        raw_coverage = (await session.execute(text(coverage_sql))).scalar() or {}
        if isinstance(raw_coverage, str):
            coverage = json.loads(raw_coverage)
        else:
            coverage = dict(raw_coverage)
        eligible_key_rows = int(coverage.get("eligible_key_rows") or 0)
        eligible_null_key_rows = int(coverage.get("eligible_null_key_rows") or 0)
        _emit_progress(
            phase="address archive resolve",
            unit="phase",
            total=5,
            done=1,
            pct=20,
            message="counted staged canonical addresses",
        )
        raw_reason_buckets = (await session.execute(text(reason_sql))).scalar() or {}
        if isinstance(raw_reason_buckets, str):
            reason_buckets = json.loads(raw_reason_buckets)
        else:
            reason_buckets = dict(raw_reason_buckets)
        reason_buckets = {str(key): int(value or 0) for key, value in reason_buckets.items()}
        reason_buckets.update({
            str(key): int(value or 0)
            for key, value in alias_stats.items()
        })
        reason_buckets.update({
            str(key): int(value or 0)
            for key, value in zip_alias_stats.items()
        })
        distinct_keys = int((
            await session.execute(text(f"SELECT count(*) FROM ({dedup_cte}) d;"))
        ).scalar() or 0)
        _emit_progress(
            phase="address archive resolve",
            unit="phase",
            total=5,
            done=2,
            pct=40,
            message="classified canonical address keys",
        )

        batch_collision = (
            await session.execute(text(f"""
                SELECT address_key,
                       min(identity_key) AS first_identity_key,
                       max(identity_key) AS second_identity_key,
                       count(DISTINCT identity_key) AS identity_count
                  FROM {keyed_table}
                 WHERE address_key IS NOT NULL AND identity_key IS NOT NULL
                 GROUP BY address_key
                HAVING count(DISTINCT identity_key) > 1
                 LIMIT 1;
            """))
        ).first()
        if batch_collision:
            _emit_progress(
                status="failed",
                phase="address archive resolve",
                unit="phase",
                total=5,
                done=3,
                pct=60,
                message="canonical address key collision",
            )
            raise RuntimeError(
                "Canonical address key collision: "
                f"address_key={batch_collision.address_key} "
                f"incoming={batch_collision.first_identity_key!r} "
                f"existing={batch_collision.second_identity_key!r}"
            )

        collision = (
            await session.execute(text(f"""
                SELECT s.address_key, s.identity_key, c.identity_key AS existing_identity_key
                FROM (
                    SELECT DISTINCT address_key, identity_key
                    FROM {keyed_table}
                    WHERE address_key IS NOT NULL AND identity_key IS NOT NULL
                ) s
                JOIN {archive} c USING (address_key)
                WHERE c.identity_key IS DISTINCT FROM s.identity_key
                LIMIT 1;
            """))
        ).first()
        if collision:
            _emit_progress(
                status="failed",
                phase="address archive resolve",
                unit="phase",
                total=5,
                done=3,
                pct=60,
                message="canonical address key collision",
            )
            raise RuntimeError(
                "Canonical address key collision: "
                f"address_key={collision.address_key} "
                f"incoming={collision.identity_key!r} existing={collision.existing_identity_key!r}"
            )
        _emit_progress(
            phase="address archive resolve",
            unit="phase",
            total=5,
            done=3,
            pct=60,
            message="checked canonical address collisions",
        )

        if cancel_check:
            await cancel_check()

        inserted = int((
            await session.execute(text(f"""
                WITH dedup AS ({dedup_cte}),
                inserted AS (
                    INSERT INTO {archive} AS target (
                        address_key, identity_key, identity_version, precision, premise_key,
                        line1_norm, unit_norm, city_norm, state_code, zip5, zip4,
                        country_code, first_line, second_line, city_name, state_name,
                        postal_code, source_bits, display_priority
                    )
                    SELECT
                        address_key,
                        identity_key,
                        {CURRENT_ADDRESS_IDENTITY_VERSION},
                        CASE WHEN split_part(identity_key, '|', 8) = 'city_zip'
                             THEN 'city_zip' ELSE 'street' END,
                        premise_key,
                        line1_norm,
                        COALESCE(unit_norm, ''),
                        city_norm,
                        state_code,
                        zip5,
                        zip4,
                        country_code,
                        first_line,
                        second_line,
                        city_name,
                        state_name,
                        postal_code,
                        :source_bit,
                        :priority
                    FROM dedup
                    ON CONFLICT (address_key) DO NOTHING
                    RETURNING 1
                )
                SELECT count(*) FROM inserted;
            """), {"source_bit": source_bit, "priority": priority})
        ).scalar() or 0)
        _emit_progress(
            phase="address archive resolve",
            unit="phase",
            total=5,
            done=4,
            pct=80,
            message="inserted new canonical addresses",
        )

        if cancel_check:
            await cancel_check()

        provenance_updates = int((
            await session.execute(text(f"""
                WITH dedup AS ({dedup_cte}),
                updated AS (
                    UPDATE {archive} c
                       SET source_bits = c.source_bits | :source_bit,
                           last_seen_at = now(),
                           display_priority = CASE
                               WHEN :priority < c.display_priority THEN :priority
                               ELSE c.display_priority
                           END,
                           first_line = CASE
                               WHEN :priority < c.display_priority THEN d.first_line
                               ELSE c.first_line
                           END,
                           second_line = CASE
                               WHEN :priority < c.display_priority THEN d.second_line
                               ELSE c.second_line
                           END,
                           city_name = CASE
                               WHEN :priority < c.display_priority THEN d.city_name
                               ELSE c.city_name
                           END,
                           state_name = CASE
                               WHEN :priority < c.display_priority THEN d.state_name
                               ELSE c.state_name
                           END,
                           postal_code = CASE
                               WHEN :priority < c.display_priority THEN d.postal_code
                               ELSE c.postal_code
                           END
                      FROM dedup d
                     WHERE c.address_key = d.address_key
                       AND (
                            c.source_bits & :source_bit = 0
                            OR :priority < c.display_priority
                       )
                    RETURNING 1
                )
                SELECT count(*) FROM updated;
            """), {"source_bit": source_bit, "priority": priority})
        ).scalar() or 0)
        gate_sample_limit = max(0, min(int(gate_sample_limit or 0), 100))
        if gate_sample_limit:
            sample_rows_result = await session.execute(text(f"""
                SELECT
                    address_key::text AS address_key,
                    source_bits,
                    precision,
                    first_line,
                    second_line,
                    city_name,
                    state_name,
                    postal_code,
                    line1_norm,
                    unit_norm,
                    zip5,
                    last_seen_at::text AS last_seen_at
                FROM {archive}
                WHERE (source_bits & :source_bit) <> 0
                  AND source_bits <> :source_bit
                ORDER BY last_seen_at DESC, address_key
                LIMIT :limit;
            """), {"source_bit": source_bit, "limit": gate_sample_limit})
            gate_sample_rows = [dict(row._mapping) for row in sample_rows_result]
        else:
            gate_sample_rows = []
        gate_violations = _resolve_gate_violations(
            staged=staged,
            distinct_keys=distinct_keys,
            inserted=inserted,
            eligible_null_key_rows=eligible_null_key_rows,
        )
        if gate_violations:
            _emit_progress(
                status="warning",
                phase="address gate validation",
                unit="gate",
                total=len(gate_violations),
                done=0,
                pct=0,
                message="canonical address gate warnings",
                sample_count=len(gate_sample_rows),
                violations=list(gate_violations),
            )
        else:
            _emit_progress(
                phase="address gate validation",
                unit="gate",
                total=1,
                done=1,
                pct=100,
                message="canonical address gate passed",
                sample_count=len(gate_sample_rows),
            )
        _emit_progress(
            phase="address archive resolve",
            unit="phase",
            total=5,
            done=5,
            pct=100,
            message="canonical address archive resolved",
        )

    return ResolveStats(
        staged=staged,
        distinct_keys=distinct_keys,
        inserted=inserted,
        provenance_updates=provenance_updates,
        null_key_rows=null_key_rows,
        eligible_key_rows=eligible_key_rows,
        eligible_null_key_rows=eligible_null_key_rows,
        reason_buckets=reason_buckets,
        gate_violations=gate_violations,
        gate_sample_rows=gate_sample_rows,
        elapsed_seconds=round(time.monotonic() - started, 3),
    )


async def migrate_legacy_archive_to_v2(
    *,
    schema: str | None = None,
    legacy_table: str = "address_archive",
    archive_table: str | None = None,
    work_mem: str = "512MB",
    timeout: str = "30min",
    dry_run: bool = False,
    sample_limit: int = 20,
    cancel_check: Callable[[], Awaitable[None]] | None = None,
) -> ArchiveV2MigrationStats:
    """Copy legacy checksum archive rows into the canonical v2 archive.

    The migration is deliberately set-based and idempotent. Checksum bridge rows
    are rebuilt from the legacy source in one transaction so checksum fan-out can
    never leak into `address_checksum_map`.
    """
    started = time.monotonic()
    schema = schema or _schema_name()
    archive_table = archive_table or archive_table_name()
    qschema = _quote_ident(schema)
    geo_source_type = f"{qschema}.{_quote_ident('address_archive_geo_source')}"
    legacy = _qtable(schema, legacy_table)
    archive = _qtable(schema, archive_table)
    checksum_map = _qtable(schema, "address_checksum_map")
    checksum_collision = _qtable(schema, "address_checksum_collision")
    sample_limit = max(0, min(int(sample_limit or 0), 100))
    keyed_table = _quote_ident("address_archive_v2_keyed")
    keyed_temp_table = f"pg_temp.{keyed_table}"
    raw_table = _quote_ident("address_archive_v2_raw")
    raw_temp_table = f"pg_temp.{raw_table}"
    valid_source = f"""
        SELECT *
        FROM {keyed_table}
        WHERE address_key IS NOT NULL AND identity_key IS NOT NULL
    """
    ranked_cte = f"""
        SELECT *
        FROM (
            SELECT
                valid.*,
                row_number() OVER (
                    PARTITION BY address_key
                    ORDER BY
                        (place_id IS NOT NULL) DESC,
                        date_added DESC NULLS LAST,
                        checksum
                ) AS rn
            FROM ({valid_source}) valid
        ) ranked
        WHERE rn = 1
    """

    async with db.transaction() as session:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key));"),
            {"lock_key": _archive_lock_key(schema, archive_table, "migrate")},
        )
        await session.execute(text(f"SET LOCAL work_mem = '{_setting_value(work_mem)}';"))
        await session.execute(text(f"SET LOCAL statement_timeout = '{_setting_value(timeout)}';"))

        if cancel_check:
            await cancel_check()

        await session.execute(text(f"DROP TABLE IF EXISTS {keyed_temp_table};"))
        await session.execute(text(f"DROP TABLE IF EXISTS {raw_temp_table};"))
        await session.execute(text(f"""
            CREATE TEMP TABLE {raw_table} ON COMMIT DROP AS
            SELECT
                row_number() OVER (ORDER BY checksum, ctid) AS rn,
                checksum,
                first_line,
                second_line,
                city_name,
                state_name,
                postal_code,
                country_code,
                telephone_number,
                fax_number,
                formatted_address,
                lat,
                long,
                date_added,
                place_id
            FROM {legacy};
        """))
        await session.execute(text(f"CREATE INDEX ON {raw_table} (rn);"))
        await session.execute(text(f"""
            CREATE TEMP TABLE {keyed_table} (
                checksum bigint,
                address_key uuid,
                identity_key text,
                premise_key uuid,
                line1_norm text,
                unit_norm text,
                city_norm text,
                state_code text,
                zip5 text,
                zip4 text,
                country_code text,
                first_line text,
                second_line text,
                city_name text,
                state_name text,
                postal_code text,
                telephone_number text,
                fax_number text,
                formatted_address text,
                lat numeric,
                long numeric,
                place_id text,
                geo_source text,
                date_added date
            ) ON COMMIT DROP;
        """))
        legacy_rows = int((await session.execute(text(f"SELECT count(*) FROM {raw_table};"))).scalar() or 0)
        batch_size = int(os.getenv("HLTHPRT_ADDRESS_ARCHIVE_MIGRATE_BATCH_ROWS", "25000") or 25000)
        batch_size = max(1000, min(batch_size, 100000))
        last_rn = 0
        materialized = 0
        insert_sql = text(f"""
            INSERT INTO {keyed_table} (
                checksum, address_key, identity_key, premise_key, line1_norm,
                unit_norm, city_norm, state_code, zip5, zip4, country_code,
                first_line, second_line, city_name, state_name, postal_code,
                telephone_number, fax_number, formatted_address, lat, long,
                place_id, geo_source, date_added
            )
            VALUES (
                :checksum, :address_key, :identity_key, :premise_key, :line1_norm,
                :unit_norm, :city_norm, :state_code, :zip5, :zip4, :country_code,
                :first_line, :second_line, :city_name, :state_name, :postal_code,
                :telephone_number, :fax_number, :formatted_address, :lat, :long,
                :place_id, :geo_source, :date_added
            );
        """)
        while True:
            if cancel_check:
                await cancel_check()
            raw_rows = (
                await session.execute(text(f"""
                    SELECT *
                    FROM {raw_table}
                    WHERE rn > :last_rn
                    ORDER BY rn
                    LIMIT :limit;
                """), {"last_rn": last_rn, "limit": batch_size})
            ).all()
            if not raw_rows:
                break
            keyed_rows: list[dict[str, Any]] = []
            for row in raw_rows:
                data = row._mapping
                first_line = data.get("first_line")
                second_line = data.get("second_line")
                city_name = data.get("city_name")
                state_name = data.get("state_name")
                postal_code = data.get("postal_code")
                country = data.get("country_code") or "US"
                identity_key = identity_key_v1(first_line, second_line, city_name, state_name, postal_code, country)
                premise_identity = premise_identity_key_v1(first_line, second_line, city_name, state_name, postal_code, country)
                postal_digits = re.sub(r"[^0-9]", "", str(postal_code or ""))
                keyed_rows.append({
                    "checksum": data.get("checksum"),
                    "address_key": str(key_from_identity(identity_key)) if identity_key else None,
                    "identity_key": identity_key,
                    "premise_key": str(key_from_identity(premise_identity)) if premise_identity else None,
                    "line1_norm": street_norm(first_line, second_line),
                    "unit_norm": unit_norm(first_line, second_line),
                    "city_norm": city_norm(city_name),
                    "state_code": _clamp_text(state_code(state_name), 32),
                    "zip5": zip5_norm(postal_code),
                    "zip4": postal_digits[5:9] or None,
                    "country_code": country_code(country),
                    "first_line": str(first_line).strip() if first_line is not None and str(first_line).strip() else None,
                    "second_line": str(second_line).strip() if second_line is not None and str(second_line).strip() else None,
                    "city_name": str(city_name).strip() if city_name is not None and str(city_name).strip() else None,
                    "state_name": str(state_name).strip() if state_name is not None and str(state_name).strip() else None,
                    "postal_code": str(postal_code).strip() if postal_code is not None and str(postal_code).strip() else None,
                    "telephone_number": str(data.get("telephone_number")).strip()
                    if data.get("telephone_number") is not None and str(data.get("telephone_number")).strip()
                    else None,
                    "fax_number": str(data.get("fax_number")).strip()
                    if data.get("fax_number") is not None and str(data.get("fax_number")).strip()
                    else None,
                    "formatted_address": str(data.get("formatted_address")).strip()
                    if data.get("formatted_address") is not None and str(data.get("formatted_address")).strip()
                    else None,
                    "lat": data.get("lat"),
                    "long": data.get("long"),
                    "place_id": str(data.get("place_id")).strip()
                    if data.get("place_id") is not None and str(data.get("place_id")).strip()
                    else None,
                    "geo_source": "google"
                    if data.get("place_id") is not None and str(data.get("place_id")).strip()
                    else None,
                    "date_added": data.get("date_added"),
                })
            await session.execute(insert_sql, keyed_rows)
            last_rn = int(raw_rows[-1]._mapping["rn"])
            materialized += len(raw_rows)
            _emit_progress(
                phase="address archive v2 migration",
                unit="row",
                total=legacy_rows,
                done=materialized,
                pct=(materialized / legacy_rows) * 25.0 if legacy_rows else 25,
                message="materialized legacy archive keys",
            )
        await session.execute(text(f"ANALYZE {keyed_table};"))

        keyable_rows = int((await session.execute(text(f"SELECT count(*) FROM ({valid_source}) valid;"))).scalar() or 0)
        non_keyable_rows = legacy_rows - keyable_rows
        legacy_geocoded_keys = int((
            await session.execute(text(f"""
                SELECT count(DISTINCT address_key)
                FROM ({valid_source}) valid
                WHERE lat IS NOT NULL AND long IS NOT NULL;
            """))
        ).scalar() or 0)
        _emit_progress(
            phase="address archive v2 migration",
            unit="phase",
            total=4,
            done=1,
            pct=25,
            message="classified legacy archive rows",
        )

        upserted_rows = inserted_rows = updated_rows = 0
        checksum_map_rows = checksum_collision_rows = checksum_collision_checksums = 0
        if not dry_run:
            if cancel_check:
                await cancel_check()
            upsert_result = await session.execute(text(f"""
                WITH ranked AS ({ranked_cte}),
                upserted AS (
                    INSERT INTO {archive} AS target (
                        address_key, identity_key, identity_version, precision, premise_key,
                        line1_norm, unit_norm, city_norm, state_code, zip5, zip4,
                        country_code, first_line, second_line, city_name, state_name,
                        postal_code, telephone_number, fax_number, formatted_address,
                        lat, long, place_id, geo_source, geocode_source, geocode_quality,
                        geocoded_at, source_bits, display_priority, date_added
                    )
                    SELECT
                        address_key,
                        identity_key,
                        {CURRENT_ADDRESS_IDENTITY_VERSION},
                        CASE WHEN split_part(identity_key, '|', 8) = 'city_zip'
                             THEN 'city_zip' ELSE 'street' END,
                        premise_key,
                        line1_norm,
                        COALESCE(unit_norm, ''),
                        city_norm,
                        state_code,
                        zip5,
                        zip4,
                        country_code,
                        first_line,
                        second_line,
                        city_name,
                        state_name,
                        postal_code,
                        telephone_number,
                        fax_number,
                        formatted_address,
                        lat,
                        long,
                        place_id,
                        NULLIF(geo_source, '')::{geo_source_type},
                        CASE WHEN lat IS NOT NULL AND long IS NOT NULL THEN 'archive_backfill' ELSE NULL END,
                        CASE WHEN lat IS NOT NULL AND long IS NOT NULL THEN 'legacy_archive' ELSE NULL END,
                        CASE WHEN lat IS NOT NULL AND long IS NOT NULL THEN now() ELSE NULL END,
                        1,
                        0,
                        date_added
                    FROM ranked
                    ON CONFLICT (address_key) DO UPDATE SET
                        source_bits = target.source_bits | 1,
                        last_seen_at = now(),
                        display_priority = LEAST(target.display_priority, 0),
                        first_line = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.first_line ELSE COALESCE(target.first_line, EXCLUDED.first_line) END,
                        second_line = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.second_line ELSE COALESCE(target.second_line, EXCLUDED.second_line) END,
                        city_name = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.city_name ELSE COALESCE(target.city_name, EXCLUDED.city_name) END,
                        state_name = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.state_name ELSE COALESCE(target.state_name, EXCLUDED.state_name) END,
                        postal_code = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.postal_code ELSE COALESCE(target.postal_code, EXCLUDED.postal_code) END,
                        telephone_number = COALESCE(target.telephone_number, EXCLUDED.telephone_number),
                        fax_number = COALESCE(target.fax_number, EXCLUDED.fax_number),
                        formatted_address = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.formatted_address ELSE COALESCE(target.formatted_address, EXCLUDED.formatted_address) END,
                        lat = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.lat ELSE COALESCE(target.lat, EXCLUDED.lat) END,
                        long = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.long ELSE COALESCE(target.long, EXCLUDED.long) END,
                        place_id = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.place_id ELSE COALESCE(target.place_id, EXCLUDED.place_id) END,
                        geo_source = CASE
                            WHEN (target.place_id IS NULL AND EXCLUDED.place_id IS NOT NULL)
                              OR (
                                  (target.place_id IS NULL) = (EXCLUDED.place_id IS NULL)
                                  AND COALESCE(EXCLUDED.date_added, DATE '0001-01-01') > COALESCE(target.date_added, DATE '0001-01-01')
                              )
                            THEN EXCLUDED.geo_source ELSE COALESCE(target.geo_source, EXCLUDED.geo_source) END,
                        geocode_source = COALESCE(target.geocode_source, EXCLUDED.geocode_source),
                        geocode_quality = COALESCE(target.geocode_quality, EXCLUDED.geocode_quality),
                        geocoded_at = COALESCE(target.geocoded_at, EXCLUDED.geocoded_at),
                        date_added = NULLIF(GREATEST(COALESCE(target.date_added, DATE '0001-01-01'), COALESCE(EXCLUDED.date_added, DATE '0001-01-01')), DATE '0001-01-01')
                    RETURNING (xmax = 0) AS inserted
                )
                SELECT
                    count(*)::bigint AS upserted_rows,
                    count(*) FILTER (WHERE inserted)::bigint AS inserted_rows,
                    count(*) FILTER (WHERE NOT inserted)::bigint AS updated_rows
                FROM upserted;
            """))
            upsert_row = upsert_result.first()
            if upsert_row:
                upserted_rows = int(upsert_row.upserted_rows or 0)
                inserted_rows = int(upsert_row.inserted_rows or 0)
                updated_rows = int(upsert_row.updated_rows or 0)
            _emit_progress(
                phase="address archive v2 migration",
                unit="phase",
                total=4,
                done=2,
                pct=50,
                message="copied legacy archive rows",
            )

            if cancel_check:
                await cancel_check()
            await session.execute(text(f"TRUNCATE TABLE {checksum_map}, {checksum_collision};"))
            await session.execute(text(f"""
                WITH valid AS ({valid_source}),
                counted AS (
                    SELECT checksum, count(DISTINCT address_key) AS address_keys
                    FROM valid
                    WHERE checksum IS NOT NULL
                    GROUP BY checksum
                )
                INSERT INTO {checksum_map} (checksum, address_key)
                SELECT DISTINCT valid.checksum, valid.address_key
                FROM valid
                JOIN counted USING (checksum)
                WHERE counted.address_keys = 1;
            """))
            await session.execute(text(f"""
                WITH valid AS ({valid_source}),
                counted AS (
                    SELECT checksum, count(DISTINCT address_key) AS address_keys
                    FROM valid
                    WHERE checksum IS NOT NULL
                    GROUP BY checksum
                )
                INSERT INTO {checksum_collision} (checksum, address_key)
                SELECT DISTINCT valid.checksum, valid.address_key
                FROM valid
                JOIN counted USING (checksum)
                WHERE counted.address_keys > 1;
            """))
            _emit_progress(
                phase="address archive v2 migration",
                unit="phase",
                total=4,
                done=3,
                pct=75,
                message="rebuilt checksum bridge",
            )

        represented_missing_keys = int((
            await session.execute(text(f"""
                WITH legacy_keys AS (
                    SELECT DISTINCT address_key FROM ({valid_source}) valid
                )
                SELECT count(*) FROM legacy_keys
                WHERE NOT EXISTS (
                    SELECT 1 FROM {archive} v2 WHERE v2.address_key = legacy_keys.address_key
                );
            """))
        ).scalar() or 0)
        geocoded_missing_keys = int((
            await session.execute(text(f"""
                WITH legacy_keys AS (
                    SELECT DISTINCT address_key
                    FROM ({valid_source}) valid
                    WHERE lat IS NOT NULL AND long IS NOT NULL
                )
                SELECT count(*) FROM legacy_keys
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {archive} v2
                    WHERE v2.address_key = legacy_keys.address_key
                      AND v2.lat IS NOT NULL
                      AND v2.long IS NOT NULL
                );
            """))
        ).scalar() or 0)
        checksum_map_rows = int((await session.execute(text(f"SELECT count(*) FROM {checksum_map};"))).scalar() or 0)
        checksum_collision_rows = int((await session.execute(text(f"SELECT count(*) FROM {checksum_collision};"))).scalar() or 0)
        checksum_collision_checksums = int((
            await session.execute(text(f"SELECT count(DISTINCT checksum) FROM {checksum_collision};"))
        ).scalar() or 0)

        if sample_limit:
            sample_result = await session.execute(text(f"""
                SELECT
                    address_key::text AS address_key,
                    source_bits,
                    first_line,
                    second_line,
                    city_name,
                    state_name,
                    postal_code,
                    formatted_address,
                    lat,
                    long,
                    place_id,
                    date_added::text AS date_added
                FROM {archive}
                WHERE source_bits & 1 <> 0
                ORDER BY (place_id IS NOT NULL) DESC, date_added DESC NULLS LAST, address_key
                LIMIT :limit;
            """), {"limit": sample_limit})
            sample_rows = [dict(row._mapping) for row in sample_result]
        else:
            sample_rows = []

        _emit_progress(
            phase="address archive v2 migration",
            unit="phase",
            total=4,
            done=4,
            pct=100,
            message="verified legacy archive migration",
        )

    return ArchiveV2MigrationStats(
        legacy_rows=legacy_rows,
        keyable_rows=keyable_rows,
        non_keyable_rows=non_keyable_rows,
        upserted_rows=upserted_rows,
        inserted_rows=inserted_rows,
        updated_rows=updated_rows,
        checksum_map_rows=checksum_map_rows,
        checksum_collision_rows=checksum_collision_rows,
        checksum_collision_checksums=checksum_collision_checksums,
        represented_missing_keys=represented_missing_keys,
        legacy_geocoded_keys=legacy_geocoded_keys,
        geocoded_missing_keys=geocoded_missing_keys,
        runtime_seconds=round(time.monotonic() - started, 3),
        dry_run=dry_run,
        sample_rows=sample_rows,
    )


async def swap_archive_v2_to_current(
    *,
    schema: str | None = None,
    current_table: str = "address_archive",
    archive_table: str = "address_archive_v2",
    backup_table: str = "address_archive_legacy",
    checksum_map_table: str = "address_checksum_map",
    checksum_collision_table: str = "address_checksum_collision",
    allow_replace_backup: bool = False,
    dry_run: bool = False,
    timeout: str = "5min",
) -> ArchiveSwapStats:
    """Atomically rename canonical v2 archive into the current archive name."""
    started = time.monotonic()
    schema = schema or _schema_name()
    current = _qtable(schema, current_table)
    archive = _qtable(schema, archive_table)
    backup = _qtable(schema, backup_table)
    checksum_map = _qtable(schema, checksum_map_table)
    checksum_collision = _qtable(schema, checksum_collision_table)

    async with db.transaction() as session:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key));"),
            {"lock_key": _archive_lock_key(schema, f"{archive_table}->{current_table}", "swap")},
        )
        await session.execute(text(f"SET LOCAL statement_timeout = '{_setting_value(timeout)}';"))

        for table_name in (current_table, archive_table, checksum_map_table, checksum_collision_table):
            if not await _table_exists_in_session(session, schema, table_name):
                raise RuntimeError(f"required table is missing: {schema}.{table_name}")
        if await _table_has_column_in_session(session, schema, current_table, "address_key"):
            raise RuntimeError(f"{schema}.{current_table} already appears to be canonical")
        if not await _table_has_column_in_session(session, schema, current_table, "checksum"):
            raise RuntimeError(f"{schema}.{current_table} is not the legacy checksum archive")
        if not await _table_has_column_in_session(session, schema, archive_table, "address_key"):
            raise RuntimeError(f"{schema}.{archive_table} is not the canonical archive")
        backup_exists = await _table_exists_in_session(session, schema, backup_table)
        if backup_exists and not allow_replace_backup:
            raise RuntimeError(f"backup table already exists: {schema}.{backup_table}")

        legacy_fingerprint = await _legacy_archive_fingerprint(session, current)
        legacy_rows_before = int(legacy_fingerprint["rows"] or 0)
        archive_rows_before = int((await session.execute(text(f"SELECT count(*) FROM {archive};"))).scalar() or 0)
        checksum_map_rows = int((await session.execute(text(f"SELECT count(*) FROM {checksum_map};"))).scalar() or 0)
        checksum_collision_rows = int((await session.execute(text(f"SELECT count(*) FROM {checksum_collision};"))).scalar() or 0)
        missing_map_targets = int(
            (
                await session.execute(
                    text(
                        f"""
                        SELECT count(*)
                          FROM {checksum_map} m
                          LEFT JOIN {archive} a ON a.address_key = m.address_key
                         WHERE a.address_key IS NULL;
                        """
                    )
                )
            ).scalar()
            or 0
        )
        if legacy_rows_before <= 0:
            raise RuntimeError(f"{schema}.{current_table} is empty")
        if archive_rows_before <= 0:
            raise RuntimeError(f"{schema}.{archive_table} is empty")
        if checksum_map_rows <= 0:
            raise RuntimeError(f"{schema}.{checksum_map_table} is empty")
        if missing_map_targets:
            raise RuntimeError(f"checksum bridge has {missing_map_targets} missing canonical target(s)")

        swapped = False
        if not dry_run:
            if backup_exists and allow_replace_backup:
                await session.execute(text(f"DROP TABLE {backup};"))
            await session.execute(text(f"ALTER TABLE {current} RENAME TO {_quote_ident(backup_table)};"))
            await session.execute(text(f"ALTER TABLE {archive} RENAME TO {_quote_ident(current_table)};"))
            swapped = True

        after_legacy_table = backup if swapped else current
        after_current_table = current if swapped else archive
        after_fingerprint = await _legacy_archive_fingerprint(session, after_legacy_table)
        legacy_rows_after = int(after_fingerprint["rows"] or 0)
        current_rows_after = int((await session.execute(text(f"SELECT count(*) FROM {after_current_table};"))).scalar() or 0)
        if dict(after_fingerprint) != dict(legacy_fingerprint):
            raise RuntimeError("legacy archive fingerprint changed during swap")
        if current_rows_after != archive_rows_before:
            raise RuntimeError("canonical archive row count changed during swap")

    return ArchiveSwapStats(
        current_table=current_table,
        archive_table=archive_table,
        backup_table=backup_table,
        legacy_rows_before=legacy_rows_before,
        legacy_rows_after=legacy_rows_after,
        legacy_checksum_min=legacy_fingerprint["checksum_min"],
        legacy_checksum_max=legacy_fingerprint["checksum_max"],
        legacy_checksum_sum=str(legacy_fingerprint["checksum_sum"]) if legacy_fingerprint["checksum_sum"] is not None else None,
        archive_rows_before=archive_rows_before,
        current_rows_after=current_rows_after,
        checksum_map_rows=checksum_map_rows,
        checksum_collision_rows=checksum_collision_rows,
        missing_map_targets=missing_map_targets,
        swapped=swapped,
        runtime_seconds=round(time.monotonic() - started, 3),
        dry_run=dry_run,
    )


async def _legacy_archive_fingerprint(session: Any, table: str) -> dict[str, Any]:
    row = (
        await session.execute(
            text(
                f"""
                SELECT
                    count(*)::bigint AS rows,
                    min(checksum)::bigint AS checksum_min,
                    max(checksum)::bigint AS checksum_max,
                    sum(checksum::numeric) AS checksum_sum
                FROM {table};
                """
            )
        )
    ).first()
    return dict(row._mapping) if row is not None else {
        "rows": 0,
        "checksum_min": None,
        "checksum_max": None,
        "checksum_sum": None,
    }
