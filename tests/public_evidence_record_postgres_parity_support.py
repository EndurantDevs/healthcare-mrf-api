# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Query-only PostgreSQL 18 parity helpers for public-evidence records."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
import re
from typing import AsyncIterator

import asyncpg
import pytest
from sqlalchemy.engine import make_url

POSTGRES_DSN_ENV = "HLTHPRT_PUBLIC_EVIDENCE_RECORD_PARITY_POSTGRES_DSN"
SUPPORTED_PERSISTENCE_RECORD_TYPE = "npi_enumeration"

DIGEST_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_DIGEST_V1\x00"
REFERENCE_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_REFERENCE_V1\x00"

_INVALID = "public_evidence_record_postgres_parity_invalid"
_TEST_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)
_MIN_BIGINT, _MAX_BIGINT = -(1 << 63), (1 << 63) - 1

_OBJECT_JSON_SQL = """
WITH input(name, kind, value, ordinal) AS (
    SELECT item.name, item.kind, item.value, item.ordinal
    FROM unnest($1::text[], $2::text[], $3::text[])
         WITH ORDINALITY AS item(name, kind, value, ordinal)
),
encoded AS (
    SELECT
        name,
        CASE kind
            WHEN 'text' THEN to_json(value)::text
            WHEN 'boolean' THEN (value::boolean)::text
            WHEN 'integer' THEN (value::bigint)::text
            WHEN 'null' THEN 'null'
            WHEN 'raw' THEN (value::json)::text
        END AS value_json
    FROM input
)
SELECT convert_to(
    '{' || coalesce(
        string_agg(
            to_json(name)::text || ':' || value_json,
            ',' ORDER BY convert_to(name, 'UTF8')
        ),
        ''
    ) || '}',
    'UTF8'
)
FROM encoded
"""

_ARRAY_JSON_SQL = """
WITH input(kind, value, ordinal) AS (
    SELECT item.kind, item.value, item.ordinal
    FROM unnest($1::text[], $2::text[])
         WITH ORDINALITY AS item(kind, value, ordinal)
),
encoded AS (
    SELECT
        ordinal,
        CASE kind
            WHEN 'text' THEN to_json(value)::text
            WHEN 'boolean' THEN (value::boolean)::text
            WHEN 'integer' THEN (value::bigint)::text
            WHEN 'null' THEN 'null'
            WHEN 'raw' THEN (value::json)::text
        END AS value_json
    FROM input
)
SELECT convert_to(
    '[' || coalesce(
        string_agg(value_json, ',' ORDER BY ordinal),
        ''
    ) || ']',
    'UTF8'
)
FROM encoded
"""

_SCALAR_JSON_SQL = """
SELECT convert_to(
    CASE $1::text
        WHEN 'text' THEN to_json($2::text)::text
        WHEN 'boolean' THEN ($2::boolean)::text
        WHEN 'integer' THEN ($2::bigint)::text
        WHEN 'null' THEN 'null'
    END,
    'UTF8'
)
"""

_FRAMED_VECTOR_SQL = """
WITH frames AS (
    SELECT
        $2::bytea
            || int2send($3::smallint)
            || convert_to($4::text, 'UTF8')
            || int8send(octet_length($1::bytea)::bigint)
            || $1::bytea AS digest_frame,
        $5::bytea
            || int2send($6::smallint)
            || convert_to($7::text, 'UTF8')
            || int8send(octet_length($1::bytea)::bigint)
            || $1::bytea AS reference_frame
)
SELECT
    encode(sha256($1::bytea), 'hex') AS plain_sha256,
    digest_frame,
    encode(sha256(digest_frame), 'hex') AS framed_digest,
    reference_frame,
    $8::text || rtrim(
        translate(encode(sha256(reference_frame), 'base64'), '+/', '-_'),
        '='
    ) AS reference
FROM frames
"""

_SOURCE_LINK_ORDER_SQL = """
WITH input(input_ordinal, source_record_ref, row_sha256) AS (
    SELECT item.input_ordinal, item.source_record_ref, item.row_sha256
    FROM unnest($1::bigint[], $2::text[], $3::text[])
         AS item(input_ordinal, source_record_ref, row_sha256)
),
checked AS (
    SELECT
        input_ordinal,
        source_record_ref,
        row_sha256,
        row_number() OVER (
            ORDER BY convert_to(source_record_ref, 'UTF8')
        ) - 1 AS expected_ordinal,
        count(*) OVER (PARTITION BY source_record_ref) AS occurrence_count
    FROM input
)
SELECT
    input_ordinal,
    source_record_ref,
    row_sha256,
    expected_ordinal,
    occurrence_count
FROM checked
ORDER BY input_ordinal
"""


class PublicEvidenceRecordPostgresParityError(RuntimeError):
    """One value-free failure for the executable parity boundary."""


def parity_error() -> PublicEvidenceRecordPostgresParityError:
    """Return a fresh normalized error without retaining input details."""

    return PublicEvidenceRecordPostgresParityError(_INVALID)


@dataclass(frozen=True)
class PostgresFramedVector:
    """Exact PostgreSQL bytes and digests for one canonical payload."""

    canonical_json: bytes
    plain_sha256: str
    digest_frame: bytes
    framed_digest: str
    reference_frame: bytes | None
    reference: str | None


def _validated_dsn() -> str:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    try:
        parsed = make_url(raw_dsn)
        database_name = str(parsed.database or "")
        is_valid = (
            parsed.drivername == "postgresql"
            and bool(parsed.host)
            and bool(parsed.username)
            and _TEST_DATABASE_RE.search(database_name) is not None
        )
    except Exception:
        normalized_error = parity_error()
    else:
        if is_valid:
            return raw_dsn
        normalized_error = parity_error()
    raise normalized_error


async def _connect() -> asyncpg.Connection:
    dsn = _validated_dsn()
    try:
        connection = await asyncpg.connect(dsn)
    except Exception:
        normalized_error = parity_error()
    else:
        return connection
    raise normalized_error


async def _assert_readonly_boundary(connection: asyncpg.Connection) -> None:
    boundary = await connection.fetchrow("""
        SELECT
            current_setting('server_version_num')::integer / 10000 = 18
                AS postgres18,
            upper(current_setting('server_encoding')) = 'UTF8' AS utf8,
            current_setting('transaction_read_only') = 'on' AS readonly,
            pg_my_temp_schema() = 0 AS no_temp_schema
        """)
    if boundary is None or not all(
        boundary[field_name]
        for field_name in ("postgres18", "utf8", "readonly", "no_temp_schema")
    ):
        raise parity_error()


@asynccontextmanager
async def postgres18_readonly_connection() -> AsyncIterator[asyncpg.Connection]:
    """Yield a fresh PostgreSQL 18 session that cannot persist test state."""

    connection = await _connect()
    transaction = None
    has_transaction_started = False
    has_pending_failure = False
    try:
        try:
            transaction = connection.transaction(readonly=True)
            await transaction.start()
            has_transaction_started = True
            await _assert_readonly_boundary(connection)
        except Exception:
            has_pending_failure = True
            normalized_error = parity_error()
        else:
            normalized_error = None
        if normalized_error is not None:
            raise normalized_error
        try:
            yield connection
        except BaseException:
            has_pending_failure = True
            raise
        try:
            await _assert_readonly_boundary(connection)
        except Exception:
            has_pending_failure = True
            normalized_error = parity_error()
        else:
            normalized_error = None
        if normalized_error is not None:
            raise normalized_error
    finally:
        cleanup_error = None
        try:
            if has_transaction_started and connection.is_in_transaction():
                await transaction.rollback()
        except Exception:
            cleanup_error = parity_error()
        try:
            await connection.close()
        except Exception:
            cleanup_error = parity_error()
        if cleanup_error is not None and not has_pending_failure:
            raise cleanup_error


def _ascii_text(value: str) -> str:
    if "\x00" in value or "\x7f" in value:
        raise parity_error()
    try:
        value.encode("ascii")
    except UnicodeEncodeError:
        normalized_error = parity_error()
    else:
        return value
    raise normalized_error


def postgres18_require_supported_record_type(record_type: object) -> None:
    """Limit this parity contract to the first ASCII NPI persistence slice."""

    try:
        if (
            type(record_type) is not str
            or record_type != SUPPORTED_PERSISTENCE_RECORD_TYPE
        ):
            raise parity_error()
    except Exception:
        normalized_error = parity_error()
    else:
        return
    raise normalized_error


async def _encoded_item(
    connection: asyncpg.Connection,
    value: object,
) -> tuple[str, str]:
    if type(value) is str:
        return "text", _ascii_text(value)
    if type(value) is bool:
        return "boolean", "true" if value else "false"
    if type(value) is int:
        if not _MIN_BIGINT <= value <= _MAX_BIGINT:
            raise parity_error()
        return "integer", str(value)
    if value is None:
        return "null", ""
    if type(value) in {dict, list}:
        nested = await _postgres18_ascii_canonical_json(connection, value)
        return "raw", nested.decode("ascii")
    raise parity_error()


async def _object_json(
    connection: asyncpg.Connection,
    payload: dict[object, object],
) -> bytes:
    names: list[str] = []
    kinds: list[str] = []
    values: list[str] = []
    for name, candidate_value in payload.items():
        if type(name) is not str:
            raise parity_error()
        names.append(_ascii_text(name))
        kind, encoded_value = await _encoded_item(connection, candidate_value)
        kinds.append(kind)
        values.append(encoded_value)
    encoded = await connection.fetchval(_OBJECT_JSON_SQL, names, kinds, values)
    if type(encoded) is not bytes:
        raise parity_error()
    return encoded


async def _array_json(
    connection: asyncpg.Connection,
    payload: list[object],
) -> bytes:
    kinds: list[str] = []
    values: list[str] = []
    for candidate_value in payload:
        kind, encoded_value = await _encoded_item(connection, candidate_value)
        kinds.append(kind)
        values.append(encoded_value)
    encoded = await connection.fetchval(_ARRAY_JSON_SQL, kinds, values)
    if type(encoded) is not bytes:
        raise parity_error()
    return encoded


async def _postgres18_ascii_canonical_json(
    connection: asyncpg.Connection,
    payload: object,
) -> bytes:
    if type(payload) is dict:
        return await _object_json(connection, payload)
    if type(payload) is list:
        return await _array_json(connection, payload)
    kind, encoded_value = await _encoded_item(connection, payload)
    encoded = await connection.fetchval(_SCALAR_JSON_SQL, kind, encoded_value)
    if type(encoded) is not bytes:
        raise parity_error()
    return encoded


async def postgres18_ascii_canonical_json(
    connection: asyncpg.Connection,
    payload: object,
) -> bytes:
    """Reconstruct Python's canonical JSON for the frozen ASCII subset."""

    try:
        encoded = await _postgres18_ascii_canonical_json(connection, payload)
    except Exception:
        normalized_error = parity_error()
    else:
        return encoded
    raise normalized_error


def _purpose(value: str) -> tuple[int, str]:
    if type(value) is not str:
        raise parity_error()
    value = _ascii_text(value)
    encoded = value.encode("ascii")
    if not encoded or len(encoded) > _MAX_BIGINT or len(encoded) > 32767:
        raise parity_error()
    return len(encoded), value


async def postgres18_framed_vector(
    connection: asyncpg.Connection,
    canonical_candidate: object,
    *,
    digest_purpose: str,
    reference_purpose: str | None = None,
    reference_prefix: str = "",
) -> PostgresFramedVector:
    """Return PostgreSQL canonical bytes plus complete digest/reference frames."""

    try:
        canonical_json = await _postgres18_ascii_canonical_json(
            connection,
            canonical_candidate,
        )
        digest_length, digest_name = _purpose(digest_purpose)
        reference_name = digest_name if reference_purpose is None else reference_purpose
        reference_length, reference_name = _purpose(reference_name)
        reference_prefix = _ascii_text(reference_prefix)
        frame_record = await connection.fetchrow(
            _FRAMED_VECTOR_SQL,
            canonical_json,
            DIGEST_DOMAIN,
            digest_length,
            digest_name,
            REFERENCE_DOMAIN,
            reference_length,
            reference_name,
            reference_prefix,
        )
        if frame_record is None:
            raise parity_error()
        framed_vector = PostgresFramedVector(
            canonical_json=canonical_json,
            plain_sha256=frame_record["plain_sha256"],
            digest_frame=bytes(frame_record["digest_frame"]),
            framed_digest=frame_record["framed_digest"],
            reference_frame=(
                bytes(frame_record["reference_frame"])
                if reference_purpose is not None
                else None
            ),
            reference=(
                frame_record["reference"] if reference_purpose is not None else None
            ),
        )
    except Exception:
        normalized_error = parity_error()
    else:
        return framed_vector
    raise normalized_error


async def postgres18_validate_source_link_order(
    connection: asyncpg.Connection,
    links: object,
) -> None:
    """Require unique UTF-8-byte order and contiguous zero-based ordinals."""

    try:
        if type(links) is not list or not 1 <= len(links) <= 16:
            raise parity_error()
        ordinals: list[int] = []
        references: list[str] = []
        digests: list[str] = []
        for expected_input_ordinal, link in enumerate(links):
            if type(link) is not dict or set(link) != {
                "source_record_ordinal",
                "source_record_ref",
                "row_sha256",
            }:
                raise parity_error()
            ordinal = link["source_record_ordinal"]
            reference = link["source_record_ref"]
            digest = link["row_sha256"]
            if (
                type(ordinal) is not int
                or ordinal != expected_input_ordinal
                or type(reference) is not str
                or type(digest) is not str
            ):
                raise parity_error()
            ordinals.append(ordinal)
            references.append(_ascii_text(reference))
            digests.append(_ascii_text(digest))
        ordering_records = await connection.fetch(
            _SOURCE_LINK_ORDER_SQL,
            ordinals,
            references,
            digests,
        )
        if len(ordering_records) != len(links) or any(
            ordering_record["input_ordinal"] != ordering_record["expected_ordinal"]
            or ordering_record["occurrence_count"] != 1
            for ordering_record in ordering_records
        ):
            raise parity_error()
    except Exception:
        normalized_error = parity_error()
    else:
        return
    raise normalized_error
