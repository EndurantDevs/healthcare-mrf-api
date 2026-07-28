# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database admission store for immutable frozen PTG source-file bindings."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_CONTRACT,
    FROZEN_RATE_FILE_BINDING_OPTION,
    FROZEN_RATE_FILE_BINDING_TABLE,
    FrozenRateFileBindingMismatchError,
    assert_existing_frozen_binding,
    frozen_internal_run_id,
    frozen_rate_binding_from_params,
    frozen_rate_binding_sha256,
    source_file_import_id_from_params,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


_INSERT_BINDING_SQL = """
INSERT INTO {table} (
    source_file_import_id,
    internal_run_id,
    binding_contract,
    frozen_rate_file_set_contract,
    frozen_rate_file_set_sha256,
    frozen_rate_file_count,
    source_key,
    import_month,
    plan_ids,
    plan_market_types,
    binding_sha256,
    binding_payload
)
VALUES (
    :source_file_import_id,
    :internal_run_id,
    :binding_contract,
    :frozen_rate_file_set_contract,
    :frozen_rate_file_set_sha256,
    :frozen_rate_file_count,
    :source_key,
    CAST(:import_month AS date),
    CAST(:plan_ids AS jsonb),
    CAST(:plan_market_types AS jsonb),
    :binding_sha256,
    CAST(:binding_payload AS jsonb)
)
ON CONFLICT (source_file_import_id) DO NOTHING
"""


def _binding_table() -> str:
    schema = _quote_ident(resolve_ptg2_schema())
    return f"{schema}.{_quote_ident(FROZEN_RATE_FILE_BINDING_TABLE)}"


def _row_as_mapping(database_row: Any) -> dict[str, Any]:
    if isinstance(database_row, Mapping):
        return dict(database_row)
    return dict(getattr(database_row, "_mapping", database_row))


async def _load_frozen_binding(
    connection: Any,
    source_file_import_id: str,
) -> dict[str, Any] | None:
    binding_rows = await connection.all(
        db.text(
            f"""
            SELECT source_file_import_id,
                   internal_run_id,
                   binding_sha256,
                   binding_payload
              FROM {_binding_table()}
             WHERE source_file_import_id = :source_file_import_id
            """
        ),
        source_file_import_id=source_file_import_id,
    )
    return _row_as_mapping(binding_rows[0]) if binding_rows else None


def _binding_options_from_row(
    binding_row_by_name: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if binding_row_by_name is None:
        return {}
    binding_payload = binding_row_by_name.get("binding_payload")
    if isinstance(binding_payload, str):
        try:
            binding_payload = json.loads(binding_payload)
        except json.JSONDecodeError:
            binding_payload = None
    return {
        FROZEN_RATE_FILE_BINDING_OPTION: (
            dict(binding_payload)
            if isinstance(binding_payload, Mapping)
            else binding_payload
        )
    }


def _assert_binding_row_integrity(
    binding_row_by_name: Mapping[str, Any],
    expected_binding_by_name: Mapping[str, Any],
) -> None:
    source_file_import_id = str(
        expected_binding_by_name["source_file_import_id"]
    )
    expected_digest = frozen_rate_binding_sha256(expected_binding_by_name)
    if (
        str(binding_row_by_name.get("source_file_import_id") or "")
        != source_file_import_id
        or str(binding_row_by_name.get("internal_run_id") or "")
        != frozen_internal_run_id(source_file_import_id)
        or str(binding_row_by_name.get("binding_sha256") or "")
        != expected_digest
    ):
        raise FrozenRateFileBindingMismatchError(
            "immutable frozen source-file binding changed"
        )
    assert_existing_frozen_binding(
        _binding_options_from_row(binding_row_by_name),
        expected_binding_by_name,
        row_exists=True,
    )


async def _lock_source_file_binding(
    connection: Any,
    source_file_import_id: str,
) -> None:
    await connection.scalar(
        db.text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended(:lock_key, 0))"
        ),
        lock_key=(
            "ptg2_frozen_source_file_binding_v1:"
            f"{source_file_import_id}"
        ),
    )


async def _insert_frozen_binding_row(
    connection: Any,
    expected_binding_by_name: Mapping[str, Any],
) -> None:
    """Attempt the immutable insert while allowing exact conflict replay."""

    source_file_import_id = str(
        expected_binding_by_name["source_file_import_id"]
    )
    await connection.status(
        db.text(_INSERT_BINDING_SQL.format(table=_binding_table())),
        source_file_import_id=source_file_import_id,
        internal_run_id=frozen_internal_run_id(source_file_import_id),
        binding_contract=FROZEN_RATE_FILE_BINDING_CONTRACT,
        frozen_rate_file_set_contract=FROZEN_RATE_FILE_SET_CONTRACT,
        frozen_rate_file_set_sha256=expected_binding_by_name[
            "frozen_rate_file_set_sha256"
        ],
        frozen_rate_file_count=expected_binding_by_name[
            "frozen_rate_file_count"
        ],
        source_key=expected_binding_by_name["source_key"],
        import_month=dt.date.fromisoformat(
            str(expected_binding_by_name["import_month"])
        ),
        plan_ids=canonical_json_dumps(expected_binding_by_name["plan_ids"]),
        plan_market_types=canonical_json_dumps(
            expected_binding_by_name["plan_market_types"]
        ),
        binding_sha256=frozen_rate_binding_sha256(
            expected_binding_by_name
        ),
        binding_payload=canonical_json_dumps(expected_binding_by_name),
    )


def _assert_loaded_binding(
    binding_row_by_name: Mapping[str, Any] | None,
    expected_binding_by_name: Mapping[str, Any] | None,
    *,
    requires_durable_row: bool,
) -> None:
    assert_existing_frozen_binding(
        _binding_options_from_row(binding_row_by_name),
        expected_binding_by_name,
        row_exists=binding_row_by_name is not None,
    )
    if not requires_durable_row:
        return
    if binding_row_by_name is None or expected_binding_by_name is None:
        raise FrozenRateFileBindingMismatchError(
            "protected frozen source-file binding is missing"
        )
    _assert_binding_row_integrity(
        binding_row_by_name,
        expected_binding_by_name,
    )


async def insert_or_compare_frozen_binding(
    connection: Any,
    params_by_name: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Atomically insert one binding or compare the existing immutable row."""

    expected_binding_by_name = frozen_rate_binding_from_params(params_by_name)
    source_file_import_id = source_file_import_id_from_params(params_by_name)
    if source_file_import_id is None:
        if expected_binding_by_name is not None:
            raise FrozenRateFileBindingMismatchError(
                "protected frozen import has no source_file_import_id"
            )
        return None
    await _lock_source_file_binding(connection, source_file_import_id)
    if expected_binding_by_name is not None:
        await _insert_frozen_binding_row(
            connection,
            expected_binding_by_name,
        )
    binding_row_by_name = await _load_frozen_binding(
        connection,
        source_file_import_id,
    )
    _assert_loaded_binding(
        binding_row_by_name,
        expected_binding_by_name,
        requires_durable_row=expected_binding_by_name is not None,
    )
    return expected_binding_by_name


async def recheck_frozen_binding(
    params_by_name: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Recheck a previously admitted binding without creating one."""

    expected_binding_by_name = frozen_rate_binding_from_params(params_by_name)
    source_file_import_id = source_file_import_id_from_params(params_by_name)
    if source_file_import_id is None:
        return expected_binding_by_name
    async with db.acquire() as connection:
        binding_row_by_name = await _load_frozen_binding(
            connection,
            source_file_import_id,
        )
    _assert_loaded_binding(
        binding_row_by_name,
        expected_binding_by_name,
        requires_durable_row=expected_binding_by_name is not None,
    )
    return expected_binding_by_name


async def insert_or_compare_frozen_binding_transaction(
    params_by_name: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Insert-or-compare through one self-owned database transaction."""

    async with db.acquire() as connection:
        return await insert_or_compare_frozen_binding(
            connection,
            params_by_name,
        )


__all__ = [
    "insert_or_compare_frozen_binding",
    "insert_or_compare_frozen_binding_transaction",
    "recheck_frozen_binding",
]
