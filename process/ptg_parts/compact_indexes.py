# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact PTG2 snapshot index helpers."""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import time

from db.connection import db
from db.models import (
    PTG2PriceAtom,
    PTG2PriceCodeSet,
    PTG2PriceSetEntry,
    PTG2Procedure,
    PTG2ProviderEntryComponent,
    PTG2ProviderGroupMember,
    PTG2ProviderSet,
    PTG2ProviderSetComponent,
    PTG2ProviderSetEntry,
    PTG2ServingRateCompact,
)
from process.ptg_parts.config import (
    PTG2_COMPACT_DICTIONARY_INDEX_MODE_ENV,
    PTG2_COMPACT_SERVING_INDEX_MODE_ENV,
    PTG2_DEFAULT_INDEX_TASKS,
    PTG2_INDEX_TASKS_ENV,
    _env_int,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.screen import _emit_screen_line
from process.ptg_parts.snapshot_tables import _ptg2_snapshot_index_name

logger = logging.getLogger(__name__)


_PTG2_COMPACT_MODEL_BY_KIND = {
    "serving_rate_compact": PTG2ServingRateCompact,
    "procedure": PTG2Procedure,
    "price_code_set": PTG2PriceCodeSet,
    "price_atom": PTG2PriceAtom,
    "price_set_entry": PTG2PriceSetEntry,
    "provider_set": PTG2ProviderSet,
    "provider_set_component": PTG2ProviderSetComponent,
    "provider_set_entry": PTG2ProviderSetEntry,
    "provider_entry_component": PTG2ProviderEntryComponent,
    "provider_group_member": PTG2ProviderGroupMember,
}


def _ptg2_model_snapshot_index_role(model: type, index_name: str) -> str:
    table_prefix = f"{getattr(model, '__tablename__', '')}_"
    if index_name.startswith(table_prefix):
        return index_name[len(table_prefix):]
    return index_name


def _ptg2_index_timestamp() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


def _ptg2_compact_serving_index_mode() -> str:
    mode = str(os.getenv(PTG2_COMPACT_SERVING_INDEX_MODE_ENV) or "reported").strip().lower()
    return mode if mode in {"reported", "full", "none"} else "reported"


def _ptg2_compact_dictionary_index_mode() -> str:
    mode = str(os.getenv(PTG2_COMPACT_DICTIONARY_INDEX_MODE_ENV) or "serving").strip().lower()
    return mode if mode in {"serving", "full"} else "serving"


def _ptg2_compact_serving_reported_index_statement(schema_name: str, table_name: str) -> tuple[str, str]:
    role = "reported_system_order_idx"
    return (
        role,
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(table_name, role))} "
        f"ON {_quote_ident(schema_name)}.{_quote_ident(table_name)} "
        "(snapshot_id, plan_id, reported_code_system, reported_code, provider_count DESC, serving_rate_id);",
    )


def _ptg2_model_index_statements_for_table(model: type, schema_name: str, table_name: str) -> list[tuple[str, str]]:
    if model is PTG2ServingRateCompact:
        mode = _ptg2_compact_serving_index_mode()
        if mode == "none":
            return []
        if mode == "reported":
            return [_ptg2_compact_serving_reported_index_statement(schema_name, table_name)]

    statements: list[tuple[str, str]] = []
    primary_elements = tuple(str(element) for element in (getattr(model, "__my_index_elements__", None) or ()))
    if primary_elements:
        primary_name = getattr(model, "__primary_index_name__", None) or f"{model.__tablename__}_idx_primary"
        role = _ptg2_model_snapshot_index_role(model, primary_name)
        statements.append((
            role,
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(table_name, role))} "
            f"ON {_quote_ident(schema_name)}.{_quote_ident(table_name)} ({', '.join(primary_elements)});",
        ))
    additional_indexes = list(getattr(model, "__my_additional_indexes__", []) or ())
    if _ptg2_compact_dictionary_index_mode() == "serving":
        if model in {PTG2Procedure, PTG2PriceAtom, PTG2PriceSetEntry, PTG2ProviderSet}:
            additional_indexes = []
    for index in additional_indexes:
        elements = tuple(str(element) for element in (index.get("index_elements") or ()))
        if not elements:
            continue
        name = index.get("name") or f"{model.__tablename__}_{'_'.join(elements)}_idx"
        role = _ptg2_model_snapshot_index_role(model, str(name))
        using = index.get("using")
        where = index.get("where")
        include_elements = tuple(str(element) for element in (index.get("include") or ()))
        statement = (
            f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(table_name, role))} "
            f"ON {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        )
        if using:
            statement += f" USING {using}"
        statement += f" ({', '.join(elements)})"
        if include_elements:
            statement += f" INCLUDE ({', '.join(include_elements)})"
        if where:
            statement += f" WHERE {where}"
        statement += ";"
        statements.append((role, statement))
    return statements


async def _run_ptg2_index_statement(
    *,
    schema_name: str,
    table_name: str,
    role: str,
    statement: str,
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        label = f"{schema_name}.{table_name}.{role}"
        started_at = time.monotonic()
        start_message = f"PTG2_INDEX_START time={_ptg2_index_timestamp()} index={label}"
        _emit_screen_line(start_message)
        logger.info(start_message)
        try:
            await db.status(statement)
        except Exception as exc:
            elapsed = time.monotonic() - started_at
            fail_message = (
                f"PTG2_INDEX_FAILED time={_ptg2_index_timestamp()} index={label} "
                f"elapsed_seconds={elapsed:.2f} error={exc}"
            )
            _emit_screen_line(fail_message, stderr=True)
            logger.exception(fail_message)
            raise
        elapsed = time.monotonic() - started_at
        done_message = f"PTG2_INDEX_DONE time={_ptg2_index_timestamp()} index={label} elapsed_seconds={elapsed:.2f}"
        _emit_screen_line(done_message)
        logger.info(done_message)


async def _index_snapshot_compact_table_entries(schema_name: str, table_entries: list[tuple[str, str]]) -> float:
    index_specs: list[tuple[str, str, str]] = []
    for kind, table_name in table_entries:
        model = _PTG2_COMPACT_MODEL_BY_KIND.get(kind)
        if model is None or not table_name:
            continue
        for role, statement in _ptg2_model_index_statements_for_table(model, schema_name, table_name):
            index_specs.append((table_name, role, statement))
    if not index_specs:
        return 0.0
    task_count = max(1, _env_int(PTG2_INDEX_TASKS_ENV, PTG2_DEFAULT_INDEX_TASKS))
    task_count = min(task_count, len(index_specs))
    batch_message = (
        f"PTG2_INDEX_BATCH_START time={_ptg2_index_timestamp()} "
        f"schema={schema_name} indexes={len(index_specs)} tasks={task_count}"
    )
    _emit_screen_line(batch_message)
    logger.info(batch_message)
    started_at = time.monotonic()
    semaphore = asyncio.Semaphore(task_count)
    await asyncio.gather(
        *(
            _run_ptg2_index_statement(
                schema_name=schema_name,
                table_name=table_name,
                role=role,
                statement=statement,
                semaphore=semaphore,
            )
            for table_name, role, statement in index_specs
        )
    )
    elapsed = time.monotonic() - started_at
    done_message = (
        f"PTG2_INDEX_BATCH_DONE time={_ptg2_index_timestamp()} "
        f"schema={schema_name} indexes={len(index_specs)} tasks={task_count} elapsed_seconds={elapsed:.2f}"
    )
    _emit_screen_line(done_message)
    logger.info(done_message)
    return elapsed


async def _index_snapshot_compact_tables(schema_name: str, table_names: dict[str, str]) -> float:
    return await _index_snapshot_compact_table_entries(schema_name, list(table_names.items()))

