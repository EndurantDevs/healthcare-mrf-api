# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Spill exact native membership and occurrence batches without a match cap."""

from __future__ import annotations

import json

from sqlalchemy import text

from api.custom_import_plan_sql import CANDIDATES, MEMBERSHIPS, OCCURRENCE_COPY_COLUMNS, OCCURRENCES, PRICES
from api.ptg2_db_sidecars import _normalized_provider_shard_span
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

_NPI_BATCH_SIZE = 1024
_PRICE_BATCH_SIZE = 512


async def copy_plan_rows(session, table_name, columns, records):
    """Use bounded native COPY on the same request-owned connection."""

    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    driver_connection = raw_connection.driver_connection
    if connection.dialect.driver == "asyncpg":
        await driver_connection.copy_records_to_table(
            table_name, schema_name="pg_temp", columns=columns, records=records
        )
        return
    if connection.dialect.driver != "psycopg":
        raise PTG2ManifestArtifactError("the native plan COPY driver is unavailable")
    quote = connection.dialect.identifier_preparer.quote
    column_sql = ", ".join(quote(column_name) for column_name in columns)
    async with driver_connection.cursor() as cursor:
        async with cursor.copy(f"COPY pg_temp.{quote(table_name)} ({column_sql}) FROM STDIN") as native_copy:
            for record in records:
                await native_copy.write_row(record)


async def stage_native_memberships(session, scope, serving):
    """Read every eligible NPI in bounded batches with exact reverse membership."""

    after_npi = 0
    ordinal = scope.binding.binding_ordinal
    while True:
        query_result = await session.execute(
            text(f"""SELECT npi FROM pg_temp.{CANDIDATES}
                     WHERE binding_ordinal = :binding AND npi > :after_npi
                     ORDER BY npi LIMIT :batch_size"""),
            dict(binding=ordinal, after_npi=after_npi, batch_size=_NPI_BATCH_SIZE),
        )
        npis = tuple(int(npi) for npi in query_result.scalars())
        if not npis:
            return
        sets_by_npi = await serving._provider_set_ids_for_selected_npis(session, scope.serving_tables, npis)
        set_ids = tuple(sorted({set_id for memberships in sets_by_npi.values() for set_id in memberships}))
        keys_by_id = await serving._provider_set_keys_for_ids(session, scope.serving_tables, set_ids)
        if set(keys_by_id) != set(set_ids):
            raise PTG2ManifestArtifactError("the native membership references an unknown provider set")
        records = (
            (ordinal, npi, keys_by_id[set_id], membership_ordinal)
            for npi in npis
            for membership_ordinal, set_id in enumerate(dict.fromkeys(sets_by_npi.get(npi, ())))
        )
        await copy_plan_rows(
            session, MEMBERSHIPS, ("binding_ordinal", "npi", "provider_set_key", "membership_ordinal"), records
        )
        after_npi = npis[-1]


def _occurrence_record(scope, occurrence_id, serving_row, serving):
    """Retain the complete source payload and native identity presence bits."""

    source_id = serving_row.get("source_artifact_key", serving_row.get("source_key"))
    key = serving._ptg2_provider_rate_group_key(
        {**serving_row, "npi": 1, "location_hash": "_", "source_artifact_key": source_id}
    )
    group_columns = (*key[2:5], *key[5], *key[6], *key[7], list(key[8]), key[9])
    payload = json.dumps(serving_row, default=_native_binary_json, separators=(",", ":"))
    return (
        scope.binding.binding_ordinal,
        occurrence_id,
        int(serving_row["_ptg_provider_set_key"]),
        int(serving_row["price_key"]),
        *group_columns,
        payload,
    )


def _native_binary_json(binary_value):
    if isinstance(binary_value, (bytes, bytearray, memoryview)):
        return bytes(binary_value).hex()
    raise TypeError("the native occurrence contains unsupported JSON metadata")


async def _next_provider_shard(session, scope, span, after_shard):
    query_result = await session.execute(
        text(f"""SELECT MIN(provider_set_key) FROM pg_temp.{MEMBERSHIPS}
                 WHERE binding_ordinal = :binding AND provider_set_key >= :first_key"""),
        dict(binding=scope.binding.binding_ordinal, first_key=(after_shard + 1) * span),
    )
    first_key = query_result.scalar_one_or_none()
    return None if first_key is None else int(first_key) // span


async def _provider_keys_in_shard(session, scope, span, shard):
    query_result = await session.execute(
        text(f"""SELECT DISTINCT provider_set_key FROM pg_temp.{MEMBERSHIPS}
                 WHERE binding_ordinal = :binding AND provider_set_key >= :first_key
                   AND provider_set_key < :last_key ORDER BY provider_set_key"""),
        dict(binding=scope.binding.binding_ordinal, first_key=shard * span, last_key=(shard + 1) * span),
    )
    return tuple(int(provider_key) for provider_key in query_result.scalars())


async def stage_native_occurrences(session, scope, code_rows, serving):
    """Decode each relevant sealed provider shard once, never an order prefix."""

    span = _normalized_provider_shard_span(scope.serving_tables.provider_shard_span)
    after_shard, occurrence_id = -1, 0
    while (shard := await _next_provider_shard(session, scope, span, after_shard)) is not None:
        provider_keys = await _provider_keys_in_shard(session, scope, span, int(shard))
        serving_rows = await serving._merge_manifest_code_variant_rows(
            session,
            scope.serving_tables,
            code_rows=code_rows,
            provider_set_keys=provider_keys,
            source_trace_set_hash=None,
            network_names=scope.serving_tables.network_names or [],
            limit=None,
            offset=0,
            descending=False,
        )
        if serving_rows is None:
            raise PTG2ManifestArtifactError("the complete native procedure relation is unavailable")
        await serving._hydrate_provider_set_network_names(session, scope.serving_tables, serving_rows)
        records = (
            _occurrence_record(scope, occurrence_id + index, serving_row, serving)
            for index, serving_row in enumerate(serving_rows)
        )
        await copy_plan_rows(session, OCCURRENCES, OCCURRENCE_COPY_COLUMNS, records)
        occurrence_id += len(serving_rows)
        after_shard = int(shard)


async def stage_native_prices(session, scope, serving):
    """Spill only eligibility and exact minimum rates, then release decoded prices."""

    after_price = -1
    ordinal = scope.binding.binding_ordinal
    while True:
        query_result = await session.execute(
            text(f"""SELECT DISTINCT price_key FROM pg_temp.{OCCURRENCES}
                     WHERE binding_ordinal = :binding AND price_key > :after_price
                     ORDER BY price_key LIMIT :batch_size"""),
            dict(binding=ordinal, after_price=after_price, batch_size=_PRICE_BATCH_SIZE),
        )
        price_keys = tuple(int(price_key) for price_key in query_result.scalars())
        if not price_keys:
            return
        prices_by_key = await serving._version_three_prices_by_key(session, scope.serving_tables, price_keys)
        if any(not prices_by_key.get(price_key) for price_key in price_keys):
            raise PTG2ManifestArtifactError("the native price relation is incomplete")
        records = [
            _price_record(ordinal, price_key, prices_by_key[price_key], scope.args, serving) for price_key in price_keys
        ]
        await copy_plan_rows(session, PRICES, ("binding_ordinal", "price_key", "eligible", "minimum_rate"), records)
        after_price = price_keys[-1]


def _price_record(ordinal, price_key, prices, args, serving):
    filtered_prices = serving._ptg2_manifest_filter_prices(prices, dict(args))
    minimum = min(
        (
            rate
            for price in filtered_prices
            if (rate := serving._optional_decimal(price.get("negotiated_rate"))) is not None
        ),
        default=None,
    )
    return ordinal, price_key, bool(filtered_prices), minimum
