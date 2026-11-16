# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native distinct-input keys for the existing shared address stamping flow."""

from uuid import uuid4

from process.ext import address_batch_stage, address_canon


async def stamp_address_key_shard(
    database, shard, *, schema, staging_table, field_map, shards, update_existing, cancel_check=None
):
    """Preserve one atomic shard UPDATE while preparing repeated inputs only once."""
    table = address_canon._qtable(schema, staging_table)
    filters = [] if update_existing else ["address_key IS NULL"]
    if shards > 1:
        filters.append("mod(abs(hashtext(ctid::text)::bigint), :shards) = :shard")
    where = " WHERE " + " AND ".join(filters) if filters else ""
    projection = address_batch_stage.input_projection_sql(address_canon._address_sql(schema, field_map))
    source_sql = f"SELECT {projection} FROM {table}{where}"
    parameters_by_name = {"shards": shards, "shard": shard}
    async with address_batch_stage.staged_address_fields(
        database,
        schema,
        "address_stamp_" + uuid4().hex,
        source_sql,
        parameters=parameters_by_name,
        cancel_check=cancel_check,
        temporary=True,
    ) as prepared:
        if prepared is None:
            return None
        input_fields = ["source." + field for field in address_batch_stage.ADDRESS_FIELDS]
        fallback = f"{address_canon._quote_ident(schema)}.addr_key_v1({','.join(input_fields)})::text"
        computed = address_batch_stage.field_sql("prepared", "address_key", fallback)
        lookup = address_batch_stage.lookup_key_sql(input_fields)
        update_filter = (
            "target.address_key IS DISTINCT FROM stamped.computed_address_key"
            if update_existing
            else "target.address_key IS NULL AND stamped.computed_address_key IS NOT NULL"
        )
        return await database.status(
            f"WITH source AS (SELECT ctid AS target_row,{projection} FROM {table}{where}), "
            f"stamped AS (SELECT source.target_row,CAST({computed} AS uuid) AS computed_address_key "
            f"FROM source LEFT JOIN {prepared} prepared ON prepared.lookup_key={lookup}) "
            f"UPDATE {table} target SET address_key=stamped.computed_address_key FROM stamped "
            f"WHERE target.ctid=stamped.target_row AND {update_filter}",
            **parameters_by_name,
        )
