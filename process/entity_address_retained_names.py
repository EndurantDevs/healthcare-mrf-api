# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable names for a retained address generation and its complete table family."""

import hashlib
import json
import re

from process.entity_address_result_generation import validate_entity_address_serving_generation
from process.network_address_projection import _identifier


def retained_address_name(table_name, serving_generation):
    """Use the published origin, which survives a database copy, to name old rows."""
    if not isinstance(table_name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", table_name) is None:
        raise ValueError("retained address table name is invalid")
    generation = validate_entity_address_serving_generation(serving_generation).as_dict()
    encoded = json.dumps({"table": table_name, "generation": generation}, sort_keys=True, separators=(",", ":"))
    suffix = hashlib.sha256(encoded.encode("ascii")).hexdigest()[:24]
    return table_name[:29] + "_retained_" + suffix


async def has_matching_live_address_generation(connection, schema_name, table_name, serving_generation):
    """Bind a locked live fallback to its logical origin in the reader's snapshot."""
    expected = validate_entity_address_serving_generation(serving_generation)
    schema, table = _identifier(schema_name), _identifier(table_name)
    # A replacement renamed after RR began still has its stage name in that snapshot.
    origin_by_field = await connection.fetchrow(
        f"SELECT origin_lineage_id,origin_generation,published_at FROM {schema}.entity_address_result_generation "
        "WHERE singleton IS TRUE AND EXISTS(SELECT FROM pg_catalog.pg_class relation "
        "JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace "
        "WHERE relation.oid=pg_catalog.to_regclass($1::text) "
        "AND relation.relname=$2::text AND namespace.nspname=$3::text)",
        schema + "." + table,
        table_name,
        schema_name,
    )
    if origin_by_field is None:
        return False
    try:
        return validate_entity_address_serving_generation(dict(origin_by_field)) == expected
    except ValueError:
        return False
