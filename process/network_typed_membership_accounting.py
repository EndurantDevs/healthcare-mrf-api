# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-local accounting work for a complete typed compact candidate."""

from uuid import uuid4

from process.network_address_projection import NetworkAddressProjectionError, _identifier
from process.network_compact_offices import compact_bound_office_sql
from process.network_typed_slice_native import build_matched_facts
from process.registry_typed_office_parity import _KEYS, _has_pair_key_index, _upper_bound


async def _orphan_bindings(connection, bindings, addresses):
    """Check all binding keys, including unused aliases and the last partial page."""
    if not await _has_pair_key_index(connection, bindings, primary=True):
        raise ValueError("registry_typed_binding_domain_index_required")
    after, total = None, 0
    while (upper := await _upper_bound(connection, [f"SELECT {_KEYS} FROM {bindings}"], after)) is not None:
        low = "TRUE" if after is None else f"({_KEYS})>($1::text,$2::text,$3::uuid)"
        offset = 1 if after is None else 4
        scope = low + f" AND ({_KEYS})<=(${offset}::text,${offset + 1}::text,${offset + 2}::uuid)"
        total += await connection.fetchval(
            f"""WITH page AS MATERIALIZED (
              SELECT location_key,entity_type,entity_id FROM {bindings}
              WHERE {scope} ORDER BY {_KEYS})
            SELECT count(*) FROM page b WHERE NOT EXISTS(
              SELECT FROM {addresses} a WHERE
              (a.location_key,a.entity_type,a.entity_id)=(b.location_key,b.entity_type,b.entity_id))""",
            *((() if after is None else after) + upper),
        )
        after = upper
    return total


async def prepare_accounting_work(connection, schema, office_sources):
    """The caller pins offices and retains this work only in its own transaction."""
    work = _identifier("network_slice_work_" + uuid4().hex)
    addresses = compact_bound_office_sql(office_sources)
    counts = await build_matched_facts(connection, schema, addresses, work, None)
    counts["projected_locations"] = counts["matched_locations"]
    counts["orphan_bindings"] = await _orphan_bindings(connection, schema + ".provider_location_binding", addresses)
    if counts["invalid_memberships"]:
        raise NetworkAddressProjectionError("Selected memberships lack an exact provider/site/address binding")
    return work, counts
