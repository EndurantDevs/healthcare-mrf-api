# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native allocation of enduring integer network identities."""

from uuid import UUID

from sqlalchemy import MetaData, select
from sqlalchemy.dialects.postgresql import insert

from db.models.network_registry import NetworkRegistryIdentity

MAX_ALLOCATION_ROWS = 5000


async def allocate_network_ids(session, allocation_keys, *, schema=None):
    """Participate in the caller transaction; names and checksums never allocate IDs."""
    if not session.in_transaction():
        raise ValueError("network_allocation_requires_transaction")
    if not isinstance(allocation_keys, (list, tuple)) or len(allocation_keys) > MAX_ALLOCATION_ROWS:
        raise ValueError("network_allocation_batch_limit")
    keys = set()
    for key in allocation_keys:
        if not isinstance(key, UUID) or key.int == 0:
            raise ValueError("network_allocation_key_invalid")
        keys.add(key)
    if not keys:
        return {}
    table = NetworkRegistryIdentity.__table__
    if schema is not None:
        table = table.to_metadata(MetaData(), schema=schema)
    statement = insert(table).values([{"allocation_key": key} for key in sorted(keys)])
    statement = statement.on_conflict_do_nothing(index_elements=[table.c.allocation_key])
    statement = statement.returning(table.c.allocation_key, table.c.network_id)
    network_ids_by_key = dict((await session.execute(statement)).all())
    missing = keys.difference(network_ids_by_key)
    if missing:
        existing = await session.execute(
            select(table.c.allocation_key, table.c.network_id).where(table.c.allocation_key.in_(missing))
        )
        network_ids_by_key.update(existing.all())
    if set(network_ids_by_key) != keys or any(
        type(network_id) is not int or not 0 < network_id <= 2147483647 for network_id in network_ids_by_key.values()
    ):
        raise RuntimeError("network_allocation_incomplete")
    return network_ids_by_key
