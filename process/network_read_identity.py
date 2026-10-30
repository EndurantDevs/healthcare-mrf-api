# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check already consumed physical relations before exposing a pinned read result."""

from process.network_compact_layout import CompactAddressRelation, compact_identifier

MAX_CONSUMED_RELATIONS = 4096
_RELATION_LOCK_MODES = (
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
)
_CONSUMED_SQL = """WITH expected AS MATERIALIZED (
    SELECT * FROM unnest($1::text[],$2::bigint[]) AS pins(relation_name,relation_oid)
), held AS MATERIALIZED (
    SELECT relation::bigint AS relation_oid FROM pg_catalog.pg_locks
    WHERE pid=pg_backend_pid() AND database=(
      SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
    AND locktype='relation' AND relation::bigint=ANY($2::bigint[])
    AND granted AND mode=ANY($4::text[])
)
SELECT pg_backend_pid()=$3::integer AS backend_matches,count(*)::integer AS relation_count,
    coalesce(bool_and(coalesce(pg_catalog.to_regclass(expected.relation_name)::oid::bigint
      =expected.relation_oid,false) AND EXISTS(
        SELECT FROM held WHERE held.relation_oid=expected.relation_oid)),false) AS valid
FROM expected"""
_BOUND_NAMES_SQL = """WITH expected AS MATERIALIZED (
    SELECT * FROM unnest($1::text[],$2::bigint[]) AS pins(relation_name,relation_oid)
)
SELECT pg_backend_pid()=$3::integer AS backend_matches,count(*)::integer AS relation_count,
    coalesce(bool_and(pg_catalog.to_regclass(expected.relation_name)::oid::bigint
      IS NOT DISTINCT FROM expected.relation_oid),false) AS valid
FROM expected"""


class NetworkReadIdentityError(RuntimeError):
    """The consumed physical relations or their original backend cannot be verified."""


def _require(condition):
    if not condition:
        raise NetworkReadIdentityError("network_read_identity_invalid")


def _relation_pins(relations, expected_backend_pid):
    _require(type(relations) is tuple and len(relations) <= MAX_CONSUMED_RELATIONS)
    _require(type(expected_backend_pid) is int and 0 < expected_backend_pid <= 2147483647)
    oid_by_name = {}
    name_by_oid = {}
    for relation in relations:
        _require(type(relation) is CompactAddressRelation)
        CompactAddressRelation.__post_init__(relation)
        name = compact_identifier(relation.schema_name) + "." + compact_identifier(relation.table_name)
        _require(name not in oid_by_name or oid_by_name[name] == relation.table_oid)
        _require(relation.table_oid not in name_by_oid or name_by_oid[relation.table_oid] == name)
        oid_by_name[name] = relation.table_oid
        name_by_oid[relation.table_oid] = name
    return oid_by_name


async def verify_consumed_network_relations(
    connection,
    relations: tuple[CompactAddressRelation, ...],
    *,
    expected_backend_pid: int,
) -> None:
    """Verify after actual data consumption, on its original active transaction/backend.

    The caller must retain the exact data transaction, pin its PID before the
    query, and guard before returning or caching the result. These checks cannot
    establish transaction continuity. Custody/catalog/role authority is external.
    Empty pins touch no connection and prove no transaction or data consumption.
    """
    try:
        oid_by_name = _relation_pins(relations, expected_backend_pid)
        if not oid_by_name:
            return
        _require(connection.is_in_transaction() is True)
        receipt = await connection.fetchrow(
            _CONSUMED_SQL,
            list(oid_by_name),
            list(oid_by_name.values()),
            expected_backend_pid,
            list(_RELATION_LOCK_MODES),
        )
        _require(connection.is_in_transaction() is True and receipt is not None)
        _require(type(receipt["relation_count"]) is int and receipt["relation_count"] == len(oid_by_name))
        _require(receipt["backend_matches"] is True and receipt["valid"] is True)
    except NetworkReadIdentityError:
        raise
    except Exception as error:
        raise NetworkReadIdentityError("network_read_identity_invalid") from error


def _bound_name_pins(relations, absent_names, expected_backend_pid):
    oid_by_name = _relation_pins(relations, expected_backend_pid)
    _require(type(absent_names) is tuple and len(relations) + len(absent_names) <= MAX_CONSUMED_RELATIONS)
    for coordinates in absent_names:
        _require(type(coordinates) is tuple and len(coordinates) == 2)
        name = compact_identifier(coordinates[0]) + "." + compact_identifier(coordinates[1])
        _require(name not in oid_by_name or oid_by_name[name] is None)
        oid_by_name[name] = None
    return oid_by_name


async def verify_bound_network_names(connection, relations, *, expected_backend_pid, absent_names=()):
    """Check a complete fixed-name inventory after a buffered data query.

    This does not prove consumption. The trusted caller must inventory every
    fully qualified name its queries resolve, preserve the original transaction,
    and check before exposing or caching data. A wrong consumed heap's automatic
    SELECT lock prevents restoring its name before this check. Unused names may
    conservatively refuse a read. OID-addressed/dynamic queries and mutable view
    definitions require independent protection; existing custody stays required.
    """
    try:
        oid_by_name = _bound_name_pins(relations, absent_names, expected_backend_pid)
        if not oid_by_name:
            return
        _require(connection.is_in_transaction() is True)
        receipt = await connection.fetchrow(
            _BOUND_NAMES_SQL, list(oid_by_name), list(oid_by_name.values()), expected_backend_pid
        )
        _require(connection.is_in_transaction() is True and receipt is not None)
        _require(type(receipt["relation_count"]) is int and receipt["relation_count"] == len(oid_by_name))
        _require(receipt["backend_matches"] is True and receipt["valid"] is True)
    except NetworkReadIdentityError:
        raise
    except Exception as error:
        raise NetworkReadIdentityError("network_read_identity_invalid") from error
