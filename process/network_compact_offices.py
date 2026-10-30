# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Match compact memberships against retained public and current custom offices."""

from dataclasses import dataclass

from process.network_address_projection import PinnedAddressSource, _identifier, _projection_columns
from process.network_compact_layout import CompactAddressRelation


class CompactOfficeSourceError(ValueError):
    """The exact physical office relations differ from the prepared candidate."""


@dataclass(frozen=True)
class CompactOfficeSources:
    base: PinnedAddressSource
    custom: CompactAddressRelation | None = None
    physical_base: CompactAddressRelation | None = None
    carriers: object | None = None

    def __post_init__(self):
        if (
            type(self.base) is not PinnedAddressSource
            or (self.custom is not None and type(self.custom) is not CompactAddressRelation)
            or (self.physical_base is not None and type(self.physical_base) is not CompactAddressRelation)
        ):
            raise CompactOfficeSourceError("Compact office source types differ")
        if self.carriers is not None:
            from process.network_compact_address_carriers import CompactAddressCarrierReceipt

            if type(self.carriers) is not CompactAddressCarrierReceipt:
                raise CompactOfficeSourceError("Compact carrier receipt type differs")


def compact_base_relation_sql(office_sources):
    """Use the authenticated retained name when the original alias has rotated."""
    base = office_sources.physical_base or office_sources.base
    if base.schema_name != office_sources.base.schema_name:
        raise CompactOfficeSourceError("Physical office namespace differs")
    return _identifier(base.schema_name) + "." + _identifier(base.table_name)


def compact_office_relation_sql(office_sources):
    """Render validated physical names; the caller supplies custody and native pins."""
    if type(office_sources) is not CompactOfficeSources:
        raise CompactOfficeSourceError("Exact compact office sources are required")
    base = compact_base_relation_sql(office_sources)
    query = f"SELECT * FROM {base} WHERE row_origin<>'network_custom'"
    if office_sources.custom is not None:
        custom = office_sources.custom
        query += f" UNION ALL SELECT * FROM {_identifier(custom.schema_name)}.{_identifier(custom.table_name)}"
    return "(" + query + ")"


async def _shape(connection, table_name):
    # LIKE can reset informational array dimensions without changing the native type.
    return await connection.fetch(
        "SELECT attname,atttypid::bigint,atttypmod,attnotnull,attcollation::bigint,attgenerated::text "
        "FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        table_name,
    )


async def pin_compact_office_sources(connection, copy_target, office_sources):
    """Check candidate storage without granting ownership or source authority.

    The caller independently verifies retained EAU custody and closes candidate
    writers and rederives the entire direct-plus-carrier contribution before custody.
    """
    if type(office_sources) is not CompactOfficeSources or not connection.is_in_transaction():
        raise CompactOfficeSourceError("Exact compact office sources and transaction are required")
    base = compact_base_relation_sql(office_sources)
    if office_sources.base.schema_name == copy_target.schema_name:
        raise CompactOfficeSourceError("Public addresses must be retained outside the candidate")
    await connection.execute(f"LOCK TABLE ONLY {base} IN ACCESS SHARE MODE NOWAIT")
    if (
        office_sources.physical_base is not None
        and await connection.fetchval("SELECT to_regclass($1)::oid::bigint", base)
        != office_sources.physical_base.table_oid
    ):
        raise CompactOfficeSourceError("Physical office identity changed")
    await _projection_columns(connection, base)
    base_shape = await _shape(connection, base)
    if "row_origin" not in {attribute["attname"] for attribute in base_shape}:
        raise CompactOfficeSourceError("Compact offices require explicit row origin")
    custom = office_sources.custom
    if custom is not None:
        await _pin_custom(connection, copy_target, custom, base, base_shape, office_sources.carriers)
    return compact_office_relation_sql(office_sources)


async def _pin_custom(connection, copy_target, custom, base_table, base_shape, carriers):
    if custom.schema_name != copy_target.schema_name or custom.table_name != "entity_address_unified":
        raise CompactOfficeSourceError("Custom offices must be the exact candidate contribution")
    table = _identifier(custom.schema_name) + "." + _identifier(custom.table_name)
    await connection.execute(f"LOCK TABLE ONLY {table} IN ACCESS SHARE MODE NOWAIT")
    if await connection.fetchval("SELECT to_regclass($1)::oid::bigint", table) != custom.table_oid:
        raise CompactOfficeSourceError("Custom office physical identity changed")
    await _projection_columns(connection, table)
    if await _shape(connection, table) != base_shape:
        raise CompactOfficeSourceError("Custom office column shape differs")
    has_carriers = False
    if carriers is not None:
        if carriers.target != copy_target or carriers.address_table_oid != custom.table_oid:
            raise CompactOfficeSourceError("Compact carrier physical scope differs")
        has_carriers = carriers.materialized_address_rows > 0
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {table} custom LEFT JOIN {base_table} base USING(location_key) "
        "WHERE (NOT $1 AND custom.row_origin IS DISTINCT FROM 'network_custom') OR base.row_origin<>'network_custom')",
        has_carriers,
    ):
        raise CompactOfficeSourceError("Custom contribution contains a public row or key collision")
