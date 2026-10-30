# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare only approved direct manual/NPI offices, without cloning base addresses."""

from dataclasses import asdict, dataclass

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import (
    NetworkAddressProjectionError,
    PinnedAddressSource,
    _identifier,
    _projection_columns,
)
from process.network_custom_address_source import (
    _APPROVED_CTE,
    _INSERT_ADDRESS_SQL,
    NetworkCustomAddressSourceError,
    NetworkCustomAddressUnresolved,
    _approved_identity,
    _require_transaction,
    _source_identity,
)
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget


class CustomOfficeRowsUnsupported(NetworkCustomAddressSourceError):
    """Retained-source offices require their independent source selection carrier."""


@dataclass(frozen=True)
class CustomOfficeRowsReceipt:
    target: MembershipCopyTarget
    approved_revision: int
    base_source: PinnedAddressSource
    base_identity: tuple[int, int, int, str]
    npi_source: PinnedAddressSource | None
    npi_identity: tuple[int, int, int, str] | None
    approved_records_sha256: str
    custom_pair_count: int
    address_table_oid: int
    binding_table_oid: int

    def as_dict(self):
        """Return preparation evidence; the integrating path supplies custody."""
        return {"component": "network_custom_office_rows", "revision": 1, **asdict(self)}


def _validate_inputs(copy_target, base_source, approved_revision, runtime_roles, npi_source):
    if type(copy_target) is not MembershipCopyTarget or type(base_source) is not PinnedAddressSource:
        raise NetworkCustomAddressSourceError("Exact candidate and pinned base addresses are required")
    if type(approved_revision) is not int or not 0 <= approved_revision <= 9223372036854775807:
        raise NetworkCustomAddressSourceError("Approved custom revision is invalid")
    if npi_source is not None and (type(npi_source) is not PinnedAddressSource or npi_source.table_name != "npi"):
        raise NetworkCustomAddressSourceError("Pinned NPI source is invalid")
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or runtime_roles != tuple(sorted(set(runtime_roles)))
    ):
        raise NetworkCustomAddressSourceError("Runtime roles must be a sorted unique bounded tuple")
    for role in runtime_roles:
        _identifier(role)
    if base_source.schema_name == copy_target.schema_name or (
        npi_source is not None and npi_source.schema_name == copy_target.schema_name
    ):
        raise NetworkCustomAddressSourceError("Custom office sources must be external to the candidate")


async def _pin_source(connection, pinned_source, runtime_roles, *, address=False, control_schema=None):
    identity = await _source_identity(
        connection, pinned_source, runtime_roles, address=address, control_schema=control_schema
    )
    table = _identifier(pinned_source.schema_name) + "." + _identifier(pinned_source.table_name)
    await connection.execute(f"LOCK TABLE {table} IN ACCESS SHARE MODE NOWAIT")
    if await connection.fetchval("SELECT to_regclass($1)::oid::bigint", table) != identity[1]:
        raise NetworkCustomAddressSourceError("Retained source native identity differs")
    if address:
        await _projection_columns(connection, table)
    return identity


async def _direct_custom_identity(connection, namespace, revision):
    summary = await _approved_identity(connection, namespace, revision)
    direct_pairs = await connection.fetchval(
        _APPROVED_CTE.format(control=namespace) + "SELECT count(*) FROM custom", revision
    )
    if direct_pairs != summary["pairs"]:
        raise CustomOfficeRowsUnsupported("Direct custom offices do not accept retained-source site selections")
    return summary


async def _require_npi_pairs(connection, control, revision, npi_source):
    npi_check = (
        "false"
        if npi_source is None
        else (
            f"EXISTS(SELECT 1 FROM {_identifier(npi_source.schema_name)}.{_identifier(npi_source.table_name)} npi "
            "WHERE npi.npi=custom.provider_id::bigint)"
        )
    )
    missing_npi = await connection.fetchval(
        _APPROVED_CTE.format(control=control)
        + f"SELECT EXISTS(SELECT 1 FROM custom WHERE provider_system='npi' AND NOT ({npi_check}))",
        revision,
    )
    if missing_npi:
        raise NetworkCustomAddressUnresolved("Approved source NPI identity is unresolved")


async def _insert_custom_rows(connection, copy_target, base_source, npi_source, control, revision):
    namespace = _identifier(copy_target.schema_name)
    address_table, binding_table = (
        namespace + ".entity_address_unified",
        namespace + ".custom_provider_location_binding",
    )
    base_table = _identifier(base_source.schema_name) + "." + _identifier(base_source.table_name)
    await _require_npi_pairs(connection, control, revision, npi_source)
    await connection.execute(f"CREATE TABLE {address_table}(LIKE {base_table} INCLUDING DEFAULTS)")
    await connection.execute(f"ALTER TABLE {address_table} ADD PRIMARY KEY(location_key)")
    await connection.execute(f"""CREATE TABLE {binding_table}(
        provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
        location_key varchar(64) NOT NULL UNIQUE,entity_type text NOT NULL,entity_id text NOT NULL,
        PRIMARY KEY(provider_system,provider_id,location_id))""")
    await connection.execute(
        _APPROVED_CTE.format(control=control) + _INSERT_ADDRESS_SQL.format(address_table=address_table), revision
    )
    collision = await connection.fetchval(f"""SELECT EXISTS(SELECT 1 FROM {address_table} custom
        JOIN LATERAL(SELECT base.row_origin FROM {base_table} base
            WHERE base.location_key=custom.location_key LIMIT 1) base ON TRUE
        WHERE base.row_origin<>'network_custom')""")
    if collision:
        raise NetworkCustomAddressSourceError("Custom office key collides with a retained public address")
    await connection.execute(
        _APPROVED_CTE.format(control=control)
        + f"""INSERT INTO {binding_table}
        SELECT provider_system,provider_id,location_id,location_key,provider_system,provider_id FROM custom""",
        revision,
    )
    accounting = await connection.fetchrow(f"""SELECT (SELECT count(*) FROM {address_table}) AS address_rows,
        (SELECT count(*) FROM {binding_table}) AS binding_rows,
        EXISTS(SELECT 1 FROM {binding_table} binding LEFT JOIN {address_table} address
            USING(location_key,entity_type,entity_id) WHERE address.location_key IS NULL) AS orphan_bindings""")
    if accounting["address_rows"] != accounting["binding_rows"] or accounting["orphan_bindings"]:
        raise NetworkCustomAddressSourceError("Custom office address and exact binding accounting differs")
    return accounting["address_rows"]


async def _prepare_custom_rows(
    connection, copy_target, base_source, approved_revision, runtime_roles, npi_source, control
):
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, control)
        if (
            candidate["state"] not in {"sealed", "validated"}
            or candidate["approved_custom_revision"] != approved_revision
        ):
            raise NetworkCustomAddressSourceError("Custom offices require the exact approved sealed candidate")
        summary = await _direct_custom_identity(connection, control, approved_revision)
        base_identity = await _pin_source(
            connection, base_source, runtime_roles, address=True, control_schema=control[1:-1]
        )
        effective_npi = npi_source or (
            PinnedAddressSource(base_source.schema_name, "npi", base_source.generation_id)
            if summary["npi_pairs"]
            else None
        )
        npi_identity = None if effective_npi is None else await _pin_source(connection, effective_npi, runtime_roles)
        row_count = await _insert_custom_rows(
            connection, copy_target, base_source, effective_npi, control, approved_revision
        )
        if row_count != summary["pairs"]:
            raise NetworkCustomAddressSourceError("Approved direct custom office accounting differs")
        address_oid, binding_oid = await connection.fetchrow(
            "SELECT to_regclass($1)::oid::bigint,to_regclass($2)::oid::bigint",
            copy_target.schema_name + ".entity_address_unified",
            copy_target.schema_name + ".custom_provider_location_binding",
        )
    return CustomOfficeRowsReceipt(
        copy_target,
        approved_revision,
        base_source,
        base_identity,
        effective_npi,
        npi_identity,
        summary["fingerprint"],
        row_count,
        address_oid,
        binding_oid,
    )


async def prepare_custom_office_rows(
    connection, copy_target, base_source, *, approved_revision, runtime_roles, npi_source=None, control_schema=None
):
    """Prepare direct manual/NPI rows before exact-address validation and final freeze.

    Retained-site selections and PTG/ACA recipe carries require separate verified
    carriers and are outside this helper. The caller owns a repeatable-read
    transaction and must later merge/drop custom_provider_location_binding and
    seal both heaps under candidate custody. No base rows are copied. Composition
    must exclude all historical base row_origin='network_custom' rows; a current
    generated key cannot replace a public base key. This receipt grants no authority.
    """
    _validate_inputs(copy_target, base_source, approved_revision, runtime_roles, npi_source)
    await _require_transaction(connection)
    control = _identifier(control_schema or registry_schema())
    try:
        return await _prepare_custom_rows(
            connection, copy_target, base_source, approved_revision, runtime_roles, npi_source, control
        )
    except (asyncpg.PostgresError, NetworkAddressProjectionError) as error:
        raise NetworkCustomAddressSourceError("Custom office native preparation failed") from error
