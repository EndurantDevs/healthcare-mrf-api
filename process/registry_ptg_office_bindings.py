# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""COPY exact approved provider offices after native retained/address correspondence."""

from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import _source_identity
from process.network_initial_source_office_bindings import _candidate, _copy_bindings, _validate_roles
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_copy import MembershipCopyTarget
from process.registry_ptg_office_capture import _canonical, _digest
from process.registry_ptg_office_membership import PTGOfficeMembershipBatch, read_ptg_office_membership_batch


@dataclass(frozen=True)
class PinnedPTGOfficeAddressSource:
    address_source: PinnedAddressSource
    address_identity: tuple
    source_generation_id: str
    membership_generation_id: str
    membership_input_sha256: str
    selected_offices_sha256: str
    after_ordinal: int | None
    office_count: int


async def _offices(connection, batch, address_source, after, control_schema):
    if type(batch) is not PTGOfficeMembershipBatch or batch.unresolved_rows:
        raise ValueError("registry_ptg_office_binding_input_invalid")
    replay = await read_ptg_office_membership_batch(
        connection, batch.verified_source, batch.approved_source, after, max(1, batch.source_rows), control_schema
    )
    if replay != batch:
        raise ValueError("registry_ptg_office_binding_page_changed")
    records = [asdict(record) for record in batch.office_records]
    address = f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
    invalid = await connection.fetchval(
        f"""WITH selected AS MATERIALIZED (
      SELECT * FROM jsonb_to_recordset($1::jsonb) AS office(provider_system text,provider_id text,
        location_id uuid,location_key text,entity_type text,entity_id text,address_row_sha256 text)
    ) SELECT EXISTS(SELECT 1 FROM selected office LEFT JOIN {address} actual
      ON (actual.location_key,actual.entity_type,actual.entity_id)=
         (office.location_key,office.entity_type,office.entity_id)
      WHERE actual.location_key IS NULL OR actual.address_key IS DISTINCT FROM office.location_id
        OR encode(sha256(convert_to(to_jsonb(actual)::text,'UTF8')),'hex') IS DISTINCT FROM office.address_row_sha256)""",
        _canonical(records).decode(),
    )
    if invalid is not False or len(records) != batch.membership_rows:
        raise ValueError("registry_ptg_office_binding_unresolved")
    return records


async def pin_ptg_office_address_source(
    connection, batch, address_source, *, runtime_roles, after_ordinal=None, control_schema=None
):
    """Revalidate retained page and immutable address identity before candidate creation."""
    if type(address_source) is not PinnedAddressSource or address_source.table_name != "entity_address_unified":
        raise ValueError("registry_ptg_office_binding_source_invalid")
    _validate_roles(runtime_roles)
    identity = await _source_identity(connection, address_source, runtime_roles, address=True)
    records = await _offices(connection, batch, address_source, after_ordinal, control_schema)
    return PinnedPTGOfficeAddressSource(
        address_source,
        identity,
        batch.source.generation_id,
        batch.generation_id,
        batch.input_sha256,
        _digest(records),
        after_ordinal,
        len(records),
    )


async def copy_ptg_office_bindings(
    connection, batch, copy_target, pin, *, runtime_roles, after_ordinal=None, control_schema=None
):
    """Recheck exact candidate, page and address pins; COPY only reviewed offices."""
    if type(copy_target) is not MembershipCopyTarget or copy_target.schema_name in {
        pin.address_source.schema_name,
        batch.verified_source.descriptor.schema_name,
    }:
        raise ValueError("registry_ptg_office_binding_target_invalid")
    if type(pin) is not PinnedPTGOfficeAddressSource or after_ordinal != pin.after_ordinal:
        raise ValueError("registry_ptg_office_binding_pin_invalid")
    async with connection.transaction():
        await _candidate(connection, copy_target, batch, pin.address_source, _control_namespace(control_schema))
        observed = await pin_ptg_office_address_source(
            connection,
            batch,
            pin.address_source,
            runtime_roles=runtime_roles,
            after_ordinal=after_ordinal,
            control_schema=control_schema,
        )
        if observed != pin:
            raise ValueError("registry_ptg_office_binding_pin_changed")
        office_records = await _offices(connection, batch, pin.address_source, after_ordinal, control_schema)
        bindings = [
            (
                office["provider_system"],
                office["provider_id"],
                UUID(office["location_id"]),
                office["location_key"],
                office["entity_type"],
                office["entity_id"],
            )
            for office in office_records
        ]
        await _copy_bindings(connection, copy_target, bindings)
    return {"office_count": len(bindings)}
