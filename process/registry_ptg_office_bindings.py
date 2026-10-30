# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""COPY exact approved provider offices after native retained/address correspondence."""

from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_site_source import _address_overlap_sql
from process.network_initial_source_office_bindings import _candidate, _copy_bindings, _validate_roles
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_copy import MembershipCopyTarget
from process.registry_ptg_office_capture import _canonical, _digest
from process.registry_ptg_office_membership import PTGOfficeMembershipBatch, read_ptg_office_membership_batch
from process.registry_retained_site_adoption import _source_addresses
from process.registry_source_office_origin import resolve_source_office_origin


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
    origin_manifest_sha256: str | None = None


async def _offices(connection, batch, address_source, after, control_schema, *, address_origin=None):
    if type(batch) is not PTGOfficeMembershipBatch or batch.unresolved_rows:
        raise ValueError("registry_ptg_office_binding_input_invalid")
    replay = await read_ptg_office_membership_batch(
        connection, batch.verified_source, batch.approved_source, after, max(1, batch.source_rows), control_schema
    )
    if replay != batch:
        raise ValueError("registry_ptg_office_binding_page_changed")
    office_records = [asdict(adoption) for adoption in batch.office_records]
    serving = batch.verified_source.serving
    retained, retained_oid, arguments = _source_addresses(serving, _identifier(serving.schema_name))
    _, conflict = _address_overlap_sql(retained, _identifier(address_source.schema_name), True)
    address_relation = (
        _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name)
        if address_origin is None
        else address_origin.relation_sql
    )
    joins = (
        f"LEFT JOIN {retained} retained USING(location_key) LEFT JOIN {address_relation} address USING(location_key)"
    )
    invalid = await connection.fetchval(
        f"""WITH selected AS MATERIALIZED (
      SELECT office.*,row_number() OVER() AS ordinal FROM jsonb_to_recordset($1::jsonb) AS office(provider_system text,provider_id text,
        location_id uuid,location_key text,entity_type text,entity_id text,address_row_sha256 text)
    ), matched AS (
      SELECT count(*) OVER(PARTITION BY site.ordinal) AS matches,
        address.location_key IS NULL OR ({conflict}) AS differs
      FROM selected site {joins}
    ) SELECT to_regnamespace($2)::oid IS NULL OR ({retained_oid}) IS DISTINCT FROM {serving.address_table_oid}
      OR EXISTS(SELECT 1 FROM matched WHERE matches<>1 OR differs)
      OR (SELECT count(*)<>count(DISTINCT (provider_system,provider_id,location_id)) FROM selected)""",
        _canonical(office_records).decode(),
        serving.schema_name,
        *arguments,
        *(() if address_origin is None else address_origin.arguments),
    )
    if invalid is not False or len(office_records) != batch.membership_rows:
        raise ValueError("registry_ptg_office_binding_unresolved")
    return office_records


async def capture_ptg_office_address_source(
    connection,
    batch,
    address_source,
    *,
    runtime_roles,
    after_ordinal=None,
    control_schema=None,
    origin_manifest_sha256=None,
):
    """Revalidate retained page and immutable address identity before candidate creation."""
    if type(address_source) is not PinnedAddressSource or address_source.table_name != "entity_address_unified":
        raise ValueError("registry_ptg_office_binding_source_invalid")
    _validate_roles(runtime_roles)
    _, _, retained_arguments = _source_addresses(
        batch.verified_source.serving, _identifier(batch.verified_source.serving.schema_name)
    )
    origin = await resolve_source_office_origin(
        connection,
        address_source,
        runtime_roles=runtime_roles,
        control_schema=_control_namespace(control_schema)[1:-1],
        origin_manifest_sha256=origin_manifest_sha256,
        first_parameter=3 + len(retained_arguments),
    )
    office_records = await _offices(
        connection, batch, address_source, after_ordinal, control_schema, address_origin=origin
    )
    pin = PinnedPTGOfficeAddressSource(
        address_source,
        origin.address_identity,
        batch.source.generation_id,
        batch.generation_id,
        batch.input_sha256,
        _digest(office_records),
        after_ordinal,
        len(office_records),
        origin_manifest_sha256,
    )
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
    return pin, bindings


async def pin_ptg_office_address_source(
    connection,
    batch,
    address_source,
    *,
    runtime_roles,
    after_ordinal=None,
    control_schema=None,
    origin_manifest_sha256=None,
):
    """Pin one native selected page without requiring an OPEN target candidate."""
    pin, _ = await capture_ptg_office_address_source(
        connection,
        batch,
        address_source,
        runtime_roles=runtime_roles,
        after_ordinal=after_ordinal,
        control_schema=control_schema,
        origin_manifest_sha256=origin_manifest_sha256,
    )
    return pin


async def copy_ptg_office_bindings(
    connection,
    batch,
    copy_target,
    pin,
    *,
    runtime_roles,
    after_ordinal=None,
    control_schema=None,
    origin_manifest_sha256=None,
):
    """Recheck exact candidate, page and address pins; COPY only reviewed offices."""
    if type(copy_target) is not MembershipCopyTarget or copy_target.schema_name in {
        pin.address_source.schema_name,
        batch.verified_source.descriptor.schema_name,
    }:
        raise ValueError("registry_ptg_office_binding_target_invalid")
    if type(pin) is not PinnedPTGOfficeAddressSource or after_ordinal != pin.after_ordinal:
        raise ValueError("registry_ptg_office_binding_pin_invalid")
    if origin_manifest_sha256 is not None and origin_manifest_sha256 != pin.origin_manifest_sha256:
        raise ValueError("registry_ptg_office_binding_pin_invalid")
    async with connection.transaction():
        await _candidate(
            connection,
            copy_target,
            batch,
            pin.address_source,
            _control_namespace(control_schema),
            origin_manifest_sha256=pin.origin_manifest_sha256,
        )
        observed, bindings = await capture_ptg_office_address_source(
            connection,
            batch,
            pin.address_source,
            runtime_roles=runtime_roles,
            after_ordinal=after_ordinal,
            control_schema=control_schema,
            origin_manifest_sha256=pin.origin_manifest_sha256,
        )
        if observed != pin:
            raise ValueError("registry_ptg_office_binding_pin_changed")
        await _copy_bindings(connection, copy_target, bindings)
    return {"office_count": len(bindings)}
