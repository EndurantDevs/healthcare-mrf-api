# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read immutable approved exact offices under the genuine composition snapshot."""

import hashlib
import json
from dataclasses import asdict, dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS
from process.registry_company_approval_fence import require_registry_company_approval_fence
from process.registry_ptg_office_capture import RegistryPTGOfficeCaptureContext, _canonical, _digest, _same_transaction
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore, _protected_store


class PTGOfficeBatchBoundsError(ValueError):
    """Reduce a native page before transferring any selected rows."""


@dataclass(frozen=True)
class _VerifiedOfficeSource:
    session: object
    driver: object
    transaction: object
    pin: PinnedPTGOfficeMembershipSource
    context: object
    request: object
    descriptor: object
    serving: object


@dataclass(frozen=True)
class PTGOfficeMembershipBatch:
    source: PinnedPTGOfficeMembershipSource
    approved_source: object
    verified_source: _VerifiedOfficeSource
    source_rows: int
    membership_rows: int
    omitted_rows: int
    unresolved_rows: int
    input_bytes: bytes
    input_sha256: str
    next_ordinal: int | None
    office_records: tuple

    @property
    def generation_id(self):
        """Bind exact office and current approved membership generations."""
        return _digest([self.source.generation_id, self.approved_source.generation_id])


async def _retained_documents(session, store, pin):
    tables = [
        await _protected_store(session, store, write=False, table_name=name)
        for name in ("registry_ptg_office_approval", "registry_ptg_office_hold")
    ]
    document_rows = (
        (
            await session.execute(
                text(f"""SELECT a.approval_sha256,a.approval_json,h.hold_sha256,h.hold_json
      FROM {tables[0]} a JOIN {tables[1]} h USING(capture_id,approval_sha256)
      WHERE a.capture_id=CAST(:capture AS uuid) LIMIT 2"""),
                {"capture": pin.capture_id},
            )
        )
        .mappings()
        .all()
    )
    if len(document_rows) != 1:
        raise ValueError("registry_ptg_office_recipe_unavailable")
    document_row = document_rows[0]
    document, hold = document_row["approval_json"], document_row["hold_json"]
    if (
        type(document) is not dict
        or type(hold) is not dict
        or len(_canonical(document)) > 131072
        or len(_canonical(hold)) > 32768
        or document_row["approval_sha256"] != pin.approval_sha256
        or _digest(document) != pin.approval_sha256
        or document_row["hold_sha256"] != pin.hold_sha256
        or _digest(hold) != pin.hold_sha256
        or document["capture_id"] != pin.capture_id
        or document["client_id"] != pin.client_id
        or document["manifest_sha256"] != pin.manifest_sha256
    ):
        raise ValueError("registry_ptg_office_recipe_changed")
    return document, hold


async def _recipe_context(session, store, document, pin, control_schema):
    from process.registry_ptg_office_approval import _request
    from process.registry_ptg_office_review_contract import validated_office_review_command
    from process.registry_ptg_published_office_scope import _graph_identity
    from process.registry_ptg_published_plan_contract import RegistryPTGPublishedPlanSourceSpecification
    from process.registry_ptg_published_plan_scope import read_registry_ptg_published_plan_scope

    command = validated_office_review_command(document["command"])
    source_by_field = command["source"]
    if (
        command["capture_id"] != pin.capture_id
        or command["client_id"] != pin.client_id
        or source_by_field["coordinates"] != asdict(pin.coordinates)
        or source_by_field["network_id"] != pin.network_id
        or command["retained_generation_id"] != pin.retained_generation_id
    ):
        raise ValueError("registry_ptg_office_recipe_changed")
    review = await read_registry_ptg_published_plan_scope(
        session,
        scope_id=command["scope_id"],
        client_id=pin.client_id,
        approval_sha256=command["scope_approval_sha256"],
        store=store,
    )
    actual = review["command"]
    identity = actual["published_identity"]
    context = RegistryPTGOfficeCaptureContext(
        UUID(command["scope_id"]),
        pin.client_id,
        command["scope_approval_sha256"],
        pin.coordinates,
        RegistryPTGPublishedPlanSourceSpecification(
            actual["scope_id"], actual["source"]["ptg_schema_name"], identity["snapshot_id"], identity["source_key"]
        ),
        review["evidence"]["source_authority"],
        _graph_identity(identity),
        store,
        control_schema,
        document["witness"]["custody"]["owner_role"],
        tuple(document["witness"]["custody"]["reader_roles"]),
    )
    return context, _request(command)


async def _verify_recipe_source(session, driver, pin, approved, store, control_schema, read_budget, transaction):
    from process.network_serving_read import resolve_network_serving_manifest
    from process.ptg_parts.result_archive_published_authority import lock_ptg_published_result_for_clone
    from process.registry_ptg_office_approval import _descriptor
    from process.registry_ptg_office_retention import office_hold_document
    from process.registry_ptg_office_witness import verify_registry_ptg_office_witness

    document, hold = await _retained_documents(session, store, pin)
    if document["command"]["source"]["approved_revision"] != approved.approved_revision:
        raise ValueError("registry_ptg_office_recipe_approval_changed")
    context, request = await _recipe_context(session, store, document, pin, control_schema)
    retained = await lock_ptg_published_result_for_clone(
        session, schema_name=context.source_specification.ptg_schema_name, authority=document["source_pin"]
    )
    if (
        retained.as_dict() != document["source_pin"]
        or document["source_pin"]["operation_id"] != "registry_ptg_office_review_" + UUID(pin.capture_id).hex
    ):
        raise ValueError("registry_ptg_office_recipe_source_changed")
    descriptor = await _descriptor(session, context, request)
    witness = await verify_registry_ptg_office_witness(session, context, request, descriptor, read_budget=read_budget)
    observed = witness.as_dict()
    if {name: field_value for name, field_value in observed.items() if name != "graph_budget"} != {
        name: field_value for name, field_value in document["witness"].items() if name != "graph_budget"
    } or office_hold_document(descriptor, type(witness)(_canonical(document["witness"])), pin.approval_sha256) != hold:
        raise ValueError("registry_ptg_office_recipe_witness_changed")
    serving = await resolve_network_serving_manifest(
        driver, generation_id=request.retained_generation_id, control_schema=control_schema
    )
    _same_transaction(session, driver, transaction)
    return _VerifiedOfficeSource(session, driver, transaction, pin, context, request, descriptor, serving)


async def require_ptg_office_recipe(driver, recipe, approved, control_schema, office_authority):
    """Require real fenced reader, immutable holds and fresh full source/office witness."""
    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget

    if type(office_authority) is not tuple or len(office_authority) != 3:
        raise ValueError("registry_ptg_office_recipe_owner_unavailable")
    session, store, read_budget = office_authority
    if (
        type(session) is not AsyncSession
        or type(store) is not RegistryPTGProducerScopeStore
        or type(read_budget) is not RegistryPTGGraphReadBudget
        or (store.control_schema or registry_schema()) != control_schema
    ):
        raise ValueError("registry_ptg_office_recipe_owner_unavailable")
    await require_registry_company_approval_fence(session, control_schema)
    if (await (await session.connection()).get_raw_connection()).driver_connection is not driver:
        raise ValueError("registry_ptg_office_recipe_owner_unavailable")
    transaction = session.get_transaction()
    _same_transaction(session, driver, transaction)
    original_path = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
    await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
    original_error = None
    try:
        return await _verify_recipe_source(
            session, driver, recipe.source_pin, approved, store, control_schema, read_budget, transaction
        )
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            await session.execute(
                text("SELECT pg_catalog.set_config('search_path',:path,true)"), {"path": original_path}
            )
        except BaseException:
            if original_error is None:
                raise


async def _office_page(driver, verified, after, limit):
    namespace = _identifier(verified.descriptor.schema_name)
    page = f"SELECT ordinal,provider_system,provider_id,location_id,location_key,address_row_sha256,evidence_id FROM {namespace}.office_assertion WHERE ordinal>$1 ORDER BY ordinal LIMIT $2"
    census = await driver.fetchrow(
        f"""WITH page AS MATERIALIZED ({page}) SELECT count(*) AS rows,
      coalesce(sum(6*(octet_length(provider_system)+octet_length(provider_id)+octet_length(location_key)
        +octet_length(address_row_sha256)+octet_length(evidence_id))+512),0)+2 AS bytes FROM page""",
        after or 0,
        limit,
    )
    if (
        type(census["rows"]) is not int
        or not 0 <= census["rows"] <= limit
        or type(census["bytes"]) is not int
        or not 2 <= census["bytes"] <= MAX_INPUT_BYTES
    ):
        raise PTGOfficeBatchBoundsError("registry_ptg_office_recipe_page_bounds")
    office_rows = await driver.fetch(page, after or 0, limit)
    if len(office_rows) != census["rows"] or any(
        office_row["ordinal"] != (after or 0) + index + 1 for index, office_row in enumerate(office_rows)
    ):
        raise ValueError("registry_ptg_office_recipe_page_changed")
    if not office_rows and (after or 0) != verified.request.input_row_count:
        raise ValueError("registry_ptg_office_recipe_page_changed")
    return office_rows


async def read_ptg_office_membership_batch(driver, verified, approved, after, limit, control_schema):
    """Bound transfer and verify exact retained locations for native COPY."""
    from process.registry_retained_site_adoption import resolve_retained_site_adoptions

    if type(verified) is not _VerifiedOfficeSource:
        raise ValueError("registry_ptg_office_recipe_owner_unavailable")
    _same_transaction(verified.session, driver, verified.transaction)
    if (
        verified.driver is not driver
        or type(limit) is not int
        or not 1 <= limit <= MAX_ROWS
        or type(after) not in {int, type(None)}
        or (after is not None and after < 0)
        or await pin_approved_membership_source(
            driver, approved_revision=approved.approved_revision, control_schema=control_schema
        )
        != approved
    ):
        raise ValueError("registry_ptg_office_recipe_page_changed")
    office_rows = await _office_page(driver, verified, after, limit)
    offices = [
        {
            name: str(office_row[name])
            for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
        }
        for office_row in office_rows
    ]
    receipt = (
        await resolve_retained_site_adoptions(
            driver, verified.serving, _canonical(offices), control_schema=control_schema
        )
        if office_rows
        else None
    )
    memberships = [
        {
            "network_id": verified.pin.network_id,
            **{
                name: str(office_row[name]) for name in ("provider_system", "provider_id", "location_id", "evidence_id")
            },
        }
        for office_row in office_rows
    ]
    raw = _canonical(memberships)
    if len(raw) > MAX_INPUT_BYTES:
        raise ValueError("registry_ptg_office_recipe_page_changed")
    return PTGOfficeMembershipBatch(
        verified.pin,
        approved,
        verified,
        len(office_rows),
        len(office_rows),
        0,
        0,
        raw,
        hashlib.sha256(raw).hexdigest(),
        office_rows[-1]["ordinal"] if office_rows else None,
        receipt.records if receipt else (),
    )
