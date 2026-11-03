# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native transfer for one resource whose complete witness fanout exceeds a page."""

import json
import re

from process import network_fhir_membership_source as fhir
from process.network_address_projection import _identifier
from process.registry_source_contribution_store import (
    RegistrySourceContributionError,
    _canonical,
    _unique_object,
    decode_fhir_source_witness_page_v2,
)
from process.uhc_flex_practitioner_async_safety import drain_operation


def _require_stream_scope(
    connection, source_pin, registry_schema, after_resource, approved_source, binding_coordinates, copy_position
):
    if type(source_pin) is not fhir.PinnedFHIRMembershipSource or not connection.is_in_transaction():
        raise fhir.FHIRMembershipSourceError("Pinned FHIR reads require a caller transaction")
    fhir._validate_witness_cursor(after_resource)
    _identifier(registry_schema)
    table_name, recipe_sha256, after_ordinal = copy_position
    _identifier(table_name)
    if (
        type(recipe_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", recipe_sha256) is None
        or type(after_ordinal) is not int
        or not 0 <= after_ordinal < 2**63
    ):
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
    return fhir._reviewed_descriptor(source_pin, approved_source, binding_coordinates, True)


def _decode_witness_document(document_record):
    raw = document_record["fact"]
    if type(raw) is not str or len(raw.encode()) > fhir.MAX_INPUT_BYTES:
        raise fhir.FHIRMembershipBatchBoundsError("FHIR source witness fact exceeds bounds")
    try:
        fact = json.loads(raw, object_pairs_hook=_unique_object)
    except ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid") from None
    if type(fact) is not dict or (fact.get("resource_type"), fact.get("resource_id")) != (
        document_record["resource_type"],
        document_record["resource_id"],
    ):
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
    return fact


def _witness_frame(source_pin, header, documents):
    position = header.get("resource_type"), header.get("resource_id")
    return fhir.FHIRSourceWitnessPageV2(source_pin, 1, position, _canonical(documents).encode())


async def _stream_witness_frames(connection, source_pin, after_resource):
    header, frame_documents, frame_bytes = None, [], 2
    # At most 32 bounded documents (256 MiB) enter the driver's receive buffer.
    async for document_record in connection.cursor(
        fhir._source_witness_stream_sql_v2(source_pin),
        *fhir._witness_query_arguments(source_pin, after_resource, 1),
        prefetch=32,
    ):
        fact = _decode_witness_document(document_record)
        if header is None:
            if fact.get("kind") != "resource":
                raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
            header = fact
            page = _witness_frame(source_pin, header, [header])
            decode_fhir_source_witness_page_v2(page)
            if after_resource is not None and page.next_resource <= after_resource:
                raise fhir.FHIRMembershipSourceError("FHIR source cursor is invalid")
            frame_documents, frame_bytes = [header], len(page.source_fact_bytes)
            continue
        if fact.get("kind") != "edge":
            raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
        size = len(_canonical(fact).encode()) + 1
        if len(frame_documents) == 5000 or frame_bytes + size > fhir.MAX_INPUT_BYTES:
            yield _witness_frame(source_pin, header, frame_documents)
            frame_documents = [header]
            frame_bytes = len(_canonical(frame_documents).encode())
        if frame_bytes + size > fhir.MAX_INPUT_BYTES:
            raise fhir.FHIRMembershipBatchBoundsError("FHIR source witness fact exceeds bounds")
        frame_documents.append(fact)
        frame_bytes += size
    if header is None:
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
    yield _witness_frame(source_pin, header, frame_documents)


async def _copy_witness_frame(connection, page, copy_position, *, has_copied_header):
    table_name, recipe_sha256, ordinal = copy_position
    documents = decode_fhir_source_witness_page_v2(page)
    if has_copied_header:
        documents = documents[1:]
    if ordinal >= 2**63 - len(documents):
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
    fact_records = [(recipe_sha256, ordinal + index, _canonical(fact)) for index, fact in enumerate(documents, 1)]
    if fact_records:
        status = await drain_operation(
            connection.copy_records_to_table(
                table_name,
                schema_name="pg_temp",
                columns=("recipe_sha256", "ordinal", "fact_json"),
                records=fact_records,
            ),
            preserve_cancellation=True,
        )
        if status != f"COPY {len(fact_records)}":
            raise RegistrySourceContributionError("registry_source_contribution_accounting_invalid")
    return ordinal + len(fact_records)


async def copy_fhir_witness_resource_v2(
    connection,
    source_pin,
    *,
    registry_schema,
    after_resource,
    approved_source,
    binding_coordinates,
    retained_read,
    copy_position,
):
    """Drain one resource, storing its header once, then recheck authority before advancing.

    The savepoint removes all copied chunks and closes its cursor on any late failure.
    """
    descriptor = _require_stream_scope(
        connection, source_pin, registry_schema, after_resource, approved_source, binding_coordinates, copy_position
    )
    table_name, recipe_sha256, ordinal = copy_position
    async with connection.transaction():
        await fhir._require_source_recipe(
            connection, source_pin, descriptor, approved_source, binding_coordinates, registry_schema, retained_read
        )
        has_copied_header = False
        async for page in _stream_witness_frames(connection, source_pin, after_resource):
            ordinal = await _copy_witness_frame(
                connection, page, (table_name, recipe_sha256, ordinal), has_copied_header=has_copied_header
            )
            has_copied_header = True
        await fhir._require_source_recipe(
            connection, source_pin, descriptor, approved_source, binding_coordinates, registry_schema, retained_read
        )
    return page.next_resource, ordinal
