# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare and independently verify the current direct custom office contribution."""

import json
from dataclasses import asdict, dataclass
from uuid import UUID, uuid4

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_custom_address_source import (
    _APPROVED_CTE,
    _INSERT_ADDRESS_SQL,
    NetworkCustomAddressSourceError,
    _approved_identity,
    _require_transaction,
)
from process.network_custom_office_rows import (
    _insert_custom_rows,
    _pin_source,
    _require_npi_pairs,
    _validate_inputs,
)
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget

_COMPONENT = "network_compact_custom_offices"
_MAX_BYTES = 256 * 1024
_BINDINGS = {"custom_provider_location_binding", "provider_location_binding"}
_BINDING_COLUMNS = "provider_system,provider_id,location_id,location_key,entity_type,entity_id"


def _require(condition, reason):
    if not condition:
        raise NetworkCustomAddressSourceError("compact_custom_offices_" + reason)


@dataclass(frozen=True)
class CompactCustomOfficeReceipt:
    target: MembershipCopyTarget
    control_schema: str
    approved_revision: int
    base_source: PinnedAddressSource
    base_artifact_sha256: str
    npi_source: PinnedAddressSource | None
    approved_records_sha256: str
    custom_pair_count: int
    address_relation: CompactAddressRelation | None
    binding_relation: CompactAddressRelation | None
    revision: int = 2

    def as_dict(self):
        """Serialize bounded logical inputs and independently checked row accounting."""
        document = json.loads(json.dumps({"component": _COMPONENT, **asdict(self)}))
        parse_compact_custom_office_receipt(document)
        return document


def _closed(document, names):
    _require(type(document) is dict and set(document) == set(names), "receipt_invalid")


def _integer(value, minimum=0, maximum=9223372036854775807):
    _require(type(value) is int and minimum <= value <= maximum, "receipt_invalid")
    return value


def _digest(value):
    _require(type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value), "receipt_invalid")
    return value


def _parse_source(document):
    _closed(document, ("schema_name", "table_name", "generation_id"))
    return PinnedAddressSource(**document)


def _relation(document, target, allowed):
    if document is None:
        return None
    _closed(document, ("schema_name", "table_name"))
    relation = CompactAddressRelation(**document)
    _require(relation.schema_name == target.schema_name and relation.table_name in allowed, "receipt_invalid")
    return relation


def parse_compact_custom_office_receipt(document_by_field):
    """Decode current names and the exact historical receipt layout."""
    fields = {"component", *CompactCustomOfficeReceipt.__dataclass_fields__}
    legacy = (fields - {"address_relation", "binding_relation"}) | {
        "runtime_roles",
        "npi_identity",
        "address_table_oid",
        "binding_table_oid",
    }
    _require(type(document_by_field) is dict and set(document_by_field) in (fields, legacy), "receipt_invalid")
    try:
        _require(len(json.dumps(document_by_field, allow_nan=False).encode()) <= _MAX_BYTES, "receipt_too_large")
        _require(
            document_by_field["component"] == _COMPONENT and type(document_by_field["revision"]) is int,
            "receipt_invalid",
        )
        copy_target = document_by_field["target"]
        _closed(copy_target, ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name", "table_name"))
        copy_target = MembershipCopyTarget(**copy_target)
        count = _integer(document_by_field["custom_pair_count"])
        if set(document_by_field) == legacy:
            _require(document_by_field["revision"] == 1, "receipt_invalid")
            has_binding = document_by_field["binding_table_oid"] is not None
            document_by_field = {key: field_value for key, field_value in document_by_field.items() if key in fields}
            document_by_field["address_relation"] = (
                asdict(CompactAddressRelation(copy_target.schema_name, "entity_address_unified")) if count else None
            )
            document_by_field["binding_relation"] = (
                asdict(CompactAddressRelation(copy_target.schema_name, "provider_location_binding"))
                if has_binding
                else None
            )
        _require(document_by_field["revision"] in (1, 2), "receipt_invalid")
        base = _parse_source(document_by_field["base_source"])
        npi = None if document_by_field["npi_source"] is None else _parse_source(document_by_field["npi_source"])
        revision = _integer(document_by_field["approved_revision"])
        _validate_inputs(copy_target, base, revision, (), npi)
        _require(base.table_name == "entity_address_unified", "current_base_required")
        _identifier(document_by_field["control_schema"])
        address = _relation(document_by_field["address_relation"], copy_target, {"entity_address_unified"})
        binding = _relation(document_by_field["binding_relation"], copy_target, _BINDINGS)
        _require((address is None) == (count == 0) and (count == 0 or binding is not None), "receipt_invalid")
        return CompactCustomOfficeReceipt(
            copy_target,
            document_by_field["control_schema"],
            revision,
            base,
            _digest(document_by_field["base_artifact_sha256"]),
            npi,
            _digest(document_by_field["approved_records_sha256"]),
            count,
            address,
            binding,
            document_by_field["revision"],
        )
    except (TypeError, ValueError, KeyError) as error:
        raise NetworkCustomAddressSourceError("compact_custom_offices_receipt_invalid") from error


async def _candidate(connection, target, base, revision, control, *, verification=False):
    if verification:
        candidate = await connection.fetchrow(
            f"SELECT * FROM {control}.network_membership_candidate WHERE candidate_id=$1",
            UUID(target.candidate_id),
        )
        _require(
            candidate is not None
            and all(
                str(candidate[field]) == getattr(target, field)
                for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
            ),
            "candidate_scope_invalid",
        )
    else:
        candidate = await _locked_candidate(connection, target, control)
    generations = candidate["source_generations"]
    if type(generations) is str:
        generations = json.loads(generations)
    _require(
        candidate["state"]
        in ({"open", "sealed", "validated", "ready"} if verification else {"open", "sealed", "validated"})
        and candidate["approved_custom_revision"] == revision
        and generations.get("unified_address") == base.generation_id,
        "candidate_scope_invalid",
    )
    return candidate


async def _capture_inputs(connection, base, revision, roles, npi_source, control, candidate):
    from process.network_compact_address_origin import resolve_compact_address_origin

    summary = await _compact_direct_identity(connection, control, revision)
    base_artifact, physical = await resolve_compact_address_origin(
        connection, candidate, base, runtime_roles=roles, control_schema=control[1:-1]
    )
    from process import network_indexed_address_artifact as indexed

    if npi_source is None and base_artifact.get("contract") == indexed.CONTRACT:
        original = indexed.original_address_source(base_artifact)
        npi_source = PinnedAddressSource(original.schema_name, "npi", original.generation_id)
    effective_npi = (
        await _capture_npi(connection, base.schema_name, npi_source, roles, base.generation_id)
        if summary["npi_pairs"]
        else None
    )
    return summary, base_artifact["descriptor_sha256"], effective_npi, physical


async def _compact_direct_identity(connection, control, revision):
    summary_by_field = dict(await _approved_identity(connection, control, revision))
    direct = await connection.fetchrow(
        _APPROVED_CTE.format(control=control)
        + "SELECT count(*) AS pairs,count(*) FILTER(WHERE provider_system='npi') AS npi_pairs FROM custom",
        revision,
    )
    return {**summary_by_field, **dict(direct)}


async def _capture_npi(connection, default_schema, source, roles, default_generation):
    from process.network_npi_artifact import capture_network_npi_artifact
    from process.npi_result_generation import TABLE_NAME

    schema = default_schema if source is None else source.schema_name
    native = await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", _identifier(schema) + "." + TABLE_NAME)
    if native:
        captured, _reference = await capture_network_npi_artifact(
            connection, schema, runtime_roles=roles, generation_id=None if source is None else source.generation_id
        )
        _require(source is None or source == captured, "npi_generation_changed")
        return captured
    pinned = source or PinnedAddressSource(schema, "npi", default_generation)
    await _pin_source(connection, pinned, roles)
    return pinned


async def prepare_compact_custom_offices(
    connection, copy_target, base_source, *, approved_revision, runtime_roles, npi_source=None, control_schema=None
):
    """Prepare approved direct manual and NPI rows from their logical source editions."""
    _validate_inputs(copy_target, base_source, approved_revision, runtime_roles, npi_source)
    await _require_transaction(connection)
    control_schema = control_schema or registry_schema()
    control = _identifier(control_schema)
    try:
        async with connection.transaction():
            candidate = await _candidate(connection, copy_target, base_source, approved_revision, control)
            summary, base_digest, npi, physical = await _capture_inputs(
                connection, base_source, approved_revision, runtime_roles, npi_source, control, candidate
            )
            count = summary["pairs"]
            address_relation = binding_relation = None
            if count:
                actual = await _insert_custom_rows(
                    connection,
                    copy_target,
                    PinnedAddressSource(physical.schema_name, physical.table_name, base_source.generation_id),
                    npi,
                    control,
                    approved_revision,
                )
                _require(actual == count, "row_accounting_differs")
                address_relation = CompactAddressRelation(copy_target.schema_name, "entity_address_unified")
                binding_relation = CompactAddressRelation(copy_target.schema_name, "custom_provider_location_binding")
            else:
                await _empty_heap(connection, copy_target)
            receipt = CompactCustomOfficeReceipt(
                copy_target,
                control_schema,
                approved_revision,
                base_source,
                base_digest,
                npi,
                summary["fingerprint"],
                count,
                address_relation,
                binding_relation,
            )
            receipt.as_dict()
            return receipt
    except asyncpg.PostgresError as error:
        raise NetworkCustomAddressSourceError("compact_custom_offices_preparation_failed") from error


async def _empty_heap(connection, target):
    _require(
        await connection.fetchval(
            "SELECT to_regclass($1) IS NULL AND to_regclass($2) IS NULL",
            target.schema_name + ".entity_address_unified",
            target.schema_name + ".custom_provider_location_binding",
        ),
        "unexpected_custom_heap",
    )


async def _binding_pin(connection, receipt, binding_table):
    _require(binding_table in _BINDINGS, "binding_table_invalid")
    table = _identifier(receipt.target.schema_name) + "." + _identifier(binding_table)
    if receipt.binding_relation is None:
        _require(receipt.custom_pair_count == 0, "binding_missing")
        return None
    _require(
        receipt.binding_relation == CompactAddressRelation(receipt.target.schema_name, binding_table),
        "binding_scope_changed",
    )
    await connection.fetch(f"SELECT * FROM {table} LIMIT 0")
    return table


async def _expected_rows(connection, receipt, address_table, binding_table, binding_kind, physical, carrier=None):
    expected = _identifier("custom_expected_" + uuid4().hex)
    base = _identifier(physical.schema_name) + "." + _identifier(physical.table_name)
    control = _identifier(receipt.control_schema)
    async with connection.transaction():
        await connection.execute(f"CREATE TEMP TABLE {expected}(LIKE {base} INCLUDING DEFAULTS) ON COMMIT DROP")
        await connection.execute(
            _APPROVED_CTE.format(control=control) + _INSERT_ADDRESS_SQL.format(address_table="pg_temp." + expected),
            receipt.approved_revision,
        )
        _require(
            await connection.fetchval(f"SELECT count(*) FROM pg_temp.{expected}") == receipt.custom_pair_count,
            "expected_count_differs",
        )
        if address_table is not None:
            extra = "" if carrier is None else " UNION ALL (" + carrier.materialized_address_sql + ")"
            _require(
                await connection.fetchval(
                    f"WITH expected AS (SELECT * FROM pg_temp.{expected}{extra}) "
                    f"SELECT NOT EXISTS((SELECT to_jsonb(expected)-'canonical_network_ids' FROM expected "
                    f"EXCEPT ALL SELECT to_jsonb(actual)-'canonical_network_ids' FROM {address_table} actual) UNION ALL "
                    f"(SELECT to_jsonb(actual)-'canonical_network_ids' FROM {address_table} actual "
                    "EXCEPT ALL SELECT to_jsonb(expected)-'canonical_network_ids' FROM expected))",
                    *((receipt.approved_revision,) if carrier is not None else ()),
                ),
                "expected_rows_differ",
            )
        await _expected_bindings(connection, receipt, expected, binding_table, binding_kind)
        await connection.execute(f"DROP TABLE pg_temp.{expected}")


async def _expected_bindings(connection, receipt, expected, binding_table, binding_kind):
    if binding_table is None:
        _require(receipt.custom_pair_count == 0, "binding_identity_changed")
        return
    source_table = _identifier(receipt.target.schema_name) + ".network_membership_source"
    has_source = await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", source_table)
    outside_source = (
        f"NOT EXISTS(SELECT FROM {source_table} source WHERE "
        "(source.provider_system,source.provider_id,source.location_id)="
        "(binding.provider_system,binding.provider_id,binding.location_id))"
        if has_source
        else "TRUE"
    )
    predicate = (
        "TRUE"
        if binding_kind == "custom_provider_location_binding"
        else f"""(
        (binding.provider_system,binding.provider_id,binding.location_id) IN (SELECT provider_system,provider_id,location_id FROM custom)
        OR binding.location_key IN (SELECT location_key FROM pg_temp.{expected})
        OR ({outside_source} AND binding.location_key=encode(sha256(convert_to(jsonb_build_array('network_custom',binding.provider_system,binding.provider_id,binding.location_id)::text,'UTF8')),'hex')))"""
    )
    matches = await connection.fetchval(
        _APPROVED_CTE.format(control=_identifier(receipt.control_schema))
        + f"""
        ,expected_bindings AS (SELECT provider_system,provider_id,location_id,location_key,provider_system AS entity_type,provider_id AS entity_id FROM custom),
        actual_bindings AS (SELECT {_BINDING_COLUMNS} FROM {binding_table} binding WHERE {predicate})
        SELECT NOT EXISTS((SELECT * FROM expected_bindings EXCEPT ALL SELECT * FROM actual_bindings)
            UNION ALL (SELECT * FROM actual_bindings EXCEPT ALL SELECT * FROM expected_bindings))""",
        receipt.approved_revision,
    )
    _require(matches is True, "expected_bindings_differ")


def _require_carrier_scope(receipt, carrier):
    from process.network_compact_address_carriers import CompactAddressCarrierProjection

    _require(
        type(carrier) is CompactAddressCarrierProjection
        and carrier.receipt.target == receipt.target
        and carrier.receipt.approved_revision == receipt.approved_revision
        and carrier.receipt.base_artifact_sha256 == receipt.base_artifact_sha256,
        "carrier_scope_changed",
    )


async def verify_compact_custom_offices(
    connection,
    receipt,
    *,
    runtime_roles,
    control_schema=None,
    binding_table="custom_provider_location_binding",
    carrier=None,
):
    """Rederive current direct rows independently before publication.

    Final bindings may include unrelated source rows. Every deterministic direct
    custom key and every approved direct pair must match exactly; no hash/count
    supplied by the producer substitutes for full expected-row/binding parity.
    """
    _require(type(receipt) is CompactCustomOfficeReceipt, "receipt_invalid")
    receipt = parse_compact_custom_office_receipt(receipt.as_dict())
    if carrier is not None:
        _require_carrier_scope(receipt, carrier)
    _require(
        (control_schema or receipt.control_schema) == receipt.control_schema,
        "scope_changed",
    )
    await _require_transaction(connection)
    try:
        async with connection.transaction():
            candidate = await _candidate(
                connection,
                receipt.target,
                receipt.base_source,
                receipt.approved_revision,
                _identifier(receipt.control_schema),
                verification=True,
            )
            physical = await _verify_inputs(connection, receipt, runtime_roles, candidate)
            bindings = await _binding_pin(connection, receipt, binding_table)
            address_table = None
            if receipt.custom_pair_count or carrier is not None and carrier.receipt.materialized_address_rows:
                address_table = _identifier(receipt.target.schema_name) + ".entity_address_unified"
                await connection.fetch(f"SELECT * FROM {address_table} LIMIT 0")
            else:
                await _empty_heap(connection, receipt.target)
            await _expected_rows(connection, receipt, address_table, bindings, binding_table, physical, carrier)
            return (
                None
                if address_table is None
                else CompactAddressRelation(
                    receipt.target.schema_name,
                    "entity_address_unified",
                )
            )
    except asyncpg.PostgresError as error:
        raise NetworkCustomAddressSourceError("compact_custom_offices_verification_failed") from error


async def _verify_inputs(connection, receipt, runtime_roles, candidate):
    summary, base_digest, npi, physical = await _capture_inputs(
        connection,
        receipt.base_source,
        receipt.approved_revision,
        runtime_roles,
        receipt.npi_source,
        _identifier(receipt.control_schema),
        candidate,
    )
    _require(
        (summary["fingerprint"], summary["pairs"], base_digest, npi)
        == (
            receipt.approved_records_sha256,
            receipt.custom_pair_count,
            receipt.base_artifact_sha256,
            receipt.npi_source,
        ),
        "native_inputs_changed",
    )
    if npi is not None:
        await _require_npi_pairs(connection, _identifier(receipt.control_schema), receipt.approved_revision, npi)
    return physical
