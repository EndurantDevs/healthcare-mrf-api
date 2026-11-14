# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify authenticated named capture metadata without probing source rows."""

from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_retained_capture_contract import (
    TABLES,
    RetainedFHIRCapture,
    RetainedFHIRCaptureError,
    require,
    validate_capture,
)
from process.network_fhir_source_epoch import _borrowed_epoch_settings
from process.registry_execution_catalog import (
    _RETAINED_FHIR_RELATIONS_SQL,
    _decode_retained_fhir_document,
    _require_retained_fhir_relations,
)

_SNAPSHOT_SQL = """SELECT pg_catalog.current_setting('transaction_isolation') AS isolation,
 pg_catalog.current_setting('transaction_read_only') AS read_only"""
_CATALOG_SQL = _RETAINED_FHIR_RELATIONS_SQL


async def _snapshot(connection, expected=None):
    """Require the caller's active read-only repeatable-read transaction."""
    require(connection.is_in_transaction() is True)
    snapshot_by_field = await connection.fetchrow(_SNAPSHOT_SQL)
    require(
        snapshot_by_field is not None
        and (snapshot_by_field["isolation"], snapshot_by_field["read_only"])
        in (("repeatable read", "on"), ("serializable", "on"))
        and connection.is_in_transaction() is True
    )
    snapshot_by_field = dict(snapshot_by_field)
    require(expected is None or snapshot_by_field == expected)
    return snapshot_by_field


def _capture_pin(source_pin):
    """Revalidate the exact stored logical source, dataset and selected capture."""
    require(
        type(source_pin) is PinnedFHIRMembershipSource
        and source_pin.source_id != "cms-npd"
        and source_pin.retained_capture is not None
        and source_pin.retained_epoch is None
        and source_pin.retained_successor is None
        and source_pin.source_witness_capture is None
        and source_pin.source_witness_anchor is None
    )
    PinnedFHIRMembershipSource.__post_init__(source_pin)
    capture = validate_capture(source_pin.retained_capture.as_dict())
    require(
        source_pin.read_schema_name == capture.schema_name
        and source_pin.read_resource_relation == capture.tables[TABLES[1]]
    )
    return capture


async def _catalog_metadata(connection, capture):
    """Validate logical native shape and exact stored edition metadata only."""
    relations = await connection.fetch(_CATALOG_SQL, capture.schema_name)
    _require_retained_fhir_relations([dict(row) for row in relations], capture.schema_name)
    document = await connection.fetchval(
        "SELECT pg_catalog.obj_description(n.oid,'pg_namespace') FROM pg_catalog.pg_namespace n WHERE n.nspname=$1",
        capture.schema_name,
    )
    require(_decode_retained_fhir_document(document) == capture)


async def verify_retained_fhir_metadata(connection, source_pin) -> RetainedFHIRCapture:
    """Verify an authenticated stored capture on the caller's RR/RO transaction.

    This metadata-only check grants no source-content admission or read authority.
    The borrowed savepoint preserves caller settings and must finish on that same
    open transaction; PostgreSQL enforces all access to the selected named data.
    """
    try:
        capture = _capture_pin(source_pin)
        snapshot = await _snapshot(connection)
        async with _borrowed_epoch_settings(connection):
            await connection.execute("SET LOCAL search_path TO pg_catalog")
            await connection.execute("SET LOCAL jit = off")
            await _catalog_metadata(connection, capture)
            await _snapshot(connection, snapshot)
        await _snapshot(connection, snapshot)
        return capture
    except RetainedFHIRCaptureError:
        raise
    except Exception as error:
        raise RetainedFHIRCaptureError("retained_fhir_capture_unavailable") from error
