# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog candidate coordinates and logical content validation records."""

import json
from dataclasses import dataclass
from uuid import UUID

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import TABLES
from process.registry_fhir_catalog_validation import validate_fhir_catalog_candidate, verify_fhir_catalog_validation

PHYSICAL_TABLES = frozenset(set(TABLES) - {"registry_fhir_catalog_snapshot"})


@dataclass(frozen=True)
class FHIRCatalogTarget:
    dataset_id: str
    schema_id: str
    producer_id: str
    candidate_id: str
    schema_name: str

    def __post_init__(self):
        for value in (self.dataset_id, self.schema_id, self.producer_id, self.candidate_id):
            if type(value) is not str or not UUID(value).int or str(UUID(value)) != value:
                raise ValueError("registry_fhir_catalog_target_invalid")
        if self.schema_name != "fhir_catalog_" + UUID(self.candidate_id).hex:
            raise ValueError("registry_fhir_catalog_target_invalid")


async def validate_fhir_catalog_snapshot_data(
    connection,
    copy_target,
    *,
    expected_snapshot,
    control_schema,
    standing_grant=None,
):
    """Validate complete candidate data before its logical control record is registered."""
    if not connection.is_in_transaction() or type(copy_target) is not FHIRCatalogTarget:
        raise ValueError("registry_fhir_catalog_transaction_required")
    # An outside inheritance relation can change a catalog after its own writers close.
    candidate_state = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation') AS isolation, "
        "(SELECT count(*) = $3 FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND c.relname::text = ANY($2::text[]) "
        "AND c.relkind = 'r' AND NOT c.relispartition AND NOT c.relhassubclass AND NOT c.relhasrules "
        "AND NOT EXISTS (SELECT FROM pg_catalog.pg_depend d "
        "WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.refclassid = d.classid "
        "AND d.objid = c.oid AND d.deptype = 'n')) AS layout_complete",
        copy_target.schema_name,
        sorted(PHYSICAL_TABLES),
        len(PHYSICAL_TABLES),
    )
    if candidate_state["isolation"] != "read committed":
        raise ValueError("registry_fhir_catalog_fresh_snapshot_required")
    if not candidate_state["layout_complete"]:
        raise ValueError("registry_fhir_catalog_table_layout_invalid")
    await connection.execute("SET LOCAL row_security = off")
    return await validate_fhir_catalog_candidate(
        connection, copy_target, expected_snapshot, control_schema=control_schema, standing_grant=standing_grant
    )


async def validate_fhir_catalog_snapshot_record(connection, snapshot):
    """Validate logical generation and content accounting from retained control metadata."""
    if not connection.is_in_transaction():
        raise ValueError("registry_fhir_catalog_transaction_required")
    storage_identifier(snapshot["schema_name"])
    validation_by_field = snapshot["custody_json"]
    if type(validation_by_field) is str:
        validation_by_field = json.loads(validation_by_field)
    if type(validation_by_field) is dict and "validation" in validation_by_field:
        validation_by_field = validation_by_field["validation"]
    elif type(validation_by_field) is dict and "preparation" in validation_by_field:
        from process.registry_fhir_catalog_store import catalog_snapshot_preparation

        catalog_snapshot_preparation(snapshot)
        validation_by_field = {key: value for key, value in validation_by_field.items() if key != "preparation"}
    verify_fhir_catalog_validation(snapshot, validation_by_field)
    # PostgreSQL refuses a filtered read with this setting. It does not grant RLS bypass.
    await connection.execute("SET LOCAL row_security = off")
