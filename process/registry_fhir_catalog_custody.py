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
    observation_verifier=None,
):
    """Validate complete candidate data before its logical control record is registered."""
    if not connection.is_in_transaction() or type(copy_target) is not FHIRCatalogTarget:
        raise ValueError("registry_fhir_catalog_transaction_required")
    if await connection.fetchval("SELECT current_setting('transaction_isolation')") != "read committed":
        raise ValueError("registry_fhir_catalog_fresh_snapshot_required")
    namespace = storage_identifier(copy_target.schema_name)
    requires_replay = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_source "
        "WHERE descriptor_json->'admission'->>'kind'='catalog_observation') OR EXISTS("
        f"SELECT FROM {storage_identifier(control_schema)}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network_binding' "
        "AND record_json->'archived'='false'::jsonb AND record_json->>'source_system'='fhir' "
        "AND (record_json->'source_scope_json' ? 'insurance_plan_id' "
        "OR record_json->>'source_key' LIKE 'InsurancePlan/%')) OR EXISTS("
        f"SELECT FROM {namespace}.registry_fhir_catalog_binding "
        "WHERE document_json->'source_scope' ? 'insurance_plan_id' "
        "OR document_json->>'source_key' LIKE 'InsurancePlan/%')",
        expected_snapshot["metadata_approved_revision"],
    )
    if requires_replay or observation_verifier is not None:
        if standing_grant is None or not callable(observation_verifier):
            raise ValueError("registry_fhir_catalog_observation_verifier_required")
        await observation_verifier(
            connection=connection, copy_target=copy_target, snapshot=expected_snapshot, standing_grant=standing_grant
        )
    return await validate_fhir_catalog_candidate(
        connection, copy_target, expected_snapshot, control_schema=control_schema, standing_grant=standing_grant
    )


async def validate_fhir_catalog_snapshot_record(connection, snapshot):
    """Validate logical generation and content accounting from retained control metadata."""
    if not connection.is_in_transaction():
        raise ValueError("registry_fhir_catalog_transaction_required")
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
