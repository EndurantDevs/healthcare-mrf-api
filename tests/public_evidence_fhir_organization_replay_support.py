# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic inputs for retained FHIR Organization replay tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, NamedTuple

from public_evidence.evidence_record_primitives import (
    EvidenceSourceRecordReference,
    build_evidence_source_record_reference,
)
from public_evidence.source_record_inclusion_contract import (
    build_source_record_inventory_descriptor,
    derive_inventory_leaf_sha256,
)
from public_evidence.source_record_inclusion_primitives import (
    PublicEvidenceSourceRecordInventoryDescriptor,
)
from public_evidence.source_record_replay_primitives import (
    FHIR_ORGANIZATION_PAYLOAD_CANONICALIZATION_CONTRACT_ID,
    FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
)
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
)
from process.tin_npi_connector_evidence import (
    canonical_provider_directory_payload_hash,
)
from process.tin_npi_connector_security import TinTokenProjector
from process.tin_npi_connector_source import (
    FhirDatasetFenceIdentity,
    TinNpiConnectorSourceVector,
)
from tests.public_evidence_adapter_projection_support import (
    inventory_namespace,
    merkle_root,
)
from tests.public_evidence_record_support import source_release
from tests.tin_npi_connector_unit_support import (
    NPI_SYSTEM,
    TEST_EIN,
    TOKEN_POLICY_ID,
    TYPE_SYSTEM,
    fhir_dataset,
    source_vector,
    token_policy,
)


class ReplayFixture(NamedTuple):
    release: PublicEvidenceSourceReleaseDescriptor
    inventory: PublicEvidenceSourceRecordInventoryDescriptor
    source_vector: TinNpiConnectorSourceVector
    dataset: FhirDatasetFenceIdentity
    retained_rows: tuple[dict[str, Any], ...]
    token_projectors: tuple[TinTokenProjector, ...]
    record_policy_id: str


def retained_organization_row(
    resource_id: str,
    *,
    matched: bool,
) -> dict[str, Any]:
    identifier_rows: list[dict[str, Any]] = []
    if matched:
        identifier_rows = [
            {"system": NPI_SYSTEM, "value": "1234567893"},
            {
                "type_codes": [{"system": TYPE_SYSTEM, "code": "TAX"}],
                "value": TEST_EIN,
            },
        ]
    retained_payload_map = {
        "resource_id": resource_id,
        "active": True,
        "identifiers": identifier_rows,
    }
    return {
        "resource_type": "Organization",
        "resource_id": resource_id,
        "payload_hash": canonical_provider_directory_payload_hash(retained_payload_map),
        "payload_json": retained_payload_map,
    }


def default_retained_rows() -> tuple[dict[str, Any], ...]:
    return (
        retained_organization_row("synthetic-organization-a", matched=True),
        retained_organization_row("synthetic-organization-b", matched=False),
    )


def _source_record_reference(
    release: PublicEvidenceSourceReleaseDescriptor,
    dataset: FhirDatasetFenceIdentity,
    retained_row: dict[str, Any],
    projector: TinTokenProjector,
) -> EvidenceSourceRecordReference:
    record_hmac = projector.pseudonymize_source_record(
        source_id=dataset.source_id,
        source_endpoint_id=dataset.endpoint_id,
        source_dataset_id=dataset.dataset_id,
        resource_id=retained_row["resource_id"],
    )
    return build_evidence_source_record_reference(
        release,
        {
            "record_kind": "fhir_organization",
            "identity_contract_id": FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
            "record_hmac_sha256": record_hmac.hex(),
            "payload_sha256": retained_row["payload_hash"],
        },
    )


def inventory_for_rows(
    release: PublicEvidenceSourceReleaseDescriptor,
    dataset: FhirDatasetFenceIdentity,
    retained_rows: tuple[dict[str, Any], ...],
    projector: TinTokenProjector,
    *,
    root_override: str | None = None,
) -> PublicEvidenceSourceRecordInventoryDescriptor:
    source_records = tuple(
        sorted(
            (
                _source_record_reference(release, dataset, retained_row, projector)
                for retained_row in retained_rows
            ),
            key=lambda source_record_reference: source_record_reference.source_record_ref,
        )
    )
    namespace_by_field = inventory_namespace(source_records[0], len(source_records))
    namespace_by_field["payload_canonicalization_contract_id"] = (
        FHIR_ORGANIZATION_PAYLOAD_CANONICALIZATION_CONTRACT_ID
    )
    leaf_sha256s = tuple(
        derive_inventory_leaf_sha256(
            release,
            namespace_by_field,
            source_record_reference,
            ordinal,
        )
        for ordinal, source_record_reference in enumerate(source_records)
    )
    return build_source_record_inventory_descriptor(
        release,
        {
            **namespace_by_field,
            "member_root_sha256": root_override or merkle_root(leaf_sha256s),
        },
    )


def _projectors(
    temporary_path: Path,
    policy_ids: tuple[str, ...],
) -> tuple[TinTokenProjector, ...]:
    projectors = []
    for ordinal, policy_id in enumerate(policy_ids):
        policy_directory = temporary_path / f"policy-{ordinal}"
        policy_directory.mkdir()
        projectors.append(token_policy(policy_directory, policy_id=policy_id))
    return tuple(projectors)


def replay_fixture(
    temporary_path: Path,
    *,
    retained_rows: tuple[dict[str, Any], ...] | None = None,
    policy_ids: tuple[str, ...] = (TOKEN_POLICY_ID,),
    record_policy_id: str = TOKEN_POLICY_ID,
    inventory_root_override: str | None = None,
) -> ReplayFixture:
    temporary_path.mkdir(parents=True, exist_ok=True)
    selected_rows = default_retained_rows() if retained_rows is None else retained_rows
    organization_identities = tuple(
        (retained_row["resource_id"], retained_row["payload_hash"])
        for retained_row in selected_rows
    )
    dataset = fhir_dataset(organization_identities=organization_identities)
    selected_vector = source_vector(
        fhir_datasets=(dataset,),
        policy_ids=policy_ids,
    )
    projectors = _projectors(temporary_path, policy_ids)
    projector_by_id = {projector.token_policy_id: projector for projector in projectors}
    release = source_release("public_provider_directory_fhir")
    inventory = inventory_for_rows(
        release,
        dataset,
        selected_rows,
        projector_by_id[record_policy_id],
        root_override=inventory_root_override,
    )
    return ReplayFixture(
        release=release,
        inventory=inventory,
        source_vector=selected_vector,
        dataset=dataset,
        retained_rows=selected_rows,
        token_projectors=projectors,
        record_policy_id=record_policy_id,
    )
