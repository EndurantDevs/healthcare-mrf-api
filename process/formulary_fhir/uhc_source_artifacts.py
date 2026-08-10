# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""UHC drug-catalog projection into the generic formulary artifact ledger."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from db.models import db
from process.formulary_fhir.source_artifact_contract import (
    SourceArtifactIdentity,
)
from process.formulary_fhir.source_artifacts import register_source_file_set
from process.formulary_fhir.repository_shared import strict_hash
from process.uhc_drug_file_catalog import UHCObservedDrugCatalog
from process.uhc_drug_file_catalog import validate_observed_drug_catalog
from process.uhc_drug_file_catalog import validate_retained_drug_catalog_proof


@dataclass(frozen=True, slots=True, repr=False)
class UHCSourceArtifactRegistration:
    """One retained-listing proof and its exact drug artifact identities."""

    source_observation_sha256: str = field(repr=False)
    catalog: UHCObservedDrugCatalog = field(repr=False)
    identities: tuple[SourceArtifactIdentity, ...] = field(repr=False)

    def __post_init__(self) -> None:
        strict_hash(self.source_observation_sha256, "source observation hash")
        validate_observed_drug_catalog(self.catalog)
        if type(self.identities) is not tuple or not self.identities:
            raise ValueError("UHC drug source artifact identities are invalid")
        expected_identities = identities_from_uhc_drug_catalog(
            self.identities[0].source_id,
            self.catalog,
        )
        if self.identities != expected_identities:
            raise ValueError("UHC drug source artifact identities are inconsistent")


def identities_from_uhc_drug_catalog(
    source_id: str,
    catalog: UHCObservedDrugCatalog,
) -> tuple[SourceArtifactIdentity, ...]:
    """Project one validated UHC 48-file catalog into generic identities."""

    validate_observed_drug_catalog(catalog)
    return tuple(
        sorted(
            (
                SourceArtifactIdentity(
                    source_id=source_id,
                    source_file_set_sha256=(
                        catalog.acquisition_contract_sha256
                    ),
                    source_file_id=catalog_file.file_id,
                    raw_listing_projection_sha256=(
                        catalog.raw_listing_projection_sha256
                    ),
                    family=catalog_file.family,
                    file_name=catalog_file.file_name,
                    source_url=catalog_file.source_url,
                    catalog_modified_at=catalog_file.catalog_modified_at,
                    catalog_entry_sha256=(
                        catalog_file.catalog_entry_sha256
                    ),
                    expected_byte_count=catalog_file.size_bytes,
                )
                for catalog_file in catalog.files
            ),
            key=lambda identity: (
                identity.family,
                identity.file_name,
                identity.source_file_id,
            ),
        )
    )


def prepare_uhc_source_artifact_registration(
    source_id: str,
    raw_proof: Any,
) -> UHCSourceArtifactRegistration:
    """Rehash retained listings before projecting their 48 drug identities."""

    normalized_proof, catalog = validate_retained_drug_catalog_proof(raw_proof)
    identities = identities_from_uhc_drug_catalog(source_id, catalog)
    return UHCSourceArtifactRegistration(
        source_observation_sha256=normalized_proof["raw_set_sha256"],
        catalog=catalog,
        identities=identities,
    )


async def register_uhc_source_file_set(
    source_id: str,
    raw_proof: Any,
    *,
    database: Any = db,
) -> UHCSourceArtifactRegistration:
    """Validate retained listing bytes and durably register their drug set."""

    registration = prepare_uhc_source_artifact_registration(
        source_id,
        raw_proof,
    )
    registered_identities = await register_source_file_set(
        registration.identities,
        source_observation_sha256=registration.source_observation_sha256,
        database=database,
    )
    if registered_identities != registration.identities:
        raise RuntimeError("UHC drug source artifact registration changed")
    return registration


__all__ = (
    "UHCSourceArtifactRegistration",
    "identities_from_uhc_drug_catalog",
    "prepare_uhc_source_artifact_registration",
    "register_uhc_source_file_set",
)
