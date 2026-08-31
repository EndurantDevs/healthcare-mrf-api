# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validated sealed-root coordinates shared by Flex admission paths."""

from __future__ import annotations

from dataclasses import dataclass

from process.uhc_flex_practitioner_store_contract import (
    ACQUISITION_PATTERN,
    COHORT_PATTERN,
    HASH_PATTERN,
    INTENT_PATTERN,
    RUN_PATTERN,
    UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
)


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerSealedRoot:
    """Bounded sealed header fields used to compare one acquisition root."""

    acquisition_id: str
    cohort_id: str
    acquisition_role: str
    source_id: str
    connector_id: str
    query_contract_id: str
    storage_contract_id: str
    run_id: str
    dataset_intent_id: str
    expected_npi_count: int
    resource_count: int
    terminal_set_sha256: str
    error_count: int = 0
    cohort_complete: bool = True

    def __post_init__(self) -> None:
        if (
            type(self.acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or type(self.cohort_id) is not str
            or COHORT_PATTERN.fullmatch(self.cohort_id) is None
            or self.acquisition_role not in {"baseline", "candidate"}
            or type(self.source_id) is not str
            or not 1 <= len(self.source_id) <= 64
            or type(self.connector_id) is not str
            or not 1 <= len(self.connector_id) <= 64
            or type(self.query_contract_id) is not str
            or not 1 <= len(self.query_contract_id) <= 96
            or self.storage_contract_id
            != UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID
            or type(self.run_id) is not str
            or RUN_PATTERN.fullmatch(self.run_id) is None
            or type(self.dataset_intent_id) is not str
            or INTENT_PATTERN.fullmatch(self.dataset_intent_id) is None
            or type(self.expected_npi_count) is not int
            or self.expected_npi_count < 1
            or type(self.resource_count) is not int
            or self.resource_count < 0
            or type(self.terminal_set_sha256) is not str
            or HASH_PATTERN.fullmatch(self.terminal_set_sha256) is None
            or type(self.error_count) is not int
            or not 0 <= self.error_count <= self.expected_npi_count
            or self.cohort_complete is not (self.error_count == 0)
        ):
            raise ValueError("Flex Practitioner sealed root is invalid")


__all__ = ("UHCFlexPractitionerSealedRoot",)
