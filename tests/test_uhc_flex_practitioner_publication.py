# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from datetime import timezone
import hashlib
import json

import pytest

from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_materialization import (
    materialize_uhc_flex_practitioner_stored_resource,
)
from process.uhc_flex_practitioner_publication import (
    _canonical_json,
    build_uhc_flex_practitioner_dataset_identity,
    uhc_flex_practitioner_publication_metadata,
    UHCFlexPractitionerDatasetReadiness,
)
from process.uhc_flex_practitioner_single_root_contract import (
    build_single_root_admission,
    single_root_dataset_intent_id,
    single_root_run_id,
    UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY,
)
from process.uhc_flex_practitioner_store_contract import (
    _acquisition_id,
    UHCFlexPractitionerResourceRow,
    UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    build_uhc_flex_practitioner_dataset_intent_id,
    build_uhc_flex_practitioner_run_id,
    build_uhc_flex_practitioner_twin_admission,
    build_uhc_flex_practitioner_twin_attempt,
    UHCFlexPractitionerSealedRoot,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import MEMBER_NPIS


PROJECTION_DATE = "2026-08-10"
OPERATION_KEY = "a" * 64
COHORT_ID = "pdufc_" + "b" * 48


def _admission(resource_count: int = 1):
    intent_id = build_uhc_flex_practitioner_dataset_intent_id(
        COHORT_ID,
        PROJECTION_DATE,
        OPERATION_KEY,
    )
    roots = []
    for role, marker in (("baseline", "1"), ("candidate", "2")):
        roots.append(
            UHCFlexPractitionerSealedRoot(
                acquisition_id="pdufpa_" + marker * 48,
                cohort_id=COHORT_ID,
                acquisition_role=role,
                source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
                connector_id=UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
                query_contract_id=UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
                storage_contract_id=(UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID),
                run_id=build_uhc_flex_practitioner_run_id(intent_id, role),
                dataset_intent_id=intent_id,
                expected_npi_count=2,
                resource_count=resource_count,
                terminal_set_sha256="c" * 64,
            )
        )
    attempt = build_uhc_flex_practitioner_twin_attempt(
        roots[0],
        roots[1],
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        attempted_at=datetime(2026, 8, 10, tzinfo=timezone.utc),
    )
    return build_uhc_flex_practitioner_twin_admission(
        attempt,
        admitted_at=datetime(2026, 8, 10, 0, 1, tzinfo=timezone.utc),
    )


def _single_root_admission(error_count: int = 0):
    intent_id = single_root_dataset_intent_id(
        COHORT_ID, PROJECTION_DATE, OPERATION_KEY
    )
    candidate = UHCFlexPractitionerSealedRoot(
        acquisition_id=_acquisition_id(
            cohort_id=COHORT_ID,
            acquisition_role="candidate",
            run_id=single_root_run_id(intent_id),
            dataset_intent_id=intent_id,
            expected_npi_count=2,
        ),
        cohort_id=COHORT_ID,
        acquisition_role="candidate",
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        connector_id=UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
        query_contract_id=UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
        storage_contract_id=UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
        run_id=single_root_run_id(intent_id),
        dataset_intent_id=intent_id,
        expected_npi_count=2,
        resource_count=1,
        terminal_set_sha256="c" * 64,
        error_count=error_count,
        cohort_complete=error_count == 0,
    )
    return build_single_root_admission(
        candidate,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        admitted_at=datetime(2026, 8, 10, tzinfo=timezone.utc),
    )


def test_dataset_identity_and_metadata_bind_the_exact_admission() -> None:
    admission = _admission()
    identity = build_uhc_flex_practitioner_dataset_identity(admission)
    replay = build_uhc_flex_practitioner_dataset_identity(admission)

    assert replay == identity
    assert identity.dataset_id.startswith("pdufpd_")
    assert identity.acquisition_root_run_id.startswith("pdufpar_")
    metadata = uhc_flex_practitioner_publication_metadata(
        identity,
        admission,
    )
    assert metadata["admission_id"] == admission.admission_id
    assert metadata["candidate_acquisition_id"] == (admission.candidate_acquisition_id)
    assert metadata["cohort_id"] == admission.cohort_id
    assert metadata["dataset_intent_id"] == admission.dataset_intent_id
    assert metadata["semantic_projection_as_of"] == PROJECTION_DATE
    assert metadata["operation_key"] == OPERATION_KEY
    assert metadata["resource_hash_contract"] == (
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    assert metadata["selected_resources"] == ["Practitioner"]
    assert metadata["expected_resources"] == ["Practitioner"]
    assert metadata["cohort_complete"] is True
    assert metadata["endpoint_collection_complete"] is False
    assert metadata["endpoint_complete"] is False
    assert hashlib.sha256(_canonical_json(metadata).encode()).hexdigest() == (
        "9307e25e748aa46ac9bbef2a800c73bed8d084982aef0ddc6fdef9ff74edb02b"
    )


def test_single_root_metadata_omits_twin_coordinates() -> None:
    admission = _single_root_admission()
    metadata = uhc_flex_practitioner_publication_metadata(
        build_uhc_flex_practitioner_dataset_identity(admission), admission
    )

    assert "baseline_acquisition_id" not in metadata
    assert "baseline_run_id" not in metadata
    assert metadata["provider_directory_reviewed_root_policy_v1"] == (
        UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.document()
    )
    with pytest.raises(ValueError, match="single-root admission"):
        replace(admission, admission_id="pdufpad_" + "f" * 48)


def test_retry_exhausted_single_root_metadata_is_explicitly_partial() -> None:
    admission = _single_root_admission(error_count=1)
    metadata = uhc_flex_practitioner_publication_metadata(
        build_uhc_flex_practitioner_dataset_identity(admission),
        admission,
        1,
    )

    assert metadata["retry_exhausted_count"] == 1
    assert metadata["cohort_complete"] is False
    assert metadata["endpoint_collection_complete"] is False
    assert metadata["endpoint_complete"] is False
    with pytest.raises(ValueError, match="publication identity"):
        uhc_flex_practitioner_publication_metadata(
            build_uhc_flex_practitioner_dataset_identity(_admission()),
            _admission(),
            1,
        )


def test_stored_resource_facade_revalidates_and_materializes_one_row() -> None:
    npi = MEMBER_NPIS[0]
    practitioner_by_field = {
        "resourceType": "Practitioner",
        "id": "synthetic-practitioner",
        "identifier": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": str(npi),
            }
        ],
        "name": [{"family": "Synthetic", "given": ["Alex"]}],
    }
    payload_text = json.dumps(
        practitioner_by_field,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    stored = UHCFlexPractitionerResourceRow(
        requested_npi=npi,
        resource_id="synthetic-practitioner",
        payload_sha256=hashlib.sha256(payload_text.encode()).hexdigest(),
        payload_json_text=payload_text,
    )

    materialized = materialize_uhc_flex_practitioner_stored_resource(
        stored,
        dataset_id="pdufpd_" + "d" * 48,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        run_id="pdufpr_" + "e" * 48,
        semantic_projection_as_of=PROJECTION_DATE,
    )

    assert materialized.requested_npi == npi
    assert materialized.acquired_resource_sha256 == stored.payload_sha256
    assert materialized.dataset_resource["resource_type"] == "Practitioner"
    assert materialized.dataset_resource["payload_json"]["npi"] == npi


def test_readiness_never_promotes_exact_cohort_to_endpoint_complete() -> None:
    admission = _admission(resource_count=0)
    identity = build_uhc_flex_practitioner_dataset_identity(admission)
    readiness_by_field = dict(
        dataset_id=identity.dataset_id,
        previous_dataset_id=None,
        admission_id=admission.admission_id,
        candidate_acquisition_id=admission.candidate_acquisition_id,
        acquisition_root_run_id=identity.acquisition_root_run_id,
        cohort_id=admission.cohort_id,
        dataset_intent_id=admission.dataset_intent_id,
        endpoint_id=identity.endpoint_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        dataset_hash=hashlib.sha256(b"").hexdigest(),
        resource_count=0,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        source_authority_id="unitedhealthcare",
        cohort_complete=True,
        endpoint_collection_complete=False,
        endpoint_complete=False,
    )
    readiness = UHCFlexPractitionerDatasetReadiness(**readiness_by_field)
    assert readiness.resource_count == 0

    partial = UHCFlexPractitionerDatasetReadiness(
        **{
            **readiness_by_field,
            "cohort_complete": False,
            "retry_exhausted_count": 1,
        }
    )
    assert partial.retry_exhausted_count == 1
    assert partial.endpoint_collection_complete is False
    assert partial.endpoint_complete is False

    readiness_by_field["endpoint_complete"] = True
    with pytest.raises(ValueError, match="readiness"):
        UHCFlexPractitionerDatasetReadiness(**readiness_by_field)
