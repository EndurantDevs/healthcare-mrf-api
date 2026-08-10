# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import json

import pytest

from process.uhc_flex_practitioner_query import (
    validate_uhc_flex_practitioner_search_bundle,
)
from process.uhc_flex_practitioner_store import (
    _canonical_resource_rows,
    _terminal_record_sha256,
    build_uhc_flex_practitioner_acquisition_identity,
    release_uhc_flex_practitioner_work,
    UHCFlexPractitionerAcquisitionIdentity,
    UHCFlexPractitionerAcquisitionSummary,
    UHCFlexPractitionerResourceRow,
    UHCFlexPractitionerStoreError,
    UHCFlexPractitionerWorkClaim,
    UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    cohort_fixture,
)


RUN_ID = "pdufpr_" + "1" * 48
INTENT_ID = "pdufdi_" + "2" * 48
NPI = 1003821380


class _ReleaseDatabase:
    def __init__(self, updated_count: int) -> None:
        self.updated_count = updated_count
        self.statements: list[tuple[str, dict[str, object]]] = []

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def scalar(self, statement: str, **parameters):
        self.statements.append((statement, parameters))
        return ""

    async def status(self, statement: str, **parameters):
        self.statements.append((statement, parameters))
        return self.updated_count


def _identity(role: str = "baseline") -> UHCFlexPractitionerAcquisitionIdentity:
    return build_uhc_flex_practitioner_acquisition_identity(
        cohort_fixture(),
        acquisition_role=role,
        run_id=RUN_ID,
        dataset_intent_id=INTENT_ID,
    )


def _terminal_claim(
    role: str,
    requested_npi: int = NPI,
) -> UHCFlexPractitionerWorkClaim:
    identity = build_uhc_flex_practitioner_acquisition_identity(
        cohort_fixture(),
        acquisition_role=role,
        run_id=RUN_ID if role == "baseline" else "pdufpr_" + "3" * 48,
        dataset_intent_id=INTENT_ID,
    )
    return UHCFlexPractitionerWorkClaim(
        acquisition_id=identity.acquisition_id,
        cohort_id=identity.cohort_id,
        requested_npi=requested_npi,
        attempt=1,
        lease_token=("4" if role == "baseline" else "5") * 64,
    )


def _matched_result():
    return validate_uhc_flex_practitioner_search_bundle(
        NPI,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 1,
            "entry": [
                {
                    "resource": {
                        "resourceType": "Practitioner",
                        "id": "practitioner.synthetic-1",
                        "identifier": [
                            {
                                "system": "http://hl7.org/fhir/sid/us-npi",
                                "value": str(NPI),
                            }
                        ],
                        "telecom": [{"system": "email", "value": "a@b.test"}],
                    }
                }
            ],
        },
    )


def test_acquisition_identity_binds_role_run_intent_and_exact_cohort() -> None:
    baseline = _identity("baseline")
    candidate = build_uhc_flex_practitioner_acquisition_identity(
        cohort_fixture(),
        acquisition_role="candidate",
        run_id="pdufpr_" + "3" * 48,
        dataset_intent_id=INTENT_ID,
    )

    assert baseline.acquisition_id.startswith("pdufpa_")
    assert baseline.acquisition_id != candidate.acquisition_id
    assert baseline.storage_contract_id == (
        UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID
    )
    assert baseline.dataset_intent_id == candidate.dataset_intent_id
    assert baseline.endpoint_collection_complete is False
    assert baseline.endpoint_complete is False
    with pytest.raises(ValueError):
        build_uhc_flex_practitioner_acquisition_identity(
            cohort_fixture(),
            acquisition_role="single",
            run_id=RUN_ID,
            dataset_intent_id=INTENT_ID,
        )


def test_terminal_record_hash_and_payload_rows_match_query_result() -> None:
    query_result = _matched_result()
    identity = _identity()
    claim = UHCFlexPractitionerWorkClaim(
        acquisition_id=identity.acquisition_id,
        cohort_id=identity.cohort_id,
        requested_npi=NPI,
        attempt=2,
        lease_token="4" * 64,
    )

    resource_rows = _canonical_resource_rows(query_result)
    assert len(resource_rows) == 1
    resource_fields = resource_rows[0]
    assert hashlib.sha256(
        str(resource_fields["payload_json_text"]).encode("utf-8")
    ).hexdigest() == resource_fields["payload_sha256"]
    expected = hashlib.sha256(
        "\x1f".join(
            (
                "healthporta.provider-directory.uhc-flex-practitioner-"
                "terminal-record.v1",
                str(NPI),
                "matched",
                query_result.result_sha256,
                "1",
                "",
            )
        ).encode("utf-8")
    ).hexdigest()
    assert _terminal_record_sha256(
        claim,
        status="matched",
        result_sha256=query_result.result_sha256,
        resource_count=1,
        error_code=None,
    ) == expected


def test_terminal_record_root_is_acquisition_neutral_but_outcome_exact() -> None:
    comparable_by_field = {
        "status": "matched",
        "result_sha256": "6" * 64,
        "resource_count": 1,
        "error_code": None,
    }
    baseline_root = _terminal_record_sha256(
        _terminal_claim("baseline"),
        **comparable_by_field,
    )
    assert baseline_root == _terminal_record_sha256(
        _terminal_claim("candidate"),
        **comparable_by_field,
    )


@pytest.mark.parametrize(
    (
        "requested_npi",
        "status",
        "result_sha256",
        "resource_count",
        "error_code",
    ),
    (
        (1518379601, "matched", "6" * 64, 1, None),
        (NPI, "unmatched", "6" * 64, 1, None),
        (NPI, "matched", "8" * 64, 1, None),
        (NPI, "matched", "6" * 64, 2, None),
        (NPI, "error", None, 0, "response_invalid"),
    ),
)
def test_terminal_record_root_binds_each_comparable_outcome_field(
    requested_npi: int,
    status: str,
    result_sha256: str | None,
    resource_count: int,
    error_code: str | None,
) -> None:
    baseline_root = _terminal_record_sha256(
        _terminal_claim("baseline"),
        status="matched",
        result_sha256="6" * 64,
        resource_count=1,
        error_code=None,
    )
    drifted_root = _terminal_record_sha256(
        _terminal_claim("candidate", requested_npi),
        status=status,
        result_sha256=result_sha256,
        resource_count=resource_count,
        error_code=error_code,
    )
    assert drifted_root != baseline_root


def test_manifest_row_and_terminal_summary_fail_closed() -> None:
    query_result = _matched_result()
    resource_fields = _canonical_resource_rows(query_result)[0]
    stored_resource = UHCFlexPractitionerResourceRow(
        requested_npi=NPI,
        resource_id=str(resource_fields["resource_id"]),
        payload_sha256=str(resource_fields["payload_sha256"]),
        payload_json_text=str(resource_fields["payload_json_text"]),
    )
    assert json.loads(stored_resource.payload_json_text)["id"] == (
        stored_resource.resource_id
    )
    with pytest.raises(ValueError):
        UHCFlexPractitionerResourceRow(
            requested_npi=NPI,
            resource_id=stored_resource.resource_id,
            payload_sha256="0" * 64,
            payload_json_text=stored_resource.payload_json_text,
        )
    identity = _identity()
    with pytest.raises(ValueError):
        UHCFlexPractitionerAcquisitionSummary(
            acquisition_id=identity.acquisition_id,
            expected_npi_count=2,
            matched_count=1,
            unmatched_count=0,
            error_count=1,
            resource_count=1,
            terminal_set_sha256="5" * 64,
            cohort_complete=True,
            endpoint_collection_complete=False,
            endpoint_complete=False,
        )


@pytest.mark.asyncio
async def test_release_is_exact_token_fenced_and_stale_safe() -> None:
    identity = _identity()
    claim = UHCFlexPractitionerWorkClaim(
        acquisition_id=identity.acquisition_id,
        cohort_id=identity.cohort_id,
        requested_npi=NPI,
        attempt=3,
        lease_token="6" * 64,
    )
    database = _ReleaseDatabase(1)
    await release_uhc_flex_practitioner_work(claim, database=database)
    update_sql, parameters = database.statements[-1]
    assert "SET status = 'pending'" in update_sql
    assert "attempt_count = :attempt" in update_sql
    assert "lease_token = :lease_token" in update_sql
    assert parameters["attempt"] == 3
    assert parameters["lease_token"] == "6" * 64

    with pytest.raises(UHCFlexPractitionerStoreError) as stale_error:
        await release_uhc_flex_practitioner_work(
            claim,
            database=_ReleaseDatabase(0),
        )
    assert stale_error.value.code == "lease_lost"
