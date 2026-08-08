# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed value validation for public FHIR formulary drugs."""

import datetime as dt

import pytest

from api import formulary_fhir_drug_values as drug_values
from api import formulary_fhir_serving as serving


FORMULARY_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
ALIAS_ID = "ffa_" + "1" * 48
DRUG_A = "ffm_" + "1" * 48
DRUG_B = "ffm_" + "2" * 48
LAST_UPDATED = dt.datetime(2026, 8, 8, 6, tzinfo=dt.UTC)


def _public_drug(**changes):
    drug_by_field = {
        "formulary_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "drug_id": DRUG_A,
        "status": "active",
        "name": "Synthetic Medication",
        "rxnorm_id": "123456",
        "ndc11": "00011122233",
        "last_updated": LAST_UPDATED,
        "tier": "Preferred",
        "prior_authorization": False,
        "step_therapy": True,
        "quantity_limit": False,
        "alternatives": drug_values.PublicFHIRFormularyAlternatives((), 0),
    }
    drug_by_field.update(changes)
    return drug_values.PublicFHIRFormularyDrug(**drug_by_field)


@pytest.mark.parametrize(
    "filter_changes",
    (
        {"rxnorm_id": 1},
        {"rxnorm_id": "invalid"},
        {"ndc11": 1},
        {"ndc11": "123"},
        {"tier": 1},
        {"tier": ""},
        {"prior_authorization": 1},
        {"step_therapy": "false"},
        {"quantity_limit": 0},
    ),
)
def test_drug_filters_reject_noncanonical_codes_text_and_flags(filter_changes):
    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        drug_values.FHIRFormularyDrugFilters(**filter_changes)


def test_optional_drug_fields_accept_none():
    drug = drug_values.validate_public_fhir_formulary_drug(
        _public_drug(
            status=None,
            name=None,
            rxnorm_id=None,
            ndc11=None,
            tier=None,
            prior_authorization=None,
            step_therapy=None,
            quantity_limit=None,
        )
    )

    assert drug.rxnorm_id is None
    assert drug.ndc11 is None
    assert drug.prior_authorization is None
    assert drug.step_therapy is None
    assert drug.quantity_limit is None


@pytest.mark.parametrize(
    "drug_changes",
    (
        {"rxnorm_id": "invalid"},
        {"ndc11": "123"},
        {"prior_authorization": 1},
        {"step_therapy": "true"},
        {"quantity_limit": 0},
    ),
)
def test_public_drug_rejects_invalid_optional_codes_and_flags(drug_changes):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drug_values.validate_public_fhir_formulary_drug(
            _public_drug(**drug_changes)
        )


def _many_drug_ids(count: int) -> tuple[str, ...]:
    return tuple("ffm_" + f"{index:048x}" for index in range(count))


@pytest.mark.parametrize(
    "alternatives",
    (
        None,
        drug_values.PublicFHIRFormularyAlternatives([], 0),
        drug_values.PublicFHIRFormularyAlternatives(_many_drug_ids(101), 0),
        drug_values.PublicFHIRFormularyAlternatives(("invalid",), 0),
        drug_values.PublicFHIRFormularyAlternatives((), True),
        drug_values.PublicFHIRFormularyAlternatives((), -1),
        drug_values.PublicFHIRFormularyAlternatives((), 101),
        drug_values.PublicFHIRFormularyAlternatives((DRUG_A,), 100),
        drug_values.PublicFHIRFormularyAlternatives((DRUG_A, DRUG_A), 0),
        drug_values.PublicFHIRFormularyAlternatives((DRUG_B, DRUG_A), 0),
    ),
)
def test_public_alternatives_reject_invalid_shape_bounds_and_order(alternatives):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drug_values.validate_public_fhir_formulary_alternatives(alternatives)


@pytest.mark.parametrize(
    "drug",
    (
        None,
        _public_drug(formulary_id=None),
        _public_drug(formulary_id="invalid"),
        _public_drug(alias_id=None),
        _public_drug(alias_id="invalid"),
        _public_drug(drug_id=None),
        _public_drug(drug_id="invalid"),
    ),
)
def test_public_drug_rejects_invalid_container_and_identity(drug):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drug_values.validate_public_fhir_formulary_drug(drug)


@pytest.mark.parametrize(
    "drug_page",
    (
        None,
        drug_values.PublicFHIRFormularyDrugPage([], None),
        drug_values.PublicFHIRFormularyDrugPage((), 1),
        drug_values.PublicFHIRFormularyDrugPage((), "invalid.cursor"),
    ),
)
def test_public_drug_page_rejects_invalid_container_and_cursor(drug_page):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drug_values.public_fhir_formulary_drug_page_payload(drug_page)
