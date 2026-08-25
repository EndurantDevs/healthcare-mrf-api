# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic CMS v3 tall- and wide-CSV codecs for the schema bakeoff."""

from __future__ import annotations

import csv
from pathlib import Path

from scripts.research.hospital_hpt_corpus import (
    ATTESTATION_TEXT,
    Hospital,
    PriceFact,
    _decimal_or_none,
    _decimal_text,
    _fact_sort_key,
    _hospital_from_values,
    _service_groups,
    _text_or_none,
)

GENERAL_HEADERS = (
    "hospital_name",
    "last_updated_on",
    "version",
    "location_name",
    "hospital_address",
    "license_number|TX",
    "type_2_npi",
    "financial_aid_policy",
    "general_contract_provisions",
    ATTESTATION_TEXT,
    "attester_name",
)
TALL_HEADERS = (
    "description",
    "code|1",
    "code|1|type",
    "modifiers",
    "setting",
    "billing_class",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "standard_charge|gross",
    "standard_charge|discounted_cash",
    "payer_name",
    "plan_name",
    "standard_charge|negotiated_dollar",
    "standard_charge|negotiated_percentage",
    "standard_charge|negotiated_algorithm",
    "median_amount",
    "10th_percentile",
    "90th_percentile",
    "count",
    "standard_charge|methodology",
    "standard_charge|min",
    "standard_charge|max",
    "additional_generic_notes",
)
WIDE_BASE_HEADERS = (
    "description",
    "code|1",
    "code|1|type",
    "modifiers",
    "setting",
    "billing_class",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "standard_charge|gross",
    "standard_charge|discounted_cash",
    "standard_charge|min",
    "standard_charge|max",
    "additional_generic_notes",
)
WIDE_PAYER_FIELDS = (
    "negotiated_dollar",
    "negotiated_percentage",
    "negotiated_algorithm",
    "median_amount",
    "10th_percentile",
    "90th_percentile",
    "count",
    "methodology",
    "additional_payer_notes",
)


def _general_values(hospital: Hospital) -> tuple[str, ...]:
    return (
        hospital.name,
        "2026-08-25",
        "3.0.0",
        hospital.location,
        hospital.address,
        hospital.license_number,
        " | ".join(hospital.npis),
        hospital.financial_aid_policy,
        hospital.contract_provisions[0][2],
        "TRUE",
        "Synthetic Attester",
    )


def write_tall_csv(path: Path, hospital: Hospital, facts: list[PriceFact]) -> None:
    """Write one synthetic CMS-shaped tall CSV MRF."""
    width = max(len(GENERAL_HEADERS), len(TALL_HEADERS))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow((*GENERAL_HEADERS, *("" for _ in range(width - len(GENERAL_HEADERS)))))
        writer.writerow((*_general_values(hospital), *("" for _ in range(width - len(GENERAL_HEADERS)))))
        writer.writerow(TALL_HEADERS)
        for fact in sorted(facts, key=_fact_sort_key):
            writer.writerow(
                (
                    fact.description,
                    fact.code,
                    fact.code_system,
                    fact.modifiers,
                    fact.setting,
                    fact.billing_class,
                    fact.drug_unit,
                    fact.drug_type,
                    fact.gross_amount,
                    fact.discounted_cash,
                    fact.payer_name,
                    fact.plan_name,
                    fact.negotiated_dollar,
                    fact.negotiated_percentage,
                    fact.negotiated_algorithm,
                    fact.median_amount,
                    fact.percentile_10,
                    fact.percentile_90,
                    fact.allowed_count,
                    fact.methodology,
                    fact.minimum_amount,
                    fact.maximum_amount,
                    fact.additional_payer_notes or fact.additional_generic_notes,
                )
            )


def write_wide_csv(path: Path, hospital: Hospital, facts: list[PriceFact]) -> None:
    """Write one synthetic CMS-shaped wide CSV MRF."""
    payer_plans = sorted({(fact.payer_name, fact.plan_name) for fact in facts})
    data_headers = _wide_headers(payer_plans)
    width = max(len(GENERAL_HEADERS), len(data_headers))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow((*GENERAL_HEADERS, *("" for _ in range(width - len(GENERAL_HEADERS)))))
        writer.writerow((*_general_values(hospital), *("" for _ in range(width - len(GENERAL_HEADERS)))))
        writer.writerow(data_headers)
        for group in _service_groups(facts):
            writer.writerow(_wide_service_values(group, payer_plans))


def _wide_headers(payer_plans: list[tuple[str, str]]) -> list[str]:
    headers = list(WIDE_BASE_HEADERS)
    headers.extend(
        (
            f"standard_charge|{payer}|{plan}|{field}"
            if field.startswith("negotiated_") or field == "methodology"
            else f"{field}|{payer}|{plan}"
        )
        for payer, plan in payer_plans
        for field in WIDE_PAYER_FIELDS
    )
    return headers


def _wide_service_values(
    service_facts: list[PriceFact], payer_plans: list[tuple[str, str]]
) -> list[object]:
    first = service_facts[0]
    fact_by_payer = {
        (fact.payer_name, fact.plan_name): fact for fact in service_facts
    }
    values: list[object] = [
        first.description, first.code, first.code_system, first.modifiers,
        first.setting, first.billing_class, first.drug_unit, first.drug_type, first.gross_amount,
        first.discounted_cash, first.minimum_amount, first.maximum_amount,
        first.additional_generic_notes,
    ]
    for payer_plan in payer_plans:
        fact = fact_by_payer.get(payer_plan)
        values.extend(
            ("", "", "", "", "", "", "", "", "")
            if fact is None
            else (
                fact.negotiated_dollar, fact.negotiated_percentage,
                fact.negotiated_algorithm, fact.median_amount,
                fact.percentile_10, fact.percentile_90, fact.allowed_count,
                fact.methodology, fact.additional_payer_notes,
            )
        )
    return values


def _csv_preamble(path: Path) -> tuple[dict[str, str], list[str], list[list[str]]]:
    with path.open(newline="", encoding="utf-8-sig") as stream:
        rows = list(csv.reader(stream))
    if len(rows) < 3:
        raise ValueError(f"CSV has fewer than three rows: {path}")
    return dict(zip(rows[0], rows[1])), rows[2], rows[3:]


def _csv_hospital(path: Path, hospital_id: str, general: dict[str, str]) -> Hospital:
    license_key = next((key for key in general if key.startswith("license_number|")), "")
    state = license_key.rsplit("|", 1)[-1] if license_key else ""
    return _hospital_from_values(
        hospital_id,
        {
            **general,
            "location_name": [part.strip() for part in general["location_name"].split("|")],
            "hospital_address": [part.strip() for part in general["hospital_address"].split("|")],
            "type_2_npi": [part.strip() for part in general["type_2_npi"].split("|")],
            "license_information": {"state": state, "license_number": general.get(license_key, "")},
        },
        path,
    )


def _has_tall_charge(fact_by_field: dict[str, str]) -> bool:
    return any(
        fact_by_field.get(name)
        for name in (
            "standard_charge|negotiated_dollar",
            "standard_charge|negotiated_percentage",
            "standard_charge|negotiated_algorithm",
        )
    )


def _tall_service_key(fact_by_field: dict[str, str]) -> tuple[str, ...]:
    return tuple(
        fact_by_field[name]
        for name in (
            "description", "code|1", "code|1|type", "setting", "billing_class"
        )
    )


def _tall_price_fact(
    hospital_id: str, service_ordinal: int, fact_by_field: dict[str, str]
) -> PriceFact:
    return PriceFact(
        hospital_id=hospital_id,
        service_ordinal=service_ordinal,
        description=fact_by_field["description"],
        code_system=fact_by_field["code|1|type"],
        code=fact_by_field["code|1"],
        setting=fact_by_field["setting"],
        billing_class=fact_by_field["billing_class"],
        modifiers=_text_or_none(fact_by_field["modifiers"]),
        drug_unit=_decimal_or_none(fact_by_field["drug_unit_of_measurement"]),
        drug_type=_text_or_none(fact_by_field["drug_type_of_measurement"]),
        gross_amount=_decimal_or_none(fact_by_field["standard_charge|gross"]),
        discounted_cash=_decimal_or_none(
            fact_by_field["standard_charge|discounted_cash"]
        ),
        payer_name=fact_by_field["payer_name"],
        plan_name=fact_by_field["plan_name"],
        negotiated_dollar=_decimal_or_none(
            fact_by_field["standard_charge|negotiated_dollar"]
        ),
        negotiated_percentage=_decimal_or_none(
            fact_by_field["standard_charge|negotiated_percentage"]
        ),
        negotiated_algorithm=_text_or_none(
            fact_by_field["standard_charge|negotiated_algorithm"]
        ),
        methodology=fact_by_field["standard_charge|methodology"],
        minimum_amount=_decimal_text(fact_by_field["standard_charge|min"]),
        maximum_amount=_decimal_text(fact_by_field["standard_charge|max"]),
        median_amount=_decimal_text(fact_by_field["median_amount"]),
        percentile_10=_decimal_text(fact_by_field["10th_percentile"]),
        percentile_90=_decimal_text(fact_by_field["90th_percentile"]),
        allowed_count=fact_by_field["count"],
        additional_generic_notes=None,
        additional_payer_notes=_text_or_none(
            fact_by_field["additional_generic_notes"]
        ),
    )


def read_tall_csv(path: Path, hospital_id: str) -> tuple[Hospital, list[PriceFact]]:
    """Read the supported tall CSV subset into canonical facts."""
    general, headers, csv_rows = _csv_preamble(path)
    hospital = _csv_hospital(path, hospital_id, general)
    facts = []
    service_ordinal = -1
    prior_service_key = None
    for csv_values in csv_rows:
        fact_by_field = dict(zip(headers, csv_values))
        if not _has_tall_charge(fact_by_field):
            continue
        service_key = _tall_service_key(fact_by_field)
        if service_key != prior_service_key:
            service_ordinal += 1
            prior_service_key = service_key
        facts.append(_tall_price_fact(hospital_id, service_ordinal, fact_by_field))
    return hospital, facts


def _wide_payer_plans(headers: list[str]) -> list[tuple[str, str]]:
    return sorted(
        {
            tuple(header.split("|")[1:3])
            for header in headers
            if header.startswith("standard_charge|")
            and header.endswith("|negotiated_dollar")
        }
    )


def _wide_price_fact(
    hospital_id: str,
    service_ordinal: int,
    fact_by_field: dict[str, str],
    payer_name: str,
    plan_name: str,
) -> PriceFact | None:
    standard_prefix = f"standard_charge|{payer_name}|{plan_name}|"
    suffix = f"|{payer_name}|{plan_name}"
    dollar = fact_by_field.get(standard_prefix + "negotiated_dollar")
    percentage = fact_by_field.get(standard_prefix + "negotiated_percentage")
    algorithm = fact_by_field.get(standard_prefix + "negotiated_algorithm")
    if not any((dollar, percentage, algorithm)):
        return None
    return PriceFact(
        hospital_id=hospital_id,
        service_ordinal=service_ordinal,
        description=fact_by_field["description"],
        code_system=fact_by_field["code|1|type"],
        code=fact_by_field["code|1"],
        setting=fact_by_field["setting"],
        billing_class=fact_by_field["billing_class"],
        modifiers=_text_or_none(fact_by_field["modifiers"]),
        drug_unit=_decimal_or_none(fact_by_field["drug_unit_of_measurement"]),
        drug_type=_text_or_none(fact_by_field["drug_type_of_measurement"]),
        gross_amount=_decimal_or_none(fact_by_field["standard_charge|gross"]),
        discounted_cash=_decimal_or_none(
            fact_by_field["standard_charge|discounted_cash"]
        ),
        payer_name=payer_name,
        plan_name=plan_name,
        negotiated_dollar=_decimal_or_none(dollar),
        negotiated_percentage=_decimal_or_none(percentage),
        negotiated_algorithm=_text_or_none(algorithm or ""),
        methodology=fact_by_field[standard_prefix + "methodology"],
        minimum_amount=_decimal_text(fact_by_field["standard_charge|min"]),
        maximum_amount=_decimal_text(fact_by_field["standard_charge|max"]),
        median_amount=_decimal_text(fact_by_field[f"median_amount{suffix}"]),
        percentile_10=_decimal_text(fact_by_field[f"10th_percentile{suffix}"]),
        percentile_90=_decimal_text(fact_by_field[f"90th_percentile{suffix}"]),
        allowed_count=fact_by_field[f"count{suffix}"],
        additional_generic_notes=_text_or_none(
            fact_by_field["additional_generic_notes"]
        ),
        additional_payer_notes=_text_or_none(
            fact_by_field[f"additional_payer_notes{suffix}"]
        ),
    )


def read_wide_csv(path: Path, hospital_id: str) -> tuple[Hospital, list[PriceFact]]:
    """Read the supported wide CSV subset into canonical facts."""
    general, headers, csv_rows = _csv_preamble(path)
    hospital = _csv_hospital(path, hospital_id, general)
    payer_plans = _wide_payer_plans(headers)
    facts = []
    for service_ordinal, csv_values in enumerate(csv_rows):
        fact_by_field = dict(zip(headers, csv_values))
        for payer_name, plan_name in payer_plans:
            fact = _wide_price_fact(
                hospital_id, service_ordinal, fact_by_field, payer_name, plan_name
            )
            if fact is not None:
                facts.append(fact)
    return hospital, facts
