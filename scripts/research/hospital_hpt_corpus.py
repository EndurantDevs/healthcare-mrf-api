#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic synthetic hospital-price MRF corpus codecs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True, order=True)
class Hospital:
    hospital_id: str
    name: str
    location: str
    address: str
    state: str
    license_number: str
    npis: tuple[str, ...]
    ein: str
    financial_aid_policy: str
    contract_provisions: tuple[tuple[str | None, str | None, str], ...]


@dataclass(frozen=True)
class PriceFact:
    hospital_id: str
    service_ordinal: int
    description: str
    code_system: str
    code: str
    setting: str
    billing_class: str
    modifiers: str | None
    drug_unit: str | None
    drug_type: str | None
    gross_amount: str | None
    discounted_cash: str | None
    payer_name: str
    plan_name: str
    negotiated_dollar: str | None
    negotiated_percentage: str | None
    negotiated_algorithm: str | None
    methodology: str
    minimum_amount: str
    maximum_amount: str
    median_amount: str
    percentile_10: str
    percentile_90: str
    allowed_count: str
    additional_generic_notes: str | None
    additional_payer_notes: str | None


ATTESTATION_TEXT = (
    "To the best of its knowledge and belief, this hospital has included all "
    "applicable standard charge information in accordance with the requirements "
    "of 45 CFR 180.50, and the information encoded is true, accurate, and complete "
    "as of the date in the file. This hospital has included all payer-specific "
    "negotiated charges in dollars that can be expressed as a dollar amount. For "
    "payer-specific negotiated charges that cannot be expressed as a dollar amount "
    "in the machine-readable file or not knowable in advance, the hospital attests "
    "that the payer-specific negotiated charge is based on a contractual algorithm, "
    "percentage or formula that precludes the provision of a dollar amount and has "
    "provided all necessary information available to the hospital for the public to "
    "be able to derive the dollar amount, including, but not limited to, the specific "
    "fee schedule or components referenced in such percentage, algorithm or formula."
)


def _npi(index: int) -> str:
    stem = f"1{index:08d}"[-9:]
    digits = [int(value) for value in f"80840{stem}"]
    total = sum(value * (1 if offset % 2 == 0 else 2) // 10 + value * (1 if offset % 2 == 0 else 2) % 10 for offset, value in enumerate(digits))
    return stem + str((-total) % 10)


def _synthetic_hospital(hospital_index: int) -> Hospital:
    ordinal = hospital_index + 1
    return Hospital(
        hospital_id=f"hospital-{ordinal:05d}",
        name=f"Synthetic Hospital {ordinal:05d}",
        location=f"Synthetic Campus {ordinal:05d}",
        address=f"{ordinal} Test Avenue, Example, TX 75001",
        state="TX",
        license_number=f"L{ordinal:07d}",
        npis=(_npi(hospital_index * 2 + 1), _npi(hospital_index * 2 + 2)),
        ein=f"{10 + hospital_index % 80:02d}{ordinal:07d}"[-9:],
        financial_aid_policy=f"https://example.test/hospitals/{ordinal}/financial-aid",
        contract_provisions=((None, None, f"Synthetic contract provision {ordinal}"),),
    )


def _negotiated_values(
    payer_index: int, amount_cents: int
) -> tuple[str | None, str | None, str | None]:
    charge_variant = payer_index % 3
    return (
        _money(amount_cents) if charge_variant == 0 else None,
        f"{80 + payer_index % 20}.125" if charge_variant == 1 else None,
        (
            f'Fee schedule tier "{payer_index % 5}", less 3%'
            if charge_variant == 2
            else None
        ),
    )


def _synthetic_fact(
    hospital: Hospital,
    hospital_index: int,
    fact_index: int,
    facts_per_hospital: int,
    payers: int,
) -> PriceFact:
    """Build one deterministic canonical fact with CMS v3 edge values."""

    service_ordinal, payer_index = divmod(fact_index, payers)
    service_minimum = 10_000 + hospital_index * 97 + service_ordinal * 113
    payer_count = min(payers, facts_per_hospital - service_ordinal * payers)
    service_maximum = service_minimum + (payer_count - 1) * 29
    amount_cents = service_minimum + payer_index * 29
    code_system = (
        "NDC"
        if service_ordinal % 11 == 10
        else ("CPT" if service_ordinal % 3 else "MS-DRG")
    )
    dollar, percentage, algorithm = _negotiated_values(payer_index, amount_cents)
    return PriceFact(
        hospital_id=hospital.hospital_id,
        service_ordinal=service_ordinal,
        description=f'Synthetic service {service_ordinal:06d}, "tier {service_ordinal % 5}"',
        code_system=code_system,
        code=(
            f"{12345000000 + service_ordinal:011d}"
            if code_system == "NDC"
            else f"{10000 + service_ordinal % 89999:05d}"
        ),
        setting="outpatient" if service_ordinal % 2 else "inpatient",
        billing_class=("professional", "facility", "both")[service_ordinal % 3],
        modifiers="25|59" if service_ordinal % 7 == 0 else None,
        drug_unit="2.5" if code_system == "NDC" else None,
        drug_type="ML" if code_system == "NDC" else None,
        gross_amount=_money(service_maximum + 5_000),
        discounted_cash=_money(service_minimum - 500),
        payer_name=f"Synthetic Payer {payer_index + 1:03d}",
        plan_name=f"Synthetic Plan {payer_index + 1:03d}",
        negotiated_dollar=dollar,
        negotiated_percentage=percentage,
        negotiated_algorithm=algorithm,
        methodology="fee schedule" if service_ordinal % 2 else "case rate",
        minimum_amount=_money(service_minimum),
        maximum_amount=_money(service_maximum),
        median_amount=_money(amount_cents),
        percentile_10=_money(amount_cents - 50),
        percentile_90=_money(amount_cents + 50),
        allowed_count=(
            "1 through 10"
            if (service_ordinal + payer_index) % 19 == 0
            else str(11 + service_ordinal % 17)
        ),
        additional_generic_notes=None,
        additional_payer_notes=(
            'Synthetic payer note, "reviewed"'
            if service_ordinal % 13 == 0
            else None
        ),
    )


def build_corpus(
    *, hospitals: int, facts_per_hospital: int, payers: int
) -> tuple[list[Hospital], list[PriceFact]]:
    """Build deterministic hospital metadata and canonical price facts."""
    if min(hospitals, facts_per_hospital, payers) < 1:
        raise ValueError("hospitals, facts_per_hospital, and payers must be positive")
    hospital_rows = [_synthetic_hospital(index) for index in range(hospitals)]
    facts = [
        _synthetic_fact(
            hospital, hospital_index, fact_index, facts_per_hospital, payers
        )
        for hospital_index, hospital in enumerate(hospital_rows)
        for fact_index in range(facts_per_hospital)
    ]
    return hospital_rows, facts


def semantic_digest(hospitals: Iterable[Hospital], facts: Iterable[PriceFact]) -> str:
    """Hash sorted hospital metadata and facts for exact parity checks."""
    digest = hashlib.sha256()
    for kind, rows in (("hospital", hospitals), ("fact", facts)):
        canonical_rows = sorted(
            rows,
            key=lambda row: json.dumps(
                asdict(row), separators=(",", ":"), sort_keys=True
            ),
        )
        for row in canonical_rows:
            digest.update(kind.encode())
            digest.update(b"\t")
            digest.update(json.dumps(asdict(row), separators=(",", ":"), sort_keys=True).encode())
            digest.update(b"\n")
    return digest.hexdigest()


def _money(cents: int) -> str:
    return _decimal_text(Decimal(cents) / 100)


def _decimal_text(value: object) -> str:
    decimal = Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("numeric value must be finite")
    rendered = format(decimal, "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def _json_number(value: str | None) -> int | float | None:
    if value is None:
        return None
    return float(value) if "." in value else int(value)


def _decimal_or_none(value: object) -> str | None:
    return None if value in (None, "") else _decimal_text(value)


def _text_or_none(value: str) -> str | None:
    return value or None


def _mapping_text_or_none(fields_by_name: dict[str, object], name: str) -> str | None:
    value = fields_by_name.get(name)
    return str(value) if value is not None else None


def _filename(hospital: Hospital, suffix: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", hospital.name.lower()).strip("-")
    return f"{hospital.ein}_{slug}_standardcharges.{suffix}"


def _group_facts(facts: Iterable[PriceFact]) -> dict[str, list[PriceFact]]:
    facts_by_hospital: dict[str, list[PriceFact]] = {}
    for fact in facts:
        facts_by_hospital.setdefault(fact.hospital_id, []).append(fact)
    return facts_by_hospital


def _service_groups(facts: Iterable[PriceFact]) -> list[list[PriceFact]]:
    facts_by_service: dict[tuple[str, int], list[PriceFact]] = {}
    for fact in facts:
        key = (fact.hospital_id, fact.service_ordinal)
        facts_by_service.setdefault(key, []).append(fact)
    return [facts_by_service[key] for key in sorted(facts_by_service)]


def _fact_sort_key(fact: PriceFact) -> tuple[str, int, str]:
    return (
        fact.hospital_id,
        fact.service_ordinal,
        json.dumps(asdict(fact), separators=(",", ":"), sort_keys=True),
    )


def _without_nulls(fields_by_name: dict[str, object]) -> dict[str, object]:
    return {name: value for name, value in fields_by_name.items() if value is not None}


def _payer_payload(fact: PriceFact) -> dict[str, object]:
    return _without_nulls(
        {
            "payer_name": fact.payer_name,
            "plan_name": fact.plan_name,
            "standard_charge_dollar": _json_number(fact.negotiated_dollar),
            "standard_charge_percentage": _json_number(
                fact.negotiated_percentage
            ),
            "standard_charge_algorithm": fact.negotiated_algorithm,
            "methodology": fact.methodology,
            "median_amount": _json_number(fact.median_amount),
            "10th_percentile": _json_number(fact.percentile_10),
            "90th_percentile": _json_number(fact.percentile_90),
            "count": fact.allowed_count,
            "additional_payer_notes": fact.additional_payer_notes,
        }
    )


def write_json(path: Path, hospital: Hospital, facts: list[PriceFact]) -> None:
    """Write one synthetic CMS-shaped JSON MRF."""
    services = []
    for group in _service_groups(facts):
        first = group[0]
        charge = _without_nulls(
            {
                "setting": first.setting,
                "billing_class": first.billing_class,
                "minimum": _json_number(first.minimum_amount),
                "maximum": _json_number(first.maximum_amount),
                "gross_charge": _json_number(first.gross_amount),
                "discounted_cash": _json_number(first.discounted_cash),
                "modifier_code": first.modifiers.split("|") if first.modifiers else None,
                "additional_generic_notes": first.additional_generic_notes,
                "payers_information": [
                    _payer_payload(fact) for fact in sorted(group, key=_fact_sort_key)
                ],
            }
        )
        service_by_field = {
            "description": first.description,
            "code_information": [{"code": first.code, "type": first.code_system}],
            "standard_charges": [charge],
        }
        if first.drug_unit is not None:
            service_by_field["drug_information"] = {
                "unit": _json_number(first.drug_unit), "type": first.drug_type,
            }
        services.append(service_by_field)
    path.write_text(
        json.dumps(
            {
                "hospital_name": hospital.name,
                "last_updated_on": "2026-08-25",
                "version": "3.0.0",
                "location_name": [hospital.location],
                "hospital_address": [hospital.address],
                "license_information": {"state": hospital.state,
                                        "license_number": hospital.license_number},
                "type_2_npi": list(hospital.npis),
                "financial_aid_policy": hospital.financial_aid_policy,
                "general_contract_provisions": [
                    _without_nulls(
                        {
                            "payer_name": payer_name,
                            "plan_name": plan_name,
                            "provisions": provisions,
                        }
                    )
                    for payer_name, plan_name, provisions in hospital.contract_provisions
                ],
                "attestation": {"attestation": ATTESTATION_TEXT,
                                "confirm_attestation": True, "attester_name": "Synthetic Attester"},
                "standard_charge_information": services,
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _ein_from_path(path: Path) -> str:
    ein = path.name.split("_", 1)[0]
    if not re.fullmatch(r"\d{9}", ein):
        raise ValueError(f"MRF filename does not begin with a nine-digit EIN: {path.name}")
    return ein


def _hospital_from_values(
    hospital_id: str, fields_by_name: dict[str, object], path: Path
) -> Hospital:
    license_information = fields_by_name.get("license_information") or {}
    if not isinstance(license_information, dict):
        raise ValueError("license_information must be an object")
    locations = fields_by_name.get("location_name") or []
    addresses = fields_by_name.get("hospital_address") or []
    npis = fields_by_name.get("type_2_npi") or []
    if isinstance(locations, str):
        locations = [locations]
    if isinstance(addresses, str):
        addresses = [addresses]
    if isinstance(npis, str):
        npis = [part.strip() for part in npis.split("|") if part.strip()]
    raw_provisions = fields_by_name.get("general_contract_provisions") or []
    if isinstance(raw_provisions, str):
        raw_provisions = (
            [{"provisions": raw_provisions}] if raw_provisions else []
        )
    provisions = tuple(
        (
            _mapping_text_or_none(provision, "payer_name"),
            _mapping_text_or_none(provision, "plan_name"),
            str(provision["provisions"]),
        )
        for provision in raw_provisions
    )
    return Hospital(
        hospital_id=hospital_id,
        name=str(fields_by_name["hospital_name"]),
        location=str(locations[0]),
        address=str(addresses[0]),
        state=str(license_information.get("state") or "TX"),
        license_number=str(
            license_information.get("license_number")
            or fields_by_name.get("license_number|TX")
            or ""
        ),
        npis=tuple(str(npi) for npi in npis),
        ein=_ein_from_path(path),
        financial_aid_policy=str(fields_by_name.get("financial_aid_policy") or ""),
        contract_provisions=provisions,
    )


def _json_price_fact(
    hospital_id: str,
    service_ordinal: int,
    service: dict[str, object],
    code: dict[str, object],
    charge: dict[str, object],
    payer: dict[str, object],
    drug: dict[str, object],
) -> PriceFact:
    modifiers = charge.get("modifier_code") or []
    return PriceFact(
        hospital_id=hospital_id,
        service_ordinal=service_ordinal,
        description=str(service["description"]),
        code_system=str(code["type"]),
        code=str(code["code"]),
        setting=str(charge["setting"]),
        billing_class=str(charge["billing_class"]),
        modifiers="|".join(str(modifier) for modifier in modifiers) or None,
        drug_unit=_decimal_or_none(drug.get("unit")),
        drug_type=_mapping_text_or_none(drug, "type"),
        gross_amount=_decimal_or_none(charge.get("gross_charge")),
        discounted_cash=_decimal_or_none(charge.get("discounted_cash")),
        payer_name=str(payer["payer_name"]),
        plan_name=str(payer["plan_name"]),
        negotiated_dollar=_decimal_or_none(
            payer.get("standard_charge_dollar")
        ),
        negotiated_percentage=_decimal_or_none(
            payer.get("standard_charge_percentage")
        ),
        negotiated_algorithm=_mapping_text_or_none(
            payer, "standard_charge_algorithm"
        ),
        methodology=str(payer["methodology"]),
        minimum_amount=_decimal_text(charge["minimum"]),
        maximum_amount=_decimal_text(charge["maximum"]),
        median_amount=_decimal_text(payer["median_amount"]),
        percentile_10=_decimal_text(payer["10th_percentile"]),
        percentile_90=_decimal_text(payer["90th_percentile"]),
        allowed_count=str(payer["count"]),
        additional_generic_notes=_mapping_text_or_none(
            charge, "additional_generic_notes"
        ),
        additional_payer_notes=_mapping_text_or_none(
            payer, "additional_payer_notes"
        ),
    )


def read_json(path: Path, hospital_id: str) -> tuple[Hospital, list[PriceFact]]:
    """Read the supported JSON subset into canonical facts."""
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    hospital = _hospital_from_values(hospital_id, payload, path)
    facts = []
    for service_ordinal, service in enumerate(
        payload["standard_charge_information"]
    ):
        drug = service.get("drug_information") or {}
        for charge in service["standard_charges"]:
            for code in service["code_information"]:
                for payer in charge.get("payers_information") or []:
                    facts.append(
                        _json_price_fact(
                            hospital_id,
                            service_ordinal,
                            service,
                            code,
                            charge,
                            payer,
                            drug,
                        )
                    )
    return hospital, facts
