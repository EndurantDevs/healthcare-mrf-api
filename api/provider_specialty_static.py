# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Curated provider-specialty taxonomy aliases."""

from __future__ import annotations


PRIMARY_CARE_TAXONOMY_CODES: tuple[str, ...] = (
    "207Q00000X",  # Family Medicine
    "207R00000X",  # Internal Medicine
    "208000000X",  # Pediatrics
    "208D00000X",  # General Practice
    "363LA2200X",  # Adult Health Nurse Practitioner
    "363LF0000X",  # Family Nurse Practitioner
    "363LP0200X",  # Pediatrics Nurse Practitioner
    "363LP2300X",  # Primary Care Nurse Practitioner
)

FAMILY_MEDICINE_TAXONOMY_CODES: tuple[str, ...] = (
    "207Q00000X",  # Family Medicine
    "208D00000X",  # General Practice
)

# Full NUCC 207X "Orthopaedic Surgery" family (base + subspecialties), so that a
# request for an orthopedic procedure (e.g. ACL reconstruction, CPT 29888) scopes to
# every orthopedic surgeon regardless of subspecialty.
ORTHOPAEDIC_SURGERY_TAXONOMY_CODES: tuple[str, ...] = (
    "207X00000X",  # Orthopaedic Surgery
    "207XP3100X",  # Pediatric Orthopaedic Surgery
    "207XS0106X",  # Hand Surgery (Orthopaedic)
    "207XS0114X",  # Adult Reconstructive Orthopaedic Surgery
    "207XS0117X",  # Orthopaedic Surgery of the Spine
    "207XX0004X",  # Foot and Ankle Orthopaedic Surgery
    "207XX0005X",  # Sports Medicine (Orthopaedic Surgery)
    "207XX0801X",  # Orthopaedic Trauma
)

_SPECIALTY_TAXONOMY_CODE_ALIASES: dict[str, tuple[str, ...]] = {
    "primary care": PRIMARY_CARE_TAXONOMY_CODES,
    "pcp": PRIMARY_CARE_TAXONOMY_CODES,
    "primary care physician": PRIMARY_CARE_TAXONOMY_CODES,
    "primary care provider": PRIMARY_CARE_TAXONOMY_CODES,
    "family doctor": FAMILY_MEDICINE_TAXONOMY_CODES,
    "family medicine": FAMILY_MEDICINE_TAXONOMY_CODES,
    "family physician": FAMILY_MEDICINE_TAXONOMY_CODES,
    "family practice": FAMILY_MEDICINE_TAXONOMY_CODES,
    "general practice": ("208D00000X",),
    "general practitioner": ("208D00000X",),
    "pediatrics": ("208000000X",),
    "pediatrician": ("208000000X",),
    "dermatology": ("207N00000X",),
    "dermatologist": ("207N00000X",),
    "cardiology": ("207RC0000X",),
    "cardiologist": ("207RC0000X",),
    "emergency medicine": ("207P00000X",),
    "emergency room": ("207P00000X",),
    "er": ("207P00000X",),
    "spinal surgeon": ("207XS0117X", "207T00000X"),
    "spine surgeon": ("207XS0117X", "207T00000X"),
    "orthopedic surgery": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedic surgery": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedic surgeon": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedic surgeon": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedics": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedics": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedist": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedist": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedic": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedic": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "ortho": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "neurology": ("2084N0400X",),
    "neurologist": ("2084N0400X",),
    "multiple sclerosis": ("2084N0400X",),
    "ent": ("207Y00000X",),
    "otolaryngology": ("207Y00000X",),
    "otolaryngologist": ("207Y00000X",),
    "gastroenterology": ("207RG0100X",),
    "gastroenterologist": ("207RG0100X",),
    "physical therapist": ("225100000X",),
    "physical therapy": ("225100000X",),
    "dietitian": ("133V00000X",),
    "registered dietitian": ("133V00000X",),
    "mental health": ("101YP2500X", "103TC0700X", "1041C0700X", "106H00000X"),
    "therapist": ("101YP2500X", "103TC0700X", "1041C0700X", "106H00000X"),
    "psychologist": ("103T00000X", "103TC0700X"),
    "rheumatology": ("207RR0500X",),
    "rheumatologist": ("207RR0500X",),
    "ob-gyn": ("207V00000X",),
    "obgyn": ("207V00000X",),
    "obstetrics and gynecology": ("207V00000X",),
    "obstetrics gynecology": ("207V00000X",),
    "obstetrics": ("207V00000X",),
    "obstetrician": ("207V00000X",),
    "gynecology": ("207V00000X",),
    "gynecologist": ("207V00000X",),
    "obstetrician gynecologist": ("207V00000X",),
    "infertility specialist": ("207VE0102X",),
    "ophthalmology": ("207W00000X",),
    "ophthalmologist": ("207W00000X",),
    "optometry": ("152W00000X",),
    "optometrist": ("152W00000X",),
    "laboratory": ("291U00000X",),
    "lab": ("291U00000X",),
    "clinical lab": ("291U00000X",),
    "dme": ("332B00000X",),
    "dme supplier": ("332B00000X",),
    "durable medical equipment": ("332B00000X",),
    "urgent care": ("261QU0200X",),
    "acupuncturist": ("171100000X",),
    "acupuncture": ("171100000X",),
    "massage therapist": ("225700000X",),
    "massage therapy": ("225700000X",),
    "hospice": ("251G00000X",),
    "home hospice": ("251G00000X", "251E00000X"),
    "hospital": ("282N00000X",),
}

_SPECIALTY_CLASSIFICATION_ALIASES: dict[str, str] = {
    "internal medicine": "Internal Medicine",
    "internist": "Internal Medicine",
    "dentist": "Dentist",
    "dental": "Dentist",
    "nurse practitioner": "Nurse Practitioner",
    "physician assistant": "Physician Assistant",
}

_CLASSIFICATION_BASE_TAXONOMY_CODE_ALIASES: dict[str, tuple[str, ...]] = {
    "family medicine": FAMILY_MEDICINE_TAXONOMY_CODES,
    "internal medicine": ("207R00000X",),
    "pediatrics": ("208000000X",),
    "general practice": ("208D00000X",),
    "dermatology": ("207N00000X",),
    "emergency medicine": ("207P00000X",),
    "dentist": ("122300000X",),
}


__all__ = (
    "FAMILY_MEDICINE_TAXONOMY_CODES",
    "ORTHOPAEDIC_SURGERY_TAXONOMY_CODES",
    "PRIMARY_CARE_TAXONOMY_CODES",
)
