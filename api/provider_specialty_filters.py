# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import difflib
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Mapping

logger = logging.getLogger(__name__)


PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")

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


def _normalize_specialty_key(value: str) -> str:
    # Same normalization as the terminology_synonym term_key column, so alias
    # lookups here and DB rows seeded by process.terminology_synonyms agree.
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _specialty_key_variants(value: str) -> tuple[str, ...]:
    # NUCC spells families with "&" (normalizes to a dropped token) while
    # callers often write "and"; probe both shapes so "obstetrics & gynecology"
    # and "obstetrics and gynecology" resolve identically.
    key = _normalize_specialty_key(value)
    if not key:
        return ()
    without_and = " ".join(token for token in key.split() if token != "and")
    if without_and and without_and != key:
        return (key, without_and)
    return (key,)


class SpecialtyResolutionCache:
    """Term -> taxonomy-code-set index shared by all specialty-filter surfaces.

    Precedence (later overwrites earlier): NUCC-derived names (classification
    families, specializations, display names) < terminology_synonym rows
    (domain=provider_type) < the curated static dict above. The static tier is
    always present, so cold starts and DB-less tests behave like today.
    """

    # Cache entry: (all_codes, base_codes). base_codes backs
    # include_subspecialties=false — for NUCC classification families it holds
    # the specialization-empty rows, falling back to all_codes for families
    # with no base row (e.g. Radiology) so the flag never empties a result.
    _RETRY_AFTER_FAILURE_SECONDS = 120.0

    def __init__(self, ttl_seconds: float = 900.0) -> None:
        self._ttl_seconds = ttl_seconds
        self._alias_codes: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
        self._loaded_at: float | None = None
        self._lock = asyncio.Lock()
        self._rebuild_from_static()

    def _rebuild_from_static(
        self,
        dynamic_entries: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] | None = None,
    ) -> None:
        entries_by_variant: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = dict(dynamic_entries or {})
        for alias, codes in _SPECIALTY_TAXONOMY_CODE_ALIASES.items():
            for variant in _specialty_key_variants(alias):
                entries_by_variant[variant] = (codes, codes)
        self._alias_codes = entries_by_variant

    def lookup(self, term: str, include_subspecialties: bool = True) -> tuple[str, ...]:
        """Resolve a specialty term to taxonomy codes from the shared cache."""

        for variant in _specialty_key_variants(term):
            entry = self._alias_codes.get(variant)
            if entry:
                all_codes, base_codes = entry
                return all_codes if include_subspecialties else base_codes
        return ()

    def suggestions(self, term: str, count: int = 3) -> tuple[str, ...]:
        """Return close normalized specialty aliases for unresolved input."""

        key = _normalize_specialty_key(term)
        if not key:
            return ()
        return tuple(difflib.get_close_matches(key, self._alias_codes.keys(), n=count, cutoff=0.75))

    @property
    def is_stale(self) -> bool:
        """Return whether the cache should be refreshed from the DB."""

        return self._loaded_at is None or (time.monotonic() - self._loaded_at) >= self._ttl_seconds

    async def ensure(self, session) -> None:
        """Refresh the cache once per TTL, preserving the current tier on failures."""

        if not self.is_stale:
            return
        async with self._lock:
            if not self.is_stale:
                return
            try:
                dynamic_entries = await self._load_dynamic_entries(session)
            except Exception:
                logger.warning("specialty resolution cache refresh failed; keeping current tier", exc_info=True)
                # Retry well before the full TTL so one transient DB error does
                # not pin static-only resolution for 15 minutes.
                self._loaded_at = time.monotonic() - self._ttl_seconds + self._RETRY_AFTER_FAILURE_SECONDS
                return
            self._rebuild_from_static(dynamic_entries)
            self._loaded_at = time.monotonic()

    async def _load_dynamic_entries(self, session) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
        """Load DB-backed NUCC and synonym specialty aliases."""

        from api.provider_specialty_cache_entries import load_dynamic_specialty_entries

        return await load_dynamic_specialty_entries(session, _specialty_key_variants)

_SPECIALTY_RESOLUTION_CACHE = SpecialtyResolutionCache()


async def ensure_specialty_resolution_cache(session) -> None:
    """Refresh the shared specialty cache from the DB when stale (lazy, TTL)."""
    await _SPECIALTY_RESOLUTION_CACHE.ensure(session)


def specialty_resolution_cache() -> SpecialtyResolutionCache:
    """Return the process-wide provider specialty resolution cache."""

    return _SPECIALTY_RESOLUTION_CACHE


_NORMALIZED_SPECIALTY_CLASSIFICATION_ALIASES: dict[str, str] = {
    variant: classification
    for alias, classification in _SPECIALTY_CLASSIFICATION_ALIASES.items()
    for variant in _specialty_key_variants(alias)
}


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_taxonomy_codes(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_items = value.replace(";", ",").split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = [value]
    codes: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        code = str(item or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return tuple(codes)


@dataclass(frozen=True)
class ProviderSpecialtyFilter:
    specialty: str | None = None
    classification: str | None = None
    taxonomy_codes: tuple[str, ...] = ()
    include_subspecialties: bool = False
    primary_only: bool = True
    use_classification_predicate: bool = True
    unresolved_specialty: str | None = None
    suggested_specialties: tuple[str, ...] = field(default=())

    @property
    def active(self) -> bool:
        return bool(self.classification or self.taxonomy_codes)

    def response_payload(self) -> dict[str, Any] | None:
        if not self.active:
            return None
        payload: dict[str, Any] = {
            "primary_only": self.primary_only,
            "include_subspecialties": self.include_subspecialties,
        }
        if self.specialty:
            payload["specialty"] = self.specialty
        if self.classification:
            payload["classification"] = self.classification
        if self.taxonomy_codes:
            payload["taxonomy_code_set"] = list(self.taxonomy_codes)
        return payload


def resolve_provider_specialty_filter(args: Mapping[str, Any]) -> ProviderSpecialtyFilter:
    specialty = str(args.get("specialty") or "").strip()
    classification = str(args.get("classification") or "").strip()
    taxonomy_codes = _normalize_taxonomy_codes(args.get("taxonomy_codes"))
    include_subspecialties = _normalize_bool(args.get("include_subspecialties"))
    primary_only = True
    if args.get("primary_only") not in (None, ""):
        primary_only = _normalize_bool(args.get("primary_only"))

    key = _normalize_specialty_key(specialty)
    if not taxonomy_codes and specialty:
        taxonomy_codes = _SPECIALTY_RESOLUTION_CACHE.lookup(
            specialty, include_subspecialties=include_subspecialties
        )
    if not classification and key in _NORMALIZED_SPECIALTY_CLASSIFICATION_ALIASES:
        classification = _NORMALIZED_SPECIALTY_CLASSIFICATION_ALIASES[key]
    unresolved_specialty: str | None = None
    suggested_specialties: tuple[str, ...] = ()
    if specialty and not taxonomy_codes and not classification:
        # No alias, terminology, or NUCC-name match. Deliberately do NOT guess a
        # classification predicate here: an arbitrary string as classification
        # silently returns empty provider lists on every surface. Callers see
        # unresolved_specialty and decide (guard: informative 400; list
        # endpoints: warn and skip the filter).
        unresolved_specialty = specialty
        suggested_specialties = _SPECIALTY_RESOLUTION_CACHE.suggestions(specialty)
    use_classification_predicate = True
    classification_key = classification.lower()
    if (
        classification_key
        and not taxonomy_codes
        and not include_subspecialties
        and classification_key in _CLASSIFICATION_BASE_TAXONOMY_CODE_ALIASES
    ):
        taxonomy_codes = _CLASSIFICATION_BASE_TAXONOMY_CODE_ALIASES[classification_key]
        use_classification_predicate = False

    return ProviderSpecialtyFilter(
        specialty=specialty or None,
        classification=classification or None,
        taxonomy_codes=taxonomy_codes,
        include_subspecialties=include_subspecialties,
        primary_only=primary_only,
        use_classification_predicate=use_classification_predicate,
        unresolved_specialty=unresolved_specialty,
        suggested_specialties=suggested_specialties,
    )


def provider_specialty_taxonomy_semijoin_sql(
    params: dict[str, Any],
    param_prefix: str,
    specialty_filter: ProviderSpecialtyFilter,
    *,
    schema: str = PTG2_SCHEMA,
    nt_alias: str | None = None,
    nucc_alias: str | None = None,
) -> str:
    """Uncorrelated ``SELECT npi FROM npi_taxonomy`` subquery for ``<npi> IN (...)``.

    Same row-level semantics as provider_specialty_taxonomy_exists_sql, but shaped
    so Postgres resolves the matching-NPI set ONCE (driving the
    (healthcare_provider_taxonomy_code, npi) index) and semi-joins it, instead of
    re-probing npi_taxonomy per outer row. Use this when the outer row source is
    unbounded — e.g. a whole group-plan network member table, where the correlated
    EXISTS form degraded to one index probe per member row.
    """
    if not specialty_filter.active:
        return ""

    nt = nt_alias or f"{param_prefix}_nt"
    nucc = nucc_alias or f"{param_prefix}_nucc"
    clauses: list[str] = []
    joins = ""

    if specialty_filter.primary_only:
        clauses.append(f"UPPER(COALESCE({nt}.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'")

    if specialty_filter.taxonomy_codes:
        code_placeholders: list[str] = []
        for idx, code in enumerate(specialty_filter.taxonomy_codes):
            key = f"{param_prefix}_taxonomy_code_{idx}"
            # Same sargability contract as the EXISTS builder: uppercase the
            # parameter, never wrap the indexed column.
            params[key] = str(code or "").upper()
            code_placeholders.append(f":{key}")
        clauses.append(
            f"{nt}.healthcare_provider_taxonomy_code IN ({', '.join(code_placeholders)})"
        )

    if specialty_filter.classification and specialty_filter.use_classification_predicate:
        classification_key = f"{param_prefix}_classification"
        params[classification_key] = specialty_filter.classification
        joins = f"\n                    JOIN {schema}.nucc_taxonomy {nucc} ON {nucc}.code = {nt}.healthcare_provider_taxonomy_code"
        clauses.append(f"LOWER(COALESCE({nucc}.classification, '')) = LOWER(:{classification_key})")
        if not specialty_filter.include_subspecialties:
            clauses.append(f"NULLIF(BTRIM(COALESCE({nucc}.specialization, '')), '') IS NULL")

    where = "\n                      AND ".join(clauses)
    return f"""SELECT {nt}.npi
                    FROM {schema}.npi_taxonomy {nt}{joins}
                    WHERE {where}"""


def provider_specialty_taxonomy_exists_sql(
    npi_sql: str,
    params: dict[str, Any],
    param_prefix: str,
    specialty_filter: ProviderSpecialtyFilter,
    *,
    schema: str = PTG2_SCHEMA,
    nt_alias: str | None = None,
    nucc_alias: str | None = None,
) -> str:
    if not specialty_filter.active:
        return ""

    nt = nt_alias or f"{param_prefix}_nt"
    nucc = nucc_alias or f"{param_prefix}_nucc"
    clauses = [f"{nt}.npi = {npi_sql}"]
    joins = ""

    if specialty_filter.primary_only:
        clauses.append(f"UPPER(COALESCE({nt}.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'")

    if specialty_filter.taxonomy_codes:
        code_placeholders: list[str] = []
        for idx, code in enumerate(specialty_filter.taxonomy_codes):
            key = f"{param_prefix}_taxonomy_code_{idx}"
            # Stored NUCC codes are canonically uppercase (verified: zero
            # mixed-case rows on dev); normalize the parameter instead of
            # wrapping the column in UPPER(), which defeats the
            # (taxonomy_code, npi) index and forces a 12M-row scan.
            params[key] = str(code or "").upper()
            code_placeholders.append(f":{key}")
        clauses.append(
            f"{nt}.healthcare_provider_taxonomy_code IN ({', '.join(code_placeholders)})"
        )

    if specialty_filter.classification and specialty_filter.use_classification_predicate:
        classification_key = f"{param_prefix}_classification"
        params[classification_key] = specialty_filter.classification
        joins = f"\n                    JOIN {schema}.nucc_taxonomy {nucc} ON {nucc}.code = {nt}.healthcare_provider_taxonomy_code"
        clauses.append(f"LOWER(COALESCE({nucc}.classification, '')) = LOWER(:{classification_key})")
        if not specialty_filter.include_subspecialties:
            clauses.append(f"NULLIF(BTRIM(COALESCE({nucc}.specialization, '')), '') IS NULL")

    where = "\n                      AND ".join(clauses)
    return f"""EXISTS (
                    SELECT 1
                    FROM {schema}.npi_taxonomy {nt}{joins}
                    WHERE {where}
                )"""
