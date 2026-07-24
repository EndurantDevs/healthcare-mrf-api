# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from collections.abc import Callable, Mapping

from sqlalchemy import func, or_, select

from db.models import NUCCTaxonomy, TerminologySynonym

SpecialtyEntries = dict[str, tuple[tuple[str, ...], tuple[str, ...]]]
SpecialtyKeyVariants = Callable[[str], tuple[str, ...]]


def _index_entry(
    entries_by_variant: SpecialtyEntries,
    key_variants: SpecialtyKeyVariants,
    entry_name: str,
    code_list: list[str],
    base_code_list: list[str] | None = None,
) -> None:
    deduped_codes = tuple(dict.fromkeys(code_list))
    if not deduped_codes:
        return
    base_codes = tuple(dict.fromkeys(base_code_list)) if base_code_list else deduped_codes
    for variant in key_variants(entry_name):
        entries_by_variant[variant] = (deduped_codes, base_codes)


async def _load_nucc_entries(session, key_variants: SpecialtyKeyVariants) -> SpecialtyEntries:
    nucc_table = NUCCTaxonomy.__table__
    nucc_result = await session.execute(
        select(
            nucc_table.c.code,
            nucc_table.c.classification,
            nucc_table.c.specialization,
            nucc_table.c.display_name,
        )
    )
    codes_by_classification: dict[str, list[str]] = {}
    base_codes_by_classification: dict[str, list[str]] = {}
    codes_by_specialization: dict[str, list[str]] = {}
    codes_by_display: dict[str, list[str]] = {}
    for nucc_mapping in nucc_result.mappings():
        code = str(nucc_mapping.get("code") or "").strip().upper()
        if not code:
            continue
        classification = str(nucc_mapping.get("classification") or "").strip()
        specialization = str(nucc_mapping.get("specialization") or "").strip()
        display_name = str(nucc_mapping.get("display_name") or "").strip()
        if classification:
            codes_by_classification.setdefault(classification, []).append(code)
            if not specialization:
                base_codes_by_classification.setdefault(classification, []).append(code)
        if specialization:
            codes_by_specialization.setdefault(specialization, []).append(code)
        if display_name:
            codes_by_display.setdefault(display_name, []).append(code)

    entries_by_variant: SpecialtyEntries = {}
    # Order matters: display names are the most specific, then specializations;
    # classification families win same-key collisions so family terms mean the
    # whole family. Direct specialization/display names keep base == all.
    for entry_name, code_list in codes_by_display.items():
        _index_entry(entries_by_variant, key_variants, entry_name, code_list)
    for entry_name, code_list in codes_by_specialization.items():
        _index_entry(entries_by_variant, key_variants, entry_name, code_list)
    for entry_name, code_list in codes_by_classification.items():
        _index_entry(
            entries_by_variant,
            key_variants,
            entry_name,
            code_list,
            base_code_list=base_codes_by_classification.get(entry_name),
        )
    return entries_by_variant


async def _load_synonym_entries(session, key_variants: SpecialtyKeyVariants) -> SpecialtyEntries:
    synonym_table = TerminologySynonym.__table__
    synonym_result = await session.execute(
        select(
            synonym_table.c.synonym,
            synonym_table.c.target_system,
            synonym_table.c.target_code,
            synonym_table.c.metadata_json,
        ).where(synonym_table.c.domain == "provider_type")
    )
    # Rows sharing one key are unioned and sorted so un-ORDERed SELECT output
    # cannot decide the winner across workers or TTL refreshes.
    codes_by_synonym_variant: dict[str, list[str]] = {}
    for synonym_mapping in synonym_result.mappings():
        synonym = str(synonym_mapping.get("synonym") or "").strip()
        if not synonym:
            continue
        code = _synonym_code(synonym_mapping)
        if code:
            for variant in key_variants(synonym):
                codes_by_synonym_variant.setdefault(variant, []).append(code)

    entries_by_variant: SpecialtyEntries = {}
    for variant, code_list in codes_by_synonym_variant.items():
        unioned_codes = tuple(sorted(dict.fromkeys(code_list)))
        entries_by_variant[variant] = (unioned_codes, unioned_codes)
    return entries_by_variant


async def load_dynamic_specialty_entries_by_variant(session, key_variants: SpecialtyKeyVariants) -> SpecialtyEntries:
    """Load DB-backed NUCC and synonym specialty aliases."""

    entries_by_variant = await _load_nucc_entries(session, key_variants)
    entries_by_variant.update(await _load_synonym_entries(session, key_variants))
    return entries_by_variant


def _normalized_term_sql(column):
    """Return the SQL form used by terminology_synonym.term_key.

    NUCC has no normalized-term column.  This expression is deliberately used
    only against its small reference table after the indexed synonym lookup
    misses; it prevents loading the reference table into an API worker.
    """

    punctuation_normalized = func.regexp_replace(
        func.lower(column),
        "[^a-z0-9]+",
        " ",
        "g",
    )
    return func.regexp_replace(func.btrim(punctuation_normalized), "\\s+", " ", "g")


def _synonym_code(mapping) -> str:
    target_system = str(mapping.get("target_system") or "").strip().upper()
    if target_system == "NUCC":
        return str(mapping.get("target_code") or "").strip().upper()
    metadata_raw = mapping.get("metadata_json")
    if not metadata_raw:
        return ""
    try:
        return str((json.loads(metadata_raw) or {}).get("nucc_code") or "").strip().upper()
    except (AttributeError, TypeError, ValueError):
        return ""


def _sorted_codes(codes: list[str]) -> tuple[str, ...]:
    return tuple(sorted({code for code in codes if code}))


async def _load_synonym_codes(
    session,
    variants: tuple[str, ...],
) -> tuple[str, ...]:
    synonym_table = TerminologySynonym.__table__
    synonym_result = await session.execute(
        select(
            synonym_table.c.target_system,
            synonym_table.c.target_code,
            synonym_table.c.metadata_json,
        ).where(
            synonym_table.c.domain == "provider_type",
            synonym_table.c.term_key.in_(variants),
        )
    )
    return _sorted_codes([
        _synonym_code(synonym_mapping)
        for synonym_mapping in synonym_result.mappings()
    ])


async def _load_matching_nucc_mappings(
    session,
    variants: tuple[str, ...],
) -> list[Mapping[str, object]]:
    nucc_table = NUCCTaxonomy.__table__
    classification_key = _normalized_term_sql(nucc_table.c.classification)
    specialization_key = _normalized_term_sql(nucc_table.c.specialization)
    display_key = _normalized_term_sql(nucc_table.c.display_name)
    nucc_result = await session.execute(
        select(
            nucc_table.c.code,
            nucc_table.c.classification,
            nucc_table.c.specialization,
            nucc_table.c.display_name,
        ).where(
            or_(
                classification_key.in_(variants),
                specialization_key.in_(variants),
                display_key.in_(variants),
            )
        )
    )
    return list(nucc_result.mappings())


def _matching_nucc_mappings(
    nucc_mappings: list[Mapping[str, object]],
    field_name: str,
    variant_set: set[str],
) -> list[Mapping[str, object]]:
    return [
        nucc_mapping
        for nucc_mapping in nucc_mappings
        if _normalized_term_value(nucc_mapping.get(field_name)) in variant_set
    ]


def _codes_from_nucc_mappings(
    nucc_mappings: list[Mapping[str, object]],
) -> tuple[str, ...]:
    return _sorted_codes([
        str(nucc_mapping.get("code") or "").strip().upper()
        for nucc_mapping in nucc_mappings
    ])


def _resolve_nucc_entry(
    nucc_mappings: list[Mapping[str, object]],
    variants: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    variant_set = set(variants)
    classification_mappings = _matching_nucc_mappings(
        nucc_mappings,
        "classification",
        variant_set,
    )
    if classification_mappings:
        all_codes = _codes_from_nucc_mappings(classification_mappings)
        base_codes = _codes_from_nucc_mappings([
            nucc_mapping
            for nucc_mapping in classification_mappings
            if not str(nucc_mapping.get("specialization") or "").strip()
        ])
        return all_codes, base_codes or all_codes

    specialization_mappings = _matching_nucc_mappings(
        nucc_mappings,
        "specialization",
        variant_set,
    )
    matching_mappings = specialization_mappings or _matching_nucc_mappings(
        nucc_mappings,
        "display_name",
        variant_set,
    )
    codes = _codes_from_nucc_mappings(matching_mappings)
    return (codes, codes) if codes else None


async def load_dynamic_specialty_entry(
    session,
    key_variants: SpecialtyKeyVariants,
    term: str,
) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    """Resolve one dynamic provider-specialty term from PostgreSQL.

    The terminology query uses the ``(domain, term_key)`` index.  On a miss,
    the NUCC reference lookup returns only rows whose normalized
    classification, specialization, or display name matches this term.  This
    preserves cache precedence without a process-global, TTL-dependent view.
    """

    variants = key_variants(term)
    if not variants:
        return None

    synonym_codes = await _load_synonym_codes(session, variants)
    if synonym_codes:
        return synonym_codes, synonym_codes

    nucc_mappings = await _load_matching_nucc_mappings(session, variants)
    if not nucc_mappings:
        return None
    return _resolve_nucc_entry(nucc_mappings, variants)


def _normalized_term_value(value: object) -> str:
    return " ".join(
        "".join(character if character.isalnum() and character.isascii() else " " for character in str(value or "").lower()).split()
    )
