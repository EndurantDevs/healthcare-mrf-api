# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from collections.abc import Callable

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
        target_system = str(synonym_mapping.get("target_system") or "").strip().upper()
        code = ""
        if target_system == "NUCC":
            code = str(synonym_mapping.get("target_code") or "").strip().upper()
        else:
            metadata_raw = synonym_mapping.get("metadata_json")
            if metadata_raw:
                try:
                    code = str((json.loads(metadata_raw) or {}).get("nucc_code") or "").strip().upper()
                except (TypeError, ValueError):
                    code = ""
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
    except (TypeError, ValueError):
        return ""


def _sorted_codes(codes: list[str]) -> tuple[str, ...]:
    return tuple(sorted({code for code in codes if code}))


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
    synonym_codes = _sorted_codes([
        _synonym_code(mapping)
        for mapping in synonym_result.mappings()
    ])
    if synonym_codes:
        return synonym_codes, synonym_codes

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
    rows = list(nucc_result.mappings())
    if not rows:
        return None

    variant_set = set(variants)
    classification_rows = [
        row for row in rows
        if _normalized_term_value(row.get("classification")) in variant_set
    ]
    if classification_rows:
        all_codes = _sorted_codes([
            str(row.get("code") or "").strip().upper()
            for row in classification_rows
        ])
        base_codes = _sorted_codes([
            str(row.get("code") or "").strip().upper()
            for row in classification_rows
            if not str(row.get("specialization") or "").strip()
        ])
        return all_codes, base_codes or all_codes

    specialization_rows = [
        row for row in rows
        if _normalized_term_value(row.get("specialization")) in variant_set
    ]
    matching_rows = specialization_rows or [
        row for row in rows
        if _normalized_term_value(row.get("display_name")) in variant_set
    ]
    codes = _sorted_codes([
        str(row.get("code") or "").strip().upper()
        for row in matching_rows
    ])
    return (codes, codes) if codes else None


def _normalized_term_value(value: object) -> str:
    return " ".join(
        "".join(character if character.isalnum() and character.isascii() else " " for character in str(value or "").lower()).split()
    )
