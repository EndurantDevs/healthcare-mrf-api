# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from collections.abc import Callable

from sqlalchemy import select

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


async def load_dynamic_specialty_entries(session, key_variants: SpecialtyKeyVariants) -> SpecialtyEntries:
    """Load DB-backed NUCC and synonym specialty aliases."""

    entries_by_variant = await _load_nucc_entries(session, key_variants)
    entries_by_variant.update(await _load_synonym_entries(session, key_variants))
    return entries_by_variant
