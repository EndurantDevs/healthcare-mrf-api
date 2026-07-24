# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""RxNorm and SNOMED archive parsers for clinical-reference imports."""

from __future__ import annotations

import csv
import re
import zipfile
from pathlib import Path
from typing import Any

from process.clinical_reference_rows import (
    NLM_ATTRIBUTION,
    SNOMED_FSN_TYPE_ID,
    _code_type_for_snomed,
    _concept_row,
    _crosswalk_row,
    _relationship_row,
    _synonym_row,
)


def _read_rxnorm_concepts(
    archive: zipfile.ZipFile,
    test_limit: int | None,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    concepts_by_rxcui: dict[str, dict[str, Any]] = {}
    synonym_rows: list[dict[str, Any]] = []
    priority_by_term_type = {"IN": 1, "PIN": 2, "SCD": 3, "SBD": 4, "BN": 5, "MIN": 6}
    seen_synonym_keys: set[tuple[str, str, str]] = set()
    concept_member = next(name for name in archive.namelist() if name.endswith("RXNCONSO.RRF"))
    with archive.open(concept_member) as raw_lines:
        for raw_line in raw_lines:
            fields = raw_line.decode("utf-8", errors="replace").rstrip("\n").split("|")
            if len(fields) < 18:
                continue
            rxcui, language, source_name = fields[0], fields[1], fields[11]
            term_type, term, suppress = fields[12], fields[14], fields[16]
            is_eligible_atom = (
                language == "ENG"
                and source_name == "RXNORM"
                and suppress in {"N", ""}
                and bool(rxcui)
                and bool(term)
            )
            if not is_eligible_atom:
                continue
            current_concept = concepts_by_rxcui.get(rxcui)
            term_priority = priority_by_term_type.get(term_type, 99)
            if current_concept is None or term_priority < current_concept["_priority"]:
                concepts_by_rxcui[rxcui] = {
                    **_concept_row(
                        "RXNORM",
                        rxcui,
                        "drug",
                        term,
                        "nlm_rxnorm",
                        None,
                        attribution=NLM_ATTRIBUTION,
                    ),
                    "_priority": term_priority,
                }
            synonym_key = (rxcui, term, term_type or "atom")
            if synonym_key not in seen_synonym_keys:
                seen_synonym_keys.add(synonym_key)
                synonym_rows.append(
                    _synonym_row(
                        "RXNORM",
                        rxcui,
                        term,
                        term_type or "atom",
                        "nlm_rxnorm",
                        NLM_ATTRIBUTION,
                    )
                )
            if test_limit and len(concepts_by_rxcui) >= test_limit:
                break
    return concepts_by_rxcui, synonym_rows


def _read_rxnorm_relationships(
    archive: zipfile.ZipFile,
    known_rxcuis: set[str],
    test_limit: int | None,
) -> list[dict[str, Any]]:
    relationship_member = next(
        (name for name in archive.namelist() if name.endswith("RXNREL.RRF")),
        None,
    )
    if not relationship_member:
        return []
    relationship_rows: list[dict[str, Any]] = []
    with archive.open(relationship_member) as raw_lines:
        for raw_line in raw_lines:
            fields = raw_line.decode("utf-8", errors="replace").rstrip("\n").split("|")
            if len(fields) < 11:
                continue
            source_rxcui, relationship, target_rxcui, detailed_relationship, suppress = (
                fields[0],
                fields[3],
                fields[4],
                fields[7],
                fields[10],
            )
            is_eligible_relationship = (
                suppress in {"N", ""} and bool(source_rxcui) and bool(target_rxcui)
            )
            if not is_eligible_relationship:
                continue
            if source_rxcui in known_rxcuis and target_rxcui in known_rxcuis:
                relationship_rows.append(
                    _relationship_row(
                        "RXNORM",
                        source_rxcui,
                        detailed_relationship or relationship or "related",
                        "RXNORM",
                        target_rxcui,
                        "nlm_rxnorm",
                    )
                )
            if test_limit and len(relationship_rows) >= test_limit:
                break
    return relationship_rows


def _parse_rxnorm(
    path: Path,
    test_limit: int | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    with zipfile.ZipFile(path) as archive:
        concepts_by_rxcui, synonym_rows = _read_rxnorm_concepts(archive, test_limit)
        relationship_rows = _read_rxnorm_relationships(
            archive,
            set(concepts_by_rxcui),
            test_limit,
        )
    concept_rows = []
    for concept_map in concepts_by_rxcui.values():
        concept_map.pop("_priority", None)
        concept_rows.append(concept_map)
    return concept_rows, synonym_rows, relationship_rows


def _read_active_snomed_codes(
    archive: zipfile.ZipFile,
    test_limit: int | None,
) -> set[str]:
    concept_member = next(
        name
        for name in archive.namelist()
        if "/Snapshot/" in name
        and "sct2_Concept_Snapshot" in name
        and name.endswith(".txt")
    )
    active_codes: set[str] = set()
    with archive.open(concept_member) as raw_lines:
        reader = csv.DictReader(
            (line.decode("utf-8", errors="replace") for line in raw_lines),
            delimiter="\t",
        )
        for concept_map in reader:
            if concept_map.get("active") == "1":
                active_codes.add(concept_map["id"])
                if test_limit and len(active_codes) >= test_limit:
                    break
    return active_codes


def _read_snomed_descriptions(
    archive: zipfile.ZipFile,
    active_codes: set[str],
) -> tuple[dict[str, str], dict[str, str], list[dict[str, Any]]]:
    description_member = next(
        name
        for name in archive.namelist()
        if "/Snapshot/" in name
        and "sct2_Description_Snapshot" in name
        and name.endswith(".txt")
    )
    display_by_code: dict[str, str] = {}
    type_by_code: dict[str, str] = {}
    synonym_rows: list[dict[str, Any]] = []
    seen_synonym_keys: set[tuple[str, str, str]] = set()
    with archive.open(description_member) as raw_lines:
        reader = csv.DictReader(
            (line.decode("utf-8", errors="replace") for line in raw_lines),
            delimiter="\t",
        )
        for description_map in reader:
            concept_id = description_map.get("conceptId")
            if description_map.get("active") != "1" or concept_id not in active_codes:
                continue
            term = description_map.get("term") or ""
            type_id = description_map.get("typeId") or ""
            if type_id == SNOMED_FSN_TYPE_ID:
                display_by_code.setdefault(
                    concept_id,
                    re.sub(r"\s+\([^)]*\)$", "", term).strip() or term,
                )
                type_by_code[concept_id] = _code_type_for_snomed(term)
            synonym_key = (concept_id, term, type_id)
            if term and synonym_key not in seen_synonym_keys:
                seen_synonym_keys.add(synonym_key)
                synonym_rows.append(
                    _synonym_row(
                        "SNOMEDCT_US",
                        concept_id,
                        term,
                        "fsn" if type_id == SNOMED_FSN_TYPE_ID else "synonym",
                        "nlm_snomedct_us",
                        NLM_ATTRIBUTION,
                    )
                )
    return display_by_code, type_by_code, synonym_rows


def _read_snomed_relationships(
    archive: zipfile.ZipFile,
    active_codes: set[str],
    test_limit: int | None,
) -> list[dict[str, Any]]:
    relationship_member = next(
        (
            name
            for name in archive.namelist()
            if "/Snapshot/" in name
            and "sct2_Relationship_Snapshot" in name
            and name.endswith(".txt")
        ),
        None,
    )
    if not relationship_member:
        return []
    relationship_rows: list[dict[str, Any]] = []
    with archive.open(relationship_member) as raw_lines:
        reader = csv.DictReader(
            (line.decode("utf-8", errors="replace") for line in raw_lines),
            delimiter="\t",
        )
        for relationship_map in reader:
            is_active_known_pair = (
                relationship_map.get("active") == "1"
                and relationship_map.get("sourceId") in active_codes
                and relationship_map.get("destinationId") in active_codes
            )
            if is_active_known_pair:
                relationship_rows.append(
                    _relationship_row(
                        "SNOMEDCT_US",
                        relationship_map["sourceId"],
                        relationship_map.get("typeId") or "related",
                        "SNOMEDCT_US",
                        relationship_map["destinationId"],
                        "nlm_snomedct_us",
                    )
                )
            if test_limit and len(relationship_rows) >= test_limit:
                break
    return relationship_rows


def _parse_snomed(
    path: Path,
    test_limit: int | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    with zipfile.ZipFile(path) as archive:
        active_codes = _read_active_snomed_codes(archive, test_limit)
        display_by_code, type_by_code, synonym_rows = _read_snomed_descriptions(
            archive,
            active_codes,
        )
        relationship_rows = _read_snomed_relationships(
            archive,
            active_codes,
            test_limit,
        )
    concept_rows = [
        _concept_row(
            "SNOMEDCT_US",
            code,
            type_by_code.get(code, "concept"),
            display_by_code.get(code, code),
            "nlm_snomedct_us",
            None,
            attribution=NLM_ATTRIBUTION,
        )
        for code in active_codes
    ]
    return concept_rows, synonym_rows, relationship_rows


def _parse_snomed_icd_map(
    path: Path,
    test_limit: int | None = None,
) -> list[dict[str, Any]]:
    crosswalk_rows: list[dict[str, Any]] = []
    with zipfile.ZipFile(path) as archive:
        map_members = [
            name
            for name in archive.namelist()
            if name.lower().endswith((".txt", ".tsv"))
            and ("map" in name.lower() or "extendedmap" in name.lower())
        ]
        if not map_members:
            return crosswalk_rows
        with archive.open(map_members[0]) as raw_lines:
            reader = csv.DictReader(
                (line.decode("utf-8", errors="replace") for line in raw_lines),
                delimiter="\t",
            )
            for map_entry in reader:
                active = map_entry.get("active", "1")
                snomed_code = (
                    map_entry.get("referencedComponentId")
                    or map_entry.get("referencedcomponentid")
                )
                icd_code = map_entry.get("mapTarget") or map_entry.get("maptarget")
                is_valid_map = (
                    active == "1"
                    and snomed_code
                    and icd_code
                    and re.match(r"^[A-Z][0-9A-Z]", icd_code)
                )
                if is_valid_map:
                    crosswalk_rows.append(
                        _crosswalk_row(
                            "SNOMEDCT_US",
                            snomed_code,
                            "ICD10CM",
                            icd_code,
                            "official_map",
                            "nlm_snomedct_icd10cm_map",
                        )
                    )
                if test_limit and len(crosswalk_rows) >= test_limit:
                    break
    return crosswalk_rows
