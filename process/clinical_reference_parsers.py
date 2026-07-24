# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""ICD-10-CM and MeSH artifact parsers for clinical-reference imports."""

from __future__ import annotations

import gzip
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any, Iterable

from process.clinical_reference_rows import (
    NLM_ATTRIBUTION,
    _code_type_for_mesh,
    _concept_row,
    _crosswalk_row,
    _relationship_row,
    _synonym_row,
)


def _parse_icd10cm(
    path: Path,
    test_limit: int | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    concept_rows: list[dict[str, Any]] = []
    synonym_rows: list[dict[str, Any]] = []
    crosswalk_rows: list[dict[str, Any]] = []
    with zipfile.ZipFile(path) as archive:
        text_members = [name for name in archive.namelist() if name.lower().endswith(".txt")]
        target_member = next(
            (name for name in text_members if "order" not in name.lower()),
            text_members[0],
        )
        with archive.open(target_member) as raw_lines:
            for raw_line in raw_lines:
                normalized_line = raw_line.decode("utf-8", errors="replace").rstrip()
                if not normalized_line.strip():
                    continue
                code, _, description = normalized_line.partition(" ")
                code = code.strip().upper()
                description = re.sub(r"\s+", " ", description).strip()
                if not code or not description:
                    continue
                concept_rows.append(
                    _concept_row("ICD10CM", code, "condition", description, "cdc_icd10cm", "2026")
                )
                synonym_rows.append(
                    _synonym_row("ICD10CM", code, description, "preferred", "cdc_icd10cm")
                )
                compact_code = re.sub(r"[^A-Z0-9]", "", code)
                if compact_code != code:
                    crosswalk_rows.append(
                        _crosswalk_row(
                            "ICD10CM_COMPACT",
                            compact_code,
                            "ICD10CM",
                            code,
                            "code_format_equivalent",
                            "cdc_icd10cm",
                        )
                    )
                if test_limit and len(concept_rows) >= test_limit:
                    break
    return concept_rows, synonym_rows, crosswalk_rows


def _mesh_text(element: ET.Element, element_path: str) -> str:
    found_element = element.find(element_path)
    return (
        (found_element.text or "").strip()
        if found_element is not None and found_element.text
        else ""
    )


def _mesh_record_fields(
    element: ET.Element,
    record_tag: str,
) -> tuple[str, str, list[str], str, Iterable[ET.Element]]:
    term_path = "./ConceptList/Concept/TermList/Term/String"
    if record_tag == "DescriptorRecord":
        tree_numbers = [
            node.text.strip()
            for node in element.findall("./TreeNumberList/TreeNumber")
            if node.text
        ]
        return (
            _mesh_text(element, "DescriptorUI"),
            _mesh_text(element, "DescriptorName/String"),
            tree_numbers,
            _code_type_for_mesh(tree_numbers),
            element.findall(term_path),
        )
    if record_tag == "SupplementalRecord":
        return (
            _mesh_text(element, "SupplementalRecordUI"),
            _mesh_text(element, "SupplementalRecordName/String"),
            [],
            "concept",
            element.findall(term_path),
        )
    return (
        _mesh_text(element, "QualifierUI"),
        _mesh_text(element, "QualifierName/String"),
        [],
        "qualifier",
        element.findall(term_path),
    )


def _parse_mesh_record(
    element: ET.Element,
    record_tag: str,
    source_name: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
    code, display, tree_numbers, code_type, term_nodes = _mesh_record_fields(
        element,
        record_tag,
    )
    if not code or not display:
        return None, [], []
    concept_map = _concept_row(
        "MESH",
        code,
        code_type,
        display,
        source_name,
        "2026",
        attribution=NLM_ATTRIBUTION,
    )
    synonym_maps = [
        _synonym_row("MESH", code, display, "preferred", source_name, NLM_ATTRIBUTION)
    ]
    synonym_maps.extend(
        _synonym_row("MESH", code, term, "synonym", source_name, NLM_ATTRIBUTION)
        for term_node in term_nodes
        if (term := (term_node.text or "").strip()) and term != display
    )
    relationship_maps = [
        _relationship_row(
            "MESH",
            code,
            "has_tree_number",
            "MESH_TREE",
            tree_number,
            source_name,
        )
        for tree_number in tree_numbers
    ]
    return concept_map, synonym_maps, relationship_maps


def _parse_mesh_file(
    path: Path,
    source_name: str,
    test_limit: int | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    concept_rows: list[dict[str, Any]] = []
    synonym_rows: list[dict[str, Any]] = []
    relationship_rows: list[dict[str, Any]] = []
    open_artifact = gzip.open if path.suffix == ".gz" else open
    record_tags = {"DescriptorRecord", "SupplementalRecord", "QualifierRecord"}
    with open_artifact(path, "rb") as source_stream:
        for _, element in ET.iterparse(source_stream, events=("end",)):
            record_tag = element.tag.rsplit("}", 1)[-1]
            if record_tag not in record_tags:
                continue
            concept_map, synonym_maps, relationship_maps = _parse_mesh_record(
                element,
                record_tag,
                source_name,
            )
            if concept_map:
                concept_rows.append(concept_map)
                synonym_rows.extend(synonym_maps)
                relationship_rows.extend(relationship_maps)
            element.clear()
            if test_limit and len(concept_rows) >= test_limit:
                break
    return concept_rows, synonym_rows, relationship_rows
