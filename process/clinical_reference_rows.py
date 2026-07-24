# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Normalized row and clinical-area builders for terminology sources."""

from __future__ import annotations

import datetime
import re
from typing import Any

NLM_ATTRIBUTION = (
    "This product uses publicly available data from the U.S. National Library of Medicine (NLM), "
    "National Institutes of Health, Department of Health and Human Services; NLM is not responsible "
    "for the product and does not endorse or recommend this or any other product."
)

SNOMED_FSN_TYPE_ID = "900000000000003001"
SNOMED_SYNONYM_TYPE_ID = "900000000000013009"


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _code_type_for_snomed(term: str) -> str:
    lower_term = term.lower()
    if "(disorder)" in lower_term or "(finding)" in lower_term:
        return "condition"
    if "(procedure)" in lower_term or "(regime/therapy)" in lower_term:
        return "treatment"
    if "(substance)" in lower_term or "(product)" in lower_term:
        return "substance"
    return "concept"


def _code_type_for_mesh(tree_numbers: list[str], descriptor_class: str | None = None) -> str:
    if any(tree_number.startswith("C") for tree_number in tree_numbers):
        return "condition"
    if any(tree_number.startswith("F03") for tree_number in tree_numbers):
        return "condition"
    if any(tree_number.startswith("E") for tree_number in tree_numbers):
        return "treatment"
    if any(tree_number.startswith("D") for tree_number in tree_numbers):
        return "substance"
    if descriptor_class == "SCR":
        return "concept"
    return "concept"


def _concept_row(
    system: str,
    code: str,
    code_type: str,
    display: str,
    source: str,
    release: str | None,
    *,
    attribution: str | None = None,
    long_description: str | None = None,
) -> dict[str, Any]:
    return {
        "code_system": system,
        "code": code,
        "code_type": code_type,
        "display_name": display,
        "short_description": display,
        "long_description": long_description,
        "is_active": True,
        "source": source,
        "source_release": release,
        "source_attribution": attribution,
        "updated_at": _now(),
    }


def _synonym_row(
    system: str,
    code: str,
    synonym: str,
    term_type: str,
    source: str,
    attribution: str | None = None,
) -> dict[str, Any]:
    return {
        "code_system": system,
        "code": code,
        "synonym": synonym,
        "term_type": term_type,
        "language": "ENG",
        "source": source,
        "source_attribution": attribution,
        "updated_at": _now(),
    }


def _crosswalk_row(
    from_system: str,
    from_code: str,
    to_system: str,
    to_code: str,
    match_type: str,
    source: str,
) -> dict[str, Any]:
    return {
        "from_system": from_system,
        "from_code": from_code,
        "to_system": to_system,
        "to_code": to_code,
        "match_type": match_type,
        "confidence": 1.0,
        "source": source,
        "source_attribution": NLM_ATTRIBUTION if source.startswith(("nlm_", "umls_")) else None,
        "updated_at": _now(),
    }


def _relationship_row(
    from_system: str,
    from_code: str,
    relationship: str,
    to_system: str,
    to_code: str,
    source: str,
) -> dict[str, Any]:
    return {
        "from_system": from_system,
        "from_code": from_code,
        "relationship": relationship,
        "to_system": to_system,
        "to_code": to_code,
        "source": source,
        "source_attribution": (
            NLM_ATTRIBUTION if source.startswith(("nlm_", "rxclass_", "umls_")) else None
        ),
        "updated_at": _now(),
    }


def _area_row(
    area_id: str,
    display: str,
    anchor_code: str,
    source: str = "nlm_mesh_tree",
) -> dict[str, Any]:
    return {
        "clinical_area_id": area_id,
        "display_name": display,
        "description": f"Clinical area rooted at MeSH tree number {anchor_code}.",
        "anchor_system": "MESH_TREE",
        "anchor_code": anchor_code,
        "source": source,
        "source_attribution": NLM_ATTRIBUTION,
        "updated_at": _now(),
    }


def _area_condition_row(area_id: str, system: str, code: str, source: str) -> dict[str, Any]:
    return {
        "clinical_area_id": area_id,
        "condition_system": system,
        "condition_code": code,
        "source": source,
        "updated_at": _now(),
    }


def _area_treatment_row(area_id: str, system: str, code: str, source: str) -> dict[str, Any]:
    return {
        "clinical_area_id": area_id,
        "treatment_system": system,
        "treatment_code": code,
        "source": source,
        "updated_at": _now(),
    }


def _mesh_clinical_area_root(tree_number: str) -> str | None:
    normalized_tree = str(tree_number or "").strip().upper()
    if re.match(r"^C[0-9]{2}(?:\.|$)", normalized_tree):
        return normalized_tree[:3]
    if re.match(r"^E[0-9]{2}(?:\.|$)", normalized_tree):
        return normalized_tree[:3]
    if normalized_tree == "F03" or normalized_tree.startswith("F03."):
        return "F03"
    return None


def _index_mesh_tree_memberships(
    relationships_by_identity: dict[tuple[str, str, str, str, str], dict[str, Any]],
) -> tuple[dict[str, set[str]], dict[str, str]]:
    tree_numbers_by_mesh_code: dict[str, set[str]] = {}
    mesh_code_by_tree_number: dict[str, str] = {}
    for relationship_map in relationships_by_identity.values():
        is_mesh_tree_relationship = (
            relationship_map["from_system"] == "MESH"
            and relationship_map["relationship"] == "has_tree_number"
            and relationship_map["to_system"] == "MESH_TREE"
        )
        if not is_mesh_tree_relationship:
            continue
        mesh_code = relationship_map["from_code"]
        tree_number = str(relationship_map["to_code"]).upper()
        tree_numbers_by_mesh_code.setdefault(mesh_code, set()).add(tree_number)
        mesh_code_by_tree_number.setdefault(tree_number, mesh_code)
    return tree_numbers_by_mesh_code, mesh_code_by_tree_number


def _create_area_rows_by_id(
    concepts_by_identity: dict[tuple[str, str], dict[str, Any]],
    tree_numbers_by_mesh_code: dict[str, set[str]],
    mesh_code_by_tree_number: dict[str, str],
) -> dict[str, dict[str, Any]]:
    area_roots = sorted(
        {
            root
            for tree_numbers in tree_numbers_by_mesh_code.values()
            for tree_number in tree_numbers
            if (root := _mesh_clinical_area_root(tree_number))
        }
    )
    area_rows_by_id: dict[str, dict[str, Any]] = {}
    for area_root in area_roots:
        root_code = mesh_code_by_tree_number.get(area_root)
        root_concept = concepts_by_identity.get(("MESH", root_code)) if root_code else None
        if root_concept:
            area_id = f"mesh:{area_root}"
            area_rows_by_id[area_id] = _area_row(
                area_id,
                root_concept["display_name"],
                area_root,
            )
    return area_rows_by_id


def _create_mesh_area_memberships(
    concepts_by_identity: dict[tuple[str, str], dict[str, Any]],
    tree_numbers_by_mesh_code: dict[str, set[str]],
    area_rows_by_id: dict[str, dict[str, Any]],
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]], dict[str, set[str]]]:
    condition_rows_by_identity: dict[tuple[str, str, str], dict[str, Any]] = {}
    treatment_rows_by_identity: dict[tuple[str, str, str], dict[str, Any]] = {}
    condition_area_ids_by_mesh_code: dict[str, set[str]] = {}
    for mesh_code, tree_numbers in tree_numbers_by_mesh_code.items():
        mesh_concept = concepts_by_identity.get(("MESH", mesh_code))
        if not mesh_concept:
            continue
        area_roots = {
            root
            for tree_number in tree_numbers
            if (root := _mesh_clinical_area_root(tree_number))
            and f"mesh:{root}" in area_rows_by_id
        }
        for area_root in area_roots:
            area_id = f"mesh:{area_root}"
            membership_key = (area_id, "MESH", mesh_code)
            is_condition_area = area_root.startswith("C") or area_root == "F03"
            if is_condition_area and mesh_concept.get("code_type") == "condition":
                condition_rows_by_identity[membership_key] = _area_condition_row(
                    area_id, "MESH", mesh_code, "nlm_mesh_tree"
                )
                condition_area_ids_by_mesh_code.setdefault(mesh_code, set()).add(area_id)
            elif area_root.startswith("E") and mesh_concept.get("code_type") == "treatment":
                treatment_rows_by_identity[membership_key] = _area_treatment_row(
                    area_id, "MESH", mesh_code, "nlm_mesh_tree"
                )
    return (
        condition_rows_by_identity,
        treatment_rows_by_identity,
        condition_area_ids_by_mesh_code,
    )


def _add_rxnorm_area_treatments(
    treatment_rows_by_identity: dict[tuple[str, str, str], dict[str, Any]],
    condition_area_ids_by_mesh_code: dict[str, set[str]],
    relationships_by_identity: dict[tuple[str, str, str, str, str], dict[str, Any]],
) -> None:
    for relationship_map in relationships_by_identity.values():
        is_treatment_relationship = (
            relationship_map["from_system"] == "RXNORM"
            and relationship_map["relationship"] == "may_treat"
            and relationship_map["to_system"] == "MESH"
        )
        if not is_treatment_relationship:
            continue
        area_ids = condition_area_ids_by_mesh_code.get(relationship_map["to_code"], set())
        for area_id in area_ids:
            treatment_key = (area_id, "RXNORM", relationship_map["from_code"])
            treatment_rows_by_identity[treatment_key] = _area_treatment_row(
                area_id,
                "RXNORM",
                relationship_map["from_code"],
                "rxclass_medrt_area",
            )


def _build_clinical_area_rows(
    concepts_by_identity: dict[tuple[str, str], dict[str, Any]],
    relationships_by_identity: dict[tuple[str, str, str, str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build clinical-area membership rows from normalized terminology relationships."""
    tree_numbers_by_mesh_code, mesh_code_by_tree_number = _index_mesh_tree_memberships(
        relationships_by_identity
    )
    area_rows_by_id = _create_area_rows_by_id(
        concepts_by_identity,
        tree_numbers_by_mesh_code,
        mesh_code_by_tree_number,
    )
    condition_rows_by_identity, treatment_rows_by_identity, area_ids_by_mesh_code = (
        _create_mesh_area_memberships(
            concepts_by_identity,
            tree_numbers_by_mesh_code,
            area_rows_by_id,
        )
    )
    _add_rxnorm_area_treatments(
        treatment_rows_by_identity,
        area_ids_by_mesh_code,
        relationships_by_identity,
    )
    return (
        list(area_rows_by_id.values()),
        list(condition_rows_by_identity.values()),
        list(treatment_rows_by_identity.values()),
    )
