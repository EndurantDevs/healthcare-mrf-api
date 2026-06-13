# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import csv
import asyncio
import datetime
import gzip
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any, Iterable

import asyncpg
import redis

from db.connection import init_db
from db.models import (
    ClinicalArea,
    ClinicalAreaCondition,
    ClinicalAreaTreatment,
    CodeCatalog,
    CodeCrosswalk,
    CodeRelationship,
    CodeSynonym,
    db,
)
from process.control_cancel import ImportCancelledError
from process.ext.utils import ensure_database, make_class, push_objects
from process.redis_config import build_redis_settings

NLM_ATTRIBUTION = (
    "This product uses publicly available data from the U.S. National Library of Medicine (NLM), "
    "National Institutes of Health, Department of Health and Human Services; NLM is not responsible "
    "for the product and does not endorse or recommend this or any other product."
)

CDC_ICD10CM_URL = (
    "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2026/"
    "icd10cm-Code%20Descriptions-2026.zip"
)
MESH_DESC_URL = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc2026.gz"
MESH_SUPP_URL = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/supp2026.gz"
UMLS_RELEASES_URL = "https://uts-ws.nlm.nih.gov/releases"
UMLS_DOWNLOAD_URL = "https://uts-ws.nlm.nih.gov/download"
RXCLASS_BY_RXCUI_URL = "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json"

DEFAULT_ARTIFACT_ROOT = Path("/Volumes/Data/data/artifacts/terminology")
DEFAULT_BATCH_SIZE = 5000
DEFAULT_CLINICAL_REFERENCE_SOURCES = "icd10cm,mesh,rxnorm,medrt"
CLINICAL_REFERENCE_SOURCES = (
    "cdc_icd10cm",
    "nlm_mesh_descriptor",
    "nlm_mesh_supplemental",
    "nlm_rxnorm",
    "nlm_snomedct_us",
    "nlm_snomedct_icd10cm_map",
    "rxclass_medrt",
)

SNOMED_FSN_TYPE_ID = "900000000000003001"
SNOMED_SYNONYM_TYPE_ID = "900000000000013009"
RESTRICTED_SOURCE_ALIASES = {"snomed"}


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _artifact_root(override: str | None = None) -> Path:
    configured = override or os.getenv("HLTHPRT_TERMINOLOGY_ARTIFACT_ROOT")
    return Path(configured).expanduser() if configured else DEFAULT_ARTIFACT_ROOT


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _normalize_import_id(raw: str | None) -> str:
    raw = raw or os.getenv("HLTHPRT_CLINICAL_REFERENCE_IMPORT_ID")
    if raw:
        cleaned = "".join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return _now().strftime("%Y%m%d")


def _stage_index_name(stage_table: str, index_name: str) -> str:
    return f"{stage_table}_idx_{index_name}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cancel_requested(run_id: str | None) -> bool:
    if not run_id:
        return False
    try:
        settings = build_redis_settings()
        dsn = os.getenv("HLTHPRT_REDIS_ADDRESS")
        if dsn:
            client = redis.Redis.from_url(dsn, socket_connect_timeout=2, socket_timeout=2)
        else:
            client = redis.Redis(
                host=settings.host,
                port=settings.port,
                password=settings.password,
                db=settings.database,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        value = client.get(f"cancel:{run_id}")
        return value in {b"1", "1", 1, True}
    except Exception:
        return False


def _raise_if_cancelled(run_id: str | None) -> None:
    if _cancel_requested(run_id):
        raise ImportCancelledError(f"import run {run_id} was cancelled")


def _download_url(url: str, path: Path, *, api_key: str | None = None, force: bool = False, run_id: str | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0 and not force:
        return path
    _raise_if_cancelled(run_id)
    tmp = path.with_suffix(path.suffix + ".tmp")
    request_url = url
    if api_key:
        request_url = _umls_download_url(url, api_key)
    request = urllib.request.Request(request_url, headers={"User-Agent": "HealthPorta terminology importer"})
    try:
        with urllib.request.urlopen(request, timeout=3600) as response, tmp.open("wb") as out:
            while True:
                _raise_if_cancelled(run_id)
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
    except Exception as exc:
        raise RuntimeError(f"download failed for {_redact_sensitive_url(request_url)}: {_redact_sensitive_url(str(exc))}") from exc
    _raise_if_cancelled(run_id)
    tmp.replace(path)
    manifest = {
        "source_url": url,
        "downloaded_at": _now().isoformat() + "Z",
        "byte_count": path.stat().st_size,
        "sha256": _sha256_file(path),
    }
    path.with_suffix(path.suffix + ".manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


def _umls_download_url(url: str, api_key: str) -> str:
    return f"{UMLS_DOWNLOAD_URL}?url={urllib.parse.quote(url, safe='')}&apiKey={urllib.parse.quote(api_key)}"


def _redact_sensitive_url(value: str) -> str:
    return re.sub(r"(?i)(apiKey=)[^\s&]+", r"\1<redacted>", str(value))


def _release_current(release_type: str) -> dict[str, Any]:
    with urllib.request.urlopen(f"{UMLS_RELEASES_URL}?releaseType={release_type}", timeout=60) as response:
        releases = json.loads(response.read().decode("utf-8"))
    for release in releases:
        if release.get("current"):
            return release
    if releases:
        return releases[0]
    raise RuntimeError(f"No UMLS release found for {release_type}")


def _batch(rows: list[dict[str, Any]], size: int = DEFAULT_BATCH_SIZE) -> Iterable[list[dict[str, Any]]]:
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def _code_type_for_snomed(term: str) -> str:
    lower = term.lower()
    if "(disorder)" in lower or "(finding)" in lower:
        return "condition"
    if "(procedure)" in lower or "(regime/therapy)" in lower:
        return "treatment"
    if "(substance)" in lower or "(product)" in lower:
        return "substance"
    return "concept"


def _code_type_for_mesh(tree_numbers: list[str], descriptor_class: str | None = None) -> str:
    if any(tree.startswith("C") for tree in tree_numbers):
        return "condition"
    if any(tree.startswith("F03") for tree in tree_numbers):
        return "condition"
    if any(tree.startswith("E") for tree in tree_numbers):
        return "treatment"
    if any(tree.startswith("D") for tree in tree_numbers):
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


def _synonym_row(system: str, code: str, synonym: str, term_type: str, source: str, attribution: str | None = None):
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


def _crosswalk_row(from_system: str, from_code: str, to_system: str, to_code: str, match_type: str, source: str):
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


def _relationship_row(from_system: str, from_code: str, relationship: str, to_system: str, to_code: str, source: str):
    return {
        "from_system": from_system,
        "from_code": from_code,
        "relationship": relationship,
        "to_system": to_system,
        "to_code": to_code,
        "source": source,
        "source_attribution": NLM_ATTRIBUTION if source.startswith(("nlm_", "rxclass_", "umls_")) else None,
        "updated_at": _now(),
    }


def _area_row(area_id: str, display: str, anchor_code: str, source: str = "nlm_mesh_tree") -> dict[str, Any]:
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
    tree = str(tree_number or "").strip().upper()
    if re.match(r"^C[0-9]{2}(?:\.|$)", tree):
        return tree[:3]
    if re.match(r"^E[0-9]{2}(?:\.|$)", tree):
        return tree[:3]
    if tree == "F03" or tree.startswith("F03."):
        return "F03"
    return None


def _build_clinical_area_rows(
    concepts: dict[tuple[str, str], dict[str, Any]],
    relationships: dict[tuple[str, str, str, str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    mesh_tree_by_code: dict[str, set[str]] = {}
    mesh_code_by_tree: dict[str, str] = {}
    for row in relationships.values():
        if row["from_system"] != "MESH" or row["relationship"] != "has_tree_number" or row["to_system"] != "MESH_TREE":
            continue
        code = row["from_code"]
        tree_number = str(row["to_code"]).upper()
        mesh_tree_by_code.setdefault(code, set()).add(tree_number)
        mesh_code_by_tree.setdefault(tree_number, code)

    area_roots = sorted(
        {
            root
            for tree_numbers in mesh_tree_by_code.values()
            for tree_number in tree_numbers
            if (root := _mesh_clinical_area_root(tree_number))
        }
    )
    area_rows_by_id: dict[str, dict[str, Any]] = {}
    for root in area_roots:
        root_code = mesh_code_by_tree.get(root)
        root_concept = concepts.get(("MESH", root_code)) if root_code else None
        if not root_concept:
            continue
        area_id = f"mesh:{root}"
        area_rows_by_id[area_id] = _area_row(area_id, root_concept["display_name"], root)

    condition_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    treatment_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    mesh_condition_areas: dict[str, set[str]] = {}

    for mesh_code, tree_numbers in mesh_tree_by_code.items():
        concept = concepts.get(("MESH", mesh_code))
        if not concept:
            continue
        roots = {
            root
            for tree_number in tree_numbers
            if (root := _mesh_clinical_area_root(tree_number)) and f"mesh:{root}" in area_rows_by_id
        }
        if not roots:
            continue
        code_type = concept.get("code_type")
        for root in roots:
            area_id = f"mesh:{root}"
            if root.startswith("C") or root == "F03":
                if code_type == "condition":
                    key = (area_id, "MESH", mesh_code)
                    condition_rows[key] = _area_condition_row(area_id, "MESH", mesh_code, "nlm_mesh_tree")
                    mesh_condition_areas.setdefault(mesh_code, set()).add(area_id)
            elif root.startswith("E") and code_type == "treatment":
                key = (area_id, "MESH", mesh_code)
                treatment_rows[key] = _area_treatment_row(area_id, "MESH", mesh_code, "nlm_mesh_tree")

    for row in relationships.values():
        if row["from_system"] != "RXNORM" or row["relationship"] != "may_treat" or row["to_system"] != "MESH":
            continue
        for area_id in mesh_condition_areas.get(row["to_code"], set()):
            key = (area_id, "RXNORM", row["from_code"])
            treatment_rows[key] = _area_treatment_row(area_id, "RXNORM", row["from_code"], "rxclass_medrt_area")

    return list(area_rows_by_id.values()), list(condition_rows.values()), list(treatment_rows.values())


def _parse_icd10cm(path: Path, test_limit: int | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    rows: list[dict[str, Any]] = []
    synonyms: list[dict[str, Any]] = []
    crosswalks: list[dict[str, Any]] = []
    with zipfile.ZipFile(path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".txt")]
        target = next((name for name in members if "order" not in name.lower()), members[0])
        with zf.open(target) as raw:
            for raw_line in raw:
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                if not line.strip():
                    continue
                code, _, description = line.partition(" ")
                code = code.strip().upper()
                description = re.sub(r"\s+", " ", description).strip()
                if not code or not description:
                    continue
                rows.append(_concept_row("ICD10CM", code, "condition", description, "cdc_icd10cm", "2026"))
                synonyms.append(_synonym_row("ICD10CM", code, description, "preferred", "cdc_icd10cm"))
                compact = re.sub(r"[^A-Z0-9]", "", code)
                if compact != code:
                    crosswalks.append(_crosswalk_row("ICD10CM_COMPACT", compact, "ICD10CM", code, "code_format_equivalent", "cdc_icd10cm"))
                if test_limit and len(rows) >= test_limit:
                    break
    return rows, synonyms, crosswalks


def _mesh_text(element: ET.Element, path: str) -> str:
    found = element.find(path)
    return (found.text or "").strip() if found is not None and found.text else ""


def _parse_mesh_file(path: Path, source: str, test_limit: int | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    rows: list[dict[str, Any]] = []
    synonyms: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    open_fn = gzip.open if path.suffix == ".gz" else open
    record_tags = {"DescriptorRecord", "SupplementalRecord", "QualifierRecord"}
    with open_fn(path, "rb") as fh:
        for _, elem in ET.iterparse(fh, events=("end",)):
            tag = elem.tag.rsplit("}", 1)[-1]
            if tag not in record_tags:
                continue
            if tag == "DescriptorRecord":
                code = _mesh_text(elem, "DescriptorUI")
                display = _mesh_text(elem, "DescriptorName/String")
                tree_numbers = [node.text.strip() for node in elem.findall("./TreeNumberList/TreeNumber") if node.text]
                code_type = _code_type_for_mesh(tree_numbers)
                term_nodes = elem.findall("./ConceptList/Concept/TermList/Term/String")
            elif tag == "SupplementalRecord":
                code = _mesh_text(elem, "SupplementalRecordUI")
                display = _mesh_text(elem, "SupplementalRecordName/String")
                tree_numbers = []
                code_type = "concept"
                term_nodes = elem.findall("./ConceptList/Concept/TermList/Term/String")
            else:
                code = _mesh_text(elem, "QualifierUI")
                display = _mesh_text(elem, "QualifierName/String")
                tree_numbers = []
                code_type = "qualifier"
                term_nodes = elem.findall("./ConceptList/Concept/TermList/Term/String")
            if code and display:
                rows.append(_concept_row("MESH", code, code_type, display, source, "2026", attribution=NLM_ATTRIBUTION))
                synonyms.append(_synonym_row("MESH", code, display, "preferred", source, NLM_ATTRIBUTION))
                for term_node in term_nodes:
                    term = (term_node.text or "").strip()
                    if term and term != display:
                        synonyms.append(_synonym_row("MESH", code, term, "synonym", source, NLM_ATTRIBUTION))
                for tree_number in tree_numbers:
                    relationships.append(_relationship_row("MESH", code, "has_tree_number", "MESH_TREE", tree_number, source))
            elem.clear()
            if test_limit and len(rows) >= test_limit:
                break
    return rows, synonyms, relationships


def _parse_rxnorm(path: Path, test_limit: int | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    rows_by_rxcui: dict[str, dict[str, Any]] = {}
    synonyms: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    tty_priority = {"IN": 1, "PIN": 2, "SCD": 3, "SBD": 4, "BN": 5, "MIN": 6}
    seen_synonyms: set[tuple[str, str, str]] = set()
    with zipfile.ZipFile(path) as zf:
        conso_name = next(name for name in zf.namelist() if name.endswith("RXNCONSO.RRF"))
        with zf.open(conso_name) as raw:
            for raw_line in raw:
                parts = raw_line.decode("utf-8", errors="replace").rstrip("\n").split("|")
                if len(parts) < 17:
                    continue
                rxcui, lat, _, _, _, _, _, _, _, _, _, sab, tty, _, term, _, suppress, _ = parts[:18]
                if lat != "ENG" or sab != "RXNORM" or suppress not in {"N", ""} or not rxcui or not term:
                    continue
                current = rows_by_rxcui.get(rxcui)
                if current is None or tty_priority.get(tty, 99) < current["_priority"]:
                    rows_by_rxcui[rxcui] = {
                        **_concept_row("RXNORM", rxcui, "drug", term, "nlm_rxnorm", None, attribution=NLM_ATTRIBUTION),
                        "_priority": tty_priority.get(tty, 99),
                    }
                key = (rxcui, term, tty or "atom")
                if key not in seen_synonyms:
                    seen_synonyms.add(key)
                    synonyms.append(_synonym_row("RXNORM", rxcui, term, tty or "atom", "nlm_rxnorm", NLM_ATTRIBUTION))
                if test_limit and len(rows_by_rxcui) >= test_limit:
                    break
        rel_name = next((name for name in zf.namelist() if name.endswith("RXNREL.RRF")), None)
        if rel_name:
            with zf.open(rel_name) as raw:
                for raw_line in raw:
                    parts = raw_line.decode("utf-8", errors="replace").rstrip("\n").split("|")
                    if len(parts) < 11:
                        continue
                    rxcui1, _, _, rel, rxcui2, _, _, rela, _, _, suppress = parts[:11]
                    if suppress not in {"N", ""} or not rxcui1 or not rxcui2:
                        continue
                    if rxcui1 in rows_by_rxcui and rxcui2 in rows_by_rxcui:
                        relationships.append(_relationship_row("RXNORM", rxcui1, rela or rel or "related", "RXNORM", rxcui2, "nlm_rxnorm"))
                    if test_limit and len(relationships) >= test_limit:
                        break
    rows = []
    for row in rows_by_rxcui.values():
        row.pop("_priority", None)
        rows.append(row)
    return rows, synonyms, relationships


def _parse_snomed(path: Path, test_limit: int | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    active_concepts: set[str] = set()
    display_by_code: dict[str, str] = {}
    type_by_code: dict[str, str] = {}
    synonyms: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    seen_synonyms: set[tuple[str, str, str]] = set()
    with zipfile.ZipFile(path) as zf:
        concept_name = next(name for name in zf.namelist() if "/Snapshot/" in name and "sct2_Concept_Snapshot" in name and name.endswith(".txt"))
        with zf.open(concept_name) as raw:
            reader = csv.DictReader((line.decode("utf-8", errors="replace") for line in raw), delimiter="\t")
            for row in reader:
                if row.get("active") == "1":
                    active_concepts.add(row["id"])
                    if test_limit and len(active_concepts) >= test_limit:
                        break
        description_name = next(name for name in zf.namelist() if "/Snapshot/" in name and "sct2_Description_Snapshot" in name and name.endswith(".txt"))
        with zf.open(description_name) as raw:
            reader = csv.DictReader((line.decode("utf-8", errors="replace") for line in raw), delimiter="\t")
            for row in reader:
                concept_id = row.get("conceptId")
                if row.get("active") != "1" or concept_id not in active_concepts:
                    continue
                term = row.get("term") or ""
                type_id = row.get("typeId") or ""
                if type_id == SNOMED_FSN_TYPE_ID:
                    display_by_code.setdefault(concept_id, re.sub(r"\s+\([^)]*\)$", "", term).strip() or term)
                    type_by_code[concept_id] = _code_type_for_snomed(term)
                key = (concept_id, term, type_id)
                if term and key not in seen_synonyms:
                    seen_synonyms.add(key)
                    synonyms.append(_synonym_row("SNOMEDCT_US", concept_id, term, "fsn" if type_id == SNOMED_FSN_TYPE_ID else "synonym", "nlm_snomedct_us", NLM_ATTRIBUTION))
        relationship_name = next((name for name in zf.namelist() if "/Snapshot/" in name and "sct2_Relationship_Snapshot" in name and name.endswith(".txt")), None)
        if relationship_name:
            with zf.open(relationship_name) as raw:
                reader = csv.DictReader((line.decode("utf-8", errors="replace") for line in raw), delimiter="\t")
                for row in reader:
                    if row.get("active") == "1" and row.get("sourceId") in active_concepts and row.get("destinationId") in active_concepts:
                        relationships.append(_relationship_row("SNOMEDCT_US", row["sourceId"], row.get("typeId") or "related", "SNOMEDCT_US", row["destinationId"], "nlm_snomedct_us"))
                    if test_limit and len(relationships) >= test_limit:
                        break
    rows = [
        _concept_row("SNOMEDCT_US", code, type_by_code.get(code, "concept"), display_by_code.get(code, code), "nlm_snomedct_us", None, attribution=NLM_ATTRIBUTION)
        for code in active_concepts
    ]
    return rows, synonyms, relationships


def _parse_snomed_icd_map(path: Path, test_limit: int | None = None) -> list[dict[str, Any]]:
    crosswalks: list[dict[str, Any]] = []
    with zipfile.ZipFile(path) as zf:
        candidates = [
            name for name in zf.namelist()
            if name.lower().endswith((".txt", ".tsv")) and ("map" in name.lower() or "extendedmap" in name.lower())
        ]
        if not candidates:
            return crosswalks
        target = candidates[0]
        with zf.open(target) as raw:
            reader = csv.DictReader((line.decode("utf-8", errors="replace") for line in raw), delimiter="\t")
            for row in reader:
                active = row.get("active", "1")
                snomed_code = row.get("referencedComponentId") or row.get("referencedcomponentid")
                icd_code = row.get("mapTarget") or row.get("maptarget")
                if active == "1" and snomed_code and icd_code and re.match(r"^[A-Z][0-9A-Z]", icd_code):
                    crosswalks.append(_crosswalk_row("SNOMEDCT_US", snomed_code, "ICD10CM", icd_code, "official_map", "nlm_snomedct_icd10cm_map"))
                if test_limit and len(crosswalks) >= test_limit:
                    break
    return crosswalks


async def _load_product_rxcuis(test_limit: int | None = None) -> list[str]:
    rx_schema = os.getenv("HLTHPRT_RX_DB_SCHEMA") or os.getenv("HLTHPRT_DRUG_DB_SCHEMA") or "rx_data"
    rx_database = (
        os.getenv("HLTHPRT_RX_DB_DATABASE")
        or os.getenv("HLTHPRT_DRUG_DB_DATABASE")
        or os.getenv("HLTHPRT_DB_DATABASE")
        or "postgres"
    )
    if rx_database:
        connection = None
        try:
            connection = await asyncpg.connect(
                host=os.getenv("HLTHPRT_RX_DB_HOST") or os.getenv("HLTHPRT_DRUG_DB_HOST") or os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1",
                port=int(os.getenv("HLTHPRT_RX_DB_PORT") or os.getenv("HLTHPRT_DRUG_DB_PORT") or os.getenv("HLTHPRT_DB_PORT") or "5432"),
                user=os.getenv("HLTHPRT_RX_DB_USER") or os.getenv("HLTHPRT_DRUG_DB_USER") or os.getenv("HLTHPRT_DB_USER") or "postgres",
                password=os.getenv("HLTHPRT_RX_DB_PASSWORD") or os.getenv("HLTHPRT_DRUG_DB_PASSWORD") or os.getenv("HLTHPRT_DB_PASSWORD") or "",
                database=rx_database,
            )
            rows = await connection.fetch(
                f"""
                SELECT DISTINCT unnest(rxnorm_ids) AS rxcui
                  FROM {rx_schema}.product
                 WHERE rxnorm_ids IS NOT NULL
                """
            )
            values = [str(row["rxcui"]) for row in rows if row["rxcui"]]
            return values[:test_limit] if test_limit else values
        except Exception as exc:
            print(f"MED-RT source skipped: {rx_schema}.product unavailable in {rx_database} ({exc})")
            return []
        finally:
            if connection is not None:
                await connection.close()
    try:
        rows = await db.all(
            """
            SELECT DISTINCT unnest(rxnorm_ids) AS rxcui
              FROM rx_data.product
             WHERE rxnorm_ids IS NOT NULL
            """
        )
    except Exception as exc:
        print(f"MED-RT source skipped: rx_data.product unavailable ({exc})")
        return []
    values = [str(row[0]) for row in rows if row[0]]
    return values[:test_limit] if test_limit else values


def _rxclass_for_rxcui(rxcui: str) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"rxcui": rxcui, "relaSource": "MEDRT", "relas": "may_treat"})
    last_exc: Exception | None = None
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(f"{RXCLASS_BY_RXCUI_URL}?{params}", timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return payload.get("rxclassDrugInfoList", {}).get("rxclassDrugInfo", []) or []
        except Exception as exc:
            last_exc = exc
            if attempt < 3:
                time.sleep(0.5 * attempt)
    if last_exc:
        raise last_exc
    return []


async def _load_medrt_from_rxclass(test_mode: bool) -> tuple[list[dict], list[dict], list[dict]]:
    default_limit = "40" if test_mode else "0"
    limit = int(os.getenv("HLTHPRT_MEDRT_RXCUI_LIMIT") or os.getenv("HLTHPRT_MEDRT_TEST_RXCUI_LIMIT") or default_limit)
    concurrency = max(1, int(os.getenv("HLTHPRT_MEDRT_RXCLASS_CONCURRENCY", "24")))
    rxcuis = await _load_product_rxcuis(limit if limit > 0 else None)
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    synonyms: dict[tuple[str, str, str], dict[str, Any]] = {}
    relationships: list[dict[str, Any]] = []
    semaphore = asyncio.Semaphore(concurrency)
    completed = 0

    async def lookup(rxcui: str) -> tuple[str, list[dict[str, Any]]]:
        async with semaphore:
            try:
                return rxcui, await asyncio.to_thread(_rxclass_for_rxcui, rxcui)
            except Exception as exc:
                print(f"RxClass MEDRT lookup failed for RXCUI {rxcui}: {exc}")
                return rxcui, []

    print(f"RxClass MEDRT lookup start: rxcuis={len(rxcuis)} concurrency={concurrency}")
    tasks = [asyncio.create_task(lookup(rxcui)) for rxcui in rxcuis]
    for task in asyncio.as_completed(tasks):
        rxcui, infos = await task
        completed += 1
        for info in infos:
            item = info.get("rxclassMinConceptItem") or {}
            class_id = item.get("classId")
            class_name = item.get("className")
            class_type = item.get("classType") or "CLASS"
            if not class_id or not class_name:
                continue
            system = "MESH" if class_id.startswith("D") else "MEDRT"
            code_type = "condition" if class_type == "DISEASE" else "concept"
            rows[(system, class_id)] = _concept_row(system, class_id, code_type, class_name, "rxclass_medrt", None, attribution=NLM_ATTRIBUTION)
            synonyms[(system, class_id, class_name)] = _synonym_row(system, class_id, class_name, "preferred", "rxclass_medrt", NLM_ATTRIBUTION)
            relationships.append(_relationship_row("RXNORM", rxcui, info.get("rela") or "may_treat", system, class_id, "rxclass_medrt"))
        if completed % 250 == 0 or completed == len(rxcuis):
            print(f"RxClass MEDRT progress: {completed}/{len(rxcuis)} RXCUIs")
    return list(rows.values()), list(synonyms.values()), relationships


async def _ensure_schema_exists(schema: str) -> None:
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")


async def _ensure_unified_code_tables(schema: str) -> None:
    await db.create_table(CodeCatalog.__table__, checkfirst=True)
    await db.create_table(CodeCrosswalk.__table__, checkfirst=True)
    await db.create_table(CodeSynonym.__table__, checkfirst=True)
    await db.create_table(CodeRelationship.__table__, checkfirst=True)
    await _create_stage_indexes(CodeCatalog, schema)
    await _create_stage_indexes(CodeCrosswalk, schema)
    await _create_stage_indexes(CodeSynonym, schema)
    await _create_stage_indexes(CodeRelationship, schema)


def _source_sql_list() -> str:
    return ", ".join(f"'{source}'" for source in CLINICAL_REFERENCE_SOURCES)


async def _create_stage_indexes(stage_cls, schema: str) -> None:
    if getattr(stage_cls, "__my_index_elements__", None):
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {stage_cls.__tablename__}_idx_primary "
            f"ON {schema}.{stage_cls.__tablename__} "
            f"({', '.join(stage_cls.__my_index_elements__)});"
        )
    for index in getattr(stage_cls, "__my_additional_indexes__", []) or []:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        using = f"USING {index.get('using')} " if index.get("using") else ""
        where = f" WHERE {index.get('where')}" if index.get("where") else ""
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"ON {schema}.{stage_cls.__tablename__} {using}"
            f"({', '.join(index.get('index_elements'))}){where};"
        )


async def _publish_table(model_cls, stage_cls, schema: str) -> None:
    table = model_cls.__main_table__
    await db.status(f"DROP TABLE IF EXISTS {schema}.{table};")
    await db.status(f"ALTER TABLE IF EXISTS {schema}.{stage_cls.__tablename__} RENAME TO {table};")
    await db.status(f"ALTER INDEX IF EXISTS {schema}.{stage_cls.__tablename__}_idx_primary RENAME TO {table}_idx_primary;")
    for index in getattr(stage_cls, "__my_additional_indexes__", []) or []:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        old_live_name = f"{table}_idx_{index_name}"
        await db.status(
            f"ALTER INDEX IF EXISTS {schema}.{_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"RENAME TO {old_live_name};"
        )


async def _merge_code_catalog_stage(stage_cls, schema: str) -> None:
    sources = _source_sql_list()
    await db.status(f"DELETE FROM {schema}.{CodeCatalog.__tablename__} WHERE source IN ({sources});")
    await db.status(
        f"""
        INSERT INTO {schema}.{CodeCatalog.__tablename__}
            (code_system, code, code_type, display_name, short_description, long_description,
             is_active, source, source_release, source_attribution, updated_at)
        SELECT code_system,
               code,
               code_type,
               display_name,
               short_description,
               long_description,
               is_active,
               source,
               source_release,
               source_attribution,
               updated_at
          FROM {schema}.{stage_cls.__tablename__}
        ON CONFLICT (code_system, code) DO UPDATE SET
            code_type = EXCLUDED.code_type,
            display_name = EXCLUDED.display_name,
            short_description = EXCLUDED.short_description,
            long_description = EXCLUDED.long_description,
            is_active = EXCLUDED.is_active,
            source = EXCLUDED.source,
            source_release = EXCLUDED.source_release,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """
    )


async def _merge_code_crosswalk_stage(stage_cls, schema: str) -> None:
    sources = _source_sql_list()
    await db.status(f"DELETE FROM {schema}.{CodeCrosswalk.__tablename__} WHERE source IN ({sources});")
    await db.status(
        f"""
        INSERT INTO {schema}.{CodeCrosswalk.__tablename__}
            (from_system, from_code, to_system, to_code, match_type, confidence, source, source_attribution, updated_at)
        SELECT from_system,
               from_code,
               to_system,
               to_code,
               match_type,
               confidence,
               source,
               source_attribution,
               updated_at
          FROM {schema}.{stage_cls.__tablename__}
        ON CONFLICT (from_system, from_code, to_system, to_code) DO UPDATE SET
            match_type = EXCLUDED.match_type,
            confidence = EXCLUDED.confidence,
            source = EXCLUDED.source,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """
    )


async def _merge_replace_source_table(model_cls, stage_cls, schema: str) -> None:
    sources = _source_sql_list()
    columns = [column.name for column in model_cls.__table__.columns]
    column_list = ", ".join(columns)
    await db.status(f"DELETE FROM {schema}.{model_cls.__tablename__} WHERE source IN ({sources});")
    await db.status(
        f"""
        INSERT INTO {schema}.{model_cls.__tablename__} ({column_list})
        SELECT {column_list}
          FROM {schema}.{stage_cls.__tablename__}
        ON CONFLICT DO NOTHING;
        """
    )


async def _push(stage_cls, rows: list[dict[str, Any]]) -> int:
    count = 0
    for chunk in _batch(rows):
        await push_objects(chunk, stage_cls)
        count += len(chunk)
    return count


def _selected_sources(raw: str | None) -> set[str]:
    explicit = raw if raw is not None else os.getenv("HLTHPRT_CLINICAL_REFERENCE_SOURCES")
    value = explicit or DEFAULT_CLINICAL_REFERENCE_SOURCES
    selected = {item.strip().lower() for item in value.split(",") if item.strip()}
    restricted = selected & RESTRICTED_SOURCE_ALIASES
    if restricted and not _restricted_terminology_enabled():
        joined = ", ".join(sorted(restricted))
        raise RuntimeError(
            f"restricted terminology source(s) require HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES=1: {joined}"
        )
    return selected


def _restricted_terminology_enabled() -> bool:
    return str(os.getenv("HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES") or "").strip().lower() in {"1", "true", "yes"}


async def import_clinical_reference(
    test_mode: bool = False,
    import_id: str | None = None,
    sources: str | None = None,
    artifact_root: str | None = None,
    force_download: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    await ensure_database(test_mode)
    schema = _schema()
    import_suffix = _normalize_import_id(import_id)
    root = _artifact_root(artifact_root)
    selected = _selected_sources(sources)
    umls_key = os.getenv("HLTHPRT_UMLS_API_KEY") or os.getenv("UMLS_API_KEY")
    source_test_limit = int(os.getenv("HLTHPRT_CLINICAL_REFERENCE_TEST_LIMIT", "250")) if test_mode else None
    await _ensure_schema_exists(schema)
    await _ensure_unified_code_tables(schema)

    shared_models = [
        CodeCatalog,
        CodeCrosswalk,
        CodeSynonym,
        CodeRelationship,
    ]
    area_models = [
        ClinicalArea,
        ClinicalAreaCondition,
        ClinicalAreaTreatment,
    ]
    models = shared_models + area_models
    stages = {model: make_class(model, import_suffix) for model in models}
    for stage_cls in stages.values():
        await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_cls.__tablename__};")
        await db.create_table(stage_cls.__table__, checkfirst=True)

    concept_rows: list[dict[str, Any]] = []
    synonym_rows: list[dict[str, Any]] = []
    crosswalk_rows: list[dict[str, Any]] = []
    relationship_rows: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}

    if "icd10cm" in selected:
        icd_path = _download_url(os.getenv("HLTHPRT_ICD10CM_URL", CDC_ICD10CM_URL), root / "icd10cm" / "icd10cm-CodeDescriptions-2026.zip", force=force_download, run_id=run_id)
        rows, synonyms, crosswalks = _parse_icd10cm(icd_path, source_test_limit)
        concept_rows.extend(rows); synonym_rows.extend(synonyms); crosswalk_rows.extend(crosswalks)
        source_counts["icd10cm"] = len(rows)

    if "mesh" in selected:
        desc_path = _download_url(os.getenv("HLTHPRT_MESH_DESC_URL", MESH_DESC_URL), root / "mesh" / "desc2026.gz", force=force_download, run_id=run_id)
        supp_path = _download_url(os.getenv("HLTHPRT_MESH_SUPP_URL", MESH_SUPP_URL), root / "mesh" / "supp2026.gz", force=force_download, run_id=run_id)
        for path, source in ((desc_path, "nlm_mesh_descriptor"), (supp_path, "nlm_mesh_supplemental")):
            _raise_if_cancelled(run_id)
            rows, synonyms, relationships = _parse_mesh_file(path, source, source_test_limit)
            concept_rows.extend(rows); synonym_rows.extend(synonyms); relationship_rows.extend(relationships)
            source_counts[source] = len(rows)

    if "rxnorm" in selected:
        _raise_if_cancelled(run_id)
        release = _release_current("rxnorm-full-monthly-release")
        rx_path = _download_url(release["downloadUrl"], root / "rxnorm" / release["fileName"], api_key=umls_key, force=force_download, run_id=run_id)
        rows, synonyms, relationships = _parse_rxnorm(rx_path, source_test_limit)
        for row in rows:
            row["source_release"] = release.get("releaseVersion")
        concept_rows.extend(rows); synonym_rows.extend(synonyms); relationship_rows.extend(relationships)
        source_counts["rxnorm"] = len(rows)

    if "snomed" in selected:
        _raise_if_cancelled(run_id)
        if not umls_key:
            raise RuntimeError("SNOMED import requires HLTHPRT_UMLS_API_KEY or UMLS_API_KEY.")
        release = _release_current("snomed-ct-us-edition")
        snomed_path = _download_url(release["downloadUrl"], root / "snomedct_us" / release["fileName"], api_key=umls_key, force=force_download, run_id=run_id)
        rows, synonyms, relationships = _parse_snomed(snomed_path, source_test_limit)
        for row in rows:
            row["source_release"] = release.get("releaseVersion")
        concept_rows.extend(rows); synonym_rows.extend(synonyms); relationship_rows.extend(relationships)
        source_counts["snomed"] = len(rows)
        try:
            map_release = _release_current("snomed-ct-to-icd-10-cm-mapping-resources")
            map_path = _download_url(map_release["downloadUrl"], root / "snomedct_icd10cm" / map_release["fileName"], api_key=umls_key, force=force_download, run_id=run_id)
            map_rows = _parse_snomed_icd_map(map_path, source_test_limit)
            crosswalk_rows.extend(map_rows)
            source_counts["snomed_icd10cm_map"] = len(map_rows)
        except Exception as exc:
            print(f"SNOMED ICD-10-CM map skipped: {exc}")

    if "medrt" in selected:
        _raise_if_cancelled(run_id)
        rows, synonyms, relationships = await _load_medrt_from_rxclass(test_mode)
        concept_rows.extend(rows); synonym_rows.extend(synonyms); relationship_rows.extend(relationships)
        source_counts["medrt"] = len(rows)

    deduped_concepts = {(row["code_system"], row["code"]): row for row in concept_rows}
    deduped_synonyms = {(row["code_system"], row["code"], row["synonym"], row["term_type"]): row for row in synonym_rows}
    deduped_crosswalks = {(row["from_system"], row["from_code"], row["to_system"], row["to_code"]): row for row in crosswalk_rows}
    deduped_relationships = {
        (row["from_system"], row["from_code"], row["relationship"], row["to_system"], row["to_code"]): row
        for row in relationship_rows
    }

    await _push(stages[CodeCatalog], list(deduped_concepts.values()))
    await _push(stages[CodeSynonym], list(deduped_synonyms.values()))
    await _push(stages[CodeCrosswalk], list(deduped_crosswalks.values()))
    await _push(stages[CodeRelationship], list(deduped_relationships.values()))

    area_rows, area_condition_rows, area_treatment_rows = _build_clinical_area_rows(
        deduped_concepts,
        deduped_relationships,
    )
    await _push(stages[ClinicalArea], area_rows)
    await _push(stages[ClinicalAreaCondition], area_condition_rows)
    await _push(stages[ClinicalAreaTreatment], area_treatment_rows)

    for stage_cls in stages.values():
        await _create_stage_indexes(stage_cls, schema)

    concept_count = int(await db.scalar(f"SELECT COUNT(*) FROM {schema}.{stages[CodeCatalog].__tablename__};") or 0)
    min_rows = int(os.getenv("HLTHPRT_CLINICAL_REFERENCE_MIN_ROWS", "1" if test_mode else "1000"))
    if concept_count < min_rows:
        raise RuntimeError(f"Clinical reference stage has {concept_count} code rows, below minimum {min_rows}.")

    async with db.transaction():
        await _merge_code_catalog_stage(stages[CodeCatalog], schema)
        await _merge_code_crosswalk_stage(stages[CodeCrosswalk], schema)
        await _merge_replace_source_table(CodeSynonym, stages[CodeSynonym], schema)
        await _merge_replace_source_table(CodeRelationship, stages[CodeRelationship], schema)
        for model in area_models:
            await _publish_table(model, stages[model], schema)
        for model in shared_models:
            await db.status(f"DROP TABLE IF EXISTS {schema}.{stages[model].__tablename__};")

    result = {
        "import_id": import_suffix,
        "sources": sorted(selected),
        "source_counts": source_counts,
        "concept_rows": concept_count,
        "synonym_rows": len(deduped_synonyms),
        "crosswalk_rows": len(deduped_crosswalks),
        "relationship_rows": len(deduped_relationships),
        "clinical_area_rows": len(area_rows),
        "clinical_area_condition_rows": len(area_condition_rows),
        "clinical_area_treatment_rows": len(area_treatment_rows),
        "artifact_root": str(root),
        "test_mode": bool(test_mode),
    }
    print(f"Clinical reference import done: {result}")
    return result


async def main(
    test_mode: bool = False,
    import_id: str | None = None,
    sources: str | None = None,
    artifact_root: str | None = None,
    force_download: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    await init_db(db)
    try:
        return await import_clinical_reference(
            test_mode=test_mode,
            import_id=import_id,
            sources=sources,
            artifact_root=artifact_root,
            force_download=force_download,
            run_id=run_id,
        )
    finally:
        await db.disconnect()
