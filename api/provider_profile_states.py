# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose independently published state assertions without replacing Florida."""

from __future__ import annotations

import copy
import json
import re
from collections import defaultdict
from collections.abc import Mapping

from sqlalchemy import text

from api.provider_education import _source_assertions
from api.provider_language_merge import _apply_provenance, _provenance_sets
from api.provider_profile_composer_parts import _empty_profile, _source_generation_ids
from db.models import (
    ProviderProfileFact,
    ProviderProfileImportRun,
    ProviderProfileSourcePublication,
    db,
)
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION, _profile_categories, _profile_item

MASSACHUSETTS_SOURCE_KEY = "massachusetts-borim"


async def fetch_massachusetts_profile_projection(npi: int) -> dict | None:
    """Read the publication pointer, run and facts in one database snapshot."""
    schema = ProviderProfileSourcePublication.__table__.schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("provider_profile_schema_invalid")
    table_by_role = {
        "publication": f"{schema}.{ProviderProfileSourcePublication.__tablename__}",
        "run": f"{schema}.{ProviderProfileImportRun.__tablename__}",
        "fact": f"{schema}.{ProviderProfileFact.__tablename__}",
    }
    if not await db.scalar(text(
        "SELECT to_regclass(:publication) IS NOT NULL "
        "AND to_regclass(:run) IS NOT NULL AND to_regclass(:fact) IS NOT NULL"
    ), **table_by_role):
        return None
    fact_rows = await db.all(text(f"""
        SELECT publication.current_run_id AS generation_id,
               publication.published_at AS source_published_at,
               run.status AS run_status, run.source_manifest, fact.*
          FROM {table_by_role['publication']} publication
          JOIN {table_by_role['run']} run ON run.run_id = publication.current_run_id
                                  AND run.source_key = publication.source_key
          LEFT JOIN {table_by_role['fact']} fact ON fact.run_id = publication.current_run_id
                                         AND fact.npi = :npi
         WHERE publication.source_key = :source_key
         ORDER BY fact.fact_id
    """), npi=npi, source_key=MASSACHUSETTS_SOURCE_KEY)
    return _state_projection(npi, [fact_row._mapping for fact_row in fact_rows]) if fact_rows else None


def _state_projection(npi: int, fact_rows: list[Mapping]) -> dict | None:
    """Keep public assertion references separate from their parent profile record."""
    generation_ids = {fact_row["generation_id"] for fact_row in fact_rows}
    if len(generation_ids) != 1 or any(fact_row["run_status"] != "completed" for fact_row in fact_rows):
        raise RuntimeError("state_profile_publication_invalid")
    generation_id = next(iter(generation_ids))
    manifest = fact_rows[0]["source_manifest"]
    descriptor = manifest["source"]
    if descriptor["source_key"] != MASSACHUSETTS_SOURCE_KEY or descriptor["source_kind"] != "state_regulator":
        raise RuntimeError("state_profile_source_mismatch")
    source_by_field = {field: descriptor[field] for field in (
        "source_key", "source_kind", "agency", "jurisdiction", "coverage_scope", "registry_generation",
    )}
    loaded_categories = set(manifest["categories"])
    if not loaded_categories <= {"education", "training"}:
        raise RuntimeError("state_profile_categories_invalid")
    grouped = defaultdict(dict)
    evidence_records = []
    for fact in fact_rows:
        if not fact.get("fact_id") or fact["sensitive"] or not fact["public_default"] or fact["availability"] != "available":
            continue
        if fact["category"] not in loaded_categories:
            raise RuntimeError("state_profile_categories_invalid")
        evidence = fact["source_json"]
        if evidence["source_key"] != source_by_field["source_key"] or evidence["source_record_id"] != fact["source_record_id"]:
            raise RuntimeError("state_profile_source_mismatch")
        # A profile contains many facts; page evidence must identify each assertion.
        public_record_id = f"{source_by_field['source_key']}:{fact['fact_id']}"
        profile_item = _profile_item({**fact, "source_record_id": public_record_id})
        profile_item["source_ids"] = [source_by_field["source_key"]]
        profile_item["quality_flags"] = list(evidence.get("quality_flags") or [])
        profile_item = _source_assertions(profile_item)
        grouped[fact["category"]][fact["fact_id"]] = profile_item
        evidence_records.append({
            **copy.deepcopy(evidence),
            "source_record_id": public_record_id,
            "profile_source_record_id": fact["source_record_id"],
            "fact_id": fact["fact_id"],
        })
    if not evidence_records:
        return None
    return {
        "generation_id": generation_id,
        "source": source_by_field,
        "categories": _profile_categories(grouped, loaded_categories),
        "evidence": {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "npi": npi,
            "generation_id": generation_id,
            "records": evidence_records,
        },
    }


def merge_state_profile_projection(npi: int, projection: Mapping | None, state_projection: Mapping | None) -> dict | None:
    """Append one source under its actual identity, preserving legacy evidence."""
    if state_projection is None:
        return projection
    merged = copy.deepcopy(projection) if projection else {"profile": _empty_profile(npi)}
    if not isinstance(merged.get("profile"), Mapping):
        merged["profile"] = _empty_profile(npi)
    profile = merged["profile"]
    for category, source_group in state_projection["categories"].items():
        group = profile.setdefault("categories", {}).setdefault(category, {"availability": "unavailable", "items": []})
        group["items"].extend(copy.deepcopy(source_group["items"]))
        if group["items"]:
            group["availability"] = "available"
        elif group["availability"] == "unavailable":
            group["availability"] = source_group["availability"]
    profile.setdefault("sources", []).append(copy.deepcopy(state_projection["source"]))
    profile.setdefault("important_context", []).append(
        "Massachusetts education and training are source-reported. Missing training dates do not establish "
        "current enrollment or completion; no clinical experience is inferred."
    )
    source_key = state_projection["source"]["source_key"]
    merged["source_generations"] = {
        **_source_generation_ids(projection, projection.get("profile") if projection else None, None),
        source_key: state_projection["generation_id"],
    }
    merged.setdefault("additional_state_evidence", {})[source_key] = copy.deepcopy(state_projection["evidence"])
    return merged


def canonicalize_training_category(group: dict) -> None:
    """Union support only for equal training values with equal visibility."""
    exact_groups = defaultdict(list)
    for fact in group.get("items", []):
        key = (fact.get("type"), json.dumps(fact.get("value"), sort_keys=True, default=str),
               bool(fact.get("sensitive")), bool(fact.get("public_default")))
        exact_groups[key].append(_source_assertions(fact))
    items = []
    for facts in exact_groups.values():
        merged = copy.deepcopy(facts[0])
        kinds, source_ids, record_ids = _provenance_sets(facts)
        _apply_provenance(merged, facts, kinds, source_ids, record_ids)
        merged["assertion_count"] = max(len(record_ids), *(int(fact.get("assertion_count") or 1) for fact in facts))
        merged["quality_flags"] = sorted({flag for fact in facts for flag in fact.get("quality_flags", [])})
        items.append(merged)
    group["items"] = items
