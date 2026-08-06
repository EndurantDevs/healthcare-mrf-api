# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only handlers for the atomically published FHIR generation."""

from __future__ import annotations

from typing import Any

from sanic import response
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy import text

from api.endpoint.pagination import parse_pagination
from api.formulary_fhir_serving_common import DEFAULT_PAGE_SIZE
from api.formulary_fhir_serving_common import FHIR_SOURCE_TYPE
from api.formulary_fhir_serving_common import MAX_PAGE_SIZE
from api.formulary_fhir_serving_common import current_join
from api.formulary_fhir_serving_common import dataset_payload as _dataset_payload
from api.formulary_fhir_serving_common import iso_value as _iso
from api.formulary_fhir_serving_common import is_fhir_formulary_id
from api.formulary_fhir_serving_common import json_value as _json_value
from api.formulary_fhir_serving_common import row_mapping as _mapping
from api.formulary_fhir_serving_common import source_conditions
from api.formulary_fhir_serving_common import source_plan_identifier
from api.formulary_fhir_serving_common import source_selection
from api.formulary_fhir_serving_common import table_name as _table
from api.formulary_fhir_serving_common import upstream_payload as _upstream_payload
from api.formulary_fhir_serving_lists import list_all_formularies
from api.formulary_fhir_serving_lists import list_fhir_formularies
from api.formulary_fhir_serving_lists import merge_cross_formulary_responses
from api.formulary_fhir_serving_queries import available_tiers as _available_tiers
from api.formulary_fhir_serving_queries import formulary_header as _formulary_header
from api.formulary_fhir_serving_queries import paged_rxnorm_ids as _paged_rxnorm_ids
from api.formulary_fhir_serving_queries import summary_statistics as _summary_statistics
from api.formulary_fhir_serving_queries import variant_rows as _variant_rows
from api.formulary_fhir_serving_summary import summary_payload as _summary_payload
from api.tier_utils import normalize_drug_tier_slug


def _header_payload(database_row) -> dict[str, Any]:
    formulary_by_field = _mapping(database_row)
    return {
        "formulary_id": formulary_by_field["public_id"],
        "formulary_uri": formulary_by_field["public_id"],
        "source_type": FHIR_SOURCE_TYPE,
        "source_id": formulary_by_field["source_id"],
        "plan": {
            "plan_id": None,
            "year": None,
            "marketing_name": formulary_by_field.get("title")
            or formulary_by_field.get("name"),
            "state": None,
            "summary_url": None,
            "marketing_url": None,
        },
        "issuer": {
            "issuer_id": None,
            "issuer_name": None,
            "issuer_marketing_name": None,
        },
        "upstream": _upstream_payload(formulary_by_field),
        "coverage_plan": _json_value(
            formulary_by_field.get("metadata_json"),
            {},
        ),
        "source_plan_identifiers": list(
            formulary_by_field.get("source_plan_identifiers") or []
        ),
        "drug_count": int(formulary_by_field.get("drug_count") or 0),
        "last_updated": _iso(
            formulary_by_field.get("upstream_last_updated")
        ),
        "dataset": _dataset_payload(formulary_by_field),
    }


async def _required_header(
    request,
    formulary_id: str,
    selected_alias: str | None,
):
    header_row = await _formulary_header(
        request.ctx.sa_session,
        formulary_id,
        source_plan_identifier=selected_alias,
    )
    if header_row is None:
        raise NotFound("Unknown formulary identifier or source plan alias")
    return header_row, _mapping(header_row)


async def get_fhir_formulary(request, formulary_id: str):
    """Return CoveragePlan, alias, tier, and dataset metadata."""

    selected_alias = source_plan_identifier(request.args)
    header_row, header_by_field = await _required_header(
        request,
        formulary_id,
        selected_alias,
    )
    formulary_payload = _header_payload(header_row)
    tier_values = await _available_tiers(
        request.ctx.sa_session,
        formulary_id,
        source_plan_identifier=selected_alias,
    )
    formulary_payload["available_tiers"] = [
        {
            "tier_slug": normalize_drug_tier_slug(tier_value),
            "tier_label": tier_value,
        }
        for tier_value in tier_values
    ]
    formulary_payload["available_pharmacy_types"] = []
    formulary_payload["source_id"] = header_by_field.get("source_id")
    return response.json(formulary_payload)


def _consensus(medication_rows: list[dict[str, Any]], field: str):
    field_values = {
        medication_by_field.get(field)
        for medication_by_field in medication_rows
    }
    return next(iter(field_values)) if len(field_values) == 1 else None


def _coverage_variant_payload(
    medication_by_field: dict[str, Any],
) -> dict[str, Any]:
    tier = medication_by_field.get("drug_tier")
    return {
        "source_plan_identifier": medication_by_field[
            "source_plan_identifier"
        ],
        "upstream_medication_id": medication_by_field[
            "upstream_medication_id"
        ],
        "upstream_version_id": medication_by_field.get("upstream_version_id"),
        "drug_tier": tier,
        "drug_tier_slug": normalize_drug_tier_slug(tier) if tier else None,
        "prior_authorization": medication_by_field.get("prior_authorization"),
        "step_therapy": medication_by_field.get("step_therapy"),
        "quantity_limit": medication_by_field.get("quantity_limit"),
        "codings": _json_value(medication_by_field.get("codings_json"), []),
        "alternatives": _json_value(
            medication_by_field.get("alternatives_json"),
            [],
        ),
    }


def _drug_payload(
    rxnorm_id: str,
    medication_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    tier = _consensus(medication_rows, "drug_tier")
    latest_timestamp = max(
        (
            _iso(medication_by_field.get("upstream_last_updated"))
            for medication_by_field in medication_rows
            if medication_by_field.get("upstream_last_updated")
        ),
        default=None,
    )
    return {
        "rxnorm_id": rxnorm_id,
        "drug_name": _consensus(medication_rows, "drug_name")
        or medication_rows[0].get("drug_name"),
        "drug_tier": tier,
        "drug_tier_slug": normalize_drug_tier_slug(tier) if tier else None,
        "prior_authorization": _consensus(
            medication_rows,
            "prior_authorization",
        ),
        "step_therapy": _consensus(medication_rows, "step_therapy"),
        "quantity_limit": _consensus(medication_rows, "quantity_limit"),
        "last_updated": latest_timestamp,
        "coverage_variants": [
            _coverage_variant_payload(medication_by_field)
            for medication_by_field in medication_rows
        ],
    }


def _validated_drug_sort(args) -> tuple[str, str]:
    sort_field = str(args.get("sort") or "name").lower()
    order = str(args.get("order") or "asc").lower()
    if sort_field not in {"name", "tier"} or order not in {"asc", "desc"}:
        raise InvalidUsage("Unsupported FHIR formulary drug sort")
    return sort_field, order


def _variants_by_rxnorm_id(
    medication_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    variants_by_rxnorm_id: dict[str, list[dict[str, Any]]] = {}
    for medication_by_field in medication_rows:
        variants_by_rxnorm_id.setdefault(
            medication_by_field["rxnorm_id"],
            [],
        ).append(medication_by_field)
    return variants_by_rxnorm_id


def _drug_list_payload(
    formulary_id: str,
    selected_alias: str | None,
    header_by_field: dict[str, Any],
    pagination,
    total: int,
    drug_items: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "formulary_id": formulary_id,
        "formulary_uri": formulary_id,
        "source_type": FHIR_SOURCE_TYPE,
        "source_id": header_by_field.get("source_id"),
        "plan_id": None,
        "year": None,
        "source_plan_identifier": selected_alias,
        "upstream": _upstream_payload(header_by_field),
        "coverage_plan": _json_value(
            header_by_field.get("metadata_json"),
            {},
        ),
        "dataset": _dataset_payload(header_by_field),
        "page": pagination.page,
        "page_size": pagination.limit,
        "limit": pagination.limit,
        "offset": pagination.offset,
        "total": total,
        "available_pharmacy_types": [],
        "items": drug_items,
    }


async def list_fhir_formulary_drugs(request, formulary_id: str):
    """Return one SQL-paged drug list with alias-specific variants."""

    selected_alias = source_plan_identifier(request.args)
    _, header_by_field = await _required_header(
        request,
        formulary_id,
        selected_alias,
    )
    sort_field, order = _validated_drug_sort(request.args)
    pagination = parse_pagination(
        request.args,
        default_limit=DEFAULT_PAGE_SIZE,
        max_limit=MAX_PAGE_SIZE,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )
    rxnorm_ids, total = await _paged_rxnorm_ids(
        request.ctx.sa_session,
        formulary_id,
        source_plan_identifier=selected_alias,
        args=request.args,
        limit=pagination.limit,
        offset=pagination.offset,
        sort_field=sort_field,
        order=order,
    )
    medication_rows = await _variant_rows(
        request.ctx.sa_session,
        formulary_id,
        source_plan_identifier=selected_alias,
        rxnorm_ids=rxnorm_ids,
    )
    variants_by_id = _variants_by_rxnorm_id(medication_rows)
    drug_items = [
        _drug_payload(rxnorm_id, variants_by_id[rxnorm_id])
        for rxnorm_id in rxnorm_ids
        if rxnorm_id in variants_by_id
    ]
    return response.json(
        _drug_list_payload(
            formulary_id,
            selected_alias,
            header_by_field,
            pagination,
            total,
            drug_items,
        )
    )


async def get_fhir_formulary_drug(
    request,
    formulary_id: str,
    rxnorm_id: str,
):
    """Return one drug with consensus scalars and all alias variants."""

    selected_alias = source_plan_identifier(request.args)
    _, header_by_field = await _required_header(
        request,
        formulary_id,
        selected_alias,
    )
    medication_rows = await _variant_rows(
        request.ctx.sa_session,
        formulary_id,
        source_plan_identifier=selected_alias,
        rxnorm_id=rxnorm_id,
    )
    if not medication_rows:
        raise NotFound("Drug not found within formulary")
    drug_payload_by_field = _drug_payload(rxnorm_id, medication_rows)
    drug_payload_by_field.update(
        {
            "formulary_id": formulary_id,
            "formulary_uri": formulary_id,
            "source_type": FHIR_SOURCE_TYPE,
            "source_id": header_by_field.get("source_id"),
            "plan_id": None,
            "year": None,
            "upstream": _upstream_payload(header_by_field),
            "coverage_plan": _json_value(
                header_by_field.get("metadata_json"),
                {},
            ),
            "dataset": _dataset_payload(header_by_field),
            "available_pharmacy_types": [],
            "linked_plans": [],
        }
    )
    return response.json(drug_payload_by_field)


async def get_fhir_formulary_summary(request, formulary_id: str):
    """Return consensus tier and restriction statistics for a formulary."""

    selected_alias = source_plan_identifier(request.args)
    _, header_by_field = await _required_header(
        request,
        formulary_id,
        selected_alias,
    )
    summary_by_name, tier_counts = await _summary_statistics(
        request.ctx.sa_session,
        formulary_id,
        source_plan_identifier=selected_alias,
    )
    return response.json(
        _summary_payload(
            formulary_id,
            selected_alias,
            header_by_field,
            summary_by_name,
            tier_counts,
        )
    )


async def _cross_rows(request, rxnorm_id: str) -> list[dict[str, Any]]:
    query_params_by_name: dict[str, Any] = {"rxnorm_id": rxnorm_id}
    conditions = source_conditions(request.args, query_params_by_name)
    conditions.append("m.rxnorm_id = :rxnorm_id")
    cross_query = (
        "SELECT cp.public_id, cp.source_id, cp.upstream_list_id, cpv.title, "
        "cpv.upstream_version_id AS coverage_upstream_version_id, "
        "cpv.upstream_last_updated AS coverage_upstream_last_updated, "
        "cpv.status AS coverage_status, cpv.metadata_json, d.dataset_id, "
        "d.cutoff_at, d.published_at, d.coverage_hash, d.membership_hash, "
        "a.source_plan_identifier, m.drug_tier, m.prior_authorization, "
        "m.step_therapy, m.quantity_limit, med.upstream_medication_id, "
        "med.upstream_version_id, med.drug_name, med.codings_json, "
        "med.upstream_last_updated "
        + current_join()
        + f"JOIN {_table('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id "
        f"JOIN {_table('fhir_formulary_medication')} med "
        "ON med.medication_version_id = m.medication_version_id WHERE "
        + " AND ".join(conditions)
        + " ORDER BY cp.public_id, a.source_plan_identifier"
    )
    database_rows = (
        await request.ctx.sa_session.execute(
            text(cross_query),
            query_params_by_name,
        )
    ).all()
    return [_mapping(database_row) for database_row in database_rows]


def _cross_formulary_item(
    public_id: str,
    plan_by_field: dict[str, Any],
    rxnorm_id: str,
    medication_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    drug_by_field = _drug_payload(rxnorm_id, medication_rows)
    upstream_by_field = _upstream_payload(
        {
            **plan_by_field,
            "upstream_version_id": plan_by_field.get(
                "coverage_upstream_version_id"
            ),
            "upstream_last_updated": plan_by_field.get(
                "coverage_upstream_last_updated"
            ),
            "status": plan_by_field.get("coverage_status"),
        }
    )
    coverage_fields = (
        "drug_tier",
        "drug_tier_slug",
        "prior_authorization",
        "step_therapy",
        "quantity_limit",
        "coverage_variants",
    )
    return {
        "formulary_id": public_id,
        "formulary_uri": public_id,
        "source_type": FHIR_SOURCE_TYPE,
        "source_id": plan_by_field["source_id"],
        "plan_id": None,
        "year": None,
        "plan_marketing_name": plan_by_field.get("title"),
        "state": None,
        "issuer": {"issuer_id": None, "issuer_name": None},
        "upstream_list_id": plan_by_field["upstream_list_id"],
        "dataset_id": plan_by_field["dataset_id"],
        "upstream": upstream_by_field,
        "coverage_plan": _json_value(
            plan_by_field.get("metadata_json"),
            {},
        ),
        "dataset": _dataset_payload(plan_by_field),
        **{
            coverage_field: drug_by_field[coverage_field]
            for coverage_field in coverage_fields
        },
    }


async def cross_fhir_formulary_drug(request, rxnorm_id: str):
    """Return every published FHIR formulary covering one RxNorm drug."""

    medication_rows = await _cross_rows(request, rxnorm_id)
    if not medication_rows:
        raise NotFound("Drug not present in any known formulary")
    variants_by_public_id: dict[str, list[dict[str, Any]]] = {}
    plans_by_public_id: dict[str, dict[str, Any]] = {}
    for medication_by_field in medication_rows:
        public_id = medication_by_field["public_id"]
        variants_by_public_id.setdefault(public_id, []).append(
            medication_by_field
        )
        plans_by_public_id[public_id] = medication_by_field
    formulary_items = [
        _cross_formulary_item(
            public_id,
            plans_by_public_id[public_id],
            rxnorm_id,
            variants_by_public_id[public_id],
        )
        for public_id in sorted(variants_by_public_id)
    ]
    return response.json(
        {"rxnorm_id": rxnorm_id, "formularies": formulary_items}
    )
