# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite native batch eligibility, paging, and existing provider projections."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sanic import response
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import String, column, select, values

from api.endpoint import npi
from api.endpoint.pagination import parse_pagination
from process.custom_import.read_contracts import CustomImportReadUnavailableError

_PAGE_KEYS = {"page", "page_size", "limit", "start", "offset"}
_SHAPE_KEYS = {"extra_info", "address_grouping", "view", "debug", "show"}
_CONTROL_KEYS = (
    _PAGE_KEYS
    | _SHAPE_KEYS
    | {"order_by", "include_total", "include_sources", "include_evidence", "primary_only", "year"}
)


@dataclass(frozen=True, slots=True)
class _NativeBatchState:
    details: dict
    addresses: dict


def parse_native_batch_query(document):
    """Use the existing list vocabulary plus its exact-provider shape options."""

    from api.custom_import_provider_http import _parse_native_query

    if type(document) is not dict:
        raise InvalidUsage("native_query must be an object")
    shape_by_field = {name: value for name, value in document.items() if name in {"extra_info", "address_grouping"}}
    if any(type(value) is not str or len(value) > 2048 for value in shape_by_field.values()):
        raise InvalidUsage("native shape options must be strings")
    list_query_by_field = {name: value for name, value in document.items() if name not in shape_by_field}
    if list_query_by_field.get("view") == "full":
        list_query_by_field["view"] = ""
    args = _parse_native_query(list_query_by_field)
    for name, value in shape_by_field.items():
        args[name] = [value]
    for name in ("address_key", "address_site_key"):
        if name in args:
            args[name] = [npi._normalize_uuid_key(args.get(name), name) or ""]
    if args.get("view", "") not in {"", "card"}:
        raise InvalidUsage("view must be full or card")
    grouping = args.get("address_grouping", "flat").strip().lower()
    if grouping not in npi.ADDRESS_GROUPING_VALUES:
        raise InvalidUsage("address_grouping must be flat or premise")
    if grouping == npi.ADDRESS_GROUPING_PREMISE and any(args.get(name) for name in ("address_key", "address_site_key")):
        raise InvalidUsage("address selectors are not supported with premise grouping")
    return args


def _batch_shape_params(batch_params, native_args):
    options_by_field = dict(batch_params)
    npi._normalize_exact_npi(native_args.get("npi"))
    if npi._is_truthy_arg(native_args.get("debug"), default=False):
        options_by_field.update(include_sources=True, include_evidence=True)
    if native_args.get("address_grouping", "flat").strip().lower() == npi.ADDRESS_GROUPING_PREMISE:
        if not 1 <= options_by_field["address_limit"] <= npi.NPI_DETAIL_ADDRESS_GROUP_MAX_LIMIT:
            raise InvalidUsage("premise address_limit must be between 1 and 5")
    for name in ("include_sources", "include_evidence"):
        if name in native_args and npi._is_truthy_arg(native_args.get(name), default=False) != batch_params[name]:
            raise InvalidUsage("native batch source options must agree")
    return options_by_field


async def read_native_batch(request, batch_params, *, native_args, session, import_context=None):
    """Match every requested identity before paging successful native providers."""

    params = _batch_shape_params(batch_params, native_args)
    state = await _prepare_native_batch(params["npis"], native_args, session, import_context=import_context)
    state = _filter_batch_address_state(state, native_args)
    found_npis = [
        identity for identity in params["npis"] if state.details.get(identity) is not None or state.addresses[identity]
    ]
    witness_by_npi = {}
    eligible_npis = await _batch_eligible_npis(
        request, native_args, import_context, params["npis"], found_npis, session, witness_rows=witness_by_npi
    )
    effective_order = bool(
        (import_context is not None and import_context.prepared.normalized_order_terms) or native_args.get("order_by")
    )
    if not effective_order:
        selected_set = set(eligible_npis)
        eligible_npis = [identity for identity in params["npis"] if identity in selected_set]
    pagination = parse_pagination(native_args, default_limit=len(params["npis"]), max_limit=npi.NPI_BATCH_MAX_SIZE)
    selected = eligible_npis[pagination.offset : pagination.offset + pagination.limit]
    successes = await _hydrate_native_batch(selected, state, params, native_args, session, witness_rows=witness_by_npi)
    missing_npis = [identity for identity in params["npis"] if identity not in set(eligible_npis)]
    failures = [
        npi._npi_batch_provider_result(identity, None, (), (), (), None, params)[0] for identity in missing_npis
    ]
    provider_items = successes + failures
    if not effective_order and not (set(native_args) & _PAGE_KEYS):
        by_npi = {provider_item["npi"]: provider_item for provider_item in provider_items}
        provider_items = [by_npi[identity] for identity in params["npis"]]
    return {
        "items": provider_items,
        "requested": len(params["npis"]),
        "found": len(eligible_npis),
        "not_found": len(missing_npis),
        "pagination": {
            "total": len(eligible_npis),
            "page": pagination.page,
            "offset": pagination.offset,
            "limit": pagination.limit,
            "has_more": pagination.offset + len(selected) < len(eligible_npis),
        },
    }


async def _prepare_native_batch(npis, native_args, session, *, import_context=None):
    details = await npi._build_npi_identity_details_map(npis, session=session)
    has_import_context = import_context is not None
    if not npi._is_truthy_arg(native_args.get("extra_info"), default=False):
        return _NativeBatchState(
            details,
            await npi._rank_npi_batch_addresses(
                npis, session=session, use_request_session=has_import_context, fail_closed=has_import_context
            ),
        )
    base = await npi._fetch_npi_location_candidates_map(npis, session=session)
    overlays = await npi._fetch_provider_directory_address_overlay_map(npis, session=session)
    await npi._apply_location_statuses(
        [address for identity in npis for address in base.get(identity, ())],
        session=session,
        use_request_session=has_import_context,
        fail_closed=has_import_context,
    )
    addresses_by_npi = {
        identity: npi._rank_provider_locations(
            npi._dedupe_addresses_by_key(list(base.get(identity, ())) + list(overlays.get(identity, ())))
        )
        for identity in npis
    }
    return _NativeBatchState(details, addresses_by_npi)


def _filter_batch_address_state(state, native_args):
    address_key = native_args.get("address_key")
    site_key = native_args.get("address_site_key")
    if not address_key and not site_key:
        return state
    addresses_by_npi = {
        identity: [
            address
            for address in addresses
            if (not address_key or str(address.get("address_key") or "").lower() == address_key)
            and (not site_key or npi._is_address_site_key_match(address, site_key))
        ]
        for identity, addresses in state.addresses.items()
    }
    return _NativeBatchState(state.details, addresses_by_npi)


async def _batch_eligible_npis(
    request, native_args, import_context, requested, found_npis, session, *, witness_rows=None
):
    exact_npi = npi._normalize_exact_npi(native_args.get("npi"))
    if exact_npi is not None:
        found_npis = [identity for identity in found_npis if identity == exact_npi]
    if not found_npis:
        return []
    if set(native_args) - _CONTROL_KEYS - {"npi"}:
        args = RequestParameters(
            {
                name: list(field_values)
                for name, field_values in native_args.items()
                if name not in _SHAPE_KEYS | _PAGE_KEYS | {"include_sources", "include_evidence"}
            }
        )
        args.update(page=["1"], limit=[str(len(requested))], start=["0"], view=["card"])
        reply = await npi.list_providers(
            request, native_args=args, import_context=import_context, native_npis=tuple(requested)
        )
        if reply.status != 200:
            raise CustomImportReadUnavailableError("native batch eligibility is unavailable")
        from api.custom_import_provider_http import _provider_payload

        page_payload = _provider_payload(reply.body)
        selected_npis = [int(provider["npi"]) for provider in page_payload["rows"]]
        if witness_rows is not None:
            witness_rows.update({int(provider["npi"]): provider for provider in page_payload["rows"]})
        if page_payload.get("total") != len(selected_npis) or page_payload.get("total_source") not in {
            None,
            "computed",
        }:
            raise CustomImportReadUnavailableError("native batch totals are unavailable")
    elif import_context is not None:
        selected_npis = await _imported_batch_npis(import_context, found_npis, session)
    else:
        selected_npis = sorted(found_npis)
    if native_args.get("order_by", "npi") not in {"npi", "relevance", ""}:
        raise InvalidUsage("order_by must be npi or relevance")
    if native_args.get("order_by") == "relevance" and not native_args.get("q"):
        raise InvalidUsage("order_by=relevance requires q")
    if len(selected_npis) != len(set(selected_npis)) or not set(selected_npis) <= set(found_npis):
        raise CustomImportReadUnavailableError("native batch identity is unavailable")
    return selected_npis


async def _imported_batch_npis(context, found, session):
    eligible = values(column("npi", String), name="native_batch_values").data([(str(value),) for value in found])
    imported = context.prepared.statement.subquery("batch_imported_values")
    statement = select(eligible.c.npi).select_from(
        eligible.outerjoin(imported, imported.c.entity_value == eligible.c.npi)
    )
    if context.require_match:
        statement = statement.where(imported.c.entity_value.is_not(None))
    order_expressions = [imported.c.entity_value.is_(None).asc()]
    for ordinal, term in enumerate(context.prepared.normalized_order_terms):
        expression = imported.c[f"sort_{ordinal}"]
        order_expressions.append((expression.asc() if term.direction == "asc" else expression.desc()).nulls_last())
    result = await session.execute(statement.order_by(*order_expressions, eligible.c.npi.asc()))
    return [int(value) for value in result.scalars().all()]


def _batch_address_selection(npis, state, params, native_args):
    is_premise_grouping = native_args.get("address_grouping", "flat").strip().lower() == npi.ADDRESS_GROUPING_PREMISE
    groups_by_npi = {}
    addresses_by_npi = {}
    for identity in npis:
        ranked = state.addresses[identity]
        if is_premise_grouping:
            complete = npi._group_provider_locations_by_premise(ranked)
            selected = complete[params["address_offset"] : params["address_offset"] + params["address_limit"]]
            groups_by_npi[identity] = (complete, selected)
            addresses_by_npi[identity] = [
                member for group in selected for member in group["members"][: npi.NPI_DETAIL_ADDRESS_GROUP_MEMBER_LIMIT]
            ]
        else:
            addresses_by_npi[identity] = (
                ranked[params["address_offset"] : params["address_offset"] + params["address_limit"]]
                if params["address_limit"]
                else ranked
            )
    return addresses_by_npi, groups_by_npi


async def _hydrate_native_batch(npis, state, params, native_args, session, *, witness_rows=None):
    if not npis:
        return []
    selected, groups = _batch_address_selection(npis, state, params, native_args)
    hydrated = await npi._hydrate_npi_batch_addresses(
        npis,
        selected,
        address_limit=0,
        address_offset=0,
        include_sources=params["include_sources"],
        include_evidence=params["include_evidence"],
        session=session,
    )
    names = await npi._fetch_other_names_map(npis, session=session)
    include_chain = npi._include_chain_provider_enrichment(native_args.get("show"))
    enrichment = await npi._fetch_provider_enrichment_summary_map(npis, include_chain=include_chain, session=session)
    provider_items = []
    for identity in npis:
        provider_item, was_found = npi._npi_batch_provider_result(
            identity,
            state.details.get(identity),
            state.addresses[identity],
            hydrated[identity],
            names.get(identity, ()),
            enrichment.get(identity),
            params,
        )
        if not was_found:
            raise CustomImportReadUnavailableError("native batch provider is unavailable")
        provider = provider_item["provider"]
        provider["provider_enrichment"]["ffs_visibility"] = npi._provider_enrichment_visibility(
            enrichment.get(identity), include_chain=include_chain
        )
        if identity in groups:
            _apply_premise_groups(provider, groups[identity], hydrated[identity], params)
        if native_args.get("view") == "card":
            first_address = hydrated[identity][0] if hydrated[identity] else {}
            provider_item["provider"] = (
                dict(witness_rows[identity])
                if witness_rows and identity in witness_rows
                else npi._provider_card_from_mapping({**provider, **first_address, "npi": identity})
            )
        provider_items.append(provider_item)
    return provider_items


def _apply_premise_groups(provider, groups, hydrated, params):
    complete, selected = groups
    response_groups = []
    cursor = 0
    for group in selected:
        returned = min(len(group["members"]), npi.NPI_DETAIL_ADDRESS_GROUP_MEMBER_LIMIT)
        members = [
            npi._finalize_public_provider_address(
                dict(address),
                include_sources=params["include_sources"],
                include_evidence=params["include_evidence"],
                suppress_conflicting_site_key=True,
            )
            for address in hydrated[cursor : cursor + returned]
        ]
        cursor += returned
        response_groups.append(
            {
                **{name: field_value for name, field_value in group.items() if name != "members"},
                "members": members,
                "member_pagination": npi._member_pagination(len(group["members"]), len(members)),
            }
        )
    provider.pop("address_list", None)
    provider.pop("address_pagination", None)
    provider.update(
        address_grouping=npi.ADDRESS_GROUPING_PREMISE,
        address_groups=response_groups,
        address_group_pagination=npi._address_group_pagination(
            limit=params["address_limit"],
            offset=params["address_offset"],
            returned=len(response_groups),
            total=len(complete),
        ),
    )


def normalize_native_batch_request(raw_body: Any) -> dict[str, Any]:
    """Validate the bounded, shared-option provider batch contract."""
    if not isinstance(raw_body, Mapping):
        raise InvalidUsage("request body must be a JSON object")
    allowed_fields = {
        "npis",
        "address_limit",
        "address_offset",
        "include_sources",
        "include_evidence",
    }
    unknown_fields = sorted(set(raw_body) - allowed_fields)
    if unknown_fields:
        raise InvalidUsage(f"unsupported batch field: {unknown_fields[0]}")
    address_limit = raw_body.get("address_limit", npi.NPI_BATCH_ADDRESS_DEFAULT_LIMIT)
    if isinstance(address_limit, str) and address_limit.strip().lower() == "all":
        address_limit = 0
    return {
        "npis": npi._normalize_npi_batch_npis(raw_body.get("npis")),
        "address_limit": npi._bounded_npi_batch_integer(
            {"address_limit": address_limit},
            "address_limit",
            default=npi.NPI_BATCH_ADDRESS_DEFAULT_LIMIT,
            minimum=0,
            maximum=npi.NPI_BATCH_ADDRESS_MAX_LIMIT,
        ),
        "address_offset": npi._bounded_npi_batch_integer(
            raw_body,
            "address_offset",
            default=0,
            minimum=0,
            maximum=1_000_000,
        ),
        **npi._npi_batch_boolean_map(raw_body),
    }


async def build_native_batch_payload(
    batch_params: Mapping[str, Any],
    *,
    session: Any = None,
) -> dict[str, Any]:
    """Assemble ordered provider summaries with set-based database reads."""
    npis = list(batch_params["npis"])
    detail_by_npi = await npi._build_npi_identity_details_map(npis, session=session)
    ranked_addresses_by_npi = await npi._rank_npi_batch_addresses(npis, session=session)
    selected_addresses_by_npi = await npi._hydrate_npi_batch_addresses(
        npis,
        ranked_addresses_by_npi,
        address_limit=int(batch_params["address_limit"]),
        address_offset=int(batch_params["address_offset"]),
        include_sources=bool(batch_params["include_sources"]),
        include_evidence=bool(batch_params["include_evidence"]),
        session=session,
    )
    other_names_by_npi = await npi._fetch_other_names_map(npis, session=session)
    enrichment_by_npi = await npi._fetch_provider_enrichment_summary_map(npis, session=session)
    response_items: list[dict[str, Any]] = []
    found_count = 0
    for identity in npis:
        provider_result_map, was_found = npi._npi_batch_provider_result(
            identity,
            detail_by_npi.get(identity),
            ranked_addresses_by_npi[identity],
            selected_addresses_by_npi[identity],
            other_names_by_npi.get(identity, []),
            enrichment_by_npi.get(identity),
            batch_params,
        )
        response_items.append(provider_result_map)
        found_count += int(was_found)
    return {
        "items": response_items,
        "requested": len(npis),
        "found": found_count,
        "not_found": len(npis) - found_count,
    }


async def serve_native_batch(request):
    """Return up to 100 provider summaries from one bounded database pipeline."""
    started = time.monotonic()
    request_body = request.json
    native_query = request_body.get("native_query") if isinstance(request_body, Mapping) else None
    if isinstance(request_body, Mapping) and "native_query" in request_body:
        batch_params = npi._normalize_npi_batch_request(
            {name: value for name, value in request_body.items() if name != "native_query"}
        )
        payload = await read_native_batch(
            request,
            batch_params,
            native_args=parse_native_batch_query(native_query),
            session=npi._request_session(request),
        )
    else:
        batch_params = npi._normalize_npi_batch_request(request_body)
        payload = await npi._build_npi_batch_payload(batch_params, session=npi._request_session(request))
    payload["meta"] = {
        "elapsed_ms": round((time.monotonic() - started) * 1000.0, 2),
        "max_batch_size": npi.NPI_BATCH_MAX_SIZE,
        "view": "summary",
    }
    return response.json(payload, default=str)
