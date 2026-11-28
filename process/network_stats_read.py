# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned read queries and request parsers for precomputed network statistics."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import parse_qs

from process.network_compact_layout import compact_identifier
from process.network_serving_read import PinnedNetworkServingManifest, parse_canonical_network_ids
from process.network_stats_contract import (
    DIM_TABLE,
    SPECIALTY_TABLE,
    STATE_SPECIALTY_TABLE,
    STATE_TABLE,
    SUMMARY_TABLE,
    validate_network_stats_summary_document,
)

MAX_STATS_RESPONSE_BYTES = 1_048_576
_STATE_RE = re.compile(r"[A-Z0-9]{2,8}\Z")
_SPECIALTY_KINDS = frozenset({"taxonomy", "classification", "rollup"})
_ALLOWED_QUERY_PARAMS = frozenset(
    {
        "network_ids",
        "network_generation",
        "include_states",
        "include_specialties",
        "include_state_specialties",
        "state",
        "specialty_kind",
        "specialty_key",
    }
)


class NetworkStatsRequestInvalid(ValueError):
    """Query selector or fanout bounds for network statistics are invalid."""


class NetworkStatsUnavailable(ValueError):
    """Selected generation does not publish precomputed network statistics."""


class NetworkStatsDenied(ValueError):
    """Requested network is excluded by the caller's visibility policy."""


@dataclass(frozen=True)
class NetworkStatsQuery:
    network_ids: tuple[int, ...]
    generation_id: int | None = None
    include_states: bool = True
    include_specialties: bool = False
    include_state_specialties: bool = False
    state_codes: tuple[str, ...] = ()
    specialty_kind: str | None = None
    specialty_keys: tuple[str, ...] = ()

    def __post_init__(self):
        if (
            type(self.network_ids) is not tuple
            or not 1 <= len(self.network_ids) <= 100
            or len(set(self.network_ids)) != len(self.network_ids)
            or any(type(nid) is not int or not 1 <= nid <= 2147483647 for nid in self.network_ids)
        ):
            raise NetworkStatsRequestInvalid("network_stats_network_ids_invalid")
        if self.generation_id is not None and (
            type(self.generation_id) is not int or not 1 <= self.generation_id <= 9223372036854775807
        ):
            raise NetworkStatsRequestInvalid("network_stats_generation_invalid")
        if any(
            type(flag_val) is not bool
            for flag_val in (self.include_states, self.include_specialties, self.include_state_specialties)
        ):
            raise NetworkStatsRequestInvalid("network_stats_boolean_invalid")
        if (
            type(self.state_codes) is not tuple
            or len(self.state_codes) > 60
            or len(set(self.state_codes)) != len(self.state_codes)
            or any(type(code) is not str or _STATE_RE.fullmatch(code) is None for code in self.state_codes)
        ):
            raise NetworkStatsRequestInvalid("network_stats_state_invalid")
        if self.specialty_kind is not None and (
            type(self.specialty_kind) is not str or self.specialty_kind not in _SPECIALTY_KINDS
        ):
            raise NetworkStatsRequestInvalid("network_stats_specialty_kind_invalid")
        if (
            type(self.specialty_keys) is not tuple
            or len(self.specialty_keys) > 50
            or len(set(self.specialty_keys)) != len(self.specialty_keys)
            or any(not _is_valid_specialty_key(spec_key) for spec_key in self.specialty_keys)
        ):
            raise NetworkStatsRequestInvalid("network_stats_specialty_key_invalid")
        self._require_fanout_bounds()

    def _require_fanout_bounds(self) -> None:
        has_narrow_spec = bool(self.specialty_keys) or self.specialty_kind == "rollup"
        if self.include_specialties and not has_narrow_spec and len(self.network_ids) > 10:
            raise NetworkStatsRequestInvalid("network_stats_fanout_exceeded")
        if self.include_state_specialties and not self.specialty_keys:
            max_networks = 25 if (self.state_codes or self.specialty_kind == "rollup") else 5
            if len(self.network_ids) > max_networks:
                raise NetworkStatsRequestInvalid("network_stats_fanout_exceeded")


def _is_valid_specialty_key(spec_key) -> bool:
    if type(spec_key) is not str or not spec_key or spec_key.strip() != spec_key:
        return False
    try:
        encoded_key = spec_key.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return len(encoded_key) <= 256 and all(ord(ch) >= 32 and ord(ch) != 127 for ch in spec_key)


def _is_enabled_token(raw_token: str) -> bool:
    if raw_token in ("true", "false"):
        return raw_token == "true"
    raise NetworkStatsRequestInvalid("network_stats_boolean_invalid")


def _parse_generation_token(raw_token: str) -> int:
    if (
        type(raw_token) is not str
        or not raw_token.isascii()
        or not raw_token.isdecimal()
        or len(raw_token) > 19
        or raw_token.startswith("0")
    ):
        raise NetworkStatsRequestInvalid("network_stats_generation_invalid")
    return int(raw_token)


def _parse_csv_tokens(raw_csv: str, max_length: int, *, uppercase: bool = False) -> tuple[str, ...]:
    if type(raw_csv) is not str or not raw_csv or len(raw_csv) > max_length:
        raise NetworkStatsRequestInvalid("network_stats_csv_invalid")
    return tuple(part.strip().upper() if uppercase else part.strip() for part in raw_csv.split(","))


def parse_network_stats_http_query(request) -> NetworkStatsQuery:
    """Decode and validate a strict GET query string for /registry/serving/networks/stats."""
    if request.body or not request.query_string or len(request.query_string) > 2048:
        raise NetworkStatsRequestInvalid("network_stats_request_invalid")
    try:
        params_by_name = parse_qs(
            request.query_string, keep_blank_values=True, strict_parsing=True, errors="strict", max_num_fields=8
        )
    except (ValueError, UnicodeDecodeError) as err:
        raise NetworkStatsRequestInvalid("network_stats_request_invalid") from err
    if (
        set(params_by_name) - _ALLOWED_QUERY_PARAMS
        or "network_ids" not in params_by_name
        or any(len(param_list) != 1 for param_list in params_by_name.values())
    ):
        raise NetworkStatsRequestInvalid("network_stats_request_invalid")
    try:
        network_ids = parse_canonical_network_ids(params_by_name["network_ids"][0])
    except ValueError as err:
        raise NetworkStatsRequestInvalid("network_stats_network_ids_invalid") from err
    generation_id = (
        _parse_generation_token(params_by_name["network_generation"][0])
        if "network_generation" in params_by_name
        else None
    )
    state_codes = (
        _parse_csv_tokens(params_by_name["state"][0], 512, uppercase=True) if "state" in params_by_name else ()
    )
    specialty_kind = params_by_name["specialty_kind"][0] if "specialty_kind" in params_by_name else None
    specialty_keys = (
        _parse_csv_tokens(params_by_name["specialty_key"][0], 2048) if "specialty_key" in params_by_name else ()
    )
    has_spec_filter = specialty_kind is not None or bool(specialty_keys)
    return NetworkStatsQuery(
        network_ids=network_ids,
        generation_id=generation_id,
        include_states=(
            _is_enabled_token(params_by_name["include_states"][0]) if "include_states" in params_by_name else True
        ),
        include_specialties=(
            _is_enabled_token(params_by_name["include_specialties"][0])
            if "include_specialties" in params_by_name
            else has_spec_filter
        ),
        include_state_specialties=(
            _is_enabled_token(params_by_name["include_state_specialties"][0])
            if "include_state_specialties" in params_by_name
            else (bool(state_codes) and has_spec_filter)
        ),
        state_codes=state_codes,
        specialty_kind=specialty_kind,
        specialty_keys=specialty_keys,
    )


def _post_string_tuple(raw_entry, max_csv_length: int, *, uppercase: bool = False) -> tuple[str, ...]:
    if type(raw_entry) is str:
        return _parse_csv_tokens(raw_entry, max_csv_length, uppercase=uppercase)
    if type(raw_entry) is list and raw_entry and all(type(token) is str for token in raw_entry):
        return tuple(token.upper() if uppercase else token for token in raw_entry)
    raise NetworkStatsRequestInvalid("network_stats_request_invalid")


def parse_network_stats_post_query(query_by_field) -> tuple[NetworkStatsQuery, int | None]:
    """Decode a closed JSON query object for POST /registry/serving/catalog/stats."""
    if (
        type(query_by_field) is not dict
        or set(query_by_field) - _ALLOWED_QUERY_PARAMS
        or "network_ids" not in query_by_field
    ):
        raise NetworkStatsRequestInvalid("network_stats_request_invalid")
    raw_ids = query_by_field["network_ids"]
    if type(raw_ids) is not list or not raw_ids or any(type(nid) is not int for nid in raw_ids):
        raise NetworkStatsRequestInvalid("network_stats_network_ids_invalid")
    if len(set(raw_ids)) != len(raw_ids):
        raise NetworkStatsRequestInvalid("network_stats_network_ids_invalid")
    generation_id = (
        _parse_generation_token(query_by_field["network_generation"])
        if "network_generation" in query_by_field
        else None
    )
    state_codes = _post_string_tuple(query_by_field["state"], 512, uppercase=True) if "state" in query_by_field else ()
    specialty_kind = query_by_field.get("specialty_kind")
    specialty_keys = (
        _post_string_tuple(query_by_field["specialty_key"], 2048) if "specialty_key" in query_by_field else ()
    )
    has_spec_filter = specialty_kind is not None or bool(specialty_keys)
    selector = NetworkStatsQuery(
        network_ids=tuple(sorted(raw_ids)),
        generation_id=generation_id,
        include_states=query_by_field.get("include_states", True),
        include_specialties=query_by_field.get("include_specialties", has_spec_filter),
        include_state_specialties=query_by_field.get(
            "include_state_specialties", bool(state_codes) and has_spec_filter
        ),
        state_codes=state_codes,
        specialty_kind=specialty_kind,
        specialty_keys=specialty_keys,
    )
    return selector, generation_id


_BREAKDOWN_BATCH_ROWS = 500


def empty_network_stats_summary() -> dict:
    """Zero-count summary object for networks with no summary row in the pinned generation."""
    return {
        "total_npi_count": 0,
        "office_npi_count": 0,
        "provider_only_npi_count": 0,
        "individual_npi_count": 0,
        "organization_npi_count": 0,
        "unique_address_count": 0,
        "unique_location_count": 0,
        "state_count": 0,
        "states": [],
        "core_state_count": 0,
        "core_states": [],
        "county_count": 0,
        "zip5_count": 0,
    }


def _qualified(schema_name: str, table_name: str) -> str:
    return compact_identifier(schema_name) + "." + compact_identifier(table_name)


def _record_budget_bytes(budget_bytes: list[int], payload_by_field: dict) -> dict:
    encoded = json.dumps(payload_by_field, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    budget_bytes[0] += len(encoded) + 1
    if budget_bytes[0] > MAX_STATS_RESPONSE_BYTES:
        raise NetworkStatsRequestInvalid("network_stats_response_bounds_exceeded")
    return payload_by_field


async def _fetch_bounded_pairs(
    connection, statement: str, params: tuple, mapper, budget_bytes: list[int]
) -> list[tuple[int, dict]]:
    collected_pairs: list[tuple[int, dict]] = []
    offset = 0
    limit_idx = len(params) + 1
    offset_idx = len(params) + 2
    paged_sql = f"{statement} LIMIT ${limit_idx}::integer OFFSET ${offset_idx}::integer"
    while True:
        batch_entries = await connection.fetch(paged_sql, *params, _BREAKDOWN_BATCH_ROWS, offset)
        for entry in batch_entries:
            net_id, item_by_field = mapper(entry)
            collected_pairs.append((net_id, _record_budget_bytes(budget_bytes, item_by_field)))
        if len(batch_entries) < _BREAKDOWN_BATCH_ROWS:
            return collected_pairs
        offset += _BREAKDOWN_BATCH_ROWS


def _group_pairs_by_network(network_ids: tuple[int, ...], pairs: list[tuple[int, dict]]) -> dict[int, list[dict]]:
    grouped_by_network: dict[int, list[dict]] = {nid: [] for nid in network_ids}
    for net_id, item_by_field in pairs:
        grouped_by_network[net_id].append(item_by_field)
    return grouped_by_network


def _map_state_row(entry) -> tuple[int, dict]:
    return int(entry["network_id"]), {
        "state_code": entry["state_code"],
        "is_core_state": bool(entry["is_core_state"]),
        "office_npi_count": int(entry["office_npi_count"]),
        "individual_npi_count": int(entry["individual_npi_count"]),
        "organization_npi_count": int(entry["organization_npi_count"]),
        "unique_address_count": int(entry["unique_address_count"]),
        "unique_location_count": int(entry["unique_location_count"]),
        "county_count": int(entry["county_count"]),
        "zip5_count": int(entry["zip5_count"]),
    }


def _map_specialty_row(entry) -> tuple[int, dict]:
    return int(entry["network_id"]), {
        "specialty_kind": entry["specialty_kind"],
        "specialty_key": entry["specialty_key"],
        "display_name": entry["display_name"],
        "total_npi_count": int(entry["total_npi_count"]),
        "office_npi_count": int(entry["office_npi_count"]),
        "primary_office_npi_count": int(entry["primary_office_npi_count"]),
        "unique_address_count": int(entry["unique_address_count"]),
        "unique_location_count": int(entry["unique_location_count"]),
        "state_count": int(entry["state_count"]),
    }


def _map_state_specialty_row(entry) -> tuple[int, dict]:
    return int(entry["network_id"]), {
        "state_code": entry["state_code"],
        "specialty_kind": entry["specialty_kind"],
        "specialty_key": entry["specialty_key"],
        "display_name": entry["display_name"],
        "office_npi_count": int(entry["office_npi_count"]),
        "primary_office_npi_count": int(entry["primary_office_npi_count"]),
        "unique_address_count": int(entry["unique_address_count"]),
        "unique_location_count": int(entry["unique_location_count"]),
    }


async def _fetch_summaries(connection, stats_schema: str, network_ids: tuple[int, ...]) -> dict[int, dict]:
    summary_rel = _qualified(stats_schema, SUMMARY_TABLE)
    entries = await connection.fetch(
        f"""SELECT network_id, total_npi_count, office_npi_count, provider_only_npi_count,
                   individual_npi_count, organization_npi_count,
                   unique_address_count, unique_location_count,
                   state_count::integer AS state_count, states,
                   core_state_count::integer AS core_state_count, core_states,
                   county_count, zip5_count
            FROM ONLY {summary_rel}
            WHERE network_id = ANY($1::integer[])""",
        list(network_ids),
    )
    summary_by_network: dict[int, dict] = {}
    for entry in entries:
        net_id = int(entry["network_id"])
        doc_by_field = {
            "total_npi_count": int(entry["total_npi_count"]),
            "office_npi_count": int(entry["office_npi_count"]),
            "provider_only_npi_count": int(entry["provider_only_npi_count"]),
            "individual_npi_count": int(entry["individual_npi_count"]),
            "organization_npi_count": int(entry["organization_npi_count"]),
            "unique_address_count": int(entry["unique_address_count"]),
            "unique_location_count": int(entry["unique_location_count"]),
            "state_count": int(entry["state_count"]),
            "states": list(entry["states"]),
            "core_state_count": int(entry["core_state_count"]),
            "core_states": list(entry["core_states"]),
            "county_count": int(entry["county_count"]),
            "zip5_count": int(entry["zip5_count"]),
        }
        summary_by_network[net_id] = validate_network_stats_summary_document(doc_by_field)
    return summary_by_network


async def _fetch_states(
    connection, stats_schema: str, query: NetworkStatsQuery, budget_bytes: list[int]
) -> dict[int, list[dict]]:
    state_rel = _qualified(stats_schema, STATE_TABLE)
    statement = f"""SELECT network_id, state_code, is_core_state, office_npi_count,
                   individual_npi_count, organization_npi_count,
                   unique_address_count, unique_location_count, county_count, zip5_count
            FROM ONLY {state_rel}
            WHERE network_id = ANY($1::integer[])
              AND ($2::text[] IS NULL OR state_code = ANY($2::text[]))
            ORDER BY network_id, office_npi_count DESC, state_code COLLATE "C" ASC"""
    params = (list(query.network_ids), list(query.state_codes) if query.state_codes else None)
    pairs = await _fetch_bounded_pairs(connection, statement, params, _map_state_row, budget_bytes)
    return _group_pairs_by_network(query.network_ids, pairs)


async def _fetch_specialties(
    connection, stats_schema: str, query: NetworkStatsQuery, budget_bytes: list[int]
) -> dict[int, list[dict]]:
    spec_rel = _qualified(stats_schema, SPECIALTY_TABLE)
    dim_rel = _qualified(stats_schema, DIM_TABLE)
    statement = f"""SELECT s.network_id, d.specialty_kind, d.specialty_key, d.display_name,
                   s.total_npi_count, s.office_npi_count, s.primary_office_npi_count,
                   s.unique_address_count, s.unique_location_count, s.state_count::integer AS state_count
            FROM ONLY {spec_rel} s
            JOIN ONLY {dim_rel} d USING (specialty_id)
            WHERE s.network_id = ANY($1::integer[])
              AND ($2::text IS NULL OR d.specialty_kind = $2::text)
              AND ($3::text[] IS NULL OR d.specialty_key = ANY($3::text[]))
            ORDER BY s.network_id, d.specialty_kind COLLATE "C",
                     s.office_npi_count DESC, s.total_npi_count DESC, d.specialty_key COLLATE "C" ASC"""
    params = (
        list(query.network_ids),
        query.specialty_kind,
        list(query.specialty_keys) if query.specialty_keys else None,
    )
    pairs = await _fetch_bounded_pairs(connection, statement, params, _map_specialty_row, budget_bytes)
    return _group_pairs_by_network(query.network_ids, pairs)


async def _fetch_state_specialties(
    connection, stats_schema: str, query: NetworkStatsQuery, budget_bytes: list[int]
) -> dict[int, list[dict]]:
    state_spec_rel = _qualified(stats_schema, STATE_SPECIALTY_TABLE)
    dim_rel = _qualified(stats_schema, DIM_TABLE)
    statement = f"""SELECT ss.network_id, ss.state_code, d.specialty_kind, d.specialty_key, d.display_name,
                   ss.office_npi_count, ss.primary_office_npi_count,
                   ss.unique_address_count, ss.unique_location_count
            FROM ONLY {state_spec_rel} ss
            JOIN ONLY {dim_rel} d USING (specialty_id)
            WHERE ss.network_id = ANY($1::integer[])
              AND ($2::text[] IS NULL OR ss.state_code = ANY($2::text[]))
              AND ($3::text IS NULL OR d.specialty_kind = $3::text)
              AND ($4::text[] IS NULL OR d.specialty_key = ANY($4::text[]))
            ORDER BY ss.network_id, ss.state_code COLLATE "C" ASC, d.specialty_kind COLLATE "C",
                     ss.office_npi_count DESC, d.specialty_key COLLATE "C" ASC"""
    params = (
        list(query.network_ids),
        list(query.state_codes) if query.state_codes else None,
        query.specialty_kind,
        list(query.specialty_keys) if query.specialty_keys else None,
    )
    pairs = await _fetch_bounded_pairs(connection, statement, params, _map_state_specialty_row, budget_bytes)
    return _group_pairs_by_network(query.network_ids, pairs)


async def _require_approved_networks(
    connection, manifest: PinnedNetworkServingManifest, network_ids: tuple[int, ...], control_schema: str
) -> None:
    from process.registry_effective_network_catalog import effective_catalog_networks_sql

    networks_sql = await effective_catalog_networks_sql(connection, manifest, control_schema)
    if networks_sql is not None:
        found = await connection.fetchval(
            f"SELECT count(DISTINCT network.network_id)::integer FROM {networks_sql} network "
            "WHERE network.network_id = ANY($1::integer[])",
            list(network_ids),
        )
    else:
        control_ident = compact_identifier(control_schema)
        found = await connection.fetchval(
            f"""SELECT count(*)::integer FROM (
                  SELECT DISTINCT ON (record_key)
                    (record_json->>'network_id')::integer AS network_id,
                    (record_json->>'archived')::boolean AS deleted
                  FROM ONLY {control_ident}.registry_approved_record
                  WHERE record_kind = 'network'
                    AND approved_revision <= $1
                    AND record_key = ANY($2::text[])
                    AND jsonb_typeof(record_json->'network_id') = 'number'
                    AND record_json->>'network_id' = record_key
                    AND jsonb_typeof(record_json->'archived') = 'boolean'
                  ORDER BY record_key, custom_revision DESC
                ) latest WHERE NOT deleted AND network_id = ANY($3::integer[])""",
            manifest.approved_custom_revision,
            [str(nid) for nid in network_ids],
            list(network_ids),
        )
    if int(found or 0) != len(network_ids):
        raise NetworkStatsDenied("network_stats_unknown_network")


async def read_network_stats(
    connection,
    manifest: PinnedNetworkServingManifest,
    query: NetworkStatsQuery,
    *,
    excluded_network_ids: tuple[int, ...] = (),
    control_schema: str | None = None,
) -> list[dict]:
    """Read verified precomputed network statistics for the selected published generation.

    When control_schema is provided, requested IDs must exist as approved canonical networks;
    otherwise serving-plane reads return the zero-count summary for absent or empty IDs.
    """
    if type(query) is not NetworkStatsQuery:
        raise NetworkStatsRequestInvalid("network_stats_request_invalid")
    if excluded_network_ids and any(nid in excluded_network_ids for nid in query.network_ids):
        raise NetworkStatsDenied("network_stats_forbidden_network")
    if control_schema is not None:
        await _require_approved_networks(connection, manifest, query.network_ids, control_schema)
    layout = manifest.compact_layout if manifest is not None else None
    stats_schema = layout.network_stats_schema if layout is not None else None
    if stats_schema is None:
        raise NetworkStatsUnavailable("network_stats_unavailable")
    summary_by_network = await _fetch_summaries(connection, stats_schema, query.network_ids)
    budget_bytes = [2]
    for net_id in query.network_ids:
        summary_doc_by_field = summary_by_network.get(net_id) or empty_network_stats_summary()
        _record_budget_bytes(budget_bytes, {"network_id": net_id, "summary": summary_doc_by_field})
    states_by_network = (
        await _fetch_states(connection, stats_schema, query, budget_bytes) if query.include_states else None
    )
    specs_by_network = (
        await _fetch_specialties(connection, stats_schema, query, budget_bytes) if query.include_specialties else None
    )
    state_specs_by_network = (
        await _fetch_state_specialties(connection, stats_schema, query, budget_bytes)
        if query.include_state_specialties
        else None
    )
    network_entries = []
    for net_id in query.network_ids:
        summary_by_field = summary_by_network.get(net_id) or empty_network_stats_summary()
        net_entry_by_field = {"network_id": net_id, "summary": summary_by_field}
        if states_by_network is not None:
            net_entry_by_field["states_breakdown"] = states_by_network[net_id]
        if specs_by_network is not None:
            net_entry_by_field["specialties"] = specs_by_network[net_id]
        if state_specs_by_network is not None:
            net_entry_by_field["state_specialties"] = state_specs_by_network[net_id]
        network_entries.append(net_entry_by_field)
    encoded_len = len(
        json.dumps(network_entries, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    )
    if encoded_len > MAX_STATS_RESPONSE_BYTES:
        raise NetworkStatsRequestInvalid("network_stats_response_bounds_exceeded")
    return network_entries
