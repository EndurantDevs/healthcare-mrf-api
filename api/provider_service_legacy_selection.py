# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve managed legacy selectors without weakening frozen release identity."""

import hashlib
import json
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

import asyncpg
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api.network_address_scope import current_network_address_scope
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PTG2_SCHEMA, resolve_plan_release_serving
from api.ptg2_serving_utils import ein_plan_id_variants
from process.network_address_projection import _identifier
from process.ptg_parts.canonical import semantic_sha256

_SELECTOR_FIELDS = (
    "plan_id",
    "plan_external_id",
    "plan_id_type",
    "plan_market_type",
    "market_type",
    "source_key",
    "snapshot_id",
)
_TYPE_CONTRACT = "plan_release_identifier_types_v1"
_TYPE_MANIFEST_FIELD = "legacy_plan_identifier_types"
_RESTRICTION = ContextVar("provider_service_legacy_selection", default=None)


@dataclass(frozen=True)
class _LegacySelection:
    selection: object
    binding_keys: frozenset[tuple[str, int]]
    selectors: tuple[tuple[str, str], ...]
    sha256: str


def has_legacy_service_selectors(args):
    """Recognize routing selectors without treating them as authorization."""
    return any(
        str(args.get(name) or "").strip()
        for name in ("plan_id", "plan_external_id", "plan_id_type", "source_key", "snapshot_id")
    )


def service_legacy_selection_sha256():
    """Bind continuation to every original selector and its exact resolved release."""
    restriction = _RESTRICTION.get()
    return None if restriction is None else restriction.sha256


def restrict_service_bindings(selection, bindings):
    """Intersect admitted original ordinals while preserving the complete release."""
    restriction = _RESTRICTION.get()
    if restriction is None:
        return bindings
    if selection != restriction.selection or selection.bindings != restriction.selection.bindings:
        raise PlanPricingProjectionUnavailable("managed legacy release identity changed")
    if any(binding not in selection.bindings for binding in bindings):
        raise PlanPricingProjectionUnavailable("managed legacy binding identity changed")
    return tuple(binding for binding in bindings if (binding.role, binding.binding_ordinal) in restriction.binding_keys)


def _selectors(args):
    selectors_by_name = {name: str(args.get(name) or "").strip() for name in _SELECTOR_FIELDS}
    for name in ("plan_id_type", "plan_market_type", "market_type", "source_key"):
        selectors_by_name[name] = selectors_by_name[name].lower()
    if args.get("plan_release_id"):
        raise InvalidUsage("Choose a canonical release or legacy plan selectors.")
    if selectors_by_name["plan_id_type"] not in {"", "ein", "hios"}:
        raise InvalidUsage("Legacy plan identifier type is invalid.")
    for primary, alias in (("plan_id", "plan_external_id"), ("plan_market_type", "market_type")):
        if (
            selectors_by_name[primary]
            and selectors_by_name[alias]
            and selectors_by_name[primary] != selectors_by_name[alias]
        ):
            raise InvalidUsage("Legacy plan selectors disagree.")
    if not any(selectors_by_name[name] for name in ("plan_id", "plan_external_id", "source_key", "snapshot_id")):
        raise InvalidUsage("Legacy plan selectors require a source, snapshot or plan identity.")
    return selectors_by_name


def _plan_ids(plan_id, plan_id_type=""):
    """Retain documented EIN punctuation aliases; all other identifiers stay exact."""
    if plan_id_type != "hios" and re.fullmatch(r"[0-9]{2}-?[0-9]{7}", plan_id):
        return ein_plan_id_variants(plan_id)
    return [plan_id] if plan_id else []


def _is_matching_binding(binding, selectors_by_name):
    plan_id = selectors_by_name["plan_id"] or selectors_by_name["plan_external_id"]
    market = selectors_by_name["plan_market_type"] or selectors_by_name["market_type"]
    return (
        (not plan_id or binding.plan_id in _plan_ids(plan_id, selectors_by_name["plan_id_type"]))
        and (not market or binding.plan_market_type.lower() == market)
        and (not selectors_by_name["source_key"] or binding.source_key.lower() == selectors_by_name["source_key"])
        and (not selectors_by_name["snapshot_id"] or binding.snapshot_id == selectors_by_name["snapshot_id"])
    )


async def _resolve_selection(session, selectors_by_name):
    """Use exact source-qualified frozen bindings, never a mutable source pointer."""
    schema = _identifier(PTG2_SCHEMA)
    plan_id = selectors_by_name["plan_id"] or selectors_by_name["plan_external_id"]
    parameters_by_name = {
        "plan_ids": _plan_ids(plan_id, selectors_by_name["plan_id_type"]),
        "plan_id_type": selectors_by_name["plan_id_type"],
        "market": selectors_by_name["plan_market_type"] or selectors_by_name["market_type"],
        "source_key": selectors_by_name["source_key"],
        "snapshot_id": selectors_by_name["snapshot_id"],
    }
    release_result = await session.execute(
        text(f"""
        SELECT DISTINCT revision.plan_release_id
          FROM {schema}.plan_release_serving_revision revision
          JOIN {schema}.plan_release_snapshot_binding binding
            ON binding.serving_revision_id=revision.serving_revision_id
         WHERE revision.serving_status='published' AND revision.release_status='published'
           AND revision.expected_binding_count BETWEEN 1 AND 5000
           AND revision.is_current AND binding.role IN ('in_network','allowed_amounts')
           AND (cardinality(CAST(:plan_ids AS text[]))=0 OR binding.plan_id=ANY(CAST(:plan_ids AS text[])))
           AND (:market='' OR lower(binding.plan_market_type)=:market)
           AND (:source_key='' OR lower(binding.source_key)=:source_key)
           AND (:snapshot_id='' OR binding.snapshot_id=:snapshot_id)
           AND (:plan_id_type='' OR EXISTS (
             SELECT FROM jsonb_array_elements(CASE WHEN jsonb_typeof(
               revision.source_manifest->'legacy_plan_identifier_types'->'bindings')='array'
               THEN revision.source_manifest->'legacy_plan_identifier_types'->'bindings' ELSE '[]'::jsonb END) proof
             WHERE proof->>'plan_id_type'=:plan_id_type AND proof->>'role'=binding.role
               AND proof->'binding_ordinal'=to_jsonb(binding.binding_ordinal)
               AND proof->>'snapshot_id'=binding.snapshot_id AND proof->>'source_key'=binding.source_key
               AND proof->>'plan_id'=binding.plan_id
               AND proof->>'plan_market_type'=coalesce(binding.plan_market_type,'')))
         LIMIT 2
    """),
        parameters_by_name,
    )
    release_ids = list(release_result.scalars())
    if len(release_ids) != 1:
        raise InvalidUsage("Legacy plan selectors do not identify one canonical release.")
    selection = await resolve_plan_release_serving(session, release_ids[0], projection_only=True)
    if selection is None or not selection.bindings:
        raise PlanPricingProjectionUnavailable("managed legacy release is unavailable")
    return selection


def _type_entry(binding):
    return {
        name: getattr(binding, name)
        for name in ("role", "binding_ordinal", "snapshot_id", "source_key", "plan_id", "plan_market_type")
    }


def _validated_type_document(selection, encoded):
    document = json.loads(encoded) if type(encoded) is str else encoded
    identity_by_field = {
        name: getattr(selection, name)
        for name in (
            "healthporta_plan_id",
            "plan_version_id",
            "plan_release_id",
            "serving_revision_id",
            "binding_set_digest",
        )
    }
    if type(document) is not dict or set(document) != {*identity_by_field, "contract", "bindings", "sha256"}:
        raise ValueError("identifier type commitment is incomplete")
    body_by_field = {name: entry for name, entry in document.items() if name != "sha256"}
    digest = hashlib.sha256(
        json.dumps(body_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()
    if (
        document["contract"] != _TYPE_CONTRACT
        or document["sha256"] != digest
        or any(document[name] != coordinates_by_field for name, coordinates_by_field in identity_by_field.items())
    ):
        raise ValueError("identifier type commitment identity differs")
    entries = document["bindings"]
    if type(entries) is not list or len(entries) != len(selection.bindings) or not 1 <= len(entries) <= 5000:
        raise ValueError("identifier type commitment recipe differs")
    for entry, binding in zip(entries, selection.bindings, strict=True):
        coordinates_by_field = _type_entry(binding)
        if (
            type(entry) is not dict
            or set(entry) != {*coordinates_by_field, "plan_id_type", "source_namespace"}
            or any(
                type(entry[name]) is not type(coordinate) or entry[name] != coordinate
                for name, coordinate in coordinates_by_field.items()
            )
        ):
            raise ValueError("identifier type commitment coordinates differ")
        kind, namespace = entry["plan_id_type"], entry["source_namespace"]
        if kind is None:
            if namespace is not None:
                raise ValueError("unproven identifier type has provenance")
        elif (
            kind not in {"ein", "hios"}
            or type(namespace) is not str
            or not namespace
            or namespace != namespace.strip()
            or len(namespace.encode()) > 128
            or any(ord(character) < 32 for character in namespace)
        ):
            raise ValueError("identifier type commitment provenance is invalid")
    return document


_TYPED_WITNESSES_SQL = """
WITH bindings AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset($1::jsonb) AS binding(role text,binding_ordinal integer,
 snapshot_id text,source_key text,plan_id text,plan_market_type text,plan_id_type text)
)
SELECT binding.*,snapshot.status AS snapshot_status,
 CASE binding.role WHEN 'in_network' THEN snapshot.manifest::jsonb->'serving_index'->>'source_key'
 ELSE snapshot.manifest::jsonb->'allowed_amount_index'->>'source_key' END AS snapshot_source,
 witness.witnessed_type,witness.plan_hash,witness.canonical_payload
FROM bindings binding LEFT JOIN {schema}.ptg2_snapshot snapshot USING(snapshot_id)
LEFT JOIN LATERAL (
 SELECT plan.plan_id_type AS witnessed_type,plan.plan_hash::text,plan.canonical_payload::jsonb
 FROM {schema}.ptg2_plan_month month JOIN {schema}.ptg2_plan plan USING(plan_hash)
 WHERE binding.role='in_network' AND month.snapshot_id=binding.snapshot_id
   AND plan.plan_id=binding.plan_id AND coalesce(lower(plan.plan_market_type),'')=binding.plan_market_type
 UNION ALL
 SELECT plan.plan_id_type,plan.plan_hash::text,NULL::jsonb
 FROM {schema}.ptg2_allowed_amount_plan plan
 WHERE binding.role='allowed_amounts' AND plan.snapshot_id=binding.snapshot_id
   AND plan.plan_id=binding.plan_id AND coalesce(lower(plan.plan_market_type),'')=binding.plan_market_type
) witness ON true
"""


def _validate_retained_types(entries, witness_rows):
    entry_by_key = {(entry["role"], entry["binding_ordinal"]): entry for entry in entries}
    seen_keys = set()
    for witness in witness_rows:
        key = witness["role"], witness["binding_ordinal"]
        entry = entry_by_key[key]
        if any(witness[name] != coordinate for name, coordinate in entry.items() if name != "source_namespace") or (
            witness["snapshot_status"] != "published"
            or witness["snapshot_source"] != entry["source_key"]
            or type(witness["witnessed_type"]) is not str
            or witness["witnessed_type"].strip().lower() != entry["plan_id_type"]
        ):
            raise ValueError("retained identifier type differs")
        if entry["role"] == "in_network":
            canonical_plan = witness["canonical_payload"]
            if (
                type(canonical_plan) is not dict
                or set(canonical_plan)
                != {"plan_id", "plan_id_type", "plan_market_type", "plan_name", "issuer_name", "plan_sponsor_name"}
                or semantic_sha256(canonical_plan, domain="plan") != witness["plan_hash"]
                or (
                    canonical_plan["plan_id"] != entry["plan_id"]
                    or canonical_plan["plan_id_type"] != witness["witnessed_type"]
                    or str(canonical_plan["plan_market_type"] or "").strip().lower() != entry["plan_market_type"]
                )
            ):
                raise ValueError("retained plan canonical_plan differs")
        seen_keys.add(key)
    if seen_keys != set(entry_by_key):
        raise ValueError("retained identifier type is absent")


async def _typed_restriction(session, selection, selectors_by_name):
    if not selectors_by_name["plan_id_type"]:
        return None, None
    try:
        encoded = await session.scalar(
            text(f"""
          SELECT CASE WHEN octet_length((source_manifest->'{_TYPE_MANIFEST_FIELD}')::text)<=8388608
            THEN (source_manifest->'{_TYPE_MANIFEST_FIELD}')::text END
          FROM {_identifier(PTG2_SCHEMA)}.plan_release_serving_revision
          WHERE serving_revision_id=:revision AND plan_release_id=:release AND binding_set_digest=:digest
            AND is_current AND serving_status='published' AND release_status='published'
        """),
            {
                "revision": selection.serving_revision_id,
                "release": selection.plan_release_id,
                "digest": selection.binding_set_digest,
            },
        )
        document = _validated_type_document(selection, encoded)
        entries = [
            entry
            for entry, binding in zip(document["bindings"], selection.bindings, strict=True)
            if entry["plan_id_type"] == selectors_by_name["plan_id_type"]
            and _is_matching_binding(binding, selectors_by_name)
        ]
        if not entries:
            raise ValueError("identifier type has no original contributors")
        from api.provider_service_source_policy import _bounded_rows

        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        witness_rows = await _bounded_rows(
            driver, _TYPED_WITNESSES_SQL.format(schema=_identifier(PTG2_SCHEMA)), json.dumps(entries)
        )
        _validate_retained_types(entries, witness_rows)
        return frozenset((entry["role"], entry["binding_ordinal"]) for entry in entries), document["sha256"]
    except ValueError, TypeError, KeyError, asyncpg.PostgresError, asyncpg.InterfaceError, DBAPIError:
        raise PlanPricingProjectionUnavailable("managed legacy identifier type evidence is unavailable") from None


def _restriction(selection, selectors_by_name, *, typed_keys=None, identifier_types_sha256=None):
    binding_keys = frozenset(
        (binding.role, binding.binding_ordinal)
        for binding in selection.bindings
        if _is_matching_binding(binding, selectors_by_name)
        and (typed_keys is None or (binding.role, binding.binding_ordinal) in typed_keys)
    )
    if not binding_keys or len(selection.bindings) > 5000:
        raise PlanPricingProjectionUnavailable("managed legacy binding identity is unavailable")
    selectors = tuple(sorted(selectors_by_name.items()))
    document_by_field = {
        "selectors": selectors,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "binding_set_digest": selection.binding_set_digest,
        "binding_keys": sorted(binding_keys),
    }
    if identifier_types_sha256 is not None:
        document_by_field["identifier_types_sha256"] = identifier_types_sha256
    digest = hashlib.sha256(json.dumps(document_by_field, separators=(",", ":"), sort_keys=True).encode()).hexdigest()
    return _LegacySelection(selection, binding_keys, selectors, digest)


@asynccontextmanager
async def legacy_service_selection(session, args):
    """Normalize under a trusted managed policy and restore it on every exit."""
    scope = current_network_address_scope()
    if scope is None or not has_legacy_service_selectors(args):
        yield args
        return
    selectors_by_name = _selectors(args)
    selection = await _resolve_selection(session, selectors_by_name)
    typed_keys, type_digest = await _typed_restriction(session, selection, selectors_by_name)
    restriction = _restriction(selection, selectors_by_name, typed_keys=typed_keys, identifier_types_sha256=type_digest)
    normalized_by_name = {name: args.getlist(name) for name in args if name not in _SELECTOR_FIELDS}
    normalized_by_name["plan_release_id"] = [selection.plan_release_id]
    normalized = RequestParameters(normalized_by_name)
    token = _RESTRICTION.set(restriction)
    try:
        yield normalized
    finally:
        _RESTRICTION.reset(token)
