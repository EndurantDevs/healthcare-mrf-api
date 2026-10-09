# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned imported membership and exact ordering for billing candidates."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from copy import copy, deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from types import MappingProxyType
from uuid import UUID

from sqlalchemy import BigInteger, String, any_, bindparam, case, cast, column, func, select, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql.type_api import TypeEngine

from api import custom_import_read_http as transport
from api.billing_search_import_contract import (
    BillingSearchImportCursorScope,
    _new_billing_search_composed_order,
)
from api.custom_import_provider_service_sql import ProviderServiceImportQuery
from api.custom_import_provider_sql import CompiledNpiEntityRelation, compile_npi_entity_relation
from api.ptg2_billing_search_contract import BillingSearchProviderCandidate, serving_unavailable
from process.custom_import.read_contracts import PinnedReadTarget
from process.custom_import.read_core import PreparedNpiEntityRelation

_SEAL_KEY = secrets.token_bytes(32)
_SEAL_DOMAIN = b"BILLING_IMPORTED_QUERY_STATE_V1\x00"
_GENERATION_DOMAIN = b"BILLING_IMPORTED_GENERATION_V1\x00"
_QUERY_DOMAIN = b"BILLING_IMPORTED_QUERY_SCOPE_V1\x00"


@dataclass(frozen=True, slots=True, repr=False)
class BillingImportQuery:
    """Factory-sealed prepared query paired with one native access state."""

    prepared: PreparedNpiEntityRelation
    compiled: CompiledNpiEntityRelation
    output_types: tuple
    require_match: bool
    import_scope: BillingSearchImportCursorScope
    endpoint_access_state_sha256: str
    _seal: bytes = field(repr=False)


def _binding_value(value):
    """Keep type and exact scalar value in private statement authentication."""

    kind = f"{type(value).__module__}.{type(value).__qualname__}"
    if value is None or type(value) in {bool, int, float, str}:
        return [kind, value]
    if type(value) in {Decimal, date, datetime, UUID}:
        return [kind, str(value)]
    if type(value) is bytes:
        return [kind, value.hex()]
    if type(value) in {tuple, list}:
        return [kind, [_binding_value(element) for element in value]]
    if type(value) is dict and all(type(key) is str for key in value):
        return [kind, {key: _binding_value(value[key]) for key in sorted(value)}]
    raise serving_unavailable()


def _type_signature(type_, ancestor_ids=()):
    """Authenticate current constructor semantics, bypassing memoized type keys."""

    if not isinstance(type_, TypeEngine) or id(type_) in ancestor_ids:
        raise serving_unavailable()
    ancestors = (*ancestor_ids, id(type_))
    fresh_type = copy(type_)
    fresh_type.__dict__.pop("_static_cache_key", None)
    nested_types = [
        (name, _type_signature(nested_type, ancestors))
        for name, nested_type in sorted(vars(fresh_type).items())
        if isinstance(nested_type, TypeEngine)
    ]
    variants = [
        (dialect, _type_signature(variant, ancestors)) for dialect, variant in sorted(type_._variant_mapping.items())
    ]
    return [str(fresh_type), repr(fresh_type._static_cache_key), nested_types, variants]


def _compiled_statement_digest(query):
    """Authenticate detached execution values without recompiling the query."""

    compiled = query.compiled
    if type(compiled) is not CompiledNpiEntityRelation:
        raise serving_unavailable()
    if any(
        binding.callable is not None or binding.expanding or binding.literal_execute for binding in compiled.typed_binds
    ):
        raise serving_unavailable()
    binding_documents = [
        [binding.key, _type_signature(binding.type), _binding_value(binding.value)] for binding in compiled.typed_binds
    ]
    if len(binding_documents) != len(compiled.values) or any(
        _binding_value(binding.value) != _binding_value(compiled.values[binding.key])
        for binding in compiled.typed_binds
    ):
        raise serving_unavailable()
    statement_by_field = {
        "sql": compiled.sql,
        "bindings": binding_documents,
        "output_types": [[name, _type_signature(type_)] for name, type_ in query.output_types],
    }
    return hashlib.sha256(transport._canonical_json_bytes(statement_by_field)).hexdigest()


def _freeze_relation(prepared):
    """Compile once and detach mutable parameters/types from the source Select."""

    source = compile_npi_entity_relation(prepared.statement)
    values_by_name = deepcopy(dict(source.values))
    bindings = tuple(
        bindparam(binding.key, value=deepcopy(values_by_name[binding.key]), type_=deepcopy(binding.type))
        for binding in source.typed_binds
    )
    compiled = CompiledNpiEntityRelation(source.sql, MappingProxyType(values_by_name), bindings)
    output_types = tuple((name, deepcopy(column.type)) for name, column in prepared.statement.selected_columns.items())
    return compiled, output_types


def _query_fingerprint(prepared, require_match):
    """Bind effective membership even when the generic read hash omits it."""

    return transport._framed_sha256(
        _QUERY_DOMAIN,
        transport._canonical_json_bytes(
            {
                "prepared_query_fingerprint": prepared.query_fingerprint,
                "require_match": require_match,
            }
        ),
    )


def _query_state(query):
    if type(query) is not BillingImportQuery:
        raise serving_unavailable()
    validated = ProviderServiceImportQuery(query.prepared, query.require_match)
    if validated.require_match != query.require_match:
        raise serving_unavailable()
    if type(query.import_scope) is not BillingSearchImportCursorScope:
        raise serving_unavailable()
    query.import_scope.__post_init__()
    for term in query.prepared.normalized_order_terms:
        term.__post_init__()
    if (
        query.import_scope.query_fingerprint_sha256 != _query_fingerprint(query.prepared, query.require_match)
        or query.import_scope.authorization_scope_sha256 != query.prepared.authorization_scope_sha256
        or type(query.endpoint_access_state_sha256) is not str
        or len(query.endpoint_access_state_sha256) != 64
        or any(character not in "0123456789abcdef" for character in query.endpoint_access_state_sha256)
    ):
        raise serving_unavailable()
    return transport._canonical_json_bytes(
        {
            "prepared_identity": id(query.prepared),
            "statement_identity": id(query.prepared.statement),
            "statement_semantics": _compiled_statement_digest(query),
            "query_fingerprint": query.prepared.query_fingerprint,
            "authorization_scope": query.prepared.authorization_scope_sha256,
            "generation_bundle": query.import_scope.generation_bundle_sha256,
            "endpoint_access": query.endpoint_access_state_sha256,
            "require_match": query.require_match,
            "order": [(term.field_id, term.direction, term.nulls) for term in query.prepared.normalized_order_terms],
        }
    )


def _new_billing_import_query(prepared, require_match, pinned_target, endpoint_access_state_sha256):
    """Seal only the validated prepared query from an authorized read."""

    if type(pinned_target) is not PinnedReadTarget:
        raise serving_unavailable()
    pinned_target.__post_init__()
    validated = ProviderServiceImportQuery(prepared, require_match)
    generation_by_field = {
        "dataset_id": pinned_target.dataset_id,
        "generation_id": pinned_target.generation_id,
        "definition_revision_id": pinned_target.definition_revision_id,
        "schema_revision_id": pinned_target.schema_revision_id,
        "profile_id": pinned_target.profile_id,
    }
    scope = BillingSearchImportCursorScope(
        _query_fingerprint(prepared, validated.require_match),
        prepared.authorization_scope_sha256,
        transport._framed_sha256(_GENERATION_DOMAIN, transport._canonical_json_bytes(generation_by_field)),
    )
    compiled, output_types = _freeze_relation(prepared)
    query = BillingImportQuery(
        prepared, compiled, output_types, validated.require_match, scope, endpoint_access_state_sha256, b""
    )
    object.__setattr__(query, "_seal", hmac.new(_SEAL_KEY, _SEAL_DOMAIN + _query_state(query), hashlib.sha256).digest())
    return validate_billing_import_query(query, endpoint_access_state_sha256=endpoint_access_state_sha256)


def validate_billing_import_query(query, *, endpoint_access_state_sha256):
    """Reject forged, mutated or cross-request composition state."""

    expected = hmac.new(_SEAL_KEY, _SEAL_DOMAIN + _query_state(query), hashlib.sha256).digest()
    if (
        type(query._seal) is not bytes
        or not hmac.compare_digest(query._seal, expected)
        or not hmac.compare_digest(query.endpoint_access_state_sha256, endpoint_access_state_sha256)
    ):
        raise serving_unavailable()
    return query


def _candidate_statement(candidates, query):
    """Order the complete native identity scope with typed configured values."""

    candidate_npis = tuple(candidate.address.npi for candidate in candidates)
    native = (
        func.unnest(bindparam("billing_candidate_npis", candidate_npis, type_=ARRAY(BigInteger)))
        .table_valued(column("npi", BigInteger), with_ordinality="ordinal")
        .render_derived(name="billing_candidates")
    )
    entity_scope = bindparam(
        "billing_candidate_entities",
        tuple(dict.fromkeys(str(npi) for npi in candidate_npis)),
        type_=ARRAY(String),
    )
    frozen_relation = (
        text(query.compiled.sql)
        .bindparams(*query.compiled.typed_binds)
        .columns(**dict(query.output_types))
        .subquery("billing_imported_source")
    )
    imported = (
        select(frozen_relation)
        .where(
            frozen_relation.c.entity_value == any_(entity_scope),
        )
        .subquery("billing_imported")
    )
    is_matching_npi = imported.c.entity_value == cast(native.c.npi, String)
    candidate_join = (
        native.join(imported, is_matching_npi) if query.require_match else native.outerjoin(imported, is_matching_npi)
    )
    statement = select(native.c.ordinal).select_from(candidate_join)
    order_expressions = []
    if query.prepared.normalized_order_terms:
        order_expressions.append(case((imported.c.entity_value.is_(None), 1), else_=0).asc())
        for ordinal, term in enumerate(query.prepared.normalized_order_terms):
            expression = imported.c[f"sort_{ordinal}"]
            directed = expression.asc() if term.direction == "asc" else expression.desc()
            order_expressions.append(directed.nullsfirst() if term.nulls == "first" else directed.nullslast())
    # Input candidates are in native identity order; the ordinal preserves it exactly.
    return statement.order_by(*order_expressions, native.c.ordinal.asc())


async def compose_billing_candidates(session, candidates, query, *, endpoint_access_state_sha256):
    """Select/order all candidates before existing price hydration and paging."""

    query = validate_billing_import_query(query, endpoint_access_state_sha256=endpoint_access_state_sha256)
    if type(candidates) is not tuple or any(
        type(candidate) is not BillingSearchProviderCandidate for candidate in candidates
    ):
        raise serving_unavailable()
    candidate_keys = tuple(candidate.sort_key for candidate in candidates)
    if candidate_keys != tuple(sorted(set(candidate_keys))):
        raise serving_unavailable()
    result = await session.execute(_candidate_statement(candidates, query))
    ordinals = tuple(result.scalars().all())
    if (
        any(type(ordinal) is not int or not 1 <= ordinal <= len(candidates) for ordinal in ordinals)
        or len(set(ordinals)) != len(ordinals)
        or (not query.require_match and len(ordinals) != len(candidates))
    ):
        raise serving_unavailable()
    ordered_candidates = tuple(candidates[ordinal - 1] for ordinal in ordinals)
    order = _new_billing_search_composed_order(
        tuple(candidate.sort_key for candidate in ordered_candidates), query.import_scope
    )
    return ordered_candidates, order
