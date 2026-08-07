# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation-bound keyset pagination for exact billing search."""

from __future__ import annotations

from dataclasses import dataclass
import hmac
import re
from typing import Any

from sqlalchemy import text

from api import ptg2_serving
from api.billing_search_access_contract import (
    BillingSearchAuthorizationContext,
    validate_billing_search_authorization_context,
)
from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    BillingSearchCursorKeyring,
    BillingSearchSealedPageCursor,
    BillingSearchCursorState,
    _new_sealed_page_cursor,
    open_billing_search_cursor,
    seal_billing_search_cursor,
)
from api.billing_search_request import (
    BillingSearchRequest,
    validate_billing_search_request,
)
from api.billing_search_transport_contract import (
    _canonical_json_bytes,
    _canonical_sha256,
    _canonical_utc,
    _framed_sha256,
)
from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_geo_contract import BILLING_ADDRESS_SELECTION_CONTRACT
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

_AUTHORIZATION_SCOPE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_CURSOR_AUTH_SCOPE_V1\x00"
_SNAPSHOT_SET_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_SNAPSHOT_SET_V1\x00"
_GENERATION_BUNDLE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_GENERATION_BUNDLE_V1\x00"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}", flags=re.ASCII)
_ADDRESS_TABLES = (
    "entity_address_evidence",
    "entity_address_unified",
)


def _invalid_generation() -> PTG2ManifestArtifactError:
    return PTG2ManifestArtifactError(
        "PTG2 exact billing serving generation is unavailable"
    )


def _strict_sha256(value: object) -> str:
    if (
        type(value) is not str
        or _SHA256_PATTERN.fullmatch(value) is None
        or value == "0" * 64
    ):
        raise _invalid_generation()
    return value


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchGenerationPin:
    """Exact immutable pricing/address generation observed by one request."""

    snapshot_set_sha256: str
    generation_bundle_sha256: str
    address_relation_oid: int
    address_evidence_relation_oid: int

    def __post_init__(self) -> None:
        _strict_sha256(self.snapshot_set_sha256)
        _strict_sha256(self.generation_bundle_sha256)
        if any(
            type(relation_oid) is not int or relation_oid <= 0
            for relation_oid in (
                self.address_relation_oid,
                self.address_evidence_relation_oid,
            )
        ):
            raise _invalid_generation()

    def __repr__(self) -> str:
        return "<billing-search-generation-pin>"


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchCursorBinding:
    """Stable request, authority, and generation inputs for one cursor page."""

    request_fingerprint_sha256: str
    authorization_scope_sha256: str
    generation_bundle_sha256: str
    snapshot_set_sha256: str
    trusted_now: int

    def __post_init__(self) -> None:
        for digest_value in (
            self.request_fingerprint_sha256,
            self.authorization_scope_sha256,
            self.generation_bundle_sha256,
            self.snapshot_set_sha256,
        ):
            _canonical_sha256(digest_value)
        if type(self.trusted_now) is not int or not 0 <= self.trusted_now < 2**63:
            raise _invalid_generation()

    def __repr__(self) -> str:
        return "<billing-search-cursor-binding>"


def billing_search_authorization_scope_sha256(
    authorization_context: BillingSearchAuthorizationContext,
    *,
    trusted_now: str,
) -> str:
    """Digest stable authority while excluding per-request issue/expiry times."""

    validated = validate_billing_search_authorization_context(
        authorization_context,
        trusted_now=trusted_now,
    )
    stable_scope_by_name = {
        "audit_scope_sha256": validated.audit_scope_sha256,
        "capabilities": validated.capabilities,
        "plan_entitlement_sha256": validated.plan_entitlement_sha256,
        "principal_scope_sha256": validated.principal_scope_sha256,
        "quota_scope_sha256": validated.quota_scope_sha256,
        "tenant_scope_sha256": validated.tenant_scope_sha256,
    }
    return _framed_sha256(
        _AUTHORIZATION_SCOPE_DOMAIN,
        _canonical_json_bytes(stable_scope_by_name),
    )


def _optional_generation_text(
    value: object,
    *,
    maximum_characters: int = 4096,
) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_characters
        or not value.isprintable()
    ):
        raise _invalid_generation()
    return value


def _binding_generation_payload(
    selection: PlanReleaseServingSelection,
) -> list[dict[str, Any]]:
    serving_tables_by_snapshot = selection.network_tables_by_snapshot()
    if serving_tables_by_snapshot is None or not selection.in_network_bindings:
        raise _invalid_generation()
    binding_payloads: list[dict[str, Any]] = []
    for binding in selection.in_network_bindings:
        serving_tables = serving_tables_by_snapshot.get(binding.snapshot_id)
        if (
            serving_tables is None
            or not serving_tables.uses_v4_graph
            or not is_release_binding_serving_scope_exact(serving_tables, binding)
            or type(serving_tables.shared_snapshot_key) is not int
            or type(serving_tables.source_count) is not int
        ):
            raise _invalid_generation()
        binding_payloads.append(
            {
                "binding_ordinal": binding.binding_ordinal,
                "coverage_scope_id": _optional_generation_text(
                    serving_tables.coverage_scope_id
                ),
                "plan_id": binding.plan_id,
                "plan_market_type": binding.plan_market_type,
                "shared_snapshot_key": serving_tables.shared_snapshot_key,
                "snapshot_id": binding.snapshot_id,
                "source_count": serving_tables.source_count,
                "source_key": binding.source_key,
                "source_trace_set_hash": _optional_generation_text(
                    serving_tables.source_trace_set_hash
                ),
                "storage_generation": _optional_generation_text(
                    serving_tables.storage_generation
                ),
            }
        )
    return binding_payloads


def billing_search_snapshot_set_sha256(
    selection: PlanReleaseServingSelection,
) -> str:
    """Digest the ordered canonical release and immutable snapshot bindings."""

    if type(selection) is not PlanReleaseServingSelection:
        raise _invalid_generation()
    snapshot_set_by_name = {
        "binding_set_digest": _strict_sha256(selection.binding_set_digest),
        "bindings": _binding_generation_payload(selection),
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
    }
    return _framed_sha256(
        _SNAPSHOT_SET_DOMAIN,
        _canonical_json_bytes(snapshot_set_by_name),
    )


def _quoted_address_relations() -> tuple[tuple[str, str], ...]:
    schema_name = str(ptg2_serving.PTG2_SCHEMA or "")
    if _SCHEMA_PATTERN.fullmatch(schema_name) is None:
        raise _invalid_generation()
    return tuple(
        (
            f"{schema_name}.{table_name}",
            f'"{schema_name}"."{table_name}"',
        )
        for table_name in _ADDRESS_TABLES
    )


async def _locked_address_relation_oids(session) -> tuple[int, int]:
    address_relations = _quoted_address_relations()
    await session.execute(
        text(
            "LOCK TABLE "
            + ", ".join(
                quoted_name for _qualified_name, quoted_name in address_relations
            )
            + " IN ACCESS SHARE MODE"
        )
    )
    relation_oids = await session.scalar(
        text(
            "SELECT ARRAY["
            "to_regclass(:address_relation_name)::oid::bigint, "
            "to_regclass(:evidence_relation_name)::oid::bigint]"
        ),
        {
            "address_relation_name": address_relations[1][0],
            "evidence_relation_name": address_relations[0][0],
        },
    )
    if (
        type(relation_oids) not in {list, tuple}
        or len(relation_oids) != 2
        or any(
            type(relation_oid) is not int or relation_oid <= 0
            for relation_oid in relation_oids
        )
    ):
        raise _invalid_generation()
    return relation_oids[0], relation_oids[1]


async def capture_billing_search_generation_pin(
    session,
    selection: PlanReleaseServingSelection,
) -> BillingSearchGenerationPin:
    """Lock the address relation and capture the complete serving generation."""

    snapshot_set_sha256 = billing_search_snapshot_set_sha256(selection)
    address_relation_oid, address_evidence_relation_oid = (
        await _locked_address_relation_oids(session)
    )
    generation_bundle_sha256 = _framed_sha256(
        _GENERATION_BUNDLE_DOMAIN,
        _canonical_json_bytes(
            {
                "address_relation_oid": address_relation_oid,
                "address_evidence_relation_oid": address_evidence_relation_oid,
                "address_selection_contract": BILLING_ADDRESS_SELECTION_CONTRACT,
                "snapshot_set_sha256": snapshot_set_sha256,
            }
        ),
    )
    return BillingSearchGenerationPin(
        snapshot_set_sha256=snapshot_set_sha256,
        generation_bundle_sha256=generation_bundle_sha256,
        address_relation_oid=address_relation_oid,
        address_evidence_relation_oid=address_evidence_relation_oid,
    )


def build_billing_search_cursor_binding(
    request: BillingSearchRequest,
    authorization_context: BillingSearchAuthorizationContext,
    generation_pin: BillingSearchGenerationPin,
    *,
    trusted_now: str,
) -> BillingSearchCursorBinding:
    """Build stable cursor bindings from fully validated request state."""

    validated_request = validate_billing_search_request(request)
    if type(generation_pin) is not BillingSearchGenerationPin:
        raise _invalid_generation()
    generation_pin.__post_init__()
    _, trusted_time = _canonical_utc(trusted_now)
    return BillingSearchCursorBinding(
        request_fingerprint_sha256=validated_request.request_fingerprint_sha256,
        authorization_scope_sha256=billing_search_authorization_scope_sha256(
            authorization_context,
            trusted_now=trusted_now,
        ),
        generation_bundle_sha256=generation_pin.generation_bundle_sha256,
        snapshot_set_sha256=generation_pin.snapshot_set_sha256,
        trusted_now=int(trusted_time.timestamp()),
    )


def open_billing_search_page_cursor(
    request: BillingSearchRequest,
    *,
    keyring: BillingSearchCursorKeyring,
    binding: BillingSearchCursorBinding,
) -> tuple[int | float | str, ...] | None:
    """Open the optional request cursor and return its internal stable key."""

    validated_request = validate_billing_search_request(request)
    if type(binding) is not BillingSearchCursorBinding:
        raise _invalid_generation()
    binding.__post_init__()
    if validated_request.cursor is None:
        return None
    state = open_billing_search_cursor(
        validated_request.cursor,
        keyring=keyring,
        trusted_now=binding.trusted_now,
        request_fingerprint_sha256=binding.request_fingerprint_sha256,
        authorization_context_sha256=binding.authorization_scope_sha256,
        generation_bundle_sha256=binding.generation_bundle_sha256,
        snapshot_set_sha256=binding.snapshot_set_sha256,
    )
    return state.sort_key


def seal_billing_search_page_cursor(
    sort_key: tuple[int | float | str, ...],
    *,
    keyring: BillingSearchCursorKeyring,
    binding: BillingSearchCursorBinding,
) -> BillingSearchSealedPageCursor:
    """Seal one next-page key against stable request, authority, and generation."""

    if type(binding) is not BillingSearchCursorBinding:
        raise _invalid_generation()
    binding.__post_init__()
    state = BillingSearchCursorState(
        request_fingerprint_sha256=binding.request_fingerprint_sha256,
        authorization_context_sha256=binding.authorization_scope_sha256,
        generation_bundle_sha256=binding.generation_bundle_sha256,
        snapshot_set_sha256=binding.snapshot_set_sha256,
        sort_key=sort_key,
        issued_at=binding.trusted_now,
        expires_at=binding.trusted_now + BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    )
    return _new_sealed_page_cursor(
        seal_billing_search_cursor(
            state,
            keyring=keyring,
            trusted_now=binding.trusted_now,
        ),
        state.sort_key,
    )


__all__ = [
    "BillingSearchCursorBinding",
    "BillingSearchGenerationPin",
    "billing_search_authorization_scope_sha256",
    "billing_search_snapshot_set_sha256",
    "build_billing_search_cursor_binding",
    "capture_billing_search_generation_pin",
    "open_billing_search_page_cursor",
    "seal_billing_search_page_cursor",
]
