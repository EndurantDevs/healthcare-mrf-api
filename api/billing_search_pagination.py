# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation-bound keyset pagination for exact billing search."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import text

from api import ptg2_serving
from api.billing_search_access_contract import BillingSearchAuthorizationContext
from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    BillingSearchCursorKeyring,
    BillingSearchSealedPageCursor,
    BillingSearchCursorState,
    _new_sealed_page_cursor,
    open_billing_search_cursor,
    seal_billing_search_cursor,
)
from api.billing_search_cursor_authentication import (
    billing_search_authorization_scope_sha256,
)
from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_geo_contract import BILLING_ADDRESS_SELECTION_CONTRACT
from process.provider_directory_profile import is_valid_npi
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)

_SNAPSHOT_SET_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_SNAPSHOT_SET_V2\x00"
_GENERATION_BUNDLE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_GENERATION_BUNDLE_V1\x00"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}", flags=re.ASCII)
_LOCATION_KEY_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_ADDRESS_TABLES = ("entity_address_evidence", "entity_address_unified")
_MAX_SNAPSHOT_ID_CHARACTERS = 256
_SERVING_GENERATION_TEXT_FIELDS = (
    "snapshot_id",
    "arch_version",
    "storage",
    "storage_generation",
    "cold_lookup_contract",
    "shared_block_layout",
    "coverage_scope_id",
    "plan_id",
    "plan_market_type",
    "source_key",
    "source_trace_set_hash",
)
_SERVING_GENERATION_INTEGER_FIELDS = (
    "shared_snapshot_key",
    "price_dictionary_item_count",
    "price_dictionary_block_bytes",
    "provider_shard_span",
    "atom_key_bits",
    "price_key_block_span",
    "atom_key_block_span",
    "source_count",
    "code_count",
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


def _canonical_json_bytes(json_object: object) -> bytes:
    try:
        return json.dumps(
            json_object,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise _invalid_generation() from None


def _framed_sha256(domain: bytes, encoded_value: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(encoded_value).to_bytes(8, "big"))
    digest.update(encoded_value)
    return digest.hexdigest()


def _generation_text(
    value: object,
    *,
    optional: bool = False,
    maximum_characters: int = 4096,
) -> str | None:
    if optional and value is None:
        return None
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_characters
        or not value.isascii()
        or not value.isprintable()
    ):
        raise _invalid_generation()
    return value


def _generation_integer(value: object, *, optional: bool = True) -> int | None:
    if optional and value is None:
        return None
    if type(value) is not int or not 0 <= value < 2**63:
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
    """Stable POST request, authority, and generation inputs for one page."""

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
            _strict_sha256(digest_value)
        if type(self.trusted_now) is not int or not 0 <= self.trusted_now < 2**63:
            raise _invalid_generation()

    def __repr__(self) -> str:
        return "<billing-search-cursor-binding>"


def _source_publication_payload(
    serving_tables: object,
) -> dict[str, object]:
    publication = getattr(
        serving_tables, "provider_tax_identity_source_publication", None
    )
    if type(
        publication
    ) is not TaxIdentitySourcePublication or publication.source_count != getattr(
        serving_tables, "source_count", None
    ):
        raise _invalid_generation()
    try:
        publication_payload = publication.as_dict()
        canonical_publication = tax_identity_source_publication_from_metadata(
            publication_payload
        )
    except Exception:
        raise _invalid_generation() from None
    if canonical_publication != publication:
        raise _invalid_generation()
    return publication_payload


def _serving_generation_payload(serving_tables: object) -> dict[str, object]:
    serving_values_by_name: dict[str, object] = {}
    for field_name in _SERVING_GENERATION_TEXT_FIELDS:
        serving_values_by_name[field_name] = _generation_text(
            getattr(serving_tables, field_name, None),
            optional=field_name
            in {"storage", "coverage_scope_id", "source_trace_set_hash"},
        )
    for field_name in _SERVING_GENERATION_INTEGER_FIELDS:
        serving_values_by_name[field_name] = _generation_integer(
            getattr(serving_tables, field_name, None)
        )
    serving_values_by_name["tax_identity_source_publication"] = (
        _source_publication_payload(serving_tables)
    )
    return serving_values_by_name


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
            or serving_tables.snapshot_id != binding.snapshot_id
            or not is_release_binding_serving_scope_exact(serving_tables, binding)
            or type(binding.binding_ordinal) is not int
            or binding.binding_ordinal < 0
            or type(binding.required) is not bool
        ):
            raise _invalid_generation()
        binding_payloads.append(
            {
                "binding_ordinal": binding.binding_ordinal,
                "plan_id": _generation_text(binding.plan_id),
                "plan_market_type": _generation_text(binding.plan_market_type),
                "required": binding.required,
                "role": "in_network",
                "serving_generation": _serving_generation_payload(serving_tables),
                "snapshot_id": _generation_text(binding.snapshot_id),
                "source_key": _generation_text(binding.source_key),
            }
        )
    return binding_payloads


def billing_search_snapshot_set_sha256(
    selection: PlanReleaseServingSelection,
) -> str:
    """Digest the ordered release, snapshot, and source-publication bindings."""

    if type(selection) is not PlanReleaseServingSelection:
        raise _invalid_generation()
    snapshot_set_by_name = {
        "binding_set_digest": _strict_sha256(selection.binding_set_digest),
        "bindings": _binding_generation_payload(selection),
        "plan_release_id": _generation_text(selection.plan_release_id),
        "serving_revision_id": _generation_text(selection.serving_revision_id),
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
    """Lock address relations and capture the complete serving generation."""

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
    request_fingerprint_sha256: str,
    authorization_context: BillingSearchAuthorizationContext,
    generation_pin: BillingSearchGenerationPin,
    *,
    trusted_now: str,
) -> BillingSearchCursorBinding:
    """Bind a cursor to a canonical cursor-independent POST body fingerprint."""

    if type(generation_pin) is not BillingSearchGenerationPin:
        raise _invalid_generation()
    generation_pin.__post_init__()
    return BillingSearchCursorBinding(
        request_fingerprint_sha256=_strict_sha256(request_fingerprint_sha256),
        authorization_scope_sha256=billing_search_authorization_scope_sha256(
            authorization_context,
            trusted_now=trusted_now,
        ),
        generation_bundle_sha256=generation_pin.generation_bundle_sha256,
        snapshot_set_sha256=generation_pin.snapshot_set_sha256,
        trusted_now=int(
            datetime.strptime(trusted_now, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        ),
    )


def validate_billing_search_page_sort_key(
    candidate_sort_key: object,
) -> tuple[int, float, int, str, int, str, str]:
    """Validate the exact provider-address key used for stable pagination."""

    if type(candidate_sort_key) not in {tuple, list} or len(candidate_sort_key) != 7:
        raise _invalid_generation()
    (
        missing_distance,
        distance_miles,
        binding_ordinal,
        snapshot_id,
        npi,
        address_key,
        location_key,
    ) = candidate_sort_key
    if (
        type(missing_distance) is not int
        or missing_distance not in {0, 1}
        or type(distance_miles) is not float
        or not math.isfinite(distance_miles)
        or distance_miles < 0
        or (missing_distance == 1 and distance_miles != 0.0)
        or type(binding_ordinal) is not int
        or not 0 <= binding_ordinal < 2**31
        or type(snapshot_id) is not str
        or not 1 <= len(snapshot_id) <= _MAX_SNAPSHOT_ID_CHARACTERS
        or not snapshot_id.isascii()
        or not snapshot_id.isprintable()
        or type(npi) is not int
        or not is_valid_npi(npi)
        or type(address_key) is not str
        or type(location_key) is not str
        or _LOCATION_KEY_PATTERN.fullmatch(location_key) is None
    ):
        raise _invalid_generation()
    try:
        canonical_address_key = str(UUID(address_key))
    except (AttributeError, ValueError):
        raise _invalid_generation() from None
    if canonical_address_key != address_key:
        raise _invalid_generation()
    return (
        missing_distance,
        0.0 if distance_miles == 0.0 else distance_miles,
        binding_ordinal,
        snapshot_id,
        npi,
        address_key,
        location_key,
    )


def open_billing_search_page_cursor(
    cursor_token: object | None,
    *,
    keyring: BillingSearchCursorKeyring,
    binding: BillingSearchCursorBinding,
) -> tuple[int, float, int, str, int, str, str] | None:
    """Open the optional POST cursor and return its stable provider key."""

    if type(binding) is not BillingSearchCursorBinding:
        raise _invalid_generation()
    binding.__post_init__()
    if cursor_token is None:
        return None
    state = open_billing_search_cursor(
        cursor_token,
        keyring=keyring,
        trusted_now=binding.trusted_now,
        request_fingerprint_sha256=binding.request_fingerprint_sha256,
        authorization_context_sha256=binding.authorization_scope_sha256,
        generation_bundle_sha256=binding.generation_bundle_sha256,
        snapshot_set_sha256=binding.snapshot_set_sha256,
    )
    return validate_billing_search_page_sort_key(state.sort_key)


def seal_billing_search_page_cursor(
    sort_key: tuple[int | float | str, ...],
    *,
    keyring: BillingSearchCursorKeyring,
    binding: BillingSearchCursorBinding,
) -> BillingSearchSealedPageCursor:
    """Seal one next-page key against POST, authority, and generation state."""

    if type(binding) is not BillingSearchCursorBinding:
        raise _invalid_generation()
    binding.__post_init__()
    validated_sort_key = validate_billing_search_page_sort_key(sort_key)
    state = BillingSearchCursorState(
        request_fingerprint_sha256=binding.request_fingerprint_sha256,
        authorization_context_sha256=binding.authorization_scope_sha256,
        generation_bundle_sha256=binding.generation_bundle_sha256,
        snapshot_set_sha256=binding.snapshot_set_sha256,
        sort_key=validated_sort_key,
        issued_at=binding.trusted_now,
        expires_at=binding.trusted_now + BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    )
    return _new_sealed_page_cursor(
        seal_billing_search_cursor(
            state,
            keyring=keyring,
            trusted_now=binding.trusted_now,
        ),
        state,
    )
