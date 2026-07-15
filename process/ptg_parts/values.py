# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 value-builder helpers for compact graph objects."""

from __future__ import annotations

from typing import Any

from process.ptg_parts.canonical import (
    _canonicalize_for_json,
    hash_prefix,
    semantic_hash,
    semantic_sha256,
)
from process.ptg_parts.config import PTG2_PROVIDER_BUCKET_COUNT_ENV, _env_int
from process.ptg_parts.domain import (
    PTG2PriceAtomEvent,
    PTG2PriceSetValue,
    PTG2ProviderSetValue,
    PTG2RatePackValue,
    PTG2SourceCatalogEntry,
    PTG2SourceTraceSetValue,
)


def _source_identity_hash(source_type: str, canonical_url: str) -> str:
    return semantic_hash({"source_type": source_type, "canonical_url": canonical_url}, domain="source_identity")


def _catalog_entry_id(entry: PTG2SourceCatalogEntry) -> str:
    return semantic_hash(entry, domain="source_catalog")


def build_provider_set(
    npi: list[int] | tuple[int, ...],
    tin_type: str | None = None,
    tin_value: str | None = None,
) -> dict[str, Any]:
    """Build a canonical, hashed provider-set record from NPIs and optional TIN."""
    normalized_npi = tuple(sorted({int(value) for value in npi if value is not None}))
    payload = PTG2ProviderSetValue(npi=normalized_npi, tin_type=tin_type, tin_value=tin_value)
    provider_set_hash = semantic_hash(payload, domain="provider_set")
    return {
        "provider_set_hash": provider_set_hash,
        "hash_prefix": hash_prefix(provider_set_hash),
        "provider_count": len(normalized_npi),
        "npi": list(normalized_npi),
        "tin_type": tin_type,
        "tin_value": tin_value,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def provider_hash_bucket(provider_set_hash: str, bucket_count: int = 256) -> str:
    """Return the fixed-width bucket derived from a provider-set hash."""
    if bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    bucket = int(str(provider_set_hash)[:8], 16) % bucket_count
    width = max(2, len(format(bucket_count - 1, "x")))
    return format(bucket, f"0{width}x")


def ptg2_provider_bucket_count() -> int:
    """Return the configured positive count of PTG2 provider hash buckets."""
    return max(_env_int(PTG2_PROVIDER_BUCKET_COUNT_ENV, 64), 1)


def build_price_atom(price: PTG2PriceAtomEvent | dict[str, Any]) -> dict[str, Any]:
    """Build a canonical, hashed record for one negotiated-price event."""
    payload = _canonicalize_for_json(price)
    price_atom_hash = semantic_hash(payload, domain="price_atom")
    return {
        "price_atom_hash": price_atom_hash,
        "hash_prefix": hash_prefix(price_atom_hash),
        "canonical_payload": payload,
    }


def build_price_set(price_atom_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    """Build a canonical, hashed set of unique price-atom hashes."""
    normalized_hashes = tuple(sorted({str(value) for value in price_atom_hashes if value}))
    payload = PTG2PriceSetValue(price_atom_hashes=normalized_hashes)
    price_set_hash = semantic_hash(payload, domain="price_set")
    return {
        "price_set_hash": price_set_hash,
        "hash_prefix": hash_prefix(price_set_hash),
        "price_atom_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_source_trace_set(source_trace_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    """Build a canonical, hashed set of unique source-trace hashes."""
    normalized_hashes = tuple(sorted({str(value) for value in source_trace_hashes if value}))
    payload = PTG2SourceTraceSetValue(source_trace_hashes=normalized_hashes)
    source_trace_set_hash = semantic_sha256(payload, domain="source_trace_set")
    return {
        "source_trace_set_hash": source_trace_set_hash,
        "source_trace_hashes": list(normalized_hashes),
    }


def build_rate_pack(
    context_hash: str,
    domain: str,
    procedure_hash: str,
    provider_set_hash: str,
    price_set_hash: str,
    source_trace_set_hash: str,
) -> dict[str, Any]:
    """Build a canonical, hashed rate pack for one provider-procedure context."""
    payload = PTG2RatePackValue(
        context_hash=context_hash,
        domain=domain,
        procedure_hash=procedure_hash,
        provider_set_hash=provider_set_hash,
        price_set_hash=price_set_hash,
        source_trace_set_hash=source_trace_set_hash,
    )
    rate_pack_hash = semantic_hash(payload, domain="rate_pack")
    return {
        "rate_pack_hash": rate_pack_hash,
        "hash_prefix": hash_prefix(rate_pack_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_set_hash": provider_set_hash,
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_provider_set_collection(provider_set_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    """Build a canonical, hashed collection of unique provider-set hashes."""
    normalized_hashes = tuple(sorted({str(value) for value in provider_set_hashes if value}))
    payload = {"provider_set_hashes": normalized_hashes}
    collection_hash = semantic_hash(payload, domain="provider_set_collection")
    return {
        "provider_set_collection_hash": collection_hash,
        "hash_prefix": hash_prefix(collection_hash),
        "provider_set_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_procedure_collection(procedure_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    """Build a canonical, hashed collection of unique procedure hashes."""
    normalized_hashes = tuple(sorted({str(value) for value in procedure_hashes if value}))
    payload = {"procedure_hashes": normalized_hashes}
    collection_hash = semantic_hash(payload, domain="procedure_collection")
    return {
        "procedure_collection_hash": collection_hash,
        "hash_prefix": hash_prefix(collection_hash),
        "procedure_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_rate_pack_group(
    context_hash: str,
    domain: str,
    procedure_hash: str,
    provider_set_hashes: list[str] | tuple[str, ...],
    price_set_hash: str,
    source_trace_set_hash: str,
) -> dict[str, Any]:
    """Build a rate pack that groups provider sets for one procedure context."""
    provider_collection = build_provider_set_collection(provider_set_hashes)
    payload = {
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "provider_set_hashes": tuple(provider_collection["provider_set_hashes"]),
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
    }
    rate_pack_hash = semantic_hash(payload, domain="rate_pack")
    return {
        "rate_pack_hash": rate_pack_hash,
        "hash_prefix": hash_prefix(rate_pack_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_rate_pack_procedure_group(
    context_hash: str,
    domain: str,
    procedure_hashes: list[str] | tuple[str, ...],
    provider_set_hashes: list[str] | tuple[str, ...],
    price_set_hash: str,
    source_trace_set_hash: str,
) -> dict[str, Any]:
    """Build a rate pack that groups procedures and provider sets by context."""
    provider_collection = build_provider_set_collection(provider_set_hashes)
    procedure_collection = build_procedure_collection(procedure_hashes)
    payload = {
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_collection["procedure_collection_hash"],
        "procedure_hashes": tuple(procedure_collection["procedure_hashes"]),
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "provider_set_hashes": tuple(provider_collection["provider_set_hashes"]),
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
    }
    rate_pack_hash = semantic_hash(payload, domain="rate_pack")
    return {
        "rate_pack_hash": rate_pack_hash,
        "hash_prefix": hash_prefix(rate_pack_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_collection["procedure_collection_hash"],
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_fact_chunk(
    context_hash: str,
    domain: str,
    procedure_hash: str,
    provider_bucket: str,
    rate_pack_hashes: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Build a canonical, hashed chunk of rate packs for one provider bucket."""
    normalized_hashes = tuple(sorted({str(value) for value in rate_pack_hashes if value}))
    payload = {
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_bucket": provider_bucket,
        "rate_pack_hashes": normalized_hashes,
    }
    fact_chunk_hash = semantic_hash(payload, domain="fact_chunk")
    return {
        "fact_chunk_hash": fact_chunk_hash,
        "hash_prefix": hash_prefix(fact_chunk_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_bucket": provider_bucket,
        "rate_pack_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_rate_set(context_hash: str, chunk_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    """Build a canonical, hashed set of fact-chunk hashes for a context."""
    normalized_hashes = tuple(sorted({str(value) for value in chunk_hashes if value}))
    payload = {"context_hash": context_hash, "chunk_hashes": normalized_hashes}
    rate_set_hash = semantic_hash(payload, domain="rate_set")
    return {
        "rate_set_hash": rate_set_hash,
        "hash_prefix": hash_prefix(rate_set_hash),
        "context_hash": context_hash,
        "chunk_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }
