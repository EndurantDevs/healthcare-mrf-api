# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog only the immutable FHIR captures represented by published recipes."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from urllib.parse import urlsplit
from uuid import UUID

from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_retained_capture import pin_retained_fhir_read, require_retained_fhir_capture
from process.network_fhir_retained_capture_contract import PRODUCER, canonical, is_sha256, text
from process.network_serving_read import PinnedNetworkServingManifest, resolve_network_serving_manifest
from process.registry_fhir_catalog_sources import FHIRCatalogSourceDisposition, FHIRCatalogSourceReadProfile
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    resolve_retained_registry_source_recipes,
)


class FHIRCatalogRetainedError(ValueError):
    """Published recipe provenance or retained capture custody is unavailable."""


def _require(condition):
    if not condition:
        raise FHIRCatalogRetainedError("registry_fhir_catalog_retained_unavailable")


def normalize_fhir_catalog_retained_source_uri(value):
    """Normalize an optional trusted endpoint URI for policy and descriptor equality."""
    if value is None:
        return None
    text(value, 2048)
    parsed = urlsplit(value)
    _require(
        parsed.scheme in {"http", "https"}
        and parsed.netloc
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )
    return value.rstrip("/") + "/"


def _manifest_document(manifest):
    _require(type(manifest) is PinnedNetworkServingManifest)
    return {
        "generation_id": manifest.generation_id,
        "candidate_id": manifest.candidate_id,
        "manifest_sha256": manifest.manifest_sha256,
        "source_recipes_sha256": manifest.source_generations.get(RECIPE_DIGEST_KEY),
    }


def _descriptor(recipe, manifest, database_oid, source_uri):
    source_pin = recipe.source_pin
    _require(type(source_pin) is PinnedFHIRMembershipSource and source_pin.retained_capture is not None)
    capture = source_pin.retained_capture
    _require(
        recipe.binding_coordinates.producer_id == PRODUCER
        and recipe.binding_coordinates.edition_id == capture.capture_id
    )
    oid_by_table = dict(capture.relation_oids)
    return {
        "coordinates": asdict(recipe.binding_coordinates),
        "profile": "retained_normalized",
        "endpoint_id": source_pin.endpoint_id,
        "dataset_sha256": source_pin.dataset_sha256,
        "resource_table_oid": source_pin.read_resource_table_oid,
        "resource_leaf_oids": [source_pin.read_resource_table_oid],
        "header_table_oid": oid_by_table["provider_directory_endpoint_dataset"],
        "database_oid": database_oid,
        "source_uri": normalize_fhir_catalog_retained_source_uri(source_uri),
        "accepted_receipt_id": capture.release_id,
        "accepted_receipt_sha256": capture.capture_sha256,
        # The entire frozen capture is available; this is not current import completeness.
        "complete": True,
        "previous_dataset_id": None,
        "source_aliases": [source_pin.source_id],
        "native_read_profile": FHIRCatalogSourceReadProfile(
            capture.schema_name, capture.owner_role, capture.runtime_roles
        ).document(),
        "native_catalog_sha256": capture.catalog_sha256,
        "admission": None,
        "capture_sha256": capture.capture_sha256,
        "retained_recipe": json.loads(canonical_registry_source_recipes((recipe,))),
        "retained_manifest": manifest,
    }


def retained_fhir_catalog_recipe(descriptor):
    """Decode one closed published recipe, preserving its original identity."""
    _require(type(descriptor) is dict)
    recipes = decode_registry_source_recipes(descriptor.get("retained_recipe"))
    _require(len(recipes) == 1)
    recipe = recipes[0]
    manifest = descriptor.get("retained_manifest")
    _require(
        manifest is None
        or (
            type(manifest) is dict
            and set(manifest) == {"generation_id", "candidate_id", "manifest_sha256", "source_recipes_sha256"}
            and type(manifest["generation_id"]) is int
            and 0 < manifest["generation_id"] < 2**63
            and type(manifest["candidate_id"]) is str
            and str(UUID(manifest["candidate_id"])) == manifest["candidate_id"]
            and UUID(manifest["candidate_id"]).int
            and is_sha256(manifest["manifest_sha256"])
            and is_sha256(manifest["source_recipes_sha256"])
        )
    )
    database_oid = descriptor.get("database_oid")
    _require(type(database_oid) is int and 0 < database_oid < 2**32)
    expected = _descriptor(recipe, manifest, database_oid, descriptor.get("source_uri"))
    _require(canonical(descriptor) == canonical(expected))
    return recipe


def validate_retained_fhir_catalog_descriptor(descriptor):
    """Check closed linkage; publication and native read checks confer authority."""
    retained_fhir_catalog_recipe(descriptor)
    return json.loads(canonical(descriptor))


async def resolve_fhir_catalog_retained_sources(connection, manifest, *, control_schema, source_uris=None):
    """Read published recipes only; omitted legacy imports remain outside this inventory."""
    recipes = await resolve_retained_registry_source_recipes(connection, manifest, control_schema=control_schema)
    manifest_document = _manifest_document(manifest)
    source_uris = {} if source_uris is None else source_uris
    _require(type(source_uris) is dict)
    database_oid = await connection.fetchval("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")
    accepted_at = datetime.now(timezone.utc)
    dispositions = []
    for recipe in recipes:
        source = recipe.source_pin
        if type(source) is not PinnedFHIRMembershipSource or source.retained_capture is None:
            continue
        descriptor = _descriptor(recipe, manifest_document, database_oid, source_uris.get(source.endpoint_id))
        validate_retained_fhir_catalog_descriptor(descriptor)
        dispositions.append(
            FHIRCatalogSourceDisposition(
                source.endpoint_id, source.dataset_id, (source.source_id,), 1, "accepted", None, descriptor, accepted_at
            )
        )
    return tuple(dispositions)


async def resolve_granted_fhir_catalog_sources(connection, recipes, *, source_uris=None):
    """Read exact reviewed capture inputs without requiring a directory publication."""
    if not recipes:
        return ()
    source_uris = {} if source_uris is None else source_uris
    _require(type(source_uris) is dict)
    database_oid = await connection.fetchval("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")
    accepted_at = datetime.now(timezone.utc)
    dispositions = []
    for recipe in recipes:
        _require(type(recipe.source_pin) is PinnedFHIRMembershipSource)
        capture = recipe.source_pin.retained_capture
        _require(capture is not None)
        await require_retained_fhir_capture(connection, capture, verify_content=False)
        descriptor = _descriptor(recipe, None, database_oid, source_uris.get(recipe.source_pin.endpoint_id))
        validate_retained_fhir_catalog_descriptor(descriptor)
        dispositions.append(
            FHIRCatalogSourceDisposition(
                recipe.source_pin.endpoint_id,
                recipe.source_pin.dataset_id,
                (recipe.source_pin.source_id,),
                1,
                "accepted",
                None,
                descriptor,
                accepted_at,
            )
        )
    return tuple(dispositions)


async def has_verified_retained_fhir_catalog_sources(connection, descriptors, *, control_schema):
    """Recheck exact capture custody and any declared publication without writes."""
    _require(type(descriptors) is tuple and 0 < len(descriptors) <= 1000)
    recipes_by_manifest = {}
    for descriptor in descriptors:
        recipe = retained_fhir_catalog_recipe(descriptor)
        expected = descriptor["retained_manifest"]
        if expected is None:
            await require_retained_fhir_capture(connection, recipe.source_pin.retained_capture, verify_content=False)
            continue
        key = canonical(expected)
        if key not in recipes_by_manifest:
            manifest = await resolve_network_serving_manifest(
                connection, generation_id=expected["generation_id"], control_schema=control_schema
            )
            _require(_manifest_document(manifest) == expected)
            recipes_by_manifest[key] = await resolve_retained_registry_source_recipes(
                connection, manifest, control_schema=control_schema
            )
        _require(recipe in recipes_by_manifest[key])
    return True


require_retained_fhir_catalog_sources = has_verified_retained_fhir_catalog_sources


async def pin_retained_fhir_catalog_source(connection, descriptor):
    """Admit the full retained content once for the caller's read-only transaction."""
    recipe = retained_fhir_catalog_recipe(descriptor)
    database_oid = await connection.fetchval("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")
    _require(database_oid == descriptor["database_oid"])
    return await pin_retained_fhir_read(connection, recipe.source_pin.retained_capture)
