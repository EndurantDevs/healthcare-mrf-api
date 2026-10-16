# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full-policy Source reads on the exact approved native Reader snapshot."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from urllib.parse import urlencode

from sanic import response
from sqlalchemy import text

from api.endpoint import npi
from api.provider_collection_network_read import _request_reader
from api.provider_detail_network_read import managed_provider_network_read_scope
from api.provider_directory_cms_entities import read_cms_entities_snapshot
from api.provider_directory_entities_contract import DirectoryReadError, parse_directory_read
from api.provider_directory_medical_groups import read_medical_groups_snapshot
from api.provider_profile_snapshot import provider_profile_read_snapshot
from process.custom_import.read_core import _bounded_read_window
from process.registry_catalog_policy import RegistryOrganizationVisibility

MAX_DIRECTORY_BYTES = 4 * 1024 * 1024
_SOURCE_TIMEOUT_MS = 2_000
_FIELDS = {"kind", "source_id", "shape", "entity_id", "generation_id", "limit", "cursor", "network_generation"}


def parse_managed_directory_query(document):
    """Keep Source-native options closed without admitting private scope authority."""
    if type(document) is not dict or not {"kind", "source_id", "shape"} <= set(document) <= _FIELDS:
        raise ValueError("invalid directory query")
    kind = document["kind"]
    source_id = document["source_id"]
    shape = document["shape"]
    kinds_by_source = {
        "cms-npd": {"organizations", "sites", "payers", "plans", "networks", "practitioner-roles"},
        "cms-doctors": {"medical-groups", "sites"},
    }
    if type(source_id) is not str or type(kind) is not str or kind not in kinds_by_source.get(source_id, ()):
        raise ValueError("invalid source")
    if type(shape) is not str or shape not in {"entities", "entity", "relationships"}:
        raise ValueError("invalid shape")
    if (shape != "entities") != ("entity_id" in document) or (
        "entity_id" in document and type(document["entity_id"]) is not str
    ):
        raise ValueError("invalid identity")
    limit = document.get("limit", 25)
    if type(limit) is not int or not 1 <= limit <= 100 or (shape == "entity" and "cursor" in document):
        raise ValueError("invalid paging")
    fields_by_name = {name: document[name] for name in ("source_id", "generation_id", "cursor") if name in document}
    if any(type(token_text) is not str for token_text in fields_by_name.values()):
        raise ValueError("invalid source token")
    if shape != "entity":
        fields_by_name["limit"] = str(limit)
    query = parse_directory_read(kind, document.get("entity_id"), shape, urlencode(fields_by_name))
    raw_generation = document.get("network_generation")
    if raw_generation is not None:
        from api.provider_detail_network_contract import _decimal

        generation = _decimal(raw_generation, 9223372036854775807, minimum=1)
    else:
        if "network_generation" in document:
            raise ValueError("invalid network generation")
        generation = None
    return replace(query, limit=limit, network_generation=generation)


async def read_managed_directory(request, query, *, policy, exclusions, access_scope_sha256):
    """Read CMS or public Doctors data under one full-policy pin and real request."""
    async with _managed_directory_scope(request, query, policy, exclusions, access_scope_sha256) as (session, manifest):
        query = replace(
            query,
            excluded_network_ids=exclusions,
            network_manifest=manifest,
            network_generation=manifest.generation_id,
            access_scope_sha256=access_scope_sha256,
        )
        source_headers_by_name = {}
        if query.source_id == "cms-doctors":
            source_data = await read_medical_groups_snapshot(session, query)
        else:
            source_data = await read_cms_entities_snapshot(session, query, response_headers=source_headers_by_name)
        response_by_field = {
            "client_id": policy["client_id"],
            "policy_revision": policy["policy_revision"],
            "generation": str(manifest.generation_id),
            "approved_custom_revision": str(manifest.approved_custom_revision),
            "manifest_sha256": manifest.manifest_sha256,
            "data": source_data,
        }
        reply = response.json(
            response_by_field,
            headers={
                "Cache-Control": "private, no-store",
                "X-Network-Generation": str(manifest.generation_id),
                "X-Network-Access-Scope": access_scope_sha256,
            },
        )
        if source_headers_by_name.get("X-CMS-Retained-Publication") == "committed/v1":
            reply.headers["X-CMS-Retained-Publication"] = "committed/v1"
        if len(reply.body) > MAX_DIRECTORY_BYTES:
            raise DirectoryReadError(503)
        return reply


@asynccontextmanager
async def _managed_directory_scope(request, query, policy, exclusions, access_scope_sha256):
    visibility = RegistryOrganizationVisibility(
        tuple(policy["excluded_company_ids"]), tuple(policy["excluded_group_ids"])
    )
    try:
        async with (
            asyncio.timeout(_SOURCE_TIMEOUT_MS / 1000),
            provider_profile_read_snapshot(npi.db, npi._runtime_db_schema(), include_detail=True) as session,
        ):
            await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
            await session.execute(text("SET LOCAL work_mem = '4MB'"))
            async with (
                _bounded_read_window(session, timeout_ms=_SOURCE_TIMEOUT_MS),
                managed_provider_network_read_scope(
                    session,
                    selector=None,
                    network_generation=query.network_generation,
                    excluded_network_ids=exclusions,
                    access_scope_sha256=access_scope_sha256,
                    organization_policy=visibility,
                ) as manifest,
            ):
                with _request_reader(request, session):
                    yield session, manifest
    except TimeoutError:
        raise DirectoryReadError(503) from None
