# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin a signed canonical membership binding in the provider Reader snapshot."""

from __future__ import annotations

import hashlib
import hmac
import re
import unicodedata
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import text

from api import custom_import_read_http as transport
from api.network_address_scope import network_address_read_scope
from process.custom_import.read_contracts import CustomImportReadRequestError, CustomImportReadUnavailableError
from process.network_serving_read import resolve_network_serving_manifest

_BINDING_FIELDS = {
    "version",
    "client_id",
    "policy_revision",
    "generation_id",
    "manifest_sha256",
    "selector",
    "network_ids",
    "query_sha256",
}
_SHA256 = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class ProviderNetworkBinding:
    """An exact binding authenticated by the existing provider transport permit."""

    generation_id: int
    manifest_sha256: str
    network_ids: tuple[int, ...]
    access_scope_sha256: str


def parse_provider_network_binding(document: dict) -> ProviderNetworkBinding | None:
    """Validate the closed binding against the complete canonical request body."""

    if "network_binding" not in document:
        return None
    binding = document["network_binding"]
    invalid = CustomImportReadRequestError("canonical provider binding is invalid")
    if type(binding) is not dict or set(binding) != _BINDING_FIELDS:
        raise invalid
    _validate_authority(binding)
    identities = _binding_network_ids(binding)
    if type(document["native_query"]) is not dict or set(document["native_query"]) & {
        "plan_network",
        "plan_release_id",
    }:
        raise invalid
    unsigned_by_field = {name: value for name, value in document.items() if name != "network_binding"}
    expected = hashlib.sha256(transport._canonical_json_bytes(unsigned_by_field)).hexdigest()
    if not hmac.compare_digest(binding["query_sha256"], expected):
        raise invalid
    return ProviderNetworkBinding(
        binding["generation_id"],
        binding["manifest_sha256"],
        identities,
        hashlib.sha256(transport._canonical_json_bytes(binding)).hexdigest(),
    )


def _validate_authority(binding):
    """Keep the trusted network policy identity and generation canonical."""

    client = binding["client_id"]
    if (
        type(binding["version"]) is not int
        or binding["version"] != 2
        or type(client) is not str
        or not 1 <= len(client) <= 64
        or client.strip() != client
        or any(unicodedata.category(character) == "Cc" for character in client)
        or type(binding["policy_revision"]) is not int
        or not 0 <= binding["policy_revision"] <= 9223372036854775807
        or type(binding["generation_id"]) is not int
        or not 0 < binding["generation_id"] <= 9223372036854775807
    ):
        raise CustomImportReadRequestError("canonical provider binding is invalid")
    if any(
        type(binding[name]) is not str or _SHA256.fullmatch(binding[name]) is None
        for name in ("manifest_sha256", "query_sha256")
    ):
        raise CustomImportReadRequestError("canonical provider binding is invalid")


def _binding_network_ids(binding):
    """Require exact canonical selector equality without legacy interpretation."""

    selector = binding["selector"]
    identities = binding["network_ids"]
    if (
        type(selector) is not dict
        or set(selector) != {"namespace", "values"}
        or selector["namespace"] != "canonical"
        or type(selector["values"]) is not list
        or type(identities) is not list
        or not 1 <= len(identities) <= 50
        or any(type(identity) is not int or not 0 < identity <= 2147483647 for identity in identities)
        or identities != sorted(set(identities))
        or selector["values"] != identities
        or any(type(identity) is not int for identity in selector["values"])
    ):
        raise CustomImportReadRequestError("canonical provider binding is invalid")
    return tuple(identities)


@asynccontextmanager
async def provider_network_read_scope(session, binding: ProviderNetworkBinding | None):
    """Resolve and hold the eligible retained heap in the caller's Reader transaction."""

    if binding is None:
        yield None
        return
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    manifest = await resolve_network_serving_manifest(driver, generation_id=binding.generation_id)
    if manifest.generation_id != binding.generation_id or not hmac.compare_digest(
        manifest.manifest_sha256, binding.manifest_sha256
    ):
        raise CustomImportReadUnavailableError("canonical provider generation is unavailable")
    await session.execute(text(f'LOCK TABLE "{manifest.schema_name}".entity_address_unified IN ACCESS SHARE MODE'))
    with network_address_read_scope(manifest, binding.network_ids, access_scope_sha256=binding.access_scope_sha256):
        yield manifest


def provider_network_response(body, manifest):
    """Bind a successful signed response to its exact retained generation."""

    reply = transport._response(body, 200)
    if manifest is not None:
        reply.headers["X-Network-Generation"] = str(manifest.generation_id)
    return reply
