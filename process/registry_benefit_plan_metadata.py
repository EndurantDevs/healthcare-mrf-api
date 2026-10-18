# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed trusted metadata reads for explicitly selected permanent benefit plans."""

import asyncio
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit
from uuid import UUID

import httpx

ORIGIN_CONFIG = "HLTHPRT_REGISTRY_PLAN_METADATA_ORIGIN"
TOKEN_CONFIG = "HLTHPRT_REGISTRY_PLAN_METADATA_TOKEN"
MAX_PLAN_IDS = 5000
MAX_CHUNK_IDS = 512
MAX_RESPONSE_BYTES = 1024 * 1024
TIMEOUT_SECONDS = 15
_PATH = "/v1/plan-identity/plans/selected-metadata"
_ACCESS_PATH = "/v1/plan-identity/plans/selected-access"
_PLAN_ID = re.compile(r"hpplan_[0-9A-HJKMNP-TV-Z]{26}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_ITEM_FIELDS = {"healthporta_plan_id", "status", "plan_kind", "display_name"}


class RegistryBenefitPlanMetadataUnavailable(RuntimeError):
    """Configured authority or its bounded response could not be verified."""

    def __init__(self):
        super().__init__("registry_benefit_plan_metadata_unavailable")


class RegistryBenefitPlanSelectionInvalid(ValueError):
    """A selected permanent ID is absent, retired or merged."""

    def __init__(self):
        super().__init__("registry_benefit_plan_selection_invalid")


class RegistryBenefitPlanAccessDenied(PermissionError):
    """Current direct client entitlement or association denies a selected family."""

    def __init__(self):
        super().__init__("registry_benefit_plan_access_denied")


@dataclass(frozen=True)
class RegistryBenefitPlanMetadataSnapshot:
    """One coherent authoritative read; it grants no tenant access or future lease."""

    generation_id: str
    head_fence: int
    receipt_digest: str
    plan_ids: tuple[str, ...]
    client_id: str | None = None
    access_checked_at: tuple[str, ...] = ()


def _invalid():
    return ValueError("registry_benefit_plan_metadata_invalid")


def _origin(value):
    if (
        type(value) is not str
        or not 0 < len(value) <= 512
        or not value.isascii()
        or any(not 32 < ord(character) < 127 for character in value)
        or any(character in value for character in ("?", "#", "\\"))
    ):
        raise _invalid()
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.port is not None
        and not 0 < parsed.port <= 65535
    ):
        raise _invalid()
    return value.removesuffix("/")


def _plan_ids(plan_ids):
    if (
        type(plan_ids) not in {list, tuple}
        or len(plan_ids) > MAX_PLAN_IDS
        or any(type(plan_id) is not str or _PLAN_ID.fullmatch(plan_id) is None for plan_id in plan_ids)
        or len(set(plan_ids)) != len(plan_ids)
    ):
        raise RegistryBenefitPlanSelectionInvalid()
    return tuple(sorted(plan_ids))


def _unique_object(pairs):
    fields_by_name = {}
    for name, field in pairs:
        if name in fields_by_name:
            raise _invalid()
        fields_by_name[name] = field
    return fields_by_name


def _reject_number(_value):
    raise _invalid()


def _header(reply, name):
    values = reply.headers.get_list(name)
    if not values:
        return None
    if len(values) != 1 or not values[0].isascii() or not 0 < len(values[0]) <= 128:
        raise _invalid()
    return values[0].lower()


async def _document(reply, maximum=MAX_RESPONSE_BYTES):
    if (
        reply.status_code != 200
        or _header(reply, "content-type") not in {"application/json", "application/json; charset=utf-8"}
        or _header(reply, "cache-control") not in {"no-store", "private, no-store"}
        or _header(reply, "content-encoding") not in {None, "identity"}
        or reply.headers.get_list("set-cookie")
    ):
        raise _invalid()
    declared, transfer = _header(reply, "content-length"), _header(reply, "transfer-encoding")
    if (
        transfer not in {None, "chunked"}
        or declared is not None
        and (transfer is not None or not declared.isdecimal() or len(declared) > 7 or not 0 < int(declared) <= maximum)
    ):
        raise _invalid()
    body = bytearray()
    async for chunk in reply.aiter_raw(chunk_size=maximum + 1):
        if len(body) + len(chunk) > maximum:
            raise _invalid()
        body.extend(chunk)
    if not body or declared is not None and len(body) != int(declared):
        raise _invalid()
    return json.loads(body.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_number)


def _pin(document):
    if type(document) is not dict or set(document) != {"generation_id", "head_fence", "receipt_digest"}:
        raise _invalid()
    generation = document["generation_id"]
    if (
        type(generation) is not str
        or str(UUID(generation)) != generation
        or UUID(generation).int == 0
        or type(document["head_fence"]) is not int
        or not 0 < document["head_fence"] < 2**63
        or type(document["receipt_digest"]) is not str
        or _DIGEST.fullmatch(document["receipt_digest"]) is None
    ):
        raise _invalid()
    return generation, document["head_fence"], document["receipt_digest"]


def _metadata(document, plan_ids):
    if type(document) is not dict or set(document) != {"registry_pin", "items", "missing_ids"}:
        raise _invalid()
    pin = _pin(document["registry_pin"])
    plan_metadata_rows, missing_ids = document["items"], document["missing_ids"]
    if type(plan_metadata_rows) is not list or len(plan_metadata_rows) > len(plan_ids) or type(missing_ids) is not list:
        raise _invalid()
    present_ids = set()
    has_inactive = False
    for plan_metadata in plan_metadata_rows:
        if (
            type(plan_metadata) is not dict
            or set(plan_metadata) != _ITEM_FIELDS
            or type(plan_metadata["healthporta_plan_id"]) is not str
            or plan_metadata["healthporta_plan_id"] not in plan_ids
            or plan_metadata["healthporta_plan_id"] in present_ids
            or plan_metadata["status"] not in {"active", "retired", "merged"}
            or plan_metadata["plan_kind"] not in {"canonical", "custom_composite"}
            or plan_metadata["display_name"] is not None
            and (type(plan_metadata["display_name"]) is not str or len(plan_metadata["display_name"]) > 512)
        ):
            raise _invalid()
        present_ids.add(plan_metadata["healthporta_plan_id"])
        has_inactive |= plan_metadata["status"] != "active"
    if (
        any(type(plan_id) is not str or plan_id not in plan_ids for plan_id in missing_ids)
        or len(set(missing_ids)) != len(missing_ids)
        or present_ids.intersection(missing_ids)
        or present_ids.union(missing_ids) != set(plan_ids)
    ):
        raise _invalid()
    if missing_ids or has_inactive:
        raise RegistryBenefitPlanSelectionInvalid()
    return pin


def _access_client_id(client_id):
    if (
        type(client_id) is not str
        or not 0 < len(client_id) <= 64
        or client_id != client_id.strip()
        or "\0" in client_id
    ):
        raise _invalid()
    return client_id


def _access(document, client_id, plan_ids):
    if type(document) is not dict or set(document) != {"client_id", "allowed_ids", "denied_ids", "checked_at"}:
        raise _invalid()
    if type(document["client_id"]) is not str or document["client_id"] != client_id:
        raise _invalid()
    partitions = [document["allowed_ids"], document["denied_ids"]]
    for identities in partitions:
        if type(identities) is not list or any(
            type(identity) is not str or identity not in plan_ids for identity in identities
        ):
            raise _invalid()
        identity_set = set(identities)
        if identities != [identity for identity in plan_ids if identity in identity_set]:
            raise _invalid()
    if set(partitions[0]).intersection(partitions[1]) or set(partitions[0]).union(partitions[1]) != set(plan_ids):
        raise _invalid()
    checked_at = document["checked_at"]
    if (
        type(checked_at) is not str
        or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{6})?Z", checked_at) is None
    ):
        raise _invalid()
    datetime.fromisoformat(checked_at)
    if partitions[1]:
        raise RegistryBenefitPlanAccessDenied()
    return checked_at


class RegistryBenefitPlanMetadataClient:
    """Constructor-owned origin/token; every nonempty validation makes fresh reads."""

    def __init__(self, base_url, service_token, *, transport=None):
        try:
            origin = _origin(base_url)
            if (
                type(service_token) is not str
                or not 0 < len(service_token) <= 4096
                or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", service_token) is None
                or transport is not None
                and type(transport) is not httpx.MockTransport
            ):
                raise _invalid()
        except ValueError, TypeError:
            origin = None
        if origin is None:
            raise _invalid()
        self._origin = origin
        self._headers = {"Authorization": "Bearer " + service_token, "Content-Type": "application/json"}
        self._client = httpx.AsyncClient(
            verify=True,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(TIMEOUT_SECONDS, connect=5),
            transport=transport,
            headers={"Accept": "application/json", "Accept-Encoding": "identity", "Cache-Control": "no-store"},
        )

    @classmethod
    def from_config(cls, configuration):
        """Only trusted runtime configuration supplies the optional service binding."""
        if not isinstance(configuration, Mapping):
            raise _invalid()
        origin, token = configuration.get(ORIGIN_CONFIG), configuration.get(TOKEN_CONFIG)
        return None if origin is None and token is None else cls(origin, token)

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *arguments):
        return await self._client.__aexit__(*arguments)

    async def aclose(self):
        """Drain the caller-owned HTTP pool at its normal runtime boundary."""
        await self._client.aclose()

    async def _read(self, path, document):
        body = json.dumps(document, separators=(",", ":")).encode()
        if len(body) > 32768:
            raise _invalid()
        request = self._client.build_request("POST", self._origin + path, content=body, headers=self._headers)
        request.headers.pop("cookie", None)
        reply = await self._client.send(request, stream=True)
        try:
            return await _document(reply, 65536 if path == _ACCESS_PATH else MAX_RESPONSE_BYTES)
        finally:
            await reply.aclose()

    async def validate(self, plan_ids, *, client_id=None):
        """Validate the complete exact set at one coherent pin or return no partial result."""
        identities = _plan_ids(plan_ids)
        if not identities:
            return None
        pin = None
        access_checks = []
        if client_id is not None:
            _access_client_id(client_id)
        try:
            async with asyncio.timeout(TIMEOUT_SECONDS):
                for offset in range(0, len(identities), MAX_CHUNK_IDS):
                    chunk = identities[offset : offset + MAX_CHUNK_IDS]
                    current_pin = _metadata(await self._read(_PATH, {"healthporta_plan_ids": chunk}), chunk)
                    if client_id is not None:
                        access_checks.append(
                            _access(
                                await self._read(_ACCESS_PATH, {"client_id": client_id, "healthporta_plan_ids": chunk}),
                                client_id,
                                chunk,
                            )
                        )
                    if pin is not None and current_pin != pin:
                        raise _invalid()
                    pin = current_pin
            return RegistryBenefitPlanMetadataSnapshot(*pin, identities, client_id, tuple(access_checks))
        except RegistryBenefitPlanSelectionInvalid, RegistryBenefitPlanAccessDenied:
            raise
        except (
            httpx.HTTPError,
            OSError,
            TimeoutError,
            RuntimeError,
            ValueError,
            TypeError,
            UnicodeError,
            RecursionError,
        ):
            failure = RegistryBenefitPlanMetadataUnavailable()
        finally:
            self._client.cookies.clear()
        raise failure


def collect_benefit_plan_ids(assertions):
    """Collect only explicit choices; native assertion validation remains required."""
    identities = set()
    for assertion in assertions:
        selected = assertion.get("benefit_plan_ids", [])
        if type(selected) is not list or len(selected) > MAX_CHUNK_IDS:
            raise RegistryBenefitPlanSelectionInvalid()
        identities.update(_plan_ids(selected))
        if len(identities) > MAX_PLAN_IDS:
            raise RegistryBenefitPlanSelectionInvalid()
    return tuple(sorted(identities))


async def validate_benefit_plan_ids(plan_ids, client, actor):
    """Return server-only audit context after the complete trusted read succeeds."""
    identities = _plan_ids(plan_ids)
    if not identities:
        return {}
    from process.registry_record_store import RegistryActor, RegistryAddressUnavailable, _validated_actor

    if not isinstance(actor, RegistryActor):
        raise ValueError("registry_actor_invalid")
    _validated_actor(actor)
    client_id = actor.client_id if actor.kind == "client_owner" else None

    if type(client) is not RegistryBenefitPlanMetadataClient:
        raise RegistryAddressUnavailable("registry_benefit_plan_metadata_unavailable")
    try:
        snapshot = await client.validate(identities, client_id=client_id)
    except RegistryBenefitPlanMetadataUnavailable:
        raise RegistryAddressUnavailable("registry_benefit_plan_metadata_unavailable") from None
    context_by_name = {
        "benefit_plan_metadata": {
            "generation_id": snapshot.generation_id,
            "head_fence": snapshot.head_fence,
            "receipt_digest": snapshot.receipt_digest,
            "selected_ids_sha256": hashlib.sha256(json.dumps(identities, separators=(",", ":")).encode()).hexdigest(),
        }
    }

    if snapshot.client_id is not None:
        context_by_name["benefit_plan_metadata"]["tenant_access"] = {
            "client_id": snapshot.client_id,
            "checked_at": list(snapshot.access_checked_at),
        }
    return context_by_name
