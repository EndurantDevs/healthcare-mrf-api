# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed managed profile inputs and authorization-bound pagination identity."""

import hashlib
import json
import re
from dataclasses import asdict

from sanic.exceptions import InvalidUsage

from process.network_serving_read import PinnedNetworkServingManifest
from process.provider_directory_profile import is_valid_npi

_FIELDS = {"npi", "include_evidence", "include_sensitive", "categories", "category", "limit", "offset", "generation_id"}


def parse_managed_profile_query(document):
    """Reuse public profile semantics after admitting normalized string fields."""
    from api.endpoint.npi import _provider_profile_query_from_args

    if (
        type(document) is not dict
        or not {"npi"} <= set(document) <= _FIELDS
        or any(type(field_value) is not str or len(field_value.encode()) > 2048 for field_value in document.values())
        or not is_valid_npi(document["npi"])
    ):
        raise ValueError("managed_profile_query_invalid")
    for name in ("include_evidence", "include_sensitive"):
        if name in document and document[name] not in {"true", "false"}:
            raise ValueError("managed_profile_query_invalid")
    for name in ("limit", "offset"):
        if name in document and re.fullmatch(r"0|[1-9][0-9]{0,6}", document[name]) is None:
            raise ValueError("managed_profile_query_invalid")
    if "generation_id" in document and re.fullmatch(r"[0-9a-f]{64}", document["generation_id"]) is None:
        raise ValueError("managed_profile_query_invalid")
    for name in ("category", "categories"):
        if name in document:
            categories = document[name].split(",")
            if not all(category and category == category.strip() for category in categories) or len(categories) != len(
                set(categories)
            ):
                raise ValueError("managed_profile_query_invalid")
    try:
        query = _provider_profile_query_from_args(
            {name: field_value for name, field_value in document.items() if name != "npi"}
        )
    except InvalidUsage as error:
        raise ValueError("managed_profile_query_invalid") from error
    return document["npi"], query


def bind_managed_profile_generation(profile, manifest, access_scope_sha256):
    """Bind composed source identity to already verified policy and network pins."""
    if (
        type(profile) is not dict
        or type(profile.get("generation_id")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", profile["generation_id"]) is None
        or type(manifest) is not PinnedNetworkServingManifest
        or type(access_scope_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", access_scope_sha256) is None
    ):
        raise ValueError("managed_profile_generation_invalid")
    identity_by_field = {
        "contract": "registry-provider-profile/v1",
        "profile_generation": profile["generation_id"],
        "access_scope_sha256": access_scope_sha256,
        "network_manifest": asdict(manifest),
    }
    encoded = json.dumps(identity_by_field, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return profile | {"generation_id": hashlib.sha256(encoded).hexdigest()}
