# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stable, route-safe identities for non-annual FHIR formularies."""

from __future__ import annotations

import base64
import hashlib
import urllib.parse


FHIR_PUBLIC_ID_PREFIX = "fhir_"
FHIR_PUBLIC_ID_BITS = 130
FHIR_PUBLIC_ID_CHARS = FHIR_PUBLIC_ID_BITS // 5


def canonical_fhir_base(base_url: str) -> str:
    """Return a normalized HTTPS FHIR base without credentials or suffix data."""

    parsed = urllib.parse.urlsplit(str(base_url or "").strip())
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise ValueError("FHIR base must be an absolute HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("FHIR base must not contain user information")
    if parsed.query or parsed.fragment:
        raise ValueError("FHIR base must not contain query or fragment data")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("FHIR base has an invalid port") from exc
    if port not in (None, 443):
        raise ValueError("FHIR base must use the HTTPS default port")
    path = "/" + "/".join(part for part in parsed.path.split("/") if part)
    return urllib.parse.urlunsplit(
        ("https", parsed.hostname.lower(), path.rstrip("/"), "", "")
    )


def canonical_list_identity(base_url: str, list_id: str) -> str:
    """Return the unhashed canonical identity for one upstream List."""

    clean_list_id = str(list_id or "").strip()
    if (
        not clean_list_id
        or clean_list_id in {".", ".."}
        or "/" in clean_list_id
        or "?" in clean_list_id
        or "#" in clean_list_id
    ):
        raise ValueError("List id must be one non-empty FHIR logical id")
    return f"{canonical_fhir_base(base_url)}/List/{clean_list_id}"


def public_formulary_id(base_url: str, list_id: str) -> str:
    """Return ``fhir_`` plus the leading 130 SHA-256 bits in base32."""

    identity = canonical_list_identity(base_url, list_id).encode("utf-8")
    encoded = base64.b32encode(hashlib.sha256(identity).digest()).decode("ascii")
    return FHIR_PUBLIC_ID_PREFIX + encoded.lower()[:FHIR_PUBLIC_ID_CHARS]
