# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Narrow redirect handling for exact formulary artifact downloads."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity
from process.uhc_provider_file_catalog_artifacts import MAX_REDIRECTS
from process.uhc_provider_file_catalog_artifacts import REDIRECT_STATUSES
from process.uhc_provider_file_catalog_contract import UHCFileCatalogError
from process.uhc_provider_file_catalog_contract import trusted_public_https_url


_EXTERNAL_SOURCE_HOST = "legacy.providerlookuponline.com"
_EXACT_HTTPS_MIRROR_HOST = "www.providerlookuponline.com"


def _exact_https_mirror(
    response_url: str,
    location: str,
    identity: SourceArtifactIdentity,
) -> str | None:
    """Rewrite one reviewed HTTP sibling signal without requesting it."""

    try:
        source_url_parts = urlsplit(response_url)
        redirect = urlsplit(location)
    except ValueError:
        return None
    if (
        source_url_parts.scheme != "https"
        or source_url_parts.netloc != _EXTERNAL_SOURCE_HOST
        or not source_url_parts.path.startswith("/")
        or redirect.scheme != "http"
        or redirect.netloc != _EXACT_HTTPS_MIRROR_HOST
        or not redirect.path.startswith("/")
        or redirect.query
        or redirect.fragment
    ):
        return None
    expected_file_name = identity.file_name
    if unquote(source_url_parts.path.rsplit("/", 1)[-1]) != expected_file_name:
        return None
    # The 302 path is a landing route, not artifact identity; retain the
    # catalog-bound source path and never request the redirect target.
    mirror_url = urlunsplit(
        ("https", _EXACT_HTTPS_MIRROR_HOST, source_url_parts.path, "", "")
    )
    mirror = urlsplit(mirror_url)
    if (
        mirror.scheme != "https"
        or mirror.netloc != _EXACT_HTTPS_MIRROR_HOST
        or mirror.path != source_url_parts.path
        or unquote(mirror.path.rsplit("/", 1)[-1]) != expected_file_name
        or mirror.query
        or mirror.fragment
        or urlunsplit(mirror) != mirror_url
    ):
        return None
    return mirror_url


@asynccontextmanager
async def validated_drug_get(
    session: Any,
    source_url: str,
    identity: SourceArtifactIdentity,
):
    """Yield one direct response after bounded exact redirect validation."""

    request_url = source_url
    redirect_count = 0
    has_requested_mirror = False
    while True:
        async with session.get(
            request_url,
            allow_redirects=False,
            headers={"Accept-Encoding": "identity"},
        ) as response:
            response_url = str(response.url)
            if not has_requested_mirror:
                response_url = trusted_public_https_url(response_url)
            if response_url != request_url:
                raise UHCFileCatalogError("UHC drug response URL is invalid")
            if response.status not in REDIRECT_STATUSES:
                yield response
                return
            raw_location = str(response.headers.get("Location") or "")
            location = raw_location.strip()
            if (
                not location
                or redirect_count >= MAX_REDIRECTS
                or has_requested_mirror
            ):
                raise UHCFileCatalogError("UHC drug redirect is invalid")
            mirror_url = (
                _exact_https_mirror(response_url, location, identity)
                if response.status == 302
                and redirect_count == 0
                and raw_location == location
                and location.isprintable()
                else None
            )
            if mirror_url is None:
                request_url = trusted_public_https_url(
                    urljoin(response_url, location)
                )
            else:
                request_url = mirror_url
                has_requested_mirror = True
            redirect_count += 1
