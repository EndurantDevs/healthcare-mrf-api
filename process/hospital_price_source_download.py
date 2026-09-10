# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Hospital source-download compatibility policy."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

from process.control_cancel import ImportCancelledError
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.domain import PTG2RawArtifact
from process.ptg_parts.source_download import download_raw_artifact_via_proxy
from process.ptg_parts.source_download import is_prebody_connect_or_timeout
from process.ptg_parts.source_download import validated_http_proxy_url

_RUNTIME_USER_AGENT = "Python/3.12 aiohttp/3.11"
_AVERA_BROWSER_PROFILE = "chrome136"
_HOSPITAL_PROXY_ENV = "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY"
_PROXY_HOSTS_ENV = "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS"
_PROXY_MAX_BYTES = 512 * 1024**2


def _browser_profile(url: str) -> str | None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.avera.org"
        or (parsed.port or 443) != 443
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    if parsed.path == "/cms-hpt.txt" or parsed.path.startswith("/app/files/public/"):
        return _AVERA_BROWSER_PROFILE
    return None


def _configured_proxy(url: str) -> str | None:
    value = os.getenv(_HOSPITAL_PROXY_ENV, "").strip()
    if not value:
        return None
    proxy_url = validated_http_proxy_url(value)
    allowed_hosts = {
        host.strip().lower()
        for host in os.getenv(_PROXY_HOSTS_ENV, "").split(",")
        if host.strip()
    }
    if not allowed_hosts or any(
        urlsplit(f"https://{host}").hostname != host for host in allowed_hosts
    ):
        raise RuntimeError(f"{_PROXY_HOSTS_ENV} must contain exact hostnames")
    source = urlsplit(url)
    return (
        proxy_url
        if source.scheme == "https"
        and source.hostname
        and source.hostname.lower() in allowed_hosts
        else None
    )


async def _download_with_user_agents(
    download: Callable[..., Awaitable[PTG2RawArtifact]],
    url: str,
    options: dict[str, object],
    user_agent: str,
) -> PTG2RawArtifact:
    first_error: Exception | None = None
    for index, override in enumerate((user_agent, None, _RUNTIME_USER_AGENT)):
        try:
            return await download(
                url,
                **options,
                **({"user_agent": override} if override else {}),
            )
        except (ImportCancelledError, asyncio.CancelledError):
            raise
        except Exception as exc:
            if first_error is None:
                first_error = exc
            setattr(first_error, "_hospital_terminal_error", exc)
            if not (
                index < 2
                and getattr(exc, "status", None) == 403
                and getattr(exc, "_ptg2_response_body_started", None) is False
            ):
                break
    assert first_error is not None
    raise first_error


def _is_proxyable_failure(error: Exception) -> bool:
    terminal_error = getattr(error, "_hospital_terminal_error", error)
    return (
        getattr(terminal_error, "_ptg2_response_body_started", None) is False
        and (
            getattr(terminal_error, "status", None) == 403
            or is_prebody_connect_or_timeout(terminal_error)
        )
    )


async def download_hospital_source(
    download: Callable[..., Awaitable[PTG2RawArtifact]],
    url: str,
    store: PTG2ArtifactStore,
    max_bytes: int,
    user_agent: str,
    *,
    exact_get_evidence: bool = False,
) -> PTG2RawArtifact:
    """Retry an exact pre-body 403 with the approved source agents."""

    download_option_map = {
        "store": store,
        "reuse_raw_artifacts": False,
        "max_bytes": max_bytes,
        "keep_partial_artifacts": False,
        "exact_get_evidence": exact_get_evidence,
    }
    browser_profile = _browser_profile(url)
    proxy_url = _configured_proxy(url)
    try:
        if browser_profile:
            return await download(
                url,
                browser_profile=browser_profile,
                **download_option_map,
            )
        return await _download_with_user_agents(
            download, url, download_option_map, user_agent
        )
    except (ImportCancelledError, asyncio.CancelledError):
        raise
    except Exception as exc:
        if not proxy_url or not _is_proxyable_failure(exc):
            raise
        direct_error = exc
    # ponytail: proxy transfers restart; raise this cap after range resume exists.
    try:
        return await download_raw_artifact_via_proxy(
            url,
            proxy_url=proxy_url,
            store=store,
            max_bytes=min(max_bytes, _PROXY_MAX_BYTES),
            exact_get_evidence=exact_get_evidence,
            user_agent=None if browser_profile else user_agent,
            browser_profile=browser_profile,
        )
    except (ImportCancelledError, asyncio.CancelledError):
        raise
    except Exception as proxy_error:
        if (
            getattr(direct_error, "status", None) == 403
            and is_prebody_connect_or_timeout(proxy_error)
        ):
            raise direct_error
        raise
