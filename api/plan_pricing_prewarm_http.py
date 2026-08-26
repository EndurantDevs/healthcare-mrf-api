# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dedicated API Layer connection contract for plan-pricing prewarm."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit


PREWARM_API_BASE_URL_ENV = "HLTHPRT_PLAN_PRICING_PREWARM_API_BASE_URL"
PREWARM_API_TOKEN_ENV = "HP_API_" + "LAYER_PLAN_PRICING_PREWARM_TOKEN"
PREWARM_PATH = "/internal/v1/plan-pricing/prewarm"
_PREWARM_SERVICE_HOST = "api" + "-" + "layer"


@dataclass(frozen=True)
class PrewarmHttpConfig:
    base_url: str
    token: str = field(repr=False)
    verify_tls: bool = True

    @property
    def headers(self) -> dict[str, str]:
        """Return the dedicated API Layer authorization headers."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "User-Agent": "plan-pricing-prewarm/1.0",
        }


def _is_trusted_cluster_http(parsed_url: Any) -> bool:
    hostname = str(parsed_url.hostname or "").lower()
    return parsed_url.scheme == "http" and (
        hostname == _PREWARM_SERVICE_HOST
        or hostname.startswith(f"{_PREWARM_SERVICE_HOST}.")
        and (
            hostname.endswith(".svc")
            or hostname.endswith(".svc.cluster.local")
        )
    )


def prewarm_http_config() -> PrewarmHttpConfig:
    """Load one credential without reflecting it into receipts or errors."""

    base_url = str(os.getenv(PREWARM_API_BASE_URL_ENV) or "").strip().rstrip("/")
    token = str(os.getenv(PREWARM_API_TOKEN_ENV) or "").strip()
    if not base_url:
        raise ValueError(f"{PREWARM_API_BASE_URL_ENV} is required")
    if not token:
        raise ValueError(f"{PREWARM_API_TOKEN_ENV} is required")
    parsed_url = urlsplit(base_url)
    if (
        parsed_url.username
        or parsed_url.password
        or parsed_url.query
        or parsed_url.fragment
        or parsed_url.path not in {"", "/"}
    ):
        raise ValueError("plan-pricing prewarm API origin is invalid")
    if parsed_url.scheme == "https" and parsed_url.netloc:
        should_verify_tls = True
    elif _is_trusted_cluster_http(parsed_url):
        should_verify_tls = False
    else:
        raise ValueError(
            "plan-pricing prewarm requires verified HTTPS or API Layer cluster HTTP"
        )
    return PrewarmHttpConfig(
        base_url=base_url,
        token=token,
        verify_tls=should_verify_tls,
    )
