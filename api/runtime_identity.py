"""Per-process and exact-image identity for direct PTG V4 canary evidence."""

from __future__ import annotations

import hashlib
import os
import secrets
from datetime import datetime, timezone
from typing import Any


PROCESS_IDENTITY_HEADER = "X-HealthPorta-Process-Identity"
PROCESS_STARTED_AT_HEADER = "X-HealthPorta-Process-Started-At"
IMAGE_IDENTITY_HEADER = "X-HealthPorta-Image-Identity"
RUNTIME_IMAGE_ENV = "HLTHPRT_RUNTIME_IMAGE_IDENTITY"
RUNTIME_IDENTITY_HEADERS_ENV = "HLTHPRT_RUNTIME_IDENTITY_HEADERS_ENABLED"
_PROCESS_STARTED_AT = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
_PROCESS_IDENTITY = hashlib.sha256(
    (
        f"{os.getpid()}:{os.getenv('HOSTNAME', '')}:"
        f"{_PROCESS_STARTED_AT}:{secrets.token_hex(32)}"
    ).encode("utf-8")
).hexdigest()


def runtime_identity() -> dict[str, str]:
    """Return immutable identity fields for this exact operating-system process."""

    return {
        "process_identity": _PROCESS_IDENTITY,
        "process_started_at": _PROCESS_STARTED_AT,
        "image_identity": str(os.getenv(RUNTIME_IMAGE_ENV) or "").strip(),
    }


def add_runtime_identity_headers(_request: Any, response: Any) -> None:
    """Attach the process/image tuple used to bind direct-pod canary evidence."""

    if (
        str(os.getenv(RUNTIME_IDENTITY_HEADERS_ENV) or "").strip().lower()
        not in {"1", "true", "yes", "on"}
        or response is None
        or not hasattr(response, "headers")
    ):
        return
    identity_by_field = runtime_identity()
    response.headers[PROCESS_IDENTITY_HEADER] = identity_by_field[
        "process_identity"
    ]
    response.headers[PROCESS_STARTED_AT_HEADER] = identity_by_field[
        "process_started_at"
    ]
    response.headers[IMAGE_IDENTITY_HEADER] = identity_by_field[
        "image_identity"
    ]
