# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Hospital source-download compatibility policy."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from process.control_cancel import ImportCancelledError
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.domain import PTG2RawArtifact

_RUNTIME_USER_AGENT = "Python/3.12 aiohttp/3.11"


async def download_hospital_source(
    download: Callable[..., Awaitable[PTG2RawArtifact]],
    url: str,
    store: PTG2ArtifactStore,
    max_bytes: int,
    user_agent: str,
) -> PTG2RawArtifact:
    """Retry an exact pre-body 403 with the approved source agents."""

    first_error: Exception | None = None
    user_agent_overrides = (user_agent, None, _RUNTIME_USER_AGENT)
    for index, user_agent_override in enumerate(user_agent_overrides):
        try:
            return await download(
                url, store=store, reuse_raw_artifacts=False,
                max_bytes=max_bytes, keep_partial_artifacts=False,
                **({"user_agent": user_agent_override} if user_agent_override else {}),
            )
        except (ImportCancelledError, asyncio.CancelledError):
            raise
        except Exception as exc:
            if first_error is None:
                first_error = exc
            if not (
                index + 1 < len(user_agent_overrides)
                and getattr(exc, "status", None) == 403
                and getattr(exc, "_ptg2_response_body_started", None) is False
            ):
                break
    assert first_error is not None
    raise first_error
