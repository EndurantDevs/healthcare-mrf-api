# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated Provider Directory catalog projection for control clients."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from api.provider_directory_source_outcomes import (
    enrich_provider_directory_source_catalog,
)
from api.provider_directory_sources import provider_directory_source_catalog
from process.provider_directory_profile_selection import (
    current_profile_selection_request,
)

_OUTCOME_ENRICHMENT_TIMEOUT_SECONDS = 5.0


async def provider_directory_control_catalog() -> dict[str, Any]:
    """Return optional outcomes and the exact current Profile request."""

    static_map = provider_directory_source_catalog()
    selection_payload = None
    try:
        selection_payload = await current_profile_selection_request(static_map)
    except Exception:
        logging.getLogger(__name__).warning(
            "Provider Directory selection projection failed",
            exc_info=True,
        )
    try:
        async with asyncio.timeout(_OUTCOME_ENRICHMENT_TIMEOUT_SECONDS):
            catalog_map = await enrich_provider_directory_source_catalog(static_map)
    except Exception:
        logging.getLogger(__name__).warning(
            "Provider Directory outcome enrichment failed; returning static catalog",
            exc_info=True,
        )
        catalog_map = static_map
    if selection_payload is not None:
        catalog_map = {
            **catalog_map,
            "profile_selection_request": selection_payload,
        }
    return catalog_map
