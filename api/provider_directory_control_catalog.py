# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated Provider Directory catalog projection for control clients."""

from __future__ import annotations

import logging
from typing import Any

from api.provider_directory_source_outcomes import (
    enrich_provider_directory_source_catalog,
)
from api.provider_directory_sources import provider_directory_source_catalog
from process.provider_directory_profile_selection import (
    current_profile_selection_request,
)


async def provider_directory_control_catalog() -> dict[str, Any]:
    """Return optional outcomes and the exact current Profile request."""

    static_map = provider_directory_source_catalog()
    try:
        selection_payload = await current_profile_selection_request(static_map)
        catalog_map = await enrich_provider_directory_source_catalog(static_map)
    except Exception:
        logging.getLogger(__name__).warning(
            "Provider Directory enrichment failed; returning static catalog",
            exc_info=True,
        )
        return static_map
    if selection_payload is not None:
        catalog_map = {
            **catalog_map,
            "profile_selection_request": selection_payload,
        }
    return catalog_map
