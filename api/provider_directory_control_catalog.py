# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated Provider Directory catalog projection for control clients."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from api.provider_directory_cms_candidate_catalog import cms_serving_candidate
from api.provider_directory_source_outcomes import (
    enrich_provider_directory_source_catalog,
)
from api.provider_directory_sources import provider_directory_source_catalog
from process.provider_directory_profile_selection import (
    current_profile_selection_request,
)

_OUTCOME_ENRICHMENT_TIMEOUT_SECONDS = 5.0
_SELECTION_PROJECTION_TIMEOUT_SECONDS = 5.0
_CMS_CANDIDATE_TIMEOUT_SECONDS = 5.0


class _OptionalCatalogProjectionError(RuntimeError):
    """An owning transaction must roll back before static catalog fallback."""


async def provider_directory_control_catalog(editions=(), *, allow_optional_fallback: bool = True) -> dict[str, Any]:
    """Project optional fields; transaction owners defer fallback until rollback."""

    static_map = provider_directory_source_catalog()
    selection_payload = None
    try:
        async with asyncio.timeout(_SELECTION_PROJECTION_TIMEOUT_SECONDS):
            selection_payload = await current_profile_selection_request(static_map)
    except Exception:
        logging.getLogger(__name__).warning(
            "Provider Directory selection projection failed",
            exc_info=True,
        )
        if not allow_optional_fallback:
            raise _OptionalCatalogProjectionError("selection projection failed") from None
    try:
        async with asyncio.timeout(_OUTCOME_ENRICHMENT_TIMEOUT_SECONDS):
            catalog_map = await enrich_provider_directory_source_catalog(static_map, editions=editions)
    except Exception:
        logging.getLogger(__name__).warning(
            "Provider Directory outcome enrichment failed",
            exc_info=True,
        )
        if not allow_optional_fallback:
            raise _OptionalCatalogProjectionError("outcome enrichment failed") from None
        catalog_map = static_map
    if selection_payload is not None:
        catalog_map = {
            **catalog_map,
            "profile_selection_request": selection_payload,
        }
    try:
        async with asyncio.timeout(_CMS_CANDIDATE_TIMEOUT_SECONDS):
            candidate = await cms_serving_candidate(static_map)
        if candidate is not None:
            catalog_map = {**catalog_map, "cms_serving_candidate": candidate}
    except Exception:
        logging.getLogger(__name__).warning("CMS candidate projection failed", exc_info=True)
        if not allow_optional_fallback:
            raise _OptionalCatalogProjectionError("CMS candidate projection failed") from None
    return catalog_map
