# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed Sanic boundary for exact billing-identity pricing search."""

from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
import logging
import os
from typing import Any

import orjson
from sanic import response
from sqlalchemy import text

from api.billing_search_access_contract import BILLING_SEARCH_CACHE_CONTROL
from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
)
from api.billing_search_cursor_keys import (
    BILLING_SEARCH_CURSOR_KEYRING_ENV,
    load_billing_search_cursor_keyring,
)
from api.billing_search_endpoint_access import (
    BillingSearchEndpointAccessError,
    authorize_billing_search_endpoint,
)
from api.billing_search_response import shape_billing_search_response
from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_PATH
from api.billing_search_transport_keys import (
    BILLING_SEARCH_TRANSPORT_KEYRING_ENV,
    load_billing_search_transport_keyring,
)
from api.ptg2_billing_search_contract import (
    BillingSearchResourceNotFoundError,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_search_service import search_exact_billing_provider_page

logger = logging.getLogger(__name__)

_MAX_SUCCESS_BODY_BYTES = 256 * 1024
_READ_TRANSACTION_SQL = text(
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
)
_ERRORS_BY_STATUS = {
    400: (
        "billing_search_cursor_invalid",
        "Invalid billing search cursor.",
    ),
    404: (
        "resource_not_found",
        "Resource not found.",
    ),
    409: (
        "billing_search_cursor_generation_expired",
        "Billing search cursor generation is no longer available.",
    ),
    503: (
        "billing_search_serving_unavailable",
        "Billing search is temporarily unavailable.",
    ),
}


class _BillingSearchResponseFailure(RuntimeError):
    """Internal marker after a response-shaping failure was safely logged."""


@lru_cache(maxsize=1)
def _transport_keyring_for_document(document: str | None):
    environment_map = (
        {} if document is None else {BILLING_SEARCH_TRANSPORT_KEYRING_ENV: document}
    )
    return load_billing_search_transport_keyring(environment_map)


@lru_cache(maxsize=1)
def _cursor_keyring_for_document(document: str | None):
    environment_map = (
        {} if document is None else {BILLING_SEARCH_CURSOR_KEYRING_ENV: document}
    )
    return load_billing_search_cursor_keyring(environment_map)


def _transport_keyring():
    return _transport_keyring_for_document(
        os.environ.get(BILLING_SEARCH_TRANSPORT_KEYRING_ENV)
    )


def _cursor_keyring():
    return _cursor_keyring_for_document(
        os.environ.get(BILLING_SEARCH_CURSOR_KEYRING_ENV)
    )


def _trusted_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_bytes_response(body: bytes, *, status: int):
    return response.raw(
        body,
        status=status,
        headers={"Cache-Control": BILLING_SEARCH_CACHE_CONTROL},
        content_type="application/json",
    )


def _error_response(status: int):
    code, message = _ERRORS_BY_STATUS[status]
    return _json_bytes_response(
        orjson.dumps({"error": {"code": code, "message": message}}),
        status=status,
    )


def _log_failure(failure: BaseException) -> None:
    logger.warning(
        "Billing search request failed",
        extra={"billing_search_failure_class": type(failure).__name__},
    )


async def _encoded_service_response(
    session: Any,
    *,
    access: Any,
    cursor_keyring: Any,
    trusted_now: str,
) -> bytes:
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        service_result = await search_exact_billing_provider_page(
            session,
            access=access,
            cursor_keyring=cursor_keyring,
            trusted_now=trusted_now,
        )
        try:
            response_payload_by_field = shape_billing_search_response(
                access,
                service_result,
                cursor_keyring=cursor_keyring,
                trusted_now=trusted_now,
            )
            encoded_body = orjson.dumps(response_payload_by_field)
            if len(encoded_body) > _MAX_SUCCESS_BODY_BYTES:
                raise BillingSearchServingUnavailableError(
                    "billing_search_serving_generation_unavailable"
                )
            return encoded_body
        except Exception as exc:
            _log_failure(exc)
            raise _BillingSearchResponseFailure from None


def _failure_status(failure: Exception) -> int:
    if isinstance(failure, BillingSearchCursorGenerationExpired):
        return 409
    if isinstance(failure, BillingSearchCursorError):
        return 400
    if isinstance(
        failure,
        (BillingSearchEndpointAccessError, BillingSearchResourceNotFoundError),
    ):
        return 404
    return 503


def _failure_response(failure: Exception):
    if not isinstance(failure, _BillingSearchResponseFailure):
        _log_failure(failure)
    return _error_response(_failure_status(failure))


async def serve_billing_search_get(request: Any, session: Any):
    """Serve the canonical authenticated GET mode without legacy fallbacks."""

    if (
        getattr(request, "method", None) != "GET"
        or getattr(request, "path", None) != BILLING_SEARCH_TRANSPORT_PATH
    ):
        return _error_response(404)

    trusted_now = _trusted_now()
    try:
        transport_keyring = _transport_keyring()
        cursor_keyring = _cursor_keyring()
        access = authorize_billing_search_endpoint(
            request.args,
            request.headers,
            method=request.method,
            path=request.path,
            trusted_now=trusted_now,
            keyring=transport_keyring,
        )
        encoded_body = await _encoded_service_response(
            session,
            access=access,
            cursor_keyring=cursor_keyring,
            trusted_now=trusted_now,
        )
    except Exception as failure:
        return _failure_response(failure)

    return _json_bytes_response(encoded_body, status=200)


__all__ = ["serve_billing_search_get"]
