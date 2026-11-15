# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compatibility entry points for isolated, managed code-set publication."""

from __future__ import annotations

import json

from process import code_sets_result_archive as codes
from process import reference_family_archive as native
from process import scoped_catalog_named_publication as named

CONTRACT = "code-sets-mutable-publication.v1"
METRIC = "code_sets_mutable_publication"
MAX_ROWS = 20_000
MAX_BYTES = 16 * 1024**2


def validate_mutable_attempt(database, ctx, schema, test_mode):
    """Keep the documented mode selector, but require the managed isolated path."""
    native._schema_name(schema)
    if ctx is None or test_mode:
        raise RuntimeError("code-set publication requires a full managed attempt")
    if database._transaction_binding() is not None or database._reader_binding() is not None:
        raise RuntimeError("code-set publication requires its own transaction")
    return named.attempt_from_context(ctx)


def _validate_payloads(payloads):
    if (
        not 0 < len(payloads) <= MAX_ROWS
        or len(json.dumps(payloads, default=str).encode()) > MAX_BYTES
        or {(row.get("source"), row.get("code_system")) for row in payloads} != set(codes.SOURCES)
    ):
        raise RuntimeError("code-set input scope or bound differs")


async def capture_mutable_code_sets(database, ctx, *, schema, test_mode=False):
    """Keep the legacy mode selector while capturing the isolated named publication path."""
    validate_mutable_attempt(database, ctx, schema, test_mode)
    expected = await named.capture_expected(database, ctx, importer="code-sets", schema=schema)
    return expected, ctx.get("context", {}).get("_control_committed_result")


async def publish_mutable_code_sets(database, ctx, *, schema, payloads, metrics, expected=None, test_mode=False):
    """Both mode names prepare isolated input; only the protected continuation publishes."""
    validate_mutable_attempt(database, ctx, schema, test_mode)
    _validate_payloads(payloads)
    if expected != ctx.get("context", {}).get(named._CONTEXT_EXPECTED):
        raise RuntimeError("code-set acquisition generation changed")
    return await named.prepare(
        database,
        ctx,
        importer="code-sets",
        schema=schema,
        payloads={"code_catalog": payloads},
        options={"include_relationships": False, "test_mode": False},
        metrics=metrics,
    )
