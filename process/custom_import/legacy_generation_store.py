# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded protected membership writes in the legacy caller's transaction."""

from __future__ import annotations

from process.custom_import.execution import lease_token_sha256
from process.custom_import.materialization_store import _flush_pending, _page, _runner_window
from process.custom_import.runner_types import CandidateRunnerError, PublishedCandidateFamily

_PAGE_ROWS = 16_384
_PAGE_BYTES = 512 * 1024
_PAGE_OVERHEAD_BYTES = 1024
_MEMBERSHIP_ARRAY_HEADER_BYTES = 40
# Each of two binary bigint arrays stores an int32 length and an int64 value.
_MEMBERSHIP_ROW_BYTES = 24


def _generation_arguments(request, generation, window):
    """Validate the exact producer tuple before any caller-owned flush or write."""

    names = (
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
    )
    generation_ids = tuple(getattr(generation, name) for name in names)
    if any(type(identity_id) is not int or not 0 < identity_id < 2**63 for identity_id in generation_ids):
        raise CandidateRunnerError("legacy generation identity is malformed")
    token = generation.producing_token_sha256
    if not isinstance(token, (bytes, bytearray, memoryview)) or len(token) != 32:
        raise CandidateRunnerError("legacy generation producer token is malformed")
    token = bytes(token)
    authority = generation_ids[1:] + (token,)
    if (
        authority[:4]
        != (request.dataset_id, request.definition_revision_id, request.schema_revision_id, request.execution_id)
        or token != lease_token_sha256(request.lease_token)
        or (window is not None and authority != window.authority)
    ):
        raise CandidateRunnerError("legacy generation authority identity differs")
    return tuple(("bigint", identity_id) for identity_id in generation_ids) + (("bytea", token),)


async def persist_generation_families(session, request, generation, published_families):
    """Append ordered membership pages, preserving duplicates for native rejection.

    SQL derives complete producer authority from the exact generation and
    refuses bounded-build owners. A real runner window is an extra expectation;
    standalone callers retain their own transaction and need no fabricated one.
    """

    window = await _runner_window(session)
    generation_arguments = _generation_arguments(request, generation, window)
    for family in published_families:
        if not isinstance(family, PublishedCandidateFamily) or any(
            type(member_id) is not int or not 0 < member_id < 2**63
            for member_id in (family.root_record_id, family.family_revision_id)
        ):
            raise CandidateRunnerError("legacy generation membership identity is malformed")
    page_limit = min(
        _PAGE_ROWS,
        (_PAGE_BYTES - _PAGE_OVERHEAD_BYTES - _MEMBERSHIP_ARRAY_HEADER_BYTES) // _MEMBERSHIP_ROW_BYTES,
    )
    if page_limit <= 0:
        raise CandidateRunnerError("legacy generation membership page budget is too small")
    budget_scope = (
        ("bigint", generation.dataset_id),
        ("bigint[]", ()),
        ("bigint[]", ()),
        ("bigint", generation.generation_id),
    )
    await _flush_pending(session, window)
    for start in range(0, len(published_families), page_limit):
        page = published_families[start : start + page_limit]
        arguments = generation_arguments + (
            ("bigint[]", tuple(family.root_record_id for family in page)),
            ("bigint[]", tuple(family.family_revision_id for family in page)),
        )
        count = await _page(
            session, "persist_custom_import_legacy_generation_family_set", arguments, budget_scope, window
        )
        if type(count) is not int or count != len(page):
            raise CandidateRunnerError("legacy generation membership persisted count differs")
    await _page(
        session, "check_custom_import_generation_materialization_authority", generation_arguments, budget_scope, window
    )
