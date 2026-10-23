# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete dense forward-code scans using the canonical sidecar reader guards."""

from typing import Any


async def collect_dense_code_prefix(
    session: Any,
    code_key: int,
    *,
    limit: int,
    descending: bool = False,
    **read_options: Any,
) -> list[tuple[int, int, int]]:
    """Return dense occurrences while validating every complete forward fragment."""
    from api import ptg2_db_sidecars as sidecars

    if isinstance(limit, bool) or int(limit) <= 0:
        raise ValueError("PTG2 serving binary prefix limit must be positive")
    normalized_limit = int(limit)
    options = sidecars._ForwardLookupOptions(**read_options)
    normalized_code_key = sidecars._normalized_code_key(code_key)
    shard_keys_by_code, normalized_provider_filter, require_all = await sidecars._forward_shard_keys_for_read(
        session,
        options,
        (normalized_code_key,),
    )
    block_keys = shard_keys_by_code[normalized_code_key]
    if not block_keys:
        return []
    collector = sidecars._forward_prefix_collector(
        options,
        normalized_code_key,
        normalized_provider_filter,
        block_keys,
        bool(descending),
        normalized_limit,
    )
    await sidecars._collect_forward_prefix(
        session,
        options,
        block_keys,
        require_all,
        collector,
    )
    return collector.selected_keys()


async def materialize_code_prefix_rows(
    session: Any,
    code_key: int,
    *,
    limit: int,
    descending: bool = False,
    **read_options: Any,
) -> tuple:
    """Return the exact best bounded prefix while validating the full code block."""
    from api import ptg2_db_sidecars as sidecars

    decoded_keys = await sidecars.lookup_code_prefix_occurrences_from_db(
        session, code_key, limit=limit, descending=descending, **read_options
    )
    if not decoded_keys:
        return ()
    return await sidecars._materialize_forward_prefix(
        session,
        sidecars._ForwardLookupOptions(**read_options),
        sidecars._normalized_code_key(code_key),
        decoded_keys,
        bool(descending),
    )
