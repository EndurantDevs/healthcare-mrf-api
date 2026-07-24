# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded database stage identities for reference-data imports."""

from __future__ import annotations

import hashlib
import secrets
from typing import Any, Iterable


def build_reference_stage_suffix(
    importer_prefix: str,
    import_suffix: str,
    run_id: str | None,
) -> str:
    """Return a lowercase stage suffix unique to one importer execution."""
    execution_identity = run_id or secrets.token_hex(8)
    readable_import_id = "".join(
        character
        for character in import_suffix.lower()
        if character.isalnum()
    )[:8] or "import"
    identity_payload = (
        f"{importer_prefix}\0{import_suffix}\0{execution_identity}"
    ).encode("utf-8")
    identity_digest = hashlib.sha256(identity_payload).hexdigest()[:12]
    return f"{importer_prefix}_{readable_import_id}_{identity_digest}"


async def _drop_stage_tables(
    database: Any,
    schema: str,
    stage_classes: Iterable[Any],
) -> None:
    for stage_class in stage_classes:
        await database.status(
            f"DROP TABLE IF EXISTS {schema}.{stage_class.__tablename__};"
        )
