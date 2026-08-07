# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for the source-local projection binding seal."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_tax_identity_source_preflight as preflight
from process.ptg_parts import ptg2_tax_identity_source_projection as projection
from process.ptg_parts import ptg2_tax_identity_source_stage as source_stage
from tests.test_ptg2_tax_identity_source_preflight_postgres import (
    _prepared_projection_database,
)
from tests.test_ptg2_tax_identity_source_projection_postgres import (
    _fresh_projection,
)


async def _assert_exact_binding_seal(session, prepared, monkeypatch) -> None:
    staged = await source_stage.stage_tax_identity_source_projection(
        session,
        prepared,
    )
    await preflight.validate_staged_tax_identity_source_projection(
        session,
        staged=staged,
        prepared=replace(prepared),
    )
    changed_binding = replace(
        prepared.bindings[0],
        artifact_sha256=b"x" * 32,
    )
    reconstructed = replace(
        prepared,
        bindings=(changed_binding, *prepared.bindings[1:]),
    )
    content_validator = AsyncMock(
        side_effect=AssertionError("content validation reached")
    )
    monkeypatch.setattr(
        preflight,
        "_validate_stage_content_digest",
        content_validator,
    )

    with pytest.raises(
        projection.TaxIdentitySourceProjectionError,
        match="ptg2_tax_identity_source_projection_invalid",
    ):
        await preflight.validate_staged_tax_identity_source_projection(
            session,
            staged=staged,
            prepared=reconstructed,
        )
    content_validator.assert_not_awaited()


@pytest.mark.asyncio
async def test_stage_seal_binds_every_persisted_source_binding(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove reconstructed metadata must retain the exact binding vector."""

    async with _prepared_projection_database(monkeypatch, tmp_path) as prepared_db:
        database, _schema_name, prepare_projection = prepared_db
        with _fresh_projection(prepare_projection) as prepared:
            async with database.transaction() as session:
                await _assert_exact_binding_seal(session, prepared, monkeypatch)
