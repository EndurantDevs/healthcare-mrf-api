# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import importlib.util
import os
from pathlib import Path
import struct
from types import SimpleNamespace
from typing import Any, Iterable
from unittest.mock import AsyncMock

import pytest

from api.ptg2_code_filters import InferredProviderTaxonomyRule
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api import ptg2_v4_graph as v4_graph
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts import ptg2_v4_taxonomy_candidates as candidates
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.ptg2_v4_coverage_support import (
    _metadata,
    _summary,
    synthetic_adaptive_layout_decision,
)

from tests.ptg2_v4_taxonomy_candidate_test_support import (
    _PreparedCompilerInputSession,
    _PublicationSession,
    _Result,
    _ScriptedSession,
    _assert_candidate_load_rejected,
    _assert_direct_publication_contract,
    _compiler_rules,
    _load_candidate_projection,
    _noop_map_write_lock,
    _observe_projection_row,
    _projection_row,
    _publish_candidate_projection,
    _reader_row,
    _rules,
    _tampered_pattern_projection,
)

@pytest.mark.asyncio
async def test_prepared_compiler_input_reads_ten_rules_once_and_fsyncs(
    monkeypatch,
    tmp_path,
) -> None:
    """Prepare five retained and five bounded observe vectors in one snapshot."""

    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    session = _PreparedCompilerInputSession(
        common_count=1,
        observe_count=2,
    )
    artifact_path = tmp_path / "taxonomy-members.u32le"
    prepared = await candidates.prepare_v4_inferred_taxonomy_compiler_input(
        session,
        schema_name="mrf",
        npi_scope_stage_table="ptg2_v4_npi_scope_test",
        npi_scope_sha256="a" * 64,
        rules=_compiler_rules(),
        members_path=artifact_path,
    )

    assert prepared["contract"] == ("ptg2_v4_inferred_taxonomy_compiler_input_v1")
    assert prepared["npi_scope_sha256"] == "a" * 64
    assert len(prepared["rules"]) == 10
    assert sorted(rule["member_count"] for rule in prepared["rules"]) == (
        [1] * 5 + [2] * 5
    )
    assert prepared["members"]["byte_count"] == 60
    assert (
        prepared["members"]["sha256"]
        == hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    )
    assert os.stat(artifact_path).st_mode & 0o777 == 0o600
    assert "current_setting('transaction_isolation')" in session.calls[0][0]
    catalog_calls = session.calls[1:]
    assert len(catalog_calls) == 10
    assert all("ptg2_v4_npi_scope_test" in sql for sql, _ in catalog_calls)
    assert {parameters["candidate_limit"] for _sql, parameters in catalog_calls} == {2}
    expected_offset = 0
    for rule in prepared["rules"]:
        assert rule["member_offset_bytes"] == expected_offset
        assert rule["member_byte_count"] == rule["member_count"] * 4
        expected_offset += rule["member_byte_count"]
    assert expected_offset == prepared["members"]["byte_count"]


@pytest.mark.asyncio
async def test_prepared_compiler_input_does_not_replace_existing_artifact(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    artifact_path = tmp_path / "taxonomy-members.u32le"
    artifact_path.write_bytes(b"retained")
    with pytest.raises(FileExistsError):
        await candidates.prepare_v4_inferred_taxonomy_compiler_input(
            _PreparedCompilerInputSession(
                common_count=1,
                observe_count=2,
            ),
            schema_name="mrf",
            npi_scope_stage_table="ptg2_v4_npi_scope_test",
            npi_scope_sha256="a" * 64,
            rules=_compiler_rules(),
            members_path=artifact_path,
        )
    assert artifact_path.read_bytes() == b"retained"


@pytest.mark.asyncio
async def test_prepared_compiler_input_rejects_dangling_member_symlink(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        candidates,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    artifact_path = tmp_path / "taxonomy-members.u32le"
    artifact_path.symlink_to(tmp_path / "missing-members")
    with pytest.raises(RuntimeError, match="member artifact is unsafe"):
        await candidates.prepare_v4_taxonomy_input(
            _PreparedCompilerInputSession(
                common_count=1,
                observe_count=2,
            ),
            schema_name="mrf",
            npi_scope_stage_table="ptg2_v4_npi_scope_test",
            npi_scope_sha256="a" * 64,
            rules=_compiler_rules(),
            members_path=artifact_path,
        )
    assert artifact_path.is_symlink()


@pytest.mark.asyncio
async def test_prepared_compiler_input_requires_stable_read_transaction(
    tmp_path,
) -> None:
    session = _ScriptedSession(
        _Result(({"isolation": "read committed", "read_only": "off"},))
    )
    with pytest.raises(RuntimeError, match="transaction is not stable"):
        await candidates.prepare_v4_taxonomy_input(
            session,
            schema_name="mrf",
            npi_scope_stage_table="ptg2_v4_npi_scope_test",
            npi_scope_sha256="a" * 64,
            rules=_compiler_rules(),
            members_path=tmp_path / "taxonomy-members.u32le",
        )
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_prepared_publication_uses_only_selected_copy_rows(
    monkeypatch,
) -> None:
    """Publish an exact staged COPY without issuing a mutable catalog query."""

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        _noop_map_write_lock,
    )
    rules = _compiler_rules()
    stage_rows = tuple(
        sorted(
            (_projection_row(rule, npi_keys=(0,)) for rule in rules),
            key=lambda row: row["rule_digest"],
        )
    )
    session = _ScriptedSession(
        _Result(stage_rows),
        _Result(),
        _Result(stage_rows),
    )
    publication = await candidates.publish_prepared_v4_inferred_taxonomy_candidates(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="build-token",
        stage_table="ptg2_v4_taxonomy_selected_test",
        rules=rules,
        npi_count=1,
        pattern_count=0,
    )

    assert publication.rule_count == 10
    assert publication.member_count == 10
    assert publication.observe_only_rule_count == 0
    sql_calls = tuple(sql for sql, _parameters in session.calls)
    assert len(sql_calls) == 3
    assert all("npi_taxonomy" not in sql for sql in sql_calls)
    assert "ptg2_v4_taxonomy_selected_test" in sql_calls[0]
    assert "INSERT INTO" in sql_calls[1]
    assert "ptg2_v4_inferred_taxonomy_candidate" in sql_calls[1]


@pytest.mark.asyncio
async def test_compiler_copy_stage_reauthenticates_and_drops_on_drift(
    tmp_path,
) -> None:
    """Do not retain a stage when the selected COPY differs from its summary."""

    copy_path = tmp_path / "selected.copy"
    copy_path.write_bytes(b"compiler-selected-copy")
    session = _ScriptedSession(_Result(), _Result())
    with pytest.raises(RuntimeError, match="COPY changed"):
        await candidates.stage_v4_inferred_taxonomy_compiler_copy(
            session,
            copy_path=copy_path,
            expected_byte_count=copy_path.stat().st_size,
            expected_sha256="0" * 64,
        )

    sql_calls = tuple(sql for sql, _parameters in session.calls)
    assert len(sql_calls) == 2
    assert "CREATE TEMP TABLE" in sql_calls[0]
    assert "ON COMMIT DROP" in sql_calls[0]
    assert 'DROP TABLE IF EXISTS "pg_temp"' in sql_calls[1]


@pytest.mark.asyncio
async def test_compiler_copy_stage_rejects_dangling_symlink(
    tmp_path,
) -> None:
    copy_path = tmp_path / "selected.copy"
    copy_path.symlink_to(tmp_path / "missing.copy")
    with pytest.raises(RuntimeError, match="compiler COPY is invalid"):
        await candidates.stage_v4_taxonomy_copy(
            object(),
            copy_path=copy_path,
            expected_byte_count=1,
            expected_sha256="0" * 64,
        )
    assert copy_path.is_symlink()


@pytest.mark.parametrize("exit_mode", ("success", "failure", "cancel"))
@pytest.mark.asyncio
async def test_managed_compiler_copy_stage_always_drops(
    monkeypatch,
    tmp_path,
    exit_mode,
) -> None:
    stage = candidates.V4InferredTaxonomyCopyStage(
        table_name="ptg2_v4_taxonomy_test",
        copy_path=tmp_path / "selected.copy",
        byte_count=7,
        row_count=10,
    )
    create_stage = AsyncMock(return_value=stage)
    drop_stage = AsyncMock()
    monkeypatch.setattr(candidates, "stage_v4_taxonomy_copy", create_stage)
    monkeypatch.setattr(candidates, "remove_v4_taxonomy_stage", drop_stage)

    async def use_stage() -> None:
        async with candidates.managed_v4_taxonomy_copy_stage(
            object(),
            copy_path=stage.copy_path,
            expected_byte_count=7,
            expected_sha256="a" * 64,
        ) as opened_stage:
            assert opened_stage is stage
            if exit_mode == "failure":
                raise ValueError("injected publication failure")
            if exit_mode == "cancel":
                raise asyncio.CancelledError

    if exit_mode == "failure":
        with pytest.raises(ValueError, match="injected"):
            await use_stage()
    elif exit_mode == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await use_stage()
    else:
        await use_stage()

    create_stage.assert_awaited_once()
    created_session = create_stage.await_args.args[0]
    drop_stage.assert_awaited_once_with(
        created_session,
        stage_table=stage.table_name,
    )
