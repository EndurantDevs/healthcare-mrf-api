# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import struct
from collections import defaultdict
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis

from process import live_progress
from process.ptg_parts import ptg2_shared_snapshot_publish as shared_snapshot_publish
from process.ptg_parts import ptg2_shared_publish as shared_publish
from process.ptg_parts import live_progress as ptg_live_progress
from process.ptg_parts import ptg2_v4_graph_compiler as graph_compiler
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_price import PreparedSharedPriceKeyMap
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
    _validate_authoritative_mapping_summary,
)

from tests.ptg2_shared_snapshot_dictionary_test_support import _row_result

def _failed_tax_stage_compilation(tmp_path):
    """Return the minimum compilation needed to reach the build fence."""

    taxonomy_path = tmp_path / "inferred-taxonomy.copy"
    references_path = tmp_path / "graph-references.jsonl"
    return SimpleNamespace(
        observe=defaultdict(
            int,
            {
                "group_count": 4,
                "component_count": 0,
                "npi_count": 0,
                "npi_prefix_override_owner_count": 0,
                "npi_prefix_override_member_count": 0,
            },
        ),
        resource_admission=defaultdict(int),
        selected_layout="direct",
        pattern_copy_path=None,
        summary=defaultdict(int, {"npi_prefix_target": 201}),
        relation_summaries=(),
        heavy_bitmaps=(),
        group_copy_path=tmp_path / "groups.copy",
        component_copy_path=tmp_path / "components.copy",
        npi_copy_path=tmp_path / "npi.copy",
        provider_set_npi_prefix_override_copy_path=tmp_path / "prefix.copy",
        provider_tax_identity_copy_path=tmp_path / "tax.copy",
        provider_group_tax_identity_copy_path=tmp_path / "group-tax.copy",
        inferred_taxonomy_copy_path=taxonomy_path,
        reference_manifest_path=references_path,
        output_artifacts=(
            SimpleNamespace(
                name="inferred_taxonomy_candidates",
                path=taxonomy_path,
                byte_count=0,
                sha256="e3b0c44298fc1c149afbf4c8996fb924"
                "27ae41e4649b934ca495991b7852b855",
                row_count=0,
            ),
            SimpleNamespace(
                name="graph_references",
                path=references_path,
                byte_count=0,
                sha256="e3b0c44298fc1c149afbf4c8996fb924"
                "27ae41e4649b934ca495991b7852b855",
                row_count=0,
            ),
        ),
    )


def _tax_stage_contract():
    """Return one complete four-state publication contract."""

    return shared_snapshot_publish._V4TaxIdentityContract(
        token_policy_id="ptg-tin-hmac-sha256-v1:release-1",
        token_policy_descriptor_sha256=b"p" * 32,
        source_ordinal_map=({"shard_id": "shard-a", "ordinal": 0},),
        source_ordinal_map_digest=b"s" * 32,
        source_shard_count=1,
        source_bitmap_bytes=1,
        provider_group_count=4,
        tax_identity_count=1,
        matched_ein_count=1,
        missing_count=1,
        malformed_count=1,
        unsupported_type_count=1,
        content_digest=b"c" * 32,
    )


class _AtomicSourceTransactionFixture:
    """Record one source-publication rollback path through real ordering."""

    def __init__(self) -> None:
        self.session = _AtomicSourceSession()
        self.publication_events = []
        self.staged = object()
        self.prepared = SimpleNamespace(cleanup=self._cleanup)

    def _cleanup(self) -> None:
        self.publication_events.append(("cleanup", None))

    @asynccontextmanager
    async def transaction(self):
        self.publication_events.append(("begin", self.session))
        try:
            yield self.session
        except BaseException:
            self.publication_events.append(("rollback", self.session))
            raise
        else:
            self.publication_events.append(("commit", self.session))

    async def stage_source(self, actual_session, actual_prepared):
        assert actual_session is self.session
        assert actual_prepared is self.prepared
        self.publication_events.append(("source-stage", actual_session))
        return self.staged

    async def publish_tax_groups(self, actual_session, **_kwargs):
        assert actual_session is self.session
        self.publication_events.append(("merged-tax-groups", actual_session))

    async def lock_physical_layout(self, actual_session, **_kwargs):
        assert actual_session is self.session
        self.publication_events.append(("physical-layout-lock", actual_session))

    async def publish_source(self, actual_session, **kwargs):
        assert actual_session is self.session
        assert kwargs["prepared"] is self.prepared
        assert kwargs["staged"] is self.staged
        assert kwargs["logical_snapshot_id"] == "synthetic-snapshot"
        self.publication_events.append(("source-local-tax", actual_session))
        raise RuntimeError("post-source graph failure")


class _AtomicSourceResult:
    def one(self):
        return ()

    def scalar(self):
        return 1


class _AtomicSourceSession:
    async def execute(self, *_args, **_kwargs):
        return _AtomicSourceResult()

    async def scalar(self, *_args, **_kwargs):
        return 0


def _atomic_source_transaction_fixture():
    """Return callbacks that record one source-publication rollback path."""

    return _AtomicSourceTransactionFixture()


def _install_atomic_source_transaction_mocks(monkeypatch, atomic_fixture) -> None:
    """Replace unrelated graph work while retaining the real call ordering."""

    monkeypatch.setattr(shared_snapshot_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "transaction",
        atomic_fixture.transaction,
    )
    replacements_by_name = {
        "_validated_v4_tax_identity_contract": (
            lambda _compilation: _tax_stage_contract()
        ),
        "_v4_tax_artifact_byte_count": lambda _compilation: 394,
        "prepare_tax_identity_source_projection": (
            lambda *_args, **_kwargs: atomic_fixture.prepared
        ),
        "_copy_binary_file_to_stage": AsyncMock(),
        "stage_tax_identity_source_projection": atomic_fixture.stage_source,
        "lock_v4_shared_layout_for_map_write": atomic_fixture.lock_physical_layout,
        "prepare_v4_cas_block_stage": AsyncMock(),
        "_publish_v4_cas_in_session": AsyncMock(return_value=object()),
        "stage_v4_inferred_taxonomy_compiler_copy": AsyncMock(
            return_value=SimpleNamespace(table_name="taxonomy-stage")
        ),
        "_validate_v4_dictionary_stage": AsyncMock(),
        "_validate_v4_tax_identity_stages": AsyncMock(),
        "publish_v4_snapshot_maps": AsyncMock(return_value=object()),
        "_require_v4_atomic_map_publication": lambda *_args: None,
        "_publish_v4_tax_identity_manifest": AsyncMock(return_value={}),
        "_publish_v4_dictionary_stage_ranges": AsyncMock(),
        "_publish_v4_tax_group_ranges": atomic_fixture.publish_tax_groups,
        "publish_v4_relation_manifests": AsyncMock(),
        "publish_v4_heavy_owners": AsyncMock(),
        "publish_prepared_v4_inferred_taxonomy_candidates": AsyncMock(
            return_value=SimpleNamespace(
                rule_count=0,
                observe_only_rule_count=0,
            )
        ),
        "publish_staged_tax_identity_source_projection": (
            atomic_fixture.publish_source
        ),
    }
    for name, replacement in replacements_by_name.items():
        monkeypatch.setattr(shared_snapshot_publish, name, replacement)
    monkeypatch.setattr(shared_snapshot_publish, "PTG2_V4_GRAPH_RESOURCE_FIELDS", ())
    monkeypatch.setattr(
        shared_snapshot_publish,
        "PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS",
        (),
    )
