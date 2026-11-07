# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real owner artifacts and digest; SQL/transactions are explicit transport fakes."""

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_projection_complete as completion
from process.custom_import.family_block_validate import ClosedCandidateIdentity
from process.custom_import.family_shard_projection_verify import ClosedProjectionEvidence
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_family_shard_projection_stage import _binding, _control_rows, _source_context
from tests.test_custom_import_query_projection_evidence import _fixture_digest

_NOW = datetime(2030, 1, 3, tzinfo=timezone.utc)


def _inputs(common):
    original = _binding(common)
    binding = replace(original, columns_sha256_by_role={role: b"s" * 32 for role in original.tables_by_role})
    build, generation, _snapshot, _source_binding = _control_rows(common)
    scope = completion._scope(common, build, generation)
    identity = ClosedCandidateIdentity(
        scope, _NOW, common.layout.digest, binding.owner_oid, tuple(sorted(binding.oids_by_role.items()))
    )
    digest = _fixture_digest(common.request.definition)
    proof = ClosedProjectionEvidence(identity, digest.finish(), digest.checkpoint())
    return binding, build, generation, proof


class _NativeBoundary:
    """No PostgreSQL is used; the real native custody path is qualified separately."""

    def __init__(self, common, binding, build, generation, proof):
        self.common, self.binding, self.build = common, binding, build
        self.generation, self.proof = generation, proof
        self.manifest = None
        self.observations = []
        self.fail_commit = False

    @asynccontextmanager
    async def page(self, factory, request, build_id):
        assert factory is None and request is self.common.request and build_id == self.build.build_id
        previous = self.manifest
        self.observations.append("begin")
        try:
            yield self, self.build
            if self.fail_commit:
                raise RuntimeError("synthetic fresh authority lost at commit")
        except BaseException:
            self.manifest = previous
            self.observations.append("rollback")
            raise
        self.observations.append("commit")

    async def current(self, session, common, build, proof):
        assert (session, common, build, proof) == (self, self.common, self.build, self.proof)
        self.observations.append("current_identity")
        return self.binding, self.generation

    async def execute(self, session, statement):
        assert session is self
        sql = str(statement)
        if "clock_timestamp" in sql:
            self.observations.append("clock")
            return SimpleNamespace(scalar_one=lambda: _NOW)
        assert "custom_import_query_projection" in sql
        self.observations.append("manifest")
        return SimpleNamespace(scalar_one_or_none=lambda: self.manifest)

    def add(self, manifest):
        self.observations.append("add")
        self.manifest = manifest

    async def flush(self, session):
        assert session is self
        self.observations.append("flush")

    def install(self, monkeypatch):
        monkeypatch.setattr(completion, "_page_session", self.page)
        monkeypatch.setattr(completion, "_current", self.current)
        monkeypatch.setattr(completion, "_execute", self.execute)
        monkeypatch.setattr(completion, "_flush_page", self.flush)


@pytest.mark.asyncio
async def test_exact_manifest_replay_preserves_timestamp_and_build_state(tmp_path, monkeypatch):
    async with _source_context(tmp_path, monkeypatch) as common:
        binding, build, generation, proof = _inputs(common)
        boundary = _NativeBoundary(common, binding, build, generation, proof)
        boundary.install(monkeypatch)
        build_state_by_name = dict(vars(build))
        for _attempt in range(2):
            assert (
                await completion.complete_shard_query_projection(None, common, build.build_id, proof) == proof.evidence
            )
        assert boundary.observations.count("add") == boundary.observations.count("flush") == 1
        assert boundary.observations.count("clock") == 1 and boundary.observations.count("commit") == 2
        assert boundary.manifest.complete_at == _NOW and vars(build) == build_state_by_name
        for scope in ("root", "child"):
            assert getattr(boundary.manifest, scope + "_table_oid") == binding.oids_by_role["query_" + scope]
            assert getattr(boundary.manifest, scope + "_columns_sha256") == b"s" * 32


def _changed_proof(proof, fault):
    if fault == "digest":
        return replace(proof, evidence=replace(proof.evidence, projection_sha256=b"x" * 32))
    if fault == "checkpoint":
        return replace(proof, checkpoint=replace(proof.checkpoint, chain_sha256=b"x" * 32))
    if fault == "definition":
        return replace(proof, checkpoint=replace(proof.checkpoint, capability_sha256=b"x" * 32))
    if fault == "disabled":
        return replace(proof, evidence=None, checkpoint=None)
    return proof


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("digest", "checkpoint", "definition", "disabled", "commit", "retry"))
async def test_inconsistent_evidence_or_authority_cannot_complete(tmp_path, monkeypatch, fault):
    async with _source_context(tmp_path, monkeypatch) as common:
        binding, build, generation, proof = _inputs(common)
        boundary = _NativeBoundary(common, binding, build, generation, proof)
        boundary.install(monkeypatch)
        changed = _changed_proof(proof, fault)
        if fault == "commit":
            boundary.fail_commit = True
        if fault == "retry":
            await completion.complete_shard_query_projection(None, common, build.build_id, proof)
            boundary.manifest.root_columns_sha256 = b"x" * 32
        with pytest.raises((CandidateRunnerError, RuntimeError)):
            await completion.complete_shard_query_projection(None, common, build.build_id, changed)
        if fault in {"digest", "checkpoint", "definition", "disabled"}:
            assert not boundary.observations
        elif fault == "commit":
            assert boundary.manifest is None and boundary.observations[-1] == "rollback"


@pytest.mark.asyncio
async def test_disabled_capability_rechecks_authority_and_refuses_a_manifest(tmp_path, monkeypatch):
    async with _source_context(tmp_path, monkeypatch, disabled=True) as common:
        binding, build, generation, proof = _inputs(common)
        proof = replace(proof, evidence=None, checkpoint=None)
        boundary = _NativeBoundary(common, binding, build, generation, proof)
        boundary.install(monkeypatch)
        assert await completion.complete_shard_query_projection(None, common, build.build_id, proof) is None
        assert boundary.observations == ["begin", "current_identity", "manifest", "commit"]
        boundary.manifest = object()
        with pytest.raises(CandidateRunnerError, match="unconfigured"):
            await completion.complete_shard_query_projection(None, common, build.build_id, proof)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ("phase", "source", "graph", "verified", "count", "error", "freeze", "base", "producer")
)
async def test_completion_requires_exact_unfinished_native_output(tmp_path, monkeypatch, fault):
    async with _source_context(tmp_path, monkeypatch) as common:
        _binding, build, generation, _proof = _inputs(common)
        build.phase, build.output_frozen_at = "verifying", _NOW
        completion._scope(common, build, generation)
        changes_by_fault = {
            "phase": {"phase": "graph"},
            "source": {"source_frozen_at": None},
            "graph": {"graph_frozen_at": None},
            "verified": {"verified_at": _NOW},
            "count": {"source_occurrence_count": 99},
            "error": {"candidate_error_count": 1},
            "freeze": {"output_frozen_at": None},
            "producer": {"producing_fence": 99},
        }
        if fault == "base":
            generation.base_generation_id = 99
        else:
            for name, value in changes_by_fault[fault].items():
                setattr(build, name, value)
        with pytest.raises(CandidateRunnerError):
            completion._scope(common, build, generation)
