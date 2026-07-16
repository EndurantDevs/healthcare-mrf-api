# See LICENSE.

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sanic.exceptions import Forbidden

from api import ptg2_snapshot
from api.ptg2_candidate_audit import (
    PTG2_CANDIDATE_AUDIT_ACCESS_ARG,
    PTG2_CANDIDATE_AUDIT_HEADER,
    PTG2CandidateAuditAccess,
    candidate_audit_access_from_args,
    candidate_audit_access_from_request,
)
from api.ptg2_tables import snapshot_serving_tables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def _request(headers=None):
    return SimpleNamespace(headers=headers or {})


def _candidate_access():
    return PTG2CandidateAuditAccess(
        snapshot_id="candidate-snapshot",
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
    )


def test_candidate_audit_access_requires_explicit_header_and_control_token(monkeypatch):
    query_arg_map = {
        "snapshot_id": "candidate-snapshot",
        "source_key": "source-a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
    }
    assert candidate_audit_access_from_request(_request(), query_arg_map) is None

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    with pytest.raises(Forbidden, match="control API token is invalid"):
        candidate_audit_access_from_request(
            _request({PTG2_CANDIDATE_AUDIT_HEADER: "candidate-snapshot"}),
            query_arg_map,
        )

    access = candidate_audit_access_from_request(
        _request(
            {
                PTG2_CANDIDATE_AUDIT_HEADER: "candidate-snapshot",
                "Authorization": "Bearer operator-secret",
            }
        ),
        query_arg_map,
    )
    assert access == _candidate_access()


def test_candidate_audit_access_is_exact_and_user_scalars_are_ignored(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    request_header_map = {
        PTG2_CANDIDATE_AUDIT_HEADER: "candidate-snapshot",
        "X-HealthPorta-Control-Token": "operator-secret",
    }
    with pytest.raises(Forbidden, match="candidate audit access is invalid"):
        candidate_audit_access_from_request(
            _request(request_header_map),
            {
                "snapshot_id": "candidate-snapshot",
                "source_key": "source-a",
                "plan_id": "12-3456789",
            },
        )
    assert (
        candidate_audit_access_from_args(
            {PTG2_CANDIDATE_AUDIT_ACCESS_ARG: "candidate-snapshot"}
        )
        is None
    )


@pytest.mark.asyncio
async def test_candidate_snapshot_resolution_requires_matching_request_capability():
    class Result:
        def scalar(self):
            return "candidate-snapshot"

    class Session:
        def __init__(self):
            self.calls = []

        async def execute(self, statement, params):
            self.calls.append((str(statement), dict(params)))
            return Result()

    session = Session()
    access = _candidate_access()
    resolved = await ptg2_snapshot.current_snapshot_id(
        session,
        requested_snapshot_id="candidate-snapshot",
        requested_source_key="source-a",
        requested_plan_id="12-3456789",
        requested_plan_market_type="group",
        candidate_audit_access=access,
    )
    assert resolved == "candidate-snapshot"
    sql, params = session.calls[0]
    assert "status = 'validated'" in sql
    assert "manifest->'activation'->>'contract'" in sql
    assert "status = 'published'" not in sql
    assert params["candidate_source_key"] == "source-a"

    mismatch_session = Session()
    assert (
        await ptg2_snapshot.current_snapshot_id(
            mismatch_session,
            requested_snapshot_id="different-snapshot",
            requested_source_key="source-a",
            requested_plan_id="12-3456789",
            requested_plan_market_type="group",
            candidate_audit_access=access,
        )
        is None
    )
    assert mismatch_session.calls == []


@pytest.mark.asyncio
async def test_candidate_serving_metadata_query_is_validated_and_scope_bound():
    class EmptyResult:
        def one_or_none(self):
            return None

    class Session:
        def __init__(self):
            self.calls = []

        async def execute(self, statement, params):
            self.calls.append((str(statement), dict(params)))
            return EmptyResult()

    session = Session()
    with pytest.raises(PTG2ManifestArtifactError):
        await snapshot_serving_tables(
            session,
            "candidate-snapshot",
            candidate_audit_access=_candidate_access(),
        )
    sql, params = session.calls[0]
    assert "snapshot.status = 'validated'" in sql
    assert "snapshot.status = 'published'" not in sql
    assert "snapshot_scope.plan_id = ANY" in sql
    assert "snapshot_scope.plan_market_type = :candidate_plan_market_type" in sql
    assert params["candidate_plan_ids"] == ["12-3456789", "123456789"]
    assert params["candidate_plan_market_type"] == "group"
