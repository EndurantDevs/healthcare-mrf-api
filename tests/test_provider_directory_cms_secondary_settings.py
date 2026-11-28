# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Secondary consumers accept exact source settings successors and reject near misses."""

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import migration_generic_source_candidates as candidates
from process import provider_directory_source_checkpoint_catalog as checkpoint
from process import provider_directory_terminal_root_retirement_controls as retirement
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_source_checkpoint_catalog import _guards

_OLD = ["search_path=pg_catalog"]
_NEW = ["search_path=pg_catalog, pg_temp"]
_UNAPPROVED = (
    None,
    [],
    ["search_path=pg_temp, pg_catalog"],
    ["search_path=pg_catalog", "search_path=pg_temp"],
    ["search_path=pg_catalog, pg_temp", "TimeZone=UTC"],
    ["search_path=pg_catalog, public, pg_temp"],
)


def _checkpoint_case(monkeypatch, root, settings):
    guards = _guards(root)
    monkeypatch.setattr(
        checkpoint,
        "_BODIES",
        {name: hashlib.sha256(b"BEGIN RETURN NULL; END;").hexdigest() for name in checkpoint._BODIES},
    )
    for guard in guards:
        guard["proconfig"] = deepcopy(settings)
    policy = SimpleNamespace(schema="sample", owner_role="owner", control_runtime_roles=("builder",))
    return guards, policy


@pytest.mark.parametrize("root", checkpoint.CHECKPOINT_LAYOUTS)
@pytest.mark.parametrize("settings", [_OLD, _NEW])
def test_checkpoint_settings_pair_preserves_complete_invocation(monkeypatch, root, settings):
    guards, policy = _checkpoint_case(monkeypatch, root, settings)
    original = deepcopy(guards)
    checkpoint._require_guards(guards, root, policy, legacy=True)
    assert guards == original
    guards[0]["tgargs"] = b"changed\x00"
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_changed"):
        checkpoint._require_guards(guards, root, policy, legacy=True)


@pytest.mark.parametrize("settings", _UNAPPROVED)
def test_checkpoint_data_contract_ignores_settings(monkeypatch, settings):
    """Opaque historical settings do not replace the complete DATA invocation contract."""
    guards, policy = _checkpoint_case(monkeypatch, checkpoint.BULK_ROOT, settings)
    before = deepcopy(guards)
    checkpoint._require_guards(guards, checkpoint.BULK_ROOT, policy, legacy=True)
    assert guards == before
    guards[0]["tgargs"] = b"changed\x00"
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_changed"):
        checkpoint._require_guards(guards, checkpoint.BULK_ROOT, policy, legacy=True)


@pytest.mark.asyncio
async def test_maintenance_query_keeps_exact_source_derived_settings_body_and_hooks(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "snapshot_schema")
    monkeypatch.setattr(retirement, "require_native_retirement_controls", AsyncMock(return_value=set()))
    database = SimpleNamespace(scalar=AsyncMock(return_value=True))
    await retirement.require_retirement_controls(database)
    call = database.scalar.await_args
    functions = json.loads(call.kwargs["functions"])
    for function in functions:
        legacy = function["settings"]
        assert legacy in (_OLD, [*_OLD, "TimeZone=UTC"])
        assert function["settings_successor"] == [*_NEW, *legacy[1:]]
        assert function["body"] and function["language"] in {"sql", "plpgsql"}
    assert json.loads(call.kwargs["triggers"])
    assert "IS DISTINCT FROM expected.settings\n                   AND" in call.args[0]
    assert "IS DISTINCT FROM expected.settings_successor)" in call.args[0]
    assert "overload.oid<>p.oid" in call.args[0]
    assert "t.tgfoid<>pg_catalog.to_regprocedure(expected.function)" in call.args[0]


def test_existing_copy_guard_snapshot_keeps_finite_stored_settings():
    statement = str(candidates._COPY_GUARD_SNAPSHOT)
    assert "p.proconfig IN (ARRAY['search_path=pg_catalog']::text[]," in statement
    assert "ARRAY['search_path=pg_catalog, pg_temp']::text[])" in statement
    assert "p.prorettype='trigger'::regtype AND language.lanname='plpgsql' AND p.prosecdef" in statement
