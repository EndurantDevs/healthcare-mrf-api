# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Code pricing API reads serve the stored capability located by schema-qualified name."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_code_storage as storage
from api import plan_pricing_projection_read as projection
from api.plan_pricing_code_contract import identity_digest
from api.plan_pricing_code_read import CodePricingCapability, resolve_code_pricing
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable, code_projection_scope
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_code_projection import _identity, _selection
from tests.test_plan_pricing_code_zip import _params, _zip_identity

SCHEMA = "reference_family_archive_" + "1" * 32
ARGS = {"code_system": "CPT", "code": "27447"}


class _Result:
    def __init__(self, value):
        self.value = value

    def all(self):
        return self.value

    def mappings(self):
        return self

    def one(self):
        return self.value

    def scalars(self):
        return self


class _Session:
    """Answer the by-name statements; any custody query fails the test."""

    def __init__(self, header_identity):
        self.statements, self.attached, self.plain, self.pin = [], True, True, True
        self.dependencies = [(SCHEMA, name) for name in storage.SPEC.table_names]
        self.header = {"identity": header_identity, "content_digest": "a" * 64, "counts": {}}

    async def scalar(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        if sql == "SELECT to_regclass(:view) IS NOT NULL":
            return self.attached
        if sql == storage._PLAIN_HEAPS_SQL:
            return len(parameters["relations"]) - (not self.plain)
        assert "ptg2_snapshot_pin" in sql, sql
        return self.pin

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        if sql == storage._FAMILY_SQL:
            return _Result(self.dependencies)
        if "WITH center AS MATERIALIZED" in sql:
            return _Result(["48104", "48105"])
        if "plan_pricing_code_manifest" in sql:
            return _Result(self.header)
        assert sql.startswith("LOCK TABLE "), sql
        return _Result(None)


@pytest.fixture(autouse=True)
def _no_custody(monkeypatch):
    refuse = AsyncMock(side_effect=AssertionError("custody is not part of API reads"))
    monkeypatch.setattr(storage, "protected_owner", refuse)
    monkeypatch.setattr(storage, "_require_no_untrusted_mutation", refuse)
    monkeypatch.setattr(storage, "_require_closed_local_custody", refuse)
    monkeypatch.setattr(storage.native, "capture_model_family_stage_ownership", refuse)
    monkeypatch.setattr(storage.native, "_lock_family", refuse)


async def test_copy_with_different_recorded_identifiers_serves_the_stored_capability():
    stored = _identity()
    # Identifiers recorded on another database never match this catalog and are not consulted.
    stored["sources"][0]["membership"]["active_attachment"]["relation_oid"] = 4_000_000_000
    stored["sources"][0]["source_identity"]["source_storage"] = {"database_oid": 1, "relation_oids": [["x", 2]]}
    session = _Session(copy.deepcopy(stored))
    capability = await resolve_code_pricing(session, _selection(), ARGS)
    assert capability.identity == stored and capability.schema_name == SCHEMA
    assert capability.projection_id == identity_digest(stored)
    _view, family, lock, plain, manifest, pin = session.statements
    assert family == storage._FAMILY_SQL and plain == storage._PLAIN_HEAPS_SQL
    relations = [f'"{SCHEMA}"."{name}"' for name in storage.SPEC.table_names]
    assert lock == f'LOCK TABLE {",".join(relations)},"mrf".ptg2_snapshot_pin IN ACCESS SHARE MODE'
    assert f'FROM ONLY "{SCHEMA}".plan_pricing_code_manifest' in manifest and "ptg2_snapshot_pin" in pin


async def test_family_row_security_refuses_before_manifest_read():
    session = _Session(_identity())
    session.plain = False
    with pytest.raises(PTG2ManifestArtifactError, match="plain heap"):
        await resolve_code_pricing(session, _selection(), ARGS)
    assert not any("plan_pricing_code_manifest WHERE" in sql for sql in session.statements)


async def test_missing_view_is_not_found_and_takes_no_lock():
    session = _Session(_identity())
    session.attached = False
    assert await resolve_code_pricing(session, _selection(), ARGS) is None
    assert session.statements == ["SELECT to_regclass(:view) IS NOT NULL"]


@pytest.mark.parametrize("damage", ["missing", "split", "prefix", "retention_only"])
async def test_view_dependencies_must_be_one_complete_family(damage):
    session = _Session(_identity())
    if damage == "missing":
        session.dependencies.pop()
    elif damage == "split":
        session.dependencies[0] = ("reference_family_archive_" + "2" * 32, session.dependencies[0][1])
    elif damage == "prefix":
        session.dependencies = [("public", name) for _, name in session.dependencies]
    else:
        session.dependencies = []
    with pytest.raises(PTG2ManifestArtifactError, match="dependencies differ"):
        await resolve_code_pricing(session, _selection(), ARGS)
    assert not any(sql.startswith("LOCK") for sql in session.statements)


@pytest.mark.parametrize("damage", ["code", "revision", "malformed", "digest"])
async def test_stored_identity_must_name_the_requested_anchor(damage):
    stored = _identity()
    session = _Session(stored)
    if damage == "code":
        session.header["identity"] = stored | {"code": "99213"}
    elif damage == "revision":
        session.header["identity"] = stored | {"serving_revision_id": "other-revision"}
    elif damage == "malformed":
        session.header["identity"] = "not an identity"
    else:
        session.header["content_digest"] = "A" * 64
    with pytest.raises(PTG2ManifestArtifactError, match="sealed identity differs"):
        await resolve_code_pricing(session, _selection(), ARGS)


async def test_missing_retention_pin_refuses():
    session = _Session(_identity())
    session.pin = False
    with pytest.raises(PTG2ManifestArtifactError, match="retention pin"):
        await resolve_code_pricing(session, _selection(), ARGS)


@pytest.mark.parametrize("plain", [True, False])
async def test_zip_page_share_locks_plain_geography_before_reading_it(monkeypatch, plain):
    capability = CodePricingCapability(_zip_identity(), SCHEMA)
    session = _Session(capability.identity)
    session.plain = plain
    monkeypatch.setattr(projection, "_read_page", AsyncMock(return_value=([], 0)))
    request = projection._ProjectionRequest(
        "rate_aggregates", capability.projection_id, capability.identity["contract"], "CPT", "27447"
    )
    pagination = SimpleNamespace(limit=2, offset=0, page=1)
    with code_projection_scope(capability):
        page = projection._projection_page_response(
            session, request, _params(expected=False), pagination, None, _selection()
        )
        if not plain:
            with pytest.raises(PlanPricingProjectionUnavailable, match="plain table"):
                await page
            assert len(session.statements) == 2
            return
        await page
    assert session.statements[0] == 'LOCK TABLE "mrf"."geo_zip_lookup" IN ACCESS SHARE MODE'
    assert session.statements[1] == storage._PLAIN_HEAPS_SQL
    assert "WITH center AS MATERIALIZED" in session.statements[2]
    projection._read_page.assert_awaited_once()
