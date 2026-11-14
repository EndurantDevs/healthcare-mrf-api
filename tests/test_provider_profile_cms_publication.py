# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A same-source Doctors republication fences category pages without changing provenance."""

import datetime
import importlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import provider_profile as profile
from api import provider_profile_cms as cms
from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from process import cms_doctors_immutable as immutable
from process import cms_doctors_preparation as preparation
from process import reference_family_result_generation as generation
from process import reference_source_generation as revisions
from tests.network_read_execution_support import with_read_identity_receipts
from tests.test_cms_doctors_organizations import _unit_source_transaction
from tests.test_provider_profile_cms import CMS_GENERATION, NPI, _credential_row, _education_row

native = importlib.import_module("process.cms_doctors")
RELATIONS = generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
OIDS = (21001, 21002, 21003)


@asynccontextmanager
async def _reader_scope(session, state_by_field):
    """Model the explicit Reader owner separately from mocked publication transactions."""
    assert state_by_field.get("binding") is None
    state_by_field["reader"] = True
    session.info["api_reader_verified"] = True
    try:
        yield session
    finally:
        session.info.pop("api_reader_verified", None)
        session.info.pop("provider_profile_relations", None)
        state_by_field.pop("reader", None)


def _mock_doctors_owner(monkeypatch):
    """Only model transaction ownership; genuine Publisher custody is native proof."""
    monkeypatch.setattr(native, "source_transaction", lambda database, **kwargs: database.transaction())
    monkeypatch.setattr(native, "source_owner_transaction", _unit_source_transaction)
    monkeypatch.setattr(preparation, "source_owner_transaction", _unit_source_transaction)
    monkeypatch.setattr(native, "publish_doctors_identities", AsyncMock())
    monkeypatch.setattr(native, "seal_doctors_stages", AsyncMock())
    monkeypatch.setattr(immutable, "_writer", AsyncMock())
    monkeypatch.setattr(immutable, "require_doctors_storage", AsyncMock(return_value=tuple(zip(RELATIONS, OIDS))))


def _reader_and_writer_session(state_by_field):
    """Keep native receipt identity stable while semantic authority changes after publication."""
    session = SimpleNamespace(info={}, execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    session.in_transaction = lambda: bool(state_by_field.get("reader") or state_by_field.get("binding"))
    with_read_identity_receipts(session)

    async def execute(statement, _parameters=None):
        if str(statement).startswith("SELECT local_lineage_id,local_generation,"):
            authority = state_by_field["authority"]
            fields = ("local_lineage_id", "local_generation", "origin_lineage_id", "origin_generation", "published_at")
            row = {name: authority[name] for name in fields} if authority is not None else None
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))
        if "SELECT EXISTS" in str(statement) and generation.TABLE_NAME in str(statement):
            assert state_by_field.get("reader") is True
            return SimpleNamespace(scalar=lambda: session.scalar.return_value)
        return session.execute.return_value

    session.execute.side_effect = execute
    return session


def _named_doctors_relations():
    installed_names = {*RELATIONS, generation.TABLE_NAME}
    return {
        f"{schema}.{name}": f"{schema}.{name}" if name in installed_names else None
        for schema, name in snapshot.provider_snapshot_names("mrf")
    }


@pytest.fixture
def runtime(monkeypatch):
    """Keep Reader refusal proof and publication state under distinct mocked owners."""
    state_by_field = {
        "credentials": [],
        "authority": {
            "importer_id": "cms-doctors",
            "local_lineage_id": "00000000-0000-0000-0000-000000000001",
            "local_generation": 1,
            "origin_lineage_id": "00000000-0000-0000-0000-000000000001",
            "origin_generation": 1,
            "published_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            "relation_oids": list(OIDS),
        },
    }
    session = _reader_and_writer_session(state_by_field)
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value="installed"))
    database._transaction_binding = lambda: state_by_field.get("binding")
    database.has_reader_session = lambda: state_by_field.get("reader", False)

    @asynccontextmanager
    async def transaction():
        state_by_field["binding"] = SimpleNamespace(session=session)
        try:
            yield session
        finally:
            state_by_field.pop("binding", None)

    async def rows(statement, **_params):
        source_rows = state_by_field["credentials"] if "row_number" in str(statement) else [_education_row()]
        return [SimpleNamespace(_mapping=row) for row in source_rows]

    async def first(_database, statement, **params):
        if str(statement).lstrip().startswith("UPDATE "):
            state_by_field["authority"].update(
                local_generation=params["next"],
                origin_generation=params["generation"],
                relation_oids=list(params["oids"]),
            )
        return state_by_field["authority"]

    _mock_doctors_owner(monkeypatch)
    database.transaction, database.all = transaction, rows
    database.reader_session = lambda: _reader_scope(session, state_by_field)
    monkeypatch.setattr(cms, "db", database)
    monkeypatch.setattr(npi, "db", database)
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(npi, "_runtime_db_schema", lambda: "mrf")
    monkeypatch.setattr(profile, "fetch_state_profile_projection", AsyncMock(return_value=None))
    monkeypatch.setattr(profile, "fetch_additional_state_profile_projections", AsyncMock(return_value=[]))
    monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", AsyncMock(return_value={}))
    relations_by_name = _named_doctors_relations()
    monkeypatch.setattr(snapshot, "lock_reader_relations", AsyncMock(return_value=relations_by_name))
    monkeypatch.setattr(snapshot, "_inventory_relation_names", AsyncMock(return_value=relations_by_name))
    monkeypatch.setattr(snapshot, "_requires_cms_receipt", AsyncMock(return_value=False))
    monkeypatch.setattr(generation, "_first", first)
    monkeypatch.setattr(generation, "current_reference_family_relation_oids", AsyncMock(return_value=OIDS))
    monkeypatch.setattr(revisions, "install_reference_revision_guards", AsyncMock())
    return SimpleNamespace(
        state_by_field=state_by_field, database=database, session=session, relations_by_name=relations_by_name
    )


async def republish(runtime, monkeypatch, mode):
    """Run each publication entry point with one complete prelock and shared authority update."""
    prelock = AsyncMock(wraps=native._lock_cms_doctors_publication)
    monkeypatch.setattr(native, "_lock_cms_doctors_publication", prelock)
    runtime.database.text = text
    runtime.session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: [*RELATIONS, *(name + "_old" for name in RELATIONS)])
    )
    live_lock_statements = []

    async def status(statement):
        if statement.startswith("LOCK TABLE "):
            live_lock_statements.append(statement)
            assert len(live_lock_statements) == 1

    runtime.database.status.side_effect = status
    monkeypatch.setattr(native, "swap_education_stage", AsyncMock())
    monkeypatch.setattr(native, "swap_group_site_stage", AsyncMock())
    stage = native.make_class(native.DoctorClinicianAddress, "synthetic")
    if mode == "ordinary":
        receipt = await native._publish_cms_doctors_stage(stage, "mrf", "synthetic")
        prelock.assert_awaited_once_with(runtime.session, stage, "mrf", "synthetic")
        assert len(live_lock_statements) == 1
        return receipt
    monkeypatch.setattr(preparation.archive, "_verify_incumbent", AsyncMock())
    for name in ("_assert_stage", "_assert_stage_indexes", "assert_prepared_cms_doctors_seal"):
        monkeypatch.setattr(preparation, name, AsyncMock())
    prepared = preparation.PreparedCMSDoctorsGeneration(
        "mrf",
        "synthetic",
        tuple((name, name + "_synthetic", oid) for name, oid in zip(RELATIONS, OIDS, strict=True)),
        SimpleNamespace(relation_oids=tuple(zip(RELATIONS, OIDS, strict=True))),
        generation.validate_reference_family_result_generation_authority(runtime.state_by_field["authority"]),
        {},
        {},
    )
    async with runtime.database.transaction():
        receipt = await preparation.apply_prepared_cms_doctors_generation(prepared)
    prelock.assert_awaited_once_with(runtime.session, stage, "mrf", "synthetic")
    assert len(live_lock_statements) == 1
    return receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ordinary", "prepared"])
async def test_republication_refuses_old_credential_page(runtime, monkeypatch, mode):
    request = SimpleNamespace(args={"category": "certifications", "limit": "1", "include_evidence": "true"})
    before = await npi.get_provider_profile(request, str(NPI))
    before_profile = json.loads(before.body)["provider_profile"]
    assert before.status == 200 and not before_profile["categories"]["certifications"]["items"]
    receipt = await republish(runtime, monkeypatch, mode)
    assert receipt.serving_generation.origin_generation == 2
    republished = json.loads((await npi.get_provider_profile(request, str(NPI))).body)["provider_profile"]
    assert republished["generation_id"] != before_profile["generation_id"]
    assert republished["categories"] == before_profile["categories"]
    runtime.state_by_field["credentials"] = [_credential_row(1, "MD")]
    current = await npi.get_provider_profile(request, str(NPI))
    body = json.loads(current.body)
    current_profile = body["provider_profile"]
    assert current.status == 200
    assert before_profile["composer_version"] == current_profile["composer_version"]
    assert before_profile["generation_id"] != current_profile["generation_id"]
    assert republished["generation_id"] == current_profile["generation_id"]
    assert before_profile["sources"] == current_profile["sources"]
    assert set(current_profile["source_generations"]) == {"cms_doctors"}
    assert current_profile["categories"]["certifications"]["items"][0]["value"] == "MD"
    evidence = body["provider_profile_evidence"]["sources"]["cms_doctors"]
    assert evidence["generation_id"] == CMS_GENERATION
    assert evidence["records"][0]["generation_id"] == CMS_GENERATION
    assert evidence["records"][0]["raw_fields"]["cred"] == "MD"
    request.args["generation_id"] = before_profile["generation_id"]
    rejected = await npi.get_provider_profile(request, str(NPI))
    assert rejected.status == 409
    assert json.loads(rejected.body)["error"] == "provider_profile_generation_changed"
    request.args["generation_id"] = current_profile["generation_id"]
    assert (await npi.get_provider_profile(request, str(NPI))).status == 200
    assert snapshot.snapshot_cms_serving_generation("mrf") is None
    assert not runtime.session.info and runtime.database._transaction_binding() is None
    assert not runtime.database.has_reader_session()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "names", "source", "schema"])
async def test_snapshot_and_source_drift_still_refuse(runtime, monkeypatch, fault):
    if fault == "missing":
        runtime.state_by_field["authority"] = None
    elif fault == "names":
        runtime.relations_by_name[f"mrf.{RELATIONS[0]}"] = None
    elif fault == "source":
        runtime.state_by_field["credentials"] = [_credential_row(1, "MD", generation="d" * 64)]
    else:
        monkeypatch.setattr(cms.CMSDoctorEducation.__table__, "schema", "synthetic")
    error, reason = {
        "missing": (ServiceUnavailable, r"^Provider data is temporarily unavailable\.$"),
        "source": (RuntimeError, "^cms_education_profile_generation_mixed$"),
        "names": (ServiceUnavailable, r"^Provider data is temporarily unavailable\.$"),
        "schema": (ServiceUnavailable, r"^Provider data is temporarily unavailable\.$"),
    }[fault]
    with pytest.raises(error, match=reason):
        await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
    assert snapshot.snapshot_cms_serving_generation("mrf") is None
    assert not runtime.session.info and runtime.database._transaction_binding() is None


@pytest.mark.asyncio
async def test_legacy_relation_ids_do_not_change_profile_result(runtime):
    before = await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
    runtime.state_by_field["authority"]["relation_oids"] = [31001, 31002, 31003]
    after = await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
    assert before.status == after.status == 200 and before.body == after.body
    assert not runtime.session.info and runtime.database._transaction_binding() is None


@pytest.mark.asyncio
async def test_legacy_no_publication_identity_is_not_invented(runtime):
    runtime.session.scalar.return_value = False
    legacy = await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
    assert legacy.status == 200
    assert json.loads(legacy.body)["provider_profile"]["source_generations"] == {"cms_doctors": CMS_GENERATION}
    assert not runtime.session.info and snapshot.snapshot_cms_serving_generation("mrf") is None
