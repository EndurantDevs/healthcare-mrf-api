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

from api import provider_profile as profile
from api import provider_profile_cms as cms
from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from process import cms_doctors_preparation as preparation
from process import reference_family_result_generation as generation
from process import reference_source_generation as revisions
from tests.test_provider_profile_cms import CMS_GENERATION, NPI, _credential_row, _education_row

native = importlib.import_module("process.cms_doctors")
RELATIONS = generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
OIDS = (21001, 21002, 21003)


@pytest.fixture
def runtime(monkeypatch):
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
    session = SimpleNamespace(info={}, execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value="installed"))
    database._transaction_binding = lambda: state_by_field.get("binding")

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
                local_generation=params["next_generation"],
                origin_generation=params["next_generation"],
                relation_oids=list(params["relation_oids"]),
            )
        return state_by_field["authority"]

    database.transaction, database.all = transaction, rows
    monkeypatch.setattr(cms, "db", database)
    monkeypatch.setattr(npi, "db", database)
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(npi, "_runtime_db_schema", lambda: "mrf")
    monkeypatch.setattr(profile, "fetch_state_profile_projection", AsyncMock(return_value=None))
    monkeypatch.setattr(profile, "fetch_additional_state_profile_projections", AsyncMock(return_value=[]))
    monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", AsyncMock(return_value={}))
    oid_by_relation = {f"mrf.{name}": oid for name, oid in zip(RELATIONS, OIDS, strict=True)}
    oid_by_relation.update(
        {f"mrf.{generation.TABLE_NAME}": 22000, f"mrf.{snapshot.address_generation.TABLE_NAME}": None}
    )
    monkeypatch.setattr(snapshot, "_lock_serving_relations", AsyncMock(return_value=oid_by_relation))
    monkeypatch.setattr(snapshot, "_requires_cms_receipt", AsyncMock(return_value=False))
    monkeypatch.setattr(generation, "_first", first)
    monkeypatch.setattr(generation, "current_reference_family_relation_oids", AsyncMock(return_value=OIDS))
    monkeypatch.setattr(revisions, "install_reference_revision_guards", AsyncMock())
    return SimpleNamespace(state_by_field=state_by_field, database=database, session=session)


async def republish(runtime, monkeypatch, mode):
    """Run each publication entry point through the actual shared authority update."""
    monkeypatch.setattr(native, "_lock_cms_doctors_publication", AsyncMock())
    monkeypatch.setattr(native, "swap_education_stage", AsyncMock())
    monkeypatch.setattr(native, "swap_group_site_stage", AsyncMock())
    stage = native.make_class(native.DoctorClinicianAddress, "synthetic")
    if mode == "ordinary":
        return await native._publish_cms_doctors_stage(stage, "mrf", "synthetic")
    monkeypatch.setattr(preparation.archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(preparation.archive, "_verify_incumbent", AsyncMock())
    monkeypatch.setattr(native, "lock_live_serving_relations", AsyncMock())
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
        return await preparation.apply_prepared_cms_doctors_generation(prepared)


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


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "oids", "source", "schema"])
async def test_snapshot_and_source_drift_still_refuse(runtime, monkeypatch, fault):
    if fault == "missing":
        runtime.state_by_field["authority"] = None
    elif fault == "oids":
        runtime.state_by_field["authority"]["relation_oids"][2] += 1
    elif fault == "source":
        runtime.state_by_field["credentials"] = [_credential_row(1, "MD", generation="d" * 64)]
    else:
        monkeypatch.setattr(cms.CMSDoctorEducation.__table__, "schema", "synthetic")
    error, reason = {
        "missing": (RuntimeError, "^reference family generation authority is unavailable$"),
        "source": (RuntimeError, "^cms_education_profile_generation_mixed$"),
        "oids": (ServiceUnavailable, r"^Provider data is temporarily unavailable\.$"),
        "schema": (ServiceUnavailable, r"^Provider data is temporarily unavailable\.$"),
    }[fault]
    with pytest.raises(error, match=reason):
        await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
    assert snapshot.snapshot_cms_serving_generation("mrf") is None
    assert not runtime.session.info and runtime.database._transaction_binding() is None


@pytest.mark.asyncio
async def test_legacy_no_publication_identity_is_not_invented(runtime):
    runtime.session.scalar.return_value = False
    legacy = await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
    assert legacy.status == 200
    assert json.loads(legacy.body)["provider_profile"]["source_generations"] == {"cms_doctors": CMS_GENERATION}
    assert not runtime.session.info and snapshot.snapshot_cms_serving_generation("mrf") is None
