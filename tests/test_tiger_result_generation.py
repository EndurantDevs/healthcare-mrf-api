# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from db.tiger_models import Zip_zcta5, ZipState
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.tiger_result_generation import publish_tiger_generation


def test_tiger_family_matches_installed_native_shapes():
    assert archive.reference_family_spec("tiger").table_names == ("zip_state", "zcta5")
    assert tuple(ZipState.__table__.primary_key.columns.keys()) == ("zip", "stusps")
    assert tuple(Zip_zcta5.__table__.primary_key.columns.keys()) == ("zcta5ce", "statefp")
    dialect = postgresql.dialect()
    assert str(ZipState.__table__.c.statefp.type.compile(dialect=dialect)) == "VARCHAR(2)"
    assert str(Zip_zcta5.__table__.c.gid.type.compile(dialect=dialect)) == "INTEGER"
    assert str(Zip_zcta5.__table__.c.aland.type.compile(dialect=dialect)) == "DOUBLE PRECISION"
    assert str(Zip_zcta5.__table__.c.the_geom.type.compile(dialect=dialect)) == "geometry(MultiPolygon,4269)"
    assert Zip_zcta5.__table__.c.gid.default.name == "zcta5_gid_seq"


def test_only_tiger_routes_authority_to_application_schema(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "application")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    assert generation._authority_schema("tiger", "tiger") == "application"
    assert generation._authority_schema("cms-doctors", "other") == "other"
    with pytest.raises(ValueError, match="serving schema"):
        generation._authority_schema("tiger", "other")
    monkeypatch.setenv("DB_SCHEMA", "different")
    with pytest.raises(ValueError, match="same schema"):
        generation._authority_schema("tiger", "tiger")
    monkeypatch.setenv("DB_SCHEMA", "tiger")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tiger")
    with pytest.raises(ValueError, match="separate application schema"):
        generation._authority_schema("tiger", "tiger")


@pytest.mark.asyncio
async def test_tiger_publication_requires_the_callers_transaction_before_sql():
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="transaction"):
        await publish_tiger_generation(
            SimpleNamespace(in_transaction=lambda: False),
            ownership=None,
            manifest=None,
            expected_incumbent=None,
            validation_receipt=None,
            cutover=None,
        )
