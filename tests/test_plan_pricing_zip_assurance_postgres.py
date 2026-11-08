# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete native address multiset parity with the existing assurance predicate."""

from collections import Counter
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api import ptg2_geo_projection as assurance
from api import ptg2_serving as serving
from tests import test_plan_pricing_code_postgres as fixture
from tests.ptg2_serving_address_evidence_postgres_support import (
    _create_cms_address_table,
    _create_mrf_address_table,
    _create_npi_address_table,
    _create_unified_address_table,
)
from tests.test_ptg2_serving_billing_address_lineage import _location_query

_TABLES = ("entity_address_unified", "npi_address", "mrf_address", "doctor_clinician_address")
_CELLS = ["48104"]


_ADDRESS_RECORDS = [
    ("stored-zero", 1, "practice", 1, 1, 0, "48104", "48104"),
    ("stored-nppes", 2, "primary", 0, 1, 1, "48104", "48104"),
    ("stored-mrf", 3, "secondary", 0, 1, 2, "48104", "48104"),
    ("stored-cms", 4, "site", 0, 1, 3, "48104", "48104"),
    ("stale-nppes", 5, "practice", 1, 2, 1, "48104", "48104"),
    ("invalid-mrf", 6, "primary", 2, 1, 9, "48104", "48104"),
    ("missing-version-cms", 7, "secondary", 4, None, 3, "48104", "48104"),
    ("missing-class-nppes", 8, "site", 1, 1, None, "48104", "48104"),
    ("no-lineage", 9, "practice", 2, None, None, "48104", "48104"),
    ("nppes-precedence", 10, "practice", 7, None, None, "48104", "48104"),
    ("cms-incomplete-anchor", 11, "site", 4, None, None, "48104", "48104"),
    ("null-key", 12, "primary", 1, None, None, "48104", "48104"),
    ("cms-incomplete-doctor", 13, "practice", 4, None, None, "48104", "48104"),
    ("cms-null-premise", 14, "practice", 4, None, None, "48104", "48104"),
    ("duplicate-a", 15, "site", 1, None, None, "48104", "48104"),
    ("duplicate-b", 15, "site", 1, None, None, "48104", "48104"),
    ("postal-fallback", 16, "practice", 1, None, None, None, "48104-1234"),
    ("billing", 17, "billing", 1, None, None, "48104", "48104"),
    ("null-npi", None, "practice", 1, None, None, "48104", "48104"),
    ("zero-npi", 0, "practice", 1, None, None, "48104", "48104"),
    ("other-zip", 18, "practice", 1, None, None, "48105", "48105"),
    ("mrf-precedence", 20, "site", 6, None, None, "48104", "48104"),
    ("blank-zip", 21, "practice", 1, None, None, "", "48104"),
]


async def _insert_addresses(session):
    """Seed all target addresses, including duplicate rows and incomplete identities."""
    for index, (name, npi, kind, mask, version, evidence_source_id, zip5, postal) in enumerate(_ADDRESS_RECORDS):
        # The duplicate addresses deliberately share every classifier input.
        key = "duplicate" if name.startswith("duplicate-") else name
        await session.execute(
            text("""INSERT INTO mrf.entity_address_unified
            (location_key,npi,type,address_key,premise_key,address_source_mask,geo_assurance_version,
             geo_evidence_source_id,zip5,postal_code,checksum)
            VALUES(:name,:npi,:kind,md5(CAST(:key AS text))::uuid,md5(CAST(:key AS text)||'-premise')::uuid,
             :mask,:version,:source,:zip,:postal,:checksum)"""),
            {
                "name": name,
                "npi": npi,
                "kind": kind,
                "key": key,
                "mask": mask,
                "version": version,
                "source": evidence_source_id,
                "zip": zip5,
                "postal": postal,
                "checksum": index,
            },
        )
    await session.execute(text("UPDATE mrf.entity_address_unified SET address_key=NULL WHERE location_key='null-key'"))
    await session.execute(
        text("UPDATE mrf.entity_address_unified SET premise_key=NULL WHERE location_key='cms-null-premise'")
    )
    await session.execute(
        text("""INSERT INTO mrf.npi_address(npi,address_key,date_added)
        SELECT npi,address_key,'2026-01-01'::date FROM mrf.entity_address_unified
        WHERE npi IN (1,5,8,10,15,16,17,18,21)""")
    )
    # Evidence duplicates must never multiply the complete address population.
    await session.execute(text("INSERT INTO mrf.npi_address SELECT * FROM mrf.npi_address WHERE npi IN (5,15)"))


async def _insert_evidence(session):
    """Keep outside-ZIP anchors and duplicate evidence in the native fixture."""
    for name in ("invalid-mrf", "no-lineage", "nppes-precedence", "mrf-precedence"):
        await session.execute(
            text("""INSERT INTO mrf.mrf_address(npi,address_key,source_issuer_names,date_added)
            SELECT npi,address_key,ARRAY['Issuer A','Issuer B']::varchar[],
              CASE WHEN location_key='no-lineage' THEN NULL ELSE '2026-01-01'::date END
            FROM mrf.entity_address_unified WHERE location_key=:name"""),
            {"name": name},
        )
    await session.execute(text("INSERT INTO mrf.mrf_address SELECT * FROM mrf.mrf_address WHERE npi=6"))
    for npi in (7, 10, 11, 13, 14, 20):
        await session.execute(
            text("""INSERT INTO mrf.entity_address_unified
            (location_key,npi,type,address_key,premise_key,address_source_mask,zip5,postal_code,checksum)
            SELECT 'anchor-'||npi,npi,'primary',md5('anchor-'||npi)::uuid,premise_key,1,'99999','99999',100
            FROM mrf.entity_address_unified WHERE npi=:npi"""),
            {"npi": npi},
        )
    await session.execute(
        text("""INSERT INTO mrf.npi_address(npi,address_key,date_added)
        SELECT npi,address_key,CASE WHEN npi=11 THEN NULL ELSE '2026-01-01'::date END
        FROM mrf.entity_address_unified WHERE location_key LIKE 'anchor-%'""")
    )
    await session.execute(
        text("""INSERT INTO mrf.doctor_clinician_address(npi,address_key,updated_at)
        SELECT npi,address_key,CASE WHEN npi=13 THEN NULL ELSE '2026-01-01'::timestamp END
        FROM mrf.entity_address_unified WHERE npi IN (7,10,11,13,14,20) AND location_key NOT LIKE 'anchor-%'""")
    )
    await session.execute(
        text("INSERT INTO mrf.doctor_clinician_address SELECT * FROM mrf.doctor_clinician_address WHERE npi=7")
    )


async def _source(db):
    async def status(sql):
        async with db.sessions() as session, session.begin():
            await session.execute(text(sql))

    database = SimpleNamespace(status=status)
    for create in (
        _create_unified_address_table,
        _create_npi_address_table,
        _create_mrf_address_table,
        _create_cms_address_table,
    ):
        await create(database, "mrf")
    async with db.sessions() as session, session.begin():
        # Valid stored classes take precedence, including zero even with usable legacy evidence.
        await _insert_addresses(session)
        await _insert_evidence(session)


def _located():
    zip5 = serving._ptg2_address_zip5_sql("addr", unified=True)
    return f"""SELECT addr.*,{zip5} AS geo_cell,addr.checksum::double precision AS distance_miles
        FROM mrf.entity_address_unified addr WHERE addr.type IN ('practice','primary','secondary','site')
        AND addr.npi>0 AND {zip5}=ANY(CAST(:cells AS varchar[]))"""


def _assert_complete_classes(classified, projection_available):
    """Check precedence and exclusions independently of address-multiset parity."""
    names = {address["location_key"] for address in classified}
    assert {
        "stale-nppes",
        "invalid-mrf",
        "missing-version-cms",
        "missing-class-nppes",
        "nppes-precedence",
        "duplicate-a",
        "duplicate-b",
        "postal-fallback",
        "mrf-precedence",
    } <= names
    assert (
        not {
            "billing",
            "null-npi",
            "zero-npi",
            "other-zip",
            "no-lineage",
            "null-key",
            "cms-incomplete-anchor",
            "cms-incomplete-doctor",
            "cms-null-premise",
            "blank-zip",
        }
        & names
    )
    assert ("stored-zero" in names) is (projection_available is not True)
    for name in ("stored-nppes", "stored-mrf", "stored-cms"):
        assert (name in names) is (projection_available is True)
    levels_by_name = {address["location_key"]: address["geo_evidence_level"] for address in classified}
    assert levels_by_name["nppes-precedence"] == "nppes_registry_address"
    assert levels_by_name["mrf-precedence"] == "multi_issuer_marketplace_address"
    assert levels_by_name["missing-version-cms"] == "cms_doctors_source_with_nppes_identity_anchor"
    assert (
        Counter(
            (address["npi"], address["geo_cell"], address["type"], address["geo_evidence_level"])
            for address in classified
        )[(15, "48104", "site", "nppes_registry_address")]
        == 2
    )


async def _assert_nearest_consistent(session, direct, parameters_by_name):
    """The ordinary reader keeps its existing single-office ranking."""
    # Execute the unchanged ordinary renderer; only its nearest-address tail
    # chooses one office. The complete classifier above keeps both duplicates.
    context = replace(
        _location_query(address_assurance_sql="required"),
        npi_scope_table="(SELECT DISTINCT npi FROM mrf.entity_address_unified)",
        filter_sql="addr.npi>0 AND addr.type IN ('practice','primary','secondary','site') AND "
        + serving._ptg2_address_zip5_sql("addr", unified=True)
        + "=ANY(CAST(:cells AS varchar[]))",
        distance_sql="addr.checksum::double precision",
    )
    nearest = (
        (
            await session.execute(
                text(serving._membership_location_sql(context, limit=100, offset=0)), parameters_by_name
            )
        )
        .mappings()
        .all()
    )
    expected_by_npi = {}
    for address in sorted(
        direct,
        key=lambda r: (r["checksum"], {"practice": 0, "primary": 1}.get(r["type"], 2), r["location_key"]),
    ):
        expected_by_npi.setdefault(address["npi"], address)
    assert {address["npi"]: (address["checksum"], address["_geo_evidence_level"]) for address in nearest} == {
        npi: (address["checksum"], address["geo_evidence_level"]) for npi, address in expected_by_npi.items()
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("projection_available", [True, False, None])
async def test_complete_assurance_matches_direct_predicate_and_nearest(monkeypatch, projection_available):
    # Dependency-signature validation is covered separately; both real SQL classifiers
    # receive the same valid, stale or absent projection-availability input here.
    monkeypatch.setattr(assurance, "projection_state_available_sql", lambda _schema: ":projection_available")
    monkeypatch.setattr(fixture, "_SOURCE_TABLES", fixture._SOURCE_TABLES + _TABLES)
    async with fixture._database(monkeypatch) as db:
        await _source(db)
        parameters_by_name = {"cells": _CELLS, "projection_available": projection_available, "limit": 100, "offset": 0}
        async with db.sessions() as session, session.begin():
            direct_sql = f"""WITH raw AS ({_located()}) SELECT raw.location_key,raw.npi,raw.geo_cell,raw.type,
                {serving._ptg2_geo_evidence_level_sql("raw")} AS geo_evidence_level,raw.checksum
                FROM raw WHERE ({serving._ptg2_geo_evidence_level_sql("raw")}) IS NOT NULL"""
            direct = (await session.execute(text(direct_sql), parameters_by_name)).mappings().all()
            new_sql = serving._complete_unified_address_assurance_ctes_sql(_located())
            classified = (
                (
                    await session.execute(
                        text(
                            new_sql
                            + " SELECT location_key,npi,geo_cell,type,geo_evidence_level,checksum FROM classified_locations WHERE geo_evidence_level IS NOT NULL"
                        ),
                        parameters_by_name,
                    )
                )
                .mappings()
                .all()
            )
            assert Counter(tuple(address.values()) for address in classified) == Counter(
                tuple(address.values()) for address in direct
            )
            _assert_complete_classes(classified, projection_available)
            await _assert_nearest_consistent(session, direct, parameters_by_name)
