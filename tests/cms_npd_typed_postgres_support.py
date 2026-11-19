# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic typed CMS NPD releases in the guarded, disposable directory test database."""

import hashlib
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import text

from process.cms_npd_typed_schema import (
    FINISH_STATEMENTS,
    RELEASE_TABLE_STATEMENTS,
    TABLE_STATEMENTS,
    release_schema_name,
)
from tests.provider_directory_entities_postgres_support import directory_database

ORG_A = "1234567893"
ORG_B = "0000000b-0000-4000-8000-000000000001"
ORG_C = "org-control-char"
NET_ORG = "net-example"
NET_NESTED = "net-nested"  # named only by plan[].network
NET_DECLARED = "net-declared"  # declares the network type; nothing references it
SITE_1 = UUID("00000001-0000-4000-8000-000000000001")
SITE_2 = UUID("00000001-0000-4000-8000-000000000002")
SITE_MISSING = UUID("00000001-0000-4000-8000-0000000000ff")
PLAN_1 = UUID("00000002-0000-4000-8000-000000000001")
PLAN_2 = UUID("00000002-0000-4000-8000-000000000002")
PLAN_3 = UUID("00000002-0000-4000-8000-000000000003")
PLAN_MISSING = UUID("00000002-0000-4000-8000-0000000000ff")
ROLE_1 = UUID("00000003-0000-4000-8000-000000000001")
ROLE_2 = UUID("00000003-0000-4000-8000-000000000002")
ROLE_3 = UUID("00000003-0000-4000-8000-000000000003")
ROLE_4 = UUID("00000003-0000-4000-8000-000000000004")
NPI = 1234567893
NPI_NOT_EXPLICIT = 1245319599
NPI_WITH_OLD = 1999999984
MISSING_NPI = 1999999990


def organization_entity(organization_id):
    try:
        return str(UUID(organization_id))
    except ValueError:
        return str(UUID(hashlib.md5(f"cms-npd/Organization/{organization_id}".encode()).hexdigest()))


def network_entity(organization_id):
    return str(UUID(hashlib.md5(f"cms-npd/network/{organization_id}".encode()).hexdigest()))


_SEED = (
    """INSERT INTO organization (id, npi, name, active, part_of) VALUES
    ('1234567893', 1234567893, 'Example Health Organization', true, '0000000b-0000-4000-8000-000000000001'),
    ('0000000b-0000-4000-8000-000000000001', NULL, 'Example Parent Organization', true, 'missing-org'),
    ('org-control-char', NULL, E'Bad\\u0007Name', false, NULL),
    ('net-example', NULL, 'Example Network', true, NULL),
    ('net-nested', NULL, 'Example Nested Network', true, NULL)""",
    """INSERT INTO organization (id, name, active, type_code, other_type_codes) VALUES
    ('net-declared', 'Example Declared Network', true, 'prov', ARRAY['ntwk'])""",
    """INSERT INTO location (id, name, status, organization_id) VALUES
    ('00000001-0000-4000-8000-000000000001', 'Example Clinic', 'active', '1234567893'),
    ('00000001-0000-4000-8000-000000000002', 'Example Annex', 'suspended', 'missing-org')""",
    "UPDATE location SET part_of='00000001-0000-4000-8000-000000000001' WHERE name='Example Annex'",
    """INSERT INTO insurance_plan (id, name, status, owned_by, network_ids, period_start, period_end) VALUES
    ('00000002-0000-4000-8000-000000000001', 'Example Plan', 'active', '1234567893',
     ARRAY['net-example', 'net-missing'], '2026-01-01', '2026-12-31'),
    ('00000002-0000-4000-8000-000000000002', 'Example Draft Plan', 'draft', NULL, ARRAY['net-example'], NULL, NULL)""",
    """INSERT INTO insurance_plan (id, name, status, period_start, period_end, administered_by, coverage_area_ids,
        nested_references) VALUES
    ('00000002-0000-4000-8000-000000000003', 'Example Nested Plan', 'active', '2026-01-01', '2026-12-31', '1234567893',
     ARRAY['00000001-0000-4000-8000-000000000001', '00000001-0000-4000-8000-0000000000ff']::uuid[],
     '[{"field": "plan.network", "parent": 0, "ord": 0, "target": "net-nested",
        "period_start": "2026-03-01", "period_end": "2026-06-30"},
       {"field": "plan.coverageArea", "parent": 0, "ord": 0, "target": "00000001-0000-4000-8000-000000000002",
        "period_start": "2026-03-01", "period_end": "2026-06-30"},
       {"field": "coverage.network", "parent": 0, "ord": 0, "target": "net-missing",
        "period_start": "2026-01-01", "period_end": "2026-12-31"}]')""",
    """INSERT INTO practitioner (npi, active, family, npi_explicit, old_npis) VALUES
    (1234567893, true, 'Example', true, NULL),
    (1245319599, true, 'Example', false, NULL),
    (1999999984, true, 'Example', true, ARRAY[1003000126])""",
    """INSERT INTO practitioner_role (id, practitioner_npi, organization_id, active, location_ids, network_id,
        period_start, period_end) VALUES
    ('00000003-0000-4000-8000-000000000001', 1234567893, '1234567893', true,
     ARRAY['00000001-0000-4000-8000-000000000001', '00000001-0000-4000-8000-0000000000ff']::uuid[],
     'net-example', '2026-02-01', NULL),
    ('00000003-0000-4000-8000-000000000002', 1999999990, NULL, false, NULL, NULL, NULL, NULL)""",
    """INSERT INTO practitioner_role (id, practitioner_npi, active, insurance_plan_ids, network_ids,
        period_start, period_end) VALUES
    ('00000003-0000-4000-8000-000000000003', 1245319599, true,
     ARRAY['00000002-0000-4000-8000-000000000001', '00000002-0000-4000-8000-0000000000ff']::uuid[],
     ARRAY['net-example', 'net-missing'], '2026-04-01', '2026-05-31'),
    ('00000003-0000-4000-8000-000000000004', 1999999984, true, NULL, NULL, NULL, NULL)""",
)


async def load_release(session, release_id, status="current", seed=_SEED):
    """Create, seed and finish one release schema, then register it in the control table."""
    schema = release_schema_name(release_id)
    control = await session.scalar(text("SELECT current_schema()"))
    await session.execute(text(f'CREATE SCHEMA "{schema}"'))
    await session.execute(text(f'SET LOCAL search_path TO "{schema}"'))
    for statement in (*TABLE_STATEMENTS, *seed, *FINISH_STATEMENTS):
        await session.execute(text(statement))
    await session.execute(text(f'SET LOCAL search_path TO "{control}"'))
    await session.execute(
        text("""INSERT INTO cms_npd_release (release_id, schema_name, status, published_at, row_counts,
            unresolved_counts, files) VALUES (:release_id, :schema, :status, :published_at, '{}', '{}', '{}')"""),
        {"release_id": release_id, "schema": schema, "status": status, "published_at": datetime.now(timezone.utc)},
    )
    return schema


@asynccontextmanager
async def typed_cms_database(monkeypatch):
    """Yield sessions over a control schema holding one current typed release."""
    async with directory_database(monkeypatch) as sessions:
        control = os.environ["HLTHPRT_DB_SCHEMA"]
        try:
            async with sessions() as session, session.begin():
                for statement in RELEASE_TABLE_STATEMENTS:
                    await session.execute(text(statement))
                await load_release(session, secrets.token_hex(32))
            yield sessions
        finally:
            async with sessions() as session, session.begin():
                exists = await session.scalar(
                    text("SELECT to_regclass(:name)"), {"name": f'"{control}".cms_npd_release'}
                )
                release_schemas = (
                    (await session.scalars(text(f'SELECT schema_name FROM "{control}".cms_npd_release'))).all()
                    if exists
                    else ()
                )
                for schema in release_schemas:
                    await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
