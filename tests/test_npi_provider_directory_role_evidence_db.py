# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

import pytest

from api.endpoint import npi as npi_module
from db.models import db


def _requires_test_database():
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database.lower():
        pytest.skip("DB-backed role-evidence SQL test requires a disposable test database")


@pytest.mark.asyncio(loop_scope="module")
async def test_provider_directory_role_evidence_sql_explains_with_array_binds():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    sql = npi_module._provider_directory_role_evidence_sql(schema, has_catalog=True)

    plan = await db.all(
        f"EXPLAIN {sql}",
        source_ids=["pdfhir_execution_test"],
        role_ids=["practitioner-role-execution-test"],
    )

    assert plan
