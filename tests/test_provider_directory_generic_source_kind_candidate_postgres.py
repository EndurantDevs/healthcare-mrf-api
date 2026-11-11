# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native closed-set content and relationship checks before rooted attachment."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import provider_directory_generic_source_kind_candidate as candidate
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.formulary_fhir_twin_admission_pg_support import connect, database_url, quoted


@pytest.fixture
async def closed_candidate():
    connection = await connect(database_url())
    name = "candidate_kind_test_" + uuid4().hex
    schema = quoted(name)
    try:
        await connection.execute(f"CREATE SCHEMA {schema}")
        await connection.execute(
            f"CREATE TABLE {schema}.resources(dataset_id text NOT NULL,resource_type text NOT NULL,"
            "resource_id text NOT NULL,payload_hash text NOT NULL,payload_json jsonb NOT NULL,"
            "acquired_resource_sha256 text,PRIMARY KEY(dataset_id,resource_type,resource_id));"
            f"CREATE TABLE {schema}.evidence(dataset_id text NOT NULL,resource_type text NOT NULL,"
            "resource_id text NOT NULL,published_payload_hash text NOT NULL,root_dataset_id text NOT NULL,"
            "publication_acquisition_id text NOT NULL,PRIMARY KEY(dataset_id,resource_type,resource_id));"
            f"CREATE TABLE {schema}.provider_directory_rooted_graph_dataset(dataset_id text PRIMARY KEY,"
            "root_dataset_id text NOT NULL,publication_acquisition_id text NOT NULL);"
            f"CREATE TABLE {schema}.provider_directory_endpoint_dataset(dataset_id text PRIMARY KEY);"
            f"CREATE TABLE {schema}.provider_directory_rooted_graph_acquisition(acquisition_id text PRIMARY KEY);"
            f"INSERT INTO {schema}.resources VALUES('candidate','Practitioner','root',repeat('a',64),"
            '\'{"resource_id":"root"}\',NULL);'
            f"INSERT INTO {schema}.evidence VALUES('candidate','Practitioner','root',repeat('a',64),'root-dataset','acquisition');"
            f"INSERT INTO {schema}.provider_directory_rooted_graph_dataset VALUES('candidate','root-dataset','acquisition');"
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset VALUES('candidate');"
            f"INSERT INTO {schema}.provider_directory_rooted_graph_acquisition VALUES('acquisition')"
        )
        yield SimpleNamespace(
            connection=connection,
            schema=name,
            relations={candidate.RESOURCE: "resources", candidate._PROVENANCE["rooted"]: "evidence"},
        )
    finally:
        try:
            await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            assert await connection.fetchval("SELECT to_regnamespace($1)", name) is None
        finally:
            await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "UPDATE evidence SET published_payload_hash=repeat('b',64)",
        "UPDATE evidence SET root_dataset_id='other-root'",
        "UPDATE evidence SET publication_acquisition_id='other-acquisition'",
        "DELETE FROM resources",
        "DELETE FROM evidence",
        "UPDATE resources SET payload_json='{}'",
    ],
)
async def test_native_closed_content_rejects_provenance_and_resource_drift(closed_candidate, damage):
    case = closed_candidate
    await candidate._validate_content(case.connection, case.schema, case.relations, "candidate", "rooted")
    async with case.connection.transaction():
        await case.connection.execute(f"SET LOCAL search_path={quoted(case.schema)},pg_catalog")
        await case.connection.execute(damage)
        with pytest.raises(GenericCandidateError, match="candidate_content_changed"):
            await candidate._validate_content(case.connection, case.schema, case.relations, "candidate", "rooted")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing", ["provider_directory_endpoint_dataset", "provider_directory_rooted_graph_acquisition"]
)
async def test_native_set_anti_join_rejects_dangling_dataset_or_acquisition(closed_candidate, missing):
    case = closed_candidate
    await candidate._validate_relationships(case.connection, case.schema, case.relations, "candidate")
    await case.connection.execute(f"DELETE FROM {quoted(case.schema)}.{quoted(missing)}")
    with pytest.raises(GenericCandidateError, match="provenance_relationship_changed"):
        await candidate._validate_relationships(case.connection, case.schema, case.relations, "candidate")
