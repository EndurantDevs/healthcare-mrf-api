# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seed retained legacy custody through its original native guards, after real SOURCE admission."""

from collections import defaultdict

from process import provider_directory_cms_serving_coverage as coverage
from process.provider_directory_generic_source_policy import bind_generic_source_transaction_factory
from tests import cms_npd_admission_postgres_support as support

_HEADER = "provider_directory_endpoint_dataset"
_COVERAGE = "provider_directory_cms_candidate_coverage"
_RETAINED_TABLES = (
    "provider_directory_api_endpoint",
    "provider_directory_source",
    "provider_directory_organization_identity",
    "provider_directory_site_identity",
    "provider_directory_insurance_network_identity",
    "provider_directory_resource_identity",
    _HEADER,
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
    "provider_directory_dataset_resource",
    "provider_directory_cms_npd_resource_witness",
    "provider_directory_cms_npd_relationship",
    "provider_directory_cms_npd_relationship_receipt",
    _COVERAGE,
)
_LOADING_NULLS = frozenset(
    (
        "dataset_hash",
        "validated_at",
        "published_at",
        "superseded_at",
        "publication_metadata_summary_json",
        "publication_metadata_sha256",
        "content_proof_admission_version",
        "content_proof_admission_kind",
        "content_proof_admission_sha256",
        "content_proof_resource_types",
        "artifact_selection_receipt_json",
        "completion_proof_json",
        "completion_proof_sha256",
    )
)


def loading_header(columns, accepted):
    """Keep the producer's terminal receipt intact while opening an acquiring seed."""
    header_by_field = dict(zip(columns, accepted, strict=True))
    assert header_by_field["status"] == "validated" and header_by_field["is_current"] is False
    header_by_field.update({name: None for name in _LOADING_NULLS if name in header_by_field})
    header_by_field.update(status="acquiring", is_current=False, resource_count=0)
    return tuple(header_by_field[name] for name in columns)


async def _legacy_columns(database):
    columns_by_table = defaultdict(list)
    async with database.session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        rows = await driver.fetch(
            "SELECT c.relname,a.attname,t.typname FROM pg_class c "
            "JOIN pg_attribute a ON a.attrelid=c.oid JOIN pg_type t ON t.oid=a.atttypid "
            "WHERE c.relnamespace='mrf'::regnamespace AND c.relname=ANY($1::text[]) "
            "AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated='' "
            "ORDER BY c.relname,a.attnum",
            list(_RETAINED_TABLES),
        )
        for retained_row in rows:
            columns_by_table[retained_row["relname"]].append((retained_row["attname"], retained_row["typname"]))
    assert set(columns_by_table) == set(_RETAINED_TABLES)
    return dict(columns_by_table)


def _projection(columns):
    return ",".join(f'"{name}"' + ("::text" if kind in {"json", "jsonb"} else "") for name, kind in columns)


async def _completed_source(monkeypatch, directory, columns_by_table):
    from tests.test_cms_npd_candidate_coverage_postgres import _stage

    with monkeypatch.context() as producer_settings:
        async with support.protected_admission_database(producer_settings) as administrator:
            component = administrator._source_component
            async with bind_generic_source_transaction_factory(component.factory):
                candidate, release, digest = await _stage(producer_settings, directory)
                await coverage.prepare_cms_candidate_coverage(support.fhir, candidate, release, digest)
            async with component.admin_engine.connect() as observer:
                driver = (await observer.get_raw_connection()).driver_connection
                rows_by_table = {
                    table: await driver.fetch(f'SELECT {_projection(columns_by_table[table])} FROM "mrf"."{table}"')
                    for table in _RETAINED_TABLES
                }
    assert len(rows_by_table[_HEADER]) == 1
    return candidate, release, digest, rows_by_table


async def _terminal_header(driver, candidate, header_columns, accepted_values):
    columns = tuple(name for name, _kind in header_columns)
    setters = ",".join(f'"{name}"=${index}' for index, name in enumerate(columns, 1))
    assert (
        await driver.execute(
            f'UPDATE "mrf"."{_HEADER}" SET {setters} WHERE dataset_id=${len(columns) + 1}',
            *accepted_values,
            candidate.dataset_id,
        )
        == "UPDATE 1"
    )


async def stage_legacy_retained_source(monkeypatch, directory, database):
    """Copy genuine completed rows through legacy FK/triggers; never load through retired writers."""
    columns_by_table = await _legacy_columns(database)
    candidate, release, digest, rows_by_table = await _completed_source(monkeypatch, directory, columns_by_table)
    async with database.engine.begin() as connection:
        driver = (await connection.get_raw_connection()).driver_connection
        for table in _RETAINED_TABLES:
            if table == _COVERAGE:
                continue
            columns = tuple(name for name, _kind in columns_by_table[table])
            retained_rows = [tuple(retained_row) for retained_row in rows_by_table[table]]
            if table == _HEADER:
                retained_rows = [loading_header(columns, retained_rows[0])]
            assert (
                await driver.copy_records_to_table(table, schema_name="mrf", columns=columns, records=retained_rows)
                == f"COPY {len(retained_rows)}"
            )
        await _terminal_header(driver, candidate, columns_by_table[_HEADER], rows_by_table[_HEADER][0])
        retained_rows = [tuple(retained_row) for retained_row in rows_by_table[_COVERAGE]]
        assert len(retained_rows) == 1
        assert (
            await driver.copy_records_to_table(
                _COVERAGE,
                schema_name="mrf",
                columns=tuple(name for name, _kind in columns_by_table[_COVERAGE]),
                records=retained_rows,
            )
            == "COPY 1"
        )
        for table in _RETAINED_TABLES:
            copied = await driver.fetch(f'SELECT {_projection(columns_by_table[table])} FROM "mrf"."{table}"')
            assert sorted(repr(tuple(retained_row)) for retained_row in copied) == sorted(
                repr(tuple(retained_row)) for retained_row in rows_by_table[table]
            )
    return candidate, release, digest


async def assert_legacy_candidate_coverage(database, candidate, release, digest):
    """Check the retained genuine seal without calling the retired legacy writer."""
    proof_by_field = dict(
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
        release_id=release,
        dataset_hash=digest,
        proof_version=2,
    )
    async with database.transaction() as session:
        await coverage.assert_sealed_cms_candidate_coverage(session, "mrf", proof_by_field)
    return proof_by_field


async def publish_legacy_retained_source(database, candidate, release, digest):
    """Seed the legacy published state using its native transition and immutable coverage guards."""
    fhir = support.fhir
    async with database.transaction() as session:
        await coverage.validate_cms_candidate_coverage(session, candidate.dataset_id, release)
        assert (
            await database.status(
                'UPDATE "mrf".provider_directory_endpoint_dataset '
                "SET status='published',is_current=true,published_at=transaction_timestamp() "
                "WHERE dataset_id=:dataset AND status='validated' AND NOT is_current",
                dataset=candidate.dataset_id,
            )
            == 1
        )
        await coverage.seal_cms_candidate_coverage(fhir, candidate, release, digest)
