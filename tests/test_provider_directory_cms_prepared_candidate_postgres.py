# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native data composition only; disposable grants do not issue CMS authority."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateColumn

from db import models
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_candidate as candidate
from process import provider_directory_cms_prepared_context as context
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_admission_seal import admission_seal_from_validated_metadata
from process.provider_directory_cms_prepared_allocation import allocate_prepared_cms_candidate
from process.provider_directory_cms_prepared_workspace import reopen_prepared_cms_workspace
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_cms_prepared_identity_publication_postgres import (
    CANONICAL_ROOTS,
    assert_complete_copy,
    private_snapshot,
    publication_case,
)
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    full_private_case,
    native_case,
    native_derived_case,
    native_frozen_case,
    observe,
    publisher,
)
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    prepare as prepare_outputs,
)
from tests.test_provider_directory_cms_prepared_payload_publication_postgres import payload_case, target_counts
from tests.test_provider_directory_cms_prepared_relationships_postgres import input_snapshots


def _definition(column):
    return str(CreateColumn(Column(column.name, column.type, nullable=column.nullable)).compile(dialect=dialect()))


def _admitted_header(case, document):
    metadata = deepcopy(document["identity"]["metadata"])
    counts_by_type = {kind: len(resource_list) for kind, resource_list in case.raw.items()}
    metadata["provider_directory_content_proof_v1"] = {
        "dataset_hash": document["admission"]["dataset_hash"],
        "resource_count": sum(counts_by_type.values()),
        "resource_counts": counts_by_type,
        "resource_hashes": {kind: "a" * 64 for kind in counts_by_type},
        "proof_sha256": document["admission"]["content_proof_admission_sha256"],
        "shards": [{"resource_count": sum(counts_by_type.values()), "resource_counts": counts_by_type}],
    }
    seal = admission_seal_from_validated_metadata(metadata)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    header = dict.fromkeys(candidate._HEADER_COLUMNS)
    header.update(document["admission"])
    header.update({key: document["identity"][key] for key in ("dataset_id", "endpoint_id", "acquisition_root_run_id")})
    header.update(
        status="validated",
        is_current=False,
        created_at=now,
        validated_at=now,
        publication_metadata_json=metadata,
        publication_metadata_summary_json=seal.metadata_summary,
        publication_metadata_sha256=seal.metadata_sha256,
        content_proof_admission_version=seal.admission_version,
        content_proof_admission_kind=seal.admission_kind,
        content_proof_resource_types=list(seal.resource_types),
        candidate_storage_json=document,
    )
    return header


async def _complete_frozen_header(case):
    document = case.handoff.document
    relation = storage_relation(case.storage.storage_schema, document["tables"][storage.HEADER]["name"])
    header = _admitted_header(case, document)
    existing_column_set = {"dataset_id", "acquisition_root_run_id", "import_run_id"}
    for column in models.ProviderDirectoryEndpointDataset.__table__.columns:
        if column.name not in existing_column_set:
            definition = str(CreateColumn(Column(column.name, column.type)).compile(dialect=dialect()))
            await case.admin.execute(f"ALTER TABLE {relation} ADD COLUMN {definition}")
    await case.admin.execute(f"DELETE FROM {relation}")
    fields = ",".join(storage_identifier(key) for key in candidate._HEADER_COLUMNS)
    parameters = ",".join("$" + str(index) for index in range(1, len(header) + 1))
    await case.admin.execute(
        f"INSERT INTO {relation}({fields}) VALUES({parameters})",
        *(
            storage._encoded(header[key]) if key in candidate._JSON_COLUMNS and header[key] is not None else header[key]
            for key in candidate._HEADER_COLUMNS
        ),
    )
    for column in models.ProviderDirectoryEndpointDataset.__table__.columns:
        if not column.nullable:
            await case.admin.execute(
                f"ALTER TABLE {relation} ALTER COLUMN {storage_identifier(column.name)} SET NOT NULL"
            )
    case.handoff = FHIRSourceCandidateHandoff(storage._encoded(document))
    native_rows = await case.admin.fetch(
        context._INVENTORY_SQL, [native_row["relation_oid"] for native_row in case.inventory["relations"]]
    )
    case.inventory["relations"] = [
        {key: column_value for key, column_value in dict(native_row).items() if key != "supported"}
        for native_row in native_rows
    ]
    return relation


async def _canonical_controls(case, frozen):
    schema = storage_identifier(case.storage.catalog_schema)
    header = storage_relation(case.storage.catalog_schema, storage.HEADER)
    columns = ",".join(_definition(column) for column in models.ProviderDirectoryEndpointDataset.__table__.columns)
    await case.admin.execute(f"CREATE TABLE {header}({columns},PRIMARY KEY(dataset_id))")
    await case.admin.execute(f"CREATE UNIQUE INDEX one_current ON {header}(endpoint_id) WHERE is_current")
    await case.admin.execute(
        f"CREATE TABLE {schema}.{candidate._RELATIONSHIP_RECEIPT}(dataset_id varchar(96) PRIMARY KEY,"
        "release_id varchar(64) NOT NULL CHECK(release_id ~ '^[0-9a-f]{64}$'),"
        "projection_contract varchar(64) NOT NULL CHECK(projection_contract='cms-npd-reference-ledger-v1'),"
        "relationship_count bigint NOT NULL CHECK(relationship_count>=0))"
    )
    await case.admin.execute(
        f"CREATE TABLE {schema}.{candidate.coverage._CANDIDATE_TABLE}(dataset_id varchar(96) NOT NULL,"
        "endpoint_id varchar(96) NOT NULL,release_id varchar(64) NOT NULL,dataset_hash varchar(64) NOT NULL,"
        "proof_version smallint NOT NULL CHECK(proof_version=2),admission_sha256 varchar(64) NOT NULL,"
        "metadata_sha256 varchar(64) NOT NULL,relationship_count bigint NOT NULL CHECK(relationship_count>=0),"
        "created_at timestamptz NOT NULL,PRIMARY KEY(dataset_id,release_id,proof_version))"
    )
    for root, lock_column in (
        (storage.HEADER, "status"),
        (candidate._RELATIONSHIP_RECEIPT, "relationship_count"),
        (candidate.coverage._CANDIDATE_TABLE, "created_at"),
    ):
        relation = storage_relation(case.storage.catalog_schema, root)
        await case.admin.execute(f'ALTER TABLE {relation} OWNER TO "{case.roles["namespace"]}"')
        await case.admin.execute(
            f'GRANT SELECT,INSERT,UPDATE({lock_column}) ON {relation} TO "{case.policy.publisher_role}"'
        )
    await case.admin.execute(f'GRANT UPDATE(validated_at) ON {header} TO "{case.policy.publisher_role}"')
    fields = ",".join(storage_identifier(key) for key in candidate._HEADER_COLUMNS)
    override_sql_by_column = {
        "dataset_id": "'incumbent'",
        "acquisition_root_run_id": "'incumbent-root'",
        "status": "'published'",
        "is_current": "true",
        "published_at": "validated_at",
        "candidate_storage_json": "NULL",
    }
    selected = ",".join(override_sql_by_column.get(key, storage_identifier(key)) for key in candidate._HEADER_COLUMNS)
    await case.admin.execute(f"INSERT INTO {header}({fields}) SELECT {selected} FROM {frozen}")


@pytest_asyncio.fixture
async def unallocated_candidate_case(publication_case, payload_case, monkeypatch):
    assert publication_case is payload_case
    case = publication_case
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", case.storage.catalog_schema)
    async with case.admin.transaction():
        frozen = await _complete_frozen_header(case)
        await _canonical_controls(case, frozen)
    case.input_snapshot = await input_snapshots(case)
    return case


@pytest_asyncio.fixture
async def candidate_case(unallocated_candidate_case):
    case = unallocated_candidate_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare_outputs(case, session, driver, inputs) as (workspace, _):
            case.workspace_binding = workspace.binding
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with reopen(case, session, driver, inputs) as workspace:
            case.allocation = await allocate_prepared_cms_candidate(workspace, case.handoff)
    return case


def reopen(case, session, driver, inputs):
    return reopen_prepared_cms_workspace(
        case.database,
        session,
        driver,
        source_binding=case.storage,
        prepared_inputs=inputs,
        handoff=case.handoff,
        expected_workspace=case.workspace_binding,
    )


@asynccontextmanager
async def prepare(case, session, driver, inputs):
    from process.provider_directory_cms_prepared_outputs import read_prepared_cms_outputs

    async with reopen(case, session, driver, inputs) as workspace:
        yield workspace, await read_prepared_cms_outputs(workspace, case.handoff)


async def canonical_snapshot(case, connection):
    roots = (
        *CANONICAL_ROOTS,
        *candidate.storage.RELATIONS,
        storage.HEADER,
        candidate._RELATIONSHIP_RECEIPT,
        candidate.coverage._CANDIDATE_TABLE,
    )
    return {
        root: storage._json(
            await connection.fetchval(
                f"SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) "
                f"FROM {storage_relation(case.storage.catalog_schema, root)} r"
            )
        )
        for root in roots
    }


async def test_native_all_families_compose_noncurrent_candidate_without_context_change(candidate_case):
    case = candidate_case
    frozen = await input_snapshots(case)
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    before = await canonical_snapshot(case, case.admin)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            private = await private_snapshot(workspace)
            descriptor = await candidate.finish_prepared_cms_candidate(
                workspace, case.handoff, allocation=case.allocation
            )
            candidate_evidence = await candidate.read_prepared_cms_candidate(workspace, case.handoff)
            assert set(descriptor) == {
                "version",
                "status",
                "desired_cms_dataset",
                "expected_cms_incumbent",
                "release_id",
                "proof_version",
            }
            assert descriptor == candidate_evidence["descriptor"] and descriptor["status"] == "ready"
            assert descriptor["desired_cms_dataset"]["dataset_id"] == workspace.identity["dataset_id"]
            assert descriptor["expected_cms_incumbent"]["dataset_id"] == "incumbent"
            assert (
                candidate_evidence["header"]["status"] == "validated" and not candidate_evidence["header"]["is_current"]
            )
            assert candidate_evidence["header"]["candidate_storage_json"] is None
            assert len(candidate_evidence["identities"]["canonical_mints"]) == 3
            assert all((await target_counts(driver, case)).values())
            await assert_complete_copy(workspace)
            assert await private_snapshot(workspace) == private
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            header = storage_relation(case.storage.catalog_schema, storage.HEADER)
            incumbent = await driver.fetchval(f"SELECT to_jsonb(r) FROM {header} r WHERE dataset_id='incumbent'")
            assert storage._json(incumbent) == next(
                header_by_field
                for header_by_field in before[storage.HEADER]
                if header_by_field["dataset_id"] == "incumbent"
            )
            assert await driver.fetchval(f"SELECT count(*) FROM {header} WHERE is_current") == 1
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
    assert await input_snapshots(case) == frozen


@pytest.mark.parametrize("failure", ["coverage", "cancel"])
async def test_native_late_failure_rolls_back_all_canonical_writes_then_retries(candidate_case, monkeypatch, failure):
    case = candidate_case
    before = await canonical_snapshot(case, case.admin)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            private = await private_snapshot(workspace)
            original = candidate.coverage.prepare_cms_candidate_coverage_in_session

            async def interrupt(*args):
                await original(*args)
                assert all((await target_counts(driver, case)).values())
                assert (
                    await driver.fetchval(
                        f"SELECT count(*) FROM {storage_relation(case.storage.catalog_schema, candidate.coverage._CANDIDATE_TABLE)}"
                    )
                    == 1
                )
                raise asyncio.CancelledError() if failure == "cancel" else RuntimeError("late coverage failure")

            with monkeypatch.context() as patch:
                patch.setattr(candidate.coverage, "prepare_cms_candidate_coverage_in_session", interrupt)
                with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
                    await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation)
            assert await canonical_snapshot(case, driver) == before
            assert await private_snapshot(workspace) == private
            assert len(await workspace.verify_current()) == 11
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation))[
                "status"
            ] == "ready"


async def test_native_existing_target_header_refuses_without_adoption_or_repair(candidate_case):
    case = candidate_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation)
            accepted = await canonical_snapshot(case, driver)
            with pytest.raises(ValueError, match="header_changed"):
                await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation)
            assert await canonical_snapshot(case, driver) == accepted


@pytest.mark.parametrize("drift", ["header", "receipt", "coverage"])
async def test_native_complete_readback_refuses_or_exposes_canonical_drift(candidate_case, drift):
    case = candidate_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation)
            accepted = await candidate.read_prepared_cms_candidate(workspace, case.handoff)
            await driver.execute("SAVEPOINT data_probe")
            if drift == "header":
                relation = storage_relation(case.storage.catalog_schema, storage.HEADER)
                await driver.execute(
                    f"UPDATE {relation} SET validated_at=validated_at+interval '1 second' WHERE NOT is_current"
                )
            elif drift in {"receipt", "coverage"}:
                root = candidate._RELATIONSHIP_RECEIPT if drift == "receipt" else candidate.coverage._CANDIDATE_TABLE
                relation = storage_relation(case.storage.catalog_schema, root)
                column = "relationship_count" if drift == "receipt" else "created_at"
                increment = "1" if drift == "receipt" else "interval '1 second'"
                await driver.execute(f"UPDATE {relation} SET {column}={column}+{increment}")
            if drift in {"header", "receipt"}:
                with pytest.raises((ValueError, RuntimeError)):
                    await candidate.read_prepared_cms_candidate(workspace, case.handoff)
            else:
                changed = await candidate.read_prepared_cms_candidate(workspace, case.handoff)
                assert changed != accepted
            await driver.execute("ROLLBACK TO SAVEPOINT data_probe")
            await driver.execute("RELEASE SAVEPOINT data_probe")
            assert await candidate.read_prepared_cms_candidate(workspace, case.handoff) == accepted


async def test_native_retained_candidate_preserves_rich_evidence_and_detects_mint_time(candidate_case):
    from process.provider_directory_cms_prepared_outputs import read_prepared_cms_outputs
    from process.provider_directory_cms_prepared_workspace import reopen_prepared_cms_workspace
    from tests.test_provider_directory_cms_prepared_identity_validation_postgres import MINT_ROOTS

    case = candidate_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, private):
            descriptor = await candidate.finish_prepared_cms_candidate(
                workspace, case.handoff, allocation=case.allocation
            )
            rich = await candidate.read_prepared_cms_candidate(workspace, case.handoff)
            binding = workspace.binding
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with reopen_prepared_cms_workspace(
            case.database,
            session,
            driver,
            source_binding=case.storage,
            prepared_inputs=inputs,
            handoff=case.handoff,
            expected_workspace=binding,
        ) as workspace:
            assert await read_prepared_cms_outputs(workspace, case.handoff) == private
            assert await candidate.read_prepared_cms_candidate(workspace, case.handoff) == rich
            assert rich["descriptor"] == descriptor
            root = MINT_ROOTS[0][0]
            relation = storage_relation(case.storage.catalog_schema, root)
            async with case.admin.transaction():
                await case.admin.execute(f"UPDATE {relation} SET created_at=created_at+interval '1 second'")
            changed = await candidate.read_prepared_cms_candidate(workspace, case.handoff)
            assert changed["descriptor"] == descriptor and changed["identities"] != rich["identities"]


async def test_native_same_transaction_allocation_refuses_then_committed_record_adopts(unallocated_candidate_case):
    case = unallocated_candidate_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare_outputs(case, session, driver, inputs) as (workspace, _):
            case.workspace_binding = workspace.binding
            case.allocation = await allocate_prepared_cms_candidate(workspace, case.handoff)
            before = await canonical_snapshot(case, driver)
            with pytest.raises(ValueError, match="allocation_not_committed"):
                await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation)
            assert await canonical_snapshot(case, driver) == before
            assert not any((await target_counts(driver, case)).values())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            assert (await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation))[
                "status"
            ] == "ready"


@pytest.mark.parametrize("drift", ["header", "same_values", "acl", "record"])
async def test_native_allocation_drift_refuses_before_copy(candidate_case, drift):
    case = candidate_case
    allocation = deepcopy(case.allocation)
    if drift == "header":
        header = storage_relation(case.storage.catalog_schema, storage.HEADER)
        await case.admin.execute(f"UPDATE {header} SET created_at=created_at+interval '1 second' WHERE NOT is_current")
    elif drift == "same_values":
        header = storage_relation(case.storage.catalog_schema, storage.HEADER)
        await case.admin.execute(f"UPDATE {header} SET created_at=created_at WHERE NOT is_current")
    elif drift == "acl":
        target = storage_relation(case.storage.catalog_schema, storage.RESOURCE)
        await case.admin.execute(f"GRANT SELECT ON {target} TO PUBLIC")
    else:
        allocation["workspace"]["admission_sha256"] = "0" * 64
    before = await canonical_snapshot(case, case.admin)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            with pytest.raises(ValueError, match="cms_prepared_allocation_(header|inventory|record)_changed"):
                await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=allocation)
            assert await canonical_snapshot(case, driver) == before
            assert not any((await target_counts(driver, case)).values())


@pytest.mark.parametrize("failure", ["inventory", "savepoint"])
async def test_native_allocation_failure_rolls_back_header_then_retries(
    unallocated_candidate_case, monkeypatch, failure
):
    from process import provider_directory_cms_prepared_allocation as allocation

    case = unallocated_candidate_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare_outputs(case, session, driver, inputs) as (workspace, _):
            before = await canonical_snapshot(case, driver)

            async def interrupted_inventory(_):
                raise RuntimeError("interrupted allocation inventory")

            with pytest.raises(RuntimeError, match="interrupted allocation"), monkeypatch.context() as patch:
                if failure == "inventory":
                    patch.setattr(allocation, "_inventory", interrupted_inventory)
                async with source._savepoint(driver):
                    await allocate_prepared_cms_candidate(workspace, case.handoff)
                    raise RuntimeError("interrupted allocation enclosing savepoint")
            assert await canonical_snapshot(case, driver) == before
            assert await allocate_prepared_cms_candidate(workspace, case.handoff)


async def test_native_allocation_outer_transaction_rollback_then_fresh_retry(unallocated_candidate_case):
    case = unallocated_candidate_case
    before = await canonical_snapshot(case, case.admin)
    with pytest.raises(RuntimeError, match="rollback publisher transaction"):
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            async with prepare_outputs(case, session, driver, inputs) as (workspace, _):
                rolled_back_binding = workspace.binding
                await allocate_prepared_cms_candidate(workspace, case.handoff)
            raise RuntimeError("rollback publisher transaction")
    assert await canonical_snapshot(case, case.admin) == before
    for receipt in rolled_back_binding["tables"].values():
        assert (
            await case.admin.fetchval(
                "SELECT to_regclass($1)", storage_relation(case.storage.storage_schema, receipt["name"])
            )
            is None
        )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare_outputs(case, session, driver, inputs) as (workspace, _):
            case.workspace_binding = workspace.binding
            case.allocation = await allocate_prepared_cms_candidate(workspace, case.handoff)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            assert (await candidate.finish_prepared_cms_candidate(workspace, case.handoff, allocation=case.allocation))[
                "status"
            ] == "ready"
