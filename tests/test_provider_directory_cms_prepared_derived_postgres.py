# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native derived-copy/projection proof; fixtures issue no CMS authority."""

import hashlib
from copy import deepcopy

import pytest
import pytest_asyncio
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateColumn

from db import models
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_derived as derived
from process import provider_directory_generic_source_policy as source
from process.provider_directory_generic_candidate import RESOURCE_COLUMNS, WITNESS_COLUMNS
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_cms_prepared_relationships_postgres import (
    input_snapshots,
    observe,
    publisher,
    scope,
)
from tests.test_provider_directory_cms_prepared_relationships_postgres import (
    native_case as native_case,
)
from tests.test_provider_directory_cms_prepared_relationships_postgres import (
    native_frozen_case as native_frozen_case,
)


@pytest_asyncio.fixture
async def native_derived_case(native_frozen_case):
    """Extend only this child with typed header coordinates and full derived3."""
    async with native_frozen_case.admin.transaction():
        return await _typed_derived_inputs(native_frozen_case)


async def _type_derived_header(case, document, tables, schema):
    """Add only the three modeled acquisition coordinates used by this fixture."""
    header = storage_relation(schema, tables[derived.storage.HEADER]["name"])
    await case.admin.execute(f"ALTER TABLE {header} DROP COLUMN id")
    for name in ("dataset_id", "acquisition_root_run_id", "import_run_id"):
        column = models.ProviderDirectoryEndpointDataset.__table__.c[name]
        definition = str(CreateColumn(Column(name, column.type, nullable=column.nullable)).compile(dialect=dialect()))
        await case.admin.execute(f"ALTER TABLE {header} ADD COLUMN {definition}")
    await case.admin.execute(
        f"INSERT INTO {header} VALUES($1,$2,NULL)",
        document["identity"]["dataset_id"],
        document["identity"]["acquisition_root_run_id"],
    )
    return header


async def _type_derived_relations(case, tables, schema):
    """Retain typed derived columns and their existing native primary keys."""
    layouts = derived._layouts()
    for root in derived.DERIVED_ROOTS:
        relation = storage_relation(schema, tables[root]["name"])
        await case.admin.execute(f"ALTER TABLE {relation} DROP COLUMN id")
        for definition in layouts[root][0]:
            await case.admin.execute(f"ALTER TABLE {relation} ADD COLUMN {definition}")
        keys = ",".join(storage_identifier(key) for key in derived.completion._KEYS[root])
        await case.admin.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY({keys})")


async def _write_plan_twins(case, document, resource, release, tables, schema, directory):
    """Add the same six genuine twins to exercise COPY page boundaries."""
    for ordinal in range(6):
        raw_plan_by_field = {**deepcopy(case.raw["InsurancePlan"][0]), "id": f"plan-{ordinal}"}
        normalized = directory.materialize_provider_directory_dataset_fhir_resource(
            source_id="cms-npd",
            dataset_id=document["identity"]["dataset_id"],
            resource=raw_plan_by_field,
            run_id=document["identity"]["acquisition_root_run_id"],
            semantic_projection_as_of=release["generated_at"],
        )
        resource_values = [normalized.get(column) for column in RESOURCE_COLUMNS]
        resource_values[RESOURCE_COLUMNS.index("payload_json")] = derived.storage._encoded(normalized["payload_json"])
        await case.admin.execute(
            f"INSERT INTO {resource} ({','.join(RESOURCE_COLUMNS)}) VALUES({','.join('$' + str(parameter_index) for parameter_index in range(1, len(RESOURCE_COLUMNS) + 1))})",
            *resource_values,
        )
        encoded = derived.storage._encoded(raw_plan_by_field)
        witness = storage_relation(schema, tables[derived.storage.WITNESS]["name"])
        await case.admin.execute(
            f"INSERT INTO {witness} ({','.join(WITNESS_COLUMNS)}) VALUES({','.join('$' + str(parameter_index) for parameter_index in range(1, len(WITNESS_COLUMNS) + 1))})",
            document["identity"]["dataset_id"],
            "cms-npd",
            release["vector_sha256"],
            "InsurancePlan",
            raw_plan_by_field["id"],
            hashlib.sha256(encoded.encode()).hexdigest(),
            normalized["payload_hash"],
            encoded,
        )
        case.raw["InsurancePlan"].append(raw_plan_by_field)
        case.normalized.append(normalized)


async def _write_derived_edges(case, directory, header, resource, document, schema, tables):
    """Use the same existing set-based network and affiliation SQL projections."""
    reader = derived.completion._FrozenSourceRows(case.admin)
    pattern = r"(?i)(?:^|/)Organization/([A-Za-z0-9.-]{1,64})(?:/_history/[A-Za-z0-9.-]{1,64})?/?(?:[?#].*)?$"
    for root, source_cte, edge_cte in (
        (derived.DERIVED_ROOTS[1], directory._network_plan_source_ctes, directory._network_plan_edge_ctes),
        (derived.DERIVED_ROOTS[2], directory._affiliation_org_source_ctes, directory._affiliation_org_edge_ctes),
    ):
        fields = derived.completion._KEYS[root][1:]
        target_relation = storage_relation(schema, tables[root]["name"])
        await reader.all(
            source_cte(header, resource, "")
            + edge_cte(pattern)
            + f" INSERT INTO {target_relation} SELECT :dataset_id,{','.join(fields)} FROM expected_edges",
            dataset_id=document["identity"]["dataset_id"],
        )


async def _typed_derived_inputs(case):
    """Extend this child only, preserving its frozen-input and inventory baseline."""
    document = case.handoff.document
    tables = document["tables"]
    schema = case.storage.storage_schema
    header = await _type_derived_header(case, document, tables, schema)
    await _type_derived_relations(case, tables, schema)
    resource = storage_relation(schema, tables[derived.storage.RESOURCE]["name"])
    plan = storage_relation(schema, tables[derived.DERIVED_ROOTS[0]]["name"])
    directory = derived.import_module("process.provider_directory_fhir")
    release = document["identity"]["metadata"]["source_release"]
    await _write_plan_twins(case, document, resource, release, tables, schema, directory)
    plan_file = next(
        name for name, kind in derived.import_module("process.cms_npd_source").RESOURCE_FILES if kind == "InsurancePlan"
    )
    release["files"][plan_file].update(row_count=7, distinct_count=7)
    document["admission"]["resource_count"] += 6
    await case.admin.execute(
        f"INSERT INTO {plan}(dataset_id,resource_id,payload_hash,payload_json) "
        f"SELECT dataset_id,resource_id,payload_hash,payload_json FROM {resource} WHERE resource_type='InsurancePlan'"
    )
    await _write_derived_edges(case, directory, header, resource, document, schema, tables)
    native_rows = await case.admin.fetch(
        derived.context._INVENTORY_SQL, [native_row["relation_oid"] for native_row in case.inventory["relations"]]
    )
    case.inventory["relations"] = [
        {key: field_value for key, field_value in dict(native_row).items() if key != "supported"}
        for native_row in native_rows
    ]
    case.handoff = FHIRSourceCandidateHandoff(derived.storage._encoded(document))
    case.input_snapshot = await input_snapshots(case)
    return case


async def counts(current):
    return {
        root: await current.connection.fetchval(
            f"SELECT count(*) FROM {storage_relation(current.policy.schema, current.receipts[root]['name'])}"
        )
        for root in derived.DERIVED_ROOTS
    }


async def test_native_copy_and_exact_projection_keep_inputs_and_source_context_untouched(native_derived_case):
    case = native_derived_case
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    before = await input_snapshots(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            copied = await derived.copy_prepared_cms_derived(current, case.handoff)
            assert list(copied["row_counts"].values()) == [7, 7, 1]
            await current.finish_indexes()
            verified = await derived.verify_prepared_cms_derived(current, case.handoff)
            assert verified["row_counts"] == copied["row_counts"]
            assert verified == await derived.verify_prepared_cms_derived(current, case.handoff)
            plan = storage_relation(current.policy.schema, current.receipts[derived.DERIVED_ROOTS[0]]["name"])
            assert await driver.fetchval(f"SELECT bool_and(plan_active) FROM {plan}") is True
            assert await driver.fetchval(f"SELECT bool_and(plan_identifier IS NULL) FROM {plan}") is True
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            assert await input_snapshots(case) == before
    assert all(
        [
            await case.admin.fetchval(f"SELECT count(*)=0 FROM {storage_relation(case.storage.catalog_schema, root)}")
            for root in derived.storage.AUXILIARY_ROOTS
        ]
    )


@pytest.mark.parametrize("bound", ("rows", "bytes"))
async def test_native_copy_pages_are_bounded_before_native_copy(native_derived_case, monkeypatch, bound):
    case = native_derived_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            original, batches = current.copy_rows, []
            if bound == "rows":
                monkeypatch.setattr(derived, "COPY_MAX_ROWS", 2)
            else:
                relation = storage_relation(
                    case.storage.storage_schema, case.handoff.document["tables"][derived.DERIVED_ROOTS[0]]["name"]
                )
                width = "2+" + "+".join(
                    "4+coalesce(octet_length(" + storage_identifier(str(column)) + "::text)+1,0)"
                    for column in derived._layouts()[derived.DERIVED_ROOTS[0]][1]
                )
                limit = await driver.fetchval(f"SELECT max({width}) FROM {relation}")
                monkeypatch.setattr(derived, "COPY_MAX_BYTES", int(limit) + 21)

            async def capture(root, columns, records):
                if root == derived.DERIVED_ROOTS[0]:
                    batches.append(deepcopy(records))
                return await original(root, columns, records)

            monkeypatch.setattr(current, "copy_rows", capture)
            assert await derived.copy_prepared_cms_derived(current, case.handoff)
            assert len(batches) > 1 and sum(map(len, batches)) == 7
            assert (
                all(len(batch) <= 2 for batch in batches)
                if bound == "rows"
                else all(len(batch) == 1 for batch in batches)
            )
            await current.finish_indexes()
            assert await derived.verify_prepared_cms_derived(current, case.handoff)


async def test_native_late_copy_failure_rolls_back_prior_root_and_retry_succeeds(native_derived_case, monkeypatch):
    case = native_derived_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            original = current.copy_rows

            async def fail(root, columns, records):
                result = await original(root, columns, records)
                if root == derived.DERIVED_ROOTS[1]:
                    raise RuntimeError("late derived COPY failure")
                return result

            with monkeypatch.context() as patch:
                patch.setattr(current, "copy_rows", fail)
                with pytest.raises(RuntimeError, match="late derived"):
                    await derived.copy_prepared_cms_derived(current, case.handoff)
            assert list((await counts(current)).values()) == [0, 0, 0]
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert await derived.copy_prepared_cms_derived(current, case.handoff)


@pytest.mark.parametrize("change", ("plan", "edge", "missing"))
async def test_native_full_output_row_difference_refuses_after_indexes(native_derived_case, change):
    case = native_derived_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await derived.copy_prepared_cms_derived(current, case.handoff)
            await current.finish_indexes()
            root = derived.DERIVED_ROOTS[("plan", "edge", "missing").index(change)]
            relation = storage_relation(current.policy.schema, current.receipts[root]["name"])
            if change == "plan":
                await driver.execute(f'UPDATE {relation} SET payload_json=\'{{"status":"inactive"}}\'')
            elif change == "edge":
                await driver.execute(
                    f"INSERT INTO {relation} VALUES($1,'unexpected','plan-one')", current.identity["dataset_id"]
                )
            else:
                await driver.execute(f"DELETE FROM {relation}")
            with pytest.raises(ValueError, match="projection_changed"):
                await derived.verify_prepared_cms_derived(current, case.handoff)


async def test_native_invalid_frozen_projection_refuses_before_any_output_copy(native_derived_case):
    case = native_derived_case
    relation = storage_relation(
        case.storage.storage_schema, case.handoff.document["tables"][derived.DERIVED_ROOTS[0]]["name"]
    )
    old = await case.admin.fetchval(f"SELECT payload_hash FROM {relation} WHERE resource_id='plan-one'")
    await case.admin.execute(f"UPDATE {relation} SET payload_hash=$1 WHERE resource_id='plan-one'", "0" * 64)
    try:
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            async with scope(case, session, driver, inputs) as current:
                with pytest.raises(ValueError, match="plan_projection_changed"):
                    await derived.copy_prepared_cms_derived(current, case.handoff)
                assert list((await counts(current)).values()) == [0, 0, 0]
    finally:
        await case.admin.execute(f"UPDATE {relation} SET payload_hash=$1 WHERE resource_id='plan-one'", old)
