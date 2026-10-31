# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete private output recipe; no canonical writes or protected issuance."""

import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_derived as derived
from process import provider_directory_cms_prepared_outputs as outputs
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace
from process.provider_directory_generic_candidate import RESOURCE_COLUMNS, WITNESS_COLUMNS
from tests.test_provider_directory_cms_prepared_derived_postgres import (
    native_case,
    native_derived_case,
    native_frozen_case,
    observe,
    publisher,
)
from tests.test_provider_directory_cms_prepared_identity_validation_postgres import MINT_ROOTS
from tests.test_provider_directory_cms_prepared_relationships_postgres import input_snapshots
from tests.test_provider_directory_cms_prepared_workspace_postgres import assert_payload_snapshot_bounds, scope


@pytest_asyncio.fixture
async def full_private_case(native_derived_case):
    case = native_derived_case
    header = storage_relation(case.storage.storage_schema, case.handoff.document["tables"][storage.HEADER]["name"])
    await case.admin.execute(f"ALTER TABLE {header} ADD PRIMARY KEY(dataset_id)")
    for root, column, _ in MINT_ROOTS:
        relation = storage_relation(case.storage.catalog_schema, root)
        await case.admin.execute(f"CREATE TABLE {relation} ({column} uuid PRIMARY KEY,created_at timestamptz NOT NULL)")
        await case.admin.execute(f'GRANT SELECT ON {relation} TO "{case.policy.publisher_role}"')
        assert not await case.admin.fetchval(
            "SELECT has_table_privilege($1,$2,'INSERT,UPDATE,DELETE,TRUNCATE')", case.policy.publisher_role, relation
        )
    return case


def prepare(case, session, driver, inputs):
    return outputs.prepare_private_cms_outputs(
        case.database,
        session,
        driver,
        source_binding=case.storage,
        prepared_inputs=inputs,
        handoff=case.handoff,
        preparation_id=case.preparation_id,
        admission_sha256=case.admission,
    )


async def frozen_and_canonical(case):
    canonical_counts = []
    for root in (*storage.AUXILIARY_ROOTS, *(root for root, _, _ in MINT_ROOTS)):
        canonical_counts.append(
            await case.admin.fetchval(f"SELECT count(*) FROM {storage_relation(case.storage.catalog_schema, root)}")
        )
    canonical_objects = await case.admin.fetch(
        "SELECT c.oid::bigint,c.relname,c.relkind::text,c.relfilenode::bigint,c.relowner::bigint "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 ORDER BY c.oid",
        case.storage.catalog_schema,
    )
    return await input_snapshots(case), canonical_counts, canonical_objects


async def assert_private_payload_twins(workspace, handoff):
    await assert_payload_snapshot_bounds(workspace)
    row_counts_by_root = {}
    for root, columns, json_field in (
        (storage.RESOURCE, RESOURCE_COLUMNS, "payload_json"),
        (storage.WITNESS, WITNESS_COLUMNS, "raw_payload_json"),
    ):
        frozen = handoff.document["tables"][root]
        copied = workspace.receipts[root]
        assert frozen["oid"] != copied["oid"]
        source_relation = storage_relation(workspace.policy.schema, frozen["name"])
        target_relation = storage_relation(workspace.policy.schema, copied["name"])
        projection = ",".join(
            storage_identifier(column) + ("::jsonb" if column == json_field else "") for column in columns
        )
        expected, actual = (
            f"SELECT {projection} FROM ONLY {source_relation}",
            f"SELECT {projection} FROM ONLY {target_relation}",
        )
        assert (
            await workspace.connection.fetchval(
                f"SELECT EXISTS(({expected} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {expected}))"
            )
            is False
        )
        row_counts_by_root[root] = await workspace.connection.fetchval(f"SELECT count(*) FROM ONLY {target_relation}")
        assert row_counts_by_root[root] > 0
        assert (
            await workspace.connection.fetchval(
                "SELECT atttypid='jsonb'::regtype FROM pg_attribute WHERE attrelid=$1 AND attname=$2",
                copied["oid"],
                json_field,
            )
            is True
        )
        assert (
            await workspace.connection.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid=$1 AND contype='f'", copied["oid"]
            )
            == 0
        )
    return row_counts_by_root


@pytest.mark.parametrize("root", [storage.RESOURCE, storage.WITNESS])
async def test_native_oversized_frozen_payload_is_masked_and_refused_before_copy(full_private_case, monkeypatch, root):
    case = full_private_case
    before = await frozen_and_canonical(case)
    cap, copied = 32, []
    columns = derived._layouts()[root][1]
    frozen = case.handoff.document["tables"][root]
    relation = storage_relation(case.storage.storage_schema, frozen["name"])

    async def refuse_copy(*args):
        copied.append(args)
        pytest.fail("oversized frozen payload reached COPY")

    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            rows = await driver.fetch(derived._page(relation, root, columns), "", "", "", 4, cap)
            assert len(rows) == 1
            assert type(rows[0]["row_bytes"]) is int and rows[0]["row_bytes"] > cap
            assert all(rows[0][column] is None for column in columns)
            monkeypatch.setattr(derived, "COPY_MAX_BYTES", cap + 21)
            monkeypatch.setattr(current, "copy_rows", refuse_copy)
            with pytest.raises(ValueError, match="cms_prepared_derived_copy_page_unbounded"):
                await derived._copy_inputs(current, case.handoff.document, {root: current.receipts[root]})
            assert copied == []
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    assert await frozen_and_canonical(case) == before


def plan_nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from plan_nodes(child)


async def seed_frozen_transport_rows(case, root, relation, columns):
    """Seed native transport rows only within the caller's rollback transaction."""
    row_id_sql = "(ARRAY['a-','B-','Z-','z-'])[1+(ordinal%4)]||lpad(ordinal::text,8,'0')"
    payload_sql = f"jsonb_build_object('resourceType','Organization','id',{row_id_sql})"
    if root == storage.RESOURCE:
        projection_sql = f"$1,'Organization',{row_id_sql},repeat('a',64),{payload_sql},repeat('b',64)"
        arguments = (case.handoff.document["identity"]["dataset_id"],)
    else:
        projection_sql = f"$1,'cms-npd',$2,'Organization',{row_id_sql},repeat('a',64),repeat('b',64),{payload_sql}"
        arguments = (
            case.handoff.document["identity"]["dataset_id"],
            case.handoff.document["identity"]["metadata"]["source_release"]["vector_sha256"],
        )
    await case.admin.execute(
        f"INSERT INTO {relation} ({','.join(columns)}) SELECT {projection_sql} "
        "FROM generate_series(1,100000) AS ordinal",
        *arguments,
    )
    await case.admin.execute(f"ANALYZE {relation}")


@pytest.mark.parametrize("root", [storage.RESOURCE, storage.WITNESS])
async def test_native_generic_middle_payload_page_uses_existing_collated_primary_key(full_private_case, root):
    case = full_private_case
    before = await frozen_and_canonical(case)
    frozen = case.handoff.document["tables"][root]
    relation = storage_relation(case.storage.storage_schema, frozen["name"])
    primary_key_sql = (
        "SELECT i.indexrelid::bigint AS oid,i.indkey::text,pg_get_indexdef(i.indexrelid) AS definition "
        "FROM pg_index i WHERE i.indrelid=$1 AND i.indisprimary"
    )
    primary_key = await case.admin.fetchrow(primary_key_sql, frozen["oid"])
    assert primary_key is not None
    transaction, has_prepared_statement = case.admin.transaction(), False
    await transaction.start()
    try:
        assert tuple(
            await case.admin.fetchrow(
                "SELECT datlocprovider::text,datcollate FROM pg_database WHERE datname=current_database()"
            )
        ) in {("c", "en_US.UTF-8"), ("c", "en_US.utf8")}
        columns = derived._layouts()[root][1]
        await seed_frozen_transport_rows(case, root, relation, columns)
        await case.admin.execute("SET LOCAL plan_cache_mode=force_generic_plan")
        await case.admin.execute(
            "PREPARE cms_payload_seek(text,text,text,integer,integer) AS " + derived._page(relation, root, columns)
        )
        has_prepared_statement = True
        dataset = case.handoff.document["identity"]["dataset_id"]
        # EXPLAIN EXECUTE parameters cannot use extended-query bind parameters.
        dataset_literal = "'" + dataset.replace("'", "''") + "'"
        explanation = json.loads(
            await case.admin.fetchval(
                "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE cms_payload_seek("
                + dataset_literal
                + ",'Organization','B-00050001',4096,33554411)"
            )
        )[0]["Plan"]
        page = next(node for node in plan_nodes(explanation) if node.get("Subplan Name") == "CTE page")
        assert page["Node Type"] == "Limit" and page["Actual Rows"] == 4096
        inputs = list(plan_nodes(page))[1:]
        assert len(inputs) == 1 and inputs[0]["Node Type"] in {"Index Scan", "Index Only Scan"}
        scan = inputs[0]
        assert scan["Actual Rows"] == 4096 and "Filter" not in scan
        assert scan.get("Rows Removed by Filter", 0) == 0
        assert all(key in scan["Index Cond"] for key in ("dataset_id", "resource_type", "resource_id"))
        assert all(parameter in scan["Index Cond"] for parameter in ("$1", "$2", "$3"))
        assert "ROW(" in scan["Index Cond"] and "COLLATE" not in scan["Index Cond"]
        index = storage_relation(case.storage.storage_schema, scan["Index Name"])
        assert await case.admin.fetchval("SELECT $1::regclass::oid", index) == primary_key["oid"]
        assert await case.admin.fetchrow(primary_key_sql, frozen["oid"]) == primary_key
    finally:
        await transaction.rollback()
        if has_prepared_statement:
            await case.admin.execute("DEALLOCATE cms_payload_seek")
    assert await case.admin.fetchrow(primary_key_sql, frozen["oid"]) == primary_key
    assert await frozen_and_canonical(case) == before


async def test_native_complete_private_recipe_has_exact_outputs_and_immutable_inputs(full_private_case):
    case = full_private_case
    before = await frozen_and_canonical(case)
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            assert evidence["state"] == "verified_private" and len(evidence["tables"]) == 11
            assert evidence["contract"] == "cms-prepared-private-output-evidence-v2"
            assert workspace.binding["contract"] == "cms-prepared-private-workspace-v2"
            assert evidence["handoff_sha256"] == case.handoff.document_sha256
            assert evidence["admission_sha256"] == case.admission
            assert len(evidence["identity_parity"]["content_sha256"]) == 64
            assert len(evidence["relationship_parity"]["content_sha256"]) == 64
            assert len(evidence["frozen_content"]["content"]) == 8
            assert evidence["resource_counts"]["InsurancePlan"] == 7
            assert evidence["derived_parity"]["row_counts"][storage.RELATIONS[1]] == 7
            assert evidence["relationship_parity"]["relationship_count"] > 20
            assert evidence["input_payload_parity"]["row_counts"] == await assert_private_payload_twins(
                workspace, case.handoff
            )
            assert workspace.binding["indexed"]
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            assert await frozen_and_canonical(case) == before
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
    assert await frozen_and_canonical(case) == before


@pytest.mark.parametrize("root", [storage.RESOURCE, storage.WITNESS])
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_native_partial_private_payload_copy_rolls_back_and_retries(
    full_private_case, monkeypatch, root, failure
):
    case = full_private_case
    before = await frozen_and_canonical(case)
    original = CMSPreparedWorkspace.copy_rows
    copied_batches = []

    async def interrupted(current, selected_root, columns, records):
        copied = await original(current, selected_root, columns, records)
        if selected_root == root:
            assert copied > 0
            copied_batches.append(copied)
            assert await current.connection.fetchval("SELECT current_user") == case.policy.publisher_role
            raise failure("synthetic interruption after native payload COPY")
        return copied

    with monkeypatch.context() as patch:
        patch.setattr(CMSPreparedWorkspace, "copy_rows", interrupted)
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            with pytest.raises(failure):
                async with prepare(case, session, driver, inputs):
                    pytest.fail("partial private payload reached the caller")
            assert copied_batches
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
    assert await frozen_and_canonical(case) == before
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='private_source' AND c.relkind='r'"
        )
        == 8
    )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (current, evidence):
            assert evidence["input_payload_parity"]["row_counts"] == await assert_private_payload_twins(
                current, case.handoff
            )
    assert await frozen_and_canonical(case) == before


@pytest.mark.parametrize("root", [storage.RESOURCE, storage.WITNESS])
async def test_native_private_payload_drift_cannot_rewrite_report_baseline(full_private_case, root):
    case = full_private_case
    before = await frozen_and_canonical(case)
    modified_roots = []
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(ValueError):
            async with prepare(case, session, driver, inputs) as (current, evidence):
                await assert_private_payload_twins(current, case.handoff)
                output_relation = storage_relation(current.policy.schema, current.receipts[root]["name"])
                field = "payload_json" if root == storage.RESOURCE else "raw_payload_json"
                assert (
                    await driver.execute(
                        f"UPDATE {output_relation} SET {field}=jsonb_set({field},'{{unreferenced_name}}','\"Changed\"'::jsonb) "
                        "WHERE resource_type='Practitioner'"
                    )
                    == "UPDATE 1"
                )
                modified_roots.append(root)
                evidence["input_payload_parity"].clear()
        assert modified_roots == [root]
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
        assert (
            await driver.fetchval(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='private_source' AND c.relkind='r'"
            )
            == 8
        )
        async with prepare(case, session, driver, inputs) as (current, evidence):
            assert evidence["input_payload_parity"]["row_counts"] == await assert_private_payload_twins(
                current, case.handoff
            )
    assert await frozen_and_canonical(case) == before


@pytest.mark.parametrize("failure", [asyncio.CancelledError, TimeoutError])
async def test_native_interrupted_frozen_copy_closes_original_transport_and_retries(
    full_private_case, monkeypatch, failure
):
    case = full_private_case
    before = await frozen_and_canonical(case)
    copy_content = outputs._copy_frozen_content
    interrupted_drivers = []
    interrupted_pids = []

    async def interrupt(workspace, receipt):
        driver = workspace.connection
        interrupted_drivers.append(driver)
        interrupted_pids.append(driver.get_server_pid())

        async def export(query, *, output, **options):
            async def stop_after_chunk(chunk):
                await output(chunk)
                raise failure("synthetic interrupted COPY")

            return await driver.copy_from_query(query, output=stop_after_chunk, **options)

        connection = SimpleNamespace(
            fetch=driver.fetch,
            copy_from_query=export,
            close=driver.close,
            terminate=driver.terminate,
            _transport=driver._transport,
        )
        return await copy_content(
            SimpleNamespace(connection=connection, session=workspace.session, policy=workspace.policy), receipt
        )

    with monkeypatch.context() as patch:
        patch.setattr(outputs, "_copy_frozen_content", interrupt)
        with pytest.raises(failure):
            async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
                async with prepare(case, session, driver, inputs):
                    pytest.fail("interrupted input stream reached caller")
    assert len(interrupted_drivers) == 1 and interrupted_drivers[0].is_closed()
    async with asyncio.timeout(3):
        while await case.admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", interrupted_pids[0]
        ):
            await asyncio.sleep(0.01)
    assert await frozen_and_canonical(case) == before
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='private_source' AND c.relkind='r'"
        )
        == 8
    )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            assert len(evidence["frozen_content"]["content"]) == 8
    assert await frozen_and_canonical(case) == before


@pytest.mark.parametrize("root", [storage.RESOURCE, storage.HEADER])
async def test_native_inherited_child_cannot_escape_only_input_commitment(full_private_case, root):
    case = full_private_case
    before = await frozen_and_canonical(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(ValueError, match="provider_directory_generic_source_storage_missing"):
            async with prepare(case, session, driver, inputs) as (workspace, evidence):
                relation = storage_relation(workspace.policy.schema, case.handoff.document["tables"][root]["name"])
                await driver.execute(f"CREATE TABLE private_source.extra_child () INHERITS ({relation})")
                await driver.execute(f"INSERT INTO private_source.extra_child SELECT * FROM ONLY {relation}")
        assert await driver.fetchval("SELECT to_regclass('private_source.extra_child')") is None
        assert await frozen_and_canonical(case) == before
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            assert len(evidence["frozen_content"]["content"]) == 8
    assert await frozen_and_canonical(case) == before


async def change_frozen_input(case, workspace, driver, root):
    """Change exactly the selected private input without updating its evidence."""
    receipt = case.handoff.document["tables"][root]
    relation = storage_relation(workspace.policy.schema, receipt["name"])
    if root in {storage.RESOURCE, storage.WITNESS}:
        field = "payload_json" if root == storage.RESOURCE else "raw_payload_json"
        await driver.execute(
            f"UPDATE {relation} SET {field}=jsonb_set({field}::jsonb,"
            "'{unreferenced_name}','\"Changed without changing its recorded hash\"'::jsonb) "
            "WHERE resource_type='Practitioner'"
        )
    elif root == storage.HEADER:
        await driver.execute(f"UPDATE {relation} SET import_run_id='changed'")
    elif root in storage.HANDOFF_CONTROL_ROOTS or root == storage.RELATIONSHIP:
        await driver.execute(f"INSERT INTO {relation}(id) VALUES(17)")
    else:
        await driver.execute(f"DELETE FROM {relation}")


@pytest.mark.parametrize(
    "root",
    [*storage.RELATIONS, *storage.HANDOFF_CONTROL_ROOTS],
)
async def test_native_every_frozen_input_value_is_checked_and_rolled_back(full_private_case, root):
    case = full_private_case
    before = await frozen_and_canonical(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(ValueError, match="cms_prepared_outputs_content_changed"):
            async with prepare(case, session, driver, inputs) as (workspace, evidence):
                await change_frozen_input(case, workspace, driver, root)
                # The public report must not become the private comparison baseline.
                evidence["frozen_content"].clear()
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
        assert await frozen_and_canonical(case) == before
        assert (
            await driver.fetchval(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='private_source' AND c.relkind='r'"
            )
            == 8
        )
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            assert len(evidence["frozen_content"]["content"]) == 8
    assert await frozen_and_canonical(case) == before


@pytest.mark.parametrize("failure", ["body", "cancel", "content", "content_and_evidence"])
async def test_native_recipe_failure_or_drift_rolls_back_and_retries(full_private_case, failure):
    case = full_private_case
    before = await frozen_and_canonical(case)
    expected = asyncio.CancelledError if failure == "cancel" else (RuntimeError if failure == "body" else ValueError)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(expected):
            async with prepare(case, session, driver, inputs) as (workspace, evidence):
                if failure == "body":
                    raise RuntimeError("synthetic caller failure")
                if failure == "cancel":
                    raise asyncio.CancelledError()
                relation = storage_relation(
                    workspace.policy.schema, workspace.receipts[storage.CMS_IDENTITY_ROOTS[0]]["name"]
                )
                # A different unused opaque UUID can still satisfy source facts;
                # the exact identity commitment must detect this body mutation.
                await driver.execute(
                    f"UPDATE {relation} SET organization_id=gen_random_uuid() WHERE resource_type='Organization'"
                )
                if failure == "content_and_evidence":
                    evidence["identity_parity"].update(
                        await outputs.assert_prepared_cms_identities(workspace, case.handoff)
                    )
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
        assert (
            await driver.fetchval(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='private_source' AND c.relkind='r'"
            )
            == 8
        )
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            assert evidence["state"] == "verified_private"
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
    assert await frozen_and_canonical(case) == before


async def test_native_retained_private_evidence_matches_after_commit(full_private_case):
    from process.provider_directory_cms_prepared_workspace import reopen_prepared_cms_workspace

    case = full_private_case
    before = await frozen_and_canonical(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
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
            assert await outputs.read_prepared_cms_outputs(workspace, case.handoff) == evidence
    assert await frozen_and_canonical(case) == before
