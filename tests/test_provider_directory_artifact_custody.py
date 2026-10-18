# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual artifact payload/control owners and authentic terminal outcomes."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_profile_native_copy as native_copy
from tests.test_provider_directory_control_custody import control_wave as control_wave
from tests.test_provider_directory_owned_evidence_wave import Session, custody, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave

pytestmark = pytest.mark.asyncio


def _status_source_case(kind, status):
    """Use real dataset/batch/binding shapes; header/catalog transport remains synthetic."""
    coordinate = batch(kind)
    selected = fhir.ProviderDirectoryArtifactDataset(
        coordinate.source_id,
        "synthetic_endpoint",
        "synthetic_dataset",
        "synthetic_run",
        status=status,
        is_current=status == "published",
        promote_on_cutover=status == "validated",
    )
    identity_by_field = dict(
        dataset_id=selected.dataset_id,
        endpoint_id=selected.endpoint_id,
        acquisition_root_run_id=selected.evidence_run_id,
        metadata=dict(source_ids=[selected.source_id]),
    )
    tables_by_root = {
        root: dict(name="pdgs_" + format(number + 1, "032x") + "_r", oid=number + 100, owner=23, layout_sha256="a" * 64)
        for number, root in enumerate(native_copy.source.RELATIONS)
    }
    binding_by_field = dict(
        contract=native_copy.source.CONTRACT,
        identity=identity_by_field,
        phase="published",
        ordinal=1,
        resets={},
        tables=tables_by_root,
        auxiliary=None,
        relationship_sha256=None,
    )
    header_by_field = dict(
        endpoint_id=selected.endpoint_id,
        acquisition_root_run_id=selected.evidence_run_id,
        status=status,
        publication_metadata_json=identity_by_field["metadata"],
        candidate_storage_json=binding_by_field,
    )
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=header_by_field), execute=AsyncMock())
    publisher = SimpleNamespace(connection=driver, policy=SimpleNamespace(schema="synthetic"))
    return coordinate, selected, binding_by_field, header_by_field, publisher


@pytest.mark.parametrize("kind", ["source", "Practitioner"])
@pytest.mark.parametrize("selected_status", ["published", "validated"])
@pytest.mark.parametrize("changed", [False, True])
async def test_copy_header_status_matches_exact_issued_selection(monkeypatch, kind, selected_status, changed):
    from process import provider_directory_profile_source_inputs as inputs

    coordinate, selected, binding, header, publisher = _status_source_case(kind, selected_status)

    # The genuine module owns fence authority; this test isolates the native-header recheck.
    def select(module, frozen_batch):
        assert module is fhir and frozen_batch is coordinate
        return selected

    monkeypatch.setattr(inputs, "dataset", select)
    verify = AsyncMock()
    monkeypatch.setattr(native_copy.source, "_verify_tables", verify)
    if changed:
        header["status"] = "published" if selected_status == "validated" else "validated"
        with pytest.raises(RuntimeError, match="native_copy_custody_invalid"):
            await native_copy._source(fhir, publisher, binding, coordinate)
        verify.assert_not_awaited()
    else:
        await native_copy._source(fhir, publisher, binding, coordinate)
        verify.assert_awaited_once_with(publisher.connection, publisher.policy, binding)
        assert selected.status == selected_status and binding["phase"] == "published"


@pytest.mark.parametrize("field", ["source_id", "endpoint_id", "dataset_id", "evidence_run_id"])
async def test_copy_selected_dataset_identity_refuses_before_header_io(monkeypatch, field):
    from process import provider_directory_profile_source_inputs as inputs

    coordinate, selected, binding, _header, publisher = _status_source_case("source", "validated")
    monkeypatch.setattr(inputs, "dataset", lambda *_args: replace(selected, **{field: "changed"}))
    with pytest.raises(RuntimeError, match="native_copy_custody_invalid"):
        await native_copy._source(fhir, publisher, binding, coordinate)
    publisher.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("status", ["verification_baseline", "verification_mismatch", "validated"])
async def test_copy_cannot_override_input_scope_authority_refusal(monkeypatch, status):
    from process import provider_directory_profile_source_inputs as inputs

    coordinate, _selected, binding, _header, publisher = _status_source_case("source", status)

    def refuse(*_args):
        raise RuntimeError("issued selected fence authority refused")

    monkeypatch.setattr(inputs, "dataset", refuse)
    with pytest.raises(RuntimeError, match="issued selected fence authority refused"):
        await native_copy._source(fhir, publisher, binding, coordinate)
    publisher.connection.fetchrow.assert_not_awaited()


def _legacy_target_columns(model):
    """Project synthetic columns from the existing CreateColumn model contract."""
    columns = [
        dict(
            number=number,
            name=str(column.name),
            type=native_copy._column_type(column, {}),
            required=not column.nullable,
            default=None if column.server_default is None else str(column.server_default.arg),
            identity="",
            generated="",
            native_collation=True,
        )
        for number, column in enumerate(model.__table__.columns, 1)
    ]
    return columns


def _legacy_primary_index(key_name, key_columns):
    """Describe only the original legacy primary index in catalog units."""
    return dict(
        oid=301,
        name=key_name,
        primary=True,
        unique=True,
        method="btree",
        keys=key_columns,
        valid=True,
        ready=True,
        live=True,
        immediate=True,
        native=True,
        exclusion=False,
        keys_count=len(key_columns),
        total_count=len(key_columns),
        owner=23,
        expression=None,
        predicate=None,
        constraint_oid=201,
    )


def _legacy_target_catalog(model, relation="scope_target"):
    """Synthetic catalog rows exercise the fixed legacy layout, not native authority."""
    columns = _legacy_target_columns(model)
    key_name = fhir._artifact_scope_pk_names(relation)[1]
    key_columns = [str(column.name) for column in model.__table__.primary_key.columns]
    number_by_column = {column["name"]: column["number"] for column in columns}
    primary_by_field = dict(
        oid=201,
        name=key_name,
        type="p",
        keys=[number_by_column[name] for name in key_columns],
        validated=True,
        deferrable=False,
        deferred=False,
    )
    index_by_field = _legacy_primary_index(key_name, key_columns)
    indexes = [index_by_field]
    if model in (fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation):
        indexes.append(
            {
                **index_by_field,
                "oid": 302,
                "name": fhir._provider_directory_profile_bucket_index_sql("synthetic", relation)[0],
                "primary": False,
                "unique": False,
                "keys": ["source_id", None],
                "keys_count": 2,
                "total_count": 2,
                "expression": "native bucket expression",
                "constraint_oid": None,
            }
        )
    return dict(
        oid=17,
        relowner=23,
        relname=relation,
        acl_closed=True,
        readers_closed=True,
        acl=dict(columns=[]),
        topology=dict(
            kind="r",
            persistence="u",
            partition=False,
            rls=False,
            force_rls=False,
            parents=[],
            children=[],
            hooks=0,
            rules=0,
        ),
        columns=columns,
        constraints=[primary_by_field],
        indexes=indexes,
    )


def _drift_target_catalog(catalog, drift):
    """Introduce one independent drift while preserving the other catalog facts."""
    match drift:
        case "trigger" | "rule" | "parent" | "rls":
            field = {"trigger": "hooks", "rule": "rules", "parent": "parents", "rls": "rls"}[drift]
            catalog["topology"][field] = [99] if drift == "parent" else True
        case "fk":
            catalog["constraints"].append(dict(type="f", validated=True, deferrable=False, deferred=False))
        case "missing_pk":
            catalog["constraints"] = []
        case "pk_keys":
            catalog["constraints"][0]["keys"] = [999]
        case "extra_column":
            catalog["columns"].append({**catalog["columns"][0], "name": "unexpected"})
        case "type" | "default" | "generated" | "collation":
            field = "native_collation" if drift == "collation" else drift
            catalog["columns"][0][field] = False if drift == "collation" else "unexpected"
        case "extra_index":
            catalog["indexes"].append({**catalog["indexes"][0], "name": "unexpected"})
        case "index_flags":
            catalog["indexes"][0]["valid"] = False
        case "index_owner":
            catalog["indexes"][0]["owner"] = 999
        case "acl":
            catalog["acl_closed"] = False


@pytest.mark.parametrize("model", (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS))
async def test_profile_copy_loading_accepts_exact_unindexed_heaps(model):
    catalog = _legacy_target_catalog(model)
    catalog["constraints"], catalog["indexes"] = [], []
    assert native_copy._target_catalog(fhir, model, "scope_target", 17, 23, catalog) is False


async def test_profile_copy_loading_refuses_premature_index():
    model = fhir.ProviderDirectorySource
    with pytest.raises(RuntimeError, match="native_copy_custody_invalid"):
        native_copy._target_catalog(fhir, model, "scope_target", 17, 23, _legacy_target_catalog(model))


async def test_completed_catalog_requires_original_source_owner(artifact_wave):
    _, database, _, *_ = artifact_wave
    async with custody.registry_owned_wal_transaction(database) as outcome:
        with pytest.raises(Exception, match="publisher_connection_required"):
            await native_copy.completed_target_catalog(
                fhir, fhir.ProviderDirectorySource, "scope_target", 17, outcome=outcome
            )


@pytest.mark.parametrize("model", (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS))
async def test_profile_copy_keeps_exact_original_nine_layouts(model):
    catalog = _legacy_target_catalog(model)
    assert native_copy._target_catalog(fhir, model, "scope_target", 17, 23, catalog, phase="completed") is (
        model in (fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation)
    )


@pytest.mark.parametrize(
    "drift",
    [
        "trigger",
        "rule",
        "parent",
        "rls",
        "fk",
        "missing_pk",
        "pk_keys",
        "extra_column",
        "type",
        "default",
        "generated",
        "collation",
        "extra_index",
        "index_flags",
        "index_owner",
        "acl",
    ],
)
async def test_profile_copy_target_drift_refuses_before_transport(drift):
    model = fhir.ProviderDirectoryPractitioner
    catalog = _legacy_target_catalog(model)
    _drift_target_catalog(catalog, drift)
    with pytest.raises(RuntimeError, match="native_copy_custody_invalid"):
        native_copy._target_catalog(fhir, model, "scope_target", 17, 23, catalog, phase="completed")


async def test_profile_copy_bucket_expression_uses_existing_native_guard(monkeypatch):
    model = fhir.ProviderDirectoryPractitionerRole
    catalog = _legacy_target_catalog(model)
    driver = SimpleNamespace(
        execute=AsyncMock(), fetchval=AsyncMock(return_value=23), fetch=AsyncMock(return_value=[catalog])
    )
    publisher = SimpleNamespace(connection=driver, policy=SimpleNamespace(schema="synthetic", owner_role="owner"))
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: SimpleNamespace(run_id="run"))
    monkeypatch.setattr(fhir, "_provider_directory_artifact_scope_table_name", lambda *_args: "scope_target")
    index_map = deepcopy(catalog["indexes"][1])
    monkeypatch.setattr(fhir, "_profile_bucket_index_row", AsyncMock(return_value=index_map))
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="bucket_index_changed"):
        await native_copy._target(fhir, publisher, model, "scope_target", 17, phase="completed")
    driver.execute.assert_awaited_once_with('LOCK TABLE "synthetic"."scope_target" IN ROW EXCLUSIVE MODE NOWAIT')


def _completed_target_family(monkeypatch):
    models = (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS)
    targets = tuple((model, "scope_" + str(number), 100 + number) for number, model in enumerate(models))
    catalog_by_root = {
        model.__tablename__: {**_legacy_target_catalog(model, name), "oid": oid} for model, name, oid in targets
    }
    driver = SimpleNamespace(
        execute=AsyncMock(),
        fetchval=AsyncMock(return_value=23),
        fetch=AsyncMock(return_value=list(reversed(catalog_by_root.values()))),
    )
    publisher = SimpleNamespace(connection=driver, policy=SimpleNamespace(schema="synthetic", owner_role="owner"))
    name_by_root = {model.__tablename__: name for model, name, _ in targets}
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: SimpleNamespace(run_id="run"))
    monkeypatch.setattr(fhir, "_provider_directory_artifact_scope_table_name", lambda root, _run: name_by_root[root])
    monkeypatch.setattr(fhir, "_profile_bucket_index_row", AsyncMock(return_value={"synthetic": True}))
    monkeypatch.setattr(fhir, "_validate_profile_bucket_index_shape", Mock())
    monkeypatch.setattr(fhir, "_validate_profile_bucket_index_tablespace", Mock())
    return publisher, targets, catalog_by_root


async def test_completed_family_uses_one_fresh_catalog_and_checks_both_bucket_indexes(monkeypatch):
    publisher, targets, expected = _completed_target_family(monkeypatch)
    assert await native_copy._target_family_catalog(fhir, publisher, targets, phase="completed") == expected
    names = sorted(name for _, name, _ in targets)
    publisher.connection.execute.assert_awaited_once_with(
        "LOCK TABLE " + ",".join(f'"synthetic"."{name}"' for name in names) + " IN ROW EXCLUSIVE MODE NOWAIT"
    )
    publisher.connection.fetchval.assert_awaited_once()
    publisher.connection.fetch.assert_awaited_once_with(native_copy._TARGET_CATALOG_SQL, "synthetic", names, 23, [])
    assert fhir._profile_bucket_index_row.await_count == 2
    assert fhir._validate_profile_bucket_index_shape.call_count == 2
    assert fhir._validate_profile_bucket_index_tablespace.call_count == 2
    expected[targets[-1][0].__tablename__]["acl_closed"] = False
    with pytest.raises(RuntimeError, match="native_copy_custody_invalid"):
        await native_copy._target_family_catalog(fhir, publisher, targets, phase="completed")
    assert publisher.connection.fetch.await_count == 2


@pytest.mark.parametrize("damage", ["missing", "duplicate", "unexpected", "replaced_oid"])
async def test_completed_family_refuses_incomplete_or_replaced_catalog(monkeypatch, damage):
    publisher, targets, _expected = _completed_target_family(monkeypatch)
    rows = publisher.connection.fetch.return_value
    if damage == "missing":
        rows.pop()
    elif damage == "duplicate":
        rows[-1] = dict(rows[0])
    elif damage == "unexpected":
        rows[-1]["relname"] = "other_scope"
    else:
        rows[-1]["oid"] += 1000
    with pytest.raises(RuntimeError, match="native_copy_custody_invalid"):
        await native_copy._target_family_catalog(fhir, publisher, targets, phase="completed")


async def _copy_pages():
    """One synthetic transport page; native custody is tested separately."""
    yield [{"npi": 123}]


def _copy_request():
    """Use the real frozen source-batch type and existing Source model encoder."""
    coordinate = batch()
    table = fhir._ProviderDirectoryArtifactScopeTableProjection(
        fhir.ProviderDirectorySource.__tablename__, "source", 1, 128, (coordinate,)
    )
    return native_copy.NativeCopyRequest(
        fhir.ProviderDirectorySource,
        "scratch_source",
        17,
        coordinate,
        table,
        {},
        "SELECT source_id FROM synthetic_source",
        {},
        fhir._ProviderDirectoryArtifactScopeExactProjection((table,)),
    )


async def test_native_copy_unissued_and_missing_source_custody_refuse(artifact_wave):
    _, database, _, *_ = artifact_wave
    forged = native_copy.NativeCopyWitness("{}", 17, ("npi",), 1, (1,), "{}")
    assert not native_copy.matches(forged, None, asyncio.current_task())
    async with custody.registry_owned_wal_transaction(database) as outcome:
        with pytest.raises(Exception, match="publisher_connection_required"):
            await native_copy._origin(fhir, outcome)
    assert outcome.is_committed


async def test_native_copy_transport_retains_whole_original_owner(artifact_wave, monkeypatch):
    """Exercise COPY protocol and owner completion; do not simulate native authority."""
    _, database, admission, *_ = artifact_wave
    request = _copy_request()
    origin_by_outcome = {}

    async def authenticate(_module, outcome):
        publisher = SimpleNamespace(policy=SimpleNamespace(schema="synthetic"))
        return origin_by_outcome.setdefault(
            id(outcome), (publisher, outcome.retained_driver, {"backend": outcome.retained_pid})
        )

    monkeypatch.setattr(native_copy, "_origin", authenticate)
    monkeypatch.setattr(fhir, "_assert_artifact_scope_projection", lambda projection: None)
    monkeypatch.setattr(native_copy, "_source", AsyncMock())
    monkeypatch.setattr(native_copy, "_target", AsyncMock(return_value='"synthetic"."scratch_source"'))
    payload_sql = AsyncMock()
    monkeypatch.setattr(database, "execute", payload_sql)
    monkeypatch.setattr(fhir, "_copy_record", lambda row, columns, json_columns: tuple(row.values()))
    async with custody.registry_owned_wal_transaction(database) as outcome:
        copy_command = AsyncMock(return_value="COPY 1")
        outcome.retained_driver.copy_records_to_table = copy_command
        async with fhir.profile_artifact_custody.copy_payload_transaction(fhir, request, outcome):
            assert await fhir.profile_artifact_custody.execute_copy(fhir, request, _copy_pages(), outcome) == 1
        group = admission.wal_tracker.owned_control_transaction_groups[0]
        witness = group["statement_custody"]["witness"]
        assert type(witness) is native_copy.NativeCopyWitness
        assert not native_copy.matches(witness, outcome, asyncio.current_task())
        assert not group["consumed"]
        copy_command.assert_awaited_once()
        payload_sql.assert_not_awaited()
        assert witness.semantic_scope == "transport_only"
    fhir._profile_owned_transaction_capture(outcome, None)
    assert group["consumed"] and native_copy.matches(witness, outcome, asyncio.current_task())
    capture = group["statement_custody"]["native_copy_measurement"]
    assert capture["wal_record_bytes"] == int(outcome.measurement.wal_bytes_delta)
    assert capture["global_physical_lsn_span"] == outcome.measurement.global_insert_lsn_span
    assert capture["global_physical_lsn_span_authority"] == "diagnostic_only"
    assert not capture["accounting_authority"] and not capture["reservation_refund"]
    assert not native_copy.matches(replace(witness), outcome, asyncio.current_task())
    assert not native_copy.matches(witness, replace(outcome), asyncio.current_task())
    assert not native_copy.matches(witness, outcome, object())
    outcome.cleanup_complete = False
    assert not native_copy.matches(witness, outcome, asyncio.current_task())
    outcome.cleanup_complete = True
    object.__setattr__(witness, "target_oid", 18)
    assert not native_copy.matches(witness, outcome, asyncio.current_task())


@pytest.mark.parametrize("fault", ["oversized_page", "excess_rows", "command_count"])
async def test_native_copy_transport_refuses_invalid_page_or_command(artifact_wave, monkeypatch, fault):
    """Check transport refusal without substituting successful native authentication."""
    _, database, _, *_ = artifact_wave
    request = _copy_request()
    page = [{"npi": 123}] * (513 if fault == "oversized_page" else 2 if fault == "excess_rows" else 1)

    async def pages():
        yield page

    monkeypatch.setattr(fhir, "_copy_record", lambda row, columns, json_columns: tuple(row.values()))
    async with custody.registry_owned_wal_transaction(database) as outcome:
        publisher = SimpleNamespace(policy=SimpleNamespace(schema="synthetic"))
        origin = publisher, outcome.retained_driver, {"backend": outcome.retained_pid}
        monkeypatch.setattr(native_copy, "_origin", AsyncMock(return_value=origin))
        command = AsyncMock(return_value="COPY 0")
        outcome.retained_driver.copy_records_to_table = command
        with pytest.raises(RuntimeError, match="native_copy_custody_invalid"):
            await native_copy._copy_pages(fhir, request, pages(), outcome, origin, ("npi",))
        assert command.await_count == (1 if fault == "command_count" else 0)


async def test_native_copy_transport_requires_separate_completed_snapshot_closure():
    witness = native_copy.NativeCopyWitness("{}", 17, ("source_id",), 1, (1,), "{}")
    assert witness.semantic_scope == "transport_only"
    assert not native_copy.matches(witness, None, asyncio.current_task())


@pytest.fixture
def artifact_wave(control_wave, monkeypatch):
    state, database, admission, *_ = control_wave
    events = []

    async def project(*args):
        events.append("project")

    async def reserve(*args, **kwargs):
        events.append("reserve")

    monkeypatch.setattr(fhir, "_project_artifact_batch_capacity", project)
    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", reserve)

    @asynccontextmanager
    async def window(relation_class, relations=()):
        token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), relation_class))
        try:
            yield
        finally:
            fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)

    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", window)
    monkeypatch.setattr(
        database,
        "first",
        AsyncMock(return_value={"projected_rows": 1, "projected_logical_bytes": 128, "last_cursor": None}),
    )

    class ReturningSession(Session):
        async def execute(self, statement, parameters):
            result = await super().execute(statement, parameters)
            if str(statement).endswith(" RETURNING 1;"):
                result.scalar()[0]["Plan"]["Actual Rows"] = result.rowcount
            return result

    monkeypatch.setattr(custody, "AsyncSession", ReturningSession)
    scalar = AsyncMock(wraps=database.scalar)
    status = AsyncMock(wraps=database.status)
    monkeypatch.setattr(database, "scalar", scalar)
    monkeypatch.setattr(database, "status", status)
    return state, database, admission, events, scalar, status


def batch(kind="source", *, rows=1):
    return fhir._ProviderDirectoryArtifactScopeBatchProjection(
        batch_number=7,
        source_id="synthetic_source",
        dataset_id=None if kind == "source" else "synthetic_dataset",
        evidence_run_id=None if kind == "source" else "synthetic_run",
        resource_type=kind,
        after_resource_id=None,
        last_resource_id="r1" if kind != "source" and rows else None,
        projected_rows=rows,
        projected_logical_bytes=rows * 128,
    )


async def source_insert(fixture):
    source_batch = batch()
    projection = SimpleNamespace(batches=(source_batch,), projected_rows=1)
    statement = fhir._provider_directory_artifact_source_insert_sql(
        fhir.ProviderDirectorySource, "synthetic", "scratch_source"
    )
    result = await fhir._execute_artifact_source_batch(
        source_batch,
        projection,
        "SELECT synthetic_projection;",
        statement,
        relation_ref='"synthetic"."scratch_source"',
    )
    return result, source_batch, projection, statement


async def resource_insert(fixture, *, rows=1):
    resource_batch = batch("Practitioner", rows=rows)
    fixture[1].first.return_value = {
        "projected_rows": rows,
        "projected_logical_bytes": rows * 128,
        "last_cursor": resource_batch.last_resource_id,
    }
    statement = fhir._artifact_resource_batch_insert_sql(
        fhir.ProviderDirectoryPractitioner, "synthetic", "scratch_resource"
    )
    params_by_name = {
        "source_id": resource_batch.source_id,
        "dataset_id": resource_batch.dataset_id,
        "evidence_run_id": resource_batch.evidence_run_id,
        "resource_type": "Practitioner",
        "after_resource_id": None,
        "scope_batch_size": 2,
    }
    result = await fhir._execute_artifact_resource_batch(
        fhir.ProviderDirectoryPractitioner,
        "synthetic",
        statement,
        params_by_name,
        resource_batch,
        relation_ref='"synthetic"."scratch_resource"',
    )
    return result, resource_batch, None, statement


@pytest.mark.parametrize("kind", ["source", "resource"])
async def test_artifact_original_coordinate_statement_and_native_terminal_owner(artifact_wave, kind):
    state, _, admission, events, scalar, status = artifact_wave
    result, original_batch, projection, statement = await (
        source_insert(artifact_wave) if kind == "source" else resource_insert(artifact_wave)
    )
    assert result == 1 and events == ["project", "reserve"]
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    identity = group["original_identity"]
    assert identity[0:2] == ("artifact_scope_payload", kind)
    assert identity[3] is original_batch and identity[4] is projection
    assert identity[5] is statement
    record = group["statement_custody"]
    outcome = group["original_outcome"]
    assert record is group["original_statement_custody"]
    assert record["outcome"] is outcome and record["session"] is outcome.session
    assert record["witness"] is record["original_witness"] and record["witness"].affected_rows == 1
    assert record["statement"] is statement and record["params"] == identity[6]
    assert group["consumed"] and outcome.is_committed and outcome.cleanup_complete
    assert not group["accounting_authority"] and not group["reservation_refund"]
    scalar.assert_awaited_once_with(
        fhir.profile_statement_wal._EXPLAIN + statement.strip()[:-1] + " RETURNING 1;", **record["params"]
    )
    status.assert_not_awaited()
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1


async def test_artifact_zero_probe_has_original_reader_owner_and_no_payload(artifact_wave):
    state, _, admission, events, scalar, status = artifact_wave
    result, original_batch, _, statement = await resource_insert(artifact_wave, rows=0)
    assert result == 0 and events == []
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["identity"][0] == "artifact_scope_zero_probe" and group["identity"][3] is original_batch
    assert group["identity"][5] is statement and group["statement_custody"] is None
    assert group["consumed"] and group["original_outcome"].is_committed
    scalar.assert_not_awaited()
    status.assert_not_awaited()
    assert "write" not in state.connections[0].events


@pytest.mark.parametrize("kind", ["source", "resource"])
@pytest.mark.parametrize(
    "mode", ["rollback", "cancel", "postcommit_sample", "unknown_commit", "restore_failure", "overrun"]
)
async def test_artifact_failure_preserves_primary_commit_truth_and_held_owner(artifact_wave, kind, mode):
    state, _, admission, *_ = artifact_wave
    state.mode = mode
    with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception):
        await (source_insert(artifact_wave) if kind == "source" else resource_insert(artifact_wave))
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert not group["consumed"] and group["status"] == "incomplete"
    assert group["failure"] is not None and outcome.cleanup_complete
    if mode in ["postcommit_sample", "restore_failure"]:
        assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert not outcome.is_committed and outcome.commit_state == "attempted"
    else:
        assert outcome.commit_state == "rolled_back"
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


async def test_artifact_projection_drift_rolls_back_before_insert(artifact_wave):
    _, database, admission, _, scalar, status = artifact_wave
    database.first.return_value = {"projected_rows": 2, "projected_logical_bytes": 128}
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="source_projection_changed"):
        await source_insert(artifact_wave)
    assert admission.wal_tracker.owned_control_transaction_groups[0]["original_outcome"].commit_state == "rolled_back"
    scalar.assert_not_awaited()
    status.assert_not_awaited()


@pytest.mark.parametrize("operation", ["layout", "analyze", "drop"])
async def test_artifact_ordinary_control_writers_have_exact_operation_relation_and_owner(artifact_wave, operation):
    _, _, admission, _, _, status = artifact_wave
    if operation == "layout":
        await fhir._create_provider_directory_artifact_scope_layout(
            fhir.ProviderDirectorySource, "synthetic", "scratch"
        )
    elif operation == "analyze":
        await fhir._analyze_artifact_scope_table("synthetic", "scratch")
    else:
        await fhir._drop_artifact_scope_tables("synthetic", ["scratch"])
    groups = admission.wal_tracker.owned_control_transaction_groups
    assert len(groups) == status.await_count == 1
    for group, call in zip(groups, status.await_args_list, strict=True):
        assert group["identity"][:2] == ("artifact_scope_" + operation, '"synthetic"."scratch"')
        assert group["identity"][2] is call.args[0]
        assert group["original_outcome"].is_committed and group["consumed"]
        assert not group["accounting_authority"] and not group["reservation_refund"]


async def test_artifact_recovery_retains_one_original_atomic_owner_and_all_statements(artifact_wave, monkeypatch):
    state, _, admission, _, _, status = artifact_wave
    plan = SimpleNamespace(
        relation_by_table={fhir.ProviderDirectorySource.__tablename__: "current"},
        model_by_table_name={fhir.ProviderDirectorySource.__tablename__: fhir.ProviderDirectorySource},
        created_tables=[],
    )
    coordinate = fhir._ArtifactScopeRecoveryCoordinate(fhir.ProviderDirectorySource.__tablename__, "prior", "current")
    identities_by_relation = {"prior": (17, "r", "u")}
    monkeypatch.setattr(fhir, "_artifact_scope_relation_identities", AsyncMock(return_value=identities_by_relation))
    monkeypatch.setattr(fhir, "_assert_artifact_scope_recovery_unreferenced", AsyncMock())
    monkeypatch.setattr(fhir, "_assert_artifact_scope_recovery_layouts", AsyncMock())
    await fhir._replace_terminal_artifact_scope(
        "synthetic", plan, (coordinate,), ("prior",), identities_by_relation, admission
    )
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert group["identity"][0] == "artifact_scope_recovery"
    assert group["identity"][1][1][0] is coordinate
    statements = group["identity"][2]
    assert len(statements) == status.await_count == 3
    assert [row[0] for row in statements] == [call.args[0] for call in status.await_args_list]
    assert statements[0][0].startswith("LOCK TABLE ") and statements[-1][0].startswith("DROP TABLE ")
    assert all(row[2] is outcome.session for row in statements)
    assert group["consumed"] and outcome.is_committed and outcome.cleanup_complete
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1
    assert plan.created_tables == ["current"]


@pytest.mark.parametrize("field", ["witness", "params", "outcome", "statement_custody"])
async def test_artifact_missing_or_replaced_root_keeps_authentic_commit_and_incomplete_exposure(
    artifact_wave, monkeypatch, field
):
    _, _, admission, *_ = artifact_wave
    consume = fhir.profile_control_custody.consume_owned_groups

    def corrupt(module, outcome, failure, *, groups=None):
        group = groups[0]
        if field == "statement_custody":
            group[field] = None
        elif field == "witness":
            group["statement_custody"][field] = replace(group["statement_custody"][field])
        else:
            group["statement_custody"][field] = object()
        consume(module, outcome, failure, groups=groups)

    monkeypatch.setattr(fhir.profile_control_custody, "consume_owned_groups", corrupt)
    with pytest.raises(RuntimeError, match="control_custody_incomplete"):
        await source_insert(artifact_wave)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["original_outcome"].is_committed and group["original_outcome"].status == "committed_measured"
    assert not group["consumed"]


async def test_artifact_unknown_borrowed_owner_executes_original_once_and_holds_uncertainty(artifact_wave, monkeypatch):
    _, database, admission, _, scalar, status = artifact_wave
    monkeypatch.setattr(database, "transaction", nullcontext)
    original_batch = batch()
    async with custody.registry_owned_wal_transaction(database) as outcome:
        token = custody._ACTIVE_OWNED_TRANSACTION.set(None)
        try:
            async with fhir.profile_artifact_custody.payload_transaction(
                fhir, "source", "scratch", original_batch, object(), "INSERT INTO scratch SELECT :npi;", {"npi": 1}
            ):
                assert (
                    await fhir.profile_artifact_custody.execute_insert(
                        fhir, "INSERT INTO scratch SELECT :npi;", {"npi": 1}
                    )
                    == 1
                )
        finally:
            custody._ACTIVE_OWNED_TRANSACTION.reset(token)
    fhir._profile_owned_transaction_capture(outcome, None)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    status.assert_awaited_once_with("INSERT INTO scratch SELECT :npi;", npi=1)
    scalar.assert_not_awaited()
    assert group["original_outcome"] is None and not group["consumed"]
    assert group["statement_custody"]["witness"] is None and outcome.is_committed


@pytest.mark.parametrize("mode", ["body", "cancel", "postcommit_sample", "unknown_commit"])
async def test_artifact_atomic_recovery_failure_preserves_owner_and_primary(artifact_wave, monkeypatch, mode):
    state, _, admission, _, _, status = artifact_wave
    plan = SimpleNamespace(
        relation_by_table={fhir.ProviderDirectorySource.__tablename__: "current"},
        model_by_table_name={fhir.ProviderDirectorySource.__tablename__: fhir.ProviderDirectorySource},
        created_tables=[],
    )
    coordinate = fhir._ArtifactScopeRecoveryCoordinate(fhir.ProviderDirectorySource.__tablename__, "prior", "current")
    identities_by_relation = {"prior": (17, "r", "u")}
    primary = OSError("synthetic original recovery failure")
    monkeypatch.setattr(fhir, "_artifact_scope_relation_identities", AsyncMock(return_value=identities_by_relation))
    monkeypatch.setattr(
        fhir, "_assert_artifact_scope_recovery_unreferenced", AsyncMock(side_effect=primary if mode == "body" else None)
    )
    monkeypatch.setattr(fhir, "_assert_artifact_scope_recovery_layouts", AsyncMock())
    if mode != "body":
        state.mode = mode
    with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception) as caught:
        await fhir._replace_terminal_artifact_scope(
            "synthetic", plan, (coordinate,), ("prior",), identities_by_relation, admission
        )
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert not group["consumed"] and group["status"] == "incomplete" and outcome.cleanup_complete
    assert state.checkedout == 0
    if mode == "body":
        assert caught.value is primary and group["failure"] is primary and status.await_count == 1
    if mode == "postcommit_sample":
        assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert not outcome.is_committed and outcome.commit_state == "attempted"
    else:
        assert outcome.commit_state == "rolled_back"


async def test_artifact_failed_concurrent_batches_drain_original_tasks_and_connections(artifact_wave):
    state, _, admission, *_ = artifact_wave
    state.mode = "rollback"
    tasks = [asyncio.create_task(source_insert(artifact_wave)) for _ in range(2)]
    with pytest.raises(Exception):
        await fhir._gather_provider_directory_profile_tasks(tasks)
    assert all(task.done() for task in tasks) and state.checkedout == 0
    assert all(connection.closed for connection in state.connections)
    assert all(not group["consumed"] for group in admission.wal_tracker.owned_control_transaction_groups)
