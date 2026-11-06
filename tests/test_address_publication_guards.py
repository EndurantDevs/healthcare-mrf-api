# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partial replacement keeps its source, stage and publication boundaries closed."""

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from process import entity_address_candidate_preparation as candidate
from process import entity_address_native_publication as publication
from process import entity_address_rows_preparation as rows
from process import provider_directory_published_replacement_inputs as inputs
from process import provider_directory_published_replacement_journal as journal

address = importlib.import_module("process.entity_address_unified")
directory = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def publication_case(monkeypatch):
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock())
    monkeypatch.setattr(address, "db", database)
    monkeypatch.setattr(address, "ensure_database", AsyncMock())
    monkeypatch.setattr(publication, "controlled_publication_enabled", lambda _ctx: False)
    monkeypatch.setattr(candidate, "current", lambda: None)
    monkeypatch.setattr(candidate, "geo_dependency_options", lambda: {})
    monkeypatch.setattr(address, "make_class", lambda *_args: SimpleNamespace(__tablename__="candidate_stage"))
    monkeypatch.setattr(address, "_has_table", AsyncMock(return_value=False))
    read = AsyncMock(side_effect=AssertionError("publication guard reached data reads"))
    monkeypatch.setattr(address.preparation_admission, "read_scalar", read)
    context_by_field = {
        "import_date": "synthetic",
        "context": {
            "run": True,
            "serving_only_refresh": True,
            "refresh_mode": address.ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL,
            "publish_requested": False,
        },
    }
    return SimpleNamespace(database=database, read=read, context_by_field=context_by_field)


@pytest.mark.asyncio
async def test_prepare_only_requires_internal_inputs_before_database_access(publication_case):
    with pytest.raises(RuntimeError, match="internal desired inputs"):
        await address.publish_entity_address_unified_generation(publication_case.context_by_field, prepare_only=True)
    address.ensure_database.assert_not_awaited()
    publication_case.read.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_jobs_cannot_enter_publication_or_cleanup(publication_case):
    publication_case.context_by_field["context"]["run"] = False
    assert await address.publish_entity_address_unified_generation(publication_case.context_by_field) is None
    address.ensure_database.assert_not_awaited()
    publication_case.database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_dependency_binding_drift_refuses_before_stage_reads(publication_case, monkeypatch):
    monkeypatch.setattr(candidate, "geo_dependency_options", lambda: {"dependency_bindings": {"generation": 1}})
    monkeypatch.setattr(address.geo_projection, "validate_projection_dependency_bindings", lambda *_: {"generation": 2})
    with pytest.raises(RuntimeError, match="desired dependency bindings differ"):
        await address.publish_entity_address_unified_generation(
            publication_case.context_by_field, dependency_bindings={"generation": 2}
        )
    publication_case.read.assert_not_awaited()
    publication_case.database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes,protected,message",
    [
        ({}, True, "complete isolated seven-table generation"),
        ({"partial_main_patch_publish": True}, False, "main-table patch publishing is disabled"),
        ({"partial_provider_directory_main_patch_publish": True}, False, "main-table patch publishing is disabled"),
        ({"partial_support_patch_publish": True}, False, "support patch publishing is disabled"),
        ({"partial_provider_directory_replacement_publish": True}, False, "affected location scope"),
        (
            {
                "partial_provider_directory_replacement_publish": True,
                "partial_provider_directory_coordinate_scope_table": "missing",
            },
            False,
            "affected location scope",
        ),
    ],
)
async def test_partial_publication_refuses_unsafe_cutover_before_data_reads(
    publication_case, monkeypatch, changes, protected, message
):
    publication_case.context_by_field["context"].update(changes)
    monkeypatch.setattr(publication, "controlled_publication_enabled", lambda _ctx: protected)
    with pytest.raises(RuntimeError, match=message):
        await address.publish_entity_address_unified_generation(publication_case.context_by_field)
    publication_case.read.assert_not_awaited()
    publication_case.database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_stage_cleanup_visits_only_explicit_nonempty_artifacts(monkeypatch):
    drop = AsyncMock()
    monkeypatch.setattr(address.preparation_admission, "drop_stage", drop)
    await address._drop_stage_artifacts(
        "sample",
        SimpleNamespace(__tablename__="main_stage"),
        {object(): SimpleNamespace(__tablename__="support_stage")},
        extra_tables=(None, "", "affected_stage"),
    )
    assert drop.await_args_list == [
        call("sample", "main_stage"),
        call("sample", "support_stage"),
        call("sample", "affected_stage"),
    ]


@pytest.mark.asyncio
async def test_row_scratch_cleanup_cannot_drop_an_unowned_name(monkeypatch):
    drop = AsyncMock()
    monkeypatch.setattr(rows.admission, "drop_stage", drop)
    monkeypatch.setattr(rows.ordinary_preparation, "current", lambda: SimpleNamespace(owned_oids={"owned": 101}))
    token = rows.admission._ADMISSION.set(None)
    try:
        await rows._drop_scratch(SimpleNamespace(), "sample", ("owned", "foreign"), {"foreign": 202})
    finally:
        rows.admission._ADMISSION.reset(token)
    drop.assert_awaited_once_with("sample", "owned")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "build_network,missing",
    [
        (False, None),
        (True, None),
        (False, address.EntityAddressPlanBridge.__main_table__),
        (False, address.EntityAddressProcedureBridge.__main_table__),
        (False, address.EntityAddressMedicationBridge.__main_table__),
        (True, address.EntityAddressNetworkBridge.__main_table__),
    ],
)
async def test_bridge_reuse_requires_every_selected_retained_relation(monkeypatch, build_network, missing):
    lookup = AsyncMock(side_effect=lambda _schema, table: table != missing)
    monkeypatch.setattr(address, "_has_table", lookup)
    assert await address._is_support_bridge_reuse_available("sample", build_network_bridge=build_network) is (
        missing is None
    )
    tables = {statement.args[1] for statement in lookup.await_args_list}
    assert (address.EntityAddressNetworkBridge.__main_table__ in tables) is build_network


@pytest.mark.asyncio
async def test_absent_compatibility_sources_do_not_query_or_borrow_another_dataset(monkeypatch):
    read = AsyncMock()
    monkeypatch.setattr(address.preparation_admission, "read_scalar", read)
    assert await address._has_provider_directory_compatibility_data("sample", {}) is False
    read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("exists", [False, True])
async def test_missing_latest_source_or_run_returns_an_empty_scope(monkeypatch, exists):
    monkeypatch.setattr(address, "_has_table", AsyncMock(return_value=exists))
    read = AsyncMock(return_value=None)
    monkeypatch.setattr(address.preparation_admission, "read_first", read)
    assert await address._latest_provider_directory_partial_scope("sample") == (None, [], [])
    assert read.await_count == int(exists)


@pytest.mark.parametrize("sources,validated", [(None, False), ([], False), (["one", "two"], False), (["one"], True)])
def test_replacement_sql_cannot_expand_or_borrow_validated_candidate_scope(sources, validated):
    with pytest.raises(ValueError, match="replacement_scope_invalid"):
        directory._provider_directory_artifact_dataset_selection_sql(
            sources, should_select_validated_candidates=validated, allow_published_replacement=True
        )


def test_empty_peer_set_cannot_grant_ordinary_alias_authority():
    assert candidate._ordinary_serving_peers_sql("sample", ()) == "FALSE"
    assert journal.capture(SimpleNamespace(), object(), {}) is None


@pytest.mark.parametrize("value", [None, 1, "not-a-time", "2026-01-01T00:00:00Z"])
def test_retained_timestamp_requires_the_exact_canonical_encoding(value):
    assert inputs._is_timestamp(value) is False
    assert inputs._is_timestamp("2026-01-01T00:00:00+00:00") is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "required,message", [(["stage"], "missing required staged relations"), ([], "no relations to lock")]
)
async def test_cutover_refuses_missing_stages_or_an_empty_lock_set(monkeypatch, required, message):
    database = SimpleNamespace(status=AsyncMock())
    monkeypatch.setattr(address, "db", database)
    monkeypatch.setattr(address.preparation_admission, "read_scalar", AsyncMock(return_value=True))
    monkeypatch.setattr(address, "_existing_cutover_relations", AsyncMock(return_value=[]))
    with pytest.raises(RuntimeError, match=message):
        await address._acquire_cutover_locks("sample", ["stage"], required)
    database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("persistence", [None, "u"])
async def test_missing_or_still_unlogged_stage_cannot_reach_cutover(monkeypatch, persistence):
    @asynccontextmanager
    async def logging_scope(*_args):
        yield

    database = SimpleNamespace(status=AsyncMock())
    monkeypatch.setattr(address, "db", database)
    monkeypatch.setattr(address.preparation_admission, "stage_logging_scope", logging_scope)
    monkeypatch.setattr(address, "_stage_table_persistence", AsyncMock(return_value=persistence))
    with pytest.raises(RuntimeError, match="does not exist|not permanent"):
        await address._ensure_promoted_stage_logged("sample", "stage")
    if persistence is None:
        database.status.assert_not_awaited()
    else:
        database.status.assert_awaited_once()
        assert "SET LOGGED" in database.status.await_args.args[0]


def test_replacement_cannot_publish_a_large_loss_of_existing_rows():
    with pytest.raises(RuntimeError, match="below 80% of previous publish"):
        address._validate_publish_row_count(stage_rows=79, previous_rows=100, test_mode=False, min_rows_required=1)


def test_partial_selection_rejects_an_unknown_scope():
    with pytest.raises(ValueError, match="Unsupported provider_directory_partial_scope"):
        address._entity_address_provider_directory_partial_scope({"provider_directory_partial_scope": "unreviewed"})


@pytest.mark.parametrize("raw,expected", [(None, []), ((" source ", "source", None, "other"), ["source", "other"])])
def test_partial_source_vector_does_not_invent_empty_or_duplicate_identifiers(raw, expected):
    assert address._coerce_str_list(raw) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing,partial,reuse,message",
    [
        ("geo_state", False, False, "migrated geo assurance schema"),
        ("alias_state", False, False, "migrated address alias schema"),
        ("npi_address", False, False, "geo assurance relations"),
        ("tiger.zcta5", False, False, "geo assurance relations"),
        (None, True, True, "cannot be combined"),
        ("candidate_stage", False, True, "does not exist"),
        ("stale_aliases", True, False, "full entity-address-unified rebuild is required"),
    ],
)
async def test_build_refuses_incomplete_or_stale_stage_dependencies_before_loading(
    publication_case, monkeypatch, missing, partial, reuse, message
):
    task_by_field = {
        "skip_publish": True,
        "refresh_mode": address.ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL if partial else "full",
        "provider_directory_partial_scope": "latest-run",
        "provider_directory_source_ids": ["source"],
        "provider_directory_run_id": "root",
        "provider_directory_dataset_id": "dataset" if partial else None,
    }
    publication_case.context_by_field["context"]["stage_prepared"] = missing != "candidate_stage"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "sample")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE", "1" if reuse else "0")
    monkeypatch.setattr(address.serving_recovery, "resume_address", AsyncMock(return_value=None))
    monkeypatch.setattr(address, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(address, "_address_alias_generation", AsyncMock(return_value=1))
    missing_table = {
        "geo_state": address.geo_projection.GEO_ASSURANCE_STATE_TABLE,
        "alias_state": address.address_alias_sql.ADDRESS_ALIAS_STATE_TABLE,
    }.get(missing, missing)
    monkeypatch.setattr(
        address,
        "_has_table",
        AsyncMock(side_effect=lambda schema, name: name != missing_table and f"{schema}.{name}" != missing_table),
    )
    read = AsyncMock(return_value=2)
    monkeypatch.setattr(address.preparation_admission, "read_scalar", read)
    create = AsyncMock()
    monkeypatch.setattr(address.preparation_admission, "create_stage_table", create)
    with pytest.raises(RuntimeError, match=message):
        await address.process_entity_address_unified_data(publication_case.context_by_field, task_by_field)
    create.assert_not_awaited()
    publication_case.database.status.assert_not_awaited()
    assert read.await_count == int(missing == "stale_aliases")


@pytest.mark.asyncio
async def test_compatibility_lookup_is_limited_to_declared_address_sources(monkeypatch):
    table = address.PROVIDER_DIRECTORY_COMPATIBILITY_ADDRESS_TABLES[0]
    read = AsyncMock(return_value=False)
    monkeypatch.setattr(address.preparation_admission, "read_scalar", read)
    assert await address._has_provider_directory_compatibility_data("sample", {table: True, "foreign": True}) is False
    sql = read.await_args.args[1]
    assert f"sample.{table}" in sql and "foreign" not in sql


@pytest.mark.asyncio
async def test_absent_archive_coordinates_do_not_invent_inherited_rows(monkeypatch):
    monkeypatch.setattr(address.preparation_admission, "read_all", AsyncMock(return_value=[]))
    assert await address._inherit_archive_coordinates("sample", "stage") == {"inherited_rows": 0, "ambiguous_rows": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_binding", [False, True])
async def test_geo_projection_refusal_propagates_into_the_transaction_context(monkeypatch, changed_binding):
    exits = []

    @asynccontextmanager
    async def transaction():
        try:
            yield
        except BaseException:
            exits.append("rollback")
            raise

    database = SimpleNamespace(status=AsyncMock(return_value="UPDATE 2"))
    monkeypatch.setattr(address, "db", database)
    monkeypatch.setattr(address.preparation_admission, "native_transaction", transaction)
    monkeypatch.setattr(address, "_apply_entity_address_transaction_settings", AsyncMock())
    monkeypatch.setattr(address.geo_projection, "validate_projection_dependency_bindings", lambda *_: {"generation": 1})
    monkeypatch.setattr(
        address.geo_projection, "projection_dependency_lock_sql", lambda *_args, **_kw: "LOCK DEPENDENCIES"
    )
    monkeypatch.setattr(
        address.geo_projection, "projection_dependency_bindings_match_sql", lambda *_: "dependencies_match"
    )
    monkeypatch.setattr(address.geo_projection, "projection_state_available_sql", lambda *_: "projection_available")
    monkeypatch.setattr(address, "_materialize_geo_assurance_sql", lambda *_args, **_kw: "PROJECT STAGE")
    monkeypatch.setattr(address, "_validate_geo_assurance_projection", AsyncMock(return_value=0))
    monkeypatch.setattr(address, "_geo_assurance_candidate_receipt_sql", lambda *_args, **_kw: "READ STAGE OID")
    reads = [False] if changed_binding else [True, None]
    monkeypatch.setattr(address.preparation_admission, "read_scalar", AsyncMock(side_effect=reads))
    message = "dependency binding changed" if changed_binding else "stage disappeared before receipt"
    with pytest.raises(RuntimeError, match=message):
        await address._project_geo_assurance_transaction(
            "sample", "stage", force=False, dependency_bindings={"generation": 1} if changed_binding else None
        )
    assert exits == ["rollback"]
    assert [statement.args[0] for statement in database.status.await_args_list] == (
        ["LOCK DEPENDENCIES"] if changed_binding else ["LOCK DEPENDENCIES", "PROJECT STAGE"]
    )


@pytest.mark.asyncio
async def test_failed_parallel_stage_index_build_is_not_treated_as_success(monkeypatch):
    failure = RuntimeError("index build refused")
    build = AsyncMock(side_effect=[None, failure])
    monkeypatch.setattr(address, "_build_stage_index", build)
    with pytest.raises(RuntimeError) as raised:
        await address._run_stage_index_statements([("first", "INDEX FIRST"), ("second", "INDEX SECOND")], {}, 2, [])
    assert raised.value is failure and build.await_count == 2


@pytest.mark.asyncio
async def test_missing_geo_taxonomy_index_cannot_mark_the_stage_valid(monkeypatch):
    monkeypatch.setattr(address.preparation_admission, "read_scalar", AsyncMock(return_value=False))
    context_by_field = {"stage_index_profile": "all"}
    with pytest.raises(RuntimeError, match="valid geo_taxonomy GiST index"):
        await address._require_geo_taxonomy_stage_index(
            SimpleNamespace(__tablename__="stage"), "sample", context_by_field
        )
    assert "geo_taxonomy_stage_index_valid" not in context_by_field


@pytest.mark.asyncio
async def test_missing_live_table_does_not_receive_column_changes(monkeypatch):
    database = SimpleNamespace(status=AsyncMock())
    monkeypatch.setattr(address, "db", database)
    monkeypatch.setattr(address, "_has_table", AsyncMock(return_value=False))
    read = AsyncMock()
    monkeypatch.setattr(address.preparation_admission, "read_all", read)
    await address._ensure_entity_address_unified_live_columns("sample")
    read.assert_not_awaited()
    database.status.assert_not_awaited()


def test_empty_source_vector_cannot_construct_unscoped_sql():
    with pytest.raises(ValueError, match="values must be non-empty"):
        address._string_array_literal([])
