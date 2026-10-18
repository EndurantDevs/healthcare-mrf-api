# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Final control grants cannot replace native custody or broaden writer rights."""

import ast
import asyncio
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_control_privileges as privileges

ROOT = Path(__file__).resolve().parents[1]


def _control_grants(statements):
    """Decode the actual emitted SQL, rejecting unreviewed SQL shapes."""
    rights_by_relation = {}
    for statement in statements:
        if statement == 'GRANT SET ON PARAMETER temp_file_limit TO "control_owner"':
            continue
        match = re.fullmatch(r'GRANT (.+) ON "control_schema"\."([a-z_][a-z_0-9]*)" TO "control_owner"', statement)
        assert match is not None, statement
        grant_text, relation = match.groups()
        for grant in re.finditer(r"(SELECT|INSERT|UPDATE|DELETE|MAINTAIN)(?:\(([^)]+)\))?(?:,|$)", grant_text):
            operation, columns = grant.groups()
            coordinate = (relation, operation)
            assert coordinate not in rights_by_relation
            rights_by_relation[coordinate] = None if columns is None else frozenset(columns.replace('"', "").split(","))
        assert (
            ",".join(
                match.group(0).rstrip(",")
                for match in re.finditer(r"(SELECT|INSERT|UPDATE|DELETE|MAINTAIN)(?:\(([^)]+)\))?(?:,|$)", grant_text)
            )
            == grant_text
        )
    return rights_by_relation


@pytest.fixture
async def grant_statements():
    connection = SimpleNamespace(execute=AsyncMock())
    policy = SimpleNamespace(schema="control_schema", owner_role="control_owner")
    await privileges.grant_cms_control_privileges(connection, policy)
    return [call.args[0] for call in connection.execute.await_args_list]


def test_fragment_only_grants_fixed_controls_to_configured_owner(grant_statements):
    rights = _control_grants(grant_statements)
    assert {relation for relation, _operation in rights} == {
        "provider_directory_source",
        "provider_directory_api_endpoint",
        "import_run",
        "provider_directory_profile_serving_generation",
        "provider_directory_profile_delta_receipt",
        "provider_directory_profile_initial_receipt",
        "provider_directory_profile_build_checkpoint",
        "provider_directory_profile_capacity_preflight_receipt",
        "provider_directory_profile_capacity_lease_consumption",
        "reference_family_result_generation",
        "entity_address_result_generation",
        "entity_address_geo_assurance_state",
        "address_alias_state_v1",
        "address_alias_artifact_state_v1",
        "ptg_import_wave",
        "ptg_import_wave_admission_rollback",
        "ptg_import_wave_claim",
        "ptg_import_wave_intent",
        "ptg_import_wave_ordinary_terminal_receipt",
        "ptg_import_wave_outcome",
        "ptg_import_wave_quarantine",
        "ptg_import_wave_supersession",
        "ptg_source_attempt_event",
    }
    assert grant_statements[-1] == 'GRANT SET ON PARAMETER temp_file_limit TO "control_owner"'
    assert all(statement.startswith("GRANT ") for statement in grant_statements)
    assert all(
        not any(
            word in statement
            for word in (
                "EXECUTE",
                "ALTER",
                "CREATE",
                "TRUNCATE",
                "ALL ",
                "GRANT OPTION",
                "RESET",
                "COMMIT",
                "ROLLBACK",
            )
        )
        for statement in grant_statements
    )


def test_configuration_and_progress_grants_remain_column_scoped(grant_statements):
    rights = _control_grants(grant_statements)
    assert rights["provider_directory_source", "UPDATE"] == {"endpoint_id", "updated_at"}
    assert rights["import_run", "SELECT"] == {
        "run_id",
        "engine",
        "node_id",
        "importer",
        "family",
        "status",
        "phase_detail",
        "params",
        "idempotency_key",
        "triggered_by",
        "schedule_id",
        "subscription_id",
        "source_file_import_id",
        "created_at",
        "started_at",
        "finished_at",
        "progress",
        "metrics",
        "error",
        "snapshot_id",
        "import_id",
        "retry_of_run_id",
    }
    assert rights["import_run", "UPDATE"] == {
        "status",
        "phase_detail",
        "heartbeat_at",
        "progress",
        "error",
        "metrics",
        "started_at",
        "finished_at",
    }
    assert ("provider_directory_api_endpoint", "UPDATE") not in rights
    # The separately verified receipt Owner already has its native table rights.
    assert all(root != "provider_directory_cms_serving_receipt" for root, _operation in rights)
    assert rights["address_alias_state_v1", "UPDATE"] == {"singleton"}
    assert rights["address_alias_artifact_state_v1", "INSERT"] == {"artifact_name", "generation", "updated_at"}
    assert rights["address_alias_artifact_state_v1", "UPDATE"] == {"generation", "updated_at"}


def test_progress_guard_reads_match_actual_migration_functions(grant_statements):
    """Grant every invoker read without adding PTG writes or full-table reads."""
    from tests.test_provider_directory_import_run_guards import _migration_statements

    functions, _triggers = _migration_statements("control_schema")
    guard_names = {
        "ptg_import_wave_abandonment_run_guard",
        "ptg_import_wave_materialized_run_guard",
        "ptg_import_wave_materialized_write_isolation_guard",
        "ptg_import_wave_v12_abandoned_run_guard",
        "ptg_import_wave_v13_abandoned_run_guard",
    }
    expected_columns_by_relation = {}
    for name in guard_names:
        sql = functions[name]
        assert "SECURITY DEFINER" not in sql
        for relation, alias in re.findall(
            r'(?:FROM|JOIN) "control_schema"\."([a-z_][a-z_0-9]*)" AS ([a-z_][a-z_0-9]*)', sql
        ):
            columns = set(re.findall(r"\b" + alias + r"\.([a-z_][a-z_0-9]*)", sql))
            expected_columns_by_relation.setdefault(relation, set()).update(columns)
    receipt_sql = functions["ptg_wave_ordinary_terminal_run_immutable_guard"]
    assert "SECURITY DEFINER" not in receipt_sql
    assert 'FROM "control_schema"."ptg_import_wave_ordinary_terminal_receipt"' in receipt_sql
    expected_columns_by_relation["ptg_import_wave_ordinary_terminal_receipt"] = set(
        re.findall(r"\bWHERE ([a-z_][a-z_0-9]*) = candidate_run_id", receipt_sql)
    )
    assert expected_columns_by_relation.pop("import_run") <= set(privileges.IMPORT_RUN_SELECT)
    rights = _control_grants(grant_statements)
    granted_columns_by_relation = {
        root: columns for (root, operation), columns in rights.items() if root.startswith("ptg_")
    }
    assert granted_columns_by_relation == expected_columns_by_relation
    assert len(granted_columns_by_relation) == 9 and sum(map(len, granted_columns_by_relation.values())) == 72
    assert all(operation == "SELECT" for root, operation in rights if root in granted_columns_by_relation)
    assert all(columns is not None for columns in granted_columns_by_relation.values())


def test_other_progress_guards_need_no_additional_owner_grants():
    """Keep short-circuited and definer-owned dependencies outside the grant set."""
    from tests.test_provider_directory_import_run_guards import _migration_statements

    functions, _triggers = _migration_statements("control_schema")
    retired = functions["ptg_import_wave_retired_run_guard"]
    assert retired.index("IF NEW.importer IS DISTINCT FROM 'ptg'") < retired.index("FROM ")
    for name in (
        "guard_npi_canonical_publication_run",
        "guard_provider_directory_terminal_root_retirement_run",
        "provider_directory_terminal_root_run_retired",
    ):
        assert "SECURITY DEFINER" in functions[name]
        assert re.search(r"SET search_path\s*=\s*pg_catalog", functions[name])


def test_profile_final_rights_preserve_preparation_and_receipt_immutability(grant_statements):
    rights = _control_grants(grant_statements)
    assert rights["provider_directory_profile_delta_receipt", "UPDATE"] == {"build_id"}
    assert {relation for relation, operation in rights if operation == "DELETE"} == {
        "provider_directory_profile_build_checkpoint"
    }
    assert ("provider_directory_profile_build_checkpoint", "INSERT") not in rights
    assert rights["provider_directory_profile_build_checkpoint", "UPDATE"] == {
        "cutover_forecast_status",
        "cutover_forecast_hash",
        "cutover_forecast_json",
        "updated_at",
    }
    for relation in (
        "provider_directory_profile_capacity_preflight_receipt",
        "provider_directory_profile_capacity_lease_consumption",
    ):
        assert {operation for root, operation in rights if root == relation} == {"SELECT", "MAINTAIN"}
    assert rights["provider_directory_profile_serving_generation", "UPDATE"] == set(
        privileges.PROFILE_GENERATION_UPDATE
    )
    assert len(privileges.PROFILE_GENERATION_UPDATE) == 22
    assert set(privileges.PROFILE_GENERATION_INSERT) - set(privileges.PROFILE_GENERATION_UPDATE) == {
        "singleton_key",
        "evidence_target_oid",
        "profile_target_oid",
    }
    assert all(
        columns is not None for (_root, operation), columns in rights.items() if operation in {"INSERT", "UPDATE"}
    )


def test_brief_table_fences_do_not_require_table_wide_update(grant_statements):
    rights = _control_grants(grant_statements)
    assert {relation for relation, operation in rights if operation == "MAINTAIN"} == {
        "provider_directory_source",
        "provider_directory_api_endpoint",
        "import_run",
        "provider_directory_profile_build_checkpoint",
        "provider_directory_profile_capacity_lease_consumption",
        "provider_directory_profile_serving_generation",
        "provider_directory_profile_capacity_preflight_receipt",
        "provider_directory_profile_initial_receipt",
        "reference_family_result_generation",
    }
    assert ("reference_family_result_generation", "UPDATE") not in rights
    assert ("reference_family_result_generation", "INSERT") not in rights


def _source_function(relative, name):
    """Read one real caller without importing the importer or invoking its runtime."""
    source = (ROOT / relative).read_text()
    tree = ast.parse(source)
    node = next(
        node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    )
    return ast.get_source_segment(source, node)


def _sql_constant(name):
    tree = ast.parse((ROOT / "process/provider_directory_fhir.py").read_text())
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
    )
    return ast.literal_eval(assignment.value)


def test_granted_profile_delta_columns_match_actual_cas_and_receipt_sql():
    update_sql = _sql_constant("_PROFILE_DELTA_SERVING_UPDATE_SQL")
    assignment_sql = update_sql.split("SET", 1)[1].split("WHERE", 1)[0]
    assert set(re.findall(r"\b([a-z_]+)\s*=", assignment_sql)) == set(privileges.PROFILE_GENERATION_UPDATE)
    insert_sql = _sql_constant("_PROFILE_DELTA_RECEIPT_INSERT_SQL")
    columns = re.search(r"INSERT INTO \{receipt_ref\}\s*\((.*?)\)\s*VALUES", insert_sql, re.S).group(1)
    assert tuple(column.strip() for column in columns.split(",")) == privileges.PROFILE_DELTA_INSERT


def test_final_checkpoint_forecast_grants_match_actual_cas(grant_statements):
    rights = _control_grants(grant_statements)
    forecast_source = _source_function(
        "process/provider_directory_fhir.py", "_persist_provider_directory_profile_cutover_forecast"
    )
    assignments = forecast_source.split("SET", 1)[1].split("WHERE", 1)[0]
    columns = set(re.findall(r"\b([a-z_]+)\s*=", assignments))
    assert columns == {"cutover_forecast_status", "cutover_forecast_hash", "cutover_forecast_json", "updated_at"}
    assert rights["provider_directory_profile_build_checkpoint", "UPDATE"] == columns
    assert "FOR UPDATE" in forecast_source
    assert "FOR UPDATE" in _source_function("process/provider_directory_fhir.py", "_lock_profile_delta_checkpoint")


def test_granted_profile_initial_columns_match_actual_initial_cut():
    initial_values = ast.parse(_source_function("process/provider_directory_profile_initial.py", "_serving_values"))
    return_node = next(node for node in ast.walk(initial_values) if isinstance(node, ast.Return))
    value_columns = {key.value for key in return_node.value.keys}
    finish_source = _source_function("process/provider_directory_profile_initial.py", "finish_cutover")
    finish_tree = ast.parse(finish_source)
    values_assignment = next(
        node
        for node in ast.walk(finish_tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "serving_by_column" for target in node.targets)
    )
    extra_columns = {key.value for key in values_assignment.value.keys if key is not None}
    assert set(privileges.PROFILE_GENERATION_INSERT) == value_columns | extra_columns | {
        "singleton_key",
        "capacity_geometry_status",
        "published_at",
        "updated_at",
    }
    receipt_columns = re.search(r"\(build_id,attestation_id,(.*?)\)\s*SELECT", finish_source, re.S).group(1)
    assert (
        "build_id",
        "attestation_id",
        *(column.strip() for column in receipt_columns.split(",")),
    ) == privileges.PROFILE_INITIAL_INSERT


def test_granted_address_columns_match_actual_native_finalizers(grant_statements):
    rights = _control_grants(grant_statements)
    generation_source = _source_function(
        "process/entity_address_result_generation.py", "publish_local_entity_address_generation"
    )
    updated_columns = set(re.findall(r'"([a-z_]+)=', generation_source))
    assert updated_columns == rights["entity_address_result_generation", "UPDATE"]
    assert rights["entity_address_result_generation", "SELECT"] == {
        "singleton",
        "local_lineage_id",
        "local_generation",
        "origin_lineage_id",
        "origin_generation",
        "published_at",
        "relation_oids",
    }
    geo_source = _source_function(
        "process/entity_address_dependency_bindings.py", "activate_geo_assurance_candidate_sql"
    )
    assignments = geo_source.split("SET", 1)[1].split("WHERE", 1)[0]
    assert set(re.findall(r"\b([a-z_]+)\s*=", assignments)) == rights["entity_address_geo_assurance_state", "UPDATE"]
    assert rights["entity_address_geo_assurance_state", "SELECT"] == {"singleton", *privileges.GEO_STATE_UPDATE}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", "invalid.schema"),
        ("owner_role", "invalid;role"),
        ("schema", None),
        ("owner_role", "x" * 64),
    ],
)
async def test_invalid_identifier_refuses_before_any_grant(field, value):
    policy_by_field = {"schema": "control_schema", "owner_role": "control_owner", field: value}
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="storage_identifier_invalid"):
        await privileges.grant_cms_control_privileges(connection, SimpleNamespace(**policy_by_field))
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [RuntimeError("rejected"), asyncio.CancelledError()])
async def test_error_or_cancellation_propagates_without_retry_or_transaction_takeover(interruption):
    connection = SimpleNamespace(execute=AsyncMock(side_effect=[None, None, interruption]))
    policy = SimpleNamespace(schema="control_schema", owner_role="control_owner")
    with pytest.raises(type(interruption)) as caught:
        await privileges.grant_cms_control_privileges(connection, policy)
    assert caught.value is interruption
    assert connection.execute.await_count == 3


@pytest.mark.asyncio
async def test_read_plan_limits_columns_and_grantees():
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    policy = GenericSourcePolicy(
        "control_schema",
        "control_owner",
        "control_publisher",
        ("control_builder", "control_reader"),
        ("control_builder",),
    )
    connection = SimpleNamespace(execute=AsyncMock())
    await privileges.grant_cms_control_read_privileges(connection, policy)
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert len(statements) == 12
    expected = _expected_read_columns()
    assert privileges.READ_COLUMN_GRANTS == expected
    assert set(statements) == {
        "GRANT SELECT("
        + ",".join('"' + name + '"' for name in columns)
        + ') ON "control_schema"."'
        + root
        + '" TO "'
        + role
        + '"'
        for root, columns in expected.items()
        for role in ("control_publisher", "control_reader")
    }
    assert privileges.cms_control_read_roles(policy) == ("control_publisher", "control_reader")


@pytest.mark.asyncio
async def test_read_fragment_refuses_unadmitted_policy_before_sql():
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="cms_control_read_policy_required"):
        await privileges.grant_cms_control_read_privileges(
            connection,
            SimpleNamespace(
                schema="control_schema",
                publisher_role="control_publisher",
                runtime_roles=("foreign",),
                control_runtime_roles=(),
            ),
        )
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("change", ["missing", "duplicate", "foreign"])
def test_read_role_resolution_requires_exact_declared_native_identities(change):
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    policy = GenericSourcePolicy(
        "control_schema",
        "control_owner",
        "control_publisher",
        ("control_builder", "control_reader"),
        ("control_builder",),
    )
    graph_by_field = {
        "roles": [{"rolname": "control_publisher", "oid": "18"}, {"rolname": "control_reader", "oid": "20"}]
    }
    assert privileges.cms_control_read_oids(policy, graph_by_field) == (18, 20)
    if change == "missing":
        graph_by_field["roles"].pop()
    elif change == "duplicate":
        graph_by_field["roles"].append(graph_by_field["roles"][0])
    else:
        graph_by_field["roles"][-1]["rolname"] = "archive_reader"
    with pytest.raises(ValueError, match="cms_control_read_roles_changed"):
        privileges.cms_control_read_oids(policy, graph_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [RuntimeError("read rejected"), asyncio.CancelledError()])
async def test_read_grant_failure_propagates_to_original_admin_rollback(interruption):
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    policy = GenericSourcePolicy("control_schema", "control_owner", "control_publisher", ("control_reader",))
    connection = SimpleNamespace(execute=AsyncMock(side_effect=[None, interruption]))
    with pytest.raises(type(interruption)) as caught:
        await privileges.grant_cms_control_read_privileges(connection, policy)
    assert caught.value is interruption
    assert connection.execute.await_count == 2


def _expected_read_columns():
    return {
        "provider_directory_source": ("source_id", "endpoint_id"),
        "provider_directory_cms_serving_receipt": (
            "receipt_id",
            "payload",
            "publication_xid",
            "profile_generation_id",
            "registry_source_pair_json",
            "address_lineage_id",
            "address_generation",
        ),
        "entity_address_result_generation": privileges.ADDRESS_GENERATION_SELECT,
        "provider_directory_profile_serving_generation": (
            "singleton_key",
            "status",
            "operation",
            "control_generation",
            "generation_id",
            "selection_proof_id",
            "authority_revision",
            "profile_schema_version",
            "profile_strategy_version",
            "source_vector_hash",
            "source_context_vector_hash",
            "executable_plan_hash",
            "profile_as_of",
            "evidence_target_oid",
            "profile_target_oid",
            "evidence_rows",
            "profile_rows",
        ),
        "reference_family_result_generation": (
            "importer_id",
            "local_lineage_id",
            "local_generation",
            "origin_lineage_id",
            "origin_generation",
            "published_at",
            "relation_oids",
        ),
        "address_alias_state_v1": ("singleton", "generation"),
    }


def _compiled_current_read_closure():
    from process import provider_directory_cms_callable_custody as custody
    from tests.test_provider_directory_cms_callable_custody import _migration_functions

    functions = _migration_functions()
    body_by_name = {}
    pending_names = {"cms_serving_current_receipt_matches"}
    while pending_names:
        name = pending_names.pop()
        body = functions[name][-1]
        assert custody._body_digest(body, "snapshot_schema") == custody._CONTRACTS[name][-1]
        body_by_name[name] = body
        calls = set(re.findall(r'"snapshot_schema"\.(cms_serving_[a-z_0-9]+)\(', body))
        pending_names.update(calls - body_by_name.keys())
    assert set(body_by_name) == {
        "cms_serving_current_receipt_matches",
        "cms_serving_native_matches",
        "cms_serving_native_snapshot",
        "cms_serving_oids",
    }
    return body_by_name


def test_migration_built_read_closure_covers_lineage_metadata():
    body_by_name = _compiled_current_read_closure()
    existing_columns_by_root = {
        "provider_directory_endpoint_dataset": {
            "dataset_id",
            "endpoint_id",
            "dataset_hash",
            "acquisition_root_run_id",
            "is_current",
            "status",
            "published_at",
        },
        "provider_directory_cms_serving_coverage": {
            "dataset_id",
            "dataset_hash",
            "proof_version",
            "release_id",
            "published_at",
        },
    }
    roots = {
        name
        for body in body_by_name.values()
        for name in re.findall(r'(?:FROM|JOIN)\s+"snapshot_schema"\.([a-z_][a-z_0-9]*)', body)
    }
    assert (
        roots | {"provider_directory_cms_serving_receipt"}
        == set(privileges.READ_COLUMN_GRANTS) | existing_columns_by_root.keys()
    )
    assert len(roots) == 7
    current = body_by_name["cms_serving_current_receipt_matches"]
    for alias, root in (
        ("s", "provider_directory_source"),
        ("d", "provider_directory_endpoint_dataset"),
        ("c", "provider_directory_cms_serving_coverage"),
    ):
        observed_columns = set(re.findall(r"\b" + alias + r"\.([a-z_][a-z_0-9]*)", current))
        assert observed_columns == set(privileges.READ_COLUMN_GRANTS.get(root, existing_columns_by_root.get(root, ())))
    assert (
        sum(map(len, privileges.READ_COLUMN_GRANTS.values())) + sum(map(len, existing_columns_by_root.values())) == 54
    )
    assert not existing_columns_by_root.keys() & privileges.READ_COLUMN_GRANTS.keys()
    _require_snapshot_projection(body_by_name["cms_serving_native_snapshot"])


def _require_snapshot_projection(body):
    for alias, root, predicates in (
        ("p", "provider_directory_profile_serving_generation", {"singleton_key"}),
        ("a", "entity_address_result_generation", {"singleton"}),
        ("d", "reference_family_result_generation", set()),
    ):
        observed_columns = set(re.findall(r"\b" + alias + r"\.([a-z_][a-z_0-9]*)", body)) | predicates
        assert observed_columns == set(privileges.READ_COLUMN_GRANTS[root])
    assert 'SELECT generation FROM "snapshot_schema".address_alias_state_v1 WHERE singleton' in body


def _import_run_toastable_columns():
    from sqlalchemy import JSON, String

    from db.models.system import ImportRun

    return tuple(column.name for column in ImportRun.__table__.columns if isinstance(column.type, (String, JSON)))


def test_run_toast_grants(grant_statements):
    rights = _control_grants(grant_statements)
    assert rights["import_run", "SELECT"] == {
        *_import_run_toastable_columns(),
        "created_at",
        "started_at",
        "finished_at",
    }
    assert "heartbeat_at" not in rights["import_run", "SELECT"]
    assert {operation for root, operation in rights if root == "import_run"} == {"SELECT", "UPDATE", "MAINTAIN"}


async def test_progress_update_grants_match_the_actual_running_lifecycle(monkeypatch, grant_statements):
    from process import control_lifecycle

    update = AsyncMock(return_value=1)
    monkeypatch.setattr(control_lifecycle, "_should_update_control_run_db", AsyncMock(return_value=True))
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", update)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda **_values: True)
    assert await control_lifecycle.mark_control_run(
        "run_" + "1" * 32,
        status="running",
        phase_detail="profile",
        progress_message="building profile",
        metrics={"rows": 2},
        progress={"done": 1, "total": 2},
    )
    statement = update.await_args.args[0]
    updated_columns = {getattr(column, "key", column) for column in statement._values}
    rights = _control_grants(grant_statements)
    assert updated_columns == rights["import_run", "UPDATE"] == set(privileges.IMPORT_RUN_PROGRESS_UPDATE)
    assert tuple(column.name for column in statement._returning) == ("run_id",)
    assert rights["import_run", "SELECT"] is not None and rights["import_run", "UPDATE"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_chunks", [1, 2])
async def test_admission_toast_census(monkeypatch, observed_chunks):
    import importlib

    importer = importlib.import_module("process.provider_directory_fhir")
    toastable_columns = _import_run_toastable_columns()
    fingerprint = AsyncMock(
        return_value=SimpleNamespace(relation_oid=41, toast_oid=42, toastable_columns=toastable_columns)
    )
    monkeypatch.setattr(importer, "_provider_directory_profile_relation_storage_fingerprint", fingerprint)
    query = AsyncMock(return_value={"toast_chunk_count": observed_chunks, "compression_methods": ["pglz"]})
    monkeypatch.setattr(importer.db, "first", query)
    run_id = "run_" + "1" * 32
    arguments = (
        run_id,
        SimpleNamespace(import_run_oid=41),
        SimpleNamespace(postgres_default_toast_compression="pglz"),
        SimpleNamespace(import_run_update=SimpleNamespace(deleted_toast_chunks=1)),
    )
    if observed_chunks > 1:
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="import_run_toast_changed"):
            await importer._assert_admission_run_toast(*arguments)
    else:
        await importer._assert_admission_run_toast(*arguments)
    fingerprint.assert_awaited_once_with(41, expected_persistence="p")
    query.assert_awaited_once()
    sql = query.await_args.args[0]
    expected_projection = ", ".join(importer._q(name) for name in toastable_columns)
    assert f"SELECT {expected_projection} FROM " in sql
    assert "SELECT *" not in sql
    assert "WHERE run_id = :run_id" in sql
    assert query.await_args.kwargs == {"run_id": run_id}
    for name in toastable_columns:
        quoted_name = importer._q(name)
        assert f"pg_column_toast_chunk_id(exact_rows.{quoted_name})" in sql
        assert f"pg_column_compression(exact_rows.{quoted_name})" in sql
    assert "heartbeat_at" not in sql
