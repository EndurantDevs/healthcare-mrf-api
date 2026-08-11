# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path

import pytest
from sqlalchemy import CheckConstraint, Column
from sqlalchemy import ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint

from db.models import provider_directory_rooted_graph as rooted_graph_models
from db.models.provider_directory_rooted_graph import (
    ProviderDirectoryRootedGraphAcquisition,
    ProviderDirectoryRootedGraphWork,
)
from db.models.provider_directory_rooted_graph_publication import (
    ProviderDirectoryRootedGraphDataset,
    ProviderDirectoryRootedGraphDatasetResource,
)
from db.models.provider_directory_rooted_graph_twin import (
    ProviderDirectoryRootedGraphTwinAdmission,
    ProviderDirectoryRootedGraphTwinAttempt,
)
from db.models.provider_directory_rooted_graph_witness import (
    ProviderDirectoryRootedGraphEdge,
    ProviderDirectoryRootedGraphResource,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
)
from process.provider_directory_rooted_graph_publication_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_ROOT_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_store_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_publication import (
    UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / ("20260811020000_provider_directory_rooted_graph_acquisition.py")
)


def _migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_rooted_graph_acquisition_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _migration_table_items() -> dict[str, tuple[object, ...]]:
    migration = _migration()
    items_by_table: dict[str, tuple[object, ...]] = {}

    def capture_table(_op, table_name, *schema_items, schema):
        assert schema == "mrf"
        items_by_table[table_name] = schema_items

    migration.create_table_or_validate = capture_table
    for create_tables in (
        migration._create_acquisition,
        migration._create_work,
        migration._create_resource,
        migration._create_edge,
        migration._create_twin_tables,
        migration._create_publication_tables,
    ):
        create_tables("mrf")
    return items_by_table


def _named_checks(schema_items) -> dict[str, str]:
    return {
        str(item.name): " ".join(str(item.sqltext).split())
        for item in schema_items
        if isinstance(item, CheckConstraint) and item.name is not None
    }


def _column_specs(schema_items) -> dict[str, tuple[object, ...]]:
    return {
        item.name: (
            str(item.type),
            item.nullable,
            (
                None
                if item.server_default is None
                else " ".join(str(item.server_default.arg).split()).lower()
            ),
        )
        for item in schema_items
        if isinstance(item, Column)
    }


def _constraint_columns(constraint) -> tuple[str, ...]:
    bound_columns = tuple(constraint.columns.keys())
    if bound_columns:
        return bound_columns
    return tuple(
        pending if isinstance(pending, str) else pending.name
        for pending in getattr(constraint, "_pending_colargs", ())
    )


def _named_key_specs(schema_items) -> set[tuple[object, ...]]:
    key_types = (PrimaryKeyConstraint, UniqueConstraint, ForeignKeyConstraint)
    return {
        (
            (
                "primary"
                if isinstance(item, PrimaryKeyConstraint)
                else "foreign" if isinstance(item, ForeignKeyConstraint) else "unique"
            ),
            item.name,
            _constraint_columns(item),
            (
                tuple(foreign_key._colspec for foreign_key in item.elements)
                if isinstance(item, ForeignKeyConstraint)
                else ()
            ),
            item.ondelete if isinstance(item, ForeignKeyConstraint) else None,
        )
        for item in schema_items
        if isinstance(item, key_types) and item.name is not None
    }


def _migration_index_specs() -> dict[str, tuple[object, ...]]:
    migration = _migration()
    index_spec_by_name: dict[str, tuple[object, ...]] = {}

    def capture_index(
        _operations,
        index_name,
        table_name,
        index_elements,
        *,
        schema,
        unique=False,
        postgresql_where=None,
    ):
        assert schema == "mrf"
        predicate = None if postgresql_where is None else str(postgresql_where)
        index_spec_by_name[index_name] = (
            table_name,
            tuple(index_elements),
            unique,
            predicate,
        )

    migration.create_index_if_missing = capture_index
    migration._create_indexes("mrf")
    return index_spec_by_name


def _model_index_specs(models) -> dict[str, tuple[object, ...]]:
    return {
        descriptor["name"]: (
            model.__tablename__,
            tuple(descriptor["index_elements"]),
            descriptor.get("unique", False),
            descriptor.get("where"),
        )
        for model in models
        for descriptor in getattr(model, "__my_additional_indexes__", ())
    }


def test_revision_is_the_single_linear_follow_up() -> None:
    migration = _migration()

    assert migration.revision == (
        "20260811020000_provider_directory_rooted_graph_acquisition"
    )
    assert migration.down_revision == (
        "20260811010000_provider_directory_profile_capacity_preflight_receipt"
    )


def test_migration_and_model_pin_the_current_graph_contract() -> None:
    migration = _migration()
    legacy_endpoint = uhc_flex_practitioner_endpoint_identity()

    assert (
        migration._CONNECTOR_ID
        == rooted_graph_models._CONNECTOR_ID
        == (PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID)
    )
    assert (
        migration._GRAPH_CONTRACT_SHA256
        == (rooted_graph_models._GRAPH_CONTRACT_SHA256)
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256
    )
    assert (
        migration._QUERY_CONTRACT_SHA256
        == (rooted_graph_models._QUERY_CONTRACT_SHA256)
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256
    )
    assert migration._ROOTED_SOURCE_ID == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
    assert migration._ROOTED_ENDPOINT_ID == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
    assert (
        migration._ROOTED_ENDPOINT_SIGNATURE
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
    )
    assert migration._SOURCE_AUTHORITY == PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID
    assert migration._LEGACY_SOURCE_ID == UHC_FLEX_PRACTITIONER_SOURCE_ID
    assert migration._LEGACY_ENDPOINT_ID == legacy_endpoint.endpoint_id
    assert (
        migration._LEGACY_ENDPOINT_SIGNATURE == legacy_endpoint.endpoint_signature_hash
    )
    assert (
        migration._PUBLICATION_CONTRACT
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID
    )
    assert (
        migration._LEGACY_PUBLICATION_CONTRACT
        == UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID
    )
    assert (
        migration._PUBLICATION_ROOT_CONTRACT
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_ROOT_CONTRACT_ID
    )
    assert migration._HASH_CONTRACT == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    assert (
        migration._STORAGE_CONTRACT
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID
    )


def test_all_model_checks_exactly_match_migration_adoption_checks() -> None:
    migration_items = _migration_table_items()
    models = (
        ProviderDirectoryRootedGraphAcquisition,
        ProviderDirectoryRootedGraphWork,
        ProviderDirectoryRootedGraphResource,
        ProviderDirectoryRootedGraphEdge,
        ProviderDirectoryRootedGraphTwinAttempt,
        ProviderDirectoryRootedGraphTwinAdmission,
        ProviderDirectoryRootedGraphDataset,
        ProviderDirectoryRootedGraphDatasetResource,
    )

    assert set(migration_items) == {model.__tablename__ for model in models}
    for model in models:
        assert _named_checks(model.__table__.constraints) == _named_checks(
            migration_items[model.__tablename__]
        )


def test_all_model_columns_and_keys_exactly_match_migration_tables() -> None:
    migration_items = _migration_table_items()
    models = (
        ProviderDirectoryRootedGraphAcquisition,
        ProviderDirectoryRootedGraphWork,
        ProviderDirectoryRootedGraphResource,
        ProviderDirectoryRootedGraphEdge,
        ProviderDirectoryRootedGraphTwinAttempt,
        ProviderDirectoryRootedGraphTwinAdmission,
        ProviderDirectoryRootedGraphDataset,
        ProviderDirectoryRootedGraphDatasetResource,
    )

    for model in models:
        expected_items = migration_items[model.__tablename__]
        assert _column_specs(model.__table__.columns) == _column_specs(expected_items)
        assert _named_key_specs(model.__table__.constraints) == _named_key_specs(
            expected_items
        )


def test_all_model_indexes_exactly_match_migration_indexes() -> None:
    models = (
        ProviderDirectoryRootedGraphWork,
        ProviderDirectoryRootedGraphResource,
        ProviderDirectoryRootedGraphEdge,
        ProviderDirectoryRootedGraphDataset,
        ProviderDirectoryRootedGraphDatasetResource,
    )

    assert _model_index_specs(models) == _migration_index_specs()


def test_models_expose_fenced_work_and_immutable_witness_keys() -> None:
    acquisition = ProviderDirectoryRootedGraphAcquisition.__table__
    work = ProviderDirectoryRootedGraphWork.__table__
    resource = ProviderDirectoryRootedGraphResource.__table__
    edge = ProviderDirectoryRootedGraphEdge.__table__

    assert tuple(acquisition.primary_key.columns.keys()) == ("acquisition_id",)
    assert tuple(work.primary_key.columns.keys()) == ("acquisition_id", "query_id")
    assert tuple(resource.primary_key.columns.keys()) == (
        "acquisition_id",
        "query_id",
        "attempt",
        "resource_type",
        "resource_id",
    )
    assert tuple(edge.primary_key.columns.keys()) == (
        "acquisition_id",
        "query_id",
        "attempt",
        "edge_sha256",
    )
    assert {
        "root_dataset_id",
        "root_cohort_id",
        "endpoint_signature_sha256",
        "graph_contract_sha256",
        "query_contract_sha256",
        "rooted_graph_complete",
        "endpoint_collection_complete",
        "endpoint_complete",
        "rooted_graph_sha256",
    }.issubset(acquisition.c.keys())
    assert {
        "attempt_count",
        "lease_token",
        "discovered_by_query_id",
        "discovered_edge_sha256",
        "advertised_total",
        "terminal_page_count",
        "pagination_terminal",
        "missing_http_status",
    }.issubset(work.c.keys())


@pytest.mark.parametrize(
    "module_name",
    (
        "provider_directory_rooted_graph",
        "provider_directory_rooted_graph_witness",
        "provider_directory_rooted_graph_twin",
        "provider_directory_rooted_graph_publication",
    ),
)
def test_models_reject_conflicting_runtime_schemas(monkeypatch, module_name) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    model_path = Path(__file__).resolve().parents[1] / "db/models" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"{module_name}_schema_conflict",
        model_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)

    with pytest.raises(RuntimeError, match="DB_SCHEMA and HLTHPRT_DB_SCHEMA"):
        spec.loader.exec_module(module)


def test_guards_bind_root_census_discovery_and_fixed_point() -> None:
    migration = _migration()
    schema = "rooted_graph_test_static"
    acquisition_sql = " ".join(migration._acquisition_guard_sql(schema).split())
    work_sql = " ".join(migration._work_guard_sql(schema).split())
    resource_sql = " ".join(migration._resource_guard_sql(schema).split())

    for fragment in (
        "parent_dataset.status IS DISTINCT FROM 'published'",
        "member.resource_type = 'Practitioner'",
        "terminal_set_sha256",
        "root_census_incomplete",
        "plan_intersection_invalid",
        "fixed_point_incomplete",
        "actual_error <> 0",
        "root_network_edge.field_path LIKE 'network[%'",
        "root_network_edge.field_path LIKE 'extension[%.valueReference'",
    ):
        assert fragment in acquisition_sql
    for fragment in (
        "OLD.lease_expires_at <= clock_timestamp()",
        "NEW.attempt_count = OLD.attempt_count + 1",
        "proof.edge_sha256 = NEW.discovered_edge_sha256",
        "proof.resource_type = 'Organization'",
        "parent.terminal_at IS NOT DISTINCT FROM transaction_timestamp()",
        "root_fixed_point_incomplete",
        "root_closure_frozen",
        "action IN ('claim', 'claim_census')",
        "OLD.kind = 'full_insurance_plan_census'",
        "NEW.missing_http_status NOT IN (404, 410)",
        "'missing'",
        "provider_directory_rooted_graph_lease_lost",
    ):
        assert fragment in work_sql
    assert "payload -> 'practitioner' ->> 'reference'" in resource_sql
    assert "payload -> 'participatingOrganization' ->> 'reference'" in resource_sql


def test_rooted_validity_traces_one_current_official_terminal_lineage() -> None:
    migration = _migration()
    schema = "rooted_graph_lineage_static"
    intrinsic_sql = " ".join(
        migration._rooted_intrinsic_valid_function_sql(schema).split()
    )
    lineage_sql = " ".join(
        migration._rooted_official_lineage_current_function_sql(schema).split()
    )
    valid_sql = " ".join(migration._rooted_valid_function_sql(schema).split())
    header_guard_sql = " ".join(migration._rooted_header_guard_sql(schema).split())
    current_guard_sql = " ".join(migration._logical_current_guard_sql(schema).split())

    assert migration._ROOTED_INTRINSIC_VALID in intrinsic_sql
    for fragment in (
        "WITH RECURSIVE lineage",
        "visited_dataset_ids",
        "cycle_detected",
        "lineage.depth < 1024",
        migration._ROOTED_INTRINSIC_VALID,
        migration._LEGACY_VALID,
        "legacy.dataset_hash = child.root_dataset_hash",
        "legacy.cohort_id = child.root_cohort_id",
        "official.status = 'published'",
        "official.is_current IS TRUE",
        "(SELECT count(*) FROM terminal) = 1",
    ):
        assert fragment in lineage_sql
    assert migration._ROOTED_INTRINSIC_VALID in valid_sql
    assert migration._ROOTED_OFFICIAL_LINEAGE_CURRENT in valid_sql
    assert "NEW.status IN ('validated', 'published')" in header_guard_sql
    assert migration._ROOTED_INTRINSIC_VALID in current_guard_sql
    assert migration._ROOTED_READY not in current_guard_sql
    upgrade_source = inspect.getsource(migration.upgrade)
    assert (
        upgrade_source.index("_rooted_intrinsic_valid_function_sql")
        < (upgrade_source.index("_rooted_official_lineage_current_function_sql"))
        < upgrade_source.index("_rooted_valid_function_sql")
        < upgrade_source.index("_rooted_ready_function_sql")
    )


def test_witnesses_are_insert_only_and_downgrade_is_nonempty_fenced() -> None:
    migration = _migration()
    resource_guard = migration._resource_guard_sql("rooted_graph_test_static")
    edge_guard = migration._edge_guard_sql("rooted_graph_test_static")
    source = MIGRATION_PATH.read_text()

    assert "TG_OP <> 'INSERT'" in resource_guard
    assert "payload_sha256" in resource_guard
    assert "TG_OP <> 'INSERT'" in edge_guard
    assert "actual_reference IS DISTINCT FROM" in edge_guard
    assert "extension_cursor" in edge_guard
    assert "valueReference" in edge_guard
    assert all(
        extension_url in edge_guard
        for extension_url in migration._PLAN_NET_NETWORK_EXTENSION_URLS
    )
    edge_constraint_by_name = {
        constraint.name: str(constraint.sqltext)
        for constraint in ProviderDirectoryRootedGraphEdge.__table__.constraints
        if constraint.name and hasattr(constraint, "sqltext")
    }
    assert (
        "extension\\[[0-9]+\\]"
        in edge_constraint_by_name["provider_directory_rooted_graph_edge_value_check"]
    )
    assert "provider_directory_rooted_graph_downgrade_blocked" in source
    assert "create_table_or_validate" in source
    assert "create_index_if_missing" in source
