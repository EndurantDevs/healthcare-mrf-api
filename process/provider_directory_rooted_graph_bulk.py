# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native landing for one rooted-graph query_result transaction."""

from __future__ import annotations

from itertools import islice
from typing import Any

from process.provider_directory_dataset_candidate import _bounded_copy_batches
from process.provider_directory_projection_stage import _copy_driver
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
)
from process.provider_directory_rooted_graph_store_support import rooted_graph_transaction, row_fields

_RESOURCE_COLUMNS = (
    "acquisition_id",
    "scope_id",
    "query_id",
    "attempt",
    "resource_type",
    "resource_id",
    "payload_sha256",
    "payload_json_text",
    "closure_scope",
)
_EDGE_COLUMNS = (
    "acquisition_id",
    "scope_id",
    "query_id",
    "attempt",
    "source_resource_type",
    "source_resource_id",
    "field_path",
    "target_resource_type",
    "target_resource_id",
    "edge_sha256",
    "closure_scope",
)
_COPY_ROWS = 1024


async def _copy_stage(driver, name, columns, records):
    iterator = iter(records)
    while batch_records := tuple(islice(iterator, _COPY_ROWS)):
        for bounded_records in _bounded_copy_batches(batch_records):
            status = await driver.copy_records_to_table(
                name,
                schema_name="pg_temp",
                columns=columns,
                records=bounded_records,
            )
            if status != f"COPY {len(bounded_records)}":
                raise ProviderDirectoryRootedGraphStoreError("state")


async def admit_result_witnesses(database: Any, transaction: Any, claim: Any, query_result: Any) -> None:
    """Land and validate one complete logical result on its isolated generation."""
    if _named_runtime(database) is None:
        raise RuntimeError(
            "Rooted writes require a configured isolated generation prepared through the Source operator"
        )
    await _named_result_admission(database, transaction, claim, query_result)


_WORK_COLUMNS = (
    "acquisition_id",
    "scope_id",
    "query_id",
    "query_identity_sha256",
    "query_identity_json_text",
    "kind",
    "resource_type",
    "search_parameter",
    "reference_type",
    "reference_id",
    "closure_scope",
    "discovered_by_query_id",
    "discovered_source_type",
    "discovered_source_id",
    "discovered_edge_sha256",
    "status",
    "attempt_count",
    "pagination_terminal",
)


async def admit_work_specs(database, acquisition_id, work_specs, *, action):
    """Copy and validate a complete derived frontier within the current transaction."""
    from process.provider_directory_rooted_graph_store_support import WORK_TABLE

    async with rooted_graph_transaction(database) as transaction:
        driver = await _copy_driver(transaction)
        await _prepare_named_stage(database, "pdrg_work_stage", WORK_TABLE)
        await _copy_stage(
            driver,
            "pdrg_work_stage",
            _WORK_COLUMNS,
            (
                (acquisition_id,)
                + tuple(getattr(spec, column) for column in _WORK_COLUMNS[1:-3])
                + ("pending", 0, False)
                for spec in work_specs
            ),
        )
        return await _named_work_admission(database, acquisition_id, action)


async def copy_initial_root_work(database, acquisition_id):
    """Land bounded named-source pages before complete set validation and admission."""
    if _named_runtime(database) is None:
        raise RuntimeError(
            "Rooted writes require a configured isolated generation prepared through the Source operator"
        )
    return await _named_initial_work(database, acquisition_id)


def _initial_work_sql(schema, resource_relation):
    """Reuse the canonical query identity and paging SQL against the selected named dataset."""
    from pathlib import Path

    from db.plan_release_storage import storage_identifier

    sql = (Path(__file__).resolve().parents[1] / "db" / "sql" / "rooted_graph_set_validation.sql").read_text()
    body = sql.split("CREATE FUNCTION __SCHEMA__.read_provider_directory_rooted_graph_initial_work(", 1)[1]
    body = body.split("AS $function$", 1)[1].split("$function$;", 1)[0]
    query = (
        body.replace("__SCHEMA__.provider_directory_dataset_resource", resource_relation)
        .replace("__SCHEMA__", storage_identifier(schema))
        .replace("p_acquisition", ":acquisition_id")
        .replace("p_after", ":after_id")
        .replace("p_limit", ":limit")
    )
    return (
        "WITH initial_work ("
        + ",".join(_WORK_COLUMNS)
        + ") AS ("
        + query.strip().removesuffix(";")
        + ") SELECT * FROM initial_work ORDER BY reference_id"
    )


def _named_runtime(database):
    from process.provider_directory_rooted_graph_source_runtime import current_rooted_graph_runtime

    return current_rooted_graph_runtime(database)


def _candidate_sql(filename, schema):
    from pathlib import Path

    from db.plan_release_storage import storage_identifier

    return (
        (Path(__file__).resolve().parents[1] / "db" / "sql" / filename)
        .read_text()
        .replace("__SCHEMA__", storage_identifier(schema))
    )


async def _prepare_named_stage(database, name, root):
    from process.provider_directory_rooted_graph_store_support import table_ref

    await database.status(
        f'CREATE TEMP TABLE "{name}" (LIKE {table_ref(root)} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP'
    )


async def _named_work_admission(database, acquisition_id, action):
    import json

    from db.plan_release_storage import storage_relation
    from process.provider_directory_rooted_graph_store_support import ACQUISITION_TABLE, WORK_TABLE, table_ref
    from process.provider_directory_source_storage_binding import dataset_source_relations

    runtime = _named_runtime(database)
    header = row_fields(
        await database.first(
            f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} WHERE acquisition_id=:acquisition_id AND status='building' FOR UPDATE",
            acquisition_id=acquisition_id,
        )
    )
    if not header or action not in {"initialize", "derive", "census"}:
        raise ProviderDirectoryRootedGraphStoreError("state")
    stage = 'pg_temp."pdrg_work_stage"'
    await database.status(f"ALTER TABLE {stage} ADD PRIMARY KEY(acquisition_id,query_id)")
    total = await database.scalar(f"SELECT count(*) FROM {stage}")
    if (action == "initialize" and total != header["root_resource_count"]) or (action == "census" and total != 1):
        raise ProviderDirectoryRootedGraphStoreError("state")
    root = "provider_directory_dataset_resource"
    root_relations = await dataset_source_relations(
        database, runtime.catalog_schema, header["root_dataset_id"], (root,)
    )
    relation = storage_relation(root_relations[root]["schema"], root_relations[root]["name"])
    query = _candidate_sql("rooted_graph_work_candidate_validation.sql", runtime.candidate_schema)
    invalid = await database.scalar(
        query.replace("__ROOT_RESOURCE__", relation), acquisition_id=acquisition_id, action=action
    )
    invalid = json.loads(invalid) if isinstance(invalid, str) else invalid
    if invalid != []:
        raise ProviderDirectoryRootedGraphStoreError("state")
    conflict = "" if action == "initialize" else " ON CONFLICT(acquisition_id,query_id) DO NOTHING"
    inserted = await database.scalar(
        f"WITH inserted AS (INSERT INTO {table_ref(WORK_TABLE)} SELECT * FROM {stage}{conflict} RETURNING 1) "
        "SELECT count(*) FROM inserted"
    )
    changed = await database.status(
        f"UPDATE {table_ref(ACQUISITION_TABLE)} SET used_work_items=used_work_items+:inserted "
        "WHERE acquisition_id=:acquisition_id AND status='building' AND used_work_items+:inserted<=max_work_items",
        acquisition_id=acquisition_id,
        inserted=inserted,
    )
    if changed != 1:
        raise ProviderDirectoryRootedGraphStoreError("state")
    await database.status(f"DROP TABLE {stage}")
    return inserted


async def _lock_live_result(database, claim):
    """Lock header then work, and check the exact live claim after both locks."""
    from process.provider_directory_rooted_graph_store_support import ACQUISITION_TABLE, WORK_TABLE, table_ref

    header = await database.first(
        f"SELECT acquisition_id FROM {table_ref(ACQUISITION_TABLE)} "
        "WHERE acquisition_id=:acquisition_id AND status='building' FOR UPDATE",
        acquisition_id=claim.acquisition_id,
    )
    if header is None:
        raise ProviderDirectoryRootedGraphStoreError("state")
    await database.first(
        f"SELECT query_id FROM {table_ref(WORK_TABLE)} "
        "WHERE acquisition_id=:acquisition_id AND query_id=:query_id FOR UPDATE",
        acquisition_id=claim.acquisition_id,
        query_id=claim.query_id,
    )
    expected = await database.first(
        f"SELECT * FROM {table_ref(WORK_TABLE)} WHERE acquisition_id=:acquisition_id AND query_id=:query_id "
        "AND status='leased' AND attempt_count=:attempt AND lease_token=:lease_token "
        "AND lease_expires_at>clock_timestamp()",
        acquisition_id=claim.acquisition_id,
        query_id=claim.query_id,
        attempt=claim.attempt,
        lease_token=claim.lease_token,
    )
    if expected is None:
        raise ProviderDirectoryRootedGraphStoreError("lease_lost")


async def _named_result_admission(database, transaction, claim, query_result):
    """Validate and account one leased result before installing its staged data."""
    from process.provider_directory_rooted_graph_store_support import (
        ACQUISITION_TABLE,
        EDGE_TABLE,
        RESOURCE_TABLE,
        table_ref,
    )

    runtime = _named_runtime(database)
    await _lock_live_result(database, claim)
    await _copy_named_result_stages(database, transaction, claim, query_result)
    parameters_by_name = {"acquisition_id": claim.acquisition_id, "query_id": claim.query_id}
    resource_count, payload_bytes, invalid = await database.first(
        _candidate_sql("rooted_graph_resource_candidate_validation.sql", runtime.candidate_schema), **parameters_by_name
    )
    if invalid is not False or resource_count != len(query_result.resources):
        raise ProviderDirectoryRootedGraphStoreError("state")
    edge_count, invalid = await database.first(
        _candidate_sql("rooted_graph_edge_candidate_validation.sql", runtime.candidate_schema), **parameters_by_name
    )
    if invalid is not False or edge_count != len(query_result.edges):
        raise ProviderDirectoryRootedGraphStoreError("state")
    changed = await database.status(
        f"UPDATE {table_ref(ACQUISITION_TABLE)} SET used_resource_rows=used_resource_rows+:resources, "
        "used_edge_rows=used_edge_rows+:edges,used_payload_bytes=used_payload_bytes+:payload_bytes "
        "WHERE acquisition_id=:acquisition_id AND status='building' "
        "AND used_resource_rows+:resources<=max_resource_rows AND used_edge_rows+:edges<=max_edge_rows "
        "AND used_payload_bytes+:payload_bytes<=max_payload_bytes",
        acquisition_id=claim.acquisition_id,
        resources=resource_count,
        edges=edge_count,
        payload_bytes=payload_bytes,
    )
    if changed != 1:
        raise ProviderDirectoryRootedGraphStoreError("state")
    for root, stage in ((RESOURCE_TABLE, "pdrg_resource_stage"), (EDGE_TABLE, "pdrg_edge_stage")):
        await database.status(f"INSERT INTO {table_ref(root)} SELECT * FROM pg_temp.{stage}")
        await database.status(f"DROP TABLE pg_temp.{stage}")


async def _copy_named_result_stages(database, transaction, claim, query_result):
    """Copy bounded result batches and close each temporary stage with its native key."""
    from process.provider_directory_rooted_graph_store_support import EDGE_TABLE, RESOURCE_TABLE

    driver = await _copy_driver(transaction)
    prefix = (claim.acquisition_id, claim.scope_id, claim.query_id, claim.attempt)
    for name, root, columns, result_records in (
        ("pdrg_resource_stage", RESOURCE_TABLE, _RESOURCE_COLUMNS, query_result.resources),
        ("pdrg_edge_stage", EDGE_TABLE, _EDGE_COLUMNS, query_result.edges),
    ):
        await _prepare_named_stage(database, name, root)
        await _copy_stage(
            driver,
            name,
            columns,
            (
                prefix + tuple(getattr(result_record, field) for field in columns[4:])
                for result_record in result_records
            ),
        )
    await database.status(
        "ALTER TABLE pg_temp.pdrg_resource_stage ADD PRIMARY KEY(acquisition_id,query_id,attempt,resource_type,resource_id)"
    )
    await database.status(
        "ALTER TABLE pg_temp.pdrg_edge_stage ADD PRIMARY KEY(acquisition_id,query_id,attempt,edge_sha256)"
    )


async def _named_initial_work(database, acquisition_id):
    from db.plan_release_storage import storage_relation
    from process.provider_directory_rooted_graph_store_support import ACQUISITION_TABLE, WORK_TABLE, table_ref
    from process.provider_directory_source_storage_binding import dataset_source_relations

    async with rooted_graph_transaction(database) as transaction:
        header = row_fields(
            await database.first(
                f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} WHERE acquisition_id=:acquisition_id AND status='building' FOR UPDATE",
                acquisition_id=acquisition_id,
            )
        )
        if not header:
            raise ProviderDirectoryRootedGraphStoreError("state")
        if await database.scalar(
            f"SELECT EXISTS(SELECT FROM {table_ref(WORK_TABLE)} WHERE acquisition_id=:acquisition_id)",
            acquisition_id=acquisition_id,
        ):
            return 0
        runtime = _named_runtime(database)
        root = "provider_directory_dataset_resource"
        tables = await dataset_source_relations(database, runtime.catalog_schema, header["root_dataset_id"], (root,))
        resource = storage_relation(tables[root]["schema"], tables[root]["name"])
        query = _initial_work_sql(runtime.candidate_schema, resource)
        await _prepare_named_stage(database, "pdrg_work_stage", WORK_TABLE)
        driver = await _copy_driver(transaction)
        after_id = ""
        while True:
            page = await database.all(query, acquisition_id=acquisition_id, after_id=after_id, limit=_COPY_ROWS)
            if not page:
                break
            fields = [row_fields(work_record) for work_record in page]
            await _copy_stage(
                driver,
                "pdrg_work_stage",
                _WORK_COLUMNS,
                (tuple(work_record[column] for column in _WORK_COLUMNS) for work_record in fields),
            )
            after_id = fields[-1]["reference_id"]
        return await _named_work_admission(database, acquisition_id, "initialize")


def _data_module(filename):
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / filename
    specification = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


async def named_acquisition_hashes(database, acquisition_id):
    """Keep the original DEFAULT-order content encodings against the named input group."""
    import hashlib
    import re

    from process.provider_directory_rooted_graph_store_support import ACQUISITION_TABLE, table_ref

    runtime = _named_runtime(database)
    original = _data_module("20260811020000_provider_directory_rooted_graph_acquisition.py")
    digests_by_field = {}
    for field, statement in zip(
        ("terminal_set_sha256", "resource_set_sha256", "edge_set_sha256"),
        original._hash_functions_sql(runtime.candidate_schema)[:3],
    ):
        body = re.search(r"AS \$function\$(.*?)\$function\$", statement, re.S).group(1)
        query = body.replace("target_acquisition_id", ":acquisition_id")
        digests_by_field[field] = await database.scalar(query, acquisition_id=acquisition_id)
    scope = await database.scalar(
        f"SELECT scope_id FROM {table_ref(ACQUISITION_TABLE)} WHERE acquisition_id=:acquisition_id",
        acquisition_id=acquisition_id,
    )
    material = "\x1f".join(
        (
            original._ROOT_CONTRACT,
            scope,
            *(digests_by_field[field] for field in ("terminal_set_sha256", "resource_set_sha256", "edge_set_sha256")),
        )
    )
    digests_by_field["rooted_graph_sha256"] = hashlib.sha256(material.encode()).hexdigest()
    return digests_by_field


async def named_request_coverage(database, acquisition_id):
    """Validate inherited request counts and the complete terminal query census."""
    import json

    from db.plan_release_storage import storage_relation
    from process.provider_directory_rooted_graph_request_coverage import validate_rooted_request_coverage
    from process.provider_directory_rooted_graph_store_support import WORK_TABLE, table_ref

    runtime = _named_runtime(database)
    latest = _data_module("20260909120000_fhir_request_failure_budget.py")
    query = latest._coverage_lineage_sql(runtime.catalog_schema).replace("INTO lineage", "")
    canonical = storage_relation(runtime.catalog_schema, "provider_directory_rooted_graph_acquisition")
    query = query.replace(canonical, table_ref("provider_directory_rooted_graph_acquisition"))
    lineage = row_fields(
        await database.first(query.replace("target_acquisition_id", ":acquisition_id"), acquisition_id=acquisition_id)
    )
    if not lineage or type(lineage.get("npi_count")) is not int or lineage["npi_count"] <= 0:
        raise ProviderDirectoryRootedGraphStoreError("state")
    metadata = lineage["metadata"]
    metadata = json.loads(metadata) if isinstance(metadata, str) else metadata
    inherited = metadata.get("retry_exhausted_count", 0)
    if (
        type(inherited) is not int
        or not 0 <= inherited <= min(lineage["npi_count"], 9007199254740991)
        or metadata.get("cohort_complete") is not lineage["cohort_complete"]
        or lineage["cohort_complete"] is not (inherited == 0)
    ):
        raise ProviderDirectoryRootedGraphStoreError("state")
    census = row_fields(
        await database.first(
            "SELECT count(*) AS total,count(*) FILTER(WHERE status='error') AS failed,"
            "count(*) FILTER(WHERE status NOT IN ('completed','error')) AS incomplete,"
            "count(*) FILTER(WHERE status='error' AND error_code IS DISTINCT FROM 'transport_timeout') AS invalid_errors "
            f"FROM {table_ref(WORK_TABLE)} WHERE acquisition_id=:acquisition_id",
            acquisition_id=acquisition_id,
        )
    )
    total_requests, failed_requests, incomplete_requests, invalid_errors = (
        census[key] for key in ("total", "failed", "incomplete", "invalid_errors")
    )
    if total_requests == 0 or incomplete_requests != 0 or invalid_errors != 0:
        raise ProviderDirectoryRootedGraphStoreError("state")
    if failed_requests + inherited == 0:
        return None
    return validate_rooted_request_coverage(
        {
            "policy_id": latest._POLICY,
            "resource_coverage": "unknown",
            "total_requests": lineage["npi_count"] + total_requests,
            "failed_requests": inherited + failed_requests,
            "rooted_total_requests": total_requests,
            "rooted_failed_requests": failed_requests,
        }
    )


async def finish_named_acquisition_storage(database, acquisition_id):
    """Build lookup indexes only after every logical work item terminalizes."""
    from process.provider_directory_rooted_graph_store_support import (
        ACQUISITION_TABLE,
        EDGE_TABLE,
        RESOURCE_TABLE,
        WORK_TABLE,
        table_ref,
    )

    accepted = await database.scalar(
        f"SELECT EXISTS(SELECT FROM {table_ref(ACQUISITION_TABLE)} h WHERE acquisition_id=:acquisition_id "
        f"AND status='building' AND NOT EXISTS(SELECT FROM {table_ref(WORK_TABLE)} w "
        "WHERE w.acquisition_id=h.acquisition_id AND w.status IN ('pending','leased')))",
        acquisition_id=acquisition_id,
    )
    if accepted is not True:
        raise ProviderDirectoryRootedGraphStoreError("state")
    await database.status(
        f"CREATE INDEX IF NOT EXISTS pdrg_named_resource_closure ON {table_ref(RESOURCE_TABLE)} "
        "(acquisition_id,closure_scope,resource_type,resource_id)"
    )
    await database.status(
        f"CREATE INDEX IF NOT EXISTS pdrg_named_edge_target ON {table_ref(EDGE_TABLE)} "
        "(acquisition_id,closure_scope,target_resource_type,target_resource_id)"
    )
    for root in (WORK_TABLE, RESOURCE_TABLE, EDGE_TABLE):
        await database.status(f"ANALYZE {table_ref(root)}")


def acquisition_hash_expression(schema, function_name, acquisition_expression):
    """Inline the original pure content SQL over a named acquisition group."""
    import re

    from db.plan_release_storage import storage_relation

    original = _data_module("20260811020000_provider_directory_rooted_graph_acquisition.py")
    names = (original._TERMINAL_SET, original._RESOURCE_SET, original._EDGE_SET, original._ROOT_HASH)
    sql_by_name = {
        name: re.search(r"AS \$function\$(.*?)\$function\$", statement, re.S).group(1).strip().removesuffix(";")
        for name, statement in zip(names, original._hash_functions_sql(schema))
    }
    query = sql_by_name[function_name]
    if function_name == original._ROOT_HASH:
        for name in names[:3]:
            call = storage_relation(schema, name) + "(header.acquisition_id)"
            query = query.replace(call, acquisition_hash_expression(schema, name, "header.acquisition_id"))
    return "(" + query.replace("target_acquisition_id", acquisition_expression) + ")"
