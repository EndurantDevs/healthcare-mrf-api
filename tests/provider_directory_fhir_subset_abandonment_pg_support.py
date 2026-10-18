# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Committed PostgreSQL fixture for reviewed subset abandonment."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from contextvars import ContextVar

from sqlalchemy.ext.asyncio import create_async_engine

from db.migration_provider_directory_terminal_root_retirement_guards import child_guard_function_sql
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_source_callable_custody as callable_custody
from process.plan_release_storage_provision import _close_relation_grants
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
)
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session
from process.provider_directory_source_checkpoint_catalog import (
    _GUARDS,
    CHECKPOINT_LAYOUTS,
    RETIRED_BULK_HOOKS,
    capture_checkpoint_controls,
    retire_bulk_abandonment_hooks,
)
from process.provider_directory_source_checkpoint_control import source_checkpoint_transaction
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS
from tests.provider_directory_dataset_rehydrate_checkpoint_support import _guard, _layout
from tests.provider_directory_source_postgres_support import native_source_component
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
    replace_subset_source,
)
from tests.provider_directory_subset_completion_pg_setup import (
    load_migration as load_subset_migration,
)
from tests.provider_directory_subset_completion_pg_support import (
    RESOURCE_TYPES,
    VALID_SOURCE_SCOPE_SHA256,
    valid_source_metadata,
)
from tests.tin_npi_connector_postgres_support import (
    POSTGRES_DSN_ENV,
    load_admission_seal_migration,
)

_HISTORICAL_CHECKPOINT_CATALOG = ContextVar("historical_checkpoint_catalog", default=None)

CANONICAL_API_BASE = "https://directory.example.test/fhir"
DATASET_ID = "dataset-abandoned"
ENDPOINT_ID = "endpoint-a"
SERVING_ENDPOINT_ID = "endpoint-serving"
OWNER_RUN_ID = "owner-abandoned"
ROOT_RUN_ID = "root-abandoned"
SOURCE_ID = "synthetic-source"
VERIFICATION_CAMPAIGN_ID = valid_source_metadata("pending_two_matching_reviewed_subset_acquisitions")[
    "provider_directory_verification_campaign_id"
]


async def install_admission_query_surface(scenario) -> None:
    """Install current admission readers in the historical fixture."""

    subset = load_subset_migration()
    admission = load_admission_seal_migration()
    signature = subset._qf(scenario.schema, subset._CONTENT_PROOF_VALID_FUNCTION) + (
        "(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)"
    )
    for statement in (
        subset._content_proof_valid_function_sql(scenario.schema),
        f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC;",
        admission._add_columns_sql(scenario.schema),
        admission._digest_function_sql(scenario.schema),
    ):
        await scenario.connection.execute(statement)


def guard_handoff_context(importer):
    """Build the exact retained-root guard context used by the race proof."""

    return importer.PaginationCheckpointContext(
        canonical_api_base=CANONICAL_API_BASE,
        source_scope_hash="1" * 64,
        source_ids=(SOURCE_ID,),
        owner_run_id=OWNER_RUN_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
    )


_PROOF_RELATION_SQL = """
CREATE TABLE {schema}.provider_directory_dataset_proof_shard (
    dataset_id varchar(96) NOT NULL,
    shard_id varchar(64) NOT NULL,
    endpoint_id varchar(64) NOT NULL,
    acquisition_root_run_id varchar(64) NOT NULL,
    source_ids_json jsonb NOT NULL,
    resource_count bigint NOT NULL,
    resource_counts_json jsonb NOT NULL,
    first_identity_json jsonb NOT NULL,
    last_identity_json jsonb NOT NULL,
    input_sha256 varchar(64) NOT NULL,
    artifact_sha256 varchar(64) NOT NULL,
    artifact_byte_count bigint NOT NULL,
    payload_bytes bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, shard_id),
    FOREIGN KEY (dataset_id) REFERENCES
        {schema}.provider_directory_endpoint_dataset(dataset_id)
        ON DELETE CASCADE
)
"""


async def _current_checkpoint_guards(scenario, retired_guards):
    connection, schema = scenario.connection, scenario.schema
    await connection.execute(child_guard_function_sql(schema))
    retired_guards.extend(
        await connection.fetch(
            "SELECT tgname,pg_get_triggerdef(oid) AS definition FROM pg_trigger "
            "WHERE tgrelid=to_regclass($1) AND tgname=ANY($2::text[])",
            f'"{schema}"."provider_directory_bulk_acquisition_checkpoint"',
            sorted(RETIRED_BULK_HOOKS),
        )
    )
    for root, guards in _GUARDS.items():
        existing = await connection.fetch(
            "SELECT tgname FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal",
            f'"{schema}"."{root}"',
        )
        existing_names = {entry["tgname"] for entry in existing}
        for name, guard in guards.items():
            if name not in RETIRED_BULK_HOOKS and name not in existing_names:
                await _guard(connection, schema, root, name, guard)


async def _restore_checkpoint_bulk_guards(scenario, guards):
    assert source_policy._FACTORY.get() is None
    await _restore_saved_bulk_guards(scenario, guards)


async def _restore_saved_bulk_guards(scenario, guards):
    for guard in guards:
        exists = await scenario.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname=$2)",
            f"{scenario.quoted_schema}.provider_directory_bulk_acquisition_checkpoint",
            guard["tgname"],
        )
        if exists:
            continue
        await scenario.connection.execute(guard["definition"])
        await scenario.connection.execute(
            f"ALTER TABLE {scenario.quoted_schema}.provider_directory_bulk_acquisition_checkpoint "
            f'ENABLE ALWAYS TRIGGER "{guard["tgname"]}"'
        )


@asynccontextmanager
async def historical_checkpoint_catalog(scenario, *, retire_after=True):
    """Run historical migration fences against their exact saved predecessor hooks."""
    current = _HISTORICAL_CHECKPOINT_CATALOG.get()
    if current is None:
        yield
        return
    original, policy, guards = current
    assert scenario is original and source_policy._PUBLISHER.get() is None
    await _restore_saved_bulk_guards(scenario, guards)
    yield
    if retire_after:
        before = await capture_checkpoint_controls(scenario.connection, policy, legacy=True)
        await retire_bulk_abandonment_hooks(scenario.connection, policy, before)


_HISTORICAL_METADATA_CONTRACTS = {
    "provider_directory_subset_replay_evidence_shape_valid(jsonb,text,jsonb,text)": (
        (
            ("replay_candidate", "jsonb"),
            ("replay_sha256", "text"),
            ("completion_candidate", "jsonb"),
            ("completion_sha256", "text"),
        ),
        "boolean",
        (),
        "plpgsql",
        "i",
        True,
        "s",
        True,
        "6293d6acf2d5cde949fc58f0444575073a1d90258441ff9ab429fefcec4cd1d9",
    ),
    "provider_directory_subset_coverage_shape_valid(jsonb,jsonb,text,text)": (
        (
            ("coverage_candidate", "jsonb"),
            ("completion_candidate", "jsonb"),
            ("completion_sha256", "text"),
            ("expected_twin_state", "text"),
        ),
        "boolean",
        (),
        "plpgsql",
        "i",
        True,
        "s",
        True,
        "d24502e18298579a1169cf748f456830265e1ada3c6d916256d442f0706bc779",
    ),
}
_METADATA_SIGNATURES = (
    *(
        signature
        for signature in METADATA_FUNCTIONS
        if signature
        in {
            "provider_directory_subset_canonical_sha256(jsonb)",
            "provider_directory_subset_completion_proof_pair_valid(jsonb,text)",
            "provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)",
        }
    ),
    *_HISTORICAL_METADATA_CONTRACTS,
)

# These two historical fixtures intentionally precede the terminal-tail migration.
# Keep all other native definition fields and ACL checks identical to current custody.
_HISTORICAL_SHAPE_DIGESTS = frozenset(
    (
        "aa0730df0506d9288e7ff8d0eb3c1b7ca871ffd74de744f84af9540a36c835e9",
        "02dfe5b14bf1c68e820f54fc47b389addef6f037c38b7ee4814994318304ab7a",
    )
)


_OPTIONAL_METADATA_SIGNATURE = (
    "provider_directory_subset_content_proof_valid(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)"
)


def _require_metadata_entry(entry, policy, namespace, owner_oid, admin_oid, contract_by_name):
    """Validate one fixed callable's exact native definition, owner and ACL."""
    definition = entry["definition"]
    contract = contract_by_name[definition["proname"]]
    if definition["proname"] == "provider_directory_subset_completion_proof_shape_valid":
        digest = callable_custody._body_digest(definition["prosrc"], policy.schema)
        if digest in _HISTORICAL_SHAPE_DIGESTS:
            contract = (*contract[:-1], digest)
    callable_custody._require_definition(
        entry,
        contract,
        policy.schema,
        namespace,
        callable_custody._TYPE_OIDS,
        storage_identifier(policy.schema),
    )
    assert int(definition["proowner"]) == admin_oid
    edges = set(map(tuple, entry["acl"]))
    required_edges = {(admin_oid, admin_oid, "EXECUTE", False)}
    assert len(edges) == len(entry["acl"]) and required_edges <= edges
    assert edges <= required_edges | {(owner_oid, admin_oid, "EXECUTE", False)}


async def _metadata_catalog(connection, policy, owner_oid, admin_oid):
    optional_name, _, optional_arguments = _OPTIONAL_METADATA_SIGNATURE.partition("(")
    optional_signature = storage_relation(policy.schema, optional_name) + "(" + optional_arguments
    optional_oid = await connection.fetchval("SELECT to_regprocedure($1)::oid", optional_signature)
    selected = (*_METADATA_SIGNATURES, *((_OPTIONAL_METADATA_SIGNATURE,) if optional_oid is not None else ()))
    names = [signature.split("(")[0] for signature in selected]
    signatures = [
        storage_relation(policy.schema, name) + "(" + signature.split("(", 1)[1]
        for name, signature in zip(names, selected, strict=True)
    ]
    namespace = await connection.fetchval("SELECT to_regnamespace($1)::oid", policy.schema)
    catalog_rows = await connection.fetch(
        callable_custody._FUNCTION_SQL, signatures, policy.schema, names, list(policy.runtime_roles)
    )
    entries = [
        {
            **dict(catalog_entry),
            **{key: callable_custody._decoded(catalog_entry[key]) for key in ("definition", "acl", "access")},
        }
        for catalog_entry in catalog_rows
    ]
    assert len(entries) == len(signatures) and {entry["signature"] for entry in entries} == set(signatures)
    assert len({int(entry["definition"]["oid"]) for entry in entries}) == len(entries)
    contract_by_name = {
        key.split("(")[0]: spec
        for key, spec in {**callable_custody._CONTRACTS, **_HISTORICAL_METADATA_CONTRACTS}.items()
    }
    for entry in entries:
        _require_metadata_entry(entry, policy, namespace, owner_oid, admin_oid, contract_by_name)
    return {entry["signature"]: entry for entry in entries}


def _require_metadata_delta(before, after, owner_oid, admin_oid, owner_usage_by_oid):
    assert before.keys() == after.keys()
    for signature, previous in before.items():
        current = after[signature]
        assert {key: value for key, value in previous["definition"].items() if key != "proacl"} == {
            key: value for key, value in current["definition"].items() if key != "proacl"
        }
        assert previous["language"] == current["language"]
        expected_acl = set(map(tuple, previous["acl"])) | {(owner_oid, admin_oid, "EXECUTE", False)}
        assert set(map(tuple, current["acl"])) == expected_acl
        expected_access = [
            [*catalog_entry[:-1], catalog_entry[-1] or owner_usage_by_oid[catalog_entry[0]]]
            for catalog_entry in previous["access"]
        ]
        assert current["access"] == expected_access


async def _grant_metadata_controls(connection, policy):
    """Grant only the fixed metadata predicates inside original admin custody."""
    assert connection.is_in_transaction() is True and len(_METADATA_SIGNATURES) == 5
    task = asyncio.current_task()
    async with connection.transaction():
        actor = await callable_custody._admin_identity(connection, policy)
        owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", policy.owner_role)
        admin_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", actor["current"])
        assert type(owner_oid) is int and type(admin_oid) is int and owner_oid > 0 and admin_oid > 0
        owner_usage_by_oid = {
            catalog_entry["oid"]: catalog_entry["owner_usage"]
            for catalog_entry in await connection.fetch(
                "SELECT oid::bigint AS oid,pg_has_role(oid,$1::oid,'USAGE') AS owner_usage FROM pg_roles ORDER BY oid",
                owner_oid,
            )
        }
        before = await _metadata_catalog(connection, policy, owner_oid, admin_oid)
        for signature in before:
            await connection.execute(
                f"GRANT EXECUTE ON FUNCTION {signature} TO {storage_identifier(policy.owner_role)}"
            )
        after = await _metadata_catalog(connection, policy, owner_oid, admin_oid)
        _require_metadata_delta(before, after, owner_oid, admin_oid, owner_usage_by_oid)
        assert asyncio.current_task() is task
        assert await callable_custody._admin_identity(connection, policy) == actor


async def _enroll_checkpoint_custody(scenario, component):
    connection, schema = scenario.connection, scenario.schema
    await _grant_metadata_controls(connection, component.policy)
    for root in CHECKPOINT_LAYOUTS:
        relation = f'"{schema}"."{root}"'
        oid = await connection.fetchval("SELECT to_regclass($1)::oid", relation)
        assert type(oid) is int and oid > 0
        await _close_relation_grants(
            connection,
            {"oid": oid, "nspname": schema, "relname": root},
            component.owner,
            component.policy.runtime_roles,
        )
        assert await connection.fetchval("SELECT to_regclass($1)::oid", relation) == oid
    function_names = {guard[-1] for guards in _GUARDS.values() for guard in guards.values()}
    for name in sorted(function_names):
        function = f'"{schema}"."{name}"()'
        oid = await connection.fetchval("SELECT to_regprocedure($1)::oid", function)
        assert type(oid) is int and oid > 0
        await connection.execute(f'ALTER FUNCTION {function} OWNER TO "{component.owner}"')
        assert await connection.fetchval("SELECT to_regprocedure($1)::oid", function) == oid
    for name in ("provider_directory_subset_abandonment_valid", "provider_directory_subset_terminal_disposition_valid"):
        function = f'"{schema}"."{name}"(text)'
        assert await connection.fetchval("SELECT to_regprocedure($1)::oid", function) is not None
        await connection.execute(f'GRANT EXECUTE ON FUNCTION {function} TO "{component.owner}"')


@asynccontextmanager
async def _current_writer(scenario, monkeypatch):
    """Issue current checkpoint custody after the independent admin migration setup."""
    assert source_policy._FACTORY.get() is None
    dsn = os.environ[POSTGRES_DSN_ENV].replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(dsn, pool_size=1, max_overflow=0)
    async with AsyncExitStack() as resources:
        resources.push_async_callback(engine.dispose)
        retired_guards = []
        resources.push_async_callback(_restore_checkpoint_bulk_guards, scenario, retired_guards)
        component = await resources.enter_async_context(native_source_component(engine, scenario.schema, monkeypatch))
        async with scenario.connection.transaction():
            await _current_checkpoint_guards(scenario, retired_guards)
            await _enroll_checkpoint_custody(scenario, component)
            checkpoint_before = await capture_checkpoint_controls(scenario.connection, component.policy, legacy=True)
            await retire_bulk_abandonment_hooks(scenario.connection, component.policy, checkpoint_before)
        async with source_checkpoint_transaction(component.database, schema=scenario.schema):
            publisher = bound_publisher()
            assert publisher.policy is component.policy
            assert source_copy_session() is component.database._transaction_binding().session
        token = _HISTORICAL_CHECKPOINT_CATALOG.set((scenario, component.policy, tuple(retired_guards)))
        try:
            yield component.database
        finally:
            _HISTORICAL_CHECKPOINT_CATALOG.reset(token)


async def upgrade_abandonment_writer(scenario):
    """Explicitly reach the current guard revision before using the current writer."""
    from tests.test_provider_directory_reviewed_subset_terminal_disposition_postgres import (
        _load_bounded_migration,
        _load_effective_endpoint_migration,
        _load_migration,
        _load_policy_migration,
        _run_migration,
    )

    for migration in (
        _load_effective_endpoint_migration(),
        _load_policy_migration(),
        _load_bounded_migration(),
        _load_migration(),
    ):
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")


async def create_abandonment_relations(scenario) -> None:
    """Create the proof relation and all four fixed current checkpoint layouts."""

    await scenario.connection.execute(_PROOF_RELATION_SQL.format(schema=scenario.quoted_schema))
    for root, layout in CHECKPOINT_LAYOUTS.items():
        await _layout(scenario.connection, scenario.schema, root, layout)


def terminal_diagnostics() -> dict[str, dict[str, object]]:
    return {
        resource_type: {
            "bounded": False,
            "complete": False,
            "error": f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:http_410",
            "fetch_mode": SERVER_ISSUED_SUBSET_FETCH_MODE,
        }
        for resource_type in RESOURCE_TYPES
    }


def authorize_operator(monkeypatch, enabled_env: str) -> None:
    """Bind the selector-free operator to the neutral reviewed fixture."""

    monkeypatch.setenv(enabled_env, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog.reviewed_manual_census_source_id",
        lambda: SOURCE_ID,
    )


async def seed_expired_root(scenario) -> None:
    """Seed one exact seven-resource failed root before guard adoption."""

    await _seed_source_alias(scenario)
    await insert_subset_candidate(
        scenario,
        dataset_id=DATASET_ID,
        root_run_id=ROOT_RUN_ID,
        resource_count=0,
    )
    await _seed_serving_decoy(scenario)
    await _bind_expired_candidate_identity(scenario)
    await insert_valid_subset_resources(scenario, DATASET_ID)
    await _seed_proof_shard(scenario)
    await _seed_pagination_checkpoints(scenario)


async def _seed_source_alias(scenario) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ($1), ($2)
        """,
        ENDPOINT_ID,
        SERVING_ENDPOINT_ID,
    )
    await replace_subset_source(
        scenario,
        "pending_two_matching_reviewed_subset_acquisitions",
        last_resource_import={
            "run_id": OWNER_RUN_ID,
            "resources": terminal_diagnostics(),
        },
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET endpoint_id = $1
         WHERE source_id = $2
        """,
        SERVING_ENDPOINT_ID,
        SOURCE_ID,
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type, metadata_json
        ) VALUES (
            'synthetic-serving-sibling', $1, $2,
            false, false, 'none', $3::jsonb
        )
        """,
        SERVING_ENDPOINT_ID,
        CANONICAL_API_BASE,
        json.dumps({"provider_directory_configured_endpoint_id": SERVING_ENDPOINT_ID}),
    )


async def _seed_serving_decoy(scenario) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, status,
            is_current, resource_count, publication_metadata_json,
            completion_proof_required_version
        ) VALUES (
            'dataset-serving-decoy', $1, 'root-serving-decoy', 'failed',
            false, 0, $2::jsonb, NULL
        )
        """,
        SERVING_ENDPOINT_ID,
        json.dumps(
            {
                "source_ids": [SOURCE_ID],
                "selected_resources": list(RESOURCE_TYPES),
            }
        ),
    )


async def _bind_expired_candidate_identity(scenario) -> None:
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET import_run_id = $1,
               status = 'failed',
               publication_metadata_json = $2::jsonb
         WHERE dataset_id = $3
        """,
        OWNER_RUN_ID,
        json.dumps(
            {
                "source_ids": [SOURCE_ID],
                "selected_resources": list(RESOURCE_TYPES),
                "verification_source_scope_hash": VALID_SOURCE_SCOPE_SHA256,
                "verification_campaign_id": VERIFICATION_CAMPAIGN_ID,
            }
        ),
        DATASET_ID,
    )


async def _seed_proof_shard(scenario) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_proof_shard (
            dataset_id, shard_id, endpoint_id, acquisition_root_run_id,
            source_ids_json, resource_count, resource_counts_json,
            first_identity_json, last_identity_json, input_sha256,
            artifact_sha256, artifact_byte_count, payload_bytes
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7::jsonb,
            $8::jsonb, $9::jsonb, $10, $11, $12, $13
        )
        """,
        DATASET_ID,
        "a" * 64,
        ENDPOINT_ID,
        ROOT_RUN_ID,
        json.dumps([SOURCE_ID]),
        len(RESOURCE_TYPES),
        json.dumps(dict.fromkeys(RESOURCE_TYPES, 1)),
        json.dumps([RESOURCE_TYPES[0], "resource-a", "b" * 64]),
        json.dumps([RESOURCE_TYPES[-1], "resource-z", "c" * 64]),
        "d" * 64,
        "e" * 64,
        5,
        b"proof",
    )


async def _seed_pagination_checkpoints(scenario) -> None:
    await scenario.connection.executemany(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_pagination_checkpoint (
            canonical_api_base, resource_type, source_scope_hash, dataset_id,
            source_ids, acquisition_root_run_id, owner_run_id, retry_of_run_id,
            start_url_hash, next_url, state, pages_processed, rows_processed,
            recent_cursor_hashes, completeness_json
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
            $9, $10, 'active', 1, 1, $11::jsonb, $12::jsonb
        )
        """,
        [
            (
                CANONICAL_API_BASE,
                resource_type,
                "1" * 64,
                DATASET_ID,
                json.dumps([SOURCE_ID]),
                ROOT_RUN_ID,
                OWNER_RUN_ID,
                "owner-prior",
                f"{ordinal:064x}",
                f"https://directory.example.test/next/{ordinal}",
                json.dumps([f"{ordinal + 10:064x}"]),
                json.dumps({"verified": False}),
            )
            for ordinal, resource_type in enumerate(RESOURCE_TYPES, start=1)
        ],
    )


async def retained_evidence_snapshot(scenario) -> dict[str, str]:
    """Return stable JSON snapshots of every retained evidence relation."""

    queries_by_name = {
        "resources": f"""
            SELECT COALESCE(jsonb_agg(to_jsonb(resource) ORDER BY
                       resource.resource_type, resource.resource_id), '[]'::jsonb)::text
              FROM {scenario.quoted_schema}.provider_directory_dataset_resource AS resource
             WHERE resource.dataset_id = $1
        """,
        "proofs": f"""
            SELECT COALESCE(jsonb_agg(to_jsonb(shard) ORDER BY shard.shard_id),
                            '[]'::jsonb)::text
              FROM {scenario.quoted_schema}.provider_directory_dataset_proof_shard AS shard
             WHERE shard.dataset_id = $1
        """,
        "checkpoints": f"""
            SELECT COALESCE(jsonb_agg(
                       to_jsonb(checkpoint) - ARRAY['state','updated_at','completed_at']
                       ORDER BY checkpoint.resource_type), '[]'::jsonb)::text
              FROM {scenario.quoted_schema}.provider_directory_pagination_checkpoint
                   AS checkpoint
             WHERE checkpoint.dataset_id = $1
        """,
        "source": f"""
            SELECT metadata_json::jsonb::text
              FROM {scenario.quoted_schema}.provider_directory_source
             WHERE source_id = $1
        """,
    }
    return {
        snapshot_name: await scenario.connection.fetchval(
            snapshot_query,
            SOURCE_ID if snapshot_name == "source" else DATASET_ID,
        )
        for snapshot_name, snapshot_query in queries_by_name.items()
    }


async def close_abandonment_scenario(scenario, *resources) -> None:
    for resource in resources:
        with suppress(Exception):
            await resource.disconnect()
        with suppress(Exception):
            await resource.close()
    with suppress(Exception):
        await scenario.connection.execute("ROLLBACK")
    await scenario.connection.execute(f"DROP SCHEMA IF EXISTS {scenario.quoted_schema} CASCADE")
    await scenario.connection.close()
