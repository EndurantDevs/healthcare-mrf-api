# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native SOURCE authority and isolated NPPES candidates on disposable data."""

import ast
import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from graphlib import TopologicalSorter
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import Index, MetaData, text
from sqlalchemy.ext.asyncio import create_async_engine

from db.migration_nppes_evidence_candidates import upgrade_nppes_evidence_candidates
from db.plan_release_storage import storage_identifier, storage_relation
from process import nppes_public_evidence_storage as storage
from process import provider_directory_generic_source_policy as source
from process import provider_directory_source_acquisition_custody as acquisition
from process import provider_directory_source_cohort_custody as cohort
from process.nppes_public_evidence_control_custody import adopt_nppes_control_custody
from process.provider_directory_cms_control_custody import _RIGHTS as control_rights
from tests.cms_control_privileges_support import grant_cms_origin_metadata_read_privileges
from tests.cms_npd_admission_postgres_support import (
    _owned_database,
    _provision_cms_epoch_controls,
    _run_migrations,
)
from tests.formulary_fhir_twin_admission_pg_support import load_migration, run_migration
from tests.provider_directory_source_postgres_support import (
    _named_component_generations,
    native_source_component,
    provision_source_component,
    source_component_migrations,
)
from tests.public_evidence_storage_postgres_support import database_url

_COMPONENT = ContextVar("nppes_candidate_native_component", default=None)
SYNTHETIC_PRODUCER = ("a" * 40, "sha256:" + "b" * 64)


def nppes_source_component_migrations():
    """Include the authentic full callable corpus and its fixed typed-call repairs."""
    prefixes = tuple(
        dict.fromkeys(
            (
                *source_component_migrations(),
                "20260825090000",
                "20260830090000",
                "20260909120000",
                "20261001110000",
                "20261004000000",
                "20261016010000",
                "20261016030000",
                "20261016040000",
                "20261016050000",
                "20261016060000",
                "20261019130000",
            )
        )
    )
    versions = Path(__file__).parents[1] / "alembic" / "versions"
    metadata = {}
    revision_by_path = {}
    for path in versions.glob("*.py"):
        fields_by_key = {}
        for node in ast.parse(path.read_text()).body:
            migration_targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
                if isinstance(node, ast.AnnAssign)
                else ()
            )
            for migration_target in migration_targets:
                if isinstance(migration_target, ast.Name) and migration_target.id in {"revision", "down_revision"}:
                    fields_by_key[migration_target.id] = ast.literal_eval(node.value)
        if fields_by_key.get("revision"):
            revision = fields_by_key["revision"]
            assert revision not in metadata
            parent = fields_by_key.get("down_revision")
            metadata[revision] = () if parent is None else (parent,) if isinstance(parent, str) else parent
            revision_by_path[path] = revision
    selected_by_revision = {}
    for prefix in prefixes:
        paths = list(versions.glob(prefix + "*.py"))
        if len(paths) > 1:
            paths = [path for path in paths if "provider_directory" in path.name]
        assert len(paths) == 1, prefix
        selected_by_revision[revision_by_path[paths[0]]] = prefix
    graph_by_revision = {}
    for revision in selected_by_revision:
        ancestors, pending = set(), list(metadata[revision])
        while pending:
            parent = pending.pop()
            if parent not in ancestors:
                ancestors.add(parent)
                pending.extend(metadata.get(parent, ()))
        graph_by_revision[revision] = tuple(parent for parent in selected_by_revision if parent in ancestors)
    return tuple(selected_by_revision[revision] for revision in TopologicalSorter(graph_by_revision).static_order())


def nppes_source_component_migration_stages():
    """Install canonical primitives before typed-call repairs and native control DCL."""
    prefixes = nppes_source_component_migrations()
    boundary = prefixes.index("20261016040000")
    return prefixes[:boundary], prefixes[boundary:]


async def _bootstrap_nppes_parent_component(engine, policy, prefixes, *, migration_role=None):
    """Create the native family as its declared initial actor, defaulting to the legacy Builder."""
    from db.models import CMSDoctorEducation, DoctorClinicianAddress

    actor = policy.control_runtime_roles[0] if migration_role is None else migration_role
    async with engine.begin() as connection:
        if migration_role is not None:
            assert await connection.scalar(text("SELECT session_user::text")) == migration_role
            assert await connection.scalar(
                text("SELECT current_user=session_user AND rolsuper FROM pg_roles WHERE rolname=current_user")
            )
        database = await connection.scalar(text("SELECT current_database()"))
        await connection.execute(text(f'GRANT CREATE ON DATABASE "{database}" TO "{actor}"'))
    historical = create_async_engine(engine.url.set(username=actor))
    try:
        async with historical.begin() as connection:
            for model in (DoctorClinicianAddress, CMSDoctorEducation):
                table = model.__table__.to_metadata(MetaData(), schema=policy.schema)
                assert await connection.scalar(text("SELECT to_regclass(:name)"), dict(name=table.fullname)) is None
                await connection.run_sync(lambda native: table.create(native, checkfirst=False))
                for specification in getattr(model, "__my_additional_indexes__", ()):
                    index = Index(
                        table.name + "_idx_" + specification["name"],
                        *(table.c[name] for name in specification["index_elements"]),
                    )
                    await connection.run_sync(lambda native: index.create(native, checkfirst=False))
        async with historical.connect() as connection:
            await connection.run_sync(_run_migrations, prefixes)
    finally:
        await historical.dispose()
        async with engine.begin() as connection:
            await connection.execute(text(f'REVOKE CREATE ON DATABASE "{database}" FROM "{actor}"'))


async def install_source_canonical_functions(engine, schema):
    """Install authentic shared primitives without unrelated terminal guard patches."""
    signatures = (
        "ptg_wave_json_ascii_text_v1(text)",
        "ptg_wave_canonical_json_ascii_v1(jsonb)",
        "ptg_wave_canonical_json_ascii_v1(json)",
        "ptg_wave_hex_to_numeric_v1(text)",
        "ptg_wave_rsa2048_pkcs1_sha256_verify_v1(text,text,integer,bytea)",
        "ptg_wave_is_valid_signed_receipt_v1(jsonb,text,jsonb,text,text,integer)",
        "ptg_wave_ordinary_terminal_receipt_guard()",
    )
    async with engine.connect() as connection:
        assert (
            await connection.scalar(
                text(
                    "SELECT NOT EXISTS (SELECT 1 FROM unnest(CAST(:signatures AS text[])) f(signature) "
                    "WHERE to_regprocedure(signature) IS NOT NULL)"
                ),
                dict(signatures=[f'"{schema}".{signature}' for signature in signatures]),
            )
            is True
        ), "canonical fragment installation requires the untouched component slice"
    versions = Path(__file__).parents[1] / "alembic" / "versions"
    canonical = load_migration(versions / "20260810110000_ptg_wave_receipt_authority.py", "nppes_canonical")
    canonical.install = lambda: canonical._install_receipt_verification_functions(schema)
    await run_migration(engine, canonical, "install")
    json_overload = load_migration(
        versions / "20260820030000_ptg_ordinary_terminal_json_canonical_digest.py", "nppes_json_canonical"
    )
    json_overload._schema = lambda: schema
    json_overload.install = lambda: json_overload.op.execute(json_overload._canonical_json_function_sql())
    await run_migration(engine, json_overload, "install")


async def install_nppes_enumeration_storage(engine, policy):
    """Run authentic enumeration SQL with the pure NPI helper's legacy Builder actor."""
    path = (
        Path(__file__).parents[1] / "alembic" / "versions" / "20260808170000_public_evidence_npi_enumeration_storage.py"
    )
    migration = load_migration(path, "nppes_enumeration")

    def install():
        operation = migration.op
        name = f'"{policy.schema}"."public_evidence_npi_valid"('

        def execute(statement):
            is_pure_helper = statement.lstrip().startswith(
                ("CREATE FUNCTION " + name, "REVOKE ALL ON FUNCTION " + name)
            )
            if not is_pure_helper:
                return operation.execute(statement)
            operation.execute(f'SET LOCAL ROLE "{policy.control_runtime_roles[0]}"')
            try:
                return operation.execute(statement)
            finally:
                operation.execute(f'SET LOCAL ROLE "{policy.owner_role}"')

        operation.execute(f'SET LOCAL ROLE "{policy.owner_role}"')
        migration.op = SimpleNamespace(execute=execute)
        try:
            migration.upgrade()
        finally:
            migration.op = operation

    migration.install = install
    await run_migration(engine, migration, "install")


def candidate_component(schema):
    component = _COMPONENT.get()
    assert component is not None and component.policy.schema == schema
    return component


async def resolved_family(schema, admission_ref):
    component = candidate_component(schema)
    async with source.source_transaction(component.database, isolation="repeatable read"):
        connection = source.bound_publisher().connection
        return await storage.resolve_nppes_evidence_family(connection, schema, admission_ref)


async def loading_family(connection, schema, admission_ref):
    """Inspect only the test administrator's persisted loading candidate binding."""
    value = await connection.fetchval(
        f'SELECT candidate_storage_json FROM "{schema}"."{storage.HEADER}" WHERE admission_ref=$1', admission_ref
    )
    binding = json.loads(value) if isinstance(value, str) else value
    assert binding["phase"] == "loading"
    return {root: proof["name"] for root, proof in binding["tables"].items()}


# The Source callables the removed acquisition adoption re-owned; "@work" is the rooted work row type.
_SOURCE_CALLABLES = """
account_provider_directory_rooted_graph_edge_budget() account_provider_directory_rooted_graph_resource_budget()
account_provider_directory_rooted_graph_work_budget() advance_reference_source_generation()
guard_pd_dataset_bulk_parent_delete() guard_pd_dataset_parent_key_update() guard_pd_uhc_flex_candidate_insert()
guard_pd_uhc_flex_practitioner_acquisition() guard_pd_uhc_flex_practitioner_admission_immutable()
guard_pd_uhc_flex_practitioner_admission_insert() guard_pd_uhc_flex_practitioner_dataset()
guard_pd_uhc_flex_practitioner_dataset_endpoint() guard_pd_uhc_flex_practitioner_dataset_parent()
guard_pd_uhc_flex_practitioner_dataset_resource() guard_pd_uhc_flex_practitioner_dataset_source()
guard_pd_uhc_flex_practitioner_resource() guard_pd_uhc_flex_practitioner_single_root_admission_insert()
guard_pd_uhc_flex_practitioner_twin_attempt_immutable() guard_pd_uhc_flex_practitioner_twin_attempt_insert()
guard_pd_uhc_flex_practitioner_work() guard_provider_directory_endpoint_dataset_admission_seal()
guard_provider_directory_exact_logical_current() guard_provider_directory_reviewed_subset_activation_source()
guard_provider_directory_rooted_graph_acquisition() guard_provider_directory_rooted_graph_copy_owner()
guard_provider_directory_rooted_graph_dataset() guard_provider_directory_rooted_graph_dataset_resource()
guard_provider_directory_rooted_graph_dependency() guard_provider_directory_rooted_graph_single_root_admission()
guard_provider_directory_rooted_graph_storage_seal() guard_provider_directory_rooted_graph_twin_admission()
guard_provider_directory_rooted_graph_twin_attempt() guard_provider_directory_rooted_graph_work()
guard_provider_directory_subset_published_source() guard_provider_directory_terminal_root_retirement_child()
guard_provider_directory_uhc_flex_npi_cohort() guard_provider_directory_uhc_flex_npi_member()
guard_tin_npi_connector_endpoint_dataset() pd_practitioner_migration_fence()
provider_directory_profile_delta_receipt_immutable_v1()
reject_provider_directory_rooted_graph_witness_mutation() validate_provider_directory_uhc_flex_npi_members()
admit_pd_uhc_flex_practitioner_stage(text,text,bigint,integer,text)
admit_provider_directory_rooted_graph_witnesses(text,text,integer,text,regclass,regclass)
admit_provider_directory_rooted_graph_work(text,text) assert_provider_directory_rooted_graph_storage(text)
attach_provider_directory_rooted_graph_storage(text)
finish_provider_directory_rooted_graph_initial_storage(text)
finish_provider_directory_rooted_graph_storage(text) initialize_pd_uhc_flex_practitioner_work(text,text)
initialize_provider_directory_rooted_graph_work(text) pd_uhc_flex_practitioner_terminal_set_sha256(text)
prepare_pd_uhc_flex_practitioner_stage() prepare_pd_uhc_flex_practitioner_work(text,text)
prepare_provider_directory_rooted_graph_storage(text) prepare_provider_directory_rooted_graph_witness_stage()
prepare_provider_directory_rooted_graph_work_stage()
provider_directory_endpoint_dataset_admission_metadata_sha256(jsonb,smallint,text,text,varchar[])
provider_directory_fhir_request_failure_coverage_valid(jsonb)
provider_directory_reviewed_subset_activation_valid(text)
provider_directory_rooted_graph_dataset_intrinsic_valid(text)
provider_directory_rooted_graph_dataset_ready(text) provider_directory_rooted_graph_dataset_valid(text)
provider_directory_rooted_graph_edge_set_sha256(text)
provider_directory_rooted_graph_official_lineage_current(text)
provider_directory_rooted_graph_request_failure_coverage(text)
provider_directory_rooted_graph_resource_set_sha256(text) provider_directory_rooted_graph_sha256(text)
provider_directory_rooted_graph_terminal_set_sha256(text) provider_directory_subset_canonical_sha256(jsonb)
provider_directory_subset_completion_canonical_json(jsonb)
provider_directory_subset_completion_proof_pair_valid(jsonb,text)
provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)
provider_directory_subset_content_proof_valid(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)
provider_directory_subset_payload_canonical_json(jsonb) provider_directory_subset_payload_sha256(jsonb)
provider_directory_uhc_flex_practitioner_dataset_ready(text)
provider_directory_uhc_flex_practitioner_dataset_valid(text) ptg_wave_canonical_json_ascii_v1(json)
ptg_wave_canonical_json_ascii_v1(jsonb) ptg_wave_json_ascii_text_v1(text) public_evidence_npi_valid(text)
read_provider_directory_rooted_graph_initial_work(text,text,integer)
validate_provider_directory_rooted_graph_stage_checks(regclass,regclass)
validate_provider_directory_rooted_graph_witnesses(regclass,regclass,@work)
validate_provider_directory_rooted_graph_work_stage(text,text)
""".split()


async def _adopt_acquisition_family(driver, policy):
    """Stand in for the operator's one-time adoption: the Source owner owns the acquisition family
    with full rights, and the builder keeps only its fixed writer rights."""
    owner, builder = storage_identifier(policy.owner_role), storage_identifier(policy.control_runtime_roles[0])
    members = [(root, root, False) for root in (*acquisition.ACQUISITION_ROOTS, *acquisition.METADATA_ROOTS)]
    members += [(root + "_legacy", root, True) for root in acquisition.BULK_ROOTS]
    for name, root, history in members:
        table = storage_relation(policy.schema, name)
        columns = await driver.fetch(
            "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped "
            "ORDER BY attnum",
            table,
        )
        if not columns:
            continue
        await driver.execute(f"ALTER TABLE {table} OWNER TO {owner}")
        await driver.execute(f"REVOKE ALL ON TABLE {table} FROM {builder}")
        await driver.execute(f"GRANT {','.join(sorted(control_rights))} ON TABLE {table} TO {owner}")
        table_rights, column_rights = acquisition._writer_rights(
            root, [column["attname"] for column in columns], history=history
        )
        if table_rights:
            await driver.execute(f"GRANT {','.join(sorted(table_rights))} ON TABLE {table} TO {builder}")
        for right, names in column_rights.items():
            selected = ",".join(storage_identifier(column) for column in names)
            await driver.execute(f"GRANT {right}({selected}) ON TABLE {table} TO {builder}")
    work = storage_relation(policy.schema, "provider_directory_rooted_graph_work")
    for callable_signature in _SOURCE_CALLABLES:
        name, arguments = callable_signature.split("(", 1)
        signature = storage_relation(policy.schema, name) + "(" + arguments.replace("@work", work)
        # Source callables, including the definer guards, run as the Source owner; the builder may execute them.
        if await driver.fetchval("SELECT to_regprocedure($1)", signature) is not None:
            # The shared Reference counter keeps its migration actor and mutable-ledger rights.
            if signature != storage_relation(policy.schema, "advance_reference_source_generation") + "()":
                await driver.execute(f"ALTER FUNCTION {signature} OWNER TO {owner}")
            await driver.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {builder}")
    await _retire_cohort_intake_guards(driver, policy)


async def _retire_cohort_intake_guards(driver, policy):
    """The adoption retired the replaced bulk cohort checks; the header guard keeps refusing UPDATE and DELETE."""
    history = storage_relation(policy.schema, cohort.HISTORY)
    header = storage_relation(policy.schema, cohort.HEADER)
    hooks = tuple(cohort._history_hooks(True))
    legacy = await driver.fetchval(
        "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname=$2)", history, hooks[-1]
    )
    if not legacy:
        return
    for name in hooks:
        await driver.execute(f"DROP TRIGGER {storage_identifier(name)} ON {history}")
    await driver.execute(f"ALTER TABLE {history} DROP CONSTRAINT pd_uhc_flex_npi_member_cohort_fkey")
    await driver.execute(f"DROP TRIGGER pd_uhc_flex_npi_cohort_guard ON {header}")
    await driver.execute(
        f"CREATE TRIGGER pd_uhc_flex_npi_cohort_guard BEFORE UPDATE OR DELETE ON {header} FOR EACH ROW "
        f"EXECUTE FUNCTION {storage_relation(policy.schema, cohort._HEADER_GUARD)}()"
    )
    await driver.execute(f"ALTER TABLE {header} ENABLE ALWAYS TRIGGER pd_uhc_flex_npi_cohort_guard")


async def _provision_native_controls(component, *, native_inputs=False, metadata_read_roles=()):
    """Use configured PostgreSQL roles and preserve native NPPES data validation."""
    policy = component.policy
    await _provision_cms_epoch_controls(component)
    async with component.admin_engine.begin() as connection:
        await connection.execute(text("SELECT 1"))
        driver = (await connection.get_raw_connection()).driver_connection
        await _adopt_acquisition_family(driver, policy)
        await adopt_nppes_control_custody(
            driver,
            policy.schema,
            policy,
            migration_role=policy.control_runtime_roles[0],
            namespace_owner_role=policy.owner_role,
        )
        if metadata_read_roles:
            await grant_cms_origin_metadata_read_privileges(driver, policy, metadata_read_roles=metadata_read_roles)
    if native_inputs:
        assert component.factory._native_input_producer is not None
        component.factory._require_current_policy()
    else:
        component.factory.producer_identity = SYNTHETIC_PRODUCER


async def _install_nppes_historical_storage(engine, url, component, migrations):
    historical = create_async_engine(url.set(drivername="postgresql+asyncpg", username=component.builder))
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'GRANT CREATE ON DATABASE "{url.database}" TO "{component.builder}"'))
        await install_source_canonical_functions(historical, "mrf")
        async with historical.connect() as connection:
            await connection.run_sync(_run_migrations, migrations)
        async with engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{component.owner}"'))
            await connection.run_sync(
                _run_migrations,
                (
                    "20260808090000",
                    "20260808100000",
                ),
            )
        await install_nppes_enumeration_storage(engine, component.policy)
        async with engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{component.owner}"'))
            await connection.run_sync(_run_migrations, ("20260808220000", "20260809020000"))
    finally:
        await historical.dispose()
        async with engine.begin() as connection:
            await connection.execute(text(f'REVOKE CREATE ON DATABASE "{url.database}" FROM "{component.builder}"'))


async def _install_nppes_publication(engine):
    from tests.npi_canonical_publication_postgres_support import CANONICAL_TABLES, load_publication_migration

    async with engine.begin() as connection:
        for ordinal, table in enumerate(CANONICAL_TABLES, 1):
            await connection.execute(text(f'CREATE TABLE "mrf"."{table}" (synthetic_id bigint PRIMARY KEY)'))
            await connection.execute(text(f'INSERT INTO "mrf"."{table}" SELECT generate_series(1,{ordinal})'))
    from tests.public_evidence_storage_postgres_support import run_migration_action

    migration = load_publication_migration()
    migration._schema = lambda: "mrf"
    await run_migration_action(engine, migration, "upgrade")
    return migration


@asynccontextmanager
async def existing_nppes_component(component):
    """Borrow the caller's configured Source scope with its actual native data catalog."""
    from process.nppes_public_evidence_catalog import assert_nppes_candidate_catalog

    factory = component.factory
    assert _COMPONENT.get() is None and source._FACTORY.get() is factory
    factory._require_current_policy()
    async with source.source_transaction(component.database):
        await assert_nppes_candidate_catalog(
            source.bound_publisher().connection, component.policy.schema, component.policy
        )
    token = _COMPONENT.set(component)
    try:
        yield component
    finally:
        _COMPONENT.reset(token)


async def _prepare_nppes_component(component, url, publication):
    """Bootstrap real storage and configured DCL before exposing the native component."""
    engine = component.admin_engine
    before_canonical, after_canonical = nppes_source_component_migration_stages()
    await _bootstrap_nppes_parent_component(engine, component.policy, before_canonical)
    await _install_nppes_historical_storage(engine, url, component, after_canonical)
    async with engine.begin() as connection:
        await connection.run_sync(_named_component_generations, component.policy.schema)
    await provision_source_component(engine, component.policy)
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: upgrade_nppes_evidence_candidates(sync, "mrf"))
    await _provision_native_controls(component)
    return await _install_nppes_publication(engine) if publication else None


@asynccontextmanager
async def _ready_nppes_component(engine, url, settings, publication):
    async with native_source_component(engine, "mrf", settings, legacy_builder=True) as component:
        migration = await _prepare_nppes_component(component, url, publication)
        token = _COMPONENT.set(component)
        try:
            yield migration
        finally:
            _COMPONENT.reset(token)


@asynccontextmanager
async def composed_nppes_component(component, *, migration_prefixes=None):
    """Prepare the caller's real model family and configured native control permissions."""
    if migration_prefixes is not None:
        from tests.cms_registry_complete_publication_postgres_support import source_component_native_migrations

        if type(migration_prefixes) is not tuple or migration_prefixes != source_component_native_migrations():
            raise RuntimeError("caller_owned_nppes_migration_profile_required")
    engine, policy = component.admin_engine, component.policy
    assert _COMPONENT.get() is None and source._FACTORY.get() is component.factory
    borrowed_attributes = (
        "producer_identity",
        "_native_input_producer",
        "native_tiger_input_fence",
    )
    prior_value_by_name = {
        name: getattr(component.factory, name) for name in borrowed_attributes if hasattr(component.factory, name)
    }
    try:
        if component.factory._native_input_producer is None:
            component.factory.enroll_ordinary_native_inputs()
        component.factory._require_current_policy()
        if migration_prefixes is None:
            base_prefixes = set(source_component_migrations())
            missing_prefixes = tuple(
                prefix for prefix in nppes_source_component_migrations() if prefix not in base_prefixes
            )
            await _install_nppes_historical_storage(engine, engine.url, component, missing_prefixes)
        async with engine.begin() as connection:
            await connection.run_sync(lambda sync: upgrade_nppes_evidence_candidates(sync, policy.schema))
        if migration_prefixes is None:
            await _install_existing_npi_publication(engine, policy.schema)
        await _provision_native_controls(component, native_inputs=True)
        async with existing_nppes_component(component):
            yield component
    finally:
        for name in borrowed_attributes:
            if name in prior_value_by_name:
                setattr(component.factory, name, prior_value_by_name[name])
            elif hasattr(component.factory, name):
                delattr(component.factory, name)


async def _install_existing_npi_publication(engine, schema):
    """Install the real receipt migration on the six existing canonical model heaps."""
    from tests.npi_canonical_publication_postgres_support import CANONICAL_TABLES, load_publication_migration
    from tests.public_evidence_storage_postgres_support import run_migration_action

    migration = load_publication_migration()
    migration._schema = lambda: schema
    async with engine.begin() as connection:
        for name in CANONICAL_TABLES:
            assert await connection.scalar(text("SELECT to_regclass(:name)"), {"name": f"{schema}.{name}"}) is not None
    await run_migration_action(engine, migration, "upgrade")


@asynccontextmanager
async def nppes_candidate_schema(*, publication=False):
    """Construct the native migration, data-validation and PostgreSQL permission fixture."""
    async with _owned_database(database_url()) as (url, _admin):
        engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
        try:
            with pytest.MonkeyPatch.context() as settings:
                settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
                settings.setenv("DB_SCHEMA", "mrf")
                async with engine.begin() as connection:
                    await connection.execute(text('CREATE SCHEMA "mrf"'))
                async with _ready_nppes_component(engine, url, settings, publication) as migration:
                    yield engine, url, "mrf", migration
        finally:
            await engine.dispose()


@asynccontextmanager
async def nppes_candidate_publication_schema():
    async with nppes_candidate_schema(publication=True) as context:
        yield context
