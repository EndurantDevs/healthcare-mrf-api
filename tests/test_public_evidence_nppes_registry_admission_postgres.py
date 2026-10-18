# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL 18 proof for complete sealed NPPES registry admission."""

from __future__ import annotations

import asyncio
import importlib.util
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from process import nppes_public_evidence_writer as writer
from process import provider_directory_generic_source_policy as source
from process import provider_directory_source_callable_custody as source_callables
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
from process.nppes_public_evidence_rows import SOURCE_RECORD_COLUMNS
from process.nppes_public_evidence_writer import (
    _insert_single,
)
from process.nppes_public_evidence_writer_contract import NppesPublicEvidenceWriterError
from public_evidence.nppes_registry_merkle import NppesEvidenceRootAccumulator
from tests.nppes_candidate_postgres_support import (
    assert_source_callable_census,
    candidate_component,
    capture_source_legacy_callable_custody,
    install_source_canonical_functions,
    loading_family,
    nppes_candidate_schema,
    nppes_source_component_migration_stages,
    nppes_source_component_migrations,
    resolved_family,
)
from tests.nppes_public_evidence_process_support import prepared_sized_archive
from tests.provider_directory_source_postgres_support import source_component_migrations
from tests.public_evidence_nppes_admission_postgres_support import (
    NEW_TABLES,
    admit_chain,
    admit_replay,
    alternate_source_record_values,
    assert_admission_catalog,
    finished_chain_receipt,
    nppes_admission_schema,
    prepared_replay,
    required_config,
)
from tests.public_evidence_storage_postgres_support import (
    connect,
    run_migration_action,
)

EXPECTED_CONSTRAINT_NAMES = {
    "public_evidence_nppes_registry_admission_pkey",
    "public_evidence_nppes_registry_admission_release_key",
    "public_evidence_nppes_registry_admission_owner_key",
    "public_evidence_nppes_registry_admission_chain_owner_key",
    "public_evidence_nppes_registry_admission_release_fkey",
    "public_evidence_nppes_registry_admission_shape_check",
    "public_evidence_nppes_registry_admission_seal_pkey",
    "public_evidence_nppes_registry_admission_seal_parent_fkey",
    "public_evidence_nppes_registry_admission_seal_shape_check",
    "public_evidence_nppes_registry_member_pkey",
    "public_evidence_nppes_registry_member_npi_key",
    "public_evidence_nppes_registry_member_source_key",
    "public_evidence_nppes_registry_member_evidence_key",
    "public_evidence_nppes_registry_member_admission_fkey",
    "public_evidence_nppes_registry_member_source_fkey",
    "public_evidence_nppes_registry_member_evidence_fkey",
    "public_evidence_nppes_registry_member_shape_check",
    "public_evidence_nppes_registry_member_digest_check",
    "public_evidence_nppes_registry_chain_admission_pkey",
    "public_evidence_nppes_registry_chain_admission_owner_key",
    "public_evidence_nppes_registry_chain_listing_key",
    "public_evidence_nppes_registry_chain_shape_check",
    "public_evidence_nppes_registry_chain_admission_seal_pkey",
    "public_evidence_nppes_registry_chain_admission_seal_parent_fkey",
    "public_evidence_nppes_registry_chain_admission_seal_shape_check",
    "public_evidence_nppes_registry_chain_archive_pkey",
    "public_evidence_nppes_registry_chain_archive_admission_key",
    "public_evidence_nppes_registry_chain_archive_release_key",
    "public_evidence_nppes_registry_chain_archive_name_key",
    "public_evidence_nppes_registry_chain_archive_artifact_key",
    "public_evidence_nppes_registry_chain_archive_manifest_key",
    "public_evidence_nppes_registry_chain_archive_parent_fkey",
    "public_evidence_nppes_registry_chain_archive_admission_fkey",
    "public_evidence_nppes_registry_chain_archive_shape_check",
}
ALTERED_TABLES = (
    "public_evidence_source_record",
    "public_evidence_record",
    "public_evidence_record_source_link",
    "public_evidence_npi_enumeration",
)
APPEND_TABLES = (*ALTERED_TABLES, NEW_TABLES[2], NEW_TABLES[5])
FROZEN = {
    "artifact": "18f8d516f2e5968a282a73a82c4f6c1e4a66232e47c05a588f5fcc2769453c68",
    "release": "perel1_thOXFZ-8-p-uFu5YGFWLNDAhZuQ5RYasRRJjpbr7fyQ",
    "release_sha": "686d441a11182c61aa5197d52082f4d3b2e556a03c1d500a24d7c67edeb3ba94",
    "manifest": "497247a67f817db5a014d2a272c150b1c167339d9e3dc025a71607dd1ad0dba7",
    "root": "70edf8081737f9bd597ad4c457d19a47e0cbef39d37854b22d7a13f202ad7385",
    "admission": "penpa1_sZ_9hhZxyiQpWHwx4msF0_g2duVBgQb3bhRPtME91b0",
    "admission_sha": "ea40e980a27fcf94bd8e60773c45d1caf674947c34c7360019c2fc0855f3f1f7",
    "chain": "penpc1_ZQAuLpMS3kgkBDE8ssSqXo0iE6tqPjhiJRRWJMbTzb4",
    "chain_sha": "72605f4d7d53d571253fef93c8be33a36e4e8597cb3905d2bb85b07f4b4d44e5",
}
FROZEN_MEMBERS = (
    (
        "d53a33bc58ed590cee62f4ed16cbefad84aea2a6aaadcfc4545a366aaa61641b",
        "5ec6ad8a264ee27b16afdcb1e98f944c6e142c0f7b0f32275762a6a1c69ca38c",
        "7cad6728795d191454daf4eff28092209cc1b685b152564606a99259bd074034",
        "08d6b58fa9f192df390dacccfe6e96e0bcf0455c948cee9bd49c23224a6eb6af",
    ),
    (
        "49168a7d68de86a7cb9c7ade7d9c370bb62930b2df3c1953dedbdd9da7a03b7d",
        "ece1a38cb404b65e33ab0a19fadea6569e164a0cb3ab8c977875e3e6dbfcb435",
        "712f00eca3391d9378caa5aaaf663a5c10135c8a6a1f9302a538ba3a1e49e8de",
        "e0a5f3899023324862ce0bf0afae0e38c48e36094875460f1a3c0753814ad4af",
    ),
    (
        "1eccbb76a2e7e4d1d213ba7c08557397bbd5bc6960272d617f347b08f34ab067",
        "d219a342162d3f8256db4c225dcdd771d9838555fc8e7ef0001634d6e56ecaec",
        "57e217d2defa8fabbae15b239383ed4a2acf7d8ed2948df0f40f161142fece6e",
        "e273784c81173956e7eb9464b280a87b1d218776362ccfed2f34bdc9718c72b8",
    ),
)


def test_native_fixture_migrations_cover_full_callable_contract_without_changing_legacy_slice():
    original = source_component_migrations()
    prefixes = nppes_source_component_migrations()
    assert set(original) < set(prefixes)
    assert len(prefixes) == len(set(prefixes))
    assert prefixes.index("20260930140000") < prefixes.index("20260929040000") < prefixes.index("20261001100000")
    assert not {"20260820020000", "20260820030000"}.intersection(prefixes)
    assert prefixes.index("20260830090000") < prefixes.index("20260830100000")
    assert prefixes.index("20260909120000") < prefixes.index("20260914100000")
    assert prefixes.index("20260909120000") < prefixes.index("20261016040000")
    assert prefixes.index("20261016040000") < prefixes.index("20261016050000")
    assert source_component_migrations() == original
    for prefix in ("20261016040000", "20261016050000"):
        path = next((Path(__file__).parents[1] / "alembic" / "versions").glob(prefix + "*.py"))
        spec = importlib.util.spec_from_file_location("native_fixture_repair_" + prefix, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for contract in module._CONTRACTS:
            matching_entries = [
                value[-1] for key, value in source_callables._CONTRACTS.items() if key.split("(")[0] == contract[0]
            ]
            assert contract[-1] in matching_entries, contract[0]


def test_native_fixture_retry_and_json_canonical_predecessors_are_authentic():
    from tests.test_source_validator_argument_types_migration import _digest, _load, _parse_function

    retry = _load("20260830090000_uhc_flex_retry_exhaustion.py")
    repair = _load("20261016040000_source_validator_argument_types.py")
    name, attributes = _parse_function(retry._publication_valid_sql("snapshot_schema", partial=True))
    contract = next(entry for entry in repair._CONTRACTS if entry[0] == name)
    assert _digest(attributes[-1]) == contract[-2]
    versions = Path(__file__).parents[1] / "alembic" / "versions"
    paths = list(versions.glob("20260810110000*.py"))
    assert len(paths) == 2
    selected_entries = [path for path in paths if "provider_directory" in path.name]
    assert len(selected_entries) == 1
    actual_upgrade = _load(selected_entries[0].name).upgrade
    assert "_install_receipt_verification_functions" not in actual_upgrade.__code__.co_names
    assert "_install_ordinary_terminal_receipt_guard" not in actual_upgrade.__code__.co_names


def test_native_fixture_parent_migrations_preserve_legacy_writer_grants():
    before, after = nppes_source_component_migration_stages()
    assert (*before, *after) == nppes_source_component_migrations()
    assert after == ("20261016040000", "20261016050000", "20261016060000", "20261019130000")
    for prefix in (
        "20260825090000",
        "20261001110000",
        "20261004000000",
        "20261005100000",
        "20261005120000",
        "20261005130000",
        "20261015015000",
        "20261016010000",
        "20261016030000",
    ):
        assert prefix in before
    sql = Path(__file__).parents[1] / "db" / "sql"
    rooted = (sql / "rooted_graph_set_validation.sql").read_text()
    practitioner = (sql / "practitioner_set_validation.sql").read_text()
    assert "grant_row.privilege_type='INSERT'" in rooted
    assert "GRANT EXECUTE ON FUNCTION __SCHEMA__.prepare_provider_directory_rooted_graph_storage(text)" in rooted
    assert "grant_row.privilege_type='INSERT'" in practitioner
    assert 'GRANT EXECUTE ON FUNCTION "__SCHEMA__"."initialize_pd_uhc_flex_practitioner_work"' in practitioner


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (False, True))
async def test_native_parent_bootstrap_uses_real_builder_and_restores_creation_grant(monkeypatch, failure):
    from sqlalchemy.engine import make_url

    from tests import nppes_candidate_postgres_support as support

    statements, migrations = [], []
    administrator = SimpleNamespace(scalar=AsyncMock(return_value="test_component"))

    async def execute(statement):
        statements.append(str(statement))

    administrator.execute = execute
    historical_connection = SimpleNamespace(scalar=AsyncMock(return_value=None))

    async def run_sync(operation, *arguments):
        if operation is support._run_migrations:
            migrations.append(arguments)
            if failure:
                raise RuntimeError("synthetic migration refusal")

    historical_connection.run_sync = run_sync

    @asynccontextmanager
    async def admin_transaction():
        yield administrator

    @asynccontextmanager
    async def builder_connection():
        yield historical_connection

    historical = SimpleNamespace(begin=builder_connection, connect=builder_connection, dispose=AsyncMock())
    engine = SimpleNamespace(
        url=make_url("postgresql+asyncpg://administrator@localhost/test_component"), begin=admin_transaction
    )

    def create(url):
        assert url.username == "builder" and url.password is None and url.database == "test_component"
        return historical

    monkeypatch.setattr(support, "create_async_engine", create)
    policy = SimpleNamespace(schema="mrf", control_runtime_roles=("builder",))
    prefixes = ("20260610120000",)
    if failure:
        with pytest.raises(RuntimeError, match="synthetic migration refusal"):
            await support._bootstrap_nppes_parent_component(engine, policy, prefixes)
    else:
        await support._bootstrap_nppes_parent_component(engine, policy, prefixes)
    assert migrations == [(prefixes,)]
    assert statements == [
        'GRANT CREATE ON DATABASE "test_component" TO "builder"',
        'REVOKE CREATE ON DATABASE "test_component" FROM "builder"',
    ]
    historical.dispose.assert_awaited_once()


def _parent_transaction_fixture(driver, transactions):
    transaction_by_key = dict(materialized=False)

    async def execute(statement):
        if str(statement) == "SELECT current_user=session_user":
            transaction_by_key["materialized"] = True

    async def raw_connection():
        assert transaction_by_key["materialized"], "original native transaction must exist before custody capture"
        return SimpleNamespace(driver_connection=driver)

    connection = SimpleNamespace(
        execute=AsyncMock(side_effect=execute), get_raw_connection=AsyncMock(side_effect=raw_connection)
    )

    @asynccontextmanager
    async def begin():
        transaction_by_key["materialized"] = False
        state_by_key = dict(committed=False, rolled_back=False)
        transactions.append(state_by_key)
        try:
            yield connection
        except BaseException:
            state_by_key["rolled_back"] = True
            raise
        else:
            state_by_key["committed"] = True

    return connection, begin


_PARENT_CUSTODY_EVENTS_BY_FUNCTION = {
    "_provision_cms_epoch_controls": "epoch",
    "provision_cms_callable_custody": "cms_callable",
    "provision_cms_serving_receipt_custody": "cms_receipt",
    "provision_initial_callable_custody": "initial",
    "provision_source_namespace_custody": "namespace_provision",
    "capture_source_namespace_custody": "namespace_capture",
    "assert_source_callable_census": "census",
    "capture_source_legacy_callable_custody": "legacy_source",
    "lock_source_control_family": "parent_lock",
    "adopt_cms_control_custody": "parent_adopt",
    "adopt_nppes_control_custody": "nppes_adopt",
    "attest_nppes_control_custody": "nppes_attest",
}


def _install_parent_custody_mocks(monkeypatch, support, failure, parent_identity, before, after, checks, events):
    driver, policy = parent_identity

    async def invoke(name, *arguments, **keywords):
        events.append(name)
        if name == failure:
            raise RuntimeError("synthetic custody refusal")
        if name == "parent_before":
            assert keywords == dict(legacy=True, migration_role="migrator", namespace_owner_role="namespace")
            return before
        if name == "parent_adopt":
            assert arguments == (driver, policy, before)
            return checks
        if name == "parent_after":
            assert keywords == dict(migration_role="migrator", namespace_owner_role="namespace")
            return after

    for function, event in _PARENT_CUSTODY_EVENTS_BY_FUNCTION.items():

        async def recorded(*arguments, _event=event, **keywords):
            return await invoke(_event, *arguments, **keywords)

        monkeypatch.setattr(support, function, recorded)

    async def capture(*arguments, **keywords):
        return await invoke("parent_before" if keywords.get("legacy") else "parent_after", *arguments, **keywords)

    def delta(*arguments):
        assert arguments == (before, after, checks)
        events.append("parent_delta")
        if failure == "parent_delta":
            raise RuntimeError("synthetic custody refusal")

    monkeypatch.setattr(support, "capture_cms_control_custody", capture)
    monkeypatch.setattr(support, "require_cms_control_adoption_delta", delta)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "cms_receipt", "parent_adopt", "parent_delta", "nppes_attest"))
async def test_native_parent_custody_preserves_preimages_order_and_failure_barrier(monkeypatch, failure):
    from tests import nppes_candidate_postgres_support as support

    events, transactions = [], []
    driver, before, after, checks = object(), object(), object(), object()
    policy = source.GenericSourcePolicy("snapshot_schema", "owner", "publisher", ("reader", "builder"), ("builder",))
    connection, begin = _parent_transaction_fixture(driver, transactions)

    _install_parent_custody_mocks(monkeypatch, support, failure, (driver, policy), before, after, checks, events)
    component = SimpleNamespace(
        policy=policy,
        admin_engine=SimpleNamespace(begin=begin),
        retained_publisher="retained",
        factory=SimpleNamespace(),
    )
    expected_entries = [
        "epoch",
        "cms_callable",
        "cms_receipt",
        "initial",
        "namespace_provision",
        "namespace_capture",
        "census",
        "legacy_source",
        "parent_lock",
        "parent_before",
        "parent_adopt",
        "parent_after",
        "parent_delta",
        "nppes_adopt",
        "nppes_attest",
    ]
    if failure:
        with pytest.raises(RuntimeError, match="synthetic custody refusal"):
            await support._enroll_native_custody(component, "namespace", "migrator")
        assert events == expected_entries[: expected_entries.index(failure) + 1]
        assert transactions[-1]["rolled_back"] and not vars(component.factory)
    else:
        await support._enroll_native_custody(component, "namespace", "migrator")
        assert events == expected_entries and all(state["committed"] for state in transactions)
        assert component.factory.producer_identity == support.SYNTHETIC_PRODUCER
    assert not any("ALTER TABLE" in str(call.args[0]) for call in connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (False, True))
async def test_native_npi_pure_helper_uses_original_sql_and_builder_before_custody(monkeypatch, failure):
    from tests import nppes_candidate_postgres_support as support

    statements = []
    policy = SimpleNamespace(schema="snapshot_schema", owner_role="owner", control_runtime_roles=("builder",))

    async def run(engine, module, action):
        def execute(statement):
            statements.append(statement)
            if failure and 'CREATE FUNCTION "snapshot_schema"."public_evidence_npi_valid"(' in statement:
                raise RuntimeError("synthetic helper creation refusal")

        module.op = SimpleNamespace(execute=execute)
        module._schema = lambda: "snapshot_schema"
        module._create_tables = module._create_validator = module._install_guards = lambda schema: None
        try:
            getattr(module, action)()
        finally:
            assert module.op.execute is execute

    monkeypatch.setattr(support, "run_migration", run)
    if failure:
        with pytest.raises(RuntimeError, match="synthetic helper creation refusal"):
            await support.install_nppes_enumeration_storage(object(), policy)
    else:
        await support.install_nppes_enumeration_storage(object(), policy)
    creates = [statement for statement in statements if statement.lstrip().startswith("CREATE FUNCTION")]
    assert len(creates) == 3
    original = support.load_migration(
        Path(support.__file__).parents[1]
        / "alembic"
        / "versions"
        / "20260808170000_public_evidence_npi_enumeration_storage.py",
        "original_enumeration_helpers",
    )
    expected_entries = []
    original.op = SimpleNamespace(execute=expected_entries.append)
    original._create_digest_helpers("snapshot_schema")
    assert creates == expected_entries
    body = creates[-1].split("$function$")[1]
    assert (
        source_callables._body_digest(body, "snapshot_schema")
        == source_callables._CONTRACTS["public_evidence_npi_valid(text)"][-1]
    )
    index = statements.index(creates[-1])
    assert statements[index - 1] == 'SET LOCAL ROLE "builder"'
    assert statements[index + 1] == 'SET LOCAL ROLE "owner"'
    assert statements[0] == 'SET LOCAL ROLE "owner"'
    if not failure:
        revoke = next(
            statement
            for statement in statements
            if statement.startswith('REVOKE ALL ON FUNCTION "snapshot_schema"."public_evidence_npi_valid"(')
        )
        index = statements.index(revoke)
        assert statements[index - 1] == 'SET LOCAL ROLE "builder"'
        assert statements[index + 1] == 'SET LOCAL ROLE "owner"'


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "acl", "duplicate_acl", "other"))
async def test_native_legacy_acl_diagnostic_retains_proof_and_identifies_exact_refusal(monkeypatch, failure):
    from process.provider_directory_generic_source_policy import GenericSourcePolicy
    from tests import nppes_candidate_postgres_support as support

    policy = GenericSourcePolicy("snapshot_schema", "owner", "publisher", ("reader", "builder"), ("builder",))
    arguments_by_key = dict(migration_role="migrator", namespace_owner_role="namespace_owner")
    proof = object()
    original = ValueError("source_callable_acl_changed" if failure != "other" else "source_callable_definition_changed")
    capture = AsyncMock(return_value=proof, side_effect=original if failure else None)
    monkeypatch.setattr(support, "capture_source_callable_custody", capture)
    signature = '"snapshot_schema"."prepare_provider_directory_rooted_graph_storage"(text)'
    acl_entries = [[12, 12, "EXECUTE", False]]
    acl_entries.append([11, 12, "EXECUTE", False] if failure == "acl" else acl_entries[0])
    roles = [dict(rolname=name, oid=oid) for name, oid in (("owner", 11), ("builder", 12), ("namespace_owner", 13))]
    catalog_rows = [
        dict(
            signature=signature,
            definition=dict(proname="prepare_provider_directory_rooted_graph_storage", proowner=12),
            acl=acl_entries,
        )
    ]
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=(roles, catalog_rows)))
    if failure is None:
        assert await capture_source_legacy_callable_custody(connection, policy, **arguments_by_key) is proof
        connection.fetch.assert_not_awaited()
    elif failure == "other":
        with pytest.raises(ValueError) as refused:
            await capture_source_legacy_callable_custody(connection, policy, **arguments_by_key)
        assert refused.value is original
        connection.fetch.assert_not_awaited()
    else:
        with pytest.raises(ValueError, match="native_source_legacy_acl_refused") as refused:
            await capture_source_legacy_callable_custody(connection, policy, **arguments_by_key)
        assert "prepare_provider_directory_rooted_graph_storage" in str(refused.value)
        assert "source_callable_acl_changed" in str(refused.value)
        assert refused.value.__cause__ is original
        assert connection.fetch.await_count == 2
        assert connection.fetch.await_args_list[1].args[1] == list(
            source_callables.source_callable_signatures(policy.schema)
        )
    capture.assert_awaited_once_with(connection, policy, legacy=True, **arguments_by_key)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "precondition", "installer"))
async def test_native_canonical_fragment_plan_preserves_authentic_bodies_and_failures(monkeypatch, failure):
    from tests import nppes_candidate_postgres_support as support
    from tests.test_acquisition_validator_argument_types_migration import _parse_functions

    statements, actions = [], []
    connection = SimpleNamespace(scalar=AsyncMock(return_value=failure != "precondition"))

    @asynccontextmanager
    async def connect():
        yield connection

    async def run_migration(engine, module, action):
        actions.append((module.revision, action))
        module.op = SimpleNamespace(execute=statements.append)
        getattr(module, action)()
        if failure == "installer":
            raise RuntimeError("authentic canonical installation failed")

    monkeypatch.setattr(support, "run_migration", run_migration)
    engine = SimpleNamespace(connect=connect)
    if failure:
        with pytest.raises(
            AssertionError if failure == "precondition" else RuntimeError,
            match="untouched component slice" if failure == "precondition" else "canonical installation failed",
        ):
            await install_source_canonical_functions(engine, "snapshot_schema")
        assert len(actions) == (0 if failure == "precondition" else 1)
        return
    await install_source_canonical_functions(engine, "snapshot_schema")
    assert actions == [
        ("20260810110000_ptg_wave_receipt_authority", "install"),
        ("20260820030000_ptg_ordinary_terminal_json_canonical_digest", "install"),
    ]
    assert len(statements) == 6 and all(statement.lstrip().startswith("CREATE FUNCTION") for statement in statements)
    assert all("ptg_wave_ordinary_terminal_receipt_guard" not in statement for statement in statements)
    parsed_by_key = {}
    for statement in statements:
        parsed_by_key.update(_parse_functions(statement))
    assert {f"{name}({kinds})" for name, kinds in parsed_by_key} == {
        "ptg_wave_canonical_json_ascii_v1(json)",
        "ptg_wave_canonical_json_ascii_v1(jsonb)",
        "ptg_wave_json_ascii_text_v1(text)",
    }
    for (name, kinds), attributes in parsed_by_key.items():
        assert (
            source_callables._body_digest(attributes[-1], "snapshot_schema")
            == source_callables._CONTRACTS[f"{name}({kinds})"][-1]
        )
    parameters = connection.scalar.await_args.args[1]
    assert len(parameters["signatures"]) == 7
    assert '"snapshot_schema".ptg_wave_ordinary_terminal_receipt_guard()' in parameters["signatures"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "missing", "unexpected"))
async def test_native_callable_census_explains_missing_and_extra_overloads(change):
    policy = SimpleNamespace(schema="native_fixture")
    signatures = source_callables.source_callable_signatures(policy.schema)
    expected_entries = [dict(signature=signature, oid=index) for index, signature in enumerate(signatures, 100)]
    actual_entries = [dict(oid=record["oid"], name=record["signature"], arguments="") for record in expected_entries]
    if change == "missing":
        actual_entries.pop()
        expected_entries[-1]["oid"] = None
    elif change == "unexpected":
        actual_entries.append(dict(oid=9999, name="unexpected_overload", arguments="text"))
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=(expected_entries, actual_entries)))
    if change is None:
        await assert_source_callable_census(connection, policy)
    else:
        with pytest.raises(
            AssertionError, match="missing_signatures" if change == "missing" else "unexpected_overload"
        ):
            await assert_source_callable_census(connection, policy)
    assert connection.fetch.await_count == 2


async def _assert_constraints_and_triggers(connection: asyncpg.Connection, schema: str) -> None:
    """Require exact immediate constraints and enabled-always trigger guards."""

    constraint_rows = await connection.fetch(
        "SELECT relation.relname, constraint_record.conname, "
        "constraint_record.contype::text, constraint_record.condeferrable, "
        "constraint_record.condeferred, constraint_record.convalidated, "
        "constraint_record.conenforced FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid=constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
        "AND constraint_record.contype NOT IN ('n','t')",
        schema,
        list(NEW_TABLES),
    )
    assert {constraint_row["conname"] for constraint_row in constraint_rows} == EXPECTED_CONSTRAINT_NAMES
    assert all(constraint_row["convalidated"] and constraint_row["conenforced"] for constraint_row in constraint_rows)
    assert all(
        not constraint_row["condeferrable"] and not constraint_row["condeferred"]
        for constraint_row in constraint_rows
        if constraint_row["contype"] == "f"
    )
    rewired_names = {
        "public_evidence_source_record_admission_fkey",
        "public_evidence_record_admission_fkey",
        "public_evidence_record_source_link_record_fkey",
        "public_evidence_record_source_link_source_fkey",
        "public_evidence_npi_enumeration_record_fkey",
    }
    rewired_rows = await connection.fetch(
        "SELECT constraint_record.conname, constraint_record.condeferrable, "
        "constraint_record.condeferred, constraint_record.convalidated, "
        "constraint_record.conenforced FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid=constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND constraint_record.conname=ANY($2::text[])",
        schema,
        list(rewired_names),
    )
    assert {rewired_constraint_row["conname"] for rewired_constraint_row in rewired_rows} == rewired_names
    assert all(
        not rewired_constraint_row["condeferrable"]
        and not rewired_constraint_row["condeferred"]
        and rewired_constraint_row["convalidated"]
        and rewired_constraint_row["conenforced"]
        for rewired_constraint_row in rewired_rows
    )
    await _assert_triggers(connection, schema)


async def _assert_triggers(connection: asyncpg.Connection, schema: str) -> None:
    """Require exact enabled-always integrity, append, and immutable guards."""

    trigger_rows = await connection.fetch(
        "SELECT relation.relname, trigger_record.tgname, trigger_record.tgtype::integer, "
        "trigger_record.tgenabled::text, trigger_record.tgdeferrable, "
        "trigger_record.tginitdeferred, procedure_namespace.nspname AS function_schema, "
        "procedure.proname AS function_name, procedure.prosecdef AS security_definer, "
        "procedure.proconfig = ARRAY['search_path=pg_catalog']::text[] "
        "AS exact_search_path "
        "FROM pg_trigger AS trigger_record "
        "JOIN pg_class AS relation ON relation.oid=trigger_record.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "JOIN pg_proc AS procedure ON procedure.oid=trigger_record.tgfoid "
        "JOIN pg_namespace AS procedure_namespace "
        "ON procedure_namespace.oid=procedure.pronamespace "
        "WHERE namespace.nspname=$1 AND NOT trigger_record.tgisinternal",
        schema,
    )
    task_triggers = [
        trigger_row
        for trigger_row in trigger_rows
        if trigger_row["relname"] in NEW_TABLES or trigger_row["tgname"].endswith("_admission_append_guard")
    ]
    assert all(trigger_row["tgenabled"] == "A" for trigger_row in task_triggers)
    assert all(trigger_row["function_schema"] == schema for trigger_row in task_triggers)
    integrity_rows = [trigger_row for trigger_row in task_triggers if "integrity_guard" in trigger_row["tgname"]]
    _assert_integrity_triggers(integrity_rows)
    append_rows = [
        trigger_row
        for trigger_row in task_triggers
        if trigger_row["relname"] in APPEND_TABLES and trigger_row["tgtype"] == 4
    ]
    assert {trigger_row["relname"] for trigger_row in append_rows} == set(APPEND_TABLES)
    assert all(trigger_row["tgtype"] == 4 and not trigger_row["tgdeferrable"] for trigger_row in append_rows)
    assert not any(
        trigger_row["tgname"] in {f"{table}_integrity_guard" for table in ALTERED_TABLES[1:]}
        for trigger_row in trigger_rows
    )


def _assert_integrity_triggers(integrity_rows: list[asyncpg.Record]) -> None:
    assert len(integrity_rows) == 2
    assert all(
        trigger_row["tgtype"] == 5
        and trigger_row["tgdeferrable"]
        and trigger_row["tginitdeferred"]
        and trigger_row["security_definer"]
        and trigger_row["exact_search_path"]
        for trigger_row in integrity_rows
    )
    assert {trigger_row["relname"]: trigger_row["function_name"] for trigger_row in integrity_rows} == {
        NEW_TABLES[0]: ("validate_public_evidence_nppes_registry_admission_lifecycle_v2"),
        NEW_TABLES[3]: "validate_public_evidence_nppes_chain_admission",
    }


async def _assert_private(connection: asyncpg.Connection, schema: str) -> None:
    for table in NEW_TABLES:
        for privilege in (
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "REFERENCES",
            "TRIGGER",
            "MAINTAIN",
        ):
            assert not await connection.fetchval(
                "SELECT has_table_privilege('public', $1, $2)",
                f"{schema}.{table}",
                privilege,
            )
    public_helpers = await connection.fetchval(
        "SELECT count(*) FROM pg_proc AS procedure "
        "JOIN pg_namespace AS namespace ON namespace.oid=procedure.pronamespace "
        "WHERE namespace.nspname=$1 "
        "AND (procedure.proname LIKE 'public_evidence_nppes_%' "
        "OR procedure.proname='nppes_registry_payload_digest' "
        "OR procedure.proname LIKE 'validate_public_evidence_nppes_%' "
        "OR procedure.proname LIKE 'guard_%_admission_append') "
        "AND has_function_privilege('public', procedure.oid, 'EXECUTE')",
        schema,
    )
    assert public_helpers == 0
    for table in APPEND_TABLES[:-1]:
        assert await connection.fetchval(
            "SELECT has_table_privilege(current_user, $1, 'MAINTAIN')",
            f"{schema}.{table}",
        )


@pytest.mark.asyncio
async def test_catalog_is_exact_private_immediate_and_statement_sealed() -> None:
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            await assert_admission_catalog(connection, schema, ALTERED_TABLES)
            await _assert_constraints_and_triggers(connection, schema)
            await _assert_private(connection, schema)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_frozen_archive_and_chain_insert_idempotently(
    tmp_path: Path,
) -> None:
    replay = await prepared_replay(tmp_path)
    assert replay.admission_row.artifact_sha256 == FROZEN["artifact"]
    assert replay.manifest.release.source_release_ref == FROZEN["release"]
    assert replay.manifest.release.contract_sha256 == FROZEN["release_sha"]
    assert replay.manifest.manifest_sha256 == FROZEN["manifest"]
    assert replay.manifest.evidence_root_sha256 == FROZEN["root"]
    assert replay.admission_row.admission_ref == FROZEN["admission"]
    assert replay.admission_row.contract_sha256 == FROZEN["admission_sha"]
    async with nppes_candidate_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            async with source.source_transaction(candidate_component(schema).database):
                publisher = source.bound_publisher().connection
                await publisher.execute("SET LOCAL TIME ZONE 'Pacific/Kiritimati'")
                await publisher.execute("SET LOCAL DateStyle='SQL, DMY'")
                assert (await admit_replay(connection, schema, replay)).write_state == "inserted"
            factory = candidate_component(schema).factory
            factory.readonly = True
            try:
                admitted = await admit_replay(connection, schema, replay)
            finally:
                factory.readonly = False
            assert admitted.write_state == "already_present"
            chain = finished_chain_receipt(replay, admitted)
            assert chain.chain_ref == FROZEN["chain"]
            assert chain.contract_sha256 == FROZEN["chain_sha"]
            await admit_chain(connection, schema, chain)
            await admit_chain(connection, schema, chain)
            family = await resolved_family(schema, replay.admission_row.admission_ref)
            member_name = family["member"]["name"]
            member_rows = await connection.fetch(
                f"SELECT encode(payload_sha256,'hex') AS payload, "
                "encode(record_hmac_sha256,'hex') AS hmac, "
                "encode(leaf_sha256,'hex') AS leaf, encode(row_sha256,'hex') AS row_sha "
                f'FROM "{schema}"."{member_name}" '
                "ORDER BY source_row_ordinal"
            )
            assert tuple(tuple(member_row.values()) for member_row in member_rows) == FROZEN_MEMBERS
            merkle = NppesEvidenceRootAccumulator()
            for member_row in member_rows:
                merkle.add(member_row["leaf"])
            assert merkle.finish() == FROZEN["root"]
            for template, *_rest in writer.storage.ROOTS.values():
                assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{template}"') == 0
            counts = await connection.fetchrow(
                f'SELECT (SELECT count(*) FROM "{schema}".'
                '"public_evidence_nppes_registry_admission_seal") AS archive_seals, '
                f'(SELECT count(*) FROM "{schema}".'
                '"public_evidence_nppes_registry_chain_admission_seal") AS chain_seals'
            )
            assert tuple(counts.values()) == (1, 1)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_cross_family_tamper_never_publishes(tmp_path: Path, monkeypatch) -> None:
    replay = await prepared_replay(tmp_path)
    async with nppes_candidate_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            prepare = writer.storage.prepare_nppes_evidence_publication

            async def corrupt(database, candidate, prepared, **kwargs):
                tables = await loading_family(connection, schema, replay.admission_row.admission_ref)
                await connection.execute(f'DELETE FROM "{schema}"."{tables["typed"]}"')
                return await prepare(database, candidate, prepared, **kwargs)

            monkeypatch.setattr(writer.storage, "prepare_nppes_evidence_publication", corrupt)
            with pytest.raises(NppesPublicEvidenceWriterError):
                await admit_replay(connection, schema, replay)
            for table in ("public_evidence_source_release", "public_evidence_source_record", *NEW_TABLES):
                assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == 0
        finally:
            await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (ImportCancelledError, asyncio.CancelledError))
async def test_committed_partial_candidate_resumes_after_cancellation(tmp_path, monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", "100")
    archive = prepared_sized_archive(tmp_path, "NPPES_Data_Dissemination_July_2026_V2.zip", "20260712", 257)
    replay = await prepare_nppes_registry_replay(archive, required_config())
    async with nppes_candidate_schema() as (_engine, url, schema, _migration):
        component = candidate_component(schema)

        def cancel_after_batch(count):
            assert count == 100
            raise failure()

        with pytest.raises(failure):
            await writer.admit_nppes_registry_archive(
                replay, required_config(), schema=schema, database=component.database, progress=cancel_after_batch
            )
        connection = await connect(url)
        try:
            tables = await loading_family(connection, schema, replay.admission_row.admission_ref)
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{tables["member"]}"') == 100
            assert (
                await connection.fetchval(f'SELECT count(*) FROM "{schema}"."public_evidence_nppes_registry_admission"')
                == 0
            )
            assert (await admit_replay(connection, schema, replay)).write_state == "inserted"
            family = await resolved_family(schema, replay.admission_row.admission_ref)
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{family["member"]["name"]}"') == 257
            assert (await admit_replay(connection, schema, replay)).write_state == "already_present"
        finally:
            await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (ImportCancelledError, asyncio.CancelledError))
async def test_prepared_chunk_drop_cancellation_rolls_back_and_resumes(tmp_path, monkeypatch, failure):
    replay = await prepared_replay(tmp_path)
    storage, observed = writer.storage, {}
    verify = storage._verify

    async def cancel_after_drop(connection, policy, binding):
        tables = await verify(connection, policy, binding)
        if binding["phase"] == "prepared":
            observed["binding"] = binding
            observed["relation"] = f'"{policy.schema}"."{binding["chunks"]["table"]["name"]}"'
            # Publisher and administrator connections use different JSONB codecs.
            columns = ",".join(
                '"counts_json"::text AS "counts_json"' if column == "counts_json" else f'"{column}"'
                for column in storage._CHUNK_COLUMNS
            )
            observed["query"] = f"SELECT {columns} FROM {observed['relation']} ORDER BY chunk_ordinal"
            observed["rows"] = await connection.fetch(observed["query"])
        elif binding["phase"] == "published":
            assert binding["chunks"]["table"] is None
            assert await connection.fetchval("SELECT to_regclass($1)", observed["relation"]) is None
            observed["dropped"] = True
            raise failure()
        return tables

    async with nppes_candidate_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            header_sql = f'SELECT candidate_storage_json FROM "{schema}"."{storage.HEADER}" WHERE admission_ref=$1'
            controls = ("public_evidence_source_identity", "public_evidence_source_release", *NEW_TABLES[:2])
            monkeypatch.setattr(storage, "_verify", cancel_after_drop)
            with pytest.raises(failure):
                await admit_replay(connection, schema, replay)
            assert observed["dropped"] and observed["rows"]
            header = await connection.fetchval(header_sql, replay.admission_row.admission_ref)
            assert (json.loads(header) if isinstance(header, str) else header) == observed["binding"]
            assert observed["binding"]["phase"] == "prepared"
            assert (
                await connection.fetchval("SELECT to_regclass($1)::oid", observed["relation"])
                == (observed["binding"]["chunks"]["table"]["oid"])
            )
            assert await connection.fetch(observed["query"]) == observed["rows"]
            for table in controls:
                assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == 0
            monkeypatch.setattr(storage, "_verify", verify)
            assert (await admit_replay(connection, schema, replay)).write_state == "inserted"
            assert await connection.fetchval("SELECT to_regclass($1)", observed["relation"]) is None
            family = await resolved_family(schema, replay.admission_row.admission_ref)
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{family["member"]["name"]}"') == (
                replay.manifest.source_record_count
            )
            assert (await admit_replay(connection, schema, replay)).write_state == "already_present"
            for table in controls:
                assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == 1
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_seal_rejects_same_transaction_and_later_append(
    tmp_path: Path,
) -> None:
    replay = await prepared_replay(tmp_path)
    async with nppes_candidate_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            with pytest.raises(
                asyncpg.CheckViolationError,
                match="public_evidence_nppes_append_outside_admission",
            ):
                async with source.source_transaction(candidate_component(schema).database):
                    await admit_replay(connection, schema, replay)
                    await _insert_single(
                        source.bound_publisher().connection,
                        schema,
                        "public_evidence_source_record",
                        SOURCE_RECORD_COLUMNS,
                        alternate_source_record_values(replay),
                    )
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."public_evidence_source_record"') == 0
            await admit_replay(connection, schema, replay)
            with pytest.raises(
                asyncpg.CheckViolationError,
                match="public_evidence_nppes_append_outside_admission",
            ):
                async with connection.transaction():
                    await connection.execute("SET LOCAL session_replication_role='replica'")
                    await _insert_single(
                        connection,
                        schema,
                        "public_evidence_source_record",
                        SOURCE_RECORD_COLUMNS,
                        alternate_source_record_values(replay),
                    )
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."public_evidence_source_record"') == 0
            family = await resolved_family(schema, replay.admission_row.admission_ref)
            assert (
                await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{family["source_record"]["name"]}"')
                == replay.manifest.source_record_count
            )
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_downgrade_is_empty_only_and_restores_legacy_contract(
    tmp_path: Path,
) -> None:
    replay = await prepared_replay(tmp_path)
    async with nppes_candidate_schema() as (engine, url, schema, _candidate_migration):
        from tests.public_evidence_nppes_admission_postgres_support import load_lifecycle_migration

        migration = load_lifecycle_migration()
        migration._schema = lambda: schema
        connection = await connect(url)
        try:
            await admit_replay(connection, schema, replay)
            with pytest.raises(
                DBAPIError,
                match="nppes_lifecycle_downgrade_requires_empty_admission",
            ):
                await run_migration_action(engine, migration, "downgrade")
            assert (
                await connection.fetchval(
                    f'SELECT count(*) FROM "{schema}"."public_evidence_nppes_registry_admission_seal"'
                )
                == 1
            )
        finally:
            await connection.close()

    async with nppes_admission_schema() as (engine, url, schema, migration):
        await run_migration_action(engine, migration, "downgrade")
        connection = await connect(url)
        try:
            for table in NEW_TABLES:
                assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{table}") is None
            restored_trigger_rows = await connection.fetch(
                "SELECT trigger_record.tgname, trigger_record.tgenabled::text, "
                "trigger_record.tgdeferrable, trigger_record.tginitdeferred "
                "FROM pg_trigger AS trigger_record JOIN pg_class AS relation "
                "ON relation.oid=trigger_record.tgrelid JOIN pg_namespace AS namespace "
                "ON namespace.oid=relation.relnamespace WHERE namespace.nspname=$1 "
                "AND trigger_record.tgname=ANY($2::text[])",
                schema,
                [f"{table}_integrity_guard" for table in ALTERED_TABLES[1:]],
            )
            assert len(restored_trigger_rows) == 3
            assert all(
                restored_trigger_row["tgenabled"] == "A"
                and restored_trigger_row["tgdeferrable"]
                and restored_trigger_row["tginitdeferred"]
                for restored_trigger_row in restored_trigger_rows
            )
        finally:
            await connection.close()
        await run_migration_action(engine, migration, "upgrade")
