# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record bootstrap actor boundaries; these host doubles prove no database authority."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests import cms_doctors_component_postgres_support as doctors
from tests import cms_registry_complete_publication_postgres_support as fixture
from tests import geo_source_postgres_support as geo


@asynccontextmanager
async def _context(value):
    yield value


async def test_fixed_native_families_are_provisioned_then_published_on_original_source(monkeypatch):
    events = []
    connection = SimpleNamespace(run_sync=AsyncMock(), execute=AsyncMock(), scalar=AsyncMock(side_effect=[71, True]))
    engine = SimpleNamespace(begin=lambda: _context(connection))
    policy = SimpleNamespace(schema="mrf")
    component = SimpleNamespace(
        admin_engine=engine, policy=policy, owner="source_owner", factory=object(), database=object()
    )
    database = SimpleNamespace(engine=engine)
    session = object()
    expected_authorities = [SimpleNamespace(local_generation=0, serving_generation=None) for _ in range(2)]

    async def provision(actual, actual_policy, *verifiers):
        assert actual is engine and actual_policy is policy
        events.append("provision")

    @asynccontextmanager
    async def bind(actual):
        assert actual is component.factory
        events.append("factory")
        yield
        events.append("factory_exit")

    @asynccontextmanager
    async def transaction(actual):
        assert actual is component.database
        events.append("transaction")
        yield session
        events.append("commit")

    async def publish(actual, *, schema_name, expected_authority):
        assert actual is session and schema_name == "mrf" and expected_authority in expected_authorities
        events.append("publish")

    monkeypatch.setattr(doctors, "_provision_component", provision)
    monkeypatch.setattr(geo, "provision_geo", provision)
    monkeypatch.setattr(fixture.source_policy, "bind_generic_source_transaction_factory", bind)
    monkeypatch.setattr(fixture.source_policy, "source_transaction", transaction)
    authority = AsyncMock(side_effect=expected_authorities)
    monkeypatch.setattr(fixture.reference, "read_reference_family_result_generation_authority", authority)
    monkeypatch.setattr(fixture.immutable_doctors, "publish_doctors_generation", publish)
    monkeypatch.setattr(fixture.immutable_geo, "publish_geo_generation", publish)
    await fixture._publish_fixed_native_families(database, component)
    assert events == [
        "provision",
        "provision",
        "factory",
        "transaction",
        "publish",
        "publish",
        "commit",
        "factory_exit",
    ]
    assert [call.kwargs["importer_id"] for call in authority.await_args_list] == ["cms-doctors", "geo"]
    assert all(call.args[0] is session and call.kwargs["lock"] for call in authority.await_args_list)
    assert len(connection.scalar.await_args_list) == 2
    assert 'OWNER TO "source_owner"' in str(connection.execute.await_args.args[0])


async def test_legacy_generation_api_is_used_only_for_unmigrated_families(monkeypatch, tmp_path):
    session = SimpleNamespace(execute=AsyncMock())
    session.begin = lambda: _context(None)

    class Session:
        execute = session.execute

        async def __aenter__(self):
            return self

        async def __aexit__(self, *error):
            return False

        def begin(self):
            return _context(None)

    engine = object()
    database = SimpleNamespace(engine=engine, session_factory=Session)
    component = SimpleNamespace(admin_engine=engine, policy=SimpleNamespace(owner_role="source_owner"))
    create, seed, fixed = AsyncMock(), AsyncMock(), AsyncMock()
    legacy = AsyncMock()
    monkeypatch.setattr(fixture, "_create_native_relations", create)
    monkeypatch.setattr(fixture, "_seed_native_doctors", seed)
    monkeypatch.setattr(fixture, "_publish_fixed_native_families", fixed)
    monkeypatch.setattr(fixture.npi, "install_npi_result_revision_guards", AsyncMock())
    monkeypatch.setattr(fixture.npi, "bootstrap_npi_result_generation", AsyncMock())
    monkeypatch.setattr(fixture.reference, "publish_local_reference_family_generation", legacy)
    register = AsyncMock()
    monkeypatch.setattr(fixture.inputs, "register_native_address_inputs", register)
    await fixture._initialize_native_inputs(database, monkeypatch, tmp_path, "a" * 40, component=component)
    create.assert_awaited_once_with(database, owner_role=component.policy.owner_role)
    fixed.assert_awaited_once_with(database, component)
    assert [call.kwargs["importer_id"] for call in legacy.await_args_list] == [
        "facility-anchors",
        "mrf-address",
        "tiger",
    ]
    register.assert_not_awaited()


async def test_selection_fixture_uses_explicit_admin_and_native_upsert(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())

    class Session:
        execute = session.execute

        async def __aenter__(self):
            return self

        async def __aexit__(self, *error):
            return False

        def begin(self):
            return _context(None)

    engine = object()
    database = SimpleNamespace(
        engine=engine, _source_component=SimpleNamespace(admin_engine=object()), session_factory=Session
    )
    model = fixture.source.fhir.ProviderDirectoryAPIEndpoint
    fields_by_name = {"endpoint_id": "endpoint-fixture", "canonical_api_base": "https://example.test/fhir"}
    try:
        await fixture._seed_selection_row(database, model, fields_by_name)
    except AssertionError as error:
        assert not error.args
    else:
        raise AssertionError("a substituted administrator engine was admitted")
    session.execute.assert_not_awaited()
    database._source_component.admin_engine = engine
    await fixture._seed_selection_row(database, model, fields_by_name)
    statement = session.execute.await_args.args[0]
    assert statement.table is model.__table__
    assert "ON CONFLICT (endpoint_id) DO UPDATE" in str(statement)


@pytest.mark.parametrize("generation", [1, 2, 3])
@pytest.mark.parametrize("same_candidate", [False, True])
async def test_successor_selection_attests_freshly_without_rewriting_fixed_registry(
    monkeypatch, generation, same_candidate
):
    session, request, attestation = object(), object(), object()
    database = SimpleNamespace(session_factory=lambda: _context(session))
    incumbent_by_field = {
        "source_id": "cms-npd",
        "endpoint_id": "synthetic-endpoint",
        "dataset_id": "synthetic-incumbent",
        "dataset_hash": "a" * 64,
        "acquisition_root_run_id": "synthetic-run",
    }
    expected_pair_by_field = {
        **incumbent_by_field,
        "publication_status": "published",
        "is_current": True,
        "lineage_authority": fixture.selection.PROFILE_SELECTION_LINEAGE_AUTHORITY,
    }
    desired_by_field = {
        **expected_pair_by_field,
        "dataset_id": incumbent_by_field["dataset_id"] if same_candidate else "synthetic-successor",
        "publication_status": "validated",
        "is_current": False,
    }
    initial_by_field = {
        "cms_serving_candidate": {"desired_cms_dataset": desired_by_field},
        "registry_source_admission": {"semantic_projection_as_of": "2026-01-01"},
    }
    seed = AsyncMock()
    current_request = AsyncMock(return_value=request)
    attest = AsyncMock(return_value=attestation)
    monkeypatch.setattr(fixture, "_seed_selection_row", seed)
    monkeypatch.setattr(
        fixture, "_variant_registry_rows", lambda: [{"source_id": "synthetic-variant", "endpoint_id": "variant"}]
    )
    read_current = AsyncMock(return_value={"payload": {"cms": incumbent_by_field}})
    monkeypatch.setattr(fixture.serving_receipt, "read_current_receipt", read_current)
    monkeypatch.setattr(fixture.selection, "current_profile_selection_request", current_request)
    monkeypatch.setattr(fixture.selection, "attest_profile_selection", attest)
    monkeypatch.setattr(fixture.selection, "validated_profile_selection_attestation", lambda value: value)
    execution = await fixture.attested_execution(database, initial_by_field, monkeypatch, generation=generation)
    assert seed.await_count == (2 if generation == 1 else 0)
    read_current.assert_awaited_once_with(session, "mrf")
    catalog = fixture.source.fhir._provider_directory_profile_selection_catalog()
    current_request.assert_awaited_once_with(
        catalog,
        desired_cms_dataset=expected_pair_by_field if same_candidate else desired_by_field,
        expected_cms_incumbent=expected_pair_by_field,
        desired_profile_as_of="2026-01-01",
    )
    fixture.selection._validated_desired_selection(current_request.await_args.kwargs)
    attest.assert_awaited_once_with(request, catalog)
    assert execution.attestation is attestation and execution.generation == generation


@pytest.mark.parametrize("suites", [False, True])
def test_historical_offices_preserve_doctor_identity(suites):
    from tests import cms_registry_native_producer_postgres_support as producer

    canonicalizer = fixture.address_canon
    addresses = [
        ("123 Example Street", f"Suite {unit}", "Sample City", "CA", "90210", "US")
        if suites
        else (f"{unit - 1} Example Street", None, "Example City", "CA", "90210", "US")
        for unit in (2, 3)
    ]
    location_keys = (
        [
            "538e9f4c87a4690cc29f1f7b8356738b274ec26200f65ab375087ccd89a9644f",
            "61d7e4cda7468b733818ec836fa8439e5ee7d2a09b02f0a35cd3bdcc90c4cfca",
        ]
        if suites
        else [
            "a008243ccf3c2ffb815fc0e69260dc6b1b061fb630da03f4767926a22d70304d",
            "cbf3d6078f5ac9c6a39201b65497f3480837abaffefc8df048b2236a921e3ee3",
        ]
    )
    doctors = [
        dict(
            location_key=location_keys[unit - 2],
            npi=1000000491,
            address_checksum=unit,
            address_line1=address[0],
            address_line2=address[1],
            city=address[2],
            state=address[3],
            zip_code=address[4],
            address_key=canonicalizer.address_key_v1(*address),
        )
        for unit, address in enumerate(addresses, 2)
    ]
    offices = producer._historical_office_rows(doctors)
    assert [office["location_key"] for office in offices] == location_keys
    assert {office["entity_id"] for office in offices} == {"1000000491"}
    assert [office["address_key"] for office in offices] == [doctor["address_key"] for doctor in doctors]
    assert [
        tuple(
            office[field]
            for field in ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")
        )
        for office in offices
    ] == addresses
    assert len({office["address_key"] for office in offices}) == 2
    assert (len({office["premise_key"] for office in offices}) == 1) is suites


def test_historical_profile_seed_uses_canonical_current_lineage():
    from dataclasses import replace

    from tests import cms_registry_native_producer_postgres_support as producer
    from tests.test_provider_directory_profile_selection_attestation import _execution

    execution = _execution()
    execution = replace(
        execution,
        attestation=replace(
            execution.attestation, payload={**execution.attestation.payload, "desired_profile_as_of": "2026-01-01"}
        ),
    )
    fhir = fixture.source.fhir
    contexts = tuple(
        fhir._ProviderDirectoryProfileSourceContext(
            source_id=pair["source_id"],
            endpoint_id=pair["endpoint_id"],
            canonical_api_base="https://example.test/fhir",
            org_name="Example",
            plan_name=None,
            authority_id=None,
        )
        for pair in execution.attestation.pairs
    )
    targets_by_field = {"evidence_target_oid": 71, "profile_target_oid": 72, "evidence_rows": 0, "profile_rows": 0}
    profile_by_field = producer._historical_profile_values(execution, contexts, targets_by_field)
    state = fhir._profile_serving_state_from_row(
        {
            **profile_by_field,
            "status": "published",
            "capacity_geometry_status": "legacy_unavailable",
            "capacity_geometry_hash": None,
            "capacity_geometry_json": None,
        }
    )
    assert state.profile_strategy_version == fixture.profile.PROFILE_BUILD_STRATEGY_VERSION
    assert state.source_vector == tuple(
        sorted((pair["source_id"], pair["dataset_id"]) for pair in execution.attestation.pairs)
    )
    assert {source_id for source_id, _digest in state.source_context_vector} == {
        source_id for source_id, _dataset in state.source_vector
    }
    assert state.evidence_target_oid == 71 and state.profile_target_oid == 72
    assert state.capacity_geometry_status == "legacy_unavailable"


async def test_native_sql_observers_accept_native_row_mappings():
    from tests import test_network_registry_cms_prepared_pair_postgres as retained

    default_by_field = {
        "default_expr": "nextval('mrf.synthetic_sequence'::regclass)",
        "sequence": "mrf.synthetic_sequence",
    }
    sequence_by_field = {"last_value": 7, "is_called": True}
    identity_by_field = {"database_oid": 73, "database_name": "synthetic", "database_system_identifier": "71"}
    first = AsyncMock(
        side_effect=[
            SimpleNamespace(_mapping=default_by_field),
            SimpleNamespace(_mapping=sequence_by_field),
            SimpleNamespace(_mapping=identity_by_field),
        ]
    )
    prepared = SimpleNamespace(
        address=SimpleNamespace(db_schema="mrf", stage_oids=(("entity_address_evidence", "synthetic_evidence", 71),)),
        fhir=SimpleNamespace(db=SimpleNamespace(first=first)),
    )
    assert await retained._source_evidence_sequence_state(prepared) == (default_by_field, sequence_by_field)
    assert first.await_args_list[0].kwargs == {"relation": "mrf.synthetic_evidence"}
    assert first.await_args_list[1].args == ("SELECT last_value,is_called FROM mrf.synthetic_sequence",)
    assert await retained._native_database_identity(prepared.fhir) == identity_by_field


def _split_native_copy_probe(counts):
    from process.network_fhir_source_epoch_pin import _CopyConnections

    async def export(query, *arguments, output, **options):
        assert options["format"] == "binary"
        await output(b"native-copy-bytes")
        return "COPY 1"

    async def import_rows(table, *, schema_name, source, **options):
        assert options["format"] == "binary" and source.read() == b"native-copy-bytes"
        return "COPY 1"

    reader = SimpleNamespace(
        connection=SimpleNamespace(copy_from_query=AsyncMock(side_effect=export)), verify_transaction=AsyncMock()
    )
    writer = SimpleNamespace(
        transaction=lambda: _context(None),
        copy_to_table=AsyncMock(side_effect=import_rows),
        fetchval=AsyncMock(side_effect=counts),
    )
    return _CopyConnections(reader, writer)


@pytest.mark.parametrize("interrupted", [False, True])
async def test_native_address_copy_observers_use_import_writer(monkeypatch, interrupted):
    from tests import test_network_registry_cms_prepared_pair_postgres as retained

    schema, table = "synthetic_retained", "entity_address_unified"
    transport = _split_native_copy_probe([1, 1] if interrupted else [0, 1])
    admission = SimpleNamespace(
        _relations={},
        _external_relations=set(),
        _native_relations={(schema, name): object() for name in retained.RELATION_NAMES},
        plan=SimpleNamespace(batch_size=32),
    )
    prepared = SimpleNamespace(
        nonprofile_admission=admission, fhir=SimpleNamespace(db=SimpleNamespace(scalar=AsyncMock(return_value=None)))
    )
    imported, phases = [], []
    callback = AsyncMock()
    if interrupted:

        async def prepare(*args, **kwargs):
            await retained.prepared_address._copy_native_batch(
                transport,
                "SELECT * FROM synthetic ORDER BY key",
                (),
                schema,
                table,
                1,
                byte_limit=64,
                import_admission=callback,
            )

        monkeypatch.setattr(retained.retention, "prepare_registry_cms_source_pair", prepare)
        from uuid import uuid4

        await retained._reject_address_copy_interruptions(
            prepared, object(), object(), ("synthetic_owner", "synthetic_reader"), uuid4(), monkeypatch
        )
        assert transport.target.copy_to_table.await_count == 2
    else:
        created_relations = [(schema, name, index) for index, name in enumerate(retained.RELATION_NAMES, 1)]
        observe = retained._address_copy_observer(prepared, created_relations, imported, phases)
        await observe(
            transport,
            "SELECT * FROM synthetic ORDER BY key",
            (),
            schema,
            table,
            1,
            byte_limit=64,
            import_admission=callback,
        )
        assert phases == [(table, "before_import", 0), (table, "after_import", 1)]
        assert imported == [(table, 1, len(b"native-copy-bytes"))]
    assert transport.target.fetchval.await_count == 2
    assert transport.source.verify_transaction.await_count == 2 * transport.target.copy_to_table.await_count


def test_native_address_copy_counts_accept_empty_families():
    from tests import test_network_registry_cms_prepared_pair_postgres as retained

    table = "entity_address_unified"
    copied_batches = [(table, 1, len(b"native-copy-bytes"))]
    receipt = SimpleNamespace(
        tables=[SimpleNamespace(table_name=name, row_count=int(name == table)) for name in retained.RELATION_NAMES]
    )
    retained._assert_address_copy_counts(copied_batches, receipt)
    receipt.tables[0].row_count += 1
    with pytest.raises(AssertionError):
        retained._assert_address_copy_counts(copied_batches, receipt)


@pytest.mark.parametrize("observed", [False, True])
async def test_native_binder_observer_follows_original_custody_probes(monkeypatch, observed):
    from dataclasses import dataclass

    from process import network_registry_cms_prepared_pair as retention
    from tests import cms_registry_native_producer_postgres_support as producer

    @dataclass(frozen=True)
    class SourceJob:
        capture_id: object
        notify_receipt: object

    original, observed_bind, retained, original_prepare = object(), object(), object(), object()
    job = SourceJob(object(), object())
    state = SimpleNamespace(captures=[], store=object(), roles=(), copy_bounds=(32, 64), acquired=object())
    module = SimpleNamespace(retention=retention)

    async def prepare(*arguments):
        assert retention.bind_prepared_registry_cms_source_pair is original
        return retained

    async def publish(database, execution, patch, **options):
        assert retention.bind_prepared_registry_cms_source_pair is original
        if observed:
            assert await retention.prepare_registry_cms_source_pair(object(), object(), object()) is retained
            assert retention.bind_prepared_registry_cms_source_pair is observed_bind
        else:
            assert retention.prepare_registry_cms_source_pair is original_prepare
        assert options["job"].capture_id is job.capture_id

    def observe_bind(actual_module, actual, actual_state, patch):
        assert actual_module is module and actual is original and actual_state is state
        return observed_bind

    monkeypatch.setattr(retention, "bind_prepared_registry_cms_source_pair", original)
    monkeypatch.setattr(retention, "prepare_registry_cms_source_pair", original_prepare)
    monkeypatch.setattr(producer, "issued_job", AsyncMock(return_value=job))
    monkeypatch.setattr(producer, "_original_binder_observer", observe_bind)
    monkeypatch.setattr(producer, "publish_native_execution", publish)
    await producer._publish_successor(
        module,
        object(),
        object(),
        object(),
        state,
        monkeypatch,
        preparation_observer=prepare if observed else None,
    )
    assert retention.bind_prepared_registry_cms_source_pair is original
    assert state.captures == [job.capture_id]


@pytest.mark.parametrize("drift", [False, True])
@pytest.mark.parametrize("address_copy", [False, True])
async def test_native_preparation_failure_sentinel_requires_selected_probes_and_source_parity(
    monkeypatch, drift, address_copy
):
    from tests import cms_registry_native_producer_postgres_support as producer

    original = AsyncMock()
    prepared, factory, request, identity = object(), object(), object(), object()
    retention = SimpleNamespace(prepare_registry_cms_source_pair=object())
    original_prepare = retention.prepare_registry_cms_source_pair
    state = SimpleNamespace(database=object(), roles=("owner", "reader"), address_copy_interruptions=address_copy)
    options_by_field = {"capture_id": object()}
    events = []

    def probe(name):
        async def observe(*arguments):
            assert retention.prepare_registry_cms_source_pair.func is original
            assert retention.prepare_registry_cms_source_pair.keywords == options_by_field
            events.append(name)

        return observe

    names = (
        "_reject_preparation",
        "_reject_raw_oid",
        "_reject_copy_batch_interruptions",
        "_reject_address_copy_interruptions",
    )
    module = SimpleNamespace(retention=retention, **{name: probe(name) for name in names})
    source_identity = AsyncMock(side_effect=[identity, object() if drift else identity])
    monkeypatch.setattr(producer, "_prepared_source_identity", source_identity)
    expected = AssertionError if drift else RuntimeError
    match = None if drift else "^synthetic preparation probes complete$"
    with pytest.raises(expected, match=match):
        await producer._observe_preparation_failures(
            module,
            original,
            prepared,
            factory,
            request,
            options_by_field,
            monkeypatch,
            state,
        )
    assert events == list(names[-1:] if address_copy else names[:-1]) and source_identity.await_count == 2
    assert getattr(state, "preparation_failures_verified", False) is not drift
    assert retention.prepare_registry_cms_source_pair is original_prepare


@pytest.mark.parametrize("claim", ["stopped", "mismatched", "still_running"])
async def test_native_fixture_cleanup_requires_exact_stopped_attempt(claim):
    import asyncio

    from tests import cms_registry_native_producer_postgres_support as producer

    attempt = SimpleNamespace(
        run_id="synthetic-run", attempt_id="synthetic-attempt", attempt_started_at="synthetic-start"
    )
    session = SimpleNamespace(scalar=AsyncMock(return_value=None if claim == "mismatched" else attempt.run_id))
    session.begin = lambda: _context(None)
    state = SimpleNamespace(
        publication_returned=claim != "still_running",
        publication_task=asyncio.current_task(),
        retention_sessions=SimpleNamespace(kw={"bind": SimpleNamespace(pool=SimpleNamespace(checkedout=lambda: 0))}),
        database=SimpleNamespace(session_factory=lambda: _context(session)),
        store=SimpleNamespace(source_attempt=attempt),
    )
    if claim == "stopped":
        await producer._record_failed_fixture_attempt(state)
    else:
        with pytest.raises(AssertionError):
            await producer._record_failed_fixture_attempt(state)
    if claim == "still_running":
        session.scalar.assert_not_awaited()
    else:
        statement, parameters_by_field = session.scalar.await_args.args
        assert "status='running'" in str(statement) and "status='failed'" in str(statement)
        assert "importer='provider-directory-fhir'" in str(statement)
        assert "progress->>'attempt_id'=:attempt" in str(statement)
        assert "progress->>'attempt_started_at'=:started" in str(statement)
        assert parameters_by_field == {
            "run": attempt.run_id,
            "attempt": attempt.attempt_id,
            "started": attempt.attempt_started_at,
        }


@pytest.mark.parametrize("observed", [False, True])
async def test_native_successor_probes_require_explicit_opt_in(monkeypatch, tmp_path, observed):
    from tests import cms_registry_native_producer_postgres_support as producer

    state = SimpleNamespace(database=object(), acquired=object(), retention_sessions=object())
    publish = AsyncMock()
    monkeypatch.setattr(producer, "_native_incumbent", lambda *arguments: _context(state))
    monkeypatch.setattr(producer, "_publish_successor_pair", publish)
    options_by_field = {"observe_preparation": True} if observed else {}
    copy_bounds = (32, 64)
    async with producer.genuine_prepared_publication(monkeypatch, tmp_path, copy_bounds, **options_by_field) as actual:
        assert actual is state
    publish.assert_awaited_once()
    assert publish.await_args.args[1:] == (
        state.database,
        state.acquired,
        state.retention_sessions,
        state,
        monkeypatch,
    )
    assert publish.await_args.kwargs == {"preparation_observer": producer._observe_preparation if observed else None}


async def test_native_source_cases_keep_ordinary_baseline_explicit(monkeypatch):
    from tests import test_network_registry_cms_prepared_pair_postgres as retained

    assert retained.cms_resource_template._fixture_function_marker.autouse is False
    assert retained.retained_catalog_baseline._fixture_function_marker.autouse is False
    assert any(
        mark.name == "usefixtures" and mark.args == ("retained_catalog_baseline",)
        for mark in retained.test_native_wal_attribution_preserves_original_refusal.pytestmark
    )
    request, events = object(), []

    async def baseline(actual):
        assert actual is request
        events.append("open")
        try:
            yield
        finally:
            events.append("close")

    monkeypatch.setattr(retained.support, "cms_admission_template", SimpleNamespace(__wrapped__=baseline))
    fixture = retained.cms_resource_template.__wrapped__(request, None)
    assert not events
    await anext(fixture)
    assert events == ["open"]
    await fixture.aclose()
    assert events == ["open", "close"]


@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_native_source_envelope_restores_exact_owned_scope(monkeypatch, failure):
    import os

    from sqlalchemy.engine import make_url

    from tests import provider_directory_source_postgres_support as source
    from tests import test_provider_directory_cms_resource_batch_postgres as resources

    incoming = make_url("postgresql://postgres@127.0.0.1:5432/hc_network_registry_" + "a" * 32)
    strict = incoming.set(drivername="postgresql+asyncpg", database="hc_cms_admission_test_" + "b" * 32)
    rendered = incoming.render_as_string(hide_password=False)
    events = []
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", rendered)
    monkeypatch.setenv(resources.support._DSN_ENV, rendered)

    @asynccontextmanager
    async def owned(url):
        assert url == incoming.set(drivername="postgresql+asyncpg")
        events.append("owned_open")
        try:
            yield strict, object()
        finally:
            assert os.environ[resources.support._DSN_ENV] == rendered
            events.append("owned_close")

    @asynccontextmanager
    async def templates():
        events.append("templates_open")
        try:
            yield
        finally:
            assert os.environ[resources.support._DSN_ENV] == rendered
            events.append("templates_close")

    monkeypatch.setattr(resources.support, "_owned_database", owned)
    monkeypatch.setattr(source, "source_component_templates", templates)
    outcome = nullcontext() if failure is None else pytest.raises(failure, match="^synthetic scope failure$")
    with outcome:
        async with resources._cms_source_scope():
            assert resources.support._database_url() == strict
            if failure is not None:
                raise failure("synthetic scope failure")
    assert os.environ[resources.support._DSN_ENV] == rendered
    assert events == ["owned_open", "templates_open", "templates_close", "owned_close"]


@pytest.mark.parametrize(
    "name",
    [
        "test_network_registry_cms_prepared_pair_postgres",
        "test_registry_cms_source_consumer_postgres",
        "test_registry_cms_source_consumer_initial_postgres",
        "test_registry_cms_source_consumer_race_postgres",
        "test_registry_cms_source_mixed_postgres",
    ],
)
def test_native_source_callers_use_one_explicit_envelope(name):
    import importlib

    from tests import test_provider_directory_cms_resource_batch_postgres as resources

    module = importlib.import_module("tests." + name)
    assert module.cms_source_template is resources.cms_source_template
    marks = module.pytestmark if isinstance(module.pytestmark, list) else [module.pytestmark]
    assert any(mark.name == "usefixtures" and mark.args == ("cms_source_template",) for mark in marks)


async def test_native_registration_proves_partition_roots_before_enrolling_heaps(monkeypatch):
    from process import provider_directory_cms_native_registration as registration
    from process import provider_directory_projection_stage as stage

    driver = SimpleNamespace(fetchval=AsyncMock(return_value=71), execute=AsyncMock())
    session = SimpleNamespace(execute=AsyncMock())
    database = SimpleNamespace(session_factory=lambda: _context(session))
    session.begin = lambda: _context(None)
    component = SimpleNamespace(policy=SimpleNamespace(owner_role="source_owner"))
    candidates = (SimpleNamespace(stage_schema="mrf", stage_table="address_alias_v1", target_table="address_alias_v1"),)
    capture = AsyncMock(return_value={"protected_fhir": object(), "candidates": candidates})
    register = AsyncMock()
    monkeypatch.setattr(stage, "_copy_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(registration, "capture_current_native_input_candidates", capture)
    monkeypatch.setattr(registration, "register_native_input_candidates", register)
    await fixture._register_native_heaps(database, component)
    capture.assert_awaited_once_with(driver, ledger_schema="mrf", source_policy=component.policy)
    register.assert_awaited_once_with(driver, ledger_schema="mrf", candidates=candidates, expected_actor_oid=71)
    assert [call.args[0] for call in driver.execute.await_args_list] == [
        'REVOKE ALL ON TABLE "mrf"."address_alias_v1" FROM "source_owner"',
        'GRANT SELECT,MAINTAIN ON TABLE "mrf"."address_alias_v1" TO "source_owner"',
    ]


async def test_builder_migrations_wrap_authentic_canonical_install(monkeypatch):
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext import asyncio as native_engines

    from tests import nppes_candidate_postgres_support as nppes

    events = []
    prefixes = fixture.source_component_native_migrations()
    boundary = prefixes.index("20261016040000")
    policy = SimpleNamespace(schema="mrf", control_runtime_roles=("source_builder",))
    connection = SimpleNamespace(run_sync=AsyncMock())
    historical = SimpleNamespace(connect=lambda: _context(connection), dispose=AsyncMock())
    engine = SimpleNamespace(url=make_url("postgresql+asyncpg://admin@127.0.0.1/fixture"))

    async def bootstrap(actual, actual_policy, selected):
        assert actual is engine and actual_policy is policy and selected == prefixes[:boundary]
        events.append("builder")

    async def canonical(actual, schema):
        assert actual is historical and schema == policy.schema
        events.append("canonical")

    async def migrations(callback, selected):
        assert callback is fixture.source._run_migrations and selected == prefixes[boundary:]
        events.append("typed_repairs")

    def historical_engine(url):
        assert url.username == policy.control_runtime_roles[0]
        return historical

    connection.run_sync.side_effect = migrations
    monkeypatch.setattr(native_engines, "create_async_engine", historical_engine)
    monkeypatch.setattr(nppes, "_bootstrap_nppes_parent_component", bootstrap)
    monkeypatch.setattr(nppes, "install_source_canonical_functions", canonical)
    await fixture._bootstrap_native_source(engine, policy, migration_prefixes=prefixes)
    assert events == ["builder", "canonical", "typed_repairs"]
    historical.dispose.assert_awaited_once()


async def test_profile_controls_have_only_protocol_dml_grants():
    toast_rows = [SimpleNamespace(relname="pg_toast_71"), SimpleNamespace(relname="pg_toast_72")]
    driver = SimpleNamespace(execute=AsyncMock())
    connection = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: toast_rows)),
        scalar=AsyncMock(return_value=True),
        get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)),
    )
    database = SimpleNamespace(engine=SimpleNamespace(begin=lambda: _context(connection)))
    await fixture._grant_profile_controls(database, SimpleNamespace(schema="mrf", owner_role="source_owner"))
    statements = [
        str(call.args[0]) for call in connection.execute.await_args_list if str(call.args[0]).startswith("GRANT")
    ]
    assert len(statements) == 14 + len(fixture.addresses.RELATION_NAMES)
    assert all(statement.endswith('TO "source_owner"') for statement in statements)
    assert all("ALL" not in statement and "ALL TABLES" not in statement for statement in statements)
    assert 'GRANT SELECT,UPDATE ON "mrf"."import_run" TO "source_owner"' not in statements
    assert all('ON "mrf"."import_run"' not in statement for statement in statements)
    assert any(
        'UPDATE("status","phase_detail","heartbeat_at","progress","error","metrics","started_at","finished_at") '
        'ON "mrf"."import_run"' in call.args[0]
        for call in driver.execute.await_args_list
    )
    assert connection.scalar.await_count == 1
    assert str(connection.scalar.await_args.args[0]) == "SELECT NOT has_table_privilege(:owner,:table,'SELECT,UPDATE')"
    assert connection.scalar.await_args.args[1] == {"owner": "source_owner", "table": '"mrf"."import_run"'}
    assert sum("DELETE" in statement for statement in statements) == 1
    assert (
        'GRANT SELECT,INSERT,UPDATE,DELETE ON "mrf"."provider_directory_profile_build_checkpoint" TO "source_owner"'
        in statements
    )

    for table in (
        "provider_directory_profile_selection_proof",
        "provider_directory_profile_capacity_lease_consumption",
    ):
        assert f'GRANT SELECT,INSERT,MAINTAIN ON "mrf"."{table}" TO "source_owner"' in statements
    assert 'GRANT SELECT ON "mrf"."provider_directory_profile_failed_cleanup_claim" TO "source_owner"' in statements
    assert all(
        f'GRANT SELECT ON "mrf"."{table}" TO "source_owner"' in statements for table in fixture.addresses.RELATION_NAMES
    )

    assert 'GRANT SELECT(chunk_id) ON "pg_toast"."pg_toast_71" TO "source_owner"' in statements
    assert 'GRANT SELECT(chunk_id) ON "pg_toast"."pg_toast_72" TO "source_owner"' in statements
    selection = next(call for call in connection.execute.await_args_list if str(call.args[0]).startswith("SELECT"))
    assert "relation.relname=ANY(:names)" in str(selection.args[0])
    assert selection.args[1]["schema"] == "mrf" and selection.args[1]["names"] == ["import_run"]
    assert "toast.relowner<>" in str(selection.args[0])
    assert selection.args[1]["owner"] == "source_owner"


@pytest.mark.parametrize("damage", ["trigger", "body", "namespace", "defaults", "composite"])
def test_counter_custody_projection_preserves_all_other_metadata(damage):
    from copy import deepcopy

    from tests.test_native_counter_callable_capability import CounterProtocol

    protocol = CounterProtocol(owner=103)
    previous_by_field = dict(
        catalog=dict(ledger=dict(catalog=protocol.catalog, composite=protocol.composite), functions=protocol.functions),
        namespace=dict(oid=201),
        defaults=[],
    )
    changed = deepcopy(previous_by_field)
    match damage:
        case "trigger":
            changed["catalog"]["ledger"]["catalog"]["triggers"][0]["tgenabled"] = "D"
        case "body":
            changed["catalog"]["functions"]["cms_native_input_advance"]["definition"]["prosrc"] = "different"
        case "namespace":
            changed["namespace"]["oid"] = 202
        case "defaults":
            changed["defaults"].append({"defaclrole": 101})
        case _:
            changed["catalog"]["ledger"]["composite"]["oid"] = "14"
    assert fixture._counter_immutable_inventory(changed) != fixture._counter_immutable_inventory(previous_by_field)


async def test_all_four_serving_targets_are_empty_and_source_owned_before_attestation():
    native_targets = tuple(
        model.__tablename__
        for model in (
            fixture.entity_address_unified.EntityAddressUnified,
            *fixture.entity_address_unified.SUPPORT_TABLE_MODELS,
        )
    )
    source_oids_by_name = {f'"mrf"."{name}"': 71 + ordinal for ordinal, name in enumerate(native_targets)}

    async def scalar(statement, params=None):
        sql = str(statement)
        if sql == "SELECT to_regclass(:name)":
            return 71
        if sql == "SELECT to_regclass(:name)::oid::bigint":
            return source_oids_by_name[params["name"]]
        if sql == (
            "SELECT oid=:oid AND relowner=(SELECT oid FROM pg_roles WHERE rolname=:owner) "
            "FROM pg_class WHERE oid=to_regclass(:name)"
        ):
            assert params == {
                "oid": source_oids_by_name[params["name"]],
                "owner": "source_owner",
                "name": params["name"],
            }
            return True
        assert sql.startswith("SELECT count(*) FROM ")
        return 0

    connection = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=scalar), run_sync=AsyncMock())
    database = SimpleNamespace(engine=SimpleNamespace(begin=lambda: _context(connection)))
    await fixture._create_native_relations(database, owner_role="source_owner")
    serving_targets = (
        "provider_directory_profile",
        "provider_directory_profile_evidence",
        "provider_directory_address_overlay",
        "provider_directory_network_catalog",
    )
    assert [str(call.args[0]) for call in connection.execute.await_args_list if " OWNER TO " in str(call.args[0])] == [
        f'ALTER TABLE "mrf"."{name}" OWNER TO "source_owner"' for name in (*native_targets, *serving_targets)
    ]
    assert {
        call.args[1]["name"]
        for call in connection.scalar.await_args_list
        if str(call.args[0]).startswith("SELECT oid=:oid AND relowner=")
    } == set(source_oids_by_name)
    assert [
        str(call.args[0]) for call in connection.scalar.await_args_list if str(call.args[0]).startswith("SELECT count")
    ] == [f'SELECT count(*) FROM "mrf"."{name}"' for name in serving_targets]
    assert connection.run_sync.await_count == 2


@pytest.mark.parametrize("replace_profile_target", [False, True], ids=["original-source", "profile-oid"])
async def test_resume_probes_open_source_after_mutation_and_close_before_restore(monkeypatch, replace_profile_target):
    """Record transaction order only; these doubles confer no native authority."""
    from process import provider_directory_profile_scope_copy as scope_copy

    events, directory = [], object()

    @asynccontextmanager
    async def input_transaction(actual):
        assert actual is directory
        events.append("open")
        try:
            yield
        finally:
            events.append("close")

    async def status(*args, **kwargs):
        events.append("write")

    database = SimpleNamespace(
        transaction=lambda: _context(None),
        scalar=AsyncMock(side_effect=[11, 12, 12, 11] if replace_profile_target else ["Original"]),
        status=status,
    )
    arguments = (directory, object(), object(), object(), object(), frozenset({"Practitioner"}))
    reason = (
        "provider_directory_profile_capacity_target_identity_changed"
        if replace_profile_target
        else "provider_directory_nonprofile_capacity_profile_geometry_changed"
    )

    async def resume(*actual):
        assert all(left is right for left, right in zip(actual, arguments, strict=True))
        events.append("reject")
        raise RuntimeError(reason)

    monkeypatch.setattr(fixture.source_policy, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(scope_copy, "input_transaction", input_transaction)
    probe = fixture._reject_resume_target_change if replace_profile_target else fixture._reject_resume_source_change
    await probe(database, resume, arguments)
    writes_before, writes_after = (2, 2) if replace_profile_target else (1, 1)
    assert events == [*["write"] * writes_before, "open", "reject", "close", *["write"] * writes_after]


async def test_native_registration_enrolls_only_archive_writer_before_registering(
    monkeypatch,
):
    from process import provider_directory_cms_native_registration as registration
    from process import provider_directory_projection_stage as stage
    from tests import cms_archive_writer_postgres_support as archive_writer

    driver = SimpleNamespace(fetchval=AsyncMock(return_value=71), execute=AsyncMock())
    session = SimpleNamespace(execute=AsyncMock())
    database = SimpleNamespace(session_factory=lambda: _context(session))
    session.begin = lambda: _context(None)
    component = SimpleNamespace(policy=SimpleNamespace(schema="mrf", owner_role="source_owner"))
    previous = registration.NativeInputCandidate(
        "mrf", "address_archive_v2", "mrf", "address_archive_v2", 100, 200, 100
    )
    current = registration.NativeInputCandidate("mrf", "address_archive_v2", "mrf", "address_archive_v2", 100, 300, 100)
    capture = AsyncMock(return_value={"protected_fhir": object(), "candidates": (previous,)})
    enroll, register = AsyncMock(return_value=current), AsyncMock()
    monkeypatch.setattr(stage, "_copy_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(registration, "capture_current_native_input_candidates", capture)
    monkeypatch.setattr(registration, "register_native_input_candidates", register)
    monkeypatch.setattr(archive_writer, "enroll_archive_writer", enroll)
    await fixture._register_native_heaps(database, component)
    enroll.assert_awaited_once_with(driver, component.policy, previous)
    register.assert_awaited_once_with(driver, ledger_schema="mrf", candidates=(current,), expected_actor_oid=71)
    driver.execute.assert_not_awaited()
