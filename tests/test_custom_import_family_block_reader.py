# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed Reader capability, native identity and prepublication failure boundaries."""

from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from api import custom_import_owner_worker as worker
from process.custom_import import family_block_reader as readers
from process.custom_import import family_block_store as store
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import snowflake_segmented_runner as runner
from process.custom_import.build_source import _WINDOW, _PageWindow
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import ROLE_SPECS
from process.custom_import.family_shard_owner_cli import NODE_ENV, PROFILE_ENV, profile
from tests.test_control_owner_handoff import _profile, _signed_profile
from tests.test_custom_import_build_graph import _request


def _document():
    owner, _signed = _profile()
    owner["reader_activation"] = {
        "contract": "custom-import/reader-activation/v1",
        "dataset_id": 7,
        "source_family_id": 31,
        "grantee": "synthetic_reader",
        "grantee_oid": 88,
        "secret": "synthetic-reader",
    }
    return owner


def _binding():
    return SimpleNamespace(
        family_id=42,
        owner_oid=77,
        layout=SimpleNamespace(digest=b"l" * 32),
        oids_by_role={role: 100 + number for number, role in enumerate(ROLE_SPECS)},
        columns_sha256_by_role={role: b"c" * 32 for role in ROLE_SPECS},
    )


@pytest.mark.parametrize("changed", [None, -1, 0, 1, 2, 3, 4, 5, 6])
async def test_actual_role_oid_login_and_owner_privileges_are_rechecked(monkeypatch, changed):
    native_role_cells = [88, "synthetic_reader", True, False, False, False, False]
    if changed == -1:
        native_role_cells = None
    elif changed is not None:
        native_role_cells[changed] = (
            89 if changed == 0 else "different_reader" if changed == 1 else not native_role_cells[changed]
        )
    monkeypatch.setattr(
        store, "_execute", AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: native_role_cells))
    )
    if changed is None:
        await readers._role(object(), readers.activation_profile(_document()))
    else:
        with pytest.raises(FamilyBlockUnavailable, match="identity"):
            await readers._role(object(), readers.activation_profile(_document()))


def _result(*, rows=(), scalar=None):
    return SimpleNamespace(all=lambda: rows, scalar_one=lambda: scalar)


@pytest.mark.parametrize(
    "change",
    [
        {"extra": True},
        {"dataset_id": True},
        {"source_family_id": 0},
        {"grantee_oid": 0},
        {"grantee": "PUBLIC"},
        {"grantee": "synthetic_owner"},
        {"secret": "synthetic-secret"},
        {"secret": "../reader"},
    ],
)
def test_profile_refuses_unapproved_shape_or_nonindependent_reader(change):
    document = _document()
    document["reader_activation"] |= change
    with pytest.raises(FamilyBlockUnavailable):
        profile({PROFILE_ENV: handoff.canonical(document).decode(), NODE_ENV: document["node_name"]})


def test_typed_profile_is_optional_and_secret_is_only_an_exact_reference():
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    original, _signed = _profile()
    assert readers.activation_profile(original) is None
    document = _document()
    checked = profile({PROFILE_ENV: handoff.canonical(document).decode(), NODE_ENV: document["node_name"]})[0]
    assert readers.activation_profile(checked) == readers.ReaderActivationProfile(
        7, 31, "synthetic_reader", 88, "synthetic-reader"
    )
    signed = _signed_profile(document, Ed25519PrivateKey.generate())
    container = worker.manifest(signed, document)["spec"]["template"]["spec"]["containers"][0]
    assert len(container["envFrom"]) == 2
    assert container["env"][-2:] == [
        {"name": name, "valueFrom": {"secretKeyRef": {"name": "synthetic-reader", "key": name}}}
        for name in ("HLTHPRT_DB_READER_USER", "HLTHPRT_DB_READER_PASSWORD")
    ]
    assert all("value" not in entry for entry in container["env"] if "PASSWORD" in entry["name"])
    assert len(worker._owner_container(document, "a" * 64)["env"]) == 5
    assert (
        len(worker.manifest(signed, document, with_reader=False)["spec"]["template"]["spec"]["containers"][0]["env"])
        == 5
    )


def _acl_cells(binding, *, complete=True):
    relation_acls = [
        (oid, binding.owner_oid, binding.owner_oid, "SELECT", False) for oid in binding.oids_by_role.values()
    ]
    schema_acls = [(51, binding.owner_oid, binding.owner_oid, "USAGE", False)]
    if complete:
        relation_acls += [
            (binding.oids_by_role[role], binding.owner_oid, 88, "SELECT", False) for role in readers._SERVING
        ]
        schema_acls.append((51, binding.owner_oid, 88, "USAGE", False))
    return relation_acls, schema_acls


async def _observe_acl(monkeypatch, binding, relation_acls, schema_acls, *, complete=True):
    responses = iter((relation_acls, schema_acls))

    async def execute(_session, sql, parameters):
        if "pg_class" in sql:
            assert set(parameters["oids"]) == set(binding.oids_by_role.values())
        else:
            assert parameters == {"schema": "ci_snapshot_42"}
        return _result(rows=next(responses))

    monkeypatch.setattr(store, "_execute", execute)
    return await readers._acl_state(object(), binding, readers.activation_profile(_document()), complete=complete)


async def test_exact_serving_acls_and_empty_partial_state_have_distinct_oracles(monkeypatch):
    binding = _binding()
    assert await _observe_acl(monkeypatch, binding, *_acl_cells(binding))
    assert await _observe_acl(monkeypatch, binding, *_acl_cells(binding, complete=False), complete=False)
    with pytest.raises(FamilyBlockUnavailable, match="incomplete"):
        await _observe_acl(monkeypatch, binding, *_acl_cells(binding, complete=False))


@pytest.mark.parametrize(
    "change",
    [
        "public",
        "other_role",
        "write",
        "grant_option",
        "pack",
        "rejection",
        "shard_receipt",
        "owner",
        "missing_relation",
        "schema_create",
    ],
)
async def test_candidate_acl_drift_and_excluded_storage_refuse(monkeypatch, change):
    binding = _binding()
    relation_acls, schema_acls = _acl_cells(binding)
    oid = binding.oids_by_role["query_root"]
    match change:
        case "public":
            relation_acls.append((oid, 77, 0, "SELECT", False))
        case "other_role":
            relation_acls.append((oid, 77, 99, "SELECT", False))
        case "write":
            relation_acls.append((oid, 77, 88, "INSERT", False))
        case "grant_option":
            relation_acls.append((oid, 77, 88, "SELECT", True))
        case "owner":
            relation_acls[0] = (relation_acls[0][0], 79, 79, "SELECT", False)
        case "missing_relation":
            relation_acls = [acl for acl in relation_acls if acl[0] != oid]
        case "schema_create":
            schema_acls.append((51, 77, 88, "CREATE", False))
        case _:
            relation_acls.append((binding.oids_by_role[change], 77, 88, "SELECT", False))
    with pytest.raises(FamilyBlockUnavailable):
        await _observe_acl(monkeypatch, binding, relation_acls, schema_acls)


async def test_old_source_helper_queries_exact_snapshot_and_keeps_all_source_intersection(monkeypatch):
    async def control(_session):
        return dialect().identifier_preparer, "synthetic_control"

    async def execute(statement, parameters):
        assert parameters == {
            "names": [
                "ci_snapshot_31.custom_import_family_revision",
                "ci_snapshot_31.custom_import_root_revision",
                "ci_snapshot_31.custom_import_root_scalar",
            ],
            "count": 3,
        }
        assert "a.grantee NOT IN (0,c.relowner)" in str(statement)
        assert "HAVING count(DISTINCT c.oid)=:count" in str(statement)
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: ["synthetic_reader"]))

    monkeypatch.setattr(store, "_control_schema", control)
    assert await store.reader_capability_plan(SimpleNamespace(execute=execute), "family", source_family_id=31) == (
        "synthetic_reader",
    )


@pytest.mark.parametrize(
    "problem", [None, "missing_scalar", "other_role", "source_family", "not_legacy", "schema_usage"]
)
async def test_source_capabilities_require_one_exact_grantee_on_every_input(monkeypatch, problem):
    calls = []

    async def capability(_session, role, **arguments):
        assert arguments["source_family_id"] == 31
        calls.append(role)
        return (
            ()
            if problem == "missing_scalar" and role == "fragment"
            else ("different_reader" if problem == "other_role" else "synthetic_reader",)
        )

    pin = SimpleNamespace(family_id=32 if problem == "source_family" else 31, legacy_snapshot=problem != "not_legacy")
    monkeypatch.setattr(readers, "resolve_generation_snapshot_binding", AsyncMock(return_value=pin))
    monkeypatch.setattr(readers, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(store, "reader_capability_plan", capability)
    monkeypatch.setattr(store, "_execute", AsyncMock(return_value=_result(scalar=problem != "schema_usage")))
    request, family, generation = _capability_source()
    session = _source_session(family, generation)
    if problem is None:
        assert await readers._source_pin(session, request, readers.activation_profile(_document())) is pin
        target = readers.resolve_generation_snapshot_binding.await_args.args[1]
        assert (target.generation_id, target.definition_revision_id) == (13, 2)
        assert target.generation_id != request.expected_base_generation_id
        assert calls == list(readers._SERVING)
    else:
        with pytest.raises(FamilyBlockUnavailable):
            await readers._source_pin(session, request, readers.activation_profile(_document()))


def _capability_source():
    request = SimpleNamespace(
        dataset_id=7, expected_base_generation_id=99, definition_revision_id=3, schema_revision_id=4
    )
    generation = SimpleNamespace(
        generation_id=13,
        dataset_id=7,
        definition_revision_id=2,
        schema_revision_id=4,
        execution_id=21,
        capture_bundle_id=22,
        producing_fence=1,
        producing_token_sha256=b"t" * 32,
    )
    family = SimpleNamespace(**vars(generation), family_id=31, frozen_at=object())
    return request, family, generation


def _source_session(family, generation):
    async def get(model, identity, *, populate_existing=False):
        if model is readers.CustomImportSnapshotFamily:
            assert identity == 31 and populate_existing is True
            return family
        assert model is readers.CustomImportGeneration and identity == 13
        assert populate_existing is True
        return generation

    return SimpleNamespace(get=AsyncMock(side_effect=get))


@pytest.mark.parametrize(
    "problem",
    [
        "missing_family",
        "unbound",
        "zero_generation",
        "unfrozen",
        "wrong_family",
        "wrong_dataset",
        "wrong_schema",
        "missing_generation",
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
    ],
)
async def test_capability_source_requires_the_original_frozen_native_registry(monkeypatch, problem):
    request, family, generation = _capability_source()
    if problem == "missing_family":
        family = None
    elif problem == "missing_generation":
        generation = None
    elif problem in {"unbound", "zero_generation", "unfrozen", "wrong_family", "wrong_dataset", "wrong_schema"}:
        field, value = {
            "unbound": ("generation_id", None),
            "zero_generation": ("generation_id", 0),
            "unfrozen": ("frozen_at", None),
            "wrong_family": ("family_id", 32),
            "wrong_dataset": ("dataset_id", 8),
            "wrong_schema": ("schema_revision_id", 5),
        }[problem]
        setattr(family, field, value)
    else:
        value = getattr(generation, problem)
        setattr(generation, problem, b"x" * 32 if type(value) is bytes else value + 1)
    pin = AsyncMock()
    capability = AsyncMock()
    monkeypatch.setattr(readers, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(readers, "resolve_generation_snapshot_binding", pin)
    monkeypatch.setattr(store, "reader_capability_plan", capability)
    with pytest.raises(FamilyBlockUnavailable):
        await readers._source_pin(_source_session(family, generation), request, readers.activation_profile(_document()))
    pin.assert_not_awaited()
    capability.assert_not_awaited()


async def test_retired_capability_source_never_falls_back_to_the_current_base(monkeypatch):
    from process.custom_import.read_contracts import CustomImportReadUnavailableError

    request, family, generation = _capability_source()
    retired = CustomImportReadUnavailableError("pinned generation storage is retired")
    pin = AsyncMock(side_effect=retired)
    capability = AsyncMock()
    monkeypatch.setattr(readers, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(readers, "resolve_generation_snapshot_binding", pin)
    monkeypatch.setattr(store, "reader_capability_plan", capability)
    with pytest.raises(CustomImportReadUnavailableError) as failure:
        await readers._source_pin(_source_session(family, generation), request, readers.activation_profile(_document()))
    assert failure.value is retired and pin.await_count == 1
    assert pin.await_args.args[1].generation_id == family.generation_id != request.expected_base_generation_id
    capability.assert_not_awaited()


async def test_install_rechecks_scope_and_never_grants_a_failed_source(monkeypatch):
    events = []
    binding = _binding()

    @asynccontextmanager
    async def transaction(*_args, **arguments):
        assert _args[-1] == "logged"
        assert arguments == {"expected_identity": "original-proof"}
        events.append("native-open")
        yield object(), object(), binding
        events.append("native-commit")

    monkeypatch.setattr(store, "_block_session", transaction)
    monkeypatch.setattr(readers, "_effective_rights", AsyncMock())
    monkeypatch.setattr(readers, "_role", AsyncMock(side_effect=lambda *_: events.append("role")))
    monkeypatch.setattr(readers, "_source_pin", AsyncMock(side_effect=lambda *_: events.append("source")))
    monkeypatch.setattr(
        readers,
        "_acl_state",
        AsyncMock(side_effect=lambda *_, **kw: events.append("complete" if kw["complete"] else "partial")),
    )
    grant = AsyncMock(side_effect=lambda *_: events.append("grant"))
    monkeypatch.setattr(readers, "_grant_serving", grant)
    activation = readers.ReaderActivation(
        readers.activation_profile(_document()), SimpleNamespace(reader_session=lambda: None)
    )
    context = SimpleNamespace(
        common=SimpleNamespace(request=object(), layout=object(), source=SimpleNamespace(snapshot_family_id=42))
    )
    await activation.install(object(), context, identity="original-proof")
    assert events == [
        "native-open",
        "role",
        "source",
        "partial",
        "grant",
        "role",
        "source",
        "complete",
        "native-commit",
    ]
    monkeypatch.setattr(readers, "_source_pin", AsyncMock(side_effect=FamilyBlockUnavailable("missing source")))
    grant.reset_mock()
    with pytest.raises(FamilyBlockUnavailable):
        await activation.install(object(), context, identity="original-proof")
    grant.assert_not_awaited()


async def test_grant_statements_are_only_eleven_closed_serving_roles_and_usage(monkeypatch):
    statements = []
    monkeypatch.setattr(store, "_control_schema", AsyncMock(return_value=(dialect().identifier_preparer, "control")))
    monkeypatch.setattr(store, "_execute", AsyncMock(side_effect=lambda _session, sql: statements.append(sql)))
    await readers._grant_serving(object(), _binding(), readers.activation_profile(_document()))
    assert statements[0] == "GRANT USAGE ON SCHEMA ci_snapshot_42 TO synthetic_reader"
    assert statements[1:] == [
        f"GRANT SELECT ON TABLE ci_snapshot_42.{ROLE_SPECS[role][1]} TO synthetic_reader" for role in readers._SERVING
    ]
    assert len(statements) == 12 and set(readers._SERVING).isdisjoint(readers._EXCLUDED)


def _publication(monkeypatch, *, problem=None):
    binding = _binding()
    session = SimpleNamespace(info={}, no_autoflush=nullcontext(), execute=AsyncMock())
    events = []
    request = _request(
        dataset_id=7,
        expected_base_generation_id=9,
        expected_pointer_version=1,
        definition_revision_id=3,
        schema_revision_id=4,
        statement_timeout_ms=1000,
    )

    @asynccontextmanager
    async def transaction(*_args, **_kwargs):
        try:
            events.append("open")
            yield session
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    async def probe(*_args):
        events.append("reader")
        if problem == "timeout":
            raise TimeoutError("Reader probe expired")

    acl_observations = []

    async def acls(*_args, **_kwargs):
        acl_observations.append(None)
        return (
            "changed"
            if (
                problem == "acl_drift"
                and len(acl_observations) > 1
                or problem == "acl_after_cas"
                and len(acl_observations) > 2
            )
            else "same"
        )

    pin = SimpleNamespace(checked_block_oids=AsyncMock(return_value=binding.oids_by_role))
    monkeypatch.setattr(readers, "_session", transaction)
    monkeypatch.setattr(readers, "_role", AsyncMock())
    monkeypatch.setattr(readers, "_source_pin", AsyncMock())
    monkeypatch.setattr(readers, "_sealed_binding", AsyncMock(return_value=(pin, binding)))
    monkeypatch.setattr(readers, "_acl_state", acls)
    monkeypatch.setattr(readers, "_effective_rights", AsyncMock())
    monkeypatch.setattr(readers, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(readers.ReaderActivation, "_probe", probe)
    activation = readers.ReaderActivation(
        readers.activation_profile(_document()), SimpleNamespace(reader_session=lambda: None)
    )
    return activation, request, session, events


@pytest.mark.parametrize("problem", [None, "timeout", "acl_drift", "acl_after_cas", "uncertain_reply"])
async def test_probe_and_fresh_acl_gate_precede_pointer_cas_in_same_transaction(monkeypatch, problem):
    activation, request, session, events = _publication(monkeypatch, problem=problem)

    async def pointer(actual, **_kwargs):
        assert actual is session and events[-1] == "reader"
        events.append("pointer")
        if problem == "uncertain_reply":
            raise ConnectionError("pointer reply is unknown")
        return "published"

    native_cas = AsyncMock(side_effect=pointer)
    monkeypatch.setattr(runner, "activate_generation", native_cas)
    output = SimpleNamespace(generation_id=17)
    if problem:
        with pytest.raises((FamilyBlockUnavailable, TimeoutError, ConnectionError)):
            await runner._activate(object(), request, output, reader_activation=activation, reader_layout=object())
        if problem in ("acl_after_cas", "uncertain_reply"):
            native_cas.assert_awaited_once()
        else:
            native_cas.assert_not_awaited()
        assert events[-1] == "rollback"
    else:
        assert (
            await runner._activate(object(), request, output, reader_activation=activation, reader_layout=object())
            == "published"
        )
        assert events == ["open", "reader", "pointer", "commit"]
