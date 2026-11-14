# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider receipts survive validation and gate every serving-head writer."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import asyncpg
import pytest

from process import network_em_provider_inputs as provider
from process import network_membership_pipeline as pipeline
from process import network_membership_publication as publication
from process import network_membership_serving_indexes as serving_indexes
from process import network_membership_validation as validation
from process import network_serving_reactivation as reactivation
from process import network_serving_read as serving
from process import registry_candidate_composition as composition
from process import registry_fhir_serving_preservation as preservation
from process.network_address_projection import PinnedAddressSource
from process.network_membership_copy import MembershipCopyTarget
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_network_em_provider_inputs import family, prepare

_BASE_RECEIPT = prepare()
pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def legacy_custody_lookup(monkeypatch):
    monkeypatch.setattr(publication, "verify_network_candidate_custody", AsyncMock(return_value=None))


@pytest.fixture(autouse=True)
def search_index_gate(monkeypatch):
    """Model native readiness separately from these synthetic custody/CAS protocols."""
    verify = AsyncMock()
    monkeypatch.setattr(serving_indexes, "verify_network_search_indexes", verify)
    return verify


def _candidate(state="ready"):
    receipt_by_field = copy.deepcopy(_BASE_RECEIPT)
    ids = [str(UUID(int=n)) for n in range(1, 5)]
    target = MembershipCopyTarget(*ids, "network_candidate_" + UUID(ids[-1]).hex)
    candidate_by_field = {
        name: getattr(target, name)
        for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    candidate_by_field.update(
        state=state,
        schema_revision=1,
        approved_custom_revision=2,
        expected_head=16,
        expected_rows=0,
        accepted_rows=0,
        source_recipes_json=[],
        source_generations={
            "unified_address": "address-edition",
            "em_provider_inputs": receipt_by_field["receipt_sha256"],
        },
        validation_json={"em_provider_inputs": receipt_by_field},
    )
    return candidate_by_field, target, receipt_by_field


def _drop_receipt(candidate):
    """Keep the declared marker and a readable report, without the composition-time receipt."""
    source = PinnedAddressSource("retained_addresses", "entity_address_unified", "address-edition")
    candidate["validation_json"] = {"address_source": asdict(source)}


def _closure():
    return {"owner_role": "protected_owner", "loader_roles": [], "reader_roles": ["runtime_reader"]}


def _held_by_name(_sql, requested):
    """Resolve every declared input by name to a plain table whose ids differ from the recorded ones."""
    return [
        {"role": held["role"], "relation_oid": 900 + index, "relfilenode": 950 + index}
        for index, held in enumerate(json.loads(requested))
    ]


def _connection():
    @asynccontextmanager
    async def transaction(**options):
        yield

    return SimpleNamespace(
        transaction=transaction,
        fetch=AsyncMock(side_effect=_held_by_name),
        fetchrow=AsyncMock(return_value=None),
        fetchval=AsyncMock(),
        execute=AsyncMock(return_value="UPDATE 1"),
    )


def _predecessor_lookup(monkeypatch, candidate):
    """Keep the unrelated non-FHIR predecessor lookup explicit in custody protocol tests."""
    predecessor = PinnedNetworkServingManifest(
        16, candidate["candidate_id"], candidate["schema_name"], 1, {}, 2, "f" * 64, 100
    )
    object.__setattr__(predecessor, "_custody_seal_sha256", "c" * 64)
    resolver = AsyncMock(return_value=predecessor)
    monkeypatch.setattr(preservation, "resolve_network_serving_manifest", resolver)
    return resolver


async def test_scope_and_actual_validation_update_preserve_exact_receipt(monkeypatch):
    candidate, target, receipt = _candidate("sealed")
    address = PinnedAddressSource("retained_addresses", "entity_address_unified", "address-edition")
    scope = validation._scope_report(candidate, address)
    assert scope["em_provider_inputs"] == receipt
    conn = _connection()
    monkeypatch.setattr(validation, "_lock_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(
        validation,
        "_membership_accounting",
        AsyncMock(
            return_value={
                name: 0
                for name in ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings")
            }
        ),
    )
    monkeypatch.setattr(
        validation,
        "_native_accounting",
        AsyncMock(return_value={name: 0 for name in ("raw_rows", "unknown_network_rows", "batch_rows", "batch_count")}),
    )
    report, prepared_work = await validation._validate_sealed(conn, target, address, "registry")
    assert prepared_work is None
    assert report["em_provider_inputs"] == receipt
    stored = json.loads(conn.execute.await_args.args[2])
    assert stored["em_provider_inputs"] == receipt and stored["source_generations"] == candidate["source_generations"]
    candidate.update(state="validated", validation_json=stored)
    assert validation._validated_replay(candidate, validation._scope_report(candidate, address)) == stored


@pytest.mark.parametrize("damage", ["marker_missing", "receipt_missing", "null_receipt", "hash", "shape", "extra"])
async def test_unpaired_or_malformed_recipe_is_refused_before_validation(damage):
    candidate, _, _ = _candidate()
    if damage == "marker_missing":
        candidate["source_generations"].pop("em_provider_inputs")
    if damage == "receipt_missing":
        candidate["validation_json"].pop("em_provider_inputs")
    if damage == "null_receipt":
        candidate["validation_json"]["em_provider_inputs"] = None
    if damage == "hash":
        candidate["source_generations"]["em_provider_inputs"] = "0" * 64
    if damage == "shape":
        candidate["validation_json"]["em_provider_inputs"]["relations"].pop()
    if damage == "extra":
        candidate["validation_json"]["em_provider_inputs"]["extra"] = True
    with pytest.raises(validation.NetworkMembershipValidationError):
        validation._em_provider_receipt(candidate)


async def test_legacy_candidate_does_not_require_provider_receipt_or_native_read():
    candidate, _, _ = _candidate()
    candidate["source_generations"].pop("em_provider_inputs")
    candidate["validation_json"] = {}
    assert await provider.require_readable_em_provider_inputs(object(), candidate) is None


@pytest.mark.parametrize(
    "failure",
    [asyncpg.CannotConnectNowError("temporary custody outage"), asyncpg.QueryCanceledError("custody query timeout")],
)
async def test_candidate_custody_preserves_actual_prepare_database_cause(failure):
    candidate, _, _ = _candidate()
    connection = _connection()
    connection.fetch.side_effect = failure
    with pytest.raises(ValueError, match="^Managed provider input custody is unavailable$") as caught:
        await provider.require_readable_em_provider_inputs(connection, candidate)
    assert caught.value.__cause__ is failure
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("state", ["ready", "published"])
@pytest.mark.parametrize("stale", [False, True])
async def test_actual_publication_gate_precedes_insert_or_retained_replay(monkeypatch, state, stale, search_index_gate):
    """The stored receipt must be present and well formed; its input tables are not captured again."""
    candidate, copy_target, _ = _candidate(state)
    conn = _connection()
    events = []
    search_index_gate.side_effect = lambda *args, **kwargs: events.append("search_indexes")
    monkeypatch.setattr(publication, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
    monkeypatch.setattr(publication, "_locked_candidate", AsyncMock(return_value=candidate))
    if stale:
        _drop_receipt(candidate)
    else:
        monkeypatch.setattr(publication, "_publication_readiness", lambda _candidate: ({}, {}, {}, _closure()))
    monkeypatch.setattr(publication, "verify_network_candidate_writer_closure", AsyncMock())
    resolve_predecessor = _predecessor_lookup(monkeypatch, candidate)
    native = AsyncMock()
    monkeypatch.setattr(provider, "prepare_em_provider_inputs", native)

    async def insert(*args):
        events.append("publish")
        return {"replayed": False}

    monkeypatch.setattr(publication, "_insert_manifest", insert)
    monkeypatch.setattr(
        publication, "_retained_manifest", lambda *args, **kwargs: events.append("replay") or {"replayed": True}
    )
    conn.fetchrow.return_value = {} if state == "published" else None
    if stale:
        with pytest.raises(validation.NetworkMembershipValidationError, match="provider inputs"):
            await publication._publish_candidate(conn, copy_target, '"registry"')
        assert events == []
        conn.fetchrow.assert_not_awaited()
    else:
        assert (await publication._publish_candidate(conn, copy_target, '"registry"'))["replayed"] == (
            state == "published"
        )
        assert events == (["replay"] if state == "published" else ["search_indexes", "publish"])
    native.assert_not_awaited()
    if not stale and state == "ready":
        resolve_predecessor.assert_awaited_once_with(conn, generation_id=16, control_schema="registry")
    else:
        resolve_predecessor.assert_not_awaited()
    conn.execute.assert_not_awaited()
    assert conn.fetch.await_count == search_index_gate.await_count == int(not stale and state == "ready")


async def test_phase_owned_publication_starts_rr_before_control_locks(monkeypatch):
    candidate, target, _ = _candidate()
    options_by_name = {}
    conn = _connection()

    @asynccontextmanager
    async def transaction(**options):
        options_by_name.update(options)
        yield

    conn.transaction = transaction

    async def controls(*args):
        assert options_by_name == {"isolation": "repeatable_read"}
        return 2, 16

    monkeypatch.setattr(pipeline, "_lock_publication_controls", controls)
    monkeypatch.setattr(pipeline, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "freeze_network_candidate_writers", AsyncMock(return_value=_closure()))
    monkeypatch.setattr(pipeline, "_retain_closure", AsyncMock())
    publish = AsyncMock(return_value={"generation_id": 17})
    monkeypatch.setattr(pipeline, "publish_network_candidate", publish)
    assert await pipeline._publish_phase(conn, target, {}, "registry") == {"generation_id": 17}
    publish.assert_awaited_once()


async def test_pipeline_retained_replay_checks_custody_before_serving_reads(monkeypatch):
    candidate, target, _ = _candidate("published")
    _drop_receipt(candidate)
    conn = _connection()
    monkeypatch.setattr(pipeline, "_inspect_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_scope_report", lambda *args: {})
    monkeypatch.setattr(pipeline, "_validated_replay", lambda *args: None)
    with pytest.raises(validation.NetworkMembershipValidationError, match="provider inputs"):
        await pipeline._published_replay(conn, target, object(), {}, "registry")
    conn.fetchrow.assert_not_awaited()
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize("head", [16, 17])
@pytest.mark.parametrize("stale", [False, True])
async def test_reactivation_checks_custody_before_cas_even_on_noop(monkeypatch, head, stale, search_index_gate):
    candidate, _, _ = _candidate("published")
    conn = _connection()
    conn.fetchrow.return_value = candidate
    pinned = PinnedNetworkServingManifest(
        17, candidate["candidate_id"], candidate["schema_name"], 1, candidate["source_generations"], 2, "f" * 64, 100
    )
    monkeypatch.setattr(reactivation, "_lock_publication_controls", AsyncMock(return_value=(2, head)))
    monkeypatch.setattr(reactivation, "resolve_network_serving_manifest", AsyncMock(return_value=pinned))
    if stale:
        _drop_receipt(candidate)
    else:
        monkeypatch.setattr(reactivation, "_publication_readiness", lambda *args: ({}, {}, {}, _closure()))
    native = AsyncMock()
    monkeypatch.setattr(provider, "prepare_em_provider_inputs", native)
    search_index_gate.side_effect = lambda *args, **kwargs: conn.execute.assert_not_awaited()
    if stale:
        with pytest.raises(validation.NetworkMembershipValidationError, match="provider inputs"):
            await reactivation._reactivate(conn, '"registry"', 17, head, 2)
        conn.execute.assert_not_awaited()
    else:
        result = await reactivation._reactivate(conn, '"registry"', 17, head, 2)
        assert result.changed == (head != 17)
        statements = [call.args[0].split()[0] for call in conn.execute.await_args_list]
        assert statements == (["UPDATE"] if head != 17 else []) and conn.fetch.await_count == int(head != 17)
    native.assert_not_awaited()
    assert search_index_gate.await_count == int(not stale and head != 17)


@pytest.mark.parametrize("site", ["publication", "reactivation"])
@pytest.mark.parametrize("absent_role", ["npi", "tiger.zip_state"])
async def test_head_stays_when_declared_input_is_unreadable(monkeypatch, site, absent_role):
    """The shared catalog check returns no row for a table that no longer carries its name or lacks a read column."""
    candidate, copy_target, _ = _candidate("ready" if site == "publication" else "published")
    conn = _connection()
    conn.fetch.side_effect = lambda *requested: [
        held for held in _held_by_name(*requested) if held["role"] != absent_role
    ]
    conn.fetchrow.return_value = candidate if site == "reactivation" else None
    pinned = PinnedNetworkServingManifest(
        17, candidate["candidate_id"], candidate["schema_name"], 1, candidate["source_generations"], 2, "f" * 64, 100
    )
    for module in (publication, reactivation):
        monkeypatch.setattr(module, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
        monkeypatch.setattr(module, "_publication_readiness", lambda *args: ({}, {}, {}, _closure()))
    monkeypatch.setattr(publication, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(publication, "verify_network_candidate_writer_closure", AsyncMock())
    insert = AsyncMock()
    monkeypatch.setattr(publication, "_insert_manifest", insert)
    _predecessor_lookup(monkeypatch, candidate)
    monkeypatch.setattr(reactivation, "resolve_network_serving_manifest", AsyncMock(return_value=pinned))
    with pytest.raises(ValueError, match="^Managed provider input custody is unavailable$") as caught:
        if site == "publication":
            await publication._publish_candidate(conn, copy_target, '"registry"')
        else:
            await reactivation._reactivate(conn, '"registry"', 17, 16, 2)
    assert caught.value.__cause__ is None and conn.fetch.await_args.args[0] is provider._API_HEAPS_SQL
    insert.assert_not_awaited()
    conn.execute.assert_not_awaited()


async def test_trusted_refresh_requires_explicit_family_and_replaces_inherited_pin(monkeypatch):
    candidate, _, _ = _candidate()
    pinned = PinnedNetworkServingManifest(
        17, candidate["candidate_id"], candidate["schema_name"], 1, candidate["source_generations"], 2, "f" * 64, 100
    )
    old = family()
    fresh = provider.EMProviderInputs(tuple(replace(item, generation_id="fresh-edition") for item in old.relations))
    address = composition.RegistryCompositionAddressSources(
        PinnedAddressSource("retained", "entity_address_unified", "edition"), em_provider_inputs=old
    )
    compose = AsyncMock(return_value=("target", "address"))
    monkeypatch.setattr(composition, "compose_registry_membership_candidate", compose)
    request = UUID(int=9)
    assert await composition.compose_registry_provider_refresh(
        object(), request, 2, pinned, address, fresh, {}, "registry"
    ) == ("target", "address")
    kwargs = compose.await_args.kwargs
    assert kwargs["expected_head"] == 17 and kwargs["source_manifest"] is pinned
    assert kwargs["address_sources"].em_provider_inputs == fresh
    assert address.em_provider_inputs == old
    compose.reset_mock()
    with pytest.raises(composition.RegistryCompositionError):
        await composition.compose_registry_provider_refresh(object(), request, 2, pinned, address, None, {}, "registry")
    compose.assert_not_awaited()


@pytest.mark.parametrize("is_database_failure", [False, True])
async def test_reactivation_wrapper_preserves_explicit_custody_classification(monkeypatch, is_database_failure):
    from process.registry_publication_consumer import _has_database_cause

    storage_failure = asyncpg.CannotConnectNowError("temporary custody outage")

    async def refused(*arguments):
        raise ValueError("controls refused") from (storage_failure if is_database_failure else None)

    monkeypatch.setattr(reactivation, "_reactivate", refused)
    connection = _connection()
    connection.is_in_transaction = lambda: True
    with pytest.raises(
        reactivation.NetworkServingReactivationError, match="network_reactivation_controls_unavailable"
    ) as caught:
        await reactivation.reactivate_network_serving_generation(
            connection, generation_id=17, expected_head=16, expected_approved_revision=2, control_schema="registry"
        )
    assert isinstance(caught.value.__cause__, ValueError)
    assert _has_database_cause(caught.value) is is_database_failure
    assert caught.value.__cause__.__cause__ is (storage_failure if is_database_failure else None)


def _seal():
    return {
        "seal_sha256": "c" * 64,
        "receipt": {
            "index_definition_sha256": "d" * 64,
            "control": {
                "database_name": "synthetic_registry",
                "database_oid": 100,
                "schema_oid": 101,
                "owner_oid": 104,
                "candidate_table_oid": 102,
                "seal_table_oid": 103,
            },
            "closure": {
                "owner_role": "final_owner",
                "loader_roles": [],
                "reader_roles": [f"runtime_reader_{n}" for n in range(128)],
                "relation_oids": {"entity_address_unified": 222},
            },
        },
    }


@pytest.mark.parametrize("state", ["ready", "published"])
@pytest.mark.parametrize("changed", [False, True])
async def test_protected_seal_gates_publication_without_final_owner_assumption(
    monkeypatch, state, changed, search_index_gate
):
    candidate, copy_target, _ = _candidate(state)
    conn = _connection()
    native = AsyncMock(return_value=_seal(), side_effect=ValueError("changed protected seal") if changed else None)
    monkeypatch.setattr(publication, "verify_network_candidate_custody", native)
    monkeypatch.setattr(publication, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
    monkeypatch.setattr(publication, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(publication, "_publication_readiness", lambda _candidate: ({}, {}, {}, _closure()))
    resolve_predecessor = _predecessor_lookup(monkeypatch, candidate)
    old_native = AsyncMock(side_effect=AssertionError("sealed candidate must not assume final owner"))
    monkeypatch.setattr(publication, "verify_network_candidate_writer_closure", old_native)
    insert = AsyncMock(return_value={"replayed": False})
    monkeypatch.setattr(publication, "_insert_manifest", insert)
    monkeypatch.setattr(publication, "_retained_manifest", lambda *args, **kwargs: {"replayed": True})
    conn.fetchrow.return_value = {} if state == "published" else None
    if changed:
        with pytest.raises(ValueError, match="changed protected seal"):
            await publication._publish_candidate(conn, copy_target, '"registry"')
        insert.assert_not_awaited()
        conn.fetchrow.assert_not_awaited()
    else:
        published_manifest = await publication._publish_candidate(conn, copy_target, '"registry"')
        assert published_manifest["replayed"] == (state == "published")
    if not changed and state == "ready":
        resolve_predecessor.assert_awaited_once_with(conn, generation_id=16, control_schema="registry")
    else:
        resolve_predecessor.assert_not_awaited()
    native.assert_awaited_once_with(conn, copy_target, control_schema="registry", required=False)
    old_native.assert_not_awaited()
    conn.execute.assert_not_awaited()
    assert search_index_gate.await_count == int(not changed and state == "ready")


@pytest.mark.parametrize("failure", [None, ValueError("seal refused"), asyncio.CancelledError()])
async def test_seal_callback_runs_after_committed_prepare_and_before_publish(monkeypatch, failure):
    candidate, copy_target, _ = _candidate()
    connection = SimpleNamespace(is_in_transaction=lambda: False)
    events = []

    async def prepare(*args, **kwargs):
        events.append("committed_prepare")
        return candidate

    async def seal(actual):
        assert actual is copy_target and not connection.is_in_transaction()
        events.append("seal")
        if failure is not None:
            raise failure

    async def publish(*args):
        events.append("publish")
        return {"generation_id": 17}

    monkeypatch.setattr(pipeline, "prepare_network_candidate", prepare)
    final_publish = AsyncMock(side_effect=publish)
    monkeypatch.setattr(pipeline, "_publish_phase", final_publish)
    arguments_by_name = dict(
        owner_role="protected_owner",
        loader_roles=(),
        reader_roles=("runtime_reader",),
        control_schema="registry",
        seal_candidate=seal,
    )
    if failure is None:
        assert await pipeline.prepare_and_publish_network_candidate(
            connection, copy_target, object(), **arguments_by_name
        ) == {"generation_id": 17}
        assert events == ["committed_prepare", "seal", "publish"]
    else:
        with pytest.raises(type(failure)):
            await pipeline.prepare_and_publish_network_candidate(connection, copy_target, object(), **arguments_by_name)
        assert events == ["committed_prepare", "seal"]
        final_publish.assert_not_awaited()


async def test_published_replay_never_reinvokes_seal_callback(monkeypatch):
    candidate, target, _ = _candidate("published")
    monkeypatch.setattr(pipeline, "prepare_network_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_published_replay", AsyncMock(return_value={"replayed": True}))
    seal = AsyncMock()
    assert await pipeline.prepare_and_publish_network_candidate(
        object(), target, object(), owner_role="protected_owner", loader_roles=(), reader_roles=(), seal_candidate=seal
    ) == {"replayed": True}
    seal.assert_not_awaited()


async def test_sealed_ready_retry_skips_owner_freeze_and_commits_no_new_metadata(monkeypatch):
    candidate, target, _ = _candidate()
    conn = _connection()
    monkeypatch.setattr(publication, "verify_network_candidate_custody", AsyncMock(return_value=_seal()))
    monkeypatch.setattr(pipeline, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
    freeze, retain = AsyncMock(), AsyncMock()
    monkeypatch.setattr(pipeline, "freeze_network_candidate_writers", freeze)
    monkeypatch.setattr(pipeline, "_retain_closure", retain)
    monkeypatch.setattr(pipeline, "publish_network_candidate", AsyncMock(return_value={"generation_id": 17}))
    monkeypatch.setattr(pipeline, "_publication_readiness", lambda *args: ({}, {}, {}, _closure()))
    roles_by_name = {"owner_role": "protected_owner", "loader_roles": (), "reader_roles": ("runtime_reader",)}
    assert await pipeline._publish_phase(conn, target, roles_by_name, "registry") == {"generation_id": 17}
    freeze.assert_not_awaited()
    retain.assert_not_awaited()
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize("sealed", [False, True])
async def test_replay_retains_legacy_scans_but_sealed_custody_uses_native_proof(monkeypatch, sealed):
    candidate, target, _ = _candidate("published")
    conn = _connection()
    monkeypatch.setattr(pipeline, "_inspect_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_scope_report", lambda *args: {})
    monkeypatch.setattr(pipeline, "_validated_replay", lambda *args: None)
    monkeypatch.setattr(
        pipeline, "_publication_readiness", lambda *args: ({}, {}, {"index_definition_sha256": "d" * 64}, _closure())
    )
    monkeypatch.setattr(
        publication, "verify_network_candidate_custody", AsyncMock(return_value=_seal() if sealed else None)
    )
    monkeypatch.setattr(pipeline, "_read_manifest", AsyncMock(return_value={}))
    pinned = Mock()
    monkeypatch.setattr(pipeline, "_pinned_manifest", pinned)
    scans = [AsyncMock(), AsyncMock(), AsyncMock(return_value="d" * 64)]
    for name, operation in zip(["_matching_inputs", "_source_semantics", "_index_digest"], scans, strict=True):
        monkeypatch.setattr(pipeline, name, operation)
    monkeypatch.setattr(pipeline, "_retained_manifest", lambda *args, **kwargs: {"replayed": True})
    conn.fetchrow.return_value = {"generation_id": 17}
    roles_by_name = {"owner_role": "protected_owner", "loader_roles": (), "reader_roles": ("runtime_reader",)}
    source = PinnedAddressSource("retained", "entity_address_unified", "address-edition")
    assert await pipeline._published_replay(conn, target, source, roles_by_name, "registry") == {"replayed": True}
    assert [operation.await_count for operation in scans] == ([0, 0, 0] if sealed else [1, 1, 1])
    pinned.assert_called_once_with({}, custody=_seal() if sealed else None)
    conn.execute.assert_not_awaited()


def _serving_entry(monkeypatch):
    candidate, _, _ = _candidate("published")
    logical = _closure()
    logical["relation_oids"] = {"entity_address_unified": 111}
    readiness = ({}, {}, {"index_definition_sha256": "d" * 64}, logical)
    retained_by_name = dict(
        generation_id=17,
        candidate_id=candidate["candidate_id"],
        schema_name=candidate["schema_name"],
        schema_revision=1,
        source_generations=candidate["source_generations"],
        approved_custom_revision=2,
        manifest_sha256="f" * 64,
    )
    monkeypatch.setattr(serving, "_publication_readiness", lambda actual: readiness)
    retained_call = Mock(return_value=retained_by_name)
    monkeypatch.setattr(serving, "_retained_manifest", retained_call)
    legacy_native = Mock(return_value=111)
    monkeypatch.setattr(serving, "_verified_writer_closure", legacy_native)
    return (
        candidate,
        {"candidate_json": json.dumps(candidate), "manifest_json": "{}", "catalog_json": "{}"},
        readiness,
        retained_call,
        legacy_native,
    )


@pytest.mark.parametrize("sealed", [False, True])
async def test_serving_pin_keeps_logical_manifest_and_uses_verified_physical_owner(monkeypatch, sealed):
    candidate, entry, readiness, retained, legacy = _serving_entry(monkeypatch)
    conn = SimpleNamespace(is_in_transaction=lambda: True)
    monkeypatch.setattr(serving, "_read_manifest", AsyncMock(return_value=entry))
    native = AsyncMock(return_value=_seal() if sealed else None)
    monkeypatch.setattr(publication, "verify_network_candidate_custody", native)
    pinned = await serving.resolve_network_serving_manifest(conn, generation_id=17, control_schema="registry")
    assert pinned.address_table_oid == (222 if sealed else 111)
    assert getattr(pinned, "_custody_seal_sha256", None) == ("c" * 64 if sealed else None)
    assert pinned.custody_control == (_seal()["receipt"]["control"] if sealed else None)
    assert [(relation.table_name, relation.table_oid) for relation in pinned.candidate_relation_pins] == [
        ("entity_address_unified", 222 if sealed else 111)
    ]
    assert set(asdict(pinned)) == {
        "generation_id",
        "candidate_id",
        "schema_name",
        "schema_revision",
        "source_generations",
        "approved_custom_revision",
        "manifest_sha256",
        "address_table_oid",
    }
    assert not hasattr(replace(pinned), "_custody_seal_sha256")
    assert retained.call_args.args[1] is readiness and readiness[3]["owner_role"] == "protected_owner"
    assert legacy.call_count == int(not sealed)
    assert native.await_args.kwargs == {"control_schema": "registry", "required": False}
    assert native.await_args.args[1].candidate_id == candidate["candidate_id"]


async def test_changed_native_proof_refuses_reads_and_reactivation(monkeypatch):
    candidate, entry, _, _, legacy = _serving_entry(monkeypatch)
    conn = _connection()
    conn.is_in_transaction = lambda: True
    conn.fetchrow.return_value = candidate
    monkeypatch.setattr(serving, "_read_manifest", AsyncMock(return_value=entry))
    monkeypatch.setattr(reactivation, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
    changed = _seal()
    changed["receipt"]["index_definition_sha256"] = "e" * 64
    monkeypatch.setattr(publication, "verify_network_candidate_custody", AsyncMock(return_value=changed))
    with pytest.raises(serving.NetworkServingReadUnavailable, match="index definitions"):
        await serving.resolve_network_serving_manifest(conn, generation_id=17, control_schema="registry")
    with pytest.raises(reactivation.NetworkServingReactivationError):
        await reactivation.reactivate_network_serving_generation(
            conn, generation_id=17, expected_head=16, expected_approved_revision=2, control_schema="registry"
        )
    conn.execute.assert_not_awaited()
    legacy.assert_not_called()


@pytest.mark.parametrize("phase", ["prepare", "publish"])
@pytest.mark.parametrize("changed_roles", [False, True])
async def test_sealed_ready_phase_keeps_configured_roles_and_skips_mutating_preparation(
    monkeypatch, phase, changed_roles
):
    candidate, copy_target, _ = _candidate()
    conn = _connection()
    observed_options = []

    @asynccontextmanager
    async def transaction(**options):
        observed_options.append(options)
        yield

    conn.transaction = transaction
    monkeypatch.setattr(publication, "verify_network_candidate_custody", AsyncMock(return_value=_seal()))
    monkeypatch.setattr(pipeline, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_scope_report", lambda *args: {})
    monkeypatch.setattr(pipeline, "_validated_replay", lambda *args: None)
    monkeypatch.setattr(pipeline, "_publication_readiness", lambda *args: ({}, {}, {}, _closure()))
    monkeypatch.setattr(pipeline, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
    mutators = [AsyncMock() for _ in range(3)]
    for name, operation in zip(
        ["freeze_network_candidate_writers", "_retain_closure", "prepare_network_serving_indexes"],
        mutators,
        strict=True,
    ):
        monkeypatch.setattr(pipeline, name, operation)
    publish = AsyncMock(return_value={"generation_id": 17})
    monkeypatch.setattr(pipeline, "publish_network_candidate", publish)
    roles_by_name = {
        "owner_role": "changed_owner" if changed_roles else "protected_owner",
        "loader_roles": (),
        "reader_roles": ("runtime_reader",),
    }
    operation = (
        pipeline._prepare_phase(conn, copy_target, object(), roles_by_name, "registry")
        if phase == "prepare"
        else pipeline._publish_phase(conn, copy_target, roles_by_name, "registry")
    )
    if changed_roles:
        with pytest.raises(pipeline.NetworkMembershipPipelineError, match="Configured writer roles"):
            await operation
        publish.assert_not_awaited()
    else:
        await operation
        assert publish.await_count == int(phase == "publish")
    assert observed_options == [{"isolation": "repeatable_read"}]
    assert all(mutator.await_count == 0 for mutator in mutators)
    conn.execute.assert_not_awaited()
