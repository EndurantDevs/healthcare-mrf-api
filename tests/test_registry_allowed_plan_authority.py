# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure allowed review lineage, custody and managed admission regressions."""

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_allowed_amount_policy as allowed
from process import registry_ptg_allowed_plan_contract as contract
from process import registry_ptg_allowed_plan_source as source
from process import registry_ptg_published_plan_contract as published
from process import registry_ptg_published_plan_scope as review
from process import registry_ptg_scope_engine as engine
from process import registry_published_plan_binding_refs as references
from process.ptg_parts import result_archive_allowed_authority as custody
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthorityError
from tests import test_provider_service_source_policy as sources
from tests.test_pricing_api import _canonical_release_selection
from tests.test_registry_ptg_scope_engine import _ownership
from tests.test_registry_published_plan_binding_refs import reviewed_binding


def allowed_inventory():
    template, document = reviewed_binding()
    command = document["document"]["command"]
    version = command["file_versions"][0]
    manifest_by_field = {
        "snapshot_id": "snapshot_example",
        "import_month": "2026-01-01",
        "source_key": "source_example",
        "activation_status": "not_applicable",
        "data_domains": ["allowed_amounts"],
        "allowed_amount_index": {
            "contract": "ptg2_allowed_amounts_v1",
            "arch_version": "postgres_binary_v3",
            "storage": "postgresql",
            "data_domain": "allowed_amounts",
            "source_key": "source_example",
            "snapshot_scoped": True,
            "allowed_amount_evidence": True,
            "allowed_amount_payments": 2,
            "allowed_amount_provider_payments": 3,
        },
        "source_file_versions": [
            {
                "engine_source_file_version_id": version["source_file_version_id"],
                "engine_source_identity_hash": version["source_identity_sha256"],
                "raw_sha256": version["raw_sha256"],
            }
        ],
    }
    snapshot_by_field = {
        "snapshot_id": "snapshot_example",
        "import_run_id": "run_example",
        "import_month": "2026-01-01",
        "run_import_month": "2026-01-01",
        "run_source_key": "source_example",
        "status": "published",
        "manifest": manifest_by_field,
    }
    plans = [{"plan_id": command["plan_id"], "plan_market_type": command["plan_market_type"]}]
    versions = [{**version, "file_id": 17, "identity_hash": version["source_identity_sha256"]}]
    return snapshot_by_field, plans, versions


def allowed_case():
    snapshot_by_field, plans, versions = allowed_inventory()
    inventory = source.validated_allowed_inventory(snapshot_by_field, plans, versions)
    mapping, stored = reviewed_binding()
    command = stored["document"]["command"]
    command.update(
        review_type=contract.REVIEW_TYPE,
        selection_mode=contract.SELECTION_MODE,
        published_identity=inventory["identity"],
    )
    command["coordinates"].update(
        producer_id=contract.AUTHORITY_CONTRACT, edition_id=inventory["identity"]["snapshot_manifest_sha256"]
    )
    command = published.validated_command(command)
    authority = custody.AllowedSourceAuthority(
        "registry_ptg_published_review_" + command["scope_id"].replace("-", ""), inventory["identity"]
    )
    inventory["authority"] = authority.as_dict()
    evidence_by_field = {
        "source_authority": authority.as_dict(),
        "selected_source_keys": [17],
        "selection_mode": contract.SELECTION_MODE,
    }
    stored["document"] = review._document(command, stored["document"]["actor"], evidence_by_field)
    stored["approval_sha256"] = review._digest(stored["document"])
    mapping.update(command["coordinates"])
    mapping["source_scope_json"].update(
        review_type=contract.REVIEW_TYPE,
        selection_mode=contract.SELECTION_MODE,
        approval_sha256=stored["approval_sha256"],
    )
    mapping["evidence_sha256"] = stored["approval_sha256"]
    return mapping, stored, snapshot_by_field["manifest"], inventory


def test_allowed_contract_uses_real_identity_and_closed_role_reference():
    mapping, stored, _, _ = allowed_case()
    command = stored["document"]["command"]
    assert (
        engine._intent({name: command[name] for name in published.INTENT_FIELDS})["review_type"] == contract.REVIEW_TYPE
    )
    assert engine._full(command) == command
    assert not {"snapshot_key", "layout_mapping_digest", "map_digest", "finalizer_map_digest"} & set(
        command["published_identity"]
    )
    parsed = references._review_commands(json.dumps([stored]))
    references._require_reference(mapping, parsed[(stored["scope_id"], stored["approval_sha256"])])
    assert references.published_plan_binding_requests(json.dumps([mapping]).encode()) == [mapping]


@pytest.mark.parametrize(
    "damage", ["extra_v4", "file_digest", "month", "source", "role", "selection", "missing_file", "client"]
)
def test_closed_allowed_command_rejects_substitution(damage):
    _, stored, _, _ = allowed_case()
    command = copy.deepcopy(stored["document"]["command"])
    mutations_by_kind = {
        "extra_v4": lambda: command["published_identity"].update(snapshot_key=17),
        "file_digest": lambda: command["published_identity"].update(file_versions_sha256="f" * 64),
        "month": lambda: command["ownership"].update(import_month="2026-02-01"),
        "source": lambda: command["source"].update(binding_source_key="other"),
        "role": lambda: command.update(review_type=published.REVIEW_TYPE),
        "selection": lambda: command.update(selection_mode=published.SELECTION_MODE),
        "client": lambda: command["ownership"].update(client_id="other_client"),
        "missing_file": lambda: command.update(file_versions=[]),
    }
    mutations_by_kind[damage]()
    with pytest.raises(ValueError):
        published.validated_command(command)


@pytest.mark.parametrize(
    "damage",
    ["null_version", "raw", "missing_identity", "omitted_file", "duplicate", "no_plan", "source", "serving", "flag"],
)
def test_complete_allowed_inventory_rejects_missing_or_changed_lineage(damage):
    snapshot_by_field, plans, versions = allowed_inventory()
    mutations_by_kind = {
        "null_version": lambda: versions[0].update(source_file_version_id=None),
        "raw": lambda: versions[0].update(raw_sha256="f" * 64),
        "missing_identity": lambda: versions[0].update(identity_hash=None),
        "omitted_file": lambda: snapshot_by_field["manifest"]["source_file_versions"].append({}),
        "duplicate": lambda: versions.append(dict(versions[0])),
        "no_plan": plans.clear,
        "source": lambda: snapshot_by_field.update(run_source_key="other"),
        "serving": lambda: snapshot_by_field["manifest"].update(serving_index={}),
        "flag": lambda: snapshot_by_field["manifest"]["allowed_amount_index"].update(snapshot_scoped=1),
    }
    mutations_by_kind[damage]()
    with pytest.raises((ValueError, KeyError, TypeError)):
        source.validated_allowed_inventory(snapshot_by_field, plans, versions)


@pytest.mark.asyncio
async def test_preview_resolves_complete_role_inventory_after_protected_privileges(monkeypatch):
    _, stored, _, inventory = allowed_case()
    command = stored["document"]["command"]
    protected = AsyncMock()
    inventory_loader = AsyncMock(return_value=inventory)
    monkeypatch.setattr(review, "require_published_lock_privileges", protected)
    monkeypatch.setattr(source, "allowed_plan_inventory", inventory_loader)
    actual, authority, evidence_by_field = await review.resolve_published_plan(
        None, "synthetic_ptg", {name: command[name] for name in published.INTENT_FIELDS}, command["ownership"], {}
    )
    assert actual == command and authority == inventory["authority"]
    assert evidence_by_field["selection_mode"] == contract.SELECTION_MODE
    protected.assert_awaited_once_with(None, "synthetic_ptg", pin_writer=False)
    protected.side_effect = ValueError("unsafe role")
    inventory_loader.reset_mock()
    with pytest.raises(ValueError):
        await review.resolve_published_plan(
            None, "synthetic_ptg", {name: command[name] for name in published.INTENT_FIELDS}, command["ownership"], {}
        )
    inventory_loader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("pins", [[], [{"snapshot_id": "other", "reason": "other"}]])
async def test_allowed_retained_read_refuses_missing_or_substituted_pin(monkeypatch, pins):
    _, _, _, inventory = allowed_case()
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(return_value=SimpleNamespace(one=lambda: ("0", "0")))
    )
    monkeypatch.setattr(custody, "load_allowed_plan_inventory", AsyncMock(return_value=inventory))
    monkeypatch.setattr(custody, "_pin_rows", AsyncMock(return_value=pins))
    with pytest.raises(PtgResultArchiveSourceAuthorityError, match="pin"):
        await custody.lock_allowed_source_authority(
            session, schema_name="synthetic_ptg", authority=inventory["authority"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("excluded,expected", [((), True), ((7,), False)])
async def test_pure_allowed_review_admits_exact_role_before_readiness(monkeypatch, excluded, expected):
    def reviewed_source():
        mapping, stored, manifest_by_field, _ = allowed_case()
        mapping.update(
            archived=False, binding_id="00000000-0000-0000-0000-000000000050", revision=1, binding_key="e" * 64
        )
        return mapping, stored, manifest_by_field

    monkeypatch.setattr(sources, "_reviewed_source", reviewed_source)
    fixture = sources._fixture(monkeypatch, excluded=excluded)
    binding = replace(fixture.binding, role="allowed_amounts")
    selection = _canonical_release_selection((binding,))
    monkeypatch.setattr(allowed, "current_network_address_scope", lambda: fixture.scope)
    readiness = AsyncMock(return_value=True)
    monkeypatch.setattr(allowed, "has_allowed_amount_binding_coverage", readiness)
    assert await allowed.admitted_allowed_amount_bindings(fixture.session, selection) == (
        (binding,) if expected else ()
    )
    assert readiness.await_count == int(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["plan_id", "engine_source_file_version_id", "engine_source_identity_hash", "client_id", "source_key"]
)
async def test_preview_refuses_wrong_plan_or_source_ownership(monkeypatch, changed):
    _, stored, _, inventory = allowed_case()
    command = stored["document"]["command"]
    monkeypatch.setattr(review, "require_published_lock_privileges", AsyncMock())
    monkeypatch.setattr(source, "allowed_plan_inventory", AsyncMock(return_value=inventory))
    intent_by_field = {name: command[name] for name in published.INTENT_FIELDS}
    ownership_by_field = dict(command["ownership"])
    if changed == "plan_id":
        intent_by_field[changed] = "OTHER_PLAN"
    else:
        ownership_by_field[changed] = "other" if changed != "engine_source_identity_hash" else "f" * 64
    with pytest.raises((ValueError, review.RegistryPTGProducerScopeError)):
        await review.resolve_published_plan(None, "synthetic_ptg", intent_by_field, ownership_by_field, {})


@pytest.mark.asyncio
async def test_allowed_read_reconstructs_protected_review_and_owned_pin(monkeypatch):
    _, stored, _, inventory = allowed_case()
    command = stored["document"]["command"]
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(
                mappings=lambda: SimpleNamespace(
                    one_or_none=lambda: {
                        "approval_sha256": stored["approval_sha256"],
                        "approval_json": stored["document"],
                    }
                )
            )
        )
    )
    protected = AsyncMock(return_value='"registry_control".registry_ptg_published_plan_scope')
    monkeypatch.setattr(review, "_protected_store", protected)
    monkeypatch.setattr(review, "require_published_lock_privileges", AsyncMock())
    monkeypatch.setattr(source, "allowed_plan_inventory", AsyncMock(return_value=inventory))
    retained = custody.validated_allowed_authority(inventory["authority"])
    lock = AsyncMock(return_value=retained)
    monkeypatch.setattr(custody, "lock_allowed_source_authority", lock)
    actual = await review.read_registry_ptg_published_plan_scope(
        session,
        scope_id=command["scope_id"],
        client_id=command["client_id"],
        approval_sha256=stored["approval_sha256"],
        store=object(),
    )
    assert actual["command"] == command
    assert protected.await_args.kwargs == {"write": False, "table_name": review.TABLE}
    lock.assert_awaited_once_with(session, schema_name="synthetic_ptg", authority=inventory["authority"])


@pytest.mark.asyncio
async def test_allowed_lineage_query_uses_both_tables_and_preserves_missing_references():
    snapshot_by_field, plans, versions = allowed_inventory()
    batches = [[snapshot_by_field], plans, versions]

    async def execute(query, parameters):
        batch = batches.pop(0)
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: batch))

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    actual = await source.load_allowed_plan_inventory(session, "synthetic_ptg", "snapshot_example")
    assert actual["source_keys"] == [17]
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert "FOR KEY SHARE OF snapshot,internal_run" in statements[0]
    assert "ptg2_allowed_amount_plan" in statements[2] and "ptg2_allowed_amount_item" in statements[2]
    assert "LEFT JOIN" in statements[2] and "ptg2_source_identity" in statements[2]
    assert all("ptg2_v3" not in statement and "ptg2_v4" not in statement for statement in statements)


@pytest.mark.asyncio
async def test_allowed_approval_freshly_authorizes_before_persisting_owned_pin(monkeypatch):
    from contextlib import asynccontextmanager

    _, stored, _, inventory = allowed_case()
    command, actor = stored["document"]["command"], stored["document"]["actor"]
    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield session

    monkeypatch.setattr(review, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(review, "_protected_store", AsyncMock(return_value='"registry_control".review'))
    monkeypatch.setattr(review, "require_published_lock_privileges", AsyncMock())
    monkeypatch.setattr(source, "allowed_plan_inventory", AsyncMock(return_value=inventory))
    monkeypatch.setattr(review, "_approved_association", AsyncMock())
    parameters = review._parameters(stored["document"])
    monkeypatch.setattr(
        review,
        "_existing",
        AsyncMock(
            side_effect=[[], [{"approval_sha256": parameters["approval_sha256"], "approval_json": stored["document"]}]]
        ),
    )
    approval_events = []

    async def authorize(*args, **kwargs):
        approval_events.append("fresh_authority")

    async def pin(*args, **kwargs):
        approval_events.append("owned_pin")
        assert kwargs["authority"] == inventory["authority"]

    monkeypatch.setattr(custody, "commit_allowed_source_authority", AsyncMock(side_effect=pin))
    service = SimpleNamespace(
        approval_sessions=object(),
        store=SimpleNamespace(control_schema="registry_control"),
        schema_name="synthetic_ptg",
        authority=SimpleNamespace(authorize=AsyncMock(side_effect=authorize)),
    )
    state_by_field = {"commit_started": False}
    receipt = await review.approve_published_plan(service, {"command": command, "actor": actor}, 10**20, state_by_field)
    assert receipt["approval_sha256"] == stored["approval_sha256"] and state_by_field["commit_started"] is True
    assert approval_events == ["fresh_authority", "owned_pin"]
    assert "INSERT INTO" in str(session.execute.await_args.args[0])


def test_forward_receipt_migration_preserves_invariants_and_closed_role_pairs(monkeypatch):
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path

    from process.network_approved_source_bindings import _VALIDATION_SQL

    path = Path(__file__).parents[1] / "alembic/versions/20261016070000_registry_allowed_plan_scope.py"
    specification = spec_from_file_location("allowed_review_migration", path)
    migration = module_from_spec(specification)
    specification.loader.exec_module(migration)
    statements = migration._ddl("synthetic_registry", include_allowed=True)
    assert len(statements) == 3 and "ACCESS EXCLUSIVE" in statements[0]
    forward = statements[-1]
    assert "published_allowed_amount_plan" in forward and "published_complete_snapshot_plan" in forward
    for invariant in (
        "jsonb_array_length",
        "'platform_admin'",
        "'system'",
        "'ownership'",
        "'source_count'",
        "scope_id::text",
        "idempotency_key",
    ):
        assert invariant in forward
    assert "GRANT" not in forward and "DROP TABLE" not in forward
    assert "('published_allowed_amount_plan','complete_allowed_snapshot_source_set')" in _VALIDATION_SQL
    calls = []
    monkeypatch.setattr(migration.op, "execute", calls.append)
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(scalar=lambda query: True))
    with pytest.raises(RuntimeError, match="Retained allowed"):
        migration.downgrade()
    assert len(calls) == 1 and "LOCK TABLE" in calls[0]


@pytest.mark.asyncio
async def test_allowed_binding_reference_uses_protected_set_loader():
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
    from tests.test_registry_published_plan_binding_refs import Driver

    mapping, stored, _, _ = allowed_case()
    driver = Driver(stored)
    await references.require_published_plan_binding_references(
        driver,
        '"synthetic_control"',
        json.dumps([mapping] * 64).encode(),
        RegistryPTGProducerScopeStore("scope_owner", "scope_approver", "synthetic_control"),
    )
    assert driver.fetchrow.await_count == 1 and driver.rollback_count == 0
    assert driver.fetchval.await_args.args[-1] == "ordinary,pg_catalog"


def test_allowed_review_authority_cannot_reuse_another_scope_pin():
    _, stored, _, inventory = allowed_case()
    authority = custody.AllowedSourceAuthority("other_scope_review", inventory["identity"])
    document = stored["document"]
    document["evidence"]["source_authority"] = authority.as_dict()
    stored["document"] = review._document(document["command"], document["actor"], document["evidence"])
    stored["approval_sha256"] = review._digest(stored["document"])
    with pytest.raises(ValueError):
        references._review_commands(json.dumps([stored]))
