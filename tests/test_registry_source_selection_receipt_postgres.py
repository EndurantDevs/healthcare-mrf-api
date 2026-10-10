# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable reviewed selection evidence against actual native source candidates."""

import hashlib
import json
from contextlib import aclosing
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_recipe_composition import stage_registry_source_recipes
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.registry_source_selection_receipt import (
    MAX_RECEIPT_BYTES,
    SUMMARY_KEY,
    RegistrySourceSelectionConflict,
    RegistrySourceSelectionError,
    build_registry_source_selection_receipt,
    record_registry_source_selection,
    validate_registry_source_selection_receipt,
    verify_registry_source_selection_receipt,
)
from tests import cms_registry_recipe_postgres_support as cms_recipes
from tests.cms_registry_recipe_postgres_support import (
    cms_recipe_database as cms_recipe_database,
)
from tests.cms_registry_recipe_postgres_support import (
    serving_schema as serving_schema,
)
from tests.test_network_fhir_membership_source_postgres import _approved_pin, _reviewed_binding
from tests.test_registry_approval_store_postgres import _approve, _command

pytestmark = pytest.mark.asyncio

# Native selection tests explicitly request the existing module template.
cms_recipe_template = pytest_asyncio.fixture(scope="module", loop_scope="module")(
    cms_recipes.cms_recipe_template.__wrapped__
)


@pytest.fixture
def selection_receipt():
    """Return fresh bounded wire data; native roundtrip tests cover actual provenance."""
    receipt_by_field = {
        "component": SUMMARY_KEY,
        "revision": 1,
        "selection_policy": "approved-bindings-only",
        "recipe_sha256": "a" * 64,
        "recipe_count": 1,
        "approved_revision": 1,
        "approved_generation_sha256": "b" * 64,
        "stage_generation_sha256": "c" * 64,
        "mapped_rows": 1,
        "omitted_rows": 0,
    }
    assert validate_registry_source_selection_receipt(receipt_by_field) == receipt_by_field
    return receipt_by_field


async def _prepare_selection_evidence(source_fixture, actor, binding, approved, case):
    """Approve explicit closure before constructing the retained-source candidate."""
    connection, schema, _, _ = source_fixture
    if case == "omitted":
        closed = (
            await _reviewed_binding(
                connection,
                schema,
                actor,
                [
                    {
                        **binding,
                        "operation": "close",
                        "expected_revision": 1,
                        "expected_network_id": binding["network_id"],
                    },
                ],
            )
        )["records"][0]
        await _approve(connection, schema, await _command(connection, schema, closed), actor)
        return await _approved_pin(connection, schema)
    return approved


@pytest.fixture
async def selection_db(cms_recipe_template, cms_recipe_database, serving_schema, monkeypatch, tmp_path, request):
    """Retain a real reviewed source stage with cleanup registered before native creation."""
    case = getattr(request, "param", "mapped")
    original_resources = cms_recipes._membership_resource_rows

    def resources(original, revision):
        rows = original_resources(original, revision)
        if type(case) is int and case > 1:
            role = rows["PractitionerRole"][0]
            rows["PractitionerRole"].extend({**role, "id": f"role-extra-{index}"} for index in range(1, case))
        return rows

    monkeypatch.setattr(cms_recipes, "_membership_resource_rows", resources)
    async with aclosing(
        cms_recipes.reviewed_cms_source.__wrapped__(cms_recipe_database, serving_schema, monkeypatch, tmp_path)
    ) as source:
        async with aclosing(_selection_candidate(await anext(source), case)) as candidate:
            yield await anext(candidate)


async def _selection_candidate(reviewed_source, case):
    source_fixture, actor, _, binding, coordinates, approved = reviewed_source
    connection, schema, source_pin, _ = source_fixture
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)),
        str(identity),
        "network_candidate_" + identity.hex,
    )
    stage = None
    try:
        approved = await _prepare_selection_evidence(source_fixture, actor, binding, approved, case)
        recipes = (RegistrySourceMembershipRecipe(source_pin, coordinates),)
        async with connection.transaction(isolation="repeatable_read"):
            stage = await stage_registry_source_recipes(
                connection,
                recipes,
                approved,
                request_id=uuid4(),
                control_schema=schema,
            )
            await create_network_candidate(
                connection,
                copy_target,
                source_generations={
                    "custom_membership": approved.generation_id,
                    "registry_source_membership": stage.generation_sha256,
                },
                approved_custom_revision=approved.approved_revision,
                expected_head=0,
                expected_rows=stage.membership_rows + approved.total_rows,
                source_recipes=recipes,
                control_schema=schema,
            )
        yield SimpleNamespace(
            connection=connection,
            schema=schema,
            copy_target=copy_target,
            recipes=recipes,
            approved=approved,
            stage=stage,
            actor=actor,
            binding=binding,
        )
    finally:
        if stage is not None:
            await connection.execute(f'DROP TABLE IF EXISTS pg_temp."{stage.table_name}"')
            assert await connection.fetchval("SELECT to_regclass($1)", "pg_temp." + stage.table_name) is None
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None


async def _record(fixture, **changes):
    arguments_by_name = {
        "copy_target": fixture.copy_target,
        "recipes": fixture.recipes,
        "approved_source": fixture.approved,
        "stage": fixture.stage,
        "control_schema": fixture.schema,
        **changes,
    }
    async with fixture.connection.transaction(isolation="repeatable_read"):
        return await record_registry_source_selection(fixture.connection, **arguments_by_name)


async def _candidate(fixture):
    return dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(fixture.copy_target.candidate_id),
        )
    )


def _receipt(fixture):
    return build_registry_source_selection_receipt(fixture.recipes, fixture.approved, fixture.stage)


async def _seal(fixture):
    input_json = await fixture.connection.fetchval(
        f"SELECT coalesce(jsonb_agg(input_json::jsonb ORDER BY ordinal),'[]'::jsonb)::text "
        f'FROM pg_temp."{fixture.stage.table_name}"'
    )
    input_bytes = input_json.encode()
    async with fixture.connection.transaction():
        await admit_network_membership_batch(
            fixture.connection,
            fixture.copy_target,
            batch_id=uuid4(),
            input_bytes=input_bytes,
            expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
            control_schema=fixture.schema,
        )
        await seal_network_candidate(fixture.connection, fixture.copy_target, control_schema=fixture.schema)


@pytest.mark.parametrize("selection_db", ["mapped", "omitted"], indirect=True)
async def test_native_receipt_retains_mapped_or_omitted_counts_and_exact_replay(selection_db):
    fixture = selection_db
    receipt = await _record(fixture)
    assert (receipt["mapped_rows"], receipt["omitted_rows"]) == (
        fixture.stage.membership_rows,
        fixture.stage.omitted_rows,
    )
    assert receipt["mapped_rows"] + receipt["omitted_rows"] == receipt["recipe_count"] == 1
    assert validate_registry_source_selection_receipt(json.dumps(receipt)) == receipt
    assert verify_registry_source_selection_receipt(await _candidate(fixture)) == receipt
    before = await _candidate(fixture)
    queries = []
    with fixture.connection.query_logger(queries.append):
        assert await _record(fixture) == receipt
    assert not any(query.query.lstrip().upper().startswith("UPDATE") for query in queries)
    assert await _candidate(fixture) == before


async def test_native_storage_preserves_ordinary_validation_fields(selection_db):
    fixture = selection_db
    report_by_name = {"ordinary": {"ready": False, "count": 7}, "writer_closure": {"synthetic": True}}
    await fixture.connection.execute(
        f'UPDATE "{fixture.schema}".network_membership_candidate SET validation_json=$2::jsonb WHERE candidate_id=$1',
        UUID(fixture.copy_target.candidate_id),
        json.dumps(report_by_name),
    )
    receipt = await _record(fixture)
    retained = json.loads((await _candidate(fixture))["validation_json"])
    assert retained.pop(SUMMARY_KEY) == receipt and retained == report_by_name


@pytest.mark.parametrize("counter", [True, False, -1, 2**63, 1.0, "1", None])
@pytest.mark.parametrize("field", ["approved_revision", "mapped_rows", "omitted_rows", "recipe_count"])
async def test_receipt_rejects_noninteger_and_unbounded_counts(selection_receipt, field, counter):
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
        validate_registry_source_selection_receipt({**selection_receipt, field: counter})


@pytest.mark.parametrize(
    "damage",
    ["extra", "missing", "component", "policy", "revision", "recipe", "approved", "stage", "sum", "empty", "limit"],
)
async def test_closed_receipt_shape_and_hashes(selection_receipt, damage):
    receipt = selection_receipt
    changes_by_damage = {
        "extra": {"extra": "private-document"},
        "component": {"component": "unknown"},
        "policy": {"selection_policy": "legacy"},
        "revision": {"revision": True},
        "recipe": {"recipe_sha256": "A" * 64},
        "approved": {"approved_generation_sha256": "z" * 64},
        "stage": {"stage_generation_sha256": "0" * 63},
        "sum": {"mapped_rows": 2**63 - 1, "omitted_rows": 1},
        "empty": {"recipe_count": 0},
        "limit": {"recipe_count": 101},
    }
    receipt.update(changes_by_damage.get(damage, {}))
    if damage == "missing":
        receipt.pop("mapped_rows")
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
        validate_registry_source_selection_receipt(receipt)


async def test_exact_json_byte_boundary_duplicate_fields_and_strict_top_level(selection_receipt):
    receipt = selection_receipt
    encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    bounded = encoded + " " * (MAX_RECEIPT_BYTES - len(encoded.encode()))
    assert validate_registry_source_selection_receipt(bounded) == receipt
    for invalid in (bounded + " ", '{"mapped_rows":1,' + encoded[1:], "[]", "null", "[NaN]", "{"):
        with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
            validate_registry_source_selection_receipt(invalid)


@pytest.mark.parametrize(
    "changes",
    [
        {"omitted_rows": 1},
        {"membership_rows": 2},
        {"generation_sha256": "0" * 64},
        {"source_generations": ()},
        {"source_generations": ("A" * 64,)},
        {"table_name": "untrusted"},
    ],
)
async def test_actual_stage_digest_rejects_changed_counts_before_database(selection_db, changes):
    fixture = selection_db
    queries = []
    with fixture.connection.query_logger(queries.append):
        with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
            await _record(fixture, stage=replace(fixture.stage, **changes))
    assert not any("SELECT" in query.query or "UPDATE" in query.query for query in queries)
    assert (await _candidate(fixture))["validation_json"] is None


async def test_native_stage_row_tamper_rejects_without_partial_receipt(selection_db):
    fixture = selection_db
    await fixture.connection.execute(f'DELETE FROM pg_temp."{fixture.stage.table_name}"')
    with pytest.raises(RegistrySourceSelectionConflict, match="^registry_source_selection_conflict$"):
        await _record(fixture)
    assert (await _candidate(fixture))["validation_json"] is None


async def test_changed_retained_summary_conflicts_and_closed_candidate_is_immutable(selection_db):
    fixture = selection_db
    receipt = await _record(fixture)
    changed_receipt_dict = {**receipt, "omitted_rows": 1}
    await fixture.connection.execute(
        f'UPDATE "{fixture.schema}".network_membership_candidate '
        "SET validation_json=jsonb_set(validation_json,ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
        UUID(fixture.copy_target.candidate_id),
        SUMMARY_KEY,
        json.dumps(changed_receipt_dict),
    )
    with pytest.raises(RegistrySourceSelectionConflict, match="^registry_source_selection_conflict$"):
        await _record(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.schema}".network_membership_candidate '
        "SET validation_json=jsonb_set(validation_json,ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
        UUID(fixture.copy_target.candidate_id),
        SUMMARY_KEY,
        json.dumps(receipt),
    )
    await _seal(fixture)
    with pytest.raises(RegistrySourceSelectionConflict, match="^registry_source_selection_conflict$"):
        await _record(fixture)
    assert verify_registry_source_selection_receipt(await _candidate(fixture)) == receipt


@pytest.mark.parametrize(
    "field",
    ["recipe_sha256", "approved_generation_sha256", "stage_generation_sha256", "approved_revision", "recipe_count"],
)
async def test_verifier_rejects_receipt_scope_drift(selection_db, field):
    fixture = selection_db
    await _record(fixture)
    candidate = await _candidate(fixture)
    report_by_name = json.loads(candidate["validation_json"])
    report_by_name[SUMMARY_KEY][field] = "0" * 64 if "sha256" in field else 0
    candidate["validation_json"] = report_by_name
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
        verify_registry_source_selection_receipt(candidate)


async def test_verifier_preserves_legacy_initial_manifests_but_requires_staged_receipt(selection_db):
    fixture = selection_db
    candidate = await _candidate(fixture)
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
        verify_registry_source_selection_receipt(candidate)
    generations_by_source = json.loads(candidate["source_generations"])
    generations_by_source.pop("registry_source_membership")
    candidate["source_generations"] = generations_by_source
    assert verify_registry_source_selection_receipt(candidate) is None
    candidate["source_generations"] = {}
    candidate["source_recipes_json"] = []
    assert verify_registry_source_selection_receipt(candidate) is None


async def test_current_pin_candidate_scope_and_caller_isolation_are_required(selection_db):
    fixture = selection_db
    with pytest.raises(RegistrySourceSelectionError):
        await _record(fixture, copy_target=replace(fixture.copy_target, dataset_id=str(uuid4())))
    async with fixture.connection.transaction():
        with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
            await record_registry_source_selection(
                fixture.connection,
                fixture.copy_target,
                fixture.recipes,
                fixture.approved,
                fixture.stage,
                control_schema=fixture.schema,
            )
        assert await fixture.connection.fetchval("SELECT 1") == 1
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
        await record_registry_source_selection(
            fixture.connection,
            fixture.copy_target,
            fixture.recipes,
            fixture.approved,
            fixture.stage,
            control_schema=fixture.schema,
        )
    assert (await _candidate(fixture))["validation_json"] is None


async def test_caller_rollback_restores_candidate_without_receipt(selection_db):
    fixture = selection_db
    with pytest.raises(RuntimeError, match="synthetic rollback"):
        async with fixture.connection.transaction(isolation="repeatable_read"):
            await record_registry_source_selection(
                fixture.connection,
                fixture.copy_target,
                fixture.recipes,
                fixture.approved,
                fixture.stage,
                control_schema=fixture.schema,
            )
            raise RuntimeError("synthetic rollback")
    assert (await _candidate(fixture))["validation_json"] is None


async def test_native_row_is_normalized_before_shared_verification(selection_db):
    fixture = selection_db
    receipt = await _record(fixture)
    candidate_record = await fixture.connection.fetchrow(
        f'SELECT * FROM "{fixture.schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(fixture.copy_target.candidate_id),
    )
    assert verify_registry_source_selection_receipt(dict(candidate_record)) == receipt
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
        verify_registry_source_selection_receipt(candidate_record)


async def test_new_real_approval_rejects_previous_pin_without_receipt(selection_db):
    fixture = selection_db
    actor, binding = fixture.actor, fixture.binding
    closed = (
        await _reviewed_binding(
            fixture.connection,
            fixture.schema,
            actor,
            [{**binding, "operation": "close", "expected_revision": 1, "expected_network_id": binding["network_id"]}],
        )
    )["records"][0]
    await _approve(
        fixture.connection, fixture.schema, await _command(fixture.connection, fixture.schema, closed), actor
    )
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_invalid$"):
        await _record(fixture)
    assert (await _candidate(fixture))["validation_json"] is None


@pytest.mark.parametrize("selection_db", [1, 100], indirect=True)
async def test_native_storage_uses_fixed_set_query_count(selection_db):
    fixture = selection_db
    queries = []
    with fixture.connection.query_logger(queries.append):
        receipt = await _record(fixture)
    statements = [query for query in queries if query.query.lstrip().upper().startswith(("SELECT", "WITH", "UPDATE"))]
    assert len(statements) == 4
    assert receipt["mapped_rows"] == fixture.stage.membership_rows and receipt["omitted_rows"] == 0


async def test_missing_native_stage_fails_without_persisting_receipt(selection_db):
    fixture = selection_db
    await fixture.connection.execute(f'DROP TABLE pg_temp."{fixture.stage.table_name}"')
    with pytest.raises(RegistrySourceSelectionError, match="^registry_source_selection_unavailable$"):
        await _record(fixture)
    assert (await _candidate(fixture))["validation_json"] is None
