# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private projection checks; no database privileges or native plan claims."""

import hashlib
import inspect
import json
import re
import sqlite3
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from api.network_provider_only_read import _PAGE_SQL, _result
from api.network_provider_read import MAX_RESPONSE_BYTES, NetworkProviderReadUnavailable
from process import network_provider_only_projection as projection
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_contribution_store import _COUNTERS, locally_owned_source_descriptor
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_source_contributions import _descriptor, _fact, _recipe
from tests.test_registry_source_recipe_store_postgres import _example


def _target():
    identity = str(uuid4())
    return MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), identity, "network_candidate_" + identity.replace("-", "")
    )


def _capture_recipe():
    source = pin(retained())
    return RegistrySourceMembershipRecipe(
        source,
        RegistryNetworkSourceCoordinates(
            "fhir",
            source.source_id,
            source.schema_name,
            source.dataset_id,
            "registry-retained-normalized-fhir-v1",
            source.retained_capture.capture_sha256,
        ),
    )


def _sqlite():
    """Exercise selectors with stdlib JSON SQL; native PG casts/plans remain untested."""
    connection = sqlite3.connect(":memory:")
    connection.create_function("to_jsonb", 1, json.dumps)
    connection.create_function("convert_to", 2, lambda value, _: value.encode())
    connection.create_function("sha256", 1, lambda value: hashlib.sha256(value).digest())
    connection.create_function("encode", 2, lambda value, _: value.hex())
    connection.create_function("btrim", 1, str.strip)
    connection.create_function("octet_length", 1, lambda value: len(value.encode()))
    connection.create_collation("C", lambda left, right: (left > right) - (left < right))
    connection.create_function(
        "jsonb_typeof",
        1,
        lambda value: ("number" if type(json.loads(value)) in (int, float) else "other") if value is not None else None,
    )
    connection.create_function("regexp", 2, lambda pattern, value: bool(re.search(pattern, value or "")))

    def jsonb_set(value, path, replacement):
        document = json.loads(value)
        document[int(path[1:-1])] = json.loads(replacement)
        return json.dumps(document, ensure_ascii=False)

    connection.create_function("jsonb_set", 3, jsonb_set)
    return connection


def _select_rows(facts, bindings):
    with _sqlite() as connection:
        connection.execute("CREATE TABLE facts(recipe_sha256 text,fact_json text)")
        connection.execute("CREATE TABLE selected_bindings(recipe_sha256 text,network_id integer,record_json text)")
        connection.executemany(
            "INSERT INTO facts VALUES(?,?)", [(identity, json.dumps(fact, sort_keys=True)) for identity, fact in facts]
        )
        connection.executemany(
            "INSERT INTO selected_bindings VALUES(?,?,?)",
            [(identity, network, json.dumps(binding, sort_keys=True)) for identity, network, binding in bindings],
        )
        query = projection._SELECT_SQL.format(facts="facts").replace("::text", "").replace("::jsonb", "")
        return sorted(connection.execute(query).fetchall())


def test_selected_expression_is_exact_legacy_evidence_derivation():
    selected = (
        _PAGE_SQL.split("selected AS MATERIALIZED (\n", 1)[1].split("), identities AS MATERIALIZED (", 1)[0].strip()
    )
    selected = selected.replace("WHERE approved.network_id=ANY(${first}::integer[])\n    AND", "WHERE")
    assert projection._SELECT_SQL == selected


def test_actual_rows_deduplicate_and_evidence_hash_preserves_scalar_array_bytes():
    recipe = _recipe()
    fact = _fact(recipe.source_pin)
    fact["location_id"] = fact["evidence_basis"][-1] = None
    second_fact_by_field = {**fact, "resource_id": "role-two", "evidence_basis": fact["evidence_basis"].copy()}
    second_fact_by_field["evidence_basis"][4] = "role-two"
    binding_by_field = {"source_key": fact["source_key"], "source_scope_json": fact["source_scope_json"]}
    rows = _select_rows(
        [("a", fact), ("a", fact), ("a", second_fact_by_field)],
        [("a", 11, binding_by_field), ("a", 12, binding_by_field)],
    )
    expected_rows = []
    for network in (11, 12):
        for document in (fact, second_fact_by_field):
            basis = document["evidence_basis"].copy()
            basis[6] = network
            expected_rows.append(
                (
                    "npi",
                    fact["provider_id"],
                    network,
                    hashlib.sha256(json.dumps(basis, ensure_ascii=False).encode()).hexdigest(),
                )
            )
    assert rows == sorted(expected_rows)


@pytest.mark.parametrize("change", ["missing_location", "missing_omission", "text_null", "office", "omitted"])
def test_json_null_subset_excludes_missing_omission_and_office(change):
    fact = _fact(_recipe().source_pin)
    fact["location_id"] = fact["evidence_basis"][-1] = None
    binding_by_field = {"source_key": fact["source_key"], "source_scope_json": fact["source_scope_json"]}
    assert len(_select_rows([("a", fact)], [("a", 1, binding_by_field)])) == 1
    if change == "missing_location":
        fact.pop("location_id")
    elif change == "missing_omission":
        fact.pop("omission_reason")
    elif change == "text_null":
        fact["location_id"] = "null"
    elif change == "office":
        fact["location_id"] = str(uuid4())
    else:
        fact["omission_reason"] = "inactive"
    assert _select_rows([("a", fact)], [("a", 1, binding_by_field)]) == []


@pytest.mark.parametrize("field,value", [("recipe", "b"), ("source_key", "other"), ("scope", {})])
def test_fact_join_requires_exact_recipe_source_key_and_complete_scope(field, value):
    fact = _fact(_recipe().source_pin)
    fact["location_id"] = fact["evidence_basis"][-1] = None
    binding_by_field = {"source_key": fact["source_key"], "source_scope_json": fact["source_scope_json"]}
    identity = "a"
    if field == "recipe":
        identity = value
    elif field == "scope":
        binding_by_field["source_scope_json"] = value
    else:
        binding_by_field[field] = value
    assert _select_rows([("a", fact)], [(identity, 1, binding_by_field)]) == []


def test_source_parameters_keep_original_capture_and_all_six_coordinates():
    approved = ApprovedMembershipSource(3, "d" * 64)
    for recipe in (_recipe(), _capture_recipe()):
        query = projection._source_query(recipe, '"source".facts', "controls")
        arguments = projection._source_arguments(recipe, approved)
        assert arguments[:7] == (3, *recipe.binding_coordinates.sql_parameters)
        if recipe.source_pin.retained_capture is None:
            assert "->>'alias_scope'=$9::text" in query
            assert arguments[-1] == recipe.source_pin.alias_scope
        else:
            assert "->>'capture_sha256'=$9::text" in query
            assert "->>'organization_id'=record_json->>'source_key'" in query
            assert arguments[-1] == recipe.source_pin.retained_capture.capture_sha256


def test_read_shape_reuses_entire_legacy_response_and_bounds_without_recipes():
    query = projection.provider_only_page_sql("candidate", control_schema="controls")
    marker = "), identities AS MATERIALIZED ("
    legacy = _PAGE_SQL.split(marker, 1)[1].format(
        first=2, limit=3, offset=4, maximum_evidence=5, maximum_bytes=6, generation=7
    )
    assert query.split(marker, 1)[1] == legacy
    assert set(re.findall(r"\$(\d+)", query)) == set("1234567")
    assert all(
        field in query
        for field in (
            "source.source_system",
            "source.source_id",
            "source.dataset_schema",
            "source.dataset_id",
            "source.producer_id",
            "source.edition_id",
            "source.fhir_scope_key",
            "source.fhir_scope_value",
        )
    )
    assert all(
        omitted not in query
        for omitted in (
            "recipe_json",
            "source_recipes_json",
            "network_source_contribution",
            "fact_json",
            "UNION ALL",
        )
    )
    assert "source.source_system='fhir'" in query
    assert "binding.approved_revision=$1" in query
    assert "SELECT 1 FROM selected_bindings WHERE network_id=selected_id" in query


def _inventory_networks(recipe, binding_changes=None):
    query = projection.provider_only_page_sql("candidate", control_schema="controls")
    query = query.split("WITH selected_bindings AS MATERIALIZED (", 1)[1].split("), selected AS MATERIALIZED (", 1)[0]
    # Only the already tested numeric parser/parameter syntax differs in SQLite.
    query = re.sub(r"CROSS JOIN LATERAL \(SELECT CASE.*?END AS network_id\) parsed", "", query, flags=re.S)
    query = query.replace("parsed.network_id", "binding.network_id")
    query = query.replace('"candidate".network_source_inventory', "inventory")
    query = query.replace('"controls".registry_approved_record', "approved")
    query = query.replace("::text", "").replace("::jsonb", "")
    query = query.replace("FROM ONLY ", "FROM ").replace("JOIN ONLY ", "JOIN ")
    query = query.replace("binding.network_id=ANY($2::integer[])", "binding.network_id=11")
    source_pin = recipe.source_pin
    capture = source_pin.retained_capture
    scope_key = "alias_scope" if capture is None else "capture_sha256"
    scope_value = source_pin.alias_scope if capture is None else capture.capture_sha256
    scope_by_field = {scope_key: scope_value, "organization_id": "organization-one"}
    binding_by_field = {
        **asdict(recipe.binding_coordinates),
        "source_key": "organization-one",
        "source_scope_json": scope_by_field,
        "archived": False,
        "network_id": 11,
    }
    binding_by_field.update(binding_changes or {})
    with _sqlite() as connection:
        connection.execute(
            "CREATE TABLE approved(approved_revision int,record_kind text,record_key text,"
            "record_json text,network_id int)"
        )
        connection.execute(
            "CREATE TABLE inventory(source_system text,source_id text,dataset_schema text,"
            "dataset_id text,producer_id text,edition_id text,fhir_scope_key text,fhir_scope_value text)"
        )
        connection.execute(
            "INSERT INTO inventory VALUES(?,?,?,?,?,?,?,?)",
            (*recipe.binding_coordinates.sql_parameters, scope_key, scope_value),
        )
        connection.executemany(
            "INSERT INTO approved VALUES(?,?,?,?,?)",
            [
                (3, "network_binding", "binding-one", json.dumps(binding_by_field), 11),
                (3, "network", "11", json.dumps({"archived": False, "network_id": 11}), 11),
            ],
        )
        return connection.execute(query, {"1": 3}).fetchall()


def test_empty_network_validity_uses_scope_inventory_without_an_evidence_row():
    for recipe in (_recipe(), _capture_recipe()):
        assert _inventory_networks(recipe) == [(11,)]
        for coordinate in recipe.binding_coordinates.__dataclass_fields__:
            assert _inventory_networks(recipe, {coordinate: "different"}) == []
        assert _inventory_networks(recipe, {"archived": True}) == []
        assert _inventory_networks(recipe, {"source_scope_json": {"capture_sha256": "f" * 64}}) == []
    capture = _capture_recipe()
    assert _inventory_networks(capture, {"source_key": "different"}) == []


def test_unchanged_decoder_accepts_valid_empty_and_refuses_evidence_and_byte_overflow():
    scope = SimpleNamespace(manifest=SimpleNamespace(generation_id=9), network_ids=(11,))
    empty_by_field = {
        "membership_level": "provider",
        "generation_id": 9,
        "total_count": 0,
        "limit": 2,
        "offset": 0,
        "has_more": False,
        "providers": [],
    }
    receipt_by_field = {"valid": True, "bounded": True, "result": json.dumps(empty_by_field)}
    assert _result(receipt_by_field, scope, 2, 0) == empty_by_field
    for change in ({"valid": False}, {"bounded": False}):
        with pytest.raises(NetworkProviderReadUnavailable):
            _result(receipt_by_field | change, scope, 2, 0)
    with pytest.raises(ValueError):
        _result(receipt_by_field | {"result": "x" * (MAX_RESPONSE_BYTES + 1)}, scope, 2, 0)
    provider_by_field = {
        "provider_system": "npi",
        "provider_id": "1000000491",
        "display_name": None,
        "npi": 1000000491,
        "network_memberships": [{"network_id": 11, "evidence_ids": [f"{number:064x}" for number in range(1001)]}],
    }
    with pytest.raises(ValueError):
        _result(
            receipt_by_field
            | {"result": json.dumps(empty_by_field | {"total_count": 1, "providers": [provider_by_field]})},
            scope,
            2,
            0,
        )


@pytest.mark.asyncio
async def test_preparation_ddl_contains_only_real_rows_and_native_constraints():
    target = _target()
    candidate_by_field = {**asdict(target), "state": "open"}
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=candidate_by_field), execute=AsyncMock()
    )
    await projection.create_provider_only_projection(connection, target, control_schema="controls")
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert len(statements) == 2 and all("INSERT" not in statement for statement in statements)
    assert "PRIMARY KEY(network_id,provider_system,provider_id,evidence_id)" in statements[0]
    assert "(provider_system,provider_id,network_id,evidence_id)" in statements[1]
    assert all(
        forbidden not in " ".join(statements)
        for forbidden in (
            "location_id",
            "FOREIGN KEY",
            "TRIGGER",
            "FUNCTION",
            "sentinel",
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["state", "schema_name", "approved_revision", "approved_generation"])
async def test_candidate_mismatch_refuses_before_projection_mutation(mismatch):
    target = _target()
    approved = ApprovedMembershipSource(3, "d" * 64)
    candidate_by_field = {
        **asdict(target),
        "state": "open",
        "approved_custom_revision": 3,
        "approved_generation": "d" * 64,
    }
    changes_by_case = {
        "state": ("state", "ready"),
        "schema_name": ("schema_name", "other"),
        "approved_revision": ("approved_custom_revision", 4),
        "approved_generation": ("approved_generation", "e" * 64),
    }
    key, value = changes_by_case[mismatch]
    candidate_by_field[key] = value
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=candidate_by_field), execute=AsyncMock()
    )
    with pytest.raises(projection.ProviderOnlyProjectionError):
        await projection._candidate(connection, target, approved, "controls", preparing=True)
    connection.execute.assert_not_awaited()


async def _entries(*entries):
    for entry in entries:
        yield entry


def _sqlite_evidence_ddl(query):
    return query.replace("pg_temp.", "temp.").replace("evidence_id ~", "evidence_id REGEXP")


def _coarse_unique_connection(database, evidence_rows):
    observed_by_name = {}

    async def execute(query, *_arguments):
        assert "LIKE" not in query and "INCLUDING" not in query
        if query.startswith("CREATE TEMP TABLE pg_temp."):
            database.execute(_sqlite_evidence_ddl(query))
            observed_by_name["expected_relation"] = query.split("(", 1)[0].split("TABLE ", 1)[1]
        elif query.startswith("INSERT INTO pg_temp."):
            destination = query.split("(", 1)[0].split("INTO ", 1)[1].replace("pg_temp.", "temp.")
            database.executemany(f"INSERT OR IGNORE INTO {destination} VALUES(?,?,?,?)", evidence_rows)

    async def fetchrow(query):
        assert query.count("EXCEPT ALL") == 2
        expected = observed_by_name["expected_relation"].replace("pg_temp.", "temp.")
        actual_rows = database.execute(f"SELECT {projection.COLUMNS} FROM actual").fetchall()
        expected_rows = database.execute(f"SELECT {projection.COLUMNS} FROM {expected}").fetchall()
        observed_by_name["counts"] = (len(actual_rows), len(expected_rows))
        differences = database.execute(
            f"SELECT {projection.COLUMNS} FROM {expected} EXCEPT SELECT {projection.COLUMNS} FROM actual"
        ).fetchall()
        return {"actual_count": len(actual_rows), "expected_count": len(expected_rows), "differs": bool(differences)}

    return SimpleNamespace(execute=execute, fetchrow=fetchrow), observed_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["raw", "staged"])
async def test_extra_producer_unique_cannot_suppress_expected_evidence(monkeypatch, mode):
    recipe = _recipe()
    first_fact = _fact(recipe.source_pin)
    first_fact["location_id"] = first_fact["evidence_basis"][-1] = None
    second_fact_by_field = {
        **first_fact,
        "resource_id": "role-two",
        "evidence_basis": first_fact["evidence_basis"].copy(),
    }
    second_fact_by_field["evidence_basis"][4] = "role-two"
    binding_by_field = {"source_key": first_fact["source_key"], "source_scope_json": first_fact["source_scope_json"]}
    selected_rows = _select_rows([("a", first_fact), ("a", second_fact_by_field)], [("a", 11, binding_by_field)])
    evidence_rows = [(network, system, identity, digest) for system, identity, network, digest in selected_rows]
    assert len(evidence_rows) == 2 and evidence_rows[0][3] != evidence_rows[1][3]
    descriptor = _descriptor(recipe)
    monkeypatch.setattr(projection, "_candidate", AsyncMock())
    monkeypatch.setattr(projection, "_relation", lambda _: "actual")
    monkeypatch.setattr(projection, "_rebuild_source", AsyncMock())
    monkeypatch.setattr(projection, "map_fhir_facts", AsyncMock(return_value=descriptor))
    receipt = AsyncMock()
    monkeypatch.setattr(projection, "_receipt", receipt)
    ddl = SimpleNamespace(execute=AsyncMock())
    await projection._create_evidence_heap(ddl, "actual")
    with _sqlite() as database:
        database.execute(_sqlite_evidence_ddl(ddl.execute.await_args.args[0]))
        database.execute("CREATE UNIQUE INDEX coarse_producer_key ON actual(network_id,provider_system,provider_id)")
        database.executemany("INSERT OR IGNORE INTO actual VALUES(?,?,?,?)", evidence_rows)
        connection, observed_by_name = _coarse_unique_connection(database, evidence_rows)
        with pytest.raises(projection.ProviderOnlyProjectionError):
            if mode == "raw":
                await projection.verify_provider_only_projection(
                    connection, _target(), _entries((recipe, descriptor)), ApprovedMembershipSource(3, "d" * 64)
                )
            else:
                await projection._verify_staged_provider_only_projection(
                    connection,
                    _target(),
                    (recipe,),
                    _staged((locally_owned_source_descriptor(descriptor),)),
                    ApprovedMembershipSource(3, "d" * 64),
                )
        assert observed_by_name["counts"] == (1, 2)
    receipt.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "source_descriptor", "missing", "extra", "duplicate_recipe"])
async def test_independent_verification_rebuilds_source_and_refuses_both_parity_directions(monkeypatch, damage):
    recipe, copy_target = _recipe(), _target()
    descriptor = _descriptor(recipe)
    projection_identity = descriptor["recipe_sha256"]
    candidate = AsyncMock()
    rebuild, append = AsyncMock(), AsyncMock()
    mapped = {**descriptor, "fact_sha256": "f" * 64} if damage == "source_descriptor" else descriptor
    monkeypatch.setattr(projection, "_candidate", candidate)
    monkeypatch.setattr(projection, "_rebuild_source", rebuild)
    monkeypatch.setattr(projection, "_append", append)
    monkeypatch.setattr(projection, "map_fhir_facts", AsyncMock(return_value=mapped))
    monkeypatch.setattr(projection, "_receipt", AsyncMock(return_value={"row_count": 1, "rows_sha256": "f" * 64}))
    parity_by_field = {"differs": damage in {"missing", "extra"}, "actual_count": 1, "expected_count": 1}
    connection = SimpleNamespace(execute=AsyncMock(), fetchrow=AsyncMock(return_value=parity_by_field))
    entries = [(recipe, descriptor)] * (2 if damage == "duplicate_recipe" else 1)
    operation = projection.verify_provider_only_projection(
        connection, copy_target, _entries(*entries), ApprovedMembershipSource(3, "d" * 64), control_schema="controls"
    )
    if damage is None:
        receipt = await operation
        assert receipt["source_count"] == 1
        assert receipt["source_identities_sha256"] == hashlib.sha256(projection_identity.encode()).hexdigest()
        rebuild.assert_awaited_once()
        assert append.await_args.args[1].startswith('pg_temp."provider_only_expected_')
        assert append.await_args.args[2].startswith('pg_temp."provider_only_expected_')
        parity_sql = connection.fetchrow.await_args.args[0]
        assert parity_sql.count("EXCEPT ALL") == 2 and "actual_count" in parity_sql
        assert all("network_provider_only_evidence" not in str(call) for call in rebuild.await_args_list)
    else:
        with pytest.raises(projection.ProviderOnlyProjectionError):
            await operation
    assert connection.execute.await_args_list[-1].args[0].startswith("DROP TABLE IF EXISTS pg_temp.")


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "generation", "unresolved", "hash", "cursor"])
async def test_source_rebuild_reauthenticates_then_copies_bounded_real_fact_pages(monkeypatch, damage):
    recipe = _recipe()
    descriptor = _descriptor(recipe)
    fact = _fact(recipe.source_pin)
    facts = json.dumps([fact]).encode()
    page = SimpleNamespace(
        complete_raw_facts=True,
        unresolved_rows=0,
        unresolved_source_rows=0,
        input_bytes=b"[]",
        input_sha256=hashlib.sha256(b"[]").hexdigest(),
        generation_id="one",
        source_fact_bytes=facts,
        source=recipe.source_pin,
        approved_source=ApprovedMembershipSource(3, "d" * 64),
        binding_coordinates=recipe.binding_coordinates,
        source_rows=1,
        membership_rows=1,
        omitted_rows=0,
    )
    terminal = SimpleNamespace(**{**vars(page), "source_rows": 0, "membership_rows": 0, "source_fact_bytes": b"[]"})
    if damage == "generation":
        terminal.generation_id = "two"
    elif damage == "unresolved":
        page.unresolved_source_rows = 1
    elif damage == "hash":
        page.input_sha256 = "f" * 64
    next_cursor = None if damage == "cursor" else ("PractitionerRole", "role-one")
    reader = AsyncMock(side_effect=[(page, next_cursor, 1000), (terminal, next_cursor, 1000)])
    custody = AsyncMock(return_value="authenticated-source-read")
    monkeypatch.setattr(projection, "_require_recipe_custody", custody)
    monkeypatch.setattr(projection, "_read_recipe_page", reader)
    connection = SimpleNamespace(copy_records_to_table=AsyncMock(return_value="COPY 1"))
    operation = projection._rebuild_source(
        connection, recipe, "expected_facts", ApprovedMembershipSource(3, "d" * 64), "controls", "office-authority"
    )
    if damage is None:
        await operation
        assert reader.await_count == 2
        assert reader.await_args_list[0].kwargs == {"capture_facts": True}
        fact_records = connection.copy_records_to_table.await_args.kwargs["records"]
        assert fact_records[0][:2] == (descriptor["recipe_sha256"], 1)
        assert json.loads(fact_records[0][2]) == fact
    else:
        with pytest.raises(projection.ProviderOnlyProjectionError):
            await operation
    custody.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("anchored", [False, True])
async def test_builder_authenticates_fact_origin_but_only_prepares_rows(monkeypatch, anchored):
    recipe, target = _recipe(), _target()
    descriptor = _descriptor(recipe)
    if anchored:
        descriptor |= {"fact_candidate_id": str(uuid4()), "fact_table_oid": 123, "fact_seal_sha256": "c" * 64}
    native = projection._validated_descriptor(descriptor)
    verify = AsyncMock(return_value={native["recipe_sha256"]: SimpleNamespace(relation='"original".facts')})
    monkeypatch.setattr(projection, "_candidate", AsyncMock())
    monkeypatch.setattr(projection, "require_retained_approved_network_source_bindings", AsyncMock())
    monkeypatch.setattr(projection, "verify_anchored_source_fact_contents", verify)
    monkeypatch.setattr(projection, "map_fhir_facts", AsyncMock(return_value=native))
    append = AsyncMock()
    monkeypatch.setattr(projection, "_append", append)
    await projection.append_provider_only_source(
        object(), target, recipe, descriptor, ApprovedMembershipSource(3, "d" * 64), control_schema="controls"
    )
    assert verify.await_count == int(anchored)
    assert append.await_args.args[2] == (
        '"original".facts' if anchored else f'"{target.schema_name}".network_source_contribution'
    )
    assert append.await_args.args[1] == f'"{target.schema_name}".network_provider_only_evidence'


def _staged(descriptors):
    return RegistrySourceRecipeStage(
        "registry_source_page_" + uuid4().hex,
        0,
        0,
        "a" * 64,
        contribution_table_name="registry_source_fact_" + uuid4().hex,
        contribution_receipts=tuple(descriptors),
        contribution_revision=2,
    )


def _staged_descriptors(recipes, *, anchored=False, zero=False):
    descriptors = [locally_owned_source_descriptor(_descriptor(recipe)) for recipe in recipes]
    for descriptor in descriptors:
        if anchored:
            descriptor.update(fact_candidate_id=str(uuid4()), fact_table_oid=123, fact_seal_sha256="c" * 64)
        if zero:
            descriptor.update(dict.fromkeys(_COUNTERS, 0))
            descriptor["fact_sha256"] = hashlib.sha256(b"").hexdigest()
    return descriptors


def _staged_protocol(monkeypatch):
    """Component flow mocks do not establish independent-stage provenance or native authority."""
    for name in (
        "_rebuild_source",
        "map_fhir_facts",
        "verify_anchored_source_fact_contents",
        "create_contribution_stage",
    ):
        monkeypatch.setattr(projection, name, AsyncMock(side_effect=AssertionError("Staged path repeated SOURCE work")))
    monkeypatch.setattr(projection, "_candidate", AsyncMock())
    monkeypatch.setattr(projection, "_receipt", AsyncMock(return_value={"row_count": 0, "rows_sha256": "f" * 64}))
    connection = SimpleNamespace(
        execute=AsyncMock(),
        fetchrow=AsyncMock(
            return_value={
                "differs": False,
                "actual_count": 0,
                "expected_count": 0,
            }
        ),
    )
    append = AsyncMock()
    monkeypatch.setattr(projection, "_append", append)
    return connection, append


@pytest.mark.asyncio
@pytest.mark.parametrize("anchored", [False, True])
async def test_staged_relations_include_all_zero_evidence_fhir_identities(monkeypatch, anchored):
    recipes = (_example(1), _example(2))
    descriptors = _staged_descriptors(recipes, anchored=anchored, zero=True)
    stage = _staged(descriptors)
    connection, append = _staged_protocol(monkeypatch)
    metadata = AsyncMock(
        return_value={
            descriptor["recipe_sha256"]: SimpleNamespace(relation='"original".facts') for descriptor in descriptors
        }
    )
    monkeypatch.setattr(projection, "authenticate_anchored_source_fact_metadata", metadata)
    receipt = await projection._verify_staged_provider_only_projection(
        connection, _target(), (ptg_recipe(), *reversed(recipes)), stage, ApprovedMembershipSource(3, "d" * 64)
    )
    identities = sorted(descriptor["recipe_sha256"] for descriptor in descriptors)
    assert receipt["source_count"] == 2 and receipt["row_count"] == 0
    assert receipt["source_identities_sha256"] == hashlib.sha256("".join(identities).encode()).hexdigest()
    assert metadata.await_count == int(anchored) and append.await_count == 2
    expected_relation = '"original".facts' if anchored else 'pg_temp."' + stage.contribution_table_name + '"'
    assert {call.args[2] for call in append.await_args_list} == {expected_relation}
    assert "stage" not in inspect.signature(projection.verify_provider_only_projection).parameters


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [101, 1000])
async def test_staged_anchor_metadata_resolution_is_bounded(monkeypatch, count):
    recipes = tuple(_example(index) for index in range(count))
    descriptors = _staged_descriptors(recipes, anchored=True)
    connection, append = _staged_protocol(monkeypatch)

    async def metadata(_connection, selected, **_options):
        assert 1 <= len(selected) <= 100
        return {descriptor["recipe_sha256"]: SimpleNamespace(relation='"original".facts') for descriptor in selected}

    resolver = AsyncMock(side_effect=metadata)
    monkeypatch.setattr(projection, "authenticate_anchored_source_fact_metadata", resolver)
    receipt = await projection._verify_staged_provider_only_projection(
        connection, _target(), recipes, _staged(descriptors), ApprovedMembershipSource(3, "d" * 64)
    )
    assert receipt["source_count"] == count and append.await_count == count
    assert resolver.await_count == (count + 99) // 100


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "duplicate_recipe",
        "duplicate_descriptor",
        "missing",
        "orphan",
        "capture",
        "legacy_anchor",
        "partial_anchor",
        "stage_revision",
        "stage_type",
        "temp_name",
    ],
)
async def test_staged_identity_inventory_refuses_incomplete_or_mismatched_inputs(monkeypatch, damage):
    recipes = [_example(1), _example(2)]
    descriptors = _staged_descriptors(recipes)
    match damage:
        case "duplicate_recipe":
            recipes[1] = recipes[0]
        case "duplicate_descriptor":
            descriptors[1] = descriptors[0]
        case "missing":
            descriptors.pop()
        case "orphan":
            descriptors.extend(_staged_descriptors((_example(3),)))
        case "capture":
            descriptors[0]["capture_sha256"] = "f" * 64
        case "legacy_anchor":
            for key in projection.FACT_ANCHOR_FIELDS:
                descriptors[0].pop(key)
        case "partial_anchor":
            descriptors[0]["fact_table_oid"] = 123
    stage = _staged(descriptors)
    if damage == "stage_revision":
        stage = replace(stage, contribution_revision=1)
    elif damage == "stage_type":
        stage = SimpleNamespace(**asdict(stage))
    elif damage == "temp_name":
        stage = replace(stage, contribution_table_name="producer_schema.facts")
    connection, append = _staged_protocol(monkeypatch)
    with pytest.raises(ValueError):
        await projection._verify_staged_provider_only_projection(
            connection, _target(), recipes, stage, ApprovedMembershipSource(3, "d" * 64)
        )
    append.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "orphan"])
async def test_staged_anchor_resolution_requires_exact_requested_identity_set(monkeypatch, damage):
    recipe = _example()
    descriptor = _staged_descriptors((recipe,), anchored=True)[0]
    resolved = {} if damage == "missing" else {"f" * 64: SimpleNamespace(relation='"wrong".facts')}
    monkeypatch.setattr(projection, "authenticate_anchored_source_fact_metadata", AsyncMock(return_value=resolved))
    connection, append = _staged_protocol(monkeypatch)
    with pytest.raises(projection.ProviderOnlyProjectionError):
        await projection._verify_staged_provider_only_projection(
            connection, _target(), (recipe,), _staged((descriptor,)), ApprovedMembershipSource(3, "d" * 64)
        )
    append.assert_not_awaited()
