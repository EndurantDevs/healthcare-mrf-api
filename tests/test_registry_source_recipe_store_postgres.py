# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed recipe roundtrips and retained reads through real native publication."""

import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_source_epoch import _TABLES, RetainedCMSFHIRSourceEpoch
from process.network_membership_candidate_lifecycle import (
    MembershipCandidateError,
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_source_recipe_store import (
    MAX_SOURCE_RECIPE_BYTES,
    MAX_SOURCE_RECIPE_INPUT_BYTES,
    RECIPE_DIGEST_KEY,
    RegistrySourceMembershipRecipe,
    RegistrySourceRecipeError,
    _canonical_json,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    registry_source_recipes_sha256,
    resolve_retained_registry_source_recipes,
    verify_registry_source_recipes,
)
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_fhir_membership_source_postgres import fhir_source, reviewed_fhir_source
from tests.test_network_legacy_membership_source_postgres import reviewed_aca_source
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_candidate_composition_postgres import _roles
from tests.test_registry_retained_site_adoption_postgres import _source_addresses

pytestmark = pytest.mark.asyncio


def _example(index=0):
    source = PinnedFHIRMembershipSource(
        "source_example",
        "source-example",
        "endpoint-example",
        f"dataset-{index}",
        "a" * 64,
        "release-one",
        123,
        "medical",
        "2026-10-01",
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        source.source_id,
        source.schema_name,
        source.dataset_id,
        "producer-example",
        "edition-one",
    )
    return RegistrySourceMembershipRecipe(source, coordinates)


def _candidate(recipes):
    canonical = canonical_registry_source_recipes(recipes)
    return {
        "source_recipes_json": json.loads(canonical),
        "source_generations": {
            RECIPE_DIGEST_KEY: registry_source_recipes_sha256(canonical),
        },
    }


async def test_empty_sorted_recipe_digest_and_frozen_policy():
    recipes = (_example(1), _example(0))
    canonical = canonical_registry_source_recipes(recipes)
    assert canonical == canonical_registry_source_recipes(tuple(reversed(recipes)))
    assert registry_source_recipes_sha256(canonical) == hashlib.sha256(canonical.encode()).hexdigest()
    assert canonical_registry_source_recipes(decode_registry_source_recipes(canonical)) == canonical
    assert verify_registry_source_recipes(_candidate(recipes)) == decode_registry_source_recipes(canonical)
    assert verify_registry_source_recipes({"source_recipes_json": [], "source_generations": {}}) == ()
    assert canonical_registry_source_recipes(()) == "[]"
    with pytest.raises(FrozenInstanceError):
        recipes[0].selection_policy = "legacy-alias"
    with pytest.raises(RegistrySourceRecipeError):
        replace(recipes[0], selection_policy="legacy-alias")


def _cms_custody_recipe():
    """Build one complete private CMS recipe without conferring source admission."""
    legacy = _example()
    source = replace(
        legacy.source_pin,
        source_id="cms-npd",
        release_id="e" * 64,
        custody_owner_role="cms_owner",
        custody_runtime_roles=("cms_reader", "cms_writer"),
        custody_proof_sha256="b" * 64,
        custody_catalog_sha256="c" * 64,
    )
    return RegistrySourceMembershipRecipe(source, replace(legacy.binding_coordinates, source_id="cms-npd"))


async def test_cms_custody_wire_preserves_legacy_generation_and_binds_complete_receipt():
    """Legacy nine-field hashes stay stable; a CMS receipt binds all new coordinates."""
    legacy = _example()
    legacy_pin = json.loads(canonical_registry_source_recipes((legacy,)))[0]["source_pin"]
    assert len(legacy_pin) == 9 and not any(name.startswith("custody_") for name in legacy_pin)
    assert (
        legacy.source_pin.generation_id
        == hashlib.sha256(json.dumps(legacy_pin, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )
    recipe = _cms_custody_recipe()
    canonical = canonical_registry_source_recipes((recipe,))
    assert decode_registry_source_recipes(canonical) == (recipe,)
    pin = json.loads(canonical)[0]["source_pin"]
    assert len(pin) == 13 and pin["custody_runtime_roles"] == ["cms_reader", "cms_writer"]
    assert replace(recipe.source_pin, custody_catalog_sha256="d" * 64).generation_id != recipe.source_pin.generation_id


def _cms_epoch_recipe():
    legacy = _example()
    source_pin = replace(legacy.source_pin, source_id="cms-npd", release_id="e" * 64)
    identity = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    epoch = RetainedCMSFHIRSourceEpoch(
        tuple(source_pin.coordinates.values()),
        identity,
        "registry_cms_epoch_" + identity.hex,
        1000,
        tuple((name, 2000 + index) for index, name in enumerate(sorted(_TABLES))),
        "cms_epoch_owner",
        ("cms_reader",),
        "b" * 64,
        "c" * 64,
        "d" * 64,
        "f" * 64,
    )
    return RegistrySourceMembershipRecipe(
        replace(source_pin, retained_epoch=epoch), replace(legacy.binding_coordinates, source_id="cms-npd")
    )


async def test_cms_epoch_wire_preserves_origin_and_binds_physical_custody():
    recipe = _cms_epoch_recipe()
    canonical = canonical_registry_source_recipes((recipe,))
    assert decode_registry_source_recipes(canonical) == (recipe,)
    document = json.loads(canonical)[0]["source_pin"]
    assert len(document) == 10 and document["schema_name"] == "source_example"
    assert document["resource_table_oid"] == 123
    assert recipe.source_pin.read_schema_name == recipe.source_pin.retained_epoch.schema_name
    assert recipe.source_pin.read_resource_table_oid != recipe.source_pin.resource_table_oid
    changed_epoch = replace(recipe.source_pin.retained_epoch, content_sha256="1" * 64)
    assert replace(recipe.source_pin, retained_epoch=changed_epoch).generation_id != recipe.source_pin.generation_id


@pytest.mark.parametrize(
    "damage", ["roles_text", "origin_text", "oid_bool", "origin_schema", "extra", "mixed", "nil", "uuid_case"]
)
async def test_cms_epoch_wire_rejects_malformed_or_mixed_receipt(damage):
    document = json.loads(canonical_registry_source_recipes((_cms_epoch_recipe(),)))[0]
    pin = document["source_pin"]
    epoch = pin["retained_epoch"]
    if damage == "origin_schema":
        epoch["origin_coordinates"][0] = "other_source"
    elif damage == "mixed":
        pin.update(json.loads(canonical_registry_source_recipes((_cms_custody_recipe(),)))[0]["source_pin"])
    elif damage == "nil":
        pin["retained_epoch"] = None
    else:
        key, content = {
            "roles_text": ("runtime_roles", "abc"),
            "origin_text": ("origin_coordinates", "abcdefghi"),
            "oid_bool": ("schema_oid", True),
            "extra": ("unknown", 1),
            "uuid_case": ("epoch_id", epoch["epoch_id"].upper()),
        }[damage]
        epoch[key] = content
    with pytest.raises(RegistrySourceRecipeError, match="^registry_source_recipes_invalid$"):
        decode_registry_source_recipes([document])


@pytest.mark.parametrize(
    "damage",
    ["partial", "roles_text", "role_type", "duplicate", "unsorted", "owner", "digest", "empty", "other_source"],
)
async def test_cms_custody_wire_rejects_partial_or_malformed_receipts(damage):
    """Closed decoding rejects ambiguous receipt defaults and unsafe role coordinates."""
    document = json.loads(canonical_registry_source_recipes((_cms_custody_recipe(),)))[0]
    pin = document["source_pin"]
    if damage == "partial":
        pin.pop("custody_catalog_sha256")
    elif damage == "other_source":
        pin["source_id"] = document["binding_coordinates"]["source_id"] = "source-example"
    else:
        key, value = {
            "roles_text": ("custody_runtime_roles", "cms_reader"),
            "role_type": ("custody_runtime_roles", [1]),
            "duplicate": ("custody_runtime_roles", ["cms_reader", "cms_reader"]),
            "unsorted": ("custody_runtime_roles", ["cms_writer", "cms_reader"]),
            "owner": ("custody_runtime_roles", ["cms_owner"]),
            "digest": ("custody_proof_sha256", "B" * 64),
            "empty": ("custody_owner_role", None),
        }[damage]
        pin[key] = value
    with pytest.raises(RegistrySourceRecipeError, match="^registry_source_recipes_invalid$"):
        decode_registry_source_recipes([document])


@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "policy",
        "source_kind",
        "coordinate_extra",
        "source_extra",
        "source_hash",
        "source_oid",
        "bool_oid",
        "coordinate_source",
        "coordinate_schema",
        "coordinate_dataset",
        "date",
        "nil_pin",
    ],
)
async def test_closed_wire_rejects_malformed_batch(mutation):
    document = json.loads(canonical_registry_source_recipes((_example(),)))[0]
    header_by_mutation = {
        "extra": {"approved_revision": 1},
        "policy": {"selection_policy": "legacy-alias"},
        "source_kind": {"source_kind": "ptg"},
        "nil_pin": {"source_pin": None},
    }
    if mutation in header_by_mutation:
        document.update(header_by_mutation[mutation])
    elif mutation.startswith("coordinate"):
        key = {
            "coordinate_extra": "extra",
            "coordinate_source": "source_id",
            "coordinate_schema": "dataset_schema",
            "coordinate_dataset": "dataset_id",
        }[mutation]
        document["binding_coordinates"][key] = "unrelated"
    else:
        key, value = {
            "source_extra": ("extra", 1),
            "source_hash": ("dataset_sha256", "A" * 64),
            "source_oid": ("resource_table_oid", 4294967296),
            "bool_oid": ("resource_table_oid", True),
            "date": ("as_of", "20261001"),
        }[mutation]
        document["source_pin"][key] = value
    with pytest.raises(RegistrySourceRecipeError, match="^registry_source_recipes_invalid$"):
        decode_registry_source_recipes([document])


@pytest.mark.parametrize(
    "wire",
    [
        "null",
        "{}",
        "[true]",
        "[NaN]",
        "[Infinity]",
        '[{"source_kind":"fhir","source_kind":"aca"}]',
        "[" * 2000 + "]" * 2000,
        "[]" + " " * MAX_SOURCE_RECIPE_INPUT_BYTES,
    ],
)
async def test_json_limits_and_duplicate_fields_are_whole_batch_errors(wire):
    with pytest.raises(RegistrySourceRecipeError, match="^registry_source_recipes_invalid$"):
        decode_registry_source_recipes(wire)


async def test_count_duplicate_last_bad_and_noncanonical_hash_rejected():
    recipes = tuple(_example(index) for index in range(100))
    canonical = canonical_registry_source_recipes(recipes)
    assert len(decode_registry_source_recipes(canonical)) == 100
    for batch in [(*recipes, _example(100)), (_example(), _example()), (_example(), None)]:
        with pytest.raises(RegistrySourceRecipeError):
            canonical_registry_source_recipes(batch)
    documents = json.loads(canonical)
    documents[-1]["source_pin"]["resource_table_oid"] = False
    with pytest.raises(RegistrySourceRecipeError):
        decode_registry_source_recipes(documents)
    with pytest.raises(RegistrySourceRecipeError):
        registry_source_recipes_sha256(json.dumps(json.loads(canonical), indent=2))
    same_source = replace(_example(), binding_coordinates=replace(_example().binding_coordinates, producer_id="other"))
    with pytest.raises(RegistrySourceRecipeError):
        canonical_registry_source_recipes((_example(), same_source))


@pytest.mark.parametrize(
    "candidate",
    [
        {},
        {"source_recipes_json": [], "source_generations": {RECIPE_DIGEST_KEY: "a" * 64}},
        {"source_recipes_json": [], "source_generations": []},
    ],
)
async def test_empty_digest_or_missing_fields_rejected(candidate):
    with pytest.raises(RegistrySourceRecipeError):
        verify_registry_source_recipes(candidate)


@pytest.mark.parametrize("digest", [None, True, "a" * 64, "A" * 64, "x" * 65])
async def test_nonempty_digest_must_match_exact_receipt(digest):
    candidate = _candidate((_example(),))
    candidate["source_generations"][RECIPE_DIGEST_KEY] = digest
    with pytest.raises(RegistrySourceRecipeError):
        verify_registry_source_recipes(candidate)


async def test_canonical_and_raw_json_have_explicit_independent_bounds():
    canonical = canonical_registry_source_recipes((_example(),))
    assert decode_registry_source_recipes(canonical + " " * (MAX_SOURCE_RECIPE_INPUT_BYTES - len(canonical)))
    with pytest.raises(RegistrySourceRecipeError):
        decode_registry_source_recipes(canonical + " " * (MAX_SOURCE_RECIPE_INPUT_BYTES + 1 - len(canonical)))
    oversized = json.loads(canonical)
    oversized[0]["source_pin"]["untrusted"] = "x" * MAX_SOURCE_RECIPE_BYTES
    with pytest.raises(RegistrySourceRecipeError):
        decode_registry_source_recipes(oversized)


@pytest.mark.parametrize("character", ["x", "é"])
async def test_canonical_encoder_exact_utf8_byte_limit(character):
    text = character * ((MAX_SOURCE_RECIPE_BYTES - 2) // len(character.encode()))
    assert len(_canonical_json(text).encode()) == MAX_SOURCE_RECIPE_BYTES
    with pytest.raises(RegistrySourceRecipeError, match="^registry_source_recipes_invalid$"):
        _canonical_json(text + "x")


async def test_real_fhir_and_full_aca_receipts_roundtrip(reviewed_fhir_source, reviewed_aca_source):
    fhir_fixture, _, _, _, fhir_coordinates, _ = reviewed_fhir_source
    aca_fixture, _, _, _, aca_coordinates, _ = reviewed_aca_source
    recipes = (
        RegistrySourceMembershipRecipe(fhir_fixture[2], fhir_coordinates),
        RegistrySourceMembershipRecipe(aca_fixture[2], aca_coordinates),
    )
    decoded = decode_registry_source_recipes(canonical_registry_source_recipes(recipes))
    assert set(recipe.source_pin.generation_id for recipe in decoded) == {
        fhir_fixture[2].generation_id,
        aca_fixture[2].generation_id,
    }
    aca = next(recipe for recipe in decoded if recipe.binding_coordinates.source_system == "aca")
    assert aca.source_pin == aca_fixture[2]
    assert type(aca.source_pin.prepared.ownership.dataset_id) is UUID
    assert type(aca.source_pin.prepared.ownership.relation_oids) is tuple
    assert all(type(entry) is tuple for entry in aca.source_pin.prepared.ownership.relation_oids)
    wire = canonical_registry_source_recipes(decoded)
    assert "approved_revision" not in wire and "legacy-alias" not in wire


@pytest.mark.parametrize(
    "damage",
    [
        "manifest",
        "validation",
        "ownership_uuid",
        "ownership_oid",
        "ownership_extra",
        "relations",
        "scope",
        "producer",
        "edition",
        "runtime_roles",
    ],
)
async def test_real_aca_receipt_tamper_is_sanitized(reviewed_aca_source, damage):
    fixture, _, _, _, coordinates, _ = reviewed_aca_source
    recipe = RegistrySourceMembershipRecipe(fixture[2], coordinates)
    document = json.loads(canonical_registry_source_recipes((recipe,)))[0]
    pin = document["source_pin"]
    if damage in {"manifest", "validation"}:
        container = pin["prepared"]["manifest"] if damage == "manifest" else pin["validation"]
        container["schema_sha256" if damage == "manifest" else "validation_sha256"] = "0" * 64
    elif damage.startswith("ownership"):
        key, value = {
            "ownership_uuid": ("dataset_id", str(UUID(int=0))),
            "ownership_oid": ("schema_oid", True),
            "ownership_extra": ("extra", "untrusted"),
        }[damage]
        pin["prepared"]["ownership"][key] = value
    elif damage == "relations":
        pin["prepared"]["ownership"]["relation_oids"][-1][1] = 4294967296
    elif damage == "runtime_roles":
        pin["runtime_roles"] = list(reversed(pin["runtime_roles"])) + pin["runtime_roles"]
    else:
        key = {"scope": "dataset_schema", "producer": "producer_id", "edition": "edition_id"}[damage]
        document["binding_coordinates"][key] = "unrelated"
    with pytest.raises(RegistrySourceRecipeError, match="^registry_source_recipes_invalid$"):
        decode_registry_source_recipes([document])


async def _seed_recipe_membership(fixture, copy_target, source_table, provider_id, network_id):
    """Admit one exact retained office in the caller's native candidate transaction."""
    await fixture.connection.execute(f'''CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))''')
    await fixture.connection.execute(
        f'''INSERT INTO "{copy_target.schema_name}".provider_location_binding
      SELECT 'npi',$1,md5(location_key)::uuid,location_key,entity_type,entity_id FROM {source_table}''',
        provider_id,
    )
    site_id = await fixture.connection.fetchval(
        f'SELECT location_id FROM "{copy_target.schema_name}".provider_location_binding'
    )
    members = json.dumps(
        [
            {
                "network_id": network_id,
                "provider_system": "npi",
                "provider_id": provider_id,
                "location_id": str(site_id),
                "evidence_id": "e" * 64,
            },
        ]
    ).encode()
    await admit_network_membership_batch(
        fixture.connection,
        copy_target,
        batch_id=uuid4(),
        input_bytes=members,
        expected_input_sha256=hashlib.sha256(members).hexdigest(),
        control_schema=fixture.control_schema,
    )
    await seal_network_candidate(fixture.connection, copy_target, control_schema=fixture.control_schema)


@pytest.fixture
async def recipe_serving(custom_db, reviewed_fhir_source, reviewed_aca_source):
    """Publish actual source recipes and register exact candidate cleanup before creation."""
    fixture = custom_db
    fhir_fixture, _, networks, _, fhir_coordinates, _ = reviewed_fhir_source
    aca_fixture, _, _, _, aca_coordinates, _ = reviewed_aca_source
    recipes = (
        RegistrySourceMembershipRecipe(fhir_fixture[2], fhir_coordinates),
        RegistrySourceMembershipRecipe(aca_fixture[2], aca_coordinates),
    )
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    try:
        provider_id, source_table = await _source_addresses(fixture, 1, "npi")
        revision = await fixture.connection.fetchval(
            f'SELECT approved_revision FROM "{fixture.control_schema}".registry_revision_control'
        )
        arguments_by_name = {
            "source_generations": {"unified_address": fixture.base.generation_id},
            "source_recipes": recipes,
            "approved_custom_revision": revision,
            "expected_head": 0,
            "expected_rows": 1,
            "control_schema": fixture.control_schema,
        }
        async with fixture.connection.transaction():
            await create_network_candidate(fixture.connection, copy_target, **arguments_by_name)
            await create_network_candidate(fixture.connection, copy_target, **arguments_by_name)
            await create_network_candidate(
                fixture.connection, copy_target, **{**arguments_by_name, "source_recipes": tuple(reversed(recipes))}
            )
            with pytest.raises(MembershipCandidateError):
                await create_network_candidate(
                    fixture.connection, copy_target, **{**arguments_by_name, "source_recipes": ()}
                )
            await _seed_recipe_membership(fixture, copy_target, source_table, provider_id, networks[0]["record_id"])
        await prepare_and_publish_network_candidate(
            fixture.connection, copy_target, fixture.base, **_roles(fixture), control_schema=fixture.control_schema
        )
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        yield SimpleNamespace(fixture=fixture, manifest=manifest, recipes=recipes, target=copy_target)
    finally:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None


async def test_native_retained_recipes_revalidate_manifest_and_do_not_mutate(recipe_serving):
    fixture, manifest = recipe_serving.fixture, recipe_serving.manifest
    before = await fixture.connection.fetchval(
        f'SELECT to_jsonb(candidate)::text FROM "{fixture.control_schema}".network_membership_candidate candidate'
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        retained = await resolve_retained_registry_source_recipes(
            fixture.connection,
            manifest,
            control_schema=fixture.control_schema,
        )
    assert canonical_registry_source_recipes(retained) == canonical_registry_source_recipes(recipe_serving.recipes)
    assert (
        await fixture.connection.fetchval(
            f'SELECT to_jsonb(candidate)::text FROM "{fixture.control_schema}".network_membership_candidate candidate'
        )
        == before
    )
    assert set(manifest.source_generations) == {"unified_address", RECIPE_DIGEST_KEY}


@pytest.mark.parametrize("damage", ["isolation", "digest", "recipes", "manifest", "ineligible", "writable"])
async def test_native_retained_tamper_rejected(recipe_serving, damage):
    fixture, manifest = recipe_serving.fixture, recipe_serving.manifest
    isolation = "read_committed" if damage == "isolation" else "repeatable_read"
    async with fixture.connection.transaction(isolation=isolation):
        sql_by_damage = {
            "digest": f'UPDATE "{fixture.control_schema}".network_membership_candidate SET '
            f"source_generations=jsonb_set(source_generations,'{{{RECIPE_DIGEST_KEY}}}',to_jsonb('wrong'::text))",
            "recipes": f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET source_recipes_json='[]'",
            "ineligible": f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false',
            "writable": f'GRANT UPDATE ON "{manifest.schema_name}".network_membership TO "{fixture.roles["reader"]}"',
        }
        if damage in sql_by_damage:
            await fixture.connection.execute(sql_by_damage[damage])
        if damage == "manifest":
            manifest = replace(manifest, manifest_sha256="0" * 64)
        with pytest.raises(RegistrySourceRecipeError, match="^registry_source_recipes_unavailable$"):
            await resolve_retained_registry_source_recipes(
                fixture.connection, manifest, control_schema=fixture.control_schema
            )
