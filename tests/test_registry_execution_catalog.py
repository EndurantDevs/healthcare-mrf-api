# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject unsupported model types and unbound private execution inputs."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace

import pytest
from sqlalchemy import types
from sqlalchemy.dialects import postgresql

from process import network_bootstrap_sources as bootstrap
from process import reference_family_archive as archive
from process import registry_aca_source_recipe as aca_recipe
from process import registry_execution_catalog as catalog
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_legacy_membership_source import PinnedACAMembershipSource
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    RegistrySourceRecipeError,
    canonical_registry_source_recipes,
)
from tests.test_registry_aca_source_recipe import _DATASET, _metadata


@pytest.mark.parametrize(
    "column_type,reason",
    [(types.Float(), "model_type"), (postgresql.ARRAY(types.Date()), "model_array_type")],
)
def test_catalog_compiler_refuses_types_outside_its_native_contract(column_type, reason):
    with pytest.raises(catalog.RegistryExecutionCatalogError, match=reason):
        catalog._native_type(column_type)


def _private_surface(namespace, comments):
    return {
        "private_namespaces": [{"oid": "20001", "nspname": namespace, "nspowner": "20002"}],
        "private_comments": comments,
        "triggers": [],
        "rules": [],
        "functions": [],
    }


@pytest.mark.parametrize(
    "namespace", ["unscoped", "registry_fhir_capture_" + "a" * 31, "network_bootstrap_" + "A" * 32]
)
def test_private_catalog_refuses_unknown_or_noncanonical_names_before_receipt_decode(monkeypatch, namespace):
    def unexpected(*_args):
        pytest.fail("an unrecognized namespace must never reach retained-input admission")

    monkeypatch.setattr(catalog, "_require_retained_fhir_input", unexpected)
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="private_namespace"):
        catalog._require_private_read_surfaces(_private_surface(namespace, []), {})


@pytest.mark.parametrize("description", [None, {}, "x" * (2097152 + 1)])
def test_private_catalog_requires_a_bounded_native_receipt_before_admission(monkeypatch, description):
    def unexpected(*_args):
        pytest.fail("missing or invalid receipts must not reach retained-input admission")

    monkeypatch.setattr(catalog, "_require_retained_fhir_input", unexpected)
    comments = [] if description is None else [{"objoid": "20001", "description": description}]
    observed = _private_surface("registry_fhir_capture_" + "a" * 32, comments)
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="private_receipt"):
        catalog._require_private_read_surfaces(observed, {})


def _bootstrap_surface():
    """Bind an encoded receipt to observations of its two protected native heaps."""
    specification = bootstrap.NetworkBootstrapSourceSpecification(
        "10000000-0000-0000-0000-000000000001", "source_addresses", "source_npi", 30003, 30004
    )
    parents = tuple(
        bootstrap.BootstrapParentReceipt(schema, table, 30001, oid, 30002, 30005, "{}", "a" * 64, 0, "b" * 64)
        for schema, table, oid in (
            ("source_addresses", "entity_address_unified", 30003),
            ("source_npi", "npi", 30004),
        )
    )
    receipt = bootstrap.NetworkBootstrapSourceReceipt(
        specification,
        "capture_owner",
        ("capture_reader",),
        parents,
        "42:42:",
        "2026-01-01T00:00:00Z",
        20001,
        20002,
        (("entity_address_unified", 20003), ("npi", 20004)),
        (),
        "",
    )
    receipt = replace(receipt, generation_sha256=bootstrap._generation(receipt))
    observed = _private_surface(
        bootstrap._schema_name(specification), [{"objoid": "20001", "description": json.dumps(receipt.as_dict())}]
    )
    relations_by_oid = {
        str(oid): dict(
            oid=str(oid),
            relname=name,
            relnamespace="20001",
            relowner="20002",
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
            relispartition=False,
        )
        for name, oid in receipt.relation_oids
    }
    return observed, relations_by_oid


def test_private_bootstrap_certificate_preserves_exact_receipt_and_native_heap_identity():
    observed, relations_by_oid = _bootstrap_surface()
    before = deepcopy((observed, relations_by_oid))
    assert catalog._require_private_read_surfaces(observed, relations_by_oid) is None
    assert (observed, relations_by_oid) == before


@pytest.mark.parametrize("mutation", ["missing_heap", "owner", "unlogged", "rls", "function", "trigger", "rule"])
def test_private_bootstrap_cannot_gain_new_storage_or_callback_authority(mutation):
    observed, relations_by_oid = _bootstrap_surface()
    reason = "private_storage"
    if mutation == "missing_heap":
        del relations_by_oid["20004"]
        reason = "bootstrap_identity"
    elif mutation in {"owner", "unlogged", "rls"}:
        field, value = {
            "owner": ("relowner", "20099"),
            "unlogged": ("relpersistence", "u"),
            "rls": ("relrowsecurity", True),
        }[mutation]
        relations_by_oid["20003"][field] = value
    else:
        key, value = {
            "function": ("functions", {"pronamespace": "20001"}),
            "trigger": ("triggers", {"tgrelid": "20003"}),
            "rule": ("rules", {"ev_class": "20003"}),
        }[mutation]
        observed[key].append(value)
        reason = "private_callback"
    before = deepcopy((observed, relations_by_oid))
    with pytest.raises(catalog.RegistryExecutionCatalogError, match=reason):
        catalog._require_private_read_surfaces(observed, relations_by_oid)
    assert (observed, relations_by_oid) == before


def _archive_manifest(canonical):
    """Use the existing synthetic source coordinates and real manifest validation."""
    specification = archive.reference_family_spec("mrf", canonical=canonical)
    tables = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 0)
        for model in specification.model_types
    )
    auxiliary = (
        archive._canonical_model_receipt()
        if canonical
        else {
            "table_name": archive.STAGE_TABLE,
            "archive_name": archive.archive_table_name(),
            "schema_sha256": hashlib.sha256(archive._AUX_SCHEMA.encode()).hexdigest(),
            "row_count": 0,
            "content_sha256": "b" * 64,
            "publication_sha256": "c" * 64,
        }
    )
    metadata = _metadata()
    return archive.validate_reference_family_manifest(
        archive.ReferenceFamilyManifest(
            "mrf",
            tables,
            metadata,
            archive._source_metadata(metadata)[1],
            archive._schema_digest(tables),
            {"plan-attributes": "d" * 64},
            auxiliary,
        )
    )


def _archive_recipe(canonical=False):
    manifest = _archive_manifest(canonical)
    relation_oids = tuple(
        (name, 20010 + index) for index, name in enumerate(sorted(receipt.table_name for receipt in manifest.tables))
    )
    ownership = archive.ReferenceFamilyStageOwnership(
        "mrf",
        _DATASET,
        archive.reference_family_stage_schema(_DATASET),
        20001,
        relation_oids,
        (("issuer_issuer_id_seq", 20101, "issuer", "issuer_id"),),
        None if canonical else 20100,
    )
    validation_by_field = {
        "contract": archive.VALIDATION_CONTRACT,
        "importer_id": "mrf",
        "package_id": "e" * 64,
        "profile_contract": archive.reference_family_profile_contract(manifest),
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in relation_oids],
        "sealed_owner_oid": 20002,
        "manifest_sha256": hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest(),
        "tables": [table.as_dict() for table in manifest.tables],
    }
    validation = archive.validate_reference_family_validation_receipt(
        {**validation_by_field, "validation_sha256": archive._validation_digest(validation_by_field)}
    )
    metadata = manifest.source_metadata
    pin = PinnedACAMembershipSource(
        archive.ReferenceFamilyPreparedSource(manifest, ownership),
        validation,
        **{**metadata["network_membership"], "runtime_roles": tuple(metadata["network_membership"]["runtime_roles"])},
    )
    coordinates = RegistryNetworkSourceCoordinates(
        **{
            name: coordinate_value
            for name, coordinate_value in metadata["network_bindings"].items()
            if name != "source_key_kind"
        }
    )
    return RegistrySourceMembershipRecipe(pin, coordinates)


def _archive_relation(name, oid, kind="r"):
    return dict(
        oid=str(oid),
        relname=name,
        relnamespace="20001",
        relowner="20002",
        relkind=kind,
        relpersistence="p",
        relrowsecurity=False,
        relforcerowsecurity=False,
        relispartition=False,
    )


def _archive_surface(canonical=False):
    recipe = _archive_recipe(canonical)
    ownership = recipe.source_pin.prepared.ownership
    document = aca_recipe._PREFIX + canonical_registry_source_recipes((recipe,))
    observed = _private_surface(ownership.schema_name, [{"objoid": "20001", "description": document}])
    relations_by_oid = {str(oid): _archive_relation(name, oid) for name, oid in ownership.relation_oids}
    if ownership.auxiliary_oid is not None:
        relations_by_oid["20100"] = _archive_relation("mrf_canonical_address", ownership.auxiliary_oid)
    relations_by_oid["20101"] = _archive_relation("issuer_issuer_id_seq", 20101, "S")
    return observed, relations_by_oid


@pytest.mark.parametrize("canonical", [False, True])
def test_private_archive_accepts_exact_typed_receipt_and_native_catalog(canonical):
    observed, relations_by_oid = _archive_surface(canonical)
    before = deepcopy((observed, relations_by_oid))
    assert catalog._require_private_read_surfaces(observed, relations_by_oid) is None
    assert (observed, relations_by_oid) == before


@pytest.mark.parametrize(
    "target,field,value",
    [
        ("namespace", "nspname", "reference_family_archive_" + "f" * 32),
        ("namespace", "nspowner", "20999"),
        ("20010", "oid", "20999"),
        ("20010", "relname", "unrecorded_heap"),
        ("20100", "oid", "20999"),
        ("20101", "oid", "20999"),
        ("20101", "relname", "unrecorded_sequence"),
    ],
)
def test_private_archive_rejects_receipt_catalog_identity_drift(target, field, value):
    observed, relations_by_oid = _archive_surface()
    native_row = observed["private_namespaces"][0] if target == "namespace" else relations_by_oid[target]
    native_row[field] = value
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="archive_identity"):
        catalog._require_private_read_surfaces(observed, relations_by_oid)


def test_private_archive_rejects_recreated_namespace_with_same_name_and_inventory():
    observed, relations_by_oid = _archive_surface()
    observed["private_namespaces"][0]["oid"] = "20999"
    observed["private_comments"][0]["objoid"] = "20999"
    for native_row in relations_by_oid.values():
        native_row["relnamespace"] = "20999"
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="archive_identity"):
        catalog._require_private_read_surfaces(observed, relations_by_oid)


@pytest.mark.parametrize("oid", ["20010", "20100", "20101"])
@pytest.mark.parametrize("change", ["missing", "extra"])
def test_private_archive_requires_complete_heap_auxiliary_and_sequence_inventory(oid, change):
    observed, relations_by_oid = _archive_surface()
    native_row = relations_by_oid[oid]
    if change == "missing":
        del relations_by_oid[oid]
    else:
        relations_by_oid["20999"] = {**native_row, "oid": "20999", "relname": "unrecorded_relation"}
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="archive_identity"):
        catalog._require_private_read_surfaces(observed, relations_by_oid)


@pytest.mark.parametrize(
    "document,reason", [("unknown:[]", "archive_receipt"), (aca_recipe._BATCH_PREFIX + "[]", "archive_kind")]
)
def test_private_archive_requires_recognized_nonempty_archive_receipt(document, reason):
    observed, relations_by_oid = _archive_surface()
    observed["private_comments"][0]["description"] = document
    with pytest.raises(catalog.RegistryExecutionCatalogError, match=reason):
        catalog._require_private_read_surfaces(observed, relations_by_oid)


@pytest.mark.parametrize("damage", ["json", "validation_digest", "manifest_digest"])
def test_private_archive_cannot_bypass_recipe_and_archive_receipt_validation(damage):
    observed, relations_by_oid = _archive_surface()
    document = json.loads(observed["private_comments"][0]["description"][len(aca_recipe._PREFIX) :])
    if damage == "validation_digest":
        document[0]["source_pin"]["validation"]["validation_sha256"] = "f" * 64
    elif damage == "manifest_digest":
        document[0]["source_pin"]["prepared"]["manifest"]["source_metadata_sha256"] = "f" * 64
    observed["private_comments"][0]["description"] = aca_recipe._PREFIX + (
        "[" if damage == "json" else json.dumps(document)
    )
    with pytest.raises(RegistrySourceRecipeError, match="registry_source_recipes_invalid"):
        catalog._require_private_read_surfaces(observed, relations_by_oid)


def test_private_archive_refuses_valid_recipe_for_another_source_kind():
    observed, relations_by_oid = _archive_surface()
    coordinates = RegistryNetworkSourceCoordinates(
        "ptg", "source-example", "source_offices", str(_DATASET), "producer-example", "edition-one"
    )
    pin = PinnedPTGOfficeMembershipSource(
        str(_DATASET), "example-client", "a" * 64, "b" * 64, "c" * 64, coordinates, 1, 1
    )
    document = canonical_registry_source_recipes((RegistrySourceMembershipRecipe(pin, coordinates),))
    observed["private_comments"][0]["description"] = aca_recipe._PREFIX + document
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="archive_kind"):
        catalog._require_private_read_surfaces(observed, relations_by_oid)
