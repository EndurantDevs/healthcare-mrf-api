# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from process import uhc_provider_file_identity as identity
from process import uhc_provider_file_source_identity as source_identity


CS_PRODUCT_TOKENS = (
    "AZDC",
    "AZLTC",
    "AZMA",
    "COCHIP",
    "COMA",
    "CORAE",
    "DCFIDE",
    "DCHIDE",
    "FLLTC",
    "FLMMA",
    "HIMC",
    "HIQIMCAID",
    "HMOSNP",
    "IDFIDE",
    "IDMLTSS",
    "INFIDE",
    "INMA",
    "INMLTSS",
    "KSMA",
    "KYMA",
    "LAMA",
    "MAHIDE",
    "MASCO",
    "MDCAID",
    "MICAID",
    "MIHIDE",
    "MILTSS",
    "MOMA",
    "NCMA",
    "NEMA",
    "NJMA",
    "NMMA",
    "NYEPP",
    "NYHARP",
    "NYMA",
    "NYMAP",
    "OHMD",
    "PACP",
    "PAMA",
    "PPOSNP",
    "RIMA",
    "TNCARE",
    "TNKIDS",
    "TNLTC",
    "TXCHIP",
    "TXKIDS",
    "TXSPLUS",
    "TXSTAR",
    "VAFIDE",
    "VAMA",
    "WAAHE",
    "WAIMC",
    "WIMA",
)
IFP_STATE_CODES = (
    "AL",
    "AZ",
    "FL",
    "GA",
    "IA",
    "IL",
    "IN",
    "KS",
    "LA",
    "MI",
    "MO",
    "MS",
    "NC",
    "NE",
    "NJ",
    "NM",
    "OH",
    "OK",
    "SC",
    "TN",
    "TX",
    "VA",
    "WI",
    "WY",
)


def _descriptor(family, collection_kind, file_name):
    return identity.UHCSourceFileDescriptor(family, collection_kind, file_name)


def _scope(file_name="JSON_Providers_AZDC.json"):
    family = "ifp" if "IEX" in file_name or "UHCEX" in file_name else "cs"
    return identity.logical_scope_for_file(
        _descriptor(family, identity.PROVIDER_MEMBERSHIP, file_name)
    )


def _mutate_cs_product_token(census_document):
    product_by_token = dict(census_document["cs_provider_products"])
    product_by_token["AZDX"] = product_by_token.pop("AZDC")
    census_document["cs_provider_products"] = dict(
        sorted(product_by_token.items())
    )


def _replace_ifp_paired_state(census_document):
    states = list(census_document["ifp_paired_states"])
    states[states.index("AL")] = "AK"
    census_document["ifp_paired_states"] = sorted(states)


def test_corporate_source_identity_is_source_neutral_and_manifest_stays_louisiana():
    official_files = source_identity.source_identity_from_registry()
    manifest = json.loads(
        (
            source_identity.SOURCE_NEUTRAL_REGISTRY_PATH.parent
            / "provider_directory_endpoint_acquisition_manifest.json"
        ).read_text()
    )
    uhc_entry = next(entry for entry in manifest["entries"] if entry["entry_id"] == "uhc")

    assert official_files.entry_id == "uhc-provider-files"
    assert official_files.display_name == "UnitedHealthcare Official Provider Files"
    assert official_files.owner_id == "unitedhealthcare"
    assert official_files.portal_base != uhc_entry["canonical_base"]
    assert official_files.catalog_base != uhc_entry["canonical_base"]
    assert official_files.registered_source_id is None
    assert official_files.acquisition_manifest_entry_id is None
    assert official_files.acquisition_runnable is False
    assert official_files.profile_eligible is False
    assert official_files.publication_ready is False
    assert all(entry["entry_id"] != official_files.entry_id for entry in manifest["entries"])
    assert uhc_entry == {
        "entry_id": "uhc",
        "display_name": "UHC",
        "owner_id": "unitedhealthcare-community-plan-louisiana",
        "source_ids": ["pdfhir_0b5cfd565c53364a73981dcb"],
        "canonical_base": "https://flex.optum.com/fhirpublic/R4",
        "classification": "probe_only",
        "launch_mode": "create",
        "resource_profile": "NONE",
        "resources": [],
    }


def test_reviewed_census_contains_the_exact_102_official_filenames_once():
    census = identity.provider_file_census_from_spec()
    source_files = identity.current_census_source_files()
    scopes = identity.logical_scopes_for_current_census(reversed(source_files))

    assert tuple(product.product for product in census.cs_provider_products) == CS_PRODUCT_TOKENS
    assert census.ifp_paired_states == IFP_STATE_CODES
    assert census.ifp_unpaired_product == identity.UHCProductJurisdiction(
        "UHCEX_HIX",
        None,
    )
    assert len(source_files) == len(scopes) == 102
    assert len({scope.source_file.source_file_id for scope in scopes}) == 102
    assert len({scope.logical_scope_id for scope in scopes}) == 78
    assert sum(scope.family == "cs" for scope in scopes) == 53
    assert sum(
        scope.family == "ifp" and scope.collection_kind == identity.PROVIDER_MEMBERSHIP
        for scope in scopes
    ) == 25
    assert sum(scope.collection_kind == identity.PLAN_REFERENCE for scope in scopes) == 24
    census_document = json.loads(identity.UHC_CENSUS_PATH.read_text())
    assert (
        identity._sha256_json(census_document)
        == identity.UHC_CENSUS_SEMANTIC_SHA256
    )


def test_ifp_state_pairs_share_scope_and_exception_is_retained_only():
    scopes = identity.logical_scopes_for_current_census(
        identity.current_census_source_files()
    )
    paired_scopes = [
        scope for scope in scopes if scope.pairing_status == identity.PAIRING_PAIRED
    ]
    scope_ids_by_state = {
        state: {scope.logical_scope_id for scope in paired_scopes if scope.jurisdiction == state}
        for state in IFP_STATE_CODES
    }
    collection_kinds_by_state = {
        state: {scope.collection_kind for scope in paired_scopes if scope.jurisdiction == state}
        for state in IFP_STATE_CODES
    }

    assert all(len(scope_ids) == 1 for scope_ids in scope_ids_by_state.values())
    assert all(
        kinds == {identity.PROVIDER_MEMBERSHIP, identity.PLAN_REFERENCE}
        for kinds in collection_kinds_by_state.values()
    )
    exceptional = next(scope for scope in scopes if scope.product == "UHCEX_HIX")
    assert exceptional.jurisdiction is None
    assert exceptional.collection_kind == identity.PROVIDER_MEMBERSHIP
    assert exceptional.pairing_status == identity.PAIRING_UNPAIRED_RETAINED_ONLY
    assert all(
        scope.collection_kind == identity.PROVIDER_MEMBERSHIP
        and scope.pairing_status == identity.PAIRING_NOT_APPLICABLE
        for scope in scopes
        if scope.family == "cs"
    )


@pytest.mark.parametrize(
    "source_file",
    [
        _descriptor("cs", identity.PROVIDER_MEMBERSHIP, "JSON_Providers_UNKNOWN.json"),
        _descriptor("cs", identity.PROVIDER_MEMBERSHIP, "json_Providers_AZDC.json"),
        _descriptor("cs", identity.PROVIDER_MEMBERSHIP, "../JSON_Providers_AZDC.json"),
        _descriptor("cs", identity.PROVIDER_MEMBERSHIP, "JSON_Providers_AZDC.JSON"),
        _descriptor("ifp", identity.PROVIDER_MEMBERSHIP, "JSON_Providers_AZDC.json"),
        _descriptor("cs", identity.PLAN_REFERENCE, "JSON_PLANS_AZ.json"),
    ],
)
def test_scope_parser_rejects_unknown_malformed_or_misclassified_files(source_file):
    with pytest.raises(identity.UHCProviderFileIdentityError):
        identity.logical_scope_for_file(source_file)


def test_exact_census_rejects_duplicate_missing_unknown_and_unpaired_inputs():
    source_files = list(identity.current_census_source_files())

    with pytest.raises(identity.UHCProviderFileIdentityError, match="duplicate"):
        identity.logical_scopes_for_current_census([*source_files, source_files[0]])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="incomplete"):
        identity.logical_scopes_for_current_census(source_files[:-1])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="unknown"):
        identity.logical_scopes_for_current_census(
            [
                *source_files[:-1],
                _descriptor("ifp", identity.PLAN_REFERENCE, "JSON_PLANS_CA.json"),
            ]
        )


def test_cs_products_remain_product_scoped_without_inferred_jurisdiction():
    assert _scope("JSON_Providers_HMOSNP.json").jurisdiction is None
    assert _scope("JSON_Providers_PPOSNP.json").jurisdiction is None
    assert _scope("JSON_Providers_HIMC.json").jurisdiction is None


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda registry: registry["entries"][0].update({"registered_source_id": "pdfhir_" + "a" * 24}), "gates"),
        (lambda registry: registry["entries"][0].update({"profile_eligible": True}), "gates"),
        (lambda registry: registry["entries"][0].update({"enabling_gate": "later"}), "gates"),
        (lambda registry: registry["entries"][0].update({"owner_id": "unitedhealthcare-community-plan-louisiana"}), "identity"),
    ],
)
def test_source_registry_rejects_binding_or_capability_drift(tmp_path, mutation, message):
    registry = json.loads(source_identity.SOURCE_NEUTRAL_REGISTRY_PATH.read_text())
    mutation(registry)
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps(registry))

    with pytest.raises(identity.UHCProviderFileIdentityError, match=message):
        source_identity._source_identity_from_registry_path(registry_path)


def test_source_registry_has_no_public_path_override():
    with pytest.raises(TypeError):
        source_identity.source_identity_from_registry(
            source_identity.SOURCE_NEUTRAL_REGISTRY_PATH
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda census: census.update({"observed_on": "2026-07-21"}),
        lambda census: census.update({"provenance": "forged provenance"}),
        lambda census: census["cs_provider_products"].pop("AZDC"),
        lambda census: census["cs_provider_products"].update({"AZDC": "AZ"}),
        _mutate_cs_product_token,
        lambda census: census["ifp_paired_states"].append("CA"),
        _replace_ifp_paired_state,
        lambda census: census["ifp_unpaired_provider_products"][0].update(
            {"product": "UHCEX_HIX2"}
        ),
        lambda census: census["ifp_unpaired_provider_products"][0].update(
            {"retention_policy": "publish"}
        ),
        lambda census: census["census_counts"].update(
            {"catalog_file_count": 101}
        ),
    ],
)
def test_census_spec_rejects_header_shape_metadata_and_count_drift(
    tmp_path,
    mutation,
):
    census_document = json.loads(identity.UHC_CENSUS_PATH.read_text())
    mutation(census_document)
    census_path = tmp_path / "census.json"
    census_path.write_text(json.dumps(census_document))

    with pytest.raises(identity.UHCProviderFileIdentityError, match="frozen"):
        identity._provider_file_census_from_path(census_path)


def test_forged_census_has_no_public_override_seam():
    census = identity.provider_file_census_from_spec()
    forged_census = replace(
        census,
        cs_provider_products=(
            replace(census.cs_provider_products[0], jurisdiction="AZ"),
            *census.cs_provider_products[1:],
        ),
    )
    source_files = identity.current_census_source_files()

    public_override_calls = (
        lambda: identity.provider_file_census_from_spec(forged_census),
        lambda: identity.current_census_source_files(forged_census),
        lambda: identity.logical_scope_for_file(source_files[0], forged_census),
        lambda: identity.logical_scopes_for_current_census(
            source_files,
            forged_census,
        ),
    )
    for override_call in public_override_calls:
        with pytest.raises(TypeError):
            override_call()


def test_scope_collection_validator_rejects_types_and_forged_pairing():
    source_files = identity.current_census_source_files()
    scopes = identity.logical_scopes_for_current_census(source_files)
    cs_scope = next(scope for scope in scopes if scope.family == "cs")
    paired_scope = next(
        scope
        for scope in scopes
        if scope.family == "ifp" and scope.pairing_status == identity.PAIRING_PAIRED
    )
    exceptional = next(scope for scope in scopes if scope.product == "UHCEX_HIX")
    census = identity.provider_file_census_from_spec()

    with pytest.raises(identity.UHCProviderFileIdentityError, match="descriptor type"):
        identity.logical_scope_for_file("JSON_Providers_AZDC.json")
    with pytest.raises(identity.UHCProviderFileIdentityError, match="descriptor type"):
        identity.logical_scopes_for_current_census([*source_files[:-1], object()])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="CS scope"):
        identity._validate_scope_pairing(
            [replace(cs_scope, pairing_status=identity.PAIRING_PAIRED)],
            census,
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="unpaired scope"):
        identity._validate_scope_pairing(
            [replace(exceptional, collection_kind=identity.PLAN_REFERENCE)],
            census,
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="are unpaired"):
        identity._validate_scope_pairing([paired_scope], census)


def test_census_validators_reject_noncanonical_internal_shapes():
    census_document = json.loads(identity.UHC_CENSUS_PATH.read_text())

    invalid_header_by_field = dict(census_document)
    invalid_header_by_field["schema_version"] = 2
    with pytest.raises(identity.UHCProviderFileIdentityError, match="header"):
        identity._validate_census_header(invalid_header_by_field)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="CS product"):
        identity._validated_cs_products([])
    non_text_product_by_token = dict(census_document["cs_provider_products"])
    non_text_product_by_token[1] = non_text_product_by_token.pop("AZDC")
    with pytest.raises(identity.UHCProviderFileIdentityError, match="CS product"):
        identity._validated_cs_products(non_text_product_by_token)
    reversed_product_by_token = dict(
        reversed(census_document["cs_provider_products"].items())
    )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="CS product"):
        identity._validated_cs_products(reversed_product_by_token)
    invalid_product_by_token = dict(census_document["cs_provider_products"])
    invalid_product_by_token["AZDC"] = "AZ"
    with pytest.raises(identity.UHCProviderFileIdentityError, match="CS product"):
        identity._validated_cs_products(invalid_product_by_token)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="paired-state"):
        identity._validated_ifp_states([])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="paired-state"):
        identity._validated_ifp_states(
            list(reversed(census_document["ifp_paired_states"]))
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="unpaired-product"):
        identity._validated_unpaired_product([])

    census = identity.provider_file_census_from_spec()
    with pytest.raises(identity.UHCProviderFileIdentityError, match="counts"):
        identity._validate_declared_counts({}, census)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="classification"):
        identity.logical_scope_for_file(
            _descriptor(
                "unknown",
                identity.PROVIDER_MEMBERSHIP,
                "JSON_Providers_AZDC.json",
            )
        )


def test_source_registry_rejects_invalid_documents_and_urls(tmp_path):
    invalid_json_path = tmp_path / "invalid.json"
    invalid_json_path.write_text("{")
    scalar_path = tmp_path / "scalar.json"
    scalar_path.write_text("[]")
    invalid_registry_path = tmp_path / "registry.json"
    invalid_registry_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contract": source_identity.SOURCE_REGISTRY_CONTRACT,
                "entries": [],
            }
        )
    )

    for document_path in (invalid_json_path, scalar_path, invalid_registry_path):
        with pytest.raises(identity.UHCProviderFileIdentityError):
            source_identity._source_identity_from_registry_path(document_path)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="fields"):
        source_identity._exact_fields({"extra": True}, set(), "test")
    with pytest.raises(identity.UHCProviderFileIdentityError, match="HTTPS"):
        source_identity._canonical_https_base("http://example.test", "other", "test")
    with pytest.raises(identity.UHCProviderFileIdentityError, match="HTTPS"):
        source_identity._canonical_https_base(
            "http://example.test",
            "http://example.test",
            "test",
        )
