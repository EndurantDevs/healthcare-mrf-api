# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process import uhc_provider_file_identity as identity
from process import uhc_provider_file_semantic_identity as semantic


def _scope(file_name="JSON_Providers_AZDC.json"):
    family = "ifp" if "IEX" in file_name else "cs"
    source_file = identity.UHCSourceFileDescriptor(
        family,
        identity.PROVIDER_MEMBERSHIP,
        file_name,
    )
    return identity.logical_scope_for_file(source_file)


def _dataset_identity(scopes, artifacts, **overrides):
    fields_by_name = {
        "adapter_version": "catalog-1.0.0",
        "transform_version": "scope-1.0.0",
        "catalog_set_sha256": "c" * 64,
        "logical_scopes": scopes,
        "artifact_digests": artifacts,
        "as_of": "2026-07-22",
        "plan_years": [2026, 2027],
    }
    fields_by_name.update(overrides)
    return semantic.dataset_build_identity(**fields_by_name)


def _rehashed_plan_key(plan_key, **changes):
    candidate = replace(plan_key, **changes)
    return replace(
        candidate,
        plan_key_id=semantic._sha256_json(
            semantic._plan_key_fields(
                candidate.scope,
                candidate.plan_id_type,
                candidate.plan_id,
                candidate.plan_year,
            )
        ),
    )


def _rehashed_network_key(network_key, **changes):
    candidate = replace(network_key, **changes)
    return replace(
        candidate,
        network_key_id=semantic._sha256_json(
            semantic._network_key_fields(
                candidate.plan_key,
                candidate.network_tier,
            )
        ),
    )


def test_plan_and_network_keys_bind_every_required_dimension():
    cs_scope = _scope()
    ifp_scope = _scope("JSON_Providers_AZIEX.json")
    plan_key = semantic.plan_key_for_scope(
        cs_scope,
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2026,
    )
    changed_scope_key = semantic.plan_key_for_scope(
        ifp_scope,
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2026,
    )
    changed_type_key = semantic.plan_key_for_scope(
        cs_scope,
        plan_id_type="issuer-plan",
        plan_id="12345AZ001",
        plan_year=2026,
    )
    changed_id_key = semantic.plan_key_for_scope(
        cs_scope,
        plan_id_type="hios",
        plan_id="12345AZ002",
        plan_year=2026,
    )
    changed_year_key = semantic.plan_key_for_scope(
        cs_scope,
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2027,
    )

    assert plan_key.logical_scope_id == cs_scope.logical_scope_id
    assert plan_key.family == cs_scope.family
    assert plan_key.market == cs_scope.market
    assert len(
        {
            plan_key.plan_key_id,
            changed_scope_key.plan_key_id,
            changed_type_key.plan_key_id,
            changed_id_key.plan_key_id,
            changed_year_key.plan_key_id,
        }
    ) == 5
    preferred = semantic.network_key_for_plan(plan_key, network_tier="preferred")
    standard = semantic.network_key_for_plan(plan_key, network_tier="standard")
    assert preferred.network_tier == "preferred"
    assert preferred.network_key_id != standard.network_key_id
    assert preferred.plan_key is plan_key
    assert preferred.logical_scope_id == plan_key.logical_scope_id
    assert preferred.family == plan_key.family
    assert preferred.market == plan_key.market
    assert preferred.plan_id_type == plan_key.plan_id_type
    assert preferred.plan_id == plan_key.plan_id
    assert preferred.plan_year == plan_key.plan_year
    assert semantic.validate_plan_key_identity(plan_key) is plan_key
    assert semantic.validate_network_key_identity(preferred) is preferred


def test_dataset_build_identity_is_order_independent_and_binds_every_artifact():
    scopes = (_scope(), _scope("JSON_Providers_AZIEX.json"))
    artifacts = tuple(
        semantic.UHCArtifactDigest(scope.source_file.source_file_id, digest * 64)
        for scope, digest in zip(scopes, ("a", "b"), strict=True)
    )
    first_identity = _dataset_identity(
        reversed(scopes),
        reversed(artifacts),
        plan_years=[2027, 2026],
    )
    reordered_identity = _dataset_identity(scopes, artifacts)

    assert first_identity == reordered_identity
    expected_ordered_scopes = tuple(
        sorted(
            scopes,
            key=lambda scope: (
                scope.logical_scope_id,
                scope.collection_kind,
                scope.source_file.source_file_id,
            ),
        )
    )
    assert first_identity.logical_scopes == expected_ordered_scopes
    assert first_identity.plan_years == (2026, 2027)
    assert semantic.validate_dataset_build_identity(first_identity) is first_identity


def test_dataset_build_identity_binds_versions_years_and_artifact_set():
    scopes = (_scope(), _scope("JSON_Providers_AZIEX.json"))
    artifacts = tuple(
        semantic.UHCArtifactDigest(scope.source_file.source_file_id, digest * 64)
        for scope, digest in zip(scopes, ("a", "b"), strict=True)
    )
    baseline = _dataset_identity(scopes, artifacts)
    changed_transform = _dataset_identity(
        scopes,
        artifacts,
        transform_version="scope-1.0.1",
    )
    single_year = _dataset_identity(scopes, artifacts, plan_years=[2026])

    assert baseline.dataset_build_id != changed_transform.dataset_build_id
    assert baseline.dataset_build_id != single_year.dataset_build_id
    with pytest.raises(identity.UHCProviderFileIdentityError, match="do not match"):
        _dataset_identity(scopes, artifacts[:-1])


def test_identity_builders_reject_wrong_hashes_and_malformed_values():
    scope = _scope()
    plan_key = semantic.plan_key_for_scope(
        scope,
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2026,
    )

    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scope"):
        semantic.plan_key_for_scope(
            replace(scope, logical_scope_id="f" * 64),
            plan_id_type="hios",
            plan_id="12345AZ001",
            plan_year=2026,
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="plan key"):
        semantic.network_key_for_plan(
            replace(plan_key, plan_key_id="f" * 64),
            network_tier="preferred",
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="plan_year"):
        semantic.plan_key_for_scope(
            scope,
            plan_id_type="hios",
            plan_id="12345AZ001",
            plan_year=True,
        )


@pytest.mark.parametrize(
    "scope_change",
    [
        {"logical_scope_id": "f" * 64},
        {"family": "ifp"},
        {"market": identity.INDIVIDUAL_EXCHANGE_MARKET},
        {"product": "FORGED"},
        {"jurisdiction": "AZ"},
        {"collection_kind": identity.PLAN_REFERENCE},
        {"pairing_status": identity.PAIRING_PAIRED},
    ],
)
def test_rehashed_plan_key_rejects_every_forged_scope_field(scope_change):
    plan_key = semantic.plan_key_for_scope(
        _scope(),
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2026,
    )
    forged_scope = replace(plan_key.scope, **scope_change)
    forged_plan_key = _rehashed_plan_key(plan_key, scope=forged_scope)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scope"):
        semantic.validate_plan_key_identity(forged_plan_key)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scope"):
        semantic.network_key_for_plan(forged_plan_key, network_tier="preferred")


@pytest.mark.parametrize(
    "source_file_change",
    [
        {"entry_id": "uhc"},
        {"family": "ifp"},
        {"collection_kind": identity.PLAN_REFERENCE},
        {"file_name": "JSON_Providers_AZMA.json"},
        {"source_file_id": "f" * 64},
    ],
)
def test_rehashed_plan_key_rejects_every_forged_source_file_field(
    source_file_change,
):
    plan_key = semantic.plan_key_for_scope(
        _scope(),
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2026,
    )
    forged_source_file = replace(plan_key.scope.source_file, **source_file_change)
    forged_scope = replace(plan_key.scope, source_file=forged_source_file)
    forged_plan_key = _rehashed_plan_key(plan_key, scope=forged_scope)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scope"):
        semantic.network_key_for_plan(forged_plan_key, network_tier="preferred")


@pytest.mark.parametrize(
    "plan_change",
    [
        {"plan_id_type": ""},
        {"plan_id": ""},
        {"plan_year": True},
    ],
)
def test_rehashed_plan_key_rejects_invalid_plan_dimensions(plan_change):
    plan_key = semantic.plan_key_for_scope(
        _scope(),
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2026,
    )
    forged_plan_key = _rehashed_plan_key(plan_key, **plan_change)

    with pytest.raises(identity.UHCProviderFileIdentityError):
        semantic.network_key_for_plan(forged_plan_key, network_tier="preferred")


def test_network_validator_rejects_direct_nested_and_tier_forgery():
    plan_key = semantic.plan_key_for_scope(
        _scope(),
        plan_id_type="hios",
        plan_id="12345AZ001",
        plan_year=2026,
    )
    network_key = semantic.network_key_for_plan(plan_key, network_tier="preferred")
    forged_plan_key = _rehashed_plan_key(
        plan_key,
        scope=replace(plan_key.scope, market="forged"),
    )
    forged_nested_key = _rehashed_network_key(
        network_key,
        plan_key=forged_plan_key,
    )
    forged_tier_key = _rehashed_network_key(network_key, network_tier="")

    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scope"):
        semantic.validate_network_key_identity(forged_nested_key)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="network_tier"):
        semantic.validate_network_key_identity(forged_tier_key)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="network key"):
        semantic.validate_network_key_identity(
            replace(network_key, network_key_id="f" * 64)
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="plan key type"):
        semantic.validate_plan_key_identity(object())
    with pytest.raises(identity.UHCProviderFileIdentityError, match="network key type"):
        semantic.validate_network_key_identity(object())
    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scope type"):
        semantic.plan_key_for_scope(
            object(),
            plan_id_type="hios",
            plan_id="12345AZ001",
            plan_year=2026,
        )


def test_dataset_build_rejects_empty_duplicate_and_invalid_scope_inputs():
    scope = _scope()
    artifact = semantic.UHCArtifactDigest(scope.source_file.source_file_id, "a" * 64)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="no logical scopes"):
        _dataset_identity([], [])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scopes"):
        _dataset_identity(None, [])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="repeats a source"):
        _dataset_identity([scope, scope], [artifact])


def test_dataset_build_rejects_invalid_artifact_inputs():
    scope = _scope()
    artifact = semantic.UHCArtifactDigest(scope.source_file.source_file_id, "a" * 64)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="artifact digest type"):
        _dataset_identity([scope], [object()])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="artifact digests"):
        _dataset_identity([scope], None)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="repeats an artifact"):
        _dataset_identity([scope], [artifact, artifact])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="SHA-256"):
        _dataset_identity([scope], [replace(artifact, artifact_sha256="bad")])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="SHA-256"):
        _dataset_identity([scope], [replace(artifact, source_file_id="bad")])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="do not match"):
        _dataset_identity(
            [scope],
            [
                artifact,
                semantic.UHCArtifactDigest("f" * 64, "b" * 64),
            ],
        )


def test_dataset_build_rejects_invalid_as_of_and_plan_year_inputs():
    scope = _scope()
    artifact = semantic.UHCArtifactDigest(scope.source_file.source_file_id, "a" * 64)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="ISO date"):
        _dataset_identity([scope], [artifact], as_of="July 22")
    with pytest.raises(identity.UHCProviderFileIdentityError, match="plan_years"):
        _dataset_identity([scope], [artifact], plan_years=[2026, 2026])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="plan_years"):
        _dataset_identity([scope], [artifact], plan_years=None)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="plan_years"):
        _dataset_identity([scope], [artifact], plan_years=[])
    with pytest.raises(identity.UHCProviderFileIdentityError, match="ISO date"):
        _dataset_identity([scope], [artifact], as_of=None)
    with pytest.raises(identity.UHCProviderFileIdentityError, match="ISO date"):
        _dataset_identity([scope], [artifact], as_of="20260722")


def test_dataset_build_validator_rejects_direct_noncanonical_construction():
    scopes = (_scope(), _scope("JSON_Providers_AZIEX.json"))
    artifacts = tuple(
        semantic.UHCArtifactDigest(scope.source_file.source_file_id, digest * 64)
        for scope, digest in zip(scopes, ("a", "b"), strict=True)
    )
    build_identity = _dataset_identity(scopes, artifacts)

    with pytest.raises(identity.UHCProviderFileIdentityError, match="type"):
        semantic.validate_dataset_build_identity(object())
    with pytest.raises(identity.UHCProviderFileIdentityError, match="dataset build"):
        semantic.validate_dataset_build_identity(
            replace(build_identity, dataset_build_id="f" * 64)
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="dataset build"):
        semantic.validate_dataset_build_identity(
            replace(
                build_identity,
                logical_scopes=tuple(reversed(build_identity.logical_scopes)),
            )
        )
    with pytest.raises(identity.UHCProviderFileIdentityError, match="logical scope"):
        semantic.validate_dataset_build_identity(
            replace(
                build_identity,
                logical_scopes=(
                    replace(build_identity.logical_scopes[0], product="FORGED"),
                    build_identity.logical_scopes[1],
                ),
            )
        )
