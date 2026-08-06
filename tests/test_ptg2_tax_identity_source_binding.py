# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tests for the pathless PTG tax-sidecar rate-source binding."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts import ptg2_tax_identity_source_binding as source_binding
from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
)
from process.ptg_parts.ptg2_tax_identity_source_binding import (
    PTG2_TAX_IDENTITY_RATE_SOURCE_BINDING_CONTRACT,
    TaxIdentityRateSourceBinding,
    TaxIdentityRateSourceBindingError,
    bind_tax_identity_sidecar_to_rate_source,
    bind_tax_identity_sidecars_to_rate_sources,
    bind_tax_sidecar_source_key,
    bind_tax_source_sidecars,
    build_tax_identity_rate_source_binding_index,
    build_tax_source_bindings,
)
from tests.ptg2_v4_graph_compiler_test_support import _write_tax_identity


def _identity(digit: str) -> SharedPhysicalArtifactIdentity:
    return SharedPhysicalArtifactIdentity(
        source_type="in_network",
        identity_kind="logical_json_sha256_v1",
        identity_sha256=digit * 64,
    )


def _assignment(source_key: int, digit: str) -> SharedSnapshotSourceAssignment:
    identity = _identity(digit)
    return SharedSnapshotSourceAssignment(
        source_key=source_key,
        identity=identity,
        source_trace_set_hash="a" * 64,
        source_trace_hashes=("b" * 64,),
        raw_container_sha256="c" * 64,
        logical_json_sha256=identity.identity_sha256,
        logical_hash_deferred=False,
    )


def _error() -> str:
    return "ptg2_tax_identity_rate_source_binding_invalid"


def test_source_binding_index_is_dense_immutable_and_redacted():
    first = _assignment(0, "1")
    second = _assignment(1, "2")

    bindings = build_tax_source_bindings((second, first))

    assert bindings[first.identity].as_dict() == {
        "contract": PTG2_TAX_IDENTITY_RATE_SOURCE_BINDING_CONTRACT,
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "1" * 64,
        "source_key": 0,
    }
    assert bindings[first.identity].identity == first.identity
    assert len(bindings) == 2
    assert tuple(bindings) == tuple(sorted((first.identity, second.identity)))
    assert "1" * 16 not in repr(bindings[first.identity])
    assert "1" * 16 not in repr(bindings)
    with pytest.raises(TypeError):
        bindings[first.identity] = bindings[second.identity]


def test_source_binding_index_redacts_iterator_failure():
    def failing_assignments():
        yield _assignment(0, "1")
        raise RuntimeError("private iterator detail")

    with pytest.raises(TaxIdentityRateSourceBindingError) as raised:
        build_tax_source_bindings(failing_assignments())

    assert str(raised.value) == _error()
    assert "private iterator detail" not in str(raised.value)


def test_source_binding_rejects_normalizer_contract_drift(monkeypatch):
    monkeypatch.setattr(
        source_binding,
        "normalized_physical_artifact_identity",
        lambda _value: _identity("2"),
    )

    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        build_tax_source_bindings((_assignment(0, "1"),))


def test_raw_container_identity_is_supported():
    assignment = _assignment(0, "1")
    raw_identity = SharedPhysicalArtifactIdentity(
        source_type="in_network",
        identity_kind="raw_container_sha256_v1",
        identity_sha256="1" * 64,
    )
    assignment = replace(assignment, identity=raw_identity)

    bindings = build_tax_source_bindings((assignment,))

    assert bindings[raw_identity].identity_kind == "raw_container_sha256_v1"


def test_tax_sidecar_gets_fresh_strict_binding_without_mutating_input():
    assignment = _assignment(0, "3")
    index = build_tax_source_bindings((assignment,))
    sidecar_by_field = {
        "name": "provider_group_tax_identity",
        "path": "/private/temporary/tax.ptg2tax",
        "sha256": "d" * 64,
    }

    bound = bind_tax_sidecar_source_key(
        sidecar_by_field,
        physical_identity=assignment.identity.as_dict(),
        binding_index=index,
    )

    assert bound is not sidecar_by_field
    assert bound["path"] == sidecar_by_field["path"]
    assert bound["physical_source_binding"] == index[assignment.identity].as_dict()
    assert "physical_source_binding" not in sidecar_by_field
    assert set(bound["physical_source_binding"]) == {
        "contract",
        "source_type",
        "identity_kind",
        "identity_sha256",
        "source_key",
    }


def test_tax_sidecar_name_is_canonicalized_before_binding():
    assignment = _assignment(0, "3")

    bound = bind_tax_sidecar_source_key(
        {"name": " provider_group_tax_identity "},
        physical_identity=assignment.identity,
        binding_index=build_tax_source_bindings((assignment,)),
    )

    assert bound["name"] == "provider_group_tax_identity"
    assert bound["physical_source_binding"]["source_key"] == 0


def test_non_tax_sidecar_is_copied_without_requiring_identity_or_index():
    sidecar_by_field = {
        "name": "provider_forward",
        "path": "/private/temporary/graph",
        "physical_source_binding": {"preserved": True},
    }

    copied = bind_tax_sidecar_source_key(
        sidecar_by_field,
        physical_identity=None,
        binding_index=None,
    )

    assert copied == sidecar_by_field
    assert copied is not sidecar_by_field


def test_compatibility_exports_resolve_to_validated_implementations():
    assert bind_tax_identity_sidecar_to_rate_source is bind_tax_sidecar_source_key
    assert build_tax_identity_rate_source_binding_index is build_tax_source_bindings
    assert bind_tax_identity_sidecars_to_rate_sources is bind_tax_source_sidecars


def test_tax_sidecar_vector_requires_exact_source_coverage():
    first = _assignment(0, "1")
    second = _assignment(1, "2")
    first_sidecar_by_field = {
        "name": "provider_group_tax_identity",
        "path": "first",
    }
    second_sidecar_by_field = {
        "name": "provider_group_tax_identity",
        "path": "second",
    }

    bound = bind_tax_source_sidecars(
        (
            (second_sidecar_by_field, second.identity),
            (first_sidecar_by_field, first.identity.as_dict()),
        ),
        binding_index=build_tax_source_bindings((first, second)),
    )

    assert [entry["physical_source_binding"]["source_key"] for entry in bound] == [
        1,
        0,
    ]
    assert all(
        "physical_source_binding" not in entry
        for entry in (first_sidecar_by_field, second_sidecar_by_field)
    )


@pytest.mark.parametrize(
    "source_entries",
    [
        (),
        "not-a-source-vector",
        {"not": "a-source-vector"},
        (({"name": "provider_group_tax_identity"}, _identity("1")),),
        (
            ({"name": "provider_group_tax_identity"}, _identity("1")),
            ({"name": "provider_group_tax_identity"}, _identity("1")),
        ),
        (
            ({"name": "provider_group_tax_identity"}, _identity("1")),
            ({"name": "provider_group_tax_identity"}, _identity("3")),
        ),
        (
            ({"name": "provider_forward"}, _identity("1")),
            ({"name": "provider_group_tax_identity"}, _identity("2")),
        ),
        (object(), object()),
    ],
)
def test_tax_sidecar_vector_rejects_incomplete_or_ambiguous_coverage(
    source_entries,
):
    index = build_tax_source_bindings((_assignment(0, "1"), _assignment(1, "2")))

    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        bind_tax_source_sidecars(source_entries, binding_index=index)


def test_tax_sidecar_vector_redacts_iterator_failure():
    def failing_sources():
        yield ({"name": "provider_group_tax_identity"}, _identity("1"))
        raise RuntimeError("private vector detail")

    with pytest.raises(TaxIdentityRateSourceBindingError) as raised:
        bind_tax_source_sidecars(
            failing_sources(),
            binding_index=build_tax_source_bindings((_assignment(0, "1"),)),
        )

    assert str(raised.value) == _error()
    assert "private vector detail" not in str(raised.value)


def test_tax_sidecar_vector_rejects_untrusted_index():
    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        bind_tax_source_sidecars((), binding_index=MappingProxyType({}))


@pytest.mark.parametrize(
    "assignments",
    [
        (),
        "not-an-assignment-vector",
        {"not": "an-assignment-vector"},
        (object(),),
        (_assignment(-1, "4"),),
        (_assignment(True, "4"),),
        (_assignment(1, "4"),),
        (_assignment(0, "4"), _assignment(0, "5")),
        (_assignment(0, "4"), _assignment(1, "4")),
        (_assignment(0, "4"), _assignment(2, "5")),
        (replace(_assignment(0, "4"), identity={"invalid": "identity"}),),
    ],
)
def test_source_binding_index_rejects_incomplete_or_invalid_vectors(assignments):
    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        build_tax_source_bindings(assignments)


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "source_type": "INVALID SOURCE",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "6" * 64,
            "source_key": 0,
        },
        {
            "source_type": 123456789,
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "6" * 64,
            "source_key": 0,
        },
        {
            "source_type": "123456789",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "6" * 64,
            "source_key": 0,
        },
        {
            "source_type": "EXAMPLE.COM",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "6" * 64,
            "source_key": 0,
        },
        {
            "source_type": "in_network",
            "identity_kind": "unknown_identity",
            "identity_sha256": "6" * 64,
            "source_key": 0,
        },
        {
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "not-a-digest",
            "source_key": 0,
        },
        {
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "A" * 64,
            "source_key": 0,
        },
        {
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "6" * 64,
            "source_key": -1,
        },
        {
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "6" * 64,
            "source_key": True,
        },
        {
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "6" * 64,
            "source_key": 2**31,
        },
    ],
)
def test_binding_value_rejects_invalid_fields_without_echo(kwargs):
    with pytest.raises(TaxIdentityRateSourceBindingError) as raised:
        TaxIdentityRateSourceBinding(**kwargs)
    assert str(raised.value) == _error()
    assert "6" * 16 not in str(raised.value)


@pytest.mark.parametrize(
    ("sidecar", "identity", "index"),
    [
        (object(), None, None),
        ({}, None, None),
        ({"name": ""}, None, None),
        ({"name": "   "}, None, None),
        ({"name": 7}, None, None),
        (
            {
                "name": "provider_group_tax_identity",
                "physical_source_binding": {},
            },
            None,
            None,
        ),
        (
            {"name": "provider_group_tax_identity"},
            None,
            MappingProxyType({}),
        ),
        (
            {"name": "provider_group_tax_identity"},
            _identity("7"),
            {},
        ),
        (
            {"name": "provider_group_tax_identity"},
            _identity("7"),
            MappingProxyType({}),
        ),
        (
            {"name": "provider_group_tax_identity"},
            _identity("7"),
            MappingProxyType({_identity("7"): object()}),
        ),
    ],
)
def test_sidecar_binding_rejects_untrusted_inputs(sidecar, identity, index):
    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        bind_tax_sidecar_source_key(
            sidecar,
            physical_identity=identity,
            binding_index=index,
        )


def test_sidecar_binding_rejects_identity_not_in_published_dictionary():
    index = build_tax_source_bindings((_assignment(0, "8"),))

    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        bind_tax_sidecar_source_key(
            {"name": "provider_group_tax_identity"},
            physical_identity=_identity("9"),
            binding_index=index,
        )


def test_sidecar_binding_redacts_normalization_failure():
    class SensitiveSourceType:
        def __str__(self):
            raise RuntimeError("RAW-SECRET-VALUE")

    with pytest.raises(TaxIdentityRateSourceBindingError) as raised:
        bind_tax_sidecar_source_key(
            {"name": "provider_group_tax_identity"},
            physical_identity={
                "source_type": SensitiveSourceType(),
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": "1" * 64,
            },
            binding_index=build_tax_source_bindings((_assignment(0, "1"),)),
        )

    assert str(raised.value) == _error()
    assert "RAW-SECRET-VALUE" not in str(raised.value)


def test_sidecar_binding_rejects_corrupted_published_binding():
    assignment = _assignment(0, "1")
    index = build_tax_source_bindings((assignment,))
    binding = index[assignment.identity]
    object.__setattr__(binding, "source_key", -1)

    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        bind_tax_sidecar_source_key(
            {"name": "provider_group_tax_identity"},
            physical_identity=assignment.identity,
            binding_index=index,
        )


def test_sidecar_binding_rejects_binding_stored_under_another_identity():
    requested = _identity("1")
    index = build_tax_source_bindings((_assignment(0, "1"),))
    other_binding = TaxIdentityRateSourceBinding.from_assignment(_assignment(0, "2"))
    object.__setattr__(
        index,
        "_binding_by_identity",
        MappingProxyType({requested: other_binding}),
    )

    with pytest.raises(TaxIdentityRateSourceBindingError, match=_error()):
        bind_tax_sidecar_source_key(
            {"name": "provider_group_tax_identity"},
            physical_identity=requested,
            binding_index=index,
        )


def test_v4_binding_compatibility(tmp_path):
    """Keep the existing V4 compiler manifest contract unchanged."""

    entry = _write_tax_identity(
        tmp_path / "tax.sidecar",
        shard_id="source-a",
        tax_observations=[
            (bytes.fromhex("10" * 16), 1, bytes.fromhex("11" * 32)),
        ],
    )
    entry["physical_source_binding"] = {
        "contract": PTG2_TAX_IDENTITY_RATE_SOURCE_BINDING_CONTRACT,
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "1" * 64,
        "source_key": 0,
    }

    artifact, _byte_count = compiler._tax_identity_artifact_manifest(entry)

    assert "physical_source_binding" not in artifact["metadata"]
