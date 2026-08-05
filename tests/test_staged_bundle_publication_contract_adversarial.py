from __future__ import annotations

import ast
from dataclasses import fields
from importlib import util
import inspect
from pathlib import Path
import sys
import types

import pytest

REPO_ROOT = Path(__file__).parents[1]
if "process" not in sys.modules:
    process_package = types.ModuleType("process")
    process_package.__path__ = [str(REPO_ROOT / "process")]
    sys.modules["process"] = process_package
MODULE_NAME = "process.staged_bundle_publication_contract"
if MODULE_NAME in sys.modules:
    publication = sys.modules[MODULE_NAME]
else:
    module_path = REPO_ROOT / "process" / "staged_bundle_publication_contract.py"
    module_spec = util.spec_from_file_location(MODULE_NAME, module_path)
    assert module_spec is not None and module_spec.loader is not None
    publication = util.module_from_spec(module_spec)
    sys.modules[MODULE_NAME] = publication
    module_spec.loader.exec_module(publication)

FIXED_INPUTS = {
    "serving_authority": "none",
    "publication_authorized": False,
    "cleanup_authorized": False,
    "reverse_swap_authorized": False,
    "database_io_enabled": False,
    "retained_old_required": True,
    "automatic_old_deletion_enabled": False,
    "automatic_gc_enabled": False,
}
FINGERPRINT_FIELDS = tuple(
    "schema_sha256 columns_sha256 constraints_sha256 indexes_sha256 owner_sha256 "
    "privileges_sha256".split()
)


class AlwaysEqualHazard:
    def __eq__(self, _other):
        raise AssertionError("hostile equality executed")

    def __iter__(self):
        raise AssertionError("hostile iteration executed")

    def __len__(self):
        raise AssertionError("hostile length executed")

    def __bool__(self):
        raise AssertionError("hostile truthiness executed")


class IterationHazard:
    def __iter__(self):
        raise AssertionError("hostile iteration executed")

    def __len__(self):
        raise AssertionError("hostile length executed")

    def __bool__(self):
        raise AssertionError("hostile truthiness executed")


def _sha(character: str) -> str:
    return character * 64


def _fingerprints(character: str = "b") -> dict[str, object]:
    return {name: _sha(character) for name in FINGERPRINT_FIELDS}


def _relation(index: int = 0) -> dict[str, object]:
    role = f"role_{index:04d}"
    live_relation = f"live_{index:04d}"
    fingerprints = _fingerprints()
    return {
        "role": role,
        "live_relation": live_relation,
        "stage_relation": publication.derive_stage_relation_name(
            "mrf", "build-run-synthetic", role, live_relation
        ),
        "old_relation": f"{live_relation}_old",
        "observed_live_oid": index + 1,
        "observed_stage_oid": index + 10_001,
        "observed_old_oid": None,
        "stage_logged": True,
        "old_relation_expected_absent": True,
        "catalog_parity_verified": True,
        "stage_fingerprints": fingerprints,
        "live_fingerprints": dict(fingerprints),
    }


def _bundle(relations: tuple[object, ...] | None = None) -> dict[str, object]:
    predecessor = "generation-synthetic-001"
    return {
        "schema": "mrf",
        "run_id": "build-run-synthetic",
        "generation_id": "generation-synthetic-002",
        "expected_predecessor_generation_id": predecessor,
        "expected_current_generation_id": predecessor,
        "expected_previous_generation_id": "generation-synthetic-000",
        "source_vector_sha256": _sha("a"),
        "source_vector_canonical": True,
        "relations": (_relation(),) if relations is None else relations,
        **FIXED_INPUTS,
    }


def _build():
    return publication.build_staged_bundle_publication_contract(_bundle())


def _init_fields(instance: object) -> dict[str, object]:
    return {
        field.name: getattr(instance, field.name)
        for field in fields(instance)
        if field.init
    }


def _reject(callable_) -> None:
    with pytest.raises(
        publication.StagedBundlePublicationContractError,
        match="^staged_bundle_publication_contract_invalid$",
    ):
        callable_()


def test_public_limit_accepts_exactly_4096_relations() -> None:
    assert publication.MAX_STAGED_BUNDLE_RELATIONS == 4096
    assert "4,096" in publication.__doc__
    relations = tuple(_relation(index) for index in range(4096))

    descriptor = publication.build_staged_bundle_publication_contract(
        _bundle(relations)
    )

    assert len(descriptor.relations) == 4096
    assert descriptor.relation_order[0] == "live_0000"
    assert descriptor.relation_order[-1] == "live_4095"


def test_limit_rejects_4097_before_relation_traversal(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("relation traversal occurred before limit check")

    monkeypatch.setattr(publication, "_relation_from_raw", fail_if_called)
    _reject(
        lambda: publication.build_staged_bundle_publication_contract(
            _bundle((object(),) * 4097)
        )
    )


def test_direct_relation_intent_accepts_only_canonical_immutable_state() -> None:
    original = _build().relations[0]
    canonical = publication.StagedRelationIntent(**_init_fields(original))

    assert canonical == original
    assert type(canonical.observed_stage_oid) is int
    assert type(canonical.stage_fingerprints) is tuple
    assert type(canonical.live_fingerprints) is tuple

    class StringSubclass(str):
        pass

    class IntegerSubclass(int):
        pass

    class TupleSubclass(tuple):
        pass

    invalid_by_field = (
        ("role", StringSubclass(original.role)),
        ("stage_relation", StringSubclass(original.stage_relation)),
        ("observed_live_oid", True),
        ("observed_stage_oid", True),
        ("observed_stage_oid", -1),
        ("observed_stage_oid", IntegerSubclass(original.observed_stage_oid)),
        ("observed_old_oid", 301),
        ("stage_logged", False),
        ("old_relation_expected_absent", False),
        ("catalog_parity_verified", False),
        ("old_relation", "different_old"),
        ("stage_relation", original.role),
        ("stage_relation", "caller_stage"),
        ("stage_fingerprints", list(original.stage_fingerprints)),
        ("stage_fingerprints", TupleSubclass(original.stage_fingerprints)),
        ("stage_fingerprints", IterationHazard()),
        ("live_fingerprints", list(original.live_fingerprints)),
        ("live_fingerprints", (_sha("f"), *original.live_fingerprints[1:])),
    )
    for field_name, invalid_value in invalid_by_field:
        relation_fields_by_name = _init_fields(original)
        relation_fields_by_name[field_name] = invalid_value
        _reject(
            lambda fields=relation_fields_by_name: (
                publication.StagedRelationIntent(**fields)
            )
        )


def test_direct_stage_name_is_structural_but_descriptor_hash_is_exact() -> None:
    original = _build().relations[0]
    alternate_digest = "f" * 24
    if original.stage_relation.endswith(alternate_digest):
        alternate_digest = "e" * 24
    structural_stage = f"{original.live_relation}_stage_{alternate_digest}"
    relation_fields_by_name = _init_fields(original)
    relation_fields_by_name["stage_relation"] = structural_stage

    standalone = publication.StagedRelationIntent(**relation_fields_by_name)

    assert standalone.stage_relation == structural_stage
    numeric_stage = f"{original.live_relation}_stage_{'1' * 24}"
    relation_fields_by_name["stage_relation"] = numeric_stage
    assert publication.StagedRelationIntent(**relation_fields_by_name)
    raw = _bundle()
    raw["relations"][0]["stage_relation"] = structural_stage
    _reject(lambda: publication.build_staged_bundle_publication_contract(raw))

    malformed_stages = (
        f"other_live_stage_{'f' * 24}",
        f"{original.live_relation}_stage_{'f' * 23}",
        f"{original.live_relation}_stage_{'f' * 25}",
    )
    for malformed_stage in malformed_stages:
        relation_fields_by_name = _init_fields(original)
        relation_fields_by_name["stage_relation"] = malformed_stage
        _reject(
            lambda fields=relation_fields_by_name: (
                publication.StagedRelationIntent(**fields)
            )
        )


def test_direct_descriptor_rejects_magic_without_executing_it() -> None:
    descriptor = _build()
    invalid_by_field = (
        ("relations", IterationHazard()),
        ("mode", AlwaysEqualHazard()),
        ("relation_order", AlwaysEqualHazard()),
        ("relation_order", (AlwaysEqualHazard(),)),
        ("lock_order", AlwaysEqualHazard()),
        ("source_fence_sha256", AlwaysEqualHazard()),
        ("contract_sha256", AlwaysEqualHazard()),
    )
    for field_name, invalid_value in invalid_by_field:
        descriptor_fields_by_name = _init_fields(descriptor)
        descriptor_fields_by_name[field_name] = invalid_value
        _reject(
            lambda fields=descriptor_fields_by_name: (
                publication.StagedBundlePublicationDescriptor(**fields)
            )
        )

    object.__setattr__(descriptor, "serving_authority", AlwaysEqualHazard())
    _reject(lambda: publication.validate_staged_bundle_publication_contract(descriptor))


def test_direct_descriptor_binds_exact_canonical_derived_state() -> None:
    descriptor = _build()
    reconstructed = publication.StagedBundlePublicationDescriptor(
        **_init_fields(descriptor)
    )
    assert reconstructed == descriptor

    invalid_by_field = (
        ("relations", list(descriptor.relations)),
        ("mode", "initial"),
        ("relation_order", ("different_live",)),
        ("relation_order", ("safe_name",) * 4097),
        ("lock_order", ()),
        ("source_fence_sha256", _sha("f")),
        ("pointer_fence_sha256", _sha("e")),
        ("oid_fence_sha256", _sha("d")),
        ("contract_sha256", _sha("c")),
        ("source_vector_canonical", 1),
    )
    for field_name, invalid_value in invalid_by_field:
        descriptor_fields_by_name = _init_fields(descriptor)
        descriptor_fields_by_name[field_name] = invalid_value
        _reject(
            lambda fields=descriptor_fields_by_name: (
                publication.StagedBundlePublicationDescriptor(**fields)
            )
        )


def _public_sensitive_shapes() -> tuple[str, ...]:
    return (
        "run_123456789",
        "run_1234567893",
        "runalpha123456789omega",
        "runalpha12_3456789omega",
        "run_til1_" + "a" * 32,
        "runalphatil1_" + "a" * 32 + "omega",
        "run_tih1_" + "b" * 64,
        "run_tip1_" + "c" * 64,
        "run_api_key_" + "x" * 24,
        "run_api__key_" + "x" * 24,
        "run-api.key-" + "x" * 24,
        "run_ghp_" + "x" * 20,
        "run_github_pat_" + "x" * 20,
        "run_sk_live_" + "x" * 20,
        "run_sk-" + "x" * 20,
    )


def _postgres_sensitive_shapes() -> tuple[str, ...]:
    return (
        "tenant_123456789",
        "npi_1234567893",
        "relalpha123456789omega",
        "relalpha12_3456789omega",
        "relation_til1_" + "a" * 32,
        "reltil1_" + "a" * 32 + "omega",
        "relation_api_key_" + "x" * 24,
        "relation_ghp_" + "x" * 20,
        "relation_github_pat_" + "x" * 20,
        "relation_sk_live_" + "x" * 20,
    )


def test_sensitive_shape_boundaries_preserve_neutral_identifiers() -> None:
    allowed_public_ids = (
        "runalpha12345678omega",
        "run_til1_" + "a" * 31,
        "run_til1_" + "a" * 33,
        "run_tih1_" + "b" * 63,
        "run_tih1_" + "b" * 65,
        "run_tip1_" + "c" * 63,
        "run_tip1_" + "c" * 65,
        "run_api_keyish_synthetic",
    )
    for run_id in allowed_public_ids:
        raw = _bundle()
        raw["run_id"] = run_id
        relation = raw["relations"][0]
        relation["stage_relation"] = publication.derive_stage_relation_name(
            raw["schema"], run_id, relation["role"], relation["live_relation"]
        )
        assert publication.build_staged_bundle_publication_contract(raw)

    allowed_pg_identifiers = (
        "relalpha12345678omega",
        "til1_" + "a" * 31,
        "til1_" + "a" * 33,
        "relation_api_keyish",
    )
    for role in allowed_pg_identifiers:
        assert publication.derive_stage_relation_name(
            "mrf", "build-run-synthetic", role, "live_safe"
        ).startswith("live_safe_stage_")


def test_rejects_sensitive_shapes_from_every_public_id_field() -> None:
    public_fields = (
        "run_id",
        "generation_id",
        "expected_predecessor_generation_id",
        "expected_current_generation_id",
        "expected_previous_generation_id",
    )
    for field_name in public_fields:
        for sensitive_shape in _public_sensitive_shapes():
            raw = _bundle()
            raw[field_name] = sensitive_shape
            _reject(
                lambda raw=raw: (
                    publication.build_staged_bundle_publication_contract(raw)
                )
            )


def test_rejects_sensitive_shapes_from_every_postgresql_identifier() -> None:
    intent = _build().relations[0]
    descriptor = _build()
    for sensitive_shape in _postgres_sensitive_shapes():
        for position in ("schema", "role", "live"):
            arguments = ["mrf", "build-run-synthetic", "role_safe", "live_safe"]
            arguments[{"schema": 0, "role": 2, "live": 3}[position]] = sensitive_shape
            _reject(
                lambda arguments=arguments: (
                    publication.derive_stage_relation_name(*arguments)
                )
            )
        for field_name in ("role", "live_relation", "stage_relation", "old_relation"):
            relation_fields_by_name = _init_fields(intent)
            relation_fields_by_name[field_name] = sensitive_shape
            _reject(
                lambda fields=relation_fields_by_name: (
                    publication.StagedRelationIntent(**fields)
                )
            )
        for field_name in ("relation_order", "lock_order"):
            descriptor_fields_by_name = _init_fields(descriptor)
            descriptor_fields_by_name[field_name] = (sensitive_shape,)
            _reject(
                lambda fields=descriptor_fields_by_name: (
                    publication.StagedBundlePublicationDescriptor(**fields)
                )
            )


def test_neutral_npi_and_ein_schema_words_remain_allowed() -> None:
    stage_name = publication.derive_stage_relation_name(
        "npi_ein_schema",
        "run-npi-ein-evidence",
        "tax_identity_npi_evidence",
        "entity_ein_live",
    )
    assert stage_name.startswith("entity_ein_live_stage_")


def test_exact_dict_and_fixed_literals_reject_hostile_or_wrong_state() -> None:
    raw = _bundle()
    schema = raw.pop("schema")
    raw[object()] = schema
    _reject(lambda: publication.build_staged_bundle_publication_contract(raw))

    raw = _bundle()
    raw["serving_authority"] = "some"
    _reject(lambda: publication.build_staged_bundle_publication_contract(raw))

    raw = _bundle()
    raw["serving_authority"] = "".join(("n", "o", "n", "e"))
    assert publication.build_staged_bundle_publication_contract(raw)


def test_post_init_wraps_unexpected_failures_without_values(monkeypatch) -> None:
    relation_fields_by_name = _init_fields(_build().relations[0])
    monkeypatch.setattr(
        publication,
        "_validated_relation_values",
        lambda _relation: (_ for _ in ()).throw(ValueError("sensitive")),
    )
    _reject(lambda: publication.StagedRelationIntent(**relation_fields_by_name))

    monkeypatch.undo()
    descriptor_fields_by_name = _init_fields(_build())
    monkeypatch.setattr(
        publication,
        "_validated_descriptor_state",
        lambda _descriptor: (_ for _ in ()).throw(ValueError("sensitive")),
    )
    _reject(
        lambda: publication.StagedBundlePublicationDescriptor(
            **descriptor_fields_by_name
        )
    )


def test_package_is_stdlib_only_and_has_no_io_or_mutation_calls() -> None:
    modules = (
        publication,
        sys.modules["process.staged_bundle_publication_contract_core"],
    )
    trees = tuple(ast.parse(inspect.getsource(module)) for module in modules)
    imported_roots = {
        alias.name.split(".", 1)[0]
        for tree in trees
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        node.module.split(".", 1)[0]
        for tree in trees
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.level == 0
        and node.module != "__future__"
    )
    forbidden_calls = set(
        "open exec eval connect execute remove rename replace_file unlink".split()
    )
    called_names = {
        node.func.id
        for tree in trees
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    assert imported_roots <= {
        "dataclasses",
        "hashlib",
        "hmac",
        "json",
        "re",
        "typing",
    }
    assert called_names.isdisjoint(forbidden_calls)
