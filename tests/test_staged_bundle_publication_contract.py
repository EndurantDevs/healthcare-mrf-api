from __future__ import annotations

from dataclasses import FrozenInstanceError
from importlib import util
from pathlib import Path
import re
import sys
import types

import pytest

REPO_ROOT = Path(__file__).parents[1]
if "process" not in sys.modules:
    process_package = types.ModuleType("process")
    process_package.__path__ = [str(REPO_ROOT / "process")]
    sys.modules["process"] = process_package
MODULE_FILE = REPO_ROOT / "process" / "staged_bundle_publication_contract.py"
MODULE_SPEC = util.spec_from_file_location(
    "process.staged_bundle_publication_contract", MODULE_FILE
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
publication = util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = publication
MODULE_SPEC.loader.exec_module(publication)


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


def _sha(character: str) -> str:
    return character * 64


def _fingerprints(character: str) -> dict[str, object]:
    return {name: _sha(character) for name in FINGERPRINT_FIELDS}


def _relation(
    role: str,
    live_relation: str,
    *,
    run_id: str = "build-run-synthetic",
    live_oid: int | None = 101,
    stage_oid: int = 201,
    fingerprint_character: str = "b",
) -> dict[str, object]:
    fingerprints = _fingerprints(fingerprint_character)
    return {
        "role": role,
        "live_relation": live_relation,
        "stage_relation": publication.derive_stage_relation_name(
            "mrf", run_id, role, live_relation
        ),
        "old_relation": f"{live_relation}_old",
        "observed_live_oid": live_oid,
        "observed_stage_oid": stage_oid,
        "observed_old_oid": None,
        "stage_logged": True,
        "old_relation_expected_absent": True,
        "catalog_parity_verified": True,
        "stage_fingerprints": fingerprints,
        "live_fingerprints": None if live_oid is None else dict(fingerprints),
    }


def _bundle(*, replacement: bool = True) -> dict[str, object]:
    relation_parameters = (
        ("identity_member", "identity_live", 101, 201, "b"),
        ("evidence_member", "evidence_live", 102, 202, "c"),
    )
    relations = tuple(
        _relation(
            role,
            live,
            live_oid=live_oid if replacement else None,
            stage_oid=stage_oid,
            fingerprint_character=character,
        )
        for role, live, live_oid, stage_oid, character in relation_parameters
    )
    predecessor = "generation-synthetic-001" if replacement else None
    return {
        "schema": "mrf",
        "run_id": "build-run-synthetic",
        "generation_id": "generation-synthetic-002",
        "expected_predecessor_generation_id": predecessor,
        "expected_current_generation_id": predecessor,
        "expected_previous_generation_id": (
            "generation-synthetic-000" if replacement else None
        ),
        "source_vector_sha256": _sha("a"),
        "source_vector_canonical": True,
        "relations": relations,
        **FIXED_INPUTS,
    }


def _build(*, replacement: bool = True):
    return publication.build_staged_bundle_publication_contract(
        _bundle(replacement=replacement)
    )


def _assert_revalidation_rejects(descriptor: object) -> None:
    with pytest.raises(publication.StagedBundlePublicationContractError):
        publication.validate_staged_bundle_publication_contract(descriptor)


def test_accepts_replacement_and_exposes_only_preparation_state() -> None:
    descriptor = _build()

    assert descriptor.mode == "replacement"
    assert descriptor.relation_order == ("evidence_live", "identity_live")
    expected_locks = sorted(
        name
        for relation in descriptor.relations
        for name in (relation.live_relation, relation.stage_relation)
    )
    assert descriptor.lock_order == tuple(expected_locks)
    assert all(relation.stage_logged for relation in descriptor.relations)
    assert all(
        relation.live_fingerprints == relation.stage_fingerprints
        for relation in descriptor.relations
    )
    assert all(
        getattr(descriptor, name) is value for name, value in FIXED_INPUTS.items()
    )
    with pytest.raises(FrozenInstanceError):
        descriptor.generation_id = "generation-synthetic-003"


def test_accepts_initial_bundle_without_live_or_pointer_state() -> None:
    descriptor = _build(replacement=False)

    assert descriptor.mode == "initial"
    assert all(relation.observed_live_oid is None for relation in descriptor.relations)
    assert all(relation.live_fingerprints is None for relation in descriptor.relations)
    assert descriptor.lock_order == tuple(
        sorted(relation.stage_relation for relation in descriptor.relations)
    )


def test_canonical_digest_and_order_are_deterministic() -> None:
    raw = _bundle()
    reversed_bundle_dict = dict(reversed(tuple(raw.items())))
    reversed_bundle_dict["relations"] = tuple(reversed(raw["relations"]))

    first = publication.build_staged_bundle_publication_contract(raw)
    second = publication.build_staged_bundle_publication_contract(reversed_bundle_dict)

    assert first == second
    assert first.contract_sha256 == second.contract_sha256
    assert re.fullmatch(r"[0-9a-f]{64}", first.contract_sha256)


def test_stage_names_are_run_scoped_bounded_and_use_96_bit_suffixes() -> None:
    live = "r" * 59
    baseline = publication.derive_stage_relation_name(
        "mrf", "build-run-synthetic", "relation_member", live
    )

    assert len(baseline.encode("ascii")) == 63
    assert re.search(r"_stage_[0-9a-f]{24}$", baseline)
    assert baseline == publication.derive_stage_relation_name(
        "mrf", "build-run-synthetic", "relation_member", live
    )
    variants = (
        ("other", "build-run-synthetic", "relation_member", live),
        ("mrf", "build-run-other", "relation_member", live),
        ("mrf", "build-run-synthetic", "other_member", live),
        ("mrf", "build-run-synthetic", "relation_member", "s" * 59),
    )
    assert all(
        publication.derive_stage_relation_name(*variant) != baseline
        for variant in variants
    )


def test_fences_bind_source_pointer_oid_and_catalog_state_separately() -> None:
    baseline = _build()
    changed_source = _bundle()
    changed_source["source_vector_sha256"] = _sha("d")
    source = publication.build_staged_bundle_publication_contract(changed_source)
    changed_pointer = _bundle()
    changed_pointer["expected_previous_generation_id"] = "generation-synthetic-prior"
    pointer = publication.build_staged_bundle_publication_contract(changed_pointer)
    changed_oid = _bundle()
    changed_oid["relations"][0]["observed_stage_oid"] = 301
    oid = publication.build_staged_bundle_publication_contract(changed_oid)
    changed_catalog = _bundle()
    changed_catalog["relations"][0]["stage_fingerprints"]["owner_sha256"] = _sha("e")
    changed_catalog["relations"][0]["live_fingerprints"]["owner_sha256"] = _sha("e")
    catalog = publication.build_staged_bundle_publication_contract(changed_catalog)

    assert source.source_fence_sha256 != baseline.source_fence_sha256
    assert source.pointer_fence_sha256 == baseline.pointer_fence_sha256
    assert pointer.pointer_fence_sha256 != baseline.pointer_fence_sha256
    assert pointer.oid_fence_sha256 == baseline.oid_fence_sha256
    assert oid.oid_fence_sha256 != baseline.oid_fence_sha256
    assert catalog.source_fence_sha256 == baseline.source_fence_sha256
    assert catalog.contract_sha256 != baseline.contract_sha256


@pytest.mark.parametrize(
    "field_name", tuple(FIXED_INPUTS) + ("source_vector_canonical",)
)
def test_requires_exact_fail_closed_assertions(field_name: str) -> None:
    raw = _bundle()
    raw[field_name] = 1 if raw[field_name] is False else False

    with pytest.raises(publication.StagedBundlePublicationContractError):
        publication.build_staged_bundle_publication_contract(raw)


@pytest.mark.parametrize(
    ("level", "field_name"),
    (("bundle", "source_path"), ("relation", "ddl"), ("fingerprint", "table_path")),
)
def test_rejects_unknown_fields_at_every_mapping_level(
    level: str, field_name: str
) -> None:
    raw = _bundle()
    target = raw
    if level == "relation":
        target = raw["relations"][0]
    elif level == "fingerprint":
        target = raw["relations"][0]["stage_fingerprints"]
    target[field_name] = "not-retained"

    with pytest.raises(publication.StagedBundlePublicationContractError):
        publication.build_staged_bundle_publication_contract(raw)


def test_rejects_missing_empty_and_non_exact_container_types() -> None:
    raw = _bundle()
    del raw["generation_id"]
    with pytest.raises(publication.StagedBundlePublicationContractError):
        publication.build_staged_bundle_publication_contract(raw)

    class Dictionary(dict[str, object]):
        pass

    invalid_values = (
        Dictionary(_bundle()),
        {**_bundle(), "relations": []},
        {**_bundle(), "relations": ()},
        {**_bundle(), "relations": (Dictionary(_bundle()["relations"][0]),)},
    )
    for invalid in invalid_values:
        with pytest.raises(publication.StagedBundlePublicationContractError):
            publication.build_staged_bundle_publication_contract(invalid)


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("schema", "Bad-Schema"),
        ("run_id", "https://invalid.test/source"),
        ("run_id", "run-secretvalue"),
        ("generation_id", "generation-00123456789"),
        ("generation_id", "generation-raw-tin"),
        ("generation_id", "generation\u202e"),
    ),
)
def test_rejects_unsafe_identifiers_without_echoing(
    field_name: str, value: str
) -> None:
    raw = _bundle()
    raw[field_name] = value

    with pytest.raises(
        publication.StagedBundlePublicationContractError,
        match="^staged_bundle_publication_contract_invalid$",
    ) as error:
        publication.build_staged_bundle_publication_contract(raw)
    assert value not in str(error.value)


@pytest.mark.parametrize("value", (True, 0, -1, 2**32, "201"))
def test_rejects_invalid_postgresql_oids(value: object) -> None:
    raw = _bundle()
    raw["relations"][0]["observed_stage_oid"] = value
    with pytest.raises(publication.StagedBundlePublicationContractError):
        publication.build_staged_bundle_publication_contract(raw)


def test_rejects_old_mixed_or_duplicate_oid_state() -> None:
    cases = []
    old_present = _bundle()
    old_present["relations"][0]["observed_old_oid"] = 401
    cases.append(old_present)
    mixed = _bundle()
    mixed["relations"][0]["observed_live_oid"] = None
    mixed["relations"][0]["live_fingerprints"] = None
    cases.append(mixed)
    duplicate = _bundle()
    duplicate["relations"][1]["observed_stage_oid"] = 201
    cases.append(duplicate)
    local_duplicate = _bundle()
    local_duplicate["relations"][0]["observed_stage_oid"] = 101
    cases.append(local_duplicate)

    for raw in cases:
        with pytest.raises(publication.StagedBundlePublicationContractError):
            publication.build_staged_bundle_publication_contract(raw)


def test_rejects_relation_name_collisions_and_nondeterministic_names() -> None:
    cases = []
    wrong_old = _bundle()
    wrong_old["relations"][0]["old_relation"] = "wrong_old"
    cases.append(wrong_old)
    wrong_stage = _bundle()
    wrong_stage["relations"][0]["stage_relation"] = "caller_stage"
    cases.append(wrong_stage)
    duplicate_role = _bundle()
    duplicate_role["relations"][1]["role"] = "identity_member"
    duplicate_role["relations"][1]["stage_relation"] = (
        publication.derive_stage_relation_name(
            "mrf", "build-run-synthetic", "identity_member", "evidence_live"
        )
    )
    cases.append(duplicate_role)
    role_name_collision = _bundle()
    role_name_collision["relations"][1]["role"] = "identity_live_old"
    role_name_collision["relations"][1]["stage_relation"] = (
        publication.derive_stage_relation_name(
            "mrf", "build-run-synthetic", "identity_live_old", "evidence_live"
        )
    )
    cases.append(role_name_collision)

    for raw in cases:
        with pytest.raises(publication.StagedBundlePublicationContractError):
            publication.build_staged_bundle_publication_contract(raw)


@pytest.mark.parametrize(
    "mutation",
    (
        {"expected_predecessor_generation_id": None},
        {"expected_current_generation_id": "generation-other"},
        {"expected_previous_generation_id": "generation-synthetic-001"},
        {"generation_id": "generation-synthetic-001"},
        {"generation_id": "generation-synthetic-000"},
    ),
)
def test_rejects_stale_or_ambiguous_replacement_pointer_state(
    mutation: dict[str, object],
) -> None:
    raw = _bundle()
    raw.update(mutation)
    with pytest.raises(publication.StagedBundlePublicationContractError):
        publication.build_staged_bundle_publication_contract(raw)


def test_initial_pointer_state_must_be_absent() -> None:
    for field_name in (
        "expected_predecessor_generation_id",
        "expected_current_generation_id",
        "expected_previous_generation_id",
    ):
        raw = _bundle(replacement=False)
        raw[field_name] = "generation-unexpected"
        with pytest.raises(publication.StagedBundlePublicationContractError):
            publication.build_staged_bundle_publication_contract(raw)


def test_requires_exact_catalog_fingerprints_and_parity() -> None:
    cases = []
    missing_live = _bundle()
    missing_live["relations"][0]["live_fingerprints"] = None
    cases.append(missing_live)
    mismatch = _bundle()
    mismatch["relations"][0]["live_fingerprints"]["owner_sha256"] = _sha("d")
    cases.append(mismatch)
    malformed = _bundle()
    malformed["relations"][0]["stage_fingerprints"]["owner_sha256"] = "A" * 64
    cases.append(malformed)
    unverified = _bundle()
    unverified["relations"][0]["catalog_parity_verified"] = 1
    cases.append(unverified)
    initial_live = _bundle(replacement=False)
    initial_live["relations"][0]["live_fingerprints"] = _fingerprints("b")
    cases.append(initial_live)

    for raw in cases:
        with pytest.raises(publication.StagedBundlePublicationContractError):
            publication.build_staged_bundle_publication_contract(raw)


def test_revalidates_exact_descriptor_and_rejects_tampering() -> None:
    descriptor = _build()
    rebuilt = publication.validate_staged_bundle_publication_contract(descriptor)
    assert rebuilt == descriptor
    assert rebuilt is not descriptor

    object.__setattr__(descriptor, "generation_id", "generation-synthetic-003")
    _assert_revalidation_rejects(descriptor)

    descriptor = _build()
    object.__setattr__(descriptor, "publication_authorized", True)
    _assert_revalidation_rejects(descriptor)


def test_revalidation_rejects_foreign_deleted_and_nested_tampered_state() -> None:
    class Hostile:
        @property
        def contract(self) -> str:
            raise AssertionError("foreign property evaluated")

    _assert_revalidation_rejects(Hostile())

    descriptor = _build()
    object.__delattr__(descriptor, "generation_id")
    _assert_revalidation_rejects(descriptor)

    descriptor = _build()
    object.__setattr__(descriptor.relations[0], "observed_stage_oid", 999)
    _assert_revalidation_rejects(descriptor)

    descriptor = _build()
    object.__delattr__(descriptor.relations[0], "role")
    _assert_revalidation_rejects(descriptor)

    descriptor = _build()
    object.__setattr__(descriptor, "contract_sha256", _sha("f"))
    _assert_revalidation_rejects(descriptor)

    descriptor = _build()
    object.__setattr__(descriptor.relations[0], "stage_fingerprints", ())
    _assert_revalidation_rejects(descriptor)

    descriptor = _build()
    object.__setattr__(descriptor, "relations", (object(),))
    _assert_revalidation_rejects(descriptor)


def test_descriptor_and_nested_intents_are_redacted() -> None:
    descriptor = _build()
    hidden_values = (
        descriptor.run_id,
        descriptor.generation_id,
        descriptor.source_vector_sha256,
        descriptor.relations[0].stage_relation,
    )
    assert all(value not in repr(descriptor) for value in hidden_values)
    assert descriptor.relations[0].live_relation not in repr(descriptor.relations[0])


def test_wraps_unexpected_builder_and_validator_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publication, "_normalized_bundle", lambda _raw: 1 / 0)
    with pytest.raises(publication.StagedBundlePublicationContractError):
        publication.build_staged_bundle_publication_contract(_bundle())

    monkeypatch.undo()
    descriptor = _build()
    monkeypatch.setattr(
        publication, "_descriptor_from_normalized", lambda _normalized: 1 / 0
    )
    _assert_revalidation_rejects(descriptor)
