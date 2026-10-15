# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Semantic commitments and lightweight admission fail closed on incomplete evidence."""

import datetime as dt
from dataclasses import replace
from decimal import Decimal, localcontext
from itertools import product
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.models.custom_import_query_projection import QUERY_PROJECTION_CONTRACT
from process.custom_import import publication, query_projection_evidence
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.query_projection_binding import QueryProjectionUnavailable
from process.custom_import.query_projection_evidence import (
    ProjectionDigest,
    ProjectionDigestState,
    QueryProjectionEvidence,
    completed_projection_evidence,
    read_projection_tables,
    read_seal_projection_evidence,
)
from process.custom_import.query_projection_sources import capability_sha256
from process.custom_import.query_projection_storage import query_projection_index_count, query_projection_tables
from process.custom_import.query_projection_values import configured_fields
from tests.test_custom_import_derived_query import derived_document

_NOW = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
_MIN_BIGINT, _MAX_BIGINT = -(2**63), 2**63 - 1


@pytest.fixture
def definition():
    return CustomImportDefinition.from_mapping(derived_document())


def _evidence(definition, **changes):
    return replace(_fixture_digest(definition).finish(), **changes)


def _fixture_digest(definition):
    digest = ProjectionDigest(definition)
    for is_child, count in ((False, 2), (True, 3)):
        values_by_field = {
            field.field_id: None
            for field in configured_fields(definition)
            if (field.collection is not None) == is_child
        }
        if not values_by_field:
            continue
        for index in range(count):
            if is_child:
                digest.add_child("entity_a", 0, f'["service_{index}"]', values_by_field)
            else:
                digest.add_root(f"entity_{index}", 0, values_by_field)
    return digest


def _generation(**changes):
    return SimpleNamespace(
        **dict(
            generation_id=17,
            dataset_id=11,
            definition_revision_id=7,
            schema_revision_id=5,
            execution_id=3,
            capture_bundle_id=19,
            producing_fence=2,
            producing_token_sha256=b"t" * 32,
        )
        | changes
    )


def _manifest(definition, evidence, generation=None, **changes):
    values = vars(generation or _generation()).copy()
    values.pop("capture_bundle_id")
    state = _fixture_digest(definition).checkpoint()
    values.update(
        family_id=23,
        phase="complete",
        complete_at=_NOW,
        index_count=query_projection_index_count(definition),
        definition_sha256=bytes.fromhex(definition.digest),
        projection_contract=evidence.contract,
        capability_sha256=evidence.capability_sha256,
        projection_sha256=evidence.projection_sha256,
        root_count=evidence.root_count,
        child_count=evidence.child_count,
        verified_root_count=state.root_count,
        verified_child_count=state.child_count,
        digested_root_count=state.root_count,
        digested_child_count=state.child_count,
        digest_chain_sha256=state.chain_sha256,
        digest_after_entity=state.canonical_entity,
        digest_after_selected_value=state.selected_value,
        digest_after_child_key=state.canonical_child_key,
    )
    return SimpleNamespace(**(values | changes))


def _seal(evidence, generation=None, **changes):
    values = vars(generation or _generation()).copy()
    values["seal_contract"] = "custom-import-generation-seal/v1"
    values["sealing_fence"] = values.pop("producing_fence")
    values["sealing_token_sha256"] = values.pop("producing_token_sha256")
    return SimpleNamespace(**(values | evidence.seal_values() | changes))


def _root(digest, entity="entity_a", selected=0, value=None):
    digest.add_root(entity, selected, {"combined_score": value})


def _child(digest, entity="entity_a", selected=0, key='["service_a"]', **values):
    digest.add_child(entity, selected, key, {"combined_quality": None, "merged_cost": None} | values)


@pytest.mark.parametrize("present", tuple(product((False, True), repeat=5)))
def test_seal_markers_are_all_or_none(definition, present):
    evidence = _evidence(definition)
    marker_by_name = {
        name: value if keep else None
        for (name, value), keep in zip(evidence.seal_values().items(), present, strict=True)
    }
    seal = SimpleNamespace(**marker_by_name)
    if all(present):
        assert read_seal_projection_evidence(seal) == evidence
    elif not any(present):
        assert read_seal_projection_evidence(seal) is None
    else:
        with pytest.raises(QueryProjectionUnavailable):
            read_seal_projection_evidence(seal)


def test_missing_seal_markers_are_not_legacy(definition):
    values = dict.fromkeys(_evidence(definition).seal_values())
    del values["query_projection_contract"]
    for seal in (None, SimpleNamespace(), SimpleNamespace(**values)):
        with pytest.raises(QueryProjectionUnavailable, match="missing"):
            read_seal_projection_evidence(seal)


@pytest.mark.parametrize(
    "name,value",
    [
        ("contract", None),
        ("contract", "unknown"),
        ("capability_sha256", b"short"),
        ("projection_sha256", "p" * 32),
        ("root_count", True),
        ("root_count", -1),
        ("root_count", 2**63),
        ("child_count", 1.0),
        ("child_count", None),
    ],
)
def test_evidence_requires_exact_native_values(definition, name, value):
    with pytest.raises(QueryProjectionUnavailable):
        _evidence(definition, **{name: value})


def test_hash_bytes_are_copied_and_counts_are_bigints(definition):
    digest = bytearray(b"x" * 32)
    evidence = _evidence(definition, projection_sha256=digest, root_count=_MAX_BIGINT, child_count=0)
    digest[0] = 0
    assert evidence.projection_sha256 == b"x" * 32
    assert read_seal_projection_evidence(_seal(evidence)) == evidence


def test_completion_admits_exact_manifest_and_seal(definition):
    evidence = _evidence(definition)
    assert (
        completed_projection_evidence(
            _manifest(definition, evidence), generation=_generation(), definition=definition, seal=_seal(evidence)
        )
        == evidence
    )
    assert (
        completed_projection_evidence(_manifest(definition, evidence), generation=_generation(), definition=definition)
        == evidence
    )


@pytest.mark.parametrize(
    "name",
    ["generation_id", "dataset_id", "definition_revision_id", "schema_revision_id", "execution_id", "producing_fence"],
)
@pytest.mark.parametrize("value", [True, 0, 99])
def test_manifest_requires_exact_producer_pins(definition, name, value):
    evidence = _evidence(definition)
    with pytest.raises(QueryProjectionUnavailable):
        completed_projection_evidence(
            _manifest(definition, evidence, **{name: value}), generation=_generation(), definition=definition
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"phase": "indexes"},
        {"complete_at": None},
        {"complete_at": "complete"},
        {"complete_at": _NOW.replace(tzinfo=None)},
        {"index_count": 2},
        {"index_count": 3},
        {"index_count": True},
        {"definition_sha256": b"d" * 32},
        {"capability_sha256": b"c" * 32},
        {"producing_token_sha256": b"x" * 32},
        {"projection_sha256": None},
    ],
)
def test_completion_rejects_incomplete_or_foreign_metadata(definition, changes):
    with pytest.raises(QueryProjectionUnavailable):
        completed_projection_evidence(
            _manifest(definition, _evidence(definition), **changes), generation=_generation(), definition=definition
        )


@pytest.mark.parametrize(
    "name", ["verified_root_count", "verified_child_count", "digested_root_count", "digested_child_count"]
)
@pytest.mark.parametrize("value", [True, -1, 0, 4])
def test_completion_requires_verified_and_digested_coverage(definition, name, value):
    evidence = _evidence(definition)
    with pytest.raises(QueryProjectionUnavailable, match="coverage"):
        completed_projection_evidence(
            _manifest(definition, evidence, **{name: value}), generation=_generation(), definition=definition
        )


@pytest.mark.parametrize("chain", [None, b"short", b"x" * 32])
def test_completion_reconstructs_retained_chain(definition, chain):
    evidence = _evidence(definition)
    with pytest.raises(QueryProjectionUnavailable, match="chain"):
        completed_projection_evidence(
            _manifest(definition, evidence, digest_chain_sha256=chain), generation=_generation(), definition=definition
        )


def test_completion_requires_retained_digest_cursor(definition):
    evidence = _evidence(definition)
    with pytest.raises(QueryProjectionUnavailable, match="cursor"):
        completed_projection_evidence(
            _manifest(definition, evidence, digest_after_child_key=None),
            generation=_generation(),
            definition=definition,
        )


@pytest.mark.parametrize(
    "name",
    [
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "sealing_fence",
        "sealing_token_sha256",
    ],
)
def test_seal_requires_exact_producer_pins(definition, name):
    evidence = _evidence(definition)
    value = b"x" * 32 if name.endswith("sha256") else 99
    with pytest.raises(QueryProjectionUnavailable, match="producer"):
        completed_projection_evidence(
            _manifest(definition, evidence),
            generation=_generation(),
            definition=definition,
            seal=_seal(evidence, **{name: value}),
        )


@pytest.mark.parametrize(
    "name,value",
    [("projection_sha256", b"x" * 32), ("capability_sha256", b"c" * 32), ("root_count", 3), ("child_count", 4)],
)
def test_seal_cannot_advertise_another_commitment(definition, name, value):
    evidence = _evidence(definition)
    with pytest.raises(QueryProjectionUnavailable, match="manifest and seal"):
        completed_projection_evidence(
            _manifest(definition, evidence),
            generation=_generation(),
            definition=definition,
            seal=_seal(replace(evidence, **{name: value})),
        )


def test_legacy_seal_cannot_admit_completed_manifest(definition):
    evidence = _evidence(definition)
    with pytest.raises(QueryProjectionUnavailable, match="manifest and seal"):
        completed_projection_evidence(
            _manifest(definition, evidence),
            generation=_generation(),
            definition=definition,
            seal=_seal(evidence, **dict.fromkeys(evidence.seal_values())),
        )


def test_missing_completion_and_generation_fail_closed(definition):
    evidence = _evidence(definition)
    for manifest, generation in ((None, _generation()), (_manifest(definition, evidence), None)):
        with pytest.raises(QueryProjectionUnavailable, match="missing"):
            completed_projection_evidence(manifest, generation=generation, definition=definition)


def test_semantic_commitment_ignores_producer_allocations(definition):
    evidence = _evidence(definition)
    generation = _generation(
        generation_id=101,
        dataset_id=102,
        definition_revision_id=103,
        schema_revision_id=104,
        execution_id=105,
        capture_bundle_id=106,
        producing_fence=107,
        producing_token_sha256=b"n" * 32,
    )
    assert (
        completed_projection_evidence(
            _manifest(definition, evidence, generation),
            generation=generation,
            definition=definition,
            seal=_seal(evidence, generation),
        )
        == evidence
    )
    assert set(evidence.seal_values()) == {
        "query_projection_contract",
        "query_projection_capability_sha256",
        "query_projection_sha256",
        "query_projection_root_count",
        "query_projection_child_count",
    }


def test_digest_matches_existing_canonical_frames(definition):
    digest = ProjectionDigest(definition)
    _root(digest, selected=_MIN_BIGINT, value=Decimal("12.50"))
    _child(digest, selected=_MAX_BIGINT, merged_cost=Decimal("-0.00"))
    actual = digest.finish()
    expected = publication._new_digest("query-projection-content/v1")
    publication._add_digest_record(
        expected,
        "capability",
        {
            "contract": QUERY_PROJECTION_CONTRACT,
            "definition_sha256": definition.digest,
            "capability_sha256": capability_sha256(definition).hex(),
        },
    )
    expected = _next_chain("query-projection-step/v1", expected.digest())
    publication._add_digest_record(
        expected,
        "root",
        {"entity": "entity_a", "selected_value": _MIN_BIGINT, "values": {"combined_score": "12.500000000000"}},
    )
    expected = _next_chain("query-projection-step/v1", expected.digest())
    publication._add_digest_record(
        expected,
        "child",
        {
            "entity": "entity_a",
            "selected_value": _MAX_BIGINT,
            "canonical_child_key": '["service_a"]',
            "values": {"combined_quality": None, "merged_cost": "0.000000000000"},
        },
    )
    expected = _next_chain("query-projection-finish/v1", expected.digest())
    publication._add_digest_record(expected, "counts", {"root_count": 1, "child_count": 1})
    assert actual.projection_sha256 == expected.digest()
    assert (actual.root_count, actual.child_count) == (1, 1)
    assert digest.finish() is actual
    with pytest.raises(QueryProjectionUnavailable, match="already complete"):
        _root(digest, entity="entity_b")


def _next_chain(domain, previous):
    digest = publication._new_digest(domain)
    digest.update(previous)
    return digest


@pytest.mark.parametrize("page_size", [1, 2, 3, 5, 256])
def test_checkpoints_preserve_content_across_page_boundaries(definition, page_size):
    one_shot, resumed = ProjectionDigest(definition), ProjectionDigest(definition)
    for index in range(11):
        for digest in (one_shot, resumed):
            if index < 4:
                _root(digest, entity=f"entity_{index}", value=Decimal(f"{index}.125"))
            else:
                _child(digest, entity="entity_a", selected=_MIN_BIGINT, key=f'["service_{index:02}"]')
        if (index + 1) % page_size == 0:
            resumed = ProjectionDigest(definition, state=resumed.checkpoint())
    assert resumed.checkpoint() == one_shot.checkpoint()
    assert resumed.finish() == one_shot.finish()
    assert resumed.checkpoint().chain_sha256 != resumed.finish().projection_sha256


def test_empty_checkpoint_and_finished_checkpoint_are_replayable(definition):
    digest = ProjectionDigest(definition)
    empty = digest.checkpoint()
    assert (empty.root_count, empty.child_count, empty.canonical_entity, empty.selected_value) == (0, 0, None, None)
    assert empty.canonical_child_key is None
    assert ProjectionDigest(definition, state=empty).finish() == digest.finish()
    assert digest.checkpoint() == empty


def test_checkpoint_retains_exact_last_key_and_copies_bytes(definition):
    digest = ProjectionDigest(definition)
    _child(digest, entity="ζ", selected=_MAX_BIGINT, key='["é"]')
    checkpoint = digest.checkpoint()
    chain = bytearray(checkpoint.chain_sha256)
    copied = replace(checkpoint, chain_sha256=chain)
    chain[0] ^= 0xFF
    assert copied == checkpoint
    assert (checkpoint.canonical_entity, checkpoint.selected_value, checkpoint.canonical_child_key) == (
        "ζ",
        _MAX_BIGINT,
        '["é"]',
    )
    assert ProjectionDigest(definition, state=copied).finish() == digest.finish()


@pytest.mark.parametrize(
    "changes",
    [
        {"capability_sha256": b"short"},
        {"chain_sha256": None},
        {"root_count": True},
        {"root_count": -1},
        {"child_count": 2**63},
        {"canonical_entity": None},
        {"canonical_entity": True},
        {"canonical_entity": ""},
        {"canonical_entity": "\ud800"},
        {"canonical_entity": "é" * 257},
        {"selected_value": None},
        {"selected_value": True},
        {"selected_value": _MAX_BIGINT + 1},
        {"canonical_child_key": '["service_a"]'},
        {"child_count": 1},
    ],
)
def test_checkpoint_requires_exact_types_and_consistent_cursor(definition, changes):
    digest = ProjectionDigest(definition)
    _root(digest)
    with pytest.raises(QueryProjectionUnavailable):
        replace(digest.checkpoint(), **changes)


def test_resume_rejects_empty_chain_and_capability_changes(definition):
    empty = ProjectionDigest(definition).checkpoint()
    with pytest.raises(QueryProjectionUnavailable, match="empty checkpoint chain"):
        ProjectionDigest(definition, state=replace(empty, chain_sha256=b"x" * 32))
    for state in ({}, replace(empty, capability_sha256=b"x" * 32)):
        with pytest.raises(QueryProjectionUnavailable, match="capability"):
            ProjectionDigest(definition, state=state)
    document = derived_document()
    document["query"]["derived_fields"][0]["output_path"] = ["different"]
    with pytest.raises(QueryProjectionUnavailable, match="capability"):
        ProjectionDigest(CustomImportDefinition.from_mapping(document), state=empty)


def test_resume_rejects_out_of_order_records_without_changing_checkpoint(definition):
    digest = ProjectionDigest(definition)
    _root(digest, entity="entity_b")
    resumed = ProjectionDigest(definition, state=digest.checkpoint())
    for entity in ("entity_a", "entity_b"):
        with pytest.raises(QueryProjectionUnavailable, match="ordered"):
            _root(resumed, entity=entity)
    assert resumed.checkpoint() == digest.checkpoint()
    _child(resumed)
    child_checkpoint = resumed.checkpoint()
    resumed = ProjectionDigest(definition, state=child_checkpoint)
    with pytest.raises(QueryProjectionUnavailable, match="ordered"):
        _root(resumed, entity="entity_c")
    with pytest.raises(QueryProjectionUnavailable, match="ordered"):
        _child(resumed)
    assert resumed.checkpoint() == child_checkpoint


def test_resume_rejects_count_overflow_before_hashing(definition):
    digest = ProjectionDigest(definition)
    _root(digest)
    checkpoint = replace(digest.checkpoint(), root_count=_MAX_BIGINT)
    resumed = ProjectionDigest(definition, state=checkpoint)
    with pytest.raises(QueryProjectionUnavailable, match="context count"):
        _root(resumed, entity="entity_b")
    assert resumed.checkpoint() == checkpoint


def test_resume_rejects_rows_in_an_unconfigured_scope():
    document = derived_document()
    document["query"].update(filterable_fields=[], sortable_fields=[], order=[])
    definition = CustomImportDefinition.from_mapping(document)
    checkpoint = ProjectionDigestState(capability_sha256(definition), b"x" * 32, 1, 0, "entity_a", 0)
    with pytest.raises(QueryProjectionUnavailable, match="unconfigured scope"):
        ProjectionDigest(definition, state=checkpoint)


def test_decimal_encoding_is_exact_and_context_independent(definition):
    def calculate(values):
        digest = ProjectionDigest(definition)
        for selected, value in enumerate(values):
            _root(digest, selected=selected, value=value)
        return digest.finish()

    values = [
        Decimal("-9223372036854775808.000000000000"),
        Decimal("9223372036854775807.000000000000"),
        Decimal("9999999999999999999.999999999999"),
        Decimal("0.000000000001"),
        Decimal("1.2500000000000"),
        Decimal("-0.000"),
    ]
    expected = calculate(values)
    with localcontext() as context:
        context.prec = 2
        assert calculate(values) == expected
    assert calculate([Decimal("1.25"), Decimal("0")]) == calculate([Decimal("1.25000000000000"), Decimal("-0E+100")])


@pytest.mark.parametrize(
    "value",
    [
        True,
        1,
        1.0,
        "1",
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("10000000000000000000"),
        Decimal("0.0000000000001"),
    ],
)
def test_decimal_values_are_never_coerced_or_rounded(definition, value):
    digest = ProjectionDigest(definition)
    with pytest.raises(QueryProjectionUnavailable, match="decimal"):
        _root(digest, value=value)
    assert digest.finish().root_count == 0


def test_integer_values_keep_full_signed_domain():
    document = derived_document()
    document["query"]["derived_fields"][0]["expression"] = {
        "type": "preferred_non_null",
        "scope": "root",
        "field_id": "weight",
        "group_values": ["segment_a", "segment_b"],
    }
    definition = CustomImportDefinition.from_mapping(document)
    digest = ProjectionDigest(definition)
    for selected, value in enumerate((_MIN_BIGINT, 0, _MAX_BIGINT)):
        _root(digest, selected=selected, value=value)
    assert digest.finish().root_count == 3
    for value in (True, 1.0, Decimal(1), "1", _MIN_BIGINT - 1, _MAX_BIGINT + 1):
        with pytest.raises(QueryProjectionUnavailable, match="integer"):
            _root(ProjectionDigest(definition), value=value)


@pytest.mark.parametrize("selected", [True, 1.0, "1", None, _MIN_BIGINT - 1, _MAX_BIGINT + 1])
def test_selection_requires_a_signed_bigint(definition, selected):
    with pytest.raises(QueryProjectionUnavailable, match="selection"):
        _root(ProjectionDigest(definition), selected=selected)


@pytest.mark.parametrize("values", [{}, {"combined_score": None, "extra": None}, {"combined_alias": None}, [None]])
def test_field_mapping_must_be_complete_and_exact(definition, values):
    with pytest.raises(QueryProjectionUnavailable, match="configured fields"):
        ProjectionDigest(definition).add_root("entity_a", 0, values)


def test_all_null_context_is_distinct_from_missing(definition):
    missing = ProjectionDigest(definition).finish()
    digest = ProjectionDigest(definition)
    _root(digest)
    assert digest.finish().root_count == 1
    assert digest.finish().projection_sha256 != missing.projection_sha256


@pytest.mark.parametrize("first,second", [(("b", 0), ("a", 0)), (("a", 1), ("a", 0)), (("a", 0), ("a", 0))])
def test_root_contexts_must_be_strictly_ordered(definition, first, second):
    digest = ProjectionDigest(definition)
    _root(digest, *first)
    with pytest.raises(QueryProjectionUnavailable, match="ordered"):
        _root(digest, *second)
    assert digest.finish().root_count == 1


def test_child_order_is_canonical_and_roots_come_first(definition):
    digest = ProjectionDigest(definition)
    _root(digest, entity="ζ")
    _child(digest, entity="a", key='["a"]')
    _child(digest, entity="a", key='["ζ"]')
    for entity, key in (("a", '["a"]'), ("a", '["ζ"]')):
        with pytest.raises(QueryProjectionUnavailable, match="ordered"):
            _child(digest, entity=entity, key=key)
    with pytest.raises(QueryProjectionUnavailable, match="ordered"):
        _root(digest, entity="Ω")
    assert (digest.finish().root_count, digest.finish().child_count) == (1, 2)


@pytest.mark.parametrize("value", [None, True, "", "\x00", "\ud800", "a" * 513, "é" * 257])
def test_entity_identity_is_bounded_utf8(definition, value):
    with pytest.raises(QueryProjectionUnavailable, match="canonical entity"):
        _root(ProjectionDigest(definition), entity=value)


@pytest.mark.parametrize("value", [None, True, "", "\x00", "\ud800", "a" * 8193, "é" * 4097])
def test_child_identity_is_bounded_utf8(definition, value):
    with pytest.raises(QueryProjectionUnavailable, match="canonical child key"):
        _child(ProjectionDigest(definition), key=value)


def test_disabled_scopes_have_no_rows_or_counts():
    document = derived_document()
    document["query"].update(filterable_fields=[], sortable_fields=[], order=[])
    definition = CustomImportDefinition.from_mapping(document)
    digest = ProjectionDigest(definition)
    with pytest.raises(QueryProjectionUnavailable, match="no configured fields"):
        digest.add_root("entity_a", 0, {})
    evidence = digest.finish()
    assert (evidence.root_count, evidence.child_count) == (0, 0)
    manifest = _manifest(definition, evidence, index_count=0)
    assert completed_projection_evidence(manifest, generation=_generation(), definition=definition) == evidence
    for name in ("root_count", "child_count"):
        with pytest.raises(QueryProjectionUnavailable, match="unconfigured scope"):
            completed_projection_evidence(
                _manifest(definition, evidence, index_count=0, **{name: 1}),
                generation=_generation(),
                definition=definition,
            )


def test_definition_change_changes_empty_commitment(definition):
    document = derived_document()
    document["query"]["derived_fields"][0]["output_path"] = ["different"]
    changed = CustomImportDefinition.from_mapping(document)
    first, second = ProjectionDigest(definition).finish(), ProjectionDigest(changed).finish()
    assert first.capability_sha256 != second.capability_sha256
    assert first.projection_sha256 != second.projection_sha256


def test_digest_requires_a_parsed_definition():
    with pytest.raises(QueryProjectionUnavailable, match="parsed definition"):
        ProjectionDigest({})


@pytest.fixture
def read_case(definition, monkeypatch):
    evidence = _evidence(definition)
    generation, seal, manifest = _generation(), _seal(evidence), _manifest(definition, evidence)
    result = Mock()
    result.one_or_none.return_value = (generation, seal, manifest)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(return_value=result))
    target = SimpleNamespace(
        **{
            name: getattr(generation, name)
            for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id")
        }
    )
    tables = query_projection_tables(manifest.family_id, definition)
    lock = AsyncMock(return_value=tables)
    monkeypatch.setattr(query_projection_evidence, "lock_projection_tables", lock)
    return SimpleNamespace(
        session=session,
        target=target,
        generation=generation,
        seal=seal,
        manifest=manifest,
        result=result,
        tables=tables,
        lock=lock,
    )


@pytest.mark.asyncio
async def test_reader_pins_exact_metadata_and_custody(definition, read_case):
    case = read_case
    assert await read_projection_tables(case.session, case.target, definition, snapshot_family_id=23) is case.tables
    case.lock.assert_awaited_once_with(case.session, case.manifest, definition)
    case.session.execute.assert_awaited_once()
    statement = case.session.execute.call_args.args[0]
    assert statement.get_execution_options()["populate_existing"] is True
    assert statement._for_update_arg is None
    assert set(statement.compile().params.values()) == {17, 11, 7, 5}
    sql = str(statement)
    assert "custom_import_generation_seal" in sql and "custom_import_query_projection" in sql
    assert "count(" not in sql and "custom_import_derived_root" not in sql


@pytest.mark.asyncio
async def test_legacy_reader_needs_no_projection_storage(definition, read_case):
    case = read_case
    for name in _evidence(definition).seal_values():
        setattr(case.seal, name, None)
    case.result.one_or_none.return_value = (case.generation, case.seal, None)
    assert await read_projection_tables(case.session, case.target, definition, snapshot_family_id=None) is None
    case.lock.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_requires_an_existing_transaction(definition, read_case):
    case = read_case
    case.session.in_transaction = lambda: False
    with pytest.raises(QueryProjectionUnavailable, match="transaction"):
        await read_projection_tables(case.session, case.target, definition, snapshot_family_id=23)
    case.session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", [None, True, 0, 24])
async def test_reader_requires_exact_snapshot_family(definition, read_case, family_id):
    case = read_case
    with pytest.raises(QueryProjectionUnavailable, match="snapshot family"):
        await read_projection_tables(case.session, case.target, definition, snapshot_family_id=family_id)
    case.lock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["generation", "manifest"])
async def test_reader_rejects_missing_advertised_evidence(definition, read_case, missing):
    case = read_case
    case.result.one_or_none.return_value = None if missing == "generation" else (case.generation, case.seal, None)
    with pytest.raises(QueryProjectionUnavailable, match="missing"):
        await read_projection_tables(case.session, case.target, definition, snapshot_family_id=23)
    case.lock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "object_name,field,value",
    [
        ("generation", "dataset_id", 99),
        ("seal", "seal_contract", "unknown"),
        ("seal", "query_projection_contract", None),
        ("seal", "execution_id", 99),
        ("manifest", "child_count", 9),
        ("manifest", "phase", "indexes"),
    ],
)
async def test_reader_rejects_changed_evidence(definition, read_case, object_name, field, value):
    case = read_case
    setattr(getattr(case, object_name), field, value)
    with pytest.raises(QueryProjectionUnavailable):
        await read_projection_tables(case.session, case.target, definition, snapshot_family_id=23)
    case.lock.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_preserves_custody_failures(definition, read_case):
    case = read_case
    case.lock.side_effect = QueryProjectionUnavailable("projection retained table identity differs")
    with pytest.raises(QueryProjectionUnavailable, match="retained table identity"):
        await read_projection_tables(case.session, case.target, definition, snapshot_family_id=23)


@pytest.mark.asyncio
async def test_legacy_reader_still_checks_producer(definition, read_case):
    case = read_case
    for name in _evidence(definition).seal_values():
        setattr(case.seal, name, None)
    case.seal.sealing_fence = 99
    with pytest.raises(QueryProjectionUnavailable, match="producer"):
        await read_projection_tables(case.session, case.target, definition, snapshot_family_id=None)
    case.lock.assert_not_awaited()
