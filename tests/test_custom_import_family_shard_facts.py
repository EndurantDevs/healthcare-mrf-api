# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded real fact production, including global raw/typed reconciliation."""

import json
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_build as admission
from process.custom_import import family_shard_facts as producer
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_shard_input_records import prepare_record
from process.custom_import.runner_codec import digest_text, record_payload
from tests.test_custom_import_family_shard_build import _child, _compound_definition, _root
from tests.test_custom_import_identical_children_postgres import _membership_definition, _membership_records
from tests.test_custom_import_snowflake_bundle import _definition


def _records(definition, batches):
    records = []
    slots_by_collection = {collection.name: index for index, collection in enumerate(definition.child_collections, 1)}
    for slot, stream in enumerate(definition.source_streams, 1):
        fields = sorted(
            (field for field in definition.fields if field.collection == stream.child_collection),
            key=lambda field: field.field_slot,
        )
        source = 0
        for part, rows in enumerate(batches[stream.stream_id], 1):
            for row, values in enumerate(rows):
                records.append(
                    prepare_record(
                        definition,
                        stream,
                        {field.field_id: values[field.field_id] for field in fields},
                        stream_slot=slot,
                        collection_slot=slots_by_collection.get(stream.child_collection, 0),
                        part_ordinal=part,
                        row_ordinal=row,
                        source_ordinal=source,
                    )
                )
                source += 1
    return records


def _produce(tmp_path, definition, source_records, *, memory=4096, expected=None, budget=64 * 1024**2):
    root = tmp_path / "facts"
    registered_paths = []

    def register(path):
        assert not path.exists()
        registered_paths.append(path)

    observed_results = []
    limits = producer.SourceFactLimits(budget, min(1024**2, budget), memory)
    with producer.prepare_source_families(
        iter(source_records),
        definition,
        root=root,
        register_cleanup=register,
        limits=limits,
        expected_records=len(source_records) if expected is None else expected,
    ) as families:
        for family in families:
            observed_results.append(
                dict(
                    key=family.typed_key,
                    rows=list(family.rows()),
                    root=family.source_root,
                    source_facts=family.source_facts,
                    resolution=family.root_resolution,
                    errors=family.candidate_errors,
                    fatal=family.fatal_problem,
                    children={name: list(family.children(name)) for name in definition.collections_by_name},
                    spilled=family.group.file._rolled,
                )
            )
    assert registered_paths == [root.absolute()] and not root.exists()
    return observed_results


def _ordered_rows(groups):
    return sorted(
        (row for group in groups for row in group["rows"]), key=lambda row: admission.occurrence_order(row[0])
    )


def test_actual_producer_matches_global_native_golden_and_rejection_order(tmp_path):
    definition = _definition()
    family_records = _records(
        definition,
        {
            "root_source": [[_root(), _root("1234567893", None)], [_root()]],
            "detail_source": [[_child(), _child("1234567893", None)], [_child(), _child("absent-parent", None)]],
        },
    )
    groups = _produce(tmp_path, definition, list(reversed(family_records)))
    family_rows = _ordered_rows(groups)
    rejection_targets = sorted(
        (rejection_target for _record, _facts, decision in family_rows for rejection_target in decision.targets),
        key=lambda target: (target.occurrence, target.event.occurrence),
    )
    resolutions_by_key = {
        rejection_target.occurrence: rejection_target
        for rejection_target in admission.first_resolutions(rejection_targets)
    }
    assert [
        resolutions_by_key[admission.occurrence_order(family_record)].event.code
        for family_record, _facts, _decision in family_rows
    ] == [
        "duplicate_root_key",
        "required_field_null",
        "duplicate_root_key",
        "duplicate_child_key",
        "required_field_null",
        "duplicate_child_key",
        "orphan_child",
    ]
    assert sum(group["errors"] for group in groups) == 1
    initial_items = [
        event
        for family_record, _facts, _decision in family_rows
        if (event := admission.initial_rejection(family_record)) is not None
    ]
    added_items = [
        decision.event
        for _record, _facts, decision in family_rows
        if decision.event is not None and decision.event.origin == "admission"
    ]
    assert [event.origin for _ordinal, event in admission.rejection_ordinals(initial_items, added_items)] == [
        "source"
    ] * 3 + ["admission"] * 5


def test_raw_evidence_reconciles_invalid_lane_before_typed_root_count(tmp_path):
    definition = _compound_definition()
    records = _records(definition, {"root_source": [[_root(score=1), _root(score=True)]], "detail_source": [[]]})
    groups = _produce(tmp_path, definition, records)
    assert len(groups) == 1 and groups[0]["source_facts"].typed_root_count == 1
    assert [decision.code for _record, _facts, decision in _ordered_rows(groups)] == [
        "duplicate_root_key",
        "field_type_invalid",
    ]


def test_decimal_variants_keep_raw_parent_rules_within_typed_group(tmp_path):
    definition = _compound_definition("decimal")
    records = _records(
        definition, {"root_source": [[_root(score="1.0")], [_root(score="1.00")]], "detail_source": [[]]}
    )
    groups = _produce(tmp_path, definition, records)
    assert len(groups) == 1
    assert all(
        facts.raw_root_count == 1 and facts.typed_root_count == 2 and decision.code == "duplicate_root_key"
        for _record, facts, decision in groups[0]["rows"]
    )


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("missing", [False, True])
def test_membership_uses_complete_initially_valid_peer_set_and_original_order(tmp_path, reverse, missing):
    definition = _membership_definition(reverse=reverse)
    records = _records(definition, _membership_records(missing=missing))
    groups = _produce(tmp_path, definition, list(reversed(records)))
    assert all(group["fatal"] is None for group in groups)
    assert sum(group["resolution"] is not None for group in groups) == int(missing)
    if missing:
        assert any(decision.code == "child_membership_missing" for _record, _facts, decision in _ordered_rows(groups))


def test_collapse_keeps_last_original_lexical_record_and_codec_order(tmp_path):
    document = json.loads(_definition().canonical)
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    definition = CustomImportDefinition.from_mapping(document)
    records = _records(
        definition, {"root_source": [[_root()]], "detail_source": [[_child(amount="2.0")], [_child(amount="2.00")]]}
    )
    groups = _produce(tmp_path, definition, records)
    children = groups[0]["children"]["details"]
    assert len(children) == 1 and children[0].source_ordinal == 1
    assert dict(children[0].values)["amount"] == "2.00"
    assert groups[0]["resolution"] is None


def test_child_first_rejection_suppresses_first_root_event_but_not_duplicate(tmp_path):
    document = json.loads(_definition().canonical)
    document["streams"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    groups = _produce(
        tmp_path,
        definition,
        _records(definition, {"root_source": [[_root(), _root()]], "detail_source": [[_child(amount=None)]]}),
    )
    rows = _ordered_rows(groups)
    assert [decision.code for _record, _facts, decision in rows] == [
        "required_field_null",
        "duplicate_root_key",
        "duplicate_root_key",
    ]
    assert [None if decision.event is None else decision.event.origin for _record, _facts, decision in rows] == [
        "source",
        None,
        "admission",
    ]
    assert groups[0]["resolution"].event.occurrence == (1, 0)


def test_invalid_last_child_keeps_initial_event_and_last_valid_collapse_witness(tmp_path):
    document = json.loads(_definition().canonical)
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    definition = CustomImportDefinition.from_mapping(document)
    groups = _produce(
        tmp_path,
        definition,
        _records(
            definition,
            {"root_source": [[_root()]], "detail_source": [[_child(), _child(amount="2.00")], [_child(amount=None)]]},
        ),
    )
    rows = _ordered_rows(groups)
    assert [facts.last_child for record, facts, _decision in rows if record.child_key is not None] == [(2, 1)] * 2
    assert [decision.code for _record, _facts, decision in rows] == [None, None, None, "required_field_null"]
    assert groups[0]["resolution"].event.occurrence == (2, 2)
    assert not groups[0]["children"]["details"]


def test_large_valid_family_spills_without_child_limit_and_keeps_all_children(tmp_path, monkeypatch):
    definition = _definition()
    child_records = [dict(_child(), detail_id=f"K{index}") for index in range(257)]
    records = _records(
        definition, {"root_source": [[_root()]], "detail_source": [child_records[:128], child_records[128:]]}
    )
    observer = SimpleNamespace(payload_calls=0)

    def counted_payload(*args):
        observer.payload_calls += 1
        return record_payload(*args)

    with monkeypatch.context() as patch:
        patch.setattr(producer.codec, "record_payload", counted_payload)
        groups = _produce(tmp_path, definition, records, memory=2048)
    assert observer.payload_calls == len(child_records)  # Keep selected output encoding, skip self-comparison.
    assert len(groups) == 1 and groups[0]["spilled"]
    actual = groups[0]["children"]["details"]
    assert len(actual) == 257 and {record.source_ordinal for record in actual} == set(range(257))
    documents = [
        (
            digest_text("child-key", record.child_key),
            record.child_key,
            record_payload(definition.child_fields, record.normalized_values(definition, definition.source_streams[1])),
        )
        for record in actual
    ]
    assert documents == sorted(documents)


def test_sort_bounds_live_runs_and_closes_partial_reads(tmp_path):
    root = tmp_path / "sort"
    work = producer._Work(root, producer.SourceFactLimits(8 * 1024**2, 4096, 1024), lambda _path: None)
    observer = SimpleNamespace(peak=0)
    original = work.path

    def observed_path():
        result = original()
        observer.peak = max(observer.peak, len(work.paths))
        return result

    work.path = observed_path
    values = producer._sorted(work, range(4097, -1, -1), lambda value: value)
    try:
        assert next(values) == 0 and next(values) == 1
        assert observer.peak <= 36  # At most seven runs per base-eight level, plus an active merge.
    finally:
        values.close()
    assert not work.paths and work.used == 0
    work.close()
    assert not root.exists()


def test_early_family_consumer_close_removes_only_owned_spool(tmp_path):
    definition = _definition()
    records = _records(definition, {"root_source": [[_root(), _root("1234567893")]], "detail_source": [[_child()]]})
    root = tmp_path / "facts"
    sentinel = tmp_path / "unrelated.txt"
    sentinel.write_text("preserve")
    with producer.prepare_source_families(
        iter(records),
        definition,
        root=root,
        register_cleanup=lambda _path: None,
        limits=producer.SourceFactLimits(1024**2, 64 * 1024, 2048),
        expected_records=len(records),
    ) as families:
        assert next(families).source_root is not None
    assert not root.exists() and sentinel.read_text() == "preserve"


@pytest.mark.parametrize("refresh", ["snapshot", "upsert"])
def test_raw_equal_untypable_root_is_absent_from_typed_base_choice(tmp_path, refresh):
    document = json.loads(_compound_definition().canonical)
    document["refresh_mode"] = refresh
    definition = CustomImportDefinition.from_mapping(document)
    records = _records(definition, {"root_source": [[_root(score=True)]], "detail_source": [[]]})
    with producer.prepare_source_families(
        iter(records),
        definition,
        root=tmp_path / "facts",
        register_cleanup=lambda _path: None,
        limits=producer.SourceFactLimits(1024**2, 64 * 1024, 2048),
        expected_records=1,
    ) as families:
        family = next(families)
        assert family.typed_key is None and family.source_root is None and family.root_resolution is None
        assert family.selection(has_base=True, complete_scope=True) == ("retained" if refresh == "upsert" else "absent")
        assert list(families) == []


@pytest.mark.parametrize("reason", ["count", "spool", "source_failure"])
def test_incomplete_input_and_resource_failures_preserve_other_paths(tmp_path, reason):
    definition = _definition()
    records = _records(definition, {"root_source": [[_root()]], "detail_source": [[_child()]]})
    sentinel = tmp_path / "unrelated.txt"
    sentinel.write_text("preserve")

    def failed():
        yield records[0]
        raise RuntimeError("source failed")

    with pytest.raises((ValueError, OSError, RuntimeError)):
        _produce(
            tmp_path,
            definition,
            failed() if reason == "source_failure" else records,
            expected=3 if reason == "count" else 2,
            budget=4096 if reason == "spool" else 64 * 1024**2,
            memory=2048,
        )
    assert not (tmp_path / "facts").exists() and sentinel.read_text() == "preserve"


def test_optional_work_check_preserves_existing_materialization_deadline_callback(tmp_path):
    from types import SimpleNamespace

    from process.custom_import.family_block_materialization import _CheckedWork

    calls = []

    def check():
        calls.append(None)
        if len(calls) == 2:
            raise RuntimeError("synthetic materialization deadline")

    plan = SimpleNamespace(
        root=tmp_path / "materialization",
        limits=producer.SourceFactLimits(1024**2, 65536, 4096),
        register_cleanup=lambda _root: None,
    )
    work = _CheckedWork(plan, check)
    try:
        with pytest.raises(RuntimeError, match="materialization deadline"):
            work.write((("synthetic", 1),))
        assert calls == [None, None]
    finally:
        work.close()
    assert not plan.root.exists()


def _framed_facts_case(scenario):
    if scenario.startswith("membership"):
        definition = _membership_definition()
        return definition, _membership_records(missing=scenario == "membership_missing")
    if scenario == "legacy_decimal":
        definition = _compound_definition("decimal")
        assert "decimal_conversions" not in json.loads(definition.canonical)
        return definition, {
            "root_source": [[_root(score="1.0"), _root(score="1.00")]],
            "detail_source": [[dict(_child(amount="2.00"), detail_score="1.0")]],
        }
    document = json.loads(_definition().canonical)
    if scenario == "collapse_identical":
        document["streams"][1]["duplicate_policy"] = "collapse_identical"
    child_batches = [[_child()]]
    if scenario == "rejected":
        child_batches = [[_child(amount=None), _child("absent-parent")]]
    elif scenario in {"duplicate", "collapse_identical"}:
        child_batches = [[_child(amount="2.0")], [_child(amount="2.00")]]
    return CustomImportDefinition.from_mapping(document), {"root_source": [[_root()]], "detail_source": child_batches}


@pytest.mark.parametrize(
    "scenario",
    ["valid", "rejected", "duplicate", "collapse_identical", "membership", "membership_missing", "legacy_decimal"],
)
def test_initial_facts_preserve_framed_bytes_and_validation(tmp_path, monkeypatch, scenario):
    definition, batches = _framed_facts_case(scenario)
    source_records = list(reversed(_records(definition, batches)))
    frames, validations = [], []
    initial_facts = producer._initial_facts
    post_init = admission.SourceAdmissionFacts.__post_init__

    def framed_facts(*args):
        for row in initial_facts(*args):
            payload = producer._pack(row)
            frames.append(producer._LENGTH.pack(len(payload)) + payload)
            yield row

    def validate(facts):
        validations.append(None)
        post_init(facts)

    def recursive_copy(facts, **changes):
        return admission.SourceAdmissionFacts(**(producer.asdict(facts) | changes))

    monkeypatch.setattr(producer, "_initial_facts", framed_facts)
    monkeypatch.setattr(admission.SourceAdmissionFacts, "__post_init__", validate)
    actual = _produce(tmp_path, definition, source_records)
    actual_frames, actual_validations = list(frames), len(validations)
    frames.clear()
    validations.clear()
    monkeypatch.setattr(producer, "replace", recursive_copy)
    historical = _produce(tmp_path, definition, source_records)
    assert actual == historical
    assert frames == actual_frames and len(frames) == len(source_records)
    assert actual_validations == len(validations) > 0
