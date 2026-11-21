# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordered acquisition recovery must close its seam before moving or finishing."""

import hashlib
import json
from copy import deepcopy

import pytest

from process import provider_directory_timestamp_recovery as recovery

BASE = "https://directory.example/fhir"
COLLECTION = BASE + "/Organization"
ORIGINAL = COLLECTION + "?_page_token=prior%2B%2F"
NEXT = COLLECTION + "?_page_token=page%2B"
LATER = COLLECTION + "?_page_token=later%2F"


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


IDENTITY = dict(
    canonical_api_base=BASE,
    source_id="source",
    dataset_id="dataset",
    acquisition_root_run_id="root",
    resource_type="Organization",
    start_url_hash=digest(COLLECTION + "?_count=20"),
    source_scope_hash=digest("scope"),
)


def stamp(second):
    return f"2026-01-01T00:00:{second:02d}.123456Z"


def start(**overrides):
    arguments_by_name = dict(
        identity=IDENTITY,
        original_url=ORIGINAL,
        original_pages=4,
        original_rows=80,
        upper=stamp(9),
        overlap_timestamp=stamp(7),
        overlap_hashes={"a": digest("a"), "b": digest("b")},
    )
    return recovery.begin({"other": {"preserve": True}}, **(arguments_by_name | overrides))


def page(rows, next_url):
    return dict(
        resourceType="Bundle",
        type="searchset",
        entry=[
            dict(resource=dict(resourceType="Organization", id=rid, meta={"lastUpdated": stamp(t)})) for rid, t in rows
        ],
        link=[] if next_url is None else [{"relation": "next", "url": next_url}],
    )


def step(proof, rows, next_url=None, **overrides):
    arguments_by_name = dict(
        identity=IDENTITY,
        requested_url=proof[recovery.KEY]["url"],
        payload=page(rows, next_url),
        trusted_next=next_url,
        content_hashes={rid: digest(rid) for rid, _ in rows},
    )
    before = deepcopy(proof)
    try:
        return recovery.advance(proof, **(arguments_by_name | overrides))
    finally:
        assert proof == before


def restore(proof, **overrides):
    proof = json.loads(json.dumps(proof))
    state = proof[recovery.KEY]
    arguments_by_name = dict(
        identity=IDENTITY,
        next_url=state["url"],
        pages_processed=4 + state["pages"],
        rows_processed=80 + state["observations"],
        complete=state["action"] == "terminal",
    )
    return recovery.validate_checkpoint(proof, **(arguments_by_name | overrides))


def test_rollover_and_reordered_split_seam_survive_restart(monkeypatch):
    monkeypatch.setattr(recovery, "SEGMENT_PAGES", 2)
    first = start()
    assert restore(first)["action"] == "fresh_lt"
    saved = step(first, [("newer", 8), ("b", 7)], NEXT)
    assert restore(saved)["remaining_overlap"] == {"a": digest("a")}
    rolled = step(saved, [("a", 7), ("c", 5), ("d", 5)], LATER)
    assert (restore(rolled)["upper"], restore(rolled)["overlap_timestamp"]) == (stamp(7), stamp(5))
    assert restore(rolled)["remaining_overlap"] == {"c": digest("c"), "d": digest("d")}
    saved = step(rolled, [("intervening", 6), ("d", 5)], NEXT)
    finished = step(saved, [("c", 5), ("new", 4)])
    state = restore(finished)
    assert finished["other"] == first["other"]
    assert state["original"] == {
        "url": ORIGINAL,
        "pages": 4,
        "rows": 80,
        "upper": stamp(9),
        "overlap_timestamp": stamp(7),
    }
    assert (state["action"], state["url"], state["pages"], state["observations"]) == ("terminal", None, 4, 9)
    assert state["terminal_request_sha256"] == digest(NEXT)
    with pytest.raises(ValueError, match="request"):
        step(finished, [])


def test_empty_terminal_after_verified_seam():
    saved = step(start(), [("b", 7), ("a", 7), ("older", 6)], NEXT)
    assert restore(step(saved, []))["action"] == "terminal"


@pytest.mark.parametrize(
    "rows,next_url,change",
    [
        ([], None, {}),
        ([], NEXT, {}),
        ([("a", 7), ("older", 6)], NEXT, {}),
        ([("a", 8), ("b", 7)], NEXT, {}),
        ([("a", 7), ("b", 7)], NEXT, {"content_hashes": {"a": digest("changed"), "b": digest("b")}}),
        ([("future", 9)], NEXT, {}),
        ([("a", 7), ("up", 8)], NEXT, {}),
        ([("a", 7), ("a", 7)], NEXT, {}),
        ([("a", 7)], NEXT, {"content_hashes": {"other": digest("other")}}),
        ([("a", 7)], NEXT, {"requested_url": ORIGINAL}),
        ([("a", 7)], NEXT, {"trusted_next": LATER}),
        ([("a", 7)], "https://other.example/fhir/Organization?_page_token=x", {}),
    ],
)
def test_reject_malformed_or_unproven_page(rows, next_url, change):
    with pytest.raises(ValueError):
        step(start(), rows, next_url, **change)


@pytest.mark.parametrize(
    "raw_link,basis,trusted_next",
    [
        ("?_page_token=page%2B", COLLECTION, NEXT),
        ("Organization?_page_token=page%2B", ORIGINAL, NEXT),
        ("/fhir/Organization?_page_token=page%2B", ORIGINAL, NEXT),
        ("?_page_token=alias%2F", BASE + "?_page_token=redirect", BASE + "?_page_token=alias%2F"),
    ],
)
def test_relative_next_links_use_the_readers_resolution_basis(raw_link, basis, trusted_next):
    rows = [("a", 7)]
    advanced = step(start(), rows, trusted_next, payload=page(rows, raw_link), link_base_url=basis)
    assert restore(advanced)["url"] == trusted_next


@pytest.mark.parametrize(
    "raw_link,trusted_next",
    [
        (
            "//other.example/fhir/Organization?_page_token=page%2B",
            "https://other.example/fhir/Organization?_page_token=page%2B",
        ),
        ("Location?_page_token=page%2B", BASE + "/Location?_page_token=page%2B"),
        ("?_page_token=page%2B", LATER),
        ("?_page_token=pa\nge%2B", NEXT),
    ],
)
def test_relative_next_link_must_resolve_to_the_exact_allowed_resource(raw_link, trusted_next):
    rows = [("a", 7)]
    with pytest.raises(ValueError, match="timestamp_recovery_next"):
        step(start(), rows, trusted_next, payload=page(rows, raw_link))


def test_crosspage_ties_and_orders(monkeypatch):
    monkeypatch.setattr(recovery, "SEGMENT_PAGES", 2)
    saved = step(start(), [("a", 7)], NEXT)
    for rows in [[("up", 8)], [("a", 7)], [("b", 7)]]:
        with pytest.raises(ValueError):
            step(saved, rows, LATER)
    monkeypatch.setattr(recovery, "MAX_TIES", 2)
    with pytest.raises(ValueError, match="tie_limit"):
        step(saved, [("b", 7), ("c", 7)], LATER)


@pytest.mark.parametrize(
    "bundle_patch,entry_patch,resource_patch",
    [
        ({"type": "collection"}, {}, {}),
        ({"resourceType": "OperationOutcome"}, {}, {}),
        ({"link": []}, {}, {}),
        ({}, {"search": {"mode": "include"}}, {}),
        ({}, {}, {"resourceType": "Location"}),
        ({}, {"resource": {"resourceType": "Organization", "id": "a"}}, {}),
        ({}, {}, {"meta": {"lastUpdated": stamp(7).replace("456Z", "4560Z")}}),
        ({"link": [None]}, {}, {}),
        ({"link": [{"relation": "next", "url": NEXT}] * 2}, {}, {}),
    ],
)
def test_raw_response_must_be_a_valid_searchset(bundle_patch, entry_patch, resource_patch):
    rows = [("a", 7), ("b", 7)]
    body = page(rows, NEXT)
    body.update(bundle_patch)
    first_entry = body["entry"][0]
    first_entry.update(entry_patch)
    first_entry["resource"].update(resource_patch)
    with pytest.raises(ValueError):
        step(start(), rows, NEXT, payload=body)


@pytest.mark.parametrize(
    "changed",
    [
        {"lowest": stamp(9)},
        {"closed": stamp(7)},
        {"lowest_hashes": {}},
        {"observations": 90},
        {"action": "unknown"},
        {"segment_pages": 0},
        {"upper": stamp(7)},
        {"extra": None},
        {"remaining_overlap": {"b": "bad"}},
        {"url": "https://other.example/fhir/Organization?_page_token=x"},
        {"original": {"url": ORIGINAL, "rows": 0, "pages": 4}},
    ],
)
def test_corrupt_saved_state_is_rejected(changed):
    saved = step(start(), [("a", 7)], NEXT)
    saved[recovery.KEY].update(changed)
    with pytest.raises(ValueError):
        restore(saved)


@pytest.mark.parametrize(
    "change",
    [
        {"identity": IDENTITY | {"dataset_id": "another"}},
        {"identity": IDENTITY | {"source_scope_hash": digest("another")}},
        {"next_url": ORIGINAL},
        {"rows_processed": True},
        {"pages_processed": 4},
        {"complete": True},
    ],
)
def test_restore_binds_checkpoint(change):
    with pytest.raises(ValueError):
        restore(step(start(), [("a", 7)], NEXT), **change)


def test_unknown_version_rejected_even_on_completed_checkpoint():
    finished = step(start(), [("b", 7), ("a", 7)])
    finished["descending_timestamp_recovery_v2"] = {}
    with pytest.raises(ValueError, match="version"):
        restore(finished)
    del finished["descending_timestamp_recovery_v2"]
    finished[recovery.KEY]["terminal_request_sha256"] = None
    with pytest.raises(ValueError, match="terminal"):
        restore(finished)


@pytest.mark.parametrize(
    "change",
    [
        {"original_url": COLLECTION},
        {"upper": stamp(7)},
        {"original_pages": True},
        {"overlap_hashes": {}},
        {"identity": IDENTITY | {"start_url_hash": "garbled"}},
        {"overlap_hashes": {"a": "bad"}},
    ],
)
def test_begin_requires_original_and_verified_boundary_values(change):
    with pytest.raises(ValueError):
        start(**change)


def test_new_retry_recovers_from_closed_group_before_page_limit(monkeypatch):
    monkeypatch.setattr(recovery, "SEGMENT_PAGES", 3)
    initial = start()
    assert recovery.reanchor(initial, identity=IDENTITY) == initial
    issued = step(initial, [("a", 7), ("b", 7), ("old", 6)], NEXT)
    before = deepcopy(issued)
    fresh = recovery.reanchor(issued, identity=IDENTITY)
    assert before == issued
    anchored = restore(fresh)
    assert (anchored["upper"], anchored["overlap_timestamp"]) == (stamp(7), stamp(6))
    assert anchored["remaining_overlap"] == {"old": digest("old")}
    assert (anchored["pages"], anchored["observations"], anchored["segment_start_pages"]) == (1, 3, 1)
    assert anchored["url"] != NEXT and anchored["original"] == issued[recovery.KEY]["original"]
    saved = step(fresh, [("old", 6), ("c", 5)], NEXT)
    saved = step(saved, [("d", 4)], LATER)
    rolled = step(saved, [("e", 3)], NEXT)
    assert restore(rolled)["segment_start_pages"] == 4
    finished = step(rolled, [("e", 3), ("f", 2)])
    assert restore(finished)["action"] == "terminal"
    with pytest.raises(ValueError, match="terminal_retry"):
        recovery.reanchor(finished, identity=IDENTITY)


@pytest.mark.parametrize("rows", [[("a", 7), ("b", 7)], [("newer", 8), ("a", 7)]])
def test_new_retry_requires_both_closed_group_and_verified_overlap(rows):
    saved = step(start(), rows, NEXT)
    with pytest.raises(ValueError, match="no_safe_rollover"):
        recovery.reanchor(saved, identity=IDENTITY)


def test_terminal_on_fresh_search_binds_exact_filtered_url():
    initial = start()
    url = initial[recovery.KEY]["url"]
    finished = step(initial, [("a", 7), ("b", 7)])
    restored = restore(finished)
    assert restored["terminal_request_url"] == url
    assert restored["terminal_request_sha256"] == digest(url)
    restored = finished[recovery.KEY]
    restored.update(terminal_request_url=NEXT, terminal_request_sha256=digest(NEXT))
    with pytest.raises(ValueError, match="terminal"):
        restore(finished)


@pytest.mark.parametrize(
    "url,hash_value",
    [
        (NEXT, digest(LATER)),
        (LATER, digest(NEXT)),
        (None, digest(NEXT)),
        (
            "https://other.example/fhir/Organization?_page_token=x",
            digest("https://other.example/fhir/Organization?_page_token=x"),
        ),
    ],
)
def test_terminal_on_issued_page_validates_scope_and_raw_bytes(url, hash_value):
    saved = step(start(), [("a", 7), ("b", 7)], NEXT)
    finished = step(saved, [])
    assert restore(finished)["terminal_request_url"] == NEXT
    finished[recovery.KEY].update(terminal_request_url=url, terminal_request_sha256=hash_value)
    with pytest.raises(ValueError, match="terminal"):
        restore(finished)


def test_active_checkpoint_cannot_have_a_terminal_request():
    initial = start()
    initial[recovery.KEY]["terminal_request_url"] = NEXT
    with pytest.raises(ValueError, match="progress"):
        restore(initial)


def diagnostic(proof):
    state = restore(proof)
    return {
        recovery.KEY: recovery.completion_receipt(proof),
        "fetch_mode": recovery.FETCH_MODE,
        "pages_fetched": 4 + state["pages"],
        "rows_fetched": 80 + state["observations"],
        "complete": state["action"] == "terminal",
        "error": None,
        "bounded": False,
        "next_url_remaining": state["action"] != "terminal",
    }


def test_diagnostic_receipt_exposes_only_logical_scope_and_aggregate_evidence():
    issued = step(start(), [("a", 7), ("b", 7), ("last", 6)], NEXT)
    fresh = recovery.reanchor(issued, identity=IDENTITY)
    finished = step(fresh, [("last", 6), ("older", 5)])
    report = diagnostic(finished)
    terminal_url = finished[recovery.KEY]["terminal_request_url"]
    assert report[recovery.KEY] == {
        "version": 1,
        "complete": True,
        "identity": IDENTITY,
        "original_pages": 4,
        "original_rows": 80,
        "recovery_pages": 2,
        "recovery_observations": 5,
        "original_request_sha256": digest(ORIGINAL),
        "terminal_request_sha256": digest(terminal_url),
        "initial_upper": stamp(9),
        "initial_overlap": stamp(7),
        "final_upper": stamp(7),
    }
    wire = json.dumps(report)
    for private_value in (ORIGINAL, NEXT, terminal_url, digest("a"), digest("b"), digest("last")):
        assert private_value not in wire
    assert recovery.diagnostic_is_complete(report, expected={"dataset_id": IDENTITY["dataset_id"]})
    assert not recovery.diagnostic_is_complete(report, expected={"source_id": "other"})
    assert not recovery.diagnostic_is_complete(report, expected={"unknown": None})
    assert not recovery.diagnostic_is_complete(report, expected=["dataset_id"])
    assert not recovery.diagnostic_is_complete(report | {recovery.KEY: finished[recovery.KEY]})
    assert not recovery.diagnostic_is_complete(report | {recovery.FETCH_MODE + "_v2": {}})


def test_active_diagnostic_is_minimal_and_cannot_be_promoted():
    report = diagnostic(step(start(), [("a", 7)], NEXT))
    assert report[recovery.KEY] == {"version": 1, "complete": False}
    assert not recovery.diagnostic_is_complete(report)
    assert not recovery.diagnostic_is_complete(report | {"complete": True, "next_url_remaining": False})
    assert not recovery.diagnostic_is_complete({"fetch_mode": recovery.FETCH_MODE})
    assert not recovery.diagnostic_is_complete({"fetch_mode": recovery.FETCH_MODE + "_v2"})
    assert recovery.diagnostic_is_complete({"fetch_mode": "checkpoint_complete"})


@pytest.mark.parametrize(
    "receipt_change",
    [
        {"version": True},
        {"version": 2},
        {"complete": False},
        {"original_pages": True},
        {"original_rows": 0},
        {"recovery_pages": 0},
        {"recovery_observations": 0},
        {"original_request_sha256": "unknown"},
        {"identity": IDENTITY | {"canonical_api_base": COLLECTION + "?_page_token=secret"}},
        {"terminal_request_sha256": None},
        {"initial_overlap": stamp(9)},
        {"final_upper": stamp(10)},
        {"initial_upper": "2026-01-01"},
        {"unexpected": True},
    ],
)
def test_invalid_compact_receipt_is_rejected(receipt_change):
    report = diagnostic(step(start(), [("a", 7), ("b", 7)]))
    report[recovery.KEY].update(receipt_change)
    assert not recovery.diagnostic_is_complete(report)


@pytest.mark.parametrize(
    "change",
    [
        {"error": "error"},
        {"bounded": True},
        {"bounded": None},
        {"complete": False},
        {"next_url_remaining": True},
        {"pages_fetched": 4},
        {"rows_fetched": True},
        {"fetch_mode": "checkpoint_complete"},
    ],
)
def test_outer_diagnostic_must_agree_with_receipt(change):
    report = diagnostic(step(start(), [("a", 7), ("b", 7)]))
    assert not recovery.diagnostic_is_complete(report | change)


def test_terminal_receipt_page_counts_require_a_reachable_final_bound(monkeypatch):
    monkeypatch.setattr(recovery, "SEGMENT_PAGES", 2)
    one_page = diagnostic(step(start(), [("a", 7), ("b", 7)]))
    assert recovery.diagnostic_is_complete(one_page)
    one_page[recovery.KEY]["final_upper"] = stamp(8)
    assert not recovery.diagnostic_is_complete(one_page)

    saved = step(start(), [("a", 7), ("b", 7), ("older", 6)], NEXT)
    at_limit = diagnostic(step(saved, []))
    assert recovery.diagnostic_is_complete(at_limit)
    at_limit[recovery.KEY]["recovery_pages"] += 1
    at_limit["pages_fetched"] += 1
    assert not recovery.diagnostic_is_complete(at_limit)


def test_receipt_requires_validated_original_bounds_and_terminal_request():
    finished = step(start(), [("a", 7), ("b", 7)])
    finished[recovery.KEY]["original"]["upper"] = stamp(11)
    with pytest.raises(ValueError, match="initial_segment"):
        recovery.completion_receipt(finished)
    finished[recovery.KEY]["original"]["upper"] = stamp(9)
    finished[recovery.KEY]["terminal_request_sha256"] = digest("different")
    with pytest.raises(ValueError, match="terminal"):
        recovery.completion_receipt(finished)
