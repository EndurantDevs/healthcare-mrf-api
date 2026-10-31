# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A full common cut cannot be confused with the legacy two-stage history."""

from copy import deepcopy

import pytest

from process import provider_directory_common_serving_journal as journal
from process import provider_directory_serving_recovery as recovery


@pytest.fixture
def history():
    before_by_field = {
        "roots": {"profile": 11, "address": 12},
        "initial_targets": {"original": True},
        "metadata": {
            "alias": 1,
            "overlay_alias": 2,
            "profile": None,
            "doctors": 3,
            "geo": 4,
            "relations": [12],
            "address": 5,
            "common_receipt": None,
        },
    }
    after = deepcopy(before_by_field)
    after["roots"]["profile"] = 13
    after["initial_targets"] = None
    after["metadata"].update(profile=6, address=6)
    published_by_field = {
        "source": {"source_id": "example"},
        "preparation": {"contract_id": journal.documents.CONTRACT},
        "before": before_by_field,
        "after": after,
        "predecessor": None,
    }
    restored = deepcopy(before_by_field)
    restored["metadata"]["address"] = 7
    recovered_by_field = {
        "source": deepcopy(published_by_field["source"]),
        "preparation": deepcopy(published_by_field["preparation"]),
        "before": deepcopy(after),
        "after": restored,
        "predecessor": "ab" * 32,
    }
    return {
        "published": {"document": published_by_field, "sha256": "ab" * 32},
        "recovered": {"document": recovered_by_field, "sha256": "cd" * 32},
    }


def test_exact_inverse_restores_head_absence_but_keeps_monotonic_address_authority(history):
    assert journal.validate_history(history) is history
    assert journal.validate_history({"published": history["published"]}) == {"published": history["published"]}


@pytest.mark.parametrize(
    "field",
    [
        "predecessor",
        "source",
        "preparation",
        "before",
        "roots",
        "initial_targets",
        "alias",
        "profile",
        "geo",
        "relations",
    ],
)
def test_recovery_cannot_substitute_another_source_scope_or_baseline(history, field):
    document = history["recovered"]["document"]
    if field in {"predecessor", "source", "preparation", "before"}:
        document[field] = None
    elif field in {"roots", "initial_targets"}:
        document["after"][field] = None
    else:
        document["after"]["metadata"][field] = "changed"
    with pytest.raises(ValueError, match="common_inverse_changed"):
        journal.validate_history(history)


@pytest.mark.parametrize("phases", [[], ["recovered"], ["overlay"], ["overlay", "published"]])
def test_refuse_missing_common_cut_and_mixed_legacy_history(history, phases):
    events_by_phase = {phase: history.get(phase, history["published"]) for phase in phases}
    with pytest.raises(ValueError, match="common_history_changed"):
        journal.validate_history(events_by_phase)


def test_common_append_has_one_forward_cut_and_one_inverse(history):
    preparation = history["published"]["document"]["preparation"]
    assert journal.append_predecessor({}, "published", preparation) is None
    assert journal.append_predecessor({"published": history["published"]}, "recovered", preparation) == "ab" * 32
    for events, phase in ((history, "recovered"), (history, "published"), ({}, "recovered"), ({}, "overlay")):
        with pytest.raises(ValueError, match="common_"):
            journal.append_predecessor(events, phase, preparation)


def test_common_decoder_dispatch_preserves_original_document_arguments(monkeypatch):
    document_by_field = {"preparation": {"contract_id": journal.documents.CONTRACT}}
    calls = []

    def decode(*args):
        calls.append(args)
        return document_by_field

    monkeypatch.setattr(journal.documents, "document", decode)
    assert recovery._document(document_by_field, "published", "generation", "example") is document_by_field
    assert calls == [(document_by_field, "published", "generation", "example")]


@pytest.mark.parametrize("value", [None, {}, {"contract_id": "legacy"}, [("contract_id", journal.documents.CONTRACT)]])
def test_only_closed_common_contract_selects_new_path(value):
    assert journal.is_common(value) is False
