# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Payer discovery identity must survive reviewed source renames safely."""

import datetime as dt

from process import mrf_source_discovery as discovery


def _candidate(name="Example Old Plan", *, url="https://example.test/mrf", **kwargs):
    return discovery.SourceCandidate(
        payer_name=name,
        provider="master-list",
        index_url=url,
        **kwargs,
    )


def _stored_source(candidate):
    _, row = discovery._candidate_to_rows(
        candidate,
        dt.datetime(2026, 1, 1),
        payer_id="mrfpayer_legacy",
        source_id="mrfsource_legacy",
    )
    assert row is not None
    return row


def test_new_payer_and_source_ids_are_opaque_and_not_name_derived():
    candidate = _candidate()
    first_payer, first_source = discovery._candidate_to_rows(candidate, dt.datetime(2026, 1, 1))
    second_payer, second_source = discovery._candidate_to_rows(candidate, dt.datetime(2026, 1, 1))

    assert first_source is not None and second_source is not None
    assert first_payer["payer_id"].startswith("mrfpayer_")
    assert first_source["source_id"].startswith("mrfsource_")
    assert first_payer["payer_id"] != second_payer["payer_id"]
    assert first_source["source_id"] != second_source["source_id"]


def test_existing_source_requires_url_provider_query_and_name_evidence():
    old = _candidate()
    stored = _stored_source(old)
    renamed = _candidate("Example New Plan", aliases=("Example Old Plan",))

    assert discovery._candidate_matches_stored_source(renamed, stored)
    assert not discovery._candidate_matches_stored_source(_candidate("Example New Plan"), stored)
    assert not discovery._candidate_matches_stored_source(
        _candidate("Example New Plan", url="https://example.test/other", aliases=("Example Old Plan",)),
        stored,
    )
    assert not discovery._candidate_matches_stored_source(
        discovery.SourceCandidate(
            payer_name="Example New Plan",
            provider="another-list",
            index_url=old.index_url,
            aliases=("Example Old Plan",),
        ),
        stored,
    )
    assert not discovery._candidate_matches_stored_source(
        _candidate(
            "Example New Plan",
            aliases=("Example Old Plan",),
            raw_payload={"target_payer_query": "Distinct Employer"},
        ),
        stored,
    )


def test_catalog_updates_preserve_existing_payer_and_source_facts():
    old = _candidate()
    existing_source = _stored_source(old)
    existing_source["metadata_json"]["catalog_paging_manifest"] = {"snapshot": "keep"}
    existing_source["etag"] = "keep-etag"
    existing_source["status"] = "active"
    renamed = _candidate("Example New Plan", aliases=("Example Old Plan",))
    incoming_payer, incoming_source = discovery._candidate_to_rows(
        renamed,
        dt.datetime(2026, 2, 1),
        discovery_run_id="run_new",
        payer_id="mrfpayer_legacy",
        source_id="mrfsource_legacy",
    )
    existing_payer_by_field = {
        "canonical_name": "Example Old Plan",
        "aliases": ["Example Old Plan"],
        "eins": ["12-3456789"],
        "lifecycle": "reviewed",
        "created_at": dt.datetime(2025, 1, 1),
        "metadata_json": {"reviewed_evidence": "keep"},
    }

    payer_changes = discovery._preserved_payer_updates(
        existing_payer_by_field, incoming_payer, is_confirmed_rename=True
    )
    assert payer_changes["canonical_name"] == "Example New Plan"
    assert payer_changes["aliases"] == ["Example New Plan", "Example Old Plan"]
    assert payer_changes["metadata_json"]["reviewed_evidence"] == "keep"
    assert "eins" not in payer_changes
    assert "lifecycle" not in payer_changes
    assert "created_at" not in payer_changes

    assert incoming_source is not None
    source_changes = discovery._preserved_source_updates(existing_source, incoming_source, is_confirmed_rename=True)
    assert source_changes["display_name"] == "Example New Plan"
    assert source_changes["metadata_json"]["catalog_paging_manifest"] == {"snapshot": "keep"}
    assert source_changes["metadata_json"]["discovery_run_id"] == "run_new"
    assert "etag" not in source_changes
    assert "status" not in source_changes
    assert "source_key" not in source_changes


def test_matched_source_tier_tracks_both_catalog_transitions():
    """A reviewed source follows catalog importability without losing crawl facts."""
    for previous_tier, current_tier in (
        ("mrf_importable", "coverage_evidence"),
        ("coverage_evidence", "mrf_importable"),
    ):
        previous = _stored_source(_candidate(source_tier=previous_tier))
        previous["metadata_json"]["catalog_paging_manifest"] = {"snapshot": "keep"}
        previous["review_status"] = "approved"
        _, incoming = discovery._candidate_to_rows(
            _candidate(source_tier=current_tier),
            dt.datetime(2026, 2, 1),
            payer_id="mrfpayer_legacy",
            source_id="mrfsource_legacy",
        )
        assert incoming is not None
        changes = discovery._preserved_source_updates(previous, incoming, is_confirmed_rename=False)
        assert changes["metadata_json"]["source_tier"] == current_tier
        assert changes["metadata_json"]["catalog_paging_manifest"] == {"snapshot": "keep"}
        assert "review_status" not in changes
        assert discovery._source_row_source_tier({**previous, **changes}) == current_tier


def test_one_curated_row_with_two_urls_has_one_payer_group():
    rows = discovery.parse_master_list(
        "| Payer | Type | Public MRF TOC / landing URL | Notes |\n"
        "|---|---|---|---|\n"
        "| Example Plan | regional | https://example.test/one · https://example.test/two | public indexes |\n"
    )
    assert len(rows) == 2
    assert discovery._curated_payer_row_key(rows[0]) == discovery._curated_payer_row_key(rows[1])
