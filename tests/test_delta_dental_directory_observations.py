# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Qualify rendered result structure without asserting an entire directory."""

import pytest

from process.delta_dental_directory_observations import qualify_rendered_finder


def test_badges_stay_with_their_office():
    report_by_field = qualify_rendered_finder(
        b"""
      <h2>100 results(within 30 miles) All networks</h2>
      <span>Delta Dental Medicare Advantage</span>
      <article>
        <a href="#" aria-label="1 Example St, Example, AA 00000, get directions for Example Dentist">Address</a>
        <div role="group" aria-label="Example Dentist participates in">
          <button disabled>Delta Dental PPO</button><button disabled>Delta Dental Premier</button>
        </div>
      </article>
      <article>
        <a href="#" aria-label="2 Test St, Example, AA 00000, get directions for Example Dentist">Address</a>
        <fieldset>
          <legend>Example Dentist participates in</legend>
          <button aria-disabled="true">Delta Dental Patient Direct</button>
          <button disabled>Delta Dental Example Regional Plan</button>
        </fieldset>
      </article>
    """,
        include_observations=True,
    )
    assert report_by_field["card_observations"] == 2
    assert report_by_field["membership_observations"] == {
        "Delta Dental PPO": 1,
        "Delta Dental Premier": 1,
        "Delta Dental Patient Direct": 1,
    }
    assert report_by_field["gaps"] == [
        "displayed_total_exceeds_observations",
        "geographic_coverage_unverified",
        "page_identity_unverified",
        "publisher_identity_unverified",
        "result_cap_reached",
        "unmapped_card_network",
    ]
    first, second = report_by_field["office_observations"]
    assert first["address"].startswith("1 Example St")
    assert first["network_labels"] == ("Delta Dental PPO", "Delta Dental Premier")
    assert second["address"].startswith("2 Test St")
    assert second["network_labels"] == ("Delta Dental Patient Direct", "Delta Dental Example Regional Plan")
    assert first["practitioner_npi"] is second["practitioner_id"] is second["office_id"] is None


def test_unrendered_form_is_rejected():
    with pytest.raises(ValueError, match="rendered_result_heading_missing_or_ambiguous"):
        qualify_rendered_finder(b"<h1>Looking for a dentist?</h1>")
    with pytest.raises(ValueError, match="rendered_membership_groups_missing"):
        qualify_rendered_finder(b"<h1>2 results(within 5 miles) Delta Dental Premier</h1>")


@pytest.mark.asyncio
async def test_private_scoped_capture_export(tmp_path):
    import json

    from process.delta_dental_directory_capture import FinderCapturePlan, FinderSearch, iter_finder_windows
    from process.delta_dental_directory_observations import export_retained_capture, parse_rendered_finder_page
    from process.delta_dental_directory_worker import _capture

    async def fetch(_search, _number, _previous_page):
        return b"""
          <div aria-live="polite">2 dentist results found within 30 miles for Delta Dental PPO</div>
          <article>
            <a href="#" aria-label="1 Example St, Example, AA 00000, get directions for Example Dentist">Address</a>
            <div aria-label="Example Dentist participates in"><button disabled>Delta Dental PPO</button></div>
          </article>
          <article>
            <div aria-label="Unbound Dentist participates in"><button disabled>Delta Dental PPO</button></div>
          </article>
        """

    async def check():
        return None

    search = FinderSearch("Delta Dental PPO", "00000", 30)
    windows = [
        window
        async for window in iter_finder_windows(
            FinderCapturePlan(tmp_path, "example", "example-edition", (search,)),
            fetch_page=fetch,
            parse_page=parse_rendered_finder_page,
            cancel_check=check,
        )
    ]
    assert len(windows) == 1
    output = tmp_path / "offices.ndjson"
    report = export_retained_capture(tmp_path / "finder-example.sqlite3", output)
    assert report == {
        "directory_complete": False,
        "pagination_gaps": {},
        "pages": 1,
        "card_observations": 2,
        "office_observations": 1,
    }
    assert output.stat().st_mode & 0o777 == 0o600
    observation_by_field, unbound_by_field = map(json.loads, output.read_text().splitlines())
    assert observation_by_field["edition"] == "example-edition"
    assert observation_by_field["search"] == {"selector": "Delta Dental PPO", "zip_code": "00000", "radius_miles": 30}
    assert observation_by_field["memberships"] == ["Delta Dental PPO"]
    assert observation_by_field["page"] == observation_by_field["ordinal"] == 1
    assert observation_by_field["office_id"] is observation_by_field["practitioner_npi"] is None
    assert unbound_by_field["address"] is None and unbound_by_field["memberships"] == []
    assert unbound_by_field["network_labels"] == ["Delta Dental PPO"]

    async def unexpected_egress(*_arguments):
        pytest.fail("retained replay must not contact the source")

    capture = FinderCapturePlan(tmp_path, "example", "example-edition", (search,), plan_id="replay")
    metrics_by_name = await _capture(capture, check, fetch_page=unexpected_egress)
    assert metrics_by_name["card_observations"] == 2
    assert metrics_by_name["office_observations"] == 1
