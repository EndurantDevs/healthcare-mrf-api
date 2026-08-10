import importlib
import datetime
import io
import zipfile
from unittest.mock import AsyncMock

import pytest

pharmacy_license = importlib.import_module("process.pharmacy_license")

_PHARMACY_SOURCE_HTML = (
    "<h2>Board of Pharmacy License Databases by State</h2>"
    '<a href="https://example.com/tx">Texas</a>'
)


def test_parse_fda_state_sources_extracts_known_states():
    html = """
    <h2>Board of Pharmacy License Databases by State</h2>
    <ul>
      <li><a href=\"https://example.com/tx\">Texas</a></li>
      <li><a href=\"https://example.com/nm\">New Mexico </a></li>
      <li><a href=\"https://example.com/ok\">Oklahoma</a></li>
    </ul>
    """

    sources = pharmacy_license._parse_fda_state_sources(html)

    assert [item.state_code for item in sources] == ["NM", "OK", "TX"]
    assert sources[0].board_url == "https://example.com/nm"


def test_normalize_license_status_maps_known_values():
    assert pharmacy_license._normalize_license_status("ACTIVE") == "active"
    assert pharmacy_license._normalize_license_status("License suspended") == "suspended"
    assert pharmacy_license._normalize_license_status("revoked by board") == "revoked"
    assert pharmacy_license._normalize_license_status("expired") == "expired"
    assert pharmacy_license._normalize_license_status("Clear") == "active"
    assert pharmacy_license._normalize_license_status("Null And Void") == "inactive"
    assert pharmacy_license._normalize_license_status(None) == "unknown"


@pytest.mark.asyncio
async def test_materialize_snapshot_aborts_on_canonical_address_failure(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "source_enabled", lambda source: source == "pharmacy_license")

    async def fail_stamp(*_args, **_kwargs):
        raise RuntimeError("collision")

    monkeypatch.setattr(pharmacy_license, "stamp_address_keys", fail_stamp)

    with pytest.raises(
        pharmacy_license.PharmacyLicenseCanonicalAddressError,
        match="canonical address resolve failed",
    ):
        await pharmacy_license._materialize_snapshot("mrf", "snapshot_1", "run_1")


class _FakeClientSession:
    """Minimal async session used by pharmacy failure-path tests."""

    def __init__(self, *_args, **_kwargs):
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


def _patch_failed_pharmacy_run(monkeypatch, updates_by_name):
    """Install a complete import whose materialization fails canonically."""
    async def noop(*_args, **_kwargs):
        return None

    async def fake_download(*_args, **_kwargs):
        return _PHARMACY_SOURCE_HTML

    async def fake_import_state(*_args, **_kwargs):
        return pharmacy_license.StateImportStats(
            supported=True,
            status="completed",
            source_url="https://example.com/tx.csv",
            unsupported_reason=None,
            error_text=None,
            row_count_parsed=1,
            row_count_matched=1,
            row_count_dropped=0,
            row_count_inserted=0,
            metadata={},
        )

    async def fail_materialize(*_args, **_kwargs):
        raise pharmacy_license.PharmacyLicenseCanonicalAddressError("canonical collision")

    async def fake_upsert_run(payload):
        updates_by_name["run"].append(payload)

    async def fake_upsert_snapshot(payload):
        updates_by_name["snapshot"].append(payload)

    async def fake_upsert_coverage(payload):
        updates_by_name["coverage"].append(payload)

    async def fake_mark_control_run(run_id, **payload):
        updates_by_name["control"].append({"run_id": run_id, **payload})

    replacement_map = {
        "ensure_database": noop,
        "_ensure_tables": AsyncMock(return_value="mrf"),
        "_truncate_stage_table": noop,
        "_drop_secondary_indexes": noop,
        "_ensure_secondary_indexes": noop,
        "_analyze_tables": noop,
        "download_it": fake_download,
        "_import_state_source": fake_import_state,
        "_materialize_snapshot": fail_materialize,
        "_upsert_run": fake_upsert_run,
        "_upsert_snapshot": fake_upsert_snapshot,
        "_upsert_coverage": fake_upsert_coverage,
        "mark_control_run": fake_mark_control_run,
        "enqueue_live_progress": lambda **_payload: None,
    }
    for name, replacement in replacement_map.items():
        monkeypatch.setattr(pharmacy_license, name, replacement)
    monkeypatch.setattr(
        pharmacy_license.aiohttp,
        "ClientSession",
        _FakeClientSession,
    )


@pytest.mark.asyncio
async def test_start_marks_run_failed_on_canonical_address_failure(monkeypatch):
    """Verify canonical-address failures terminalize all license run state."""
    updates_by_name = {
        "run": [],
        "snapshot": [],
        "coverage": [],
        "control": [],
    }
    _patch_failed_pharmacy_run(monkeypatch, updates_by_name)

    with pytest.raises(pharmacy_license.PharmacyLicenseCanonicalAddressError, match="canonical collision"):
        await pharmacy_license.pharmacy_license_start(
            {},
            {"run_id": "run_1", "import_id": "import_1", "test_mode": True},
        )

    assert updates_by_name["run"][-1]["status"] == "failed"
    assert updates_by_name["run"][-1]["error_text"] == "canonical collision"
    assert updates_by_name["snapshot"][-1]["status"] == "failed"
    assert updates_by_name["coverage"][-1]["status"] == "failed"
    assert updates_by_name["control"][-1]["status"] == "failed"
    assert updates_by_name["control"][-1]["error"]["message"] == "canonical collision"


def test_normalize_stage_row_drops_missing_npi():
    source = pharmacy_license.StateSource(state_code="TX", state_name="Texas", board_url="https://example.com/tx")
    row_map = {
        "License Number": "TX-1234",
        "License Status": "Active",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/feed.csv",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
    )

    assert payload is None
    assert reason == "missing_npi"


def test_normalize_stage_row_maps_interesting_fields():
    state_source = pharmacy_license.StateSource(
        state_code="TX",
        state_name="Texas",
        board_url="https://example.com/tx",
    )
    license_field_map = {
        "NPI": "1518379601",
        "License Number": "TX-PH-00001",
        "License Type": "Pharmacy",
        "License Status": "Active",
        "Expiration Date": "2027-01-31",
        "Issue Date": "2019-05-01",
        "Entity Name": "Sample Pharmacy",
        "DBA": "Sample RX",
        "Address": "100 Main",
        "City": "Austin",
        "State": "TX",
        "Zip": "78701",
        "Phone": "555-555-5555",
        "Disciplinary Summary": "",
        "Last Updated": "2026-02-15",
    }

    stage_payload, reason = pharmacy_license._normalize_stage_row(
        license_field_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=state_source,
        source_url="https://example.com/feed.csv",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
    )

    assert reason is None
    assert stage_payload is not None
    assert stage_payload["npi"] == 1518379601
    assert stage_payload["license_number"] == "TX-PH-00001"
    assert stage_payload["license_status"] == "active"
    assert stage_payload["license_expiration_date"].isoformat() == "2027-01-31"
    assert stage_payload["state_code"] == "TX"


def test_extract_candidate_file_links_filters_noise():
    html = """
    <a href=\"/export/pharmacy.csv\">CSV</a>
    <a href=\"https://example.com/sitemap.xml\">Sitemap</a>
    <a href=\"/download/pharmacy.json\">JSON</a>
    """

    links = pharmacy_license._extract_candidate_file_links(html, "https://state.example.com/lookup")

    assert "https://state.example.com/export/pharmacy.csv" in links
    assert "https://state.example.com/download/pharmacy.json" in links
    assert all("sitemap" not in link for link in links)


def test_parse_datagrid_rows_handles_nested_aspnet_cells():
    html = """
    <table id="unrelated"><tr><td>Ignore this table</td></tr></table>
    <table id="datagrid_results">
      <tr>
        <th>Name</th><th>License #</th><th>License Type</th><th>Status</th><th>Address</th>
      </tr>
      <tr>
        <td>
          <table role="presentation">
            <tr><td><a href="Details.aspx?result=abc">2200 PHARMACY INC</a></td></tr>
            <tr><td></td></tr>
          </table>
        </td>
        <td><span>60002818A</span></td>
        <td><span>Pharmacy</span></td>
        <td><span>Expired</span></td>
        <td><span>GARY IN 46404</span></td>
      </tr>
    </table>
    """

    parsed_rows = pharmacy_license._parse_datagrid_rows(html)

    assert len(parsed_rows) == 1
    assert parsed_rows[0]["Name"] == "2200 PHARMACY INC"
    assert parsed_rows[0]["License #"] == "60002818A"
    assert parsed_rows[0]["Address"] == "GARY IN 46404"

    hydrated = pharmacy_license._hydrate_row_with_address_parts(parsed_rows[0])
    assert hydrated["City"] == "GARY"
    assert hydrated["State"] == "IN"
    assert hydrated["Zip"] == "46404"


def test_extract_postback_targets_parses_numeric_pager_links():
    html = """
    <a href="javascript:__doPostBack(&#39;datagrid_results$_ctl44$_ctl1&#39;,&#39;&#39;)"><font>2</font></a>
    <a href="javascript:__doPostBack(&#39;datagrid_results$_ctl44$_ctl2&#39;,&#39;&#39;)"><font>3</font></a>
    <a href="javascript:__doPostBack(&#39;datagrid_results$_ctl44$_ctl3&#39;,&#39;&#39;)"><font>Next</font></a>
    """

    targets = pharmacy_license._extract_postback_targets(html)

    assert targets == {
        2: "datagrid_results$_ctl44$_ctl1",
        3: "datagrid_results$_ctl44$_ctl2",
    }


def test_normalize_stage_row_uses_npi_resolver_when_npi_missing():
    resolver = pharmacy_license.StateNpiResolver(state_code="IN")
    resolver.by_license = {"60002818A": 1518379601}
    source = pharmacy_license.StateSource(state_code="IN", state_name="Indiana", board_url="https://example.com")
    row_map = {
        "Name": "2200 PHARMACY INC",
        "License #": "60002818A",
        "License Type": "Pharmacy",
        "Status": "Active",
        "Address": "GARY IN 46404",
        "City": "GARY",
        "State": "IN",
        "Zip": "46404",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/results",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
        npi_resolver=resolver,
    )

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1518379601


def test_normalize_stage_row_uses_other_identifier_resolver_when_npi_missing():
    resolver = pharmacy_license.StateNpiResolver(state_code="MA")
    resolver.by_other_identifier = {"MAPH00123": 1518379602}
    source = pharmacy_license.StateSource(state_code="MA", state_name="Massachusetts", board_url="https://example.com")
    row_map = {
        "License Number": "MA-PH-00123",
        "License Type": "Pharmacy",
        "License Status": "Active",
        "Entity Name": "Sample Pharmacy",
        "City": "Boston",
        "State": "MA",
        "Zip": "02108",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/results",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
        npi_resolver=resolver,
    )

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1518379602
    assert resolver.stats == {"other_identifier": 1}


def test_normalize_stage_row_uses_other_identifier_digits_resolver_when_needed():
    resolver = pharmacy_license.StateNpiResolver(state_code="MA")
    resolver.by_other_identifier_digits = {"1200345": 1518379603}
    source = pharmacy_license.StateSource(state_code="MA", state_name="Massachusetts", board_url="https://example.com")
    row_map = {
        "License Number": "12-00345",
        "License Type": "Pharmacy",
        "License Status": "Active",
        "Entity Name": "Digits Pharmacy",
        "City": "Boston",
        "State": "MA",
        "Zip": "02109",
    }

    payload, reason = pharmacy_license._normalize_stage_row(
        row_map,
        run_id="run_1",
        snapshot_id="snap_1",
        state_source=source,
        source_url="https://example.com/results",
        imported_at=datetime.datetime(2026, 3, 10, 0, 0, 0),
        npi_resolver=resolver,
    )

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1518379603
    assert resolver.stats == {"other_identifier_digits": 1}


def test_state_npi_resolver_name_fallback_requires_partd_quality_gate():
    resolver = pharmacy_license.StateNpiResolver(state_code="NJ")
    resolver.by_name_zip = {("samplepharmacy", "07001"): 1518379604}

    blocked = resolver.resolve(
        license_number=None,
        entity_name="Sample Pharmacy",
        dba_name=None,
        city="Newark",
        zip_code="07001",
    )
    assert blocked is None

    resolver.partd_name_fallback_enabled = True
    allowed = resolver.resolve(
        license_number=None,
        entity_name="Sample Pharmacy",
        dba_name=None,
        city="Newark",
        zip_code="07001",
    )
    assert allowed == 1518379604
    assert resolver.stats["name_zip"] == 1


def test_state_npi_resolver_uses_registry_name_city_before_partd_fallback():
    resolver = pharmacy_license.StateNpiResolver(state_code="NJ")
    resolver.by_registry_name_city = {("acmepharmacy", "newark"): 1518379605}
    resolver.by_name_city = {("acmepharmacy", "newark"): 1518379999}
    resolver.partd_name_fallback_enabled = True

    mapped = resolver.resolve(
        license_number=None,
        entity_name="Acme Pharmacy",
        dba_name=None,
        city="Newark",
        zip_code=None,
    )

    assert mapped == 1518379605
    assert resolver.stats == {"registry_name_city": 1}


def test_name_candidates_for_match_handles_dba_and_department_suffix():
    keys = pharmacy_license._name_candidates_for_match(
        "ACME MARKETS, INC., D/B/A ACME PHARMACY DEPT. 1054",
        None,
    )

    assert "acmemarketsincdbaacmepharmacydept1054" in keys
    assert "acmepharmacy" in keys


def test_license_like_identifier_issuer_filters_non_license_values():
    assert pharmacy_license._is_license_like_identifier_issuer("STATE LICENSE") is True
    assert pharmacy_license._is_license_like_identifier_issuer("Medical License") is True
    assert pharmacy_license._is_license_like_identifier_issuer("AETNA") is False
    assert pharmacy_license._is_license_like_identifier_issuer(None) is False


def test_partd_quality_gate_requires_rows_name_and_location():
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=10, named_rows=10, city_rows=10, zip_rows=10)
        is False
    )
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=50, named_rows=0, city_rows=50, zip_rows=50)
        is False
    )
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=50, named_rows=10, city_rows=0, zip_rows=0)
        is False
    )
    assert (
        pharmacy_license._partd_name_fallback_quality_ok(total_rows=50, named_rows=10, city_rows=5, zip_rows=0)
        is True
    )
