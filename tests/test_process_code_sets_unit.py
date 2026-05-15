# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib

import pytest

code_sets = importlib.import_module("process.code_sets")


POS_HTML = """
<table>
  <tr><th>Place of Service Code(s)</th><th>Place of Service Name</th><th>Place of Service Description</th></tr>
  <tr><th scope="row">23</th><td>Emergency Room - Hospital</td><td>A portion of a hospital where emergency care is provided.</td></tr>
  <tr><th scope="row">28-30</th><td>Unassigned</td><td>N/A</td></tr>
</table>
"""


RC_HTML = """
<table>
  <tr><th>Code</th><th>Display</th></tr>
  <tr><td>0450</td><td>EMERGENCY ROOM - GENERAL CLASSIFICATION</td></tr>
  <tr><td>0981</td><td>PROFESSIONAL FEES - EMERGENCY ROOM SERVICES</td></tr>
</table>
"""


def test_parse_pos_code_rows_expands_ranges():
    rows = code_sets.parse_pos_code_rows(POS_HTML)

    by_code = {row.code: row for row in rows}
    assert by_code["23"].display_name == "Emergency Room - Hospital"
    assert by_code["23"].source == code_sets.SOURCE_POS
    assert by_code["28"].display_name == "Unassigned"
    assert by_code["30"].display_name == "Unassigned"


def test_parse_revenue_code_rows():
    rows = code_sets.parse_revenue_code_rows(RC_HTML)

    by_code = {row.code: row for row in rows}
    assert by_code["0450"].display_name == "EMERGENCY ROOM - GENERAL CLASSIFICATION"
    assert by_code["0981"].source == code_sets.SOURCE_RC


@pytest.mark.asyncio
async def test_upsert_code_rows_uses_code_catalog(monkeypatch):
    calls = []

    async def fake_status(sql, **params):
        calls.append((sql, params))

    monkeypatch.setattr(code_sets.db, "status", fake_status)

    inserted = await code_sets._upsert_code_rows(
        "mrf",
        [
            code_sets.CodeSetRow(
                code_system="RC",
                code="0450",
                display_name="EMERGENCY ROOM - GENERAL CLASSIFICATION",
                short_description="EMERGENCY ROOM - GENERAL CLASSIFICATION",
                source=code_sets.SOURCE_RC,
            )
        ],
    )

    assert inserted == 1
    assert "INSERT INTO mrf.code_catalog" in calls[0][0]
    assert calls[0][1]["code_system"] == "RC"
    assert calls[0][1]["code"] == "0450"
