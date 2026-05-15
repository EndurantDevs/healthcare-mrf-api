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


def test_modifier_code_rows_include_professional_and_technical_components():
    rows = code_sets.modifier_code_rows()

    by_code = {row.code: row for row in rows}
    assert by_code["26"].display_name == "Professional component"
    assert by_code["TC"].display_name == "Technical component"
    assert by_code["NU"].display_name == "New equipment"
    assert by_code["RR"].display_name == "Rental"
    assert by_code["QW"].display_name == "CLIA waived test"
    assert by_code["95"].display_name == "Synchronous telemedicine service"
    assert by_code["GQ"].display_name == "Asynchronous telecommunications system"
    assert by_code["UE"].display_name == "Used durable medical equipment"
    assert by_code["TC"].source == code_sets.SOURCE_MODIFIER


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
