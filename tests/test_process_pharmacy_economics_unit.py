# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from pathlib import Path

import pytest


@pytest.fixture
def econ_module():
    return importlib.import_module("process.pharmacy_economics")


@pytest.mark.asyncio
async def test_fetch_sdud_uses_prescription_counts_only(monkeypatch, econ_module):
    csv_payload = (
        "state,ndc,product_name,number_of_prescriptions,total_amount_reimbursed\n"
        "IL,12345-6789-01,Drug A,150,99999\n"
        "IL,12345-6789-02,Drug B,,120000\n"
        "XX,12345-6789-03,Drug C,111,11111\n"
    )

    async def _fake_download(_client, _url, tmp_dir, _file_name):
        path = Path(tmp_dir) / "sdud.csv"
        path.write_text(csv_payload, encoding="utf-8")
        return str(path)

    monkeypatch.setattr(econ_module, "_download_to_temp_csv", _fake_download)

    result = await econ_module._fetch_sdud(client=None, sdud_url="https://example.invalid/sdud.csv")
    assert "IL" in result
    assert "12345678901" in result["IL"]
    assert result["IL"]["12345678901"]["volume"] == 150
    assert "12345678902" not in result["IL"]
    assert "XX" not in result
