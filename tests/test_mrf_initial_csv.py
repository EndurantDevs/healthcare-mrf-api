# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from process import initial


def _csv_bytes(attribute_rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(attribute_rows[0]))
    writer.writeheader()
    writer.writerows(attribute_rows)
    text = stream.getvalue()
    assert len(text) > 131072
    return text.encode("utf-8-sig")


def _federal_attribute_rows():
    attribute_rows = []
    for index in range(1024):
        plan_id = f"{20000 + index}AA{index:07d}"
        attribute_rows.append(
            {
                "StandardComponentId": plan_id,
                "PlanId": f"{plan_id}-00",
                "BusinessYear": "2026",
                "IssuerId": str(20000 + index),
                "StateCode": "aa",
                "PlanMarketingName": f'Synthetic {index}, "café" coverage\n' + "naïve 東京 " * 13,
                "URLForSummaryofBenefitsCoverage": f"https://data.example.invalid/{plan_id}/summary",
                "PlanBrochure": f"https://data.example.invalid/{plan_id}/brochure",
                "FormularyURL": f"https://data.example.invalid/{plan_id}/formulary",
                "NetworkId": f"network-{index}",
                "ImportDate": "2026-01-02",
                "IssuerMarketPlaceMarketingName": f"Synthetic société {index}",
            }
        )
    return attribute_rows


@pytest.mark.asyncio
async def test_federal_csv_preserves_identities_and_text_across_read_boundaries(monkeypatch):
    """Preserve plan identities and Unicode fields across CSV read boundaries."""
    attribute_rows = _federal_attribute_rows()
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("attributes.csv", _csv_bytes(attribute_rows))
    download_directories = []

    async def download(_source_url, destination):
        destination = Path(destination)
        download_directories.append(destination.parent)
        destination.write_bytes(archive.getvalue())

    monkeypatch.setattr(initial, "download_it_and_save", download)
    monkeypatch.setattr(initial, "unzip", AsyncMock(side_effect=OSError("use standard archive reader")))
    plans, issuers = {}, {}

    await initial._read_unknown_federal_attribute_file("https://data.example.invalid/catalog.zip", plans, issuers)

    expected_plan_values_by_key = {
        f"{attribute_row['StandardComponentId']}_2026": (
            attribute_row["StandardComponentId"],
            int(attribute_row["IssuerId"]),
            attribute_row["PlanMarketingName"],
            attribute_row["URLForSummaryofBenefitsCoverage"],
            attribute_row["PlanBrochure"],
            attribute_row["FormularyURL"],
        )
        for attribute_row in attribute_rows
    }
    actual_plan_values_by_key = {
        key: tuple(
            plan[field]
            for field in ("plan_id", "issuer_id", "marketing_name", "summary_url", "marketing_url", "formulary_url")
        )
        for key, plan in plans.items()
    }
    assert actual_plan_values_by_key == expected_plan_values_by_key
    assert {key: issuer["issuer_name"] for key, issuer in issuers.items()} == {
        int(attribute_row["IssuerId"]): attribute_row["IssuerMarketPlaceMarketingName"]
        for attribute_row in attribute_rows
    }
    assert all(not directory.exists() for directory in download_directories)


@pytest.mark.asyncio
async def test_rate_review_csv_preserves_names_and_row_count_across_read_boundaries(tmp_path):
    attribute_rows = [
        {
            "ISSUER_ID": str(20000 + index),
            "STATE": "aa",
            "COMPANY": f'Synthetic {index}, "café"\n' + "société 東京 " * 17,
        }
        for index in range(1024)
    ]
    source = tmp_path / "RatePUF.csv"
    source.write_bytes(_csv_bytes(attribute_rows))

    issuers, row_count = await initial._read_rate_review_issuer_names([str(source)], None)

    assert row_count == len(attribute_rows)
    assert {key: issuer["issuer_name"] for key, issuer in issuers.items()} == {
        int(attribute_row["ISSUER_ID"]): attribute_row["COMPANY"].strip() for attribute_row in attribute_rows
    }
