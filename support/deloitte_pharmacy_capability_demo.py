#!/usr/bin/env python3
"""Generate Deloitte NYC pharmacy capability demo CSV + PDF from live APIs."""

from __future__ import annotations

import argparse
import csv
import json
import os
import textwrap
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPORT_TITLE = "HealthPorta Pharmacy Access Capability Demo - Deloitte NYC"
OFFICE_NAME = "Deloitte US National Office"
OFFICE_ADDRESS = "30 Rockefeller Plaza, 41st floor, New York, NY 10112-0015"
OFFICE_ZIP = "10112"
PRIMARY_BENCHMARK_ZIP = "10001"
CHICAGO_COMPARISON_ZIP = "60654"
COMPARISON_ZIPS = ("10001", "10020", "10019", "10036", "10022", "60654")
DEFAULT_BASE_URL = "http://127.0.0.1:8085/api/v1"
DEFAULT_TIMEOUT_SECONDS = 120
CSV_FILENAME = "deloitte_nyc_pharmacy_access_demo.csv"
PDF_FILENAME = "HealthPorta_Pharmacy_Access_Capability_Demo_Deloitte_NYC.pdf"
PDF_PAGE_WIDTH = 792
PDF_PAGE_HEIGHT = 612
PDF_MARGIN_X = 40
PDF_TITLE_FONT_SIZE = 16
PDF_BODY_FONT_SIZE = 9
PDF_TITLE_Y = 570
PDF_META_Y = 552
PDF_BODY_START_Y = 528
PDF_BODY_LINE_STEP = 12
SENTINEL_VALUES = {
    "",
    "-666666666",
    "-222222222",
    "-333333333",
    "-999999999",
    "-999",
}
SENTINEL_NUMBERS = {
    -666666666,
    -222222222,
    -333333333,
    -999999999,
    -999,
}
PLACES_MEASURES = {
    "ACCESS2": "places_access2_pct",
    "DIABETES": "places_diabetes_pct",
    "CSMOKING": "places_csmoking_pct",
}
CENSUS_COLUMNS = (
    "total_population",
    "median_household_income",
    "svi_overall",
    "total_employer_establishments",
    "business_employment",
)
CSV_COLUMNS = (
    "zip_code",
    "city",
    "nearest_pharmacy_npi",
    "nearest_pharmacy_name",
    "nearest_pharmacy_distance_miles",
    "access_band",
    "retail_pharmacy_count",
    "active_medicare_share",
    "top_chain_1",
    "top_chain_1_count",
    "commercial_network_match_count_sample",
    "medicare_active_sample",
    "census_total_population",
    "census_median_household_income",
    "census_svi_overall",
    "census_total_employer_establishments",
    "census_business_employment",
    "places_year",
    "places_access2_pct",
    "places_diabetes_pct",
    "places_csmoking_pct",
    "census_data_status",
    "places_data_status",
    "notes",
)


def build_url(base_url: str, path: str, query: dict[str, Any] | None = None) -> str:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if query:
        encoded = urllib.parse.urlencode(query)
        if encoded:
            url = f"{url}?{encoded}"
    return url


def fetch_json(
    base_url: str,
    path: str,
    query: dict[str, Any] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> tuple[int, Any]:
    url = build_url(base_url, path, query)
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            return int(response.status), json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body.strip()}
        return int(exc.code), payload


def clean_network_count(entries: list[dict[str, Any]]) -> int:
    checksums = {
        int(item.get("npi_info", {}).get("checksum_network"))
        for item in entries
        if item.get("npi_info", {}).get("checksum_network") not in (None, "", "0", 0)
    }
    return len(checksums)


def clean_network_array_count(values: list[Any] | None) -> int:
    cleaned = set()
    for value in values or []:
        if value in (None, "", "0", 0):
            continue
        try:
            cleaned.add(int(value))
        except (TypeError, ValueError):
            continue
    return len(cleaned)


def normalize_census_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {item.lower() for item in SENTINEL_VALUES}:
        return None
    if isinstance(value, (int, float)) and value in SENTINEL_NUMBERS:
        return None
    return value


def normalize_census_profile(profile: dict[str, Any] | None) -> tuple[dict[str, Any], str]:
    if not profile:
        return {}, "not_found"

    normalized: dict[str, Any] = {}
    suppressed = False
    for key, value in profile.items():
        cleaned = normalize_census_value(value)
        if cleaned is None and value is not None:
            suppressed = True
        normalized[key] = cleaned

    if suppressed:
        return normalized, "suppressed"
    return normalized, "available"


def access_band_for_distance(distance_miles: float | None) -> str:
    if distance_miles is None:
        return "Unknown"
    if distance_miles <= 0.10:
        return "Walkable (<=0.10 mi)"
    if distance_miles <= 0.25:
        return "Very close (0.10-0.25 mi)"
    if distance_miles <= 0.50:
        return "Short trip (0.25-0.50 mi)"
    if distance_miles <= 1.00:
        return "Local trip (0.50-1.00 mi)"
    return "Extended trip (>1.00 mi)"


def format_number(value: Any, decimals: int = 1) -> str:
    if value in (None, ""):
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if decimals == 0:
        return f"{int(round(number)):,}"
    return f"{number:.{decimals}f}"


def wrap_text(text: str, width: int = 108, indent: str = "") -> list[str]:
    return textwrap.wrap(
        text,
        width=width,
        initial_indent=indent,
        subsequent_indent=indent,
        break_long_words=False,
        break_on_hyphens=False,
    ) or [indent.rstrip()]


def top_chain_summary(market_item: dict[str, Any]) -> tuple[str, Any]:
    top = ((market_item.get("metrics") or {}).get("top_chains") or [])
    if not top:
        return "", ""
    return str(top[0].get("chain") or ""), top[0].get("pharmacy_count") or ""


def derive_row_notes(
    zip_code: str,
    census_status: str,
    places_status: str,
    population: Any,
) -> str:
    notes: list[str] = []
    if zip_code == OFFICE_ZIP:
        notes.append("Office/business ZIP anchored to Deloitte's Rockefeller Plaza office.")
    if zip_code == PRIMARY_BENCHMARK_ZIP:
        notes.append("Primary NYC benchmark ZIP used instead of office ZIP 10112.")
    if zip_code == CHICAGO_COMPARISON_ZIP:
        notes.append("Chicago comparison ZIP included as a second urban market check.")
    if census_status == "suppressed":
        notes.append("Some Census residential fields are suppressed or unavailable.")
    if places_status == "not_found":
        notes.append("CDC PLACES measures are unavailable for this ZIP.")
    if population in (None, "", 0):
        notes.append("Use nearest-distance and market counts more heavily than per-capita context.")
    return " ".join(notes)


def fetch_places_snapshot(base_url: str, zip_code: str) -> tuple[dict[str, Any], str, Any]:
    values = {column: None for column in PLACES_MEASURES.values()}
    year = None
    found_any = False

    for measure_id, column_name in PLACES_MEASURES.items():
        status, payload = fetch_json(base_url, f"/geo/zip/{zip_code}/places", {"measure_id": measure_id})
        if status != 200:
            continue
        measures = payload.get("measures") or []
        if not measures:
            continue
        found_any = True
        year = payload.get("year") if year is None else year
        values[column_name] = measures[0].get("data_value")

    return values, ("available" if found_any else "not_found"), year


def fetch_zip_bundle(base_url: str, zip_code: str) -> dict[str, Any]:
    _, market_payload = fetch_json(base_url, f"/reports/pharmacies/markets/zip:{zip_code}")
    market_item = market_payload.get("item") or {}
    metrics = market_item.get("metrics") or {}

    _, nearest_payload = fetch_json(
        base_url,
        "/npi/near/",
        {"zip_codes": zip_code, "classification": "Pharmacy", "limit": 1},
    )
    nearest = (nearest_payload or [None])[0] if isinstance(nearest_payload, list) else None
    nearest_npi = nearest.get("npi") if nearest else None
    nearest_distance = nearest.get("distance") if nearest else None

    plans_payload = {"npi_data": []}
    if nearest_npi:
        _, plans_payload = fetch_json(base_url, f"/npi/plans_by_npi/{nearest_npi}")
    plan_entries = plans_payload.get("npi_data") or []
    commercial_network_count = clean_network_count(plan_entries)
    if commercial_network_count == 0 and nearest:
        commercial_network_count = clean_network_array_count(nearest.get("plans_network_array"))

    issuer_names = sorted(
        {
            item.get("issuer_info", {}).get("issuer_name")
            for item in plan_entries
            if item.get("issuer_info", {}).get("issuer_name")
        }
    )

    medicare_payload = {"medicare_active": None}
    if nearest_npi:
        _, medicare_payload = fetch_json(base_url, f"/formulary/partd/pharmacies/{nearest_npi}/activity")

    _, geo_payload = fetch_json(base_url, f"/geo/zip/{zip_code}")
    census_profile, census_status = normalize_census_profile(geo_payload.get("census_profile"))
    places_values, places_status, places_year = fetch_places_snapshot(base_url, zip_code)
    top_chain_name, top_chain_count = top_chain_summary(market_item)

    return {
        "zip_code": zip_code,
        "city": market_item.get("city") or geo_payload.get("city") or "",
        "market_item": market_item,
        "metrics": metrics,
        "nearest": nearest or {},
        "nearest_npi": nearest_npi,
        "nearest_distance": nearest_distance,
        "commercial_network_count": commercial_network_count,
        "issuer_names": issuer_names,
        "medicare_payload": medicare_payload,
        "geo_payload": geo_payload,
        "census_profile": census_profile,
        "census_status": census_status,
        "places_values": places_values,
        "places_status": places_status,
        "places_year": places_year,
        "top_chain_name": top_chain_name,
        "top_chain_count": top_chain_count,
    }


def build_csv_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        census = record["census_profile"]
        row = {
            "zip_code": record["zip_code"],
            "city": record["city"],
            "nearest_pharmacy_npi": record["nearest_npi"] or "",
            "nearest_pharmacy_name": record["nearest"].get("do_business_as_text")
            or record["nearest"].get("provider_organization_name")
            or "",
            "nearest_pharmacy_distance_miles": record["nearest_distance"],
            "access_band": access_band_for_distance(record["nearest_distance"]),
            "retail_pharmacy_count": record["metrics"].get("retail_count"),
            "active_medicare_share": record["metrics"].get("active_medicare_share"),
            "top_chain_1": record["top_chain_name"],
            "top_chain_1_count": record["top_chain_count"],
            "commercial_network_match_count_sample": record["commercial_network_count"],
            "medicare_active_sample": record["medicare_payload"].get("medicare_active"),
            "census_total_population": census.get("total_population"),
            "census_median_household_income": census.get("median_household_income"),
            "census_svi_overall": census.get("svi_overall"),
            "census_total_employer_establishments": census.get("total_employer_establishments"),
            "census_business_employment": census.get("business_employment"),
            "places_year": record["places_year"],
            "places_access2_pct": record["places_values"]["places_access2_pct"],
            "places_diabetes_pct": record["places_values"]["places_diabetes_pct"],
            "places_csmoking_pct": record["places_values"]["places_csmoking_pct"],
            "census_data_status": record["census_status"],
            "places_data_status": record["places_status"],
            "notes": derive_row_notes(
                record["zip_code"],
                record["census_status"],
                record["places_status"],
                census.get("total_population"),
            ),
        }
        rows.append(row)
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _pdf_lines_for_page(page_title: str, body_lines: list[str], page_no: int, page_count: int) -> bytes:
    commands = ["BT"]
    commands.append(f"/F1 {PDF_TITLE_FONT_SIZE} Tf")
    commands.append(f"1 0 0 1 {PDF_MARGIN_X} {PDF_TITLE_Y} Tm")
    commands.append(f"({_pdf_escape(page_title)}) Tj")
    commands.append(f"/F2 {PDF_BODY_FONT_SIZE} Tf")
    commands.append(f"1 0 0 1 {PDF_MARGIN_X} {PDF_META_Y} Tm")
    commands.append(f"({_pdf_escape(f'Page {page_no} of {page_count}')}) Tj")

    y = PDF_BODY_START_Y
    for line in body_lines:
        commands.append(f"1 0 0 1 {PDF_MARGIN_X} {y} Tm")
        commands.append(f"({_pdf_escape(line)}) Tj")
        y -= PDF_BODY_LINE_STEP
    commands.append("ET")
    stream = "\n".join(commands).encode("latin-1", errors="replace")
    return b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream"


def render_pdf(path: Path, pages: list[tuple[str, list[str]]]) -> None:
    objects: dict[int, bytes] = {}

    def reserve() -> int:
        next_id = len(objects) + 1
        objects[next_id] = b""
        return next_id

    def set_object(object_id: int, content: bytes) -> None:
        objects[object_id] = content

    catalog_id = reserve()
    pages_id = reserve()
    font_bold_id = reserve()
    font_body_id = reserve()

    page_ids: list[int] = []
    content_ids: list[int] = []
    for _ in pages:
        page_ids.append(reserve())
        content_ids.append(reserve())

    set_object(font_bold_id, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")
    set_object(font_body_id, b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")

    page_count = len(pages)
    for idx, ((title, body_lines), page_id, content_id) in enumerate(zip(pages, page_ids, content_ids), start=1):
        set_object(content_id, _pdf_lines_for_page(title, body_lines, idx, page_count))
        set_object(
            page_id,
            (
                (
                    "<< /Type /Page /Parent %d 0 R /MediaBox [0 0 %d %d] "
                    "/Resources << /Font << /F1 %d 0 R /F2 %d 0 R >> >> "
                    "/Contents %d 0 R >>"
                )
                % (
                    pages_id,
                    PDF_PAGE_WIDTH,
                    PDF_PAGE_HEIGHT,
                    font_bold_id,
                    font_body_id,
                    content_id,
                )
            ).encode("ascii")
        )

    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    set_object(pages_id, f"<< /Type /Pages /Count {len(page_ids)} /Kids [{kids}] >>".encode("ascii"))
    set_object(catalog_id, f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("ascii"))

    pdf = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for object_id in range(1, len(objects) + 1):
        offsets.append(len(pdf))
        pdf.extend(f"{object_id} 0 obj\n".encode("ascii"))
        pdf.extend(objects[object_id])
        pdf.extend(b"\nendobj\n")

    xref_start = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_start}\n%%EOF\n"
        ).encode("ascii")
    )
    path.write_bytes(bytes(pdf))


def build_pdf_pages(
    rows: list[dict[str, Any]],
    office_record: dict[str, Any],
    source_meta: dict[str, Any],
    generated_at: str,
) -> list[tuple[str, list[str]]]:
    rows_by_zip = {row["zip_code"]: row for row in rows}
    benchmark_row = rows_by_zip[PRIMARY_BENCHMARK_ZIP]

    total_retail = sum(int(row["retail_pharmacy_count"] or 0) for row in rows)
    distances = [
        float(row["nearest_pharmacy_distance_miles"])
        for row in rows
        if row["nearest_pharmacy_distance_miles"] not in (None, "")
    ]
    office_issuers = ", ".join(office_record["issuer_names"][:3]) or "No issuer names surfaced in the sample route."

    page1: list[str] = []
    page1 += wrap_text(
        "This capability demo shows what the current HealthPorta data can already support for "
        "retail-pharmacy landscape analysis at ZIP level without building a new product surface."
    )
    page1.append("")
    page1 += wrap_text(
        f"Anchor: {OFFICE_NAME}, {OFFICE_ADDRESS}. Comparison set: {', '.join(COMPARISON_ZIPS)}."
    )
    page1 += wrap_text(
        f"Across the six-ZIP sample, the current data surfaces {total_retail} retail pharmacy rows "
        f"and nearest-pharmacy distances ranging from {min(distances):.2f} to {max(distances):.2f} miles."
    )
    page1 += wrap_text(
        f"ZIP {PRIMARY_BENCHMARK_ZIP} is used as the primary NYC benchmark because it has stronger resident and "
        f"health-context signal than office ZIP {OFFICE_ZIP}."
    )
    page1.append("")
    page1 += wrap_text("Questions this demo can answer today:")
    page1 += wrap_text("- Where retail pharmacies are concentrated at ZIP level.")
    page1 += wrap_text("- How far a user may travel to the nearest pharmacy from a ZIP centroid.")
    page1 += wrap_text("- Whether a sample pharmacy shows commercial network presence.")
    page1 += wrap_text("- Whether the sample pharmacy is Medicare Part D active.")
    page1 += wrap_text("- What the surrounding ZIP context looks like via Census and CDC PLACES.")
    page1.append("")
    page1 += wrap_text("Important framing:")
    page1 += wrap_text(
        "- This is a capability demonstration built from current live data, not a production recurring report."
    )
    page1 += wrap_text(
        "- Access bands are derived from nearest retail pharmacy distance because there is no first-class "
        "pharmacy access band endpoint today."
    )
    page1 += wrap_text("- Pricing and commercial terms are intentionally left out of the artifacts.")

    page2: list[str] = []
    page2 += wrap_text(f"Deloitte office anchor: {OFFICE_ADDRESS}")
    page2.append("")
    page2 += wrap_text(
        f"Nearest pharmacy sample: "
        f"{office_record['nearest'].get('do_business_as_text') or office_record['nearest'].get('provider_organization_name') or '-'} "
        f"(NPI {office_record['nearest_npi'] or '-'})"
    )
    page2 += wrap_text(
        f"Nearest-pharmacy distance: {format_number(office_record['nearest_distance'], 2)} miles"
    )
    page2 += wrap_text(f"Derived access band: {access_band_for_distance(office_record['nearest_distance'])}")
    page2 += wrap_text(
        f"Commercial network presence count (sample): {format_number(office_record['commercial_network_count'], 0)}"
    )
    page2 += wrap_text(
        f"Medicare Part D active sample flag: {format_number(office_record['medicare_payload'].get('medicare_active'))}"
    )
    page2 += wrap_text(f"Sample issuer names: {office_issuers}")
    page2.append("")
    page2 += wrap_text("Why this ZIP is useful and why it needs caveats:")
    page2 += wrap_text(
        "- The live data resolves a pharmacy at 30 Rockefeller Plaza, so the location feels concrete and real."
    )
    page2 += wrap_text(
        "- ZIP 10112 behaves like an office/business ZIP rather than a residential benchmark."
    )
    page2 += wrap_text(
        f"- Census total population is {format_number(office_record['census_profile'].get('total_population'), 0)}, "
        f"while business employment is {format_number(office_record['census_profile'].get('business_employment'), 0)}."
    )
    page2 += wrap_text(
        "- Residential Census fields are partially suppressed and CDC PLACES returns no ZIP health measures here."
    )
    page2.append("")
    page2 += wrap_text(
        f"ZIP {PRIMARY_BENCHMARK_ZIP} is the stronger Manhattan benchmark for the structured CSV and comparison tables."
    )
    page2 += wrap_text(
        f"ZIP {PRIMARY_BENCHMARK_ZIP} nearest pharmacy sample: {benchmark_row['nearest_pharmacy_name']} at "
        f"{format_number(benchmark_row['nearest_pharmacy_distance_miles'], 2)} miles."
    )

    page3: list[str] = []
    page3 += wrap_text("Structured comparison set: NYC benchmarks plus Chicago check")
    page3.append("")
    page3.append(
        "ZIP     City        Retail  Nearest(mi)  Access band               Medicare share  Top chain"
    )
    page3.append(
        "------  ---------- ------  -----------  ------------------------  --------------  ----------------"
    )
    for row in rows:
        chain = row["top_chain_1"] or "-"
        if row["top_chain_1_count"] not in ("", None):
            chain = f"{chain} ({row['top_chain_1_count']})"
        page3.append(
            f"{row['zip_code']:<6}  "
            f"{row['city'][:10]:<10} "
            f"{int(row['retail_pharmacy_count'] or 0):>6}  "
            f"{float(row['nearest_pharmacy_distance_miles'] or 0):>11.2f}  "
            f"{row['access_band'][:24]:<24}  "
            f"{float(row['active_medicare_share'] or 0):>14.4f}  "
            f"{chain[:16]:<16}"
        )
    page3.append("")
    page3 += wrap_text(
        f"Takeaway: ZIP {PRIMARY_BENCHMARK_ZIP} is the better NYC benchmark, while ZIP {CHICAGO_COMPARISON_ZIP} "
        "adds a second urban market so the story is not only Manhattan-centric."
    )

    page4: list[str] = []
    page4 += wrap_text("ZIP context using Census and CDC PLACES")
    page4.append("")
    page4.append(
        "ZIP     Pop       Income     SVI     Employers  Jobs      Uninsured  Diabetes  Smoking  Status"
    )
    page4.append(
        "------  --------  ---------  ------  ---------  --------  ---------  --------  -------  ---------"
    )
    for row in rows:
        status = f"{row['census_data_status']}/{row['places_data_status']}"
        page4.append(
            f"{row['zip_code']:<6}  "
            f"{format_number(row['census_total_population'], 0):>8}  "
            f"{format_number(row['census_median_household_income'], 0):>9}  "
            f"{format_number(row['census_svi_overall'], 4):>6}  "
            f"{format_number(row['census_total_employer_establishments'], 0):>9}  "
            f"{format_number(row['census_business_employment'], 0):>8}  "
            f"{format_number(row['places_access2_pct'], 1):>9}  "
            f"{format_number(row['places_diabetes_pct'], 1):>8}  "
            f"{format_number(row['places_csmoking_pct'], 1):>7}  "
            f"{status[:9]:<9}"
        )
    page4.append("")
    page4 += wrap_text(
        "Context use: these measures should frame what kind of ZIP each market is, not replace the pharmacy-access "
        f"story. ZIP {OFFICE_ZIP} remains the office anchor, ZIP {PRIMARY_BENCHMARK_ZIP} is the better NYC benchmark, "
        f"and ZIP {CHICAGO_COMPARISON_ZIP} gives a second city for comparison."
    )

    partd_status = source_meta["partd_status"]
    license_status = source_meta["license_status"]
    npi_status = source_meta["npi_status"]
    page5: list[str] = []
    page5 += wrap_text("Data sourcing and freshness")
    page5.append("")
    page5 += wrap_text("- NPPES pharmacy identity, address, taxonomy, and nearby search data.")
    page5 += wrap_text("- CMS Part D pharmacy activity and pharmacy-network-derived commercial evidence.")
    page5 += wrap_text("- U.S. Census API ZIP/ZCTA profile enrichment via local geo-census import.")
    page5 += wrap_text("- CDC PLACES ZCTA health measures via local places-zcta import.")
    page5.append("")
    page5 += wrap_text(f"Report generated: {generated_at}")
    page5 += wrap_text(
        f"Latest Part D import: {partd_status.get('status', 'unknown')} "
        f"(run {partd_status.get('run_id', '-')}, finished {partd_status.get('finished_at', '-')})"
    )
    page5 += wrap_text(
        f"NPI index status timestamp: {npi_status.get('date', '-')}"
    )
    page5 += wrap_text(
        f"Pharmacy-license import: {license_status.get('status', 'unknown')} "
        f"(run {license_status.get('run_id', '-')}, finished {license_status.get('finished_at', '-')})"
    )
    page5 += wrap_text(
        f"PLACES measure year is copied exactly from the API response per ZIP. In this sample, ZIP "
        f"{PRIMARY_BENCHMARK_ZIP}, ZIP 10019, and ZIP {CHICAGO_COMPARISON_ZIP} return year 2023."
    )
    page5.append("")
    page5 += wrap_text("Current caveats in this local environment:")
    page5 += wrap_text("- Pharmacy-license coverage is omitted from the client narrative because the latest local run produced zero normalized rows.")
    page5 += wrap_text("- Suppressed Census values are blanked rather than shown as raw sentinel numbers.")
    page5 += wrap_text("- CDC PLACES is not available for every ZIP, especially office/commercial ZIPs like 10112.")

    page6: list[str] = []
    page6 += wrap_text("What a production report could include")
    page6.append("")
    page6 += wrap_text("- Larger geography sets: county, city, state, or client-specific ZIP cohorts.")
    page6 += wrap_text("- Scheduled refreshes using the same live source families already in the platform.")
    page6 += wrap_text("- Chain concentration trends and nearest-pharmacy travel comparisons over time.")
    page6 += wrap_text("- Network-specific extracts keyed to selected issuers, plans, or pharmacy groups.")
    page6 += wrap_text("- Drug-level Part D cost and formulary overlays where medication examples matter.")
    page6 += wrap_text("- Customer-ready recurring PDF/CSV packages after the exact business rules are locked.")
    page6.append("")
    page6 += wrap_text(
        "Commercial pricing is intentionally excluded here. Treat this PDF and CSV as a proof of capability and "
        "use the pricing discussion as a separate sales follow-up."
    )

    return [
        ("Executive Summary", page1),
        ("Office Anchor", page2),
        ("ZIP Comparison", page3),
        ("ZIP Context", page4),
        ("Sources and Freshness", page5),
        ("Production Next Step", page6),
    ]


def generate_demo(output_dir: Path, base_url: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    office_record = fetch_zip_bundle(base_url, OFFICE_ZIP)
    records = [fetch_zip_bundle(base_url, zip_code) for zip_code in COMPARISON_ZIPS]
    rows = build_csv_rows(records)
    csv_path = output_dir / CSV_FILENAME
    write_csv(csv_path, rows)

    _, partd_status = fetch_json(base_url, "/formulary/partd/import/status")
    _, license_status = fetch_json(base_url, "/pharmacy-license/import/status")
    _, npi_status = fetch_json(base_url, "/npi/")

    pages = build_pdf_pages(
        rows,
        office_record=office_record,
        source_meta={
            "partd_status": partd_status,
            "license_status": license_status,
            "npi_status": npi_status,
        },
        generated_at=generated_at,
    )
    pdf_path = output_dir / PDF_FILENAME
    render_pdf(pdf_path, pages)
    return csv_path, pdf_path


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("HP_DEMO_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--output-dir", default=str(repo_root / "reports"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    csv_path, pdf_path = generate_demo(Path(args.output_dir), args.base_url)
    print(f"CSV: {csv_path}")
    print(f"PDF: {pdf_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
