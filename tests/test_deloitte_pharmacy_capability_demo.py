from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "support" / "deloitte_pharmacy_capability_demo.py"
SPEC = importlib.util.spec_from_file_location("deloitte_demo", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def test_access_band_for_distance_thresholds():
    assert MODULE.access_band_for_distance(0.02) == "Walkable (<=0.10 mi)"
    assert MODULE.access_band_for_distance(0.25) == "Very close (0.10-0.25 mi)"
    assert MODULE.access_band_for_distance(0.40) == "Short trip (0.25-0.50 mi)"
    assert MODULE.access_band_for_distance(0.75) == "Local trip (0.50-1.00 mi)"
    assert MODULE.access_band_for_distance(1.25) == "Extended trip (>1.00 mi)"


def test_clean_network_count_ignores_zero_and_duplicates():
    entries = [
        {"npi_info": {"checksum_network": 123}},
        {"npi_info": {"checksum_network": 123}},
        {"npi_info": {"checksum_network": 0}},
        {"npi_info": {"checksum_network": None}},
        {"npi_info": {"checksum_network": -7}},
    ]
    assert MODULE.clean_network_count(entries) == 2


def test_normalize_census_profile_marks_suppressed_values():
    profile, status = MODULE.normalize_census_profile(
        {
            "total_population": 5,
            "median_household_income": -666666666.0,
            "svi_overall": -999.0,
            "business_employment": 22516,
        }
    )
    assert status == "suppressed"
    assert profile["total_population"] == 5
    assert profile["median_household_income"] is None
    assert profile["svi_overall"] is None
    assert profile["business_employment"] == 22516


def test_derive_row_notes_for_office_zip():
    notes = MODULE.derive_row_notes("10112", "suppressed", "not_found", 5)
    assert "Office/business ZIP" in notes
    assert "suppressed" in notes.lower()
    assert "CDC PLACES" in notes


def test_derive_row_notes_for_benchmark_and_chicago_zips():
    benchmark_notes = MODULE.derive_row_notes("10001", "available", "available", 1000)
    chicago_notes = MODULE.derive_row_notes("60654", "available", "available", 1000)
    assert "Primary NYC benchmark ZIP" in benchmark_notes
    assert "Chicago comparison ZIP" in chicago_notes


def test_comparison_zip_set_uses_10001_and_60654_not_10112():
    assert MODULE.PRIMARY_BENCHMARK_ZIP == "10001"
    assert MODULE.CHICAGO_COMPARISON_ZIP == "60654"
    assert "10001" in MODULE.COMPARISON_ZIPS
    assert "60654" in MODULE.COMPARISON_ZIPS
    assert "10112" not in MODULE.COMPARISON_ZIPS


def test_pdf_layout_constants_use_landscape_letter():
    assert MODULE.PDF_PAGE_WIDTH == 792
    assert MODULE.PDF_PAGE_HEIGHT == 612
    assert MODULE.PDF_PAGE_WIDTH > MODULE.PDF_PAGE_HEIGHT
