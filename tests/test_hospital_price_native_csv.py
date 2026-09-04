# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from tests.test_hospital_price_native import native


def test_format_detection_skips_blank_structural_records_beyond_sniff(tmp_path):
    source = tmp_path / "input.csv"
    source.write_bytes(
        b"\n" * 4096
        + b"hospital_name,last_updated_on,version\n\n,,\n"
        b"Example,2026-08-25,3.0.0\n  , , \ndescription,payer_name\n"
    )

    assert native.detect_hospital_mrf_format(source) == "csv-tall"


def test_format_detection_skips_cp1252_nbsp_structural_record(tmp_path):
    source = tmp_path / "input.csv"
    source.write_bytes(
        b"hospital_name,last_updated_on,version\n\xa0,\xa0,\xa0\n"
        b"Example,2026-08-25,3.0.0\ndescription,payer_name\n"
    )

    assert native.detect_hospital_mrf_format(source) == "csv-tall"


def test_format_detection_scans_a_bounded_metadata_preamble(tmp_path):
    source = tmp_path / "input.csv"
    payload = (
        b"***** END NOTES,,,\n"
        b"hospital_name,last_updated_on,version\n"
        b"Example,2026-08-25,3.0.0\n"
        b"description,payer_name\n"
    )
    source.write_bytes(payload)

    assert native.detect_hospital_mrf_format(source) == "csv-tall"

    source.write_bytes(
        b"note\n" * native.HOSPITAL_MRF_CSV_HEADER_SCAN_MAX_RECORDS
        + payload
    )
    with pytest.raises(ValueError, match="scan limit"):
        native.detect_hospital_mrf_format(source)
