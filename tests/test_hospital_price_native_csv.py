# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from tests.test_hospital_price_native import native


def test_format_detection_skips_blank_structural_records_beyond_sniff(tmp_path):
    source = tmp_path / "input.csv"
    source.write_bytes(
        b"\n" * 4096
        + b"hospital_name,version\n\n,\nExample,3.0.0\n  , \ndescription,payer_name\n"
    )

    assert native.detect_hospital_mrf_format(source) == "csv-tall"


def test_format_detection_skips_cp1252_nbsp_structural_record(tmp_path):
    source = tmp_path / "input.csv"
    source.write_bytes(
        b"hospital_name,version\n\xa0,\xa0\nExample,3.0.0\ndescription,payer_name\n"
    )

    assert native.detect_hospital_mrf_format(source) == "csv-tall"
