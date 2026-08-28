# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import io
import zipfile

import pytest

from process import hospital_price_native as native


def _tall_csv() -> bytes:
    return (
        b"hospital_name,version\nExample,3.0.0\n"
        b"description,payer_name\n"
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_matching_appledouble_member_is_ignored(tmp_path, reverse):
    members = [
        ("prices.mrf", _tall_csv()),
        ("__MACOSX/._prices.mrf", b"AppleDouble metadata"),
    ]
    if reverse:
        members.reverse()
    archive_path = tmp_path / "appledouble.zip"
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members:
            archive.writestr(name, payload)

    assert native.detect_hospital_mrf_format(archive_path) == "csv-tall"


def test_unpaired_and_unsupported_zip_layouts_are_rejected(tmp_path):
    cases_by_name = {
        "appledouble-only": [("__MACOSX/._prices.json", b"metadata")],
        "mismatched-appledouble": [
            ("prices.json", b"{}"),
            ("__MACOSX/._other.json", b"metadata"),
        ],
        "arbitrary-extra": [
            ("prices.json", b"{}"),
            ("__MACOSX/._prices.json", b"metadata"),
            ("README.txt", b"metadata"),
        ],
        "ooxml": [
            ("[Content_Types].xml", b"<Types/>"),
            ("xl/workbook.xml", b"<workbook/>"),
        ],
    }
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as archive:
        archive.writestr("prices.json", b"{}")
    cases_by_name["nested"] = [("inner.zip", inner.getvalue())]

    for name, members in cases_by_name.items():
        archive_path = tmp_path / f"{name}.zip"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
            for member_name, payload in members:
                archive.writestr(member_name, payload)
        with pytest.raises(ValueError):
            native.detect_hospital_mrf_format(archive_path)
