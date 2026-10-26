# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import zipfile

import pylightxl as xl
import pytest

from process.mrf_workbook import read_mrf_workbook


@pytest.mark.parametrize("constant", [None, '"synthetic-identifier"', "123"])
def test_workbook_preserves_cells_and_ranges(tmp_path, constant):
    original = tmp_path / "original.xlsx"
    workbook = xl.Database()
    workbook.add_ws(ws="Transparency")
    workbook.ws("Transparency").update_index(row=1, col=1, val="Issuer_ID")
    workbook.ws("Transparency").update_index(row=2, col=1, val=12345)
    xl.writexl(workbook, str(original))
    source = tmp_path / "source.xlsx"
    with zipfile.ZipFile(original) as incoming, zipfile.ZipFile(source, "w") as outgoing:
        for member in incoming.infolist():
            content = incoming.read(member)
            if member.filename == "xl/workbook.xml":
                names = '<definedNames><definedName name="Issuer">Transparency!$A$2</definedName>'
                if constant is not None:
                    names += f'<definedName name="Metadata">{constant}</definedName>'
                names += "</definedNames>"
                assert b"</workbook>" in content
                content = content.replace(
                    b"<workbook ", b'<workbook xmlns:vendor="urn:example:metadata" vendor:version="1" '
                )
                content = content.replace(b"</workbook>", names.encode() + b"</workbook>")
            outgoing.writestr(member, content)
    original_bytes = source.read_bytes()

    parsed = read_mrf_workbook(str(source))

    assert list(parsed.ws("Transparency").rows) == [["Issuer_ID"], [12345]]
    assert parsed.nr("Issuer") == [[12345]]
    assert source.read_bytes() == original_bytes


def test_workbook_preserves_unrelated_reader_errors(tmp_path):
    with pytest.raises(UserWarning, match="does not exit"):
        read_mrf_workbook(str(tmp_path / "missing.xlsx"))
