# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read MRF workbook cells when Excel also defines non-cell named constants."""

import shutil
import tempfile
import zipfile
from pathlib import Path
from xml.dom import Node, minidom

import pylightxl as xl


def read_mrf_workbook(filename):
    """Ignore unsupported named constants without changing the source or cells."""
    try:
        return xl.readxl(filename)
    except UserWarning as error:
        if "NamedRange does not contain sheet reference" not in str(error):
            raise
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    with tempfile.TemporaryDirectory() as directory, zipfile.ZipFile(filename) as source:
        raw_xml = source.read("xl/workbook.xml")
        workbook = minidom.parseString(raw_xml)
        names = workbook.getElementsByTagNameNS(namespace, "definedName")
        if not names:
            raise ValueError("Workbook named-range metadata is missing")
        for name in names:
            value = "".join(
                child.data for child in name.childNodes if child.nodeType in (Node.TEXT_NODE, Node.CDATA_SECTION_NODE)
            )
            if "!" not in value:
                name.parentNode.removeChild(name)
        normalized = Path(directory) / "workbook.xlsx"
        with zipfile.ZipFile(normalized, "w") as target:
            for member in source.infolist():
                if member.filename == "xl/workbook.xml":
                    target.writestr(member, workbook.toxml(encoding="utf-8"))
                    continue
                with source.open(member) as incoming, target.open(member, "w") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
        return xl.readxl(str(normalized))
