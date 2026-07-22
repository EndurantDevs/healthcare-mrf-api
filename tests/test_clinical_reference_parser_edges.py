# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import gzip
import importlib
import zipfile
from pathlib import Path

clinical_reference = importlib.import_module("process.clinical_reference")


def _write_zip(path: Path, entries: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, value in entries.items():
            archive.writestr(name, value)
    return path


def _rxnorm_concept_row(
    rxcui: str,
    term: str,
    *,
    lat: str = "ENG",
    sab: str = "RXNORM",
    tty: str = "IN",
    suppress: str = "N",
) -> str:
    fields = [""] * 18
    fields[0] = rxcui
    fields[1] = lat
    fields[11] = sab
    fields[12] = tty
    fields[14] = term
    fields[16] = suppress
    return "|".join(fields) + "\n"


def _rxnorm_relationship_row(
    source: str,
    target: str,
    *,
    relationship: str = "RO",
    detailed_relationship: str = "",
    suppress: str = "N",
) -> str:
    fields = [""] * 11
    fields[0] = source
    fields[3] = relationship
    fields[4] = target
    fields[7] = detailed_relationship
    fields[10] = suppress
    return "|".join(fields) + "\n"


_MESH_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MeshRecords>
  <IgnoredRecord><Value>ignored</Value></IgnoredRecord>
  <DescriptorRecord>
    <DescriptorUI>D0001</DescriptorUI>
    <DescriptorName><String>Disease One</String></DescriptorName>
    <TreeNumberList><TreeNumber>C01.100</TreeNumber><TreeNumber>D01.200</TreeNumber></TreeNumberList>
    <ConceptList><Concept><TermList><Term><String>Disease One</String></Term><Term><String>First disease</String></Term></TermList></Concept></ConceptList>
  </DescriptorRecord>
  <DescriptorRecord><DescriptorUI>D-BAD</DescriptorUI><DescriptorName><String /></DescriptorName></DescriptorRecord>
  <SupplementalRecord>
    <SupplementalRecordUI>S0001</SupplementalRecordUI>
    <SupplementalRecordName><String>Supplement One</String></SupplementalRecordName>
    <ConceptList><Concept><TermList><Term><String>Supplement alias</String></Term></TermList></Concept></ConceptList>
  </SupplementalRecord>
  <QualifierRecord>
    <QualifierUI>Q0001</QualifierUI><QualifierName><String>Qualifier One</String></QualifierName>
    <ConceptList><Concept><TermList><Term><String>Qualifier alias</String></Term></TermList></Concept></ConceptList>
  </QualifierRecord>
</MeshRecords>
"""


def _write_rxnorm_main_archive(path: Path) -> Path:
    concept_text = "".join(
        [
            "too|short\n",
            _rxnorm_concept_row("bad-language", "French", lat="FRE"),
            _rxnorm_concept_row("bad-source", "Other source", sab="MTHSPL"),
            _rxnorm_concept_row("bad-suppress", "Suppressed", suppress="Y"),
            _rxnorm_concept_row("", "Missing identifier"),
            _rxnorm_concept_row("100", "Branded drug", tty="SBD"),
            _rxnorm_concept_row("100", "Ingredient drug", tty="IN"),
            _rxnorm_concept_row("100", "Ingredient drug", tty="IN"),
            _rxnorm_concept_row("200", "Second drug", tty="ZZZ"),
        ]
    )
    relationship_text = "".join(
        [
            "too|short\n",
            _rxnorm_relationship_row("100", "200", suppress="Y"),
            _rxnorm_relationship_row("", "200"),
            _rxnorm_relationship_row(
                "100", "200", detailed_relationship="has_ingredient"
            ),
            _rxnorm_relationship_row("200", "100"),
        ]
    )
    return _write_zip(
        path,
        {
            "rrf/RXNCONSO.RRF": concept_text,
            "rrf/RXNREL.RRF": relationship_text,
        },
    )


def _write_snomed_main_archive(path: Path) -> Path:
    fsn_type_id = clinical_reference.SNOMED_FSN_TYPE_ID
    return _write_zip(
        path,
        {
            "SnomedCT_US/Snapshot/Terminology/sct2_Concept_Snapshot_INT.txt": (
                "id\tactive\ninactive\t0\n100\t1\n200\t1\n300\t1\n"
            ),
            "SnomedCT_US/Snapshot/Terminology/sct2_Description_Snapshot-en_INT.txt": (
                "active\tconceptId\tterm\ttypeId\n"
                "0\t100\tInactive term\t900000000000013009\n"
                "1\tunknown\tUnknown term\t900000000000013009\n"
                f"1\t100\tDiabetes mellitus (disorder)\t{fsn_type_id}\n"
                "1\t100\tDiabetes\t900000000000013009\n"
                "1\t100\tDiabetes\t900000000000013009\n"
                f"1\t200\tAppendectomy (procedure)\t{fsn_type_id}\n"
            ),
            "SnomedCT_US/Snapshot/Terminology/sct2_Relationship_Snapshot_INT.txt": (
                "active\tsourceId\tdestinationId\ttypeId\n"
                "0\t100\t200\tinactive\n"
                "1\tunknown\t200\tunknown-source\n"
                "1\t100\t200\tis-a\n"
                "1\t200\t100\trelated-to\n"
            ),
        },
    )


def test_parse_icd10cm_keeps_valid_codes_and_compact_crosswalks(tmp_path):
    archive_path = _write_zip(
        tmp_path / "icd.zip",
        {
            "icd10cm_order_2026.txt": "ignored order file\n",
            "icd10cm_codes_2026.txt": (
                "\n"
                "INVALID_ONLY\n"
                "A00 Cholera\n"
                "B01.2   Varicella   pneumonia\n"
                "C03 Third row is outside the test limit\n"
            ),
        },
    )

    concept_rows, synonym_rows, crosswalk_rows = clinical_reference._parse_icd10cm(
        archive_path,
        test_limit=2,
    )

    assert [
        (concept_by_field["code"], concept_by_field["display_name"])
        for concept_by_field in concept_rows
    ] == [
        ("A00", "Cholera"),
        ("B01.2", "Varicella pneumonia"),
    ]
    assert [synonym_by_field["code"] for synonym_by_field in synonym_rows] == [
        "A00",
        "B01.2",
    ]
    assert [
        (crosswalk_by_field["from_code"], crosswalk_by_field["to_code"])
        for crosswalk_by_field in crosswalk_rows
    ] == [("B012", "B01.2")]

    empty_archive = _write_zip(
        tmp_path / "icd-empty.zip",
        {"icd10cm_codes_2026.txt": ""},
    )
    assert clinical_reference._parse_icd10cm(empty_archive) == ([], [], [])


def test_parse_mesh_handles_all_record_types_and_both_file_encodings(tmp_path):
    xml_path = tmp_path / "mesh.xml"
    xml_path.write_text(_MESH_XML, encoding="utf-8")
    gzip_path = tmp_path / "mesh.xml.gz"
    with gzip.open(gzip_path, "wb") as compressed:
        compressed.write(_MESH_XML.encode("utf-8"))

    concept_rows, synonym_rows, relationship_rows = clinical_reference._parse_mesh_file(
        xml_path,
        "mesh_plain",
    )
    limited_concept_rows, _limited_synonyms, _limited_relationships = (
        clinical_reference._parse_mesh_file(
            gzip_path,
            "mesh_gzip",
            test_limit=2,
        )
    )

    assert [
        (concept_by_field["code"], concept_by_field["code_type"])
        for concept_by_field in concept_rows
    ] == [
        ("D0001", "condition"),
        ("S0001", "concept"),
        ("Q0001", "qualifier"),
    ]
    assert {synonym_by_field["synonym"] for synonym_by_field in synonym_rows} == {
        "Disease One",
        "First disease",
        "Supplement One",
        "Supplement alias",
        "Qualifier One",
        "Qualifier alias",
    }
    assert {
        (relationship_by_field["from_code"], relationship_by_field["to_code"])
        for relationship_by_field in relationship_rows
    } == {("D0001", "C01.100"), ("D0001", "D01.200")}
    assert [
        concept_by_field["code"] for concept_by_field in limited_concept_rows
    ] == ["D0001", "S0001"]


def test_parse_rxnorm_filters_atoms_deduplicates_and_loads_relationships(tmp_path):
    archive_path = _write_rxnorm_main_archive(tmp_path / "rxnorm.zip")

    concept_rows, synonym_rows, relationship_rows = clinical_reference._parse_rxnorm(
        archive_path,
        test_limit=2,
    )

    assert [
        (concept_by_field["code"], concept_by_field["display_name"])
        for concept_by_field in concept_rows
    ] == [
        ("100", "Ingredient drug"),
        ("200", "Second drug"),
    ]
    assert {
        (
            synonym_by_field["code"],
            synonym_by_field["synonym"],
            synonym_by_field["term_type"],
        )
        for synonym_by_field in synonym_rows
    } == {
        ("100", "Branded drug", "SBD"),
        ("100", "Ingredient drug", "IN"),
        ("200", "Second drug", "ZZZ"),
    }
    assert [
        relationship_by_field["relationship"]
        for relationship_by_field in relationship_rows
    ] == [
        "has_ingredient",
        "RO",
    ]

    archive_without_relationships = _write_zip(
        tmp_path / "rxnorm-no-rel.zip",
        {"RXNCONSO.RRF": _rxnorm_concept_row("300", "Third drug", tty="")},
    )
    no_rel_concept_rows, no_rel_synonym_rows, no_rel_relationships = (
        clinical_reference._parse_rxnorm(archive_without_relationships)
    )
    assert [
        concept_by_field["code"] for concept_by_field in no_rel_concept_rows
    ] == ["300"]
    assert no_rel_synonym_rows[0]["term_type"] == "atom"
    assert no_rel_relationships == []


def test_parse_snomed_filters_snapshot_rows_and_uses_fsn_types(tmp_path):
    concept_header = "id\tactive\n"
    description_header = "active\tconceptId\tterm\ttypeId\n"
    archive_path = _write_snomed_main_archive(tmp_path / "snomed.zip")

    concept_rows, synonym_rows, relationship_rows = clinical_reference._parse_snomed(
        archive_path,
        test_limit=2,
    )

    assert {
        (
            concept_by_field["code"],
            concept_by_field["display_name"],
            concept_by_field["code_type"],
        )
        for concept_by_field in concept_rows
    } == {
        ("100", "Diabetes mellitus", "condition"),
        ("200", "Appendectomy", "treatment"),
    }
    assert len(synonym_rows) == 3
    assert [
        (relationship_by_field["from_code"], relationship_by_field["to_code"])
        for relationship_by_field in relationship_rows
    ] == [
        ("100", "200"),
        ("200", "100"),
    ]

    archive_without_relationships = _write_zip(
        tmp_path / "snomed-no-rel.zip",
        {
            "SnomedCT_US/Snapshot/sct2_Concept_Snapshot_INT.txt": (
                concept_header + "400\t1\n"
            ),
            "SnomedCT_US/Snapshot/sct2_Description_Snapshot-en_INT.txt": (
                description_header
                + "1\t400\tPlain concept\t900000000000013009\n"
            ),
        },
    )
    fallback_concept_rows, _fallback_synonyms, fallback_relationships = (
        clinical_reference._parse_snomed(archive_without_relationships)
    )
    assert [
        (
            concept_by_field["code"],
            concept_by_field["display_name"],
            concept_by_field["code_type"],
        )
        for concept_by_field in fallback_concept_rows
    ] == [("400", "400", "concept")]
    assert fallback_relationships == []


def test_parse_snomed_icd_map_handles_empty_invalid_and_lowercase_headers(tmp_path):
    no_map_archive = _write_zip(
        tmp_path / "no-map.zip",
        {"Snapshot/readme.txt": "not a map\n"},
    )
    assert clinical_reference._parse_snomed_icd_map(no_map_archive) == []

    empty_map_archive = _write_zip(
        tmp_path / "empty-map.zip",
        {"Snapshot/extendedMap.tsv": "active\treferencedComponentId\tmapTarget\n"},
    )
    assert clinical_reference._parse_snomed_icd_map(empty_map_archive) == []

    map_archive = _write_zip(
        tmp_path / "map.zip",
        {
            "Snapshot/extendedMap.tsv": (
                "active\treferencedcomponentid\tmaptarget\n"
                "0\t100\tA00\n"
                "1\t\tB00\n"
                "1\t200\tinvalid\n"
                "1\t300\tC01.2\n"
                "1\t400\tD02\n"
            )
        },
    )
    crosswalk_rows = clinical_reference._parse_snomed_icd_map(
        map_archive,
        test_limit=1,
    )
    assert [
        (crosswalk_by_field["from_code"], crosswalk_by_field["to_code"])
        for crosswalk_by_field in crosswalk_rows
    ] == [
        ("300", "C01.2")
    ]
