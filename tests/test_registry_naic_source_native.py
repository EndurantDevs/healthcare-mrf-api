# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The compiled listing export returns complete native COPY and exact metadata."""

import inspect
import json
import struct

import pytest

from process.registry_naic_source import LAYOUTS_BY_MEMBER, MEMBERS, PARSER_VERSION, _canonical
from tests.registry_naic_fixtures import prepared_listings

native = pytest.importorskip("ptg2_address_canon")


@pytest.mark.parametrize("member_name", MEMBERS)
def test_compiled_listing_copy_preserves_native_observation_and_edition(tmp_path, member_name):
    prepared = prepared_listings(tmp_path)
    member = next(member for member in prepared.members if member.member_name == member_name)
    metadata = {
        "snapshot_id": str(prepared.edition.snapshot_id),
        "edition_id": prepared.edition.edition_id,
        "reporting_year": 2026,
        "artifact_sha256": prepared.edition.artifact_sha256,
        "member": member_name,
        "member_sha256": member.input_sha256,
        "layout": LAYOUTS_BY_MEMBER[member_name],
        "layout_pdf_sha256": member.layout_pdf_sha256,
        "header_sha256": member.header_sha256,
        "parser_version": PARSER_VERSION,
    }
    encoder = getattr(native, "encode_naic_company_registry_observations", None)
    assert inspect.isbuiltin(encoder), "compiled company listing COPY export is required"
    member_bytes = member.input_path.read_bytes()
    frame, row_count, encoded = encoder(member_bytes, _canonical(metadata))
    assert row_count == 1 and frame.endswith(b"\xff\xff")
    metadata_by_field = json.loads(encoded)
    assert metadata_by_field["edition"] == metadata and metadata_by_field["counts"]["input_rows"] == 1
    assert struct.unpack_from("!h", frame, 19)[0] == 6
    offset, fields = 21, []
    for _ in range(6):
        length = struct.unpack_from("!i", frame, offset)[0]
        offset += 4
        fields.append(frame[offset : offset + length])
        offset += length
    observation = json.loads(fields[4][1:])
    assert fields[1].decode() == f"{member_name}:row:2"
    assert observation["source_evidence"]["member_sha256"] == member.input_sha256
    assert observation["valid_from"] is None and observation["valid_to"] is None
    with pytest.raises(ValueError, match="digest_mismatch"):
        encoder(member_bytes + b"x", _canonical(metadata))
