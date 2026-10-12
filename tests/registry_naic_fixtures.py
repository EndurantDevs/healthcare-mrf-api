# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic byte members shared by listing admission contract checks."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

from process.registry_naic_source import (
    HEADERS_BY_MEMBER,
    MEMBERS,
    PARSER_VERSION,
    RegistryNaicMemberInput,
    RegistryNaicPreparedInput,
    _canonical,
)
from process.registry_source_admission import RegistrySourceEdition


def listing_row(**changes):
    return {
        "COMPANY NAME": "Synthetic Company",
        "GROUP NAME": "Synthetic Group",
        "STATE OF DOMICILE": "CA",
        "GROUP CODE": "00707",
        "COMPANY CODE": "707",
        "FEIN NUMBER": "01-2345678",
        "COMPANY STATUS": "1",
        **changes,
    }


def prepared_listings(root, *, rows_by_member=None, edition_id="synthetic-2026"):
    rows_by_member = rows_by_member or {name: [listing_row()] for name in MEMBERS}
    members = []
    for name in MEMBERS:
        headers = HEADERS_BY_MEMBER[name]
        record_bytes = [b",".join(header.encode("ascii") for header in headers)]
        for source_row in rows_by_member[name]:
            record_bytes.append(
                b",".join(
                    field_value if isinstance(field_value, bytes) else field_value.encode("ascii")
                    for field_value in (source_row.get(header, "") for header in headers)
                )
            )
        member_bytes = b"\r\n".join(record_bytes) + b"\r\n"
        path = Path(root) / name
        path.write_bytes(member_bytes)
        members.append(
            RegistryNaicMemberInput(
                name,
                path,
                hashlib.sha256(member_bytes).hexdigest(),
                len(member_bytes),
                hashlib.sha256(json.dumps(headers, separators=(",", ":")).encode()).hexdigest(),
                "b" * 64,
            )
        )
    member_witnesses = [member.evidence() for member in members]
    edition = RegistrySourceEdition(
        uuid4(),
        "naic",
        "company-listings",
        edition_id,
        "https://example.test/source.zip",
        "a" * 64,
        hashlib.sha256(_canonical(member_witnesses)).hexdigest(),
        PARSER_VERSION,
        2026,
        None,
    )
    return RegistryNaicPreparedInput(edition, tuple(members))


def write_manifest(path, prepared):
    members = [
        {
            "member_name": member.member_name,
            "input_path": str(member.input_path),
            "input_sha256": member.input_sha256,
            "input_bytes": member.input_bytes,
            "header_sha256": member.header_sha256,
            "layout_pdf_sha256": member.layout_pdf_sha256,
        }
        for member in prepared.members
    ]
    Path(path).write_text(json.dumps({"members": members}))
    return path
