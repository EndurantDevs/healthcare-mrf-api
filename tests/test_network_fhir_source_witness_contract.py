# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source declarations are closed and remain distinct from original Reader captures."""

import hashlib
from dataclasses import FrozenInstanceError, asdict, make_dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import network_fhir_retained_capture_contract as reader
from process import network_fhir_source_witness_contract as witness

CAPTURE_ID = "00000000-0000-0000-0000-000000000001"


def origin_fields():
    return {
        "schema_name": "source_example",
        "source_id": "source-one",
        "endpoint_id": "endpoint-one",
        "dataset_id": "dataset-one",
        "dataset_sha256": "a" * 64,
        "root_run_id": "run-one",
        "header_oid": 10,
        "resource_parent_oid": 11,
        "resource_leaf_oid": 12,
        "metadata_bytes": 100,
        "metadata_text_sha256": "b" * 64,
        "summary_sha256": "c" * 64,
        "proof_sha256": "d" * 64,
        "resource_counts": (("Location", 1), ("Organization", 1), ("Practitioner", 0)),
        "resource_hashes": (("Location", "e" * 64), ("Organization", "f" * 64), ("Practitioner", "0" * 64)),
        "descriptor_counts": (("Location", 1), ("Organization", 1), ("Practitioner", 0)),
        "admission_profile": reader.SEMANTIC_PROFILE,
        "admission_metadata_sha256": "1" * 64,
        "semantic_projection_as_of": "2026-01-01",
        "unresolved_plan_network_count": 0,
        "source_uri": "https://directory.example.test/fhir",
        "source_aliases": ("source-one", "source-two"),
        "native_catalog_sha256": "2" * 64,
        "database_oid": 14,
        "database_system_identifier": "123456789",
    }


def database_fields():
    return {
        "database_name": "source_example",
        "database_oid": 14,
        "system_identifier": "123456789",
        "postmaster_started": datetime(2026, 1, 1, 2, 3, 4, 5, tzinfo=UTC),
        "recovering": False,
    }


def captured():
    capture = witness.SourceWitnessCapture(
        witness.source_origin(origin_fields()),
        witness.database_identity(database_fields()),
        CAPTURE_ID,
        witness.SCHEMA_PREFIX + UUID(CAPTURE_ID).hex,
        20,
        tuple((name, index + 30) for index, name in enumerate(sorted(witness.TABLES))),
        "owner_example",
        ("reader_example",),
        "2026-01-02T00:00:00.000000+00:00",
        "3" * 64,
        "4" * 64,
        "0" * 64,
    )
    return replace(capture, capture_sha256=witness.capture_digest(capture))


def reader_capture():
    origin = reader.RetainedFHIRInput(
        "source_example",
        "source-one",
        "endpoint-one",
        "dataset-one",
        "a" * 64,
        "run-one",
        10,
        11,
        12,
        "reader_example",
        100,
        "b" * 64,
        "c" * 64,
        "d" * 64,
        (("Location", 1), ("Organization", 1)),
        (("Location", "e" * 64), ("Organization", "f" * 64)),
        (("Location", 1), ("Organization", 2)),
        reader.REFUSAL,
    )
    capture = reader.RetainedFHIRCapture(
        origin,
        CAPTURE_ID,
        "registry_fhir_capture_" + UUID(CAPTURE_ID).hex,
        20,
        tuple((name, index + 30) for index, name in enumerate(sorted(reader.TABLES))),
        "owner_example",
        ("reader_example",),
        "1" * 64,
        "2" * 64,
        "0" * 64,
    )
    return replace(capture, capture_sha256=reader.capture_digest(capture))


def test_source_origin_has_exact_source_fields_without_reader_authority():
    fields = origin_fields()
    declaration = make_dataclass("SourceDeclaration", [(name, object) for name in fields], frozen=True)(**fields)
    origin = witness.source_origin(declaration)
    assert len(asdict(origin)) == 25 and asdict(origin) == fields
    assert witness.source_origin(origin) is origin
    assert witness.source_origin(captured().as_dict()["origin"]) == origin
    assert hash(origin)
    with pytest.raises(FrozenInstanceError):
        origin.dataset_id = "changed"


@pytest.mark.parametrize("field", tuple(origin_fields()))
def test_missing_origin_field_refuses_whole_declaration(field):
    fields = origin_fields()
    fields.pop(field)
    with pytest.raises(ValueError):
        witness.source_origin(fields)


@pytest.mark.parametrize("field", ["reader_role", "preparation_schema", "original_admission_refusal", "complete"])
def test_extra_origin_field_cannot_borrow_reader_or_completion_authority(field):
    with pytest.raises(ValueError):
        witness.source_origin(origin_fields() | {field: "untrusted"})


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_name": "invalid.schema"},
        {"source_id": "cms-npd"},
        {"source_id": " source-one"},
        {"header_oid": True},
        {"resource_leaf_oid": 2**32},
        {"database_oid": 0},
        {"dataset_sha256": "A" * 64},
        {"native_catalog_sha256": "bad"},
        {"metadata_bytes": True},
        {"metadata_bytes": 64 * 1024**2 - 32768},
        {"database_system_identifier": "01"},
        {"database_system_identifier": str(2**64)},
        {"database_system_identifier": "１２"},
        {"source_aliases": ("source-two",)},
        {"source_aliases": ("source-two", "source-one")},
        {"source_aliases": ("source-one", "source-one")},
        {"resource_counts": ()},
        {"resource_counts": (("Location", 0),)},
        {"resource_counts": (("Location", True),)},
        {"resource_hashes": (("Location", "e" * 64),)},
        {"descriptor_counts": (("Location", 2), ("Organization", 1), ("Practitioner", 0))},
        {"admission_profile": reader.LEGACY_PROFILE},
        {"admission_metadata_sha256": None},
        {"semantic_projection_as_of": "2026-1-1"},
        {"semantic_projection_as_of": None},
        {"unresolved_plan_network_count": True},
        {"unresolved_plan_network_count": -1},
    ],
)
def test_invalid_source_origin_refuses(changes):
    with pytest.raises(ValueError):
        witness.source_origin(origin_fields() | changes)


@pytest.mark.parametrize("uri", ["http://example.test/fhir", "https://example.test/fhir", "https://example.test/fhir/"])
def test_valid_registered_uri_spelling_is_preserved(uri):
    assert witness.source_origin(origin_fields() | {"source_uri": uri}).source_uri == uri


@pytest.mark.parametrize(
    "uri",
    [
        None,
        "",
        "ftp://example.test/fhir",
        "https://u:p@example.test/fhir",
        "https://example.test/fhir?q=1",
        "https://example.test/fhir#x",
        " https://example.test/fhir",
        "https://example.test/fhir ",
    ],
)
def test_invalid_source_uri_refuses(uri):
    with pytest.raises(ValueError):
        witness.source_origin(origin_fields() | {"source_uri": uri})


def test_database_identity_has_one_canonical_utc_timestamp_and_is_hashable():
    fields = database_fields()
    fields["postmaster_started"] = datetime(2026, 1, 1, 3, 3, 4, 5, tzinfo=timezone(timedelta(hours=1)))
    identity = witness.database_identity(fields)
    assert identity.postmaster_started == "2026-01-01T02:03:04.000005+00:00"
    assert witness.database_identity(asdict(identity)) == identity
    assert witness.database_identity(identity) is identity and hash(identity)


@pytest.mark.parametrize("field", tuple(database_fields()))
def test_database_identity_missing_fields_refuse(field):
    fields = database_fields()
    fields.pop(field)
    with pytest.raises(ValueError):
        witness.database_identity(fields)


@pytest.mark.parametrize(
    "changes",
    [
        {"recovering": True},
        {"recovering": 0},
        {"database_oid": True},
        {"database_oid": 2**32},
        {"system_identifier": "0"},
        {"database_name": ""},
        {"untrusted": True},
        {"postmaster_started": datetime(2026, 1, 1)},
        {"postmaster_started": "2026-01-01T00:00:00Z"},
        {"postmaster_started": "2026-01-01T00:00:00+00:00"},
        {"postmaster_started": "2026-01-01T01:00:00.000000+01:00"},
    ],
)
def test_database_identity_invalid_fields_refuse(changes):
    with pytest.raises(ValueError):
        witness.database_identity(database_fields() | changes)


def test_capture_keeps_source_database_and_target_oids_separate():
    capture = captured()
    assert witness.validate_capture(capture) is capture
    assert witness.validate_capture(capture.as_dict()) == capture
    assert capture.release_id == "registry-source-witness:" + CAPTURE_ID
    assert capture.contract == witness.CAPTURE_CONTRACT and hash(capture)
    assert "provider_directory_endpoint_dataset" not in witness.TABLES
    assert len(witness.TABLES) == 4 and capture.schema_oid != capture.source_database.database_oid
    overlapping_oid = replace(capture, schema_oid=capture.source_database.database_oid)
    overlapping_oid = replace(overlapping_oid, capture_sha256=witness.capture_digest(overlapping_oid))
    assert witness.validate_capture(overlapping_oid) == overlapping_oid


@pytest.mark.parametrize("field", tuple(witness.SourceWitnessCapture.__dataclass_fields__))
def test_capture_missing_fields_refuse(field):
    document = captured().as_dict()
    document.pop(field)
    with pytest.raises(ValueError):
        witness.validate_capture(document)


@pytest.mark.parametrize(
    "field", ["origin", "source_database", "postmaster_started", "resource_counts", "relation_oids"]
)
def test_capture_wire_rejects_nested_non_json_declarations(field):
    capture = captured()
    document = capture.as_dict()
    if field == "origin":
        document[field] = capture.origin
    elif field == "source_database":
        document[field] = capture.source_database
    elif field == "postmaster_started":
        document["source_database"][field] = database_fields()[field]
    elif field == "resource_counts":
        document["origin"][field] = capture.origin.resource_counts
    else:
        document[field] = capture.relation_oids
    with pytest.raises(ValueError):
        witness.validate_capture(document)


@pytest.mark.parametrize(
    "changes",
    [
        {"contract": witness.CONTRACT},
        {"capture_id": "00000000-0000-0000-0000-000000000000"},
        {"schema_name": "registry_fhir_capture_" + UUID(CAPTURE_ID).hex},
        {"schema_oid": True},
        {"runtime_roles": ("reader_example", "reader_example")},
        {"runtime_roles": ("owner_example",)},
        {"runtime_roles": ()},
        {"catalog_sha256": "A" * 64},
        {"content_sha256": "5" * 64},
        {"observed_at": "2026-01-03T00:00:00.000000+00:00"},
        {"observed_at": datetime(2026, 1, 1, tzinfo=UTC)},
    ],
)
def test_capture_invalid_or_digest_changed_fields_refuse(changes):
    with pytest.raises(ValueError):
        witness.validate_capture(replace(captured(), **changes))


def test_capture_binds_source_identity_raw_uri_and_every_target_relation():
    capture = captured()
    changes = [
        replace(capture, source_database=replace(capture.source_database, database_oid=15)),
        replace(capture, source_database=replace(capture.source_database, system_identifier="123456790")),
        replace(capture, origin=replace(capture.origin, source_uri=capture.origin.source_uri + "/")),
        replace(capture, relation_oids=tuple((name, 30) for name, _ in capture.relation_oids)),
        replace(capture, relation_oids=capture.relation_oids[:-1]),
    ]
    for changed in changes:
        assert witness.capture_digest(changed) != capture.capture_sha256
        with pytest.raises(ValueError):
            witness.validate_capture(changed)
    with pytest.raises(ValueError):
        witness.validate_capture(capture.as_dict() | {"complete": True})


@pytest.mark.parametrize("changes", [{"database_oid": 15}, {"system_identifier": "123456790"}])
def test_source_database_binding_refuses_even_with_a_recomputed_capture_digest(changes):
    capture = captured()
    capture = replace(capture, source_database=replace(capture.source_database, **changes))
    capture = replace(capture, capture_sha256=witness.capture_digest(capture))
    with pytest.raises(ValueError):
        witness.validate_capture(capture)


def test_capture_receipt_remains_bounded_even_for_valid_individual_aliases():
    aliases = tuple(sorted(("source-one", *(f"source-{index:04d}-" + "x" * 52 for index in range(999)))))
    capture = replace(captured(), origin=replace(captured().origin, source_aliases=aliases))
    capture = replace(capture, capture_sha256=witness.capture_digest(capture))
    assert len(witness.canonical(capture.as_dict()).encode()) > 65536
    with pytest.raises(ValueError):
        witness.validate_capture(capture)


def test_original_reader_capture_is_unchanged_and_never_a_source_witness():
    capture = reader_capture()
    assert reader.validate_capture(capture.as_dict()) == capture
    assert hashlib.sha256(reader.canonical(capture.as_dict()).encode()).hexdigest() == (
        "7bffee458edde1a95cec5ab7e770ff40865fa68ea32f097ede6cba9adb01be11"
    )
    for value in (asdict(capture.origin), capture.origin, capture.as_dict(), capture):
        with pytest.raises(ValueError):
            witness.source_origin(value)
    for value in (capture, capture.as_dict()):
        with pytest.raises(ValueError):
            witness.validate_capture(value)
    with pytest.raises(ValueError):
        reader.validate_capture(captured().as_dict())


@pytest.fixture
def expected_anchor():
    """Declare independent Core coordinates explicitly, before consulting any candidate receipt."""
    return witness.SourceWitnessCaptureAnchor(
        CAPTURE_ID, "registry-source-witness:" + CAPTURE_ID, "2026-01-02T00:00:00.000000+00:00"
    )


def test_independent_anchor_is_required_and_immutable(expected_anchor):
    capture = captured()
    assert witness.require_capture_anchor(capture, expected_anchor) is capture
    assert witness.require_capture_anchor(capture.as_dict(), expected_anchor) == capture
    assert hash(expected_anchor)
    with pytest.raises(FrozenInstanceError):
        expected_anchor.observed_at = "2026-01-03T00:00:00.000000+00:00"
    with pytest.raises(TypeError):
        witness.require_capture_anchor(capture)
    assert hashlib.sha256(witness.canonical(capture.as_dict()).encode()).hexdigest() == (
        "77720c2dc168c7a4106416351ab1922fa2753dbd34c1540a31b49a1a7060ff66"
    )


@pytest.mark.parametrize("field", ["capture_id", "release_id", "observed_at"])
def test_anchor_has_no_missing_or_default_coordinates(expected_anchor, field):
    anchor_by_field = asdict(expected_anchor)
    anchor_by_field.pop(field)
    with pytest.raises(TypeError):
        witness.SourceWitnessCaptureAnchor(**anchor_by_field)


def test_anchor_rejects_extra_fields(expected_anchor):
    with pytest.raises(TypeError):
        witness.SourceWitnessCaptureAnchor(**(asdict(expected_anchor) | {"candidate_owned": True}))


@pytest.mark.parametrize("kind", ["none", "dictionary", "namespace", "subclass"])
def test_anchor_requires_its_exact_type(expected_anchor, kind):
    anchors_by_kind = {
        "none": None,
        "dictionary": asdict(expected_anchor),
        "namespace": SimpleNamespace(**asdict(expected_anchor)),
        "subclass": type("UnitAnchorSubclass", (witness.SourceWitnessCaptureAnchor,), {})(**asdict(expected_anchor)),
    }
    with pytest.raises(ValueError):
        witness.require_capture_anchor(captured(), anchors_by_kind[kind])


@pytest.mark.parametrize(
    "changes",
    [
        {"capture_id": None},
        {"capture_id": True},
        {"capture_id": "00000000-0000-0000-0000-000000000000"},
        {"capture_id": "12345678-9ABC-4DEF-8ABC-123456789ABC"},
        {"release_id": None},
        {"release_id": "registry-capture:" + CAPTURE_ID},
        {"release_id": "registry-source-witness:00000000-0000-0000-0000-000000000002"},
        {"observed_at": None},
        {"observed_at": datetime(2026, 1, 2, tzinfo=UTC)},
        {"observed_at": "2026-01-02T00:00:00Z"},
        {"observed_at": "2026-01-02T00:00:00+00:00"},
        {"observed_at": "2026-01-02T01:00:00.000000+01:00"},
    ],
)
def test_anchor_rejects_invalid_core_coordinates(expected_anchor, changes):
    with pytest.raises(ValueError):
        witness.SourceWitnessCaptureAnchor(**(asdict(expected_anchor) | changes))


@pytest.mark.parametrize("changed", ["identity", "time", "both"])
def test_recomputed_capture_cannot_replace_independent_anchor(expected_anchor, changed):
    capture = captured()
    changed_by_field = {}
    if changed in {"identity", "both"}:
        identity = "00000000-0000-0000-0000-000000000002"
        changed_by_field.update(capture_id=identity, schema_name=witness.SCHEMA_PREFIX + UUID(identity).hex)
    if changed in {"time", "both"}:
        changed_by_field["observed_at"] = "2026-01-03T00:00:00.000000+00:00"
    forged = replace(capture, **changed_by_field)
    forged = replace(forged, capture_sha256=witness.capture_digest(forged))
    assert witness.validate_capture(forged) == forged
    with pytest.raises(ValueError):
        witness.require_capture_anchor(forged, expected_anchor)
