# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source declarations are closed and remain distinct from original Reader captures."""

from dataclasses import FrozenInstanceError, asdict, make_dataclass, replace
from datetime import UTC, datetime
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
        "header_relation": {"schema": "source_example", "name": "provider_directory_endpoint_dataset"},
        "resource_relation": {"schema": "source_example", "name": "provider_directory_dataset_resource"},
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
    }


def database_fields():
    return {
        "database_name": "source_example",
        "database_oid": 14,
        "system_identifier": "123456789",
        "postmaster_started": datetime(2026, 1, 1, 2, 3, 4, 5, tzinfo=UTC),
        "recovering": False,
    }


def legacy_origin_fields():
    return origin_fields() | {
        "admission_profile": witness.OBSERVED_LEGACY_PROFILE,
        "proof_sha256": None,
        "admission_metadata_sha256": None,
        "semantic_projection_as_of": None,
        "descriptor_counts": None,
    }


def captured(*, legacy=False):
    capture = witness.SourceWitnessCapture(
        origin=witness.source_origin(legacy_origin_fields() if legacy else origin_fields()),
        capture_id=CAPTURE_ID,
        schema_name=witness.SCHEMA_PREFIX + UUID(CAPTURE_ID).hex,
        tables={
            name: {"schema": witness.SCHEMA_PREFIX + UUID(CAPTURE_ID).hex, "name": name} for name in witness.TABLES
        },
        observed_at="2026-01-02T00:00:00.000000+00:00",
        content_sha256="4" * 64,
        capture_sha256="0" * 64,
    )
    return replace(capture, capture_sha256=witness.capture_digest(capture))


def reader_capture():
    origin = reader.RetainedFHIRInput(
        schema_name="source_example",
        source_id="source-one",
        endpoint_id="endpoint-one",
        dataset_id="dataset-one",
        dataset_sha256="a" * 64,
        root_run_id="run-one",
        header_relation={"schema": "source_example", "name": "provider_directory_endpoint_dataset"},
        resource_relation={"schema": "source_example", "name": "provider_directory_dataset_resource"},
        metadata_bytes=100,
        metadata_text_sha256="b" * 64,
        summary_sha256="c" * 64,
        proof_sha256="d" * 64,
        resource_counts=(("Location", 1), ("Organization", 1)),
        resource_hashes=(("Location", "e" * 64), ("Organization", "f" * 64)),
        descriptor_counts=(("Location", 1), ("Organization", 2)),
        original_admission_refusal=reader.REFUSAL,
    )
    schema = "registry_fhir_capture_" + UUID(CAPTURE_ID).hex
    capture = reader.RetainedFHIRCapture(
        origin=origin,
        capture_id=CAPTURE_ID,
        schema_name=schema,
        tables={name: {"schema": schema, "name": name} for name in reader.TABLES},
        content_sha256="2" * 64,
        capture_sha256="0" * 64,
    )
    return replace(capture, capture_sha256=reader.capture_digest(capture))


def test_source_origin_has_exact_source_fields_without_reader_authority():
    fields = origin_fields()
    declaration = make_dataclass("SourceDeclaration", [(name, object) for name in fields], frozen=True)(**fields)
    origin = witness.source_origin(declaration)
    assert len(asdict(origin)) == 21 and asdict(origin) == fields
    assert witness.source_origin(origin) is origin
    assert witness.source_origin(captured().as_dict()["origin"]) == origin
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
        {"header_relation": {"schema": "bad.schema", "name": "header"}},
        {"resource_relation": {"schema": "other", "name": "resource"}},
        {"dataset_sha256": "A" * 64},
        {"metadata_bytes": True},
        {"metadata_bytes": 256 * 1024**2 + 1},
        {"source_aliases": ("source-two",)},
        {"source_aliases": ("source-two", "source-one")},
        {"source_aliases": ("source-one", "source-one")},
        {"resource_counts": ()},
        {"resource_counts": (("Location", 0),)},
        {"resource_counts": (("Location", True),)},
        {"resource_hashes": (("Location", "e" * 64),)},
        {"descriptor_counts": (("Location", 0), ("Organization", 1), ("Practitioner", 0))},
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


@pytest.mark.parametrize("timestamp", ["2026-13-01T00:00:00.000000+00:00", "2026-01-32T00:00:00.000000+00:00"])
def test_malformed_canonical_timestamps_refuse(timestamp):
    with pytest.raises(witness.SourceWitnessContractError):
        witness.canonical_timestamp(timestamp)


def test_capture_routes_by_names_and_preserves_logical_content():
    capture = captured()
    assert witness.validate_capture(capture) is capture
    assert witness.validate_capture(capture.as_dict()) == capture
    assert capture.release_id == "registry-source-witness:" + CAPTURE_ID
    assert capture.contract == witness.CAPTURE_CONTRACT
    assert set(capture.tables) == set(witness.TABLES)
    assert all(relation["schema"] == capture.schema_name for relation in capture.tables.values())


@pytest.mark.parametrize("field", tuple(witness.SourceWitnessCapture.__dataclass_fields__))
def test_capture_missing_fields_refuse(field):
    document = captured().as_dict()
    document.pop(field)
    with pytest.raises(ValueError):
        witness.validate_capture(document)


@pytest.mark.parametrize("field", ["origin", "resource_counts"])
def test_capture_wire_rejects_nested_non_json_declarations(field):
    document = captured().as_dict()
    if field == "origin":
        document[field] = captured().origin
    else:
        document["origin"][field] = captured().origin.resource_counts
    with pytest.raises(ValueError):
        witness.validate_capture(document)


@pytest.mark.parametrize(
    "changes",
    [
        {"contract": witness.CONTRACT},
        {"capture_id": "00000000-0000-0000-0000-000000000000"},
        {"schema_name": "registry_fhir_capture_" + UUID(CAPTURE_ID).hex},
        {"tables": {}},
        {"content_sha256": "5" * 64},
        {"observed_at": "2026-01-03T00:00:00.000000+00:00"},
        {"observed_at": datetime(2026, 1, 1, tzinfo=UTC)},
    ],
)
def test_capture_invalid_or_digest_changed_fields_refuse(changes):
    with pytest.raises(ValueError):
        witness.validate_capture(replace(captured(), **changes))


def test_capture_binds_source_content_uri_and_every_named_relation():
    capture = captured()
    changes = [
        replace(capture, origin=replace(capture.origin, source_uri=capture.origin.source_uri + "/")),
        replace(capture, content_sha256="8" * 64),
        replace(capture, tables={}),
    ]
    for changed in changes:
        assert witness.capture_digest(changed) != capture.capture_sha256
        with pytest.raises(ValueError):
            witness.validate_capture(changed)
    with pytest.raises(ValueError):
        witness.validate_capture(capture.as_dict() | {"complete": True})


def test_capture_receipt_remains_bounded_even_for_valid_individual_aliases():
    aliases = tuple(sorted(("source-one", *(f"source-{index:04d}-" + "x" * 52 for index in range(999)))))
    capture = replace(captured(), origin=replace(captured().origin, source_aliases=aliases))
    capture = replace(capture, capture_sha256=witness.capture_digest(capture))
    assert len(witness.canonical(capture.as_dict()).encode()) > 65536
    with pytest.raises(ValueError):
        witness.validate_capture(capture)


def test_retained_profile_keeps_distinct_logical_data_contract():
    capture = reader_capture()
    assert reader.validate_capture(capture.as_dict()) == capture
    assert set(capture.tables) == set(reader.TABLES)
    assert all(table["schema"] == capture.schema_name for table in capture.tables.values())
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
        schema = witness.SCHEMA_PREFIX + UUID(identity).hex
        changed_by_field.update(
            capture_id=identity,
            schema_name=schema,
            tables={name: {"schema": schema, "name": name} for name in witness.TABLES},
        )
    if changed in {"time", "both"}:
        changed_by_field["observed_at"] = "2026-01-03T00:00:00.000000+00:00"
    forged = replace(capture, **changed_by_field)
    forged = replace(forged, capture_sha256=witness.capture_digest(forged))
    assert witness.validate_capture(forged) == forged
    with pytest.raises(ValueError):
        witness.require_capture_anchor(forged, expected_anchor)


def test_observed_legacy_origin_preserves_absent_metadata_and_wire_shape():
    origin = witness.source_origin(legacy_origin_fields())
    assert len(asdict(origin)) == 21
    assert asdict(origin) == legacy_origin_fields()
    assert witness.validate_capture(captured(legacy=True).as_dict()) == captured(legacy=True)
    with pytest.raises(ValueError):
        replace(reader_capture().origin, admission_profile=witness.OBSERVED_LEGACY_PROFILE)


def test_semantic_origin_preserves_actual_descriptor_occurrences():
    fields = origin_fields()
    fields["descriptor_counts"] = (("Location", 2), ("Organization", 1), ("Practitioner", 0))
    assert asdict(witness.source_origin(fields)) == fields


def test_rooted_observation_keeps_its_existing_metadata_budget():
    limit = 64 * 1024**2 - 32768 - 1
    fields = origin_fields() | {
        "admission_profile": witness.OBSERVED_ROOTED_PROFILE,
        "admission_metadata_sha256": None,
        "descriptor_counts": None,
        "metadata_bytes": limit,
    }
    assert witness.source_origin(fields).metadata_bytes == limit
    with pytest.raises(ValueError):
        witness.source_origin(fields | {"metadata_bytes": limit + 1})


@pytest.mark.parametrize("profile", [witness.SEMANTIC_PROFILE, witness.ADMITTED_LEGACY_PROFILE])
def test_admitted_profiles_use_existing_streaming_admission_bound(profile):
    fields = origin_fields() | {
        "admission_profile": profile,
        "semantic_projection_as_of": "2026-01-01" if profile == witness.SEMANTIC_PROFILE else None,
        "metadata_bytes": 160 * 1024**2,
    }
    assert witness.source_origin(fields).metadata_bytes == 160 * 1024**2
    limit = witness.source_metadata_max_bytes(profile)
    assert limit == 256 * 1024**2
    assert witness.source_origin(fields | {"metadata_bytes": limit}).metadata_bytes == limit
    with pytest.raises(ValueError):
        witness.source_origin(fields | {"metadata_bytes": limit + 1})
    with pytest.raises(ValueError):
        witness.source_origin(legacy_origin_fields() | {"metadata_bytes": 64 * 1024**2 - 32768})
    with pytest.raises(ValueError):
        replace(reader_capture().origin, metadata_bytes=64 * 1024**2 - 32768)


@pytest.mark.parametrize("metadata_bytes", [72 * 1024**2 + 17, 240 * 1024**2 + 23])
def test_semantic_original_metadata_size_does_not_expand_declaration(metadata_bytes):
    origin = witness.source_origin(origin_fields() | {"metadata_bytes": metadata_bytes})
    assert origin.metadata_bytes == metadata_bytes
    assert len(witness.canonical(asdict(origin)).encode()) < 2048
    assert witness.source_origin(asdict(origin)) == origin


@pytest.mark.parametrize(
    "profile", [witness.ADMITTED_LEGACY_PROFILE, witness.ADMITTED_TRANSPORT_PROFILE, witness.ADMITTED_UHC_PROFILE]
)
def test_profiles_without_semantic_date_use_original_capture_anchor(profile):
    capture = captured()
    origin = witness.source_origin(origin_fields() | {"admission_profile": profile, "semantic_projection_as_of": None})
    capture = replace(capture, origin=origin)
    capture = replace(capture, capture_sha256=witness.capture_digest(capture))
    anchor = witness.SourceWitnessCaptureAnchor(capture.capture_id, capture.release_id, capture.observed_at)
    assert witness.source_witness_membership_date(capture, anchor) == "2026-01-02"


def test_completion_origin_preserves_closed_wire_shapes():
    fields = origin_fields() | {
        "admission_profile": witness.ADMITTED_TRANSPORT_PROFILE,
        "semantic_projection_as_of": None,
        "completion_required_version": 3,
        "completion_sha256": "8" * 64,
    }
    origin = witness.source_origin(fields)
    assert type(origin) is witness.SourceWitnessCompletionOrigin and len(asdict(origin)) == 23
    original = captured()
    capture = replace(original, origin=origin)
    capture = replace(capture, capture_sha256=witness.capture_digest(capture))
    assert witness.validate_capture(capture.as_dict()) == capture
    assert len(asdict(witness.source_origin(origin_fields()))) == 21
    for change in (
        {"completion_required_version": True},
        {"completion_sha256": "bad"},
        {"admission_profile": witness.ADMITTED_LEGACY_PROFILE},
    ):
        with pytest.raises(ValueError):
            witness.source_origin(fields | change)
    for missing in ("completion_sha256", "completion_required_version"):
        with pytest.raises(ValueError):
            witness.source_origin({key: value for key, value in fields.items() if key != missing})


@pytest.mark.parametrize(
    "field", ["proof_sha256", "admission_metadata_sha256", "semantic_projection_as_of", "descriptor_counts"]
)
def test_observed_legacy_origin_rejects_mixed_modern_metadata(field):
    with pytest.raises(ValueError):
        witness.source_origin(legacy_origin_fields() | {field: origin_fields()[field]})
    with pytest.raises(ValueError):
        witness.source_origin(origin_fields() | {field: None})


@pytest.mark.parametrize(
    "field", ["root_run_id", "summary_sha256", "dataset_sha256", "resource_counts", "resource_hashes"]
)
def test_observed_legacy_origin_requires_original_roots_and_complete_families(field):
    with pytest.raises(ValueError):
        witness.source_origin(legacy_origin_fields() | {field: None})


def test_legacy_membership_date_is_anchored_observation_utc_date():
    capture = captured(legacy=True)
    anchor = witness.SourceWitnessCaptureAnchor(capture.capture_id, capture.release_id, capture.observed_at)
    assert witness.source_witness_membership_date(capture, anchor) == "2026-01-02"
    assert witness.source_witness_membership_date(captured(), anchor) == "2026-01-01"
    changed = replace(capture, observed_at="2026-01-03T00:00:00.000000+00:00")
    changed = replace(changed, capture_sha256=witness.capture_digest(changed))
    with pytest.raises(ValueError):
        witness.source_witness_membership_date(changed, anchor)
    with pytest.raises(ValueError):
        witness.source_witness_membership_date(capture, None)


def test_legacy_capture_projects_stored_names_and_preserves_generation():
    capture = captured()
    legacy = capture.as_dict()
    legacy.pop("tables")
    legacy.update(
        contract=witness.LEGACY_CAPTURE_CONTRACT,
        source_database={"database_oid": 999},
        schema_oid=12,
        relation_oids=[],
        owner_role="historical_owner",
        runtime_roles=[],
        catalog_sha256="e" * 64,
    )
    projected = witness.validate_capture(legacy)
    assert projected.capture_sha256 == capture.capture_sha256
    assert projected.tables == capture.tables
    assert set(projected.as_dict()) == set(witness.SourceWitnessCapture.__dataclass_fields__)
    copied = witness.validate_capture(dict(legacy, schema_oid=9999, source_database={"database_oid": 123}))
    assert copied == projected
