# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused replay contracts for the bounded Snowflake family bridge."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import snowflake
from process.custom_import.capture import capture_stream
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner_codec import child_key_hash, new_family_hash, root_key_hash
from process.custom_import.snowflake_candidate import (
    SnowflakeCandidateError,
    SnowflakeCandidateRequest,
    _prepare_candidate,
    run_snowflake_candidate,
)


def _definition(*, with_child: bool = False, with_nullable_root_field: bool = False) -> CustomImportDefinition:
    child_collections = []
    root_fields = [
        {"id": "npi", "slot": 1, "type": "string", "nullable": False},
        {"id": "display_name", "slot": 2, "type": "string", "nullable": False},
    ]
    if with_nullable_root_field:
        root_fields.append({"id": "note", "slot": 3, "type": "string", "nullable": True})
    streams = [
        {
            "id": "snowflake_result",
            "kind": "root",
            "format": "parquet",
            "compression": "none",
            "snapshot_token": "source_snapshot",
        }
    ]
    if with_child:
        child_collections.append(
            {
                "name": "details",
                "parent_key": [{"child": "detail_npi", "root": "npi"}],
                "child_key": ["detail_id"],
                "fields": [
                    {"id": "detail_npi", "slot": 3, "type": "string", "nullable": False},
                    {"id": "detail_id", "slot": 4, "type": "string", "nullable": False},
                ],
            }
        )
        streams.append(
            {
                "id": "details",
                "kind": "child",
                "child": "details",
                "format": "parquet",
                "compression": "none",
                "snapshot_token": "source_snapshot",
            }
        )
    return CustomImportDefinition.from_mapping(
        {
            "contract": "custom-import/v1",
            "revision": {"definition": 1, "schema": 1},
            "refresh_mode": "snapshot",
            "streams": streams,
            "schema": {
                "root": {
                    "logical_key": ["npi"],
                    "entity": {"adapter": "npi", "field": "npi"},
                    "fields": root_fields,
                },
                "children": child_collections,
            },
            "aliases": {stream["id"]: {} for stream in streams},
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _capture(table: pa.Table, snapshot_token: str):
    destination = BytesIO()
    pq.write_table(table, destination)
    return capture_stream(
        BytesIO(destination.getvalue()),
        snowflake.SNOWFLAKE_RESULT_STREAM,
        source_snapshot_token=snapshot_token,
    )


def _acquisition(
    definition: CustomImportDefinition,
    *,
    first_npi: object = "1234567893",
) -> snowflake.SnowflakeAcquisition:
    return _table_acquisition(
        definition,
        ("npi", "display_name"),
        (
            pa.table({"npi": [first_npi], "display_name": ["Synthetic One"]}),
            pa.table({"npi": ["1234567893"], "display_name": ["Synthetic Two"]}),
        ),
    )


def _table_acquisition(
    definition: CustomImportDefinition,
    field_ids: tuple[str, ...],
    partitions: tuple[pa.Table, ...],
    *,
    snapshot_token: str = "synthetic-snapshot",
    column_identifiers: tuple[str, ...] | None = None,
) -> snowflake.SnowflakeAcquisition:
    """Seal synthetic result bytes independently of the declared projection."""

    request = snowflake.SnowflakeReadRequest(
        relation=snowflake.SnowflakeRelation(database="synthetic", schema="public", name="root_records"),
        selected_columns=tuple(
            snowflake.SnowflakeDeclaredColumn(field_id=field_id, column_identifier=column)
            for field_id, column in zip(field_ids, column_identifiers or field_ids, strict=True)
        ),
        definition_sha256=definition.digest,
        schema_sha256=definition.schema_digest,
    )
    statement = snowflake.SnowflakeReadStatement(request=request)
    captures = tuple(_capture(table, snapshot_token) for table in partitions)
    receipts = tuple(
        snowflake._capture_receipt(
            capture,
            ordinal=ordinal,
            source_snapshot_token=snapshot_token,
        )
        for ordinal, capture in enumerate(captures, start=1)
    )
    content_hasher = snowflake._ResultContentHasher()
    for receipt in receipts:
        content_hasher.add(receipt)
    _, schema_fingerprint = snowflake._identity_sha256(
        "result-schema",
        {
            "columns": [{"field_id": field_id, "nullable": False, "source_type": "TEXT"} for field_id in field_ids],
            "contract": snowflake.CONNECTOR_CONTRACT,
            "format": snowflake.PARQUET_RESULT_FORMAT,
        },
    )
    return snowflake.SnowflakeAcquisition(
        statement=statement,
        manifest=snowflake.SnowflakeAcquisitionManifest(
            request_sha256=request.request_sha256,
            statement_sha256=statement.statement_sha256,
            source_snapshot_token=snapshot_token,
            schema_fingerprint=schema_fingerprint,
            result_partitions=receipts,
            content_sha256=content_hasher.hexdigest(),
        ),
        parquet_captures=captures,
    )


def _request(
    definition: CustomImportDefinition, acquisition: snowflake.SnowflakeAcquisition
) -> SnowflakeCandidateRequest:
    return SnowflakeCandidateRequest(
        dataset_id=1,
        definition_revision_id=1,
        schema_revision_id=1,
        definition=definition,
        acquisition=acquisition,
        idempotency_key="synthetic-snowflake-candidate",
        lease_token="synthetic-snowflake-lease",
    )


def test_bridge_replays_each_partition_before_preparing_a_root_candidate():
    definition = _definition()
    acquisition = _acquisition(definition)

    prepared = _prepare_candidate(_request(definition, acquisition))

    assert [root["display_name"] for root in prepared.roots] == ["Synthetic One", "Synthetic Two"]
    assert prepared.receipts[0].content_sha256 == acquisition.manifest.content_sha256
    assert prepared.children_by_collection == {}
    with pytest.raises(SnowflakeCandidateError, match="exactly cover"):
        _prepare_candidate(_request(_definition(with_child=True), acquisition))
    with pytest.raises(SnowflakeCandidateError, match="aggregate record"):
        _prepare_candidate(
            _request(
                definition, replace(acquisition, capture_limits=replace(acquisition.capture_limits, maximum_records=1))
            )
        )
    with pytest.raises(SnowflakeCandidateError, match="schema type"):
        _prepare_candidate(_request(definition, _acquisition(definition, first_npi=1234567893)))
    object.__setattr__(acquisition, "parquet_captures", tuple(reversed(acquisition.parquet_captures)))
    with pytest.raises(SnowflakeCandidateError, match="seal"):
        _prepare_candidate(_request(definition, acquisition))


def test_approved_root_acquisition_replays_through_the_generic_candidate_boundary():
    document = json.loads(_definition().canonical)
    document["aliases"]["snowflake_result"] = {
        "PROVIDER_NPI": "npi",
        "PROVIDER_DISPLAY_NAME": "display_name",
    }
    definition = CustomImportDefinition.from_mapping(document)
    relation = snowflake.SnowflakeRelation(database="synthetic", schema="public", name="root_records")
    approved_relation = snowflake.SnowflakeApprovedRelation(
        relation=relation,
        columns=(
            snowflake.SnowflakeDeclaredColumn(field_id="npi", column_identifier="provider_npi"),
            snowflake.SnowflakeDeclaredColumn(field_id="display_name", column_identifier="provider_display_name"),
        ),
    )
    destination = BytesIO()
    pq.write_table(pa.table({"npi": ["1234567893"], "display_name": ["Synthetic One"]}), destination)
    statements = []

    def partition_sources():
        yield BytesIO(destination.getvalue())

    def fetch_parquet(statement, _credentials):
        statements.append(statement)
        return snowflake.SnowflakeParquetResult(
            source_snapshot_token="synthetic-snapshot",
            schema=(
                snowflake.SnowflakeResultColumn(field_id="npi", source_type="TEXT", nullable=False),
                snowflake.SnowflakeResultColumn(field_id="display_name", source_type="TEXT", nullable=False),
            ),
            partition_sources=partition_sources(),
        )

    connector = snowflake.SnowflakeAcquisitionConnector(
        approved_relations=(approved_relation,),
        credential_provider=SimpleNamespace(
            load_key_pair=lambda: snowflake.SnowflakeKeyPairCredentials(
                account="synthetic-account",
                user="synthetic-user",
                private_key_pem=b"-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----\n",
            )
        ),
        adapter=SimpleNamespace(fetch_parquet=fetch_parquet),
    )

    acquisition = connector.acquire(
        connector.prepare_request(
            definition,
            relation=relation,
            selected_field_ids=("npi", "display_name"),
        )
    )
    prepared = _prepare_candidate(_request(definition, acquisition))

    assert statements == [acquisition.statement]
    assert prepared.roots == ({"npi": "1234567893", "display_name": "Synthetic One"},)
    assert prepared.children_by_collection == {}
    assert prepared.receipts[0].stream_id == "snowflake_result"


@pytest.mark.parametrize("target", ("request", "manifest"))
def test_bridge_rejects_stale_nested_identity_seals(target):
    definition = _definition()
    acquisition = _acquisition(definition)
    if target == "request":
        object.__setattr__(acquisition.statement.request, "definition_sha256", "f" * 64)
    else:
        object.__setattr__(acquisition.manifest, "canonical_manifest", "{}")

    with pytest.raises(SnowflakeCandidateError, match="seal"):
        _prepare_candidate(_request(definition, acquisition))


@pytest.mark.asyncio
async def test_bridge_rejects_omitted_nullable_root_field_before_storage():
    definition = _definition(with_nullable_root_field=True)

    def session_factory():
        raise AssertionError("storage must not open for an incomplete root projection")

    with pytest.raises(SnowflakeCandidateError, match="exactly cover"):
        await run_snowflake_candidate(
            session_factory,
            _request(definition, _acquisition(definition)),
        )


def _family_request(
    *,
    child_values: tuple[object, ...] = ("second", "first"),
    child_snapshot: str = "synthetic-snapshot",
) -> SnowflakeCandidateRequest:
    document = json.loads(_definition(with_child=True).canonical)
    document["aliases"] = {
        "snowflake_result": {"PROVIDER_NPI": "npi", "PROVIDER_NAME": "display_name"},
        "details": {"PARENT_NPI": "detail_npi", "DETAIL_KEY": "detail_id"},
    }
    definition = CustomImportDefinition.from_mapping(document)
    root_acquisition = _table_acquisition(
        definition,
        ("npi", "display_name"),
        (pa.table({"npi": ["1234567893"], "display_name": ["Synthetic One"]}),),
        column_identifiers=("PROVIDER_NPI", "PROVIDER_NAME"),
    )
    child_acquisition = _table_acquisition(
        definition,
        ("detail_npi", "detail_id"),
        (
            pa.table(
                {
                    "detail_npi": pa.array(["1234567893"] * len(child_values), type=pa.string()),
                    "detail_id": pa.array(child_values, type=None if child_values else pa.string()),
                }
            ),
        ),
        snapshot_token=child_snapshot,
        column_identifiers=("PARENT_NPI", "DETAIL_KEY"),
    )
    return replace(_request(definition, root_acquisition), child_acquisition=child_acquisition)


def test_bridge_binds_shared_snapshot_and_family_keys():
    request = _family_request()
    prepared = _prepare_candidate(request)
    admitted = assemble_root_families(request.definition, prepared.roots, prepared.children_by_collection)
    reordered = replace(
        prepared,
        children_by_collection={"details": tuple(reversed(prepared.children_by_collection["details"]))},
    )
    replay = assemble_root_families(request.definition, reordered.roots, reordered.children_by_collection)

    assert admitted.rejections == admitted.candidate_errors == ()
    assert len(admitted.families) == 1
    assert admitted.families[0].root_key == ("1234567893",)
    assert [receipt.stream_id for receipt in prepared.receipts] == ["snowflake_result", "details"]
    assert {receipt.source_snapshot_token for receipt in prepared.receipts} == {"synthetic-snapshot"}
    assert new_family_hash(request.definition, admitted.families[0]) == new_family_hash(
        request.definition, replay.families[0]
    )
    assert root_key_hash(request.definition, admitted.families[0].root) == root_key_hash(
        request.definition, replay.families[0].root
    )
    assert (
        len(
            {
                child_key_hash(request.definition, "details", child)
                for child in prepared.children_by_collection["details"]
            }
        )
        == 2
    )
    assert prepared.receipts[1].manifest_sha256 == request.child_acquisition.manifest.manifest_sha256


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ("snapshot", "missing_child", "extra_child", "swapped_scope", "seal", "type"))
async def test_bridge_rejects_invalid_source_bundles_before_storage(defect):
    request = _family_request()
    match defect:
        case "snapshot":
            request = _family_request(child_snapshot="different-synthetic-snapshot")
        case "missing_child":
            request = replace(request, child_acquisition=None)
        case "extra_child":
            definition = _definition()
            request = replace(
                _request(definition, _acquisition(definition)), child_acquisition=request.child_acquisition
            )
        case "swapped_scope":
            request = replace(request, acquisition=request.child_acquisition, child_acquisition=request.acquisition)
        case "seal":
            object.__setattr__(request.child_acquisition.manifest, "canonical_manifest", "{}")
        case "type":
            request = _family_request(child_values=(1,))

    def session_factory():
        raise AssertionError("storage must not open for an invalid source bundle")

    with pytest.raises(SnowflakeCandidateError):
        await run_snowflake_candidate(session_factory, request)


@pytest.mark.parametrize("alias_mapping", ({"NPI": "display_name"}, {"UNSELECTED": "npi"}))
def test_bridge_rejects_inconsistent_source_aliases(alias_mapping):
    document = json.loads(_definition().canonical)
    document["aliases"] = {"snowflake_result": alias_mapping}
    definition = CustomImportDefinition.from_mapping(document)

    with pytest.raises(SnowflakeCandidateError, match="aliases"):
        _prepare_candidate(_request(definition, _acquisition(definition)))


@pytest.mark.parametrize("has_rows", (False, True))
@pytest.mark.parametrize("defect", ("missing", "extra", "order", "type"))
def test_bridge_validates_every_partition_schema(has_rows, defect):
    definition = _definition()
    columns_by_name = {
        "npi": pa.array(["1234567893"] if has_rows else [], type=pa.string()),
        "display_name": pa.array(["Synthetic One"] if has_rows else [], type=pa.string()),
    }
    if defect == "missing":
        columns_by_name.pop("display_name")
    elif defect == "extra":
        columns_by_name["unselected"] = columns_by_name["npi"]
    elif defect == "order":
        columns_by_name = dict(reversed(tuple(columns_by_name.items())))
    else:
        columns_by_name["display_name"] = pa.array([1] if has_rows else [], type=pa.int64())
    acquisition = _table_acquisition(definition, ("npi", "display_name"), (pa.table(columns_by_name),))

    with pytest.raises(SnowflakeCandidateError, match="schema"):
        _prepare_candidate(_request(definition, acquisition))


@pytest.mark.parametrize("child_values", (("same", "same"), ("orphan",)))
def test_bridge_preserves_generic_child_rejection(child_values):
    request = _family_request(child_values=child_values)
    prepared = _prepare_candidate(request)
    children = prepared.children_by_collection["details"]
    if child_values == ("orphan",):
        children = ({**children[0], "detail_npi": "9876543215"},)
    admitted = assemble_root_families(request.definition, prepared.roots, {"details": children})

    if child_values == ("orphan",):
        assert admitted.candidate_errors == ("orphan_child",)
    else:
        assert admitted.families == ()
        assert [rejection.code for rejection in admitted.rejections] == ["duplicate_child_key"]


@pytest.mark.parametrize("amount", ("12.50", Decimal("12.50"), 12))
def test_bridge_reuses_declared_decimal_types(amount):
    document = json.loads(_definition().canonical)
    document["schema"]["root"]["fields"][1]["type"] = "decimal"
    definition = CustomImportDefinition.from_mapping(document)
    acquisition = _table_acquisition(
        definition,
        ("npi", "display_name"),
        (pa.table({"npi": ["1234567893"], "display_name": [amount]}),),
    )

    prepared = _prepare_candidate(_request(definition, acquisition))

    assert prepared.roots[0]["display_name"] == amount


def test_bridge_accepts_schema_bearing_empty_child():
    request = _family_request(child_values=())

    prepared = _prepare_candidate(request)

    assert prepared.children_by_collection == {"details": ()}
    assert len(prepared.receipts) == 2


@pytest.mark.parametrize(
    ("defect", "rejection_code"),
    (("null", "required_field_null"), ("decimal", "field_type_invalid"), ("npi", "entity_binding_invalid")),
)
def test_bridge_preserves_typed_row_errors_for_family_rejection(defect, rejection_code):
    definition = _definition()
    columns_by_name = {"npi": ["1234567893"], "display_name": ["Synthetic One"]}
    if defect == "null":
        columns_by_name["display_name"] = pa.array([None], type=pa.string())
    elif defect == "decimal":
        document = json.loads(definition.canonical)
        document["schema"]["root"]["fields"][1]["type"] = "decimal"
        definition = CustomImportDefinition.from_mapping(document)
        columns_by_name["display_name"] = ["12e3"]
    elif defect == "npi":
        columns_by_name["npi"] = ["1234567890"]
    acquisition = _table_acquisition(
        definition,
        ("npi", "display_name"),
        (pa.table(columns_by_name),),
    )

    prepared = _prepare_candidate(_request(definition, acquisition))
    admitted = assemble_root_families(definition, prepared.roots, {})

    assert admitted.families == ()
    assert [rejection.code for rejection in admitted.rejections] == [rejection_code]


@pytest.mark.parametrize("defect", ("no_partitions", "duplicate_column"))
def test_bridge_rejects_invalid_capture_structure(defect):
    definition = _definition()
    acquisition = _table_acquisition(
        definition,
        ("npi", "display_name"),
        () if defect == "no_partitions" else (pa.table({"npi": ["1234567893"], "display_name": ["One"]}),),
        column_identifiers=("NPI", "NPI") if defect == "duplicate_column" else None,
    )

    with pytest.raises(SnowflakeCandidateError):
        _prepare_candidate(_request(definition, acquisition))


def test_bridge_rejects_multiple_declared_children():
    request = _family_request()
    document = json.loads(request.definition.canonical)
    document["schema"]["children"].append(
        {
            "name": "extras",
            "parent_key": [{"child": "extra_npi", "root": "npi"}],
            "child_key": ["extra_id"],
            "fields": [
                {"id": "extra_npi", "slot": 5, "type": "string", "nullable": False},
                {"id": "extra_id", "slot": 6, "type": "string", "nullable": False},
            ],
        }
    )
    document["streams"].append({**document["streams"][1], "id": "extras", "child": "extras"})
    request = replace(request, definition=CustomImportDefinition.from_mapping(document))

    with pytest.raises(SnowflakeCandidateError, match="at most one"):
        _prepare_candidate(request)


@pytest.mark.parametrize("stream_update", ({"format": "json"}, {"compression": "gzip"}, {"snapshot_token": "other"}))
def test_bridge_preserves_fixed_transport_shape(stream_update):
    request = _family_request()
    document = json.loads(request.definition.canonical)
    document["streams"][1].update(stream_update)
    request = replace(request, definition=CustomImportDefinition.from_mapping(document))

    with pytest.raises(SnowflakeCandidateError, match="fixed Parquet"):
        _prepare_candidate(request)
