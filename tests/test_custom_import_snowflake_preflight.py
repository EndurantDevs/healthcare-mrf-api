# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for bounded custom-import Snowflake preflight."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from process.custom_import import snowflake_preflight
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import MAX_SCALAR_INTEGER, MIN_SCALAR_INTEGER, FamilyRejection
from process.custom_import.snowflake import SnowflakeApprovedRelation, SnowflakeDeclaredColumn
from process.custom_import.snowflake_bundle import (
    DEFAULT_BUNDLE_ENCODING,
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleBinding,
    SnowflakeBundleRequest,
)
from process.custom_import.snowflake_preflight import (
    SnowflakePreflightError,
    SnowflakePreflightLimits,
    SnowflakePreflightRejectionDiagnostic,
    SnowflakePreflightResult,
    SnowflakePreflightStatement,
    SnowflakePreflightStreamObservation,
    SnowflakePreflightValidation,
    preflight_snowflake_bundle,
)
from process.custom_import.snowflake_source_binding import (
    SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
    SOURCE_BINDING_CONTRACT,
    SnowflakeSourceBinding,
)

_TOKEN = "synthetic-snapshot"
_DEFINITION_DOCUMENT = {
    "contract": "custom-import/v1",
    "revision": {"definition": 1, "schema": 1},
    "refresh_mode": "snapshot",
    "streams": [
        {
            "id": "root_source",
            "kind": "root",
            "format": "parquet",
            "compression": "none",
            "snapshot_token": "root_snapshot",
        },
        {
            "id": "detail_source",
            "kind": "child",
            "child": "details",
            "format": "parquet",
            "compression": "none",
            "snapshot_token": "detail_snapshot",
        },
        {
            "id": "note_source",
            "kind": "child",
            "child": "notes",
            "format": "parquet",
            "compression": "none",
            "snapshot_token": "note_snapshot",
        },
    ],
    "schema": {
        "root": {
            "logical_key": ["npi", "edition"],
            "entity": {"adapter": "npi", "field": "npi"},
            "fields": [
                {"id": "npi", "slot": 1, "type": "string", "nullable": False},
                {"id": "edition", "slot": 2, "type": "integer", "nullable": False},
                {"id": "label", "slot": 3, "type": "string", "nullable": False},
            ],
        },
        "children": [
            {
                "name": "details",
                "parent_key": [
                    {"child": "detail_npi", "root": "npi"},
                    {"child": "detail_edition", "root": "edition"},
                ],
                "child_key": ["detail_id"],
                "fields": [
                    {"id": "detail_npi", "slot": 10, "type": "string", "nullable": False},
                    {"id": "detail_edition", "slot": 11, "type": "integer", "nullable": False},
                    {"id": "detail_id", "slot": 12, "type": "string", "nullable": False},
                ],
            },
            {
                "name": "notes",
                "parent_key": [
                    {"child": "note_npi", "root": "npi"},
                    {"child": "note_edition", "root": "edition"},
                ],
                "child_key": ["note_id"],
                "fields": [
                    {"id": "note_npi", "slot": 20, "type": "string", "nullable": False},
                    {"id": "note_edition", "slot": 21, "type": "integer", "nullable": False},
                    {"id": "note_id", "slot": 22, "type": "string", "nullable": False},
                ],
            },
        ],
    },
    "aliases": {
        "root_source": {"ROOT_NPI": "npi", "ROOT_EDITION": "edition", "ROOT_LABEL": "label"},
        "detail_source": {
            "DETAIL_NPI": "detail_npi",
            "DETAIL_EDITION": "detail_edition",
            "DETAIL_ID": "detail_id",
        },
        "note_source": {"NOTE_NPI": "note_npi", "NOTE_EDITION": "note_edition", "NOTE_ID": "note_id"},
    },
    "query": {"root_fields": [], "order": []},
    "selection_profiles": [],
}


class _Cursor:
    def __init__(self, statement, rows, *, column_ids=None, query_id="synthetic-query", close_error=None) -> None:
        self.column_ids = statement.column_ids if column_ids is None else column_ids
        self.query_id = query_id
        self._rows = list(rows)
        self._close_error = close_error
        self.closed = False
        self.fetch_count = 0

    def fetchone(self):
        self.fetch_count += 1
        return self._rows.pop(0) if self._rows else None

    def close(self) -> None:
        self.closed = True
        if self._close_error is not None:
            raise self._close_error


class _Adapter:
    def __init__(self, rows, *, column_ids=None, query_id="synthetic-query", open_error=None, close_error=None) -> None:
        self._rows = rows
        self._column_ids = column_ids
        self._query_id = query_id
        self._open_error = open_error
        self._close_error = close_error
        self.calls = []
        self.cursor = None

    def open_preflight(self, statement, *, timeout_seconds):
        self.calls.append((statement, timeout_seconds))
        if self._open_error is not None:
            raise self._open_error
        self.cursor = _Cursor(
            statement,
            self._rows(statement),
            column_ids=self._column_ids,
            query_id=self._query_id,
            close_error=self._close_error,
        )
        return self.cursor


class _TokenDroppingConnector(SnowflakeBundleAcquisitionConnector):
    """Synthetic connector that attempts to replace a configured token relation."""

    def prepare_request(self, definition, *, bindings, encoding=DEFAULT_BUNDLE_ENCODING):
        request = super().prepare_request(definition, bindings=bindings, encoding=encoding)
        return SnowflakeBundleRequest(
            definition=request.definition,
            bindings=tuple(
                SnowflakeBundleBinding(
                    stream_id=binding.stream_id,
                    relation=binding.relation,
                    source_snapshot_token_relation=None,
                    selected_field_ids=binding.selected_field_ids,
                    semantic_token_metadata_key=binding.semantic_token_metadata_key,
                )
                for binding in request.bindings
            ),
            encoding=request.encoding,
            capture_limits=request.capture_limits,
        )


class _InvalidRequestConnector(SnowflakeBundleAcquisitionConnector):
    def prepare_request(self, definition, *, bindings, encoding=DEFAULT_BUNDLE_ENCODING):
        return object()


class _InvalidStatementConnector(SnowflakeBundleAcquisitionConnector):
    def build_statement(self, request):
        return object()


def _definition(
    *,
    includes_date=False,
    includes_aliases=True,
    out_of_slot_order=False,
    root_only=False,
) -> CustomImportDefinition:
    document = deepcopy(_DEFINITION_DOCUMENT)
    if root_only:
        document["streams"] = [document["streams"][0]]
        document["schema"]["children"] = []
        document["aliases"] = {"root_source": document["aliases"]["root_source"]}
    if not includes_aliases:
        document["aliases"] = {}
    if out_of_slot_order:
        document["schema"]["root"]["fields"].reverse()
        for child in document["schema"]["children"]:
            child["fields"].reverse()
    if includes_date:
        document["schema"]["root"]["fields"].append({"id": "published_on", "slot": 4, "type": "date", "nullable": True})
    return CustomImportDefinition.from_mapping(document)


def _binding_stream_documents(root_columns):
    return [
        {
            "stream_id": "root_source",
            "relation": ["synthetic", "public", "roots"],
            "source_snapshot_token_relation": ["synthetic", "public", "root_tokens"],
            "semantic_token_metadata_key": "root_snapshot",
            "source_snapshot_token_column_identifier": "ROOT_TOKEN",
            "columns": root_columns,
        },
        {
            "stream_id": "detail_source",
            "relation": ["synthetic", "public", "details"],
            "source_snapshot_token_relation": ["synthetic", "public", "detail_tokens"],
            "semantic_token_metadata_key": "detail_snapshot",
            "source_snapshot_token_column_identifier": "DETAIL_TOKEN",
            "columns": [
                {"field_id": "detail_npi", "column_identifier": "DETAIL_NPI"},
                {"field_id": "detail_edition", "column_identifier": "DETAIL_EDITION"},
                {"field_id": "detail_id", "column_identifier": "DETAIL_ID"},
            ],
        },
        {
            "stream_id": "note_source",
            "relation": ["synthetic", "public", "notes"],
            "source_snapshot_token_relation": ["synthetic", "public", "note_tokens"],
            "semantic_token_metadata_key": "note_snapshot",
            "source_snapshot_token_column_identifier": "NOTE_TOKEN",
            "columns": [
                {"field_id": "note_npi", "column_identifier": "NOTE_NPI"},
                {"field_id": "note_edition", "column_identifier": "NOTE_EDITION"},
                {"field_id": "note_id", "column_identifier": "NOTE_ID"},
            ],
        },
    ]


def _binding(
    definition: CustomImportDefinition,
    *,
    definition_sha256=None,
    uses_query_identity_snapshot=False,
) -> SnowflakeSourceBinding:
    """Build one declared synthetic binding for the selected test definition."""

    root_columns = [
        {"field_id": "npi", "column_identifier": "ROOT_NPI"},
        {"field_id": "edition", "column_identifier": "ROOT_EDITION"},
        {"field_id": "label", "column_identifier": "ROOT_LABEL"},
    ]
    if any(field.field_id == "published_on" for field in definition.root_fields):
        root_columns.append({"field_id": "published_on", "column_identifier": "PUBLISHED_ON"})
    stream_documents = _binding_stream_documents(root_columns)
    if uses_query_identity_snapshot:
        stream_documents[0]["source_snapshot_token_relation"] = None
        stream_documents[0]["source_snapshot_token_column_identifier"] = None
    declared_stream_ids = {stream.stream_id for stream in definition.source_streams}
    return SnowflakeSourceBinding.from_mapping(
        {
            "contract": SOURCE_BINDING_CONTRACT,
            "connector": SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
            "definition_sha256": definition.digest if definition_sha256 is None else definition_sha256,
            "schema_sha256": definition.schema_digest,
            "source_object": {"fingerprint_sha256": "1" * 64, "version": "synthetic-v1"},
            "role": "synthetic_reader",
            "warehouse": "synthetic_load",
            "streams": [stream for stream in stream_documents if stream["stream_id"] in declared_stream_ids],
        }
    )


def _connector(
    definition,
    binding,
    *,
    approved_relations=None,
    connector_type=SnowflakeBundleAcquisitionConnector,
):
    derived_relations, _ = binding.bundle_components(definition)
    return connector_type(
        approved_relations=derived_relations if approved_relations is None else approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: None),
    )


def _bundle_statement(definition, binding):
    connector = _connector(definition, binding)
    approved_relations, bundle_bindings = binding.bundle_components(definition)
    request = connector.prepare_request(definition, bindings=bundle_bindings)
    return connector.build_statement(request), request, approved_relations


def _row(
    statement, kind, *, stream_ordinal=None, stream_id=None, key_ordinal=None, token=None, multiplicity=None, **fields
):
    values = [None] * len(statement.column_ids)
    index_by_column = {name: position for position, name in enumerate(statement.column_ids)}
    values[index_by_column["__ci_preflight_kind"]] = kind
    values[index_by_column["__ci_preflight_stream_ordinal"]] = stream_ordinal
    values[index_by_column["__ci_preflight_stream_id"]] = stream_id
    values[index_by_column["__ci_preflight_key_ordinal"]] = key_ordinal
    values[index_by_column["__ci_preflight_source_snapshot_token"]] = token
    values[index_by_column["__ci_preflight_key_multiplicity"]] = multiplicity
    for field_id, value in fields.items():
        values[index_by_column[field_id]] = value
    return tuple(values)


def _metadata_rows(statement, *, detail_token=_TOKEN):
    return [
        _row(statement, 0, stream_ordinal=1, stream_id="root_source", token=_TOKEN),
        _row(statement, 0, stream_ordinal=2, stream_id="detail_source", token=detail_token),
        _row(statement, 0, stream_ordinal=3, stream_id="note_source", token=_TOKEN),
    ]


def _complete_rows(statement):
    return [
        *_metadata_rows(statement),
        _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=1, npi="1003000126", edition=1),
        _row(statement, 1, stream_ordinal=0, key_ordinal=2, multiplicity=2, npi="1234567893", edition=2),
        _row(statement, 2, stream_ordinal=1, stream_id="root_source", npi="1003000126", edition=1, label="root"),
        _row(
            statement,
            2,
            stream_ordinal=2,
            stream_id="detail_source",
            detail_npi="1003000126",
            detail_edition=1,
            detail_id="detail-a",
        ),
        _row(
            statement,
            2,
            stream_ordinal=3,
            stream_id="note_source",
            note_npi="1003000126",
            note_edition=1,
            note_id="note-a",
        ),
    ]


def _single_root_rows(statement):
    return [
        _row(statement, 0, stream_ordinal=1, stream_id="root_source", token=None),
        _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=1, npi="1003000126", edition=1),
        _row(statement, 2, stream_ordinal=1, stream_id="root_source", npi="1003000126", edition=1, label="root"),
    ]


def _integer_rows(statement, *, root_edition, detail_edition=None):
    detail_edition = root_edition if detail_edition is None else detail_edition
    return [
        *_metadata_rows(statement),
        _row(
            statement,
            1,
            stream_ordinal=0,
            key_ordinal=1,
            multiplicity=1,
            npi="1003000126",
            edition=root_edition,
        ),
        _row(
            statement,
            2,
            stream_ordinal=1,
            stream_id="root_source",
            npi="1003000126",
            edition=root_edition,
            label="root",
        ),
        _row(
            statement,
            2,
            stream_ordinal=2,
            stream_id="detail_source",
            detail_npi="1003000126",
            detail_edition=detail_edition,
            detail_id="detail-a",
        ),
        _row(
            statement,
            2,
            stream_ordinal=3,
            stream_id="note_source",
            note_npi="1003000126",
            note_edition=root_edition,
            note_id="note-a",
        ),
    ]


def _run(rows, *, limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2)):
    definition = _definition()
    binding = _binding(definition)
    adapter = _Adapter(rows)
    result = preflight_snowflake_bundle(definition, binding, _connector(definition, binding), adapter, limits=limits)
    return result, adapter


def test_preflight_keeps_root_sentinel_as_lower_bound():
    result, adapter = _run(_complete_rows)

    assert result.status == "complete"
    assert result.sample is not None
    assert result.sample.source_snapshot_token == _TOKEN
    assert len(result.sample.families) == 1
    assert result.rejection_diagnostics == ()
    assert result.sample.families[0].root_key == ("1003000126", 1)
    assert tuple((item.stream_id, item.observed_rows, item.precision) for item in result.observations) == (
        ("root_source", 3, "lower_bound"),
        ("detail_source", 1, "exact"),
        ("note_source", 1, "exact"),
    )
    statement, timeout_seconds = adapter.calls[0]
    assert timeout_seconds == 30
    assert 'COUNT(*) AS "__ci_preflight_key_multiplicity"' in statement.sql
    assert '"DETAIL_EDITION" = "__ci_preflight_selected_keys"."edition"' in statement.sql
    assert '"NOTE_EDITION" = "__ci_preflight_selected_keys"."edition"' in statement.sql
    assert "LIMIT 2" in statement.sql
    assert "LIMIT 3" in statement.sql
    assert "DROP" not in statement.sql
    assert adapter.cursor.closed


def test_preflight_normalizes_out_of_slot_stream_fields_before_mapping_comparison():
    definition = _definition(out_of_slot_order=True)
    binding = _binding(definition)
    adapter = _Adapter(_complete_rows)

    result = preflight_snowflake_bundle(
        definition,
        binding,
        _connector(definition, binding),
        adapter,
        limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2),
    )

    assert result.status == "complete"
    assert tuple(binding.selected_field_ids for binding in adapter.calls[0][0].bundle_bindings) == (
        ("npi", "edition", "label"),
        ("detail_npi", "detail_edition", "detail_id"),
        ("note_npi", "note_edition", "note_id"),
    )


def test_preflight_discards_the_sample_when_a_child_stream_reaches_its_sentinel():
    def rows(statement):
        result = _complete_rows(statement)
        result.insert(
            -1,
            _row(
                statement,
                2,
                stream_ordinal=2,
                stream_id="detail_source",
                detail_npi="1003000126",
                detail_edition=1,
                detail_id="detail-b",
            ),
        )
        return result

    result, _adapter = _run(rows, limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=1))

    assert result.status == "unavailable"
    assert result.unavailable_reason == "child_limit_reached"
    assert result.sample is None
    assert result.observations[1].precision == "lower_bound"
    assert result.observations[1].observed_rows == 2


def test_preflight_discards_inconsistent_snapshot_tokens_without_returning_rows():
    result, _adapter = _run(
        lambda statement: [*_metadata_rows(statement, detail_token="other-snapshot"), *_complete_rows(statement)[3:]]
    )

    assert result.status == "unavailable"
    assert result.unavailable_reason == "snapshot_invalid"
    assert result.sample is None


def test_preflight_discards_a_duplicate_selected_root_key_before_family_admission():
    def rows(statement):
        return [
            *_metadata_rows(statement),
            _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=2, npi="1003000126", edition=1),
        ]

    result, _adapter = _run(rows)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "duplicate_root_key"
    assert result.sample is None
    assert result.observations[0].observed_rows == 2


def test_preflight_discards_missing_root_data_for_a_selected_key():
    def rows(statement):
        return [
            *_metadata_rows(statement),
            _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=1, npi="1003000126", edition=1),
        ]

    result, _adapter = _run(rows)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "root_data_incomplete"
    assert result.sample is None


def test_preflight_discards_a_selected_root_key_with_a_missing_component():
    def rows(statement):
        return [
            *_metadata_rows(statement),
            _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=1, npi=None, edition=1),
        ]

    result, _adapter = _run(rows)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "root_key_missing"
    assert result.sample is None


def test_preflight_accepts_integer_values_at_the_capture_boundaries():
    for edition in (MIN_SCALAR_INTEGER, MAX_SCALAR_INTEGER):
        result, _adapter = _run(lambda statement, edition=edition: _integer_rows(statement, root_edition=edition))

        assert result.status == "complete"
        assert result.validation.runtime_supported


def test_preflight_rejects_root_and_child_integers_outside_capture_boundaries():
    for root_edition, detail_edition in (
        (MIN_SCALAR_INTEGER - 1, MIN_SCALAR_INTEGER - 1),
        (MAX_SCALAR_INTEGER + 1, MAX_SCALAR_INTEGER + 1),
        (1, MIN_SCALAR_INTEGER - 1),
        (1, MAX_SCALAR_INTEGER + 1),
    ):
        result, _adapter = _run(
            lambda statement, root_edition=root_edition, detail_edition=detail_edition: _integer_rows(
                statement,
                root_edition=root_edition,
                detail_edition=detail_edition,
            )
        )

        assert result.status == "unavailable"
        assert result.unavailable_reason == "runtime_integer_unsupported"
        assert not result.validation.runtime_supported
        assert result.sample is None


def test_preflight_rejects_repeated_key_ordinals_before_the_byte_cap():
    def rows(statement):
        key_row = _row(
            statement,
            1,
            stream_ordinal=0,
            key_ordinal=1,
            multiplicity=1,
            npi="1003000126",
            edition=1,
        )
        return [*_metadata_rows(statement), *[key_row for _ in range(101)]]

    result, adapter = _run(rows)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "result_invalid"
    assert adapter.cursor.fetch_count == 5
    assert adapter.cursor.closed


def test_preflight_retains_one_private_generic_rejection_per_selected_root():
    def rows(statement):
        return [
            *_metadata_rows(statement),
            _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=1, npi="123456", edition=1),
            _row(statement, 2, stream_ordinal=1, stream_id="root_source", npi="123456", edition=1, label="root"),
        ]

    result, _adapter = _run(rows)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "family_invalid"
    assert result.sample is None
    assert tuple((item.root_key, item.code) for item in result.rejection_diagnostics) == (
        (("123456", 1), "entity_binding_invalid"),
    )
    assert "123456" not in repr(result)


def test_preflight_keeps_metadata_and_returns_an_empty_complete_sample():
    result, _adapter = _run(_metadata_rows)

    assert result.status == "complete"
    assert result.sample is not None
    assert result.sample.families == ()
    assert tuple((item.observed_rows, item.precision) for item in result.observations) == (
        (0, "exact"),
        (0, "exact"),
        (0, "exact"),
    )


def test_preflight_marks_observations_unknown_after_the_client_byte_limit():
    result, _adapter = _run(
        _complete_rows,
        limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2, maximum_total_bytes=1),
    )

    assert result.status == "unavailable"
    assert result.unavailable_reason == "byte_limit"
    assert {item.precision for item in result.observations} == {"unknown"}


def test_preflight_discards_work_that_exceeds_the_elapsed_limit():
    definition = _definition()
    binding = _binding(definition)
    adapter = _Adapter(_complete_rows)
    clock_values = iter((0.0, 2.0))

    result = preflight_snowflake_bundle(
        definition,
        binding,
        _connector(definition, binding),
        adapter,
        limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2, maximum_elapsed_seconds=1),
        clock=lambda: next(clock_values),
    )

    assert result.status == "unavailable"
    assert result.unavailable_reason == "query_timeout"
    assert adapter.calls[0][1] == 1
    assert adapter.cursor.closed


def test_preflight_rejects_mismatched_mapping_without_opening_the_adapter():
    definition = _definition()
    binding = _binding(definition, definition_sha256="0" * 64)
    adapter = _Adapter(lambda _statement: [])

    result = preflight_snowflake_bundle(definition, binding, _connector(definition, _binding(definition)), adapter)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "mapping_invalid"
    assert result.validation.definition_valid
    assert not result.validation.mapping_valid
    assert not adapter.calls


def test_preflight_rejects_a_connector_request_that_drops_the_configured_token_relation():
    definition = _definition(root_only=True)
    binding = _binding(definition)
    approved_relations, _ = binding.bundle_components(definition)
    connector = _TokenDroppingConnector(
        approved_relations=approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: None),
    )
    adapter = _Adapter(_single_root_rows)

    result = preflight_snowflake_bundle(definition, binding, connector, adapter)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "mapping_invalid"
    assert result.validation.definition_valid
    assert not result.validation.mapping_valid
    assert not adapter.calls


def test_preflight_rejects_connector_columns_that_do_not_match_the_source_binding():
    definition = _definition(includes_aliases=False)
    binding = _binding(definition)
    approved_relations, _ = binding.bundle_components(definition)
    alternate_relations = tuple(
        SnowflakeApprovedRelation(
            relation=approved.relation,
            columns=tuple(
                SnowflakeDeclaredColumn(column.field_id, "ROOT_NPI_ALT") if column.field_id == "npi" else column
                for column in approved.columns
            ),
        )
        for approved in approved_relations
    )
    adapter = _Adapter(lambda _statement: [])

    result = preflight_snowflake_bundle(
        definition,
        binding,
        _connector(definition, binding, approved_relations=alternate_relations),
        adapter,
    )

    assert result.status == "unavailable"
    assert result.unavailable_reason == "mapping_invalid"
    assert result.validation.definition_valid
    assert not result.validation.mapping_valid
    assert not adapter.calls


def test_preflight_reports_date_fields_as_unsupported_by_the_current_capture_runtime():
    definition = _definition(includes_date=True)
    binding = _binding(definition)
    adapter = _Adapter(lambda _statement: [])

    result = preflight_snowflake_bundle(definition, binding, _connector(definition, binding), adapter)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "runtime_type_unsupported"
    assert result.validation.definition_valid and result.validation.mapping_valid
    assert not result.validation.runtime_supported
    assert not adapter.calls


def test_preflight_value_objects_reject_invalid_bounds_and_states():
    for limit_name in (
        "maximum_root_keys",
        "maximum_child_rows",
        "maximum_total_bytes",
        "maximum_elapsed_seconds",
    ):
        with pytest.raises(SnowflakePreflightError):
            SnowflakePreflightLimits(**{limit_name: 0})

    with pytest.raises(SnowflakePreflightError):
        SnowflakePreflightStreamObservation("", 0, 0, "exact")
    with pytest.raises(SnowflakePreflightError):
        SnowflakePreflightStreamObservation("root_source", True, 0, "exact")
    with pytest.raises(SnowflakePreflightError):
        SnowflakePreflightStreamObservation("root_source", 0, 0, "estimated")
    with pytest.raises(SnowflakePreflightError):
        SnowflakePreflightRejectionDiagnostic((), "entity_binding_invalid")
    with pytest.raises(SnowflakePreflightError):
        SnowflakePreflightRejectionDiagnostic(("root",), "Invalid")

    result_fields_by_name = {
        "definition_sha256": None,
        "schema_sha256": None,
        "source_binding_sha256": None,
        "validation": SnowflakePreflightValidation(True, True, True),
        "observations": (),
        "observed_bytes": 0,
    }
    for result_overrides in (
        {"status": "partial"},
        {"status": "unavailable", "unavailable_reason": "result_invalid", "observed_bytes": True},
        {"status": "complete"},
        {"status": "unavailable"},
        {"status": "unavailable", "unavailable_reason": "result_invalid", "rejection_diagnostics": []},
    ):
        with pytest.raises(SnowflakePreflightError):
            SnowflakePreflightResult(**{**result_fields_by_name, **result_overrides})


def test_preflight_reports_invalid_definition_and_input_types_without_opening_the_adapter():
    definition = _definition()
    binding = _binding(definition)
    adapter = _Adapter(lambda _statement: [])
    connector = _connector(definition, binding)

    invalid_definition = preflight_snowflake_bundle(object(), binding, connector, adapter)
    invalid_limits = preflight_snowflake_bundle(definition, binding, connector, adapter, limits=object())
    invalid_binding = preflight_snowflake_bundle(definition, object(), connector, adapter)
    invalid_connector = preflight_snowflake_bundle(definition, binding, object(), adapter)

    assert invalid_definition.unavailable_reason == "definition_invalid"
    assert invalid_limits.unavailable_reason == "limits_invalid"
    assert invalid_binding.unavailable_reason == "mapping_invalid"
    assert invalid_connector.unavailable_reason == "mapping_invalid"
    assert not adapter.calls


def test_preflight_rejects_tampered_definition_and_binding_seals():
    definition = _definition()
    binding = _binding(definition)
    adapter = _Adapter(lambda _statement: [])

    malformed_definition = _definition()
    object.__setattr__(malformed_definition, "canonical", "{")
    stale_definition = _definition()
    object.__setattr__(stale_definition, "schema_digest", "0" * 64)
    stale_binding = _binding(definition)
    object.__setattr__(stale_binding, "warehouse", "synthetic_other")

    malformed_result = preflight_snowflake_bundle(
        malformed_definition,
        binding,
        _connector(definition, binding),
        adapter,
    )
    stale_definition_result = preflight_snowflake_bundle(
        stale_definition,
        binding,
        _connector(definition, binding),
        adapter,
    )
    stale_binding_result = preflight_snowflake_bundle(
        definition,
        stale_binding,
        _connector(definition, binding),
        adapter,
    )

    assert malformed_result.unavailable_reason == "definition_invalid"
    assert stale_definition_result.unavailable_reason == "definition_invalid"
    assert stale_binding_result.unavailable_reason == "mapping_invalid"
    assert not adapter.calls


def test_preflight_rejects_connector_outputs_without_declared_bundle_types():
    definition = _definition()
    binding = _binding(definition)
    adapter = _Adapter(lambda _statement: [])

    invalid_request = preflight_snowflake_bundle(
        definition,
        binding,
        _connector(definition, binding, connector_type=_InvalidRequestConnector),
        adapter,
    )
    invalid_statement = preflight_snowflake_bundle(
        definition,
        binding,
        _connector(definition, binding, connector_type=_InvalidStatementConnector),
        adapter,
    )

    assert invalid_request.unavailable_reason == "mapping_invalid"
    assert invalid_statement.unavailable_reason == "mapping_invalid"
    assert not adapter.calls


def test_preflight_statement_rejects_unsealed_bundle_and_limit_inputs():
    definition = _definition()
    binding = _binding(definition)
    bundle_statement, _request, _approved_relations = _bundle_statement(definition, binding)
    limits = SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2)
    lookalike_limits = SimpleNamespace(
        maximum_root_keys=1,
        maximum_child_rows=2,
        maximum_total_bytes=limits.maximum_total_bytes,
        maximum_elapsed_seconds=limits.maximum_elapsed_seconds,
    )

    with pytest.raises(SnowflakePreflightError):
        SnowflakePreflightStatement(bundle_statement=bundle_statement, limits=lookalike_limits)
    with pytest.raises(SnowflakePreflightError):
        SnowflakePreflightStatement(bundle_statement=object(), limits=limits)


def test_preflight_mapping_rejects_query_identity_snapshot_columns():
    definition = _definition(root_only=True)
    binding = _binding(definition, uses_query_identity_snapshot=True)
    statement, request, approved_relations = _bundle_statement(definition, binding)

    snowflake_preflight._validate_statement_mapping(statement, request, request, approved_relations)

    statement_with_source_token = SimpleNamespace(
        request=request,
        selected_columns_by_stream=statement.selected_columns_by_stream,
        source_snapshot_token_columns_by_stream=(SnowflakeDeclaredColumn("root_snapshot", "ROOT_TOKEN"),),
    )
    with pytest.raises(ValueError):
        snowflake_preflight._validate_statement_mapping(
            statement_with_source_token,
            request,
            request,
            approved_relations,
        )


def test_preflight_mapping_rejects_missing_semantic_or_approved_columns():
    definition = _definition()
    binding = _binding(definition)
    statement, request, approved_relations = _bundle_statement(definition, binding)
    first_binding = request.bindings[0]
    missing_semantic_binding = SnowflakeBundleBinding(
        stream_id=first_binding.stream_id,
        relation=first_binding.relation,
        source_snapshot_token_relation=first_binding.source_snapshot_token_relation,
        selected_field_ids=first_binding.selected_field_ids,
        semantic_token_metadata_key=None,
    )
    missing_semantic_request = SimpleNamespace(
        bindings=(missing_semantic_binding, *request.bindings[1:]),
    )
    missing_semantic_statement = SimpleNamespace(
        request=missing_semantic_request,
        selected_columns_by_stream=statement.selected_columns_by_stream,
        source_snapshot_token_columns_by_stream=statement.source_snapshot_token_columns_by_stream,
    )
    mismatched_snapshot_statement = SimpleNamespace(
        request=request,
        selected_columns_by_stream=statement.selected_columns_by_stream,
        source_snapshot_token_columns_by_stream=(None, *statement.source_snapshot_token_columns_by_stream[1:]),
    )
    approved_by_relation = {approved.relation.parts: approved for approved in approved_relations}

    with pytest.raises(ValueError):
        snowflake_preflight._validate_statement_mapping(
            missing_semantic_statement,
            missing_semantic_request,
            missing_semantic_request,
            approved_relations,
        )
    with pytest.raises(ValueError):
        snowflake_preflight._validate_statement_mapping(
            mismatched_snapshot_statement,
            request,
            request,
            approved_relations,
        )
    with pytest.raises(ValueError):
        snowflake_preflight._approved_columns({}, request.bindings[1].relation, ("detail_npi",))
    with pytest.raises(ValueError):
        snowflake_preflight._approved_columns(
            approved_by_relation,
            request.bindings[0].relation,
            ("missing_field",),
        )


def test_preflight_reports_adapter_open_schema_and_cleanup_failures():
    definition = _definition()
    binding = _binding(definition)
    connector = _connector(definition, binding)

    open_failure_adapter = _Adapter(_complete_rows, open_error=RuntimeError("synthetic open failure"))
    open_failure = preflight_snowflake_bundle(definition, binding, connector, open_failure_adapter)
    schema_failure_adapter = _Adapter(_complete_rows, column_ids=())
    schema_failure = preflight_snowflake_bundle(definition, binding, connector, schema_failure_adapter)
    cleanup_failure_adapter = _Adapter(_complete_rows, close_error=RuntimeError("synthetic close failure"))
    cleanup_failure = preflight_snowflake_bundle(definition, binding, connector, cleanup_failure_adapter)

    assert open_failure.unavailable_reason == "query_unavailable"
    assert schema_failure.unavailable_reason == "result_schema_invalid"
    assert cleanup_failure.unavailable_reason == "query_unavailable"
    assert not open_failure_adapter.cursor
    assert schema_failure_adapter.cursor.closed
    assert cleanup_failure_adapter.cursor.closed


@pytest.mark.parametrize(
    ("clock_values", "expected_fetches"),
    (((0.0, 0.0, 2.0), 0), ((0.0, 0.0, 0.0, 2.0), 1)),
    ids=("before-fetch", "after-fetch"),
)
def test_preflight_stops_row_processing_when_the_elapsed_limit_expires(clock_values, expected_fetches):
    definition = _definition()
    binding = _binding(definition)
    adapter = _Adapter(_complete_rows)
    clock = iter(clock_values)

    result = preflight_snowflake_bundle(
        definition,
        binding,
        _connector(definition, binding),
        adapter,
        limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2, maximum_elapsed_seconds=1),
        clock=lambda: next(clock),
    )

    assert result.status == "unavailable"
    assert result.unavailable_reason == "query_timeout"
    assert adapter.cursor.fetch_count == expected_fetches
    assert adapter.cursor.closed


@pytest.mark.parametrize(
    "rows",
    (
        lambda _statement: ["not-a-result-row"],
        lambda statement: [_row(statement, 0, stream_ordinal=1, stream_id="root_source", token=object())],
        lambda statement: [_row(statement, True)],
        lambda statement: [_row(statement, 3)],
    ),
    ids=("non-sequence", "unsupported-scalar", "boolean-kind", "unknown-kind"),
)
def test_preflight_rejects_malformed_generated_rows(rows):
    result, adapter = _run(rows)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "result_invalid"
    assert result.sample is None
    assert adapter.cursor.closed


def test_preflight_rejects_metadata_rows_with_an_invalid_source_ordinal():
    result, _adapter = _run(
        lambda statement: [_row(statement, 0, stream_ordinal=99, stream_id="root_source", token=_TOKEN)]
    )

    assert result.status == "unavailable"
    assert result.unavailable_reason == "result_invalid"


def test_preflight_rejects_stream_data_outside_the_declared_wire_shape():
    def out_of_scope_field_rows(statement):
        return [
            *_metadata_rows(statement),
            _row(
                statement,
                2,
                stream_ordinal=1,
                stream_id="root_source",
                npi="1003000126",
                edition=1,
                label="root",
                detail_id="unexpected",
            ),
        ]

    def data_rows_with_metadata_fields(statement):
        return [
            *_metadata_rows(statement),
            _row(
                statement,
                2,
                stream_ordinal=1,
                stream_id="root_source",
                key_ordinal=1,
                npi="1003000126",
                edition=1,
                label="root",
            ),
        ]

    out_of_scope_result, _adapter = _run(out_of_scope_field_rows)
    metadata_result, _adapter = _run(data_rows_with_metadata_fields)

    assert out_of_scope_result.status == "unavailable"
    assert out_of_scope_result.unavailable_reason == "result_invalid"
    assert metadata_result.status == "unavailable"
    assert metadata_result.unavailable_reason == "result_invalid"


def test_preflight_rejects_data_rows_that_exceed_selected_family_bounds():
    def extra_root_rows(statement):
        root = _row(
            statement,
            2,
            stream_ordinal=1,
            stream_id="root_source",
            npi="1003000126",
            edition=1,
            label="root",
        )
        return [
            *_metadata_rows(statement),
            _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=1, npi="1003000126", edition=1),
            root,
            root,
        ]

    def excessive_child_rows(statement):
        detail_rows = [
            _row(
                statement,
                2,
                stream_ordinal=2,
                stream_id="detail_source",
                detail_npi="1003000126",
                detail_edition=1,
                detail_id=f"detail-{ordinal}",
            )
            for ordinal in range(3)
        ]
        return [
            *_metadata_rows(statement),
            _row(statement, 1, stream_ordinal=0, key_ordinal=1, multiplicity=1, npi="1003000126", edition=1),
            _row(
                statement,
                2,
                stream_ordinal=1,
                stream_id="root_source",
                npi="1003000126",
                edition=1,
                label="root",
            ),
            *detail_rows,
        ]

    root_result, _adapter = _run(extra_root_rows)
    child_result, _adapter = _run(
        excessive_child_rows,
        limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=1),
    )

    assert root_result.unavailable_reason == "root_data_incomplete"
    assert child_result.unavailable_reason == "result_invalid"


def test_preflight_rejects_results_that_omit_required_stream_metadata():
    result, _adapter = _run(lambda statement: _complete_rows(statement)[3:])

    assert result.status == "unavailable"
    assert result.unavailable_reason == "result_invalid"


def test_preflight_uses_query_identity_only_for_one_root_stream():
    definition = _definition(root_only=True)
    binding = _binding(definition, uses_query_identity_snapshot=True)
    connector = _connector(definition, binding)
    valid_adapter = _Adapter(_single_root_rows)
    invalid_adapter = _Adapter(_single_root_rows, query_id=None)

    valid_result = preflight_snowflake_bundle(definition, binding, connector, valid_adapter)
    invalid_result = preflight_snowflake_bundle(definition, binding, connector, invalid_adapter)

    assert valid_result.status == "complete"
    assert valid_result.sample is not None
    assert valid_result.sample.source_snapshot_token != _TOKEN
    assert invalid_result.status == "unavailable"
    assert invalid_result.unavailable_reason == "snapshot_invalid"


def test_preflight_rejects_metadata_tokens_when_query_identity_is_authoritative():
    definition = _definition(root_only=True)
    binding = _binding(definition, uses_query_identity_snapshot=True)

    def rows(statement):
        return [
            _row(statement, 0, stream_ordinal=1, stream_id="root_source", token=_TOKEN),
            *_single_root_rows(statement)[1:],
        ]

    adapter = _Adapter(rows)
    result = preflight_snowflake_bundle(definition, binding, _connector(definition, binding), adapter)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "snapshot_invalid"


def test_preflight_never_admits_a_missing_final_snapshot_token(monkeypatch):
    definition = _definition()
    binding = _binding(definition)
    limits = SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2)
    prepared = snowflake_preflight._prepare_preflight(definition, binding, _connector(definition, binding), limits)
    observed = snowflake_preflight._ObservedRows(
        metadata_tokens={},
        key_candidates=[],
        records_by_stream={stream.stream_id: [] for stream in definition.source_streams},
        bytes_by_stream={stream.stream_id: 0 for stream in definition.source_streams},
        observed_bytes=0,
    )
    monkeypatch.setattr(snowflake_preflight, "_finalization_inputs", lambda *_args: (None, (), None))

    result = snowflake_preflight._finalize(
        prepared,
        "synthetic-query",
        observed,
        snowflake_preflight._unknown_observations(definition),
    )

    assert result.status == "unavailable"
    assert result.unavailable_reason == "snapshot_invalid"


def test_preflight_returns_no_family_when_family_assembly_raises(monkeypatch):
    def reject_family(*_args):
        raise ValueError("synthetic family failure")

    monkeypatch.setattr(snowflake_preflight, "assemble_root_families", reject_family)

    result, _adapter = _run(_complete_rows)

    assert result.status == "unavailable"
    assert result.unavailable_reason == "family_invalid"
    assert result.sample is None


def test_preflight_retains_only_selected_generic_rejection_diagnostics():
    selected_keys = (("selected", 1),)
    diagnostics = snowflake_preflight._rejection_diagnostics(
        (
            FamilyRejection(None, "entity_binding_invalid"),
            FamilyRejection(("outside", 1), "entity_binding_invalid"),
            FamilyRejection(("selected", 1), "Invalid"),
            FamilyRejection(("selected", 1), "z_code"),
            FamilyRejection(("selected", 1), "a_code"),
        ),
        selected_keys,
    )

    assert tuple((diagnostic.root_key, diagnostic.code) for diagnostic in diagnostics) == ((("selected", 1), "a_code"),)


def test_preflight_defensively_rejects_malformed_selected_key_candidates():
    definition = _definition()
    one_root_limit = SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=2)
    two_root_limit = SnowflakePreflightLimits(maximum_root_keys=2, maximum_child_rows=2)

    assert snowflake_preflight._selected_keys(definition, [(2, 1, ("root", 1))], one_root_limit)[0] == "result_invalid"
    assert (
        snowflake_preflight._selected_keys(
            definition,
            [(1, 1, ("first", 1)), (2, 1, ("second", 1)), (3, 1, ("third", 1))],
            one_root_limit,
        )[0]
        == "result_invalid"
    )
    assert (
        snowflake_preflight._selected_keys(
            definition,
            [(1, 1, ("duplicate", 1)), (2, 1, ("duplicate", 1))],
            two_root_limit,
        )[0]
        == "result_invalid"
    )


def test_preflight_counts_supported_wire_scalars_and_rejects_unsafe_values():
    assert snowflake_preflight._row_values("not-a-row", 1) is None
    assert snowflake_preflight._row_values(("one",), 2) is None
    assert snowflake_preflight._row_bytes((object(),)) is None
    assert snowflake_preflight._value_bytes(True) == 1
    assert snowflake_preflight._value_bytes(Decimal("12.5")) == 4
    assert snowflake_preflight._value_bytes(datetime(2026, 1, 2, 3, 4, 5)) == 19
    assert snowflake_preflight._value_bytes(date(2026, 1, 2)) == 10
    with pytest.raises(TypeError):
        snowflake_preflight._value_bytes(object())
    assert not snowflake_preflight._has_valid_key(())
    assert not snowflake_preflight._has_valid_key(([],))
    assert not snowflake_preflight._is_generic_rejection_code("Invalid")
    with pytest.raises(SnowflakePreflightError):
        snowflake_preflight._clock(lambda: True)
