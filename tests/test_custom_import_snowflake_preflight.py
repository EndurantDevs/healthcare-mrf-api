# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for bounded custom-import Snowflake preflight."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import MAX_SCALAR_INTEGER, MIN_SCALAR_INTEGER
from process.custom_import.snowflake import SnowflakeApprovedRelation, SnowflakeDeclaredColumn
from process.custom_import.snowflake_bundle import (
    DEFAULT_BUNDLE_ENCODING,
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleBinding,
    SnowflakeBundleRequest,
)
from process.custom_import.snowflake_preflight import SnowflakePreflightLimits, preflight_snowflake_bundle
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
    def __init__(self, statement, rows, *, column_ids=None, query_id="synthetic-query") -> None:
        self.column_ids = statement.column_ids if column_ids is None else column_ids
        self.query_id = query_id
        self._rows = list(rows)
        self.closed = False
        self.fetch_count = 0

    def fetchone(self):
        self.fetch_count += 1
        return self._rows.pop(0) if self._rows else None

    def close(self) -> None:
        self.closed = True


class _Adapter:
    def __init__(self, rows) -> None:
        self._rows = rows
        self.calls = []
        self.cursor = None

    def open_preflight(self, statement, *, timeout_seconds):
        self.calls.append((statement, timeout_seconds))
        self.cursor = _Cursor(statement, self._rows(statement))
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


def _binding(definition: CustomImportDefinition, *, definition_sha256=None) -> SnowflakeSourceBinding:
    root_columns = [
        {"field_id": "npi", "column_identifier": "ROOT_NPI"},
        {"field_id": "edition", "column_identifier": "ROOT_EDITION"},
        {"field_id": "label", "column_identifier": "ROOT_LABEL"},
    ]
    if any(field.field_id == "published_on" for field in definition.root_fields):
        root_columns.append({"field_id": "published_on", "column_identifier": "PUBLISHED_ON"})
    stream_documents = [
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


def _connector(definition, binding, *, approved_relations=None):
    derived_relations, _ = binding.bundle_components(definition)
    return SnowflakeBundleAcquisitionConnector(
        approved_relations=derived_relations if approved_relations is None else approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: None),
    )


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
