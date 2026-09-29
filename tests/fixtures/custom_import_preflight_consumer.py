# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic isolated consumer for the pure Snowflake preflight wheel."""

from __future__ import annotations

import importlib
import importlib.util
import pathlib
import sys
from types import SimpleNamespace

_BLOCKED_MODULES = {"arq", "db", "pyarrow", "snowflake", "sqlalchemy"}
_TARGET_DIRECTORY = pathlib.Path(sys.argv[1]).resolve()


class BlockedImports:
    """Reject imports that an isolated preflight consumer cannot carry."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname.partition(".")[0] in _BLOCKED_MODULES:
            raise AssertionError(f"unexpected dependency import: {fullname}")
        return None


class Cursor:
    """Provide one bounded synthetic result for the generated preflight statement."""

    def __init__(self, statement):
        self.column_ids = statement.column_ids
        self.query_id = "synthetic-query"
        self.rows = [
            _row(statement, 0, __ci_preflight_stream_ordinal=1, __ci_preflight_stream_id="root_source"),
            _row(
                statement,
                1,
                __ci_preflight_stream_ordinal=0,
                __ci_preflight_key_ordinal=1,
                __ci_preflight_key_multiplicity=1,
                npi="1003000126",
                edition=1,
            ),
            _row(
                statement,
                2,
                __ci_preflight_stream_ordinal=1,
                __ci_preflight_stream_id="root_source",
                npi="1003000126",
                edition=1,
                label="sample",
            ),
        ]
        self.closed = False

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def close(self):
        self.closed = True


class Adapter:
    """Validate actual cursor metadata before returning the synthetic cursor."""

    def __init__(self):
        self.cursor = None
        self.statement = None

    def open_preflight(self, statement, *, timeout_seconds):
        assert timeout_seconds == 30
        validate_preflight_result_schema(statement, _metadata(statement))
        self.statement = statement
        self.cursor = Cursor(statement)
        return self.cursor


class Credentials:
    """Count forbidden acquisition-side credential access."""

    def __init__(self):
        self.calls = 0

    def load_key_pair(self):
        self.calls += 1
        raise AssertionError("credentials must not be loaded")


class BundleAdapter:
    """Count forbidden acquisition-side adapter access."""

    def __init__(self):
        self.calls = 0

    def fetch_bundle(self, statement, credentials):
        self.calls += 1
        raise AssertionError("adapter must not be called")


def _row(statement, kind, **values_by_column):
    values_by_column = {"__ci_preflight_kind": kind, **values_by_column}
    return tuple(values_by_column.get(name) for name in statement.column_ids)


def _metadata(statement):
    type_details_by_field = {
        "npi": ("TEXT", None, None),
        "edition": ("FIXED", 38, 0),
        "label": ("TEXT", None, None),
    }
    fixed_columns = {
        "__ci_preflight_kind",
        "__ci_preflight_stream_ordinal",
        "__ci_preflight_key_ordinal",
        "__ci_preflight_key_multiplicity",
    }
    metadata_items = []
    for name in statement.column_ids:
        type_name, precision, scale = type_details_by_field.get(name, ("TEXT", None, None))
        if name in fixed_columns:
            type_name, precision, scale = "FIXED", 38, 0
        metadata_items.append(SimpleNamespace(name=name, type_name=type_name, precision=precision, scale=scale))
    return tuple(metadata_items)


def _definition_document():
    return {
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
            }
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
            "children": [],
        },
        "aliases": {"root_source": {"ROOT_NPI": "npi", "ROOT_EDITION": "edition", "ROOT_LABEL": "label"}},
        "query": {"root_fields": [], "order": []},
        "selection_profiles": [],
    }


def _binding_document(definition):
    return {
        "contract": SOURCE_BINDING_CONTRACT,
        "connector": SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
        "definition_sha256": definition.digest,
        "schema_sha256": definition.schema_digest,
        "source_object": {"fingerprint_sha256": "1" * 64, "version": "synthetic-v1"},
        "role": "synthetic_reader",
        "warehouse": "synthetic_load",
        "streams": [
            {
                "stream_id": "root_source",
                "relation": ["synthetic", "public", "roots"],
                "source_snapshot_token_relation": None,
                "semantic_token_metadata_key": "root_snapshot",
                "source_snapshot_token_column_identifier": None,
                "columns": [
                    {"field_id": "npi", "column_identifier": "ROOT_NPI"},
                    {"field_id": "edition", "column_identifier": "ROOT_EDITION"},
                    {"field_id": "label", "column_identifier": "ROOT_LABEL"},
                ],
            }
        ],
    }


assert importlib.util.find_spec("process") is None
_native_source = pathlib.Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else None
if _native_source is None:
    sys.meta_path.insert(0, BlockedImports())
else:
    sys.path.insert(0, str(_native_source))
    importlib.import_module("process.custom_import.definition")
sys.path.insert(0, str(_TARGET_DIRECTORY))

_MODULE_NAMES = (
    "custom_import_preflight._source_text",
    "custom_import_preflight.definition",
    "custom_import_preflight.family",
    "custom_import_preflight.capture_limits",
    "custom_import_preflight.snowflake",
    "custom_import_preflight.snowflake_bundle",
    "custom_import_preflight.snowflake_binding",
    "custom_import_preflight.snowflake_preflight",
    "custom_import_preflight.snowflake_preflight_schema",
)
_modules = tuple(importlib.import_module(name) for name in _MODULE_NAMES)
assert all(pathlib.Path(module.__file__).resolve().is_relative_to(_TARGET_DIRECTORY) for module in _modules)
assert all(
    pathlib.Path(module.__file__).resolve().is_relative_to(_TARGET_DIRECTORY)
    for name, module in sys.modules.items()
    if name == "custom_import_preflight" or name.startswith("custom_import_preflight.")
)

from custom_import_preflight.definition import CustomImportDefinition
from custom_import_preflight.snowflake import SnowflakeConnectorError
from custom_import_preflight.snowflake_binding import (
    SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
    SOURCE_BINDING_CONTRACT,
    SnowflakeSourceBinding,
)
from custom_import_preflight.snowflake_bundle import (
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleError,
    SnowflakeBundleStatementBuilder,
)
from custom_import_preflight.snowflake_preflight import (
    SnowflakePreflightLimits,
    preflight_snowflake_bundle,
)
from custom_import_preflight.snowflake_preflight_schema import validate_preflight_result_schema

_definition = CustomImportDefinition.from_mapping(_definition_document())
_binding = SnowflakeSourceBinding.from_mapping(_binding_document(_definition))
_approved_relations, _bundle_bindings = _binding.bundle_components(_definition)
_builder = SnowflakeBundleStatementBuilder(approved_relations=_approved_relations)
_request = _builder.prepare_request(_definition, bindings=_bundle_bindings)
_adapter = Adapter()
_result = preflight_snowflake_bundle(
    _definition,
    _binding,
    _builder,
    _adapter,
    limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=1),
)
assert _result.status == "complete"
assert _adapter.cursor.closed

_invalid_metadata = list(_metadata(_adapter.statement))
_invalid_metadata[-1] = SimpleNamespace(name="label", type_name=None, type_code=True)
try:
    validate_preflight_result_schema(_adapter.statement, tuple(_invalid_metadata))
except SnowflakeConnectorError:
    pass
else:
    raise AssertionError("missing type metadata was accepted")

_credentials = Credentials()
_bundle_adapter = BundleAdapter()
_connector = SnowflakeBundleAcquisitionConnector(
    approved_relations=_approved_relations,
    credential_provider=_credentials,
    adapter=_bundle_adapter,
)
try:
    _connector.acquire(_request)
except SnowflakeBundleError:
    pass
else:
    raise AssertionError("acquisition without the capture runtime was accepted")
assert _credentials.calls == 0
assert _bundle_adapter.calls == 0


if _native_source is not None:
    from dataclasses import asdict

    from process.custom_import.definition import CustomImportDefinition as NativeDefinition
    from process.custom_import.snowflake_binding import SnowflakeSourceBinding as NativeBinding
    from process.custom_import.snowflake_bundle import SnowflakeBundleStatementBuilder as NativeBuilder

    native_definition = NativeDefinition.from_mapping(_definition_document())
    native_binding = NativeBinding.from_mapping(_binding_document(native_definition))
    assert NativeDefinition is not CustomImportDefinition
    assert asdict(native_definition) == asdict(_definition)
    assert asdict(native_binding) == asdict(_binding)
    native_relations, native_bindings = native_binding.bundle_components(native_definition)
    native_request = NativeBuilder(approved_relations=native_relations).prepare_request(
        native_definition, bindings=native_bindings
    )
    assert asdict(native_request) == asdict(_request)
    for module_name in (
        "process.reference_family_archive",
        "process.entity_address_snapshot_source",
        "process.entity_address_snapshot_destination",
        "process.ptg_parts.result_archive_receive_binding",
        "process.ptg_parts.result_archive_candidate_validation",
    ):
        native_module = importlib.import_module(module_name)
        assert pathlib.Path(native_module.__file__).resolve().is_relative_to(_native_source)
    assert all(
        pathlib.Path(origin).resolve().is_relative_to(_native_source)
        for name, module in tuple(sys.modules.items())
        if name == "process" or name.startswith("process.")
        for origin in ([module.__file__] if module.__file__ else module.__path__)
    )
