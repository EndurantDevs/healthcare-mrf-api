# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Terminal evidence retains typed keys without exposing writer authority."""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import json
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql.elements import TextClause

from db.models.custom_import import CustomImportDefinitionRevision, CustomImportSchemaRevision
from process.custom_import import rejection_evidence as reader
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.runner_codec import definition_digest, digest_text, root_key_document_from_tuple

_NOW = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
_DIGEST = b"p" * 32
_PINS = {"dataset_id": 3, "execution_id": 17, "build_id": 23}
_REJECTION_OID = 1001 + reader.SNAPSHOT_MODELS.index(reader.Rejection)


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class _Session:
    def __init__(self, scope_rows, collections, events=()):
        self.scope_rows = scope_rows
        self.collections = collections
        self.events = list(events)
        self.active = False
        self.statements = []
        self.scope_reads = 0
        self.after_lock_rows = None
        self.failure = None
        self.header_source_oid = _REJECTION_OID
        self.locked_relations = [
            SimpleNamespace(relation_slot=slot, table_oid=slot + 1000, current_oid=slot + 1000, is_locked=True)
            for slot in range(1, len(reader.SNAPSHOT_MODELS) + 1)
        ]

    def in_transaction(self):
        return self.active

    @asynccontextmanager
    async def begin(self):
        self.active = True
        try:
            yield
        finally:
            self.active = False

    async def connection(self):
        return SimpleNamespace(
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {"schema_translate_map": {"mrf": 'read"scope'}}
            ),
            dialect=dialect(),
        )

    async def scalars(self, statement):
        self.statements.append(statement)
        return _Result(self.collections)

    async def execute(self, statement, parameters=None):
        self.statements.append(statement)
        if self.failure is not None:
            raise self.failure
        if isinstance(statement, TextClause):
            return _Result(self.locked_relations if "custom_import_snapshot_relation" in statement.text else [])
        first = statement.column_descriptions[0]["name"]
        if first == "CustomImportExecution":
            self.scope_reads += 1
            return _Result(
                self.after_lock_rows if self.scope_reads > 1 and self.after_lock_rows is not None else self.scope_rows
            )
        compiled = statement.compile(dialect=dialect())
        params = compiled.params
        events = [row for row in self.events if row.producing_token_sha256 == params["producing_token_sha256_1"]]
        if first == "rejection_ordinal":
            events = [row for row in events if row.rejection_ordinal > params["rejection_ordinal_1"]]
            return self._headers(events[: params["param_1"]])
        assert first == "rejection_id"
        return _Result([row for row in events if row.rejection_ordinal in params["rejection_ordinal_1"]])

    def _headers(self, events):
        rows = [
            SimpleNamespace(
                rejection_ordinal=row.rejection_ordinal,
                key_bytes=0 if row.canonical_root_key is None else len(row.canonical_root_key.encode()),
                source_oid=self.header_source_oid,
            )
            for row in events
        ]
        return _Result(
            rows or [SimpleNamespace(rejection_ordinal=None, key_bytes=None, source_oid=self.header_source_oid)]
        )


def _definition_rows(definition):
    revision = CustomImportDefinitionRevision(
        definition_revision_id=5,
        dataset_id=3,
        schema_revision_id=7,
        revision_number=1,
        contract_version="custom-import/v1",
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    schema = CustomImportSchemaRevision(
        schema_revision_id=7,
        dataset_id=3,
        revision_number=1,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    return revision, schema


def _retained(definition=None):
    if definition is None:
        definition = CustomImportDefinition.from_json(
            (Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json").read_text()
        )
    identity_dict = dict(
        dataset_id=3, execution_id=17, definition_revision_id=5, schema_revision_id=7, capture_bundle_id=11
    )
    execution = SimpleNamespace(
        **identity_dict,
        state="failed",
        finished_at=_NOW,
        request_identity_sha256=b"r" * 32,
        source_binding_revision_id=19,
    )
    build = SimpleNamespace(
        **identity_dict,
        build_id=23,
        generation_id=29,
        producing_fence=2,
        producing_token_sha256=_DIGEST,
        request_identity_sha256=b"r" * 32,
    )
    snapshot = SimpleNamespace(
        **identity_dict, family_id=31, generation_id=29, producing_fence=2, producing_token_sha256=_DIGEST
    )
    revision, schema = _definition_rows(definition)
    binding = SimpleNamespace(
        dataset_id=3,
        definition_revision_id=5,
        schema_revision_id=7,
        source_binding_revision_id=19,
        binding_sha256=b"b" * 32,
        definition_sha256=revision.definition_sha256,
        schema_sha256=schema.schema_sha256,
    )
    capture = SimpleNamespace(
        dataset_id=3,
        definition_revision_id=5,
        schema_revision_id=7,
        capture_bundle_id=11,
        capture_state="sealed",
        sealed_at=_NOW,
        manifest_sha256=b"c" * 32,
    )
    return definition, [execution, build, snapshot, revision, schema, binding, capture], _collection_rows(definition)


def _collection_rows(definition):
    collections = []
    for slot, collection in enumerate(definition.child_collections, start=4):
        shape = canonical_json(
            {
                "child_key": list(collection.child_key),
                "parent_key": [{"child": part.child_field, "root": part.root_field} for part in collection.parent_key],
            }
        )
        collections.append(
            SimpleNamespace(
                collection_name=collection.name,
                collection_slot=slot,
                canonical_key_shape=shape,
                key_shape_sha256=definition_digest("schema", shape),
            )
        )
    return collections


def _event(definition, ordinal=0, *, value="0000000001", key_values=None, **changes):
    key = root_key_document_from_tuple(definition, (value,) if key_values is None else key_values)
    event_dict = dict(
        rejection_id=ordinal + 101,
        rejection_ordinal=ordinal,
        canonical_root_key=key,
        root_key_sha256=digest_text("root-key", key),
        code="invalid_field",
        collection_slot=None,
        field_slot=None,
        source_ordinal=ordinal,
        producing_token_sha256=_DIGEST,
        canonical_evidence="must not be selected or exposed",
        source_oid=_REJECTION_OID,
    )
    event_dict.update(changes)
    return SimpleNamespace(**event_dict)


@pytest.fixture
def retained():
    definition, rows, collections = _retained()
    return SimpleNamespace(
        definition=definition, rows=rows, session=_Session([rows], collections, [_event(definition)])
    )


async def test_failed_unsealed_build_returns_typed_key_without_activation_or_authority(retained):
    page = await reader.read_rejection_page(retained.session, **_PINS)
    assert page["complete"] and page["next_after_ordinal"] is None
    assert page["returned_rejection_events"] == 1
    event = page["events"][0]
    assert event["logical_key"] == {"npi": {"type": "string", "value": "0000000001"}}
    assert event["logical_key_status"] == "retained"
    assert event["offending_value_status"] == "not_retained"
    assert page["scope"]["generation_id"] == "29"
    encoded = reader._encoded(page)
    assert len(encoded) <= reader.MAX_PAGE_BYTES
    for forbidden in (b"producer", b"snapshot", b"canonical_evidence", b"must not", _DIGEST.hex().encode()):
        assert forbidden not in encoded
    assert not retained.session.active and retained.session.scope_reads == 2
    statements = [str(statement.compile(dialect=dialect())) for statement in retained.session.statements]
    assert statements[:2] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SET LOCAL statement_timeout = '5s'",
    ]
    assert '"read""scope".lock_custom_import_materialization_storage' in statements[3]
    payload_sql = statements[-1]
    assert "ci_snapshot_31.custom_import_rejection" in payload_sql
    assert "producing_token_sha256" in payload_sql and "producing_fence" in payload_sql
    assert "canonical_evidence" not in payload_sql and "OFFSET" not in payload_sql
    assert "pg_catalog.pg_typeof(retained_rejection)" in payload_sql
    assert "LEFT OUTER JOIN LATERAL" in statements[-2] and "LIMIT" in statements[-2]
    assert "canonical_binding" not in statements[2] and "snapshot_token" not in statements[2]


@pytest.mark.parametrize("state", ["failed", "canceled", "completed", "no_change"])
async def test_terminal_state_does_not_require_a_serving_generation(retained, state):
    retained.rows[0].state = state
    retained.rows[1].generation_id = retained.rows[2].generation_id = None
    assert (await reader.read_rejection_page(retained.session, **_PINS))["scope"]["generation_id"] is None


@pytest.mark.parametrize("state", ["queued", "running", "canceling"])
async def test_nonterminal_scope_is_rejected_before_storage_lock(retained, state):
    retained.rows[0].state = state
    with pytest.raises(reader.RejectionEvidenceNotFinal, match="is not final"):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert len(retained.session.statements) == 3
    assert not retained.session.active


@pytest.mark.parametrize(
    "row_index,name,value",
    [
        (0, "dataset_id", 99),
        (0, "execution_id", 99),
        (0, "finished_at", None),
        (0, "finished_at", _NOW.replace(tzinfo=None)),
        (0, "request_identity_sha256", b"x" * 32),
        (1, "build_id", 99),
        (1, "capture_bundle_id", 99),
        (1, "request_identity_sha256", None),
        (1, "producing_token_sha256", b"x" * 32),
        (2, "family_id", 0),
        (2, "execution_id", 99),
        (2, "producing_fence", 99),
        (2, "producing_token_sha256", b"x" * 32),
        (2, "generation_id", None),
        (3, "dataset_id", 99),
        (3, "schema_revision_id", 99),
        (3, "definition_sha256", b"x" * 32),
        (3, "canonical_definition", "{}"),
        (4, "schema_sha256", b"x" * 32),
        (4, "dataset_id", 99),
        (5, "source_binding_revision_id", 99),
        (5, "definition_revision_id", 99),
        (5, "definition_sha256", b"x" * 32),
        (5, "schema_sha256", b"x" * 32),
        (6, "capture_bundle_id", 99),
        (6, "schema_revision_id", 99),
        (6, "capture_state", "pending"),
        (6, "sealed_at", None),
        (6, "manifest_sha256", b"short"),
    ],
)
async def test_retained_identity_mismatch_fails_closed(retained, row_index, name, value):
    setattr(retained.rows[row_index], name, value)
    with pytest.raises(reader.RejectionEvidenceUnavailable, match="^retained rejection evidence is unavailable$"):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert not retained.session.active


@pytest.mark.parametrize("rows", [[], [None, None]])
async def test_missing_or_ambiguous_scope_never_selects_another_snapshot(retained, rows):
    retained.session.scope_rows = rows
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)


async def test_binding_is_rechecked_after_read_lock(retained):
    changed = copy.deepcopy(retained.rows)
    changed[2].family_id = 33
    retained.session.after_lock_rows = [changed]
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert retained.session.scope_reads == 2 and len(retained.session.statements) == 6


async def test_exact_original_producer_and_keyset_survive_multiple_pages(retained):
    retained.session.events = [_event(retained.definition, value="0000000001", ordinal=index) for index in range(103)]
    retained.session.events.insert(1, _event(retained.definition, ordinal=0, producing_token_sha256=b"other"))
    first = await reader.read_rejection_page(retained.session, **_PINS)
    assert len(first["events"]) == 100 and first["next_after_ordinal"] == "99" and not first["complete"]
    second = await reader.read_rejection_page(
        retained.session, **_PINS, after_ordinal=99, scope_sha256=first["scope_sha256"]
    )
    assert [row["ordinal"] for row in second["events"]] == ["100", "101", "102"]
    assert second["complete"] and second["next_after_ordinal"] is None
    assert retained.session.scope_reads == 4
    assert first["events"][0]["logical_key"] == second["events"][0]["logical_key"]


async def test_empty_retained_scope_is_complete_not_unavailable(retained):
    retained.session.events = []
    page = await reader.read_rejection_page(retained.session, **_PINS)
    assert page["events"] == [] and page["complete"] and page["returned_rejection_events"] == 0


async def test_scope_digest_change_blocks_continuation_before_payload_read(retained):
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS, after_ordinal=0, scope_sha256="a" * 64)
    assert len(retained.session.statements) == 6


@pytest.mark.parametrize(
    "overrides",
    [
        {"dataset_id": True},
        {"execution_id": 0},
        {"build_id": 2**63},
        {"limit": 0},
        {"limit": 101},
        {"limit": True},
        {"after_ordinal": -1},
        {"after_ordinal": True},
        {"after_ordinal": 0},
        {"scope_sha256": "A" * 64},
        {"scope_sha256": "short"},
    ],
)
async def test_invalid_page_selector_does_not_open_transaction(retained, overrides):
    with pytest.raises(ValueError, match="invalid rejection page selector"):
        await reader.read_rejection_page(retained.session, **(_PINS | overrides))
    assert retained.session.statements == [] and not retained.session.active


async def test_existing_transaction_is_not_repurposed(retained):
    retained.session.active = True
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert retained.session.active and retained.session.statements == []


async def test_storage_failures_are_fixed_and_sanitized(retained):
    retained.session.failure = RuntimeError("sensitive database error and location")
    with pytest.raises(reader.RejectionEvidenceUnavailable) as error:
        await reader.read_rejection_page(retained.session, **_PINS)
    assert str(error.value) == "retained rejection evidence is unavailable"
    assert not retained.session.active


@pytest.mark.parametrize(
    "changes,status",
    [
        ({"canonical_root_key": None, "root_key_sha256": None}, "not_retained"),
        ({"canonical_root_key": "{}"}, "invalid"),
        ({"canonical_root_key": "[]"}, "invalid"),
        ({"root_key_sha256": b"x" * 32}, "invalid"),
    ],
)
async def test_unusable_keys_do_not_expose_raw_payload_or_invent_identifiers(retained, changes, status):
    retained.session.events = [_event(retained.definition, **changes)]
    event = (await reader.read_rejection_page(retained.session, **_PINS))["events"][0]
    assert event["logical_key"] is None and event["logical_key_status"] == status
    assert "canonical_root_key" not in event


async def test_configured_child_field_is_projected_without_an_offending_value(retained):
    retained.session.events = [_event(retained.definition, collection_slot=4, field_slot=5)]
    event = (await reader.read_rejection_page(retained.session, **_PINS))["events"][0]
    assert event["collection"] == "rates" and event["field_id"] == "amount"
    assert event["offending_value_status"] == "not_retained"


@pytest.mark.parametrize(
    "changes",
    [
        {"code": "raw SQL error"},
        {"collection_slot": 99},
        {"collection_slot": True},
        {"field_slot": 99},
        {"collection_slot": 4, "field_slot": 1},
        {"source_ordinal": -1},
        {"source_ordinal": True},
        {"rejection_id": 0},
    ],
)
async def test_invalid_event_metadata_fails_closed(retained, changes):
    retained.session.events = [_event(retained.definition, **changes)]
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)


async def test_byte_budget_pages_without_skipping_an_event(retained):
    retained.session.events = [_event(retained.definition, index, value="x" * 32_500) for index in range(2)]
    first = await reader.read_rejection_page(retained.session, **_PINS)
    assert len(reader._encoded(first)) <= reader.MAX_PAGE_BYTES
    assert len(first["events"]) == 1 and first["next_after_ordinal"] == "0" and not first["complete"]
    second = await reader.read_rejection_page(
        retained.session, **_PINS, after_ordinal=0, scope_sha256=first["scope_sha256"]
    )
    assert second["complete"] and [event["ordinal"] for event in second["events"]] == ["1"]


async def test_oversized_key_is_rejected_before_payload_hydration(retained):
    retained.session.events = [_event(retained.definition, value="x" * 70_000)]
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    statement = retained.session.statements[-1]
    assert statement.column_descriptions[0]["name"] == "rejection_ordinal"


async def test_encoded_event_overflow_is_explicitly_unavailable(retained):
    retained.session.events = [_event(retained.definition, value="x" * 65_000)]
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)


def _key_definition(value_type, *, composite=False):
    document = json.loads((Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json").read_text())
    root = document["schema"]["root"]
    root["logical_key"] = ["npi", "display_name"] if composite else ["display_name"]
    root["fields"][1]["type"] = value_type
    child = document["schema"]["children"][0]
    child["fields"].append({"id": "parent_key", "slot": 6, "type": value_type, "nullable": False})
    child["parent_key"] = [
        *([{"child": "rate_npi", "root": "npi"}] if composite else []),
        {"child": "parent_key", "root": "display_name"},
    ]
    document["aliases"]["rates"]["Parent Key"] = "parent_key"
    return CustomImportDefinition.from_mapping(document)


@pytest.mark.parametrize(
    "value_type,scalar,encoded",
    [
        ("string", "00004-é", "00004-é"),
        ("integer", 2**60 + 1, str(2**60 + 1)),
        ("decimal", Decimal("1234.5600"), "1234.56"),
        ("boolean", False, False),
        ("date", dt.date(2026, 1, 2), "2026-01-02"),
        ("timestamp", dt.datetime(2026, 1, 2, 3, tzinfo=dt.timezone(dt.timedelta(hours=2))), "2026-01-02T01:00:00Z"),
    ],
)
async def test_keys_preserve_declared_types_and_precision(value_type, scalar, encoded):
    definition, scope_rows, collections = _retained(_key_definition(value_type))
    session = _Session([scope_rows], collections, [_event(definition, value=scalar)])
    event = (await reader.read_rejection_page(session, **_PINS))["events"][0]
    assert event["logical_key"] == {"display_name": {"type": value_type, "value": encoded}}
    assert event["logical_key_status"] == "retained"
    assert "npi" not in event["logical_key"]


async def test_composite_keys_keep_every_configured_component():
    definition, scope_rows, collections = _retained(_key_definition("integer", composite=True))
    session = _Session([scope_rows], collections, [_event(definition, key_values=("0000000001", 2**60 + 1))])
    key = (await reader.read_rejection_page(session, **_PINS))["events"][0]["logical_key"]
    assert key == {
        "npi": {"type": "string", "value": "0000000001"},
        "display_name": {"type": "integer", "value": str(2**60 + 1)},
    }


@pytest.mark.parametrize(
    "change",
    [
        lambda document: document.update(contract="unknown"),
        lambda document: document.update(fields=[]),
        lambda document: document["fields"][0].update(field="other"),
        lambda document: document["fields"][0]["value"].update(type="integer", value=1),
        lambda document: document["fields"][0]["value"].update(state="null", value=None),
        lambda document: document["fields"][0]["value"].update(state="missing"),
    ],
)
async def test_invalid_key_grammar_cannot_become_a_typed_key(retained, change):
    rejection = retained.session.events[0]
    document = json.loads(rejection.canonical_root_key)
    change(document)
    rejection.canonical_root_key = canonical_json(document)
    rejection.root_key_sha256 = digest_text("root-key", rejection.canonical_root_key)
    event = (await reader.read_rejection_page(retained.session, **_PINS))["events"][0]
    assert event["logical_key_status"] == "invalid" and event["logical_key"] is None


@pytest.mark.parametrize("generation", [0, -1, True, 2**63])
async def test_nonpositive_generation_is_not_a_retained_pin(retained, generation):
    retained.rows[1].generation_id = retained.rows[2].generation_id = generation
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)


@pytest.mark.parametrize("request_digest", [None, b"r" * 32])
async def test_unbound_capture_remains_readable_without_inventing_binding(retained, request_digest):
    retained.rows[0].source_binding_revision_id = None
    retained.rows[0].request_identity_sha256 = retained.rows[1].request_identity_sha256 = request_digest
    retained.rows[5] = None
    retained.session.events.append(_event(retained.definition, ordinal=1))
    first = await reader.read_rejection_page(retained.session, **_PINS, limit=1)
    scope = first["scope"]
    assert scope["source_binding_revision_id"] is None and scope["source_binding_sha256"] is None
    assert scope["capture_manifest_sha256"] == (b"c" * 32).hex()
    assert not first["complete"] and first["next_after_ordinal"] == "0"
    second = await reader.read_rejection_page(
        retained.session, **_PINS, after_ordinal=0, scope_sha256=first["scope_sha256"]
    )
    assert second["scope"] == scope and second["scope_sha256"] == first["scope_sha256"]
    assert [event["ordinal"] for event in first["events"] + second["events"]] == ["0", "1"]
    assert second["complete"] and second["next_after_ordinal"] is None
    assert not retained.session.active


@pytest.mark.parametrize("request_digest", [None, b"r" * 32])
async def test_nullable_request_identity_change_invalidates_continuation(retained, request_digest):
    retained.rows[0].source_binding_revision_id = None
    retained.rows[0].request_identity_sha256 = retained.rows[1].request_identity_sha256 = request_digest
    retained.rows[5] = None
    retained.session.events.append(_event(retained.definition, ordinal=1))
    first = await reader.read_rejection_page(retained.session, **_PINS, limit=1)
    retained.rows[0].request_identity_sha256 = retained.rows[1].request_identity_sha256 = (
        b"r" * 32 if request_digest is None else None
    )
    statement_count = len(retained.session.statements)
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS, after_ordinal=0, scope_sha256=first["scope_sha256"])
    assert len(retained.session.statements) == statement_count + 6
    assert not retained.session.active


@pytest.mark.parametrize("request_digest", [b"short", b"r" * 33, "r" * 32])
async def test_unbound_request_identity_still_requires_a_valid_digest(retained, request_digest):
    retained.rows[0].source_binding_revision_id = None
    retained.rows[0].request_identity_sha256 = retained.rows[1].request_identity_sha256 = request_digest
    retained.rows[5] = None
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert len(retained.session.statements) == 3 and not retained.session.active


async def test_bound_execution_cannot_use_a_null_request_identity(retained):
    retained.rows[0].request_identity_sha256 = retained.rows[1].request_identity_sha256 = None
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert len(retained.session.statements) == 3 and not retained.session.active


@pytest.mark.parametrize("failure_stage", ["grant", "commit"])
async def test_reader_role_setup_rollback_preserves_original_failure(failure_stage):
    from tests.test_custom_import_rejection_evidence_postgres import _reader_role

    failure = RuntimeError("role setup failed")
    statements = []
    transactions = []

    async def execute(statement):
        statements.append(str(statement))
        if str(statement).startswith("DROP "):
            raise AssertionError("a rolled-back role must not be cleaned up")
        if failure_stage == "grant" and str(statement).startswith("GRANT "):
            raise failure

    @asynccontextmanager
    async def begin():
        connection = SimpleNamespace(execute=execute)
        transactions.append(connection)
        yield connection
        if failure_stage == "commit":
            raise failure

    retained = SimpleNamespace(
        case=SimpleNamespace(engine=SimpleNamespace(begin=begin), schema_name="read_scope"), family_id=31
    )
    with pytest.raises(RuntimeError) as error:
        async with _reader_role(retained):
            pytest.fail("a failed setup transaction must not yield a role")
    assert error.value is failure
    assert len(transactions) == 1
    assert statements[0].startswith("CREATE ROLE ") and statements[1].startswith("GRANT ")


async def test_collection_registry_mismatch_is_sanitized(retained):
    retained.session.collections[0].canonical_key_shape = "{}"
    with pytest.raises(reader.RejectionEvidenceUnavailable, match="^retained rejection evidence is unavailable$"):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert retained.session.statements[-1].column_descriptions[0]["name"] == "CustomImportChildCollection"


async def test_missing_snapshot_binding_is_not_replaced_by_empty_page(retained):
    retained.session.after_lock_rows = []
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)


async def test_noncanonical_key_does_not_smuggle_extra_fields(retained):
    rejection = retained.session.events[0]
    document = json.loads(rejection.canonical_root_key)
    document["unretained"] = "not exposed"
    rejection.canonical_root_key = canonical_json(document)
    rejection.root_key_sha256 = digest_text("root-key", rejection.canonical_root_key)
    event = (await reader.read_rejection_page(retained.session, **_PINS))["events"][0]
    assert event["logical_key_status"] == "invalid" and b"not exposed" not in reader._encoded(event)


async def test_unbound_execution_rejects_an_unrelated_binding(retained):
    retained.rows[0].source_binding_revision_id = None
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)


async def test_missing_model_schema_never_uses_search_path(retained, monkeypatch):
    async def connection():
        return SimpleNamespace(
            sync_connection=SimpleNamespace(get_execution_options=lambda: {"schema_translate_map": {"mrf": None}}),
            dialect=dialect(),
        )

    monkeypatch.setattr(retained.session, "connection", connection)
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert retained.session.scope_reads == 1


@pytest.mark.parametrize("ordinal", [True, 2**63])
async def test_malformed_retained_ordinal_is_not_a_cursor(retained, ordinal):
    retained.session.events[0].rejection_ordinal = ordinal
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)


@pytest.mark.parametrize("projection", ["rejection_ordinal", "rejection_id"])
async def test_inconsistent_header_or_hydration_fails_closed(retained, monkeypatch, projection):
    execute = retained.session.execute

    async def inconsistent(statement, parameters=None):
        response = await execute(statement, parameters)
        if not isinstance(statement, TextClause) and statement.column_descriptions[0]["name"] == projection:
            return _Result(
                [SimpleNamespace(rejection_ordinal=0, key_bytes=-1, source_oid=_REJECTION_OID)]
                if projection == "rejection_ordinal"
                else []
            )
        return response

    monkeypatch.setattr(retained.session, "execute", inconsistent)
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert not retained.session.active


async def test_raw_key_byte_budget_defers_hydration_without_skipping(retained):
    retained.session.events = [_event(retained.definition, index, value="x" * 33_000) for index in range(2)]
    first = await reader.read_rejection_page(retained.session, **_PINS)
    selected = retained.session.statements[-1].compile(dialect=dialect()).params["rejection_ordinal_1"]
    assert selected == [0] and first["next_after_ordinal"] == "0" and not first["complete"]
    second = await reader.read_rejection_page(
        retained.session, **_PINS, after_ordinal=0, scope_sha256=first["scope_sha256"]
    )
    assert second["complete"] and second["events"][0]["ordinal"] == "1"


async def test_caller_cancellation_propagates_and_closes_page_transaction(retained):
    retained.session.failure = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert not retained.session.active


@pytest.mark.parametrize("change", ["missing", "duplicate", "wrong_oid", "unlocked", "invalid_oid"])
async def test_registry_and_actual_locks_must_match(retained, change):
    relations = retained.session.locked_relations
    if change == "missing":
        relations.pop()
    elif change == "duplicate":
        relations.append(relations[-1])
    elif change == "wrong_oid":
        relations[-1].current_oid += 1
    elif change == "unlocked":
        relations[-1].is_locked = False
    else:
        relations[-1].table_oid = 0
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert retained.session.scope_reads == 1 and not retained.session.active


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("source_oid", [None, _REJECTION_OID + 1])
async def test_header_relation_witness_rejects_replacement_even_when_empty(retained, empty, source_oid):
    retained.session.header_source_oid = source_oid
    if empty:
        retained.session.events = []
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert retained.session.statements[-1].column_descriptions[0]["name"] == "rejection_ordinal"


@pytest.mark.parametrize("source_oid", [None, _REJECTION_OID + 1])
async def test_hydrated_relation_must_match_the_registered_header_source(retained, source_oid):
    retained.session.events[0].source_oid = source_oid
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
    assert retained.session.statements[-1].column_descriptions[0]["name"] == "rejection_id"


async def test_missing_header_witness_is_not_an_empty_page(retained, monkeypatch):
    monkeypatch.setattr(retained.session, "_headers", lambda events: _Result([]))
    with pytest.raises(reader.RejectionEvidenceUnavailable):
        await reader.read_rejection_page(retained.session, **_PINS)
