# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host checks for composition scopes, native locks and content ordering."""

import asyncio
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest
from sqlalchemy import JSON, Column, MetaData, String, Table
from sqlalchemy.dialects import postgresql

from process import reference_family_archive as archive
from process import reference_family_composition as composition


@pytest.mark.parametrize("first", ["composition", "archive"])
def test_public_import_orders_preserve_composition_bindings(first):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"from importlib import import_module; import_module('process.reference_family_{first}'); "
            "from process import reference_family_composition as composition; "
            "from process import reference_family_archive as archive; "
            "assert archive.compose_model_family_stage is composition.compose_model_family_stage; "
            "assert archive.ReferenceModelContribution is composition.ReferenceModelContribution",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def _model(name, original=None):
    table = (
        original.__table__.to_metadata(MetaData(), name=name)
        if original is not None
        else Table(
            name,
            MetaData(),
            Column("system", String, primary_key=True),
            Column("code", String, primary_key=True),
            Column("source", String),
            Column("payload", JSON),
        )
    )
    return type(name.title().replace("_", ""), (), {"__tablename__": name, "__table__": table})


CATALOG = _model("catalog_rows")
UNCHANGED = _model("unchanged_rows")
INCOMING = _model("incoming_catalog", CATALOG)
EFFECTS = _model("catalog_effects")
EFFECTS.__table__ = Table(
    EFFECTS.__tablename__,
    MetaData(),
    Column("system", String, primary_key=True),
    Column("code", String, primary_key=True),
    Column("before_image", postgresql.JSONB),
    Column("after_image", postgresql.JSONB),
    Column("baseline_image", postgresql.JSONB),
)


@asynccontextmanager
async def _bounded(session):
    session.events.append(("capture", "begin"))
    yield
    session.events.append(("capture", "end"))


def _ownership(spec, number):
    dataset_id = UUID(int=number)
    return archive.ReferenceFamilyStageOwnership(
        spec.importer_id,
        dataset_id,
        archive.reference_family_stage_schema(dataset_id),
        number,
        tuple((name, number * 10 + index) for index, name in enumerate(sorted(spec.table_names))),
    )


def _session():
    events = []

    async def execute(statement, parameters=None):
        events.append(("execute", str(statement), parameters))

    async def is_empty(statement, parameters=None):
        query = str(statement)
        events.append(("scalar", query, parameters))
        return query.startswith("SELECT NOT EXISTS(")

    return SimpleNamespace(
        events=events,
        execute=execute,
        scalar=is_empty,
        in_transaction=lambda: True,
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )


def _composition_case(monkeypatch, *, effects=False):
    spec = archive.ReferenceFamilySpec("shared-models", (CATALOG, UNCHANGED))
    incoming_spec = archive.ReferenceFamilySpec("incoming-models", (INCOMING, EFFECTS) if effects else (INCOMING,))
    ownership, incoming = _ownership(spec, 1), _ownership(incoming_spec, 2)
    incumbent = archive.ReferenceFamilyIncumbent(
        spec.importer_id, "serving", (("catalog_rows", 31), ("unchanged_rows", 32))
    )
    session = _session()
    copies = []

    async def copy_rows(actual_session, query, *arguments, **options):
        assert actual_session is session
        copies.append((query, arguments, options))
        session.events.append(("copy", options["table_name"]))
        return 3

    async def verify(_session, observed_spec, observed_ownership, **_options):
        assert (observed_spec, observed_ownership) in ((spec, ownership), (incoming_spec, incoming))
        session.events.append(("inventory", observed_ownership.schema_name))
        return observed_ownership

    monkeypatch.setattr(archive, "_bounded_capture", _bounded)
    monkeypatch.setattr(archive, "verify_model_family_stage_ownership", AsyncMock(side_effect=verify))
    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=incumbent.relation_oids))
    monkeypatch.setattr(archive, "require_native_read_catalog", AsyncMock())
    contribution = (
        archive.ReferenceModelContribution(CATALOG, INCOMING, effect_model=EFFECTS)
        if effects
        else archive.ReferenceModelContribution(CATALOG, INCOMING, ("synthetic_source", "quoted'source"))
    )
    arguments_by_field = dict(
        ownership=ownership,
        incumbent=incumbent,
        incoming_spec=incoming_spec,
        incoming=incoming,
        contributions=(contribution,),
        source_copy=archive.ReferenceFamilySourceCopy(copy_rows, 7, 30),
    )
    return SimpleNamespace(
        session=session, spec=spec, arguments=arguments_by_field, copies=copies, contribution=contribution
    )


async def _compose(case, **overrides):
    return await archive.compose_model_family_stage(case.session, case.spec, **{**case.arguments, **overrides})


@pytest.mark.asyncio
async def test_source_composition_preserves_unowned_rows_and_parameterizes_values(monkeypatch):
    case = _composition_case(monkeypatch)
    assert await _compose(case) is case.arguments["ownership"]
    changed, unchanged = case.copies
    query, arguments, options = changed
    assert 'FROM "serving"."catalog_rows" live' in query
    assert "WHERE (live.source=ANY(CAST($1 AS text[]))) IS NOT TRUE" in query
    assert 'UNION ALL SELECT "system", "code", "source", "payload" FROM' in query
    assert arguments == (["synthetic_source", "quoted'source"],)
    assert "synthetic_source" not in query and "quoted'source" not in query
    assert options["columns"] == ("system", "code", "source", "payload")
    assert unchanged[0] == 'SELECT "system", "code", "source", "payload" FROM "serving"."unchanged_rows" live'
    assert unchanged[1] == ()
    assert all(item[2]["schema_name"] == case.arguments["ownership"].schema_name for item in case.copies)
    assert [item[2]["max_bytes"] for item in case.copies] == [7, 4]
    assert 0 < unchanged[2]["timeout"] <= options["timeout"] <= 30
    case.session.commit.assert_not_awaited()
    case.session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_upsert_copies_omitted_keys_and_preserves_undeclared_fields(monkeypatch):
    case = _composition_case(monkeypatch)
    contribution = replace(case.contribution, update_columns=("source",))
    await _compose(case, contributions=(contribution,))
    query, arguments, _options = case.copies[0]
    assert "WHERE NOT EXISTS(SELECT 1 FROM" in query
    assert 'UNION ALL SELECT incoming."system", incoming."code", incoming."source"' in query
    assert 'CASE WHEN live."system" IS NULL THEN incoming."payload" ELSE live."payload" END' in query
    assert 'LEFT JOIN "serving"."catalog_rows" live ON' in query
    assert arguments == ()
    assert any(
        event[0] == "scalar" and "live.source IS DISTINCT FROM incoming.source" in event[1]
        for event in case.session.events
    )
    assert any(
        event[0] == "scalar" and event[2] == {"sources": list(contribution.source_values)}
        for event in case.session.events
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "prepare", "quote", "copy"])
async def test_bound_composition_uses_asyncpg_copy_query_protocol(monkeypatch, failure):
    """Exercise driver query formatting; server type/quote replies remain synthetic."""
    case = _composition_case(monkeypatch)
    source_values = ("synthetic_source", "quoted'source", "$2")
    statements = []
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_to_table=AsyncMock(return_value="COPY 1"),
        terminate=lambda: None,
    )

    async def prepare(query):
        assert "ANY(CAST($1 AS text[]))" in query and all(source not in query for source in source_values)
        if failure == "prepare":
            raise RuntimeError("synthetic prepare failure")
        return SimpleNamespace(get_parameters=lambda: (SimpleNamespace(name="text[]", schema="pg_catalog"),))

    async def quote(query, *arguments):
        assert query == 'SELECT quote_literal($1::"pg_catalog"."_text"::text)'
        assert arguments == (list(source_values),)
        if failure == "quote":
            raise RuntimeError("synthetic quote failure")
        return ('\'{"synthetic_source","quoted\'\'source","$2"}\'',)

    async def copy_out(statement, output, _timeout):
        statements.append(statement)
        if failure == "copy":
            raise RuntimeError("synthetic copy failure")
        await output(b"abc")
        return "COPY 1"

    driver.prepare, driver.fetchrow, driver._copy_out = prepare, quote, copy_out
    driver._format_copy_opts = MethodType(asyncpg.Connection._format_copy_opts, driver)
    driver.copy_from_query = MethodType(asyncpg.Connection.copy_from_query, driver)
    case.session.connection = AsyncMock(
        return_value=SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
        )
    )
    options_by_field = dict(
        contributions=(replace(case.contribution, source_values=source_values),),
        source_copy=archive.ReferenceFamilySourceCopy(archive.native_copy_projection, 7, 30),
    )
    if failure:
        with pytest.raises(RuntimeError, match=f"synthetic {failure} failure"):
            await _compose(case, **options_by_field)
        driver.copy_to_table.assert_not_awaited()
        assert not any("ADD PRIMARY KEY" in event[1] for event in case.session.events if event[0] == "execute")
    else:
        await _compose(case, **options_by_field)
        assert len(statements) == driver.copy_to_table.await_count == 2
        assert 'ANY(CAST(\'{"synthetic_source","quoted\'\'source","$2"}\' AS text[]))' in statements[0]
        assert all(
            statement.startswith("COPY (SELECT ") and statement.endswith("(FORMAT 'binary')")
            for statement in statements
        )
        assert "$1" not in statements[0]
    case.session.commit.assert_not_awaited()
    case.session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_composition_finishes_entire_family_before_indexes_and_set_checks(monkeypatch):
    case = _composition_case(monkeypatch)
    await _compose(case)
    events = case.session.events
    copies = [index for index, event in enumerate(events) if event[0] == "copy"]
    indexes = [index for index, event in enumerate(events) if event[0] == "execute" and "ADD PRIMARY KEY" in event[1]]
    checks = [
        index
        for index, event in enumerate(events)
        if event[0] == "scalar" and event[1].startswith("SELECT NOT EXISTS(")
    ]
    assert len(copies) == len(indexes) == len(checks) == 2
    assert max(copies) < min(indexes) < max(indexes) < min(checks)
    assert all("::jsonb" in events[index][1] for index in checks)
    assert all("IS DISTINCT FROM" in events[index][1] and "AND NOT EXISTS" in events[index][1] for index in checks)
    statements = [event[1] for event in events if event[0] == "execute"]
    assert not any(
        statement.startswith(("INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "CREATE SCHEMA"))
        for statement in statements
    )
    assert not any("SET SCHEMA" in statement or "RENAME" in statement for statement in statements)


@pytest.mark.asyncio
async def test_composition_locks_and_validates_complete_named_inputs_before_payloads(monkeypatch):
    case = _composition_case(monkeypatch)
    await _compose(case)
    events = case.session.events
    locks = [event[1] for event in events if event[0] == "execute" and event[1].startswith("LOCK TABLE")]
    assert len(locks) == 3 and locks == sorted(locks)
    assert all(statement.endswith(" NOWAIT") for statement in locks)
    assert sum("ACCESS EXCLUSIVE" in statement for statement in locks) == 1
    assert sum("IN ACCESS SHARE MODE" in statement for statement in locks) == 2
    first_copy = next(index for index, event in enumerate(events) if event[0] == "copy")
    assert {event[1] for event in events[:first_copy] if event[0] == "inventory"} == {
        case.arguments["ownership"].schema_name,
        case.arguments["incoming"].schema_name,
    }
    archive.require_native_read_catalog.assert_awaited_once_with(case.session, (10, 11, 20, 31, 32))
    assert archive._incumbent_pairs.await_count == 2
    assert {call.args[2].schema_name for call in archive.verify_model_family_stage_ownership.await_args_list[-2:]} == {
        case.arguments["ownership"].schema_name,
        case.arguments["incoming"].schema_name,
    }


@pytest.mark.asyncio
async def test_effect_composition_requires_exact_preimages_and_closed_afterimages(monkeypatch):
    case = _composition_case(monkeypatch, effects=True)
    await _compose(case)
    query, arguments, _ = case.copies[0]
    assert "WHERE NOT EXISTS(SELECT 1 FROM" in query
    assert 'live."system"=effect."system" AND live."code"=effect."code"' in query
    assert "UNION ALL SELECT" in query and arguments == ()
    checks = [event for event in case.session.events if event[0] == "scalar" and "effect.before_image" in event[1]]
    assert len(checks) == 1
    _, validation, parameters = checks[0]
    assert "effect.before_image IS DISTINCT FROM pg_catalog.to_jsonb(live)" in validation
    assert (
        "FULL JOIN" in validation and "effect.after_image IS DISTINCT FROM pg_catalog.to_jsonb(incoming)" in validation
    )
    assert 'effect."system" IS NULL OR effect."code" IS NULL' in validation
    assert 'GROUP BY "system","code" HAVING count(*)>1' in validation
    assert "baseline_image" not in query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid", [None, (), [], "duplicate", "missing_scope", "both_modes", "foreign_model", "bad_payload", "bad_sources"]
)
async def test_invalid_contributions_fail_before_catalog_access(monkeypatch, invalid):
    case = _composition_case(monkeypatch)
    rule = case.contribution
    variants_by_name = {
        "duplicate": (rule, rule),
        "missing_scope": (replace(rule, source_values=()),),
        "both_modes": (replace(rule, effect_model=EFFECTS),),
        "foreign_model": (replace(rule, model_type=INCOMING),),
        "bad_payload": (replace(rule, incoming_model=CATALOG),),
        "bad_sources": (replace(rule, source_values=(True,)),),
    }
    contributions = variants_by_name[invalid] if isinstance(invalid, str) else invalid
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="composition"):
        await _compose(case, contributions=contributions)
    assert case.session.events == [] and case.copies == []


@pytest.mark.parametrize("source_values", [("",), ("same", "same"), ("bad\x00source",), ["source"]])
def test_source_scopes_reject_invalid_or_ambiguous_values(monkeypatch, source_values):
    case = _composition_case(monkeypatch)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="declaration"):
        composition._require_model_contributions(
            case.spec, case.arguments["incoming_spec"], (replace(case.contribution, source_values=source_values),)
        )
    assert case.session.events == []


@pytest.mark.parametrize("changed", ["key_type", "missing_before", "missing_after", "image_type"])
def test_effect_declarations_require_model_keys_and_closed_json_images(monkeypatch, changed):
    case = _composition_case(monkeypatch, effects=True)
    effect_model = _model("altered_effects", EFFECTS)
    columns = effect_model.__table__.c
    if changed == "key_type":
        columns.code.type = String(9)
    elif changed == "missing_before":
        effect_model.__table__._columns.remove(columns.before_image)
    elif changed == "missing_after":
        effect_model.__table__._columns.remove(columns.after_image)
    else:
        columns.after_image.type = JSON()
    incoming_spec = replace(case.arguments["incoming_spec"], model_types=(INCOMING, effect_model))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="effect columns"):
        composition._require_model_contributions(
            case.spec, incoming_spec, (replace(case.contribution, effect_model=effect_model),)
        )
    assert case.session.events == []


def test_incoming_payload_shape_must_match_declared_output(monkeypatch):
    case = _composition_case(monkeypatch)
    incoming_model = _model("altered_payload", INCOMING)
    incoming_model.__table__.append_column(Column("foreign_payload", String))
    incoming_spec = replace(case.arguments["incoming_spec"], model_types=(incoming_model,))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="declaration"):
        composition._require_model_contributions(
            case.spec, incoming_spec, (replace(case.contribution, incoming_model=incoming_model),)
        )
    assert case.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["missing", "reordered", "wrong_name", "absent", "foreign", "same_schema", "incoming_schema"]
)
async def test_invalid_incumbent_vectors_fail_before_catalog_access(monkeypatch, change):
    case = _composition_case(monkeypatch)
    incumbent = case.arguments["incumbent"]
    changes_by_name = {
        "missing": {"relation_oids": incumbent.relation_oids[:1]},
        "reordered": {"relation_oids": incumbent.relation_oids[::-1]},
        "wrong_name": {"relation_oids": (("other_rows", 31), ("unchanged_rows", 32))},
        "absent": {"relation_oids": (("catalog_rows", None), ("unchanged_rows", 32))},
        "foreign": {"importer_id": "other-family"},
        "same_schema": {"schema_name": case.arguments["ownership"].schema_name},
        "incoming_schema": {"schema_name": case.arguments["incoming"].schema_name},
    }
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="composition"):
        await _compose(case, incumbent=replace(incumbent, **changes_by_name[change]))
    assert case.session.events == [] and case.copies == []


@pytest.mark.asyncio
@pytest.mark.parametrize("effects", [False, True])
@pytest.mark.parametrize("observed", [True, None])
async def test_scope_or_preimage_refusal_never_copies(monkeypatch, effects, observed):
    case = _composition_case(monkeypatch, effects=effects)
    original = case.session.scalar

    async def scalar(statement, parameters=None):
        if (parameters and "sources" in parameters) or "effect.before_image" in str(statement):
            return observed
        return await original(statement, parameters)

    case.session.scalar = scalar
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="scope|preimage"):
        await _compose(case)
    assert case.copies == []


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["candidate", "inventory", "catalog", "incumbent"])
async def test_unsafe_inputs_refuse_before_copy(monkeypatch, boundary):
    case = _composition_case(monkeypatch)
    if boundary == "candidate":
        case.session.scalar = AsyncMock(return_value=True)
    elif boundary == "inventory":
        archive.verify_model_family_stage_ownership.side_effect = archive.ReferenceFamilyArchiveError(
            "inventory differs"
        )
    elif boundary == "catalog":
        archive.require_native_read_catalog.side_effect = RuntimeError("catalog differs")
    else:
        archive._incumbent_pairs.return_value = (("catalog_rows", None), ("unchanged_rows", 32))
    with pytest.raises(RuntimeError, match="empty|inventory|catalog|incumbent"):
        await _compose(case)
    assert case.copies == []


@pytest.mark.asyncio
@pytest.mark.parametrize("copied", [True, -1, 5, OSError("synthetic failure")])
async def test_incomplete_family_never_builds_indexes(monkeypatch, copied):
    case = _composition_case(monkeypatch)
    callback = AsyncMock(side_effect=[3, copied])
    with pytest.raises((archive.ReferenceFamilyArchiveError, OSError), match="accounting|synthetic"):
        await _compose(case, source_copy=archive.ReferenceFamilySourceCopy(callback, 7, 30))
    assert callback.await_count == 2
    assert not any("ADD PRIMARY KEY" in event[1] for event in case.session.events if event[0] == "execute")


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["rows", "transaction", "incumbent"])
async def test_late_changes_never_return_a_completed_candidate(monkeypatch, changed):
    case = _composition_case(monkeypatch)
    original = case.session.scalar

    async def scalar(statement, parameters=None):
        if changed == "rows" and str(statement).startswith("SELECT NOT EXISTS("):
            return False
        return await original(statement, parameters)

    case.session.scalar = scalar
    if changed == "incumbent":
        archive._incumbent_pairs.side_effect = [
            case.arguments["incumbent"].relation_oids,
            (("catalog_rows", None), ("unchanged_rows", 32)),
        ]
    if changed == "transaction":
        copy_rows = case.arguments["source_copy"].copy_rows

        async def ended_session(*arguments, **options):
            result = await copy_rows(*arguments, **options)
            if len(case.copies) == 2:
                case.session.in_transaction = lambda: False
            return result

        case.arguments["source_copy"] = archive.ReferenceFamilySourceCopy(ended_session, 7, 30)
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="candidate rows|transaction or incumbent|caller transaction"
    ):
        await _compose(case)
    assert len(case.copies) == 2
    case.session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["copy", "validation"])
async def test_one_deadline_bounds_copy_and_post_copy_validation(monkeypatch, phase):
    case = _composition_case(monkeypatch)

    async def blocked(*_arguments, **_options):
        await asyncio.Future()

    callback = blocked if phase == "copy" else case.arguments["source_copy"].copy_rows
    if phase == "validation":
        monkeypatch.setattr(composition, "_is_model_projection_equal", blocked)
    with pytest.raises(TimeoutError):
        await _compose(case, source_copy=archive.ReferenceFamilySourceCopy(callback, 7, 0.01))
    case.session.commit.assert_not_awaited()
    assert len(case.copies) == (0 if phase == "copy" else 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["transaction", "copy", "excessive_timeout"])
async def test_operation_requires_transaction_and_bounded_capability(monkeypatch, missing):
    case = _composition_case(monkeypatch)
    overrides_by_field = {}
    if missing == "transaction":
        case.session.in_transaction = lambda: False
    else:
        overrides_by_field["source_copy"] = (
            None if missing == "copy" else archive.ReferenceFamilySourceCopy(AsyncMock(), 7, 86401)
        )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="transaction|capability"):
        await _compose(case, **overrides_by_field)
    assert case.session.events == [] and case.copies == []


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["destination", "source"])
async def test_effect_validation_rejects_foreign_qualified_names_before_payload_reads(monkeypatch, changed):
    case = _composition_case(monkeypatch, effects=True)
    live = '"serving"."catalog_rows"' if changed != "destination" else '"other"."catalog_rows"'
    incoming_schema = case.arguments["incoming"].schema_name
    incoming = f'"{incoming_schema}"."incoming_catalog"' if changed != "source" else '"other"."incoming_catalog"'
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="effect destination differs"):
        await composition._validate_composition_effects(
            case.session, case.contribution, live, incoming, case.arguments["incumbent"], incoming_schema
        )
    assert case.session.events == [] and case.copies == []
