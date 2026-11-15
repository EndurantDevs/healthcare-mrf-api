# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host branch/SQL regressions; native COPY, actor authority and transactional sets require PostgreSQL qualification."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from process import reference_family_archive as archive
from process import reference_family_dictionary as dictionary


def _session():
    """Expose only explicit host SQL leaves, never a native authorization receipt."""
    return SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=False), execute=AsyncMock())


@pytest.fixture
def named_effects(monkeypatch):
    """Keep image/CAS branch tests independent of the separately tested catalog decoder."""
    edition = AsyncMock(return_value="named")
    monkeypatch.setattr(dictionary, "_effect_edition", edition)
    return edition


def _ownership():
    """Use the compiled seven-heap receive identity, not a portable five-table claim."""
    spec = archive.reference_family_receive_spec("drug-claims")
    return archive.ReferenceFamilyStageOwnership(
        spec.importer_id,
        UUID(int=1),
        "candidate",
        50,
        tuple(sorted((name, oid) for oid, name in enumerate(spec.table_names, 101))),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "schema", "name", "missing", "duplicate", "generation"))
async def test_current_binding_uses_recorded_names_and_rejects_logical_drift(monkeypatch, fault):
    ownership = _ownership()
    binding_rows = [["published-generation", "mrf", name] for name, _address in ownership.relation_oids]
    match fault:
        case "schema":
            binding_rows[0][1] = "other"
        case "name":
            binding_rows[0][2] = "changed"
        case "missing":
            binding_rows.pop()
        case "duplicate":
            binding_rows.append(list(binding_rows[0]))
        case "generation":
            binding_rows[0][0] = "other-generation"
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=(301, 302)))
    monkeypatch.setattr(archive, "require_native_read_catalog", AsyncMock())
    session = _session()
    session.execute.return_value = SimpleNamespace(all=lambda: binding_rows)
    incumbent = SimpleNamespace(importer_id="drug-claims", schema_name="mrf", relation_oids=ownership.relation_oids)
    if fault is None:
        await dictionary._require_terminal_current_binding(session, incumbent)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="unregistered"):
            await dictionary._require_terminal_current_binding(session, incumbent)
    statement, parameters = session.execute.await_args.args
    binding_sql = " ".join(str(statement).split())
    assert parameters == {"importer": "drug-claims"}
    assert "r.schema_name,r.relation_name" in binding_sql
    assert "ORDER BY r.schema_name,r.relation_name" in binding_sql
    assert "pg_class" not in binding_sql and "relation_oid" not in binding_sql


@pytest.mark.asyncio
async def test_prescription_copy_selects_all_referenced_endpoints_with_one_unchanged_budget():
    """Internal keys plus touching edges include external payloads without source-label ownership."""
    spec = archive.reference_family_spec("drug-claims")
    capture = SimpleNamespace(
        schema_name="synthetic_source",
        manifest=SimpleNamespace(tables=tuple(SimpleNamespace(table_name=name) for name in spec.table_names)),
        frozen_source_projection=None,
    )
    copy = SimpleNamespace(max_bytes=1000, copy_rows=AsyncMock(return_value=10))
    await archive._copy_source_tables("session", capture, "candidate", spec, copy, 10**12)
    calls = copy.copy_rows.await_args_list
    assert len(calls) == 5
    assert [call.kwargs["max_bytes"] for call in calls] == [1000, 990, 980, 970, 960]
    for call in calls[-2:]:
        assert '"synthetic_source".pricing_prescription' in call.args[1]
        assert "canonical.source=" not in call.args[1]
    assert '"synthetic_source".code_crosswalk' in calls[-2].args[1]
    assert "edge.from_system" in calls[-2].args[1] and "edge.to_system" in calls[-2].args[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("current_present", (False, True))
async def test_effect_capture_records_absence_original_baseline_and_exact_external_payload(
    current_present, named_effects
):
    """Native images preserve every dictionary field and do not mutate the shared relation."""
    session = _session()
    await dictionary._capture_dictionary_effect_table(
        session,
        "candidate",
        "mrf",
        dictionary._DRUG_SCOPED_MODELS[0],
        dictionary._DRUG_EFFECT_MODELS[0],
        current_present,
    )
    statements = [str(call.args[0]) for call in session.scalar.await_args_list]
    collision = statements[2 if current_present else 1]
    assert "to_jsonb(live) IS DISTINCT FROM to_jsonb(incoming)" in collision
    insertion = str(session.execute.await_args.args[0])
    assert insertion.lstrip().startswith('INSERT INTO "candidate"."drug_claims_catalog_effect"')
    assert (
        "to_jsonb(live)" in insertion
        and "baseline_image,before_image,after_image,destination_schema,destination_name" in insertion
    )
    assert "CASE WHEN to_jsonb(incoming) IS NULL" in insertion
    assert session.execute.await_args.args[1] == {"destination_schema": "mrf", "destination_name": "code_catalog"}
    assert "destination_oid" not in insertion
    assert "count(*)>100000" in statements[-1]
    if current_present:
        assert "UNION SELECT" in insertion
        assert "current_row.baseline_image" in insertion
        assert 'FROM "mrf"."drug_claims_catalog_effect" WHERE destination_schema IS DISTINCT' in statements[1]
        assert session.scalar.await_args_list[1].args[1] == {
            "destination_schema": "mrf",
            "destination_name": "code_catalog",
        }
        assert "current_row.after_image IS DISTINCT FROM to_jsonb(live)" in collision
    else:
        assert "current_row" not in insertion


@pytest.mark.asyncio
@pytest.mark.parametrize("check", range(2))
async def test_effect_capture_refuses_existing_stage_or_foreign_preimage_before_any_insert(check, named_effects):
    """Keep the independent destination fence clear before testing either capture refusal."""
    session = _session()
    session.scalar.side_effect = [False] * (2 * check) + [True]
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        await dictionary._capture_dictionary_effect_table(
            session, "candidate", "mrf", dictionary._DRUG_SCOPED_MODELS[0], dictionary._DRUG_EFFECT_MODELS[0], True
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rollback,failure",
    [
        (False, "binding"),
        (False, "preimage"),
        (True, "binding"),
        (True, "preimage"),
        (True, "incoming-binding"),
        (True, "baseline"),
    ],
)
async def test_all_dictionary_cas_checks_precede_shared_mutation(monkeypatch, rollback, failure, named_effects):
    """A late crosswalk fence refusal cannot follow an earlier catalog publication."""
    session = _session()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(201, 202)))
    writer = AsyncMock()
    monkeypatch.setattr(dictionary, "_write_dictionary_effects", writer)
    queries = []

    async def has_drift(statement, parameters=None):
        query = str(statement)
        queries.append((query, parameters))
        if '"drug_claims_crosswalk_effect"' not in query:
            return False
        if failure in {"binding", "incoming-binding"}:
            schema = "candidate" if not rollback or failure == "incoming-binding" else "mrf"
            return f'FROM "{schema}"."drug_claims_crosswalk_effect" WHERE destination_schema IS DISTINCT' in query
        image = "baseline_image" if failure == "baseline" else "after_image" if rollback else "before_image"
        return f"effect.{image} IS DISTINCT FROM to_jsonb(live)" in query

    session.scalar.side_effect = has_drift
    reason = "rollback key changed" if failure == "baseline" else "destination changed"
    with pytest.raises(archive.ReferenceFamilyArchiveError, match=reason):
        await dictionary.apply_reference_dictionary_effects(
            session, incoming_schema="candidate", current_schema="mrf", rollback=rollback
        )
    writer.assert_not_awaited()
    assert '"drug_claims_catalog_effect"' in queries[0][0]
    assert '"drug_claims_crosswalk_effect"' in queries[-1][0]
    if failure in {"binding", "incoming-binding"}:
        assert queries[-1][1] == {"destination_schema": "mrf", "destination_name": "code_crosswalk"}
    else:
        assert queries[-1][1] is None
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", (False, True))
async def test_both_dictionary_fences_finish_before_publication(monkeypatch, rollback, named_effects):
    """Both named destinations and complete image sets are fenced before either write."""
    session = _session()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock())
    events = []

    async def has_cas_drift(statement, parameters=None):
        events.append(("check", str(statement), parameters))
        return False

    async def write(_session, mirror, *_args):
        events.append(("write", mirror.__tablename__, None))

    session.scalar = has_cas_drift
    monkeypatch.setattr(dictionary, "_write_dictionary_effects", write)
    await dictionary.apply_reference_dictionary_effects(
        session, incoming_schema="candidate", current_schema="mrf", rollback=rollback
    )
    checks = iter(events[:-2])
    for mirror, effect in zip(dictionary._DRUG_SCOPED_MODELS, dictionary._DRUG_EFFECT_MODELS, strict=True):
        fence = f'"{"mrf" if rollback else "candidate"}"."{effect.__tablename__}"'
        binding_by_field = {"destination_schema": "mrf", "destination_name": mirror.__source_table__}
        kind, query, parameters = next(checks)
        assert kind == "check" and f"FROM {fence} WHERE destination_schema IS DISTINCT" in query
        assert parameters == binding_by_field
        kind, query, parameters = next(checks)
        image = "after_image" if rollback else "before_image"
        assert kind == "check" and f"effect.{image} IS DISTINCT FROM to_jsonb(live)" in query
        assert parameters is None
        if rollback:
            kind, query, parameters = next(checks)
            assert kind == "check" and f'FROM "candidate"."{effect.__tablename__}" WHERE destination_schema' in query
            assert parameters == binding_by_field
            kind, query, parameters = next(checks)
            assert kind == "check" and "effect.baseline_image IS DISTINCT FROM to_jsonb(live)" in query
            assert parameters is None
    assert list(checks) == []
    assert [event[0] for event in events] == ["check"] * (8 if rollback else 4) + ["write", "write"]
    assert [event[1] for event in events[-2:]] == [model.__tablename__ for model in dictionary._DRUG_SCOPED_MODELS]


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", (False, True))
async def test_key_publication_preserves_identical_foreign_rows_and_restores_current_only_baselines(rollback):
    session = _session()
    await dictionary._write_dictionary_effects(
        session,
        dictionary._DRUG_SCOPED_MODELS[0],
        dictionary._DRUG_EFFECT_MODELS[0],
        "retained",
        "mrf",
        "mrf",
        rollback,
    )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    deletion, insertion = statements
    assert deletion.startswith('DELETE FROM "mrf"."code_catalog"')
    assert "to_jsonb(live) IS DISTINCT FROM effect.image" in deletion
    assert "WHERE source" not in deletion
    assert "effect.image IS NOT NULL AND NOT EXISTS" in insertion
    assert "jsonb_populate_record" in insertion and "source_attribution" in insertion
    if rollback:
        assert "UNION ALL" in deletion and "current.baseline_image" in deletion
        assert "NOT EXISTS" in deletion
    assert "effect.image IS DISTINCT FROM to_jsonb(live)" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
async def test_effect_image_closure(monkeypatch, named_effects):
    session = _session()
    rollup = AsyncMock()
    monkeypatch.setattr(dictionary, "validate_prescription_rollup", rollup)
    await dictionary.validate_reference_dictionary_effects(session, _ownership())
    rollup.assert_awaited_once_with(session, "candidate", require_binding=True, provider_schema="mrf")
    assert session.scalar.await_count == 2
    for call in session.scalar.await_args_list:
        query = str(call.args[0])
        for image in ("baseline_image", "before_image", "after_image"):
            assert f"jsonb_typeof(effect.{image})" in query and f"effect.{image}->>" in query
            assert f"effect.{image})) IS DISTINCT FROM effect.{image}" in query
        assert "effect.destination_schema IS DISTINCT FROM :destination_schema" in query
        assert "count(DISTINCT (destination_schema,destination_name))>1" in query
        assert "destination_oid" not in query and "count(*)>100000" in query
        assert "effect.after_image IS DISTINCT FROM to_jsonb(incoming)" in query
        assert "effect.after_image IS DISTINCT FROM effect.baseline_image" in query


@pytest.mark.asyncio
@pytest.mark.parametrize("check", range(11))
async def test_each_prescription_set_invariant_refuses_invalid_source(check):
    session = _session()
    session.scalar.side_effect = [False] * check + [True]
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        await dictionary.validate_prescription_dictionary_closure(session, "candidate")


@pytest.mark.asyncio
async def test_prescription_rollup_uses_full_aggregate_and_declared_provider_name():
    session = _session()
    await dictionary.validate_prescription_rollup(session, "candidate", require_binding=True)
    statements = [str(call.args[0]) for call in session.scalar.await_args_list]
    aggregate, binding = statements
    assert "EXCEPT ALL" in aggregate and "SUM(total_drug_cost)" in aggregate and "ROW_NUMBER()" in aggregate
    assert "INSERT INTO" not in aggregate
    assert "source_relation_fingerprint IS DISTINCT FROM" in binding
    assert session.scalar.await_args.args[1] == {"provider": "mrf.pricing_provider_prescription"}
    assert "^[0-9]+$" in binding and "to_regclass" not in binding


@pytest.mark.asyncio
async def test_local_effects_keep_the_whole_receive_byte_ceiling(monkeypatch, named_effects):
    session = _session()
    session.scalar.return_value = 1001
    ownership = _ownership()
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    monkeypatch.setattr(dictionary, "validate_prescription_dictionary_closure", AsyncMock())
    monkeypatch.setattr(
        archive,
        "capture_reference_family_incumbent",
        AsyncMock(
            return_value=SimpleNamespace(
                relation_oids=tuple(
                    (name, None) for name in archive.reference_family_receive_spec("drug-claims").table_names
                )
            )
        ),
    )
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(201, 202)))
    monkeypatch.setattr(dictionary, "_capture_dictionary_effect_table", AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="byte bound"):
        await dictionary.prepare_reference_dictionary_effects(session, ownership, max_bytes=1000)
    query = str(session.scalar.await_args.args[0])
    assert query.count("sum(pg_column_size(row_value))") == 7
    assert all(name in query for name, _oid in ownership.relation_oids)


@pytest.mark.parametrize("importer_id", ("claims-pricing", "drug-claims"))
def test_terminal_bootstrap_accepts_only_complete_ordinary_heaps_without_scoped_adoption(importer_id):
    spec = archive.reference_family_receive_spec(importer_id)
    capability = archive.TERMINAL_CAPABILITIES[importer_id]
    scoped_names = {model.__tablename__ for model in (*capability.dictionary_models, *capability.effect_models)}
    pairs = tuple(
        (name, None if name in scoped_names else 101 + position) for position, name in enumerate(spec.table_names)
    )
    assert all(dictionary.terminal_incumbent_presence(importer_id, pairs))
    partial_pairs = tuple(
        (name, 500 if name == capability.dictionary_models[0].__tablename__ else oid) for name, oid in pairs
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="incomplete"):
        dictionary.terminal_incumbent_presence(importer_id, partial_pairs)


@pytest.mark.asyncio
@pytest.mark.parametrize("populated", (True, None))
async def test_terminal_empty_bootstrap_never_adopts_nonempty_or_unproven_serving_rows(monkeypatch, populated):
    session = _session()
    session.scalar.return_value = populated
    catalog = AsyncMock()
    monkeypatch.setattr(archive, "require_native_read_catalog", catalog)
    incumbent = SimpleNamespace(schema_name="mrf", relation_oids=(("pricing_prescription", 101),))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="not empty"):
        await dictionary.require_terminal_empty_incumbent(session, incumbent)
    catalog.assert_awaited_once_with(session, (101,))


@pytest.mark.asyncio
async def test_drug_direct_generic_activation_cannot_bypass_protected_preparation():
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="protected contribution"):
        await archive.activate_reference_family_stage(
            _session(), ownership=_ownership(), manifest={}, expected_incumbent=None, authority="manual"
        )


def _effect_catalog_rows(table, edition):
    mirror = dictionary._DRUG_SCOPED_MODELS[table]
    rows = [
        (column.name, f"character varying({32 if column.name.endswith('system') else 128})", True, "", "", False, True)
        for column in mirror.__table__.primary_key.columns
    ]
    rows.extend(
        (name, "jsonb", False, "", "", False, False) for name in ("baseline_image", "before_image", "after_image")
    )
    if edition == "legacy":
        rows.append(("destination_oid", "oid", True, "", "", False, False))
    else:
        rows.extend(
            (name, "character varying(63)", True, "", "", False, False)
            for name in ("destination_schema", "destination_name")
        )
    return rows


@pytest.mark.parametrize("table", (0, 1))
def test_fresh_effect_models_declare_only_named_destination_columns(table):
    model = dictionary._DRUG_EFFECT_MODELS[table]
    columns = model.__table__.columns
    assert "destination_oid" not in columns
    for name in ("destination_schema", "destination_name"):
        assert columns[name].nullable is False
        assert str(columns[name].type.compile(dialect=postgresql.dialect())) == "VARCHAR(63)"
    legacy = dictionary._LEGACY_DRUG_EFFECT_MODELS[table]
    assert legacy.__name__ == model.__name__ and legacy.__tablename__ == model.__tablename__
    assert tuple(column.name for column in legacy.__table__.primary_key.columns) == tuple(
        column.name for column in model.__table__.primary_key.columns
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("table", (0, 1))
@pytest.mark.parametrize("edition", ("named", "legacy"))
@pytest.mark.parametrize(
    "fault",
    (None, "missing", "extra", "duplicate", "type", "nullable", "default", "generated", "identity", "primary-key"),
)
async def test_effect_edition_admits_only_complete_recorded_column_shapes(table, edition, fault):
    catalog_rows = _effect_catalog_rows(table, edition)
    if fault == "missing":
        catalog_rows.pop()
    elif fault == "extra":
        catalog_rows.append(("extra", "text", False, "", "", False, False))
    elif fault == "duplicate":
        catalog_rows.append(catalog_rows[0])
    elif fault is not None:
        column_values = list(catalog_rows[0])
        offset, field_value = {
            "type": (1, "text"),
            "nullable": (2, False),
            "default": (5, True),
            "generated": (3, "s"),
            "identity": (4, "a"),
            "primary-key": (6, False),
        }[fault]
        column_values[offset] = field_value
        catalog_rows[0] = tuple(column_values)
    session = _session()
    session.execute.return_value = SimpleNamespace(all=lambda: catalog_rows)
    arguments = (session, dictionary._DRUG_SCOPED_MODELS[table], dictionary._DRUG_EFFECT_MODELS[table], "retained")
    if fault is None:
        assert await dictionary._effect_edition(*arguments) == edition
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="column edition differs"):
            await dictionary._effect_edition(*arguments)
    statement, parameters = session.execute.await_args.args
    assert "pg_attribute" in str(statement) and "to_regclass(:relation)" in str(statement)
    assert parameters == {"relation": f'"retained"."{dictionary._DRUG_EFFECT_MODELS[table].__tablename__}"'}
    assert "destination_oid" not in str(statement)
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("editions", (("legacy", "legacy"), ("legacy", "named"), ("named", "legacy")))
async def test_unfinished_legacy_preparation_requires_explicit_transition_before_any_write(monkeypatch, editions):
    session = _session()
    saved_rows = tuple(tuple(_effect_catalog_rows(table, edition)) for table, edition in enumerate(editions))
    results = iter(saved_rows)
    session.execute.side_effect = lambda *_args: SimpleNamespace(all=lambda: next(results))
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    monkeypatch.setattr(dictionary, "validate_prescription_dictionary_closure", AsyncMock())
    capture = AsyncMock()
    monkeypatch.setattr(archive, "capture_reference_family_incumbent", capture)
    with pytest.raises(dictionary.LegacyDictionaryEffectWriteRequired) as refusal:
        await dictionary.prepare_reference_dictionary_effects(session, _ownership(), max_bytes=1000)
    assert refusal.value.action == "transition-legacy-dictionary-effect-tables"
    assert refusal.value.relations == tuple(
        ("candidate", dictionary._DRUG_EFFECT_MODELS[table].__tablename__)
        for table, edition in enumerate(editions)
        if edition == "legacy"
    )
    assert all(str(call.args[0]).startswith("SELECT a.attname") for call in session.execute.await_args_list)
    assert saved_rows == tuple(tuple(_effect_catalog_rows(table, edition)) for table, edition in enumerate(editions))
    capture.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_effect_closure_preserves_images_without_interpreting_saved_identifiers(
    monkeypatch, named_effects
):
    named_effects.return_value = "legacy"
    session = _session()
    rollup = AsyncMock()
    monkeypatch.setattr(dictionary, "validate_prescription_rollup", rollup)
    await dictionary.validate_reference_dictionary_effects(session, _ownership(), destination_schema="serving")
    rollup.assert_awaited_once_with(session, "candidate", require_binding=True, provider_schema="serving")
    for call in session.scalar.await_args_list:
        query = str(call.args[0])
        assert "destination_oid" not in query and "destination_schema" not in query
        for image in ("baseline_image", "before_image", "after_image"):
            assert f"jsonb_typeof(effect.{image})" in query
            assert f"jsonb_populate_record(NULL::" in query
        assert "effect.after_image IS DISTINCT FROM to_jsonb(incoming)" in query
        assert "effect.after_image IS DISTINCT FROM effect.baseline_image" in query
        assert "count(*)>100000" in query
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", (False, True))
@pytest.mark.parametrize("drift", (False, True))
async def test_legacy_read_and_rollback_still_fence_complete_images(monkeypatch, named_effects, rollback, drift):
    named_effects.return_value = "legacy"
    session = _session()
    lock, writer = AsyncMock(), AsyncMock()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", lock)
    monkeypatch.setattr(dictionary, "_write_dictionary_effects", writer)
    session.scalar.side_effect = [False] * (3 if rollback else 1) + [drift]
    arguments_by_field = dict(
        incoming_schema="retained", current_schema="current", destination_schema="serving", rollback=rollback
    )
    if drift:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="destination changed|rollback key changed"):
            await dictionary.apply_reference_dictionary_effects(session, **arguments_by_field)
        writer.assert_not_awaited()
    else:
        await dictionary.apply_reference_dictionary_effects(session, **arguments_by_field)
        assert writer.await_count == 2
    lock.assert_awaited_once_with(session, "serving")
    assert named_effects.await_count == (4 if rollback else 2)
    for call in session.scalar.await_args_list:
        assert '"serving".' in str(call.args[0]) and "IS DISTINCT FROM to_jsonb(live)" in str(call.args[0])
        assert "destination_oid" not in str(call.args[0])
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_preparation_records_publication_qnames_and_never_rewrites_old_receipts(monkeypatch, named_effects):
    session = _session()
    session.scalar.return_value = 1000
    ownership = _ownership()
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    monkeypatch.setattr(dictionary, "validate_prescription_dictionary_closure", AsyncMock())
    monkeypatch.setattr(
        archive,
        "capture_reference_family_incumbent",
        AsyncMock(
            return_value=SimpleNamespace(
                relation_oids=tuple(
                    (name, None) for name in archive.reference_family_receive_spec("drug-claims").table_names
                )
            )
        ),
    )
    monkeypatch.setattr(dictionary, "require_terminal_empty_incumbent", AsyncMock())
    lock, capture = AsyncMock(), AsyncMock()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", lock)
    monkeypatch.setattr(dictionary, "_capture_dictionary_effect_table", capture)
    await dictionary.prepare_reference_dictionary_effects(
        session, ownership, destination_schema="serving", max_bytes=1000
    )
    assert named_effects.await_count == 2
    lock.assert_awaited_once_with(session, "serving")
    statement, parameters = session.execute.await_args.args
    assert str(statement).lstrip().startswith('UPDATE "candidate".pricing_provider_rx_rollup')
    assert parameters == {"provider": "serving.pricing_provider_prescription"}
    assert [call.args for call in capture.await_args_list] == [
        (session, "candidate", "serving", mirror, effect, False)
        for mirror, effect in zip(dictionary._DRUG_SCOPED_MODELS, dictionary._DRUG_EFFECT_MODELS, strict=True)
    ]
