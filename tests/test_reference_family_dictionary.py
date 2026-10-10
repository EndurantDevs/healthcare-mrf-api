# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host branch/SQL regressions; native COPY, actor authority and transactional sets require PostgreSQL qualification."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as archive
from process import reference_family_dictionary as dictionary


def _session():
    """Expose only explicit host SQL leaves, never a native authorization receipt."""
    return SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=False), execute=AsyncMock())


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
@pytest.mark.parametrize("fault", (None, "name", "oid", "missing", "duplicate", "generation"))
async def test_current_binding_joins_retained_oids_to_native_names_and_rejects_drift(monkeypatch, fault):
    """The retained child stores only generation/OID; native names keep the three-field row contract."""
    ownership = _ownership()
    binding_rows = [["published-generation", name, oid] for name, oid in ownership.relation_oids]
    match fault:
        case "name":
            binding_rows[0][1] = "changed"
        case "oid":
            binding_rows[0][2] += 1
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
    incumbent = SimpleNamespace(importer_id="drug-claims", relation_oids=ownership.relation_oids)
    if fault is None:
        await dictionary._require_terminal_current_binding(session, incumbent)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="unregistered"):
            await dictionary._require_terminal_current_binding(session, incumbent)
    statement, parameters = session.execute.await_args.args
    binding_sql = " ".join(str(statement).split())
    assert parameters == {"importer": "drug-claims"}
    assert "heap.relname::text AS relation_name,r.relation_oid::bigint" in binding_sql
    assert "JOIN pg_catalog.pg_class heap ON heap.oid::bigint=r.relation_oid" in binding_sql
    assert "ORDER BY heap.relname" in binding_sql
    assert "r.relation_name" not in binding_sql and "r.table_name" not in binding_sql


@pytest.mark.asyncio
async def test_prescription_copy_selects_all_referenced_endpoints_with_one_unchanged_budget():
    """Internal keys plus touching edges include external payloads without source-label ownership."""
    spec = archive.reference_family_spec("drug-claims")
    capture = SimpleNamespace(
        schema_name="synthetic_source",
        manifest=SimpleNamespace(tables=tuple(SimpleNamespace(table_name=name) for name in spec.table_names)),
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
async def test_effect_capture_records_absence_original_baseline_and_exact_external_payload(current_present):
    """Native images preserve every dictionary field and do not mutate the shared relation."""
    session = _session()
    await dictionary._capture_dictionary_effect_table(
        session,
        "candidate",
        "mrf",
        dictionary._DRUG_SCOPED_MODELS[0],
        dictionary._DRUG_EFFECT_MODELS[0],
        201,
        current_present,
    )
    statements = [str(call.args[0]) for call in session.scalar.await_args_list]
    collision = statements[2 if current_present else 1]
    assert "to_jsonb(live) IS DISTINCT FROM to_jsonb(incoming)" in collision
    assert session.scalar.await_args_list[2 if current_present else 1].args[1] == {"oid": 201}
    insertion = str(session.execute.await_args.args[0])
    assert insertion.lstrip().startswith('INSERT INTO "candidate"."drug_claims_catalog_effect"')
    assert "to_jsonb(live)" in insertion and "baseline_image,before_image,after_image,destination_oid" in insertion
    assert "CASE WHEN to_jsonb(incoming) IS NULL" in insertion
    assert "count(*)>100000" in statements[-1]
    if current_present:
        assert "UNION SELECT" in insertion
        assert "current_row.baseline_image" in insertion
        assert 'FROM "mrf"."drug_claims_catalog_effect" WHERE destination_oid IS DISTINCT FROM :oid' in statements[1]
        assert session.scalar.await_args_list[1].args[1] == {"oid": 201}
        assert "current_row.after_image IS DISTINCT FROM to_jsonb(live)" in collision
    else:
        assert "current_row" not in insertion


@pytest.mark.asyncio
@pytest.mark.parametrize("check", range(2))
async def test_effect_capture_refuses_existing_stage_or_foreign_preimage_before_any_insert(check):
    """Keep the independent destination fence clear before testing either capture refusal."""
    session = _session()
    session.scalar.side_effect = [False] * (2 * check) + [True]
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        await dictionary._capture_dictionary_effect_table(
            session, "candidate", "mrf", dictionary._DRUG_SCOPED_MODELS[0], dictionary._DRUG_EFFECT_MODELS[0], 201, True
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
async def test_all_dictionary_cas_checks_precede_shared_mutation(monkeypatch, rollback, failure):
    """A late crosswalk fence refusal cannot follow an earlier catalog publication."""
    session = _session()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(201, 202)))
    writer = AsyncMock()
    monkeypatch.setattr(dictionary, "_write_dictionary_effects", writer)
    queries = []

    async def has_drift(statement, parameters):
        query = str(statement)
        queries.append((query, parameters))
        if '"drug_claims_crosswalk_effect"' not in query:
            return False
        if failure in {"binding", "incoming-binding"}:
            schema = "candidate" if not rollback or failure == "incoming-binding" else "mrf"
            return f'FROM "{schema}"."drug_claims_crosswalk_effect" WHERE destination_oid IS DISTINCT' in query
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
    assert queries[-1][1] == {"oid": 202}
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", (False, True))
async def test_both_dictionary_fences_finish_before_publication(monkeypatch, rollback):
    """Both destination bindings and complete image sets are fenced before either write."""
    session = _session()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(201, 202)))
    events = []

    async def has_cas_drift(statement, parameters):
        events.append(("check", str(statement), parameters))
        return False

    async def write(_session, mirror, *_args):
        events.append(("write", mirror.__tablename__, None))

    session.scalar = has_cas_drift
    monkeypatch.setattr(dictionary, "_write_dictionary_effects", write)
    await dictionary.apply_reference_dictionary_effects(
        session, incoming_schema="candidate", current_schema="mrf", rollback=rollback
    )
    expected_checks = []
    for effect, oid in zip(dictionary._DRUG_EFFECT_MODELS, (201, 202), strict=True):
        fence = f'"{"mrf" if rollback else "candidate"}"."{effect.__tablename__}"'
        image = "after_image" if rollback else "before_image"
        expected_checks.extend(
            [
                (f"FROM {fence} WHERE destination_oid IS DISTINCT FROM :oid", oid),
                (f"FROM {fence} effect LEFT JOIN", oid),
            ]
        )
        if rollback:
            expected_checks.extend(
                [
                    (f'FROM "candidate"."{effect.__tablename__}" WHERE destination_oid IS DISTINCT', oid),
                    ("effect.baseline_image IS DISTINCT FROM to_jsonb(live)", oid),
                ]
            )
        assert (
            f"effect.{image} IS DISTINCT FROM to_jsonb(live)"
            in events[len(expected_checks) - (3 if rollback else 1)][1]
        )
    assert [event[0] for event in events] == ["check"] * len(expected_checks) + ["write", "write"]
    for (kind, query, parameters), (fragment, oid) in zip(events[:-2], expected_checks, strict=True):
        assert kind == "check" and fragment in query and parameters == {"oid": oid}
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
async def test_effect_image_closure(monkeypatch):
    session = _session()
    rollup = AsyncMock()
    monkeypatch.setattr(dictionary, "validate_prescription_rollup", rollup)
    await dictionary.validate_reference_dictionary_effects(session, _ownership())
    rollup.assert_awaited_once_with(session, "candidate", require_binding=True)
    assert session.scalar.await_count == 2
    for call in session.scalar.await_args_list:
        query = str(call.args[0])
        for image in ("baseline_image", "before_image", "after_image"):
            assert f"jsonb_typeof(effect.{image})" in query and f"effect.{image}->>" in query
            assert f"effect.{image})) IS DISTINCT FROM effect.{image}" in query
        assert "effect.destination_oid=0" in query and "count(*)>100000" in query
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
async def test_prescription_rollup_uses_actual_producer_aggregate_and_local_oid_binding():
    session = _session()
    await dictionary.validate_prescription_rollup(session, "candidate", require_binding=True)
    statements = [str(call.args[0]) for call in session.scalar.await_args_list]
    aggregate, binding = statements
    assert "EXCEPT ALL" in aggregate and "SUM(total_drug_cost)" in aggregate and "ROW_NUMBER()" in aggregate
    assert "INSERT INTO" not in aggregate
    assert "source_relation_fingerprint IS DISTINCT FROM" in binding
    assert session.scalar.await_args.args[1] == {"provider": '"candidate"."pricing_provider_prescription"'}


@pytest.mark.asyncio
async def test_local_effects_keep_the_whole_receive_byte_ceiling(monkeypatch):
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
