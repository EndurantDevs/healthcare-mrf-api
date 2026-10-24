# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed-family application boundaries; native catalog/role proof is separate."""

import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_immutable as immutable
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process import reference_source_generation as revisions


def _authority(generation_number=1):
    return generation.ReferenceFamilyResultGenerationAuthority(
        "cms-doctors",
        "00000000-0000-0000-0000-000000000001",
        generation_number,
        generation.ReferenceFamilyServingGeneration(
            "00000000-0000-0000-0000-000000000001",
            generation_number,
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        ),
        (11, 12, 13),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("adopt", [False, True, None])
async def test_generation_cas_preserves_exact_origin_and_never_installs_hooks(monkeypatch, adopt):
    expected = _authority()
    monkeypatch.setattr(immutable, "_writer", AsyncMock())
    custody = AsyncMock(return_value=tuple(zip(generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"], (21, 22, 23))))
    monkeypatch.setattr(immutable, "require_doctors_storage", custody)
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=expected)
    )
    installer = AsyncMock(side_effect=AssertionError("sealed publication cannot install hooks"))
    monkeypatch.setattr(revisions, "install_reference_revision_guards", installer)
    statements = []

    async def update(session, statement, **fields):
        statements.append((str(statement), fields))
        return dict(
            importer_id="cms-doctors",
            local_lineage_id=expected.local_lineage_id,
            local_generation=fields["next"],
            origin_lineage_id=fields["origin"],
            origin_generation=fields["generation"],
            relation_oids=fields["oids"],
            published_at=fields["published"] if adopt is not False else expected.serving_generation.published_at,
        )

    monkeypatch.setattr(generation, "_first", update)
    source_generation = expected.serving_generation if adopt else None
    if adopt is False:
        published_authority = await immutable.publish_doctors_generation(
            object(), schema_name="mrf", expected_authority=expected
        )
    else:
        published_authority = await immutable.adopt_doctors_generation(
            object(), schema_name="mrf", expected_authority=expected, source_generation=source_generation
        )
    statement, fields = statements[0]
    assert "source_revision_tracked=FALSE" in statement
    assert "relation_oids IS NOT DISTINCT FROM" in statement
    assert fields["previous_oids"] == [11, 12, 13] and fields["prior"] == 1
    assert published_authority.local_generation == (2 if adopt is False else 1)
    assert published_authority.relation_oids == (None if adopt is None else (21, 22, 23))
    assert published_authority.serving_generation == (
        None
        if adopt is None
        else source_generation
        if adopt
        else generation.ReferenceFamilyServingGeneration(
            expected.local_lineage_id, 2, expected.serving_generation.published_at
        )
    )
    installer.assert_not_awaited()
    custody.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["storage", "predecessor", "cas"])
async def test_failed_native_custody_or_cas_cannot_advance_authority(monkeypatch, failure):
    expected = _authority()
    monkeypatch.setattr(immutable, "_writer", AsyncMock())
    monkeypatch.setattr(
        immutable,
        "require_doctors_storage",
        AsyncMock(
            side_effect=RuntimeError("custody") if failure == "storage" else None,
            return_value=(("a", 21), ("b", 22), ("c", 23)),
        ),
    )
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=_authority(2) if failure == "predecessor" else expected),
    )
    update = AsyncMock(return_value=None)
    monkeypatch.setattr(generation, "_first", update)
    with pytest.raises(RuntimeError):
        await immutable.publish_doctors_generation(object(), schema_name="mrf", expected_authority=expected)
    assert update.await_count == (failure == "cas")


@pytest.mark.asyncio
async def test_obsolete_doctors_installer_refuses_before_any_ddl():
    database = SimpleNamespace(status=AsyncMock(), execute=AsyncMock())
    with pytest.raises(RuntimeError, match="native_publication_required"):
        await revisions.install_reference_revision_guards(database, importer_id="cms-doctors", schema_name="mrf")
    database.status.assert_not_awaited()
    database.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_manual_archive_adoption_uses_native_cas_and_never_installs_hooks(monkeypatch):
    current = _authority()
    native = AsyncMock(return_value=current)
    monkeypatch.setattr(immutable, "adopt_doctors_generation", native)
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=current)
    )
    installer = AsyncMock(side_effect=AssertionError("manual adoption cannot install hooks"))
    monkeypatch.setattr(revisions, "install_reference_revision_guards", installer)
    session = object()
    assert (
        await generation.publish_adopted_reference_family_generation(
            session,
            importer_id="cms-doctors",
            schema_name="mrf",
            source_generation=None,
        )
        == current
    )
    native.assert_awaited_once_with(session, schema_name="mrf", expected_authority=current, source_generation=None)
    with pytest.raises(ValueError, match="cannot_install_revision_hooks"):
        await generation.publish_adopted_reference_family_generation(
            session,
            importer_id="cms-doctors",
            schema_name="mrf",
            source_generation=current.serving_generation,
            source_revision_tracked=True,
        )
    assert native.await_count == 1
    installer.assert_not_awaited()


@pytest.mark.asyncio
async def test_private_stage_refuses_unknown_model_before_acquiring_authority():
    with pytest.raises(RuntimeError, match="stage_identity_invalid"):
        await immutable.create_doctors_stage(object(), object(), "synthetic", "mrf")


@pytest.mark.asyncio
async def test_prepared_seal_requires_exact_native_custody_and_filenodes(monkeypatch):
    from process import cms_doctors_preparation as prepared_module

    prepared = SimpleNamespace(
        schema="mrf",
        stage_oids=(("a", "stage_a", 21), ("b", "stage_b", 22), ("c", "stage_c", 23)),
        sealed_filenodes=((21, 31), (22, 32), (23, 33)),
    )
    custody = AsyncMock()
    monkeypatch.setattr(immutable, "require_doctors_storage", custody)
    monkeypatch.setattr(prepared_module, "_assert_stage", AsyncMock(side_effect=[31, 32, 33, 31, 99]))
    session = object()
    await prepared_module.assert_prepared_cms_doctors_seal(session, prepared)
    custody.assert_awaited_with(
        session, schema_name="mrf", relation_pairs=(("stage_a", 21), ("stage_b", 22), ("stage_c", 23))
    )
    with pytest.raises(RuntimeError, match="physical_seal_changed"):
        await prepared_module.assert_prepared_cms_doctors_seal(session, prepared, applied=True)


def test_fixed_native_models_keep_complete_types_and_no_implicit_defaults():
    for model in immutable._models():
        columns = immutable._column_shape(model)
        assert len(columns) == len(model.__table__.columns)
        assert {name for name, _, _ in columns} == set(model.__table__.columns.keys())
        assert not model.__table__.foreign_keys
        assert all(column.server_default is None for column in model.__table__.columns)


def test_legacy_group_indexes_never_relax_immutable_current():
    group = immutable._models()[2]
    required_indexes = {(True, ("row_number",)), (False, ("npi",)), (False, ("org_pac_id",)), (False, ("adrs_id",))}
    old = required_indexes - {(False, ("adrs_id",))}
    assert immutable._index_shape_matches(group, required_indexes, required_indexes, False)
    assert immutable._index_shape_matches(group, required_indexes, old, True)
    assert not immutable._index_shape_matches(group, required_indexes, old, False)
    assert not immutable._index_shape_matches(group, required_indexes, old - {(False, ("npi",))}, True)
    assert not immutable._index_shape_matches(group, required_indexes, old | {(False, ("facility_name",))}, True)
    assert not immutable._index_shape_matches(immutable._models()[0], required_indexes, old, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_incumbent", (False, True))
async def test_geo_profiles_reject_index_drift(is_incumbent):
    from itertools import combinations

    from db.models import GeoZipLookup
    from process.geo_source_storage import GEO_LEGACY_INDEX_PROFILES

    required = {(True, ("zip_code",))} | {
        (False, tuple(index["index_elements"])) for index in GeoZipLookup.__my_additional_indexes__
    }
    assert len(required) == 6
    coordinates = (False, ("latitude", "longitude", "zip_code"))
    assert coordinates in required
    assert GEO_LEGACY_INDEX_PROFILES == ({(True, ("zip_code",)), coordinates}, required - {coordinates})
    profiles = [list(keys) for count in range(7) for keys in combinations(required, count)]
    profiles.extend([list(required) + [entry] for entry in (*required, (False, ("population",)))])
    for indexes in profiles:
        session = SimpleNamespace(
            execute=AsyncMock(
                side_effect=[
                    SimpleNamespace(all=lambda: immutable._column_shape(GeoZipLookup)),
                    SimpleNamespace(all=lambda: indexes),
                ]
            ),
            scalar=AsyncMock(return_value=["zip_code"]),
        )
        accepted = len(indexes) == len(set(indexes)) and (
            set(indexes) == required or (is_incumbent and set(indexes) in GEO_LEGACY_INDEX_PROFILES)
        )
        if accepted:
            await immutable._shape(session, GeoZipLookup, 17, indexed=True, is_geo_incumbent=is_incumbent)
        else:
            with pytest.raises(RuntimeError, match="indexes_(incomplete|duplicated)"):
                await immutable._shape(session, GeoZipLookup, 17, indexed=True, is_geo_incumbent=is_incumbent)


def test_immutable_archive_contract_accepts_only_supported_family():
    archive._validate_source_capture_contract(
        dict(
            importer_id="cms-doctors",
            tables=[{}, {}, {}],
            publication_authority="tracked-generation",
            source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
        )
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        archive._validate_source_capture_contract(
            dict(
                importer_id="geo",
                publication_authority="tracked-generation",
                source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
            )
        )


@pytest.mark.parametrize("seal,native", [(False, False), (False, True), (True, False), (True, True)])
def test_legacy_hook_census_requires_complete_exact_historical_contracts(seal, native):
    contracts = immutable._legacy_contracts("mrf")
    oids = [11, 12, 13]
    guard_rows = []
    for name, (kind, function, security, body) in contracts.items():
        if name == "cms_doctors_prepared_write_seal" and not seal:
            continue
        if name == "cms_native_input_mutation" and not native:
            continue
        for oid in oids[:1] if name == "cms_native_input_mutation" else oids:
            guard_rows.append(
                dict(
                    oid=oid,
                    name=name,
                    tgtype=kind,
                    function=function,
                    body=body,
                    prosecdef=security,
                    proconfig=["search_path=pg_catalog"],
                    shape=True,
                    returns_trigger=True,
                    lanname="plpgsql",
                    enabled="A",
                )
            )
    assert immutable._validate_legacy_hooks(guard_rows, oids, contracts) == ([11] if native else ())
    damaged_rows = [dict(guard_row) for guard_row in guard_rows]
    damaged_rows[0]["body"] += " SELECT 1;"
    with pytest.raises(RuntimeError, match="legacy_hooks_changed"):
        immutable._validate_legacy_hooks(damaged_rows, oids, contracts)
    damaged_rows = [dict(guard_row) for guard_row in guard_rows]
    damaged_rows[0]["enabled"] = "O"
    with pytest.raises(RuntimeError, match="legacy_hooks_changed"):
        immutable._validate_legacy_hooks(damaged_rows, oids, contracts)


@pytest.mark.asyncio
async def test_doctors_archive_cut_savepoint_rolls_back_even_when_caller_catches(monkeypatch):
    from contextlib import asynccontextmanager

    events = []

    @asynccontextmanager
    async def savepoint():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    session = SimpleNamespace(begin_nested=savepoint)
    try:
        async with archive._doctors_cut_savepoint(session, "cms-doctors"):
            events.append("swap")
            raise RuntimeError("late cut failure")
    except RuntimeError as error:
        assert str(error) == "late cut failure"
    assert events == ["begin", "swap", "rollback"]
    events.clear()
    from process.provider_directory_generic_source_policy import source_transaction
    from tests.test_provider_directory_generic_source_policy import synthetic_source_database

    async with synthetic_source_database(monkeypatch, SimpleNamespace()) as database:
        async with source_transaction(database) as source_session:
            source_session.begin_nested = savepoint
            original_fetchrow = source_session.fetchrow

            async def actor_query(statement, *arguments):
                actor = await original_fetchrow(statement, *arguments)
                if statement == "SELECT session_user::text s,current_user::text c":
                    return {"s": actor["session"], "c": actor["current"]}
                return actor

            monkeypatch.setattr(source_session, "fetchrow", actor_query)
            async with archive._doctors_cut_savepoint(source_session, "geo"):
                events.append("unchanged")
    assert events == ["begin", "unchanged", "commit"]


@pytest.mark.asyncio
async def test_native_address_registration_attests_doctors_without_installing_a_mutation_hook(monkeypatch):
    from process import provider_directory_cms_native_inputs as inputs

    schema = "mrf"
    relations_by_name = {
        '"mrf"."doctor_clinician_address"': {"relation_oid": 11},
        '"mrf"."address_alias_v1"': {"relation_oid": 17},
    }
    monkeypatch.setattr(inputs, "_physical_relations", AsyncMock(return_value=relations_by_name))
    monkeypatch.setattr(inputs, "_require_heaps", lambda observed: None)
    monkeypatch.setattr(inputs, "_lock_inputs", AsyncMock())
    monkeypatch.setattr(
        inputs, "_relations", lambda schema: ((schema, "doctor_clinician_address"), (schema, "address_alias_v1"))
    )
    custody = AsyncMock()
    register = AsyncMock()
    monkeypatch.setattr(immutable, "require_doctors_storage", custody)
    monkeypatch.setattr(inputs, "_register_relation", register)
    monkeypatch.setattr(inputs, "_revision_guards", AsyncMock())
    from process import geo_source_storage

    geo = AsyncMock()
    monkeypatch.setattr(geo_source_storage, "capture_geo_generation", geo)
    session = object()
    await inputs.register_native_address_inputs(session, schema)
    geo.assert_awaited_once_with(session, schema)
    custody.assert_awaited_once_with(session, schema_name=schema)
    register.assert_awaited_once_with(session, schema, schema, "address_alias_v1", 17)


@pytest.mark.asyncio
async def test_cold_native_hooks_do_not_invent_or_require_historical_tracking(monkeypatch):
    contracts = immutable._legacy_contracts("mrf")
    kind, function, security, body = contracts[revisions.REVISION_TRIGGER]
    guards = [
        dict(
            oid=oid,
            name=revisions.REVISION_TRIGGER,
            tgtype=kind,
            function=function,
            body=body,
            prosecdef=security,
            proconfig=["search_path=pg_catalog"],
            shape=True,
            returns_trigger=True,
            lanname="plpgsql",
            enabled="A",
        )
        for oid in (11, 12, 13)
    ]
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: guards)))
    )
    monkeypatch.setattr(revisions, "_guarded_oids", AsyncMock(return_value=(11, 12, 13)))
    tracking = AsyncMock(side_effect=AssertionError("native custody cannot manufacture historical tracking"))
    monkeypatch.setattr(revisions, "require_reference_revision_tracking", tracking)
    await immutable._legacy_hooks(session, "mrf", [11, 12, 13])
    tracking.assert_not_awaited()
    cold = generation.ReferenceFamilyResultGenerationAuthority(
        "cms-doctors", "00000000-0000-0000-0000-000000000001", 0, None, None
    )
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=cold))
    monkeypatch.setattr(generation, "current_reference_family_relation_oids", AsyncMock(return_value=(11, 12, 13)))
    with pytest.raises(RuntimeError, match="serving generation is unavailable"):
        await generation.capture_reference_family_serving_generation(
            session, importer_id="cms-doctors", schema_name="mrf"
        )


def test_legacy_reference_body_is_exact_actual_migration_output(monkeypatch):
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260929040000_reference_source_generation_guard.py"
    spec = importlib.util.spec_from_file_location("doctors_native_legacy_contract", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    statements = []
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    migration._revision_function("mrf")
    actual = statements[0].split("$function$")[1]
    expected = immutable._legacy_contracts("mrf")[revisions.REVISION_TRIGGER][3]
    assert " ".join(actual.split()) == " ".join(expected.split())


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "schema", "owner", "bound_policy"])
async def test_admin_executable_subjects_are_derived_from_the_actual_native_policy(monkeypatch, change):
    from process import plan_release_storage_custody as custody
    from process import provider_directory_generic_source_policy as source
    from process import provider_directory_projection_stage as stage

    policy = source.GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    driver = object()
    native_roles = AsyncMock(return_value=(10, [12]))
    monkeypatch.setattr(custody, "storage_roles", native_roles)
    monkeypatch.setattr(stage, "_copy_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(immutable, "_owner", AsyncMock(return_value=11 if change == "owner" else 10))
    token = None
    if change == "bound_policy":
        token = source._PUBLISHER.set(object())
        monkeypatch.setattr(source, "bound_publisher", lambda: SimpleNamespace(policy=object()))
    try:
        if change:
            with pytest.raises(RuntimeError, match="native_policy_changed|owner_tuple_changed"):
                await immutable._executable_subjects(
                    object(), "wrong_schema" if change == "schema" else "source_schema", policy
                )
        else:
            assert await immutable._executable_subjects(object(), "source_schema", policy) == (
                "source_owner",
                "source_publisher",
            )
            native_roles.assert_awaited_once_with(driver, "source_schema", **policy.role_arguments())
        if change in {"schema", "bound_policy"}:
            native_roles.assert_not_awaited()
    finally:
        if token is not None:
            source._PUBLISHER.reset(token)


@pytest.mark.asyncio
async def test_retention_publisher_cannot_replace_source_writer_capability(monkeypatch):
    retained = AsyncMock(return_value=40)
    monkeypatch.setattr(archive, "protected_publisher_owner", retained)
    with pytest.raises(ValueError, match="publisher_connection_required"):
        await immutable._writer(object())
    retained.assert_not_awaited()


@pytest.mark.asyncio
async def test_read_owner_uses_protected_source_header_without_retention_authority():
    session = SimpleNamespace(scalar=AsyncMock(return_value=31))
    assert await immutable._owner(session, "source_schema") == 31
    statement, params = session.scalar.await_args.args
    assert "provider_directory_endpoint_dataset" in str(statement)
    assert "hp_snapshot_retention" not in str(statement)
    assert "NOT r.rolcanlogin" in str(statement) and "elevated" in str(statement)
    assert params == {"schema": "source_schema"}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["name", "pointer"])
async def test_read_only_custody_requires_exact_canonical_pointer(monkeypatch, change):
    current = _authority()
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=current)
    )
    pairs = tuple(zip(generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"], (11, 12, 13)))
    if change == "name":
        pairs = (("private_stage", 11), *pairs[1:])
    else:
        pairs = ((pairs[0][0], 21), *pairs[1:])
    with pytest.raises(RuntimeError, match="read_pointer_changed"):
        await immutable._require_read_pointer(object(), "source_schema", pairs)


@pytest.mark.asyncio
async def test_other_model_family_seal_preserves_existing_retention_authority(monkeypatch):
    from process import entity_address_snapshot_preparation as retained

    seal = AsyncMock()
    monkeypatch.setattr(retained, "_seal_published_relation", seal)
    ownership = SimpleNamespace(importer_id="nucc")
    session = object()
    await immutable.seal_model_family_relation(session, ownership, 23, 41)
    seal.assert_awaited_once_with(session, 23, 41)


@pytest.mark.asyncio
async def test_source_archive_seal_refuses_wrong_physical_owner_before_grant_changes(monkeypatch):
    from process import provider_directory_generic_source_policy as source
    from process import provider_directory_generic_source_storage as storage

    owner = AsyncMock(return_value=31)
    close = AsyncMock()
    monkeypatch.setattr(immutable, "_writer", owner)
    monkeypatch.setattr(storage, "_close_grants", close)
    token = source._PUBLISHER.set(object())
    try:
        with pytest.raises(RuntimeError, match="owner_tuple_changed"):
            await immutable.seal_model_family_relation(object(), SimpleNamespace(importer_id="cms-doctors"), 23, 41)
    finally:
        source._PUBLISHER.reset(token)
    close.assert_not_awaited()
