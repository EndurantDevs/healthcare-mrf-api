# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed-family application boundaries; native catalog/role proof is separate."""

import asyncio
import datetime
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import cms_doctors_immutable as immutable
from process import native_publication_read_access as access
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
        tuple(("mrf", name) for name in generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]),
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
            serving_schema=fields["serving_schema"],
            serving_relation_names=fields["names"],
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
    assert "serving_schema IS NOT DISTINCT FROM" in statement
    assert "serving_relation_names IS NOT DISTINCT FROM" in statement
    assert fields["previous_schema"] == "mrf"
    assert fields["previous_names"] == list(generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"])
    assert fields["prior"] == 1
    assert published_authority.local_generation == (2 if adopt is False else 1)
    assert published_authority.relations == (None if adopt is None else expected.relations)
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
async def test_prepared_family_requires_complete_named_native_storage(monkeypatch):
    from process import cms_doctors_preparation as prepared_module

    native = prepared_module._native()
    stages = tuple(
        (model.__tablename__, native.make_class(model, "synthetic").__tablename__)
        for model in prepared_module._models()
    )
    prepared = SimpleNamespace(schema="mrf", import_date="synthetic", stage_relations=stages)
    storage = AsyncMock()
    monkeypatch.setattr(immutable, "require_doctors_storage", storage)
    session = object()
    await prepared_module.assert_prepared_cms_doctors_seal(session, prepared)
    storage.assert_awaited_with(session, schema_name="mrf", relation_names=tuple(stage for _, stage in stages))
    await prepared_module.assert_prepared_cms_doctors_seal(session, prepared, applied=True)
    storage.assert_awaited_with(session, schema_name="mrf", relation_names=tuple(target for target, _ in stages))
    storage.side_effect = RuntimeError("cms_doctors_immutable_columns_changed")
    with pytest.raises(RuntimeError, match="columns_changed"):
        await prepared_module.assert_prepared_cms_doctors_seal(session, prepared)


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
    relations = list(generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"])
    guard_rows = []
    for name, (kind, function, security, body) in contracts.items():
        if name == "cms_doctors_prepared_write_seal" and not seal:
            continue
        if name == "cms_native_input_mutation" and not native:
            continue
        for relation in relations[:1] if name == "cms_native_input_mutation" else relations:
            guard_rows.append(
                dict(
                    relation=relation,
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
    assert immutable._validate_legacy_hooks(guard_rows, relations, contracts) == (relations[:1] if native else ())
    damaged_rows = [dict(guard_row) for guard_row in guard_rows]
    damaged_rows[0]["body"] += " SELECT 1;"
    with pytest.raises(RuntimeError, match="legacy_hooks_changed"):
        immutable._validate_legacy_hooks(damaged_rows, relations, contracts)
    damaged_rows = [dict(guard_row) for guard_row in guard_rows]
    damaged_rows[0]["enabled"] = "O"
    with pytest.raises(RuntimeError, match="legacy_hooks_changed"):
        immutable._validate_legacy_hooks(damaged_rows, relations, contracts)


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
        '"mrf"."doctor_clinician_address"': {"present": True},
        '"mrf"."address_alias_v1"': {"present": True},
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
    register.assert_awaited_once_with(session, schema, schema, "address_alias_v1")


@pytest.mark.asyncio
async def test_cold_native_hooks_do_not_invent_or_require_historical_tracking(monkeypatch):
    contracts = immutable._legacy_contracts("mrf")
    kind, function, security, body = contracts[revisions.REVISION_TRIGGER]
    relations = list(generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"])
    guards = [
        dict(
            relation=relation,
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
        for relation in relations
    ]
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: guards)))
    )
    monkeypatch.setattr(revisions, "_guarded_oids", AsyncMock(return_value=(11, 12, 13)))
    tracking = AsyncMock(side_effect=AssertionError("native custody cannot manufacture historical tracking"))
    monkeypatch.setattr(revisions, "require_reference_revision_tracking", tracking)
    await immutable._legacy_hooks(session, "mrf", relations)
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


def test_named_reference_body_is_exact_actual_migration_output(monkeypatch):
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261114180000_reference_family_named_generation.py"
    spec = importlib.util.spec_from_file_location("doctors_native_named_contract", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    statements = []
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    migration._replace_revision_body("mrf")
    actual = statements[0].split("$function$")[1]
    expected = immutable._legacy_contracts("mrf")[revisions.REVISION_TRIGGER][3]
    assert " ".join(actual.split()) == " ".join(expected.split())


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_schema", [False, True])
async def test_administrative_policy_uses_declared_names_without_database_role_queries(bad_schema):
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    session = SimpleNamespace(scalar=AsyncMock(side_effect=AssertionError("role query")))
    if bad_schema:
        with pytest.raises(RuntimeError, match="native_policy_changed"):
            await immutable._owner(session, "other", native_policy=policy)
    else:
        assert await immutable._owner(session, "source_schema", native_policy=policy) == "source_owner"
        assert await immutable._executable_subjects(session, "source_schema", policy) == (
            "source_owner",
            "source_publisher",
        )
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_scope_does_not_supply_an_ordinary_source_writer(monkeypatch):
    retained = AsyncMock()
    monkeypatch.setattr(archive, "protected_publisher_owner", retained)
    with pytest.raises(ValueError, match="publisher_connection_required"):
        await immutable._writer(SimpleNamespace(in_transaction=lambda: True))
    retained.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_archive_candidate_uses_configured_roles_without_requiring_the_canonical_schema():
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    session = SimpleNamespace(scalar=AsyncMock(side_effect=AssertionError("role query")))
    assert (
        await immutable._storage_owner(
            session, "geo", "reference_family_archive_" + "a" * 32, (("geo_zip_lookup", None),), native_policy=policy
        )
        == "source_owner"
    )
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_read_pointer_binds_names_and_generation_without_comparing_old_object_ids(monkeypatch, changed):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    read = AsyncMock(return_value=_authority())
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", read)
    names = generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
    pairs = tuple((name, 1000 + index) for index, name in enumerate(names))
    if changed:
        pairs = (("other", 1000), *pairs[1:])
        with pytest.raises(RuntimeError, match="read_pointer_changed"):
            await immutable._require_read_pointer(object(), "mrf", pairs)
    else:
        # The serving authority is read from the application schema, so a stage family cannot pass as served.
        with pytest.raises(RuntimeError, match="read_pointer_changed"):
            await immutable._require_read_pointer(object(), "reference_family_archive_" + "a" * 32, pairs)
        assert read.await_args.kwargs["schema_name"] == "mrf"
        await immutable._require_read_pointer(object(), "mrf", pairs)
        session = SimpleNamespace(scalar=AsyncMock(side_effect=AssertionError("physical owner query")))
        assert await immutable._storage_owner(session, "cms-doctors", "mrf", pairs) is None
        session.scalar.assert_not_awaited()


@pytest.fixture
def protected_publication(monkeypatch):
    from process import provider_directory_projection_stage as stage

    events = []

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("release")

    session = SimpleNamespace(
        in_transaction=lambda: True, begin_nested=savepoint, execute=AsyncMock(), scalar=AsyncMock()
    )
    connection, owner = AsyncMock(), AsyncMock()
    monkeypatch.setattr(stage, "_copy_driver", connection)
    monkeypatch.setattr(archive, "protected_publisher_owner", owner)
    roles = SimpleNamespace(owner_role="retained_owner", publisher_role="publisher", runtime_roles=("reader",))
    original = archive.model_family_roles
    monkeypatch.setattr(
        archive, "model_family_roles", lambda config=None: original(roles if config is None else config)
    )
    return SimpleNamespace(session=session, connection=connection, owner=owner, events=events, roles=roles)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["geo", "cms-doctors"])
async def test_protected_publication_uses_named_dcl_and_keeps_source_routing_separate(protected_publication, importer):
    observed = protected_publication
    async with archive.protected_reference_family_publication(
        observed.session, importer_id=importer, schema_name="mrf", roles=observed.roles
    ):
        assert await immutable._archive_writer(observed.session, importer, "mrf") == "retained_owner"
        with pytest.raises(ValueError, match="publisher_connection_required"):
            await immutable._writer(observed.session)
    assert observed.events == ["savepoint", "release"]
    assert immutable._PROTECTED_PUBLICATION.get() is None
    observed.owner.assert_not_awaited()
    observed.connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["importer", "schema", "transaction"])
async def test_protected_publication_rejects_wrong_scope_or_missing_transaction(protected_publication, change):
    observed = protected_publication
    with pytest.raises((RuntimeError, ValueError), match="protected_scope_changed|transaction_required"):
        async with archive.protected_reference_family_publication(
            observed.session, importer_id="geo", schema_name="mrf"
        ):
            if change == "transaction":
                observed.session.in_transaction = lambda: False
            await immutable._archive_writer(
                observed.session,
                "cms-doctors" if change == "importer" else "geo",
                "other" if change == "schema" else "mrf",
            )
    assert observed.events == ["savepoint", "rollback"]
    assert immutable._PROTECTED_PUBLICATION.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("old_owner", [None, True, 0, 2**32, "opaque old edition"])
async def test_old_owner_argument_cannot_become_publication_authority(protected_publication, old_owner):
    observed = protected_publication
    async with archive.protected_reference_family_publication(
        observed.session, importer_id="geo", schema_name="mrf", expected_owner_oid=old_owner
    ):
        assert await immutable._archive_writer(observed.session, "geo", "mrf") == "retained_owner"
    observed.owner.assert_not_awaited()
    observed.connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_protected_publication_rolls_back_and_restores_scope(protected_publication, failure):
    observed = protected_publication
    with pytest.raises(failure):
        async with archive.protected_reference_family_publication(
            observed.session, importer_id="cms-doctors", schema_name="mrf"
        ):
            raise failure("late publication failure")
    assert observed.events == ["savepoint", "rollback"]
    assert immutable._PROTECTED_PUBLICATION.get() is None


@pytest.mark.asyncio
async def test_protected_publication_refuses_nested_and_source_routing(protected_publication):
    from process import provider_directory_generic_source_policy as source

    observed = protected_publication
    async with archive.protected_reference_family_publication(observed.session, importer_id="geo", schema_name="mrf"):
        with pytest.raises(RuntimeError, match="protected_scope_invalid"):
            async with archive.protected_reference_family_publication(
                observed.session, importer_id="geo", schema_name="mrf"
            ):
                pytest.fail("nested scope entered")
    token = source._PUBLISHER.set(object())
    try:
        with pytest.raises(RuntimeError, match="protected_scope_invalid"):
            async with archive.protected_reference_family_publication(
                observed.session, importer_id="geo", schema_name="mrf"
            ):
                pytest.fail("source scope borrowed")
    finally:
        source._PUBLISHER.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "reads", "move", "late"])
async def test_received_doctors_replays_every_read_before_atomic_rotation(protected_publication, monkeypatch, failure):
    observed, events, driver = protected_publication, [], object()
    spec = archive.reference_family_spec("cms-doctors")
    stage = SimpleNamespace(dataset_id=UUID(int=1), schema_name="incoming")
    incumbent = SimpleNamespace(schema_name="mrf", relation_oids=tuple(zip(spec.table_names, (11, 12, 13))))

    async def preserve(actual, **fields):
        assert actual is driver
        name = fields["live_table"]
        assert fields == dict(schema="mrf", live_table=name, stage_schema="incoming", stage_table=name)
        events.append(("reads", name))
        if failure == "reads" and len(events) == 2:
            raise RuntimeError("read grant refused")

    async def execute(statement, *args, **kwargs):
        sql = str(statement)
        if sql.startswith(("CREATE", "ALTER")):
            events.append(("move", sql))
            if failure == "move" and sql.startswith("ALTER"):
                raise RuntimeError("heap move refused")

    observed.session.execute.side_effect = execute
    monkeypatch.setattr(access, "preserve_publication_reads", preserve)
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=driver))

    async def rotate():
        async with archive.protected_reference_family_publication(
            observed.session, importer_id="cms-doctors", schema_name="mrf"
        ):
            await archive._rotate_family_relations(observed.session, spec, stage, incumbent)
            if failure == "late":
                raise RuntimeError("late refusal")

    if failure:
        with pytest.raises(RuntimeError, match="refused|refusal"):
            await rotate()
    else:
        await rotate()
    assert [name for kind, name in events if kind == "reads"] == list(
        spec.table_names[:2] if failure == "reads" else spec.table_names
    )
    if failure == "reads":
        assert not any(kind == "move" for kind, _ in events)
    else:
        assert events[:3] == [("reads", name) for name in spec.table_names]
        assert len(events[3:]) == (2 if failure == "move" else 7)
    assert observed.events == ["savepoint", "rollback" if failure else "release"]


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["geo", "cms-doctors", "label"])
@pytest.mark.parametrize("stage_valid", [False, True])
async def test_validated_cut_scopes_only_a_fully_declared_stage(
    protected_publication, monkeypatch, importer, stage_valid
):
    events = []
    spec = SimpleNamespace(importer_id=importer)
    incumbent = SimpleNamespace(schema_name="mrf")
    cutover = SimpleNamespace(roles=protected_publication.roles, stage_read_only=False)
    ownership = object()
    monkeypatch.setattr(archive, "_validated_activation_inputs", lambda *args: ("manifest", "validation", spec))

    async def lock(*args, **kwargs):
        events.append("locks")
        if not stage_valid:
            raise archive.ReferenceFamilyArchiveError("stage declaration differs")
        return ownership

    async def activate(session, *args):
        publication = immutable._PROTECTED_PUBLICATION.get()
        assert (publication is not None) == (importer != "label")
        if publication:
            assert publication.owner_role == "retained_owner" and publication.schema_name == "mrf"
        events.append("activate")
        return "receipt"

    monkeypatch.setattr(archive, "_lock_and_verify_activation", lock)
    monkeypatch.setattr(archive, "_activate_verified_reference_family", activate)
    fields_by_name = dict(
        ownership=ownership,
        manifest=object(),
        expected_incumbent=incumbent,
        validation_receipt=object(),
        cutover=cutover,
    )
    if stage_valid:
        assert (
            await archive.activate_validated_reference_family_stage(protected_publication.session, **fields_by_name)
            == "receipt"
        )
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="stage declaration differs"):
            await archive.activate_validated_reference_family_stage(protected_publication.session, **fields_by_name)
    assert events == (["locks", "activate"] if stage_valid else ["locks"])
    assert immutable._PROTECTED_PUBLICATION.get() is None


@pytest.mark.asyncio
async def test_family_freeze_uses_only_declared_names_and_existing_config(monkeypatch):
    freeze = AsyncMock()
    monkeypatch.setattr(immutable, "_freeze_named_model_relations", freeze)
    ownership = SimpleNamespace(importer_id="nucc", schema_name="candidate", relation_names=("nucc_taxonomy",))
    session = object()
    await immutable.seal_model_family_relation(session, ownership, "opaque", "opaque")
    freeze.assert_awaited_once_with(session, "candidate", ("nucc_taxonomy",), None)


@pytest.mark.asyncio
async def test_source_stage_freeze_threads_current_role_policy(monkeypatch):
    from process import provider_directory_generic_source_policy as source

    policy = source.GenericSourcePolicy("source_schema", "source_owner", "publisher", ("reader",))
    freeze = AsyncMock()
    monkeypatch.setattr(immutable, "_freeze_named_model_relations", freeze)
    monkeypatch.setattr(source, "bound_publisher", lambda: SimpleNamespace(policy=policy))
    token = source._PUBLISHER.set(object())
    try:
        ownership = SimpleNamespace(
            importer_id="cms-doctors",
            schema_name="candidate",
            relation_names=generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"],
        )
        await immutable.seal_model_family_relation("session", ownership)
    finally:
        source._PUBLISHER.reset(token)
    freeze.assert_awaited_once_with("session", "candidate", ownership.relation_names, policy)


@pytest.mark.asyncio
async def test_geo_predecessor_uses_declared_owner_without_live_role_mapping(protected_publication, monkeypatch):
    from process import geo_source_archive as geo

    writer, publisher = AsyncMock(), Mock()
    monkeypatch.setattr(geo, "_writer", writer)
    monkeypatch.setattr(geo, "source_publisher_body", publisher)
    observed = protected_publication
    schema = archive.reference_family_predecessor_schema(UUID(int=1))
    async with archive.protected_reference_family_publication(observed.session, importer_id="geo", schema_name="mrf"):
        await geo.create_geo_predecessor_schema(observed.session, schema)
    assert [str(call.args[0]) for call in observed.session.execute.await_args_list] == [
        f'CREATE SCHEMA "{schema}" AUTHORIZATION "retained_owner"'
    ]
    observed.session.scalar.assert_not_awaited()
    writer.assert_not_awaited()
    publisher.assert_not_called()
