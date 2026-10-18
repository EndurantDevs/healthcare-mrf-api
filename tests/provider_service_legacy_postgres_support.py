# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed legacy release SQL on a retained catalog and ordinary Reader."""

import importlib.util
import json
from pathlib import Path

import pytest
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters

from api import plan_release_serving as releases
from api import provider_service_legacy_selection as legacy
from api import provider_service_source_policy as policy
from api.plan_release_pricing_projection import plan_release_serving_queries
from api.provider_detail_network_read import managed_provider_network_read_scope
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.test_provider_service_source_policy_postgres import (
    _ALLOWED,
    _EXCLUDED,
    _reader_identity,
    _selection,
)
from tests.test_provider_service_source_policy_postgres import (
    cms_resource_template as cms_resource_template,
)
from tests.test_provider_service_source_policy_postgres import (
    profile_transport as profile_transport,
)
from tests.test_provider_service_source_policy_postgres import (
    published_profile_source as published_profile_source,
)
from tests.test_provider_service_source_policy_postgres import (
    source_admission as source_admission,
)

pytestmark = pytest.mark.asyncio


async def _release_tables(admission, monkeypatch):
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260722160000_plan_release_serving_projection.py"
    spec = importlib.util.spec_from_file_location("legacy_release_fixture_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    statements = []
    with monkeypatch.context() as patch:
        patch.setattr(module, "_schema", lambda: admission.schema)
        patch.setattr(module.op, "execute", statements.append)
        module.upgrade()
    connection = admission.held.fixture.connection
    for statement in statements:
        await connection.execute(statement)
    await connection.execute(
        f'CREATE TABLE "{admission.schema}".plan_release_snapshot_pin '
        "(serving_revision_id text,snapshot_id text,PRIMARY KEY(serving_revision_id,snapshot_id))"
    )
    await connection.execute(
        f'CREATE TABLE "{admission.schema}".ptg2_v3_snapshot_plan_scope '
        "(snapshot_id text,plan_id text,plan_market_type text,PRIMARY KEY(snapshot_id,plan_id,plan_market_type))"
    )
    monkeypatch.setattr(legacy, "PTG2_SCHEMA", admission.schema)
    monkeypatch.setattr(releases, "PTG2_SCHEMA", admission.schema)
    full, metadata = plan_release_serving_queries(admission.schema)
    monkeypatch.setattr(releases, "_PLAN_RELEASE_SERVING_SQL", full)
    monkeypatch.setattr(releases, "_PLAN_RELEASE_SERVING_WITHOUT_PROJECTION_SQL", metadata)


async def _publish_release(admission, selection, *, suffix="0", source_manifest=None):
    connection, schema = admission.held.fixture.connection, '"' + admission.schema + '"'
    release_id, revision_id = "hprelease_" + suffix * 26, "hpserve_" + suffix * 26
    await connection.execute(
        f"INSERT INTO {schema}.plan_release_serving_revision "
        "(serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,"
        "is_current,expected_binding_count,binding_set_digest,source_manifest,published_at) "
        "VALUES($1,$2,$3,$4,'published','published',true,$5,$6,$7::jsonb,now())",
        revision_id,
        release_id,
        "hpplan_" + suffix * 26,
        selection.release_month,
        len(selection.bindings),
        selection.binding_set_digest,
        json.dumps(source_manifest or {}),
    )
    await connection.executemany(
        f"INSERT INTO {schema}.plan_release_snapshot_binding "
        "(serving_revision_id,binding_ordinal,snapshot_id,source_key,plan_id,plan_market_type,role,required) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
        [
            (
                revision_id,
                binding.binding_ordinal,
                binding.snapshot_id,
                binding.source_key,
                binding.plan_id,
                binding.plan_market_type,
                binding.role,
                binding.required,
            )
            for binding in selection.bindings
        ],
    )
    await connection.executemany(
        f"INSERT INTO {schema}.ptg2_snapshot_pin(owner_type,owner_id,snapshot_id) VALUES($1,$2,$3)",
        [
            ("plan_release_serving_revision", revision_id, snapshot)
            for snapshot in {binding.snapshot_id for binding in selection.bindings}
        ],
    )
    await connection.executemany(
        f"INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
        [(binding.snapshot_id, binding.plan_id, binding.plan_market_type) for binding in selection.bindings],
    )
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{admission.held.role}"')
    return release_id


def _args(**selectors):
    return RequestParameters({name: [value] for name, value in selectors.items()})


@pytest.mark.parametrize("selected", [False, True])
async def test_native_legacy_release_preserves_originals_and_admits_before_readiness(
    source_admission, monkeypatch, selected
):
    admission = source_admission
    await _release_tables(admission, monkeypatch)
    recipe = _selection([admission.reviews_by_name[name] for name in ("allowed", "excluded", "public")])
    release_id = await _publish_release(admission, recipe)
    held = admission.held
    async with held.database.reader_session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        pid = await _reader_identity(driver, held)
        async with managed_provider_network_read_scope(
            session,
            selector=(_ALLOWED,) if selected else None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(_EXCLUDED,),
            access_scope_sha256="a" * 64,
            organization_policy=RegistryOrganizationVisibility((), ()),
        ):
            async with legacy.legacy_service_selection(session, _args(source_key="source_allowed")) as args:
                assert args["plan_release_id"] == [release_id]
                selection = legacy._RESTRICTION.get().selection
                assert len(selection.bindings) == 3 and selection._validated_serving_tables == ()
                admitted = await policy.admitted_plan_release_bindings(session, selection)
                assert admitted == (selection.bindings[0],) and admitted[0] is selection.bindings[0]
            assert legacy.service_legacy_selection_sha256() is None
            assert await _reader_identity(driver, held) == pid


async def test_native_legacy_ambiguous_releases_refuse_before_source_probe(source_admission, monkeypatch):
    admission = source_admission
    await _release_tables(admission, monkeypatch)
    recipe = _selection([admission.reviews_by_name["allowed"]])
    await _publish_release(admission, recipe)
    await _publish_release(admission, recipe, suffix="1")
    held = admission.held
    async with held.database.reader_session() as session:
        async with managed_provider_network_read_scope(
            session,
            selector=None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(),
            access_scope_sha256="a" * 64,
            organization_policy=RegistryOrganizationVisibility((), ()),
        ):
            with pytest.raises(InvalidUsage, match="one canonical release"):
                async with legacy.legacy_service_selection(session, _args(source_key="source_allowed")):
                    pytest.fail("ambiguous release entered native processing")
    assert legacy.service_legacy_selection_sha256() is None


@pytest.fixture
def typed_plan_inputs(monkeypatch):
    """Use explicit synthetic plan IDs before the reviewed-source fixture publishes."""
    from process.ptg_parts.result_archive_published_authority import PtgPublishedResultSourceAuthority
    from process.registry_ptg_published_plan_scope import _digest, _document
    from tests import test_provider_service_source_policy_postgres as sources

    original = sources._review

    def reviewed(*args, **options):
        entry = original(*args, **options)
        command = entry.document["command"]
        command["plan_id"] = "99-0000001" if entry.name == "allowed" else "990000001"
        command["published_identity"]["plan_id"] = command["plan_id"]
        entry.mapping["source_scope_json"]["plan_id"] = command["plan_id"]
        evidence = entry.document["evidence"]
        evidence["source_authority"] = PtgPublishedResultSourceAuthority(
            "typed_fixture", command["published_identity"]
        ).as_dict()
        entry.document = _document(command, entry.document["actor"], evidence)
        digest = _digest(entry.document)
        entry.mapping["source_scope_json"]["approval_sha256"] = digest
        entry.mapping["evidence_sha256"] = digest
        entry.mapping["binding_key"] = _digest(entry.mapping)
        return entry

    monkeypatch.setattr(sources, "_review", reviewed)


async def _insert_allowed_type(connection, schema, binding, kind):
    from process.ptg_parts.allowed_amounts import _allowed_amount_plan_rows

    allowed_plans = _allowed_amount_plan_rows(
        snapshot_id=binding.snapshot_id,
        source_file_version_id=None,
        file_id=71,
        meta={"plan_id": binding.plan_id, "plan_id_type": kind, "plan_market_type": binding.plan_market_type},
        plan_info=None,
    )
    allowed_plan = allowed_plans[0]
    await connection.execute(
        f"INSERT INTO {schema}.ptg2_allowed_amount_plan VALUES($1,$2,$3,$4,$5)",
        allowed_plan["snapshot_id"],
        allowed_plan["plan_hash"],
        allowed_plan["plan_id"],
        allowed_plan["plan_id_type"],
        allowed_plan["plan_market_type"],
    )
    await connection.execute(
        f"UPDATE {schema}.ptg2_snapshot SET manifest=manifest||jsonb_build_object('allowed_amount_index',jsonb_build_object('source_key',$1::text)) WHERE snapshot_id=$2",
        binding.source_key,
        binding.snapshot_id,
    )


async def _typed_rows(admission, recipe, kinds):
    import datetime

    from process.ptg_parts.plan_rows import _ptg2_plan_rows

    connection, schema = admission.held.fixture.connection, '"' + admission.schema + '"'
    await connection.execute(
        f"CREATE TABLE {schema}.ptg2_plan (plan_hash text PRIMARY KEY,plan_id text,plan_id_type text,plan_market_type text,canonical_payload json)"
    )
    await connection.execute(
        f"CREATE TABLE {schema}.ptg2_plan_month (snapshot_id text,plan_hash text,import_month date)"
    )
    await connection.execute(f"CREATE INDEX ON {schema}.ptg2_plan_month(snapshot_id)")
    await connection.execute(
        f"CREATE TABLE {schema}.ptg2_allowed_amount_plan (snapshot_id text,plan_hash bigint,plan_id text,plan_id_type text,plan_market_type text)"
    )
    await connection.execute(f"CREATE INDEX ON {schema}.ptg2_allowed_amount_plan(snapshot_id,plan_id,plan_market_type)")
    for binding, kind in zip(recipe.bindings, kinds, strict=True):
        if binding.role == "allowed_amounts":
            await _insert_allowed_type(connection, schema, binding, kind)
            continue
        plan, _, month = _ptg2_plan_rows(
            {"plan_id": binding.plan_id, "plan_id_type": kind, "plan_market_type": binding.plan_market_type},
            binding.snapshot_id,
            datetime.date(2026, 1, 1),
        )
        await connection.execute(
            f"INSERT INTO {schema}.ptg2_plan VALUES($1,$2,$3,$4,$5::json)",
            plan["plan_hash"],
            plan["plan_id"],
            plan["plan_id_type"],
            plan["plan_market_type"],
            json.dumps(plan["canonical_payload"]),
        )
        await connection.execute(
            f"INSERT INTO {schema}.ptg2_plan_month VALUES($1,$2,$3)",
            binding.snapshot_id,
            plan["plan_hash"],
            month["import_month"],
        )
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{admission.held.role}"')


async def _typed_release(admission, monkeypatch, kinds=("ein", "hios", None)):
    from dataclasses import replace

    from tests.test_provider_service_legacy_selection import _type_document

    await _release_tables(admission, monkeypatch)
    recipe = _selection([admission.reviews_by_name[name] for name in ("allowed", "excluded", "public")])
    recipe = replace(
        recipe,
        serving_revision_id="hpserve_" + "0" * 26,
        plan_release_id="hprelease_" + "0" * 26,
        healthporta_plan_id="hpplan_" + "0" * 26,
    )
    recipe = replace(
        recipe,
        bindings=recipe.bindings[:2]
        + (replace(recipe.bindings[2], role="allowed_amounts", binding_ordinal=3, plan_market_type=""),),
    )
    document = _type_document(recipe, kinds)
    await _publish_release(admission, recipe, source_manifest={legacy._TYPE_MANIFEST_FIELD: document})
    await _typed_rows(admission, recipe, kinds)
    return recipe


@pytest.mark.usefixtures("typed_plan_inputs")
@pytest.mark.parametrize("kind,ordinal", [("ein", 3), ("hios", 10)])
@pytest.mark.parametrize("selected", [False, True])
async def test_native_typed_legacy_uses_commitment_then_exact_retained_rows(
    source_admission, monkeypatch, kind, ordinal, selected
):
    admission = source_admission
    recipe = await _typed_release(admission, monkeypatch)
    held = admission.held
    async with held.database.reader_session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        pid = await _reader_identity(driver, held)
        async with managed_provider_network_read_scope(
            session,
            selector=(_ALLOWED,) if selected else None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(_EXCLUDED,),
            access_scope_sha256="a" * 64,
            organization_policy=RegistryOrganizationVisibility((), ()),
        ):
            async with legacy.legacy_service_selection(session, _args(plan_id="990000001", plan_id_type=kind)):
                selection = legacy._RESTRICTION.get().selection
                restricted = legacy.restrict_service_bindings(selection, selection.bindings)
                assert tuple(binding.binding_ordinal for binding in restricted) == (ordinal,)
                admitted = await policy.admitted_plan_release_bindings(session, selection)
                assert admitted == ((selection.bindings[0],) if kind == "ein" else ())
                assert len(selection.bindings) == len(recipe.bindings) == 3
                assert selection._validated_serving_tables == ()
            assert await _reader_identity(driver, held) == pid
    assert legacy.service_legacy_selection_sha256() is None


async def _damage_type_evidence(admission, recipe, damage):
    connection, schema = admission.held.fixture.connection, '"' + admission.schema + '"'
    if damage == "absent":
        await connection.execute(
            f"DELETE FROM {schema}.ptg2_plan_month WHERE snapshot_id=$1", recipe.bindings[0].snapshot_id
        )
    if damage == "type":
        await connection.execute(
            f"UPDATE {schema}.ptg2_plan SET plan_id_type='hios' WHERE plan_id=$1", recipe.bindings[0].plan_id
        )
    if damage == "payload":
        await connection.execute(
            f"UPDATE {schema}.ptg2_plan SET canonical_payload=json_build_object('plan_id',plan_id) WHERE plan_id=$1",
            recipe.bindings[0].plan_id,
        )
    if damage == "conflict":
        import datetime

        from process.ptg_parts.plan_rows import _ptg2_plan_rows

        binding = recipe.bindings[0]
        plan, _, month = _ptg2_plan_rows(
            {"plan_id": binding.plan_id, "plan_id_type": "hios", "plan_market_type": binding.plan_market_type},
            binding.snapshot_id,
            datetime.date(2026, 1, 1),
        )
        await connection.execute(
            f"INSERT INTO {schema}.ptg2_plan VALUES($1,$2,$3,$4,$5::json)",
            plan["plan_hash"],
            plan["plan_id"],
            plan["plan_id_type"],
            plan["plan_market_type"],
            json.dumps(plan["canonical_payload"]),
        )
        await connection.execute(
            f"INSERT INTO {schema}.ptg2_plan_month VALUES($1,$2,$3)",
            binding.snapshot_id,
            plan["plan_hash"],
            month["import_month"],
        )
    if damage == "source":
        await connection.execute(
            f"UPDATE {schema}.ptg2_snapshot SET manifest=jsonb_set(manifest,'{{serving_index,source_key}}','\"foreign_source\"') WHERE snapshot_id=$1",
            recipe.bindings[0].snapshot_id,
        )
    if damage == "commitment":
        await connection.execute(
            f"UPDATE {schema}.plan_release_serving_revision SET source_manifest=jsonb_set(source_manifest,'{{legacy_plan_identifier_types,sha256}}',to_jsonb(repeat('f',64)))"
        )


@pytest.mark.usefixtures("typed_plan_inputs")
@pytest.mark.parametrize("damage", ["absent", "type", "payload", "source", "commitment", "conflict"])
async def test_native_typed_legacy_rejects_changed_or_missing_evidence(source_admission, monkeypatch, damage):
    from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable

    admission = source_admission
    recipe = await _typed_release(admission, monkeypatch)
    connection, schema = admission.held.fixture.connection, '"' + admission.schema + '"'
    await _damage_type_evidence(admission, recipe, damage)
    held = admission.held
    async with held.database.reader_session() as session:
        async with managed_provider_network_read_scope(
            session,
            selector=None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(),
            access_scope_sha256="a" * 64,
            organization_policy=RegistryOrganizationVisibility((), ()),
        ):
            with pytest.raises(PlanPricingProjectionUnavailable, match="identifier type evidence"):
                async with legacy.legacy_service_selection(
                    session, _args(plan_id_type="ein", source_key="source_allowed")
                ):
                    pytest.fail("damaged typed identity entered native processing")
    assert legacy.service_legacy_selection_sha256() is None


@pytest.mark.usefixtures("typed_plan_inputs")
async def test_native_typed_allowed_original_role_and_blank_market_are_exact(source_admission, monkeypatch):
    admission = source_admission
    await _typed_release(admission, monkeypatch, kinds=(None, None, "hios"))
    held = admission.held
    async with held.database.reader_session() as session:
        async with managed_provider_network_read_scope(
            session,
            selector=None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(),
            access_scope_sha256="a" * 64,
            organization_policy=RegistryOrganizationVisibility((), ()),
        ):
            async with legacy.legacy_service_selection(session, _args(plan_id="990000001", plan_id_type="hios")):
                selection = legacy._RESTRICTION.get().selection
                assert legacy.restrict_service_bindings(selection, selection.in_network_bindings) == ()
                admitted = legacy.restrict_service_bindings(selection, selection.allowed_amount_bindings)
                assert len(admitted) == 1 and admitted[0] is selection.bindings[2]
                assert admitted[0].binding_ordinal == selection.bindings[0].binding_ordinal == 3
                assert admitted[0].plan_market_type == "" and len(selection.bindings) == 3


@pytest.mark.usefixtures("typed_plan_inputs")
async def test_native_typed_legacy_ambiguous_commitments_refuse(source_admission, monkeypatch):
    from dataclasses import replace

    from tests.test_provider_service_legacy_selection import _type_document

    admission = source_admission
    recipe = await _typed_release(admission, monkeypatch)
    second = replace(
        recipe,
        serving_revision_id="hpserve_" + "1" * 26,
        plan_release_id="hprelease_" + "1" * 26,
        healthporta_plan_id="hpplan_" + "1" * 26,
    )
    await _publish_release(
        admission,
        second,
        suffix="1",
        source_manifest={legacy._TYPE_MANIFEST_FIELD: _type_document(second, ("ein", "hios", None))},
    )
    async with admission.held.database.reader_session() as session:
        async with managed_provider_network_read_scope(
            session,
            selector=None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(),
            access_scope_sha256="a" * 64,
            organization_policy=RegistryOrganizationVisibility((), ()),
        ):
            with pytest.raises(InvalidUsage, match="one canonical release"):
                async with legacy.legacy_service_selection(
                    session, _args(source_key="source_allowed", plan_id_type="ein")
                ):
                    pytest.fail("ambiguous typed releases were admitted")


@pytest.mark.usefixtures("typed_plan_inputs")
async def test_native_explicit_type_disambiguates_distinct_published_release_namespaces(source_admission, monkeypatch):
    from dataclasses import replace

    from tests.test_provider_service_legacy_selection import _type_document

    admission = source_admission
    recipe = await _typed_release(admission, monkeypatch)
    await admission.held.fixture.connection.execute(
        f'UPDATE "{admission.schema}".plan_release_serving_revision SET is_current=false'
    )
    release_by_kind = {}
    for index, kind in enumerate(("ein", "hios")):
        suffix = str(index + 1)
        selected = replace(
            recipe,
            bindings=(recipe.bindings[index],),
            serving_revision_id="hpserve_" + suffix * 26,
            plan_release_id="hprelease_" + suffix * 26,
            healthporta_plan_id="hpplan_" + suffix * 26,
        )
        release_by_kind[kind] = await _publish_release(
            admission,
            selected,
            suffix=suffix,
            source_manifest={legacy._TYPE_MANIFEST_FIELD: _type_document(selected, (kind,))},
        )
    async with admission.held.database.reader_session() as session:
        async with managed_provider_network_read_scope(
            session,
            selector=None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(),
            access_scope_sha256="a" * 64,
            organization_policy=RegistryOrganizationVisibility((), ()),
        ):
            for kind in ("ein", "hios"):
                async with legacy.legacy_service_selection(
                    session, _args(plan_id="990000001", plan_id_type=kind)
                ) as normalized:
                    assert normalized.get("plan_release_id") == release_by_kind[kind]
                    assert len(legacy._RESTRICTION.get().selection.bindings) == 1
