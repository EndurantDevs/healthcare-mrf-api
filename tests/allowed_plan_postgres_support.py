# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof of allowed review DDL, protected writer and listings.

Source tables are synthetic SQL-contract fixtures; registry storage uses actual
migrations. Upstream authority is explicitly synthetic.
"""

import json
import os
from copy import deepcopy
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import registry_ptg_source_choices as choices
from process import registry_ptg_source_reviews as reviews
from process.registry_ptg_published_plan_scope import read_registry_ptg_published_plan_scope
from tests.test_registry_ptg_scope_engine_postgres import (
    _app_authority,
    _approve_published_association,
    _migration,
    _native_scope,
    _published_lock_provision,
    _published_review_setup,
)


def _allowed_source_documents():
    version_by_field = {
        "source_file_version_id": "allowed-version",
        "source_identity_sha256": "f" * 16,
        "raw_sha256": "b" * 64,
    }
    manifest_by_field = {
        "snapshot_id": "allowed-published",
        "source_key": "allowed_source",
        "import_month": "2026-01-01",
        "activation_status": "not_applicable",
        "data_domains": ["allowed_amounts"],
        "allowed_amount_index": {
            "contract": "ptg2_allowed_amounts_v1",
            "arch_version": "postgres_binary_v3",
            "storage": "postgresql",
            "data_domain": "allowed_amounts",
            "source_key": "allowed_source",
            "snapshot_scoped": True,
            "allowed_amount_evidence": True,
            "allowed_amount_payments": 2,
            "allowed_amount_provider_payments": 3,
        },
        "source_file_versions": [
            {
                "engine_source_file_version_id": version_by_field["source_file_version_id"],
                "engine_source_identity_hash": version_by_field["source_identity_sha256"],
                "raw_sha256": version_by_field["raw_sha256"],
            }
        ],
    }
    return version_by_field, manifest_by_field


async def _seed_allowed(fixture, plan_market_type):
    schema = '"' + fixture.service.schema_name + '"'
    version_by_field, manifest_by_field = _allowed_source_documents()
    async with fixture.engine.begin() as connection:
        await connection.execute(
            text(f"INSERT INTO {schema}.ptg2_import_run VALUES('allowed-run','2026-01-01',CAST(:options AS jsonb))"),
            {"options": json.dumps({"source_key": "allowed_source"})},
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_snapshot(snapshot_id,import_run_id,import_month,status,manifest) VALUES('allowed-published','allowed-run','2026-01-01','published',CAST(:manifest AS jsonb))"
            ),
            {"manifest": json.dumps(manifest_by_field)},
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_source_identity VALUES(:hash,'allowed_amounts','https://source.example/allowed')"
            ),
            {"hash": version_by_field["source_identity_sha256"]},
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_source_file_version(source_file_version_id,source_identity_hash,raw_sha256,logical_sha256,content_length,etag,last_modified,verification_mode,payload) VALUES(:version,:identity,:raw,NULL,1,NULL,NULL,'downloaded','{{}}'::jsonb)"
            ),
            {
                "version": version_by_field["source_file_version_id"],
                "identity": version_by_field["source_identity_sha256"],
                "raw": version_by_field["raw_sha256"],
            },
        )
        for name in ("plan", "item"):
            await connection.exec_driver_sql(
                f"CREATE TABLE {schema}.ptg2_allowed_amount_{name} (snapshot_id text,plan_id text,plan_market_type text,file_id bigint,source_file_version_id text)"
            )
            await connection.execute(
                text(
                    f"INSERT INTO {schema}.ptg2_allowed_amount_{name} VALUES('allowed-published','PLAN_ALLOWED',:market,17,'allowed-version')"
                ),
                {"market": plan_market_type or None},
            )
            await connection.exec_driver_sql(
                f'GRANT SELECT ON {schema}.ptg2_allowed_amount_{name} TO "{fixture.roles[1]}","{fixture.roles[2]}"'
            )
    ownership_by_field = fixture.document["ownership"] | {
        "snapshot_id": "allowed-published",
        "engine_run_id": "allowed-run",
        "source_key": "allowed_source",
        "import_month": "2026-01-01",
        "engine_source_file_version_id": version_by_field["source_file_version_id"],
        "engine_source_identity_hash": version_by_field["source_identity_sha256"],
    }
    return ownership_by_field, version_by_field


async def _run_migration(connection, control, direction):
    migration = _migration("20261016070000_registry_allowed_plan_scope.py")

    def migrate(sync_connection):
        previous = os.environ.get("HLTHPRT_NETWORK_REGISTRY_SCHEMA")
        os.environ["HLTHPRT_NETWORK_REGISTRY_SCHEMA"] = control
        try:
            with Operations.context(MigrationContext.configure(sync_connection)):
                getattr(migration, direction)()
        finally:
            if previous is None:
                os.environ.pop("HLTHPRT_NETWORK_REGISTRY_SCHEMA", None)
            else:
                os.environ["HLTHPRT_NETWORK_REGISTRY_SCHEMA"] = previous

    await connection.run_sync(migrate)


async def _malformed_receipts(fixture, table, original):
    for field in ("review_type", "selection_mode"):
        mutated = deepcopy(original)
        mutated["scope_id"] = str(uuid4())
        mutated["command"]["scope_id"] = mutated["scope_id"]
        mutated["idempotency_key"] = mutated["command"]["idempotency_key"] = uuid4().hex
        mutated["command"][field] = (
            "published_complete_snapshot_plan" if field == "review_type" else "complete_snapshot_source_set"
        )
        async with fixture.engine.connect() as connection:
            with pytest.raises(DBAPIError):
                await connection.execute(
                    text(
                        f"INSERT INTO {table}(scope_id,scope_key,actor_key,idempotency_key,approval_sha256,approval_json) VALUES(CAST(:scope AS uuid),:key,:key,:idem,:key,CAST(:document AS jsonb))"
                    ),
                    {
                        "scope": mutated["scope_id"],
                        "key": uuid4().hex * 2,
                        "idem": mutated["idempotency_key"],
                        "document": json.dumps(mutated),
                    },
                )
            await connection.rollback()


def _allowed_binding_mapping(command, receipt):
    """Build one reviewed allowed-plan binding without changing its protected scope."""
    scope_by_field = {
        name: command[name] for name in ("review_type", "scope_id", "plan_id", "plan_market_type", "selection_mode")
    }
    scope_by_field.update(snapshot_id=command["source"]["snapshot_id"], approval_sha256=receipt["approval_sha256"])
    return {
        **command["coordinates"],
        "binding_id": str(uuid4()),
        "network_id": command["network_id"],
        "source_key": command["source"]["binding_source_key"],
        "source_scope_json": scope_by_field,
        "evidence_id": "published-plan-review:" + command["scope_id"],
        "evidence_sha256": receipt["approval_sha256"],
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
    }


async def _probe_allowed_binding_batch(session, fixture, command, receipt):
    """Persist the reviewed binding through real native encoding, COPY and replay."""
    from process.network_source_binding_store import (
        NetworkSourceBindingBatchCommand,
        apply_network_source_binding_batch,
        list_network_source_bindings,
    )
    from process.registry_record_store import RegistryActor

    mapping_by_field = _allowed_binding_mapping(command, receipt)
    driver = (await (await session.connection()).get_raw_connection()).driver_connection
    actor = RegistryActor("platform_admin", UUID(fixture.document["actor"]["user_id"]), "system")
    batch = NetworkSourceBindingBatchCommand(
        json.dumps([mapping_by_field]).encode(), "Reviewed synthetic allowed plan", uuid4().hex
    )
    before = await driver.fetchrow(
        f'SELECT draft_revision,approved_revision FROM "{fixture.control}".registry_revision_control'
    )
    accepted = await apply_network_source_binding_batch(
        driver, batch, actor, control_schema=fixture.control, published_plan_store=fixture.service.store
    )
    assert accepted == {
        "custom_revision": before["draft_revision"] + 1,
        "records": [
            {
                "record_kind": "network_binding",
                "record_id": mapping_by_field["binding_id"],
                "revision": 1,
                "network_id": command["network_id"],
                "archived": False,
            }
        ],
    }
    assert (
        await apply_network_source_binding_batch(
            driver, batch, actor, control_schema=fixture.control, published_plan_store=fixture.service.store
        )
        == accepted
    )
    heads = await list_network_source_bindings(driver, command["network_id"], control_schema=fixture.control)
    assert len(heads) == 1 and heads[0]["source_scope_json"] == mapping_by_field["source_scope_json"]
    assert tuple(
        await driver.fetchrow(
            f'SELECT draft_revision,approved_revision FROM "{fixture.control}".registry_revision_control'
        )
    ) == (before["draft_revision"] + 1, before["approved_revision"])


async def _probe_allowed_listing(fixture, request, preview, receipt):
    """Use real protected reader, source choices and fenced batch listing service."""
    ownership_by_field = request["ownership"]
    selection_by_field = {
        "client_id": ownership_by_field["client_id"],
        "source_file_import_id": ownership_by_field["source_file_import_id"],
        "after": None,
        "limit": 1,
    }
    async with fixture.reader() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        retained = await read_registry_ptg_published_plan_scope(
            session,
            scope_id=receipt["scope_id"],
            client_id=ownership_by_field["client_id"],
            approval_sha256=receipt["approval_sha256"],
            store=fixture.service.store,
        )
        assert retained["command"] == preview["command"]
        await _probe_allowed_binding_batch(session, fixture, preview["command"], receipt)
        page = await choices.read_registry_ptg_source_choices(
            session, fixture.service.schema_name, ownership_by_field, selection_by_field, fixture.service.store
        )
        assert page["published_plan"]["review_type"] == "published_allowed_amount_plan"

    async def batch_authority(envelope, *, deadline):
        from process.registry_ptg_producer_scope import _digest

        return {
            "authorized": True,
            "actor": envelope["actor"],
            "request_sha256": _digest(envelope),
            "policy_revision": 1,
        }

    service = replace(fixture.service, source_reviews_authority=batch_authority)
    envelope = {name: request[name] for name in ("actor", "session_token_sha256")} | {
        "ownership": ownership_by_field,
        "selection": selection_by_field,
    }
    listed = await service.source_reviews(envelope)
    assert listed["items"][0]["command"] == preview["command"] and listed["next_cursor"] is None
    empty = await service.source_reviews(envelope | {"selection": selection_by_field | {"after": receipt["scope_id"]}})
    assert empty["items"] == []
    return retained


async def _probe_allowed_lineage_denials(fixture, request):
    """Real readers refuse missing lineage and changed selected ownership."""
    schema = '"' + fixture.service.schema_name + '"'
    async with fixture.engine.begin() as connection:
        await connection.exec_driver_sql(
            f"UPDATE {schema}.ptg2_allowed_amount_plan SET source_file_version_id=NULL WHERE snapshot_id='allowed-published'"
        )
    try:
        with pytest.raises(ValueError):
            await fixture.service.preview(request)
    finally:
        async with fixture.engine.begin() as connection:
            await connection.exec_driver_sql(
                f"UPDATE {schema}.ptg2_allowed_amount_plan SET source_file_version_id='allowed-version' WHERE snapshot_id='allowed-published'"
            )
    for field in ("client_id", "source_key", "engine_source_file_version_id"):
        changed = deepcopy(request)
        changed["ownership"][field] = "other"
        with pytest.raises(ValueError):
            await fixture.service.preview(changed)


async def _upgrade_review_store(fixture, table):
    from tests.test_registry_published_plan_binding_refs import reviewed_binding

    _, prior_review = reviewed_binding()
    async with fixture.engine.begin() as connection:
        await connection.execute(
            text(
                f"INSERT INTO {table}(scope_id,scope_key,actor_key,idempotency_key,approval_sha256,approval_json) VALUES(CAST(:scope AS uuid),:key,:key,:idem,:digest,CAST(:document AS jsonb))"
            ),
            {
                "scope": prior_review["scope_id"],
                "key": "d" * 64,
                "idem": prior_review["document"]["idempotency_key"],
                "digest": prior_review["approval_sha256"],
                "document": json.dumps(prior_review["document"]),
            },
        )
        await _run_migration(connection, fixture.control, "upgrade")
        for statement in _migration("20261017030000_registry_ptg_office_recipe_selection.py")._ddl(fixture.control):
            await connection.exec_driver_sql(statement)
        await connection.exec_driver_sql(
            f'GRANT SELECT ON "{fixture.control}".registry_approved_record,"{fixture.control}".registry_revision_control TO "{fixture.roles[2]}"'
        )
        await connection.exec_driver_sql(
            f'GRANT SELECT ON "{fixture.control}".network_registry_identity,"{fixture.control}".network_registry_record TO "{fixture.roles[2]}"'
        )
        await connection.exec_driver_sql(
            f'GRANT SELECT,INSERT,UPDATE ON "{fixture.control}".registry_network_binding TO "{fixture.roles[2]}"'
        )
        await connection.exec_driver_sql(
            f'GRANT SELECT,INSERT ON "{fixture.control}".registry_record_history,"{fixture.control}".registry_network_binding_batch TO "{fixture.roles[2]}"'
        )
        await connection.exec_driver_sql(
            f'GRANT UPDATE ON "{fixture.control}".registry_revision_control TO "{fixture.roles[2]}"'
        )


async def _source_json_storage(fixture, manifest_storage):
    """Match the real JSON headers and also qualify legacy JSONB fixtures."""
    assert manifest_storage in {"json", "jsonb"}
    schema = '"' + fixture.service.schema_name + '"'
    async with fixture.engine.begin() as connection:
        await connection.exec_driver_sql(
            f"ALTER TABLE {schema}.ptg2_snapshot ALTER COLUMN manifest TYPE {manifest_storage} USING manifest::{manifest_storage}"
        )
        await connection.exec_driver_sql(
            f"ALTER TABLE {schema}.ptg2_import_run ALTER COLUMN options TYPE json USING options::json"
        )
        actual_type = await connection.scalar(
            text(
                "SELECT format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid=CAST(:relation AS regclass) AND attname='manifest'"
            ),
            {"relation": schema + ".ptg2_snapshot"},
        )
        assert actual_type == manifest_storage


@pytest.mark.parametrize("manifest_storage", ["json", "jsonb"])
@pytest.mark.parametrize("plan_market_type", ["group", ""])
async def test_allowed_review_native_migration_writer_and_listing(
    tmp_path, monkeypatch, manifest_storage, plan_market_type
):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        await _source_json_storage(fixture, manifest_storage)
        ownership_by_field, version_by_field = await _seed_allowed(fixture, plan_market_type)
        network_id, revision = await _approve_published_association(fixture)
        table, _, _, request = await _published_review_setup(
            fixture, tmp_path, ownership_by_field, network_id, revision
        )
        request["command"].update(
            review_type="published_allowed_amount_plan",
            selection_mode="complete_allowed_snapshot_source_set",
            plan_id="PLAN_ALLOWED",
            plan_market_type=plan_market_type,
            file_versions=[version_by_field],
        )
        await _upgrade_review_store(fixture, table)
        async with _published_lock_provision(fixture, tmp_path):
            preview = await fixture.service.preview(request)
            envelope = {name: request[name] for name in ("actor", "session_token_sha256")} | preview
            receipt = await fixture.service.approve(envelope)
            assert await fixture.service.approve(envelope) == receipt and len(app.envelopes) == 2
            retained = await _probe_allowed_listing(fixture, request, preview, receipt)
            await _malformed_receipts(fixture, table, retained)
            await _probe_allowed_lineage_denials(fixture, request)
            async with fixture.engine.connect() as connection:
                before = await connection.scalar(text(f"SELECT count(*) FROM {table}"))
                with pytest.raises(RuntimeError, match="Retained allowed"):
                    await _run_migration(connection, fixture.control, "downgrade")
                await connection.rollback()
                assert await connection.scalar(text(f"SELECT count(*) FROM {table}")) == before == 2
            async with fixture.reader() as session, session.begin():
                with pytest.raises(DBAPIError):
                    await session.execute(text(f"DELETE FROM {table}"))
