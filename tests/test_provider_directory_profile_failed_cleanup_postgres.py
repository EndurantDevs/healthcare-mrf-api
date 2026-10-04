# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migrated seeded failed stages exercise disposal, never production capacity admission."""

import os
import re
import shlex
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_failed_cleanup as cleanup
from tests.cms_npd_admission_postgres_support import admission_database, fhir
from tests.cms_npd_admission_postgres_support import cms_admission_template as cms_admission_template
from tests.provider_directory_profile_delta_scenario import _delta_lineage, _insert_delta_serving_generation
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_failed_cleanup import authorization_fixture, sign
from tests.test_provider_directory_profile_failed_cleanup import cleanup_history_directory as cleanup_history_directory

BUILD = "pdpb_" + "5" * 32
OWNER = "run_" + "a" * 32


async def _seed(database):
    """Seed terminal failure and incumbent serving solely as explicit native guard fixtures."""
    schema = "mrf"
    names = (
        profile.profile_evidence_stage_table_name(BUILD),
        profile.profile_stage_table_name(BUILD),
        fhir._bounded_identifier("provider_directory_profile_affected_" + BUILD),
    )
    for statement in (
        profile.profile_evidence_table_sql(schema, profile.PROFILE_EVIDENCE_TABLE, logged=True),
        profile.profile_table_sql(schema, profile.PROFILE_TABLE, logged=True),
        profile.profile_evidence_table_sql(schema, names[0], logged=True),
        profile.profile_table_sql(schema, names[1], logged=True),
        f'CREATE TABLE "mrf"."{names[2]}" (npi bigint PRIMARY KEY)',
    ):
        await database.status(statement)
    for name, evidence in (
        (profile.PROFILE_EVIDENCE_TABLE, True),
        (profile.PROFILE_TABLE, False),
        (names[0], True),
        (names[1], False),
    ):
        for statement in profile.profile_index_statements(schema, name, evidence=evidence):
            await database.status(statement)
    oids_by_field = {
        role: await database.scalar("SELECT to_regclass(:name)::oid::bigint", name="mrf." + name)
        for role, name in zip(
            ("evidence_stage", "profile_stage", "affected_npi_stage", "evidence_target", "profile_target"),
            (*names, profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE),
            strict=True,
        )
    }
    lineage = _delta_lineage()
    scenario = SimpleNamespace(
        schema=schema,
        serving_ref=fhir._provider_directory_profile_serving_generation_ref(schema),
        old_generation="pdprofile_" + "6" * 32,
    )
    await _insert_delta_serving_generation(database, scenario, lineage, oids_by_field)
    # Explicit unadmitted fixture geometry: only its runtime and writable cleanup
    # checkpoint layout are observed here; this does not exercise Profile admission.
    geometry, fingerprints = await _seed_geometry_and_layouts(schema, lineage, oids_by_field, names)
    await _seed_failed_checkpoint(database, lineage, names, oids_by_field, geometry, fingerprints)
    return names


async def _fresh_fixture_authorization(database, *, published_run_id=None):
    """Fixture authority signs real layouts/DB identity/free space, without Linux runtime admission."""
    from uuid import uuid4

    report = await cleanup.inspect_failed_profile_cleanup(
        fhir, build_id=BUILD, owner_run_id=OWNER, published_run_id=published_run_id
    )
    now = datetime.now(timezone.utc)
    envelope, trust, key = authorization_fixture(now)
    body = envelope["authorization"]
    body["checkpoint"], body["stages"], body["limits"] = report["checkpoint"], report["stages"], report["limits"]
    if report.get("variant") == "initial_full_swap":
        body.update(
            contract_id=cleanup.INITIAL_CONTRACT,
            **{field: report[field] for field in ("variant", "physical", "initial")},
        )
    elif "variant" in report:
        body.update(
            contract_id=cleanup.LEGACY_CONTRACT,
            **{field: report[field] for field in ("variant", "physical", "publication")},
        )
    for field in ("authorization_id", "operation_id", "reservation_id", "nonce"):
        body[field] = uuid4().hex
    volume_digest, free = await _server_volume_observation(database)
    for volume in body["volumes"]:
        volume.update(
            volume_digest=volume_digest,
            reserved_bytes=body["limits"][volume["volume_class"] + "_bytes"],
            available_bytes=free,
            available_after_all_reservations_bytes=free
            - sum(body["limits"][field] for field in ("data_bytes", "temp_bytes", "wal_bytes")),
        )
    tablespace = await database.first(
        "SELECT oid::bigint,spcname FROM pg_tablespace WHERE oid=(SELECT dattablespace FROM pg_database WHERE datname=current_database())"
    )
    for entry in body["database"]["tablespaces"]:
        entry.update(tablespace_oid=tablespace[0], tablespace_name=tablespace[1], volume_digest=volume_digest)
    body["database"].update(report["database"])
    for field in ("database_system_identifier", "database_oid", "database_name"):
        setattr(trust, field, body["database"][field])
    trust.tablespaces = tuple(body["database"]["tablespaces"])
    trust.volumes = tuple(
        {field: entry[field] for field in ("volume_class", "volume_digest")} for entry in body["volumes"]
    )
    prior = await database.all(
        "SELECT reservation_id FROM mrf.provider_directory_profile_failed_cleanup_claim WHERE expires_at>clock_timestamp()"
    )
    body["observations"]["accounted_reservation_ids"] = sorted(
        [database_record[0] for database_record in prior] + [body["reservation_id"]]
    )
    return sign(body, key), trust


async def _prepare_disposal_case(database, monkeypatch, case, names, envelope):
    if case == "owner":
        await database.status("UPDATE mrf.import_run SET finished_at=NULL WHERE run_id=:owner", owner=OWNER)
    if case == "oid":
        await database.status(f'DROP TABLE "mrf"."{names[0]}"')
        await database.status(profile.profile_evidence_table_sql("mrf", names[0], logged=True))
    if case == "preimage":
        await database.status("UPDATE mrf.provider_directory_profile_build_checkpoint SET last_error='changed'")
    if case == "serving":
        await database.status("UPDATE mrf.provider_directory_profile_serving_generation SET control_generation=7")
    if case == "budget":
        # Valid signature with a deliberately insufficient separate ceiling.
        envelope["authorization"]["limits"]["wal_bytes"] = 1
        envelope["authorization"]["volumes"][2]["reserved_bytes"] = 1
        _original, _trust, key = authorization_fixture()
        envelope = sign(envelope["authorization"], key)
    if case == "claim_lost_ack":
        from contextlib import asynccontextmanager

        original_session = database.session
        ack_loss_flags = [False]

        @asynccontextmanager
        async def lose_claim_ack():
            is_claim = database._transaction_binding() is not None and not ack_loss_flags[0]
            async with original_session() as session:
                yield session
            if is_claim:
                ack_loss_flags[0] = True
                raise RuntimeError("synthetic_claim_commit_ack_lost")

        monkeypatch.setattr(database, "session", lose_claim_ack)
    if case == "lost_ack":
        original_transaction = cleanup._disposal_transaction

        async def lost_ack(*args):
            await original_transaction(*args)
            raise RuntimeError("synthetic_commit_ack_lost")

        monkeypatch.setattr(cleanup, "_disposal_transaction", lost_ack)
    if case in {"rollback", "cancel"}:
        original = cleanup._dispose_claimed_stages

        async def fail_after_claim(*args):
            if case == "rollback":
                await original(*args)
                raise RuntimeError("synthetic_after_last_drop")
            import asyncio

            raise asyncio.CancelledError()

        monkeypatch.setattr(cleanup, "_dispose_claimed_stages", fail_after_claim)
    return envelope


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "success",
        "rollback",
        "cancel",
        "owner",
        "oid",
        "budget",
        "preimage",
        "serving",
        "lost_ack",
        "claim_lost_ack",
        "expired_replay",
    ],
)
async def test_failed_profile_cleanup_one_use(monkeypatch, case):
    """Exercise one-use disposal, immutable accounting and exact reconciliation."""
    async with admission_database(monkeypatch) as database:
        names = await _seed(database)
        envelope, trust = await _fresh_fixture_authorization(database)
        identity = envelope["authorization"]["executor_identity"]
        before_by_field = dict(
            (await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping
        )
        envelope = await _prepare_disposal_case(database, monkeypatch, case, names, envelope)
        if case not in {"success", "lost_ack", "claim_lost_ack", "expired_replay"}:
            await _assert_failed_disposal_case(database, case, names, envelope, trust, identity, before_by_field)
            return
        mutation_result = await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=trust, executor_identity=identity
        )
        if case == "expired_replay":
            from datetime import timedelta

            monkeypatch.setattr(cleanup, "_now_utc", lambda: datetime.now(timezone.utc) + timedelta(hours=1))
        assert mutation_result == await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=trust, executor_identity=identity
        )
        assert mutation_result == await cleanup.reconcile_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust)
        after_by_field = dict(
            (await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping
        )
        assert after_by_field["last_error"].startswith(before_by_field["last_error"] + cleanup.MARKER)
        assert {
            key: field_value for key, field_value in before_by_field.items() if key not in {"last_error", "updated_at"}
        } == {
            key: field_value for key, field_value in after_by_field.items() if key not in {"last_error", "updated_at"}
        }
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 1
        for statement in (
            "UPDATE mrf.provider_directory_profile_failed_cleanup_claim SET nonce='changed'",
            "DELETE FROM mrf.provider_directory_profile_failed_cleanup_claim",
            "TRUNCATE mrf.provider_directory_profile_failed_cleanup_claim",
        ):
            with pytest.raises(Exception, match="claim_immutable"):
                await database.status(statement)
        counts = await database.first(
            fhir._profile_capacity_quiescence_sql("mrf"),
            active_statuses=list(fhir._PROFILE_ACTIVE_RUN_STATUSES),
            profile_params="{}",
            current_run_id=None,
            observed_at=datetime.now(timezone.utc),
            request_sha256="0" * 64,
        )
        assert counts._mapping["unexpired_capacity_consumption_count"] == 1
        from process.provider_directory_capacity_reservation_snapshot import capacity_reservation_snapshot

        observed_snapshot = await capacity_reservation_snapshot(fhir)
        assert observed_snapshot["contract_id"].endswith(".v2")
        assert len(observed_snapshot["failed_profile_cleanup_claims"]) == 1
        assert observed_snapshot["failed_profile_cleanup_claims"][0]["envelope"] == envelope
        await database.status(profile.profile_evidence_table_sql("mrf", names[0], logged=True))
        with pytest.raises(RuntimeError, match="disposed_stage_present"):
            await cleanup.reconcile_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust)


async def _seed_legacy(database):
    names = await _seed(database)
    await database.status(f'DROP TABLE "mrf"."{names[2]}"')
    await database.status("""UPDATE mrf.provider_directory_profile_build_checkpoint SET
        materialization_mode='full_swap',capacity_geometry_status='legacy_unavailable',capacity_geometry_hash=NULL,
        capacity_geometry_json=NULL,executable_plan_hash=NULL,affected_npi_stage=NULL,affected_npi_stage_oid=NULL,
        current_source_vector_hash=NULL,desired_source_vector_hash=NULL,
        current_source_context_vector_hash=NULL,desired_source_context_vector_hash=NULL,
        evidence_stage_storage_fingerprint=NULL,profile_stage_storage_fingerprint=NULL,affected_npi_stage_storage_fingerprint=NULL""")
    await database.status("UPDATE mrf.import_run SET params='{}',status='canceled' WHERE run_id=:owner", owner=OWNER)
    await database.status("DELETE FROM mrf.provider_directory_profile_serving_generation")
    published = "run_" + "b" * 32
    result_by_field = {
        "contract_id": "provider-directory-profile-selection-result-v1",
        "status": "published",
        "operation": "publish",
        "proof_id": "6" * 64,
        "generation": 6,
        "authority_revision": 6,
        "profile_generation_id": "pdprofile_" + "6" * 32,
    }
    await database.status(
        "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics,finished_at) "
        "VALUES (:run,'synthetic','provider-directory-fhir','succeeded','{}',CAST(:metrics AS json),now())",
        run=published,
        metrics=cleanup.canonical({"profile_selection_result": result_by_field}),
    )
    await database.status(
        """INSERT INTO mrf.provider_directory_profile
        (npi,profile_json,evidence_json,source_ids,endpoint_ids,dataset_ids,source_count,
         independent_source_count,fact_count,generation_id,published_at)
        VALUES (1000000004,'{}','{}',ARRAY['source-b'],ARRAY['endpoint-b'],ARRAY['dataset-b'],
                1,1,1,:generation,now())""",
        generation=result_by_field["profile_generation_id"],
    )
    return names[:2], published


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["wrong_generation", "older_history"])
async def test_legacy_history_binding(monkeypatch, case):
    """Require the latest genuine publication to be present in the incumbent Profile target."""
    async with admission_database(monkeypatch) as database:
        _names, published = await _seed_legacy(database)
        if case == "wrong_generation":
            await database.status(
                "UPDATE mrf.provider_directory_profile SET generation_id=:generation",
                generation="pdprofile_" + "9" * 32,
            )
            expected = "published_target_generation_missing"
        else:
            await database.status(
                """INSERT INTO mrf.import_run
                (run_id,engine,importer,status,params,metrics,finished_at)
                SELECT :run,engine,importer,status,params,metrics,finished_at+interval '1 second'
                FROM mrf.import_run WHERE run_id=:published""",
                run="run_" + "c" * 32,
                published=published,
            )
            expected = "published_history_not_latest"
        with pytest.raises(RuntimeError, match=expected):
            await _fresh_fixture_authorization(database, published_run_id=published)
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["success", "target_alias", "published_changed", "stage_changed", "partial", "metadata_missing", "payload"]
)
async def test_legacy_failed_profile_cleanup(monkeypatch, case):
    """Preserve incumbent targets while exact legacy cleanup succeeds or refuses stale authority."""
    async with admission_database(monkeypatch) as database:
        names, published = await _seed_legacy(database)
        if case == "partial":
            await database.status(
                "UPDATE mrf.import_run SET params=CAST(:params AS json) WHERE run_id=:owner",
                owner=OWNER,
                params=cleanup.canonical({"provider_directory_profile_generation": 7}),
            )
            with pytest.raises(RuntimeError, match="legacy_lineage_partial"):
                await _fresh_fixture_authorization(database, published_run_id=published)
            return
        if case == "payload":
            await database.status(
                "UPDATE mrf.provider_directory_profile_build_checkpoint SET last_error=:error", error="e" * 70000
            )
            with pytest.raises(RuntimeError, match="complete_checkpoint_payload_unsupported"):
                await _fresh_fixture_authorization(database, published_run_id=published)
            return
        envelope, trust = await _fresh_fixture_authorization(database, published_run_id=published)
        before_by_field = dict(
            (await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping
        )
        if case == "published_changed":
            await database.status(
                "UPDATE mrf.import_run SET params=CAST(:params AS json) WHERE run_id=:run",
                run=published,
                params=cleanup.canonical({"changed": True}),
            )
        if case == "stage_changed":
            await database.status(f'ALTER TABLE "mrf"."{names[0]}" ADD COLUMN changed text')
        if case == "metadata_missing":
            await database.status(
                "ALTER TABLE mrf.provider_directory_cms_serving_receipt RENAME TO hidden_fixture_receipt"
            )
        if case == "target_alias":
            # Re-sign exact aliasing manifest; closed validation refuses before any claim.
            envelope["authorization"]["stages"][0]["oid"] = envelope["authorization"]["publication"]["targets"][0][
                "oid"
            ]
            _unused, _trust, key = authorization_fixture()
            envelope = sign(envelope["authorization"], key)
        if case != "success":
            expected = _legacy_refusal_reason(case)
            with pytest.raises(RuntimeError, match=expected):
                await cleanup.execute_failed_profile_cleanup(
                    fhir,
                    envelope,
                    cleanup_trust=trust,
                    executor_identity=envelope["authorization"]["executor_identity"],
                )
            assert (
                await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
            )
            return
        await _assert_legacy_disposal_success(database, envelope, trust, before_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["observed_toast", "oversized_toast", "signed_payload_changed"])
async def test_cleanup_catalog_payload_projection(monkeypatch, case, record_property):
    """Refuse stale or oversized catalog payloads and preserve the full disposal bound."""
    async with admission_database(monkeypatch) as database:
        names, published = await _seed_legacy(database)
        if case != "signed_payload_changed":
            comment_text = os.urandom(2 * 1024 * 1024 if case == "oversized_toast" else 8000).hex()
            await database.status(f"""COMMENT ON TABLE "mrf"."{names[0]}" IS '{comment_text}'""")
        if case == "oversized_toast":
            with pytest.raises(RuntimeError, match="drop_catalog_wal_exceeded"):
                await _fresh_fixture_authorization(database, published_run_id=published)
        else:
            envelope, trust = await _fresh_fixture_authorization(database, published_run_id=published)
            dependencies = envelope["authorization"]["physical"]["stage_dependencies"]
            if case == "signed_payload_changed":
                await database.status(f"""COMMENT ON TABLE "mrf"."{names[0]}" IS 'changed catalog payload'""")
                with pytest.raises(RuntimeError, match="physical_preimage_changed"):
                    await cleanup.execute_failed_profile_cleanup(
                        fhir,
                        envelope,
                        cleanup_trust=trust,
                        executor_identity=envelope["authorization"]["executor_identity"],
                    )
            else:
                await _assert_catalog_disposal_budget(database, envelope, trust, dependencies, record_property)
                assert (
                    await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim")
                    == 1
                )
                return
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
        assert all([await fhir._provider_directory_profile_stage_relation_identity("mrf", name) for name in names[:2]])


@pytest.mark.asyncio
@pytest.mark.parametrize("fence", ["catalog", "publication"])
async def test_cleanup_catalog_fence_refuses_contention_before_claim(monkeypatch, fence):
    from sqlalchemy import text as sql_text

    async with admission_database(monkeypatch) as database:
        names, published = await _seed_legacy(database)
        envelope, trust = await _fresh_fixture_authorization(database, published_run_id=published)
        async with database.session() as blocker, blocker.begin():
            table_ref = (
                "pg_catalog.pg_description" if fence == "catalog" else "mrf.provider_directory_cms_serving_receipt"
            )
            await blocker.execute(sql_text(f"LOCK TABLE {table_ref} IN ROW EXCLUSIVE MODE NOWAIT"))
            with pytest.raises(Exception, match="could not obtain lock"):
                await cleanup.execute_failed_profile_cleanup(
                    fhir,
                    envelope,
                    cleanup_trust=trust,
                    executor_identity=envelope["authorization"]["executor_identity"],
                )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
        assert all([await fhir._provider_directory_profile_stage_relation_identity("mrf", name) for name in names[:2]])


@pytest.mark.asyncio
async def test_cleanup_catalog_role_refuses_without_privilege_elevation(monkeypatch):
    from sqlalchemy import text as sql_text

    async with admission_database(monkeypatch) as database:
        names = await _seed(database)
        async with database.transaction() as session:
            await session.execute(sql_text("SET LOCAL ROLE pg_monitor"))
            assert await database.scalar("SELECT rolsuper FROM pg_roles WHERE rolname=current_user") is False
            for locking in (False, True):
                with pytest.raises(RuntimeError, match="catalog_role_unsupported"):
                    await cleanup._require_drop_catalog_role(fhir, locking=locking)
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
        assert all([await fhir._provider_directory_profile_stage_relation_identity("mrf", name) for name in names])


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["default", "check", "index_expression", "foreign_key", "shadow_default"])
async def test_cleanup_metadata_refuses_executable_catalog_before_claim(monkeypatch, case):
    from sqlalchemy import text as sql_text

    async with admission_database(monkeypatch) as database:
        names = await _seed(database)
        envelope, trust = await _fresh_fixture_authorization(database)
        before_by_field = dict(
            (await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping
        )
        await database.status("CREATE TABLE mrf.unmodeled_cleanup_write (observed_at timestamptz)")
        await database.status("""CREATE FUNCTION mrf.clock_timestamp() RETURNS timestamptz LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO mrf.unmodeled_cleanup_write VALUES (pg_catalog.clock_timestamp());
            RETURN pg_catalog.clock_timestamp(); END; $$""")
        if case in {"default", "shadow_default"}:
            await database.status(
                "ALTER TABLE mrf.provider_directory_profile_failed_cleanup_claim "
                "ALTER COLUMN claimed_at SET DEFAULT mrf.clock_timestamp()"
            )
        if case == "check":
            await database.status(
                "ALTER TABLE mrf.provider_directory_profile_build_checkpoint "
                "ADD CHECK (mrf.clock_timestamp() IS NOT NULL)"
            )
            # Adding a CHECK validates existing rows; cleanup must add no further writes.
            await database.status("TRUNCATE mrf.unmodeled_cleanup_write")
        if case == "index_expression":
            await database.status(
                "CREATE INDEX unmodeled_cleanup_expression ON "
                "mrf.provider_directory_profile_failed_cleanup_claim ((lower(operation_id)))"
            )
        if case == "foreign_key":
            await database.status(
                "ALTER TABLE mrf.provider_directory_profile_failed_cleanup_claim "
                "ADD FOREIGN KEY (owner_run_id) REFERENCES mrf.import_run(run_id)"
            )
        if case == "shadow_default":
            async with database.transaction() as session:
                await session.execute(sql_text("SET LOCAL search_path=mrf,pg_catalog"))
                with pytest.raises(RuntimeError, match="metadata_executable_dependency_unsupported"):
                    await cleanup._metadata_layouts(
                        fhir,
                        "mrf",
                        locked=False,
                        tablespace_oid=envelope["authorization"]["database"]["tablespaces"][0]["tablespace_oid"],
                    )
        with pytest.raises(RuntimeError, match="metadata_catalog_unsupported"):
            await _fresh_fixture_authorization(database)
        with pytest.raises(RuntimeError, match="metadata_catalog_unsupported"):
            await cleanup.execute_failed_profile_cleanup(
                fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
            )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
        assert await database.scalar("SELECT count(*) FROM mrf.unmodeled_cleanup_write") == 0
        assert (
            dict((await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping)
            == before_by_field
        )
        assert all([await fhir._provider_directory_profile_stage_relation_identity("mrf", name) for name in names])


async def _server_volume_observation(database):
    """Observe real data/WAL storage on the server, including separated CI filesystems."""
    directory = await database.scalar("SHOW data_directory")
    assert await database.scalar("SHOW temp_tablespaces") == ""
    assert (
        await database.scalar(
            "SELECT pg_tablespace_location(dattablespace) FROM pg_database WHERE datname=current_database()"
        )
        == ""
    )
    command = "LC_ALL=C df -Pk " + shlex.quote(directory) + " " + shlex.quote(directory + "/pg_wal")
    async with database.transaction():
        await database.status("CREATE TEMP TABLE cleanup_fixture_storage (line text) ON COMMIT DROP")
        await database.status("COPY cleanup_fixture_storage FROM PROGRAM '" + command.replace("'", "''") + "'")
        rows = await database.all("SELECT line FROM cleanup_fixture_storage WHERE line NOT LIKE 'Filesystem%'")
    observations = [row[0].split(maxsplit=5) for row in rows]
    assert len(observations) == 2 and all(len(row) == 6 for row in observations)
    identities = {(row[0], row[5]) for row in observations}
    assert len(identities) == 1, "fixture requires colocated default data/temp/WAL storage"
    filesystem, mount = identities.pop()
    return cleanup.digest({"filesystem": filesystem, "mount": mount}), min(int(row[3]) * 1024 for row in observations)


@asynccontextmanager
async def _server_tablespace(database, name, metadata_ref):
    """Move metadata to exact server-owned UUID storage and verify resource cleanup."""
    import asyncpg

    from tests.cms_npd_admission_postgres_support import _database_url

    assert re.fullmatch(r"hc_(?:cleanup|initial)_test_[0-9a-f]{32}", name)
    directory = "/tmp/" + name
    admin = await asyncpg.connect(_database_url().set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        await admin.execute(f"COPY (SELECT NULL WHERE false) TO PROGRAM 'mkdir -m 700 {directory}'")
        try:
            await admin.execute(f"CREATE TABLESPACE {name} LOCATION '{directory}'")
            try:
                await database.status(f"ALTER {metadata_ref} SET TABLESPACE {name}")
                yield
            finally:
                await database.status(f"ALTER {metadata_ref} SET TABLESPACE pg_default")
                await admin.execute(f"DROP TABLESPACE {name}")
                assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_tablespace WHERE spcname=$1)", name)
        finally:
            await admin.execute(f"COPY (SELECT NULL WHERE false) TO PROGRAM 'rmdir {directory}'")
            assert await admin.fetchval("SELECT pg_stat_file($1, true)", directory) is None
    finally:
        await admin.close()


def _cleanup_tablespace_identity():
    """Return exact synthetic resource coordinates before creating either resource."""
    return "hc_cleanup_test_" + uuid4().hex


@pytest.mark.asyncio
@pytest.mark.parametrize("object_kind", ["claim_heap", "checkpoint_index"])
async def test_cleanup_metadata_refuses_unfunded_tablespace(monkeypatch, object_kind):
    async with admission_database(monkeypatch) as database:
        names = await _seed(database)
        envelope, trust = await _fresh_fixture_authorization(database)
        before_by_field = dict(
            (await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping
        )
        name = _cleanup_tablespace_identity()
        metadata_ref = (
            "TABLE mrf.provider_directory_profile_failed_cleanup_claim"
            if object_kind == "claim_heap"
            else "INDEX mrf.pd_profile_build_checkpoint_state_idx"
        )
        async with _server_tablespace(database, name, metadata_ref):
            for action in (
                _fresh_fixture_authorization(database),
                cleanup.execute_failed_profile_cleanup(
                    fhir,
                    envelope,
                    cleanup_trust=trust,
                    executor_identity=envelope["authorization"]["executor_identity"],
                ),
            ):
                with pytest.raises(RuntimeError, match="metadata_tablespace_unsupported"):
                    await action
            assert (
                await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
            )
            assert (
                dict((await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping)
                == before_by_field
            )
            assert all(
                [await fhir._provider_directory_profile_stage_relation_identity("mrf", stage) for stage in names]
            )


async def _install_initial_publication(database):
    """Apply the real sibling migration after the actual cleanup dependency slice."""
    import importlib.util

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.cms_npd_admission_postgres_support import MIGRATION_PREFIXES

    assert MIGRATION_PREFIXES[-4:] == ("20260930120000", "20260930130000", "20260930140000", "20261001100000")
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261001110000_profile_initial_publication.py"
    spec = importlib.util.spec_from_file_location("cleanup_initial_publication_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.down_revision == "20261001100000_profile_failed_cleanup_claim"

    def install(connection):
        with Operations.context(MigrationContext.configure(connection)):
            module.upgrade()

    async with database.engine.begin() as connection:
        await connection.run_sync(install)


@pytest.mark.asyncio
async def test_cleanup_after_initial_migration(monkeypatch, record_property):
    """Accept only the actual reviewed sibling CHECK while preserving old failed coordinates."""
    async with admission_database(monkeypatch) as database:
        names, published = await _seed_legacy(database)
        await _install_initial_publication(database)
        assert await database.scalar("SELECT to_regclass('mrf.provider_directory_profile_initial_receipt') IS NOT NULL")
        check = await database.scalar("""SELECT pg_get_constraintdef(oid,true) FROM pg_constraint
            WHERE conrelid='mrf.provider_directory_profile_build_checkpoint'::regclass
              AND conname='pd_profile_build_checkpoint_delta_identity_check'""")
        assert "healthporta.provider-directory-profile-initial-capacity-geometry.v1" in check
        record_property(
            "cleanup_actual_migration_chain",
            "20260930120000>20260930130000>20260930140000>20261001100000>20261001110000",
        )
        envelope, trust = await _fresh_fixture_authorization(database, published_run_id=published)
        receipt = await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
        )
        assert receipt == await cleanup.reconcile_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust)
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 1
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_initial_receipt") == 0
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is None for name in names]
        )


async def _seed_failed_checkpoint(database, lineage, names, oids_by_field, geometry, fingerprints):
    """Insert the unchanged terminal owner and retained failed checkpoint fixture."""
    await database.status(
        "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,finished_at) "
        "VALUES (:owner,'synthetic','provider-directory-fhir','failed',CAST(:params AS json),now())",
        owner=OWNER,
        params=cleanup.canonical(
            {
                "provider_directory_profile_selection_attestation": {
                    "proof_id": lineage.proof_id,
                    "authority_revision": 7,
                },
                "provider_directory_profile_generation": 7,
            }
        ),
    )
    await database.status(
        """INSERT INTO mrf.provider_directory_profile_build_checkpoint
        (build_id,strategy_version,schema_version,resume_lineage_hash,owner_run_id,state,profile_as_of,source_ids,retained_source_ids,
         dataset_ids,evidence_stage,profile_stage,affected_npi_stage,evidence_stage_oid,profile_stage_oid,affected_npi_stage_oid,
         evidence_target_oid,profile_target_oid,has_existing_artifacts,evidence_total_batches,profile_total_batches,
         materialization_mode,refresh_source_ids,removed_source_ids,current_source_vector_hash,desired_source_vector_hash,
         current_source_context_vector_hash,desired_source_context_vector_hash,capacity_geometry_status,capacity_geometry_hash,
         capacity_geometry_json,executable_plan_hash,evidence_stage_storage_fingerprint,profile_stage_storage_fingerprint,
         affected_npi_stage_storage_fingerprint,last_error)
        VALUES (:build,:strategy,1,:lineage,:owner,'failed','2026-07-30','["source-a"]','["source-b"]','["dataset-a-new"]',
         :evidence,:profile,:affected,:eoid,:poid,:aoid,:etarget,:ptarget,true,1,1,'source_delta','["source-a"]','[]',
         :current,:desired,:current_context,:desired_context,'verified',:geometry_hash,CAST(:geometry AS jsonb),:plan,:efp,:pfp,:afp,:error)""",
        build=BUILD,
        strategy=profile.PROFILE_BUILD_STRATEGY_VERSION,
        lineage=lineage.resume_hash,
        owner=OWNER,
        evidence=names[0],
        profile=names[1],
        affected=names[2],
        eoid=oids_by_field["evidence_stage"],
        poid=oids_by_field["profile_stage"],
        aoid=oids_by_field["affected_npi_stage"],
        etarget=oids_by_field["evidence_target"],
        ptarget=oids_by_field["profile_target"],
        current=lineage.from_vector_hash,
        desired=lineage.to_vector_hash,
        current_context=lineage.from_context_vector_hash,
        desired_context=lineage.to_context_vector_hash,
        geometry_hash=capacity.capacity_geometry_hash(geometry),
        geometry=capacity.canonical_capacity_geometry_json(geometry),
        plan=lineage.plan_hash,
        efp=fingerprints[0],
        pfp=fingerprints[1],
        afp=fingerprints[2],
        error="complete synthetic failed-stage error\nretained original details",
    )


async def _seed_geometry_and_layouts(schema, lineage, oids_by_field, names):
    """Observe native cleanup layouts and attach the original unadmitted fixture geometry."""
    serving = await fhir._provider_directory_profile_serving_state(schema)
    physical = await cleanup._physical_identity(fhir, schema, serving)
    geometry_payload = _geometry_payload(
        selection_proof_id=lineage.proof_id,
        profile_schema_version=1,
        profile_strategy_version=profile.PROFILE_BUILD_STRATEGY_VERSION,
        executable_plan_hash=lineage.plan_hash,
        current_source_vector_hash=lineage.from_vector_hash,
        desired_source_vector_hash=lineage.to_vector_hash,
        current_context_vector_hash=lineage.from_context_vector_hash,
        desired_context_vector_hash=lineage.to_context_vector_hash,
        evidence_target_oid=oids_by_field["evidence_target"],
        profile_target_oid=oids_by_field["profile_target"],
    )
    geometry_payload.update(
        {key: field_value for key, field_value in vars(physical).items() if key in geometry_payload}
    )
    metadata_oids = await fhir._profile_capacity_metadata_oids(schema)
    geometry_payload.update({name + "_oid": oid for name, oid in metadata_oids.items()})
    geometry_payload.update(
        physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID, artifact_scope_batch_size=1
    )
    geometry = capacity.validated_capacity_geometry(geometry_payload)
    fingerprints = [
        await fhir._provider_directory_profile_stage_storage_fingerprint(
            schema, name, expected_oid=oids_by_field[role], lock_relation=False
        )
        for name, role in zip(names, ("evidence_stage", "profile_stage", "affected_npi_stage"), strict=True)
    ]
    return geometry, fingerprints


async def _assert_failed_disposal_case(database, case, names, envelope, trust, identity, before_by_field):
    """Assert the exact refusal, retained stages and spent-claim behavior for a failed cleanup."""
    expected = {
        "owner": "owner_not_failed_terminal",
        "oid": "stage_identity_changed",
        "preimage": "checkpoint_preimage_changed",
        "serving": "build_is_serving",
        "budget": "budget_exceeded",
        "rollback": "synthetic_after_last_drop",
        "cancel": None,
    }[case]
    import asyncio

    with pytest.raises(asyncio.CancelledError if case == "cancel" else RuntimeError, match=expected):
        await cleanup.execute_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust, executor_identity=identity)
    claims = await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim")
    assert claims == (1 if case in {"rollback", "cancel"} else 0)
    if case in {"rollback", "cancel"}:
        assert (
            dict((await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping)
            == before_by_field
        )
        assert all([await fhir._provider_directory_profile_stage_relation_identity("mrf", name) for name in names])
        with pytest.raises(RuntimeError, match="completion_missing"):
            await cleanup.execute_failed_profile_cleanup(
                fhir, envelope, cleanup_trust=trust, executor_identity=identity
            )


async def _assert_legacy_disposal_success(database, envelope, trust, before_by_field):
    """Assert successful legacy disposal preserves every incumbent and original checkpoint field."""
    mutation_result = await cleanup.execute_failed_profile_cleanup(
        fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
    )
    assert mutation_result["contract_id"] == cleanup.LEGACY_RECEIPT_CONTRACT
    assert mutation_result == await cleanup.reconcile_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust)
    after_by_field = dict(
        (await database.first("SELECT * FROM mrf.provider_directory_profile_build_checkpoint"))._mapping
    )
    assert {
        name: field_value for name, field_value in before_by_field.items() if name not in {"last_error", "updated_at"}
    } == {name: field_value for name, field_value in after_by_field.items() if name not in {"last_error", "updated_at"}}
    assert (
        after_by_field["capacity_geometry_json"] is None
        and after_by_field["evidence_stage_storage_fingerprint"] is None
    )
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_serving_generation") == 0
    assert all(
        [
            await database.scalar("SELECT to_regclass(:name) IS NOT NULL", name="mrf." + name)
            for name in (profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE)
        ]
    )


async def _assert_catalog_disposal_budget(database, envelope, trust, dependencies, record_property):
    """Verify the native catalog payload projection and retain scoped WAL observations."""
    assert any(
        entry["deleted_toast_chunks"] > 0
        for dependency in dependencies
        for entry in dependency["catalog_deletions"]
        if entry["catalog"] == "pg_description"
    )
    assert all(
        dependency["drop_wal_upper_bytes"]
        == 32 * 1024 * 1024 + sum(entry["wal_upper_bytes"] for entry in dependency["catalog_deletions"])
        for dependency in dependencies
    )
    receipt = await cleanup.execute_failed_profile_cleanup(
        fhir,
        envelope,
        cleanup_trust=trust,
        executor_identity=envelope["authorization"]["executor_identity"],
    )
    committed_wal = await database.scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
        start=receipt["wal_start_lsn"],
    )
    assert committed_wal <= envelope["authorization"]["limits"]["wal_bytes"]
    record_property("cleanup_committed_cluster_wal_bytes", committed_wal)
    record_property("cleanup_signed_wal_ceiling_bytes", envelope["authorization"]["limits"]["wal_bytes"])
    record_property(
        "cleanup_description_old_toast_chunks",
        sum(
            entry["deleted_toast_chunks"]
            for dependency in dependencies
            for entry in dependency["catalog_deletions"]
            if entry["catalog"] == "pg_description"
        ),
    )
    record_property(
        "cleanup_catalog_additive_wal_bytes",
        sum(entry["wal_upper_bytes"] for dependency in dependencies for entry in dependency["catalog_deletions"]),
    )


def _legacy_refusal_reason(case):
    """Preserve the exact expected failure for each malformed legacy authority case."""
    return {
        "published_changed": "publication_preimage_changed",
        "stage_changed": "stage_manifest_changed",
        "metadata_missing": "installed_publication_metadata_missing",
        "target_alias": "stage_is_serving",
    }[case]


async def _seed_initial_cleanup(database, *, legacy):
    """Retain a real migrated verified initial checkpoint, never fabricate an absent table pair."""
    from process import provider_directory_profile_initial as initial
    from process import provider_directory_profile_initial_contract as contract

    names = await _seed(database)
    await database.status(f'DROP TABLE "mrf"."{names[2]}"')
    await database.status("DELETE FROM mrf.provider_directory_profile_serving_generation")
    await _install_initial_publication(database)
    if legacy:
        await _seed_initial_cleanup_history(database)
    async with database.transaction():
        initial_targets = await initial.capture_targets(fhir, "mrf")
        receipt = await initial.receipt_layout(fhir, "mrf")
        checkpoint_layout = (await cleanup._metadata_layouts(fhir, "mrf", locked=False, tablespace_oid=1663))[
            "checkpoint"
        ]
    checkpoint_row = await database.first(
        "SELECT capacity_geometry_json FROM mrf.provider_directory_profile_build_checkpoint"
    )
    geometry_by_field = dict(checkpoint_row[0])
    geometry_by_field.update(
        contract_id=contract.GEOMETRY_CONTRACT,
        materialization_mode="full_swap",
        current_source_vector_hash=None,
        current_context_vector_hash=None,
        initial_target_state_sha256=contract.target_state_sha256(initial_targets.payload),
        initial_receipt_oid=receipt.relation_oid,
        initial_receipt_storage_fingerprint=receipt.exact_fingerprint,
        build_checkpoint_storage_fingerprint=checkpoint_layout.exact_fingerprint,
    )
    geometry_by_field.update(
        {
            name: field_value
            for name, field_value in initial_targets.payload.items()
            if "_target_" in name and name in geometry_by_field
        }
    )
    geometry_by_field = capacity.validated_capacity_geometry(geometry_by_field)
    await database.status(
        """UPDATE mrf.provider_directory_profile_build_checkpoint SET
        materialization_mode='full_swap',affected_npi_stage=NULL,affected_npi_stage_oid=NULL,
        affected_npi_stage_storage_fingerprint=NULL,current_source_vector_hash=NULL,current_source_context_vector_hash=NULL,
        capacity_geometry_hash=:hash,capacity_geometry_json=CAST(:geometry AS jsonb)""",
        hash=capacity.capacity_geometry_hash(geometry_by_field),
        geometry=capacity.canonical_capacity_geometry_json(geometry_by_field),
    )
    return names[:2], initial_targets.payload


async def _seed_initial_cleanup_history(database):
    """Install one authentic old undated terminal result and matching incumbent witnesses."""
    from process import provider_directory_profile_selection as selection
    from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution

    result_by_field = selection.profile_selection_result(
        synthetic_profile_execution(),
        profile_generation_id="pdprofile_" + "9" * 32,
        profile_rows=1,
        profile_source_evidence_rows=1,
        profile_as_of="2026-08-09",
    )
    result_by_field.pop("profile_as_of")
    await database.status(
        """INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics,finished_at)
        VALUES (:run,'synthetic','provider-directory-fhir','succeeded','{}',CAST(:metrics AS json),now())""",
        run="run_" + "b" * 32,
        metrics=cleanup.canonical({"profile_selection_result": result_by_field}),
    )
    await database.status(
        """INSERT INTO mrf.provider_directory_profile
        (npi,profile_json,evidence_json,source_ids,endpoint_ids,dataset_ids,source_count,independent_source_count,fact_count,generation_id,published_at)
        VALUES (1234567890,'{}','{}',ARRAY['synthetic_profile_source'],ARRAY['synthetic-endpoint-1'],ARRAY['synthetic-dataset-1'],1,1,1,:generation,now())""",
        generation=result_by_field["profile_generation_id"],
    )
    await database.status(
        """INSERT INTO mrf.provider_directory_profile_evidence
        (evidence_key,npi,fact_type,fact_key,value_json,source_id,endpoint_id,dataset_id,resource_type,resource_id)
        VALUES (:key,1234567890,'name',:key,'{}','synthetic_profile_source','synthetic-endpoint-1','synthetic-dataset-1','Practitioner','synthetic-resource')""",
        key="a" * 32,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_verified_initial_failed_cleanup_preserves_incumbents(monkeypatch, legacy, record_property):
    """Two failed stages dispose under real fences while original physical targets survive."""
    from process import provider_directory_profile_initial as initial

    async with admission_database(monkeypatch) as database:
        names, before_target = await _seed_initial_cleanup(database, legacy=legacy)
        envelope, trust = await _fresh_fixture_authorization(database)
        body_bytes = len(cleanup.canonical(envelope["authorization"]).encode())
        record_property("initial_cleanup_authorization_bytes", body_bytes)
        record_property("initial_cleanup_claim_payload_upper_bytes", 4 * body_bytes + 1024)
        receipt = await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
        )
        assert receipt["contract_id"] == cleanup.INITIAL_RECEIPT_CONTRACT
        assert receipt["variant"] == "initial_full_swap"
        assert receipt["disposed_stages"] == envelope["authorization"]["stages"]
        assert len(receipt["disposed_stages"]) == 2
        async with database.transaction():
            assert (await initial.capture_targets(fhir, "mrf")).payload == before_target
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is None for name in names]
        )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_initial_receipt") == 0
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 1
        assert receipt == await cleanup.reconcile_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["geometry", "target", "receipt_layout", "receipt_binding", "target_alias", "rollback"]
)
async def test_initial_cleanup_refuses_drift_and_never_resumes_spent_disposal(
    monkeypatch, cleanup_history_directory, change
):
    """Original geometry, targets and receipt storage remain fenced before spending or DROP."""
    async with admission_database(monkeypatch) as database:
        names, _targets = await _seed_initial_cleanup(database, legacy=False)
        envelope, trust = await _fresh_fixture_authorization(database)
        if change == "geometry":
            await database.status(
                "UPDATE mrf.provider_directory_profile_build_checkpoint SET capacity_geometry_hash=:hash", hash="0" * 64
            )
        if change == "target":
            await database.status("ALTER TABLE mrf.provider_directory_profile ADD COLUMN unexpected integer")
        if change == "receipt_layout":
            await database.status(
                "ALTER TABLE mrf.provider_directory_profile_initial_receipt ADD COLUMN unexpected integer"
            )
        if change in {"receipt_binding", "target_alias"}:
            if change == "receipt_binding":
                envelope["authorization"]["initial"]["initial_receipt_storage_fingerprint"] = "0" * 64
            else:
                envelope["authorization"]["stages"][0]["oid"] = envelope["authorization"]["initial"]["target_state"][
                    "evidence_target_oid"
                ]
            envelope = sign(envelope["authorization"], authorization_fixture()[2])
        if change == "rollback":
            envelope = await _prepare_disposal_case(database, monkeypatch, change, names, envelope)
        with pytest.raises(RuntimeError):
            await cleanup.execute_failed_profile_cleanup(
                fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
            )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == (
            1 if change == "rollback" else 0
        )
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is not None for name in names]
        )
        if change == "rollback":
            from tests.test_provider_directory_profile_failed_cleanup import (
                historical_trust_document,
                install_historical_trust,
            )

            # Even an authentic archived signer cannot resume an incomplete spent permit.
            with monkeypatch.context() as settings:
                install_historical_trust(settings, cleanup_history_directory, [historical_trust_document(trust)])
                trust.keys = ()
                with pytest.raises(RuntimeError, match="completion_missing"):
                    await cleanup.execute_failed_profile_cleanup(
                        fhir,
                        envelope,
                        cleanup_trust=trust,
                        executor_identity=envelope["authorization"]["executor_identity"],
                    )
            assert all(
                [
                    await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is not None
                    for name in names
                ]
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["source_delta", "legacy_full_swap", "initial_full_swap"])
@pytest.mark.parametrize("rotation", ["removed", "retired", "volumes", "overlap_missing"])
async def test_completed_cleanup_history_survives_rotation_without_new_authority(
    monkeypatch, cleanup_history_directory, variant, rotation
):
    """Exact execute/reconcile/reaper history verifies with original public trust and no writes."""
    from datetime import timedelta

    from tests.test_provider_directory_profile_failed_cleanup import historical_trust_document, install_historical_trust

    async with admission_database(monkeypatch) as database:
        published = None
        if variant == "initial_full_swap":
            names, _target = await _seed_initial_cleanup(database, legacy=False)
        elif variant == "legacy_full_swap":
            names, published = await _seed_legacy(database)
        else:
            names = await _seed(database)
        envelope, trust = await _fresh_fixture_authorization(database, published_run_id=published)
        if rotation == "overlap_missing":
            await _assert_missing_trust_refuses_disposal(
                monkeypatch, cleanup_history_directory, database, envelope, trust, names
            )
            receipt = await _native_cleanup_operator(monkeypatch, cleanup_history_directory, envelope, "execute")
            (cleanup_history_directory / "cleanup-current.json").unlink()
        else:
            receipt = await cleanup.execute_failed_profile_cleanup(
                fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
            )
        claimed = cleanup.timestamp(envelope["authorization"]["issued_at"])
        if rotation != "overlap_missing":
            install_historical_trust(monkeypatch, cleanup_history_directory, [historical_trust_document(trust)])
        trust = _rotated_cleanup_trust(trust, rotation, claimed)
        monkeypatch.setattr(cleanup, "_now_utc", lambda: claimed + timedelta(hours=1))
        before = await database.scalar(
            "SELECT row_to_json(c)::text FROM mrf.provider_directory_profile_build_checkpoint c"
        )
        assert receipt == await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
        )
        assert receipt == await cleanup.reconcile_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust)
        if rotation == "overlap_missing":
            for mode in ("execute", "reconcile"):
                assert receipt == await _native_cleanup_operator(monkeypatch, cleanup_history_directory, envelope, mode)
        with monkeypatch.context() as settings:
            if rotation != "overlap_missing":
                settings.setattr(cleanup, "configured_cleanup_trust", lambda: trust)
            # Exercise the genuine admitted reaper branch without claiming runtime admission.
            settings.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: object())
            assert (
                await fhir._reap_stale_provider_directory_profile_builds("mrf", current_build_id="pdpb_" + "f" * 32)
                == 0
            )
        assert (
            await database.scalar("SELECT row_to_json(c)::text FROM mrf.provider_directory_profile_build_checkpoint c")
            == before
        )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 1
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is None for name in names]
        )


async def _assert_missing_trust_refuses_disposal(monkeypatch, directory, database, envelope, trust, names):
    """A real missing live file and authentic history never grant a new disposal."""
    from tests.test_provider_directory_profile_failed_cleanup import (
        install_current_trust,
        install_historical_trust,
        overlapping_historical_trust,
    )

    claimed = cleanup.timestamp(envelope["authorization"]["issued_at"])
    documents = overlapping_historical_trust(trust, claimed)
    install_historical_trust(monkeypatch, directory, documents)
    path = install_current_trust(monkeypatch, directory, documents[-1])
    path.unlink()
    with pytest.raises(FileNotFoundError):
        cleanup.configured_cleanup_trust()
    with pytest.raises(FileNotFoundError):
        await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=None, executor_identity=envelope["authorization"]["executor_identity"]
        )
    with pytest.raises(FileNotFoundError):
        await _native_cleanup_operator(monkeypatch, directory, envelope, "execute")
    path = install_current_trust(monkeypatch, directory, documents[-1])
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_TRUST_SHA256", "0" * 64)
    with pytest.raises(RuntimeError, match="independent_trust_digest_changed"):
        await _native_cleanup_operator(monkeypatch, directory, envelope, "execute")
    path.unlink()
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
    assert all(
        [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is not None for name in names]
    )

    install_current_trust(monkeypatch, directory, documents[0])


def _rotated_cleanup_trust(trust, rotation, claimed):
    """Keep the original current-policy rotation cases alongside absent live trust."""
    from datetime import timedelta

    match rotation:
        case "overlap_missing":
            trust = None
        case "removed":
            trust.keys = ()
        case "retired":
            trust.keys[0].status = "retired"
            trust.keys[0].retired_at = claimed + timedelta(minutes=1)
            trust.keys[0].verify_until = claimed + timedelta(minutes=16)
        case "volumes":
            trust.volumes = tuple(dict(volume_by_field, volume_digest="0" * 64) for volume_by_field in trust.volumes)
    return trust


async def _native_cleanup_operator(monkeypatch, directory, envelope, mode):
    """Use real CLI, protected file and DB guards with explicit connection/runtime scaffolding."""
    from unittest.mock import AsyncMock

    path = directory / "cleanup-envelope.json"
    path.write_text(cleanup.canonical(envelope))
    path.chmod(0o600)
    arguments = SimpleNamespace(
        mode=mode, private_input_file=str(path), build_id=None, owner_run_id=None, published_run_id=None
    )
    with monkeypatch.context() as settings:
        settings.setattr(fhir.db, "connect", AsyncMock())
        settings.setattr(fhir.db, "disconnect", AsyncMock())
        settings.setattr(
            cleanup,
            "observed_executor_identity",
            AsyncMock(return_value=envelope["authorization"]["executor_identity"]),
        )
        try:
            return await cleanup._operator(arguments)
        finally:
            fhir.db.connect.assert_awaited_once()
            fhir.db.disconnect.assert_awaited_once()
