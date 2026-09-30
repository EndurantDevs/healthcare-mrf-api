# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native PostgreSQL transaction proofs for scoped registration authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from db.models.custom_import import (
    CustomImportDataset,
    CustomImportRegistrationAuthority,
    CustomImportSourceBindingRevision,
)
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.registration_authority import (
    RegistrationAuthorityConflict,
    RegistrationAuthorityDenied,
    RegistrationAuthorityError,
    get_registration_authority,
    mint_registration_authority,
    register_with_authority,
    revoke_registration_authority,
)
from process.custom_import.snowflake_binding import SnowflakeSourceBinding
from process.custom_import.snowflake_source_binding import register_snowflake_source_binding
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_definition_store_postgres import _counts as _definition_graph_counts
from tests.test_custom_import_snowflake_operator_cli import _definition, _loaded_binding


@asynccontextmanager
async def _authority_case():
    # This isolated mapped table does not validate migration triggers or grants.
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(CustomImportRegistrationAuthority.__table__.create)
        yield case


def _registration(*, dataset_key: str = "synthetic_authority", unicode_alias: bool = False, refresh_mode="snapshot"):
    definition_document = json.loads(_definition().canonical)
    definition_document["refresh_mode"] = refresh_mode
    if unicode_alias:
        definition_document["aliases"]["root_source"] = {"RÓOT_NPI": "npi"}
    definition = CustomImportDefinition.from_mapping(definition_document)
    binding_document = json.loads(_loaded_binding().binding.canonical)
    binding_document["definition_sha256"] = definition.digest
    binding_document["schema_sha256"] = definition.schema_digest
    binding = SnowflakeSourceBinding.from_mapping(binding_document)
    return {
        "dataset_key": dataset_key,
        "definition": json.loads(definition.canonical),
        "source_binding": json.loads(binding.canonical),
    }


def _intent(*, seconds: float = 180):
    token = secrets.token_bytes(32)
    return secrets.token_hex(32), token, hashlib.sha256(token).digest(), datetime.now(UTC) + timedelta(seconds=seconds)


async def _mint(case, authority_id, registration, expires_at, token_sha256):
    async with case.sessions() as session, session.begin():
        return await mint_registration_authority(
            session,
            authority_id=authority_id,
            registration=registration,
            expires_at=expires_at,
            token_sha256=token_sha256,
        )


async def _register(case, authority_id, registration, token):
    async with case.sessions() as session, session.begin():
        return await register_with_authority(session, authority_id=authority_id, registration=registration, token=token)


async def _revoke(case, authority_id):
    async with case.sessions() as session, session.begin():
        return await revoke_registration_authority(session, authority_id)


async def _dataset_count(case):
    async with case.sessions() as session:
        return await session.scalar(select(func.count()).select_from(CustomImportDataset))


async def _wait_for_authority_lock(case, authority_id):
    """Observe the registrar's row lock before starting a contending revoke."""

    for _ in range(50):
        try:
            async with case.sessions() as session, session.begin():
                await session.execute(
                    select(CustomImportRegistrationAuthority)
                    .where(CustomImportRegistrationAuthority.authority_id == authority_id)
                    .with_for_update(nowait=True)
                )
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "55P03":
                return
            raise
        await asyncio.sleep(0.02)
    raise AssertionError("registration did not lock authority before dataset")


@pytest.mark.asyncio
async def test_exact_mint_register_and_completed_replay_preserve_one_graph(monkeypatch):
    registration = _registration()
    authority_id, token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        minted = await _mint(case, authority_id, registration, expires_at, token_sha256)
        replayed_mint = await _mint(case, authority_id, registration, expires_at, token_sha256)
        assert replayed_mint == minted
        assert minted.result is None
        assert minted.input_sha256 == hashlib.sha256(canonical_json(registration).encode("utf-8")).digest()

        with pytest.raises(RegistrationAuthorityConflict):
            await _mint(case, authority_id, registration, expires_at + timedelta(seconds=1), token_sha256)
        with pytest.raises(RegistrationAuthorityConflict):
            await _mint(case, authority_id, registration, expires_at, hashlib.sha256(secrets.token_bytes(32)).digest())
        with pytest.raises(RegistrationAuthorityConflict):
            await _mint(case, authority_id, _registration(dataset_key="synthetic_other"), expires_at, token_sha256)
        with pytest.raises(RegistrationAuthorityDenied):
            await _register(case, authority_id, registration, secrets.token_bytes(32))
        assert await _dataset_count(case) == 0

        completed = await _register(case, authority_id, registration, token)
        assert completed.result is not None
        assert completed.result["status"] == "registered"
        assert await _dataset_count(case) == 1
        async with case.sessions() as session:
            committed = await get_registration_authority(session, authority_id)
        assert committed == completed
        with pytest.raises(RegistrationAuthorityDenied):
            await _register(case, authority_id, registration, secrets.token_bytes(32))

        import process.custom_import.registration_authority as authority_store

        async def unexpected_graph_call(*_args, **_kwargs):
            raise AssertionError("completed replay must not invoke graph registration")

        monkeypatch.setattr(authority_store, "register_snowflake_source_binding", unexpected_graph_call)
        revoked = await _revoke(case, authority_id)
        assert revoked.revoked_at is not None
        assert revoked.result == completed.result
        replay = await _register(case, authority_id, registration, token)
        assert replay.result_receipt == completed.result_receipt
        assert replay.revoked_at == revoked.revoked_at
        assert await _dataset_count(case) == 1
        with pytest.raises(RegistrationAuthorityConflict):
            await _register(case, authority_id, _registration(dataset_key="synthetic_other"), token)


@pytest.mark.asyncio
async def test_existing_definition_identity_conflict_is_not_a_retryable_failure():
    first = _registration(dataset_key="synthetic_conflict")
    conflicting = _registration(dataset_key="synthetic_conflict", refresh_mode="upsert")
    authority_id, token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        async with case.sessions() as session, session.begin():
            await register_snowflake_source_binding(
                session,
                dataset_key=first["dataset_key"],
                definition=CustomImportDefinition.from_mapping(first["definition"]),
                binding=SnowflakeSourceBinding.from_mapping(first["source_binding"]),
            )
        await _mint(case, authority_id, conflicting, expires_at, token_sha256)
        with pytest.raises(RegistrationAuthorityConflict):
            await _register(case, authority_id, conflicting, token)
        async with case.sessions() as session:
            state = await get_registration_authority(session, authority_id)
            assert state is not None and state.result is None
            assert await session.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision)) == 1


@pytest.mark.asyncio
async def test_invalid_graph_result_rolls_back_and_is_not_unavailable(monkeypatch):
    import process.custom_import.registration_authority as authority_store
    from process.custom_import.snowflake_binding import SnowflakeSourceBindingError

    registration = _registration(dataset_key="synthetic_invalid_graph")
    authority_id, token, token_sha256, expires_at = _intent()
    original_register = authority_store.register_snowflake_source_binding

    async def invalid_graph(*args, **kwargs):
        await original_register(*args, **kwargs)
        raise SnowflakeSourceBindingError("synthetic invalid graph input")

    async with _authority_case() as case:
        await _mint(case, authority_id, registration, expires_at, token_sha256)
        monkeypatch.setattr(authority_store, "register_snowflake_source_binding", invalid_graph)
        with pytest.raises(RegistrationAuthorityError):
            await _register(case, authority_id, registration, token)
        async with case.sessions() as session:
            state = await get_registration_authority(session, authority_id)
            assert state is not None and state.result is None
            assert await _definition_graph_counts(session) == (0,) * 9
            assert await session.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision)) == 0


@pytest.mark.asyncio
async def test_absent_first_revoke_cannot_be_minted_or_registered():
    registration = _registration()
    authority_id, token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        tombstone = await _revoke(case, authority_id)
        assert tombstone.revoked_at is not None
        assert tombstone.input_sha256 is None
        assert tombstone.token_sha256 is None
        assert tombstone.expires_at is None
        assert tombstone.result is None
        assert await _revoke(case, authority_id) == tombstone
        with pytest.raises(RegistrationAuthorityConflict):
            await _mint(case, authority_id, registration, expires_at, token_sha256)
        with pytest.raises(RegistrationAuthorityDenied):
            await _register(case, authority_id, registration, token)
        second_id, second_token, second_hash, second_deadline = _intent()
        await _mint(case, second_id, registration, second_deadline, second_hash)
        await _revoke(case, second_id)
        with pytest.raises(RegistrationAuthorityDenied):
            await _register(case, second_id, registration, second_token)
        assert await _dataset_count(case) == 0


@pytest.mark.asyncio
async def test_absent_first_revoke_wins_against_concurrent_mint():
    registration = _registration()
    authority_id, _token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        task = None
        try:
            async with case.sessions() as holder, holder.begin():
                tombstone = await revoke_registration_authority(holder, authority_id)
                task = asyncio.create_task(_mint(case, authority_id, registration, expires_at, token_sha256))
                await asyncio.sleep(0.05)
                assert not task.done()
            with pytest.raises(RegistrationAuthorityConflict):
                await asyncio.wait_for(task, timeout=10)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        async with case.sessions() as session:
            assert await get_registration_authority(session, authority_id) == tombstone
        assert await _dataset_count(case) == 0


@pytest.mark.asyncio
async def test_invalid_digest_and_partial_pins_leave_no_authority():
    registration = _registration()
    authority_id, _token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        with pytest.raises(RegistrationAuthorityError):
            await _mint(case, authority_id, registration, expires_at, token_sha256[:-1])
        async with case.sessions() as session:
            assert await get_registration_authority(session, authority_id) is None

        for values in (
            {"input_sha256": hashlib.sha256(secrets.token_bytes(32)).digest(), "expires_at": expires_at},
            {"input_sha256": secrets.token_bytes(31), "token_sha256": token_sha256, "expires_at": expires_at},
        ):
            row_id = secrets.token_hex(32)
            with pytest.raises(IntegrityError):
                async with case.sessions() as session, session.begin():
                    await session.execute(
                        insert(CustomImportRegistrationAuthority.__table__).values(authority_id=row_id, **values)
                    )
            async with case.sessions() as session:
                assert await get_registration_authority(session, row_id) is None


@pytest.mark.asyncio
async def test_unicode_alias_is_rejected_before_authority_insert():
    registration = _registration(unicode_alias=True)
    authority_id, _token, token_sha256, expires_at = _intent()
    semantic_bytes = canonical_json(registration).encode("utf-8")
    assert "RÓOT_NPI".encode("utf-8") in semantic_bytes
    async with _authority_case() as case:
        with pytest.raises(RegistrationAuthorityError):
            await _mint(case, authority_id, registration, expires_at, token_sha256)
        async with case.sessions() as session:
            assert await get_registration_authority(session, authority_id) is None
        assert await _dataset_count(case) == 0


@pytest.mark.asyncio
async def test_registration_locks_authority_before_contended_dataset():
    registration = _registration(dataset_key="synthetic_contended")
    authority_id, token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        await _mint(case, authority_id, registration, expires_at, token_sha256)
        async with case.sessions() as session, session.begin():
            session.add(CustomImportDataset(dataset_key=registration["dataset_key"]))
            await session.flush()

        tasks = []
        try:
            async with case.sessions() as dataset_holder, dataset_holder.begin():
                await dataset_holder.execute(
                    select(CustomImportDataset)
                    .where(CustomImportDataset.dataset_key == registration["dataset_key"])
                    .with_for_update()
                )
                registration_task = asyncio.create_task(_register(case, authority_id, registration, token))
                tasks.append(registration_task)
                await _wait_for_authority_lock(case, authority_id)
                assert not registration_task.done()
                revoke_task = asyncio.create_task(_revoke(case, authority_id))
                tasks.append(revoke_task)
                await asyncio.sleep(0.05)
                assert not revoke_task.done()
            completed, revoked = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        assert completed.result is not None
        assert revoked.revoked_at is not None
        assert revoked.result == completed.result


@pytest.mark.asyncio
async def test_expiry_is_checked_after_authority_lock_wait():
    registration = _registration(dataset_key="synthetic_expiring")
    async with _authority_case() as case:
        authority_id, token, token_sha256, expires_at = _intent(seconds=1)
        await _mint(case, authority_id, registration, expires_at, token_sha256)
        task = None
        try:
            async with case.sessions() as holder, holder.begin():
                await holder.execute(
                    select(CustomImportRegistrationAuthority)
                    .where(CustomImportRegistrationAuthority.authority_id == authority_id)
                    .with_for_update()
                )
                task = asyncio.create_task(_register(case, authority_id, registration, token))
                await asyncio.sleep(0.1)
                assert not task.done()
                await asyncio.sleep(max(0.0, (expires_at - datetime.now(UTC)).total_seconds()) + 0.1)
            with pytest.raises(RegistrationAuthorityDenied):
                await asyncio.wait_for(task, timeout=10)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert await _dataset_count(case) == 0


@pytest.mark.asyncio
async def test_completed_result_replays_after_original_expiry(monkeypatch):
    registration = _registration(dataset_key="synthetic_completed_expiry")
    async with _authority_case() as case:
        authority_id, token, token_sha256, expires_at = _intent(seconds=1)
        await _mint(case, authority_id, registration, expires_at, token_sha256)
        completed = await _register(case, authority_id, registration, token)
        assert completed.result is not None
        await asyncio.sleep(max(0.0, (expires_at - datetime.now(UTC)).total_seconds()) + 0.1)

        import process.custom_import.registration_authority as authority_store

        async def unexpected_graph_call(*_args, **_kwargs):
            raise AssertionError("completed replay must not invoke graph registration")

        monkeypatch.setattr(authority_store, "register_snowflake_source_binding", unexpected_graph_call)
        replay = await _register(case, authority_id, registration, token)
        assert replay == completed
        assert await _dataset_count(case) == 1


@pytest.mark.asyncio
async def test_expiry_after_dataset_wait_rolls_back_graph_and_result():
    registration = _registration(dataset_key="synthetic_waited_expiry")
    async with _authority_case() as case:
        async with case.sessions() as session, session.begin():
            session.add(CustomImportDataset(dataset_key=registration["dataset_key"]))
            await session.flush()
        authority_id, token, token_sha256, expires_at = _intent(seconds=1)
        await _mint(case, authority_id, registration, expires_at, token_sha256)
        task = None
        try:
            async with case.sessions() as dataset_holder, dataset_holder.begin():
                await dataset_holder.execute(
                    select(CustomImportDataset)
                    .where(CustomImportDataset.dataset_key == registration["dataset_key"])
                    .with_for_update()
                )
                task = asyncio.create_task(_register(case, authority_id, registration, token))
                await _wait_for_authority_lock(case, authority_id)
                await asyncio.sleep(max(0.0, (expires_at - datetime.now(UTC)).total_seconds()) + 0.1)
            with pytest.raises(RegistrationAuthorityDenied):
                await asyncio.wait_for(task, timeout=10)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        async with case.sessions() as session:
            state = await get_registration_authority(session, authority_id)
            assert state is not None and state.result is None
            assert await _definition_graph_counts(session) == (1, 0, 0, 0, 0, 0, 0, 0, 0)
            assert await session.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision)) == 0
        assert await _dataset_count(case) == 1


@pytest.mark.asyncio
async def test_committed_revoke_wins_against_waiting_registration():
    registration = _registration(dataset_key="synthetic_revoked_first")
    authority_id, token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        await _mint(case, authority_id, registration, expires_at, token_sha256)
        task = None
        try:
            async with case.sessions() as holder, holder.begin():
                revoked = await revoke_registration_authority(holder, authority_id)
                assert revoked.revoked_at is not None
                task = asyncio.create_task(_register(case, authority_id, registration, token))
                await asyncio.sleep(0.05)
                assert not task.done()
            with pytest.raises(RegistrationAuthorityDenied):
                await asyncio.wait_for(task, timeout=10)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert await _dataset_count(case) == 0


@pytest.mark.asyncio
async def test_refused_result_write_rolls_back_graph_even_if_caller_catches():
    registration = _registration(dataset_key="synthetic_refused")
    authority_id, token, token_sha256, expires_at = _intent()
    async with _authority_case() as case:
        await _mint(case, authority_id, registration, expires_at, token_sha256)
        schema = case.schema_name
        async with case.engine.begin() as connection:
            await connection.execute(
                text(f"""
                CREATE FUNCTION "{schema}".skip_authority_result() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                    IF NEW.result_receipt IS NOT NULL THEN RETURN NULL; END IF;
                    RETURN NEW;
                END $$
            """)
            )
            await connection.execute(
                text(f"""
                CREATE TRIGGER skip_authority_result BEFORE UPDATE
                ON "{schema}".custom_import_registration_authority
                FOR EACH ROW EXECUTE FUNCTION "{schema}".skip_authority_result()
            """)
            )
        async with case.sessions() as session, session.begin():
            with pytest.raises(RegistrationAuthorityDenied):
                await register_with_authority(
                    session, authority_id=authority_id, registration=registration, token=token
                )
        assert await _dataset_count(case) == 0
        async with case.sessions() as session:
            state = await get_registration_authority(session, authority_id)
            assert state is not None and state.result is None
            assert await _definition_graph_counts(session) == (0,) * 9
            assert await session.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision)) == 0
