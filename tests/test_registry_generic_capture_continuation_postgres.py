# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine Reader/Writer capture continuation, exact replay and transaction rollback."""

import asyncio
import hashlib
import json
from dataclasses import replace

import pytest

from process.network_fhir_retained_capture_contract import RetainedFHIRCaptureError
from process.registry_generic_accepted_capture import (
    generic_fhir_capture_id,
    read_current_generic_fhir_head,
    read_current_generic_fhir_input,
)
from process.registry_generic_capture_continuation import (
    RegistryGenericCaptureTarget,
    capture_committed_generic_fhir_source,
)
from tests.generic_fhir_capture_postgres_support import (
    _store_fixture_admission,
)
from tests.generic_fhir_capture_postgres_support import (
    legacy_capture_db as legacy_capture_db,
)

pytestmark = pytest.mark.asyncio


async def _continue(fixture):
    reader, writer = await fixture.connect("reader"), await fixture.connect("publisher")
    copy_target = RegistryGenericCaptureTarget(
        fixture.role_by_kind["owner"],
        (fixture.role_by_kind["reader"],),
        fixture.origin.preparation_schema,
    )
    async with (
        reader.transaction(isolation="repeatable_read", readonly=True),
        writer.transaction(isolation="repeatable_read"),
    ):
        origin = await read_current_generic_fhir_input(
            reader,
            schema_name=fixture.schema,
            source_id=fixture.origin.source_id,
            endpoint_id=fixture.origin.endpoint_id,
            preparation_schema=fixture.origin.preparation_schema,
        )
        assert origin == fixture.origin
        identity = generic_fhir_capture_id(origin)
        namespace = "registry_fhir_capture_" + identity.hex
        if namespace not in fixture.captures:
            fixture.captures.append(namespace)
        capture = await capture_committed_generic_fhir_source(
            reader,
            writer,
            schema_name=fixture.schema,
            source_id=origin.source_id,
            endpoint_id=origin.endpoint_id,
            target=copy_target,
            expected_head=await read_current_generic_fhir_head(
                reader,
                schema_name=fixture.schema,
                source_id=origin.source_id,
                endpoint_id=origin.endpoint_id,
            ),
        )
        assert capture.capture_id == str(identity)
        return capture


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_native_capture_current_lookup_and_exact_postcommit_replay(legacy_capture_db, monkeypatch):
    from process import registry_generic_capture_continuation as continuation

    fixture = legacy_capture_db
    first = await _continue(fixture)

    async def no_copy(*args, **kwargs):
        raise AssertionError("replay attempted another native capture")

    monkeypatch.setattr(continuation, "capture_retained_fhir_source", no_copy)
    assert await _continue(fixture) == first
    assert await fixture.admin.fetchval("SELECT count(*) FROM pg_namespace WHERE oid=$1", first.schema_oid) == 1


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
@pytest.mark.parametrize("ensure_ascii", [False, True])
async def test_native_continuation_captures_large_admitted_semantic_metadata(legacy_capture_db, ensure_ascii):
    fixture = legacy_capture_db
    metadata = await fixture.admin.fetchval(
        f'SELECT publication_metadata_json::text FROM "{fixture.schema}".provider_directory_endpoint_dataset'
    )
    document = json.loads(metadata)
    document["native_copy_note"] = "é雪🚀"
    await _store_fixture_admission(fixture.admin, fixture.schema, document)
    metadata = json.dumps(document, ensure_ascii=ensure_ascii)
    metadata += " " * (72 * 1024**2 - len(metadata.encode()))
    await fixture.admin.execute(
        f'UPDATE "{fixture.schema}".provider_directory_endpoint_dataset SET publication_metadata_json=$1::json',
        metadata,
    )
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        origin = await read_current_generic_fhir_input(
            reader,
            schema_name=fixture.schema,
            source_id=fixture.origin.source_id,
            endpoint_id=fixture.origin.endpoint_id,
            preparation_schema=fixture.origin.preparation_schema,
        )
    assert (
        replace(
            origin,
            metadata_bytes=fixture.origin.metadata_bytes,
            metadata_text_sha256=fixture.origin.metadata_text_sha256,
            admission_metadata_sha256=fixture.origin.admission_metadata_sha256,
        )
        == fixture.origin
    )
    assert origin.metadata_text_sha256 == hashlib.sha256(metadata.encode()).hexdigest()
    fixture.origin = origin
    capture = await _continue(fixture)
    assert capture.origin.metadata_bytes == 72 * 1024**2
    assert capture.origin.metadata_text_sha256 == fixture.origin.metadata_text_sha256
    copied = await fixture.admin.fetchrow(
        f"SELECT octet_length(publication_metadata_json::text) AS metadata_bytes,"
        "encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS metadata_sha256,"
        "publication_metadata_sha256,publication_metadata_json->>'native_copy_note' AS note "
        f'FROM "{capture.schema_name}".provider_directory_endpoint_dataset'
    )
    assert tuple(copied.values()) == (
        origin.metadata_bytes,
        origin.metadata_text_sha256,
        origin.admission_metadata_sha256,
        "é雪🚀",
    )
    assert await _continue(fixture) == capture


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_native_existing_locator_tamper_is_refused(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _continue(fixture)
    await fixture.admin.execute(f"COMMENT ON SCHEMA \"{capture.schema_name}\" IS 'changed-capture-comment'")
    with pytest.raises((RetainedFHIRCaptureError, RuntimeError)):
        await _continue(fixture)


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
async def test_native_unadmitted_or_isolated_current_head_cannot_become_successor(legacy_capture_db):
    fixture = legacy_capture_db
    if fixture.origin.admission_profile != "legacy-descriptor-refusal-v1":
        await fixture.admin.execute(
            f'UPDATE "{fixture.schema}".provider_directory_endpoint_dataset SET candidate_storage_json=$1::jsonb',
            "{}",
        )
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises((RetainedFHIRCaptureError, TypeError)):
            await read_current_generic_fhir_head(
                reader,
                schema_name=fixture.schema,
                source_id=fixture.origin.source_id,
                endpoint_id=fixture.origin.endpoint_id,
            )


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_native_capture_rollback_removes_only_its_exact_new_namespace(legacy_capture_db, failure):
    fixture = legacy_capture_db
    reader, writer = await fixture.connect("reader"), await fixture.connect("publisher")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        head = await read_current_generic_fhir_head(
            reader,
            schema_name=fixture.schema,
            source_id=fixture.origin.source_id,
            endpoint_id=fixture.origin.endpoint_id,
        )
        namespace = "registry_fhir_capture_" + generic_fhir_capture_id(head).hex
        fixture.captures.append(namespace)
        with pytest.raises(failure, match="rollback-after-capture"):
            async with writer.transaction(isolation="repeatable_read"):
                await capture_committed_generic_fhir_source(
                    reader,
                    writer,
                    schema_name=fixture.schema,
                    source_id=head.source_id,
                    endpoint_id=head.endpoint_id,
                    expected_head=head,
                    target=RegistryGenericCaptureTarget(
                        fixture.role_by_kind["owner"],
                        (fixture.role_by_kind["reader"],),
                        fixture.origin.preparation_schema,
                    ),
                )
                raise failure("rollback-after-capture")
    assert await fixture.admin.fetchval("SELECT to_regnamespace($1)", namespace) is None
