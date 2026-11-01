# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native Source Publisher ingress, closed Core custody and rollback, without Reader impersonation."""

import asyncio
import hashlib
import json
from contextlib import AsyncExitStack
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from traceback import walk_tb
from uuid import uuid4

import asyncpg
import pytest

from process.network_fhir_retained_capture import require_retained_fhir_capture
from tests.source_witness_capture_postgres_support import (
    assert_independent_derived_rows,
    capture_arguments,
    copy_first_page,
    source_modules,
    source_scope,
    source_witness_case,
)

pytestmark = pytest.mark.asyncio


async def _capture(case, module, arguments):
    from process.network_fhir_source_witness_contract import SourceWitnessCaptureAnchor, canonical_timestamp

    async with case.writer.transaction(isolation="repeatable_read"):
        capture_id = str(arguments["capture_id"])
        anchor = SourceWitnessCaptureAnchor(
            capture_id,
            "registry-source-witness:" + capture_id,
            canonical_timestamp(await case.writer.fetchval("SELECT transaction_timestamp()")),
        )
        case.anchors_by_id[capture_id] = anchor
        return await module.capture_source_fhir_witness(
            case.source_export, case.writer, **{**arguments, "expected_anchor": anchor}
        )


async def _capture_after_genuine_source_exit(case, module, arguments, monkeypatch):
    original_copy, original_seal = module._copy_resources, module._seal
    observed_by_field = {}

    async def observe_eof(writer, schema, origin, export, release_id, observed_at):
        result = await original_copy(writer, schema, origin, export, release_id, observed_at)
        scope = await source_scope(export)
        assert not scope.streams_by_token
        observed_by_field["scope"] = scope
        return result

    async def require_source_exit(writer, origin, identity, schema, capture_id, owner_role, runtime_roles, observed_at):
        assert observed_by_field["scope"].is_active is False
        observed_by_field["sealed_after_source_exit"] = True
        return await original_seal(writer, origin, identity, schema, capture_id, owner_role, runtime_roles, observed_at)

    with monkeypatch.context() as patch:
        patch.setattr(module, "_copy_resources", observe_eof)
        patch.setattr(module, "_seal", require_source_exit)
        capture = await _capture(case, module, arguments)
    assert observed_by_field["sealed_after_source_exit"]
    return capture


async def _assert_rolled_back(case, arguments):
    assert not case.writer.is_in_transaction()
    async with case.writer.transaction():
        assert (
            await case.writer.fetchval(
                "SELECT to_regnamespace($1)", "registry_fhir_witness_" + arguments["capture_id"].hex
            )
            is None
        )


def _assert_original_source_refusal(error):
    catalog, producer = source_modules()
    frames = []
    trace = error.__traceback__
    while trace is not None:
        frames.append(trace.tb_frame.f_code.co_filename)
        trace = trace.tb_next
    assert any(path in {producer.__file__, catalog.__file__} for path in frames)


async def _assert_native_read_custody(case, module, capture):
    from process.network_fhir_source_witness_contract import SourceWitnessContractError

    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        assert await module.require_source_fhir_witness_capture(case.reader, capture) == capture
        read = await module.pin_source_fhir_witness_read(case.reader, capture)
        await read.verify(case.reader, capture)
        rows = await assert_independent_derived_rows(case.reader, capture)
        with pytest.raises(SourceWitnessContractError):
            await asyncio.create_task(read.verify(case.reader, capture))
    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(SourceWitnessContractError):
            await read.verify(case.reader, capture)
    return rows


async def _assert_source_guard_refusals(case, module):
    from process.network_fhir_source_witness_contract import SourceWitnessContractError

    for field in ("native_catalog_sha256", "dataset_sha256"):
        arguments = capture_arguments(case)
        original = getattr(case.origin, field)
        arguments["origin"] = replace(case.origin, **{field: ("a" if original[0] != "a" else "b") * 64})
        with pytest.raises(SourceWitnessContractError, match="^source_fhir_witness_contract_invalid$") as refused:
            await _capture(case, module, arguments)
        identity_frames = [
            frame
            for frame, _ in walk_tb(refused.value.__traceback__)
            if frame.f_code is module._export_identity.__code__
        ]
        assert len(identity_frames) == 1
        identity_by_name = identity_frames[0].f_locals
        assert identity_by_name["export"]["contract"] == module.CONTRACT
        assert identity_by_name["export"]["origin"] == case.origin
        assert identity_by_name["reviewed"] == module.source_origin(arguments["origin"])
        assert identity_by_name["reviewed"] != module.source_origin(case.origin)
        await _assert_rolled_back(case, arguments)


async def _membership_census(admin):
    return [
        dict(row)
        for row in await admin.fetch(
            "SELECT oid,roleid,member,grantor,admin_option,inherit_option,set_option FROM pg_auth_members ORDER BY oid"
        )
    ]


async def _publisher_activity(case):
    return [
        dict(row)
        for row in await case.fixture.admin.fetch(
            "SELECT pid,backend_start,xact_start,query_start,state,backend_xid,backend_xmin,query "
            "FROM pg_stat_activity WHERE usename=$1 ORDER BY pid",
            case.fixture.component.publisher,
        )
    ]


async def _assert_inherited_source_role_refuses(case, module, source_role):
    """Inject only an invalid graph in original task-owned roles, then restore it before any positive capture."""
    from process.network_fhir_source_witness_contract import SourceWitnessContractError

    admin, owner = case.fixture.admin, case.fixture.role_by_kind["owner"]
    before_memberships = await _membership_census(admin)
    assert await admin.fetchval(
        "SELECT NOT EXISTS(SELECT FROM pg_auth_members membership "
        "JOIN pg_roles parent ON parent.oid=membership.roleid "
        "JOIN pg_roles child ON child.oid=membership.member "
        "WHERE parent.rolname=$1::name AND child.rolname=$2::name)",
        source_role,
        owner,
    )
    source_activity = await _publisher_activity(case)
    arguments = capture_arguments(case)
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(admin.execute, f'REVOKE "{source_role}" FROM "{owner}"')
        await admin.execute(f'GRANT "{source_role}" TO "{owner}" WITH INHERIT TRUE, SET FALSE')
        assert await admin.fetchval("SELECT pg_has_role($1::name,$2::name,'USAGE')", owner, source_role)
        assert not await admin.fetchval("SELECT pg_has_role($1::name,$2::name,'SET')", owner, source_role)
        with pytest.raises(SourceWitnessContractError):
            await _capture(case, module, arguments)
        assert await _publisher_activity(case) == source_activity
    assert await _membership_census(admin) == before_memberships
    await _assert_rolled_back(case, arguments)


async def _assert_incomplete_source_rolls_back(case, module, monkeypatch):
    async def incomplete(writer, schema, origin, export, release_id, observed_at):
        assert origin.dataset_id == case.origin.dataset_id
        await copy_first_page(writer, schema, export)

    arguments = capture_arguments(case)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_copy_resources", incomplete)
        with pytest.raises(ValueError, match="catalog_source_stream_incomplete") as refused:
            await _capture(case, module, arguments)
    _assert_original_source_refusal(refused.value)
    await _assert_rolled_back(case, arguments)


async def _assert_cancelled_copy_rolls_back(case, module, monkeypatch):
    copied, blocked = asyncio.Event(), asyncio.Event()

    async def interrupted(writer, schema, origin, export, release_id, observed_at):
        await copy_first_page(writer, schema, export)
        copied.set()
        await blocked.wait()

    arguments = capture_arguments(case)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_copy_resources", interrupted)
        task = asyncio.create_task(_capture(case, module, arguments))
        try:
            await asyncio.wait_for(copied.wait(), 20)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    await _assert_rolled_back(case, arguments)


async def _assert_source_exit_failure_rolls_back(case, module, monkeypatch):
    original = module._copy_resources
    observed_by_field = {}

    async def terminate_after_eof(writer, schema, origin, export, release_id, observed_at):
        result = await original(writer, schema, origin, export, release_id, observed_at)
        scope = await source_scope(export)
        assert not scope.streams_by_token
        observed_by_field["pid"] = await scope.connection.fetchval("SELECT pg_backend_pid()")
        await scope.connection.close()
        observed_by_field["closed_after_eof"] = True
        return result

    arguments = capture_arguments(case)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_copy_resources", terminate_after_eof)
        with pytest.raises(asyncpg.InterfaceError, match="connection is closed") as refused:
            await _capture(case, module, arguments)
    assert observed_by_field["closed_after_eof"] and observed_by_field["pid"] > 0
    _assert_original_source_refusal(refused.value)
    await _assert_rolled_back(case, arguments)


async def _assert_changed_native_owner_refuses(case, module, capture):
    from process.network_fhir_source_witness_contract import SourceWitnessContractError

    admin = case.fixture.admin
    await admin.execute(f'ALTER SCHEMA "{capture.schema_name}" OWNER TO "{case.fixture.component.owner}"')
    try:
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(SourceWitnessContractError):
                await module.require_source_fhir_witness_capture(case.reader, capture)
    finally:
        await admin.execute(f'ALTER SCHEMA "{capture.schema_name}" OWNER TO "{capture.owner_role}"')
    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        assert await module.require_source_fhir_witness_capture(case.reader, capture) == capture


class _RollbackTamper(Exception):
    """Roll back the task-owned invalidity injection, including its forged comment."""


async def _assert_independent_derived_tamper_refuses(case, module, capture):
    from process.network_fhir_source_witness_contract import (
        COMMENT_PREFIX,
        SourceWitnessContractError,
        canonical,
        capture_digest,
    )

    with pytest.raises(_RollbackTamper):
        async with case.writer.transaction(isolation="repeatable_read"):
            await case.writer.execute(f'SET LOCAL ROLE "{capture.owner_role}"')
            changed = await case.writer.execute(
                f'UPDATE "{capture.schema_name}".provider_directory_entity_source_binding SET site_id=$1 '
                "WHERE resource_type='Location' AND resource_id='office-one'",
                uuid4(),
            )
            assert changed == "UPDATE 1"
            forged = replace(capture, content_sha256=await module._capture_content(case.writer, capture.schema_name))
            forged = replace(forged, capture_sha256=capture_digest(forged))
            comment = (COMMENT_PREFIX + canonical(forged.as_dict())).replace("'", "''")
            await case.writer.execute(f"COMMENT ON SCHEMA \"{capture.schema_name}\" IS '{comment}'")
            with pytest.raises(SourceWitnessContractError) as refused:
                await module.require_source_fhir_witness_capture(case.writer, forged)
            assert "_verify_derived" in _traceback_functions(refused.value)
            raise _RollbackTamper
    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        assert await module.require_source_fhir_witness_capture(case.reader, capture) == capture


def _traceback_functions(error):
    names = []
    trace = error.__traceback__
    while trace is not None:
        names.append(trace.tb_frame.f_code.co_name)
        trace = trace.tb_next
    return names


async def _assert_explicit_native_limits_reuse_codec(case, module, monkeypatch):
    """Force Core splitting and one bounded reference fallback while keeping real native encoding elsewhere."""
    retained = module.retained
    original_prepare, original_encoder = retained._prepare_native_resources, retained._native_payload_encoder
    prepare_sizes, native_sizes, encoded_sizes = [], [], []

    def observe_preparation(rows, origin, release_id, observed_at, derive):
        prepare_sizes.append(len(rows))
        return original_prepare(rows, origin, release_id, observed_at, derive)

    def explicit_limits(encoded):
        rows = json.loads(encoded)["rows"]
        encoded_sizes.append(len(rows))
        if len(rows) > 1 or rows[0]["resource_type"] == "Location" and rows[0]["resource_id"] == "office-one":
            raise retained._CopyBatchTooLarge()
        native_sizes.append(len(rows))
        return original_encoder(encoded)

    with monkeypatch.context() as patch:
        patch.setattr(retained, "_prepare_native_resources", observe_preparation)
        patch.setattr(retained, "_native_payload_encoder", explicit_limits)
        capture = await _capture(case, module, capture_arguments(case))
    assert prepare_sizes == encoded_sizes and max(prepare_sizes) > 1 and 1 in prepare_sizes
    assert native_sizes and all(size == 1 for size in native_sizes)
    return capture


async def test_genuine_source_witness_capture_closes_and_replays_with_original_authority(monkeypatch, record_property):
    async with source_witness_case(monkeypatch) as case:
        from process import network_fhir_source_witness_capture as capture_module
        from process.network_fhir_source_witness_contract import CAPTURE_CONTRACT, TABLES, validate_capture

        old = case.old_capture.as_dict()
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            assert await require_retained_fhir_capture(case.reader, case.old_capture) == case.old_capture
        for source_role in (case.fixture.component.owner, case.fixture.component.publisher):
            await _assert_inherited_source_role_refuses(case, capture_module, source_role)
        arguments = capture_arguments(case)
        capture = await _capture_after_genuine_source_exit(case, capture_module, arguments, monkeypatch)
        assert capture.contract == CAPTURE_CONTRACT
        assert capture.origin.dataset_id == case.origin.dataset_id
        assert asdict(capture.origin) == asdict(case.origin)
        assert validate_capture(capture.as_dict()) == capture
        assert {name for name, _ in capture.relation_oids} == set(TABLES)
        assert "provider_directory_endpoint_dataset" not in TABLES
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await case.reader.fetchval(
                    "SELECT to_regclass($1)", capture.schema_name + ".provider_directory_endpoint_dataset"
                )
                is None
            )
        resource_count = await _assert_native_read_custody(case, capture_module, capture)
        await _assert_source_guard_refusals(case, capture_module)
        await _assert_incomplete_source_rolls_back(case, capture_module, monkeypatch)
        await _assert_cancelled_copy_rolls_back(case, capture_module, monkeypatch)
        await _assert_source_exit_failure_rolls_back(case, capture_module, monkeypatch)
        await _assert_changed_native_owner_refuses(case, capture_module, capture)
        await _assert_independent_derived_tamper_refuses(case, capture_module, capture)
        # A genuinely closed Source context/pool is reusable after every refused candidate.
        following = await _assert_explicit_native_limits_reuse_codec(case, capture_module, monkeypatch)
        assert await _assert_native_read_custody(case, capture_module, following) == resource_count
        assert case.old_capture.as_dict() == old
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            assert await require_retained_fhir_capture(case.reader, case.old_capture) == case.old_capture
        record_property("component", "genuine Source Publisher to independent closed Core witness custody")
        record_property("rows", str(resource_count))
        record_property(
            "origin", "all25 actual Source metadata/header facts; Reader capture retained only as regression"
        )
        record_property(
            "invalid_graph",
            "exact ephemeral SourceOwner/Publisher->captureOwner INHERITtrue/SETfalse injections; refused before Source SQL; revoked/full membership census restored",
        )
        record_property(
            "limits",
            "LOCAL component only; no recipes, approved mappings, provider APIs, refresh, runtime or representative performance",
        )


async def _fresh_source_verify(case, module, capture, anchor, *, reviewed_origin=None, connection=None):
    arguments_by_name = {
        "source_profile": case.source_profile,
        "reviewed_origin": case.origin if reviewed_origin is None else reviewed_origin,
        "capture": capture,
        "expected_anchor": anchor,
    }
    if connection is not None:
        return await module.verify_source_fhir_witness_capture(case.source_export, connection, **arguments_by_name)
    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        return await module.verify_source_fhir_witness_capture(case.source_export, case.reader, **arguments_by_name)


async def _corrected_capture(case, module, monkeypatch):
    original_copy, original_finish = module._copy_resources, module._finish_source_replay
    original_identity, original_seal = module._export_identity, module._seal
    observed_by_field = {"exports": [], "eof_scopes": []}

    def observe_identity(export, reviewed, expected_identity=None):
        identity = original_identity(export, reviewed, expected_identity)
        observed_by_field["exports"].append(export)
        return identity

    async def observe_first_eof(writer, schema, origin, export, release_id, observed_at):
        result = await original_copy(writer, schema, origin, export, release_id, observed_at)
        observed_by_field["eof_scopes"].append(await source_scope(export))
        return result

    async def observe_second_eof(connection, replay):
        result = await original_finish(connection, replay)
        observed_by_field["eof_scopes"].append(await source_scope(observed_by_field["exports"][-1]))
        return result

    async def seal_after_both_exits(
        writer, origin, identity, schema, capture_id, owner_role, runtime_roles, observed_at
    ):
        assert len(observed_by_field["eof_scopes"]) == 2
        assert all(not scope.is_active and not scope.streams_by_token for scope in observed_by_field["eof_scopes"])
        return await original_seal(writer, origin, identity, schema, capture_id, owner_role, runtime_roles, observed_at)

    arguments = capture_arguments(case)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_export_identity", observe_identity)
        patch.setattr(module, "_copy_resources", observe_first_eof)
        patch.setattr(module, "_finish_source_replay", observe_second_eof)
        patch.setattr(module, "_seal", seal_after_both_exits)
        capture = await _capture(case, module, arguments)
    assert len(observed_by_field["exports"]) == 2
    anchor = case.anchors_by_id[str(arguments["capture_id"])]
    assert await _fresh_source_verify(case, module, capture, anchor) == capture
    return capture, anchor


async def _rewrite_local_receipt(case, module, capture):
    from process.network_fhir_source_witness_contract import COMMENT_PREFIX, canonical, capture_digest

    schema_oid, relation_oids, catalog_sha256 = await module._catalog(
        case.writer, capture.schema_name, capture.owner_role, capture.runtime_roles
    )
    assert schema_oid == capture.schema_oid and relation_oids == capture.relation_oids
    forged = replace(
        capture,
        catalog_sha256=catalog_sha256,
        content_sha256=await module._capture_content(case.writer, capture.schema_name),
    )
    forged = replace(forged, capture_sha256=capture_digest(forged))
    comment = (COMMENT_PREFIX + canonical(forged.as_dict())).replace("'", "''")
    await case.writer.execute(f"COMMENT ON SCHEMA \"{capture.schema_name}\" IS '{comment}'")
    return forged


async def _forge_resources_and_derived(case, module, capture):
    """Change a candidate resource, recompute its payload hash and regenerate every local derived table."""
    from process.provider_directory_resource_hash import (
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        resource_payload_sha256_for_contract,
    )

    payload_by_field = json.loads(
        await case.writer.fetchval(
            f'SELECT payload_json::text FROM "{capture.schema_name}".provider_directory_dataset_resource '
            "WHERE resource_type='Location' AND resource_id='office-one'"
        )
    )
    payload_by_field["first_line"] = "456 Different Example St"
    payload_hash = resource_payload_sha256_for_contract(
        payload_by_field, SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT, resource_type="Location"
    )
    await case.writer.execute(
        f'UPDATE "{capture.schema_name}".provider_directory_dataset_resource '
        "SET payload_json=$1::jsonb,payload_hash=$2 WHERE resource_type='Location' AND resource_id='office-one'",
        json.dumps(payload_by_field),
        payload_hash,
    )
    resources = await case.writer.fetch(
        f'SELECT * FROM "{capture.schema_name}".provider_directory_dataset_resource '
        'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C"'
    )
    await case.writer.execute(
        "TRUNCATE " + ",".join(f'"{capture.schema_name}"."{table}"' for table in module.TABLES[1:])
    )
    await module.retained._derived(
        case.writer,
        capture.schema_name,
        capture.origin,
        capture.release_id,
        resources,
        datetime.fromisoformat(capture.observed_at),
    )
    return resources


def _forged_source_roots(origin, resources):
    hashes_by_family = {kind: hashlib.sha256() for kind, _ in origin.resource_counts}
    counts_by_family = dict.fromkeys(hashes_by_family, 0)
    root = hashlib.sha256()
    from process.network_fhir_retained_capture import _fold_rows

    _fold_rows(resources, origin, hashes_by_family, counts_by_family, root, 0)
    return {
        "dataset_sha256": root.hexdigest(),
        "resource_counts": tuple(sorted(counts_by_family.items())),
        "resource_hashes": tuple((kind, value.hexdigest()) for kind, value in sorted(hashes_by_family.items())),
        "descriptor_counts": tuple(sorted(counts_by_family.items())),
    }


async def _assert_resource_forgery_refuses(case, module, capture, anchor, *, forge_source_declaration):
    from process.network_fhir_source_witness_contract import SourceWitnessContractError

    with pytest.raises(_RollbackTamper):
        async with case.writer.transaction(isolation="repeatable_read"):
            await case.writer.execute(f'SET LOCAL ROLE "{capture.owner_role}"')
            resources = await _forge_resources_and_derived(case, module, capture)
            reviewed_origin = case.origin
            if forge_source_declaration:
                roots_by_field = _forged_source_roots(case.origin, resources)
                capture = replace(capture, origin=replace(capture.origin, **roots_by_field))
                reviewed_origin = replace(case.origin, **roots_by_field)
            forged = await _rewrite_local_receipt(case, module, capture)
            with pytest.raises(SourceWitnessContractError) as refused:
                await _fresh_source_verify(
                    case, module, forged, anchor, reviewed_origin=reviewed_origin, connection=case.writer
                )
            assert "_verify_source_rows" in _traceback_functions(refused.value)
            if not forge_source_declaration:
                assert "_compare_source_page" in _traceback_functions(refused.value)
            raise _RollbackTamper


async def _assert_local_row_damage_refuses(case, module, capture, anchor, damage):
    from process.network_fhir_source_witness_contract import SourceWitnessContractError

    schema = capture.schema_name
    with pytest.raises(_RollbackTamper):
        async with case.writer.transaction(isolation="repeatable_read"):
            await case.writer.execute(f'SET LOCAL ROLE "{capture.owner_role}"')
            if damage == "derived":
                await case.writer.execute(
                    f'UPDATE "{schema}".provider_directory_entity_source_binding SET site_id=$1 '
                    "WHERE resource_type='Location' AND resource_id='office-one'",
                    uuid4(),
                )
            elif damage == "missing":
                await case.writer.execute(
                    f'DELETE FROM "{schema}".provider_directory_dataset_resource '
                    "WHERE resource_type='Location' AND resource_id='office-one'"
                )
            else:
                assert damage == "extra"
                await case.writer.execute(
                    f'INSERT INTO "{schema}".provider_directory_dataset_resource '
                    f"SELECT dataset_id,resource_type,'extra-local-row',payload_hash,payload_json,acquired_resource_sha256 "
                    f"FROM \"{schema}\".provider_directory_dataset_resource WHERE resource_type='Location' LIMIT 1"
                )
            forged = await _rewrite_local_receipt(case, module, capture)
            with pytest.raises(SourceWitnessContractError) as refused:
                await _fresh_source_verify(case, module, forged, anchor, connection=case.writer)
            assert "_verify_source_rows" in _traceback_functions(refused.value)
            raise _RollbackTamper


async def _assert_independent_anchors_refuse(case, module, capture, anchor):
    from process.network_fhir_source_witness_contract import (
        SourceWitnessCaptureAnchor,
        SourceWitnessContractError,
        canonical_timestamp,
    )

    alternate_id = str(uuid4())
    wrong_anchors = (
        SourceWitnessCaptureAnchor(alternate_id, "registry-source-witness:" + alternate_id, anchor.observed_at),
        replace(
            anchor,
            observed_at=canonical_timestamp(datetime.fromisoformat(anchor.observed_at) + timedelta(microseconds=1)),
        ),
    )
    for wrong_anchor in wrong_anchors:
        with pytest.raises(SourceWitnessContractError):
            await _fresh_source_verify(case, module, capture, wrong_anchor)
    altered_review = replace(
        case.origin, metadata_text_sha256=("a" if case.origin.metadata_text_sha256[0] != "a" else "b") * 64
    )
    with pytest.raises(SourceWitnessContractError):
        await _fresh_source_verify(case, module, capture, anchor, reviewed_origin=altered_review)


async def _assert_second_stream_incomplete_rolls_back(case, module, monkeypatch):
    from contextlib import aclosing

    async def incomplete(
        source_export, connection, *, source_profile, reviewed_origin, expected_anchor, expected_identity
    ):
        async with source_export(
            source_profile=source_profile,
            source_id=reviewed_origin.source_id,
            endpoint_id=reviewed_origin.endpoint_id,
            dataset_id=reviewed_origin.dataset_id,
        ) as export:
            async with aclosing(export["resources"]()) as pages:
                assert await anext(pages)
            return expected_identity

    arguments = capture_arguments(case)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_verify_source_rows", incomplete)
        with pytest.raises(ValueError, match="catalog_source_stream_incomplete") as refused:
            await _capture(case, module, arguments)
    _assert_original_source_refusal(refused.value)
    await _assert_rolled_back(case, arguments)


async def _assert_second_exit_failure_rolls_back(case, module, monkeypatch):
    original_identity, original_finish = module._export_identity, module._finish_source_replay
    observed_by_field = {}

    def observe_export(export, reviewed, expected_identity=None):
        identity = original_identity(export, reviewed, expected_identity)
        if expected_identity is not None:
            observed_by_field["second_export"] = export
        return identity

    async def terminate_after_second_eof(connection, replay):
        result = await original_finish(connection, replay)
        scope = await source_scope(observed_by_field["second_export"])
        assert not scope.streams_by_token
        observed_by_field["closed_after_eof"] = True
        await scope.connection.close()
        return result

    arguments = capture_arguments(case)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_export_identity", observe_export)
        patch.setattr(module, "_finish_source_replay", terminate_after_second_eof)
        with pytest.raises(asyncpg.InterfaceError, match="connection is closed") as refused:
            await _capture(case, module, arguments)
    assert observed_by_field["closed_after_eof"]
    _assert_original_source_refusal(refused.value)
    await _assert_rolled_back(case, arguments)


async def _assert_second_replay_cancel_rolls_back(case, module, monkeypatch):
    compared, blocked = asyncio.Event(), asyncio.Event()
    original_compare = module._compare_source_page

    async def paused_second_page(connection, replay, resource_rows):
        await original_compare(connection, replay, resource_rows)
        compared.set()
        await blocked.wait()

    arguments = capture_arguments(case)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_compare_source_page", paused_second_page)
        task = asyncio.create_task(_capture(case, module, arguments))
        try:
            await asyncio.wait_for(compared.wait(), 20)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    await _assert_rolled_back(case, arguments)


async def test_fixed_source_constructor_capture_and_fresh_reader_replay_refuse_local_forgeries(
    monkeypatch, record_property
):
    async with source_witness_case(monkeypatch) as case:
        from process import network_fhir_source_witness_capture as capture_module

        assert asdict(case.constructed_origin) == asdict(case.origin)
        original_reader_wire = case.old_capture.as_dict()
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            assert await require_retained_fhir_capture(case.reader, case.old_capture) == case.old_capture
        capture, anchor = await _corrected_capture(case, capture_module, monkeypatch)
        for forge_source_declaration in (False, True):
            await _assert_resource_forgery_refuses(
                case, capture_module, capture, anchor, forge_source_declaration=forge_source_declaration
            )
        for damage in ("derived", "missing", "extra"):
            await _assert_local_row_damage_refuses(case, capture_module, capture, anchor, damage)
        await _assert_independent_anchors_refuse(case, capture_module, capture, anchor)
        await _assert_second_stream_incomplete_rolls_back(case, capture_module, monkeypatch)
        await _assert_second_exit_failure_rolls_back(case, capture_module, monkeypatch)
        await _assert_second_replay_cancel_rolls_back(case, capture_module, monkeypatch)
        assert await _fresh_source_verify(case, capture_module, capture, anchor) == capture
        assert case.old_capture.as_dict() == original_reader_wire
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            assert await require_retained_fhir_capture(case.reader, case.old_capture) == case.old_capture
        record_property(
            "component", "fixed Source constructor and independent fresh Source replay before sealed capture"
        )
        record_property("rows", str(sum(count for _, count in case.origin.resource_counts)))
        record_property(
            "root_anchor",
            "capture UUID/release/time selected before producer from Core transaction; never receipt-derived",
        )
        record_property(
            "limits",
            "LOCAL corrected M1 only; no Source changes/grants, M2/provider-serving/runtime or representative performance",
        )
