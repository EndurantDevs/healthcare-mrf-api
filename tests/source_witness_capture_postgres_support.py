# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original Source publication/runtime and independently reviewed witness inputs."""

import hashlib
import json
import os
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import asdict, fields
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import network_fhir_retained_capture as retained
from process.registry_generic_accepted_capture import _admitted_summary, _current_header
from tests import generic_source_successor_postgres_support as source

SOURCE_PRODUCER_SHA256 = "b6496fea2e8f695b09464de5cd1180c932dfdde9cccb2d5fd1a221c57128a211"
_RESOURCE_FIELDS = (
    "dataset_id",
    "resource_type",
    "resource_id",
    "payload_hash",
    "payload_json",
    "acquired_resource_sha256",
)


def source_modules():
    """Source is an explicit cross-repository native prerequisite, never an implicit import path."""
    if os.getenv("HLTHPRT_SOURCE_WITNESS_TEST_NATIVE") != "1":
        pytest.skip("Source witness native component is not configured")
    from directory_membership_test_support import catalog, producer

    assert hashlib.sha256(Path(producer.__file__).read_bytes()).hexdigest() == SOURCE_PRODUCER_SHA256
    return catalog, producer


def configure_original_runtime(component, monkeypatch):
    """Select only the original fixture Publisher and Reader, preserving their roles and ACLs."""
    from directory_membership_test_support import configure_original_runtime as configure_source

    return configure_source(component, monkeypatch)


async def _observed_source_descriptor(runtime, profile, candidate, catalog):
    """Read the exact Source-issued metadata/native descriptor in its original snapshot."""
    descriptors = []
    async with catalog.open_catalog_source_export(runtime, source_profile=profile) as export:
        async for page in export["inventory"]():
            descriptors.extend(
                row.descriptor
                for row in page
                if row.descriptor is not None
                and row.dataset_id == candidate.dataset_id
                and row.endpoint_id == candidate.endpoint_id
            )
        assert len(descriptors) == 1
        descriptor, identity = descriptors[0], export["database_identity"]

    return descriptor, identity


async def _reviewed_source_publication(runtime, profile, candidate, catalog, descriptor, identity):
    """Read original publication facts under the genuine Publisher, without creating Reader proof."""
    async with catalog._catalog_source_export_session(runtime, source_profile=profile) as scope:
        assert await scope.connection.fetchval("SELECT session_user::text") == runtime.config.publisher_role
        header = await _current_header(scope.connection, profile.schema_name, source.SOURCE, candidate.endpoint_id)
        head, summary, source_summary = _admitted_summary(header, profile.schema_name, source.SOURCE)
        physical = await scope.connection.fetchrow(
            f"SELECT octet_length(publication_metadata_json::text)::bigint AS metadata_bytes,"
            "encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS metadata_sha256 "
            f'FROM "{profile.schema_name}".provider_directory_endpoint_dataset WHERE dataset_id=$1',
            candidate.dataset_id,
        )
        assert head.dataset_id == descriptor["coordinates"]["dataset_id"] == candidate.dataset_id
        assert head.endpoint_id == descriptor["endpoint_id"] == candidate.endpoint_id
        assert head.dataset_sha256 == descriptor["dataset_sha256"]
        assert header["header_oid"] == descriptor["header_table_oid"]
        assert scope.database_identity_by_field == identity

    return head, summary, source_summary, physical


async def reviewed_source_origin(runtime, profile, candidate, catalog, producer):
    """Derive all 25 inputs from actual Source observations; no Reader capture is consulted."""
    descriptor, identity = await _observed_source_descriptor(runtime, profile, candidate, catalog)
    head, summary, source_summary, physical = await _reviewed_source_publication(
        runtime, profile, candidate, catalog, descriptor, identity
    )
    counts = tuple(sorted(source_summary["resource_counts"].items()))
    declaration_by_field = {
        "schema_name": profile.schema_name,
        "source_id": descriptor["coordinates"]["source_id"],
        "endpoint_id": head.endpoint_id,
        "dataset_id": head.dataset_id,
        "dataset_sha256": head.dataset_sha256,
        "root_run_id": head.root_run_id,
        "header_oid": descriptor["header_table_oid"],
        "resource_parent_oid": descriptor["resource_table_oid"],
        "resource_leaf_oid": descriptor["resource_leaf_oids"][0],
        "metadata_bytes": physical["metadata_bytes"],
        "metadata_text_sha256": physical["metadata_sha256"],
        "summary_sha256": source_summary["summary_sha256"],
        "proof_sha256": head.proof_sha256,
        "resource_counts": counts,
        "resource_hashes": tuple(sorted(source_summary["resource_hashes"].items())),
        "descriptor_counts": counts,
        "admission_profile": retained.SEMANTIC_PROFILE,
        "admission_metadata_sha256": head.admission_metadata_sha256,
        "semantic_projection_as_of": head.semantic_projection_as_of,
        "unresolved_plan_network_count": summary["dataset_network_plan"]["unresolved_reference_count"],
        "source_uri": descriptor["source_uri"],
        "source_aliases": tuple(descriptor["source_aliases"]),
        "native_catalog_sha256": descriptor["native_catalog_sha256"],
        "database_oid": identity["database_oid"],
        "database_system_identifier": identity["system_identifier"],
    }
    assert len(declaration_by_field) == 25 and set(declaration_by_field) == {
        field.name for field in fields(producer.FHIRSourceMembershipInput)
    }
    return producer.FHIRSourceMembershipInput(**declaration_by_field)


async def constructed_source_origin(source_export, profile, reviewed_origin):
    """Compare the fixed-selection constructor with separate reviewed facts; this preview grants no EOF proof."""
    async with source_export(
        source_profile=profile,
        source_id=reviewed_origin.source_id,
        endpoint_id=reviewed_origin.endpoint_id,
        dataset_id=reviewed_origin.dataset_id,
    ) as export:
        actual = export["origin"]
        assert type(actual) is type(reviewed_origin)
        assert asdict(actual) == asdict(reviewed_origin)
        assert len(asdict(actual)) == 25
    return actual


@asynccontextmanager
async def source_witness_case(monkeypatch):
    """Use the existing owned native fixture and original capture Writer, without extra grants."""
    catalog, producer = source_modules()
    async with source.native_generic_source(monkeypatch) as fixture, AsyncExitStack() as cleanup:
        old_capture = await source.publish_edition(fixture, 1)
        profile = configure_original_runtime(fixture.component, monkeypatch)
        source_export = await cleanup.enter_async_context(producer.membership_source_export_runtime())
        runtime = source_export.args[0]
        assert runtime.config.publisher_role == fixture.component.publisher
        origin = await reviewed_source_origin(runtime, profile, fixture.candidates[-1], catalog, producer)
        constructed = await constructed_source_origin(source_export, profile, origin)
        connections_by_kind = {}
        for kind, role in (("writer", fixture.component.retained_publisher), ("reader", fixture.component.reader)):
            connection = await asyncpg.connect(
                fixture.component.admin_engine.url.set(drivername="postgresql", username=role).render_as_string(
                    hide_password=False
                )
            )
            cleanup.push_async_callback(connection.close)
            assert await connection.fetchval("SELECT session_user::text") == role
            connections_by_kind[kind] = connection
        case = SimpleNamespace(
            fixture=fixture,
            origin=origin,
            source_export=source_export,
            source_profile=profile,
            source_runtime=runtime,
            old_capture=old_capture,
            producer=producer,
            constructed_origin=constructed,
            anchors_by_id={},
            **connections_by_kind,
        )
        yield case
        assert fixture.commits == [candidate.dataset_id for candidate in fixture.candidates]
        assert fixture.source_writer_rejections == fixture.commits


def capture_arguments(case):
    """Register the exact candidate namespace before the application creates it."""
    from process.network_fhir_source_witness_contract import SCHEMA_PREFIX

    capture_id = uuid4()
    schema = SCHEMA_PREFIX + capture_id.hex
    case.fixture.namespaces.append(schema)
    return {
        "source_profile": case.source_profile,
        "origin": case.origin,
        "capture_id": capture_id,
        "owner_role": case.fixture.role_by_kind["owner"],
        "runtime_roles": (case.fixture.component.reader,),
    }


async def capture_retained_source_for_roles(case, runtime_roles):
    """Keep the original Reader-only capture unchanged while preparing genuine worker custody."""
    identity = uuid4()
    case.fixture.namespaces.append("registry_fhir_capture_" + identity.hex)
    async with (
        case.reader.transaction(isolation="repeatable_read", readonly=True),
        case.writer.transaction(isolation="repeatable_read"),
    ):
        return await retained.capture_retained_fhir_source(
            case.reader,
            case.writer,
            case.old_capture.origin,
            capture_id=identity,
            owner_role=case.fixture.role_by_kind["owner"],
            runtime_roles=runtime_roles,
            source_policy=case.fixture.component.policy,
        )


async def source_scope(export):
    """Find only the genuine Source closure to inject a native exit failure after database EOF."""
    from directory_membership_test_support import SourceExportScope

    matches = [
        cell.cell_contents
        for cell in export["resources"].__closure__ or ()
        if type(cell.cell_contents) is SourceExportScope
    ]
    assert len(matches) == 1
    await matches[0].verify()
    return matches[0]


async def copy_first_page(writer, schema, export):
    """Leave the genuine Source stream incomplete after one actual bounded COPY."""
    async with aclosing(export["resources"]()) as pages:
        page = await anext(pages)
        assert 0 < len(page) <= 1000
        records = [
            tuple(
                json.dumps(row[field], separators=(",", ":"))
                if field == "payload_json" and type(row[field]) is not str
                else row[field]
                for field in _RESOURCE_FIELDS
            )
            for row in page
        ]
        copied = await writer.copy_records_to_table(
            "provider_directory_dataset_resource", schema_name=schema, columns=_RESOURCE_FIELDS, records=records
        )
        assert copied == f"COPY {len(page)}"
        return len(page)


async def assert_independent_derived_rows(connection, capture):
    """Compare all retained derivations with the unchanged Python reference, including their timestamps."""
    from process.network_fhir_source_witness_contract import TABLES

    resources = [
        dict(row)
        for row in await connection.fetch(
            f'SELECT * FROM "{capture.schema_name}".provider_directory_dataset_resource '
            'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C"'
        )
    ]
    assert len(resources) == sum(count for _, count in capture.origin.resource_counts)
    expected_by_table = {table: [] for table in TABLES[1:]}
    for table, record in retained._derived_records(
        capture.origin, capture.release_id, resources, datetime.fromisoformat(capture.observed_at)
    ):
        expected_by_table[table].append(record)
    for table, rows in expected_by_table.items():
        observed = await connection.fetch(f'SELECT * FROM "{capture.schema_name}"."{table}"')
        columns = list(observed[0].keys()) if observed else []
        assert _canonical_derived_rows(observed, columns) == _canonical_derived_rows(rows, columns)
    return len(resources)


def _canonical_derived_rows(rows, columns):
    """PostgreSQL jsonb text formatting is not part of the independent row identity."""
    json_fields = {"payload_json", "network_refs", "plan_payload_json"}
    return sorted(
        json.dumps(
            [
                json.loads(value) if column in json_fields and type(value) is str else value
                for column, value in zip(columns, row)
            ],
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        for row in rows
    )
