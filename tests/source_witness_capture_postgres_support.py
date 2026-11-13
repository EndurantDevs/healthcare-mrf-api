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

SOURCE_PRODUCER_SHA256 = "25a289da181b3706007411dca30c8e121bac966adbb462f008d614f495bb51cb"
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
        descriptor = descriptors[0]
    snapshot_by_field = {key: descriptor[key] for key in ("coordinates", "dataset_sha256", "tables")}
    return descriptor, snapshot_by_field


async def _reviewed_source_publication(runtime, profile, candidate, catalog, descriptor, snapshot_by_field):
    """Read original publication facts under the genuine Publisher, without creating Reader proof."""
    from tests import generic_source_successor_postgres_support as source

    async with catalog._catalog_source_export_session(runtime, source_profile=profile) as scope:
        header_relation = descriptor["tables"]["header"]
        assert snapshot_by_field == {key: descriptor[key] for key in ("coordinates", "dataset_sha256", "tables")}
        header = await _current_header(
            scope.connection, header_relation["schema"], source.SOURCE, candidate.endpoint_id
        )
        head, summary, source_summary = _admitted_summary(header, profile.schema_name, source.SOURCE)
        metadata = await scope.connection.fetchrow(
            f"SELECT octet_length(publication_metadata_json::text)::bigint AS metadata_bytes,"
            "encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS metadata_sha256 "
            f'FROM "{header_relation["schema"]}"."{header_relation["name"]}" WHERE dataset_id=$1',
            candidate.dataset_id,
        )
        assert head.dataset_id == descriptor["coordinates"]["dataset_id"] == candidate.dataset_id
        assert head.endpoint_id == descriptor["endpoint_id"] == candidate.endpoint_id
        assert head.dataset_sha256 == descriptor["dataset_sha256"]

    return head, summary, source_summary, metadata


async def reviewed_source_origin(runtime, profile, candidate, catalog, producer):
    """Derive the complete named input from actual Source publication and content observations."""
    descriptor, snapshot_by_field = await _observed_source_descriptor(runtime, profile, candidate, catalog)
    head, summary, source_summary, metadata = await _reviewed_source_publication(
        runtime, profile, candidate, catalog, descriptor, snapshot_by_field
    )
    counts = tuple(sorted(source_summary["resource_counts"].items()))
    declaration_by_field = {
        "schema_name": descriptor["tables"]["resource"]["schema"],
        "source_id": descriptor["coordinates"]["source_id"],
        "endpoint_id": head.endpoint_id,
        "dataset_id": head.dataset_id,
        "dataset_sha256": head.dataset_sha256,
        "root_run_id": head.root_run_id,
        "header_relation": descriptor["tables"]["header"],
        "resource_relation": descriptor["tables"]["resource"],
        "metadata_bytes": metadata["metadata_bytes"],
        "metadata_text_sha256": metadata["metadata_sha256"],
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
    }
    assert len(declaration_by_field) == 21 and set(declaration_by_field) == {
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
        assert len(asdict(actual)) in {21, 23}
    return actual


@asynccontextmanager
async def source_witness_case(monkeypatch, *, legacy=False, named=False):
    """Use the existing owned native fixture and original capture Writer, without extra grants."""
    catalog, producer = source_modules()
    from tests import generic_source_successor_postgres_support as source

    if legacy:
        from tests.source_witness_legacy_postgres_support import install_original_legacy_seed, reviewed_legacy_origin

        seeded = install_original_legacy_seed(monkeypatch)
    async with (
        source.native_generic_source(monkeypatch, named=named and not legacy) as fixture,
        AsyncExitStack() as cleanup,
    ):
        old_capture = None if legacy else await source.publish_edition(fixture, 1)
        if named and not legacy:
            from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

            profile = FHIRCatalogSourceReadProfile(fixture.schema, fixture.schema)
        else:
            profile = configure_original_runtime(fixture.component, monkeypatch)
        source_export = await cleanup.enter_async_context(producer.membership_source_export_runtime())
        runtime = source_export.args[0]
        expected_publisher = (
            fixture.fixture.roles.publisher_role if named and not legacy else fixture.component.publisher
        )
        assert runtime.config.publisher_role == expected_publisher
        if legacy:
            assert seeded == [True]
            origin = await reviewed_legacy_origin(runtime, profile, catalog, producer)
        else:
            with monkeypatch.context() as selected:
                if named:
                    selected.setattr(source, "SOURCE", old_capture.origin.source_id)
                origin = await reviewed_source_origin(runtime, profile, fixture.candidates[-1], catalog, producer)
        constructed = await constructed_source_origin(source_export, profile, origin)
        connections_by_kind = await _source_witness_connections(fixture, cleanup, administrator=named)
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
        if named:
            case.application_roles = _source_witness_application_roles(fixture, legacy=legacy)
        yield case
        assert fixture.commits == [candidate.dataset_id for candidate in fixture.candidates]
        assert fixture.source_writer_rejections == fixture.commits


async def _source_witness_connections(fixture, cleanup, *, administrator):
    connections_by_kind = {}
    for kind, role in (("writer", fixture.component.retained_publisher), ("reader", fixture.component.reader)):
        if kind == "writer" and administrator:
            connections_by_kind[kind] = fixture.admin
            continue
        connection = await asyncpg.connect(
            fixture.component.admin_engine.url.set(drivername="postgresql", username=role).render_as_string(
                hide_password=False
            )
        )
        cleanup.push_async_callback(connection.close)
        connections_by_kind[kind] = connection
    return connections_by_kind


def _source_witness_application_roles(fixture, *, legacy):
    policy = fixture.component.policy if legacy else fixture.issued.policy
    return tuple(
        sorted(
            {
                policy.owner_role,
                policy.publisher_role,
                *policy.runtime_roles,
                *policy.control_runtime_roles,
                fixture.component.retained_publisher,
            }
            - {fixture.role_by_kind["owner"]}
        )
    )


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
            source_policy=case.fixture.issued.policy
            if getattr(case.fixture, "named", False)
            else case.fixture.component.policy,
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
    assert matches[0].is_active
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
