# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published import and retained CMS fixtures sharing one ordinary Reader."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from sanic import Blueprint
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import custom_import_read_http as transport
from api import signed_request_reader
from api.endpoint import extension_reads
from db.models import NPIData
from db.models.custom_import import CustomImportDataset
from process.custom_import.read_contracts import PinnedReadTarget
from process.custom_import.runner import run_candidate
from process.provider_directory_cms_serving_receipt import read_serving_receipt
from tests import test_custom_import_read_http as permits
from tests import test_custom_import_runner_postgres as runner
from tests.custom_import_postgres_support import _install_custom_import_migrations
from tests.test_custom_import_snapshot_reads_postgres import _grant_reader_access
from tests.test_network_catalog_routes import v2_envelope

PROVIDER_NPI = "1000000491"
ZERO_OFFICE_NPI = "1000000012"
BATCH_PATH = "/api/v1/extensions/custom-import/providers/batch"


@asynccontextmanager
async def import_publication_owner(context):
    """Register exact owner cleanup before creating its isolated migration authority."""
    fixture = context.fixture
    owner = "custom_import_owner_" + uuid4().hex[:16]
    is_created = False
    async with fixture.engine.connect() as connection:
        before = await read_serving_receipt(connection, "mrf")
    try:
        await fixture.connection.execute(
            f'CREATE ROLE "{owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
        )
        is_created = True
        await fixture.connection.execute(f'GRANT USAGE, CREATE ON SCHEMA mrf TO "{owner}"')
        database = fixture.engine.url.database
        await fixture.connection.execute(f'GRANT CREATE ON DATABASE "{database}" TO "{owner}"')
        publication_case = SimpleNamespace(
            engine=fixture.engine,
            sessions=async_sessionmaker(fixture.engine, expire_on_commit=False, autoflush=False),
            schema_name="mrf",
        )
        async with fixture.engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{owner}"'))
            await connection.run_sync(_install_custom_import_migrations, "mrf", True)
        async with fixture.engine.begin() as connection:
            await _grant_reader_access(connection, publication_case, context.role)
            await connection.execute(
                insert(NPIData).values(
                    npi=int(ZERO_OFFICE_NPI), provider_first_name="Known", provider_last_name="No Office"
                )
            )
        async with fixture.engine.connect() as connection:
            assert await read_serving_receipt(connection, "mrf") == before
        yield publication_case
    finally:
        if is_created:
            await fixture.connection.execute(f'DROP OWNED BY "{owner}" CASCADE')
            await fixture.connection.execute(f'DROP ROLE "{owner}"')
            assert not await fixture.connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", owner
            )


async def publish_import(context, publication_case):
    """Publish complete typed families without replacing source or read authority."""
    fixture = context.fixture
    async with fixture.engine.connect() as connection:
        before = await read_serving_receipt(connection, "mrf")
    suffix = uuid4().hex
    publication_seed = await runner._seed_case(publication_case, suffix)
    execution_id, token = await runner._new_execution(publication_case, publication_seed, suffix)
    publication_result = await run_candidate(
        publication_case.sessions,
        runner._request(
            publication_seed,
            execution_id,
            token,
            [runner._root(npi, "Synthetic imported provider") for npi in (PROVIDER_NPI, ZERO_OFFICE_NPI)],
            [
                runner._rate(npi, "SYNTHETIC_" + str(index), Decimal("12.50"))
                for index, npi in enumerate((PROVIDER_NPI, ZERO_OFFICE_NPI))
            ],
        ),
    )
    assert publication_result.status == "activated" and publication_result.accepted_family_count == 2, (
        publication_result
    )
    pinned_target = PinnedReadTarget(
        publication_seed.dataset_id,
        publication_result.generation_id,
        publication_seed.definition_revision_id,
        publication_seed.schema_revision_id,
        "default",
    )
    async with publication_case.sessions() as session:
        dataset_key = await session.scalar(
            select(CustomImportDataset.dataset_key).where(CustomImportDataset.dataset_id == publication_seed.dataset_id)
        )
    async with fixture.engine.connect() as connection:
        assert await read_serving_receipt(connection, "mrf") == before
    return (
        publication_case,
        pinned_target,
        {
            "dataset_key": dataset_key,
            "generation_id": pinned_target.generation_id,
            "definition_revision_id": pinned_target.definition_revision_id,
            "schema_revision_id": pinned_target.schema_revision_id,
            "profile_id": pinned_target.profile_id,
        },
    )


def install_signed_routes(context, monkeypatch):
    """Use the real route and keyring; no read service or authority replacements."""
    monkeypatch.setattr(signed_request_reader, "db", context.database)
    transport._keyring_for_document.cache_clear()
    transport._cursor_secret_for_document.cache_clear()
    monkeypatch.setenv(transport.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV, permits._keyring_document())
    monkeypatch.setenv(transport.CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV, permits._encoded(permits._CURSOR_KEY))
    context.app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    reader = context.database._reader_database

    @context.app.middleware("request")
    async def fresh_reader_session(request):
        if request.path.startswith("/api/v1/extensions/custom-import/"):
            request.ctx.sa_session = reader.session_factory()

    @context.app.middleware("response")
    async def close_reader_session(request, _reply):
        session = getattr(request.ctx, "sa_session", None)
        if session is not None:
            await session.close()


def signed_headers(body, target, *, path=BATCH_PATH, provider=True, **changes):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    fields = {
        "target": target,
        "issued_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": (now + timedelta(seconds=60)).isoformat().replace("+00:00", "Z"),
    } | changes
    helper = permits._resigned_provider_headers if provider else permits._resigned_headers
    return helper(body=body, path=path, **fields)


def managed_document(target, *, npi=PROVIDER_NPI, network_ids=None, excluded_network_ids=(), generation=None):
    query_by_field = {"npi": npi, "include": ["taxonomy", "plans"], "include_evidence": "true"}
    if network_ids is not None:
        query_by_field["selector"] = {"namespace": "canonical", "values": list(network_ids)}
    if generation is not None:
        query_by_field["network_generation"] = str(generation)
    return {
        "target": target,
        "native_query": {"include_total": "true"},
        "context": [],
        "filters": [],
        "order": None,
        "require_match": True,
        "include_filter": True,
        "native_batch": {
            "npis": [npi],
            "address_limit": 5,
            "address_offset": 0,
            "include_sources": False,
            "include_evidence": False,
        },
        "managed_detail": v2_envelope(query_by_field, excluded_network_ids=list(excluded_network_ids)),
    }
