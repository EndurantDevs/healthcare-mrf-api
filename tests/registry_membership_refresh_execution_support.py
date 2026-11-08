# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One native accepted ACA source, address origin and isolated publication role_by_kind."""

import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from sqlalchemy import MetaData, text

from db.models import EntityAddressUnified
from process import registry_membership_refresh_runtime as runtime
from process.network_candidate_custody import seal_network_candidate_custody
from process.registry_candidate_custody_preparation import split_registry_candidate_custody
from process.registry_management_permissions import _aca_controls, _catalog, install_registry_management_permissions
from process.registry_membership_refresh_authority import AUTHORITY_PATH, COMMIT_AUTHORITY_PATH
from process.registry_membership_refresh_contract import SERVICE_ACTOR, accepted_membership_source_sha256
from process.registry_membership_refresh_queue import enqueue_registry_membership_refresh
from process.registry_publication_consumer import RegistryPublicationConsumerConfig, consume_registry_publication_once
from process.registry_publication_ptg_runtime import RegistryPublicationSQLLimits
from process.registry_publication_worker import _BoundedPool, _close_pool, _pool_options
from process.registry_publication_worker_permissions import install_registry_publication_worker_permissions
from tests import registry_aca_refresh_anchor_postgres_support as accepted
from tests import registry_compact_source_office_postgres_support as offices
from tests import test_mrf_publication_receipt_postgres as mrf
from tests.test_registry_membership_refresh_authority import receipt
from tests.test_registry_membership_refresh_sources_postgres import _aca_context
from tests.test_registry_publication_authority import _reply


def _canonical_office():
    import ptg2_address_canon

    raw = ("123 Example Street", "Suite 2", "Sample City", "VA", "90210", "US")
    canonical = ptg2_address_canon.canonicalize_batch([raw])[0]
    return raw, canonical | {"identity_version": 2, "precision": "street", "source_bits": 16, "merged_into": None}


async def _source_rows(connection, schema, raw, canonical):
    """Write only the mutable producer input, before native completion and capture."""
    await connection.execute(
        text(f"INSERT INTO \"{schema}\".plan(plan_id,year,issuer_id,state) VALUES (:plan,2026,12345,'VA')"),
        {"plan": "12345VA0000001"},
    )
    await connection.execute(
        text(f"INSERT INTO \"{schema}\".plan_networktier VALUES ('12345VA0000001','tier-42',12345,2026,42)")
    )
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".plan_npi_raw(npi,checksum_network,network_tier,issuer_id,year) '
            "VALUES (1000000491,42,'tier-42',12345,2026)"
        )
    )
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".mrf_address_evidence '
            "(evidence_checksum,npi,type,checksum,issuer_id,year,checksum_network,network_tier,import_id,"
            "source_url,source_record_id,address_key,first_line,second_line,city_name,state_name,postal_code,country_code) "
            "VALUES (1,1000000491,'primary',1,12345,2026,42,'tier-42','accepted-import',"
            "'https://source.example.test/accepted','provider-one',:site,:first,:second,:city,:state,:zip,:country)"
        ),
        dict(zip(("first", "second", "city", "state", "zip", "country"), raw))
        | {"site": UUID(canonical["address_key"])},
    )
    address_by_field = {key: field_value for key, field_value in canonical.items() if key != "premise_identity_key"}
    columns = ",".join('"' + key + '"' for key in address_by_field)
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".address_archive_v2({columns}) '
            f'SELECT {columns} FROM jsonb_populate_record(NULL::"{schema}".address_archive_v2,CAST(:payload AS jsonb))'
        ),
        {"payload": json.dumps(address_by_field)},
    )


def _populate_before_completion(monkeypatch):
    raw, canonical = _canonical_office()
    original = mrf._configure_summary_tables

    async def configured(connection, patch, schema, **options):
        await original(connection, patch, schema, **options)
        await _source_rows(connection, schema, raw, canonical)

    monkeypatch.setattr(mrf, "_configure_summary_tables", configured)
    return raw, canonical


async def _remove_worker_roles(fixture, created_roles):
    """Remove only this fixture's candidate namespaces and registered role names."""
    for row in await fixture.connection.fetch(
        "SELECT nspname FROM pg_namespace WHERE nspowner IN (SELECT oid FROM pg_roles WHERE rolname=ANY($1::text[]))",
        [fixture.roles_by_kind[kind] for kind in ("physical", "custody")],
    ):
        await fixture.connection.execute('DROP SCHEMA "' + row["nspname"] + '" CASCADE')
    await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{fixture.control_schema}" CASCADE')
    for role in reversed(created_roles):
        await fixture.connection.execute(f'DROP OWNED BY "{role}"')
        await fixture.connection.execute(f'DROP ROLE "{role}"')
    assert not await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
    )


async def _worker_roles(fixture, stack):
    role_by_kind = {
        kind: "refresh_execution_" + kind + "_" + uuid4().hex
        for kind in ("queue", "physical", "custody", "api", "lifecycle", "execution", "custodian")
    }
    fixture.roles_by_kind, fixture.password = role_by_kind, uuid4().hex
    created_roles = []
    stack.push_async_callback(_remove_worker_roles, fixture, created_roles)
    connection, schema = fixture.connection, fixture.control_schema
    for kind, role in role_by_kind.items():
        login = "NOLOGIN" if kind in {"queue", "physical", "custody"} else f"LOGIN PASSWORD '{fixture.password}'"
        await connection.execute(f'CREATE ROLE "{role}" {login}')
        created_roles.append(role)
        if kind in {"api", "lifecycle", "execution", "custodian"}:
            await connection.execute(f"ALTER ROLE \"{role}\" SET temp_file_limit='256MB'")
    await connection.execute(f"ALTER ROLE \"{fixture.case.names.reader}\" SET temp_file_limit='256MB'")
    head_role = fixture.readers.aca_head_fence_sessions.kw["bind"].url.username
    await connection.execute(f"ALTER ROLE \"{head_role}\" SET temp_file_limit='256MB'")
    tables, sequences = await _catalog(connection, await connection.fetchval("SELECT to_regnamespace($1)::oid", schema))
    controls = await _aca_controls(connection, schema)
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{role_by_kind["queue"]}"')
    for relation in tables + controls + sequences:
        kind = "SEQUENCE" if relation in sequences else "TABLE"
        await connection.execute(f'ALTER {kind} "{schema}"."{relation["relname"]}" OWNER TO "{role_by_kind["queue"]}"')
    async with connection.transaction():
        await install_registry_management_permissions(
            connection, api_role=role_by_kind["api"], owner_role=role_by_kind["queue"], control_schema=schema
        )
        await install_registry_publication_worker_permissions(connection, **fixture.permission_roles)
    for grantee in (role_by_kind["api"], role_by_kind["execution"]):
        await connection.execute(
            f'GRANT "{fixture.case.names.reader}" TO "{grantee}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
        )
    await connection.execute(
        f'GRANT "{role_by_kind["execution"]}" TO "{role_by_kind["custodian"]}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
    )
    for owner in (role_by_kind["physical"], role_by_kind["custody"]):
        await connection.execute(
            f'GRANT "{owner}" TO "{role_by_kind["custodian"]}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE'
        )
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role_by_kind["custody"]}"')
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    await connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{role_by_kind["custody"]}"')
    async with fixture.engine.begin() as sql_connection:
        await split_registry_candidate_custody(
            sql_connection,
            owner_role=role_by_kind["queue"],
            custody_owner_role=role_by_kind["custody"],
            control_schema=schema,
        )


async def _address_origin(fixture, stack, raw, canonical):
    """Use the existing native generation/alias fixture with the admitted office."""
    seed = "refresh_address_seed_" + uuid4().hex
    await fixture.connection.execute(f'CREATE SCHEMA "{seed}"')
    stack.push_async_callback(fixture.connection.execute, f'DROP SCHEMA IF EXISTS "{seed}" CASCADE')
    async with fixture.engine.begin() as connection:
        table = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=seed)
        await connection.run_sync(lambda sync: table.create(sync))
    await fixture.connection.execute(
        f'INSERT INTO "{seed}".entity_address_unified '
        "(location_key,entity_type,entity_id,npi,checksum,type,first_line,second_line,city_name,state_name,"
        "postal_code,country_code,address_key,premise_key,address_precision,lat,long) "
        "VALUES($1,'npi','1000000491',1000000491,42,'practice',$2,$3,$4,$5,$6,$7,$8,$9,'street',34,-118)",
        "a" * 64,
        *raw,
        UUID(canonical["address_key"]),
        UUID(canonical["premise_key"]),
    )
    fixture.source_schema = seed
    namespace = "refresh_address_" + uuid4().hex
    role_by_kind = {"owner": fixture.case.names.retention_owner}
    created_namespaces, created_roles = [False], []
    stack.push_async_callback(
        offices._cleanup_native, fixture, namespace, role_by_kind, created_namespaces, created_roles
    )
    await offices._native_storage(fixture, namespace, role_by_kind, created_namespaces, created_roles)
    reader = fixture.case.names.reader
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{reader}"')
    await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{reader}"')
    fixture.address_schema = namespace


class RefreshExecution:
    """Test lifecycle; every source, queue and storage operation uses native credentials."""

    def __init__(self, case, context):
        self.case = case
        self.outer, self.readers, self.control_schema, self.approved, self.enrollment = context
        self.engine = case.admin.kw["bind"]
        self.calls, self.denied_commit, self.cancel_commit, self.after_seal = [], False, False, None

    @property
    def permission_roles(self):
        role_by_kind = self.roles_by_kind
        return dict(
            lifecycle_role=role_by_kind["lifecycle"],
            execution_role=role_by_kind["execution"],
            api_role=role_by_kind["api"],
            queue_owner_role=role_by_kind["queue"],
            physical_owner_role=role_by_kind["physical"],
            control_schema=self.control_schema,
        )

    async def http(self, request):
        self.calls.append(request.url.path)
        assert request.headers["authorization"] == "Bearer synthetic-refresh-only" and "cookie" not in request.headers
        if request.method == "GET":
            return _reply({"policy_revision": 7, "enrolled_scopes": self.enrollment, "next_binding_id": None})
        assert request.url.path in {AUTHORITY_PATH, COMMIT_AUTHORITY_PATH}
        document = json.loads(request.content)
        assert document["actor"] == SERVICE_ACTOR
        if request.url.path == COMMIT_AUTHORITY_PATH:
            await self._assert_source_fenced()
            if self.cancel_commit:
                raise asyncio.CancelledError
            if self.denied_commit:
                return _reply({"error": "revoked"}, status=403)
        return _reply(receipt(document))

    async def _assert_source_fenced(self):
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with self.connection.transaction():
                await self.connection.execute(
                    f'LOCK TABLE "{self.case.names.source_schema}".mrf_publication_receipt IN ROW EXCLUSIVE MODE NOWAIT'
                )

    async def seal(self, target):
        async def authorize(connection, supplied):
            assert connection is self.custodian and supplied == target
            return await connection.fetchval(
                "SELECT current_user=session_user AND current_user::text=$1", self.roles_by_kind["custodian"]
            )

        await seal_network_candidate_custody(
            self.custodian,
            target,
            control_schema=self.control_schema,
            runtime_roles=(self.case.names.reader,),
            authorize=authorize,
        )
        if self.after_seal is not None:
            await self.after_seal(target)

    async def enqueue(self, command):
        async with self.api.transaction(isolation="repeatable_read"):
            return await enqueue_registry_membership_refresh(
                self.api,
                {"actor": dict(SERVICE_ACTOR), "command": command},
                authorize=self.bundle.runtime.authorize,
                address_profile=self.bundle.runtime.address_profile,
                control_schema=self.control_schema,
            )

    def command(self, inputs):
        vector = inputs["accepted_sources"]
        digest = runtime._semantic_digest(
            vector,
            inputs["approved_revision"],
            inputs["address_generation"],
            inputs["address_artifact_sha256"],
            inputs["enrolled_scopes"],
        )
        return dict(
            operation="refresh_memberships",
            expected_approved_revision=inputs["approved_revision"],
            expected_serving_generation=inputs["expected_serving_generation"],
            expected_serving_manifest_sha256=inputs["expected_serving_manifest_sha256"],
            address_artifact_sha256=inputs["address_artifact_sha256"],
            accepted_sources=vector,
            accepted_source_vector_sha256=accepted_membership_source_sha256(vector),
            enrolled_scopes=inputs["enrolled_scopes"],
            reason="Refresh approved source memberships",
            idempotency_key="registry-membership-refresh:" + digest,
        )

    async def consume(self, request_id):
        config = RegistryPublicationConsumerConfig(
            **self.permission_roles,
            reader_roles=(self.case.names.reader,),
            seal_candidate=self.seal,
            only_request_id=UUID(request_id),
            membership_refresh_only=True,
            membership_refresh_runtime=self.bundle.runtime,
        )
        return await consume_registry_publication_once(
            lifecycle_pool=self.lifecycle_pool,
            execution_pool=self.execution_pool,
            authorize=self.bundle.runtime.authorize,
            config=config,
        )


async def _connect_workers(fixture, stack):
    limits = RegistryPublicationSQLLimits()
    for kind in ("api", "custodian"):
        url = fixture.engine.url.set(
            drivername="postgresql", username=fixture.roles_by_kind[kind], password=fixture.password
        )
        connection = await asyncpg.connect(
            url.render_as_string(hide_password=False),
            timeout=limits.connect_timeout_seconds,
            server_settings=limits.server_settings(),
        )
        stack.push_async_callback(connection.close)
        await limits.verify(connection)
        setattr(fixture, kind, connection)
    for kind in ("lifecycle", "execution"):
        url = fixture.engine.url.set(
            drivername="postgresql", username=fixture.roles_by_kind[kind], password=fixture.password
        )
        options_by_name = _pool_options(url, 1, limits)
        options_by_name["host"] = url.query.get("host", url.host)
        options_by_name["port"] = int(url.query.get("port", url.port or 5432))
        pool = await asyncpg.create_pool(**options_by_name)
        stack.push_async_callback(_close_pool, pool)
        setattr(fixture, kind + "_pool", _BoundedPool(pool, limits.connect_timeout_seconds))


async def _runtime(fixture, stack):
    reader_url = fixture.case.sessions.kw["bind"].url.render_as_string(hide_password=False)
    head_url = fixture.readers.aca_head_fence_sessions.kw["bind"].url.render_as_string(hide_password=False)
    settings = runtime._settings(
        dict(
            enabled=True,
            app_origin="https://authority.example.invalid",
            service_token="synthetic-refresh-only",
            control_schema=fixture.control_schema,
            fence_dsn=reader_url,
            source_reader_dsn=reader_url,
            address_reader_dsn=reader_url,
            address_schema=fixture.address_schema,
            runtime_roles=[fixture.case.names.reader],
            source_profiles={
                "aca": dict(
                    control_schema=fixture.case.store.control_schema,
                    publisher_role=fixture.case.store.publisher_role,
                    reader_roles=[fixture.case.names.reader],
                    source_schema=fixture.case.names.source_schema,
                    head_fence_dsn=head_url,
                )
            },
        )
    )
    fixture.bundle = await runtime._build_native_bundle(settings, stack, fixture.roles_by_kind["execution"])
    authority = fixture.bundle.runtime.authorize
    await authority._client.aclose()
    authority._client = httpx.AsyncClient(transport=httpx.MockTransport(fixture.http))


@asynccontextmanager
async def refresh_execution(monkeypatch):
    """Keep one disposable database and all native pools alive through exact cleanup."""
    raw, canonical = _populate_before_completion(monkeypatch)
    async with accepted.accepted_aca_anchor(monkeypatch) as case, _aca_context(case, monkeypatch) as context:
        async with AsyncExitStack() as stack:
            fixture = RefreshExecution(case, context)
            fixture.connection = await asyncpg.connect(
                case.admin.kw["bind"].url.set(drivername="postgresql").render_as_string(hide_password=False)
            )
            stack.push_async_callback(fixture.connection.close)
            monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
            await _worker_roles(fixture, stack)
            await _address_origin(fixture, stack, raw, canonical)
            await _connect_workers(fixture, stack)
            await _runtime(fixture, stack)
            yield fixture
