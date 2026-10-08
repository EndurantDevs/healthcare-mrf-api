# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native model SOURCE capabilities for disposable PostgreSQL archive tests."""

from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from process import npi_result_archive as archive


class NativeSourceCustody:
    """Bind precreated OIDs and closed native ownership to the producing transaction."""

    def __init__(self, *, owner_role, reader_role, builder_role, schemas=None):
        self.owner_role, self.reader_role = owner_role, reader_role
        self.read_only_roles = (reader_role, builder_role)
        self.precreated = {}
        self.closed = {}
        self.schemas = set() if schemas is None else schemas
        self.source_copy = archive.native_archive.ReferenceFamilySourceCopy(
            archive.native_archive.native_copy_projection, 1_000_000, 30
        )

    async def precreate(self, session, ownership):
        """Authenticate complete empty unindexed heaps before transferring custody."""
        assert session.in_transaction()
        await archive.verify_npi_stage_ownership(session, ownership)
        assert tuple(name for name, _ in ownership.relation_oids) == tuple(
            sorted(archive.npi_archive_names(canonical=True))
        )
        for table_name, relation_oid in ownership.relation_oids:
            assert await session.scalar(
                text("SELECT relowner=current_user::regrole::oid FROM pg_class WHERE oid=:oid"),
                {"oid": relation_oid},
            )
            assert not await session.scalar(
                text(f'SELECT EXISTS(SELECT 1 FROM "{ownership.schema_name}"."{table_name}")')
            )
            assert not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=:oid)"), {"oid": relation_oid}
            )
        self.precreated[ownership.dataset_id] = (session.get_transaction(), ownership)
        await session.execute(text(f'ALTER SCHEMA "{ownership.schema_name}" OWNER TO "{self.owner_role}"'))
        for table_name, _ in ownership.relation_oids:
            await session.execute(
                text(f'ALTER TABLE "{ownership.schema_name}"."{table_name}" OWNER TO "{self.owner_role}"')
            )
        await session.execute(text(f'REVOKE ALL ON SCHEMA "{ownership.schema_name}" FROM PUBLIC'))
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{ownership.schema_name}" TO "{self.reader_role}"'))
        for table_name, _ in ownership.relation_oids:
            await session.execute(
                text(f'GRANT SELECT ON "{ownership.schema_name}"."{table_name}" TO "{self.reader_role}"')
            )

    async def retain(self, session, prepared):
        """Close only the exact original heaps after model indexes and set verification."""
        transaction, initial = self.precreated[prepared.ownership.dataset_id]
        assert session.get_transaction() is transaction and transaction.is_active
        assert prepared.ownership == initial
        self.closed[initial.dataset_id] = await self._receipt(session, initial)

    async def verify(self, session, prepared):
        """Reopening export must corroborate the same owner, catalog and read-only role."""
        await archive.verify_npi_stage_ownership(session, prepared.ownership)
        actual = await self._receipt(session, prepared.ownership)
        if actual != self.closed[prepared.ownership.dataset_id]:
            raise archive.NpiResultArchiveError("NPI closed source custody differs")

    async def _receipt(self, session, ownership):
        """Reject ordinary write paths and record catalog incarnations, not trigger tokens."""
        catalog_records = (
            await session.execute(
                text(
                    "SELECT c.oid,c.relowner,c.xmin::text,c.ctid::text,c.relacl::text FROM pg_class c "
                    "WHERE c.oid=ANY(CAST(:oids AS oid[])) "
                    "OR c.oid IN(SELECT indexrelid FROM pg_index WHERE indrelid=ANY(CAST(:oids AS oid[]))) "
                    "ORDER BY c.oid"
                ),
                {
                    "oids": [oid for _, oid in ownership.relation_oids]
                    + [oid for _, oid, _, _ in ownership.sequence_oids]
                },
            )
        ).all()
        owner_oid = await session.scalar(text("SELECT CAST(:owner AS regrole)::oid"), {"owner": self.owner_role})
        assert len(catalog_records) > 7 and all(
            catalog_record.relowner == owner_oid for catalog_record in catalog_records
        )
        await self.assert_closed_roles(session, ownership)
        namespace = (
            await session.execute(
                text("SELECT nspowner,nspacl::text,xmin::text,ctid::text FROM pg_namespace WHERE oid=:oid"),
                {"oid": ownership.schema_oid},
            )
        ).one()
        assert namespace.nspowner == owner_oid
        return tuple(tuple(catalog_record) for catalog_record in catalog_records), tuple(namespace)

    async def assert_closed_roles(self, session, ownership):
        """Both independent payload principals must be read-only before validation."""
        for role in self.read_only_roles:
            assert await session.scalar(
                text(
                    "SELECT NOT has_schema_privilege(:role,CAST(:schema AS oid),'CREATE') "
                    "AND NOT EXISTS(SELECT 1 FROM unnest(CAST(:oids AS oid[])) r(oid) "
                    "WHERE has_table_privilege(:role,r.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) "
                    "AND NOT EXISTS(SELECT 1 FROM unnest(CAST(:oids AS oid[])) r(oid) "
                    "WHERE has_any_column_privilege(:role,r.oid,'INSERT,UPDATE,REFERENCES')) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=ANY(CAST(:oids AS oid[]))) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=ANY(CAST(:oids AS oid[])) AND contype='f')"
                ),
                {
                    "role": role,
                    "schema": ownership.schema_oid,
                    "oids": [oid for _, oid in ownership.relation_oids],
                },
            )
            for _, sequence_oid, _, _ in ownership.sequence_oids:
                assert not await session.scalar(
                    text("SELECT has_sequence_privilege(:role,CAST(:oid AS oid),'USAGE,UPDATE')"),
                    {"role": role, "oid": sequence_oid},
                )


@asynccontextmanager
async def native_npi_runtime(database_url):
    """Create only registered disposable principals; payload never uses the administrator."""
    from db.models import AddressArchiveV2
    from tests.test_npi_result_archive_postgres import _ensure_model_extensions

    administrator = create_async_engine(database_url, poolclass=NullPool, hide_parameters=True)
    suffix, password = uuid4().hex, uuid4().hex
    owner = f"npi_owner_{suffix}"
    publisher = f"npi_publisher_{suffix}"
    builder = f"npi_builder_{suffix}"
    reader = f"npi_reader_{suffix}"
    schemas, roles = set(), (owner, publisher, builder, reader)
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(administrator.dispose)
        cleanup.push_async_callback(_drop_role_resources, administrator, schemas, roles)
        async with administrator.begin() as connection:
            model_search_path = await _ensure_model_extensions(connection)
            if not await connection.scalar(text("SELECT to_regnamespace('mrf') IS NOT NULL")):
                schemas.add("mrf")
                await connection.execute(text('CREATE SCHEMA "mrf"'))
            await connection.run_sync(
                lambda sync: AddressArchiveV2.__table__.c.geo_source.type.create(sync, checkfirst=True)
            )
            database = await connection.scalar(text("SELECT current_database()"))
            driver = (await connection.get_raw_connection()).driver_connection
            for role in roles:
                login = "NOLOGIN" if role == owner else f"LOGIN PASSWORD '{password}'"
                try:
                    await driver.execute(f'CREATE ROLE "{role}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE')
                except Exception:
                    raise RuntimeError("native principal bootstrap failed") from None
            await connection.execute(text(f'GRANT "{owner}" TO "{publisher}"'))
            await connection.execute(text(f'GRANT CREATE ON DATABASE "{database}" TO "{owner}","{publisher}"'))
            await connection.execute(text(f'GRANT USAGE ON SCHEMA mrf TO "{publisher}","{builder}","{reader}"'))
        publisher_url = make_url(database_url).set(username=publisher, password=password)
        builder_url = publisher_url.set(username=builder)
        reader_url = publisher_url.set(username=reader)
        publishing = _model_actor_engine(publisher_url, model_search_path)
        building = _model_actor_engine(builder_url, model_search_path)
        reading = _model_actor_engine(reader_url, model_search_path)
        cleanup.push_async_callback(publishing.dispose)
        cleanup.push_async_callback(building.dispose)
        cleanup.push_async_callback(reading.dispose)
        yield SimpleNamespace(
            engine=publishing,
            administrator=administrator,
            sessions=async_sessionmaker(publishing, expire_on_commit=False),
            builders=async_sessionmaker(building, expire_on_commit=False),
            readers=async_sessionmaker(reading, expire_on_commit=False),
            custody=NativeSourceCustody(owner_role=owner, reader_role=reader, builder_role=builder, schemas=schemas),
            schemas=schemas,
            owner=owner,
            publisher=publisher,
            builder=builder,
            reader=reader,
            publisher_url=publisher_url,
            builder_url=builder_url,
            reader_url=reader_url,
            model_search_path=model_search_path,
        )


def _model_actor_engine(database_url, search_path):
    """Keep the discovered extension path on each independently authenticated fixture connection."""
    return create_async_engine(
        database_url,
        poolclass=NullPool,
        hide_parameters=True,
        connect_args={"server_settings": {"search_path": search_path}},
    )


async def create_native_npi_family(runtime, schema, *, populated=True):
    """Keep genuine historical source controls around the exact compiled payload model."""
    import importlib.util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    source_dataset = uuid4()
    runtime.schemas.update((schema, archive.npi_stage_schema(source_dataset)))
    async with runtime.sessions() as session, session.begin():
        await _create_source(session, schema, source_dataset)
        if not populated:
            for model in archive.npi_archive_models():
                await session.execute(text(f'DELETE FROM "{schema}"."{model.__tablename__}"'))
            await session.execute(text(f'DELETE FROM "{schema}".address_archive_v2'))
        await session.execute(
            text(
                f'CREATE TABLE "{schema}".npi_canonical_publication_receipt ('
                "publication_ref text PRIMARY KEY,publication_generation bigint NOT NULL,chain_ref text NOT NULL,import_date date NOT NULL,"
                + ",".join(f"{model.__tablename__}_table_oid bigint NOT NULL" for model in archive.npi_archive_models())
                + ")"
            )
        )
        await session.execute(
            text(f'CREATE TABLE "{schema}".npi_canonical_publication_receipt_seal (publication_ref text PRIMARY KEY)')
        )
        path = (
            Path(__file__).resolve().parents[1] / "alembic/versions/20260808230000_npi_canonical_publication_receipt.py"
        )
        spec = importlib.util.spec_from_file_location("native_npi_source_control", path)
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        connection = await session.connection()

        def install(sync):
            migration.op = Operations(MigrationContext.configure(sync))
            migration._create_canonical_mutation_guard(
                schema, canonical_tables=tuple(model.__tablename__ for model in archive.npi_archive_models())
            )

        await connection.run_sync(install)


async def _drop_role_resources(engine, schemas, roles):
    """Drop only names registered before creation, without forcing connections away."""
    async with engine.begin() as connection:
        for schema in sorted(schemas):
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        for role in reversed(roles):
            if await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            ):
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


async def _create_source(session, schema, dataset_id):
    """Compile source model heaps and the genuine installed canonical self edge."""
    from db.models import AddressArchiveV2
    from process import mrf_address_publication as canonical
    from tests.test_npi_result_archive_postgres import _run_migration

    initial = await archive.precreate_npi_restore(session, dataset_id=dataset_id)
    await archive.complete_npi_restore(session, initial)
    await session.execute(text(f'ALTER SCHEMA "{initial.schema_name}" RENAME TO "{schema}"'))
    spec = archive.native_archive.ReferenceFamilySpec("npi", (AddressArchiveV2,))
    await archive.native_archive._create_model_heaps(session, spec, schema, create_indexes=False, ordinary_heaps=True)
    await archive.native_archive._create_model_indexes(session, spec, schema, create_constraints=True)
    await canonical.canonical_spatial_index(session, schema, "address_archive_v2", canonical._qualified)
    await session.execute(
        text(
            f'ALTER TABLE "{schema}".address_archive_v2 ADD FOREIGN KEY (merged_into) REFERENCES "{schema}".address_archive_v2(address_key)'
        )
    )
    await session.execute(text(f"INSERT INTO \"{schema}\".npi(npi,provider_first_name) VALUES (1000000001,'before')"))
    await session.execute(
        text(
            f"INSERT INTO \"{schema}\".npi_address(npi,type,checksum,first_line) VALUES (1000000001,'primary',1,'before')"
        )
    )
    address_key = uuid4()
    await session.execute(
        text(
            f'INSERT INTO "{schema}".address_archive_v2(address_key,identity_key,source_bits,strict_source_bits) '
            "VALUES (:key,:identity,1,1)"
        ),
        {"key": address_key, "identity": f"synthetic-native-address-{address_key.hex}"},
    )
    await session.execute(text(f'UPDATE "{schema}".npi_address SET address_key=:key'), {"key": address_key})
    await _run_migration(await session.connection(), schema)
