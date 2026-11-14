# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrative grants on explicit schema-qualified registry metadata."""

from contextlib import asynccontextmanager

from db.models.company_group_registry import CompanyGroupRegistry
from db.models.company_registry import CompanyRegistry, HIOSIssuerRegistry
from db.models.company_registry_assertions import (
    CompanyRegistryGroupAssertion,
    CompanyRegistryIdentifierAssertion,
    CompanyRegistryRoleAssertion,
)
from db.models.company_registry_links import CompanyRegistryLinks, RegistryCompanyLinkBatch
from db.models.manual_directory_registry import (
    ManualLocationRegistry,
    ManualProviderLocationBinding,
    ManualProviderRegistry,
)
from db.models.network_membership_draft import NetworkMembershipDraft
from db.models.network_registry import NetworkRegistryAlias, NetworkRegistryIdentity, NetworkRegistryRecord
from db.models.network_serving import (
    NetworkCandidateCustody,
    NetworkMembershipBatch,
    NetworkMembershipCandidate,
    NetworkServingControl,
    NetworkServingManifest,
)
from db.models.registry_approval import RegistryApprovalHistory, RegistryApprovedRecord
from db.models.registry_evidence import (
    RegistryCompanyGroupAssertion,
    RegistryIdentifierBinding,
    RegistryIdentifierObservation,
    RegistryIssuerCompanyAssertion,
    RegistrySourceAdmissionReceipt,
    RegistrySourceObservation,
    RegistrySourceSnapshot,
)
from db.models.registry_network_binding import RegistryNetworkBinding, RegistryNetworkBindingBatch
from db.models.registry_publication_request import RegistryPublicationRequest
from db.models.registry_revision import RegistryRecordHistory, RegistryRevisionControl
from db.models.registry_site_binding import RegistrySiteBinding
from db.registry_fhir_catalog import SNAPSHOT_COLUMNS
from process.network_address_projection import _identifier
from process.network_membership_candidate_lifecycle import _control_namespace

_HEAD_MODELS = (
    CompanyGroupRegistry,
    CompanyRegistry,
    NetworkRegistryRecord,
    ManualProviderRegistry,
    ManualLocationRegistry,
    CompanyRegistryLinks,
    NetworkMembershipDraft,
    RegistrySiteBinding,
    RegistryNetworkBinding,
)
_MODELS = _HEAD_MODELS + (
    CompanyRegistryRoleAssertion,
    CompanyRegistryIdentifierAssertion,
    CompanyRegistryGroupAssertion,
    HIOSIssuerRegistry,
    NetworkRegistryIdentity,
    NetworkRegistryAlias,
    RegistryRevisionControl,
    RegistryRecordHistory,
    NetworkMembershipCandidate,
    NetworkCandidateCustody,
    NetworkMembershipBatch,
    NetworkServingManifest,
    NetworkServingControl,
    RegistrySourceSnapshot,
    RegistrySourceAdmissionReceipt,
    RegistrySourceObservation,
    RegistryIdentifierObservation,
    RegistryIdentifierBinding,
    RegistryIssuerCompanyAssertion,
    RegistryCompanyGroupAssertion,
    RegistryApprovalHistory,
    RegistryApprovedRecord,
    ManualProviderLocationBinding,
    RegistryPublicationRequest,
    RegistryCompanyLinkBatch,
    RegistryNetworkBindingBatch,
)
REGISTRY_TABLES = {model.__tablename__: tuple(model.__table__.columns.keys()) for model in _MODELS}

_TABLES = REGISTRY_TABLES
_PTG_TABLES = {
    "registry_ptg_producer_scope": (
        "scope_id",
        "scope_key",
        "actor_key",
        "idempotency_key",
        "approval_sha256",
        "approval_json",
        "created_at",
    ),
    "registry_ptg_published_plan_scope": (
        "scope_id",
        "scope_key",
        "actor_key",
        "idempotency_key",
        "approval_sha256",
        "approval_json",
        "created_at",
    ),
    "registry_ptg_office_approval": (
        "capture_id",
        "actor_key",
        "idempotency_key",
        "approval_sha256",
        "approval_json",
        "created_at",
    ),
    "registry_ptg_office_hold": ("capture_id", "approval_sha256", "hold_sha256", "hold_json", "created_at"),
}
_TABLES.update(_PTG_TABLES)
_TABLES["registry_fhir_catalog_snapshot"] = SNAPSHOT_COLUMNS
_INSERT_COLUMNS = {
    model.__tablename__: tuple(column for column in model.__table__.columns.keys() if column != "created_at")
    for model in _HEAD_MODELS
}
_INSERT_COLUMNS.update(
    manual_provider_location_binding=tuple(
        column for column in ManualProviderLocationBinding.__table__.columns.keys() if column != "created_at"
    ),
    registry_network_binding_batch=tuple(
        column for column in RegistryNetworkBindingBatch.__table__.columns.keys() if column != "created_at"
    ),
    registry_company_link_batch=tuple(
        column for column in RegistryCompanyLinkBatch.__table__.columns.keys() if column != "created_at"
    ),
    registry_publication_request=(
        "request_id",
        "actor_key",
        "session_token_sha256",
        "actor_json",
        "command_json",
        "idempotency_key",
        "request_sha256",
    ),
    network_registry_identity=("allocation_key",),
    registry_record_history=tuple(
        column for column in RegistryRecordHistory.__table__.columns.keys() if column != "created_at"
    ),
)
_INSERT_COLUMNS.update(
    {
        model.__tablename__: tuple(column for column in model.__table__.columns.keys() if column != "created_at")
        for model in (CompanyRegistryRoleAssertion, CompanyRegistryIdentifierAssertion, CompanyRegistryGroupAssertion)
    }
)
_UPDATE_COLUMNS = {
    model.__tablename__: tuple(
        column
        for column in model.__table__.columns.keys()
        if column not in {"created_at", *[item.name for item in model.__table__.primary_key]}
    )
    for model in _HEAD_MODELS
}
_UPDATE_COLUMNS["registry_revision_control"] = ("draft_revision",)
_UPDATE_COLUMNS["registry_network_binding"] = (
    "network_id",
    "evidence_id",
    "evidence_sha256",
    "office_recipe_json",
    "archived",
    "revision",
)
_SEQUENCES = {"network_registry_identity_network_id_seq", "network_serving_manifest_generation_id_seq"}


class RegistryManagementPermissionError(ValueError):
    """Registry configuration or migrated metadata shape is invalid."""


def _permission_scope(connection, api_role, owner_role, control_schema):
    if not connection.is_in_transaction():
        raise RegistryManagementPermissionError("Permission installation requires a caller-owned transaction")
    for role in (api_role, owner_role):
        _identifier(role)
    if api_role == owner_role:
        raise RegistryManagementPermissionError("Draft role must differ from the metadata owner")
    return _control_namespace(control_schema)[1:-1]


@asynccontextmanager
async def _administrative_role(connection, role):
    """Apply an existing role and preserve the original operation error on restoration."""
    caller = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"SET LOCAL ROLE {_identifier(role)}")
    try:
        yield
    except BaseException as primary_error:
        try:
            await connection.execute(f"SET LOCAL ROLE {caller}")
        except BaseException:
            primary_error.add_note("Registry administrative role restoration was incomplete")
        raise
    else:
        await connection.execute(f"SET LOCAL ROLE {caller}")


async def _catalog(connection, namespace):
    """Resolve the complete fixed metadata family by schema and relation names."""
    _identifier(namespace)
    tables = await connection.fetch(
        """SELECT c.relname,c.relkind::text,c.relpersistence::text,
        ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
              AND NOT a.attisdropped ORDER BY a.attnum) AS columns
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.relname""",
        namespace,
        sorted(_TABLES),
    )
    if (
        len(tables) != len(_TABLES)
        or {table["relname"] for table in tables} != set(_TABLES)
        or any(
            table["relkind"] != "r"
            or table["relpersistence"] != "p"
            or tuple(table["columns"]) != _TABLES[table["relname"]]
            for table in tables
        )
    ):
        raise RegistryManagementPermissionError("Explicit migrated registry tables are incomplete")
    sequences = await connection.fetch(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relkind='S' AND c.relname=ANY($2::text[]) ORDER BY c.relname",
        namespace,
        sorted(_SEQUENCES),
    )
    if len(sequences) != len(_SEQUENCES) or {sequence["relname"] for sequence in sequences} != _SEQUENCES:
        raise RegistryManagementPermissionError("Native registry identity sequences are incomplete")
    return (
        [{"relname": table["relname"], "columns": list(table["columns"])} for table in tables],
        [{"relname": sequence["relname"]} for sequence in sequences],
    )


async def _aca_controls(connection, namespace):
    """Include the existing optional inbox using its fixed data-column contract."""
    from db.registry_aca_source_notification import INSERT_COLUMNS, TABLE

    relation = await connection.fetchrow(
        "SELECT c.relname,c.relkind::text,c.relpersistence::text,"
        "ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 "
        "AND NOT a.attisdropped ORDER BY a.attnum) AS columns "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",
        namespace,
        TABLE,
    )
    if relation is None:
        return []
    if (
        relation["relkind"] != "r"
        or relation["relpersistence"] != "p"
        or tuple(relation["columns"]) != (*INSERT_COLUMNS, "created_at")
    ):
        raise RegistryManagementPermissionError("Accepted ACA inbox columns differ")
    return [{"relname": relation["relname"], "columns": list(relation["columns"])}]


async def _existing_readers(connection, namespace, names, *, include_writers=False):
    """Retain existing SELECT recipients during the administrative namespace move."""
    readers = await connection.fetch(
        "SELECT DISTINCT r.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a "
        "JOIN pg_roles r ON r.oid=a.grantee WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) "
        "AND a.grantee<>c.relowner AND ($3::boolean OR a.privilege_type='SELECT') ORDER BY r.rolname",
        namespace,
        sorted(names),
        include_writers,
    )
    if include_writers and len(readers) > 128:
        raise RegistryManagementPermissionError("Registry reader list exceeds its operational bound")
    return [reader["rolname"] for reader in readers]


def _installation_receipt(namespace, api_role, owner_role, tables, sequences):
    return {
        "component": "registry_management_permissions",
        "revision": 1,
        "schema_name": namespace,
        "api_role": api_role,
        "owner_role": owner_role,
        "tables": [table["relname"] for table in tables],
        "sequences": [sequence["relname"] for sequence in sequences],
    }


async def install_registry_office_recipe_permissions(connection, *, api_role, owner_role, control_schema=None):
    """Grant only the binding recipe columns through the existing administrator route."""
    namespace = _permission_scope(connection, api_role, owner_role, control_schema)
    async with connection.transaction():
        tables, sequences = await _catalog(connection, namespace)
        async with _administrative_role(connection, owner_role):
            relation = f"{_identifier(namespace)}.registry_network_binding"
            await connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
            await connection.execute(
                f"GRANT INSERT(office_recipe_json),UPDATE(office_recipe_json) ON {relation} TO {_identifier(api_role)}"
            )
        return _installation_receipt(namespace, api_role, owner_role, tables, sequences)


async def install_registry_management_permissions(connection, *, api_role, owner_role, control_schema=None):
    """Apply the existing draft grants atomically using PostgreSQL administrator rights."""
    namespace = _permission_scope(connection, api_role, owner_role, control_schema)
    async with connection.transaction():
        tables, sequences = await _catalog(connection, namespace)
        controls = await _aca_controls(connection, namespace)
        for table in tables + controls:
            await connection.execute(
                f"LOCK TABLE {_identifier(namespace)}.{_identifier(table['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
        await _catalog(connection, namespace)
        for table in tables + controls:
            await connection.execute(
                f"ALTER TABLE {_identifier(namespace)}.{_identifier(table['relname'])} OWNER TO {_identifier(owner_role)}"
            )
        await connection.execute(f"REVOKE {_identifier(owner_role)} FROM {_identifier(api_role)}")
        await connection.execute(f"REVOKE ALL ON SCHEMA {_identifier(namespace)} FROM {_identifier(api_role)} CASCADE")
        await connection.execute(f"REVOKE CREATE ON SCHEMA {_identifier(namespace)} FROM PUBLIC")
        await connection.execute(
            f"GRANT USAGE ON SCHEMA {_identifier(namespace)} TO {_identifier(api_role)},{_identifier(owner_role)}"
        )
        async with _administrative_role(connection, owner_role):
            await _grant_drafts(connection, namespace, api_role, tables, sequences)
        return _installation_receipt(namespace, api_role, owner_role, tables, sequences)


async def _grant_drafts(connection, namespace, api_role, tables, sequences):
    """Remove direct write bypasses and grant the existing draft-column policy."""
    for relation in tables + sequences:
        await _grant_draft_relation(connection, namespace, api_role, relation)
    await connection.execute(
        f"GRANT USAGE ON SEQUENCE {_identifier(namespace)}.network_registry_identity_network_id_seq TO {_identifier(api_role)}"
    )


async def _grant_draft_relation(connection, namespace, api_role, relation):
    qualified = f"{_identifier(namespace)}.{_identifier(relation['relname'])}"
    kind = "SEQUENCE" if relation["relname"] in _SEQUENCES else "TABLE"
    await connection.execute(f"REVOKE ALL ON {kind} {qualified} FROM PUBLIC,{_identifier(api_role)} CASCADE")
    if kind == "SEQUENCE":
        return
    columns = ",".join(_identifier(column) for column in relation["columns"])
    await connection.execute(f"REVOKE ALL({columns}) ON {qualified} FROM PUBLIC,{_identifier(api_role)} CASCADE")
    await connection.execute(f"GRANT SELECT ON {qualified} TO {_identifier(api_role)}")
    for privilege, policy in (("INSERT", _INSERT_COLUMNS), ("UPDATE", _UPDATE_COLUMNS)):
        if relation["relname"] in policy:
            columns = ",".join(_identifier(column) for column in policy[relation["relname"]])
            await connection.execute(f"GRANT {privilege}({columns}) ON {qualified} TO {_identifier(api_role)}")


async def assume_registry_source_owner(connection, owner_role, *, control_schema=None):
    """Apply the configured administrative role under PostgreSQL's existing role grants."""
    if not connection.is_in_transaction():
        raise RegistryManagementPermissionError("Source ownership requires a caller transaction")
    _control_namespace(control_schema)
    await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")


async def _lock_empty_registry(connection, source_schema, tables, sequences, controls=()):
    """Preserve the existing empty-data precondition under bounded relation locks."""
    namespace = _identifier(source_schema)
    for table in [*tables, *controls]:
        await connection.execute(
            f"LOCK TABLE {namespace}.{_identifier(table['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
    await _catalog(connection, source_schema)
    for table in tables:
        name = table["relname"]
        permitted = {
            "registry_revision_control": "id=1 AND draft_revision=0 AND approved_revision=0",
            "network_serving_control": "id=1 AND generation_id IS NULL",
        }.get(name, "false")
        if await connection.fetchval(
            f"SELECT EXISTS(SELECT 1 FROM {namespace}.{_identifier(name)} WHERE NOT ({permitted}))"
        ):
            raise RegistryManagementPermissionError(
                "Registry contains retained data; a retained-data migration is required"
            )


async def _move_registry_objects(connection, source_schema, target_schema, owner_role, tables, sequences, controls=()):
    """Move the enumerated empty metadata objects using their qualified names."""
    old_namespace, new_namespace = _identifier(source_schema), _identifier(target_schema)
    await connection.execute(f"CREATE SCHEMA {new_namespace} AUTHORIZATION {_identifier(owner_role)}")
    for table in [*tables, *controls]:
        await connection.execute(
            f"ALTER TABLE {old_namespace}.{_identifier(table['relname'])} SET SCHEMA {new_namespace}"
        )
    for sequence in sequences:
        qualified = f"{old_namespace}.{_identifier(sequence['relname'])}"
        if await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", qualified):
            await connection.execute(f"ALTER SEQUENCE {qualified} SET SCHEMA {new_namespace}")
    await _catalog(connection, target_schema)


async def _read_view_catalog(connection, source_schema):
    """Resolve compatibility projections and their target names without physical identifiers."""
    return await connection.fetch(
        """SELECT c.relname,c.relkind::text,c.relpersistence::text,
        ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
              AND NOT a.attisdropped ORDER BY a.attnum) AS columns,
        (SELECT jsonb_agg(DISTINCT jsonb_build_object('schema',target_schema.nspname,'name',target.relname))
         FROM pg_rewrite rewrite JOIN pg_depend dependency ON dependency.objid=rewrite.oid
         AND dependency.classid='pg_rewrite'::regclass AND dependency.refclassid='pg_class'::regclass
         JOIN pg_class target ON target.oid=dependency.refobjid
         JOIN pg_namespace target_schema ON target_schema.oid=target.relnamespace
         WHERE rewrite.ev_class=c.oid AND target.oid<>c.oid) AS targets
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.relname""",
        source_schema,
        sorted(set(_TABLES) | _SEQUENCES),
    )


async def _registry_read_views(connection, source_schema, target_schema):
    """Select only the existing complete compatibility views for this named target."""
    import json

    views = await _read_view_catalog(connection, source_schema)
    if len(views) != len(_TABLES) or {view["relname"] for view in views} != set(_TABLES):
        raise RegistryManagementPermissionError("Compatibility view inventory is incomplete or overlaps")
    for view in views:
        name = view["relname"]
        targets = json.loads(view["targets"]) if isinstance(view["targets"], str) else view["targets"]
        if (
            view["relkind"] != "v"
            or view["relpersistence"] != "p"
            or tuple(view["columns"]) != _TABLES[name]
            or targets != [{"schema": target_schema, "name": name}]
        ):
            raise RegistryManagementPermissionError("Compatibility view data projection differs")
    return [view["relname"] for view in views]


async def _create_registry_read_views(connection, source_schema, target_schema, api_role, owner_role):
    """Create read-only compatibility projections in the same namespace-move transaction."""
    async with _administrative_role(connection, owner_role):
        for name, columns in _TABLES.items():
            qualified = f"{_identifier(source_schema)}.{_identifier(name)}"
            projection = ",".join(_identifier(column) for column in columns)
            await connection.execute(
                f"CREATE VIEW {qualified} WITH (security_invoker=true) AS SELECT {projection} "
                f"FROM {_identifier(target_schema)}.{_identifier(name)}"
            )
            await connection.execute(f"REVOKE ALL ON {qualified} FROM PUBLIC,{_identifier(api_role)}")
            await connection.execute(f"GRANT SELECT ON {qualified} TO {_identifier(api_role)}")


async def provision_empty_registry_namespace(connection, *, source_schema, target_schema, api_role, owner_role):
    """Move a complete empty registry with atomic read bridges for existing consumers."""
    _permission_scope(connection, api_role, owner_role, source_schema)
    _identifier(target_schema)
    if source_schema == target_schema:
        raise RegistryManagementPermissionError("Registry provisioning requires a separate namespace")
    if await connection.fetchval("SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=$1)", target_schema):
        if await _aca_controls(connection, source_schema):
            raise RegistryManagementPermissionError("Accepted ACA inbox conflicts with the existing target namespace")
        tables, sequences = await _catalog(connection, target_schema)
        if await _read_view_catalog(connection, source_schema):
            await _registry_read_views(connection, source_schema, target_schema)
        return _installation_receipt(target_schema, api_role, owner_role, tables, sequences)
    async with connection.transaction():
        controls = await _aca_controls(connection, source_schema)
        tables, sequences = await _catalog(connection, source_schema)
        await _lock_empty_registry(connection, source_schema, tables, sequences, controls)
        readers = await _existing_readers(connection, source_schema, set(_PTG_TABLES))
        if controls:
            readers = sorted(
                set(readers)
                | set(
                    await _existing_readers(
                        connection, source_schema, {control["relname"] for control in controls}, include_writers=True
                    )
                )
            )
        await _move_registry_objects(connection, source_schema, target_schema, owner_role, tables, sequences, controls)
        receipt = await install_registry_management_permissions(
            connection, api_role=api_role, owner_role=owner_role, control_schema=target_schema
        )
        for role in readers:
            await connection.execute(f"GRANT USAGE ON SCHEMA {_identifier(target_schema)} TO {_identifier(role)}")
        await _create_registry_read_views(connection, source_schema, target_schema, api_role, owner_role)
    return receipt


async def retire_registry_read_views(connection, *, source_schema, target_schema, api_role, owner_role):
    """Drop only the named compatibility projections with native RESTRICT dependency safety."""
    _permission_scope(connection, api_role, owner_role, source_schema)
    _identifier(target_schema)
    if source_schema == target_schema:
        raise RegistryManagementPermissionError("Registry retirement requires a separate namespace")
    if await _aca_controls(connection, source_schema):
        raise RegistryManagementPermissionError("Accepted ACA inbox must move before compatibility retirement")
    await _catalog(connection, target_schema)
    async with connection.transaction():
        views = await _registry_read_views(connection, source_schema, target_schema)
        async with _administrative_role(connection, owner_role):
            for name in views:
                await connection.execute(f"DROP VIEW {_identifier(source_schema)}.{_identifier(name)} RESTRICT")
    return {
        "component": "registry_read_views_retired",
        "revision": 1,
        "retired_views": len(views),
        "source_schema": source_schema,
        "target_schema": target_schema,
        "views": views,
    }


def run_command(arguments=None):
    """Provision the explicit empty registry through existing operator database credentials."""
    import asyncio
    import json
    import sys

    from db.connection import Database
    from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
    from process.registry_source_import import _ReceiptArgumentParser

    parser = _ReceiptArgumentParser(description=run_command.__doc__, allow_abbrev=False)
    for field in ("source-schema", "target-schema", "api-role", "owner-role"):
        parser.add_argument("--" + field, required=True)
    parser.add_argument("--retire-read-views", action="store_true")
    parsed = parser.parse_args(arguments)
    retire = parsed.retire_read_views
    del parsed.retire_read_views

    async def provision():
        """Run the selected custody operation through existing trusted operator credentials."""
        database = Database()
        with _receipt_only_database_output(database):
            try:
                await database.connect()
                _set_engine_echo(database, False)
                async with database.acquire_driver() as connection, connection.transaction():
                    operation = retire_registry_read_views if retire else provision_empty_registry_namespace
                    return await operation(connection, **vars(parsed))
            finally:
                await database.disconnect()

    try:
        receipt = asyncio.run(provision())
    except Exception:
        print('{"status":"error","code":"registry_provisioning_failed"}', file=sys.stderr)
        return 1
    print(json.dumps({"status": "ok", **receipt}, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
