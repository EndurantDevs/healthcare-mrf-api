# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Precise draft privileges on explicit protected registry metadata objects."""

from db.models.company_group_registry import CompanyGroupRegistry
from db.models.company_registry import CompanyRegistry, HIOSIssuerRegistry
from db.models.company_registry_assertions import CompanyRegistryIdentifierAssertion, CompanyRegistryRoleAssertion
from db.models.company_registry_links import CompanyRegistryLinks, RegistryCompanyLinkBatch
from db.models.manual_directory_registry import (
    ManualLocationRegistry,
    ManualProviderLocationBinding,
    ManualProviderRegistry,
)
from db.models.network_membership_draft import NetworkMembershipDraft
from db.models.network_registry import NetworkRegistryAlias, NetworkRegistryIdentity, NetworkRegistryRecord
from db.models.network_serving import (
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
    RegistrySourceObservation,
    RegistrySourceSnapshot,
)
from db.models.registry_network_binding import RegistryNetworkBinding, RegistryNetworkBindingBatch
from db.models.registry_publication_request import RegistryPublicationRequest
from db.models.registry_revision import RegistryRecordHistory, RegistryRevisionControl
from db.models.registry_site_binding import RegistrySiteBinding
from process.network_address_projection import _identifier
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_writer_closure import _NATIVE_RUNTIME_ROLE_UNSAFE_SQL, _protected_owner

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
    HIOSIssuerRegistry,
    NetworkRegistryIdentity,
    NetworkRegistryAlias,
    RegistryRevisionControl,
    RegistryRecordHistory,
    NetworkMembershipCandidate,
    NetworkMembershipBatch,
    NetworkServingManifest,
    NetworkServingControl,
    RegistrySourceSnapshot,
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
_TABLES = {model.__tablename__: tuple(model.__table__.columns.keys()) for model in _MODELS}
_INSERT_COLUMNS = {
    model.__tablename__: tuple(column for column in model.__table__.columns.keys() if column != "created_at")
    for model in _HEAD_MODELS
}
_INSERT_COLUMNS.update(
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
        for model in (CompanyRegistryRoleAssertion, CompanyRegistryIdentifierAssertion)
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
_UPDATE_COLUMNS["registry_network_binding"] = ("network_id", "evidence_id", "evidence_sha256", "archived", "revision")
_SEQUENCES = {"network_registry_identity_network_id_seq", "network_serving_manifest_generation_id_seq"}


class RegistryManagementPermissionError(ValueError):
    """Native roles, ownership or effective draft privileges are unsafe."""


async def _roles(connection, namespace, api_role, owner_role):
    _identifier(api_role)
    _identifier(owner_role)
    if api_role == owner_role:
        raise RegistryManagementPermissionError("Draft role must differ from the protected owner")
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    if not _protected_owner(owner):
        raise RegistryManagementPermissionError("Owner must be an existing protected native role")
    unsafe = await connection.fetchval(
        f"""SELECT EXISTS(SELECT 1 FROM (SELECT $1::text AS name) configured
        LEFT JOIN pg_roles r ON r.rolname=configured.name CROSS JOIN (SELECT $2::oid owner_oid) s
        WHERE {_NATIVE_RUNTIME_ROLE_UNSAFE_SQL} OR r.rolbypassrls OR r.rolreplication
        OR pg_has_role(r.oid,'pg_write_all_data','MEMBER'))""",
        api_role,
        owner["oid"],
    )
    schema = await connection.fetchrow("SELECT oid,nspowner FROM pg_namespace WHERE nspname=$1", namespace)
    if unsafe or schema is None or schema["nspowner"] != owner["oid"]:
        raise RegistryManagementPermissionError("Draft role or protected schema ownership is unsafe")
    if await connection.fetchval("SELECT has_schema_privilege($1,$2::oid,'CREATE')", api_role, schema["oid"]):
        raise RegistryManagementPermissionError("Draft role must not create objects in the protected schema")
    return dict(schema)


async def _catalog(connection, schema_oid):
    tables = await connection.fetch(
        """SELECT c.oid::bigint,c.relname,c.relowner,c.relkind,c.relpersistence,
        ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
              AND NOT a.attisdropped ORDER BY a.attnum) AS columns
        FROM pg_class c WHERE c.relnamespace=$1::oid AND c.relname=ANY($2::text[]) ORDER BY c.relname""",
        schema_oid,
        sorted(_TABLES),
    )
    if len(tables) != len(_TABLES) or any(
        table["relkind"] != b"r"
        or table["relpersistence"] != b"p"
        or tuple(table["columns"]) != _TABLES[table["relname"]]
        for table in tables
    ):
        raise RegistryManagementPermissionError("Explicit migrated registry tables are incomplete")
    sequences = await connection.fetch(
        "SELECT oid::bigint,relname,relowner FROM pg_class WHERE relnamespace=$1::oid AND relkind='S' AND relname=ANY($2::text[]) ORDER BY relname",
        schema_oid,
        sorted(_SEQUENCES),
    )
    if len(sequences) != len(_SEQUENCES):
        raise RegistryManagementPermissionError("Native registry identity sequences are incomplete")
    return [dict(table) for table in tables], [dict(sequence) for sequence in sequences]


async def _acls(connection, schema_oid):
    return await connection.fetch(
        """SELECT c.relname,NULL::text AS column,a.grantee,a.privilege_type,a.is_grantable
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,
          acldefault(CASE WHEN c.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,c.relowner))) a
        WHERE c.relnamespace=$1::oid AND c.relname=ANY($2::text[])
        UNION ALL SELECT c.relname,att.attname,a.grantee,a.privilege_type,a.is_grantable
        FROM pg_class c JOIN pg_attribute att ON att.attrelid=c.oid
        CROSS JOIN LATERAL aclexplode(att.attacl) a
        WHERE c.relnamespace=$1::oid AND c.relname=ANY($2::text[]) AND att.attnum>0 AND NOT att.attisdropped""",
        schema_oid,
        sorted(set(_TABLES) | _SEQUENCES),
    )


async def _reject_inherited_grants(connection, schema_oid, api_role):
    api_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", api_role)
    if await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a
        JOIN pg_roles grantee ON grantee.oid=a.grantee WHERE n.oid=$1::oid AND grantee.oid<>$2::oid
        AND pg_has_role($3::name,grantee.oid,'MEMBER') AND a.is_grantable)""",
        schema_oid,
        api_oid,
        api_role,
    ):
        raise RegistryManagementPermissionError("Inherited schema grant options must be removed explicitly")
    unsafe_grantees = {
        acl["grantee"]
        for acl in await _acls(connection, schema_oid)
        if acl["grantee"] not in (0, api_oid) and (acl["is_grantable"] or acl["privilege_type"] != "SELECT")
    }
    for grantee in unsafe_grantees:
        if await connection.fetchval("SELECT pg_has_role($1::name,$2::oid,'MEMBER')", api_role, grantee):
            raise RegistryManagementPermissionError("Inherited registry write grants must be removed explicitly")


async def _check_direct_grants(connection, schema_oid, api_role):
    """Reject PUBLIC and grantable or excessive direct runtime ACLs."""
    api_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", api_role)
    for acl in await _acls(connection, schema_oid):
        if acl["grantee"] not in (0, api_oid):
            continue
        is_allowed = (
            acl["grantee"] == api_oid
            and not acl["is_grantable"]
            and (
                (acl["privilege_type"] == "SELECT" and acl["relname"] in _TABLES)
                or (acl["privilege_type"] == "USAGE" and acl["relname"] == "network_registry_identity_network_id_seq")
                or (acl["column"] in _INSERT_COLUMNS.get(acl["relname"], ()) and acl["privilege_type"] == "INSERT")
                or (acl["column"] in _UPDATE_COLUMNS.get(acl["relname"], ()) and acl["privilege_type"] == "UPDATE")
            )
        )
        if not is_allowed:
            raise RegistryManagementPermissionError("Registry direct or PUBLIC grants are unsafe")


async def _verify(connection, namespace, api_role, owner_role):
    """Compare native ownership and effective privileges to the explicit draft policy."""
    schema = await _roles(connection, namespace, api_role, owner_role)
    if not await connection.fetchval("SELECT has_schema_privilege($1,$2::oid,'USAGE')", api_role, schema["oid"]):
        raise RegistryManagementPermissionError("Registry schema reads are unavailable")
    if await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a
        JOIN pg_roles grantee ON grantee.oid=a.grantee
        WHERE n.oid=$1::oid AND pg_has_role($2::name,grantee.oid,'MEMBER') AND a.is_grantable)""",
        schema["oid"],
        api_role,
    ):
        raise RegistryManagementPermissionError("Draft schema privileges must not carry grant options")
    tables, sequences = await _catalog(connection, schema["oid"])
    owner_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", owner_role)
    if any(relation["relowner"] != owner_oid for relation in tables + sequences):
        raise RegistryManagementPermissionError("Registry object ownership is not protected")
    await _check_direct_grants(connection, schema["oid"], api_role)
    await _reject_inherited_grants(connection, schema["oid"], api_role)
    for table in tables:
        for privilege, permitted in (("INSERT", _INSERT_COLUMNS), ("UPDATE", _UPDATE_COLUMNS), ("REFERENCES", {})):
            actual = await connection.fetch(
                """SELECT attname,has_column_privilege($1,$2::oid,attname,$3) AS allowed
                FROM pg_attribute WHERE attrelid=$2::oid AND attnum>0 AND NOT attisdropped""",
                api_role,
                table["oid"],
                privilege,
            )
            if {column["attname"] for column in actual if column["allowed"]} != set(
                permitted.get(table["relname"], ())
            ):
                raise RegistryManagementPermissionError(
                    "Effective registry column privileges differ from the draft policy"
                )
        if not await connection.fetchval("SELECT has_table_privilege($1,$2::oid,'SELECT')", api_role, table["oid"]):
            raise RegistryManagementPermissionError("Registry metadata reads are unavailable")
        if await connection.fetchval(
            "SELECT has_table_privilege($1,$2::oid,'DELETE,TRUNCATE,TRIGGER,MAINTAIN')", api_role, table["oid"]
        ):
            raise RegistryManagementPermissionError("Effective registry table privileges exceed the draft policy")
    for sequence in sequences:
        is_permitted = sequence["relname"] == "network_registry_identity_network_id_seq"
        if await connection.fetchval(
            "SELECT has_sequence_privilege($1,$2::oid,'USAGE')", api_role, sequence["oid"]
        ) != is_permitted or await connection.fetchval(
            "SELECT has_sequence_privilege($1,$2::oid,'SELECT,UPDATE')", api_role, sequence["oid"]
        ):
            raise RegistryManagementPermissionError("Effective registry sequence privileges exceed allocation")
    return {
        "component": "registry_management_permissions",
        "revision": 1,
        "schema_name": namespace,
        "schema_oid": schema["oid"],
        "api_role": api_role,
        "owner_role": owner_role,
        "table_oids": {table["relname"]: table["oid"] for table in tables},
        "sequence_oids": {sequence["relname"]: sequence["oid"] for sequence in sequences},
    }


async def verify_registry_management_permissions(connection, *, api_role, owner_role, control_schema=None):
    """Read exact native owners, ACLs and effective column privileges without DDL."""
    return await _verify(connection, _control_namespace(control_schema)[1:-1], api_role, owner_role)


async def install_registry_management_permissions(connection, *, api_role, owner_role, control_schema=None):
    """Protect explicit new registry objects in the caller transaction and a savepoint."""
    if not connection.is_in_transaction():
        raise RegistryManagementPermissionError("Permission installation requires a caller-owned transaction")
    namespace = _control_namespace(control_schema)[1:-1]
    async with connection.transaction():
        schema = await _roles(connection, namespace, api_role, owner_role)
        if not await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", owner_role):
            raise RegistryManagementPermissionError("Installer must be able to assume the protected owner")
        tables, sequences = await _catalog(connection, schema["oid"])
        await _reject_inherited_grants(connection, schema["oid"], api_role)
        try:
            existing = await _verify(connection, namespace, api_role, owner_role)
        except RegistryManagementPermissionError:
            existing = None
        if existing is not None:
            return existing
        for table in tables:
            await connection.execute(
                f"LOCK TABLE {_identifier(namespace)}.{_identifier(table['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
        if await _catalog(connection, schema["oid"]) != (tables, sequences):
            raise RegistryManagementPermissionError("Native registry objects changed while locking")
        for table in tables:
            await connection.execute(
                f"ALTER TABLE {_identifier(namespace)}.{_identifier(table['relname'])} OWNER TO {_identifier(owner_role)}"
            )
        caller = await connection.fetchval("SELECT quote_ident(current_user)")
        await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
        await _grant_drafts(connection, namespace, api_role, tables, sequences)
        await connection.execute(f"SET LOCAL ROLE {caller}")
        return await _verify(connection, namespace, api_role, owner_role)


async def _grant_drafts(connection, namespace, api_role, tables, sequences):
    """Revoke direct bypasses and grant only bounded draft columns as the protected owner."""
    await connection.execute(f"REVOKE ALL ON SCHEMA {_identifier(namespace)} FROM {_identifier(api_role)} CASCADE")
    for relation in tables + sequences:
        qualified = f"{_identifier(namespace)}.{_identifier(relation['relname'])}"
        kind = "SEQUENCE" if relation["relname"] in _SEQUENCES else "TABLE"
        await connection.execute(f"REVOKE ALL ON {kind} {qualified} FROM PUBLIC,{_identifier(api_role)} CASCADE")
        if kind == "SEQUENCE":
            continue
        columns = ",".join(_identifier(column) for column in relation["columns"])
        await connection.execute(f"REVOKE ALL({columns}) ON {qualified} FROM PUBLIC,{_identifier(api_role)} CASCADE")
        await connection.execute(f"GRANT SELECT ON {qualified} TO {_identifier(api_role)}")
        for privilege, policy in (("INSERT", _INSERT_COLUMNS), ("UPDATE", _UPDATE_COLUMNS)):
            if relation["relname"] in policy:
                columns = ",".join(_identifier(column) for column in policy[relation["relname"]])
                await connection.execute(f"GRANT {privilege}({columns}) ON {qualified} TO {_identifier(api_role)}")
    await connection.execute(f"GRANT USAGE ON SCHEMA {_identifier(namespace)} TO {_identifier(api_role)}")
    await connection.execute(
        f"GRANT USAGE ON SEQUENCE {_identifier(namespace)}.network_registry_identity_network_id_seq TO {_identifier(api_role)}"
    )
