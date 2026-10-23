# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Precise draft privileges on explicit protected registry metadata objects."""

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
_TABLES = {model.__tablename__: tuple(model.__table__.columns.keys()) for model in _MODELS}
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


async def _ptg_readers(connection, schema_oid, owner_role, api_role):
    """Capture safe direct PTG readers already admitted by the old namespace."""
    return await connection.fetch(
        f"""SELECT DISTINCT r.rolname FROM pg_class c
        CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        JOIN pg_roles r ON r.oid=a.grantee
        CROSS JOIN (SELECT oid AS owner_oid FROM pg_roles WHERE rolname=$2) s
        WHERE c.relnamespace=$1::oid AND c.relname=ANY($4::text[])
        AND r.oid<>s.owner_oid AND r.rolname<>$3 AND a.privilege_type='SELECT' AND NOT a.is_grantable
        AND has_schema_privilege(r.oid,$1::oid,'USAGE')
        AND NOT ({_NATIVE_RUNTIME_ROLE_UNSAFE_SQL} OR r.rolreplication OR r.rolbypassrls)
        ORDER BY r.rolname""",
        schema_oid,
        owner_role,
        api_role,
        sorted(_PTG_TABLES),
    )


async def _verify_table_privileges(connection, tables, api_role, *, office_recipe_pending=False):
    """Require the exact API column policy and read-only retained tables."""
    for table in tables:
        for privilege, permitted in (("INSERT", _INSERT_COLUMNS), ("UPDATE", _UPDATE_COLUMNS), ("REFERENCES", {})):
            actual = await connection.fetch(
                """SELECT attname,has_column_privilege($1,$2::oid,attname,$3) AS allowed
                FROM pg_attribute WHERE attrelid=$2::oid AND attnum>0 AND NOT attisdropped""",
                api_role,
                table["oid"],
                privilege,
            )
            expected_columns = set(permitted.get(table["relname"], ()))
            if office_recipe_pending and table["relname"] == "registry_network_binding":
                expected_columns.discard("office_recipe_json")
            if {column["attname"] for column in actual if column["allowed"]} != expected_columns:
                raise RegistryManagementPermissionError(
                    "Effective registry column privileges differ from the draft policy"
                )
        if not await connection.fetchval("SELECT has_table_privilege($1,$2::oid,'SELECT')", api_role, table["oid"]):
            raise RegistryManagementPermissionError("Registry metadata reads are unavailable")
        if await connection.fetchval(
            "SELECT has_table_privilege($1,$2::oid,'DELETE,TRUNCATE,TRIGGER,MAINTAIN')", api_role, table["oid"]
        ):
            raise RegistryManagementPermissionError("Effective registry table privileges exceed the draft policy")


async def _aca_controls(connection, namespace):
    """Keep the accepted inbox in custody without granting draft metadata access."""
    from db.registry_aca_source_notification import INSERT_COLUMNS, TABLE

    relation = await connection.fetchrow(
        "SELECT c.oid::bigint,c.relname,c.relowner,c.relkind::text,c.relpersistence::text,"
        "h.relowner AS head_owner,o.rolcanlogin,o.rolsuper,o.rolcreatedb,o.rolcreaterole,"
        "o.rolreplication,o.rolbypassrls,c.relhastriggers,c.relhasrules,c.relrowsecurity,c.relforcerowsecurity,"
        "ARRAY(SELECT a.attname::text FROM pg_attribute a "
        "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum) AS columns "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_roles o ON o.oid=c.relowner LEFT JOIN pg_class h ON h.relnamespace=n.oid "
        "AND h.relname='network_serving_control' WHERE n.nspname=$1 AND c.relname=$2",
        namespace,
        TABLE,
    )
    if relation is None:
        return []
    if (
        relation["relkind"] != "r"
        or relation["relpersistence"] != "p"
        or relation["relowner"] != relation["head_owner"]
        or any(relation[name] for name in ("relhastriggers", "relhasrules", "relrowsecurity", "relforcerowsecurity"))
        or tuple(relation["columns"]) != (*INSERT_COLUMNS, "created_at")
    ):
        raise RegistryManagementPermissionError("Accepted ACA inbox custody differs")
    return [dict(relation)]


async def _aca_readers(connection, namespace, controls):
    """Preserve only safe existing native inbox grantees during a namespace move."""
    if not controls:
        return []
    rows = await connection.fetch(
        f"""WITH scope AS (SELECT $2::oid AS owner_oid), acl AS (
        SELECT a.grantee,a.privilege_type,a.is_grantable,NULL::text AS column_name
        FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE c.oid=$1::oid
        UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,att.attname
        FROM pg_attribute att CROSS JOIN LATERAL aclexplode(att.attacl) a
        WHERE att.attrelid=$1::oid AND att.attnum>0 AND NOT att.attisdropped
        ), roles AS (SELECT DISTINCT grantee FROM acl WHERE grantee<>$2::oid)
        SELECT configured.grantee,r.rolname,
        NOT ({_NATIVE_RUNTIME_ROLE_UNSAFE_SQL}) AND NOT r.rolcreatedb AND NOT r.rolreplication
        AND NOT r.rolbypassrls AND NOT has_parameter_privilege(r.oid,'session_replication_role','SET')
        AND has_schema_privilege(r.oid,$3,'USAGE') AND NOT has_schema_privilege(r.oid,$3,'CREATE')
        AND (SELECT count(DISTINCT grantee) FROM acl WHERE privilege_type='INSERT' AND grantee<>s.owner_oid)<=1
        AND NOT EXISTS(SELECT 1 FROM acl a WHERE a.grantee=configured.grantee AND
          (a.is_grantable OR NOT (a.privilege_type='SELECT' OR
           (a.privilege_type='INSERT' AND a.column_name=ANY($4::text[]))))) AS safe
        FROM roles configured LEFT JOIN pg_roles r ON r.oid=configured.grantee CROSS JOIN scope s""",
        controls[0]["oid"],
        controls[0]["relowner"],
        namespace,
        list(controls[0]["columns"][:-1]),
    )
    if len(rows) > 128 or any(row["safe"] is not True for row in rows):
        raise RegistryManagementPermissionError("Accepted ACA inbox grantees are unsafe")
    return [dict(row) for row in rows]


async def _verify(connection, namespace, api_role, owner_role, *, office_recipe_pending=False):
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
    controls = await _aca_controls(connection, namespace)
    tables, sequences = await _catalog(connection, schema["oid"])
    owner_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", owner_role)
    if any(relation["relowner"] != owner_oid for relation in tables + sequences + controls):
        raise RegistryManagementPermissionError("Registry object ownership is not protected")
    await _check_direct_grants(connection, schema["oid"], api_role)
    await _reject_inherited_grants(connection, schema["oid"], api_role)
    await _verify_table_privileges(connection, tables, api_role, office_recipe_pending=office_recipe_pending)
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


async def install_registry_office_recipe_permissions(connection, *, api_role, owner_role, control_schema=None):
    """Enroll only the new binding column after every existing draft privilege matches."""
    if not connection.is_in_transaction():
        raise RegistryManagementPermissionError("Permission installation requires a caller-owned transaction")
    namespace = _control_namespace(control_schema)[1:-1]
    async with connection.transaction():
        if not await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", owner_role):
            raise RegistryManagementPermissionError("Installer must be able to assume the protected owner")
        try:
            return await _verify(connection, namespace, api_role, owner_role)
        except RegistryManagementPermissionError:
            receipt = await _verify(connection, namespace, api_role, owner_role, office_recipe_pending=True)
        initial_grants = {tuple(grant.values()) for grant in await _acls(connection, receipt["schema_oid"])}
        caller = await connection.fetchval("SELECT quote_ident(current_user)")
        await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
        try:
            relation = f"{_identifier(namespace)}.registry_network_binding"
            await connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
            await _verify(connection, namespace, api_role, owner_role, office_recipe_pending=True)
            await connection.execute(
                f"GRANT INSERT(office_recipe_json),UPDATE(office_recipe_json) ON {relation} TO {_identifier(api_role)}"
            )
        finally:
            await connection.execute(f"SET LOCAL ROLE {caller}")
        updated_grants = {tuple(grant.values()) for grant in await _acls(connection, receipt["schema_oid"])}
        api_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", api_role)
        required_grants = {
            ("registry_network_binding", "office_recipe_json", api_oid, privilege, False)
            for privilege in ("INSERT", "UPDATE")
        }
        if updated_grants != initial_grants | required_grants:
            raise RegistryManagementPermissionError("Office recipe enrollment changed unrelated privileges")
        return await _verify(connection, namespace, api_role, owner_role)


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
        controls = await _aca_controls(connection, namespace)
        await _reject_inherited_grants(connection, schema["oid"], api_role)
        try:
            existing = await _verify(connection, namespace, api_role, owner_role)
        except RegistryManagementPermissionError:
            existing = None
        if existing is not None:
            return existing
        for table in tables + controls:
            await connection.execute(
                f"LOCK TABLE {_identifier(namespace)}.{_identifier(table['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
        if (
            await _catalog(connection, schema["oid"]) != (tables, sequences)
            or await _aca_controls(connection, namespace) != controls
        ):
            raise RegistryManagementPermissionError("Native registry objects changed while locking")
        for table in tables + controls:
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


async def assume_registry_source_owner(connection, owner_role, *, control_schema=None):
    """Assume an existing protected schema owner only for a trusted operator transaction."""
    if not connection.is_in_transaction():
        raise RegistryManagementPermissionError("Source ownership requires a caller transaction")
    _identifier(owner_role)
    namespace = _control_namespace(control_schema)[1:-1]
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    schema_owner = await connection.fetchval("SELECT nspowner FROM pg_namespace WHERE nspname=$1", namespace)
    if not _protected_owner(owner) or schema_owner != owner["oid"]:
        raise RegistryManagementPermissionError("Source role must be the protected registry schema owner")
    if not await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", owner_role):
        raise RegistryManagementPermissionError("Source operator cannot assume the protected owner")
    await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
    await _require_source_custody(connection, namespace)


async def _require_source_custody(connection, namespace):
    from process.registry_source_admission_receipt import _custody

    previous = await connection.fetchval("SELECT current_setting('search_path')")
    await connection.execute("SELECT set_config('search_path','pg_catalog,pg_temp',true)")
    try:
        if await _custody(connection, namespace) is None:
            raise RegistryManagementPermissionError("Registry source ownership and privileges are not closed")
    finally:
        await connection.execute("SELECT set_config('search_path',$1,true)", previous)


async def _provisioning_scope(connection, source_schema, target_schema, api_role, owner_role):
    """Validate identifiers and protected native ownership before any namespace DDL."""
    if not connection.is_in_transaction():
        raise RegistryManagementPermissionError("Registry provisioning requires a caller transaction")
    for name in (source_schema, target_schema, api_role, owner_role):
        _identifier(name)
    if source_schema == target_schema:
        raise RegistryManagementPermissionError("Registry provisioning requires a separate namespace")
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    namespace = await connection.fetchrow("SELECT oid,nspowner FROM pg_namespace WHERE nspname=$1", source_schema)
    if not _protected_owner(owner) or namespace is None:
        raise RegistryManagementPermissionError("Registry provisioning requires existing protected ownership")
    if not await connection.fetchval(
        "SELECT has_function_privilege($1,'pg_catalog.pg_control_system()','EXECUTE')", owner_role
    ):
        raise RegistryManagementPermissionError("Protected owner cannot read the native source custody identity")
    return owner, namespace


async def _require_provisioning_operator(connection, namespace, owner, tables, sequences, api_role):
    """Require existing ownership authority, including protected old-view ownership."""
    owner_oids = {namespace["nspowner"], owner["oid"], *(relation["relowner"] for relation in tables + sequences)}
    for old_owner in owner_oids:
        if not await connection.fetchval(
            "SELECT pg_has_role(current_user,$1::oid,'SET') AND pg_has_role(current_user,$1::oid,'USAGE')", old_owner
        ):
            raise RegistryManagementPermissionError("Operator cannot assume all existing and target registry owners")
    if not await connection.fetchval("SELECT has_database_privilege(current_user,current_database(),'CREATE')"):
        raise RegistryManagementPermissionError("Operator cannot create the registry namespace")
    if not await connection.fetchval(
        "SELECT has_schema_privilege($1::oid,$2::oid,'CREATE') AND has_schema_privilege($1::oid,$2::oid,'USAGE')",
        owner["oid"],
        namespace["oid"],
    ):
        raise RegistryManagementPermissionError("Protected compatibility owner cannot create old-schema views")
    if not await connection.fetchval("SELECT has_schema_privilege($1,$2::oid,'USAGE')", api_role, namespace["oid"]):
        raise RegistryManagementPermissionError("Existing API cannot read the old registry namespace")


async def _lock_empty_registry(connection, source_schema, tables, sequences, controls=()):
    """Lock and reject retained business rows without modifying receipt identities."""
    namespace = _identifier(source_schema)
    for table in [*tables, *controls]:
        await connection.execute(
            f"LOCK TABLE {namespace}.{_identifier(table['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
    schema_oid = await connection.fetchval("SELECT oid FROM pg_namespace WHERE nspname=$1", source_schema)
    if await _catalog(connection, schema_oid) != (tables, sequences) or await _aca_controls(
        connection, source_schema
    ) != list(controls):
        raise RegistryManagementPermissionError("Registry objects changed while locking")
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
    """Move only native enumerated empty objects and preserve their catalog identities."""
    old_namespace, new_namespace = _identifier(source_schema), _identifier(target_schema)
    await connection.execute(f"CREATE SCHEMA {new_namespace} AUTHORIZATION {_identifier(owner_role)}")
    for table in [*tables, *controls]:
        await connection.execute(
            f"ALTER TABLE {old_namespace}.{_identifier(table['relname'])} SET SCHEMA {new_namespace}"
        )
    for sequence in sequences:
        if (
            await connection.fetchval(
                "SELECT to_regclass($1)::oid", f"{old_namespace}.{_identifier(sequence['relname'])}"
            )
            is not None
        ):
            await connection.execute(
                f"ALTER SEQUENCE {old_namespace}.{_identifier(sequence['relname'])} SET SCHEMA {new_namespace}"
            )
    schema_oid = await connection.fetchval("SELECT oid FROM pg_namespace WHERE nspname=$1", target_schema)
    if await _catalog(connection, schema_oid) != (tables, sequences):
        raise RegistryManagementPermissionError("Registry native identities changed during provisioning")


async def _read_view_catalog(connection, source_schema):
    """Read exact scoped view definitions with a deterministic catalog search path."""
    previous = await connection.fetchval("SELECT current_setting('search_path')")
    await connection.execute("SELECT set_config('search_path','pg_catalog,pg_temp',true)")
    try:
        return await connection.fetch(
            """SELECT c.oid::bigint,c.relname,c.relkind::text,c.relowner,c.relpersistence::text,c.reloptions,
            CASE WHEN c.relkind='v' THEN pg_get_viewdef(c.oid,false) END AS definition,
            ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
                  AND NOT a.attisdropped ORDER BY a.attnum) AS columns,
            ARRAY(SELECT DISTINCT d.refobjid::bigint FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid
                  WHERE r.ev_class=c.oid AND d.classid='pg_rewrite'::regclass
                  AND d.refclassid='pg_class'::regclass AND d.refobjid<>c.oid ORDER BY d.refobjid::bigint) AS targets,
            (SELECT count(*) FROM pg_rewrite r WHERE r.ev_class=c.oid) AS rules,
            EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal) AS triggers
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.relname""",
            source_schema,
            sorted(set(_TABLES) | _SEQUENCES),
        )
    finally:
        await connection.execute("SELECT set_config('search_path',$1,true)", previous)


async def _verify_read_view_grants(connection, relation_oid, api_role, owner_oid):
    """Require only protected-owner and non-grantable API reads, including effective ACLs."""
    api_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", api_role)
    grants = await connection.fetch(
        """SELECT a.*,false AS column_grant FROM pg_class c
        CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.oid=$1::oid UNION ALL SELECT a.*,true AS column_grant FROM pg_attribute att
        CROSS JOIN LATERAL aclexplode(att.attacl) a WHERE att.attrelid=$1::oid""",
        relation_oid,
    )
    if any(
        grant["column_grant"]
        or grant["grantee"] != owner_oid
        and (grant["grantee"] != api_oid or grant["privilege_type"] != "SELECT" or grant["is_grantable"])
        for grant in grants
    ):
        raise RegistryManagementPermissionError("Compatibility view grants are unsafe")
    if not await connection.fetchval("SELECT has_table_privilege($1,$2::oid,'SELECT')", api_role, relation_oid):
        raise RegistryManagementPermissionError("Compatibility view reads are unavailable")
    if await connection.fetchval(
        "SELECT has_table_privilege($1,$2::oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
        "OR has_any_column_privilege($1,$2::oid,'INSERT,UPDATE,REFERENCES')",
        api_role,
        relation_oid,
    ):
        raise RegistryManagementPermissionError("Compatibility views must be read only")


async def _verify_registry_read_views(connection, source_schema, target_schema, api_role, owner_role):
    """Reject missing, foreign or altered compatibility objects rather than ignoring overlap."""
    if not await connection.fetchval("SELECT has_schema_privilege($1,$2,'USAGE')", api_role, source_schema):
        raise RegistryManagementPermissionError("Compatibility namespace reads are unavailable")
    catalog = await _read_view_catalog(connection, source_schema)
    if len(catalog) != len(_TABLES) or {relation["relname"] for relation in catalog} != set(_TABLES):
        raise RegistryManagementPermissionError("Compatibility view inventory is incomplete or overlaps")
    owner_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", owner_role)
    for relation in catalog:
        name, columns = relation["relname"], _TABLES[relation["relname"]]
        target_oid = await connection.fetchval(
            "SELECT to_regclass($1)::oid", f"{_identifier(target_schema)}.{_identifier(name)}"
        )
        expected = "SELECT " + ", ".join(columns) + f" FROM {target_schema}.{name};"
        definition = " ".join((relation["definition"] or "").replace('"', "").split())
        if relation["relkind"] != "v" or relation["relpersistence"] != "p":
            raise RegistryManagementPermissionError("Compatibility view relation kind differs")
        if relation["relowner"] != owner_oid:
            raise RegistryManagementPermissionError("Compatibility view protected owner differs")
        if relation["reloptions"] != ["security_invoker=true"]:
            raise RegistryManagementPermissionError("Compatibility view options differ")
        if tuple(relation["columns"]) != columns:
            raise RegistryManagementPermissionError("Compatibility view column order differs")
        if relation["targets"] != [target_oid]:
            raise RegistryManagementPermissionError("Compatibility view target identity differs")
        if relation["rules"] != 1 or relation["triggers"]:
            raise RegistryManagementPermissionError("Compatibility view rules or triggers differ")
        if definition != expected:
            raise RegistryManagementPermissionError("Compatibility view canonical definition differs")
        await _verify_read_view_grants(connection, relation["oid"], api_role, owner_oid)
    return {relation["relname"]: relation["oid"] for relation in catalog}


async def _create_registry_read_views(connection, source_schema, target_schema, api_role, owner_role):
    """Create exact read bridges as the protected owner inside the move transaction."""
    caller = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
    try:
        for name, columns in _TABLES.items():
            qualified = f"{_identifier(source_schema)}.{_identifier(name)}"
            projection = ",".join(_identifier(column) for column in columns)
            await connection.execute(
                f"CREATE VIEW {qualified} WITH (security_invoker=true) AS SELECT {projection} "
                f"FROM {_identifier(target_schema)}.{_identifier(name)}"
            )
            await connection.execute(f"REVOKE ALL ON {qualified} FROM PUBLIC,{_identifier(api_role)}")
            await connection.execute(f"GRANT SELECT ON {qualified} TO {_identifier(api_role)}")
    finally:
        await connection.execute(f"SET LOCAL ROLE {caller}")
    await _verify_registry_read_views(connection, source_schema, target_schema, api_role, owner_role)


async def provision_empty_registry_namespace(connection, *, source_schema, target_schema, api_role, owner_role):
    """Move only a complete zero-row registry, with atomic read bridges for old processes."""
    owner, namespace = await _provisioning_scope(connection, source_schema, target_schema, api_role, owner_role)
    if await connection.fetchval("SELECT oid FROM pg_namespace WHERE nspname=$1", target_schema) is not None:
        if await _aca_controls(connection, source_schema):
            raise RegistryManagementPermissionError("Accepted ACA inbox conflicts with the existing target namespace")
        receipt = await verify_registry_management_permissions(
            connection, api_role=api_role, owner_role=owner_role, control_schema=target_schema
        )
        await _require_source_custody(connection, target_schema)
        if await _read_view_catalog(connection, source_schema):
            await _verify_registry_read_views(connection, source_schema, target_schema, api_role, owner_role)
        return receipt
    controls = await _aca_controls(connection, source_schema)
    tables, sequences = await _catalog(connection, namespace["oid"])
    await _require_provisioning_operator(connection, namespace, owner, tables + controls, sequences, api_role)
    await _lock_empty_registry(connection, source_schema, tables, sequences, controls)
    ptg_readers = await _ptg_readers(connection, namespace["oid"], owner_role, api_role)
    aca_readers = await _aca_readers(connection, source_schema, controls)
    async with connection.transaction():
        await _move_registry_objects(connection, source_schema, target_schema, owner_role, tables, sequences, controls)
        receipt = await install_registry_management_permissions(
            connection, api_role=api_role, owner_role=owner_role, control_schema=target_schema
        )
        for role in ptg_readers + aca_readers:
            await connection.execute(
                f"GRANT USAGE ON SCHEMA {_identifier(target_schema)} TO {_identifier(role['rolname'])}"
            )
        await _require_source_custody(connection, target_schema)
        await _create_registry_read_views(connection, source_schema, target_schema, api_role, owner_role)
    return receipt


async def retire_registry_read_views(connection, *, source_schema, target_schema, api_role, owner_role):
    """Remove only exact bridges after the operator verifies all old registry processes are gone."""
    await _provisioning_scope(connection, source_schema, target_schema, api_role, owner_role)
    if await _aca_controls(connection, source_schema):
        raise RegistryManagementPermissionError("Accepted ACA inbox must move before compatibility retirement")
    if not await connection.fetchval(
        "SELECT pg_has_role(current_user,$1::name,'SET') AND pg_has_role(current_user,$1::name,'USAGE')", owner_role
    ):
        raise RegistryManagementPermissionError("Retirement operator cannot assume the protected owner")
    await verify_registry_management_permissions(
        connection, api_role=api_role, owner_role=owner_role, control_schema=target_schema
    )
    await _require_source_custody(connection, target_schema)
    view_oid_by_name = await _verify_registry_read_views(connection, source_schema, target_schema, api_role, owner_role)
    async with connection.transaction():
        for name in view_oid_by_name:
            await connection.execute(
                f"LOCK TABLE {_identifier(source_schema)}.{_identifier(name)} IN ACCESS SHARE MODE NOWAIT"
            )
        if (
            await _verify_registry_read_views(connection, source_schema, target_schema, api_role, owner_role)
            != view_oid_by_name
        ):
            raise RegistryManagementPermissionError("Compatibility views changed while locking")
        if await connection.fetchval(
            """SELECT EXISTS(SELECT 1 FROM pg_depend d WHERE d.refclassid='pg_class'::regclass
            AND d.refobjid=ANY($1::oid[]) AND NOT (d.classid='pg_type'::regclass AND d.deptype='i')
            AND NOT (d.classid='pg_rewrite'::regclass AND EXISTS(
                SELECT 1 FROM pg_rewrite r WHERE r.oid=d.objid AND r.ev_class=d.refobjid)))""",
            list(view_oid_by_name.values()),
        ):
            raise RegistryManagementPermissionError("Compatibility views have retained dependents")
        caller = await connection.fetchval("SELECT quote_ident(current_user)")
        await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
        try:
            for name in view_oid_by_name:
                await connection.execute(f"DROP VIEW {_identifier(source_schema)}.{_identifier(name)} RESTRICT")
        finally:
            await connection.execute(f"SET LOCAL ROLE {caller}")
    return {
        "component": "registry_read_views_retired",
        "revision": 1,
        "retired_views": len(view_oid_by_name),
        "source_schema": source_schema,
        "target_schema": target_schema,
        "view_oids": view_oid_by_name,
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
