# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed isolated SOURCE coordinates; data alone never grants publisher authority."""

import hashlib
import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass

from db.plan_release_storage import storage_identifier
from process.provider_directory_generic_candidate import GenericCandidateError

_RELATIONS = ContextVar("provider_directory_isolated_source_relations", default=None)
_CATALOG_ROOTS = frozenset({"provider_directory_source", "provider_directory_api_endpoint", "import_run"})


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_generic_source_" + code)


@dataclass(frozen=True)
class FHIRSourceStorageBinding:
    """Native coordinates pinned by the original issued manifest, not a locator."""

    catalog_schema: str
    storage_schema: str
    database_oid: int
    catalog_schema_oid: int
    storage_schema_oid: int
    owner_oid: int
    namespace_owner_oid: int
    enrollment_sha256: str

    def __post_init__(self):
        for schema in (self.catalog_schema, self.storage_schema):
            storage_identifier(schema)
            _require(schema not in {"public", "information_schema"} and not schema.startswith("pg_"), "schema_invalid")
        _require(self.catalog_schema != self.storage_schema, "isolated_schema_required")
        _require(self.catalog_schema_oid != self.storage_schema_oid, "isolated_schema_required")
        _require(
            all(
                type(oid) is int and 0 < oid < 2**32
                for oid in (
                    self.database_oid,
                    self.catalog_schema_oid,
                    self.storage_schema_oid,
                    self.owner_oid,
                    self.namespace_owner_oid,
                )
            ),
            "storage_identity_invalid",
        )
        _require(self.owner_oid != self.namespace_owner_oid, "protected_namespace_required")
        _require(
            type(self.enrollment_sha256) is str and re.fullmatch(r"[0-9a-f]{64}", self.enrollment_sha256),
            "storage_identity_invalid",
        )


@dataclass(frozen=True)
class FHIRSourceCandidateHandoff:
    """Candidate evidence awaiting protected custody, validation and publication."""

    document_json: str

    @property
    def document_sha256(self):
        """Hash the complete canonical snapshot document, including its proofs."""
        return hashlib.sha256(self.document_json.encode()).hexdigest()

    @property
    def document(self):
        """Return a copy without claiming the underlying tables are immutable."""
        return json.loads(self.document_json)


async def verify_isolated_source_storage_coordinates(connection, policy, binding):
    """Check native coordinates only; this is not a complete enrollment verifier.

    The issued manifest must independently verify the fixed header, templates,
    checkpoint/proof controls and canonical admission authority before issuance.
    """
    _require(type(binding) is FHIRSourceStorageBinding and policy.schema == binding.storage_schema, "storage_changed")
    _require(connection.is_in_transaction() is True, "transaction_required")
    actual = await connection.fetchrow(
        "SELECT d.oid::bigint AS database_oid,c.oid::bigint AS catalog_schema_oid,"
        "s.oid::bigint AS storage_schema_oid,r.oid::bigint AS owner_oid,"
        "s.nspowner::bigint AS namespace_owner_oid "
        "FROM pg_database d,pg_namespace c,pg_namespace s,pg_roles r "
        "WHERE d.datname=current_database() AND c.nspname=$1 AND s.nspname=$2 "
        "AND r.rolname=$3",
        binding.catalog_schema,
        binding.storage_schema,
        policy.owner_role,
    )
    expected_by_field = {
        field: getattr(binding, field)
        for field in ("database_oid", "catalog_schema_oid", "storage_schema_oid", "owner_oid", "namespace_owner_oid")
    }
    _require(actual is not None and dict(actual) == expected_by_field, "storage_identity_changed")


def require_storage_schema(publisher, catalog_schema):
    """Keep catalog authority separate from the issued private namespace."""
    binding = getattr(publisher, "storage_binding", None)
    if binding is None:
        _require(publisher.policy.schema == catalog_schema, "schema_changed")
        return None
    from process.provider_directory_generic_source_policy import bound_publisher

    _require(bound_publisher() is publisher, "original_storage_publisher_required")
    _require(
        catalog_schema == binding.catalog_schema and publisher.policy.schema == binding.storage_schema,
        "schema_changed",
    )
    return binding


def storage_document(publisher):
    """Bind stored candidates to the immutable enrollment and exact producer."""
    from process.provider_directory_generic_source_policy import bound_publisher, source_producer_identity

    _require(bound_publisher() is publisher and publisher.storage_binding is not None, "isolated_storage_required")
    return {**asdict(publisher.storage_binding), "producer": list(source_producer_identity())}


@contextmanager
def isolated_source_relation_scope(publisher, tables_by_root):
    """Accept only the native-verified candidate mapping from the owner scope."""
    from process.provider_directory_generic_source_policy import bound_publisher
    from process.provider_directory_generic_source_storage import RELATIONS

    _require(bound_publisher() is publisher and publisher.storage_binding is not None, "isolated_storage_required")
    _require(set(tables_by_root) == set(RELATIONS), "isolated_relation_unsupported")
    token = _RELATIONS.set((publisher, tuple(sorted(tables_by_root.items()))))
    try:
        yield
    finally:
        _RELATIONS.reset(token)


def source_relation_coordinates(schema, root, scoped_name):
    """Route a closed root set; never redirect canonical identities or aliases."""
    from process import provider_directory_generic_source_policy as source
    from process.provider_directory_generic_source_storage import HEADER, RELATIONS
    from process.provider_directory_source_checkpoint_catalog import CHECKPOINT_LAYOUTS
    from process.provider_directory_source_observation import current_storage_binding
    from process.provider_directory_source_proof_control import PROOF_ROOT

    if root in _CATALOG_ROOTS:
        factory = source._FACTORY.get()
        binding = getattr(factory, "storage_binding", None)
        if binding is not None:
            factory._require_current_policy()
            _require(schema == binding.catalog_schema, "schema_changed")
            _require(scoped_name == root, "catalog_override_forbidden")
        return schema, scoped_name
    publisher = source.bound_publisher() if source._PUBLISHER.get() is not None else None
    binding = publisher.storage_binding if publisher is not None else current_storage_binding(schema)
    if binding is None:
        binding = source.current_source_storage_binding()
    if binding is None:
        return schema, scoped_name
    if publisher is not None:
        require_storage_schema(publisher, schema)
    _require(root in {HEADER, PROOF_ROOT, *RELATIONS, *CHECKPOINT_LAYOUTS}, "isolated_relation_unsupported")
    if root in RELATIONS:
        scope = _RELATIONS.get()
        _require(
            publisher is not None
            and scope is not None
            and scope[0] is publisher
            and dict(scope[1]).get(root) == scoped_name,
            "isolated_relation_override_changed",
        )
    else:
        _require(scoped_name == root, "isolated_header_override_changed")
    return binding.storage_schema, scoped_name


_PRIVATE_COORDINATES_SQL = """SELECT d.oid::bigint AS database_oid,
 c.oid::bigint AS catalog_schema_oid,s.oid::bigint AS storage_schema_oid,
 r.oid::bigint AS owner_oid,s.nspowner::bigint AS namespace_owner_oid
 FROM pg_database d,pg_namespace c,pg_namespace s,pg_roles r
 WHERE d.datname=current_database() AND c.nspname=$1 AND s.nspname=$2 AND r.rolname=$3"""
_PRIVATE_LAYOUT_SQL = """SELECT c.oid::bigint,c.relname,n.nspname,
 (SELECT jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
 pg_get_expr(d.adbin,d.adrelid),a.attgenerated::text,a.attidentity::text,a.attcollation::bigint,
 a.attstorage::text,a.attcompression::text) ORDER BY a.attnum)
 FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
 (SELECT coalesce(jsonb_agg(jsonb_build_array(k.conname,k.contype::text,pg_get_constraintdef(k.oid),
 k.convalidated,k.condeferrable,k.condeferred,coalesce((to_jsonb(k)->>'conenforced')::boolean,true))
 ORDER BY k.conname),'[]'::jsonb) FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype NOT IN ('n','f')) AS constraints,
 (SELECT jsonb_agg(jsonb_build_array(i.indisprimary,i.indisunique,i.indimmediate,i.indisexclusion,
 i.indisvalid,i.indisready,i.indislive,i.indnkeyatts,i.indnatts,am.amname,
 pg_get_expr(i.indpred,i.indrelid),pg_get_expr(i.indexprs,i.indrelid),
 ARRAY(SELECT pg_get_indexdef(i.indexrelid,ordinal,true) FROM generate_series(1,i.indnatts) ordinal),
 i.indclass::text,i.indcollation::text,i.indoption::text) ORDER BY i.indexrelid)
 FROM pg_index i JOIN pg_class index ON index.oid=i.indexrelid JOIN pg_am am ON am.oid=index.relam
 WHERE i.indrelid=c.oid) AS indexes
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=ANY($1::text[]) AND c.relname=ANY($2::text[]) ORDER BY n.nspname,c.relname"""


async def _isolated_roles(connection, policy, migration_role, namespace_owner_role, publication):
    from process.plan_release_storage_catalog import ROLE_SQL
    from process.plan_release_storage_custody import _require_storage_roles, storage_profile

    _require(not policy.control_runtime_roles, "private_control_reader_forbidden")
    _require(type(publication) is dict and set(publication) == {"owner_role", "publisher_role"}, "publication_required")
    names = (
        policy.owner_role,
        policy.publisher_role,
        *policy.runtime_roles,
        migration_role,
        namespace_owner_role,
        publication["owner_role"],
        publication["publisher_role"],
    )
    for name in names:
        storage_identifier(name)
    _require(len(set(names)) == len(names), "role_overlap")
    await connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
    source_names = storage_profile(**policy.role_arguments())
    role_rows = await connection.fetch(ROLE_SQL, list(names), policy.owner_role, policy.publisher_role, policy.schema)
    _require_storage_roles([role for role in role_rows if role["rolname"] in source_names], **policy.role_arguments())
    roles_by_name = {role["rolname"]: dict(role) for role in role_rows}
    _require(
        set(roles_by_name) == set(names) and not any(role["elevated"] for role in role_rows), "role_authority_changed"
    )
    await _isolated_delegation(connection, policy, roles_by_name, migration_role, namespace_owner_role, publication)
    return roles_by_name


_ISOLATED_DELEGATION_SQL = """SELECT
 (SELECT coalesce(jsonb_agg(jsonb_build_array(member::bigint,inherit_option,set_option,admin_option)
  ORDER BY member),'[]'::jsonb) FROM pg_auth_members WHERE roleid=$1) AS edges,
 NOT EXISTS(SELECT FROM pg_auth_members WHERE member=ANY(ARRAY[$1::oid,$2::oid])
  AND NOT(member=$2::oid AND roleid=$1::oid)) AS forward_only,
 NOT EXISTS(SELECT FROM pg_roles WHERE NOT rolsuper AND oid<>ALL($3::oid[])
  AND (pg_has_role(oid,$1::oid,'SET') OR pg_has_role(oid,$1::oid,'USAGE'))) AS owner_closed,
 NOT EXISTS(SELECT FROM unnest($4::oid[]) actor(oid),unnest($5::oid[]) owner(oid)
  WHERE pg_has_role(actor.oid,owner.oid,'SET') OR pg_has_role(actor.oid,owner.oid,'USAGE')) AS publication_closed,
 pg_has_role($6::oid,$7::oid,'USAGE') AND pg_has_role($6::oid,$7::oid,'SET')
  AND has_schema_privilege($7::oid,$8::name,'CREATE') AS publication_available"""


async def _isolated_delegation(connection, policy, roles_by_name, migration_role, namespace_owner_role, publication):
    """Require the one-way native handoff without giving Source actors a reverse path."""
    from process.provider_directory_generic_source_storage import _json

    protected_oids = [roles_by_name[name]["oid"] for name in (namespace_owner_role, publication["owner_role"])]
    _require(
        not any(roles_by_name[name]["rolcanlogin"] for name in (namespace_owner_role, publication["owner_role"])),
        "protected_owner_required",
    )
    _require(roles_by_name[publication["publisher_role"]]["rolcanlogin"], "publication_publisher_required")
    owner_oid = roles_by_name[policy.owner_role]["oid"]
    expected = sorted(
        (roles_by_name[name]["oid"], inherit, True, False)
        for name, inherit in (
            (policy.publisher_role, False),
            (migration_role, True),
            (publication["publisher_role"], True),
        )
    )
    delegation = await connection.fetchrow(
        _ISOLATED_DELEGATION_SQL,
        owner_oid,
        roles_by_name[policy.publisher_role]["oid"],
        [owner_oid, *(entry[0] for entry in expected)],
        [roles_by_name[name]["oid"] for name in (policy.owner_role, policy.publisher_role, *policy.runtime_roles)],
        protected_oids,
        roles_by_name[publication["publisher_role"]]["oid"],
        roles_by_name[publication["owner_role"]]["oid"],
        policy.schema,
    )
    _require(
        [tuple(edge) for edge in _json(delegation["edges"])] == expected,
        "source_delegation_changed",
    )
    _require(delegation["forward_only"], "source_delegation_changed")
    _require(delegation["owner_closed"], "source_delegation_changed")
    _require(delegation["publication_closed"], "publication_reverse_delegation")
    _require(delegation["publication_available"], "publication_custody_unavailable")


def _isolated_layout_shape(row, *, template):
    from process.provider_directory_generic_source_provision import _ISOLATED_APPLICATION_CHECKS
    from process.provider_directory_generic_source_storage import HEADER, _json
    from process.provider_directory_source_checkpoint_catalog import CHECKPOINT_LAYOUTS

    columns = _json(row["columns"])
    if template and row["relname"] in CHECKPOINT_LAYOUTS:
        jsonb_names = {column[0] for column in CHECKPOINT_LAYOUTS[row["relname"]]["columns"] if column[1] == "jsonb"}
        columns = [
            [column[0], "jsonb", *column[2:]] if column[0] in jsonb_names and column[1] == "json" else column
            for column in columns
        ]
    constraints = _json(row["constraints"])
    if template:
        # LIKE leaves canonical constraint triggers intact without copying them.
        constraints = [constraint for constraint in constraints if constraint[1] != "t"]
    if template and row["relname"] == HEADER:
        constraints = [constraint for constraint in constraints if constraint[0] not in _ISOLATED_APPLICATION_CHECKS]
    # LIKE assigns fresh PK/UNIQUE/index names and native identities.
    return dict(
        columns=columns,
        constraints=sorted((constraint[1:] for constraint in constraints), key=str),
        indexes=sorted(_json(row["indexes"]) or [], key=str),
    )


_ADMISSION_RIGHTS = {
    "provider_directory_source": ("source_id", None),
    "provider_directory_api_endpoint": ("endpoint_id", None),
    "import_run": (
        "run_id",
        ["run_id", "retry_of_run_id", "engine", "node_id", "importer", "status", "finished_at", "params"],
    ),
}
_ADMISSION_RIGHTS_SQL = """SELECT relation.oid,
 NOT pg_has_role($3::oid,relation.relowner::oid,'SET')
 AND NOT pg_has_role($3::oid,relation.relowner::oid,'USAGE')
 AND has_schema_privilege($3::oid,$2::name,'USAGE') AND NOT has_schema_privilege($3::oid,$2::name,'CREATE')
 AND NOT has_table_privilege($3::oid,relation.oid::oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 AND NOT has_any_column_privilege($3::oid,relation.oid::oid,'INSERT,REFERENCES')
 AND NOT has_table_privilege($4::oid,relation.oid::oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 AND NOT has_any_column_privilege($4::oid,relation.oid::oid,'INSERT,UPDATE,REFERENCES')
 AND EXISTS(SELECT FROM pg_attribute WHERE attrelid=relation.oid AND attname=rights.key AND NOT attisdropped)
 AND NOT EXISTS(SELECT FROM pg_attribute a WHERE a.attrelid=relation.oid AND a.attnum>0 AND NOT a.attisdropped
  AND (has_column_privilege($3::oid,relation.oid::oid,a.attnum,'UPDATE')<>(a.attname=rights.key)
  OR has_column_privilege($3::oid,relation.oid::oid,a.attnum,'SELECT')<>(rights.selected IS NULL OR a.attname=ANY(rights.selected))
  OR has_column_privilege($3::oid,relation.oid::oid,a.attnum,'SELECT WITH GRANT OPTION,UPDATE WITH GRANT OPTION'))) AS closed
 FROM catalog relation JOIN jsonb_to_recordset($8::jsonb) rights(root text,key text,selected text[])
 ON relation.relname=rights.root WHERE relation.scope='admission'"""


def _isolated_catalog_sql(*, include_history_bound_identity=False):
    """Evaluate existing catalog contracts together, without retaining observations."""
    from process.plan_release_storage_catalog import CATALOG_SQL
    from process.provider_directory_generic_source_provision import _FHIR_PHYSICAL_SQL, _ISOLATED_TEMPLATE_SQL

    catalog = CATALOG_SQL.replace("$3::oid[]", "ARRAY[]::oid[]").replace("$2::oid", "$3::oid")
    catalog = catalog.replace("$1::text[]", "relation_group.names")
    template = _ISOLATED_TEMPLATE_SQL.replace("$2::text[]", "template_group.names")
    template = template.replace("$1::oid", "template_group.namespace_oid")
    physical = _FHIR_PHYSICAL_SQL.replace("$1::oid[]", "ARRAY(SELECT oid::oid FROM catalog)")
    layouts = _PRIVATE_LAYOUT_SQL.replace("$2::text[]", "$6::text[]")
    layouts = layouts.replace("$1::text[]", "ARRAY[$2::text,$1::text]")
    if include_history_bound_identity:
        layouts = layouts.replace(
            "coalesce((to_jsonb(k)->>'conenforced')::boolean,true))",
            "coalesce((to_jsonb(k)->>'conenforced')::boolean,true)) "
            "|| CASE WHEN k.conname='cms_history_bound' THEN jsonb_build_array(k.oid::bigint) ELSE '[]'::jsonb END",
        )
    return f"""WITH relation_groups(scope,names) AS (
 SELECT 'private',array_agg(quote_ident($1::text)||'.'||quote_ident(root) ORDER BY ordinal)
 FROM unnest($6::text[]) WITH ORDINALITY roots(root,ordinal)
 UNION ALL SELECT 'admission',array_agg(c.oid::text ORDER BY c.relname)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$2 AND c.relname=ANY(ARRAY(SELECT root FROM jsonb_to_recordset($8::jsonb) rights(root text)))
 ), catalog AS MATERIALIZED (
 SELECT relation_group.scope,evidence.* FROM relation_groups relation_group CROSS JOIN LATERAL ({catalog}) evidence
 ), template_groups(scope,namespace_oid,names) AS (
 SELECT 'private',to_regnamespace($1)::oid,$6::text[]
 UNION ALL SELECT 'catalog',to_regnamespace($2)::oid,ARRAY[($6::text[])[1]]
 ), templates AS (
 SELECT template_group.scope,evidence.* FROM template_groups template_group CROSS JOIN LATERAL ({template}) evidence
 ), physical AS ({physical}), layouts AS ({layouts}), admission_rights AS ({_ADMISSION_RIGHTS_SQL})
 SELECT (SELECT coalesce(jsonb_agg(catalog ORDER BY scope,oid),'[]'::jsonb) FROM catalog) AS catalog,
 (SELECT coalesce(jsonb_agg(templates ORDER BY scope,relname),'[]'::jsonb) FROM templates) AS templates,
 (SELECT coalesce(jsonb_agg(physical ORDER BY oid),'[]'::jsonb) FROM physical) AS physical,
 (SELECT coalesce(jsonb_agg(layouts ORDER BY nspname,relname),'[]'::jsonb) FROM layouts) AS layouts,
 (SELECT coalesce(jsonb_agg(admission_rights ORDER BY oid),'[]'::jsonb) FROM admission_rights) AS admission_rights,
 quote_ident($2::text)||'.' AS catalog_prefix,{_isolated_native_checks_sql()}"""


def _isolated_native_checks_sql():
    from process.provider_directory_generic_source_provision import _ISOLATED_DEPENDENCIES_SQL
    from process.provider_directory_generic_source_storage import _PRIVATE_READERS_CLOSED_SQL
    from process.provider_directory_source_checkpoint_catalog import _NATIVE_SQL

    private_oids = "ARRAY(SELECT oid::oid FROM catalog WHERE scope='private')"
    readers = _PRIVATE_READERS_CLOSED_SQL.replace("$2::oid[]", "$5::oid[]").replace("$1::oid[]", private_oids)
    dependencies = _ISOLATED_DEPENDENCIES_SQL.replace("$2::oid[]", "ARRAY[]::oid[]")
    dependencies = dependencies.replace("$1::oid[]", private_oids)
    checkpoint = _NATIVE_SQL.replace("$1::oid", "relation.oid::oid")
    return f"""({readers}) AS readers_closed,({dependencies}) AS dependencies_closed,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('oid',relation.oid,'supported',({checkpoint})) ORDER BY relation.oid),
 '[]'::jsonb) FROM catalog relation WHERE scope='private' AND relname=ANY($7::text[])) AS checkpoints"""


async def _isolated_catalog_rows(connection, policy, catalog_schema, roles_by_name, *, expected_history_bounds=None):
    from process.provider_directory_generic_source_provision import ISOLATED_FHIR_ROOTS
    from process.provider_directory_generic_source_storage import _json
    from process.provider_directory_source_checkpoint_catalog import CHECKPOINT_LAYOUTS

    evidence_by_field = dict(
        await connection.fetchrow(
            _isolated_catalog_sql(include_history_bound_identity=expected_history_bounds is not None),
            policy.schema,
            catalog_schema,
            roles_by_name[policy.owner_role]["oid"],
            roles_by_name[policy.publisher_role]["oid"],
            [roles_by_name[name]["oid"] for name in policy.runtime_roles],
            list(ISOLATED_FHIR_ROOTS),
            list(CHECKPOINT_LAYOUTS),
            json.dumps(
                [dict(root=root, key=key, selected=selected) for root, (key, selected) in _ADMISSION_RIGHTS.items()]
            ),
        )
    )
    for name in ("catalog", "templates", "physical", "layouts", "admission_rights", "checkpoints"):
        evidence_by_field[name] = _json(evidence_by_field[name])
    return evidence_by_field


def _isolated_application_checks(evidence):
    from process.provider_directory_generic_source_provision import _ISOLATED_APPLICATION_CHECKS
    from process.provider_directory_generic_source_storage import _json

    rows = [relation for relation in evidence["templates"] if relation["scope"] == "catalog"]
    _require(len(rows) == 1 and rows[0]["supported"], "header_template_changed")
    prefix = evidence["catalog_prefix"]
    checks_by_name = {check["name"]: check for check in _json(rows[0]["checks"]) or []}
    required_checks_by_name = {}
    for name, (function, digest) in _ISOLATED_APPLICATION_CHECKS.items():
        check = checks_by_name.get(name)
        _require(check is not None and check["functions"] == [prefix + function], "application_check_changed")
        normalized = check["definition"].replace(
            prefix + function.split("(")[0] + "(", "__catalog__." + function.split("(")[0] + "("
        )
        _require(hashlib.sha256(normalized.encode()).hexdigest() == digest, "application_check_changed")
        required_checks_by_name[name] = dict(check, normalized_definition_sha256=digest)
    return required_checks_by_name


def _require_isolated_relation_layouts(policy, roles_by_name, relation_rows, checkpoints):
    """Check private relation owners, native layouts and checkpoint contracts."""
    from process.provider_directory_generic_source_storage import _table_proof
    from process.provider_directory_source_checkpoint_catalog import CHECKPOINT_LAYOUTS, _require_layout
    from process.provider_directory_source_proof_control import PROOF_ROOT, _require_proof_layout

    for relation in relation_rows:
        _require(
            relation["nspname"] == policy.schema and relation["relowner"] == roles_by_name[policy.owner_role]["oid"],
            "storage_owner_changed",
        )
        _table_proof(relation, private=True)
        if relation["relname"] == PROOF_ROOT:
            _require_proof_layout(relation, isolated=True)
        if relation["relname"] in CHECKPOINT_LAYOUTS:
            _require_layout(relation, policy.schema, isolated=True)
            _require(checkpoints.get(relation["oid"]), "checkpoint_native_contract_changed")


def _isolated_bounded_template(private_layout, canonical_layout, expected_bound):
    """Project only the authenticated old canonical CHECK; private copies stay unbounded."""
    from process.provider_directory_generic_source_storage import _json

    _require(
        all(constraint[0] != "cms_history_bound" for constraint in _json(private_layout["constraints"])),
        "private_history_bound",
    )
    if expected_bound is None or canonical_layout["oid"] != expected_bound["old_oid"]:
        return canonical_layout
    constraints = _json(canonical_layout["constraints"])
    bound_constraints = [constraint for constraint in constraints if constraint[0] == "cms_history_bound"]
    _require(
        len(bound_constraints) == 1
        and bound_constraints[0][1] == "c"
        and bound_constraints[0][3:] == [True, False, False, True, expected_bound["native"]["constraint"]["oid"]],
        "history_bound_changed",
    )
    return dict(
        canonical_layout, constraints=[constraint for constraint in constraints if constraint[0] != "cms_history_bound"]
    )


def _isolated_inventory(policy, catalog_schema, roles_by_name, evidence, *, expected_history_bounds=None):
    """Verify and return the physical inventory for one isolated source schema."""
    from process.provider_directory_generic_source_provision import (
        ISOLATED_FHIR_ROOTS,
        _fhir_catalog_row,
        _fhir_physical_rows,
    )
    from process.provider_directory_generic_source_storage import _require as require_storage

    if expected_history_bounds is not None:
        from process.provider_directory_cms_history_bounds import require_cms_history_expectations

        expected_history_bounds = require_cms_history_expectations(expected_history_bounds)

    roots = list(ISOLATED_FHIR_ROOTS)
    relation_rows = [
        {key: field_value for key, field_value in relation.items() if key != "scope"}
        for relation in evidence["catalog"]
        if relation["scope"] == "private"
    ]
    _require(
        len(relation_rows) == len(roots) and {relation["relname"] for relation in relation_rows} == set(roots),
        "storage_missing",
    )
    _require_isolated_relation_layouts(
        policy, roles_by_name, relation_rows, {entry["oid"]: entry["supported"] for entry in evidence["checkpoints"]}
    )
    require_storage(evidence["readers_closed"] is True, "private_reader_access")
    _require(evidence["dependencies_closed"], "storage_dependency_changed")
    native_rows = [relation for relation in evidence["templates"] if relation["scope"] == "private"]
    _require(
        len(native_rows) == len(roots) and all(relation["supported"] for relation in native_rows),
        "storage_native_changed",
    )
    layout_by_key = {(relation["nspname"], relation["relname"]): relation for relation in evidence["layouts"]}
    _require(len(layout_by_key) == 2 * len(roots), "template_missing")
    for root in roots:
        canonical_layout = layout_by_key[catalog_schema, root]
        if expected_history_bounds is not None:
            canonical_layout = _isolated_bounded_template(
                layout_by_key[policy.schema, root], canonical_layout, expected_history_bounds.get(root)
            )
        _require(
            _isolated_layout_shape(layout_by_key[policy.schema, root], template=False)
            == _isolated_layout_shape(canonical_layout, template=True),
            "template_layout_changed",
        )
    relation_oids = {relation["oid"] for relation in relation_rows}
    physical = _fhir_physical_rows(
        relation_rows, [relation for relation in evidence["physical"] if relation["oid"] in relation_oids]
    )
    return {
        relation["relname"]: {
            **_fhir_catalog_row(relation),
            "native_layout": _fhir_catalog_row(layout_by_key[policy.schema, relation["relname"]]),
        }
        for relation in physical
    }


async def _isolated_access_closure(connection, inventory_by_name, roles_by_name, policy, migration_role, publication):
    """Owner-only ACLs cannot hide an unrelated login's inherited global privileges."""
    permitted_oids = [
        roles_by_name[name]["oid"] for name in (policy.publisher_role, migration_role, publication["publisher_role"])
    ]
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_roles actor,pg_roles reachable,unnest($1::oid[]) relation(oid) "
            "WHERE actor.rolcanlogin AND NOT actor.rolsuper AND actor.oid<>ALL($2::oid[]) "
            "AND (pg_has_role(actor.oid,reachable.oid,'SET') OR pg_has_role(actor.oid,reachable.oid,'USAGE')) "
            "AND (has_table_privilege(reachable.oid,relation.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
            "OR has_any_column_privilege(reachable.oid,relation.oid,'SELECT,INSERT,UPDATE,REFERENCES')))",
            [relation["oid"] for relation in inventory_by_name.values()],
            permitted_oids,
        ),
        "private_access_open",
    )


def _isolated_admission_rights(catalog_schema, evidence):
    from process.provider_directory_generic_source_provision import _fhir_catalog_row, _fhir_physical_rows

    relation_rows = [
        {key: value for key, value in relation.items() if key != "scope"}
        for relation in evidence["catalog"]
        if relation["scope"] == "admission"
    ]
    _require(
        len(relation_rows) == 3 and {relation["relname"] for relation in relation_rows} == _CATALOG_ROOTS,
        "admission_missing",
    )
    closed_by_oid = {relation["oid"]: relation["closed"] for relation in evidence["admission_rights"]}
    for relation in relation_rows:
        _require(
            relation["nspname"] == catalog_schema
            and relation["relkind"] == "r"
            and relation["relpersistence"] == "p"
            and not relation["relrowsecurity"]
            and not relation["relforcerowsecurity"],
            "admission_layout_changed",
        )
        _require(closed_by_oid.get(relation["oid"]), "admission_authority_changed")
    relation_oids = {relation["oid"] for relation in relation_rows}
    physical_rows = _fhir_physical_rows(
        relation_rows, [relation for relation in evidence["physical"] if relation["oid"] in relation_oids]
    )
    return {relation["relname"]: _fhir_catalog_row(relation) for relation in physical_rows}


def _isolated_metadata_sql():
    from process.provider_directory_cms_callable_custody import _FUNCTION_SQL

    functions = _FUNCTION_SQL.replace("$1::text[]", "ARRAY(SELECT oid::text FROM named_functions)")
    functions = functions.replace("$2", "$1").replace("$3::text[]", "$2::text[]").replace("$4::text[]", "$3::text[]")
    return f"""WITH named_functions AS MATERIALIZED (
 SELECT p.oid,p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname=$1 AND p.proname=ANY($2::text[])
 ), functions AS ({functions})
 SELECT functions.*,named_functions.proname,
 NOT EXISTS(SELECT FROM unnest($4::oid[]) actor(oid) WHERE
 pg_has_role(actor.oid,(definition->>'proowner')::oid,'SET')
 OR pg_has_role(actor.oid,(definition->>'proowner')::oid,'USAGE'))
 AND NOT EXISTS(SELECT FROM unnest($4::oid[]) actor(oid)
 WHERE has_function_privilege(actor.oid,(definition->>'oid')::oid,'EXECUTE')) AS authority_closed
 FROM functions JOIN named_functions ON named_functions.oid=functions.signature::oid
 ORDER BY named_functions.oid"""


async def _isolated_metadata_rows(connection, catalog_schema, runtime_roles, private_oids):
    """Resolve fixed callables and private rights in the same native snapshot."""
    from db.plan_release_storage import storage_relation
    from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS

    signatures_by_name = {
        signature.split("(")[0]: storage_relation(catalog_schema, signature.split("(")[0])
        + "("
        + signature.split("(", 1)[1]
        for signature in METADATA_FUNCTIONS
    }
    function_rows = await connection.fetch(
        _isolated_metadata_sql(), catalog_schema, list(signatures_by_name), list(runtime_roles), private_oids
    )
    _require(
        len(function_rows) == len(signatures_by_name)
        and {function["proname"] for function in function_rows} == set(signatures_by_name),
        "metadata_missing",
    )
    return [
        dict(
            {key: value for key, value in function.items() if key != "proname"},
            signature=signatures_by_name[function["proname"]],
        )
        for function in function_rows
    ]


async def _isolated_metadata_custody(connection, catalog_schema, catalog_oid, roles_by_name, policy):
    """Pin canonical callable definitions and ACLs without admitting private executors."""
    from process.provider_directory_cms_callable_custody import _decoded
    from process.provider_directory_source_callable_custody import _CONTRACTS, _TYPE_OIDS, _require_definition

    private_oids = [roles_by_name[name]["oid"] for name in (policy.owner_role, policy.publisher_role)]
    function_rows = await _isolated_metadata_rows(connection, catalog_schema, policy.runtime_roles, private_oids)
    functions_by_signature = {}
    for function in function_rows:
        function_by_field = {key: value for key, value in function.items() if key != "authority_closed"}
        for key in ("definition", "acl", "access"):
            function_by_field[key] = _decoded(function_by_field[key])
        signature = function["signature"].split(".", 1)[1].replace('"', "").replace("character varying", "varchar")
        _require_definition(
            function_by_field,
            _CONTRACTS[signature],
            catalog_schema,
            catalog_oid,
            _TYPE_OIDS,
            storage_identifier(catalog_schema),
        )
        ordinary_oids = [
            roles_by_name[name]["oid"] for name in (policy.owner_role, policy.publisher_role, *policy.runtime_roles)
        ]
        _require(function["authority_closed"], "metadata_authority_changed")
        # Existing runtime rights stay pinned; only private principals must lack canonical authority.
        function_by_field["access"] = [access for access in function_by_field["access"] if access[0] in ordinary_oids]
        functions_by_signature[signature] = function_by_field
    return functions_by_signature


async def _require_source_storage_publisher(connection, policy, publication):
    """Read and check the original publisher identity on this transaction."""
    import asyncio

    from process.provider_directory_source_callable_custody import _READ_IDENTITY_SQL

    task, identity = asyncio.current_task(), dict(await connection.fetchrow(_READ_IDENTITY_SQL))
    _require(
        identity["session"] == identity["current"]
        and identity["administrator"]
        or identity["session"] == policy.publisher_role
        and identity["current"] in (policy.publisher_role, policy.owner_role)
        or type(publication) is dict
        and identity["session"] == identity["current"] == publication.get("publisher_role"),
        "genuine_publisher_required",
    )
    return task, identity


async def _isolated_storage_identity(
    connection, policy, catalog_schema, migration_role, namespace_owner_role, publication
):
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    _require(type(policy) is GenericSourcePolicy and connection.is_in_transaction() is True, "transaction_required")
    for schema in (catalog_schema, policy.schema):
        storage_identifier(schema)
        _require(schema not in {"public", "information_schema"} and not schema.startswith("pg_"), "schema_invalid")
    _require(catalog_schema != policy.schema, "isolated_schema_required")
    task, identity = await _require_source_storage_publisher(connection, policy, publication)
    _require(await connection.fetchval("SHOW session_replication_role") == "origin", "replication_role")
    roles_by_name = await _isolated_roles(connection, policy, migration_role, namespace_owner_role, publication)
    coordinates = await connection.fetchrow(_PRIVATE_COORDINATES_SQL, catalog_schema, policy.schema, policy.owner_role)
    _require(
        coordinates is not None and coordinates["namespace_owner_oid"] == roles_by_name[namespace_owner_role]["oid"],
        "storage_identity_changed",
    )
    return task, identity, roles_by_name, coordinates


async def _isolated_canonical_authority(connection, policy, catalog_schema, catalog_oid, roles, evidence, bounds):
    authority_by_field = dict(
        admission=_isolated_admission_rights(catalog_schema, evidence),
        metadata=await _isolated_metadata_custody(connection, catalog_schema, catalog_oid, roles, policy),
    )
    if bounds is not None:
        authority_by_field["history_bounds"] = bounds
    return authority_by_field


async def _require_isolated_bound_proof(connection, catalog_schema, expected, owner_oid):
    """Bind optional canonical history proof to the issued namespace owner."""
    if expected is not None:
        from process.provider_directory_cms_history_bounds import require_cms_source_history_bounds

        await require_cms_source_history_bounds(
            connection, catalog_schema=catalog_schema, expected=expected, owner_oid=owner_oid
        )


async def capture_isolated_source_storage_custody(
    connection,
    policy,
    *,
    catalog_schema,
    migration_role,
    namespace_owner_role,
    publication,
    expected_history_bounds=None,
):
    """Attest the fixed private storage and canonical rights; never grant activation."""
    import asyncio

    from process.provider_directory_source_callable_custody import _READ_IDENTITY_SQL

    if expected_history_bounds is not None:
        from process.provider_directory_cms_history_bounds import require_cms_history_expectations

        expected_history_bounds = require_cms_history_expectations(expected_history_bounds)
    task, identity, roles_by_name, coordinates = await _isolated_storage_identity(
        connection, policy, catalog_schema, migration_role, namespace_owner_role, publication
    )
    await _require_isolated_bound_proof(
        connection, catalog_schema, expected_history_bounds, coordinates["namespace_owner_oid"]
    )
    evidence = await _isolated_catalog_rows(
        connection, policy, catalog_schema, roles_by_name, expected_history_bounds=expected_history_bounds
    )
    custody_by_field = dict(
        version=1,
        coordinates=dict(catalog_schema=catalog_schema, storage_schema=policy.schema, **dict(coordinates)),
        roles=roles_by_name,
        inventory=_isolated_inventory(
            policy, catalog_schema, roles_by_name, evidence, expected_history_bounds=expected_history_bounds
        ),
        application_checks=_isolated_application_checks(evidence),
        canonical_authority=await _isolated_canonical_authority(
            connection,
            policy,
            catalog_schema,
            coordinates["catalog_schema_oid"],
            roles_by_name,
            evidence,
            expected_history_bounds,
        ),
    )
    await _isolated_access_closure(
        connection, custody_by_field["inventory"], roles_by_name, policy, migration_role, publication
    )
    _require(
        connection.is_in_transaction() is True
        and asyncio.current_task() is task
        and dict(await connection.fetchrow(_READ_IDENTITY_SQL)) == identity,
        "transaction_changed",
    )
    return json.loads(json.dumps(custody_by_field, sort_keys=True))
