# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed compatibility output shapes and native immutable heap receipts."""

import json

from sqlalchemy.dialects import postgresql

from db.migration_expression_adoption import _normalized_expression

OUTPUT_ROOTS = (
    "provider_directory_canonical_resource",
    "provider_directory_endpoint",
    "provider_directory_healthcare_service",
    "provider_directory_insurance_plan",
    "provider_directory_location",
    "provider_directory_organization",
    "provider_directory_organization_affiliation",
    "provider_directory_practitioner",
    "provider_directory_practitioner_role",
    "provider_directory_source_resource",
)

# Foreign keys also name the referenced index; report only its owning constraint.
OUTPUT_CATALOG_SQL = """
SELECT n.oid::bigint AS schema_oid,c.oid::bigint,c.relname,c.relowner::bigint,
 jsonb_build_object('kind',c.relkind,'persistence',c.relpersistence,'partition',c.relispartition,
 'rls',c.relrowsecurity,'force_rls',c.relforcerowsecurity,'options',c.reloptions,
 'tablespace',c.reltablespace::bigint,'access_method',c.relam::bigint,
 'parents',ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent),
 'children',ARRAY(SELECT inhrelid::bigint FROM pg_inherits WHERE inhparent=c.oid ORDER BY inhrelid),
 'hooks',(SELECT count(*) FROM pg_trigger WHERE tgrelid=c.oid),
 'rules',(SELECT count(*) FROM pg_rewrite WHERE ev_class=c.oid)) AS topology,
 (SELECT COALESCE(jsonb_agg(jsonb_build_object('number',a.attnum,'name',a.attname,
 'type_oid',a.atttypid::bigint,'type',format_type(a.atttypid,a.atttypmod),'typmod',a.atttypmod,
 'collation',a.attcollation::bigint,'native_collation',a.attcollation=t.typcollation,
 'required',a.attnotnull,'default',pg_get_expr(d.adbin,d.adrelid),
 'identity',a.attidentity,'generated',a.attgenerated) ORDER BY a.attnum),'[]'::jsonb)
 FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
 LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
 (SELECT COALESCE(jsonb_agg(jsonb_build_object('oid',k.oid::bigint,'name',k.conname,
 'type',k.contype,'keys',k.conkey,'definition',pg_get_constraintdef(k.oid),
 'validated',k.convalidated,'deferrable',k.condeferrable,'deferred',k.condeferred)
 ORDER BY k.oid),'[]'::jsonb) FROM pg_constraint k WHERE k.conrelid=c.oid) AS constraints,
 (SELECT COALESCE(jsonb_agg(jsonb_build_object('oid',i.indexrelid::bigint,'name',idx.relname,
 'owner',idx.relowner::bigint,'method',am.amname,'primary',i.indisprimary,'unique',i.indisunique,
 'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,'immediate',i.indimmediate,
 'exclusion',i.indisexclusion,'keys_count',i.indnkeyatts,'total_count',i.indnatts,
 'predicate',pg_get_expr(i.indpred,i.indrelid),'expression',pg_get_expr(i.indexprs,i.indrelid),
 'keys',ARRAY(SELECT a.attname::text FROM unnest(i.indkey::smallint[]) WITH ORDINALITY x(num,pos)
 LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=x.num ORDER BY x.pos),
 'opclasses',i.indclass::oid[],'collations',i.indcollation::oid[],'options',i.indoption::smallint[],
 'native',NOT EXISTS(SELECT FROM unnest(i.indclass::oid[],i.indcollation::oid[],
 i.indkey::smallint[],i.indoption::smallint[]) x(opclass_oid,collation_oid,num,flags)
 LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=x.num
 LEFT JOIN pg_opclass op ON op.oid=x.opclass_oid
 WHERE (x.num<>0 AND a.attnum IS NULL) OR x.flags<>0
 OR x.collation_oid<>COALESCE(a.attcollation,0)
 OR NOT op.opcdefault OR op.opcnamespace<>'pg_catalog'::regnamespace),
 'constraint_oid',(SELECT k.oid::bigint FROM pg_constraint k WHERE k.conindid=i.indexrelid
 AND k.conrelid=c.oid AND k.contype IN ('p','u','x')))
 ORDER BY i.indexrelid),'[]'::jsonb)
 FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_am am ON am.oid=idx.relam
 WHERE i.indrelid=c.oid) AS indexes,
 jsonb_build_object('table',COALESCE((SELECT jsonb_agg(jsonb_build_array(g.grantee::bigint,
 g.grantor::bigint,g.privilege_type,g.is_grantable) ORDER BY g.grantee,g.grantor,g.privilege_type)
 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) g),'[]'::jsonb),
 'columns',COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attnum,g.grantee::bigint,
 g.grantor::bigint,g.privilege_type,g.is_grantable) ORDER BY a.attnum,g.grantee,g.grantor,g.privilege_type)
 FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) g
 WHERE a.attrelid=c.oid),'[]'::jsonb)) AS acl,
 NOT EXISTS(SELECT FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) g
 WHERE g.grantee<>$3::oid AND (g.grantee<>ALL($4::oid[]) OR g.privilege_type<>'SELECT' OR g.is_grantable)
 UNION ALL SELECT FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) g
 WHERE a.attrelid=c.oid AND g.grantee<>$3::oid
 AND (g.grantee<>ALL($4::oid[]) OR g.privilege_type<>'SELECT' OR g.is_grantable)) AS acl_closed,
 NOT EXISTS(SELECT FROM unnest($4::oid[]) reader(oid) JOIN pg_roles reachable
 ON pg_has_role(reader.oid,reachable.oid,'USAGE') OR pg_has_role(reader.oid,reachable.oid,'SET')
 WHERE has_table_privilege(reachable.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR has_any_column_privilege(reachable.oid,c.oid,'INSERT,UPDATE,REFERENCES')) AS readers_closed,
 NOT EXISTS(SELECT FROM unnest($4::oid[]) reader(oid)
 WHERE NOT has_table_privilege(reader.oid,c.oid,'SELECT')) AS readers_select,
 obj_description(c.oid,'pg_class') AS descriptor
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.relname
"""


def output_models():
    """Resolve only the fixed public model set; never a caller-provided model."""
    from db import models

    selected = (
        models.ProviderDirectoryCanonicalResource,
        models.ProviderDirectoryEndpoint,
        models.ProviderDirectoryHealthcareService,
        models.ProviderDirectoryInsurancePlan,
        models.ProviderDirectoryLocation,
        models.ProviderDirectoryOrganization,
        models.ProviderDirectoryOrganizationAffiliation,
        models.ProviderDirectoryPractitioner,
        models.ProviderDirectoryPractitionerRole,
        models.ProviderDirectorySourceResource,
    )
    return dict(zip(OUTPUT_ROOTS, selected, strict=True))


def encoded(value):
    """Return canonical finite JSON for immutable native receipts."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def decoded(value):
    """Decode native JSON text while retaining already-decoded values."""
    return json.loads(value) if isinstance(value, str) else value


def require_output(condition, code):
    """Refuse any unsupported output custody or publication condition."""
    if not condition:
        raise RuntimeError("provider_directory_output_snapshot_" + code)


def require_output_shape(catalog_entry, target_name, *, indexed=True):
    """Match native keys/indexes and all columns to the fixed supported model."""
    model = output_models().get(target_name)
    require_output(model is not None, "target_invalid")
    topology = decoded(catalog_entry["topology"])
    require_output(
        topology["kind"] == "r"
        and topology["persistence"] == "p"
        and not any(
            topology[key] for key in ("partition", "rls", "force_rls", "parents", "children", "hooks", "rules")
        ),
        "topology_changed",
    )
    columns = decoded(catalog_entry["columns"])
    native_type_by_column = {column["name"]: column["type"].lower() for column in columns}
    expected = sorted(
        (
            str(column.name),
            _column_type(column, native_type_by_column),
            not column.nullable,
        )
        for column in model.__table__.columns
    )
    require_output(
        sorted((column["name"], column["type"].lower(), column["required"]) for column in columns) == expected
        and all(
            column["default"] is None and column["identity"] == column["generated"] == "" and column["native_collation"]
            for column in columns
        ),
        "columns_changed",
    )
    _require_output_indexes(catalog_entry, model, native_type_by_column, indexed=indexed)


def _require_output_indexes(catalog_entry, model, native_type_by_column, *, indexed):
    """Require the fixed complete key/index set after candidate loading."""
    constraints = decoded(catalog_entry["constraints"])
    require_output(
        all(
            constraint["type"] in {"p", "n"}
            and constraint["validated"]
            and not constraint["deferrable"]
            and not constraint["deferred"]
            for constraint in constraints
        ),
        "constraints_changed",
    )
    indexes = decoded(catalog_entry["indexes"])
    primary_columns = tuple(str(column.name) for column in model.__table__.primary_key.columns)
    expected_indexes = {(True, True, "btree", primary_columns, "", "")} | {
        _index_spec(index, native_type_by_column) for index in getattr(model, "__my_additional_indexes__", ())
    }
    actual_indexes = {
        (
            index["primary"],
            index["unique"],
            index["method"],
            tuple(index["keys"]),
            _normalized_expression(index["expression"]),
            _normalized_expression(index["predicate"]),
        )
        for index in indexes
    }
    require_output(
        (actual_indexes == expected_indexes and len(indexes) == len(expected_indexes) if indexed else not indexes)
        and all(
            index["valid"]
            and index["ready"]
            and index["live"]
            and index["immediate"]
            and not index["exclusion"]
            and index["native"]
            and index["keys_count"] == index["total_count"]
            and index["owner"] == catalog_entry["relowner"]
            for index in indexes
        )
        and sum(constraint["type"] == "p" for constraint in constraints) == int(indexed),
        "indexes_incomplete",
    )


def _column_type(column, native_type_by_column):
    expected = column.type.compile(dialect=postgresql.dialect()).lower().replace("varchar", "character varying")
    # The original structural migrations use native jsonb; later additions and
    # ORM-created tables use json. Keep the existing physical representation.
    if expected == "json" and native_type_by_column.get(str(column.name)) == "jsonb":
        return "jsonb"
    return expected


def _index_spec(index, native_type_by_column):
    """Only the existing fixed model's native JSON cast/phone predicate are supported."""
    method = index.get("using", "btree")
    keys = tuple(index["index_elements"])
    expression = ""
    if method == "gin":
        require_output(
            len(keys) == 1 and keys[0] in {"location_refs", "specialty_codes", "code_codes", "network_refs"},
            "index_contract_changed",
        )
        if native_type_by_column[keys[0]] == "json":
            expression = f"({keys[0]})::jsonb"
            keys = (None,)
    predicate = ""
    if index.get("where"):
        require_output(
            index["where"] == "phone_number IS NOT NULL AND phone_number <> ''" and keys == ("phone_number",),
            "index_contract_changed",
        )
        predicate = "((phone_number IS NOT NULL) AND ((phone_number)::text <> ''::text))"
    return (
        False,
        bool(index.get("unique", False)),
        method,
        keys,
        _normalized_expression(expression),
        _normalized_expression(predicate),
    )
