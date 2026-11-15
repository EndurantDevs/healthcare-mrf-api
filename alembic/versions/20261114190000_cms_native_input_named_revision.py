# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep existing native mutation counters under their declared input names."""

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261114190000_cms_native_input_named_revision"
down_revision = "20261114180000_reference_family_named_generation"
branch_labels = None
depends_on = None


_ADDRESS_NAMES = (
    "entity_address_unified",
    "entity_address_evidence",
    "entity_address_plan_bridge",
    "entity_address_network_bridge",
    "entity_address_procedure_bridge",
    "entity_address_medication_bridge",
    "facility_anchor_npi_candidate",
)
_DOCTOR_NAMES = ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site")


_FUNCTION_BODY_DIGESTS = {
    "cms_native_input_advance": (
        "492ee877ef3c4ee7a45515665321405898815f35e6c13da96e571ab3f9425b6d",
        "c35810561436b0ba1a6ab995efafa214f720bf06f817d57c009fd64131f95086",
    ),
    "cms_native_input_revision_guard": (
        "0206ebcd3e5ce798c84e0df20877eee256792825fd2f722dacf182cfc3a363dc",
        "4158fa9b35a52fc5fe934501386cd5993cb7912ea5424dfc18e20cf4f9fd7e58",
    ),
    "cms_serving_archive_result_matches": (
        "38426c90255c28e871039fe2aa9e844758cdb67de6ab8e2199ef04912ef06b66",
        "a4031321d8f58e84b2e5a46c55c3d64793958f228b7e0b21a947fda6c1d6e346",
    ),
    "cms_serving_oids": (
        "90a3b2c46612c899a8695c8196de49fa4ca90e3ac4806e243302a167dc2532d7",
        "8e7df86d90d68da0407e632b80e13d154e27700c75f8af4a2bfc96ab6dd8d65f",
    ),
    "cms_serving_native_snapshot": (
        "91306c115e0835db8ece5ed05b21f48d021735d464d64a70f4587d6c9fc77f4e",
        "617aa2ca58ba2b3e42d4ab08ce5a5220021e509bccd1cad62555eda04548eb86",
    ),
    "cms_serving_native_matches": (
        "059b4c093c0c089ce9d5b43bdfa67111586f33d24072517fe57b37f2ea1fb287",
        "72509bebd1e1722b4004589e3015c1450e6e1ab6c7f9ac1fc3b90982ff33d3ce",
    ),
    "cms_serving_snapshot_receipt_matches": (
        "b2f575c615b7e076fb8628e3f9ba7cab0411ac7ca6c14fb310a6724441860d3c",
        "8b8bbb02d61400acd02b4e29bdd82d8bffded59e3e2bbe6dc52538fe467a6735",
    ),
    "cms_serving_current_receipt_matches": (
        "dc6a70bf64fd378e2a83a7caf180e856490c5a66676335be6fe8a1df1860f756",
        "5a8bb31f947f568f8df91785d5b13ee260bb9afdc902760d2db7f76fd60a41fc",
    ),
    "cms_serving_receipt_matches": (
        "a7e3da06af9af8c047bb262f27cb94e6cd776f40ff05087b24167ddb35b20ec9",
        "7092c5e7712d341b2efba09fe9c1694f289046c96ce21310e1396ab829a0fdba",
    ),
    "cms_serving_require_fresh": (
        "7a1769065c5da4ac2fbd9c04e8f846e3093be25baa078f5c3e2da56ed60b8469",
        "165f22f571092aee4bf4d2cb16da8e1ac96ce993bf0daaeb548be0dd5ed01ca1",
    ),
    "cms_serving_doctors_transition": (
        "ee37d843ebe88cf5433990eabb8f742200eaf3cdf2333cceaf22672f7d4f6fd0",
        "608d3f4d96f034c037cd74791c1e1c1ce47a7695ca9dd9b50366d89a3f19c22b",
    ),
    "cms_serving_address_transition": (
        "afb90c15e4a45c3022403891afaaf85b4cbc81bd759a98cbdb1d227bba8ebef7",
        "8287a14ad7c03b37ce5feb04b36b5c3687dbd10c4fd1909dd7e345d799961f34",
    ),
    "cms_serving_profile_transition": (
        "a8af12e1bb98c589ae88fd3e622c69b5b3b4e853875fb16ff0b57020f8fb6539",
        "f768e142a40cd407538f261bb0b01f30f7efb8f4036ec12a3d756ae55a8a0b1f",
    ),
}


def _qname(schema, name):
    return f"jsonb_build_object('schema_name','{schema}','relation_name','{name}')"


def _declared_names(schema, names):
    return "jsonb_build_array(" + ",".join(_qname(schema, name) for name in names) + ")"


def _recorded_names(alias):
    return f"CASE WHEN {alias}.origin_lineage_id IS NULL THEN NULL ELSE (SELECT jsonb_agg(jsonb_build_object('schema_name',{alias}.serving_schema,'relation_name',name) ORDER BY position) FROM unnest({alias}.serving_relation_names) WITH ORDINALITY declared(name,position)) END"


def _declared_authority_names(alias, schema, names):
    return f"CASE WHEN {alias}.origin_lineage_id IS NULL THEN NULL ELSE {_declared_names(schema, names)} END"


def _part(document_sql, kind, schema):
    if kind == "profile":
        old = "ARRAY['evidence_target_oid','profile_target_oid']"
        addition = f"jsonb_build_object('evidence_target',{_qname(schema, 'provider_directory_profile_evidence')},'profile_target',{_qname(schema, 'provider_directory_profile')})"
        projection = f"CASE WHEN {document_sql} ?& {old} AND NOT {document_sql} ?| ARRAY['evidence_target','profile_target'] THEN ({document_sql}-{old})||{addition} ELSE {document_sql} END"
    else:
        names = _DOCTOR_NAMES if kind == "doctors" else _ADDRESS_NAMES
        relation = f"CASE WHEN {document_sql}->>'origin_generation' IS NULL THEN 'null'::jsonb ELSE {_declared_names(schema, names)} END"
        projection = f"CASE WHEN {document_sql} ? 'relation_oids' AND NOT {document_sql} ? 'relations' THEN ({document_sql}-'relation_oids')||jsonb_build_object('relations',{relation}) ELSE {document_sql} END"
    return (
        f"(CASE WHEN {document_sql} IS NULL OR {document_sql}='null'::jsonb THEN {document_sql} ELSE {projection} END)"
    )


def _native(document_sql, schema):
    field_parts = []
    for kind in ("profile", "address", "doctors"):
        field_parts.extend((f"'{kind}'", _part(f"({document_sql}->'{kind}')", kind, schema)))
    overlay = f"CASE WHEN {document_sql} ? 'overlay' THEN {document_sql}->'overlay' WHEN {document_sql} ? 'overlay_oid' THEN {_qname(schema, 'provider_directory_address_overlay')} ELSE NULL END"
    return f"(({document_sql}-ARRAY['profile','address','doctors','overlay_oid','overlay'])||jsonb_build_object({','.join(field_parts)},'overlay',{overlay}))"


def _oids_replacements(schema):
    return (
        (
            f"SELECT jsonb_agg(to_regclass(format('%I.%I','{schema}',name))::oid::bigint ORDER BY position)",
            f"SELECT CASE WHEN bool_and(to_regclass(format('%I.%I','{schema}',name)) IS NOT NULL) THEN jsonb_agg(jsonb_build_object('schema_name','{schema}','relation_name',name) ORDER BY position) END",
        ),
    )


def _native_snapshot_replacements(schema):
    quoted = storage_identifier(schema)
    return (
        ("'relation_oids',a.relation_oids", f"'relations',{_declared_authority_names('a', schema, _ADDRESS_NAMES)}"),
        ("'relation_oids',d.relation_oids", f"'relations',{_recorded_names('d')}"),
        (
            "'evidence_target_oid',p.evidence_target_oid",
            f"'evidence_target',CASE WHEN to_regclass('{quoted}.provider_directory_profile_evidence') IS NOT NULL THEN {_qname(schema, 'provider_directory_profile_evidence')} END",
        ),
        (
            "'profile_target_oid',p.profile_target_oid",
            f"'profile_target',CASE WHEN to_regclass('{quoted}.provider_directory_profile') IS NOT NULL THEN {_qname(schema, 'provider_directory_profile')} END",
        ),
        (
            f"'overlay_oid',to_regclass('{quoted}.provider_directory_address_overlay')::oid::bigint",
            f"'overlay',CASE WHEN to_regclass('{quoted}.provider_directory_address_overlay') IS NOT NULL THEN {_qname(schema, 'provider_directory_address_overlay')} END",
        ),
    )


def _native_matches_replacements(schema):
    quoted = storage_identifier(schema)
    project = _native("p", schema)
    doc = _part("(p->'doctors')", "doctors", schema)
    address = _part("(p->'address')", "address", schema)
    profile = _part("(p->'profile')", "profile", schema)
    return (
        (
            f"p={quoted}.cms_serving_native_snapshot()",
            f"p ?& ARRAY['profile','address','doctors','alias_generation'] AND ((p ? 'overlay')<>(p ? 'overlay_oid')) AND {project}={quoted}.cms_serving_native_snapshot()",
        ),
        ("p->'doctors'->'relation_oids'", f"{doc}->'relations'"),
        ("p->'address'->'relation_oids'", f"{address}->'relations'"),
        (
            f"(p->'profile'->>'profile_target_oid')::bigint=to_regclass('{quoted}.provider_directory_profile')::oid::bigint",
            f"{profile}->'profile_target'={_qname(schema, 'provider_directory_profile')}",
        ),
        (
            f"(p->'profile'->>'evidence_target_oid')::bigint=to_regclass('{quoted}.provider_directory_profile_evidence')::oid::bigint",
            f"{profile}->'evidence_target'={_qname(schema, 'provider_directory_profile_evidence')}",
        ),
    )


def _snapshot_receipt_matches_replacements(schema):
    project = _native("p", schema)
    return (
        ("current_native->'profile'=p->'profile'", f"current_native->'profile'={project}->'profile'"),
        ("current_native->'address'=p->'address'", f"current_native->'address'={project}->'address'"),
        ("current_native->'doctors'=p->'doctors'", f"current_native->'doctors'={project}->'doctors'"),
        ("current_native->'overlay_oid'=p->'overlay_oid'", f"current_native->'overlay'={project}->'overlay'"),
    )


def _current_receipt_matches_replacements(schema):
    project = _native("p", schema)
    return (
        ("current_native->'profile'=p->'profile'", f"current_native->'profile'={project}->'profile'"),
        ("current_native->'address'=p->'address'", f"current_native->'address'={project}->'address'"),
        ("current_native->'doctors'=p->'doctors'", f"current_native->'doctors'={project}->'doctors'"),
    )


def _receipt_matches_replacements(schema):
    return (
        (
            "(p->>'overlay_oid')::bigint IS NULL",
            "(NOT p ? 'overlay' OR p->'overlay'='null'::jsonb OR p ? 'overlay_oid' OR p->'profile' ?| ARRAY['evidence_target_oid','profile_target_oid'] OR p->'address' ? 'relation_oids' OR p->'doctors' ? 'relation_oids')",
        ),
    )


def _require_fresh_replacements(schema):
    return (
        (
            "previous IS DISTINCT FROM prior->kind",
            f"(CASE kind WHEN 'profile' THEN {_part('previous', 'profile', schema)} WHEN 'address' THEN {_part('previous', 'address', schema)} WHEN 'doctors' THEN {_part('previous', 'doctors', schema)} END) IS DISTINCT FROM (CASE kind WHEN 'profile' THEN {_part('(prior->kind)', 'profile', schema)} WHEN 'address' THEN {_part('(prior->kind)', 'address', schema)} WHEN 'doctors' THEN {_part('(prior->kind)', 'doctors', schema)} END)",
        ),
    )


def _serving_replacements(schema):
    replacements_by_function = {
        "cms_serving_oids": _oids_replacements(schema),
        "cms_serving_native_snapshot": _native_snapshot_replacements(schema),
        "cms_serving_native_matches": _native_matches_replacements(schema),
        "cms_serving_snapshot_receipt_matches": _snapshot_receipt_matches_replacements(schema),
        "cms_serving_current_receipt_matches": _current_receipt_matches_replacements(schema),
        "cms_serving_receipt_matches": _receipt_matches_replacements(schema),
        "cms_serving_require_fresh": _require_fresh_replacements(schema),
    }
    for kind in ("doctors", "address"):
        pairs = []
        for alias, count in (("OLD", 2), ("NEW", 1)):
            names = (
                f"CASE WHEN {alias}.origin_lineage_id IS NOT NULL AND {alias}.serving_schema IS NULL AND {alias}.serving_relation_names IS NULL THEN {_declared_names(schema, _DOCTOR_NAMES)} ELSE {_recorded_names(alias)} END"
                if kind == "doctors"
                else _declared_authority_names(alias, schema, _ADDRESS_NAMES)
            )
            pairs.append((f"'relation_oids',{alias}.relation_oids", f"'relations',{names}", count))
        replacements_by_function[f"cms_serving_{kind}_transition"] = tuple(pairs)
    pairs = []
    for alias, count in (("OLD", 2), ("NEW", 1)):
        for old, name in (
            ("evidence_target_oid", "provider_directory_profile_evidence"),
            ("profile_target_oid", "provider_directory_profile"),
        ):
            new = "evidence_target" if old == "evidence_target_oid" else "profile_target"
            pairs.append((f"'{old}',{alias}.{old}", f"'{new}',{_qname(schema, name)}", count))
    replacements_by_function["cms_serving_profile_transition"] = tuple(pairs)
    return replacements_by_function


def _replacements(schema):
    replacements_by_function = {
        "cms_native_input_advance": (
            ("WHERE relation_oid=TG_RELID::bigint;", "WHERE schema_name=TG_TABLE_SCHEMA AND table_name=TG_TABLE_NAME;"),
        ),
        "cms_native_input_revision_guard": (
            (
                "AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name",
                "AND NEW.schema_name=OLD.schema_name",
            ),
        ),
        "cms_serving_archive_result_matches": (
            (
                "            a := p->'archive';",
                "            a := p->'archive';\n            IF prior->'archive'=a THEN RETURN COALESCE(prior->'cms'=p->'cms',false); END IF;",
            ),
            (
                "ARRAY['target_oid','from_revision','to_revision','native_input_hash','delta_rows','delta_sha256']",
                "ARRAY['target_schema','target_table','from_revision','to_revision','native_input_hash','delta_rows','delta_sha256']",
            ),
            ("(SELECT count(*) FROM jsonb_object_keys(a))<>6", "(SELECT count(*) FROM jsonb_object_keys(a))<>7"),
            (
                "ARRAY['target_oid','from_revision','to_revision','delta_rows']",
                "ARRAY['from_revision','to_revision','delta_rows']",
            ),
            (
                "IF (a->>'target_oid')::numeric NOT BETWEEN 1 AND 4294967295",
                f"IF a->>'target_schema' IS DISTINCT FROM '{schema}' OR a->>'target_table' IS DISTINCT FROM 'address_archive_v2'",
            ),
            (
                "            IF prior->'archive'=a THEN RETURN COALESCE(prior->'cms'=p->'cms',false); END IF;\n            IF to_regclass",
                "            IF to_regclass",
            ),
            (
                f"              OR (a->>'target_oid')::bigint IS DISTINCT FROM to_regclass('\"{schema}\".address_archive_v2')::oid::bigint\n",
                "",
            ),
            (
                "WHERE relation_oid=$1 AND schema_name=$2 AND table_name=''address_archive_v2'''",
                "WHERE schema_name=$1 AND table_name=''address_archive_v2'''",
            ),
            (f"USING (a->>'target_oid')::bigint,'{schema}'", "USING a->>'target_schema'"),
        ),
    }

    replacements_by_function.update(_serving_replacements(schema))
    return replacements_by_function


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _canonical_hash(column, schema):
    quoted_schema, quoted_canonical = _literal(storage_identifier(schema)), _literal('"__schema__"')
    literal_schema, literal_canonical = _literal(_literal(schema)), _literal("'__schema__'")
    return (
        f"encode(sha256(convert_to(replace(replace({column},{quoted_schema},{quoted_canonical}),"
        f"{literal_schema},{literal_canonical}),'UTF8')),'hex')"
    )


def _body_replacements(pairs):
    changes = []
    for pair in pairs:
        previous, current = pair[:2]
        count = pair[2] if len(pair) == 3 else 1
        changes.append(f"""IF (length(body)-length(replace(body,{_literal(previous)},'')))/length({_literal(previous)})<>{count} THEN
            RAISE EXCEPTION 'cms_native_named_revision_body_changed'; END IF;
            body:=replace(body,{_literal(previous)},{_literal(current)});
            definition:=replace(definition,{_literal(previous)},{_literal(current)});""")
    return "".join(changes)


def _function_sql(schema, names=None):
    operations = []
    for name, pairs in _replacements(schema).items():
        if names is not None and name not in names:
            continue
        arguments = {
            "cms_serving_oids": "text[]",
            "cms_serving_archive_result_matches": "jsonb",
            "cms_serving_native_matches": "jsonb",
            "cms_serving_snapshot_receipt_matches": "jsonb",
            "cms_serving_current_receipt_matches": "jsonb",
            "cms_serving_receipt_matches": "jsonb",
            "cms_serving_require_fresh": "text,jsonb",
        }.get(name, "")
        signature = storage_relation(schema, name) + "(" + arguments + ")"
        previous_hash, next_hash = _FUNCTION_BODY_DIGESTS[name]
        operations.append(f"""SELECT prosrc,pg_get_functiondef(oid) INTO STRICT body,definition FROM pg_proc WHERE oid=to_regprocedure({_literal(signature)});
            IF {_canonical_hash("body", schema)}={_literal(previous_hash)} THEN
                {_body_replacements(pairs)} EXECUTE definition;
            ELSIF {_canonical_hash("body", schema)}<>{_literal(next_hash)} THEN
                RAISE EXCEPTION 'cms_native_named_revision_body_changed'; END IF;
            IF (SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure({_literal(signature)})) IS DISTINCT FROM body
                OR {_canonical_hash("body", schema)}<>{_literal(next_hash)} THEN
                RAISE EXCEPTION 'cms_native_named_revision_body_changed'; END IF;""")
    return "DO $migration$ DECLARE body text; definition text; BEGIN " + "".join(operations) + " END $migration$"


def _prepared_shape_sql(schema):
    table = storage_relation(schema, "cms_native_input_revision")
    return f"""IF NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass({_literal(table)})
        AND attname='relation_oid' AND NOT attisdropped) THEN
        IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid=to_regclass({_literal(table)}) AND attnum>0 AND NOT attisdropped)
            IS DISTINCT FROM ARRAY['schema_name','table_name','revision','mutation_xid']::text[]
            OR EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass({_literal(table)}) AND attnum>0 AND NOT attisdropped AND
                (format_type(atttypid,atttypmod)<>CASE attname WHEN 'schema_name' THEN 'name' WHEN 'table_name' THEN 'name' WHEN 'revision' THEN 'bigint' WHEN 'mutation_xid' THEN 'xid8' END
                OR attnotnull<>(attname<>'mutation_xid') OR attidentity<>'' OR attgenerated<>''))
            OR EXISTS(SELECT FROM pg_attribute attribute LEFT JOIN pg_attrdef definition ON definition.adrelid=attribute.attrelid AND definition.adnum=attribute.attnum
                WHERE attribute.attrelid=to_regclass({_literal(table)}) AND attribute.attnum>0 AND NOT attribute.attisdropped
                AND pg_get_expr(definition.adbin,definition.adrelid) IS DISTINCT FROM CASE WHEN attribute.attname='revision' THEN '0' END)
            OR (SELECT array_agg(attribute.attname::text ORDER BY key.position) FROM pg_constraint constraint_row
                CROSS JOIN LATERAL unnest(constraint_row.conkey) WITH ORDINALITY key(number,position)
                JOIN pg_attribute attribute ON attribute.attrelid=constraint_row.conrelid AND attribute.attnum=key.number
                WHERE constraint_row.conrelid=to_regclass({_literal(table)}) AND constraint_row.contype='p')
                IS DISTINCT FROM ARRAY['schema_name','table_name']::text[]
            OR (SELECT count(*) FROM pg_constraint WHERE conrelid=to_regclass({_literal(table)}) AND contype='c'
                AND convalidated AND pg_get_expr(conbin,conrelid)='(revision >= 0)')<>1 THEN
            RAISE EXCEPTION 'cms_native_input_named_revision_shape_changed'; END IF;
    END IF;"""


def _key_sql(schema):
    table = storage_relation(schema, "cms_native_input_revision")
    return (
        f"""DO $migration$ BEGIN IF EXISTS(SELECT FROM {table} GROUP BY schema_name,table_name HAVING count(*)>1) THEN
        RAISE EXCEPTION 'cms_native_input_named_revision_ambiguous'; END IF;
        {_prepared_shape_sql(schema)} END $migration$""",
        f"""DO $migration$ BEGIN IF EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass({_literal(table)}) AND attname='relation_oid' AND NOT attisdropped) THEN
        ALTER TABLE {table} DROP CONSTRAINT cms_native_input_revision_pkey, ADD PRIMARY KEY(schema_name,table_name), DROP COLUMN relation_oid;
        END IF; END $migration$""",
    )


def upgrade():
    """Preserve revision values and existing guards; refuse ambiguous historical declarations."""
    schema = plan_release_schema()
    storage_identifier(schema)
    table = storage_relation(schema, "cms_native_input_revision")
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
    check, key = _key_sql(schema)
    op.execute(check)
    op.execute(_function_sql(schema))
    op.execute(key)


def downgrade():
    """Require explicit retained-history retirement before removing named publication metadata."""
    raise RuntimeError("cms_native_input_named_revision_downgrade_unsupported")
