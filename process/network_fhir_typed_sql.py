# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict direct-role graph projection and set extraction, without per-edge JSON."""

from process.network_fhir_typed_contract import ADDRESS_FIELDS, CELL_COLUMNS

CELL_DDL = """resource_type text COLLATE "C" NOT NULL,resource_id text COLLATE "C" NOT NULL,
payload_sha256 text NOT NULL,npi text,active boolean,subject_ref text,network_refs text[] NOT NULL,
location_refs text[] NOT NULL,period_start text,period_end text,location_status text,
first_line text,second_line text,city_name text,state_name text,postal_code text,country_code text,site_id uuid,
PRIMARY KEY(resource_type,resource_id)"""
ADDRESS_DDL = """raw_key text PRIMARY KEY,first_line text,second_line text,city_name text,state_name text,
postal_code text,country_code text,address_key uuid NOT NULL,canonical_identity text NOT NULL"""
PAIR_DDL = """network_source_key text NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
location_id uuid NOT NULL,address_key uuid NOT NULL,canonical_identity text NOT NULL,resource_type text NOT NULL,
resource_id text NOT NULL,location_resource_id text NOT NULL,evidence_basis text NOT NULL,reference_rows bigint NOT NULL,
PRIMARY KEY(resource_type,resource_id,network_source_key,location_resource_id)"""

_TEXT_FIELDS = (
    "practitioner_ref",
    "participating_organization_ref",
    "period_start",
    "period_end",
    "status",
    *ADDRESS_FIELDS,
)
_SHAPE = " AND ".join(
    "(p->'%s' IS NULL OR jsonb_typeof(p->'%s') IN ('null','string'))" % (key, key) for key in _TEXT_FIELDS
)
_SHAPE += " AND (p->'active' IS NULL OR jsonb_typeof(p->'active') IN ('null','boolean'))"
_SHAPE += " AND (p->'npi' IS NULL OR jsonb_typeof(p->'npi') IN ('null','string','number'))"
for _key in ("network_refs", "location_refs"):
    _SHAPE += (
        " AND (p->'%s' IS NULL OR p->'%s'='null'::jsonb OR (jsonb_typeof(p->'%s')='array' AND NOT EXISTS(SELECT FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p->'%s')='array' THEN p->'%s' ELSE '[]'::jsonb END) v WHERE jsonb_typeof(v)<>'string')))"
        % ((_key,) * 5)
    )

_ROLE_FIELDS = (
    "npi",
    "active",
    "practitioner_ref",
    "participating_organization_ref",
    "network_refs",
    "location_refs",
    "period_start",
    "period_end",
)


def _selected_json(fields):
    return "jsonb_build_object(" + ",".join("'" + key + "',raw->'" + key + "'" for key in fields) + ")"


def _project_sql(plan_tiers=False):
    # The plan pass has already validated the selector and stored its deduplicated references.
    plan_references = ",plan.network_refs AS plan_network_refs" if plan_tiers else ""
    plan_join = (
        " LEFT JOIN {plans} plan ON resource_type IN ('OrganizationAffiliation','PractitionerRole') "
        "AND plan.resource_id=split_part(raw->'insurance_plan_refs'->>0,'/',2)"
        if plan_tiers
        else ""
    )
    role_fields = _ROLE_FIELDS + (("network_key_id",) if plan_tiers else ())
    extra_references = (
        "||ARRAY(SELECT ref FROM jsonb_array_elements_text(coalesce(plan_network_refs,'[]'::jsonb)) ref "
        "WHERE NOT coalesce(nullif(p->'network_refs','null'::jsonb),'[]'::jsonb) ? ref) "
        "||CASE WHEN p->>'network_key_id' IS NULL THEN ARRAY[]::text[] "
        "ELSE ARRAY['UHCPlanTier/'||(p->>'network_key_id')] END"
        if plan_tiers
        else ""
    )
    return (
        """WITH raw_page AS MATERIALIZED (
 SELECT resource_type,resource_id,payload_hash,
 CASE WHEN jsonb_typeof(payload_json)='string' THEN (payload_json #>> '{{}}')::jsonb ELSE payload_json END raw,site_id
 FROM jsonb_to_recordset($1::jsonb) r(resource_type text,resource_id text,payload_hash text,payload_json jsonb,site_id uuid)
 WHERE resource_type=ANY($2::text[])
) , parsed AS MATERIALIZED (
 SELECT resource_type,raw_page.resource_id,payload_hash,site_id"""
        + plan_references
        + """,CASE
 WHEN resource_type IN ('Organization','Practitioner') THEN """
        + _selected_json(("npi", "active"))
        + """
 WHEN resource_type='Location' THEN """
        + _selected_json(("status", *ADDRESS_FIELDS))
        + """
 ELSE """
        + _selected_json(role_fields)
        + " END p FROM raw_page"
        + plan_join
        + """
) INSERT INTO {cells}("""
        + ",".join(CELL_COLUMNS)
        + """ )
SELECT resource_type,resource_id,payload_hash,p->>'npi',(p->>'active')::boolean,
CASE WHEN resource_type='PractitionerRole' THEN p->>'practitioner_ref'
 WHEN resource_type='OrganizationAffiliation' THEN p->>'participating_organization_ref' END,
ARRAY(SELECT jsonb_array_elements_text(coalesce(nullif(p->'network_refs','null'::jsonb),'[]'::jsonb)))"""
        + extra_references
        + """,
ARRAY(SELECT jsonb_array_elements_text(coalesce(nullif(p->'location_refs','null'::jsonb),'[]'::jsonb))),
p->>'period_start',p->>'period_end',p->>'status',"""
        + ",".join("p->>'" + key + "'" for key in ADDRESS_FIELDS)
        + """,site_id
FROM parsed WHERE """
        + _SHAPE
    )


PROJECT_SQL = _project_sql()
PLAN_TIER_PROJECT_SQL = _project_sql(True)

DATE_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2}))?$"


def _period(column, operator):
    return """CASE WHEN {c} IS NULL THEN true WHEN {c} ~ '{pattern}'
 AND pg_input_is_valid(left({c},10),'date') AND (length({c})=10 OR pg_input_is_valid({c},'timestamptz'))
 THEN left({c},10)::date {op} $2::date ELSE false END""".format(c=column, op=operator, pattern=DATE_PATTERN)


def reference_sql(expression):
    """Render the existing scoped reference expression without changing its SQL semantics."""
    return "CASE WHEN starts_with({e},$1::text||'/') THEN substr({e},length($1::text)+2) ELSE {e} END".format(
        e=expression
    )


_NPI = """CASE WHEN p.npi ~ '^[12][0-9]{9}$' THEN
(SELECT sum(CASE WHEN ((14-i)%2)=1 THEN ((substr('80840'||p.npi,i+1,1)::int*2)/10)
+((substr('80840'||p.npi,i+1,1)::int*2)%10) ELSE substr('80840'||p.npi,i+1,1)::int END)
FROM generate_series(0,14) i)%10=0 ELSE false END"""

_ROLE_SCOPE = " AND ".join((_period("r.period_start", "<="), _period("r.period_end", ">=")))
_ROLE_SCOPE += " AND CASE WHEN r.period_start IS NULL OR r.period_end IS NULL THEN true WHEN pg_input_is_valid(left(r.period_start,10),'date') AND pg_input_is_valid(left(r.period_end,10),'date') THEN left(r.period_start,10)::date<=left(r.period_end,10)::date ELSE false END"
_ADDRESS_JOIN = (
    "a.raw_key=encode(sha256(convert_to(jsonb_build_array("
    + ",".join("l." + key for key in ADDRESS_FIELDS)
    + ")::text,'UTF8')),'hex') AND "
    + " AND ".join("l." + key + " IS NOT DISTINCT FROM a." + key for key in ADDRESS_FIELDS)
)


def _reference_predicate(alias, expression, kind):
    reference = reference_sql(expression)
    return (
        alias
        + ".resource_type="
        + kind
        + " AND "
        + alias
        + ".resource_id=split_part("
        + reference
        + ", '/', 2) AND "
        + reference
        + "="
        + alias
        + ".resource_type||'/'||"
        + alias
        + ".resource_id"
    )


_PARENT_KIND = "CASE WHEN r.resource_type='PractitionerRole' THEN 'Practitioner' ELSE 'Organization' END"
_PARENT_LOOKUP = (
    "JOIN LATERAL (SELECT * FROM {cells} p WHERE "
    + _reference_predicate("p", "r.subject_ref", _PARENT_KIND)
    + " OFFSET 0) p ON true"
)
_NETWORK_LOOKUP = (
    "JOIN LATERAL (SELECT net.resource_id FROM {cells} net WHERE "
    + _reference_predicate("net", "raw.ref", "'Organization'")
    + " AND net.active IS DISTINCT FROM false OFFSET 0) net ON true"
)
PAIR_ROLE_PAGE_SIZE = 512


def _role_page_sql(first_parameter, limit=128):
    """Bound the driving relation before planning the reference joins."""
    return f"""WITH role_page AS MATERIALIZED (
 SELECT * FROM {{cells}}
 WHERE resource_type IN ('PractitionerRole','OrganizationAffiliation')
 AND resource_type=${first_parameter}::text AND resource_type=${first_parameter + 2}::text
 AND resource_id>${first_parameter + 1}::text AND resource_id<=${first_parameter + 3}::text
 ORDER BY resource_id LIMIT {limit}
)
"""


PAIR_SQL = (
    _role_page_sql(3, PAIR_ROLE_PAGE_SIZE)
    + """SELECT nr.resource_id,'npi'::text,p.npi,l.site_id,a.address_key,a.canonical_identity,
r.resource_type,r.resource_id,l.resource_id,
CASE WHEN r.resource_type='OrganizationAffiliation' THEN 'affiliation_network_participant_location'
 ELSE 'role_network_direct_location' END,nr.multiplicity*lr.multiplicity
FROM role_page r """
    + _PARENT_LOOKUP
    + """
JOIN LATERAL (SELECT net.resource_id,count(*)::bigint multiplicity FROM unnest(r.network_refs) raw(ref)
 """
    + _NETWORK_LOOKUP
    + """ GROUP BY net.resource_id) nr ON true
JOIN LATERAL (SELECT """
    + reference_sql("raw.ref")
    + """ COLLATE "C" AS ref,count(*)::bigint multiplicity FROM unnest(r.location_refs) raw(ref) GROUP BY 1) lr ON true
JOIN LATERAL (SELECT * FROM {cells} l WHERE l.resource_type='Location' AND l.resource_id=split_part(lr.ref,'/',2)
 AND lr.ref=l.resource_type||'/'||l.resource_id AND l.location_status IS DISTINCT FROM 'inactive' OFFSET 0) l ON true
JOIN LATERAL (SELECT * FROM {addresses} a WHERE """
    + _ADDRESS_JOIN
    + """ OFFSET 0) a ON true
WHERE r.resource_type IN ('PractitionerRole','OrganizationAffiliation') AND r.active IS DISTINCT FROM false
AND (r.resource_type,r.resource_id)>($3::text,$4::text) AND (r.resource_type,r.resource_id)<=($5::text,$6::text)
AND p.active IS DISTINCT FROM false AND (r.npi IS NULL OR r.npi=p.npi)
AND """
    + _NPI
    + " AND "
    + _ROLE_SCOPE
    + " AND a.canonical_identity LIKE '%|street'"
)

# The raw checker independently normalizes typed reference arrays and eligibility in
# Python. Its SQL joins those validated reference targets without producer predicates.
CHECKER_DDL = """resource_type text COLLATE "C" NOT NULL,resource_id text COLLATE "C" NOT NULL,subject_kind text NOT NULL,subject_id text,
network_ids text[] NOT NULL,location_ids text[] NOT NULL,eligible boolean NOT NULL,valid_npi boolean NOT NULL,
PRIMARY KEY(resource_type,resource_id)"""
CHECKER_PAIR_SQL = (
    _role_page_sql(1, PAIR_ROLE_PAGE_SIZE)
    + """SELECT net.resource_id,'npi'::text,p.npi,l.site_id,a.address_key,a.canonical_identity,
r.resource_type,r.resource_id,l.resource_id,
CASE WHEN r.resource_type='OrganizationAffiliation' THEN 'affiliation_network_participant_location'
 ELSE 'role_network_direct_location' END,ni.multiplicity*li.multiplicity
FROM role_page r JOIN LATERAL (SELECT * FROM {checks} x WHERE (x.resource_type,x.resource_id)=(r.resource_type,r.resource_id) OFFSET 0) x ON true
JOIN LATERAL (SELECT * FROM {cells} p WHERE p.resource_type=x.subject_kind AND p.resource_id=x.subject_id OFFSET 0) p ON true
JOIN LATERAL (SELECT * FROM {checks} px WHERE (px.resource_type,px.resource_id)=(p.resource_type,p.resource_id) OFFSET 0) px ON true
CROSS JOIN LATERAL (SELECT id,count(*)::bigint multiplicity FROM unnest(x.location_ids) id GROUP BY id) li
JOIN LATERAL (SELECT * FROM {cells} l WHERE l.resource_type='Location' AND l.resource_id=li.id OFFSET 0) l ON true
JOIN LATERAL (SELECT * FROM {checks} lx WHERE (lx.resource_type,lx.resource_id)=(l.resource_type,l.resource_id) OFFSET 0) lx ON true
CROSS JOIN LATERAL (SELECT id,count(*)::bigint multiplicity FROM unnest(x.network_ids) id GROUP BY id) ni
JOIN LATERAL (SELECT * FROM {cells} net WHERE net.resource_type='Organization' AND net.resource_id=ni.id OFFSET 0) net ON true
JOIN LATERAL (SELECT * FROM {checks} nx WHERE (nx.resource_type,nx.resource_id)=(net.resource_type,net.resource_id) OFFSET 0) nx ON true
JOIN LATERAL (SELECT * FROM {addresses} a WHERE """
    + _ADDRESS_JOIN
    + """ OFFSET 0) a ON true
WHERE r.resource_type IN ('PractitionerRole','OrganizationAffiliation') AND x.eligible AND px.eligible
AND (r.resource_type,r.resource_id)>($1::text,$2::text) AND (r.resource_type,r.resource_id)<=($3::text,$4::text)
AND lx.eligible AND nx.eligible AND px.valid_npi AND (r.npi IS NULL OR p.npi=r.npi)
AND a.canonical_identity LIKE '%|street'"""
)

PROVIDER_ONLY_PAGE_SIZE = 16384
PROVIDER_ONLY_SQL = (
    _role_page_sql(3, PROVIDER_ONLY_PAGE_SIZE)
    + """SELECT nr.resource_id AS network_source_key,p.npi AS provider_id,r.resource_type,r.resource_id
FROM role_page r """
    + _PARENT_LOOKUP
    + """
JOIN LATERAL (SELECT DISTINCT net.resource_id FROM unnest(r.network_refs) raw(ref)
 """
    + _NETWORK_LOOKUP
    + """ ) nr ON true
WHERE r.resource_type IN ('PractitionerRole','OrganizationAffiliation') AND r.active IS DISTINCT FROM false
AND (r.resource_type,r.resource_id)>($3::text,$4::text) AND (r.resource_type,r.resource_id)<=($5::text,$6::text)
AND p.active IS DISTINCT FROM false AND (r.npi IS NULL OR r.npi=p.npi) AND cardinality(r.location_refs)=0
AND """
    + _NPI
    + " AND "
    + _ROLE_SCOPE
)


def _plan_tier_networks(query):
    if query.count(_NETWORK_LOOKUP) != 1:
        raise ValueError("registry_fhir_typed_network_template_invalid")
    extended = _NETWORK_LOOKUP.replace(
        " OFFSET 0) net ON true",
        " UNION ALL SELECT raw.ref WHERE raw.ref ~ '^UHCPlanTier/[0-9a-f]{64}$' OFFSET 0) net ON true",
    )
    return query.replace(_NETWORK_LOOKUP, extended)


PLAN_TIER_PAIR_SQL = _plan_tier_networks(PAIR_SQL)
PLAN_TIER_PROVIDER_ONLY_SQL = _plan_tier_networks(PROVIDER_ONLY_SQL)

_CHECKER_NETWORK = """JOIN LATERAL (SELECT * FROM {cells} net WHERE net.resource_type='Organization' AND net.resource_id=ni.id OFFSET 0) net ON true
JOIN LATERAL (SELECT * FROM {checks} nx WHERE (nx.resource_type,nx.resource_id)=(net.resource_type,net.resource_id) OFFSET 0) nx ON true"""
_CHECKER_PLAN_TIER_NETWORK = """JOIN LATERAL (
 SELECT ni.id AS resource_id WHERE ni.id ~ '^UHCPlanTier/[0-9a-f]{64}$'
 UNION ALL SELECT net.resource_id FROM {cells} net JOIN {checks} nx
 ON (nx.resource_type,nx.resource_id)=(net.resource_type,net.resource_id)
 WHERE net.resource_type='Organization' AND net.resource_id=ni.id AND nx.eligible OFFSET 0
) net ON true"""
if CHECKER_PAIR_SQL.count(_CHECKER_NETWORK) != 1 or CHECKER_PAIR_SQL.count("AND nx.eligible") != 1:
    raise ValueError("registry_fhir_typed_network_template_invalid")
PLAN_TIER_CHECKER_PAIR_SQL = CHECKER_PAIR_SQL.replace("AND nx.eligible", "").replace(
    _CHECKER_NETWORK, _CHECKER_PLAN_TIER_NETWORK
)
