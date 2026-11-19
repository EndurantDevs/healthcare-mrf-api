# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed tables of one CMS National Provider Directory release.

Each release is loaded into its own schema. Column order of a table is the binary COPY
field order written by ``ptg2_scanner --cms-npd-typed-copy``; generated columns come last
and are not part of the COPY. Ids are FHIR ids without the ``<ResourceType>-`` prefix.
There are no foreign keys: references are checked with set-based queries before publication.
"""

import re

SOURCE_ID = "cms-npd"
RELEASE_TABLE = "cms_npd_release"
SCHEMA_PREFIX = "cms_npd_"

# How the control plane starts the importer.
QUEUE_NAME = "arq:CMSNPD"
CONTROL_ADAPTER = {
    "queue": QUEUE_NAME,
    "function": "control_single_job_start",
    "payload": "control_wrapped",
    "target_module": "process.cms_npd_typed_worker",
    "target_function": "process_data",
}

# (file stem, FHIR resource type, main table, address table or None), largest files first.
RESOURCE_FILES = (
    ("06-Practitioner", "Practitioner", "practitioner", "practitioner_address"),
    ("07-PractitionerRole", "PractitionerRole", "practitioner_role", None),
    ("01-Organization", "Organization", "organization", "organization_address"),
    ("02-Location", "Location", "location", None),
    ("03-Endpoint", "Endpoint", "endpoint", None),
    ("08-OrganizationAffiliation", "OrganizationAffiliation", "organization_affiliation", None),
    ("04-HealthcareService", "HealthcareService", "healthcare_service", None),
    ("05-InsurancePlan", "InsurancePlan", "insurance_plan", None),
)

TABLES = (
    "organization",
    "organization_address",
    "location",
    "endpoint",
    "healthcare_service",
    "insurance_plan",
    "organization_affiliation",
    "practitioner",
    "practitioner_address",
    "practitioner_role",
)


def release_schema_name(release_id):
    """Schema that holds one release; ``release_id`` is 64 lowercase hex characters."""
    if len(release_id) != 64 or any(character not in "0123456789abcdef" for character in release_id):
        raise ValueError("cms_npd_release_id_invalid")
    return SCHEMA_PREFIX + release_id[:16]


def release_table_statements(control_schema=None):
    """One row per loaded release in the control schema: the current one and the one before it."""
    if control_schema is None:
        table = RELEASE_TABLE
    elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", control_schema):
        table = f'"{control_schema}".{RELEASE_TABLE}'
    else:
        raise ValueError("cms_npd_control_schema_invalid")
    return (
        f"""CREATE TABLE IF NOT EXISTS {table} (
    release_id        text PRIMARY KEY CHECK (release_id ~ '^[0-9a-f]{{64}}$'),
    schema_name       text NOT NULL UNIQUE CHECK (schema_name ~ '^cms_npd_[0-9a-f]{{16}}$'),
    status            text NOT NULL UNIQUE CHECK (status IN ('current', 'previous')),
    generated_on      date,
    published_at      timestamptz NOT NULL,
    run_id            text,
    row_counts        jsonb NOT NULL,
    unresolved_counts jsonb NOT NULL,
    files             jsonb NOT NULL
)""",
    )


RELEASE_TABLE_STATEMENTS = release_table_statements()  # unqualified: run with search_path set

# Run in the release schema before COPY (unqualified names; set search_path first).
TABLE_STATEMENTS = (
    """CREATE TABLE organization (
    id            text NOT NULL,          -- "<npi>", "<uuid>", or other CMS forms ("State-CA", "<npi>-part-1")
    npi           bigint,                 -- first NPI identifier
    other_npis    bigint[],               -- further NPI identifiers, source order
    pseudo_eins   uuid[],                 -- us-pseudo-ein identifiers
    npi_since     date,                   -- period.start of the first NPI identifier
    name          text,
    active        boolean,
    type_code     text,                   -- type[0].coding[0].code: prov | pay | team | govt | ...
    type_text     text,                   -- type[0].text when it differs from the code's display (e.g. "ein")
    other_type_codes text[],              -- further type[] codes and texts (not type_code/type_text), source order
    part_of       text,                   -- partOf organization id
    verification  text,                   -- base-ext-verification-status code
    endpoint_ids  uuid[],
    phones        text[],
    faxes         text[],
    urls          text[],
    updated_at    timestamptz,
    entity_id     uuid GENERATED ALWAYS AS (
        CASE WHEN id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN id::uuid
             ELSE md5('cms-npd/Organization/' || id)::uuid END
    ) STORED                              -- public entity id; not part of the COPY column list
)""",
    """CREATE TABLE organization_address (
    organization_id text NOT NULL,
    ord           integer NOT NULL,       -- 0-based position in address[]
    use           text,                   -- work | billing
    type          text,                   -- physical | postal
    line1         text,
    line2         text,                   -- line[1..] joined with ", "
    city          text,
    state         text,
    postal_code   text,
    country       text
)""",
    """CREATE TABLE location (
    id            uuid NOT NULL,
    name          text,
    description   text,                   -- NULL when equal to name
    status        text,
    line1         text,
    line2         text,
    city          text,
    state         text,
    postal_code   text,
    country       text,
    longitude     double precision,
    latitude      double precision,
    organization_id text,                 -- managingOrganization
    phones        text[],
    faxes         text[],
    part_of       uuid,                   -- partOf location id
    updated_at    timestamptz
)""",
    """CREATE TABLE endpoint (
    id            uuid NOT NULL,
    connection_type text,                 -- connectionType.code
    address       text,
    name          text,
    status        text,
    verification  text,
    organization_id text,                 -- managingOrganization
    updated_at    timestamptz
)""",
    """CREATE TABLE healthcare_service (
    id            uuid NOT NULL,
    active        boolean,
    network_id    text,                   -- base-ext-network-reference organization id
    provided_by   text,                   -- providedBy organization id
    location_ids  uuid[],
    updated_at    timestamptz
)""",
    """CREATE TABLE insurance_plan (
    id            uuid NOT NULL,
    name          text,
    status        text,
    aliases       text[],
    owned_by      text,                   -- ownedBy organization id
    network_ids   text[],
    type_text     text,                   -- type[0].text
    period_start  date,
    period_end    date,
    administered_by text,                 -- administeredBy organization id
    coverage_area_ids uuid[],
    -- plan[].network, plan[].coverageArea and coverage[].network as
    -- [{"field","parent","ord","target","period_start","period_end"}], NULL when none; the period is
    -- the parent element's when it has one, otherwise the plan's
    nested_references jsonb,
    updated_at    timestamptz
)""",
    """CREATE TABLE organization_affiliation (
    id            uuid NOT NULL,
    active        boolean,
    code          text,                   -- code[0].coding[0].code
    organization_id text,
    participating_organization_id text,
    network_ids   text[],
    updated_at    timestamptz
)""",
    """CREATE TABLE practitioner (
    npi           bigint NOT NULL,        -- from the id "Practitioner-<npi>"
    active        boolean,
    gender        text,
    family        text,                   -- name[0] (the official name)
    given         text[],
    prefix        text,
    suffix        text,
    other_names   jsonb,                  -- name[1..] as [{"use","family","given":[],"prefix","suffix","since"}], NULL when none
    old_npis      bigint[],               -- NPI identifiers other than the id's NPI
    npi_explicit  boolean NOT NULL,       -- an NPI identifier carries the id's NPI, which is checksum-valid
    npi_since     date,                   -- period.start of the id's NPI identifier
    npi_until     date,                   -- period.end of the id's NPI identifier
    identity_verified          boolean,   -- base-ext-cms-identity-verified
    medicare_enrolled          boolean,   -- base-ext-cms_medicare_enrollment
    in_exclusion_list          boolean,   -- base-ext-hhs-in-exclusion-list
    aligned_with_data_network  boolean,   -- base-ext-cms_aligned_with_data_network
    taxonomy_codes   text[],              -- qualification codes in HealthcareIndividualTaxonomyVS, source order, deduplicated
    credentials      text[],              -- qualification codes in any other system (or the text when uncoded)
    license_states   text[],              -- qualifications carrying a license identifier: issuer id without "State-"
    license_numbers  text[],              --   same positions as license_states
    license_codes    text[],              --   same positions: the qualification code
    phones        text[],
    faxes         text[],
    updated_at    timestamptz
)""",
    """CREATE TABLE practitioner_address (
    npi           bigint NOT NULL,
    ord           integer NOT NULL,
    use           text,
    type          text,
    line1         text,
    line2         text,
    city          text,
    state         text,
    postal_code   text,
    country       text
)""",
    """CREATE TABLE practitioner_role (
    id            uuid NOT NULL,
    practitioner_npi bigint,
    organization_id text,
    active        boolean,
    location_ids  uuid[],
    endpoint_ids  uuid[],
    network_id    text,                   -- base-ext-network-reference organization id
    new_patients  text,                   -- base-ext-newpatients acceptingPatients code (e.g. newpt)
    specialty_codes text[],               -- specialty[].coding[0].code, or the text when uncoded
    period_start  date,
    period_end    date,
    phones        text[],
    faxes         text[],
    insurance_plan_ids uuid[],
    network_ids   text[],                 -- network[] organization ids
    updated_at    timestamptz
)""",
)


def plan_names_network(plan, organization_id):
    """SQL: the plan names the organization in network[], plan[].network or coverage[].network."""
    nested = " OR ".join(
        f"{plan}.nested_references @> jsonb_build_array(jsonb_build_object('field', '{field}', 'target', {organization_id}))"
        for field in ("plan.network", "coverage.network")
    )
    return f"({plan}.network_ids @> ARRAY[{organization_id}] OR {nested})"


# Run after COPY: keys, lookups, the derived network set, statistics.
# A duplicate id fails its key build, which fails the release before publication.
FINISH_STATEMENTS = (
    "ALTER TABLE organization ADD PRIMARY KEY (id)",
    "ALTER TABLE organization_address ADD PRIMARY KEY (organization_id, ord)",
    "ALTER TABLE location ADD PRIMARY KEY (id)",
    "ALTER TABLE endpoint ADD PRIMARY KEY (id)",
    "ALTER TABLE healthcare_service ADD PRIMARY KEY (id)",
    "ALTER TABLE insurance_plan ADD PRIMARY KEY (id)",
    "ALTER TABLE organization_affiliation ADD PRIMARY KEY (id)",
    "ALTER TABLE practitioner ADD PRIMARY KEY (npi)",
    "ALTER TABLE practitioner_address ADD PRIMARY KEY (npi, ord)",
    "ALTER TABLE practitioner_role ADD PRIMARY KEY (id)",
    "CREATE INDEX ON organization (npi) WHERE npi IS NOT NULL",
    "CREATE INDEX ON organization (part_of) WHERE part_of IS NOT NULL",
    "CREATE INDEX ON location (organization_id) WHERE organization_id IS NOT NULL",
    "CREATE INDEX ON endpoint (organization_id) WHERE organization_id IS NOT NULL",
    "CREATE INDEX ON organization_affiliation (participating_organization_id)",
    "CREATE INDEX ON organization_affiliation (organization_id)",
    "CREATE INDEX ON practitioner_role (practitioner_npi)",
    "CREATE INDEX ON practitioner_role (organization_id) WHERE organization_id IS NOT NULL",
    "CREATE INDEX ON practitioner_role USING gin (location_ids)",
    # Networks are the organizations some resource names as its network, or that declare the role.
    f"""CREATE TABLE network AS
SELECT o.id AS organization_id, md5('cms-npd/network/' || o.id)::uuid AS entity_id,
       (SELECT p.id FROM insurance_plan p WHERE {plan_names_network("p", "o.id")} ORDER BY p.id LIMIT 1) AS first_plan_id
FROM organization o
WHERE o.id IN (
    SELECT unnest(network_ids) FROM insurance_plan
    UNION SELECT r->>'target' FROM insurance_plan, jsonb_array_elements(nested_references) r
        WHERE r->>'field' IN ('plan.network', 'coverage.network')
    UNION SELECT unnest(network_ids) FROM organization_affiliation
    UNION SELECT network_id FROM healthcare_service WHERE network_id IS NOT NULL
    UNION SELECT unnest(network_ids || network_id) FROM practitioner_role  -- one scan; a NULL element matches nothing
    UNION SELECT id FROM organization
        WHERE type_code = 'ntwk' OR type_text = 'ntwk' OR 'ntwk' = ANY(other_type_codes)
)""",
    "ALTER TABLE network ADD PRIMARY KEY (organization_id)",
    "CREATE UNIQUE INDEX ON network (entity_id)",
    "CREATE UNIQUE INDEX ON organization (entity_id)",
    # Per table: a bare ANALYZE would scan every table in the database.
    *(f"ANALYZE {table}" for table in (*TABLES, "network")),
)
