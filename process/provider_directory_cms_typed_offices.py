# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact admitted CMS non-NPI offices over existing native Location preparation."""

from process import entity_address_candidate_preparation as preparation
from process.network_cms_provider_identity import cms_provider_sql_fields

CMS_OFFICE_READ_TABLES = (
    "provider_directory_cms_npd_resource_witness",
    "provider_directory_cms_candidate_coverage",
    "provider_directory_entity_source_binding",
)

_OFFICES = """, cms_datasets AS MATERIALIZED (
 SELECT DISTINCT current.dataset_id,current.run_id,current.published_at,proof.release_id
 FROM current_datasets current JOIN endpoint_aliases alias ON alias.endpoint_id=current.endpoint_id
 JOIN {schema}.provider_directory_endpoint_dataset dataset ON dataset.dataset_id=current.dataset_id
 JOIN {schema}.provider_directory_cms_candidate_coverage proof ON proof.dataset_id=dataset.dataset_id
 AND proof.endpoint_id=dataset.endpoint_id AND proof.dataset_hash=dataset.dataset_hash AND proof.proof_version=2
 WHERE alias.source_id='cms-npd'
), page AS MATERIALIZED (
 SELECT resource.*,resource.payload_json::jsonb AS payload,dataset.release_id,dataset.run_id,dataset.published_at
 FROM cms_datasets dataset JOIN {schema}.provider_directory_dataset_resource resource USING(dataset_id)
 WHERE resource.resource_type IN ('PractitionerRole','OrganizationAffiliation')
), providers AS MATERIALIZED (
 SELECT page.*,provider.payload_json::jsonb AS provider_payload,{provider_id} AS provider_id
 FROM page LEFT JOIN {schema}.provider_directory_dataset_resource provider ON provider.dataset_id=page.dataset_id
 AND provider.resource_type=CASE page.resource_type WHEN 'PractitionerRole' THEN 'Practitioner' ELSE 'Organization' END
 AND provider.resource_id=CASE
 WHEN page.resource_type='PractitionerRole' AND page.payload->>'practitioner_ref' ~ '^Practitioner/[A-Za-z0-9.-]{{1,64}}$'
 THEN split_part(page.payload->>'practitioner_ref','/',2)
 WHEN page.resource_type='OrganizationAffiliation'
 AND page.payload->>'participating_organization_ref' ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
 THEN split_part(page.payload->>'participating_organization_ref','/',2) END
 LEFT JOIN {schema}.provider_directory_cms_npd_resource_witness page_witness
 ON page_witness.dataset_id=page.dataset_id AND page_witness.resource_type=page.resource_type
 AND page_witness.resource_id=page.resource_id AND page_witness.source_id='cms-npd'
 AND page_witness.release_id=page.release_id AND page_witness.normalized_payload_hash=page.payload_hash
 AND (page.acquired_resource_sha256 IS NULL OR page.acquired_resource_sha256=page_witness.raw_payload_sha256)
 LEFT JOIN {schema}.provider_directory_cms_npd_resource_witness provider_witness
 ON provider_witness.dataset_id=provider.dataset_id AND provider_witness.resource_type=provider.resource_type
 AND provider_witness.resource_id=provider.resource_id AND provider_witness.source_id='cms-npd'
 AND provider_witness.release_id=page.release_id AND provider_witness.normalized_payload_hash=provider.payload_hash
 AND (provider.acquired_resource_sha256 IS NULL OR provider.acquired_resource_sha256=provider_witness.raw_payload_sha256)
 WHERE coalesce(page.payload->>'npi',provider.payload_json::jsonb->>'npi') IS NULL
 AND page.resource_id=page.payload->>'resource_id'
 AND (page.payload->>'source_id' IS NULL OR page.payload->>'source_id'='cms-npd')
 AND page.payload->'active' IS DISTINCT FROM 'false'::jsonb
 AND (page.payload->>'period_start' IS NULL OR (pg_input_is_valid(left(page.payload->>'period_start',10),'date')
 AND left(page.payload->>'period_start',10)<={as_of}))
 AND (page.payload->>'period_end' IS NULL OR (pg_input_is_valid(left(page.payload->>'period_end',10),'date')
 AND left(page.payload->>'period_end',10)>={as_of}))
), selected AS MATERIALIZED (
 SELECT providers.*,reference AS location_reference FROM providers CROSS JOIN LATERAL jsonb_array_elements_text(
 CASE WHEN jsonb_typeof(payload->'location_refs')='array' THEN payload->'location_refs' ELSE '[]'::jsonb END) refs(reference)
 WHERE provider_id IS NOT NULL AND reference ~ '^Location/[A-Za-z0-9.-]{{1,64}}$'
), offices AS MATERIALIZED (
 SELECT selected.*,location.payload_json::jsonb AS office,site.site_id,physical.address_key AS native_address_key,
 physical.first_line,physical.second_line,physical.city_name,physical.state_name,physical.postal_code,physical.country_code
 FROM selected LEFT JOIN {schema}.provider_directory_dataset_resource location
 ON location.dataset_id=selected.dataset_id AND location.resource_type='Location'
 AND location.resource_id=split_part(selected.location_reference,'/',2)
 LEFT JOIN {schema}.provider_directory_cms_npd_resource_witness location_witness
 ON location_witness.dataset_id=location.dataset_id AND location_witness.resource_type='Location'
 AND location_witness.resource_id=location.resource_id AND location_witness.source_id='cms-npd'
 AND location_witness.release_id=selected.release_id AND location_witness.normalized_payload_hash=location.payload_hash
 AND (location.acquired_resource_sha256 IS NULL OR location.acquired_resource_sha256=location_witness.raw_payload_sha256)
 LEFT JOIN {schema}.provider_directory_entity_source_binding site ON site.source_id='cms-npd'
 AND site.resource_type='Location' AND site.resource_id=location_witness.resource_id
 LEFT JOIN {schema}.provider_directory_location physical ON physical.source_id='cms-npd'
 AND physical.resource_id=location.resource_id AND physical.last_seen_run_id=selected.run_id
), checked AS MATERIALIZED (
 SELECT *,site_id IS NULL OR {key_invalid}
 OR {address_invalid} AS invalid
 FROM offices WHERE {keyable}
), complete AS MATERIALIZED (
 SELECT (CASE WHEN EXISTS(SELECT 1 FROM checked WHERE invalid) {partial_refusal}
 THEN 'cms_typed_office_correspondence_unavailable' ELSE '0' END)::integer AS accepted
)"""

_ROWS = """
SELECT 'provider_directory'::varchar AS entity_type,provider_id::varchar AS entity_id,NULL::bigint AS npi,
 NULL::bigint AS inferred_npi,NULL::float8 AS inference_confidence,NULL::varchar AS inference_method,
 coalesce(provider_payload->>'full_name',provider_payload->>'name')::varchar AS entity_name,
 NULL::varchar AS entity_subtype,'practice'::varchar AS type,
 ARRAY[0]::int[] AS taxonomy_array,ARRAY[0]::int[] AS plans_network_array,
 ARRAY[0]::int[] AS procedures_array,ARRAY[0]::int[] AS medications_array,
 ARRAY[]::varchar[] AS aca_plan_array,ARRAY[]::varchar[] AS aca_network_array,
 ARRAY[]::varchar[] AS ptg_plan_array,ARRAY[]::varchar[] AS ptg_source_array,ARRAY[]::varchar[] AS group_plan_array,
 {base_version}::varchar AS base_address_version,
 (office->>'first_line')::varchar AS first_line,(office->>'second_line')::varchar AS second_line,
 (office->>'city_name')::varchar AS city_name,(office->>'state_name')::varchar AS state_name,
 (office->>'postal_code')::varchar AS postal_code,coalesce(nullif(office->>'country_code',''),'US')::varchar AS country_code,
 (office->>'telephone_number')::varchar AS telephone_number,(office->>'fax_number')::varchar AS fax_number,
 NULL::varchar AS formatted_address,{latitude} AS lat,{longitude} AS long,NULL::date AS date_added,
 NULL::varchar AS place_id,{address_key} AS address_key,{updated_at} AS updated_at,
 'provider_directory_fhir'::varchar AS address_source,
 ('provider_directory_fhir:cms_typed:cms-npd:'||encode(sha256(convert_to(jsonb_build_array(dataset_id,release_id,resource_type,resource_id,
 payload_hash,provider_id,site_id)::text,'UTF8')),'hex'))::varchar AS source_record_id
FROM checked OFFSET (SELECT accepted FROM complete)
"""


def _typed_office_ctes(native, schema, *, partial_refresh):
    """Bind admitted identities, exact physical offices and the shared keyability rule."""
    identity = cms_provider_sql_fields(5)["provider_id"].replace("$5", "'cms-npd'")
    is_virtual = preparation.has_source_query()
    inputs = preparation.current() or preparation._SOURCE_QUERY.get()
    as_of = native._sql_literal(inputs.semantic_as_of) if inputs and inputs.semantic_as_of else "CURRENT_DATE::text"
    expression_by_field = {
        name: "office->>'" + name + "'" for name in ("first_line", "city_name", "state_name", "postal_code")
    }
    address_invalid = (
        " OR ".join(
            name + " IS DISTINCT FROM office->>'" + name + "'"
            for name in ("first_line", "second_line", "city_name", "state_name", "postal_code")
        )
        + " OR coalesce(nullif(country_code,''),'US') IS DISTINCT FROM coalesce(nullif(office->>'country_code',''),'US')"
    )
    offices = _OFFICES.format(
        schema=schema,
        provider_id=identity,
        as_of=as_of,
        key_invalid="false"
        if is_virtual
        else "native_address_key IS NULL OR NOT pg_input_is_valid(native_address_key,'uuid')",
        address_invalid="false" if is_virtual else "(" + address_invalid + ")",
        keyable=native._address_source_keyable_predicate(
            first_line=expression_by_field["first_line"],
            city=expression_by_field["city_name"],
            state=expression_by_field["state_name"],
            zip_code=expression_by_field["postal_code"],
            country="coalesce(nullif(office->>'country_code',''),'US')",
        ),
        partial_refusal="OR EXISTS(SELECT 1 FROM checked)" if partial_refresh else "",
    )
    if is_virtual:
        offices = offices.replace(
            "physical.address_key AS native_address_key,\n physical.first_line,physical.second_line,physical.city_name,physical.state_name,physical.postal_code,physical.country_code",
            "NULL::text AS native_address_key,NULL::text AS first_line,NULL::text AS second_line,"
            "NULL::text AS city_name,NULL::text AS state_name,NULL::text AS postal_code,NULL::text AS country_code",
        ).replace(
            " LEFT JOIN " + schema + ".provider_directory_location physical ON physical.source_id='cms-npd'\n"
            " AND physical.resource_id=location.resource_id AND physical.last_seen_run_id=selected.run_id",
            "",
        )
    return offices


def cms_typed_office_source(native, schema, available, *, source_ids=None, run_id=None, partial_refresh=False):
    """Use one shared source list for physical preparation and bounded observation."""
    if source_ids is not None and "cms-npd" not in source_ids:
        return None
    is_virtual = preparation.has_source_query()
    required_tables = CMS_OFFICE_READ_TABLES if is_virtual else (*CMS_OFFICE_READ_TABLES, "provider_directory_location")
    if not all(available.get(name, False) for name in required_tables):
        inputs = preparation.current() or preparation._SOURCE_QUERY.get()
        if inputs is not None and any(pin.source_id == "cms-npd" for pin in inputs.dataset_pins):
            raise RuntimeError("cms_typed_office_read_inputs_unavailable")
        return None
    prefix = native._provider_directory_current_overlay_ctes_sql(schema, source_ids=source_ids, run_id=run_id)
    marker = ", current_overlay AS MATERIALIZED ("
    if prefix.count(marker) != 1:
        raise RuntimeError("cms_typed_office_source_shape_changed")
    return (
        prefix.split(marker, 1)[0]
        + _typed_office_ctes(native, schema, partial_refresh=partial_refresh)
        + _ROWS.format(
            base_version=native._sql_literal(native.BASE_ADDRESS_VERSION),
            address_key="NULL::uuid" if is_virtual else "native_address_key::uuid",
            latitude="CASE WHEN pg_input_is_valid(office->>'latitude','numeric') THEN (office->>'latitude')::numeric END",
            longitude="CASE WHEN pg_input_is_valid(office->>'longitude','numeric') THEN (office->>'longitude')::numeric END",
            updated_at=preparation.semantic_now_sql()
            if preparation.current() or is_virtual
            else "published_at::timestamp",
        )
    )
