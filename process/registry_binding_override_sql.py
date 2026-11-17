# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One logical override identity shared by imported mapping and catalogue reads."""


def _native_aca_identity_sql(manual_json, imported_json):
    source_scope = "(" + imported_json + "->'source_scope')"
    manual_scope = "(" + manual_json + "->'source_scope_json')"
    checksum = source_scope + "->>'checksum_network'"
    source_key = imported_json + "->>'source_key'"
    return f"""jsonb_typeof({source_scope}->'issuer_id')='string'
      AND jsonb_typeof({source_scope}->'plan_year')='number' AND jsonb_typeof({source_scope}->'state')='string'
      AND {source_scope}->>'issuer_id' IS NOT NULL AND {source_scope}->>'plan_year' IS NOT NULL
      AND ARRAY[{manual_scope}->'issuer_id',{manual_scope}->'plan_year',{manual_scope}->'state']=
      ARRAY[{source_scope}->'issuer_id',{source_scope}->'plan_year',{source_scope}->'state'] AND (
      {source_key} ~ '^ACATier/[0-9a-f]{{64}}$' OR (
        jsonb_typeof({source_scope}->'plan_id')='string' AND length({source_scope}->>'plan_id') BETWEEN 1 AND 512
        AND {manual_scope}->'plan_id'={source_scope}->'plan_id'
        AND {manual_scope}->'checksum_network'={source_scope}->'checksum_network'
        AND {source_key}='ACAPlan/'||({source_scope}->>'plan_id')||'/'||({checksum})
        AND CASE WHEN jsonb_typeof({source_scope}->'checksum_network')='number'
          AND {checksum} ~ '^-?(0|[1-9][0-9]{{0,9}})$'
          THEN {checksum}<>'-0' AND ({checksum})::bigint BETWEEN -2147483648 AND 2147483647 ELSE false END))"""


def _legacy_aca_identity_sql(manual_json, imported_json):
    source_scope = "(" + imported_json + "->'source_scope')"
    plan_id = source_scope + "->>'plan_id'"
    return f"""{manual_json}->>'source_system'='aca'
      AND {imported_json}->>'source_system'='aca'
      AND {imported_json}->>'producer_id'='registry-native-network-source-v1'
      AND {manual_json}->>'producer_id'<>'registry-native-network-source-v1'
      AND {manual_json}->>'source_id'={imported_json}->>'source_id'
      AND (NOT {manual_json} ? 'source_key_kind' OR {manual_json}->>'source_key_kind'='hios_plan_id')
      AND {manual_json}->>'source_key'={plan_id}
      AND {plan_id} ~ '^[1-9][0-9]{{4}}[A-Z]{{2}}[0-9]{{7}}(-[0-9]{{2}})?$'
      AND left({plan_id},5)={source_scope}->>'issuer_id'
      AND substring({plan_id} FROM 6 FOR 2)={source_scope}->>'state'
      AND {imported_json}->>'source_key'='ACAPlan/'||({plan_id})||'/'||({source_scope}->>'checksum_network')
      AND coalesce(({_native_aca_identity_sql(manual_json, imported_json)}),false)"""


def binding_override_identity_sql(manual_json, imported_json, source_aliases):
    """Render only application-owned expressions; values remain native JSON data."""
    coordinates = ("source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
    manual_coordinates = ",".join(f"{manual_json}->>'{field}'" for field in coordinates)
    imported_coordinates = ",".join(f"{imported_json}->>'{field}'" for field in coordinates)
    source_key = imported_json + "->>'source_key'"
    native_key = f"""(({imported_json}->>'source_system'='ptg'
        AND {source_key}='PTGProviderSet/'||({imported_json}->>'source_id'))
      OR ({imported_json}->>'source_system'='aca' AND ({_native_aca_identity_sql(manual_json, imported_json)})))"""
    return f"""{manual_json}->>'source_system'={imported_json}->>'source_system'
      AND (({manual_json}->>'source_key'={source_key} AND (
        ({imported_json}->>'producer_id'<>'registry-native-network-source-v1' AND (
          ARRAY[{manual_coordinates}]=ARRAY[{imported_coordinates}]
          OR {imported_json}->>'source_system'='fhir' AND {source_aliases} ? ({manual_json}->>'source_id')))
        OR {manual_json}->>'producer_id'='registry-native-network-source-v1'
          AND {imported_json}->>'producer_id'='registry-native-network-source-v1'
          AND {manual_json}->>'source_id'={imported_json}->>'source_id' AND coalesce({native_key},false)))
        OR coalesce(({_legacy_aca_identity_sql(manual_json, imported_json)}),false))"""
