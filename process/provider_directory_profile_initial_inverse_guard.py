# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact fresh inverse branch of the existing deferred initial-serving guard."""

_RESTORED_UPDATE = """EXISTS (
 SELECT 1 FROM __SCHEMA__.provider_directory_serving_recovery published
 JOIN __SCHEMA__.provider_directory_serving_recovery recovered
   ON recovered.generation=published.generation AND recovered.source_id=published.source_id
 JOIN __SCHEMA__.provider_directory_profile_capacity_lease_consumption consumed
   ON consumed.run_id=recovered.document->'inverse'->>'run_id'
   AND consumed.lease_digest=recovered.document->'inverse'->>'lease_digest'
   AND consumed.admission_purpose='serving_inverse'
 CROSS JOIN LATERAL (SELECT consumed.canonical_lease_json::jsonb AS lease) signed
 CROSS JOIN LATERAL (SELECT signed.lease->'signing_preflight_guard' AS guard) signing
 CROSS JOIN LATERAL (SELECT signing.guard->'healthcare_receipt' AS receipt,
   signing.guard->'healthcare_request' AS request) proof
 CROSS JOIN LATERAL (SELECT proof.receipt->'capacity_geometry'->'scope' AS scope,
   proof.request->'recovery_execution' AS execution) authority
 JOIN __SCHEMA__.provider_directory_profile_capacity_preflight_receipt preflight
   ON preflight.receipt_sha256=signed.lease->>'nonce'
   AND preflight.consumed_run_id=consumed.run_id
   AND preflight.consumed_attestation_id=consumed.attestation_id
 JOIN __SCHEMA__.import_run worker ON worker.run_id=consumed.run_id
 JOIN __SCHEMA__.provider_directory_profile_initial_receipt initial
   ON initial.generation_id=NEW.generation_id
 JOIN __SCHEMA__.provider_directory_profile_serving_generation current_head
   ON current_head.singleton_key=NEW.singleton_key
 WHERE published.phase='published' AND recovered.phase='recovered'
   AND published.document->>'phase'='published' AND recovered.document->>'phase'='recovered'
   AND published.document->>'version'='1' AND recovered.document->>'version'='1'
   AND published.document->>'generation'=published.generation::text
   AND recovered.document->>'generation'=published.generation::text
   AND published.document->'preparation'->>'contract_id'='provider-directory-common-serving-recovery.v1'
   AND recovered.document->'preparation'=published.document->'preparation'
   AND recovered.document->'before'=published.document->'after'
   AND recovered.document->>'predecessor'=published.document_sha256
   AND recovered.document->'source'=published.document->'source'
   AND published.document->'source'->>'source_id'=published.source_id
   AND recovered.document->'after'->'roots'=published.document->'before'->'roots'
   AND recovered.document->'after'->'initial_targets'=published.document->'before'->'initial_targets'
   AND published.document->'before'->'initial_targets'='null'::jsonb
   AND to_jsonb(OLD)=to_jsonb(jsonb_populate_record(
       NULL::__SCHEMA__.provider_directory_profile_serving_generation,
       published.document->'after'->'metadata'->'profile'))
   AND to_jsonb(NEW)=to_jsonb(jsonb_populate_record(
       NULL::__SCHEMA__.provider_directory_profile_serving_generation,
       published.document->'before'->'metadata'->'profile'))
   AND to_jsonb(NEW)=to_jsonb(jsonb_populate_record(
       NULL::__SCHEMA__.provider_directory_profile_serving_generation,
       recovered.document->'after'->'metadata'->'profile'))
   AND ARRAY(SELECT jsonb_object_keys(published.document->'after'->'metadata'->'profile') ORDER BY 1)
       =ARRAY(SELECT jsonb_object_keys(to_jsonb(OLD)) ORDER BY 1)
   AND ARRAY(SELECT jsonb_object_keys(published.document->'before'->'metadata'->'profile') ORDER BY 1)
       =ARRAY(SELECT jsonb_object_keys(to_jsonb(NEW)) ORDER BY 1)
   AND ARRAY(SELECT jsonb_object_keys(recovered.document->'after'->'metadata'->'profile') ORDER BY 1)
       =ARRAY(SELECT jsonb_object_keys(to_jsonb(NEW)) ORDER BY 1)
   AND to_jsonb(current_head)=to_jsonb(NEW)
   AND NEW.singleton_key='global' AND NEW.status='published' AND NEW.operation='publish'
   AND NEW.capacity_geometry_status='verified'
   AND initial.payload->'serving'=__SERVING_SNAPSHOT__
   AND initial.payload->'capacity_geometry'=NEW.capacity_geometry_json::jsonb
   AND initial.payload->>'capacity_geometry_hash'=NEW.capacity_geometry_hash
   AND initial.payload->'source_vector'=NEW.source_vector_json::jsonb
   AND initial.payload->'source_context_vector'=NEW.source_context_vector_json::jsonb
   AND initial.payload->>'executable_plan_hash'=NEW.executable_plan_hash
   AND NEW.evidence_target_oid=to_regclass(__EVIDENCE_REF__)::oid::bigint
   AND NEW.profile_target_oid=to_regclass(__PROFILE_REF__)::oid::bigint
   AND recovered.document->'writer'=jsonb_build_object('session',session_user,'current',current_user,
       'owner',current_user::regrole::oid::bigint,'backend',pg_backend_pid(),'xid',pg_current_xact_id()::text,
       'database_oid',(SELECT oid::bigint FROM pg_database WHERE datname=current_database()))
   AND session_user=current_user
   AND authority.scope->'owner_oid'=to_jsonb(current_user::regrole::oid::bigint)
   AND authority.scope->'schema_oid'=to_jsonb(to_regnamespace(__SCHEMA_REF__)::oid::bigint)
   AND authority.scope->'database_oid'=recovered.document->'writer'->'database_oid'
   AND authority.scope->>'database_name'=current_database()
   AND consumed.contract_id='provider-directory-database-capacity-lease-v3'
   AND signed.lease->>'contract_id'=consumed.contract_id
   AND signed.lease->>'attestation_id'=consumed.attestation_id
   AND signed.lease->>'reservation_id'=consumed.reservation_id
   AND signed.lease->>'capacity_geometry_hash'=consumed.capacity_geometry_hash
   AND signed.lease->>'database_system_identifier'=consumed.database_system_identifier
   AND signed.lease->'database_oid'=to_jsonb(consumed.database_oid)
   AND signed.lease->>'database_name'=consumed.database_name
   AND authority.scope->'database_oid'=signed.lease->'database_oid'
   AND authority.scope->>'database_system_identifier'=signed.lease->>'database_system_identifier'
   AND authority.scope->>'database_name'=signed.lease->>'database_name'
   AND (signed.lease->>'observed_at')::timestamptz=consumed.observed_at
   AND (signed.lease->>'issued_at')::timestamptz=consumed.issued_at
   AND (signed.lease->>'expires_at')::timestamptz=consumed.expires_at
   AND (signed.lease->>'max_build_deadline')::timestamptz=consumed.max_build_deadline
   AND signing.guard->>'contract_id'='healthporta.provider-directory-profile-capacity-signing-preflight-guard.v2'
   AND proof.request->>'contract_id'='healthporta.provider-directory-serving-inverse-capacity-preflight-request.v1'
   AND preflight.contract_id='healthporta.provider-directory-serving-inverse-capacity-preflight.v1'
   AND preflight.request_contract_id=proof.request->>'contract_id'
   AND preflight.receipt_json::jsonb=proof.receipt
   AND proof.receipt->>'contract_id'=preflight.contract_id
   AND proof.receipt->>'receipt_sha256'=preflight.receipt_sha256
   AND proof.receipt->>'capacity_geometry_hash'=consumed.capacity_geometry_hash
   AND preflight.capacity_geometry_hash=consumed.capacity_geometry_hash
   AND proof.receipt->'capacity_geometry'->>'admission_purpose'='serving_inverse'
   AND proof.receipt->'capacity_geometry'->>'contract_id'='provider-directory-serving-inverse-capacity.v1'
   AND authority.scope->>'contract_id'='provider-directory-serving-inverse-scope.v1'
   AND authority.scope->>'input_sha256'=preflight.serving_preflight_sha256
   AND proof.receipt->'recovery_execution_identity'=authority.execution
   AND authority.execution->>'contract_id'='provider-directory-serving-inverse-execution.v1'
   AND authority.execution->>'operation'='recover'
   AND authority.execution->>'importer'='provider-directory-fhir'
   AND authority.execution->>'generation'=published.generation::text
   AND authority.execution->>'published_event_sha256'=published.document_sha256
   AND authority.execution->'source_ids'=jsonb_build_array(published.source_id)
   AND authority.scope->>'generation'=published.generation::text
   AND authority.scope->>'published_event_sha256'=published.document_sha256
   AND authority.scope->>'operation_sha256'=signing.guard->>'operation_preimage_sha256'
   AND authority.scope->>'operation_sha256'=recovered.document->'inverse'->>'operation_sha256'
   AND authority.scope->'qualified_source_ids'=published.document->'preparation'->'qualified_source_ids'
   AND worker.engine='healthcare-mrf-api' AND worker.importer='provider-directory-fhir'
   AND worker.node_id=authority.execution->>'node_id' AND worker.node_id=authority.scope->>'node_id'
   AND worker.status IN ('running','finalizing')
   AND worker.params::jsonb->>'provider_directory_serving_operation'='recover'
   AND worker.params::jsonb->>'provider_directory_serving_generation'=published.generation::text
   AND worker.params::jsonb->>'provider_directory_serving_published_event_sha256'=published.document_sha256
   AND worker.params::jsonb->'source_ids'=authority.execution->'source_ids'
   AND worker.params::jsonb->'provider_directory_serving_inverse_capacity_attestation'
       =jsonb_build_object('lease',signed.lease,'signature',consumed.signature)
   AND worker.params::jsonb-ARRAY['provider_directory_serving_operation','provider_directory_serving_generation',
       'provider_directory_serving_published_event_sha256','source_ids',
       'provider_directory_serving_inverse_capacity_attestation','run_id']='{}'::jsonb
   AND (NOT worker.params::jsonb ? 'run_id' OR worker.params::jsonb->>'run_id'=worker.run_id)
   AND consumed.observed_at<=consumed.issued_at
   AND consumed.accepted_at+interval '5 seconds'>=consumed.issued_at
   AND preflight.consumed_at<=consumed.accepted_at AND consumed.accepted_at<=clock_timestamp()
   AND clock_timestamp()<consumed.max_build_deadline AND consumed.max_build_deadline<=consumed.expires_at
   AND clock_timestamp()<preflight.expires_at
)"""


def body(schema):
    """Keep fresh initial publication unchanged; permit only an exact fresh inverse UPDATE."""
    from process import provider_directory_profile_initial_guards as initial

    quoted = initial._q(schema)
    predicate = _RESTORED_UPDATE.replace("__SCHEMA__", quoted).replace(
        "__SERVING_SNAPSHOT__", initial._serving_snapshot("NEW")
    )
    for marker, value in (
        ("__EVIDENCE_REF__", initial._qt(schema, "provider_directory_profile_evidence")),
        ("__PROFILE_REF__", initial._qt(schema, "provider_directory_profile")),
        ("__SCHEMA_REF__", quoted),
    ):
        predicate = predicate.replace(marker, initial._literal(value))
    old = initial._serving_insert_body(schema)
    refusal = "THEN RAISE EXCEPTION 'provider_directory_profile_initial_receipt_required'; END IF;"
    if old.count(refusal) != 1:
        raise RuntimeError("provider_directory_profile_initial_receipt_guard_shape_changed")
    branch = f"THEN\n            IF TG_OP='UPDATE' AND {predicate} THEN RETURN NULL; END IF;\n            "
    return old.replace(refusal, branch + refusal.removeprefix("THEN "))
