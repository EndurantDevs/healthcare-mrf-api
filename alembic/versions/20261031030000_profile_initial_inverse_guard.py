# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate fresh inverse restoration in the existing initial-serving guard."""

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261031030000_profile_initial_inverse_guard"
down_revision = "20261031020000_serving_inverse_capacity"
branch_labels = None
depends_on = None
_FUNCTION = "pd_profile_initial_serving_insert"
_SERVING = "provider_directory_profile_serving_generation"

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


def _body(initial, schema):
    """Keep fresh initial publication unchanged; permit only an exact fresh inverse UPDATE."""
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
        raise RuntimeError("profile_initial_inverse_guard_predecessor_changed")
    branch = f"THEN\n            IF TG_OP='UPDATE' AND {predicate} THEN RETURN NULL; END IF;\n            "
    return old.replace(refusal, branch + refusal.removeprefix("THEN "))


def _initial():
    path = Path(__file__).with_name("20261001110000_profile_initial_publication.py")
    spec = importlib.util.spec_from_file_location("_initial_inverse_predecessor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("profile_initial_inverse_guard_predecessor_unavailable")
    initial = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(initial)
    return initial


def _locked(initial):
    schema = initial._ORIGINAL._schema()
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SELECT pg_advisory_xact_lock(hashtextextended('provider-directory-profile-capacity-preflight:v3',0))")
    op.execute(f"LOCK TABLE {initial._qt(schema, _SERVING)} IN ACCESS EXCLUSIVE MODE")
    names = (
        "provider_directory_profile_initial_receipt",
        "provider_directory_profile_capacity_lease_consumption",
        "provider_directory_profile_capacity_preflight_receipt",
        "provider_directory_serving_recovery",
    )
    op.execute("LOCK TABLE " + ",".join(initial._qt(schema, name) for name in sorted(names)) + " IN SHARE MODE")
    return schema


def _function_identity(schema, expected):
    function_rows = (
        op.get_bind()
        .execute(
            text("""SELECT to_jsonb(p)-'prosrc' AS identity,
          (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t WHERE t.tgfoid=p.oid) AS triggers
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
        WHERE n.nspname=:schema AND p.proname=:name
          AND p.pronargs=0 AND p.prorettype='trigger'::regtype AND p.prokind='f' AND l.lanname='plpgsql'
          AND p.provolatile='v' AND NOT p.prosecdef AND NOT p.proretset AND NOT p.proisstrict
          AND NOT p.proleakproof AND p.proparallel='u' AND p.procost=100 AND p.prorows=0
          AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND p.prosrc=:body
          AND (SELECT count(*) FROM pg_proc other WHERE other.pronamespace=n.oid AND other.proname=p.proname)=1
          AND EXISTS(SELECT FROM pg_trigger t WHERE t.tgfoid=p.oid AND NOT t.tgisinternal
            AND t.tgname=p.proname AND t.tgtype=21 AND t.tgdeferrable AND t.tginitdeferred
            AND t.tgenabled='A' AND t.tgnargs=0 AND t.tgqual IS NULL AND t.tgattr::text=''
            AND t.tgrelid=to_regclass(:serving))"""),
            {
                "schema": schema,
                "name": _FUNCTION,
                "body": " " + expected + " ",
                "serving": '"' + schema.replace('"', '""') + '".' + _SERVING,
            },
        )
        .mappings()
        .all()
    )
    if len(function_rows) != 1:
        raise RuntimeError("profile_initial_inverse_guard_changed")
    return dict(function_rows[0])


def _replace(initial, schema, *, inverse):
    old, new = initial._serving_insert_body(schema), _body(initial, schema)
    before, after = (old, new) if inverse else (new, old)
    identity = _function_identity(schema, before)
    function = initial._qt(schema, _FUNCTION)
    op.execute(
        f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql "
        f"VOLATILE SET search_path=pg_catalog AS $$ {after} $$"
    )
    if _function_identity(schema, after) != identity:
        raise RuntimeError("profile_initial_inverse_guard_identity_changed")


def upgrade():
    """Keep the original function/trigger identities and every fresh-publication predicate."""
    initial = _initial()
    _replace(initial, _locked(initial), inverse=True)


def downgrade():
    """Do not invalidate any issued inverse authority or recovered publication history."""
    initial = _initial()
    schema = _locked(initial)
    consumed = initial._qt(schema, "provider_directory_profile_capacity_lease_consumption")
    preflight = initial._qt(schema, "provider_directory_profile_capacity_preflight_receipt")
    journal = initial._qt(schema, "provider_directory_serving_recovery")
    receipt = initial._qt(schema, "provider_directory_profile_initial_receipt")
    serving = initial._qt(schema, _SERVING)
    used = (
        op.get_bind()
        .execute(
            text(f"""SELECT
        EXISTS(SELECT FROM {receipt}) OR
        EXISTS(SELECT FROM {serving} WHERE status='published') OR
        EXISTS(SELECT FROM {consumed} WHERE admission_purpose='serving_inverse') OR
        EXISTS(SELECT FROM {preflight} WHERE materialization_mode='serving_inverse') OR
        EXISTS(SELECT FROM {journal} WHERE phase='recovered' AND
          document->'preparation'->>'contract_id'='provider-directory-common-serving-recovery.v1')""")
        )
        .scalar_one()
    )
    if used is not False:
        raise RuntimeError("profile_initial_inverse_history_requires_retention")
    _replace(initial, schema, inverse=False)
