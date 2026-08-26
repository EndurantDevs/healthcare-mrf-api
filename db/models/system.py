# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import (
    DATE,
    JSON,
    TEXT,
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Computed,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = (
    "ImportHistory",
    "ImportLog",
    "ImportRun",
    "PTGImportWave",
    "PTGImportWaveAdmissionRollback",
    "PTGImportWaveClaim",
    "PTGImportWaveIntent",
    "PTGImportWaveOrdinaryTerminalReceipt",
    "PTGImportWaveOutcome",
    "PTGImportWaveQuarantine",
    "PTGImportWaveSupersession",
    "MRFCrawlRun",
    "MRFDiscoveryBatch",
    "MRFDiscoverySourceCheckpoint",
    "MRFFile",
    "MRFPayer",
    "MRFPayerScorecard",
    "MRFPlan",
    "MRFSource",
    "MRFUrlObservation",
    "PartDImportRun",
    "PartDFormularySnapshot",
    "ProviderDirectoryAPIEndpoint",
    "ProviderDirectoryBulkAcquisitionCheckpoint",
    "ProviderDirectoryBulkOutputCheckpoint",
    "ProviderDirectoryCapability",
    "ProviderDirectoryCanonicalResource",
    "ProviderDirectoryDatasetAffiliationOrganization",
    "ProviderDirectoryDatasetInsurancePlan",
    "ProviderDirectoryDatasetNetworkPlan",
    "ProviderDirectoryDatasetRehydrationCheckpoint",
    "ProviderDirectoryDatasetResource",
    "ProviderDirectoryEndpoint",
    "ProviderDirectoryEndpointDataset",
    "ProviderDirectoryHealthcareService",
    "ProviderDirectoryInsurancePlan",
    "ProviderDirectoryLocation",
    "ProviderDirectoryOrganization",
    "ProviderDirectoryOrganizationAffiliation",
    "ProviderDirectoryPaginationCheckpoint",
    "ProviderDirectoryProfileBuildCheckpoint",
    "ProviderDirectoryProfileCapacityLeaseConsumption",
    "ProviderDirectoryProfileCapacityPreflightReceipt",
    "ProviderDirectoryProfileDeltaReceipt",
    "ProviderDirectoryProfileServingGeneration",
    "ProviderDirectoryProfileSelectionAuthority",
    "ProviderDirectoryProfileSelectionObservation",
    "ProviderDirectoryProfileSelectionProof",
    "ProviderDirectoryPractitioner",
    "ProviderDirectoryPractitionerRole",
    "ProviderDirectoryReverseLookupCheckpoint",
    "ProviderDirectorySource",
    "ProviderDirectorySourceResource",
)


class ImportHistory(Base, JSONOutputMixin):
    __tablename__ = 'history'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('import_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['import_id']
    import_id = Column(String)
    json_status = Column(JSON)
    when = Column(DateTime)


class ImportLog(Base, JSONOutputMixin):
    __tablename__ = 'log'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('issuer_id', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['issuer_id', 'checksum']
    issuer_id = Column(Integer)
    checksum = Column(Integer)
    type = Column(String(4))
    text = Column(String)
    url = Column(String)
    source = Column(String)  # plans, index, providers, etc.
    level = Column(String)  # network, json, etc.


class ImportRun(Base, JSONOutputMixin):
    __tablename__ = "import_run"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("status", "heartbeat_at"), "name": "import_run_status_heartbeat_idx"},
        {"index_elements": ("importer", "created_at"), "name": "import_run_importer_created_idx"},
        {
            "index_elements": ("idempotency_key",),
            "name": "import_run_active_idempotency_idx",
            "unique": True,
            "where": "status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')",
        },
        {
            "index_elements": ("importer", "idempotency_key"),
            "name": "import_run_plan_pricing_idempotency_idx",
            "unique": True,
            "where": (
                "importer IN ('plan-pricing-projection', "
                "'plan-pricing-prewarm') AND idempotency_key IS NOT NULL"
            ),
        },
        {"index_elements": ("schedule_id",), "name": "import_run_schedule_idx"},
        {"index_elements": ("subscription_id",), "name": "import_run_subscription_idx"},
        {"index_elements": ("source_file_import_id",), "name": "import_run_source_file_import_idx"},
        {
            "index_elements": ("retry_of_run_id",),
            "name": "import_run_provider_directory_retry_child_idx",
            "unique": True,
            "where": "importer = 'provider-directory-fhir' AND retry_of_run_id IS NOT NULL",
        },
        {
            "index_elements": ("retry_of_run_id",),
            "name": "import_run_mrf_discovery_retry_child_idx",
            "unique": True,
            "where": "importer = 'mrf-source-discovery' AND retry_of_run_id IS NOT NULL",
        },
    ]

    run_id = Column(String(64), nullable=False)
    engine = Column(String(64), nullable=False, default="healthcare-mrf-api")
    node_id = Column(String(64))
    importer = Column(String(64), nullable=False)
    family = Column(String(64))
    status = Column(String(32), nullable=False)
    phase_detail = Column(String(128))
    params = Column(JSON)
    idempotency_key = Column(String(160))
    triggered_by = Column(String(32))
    schedule_id = Column(String(64))
    subscription_id = Column(String(64))
    source_file_import_id = Column(String(64))
    created_at = Column(TIMESTAMP)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    heartbeat_at = Column(TIMESTAMP)
    progress = Column(JSON)
    metrics = Column(JSON)
    error = Column(JSON)
    snapshot_id = Column(String(96))
    import_id = Column(String(64))
    retry_of_run_id = Column(String(64))


class PTGImportWave(Base, JSONOutputMixin):
    """Durable, controller-owned admission record for one exact PTG wave.

    This row is intentionally not a publication API.  A later controller owns
    every transition after ``admitted`` and must persist terminal evidence
    before capacity can be released.
    """

    __tablename__ = "ptg_import_wave"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("wave_id"),
        UniqueConstraint("idempotency_key", name="ptg_import_wave_idempotency_key"),
        UniqueConstraint("wave_digest", name="ptg_import_wave_digest_key"),
        CheckConstraint(
            "request_digest ~ '^[0-9a-f]{64}$' "
            "AND cohort_attestation_digest ~ '^[0-9a-f]{64}$' "
            "AND cohort_signature_digest ~ '^[0-9a-f]{64}$' "
            "AND physical_coordinate_digest ~ '^[0-9a-f]{64}$' "
            "AND imported_coordinate_digest ~ '^[0-9a-f]{64}$' "
            "AND reused_coordinate_digest ~ '^[0-9a-f]{64}$' "
            "AND partition_digest ~ '^[0-9a-f]{64}$' "
            "AND jobs_digest ~ '^[0-9a-f]{64}$' "
            "AND manifest_digest ~ '^[0-9a-f]{64}$' "
            "AND wave_digest ~ '^[0-9a-f]{64}$' "
            "AND physical_coordinate_count > 0 AND intent_count > 0 "
            "AND imported_coordinate_count = intent_count "
            "AND physical_coordinate_count = imported_coordinate_count + reused_coordinate_count "
            "AND worker_limit = 12 "
            "AND queue = 'arq:PTGSmall' "
            "AND release_queue = 'arq:PTGSmall:wave:' || wave_digest "
            "AND worker_class = 'process.PTGSmall' "
            "AND resource_class = 'small' "
            "AND protocol_identity = 'healthporta.ptg-small.exact-wave.v1' "
            "AND serializer_identity = 'arq-0.28.process-msgpack.v1' "
            "AND state IN ('admitted', 'materializing', 'slots_waiting', "
            "'redis_releasing', 'released', 'executing', 'awaiting_linkage', 'terminalizing', 'cleaning', "
            "'uncertain', 'succeeded', 'failed', 'canceled', 'dead_letter')",
            name="ptg_import_wave_contract_check",
        ),
        CheckConstraint(
            "(state IN ('succeeded', 'failed', 'canceled', 'dead_letter') "
            "AND resolved_at IS NOT NULL "
            "AND terminal_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND cleanup_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND redis_cleanup_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND kubernetes_delete_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND linkage_ack_digest ~ '^[0-9a-f]{64}$' "
            "AND outcomes_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(terminal_summary) = 'object' "
            "AND json_typeof(cleanup_summary) = 'object' "
            "AND json_typeof(linkage_ack) = 'object') "
            "OR (state = 'cleaning' AND resolved_at IS NULL "
            "AND terminal_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND cleanup_evidence_digest IS NULL AND cleanup_summary IS NULL "
            "AND linkage_ack_digest ~ '^[0-9a-f]{64}$' "
            "AND outcomes_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(terminal_summary) = 'object' "
            "AND json_typeof(linkage_ack) = 'object') "
            "OR (state NOT IN ('succeeded', 'failed', 'canceled', 'dead_letter', 'cleaning') "
            "AND resolved_at IS NULL AND terminal_evidence_digest IS NULL "
            "AND terminal_summary IS NULL AND cleanup_evidence_digest IS NULL "
            "AND cleanup_summary IS NULL)",
            name="ptg_import_wave_terminal_evidence_check",
        ),
        CheckConstraint(
            "(state = 'uncertain' AND uncertainty_resume_state IN "
            "('materializing', 'slots_waiting', 'redis_releasing', 'released', "
            "'executing', 'awaiting_linkage', 'terminalizing', 'cleaning')) "
            "OR (state <> 'uncertain' AND uncertainty_resume_state IS NULL)",
            name="ptg_import_wave_uncertainty_resume_check",
        ),
        CheckConstraint(
            "(k8s_post_ticket IS NULL OR k8s_post_ticket ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND (redis_release_ticket IS NULL OR redis_release_ticket ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND (redis_cleanup_ticket IS NULL OR redis_cleanup_ticket ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND (kubernetes_delete_ticket IS NULL OR kubernetes_delete_ticket ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND ((k8s_post_ticket IS NULL) = (k8s_post_started_at IS NULL)) "
            "AND ((redis_release_ticket IS NULL) = (redis_release_started_at IS NULL)) "
            "AND ((redis_cleanup_ticket IS NULL) = (redis_cleanup_started_at IS NULL)) "
            "AND ((kubernetes_delete_ticket IS NULL) = (kubernetes_delete_started_at IS NULL))",
            name="ptg_import_wave_operation_ticket_check",
        ),
        CheckConstraint(
            "(failure_receipt IS NULL AND failure_receipt_digest IS NULL) "
            "OR (failure_receipt_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(failure_receipt) = 'object' "
            "AND state IN ('awaiting_linkage', 'terminalizing', 'cleaning', "
            "'succeeded', 'failed', 'canceled', 'dead_letter'))",
            name="ptg_import_wave_failure_receipt_check",
        ),
        CheckConstraint(
            "((kubernetes_job_receipt IS NULL) = (kubernetes_job_receipt_digest IS NULL)) "
            "AND ((kubernetes_ready_attestation IS NULL) = (kubernetes_ready_attestation_digest IS NULL)) "
            "AND ((redis_release_attestation IS NULL) = (redis_release_attestation_digest IS NULL)) "
            "AND ((linkage_ack IS NULL) = (linkage_ack_digest IS NULL)) "
            "AND ((terminal_summary IS NULL) = (terminal_evidence_digest IS NULL)) "
            "AND ((redis_cleanup_evidence IS NULL) = (redis_cleanup_evidence_digest IS NULL)) "
            "AND ((kubernetes_delete_evidence IS NULL) = (kubernetes_delete_evidence_digest IS NULL)) "
            "AND ((cleanup_summary IS NULL) = (cleanup_evidence_digest IS NULL))",
            name="ptg_import_wave_receipt_pairs_check",
        ),
        CheckConstraint(
            "((cohort_attestation ->> 'schema_version' = "
            "'healthporta.ptg-import-wave-attestation.v6' "
                "AND receipt_key_id IS NOT NULL "
                "AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
                "AND cohort_attestation ->> 'receipt_key_id' = receipt_key_id "
                "AND receipt_public_modulus_hex IS NOT NULL "
                "AND length(receipt_public_modulus_hex) = 512 "
                "AND receipt_public_modulus_hex ~ '^[0-9a-f]+$' "
                "AND left(receipt_public_modulus_hex, 1) ~ '^[89a-f]$' "
                "AND right(receipt_public_modulus_hex, 1) ~ '^[13579bdf]$' "
                "AND receipt_public_exponent IS NOT NULL "
                "AND receipt_public_exponent = 65537 "
                "AND cohort_attestation ->> 'receipt_public_modulus_hex' "
                "= receipt_public_modulus_hex "
                "AND (cohort_attestation ->> 'receipt_public_exponent')::integer "
                "= receipt_public_exponent) "
                "OR (cohort_attestation ->> 'schema_version' <> "
                "'healthporta.ptg-import-wave-attestation.v6' "
                "AND receipt_key_id IS NULL "
                "AND receipt_public_modulus_hex IS NULL "
                "AND receipt_public_exponent IS NULL))",
            name="ptg_import_wave_receipt_key_epoch_check",
        ),
        CheckConstraint(
            "(linkage_receipt IS NULL "
            "AND linkage_receipt_payload_digest IS NULL "
            "AND linkage_receipt_issued_at IS NULL) OR ("
            "cohort_attestation ->> 'schema_version' = "
            "'healthporta.ptg-import-wave-attestation.v6' "
            "AND linkage_ack IS NOT NULL "
            "AND linkage_ack_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(linkage_receipt) = 'object' "
            "AND linkage_receipt ->> 'schema' = "
            "'healthporta.ptg-wave-linkage-receipt.v2' "
            "AND linkage_receipt ->> 'key_id' = receipt_key_id "
            "AND linkage_receipt ->> 'payload_digest' = "
            "linkage_receipt_payload_digest "
            "AND length(linkage_receipt ->> 'signature') = 512 "
            "AND linkage_receipt ->> 'signature' ~ '^[0-9a-f]+$' "
            "AND linkage_receipt #>> '{payload,wave_id}' = wave_id "
            "AND linkage_receipt #>> '{payload,wave_digest}' = wave_digest "
            "AND linkage_receipt #>> '{payload,linkage_ack_digest}' = "
            "linkage_ack_digest "
            "AND linkage_receipt_payload_digest ~ '^[0-9a-f]{64}$' "
            "AND linkage_receipt_issued_at IS NOT NULL)",
            name="ptg_import_wave_linkage_receipt_check",
        ),
        CheckConstraint(
            "cohort_attestation ->> 'schema_version' <> "
            "'healthporta.ptg-import-wave-attestation.v6' "
            "OR linkage_ack IS NULL OR linkage_receipt IS NOT NULL",
            name="ptg_import_wave_v6_linkage_receipt_required_check",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    wave_id = Column(String(64), nullable=False)
    idempotency_key = Column(String(160), nullable=False)
    request_digest = Column(String(64), nullable=False)
    cohort_attestation = Column(JSON, nullable=False)
    cohort_attestation_digest = Column(String(64), nullable=False)
    cohort_signature_digest = Column(String(64), nullable=False)
    receipt_key_id = Column(String(64))
    receipt_public_modulus_hex = Column(String(512))
    receipt_public_exponent = Column(Integer)
    physical_coordinate_count = Column(Integer, nullable=False)
    physical_coordinate_digest = Column(String(64), nullable=False)
    imported_coordinate_count = Column(Integer, nullable=False)
    imported_coordinate_digest = Column(String(64), nullable=False)
    reused_coordinate_count = Column(Integer, nullable=False)
    reused_coordinate_digest = Column(String(64), nullable=False)
    partition_digest = Column(String(64), nullable=False)
    intent_count = Column(Integer, nullable=False)
    jobs_digest = Column(String(64), nullable=False)
    manifest_digest = Column(String(64), nullable=False)
    wave_digest = Column(String(64), nullable=False)
    queue = Column(String(64), nullable=False)
    release_queue = Column(String(160), nullable=False)
    worker_class = Column(String(64), nullable=False)
    resource_class = Column(String(32), nullable=False)
    worker_limit = Column(Integer, nullable=False)
    protocol_identity = Column(String(96), nullable=False)
    serializer_identity = Column(String(96), nullable=False)
    enqueue_time_ms = Column(BigInteger, nullable=False)
    state_version = Column(Integer, nullable=False, default=0)
    state = Column(String(32), nullable=False)
    uncertainty_resume_state = Column(String(32))
    created_at = Column(TIMESTAMP, nullable=False)
    kubernetes_manifest = Column(JSON)
    kubernetes_manifest_bytes = Column(LargeBinary)
    kubernetes_manifest_sha256 = Column(String(64))
    kubernetes_manifest_identity = Column(String(64))
    pinned_image_reference = Column(String(512))
    pinned_image_digest = Column(String(64))
    runtime_image_identity = Column(String(72))
    kubernetes_config_identity = Column(String(64))
    k8s_post_ticket = Column(String(128))
    k8s_post_started_at = Column(TIMESTAMP)
    kubernetes_job_uid = Column(String(128))
    kubernetes_job_receipt = Column(JSON)
    kubernetes_job_receipt_digest = Column(String(64))
    kubernetes_ready_attestation = Column(JSON)
    kubernetes_ready_attestation_digest = Column(String(64))
    redis_release_ticket = Column(String(128))
    redis_release_started_at = Column(TIMESTAMP)
    redis_release_attestation = Column(JSON)
    redis_release_attestation_digest = Column(String(64))
    outcomes_digest = Column(String(64))
    failure_receipt = Column(JSON)
    failure_receipt_digest = Column(String(64))
    linkage_ack = Column(JSON)
    linkage_ack_digest = Column(String(64))
    linkage_receipt = Column(JSON)
    linkage_receipt_payload_digest = Column(String(64))
    linkage_receipt_issued_at = Column(TIMESTAMP(timezone=True))
    redis_cleanup_ticket = Column(String(128))
    redis_cleanup_started_at = Column(TIMESTAMP)
    redis_cleanup_evidence = Column(JSON)
    redis_cleanup_evidence_digest = Column(String(64))
    kubernetes_delete_ticket = Column(String(128))
    kubernetes_delete_started_at = Column(TIMESTAMP)
    kubernetes_delete_evidence = Column(JSON)
    kubernetes_delete_evidence_digest = Column(String(64))
    resolved_at = Column(TIMESTAMP)
    terminal_evidence_digest = Column(String(64))
    terminal_summary = Column(JSON)
    cleanup_evidence_digest = Column(String(64))
    cleanup_summary = Column(JSON)


class PTGImportWaveQuarantine(Base, JSONOutputMixin):
    """Append-only controller exclusion and audited retirement for one wave."""

    __tablename__ = "ptg_import_wave_quarantine"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("predecessor_wave_id"),
        ForeignKeyConstraint(
            ("predecessor_wave_id",), (PTGImportWave.wave_id,),
            name="ptg_import_wave_quarantine_predecessor_wave_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "reason IN ('legacy_uncertain_slots_waiting_pre_receipt', "
            "'materialized_preclaim_failure', "
            "'v12_pristine_materialized_cutover', "
            "'v13_post_ready_unreleased_failure_cutover')",
            name="ptg_import_wave_quarantine_reason_check",
        ),
        CheckConstraint(
            "(recovery_basis IS NULL AND reason IN ("
            "'legacy_uncertain_slots_waiting_pre_receipt', "
            "'materialized_preclaim_failure') AND successor_wave_id IS NULL "
            "AND recovery_evidence IS NULL "
            "AND recovery_evidence_canonical IS NULL "
            "AND recovery_evidence_sha256 IS NULL) OR "
            "(reason = 'materialized_preclaim_failure' "
            "AND recovery_basis = 'materialized_preclaim_failure' "
            "AND successor_wave_id IS NOT NULL "
            "AND successor_wave_id <> predecessor_wave_id "
            "AND jsonb_typeof(recovery_evidence) = 'object' "
            "AND recovery_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND octet_length(recovery_evidence_canonical) > 0 "
            "AND encode(sha256(recovery_evidence_canonical), 'hex') "
            "= recovery_evidence_sha256 "
            "AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb "
            "= recovery_evidence - 'proof_digest') OR ("
            "reason = 'v12_pristine_materialized_cutover' "
            "AND recovery_basis = 'v12_pristine_materialized_cutover' "
            "AND successor_wave_id IS NOT NULL "
            "AND successor_wave_id <> predecessor_wave_id "
            "AND jsonb_typeof(recovery_evidence) = 'object' "
            "AND recovery_evidence ->> 'schema_version' = "
            "'healthporta.ptg-wave.v12-pristine-materialized-abandonment-proof.v1' "
            "AND recovery_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND octet_length(recovery_evidence_canonical) > 0 "
            "AND encode(sha256(convert_to("
            "'healthporta.ptg-wave.v12-pristine-materialized-abandonment-proof.v1', "
            "'UTF8') || decode('00', 'hex') || "
            "recovery_evidence_canonical), 'hex') = recovery_evidence_sha256 "
            "AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb "
            "= recovery_evidence - 'proof_digest') OR ("
            "reason = 'v13_post_ready_unreleased_failure_cutover' "
            "AND recovery_basis = 'v13_post_ready_unreleased_failure_cutover' "
            "AND successor_wave_id IS NOT NULL "
            "AND successor_wave_id <> predecessor_wave_id "
            "AND jsonb_typeof(recovery_evidence) = 'object' "
            "AND recovery_evidence ->> 'schema_version' = "
            "'healthporta.ptg-wave.v13-post-ready-unreleased-failure-abandonment-proof.v1' "
            "AND recovery_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND octet_length(recovery_evidence_canonical) > 0 "
            "AND encode(sha256(convert_to("
            "'healthporta.ptg-wave.v13-post-ready-unreleased-failure-abandonment-proof.v1', "
            "'UTF8') || decode('00', 'hex') || "
            "recovery_evidence_canonical), 'hex') = recovery_evidence_sha256 "
            "AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb "
            "= recovery_evidence - 'proof_digest')",
            name="ptg_import_wave_quarantine_abandonment_evidence_check",
        ),
        CheckConstraint(
            "(recovery_basis IS DISTINCT FROM "
            "'v12_pristine_materialized_cutover' "
            "AND recovery_basis IS DISTINCT FROM "
            "'v13_post_ready_unreleased_failure_cutover' "
            "AND abandonment_receipt IS NULL "
            "AND abandonment_receipt_payload_digest IS NULL "
            "AND abandonment_receipt_issued_at IS NULL "
            "AND receipt_key_id IS NULL) OR ("
            "reason = 'v12_pristine_materialized_cutover' "
            "AND recovery_basis = 'v12_pristine_materialized_cutover' "
            "AND "
            "recovery_evidence ->> 'schema_version' = "
            "'healthporta.ptg-wave.v12-pristine-materialized-abandonment-proof.v1' "
            "AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND jsonb_typeof(abandonment_receipt) = 'object' "
            "AND abandonment_receipt ->> 'schema' = "
            "'healthporta.ptg-wave-abandonment-receipt.v2' "
            "AND abandonment_receipt ->> 'key_id' = receipt_key_id "
            "AND abandonment_receipt ->> 'payload_digest' = "
            "abandonment_receipt_payload_digest "
            "AND length(abandonment_receipt ->> 'signature') = 512 "
            "AND abandonment_receipt ->> 'signature' ~ '^[0-9a-f]+$' "
            "AND abandonment_receipt #>> '{payload,wave_id}' = "
            "predecessor_wave_id "
            "AND abandonment_receipt #>> '{payload,cutover_id}' = "
            "successor_wave_id "
            "AND abandonment_receipt #>> "
            "'{payload,recovery_evidence_sha256}' = recovery_evidence_sha256 "
            "AND abandonment_receipt_payload_digest ~ '^[0-9a-f]{64}$' "
            "AND abandonment_receipt_issued_at IS NOT NULL) OR ("
            "reason = 'v13_post_ready_unreleased_failure_cutover' "
            "AND recovery_basis = 'v13_post_ready_unreleased_failure_cutover' "
            "AND recovery_evidence ->> 'schema_version' = "
            "'healthporta.ptg-wave.v13-post-ready-unreleased-failure-abandonment-proof.v1' "
            "AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND jsonb_typeof(abandonment_receipt) = 'object' "
            "AND abandonment_receipt ->> 'schema' = "
            "'healthporta.ptg-wave-abandonment-receipt.v2' "
            "AND abandonment_receipt ->> 'key_id' = receipt_key_id "
            "AND abandonment_receipt ->> 'payload_digest' = "
            "abandonment_receipt_payload_digest "
            "AND length(abandonment_receipt ->> 'signature') = 512 "
            "AND abandonment_receipt ->> 'signature' ~ '^[0-9a-f]+$' "
            "AND abandonment_receipt #>> '{payload,wave_id}' = "
            "predecessor_wave_id "
            "AND abandonment_receipt #>> '{payload,cutover_id}' = "
            "successor_wave_id "
            "AND abandonment_receipt #>> "
            "'{payload,recovery_evidence_sha256}' = recovery_evidence_sha256 "
            "AND abandonment_receipt_payload_digest ~ '^[0-9a-f]{64}$' "
            "AND abandonment_receipt_issued_at IS NOT NULL)",
            name="ptg_import_wave_quarantine_receipt_check",
        ),
        UniqueConstraint(
            "successor_wave_id",
            name="ptg_import_wave_quarantine_cutover_id_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    predecessor_wave_id = Column(String(64), nullable=False)
    reason = Column(String(64), nullable=False)
    cutover_id = Column("successor_wave_id", String(64))
    recovery_basis = Column(String(64))
    recovery_evidence = Column(JSONB)
    recovery_evidence_canonical = Column(LargeBinary)
    recovery_evidence_sha256 = Column(String(64))
    receipt_key_id = Column(String(64))
    abandonment_receipt = Column(JSONB)
    abandonment_receipt_payload_digest = Column(String(64))
    abandonment_receipt_issued_at = Column(TIMESTAMP(timezone=True))
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class PTGImportWaveSupersession(Base, JSONOutputMixin):
    """Append-only recovery linkage that retires only its predecessor wave."""

    __tablename__ = "ptg_import_wave_supersession"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("predecessor_wave_id"),
        ForeignKeyConstraint(
            ("predecessor_wave_id",), (PTGImportWave.wave_id,),
            name="ptg_import_wave_supersession_predecessor_wave_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("successor_wave_id",), (PTGImportWave.wave_id,),
            name="ptg_import_wave_supersession_successor_wave_fkey",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        UniqueConstraint(
            "successor_wave_id",
            name="ptg_import_wave_supersession_successor_wave_id_key",
        ),
        CheckConstraint(
            "predecessor_wave_id <> successor_wave_id",
            name="ptg_import_wave_supersession_distinct_check",
        ),
        CheckConstraint(
            "recovery_basis IN ('logical_preclaim_failure', "
            "'materialized_preclaim_failure')",
            name="ptg_import_wave_supersession_basis_check",
        ),
        CheckConstraint(
            "jsonb_typeof(recovery_evidence) = 'object' "
            "AND recovery_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND octet_length(recovery_evidence_canonical) > 0 "
            "AND encode(sha256(recovery_evidence_canonical), 'hex') "
            "= recovery_evidence_sha256 "
            "AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb "
            "= recovery_evidence - 'proof_digest'",
            name="ptg_import_wave_supersession_evidence_check",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    predecessor_wave_id = Column(String(64), nullable=False)
    successor_wave_id = Column(String(64), nullable=False)
    recovery_basis = Column(String(64), nullable=False)
    recovery_evidence = Column(JSONB, nullable=False)
    recovery_evidence_canonical = Column(LargeBinary, nullable=False)
    recovery_evidence_sha256 = Column(String(64), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class PTGImportWaveAdmissionRollback(Base, JSONOutputMixin):
    """Append-only retirement of one proved-absent admission request."""

    __tablename__ = "ptg_import_wave_admission_rollback"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("predecessor_wave_id"),
        ForeignKeyConstraint(
            ("successor_wave_id",),
            (PTGImportWave.wave_id,),
            name="ptg_wave_rollback_successor_wave_fkey",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        UniqueConstraint(
            "predecessor_idempotency_key",
            name="ptg_wave_rollback_predecessor_idempotency_key",
        ),
        UniqueConstraint(
            "predecessor_request_digest",
            name="ptg_wave_rollback_predecessor_request_digest_key",
        ),
        UniqueConstraint(
            "predecessor_wave_digest",
            name="ptg_wave_rollback_predecessor_wave_digest_key",
        ),
        UniqueConstraint(
            "successor_wave_id",
            name="ptg_wave_rollback_successor_wave_id_key",
        ),
        CheckConstraint(
            "predecessor_wave_id <> successor_wave_id",
            name="ptg_wave_rollback_distinct_check",
        ),
        CheckConstraint(
            "recovery_basis = 'admission_rollback_absent'",
            name="ptg_wave_rollback_basis_check",
        ),
        CheckConstraint(
            "predecessor_request_digest ~ '^[0-9a-f]{64}$' "
            "AND predecessor_wave_digest ~ '^[0-9a-f]{64}$' "
            "AND predecessor_release_queue = "
            "'arq:PTGSmall:wave:' || predecessor_wave_digest "
            "AND predecessor_intent_count BETWEEN 1 AND 4096",
            name="ptg_wave_rollback_predecessor_check",
        ),
        CheckConstraint(
            "jsonb_typeof(recovery_evidence) = 'object' "
            "AND recovery_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND octet_length(recovery_evidence_canonical) > 0 "
            "AND encode(sha256(recovery_evidence_canonical), 'hex') "
            "= recovery_evidence_sha256 "
            "AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb "
            "= recovery_evidence - 'proof_digest'",
            name="ptg_wave_rollback_evidence_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    predecessor_wave_id = Column(String(64), nullable=False)
    predecessor_idempotency_key = Column(String(160), nullable=False)
    predecessor_request_digest = Column(String(64), nullable=False)
    predecessor_wave_digest = Column(String(64), nullable=False)
    predecessor_release_queue = Column(String(160), nullable=False)
    predecessor_intent_count = Column(Integer, nullable=False)
    successor_wave_id = Column(String(64), nullable=False)
    recovery_basis = Column(String(64), nullable=False)
    recovery_evidence = Column(JSONB, nullable=False)
    recovery_evidence_canonical = Column(LargeBinary, nullable=False)
    recovery_evidence_sha256 = Column(String(64), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class PTGImportWaveIntent(Base, JSONOutputMixin):
    """One immutable, ordinal ARQ payload retained before any publication."""

    __tablename__ = "ptg_import_wave_intent"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("wave_id", "ordinal"),
        ForeignKeyConstraint(
            ("wave_id",), (PTGImportWave.wave_id,),
            name="ptg_import_wave_intent_wave_fkey", ondelete="CASCADE",
        ),
        UniqueConstraint("run_id", name="ptg_import_wave_intent_run_id_key"),
        UniqueConstraint("job_id", name="ptg_import_wave_intent_job_id_key"),
        UniqueConstraint(
            "wave_id", "ordinal", "run_id", "job_id",
            name="ptg_import_wave_intent_claim_identity_key",
        ),
        UniqueConstraint(
            "wave_id", "source_file_import_id",
            name="ptg_import_wave_intent_source_per_wave_key",
        ),
        CheckConstraint(
            "ordinal >= 0 AND length(run_id) > 0 "
            "AND length(source_file_import_id) > 0 "
            "AND length(content_version) > 0 "
            "AND length(run_idempotency_key) > 0 "
            "AND length(job_id) > 0 "
            "AND serialized_job_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(params) = 'object' AND json_typeof(job_payload) = 'object'",
            name="ptg_import_wave_intent_contract_check",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    wave_id = Column(String(64), nullable=False)
    ordinal = Column(Integer, nullable=False)
    run_id = Column(String(64), nullable=False)
    source_file_import_id = Column(String(64), nullable=False)
    content_version = Column(String(128), nullable=False)
    run_idempotency_key = Column(String(160), nullable=False)
    job_id = Column(String(96), nullable=False)
    params = Column(JSON, nullable=False)
    job_payload = Column(JSON, nullable=False)
    serialized_job = Column(LargeBinary, nullable=False)
    serialized_job_digest = Column(String(64), nullable=False)


class PTGImportWaveOrdinaryTerminalReceipt(Base, JSONOutputMixin):
    """Append-only RSA proof for one later ordinary PTG member run."""

    __tablename__ = "ptg_import_wave_ordinary_terminal_receipt"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("wave_id", "member_ordinal"),
        ForeignKeyConstraint(
            ("wave_id", "member_ordinal"),
            (PTGImportWaveIntent.wave_id, PTGImportWaveIntent.ordinal),
            name="ptg_wave_ordinary_terminal_member_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("run_id",),
            (ImportRun.run_id,),
            name="ptg_wave_ordinary_terminal_run_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "source_file_import_id",
            name="ptg_wave_ordinary_terminal_source_import_key",
        ),
        UniqueConstraint(
            "run_id",
            name="ptg_wave_ordinary_terminal_run_id_key",
        ),
        CheckConstraint(
            "member_ordinal >= 0 "
            "AND length(source_file_import_id) BETWEEN 1 AND 64 "
            "AND length(run_id) BETWEEN 1 AND 64 "
            "AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND jsonb_typeof(receipt) = 'object' "
            "AND receipt ->> 'schema' = "
            "'healthporta.ptg-wave-ordinary-terminal-receipt.v1' "
            "AND receipt ->> 'key_id' = receipt_key_id "
            "AND receipt ->> 'payload_digest' = payload_digest "
            "AND receipt #>> '{payload,wave_id}' = wave_id "
            "AND (receipt #>> '{payload,member_ordinal}')::integer "
            "= member_ordinal "
            "AND receipt #>> '{payload,source_file_import_id}' "
            "= source_file_import_id "
            "AND receipt #>> '{payload,run_id}' = run_id "
            "AND payload_digest ~ '^[0-9a-f]{64}$' "
            "AND length(receipt ->> 'signature') = 512 "
            "AND receipt ->> 'signature' ~ '^[0-9a-f]+$'",
            name="ptg_wave_ordinary_terminal_receipt_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    wave_id = Column(String(64), nullable=False)
    member_ordinal = Column(Integer, nullable=False)
    source_file_import_id = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    receipt_key_id = Column(String(64), nullable=False)
    receipt = Column(JSONB, nullable=False)
    payload_digest = Column(String(64), nullable=False)
    issued_at = Column(TIMESTAMP(timezone=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class PTGImportWaveClaim(Base, JSONOutputMixin):
    """One immutable pre-execution claim for an admitted ARQ job."""

    __tablename__ = "ptg_import_wave_claim"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("wave_id", "ordinal"),
        ForeignKeyConstraint(
            ("wave_id", "ordinal", "run_id", "job_id"),
            (
                PTGImportWaveIntent.wave_id, PTGImportWaveIntent.ordinal,
                PTGImportWaveIntent.run_id, PTGImportWaveIntent.job_id,
            ),
            name="ptg_import_wave_claim_intent_fkey", ondelete="CASCADE",
        ),
        UniqueConstraint("run_id", name="ptg_import_wave_claim_run_id_key"),
        UniqueConstraint("job_id", name="ptg_import_wave_claim_job_id_key"),
        CheckConstraint(
            "ordinal >= 0 AND slot >= 0 AND slot < 12 "
            "AND claim_status IN ('started', 'rejected') "
            "AND ((claim_status = 'started' AND failure_code IS NULL) "
            "OR (claim_status = 'rejected' "
            "AND failure_code IS NOT NULL "
            "AND failure_code ~ '^[a-z][a-z0-9_]{0,63}$')) "
            "AND claim_attempt_token ~ '^[0-9a-f]{32}$' "
            "AND manifest_identity ~ '^[0-9a-f]{64}$' "
            "AND length(pinned_image_reference) > 0 "
            "AND pinned_image_digest ~ '^[0-9a-f]{64}$' "
            "AND runtime_image_identity ~ '^sha256:[0-9a-f]{64}$' "
            "AND config_identity ~ '^[0-9a-f]{64}$' "
            "AND length(run_id) > 0 AND length(job_id) > 0 AND length(pod_uid) > 0",
            name="ptg_import_wave_claim_contract_check",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    wave_id = Column(String(64), nullable=False)
    ordinal = Column(Integer, nullable=False)
    run_id = Column(String(64), nullable=False)
    job_id = Column(String(96), nullable=False)
    slot = Column(Integer, nullable=False)
    pod_uid = Column(String(128), nullable=False)
    kubernetes_job_uid = Column(String(128), nullable=False)
    pinned_image_reference = Column(String(512), nullable=False)
    pinned_image_digest = Column(String(64), nullable=False)
    runtime_image_identity = Column(String(72), nullable=False)
    config_identity = Column(String(64), nullable=False)
    manifest_identity = Column(String(64), nullable=False)
    claim_status = Column(String(16), nullable=False, default="started")
    failure_code = Column(String(64))
    claim_attempt_token = Column(String(32), nullable=False)
    claimed_at = Column(TIMESTAMP, nullable=False)


class PTGImportWaveOutcome(Base, JSONOutputMixin):
    """Immutable terminal snapshot used for stable controller pagination."""

    __tablename__ = "ptg_import_wave_outcome"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("wave_id", "ordinal"),
        ForeignKeyConstraint(
            ("wave_id", "ordinal", "run_id", "job_id"),
            (
                PTGImportWaveIntent.wave_id, PTGImportWaveIntent.ordinal,
                PTGImportWaveIntent.run_id, PTGImportWaveIntent.job_id,
            ),
            name="ptg_import_wave_outcome_intent_fkey", ondelete="CASCADE",
        ),
        UniqueConstraint("run_id", name="ptg_import_wave_outcome_run_id_key"),
        CheckConstraint(
            "ordinal >= 0 AND status IN ('succeeded', 'failed', 'canceled', 'dead_letter') "
            "AND length(job_id) > 0 AND outcome_digest ~ '^[0-9a-f]{64}$' "
            "AND (status <> 'succeeded' OR (snapshot_id IS NOT NULL "
            "AND length(snapshot_id) > 0 AND import_id = source_file_import_id))",
            name="ptg_import_wave_outcome_contract_check",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    wave_id = Column(String(64), nullable=False)
    ordinal = Column(Integer, nullable=False)
    run_id = Column(String(64), nullable=False)
    job_id = Column(String(96), nullable=False)
    source_file_import_id = Column(String(64), nullable=False)
    content_version = Column(String(128), nullable=False)
    status = Column(String(32), nullable=False)
    snapshot_id = Column(String(96))
    import_id = Column(String(64))
    outcome_digest = Column(String(64), nullable=False)
    recorded_at = Column(TIMESTAMP, nullable=False)


class MRFPayer(Base, JSONOutputMixin):
    __tablename__ = "mrf_payer"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("payer_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["payer_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("canonical_name",), "name": "mrf_payer_name_idx"},
        {"index_elements": ("parent_group",), "name": "mrf_payer_parent_group_idx"},
        {"index_elements": ("entity_type",), "name": "mrf_payer_entity_type_idx"},
    ]

    payer_id = Column(String(64), nullable=False)
    canonical_name = Column(String(256), nullable=False)
    aliases = Column(JSON)
    parent_group = Column(String(128))
    entity_type = Column(String(64))
    states = Column(JSON)
    eins = Column(JSON)
    lifecycle = Column(String(32), nullable=False, default="active")
    source_coverage = Column(JSON)
    metadata_json = Column(JSON)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class MRFSource(Base, JSONOutputMixin):
    __tablename__ = "mrf_source"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_source_payer_idx"},
        {"index_elements": ("source_key",), "name": "mrf_source_key_idx", "unique": True},
        {"index_elements": ("canonical_url",), "name": "mrf_source_canonical_url_idx"},
        {"index_elements": ("status",), "name": "mrf_source_status_idx"},
        {"index_elements": ("hosting_platform",), "name": "mrf_source_hosting_platform_idx"},
        {"index_elements": ("seed_provider",), "name": "mrf_source_seed_provider_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source_key = Column(String(96), nullable=False)
    display_name = Column(String(256), nullable=False)
    source_type = Column(String(64))
    hosting_platform = Column(String(64))
    access_model = Column(String(32))
    index_url = Column(TEXT)
    human_url = Column(TEXT)
    canonical_url = Column(TEXT)
    domain = Column(String(256))
    status = Column(String(32), nullable=False, default="needs_review")
    schema_version = Column(String(32))
    etag = Column(String(512))
    last_modified = Column(String(256))
    content_version = Column(String(128))
    last_crawled_at = Column(TIMESTAMP)
    latest_index_date = Column(String(32))
    num_plans = Column(Integer)
    num_files = Column(Integer)
    num_indices = Column(Integer)
    total_compressed_size = Column(BigInteger)
    provenance_url = Column(TEXT)
    seed_provider = Column(String(64))
    confidence = Column(Integer)
    license_status = Column(String(64))
    review_status = Column(String(32))
    metadata_json = Column(JSON)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class MRFPlan(Base, JSONOutputMixin):
    __tablename__ = "mrf_plan"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("mrf_plan_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["mrf_plan_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_plan_payer_idx"},
        {"index_elements": ("source_id",), "name": "mrf_plan_source_idx"},
        {"index_elements": ("plan_id",), "name": "mrf_plan_plan_id_idx"},
        {"index_elements": ("market_type",), "name": "mrf_plan_market_idx"},
        {"index_elements": ("reporting_entity_name",), "name": "mrf_plan_reporting_entity_idx"},
    ]

    mrf_plan_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source_id = Column(String(64))
    plan_id = Column(String(128))
    plan_id_type = Column(String(64))
    plan_name = Column(String(512))
    market_type = Column(String(64))
    reporting_entity_name = Column(String(512))
    reporting_entity_type = Column(String(128))
    metadata_json = Column(JSON)
    first_seen_at = Column(TIMESTAMP)
    last_seen_at = Column(TIMESTAMP)


class MRFFile(Base, JSONOutputMixin):
    __tablename__ = "mrf_file"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("mrf_file_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["mrf_file_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_file_payer_idx"},
        {"index_elements": ("source_id",), "name": "mrf_file_source_idx"},
        {
            "index_elements": ("source_id", "mrf_file_id"),
            "name": "mrf_file_source_cursor_idx",
        },
        {"index_elements": ("file_type",), "name": "mrf_file_type_idx"},
        {"index_elements": ("canonical_url",), "name": "mrf_file_canonical_url_idx"},
        {"index_elements": ("last_seen_at",), "name": "mrf_file_last_seen_idx"},
    ]

    mrf_file_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source_id = Column(String(64))
    file_type = Column(String(64), nullable=False)
    url = Column(TEXT, nullable=False)
    canonical_url = Column(TEXT)
    from_index_url = Column(TEXT)
    description = Column(TEXT)
    network_name = Column(String(512))
    plan_ids = Column(JSON)
    plan_names = Column(JSON)
    market_types = Column(JSON)
    is_signed_url = Column(Boolean, nullable=False, default=False)
    size_bytes = Column(BigInteger)
    etag = Column(String(512))
    last_modified = Column(String(256))
    schema_version = Column(String(32))
    metadata_json = Column(JSON)
    first_seen_at = Column(TIMESTAMP)
    last_seen_at = Column(TIMESTAMP)


class MRFCrawlRun(Base, JSONOutputMixin):
    __tablename__ = "mrf_crawl_run"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("crawl_run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["crawl_run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("run_id",), "name": "mrf_crawl_run_control_run_idx"},
        {"index_elements": ("status",), "name": "mrf_crawl_run_status_idx"},
        {"index_elements": ("started_at",), "name": "mrf_crawl_run_started_idx"},
    ]

    crawl_run_id = Column(String(64), nullable=False)
    run_id = Column(String(64))
    provider = Column(String(128))
    mode = Column(String(64))
    status = Column(String(32), nullable=False)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    params = Column(JSON)
    sources_discovered = Column(Integer, nullable=False, default=0)
    urls_checked = Column(Integer, nullable=False, default=0)
    etag_skipped = Column(Integer, nullable=False, default=0)
    plans_discovered = Column(Integer, nullable=False, default=0)
    files_discovered = Column(Integer, nullable=False, default=0)
    bytes_streamed = Column(BigInteger, nullable=False, default=0)
    errors = Column(JSON)


class MRFDiscoveryBatch(Base, JSONOutputMixin):
    __tablename__ = "mrf_discovery_batch"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("root_run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["root_run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("latest_run_id",), "name": "mrf_discovery_batch_latest_run_idx"},
        {"index_elements": ("status", "updated_at"), "name": "mrf_discovery_batch_status_idx"},
    ]

    root_run_id = Column(String(64), nullable=False)
    latest_run_id = Column(String(64), nullable=False)
    retry_of_run_id = Column(String(64))
    strategy_version = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False)
    source_set_count = Column(Integer, nullable=False)
    source_set_sha256 = Column(String(64), nullable=False)
    source_payload_set_sha256 = Column(String(64), nullable=False)
    completed_source_count = Column(Integer, nullable=False, default=0)
    failed_source_count = Column(Integer, nullable=False, default=0)
    urls_checked = Column(Integer, nullable=False, default=0)
    plans_discovered = Column(Integer, nullable=False, default=0)
    files_discovered = Column(Integer, nullable=False, default=0)
    bytes_streamed = Column(BigInteger, nullable=False, default=0)
    lease_expires_at = Column(TIMESTAMP)
    started_at = Column(TIMESTAMP, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=False)
    completed_at = Column(TIMESTAMP)


class MRFDiscoverySourceCheckpoint(Base, JSONOutputMixin):
    __tablename__ = "mrf_discovery_source_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("root_run_id", "source_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["root_run_id", "source_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("root_run_id", "status", "source_id"),
            "name": "mrf_discovery_source_checkpoint_pending_idx",
        },
        {
            "index_elements": ("owner_run_id",),
            "name": "mrf_discovery_source_checkpoint_owner_idx",
        },
    ]

    root_run_id = Column(
        String(64),
        ForeignKey(
            f"{os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf'}.mrf_discovery_batch.root_run_id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    source_id = Column(String(64), nullable=False)
    owner_run_id = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False)
    source_payload = Column(JSON, nullable=False)
    source_payload_sha256 = Column(String(64), nullable=False)
    lease_expires_at = Column(TIMESTAMP)
    attempt_count = Column(Integer, nullable=False, default=0)
    urls_checked = Column(Integer, nullable=False, default=0)
    plans_discovered = Column(Integer, nullable=False, default=0)
    files_discovered = Column(Integer, nullable=False, default=0)
    bytes_streamed = Column(BigInteger, nullable=False, default=0)
    error = Column(JSON)
    started_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP, nullable=False)
    completed_at = Column(TIMESTAMP)


class MRFPayerScorecard(Base, JSONOutputMixin):
    __tablename__ = "mrf_payer_scorecard"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("scorecard_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["scorecard_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_payer_scorecard_payer_idx"},
        {"index_elements": ("source",), "name": "mrf_payer_scorecard_source_idx"},
    ]

    scorecard_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source = Column(String(64), nullable=False)
    score = Column(String(32))
    update_cadence = Column(String(64))
    file_accessibility_pct = Column(Integer)
    notes = Column(TEXT)
    payload = Column(JSON)
    observed_at = Column(TIMESTAMP)


class MRFUrlObservation(Base, JSONOutputMixin):
    __tablename__ = "mrf_url_observation"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("observation_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["observation_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("source_id",), "name": "mrf_url_observation_source_idx"},
        {"index_elements": ("canonical_url",), "name": "mrf_url_observation_url_idx"},
        {"index_elements": ("checked_at",), "name": "mrf_url_observation_checked_idx"},
        {"index_elements": ("status",), "name": "mrf_url_observation_status_idx"},
    ]

    observation_id = Column(String(64), nullable=False)
    source_id = Column(String(64))
    url = Column(TEXT, nullable=False)
    canonical_url = Column(TEXT)
    url_type = Column(String(64))
    status = Column(String(64), nullable=False)
    http_status = Column(Integer)
    etag = Column(String(512))
    last_modified = Column(String(256))
    content_length = Column(BigInteger)
    content_type = Column(String(256))
    final_url = Column(TEXT)
    checked_at = Column(TIMESTAMP)
    error = Column(TEXT)
    metadata_json = Column(JSON)


class ProviderDirectoryAPIEndpoint(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_api_endpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("endpoint_id"),
        UniqueConstraint(
            "canonical_api_base",
            "credential_descriptor_hash",
            "endpoint_signature_hash",
            name="provider_directory_api_endpoint_identity_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["endpoint_id"]

    endpoint_id = Column(String(64), nullable=False)
    canonical_api_base = Column(TEXT, nullable=False)
    credential_descriptor_hash = Column(String(64), nullable=False)
    endpoint_signature_hash = Column(String(64), nullable=False)
    credential_descriptor_json = Column(JSON)
    endpoint_signature_json = Column(JSON)
    first_seen_at = Column(TIMESTAMP)
    last_seen_at = Column(TIMESTAMP)
    metadata_json = Column(JSON)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryEndpointDataset(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_endpoint_dataset"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("dataset_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["dataset_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("endpoint_id",),
            "name": "provider_directory_endpoint_dataset_endpoint_idx",
        },
        {"index_elements": ("status",), "name": "provider_directory_endpoint_dataset_status_idx"},
        {
            "index_elements": ("endpoint_id",),
            "name": "provider_directory_endpoint_dataset_current_idx",
            "unique": True,
            "where": "is_current = true",
        },
        {
            "index_elements": ("endpoint_id", "acquisition_root_run_id"),
            "name": "provider_directory_endpoint_dataset_acquisition_root_idx",
            "unique": True,
            "where": "acquisition_root_run_id IS NOT NULL",
        },
        {"index_elements": ("dataset_hash",), "name": "provider_directory_endpoint_dataset_hash_idx"},
    ]

    dataset_id = Column(String(96), nullable=False)
    endpoint_id = Column(
        String(64),
        ForeignKey(
            ProviderDirectoryAPIEndpoint.endpoint_id,
            name="provider_directory_endpoint_dataset_endpoint_id_fkey",
        ),
        nullable=False,
    )
    import_run_id = Column(String(64))
    acquisition_root_run_id = Column(String(64))
    previous_dataset_id = Column(String(96))
    dataset_hash = Column(String(64))
    status = Column(String(32), nullable=False)
    is_current = Column(Boolean, nullable=False, default=False)
    resource_count = Column(BigInteger, nullable=False, default=0)
    created_at = Column(TIMESTAMP)
    validated_at = Column(TIMESTAMP)
    published_at = Column(TIMESTAMP)
    superseded_at = Column(TIMESTAMP)
    publication_metadata_json = Column(JSONB)
    publication_metadata_summary_json = Column(JSONB)
    publication_metadata_sha256 = Column(String(64))
    content_proof_admission_version = Column(SmallInteger)
    content_proof_admission_kind = Column(String(32))
    content_proof_admission_sha256 = Column(String(64))
    content_proof_resource_types = Column(ARRAY(String(64)))
    artifact_selection_receipt_json = Column(JSONB)
    completion_proof_required_version = Column(Integer)
    completion_proof_json = Column(JSONB)
    completion_proof_sha256 = Column(String(64))


class ProviderDirectoryDatasetResource(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_dataset_resource"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("dataset_id", "resource_type", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["dataset_id", "resource_type", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("resource_type", "resource_id"),
            "name": "provider_directory_dataset_resource_type_id_idx",
        },
        {
            "index_elements": ("dataset_id", "resource_id"),
            "name": "provider_directory_dataset_resource_plan_lookup_idx",
            "where": "resource_type = 'InsurancePlan'",
        },
        {"index_elements": ("payload_hash",), "name": "provider_directory_dataset_resource_hash_idx"},
    ]

    dataset_id = Column(
        String(96),
        ForeignKey(
            ProviderDirectoryEndpointDataset.dataset_id,
            name="provider_directory_dataset_resource_dataset_id_fkey",
        ),
        nullable=False,
    )
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    payload_hash = Column(String(64), nullable=False)
    payload_json = Column(JSON, nullable=False)
    acquired_resource_sha256 = Column(String(64))


class ProviderDirectoryDatasetInsurancePlan(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_dataset_insurance_plan"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("dataset_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["dataset_id", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "dataset_id",
                "resource_id",
            ),
            "include": ("plan_identifier",),
            "name": "provider_directory_dataset_insurance_plan_active_lookup_idx",
            "where": "plan_active",
        }
    ]

    dataset_id = Column(
        String(96),
        ForeignKey(
            ProviderDirectoryEndpointDataset.dataset_id,
            name="provider_directory_dataset_insurance_plan_dataset_id_fkey",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    resource_id = Column(String(256), nullable=False)
    payload_hash = Column(String(64), nullable=False)
    payload_json = Column(JSON, nullable=False)
    plan_active = Column(
        Boolean,
        Computed(
            "COALESCE(NULLIF(lower(btrim(payload_json ->> 'status')), ''), "
            "'active') = 'active'",
            persisted=True,
        ),
    )
    plan_identifier = Column(
        TEXT,
        Computed(
            "NULLIF(btrim(payload_json ->> 'plan_identifier'), '')",
            persisted=True,
        ),
    )


class ProviderDirectoryDatasetNetworkPlan(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_dataset_network_plan"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "dataset_id",
            "network_resource_id",
            "insurance_plan_resource_id",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "dataset_id",
        "network_resource_id",
        "insurance_plan_resource_id",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": ("dataset_id", "insurance_plan_resource_id"),
            "include": ("network_resource_id",),
            "name": "provider_directory_dataset_network_plan_reverse_lookup_idx",
        },
    ]
    dataset_id = Column(
        String(96),
        ForeignKey(
            ProviderDirectoryEndpointDataset.dataset_id,
            name="provider_directory_dataset_network_plan_dataset_id_fkey",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    network_resource_id = Column(String(256), nullable=False)
    insurance_plan_resource_id = Column(String(256), nullable=False)


class ProviderDirectoryDatasetAffiliationOrganization(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_dataset_affiliation_organization"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "dataset_id",
            "participating_organization_resource_id",
            "affiliation_resource_id",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "dataset_id",
        "participating_organization_resource_id",
        "affiliation_resource_id",
    ]
    dataset_id = Column(
        String(96),
        ForeignKey(
            ProviderDirectoryEndpointDataset.dataset_id,
            name="pd_dataset_affiliation_org_dataset_id_fkey",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    participating_organization_resource_id = Column(
        String(256),
        nullable=False,
    )
    affiliation_resource_id = Column(String(256), nullable=False)


class ProviderDirectorySource(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_source"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("canonical_api_base",), "name": "provider_directory_source_api_base_idx"},
        {"index_elements": ("org_name",), "name": "provider_directory_source_org_name_idx"},
        {"index_elements": ("auth_type",), "name": "provider_directory_source_auth_type_idx"},
        {"index_elements": ("last_validated_status",), "name": "provider_directory_source_validation_idx"},
        {"index_elements": ("data_quality_flag",), "name": "provider_directory_source_data_quality_idx"},
        {"index_elements": ("endpoint_id",), "name": "provider_directory_source_endpoint_id_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    org_tin = Column(String(64))
    org_name = Column(String(256), nullable=False)
    plan_name = Column(String(512))
    portal_url = Column(TEXT)
    api_base = Column(TEXT)
    canonical_api_base = Column(TEXT)
    endpoint_id = Column(
        String(64),
        ForeignKey(
            ProviderDirectoryAPIEndpoint.endpoint_id,
            name="provider_directory_source_endpoint_id_fkey",
            ondelete="SET NULL",
        ),
    )
    endpoint_insurance_plan = Column(TEXT)
    endpoint_practitioner = Column(TEXT)
    endpoint_practitioner_role = Column(TEXT)
    endpoint_organization = Column(TEXT)
    endpoint_organization_affiliation = Column(TEXT)
    endpoint_location = Column(TEXT)
    endpoint_healthcare_service = Column(TEXT)
    endpoint_network = Column(TEXT)
    endpoint_endpoint = Column(TEXT)
    requires_registration = Column(Boolean, nullable=False, default=False)
    requires_api_key = Column(Boolean, nullable=False, default=False)
    auth_type = Column(String(64))
    last_validated = Column(String(64))
    last_validated_status = Column(String(64))
    fhir_version = Column(String(32))
    compliance_flag = Column(String(64))
    violation_type = Column(String(128))
    violation_detail = Column(TEXT)
    data_quality_flag = Column(String(64))
    data_quality_sample_npi = Column(String(32))
    data_quality_practitioner_count = Column(String(64))
    data_quality_checked = Column(TEXT)
    is_medicare_advantage = Column(Boolean)
    is_medicaid_mco = Column(Boolean)
    is_chip = Column(Boolean)
    is_qhp = Column(Boolean)
    seed_source = Column(String(128))
    seed_source_detail = Column(TEXT)
    seed_source_url = Column(TEXT)
    seed_source_date = Column(String(64))
    seed_row_id = Column(String(64))
    id_provider_alt = Column(String(128))
    team_status = Column(String(128))
    last_probe_status = Column(String(64))
    last_probe_status_code = Column(Integer)
    last_probe_error = Column(TEXT)
    last_probe_run_id = Column(String(64))
    last_probed_at = Column(TIMESTAMP)
    metadata_json = Column(JSON)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryCapability(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_capability"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("probe_status",), "name": "provider_directory_capability_status_idx"},
        {"index_elements": ("fhir_version",), "name": "provider_directory_capability_fhir_version_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    api_base = Column(TEXT)
    metadata_url = Column(TEXT)
    probe_status = Column(String(64), nullable=False)
    http_status = Column(Integer)
    response_time_ms = Column(Integer)
    resource_type = Column(String(64))
    fhir_version = Column(String(32))
    software_name = Column(String(256))
    software_version = Column(String(128))
    implementation_url = Column(TEXT)
    formats = Column(JSON)
    supported_resources = Column(JSON)
    search_params = Column(JSON)
    auth_required = Column(Boolean, nullable=False, default=False)
    error = Column(TEXT)
    capability_hash = Column(String(64))
    probed_at = Column(TIMESTAMP)
    run_id = Column(String(64))
    metadata_json = Column(JSON)


class ProviderDirectoryInsurancePlan(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_insurance_plan"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("plan_identifier",), "name": "provider_directory_insurance_plan_identifier_idx"},
        {"index_elements": ("name",), "name": "provider_directory_insurance_plan_name_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    plan_identifier = Column(String(256))
    product_identifiers = Column(JSON)
    plan_backbones = Column(JSON)
    coverage = Column(JSON)
    status = Column(String(64))
    name = Column(String(512))
    aliases = Column(JSON)
    type_codes = Column(JSON)
    owned_by_ref = Column(TEXT)
    administered_by_ref = Column(TEXT)
    network_refs = Column(JSON)
    coverage_area_refs = Column(JSON)
    plan_json = Column(JSON)
    period_start = Column(String(64))
    period_end = Column(String(64))
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryPractitioner(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_practitioner"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi",), "name": "provider_directory_practitioner_npi_idx"},
        {"index_elements": ("family_name",), "name": "provider_directory_practitioner_family_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    npi = Column(BigInteger)
    active = Column(Boolean)
    identifiers = Column(JSON)
    names = Column(JSON)
    family_name = Column(String(256))
    given_names = Column(JSON)
    full_name = Column(String(512))
    administrative_gender = Column(String(32))
    age_years = Column(Integer)
    age_as_of = Column(String(10))
    years_of_practice = Column(Integer)
    years_of_practice_as_of = Column(String(10))
    years_of_practice_basis = Column(String(128))
    years_of_practice_start_date = Column(String(10))
    telecom = Column(JSON)
    addresses = Column(JSON)
    qualification_codes = Column(JSON)
    qualifications = Column(JSON)
    communication_codes = Column(JSON)
    communications = Column(JSON)
    photos = Column(JSON)
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryOrganization(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_organization"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi",), "name": "provider_directory_organization_npi_idx"},
        {"index_elements": ("tax_id",), "name": "provider_directory_organization_tax_id_idx"},
        {"index_elements": ("name",), "name": "provider_directory_organization_name_idx"},
        {
            "index_elements": ("last_seen_run_id", "source_id"),
            "name": "provider_directory_organization_run_source_idx",
        },
    ]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    npi = Column(BigInteger)
    tax_id = Column(String(64))
    tin_status = Column(String(64))
    active = Column(Boolean)
    identifiers = Column(JSON)
    name = Column(String(512))
    name_variants = Column(JSON)
    aliases = Column(JSON)
    type_codes = Column(JSON)
    telecom = Column(JSON)
    address_json = Column(JSON)
    contacts = Column(JSON)
    part_of_ref = Column(TEXT)
    endpoint_refs = Column(JSON)
    source_lineage = Column(JSON)
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryLocation(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_location"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("address_key",), "name": "provider_directory_location_address_key_idx"},
        {"index_elements": ("zip5",), "name": "provider_directory_location_zip5_idx"},
        {"index_elements": ("state_code", "city_norm"), "name": "provider_directory_location_state_city_idx"},
        {"index_elements": ("last_seen_run_id",), "name": "provider_directory_location_run_idx"},
        {"index_elements": ("last_seen_run_id", "source_id"), "name": "provider_directory_location_run_source_idx"},
        {
            "index_elements": ("phone_number",),
            "name": "provider_directory_location_phone_number_idx",
            "where": "phone_number IS NOT NULL AND phone_number <> ''",
        },
    ]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    status = Column(String(64))
    name = Column(String(512))
    description = Column(TEXT)
    mode = Column(String(64))
    type_codes = Column(JSON)
    physical_type_codes = Column(JSON)
    managing_organization_ref = Column(TEXT)
    first_line = Column(String)
    second_line = Column(String)
    city_name = Column(String)
    state_name = Column(String)
    state_code = Column(String(2))
    postal_code = Column(String)
    zip5 = Column(String(5))
    city_norm = Column(String)
    country_code = Column(String)
    telephone_number = Column(String)
    phone_number = Column(String(15))
    phone_extension = Column(String(16))
    fax_number = Column(String)
    fax_number_digits = Column(String(15))
    fax_extension = Column(String(16))
    telecom = Column(JSON)
    addresses = Column(JSON)
    hours_of_operation = Column(JSON)
    availability_exceptions = Column(TEXT)
    photos = Column(JSON)
    latitude = Column(String(64))
    longitude = Column(String(64))
    address_key = Column(String(64))
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryPractitionerRole(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_practitioner_role"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("practitioner_ref",), "name": "provider_directory_role_practitioner_idx"},
        {"index_elements": ("source_id", "practitioner_ref"), "name": "provider_directory_role_source_practitioner_idx"},
        {"index_elements": ("organization_ref",), "name": "provider_directory_role_organization_idx"},
        {"index_elements": ("source_id", "organization_ref"), "name": "provider_directory_role_source_organization_idx"},
        {"index_elements": ("last_seen_run_id", "source_id"), "name": "provider_directory_role_run_source_idx"},
        {"index_elements": ("location_refs",), "using": "gin", "name": "provider_directory_role_location_refs_gin_idx"},
        {"index_elements": ("specialty_codes",), "using": "gin", "name": "provider_directory_role_specialty_codes_gin_idx"},
        {"index_elements": ("code_codes",), "using": "gin", "name": "provider_directory_role_code_codes_gin_idx"},
        {"index_elements": ("network_refs",), "using": "gin", "name": "provider_directory_role_network_refs_gin_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    npi = Column(BigInteger)
    active = Column(Boolean)
    identifiers = Column(JSON)
    practitioner_ref = Column(TEXT)
    organization_ref = Column(TEXT)
    location_refs = Column(JSON)
    healthcare_service_refs = Column(JSON)
    network_refs = Column(JSON)
    insurance_plan_refs = Column(JSON)
    endpoint_refs = Column(JSON)
    specialty_codes = Column(JSON)
    code_codes = Column(JSON)
    telecom = Column(JSON)
    accepting_patients = Column(JSON)
    available_time = Column(JSON)
    not_available = Column(JSON)
    availability_exceptions = Column(TEXT)
    new_patient_acceptance = Column(JSON)
    telehealth = Column(JSON)
    accepting_medicaid = Column(Boolean)
    plan_scope = Column(JSON)
    network_tier = Column(String(128))
    network_key_id = Column(String(64))
    period_start = Column(String(64))
    period_end = Column(String(64))
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryHealthcareService(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_healthcare_service"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    provided_by_ref = Column(TEXT)
    accepting_patients = Column(JSON)
    npi = Column(BigInteger)
    active = Column(Boolean)
    identifiers = Column(JSON)
    name = Column(String(512))
    type_codes = Column(JSON)
    category_codes = Column(JSON)
    specialty_codes = Column(JSON)
    program_codes = Column(JSON)
    characteristic_codes = Column(JSON)
    communication_codes = Column(JSON)
    referral_method_codes = Column(JSON)
    service_provision_codes = Column(JSON)
    eligibility = Column(JSON)
    appointment_required = Column(Boolean)
    location_refs = Column(JSON)
    endpoint_refs = Column(JSON)
    telecom = Column(JSON)
    coverage_area_refs = Column(JSON)
    available_time = Column(JSON)
    not_available = Column(JSON)
    availability_exceptions = Column(TEXT)
    extra_details = Column(TEXT)
    comment = Column(TEXT)
    photos = Column(JSON)
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryOrganizationAffiliation(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_organization_affiliation"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("source_id", "organization_ref"),
            "name": "provider_directory_affiliation_source_organization_idx",
        },
        {
            "index_elements": ("source_id", "participating_organization_ref"),
            "name": "provider_directory_affiliation_source_participating_idx",
        },
        {
            "index_elements": ("last_seen_run_id", "source_id"),
            "name": "provider_directory_affiliation_run_source_idx",
        },
    ]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    active = Column(Boolean)
    identifiers = Column(JSON)
    organization_ref = Column(TEXT)
    participating_organization_ref = Column(TEXT)
    network_refs = Column(JSON)
    insurance_plan_refs = Column(JSON)
    location_refs = Column(JSON)
    healthcare_service_refs = Column(JSON)
    endpoint_refs = Column(JSON)
    specialty_codes = Column(JSON)
    code_codes = Column(JSON)
    telecom = Column(JSON)
    plan_scope = Column(JSON)
    network_tier = Column(String(128))
    network_key_id = Column(String(64))
    relationship_type = Column(String(64))
    ownership_status = Column(String(64))
    source_lineage = Column(JSON)
    period_start = Column(String(64))
    period_end = Column(String(64))
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryEndpoint(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_endpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("source_id", "managing_organization_ref"),
            "name": "provider_directory_endpoint_source_managing_org_idx",
        },
        {"index_elements": ("status",), "name": "provider_directory_endpoint_status_idx"},
        {
            "index_elements": ("connection_type_code",),
            "name": "provider_directory_endpoint_connection_type_idx",
        },
    ]

    source_id = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    status = Column(String(64))
    connection_type_system = Column(TEXT)
    connection_type_code = Column(String(128))
    connection_type_display = Column(String(256))
    name = Column(String(512))
    managing_organization_ref = Column(TEXT)
    contact = Column(JSON)
    period_start = Column(String(64))
    period_end = Column(String(64))
    payload_type_codes = Column(JSON)
    payload_mime_types = Column(JSON)
    address = Column(TEXT)
    header = Column(JSON)
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryCanonicalResource(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_canonical_resource"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("canonical_api_base", "resource_type", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["canonical_api_base", "resource_type", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("resource_type", "resource_id"),
            "name": "provider_directory_canonical_resource_type_id_idx",
        },
        {"index_elements": ("payload_hash",), "name": "provider_directory_canonical_resource_hash_idx"},
        {"index_elements": ("last_seen_run_id",), "name": "provider_directory_canonical_resource_run_idx"},
    ]

    canonical_api_base = Column(TEXT, nullable=False)
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    resource_url = Column(TEXT)
    fhir_meta = Column(JSON)
    fhir_self_url = Column(TEXT)
    fhir_fetch_url = Column(TEXT)
    fhir_fetch_mode = Column(String(32))
    payload_hash = Column(String(64))
    payload_json = Column(JSON)
    first_seen_run_id = Column(String(64))
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectorySourceResource(Base, JSONOutputMixin):
    __tablename__ = "provider_directory_source_resource"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_type", "resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "resource_type", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("canonical_api_base", "resource_type", "resource_id"),
            "name": "provider_directory_source_resource_canonical_idx",
        },
        {"index_elements": ("last_seen_run_id",), "name": "provider_directory_source_resource_run_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    canonical_api_base = Column(TEXT, nullable=False)
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    last_seen_run_id = Column(String(64))
    observed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ProviderDirectoryDatasetRehydrationCheckpoint(Base, JSONOutputMixin):
    """Durable, scope-fenced progress for retained-dataset rehydration."""

    __tablename__ = "provider_directory_dataset_rehydration_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "source_id", "dataset_id", "acquisition_root_run_id", "resource_type"
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "source_id", "dataset_id", "acquisition_root_run_id", "resource_type"
    ]
    __my_additional_indexes__ = [
        {"index_elements": ("owner_run_id",), "name": "pd_dataset_rehydrate_checkpoint_owner_idx"},
        {"index_elements": ("state", "updated_at"), "name": "pd_dataset_rehydrate_checkpoint_state_idx"},
        {"index_elements": ("dataset_id",), "name": "pd_dataset_rehydrate_checkpoint_dataset_idx"},
    ]

    source_id = Column(String(64), ForeignKey(ProviderDirectorySource.source_id), nullable=False)
    dataset_id = Column(String(96), ForeignKey(ProviderDirectoryEndpointDataset.dataset_id), nullable=False)
    acquisition_root_run_id = Column(String(64), nullable=False)
    resource_type = Column(String(64), nullable=False)
    endpoint_id = Column(String(64), ForeignKey(ProviderDirectoryAPIEndpoint.endpoint_id), nullable=False)
    dataset_hash = Column(String(64), nullable=False)
    owner_run_id = Column(String(64), nullable=False)
    state = Column(String(32), nullable=False)
    last_resource_id = Column(String(256))
    expected_input_count = Column(BigInteger, nullable=False, default=0)
    input_count = Column(BigInteger, nullable=False, default=0)
    mapped_count = Column(BigInteger, nullable=False, default=0)
    rejected_count = Column(BigInteger, nullable=False, default=0)
    evidence_json = Column(JSON, nullable=False, default=dict)
    error = Column(TEXT)
    created_at = Column(TIMESTAMP, nullable=False)
    started_at = Column(TIMESTAMP, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=False)
    completed_at = Column(TIMESTAMP)


class ProviderDirectoryBulkAcquisitionCheckpoint(Base, JSONOutputMixin):
    """Durable identity and lifecycle for one accepted FHIR Bulk Data export."""

    __tablename__ = "provider_directory_bulk_acquisition_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("checkpoint_id"),
        UniqueConstraint(
            "canonical_api_base",
            "resource_type",
            "source_scope_hash",
            "strategy_version",
            "acquisition_root_run_id",
            "dataset_id",
            name="provider_directory_bulk_acquisition_identity_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["checkpoint_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("dataset_id",),
            "name": "provider_directory_bulk_acquisition_dataset_idx",
        },
        {
            "index_elements": ("owner_run_id",),
            "name": "provider_directory_bulk_acquisition_owner_idx",
        },
        {
            "index_elements": ("acquisition_root_run_id",),
            "name": "provider_directory_bulk_acquisition_root_idx",
        },
        {
            "index_elements": ("state", "updated_at"),
            "name": "provider_directory_bulk_acquisition_state_updated_idx",
        },
    ]

    checkpoint_id = Column(String(64), nullable=False)
    canonical_api_base = Column(TEXT, nullable=False)
    resource_type = Column(String(64), nullable=False)
    source_scope_hash = Column(String(64), nullable=False)
    strategy_version = Column(String(64), nullable=False)
    acquisition_root_run_id = Column(String(64), nullable=False)
    owner_run_id = Column(String(64), nullable=False)
    retry_of_run_id = Column(String(64))
    endpoint_id = Column(
        String(64),
        ForeignKey(
            ProviderDirectoryAPIEndpoint.endpoint_id,
            name="provider_directory_bulk_acquisition_endpoint_id_fkey",
        ),
        nullable=False,
    )
    dataset_id = Column(
        String(96),
        ForeignKey(
            ProviderDirectoryEndpointDataset.dataset_id,
            name="provider_directory_bulk_acquisition_dataset_id_fkey",
        ),
        nullable=False,
    )
    start_url_hash = Column(String(64), nullable=False)
    status_url_ciphertext = Column(TEXT)
    status_url_hash = Column(String(64))
    manifest_hash = Column(String(64))
    manifest_ciphertext = Column(TEXT)
    manifest_json = Column(JSON)
    state = Column(String(32), nullable=False)
    lease_expires_at = Column(TIMESTAMP)
    rows_written = Column(BigInteger, nullable=False, default=0)
    error = Column(TEXT)
    created_at = Column(TIMESTAMP, nullable=False)
    accepted_at = Column(TIMESTAMP)
    last_polled_at = Column(TIMESTAMP)
    next_poll_at = Column(TIMESTAMP)
    manifest_received_at = Column(TIMESTAMP)
    completed_at = Column(TIMESTAMP)
    failed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP, nullable=False)


class ProviderDirectoryBulkOutputCheckpoint(Base, JSONOutputMixin):
    """Completion state for one immutable Bulk Data manifest output."""

    __tablename__ = "provider_directory_bulk_output_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("checkpoint_id", "output_id"),
        UniqueConstraint(
            "checkpoint_id",
            "output_index",
            name="provider_directory_bulk_output_index_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["checkpoint_id", "output_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("state", "updated_at"),
            "name": "provider_directory_bulk_output_state_updated_idx",
        },
    ]

    checkpoint_id = Column(
        String(64),
        ForeignKey(
            ProviderDirectoryBulkAcquisitionCheckpoint.checkpoint_id,
            name="provider_directory_bulk_output_checkpoint_id_fkey",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    output_id = Column(String(64), nullable=False)
    output_index = Column(Integer, nullable=False)
    resource_type = Column(String(64), nullable=False)
    output_url_ciphertext = Column(TEXT)
    output_url_hash = Column(String(64), nullable=False)
    state = Column(String(32), nullable=False)
    rows_written = Column(BigInteger, nullable=False, default=0)
    content_length_bytes = Column(BigInteger)
    etag_ciphertext = Column(TEXT)
    etag_hash = Column(String(64))
    committed_bytes = Column(
        BigInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    output_expires_at = Column(TIMESTAMP)
    validator_checked_at = Column(TIMESTAMP)
    attempt_count = Column(Integer, nullable=False, default=0)
    error = Column(TEXT)
    last_error = Column(TEXT)
    last_error_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, nullable=False)
    started_at = Column(TIMESTAMP)
    completed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP, nullable=False)


class ProviderDirectoryPaginationCheckpoint(Base, JSONOutputMixin):
    """Durable resume state for one source-scoped paginated resource scan."""

    __tablename__ = "provider_directory_pagination_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "canonical_api_base",
            "resource_type",
            "source_scope_hash",
            "acquisition_root_run_id",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "canonical_api_base",
        "resource_type",
        "source_scope_hash",
        "acquisition_root_run_id",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": ("owner_run_id",),
            "name": "provider_directory_pagination_checkpoint_owner_idx",
        },
        {
            "index_elements": ("state", "updated_at"),
            "name": "provider_directory_pagination_checkpoint_state_updated_idx",
        },
        {
            "index_elements": ("dataset_id",),
            "name": "provider_directory_pagination_checkpoint_dataset_idx",
        },
        {
            "index_elements": ("acquisition_root_run_id", "updated_at"),
            "name": "provider_directory_pagination_checkpoint_root_updated_idx",
        },
    ]

    canonical_api_base = Column(TEXT, nullable=False)
    resource_type = Column(String(64), nullable=False)
    source_scope_hash = Column(String(64), nullable=False)
    dataset_id = Column(
        String(96),
        ForeignKey(
            ProviderDirectoryEndpointDataset.dataset_id,
            name="provider_directory_pagination_checkpoint_dataset_id_fkey",
        ),
    )
    source_ids = Column(JSON, nullable=False)
    acquisition_root_run_id = Column(String(64), nullable=False)
    owner_run_id = Column(String(64), nullable=False)
    retry_of_run_id = Column(String(64))
    start_url_hash = Column(String(64), nullable=False)
    next_url = Column(TEXT)
    state = Column(String(32), nullable=False)
    pages_processed = Column(BigInteger, nullable=False, default=0)
    rows_processed = Column(BigInteger, nullable=False, default=0)
    recent_cursor_hashes = Column(JSON, nullable=False, default=list)
    completeness_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(TIMESTAMP, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=False)
    completed_at = Column(TIMESTAMP)


class ProviderDirectoryProfileBuildCheckpoint(Base, JSONOutputMixin):
    """Durable, lineage-fenced progress for one staged Profile build."""

    __tablename__ = "provider_directory_profile_build_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("build_id"),
        CheckConstraint(
            "resume_lineage_hash ~ '^[0-9a-f]{64}$'",
            name="pd_profile_build_checkpoint_lineage_hash_check",
        ),
        CheckConstraint(
            "evidence_stage_oid > 0 AND profile_stage_oid > 0 "
            "AND (evidence_target_oid IS NULL OR evidence_target_oid > 0) "
            "AND (profile_target_oid IS NULL OR profile_target_oid > 0)",
            name="pd_profile_build_checkpoint_oid_check",
        ),
        CheckConstraint(
            "(evidence_stage_storage_fingerprint IS NULL "
            "AND profile_stage_storage_fingerprint IS NULL "
            "AND affected_npi_stage_storage_fingerprint IS NULL) "
            "OR (evidence_stage_storage_fingerprint "
            "~ '^[0-9a-f]{64}$' "
            "AND profile_stage_storage_fingerprint "
            "~ '^[0-9a-f]{64}$' "
            "AND ((materialization_mode = 'source_delta' "
            "AND affected_npi_stage_storage_fingerprint "
            "~ '^[0-9a-f]{64}$') "
            "OR (materialization_mode = 'full_swap' "
            "AND affected_npi_stage_storage_fingerprint IS NULL)))",
            name="pd_profile_build_checkpoint_stage_storage_check",
        ),
        CheckConstraint(
            "state IN ('building_evidence', 'evidence_complete', "
            "'building_profile', 'ready', 'failed')",
            name="pd_profile_build_checkpoint_state_check",
        ),
        CheckConstraint(
            "evidence_total_batches >= 0 "
            "AND evidence_next_batch BETWEEN 0 AND evidence_total_batches "
            "AND profile_total_batches >= 0 "
            "AND profile_next_batch BETWEEN 0 AND profile_total_batches",
            name="pd_profile_build_checkpoint_batch_bounds_check",
        ),
        CheckConstraint(
            "profile_next_batch = 0 "
            "OR evidence_next_batch = evidence_total_batches",
            name="pd_profile_build_checkpoint_phase_order_check",
        ),
        CheckConstraint(
            "executable_plan_hash IS NULL "
            "OR executable_plan_hash ~ '^[0-9a-f]{64}$'",
            name="pd_profile_build_checkpoint_plan_hash_check",
        ),
        CheckConstraint(
            "materialization_mode IN ('full_swap', 'source_delta')",
            name="pd_profile_build_checkpoint_mode_check",
        ),
        CheckConstraint(
            "(materialization_mode = 'full_swap' "
            "AND current_source_vector_hash IS NULL "
            "AND desired_source_vector_hash IS NULL "
            "AND current_source_context_vector_hash IS NULL "
            "AND desired_source_context_vector_hash IS NULL "
            "AND affected_npi_stage IS NULL "
            "AND affected_npi_stage_oid IS NULL "
            "AND capacity_geometry_status = 'legacy_unavailable' "
            "AND capacity_geometry_hash IS NULL "
            "AND capacity_geometry_json IS NULL) "
            "OR (materialization_mode = 'source_delta' "
            "AND current_source_vector_hash IS NOT NULL "
            "AND current_source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND desired_source_vector_hash IS NOT NULL "
            "AND desired_source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND current_source_context_vector_hash IS NOT NULL "
            "AND current_source_context_vector_hash "
            "~ '^[0-9a-f]{64}$' "
            "AND desired_source_context_vector_hash IS NOT NULL "
            "AND desired_source_context_vector_hash "
            "~ '^[0-9a-f]{64}$' "
            "AND affected_npi_stage IS NOT NULL "
            "AND affected_npi_stage_oid IS NOT NULL "
            "AND affected_npi_stage_oid > 0 "
            "AND capacity_geometry_status = 'verified' "
            "AND capacity_geometry_hash IS NOT NULL "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_json IS NOT NULL "
            "AND jsonb_typeof(capacity_geometry_json::jsonb) = 'object')",
            name="pd_profile_build_checkpoint_delta_identity_check",
        ),
        CheckConstraint(
            "state = 'failed' "
            "OR (state = 'building_evidence' AND profile_next_batch = 0) "
            "OR (state = 'evidence_complete' "
            "AND evidence_next_batch = evidence_total_batches "
            "AND profile_next_batch = 0) "
            "OR (state = 'building_profile' "
            "AND evidence_next_batch = evidence_total_batches) "
            "OR (state = 'ready' "
            "AND evidence_next_batch = evidence_total_batches "
            "AND profile_next_batch = profile_total_batches)",
            name="pd_profile_build_checkpoint_state_progress_check",
        ),
        CheckConstraint(
            "(cutover_forecast_status = 'not_started' "
            "AND cutover_forecast_hash IS NULL "
            "AND cutover_forecast_json IS NULL) "
            "OR (cutover_forecast_status = 'verified' "
            "AND cutover_forecast_hash IS NOT NULL "
            "AND cutover_forecast_hash ~ '^[0-9a-f]{64}$' "
            "AND cutover_forecast_json IS NOT NULL "
            "AND jsonb_typeof(cutover_forecast_json::jsonb) = 'object')",
            name="pd_profile_build_checkpoint_forecast_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["build_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("state", "updated_at"),
            "name": "pd_profile_build_checkpoint_state_idx",
        },
        {
            "index_elements": ("owner_run_id",),
            "name": "pd_profile_build_checkpoint_owner_idx",
        },
    ]

    build_id = Column(String(64), nullable=False)
    strategy_version = Column(String(64), nullable=False)
    schema_version = Column(Integer, nullable=False)
    resume_lineage_hash = Column(String(64), nullable=False)
    executable_plan_hash = Column(String(64))
    owner_run_id = Column(String(64))
    state = Column(String(32), nullable=False)
    materialization_mode = Column(
        String(16),
        nullable=False,
        default="full_swap",
    )
    profile_as_of = Column(String(10), nullable=False)
    source_ids = Column(JSON, nullable=False)
    retained_source_ids = Column(JSON, nullable=False)
    dataset_ids = Column(JSON, nullable=False)
    evidence_stage = Column(String(63), nullable=False)
    profile_stage = Column(String(63), nullable=False)
    evidence_stage_oid = Column(BigInteger, nullable=False)
    profile_stage_oid = Column(BigInteger, nullable=False)
    evidence_stage_storage_fingerprint = Column(String(64))
    profile_stage_storage_fingerprint = Column(String(64))
    affected_npi_stage_storage_fingerprint = Column(String(64))
    evidence_target_oid = Column(BigInteger)
    profile_target_oid = Column(BigInteger)
    current_source_vector_hash = Column(String(64))
    desired_source_vector_hash = Column(String(64))
    current_source_context_vector_hash = Column(String(64))
    desired_source_context_vector_hash = Column(String(64))
    refresh_source_ids = Column(JSON)
    removed_source_ids = Column(JSON)
    affected_npi_stage = Column(String(63))
    affected_npi_stage_oid = Column(BigInteger)
    capacity_geometry_status = Column(
        String(32),
        nullable=False,
        default="legacy_unavailable",
        server_default=text("'legacy_unavailable'"),
    )
    capacity_geometry_hash = Column(String(64))
    capacity_geometry_json = Column(JSON)
    cutover_forecast_status = Column(
        String(32),
        nullable=False,
        default="not_started",
        server_default=text("'not_started'"),
    )
    cutover_forecast_hash = Column(String(64))
    cutover_forecast_json = Column(JSON)
    has_existing_artifacts = Column(Boolean, nullable=False)
    evidence_next_batch = Column(Integer, nullable=False, default=0)
    evidence_total_batches = Column(Integer, nullable=False)
    profile_next_batch = Column(Integer, nullable=False, default=0)
    profile_total_batches = Column(Integer, nullable=False)
    last_error = Column(TEXT)
    created_at = Column(TIMESTAMP, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=False)
    completed_at = Column(TIMESTAMP)


class ProviderDirectoryProfileServingGeneration(Base, JSONOutputMixin):
    """One exact global generation over incrementally materialized profiles."""

    __tablename__ = "provider_directory_profile_serving_generation"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("singleton_key"),
        CheckConstraint(
            "singleton_key = 'global' "
            "AND ((status = 'published' AND operation = 'publish') "
            "OR (status = 'purged' AND operation = 'purge')) "
            "AND generation_id ~ '^pdprofile_[0-9a-f]{32}$' "
            "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
            "AND source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND executable_plan_hash ~ '^[0-9a-f]{64}$' "
            "AND ((capacity_geometry_status = 'legacy_unavailable' "
            "AND capacity_geometry_hash IS NULL "
            "AND capacity_geometry_json IS NULL) "
            "OR (capacity_geometry_status = 'verified' "
            "AND capacity_geometry_hash IS NOT NULL "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_json IS NOT NULL "
            "AND jsonb_typeof(capacity_geometry_json::jsonb) = 'object')) "
            "AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
            "AND control_generation > 0 "
            "AND authority_revision > 0 "
            "AND profile_schema_version > 0 "
            "AND evidence_target_oid > 0 "
            "AND profile_target_oid > 0 "
            "AND evidence_rows >= 0 "
            "AND profile_rows >= 0 "
            "AND (cutover_forecast_hash IS NULL "
            "OR cutover_forecast_hash ~ '^[0-9a-f]{64}$')",
            name="pd_profile_serving_generation_values_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["singleton_key"]

    singleton_key = Column(String(16), nullable=False)
    status = Column(String(16), nullable=False)
    operation = Column(String(16), nullable=False)
    control_generation = Column(BigInteger, nullable=False)
    generation_id = Column(String(64), nullable=False)
    selection_proof_id = Column(String(64), nullable=False)
    authority_revision = Column(BigInteger, nullable=False)
    profile_schema_version = Column(Integer, nullable=False)
    profile_strategy_version = Column(String(128), nullable=False)
    source_vector_hash = Column(String(64), nullable=False)
    source_vector_json = Column(JSON, nullable=False)
    source_context_vector_hash = Column(String(64), nullable=False)
    source_context_vector_json = Column(JSON, nullable=False)
    executable_plan_hash = Column(String(64), nullable=False)
    capacity_geometry_status = Column(String(32), nullable=False)
    capacity_geometry_hash = Column(String(64))
    capacity_geometry_json = Column(JSON)
    cutover_forecast_hash = Column(String(64))
    evidence_target_oid = Column(BigInteger, nullable=False)
    profile_target_oid = Column(BigInteger, nullable=False)
    evidence_rows = Column(BigInteger, nullable=False)
    profile_rows = Column(BigInteger, nullable=False)
    profile_as_of = Column(String(10), nullable=False)
    published_at = Column(TIMESTAMP(timezone=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryProfileDeltaReceipt(Base, JSONOutputMixin):
    """Replay-safe receipt for one committed source-vector delta."""

    __tablename__ = "provider_directory_profile_delta_receipt"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("build_id"),
        UniqueConstraint(
            "control_generation",
            "selection_proof_id",
            name="pd_profile_delta_receipt_control_proof_key",
        ),
        CheckConstraint(
            "build_id ~ '^pdpb_[0-9a-f]{32}$' "
            "AND executable_plan_hash ~ '^[0-9a-f]{64}$' "
            "AND ((from_capacity_geometry_status = 'legacy_unavailable' "
            "AND from_capacity_geometry_hash IS NULL "
            "AND from_capacity_geometry_json IS NULL) "
            "OR (from_capacity_geometry_status = 'verified' "
            "AND from_capacity_geometry_hash IS NOT NULL "
            "AND from_capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND from_capacity_geometry_json IS NOT NULL "
            "AND jsonb_typeof("
            "from_capacity_geometry_json::jsonb) = 'object')) "
            "AND capacity_geometry_status = 'verified' "
            "AND capacity_geometry_hash IS NOT NULL "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_json IS NOT NULL "
            "AND jsonb_typeof(capacity_geometry_json::jsonb) = 'object' "
            "AND from_source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND to_source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND from_source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND to_source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND from_generation_id ~ '^pdprofile_[0-9a-f]{32}$' "
            "AND generation_id ~ '^pdprofile_[0-9a-f]{32}$' "
            "AND operation IN ('publish', 'purge') "
            "AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
            "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
            "AND control_generation > 0 "
            "AND authority_revision > 0 "
            "AND evidence_target_oid > 0 "
            "AND profile_target_oid > 0 "
            "AND evidence_rows >= 0 "
            "AND profile_rows >= 0 "
            "AND evidence_inserted >= 0 "
            "AND evidence_deleted >= 0 "
            "AND profile_inserted >= 0 "
            "AND profile_deleted >= 0 "
            "AND cutover_forecast_hash ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(cutover_forecast_json::jsonb) = 'object' "
            "AND cutover_actual_hash ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(cutover_actual_json::jsonb) = 'object' "
            "AND cutover_wal_start_lsn IS NOT NULL "
            "AND cutover_wal_observed_lsn IS NOT NULL "
            "AND cutover_wal_bytes >= 0 "
            "AND evidence_target_bytes_before >= 0 "
            "AND evidence_target_bytes_after >= 0 "
            "AND evidence_target_growth_bytes >= 0 "
            "AND profile_target_bytes_before >= 0 "
            "AND profile_target_bytes_after >= 0 "
            "AND profile_target_growth_bytes >= 0",
            name="pd_profile_delta_receipt_values_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["build_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("to_source_vector_hash",),
            "name": "pd_profile_delta_receipt_vector_idx",
        }
    ]

    build_id = Column(String(64), nullable=False)
    executable_plan_hash = Column(String(64), nullable=False)
    from_capacity_geometry_status = Column(String(32), nullable=False)
    from_capacity_geometry_hash = Column(String(64))
    from_capacity_geometry_json = Column(JSON)
    capacity_geometry_status = Column(String(32), nullable=False)
    capacity_geometry_hash = Column(String(64), nullable=False)
    capacity_geometry_json = Column(JSON, nullable=False)
    from_source_vector_hash = Column(String(64), nullable=False)
    to_source_vector_hash = Column(String(64), nullable=False)
    from_source_context_vector_hash = Column(String(64), nullable=False)
    to_source_context_vector_hash = Column(String(64), nullable=False)
    from_generation_id = Column(String(64), nullable=False)
    generation_id = Column(String(64), nullable=False)
    operation = Column(String(16), nullable=False)
    profile_as_of = Column(String(10), nullable=False)
    selection_proof_id = Column(String(64), nullable=False)
    control_generation = Column(BigInteger, nullable=False)
    authority_revision = Column(BigInteger, nullable=False)
    evidence_target_oid = Column(BigInteger, nullable=False)
    profile_target_oid = Column(BigInteger, nullable=False)
    evidence_rows = Column(BigInteger, nullable=False)
    profile_rows = Column(BigInteger, nullable=False)
    evidence_inserted = Column(BigInteger, nullable=False)
    evidence_deleted = Column(BigInteger, nullable=False)
    profile_inserted = Column(BigInteger, nullable=False)
    profile_deleted = Column(BigInteger, nullable=False)
    cutover_forecast_hash = Column(String(64), nullable=False)
    cutover_forecast_json = Column(JSON, nullable=False)
    cutover_actual_hash = Column(String(64), nullable=False)
    cutover_actual_json = Column(JSON, nullable=False)
    cutover_wal_start_lsn = Column(String(64), nullable=False)
    cutover_wal_observed_lsn = Column(String(64), nullable=False)
    cutover_wal_bytes = Column(BigInteger, nullable=False)
    evidence_target_bytes_before = Column(BigInteger, nullable=False)
    evidence_target_bytes_after = Column(BigInteger, nullable=False)
    evidence_target_growth_bytes = Column(BigInteger, nullable=False)
    profile_target_bytes_before = Column(BigInteger, nullable=False)
    profile_target_bytes_after = Column(BigInteger, nullable=False)
    profile_target_growth_bytes = Column(BigInteger, nullable=False)
    committed_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryProfileCapacityLeaseConsumption(
    Base,
    JSONOutputMixin,
):
    """Immutable one-time use of a signed database-capacity lease."""

    __tablename__ = "provider_directory_profile_capacity_lease_consumption"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("attestation_id"),
        UniqueConstraint(
            "reservation_id",
            name="pd_profile_capacity_consumption_reservation_key",
        ),
        UniqueConstraint(
            "run_id",
            name="pd_profile_capacity_consumption_run_key",
        ),
        CheckConstraint(
            "attestation_id ~ '^[0-9a-f]{64}$' "
            "AND reservation_id ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' "
            "AND lease_digest ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND executable_plan_hash ~ '^[0-9a-f]{64}$' "
            "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
            "AND source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND run_id ~ '^run_[0-9a-f]{32}$' "
            "AND build_id ~ '^pdpb_[0-9a-f]{32}$' "
            "AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
            "AND contract_id IN ("
            "'provider-directory-database-capacity-lease-v1', "
            "'provider-directory-database-capacity-lease-v2', "
            "'provider-directory-database-capacity-lease-v3') "
            "AND key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND environment_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND attestor_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND attestor_release_digest ~ '^[0-9a-f]{64}$' "
            "AND public_key_fingerprint ~ '^[0-9a-f]{64}$' "
            "AND database_system_identifier ~ '^[1-9][0-9]{0,19}$' "
            "AND database_system_identifier::numeric "
            "<= 18446744073709551615 "
            "AND database_oid BETWEEN 1 AND 4294967295 "
            "AND database_name ~ "
            "'^[A-Za-z0-9_$][A-Za-z0-9_$.-]{0,62}$' "
            "AND tablespace_identity_hash ~ '^[0-9a-f]{64}$' "
            "AND volume_identity_hash ~ '^[0-9a-f]{64}$' "
            "AND signature ~ '^[A-Za-z0-9_-]{86}$' "
            "AND observed_at <= issued_at "
            "AND issued_at - observed_at <= interval '300 seconds' "
            "AND accepted_at + interval '5 seconds' >= issued_at "
            "AND accepted_at - observed_at <= interval '305 seconds' "
            "AND accepted_at < expires_at "
            "AND accepted_at < max_build_deadline "
            "AND recorded_at = accepted_at "
            "AND recorded_at < expires_at "
            "AND recorded_at < max_build_deadline "
            "AND issued_at < max_build_deadline "
            "AND max_build_deadline <= expires_at "
            "AND expires_at - issued_at <= interval '86400 seconds'",
            name="pd_profile_capacity_consumption_values_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["attestation_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("build_id",),
            "name": "pd_profile_capacity_consumption_build_idx",
        },
    ]

    attestation_id = Column(String(64), nullable=False)
    reservation_id = Column(String(128), nullable=False)
    lease_digest = Column(String(64), nullable=False)
    capacity_geometry_hash = Column(String(64), nullable=False)
    executable_plan_hash = Column(String(64), nullable=False)
    selection_proof_id = Column(String(64), nullable=False)
    source_vector_hash = Column(String(64), nullable=False)
    source_context_vector_hash = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    build_id = Column(String(64), nullable=False)
    profile_as_of = Column(String(10), nullable=False)
    contract_id = Column(String(64), nullable=False)
    key_id = Column(String(64), nullable=False)
    environment_id = Column(String(64), nullable=False)
    attestor_id = Column(String(64), nullable=False)
    attestor_release_digest = Column(String(64), nullable=False)
    public_key_fingerprint = Column(String(64), nullable=False)
    database_system_identifier = Column(String(20), nullable=False)
    database_oid = Column(BigInteger, nullable=False)
    database_name = Column(String(63), nullable=False)
    tablespace_identity_hash = Column(String(64), nullable=False)
    volume_identity_hash = Column(String(64), nullable=False)
    canonical_lease_json = Column(TEXT, nullable=False)
    signature = Column(String(86), nullable=False)
    observed_at = Column(TIMESTAMP(timezone=True), nullable=False)
    issued_at = Column(TIMESTAMP(timezone=True), nullable=False)
    accepted_at = Column(TIMESTAMP(timezone=True), nullable=False)
    expires_at = Column(TIMESTAMP(timezone=True), nullable=False)
    max_build_deadline = Column(TIMESTAMP(timezone=True), nullable=False)
    recorded_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )


_PROFILE_CAPACITY_CONTROL_PLANE_RECEIPT_COLUMN = (
    "control_plane_receipt_sha256"
)


class ProviderDirectoryProfileCapacityPreflightReceipt(
    Base,
    JSONOutputMixin,
):
    """One authenticated, replay-fenced Profile capacity signing receipt."""

    __tablename__ = "provider_directory_profile_capacity_preflight_receipt"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("receipt_sha256"),
        UniqueConstraint(
            "request_nonce",
            name="pd_profile_capacity_preflight_request_nonce_key",
        ),
        UniqueConstraint(
            "request_sha256",
            name="pd_profile_capacity_preflight_request_sha_key",
        ),
        CheckConstraint(
            "contract_id = "
            "'healthporta.provider-directory-profile-capacity-preflight.v3' "
            "AND request_contract_id = "
            "'healthporta.provider-directory-profile-capacity-preflight-request.v3' "
            "AND limits_contract_id = "
            "'healthporta.provider-directory-profile-capacity-limits.v2' "
            "AND materialization_mode = 'source_delta' "
            "AND profile_strategy_version = "
            "'source-fact-role32-org32-member32-dataset-graph8-auth-npi5m-v6' "
            "AND receipt_sha256 ~ '^[0-9a-f]{64}$' "
            "AND request_nonce ~ '^[0-9a-f]{64}$' "
            "AND request_sha256 ~ '^[0-9a-f]{64}$' "
            f"AND {_PROFILE_CAPACITY_CONTROL_PLANE_RECEIPT_COLUMN} "
            "~ '^[0-9a-f]{64}$' "
            "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
            "AND profile_input_digest ~ '^[0-9a-f]{64}$' "
            "AND limits_sha256 ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND serving_preflight_sha256 ~ '^[0-9a-f]{64}$' "
            "AND quiescence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND control_generation > 0 "
            "AND profile_schema_version > 0 "
            "AND issued_at < expires_at "
            "AND expires_at - issued_at <= interval '86400 seconds' "
            "AND jsonb_typeof(receipt_json::jsonb) = 'object' "
            "AND ((consumed_at IS NULL AND consumed_run_id IS NULL "
            "AND consumed_attestation_id IS NULL) "
            "OR (consumed_at IS NOT NULL "
            "AND consumed_at >= issued_at AND consumed_at < expires_at "
            "AND consumed_run_id ~ '^run_[0-9a-f]{32}$' "
            "AND consumed_attestation_id ~ '^[0-9a-f]{64}$'))",
            name="pd_profile_capacity_preflight_values_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["receipt_sha256"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("consumed_at", "expires_at"),
            "name": "pd_profile_capacity_preflight_open_idx",
        },
    ]

    receipt_sha256 = Column(String(64), nullable=False)
    request_nonce = Column(String(64), nullable=False)
    request_sha256 = Column(String(64), nullable=False)
    control_plane_receipt_sha256 = Column(
        _PROFILE_CAPACITY_CONTROL_PLANE_RECEIPT_COLUMN,
        String(64),
        nullable=False,
    )
    contract_id = Column(String(96), nullable=False)
    request_contract_id = Column(String(96), nullable=False)
    limits_contract_id = Column(String(96), nullable=False)
    selection_proof_id = Column(String(64), nullable=False)
    profile_input_digest = Column(String(64), nullable=False)
    control_generation = Column(BigInteger, nullable=False)
    profile_schema_version = Column(Integer, nullable=False)
    profile_strategy_version = Column(String(128), nullable=False)
    materialization_mode = Column(String(16), nullable=False)
    limits_sha256 = Column(String(64), nullable=False)
    capacity_geometry_hash = Column(String(64), nullable=False)
    serving_preflight_sha256 = Column(String(64), nullable=False)
    quiescence_sha256 = Column(String(64), nullable=False)
    receipt_json = Column(JSONB, nullable=False)
    issued_at = Column(TIMESTAMP(timezone=True), nullable=False)
    expires_at = Column(TIMESTAMP(timezone=True), nullable=False)
    consumed_at = Column(TIMESTAMP(timezone=True))
    consumed_run_id = Column(String(64))
    consumed_attestation_id = Column(String(64))
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )


class ProviderDirectoryProfileSelectionAuthority(Base, JSONOutputMixin):
    """Durable monotonic revision authority for global Profile proofs."""

    __tablename__ = "provider_directory_profile_selection_authority"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("authority_key"),
        CheckConstraint(
            "authority_key = 'global' AND last_revision >= 0",
            name="pd_profile_selection_authority_values_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["authority_key"]

    authority_key = Column(String(16), nullable=False)
    last_revision = Column(BigInteger, nullable=False, default=0)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryProfileSelectionProof(Base, JSONOutputMixin):
    """Stable identity for one exact global Profile input."""

    __tablename__ = "provider_directory_profile_selection_proof"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("input_identity_digest"),
        UniqueConstraint(
            "proof_id",
            name="provider_directory_profile_selection_proof_id_key",
        ),
        CheckConstraint(
            "input_identity_digest ~ '^[0-9a-f]{64}$' "
            "AND proof_id ~ '^[0-9a-f]{64}$'",
            name="pd_profile_selection_proof_identity_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["input_identity_digest"]

    input_identity_digest = Column(String(64), nullable=False)
    proof_id = Column(String(64), nullable=False)
    identity_json = Column(JSON, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryProfileSelectionObservation(Base, JSONOutputMixin):
    """Immutable monotonic observation of the global Profile input."""

    __tablename__ = "provider_directory_profile_selection_observation"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("authority_revision"),
        CheckConstraint(
            "authority_revision > 0",
            name="pd_profile_selection_observation_revision_check",
        ),
        {
            "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            "extend_existing": True,
        },
    )
    __my_index_elements__ = ["authority_revision"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("input_identity_digest",),
            "name": "pd_profile_selection_observation_input_idx",
        }
    ]

    authority_revision = Column(
        BigInteger,
        autoincrement=False,
        nullable=False,
    )
    input_identity_digest = Column(
        String(64),
        ForeignKey(
            ProviderDirectoryProfileSelectionProof.input_identity_digest,
            name="pd_profile_selection_observation_input_fkey",
        ),
        nullable=False,
    )
    payload_json = Column(JSON, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryReverseLookupCheckpoint(Base, JSONOutputMixin):
    """Completed source-specific reverse lookup seeds awaiting scan completion."""

    __tablename__ = "provider_directory_reverse_lookup_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("canonical_api_base", "seed_resource_type", "seed_resource_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["canonical_api_base", "seed_resource_type", "seed_resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("last_completed_run_id",),
            "name": "provider_directory_reverse_lookup_checkpoint_run_idx",
        },
    ]

    canonical_api_base = Column(TEXT, nullable=False)
    seed_resource_type = Column(String(64), nullable=False)
    seed_resource_id = Column(String(256), nullable=False)
    last_completed_run_id = Column(String(64))
    completed_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class PartDImportRun(Base, JSONOutputMixin):
    __tablename__ = "partd_import_run_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("status",), "name": "partd_import_run_v2_status_idx"},
        {"index_elements": ("started_at",), "name": "partd_import_run_v2_started_at_idx"},
    ]

    run_id = Column(String(64), nullable=False)
    import_id = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    source_summary = Column(JSON)
    error_text = Column(TEXT)


class PartDFormularySnapshot(Base, JSONOutputMixin):
    __tablename__ = "partd_formulary_snapshot_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["snapshot_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("run_id",), "name": "partd_formulary_snapshot_v2_run_idx"},
        {"index_elements": ("source_type", "release_date"), "name": "partd_formulary_snapshot_v2_source_release_idx"},
    ]

    snapshot_id = Column(String(128), nullable=False)
    run_id = Column(String(64), nullable=False)
    source_type = Column(String(16), nullable=False)
    source_url = Column(TEXT, nullable=False)
    artifact_name = Column(String(256))
    release_date = Column(DATE)
    cutoff_month = Column(DATE)
    status = Column(String(32), nullable=False)
    row_count_activity = Column(Integer, nullable=False, default=0)
    row_count_pricing = Column(Integer, nullable=False, default=0)
    imported_at = Column(TIMESTAMP)
    metadata_json = Column(JSON)
