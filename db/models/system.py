# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import (
    DATE,
    JSON,
    TEXT,
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = (
    "ImportHistory",
    "ImportLog",
    "ImportRun",
    "MRFCrawlRun",
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
    "ProviderDirectoryDatasetNetworkPlan",
    "ProviderDirectoryDatasetResource",
    "ProviderDirectoryEndpoint",
    "ProviderDirectoryEndpointDataset",
    "ProviderDirectoryHealthcareService",
    "ProviderDirectoryInsurancePlan",
    "ProviderDirectoryLocation",
    "ProviderDirectoryOrganization",
    "ProviderDirectoryOrganizationAffiliation",
    "ProviderDirectoryPaginationCheckpoint",
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
        {"index_elements": ("schedule_id",), "name": "import_run_schedule_idx"},
        {"index_elements": ("subscription_id",), "name": "import_run_subscription_idx"},
        {"index_elements": ("source_file_import_id",), "name": "import_run_source_file_import_idx"},
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
    publication_metadata_json = Column(JSON)


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
    active = Column(Boolean)
    identifiers = Column(JSON)
    name = Column(String(512))
    aliases = Column(JSON)
    type_codes = Column(JSON)
    telecom = Column(JSON)
    address_json = Column(JSON)
    contacts = Column(JSON)
    part_of_ref = Column(TEXT)
    endpoint_refs = Column(JSON)
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
    location_refs = Column(JSON)
    healthcare_service_refs = Column(JSON)
    endpoint_refs = Column(JSON)
    specialty_codes = Column(JSON)
    code_codes = Column(JSON)
    telecom = Column(JSON)
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
    attempt_count = Column(Integer, nullable=False, default=0)
    error = Column(TEXT)
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
    created_at = Column(TIMESTAMP, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=False)
    completed_at = Column(TIMESTAMP)


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
