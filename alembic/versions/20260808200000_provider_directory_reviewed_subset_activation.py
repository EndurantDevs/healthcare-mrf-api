# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guard explicit reviewed Provider Directory subset activation.

Revision ID: 20260808200000_provider_directory_reviewed_subset_activation
Revises: 20260808190000_provider_directory_subset_completion_proof
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260808200000_provider_directory_reviewed_subset_activation"
down_revision = "20260808190000_provider_directory_subset_completion_proof"
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = (
    "20260808190000_provider_directory_subset_completion_proof.py"
)
_ENDPOINT_DATASET = "provider_directory_endpoint_dataset"
_DATASET_RESOURCE = "provider_directory_dataset_resource"
_SOURCE = "provider_directory_source"
_ACTIVATION_KEY = "provider_directory_reviewed_subset_activation_v1"
_ACTIVATION_KEY_V2 = "provider_directory_reviewed_subset_activation_v2"
_PENDING_STATUS = "pending_two_matching_reviewed_subset_acquisitions"
_VERIFIED_STATUS = "verified_two_matching_reviewed_subset_acquisitions"
_POLICY_PENDING_STATUS = "pending_reviewed_subset_acquisition"
_POLICY_VERIFIED_STATUS = "verified_reviewed_subset_acquisition"
_ACTIVATION_CONTRACT = "provider-directory-reviewed-subset-activation-v1"
_ACTIVATION_CONTRACT_V2 = "provider-directory-reviewed-subset-activation-v2"
_SOURCE_CONTRACT = "provider-directory-fhir-reviewed-subset-source-contract-v1"
_SOURCE_CONTRACT_V2 = "provider-directory-fhir-reviewed-subset-source-contract-v2"
_ACTIVATION_VALID_FUNCTION = "provider_directory_reviewed_subset_activation_valid"
_SOURCE_GUARD_FUNCTION = "guard_provider_directory_reviewed_subset_activation_source"
_DATASET_GUARD_FUNCTION = "guard_provider_directory_reviewed_subset_activation_dataset"
_SOURCE_GUARD_TRIGGER = "provider_directory_reviewed_subset_activation_source_guard"
_SOURCE_TRUNCATE_TRIGGER = (
    "provider_directory_reviewed_subset_source_truncate_guard"
)
_DATASET_GUARD_TRIGGER = "provider_directory_reviewed_subset_activation_dataset_guard"
_DATASET_TRUNCATE_TRIGGER = (
    "provider_directory_reviewed_subset_dataset_truncate_guard"
)
_PROOF_BEARING_STATUSES = (
    "verification_baseline",
    "verification_mismatch",
    "validated",
    "published",
    "superseded",
)


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_subset_completion_predecessor",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory activation predecessor is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _schema() -> str:
    return _predecessor()._schema()


def _q(identifier: str) -> str:
    return _predecessor()._q(identifier)


def _ql(value: str) -> str:
    return _predecessor()._ql(value)


def _qf(schema: str, relation: str) -> str:
    return _predecessor()._qf(schema, relation)


def _source_contract_payload_sql(
    *,
    use_configured_endpoint_identity: bool = False,
    include_reviewed_root_policy: bool = False,
) -> str:
    previous = _predecessor()
    source_metadata = "active_source.metadata_json::jsonb"
    metadata_identity = previous._subset_source_metadata_identity_sql(
        source_metadata,
        include_reviewed_root_policy=include_reviewed_root_policy,
    )
    identity_version = (
        _SOURCE_CONTRACT_V2
        if include_reviewed_root_policy
        else _SOURCE_CONTRACT
    )
    endpoint_identity = (
        f"{source_metadata} ->> "
        "'provider_directory_configured_endpoint_id'"
        if use_configured_endpoint_identity
        else "active_source.endpoint_id"
    )
    return f"""
        pg_catalog.jsonb_build_object(
            'identity_version', {_ql(identity_version)},
            'source', pg_catalog.jsonb_build_object(
                'source_id', active_source.source_id,
                'endpoint_id', {endpoint_identity},
                'canonical_api_base', active_source.canonical_api_base,
                'requires_registration', active_source.requires_registration,
                'requires_api_key', active_source.requires_api_key,
                'auth_type', active_source.auth_type
            ),
            'metadata_identity', {metadata_identity}
        )
    """


def _activation_marker_shape_sql(marker: str) -> str:
    top_fields = (
        "contract_version",
        "source_contract_sha256",
        "cutoff",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
        "source_id",
        "endpoint_id",
        "verification_campaign_id",
        "baseline",
        "candidate",
    )
    root_fields = (
        "dataset_id",
        "acquisition_root_run_id",
        "replay_evidence_sha256",
        "coverage_sha256",
    )
    top_fields_sql = ", ".join(_ql(field_name) for field_name in top_fields)
    root_fields_sql = ", ".join(_ql(field_name) for field_name in root_fields)
    digest_fields = (
        "source_contract_sha256",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
    )
    digest_sql = "\n        AND ".join(
        f"{marker} ->> {_ql(field_name)} ~ '^[0-9a-f]{{64}}$'"
        for field_name in digest_fields
    )
    root_shape_sql = "\n        AND ".join(
        f"""pg_catalog.jsonb_typeof({marker} -> {_ql(root_name)}) = 'object'
        AND ({marker} -> {_ql(root_name)}) ?&
            ARRAY[{root_fields_sql}]::text[]
        AND ({marker} -> {_ql(root_name)}) -
            ARRAY[{root_fields_sql}]::text[] = '{{}}'::jsonb
        AND {marker} -> {_ql(root_name)} ->> 'dataset_id' <> ''
        AND {marker} -> {_ql(root_name)} ->>
                'acquisition_root_run_id' <> ''
        AND {marker} -> {_ql(root_name)} ->>
                'replay_evidence_sha256' ~ '^[0-9a-f]{{64}}$'
        AND {marker} -> {_ql(root_name)} ->>
                'coverage_sha256' ~ '^[0-9a-f]{{64}}$'"""
        for root_name in ("baseline", "candidate")
    )
    return f"""
        pg_catalog.jsonb_typeof({marker}) = 'object'
        AND {marker} ?& ARRAY[{top_fields_sql}]::text[]
        AND {marker} - ARRAY[{top_fields_sql}]::text[] = '{{}}'::jsonb
        AND {marker} ->> 'contract_version' = {_ql(_ACTIVATION_CONTRACT)}
        AND {digest_sql}
        AND {marker} ->> 'cutoff' ~
            '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:'
            '[0-9]{{2}}:[0-9]{{2}}\\.[0-9]{{6}}Z$'
        AND {marker} ->> 'source_id' <> ''
        AND {marker} ->> 'endpoint_id' <> ''
        AND {marker} ->> 'verification_campaign_id' <> ''
        AND {root_shape_sql}
    """


def _activation_marker_v2_shape_sql(marker: str) -> str:
    """Validate the closed count-one or count-two activation marker."""

    common_fields = (
        "contract_version",
        "source_contract_sha256",
        "cutoff",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
        "source_id",
        "endpoint_id",
        "verification_campaign_id",
        "candidate",
        "root_policy",
    )
    root_fields = (
        "dataset_id",
        "acquisition_root_run_id",
        "replay_evidence_sha256",
        "coverage_sha256",
    )
    common_fields_sql = ", ".join(_ql(field_name) for field_name in common_fields)
    count_two_fields_sql = common_fields_sql + ", " + _ql("baseline")
    root_fields_sql = ", ".join(_ql(field_name) for field_name in root_fields)
    digest_fields = (
        "source_contract_sha256",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
    )
    string_fields = (
        "contract_version",
        "source_contract_sha256",
        "cutoff",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
        "source_id",
        "endpoint_id",
        "verification_campaign_id",
    )
    string_type_sql = "\n        AND ".join(
        f"pg_catalog.jsonb_typeof({marker} -> {_ql(field_name)}) = 'string'"
        for field_name in string_fields
    )
    digest_sql = "\n        AND ".join(
        f"{marker} ->> {_ql(field_name)} ~ '^[0-9a-f]{{64}}$'"
        for field_name in digest_fields
    )
    candidate = f"({marker} -> 'candidate')"
    baseline = f"({marker} -> 'baseline')"
    root_shape_sql = f"""
        pg_catalog.jsonb_typeof({candidate}) = 'object'
        AND {candidate} ?& ARRAY[{root_fields_sql}]::text[]
        AND {candidate} - ARRAY[{root_fields_sql}]::text[] = '{{}}'::jsonb
        AND NOT EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_each({candidate}) AS field(name, value)
             WHERE pg_catalog.jsonb_typeof(field.value) <> 'string'
        )
        AND {candidate} ->> 'dataset_id' <> ''
        AND {candidate} ->> 'acquisition_root_run_id' <> ''
        AND {candidate} ->> 'replay_evidence_sha256' ~ '^[0-9a-f]{{64}}$'
        AND {candidate} ->> 'coverage_sha256' ~ '^[0-9a-f]{{64}}$'
    """
    baseline_shape_sql = f"""
        pg_catalog.jsonb_typeof({baseline}) = 'object'
        AND {baseline} ?& ARRAY[{root_fields_sql}]::text[]
        AND {baseline} - ARRAY[{root_fields_sql}]::text[] = '{{}}'::jsonb
        AND NOT EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_each({baseline}) AS field(name, value)
             WHERE pg_catalog.jsonb_typeof(field.value) <> 'string'
        )
        AND {baseline} ->> 'dataset_id' <> ''
        AND {baseline} ->> 'acquisition_root_run_id' <> ''
        AND {baseline} ->> 'replay_evidence_sha256' ~ '^[0-9a-f]{{64}}$'
        AND {baseline} ->> 'coverage_sha256' ~ '^[0-9a-f]{{64}}$'
    """
    root_policy = f"({marker} -> 'root_policy')"
    root_policy_one_sql = f"""
        {root_policy} = pg_catalog.jsonb_build_object(
            'policy_version',
                {_ql(_predecessor()._REVIEWED_ROOT_POLICY_VERSION)},
            'required_root_count', 1
        )
    """
    root_policy_two_sql = root_policy_one_sql.replace(
        "'required_root_count', 1",
        "'required_root_count', 2",
    )
    return f"""
        pg_catalog.jsonb_typeof({marker}) = 'object'
        AND {string_type_sql}
        AND {marker} ->> 'contract_version' = {_ql(_ACTIVATION_CONTRACT_V2)}
        AND {digest_sql}
        AND {marker} ->> 'cutoff' ~
            '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:'
            '[0-9]{{2}}:[0-9]{{2}}\\.[0-9]{{6}}Z$'
        AND {marker} ->> 'source_id' <> ''
        AND {marker} ->> 'endpoint_id' <> ''
        AND {marker} ->> 'verification_campaign_id' <> ''
        AND (
            (
                ({root_policy_one_sql})
                AND {marker} ?& ARRAY[{common_fields_sql}]::text[]
                AND {marker} - ARRAY[{common_fields_sql}]::text[] = '{{}}'::jsonb
                AND ({root_shape_sql})
            ) OR (
                ({root_policy_two_sql})
                AND {marker} ?& ARRAY[{count_two_fields_sql}]::text[]
                AND {marker} - ARRAY[{count_two_fields_sql}]::text[] = '{{}}'::jsonb
                AND ({root_shape_sql})
                AND ({baseline_shape_sql})
            )
        )
    """


def _activation_matched_twin_sql(
    schema: str,
    *,
    reviewed_root_policy_aware: bool = False,
    dataset_alias: str = "candidate",
) -> str:
    """Extend the predecessor match proof to retained superseded candidates."""

    matched_twin_sql = _predecessor()._subset_matched_twin_sql(
        schema,
        reviewed_root_policy_aware=reviewed_root_policy_aware,
    ).replace(
        "NEW.",
        f"{dataset_alias}.",
    )
    active_lifecycle_sql = f"""
        AND (
            (
                {dataset_alias}.status = 'validated'
                AND {dataset_alias}.is_current IS FALSE
                AND {dataset_alias}.validated_at IS NOT NULL
                AND {dataset_alias}.published_at IS NULL
                AND {dataset_alias}.superseded_at IS NULL
            ) OR (
                {dataset_alias}.status = 'published'
                AND {dataset_alias}.is_current IS TRUE
                AND {dataset_alias}.validated_at IS NOT NULL
                AND {dataset_alias}.published_at IS NOT NULL
                AND {dataset_alias}.superseded_at IS NULL
            )
        )
    """
    retained_lifecycle_sql = f"""
        AND (
            (
                {dataset_alias}.status = 'validated'
                AND {dataset_alias}.is_current IS FALSE
                AND {dataset_alias}.validated_at IS NOT NULL
                AND {dataset_alias}.published_at IS NULL
                AND {dataset_alias}.superseded_at IS NULL
            ) OR (
                {dataset_alias}.status = 'published'
                AND {dataset_alias}.is_current IS TRUE
                AND {dataset_alias}.validated_at IS NOT NULL
                AND {dataset_alias}.published_at IS NOT NULL
                AND {dataset_alias}.superseded_at IS NULL
            ) OR (
                {dataset_alias}.status = 'superseded'
                AND {dataset_alias}.is_current IS FALSE
                AND {dataset_alias}.validated_at IS NOT NULL
                AND {dataset_alias}.published_at IS NOT NULL
                AND {dataset_alias}.superseded_at IS NOT NULL
            )
        )
    """
    if matched_twin_sql.count(active_lifecycle_sql) != 1:
        raise RuntimeError(
            "provider directory activation predecessor lifecycle changed"
        )
    return matched_twin_sql.replace(
        active_lifecycle_sql,
        retained_lifecycle_sql,
    )


def _activation_valid_function_sql(
    schema: str,
    *,
    use_configured_endpoint_identity: bool = False,
    replace_existing: bool = False,
    reviewed_root_policy_aware: bool = False,
    reviewed_subset_profile_aware: bool = False,
    reviewed_subset_terminal_window_profile_aware: bool = False,
) -> str:
    previous = _predecessor()
    source_ref = _qf(schema, _SOURCE)
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    function_ref = _qf(schema, _ACTIVATION_VALID_FUNCTION)
    canonical_sha256_ref = _qf(schema, previous._CANONICAL_SHA256_FUNCTION)
    marker = f"(active_source.metadata_json::jsonb -> {_ql(_ACTIVATION_KEY)})"
    baseline_metadata = "baseline.publication_metadata_json::jsonb"
    candidate_metadata = "candidate.publication_metadata_json::jsonb"
    matched_twin_sql = _activation_matched_twin_sql(
        schema,
        reviewed_root_policy_aware=reviewed_root_policy_aware,
    )
    source_valid_sql = previous._subset_source_sql(
        schema,
        require_verified=True,
        dataset_alias="candidate",
        use_configured_endpoint_identity=use_configured_endpoint_identity,
        require_physical_match=not use_configured_endpoint_identity,
        reviewed_root_policy_aware=reviewed_root_policy_aware,
        reviewed_subset_profile_aware=reviewed_subset_profile_aware,
        reviewed_subset_terminal_window_profile_aware=(
            reviewed_subset_terminal_window_profile_aware
        ),
    )
    source_contract_payload = _source_contract_payload_sql(
        use_configured_endpoint_identity=use_configured_endpoint_identity,
    )
    policy_marker = (
        f"(active_source.metadata_json::jsonb -> {_ql(_ACTIVATION_KEY_V2)})"
    )
    policy_candidate_metadata = (
        "policy_candidate.publication_metadata_json::jsonb"
    )
    policy_baseline_metadata = (
        "policy_baseline.publication_metadata_json::jsonb"
    )
    policy_source_contract_payload = _source_contract_payload_sql(
        use_configured_endpoint_identity=use_configured_endpoint_identity,
        include_reviewed_root_policy=True,
    )
    policy_source_valid_sql = previous._subset_source_sql(
        schema,
        require_verified=True,
        dataset_alias="policy_candidate",
        use_configured_endpoint_identity=use_configured_endpoint_identity,
        require_physical_match=not use_configured_endpoint_identity,
        reviewed_root_policy_aware=True,
        reviewed_subset_profile_aware=reviewed_subset_profile_aware,
        reviewed_subset_terminal_window_profile_aware=(
            reviewed_subset_terminal_window_profile_aware
        ),
    )
    policy_matched_twin_sql = _activation_matched_twin_sql(
        schema,
        reviewed_root_policy_aware=True,
        dataset_alias="policy_candidate",
    )
    policy_single_root_sql = previous._subset_single_root_sql(
        schema,
        dataset_alias="policy_candidate",
    )
    policy_marker_one_sql = f"""
        {policy_marker} -> 'root_policy' = pg_catalog.jsonb_build_object(
            'policy_version', {_ql(previous._REVIEWED_ROOT_POLICY_VERSION)},
            'required_root_count', 1
        )
    """
    policy_marker_two_sql = policy_marker_one_sql.replace(
        "'required_root_count', 1",
        "'required_root_count', 2",
    )
    marker_endpoint_identity = (
        "active_source.metadata_json::jsonb "
        "->> 'provider_directory_configured_endpoint_id'"
        if use_configured_endpoint_identity
        else "active_source.endpoint_id"
    )
    create_function = (
        "CREATE OR REPLACE FUNCTION"
        if replace_existing
        else "CREATE FUNCTION"
    )
    proof_statuses = ", ".join(_ql(status) for status in _PROOF_BEARING_STATUSES)
    policy_ctes_sql = ""
    policy_activation_sql = ""
    if reviewed_root_policy_aware:
        policy_ctes_sql = f"""
        , policy_candidate AS MATERIALIZED (
            SELECT dataset.*
              FROM {dataset_ref} AS dataset
              JOIN active_source
                ON dataset.dataset_id =
                   {policy_marker} -> 'candidate' ->> 'dataset_id'
        ), policy_baseline AS MATERIALIZED (
            SELECT dataset.*
              FROM {dataset_ref} AS dataset
              JOIN active_source
                ON dataset.dataset_id =
                   {policy_marker} -> 'baseline' ->> 'dataset_id'
        )
        """
        policy_activation_sql = f"""
        OR COALESCE((
            SELECT (
                active_source.metadata_json::jsonb
                    ->> 'provider_directory_candidate_status' =
                    {_ql(_POLICY_VERIFIED_STATUS)}
                AND active_source.metadata_json::jsonb
                    ? {_ql(_ACTIVATION_KEY_V2)}
                AND NOT (active_source.metadata_json::jsonb
                    ? {_ql(_ACTIVATION_KEY)})
                AND ({_activation_marker_v2_shape_sql(policy_marker)})
                AND active_source.metadata_json::jsonb
                        -> {_ql(previous._REVIEWED_ROOT_POLICY_KEY)} =
                    {policy_marker} -> 'root_policy'
                AND {policy_candidate_metadata}
                        -> {_ql(previous._REVIEWED_ROOT_POLICY_KEY)} =
                    {policy_marker} -> 'root_policy'
                AND {policy_marker} ->> 'source_id' = active_source.source_id
                AND {policy_marker} ->> 'endpoint_id' =
                    {marker_endpoint_identity}
                AND {policy_marker} ->> 'verification_campaign_id' =
                    policy_candidate.completion_proof_json ->> 'campaign_id'
                AND {policy_marker} ->> 'cutoff' =
                    policy_candidate.completion_proof_json ->> 'cutoff'
                AND {policy_marker} ->> 'verification_source_scope_sha256' =
                    {policy_candidate_metadata} ->>
                    {_ql(previous._TWIN_SCOPE_KEY)}
                AND {policy_marker} ->> 'completion_proof_sha256' =
                    policy_candidate.completion_proof_sha256
                AND {policy_marker} ->> 'source_contract_sha256' =
                    {canonical_sha256_ref}({policy_source_contract_payload})
                AND {policy_marker} -> 'candidate' ->> 'dataset_id' =
                    policy_candidate.dataset_id
                AND {policy_marker} -> 'candidate' ->>
                        'acquisition_root_run_id' =
                    policy_candidate.acquisition_root_run_id
                AND {policy_marker} -> 'candidate' ->>
                        'replay_evidence_sha256' =
                    {policy_candidate_metadata} ->>
                    {_ql(previous._REPLAY_EVIDENCE_SHA256_KEY)}
                AND {policy_marker} -> 'candidate' ->> 'coverage_sha256' =
                    {canonical_sha256_ref}(
                        {policy_candidate_metadata} ->
                        {_ql(previous._SUBSET_COVERAGE_KEY)}
                    )
                AND (
                    (
                        ({policy_marker_one_sql})
                        AND ({policy_single_root_sql})
                    ) OR (
                        ({policy_marker_two_sql})
                        AND policy_baseline.dataset_id IS NOT NULL
                        AND {policy_marker} -> 'baseline' ->> 'dataset_id' =
                            policy_baseline.dataset_id
                        AND {policy_marker} -> 'baseline' ->>
                                'acquisition_root_run_id' =
                            policy_baseline.acquisition_root_run_id
                        AND {policy_marker} -> 'baseline' ->>
                                'replay_evidence_sha256' =
                            {policy_baseline_metadata} ->>
                            {_ql(previous._REPLAY_EVIDENCE_SHA256_KEY)}
                        AND {policy_marker} -> 'baseline' ->> 'coverage_sha256' =
                            {canonical_sha256_ref}(
                                {policy_baseline_metadata} ->
                                {_ql(previous._SUBSET_COVERAGE_KEY)}
                            )
                        AND ({policy_matched_twin_sql})
                    )
                )
                AND ({policy_source_valid_sql})
                AND (
                    SELECT pg_catalog.count(*)
                      FROM {dataset_ref} AS generation
                     WHERE generation.endpoint_id = policy_candidate.endpoint_id
                       AND generation.completion_proof_required_version = 3
                       AND generation.status IN ({proof_statuses})
                       AND generation.publication_metadata_json::jsonb
                            ->> {_ql(previous._TWIN_CAMPAIGN_KEY)} =
                            {policy_candidate_metadata} ->>
                            {_ql(previous._TWIN_CAMPAIGN_KEY)}
                       AND generation.publication_metadata_json::jsonb
                            ->> {_ql(previous._TWIN_SCOPE_KEY)} =
                            {policy_candidate_metadata} ->>
                            {_ql(previous._TWIN_SCOPE_KEY)}
                ) = ({policy_marker} -> 'root_policy'
                        ->> 'required_root_count')::integer
            )
              FROM active_source
              JOIN policy_candidate ON true
              LEFT JOIN policy_baseline ON true
        ), false)
        """
    return f"""
    {create_function} {function_ref}(candidate_source_id text)
    RETURNS boolean
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        WITH active_source AS MATERIALIZED (
            SELECT source.*
              FROM {source_ref} AS source
             WHERE source.source_id = candidate_source_id
        ), candidate AS MATERIALIZED (
            SELECT dataset.*
              FROM {dataset_ref} AS dataset
              JOIN active_source
                ON dataset.dataset_id =
                   {marker} -> 'candidate' ->> 'dataset_id'
        ), baseline AS MATERIALIZED (
            SELECT dataset.*
              FROM {dataset_ref} AS dataset
              JOIN active_source
                ON dataset.dataset_id =
                   {marker} -> 'baseline' ->> 'dataset_id'
        )
        {policy_ctes_sql}
        SELECT COALESCE((
            SELECT (
                active_source.metadata_json::jsonb
                    ->> 'provider_directory_candidate_status' =
                    {_ql(_VERIFIED_STATUS)}
                AND NOT (active_source.metadata_json::jsonb
                    ? {_ql(_ACTIVATION_KEY_V2)})
                AND ({_activation_marker_shape_sql(marker)})
                AND {marker} ->> 'source_id' = active_source.source_id
                AND {marker} ->> 'endpoint_id' = {marker_endpoint_identity}
                AND {marker} ->> 'verification_campaign_id' =
                    candidate.completion_proof_json ->> 'campaign_id'
                AND {marker} ->> 'cutoff' =
                    candidate.completion_proof_json ->> 'cutoff'
                AND {marker} ->> 'verification_source_scope_sha256' =
                    {candidate_metadata} ->>
                    {_ql(previous._TWIN_SCOPE_KEY)}
                AND {marker} ->> 'completion_proof_sha256' =
                    candidate.completion_proof_sha256
                AND {marker} ->> 'source_contract_sha256' =
                    {canonical_sha256_ref}({source_contract_payload})
                AND {marker} -> 'baseline' ->> 'dataset_id' =
                    baseline.dataset_id
                AND {marker} -> 'baseline' ->>
                        'acquisition_root_run_id' =
                    baseline.acquisition_root_run_id
                AND {marker} -> 'candidate' ->> 'dataset_id' =
                    candidate.dataset_id
                AND {marker} -> 'candidate' ->>
                        'acquisition_root_run_id' =
                    candidate.acquisition_root_run_id
                AND {marker} -> 'baseline' ->>
                        'replay_evidence_sha256' =
                    {baseline_metadata} ->>
                    {_ql(previous._REPLAY_EVIDENCE_SHA256_KEY)}
                AND {marker} -> 'candidate' ->>
                        'replay_evidence_sha256' =
                    {candidate_metadata} ->>
                    {_ql(previous._REPLAY_EVIDENCE_SHA256_KEY)}
                AND {marker} -> 'baseline' ->> 'coverage_sha256' =
                    {canonical_sha256_ref}(
                        {baseline_metadata} ->
                        {_ql(previous._SUBSET_COVERAGE_KEY)}
                    )
                AND {marker} -> 'candidate' ->> 'coverage_sha256' =
                    {canonical_sha256_ref}(
                        {candidate_metadata} ->
                        {_ql(previous._SUBSET_COVERAGE_KEY)}
                    )
                AND ({matched_twin_sql})
                AND ({source_valid_sql})
                AND (
                    SELECT pg_catalog.count(*)
                      FROM {dataset_ref} AS generation
                     WHERE generation.endpoint_id = candidate.endpoint_id
                       AND generation.completion_proof_required_version = 3
                       AND generation.status IN ({proof_statuses})
                       AND generation.publication_metadata_json::jsonb
                            ->> {_ql(previous._TWIN_CAMPAIGN_KEY)} =
                            {candidate_metadata} ->>
                            {_ql(previous._TWIN_CAMPAIGN_KEY)}
                       AND generation.publication_metadata_json::jsonb
                            ->> {_ql(previous._TWIN_SCOPE_KEY)} =
                            {candidate_metadata} ->>
                            {_ql(previous._TWIN_SCOPE_KEY)}
                ) = 2
            )
              FROM active_source
              JOIN candidate ON true
              JOIN baseline ON true
        ), false)
        {policy_activation_sql};
    $function$;
    """


def _source_guard_function_sql(
    schema: str,
    *,
    allow_effective_endpoint_cutover: bool = False,
    replace_existing: bool = False,
    reviewed_root_policy_aware: bool = False,
) -> str:
    source_ref = _qf(schema, _SOURCE)
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    guard_ref = _qf(schema, _SOURCE_GUARD_FUNCTION)
    valid_ref = _qf(schema, _ACTIVATION_VALID_FUNCTION)
    key = _ql(_ACTIVATION_KEY)
    policy_key = _ql(_ACTIVATION_KEY_V2)
    pending = _ql(_PENDING_STATUS)
    verified = _ql(_VERIFIED_STATUS)
    policy_pending = _ql(_POLICY_PENDING_STATUS)
    policy_verified = _ql(_POLICY_VERIFIED_STATUS)

    def _active_endpoint_transition_sql(marker_key: str) -> str:
        if not allow_effective_endpoint_cutover:
            return ""
        return f"""
                AND (
                    NEW.endpoint_id IS NOT DISTINCT FROM OLD.endpoint_id
                    OR (
                        NEW.endpoint_id IS DISTINCT FROM OLD.endpoint_id
                        AND pg_catalog.to_jsonb(NEW)
                                - ARRAY['endpoint_id', 'updated_at']::text[]
                            = pg_catalog.to_jsonb(OLD)
                                - ARRAY['endpoint_id', 'updated_at']::text[]
                        AND NEW.updated_at IS NOT DISTINCT FROM
                            pg_catalog.transaction_timestamp()
                        AND NEW.endpoint_id = new_metadata
                                ->> 'provider_directory_configured_endpoint_id'
                        AND NEW.endpoint_id = new_metadata -> {marker_key}
                                ->> 'endpoint_id'
                        AND EXISTS (
                            SELECT 1
                              FROM {dataset_ref} AS activation_candidate
                             WHERE activation_candidate.dataset_id =
                                    new_metadata -> {marker_key}
                                        -> 'candidate' ->> 'dataset_id'
                               AND activation_candidate.endpoint_id =
                                    NEW.endpoint_id
                               AND activation_candidate.completion_proof_required_version
                                    = 3
                               AND activation_candidate.status = 'published'
                               AND activation_candidate.is_current IS TRUE
                               AND activation_candidate.validated_at IS NOT NULL
                               AND activation_candidate.published_at IS NOT NULL
                               AND activation_candidate.superseded_at IS NULL
                        )
                    )
                )
        """
    active_endpoint_transition_sql = _active_endpoint_transition_sql(key)
    policy_active_endpoint_transition_sql = _active_endpoint_transition_sql(
        policy_key
    )
    policy_truncate_sql = (
        f"""
                    OR active_source.metadata_json::jsonb ? {policy_key}
                    OR active_source.metadata_json::jsonb
                         ->> 'provider_directory_candidate_status' =
                         {policy_verified}
        """
        if reviewed_root_policy_aware
        else ""
    )
    policy_old_active_sql = (
        f"""
                OR old_metadata ? {policy_key}
                OR old_metadata ->> 'provider_directory_candidate_status' =
                   {policy_verified}
        """
        if reviewed_root_policy_aware
        else ""
    )
    policy_new_active_sql = (
        f"""
                OR new_metadata ? {policy_key}
                OR new_metadata ->> 'provider_directory_candidate_status' =
                   {policy_verified}
        """
        if reviewed_root_policy_aware
        else ""
    )
    policy_transition_sql = (
        f"""
                OR (
                    old_metadata ->> 'provider_directory_candidate_status' =
                        {policy_pending}
                    AND NOT (old_metadata ?| ARRAY[{key}, {policy_key}]::text[])
                    AND new_metadata ->> 'provider_directory_candidate_status' =
                        {policy_verified}
                    AND pg_catalog.jsonb_typeof(new_metadata -> {policy_key}) =
                        'object'
                    AND new_metadata -> {policy_key} -> 'root_policy' =
                        new_metadata ->
                        {_ql(_predecessor()._REVIEWED_ROOT_POLICY_KEY)}
                    AND pg_catalog.to_jsonb(NEW)
                            - ARRAY['metadata_json', 'updated_at']::text[]
                        = pg_catalog.to_jsonb(OLD)
                            - ARRAY['metadata_json', 'updated_at']::text[]
                    AND new_metadata
                            - ARRAY[
                                'provider_directory_candidate_status',
                                {_ql(_ACTIVATION_KEY_V2)}
                              ]::text[]
                        = old_metadata
                            - ARRAY[
                                'provider_directory_candidate_status',
                                {_ql(_ACTIVATION_KEY_V2)}
                              ]::text[]
                    AND NEW.updated_at IS NOT DISTINCT FROM
                        pg_catalog.transaction_timestamp()
                    AND {valid_ref}(NEW.source_id) IS TRUE
                )
        """
        if reviewed_root_policy_aware
        else ""
    )
    policy_replay_sql = (
        f"""
                OR (
                    old_metadata ->> 'provider_directory_candidate_status' =
                        {policy_verified}
                    AND new_metadata ->> 'provider_directory_candidate_status' =
                        {policy_verified}
                    AND pg_catalog.jsonb_typeof(old_metadata -> {policy_key}) =
                        'object'
                    AND NOT (old_metadata ? {key})
                    AND NOT (new_metadata ? {key})
                    AND new_metadata -> {policy_key} =
                        old_metadata -> {policy_key}
                    {policy_active_endpoint_transition_sql}
                    AND {valid_ref}(NEW.source_id) IS TRUE
                )
        """
        if reviewed_root_policy_aware
        else ""
    )
    create_function = (
        "CREATE OR REPLACE FUNCTION"
        if replace_existing
        else "CREATE FUNCTION"
    )
    return f"""
    {create_function} {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        old_metadata jsonb;
        new_metadata jsonb;
        old_active boolean := false;
        new_active boolean := false;
        affected_source_ids text[] := ARRAY[]::text[];
        affected_endpoint_ids text[] := ARRAY[]::text[];
    BEGIN
        IF pg_catalog.current_setting('transaction_isolation') <>
                'read committed' THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_isolation_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
                SELECT 1
                  FROM {source_ref} AS active_source
                 WHERE active_source.metadata_json::jsonb ? {key}
                    OR active_source.metadata_json::jsonb
                         ->> 'provider_directory_candidate_status' = {verified}
                    {policy_truncate_sql}
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_reviewed_subset_activation_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;

        IF TG_OP IN ('UPDATE', 'DELETE') THEN
            old_metadata := OLD.metadata_json::jsonb;
            old_active := COALESCE(old_metadata ? {key}, false)
                OR old_metadata ->> 'provider_directory_candidate_status' =
                   {verified}
                {policy_old_active_sql};
            affected_source_ids := pg_catalog.array_append(
                affected_source_ids,
                OLD.source_id
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids,
                OLD.endpoint_id
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids,
                old_metadata ->> 'provider_directory_configured_endpoint_id'
            );
        END IF;
        IF TG_OP IN ('INSERT', 'UPDATE') THEN
            new_metadata := NEW.metadata_json::jsonb;
            new_active := COALESCE(new_metadata ? {key}, false)
                OR new_metadata ->> 'provider_directory_candidate_status' =
                   {verified}
                {policy_new_active_sql};
            affected_source_ids := pg_catalog.array_append(
                affected_source_ids,
                NEW.source_id
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids,
                NEW.endpoint_id
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids,
                new_metadata ->> 'provider_directory_configured_endpoint_id'
            );
        END IF;
        affected_source_ids := pg_catalog.array_remove(
            affected_source_ids,
            NULL
        );
        affected_endpoint_ids := pg_catalog.array_remove(
            affected_endpoint_ids,
            NULL
        );

        IF TG_OP = 'UPDATE' AND NOT old_active AND new_active THEN
            LOCK TABLE {source_ref} IN SHARE MODE;
        END IF;

        IF TG_OP = 'INSERT' AND new_active THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_insert_invalid'
                USING ERRCODE = '55000';
        ELSIF TG_OP = 'DELETE' AND old_active THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_delete_forbidden'
                USING ERRCODE = '55000';
        ELSIF TG_OP = 'UPDATE' AND (old_active OR new_active) THEN
            IF (
                old_metadata ->> 'provider_directory_candidate_status' =
                    {pending}
                AND NOT (old_metadata ?| ARRAY[{key}, {policy_key}]::text[])
                AND new_metadata ->> 'provider_directory_candidate_status' =
                    {verified}
                AND pg_catalog.jsonb_typeof(new_metadata -> {key}) = 'object'
                AND NOT (new_metadata ? {policy_key})
                AND pg_catalog.to_jsonb(NEW)
                        - ARRAY['metadata_json', 'updated_at']::text[]
                    = pg_catalog.to_jsonb(OLD)
                        - ARRAY['metadata_json', 'updated_at']::text[]
                AND new_metadata
                        - ARRAY[
                            'provider_directory_candidate_status',
                            {_ql(_ACTIVATION_KEY)}
                          ]::text[]
                    = old_metadata
                        - ARRAY[
                            'provider_directory_candidate_status',
                            {_ql(_ACTIVATION_KEY)}
                          ]::text[]
                AND NEW.updated_at IS NOT DISTINCT FROM
                    pg_catalog.transaction_timestamp()
                AND {valid_ref}(NEW.source_id) IS TRUE
            )
                {policy_transition_sql}
            THEN
                NULL;
            ELSIF (
                old_metadata ->> 'provider_directory_candidate_status' =
                    {verified}
                AND new_metadata ->> 'provider_directory_candidate_status' =
                    {verified}
                AND pg_catalog.jsonb_typeof(old_metadata -> {key}) = 'object'
                AND new_metadata -> {key} = old_metadata -> {key}
                AND NOT (old_metadata ? {policy_key})
                AND NOT (new_metadata ? {policy_key})
                {active_endpoint_transition_sql}
                AND {valid_ref}(NEW.source_id) IS TRUE
            )
                {policy_replay_sql}
            THEN
                NULL;
            ELSE
                RAISE EXCEPTION
                    'provider_directory_reviewed_subset_activation_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
        END IF;

        IF EXISTS (
            WITH affected_active_sources AS MATERIALIZED (
                SELECT active_source.source_id
                  FROM {source_ref} AS active_source
                 WHERE (
                        active_source.metadata_json::jsonb ? {key}
                        OR active_source.metadata_json::jsonb
                             ->> 'provider_directory_candidate_status' =
                             {verified}
                        {policy_truncate_sql}
                   )
                   AND (
                        active_source.source_id = ANY(affected_source_ids)
                        OR active_source.endpoint_id =
                           ANY(affected_endpoint_ids)
                        OR active_source.metadata_json::jsonb
                             ->> 'provider_directory_configured_endpoint_id' =
                           ANY(affected_endpoint_ids)
                   )
            )
            SELECT 1
              FROM affected_active_sources AS affected_source
             WHERE {valid_ref}(affected_source.source_id)
                   IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_source_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NULL;
    END;
    $function$;
    """


def _dataset_guard_function_sql(
    schema: str,
    *,
    replace_existing: bool = False,
    reviewed_root_policy_aware: bool = False,
) -> str:
    source_ref = _qf(schema, _SOURCE)
    guard_ref = _qf(schema, _DATASET_GUARD_FUNCTION)
    key = _ql(_ACTIVATION_KEY)
    policy_key = _ql(_ACTIVATION_KEY_V2)
    verified = _ql(_VERIFIED_STATUS)
    policy_verified = _ql(_POLICY_VERIFIED_STATUS)
    proof_statuses = ", ".join(_ql(status) for status in _PROOF_BEARING_STATUSES)
    create_function = (
        "CREATE OR REPLACE FUNCTION"
        if replace_existing
        else "CREATE FUNCTION"
    )
    policy_truncate_sql = (
        f"""
                    OR active_source.metadata_json::jsonb ? {policy_key}
                    OR active_source.metadata_json::jsonb
                         ->> 'provider_directory_candidate_status' =
                         {policy_verified}
        """
        if reviewed_root_policy_aware
        else ""
    )
    policy_invalid_sql = (
        f"""
                OR (
                    (
                        active_source.metadata_json::jsonb ? {policy_key}
                        OR active_source.metadata_json::jsonb
                             ->> 'provider_directory_candidate_status' =
                             {policy_verified}
                    )
                    AND (
                        pg_catalog.jsonb_typeof(
                            active_source.metadata_json::jsonb -> {policy_key}
                        ) IS DISTINCT FROM 'object'
                        OR active_source.metadata_json::jsonb ? {key}
                        OR (
                            NEW.dataset_id IS DISTINCT FROM
                                active_source.metadata_json::jsonb -> {policy_key}
                                    -> 'candidate' ->> 'dataset_id'
                            AND NEW.dataset_id IS DISTINCT FROM
                                active_source.metadata_json::jsonb -> {policy_key}
                                    -> 'baseline' ->> 'dataset_id'
                            AND NEW.publication_metadata_json::jsonb
                                    ->> 'verification_campaign_id'
                                IS NOT DISTINCT FROM
                                active_source.metadata_json::jsonb -> {policy_key}
                                    ->> 'verification_campaign_id'
                            AND NEW.publication_metadata_json::jsonb
                                    ->> 'verification_source_scope_hash'
                                IS NOT DISTINCT FROM
                                active_source.metadata_json::jsonb -> {policy_key}
                                    ->> 'verification_source_scope_sha256'
                        )
                    )
                )
        """
        if reviewed_root_policy_aware
        else ""
    )
    return f"""
    {create_function} {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        lock_acquired boolean;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
                SELECT 1
                  FROM {source_ref} AS active_source
                 WHERE active_source.metadata_json::jsonb ? {key}
                    OR active_source.metadata_json::jsonb
                         ->> 'provider_directory_candidate_status' = {verified}
                    {policy_truncate_sql}
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_reviewed_subset_activation_dataset_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF NEW.completion_proof_required_version IS DISTINCT FROM 3
           OR NEW.status NOT IN ({proof_statuses}) THEN
            RETURN NEW;
        END IF;

        SELECT pg_catalog.pg_try_advisory_xact_lock(
                   pg_catalog.hashtextextended(NEW.endpoint_id, 0)
               )
          INTO lock_acquired;
        IF lock_acquired IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_busy'
                USING ERRCODE = '55000';
        END IF;

        PERFORM active_source.source_id
          FROM {source_ref} AS active_source
         WHERE active_source.endpoint_id = NEW.endpoint_id
            OR active_source.metadata_json::jsonb
                 ->> 'provider_directory_configured_endpoint_id' = NEW.endpoint_id
         ORDER BY active_source.source_id
           FOR UPDATE OF active_source;

        IF EXISTS (
            SELECT 1
              FROM {source_ref} AS active_source
             WHERE (
                    active_source.endpoint_id = NEW.endpoint_id
                    OR active_source.metadata_json::jsonb
                         ->> 'provider_directory_configured_endpoint_id' =
                         NEW.endpoint_id
               )
               AND (
                    (
                        (
                            active_source.metadata_json::jsonb ? {key}
                            OR active_source.metadata_json::jsonb
                                 ->> 'provider_directory_candidate_status' =
                                 {verified}
                        )
                        AND (
                            pg_catalog.jsonb_typeof(
                                active_source.metadata_json::jsonb -> {key}
                            ) IS DISTINCT FROM 'object'
                            OR active_source.metadata_json::jsonb ? {policy_key}
                            OR (
                                NEW.dataset_id IS DISTINCT FROM
                                    active_source.metadata_json::jsonb -> {key}
                                        -> 'baseline' ->> 'dataset_id'
                                AND NEW.dataset_id IS DISTINCT FROM
                                    active_source.metadata_json::jsonb -> {key}
                                        -> 'candidate' ->> 'dataset_id'
                                AND NEW.publication_metadata_json::jsonb
                                        ->> 'verification_campaign_id'
                                    IS NOT DISTINCT FROM
                                    active_source.metadata_json::jsonb -> {key}
                                        ->> 'verification_campaign_id'
                                AND NEW.publication_metadata_json::jsonb
                                        ->> 'verification_source_scope_hash'
                                    IS NOT DISTINCT FROM
                                    active_source.metadata_json::jsonb -> {key}
                                        ->> 'verification_source_scope_sha256'
                            )
                        )
                    )
                    {policy_invalid_sql}
               )
        ) THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_dataset_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _activation_shape_fence_sql(schema: str, *, expect_installed: bool) -> str:
    source_ref = _qf(schema, _SOURCE)
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    valid_ref = _qf(schema, _ACTIVATION_VALID_FUNCTION)
    source_guard_ref = _qf(schema, _SOURCE_GUARD_FUNCTION)
    dataset_guard_ref = _qf(schema, _DATASET_GUARD_FUNCTION)
    expected_functions = 3 if expect_installed else 0
    expected_triggers = 4 if expect_installed else 0
    return f"""
    DO $migration$
    DECLARE
        function_count bigint;
        trigger_count bigint;
    BEGIN
        SELECT pg_catalog.count(*)
          INTO function_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
         WHERE function_namespace.nspname = {_ql(schema)}
           AND (
                (
                    function_row.oid =
                        pg_catalog.to_regprocedure({_ql(valid_ref + '(text)')})
                    AND function_row.pronargs = 1
                    AND function_row.prorettype = 'pg_catalog.bool'::regtype
                    AND function_language.lanname = 'sql'
                    AND function_row.provolatile = 's'
                ) OR (
                    function_row.oid IN (
                        pg_catalog.to_regprocedure({_ql(source_guard_ref + '()')}),
                        pg_catalog.to_regprocedure({_ql(dataset_guard_ref + '()')})
                    )
                    AND function_row.pronargs = 0
                    AND function_row.prorettype =
                        'pg_catalog.trigger'::regtype
                    AND function_language.lanname = 'plpgsql'
                )
           )
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                ARRAY['search_path=pg_catalog']::text[]
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.aclexplode(
                       COALESCE(
                           function_row.proacl,
                           pg_catalog.acldefault('f', function_row.proowner)
                       )
                  ) AS function_acl
                 WHERE function_acl.grantee = 0
                   AND function_acl.privilege_type = 'EXECUTE'
           );
        IF function_count <> {expected_functions} THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_function_changed'
                USING ERRCODE = '55000';
        END IF;

        SELECT pg_catalog.count(*)
          INTO trigger_count
          FROM (
                VALUES
                    (
                        {_ql(source_ref)}::regclass,
                        {_ql(_SOURCE_GUARD_TRIGGER)},
                        29,
                        pg_catalog.to_regprocedure({_ql(source_guard_ref + '()')}),
                        true,
                        true,
                        true
                    ),
                    (
                        {_ql(source_ref)}::regclass,
                        {_ql(_SOURCE_TRUNCATE_TRIGGER)},
                        34,
                        pg_catalog.to_regprocedure({_ql(source_guard_ref + '()')}),
                        false,
                        false,
                        false
                    ),
                    (
                        {_ql(dataset_ref)}::regclass,
                        {_ql(_DATASET_GUARD_TRIGGER)},
                        23,
                        pg_catalog.to_regprocedure({_ql(dataset_guard_ref + '()')}),
                        false,
                        false,
                        false
                    ),
                    (
                        {_ql(dataset_ref)}::regclass,
                        {_ql(_DATASET_TRUNCATE_TRIGGER)},
                        34,
                        pg_catalog.to_regprocedure({_ql(dataset_guard_ref + '()')}),
                        false,
                        false,
                        false
                    )
               ) AS expected(
                    relation_oid,
                    trigger_name,
                    trigger_type,
                    function_oid,
                    is_constraint,
                    is_deferrable,
                    is_initially_deferred
               )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = expected.relation_oid
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgtype = expected.trigger_type
           AND trigger_row.tgfoid = expected.function_oid
           AND (trigger_row.tgconstraint <> 0) = expected.is_constraint
           AND trigger_row.tgdeferrable = expected.is_deferrable
           AND trigger_row.tginitdeferred = expected.is_initially_deferred
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND pg_catalog.octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NULL
           AND trigger_row.tgnewtable IS NULL;
        IF trigger_count <> {expected_triggers} THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_activation_trigger_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _activation_state_preflight_sql(schema: str, *, downgrade: bool) -> str:
    source_ref = _qf(schema, _SOURCE)
    marker = _ql(_ACTIVATION_KEY)
    status = _ql(_VERIFIED_STATUS)
    error_code = (
        "provider_directory_reviewed_subset_activation_downgrade_blocked"
        if downgrade
        else "provider_directory_reviewed_subset_activation_adoption_blocked"
    )
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM {source_ref} AS source
             WHERE source.metadata_json::jsonb ? {marker}
                OR source.metadata_json::jsonb
                     ->> 'provider_directory_candidate_status' = {status}
        ) THEN
            RAISE EXCEPTION {_ql(error_code)} USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _lock_relations(schema: str) -> None:
    op.execute(
        "LOCK TABLE "
        + ", ".join(
            _qf(schema, relation)
            for relation in (_ENDPOINT_DATASET, _DATASET_RESOURCE, _SOURCE)
        )
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _predecessor_shape_fences(schema: str) -> None:
    previous = _predecessor()
    op.execute(
        previous._relation_schema_fence_sql(
            schema,
            _ENDPOINT_DATASET,
            previous._SUBSET_ENDPOINT_DATASET_COLUMNS,
            compatible_columns=(
                previous._RECEIPT_ENDPOINT_DATASET_COLUMNS,
                previous._ADMISSION_SEAL_ENDPOINT_DATASET_COLUMNS,
                previous._COMBINED_ENDPOINT_DATASET_COLUMNS,
            ),
        )
    )
    op.execute(
        previous._relation_schema_fence_sql(
            schema,
            _DATASET_RESOURCE,
            previous._SUBSET_DATASET_RESOURCE_COLUMNS,
        )
    )
    op.execute(previous._subset_column_shape_fence_sql(schema))
    op.execute(previous._guard_trigger_shape_fence_sql(schema))
    op.execute(
        previous._source_guard_shape_fence_sql(
            schema,
            expect_installed=True,
        )
    )
    op.execute(previous._proof_function_shape_fence_sql(schema))


def _create_activation_objects(schema: str) -> None:
    source_ref = _qf(schema, _SOURCE)
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    source_guard_ref = _qf(schema, _SOURCE_GUARD_FUNCTION)
    dataset_guard_ref = _qf(schema, _DATASET_GUARD_FUNCTION)
    valid_ref = _qf(schema, _ACTIVATION_VALID_FUNCTION)
    op.execute(_activation_valid_function_sql(schema))
    op.execute(_source_guard_function_sql(schema))
    op.execute(_dataset_guard_function_sql(schema))
    op.execute(
        f"CREATE CONSTRAINT TRIGGER {_q(_SOURCE_GUARD_TRIGGER)} "
        f"AFTER INSERT OR UPDATE OR DELETE ON {source_ref} "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        f"EXECUTE FUNCTION {source_guard_ref}();"
    )
    op.execute(
        f"CREATE TRIGGER {_q(_SOURCE_TRUNCATE_TRIGGER)} "
        f"BEFORE TRUNCATE ON {source_ref} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {source_guard_ref}();"
    )
    op.execute(
        f"CREATE TRIGGER {_q(_DATASET_GUARD_TRIGGER)} "
        f"BEFORE INSERT OR UPDATE ON {dataset_ref} FOR EACH ROW "
        f"EXECUTE FUNCTION {dataset_guard_ref}();"
    )
    op.execute(
        f"CREATE TRIGGER {_q(_DATASET_TRUNCATE_TRIGGER)} "
        f"BEFORE TRUNCATE ON {dataset_ref} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {dataset_guard_ref}();"
    )
    for relation_ref, trigger_name in (
        (source_ref, _SOURCE_GUARD_TRIGGER),
        (source_ref, _SOURCE_TRUNCATE_TRIGGER),
        (dataset_ref, _DATASET_GUARD_TRIGGER),
        (dataset_ref, _DATASET_TRUNCATE_TRIGGER),
    ):
        op.execute(
            f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER "
            f"{_q(trigger_name)};"
        )
    op.execute(f"REVOKE ALL ON FUNCTION {valid_ref}(text) FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {source_guard_ref}() FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {dataset_guard_ref}() FROM PUBLIC;")


def _drop_activation_objects(schema: str) -> None:
    source_ref = _qf(schema, _SOURCE)
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    for relation_ref, trigger_name in (
        (dataset_ref, _DATASET_TRUNCATE_TRIGGER),
        (dataset_ref, _DATASET_GUARD_TRIGGER),
        (source_ref, _SOURCE_TRUNCATE_TRIGGER),
        (source_ref, _SOURCE_GUARD_TRIGGER),
    ):
        op.execute(
            f"DROP TRIGGER {_q(trigger_name)} ON {relation_ref};"
        )
    op.execute(
        f"DROP FUNCTION {_qf(schema, _DATASET_GUARD_FUNCTION)}();"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, _SOURCE_GUARD_FUNCTION)}();"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, _ACTIVATION_VALID_FUNCTION)}(text);"
    )


def upgrade() -> None:
    schema = _schema()
    _lock_relations(schema)
    _predecessor_shape_fences(schema)
    op.execute(_activation_shape_fence_sql(schema, expect_installed=False))
    op.execute(_activation_state_preflight_sql(schema, downgrade=False))
    _create_activation_objects(schema)
    op.execute(_activation_shape_fence_sql(schema, expect_installed=True))
    _predecessor_shape_fences(schema)


def downgrade() -> None:
    schema = _schema()
    _lock_relations(schema)
    _predecessor_shape_fences(schema)
    op.execute(_activation_shape_fence_sql(schema, expect_installed=True))
    op.execute(_activation_state_preflight_sql(schema, downgrade=True))
    _drop_activation_objects(schema)
    op.execute(_activation_shape_fence_sql(schema, expect_installed=False))
    _predecessor_shape_fences(schema)
