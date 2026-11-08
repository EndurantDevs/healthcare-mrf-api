-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE FUNCTION __CONTROL__.lock_custom_import_legacy_generation(p_generation_id bigint)
RETURNS __CONTROL__.custom_import_generation
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE g __CONTROL__.custom_import_generation; e __CONTROL__.custom_import_execution; owner_name name;
BEGIN
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF current_user IS DISTINCT FROM owner_name THEN RAISE EXCEPTION 'custom_import_legacy_owner_mismatch'; END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=p_generation_id;
    IF g.generation_id IS NULL THEN RAISE EXCEPTION 'custom_import_legacy_generation_missing'; END IF;
    e:=__CONTROL__.lock_custom_import_snapshot_attempt(g.execution_id,g.producing_fence,g.producing_token_sha256);
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=p_generation_id FOR UPDATE;
    IF g.generation_id IS NULL OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id)
        IS DISTINCT FROM ROW(e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.execution_id,e.capture_bundle_id)
        OR NOT EXISTS (SELECT 1 FROM __CONTROL__.custom_import_capture_bundle c
            WHERE c.capture_bundle_id=g.capture_bundle_id AND c.sealed_at IS NOT NULL)
        OR EXISTS (SELECT 1 FROM __CONTROL__.custom_import_build_attempt b
            WHERE b.execution_id=g.execution_id OR b.generation_id=g.generation_id)
        OR EXISTS (SELECT 1 FROM __CONTROL__.custom_import_generation_seal s WHERE s.execution_id=g.execution_id)
        OR EXISTS (SELECT 1 FROM __CONTROL__.custom_import_no_change_seal s WHERE s.execution_id=g.execution_id) THEN
        RAISE EXCEPTION 'custom_import_materialization_not_open_legacy_attempt'; END IF;
    PERFORM __CONTROL__.lock_custom_import_snapshot_attempt(g.execution_id,g.producing_fence,g.producing_token_sha256);
    RETURN g;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CONTROL__.lock_custom_import_legacy_generation_snapshot(p_generation_id bigint)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE g __CONTROL__.custom_import_generation; f __CONTROL__.custom_import_snapshot_family;
    snapshot_id bigint; base_id bigint;
BEGIN
    g:=__CONTROL__.lock_custom_import_legacy_generation(p_generation_id);
    snapshot_id:=__CONTROL__.lock_custom_import_writable_snapshot(
        g.execution_id,g.producing_fence,g.producing_token_sha256);
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=snapshot_id;
    IF ROW(f.generation_id,f.dataset_id,f.definition_revision_id,f.schema_revision_id,
           f.execution_id,f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
        IS DISTINCT FROM ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
           g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256)
        OR f.frozen_at IS NOT NULL THEN RAISE EXCEPTION 'custom_import_legacy_snapshot_binding_mismatch'; END IF;
    base_id:=__CONTROL__.lock_custom_import_sealed_snapshot_base(g.base_generation_id,g.dataset_id);
    IF base_id=snapshot_id THEN RAISE EXCEPTION 'custom_import_legacy_base_is_candidate'; END IF;
    PERFORM __CONTROL__.lock_custom_import_snapshot_attempt(g.execution_id,g.producing_fence,g.producing_token_sha256);
    RETURN snapshot_id;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CONTROL__.check_custom_import_materialization_expected(
    p_expected bigint[], p_token bytea, p_deadline timestamptz
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE e __CONTROL__.custom_import_execution; l __CONTROL__.custom_import_lease;
    owner_name name; now_at timestamptz; timeout_ms numeric;
BEGIN
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF current_user IS DISTINCT FROM owner_name THEN
        RAISE EXCEPTION 'custom_import_materialization_owner_mismatch'; END IF;
    IF p_expected IS NULL OR cardinality(p_expected)<>6 OR array_ndims(p_expected)<>1
        OR array_lower(p_expected,1)<>1 OR array_position(p_expected,NULL) IS NOT NULL OR 0>=ANY(p_expected)
        OR p_token IS NULL OR octet_length(p_token)<>32 OR p_deadline IS NULL OR NOT isfinite(p_deadline) THEN
        RAISE EXCEPTION 'custom_import_materialization_expected_shape'; END IF;
    PERFORM 1 FROM __CONTROL__.custom_import_dataset WHERE dataset_id=p_expected[1] FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'custom_import_materialization_dataset_missing'; END IF;
    SELECT * INTO e FROM __CONTROL__.custom_import_execution WHERE execution_id=p_expected[4] FOR UPDATE;
    SELECT * INTO l FROM __CONTROL__.custom_import_lease WHERE execution_id=p_expected[4] FOR UPDATE;
    now_at:=clock_timestamp();
    timeout_ms:=extract(epoch FROM current_setting('statement_timeout')::interval)*1000;
    IF ARRAY[e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.execution_id,e.capture_bundle_id,l.fence]
        IS DISTINCT FROM p_expected OR l.token_sha256 IS DISTINCT FROM p_token OR e.state IS DISTINCT FROM 'running'
        OR l.expires_at IS NULL OR p_deadline>l.expires_at OR least(l.expires_at,p_deadline)<=now_at
        OR timeout_ms<=0 OR timeout_ms>=extract(epoch FROM (least(l.expires_at,p_deadline)-now_at))*1000
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE execution_id=e.execution_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=e.execution_id) THEN
        RAISE EXCEPTION 'custom_import_materialization_authority_lost'; END IF;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CONTROL__.check_custom_import_materialization_authority(
    p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint,
    p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE g __CONTROL__.custom_import_generation;
BEGIN
    PERFORM __CONTROL__.check_custom_import_materialization_expected(
        ARRAY[p_dataset_id,p_definition_revision_id,p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence],
        p_token_sha256,p_deadline_at);
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_attempt WHERE execution_id=p_execution_id) THEN
        RAISE EXCEPTION 'custom_import_materialization_not_open_legacy_attempt'; END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE execution_id=p_execution_id AND producing_fence=p_fence;
    IF g.generation_id IS NULL OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.capture_bundle_id,
        g.producing_token_sha256) IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,
        p_capture_bundle_id,p_token_sha256) THEN RAISE EXCEPTION 'custom_import_legacy_generation_mismatch'; END IF;
    PERFORM __CONTROL__.lock_custom_import_legacy_generation_snapshot(g.generation_id);
    PERFORM __CONTROL__.check_custom_import_materialization_expected(
        ARRAY[p_dataset_id,p_definition_revision_id,p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence],
        p_token_sha256,p_deadline_at);
END $fn$;

-- statement boundary --

CREATE FUNCTION __CONTROL__.check_custom_import_generation_materialization_authority(
    p_generation_id bigint, p_dataset_id bigint, p_definition_revision_id bigint,
    p_schema_revision_id bigint, p_execution_id bigint, p_capture_bundle_id bigint,
    p_fence bigint, p_token bytea, p_expected_authority bigint[],
    p_expected_token bytea, p_expected_expires timestamptz
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $function$
DECLARE
    g __CONTROL__.custom_import_generation%ROWTYPE;
    e __CONTROL__.custom_import_execution%ROWTYPE;
    l __CONTROL__.custom_import_lease%ROWTYPE;
    f __CONTROL__.custom_import_snapshot_family%ROWTYPE;
    snapshot_family_id bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'custom_import_finality_requires_read_committed' USING ERRCODE = 'P0001';
    END IF;
    SELECT generation.* INTO g FROM __CONTROL__.custom_import_generation AS generation
    WHERE generation.generation_id = p_generation_id;
    IF NOT FOUND OR ROW(g.generation_id, g.dataset_id, g.definition_revision_id,
            g.schema_revision_id, g.execution_id, g.capture_bundle_id,
            g.producing_fence, g.producing_token_sha256)
        IS DISTINCT FROM ROW(p_generation_id, p_dataset_id, p_definition_revision_id,
            p_schema_revision_id, p_execution_id, p_capture_bundle_id, p_fence, p_token)
        OR g.producing_fence IS NULL OR g.producing_fence <= 0
        OR g.producing_token_sha256 IS NULL OR octet_length(g.producing_token_sha256) <> 32 THEN
        RAISE EXCEPTION 'custom_import_generation_membership_identity_mismatch' USING ERRCODE = 'P0001';
    END IF;

    -- Keep the existing low-volume finality lock order, once per set/check.
    PERFORM dataset.dataset_id FROM __CONTROL__.custom_import_dataset AS dataset
    WHERE dataset.dataset_id = g.dataset_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'custom_import_finality_dataset_missing' USING ERRCODE = 'P0001';
    END IF;
    SELECT execution.* INTO e FROM __CONTROL__.custom_import_execution AS execution
    WHERE execution.execution_id = g.execution_id FOR UPDATE;
    IF NOT FOUND OR ROW(e.dataset_id, e.definition_revision_id, e.schema_revision_id, e.capture_bundle_id)
        IS DISTINCT FROM ROW(g.dataset_id, g.definition_revision_id, g.schema_revision_id, g.capture_bundle_id)
        OR e.state IS DISTINCT FROM 'running' THEN
        RAISE EXCEPTION 'custom_import_generation_membership_execution_mismatch' USING ERRCODE = 'P0001';
    END IF;
    SELECT lease.* INTO l FROM __CONTROL__.custom_import_lease AS lease
    WHERE lease.execution_id = g.execution_id FOR UPDATE;
    IF NOT FOUND OR l.fence IS DISTINCT FROM g.producing_fence
        OR l.token_sha256 IS DISTINCT FROM g.producing_token_sha256
        OR l.expires_at IS NULL OR l.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'custom_import_output_producing_lease_lost' USING ERRCODE = 'P0001';
    END IF;

    IF NOT (p_expected_authority IS NULL AND p_expected_token IS NULL AND p_expected_expires IS NULL) THEN
        IF p_expected_authority IS NULL OR p_expected_token IS NULL OR p_expected_expires IS NULL
            OR array_ndims(p_expected_authority) IS DISTINCT FROM 1
            OR array_lower(p_expected_authority, 1) IS DISTINCT FROM 1
            OR p_expected_authority IS DISTINCT FROM ARRAY[g.dataset_id, g.definition_revision_id,
                g.schema_revision_id, g.execution_id, g.capture_bundle_id, g.producing_fence]
            OR p_expected_token IS DISTINCT FROM g.producing_token_sha256
            OR p_expected_expires IS DISTINCT FROM l.expires_at THEN
            RAISE EXCEPTION 'custom_import_generation_membership_runner_mismatch' USING ERRCODE = 'P0001';
        END IF;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_capture_bundle AS bundle
        WHERE bundle.capture_bundle_id = g.capture_bundle_id AND bundle.dataset_id = g.dataset_id
            AND bundle.definition_revision_id = g.definition_revision_id
            AND bundle.schema_revision_id = g.schema_revision_id
            AND bundle.capture_state = 'sealed' AND bundle.sealed_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'custom_import_generation_membership_capture_mismatch' USING ERRCODE = 'P0001';
    END IF;
    IF EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_generation_seal AS seal
        WHERE seal.generation_id = g.generation_id
    ) OR EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_no_change_seal AS seal
        WHERE seal.candidate_generation_id = g.generation_id
            OR (seal.execution_id = g.execution_id AND seal.dataset_id = g.dataset_id)
    ) THEN
        RAISE EXCEPTION 'custom_import_sealed_generation_append' USING ERRCODE = 'P0001';
    END IF;
    IF EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_build_attempt AS build
        WHERE build.generation_id = g.generation_id
            OR (build.execution_id = g.execution_id AND build.producing_fence = g.producing_fence)
    ) THEN
        RAISE EXCEPTION 'custom_import_generation_membership_requires_build_output' USING ERRCODE = 'P0001';
    END IF;

    -- Reuse the same protected family lock for nonempty and empty callers.
    -- It checks freshness/open state, validates the fixed OID/owner/layout map,
    -- takes ROW EXCLUSIVE locks on all leaves, and validates the map again.
    snapshot_family_id := __CONTROL__.lock_custom_import_legacy_generation_snapshot(g.generation_id);
    SELECT snapshot.* INTO f FROM __CONTROL__.custom_import_snapshot_family AS snapshot
    WHERE snapshot.family_id = snapshot_family_id;
    IF NOT FOUND OR ROW(f.generation_id, f.dataset_id, f.definition_revision_id,
            f.schema_revision_id, f.execution_id, f.capture_bundle_id,
            f.producing_fence, f.producing_token_sha256)
        IS DISTINCT FROM ROW(g.generation_id, g.dataset_id, g.definition_revision_id,
            g.schema_revision_id, g.execution_id, g.capture_bundle_id,
            g.producing_fence, g.producing_token_sha256) OR f.frozen_at IS NOT NULL THEN
        RAISE EXCEPTION 'custom_import_generation_membership_snapshot_mismatch' USING ERRCODE = 'P0001';
    END IF;
    IF l.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'custom_import_output_producing_lease_lost' USING ERRCODE = 'P0001';
    END IF;
END;
$function$;
