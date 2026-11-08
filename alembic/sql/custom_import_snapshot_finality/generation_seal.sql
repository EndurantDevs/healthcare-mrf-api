-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
DECLARE
        build __CONTROL__.custom_import_build_attempt;
        proof __CONTROL__.custom_import_build_verification;
        built_generation __CONTROL__.custom_import_generation;
        snapshot_family_id BIGINT;
        snapshot_schema TEXT;
        locked_dataset_id BIGINT;
        locked_execution_id BIGINT;
        generation_fence BIGINT;
        generation_token_sha256 BYTEA;
        generation_root_count BIGINT;
        generation_family_count BIGINT;
        lease_fence BIGINT;
        lease_token_sha256 BYTEA;
        lease_expires_at TIMESTAMP WITH TIME ZONE;
        exact_family_count BIGINT;
        exact_family_child_count BIGINT;
        exact_winner_count BIGINT;
        exact_profile_count BIGINT;
        exact_root_scalar_count BIGINT;
        exact_child_scalar_count BIGINT;
        bundle_stream_count BIGINT;
        definition_stream_count BIGINT;
        exact_capture_count BIGINT;
        has_capture_stream_mismatch BOOLEAN;
        has_family_child_mismatch BOOLEAN;
        has_duplicate_child_key BOOLEAN;
        has_child_parent_mismatch BOOLEAN;
    BEGIN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'custom_import_finality_requires_read_committed'
                USING ERRCODE = 'P0001';
        END IF;

        IF TG_RELID<>'__CONTROL__.custom_import_generation_seal'::regclass OR TG_OP<>'INSERT' OR TG_WHEN<>'BEFORE' OR TG_LEVEL<>'ROW' THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        EXECUTE format(
            'SELECT %I.lock_custom_import_snapshot_finality($1,$2,$3,$4,$5,$6,$7,$8)',
            TG_TABLE_SCHEMA
        ) INTO snapshot_family_id USING NEW.generation_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id, NEW.execution_id,
            NEW.capture_bundle_id, NEW.sealing_fence, NEW.sealing_token_sha256;
        IF snapshot_family_id IS NULL OR snapshot_family_id <= 0 THEN
            RAISE EXCEPTION 'custom_import_snapshot_finality_binding_mismatch' USING ERRCODE = 'P0001';
        END IF;
        -- The binder validates and pins all fifteen exact registered OIDs.
        -- This fixed namespace is never supplied by the sealing caller.
        snapshot_schema := 'ci_snapshot_' || snapshot_family_id::text;
        SELECT * INTO build FROM __CONTROL__.custom_import_build_attempt WHERE execution_id=NEW.execution_id AND producing_fence=NEW.sealing_fence;
        IF build.build_id IS NOT NULL THEN
            build:=__CONTROL__.lock_custom_import_build(build.build_id);
            SELECT * INTO built_generation FROM __CONTROL__.custom_import_generation WHERE generation_id=NEW.generation_id FOR UPDATE;
            SELECT * INTO proof FROM __CONTROL__.custom_import_build_verification WHERE build_id=build.build_id;
            IF build.phase<>'verified' OR build.generation_id IS DISTINCT FROM NEW.generation_id
                OR proof.verification_state IS DISTINCT FROM 'complete' OR proof.scan_stage IS DISTINCT FROM 'complete'
                OR proof.verified_at IS NULL OR proof.generation_id IS DISTINCT FROM NEW.generation_id
                OR ROW(proof.source_frozen_at,proof.graph_frozen_at,proof.output_frozen_at,proof.verified_at) IS DISTINCT FROM
                    ROW(build.source_frozen_at,build.graph_frozen_at,build.output_frozen_at,build.verified_at)
                OR ROW(NEW.dataset_id,NEW.definition_revision_id,NEW.schema_revision_id,NEW.execution_id,NEW.capture_bundle_id,NEW.sealing_fence,NEW.sealing_token_sha256)
                    IS DISTINCT FROM ROW(build.dataset_id,build.definition_revision_id,build.schema_revision_id,build.execution_id,build.capture_bundle_id,
                        build.producing_fence,build.producing_token_sha256)
                OR ROW(built_generation.root_count,built_generation.family_count) IS DISTINCT FROM ROW(proof.root_count,proof.family_count)
                OR ROW(NEW.root_count,NEW.family_count,NEW.generation_family_count,NEW.family_child_count,NEW.winner_count,NEW.profile_count,
                    NEW.root_scalar_count,NEW.child_scalar_count) IS DISTINCT FROM ROW(proof.root_count,proof.family_count,proof.generation_family_count,
                    proof.family_child_count,proof.winner_count,proof.profile_count,proof.root_scalar_count,proof.child_scalar_count) THEN
                RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
            RETURN NEW;
        END IF;
        PERFORM __CONTROL__.verify_custom_import_snapshot_structure(NEW.generation_id);
            EXECUTE format(
            'SELECT dataset_id FROM %I.custom_import_dataset WHERE dataset_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_dataset_id USING NEW.dataset_id;
        IF locked_dataset_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_generation_seal_dataset_missing' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT execution_id FROM %I.custom_import_execution '
            || 'WHERE execution_id = $1 AND dataset_id = $2 AND definition_revision_id = $3 '
            || 'AND schema_revision_id = $4 AND capture_bundle_id = $5 AND state = ''running'' FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_execution_id USING NEW.execution_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id, NEW.capture_bundle_id;
        IF locked_execution_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_generation_seal_execution_not_running' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT fence, token_sha256, expires_at FROM %I.custom_import_lease '
            || 'WHERE execution_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO lease_fence, lease_token_sha256, lease_expires_at USING NEW.execution_id;
        IF lease_fence IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp() OR
           lease_fence <> NEW.sealing_fence OR lease_token_sha256 <> NEW.sealing_token_sha256 THEN
            RAISE EXCEPTION 'custom_import_generation_seal_lease_lost' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT producing_fence, producing_token_sha256, root_count, family_count '
            || 'FROM %I.custom_import_generation WHERE generation_id = $1 AND dataset_id = $2 AND definition_revision_id = $3 '
            || 'AND schema_revision_id = $4 AND execution_id = $5 AND capture_bundle_id = $6 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO generation_fence, generation_token_sha256, generation_root_count, generation_family_count
            USING NEW.generation_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id, NEW.execution_id, NEW.capture_bundle_id;
        IF generation_fence IS NULL OR generation_fence <> NEW.sealing_fence OR
           generation_token_sha256 <> NEW.sealing_token_sha256 THEN
            RAISE EXCEPTION 'custom_import_generation_seal_authority_mismatch' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT stream_count FROM %I.custom_import_capture_bundle '
            || 'WHERE capture_bundle_id = $1 AND dataset_id = $2 '
            || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
        ) INTO bundle_stream_count USING NEW.capture_bundle_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_source_stream '
            || 'WHERE definition_revision_id = $1 AND dataset_id = $2 AND schema_revision_id = $3', TG_TABLE_SCHEMA
        ) INTO definition_stream_count USING NEW.definition_revision_id, NEW.dataset_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_capture '
            || 'WHERE capture_bundle_id = $1 AND dataset_id = $2 '
            || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
        ) INTO exact_capture_count USING NEW.capture_bundle_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %I.custom_import_source_stream stream '
            || 'WHERE stream.definition_revision_id = $3 AND stream.dataset_id = $2 '
            || 'AND stream.schema_revision_id = $4 AND NOT EXISTS (SELECT 1 '
            || 'FROM %I.custom_import_capture capture WHERE capture.capture_bundle_id = $1 '
            || 'AND capture.dataset_id = $2 AND capture.definition_revision_id = $3 '
            || 'AND capture.schema_revision_id = $4 AND capture.stream_slot = stream.stream_slot) '
            || 'UNION ALL SELECT 1 FROM %I.custom_import_capture capture '
            || 'WHERE capture.capture_bundle_id = $1 AND capture.dataset_id = $2 '
            || 'AND capture.definition_revision_id = $3 AND capture.schema_revision_id = $4 '
            || 'AND NOT EXISTS (SELECT 1 FROM %I.custom_import_source_stream stream '
            || 'WHERE stream.definition_revision_id = $3 AND stream.dataset_id = $2 '
            || 'AND stream.schema_revision_id = $4 AND stream.stream_slot = capture.stream_slot))',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO has_capture_stream_mismatch USING NEW.capture_bundle_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id;
        IF bundle_stream_count IS NULL OR bundle_stream_count <> definition_stream_count OR
           bundle_stream_count <> exact_capture_count OR has_capture_stream_mismatch THEN
            RAISE EXCEPTION 'custom_import_generation_seal_capture_incomplete' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_generation_family '
            || 'WHERE generation_id = $1 AND dataset_id = $2', snapshot_schema
        ) INTO exact_family_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_family_child child '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = child.family_revision_id '
            || 'AND family.dataset_id = child.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2',
            snapshot_schema, snapshot_schema
        ) INTO exact_family_child_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_family family '
            || 'JOIN %I.custom_import_family_revision family_revision '
            || 'ON family_revision.family_revision_id = family.family_revision_id '
            || 'AND family_revision.dataset_id = family.dataset_id '
            || 'LEFT JOIN LATERAL (SELECT count(*) AS child_count '
            || 'FROM %I.custom_import_family_child child '
            || 'WHERE child.family_revision_id = family.family_revision_id '
            || 'AND child.dataset_id = family.dataset_id) exact_child ON TRUE '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2 '
            || 'AND family_revision.child_count <> exact_child.child_count)',
            snapshot_schema, snapshot_schema, snapshot_schema
        ) INTO has_family_child_mismatch USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM ('
            || 'SELECT child.family_revision_id, child.collection_slot, child_revision.child_key_sha256 '
            || 'FROM %I.custom_import_family_child child '
            || 'JOIN %I.custom_import_child_revision child_revision '
            || 'ON child_revision.child_revision_id = child.child_revision_id '
            || 'AND child_revision.dataset_id = child.dataset_id '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = child.family_revision_id '
            || 'AND family.dataset_id = child.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2 '
            || 'GROUP BY child.family_revision_id, child.collection_slot, child_revision.child_key_sha256 '
            || 'HAVING count(*) > 1) duplicate_child_key)',
            snapshot_schema, snapshot_schema, snapshot_schema
        ) INTO has_duplicate_child_key USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_family family '
            || 'JOIN %I.custom_import_family_child child '
            || 'ON child.family_revision_id = family.family_revision_id '
            || 'AND child.dataset_id = family.dataset_id '
            || 'JOIN %I.custom_import_child_revision child_revision '
            || 'ON child_revision.child_revision_id = child.child_revision_id '
            || 'AND child_revision.dataset_id = child.dataset_id '
            || 'JOIN %I.custom_import_root_record root_record '
            || 'ON root_record.root_record_id = family.root_record_id '
            || 'AND root_record.dataset_id = family.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2 '
            || 'AND (child_revision.canonical_parent_key <> root_record.canonical_logical_key '
            || 'OR child_revision.parent_key_sha256 <> root_record.logical_key_sha256))',
            snapshot_schema, snapshot_schema, snapshot_schema, snapshot_schema
        ) INTO has_child_parent_mismatch USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_winner '
            || 'WHERE generation_id = $1 AND dataset_id = $2', snapshot_schema
        ) INTO exact_winner_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_selection_profile '
            || 'WHERE definition_revision_id = $1 AND dataset_id = $2 AND schema_revision_id = $3',
            TG_TABLE_SCHEMA
        ) INTO exact_profile_count USING NEW.definition_revision_id, NEW.dataset_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_root_scalar scalar '
            || 'JOIN %I.custom_import_family_revision family_revision '
            || 'ON family_revision.root_revision_id = scalar.root_revision_id '
            || 'AND family_revision.dataset_id = scalar.dataset_id '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = family_revision.family_revision_id '
            || 'AND family.dataset_id = family_revision.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2',
            snapshot_schema, snapshot_schema, snapshot_schema
        ) INTO exact_root_scalar_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_child_scalar scalar '
            || 'JOIN %I.custom_import_family_child child '
            || 'ON child.child_revision_id = scalar.child_revision_id '
            || 'AND child.dataset_id = scalar.dataset_id '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = child.family_revision_id '
            || 'AND family.dataset_id = child.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2',
            snapshot_schema, snapshot_schema, snapshot_schema
        ) INTO exact_child_scalar_count USING NEW.generation_id, NEW.dataset_id;
        IF has_duplicate_child_key THEN
            RAISE EXCEPTION 'custom_import_generation_seal_duplicate_child_key' USING ERRCODE = 'P0001';
        END IF;
        IF has_child_parent_mismatch THEN
            RAISE EXCEPTION 'custom_import_generation_seal_child_parent_mismatch' USING ERRCODE = 'P0001';
        END IF;
        IF has_family_child_mismatch OR generation_root_count <> exact_family_count OR
           generation_family_count <> exact_family_count OR NEW.root_count <> exact_family_count OR
           NEW.family_count <> exact_family_count OR
           NEW.generation_family_count <> exact_family_count OR
           NEW.family_child_count <> exact_family_child_count OR NEW.winner_count <> exact_winner_count OR
           NEW.profile_count <> exact_profile_count OR NEW.root_scalar_count <> exact_root_scalar_count OR
           NEW.child_scalar_count <> exact_child_scalar_count THEN
            RAISE EXCEPTION 'custom_import_generation_seal_count_mismatch' USING ERRCODE = 'P0001';
        END IF;
        RETURN NEW;
    END;
