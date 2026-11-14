-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

        DECLARE closed_authority __CONTROL__.source_bulk_authorization;
            completed __CONTROL__.source_bulk_completion;
            stream __CONTROL__.custom_import_build_stream; has_landing boolean;
        BEGIN
            SELECT * INTO closed_authority FROM __CONTROL__.source_bulk_authorization WHERE batch_id=p_batch;
            IF closed_authority.batch_id IS NULL THEN RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
            IF NOT closed_authority.accepting THEN
                b:=__CONTROL__.lock_custom_import_build(closed_authority.build_id);
                SELECT * INTO closed_authority FROM __CONTROL__.source_bulk_authorization WHERE batch_id=p_batch FOR UPDATE;
                SELECT * INTO completed FROM __CONTROL__.source_bulk_completion WHERE batch_id=p_batch FOR UPDATE;
                SELECT * INTO stream FROM __CONTROL__.custom_import_build_stream current_stream
                    WHERE current_stream.build_id=b.build_id AND current_stream.stream_slot=closed_authority.stream_slot FOR UPDATE;
                IF closed_authority.accepting IS DISTINCT FROM false OR closed_authority.transaction_id<>pg_current_xact_id()
                    OR completed.batch_id IS NULL OR b.phase<>'source'
                    OR ROW(closed_authority.build_id,closed_authority.dataset_id,closed_authority.definition_revision_id,closed_authority.schema_revision_id,
                        closed_authority.execution_id,closed_authority.capture_bundle_id,closed_authority.fence,closed_authority.token_hash)
                        IS DISTINCT FROM ROW(b.build_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,
                            b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)
                    OR ROW(completed.transaction_id,completed.build_id,completed.stream_slot,
                        completed.first_source,completed.attempted_count,completed.after_source)
                        IS DISTINCT FROM ROW(pg_current_xact_id(),closed_authority.build_id,closed_authority.stream_slot,
                            closed_authority.first_source,closed_authority.expected_count,closed_authority.first_source+closed_authority.expected_count)
                    OR stream.build_id IS NULL OR stream.replay_verified_at IS NOT NULL
                    OR stream.next_source_ordinal IS DISTINCT FROM completed.after_source
                    OR NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_source_stream declared
                        WHERE declared.definition_revision_id=b.definition_revision_id
                            AND declared.stream_slot=closed_authority.stream_slot AND declared.dataset_id=b.dataset_id
                            AND declared.schema_revision_id=b.schema_revision_id) THEN
                    RAISE EXCEPTION 'source_completed_batch_mismatch';
                END IF;
                family_id:=__CONTROL__.lock_custom_import_writable_snapshot(b.execution_id,closed_authority.fence,closed_authority.token_hash);
                PERFORM __CONTROL__.resolve_custom_import_build_base_snapshot(b.build_id);
                namespace:='ci_snapshot_'||family_id::text;
                EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.source_bulk_landing WHERE batch_id=$1)',namespace)
                    INTO has_landing USING p_batch;
                IF has_landing THEN RAISE EXCEPTION 'source_completed_batch_has_landing'; END IF;
                __CLOSE_SOURCE_LANDING_INSERT__
                PERFORM __CONTROL__.lock_custom_import_build(b.build_id);
                RETURN completed.attempted_count;
            END IF;
        END;
