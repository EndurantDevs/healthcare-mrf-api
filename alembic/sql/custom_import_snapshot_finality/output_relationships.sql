-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Complete immutable candidate relationships; canonical value/hash checks are streamed separately.
SELECT violation.* FROM (
    SELECT 'root_scalar_reference'::text AS failure_code,s.root_revision_id::bigint AS row_identity
    FROM __CANDIDATE__.custom_import_root_scalar s WHERE NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.custom_import_root_revision r
        JOIN __CONTROL__.custom_import_field f ON f.schema_revision_id=r.schema_revision_id
            AND f.dataset_id=r.dataset_id AND f.field_slot=s.field_slot AND f.collection_slot=0
        WHERE r.root_revision_id=s.root_revision_id
          AND ROW(s.dataset_id,s.schema_revision_id,s.root_record_id,s.field_collection_slot,s.field_type,s.projection_slot)
              IS NOT DISTINCT FROM ROW(r.dataset_id,r.schema_revision_id,r.root_record_id,f.collection_slot,f.field_type,f.projection_slot)
          AND f.projection_slot>0
          AND ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id) IS NOT DISTINCT FROM ROW($2,$3,$4))
    UNION ALL
    SELECT 'child_scalar_reference',s.child_revision_id FROM __CANDIDATE__.custom_import_child_scalar s WHERE NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.custom_import_child_revision c
        JOIN __CONTROL__.custom_import_field f ON f.schema_revision_id=c.schema_revision_id
            AND f.dataset_id=c.dataset_id AND f.field_slot=s.field_slot AND f.collection_slot=c.collection_slot
        WHERE c.child_revision_id=s.child_revision_id
          AND ROW(s.dataset_id,s.schema_revision_id,s.root_record_id,s.collection_slot,s.field_collection_slot,s.field_type,s.projection_slot)
              IS NOT DISTINCT FROM ROW(c.dataset_id,c.schema_revision_id,c.root_record_id,c.collection_slot,f.collection_slot,f.field_type,f.projection_slot)
          AND f.projection_slot>0
          AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS NOT DISTINCT FROM ROW($2,$3,$4))
    UNION ALL
    SELECT 'winner_reference',w.family_revision_id FROM __CANDIDATE__.custom_import_winner w WHERE NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.custom_import_generation_family m
        JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=m.family_revision_id
        JOIN __CONTROL__.custom_import_selection_profile p ON p.definition_revision_id=m.definition_revision_id
            AND p.dataset_id=m.dataset_id AND p.schema_revision_id=m.schema_revision_id AND p.profile_slot=w.profile_slot
        WHERE m.generation_id=w.generation_id AND m.dataset_id=w.dataset_id AND m.family_revision_id=w.family_revision_id
          AND ROW(m.generation_id,m.dataset_id,m.definition_revision_id,m.schema_revision_id,m.family_revision_id)
            IS NOT DISTINCT FROM ROW(w.generation_id,w.dataset_id,w.definition_revision_id,w.schema_revision_id,w.family_revision_id)
          AND ROW(w.generation_id,w.dataset_id,w.definition_revision_id,w.schema_revision_id) IS NOT DISTINCT FROM ROW($1,$2,$3,$4)
          AND w.entity_binding_id IS NOT DISTINCT FROM f.entity_binding_id
          AND ((p.context_collection_slot IS NULL AND w.context_collection_slot=0 AND w.context_child_revision_id IS NULL)
            OR (p.context_collection_slot=w.context_collection_slot AND w.context_child_revision_id IS NOT NULL AND EXISTS (
                SELECT 1 FROM __CANDIDATE__.custom_import_family_child e
                WHERE e.family_revision_id=w.family_revision_id AND e.collection_slot=w.context_collection_slot
                  AND e.child_revision_id=w.context_child_revision_id
                  AND ROW(e.family_revision_id,e.dataset_id,e.schema_revision_id,e.root_record_id,e.collection_slot,e.child_revision_id)
                  IS NOT DISTINCT FROM ROW(w.family_revision_id,w.dataset_id,w.schema_revision_id,f.root_record_id,
                      w.context_collection_slot,w.context_child_revision_id)))))
    UNION ALL
    SELECT 'context_reference',c.candidate_context_id FROM __CANDIDATE__.custom_import_build_candidate_context c
    WHERE NOT EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_build_attempt b
        JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=c.family_revision_id
        JOIN __CANDIDATE__.custom_import_generation_family m ON m.generation_id=b.generation_id AND m.family_revision_id=f.family_revision_id
        JOIN __CONTROL__.custom_import_selection_profile p ON p.definition_revision_id=b.definition_revision_id
            AND p.dataset_id=b.dataset_id AND p.schema_revision_id=b.schema_revision_id AND p.profile_slot=c.profile_slot
        WHERE b.build_id=c.build_id AND ROW(b.execution_id,b.producing_fence,b.producing_token_sha256) IS NOT DISTINCT FROM ROW($5,$7,$8)
          AND ROW(b.generation_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id) IS NOT DISTINCT FROM ROW($1,$2,$3,$4)
          AND c.entity_binding_id IS NOT DISTINCT FROM f.entity_binding_id
          AND ((p.context_collection_slot IS NULL AND c.context_collection_slot=0 AND c.context_child_revision_id IS NULL)
            OR (p.context_collection_slot=c.context_collection_slot AND EXISTS (
                SELECT 1 FROM __CANDIDATE__.custom_import_family_child e WHERE e.family_revision_id=c.family_revision_id
                    AND e.collection_slot=c.context_collection_slot AND e.child_revision_id=c.context_child_revision_id))))
    UNION ALL
    SELECT 'winner_context',w.family_revision_id FROM __CANDIDATE__.custom_import_winner w
    JOIN __CONTROL__.custom_import_build_attempt b ON b.generation_id=w.generation_id
    WHERE NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.custom_import_build_candidate_context c WHERE c.build_id=b.build_id
          AND c.profile_slot=w.profile_slot AND c.entity_binding_id=w.entity_binding_id AND c.context_key_sha256=w.context_key_sha256
          AND ROW(c.profile_slot,c.entity_binding_id,c.context_key_sha256,c.family_revision_id,c.context_child_revision_id)
            IS NOT DISTINCT FROM ROW(w.profile_slot,w.entity_binding_id,w.context_key_sha256,w.family_revision_id,w.context_child_revision_id))
    UNION ALL
    SELECT 'capture_stream',b.build_id FROM __CONTROL__.custom_import_build_attempt b
    WHERE b.execution_id=$5 AND b.producing_fence=$7 AND (
        EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_source_stream s
            LEFT JOIN __CONTROL__.custom_import_capture c ON c.capture_bundle_id=b.capture_bundle_id AND c.stream_slot=s.stream_slot
            LEFT JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=s.stream_slot
            WHERE s.definition_revision_id=b.definition_revision_id AND (
                c.stream_slot IS NULL OR bs.build_id IS NULL OR c.capture_state IS DISTINCT FROM 'sealed'
                OR ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS DISTINCT FROM ROW($2,$3,$4)
                OR bs.replay_verified_at IS NULL OR c.committed_record_count IS DISTINCT FROM bs.next_source_ordinal))
        OR (SELECT count(*) FROM __CONTROL__.custom_import_source_stream WHERE definition_revision_id=b.definition_revision_id)
            IS DISTINCT FROM (SELECT stream_count::bigint FROM __CONTROL__.custom_import_capture_bundle WHERE capture_bundle_id=b.capture_bundle_id)
        OR (SELECT count(*) FROM __CONTROL__.custom_import_capture WHERE capture_bundle_id=b.capture_bundle_id)
            IS DISTINCT FROM (SELECT stream_count::bigint FROM __CONTROL__.custom_import_capture_bundle WHERE capture_bundle_id=b.capture_bundle_id)
        OR b.source_occurrence_count IS DISTINCT FROM (SELECT count(*) FROM __CANDIDATE__.custom_import_build_occurrence WHERE origin='source')
        OR b.next_rejection_ordinal IS DISTINCT FROM (SELECT count(*) FROM __CANDIDATE__.custom_import_rejection)
        OR b.source_frozen_at IS NULL OR b.graph_frozen_at IS NULL OR b.output_frozen_at IS NULL)
) violation LIMIT 1;
