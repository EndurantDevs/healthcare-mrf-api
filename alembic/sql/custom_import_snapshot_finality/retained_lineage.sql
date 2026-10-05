-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
SELECT violation.* FROM (
        -- Retained provenance is an exact sealed-base reference, never a new-fence
        -- requirement on old rows. Legacy copies need persisted plan/occurrence evidence
        -- before this query can attest their origin; equal hashes do not invent origin.
        WITH retained_plans AS (
            SELECT plan.* FROM __CANDIDATE__.custom_import_build_family plan WHERE plan.selection_kind='retained'
        ), base_members AS (
            SELECT m.* FROM __BASE__.custom_import_generation_family m
            JOIN retained_plans plan ON plan.root_record_id=m.root_record_id AND plan.base_family_revision_id=m.family_revision_id
            WHERE m.generation_id=$9
        ), base_edges AS (
            SELECT edge.* FROM __BASE__.custom_import_family_child edge
            JOIN base_members m ON m.family_revision_id=edge.family_revision_id
        )
        SELECT 'retained_plan_base_reference'::text AS failure_code,plan.root_record_id::bigint AS row_identity
        FROM retained_plans plan WHERE NOT EXISTS (
            SELECT 1 FROM base_members m
            JOIN __CONTROL__.custom_import_generation g ON g.generation_id=m.generation_id
            JOIN __CONTROL__.custom_import_generation_seal seal ON seal.generation_id=g.generation_id
              AND ROW(seal.dataset_id,seal.definition_revision_id,seal.schema_revision_id,seal.execution_id,seal.capture_bundle_id)
                IS NOT DISTINCT FROM ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id)
              AND ROW(seal.sealing_fence,seal.sealing_token_sha256)
                IS NOT DISTINCT FROM ROW(g.producing_fence,g.producing_token_sha256)
            JOIN __BASE__.custom_import_family_revision f ON f.family_revision_id=m.family_revision_id
            JOIN __BASE__.custom_import_root_revision r ON r.root_revision_id=f.root_revision_id
            JOIN __BASE__.custom_import_root_record key ON key.root_record_id=r.root_record_id
            JOIN __BASE__.custom_import_entity_binding entity ON entity.entity_binding_id=f.entity_binding_id
            WHERE m.root_record_id=plan.root_record_id AND m.family_revision_id=plan.base_family_revision_id
              AND ROW(m.dataset_id,m.definition_revision_id,m.schema_revision_id)
                IS NOT DISTINCT FROM ROW($2,g.definition_revision_id,$4)
              AND ROW(g.dataset_id,g.schema_revision_id) IS NOT DISTINCT FROM ROW($2,$4)
              AND ROW(f.dataset_id,f.schema_revision_id,f.root_record_id)
                IS NOT DISTINCT FROM ROW($2,$4,plan.root_record_id)
              AND ROW(f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS NOT DISTINCT FROM ROW(g.execution_id,g.producing_fence,g.producing_token_sha256)
              AND ROW(r.dataset_id,r.schema_revision_id,r.root_record_id)
                IS NOT DISTINCT FROM ROW($2,$4,plan.root_record_id)
              AND key.dataset_id=$2 AND key.logical_key_sha256 IS NOT DISTINCT FROM plan.root_key_sha256
              AND entity.dataset_id=$2
        )
        UNION ALL
        SELECT 'retained_base_root_pack',r.root_revision_id FROM base_members m
        JOIN __BASE__.custom_import_family_revision f ON f.family_revision_id=m.family_revision_id
        JOIN __BASE__.custom_import_root_revision r ON r.root_revision_id=f.root_revision_id
        WHERE NOT EXISTS (
            SELECT 1 FROM __BASE__.custom_import_pack pack
            JOIN __CONTROL__.custom_import_execution execution ON execution.execution_id=pack.execution_id
              AND ROW(execution.dataset_id,execution.definition_revision_id,execution.schema_revision_id,execution.capture_bundle_id)
                IS NOT DISTINCT FROM ROW(pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id,pack.capture_bundle_id)
            JOIN __CONTROL__.custom_import_capture capture ON capture.capture_bundle_id=pack.capture_bundle_id
              AND ROW(capture.dataset_id,capture.definition_revision_id,capture.schema_revision_id,capture.stream_slot)
                IS NOT DISTINCT FROM ROW(pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id,pack.stream_slot)
              AND capture.capture_state='sealed'
            JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=pack.definition_revision_id
              AND stream.dataset_id=pack.dataset_id AND stream.schema_revision_id=pack.schema_revision_id
              AND stream.stream_slot=pack.stream_slot AND stream.record_kind='root'
            WHERE pack.pack_id=r.pack_id AND ROW(pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id)
              IS NOT DISTINCT FROM ROW($2,r.definition_revision_id,$4)
              AND ROW(pack.execution_id,pack.producing_fence,pack.producing_token_sha256)
                IS NOT DISTINCT FROM ROW(f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
        )
        UNION ALL
        SELECT 'retained_base_edge_reference',edge.child_revision_id FROM base_edges edge
        WHERE NOT EXISTS (
            SELECT 1 FROM base_members m
            JOIN __BASE__.custom_import_child_revision child ON child.child_revision_id=edge.child_revision_id
            JOIN __BASE__.custom_import_root_record key ON key.root_record_id=child.root_record_id
            JOIN __CONTROL__.custom_import_child_collection collection
              ON collection.schema_revision_id=child.schema_revision_id AND collection.dataset_id=child.dataset_id
              AND collection.collection_slot=child.collection_slot
            WHERE m.family_revision_id=edge.family_revision_id
              AND ROW(edge.dataset_id,edge.schema_revision_id,edge.root_record_id)
                IS NOT DISTINCT FROM ROW($2,$4,m.root_record_id)
              AND ROW(child.dataset_id,child.schema_revision_id,child.root_record_id,child.collection_slot)
                IS NOT DISTINCT FROM ROW(edge.dataset_id,edge.schema_revision_id,edge.root_record_id,edge.collection_slot)
              AND key.dataset_id=$2 AND key.logical_key_sha256 IS NOT DISTINCT FROM child.parent_key_sha256
              AND key.canonical_logical_key COLLATE "C" IS NOT DISTINCT FROM child.canonical_parent_key COLLATE "C"
        )
        UNION ALL
        SELECT 'retained_base_child_pack',child.child_revision_id FROM base_edges edge
        JOIN __BASE__.custom_import_child_revision child ON child.child_revision_id=edge.child_revision_id
        JOIN __BASE__.custom_import_family_revision family ON family.family_revision_id=edge.family_revision_id
        WHERE NOT EXISTS (
            SELECT 1 FROM __BASE__.custom_import_pack pack
            JOIN __CONTROL__.custom_import_execution execution ON execution.execution_id=pack.execution_id
              AND ROW(execution.dataset_id,execution.definition_revision_id,execution.schema_revision_id,execution.capture_bundle_id)
                IS NOT DISTINCT FROM ROW(pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id,pack.capture_bundle_id)
            JOIN __CONTROL__.custom_import_capture capture ON capture.capture_bundle_id=pack.capture_bundle_id
              AND ROW(capture.dataset_id,capture.definition_revision_id,capture.schema_revision_id,capture.stream_slot)
                IS NOT DISTINCT FROM ROW(pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id,pack.stream_slot)
              AND capture.capture_state='sealed'
            JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=pack.definition_revision_id
              AND stream.dataset_id=pack.dataset_id AND stream.schema_revision_id=pack.schema_revision_id
              AND stream.stream_slot=pack.stream_slot AND stream.record_kind='child' AND stream.collection_slot=child.collection_slot
            WHERE pack.pack_id=child.pack_id AND ROW(pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id)
              IS NOT DISTINCT FROM ROW($2,child.definition_revision_id,$4)
              AND ROW(pack.execution_id,pack.producing_fence,pack.producing_token_sha256)
                IS NOT DISTINCT FROM ROW(family.producing_execution_id,family.producing_fence,family.producing_token_sha256)
        )
        UNION ALL
        SELECT 'retained_family_content',plan.family_revision_id FROM retained_plans plan
        WHERE plan.family_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM base_members m
            JOIN __BASE__.custom_import_family_revision old ON old.family_revision_id=m.family_revision_id
            JOIN __CANDIDATE__.custom_import_family_revision fresh ON fresh.family_revision_id=plan.family_revision_id
            WHERE m.root_record_id=plan.root_record_id AND m.family_revision_id=plan.base_family_revision_id
              AND ROW(old.dataset_id,old.schema_revision_id,old.root_record_id,old.entity_binding_id,old.family_sha256,old.child_count)
                IS NOT DISTINCT FROM ROW(fresh.dataset_id,fresh.schema_revision_id,fresh.root_record_id,fresh.entity_binding_id,
                  fresh.family_sha256,fresh.child_count)
        )
        UNION ALL
        SELECT 'retained_occurrence_plan',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.origin='retained' AND NOT EXISTS (
            SELECT 1 FROM retained_plans plan WHERE plan.build_id=o.build_id AND plan.root_record_id=o.root_record_id
              AND plan.base_family_revision_id=o.base_family_revision_id
        )
        UNION ALL
        SELECT 'retained_root_copy',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.origin='retained' AND o.root_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM base_members m
            JOIN __BASE__.custom_import_family_revision family ON family.family_revision_id=m.family_revision_id
            JOIN __BASE__.custom_import_root_revision old ON old.root_revision_id=family.root_revision_id
            JOIN __CANDIDATE__.custom_import_root_revision fresh ON fresh.root_revision_id=o.root_revision_id
            WHERE m.family_revision_id=o.base_family_revision_id AND m.root_record_id=o.root_record_id
              AND old.root_revision_id=o.base_root_revision_id
              AND ROW(old.dataset_id,old.schema_revision_id,old.root_record_id,old.payload_sha256)
                IS NOT DISTINCT FROM ROW($2,$4,o.root_record_id,fresh.payload_sha256)
              AND old.canonical_payload COLLATE "C" IS NOT DISTINCT FROM fresh.canonical_payload COLLATE "C"
        )
        UNION ALL
        SELECT 'retained_child_copy',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.origin='retained' AND o.child_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM base_edges edge
            JOIN __BASE__.custom_import_child_revision old ON old.child_revision_id=edge.child_revision_id
            JOIN __CANDIDATE__.custom_import_child_revision fresh ON fresh.child_revision_id=o.child_revision_id
            WHERE edge.family_revision_id=o.base_family_revision_id AND edge.child_revision_id=o.base_child_revision_id
              AND ROW(edge.dataset_id,edge.schema_revision_id,edge.root_record_id,edge.collection_slot)
                IS NOT DISTINCT FROM ROW($2,$4,o.root_record_id,o.collection_slot)
              AND ROW(old.dataset_id,old.schema_revision_id,old.root_record_id,old.collection_slot,
                  old.parent_key_sha256,old.child_key_sha256,old.payload_sha256)
                IS NOT DISTINCT FROM ROW(fresh.dataset_id,fresh.schema_revision_id,fresh.root_record_id,fresh.collection_slot,
                  fresh.parent_key_sha256,fresh.child_key_sha256,fresh.payload_sha256)
              AND old.canonical_parent_key COLLATE "C" IS NOT DISTINCT FROM fresh.canonical_parent_key COLLATE "C"
              AND old.canonical_child_key COLLATE "C" IS NOT DISTINCT FROM fresh.canonical_child_key COLLATE "C"
              AND old.canonical_payload COLLATE "C" IS NOT DISTINCT FROM fresh.canonical_payload COLLATE "C"
        )
        UNION ALL
        SELECT 'retained_family_missing_root_copy',plan.family_revision_id FROM retained_plans plan
        WHERE plan.family_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f
            JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=plan.build_id
              AND o.root_record_id=plan.root_record_id AND o.origin='retained' AND o.root_revision_id=f.root_revision_id
              AND o.base_family_revision_id=plan.base_family_revision_id
            WHERE f.family_revision_id=plan.family_revision_id
        )
        UNION ALL
        SELECT 'retained_edge_missing_copy',edge.child_revision_id FROM __CANDIDATE__.custom_import_family_child edge
        JOIN retained_plans plan ON plan.family_revision_id=edge.family_revision_id
        WHERE NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE o.build_id=plan.build_id AND o.origin='retained' AND o.root_record_id=plan.root_record_id
              AND o.collection_slot=edge.collection_slot AND o.child_revision_id=edge.child_revision_id
              AND o.base_family_revision_id=plan.base_family_revision_id
        )
        UNION ALL
        SELECT 'retained_copy_missing_edge',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        JOIN retained_plans plan ON plan.build_id=o.build_id AND plan.root_record_id=o.root_record_id
          AND plan.base_family_revision_id=o.base_family_revision_id
        WHERE o.origin='retained' AND o.child_revision_id IS NOT NULL
          AND (plan.complete_at IS NOT NULL OR $1 IS NOT NULL) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_family_child edge
            WHERE edge.family_revision_id=plan.family_revision_id AND edge.collection_slot=o.collection_slot
              AND edge.child_revision_id=o.child_revision_id
        )
        UNION ALL
        SELECT 'retained_base_child_missing_copy',edge.child_revision_id FROM base_edges edge
        JOIN retained_plans plan ON plan.base_family_revision_id=edge.family_revision_id
        WHERE (plan.complete_at IS NOT NULL OR $1 IS NOT NULL) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE o.build_id=plan.build_id AND o.origin='retained' AND o.base_family_revision_id=edge.family_revision_id
              AND o.base_child_revision_id=edge.child_revision_id AND o.collection_slot=edge.collection_slot
              AND o.root_record_id=plan.root_record_id
        )
     ) violation LIMIT 1;
