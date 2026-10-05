-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
SELECT violation.* FROM (
        -- Execute only inside the protected frozen-candidate validator. All three
        -- namespace tokens are registry/OID bindings, never caller-supplied identifiers.
        -- Failure rows are internal diagnostics, not publication or validation receipts.
        WITH expected_build AS (
            SELECT b.* FROM __CONTROL__.custom_import_build_attempt b
            WHERE b.execution_id=$5 AND b.producing_fence=$7
        ), revision_counts AS (
            SELECT pack_id,count(*) AS record_count FROM (
                SELECT r.pack_id FROM __CANDIDATE__.custom_import_root_revision r
                UNION ALL SELECT c.pack_id FROM __CANDIDATE__.custom_import_child_revision c
            ) revisions GROUP BY pack_id
        ), child_counts AS (
            SELECT e.family_revision_id,count(*) AS child_count
            FROM __CANDIDATE__.custom_import_family_child e GROUP BY e.family_revision_id
        )
        SELECT 'metadata_scope'::text AS failure_code,$5::bigint AS row_identity
        WHERE NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_execution e
            JOIN __CONTROL__.custom_import_dataset d ON d.dataset_id=e.dataset_id
            JOIN __CONTROL__.custom_import_definition_revision definition
              ON definition.definition_revision_id=e.definition_revision_id
              AND definition.dataset_id=e.dataset_id AND definition.schema_revision_id=e.schema_revision_id
            JOIN __CONTROL__.custom_import_schema_revision schema_revision
              ON schema_revision.schema_revision_id=e.schema_revision_id AND schema_revision.dataset_id=e.dataset_id
            JOIN __CONTROL__.custom_import_capture_bundle capture ON capture.capture_bundle_id=e.capture_bundle_id
              AND capture.dataset_id=e.dataset_id AND capture.definition_revision_id=e.definition_revision_id
              AND capture.schema_revision_id=e.schema_revision_id AND capture.capture_state='sealed'
            WHERE ROW(e.execution_id,e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.capture_bundle_id)
              IS NOT DISTINCT FROM ROW($5,$2,$3,$4,$6)
        )
        UNION ALL
        SELECT 'generation_scope',$1 WHERE $1 IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_generation g
            WHERE ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,
                g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id,g.base_dataset_id)
              IS NOT DISTINCT FROM ROW($1,$2,$3,$4,$5,
                $6,$7,$8,$9,
                CASE WHEN $9 IS NOT NULL THEN $2 END)
        )
        UNION ALL
        SELECT 'generation_target_required',g.generation_id FROM __CONTROL__.custom_import_generation g
        WHERE $1 IS NULL AND g.execution_id=$5 AND g.producing_fence=$7
        UNION ALL
        SELECT 'base_generation_scope',$9 WHERE $9 IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_generation g
            JOIN __CONTROL__.custom_import_generation_seal seal ON seal.generation_id=g.generation_id
              AND ROW(seal.dataset_id,seal.definition_revision_id,seal.schema_revision_id,seal.execution_id,seal.capture_bundle_id)
                IS NOT DISTINCT FROM ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id)
              AND ROW(seal.sealing_fence,seal.sealing_token_sha256)
                IS NOT DISTINCT FROM ROW(g.producing_fence,g.producing_token_sha256)
            WHERE g.generation_id=$9 AND g.dataset_id=$2
        )
        UNION ALL
        SELECT 'build_scope',b.build_id FROM expected_build b
        WHERE ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.capture_bundle_id,b.producing_token_sha256,
            b.generation_id,b.base_generation_id) IS DISTINCT FROM
            ROW($2,$3,$4,$6,$8,
                $1,$9)
        UNION ALL
        SELECT 'root_dictionary_owner',r.root_record_id FROM __CANDIDATE__.custom_import_root_record r
        WHERE r.dataset_id IS DISTINCT FROM $2
        UNION ALL
        SELECT 'root_dictionary_canonical_identity',r.root_record_id FROM __CANDIDATE__.custom_import_root_record r
        WHERE NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_root_record canonical
            WHERE canonical.root_record_id=r.root_record_id AND canonical.dataset_id=r.dataset_id
              AND canonical.key_contract_sha256=r.key_contract_sha256 AND canonical.logical_key_sha256=r.logical_key_sha256
              AND canonical.canonical_logical_key COLLATE "C" IS NOT DISTINCT FROM r.canonical_logical_key COLLATE "C"
        )
        UNION ALL
        SELECT 'root_dictionary_identity',r.root_record_id FROM __CANDIDATE__.custom_import_root_record r
        JOIN __BASE__.custom_import_root_record prior ON prior.root_record_id=r.root_record_id
        WHERE $9 IS NOT NULL AND ROW(prior.dataset_id,prior.key_contract_sha256,
            prior.logical_key_sha256,prior.canonical_logical_key COLLATE "C")
            IS DISTINCT FROM ROW(r.dataset_id,r.key_contract_sha256,r.logical_key_sha256,r.canonical_logical_key COLLATE "C")
        UNION ALL
        SELECT 'entity_dictionary_owner',e.entity_binding_id FROM __CANDIDATE__.custom_import_entity_binding e
        WHERE e.dataset_id IS DISTINCT FROM $2
        UNION ALL
        SELECT 'entity_dictionary_canonical_identity',e.entity_binding_id FROM __CANDIDATE__.custom_import_entity_binding e
        WHERE NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_entity_binding canonical
            WHERE canonical.entity_binding_id=e.entity_binding_id AND canonical.dataset_id=e.dataset_id
              AND canonical.adapter_id=e.adapter_id AND canonical.value_sha256=e.value_sha256
              AND canonical.canonical_value COLLATE "C" IS NOT DISTINCT FROM e.canonical_value COLLATE "C"
        )
        UNION ALL
        SELECT 'root_dictionary_rebinding',r.root_record_id FROM __CANDIDATE__.custom_import_root_record r
        JOIN __BASE__.custom_import_root_record prior ON prior.dataset_id=r.dataset_id
          AND prior.key_contract_sha256=r.key_contract_sha256 AND prior.logical_key_sha256=r.logical_key_sha256
        WHERE $9 IS NOT NULL AND prior.root_record_id IS DISTINCT FROM r.root_record_id
        UNION ALL
        SELECT 'entity_dictionary_identity',e.entity_binding_id FROM __CANDIDATE__.custom_import_entity_binding e
        JOIN __BASE__.custom_import_entity_binding prior ON prior.entity_binding_id=e.entity_binding_id
        WHERE $9 IS NOT NULL AND ROW(prior.dataset_id,prior.adapter_id,prior.canonical_value COLLATE "C",prior.value_sha256)
            IS DISTINCT FROM ROW(e.dataset_id,e.adapter_id,e.canonical_value COLLATE "C",e.value_sha256)
        UNION ALL
        SELECT 'entity_dictionary_rebinding',e.entity_binding_id FROM __CANDIDATE__.custom_import_entity_binding e
        JOIN __BASE__.custom_import_entity_binding prior ON prior.dataset_id=e.dataset_id
          AND prior.adapter_id=e.adapter_id AND prior.canonical_value=e.canonical_value
        WHERE $9 IS NOT NULL AND prior.entity_binding_id IS DISTINCT FROM e.entity_binding_id
        UNION ALL
        SELECT 'pack_reference',p.pack_id FROM __CANDIDATE__.custom_import_pack p
        WHERE ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.capture_bundle_id,
            p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM
            ROW($5,$2,$3,$4,$6,$7,$8)
          OR NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_capture c
            JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=c.definition_revision_id
              AND stream.dataset_id=c.dataset_id AND stream.schema_revision_id=c.schema_revision_id
              AND stream.stream_slot=c.stream_slot
            WHERE ROW(c.capture_bundle_id,c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.stream_slot)
              IS NOT DISTINCT FROM ROW(p.capture_bundle_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.stream_slot)
              AND c.capture_state='sealed'
          )
        UNION ALL
        SELECT 'pack_revision_count',p.pack_id FROM __CANDIDATE__.custom_import_pack p
        LEFT JOIN revision_counts n ON n.pack_id=p.pack_id
        WHERE p.record_count IS DISTINCT FROM coalesce(n.record_count,0)
        UNION ALL
        SELECT 'pack_source_ordinal_duplicate',revision.pack_id FROM (
            SELECT r.pack_id,r.source_ordinal FROM __CANDIDATE__.custom_import_root_revision r
            UNION ALL SELECT c.pack_id,c.source_ordinal FROM __CANDIDATE__.custom_import_child_revision c
        ) revision GROUP BY revision.pack_id,revision.source_ordinal HAVING count(*)>1
        UNION ALL
        SELECT 'rejection_reference',r.rejection_id FROM __CANDIDATE__.custom_import_rejection r
        WHERE ROW(r.execution_id,r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.producing_fence,r.producing_token_sha256)
            IS DISTINCT FROM ROW($5,$2,$3,$4,$7,$8)
          OR (r.pack_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_pack p WHERE p.pack_id=r.pack_id
              AND ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.capture_bundle_id,
                  p.producing_fence,p.producing_token_sha256)
                IS NOT DISTINCT FROM ROW($5,r.dataset_id,r.definition_revision_id,r.schema_revision_id,
                  $6,$7,$8)
          ))
        UNION ALL
        SELECT 'root_revision_reference',r.root_revision_id FROM __CANDIDATE__.custom_import_root_revision r
        WHERE ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id) IS DISTINCT FROM
            ROW($2,$3,$4)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_root_record k
            WHERE k.root_record_id=r.root_record_id AND k.dataset_id=r.dataset_id
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_pack p
            JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=p.definition_revision_id
              AND stream.dataset_id=p.dataset_id AND stream.schema_revision_id=p.schema_revision_id
              AND stream.stream_slot=p.stream_slot AND stream.record_kind='root' AND stream.collection_slot IS NULL
            WHERE p.pack_id=r.pack_id AND ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256)
              IS NOT DISTINCT FROM ROW($5,r.dataset_id,r.definition_revision_id,r.schema_revision_id,
                $6,$7,$8)
          )
        UNION ALL
        SELECT 'child_revision_reference',c.child_revision_id FROM __CANDIDATE__.custom_import_child_revision c
        WHERE ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS DISTINCT FROM
            ROW($2,$3,$4)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_root_record k
            WHERE k.root_record_id=c.root_record_id AND k.dataset_id=c.dataset_id
              AND k.canonical_logical_key COLLATE "C" IS NOT DISTINCT FROM c.canonical_parent_key COLLATE "C"
              AND k.logical_key_sha256 IS NOT DISTINCT FROM c.parent_key_sha256
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_child_collection collection
            WHERE collection.collection_slot=c.collection_slot
              AND collection.dataset_id=c.dataset_id AND collection.schema_revision_id=c.schema_revision_id
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_pack p
            JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=p.definition_revision_id
              AND stream.dataset_id=p.dataset_id AND stream.schema_revision_id=p.schema_revision_id
              AND stream.stream_slot=p.stream_slot AND stream.record_kind='child' AND stream.collection_slot=c.collection_slot
            WHERE p.pack_id=c.pack_id AND ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256)
              IS NOT DISTINCT FROM ROW($5,c.dataset_id,c.definition_revision_id,c.schema_revision_id,
                $6,$7,$8)
          )
        UNION ALL
        SELECT 'family_reference',f.family_revision_id FROM __CANDIDATE__.custom_import_family_revision f
        WHERE ROW(f.dataset_id,f.schema_revision_id,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
            IS DISTINCT FROM ROW($2,$4,$5,$7,$8)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_root_revision r WHERE r.root_revision_id=f.root_revision_id
              AND ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id)
                IS NOT DISTINCT FROM ROW(f.dataset_id,$3,f.schema_revision_id,f.root_record_id)
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_entity_binding e WHERE e.entity_binding_id=f.entity_binding_id
              AND e.dataset_id=f.dataset_id
          )
        UNION ALL
        SELECT 'family_child_reference',e.child_revision_id FROM __CANDIDATE__.custom_import_family_child e
        WHERE ROW(e.dataset_id,e.schema_revision_id) IS DISTINCT FROM ROW($2,$4)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f WHERE f.family_revision_id=e.family_revision_id
              AND ROW(f.dataset_id,f.schema_revision_id,f.root_record_id)
                IS NOT DISTINCT FROM ROW(e.dataset_id,e.schema_revision_id,e.root_record_id)
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_child_revision c WHERE c.child_revision_id=e.child_revision_id
              AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot)
                IS NOT DISTINCT FROM ROW(e.dataset_id,$3,e.schema_revision_id,e.root_record_id,e.collection_slot)
          )
        UNION ALL
        SELECT 'family_child_count',f.family_revision_id FROM __CANDIDATE__.custom_import_family_revision f
        LEFT JOIN child_counts n ON n.family_revision_id=f.family_revision_id
        WHERE f.child_count IS DISTINCT FROM coalesce(n.child_count,0)
        UNION ALL
        SELECT 'family_child_duplicate_key',edge.family_revision_id FROM __CANDIDATE__.custom_import_family_child edge
        JOIN __CANDIDATE__.custom_import_child_revision child ON child.child_revision_id=edge.child_revision_id
        GROUP BY edge.family_revision_id,edge.collection_slot,child.child_key_sha256 HAVING count(*)>1
        UNION ALL
        SELECT 'generation_membership_reference',m.root_record_id FROM __CANDIDATE__.custom_import_generation_family m
        WHERE $1 IS NULL OR ROW(m.generation_id,m.dataset_id,m.definition_revision_id,m.schema_revision_id)
            IS DISTINCT FROM ROW($1,$2,$3,$4)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f WHERE f.family_revision_id=m.family_revision_id
              AND ROW(f.dataset_id,f.schema_revision_id,f.root_record_id)
                IS NOT DISTINCT FROM ROW(m.dataset_id,m.schema_revision_id,m.root_record_id)
          )
        UNION ALL
        SELECT 'family_missing_generation_membership',f.family_revision_id FROM __CANDIDATE__.custom_import_family_revision f
        WHERE $1 IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_generation_family m
            WHERE m.generation_id=$1 AND m.root_record_id=f.root_record_id
              AND ROW(m.generation_id,m.dataset_id,m.definition_revision_id,m.schema_revision_id,m.root_record_id,m.family_revision_id)
              IS NOT DISTINCT FROM ROW($1,$2,$3,$4,f.root_record_id,f.family_revision_id)
        )
     ) violation LIMIT 1;
