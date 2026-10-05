-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- __ORIGIN_QUERY__ applies only to a registered ordinary generation.
SELECT violation.* FROM (
    SELECT 'legacy_copy_root'::text AS failure_code,o.revision_id::bigint AS row_identity
    FROM __CANDIDATE__.legacy_copy_origin o WHERE o.kind='root' AND NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f
        JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=f.root_revision_id
        JOIN __BASE__.custom_import_generation_family m ON m.generation_id=$9 AND m.root_record_id=f.root_record_id
        JOIN __BASE__.custom_import_family_revision prior ON prior.family_revision_id=m.family_revision_id
        JOIN __BASE__.custom_import_root_revision original ON original.root_revision_id=prior.root_revision_id
        WHERE ROW(o.base_generation_id,o.family_revision_id,o.root_record_id,o.root_revision_id,o.base_family_revision_id,o.base_root_revision_id)
            IS NOT DISTINCT FROM ROW($9,f.family_revision_id,f.root_record_id,f.root_revision_id,prior.family_revision_id,prior.root_revision_id)
          AND o.revision_id=r.root_revision_id AND o.collection_slot IS NULL AND o.base_child_revision_id IS NULL
          AND ROW(f.dataset_id,f.schema_revision_id,f.entity_binding_id,f.family_sha256,f.child_count)
            IS NOT DISTINCT FROM ROW(prior.dataset_id,prior.schema_revision_id,prior.entity_binding_id,prior.family_sha256,prior.child_count)
          AND ROW(r.dataset_id,r.schema_revision_id,r.root_record_id,r.canonical_payload COLLATE "C",r.payload_sha256)
            IS NOT DISTINCT FROM ROW(original.dataset_id,original.schema_revision_id,original.root_record_id,
                original.canonical_payload COLLATE "C",original.payload_sha256))
    UNION ALL
    SELECT 'legacy_copy_child',o.revision_id FROM __CANDIDATE__.legacy_copy_origin o WHERE o.kind='child' AND NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.legacy_copy_origin root_origin
        JOIN __CANDIDATE__.custom_import_family_child e ON e.family_revision_id=root_origin.family_revision_id
        JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=e.child_revision_id
        JOIN __BASE__.custom_import_family_child original_edge ON original_edge.family_revision_id=root_origin.base_family_revision_id
            AND original_edge.collection_slot=e.collection_slot AND original_edge.child_revision_id=o.base_child_revision_id
        JOIN __BASE__.custom_import_child_revision original ON original.child_revision_id=original_edge.child_revision_id
        WHERE root_origin.kind='root' AND e.child_revision_id=o.revision_id
          AND ROW(o.base_generation_id,o.family_revision_id,o.root_record_id,o.root_revision_id,o.collection_slot,
                o.base_family_revision_id,o.base_root_revision_id)
            IS NOT DISTINCT FROM ROW(root_origin.base_generation_id,e.family_revision_id,e.root_record_id,root_origin.root_revision_id,e.collection_slot,
                root_origin.base_family_revision_id,root_origin.base_root_revision_id)
          AND ROW(c.dataset_id,c.schema_revision_id,c.root_record_id,c.collection_slot,c.canonical_parent_key COLLATE "C",
                c.parent_key_sha256,c.canonical_child_key COLLATE "C",c.child_key_sha256,c.canonical_payload COLLATE "C",c.payload_sha256)
            IS NOT DISTINCT FROM ROW(original.dataset_id,original.schema_revision_id,original.root_record_id,original.collection_slot,
                original.canonical_parent_key COLLATE "C",original.parent_key_sha256,original.canonical_child_key COLLATE "C",
                original.child_key_sha256,original.canonical_payload COLLATE "C",original.payload_sha256))
    UNION ALL
    SELECT 'legacy_copy_missing_child',e.child_revision_id FROM __CANDIDATE__.legacy_copy_origin root_origin
    JOIN __CANDIDATE__.custom_import_family_child e ON e.family_revision_id=root_origin.family_revision_id
    WHERE root_origin.kind='root' AND NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.legacy_copy_origin child_origin
        WHERE child_origin.kind='child' AND child_origin.revision_id=e.child_revision_id
            AND child_origin.family_revision_id=e.family_revision_id AND child_origin.collection_slot=e.collection_slot)
    UNION ALL
    SELECT 'legacy_copy_omitted_child',e.child_revision_id FROM __CANDIDATE__.legacy_copy_origin root_origin
    JOIN __BASE__.custom_import_family_child e ON e.family_revision_id=root_origin.base_family_revision_id
    WHERE root_origin.kind='root' AND NOT EXISTS (
        SELECT 1 FROM __CANDIDATE__.legacy_copy_origin child_origin
        WHERE child_origin.kind='child' AND child_origin.family_revision_id=root_origin.family_revision_id
            AND child_origin.base_child_revision_id=e.child_revision_id AND child_origin.collection_slot=e.collection_slot)
) violation LIMIT 1;
