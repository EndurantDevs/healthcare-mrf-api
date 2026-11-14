-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

DECLARE f __SCHEMA__.custom_import_snapshot_family; names text[] := __NAMES__; namespace text;
BEGIN
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family WHERE family_id=p_family_id;
    IF f.family_id IS NULL THEN RAISE EXCEPTION 'custom_import_snapshot_family_missing'; END IF;
    namespace := 'ci_snapshot_' || f.family_id::text;
    IF (SELECT count(*) FROM __SCHEMA__.custom_import_snapshot_relation WHERE family_id=f.family_id) <> 15
       OR EXISTS (
           SELECT 1 FROM __SCHEMA__.custom_import_snapshot_relation r
           WHERE r.family_id=f.family_id
             AND to_regclass(format('%I.%I',namespace,names[r.relation_slot])) IS NULL
       ) THEN RAISE EXCEPTION 'custom_import_snapshot_relation_mismatch'; END IF;
    RETURN QUERY
        SELECT r.relation_slot,to_regclass(format('%I.%I',namespace,names[r.relation_slot]))::bigint
        FROM __SCHEMA__.custom_import_snapshot_relation r
        WHERE r.family_id=f.family_id ORDER BY r.relation_slot;
END;
