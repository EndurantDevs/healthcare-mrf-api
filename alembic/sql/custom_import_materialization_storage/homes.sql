-- Batch routing metadata; never a revision-row registry or authority source.
CREATE TABLE __CONTROL__.custom_import_revision_home (
    family_id bigint NOT NULL
        REFERENCES __CONTROL__.custom_import_snapshot_family(family_id) ON DELETE RESTRICT,
    revision_kind smallint NOT NULL CHECK (revision_kind IN (1, 2)),
    revision_ids int8multirange NOT NULL,
    first_revision_id bigint GENERATED ALWAYS AS (lower(revision_ids)) STORED,
    PRIMARY KEY (revision_kind, first_revision_id),
    CHECK (NOT isempty(revision_ids)
        AND revision_ids <@ int8multirange(int8range(1, NULL, '[)'))),
    EXCLUDE USING gist (revision_ids WITH &&) WHERE (revision_kind = 1),
    EXCLUDE USING gist (revision_ids WITH &&) WHERE (revision_kind = 2)
);
-- statement boundary --
CREATE FUNCTION __CONTROL__.lookup_custom_import_revision_home(p_root_ids bigint[],p_child_ids bigint[])
RETURNS TABLE(revision_kind smallint,revision_id bigint,family_id bigint)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
WITH requested AS MATERIALIZED (
    SELECT 1::smallint AS revision_kind,id FROM unnest(p_root_ids) ids(id) GROUP BY id
    UNION ALL
    SELECT 2::smallint,id FROM unnest(p_child_ids) ids(id) GROUP BY id
), wanted AS MATERIALIZED (
    SELECT revision_kind,range_agg(int8range(id,
        CASE WHEN id=9223372036854775807 THEN NULL ELSE id+1 END,'[)')) AS revision_ids
    FROM requested GROUP BY revision_kind
), matched AS MATERIALIZED (
    SELECT 1::smallint AS revision_kind,h.family_id,h.revision_ids*w.revision_ids AS revision_ids
    FROM __CONTROL__.custom_import_revision_home h
    JOIN wanted w ON w.revision_kind=1 AND h.revision_ids&&w.revision_ids WHERE h.revision_kind=1
    UNION ALL
    SELECT 2::smallint,h.family_id,h.revision_ids*w.revision_ids
    FROM __CONTROL__.custom_import_revision_home h
    JOIN wanted w ON w.revision_kind=2 AND h.revision_ids&&w.revision_ids WHERE h.revision_kind=2
)
SELECT r.revision_kind,r.id,m.family_id FROM requested r
LEFT JOIN matched m ON m.revision_kind=r.revision_kind AND m.revision_ids@>r.id
$fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.custom_import_materialization_origins(p_roots bigint[],p_children bigint[])
RETURNS TABLE(revision_kind smallint,revision_id bigint,family_id bigint,dataset_id bigint,
    definition_revision_id bigint,schema_revision_id bigint,execution_id bigint,capture_bundle_id bigint,
    fence bigint,token_sha256 bytea)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
WITH homes AS MATERIALIZED (
    SELECT * FROM __CONTROL__.lookup_custom_import_revision_home(p_roots,p_children)
), canonical AS MATERIALIZED (
    SELECT h.revision_kind,h.revision_id,p.*
    FROM homes h JOIN __CONTROL__.custom_import_root_revision r ON r.root_revision_id=h.revision_id
    JOIN __CONTROL__.custom_import_pack p ON p.pack_id=r.pack_id
        AND (p.dataset_id,p.definition_revision_id,p.schema_revision_id)=
            (r.dataset_id,r.definition_revision_id,r.schema_revision_id)
    WHERE h.family_id IS NULL AND h.revision_kind=1
    UNION ALL
    SELECT h.revision_kind,h.revision_id,p.*
    FROM homes h JOIN __CONTROL__.custom_import_child_revision r ON r.child_revision_id=h.revision_id
    JOIN __CONTROL__.custom_import_pack p ON p.pack_id=r.pack_id
        AND (p.dataset_id,p.definition_revision_id,p.schema_revision_id)=
            (r.dataset_id,r.definition_revision_id,r.schema_revision_id)
    WHERE h.family_id IS NULL AND h.revision_kind=2
), origins AS (
    SELECT h.revision_kind,h.revision_id,h.family_id,f.dataset_id,f.definition_revision_id,f.schema_revision_id,
        f.execution_id,f.capture_bundle_id,f.producing_fence,f.producing_token_sha256
    FROM homes h JOIN __CONTROL__.custom_import_snapshot_family f ON f.family_id=h.family_id
    UNION ALL
    SELECT c.revision_kind,c.revision_id,NULL::bigint,c.dataset_id,c.definition_revision_id,c.schema_revision_id,
        c.execution_id,c.capture_bundle_id,c.producing_fence,c.producing_token_sha256
    FROM canonical c WHERE NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
        WHERE f.execution_id=c.execution_id AND f.producing_fence=c.producing_fence)
)
SELECT h.revision_kind,h.revision_id,o.family_id,o.dataset_id,o.definition_revision_id,o.schema_revision_id,
    o.execution_id,o.capture_bundle_id,o.producing_fence,o.producing_token_sha256
FROM homes h LEFT JOIN origins o USING(revision_kind,revision_id)
$fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.lock_custom_import_materialization_storage(p_family_id bigint,p_writable boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE f __CONTROL__.custom_import_snapshot_family; namespace text; relation_name text;
BEGIN
    IF p_writable IS NULL THEN RAISE EXCEPTION 'custom_import_materialization_storage_mode'; END IF;
    IF p_family_id IS NOT NULL THEN
        SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
        IF f.family_id IS NULL THEN RAISE EXCEPTION 'custom_import_materialization_storage_missing'; END IF;
        IF p_writable THEN
            IF p_family_id IS DISTINCT FROM __CONTROL__.lock_custom_import_writable_snapshot(
                f.execution_id,f.producing_fence,f.producing_token_sha256) THEN
                RAISE EXCEPTION 'custom_import_materialization_storage_mismatch'; END IF;
            RETURN;
        END IF;
        PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(p_family_id);
        namespace:='ci_snapshot_'||p_family_id::text;
    ELSE
        namespace:=__CONTROL_LITERAL__;
    END IF;
    FOREACH relation_name IN ARRAY __RELATION_NAMES__ LOOP
        EXECUTE format('LOCK TABLE %I.%I IN ACCESS SHARE MODE',namespace,relation_name);
    END LOOP;
    IF p_family_id IS NOT NULL THEN
        PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(p_family_id);
    END IF;
END $fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.append_custom_import_revision_home(
    p_family_id bigint,p_root_ids bigint[],p_child_ids bigint[]
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE f __CONTROL__.custom_import_snapshot_family; namespace text; invalid boolean;
BEGIN
    IF p_family_id IS NULL OR p_family_id<=0 OR p_root_ids IS NULL OR p_child_ids IS NULL
        OR cardinality(p_root_ids)+cardinality(p_child_ids)>100000
        OR EXISTS(SELECT 1 FROM (VALUES
            (cardinality(p_root_ids),array_ndims(p_root_ids),array_lower(p_root_ids,1)),
            (cardinality(p_child_ids),array_ndims(p_child_ids),array_lower(p_child_ids,1))
        ) a(size,dimensions,first_index) WHERE a.size>0 AND ROW(a.dimensions,a.first_index) IS DISTINCT FROM ROW(1,1))
        OR EXISTS(SELECT 1 FROM unnest(p_root_ids||p_child_ids) ids(id) WHERE id IS NULL OR id<=0)
        OR (SELECT count(DISTINCT id) FROM unnest(p_root_ids) ids(id))<>cardinality(p_root_ids)
        OR (SELECT count(DISTINCT id) FROM unnest(p_child_ids) ids(id))<>cardinality(p_child_ids) THEN
        RAISE EXCEPTION 'custom_import_revision_home_bounds'; END IF;
    -- Even an empty direct helper call must hold genuine writable authority.
    PERFORM __CONTROL__.lock_custom_import_materialization_storage(p_family_id,true);
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
    namespace:='ci_snapshot_'||p_family_id::text;
    EXECUTE format($query$
        SELECT EXISTS(
            SELECT 1 FROM unnest($1) ids(id)
            LEFT JOIN %1$I.custom_import_root_revision r ON r.root_revision_id=ids.id
            LEFT JOIN %1$I.custom_import_pack p ON p.pack_id=r.pack_id
            WHERE ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,
                p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.execution_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM
                ROW(($3).dataset_id,($3).definition_revision_id,($3).schema_revision_id,
                    ($3).dataset_id,($3).definition_revision_id,($3).schema_revision_id,($3).execution_id,
                    ($3).capture_bundle_id,($3).producing_fence,($3).producing_token_sha256)
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_root_revision old WHERE old.root_revision_id=ids.id)
            UNION ALL
            SELECT 1 FROM unnest($2) ids(id)
            LEFT JOIN %1$I.custom_import_child_revision r ON r.child_revision_id=ids.id
            LEFT JOIN %1$I.custom_import_pack p ON p.pack_id=r.pack_id
            WHERE ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,
                p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.execution_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM
                ROW(($3).dataset_id,($3).definition_revision_id,($3).schema_revision_id,
                    ($3).dataset_id,($3).definition_revision_id,($3).schema_revision_id,($3).execution_id,
                    ($3).capture_bundle_id,($3).producing_fence,($3).producing_token_sha256)
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_child_revision old WHERE old.child_revision_id=ids.id)
        )
    $query$,namespace) INTO invalid USING p_root_ids,p_child_ids,f;
    IF invalid THEN RAISE EXCEPTION 'custom_import_revision_home_lineage'; END IF;
    WITH fresh AS (
        SELECT 1::smallint AS revision_kind,id FROM unnest(p_root_ids) ids(id)
        UNION ALL SELECT 2::smallint,id FROM unnest(p_child_ids) ids(id)
    ), batches AS (
        SELECT revision_kind,range_agg(int8range(id,
            CASE WHEN id=9223372036854775807 THEN NULL ELSE id+1 END,'[)')) AS revision_ids
        FROM fresh GROUP BY revision_kind
    )
    INSERT INTO __CONTROL__.custom_import_revision_home(family_id,revision_kind,revision_ids)
        SELECT p_family_id,revision_kind,revision_ids FROM batches ORDER BY revision_kind;
    PERFORM __CONTROL__.lock_custom_import_materialization_storage(p_family_id,true);
END $fn$;
