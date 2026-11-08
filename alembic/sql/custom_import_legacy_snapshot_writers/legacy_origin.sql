-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE TABLE __CANDIDATE__.legacy_copy_origin (
    kind varchar(5) NOT NULL,
    revision_id bigint NOT NULL,
    family_revision_id bigint NOT NULL,
    root_record_id bigint NOT NULL,
    root_revision_id bigint NOT NULL,
    collection_slot smallint,
    base_generation_id bigint NOT NULL,
    base_family_revision_id bigint NOT NULL,
    base_root_revision_id bigint NOT NULL,
    base_child_revision_id bigint,
    PRIMARY KEY (kind,revision_id),
    CHECK (revision_id>0 AND family_revision_id>0 AND root_record_id>0 AND root_revision_id>0
        AND base_generation_id>0 AND base_family_revision_id>0 AND base_root_revision_id>0),
    CHECK ((kind='root' AND revision_id=root_revision_id
            AND collection_slot IS NULL AND base_child_revision_id IS NULL)
        OR (kind='child' AND collection_slot IS NOT NULL AND collection_slot>0
            AND base_child_revision_id IS NOT NULL AND base_child_revision_id>0))
)

-- statement boundary --

CREATE UNIQUE INDEX legacy_copy_root_family_key ON __CANDIDATE__.legacy_copy_origin (family_revision_id)
    WHERE kind='root'

-- statement boundary --

CREATE UNIQUE INDEX legacy_copy_child_source_key ON __CANDIDATE__.legacy_copy_origin
    (family_revision_id,collection_slot,base_child_revision_id) WHERE kind='child'
