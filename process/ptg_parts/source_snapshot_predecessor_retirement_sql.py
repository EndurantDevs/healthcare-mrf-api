# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL templates for exact predecessor-retention retirement."""

MRF_CONTEXT_QUERY_TEMPLATES = (
    (
        "snapshot_records",
        """
        SELECT snapshot_id, previous_snapshot_id, status, manifest
          FROM __SCHEMA__.ptg2_snapshot
         WHERE snapshot_id IN (
                   :current_snapshot_id,
                   :predecessor_snapshot_id
               )
         ORDER BY snapshot_id
         FOR UPDATE
        """,
    ),
    (
        "source_pointer_records",
        """
        SELECT source_key, snapshot_id, previous_snapshot_id
          FROM __SCHEMA__.ptg2_current_source_snapshot
         WHERE source_key = :source_key
            OR snapshot_id = :predecessor_snapshot_id
            OR previous_snapshot_id = :predecessor_snapshot_id
         ORDER BY source_key
         FOR UPDATE
        """,
    ),
    (
        "plan_pointer_records",
        """
        SELECT plan_source_key, source_key, snapshot_id,
               previous_snapshot_id
          FROM __SCHEMA__.ptg2_current_plan_source
         WHERE source_key = :source_key
            OR snapshot_id = :predecessor_snapshot_id
            OR previous_snapshot_id = :predecessor_snapshot_id
         ORDER BY plan_source_key
         FOR UPDATE
        """,
    ),
    (
        "global_pointer_records",
        """
        SELECT slot, snapshot_id, previous_snapshot_id
          FROM __SCHEMA__.ptg2_current_snapshot
         WHERE snapshot_id IN (
                   :current_snapshot_id,
                   :predecessor_snapshot_id
               )
            OR previous_snapshot_id = :predecessor_snapshot_id
         ORDER BY slot
         FOR UPDATE
        """,
    ),
    (
        "pin_records",
        """
        SELECT owner_type, owner_id, snapshot_id, reason
          FROM __SCHEMA__.ptg2_snapshot_pin
         WHERE snapshot_id = :predecessor_snapshot_id
         ORDER BY owner_type, owner_id
         FOR UPDATE
        """,
    ),
    (
        "release_binding_records",
        """
        SELECT serving_revision_id, role, binding_ordinal
          FROM __SCHEMA__.plan_release_snapshot_binding
         WHERE snapshot_id = :predecessor_snapshot_id
         ORDER BY serving_revision_id, role, binding_ordinal
         FOR UPDATE
        """,
    ),
)
CONTROL_CONTEXT_QUERY_TEMPLATES = (
    (
        "control_release_binding_records",
        """
        SELECT release_binding_id, serving_revision_id, role, ordinal
          FROM __SCHEMA__.hp_plan_release_binding
         WHERE snapshot_id = :predecessor_snapshot_id
         ORDER BY release_binding_id
         FOR UPDATE
        """,
    ),
    (
        "control_pin_records",
        """
        SELECT owner_type, owner_id, snapshot_id, source_key, node_id
          FROM __SCHEMA__.hp_snapshot_pin
         WHERE snapshot_id = :predecessor_snapshot_id
         ORDER BY owner_type, owner_id
         FOR UPDATE
        """,
    ),
)
SOURCE_POINTER_UPDATE = """
    UPDATE __SCHEMA__.ptg2_current_source_snapshot
       SET previous_snapshot_id = NULL
     WHERE source_key = :source_key
       AND snapshot_id = :current_snapshot_id
       AND previous_snapshot_id = :predecessor_snapshot_id
    RETURNING source_key
"""
PLAN_POINTER_UPDATE = """
    UPDATE __SCHEMA__.ptg2_current_plan_source
       SET previous_snapshot_id = NULL
     WHERE source_key = :source_key
       AND snapshot_id = :current_snapshot_id
       AND previous_snapshot_id = :predecessor_snapshot_id
    RETURNING plan_source_key
"""
GLOBAL_POINTER_UPDATE = """
    UPDATE __SCHEMA__.ptg2_current_snapshot
       SET previous_snapshot_id = NULL
     WHERE slot = 'current'
       AND snapshot_id = :current_snapshot_id
       AND previous_snapshot_id = :predecessor_snapshot_id
    RETURNING slot
"""
ROLLBACK_PIN_DELETE = """
    DELETE FROM __SCHEMA__.ptg2_snapshot_pin
     WHERE owner_type = :rollback_owner_type
       AND owner_id = :rollback_owner_id
       AND snapshot_id = :predecessor_snapshot_id
    RETURNING owner_id
"""
POSTCHECK_TEMPLATE = """
    SELECT
        (SELECT COUNT(*) FROM __SCHEMA__.ptg2_current_snapshot
          WHERE snapshot_id = :predecessor_snapshot_id
             OR previous_snapshot_id = :predecessor_snapshot_id)
            AS global_references,
        (SELECT COUNT(*) FROM __SCHEMA__.ptg2_current_source_snapshot
          WHERE snapshot_id = :predecessor_snapshot_id
             OR previous_snapshot_id = :predecessor_snapshot_id)
            AS source_references,
        (SELECT COUNT(*) FROM __SCHEMA__.ptg2_current_plan_source
          WHERE snapshot_id = :predecessor_snapshot_id
             OR previous_snapshot_id = :predecessor_snapshot_id)
            AS plan_references,
        (SELECT COUNT(*) FROM __SCHEMA__.ptg2_snapshot_pin
          WHERE snapshot_id = :predecessor_snapshot_id)
            AS pin_references,
        (SELECT COUNT(*) FROM __SCHEMA__.plan_release_snapshot_binding
          WHERE snapshot_id = :predecessor_snapshot_id)
            AS release_references,
        (SELECT COUNT(*) FROM __CONTROL_SCHEMA__.hp_plan_release_binding
          WHERE snapshot_id = :predecessor_snapshot_id)
            AS control_release_references,
        (SELECT COUNT(*) FROM __CONTROL_SCHEMA__.hp_snapshot_pin
          WHERE snapshot_id = :predecessor_snapshot_id)
            AS control_pin_references,
        (SELECT COUNT(*) FROM __SCHEMA__.ptg2_snapshot
          WHERE snapshot_id = :current_snapshot_id
            AND previous_snapshot_id = :predecessor_snapshot_id)
            AS preserved_lineage,
        (SELECT COUNT(*) FROM __SCHEMA__.ptg2_current_source_snapshot
          WHERE source_key = :source_key
            AND snapshot_id = :current_snapshot_id
            AND previous_snapshot_id IS NULL)
            AS preserved_current_pointer
"""


__all__ = [
    "CONTROL_CONTEXT_QUERY_TEMPLATES",
    "GLOBAL_POINTER_UPDATE",
    "MRF_CONTEXT_QUERY_TEMPLATES",
    "PLAN_POINTER_UPDATE",
    "POSTCHECK_TEMPLATE",
    "ROLLBACK_PIN_DELETE",
    "SOURCE_POINTER_UPDATE",
]
