# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical predecessor metadata for an exact-current atomic publication."""

import json

from db.plan_release_storage import storage_relation


def root_input_fence_sql(schema, dataset_sql, roots_sql=None, *, variant="uhc_flex_practitioner"):
    """Project immutable data fields, independent of database-local catalog identities."""
    if variant not in {"uhc_flex_practitioner", "rooted_combined"}:
        raise ValueError("rooted_variant_invalid")
    root = (
        "provider_directory_uhc_flex_practitioner_dataset"
        if variant == "uhc_flex_practitioner"
        else "provider_directory_rooted_graph_dataset"
    )
    header = storage_relation(schema, root)
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    mutable = "ARRAY['status','is_current','created_at','validated_at','published_at','superseded_at','candidate_storage_json']::text[]"
    return f"""SELECT jsonb_build_object('dataset_id',h.dataset_id,'variant','{variant}',
        'header',to_jsonb(h)-{mutable},'parent',to_jsonb(p)-{mutable})
        FROM {header} h JOIN {parent} p USING(dataset_id) WHERE h.dataset_id={dataset_sql}"""


def input_fence_query(schema, dataset_sql, oids_sql=None, *, include_bodies=False):
    """Render the legacy predecessor's logical data projection."""
    return root_input_fence_sql(schema, dataset_sql)


async def capture_input_fence(database, schema, leaves, dataset_id):
    """Read immutable logical metadata after complete content validation."""
    encoded = await database.scalar(root_input_fence_sql(schema, ":dataset_id"), dataset_id=dataset_id)
    document = json.loads(encoded) if isinstance(encoded, str) else encoded
    if not isinstance(document, dict):
        raise RuntimeError("provider_directory_rooted_graph_predecessor_invalid")
    return json.dumps(document, sort_keys=True, separators=(",", ":"))
