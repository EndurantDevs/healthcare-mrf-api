# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native metadata bounds shared by the model and forward inventory migration."""

INVENTORY_COMPONENT = "registry_source_inventory"
INVENTORY_TABLE = "network_source_inventory"
MAX_REFERENCE_BYTES = 4096

_ROOT_FIELDS = (
    "revision",
    "entry_count",
    "contribution_count",
    "derivation_count",
    "encoded_bytes",
    "recipe_bytes",
    "recipes_sha256",
    "entries_sha256",
)


def _integer_root_field(name, minimum, maximum):
    value = f"source_recipes_json#>>'{{root,{name}}}'"
    return (
        f"CASE WHEN jsonb_typeof(source_recipes_json#>'{{root,{name}}}')='number' "
        f"AND {value} ~ '^(0|[1-9][0-9]*)$' "
        f"THEN ({value})::numeric BETWEEN {minimum} AND {maximum} ELSE false END"
    )


def source_recipe_storage_check():
    """Keep legacy arrays unchanged and admit only the closed, bounded reference shape.

    This constraint validates metadata syntax, not inventory contents or custody.
    CASE protects array operations from object inputs; COALESCE rejects missing keys.
    """
    root_keys = "ARRAY[" + ",".join("'" + name + "'" for name in _ROOT_FIELDS) + "]::text[]"
    numeric = " AND ".join(
        _integer_root_field(name, minimum, maximum)
        for name, minimum, maximum in (
            ("revision", 1, 1),
            ("entry_count", 0, 1000),
            ("contribution_count", 0, 1000),
            ("derivation_count", 0, 1000),
            ("encoded_bytes", 2, 67108864),
            ("recipe_bytes", 2, 67108864),
        )
    )
    return f"""COALESCE(CASE jsonb_typeof(source_recipes_json)
      WHEN 'array' THEN jsonb_array_length(source_recipes_json)<=100
        AND octet_length(source_recipes_json::text)<=1048576
      WHEN 'object' THEN octet_length(source_recipes_json::text)<={MAX_REFERENCE_BYTES}
        AND source_recipes_json->>'component'='{INVENTORY_COMPONENT}'
        AND source_recipes_json->'revision'='1'::jsonb
        AND source_recipes_json->>'table_name'='{INVENTORY_TABLE}'
        AND source_recipes_json ?& ARRAY['component','revision','table_name','root']::text[]
        AND source_recipes_json-ARRAY['component','revision','table_name','root']::text[]='{{}}'::jsonb
        AND jsonb_typeof(source_recipes_json->'root')='object'
        AND (source_recipes_json->'root') ?& {root_keys}
        AND (source_recipes_json->'root')-{root_keys}='{{}}'::jsonb
        AND {numeric}
        AND (source_recipes_json#>>'{{root,contribution_count}}')::numeric
          <=(source_recipes_json#>>'{{root,entry_count}}')::numeric
        AND (source_recipes_json#>>'{{root,derivation_count}}')::numeric
          <=(source_recipes_json#>>'{{root,entry_count}}')::numeric
        AND (source_recipes_json#>>'{{root,recipe_bytes}}')::numeric
          <=(source_recipes_json#>>'{{root,encoded_bytes}}')::numeric
        AND jsonb_typeof(source_recipes_json#>'{{root,recipes_sha256}}')='string'
        AND source_recipes_json#>>'{{root,recipes_sha256}}' ~ '^[0-9a-f]{{64}}$'
        AND jsonb_typeof(source_recipes_json#>'{{root,entries_sha256}}')='string'
        AND source_recipes_json#>>'{{root,entries_sha256}}' ~ '^[0-9a-f]{{64}}$'
      ELSE false END,false)"""
