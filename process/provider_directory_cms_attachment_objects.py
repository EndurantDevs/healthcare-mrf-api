# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain attachment storage and object dependencies under the fixed finisher's seal.

These catalog comparisons supply no custody or authority. The caller authenticates
the protected allocation/seal and keeps its original transaction and exclusive
payload locks through the conversion, attachment and outer commit.
"""

from copy import deepcopy

from process import provider_directory_cms_allocation_semantics as semantics
from process.provider_directory_cms_preparation_custody import _decoded

_OBJECTS_SQL = """WITH roots(oid) AS (SELECT unnest($1::oid[])), classes(oid) AS (
 SELECT oid FROM roots UNION SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid=ANY($1::oid[])
), objects(classid,objid) AS (
 SELECT 'pg_catalog.pg_class'::regclass,oid FROM classes
 UNION SELECT 'pg_catalog.pg_constraint'::regclass,oid FROM pg_catalog.pg_constraint WHERE conrelid=ANY($1::oid[])
 UNION SELECT 'pg_catalog.pg_attrdef'::regclass,oid FROM pg_catalog.pg_attrdef WHERE adrelid=ANY($1::oid[])
)
SELECT pg_catalog.jsonb_build_object(
 'storage',(SELECT jsonb_agg(jsonb_build_object('oid',c.oid::bigint,'kind',c.relkind::text,
 'filenode',pg_catalog.pg_relation_filenode(c.oid)::bigint,'owner',c.relowner::bigint,
 'namespace',c.relnamespace::bigint,'tablespace',c.reltablespace::bigint,'method',c.relam::bigint,
 'options',c.reloptions,'replica',c.relreplident::text) ORDER BY c.oid)
 FROM classes JOIN pg_catalog.pg_class c USING(oid)),
 'objects',(SELECT jsonb_agg(jsonb_build_array(classid::bigint,objid::bigint) ORDER BY classid,objid) FROM objects),
 'dependencies',COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY d.classid,d.objid,d.objsubid,
 d.refclassid,d.refobjid,d.refobjsubid,d.deptype) FROM pg_catalog.pg_depend d
 WHERE EXISTS(SELECT FROM objects o WHERE (d.classid,d.objid)=(o.classid,o.objid)
 OR (d.refclassid,d.refobjid)=(o.classid,o.objid))),'[]'::jsonb),
 'shared_dependencies',COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY d.classid,d.objid,d.objsubid,
 d.refclassid,d.refobjid,d.deptype) FROM pg_catalog.pg_shdepend d
 WHERE d.dbid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
 AND EXISTS(SELECT FROM objects o WHERE (d.classid,d.objid)=(o.classid,o.objid))),'[]'::jsonb))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_attachment_objects_" + code)


async def capture_attachment_objects(connection, preimages):
    """Read only the two already authenticated payloads and their existing native objects."""
    oids = sorted(entry["oid"] for entry in preimages.values())
    _require(len(oids) == len(set(oids)) == 2, "two_payloads_required")
    observed = _decoded(await connection.fetchval(_OBJECTS_SQL, oids))
    expected_classes = {
        oid
        for entry in preimages.values()
        for oid in (entry["oid"], *(index["class"][0] for index in entry["native"]["indexes"]))
    }
    _require(
        type(observed) is dict and set(observed) == {"storage", "objects", "dependencies", "shared_dependencies"},
        "inventory_changed",
    )
    storage = observed["storage"]
    _require(
        type(storage) is list
        and len(storage) == len(expected_classes)
        and {entry["oid"] for entry in storage} == expected_classes
        and all(
            entry["kind"] in {"r", "i"} and type(entry["filenode"]) is int and entry["filenode"] > 0
            for entry in storage
        ),
        "storage_changed",
    )
    return deepcopy(observed)


def _edge(classid, oid, reference_class, reference_oid, kind):
    return dict(
        classid=classid,
        objid=str(oid),
        objsubid=0,
        refclassid=reference_class,
        refobjid=str(reference_oid),
        refobjsubid=0,
        deptype=kind,
    )


def _dependency_key(entry):
    return tuple(
        int(entry[field]) for field in ("classid", "objid", "objsubid", "refclassid", "refobjid", "refobjsubid")
    ) + (entry["deptype"],)


def attached_object_postimage(original, preimages, parents):
    """Add only native table/index/PK partition dependencies from the protected parents."""
    expected = deepcopy(original)
    additions = []
    _require(set(preimages) == set(parents) and len(preimages) == 2, "parents_required")
    for root, child in preimages.items():
        parent = parents[root]
        additions.append(_edge("1259", child["oid"], "1259", parent["oid"], "a"))
        for index in child["native"]["indexes"]:
            matches = [
                pin
                for pin in parent["native"]["indexes"]
                if semantics._index_signature(pin) == semantics._index_signature(index)
            ]
            _require(len(matches) == 1, "index_parent_changed")
            additions.extend(
                (
                    _edge("1259", index["class"][0], "1259", matches[0]["class"][0], "P"),
                    _edge("1259", index["class"][0], "1259", child["oid"], "S"),
                )
            )
        for constraint, definition in child["native"]["constraints"]:
            if constraint["contype"] not in {"p", "u"}:
                continue
            matches = [
                pin
                for pin, text in parent["native"]["constraints"]
                if pin["contype"] == constraint["contype"] and text == definition
            ]
            _require(len(matches) == 1, "constraint_parent_changed")
            additions.extend(
                (
                    _edge("2606", constraint["oid"], "2606", matches[0]["oid"], "P"),
                    _edge("2606", constraint["oid"], "1259", child["oid"], "S"),
                )
            )
    original_dependency_keys = {_dependency_key(edge) for edge in expected["dependencies"]}
    _require(
        not original_dependency_keys.intersection(_dependency_key(edge) for edge in additions),
        "partition_dependency_already_present",
    )
    expected["dependencies"] = sorted([*expected["dependencies"], *additions], key=_dependency_key)
    return expected


async def require_attachment_objects(connection, original, preimages, parents):
    """Reject physical replacement, new objects, ACL edges or unplanned dependencies."""
    expected = attached_object_postimage(original, preimages, parents)
    _require(await capture_attachment_objects(connection, preimages) == expected, "postimage_changed")
    return expected


def _retained_oids(tables, expected):
    """Link retained physical evidence to exactly the two authenticated attached heaps."""
    _require(
        type(tables) is dict
        and len(tables) == 2
        and all(type(entry.get("oid")) is int and 0 < entry["oid"] < 2**32 for entry in tables.values()),
        "two_payloads_required",
    )
    oids = sorted(entry["oid"] for entry in tables.values())
    _require(len(oids) == len(set(oids)) == 2, "two_payloads_required")
    _require(
        type(expected) is dict
        and set(expected) == {"storage", "objects", "dependencies", "shared_dependencies"}
        and all(type(expected[key]) is list for key in expected),
        "retained_postimage_required",
    )
    storage = expected["storage"]
    _require(
        {entry["oid"] for entry in storage if entry["kind"] == "r"} == set(oids)
        and len({entry["oid"] for entry in storage}) == len(storage)
        and all(
            entry["kind"] in {"r", "i"}
            and type(entry["oid"]) is int
            and 0 < entry["oid"] < 2**32
            and type(entry["filenode"]) is int
            and entry["filenode"] > 0
            for entry in storage
        ),
        "retained_storage_changed",
    )
    return oids


async def require_retained_attachment_objects(connection, tables, expected):
    """Compare the protected terminal postimage in a fresh pinned recovery transaction."""
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    retained_tables, retained_objects = deepcopy(tables), deepcopy(expected)
    oids = _retained_oids(retained_tables, retained_objects)
    _require(_decoded(await connection.fetchval(_OBJECTS_SQL, oids)) == retained_objects, "postimage_changed")
    _require(tables == retained_tables and expected == retained_objects, "inputs_changed")
