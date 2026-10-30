# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native callback contract for initial private registry workers.

This is an authority certificate, not a replacement for migration adoption or
the worker's data/permission validators. Its complete native observation is
bound by the admitting transaction before and after the exact role change.
"""

import re
from collections import defaultdict

from sqlalchemy import types
from sqlalchemy.dialects import postgresql

from db.models.registry_aca_source_notification import RegistryACASourceNotification
from db.registry_fhir_catalog import SNAPSHOT_COLUMNS
from process.network_membership_writer_closure import _NATIVE_WRITER_PRIVILEGES_SQL
from process.registry_management_permissions import _MODELS, _PTG_TABLES, _TABLES

CONTRACT = "registry-native-execution/v1"
# The installed native packages use the same protected-extension trust boundary
# as entity_address_native_publication._require_stage_execution_catalog.
_EXTENSIONS = frozenset(
    (
        "postgis",
        "intarray",
        "btree_gin",
        "btree_gist",
        "pg_trgm",
        "pgcrypto",
        "fuzzystrmatch",
        "address_standardizer",
        "address_standardizer_data_us",
        "pg_stat_statements",
        "pg_prewarm",
    )
)
_CATALOG_CLASSES = {
    "relations": "1259",
    "functions": "1255",
    "types": "1247",
    "operators": "2617",
    "opclasses": "2616",
    "opfamilies": "2753",
    "collations": "3456",
}
_RI_FUNCTIONS = {
    "1654": ("RI_FKey_noaction_del", 9, True),
    "1655": ("RI_FKey_noaction_upd", 17, True),
    "1644": ("RI_FKey_check_ins", 5, False),
    "1645": ("RI_FKey_check_upd", 17, False),
}


class RegistryExecutionCatalogError(ValueError):
    """A native execution dependency differs from the fixed source contract."""


def _require(condition, reason):
    if not condition:
        raise RegistryExecutionCatalogError("registry_execution_catalog_" + reason)


def _native_type(column_type):
    """Map the existing models' closed type set to PostgreSQL native identities."""
    if isinstance(column_type, postgresql.ARRAY):
        oid, modifier = _native_type(column_type.item_type)
        _require(oid in {"1043", "23"}, "model_array_type")
        return {"1043": "1015", "23": "1007"}[oid], modifier
    if isinstance(column_type, types.String):
        if isinstance(column_type, types.Text):
            return "25", -1
        return "1043", -1 if column_type.length is None else column_type.length + 4
    for kind, oid in (
        (types.BigInteger, "20"),
        (types.SmallInteger, "21"),
        (types.Integer, "23"),
        (types.Boolean, "16"),
        (types.Uuid, "2950"),
        (postgresql.JSONB, "3802"),
        (types.Date, "1082"),
        (postgresql.OID, "26"),
    ):
        if isinstance(column_type, kind):
            return oid, -1
    if isinstance(column_type, types.DateTime):
        return ("1184" if column_type.timezone else "1114"), -1
    raise RegistryExecutionCatalogError("registry_execution_catalog_model_type")


def registry_execution_columns():
    """Compile exact registry models and canonical non-ORM native column contracts."""
    expected_by_table = {
        model.__tablename__: tuple(
            (column.name, *_native_type(column.type), not column.nullable) for column in model.__table__.columns
        )
        for model in (*_MODELS, RegistryACASourceNotification)
    }
    receipt_type_by_name = {
        "scope_id": ("2950", -1),
        "capture_id": ("2950", -1),
        "scope_key": ("1043", 68),
        "actor_key": ("1043", 68),
        "idempotency_key": ("1043", 132),
        "approval_sha256": ("1043", 68),
        "hold_sha256": ("1043", 68),
        "approval_json": ("3802", -1),
        "hold_json": ("3802", -1),
        "created_at": ("1184", -1),
    }
    for name, columns in _PTG_TABLES.items():
        expected_by_table[name] = tuple((column, *receipt_type_by_name[column], True) for column in columns)
    # Frozen 20261017010000 registry notification storage has no ORM model.
    expected_by_table["registry_cms_source_notification"] = tuple(
        (name, oid, -1, True)
        for name, oid in (
            ("source_schema", "25"),
            ("receipt_id", "25"),
            ("capture_id", "2950"),
            ("publication_xid", "5069"),
            ("profile_generation_id", "25"),
            ("notification_sha256", "25"),
            ("notification_json", "25"),
            ("created_at", "1184"),
        )
    )
    expected_by_table["registry_fhir_catalog_snapshot"] = _catalog_snapshot_columns()
    _require(
        set(expected_by_table)
        == set(_TABLES) | {"registry_cms_source_notification", "registry_aca_source_notification"},
        "model_roster",
    )
    return expected_by_table


def _catalog_snapshot_columns():
    """Bind the canonical non-ORM snapshot definition to its complete native column contract."""
    columns_by_name = {
        "metadata_generation": ("25", -1, True),
        "schema_name": ("1043", 67, True),
        "custody_json": ("3802", -1, True),
        "network_generation": ("20", -1, False),
        "manifest_sha256": ("25", -1, False),
        "metadata_approved_revision": ("20", -1, True),
        "membership_mapping_sha256": ("25", -1, True),
        "mapping_sha256": ("25", -1, True),
        "standing_grant_sha256": ("25", -1, True),
        "source_vector_json": ("3802", -1, True),
        "projector_version": ("25", -1, True),
        "as_of": ("1082", -1, True),
        "accounting_json": ("3802", -1, True),
        "published_at": ("1184", -1, False),
    }
    _require(tuple(columns_by_name) == SNAPSHOT_COLUMNS, "model_catalog_columns")
    return tuple((name, *column) for name, column in columns_by_name.items())


def _foreign_keys():
    expected_keys = set()
    for model in _MODELS:
        for constraint in model.__table__.foreign_key_constraints:
            _require(not constraint.ondelete and not constraint.onupdate, "model_foreign_key")
            targets = [element.target_fullname.split(".") for element in constraint.elements]
            _require(len({target[-2] for target in targets}) == 1, "model_foreign_key")
            expected_keys.add(
                (
                    model.__tablename__,
                    tuple(element.parent.name for element in constraint.elements),
                    targets[0][-2],
                    tuple(target[-1] for target in targets),
                )
            )
    # Frozen 20261010020000; preserved, never created by this certificate.
    expected_keys.add(
        (
            "registry_ptg_office_hold",
            ("capture_id", "approval_sha256"),
            "registry_ptg_office_approval",
            ("capture_id", "approval_sha256"),
        )
    )
    _require(len(expected_keys) == 5, "model_foreign_key_roster")
    return expected_keys


def _by_oid(native_rows, key="oid"):
    _require(
        type(native_rows) is list and all(type(native_row) is dict and key in native_row for native_row in native_rows),
        "native_rows",
    )
    native_by_oid = {native_row[key]: native_row for native_row in native_rows}
    _require(len(native_by_oid) == len(native_rows), "duplicate_native_row")
    return native_by_oid


def _extension_members(catalog):
    extensions = _by_oid(catalog["extensions"])
    memberships = defaultdict(list)
    for dependency in catalog["dependencies"]:
        if dependency["refclassid"] == "3079" and dependency["deptype"] == "e":
            memberships[(dependency["classid"], dependency["objid"])].append(dependency["refobjid"])

    def native_member(classid, oid):
        """Require one approved package member under native public ownership."""
        members = memberships[(classid, oid)]
        if len(members) != 1 or members[0] not in extensions:
            return False
        extension = extensions[members[0]]
        return (
            extension["extname"] in _EXTENSIONS
            and extension["extnamespace"] == catalog["public_namespace"]["oid"]
            and type(extension["extversion"]) is str
            and bool(extension["extversion"])
        )

    return native_member


def _require_executable_objects(catalog, native_member, relation_ids):
    public_oid = catalog["public_namespace"]["oid"]
    native_namespaces = {"11", catalog["information_namespace"]["oid"]}
    for function in catalog["functions"]:
        _require(
            function["proowner"] == "10"
            and function["prosecdef"] is False
            and (
                function["pronamespace"] in native_namespaces
                and int(function["oid"]) < 16384
                or function["pronamespace"] == public_oid
                and native_member("1255", function["oid"])
            ),
            "function_provenance",
        )
    for key, namespace_field, owner_field in (
        ("types", "typnamespace", "typowner"),
        ("operators", "oprnamespace", "oprowner"),
        ("opclasses", "opcnamespace", "opcowner"),
        ("opfamilies", "opfnamespace", "opfowner"),
        ("collations", "collnamespace", "collowner"),
    ):
        for native_row in catalog[key]:
            if key == "types" and (
                native_row["typrelid"] in relation_ids
                or native_row["typelem"] in {entry["reltype"] for entry in catalog["relations"]}
            ):
                continue
            _require(
                native_row[owner_field] == "10"
                and (
                    native_row[namespace_field] in native_namespaces
                    and int(native_row["oid"]) < 16384
                    or native_row[namespace_field] == public_oid
                    and native_member(_CATALOG_CLASSES[key], native_row["oid"])
                ),
                "object_provenance",
            )
    _require(
        all(native_row["oid"] in {"2", "403", "2742", "783"} for native_row in catalog["access_methods"]),
        "access_method",
    )
    _require(
        all(
            native_row["lanowner"] == "10" and native_row["lanname"] in {"internal", "c", "sql", "plpgsql"}
            for native_row in catalog["languages"]
        ),
        "language",
    )


def _native_oid_census(identities):
    return (
        type(identities) is list
        and all(
            type(oid) is str and re.fullmatch(r"[1-9][0-9]{0,9}", oid) and int(oid) <= 2**32 - 1 for oid in identities
        )
        and identities == sorted(set(identities), key=int)
    )


def _unreachable_public_views(catalog, relations):
    """Use complete native role reach, independent of current schema access."""
    proof = catalog.get("public_view_reachability")
    _require(
        type(proof) is dict
        and set(proof) == {"contract", "workers", "principals", "views"}
        and proof["contract"] == "registry-public-view-reachability/v1"
        and _native_oid_census(proof["workers"])
        and len(proof["workers"]) == 5
        and _native_oid_census(proof["principals"])
        and set(proof["workers"]) <= set(proof["principals"])
        and type(proof["views"]) is list
        and all(
            type(view) is dict
            and set(view) == {"oid", "no_privilege", "no_inbound"}
            and type(view["no_privilege"]) is bool
            and type(view["no_inbound"]) is bool
            for view in proof["views"]
        ),
        "public_view_reachability",
    )
    identities = [view["oid"] for view in proof["views"]]
    expected_view_ids = {
        row["oid"]
        for row in relations.values()
        if row["relnamespace"] == catalog["public_namespace"]["oid"] and row["relkind"] == "v"
    }
    _require(_native_oid_census(identities) and set(identities) == expected_view_ids, "public_view_census")
    return {view["oid"] for view in proof["views"] if view["no_privilege"] and view["no_inbound"]}


def _require_public_relations(catalog, native_member, relations):
    unreachable_views = _unreachable_public_views(catalog, relations)
    parent_by_index = {native_row["indexrelid"]: native_row["indrelid"] for native_row in catalog["indexes"]}
    for relation in relations.values():
        if relation["relnamespace"] != catalog["public_namespace"]["oid"]:
            continue
        parent = parent_by_index.get(relation["oid"])
        member = native_member("1259", relation["oid"])
        if parent is not None:
            member = parent in relations and native_member("1259", parent)
        if relation["relkind"] == "c":
            member = native_member("1247", relation["reltype"])
        _require(
            relation["relowner"] == "10"
            and not relation["relrowsecurity"]
            and not relation["relforcerowsecurity"]
            and (
                member
                or relation["relkind"] == "v"
                and relation["relpersistence"] == "p"
                and relation["oid"] in unreachable_views
            ),
            "public_relation_provenance",
        )


def _require_control_columns(
    heaps_by_name, attributes, expected, identities_by_column, queue_owner_oid, custody_owner_oid
):
    for name, relation in heaps_by_name.items():
        owner = custody_owner_oid if name == "network_candidate_custody" else queue_owner_oid
        _require(
            relation["relowner"] == owner
            and relation["relpersistence"] == "p"
            and not any(relation[key] for key in ("relrowsecurity", "relforcerowsecurity", "relispartition")),
            "relation_custody",
        )
        columns = sorted(attributes[relation["oid"]], key=lambda native_row: native_row["attnum"])
        _require(
            tuple(
                (native_row["attname"], native_row["atttypid"], native_row["atttypmod"], native_row["attnotnull"])
                for native_row in columns
            )
            == expected[name]
            and all(
                not native_row["attisdropped"]
                and not native_row["attgenerated"]
                and native_row["attidentity"]
                == (
                    "a"
                    if (name, native_row["attname"]) in identities_by_column
                    and identities_by_column[(name, native_row["attname"])].identity.always
                    else "d"
                    if (name, native_row["attname"]) in identities_by_column
                    else ""
                )
                and native_row["attislocal"]
                and not native_row["attinhcount"]
                for native_row in columns
            ),
            "column_contract",
        )


def _require_identity_sequences(catalog, relations, heaps_by_name, attributes, identities_by_column):
    namespace_oid = catalog["namespace"]["oid"]
    sequences = _by_oid(catalog["sequences"], "seqrelid")
    for (table_name, column_name), column in identities_by_column.items():
        relation = heaps_by_name[table_name]
        sequence = next(
            native_row
            for native_row in relations.values()
            if native_row["relnamespace"] == namespace_oid
            and native_row["relname"] == table_name + "_" + column_name + "_seq"
        )
        identity, (type_oid, _modifier) = column.identity, _native_type(column.type)
        _require(
            sequences.get(sequence["oid"])
            == {
                "seqrelid": sequence["oid"],
                "seqtypid": type_oid,
                "seqstart": identity.start or 1,
                "seqincrement": identity.increment or 1,
                "seqmax": identity.maxvalue or {"20": 2**63 - 1, "23": 2**31 - 1}[type_oid],
                "seqmin": identity.minvalue or 1,
                "seqcache": identity.cache or 1,
                "seqcycle": bool(identity.cycle),
            },
            "identity_sequence",
        )
        dependencies = [
            native_row
            for native_row in catalog["dependencies"]
            if native_row["classid"] == "1259"
            and native_row["objid"] == sequence["oid"]
            and native_row["deptype"] in {"a", "i"}
        ]
        number = next(
            native_row["attnum"] for native_row in attributes[relation["oid"]] if native_row["attname"] == column_name
        )
        _require(
            dependencies
            == [
                {
                    "classid": "1259",
                    "objid": sequence["oid"],
                    "objsubid": 0,
                    "refclassid": "1259",
                    "refobjid": relation["oid"],
                    "refobjsubid": number,
                    "deptype": "i",
                }
            ],
            "identity_dependency",
        )


def _require_storage_owners(catalog, relations):
    for index in catalog["indexes"]:
        _require(
            index["indexrelid"] in relations
            and index["indrelid"] in relations
            and relations[index["indexrelid"]]["relowner"] == relations[index["indrelid"]]["relowner"],
            "index_custody",
        )
    for relation in relations.values():
        if relation["reltoastrelid"] != "0":
            _require(
                relation["reltoastrelid"] in relations
                and relations[relation["reltoastrelid"]]["relowner"] == relation["relowner"],
                "toast_custody",
            )


def _require_control_relations(catalog, expected, queue_owner_oid, custody_owner_oid):
    namespace_oid = catalog["namespace"]["oid"]
    relations = _by_oid(catalog["relations"])
    heaps_by_name = {
        native_row["relname"]: native_row
        for native_row in relations.values()
        if native_row["relnamespace"] == namespace_oid and native_row["relkind"] == "r"
    }
    _require(set(heaps_by_name) == set(expected), "relation_roster")
    attributes = defaultdict(list)
    identities_by_column = {
        (model.__tablename__, column.name): column
        for model in _MODELS
        for column in model.__table__.columns
        if column.identity is not None
    }
    for column in catalog["attributes"]:
        if column["attnum"] > 0:
            attributes[column["attrelid"]].append(column)
    _require_control_columns(
        heaps_by_name, attributes, expected, identities_by_column, queue_owner_oid, custody_owner_oid
    )
    sequence_names = {
        "network_registry_identity_network_id_seq",
        "network_serving_manifest_generation_id_seq",
        "registry_approval_history_approved_revision_seq",
    }
    _require(
        {
            native_row["relname"]
            for native_row in relations.values()
            if native_row["relnamespace"] == namespace_oid and native_row["relkind"] == "S"
        }
        == sequence_names,
        "sequence_roster",
    )
    _require(
        all(
            native_row["relkind"] in {"r", "i", "S"}
            for native_row in relations.values()
            if native_row["relnamespace"] == namespace_oid
        ),
        "relation_kind",
    )
    for relation in relations.values():
        if relation["relnamespace"] == namespace_oid and relation["relkind"] == "S":
            _require(relation["relowner"] == queue_owner_oid, "sequence_custody")
    _require_identity_sequences(catalog, relations, heaps_by_name, attributes, identities_by_column)
    _require_storage_owners(catalog, relations)
    return relations, heaps_by_name, attributes


def _require_foreign_keys(catalog, relations, attributes):
    column_by_number = {
        (oid, native_row["attnum"]): native_row["attname"]
        for oid, native_rows in attributes.items()
        for native_row in native_rows
    }
    foreign_by_oid = {
        native_row["oid"]: native_row for native_row in catalog["constraints"] if native_row["contype"] == "f"
    }
    observed_keys = set()
    for key in foreign_by_oid.values():
        _require(key["conrelid"] in relations and key["confrelid"] in relations, "foreign_key_endpoint")
        observed_keys.add(
            (
                relations[key["conrelid"]]["relname"],
                tuple(column_by_number[(key["conrelid"], number)] for number in key["conkey"]),
                relations[key["confrelid"]]["relname"],
                tuple(column_by_number[(key["confrelid"], number)] for number in key["confkey"]),
            )
        )
        _require(
            key["convalidated"]
            and key["conenforced"]
            and not key["condeferrable"]
            and not key["condeferred"]
            and key["confupdtype"] == key["confdeltype"] == "a"
            and key["confmatchtype"] == "s"
            and key["conparentid"] == "0",
            "foreign_key_contract",
        )
    _require(observed_keys == _foreign_keys() and len(foreign_by_oid) == len(observed_keys), "foreign_key_roster")
    _require(len(catalog["triggers"]) == 4 * len(foreign_by_oid), "trigger_roster")
    seen_pairs = set()
    for trigger in catalog["triggers"]:
        _require(trigger["tgconstraint"] in foreign_by_oid and trigger["tgfoid"] in _RI_FUNCTIONS, "trigger_callback")
        key = foreign_by_oid[trigger["tgconstraint"]]
        name, kind, parent = _RI_FUNCTIONS[trigger["tgfoid"]]
        functions = _by_oid(catalog["functions"])
        _require(trigger["tgfoid"] in functions and functions[trigger["tgfoid"]]["proname"] == name, "trigger_function")
        pair = (key["oid"], trigger["tgfoid"])
        _require(
            pair not in seen_pairs
            and trigger["tgisinternal"]
            and trigger["tgtype"] == kind
            and trigger["tgrelid"] == key["confrelid" if parent else "conrelid"]
            and trigger["tgconstrrelid"] == key["conrelid" if parent else "confrelid"]
            and trigger["tgconstrindid"] == key["conindid"]
            and trigger["tgenabled"] == "O"
            and not trigger["tgdeferrable"]
            and not trigger["tginitdeferred"]
            and trigger["tgnargs"] == 0
            and not trigger["tgattr"]
            and trigger["tgqual"] is None
            and trigger["tgargs"] in (r"\x", r"\\x")
            and trigger["tgoldtable"] is None
            and trigger["tgnewtable"] is None,
            "trigger_contract",
        )
        seen_pairs.add(pair)


def _require_archive_identity(document, namespace, heaps_by_name, relations):
    oid = namespace["oid"]
    from process.network_legacy_membership_source import PinnedACAMembershipSource, _validated_native_archive
    from process.registry_aca_source_recipe import _BATCH_PREFIX, _PREFIX
    from process.registry_source_recipe_store import decode_registry_source_recipes

    prefix = next((prefix for prefix in (_BATCH_PREFIX, _PREFIX) if document.startswith(prefix)), None)
    _require(prefix is not None, "archive_receipt")
    recipes = decode_registry_source_recipes(document[len(prefix) :])
    _require(
        bool(recipes) and all(type(recipe.source_pin) is PinnedACAMembershipSource for recipe in recipes),
        "archive_kind",
    )
    for recipe in recipes:
        ownership, validation = _validated_native_archive(recipe.source_pin.prepared, recipe.source_pin.validation)
        expected_heaps_by_name = dict(ownership.relation_oids)
        if ownership.auxiliary_oid is not None:
            expected_heaps_by_name["mrf_canonical_address"] = ownership.auxiliary_oid
        _require(
            ownership.schema_name == namespace["nspname"]
            and str(ownership.schema_oid) == oid
            and str(validation.sealed_owner_oid) == namespace["nspowner"]
            and {table: str(table_oid) for table, table_oid in expected_heaps_by_name.items()}
            == {table: relation["oid"] for table, relation in heaps_by_name.items()}
            and {sequence: str(sequence_oid) for sequence, sequence_oid, _, _ in ownership.sequence_oids}
            == {
                native_row["relname"]: native_row["oid"]
                for native_row in relations.values()
                if native_row["relnamespace"] == oid and native_row["relkind"] == "S"
            },
            "archive_identity",
        )


def _require_retained_fhir_input(catalog, namespace, document, heaps_by_name):
    """Certify the existing closed capture receipt against its complete native metadata."""
    import json

    from process.network_fhir_retained_capture import _require_indexes
    from process.network_fhir_retained_capture_contract import TABLES, canonical, digest, validate_capture

    prefix = "retained-normalized-fhir-v1:"
    _require(document.startswith(prefix) and len(document.encode()) <= 65536 + len(prefix), "fhir_receipt")
    capture = validate_capture(json.loads(document[len(prefix) :]))
    _require(document == prefix + canonical(capture.as_dict()), "fhir_receipt_encoding")
    observed = catalog.get("retained_fhir_catalogs", {}).get(namespace["oid"])
    _require(type(observed) is dict and observed.get("closed") is True, "fhir_native_closure")
    native_relations = observed["catalog"]
    _require(
        capture.schema_name == namespace["nspname"]
        and str(capture.schema_oid) == namespace["oid"]
        and capture.owner_role == observed["owner_role"]
        and {name: str(oid) for name, oid in capture.relation_oids}
        == {name: native_row["oid"] for name, native_row in heaps_by_name.items()}
        and set(heaps_by_name) == set(TABLES)
        and type(native_relations) is list
        and tuple(native_row["relname"] for native_row in native_relations) == tuple(sorted(TABLES))
        and all(
            str(native_row["nspowner"]) == namespace["nspowner"]
            and not any(native_row[key] for key in ("relhastriggers", "defaults", "inherited"))
            for native_row in native_relations
        )
        and digest(native_relations) == capture.catalog_sha256,
        "fhir_identity",
    )
    _require_indexes(native_relations, int(namespace["nspowner"]), capture.schema_name)
    _require_retained_fhir_columns(catalog, heaps_by_name)
    _require(observed.get("cursor_closed") is True, "fhir_cursor")


def _require_retained_fhir_columns(catalog, heaps_by_name):
    """Retained input columns have only the same built-in, ordinary native types as capture."""
    types_by_oid = _by_oid(catalog["types"])
    ids = {row["oid"] for row in heaps_by_name.values()}
    columns = [row for row in catalog["attributes"] if row["attrelid"] in ids and row["attnum"] > 0]
    _require(
        bool(columns)
        and all(
            not row["attisdropped"]
            and not row["attgenerated"]
            and not row["attidentity"]
            and not row["atthasdef"]
            and row["atttypid"] in types_by_oid
            and types_by_oid[row["atttypid"]]["typnamespace"] == "11"
            and row["attcollation"] == types_by_oid[row["atttypid"]]["typcollation"]
            for row in columns
        ),
        "fhir_columns",
    )


def _require_private_read_surfaces(catalog, relations):
    """Read-only retained inputs cannot introduce a function or implicit callback."""
    private_by_oid = {native_row["oid"]: native_row for native_row in catalog["private_namespaces"]}
    comments_by_oid = {native_row["objoid"]: native_row["description"] for native_row in catalog["private_comments"]}
    for oid, namespace in private_by_oid.items():
        name = namespace["nspname"]
        _require(
            re.fullmatch(r"(?:network_bootstrap|reference_family_archive|registry_fhir_capture)_[0-9a-f]{32}", name),
            "private_namespace",
        )
        heaps_by_name = {
            native_row["relname"]: native_row
            for native_row in relations.values()
            if native_row["relnamespace"] == oid and native_row["relkind"] == "r"
        }
        _require(
            oid in comments_by_oid
            and type(comments_by_oid[oid]) is str
            and len(comments_by_oid[oid].encode()) <= 2097152,
            "private_receipt",
        )
        if name.startswith("network_bootstrap_"):
            from process.network_bootstrap_sources import _decode, _schema_name

            receipt = _decode(comments_by_oid[oid])
            _require(
                _schema_name(receipt.specification) == name
                and str(receipt.schema_oid) == oid
                and str(receipt.owner_oid) == namespace["nspowner"]
                and {table: str(table_oid) for table, table_oid in receipt.relation_oids}
                == {table: relation["oid"] for table, relation in heaps_by_name.items()}
                and set(heaps_by_name) == {"npi", "entity_address_unified"},
                "bootstrap_identity",
            )
        elif name.startswith("registry_fhir_capture_"):
            _require_retained_fhir_input(catalog, namespace, comments_by_oid[oid], heaps_by_name)
        else:
            _require_archive_identity(comments_by_oid[oid], namespace, heaps_by_name, relations)
        _require(
            all(
                native_row["relowner"] == namespace["nspowner"]
                and native_row["relkind"] in {"r", "i", "S"}
                and native_row["relpersistence"] == "p"
                and not native_row["relrowsecurity"]
                and not native_row["relforcerowsecurity"]
                and not native_row["relispartition"]
                for native_row in relations.values()
                if native_row["relnamespace"] == oid
            ),
            "private_storage",
        )
    private_relations = {
        native_row["oid"] for native_row in relations.values() if native_row["relnamespace"] in private_by_oid
    }
    _require(
        not any(native_row["tgrelid"] in private_relations for native_row in catalog["triggers"])
        and not any(native_row["ev_class"] in private_relations for native_row in catalog["rules"])
        and not any(native_row["pronamespace"] in private_by_oid for native_row in catalog["functions"]),
        "private_callback",
    )


def _require_complete_callbacks(catalog):
    inventory_by_class = {classid: set(_by_oid(catalog[key])) for key, classid in _CATALOG_CLASSES.items()}
    inventory_by_class.update({"2605": set(_by_oid(catalog["casts"])), "2601": set(_by_oid(catalog["access_methods"]))})
    for _classid, _oid, reference_class, reference_oid in catalog["callback_edges"]:
        if reference_class in inventory_by_class and reference_class != "1259":
            _require(reference_oid in inventory_by_class[reference_class], "incomplete_callback")
    _require(len(catalog["functions"]) == len(_by_oid(catalog["functions"])), "function_roster")


def require_registry_execution_catalog(catalog, *, control_schema, queue_owner_oid, custody_owner_oid):
    """Validate only this source-owned native surface; accept no caller allowlist."""
    _require(type(catalog) is dict and catalog.get("contract") == CONTRACT, "contract")
    _require(
        type(catalog.get("server_version")) is str
        and catalog["server_version"].isdigit()
        and 180000 <= int(catalog["server_version"]) < 190000,
        "server_version",
    )
    _require(
        catalog["namespace"]["nspname"] == control_schema
        and catalog["namespace"]["nspowner"] == queue_owner_oid
        and catalog["public_namespace"]["nspname"] == "public"
        and catalog["information_namespace"]["nspname"] == "information_schema"
        and catalog["information_namespace"]["nspowner"] == "10",
        "namespace",
    )
    _require(
        catalog["native_safe"] is True
        and not catalog["event_triggers"]
        and not catalog["policies"]
        and not catalog["inherits"],
        "native_authority",
    )
    native_member = _extension_members(catalog)
    relations, heaps_by_name, attributes = _require_control_relations(
        catalog, registry_execution_columns(), queue_owner_oid, custody_owner_oid
    )
    _require_executable_objects(catalog, native_member, set(relations))
    _require_complete_callbacks(catalog)
    _require_foreign_keys(catalog, relations, attributes)
    _require_private_read_surfaces(catalog, relations)
    control_ids = {native_row["oid"] for native_row in heaps_by_name.values()}
    _require(not any(native_row["ev_class"] in control_ids for native_row in catalog["rules"]), "control_rule")
    _require_public_relations(catalog, native_member, relations)
    _require(
        all(
            native_row["indisvalid"] and native_row["indisready"] and native_row["indislive"]
            for native_row in catalog["indexes"]
        ),
        "index_state",
    )
    return {"contract": CONTRACT, "control_schema": control_schema, "control_relations": sorted(control_ids, key=int)}


# Native metadata and catalog deparsers only; no business function or default runs.
# Resolve only outgoing edges for one closure node, using the native catalog indexes.
# Both closure discovery and the exact callback certificate use the same UNION set.
_REGISTRY_DEPENDENCY_EDGES_SQL = """
 SELECT d.classid,d.objid,d.refclassid,d.refobjid FROM pg_depend d
 WHERE d.classid=node.classid AND d.objid=node.objid
 UNION SELECT 'pg_proc'::regclass,p.oid,'pg_type'::regclass,t FROM pg_proc p CROSS JOIN LATERAL unnest(p.proargtypes::oid[] || COALESCE(p.proallargtypes,ARRAY[]::oid[]) || ARRAY[p.prorettype,p.provariadic]) t WHERE node.classid='pg_proc'::regclass AND p.oid=node.objid AND t<>0
 UNION SELECT 'pg_proc'::regclass,p.oid,'pg_proc'::regclass,p.prosupport FROM pg_proc p WHERE node.classid='pg_proc'::regclass AND p.oid=node.objid AND p.prosupport<>0
 UNION SELECT 'pg_proc'::regclass,p.aggfnoid,'pg_proc'::regclass,f::oid FROM pg_aggregate p CROSS JOIN LATERAL unnest(ARRAY[p.aggtransfn,p.aggfinalfn,p.aggcombinefn,p.aggserialfn,p.aggdeserialfn,p.aggmtransfn,p.aggminvtransfn,p.aggmfinalfn]) f WHERE node.classid='pg_proc'::regclass AND p.aggfnoid=node.objid AND f<>0
 UNION SELECT 'pg_type'::regclass,t.oid,'pg_proc'::regclass,f::oid FROM pg_type t CROSS JOIN LATERAL unnest(ARRAY[t.typinput,t.typoutput,t.typreceive,t.typsend,t.typmodin,t.typmodout,t.typsubscript,t.typanalyze]) f WHERE node.classid='pg_type'::regclass AND t.oid=node.objid AND f<>0
 UNION SELECT 'pg_type'::regclass,t.oid,'pg_type'::regclass,f FROM pg_type t CROSS JOIN LATERAL unnest(ARRAY[t.typbasetype,t.typelem,t.typarray]) f WHERE node.classid='pg_type'::regclass AND t.oid=node.objid AND f<>0
 UNION SELECT 'pg_operator'::regclass,o.oid,'pg_proc'::regclass,f::oid FROM pg_operator o CROSS JOIN LATERAL unnest(ARRAY[o.oprcode,o.oprrest,o.oprjoin]) f WHERE node.classid='pg_operator'::regclass AND o.oid=node.objid AND f<>0
 UNION SELECT 'pg_opclass'::regclass,oid,'pg_opfamily'::regclass,opcfamily FROM pg_opclass WHERE node.classid='pg_opclass'::regclass AND oid=node.objid
 UNION SELECT 'pg_opfamily'::regclass,amprocfamily,'pg_proc'::regclass,amproc::oid FROM pg_amproc WHERE node.classid='pg_opfamily'::regclass AND amprocfamily=node.objid
 UNION SELECT 'pg_opfamily'::regclass,amopfamily,'pg_operator'::regclass,amopopr FROM pg_amop WHERE node.classid='pg_opfamily'::regclass AND amopfamily=node.objid
 UNION SELECT 'pg_trigger'::regclass,oid,'pg_proc'::regclass,tgfoid FROM pg_trigger WHERE node.classid='pg_trigger'::regclass AND oid=node.objid
 UNION SELECT 'pg_class'::regclass,a.attrelid,'pg_type'::regclass,a.atttypid FROM pg_attribute a WHERE node.classid='pg_class'::regclass AND a.attrelid=node.objid AND a.attnum>0 AND NOT a.attisdropped
 UNION SELECT 'pg_class'::regclass,i.indexrelid,'pg_opclass'::regclass,c FROM pg_index i CROSS JOIN LATERAL unnest(i.indclass::oid[]) c WHERE node.classid='pg_class'::regclass AND i.indexrelid=node.objid
 UNION SELECT 'pg_class'::regclass,a.attrelid,'pg_collation'::regclass,a.attcollation FROM pg_attribute a WHERE node.classid='pg_class'::regclass AND a.attrelid=node.objid AND a.attnum>0 AND a.attcollation<>0
 UNION SELECT 'pg_class'::regclass,oid,'pg_am'::regclass,relam FROM pg_class WHERE node.classid='pg_class'::regclass AND oid=node.objid AND relam<>0
 UNION SELECT 'pg_am'::regclass,oid,'pg_proc'::regclass,amhandler::oid FROM pg_am WHERE node.classid='pg_am'::regclass AND oid=node.objid
 UNION SELECT 'pg_type'::regclass,c.castsource,'pg_cast'::regclass,c.oid FROM pg_cast c WHERE node.classid='pg_type'::regclass AND c.castsource=node.objid
 UNION SELECT 'pg_type'::regclass,c.casttarget,'pg_cast'::regclass,c.oid FROM pg_cast c WHERE node.classid='pg_type'::regclass AND c.casttarget=node.objid
 UNION SELECT 'pg_cast'::regclass,c.oid,'pg_proc'::regclass,c.castfunc FROM pg_cast c WHERE node.classid='pg_cast'::regclass AND c.oid=node.objid AND c.castfunc<>0
 UNION SELECT 'pg_cast'::regclass,c.oid,'pg_type'::regclass,c.castsource FROM pg_cast c WHERE node.classid='pg_cast'::regclass AND c.oid=node.objid
 UNION SELECT 'pg_cast'::regclass,c.oid,'pg_type'::regclass,c.casttarget FROM pg_cast c WHERE node.classid='pg_cast'::regclass AND c.oid=node.objid
"""


# CASE guards are mandatory: catalog deparsing of unsupported constants can invoke user I/O.
_RETAINED_FHIR_DEPARSE_SAFE_SQL = """
SELECT
 (SELECT count(*)=5 AND bool_and(c.relkind='r' AND c.relpersistence='p'
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relhastriggers
 AND c.relname=ANY(ARRAY['provider_directory_endpoint_dataset','provider_directory_dataset_resource',
 'provider_directory_entity_source_binding','provider_directory_entity_release_evidence',
 'provider_directory_insurance_network_plan_evidence']))
 FROM pg_class c WHERE c.relnamespace=selected_capture.oid AND c.relkind<>'i')
 AND NOT EXISTS(SELECT FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
 LEFT JOIN pg_type t ON t.oid=a.atttypid WHERE c.relnamespace=selected_capture.oid AND c.relkind='r' AND a.attnum>0
 AND (a.attisdropped OR a.atthasdef OR a.attgenerated<>'' OR a.attidentity<>'' OR t.oid IS NULL
 OR t.typnamespace<>'pg_catalog'::regnamespace OR t.oid>=16384 OR t.typowner<>10 OR a.attcollation<>t.typcollation))
 AND NOT EXISTS(SELECT FROM pg_attrdef d JOIN pg_class c ON c.oid=d.adrelid WHERE c.relnamespace=selected_capture.oid)
 AND NOT EXISTS(SELECT FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid OR c.oid=i.inhparent
 WHERE c.relnamespace=selected_capture.oid)
 AND (SELECT count(*)=5 AND bool_and(k.contype='p' AND k.convalidated AND k.conenforced
 AND NOT k.condeferrable AND NOT k.condeferred AND k.conparentid=0)
 FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
 WHERE c.relnamespace=selected_capture.oid AND k.contype<>'n')
 AND NOT EXISTS(SELECT FROM pg_class c WHERE c.relnamespace=selected_capture.oid AND c.relkind='r'
 AND (SELECT count(*) FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='p')<>1)
 AND (SELECT count(*)=9 AND bool_and(ix.relam=403 AND i.indisvalid AND i.indisready AND i.indislive
 AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indnkeyatts=i.indnatts
 AND i.indnkeyatts>0 AND NOT EXISTS(SELECT FROM unnest(i.indkey::smallint[]) key(attnum) WHERE key.attnum<=0))
 FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_class ix ON ix.oid=i.indexrelid
 WHERE c.relnamespace=selected_capture.oid)
"""


# Match the retained capture's native catalog fingerprint, including its index filenodes.
_RETAINED_FHIR_RELATIONS_SQL = """SELECT c.oid::bigint,c.relname,c.relkind,c.relpersistence,c.relowner::bigint,c.relhastriggers,
      c.relrowsecurity,c.relforcerowsecurity,pg_relation_filenode(c.oid)::bigint AS filenode,n.oid::bigint AS schema_oid,n.nspowner::bigint,
      EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
      (SELECT jsonb_agg(jsonb_build_array(a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attgenerated,a.attidentity) ORDER BY a.attnum)
       FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)::text AS columns,
      EXISTS(SELECT 1 FROM pg_attrdef d WHERE d.adrelid=c.oid) AS defaults,
      (SELECT jsonb_agg(jsonb_build_object('oid',i.indexrelid::bigint,'name',ix.relname,'method',am.amname,
        'owner_oid',ix.relowner::bigint,'filenode',pg_relation_filenode(ix.oid)::bigint,'unique',i.indisunique,'primary',i.indisprimary,
        'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,'key_count',i.indnkeyatts,'attribute_count',i.indnatts,
        'expressions',i.indexprs IS NOT NULL,'predicate',i.indpred IS NOT NULL,'options',i.indoption::text,
        'columns',(SELECT jsonb_agg(a.attname ORDER BY key.ordinal) FROM unnest(i.indkey::smallint[]) WITH ORDINALITY key(attnum,ordinal)
          LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum),
        'definition',CASE WHEN shape.deparse_safe AND i.indexprs IS NULL AND i.indpred IS NULL
          THEN pg_catalog.pg_get_indexdef(i.indexrelid) END) ORDER BY ix.relname)
        FROM pg_index i JOIN pg_class ix ON ix.oid=i.indexrelid JOIN pg_am am ON am.oid=ix.relam
        WHERE i.indrelid=c.oid)::text AS indexes,
      (SELECT jsonb_agg(jsonb_build_array(k.oid,k.contype,k.conindid,CASE WHEN shape.deparse_safe AND k.contype='p'
          THEN pg_catalog.pg_get_constraintdef(k.oid) END) ORDER BY k.oid)
        FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype<>'n')::text AS constraints
      FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE n.oid=selected_capture.oid AND c.relkind IN ('r','p','v','m','f','S') ORDER BY c.relname"""
_RETAINED_FHIR_CERTIFICATE_SQL = (
    """retained_fhir_catalogs AS (
 SELECT selected_capture.oid AS schema_oid,owner.rolname AS owner_role,
 (SELECT jsonb_agg(to_jsonb(observed) ORDER BY observed.relname) FROM LATERAL (
"""
    + _RETAINED_FHIR_RELATIONS_SQL
    + """) observed) AS catalog,
 (WITH closure_scope AS (SELECT selected_capture.oid AS schema_oid,selected_capture.nspowner AS owner_oid,
 CASE WHEN jsonb_typeof(receipt.document->'runtime_roles')='array'
 THEN ARRAY(SELECT jsonb_array_elements_text(receipt.document->'runtime_roles')) ELSE ARRAY[]::text[] END AS role_names,
 NULL::text[] AS relation_names)
"""
    + _NATIVE_WRITER_PRIVILEGES_SQL
    + """) AND
 (WITH configured AS (SELECT r.* FROM unnest(CASE WHEN jsonb_typeof(receipt.document->'runtime_roles')='array'
 THEN ARRAY(SELECT jsonb_array_elements_text(receipt.document->'runtime_roles')) ELSE ARRAY[]::text[] END) selected(name)
 LEFT JOIN pg_roles r ON r.rolname=selected.name)
 SELECT NOT EXISTS(SELECT FROM configured r WHERE r.oid IS NULL OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
 OR has_parameter_privilege(r.oid,'session_replication_role','SET'))
 AND NOT EXISTS(SELECT FROM pg_proc p WHERE p.prosecdef AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype)
 AND EXISTS(SELECT FROM pg_roles definer WHERE definer.oid=p.proowner AND (definer.rolsuper OR definer.rolcreaterole
 OR pg_has_role(definer.oid,selected_capture.nspowner,'MEMBER') OR pg_has_role(definer.oid,selected_capture.nspowner,'SET')))
 AND EXISTS(SELECT FROM configured r WHERE has_schema_privilege(r.oid,p.pronamespace,'USAGE') AND has_function_privilege(r.oid,p.oid,'EXECUTE')))) AS closed,
 (SELECT count(*)=1 AND bool_and(i.indcollation[0]=(SELECT a.attcollation FROM pg_attribute a WHERE a.attrelid=i.indrelid AND a.attname='dataset_id') AND i.indcollation[1]='pg_catalog."C"'::regcollation AND i.indcollation[2]='pg_catalog."C"'::regcollation AND NOT EXISTS(SELECT FROM unnest(i.indclass::oid[]) selected(oid) LEFT JOIN pg_opclass o ON o.oid=selected.oid WHERE o.oid IS NULL OR o.opcnamespace<>'pg_catalog'::regnamespace OR o.opcname<>'text_ops' OR o.opcintype<>'text'::regtype OR o.opckeytype<>0 OR o.opcmethod<>(SELECT oid FROM pg_am WHERE amname='btree'))) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=(SELECT c.oid FROM pg_class c WHERE c.relnamespace=selected_capture.oid AND c.relname='provider_directory_dataset_resource') AND c.relname='retained_lookup_1_0') AS cursor_closed
 FROM private_namespaces selected_capture JOIN pg_roles owner ON owner.oid=selected_capture.nspowner
 CROSS JOIN LATERAL (SELECT (
"""
    + _RETAINED_FHIR_DEPARSE_SAFE_SQL
    + """) AS deparse_safe) shape
 LEFT JOIN pg_description description ON description.classoid='pg_namespace'::regclass
 AND description.objoid=selected_capture.oid AND description.objsubid=0
 CROSS JOIN LATERAL (SELECT CASE WHEN octet_length(description.description)<=65536+length('retained-normalized-fhir-v1:')
 THEN CASE WHEN description.description LIKE 'retained-normalized-fhir-v1:%'
 AND substr(description.description,length('retained-normalized-fhir-v1:')+1) IS JSON OBJECT WITH UNIQUE KEYS
 THEN substr(description.description,length('retained-normalized-fhir-v1:')+1)::jsonb END END AS document) receipt
 WHERE selected_capture.nspname ~ '^registry_fhir_capture_[0-9a-f]{32}$'),
"""
)


_PUBLIC_VIEW_REACHABILITY_SQL = """
SELECT jsonb_build_object('contract','registry-public-view-reachability/v1',
 'workers',(SELECT COALESCE(jsonb_agg(r.oid::text ORDER BY r.oid),'[]') FROM pg_roles r
 WHERE r.rolname=ANY(CAST(:execution_workers AS text[]))),
 'principals',(SELECT COALESCE(jsonb_agg(r.oid::text ORDER BY r.oid),'[]') FROM execution_principals r),
 'views',(SELECT COALESCE(jsonb_agg(jsonb_build_object('oid',v.oid::text,
 'no_privilege',NOT EXISTS(SELECT FROM execution_principals r WHERE
 has_table_privilege(r.oid,v.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR has_any_column_privilege(r.oid,v.oid,'SELECT,INSERT,UPDATE,REFERENCES')),
 'no_inbound',NOT EXISTS(SELECT FROM pg_depend d
 WHERE d.refclassid='pg_class'::regclass AND d.refobjid=v.oid AND d.deptype='n'
 AND NOT (d.classid='pg_type'::regclass AND d.objid=v.reltype AND d.objsubid=0)
 AND NOT (d.classid='pg_rewrite'::regclass AND d.objsubid=0
 AND EXISTS(SELECT FROM pg_rewrite rule WHERE rule.oid=d.objid AND rule.ev_class=v.oid AND rule.rulename='_RETURN'))))
 ORDER BY v.oid),'[]') FROM pg_class v WHERE v.relnamespace='public'::regnamespace AND v.relkind='v'))
"""


REGISTRY_EXECUTION_CATALOG_SQL = (
    """
WITH RECURSIVE
private_namespaces AS (SELECT DISTINCT n.* FROM pg_namespace n,execution_principals r
 WHERE n.nspname ~ '^(network_bootstrap|reference_family_archive|registry_fhir_capture)_[0-9a-f]{32}$'
 AND has_schema_privilege(r.oid,n.oid,'USAGE')),
"""
    + _RETAINED_FHIR_CERTIFICATE_SQL
    + """
roots AS (SELECT oid FROM pg_class WHERE relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname IN (:execution_control_schema,'public') OR oid IN (SELECT oid FROM private_namespaces))),
relations AS (SELECT oid FROM roots UNION SELECT c.reltoastrelid FROM pg_class c JOIN roots r USING(oid) WHERE c.reltoastrelid<>0 UNION SELECT i.indexrelid FROM pg_index i WHERE i.indrelid IN (SELECT oid FROM roots) OR i.indrelid IN (SELECT c.reltoastrelid FROM pg_class c JOIN roots r USING(oid))),
keys AS (SELECT oid FROM pg_constraint WHERE conrelid IN (SELECT oid FROM relations) OR confrelid IN (SELECT oid FROM relations)),
triggers AS (SELECT oid FROM pg_trigger WHERE tgrelid IN (SELECT oid FROM relations) OR tgconstraint IN (SELECT oid FROM keys)),
seeds(classid,objid) AS (
 SELECT 'pg_class'::regclass,oid FROM relations
 UNION SELECT 'pg_constraint'::regclass,oid FROM keys
 UNION SELECT 'pg_trigger'::regclass,oid FROM triggers
 UNION SELECT 'pg_attrdef'::regclass,oid FROM pg_attrdef WHERE adrelid IN (SELECT oid FROM relations)
 UNION SELECT 'pg_rewrite'::regclass,oid FROM pg_rewrite WHERE ev_class IN (SELECT oid FROM relations)
 UNION SELECT 'pg_policy'::regclass,oid FROM pg_policy WHERE polrelid IN (SELECT oid FROM relations)
 UNION SELECT 'pg_proc'::regclass,oid FROM pg_proc WHERE pronamespace IN ('public'::regnamespace,'information_schema'::regnamespace) OR pronamespace IN (SELECT oid FROM private_namespaces)
 UNION SELECT 'pg_type'::regclass,oid FROM pg_type WHERE typnamespace='public'::regnamespace
 UNION SELECT 'pg_operator'::regclass,oid FROM pg_operator WHERE oprnamespace='public'::regnamespace
 UNION SELECT 'pg_opclass'::regclass,oid FROM pg_opclass WHERE opcnamespace='public'::regnamespace
 UNION SELECT 'pg_opfamily'::regclass,oid FROM pg_opfamily WHERE opfnamespace='public'::regnamespace),
execution_principals AS (SELECT DISTINCT r.* FROM pg_roles w JOIN pg_roles r ON r.oid=w.oid OR pg_has_role(w.oid,r.oid,'SET') WHERE w.rolname=ANY(CAST(:execution_workers AS text[]))),
closure(classid,objid) AS (SELECT classid,objid FROM seeds UNION SELECT e.refclassid,e.refobjid FROM closure node CROSS JOIN LATERAL (
"""
    + _REGISTRY_DEPENDENCY_EDGES_SQL
    + """
) e)
SELECT jsonb_build_object('contract','registry-native-execution/v1','database_oid',(SELECT oid FROM pg_database WHERE datname=current_database()),
 'closure_count',(SELECT count(*) FROM closure),'relation_count',(SELECT count(*) FROM relations),
 'server_version',current_setting('server_version_num'),
 'public_namespace',(SELECT to_jsonb(n) FROM pg_namespace n WHERE nspname='public'),
 'information_namespace',(SELECT to_jsonb(n) FROM pg_namespace n WHERE nspname='information_schema'),
 'inherits',(SELECT COALESCE(jsonb_agg(to_jsonb(i) ORDER BY i.inhrelid,i.inhseqno),'[]') FROM pg_inherits i WHERE i.inhrelid IN (SELECT oid FROM relations) OR i.inhparent IN (SELECT oid FROM relations)),
 'native_safe',((SELECT count(*)=5 FROM pg_roles WHERE rolname=ANY(CAST(:execution_workers AS text[])))
 AND (SELECT count(*)=2 AND bool_and(NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
 AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls)
 FROM pg_roles WHERE rolname IN (:execution_queue_owner,:execution_custody_owner))
 AND (SELECT count(*)=1 AND bool_and(n.nspowner=r.oid AND NOT r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls)
 FROM pg_namespace n JOIN pg_roles r ON r.rolname=:execution_public_owner WHERE n.nspname='public')
 AND NOT EXISTS(SELECT FROM pg_namespace n CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
 LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE n.nspname='public' AND a.privilege_type='CREATE'
 AND (r.oid IS NULL OR NOT r.rolname=ANY(ARRAY[:execution_public_owner,:execution_migration]) AND r.oid<>10))
 AND NOT EXISTS(SELECT FROM execution_principals r WHERE pg_has_role(r.oid,10::oid,'MEMBER')
 OR pg_has_role(r.oid,CAST(CAST(:execution_public_owner AS text) AS regrole),'MEMBER')
 OR pg_has_role(r.oid,CAST(CAST(:execution_migration AS text) AS regrole),'MEMBER')
 OR pg_has_role(r.oid,CAST(CAST(:execution_queue_owner AS text) AS regrole),'MEMBER')
 OR pg_has_role(r.oid,CAST(CAST(:execution_reader AS text) AS regrole),'MEMBER'))
 AND NOT EXISTS(SELECT FROM execution_principals r CROSS JOIN pg_namespace n WHERE
 n.nspname NOT IN ('pg_catalog','information_schema','public',:execution_control_schema)
 AND n.oid NOT IN (SELECT oid FROM private_namespaces)
 AND n.nspname !~ '^pg_(toast_)?temp_' AND has_schema_privilege(r.oid,n.oid,'USAGE'))
 AND NOT EXISTS(SELECT FROM private_namespaces n JOIN pg_roles owner ON owner.oid=n.nspowner
 WHERE owner.rolcanlogin OR owner.rolsuper OR owner.rolcreatedb OR owner.rolcreaterole
 OR owner.rolreplication OR owner.rolbypassrls OR EXISTS(SELECT FROM execution_principals r
 WHERE pg_has_role(r.oid,owner.oid,'MEMBER') OR pg_has_role(r.oid,owner.oid,'SET')))
 AND NOT EXISTS(SELECT FROM private_namespaces n CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
 WHERE a.privilege_type='CREATE' AND a.grantee<>n.nspowner)
 AND NOT EXISTS(SELECT FROM execution_principals r CROSS JOIN pg_class c WHERE
 c.relnamespace IN (SELECT oid FROM private_namespaces) AND (
 (c.relkind IN ('r','p','v','m','f') AND (has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')))
 OR (c.relkind='S' AND has_sequence_privilege(r.oid,c.oid,'SELECT,USAGE,UPDATE'))))
 AND NOT EXISTS(SELECT FROM execution_principals r CROSS JOIN pg_class c WHERE c.relkind='S'
 AND c.oid NOT IN (SELECT oid FROM relations) AND ('pg_class'::regclass,c.oid) IN (SELECT classid,objid FROM closure)
 AND has_sequence_privilege(r.oid,c.oid,'SELECT,USAGE,UPDATE'))
 AND NOT EXISTS(SELECT FROM pg_event_trigger)
 AND (SELECT count(*)<=10000 FROM closure) AND (SELECT count(*)<=512 FROM relations)),
 'private_namespaces',(SELECT COALESCE(jsonb_agg(to_jsonb(n) ORDER BY n.oid),'[]') FROM private_namespaces n),
 'private_comments',(SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY d.objoid),'[]') FROM pg_description d WHERE d.classoid='pg_namespace'::regclass AND d.objsubid=0 AND d.objoid IN (SELECT oid FROM private_namespaces)),
 'external_sequences',(SELECT COALESCE(jsonb_agg(to_jsonb(c)-ARRAY['relpages','reltuples','relallvisible','relallfrozen','relfrozenxid','relminmxid'] ORDER BY c.oid),'[]') FROM pg_class c WHERE c.relkind='S' AND c.oid NOT IN (SELECT oid FROM relations) AND ('pg_class'::regclass,c.oid) IN (SELECT classid,objid FROM closure)),
 'callback_edges',(SELECT COALESCE(jsonb_agg(jsonb_build_array(e.classid,e.objid,e.refclassid,e.refobjid) ORDER BY e.classid,e.objid,e.refclassid,e.refobjid),'[]') FROM closure node CROSS JOIN LATERAL (
"""
    + _REGISTRY_DEPENDENCY_EDGES_SQL
    + """
) e),
 'namespace',(SELECT to_jsonb(n) FROM pg_namespace n WHERE nspname=:execution_control_schema),
 'relations',(SELECT coalesce(jsonb_agg(to_jsonb(c)-ARRAY['relpages','reltuples','relallvisible','relallfrozen','relfrozenxid','relminmxid'] ORDER BY c.oid),'[]') FROM pg_class c WHERE c.oid IN (SELECT oid FROM relations)),
 'attributes',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY a.attrelid,a.attnum),'[]') FROM pg_attribute a WHERE a.attrelid IN (SELECT oid FROM relations)),
 'defaults',(SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY d.oid),'[]') FROM pg_attrdef d WHERE d.adrelid IN (SELECT oid FROM relations)),
 'sequences',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY s.seqrelid),'[]') FROM pg_sequence s WHERE s.seqrelid IN (SELECT oid FROM relations)),
 'constraints',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.oid),'[]') FROM pg_constraint c WHERE c.oid IN (SELECT oid FROM keys)),
 'indexes',(SELECT coalesce(jsonb_agg(to_jsonb(i) ORDER BY i.indexrelid),'[]') FROM pg_index i WHERE i.indrelid IN (SELECT oid FROM relations)),
 'triggers',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.oid),'[]') FROM pg_trigger t WHERE t.oid IN (SELECT oid FROM triggers)),
 'rules',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r.oid),'[]') FROM pg_rewrite r WHERE r.ev_class IN (SELECT oid FROM relations)),
 'policies',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.oid),'[]') FROM pg_policy p WHERE p.polrelid IN (SELECT oid FROM relations)),
 'functions',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.oid),'[]') FROM pg_proc p WHERE ('pg_proc'::regclass,p.oid) IN (SELECT classid,objid FROM closure)),
 'types',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.oid),'[]') FROM pg_type t WHERE ('pg_type'::regclass,t.oid) IN (SELECT classid,objid FROM closure)),
 'operators',(SELECT coalesce(jsonb_agg(to_jsonb(o) ORDER BY o.oid),'[]') FROM pg_operator o WHERE ('pg_operator'::regclass,o.oid) IN (SELECT classid,objid FROM closure)),
 'opclasses',(SELECT coalesce(jsonb_agg(to_jsonb(o) ORDER BY o.oid),'[]') FROM pg_opclass o WHERE ('pg_opclass'::regclass,o.oid) IN (SELECT classid,objid FROM closure)),
 'opfamilies',(SELECT coalesce(jsonb_agg(to_jsonb(o) ORDER BY o.oid),'[]') FROM pg_opfamily o WHERE ('pg_opfamily'::regclass,o.oid) IN (SELECT classid,objid FROM closure)),
 'amprocs',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY a.oid),'[]') FROM pg_amproc a WHERE ('pg_opfamily'::regclass,a.amprocfamily) IN (SELECT classid,objid FROM closure)),
 'amops',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY a.oid),'[]') FROM pg_amop a WHERE ('pg_opfamily'::regclass,a.amopfamily) IN (SELECT classid,objid FROM closure)),
 'access_methods',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY a.oid),'[]') FROM pg_am a WHERE ('pg_am'::regclass,a.oid) IN (SELECT classid,objid FROM closure)),
 'aggregates',(SELECT coalesce(jsonb_agg(to_jsonb(a) ORDER BY a.aggfnoid),'[]') FROM pg_aggregate a WHERE ('pg_proc'::regclass,a.aggfnoid) IN (SELECT classid,objid FROM closure)),
 'casts',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.oid),'[]') FROM pg_cast c WHERE ('pg_type'::regclass,c.castsource) IN (SELECT classid,objid FROM closure) OR ('pg_type'::regclass,c.casttarget) IN (SELECT classid,objid FROM closure)),
 'collations',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.oid),'[]') FROM pg_collation c WHERE ('pg_collation'::regclass,c.oid) IN (SELECT classid,objid FROM closure)),
 'extensions',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.oid) FROM pg_extension e),
 'languages',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.oid) FROM pg_language l),
 'dependencies',(SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype),'[]') FROM pg_depend d WHERE (d.classid,d.objid) IN (SELECT classid,objid FROM closure)),
 'event_triggers',(SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY e.oid),'[]') FROM pg_event_trigger e))
 || jsonb_build_object('public_view_reachability',("""
    + _PUBLIC_VIEW_REACHABILITY_SQL
    + """))
 || CASE WHEN EXISTS(SELECT FROM retained_fhir_catalogs) THEN jsonb_build_object('retained_fhir_catalogs',
 (SELECT jsonb_object_agg(schema_oid::text,to_jsonb(f)-'schema_oid') FROM retained_fhir_catalogs f)) ELSE '{}'::jsonb END
"""
)


def registry_execution_catalog_parameters(
    control_schema,
    public_owner_role,
    migration_role,
    queue_owner_role,
    custody_owner_role,
    worker_roles,
    reader_role,
    *,
    protected_control_schema,
):
    """Bind fixed configured identities, never caller-supplied object acceptance."""
    names = (
        control_schema,
        public_owner_role,
        migration_role,
        queue_owner_role,
        custody_owner_role,
        reader_role,
        protected_control_schema,
    )
    _require(
        all(type(name) is str and 0 < len(name.encode()) <= 63 and "\x00" not in name for name in names), "identity"
    )
    _require(
        control_schema not in {"mrf", protected_control_schema, "public", "pg_catalog", "information_schema"},
        "control_separation",
    )
    _require(
        type(worker_roles) is tuple
        and len(worker_roles) == len(set(worker_roles)) == 5
        and all(type(name) is str and 0 < len(name.encode()) <= 63 and "\x00" not in name for name in worker_roles),
        "workers",
    )
    return dict(
        execution_control_schema=control_schema,
        execution_public_owner=public_owner_role,
        execution_migration=migration_role,
        execution_queue_owner=queue_owner_role,
        execution_custody_owner=custody_owner_role,
        execution_workers=list(worker_roles),
        execution_reader=reader_role,
    )
