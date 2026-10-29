# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native immutable root/child custody; global role admission remains upstream."""

import json
import re

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import (
    BINDING_COLUMNS,
    BINDING_TABLE,
    HEADER_COLUMNS,
    LEGACY_BINDING_TABLE,
    PIN_COLUMNS,
    PIN_TABLE,
    REVISION_TABLE,
    storage_identifier,
    storage_relation,
)
from process.plan_release_storage_catalog import (
    BINDING_NATIVE_CHECKS,
    CATALOG_SQL,
    HEADER_NATIVE_CHECKS,
    PIN_NATIVE_CHECKS,
    ROLE_SQL,
)


def storage_profile(owner_role, publisher_role, reader_roles, control_reader_roles=()):
    """Accept an explicit bounded deployment profile, never a role census."""
    if type(reader_roles) is not tuple or not 1 <= len(reader_roles) <= 64:
        raise ValueError("plan_release_storage_reader_profile_invalid")
    names = (owner_role, publisher_role, *reader_roles)
    for name in names:
        storage_identifier(name)
    if len(set(names)) != len(names):
        raise ValueError("plan_release_storage_roles_overlap")
    if (
        type(control_reader_roles) is not tuple
        or len(set(control_reader_roles)) != len(control_reader_roles)
        or not set(control_reader_roles) <= set(reader_roles)
    ):
        raise ValueError("plan_release_storage_control_reader_profile_invalid")
    return names


def _require(condition, code):
    """Keep a failed catalog admission distinguishable from SQL failures."""
    if not condition:
        raise ValueError("plan_release_storage_" + code)


async def storage_roles(connection, schema_name, owner_role, publisher_role, reader_roles, control_reader_roles=()):
    """Supplement the existing complete shared-role gate with exact reader closure."""
    _require(connection.is_in_transaction(), "transaction_required")
    await connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
    names = storage_profile(owner_role, publisher_role, reader_roles, control_reader_roles)
    role_rows = await connection.fetch(ROLE_SQL, list(names), owner_role, publisher_role, schema_name)
    return _require_storage_roles(role_rows, owner_role, publisher_role, reader_roles, control_reader_roles)


def _require_storage_roles(role_rows, owner_role, publisher_role, reader_roles, control_reader_roles=()):
    """Apply the same role admission to a native row set captured in one query."""
    names = (owner_role, publisher_role, *reader_roles)
    roles_by_name = {role["rolname"]: role for role in role_rows}
    _require(set(roles_by_name) == set(names), "roles_missing")
    owner = roles_by_name[owner_role]
    publisher = roles_by_name[publisher_role]
    flags = ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
    _require(
        not owner["rolcanlogin"] and not owner["elevated"] and not any(owner[flag] for flag in flags),
        "owner_unprotected",
    )
    _require(publisher["rolcanlogin"] and publisher["owner_set"] and not publisher["elevated"], "publisher_unprotected")
    for name in reader_roles:
        reader = roles_by_name[name]
        _require(not reader["owner_set"] and not reader["publisher_set"] and not reader["elevated"], "reader_authority")
        _require(not reader["namespace_owner_reachable"], "reader_namespace_owner_authority")
        _require(
            name in control_reader_roles or not reader["namespace_create"],
            "reader_namespace_authority",
        )
        if name in control_reader_roles:
            _require(
                reader["namespace_owner_protected"],
                "control_reader_namespace_authority",
            )
    _require(all(role["namespace_usage"] for role in role_rows), "namespace_usage_missing")
    return owner["oid"], [roles_by_name[name]["oid"] for name in reader_roles]


def _document(encoded):
    return json.loads(encoded) if type(encoded) is str else encoded


def _verify_indexes(indexes, key):
    usable_indexes = [
        index
        for index in indexes
        if index["valid"]
        and index["native"]
        and index["method"] == "btree"
        and index["predicate"] is None
        and index["expression"] is None
    ]
    _require(len(indexes) == len(usable_indexes) == 2, "extra_or_unready_index")
    _require(
        any(index["primary"] and index["unique"] and index["keys"] == key for index in usable_indexes),
        "primary_key_changed",
    )
    _require(
        any(
            index["keys"] == ["snapshot_id"] and not index["unique"] and not index["primary"]
            for index in usable_indexes
        ),
        "snapshot_index_missing",
    )


def _is_revision_bound(definition):
    """Admit only native equality/IN bounds over canonical immutable revision IDs."""
    literal = r"'hpserve_[0-9A-HJKMNP-TV-Z]{26}'"
    single = rf"check\(\(\(serving_revision_id\)::text={literal}::text\)\)"
    members = rf"{literal}::charactervarying(?:,{literal}::charactervarying)*"
    multiple = rf"check\(\(\(serving_revision_id\)::text=any\(\(array\[{members}\]\)::text\[\]\)\)\)"
    expression = _normalized_expression(definition)
    return re.fullmatch(single, expression) is not None or re.fullmatch(multiple, expression) is not None


def _verify_payload(relation, *, pin):
    columns = PIN_COLUMNS if pin else BINDING_COLUMNS
    _require(_document(relation["columns"]) == [list(column) for column in columns], "columns_changed")
    expected = PIN_NATIVE_CHECKS if pin else BINDING_NATIVE_CHECKS
    constraints = _document(relation["constraints"])
    checks_by_name = {constraint["name"]: constraint for constraint in constraints if constraint["type"] == "c"}
    for name, definition in expected.items():
        check = checks_by_name.get(name)
        _require(
            check is not None
            and check["valid"]
            and _normalized_expression(check["definition"]) == _normalized_expression(definition),
            "check_changed",
        )
    _require(
        all(constraint["type"] in {"p", "c"} and constraint["valid"] for constraint in constraints),
        "foreign_key_or_constraint",
    )
    extras = [check for name, check in checks_by_name.items() if name not in expected]
    _require(
        all(check["columns"] == [1] and _is_revision_bound(check["definition"]) for check in extras)
        and (relation["relkind"] != "p" or not extras),
        "extra_check_changed",
    )
    key = ["serving_revision_id", "snapshot_id"] if pin else ["serving_revision_id", "role", "binding_ordinal"]
    _verify_indexes(_document(relation["indexes"]), key)


def _verify_header(relation):
    """Keep low-volume native current uniqueness and the immutable revision key."""
    _require(relation["relkind"] == "r" and not relation["parents"], "header_changed")
    _require(_document(relation["columns"]) == [list(column) for column in HEADER_COLUMNS], "header_columns_changed")
    constraints = _document(relation["constraints"])
    checks_by_name = {constraint["name"]: constraint for constraint in constraints if constraint["type"] == "c"}
    _require(set(checks_by_name) == set(HEADER_NATIVE_CHECKS), "header_checks_changed")
    _require(
        all(
            check["valid"]
            and _normalized_expression(check["definition"]) == _normalized_expression(HEADER_NATIVE_CHECKS[name])
            for name, check in checks_by_name.items()
        )
        and all(constraint["type"] in {"p", "c"} and constraint["valid"] for constraint in constraints),
        "header_constraint_changed",
    )
    indexes = _document(relation["indexes"])
    _require(
        len(indexes) == 4
        and all(
            index["valid"] and index["native"] and index["method"] == "btree" and index["expression"] is None
            for index in indexes
        ),
        "header_index_changed",
    )
    _require(
        any(
            index["primary"] and index["unique"] and index["valid"] and index["keys"] == ["serving_revision_id"]
            for index in indexes
        ),
        "header_key_changed",
    )
    for column in ("plan_release_id", "healthporta_plan_id"):
        _require(
            any(
                index["unique"] and index["valid"] and index["keys"] == [column] and index["predicate"] == "is_current"
                for index in indexes
            ),
            "current_key_changed",
        )

    _require(
        any(
            not index["unique"]
            and not index["primary"]
            and index["predicate"] is None
            and index["keys"] == ["healthporta_plan_id", "release_month"]
            for index in indexes
        ),
        "header_lookup_index_changed",
    )


async def verify_storage_family(
    connection, schema_name, *, owner_role, publisher_role, reader_roles, control_reader_roles=()
):
    """Authenticate all attached parents/children in one native catalog query."""
    _require(connection.is_in_transaction(), "transaction_required")
    owner_oid, reader_oids = await storage_roles(
        connection, schema_name, owner_role, publisher_role, reader_roles, control_reader_roles
    )
    roots = [
        storage_relation(schema_name, name) for name in (REVISION_TABLE, BINDING_TABLE, PIN_TABLE, LEGACY_BINDING_TABLE)
    ]
    family = await connection.fetch(CATALOG_SQL, roots, owner_oid, reader_oids)
    roots_by_name = {relation["relname"]: relation for relation in family if relation["nspname"] == schema_name}
    _require(
        all(name in roots_by_name for name in (REVISION_TABLE, BINDING_TABLE, PIN_TABLE, LEGACY_BINDING_TABLE)),
        "roots_missing",
    )
    binding_oid = roots_by_name[BINDING_TABLE]["oid"]
    pin_oid = roots_by_name[PIN_TABLE]["oid"]
    for relation in family:
        _require(relation["relowner"] == owner_oid and relation["relpersistence"] == "p", "ownership_changed")
        _require(not relation["relrowsecurity"] and not relation["relforcerowsecurity"], "row_security_changed")
        _require(
            all(relation[field] is True for field in ("inert", "native_checks", "direct_acl_closed", "readers_closed")),
            "custody_open",
        )
        if relation["oid"] == roots_by_name[REVISION_TABLE]["oid"]:
            _verify_header(relation)
            continue
        if relation["oid"] in (binding_oid, pin_oid):
            _require(
                relation["relkind"] == "p"
                and relation["partition_key"] == "LIST (serving_revision_id)"
                and not relation["parents"],
                "parent_changed",
            )
        elif relation["relname"] != LEGACY_BINDING_TABLE or relation["relispartition"]:
            _require(
                relation["relkind"] == "r"
                and relation["parents"] in ([binding_oid], [pin_oid])
                and relation["bound"] != "DEFAULT",
                "partition_changed",
            )
        else:
            _require(relation["relkind"] == "r" and not relation["parents"], "legacy_parent_changed")
            _require(
                not await connection.fetchval(
                    f"SELECT EXISTS(SELECT FROM {storage_relation(schema_name, LEGACY_BINDING_TABLE)})"
                ),
                "legacy_rows_unattached",
            )
        _verify_payload(relation, pin=relation["oid"] == pin_oid or relation["parents"] == [pin_oid])
    return {"owner_oid": owner_oid, "reader_oids": reader_oids, "relations": [dict(relation) for relation in family]}


async def require_plan_release_storage_custody(
    connection, schema_name, *, owner_role, publisher_role, reader_roles, control_reader_roles=()
):
    """Recheck one cohort's installed structural boundary after shared-role admission."""
    identity = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    _require(identity["session"] == identity["current"] == publisher_role, "genuine_publisher_required")
    return await verify_storage_family(
        connection,
        schema_name,
        owner_role=owner_role,
        publisher_role=publisher_role,
        reader_roles=reader_roles,
        control_reader_roles=control_reader_roles,
    )
