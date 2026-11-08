# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only, complete-set rooted history validation inside an admitted admin cut."""

import asyncio
import json
import re
from pathlib import Path
from uuid import uuid4

from sqlalchemy import CheckConstraint

from db.models.provider_directory_rooted_graph import (
    ProviderDirectoryRootedGraphAcquisition,
    ProviderDirectoryRootedGraphWork,
)
from db.models.provider_directory_rooted_graph_witness import (
    ProviderDirectoryRootedGraphEdge,
    ProviderDirectoryRootedGraphResource,
)
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_rooted_graph_contract as contract
from process import provider_directory_rooted_graph_source_contract as source
from process.fhir_request_failure_policy import FHIR_REQUEST_FAILURE_POLICY_ID
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_rooted_graph_store_identity import _canonical_json
from process.provider_directory_rooted_graph_terminal import (
    _reject_missing_json_constant,
    _strict_missing_json_float,
    _strict_missing_json_object,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

_HEADER = "provider_directory_rooted_graph_acquisition"
_ROOTS = tuple("provider_directory_rooted_graph_" + kind for kind in ("work", "resource", "edge"))
_MODELS = (ProviderDirectoryRootedGraphWork, ProviderDirectoryRootedGraphResource, ProviderDirectoryRootedGraphEdge)
_PREFIXES = dict(zip(_ROOTS, ("pdrgw_", "pdrgr_", "pdrge_"), strict=True))
_SQL = Path(__file__).parents[1] / "db/sql/rooted_graph_history_validation.sql"
_FETCH_ROWS = 16
_BATCH_BYTES = 8 * contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES
_CONTEXT_SQL = (
    "SELECT pg_catalog.pg_backend_pid(),pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text,"
    "session_user::pg_catalog.text,current_user::pg_catalog.text"
)


def _require(value, code):
    if not value:
        raise ValueError("rooted_graph_history_" + code)


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _relation_members(policy, family):
    """Accept only the complete fresh native capture's fixed roots and exact children."""
    _require(type(policy) is GenericSourcePolicy and family.get("schema") == policy.schema, "schema")
    roots, relations = family["roots"], family["relations"]
    selected_by_name = {}
    for root in (_HEADER, *_ROOTS):
        for name in (root,) if root == _HEADER else (root, root + "_legacy"):
            native = roots[name]
            _require(name in relations and type(native["oid"]) is int and native["oid"] > 0, "native_identity")
            selected_by_name[name] = dict(
                name=name, root=root, oid=native["oid"], parent_oid=native["parent_oid"], acquisition_id=None
            )
    for member in family["members"]:
        root = member["root"]
        if root not in _ROOTS:
            continue
        acquisition = member["acquisition_id"]
        _require(type(acquisition) is str and re.fullmatch(r"pdrga_[0-9a-f]{48}", acquisition), "member")
        name = _PREFIXES[root] + acquisition[6:]
        _require(member["name"] == name and name not in selected_by_name and name in relations, "member")
        _require(acquisition in family["acquisitions"][root], "member")
        _require(type(member["oid"]) is int and member["oid"] > 0, "native_identity")
        _require(member["parent_oid"] in (None, roots[root]["oid"]), "parent")
        is_attached = member["parent_oid"] is not None
        _require(
            (is_attached and member["phase"] in {"attached", "sealed"} and member["bounds"] == [acquisition])
            or (
                not is_attached
                and member["phase"] in {"loading", "indexed"}
                and member["bounds"] is None
                and family["acquisitions"][root][acquisition] == "building"
            ),
            "phase",
        )
        selected_by_name[name] = dict(
            name=name, root=root, oid=member["oid"], parent_oid=member["parent_oid"], acquisition_id=acquisition
        )
    _require(
        len({relation_proof["oid"] for relation_proof in selected_by_name.values()}) == len(selected_by_name),
        "oid_reused",
    )
    return tuple(sorted(selected_by_name.values(), key=lambda relation_proof: relation_proof["oid"]))


def _selection(schema, members, root):
    return " UNION ALL ".join(
        "SELECT r.*,"
        + _literal(member["name"])
        + "::pg_catalog.text AS storage_name FROM ONLY "
        + storage_relation(schema, member["name"])
        + " r"
        for member in members
        if member["root"] == root
    )


def _read_members(family):
    """Use the coordinator's complete post-custody SOURCE/control input capture."""
    inputs = family["source_inputs"]
    selected_by_name = {}
    for name in ("provider_directory_source", "provider_directory_api_endpoint"):
        proof = family["control_inputs"][name]
        native = next(entry for entry in proof["catalog"]["classes"] if entry["relname"] == name)
        selected_by_name[name] = int(native["oid"])
    for name in (
        "provider_directory_endpoint_dataset",
        "provider_directory_uhc_flex_practitioner_dataset",
        "provider_directory_rooted_graph_dataset",
    ):
        proof = inputs["headers"][name]
        _require(proof["name"] == name and bool(proof["catalog"]) and bool(proof["columns"]), "source_header")
        selected_by_name[name] = proof["oid"]
    payload_by_name = {entry["relname"]: entry for entry in inputs["family"]}
    root = payload_by_name["provider_directory_dataset_resource"]
    _require(root["relkind"] == "p" and root["parents"] == [], "source_root")
    for name, native in payload_by_name.items():
        if name == root["relname"] or root["oid"] in native["parents"]:
            _require(
                native["parents"] in ([], [root["oid"]])
                and native["direct_acl_closed"] is True
                and native["readers_closed"] is True
                and native["native_checks"] is True,
                "source_custody",
            )
            selected_by_name[name] = native["oid"]
    _require(all(type(oid) is int and oid > 0 for oid in selected_by_name.values()), "source_identity")
    _require(len(set(selected_by_name.values())) == len(selected_by_name), "source_identity")
    return selected_by_name


def _checks(model, schema):
    expressions = []
    declared_schema = storage_identifier(str(model.__table__.schema))
    for constraint in model.__table__.constraints:
        if isinstance(constraint, CheckConstraint):
            expression = str(constraint.sqltext).replace(declared_schema + ".", storage_identifier(schema) + ".")
            expression = re.sub(r"(?<![\w.])(octet_length|jsonb_typeof|to_jsonb)\(", r"pg_catalog.\1(", expression)
            expressions.append("(" + expression + ")")
    _require(bool(expressions), "model_checks")
    return " AND ".join(sorted(expressions))


def _scope_json():
    """Render the existing fixed scalar identity, without a JSONB serialization surrogate."""
    expression_by_name = {
        "acquisition_endpoint_id": "a.acquisition_endpoint_id",
        "acquisition_source_id": "a.acquisition_source_id",
        "connector_id": "a.connector_id",
        "contract_id": _literal(contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID),
        "identity_contract_id": _literal(contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID),
        **{
            name: "a." + name
            for name in (
                "max_edge_rows",
                "max_payload_bytes",
                "max_resource_rows",
                "max_work_items",
                "root_content_proof_sha256",
                "root_dataset_hash",
                "root_dataset_id",
                "root_dataset_variant",
                "root_endpoint_id",
                "root_publication_contract_id",
                "root_resource_count",
                "root_resource_type",
                "root_source_id",
                "source_authority_id",
            )
        },
    }
    numeric_names = {"max_edge_rows", "max_payload_bytes", "max_resource_rows", "max_work_items", "root_resource_count"}
    fields = []
    for name, expression in sorted(expression_by_name.items()):
        rendered = (
            expression + "::pg_catalog.text"
            if name in numeric_names
            else ("pg_catalog.to_json((" + expression + ")::pg_catalog.text)::pg_catalog.text")
        )
        fields.append(_literal(("{" if not fields else ",") + '"' + name + '":') + "||" + rendered)
    return "||".join(fields) + "||'}'"


def _missing_shapes():
    cases = []
    issues = "missing_response_json_text::pg_catalog.jsonb->'issue'"
    for status, shapes in contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_MISSING_OUTCOME_ISSUE_SHAPES:
        for shape in shapes:
            terms = [f"missing_http_status={status}", f"pg_catalog.jsonb_array_length({issues})={len(shape)}"]
            for severity, code in shape:
                terms.append(
                    "(SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements("
                    + issues
                    + ") AS issue(value) WHERE issue.value->>'severity'="
                    + _literal(severity)
                    + " AND issue.value->>'code'="
                    + _literal(code)
                    + ")=1"
                )
            cases.append("(" + " AND ".join(terms) + ")")
    return " OR ".join(cases)


def _relation_values(members):
    return ",".join(
        "("
        + _literal(relation_proof["name"])
        + "::pg_catalog.text,"
        + str(relation_proof["oid"])
        + "::pg_catalog.oid,"
        + ("NULL" if relation_proof["parent_oid"] is None else str(relation_proof["parent_oid"]))
        + "::pg_catalog.oid,"
        + ("NULL" if relation_proof["acquisition_id"] is None else _literal(relation_proof["acquisition_id"]))
        + "::pg_catalog.text)"
        for relation_proof in members
    )


def _render(policy, family):
    members = _relation_members(policy, family)
    fields = ",".join(
        "("
        + ",".join((_literal(resource), _literal(name), str(cardinality == "repeated").lower(), _literal(target_type)))
        + ")"
        for resource, definitions in contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_FIELD_CONTRACT
        for name, cardinality, target_type in definitions
    )
    replacement_by_marker = {
        "__SCHEMA__": storage_identifier(policy.schema),
        "__SCHEMA_LITERAL__": _literal(policy.schema),
        "__OWNER_LITERAL__": _literal(policy.owner_role),
        "__RELATIONS__": _relation_values(members),
        "__FIELDS__": fields,
        "__SCOPE_JSON__": _scope_json(),
        "__MISSING_SHAPES__": _missing_shapes(),
        "__STRIP_CHARACTERS__": _literal(
            "".join(
                chr(code)
                for code in (
                    *range(9, 14),
                    *range(28, 33),
                    133,
                    160,
                    5760,
                    *range(8192, 8203),
                    8232,
                    8233,
                    8239,
                    8287,
                    12288,
                )
            )
        ),
        "__NETWORK_URLS__": ",".join(
            map(_literal, contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS)
        ),
        "__API_BASE__": _literal(source.PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE),
        "__ACQUISITION_SOURCE__": _literal(source.PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID),
        "__ACQUISITION_ENDPOINT__": _literal(source.PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID),
        "__OFFICIAL_SOURCE__": _literal(UHC_PROVIDER_FILE_SOURCE_ID),
        "__FAILURE_POLICY__": _literal(FHIR_REQUEST_FAILURE_POLICY_ID),
        "__HEADER_CHECKS__": _checks(ProviderDirectoryRootedGraphAcquisition, policy.schema),
        "__READ_RELATIONS__": ",".join(
            "(" + _literal(name) + "::pg_catalog.text," + str(oid) + "::pg_catalog.oid)"
            for name, oid in sorted(_read_members(family).items())
        ),
    }
    for placeholder, root, model in zip(("WORK", "RESOURCES", "EDGES"), _ROOTS, _MODELS, strict=True):
        replacement_by_marker["__" + placeholder + "__"] = _selection(policy.schema, members, root)
        replacement_by_marker[
            "__" + {"WORK": "WORK", "RESOURCES": "RESOURCE", "EDGES": "EDGE"}[placeholder] + "_CHECKS__"
        ] = _checks(model, policy.schema)
    sql = _SQL.read_text()
    for placeholder, replacement in replacement_by_marker.items():
        sql = sql.replace(placeholder, replacement)
    _require(re.search(r"__[A-Z_]+__", sql) is None, "sql_contract")
    return sql, members


def _decode_batch(texts, *, canonical):
    original = "[" + ",".join(texts) + "]"
    try:
        if canonical:
            _require(_canonical_json(json.loads(original)) == original, "payload_canonical")
        else:
            json.loads(
                original,
                object_pairs_hook=_strict_missing_json_object,
                parse_float=_strict_missing_json_float,
                parse_constant=_reject_missing_json_constant,
            )
    except MemoryError, OverflowError, RecursionError, TypeError, UnicodeError, ValueError:
        raise ValueError("rooted_graph_history_json_invalid") from None


async def _canonical_cursor(connection, select_sql, *, canonical):
    """Use a bounded native portal without owning its original transaction or connection."""
    portal = storage_identifier("pdrg_history_" + uuid4().hex)
    await connection.execute("DECLARE " + portal + " NO SCROLL CURSOR FOR " + select_sql)
    has_failed = False
    row_count = batches = 0
    try:
        texts, byte_count = [], 2
        while page_rows := await connection.fetch(f"FETCH FORWARD {_FETCH_ROWS} FROM {portal}"):
            for payload_row in page_rows:
                text, size = payload_row["payload"], payload_row["bytes"]
                _require(type(text) is str and type(size) is int and size > 0, "cursor_shape")
                maximum = (
                    contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES
                    if canonical
                    else contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES
                )
                _require(size <= maximum, "payload_size")
                if texts and (len(texts) == _FETCH_ROWS or byte_count + size + 1 > _BATCH_BYTES):
                    _decode_batch(texts, canonical=canonical)
                    batches += 1
                    texts, byte_count = [], 2
                texts.append(text)
                byte_count += size + 1
                row_count += 1
        if texts:
            _decode_batch(texts, canonical=canonical)
            batches += 1
        return dict(rows=row_count, batches=batches)
    except BaseException:
        has_failed = True
        raise
    finally:
        try:
            await connection.execute("CLOSE " + portal, timeout=10)
        except BaseException:
            if not has_failed:
                raise


def _payload_select(schema, members, *, missing):
    root = _ROOTS[0] if missing else _ROOTS[1]
    text = "missing_response_json_text" if missing else "payload_json_text"
    ordering = "acquisition_id,query_id," + (
        "storage_name" if missing else "attempt,resource_type,resource_id,storage_name"
    )
    return (
        "SELECT "
        + text
        + " AS payload,pg_catalog.octet_length("
        + text
        + ") AS bytes FROM ("
        + _selection(schema, members, root)
        + ") selected WHERE "
        + text
        + " IS NOT NULL ORDER BY "
        + ordering
    )


async def validate_rooted_graph_history(connection, policy, family):
    """Validate all retained/current sets once after native custody and before admin CAS.

    The caller owns the original admin savepoint and locks. Native cancellation
    leaves its rollback responsible for closing an aborted portal/transaction.
    This routine never commits, rolls back, changes role or closes the connection.
    """
    task = asyncio.current_task()
    _require(connection.is_in_transaction(), "transaction")
    identity_fields = tuple(await connection.fetchrow(_CONTEXT_SQL))
    _require(identity_fields[1] is not None, "transaction")
    sql, members = _render(policy, family)
    receipt = await connection.fetchval(sql)
    receipt = json.loads(receipt) if type(receipt) is str else receipt
    _require(type(receipt) is dict and receipt.get("invalid") == [], "set_invalid")
    for missing, count in ((False, "resources"), (True, "missing_responses")):
        batches = await _canonical_cursor(
            connection, _payload_select(policy.schema, members, missing=missing), canonical=not missing
        )
        _require(batches["rows"] == receipt[count], "cursor_census")
        receipt["missing_batches" if missing else "payload_batches"] = batches["batches"]
    _require(asyncio.current_task() is task and connection.is_in_transaction(), "context")
    _require(tuple(await connection.fetchrow(_CONTEXT_SQL)) == identity_fields, "context")
    return receipt
