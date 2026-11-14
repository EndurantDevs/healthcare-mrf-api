# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded prepared-stage keysets and admission without a database service."""

import asyncio
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_registry_cms_prepared_address as clone
from tests.test_network_registry_cms_prepared_pair_postgres import _retention_catalog_url


def _prepared(rows=4):
    return SimpleNamespace(
        address=SimpleNamespace(db_schema="sample"),
        fhir=object(),
        nonprofile_admission=SimpleNamespace(plan=SimpleNamespace(batch_size=rows), assert_ready=AsyncMock()),
    )


class BoundsConnection:
    """Return native aggregate metadata and retain the queries for range assertions."""

    def __init__(self, keys, *, row_bytes=10, is_valid=True):
        self.keys = keys
        self.row_bytes = row_bytes
        self.is_valid = is_valid
        self.bounds = []

    async def fetchrow(self, query, *arguments):
        assert "record_send(batch)" in query and "OFFSET" not in query and "ctid" not in query
        limit = int(re.search(r"LIMIT (\d+)", query)[1])
        keys = [key for key in self.keys if not arguments or key > arguments][:limit]
        self.bounds.append((limit, arguments, keys))
        return {
            "row_count": len(keys),
            "native_bytes": len(keys) * self.row_bytes,
            **{"copy_key_" + str(index): value for index, value in enumerate(keys[-1] if keys else (None,))},
        }

    async def fetchval(self, query, source):
        assert "relkind='r'" in query and "relpersistence='p'" in query
        assert "NOT relrowsecurity" in query and "NOT relforcerowsecurity" in query
        assert source == '"sample"."stage"'
        return self.is_valid


def _address(legacy_oid=None):
    stages = tuple((name, name + "_stage", legacy_oid) for name in clone._COPY_KEYS)
    return clone.PreparedEntityAddressGeneration("sample", [], [], [], [], stages, {}, {}, None, None)


@pytest.mark.parametrize("legacy_oid", [None, 42])
def test_prepared_stage_names_accept_portable_and_legacy_metadata(legacy_oid):
    address = _address(legacy_oid)
    assert clone.prepared_address_stages(address) == tuple(sorted(address.stage_oids))


@pytest.mark.parametrize("change", ["missing", "duplicate", "shape", "identifier", "committed"])
def test_prepared_stage_family_and_shape_remain_required(change):
    address = _address()
    if change == "missing":
        address.stage_oids = address.stage_oids[:-1]
    elif change == "duplicate":
        address.stage_oids = (address.stage_oids[-1], *address.stage_oids[1:])
    elif change in {"shape", "identifier"}:
        first = address.stage_oids[0]
        replacement = first[:2] if change == "shape" else (first[0], "invalid.stage", None)
        address.stage_oids = (replacement, *address.stage_oids[1:])
    else:
        address.committed = True
    with pytest.raises(ValueError):
        clone.prepared_address_stages(address)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_valid", [False, True])
async def test_stage_locks_and_layout_use_qualified_names(is_valid):
    address = _address()
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=is_valid))
    if is_valid:
        assert await clone.lock_prepared_address(session, address) == tuple(sorted(address.stage_oids))
    else:
        with pytest.raises(ValueError, match="prepared_address_changed"):
            await clone.lock_prepared_address(session, address)
    assert str(session.execute.call_args.args[0]) == (
        "LOCK TABLE "
        + ",".join(f'"sample"."{stage}"' for _, stage, _ in sorted(address.stage_oids))
        + " IN SHARE MODE NOWAIT"
    )
    for query, parameters in (call.args for call in session.scalar.call_args_list):
        assert "relpersistence='p'" in str(query) and "NOT relrowsecurity" in str(query)
        assert parameters["relation"] in {f'"sample"."{stage}"' for _, stage, _ in address.stage_oids}


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", ["sample", "other"])
async def test_native_layouts_match_the_full_source_name(monkeypatch, schema):
    prepared = _prepared()
    prepared.address = _address()
    measured_relations = [
        SimpleNamespace(schema=schema, relation=stage, oid=100 + index)
        for index, (_, stage, _) in enumerate(prepared.address.stage_oids)
    ]
    admission = prepared.nonprofile_admission
    admission.measure = AsyncMock(return_value=measured_relations)
    admission.plan.native_address_targets, admission.lease = (), object()
    capture = AsyncMock(side_effect=lambda _fhir, source, _targets: source)
    monkeypatch.setattr(clone, "capture_retained_native_source", capture)
    monkeypatch.setattr(clone, "retained_raw_policy", lambda _lease: {})
    if schema == "other":
        with pytest.raises(ValueError, match="prepared_address_changed"):
            await clone._prepared_native_sources(prepared, prepared.address.stage_oids)
        capture.assert_not_awaited()
    else:
        layouts, policy = await clone._prepared_native_sources(prepared, prepared.address.stage_oids)
        assert layouts == dict(zip(clone._COPY_KEYS, measured_relations, strict=True)) and policy == "{}"


def test_exact_native_model_keys_and_compound_range():
    assert clone._COPY_KEYS == {
        "entity_address_unified": ("location_key",),
        "entity_address_evidence": ("evidence_id",),
        "entity_address_plan_bridge": ("location_key", "entity_type", "entity_id", "plan_id"),
        "entity_address_network_bridge": ("location_key", "entity_type", "entity_id", "network_id"),
        "entity_address_procedure_bridge": ("location_key", "npi", "code_system", "code"),
        "entity_address_medication_bridge": ("location_key", "npi", "code_system", "code"),
        "facility_anchor_npi_candidate": ("candidate_id",),
    }
    keys = clone._COPY_KEYS["entity_address_plan_bridge"]
    predicate, arguments = clone._prepared_copy_range(keys, ("a", "npi", "1", "p1"), ("a", "npi", "1", "p2"))
    assert 'ROW("location_key","entity_type","entity_id","plan_id")>ROW($1,$2,$3,$4)' in predicate
    assert "<=ROW($5,$6,$7,$8)" in predicate
    assert arguments == ("a", "npi", "1", "p1", "a", "npi", "1", "p2")
    assert clone._prepared_copy_limits(_prepared(8192)) == (4096, 64 * 1024**2)


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [True, False, 0, -1, 1.5, "4", None])
async def test_invalid_row_limit_precedes_any_schema_work(rows):
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="copy_limits_invalid"):
        await clone.prepare_retained_registry_address(
            session, _prepared(rows), capture_id=None, owner_role="owner", runtime_roles=("reader",)
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("byte_limit", [True, 0, -1, 1.5, "64", None, 64 * 1024**2 + 1])
async def test_invalid_byte_limit_precedes_any_schema_work(monkeypatch, byte_limit):
    monkeypatch.setattr(clone, "_COPY_BATCH_BYTES", byte_limit)
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="copy_limits_invalid"):
        await clone.prepare_retained_registry_address(
            session, _prepared(), capture_id=None, owner_role="owner", runtime_roles=("reader",)
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("indexed", [False, None, 1])
async def test_missing_or_changed_native_key_is_refused(indexed):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=indexed))
    with pytest.raises(ValueError, match="copy_key_invalid"):
        await clone._require_prepared_copy_key(connection, '"sample"."stage"', 42, ("location_key",))
    query, source, keys = connection.fetchval.call_args.args
    assert (source, keys) == ('"sample"."stage"', ["location_key"])
    assert all(term in query for term in ("indimmediate", "indislive", "opcdefault", "indcollation", "attnotnull"))


@pytest.mark.asyncio
@pytest.mark.parametrize("overflow", [False, True])
@pytest.mark.parametrize("legacy_oid", [None, 42])
async def test_wide_or_actual_spool_overflow_halves_without_omitting_rows(monkeypatch, overflow, legacy_oid):
    keys = [("a", "npi", "1", "p" + str(index)) for index in range(5)]
    connection = BoundsConnection(keys, row_bytes=10 if overflow else 50)
    prepared, imports, attempted = _prepared(), [], []

    async def copy(_connection, query, arguments, schema, table, count, **options):
        attempted.append(count)
        if overflow and len(attempted) == 1:
            raise clone._CopyBatchTooLarge
        assert options["byte_limit"] == 130
        assert schema == "retained" and table == "entity_address_plan_bridge"
        assert "OFFSET" not in query and "ORDER BY" in query
        assert "<=ROW(" in query
        await options["import_admission"]("before_import")
        imports.extend(key for key in keys if (len(arguments) == 4 or key > arguments[:4]) and key <= arguments[-4:])
        await options["import_admission"]("after_import")

    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    await clone._copy_prepared_rows(
        connection, prepared, "retained", ("entity_address_plan_bridge", "stage", legacy_oid), 5, (4, 130)
    )
    assert imports == keys
    assert attempted == ([4, 2, 2, 1] if overflow else [2, 2, 1])
    assert prepared.nonprofile_admission.assert_ready.await_count == 6
    assert connection.bounds[0][:2] == (4, ()) and connection.bounds[1][:2] == (2, ())


@pytest.mark.asyncio
@pytest.mark.parametrize("overflow", [False, True])
async def test_one_overwide_row_is_refused_before_admission(monkeypatch, overflow):
    prepared = _prepared()
    connection = BoundsConnection([("a",)], row_bytes=1 if overflow else 130)
    copy = AsyncMock(side_effect=clone._CopyBatchTooLarge)
    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    with pytest.raises(ValueError, match="copy_row_too_large"):
        await clone._copy_prepared_rows(
            connection, prepared, "retained", ("entity_address_unified", "stage", 42), 1, (1, 130)
        )
    assert copy.await_count == int(overflow)
    prepared.nonprofile_admission.assert_ready.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["layout", "before", "after"])
async def test_batch_layout_refusal_and_admission_cancellation_propagate(monkeypatch, failure):
    prepared = _prepared()
    connection = BoundsConnection([("a",)], is_valid=failure != "layout")
    phases = []
    if failure != "layout":
        prepared.nonprofile_admission.assert_ready.side_effect = (
            asyncio.CancelledError if failure == "before" else [None, asyncio.CancelledError]
        )

    async def copy(*_arguments, import_admission, **_options):
        await import_admission("before_import")
        phases.append("copied")
        await import_admission("after_import")

    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    with pytest.raises(ValueError if failure == "layout" else asyncio.CancelledError):
        await clone._copy_prepared_rows(
            connection, prepared, "retained", ("entity_address_unified", "stage", 42), 1, (1, 130)
        )
    assert phases == (["copied"] if failure == "after" else [])


@pytest.mark.asyncio
async def test_copy_admission_keeps_the_original_source_transaction():
    prepared = _prepared()
    source = SimpleNamespace(verify_transaction=AsyncMock())
    reader = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    admit = clone._prepared_import_admission(reader, prepared, "retained", '"sample"."stage"', None, source)
    await admit("before_import")
    source.verify_transaction.assert_awaited_once_with()
    assert reader.fetchval.call_args.args[1] == '"sample"."stage"'
    prepared.nonprofile_admission.assert_ready.assert_awaited_once_with(prepared.fhir, "retained")
    source.verify_transaction.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await admit("after_import")
    assert reader.fetchval.await_count == prepared.nonprofile_admission.assert_ready.await_count == 1


@pytest.mark.asyncio
async def test_native_row_accounting_must_reach_exact_receipt(monkeypatch):
    connection = BoundsConnection([("a",)])
    copy = AsyncMock()
    monkeypatch.setattr(clone, "_copy_native_batch", copy)
    with pytest.raises(ValueError, match="copy_accounting_invalid"):
        await clone._copy_prepared_rows(
            connection, _prepared(), "retained", ("entity_address_unified", "stage", 42), 2, (1, 130)
        )
    assert copy.await_count == 1


@pytest.mark.parametrize("driver", ["postgresql", "postgresql+asyncpg"])
@pytest.mark.parametrize("host,is_ci", [("127.0.0.1", False), ("localhost", False), ("postgres", True)])
def test_retention_catalog_accepts_only_owned_database(driver, host, is_ci, monkeypatch):
    monkeypatch.setenv("CI", "true" if is_ci else "false")
    url = _retention_catalog_url(f"{driver}://fixture@{host}:5440/hc_network_registry_{'a' * 32}")
    assert url.drivername == "postgresql+asyncpg"
    assert url.host == host and url.database == "hc_network_registry_" + "a" * 32


@pytest.mark.parametrize(
    "configured",
    [
        "invalid",
        "sqlite:///hc_network_registry_" + "a" * 32,
        "postgresql://fixture@127.0.0.1:5440/postgres",
        "postgresql://fixture@127.0.0.1:5440/hc_network_registry_short",
        "postgresql://fixture@127.0.0.1:5440/hc_network_registry_" + "A" * 32,
        "postgresql://fixture@remote.example:5440/hc_network_registry_" + "a" * 32,
        "postgresql://fixture@postgres:5440/hc_network_registry_" + "a" * 32,
        "postgresql://fixture@127.0.0.1/hc_network_registry_" + "a" * 32,
        "postgresql://fixture@127.0.0.1:0/hc_network_registry_" + "a" * 32,
        "postgresql://fixture@127.0.0.1:5440/hc_network_registry_" + "a" * 32 + "?options=unsafe",
    ],
)
def test_retention_catalog_refuses_unowned_database(configured, monkeypatch):
    monkeypatch.setenv("CI", "false")
    with pytest.raises(ValueError, match="^retention_catalog_database_not_disposable$"):
        _retention_catalog_url(configured)


def _index_recipe(name, predicate):
    source_stage = "entity_address_unified_cms_synthetic"
    index_name = clone.entity_address_unified._stage_index_name(source_stage, name)
    prefix = f'CREATE INDEX "{index_name}" ON "source"."{source_stage}"'
    return source_stage, {
        "index_name": index_name,
        "indisunique": False,
        "constraint_name": None,
        "source_prefix": prefix,
        "definition": prefix + " USING btree (npi) WHERE " + predicate,
    }


@pytest.mark.parametrize(
    "name,predicate",
    [(name, predicate) for names, predicate in clone._SOURCE_INDEX_PREDICATES.items() for name in names],
)
def test_declared_cms_index_restores_only_exact_native_array_coercion(name, predicate):
    source_stage, recipe = _index_recipe(name, predicate)
    before = recipe.copy()
    statements = clone._source_index_statements(
        recipe, schema="retained", table="entity_address_unified", source_stage=source_stage
    )
    legacy = clone._legacy_index_statements(recipe, schema="retained", table="entity_address_unified")
    assert statements == [legacy[0].removesuffix(predicate) + predicate.replace("])::text[]", "])")]
    assert recipe == before
    assert statements[0].partition(" WHERE ")[0] == legacy[0].partition(" WHERE ")[0]


@pytest.mark.parametrize(
    "change",
    ["label", "and", "predicate", "element", "literal", "identifier", "name", "stage", "table", "unique", "constraint"],
)
def test_unrecognized_cms_index_recipe_is_not_rewritten(change):
    predicate = clone._SOURCE_INDEX_PREDICATES[("serving_zip5_npi", "serving_zip5_taxonomy")]
    source_stage, recipe = _index_recipe("serving_zip5_npi", predicate)
    table = "entity_address_unified"
    if change in {"label", "and", "predicate", "element", "literal", "identifier"}:
        replacement = {
            "label": predicate.replace("'practice'", "'other'"),
            "and": predicate + " AND npi IS NOT NULL",
            "predicate": "npi IS NOT NULL",
            "element": predicate.replace("'practice'::character varying", "'practice'::character varying::text"),
            "literal": "npi::text = '" + predicate.replace("'", "''") + "'",
            "identifier": '"' + predicate + '" IS NOT NULL',
        }[change]
        recipe["definition"] = recipe["definition"].removesuffix(predicate) + replacement
    if change == "name":
        recipe["index_name"] = "unknown_declared_index"
    if change == "stage":
        source_stage += "_foreign"
    if change == "table":
        table = "entity_address_evidence"
    if change == "unique":
        recipe["indisunique"] = True
    if change == "constraint":
        recipe.update(constraint_name="unique_key", constraint_type="u", condeferrable=False, condeferred=False)
    assert clone._source_index_statements(recipe, schema="retained", table=table, source_stage=source_stage) == (
        clone._legacy_index_statements(recipe, schema="retained", table=table)
    )


@pytest.mark.parametrize("expression", ["'])::text[]'", "E'])::text[]'", "$known$])::text[]$known$", '"])::text[]"'])
def test_native_predicate_restoration_preserves_quoted_keys(expression):
    predicate = clone._SOURCE_INDEX_PREDICATES[("serving_zip5_npi", "serving_zip5_taxonomy")]
    source_stage, recipe = _index_recipe("serving_zip5_npi", predicate)
    recipe["definition"] = recipe["definition"].replace("btree (npi)", "btree ((" + expression + "))")
    legacy = clone._legacy_index_statements(recipe, schema="retained", table="entity_address_unified")
    actual = clone._source_index_statements(
        recipe, schema="retained", table="entity_address_unified", source_stage=source_stage
    )
    assert actual[0].partition(" WHERE ")[0] == legacy[0].partition(" WHERE ")[0]
    assert actual[0].endswith(predicate.replace("])::text[]", "])"))
