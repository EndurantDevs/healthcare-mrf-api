# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Freeze populated compact coordinates from the original closed Profile inputs."""

import re
from dataclasses import replace

from process import provider_directory_generic_source_policy as source
from process import provider_directory_profile as profile
from process import provider_directory_profile_desired_snapshot as snapshot
from process import provider_directory_profile_scope_copy as scope_copy
from process import provider_directory_profile_source_inputs as inputs

_ROOTS = (
    ("practitioner", "ProviderDirectoryPractitioner"),
    ("role", "ProviderDirectoryPractitionerRole"),
    ("organization", "ProviderDirectoryOrganization"),
    ("affiliation", "ProviderDirectoryOrganizationAffiliation"),
    ("service", "ProviderDirectoryHealthcareService"),
    ("endpoint", "ProviderDirectoryEndpoint"),
)
_BUCKET_COUNT = (profile.NPI_MAX - profile.NPI_MIN) // profile.PROFILE_NPI_BATCH_SIZE + 1
_DATASETS_PER_QUERY = 64


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_profile_compact_inventory_changed")


def _range(bucket):
    start = profile.NPI_MIN + bucket * profile.PROFILE_NPI_BATCH_SIZE
    return start, min(start + profile.PROFILE_NPI_BATCH_SIZE, profile.NPI_MAX + 1)


def _bucket_sql(npi_sql):
    """Return at most one coordinate per canonical range without transporting NPIs."""
    return (
        f"SELECT DISTINCT ((npi-{profile.NPI_MIN})/{profile.PROFILE_NPI_BATCH_SIZE})::integer AS bucket "
        f"FROM ({npi_sql}) compact_npis WHERE npi BETWEEN {profile.NPI_MIN} AND {profile.NPI_MAX} "
        "ORDER BY bucket"
    )


def _coverage_sql(npi_sql):
    """Refuse a finalized candidate NPI outside the immutable compact coordinates."""
    return (
        "SELECT NOT EXISTS (SELECT 1 FROM (" + npi_sql + ") compact_npis "
        f"WHERE npi BETWEEN {profile.NPI_MIN} AND {profile.NPI_MAX} "
        f"AND NOT (((npi-{profile.NPI_MIN})/{profile.PROFILE_NPI_BATCH_SIZE})::integer "
        "=ANY(CAST(:compact_planned_buckets AS integer[]))))"
    )


def sql_contracts():
    """Expose deterministic census and refusal SQL to the existing plan SQL hash."""
    reference_by_name = {name + "_ref": "compact_" + name for name, _model in _ROOTS}
    reference_by_name.update(source_ref="compact_source", dataset_resource_ref="compact_resource")
    reference_by_name["affiliation_organization_ref"] = "compact_affiliation_edges"
    return {
        "compact_inventory_evidence": profile.profile_evidence_npi_inventory_sql(**reference_by_name),
        "compact_inventory_buckets": _bucket_sql(
            "SELECT npi FROM compact_desired UNION ALL SELECT npi FROM compact_incumbent_evidence "
            "UNION ALL SELECT npi FROM compact_incumbent_profile"
        ),
        "compact_inventory_coverage": _coverage_sql(
            "SELECT npi FROM compact_evidence_stage UNION ALL SELECT npi FROM compact_affected_stage "
            "UNION ALL SELECT npi FROM compact_profile_stage UNION ALL SELECT npi FROM compact_incumbent_evidence "
            "UNION ALL SELECT npi FROM compact_incumbent_profile"
        ),
        "compact_inventory_native_rows": _native_rows("SELECT * FROM compact_projected_payload", "compact_native_type"),
    }


def _prefix(statement, parameters, prefix):
    return re.sub(r"(?<!:):([a-z_][a-z_0-9]*)\b", lambda match: ":" + prefix + match[1], statement), {
        prefix + name: value for name, value in parameters.items()
    }


def _selected_fence(dataset_fence, materialization):
    pairs = tuple(zip(materialization.source_ids, materialization.dataset_ids, strict=True))
    _require(len(set(materialization.source_ids)) == len(pairs))
    datasets = tuple(dataset for dataset in dataset_fence.datasets if (dataset.source_id, dataset.dataset_id) in pairs)
    _require(
        len(datasets) == len(pairs) and {(dataset.source_id, dataset.dataset_id) for dataset in datasets} == set(pairs)
    )
    return replace(dataset_fence, datasets=datasets)


def _resource_ctes(fhir, schema, fence):
    """Project the same typed payloads as materialization, retaining genuine row types."""
    ctes, parameters, refs, row_types = [], {}, {}, {}
    for number, (name, model_name) in enumerate(_ROOTS):
        model = getattr(fhir, model_name)
        statement, values = scope_copy._resource_expected(
            fhir, model, schema, fence, frozenset(fhir.RESOURCE_MODELS_BY_TYPE)
        )
        statement, values = _prefix(statement, values, "m" + str(number) + "_")
        row_types[name] = fhir._qt(schema, model.__tablename__)
        statement = (
            _native_rows(statement, row_types[name])
            if statement
            else "SELECT (NULL::" + row_types[name] + ").* WHERE FALSE"
        )
        refs[name + "_ref"] = "compact_" + name
        ctes.append("compact_" + name + " AS MATERIALIZED (" + statement + ")")
        parameters.update(values)
    return ctes, parameters, refs, row_types


def _native_rows(statement, row_type):
    """Align every named projected field to the actual native composite's physical order."""
    return (
        "SELECT (jsonb_populate_record(NULL::"
        + row_type
        + ",to_jsonb(compact_projected))).* FROM ("
        + statement
        + ") compact_projected"
    )


def _child_ctes(fhir, schema, fence):
    """Read raw resources and affiliation edges only from issued immutable children."""
    resources, edges, parameters = [], [], {}
    for number, dataset in enumerate(fence.datasets):
        captured = inputs.binding(fhir, dataset)
        parameter = "compact_dataset_" + str(number)
        parameters[parameter] = dataset.dataset_id
        for statements, model in (
            (resources, fhir.ProviderDirectoryDatasetResource),
            (edges, fhir.ProviderDirectoryDatasetAffiliationOrganization),
        ):
            root = model.__tablename__
            reference = fhir._unscoped_qt(schema, captured["tables"][root]["name"])
            columns = ",".join(profile.quote_identifier(column.name) for column in model.__table__.columns)
            statements.append("SELECT " + columns + " FROM " + reference + " WHERE dataset_id=:" + parameter)
    _require(bool(resources) and bool(edges))
    return [
        "compact_resource AS MATERIALIZED (" + " UNION ALL ".join(resources) + ")",
        "compact_affiliation_edges AS MATERIALIZED (" + " UNION ALL ".join(edges) + ")",
    ], parameters


def _desired_query(fhir, schema, fence):
    ctes, parameters, refs, row_types = _resource_ctes(fhir, schema, fence)
    child_ctes, child_parameters = _child_ctes(fhir, schema, fence)
    parameters.update(child_parameters)
    source_sql = fhir._provider_directory_artifact_source_select_sql(fhir.ProviderDirectorySource, schema)
    ctes.extend(["compact_source AS MATERIALIZED (" + source_sql + ")", *child_ctes])
    refs.update(source_ref="compact_source", dataset_resource_ref="compact_resource")
    refs["affiliation_organization_ref"] = "compact_affiliation_edges"
    statement = profile.profile_evidence_npi_inventory_sql(resource_type_refs=row_types, **refs)
    return ctes, parameters, statement


async def _incumbent_refs(fhir, schema, serving_state):
    """Lock the routed incumbent pair and require the already resolved serving OIDs."""
    names = (profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE)
    expected = (serving_state.evidence_target_oid, serving_state.profile_target_oid)
    _require(all(type(oid) is int and 0 < oid < 2**32 for oid in expected))
    refs = tuple(fhir._qt(schema, name) for name in names)
    await fhir.db.status("LOCK TABLE " + ",".join(refs) + " IN ACCESS SHARE MODE NOWAIT")
    rows = await fhir.db.all(
        "SELECT requested.relation,c.oid::bigint FROM unnest(CAST(:compact_incumbent_refs AS text[])) "
        "WITH ORDINALITY requested(relation,ordinal) LEFT JOIN pg_class c ON c.oid=to_regclass(requested.relation) "
        "AND c.relkind='r' AND c.relpersistence='p' ORDER BY requested.ordinal",
        compact_incumbent_refs=list(refs),
    )
    _require([(row[0], row[1]) for row in rows] == list(zip(refs, expected, strict=True)))
    return refs


async def compact_ranges(fhir, schema, dataset_fence, materialization, *, has_existing_artifacts):
    """Observe populated ranges before immutable plan and lease issuance on SOURCE."""
    if not source.is_source_storage_required():
        return None
    if has_existing_artifacts and materialization.serving_state is None:
        return None
    profile_as_of = snapshot.profile_as_of(
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get(), materialization.serving_state
    )
    if profile_as_of is None:
        return None
    fence = _selected_fence(dataset_fence, materialization)
    async with scope_copy.input_transaction(fhir):
        buckets = set()
        if has_existing_artifacts:
            refs = await _incumbent_refs(fhir, schema, materialization.serving_state)
            buckets.update(
                await _read_buckets(fhir, [], {}, " UNION ALL ".join("SELECT npi FROM " + ref for ref in refs))
            )
        for offset in range(0, len(fence.datasets), _DATASETS_PER_QUERY):
            chunk = replace(fence, datasets=fence.datasets[offset : offset + _DATASETS_PER_QUERY])
            ctes, parameters, desired_sql = _desired_query(fhir, schema, chunk)
            parameters.update(
                source_ids=[dataset.source_id for dataset in chunk.datasets],
                dataset_ids=[dataset.dataset_id for dataset in chunk.datasets],
                profile_as_of=profile_as_of,
            )
            buckets.update(
                await _read_buckets(fhir, ctes, parameters, "SELECT npi FROM (" + desired_sql + ") compact_desired")
            )
    return tuple(_range(bucket) for bucket in sorted(buckets))


async def _read_buckets(fhir, ctes, parameters, npi_sql):
    """Bound transported metadata independently of the number of providers or datasets."""
    statement = ("WITH " + ",".join(ctes) + " " if ctes else "") + _bucket_sql(npi_sql)
    rows = await fhir.db.all(statement, **parameters)
    _require(all(len(row) == 1 for row in rows))
    buckets = [row[0] for row in rows]
    _require(
        len(buckets) <= _BUCKET_COUNT and all(type(bucket) is int and 0 <= bucket < _BUCKET_COUNT for bucket in buckets)
    )
    _require(buckets == sorted(set(buckets)))
    return buckets


def _planned_buckets(batch_plan):
    buckets = []
    for batch in batch_plan.compact_batches:
        if batch.kind == "copy":
            continue
        _require(batch.kind == "npi" and type(batch.npi_start) is int and type(batch.npi_end) is int)
        bucket = (batch.npi_start - profile.NPI_MIN) // profile.PROFILE_NPI_BATCH_SIZE
        _require(0 <= bucket < _BUCKET_COUNT and (batch.npi_start, batch.npi_end) == _range(bucket))
        buckets.append(bucket)
    _require(buckets == sorted(set(buckets)))
    return buckets


async def require_complete_inventory(fhir, build, batch_plan):
    """Refuse missing sparse coordinates before any compact payload or publication."""
    if not source.is_source_storage_required():
        return
    buckets = _planned_buckets(batch_plan)
    if buckets == list(range(_BUCKET_COUNT)):
        return
    if build.materialization_mode == "source_delta" and not build.source_ids and not build.removed_source_ids:
        return
    names = [build.evidence_stage, build.profile_stage]
    if build.affected_npi_stage is not None:
        names.append(build.affected_npi_stage)
    if batch_plan.has_existing_artifacts:
        names.extend((profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE))
    statements = ["SELECT npi FROM " + fhir._provider_directory_profile_build_ref(build, name) for name in names]
    async with scope_copy.prepared_read_transaction(fhir):
        complete = await fhir.db.scalar(_coverage_sql(" UNION ALL ".join(statements)), compact_planned_buckets=buckets)
    _require(complete is True)
