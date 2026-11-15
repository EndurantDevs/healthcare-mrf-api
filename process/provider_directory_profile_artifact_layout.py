# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Artifact layouts, native index phases and qualified-name recovery."""


def _artifact_scope_pk_names(fhir, table_name):
    return (fhir._bounded_identifier(f"{table_name}_pk_build_idx"), fhir._bounded_identifier(f"{table_name}_pkey"))


def _artifact_scope_pk_sql(fhir, model, schema, table_name):
    primary_key_columns = [fhir._q(column.name) for column in model.__table__.primary_key.columns]
    if not primary_key_columns:
        return ()
    index_name, constraint_name = fhir._artifact_scope_pk_names(table_name)
    column_sql = ", ".join(primary_key_columns)
    return (
        f"CREATE UNIQUE INDEX {fhir._q(index_name)} ON {fhir._qt(schema, table_name)} ({column_sql});",
        f"ALTER TABLE {fhir._qt(schema, table_name)} ADD CONSTRAINT {fhir._q(constraint_name)} PRIMARY KEY USING INDEX {fhir._q(index_name)};",
    )


async def _build_artifact_scope_pk(fhir, model, schema, table_name, *, status_executor=None):
    execute_status = status_executor or fhir._provider_directory_profile_capacity_status
    for statement in fhir._artifact_scope_pk_sql(model, schema, table_name):
        await execute_status(statement)


async def _artifact_scope_recovery_heap_fingerprint(fhir, schema, relation_name, model):
    """Validate the actual named heap and return portable structure, not catalog identity."""
    from process.provider_directory_output_snapshot_catalog import OUTPUT_CATALOG_SQL, decoded, portable_catalog_payload

    statement = OUTPUT_CATALOG_SQL.replace("$1", ":schema_name").replace(
        "$2::text[]", "CAST(:relation_names AS text[])"
    )
    catalog_row = await fhir.db.first(statement, schema_name=schema, relation_names=[relation_name])
    if catalog_row is None:
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_heap_missing")
    native = fhir._pagination_checkpoint_row_mapping(catalog_row)
    topology = decoded(native["topology"])
    if (
        native["relname"] != relation_name
        or topology["kind"] != "r"
        or topology["persistence"] != "u"
        or topology["access_method"] != "heap"
        or any(topology[key] for key in ("partition", "rls", "force_rls", "parents", "children", "hooks", "rules"))
    ):
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_heap_changed")
    indexes, constraints = await _recovery_index_catalog(fhir, schema, relation_name)
    await fhir._artifact_scope_recovery_index_phase(schema, relation_name, model, indexes, constraints)
    heap = portable_catalog_payload(native)
    heap.pop("relname")
    heap.pop("indexes")
    column_by_number = {column["number"]: column["name"] for column in heap["columns"]}
    heap["columns"] = [
        {key: column_value for key, column_value in column.items() if key != "number"} for column in heap["columns"]
    ]
    heap["constraints"] = [
        {
            **entry,
            "keys": [column_by_number[number] for number in entry["keys"]] if entry.get("keys") is not None else None,
        }
        for entry in heap["constraints"]
        if entry["type"] != "p"
    ]
    return tuple(index["index_name"] for index in indexes), heap


async def _artifact_scope_recovery_index_phase(fhir, schema, relation_name, model, indexes, constraints):
    """Require the exact allowed heap/PK-build/attached-PK/bucket index prefix."""
    main_indexes = [dict(index) for index in indexes]
    fields = (
        "constraint_type",
        "condeferrable",
        "condeferred",
        "convalidated",
        "connoinherit",
        "constraint_definition",
    )
    main_constraints = [{key: entry[key] for key in fields} for entry in constraints]
    fhir._assert_artifact_scope_native_not_null(model, main_constraints)
    main_constraints = [entry for entry in main_constraints if entry["constraint_type"] != "n"]
    build_name, pk_name = fhir._artifact_scope_pk_names(relation_name)
    bucket_name, _statement = fhir._provider_directory_profile_bucket_index_sql(schema, relation_name)
    by_name = {index["index_name"]: index for index in main_indexes}
    primary_names = set(by_name) & {build_name, pk_name}
    allowed_names = {build_name, pk_name}
    if model in {fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation}:
        allowed_names.add(bucket_name)
    if len(by_name) != len(main_indexes) or set(by_name) - allowed_names or len(primary_names) > 1:
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_indexes_changed")
    is_attached = pk_name in primary_names
    expected_constraints = (
        [
            {
                "constraint_type": "p",
                "condeferrable": False,
                "condeferred": False,
                "convalidated": True,
                "connoinherit": True,
                "constraint_definition": "PRIMARY KEY ("
                + ", ".join(column.name for column in model.__table__.primary_key.columns)
                + ")",
            }
        ]
        if is_attached
        else []
    )
    if main_constraints != expected_constraints or (bucket_name in by_name and not is_attached):
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_constraints_changed")
    for name, index in by_name.items():
        if index["reloptions"] != []:
            raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_indexes_changed")
        native = await fhir._profile_bucket_index_row(schema, relation_name, name, relation_name)
        if name == bucket_name:
            fhir._validate_profile_bucket_index_shape(native, relation_name)
        else:
            fhir._artifact_scope_recovery_pk_shape(native, model, is_attached)


def _assert_artifact_scope_native_not_null(fhir, model, constraints):
    """Keep complete native NOT NULL semantics for the declared model columns."""
    expected_constraints = [
        dict(
            constraint_type="n",
            condeferrable=False,
            condeferred=False,
            convalidated=True,
            connoinherit=False,
            constraint_definition="NOT NULL " + column.name,
        )
        for column in model.__table__.columns
        if not column.nullable
    ]
    observed_constraints = [
        {
            key: entry[key]
            for key in (
                "constraint_type",
                "condeferrable",
                "condeferred",
                "convalidated",
                "connoinherit",
                "constraint_definition",
            )
        }
        for entry in constraints
        if entry["constraint_type"] == "n"
    ]
    definitions = [entry["constraint_definition"] for entry in observed_constraints]
    if len(definitions) != len(set(definitions)) or (
        observed_constraints
        and sorted(observed_constraints, key=lambda entry: entry["constraint_definition"])
        != sorted(expected_constraints, key=lambda entry: entry["constraint_definition"])
    ):
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_not_null_changed")


def _artifact_scope_recovery_pk_shape(fhir, index_map, model, is_attached):
    """Require the exact ordinary model-key btree, including native key definitions."""
    keys = tuple((column.name for column in model.__table__.primary_key.columns))
    expected_by_field = {
        "table_kind": "r",
        "table_persistence": "u",
        "index_am": "btree",
        "indnatts": len(keys),
        "indnkeyatts": len(keys),
        "indisunique": True,
        "indnullsnotdistinct": False,
        "indisprimary": is_attached,
        "indisexclusion": False,
        "indimmediate": True,
        "indisclustered": False,
        "indisvalid": True,
        "indcheckxmin": False,
        "indisready": True,
        "indislive": True,
        "indisreplident": False,
        "first_key": keys[0],
        "second_key": keys[1] if len(keys) == 2 else "",
        "index_predicate": "",
    }
    if not 1 <= len(keys) <= 2 or {key: index_map.get(key) for key in expected_by_field} != expected_by_field:
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_pk_changed")


async def _finish_artifact_scope_indexes(fhir, schema, relation_by_table, model_by_table_name):
    """Execute the legacy index DDL only after the caller completes all payloads."""
    models_by_table = {model.__tablename__: model for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS)}
    if (
        dict(model_by_table_name) != models_by_table
        or set(relation_by_table) != set(models_by_table)
        or len(set(relation_by_table.values())) != len(models_by_table)
    ):
        raise RuntimeError("provider_directory_artifact_scope_index_family_changed")
    async with fhir._profile_capacity_mutation_window(None):
        await fhir._reserve_artifact_scope_layout_wal(statement_count=len(models_by_table) * 2 + 2)
        for base_name, table_name in sorted(relation_by_table.items()):
            model = models_by_table[base_name]
            execute_status = fhir.partial(
                fhir.profile_artifact_custody.status,
                fhir,
                "artifact_scope_layout",
                fhir._unscoped_qt(schema, table_name),
            )
            await fhir._build_artifact_scope_pk(model, schema, table_name, status_executor=execute_status)
            if model in {fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation}:
                _index_name, index_sql = fhir._provider_directory_profile_bucket_index_sql(schema, table_name)
                await execute_status(index_sql)


async def _create_provider_directory_artifact_scope_layout(fhir, model, schema, table_name, *, status_executor=None):
    """Create only the unlogged heap; completed payload precedes its indexes."""
    execute_status = status_executor or fhir.partial(
        fhir.profile_artifact_custody.status, fhir, "artifact_scope_layout", fhir._unscoped_qt(schema, table_name)
    )
    await execute_status(fhir._provider_directory_artifact_scope_table_sql(model, schema, table_name))


async def _reserve_artifact_scope_layout_wal(fhir, *, statement_count=None):
    """Reserve only the DDL phase about to execute, retaining the signed total."""
    heap_count = len((fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS))
    index_count = heap_count * 2 + 2
    statement_count = heap_count if statement_count is None else statement_count
    if type(statement_count) is not int or statement_count not in {heap_count, index_count}:
        raise RuntimeError("provider_directory_artifact_scope_layout_phase_invalid")
    admission = fhir._provider_directory_profile_capacity_admission()
    if admission is not None:
        if (
            fhir._provider_directory_profile_control_operation_count(admission, "artifact_scope_layout")
            != heap_count + index_count
        ):
            raise RuntimeError("provider_directory_artifact_scope_layout_budget_changed")
        await fhir._reserve_provider_directory_profile_wal_budget(
            admission, control_operation_counts={"artifact_scope_layout": statement_count}
        )


async def _is_artifact_scope_recovery_referenced(fhir, schema, relation_names):
    """Refuse native cleanup while a logical checkpoint or serving record retains a name."""
    checkpoint_ref = fhir._provider_directory_profile_checkpoint_ref(schema)
    serving_ref = fhir._provider_directory_profile_serving_generation_ref(schema)
    receipt_ref = fhir._provider_directory_profile_delta_receipt_ref(schema)
    targets = " OR ".join(
        f"(targets_json->'{role}'->>'schema'=:schema AND "
        f"targets_json->'{role}'->>'name'=ANY(CAST(:relation_names AS text[])))"
        for role in ("evidence", "profile")
    )
    stages = " OR ".join(
        f"{column}=ANY(CAST(:relation_names AS text[]))"
        for column in ("evidence_stage", "profile_stage", "affected_npi_stage")
    )
    return bool(
        await fhir.db.scalar(
            f"SELECT EXISTS(SELECT 1 FROM {checkpoint_ref} WHERE {stages} OR {targets} "
            f"UNION ALL SELECT 1 FROM {serving_ref} WHERE {targets} "
            f"UNION ALL SELECT 1 FROM {receipt_ref} WHERE {targets})",
            schema=schema,
            relation_names=relation_names,
        )
    )


async def _assert_artifact_scope_recovery_layouts(fhir, schema, coordinates, prior_identities, admission):
    """Compare complete named heap structure after validating every permitted prior phase."""
    current_identities = await fhir._artifact_scope_relation_identities(
        schema, (coordinate.current_table_name for coordinate in coordinates)
    )
    fhir._assert_artifact_scope_recovery_identity(
        (coordinate.current_table_name for coordinate in coordinates), current_identities
    )
    models_by_name = {model.__tablename__: model for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS)}
    for coordinate in coordinates:
        model = models_by_name[coordinate.base_table_name]
        _prior_indexes, prior_heap = await fhir._artifact_scope_recovery_heap_fingerprint(
            schema, coordinate.prior_table_name, model
        )
        current_indexes, current_heap = await fhir._artifact_scope_recovery_heap_fingerprint(
            schema, coordinate.current_table_name, model
        )
        if current_indexes or prior_heap != current_heap:
            raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_layout_changed")


async def _recover_provider_directory_artifact_scope_rows(fhir, schema, plan, coordinates):
    """Atomically replace exact hard-crash residue with current-run layouts."""
    admission = fhir._provider_directory_profile_capacity_admission()
    prior_names = tuple((coordinate.prior_table_name for coordinate in coordinates))
    initial_identities = await fhir._artifact_scope_relation_identities(schema, prior_names)
    fhir._assert_artifact_scope_recovery_identity(prior_names, initial_identities)
    await fhir._reserve_provider_directory_profile_wal_budget(
        admission, control_operation_counts={"artifact_scope_recovery_drop": len(coordinates)}
    )
    await fhir._reserve_artifact_scope_layout_wal()
    await fhir._replace_terminal_artifact_scope(schema, plan, coordinates, prior_names, initial_identities, admission)
    if await fhir._artifact_scope_relation_identities(schema, prior_names):
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_postimage_changed")
    fhir.LOGGER.warning(
        "Recovered terminal Provider Directory artifact scope build_id=%s relations=%d",
        admission.build_id,
        len(prior_names),
    )
    return prior_names


async def _replace_terminal_artifact_scope(fhir, schema, plan, coordinates, prior_names, initial_identities, admission):
    """Replace locked prior scratch with current layouts in one transaction."""
    identity = (
        schema,
        coordinates,
        prior_names,
        tuple(sorted(initial_identities.items())),
        tuple(sorted(plan.relation_by_table.items())),
        admission,
    )
    async with fhir.profile_artifact_custody.recovery_transaction(fhir, identity):
        await fhir.profile_artifact_custody.recovery_status(
            fhir,
            "LOCK TABLE "
            + ", ".join((fhir._qt(schema, prior_name) for prior_name in sorted(prior_names)))
            + " IN ACCESS EXCLUSIVE MODE NOWAIT;",
        )
        locked_identities = await fhir._artifact_scope_relation_identities(schema, prior_names)
        if locked_identities != initial_identities:
            raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scope_recovery_oid_changed")
        await fhir._assert_artifact_scope_recovery_unreferenced(schema, list(locked_identities), admission)
        await fhir._create_recovery_artifact_scope_layouts(schema, plan)
        await fhir._assert_artifact_scope_recovery_layouts(schema, coordinates, locked_identities, admission)
        for prior_name in reversed(sorted(prior_names)):
            await fhir.profile_artifact_custody.recovery_status(
                fhir, f"DROP TABLE {fhir._qt(schema, prior_name)} RESTRICT;"
            )


async def _recovery_index_catalog(fhir, schema, relation_name):
    """Read complete native index phases by qualified names, without stored handles."""
    relation = fhir._unscoped_qt(schema, relation_name)
    indexes = await fhir.db.all(
        "SELECT idx.relname AS index_name,COALESCE(idx.reloptions,ARRAY[]::text[]) AS reloptions "
        "FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid WHERE i.indrelid=to_regclass(:relation) ORDER BY idx.relname",
        relation=relation,
    )
    constraints = await fhir.db.all(
        "SELECT k.contype::text AS constraint_type,k.condeferrable,k.condeferred,k.convalidated,k.connoinherit,"
        "pg_get_constraintdef(k.oid) AS constraint_definition FROM pg_constraint k WHERE k.conrelid=to_regclass(:relation) ORDER BY k.contype,pg_get_constraintdef(k.oid)",
        relation=relation,
    )
    return (
        [fhir._pagination_checkpoint_row_mapping(row) for row in indexes],
        [fhir._pagination_checkpoint_row_mapping(row) for row in constraints],
    )
