# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Whole fixed-parent/history permission plans from source-defined offline evidence."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from db.migration_generic_source_candidates import _canonical_copy_guard_bodies
from db.migration_provider_directory_dataset_candidates import TABLES
from process import provider_directory_cms_callee_inventory as callee_inventory
from process import provider_directory_cms_owner_permissions as planner
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.test_provider_directory_cms_callee_custody import _catalog_definition, _catalog_rows, _parse_statements
from tests.test_provider_directory_cms_transition_routines import _native_foreign, _raw
from tests.test_source_validator_argument_types_migration import _load

_WRITER, _TARGET, _OWNER, _PUBLISHER, _READER, _PUBLICATION_OWNER = 101, 104, 201, 202, 203, 301


def _relation(oid, name, *, parents=()):
    if name in {*planner.dependencies.CONTROL_ROOTS, planner.dependencies.PROVENANCE_ROOT}:
        columns = tuple(column[0] for column in planner.dependencies._control_columns(name))
    elif name in planner.dependencies.HISTORY_ROOTS:
        model = planner.acquisition._models()[name.removesuffix("_legacy")]
        columns = tuple(column.name for column in model.__table__.columns)
    elif name in planner.dependencies.ROOTS:
        columns = tuple(column.name for column in planner.dependencies._models()[name].__table__.columns)
    elif name in planner.layouts._LAYOUTS:
        columns = tuple(column[0] for column in planner.layouts._LAYOUTS[name]["columns"])
    else:
        columns = ("dataset_id",)
    return dict(
        oid=oid,
        relname=name,
        nspname="catalog",
        relowner=_WRITER,
        relkind="r",
        relpersistence="p",
        native=dict(
            namespace=[42, "catalog", _TARGET, None],
            parents=list(parents),
            columns=[
                dict(attribute=dict(attname=name, attnum=number, attisdropped=False))
                for number, name in enumerate(columns, 1)
            ],
        ),
    )


def _role(oid, name, login=False):
    return dict(
        oid=oid,
        rolname=name,
        rolcanlogin=login,
        rolsuper=False,
        rolcreatedb=False,
        rolcreaterole=False,
        rolreplication=False,
        rolbypassrls=False,
    )


def _authenticate(case):
    case.storage = replace(case.storage, enrollment_sha256=planner.custody._digest(case.source))
    case.profile = dict(
        contract=planner.dependencies.CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(case.storage),
        policy=json.loads(json.dumps(asdict(case.policy))),
        fixed_descendant_oids=sorted(case.retained),
        **planner.custody._evidence_digests(case.inventory),
    )


def _catalog_relations():
    canonical_relations = [_relation(oid, name) for oid, name in enumerate(planner.profiles.ROOTS, 1000)]
    dependency_relations = [_relation(oid, name) for oid, name in enumerate(planner.dependencies.ROOTS, 2000)]
    dependency_histories = [_relation(oid, name) for oid, name in enumerate(planner.dependencies.HISTORY_ROOTS, 5000)]
    dependencies_by_name = {
        relation["relname"]: relation for relation in (*dependency_relations, *dependency_histories)
    }
    source_plan_rows = [
        {
            "singleton": True,
            "acquisition_oid": dependencies_by_name[planner.acquisition.ACQUISITION_ROOTS[2]]["oid"],
            "work_oid": dependency_histories[0]["oid"],
            "resource_oid": dependency_histories[1]["oid"],
            "parent_oid": dependencies_by_name[planner.acquisition.BULK_ROOTS[1]]["oid"],
            "work_parent_oid": dependencies_by_name[planner.acquisition.BULK_ROOTS[0]]["oid"],
        }
    ]
    by_name = {relation["relname"]: relation for relation in canonical_relations}
    plan_by_field = {
        "tables": {
            root: dict(original_oid=oid, parent_oid=by_name[root]["oid"], keys=[])
            for oid, root in enumerate(TABLES, 3000)
        }
    }
    histories = [_relation(oid, "pd_dataset_history_" + str(oid)) for oid in range(3000, 3006)]
    retained_relations = [
        _relation(4000, "retained_origin", parents=[by_name[TABLES[0]]["oid"]]),
        _relation(4001, "retained_nested_origin", parents=[4000]),
    ]
    physical_relations = [
        dict(
            oid=relation["oid"],
            native=dict(
                table_acl=[[_WRITER, _WRITER, right, False] for right in sorted(planner.controls._RIGHTS)]
                + [[_READER, _WRITER, "SELECT", False]],
                column_acl=[],
            ),
        )
        for relation in [
            *canonical_relations,
            *histories,
            *retained_relations,
            *dependency_relations,
            *dependency_histories,
        ]
    ]
    return dict(
        relations=[*canonical_relations, *histories, *retained_relations],
        migration_plan=plan_by_field,
        dependency_relations=dependency_relations,
        dependency_histories=dependency_histories,
        dependency_migration_plan={planner.dependencies.PROVENANCE_ROOT: source_plan_rows},
        physical=physical_relations,
    )


def _declared_actor_grants(canonical):
    relation_by_oid = {
        row["oid"]: row
        for key in ("relations", "dependency_relations", "dependency_histories")
        for row in canonical[key]
    }
    for physical in canonical["physical"]:
        native = physical["native"]
        row = relation_by_oid[physical["oid"]]
        number_by_name = {name: number for number, name in _column_names(row).items()}
        native["table_acl"].append([_PUBLICATION_OWNER, _WRITER, "SELECT", False])
        if row["relname"] in planner._KEYS:
            native["table_acl"].extend(
                [[_OWNER, _WRITER, "SELECT", False]]
                + [[_PUBLICATION_OWNER, _WRITER, right, False] for right in ("INSERT", "UPDATE")]
            )
            native["column_acl"].append(
                [number_by_name[planner._KEYS[row["relname"]]], _OWNER, _WRITER, "UPDATE", False]
            )
        if row["relname"] in {planner._HEADER, *TABLES[:4]}:
            native["table_acl"].extend([_PUBLISHER, _WRITER, right, False] for right in ("SELECT", "INSERT"))
            if row["relname"] == planner._HEADER:
                native["column_acl"].extend(
                    [number_by_name[name], _PUBLISHER, _WRITER, "UPDATE", False] for name in planner._PUBLICATION_UPDATE
                )


def _effective_inventory(case):
    physical_by_oid = {relation["oid"]: relation["native"] for relation in case.inventory["canonical"]["physical"]}
    relations = [
        relation
        for key in ("relations", "dependency_relations", "dependency_histories")
        for relation in case.inventory["canonical"][key]
    ]
    effective_rows = []
    for role in case.inventory["roles"]["roles"]:
        actor = int(role["oid"])
        reachable_oids = {actor, 0}
        for membership in case.inventory["roles"].get("memberships", []):
            if membership["member"] == actor and membership["inherit_option"]:
                reachable_oids.add(membership["roleid"])
        for relation in relations:
            native = physical_by_oid[relation["oid"]]
            tables = (
                planner.controls._RIGHTS
                if role["rolsuper"] is True
                else {right for grantee, _, right, _ in native["table_acl"] if grantee in reachable_oids}
            )
            columns = [
                [number, right]
                for number in _column_names(relation)
                for right in sorted(planner._COLUMN_RIGHTS)
                if right in tables
                or any(
                    column == number and grantee in reachable_oids and privilege == right
                    for column, grantee, _, privilege, _ in native["column_acl"]
                )
            ]
            effective_rows.append([actor, relation["oid"], sorted(tables), columns])
    case.inventory["roles"]["relation_rights"] = effective_rows
    case.inventory["roles"]["routine_rights"] = [
        [
            int(role["oid"]),
            routine["oid"],
            role["rolsuper"] is True
            or any(grantee in {0, int(role["oid"])} for grantee, *_ in routine["native"]["acl"]),
        ]
        for role in case.inventory["roles"]["roles"]
        for routine in case.inventory["routines"]
    ]


def _catalog_routines():
    functions = _catalog_rows("catalog")
    validators = {
        "provider_directory_rooted_graph_dataset_intrinsic_valid",
        "provider_directory_uhc_flex_practitioner_dataset_valid",
    }
    metadata_helpers = {signature.split("(")[0] for signature in planner.callables._SOURCE_CALLEES} - validators
    for entry in functions:
        entry["definition"]["proowner"] = str(_WRITER)
        if entry["definition"]["proname"] == "guard_pd_dataset_candidate_parent":
            entry["definition"]["prosrc"] = _canonical_copy_guard_bodies("catalog", "hp_snapshot_retention")[1]
        entry["acl"] = [[_WRITER, _WRITER, "EXECUTE", False]]
        if entry["definition"]["proname"] in planner.callables._SERVING_CALLEES:
            entry["acl"].append([0, _WRITER, "EXECUTE", False])
        elif entry["definition"]["proname"] in metadata_helpers:
            entry["definition"]["proowner"] = str(_TARGET)
            entry["acl"] = [[_WRITER, _TARGET, "EXECUTE", False], [_TARGET, _TARGET, "EXECUTE", False]]
        elif entry["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid":
            entry["acl"].append([0, _WRITER, "EXECUTE", False])
    routine_rows = [_raw(entry, "catalog") for entry in functions]
    for routine, entry in zip(routine_rows, functions, strict=True):
        routine["native"]["acl"] = deepcopy(entry["acl"])
        routine["native"]["namespace"][2] = _TARGET
    signature_by_name = {
        signature.split("(", 1)[0]: signature.replace("character varying", "varchar")
        for signature in planner.METADATA_FUNCTIONS
    }
    metadata = {
        signature_by_name[routine["native"]["definition"]["proname"]]: dict(
            definition=deepcopy(routine["native"]["definition"]),
            language=routine["native"]["language"]["lanname"],
            acl=deepcopy(routine["native"]["acl"]),
        )
        for routine in routine_rows
        if routine["native"]["definition"]["proname"] in signature_by_name
    }
    assert len(metadata) == 5
    return routine_rows, metadata


@pytest.fixture
def owner_case():
    binding = FHIRSourceStorageBinding("catalog", "private_source", 1, 42, 43, _OWNER, _TARGET, "a" * 64)
    policy = GenericSourcePolicy("private_source", "preparation_owner", "publication_publisher", ("reader",))
    routine_rows, metadata = _catalog_routines()
    roles = [
        _role(_WRITER, "ordinary_writer", True),
        _role(_TARGET, "protected_owner"),
        _role(_OWNER, "source_owner"),
        _role(_PUBLISHER, "publication_publisher", True),
        _role(_READER, "reader", True),
        _role(_PUBLICATION_OWNER, "preparation_owner"),
    ]
    source_by_field = dict(
        version=2,
        manifest_id="source-record",
        policy=dict(schema="private_source", owner_role="source_owner", namespace_owner_role="protected_owner"),
        publication=dict(owner_role=policy.owner_role, publisher_role=policy.publisher_role),
        storage_custody=dict(
            coordinates={key: coordinate for key, coordinate in asdict(binding).items() if key != "enrollment_sha256"},
            roles={role["rolname"]: deepcopy(role) for role in roles},
            canonical_authority=dict(metadata=metadata),
        ),
    )
    case = SimpleNamespace(
        storage=binding,
        policy=policy,
        source=source_by_field,
        retained=[4000, 4001],
        inventory=dict(canonical=_catalog_relations(), private=[], routines=routine_rows, roles=dict(roles=roles)),
    )
    _declared_actor_grants(case.inventory["canonical"])
    _effective_inventory(case)
    _authenticate(case)
    return case


def _plan(case):
    return planner.plan_cms_common_owner_permissions(
        case.inventory,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        source_document=case.source,
    )


def _planned_relations(plan):
    return {row["relname"]: row for row in plan["relations"]}


def _column_names(row):
    name_by_number = {entry["attribute"]["attnum"]: entry["attribute"]["attname"] for entry in row["native"]["columns"]}
    return name_by_number


def test_complete_fixed_scope_retained_ancestry_determinism_and_no_input_mutation(owner_case):
    case = owner_case
    before = deepcopy((case.inventory, case.source, case.profile, case.storage))
    plan = _plan(case)
    assert plan == _plan(case)
    assert (case.inventory, case.source, case.profile, case.storage) == before
    assert plan["owner_oid"] == _TARGET and plan["ordinary_writer_oid"] == _WRITER
    assert len(plan["relations"]) == 50 and len(plan["routines"]) == 32
    assert len({row["oid"] for row in plan["relations"]}) == 50
    assert all(row["owner_oid"] == _TARGET for row in plan["relations"])
    assert [row["oid"] for row in plan["relations"]] == sorted(row["oid"] for row in plan["relations"])


def test_exact_header_union_and_isolated_publisher_never_gets_other_writer_rights(owner_case):
    case = owner_case
    by_name = _planned_relations(_plan(case))
    header = by_name[planner._HEADER]
    original = next(row for row in case.inventory["canonical"]["relations"] if row["relname"] == planner._HEADER)
    names = _column_names(original)
    ordinary_updates = {
        names[number] for number, actor, _, right, _ in header["column_acl"] if actor == _WRITER and right == "UPDATE"
    }
    publisher_updates = {
        names[number]
        for number, actor, _, right, _ in header["column_acl"]
        if actor == _PUBLISHER and right == "UPDATE"
    }
    assert ordinary_updates == set(planner._ORDINARY_UPDATE) and len(ordinary_updates) == 21
    assert publisher_updates == set(planner._PUBLICATION_UPDATE) and len(publisher_updates) == 5
    for name, row in by_name.items():
        publisher_rights = {edge[2] for edge in row["table_acl"] if edge[0] == _PUBLISHER}
        assert publisher_rights == ({"SELECT", "INSERT"} if name in {planner._HEADER, *TABLES[:4]} else set())


def test_retained_histories_migration_plan_and_cms_heaps_are_ordinary_read_only(owner_case):
    case = owner_case
    plan = _plan(case)
    names = {
        *planner._READ_ONLY,
        "retained_origin",
        "retained_nested_origin",
        *("pd_dataset_history_" + str(oid) for oid in range(3000, 3006)),
    }
    for row in plan["relations"]:
        if row["relname"] in names:
            assert [edge for edge in row["table_acl"] if edge[0] == _WRITER] == [[_WRITER, _TARGET, "SELECT", False]]
            assert not any(edge[1] == _WRITER for edge in row["column_acl"])
            assert not any(edge[0] == _PUBLISHER for edge in row["table_acl"])


def test_source_checkpoint_proof_provenance_and_history_custody_is_read_only(owner_case):
    planned = _planned_relations(_plan(owner_case))
    roots = (
        *planner.dependencies.CONTROL_ROOTS,
        planner.dependencies.PROVENANCE_ROOT,
        *planner.dependencies.HISTORY_ROOTS,
    )
    for root in roots:
        relation = planned[root]
        assert [edge for edge in relation["table_acl"] if edge[0] == _WRITER] == [[_WRITER, _TARGET, "SELECT", False]]
        assert not any(edge[0] in {_OWNER, _PUBLISHER} for edge in relation["table_acl"])
        assert not relation["column_acl"]


@pytest.mark.parametrize("field", ["acquisition_oid", "work_oid", "resource_oid", "parent_oid", "work_parent_oid"])
def test_history_plan_must_bind_every_original_and_current_source_identity(owner_case, field):
    owner_case.inventory["canonical"]["dependency_migration_plan"][planner.dependencies.PROVENANCE_ROOT][0][field] += 1
    _authenticate(owner_case)
    with pytest.raises(ValueError, match="practitioner_migration_changed"):
        _plan(owner_case)


@pytest.mark.parametrize(
    "fault", ["missing_history", "renamed_history", "other_parent", "missing_physical", "extra_plan"]
)
def test_incomplete_source_history_provenance_refuses(owner_case, fault):
    canonical = owner_case.inventory["canonical"]
    if fault == "missing_history":
        canonical["dependency_histories"].pop()
    elif fault == "renamed_history":
        canonical["dependency_histories"][0]["relname"] = "unbound_history"
    elif fault == "other_parent":
        canonical["dependency_histories"][0]["native"]["parents"] = [9999]
    elif fault == "missing_physical":
        canonical["physical"] = [row for row in canonical["physical"] if row["oid"] != 5000]
    else:
        canonical["dependency_migration_plan"]["unbound_plan"] = []
    _authenticate(owner_case)
    with pytest.raises(
        ValueError, match="source_histories_changed|source_history_parent_changed|physical_changed|source_plan_changed"
    ):
        _plan(owner_case)


def test_existing_bound_publication_owner_can_be_closed_without_inventing_writer_rights(owner_case):
    canonical = owner_case.inventory["canonical"]
    relation = next(
        row for row in canonical["dependency_relations"] if row["relname"] == planner.dependencies.CONTROL_ROOTS[0]
    )
    relation["relowner"] = _PUBLICATION_OWNER
    physical = next(row["native"] for row in canonical["physical"] if row["oid"] == relation["oid"])
    physical["table_acl"] = [
        [_PUBLICATION_OWNER, _PUBLICATION_OWNER, right, False] for right in sorted(planner.controls._RIGHTS)
    ]
    _effective_inventory(owner_case)
    _authenticate(owner_case)
    planned = _planned_relations(_plan(owner_case))[relation["relname"]]
    assert [edge for edge in planned["table_acl"] if edge[0] == _PUBLICATION_OWNER] == [
        [_PUBLICATION_OWNER, _TARGET, "SELECT", False]
    ]
    assert not any(edge[0] == _WRITER for edge in planned["table_acl"])


def test_source_admission_and_review_lock_columns_remain_narrow(owner_case):
    case = owner_case
    planned = _planned_relations(_plan(case))
    original_by_name = {
        row["relname"]: row
        for key in ("relations", "dependency_relations", "dependency_histories")
        for row in case.inventory["canonical"][key]
    }
    for name, key in planner._KEYS.items():
        columns = _column_names(original_by_name[name])
        assert {
            columns[number]
            for number, actor, _, right, _ in planned[name]["column_acl"]
            if actor == _OWNER and right == "UPDATE"
        } == {key}
    for name in planner._REVIEWED:
        columns = _column_names(original_by_name[name])
        assert {
            columns[number]
            for number, actor, _, right, _ in planned[name]["column_acl"]
            if actor == _WRITER and right == "UPDATE"
        } == {"source_id"}


def test_existing_acquisition_and_control_contracts_keep_exact_writer_columns(owner_case):
    case = owner_case
    by_name = _planned_relations(_plan(case))
    for relation in case.inventory["canonical"]["dependency_relations"]:
        name = relation["relname"]
        if name not in {*planner.controls.CONTROL_ROOTS[:2], *planner.acquisition.ACQUISITION_ROOTS}:
            continue
        model = planner.dependencies._models()[name]
        columns = tuple(column.name for column in model.__table__.columns)
        table_rights, by_right = (
            planner.controls._writer_rights(name, columns)
            if name in planner.controls.CONTROL_ROOTS[:2]
            else planner.acquisition._writer_rights(name, columns)
        )
        assert {edge[2] for edge in by_name[name]["table_acl"] if edge[0] == _WRITER} == table_rights
        name_by_number = _column_names(relation)
        expected_columns = {(column, right) for right, names in by_right.items() for column in names}
        assert {
            (name_by_number[number], right)
            for number, actor, _, right, _ in by_name[name]["column_acl"]
            if actor == _WRITER
        } == expected_columns


def test_routine_callbacks_close_while_exact_known_callers_survive(owner_case):
    plan = _plan(owner_case)
    kept = {signature.split("(")[0] for signature in planner.callables._SOURCE_CALLEES} | set(
        planner.callables._SERVING_CALLEES
    )
    assert len(kept) == 16 and len(planner.callables._cms_callee_contracts()) == 32
    for row in plan["routines"]:
        expected_edges = [[_TARGET, _TARGET, "EXECUTE", False]]
        if row["proname"] in planner.callables._SERVING_CALLEES:
            expected_edges.append([0, _TARGET, "EXECUTE", False])
        elif row["proname"] in kept | planner.CMS_PROOF_DEFINERS:
            expected_edges.append([_WRITER, _TARGET, "EXECUTE", False])
            if row["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid":
                expected_edges.append([0, _TARGET, "EXECUTE", False])
        assert row["routine_acl"] == sorted(expected_edges)


@pytest.mark.parametrize("section", ["canonical", "private", "routines", "roles"])
def test_each_raw_digest_refuses_before_observed_data_can_become_policy(owner_case, section):
    case = owner_case
    case.inventory[section] = {} if section != "private" else [{}]
    with pytest.raises(ValueError, match="inventory_changed"):
        _plan(case)


@pytest.mark.parametrize("field", ["definition", "language", "acl"])
@pytest.mark.parametrize(
    "signature", [signature.replace("character varying", "varchar") for signature in planner.METADATA_FUNCTIONS]
)
def test_source_five_metadata_binds_exact_complete_entries(owner_case, field, signature):
    case = owner_case
    metadata = case.source["storage_custody"]["canonical_authority"]["metadata"]
    entry = metadata[signature]
    entry[field] = {} if field == "definition" else "changed" if field == "language" else []
    _authenticate(case)
    with pytest.raises(ValueError, match="source_metadata_changed"):
        _plan(case)


@pytest.mark.parametrize("kind", ["missing", "unlisted", "cycle", "foreign_parent", "root_replacement"])
def test_unbound_or_cyclic_retained_origin_refuses(owner_case, kind):
    case = owner_case
    rows = case.inventory["canonical"]["relations"]
    retained = next(row for row in rows if row["oid"] == 4000)
    if kind == "missing":
        rows.remove(retained)
    elif kind == "unlisted":
        case.retained.remove(4000)
    elif kind == "cycle":
        retained["native"]["parents"] = [4001]
    elif kind == "foreign_parent":
        retained["native"]["parents"] = [987654]
    else:
        case.inventory["canonical"]["migration_plan"]["tables"][TABLES[0]]["parent_oid"] += 1
    _authenticate(case)
    with pytest.raises(ValueError, match="descendants_changed|retained_ancestry_changed|history_changed"):
        _plan(case)


@pytest.mark.parametrize("column", [False, True])
@pytest.mark.parametrize("right", ["UPDATE", "INSERT", "TRIGGER", "TRUNCATE", "MAINTAIN"])
def test_unexplained_external_writer_is_not_preserved_from_observation(owner_case, column, right):
    case = owner_case
    physical = case.inventory["canonical"]["physical"][0]["native"]
    physical["column_acl" if column else "table_acl"].append(
        [1, _READER, _WRITER, right, False] if column else [_READER, _WRITER, right, False]
    )
    _authenticate(case)
    with pytest.raises(ValueError, match="external_column_writer_unbound|external_table_writer_unbound"):
        _plan(case)


@pytest.mark.parametrize("scope", ["table", "column", "routine"])
def test_grant_options_always_refuse(owner_case, scope):
    case = owner_case
    if scope == "routine":
        case.inventory["routines"][0]["native"]["acl"][0][-1] = True
    else:
        physical = case.inventory["canonical"]["physical"][0]["native"]
        physical[scope + "_acl"].append(
            [1, _READER, _WRITER, "SELECT", True] if scope == "column" else [_READER, _WRITER, "SELECT", True]
        )
    _authenticate(case)
    with pytest.raises(ValueError, match="grant_option_or_acl_changed"):
        _plan(case)


def test_source_record_and_profile_coordinates_are_not_free_actor_bindings(owner_case):
    case = owner_case
    case.source["publication"]["publisher_role"] = "reader"
    with pytest.raises(ValueError, match="source_changed"):
        _plan(case)
    _authenticate(case)
    with pytest.raises(ValueError, match="source_policy_changed"):
        _plan(case)


def test_namespace_owned_metadata_derives_only_the_coherent_preserved_caller(owner_case):
    case = owner_case
    metadata = case.source["storage_custody"]["canonical_authority"]["metadata"]
    for entry in metadata.values():
        entry["definition"]["proowner"] = str(_TARGET)
        entry["acl"] = [[_WRITER, _TARGET, "EXECUTE", False], [_TARGET, _TARGET, "EXECUTE", False]]
        routine = next(row for row in case.inventory["routines"] if row["oid"] == int(entry["definition"]["oid"]))
        routine["native"]["definition"] = deepcopy(entry["definition"])
        routine["native"]["acl"] = deepcopy(entry["acl"])
    _authenticate(case)
    assert _plan(case)["ordinary_writer_oid"] == _WRITER
    entry = next(iter(metadata.values()))
    entry["acl"].append([_READER, _TARGET, "EXECUTE", False])
    routine = next(row for row in case.inventory["routines"] if row["oid"] == int(entry["definition"]["oid"]))
    routine["native"]["acl"] = deepcopy(entry["acl"])
    _authenticate(case)
    with pytest.raises(ValueError, match="ordinary_writer_unbound"):
        _plan(case)


def test_fixed_native_ri_identity_and_acl_are_preserved(owner_case):
    case = owner_case
    foreign = _native_foreign("RI_FKey_check_ins")
    foreign["native"]["acl"] = [[0, 10, "EXECUTE", False], [10, 10, "EXECUTE", False]]
    case.inventory["routines"].append(foreign)
    _effective_inventory(case)
    _authenticate(case)
    expected_by_field = dict(
        oid=foreign["oid"],
        nspname="pg_catalog",
        proname="RI_FKey_check_ins",
        owner_oid=10,
        routine_acl=foreign["native"]["acl"],
    )
    assert next(row for row in _plan(case)["routines"] if row["oid"] == foreign["oid"]) == expected_by_field


def test_external_column_reads_are_retained_with_owner_grantor_substitution(owner_case):
    case = owner_case
    physical = case.inventory["canonical"]["physical"][0]
    physical["native"]["column_acl"].append([1, _READER, _WRITER, "SELECT", False])
    _authenticate(case)
    relation = next(row for row in _plan(case)["relations"] if row["oid"] == physical["oid"])
    assert [_READER, _TARGET, "SELECT", False] in relation["table_acl"]
    assert [1, _READER, _TARGET, "SELECT", False] in relation["column_acl"]


def test_source_metadata_cannot_relabel_other_authenticated_functions(owner_case):
    case = owner_case
    metadata = case.source["storage_custody"]["canonical_authority"]["metadata"]
    first, second = list(metadata)[:2]
    metadata[first], metadata[second] = metadata[second], metadata[first]
    _authenticate(case)
    with pytest.raises(ValueError, match="source_metadata_changed"):
        _plan(case)


def test_unknown_application_body_refuses_even_with_refreshed_synthetic_profile(owner_case):
    case = owner_case
    case.inventory["routines"][0]["native"]["definition"]["prosrc"] += "\n-- changed"
    _authenticate(case)
    with pytest.raises(ValueError, match="definition_changed"):
        _plan(case)


@pytest.mark.parametrize(
    "field", ["rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"]
)
def test_bound_target_role_cannot_gain_ordinary_or_elevated_authority(owner_case, field):
    case = owner_case
    next(role for role in case.inventory["roles"]["roles"] if role["oid"] == _TARGET)[field] = True
    _authenticate(case)
    with pytest.raises(ValueError, match="protected_roles_required"):
        _plan(case)


@pytest.mark.parametrize("kind", ["coordinates", "role_oid", "legacy_owner"])
def test_unbound_native_coordinates_or_owner_do_not_issue_permissions(owner_case, kind):
    case = owner_case
    if kind == "coordinates":
        case.source["storage_custody"]["coordinates"]["database_oid"] += 1
    elif kind == "role_oid":
        case.source["storage_custody"]["roles"]["publication_publisher"]["oid"] += 1
    else:
        case.inventory["canonical"]["relations"][0]["relowner"] = _OWNER
    _authenticate(case)
    with pytest.raises(ValueError, match="source_coordinates_changed|source_role_changed|relation_owner_unbound"):
        _plan(case)


def test_target_owned_payload_roots_narrow_known_ordinary_crud(owner_case):
    case = owner_case
    selected_relations = [
        relation for relation in case.inventory["canonical"]["relations"] if relation["relname"] in TABLES[:4]
    ]
    for relation in selected_relations:
        relation["relowner"] = _TARGET
        physical = next(
            row["native"] for row in case.inventory["canonical"]["physical"] if row["oid"] == relation["oid"]
        )
        physical["table_acl"] = [[_TARGET, _TARGET, right, False] for right in sorted(planner.controls._RIGHTS)] + [
            [_WRITER, _TARGET, right, False] for right in ("SELECT", "INSERT", "UPDATE", "DELETE")
        ]
        physical["column_acl"] = [[1, _WRITER, _TARGET, "UPDATE", False]]
    _effective_inventory(case)
    _authenticate(case)
    by_name = _planned_relations(_plan(case))
    for relation in selected_relations:
        planned = by_name[relation["relname"]]
        assert {right for actor, _, right, _ in planned["table_acl"] if actor == _WRITER} == {"SELECT", "INSERT"}
        assert not any(actor == _WRITER for _, actor, *_rest in planned["column_acl"])


@pytest.mark.parametrize("name", ["cms_serving_native_snapshot", "guard_pd_dataset_candidate_parent"])
def test_revoked_original_owner_execute_refuses_without_repair(owner_case, name):
    case = owner_case
    routine = next(
        row["native"] for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name
    )
    routine["acl"] = [edge for edge in routine["acl"] if edge[0] != _WRITER]
    _authenticate(case)
    before = deepcopy(case.inventory)
    with pytest.raises(ValueError, match="routine_owner_execute_missing"):
        _plan(case)
    assert case.inventory == before


@pytest.mark.parametrize("name", ["cms_serving_native_snapshot", "provider_directory_subset_payload_sha256"])
def test_unbound_extra_routine_caller_is_not_authenticated_by_observation(owner_case, name):
    case = owner_case
    routine = next(
        row["native"] for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name
    )
    routine["acl"].append([_READER, _WRITER, "EXECUTE", False])
    _authenticate(case)
    with pytest.raises(ValueError, match="acl_changed|source_routine_caller_unbound"):
        _plan(case)


def test_raw_role_oid_strings_normalize_only_after_authentication(owner_case):
    case = owner_case
    for role in case.inventory["roles"]["roles"]:
        role["oid"] = str(role["oid"])
    with pytest.raises(ValueError, match="inventory_changed"):
        _plan(case)
    _authenticate(case)
    before = deepcopy(case.inventory)
    assert _plan(case)["ordinary_writer_oid"] == _WRITER
    assert case.inventory == before


@pytest.mark.parametrize("value", [True, "0", "-1", "4294967296", "1.0", "+101", " 101", "١٠١"])
def test_invalid_raw_role_oid_does_not_change_expected_authority(owner_case, value):
    case = owner_case
    case.inventory["roles"]["roles"][0]["oid"] = value
    _authenticate(case)
    with pytest.raises(ValueError, match="oid_required"):
        _plan(case)


def test_planned_source_and_serving_acls_keep_existing_consumer_contracts(owner_case):
    _require_planned_callees(owner_case)


def _require_planned_callees(case):
    by_oid = {routine["oid"]: routine["native"] for routine in case.inventory["routines"]}
    projected_by_name = {}
    for routine in _plan(case)["routines"]:
        original = by_oid[routine["oid"]]
        definition_by_field = {**original["definition"], "proowner": str(routine["owner_oid"])}
        projected_by_name[routine["proname"]] = dict(
            definition=definition_by_field, language=original["language"]["lanname"], acl=routine["routine_acl"]
        )
    metadata_by_signature = {
        signature.replace("character varying", "varchar"): projected_by_name[signature.split("(")[0]]
        for signature in planner.METADATA_FUNCTIONS
    }
    callee_inventory._source_acl_shape(projected_by_name, _TARGET, metadata_by_signature)
    for name in planner.callables._SERVING_CALLEES:
        planner.callables._require_definition(projected_by_name[name], "catalog", 42)


def test_missing_source_public_contract_refuses_instead_of_adding_a_caller(owner_case):
    case = owner_case
    routine = next(
        row["native"]
        for row in case.inventory["routines"]
        if row["native"]["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid"
    )
    routine["acl"] = [edge for edge in routine["acl"] if edge[0] != 0]
    _authenticate(case)
    with pytest.raises(ValueError, match="source_public_contract_required"):
        _plan(case)


def _predecessor_owner(case, predecessor):
    physical_by_oid = {row["oid"]: row["native"] for row in case.inventory["canonical"]["physical"]}
    for relation in [
        row
        for key in ("relations", "dependency_relations", "dependency_histories")
        for row in case.inventory["canonical"][key]
    ]:
        previous = relation["relowner"]
        relation["relowner"] = predecessor
        for field, positions in (("table_acl", (0, 1)), ("column_acl", (1, 2))):
            for edge in physical_by_oid[relation["oid"]][field]:
                edge[:] = [
                    predecessor if position in positions and value == previous else value
                    for position, value in enumerate(edge)
                ]
    _effective_inventory(case)


@pytest.mark.parametrize("login", [False, True])
def test_authenticated_administrative_predecessor_never_lends_its_rights(owner_case, login):
    case = owner_case
    administrative_by_field = {**_role(10, "original_admin", login), "oid": "10", "rolsuper": True}
    case.inventory["roles"]["roles"].append(administrative_by_field)
    _predecessor_owner(case, 10)
    with pytest.raises(ValueError, match="inventory_changed"):
        _plan(case)
    _authenticate(case)
    before = deepcopy(case.inventory)
    planned = _plan(case)
    assert len(planned["relations"]) == 50
    assert all(not any(edge[0] == _WRITER for edge in relation["table_acl"]) for relation in planned["relations"])
    assert all(not any(edge[1] == _WRITER for edge in relation["column_acl"]) for relation in planned["relations"])
    assert case.inventory == before and administrative_by_field["rolsuper"] is True
    assert all(not any(edge[0] == 10 for edge in relation["table_acl"]) for relation in planned["relations"])


@pytest.mark.parametrize("kind", ["unknown", "ordinary_login", "unissued_nologin", "non_boolean_superuser"])
def test_other_predecessor_owners_are_not_administrative_by_observation(owner_case, kind):
    case = owner_case
    if kind != "unknown":
        role = _role(303, "unbound_owner", login=kind != "unissued_nologin")
        if kind == "non_boolean_superuser":
            role["rolsuper"] = 1
        case.inventory["roles"]["roles"].append(role)
    _predecessor_owner(case, 303)
    _authenticate(case)
    with pytest.raises(ValueError, match="relation_owner_unbound"):
        _plan(case)


def _administrative_validators(case):
    case.inventory["roles"]["roles"].append({**_role(10, "original_admin", True), "oid": "10", "rolsuper": True})
    for routine in case.inventory["routines"]:
        native = routine["native"]
        name = native["definition"]["proname"]
        if name in {
            "provider_directory_rooted_graph_dataset_intrinsic_valid",
            "provider_directory_uhc_flex_practitioner_dataset_valid",
        }:
            native["definition"]["proowner"] = "10"
            native["acl"] = [[10, 10, "EXECUTE", False]]
            if name == "provider_directory_uhc_flex_practitioner_dataset_valid":
                native["acl"].insert(0, [0, 10, "EXECUTE", False])
    _effective_inventory(case)


@pytest.mark.parametrize("public_uhc", [False, True])
def test_original_administrative_validators_move_without_new_ordinary_callers(owner_case, public_uhc):
    case = owner_case
    _administrative_validators(case)
    if not public_uhc:
        routine = next(
            row["native"]
            for row in case.inventory["routines"]
            if row["native"]["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid"
        )
        routine["acl"] = [edge for edge in routine["acl"] if edge[0] != 0]
    _predecessor_owner(case, 10)
    _authenticate(case)
    before = deepcopy(case.inventory)
    planned = _plan(case)
    assert len(planned["relations"]) == 50 and case.inventory == before
    for routine in planned["routines"]:
        if routine["proname"] == "provider_directory_rooted_graph_dataset_intrinsic_valid":
            assert routine["routine_acl"] == [[_TARGET, _TARGET, "EXECUTE", False]]
        elif routine["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid":
            assert routine["routine_acl"] == ([[0, _TARGET, "EXECUTE", False]] if public_uhc else []) + [
                [_TARGET, _TARGET, "EXECUTE", False]
            ]
    _require_planned_callees(case)


@pytest.mark.parametrize(
    "kind",
    ["rooted_public", "rooted_caller", "uhc_caller", "other_source_helper", "unqualified_owner"],
)
def test_administrative_validator_preimages_do_not_allow_other_shapes(owner_case, kind):
    case = owner_case
    _administrative_validators(case)
    by_name = {row["native"]["definition"]["proname"]: row["native"] for row in case.inventory["routines"]}
    name = (
        "provider_directory_uhc_flex_practitioner_dataset_valid"
        if kind.startswith("uhc")
        else "provider_directory_rooted_graph_dataset_intrinsic_valid"
    )
    routine = by_name[name]
    if kind in {"rooted_public", "rooted_caller", "uhc_caller"}:
        routine["acl"].append([0 if kind == "rooted_public" else _WRITER, 10, "EXECUTE", False])
    elif kind == "other_source_helper":
        routine = by_name["provider_directory_subset_payload_sha256"]
        routine["definition"]["proowner"] = "10"
        routine["acl"] = [[10, 10, "EXECUTE", False]]
    else:
        case.inventory["roles"]["roles"][-1]["rolsuper"] = False
    _authenticate(case)
    with pytest.raises(
        ValueError,
        match="source_administrative_acl_changed|source_administrative_owner_unbound|source_routine_caller_unbound",
    ):
        _plan(case)


def test_namespace_owned_closed_uhc_remains_closed_on_replanning(owner_case):
    case = owner_case
    routine = next(
        row["native"]
        for row in case.inventory["routines"]
        if row["native"]["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_valid"
    )
    routine["definition"]["proowner"] = str(_TARGET)
    routine["acl"] = [[_TARGET, _TARGET, "EXECUTE", False]]
    _authenticate(case)
    planned = next(row for row in _plan(case)["routines"] if row["oid"] == int(routine["definition"]["oid"]))
    assert planned["routine_acl"] == [[_TARGET, _TARGET, "EXECUTE", False]]
    _require_planned_callees(case)


def _before_actor(case, name, actor, tables, selected=None):
    canonical = case.inventory["canonical"]
    row = next(
        row
        for key in ("relations", "dependency_relations", "dependency_histories")
        for row in canonical[key]
        if row["relname"] == name
    )
    native = next(entry["native"] for entry in canonical["physical"] if entry["oid"] == row["oid"])
    number_by_name = {name: number for number, name in _column_names(row).items()}
    native["table_acl"] = [edge for edge in native["table_acl"] if edge[0] != actor] + [
        [actor, row["relowner"], right, False] for right in tables
    ]
    native["column_acl"] = [edge for edge in native["column_acl"] if edge[1] != actor] + [
        [number_by_name[column], actor, row["relowner"], right, False]
        for right, names in (selected or {}).items()
        for column in names
    ]
    return row


@pytest.mark.parametrize("before_rights", [("SELECT",), ("SELECT", "INSERT", "UPDATE")])
def test_admin_owned_source_preserves_existing_rights_without_delete_or_maintain(owner_case, before_rights):
    case = owner_case
    case.inventory["roles"]["roles"].append({**_role(10, "original_admin", True), "rolsuper": True})
    _predecessor_owner(case, 10)
    original_by_name = {name: _before_actor(case, name, _WRITER, before_rights) for name in planner._KEYS}
    _effective_inventory(case)
    _authenticate(case)
    planned = _planned_relations(_plan(case))
    for name, original in original_by_name.items():
        assert {edge[2] for edge in planned[name]["table_acl"] if edge[0] == _WRITER} == {"SELECT"}
        expected_pairs = {
            (column, right)
            for column in _column_names(original).values()
            for right in ("INSERT", "UPDATE")
            if right in before_rights
        }
        names = _column_names(original)
        assert {
            (names[number], right) for number, actor, _, right, _ in planned[name]["column_acl"] if actor == _WRITER
        } == expected_pairs


def test_all_fixed_relations_keep_read_only_actor_read_only(owner_case):
    case = owner_case
    for entry in case.inventory["canonical"]["physical"]:
        native = entry["native"]
        native["table_acl"] = [edge for edge in native["table_acl"] if edge[0] != _WRITER] + [
            [_WRITER, _WRITER, "SELECT", False]
        ]
        native["column_acl"] = [edge for edge in native["column_acl"] if edge[1] != _WRITER]
    _effective_inventory(case)
    _authenticate(case)
    plan = _plan(case)
    assert len(plan["relations"]) == 50
    for relation in plan["relations"]:
        assert [edge for edge in relation["table_acl"] if edge[0] == _WRITER] == [[_WRITER, _TARGET, "SELECT", False]]
        assert not any(edge[1] == _WRITER for edge in relation["column_acl"])


def test_distinct_publication_owner_uses_its_own_existing_capabilities(owner_case):
    case = owner_case
    name = next(iter(planner._KEYS))
    original = _before_actor(case, name, _WRITER, ("SELECT",))
    _effective_inventory(case)
    _authenticate(case)
    planned = _planned_relations(_plan(case))[name]
    columns = _column_names(original)
    assert {edge[2] for edge in planned["table_acl"] if edge[0] == _PUBLICATION_OWNER} == {"SELECT"}
    assert {
        (columns[number], right) for number, actor, _, right, _ in planned["column_acl"] if actor == _PUBLICATION_OWNER
    } == {(name, right) for name in columns.values() for right in ("INSERT", "UPDATE")}
    assert not any(edge[1] == _WRITER for edge in planned["column_acl"])
    assert [edge for edge in planned["column_acl"] if edge[1] == _OWNER] == [
        [
            next(number for number, column in columns.items() if column == planner._KEYS[name]),
            _OWNER,
            _TARGET,
            "UPDATE",
            False,
        ]
    ]


def test_inherited_rights_from_bound_publication_owner_are_not_new_capabilities(owner_case):
    case = owner_case
    name = TABLES[0]
    original = _before_actor(case, name, _WRITER, ("SELECT",))
    _before_actor(case, name, _PUBLICATION_OWNER, ("SELECT", "INSERT"))
    case.inventory["roles"]["memberships"] = [
        dict(roleid=_PUBLICATION_OWNER, member=_WRITER, inherit_option=True, set_option=False, admin_option=False)
    ]
    _effective_inventory(case)
    _authenticate(case)
    before = deepcopy(case.inventory)
    planned = _planned_relations(_plan(case))[name]
    assert original["relowner"] == _WRITER and case.inventory == before
    assert {edge[2] for edge in planned["table_acl"] if edge[0] == _WRITER} == {"SELECT", "INSERT"}


@pytest.mark.parametrize("missing", ["SELECT", "UPDATE"])
def test_private_source_owner_missing_admission_capability_refuses(owner_case, missing):
    case = owner_case
    name = next(iter(planner._KEYS))
    _before_actor(
        case,
        name,
        _OWNER,
        () if missing == "SELECT" else ("SELECT",),
        {} if missing == "UPDATE" else {"UPDATE": (planner._KEYS[name],)},
    )
    _effective_inventory(case)
    _authenticate(case)
    with pytest.raises(ValueError, match="source_owner_admission_rights_missing"):
        _plan(case)


@pytest.mark.parametrize("owner", ["source_owner", "protected_owner", "publication_publisher"])
def test_publication_owner_must_remain_distinct_from_other_source_principals(owner_case, owner):
    case = owner_case
    with pytest.raises(ValueError, match="publication_owner_required|plan_release_storage_roles_overlap"):
        case.policy = replace(case.policy, owner_role=owner)
        case.source["publication"]["owner_role"] = owner
        _authenticate(case)
        _plan(case)


@pytest.mark.parametrize(
    "kind", ["duplicate", "missing", "actor", "relation", "table_right", "column", "column_right", "duplicate_column"]
)
def test_malformed_effective_inventory_refuses_after_digest_authentication(owner_case, kind):
    case = owner_case
    rows = case.inventory["roles"]["relation_rights"]
    if kind == "duplicate":
        rows.append(deepcopy(rows[0]))
    if kind == "missing":
        rows.pop()
    if kind == "actor":
        rows[0][0] = 99999
    if kind == "relation":
        rows[0][1] = 99999
    if kind == "table_right":
        rows[0][2].append("UNKNOWN")
    if kind == "column":
        rows[0][3][0][0] = 99999
    if kind == "column_right":
        rows[0][3][0][1] = "DELETE"
    if kind == "duplicate_column":
        rows[0][3].append(deepcopy(rows[0][3][0]))
    with pytest.raises(ValueError, match="inventory_changed"):
        _plan(case)
    _authenticate(case)
    with pytest.raises(ValueError, match="effective_"):
        _plan(case)


def test_column_only_rights_remain_column_only_under_table_caps():
    rights_by_kind = dict(table=[], columns=[[2, "SELECT"], [2, "INSERT"]])
    assert planner.project_cms_relation_permissions(
        rights_by_kind, ({"SELECT", "INSERT", "MAINTAIN"}, {}), {"first": 1, "second": 2}
    ) == (set(), {"SELECT": ("second",), "INSERT": ("second",)})


def test_publication_owner_must_be_the_protected_nonlogin_policy_actor(owner_case):
    case = owner_case
    next(role for role in case.inventory["roles"]["roles"] if role["oid"] == _PUBLICATION_OWNER)["rolcanlogin"] = True
    _authenticate(case)
    with pytest.raises(ValueError, match="publication_owner_required"):
        _plan(case)


@pytest.mark.parametrize("kind", ["name", "attnum", "duplicate_attnum", "duplicate_right", "unknown_cap", "cap_column"])
def test_permission_projection_rejects_unbound_columns_and_caps(kind):
    number_by_name = {"first": 1, "second": 2}
    rights_by_kind = dict(table=["SELECT"], columns=[[1, "SELECT"], [2, "SELECT"]])
    maximum = ({"SELECT"}, {"UPDATE": ("second",)})
    if kind == "name":
        number_by_name[1] = 3
    if kind == "attnum":
        number_by_name["second"] = True
    if kind == "duplicate_attnum":
        number_by_name["second"] = 1
    if kind == "duplicate_right":
        rights_by_kind["table"].append("SELECT")
    if kind == "unknown_cap":
        maximum[0].add("UNKNOWN")
    if kind == "cap_column":
        maximum[1]["UPDATE"] = ("missing",)
    with pytest.raises(ValueError, match="permission_columns_changed|effective_rights_changed|permission_cap_changed"):
        planner.project_cms_relation_permissions(rights_by_kind, maximum, number_by_name)


@pytest.fixture
def proof_case(owner_case):
    from db import migration_provider_directory_terminal_root_retirement_guards as retirement
    from db import migration_provider_directory_terminal_root_retirement_v2 as retirement_v2

    case = owner_case
    statements = (
        retirement.valid_function_sql("catalog"),
        retirement_v2.valid_function_sql("catalog"),
        _load("20260909120000_fhir_request_failure_budget.py")._coverage_valid_sql("catalog"),
    )
    for oid, (name, spec) in enumerate(_parse_statements(statements, "catalog").items(), 6000):
        row = _raw(dict(definition=_catalog_definition(name, spec, _WRITER, oid), language=spec[3]), "catalog")
        row["native"]["acl"] = [[_WRITER, _WRITER, "EXECUTE", False]]
        case.inventory["routines"].append(row)
    _effective_inventory(case)
    _authenticate(case)
    return case


@pytest.mark.parametrize("name", sorted(planner.CMS_PROOF_DEFINERS))
@pytest.mark.parametrize("actor", [_WRITER, _OWNER, _PUBLICATION_OWNER, _PUBLISHER])
def test_finite_proof_execution_preserves_only_genuine_before_callers(proof_case, name, actor):
    case = proof_case
    row = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    if actor != _WRITER:
        row["native"]["acl"].append([actor, _WRITER, "EXECUTE", False])
    _effective_inventory(case)
    _authenticate(case)
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    assert planned["routine_acl"] == [
        list(edge)
        for edge in sorted(
            {
                (_TARGET, _TARGET, "EXECUTE", False),
                (_WRITER, _TARGET, "EXECUTE", False),
                (actor, _TARGET, "EXECUTE", False),
            }
        )
    ]


@pytest.mark.parametrize("name", sorted(planner.CMS_PROOF_DEFINERS))
def test_finite_proof_does_not_grant_a_missing_ordinary_route(proof_case, name):
    case = proof_case
    case.inventory["roles"]["roles"].append({**_role(10, "original_admin", True), "rolsuper": True})
    row = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    row["native"]["definition"]["proowner"] = "10"
    row["native"]["acl"] = [[10, 10, "EXECUTE", False]]
    _effective_inventory(case)
    _authenticate(case)
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    assert planned["routine_acl"] == [[_TARGET, _TARGET, "EXECUTE", False]]


def test_definer_public_access_narrows_to_authenticated_source_actors(proof_case):
    case = proof_case
    name = next(iter(sorted(planner.CMS_PROOF_DEFINERS)))
    row = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    row["native"]["acl"].append([0, _WRITER, "EXECUTE", False])
    _effective_inventory(case)
    _authenticate(case)
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    assert {edge[0] for edge in planned["routine_acl"]} == {_TARGET, _WRITER, _OWNER, _PUBLICATION_OWNER, _PUBLISHER}
    assert not any(edge[0] in {0, _READER} for edge in planned["routine_acl"])


@pytest.mark.parametrize("public", [False, True])
def test_fixed_invoker_preserves_exact_old_public_and_nonowner_edges(proof_case, public):
    case = proof_case
    name = planner.CMS_INVOKER_PROOF
    row = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    row["native"]["acl"].append([_READER, _WRITER, "EXECUTE", False])
    if public:
        row["native"]["acl"].append([0, _WRITER, "EXECUTE", False])
    _effective_inventory(case)
    _authenticate(case)
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    expected_actors = {_TARGET, _READER, 0} if public else {_TARGET, _READER, _WRITER}
    assert {edge[0] for edge in planned["routine_acl"]} == expected_actors
    assert row["native"]["definition"]["prosecdef"] is False


@pytest.mark.parametrize("field", ["prosecdef", "prosrc"])
def test_invoker_proof_requires_exact_source_security_and_body(proof_case, field):
    case = proof_case
    row = next(
        row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == planner.CMS_INVOKER_PROOF
    )
    row["native"]["definition"][field] = True if field == "prosecdef" else "RETURN true;"
    _authenticate(case)
    with pytest.raises(ValueError, match="definition_changed"):
        _plan(case)


@pytest.mark.parametrize("kind", ["missing", "duplicate", "actor", "routine", "non_boolean"])
def test_complete_authenticated_routine_rights_matrix_is_required(proof_case, kind):
    case = proof_case
    rows = case.inventory["roles"]["routine_rights"]
    if kind == "missing":
        rows.pop()
    if kind == "duplicate":
        rows.append(deepcopy(rows[0]))
    if kind == "actor":
        rows[0][0] = 99999
    if kind == "routine":
        rows[0][1] = 99999
    if kind == "non_boolean":
        rows[0][2] = 1
    with pytest.raises(ValueError, match="inventory_changed"):
        _plan(case)
    _authenticate(case)
    with pytest.raises(ValueError, match="routine_rights_"):
        _plan(case)


@pytest.fixture
def outside_case(owner_case, monkeypatch):
    from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration
    from tests.test_source_validator_argument_types_migration import _authentic_functions

    case = owner_case
    body_by_name = {
        name: attributes[-1]
        for (name, arguments), attributes in _current_functions().items()
        if name in planner.dependencies._CALLBACK_CALLEES
        and tuple(arguments.split(",")) == planner.dependencies._CALLBACK_CALLEES[name]
    }
    body_by_name.update(
        {
            name: attributes[-1]
            for name, attributes in _authentic_functions().items()
            if name in planner.dependencies._CALLBACK_CALLEES
        }
    )
    npi = _load("20260808170000_public_evidence_npi_enumeration_storage.py")
    statements = []
    monkeypatch.setattr(npi, "op", SimpleNamespace(execute=statements.append))
    npi._create_digest_helpers("catalog")
    body_by_name["public_evidence_npi_valid"] = _parse_statements(statements, "catalog")["public_evidence_npi_valid"][
        -1
    ]
    assert set(body_by_name) == set(planner.dependencies._CALLBACK_CALLEES)
    for oid, (name, arguments) in enumerate(planner.dependencies._CALLBACK_CALLEES.items(), 7000):
        body = body_by_name[name].replace('"snapshot_schema"', '"catalog"')
        for caller, old, new, _count in (
            *_load("20261016040000_source_validator_argument_types.py")._calls("catalog"),
            *_migration()._calls("catalog"),
        ):
            if caller == name:
                body = body.replace(old, new)
        signature = name + "(" + ",".join(arguments) + ")"
        spec = planner.source_callables._CONTRACTS[signature]
        settings = ["search_path=pg_catalog, catalog"] if name.startswith("ptg_wave_") else ["search_path=pg_catalog"]
        definition = _catalog_definition(name, (*spec[:-1], settings, body), _WRITER, oid)
        routine = _raw(dict(definition=definition, language=spec[3]), "catalog")
        if name.startswith("ptg_wave_"):
            routine["native"]["namespace_identifier"] = "catalog"
        routine["native"]["acl"] = [[_WRITER, _WRITER, "EXECUTE", False]]
        if name in planner.source_callables._PUBLIC_NAMES:
            routine["native"]["acl"].append([0, _WRITER, "EXECUTE", False])
        case.inventory["routines"].append(routine)
    _effective_inventory(case)
    _authenticate(case)
    return case


@pytest.mark.parametrize("name", sorted(planner.dependencies._CALLBACK_CALLEES))
@pytest.mark.parametrize("actor", [_WRITER, _OWNER, _PUBLICATION_OWNER, _PUBLISHER])
def test_outside_callable_preserves_bound_prior_execution(outside_case, name, actor):
    case = outside_case
    routine = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    if actor != _WRITER:
        routine["native"]["acl"].append([actor, _WRITER, "EXECUTE", False])
    _effective_inventory(case)
    _authenticate(case)
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    assert [actor, _TARGET, "EXECUTE", False] in planned["routine_acl"]
    assert all(edge[0] in {_TARGET, _WRITER, actor, 0} for edge in planned["routine_acl"])
    assert any(edge[0] == 0 for edge in planned["routine_acl"]) == (name in planner.source_callables._PUBLIC_NAMES)


@pytest.mark.parametrize("name", sorted(planner.dependencies._CALLBACK_CALLEES))
def test_administrative_outside_owner_never_issues_a_missing_route(outside_case, name):
    case = outside_case
    case.inventory["roles"]["roles"].append({**_role(10, "original_admin", True), "rolsuper": True})
    routine = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    routine["native"]["definition"]["proowner"] = "10"
    routine["native"]["acl"] = [[10, 10, "EXECUTE", False]]
    _effective_inventory(case)
    _authenticate(case)
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    assert planned["routine_acl"] == [[_TARGET, _TARGET, "EXECUTE", False]]


@pytest.mark.parametrize(
    "name", sorted(set(planner.dependencies._CALLBACK_CALLEES) & planner.source_callables._PUBLIC_NAMES)
)
def test_existing_public_contract_preserves_reader_without_new_actor_grants(outside_case, name):
    case = outside_case
    routine = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    row = next(row for row in case.inventory["roles"]["routine_rights"] if row[:2] == [_READER, routine["oid"]])
    assert row[2] is True
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    assert [0, _TARGET, "EXECUTE", False] in planned["routine_acl"]
    assert not any(edge[0] in {_READER, _OWNER, _PUBLICATION_OWNER, _PUBLISHER} for edge in planned["routine_acl"])


@pytest.mark.parametrize("kind", ["unbound_caller", "unbound_owner", "unissued_public", "grant_option"])
def test_outside_unknown_execution_contract_refuses_without_silent_loss(outside_case, kind):
    case = outside_case
    name = "provider_directory_rooted_graph_dataset_ready"
    routine = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    if kind == "unbound_caller":
        routine["native"]["acl"].append([_READER, _WRITER, "EXECUTE", False])
    if kind == "unbound_owner":
        case.inventory["roles"]["roles"].append(_role(404, "unbound_owner", True))
        routine["native"]["definition"]["proowner"] = "404"
        routine["native"]["acl"] = [[404, 404, "EXECUTE", False]]
    if kind == "unissued_public":
        routine["native"]["acl"].append([0, _WRITER, "EXECUTE", False])
    if kind == "grant_option":
        routine["native"]["acl"][0][-1] = True
    _effective_inventory(case)
    _authenticate(case)
    with pytest.raises(
        ValueError,
        match="outside_caller_unbound|outside_owner_unbound|outside_public_unbound|grant_option_or_acl_changed",
    ):
        _plan(case)


def test_outside_existing_factory_execution_not_reconstructed_from_a_name(outside_case):
    case = outside_case
    name = "provider_directory_rooted_graph_dataset_ready"
    routine = next(row for row in case.inventory["routines"] if row["native"]["definition"]["proname"] == name)
    expected = deepcopy(case.inventory)
    planned = next(row for row in _plan(case)["routines"] if row["proname"] == name)
    assert not any(edge[0] == _PUBLICATION_OWNER for edge in planned["routine_acl"])
    assert case.inventory == expected
