# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add semantic-content-v4 terminal-root retirement.

Revision ID: 20260810120000_provider_directory_terminal_root_retirement_v2
Revises: 20260810110000_provider_directory_reviewed_subset_direct_v4_disposition
"""

from __future__ import annotations

from hashlib import sha256
import os

from alembic import op

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as legacy_evidence,
)
from db import (
    migration_provider_directory_terminal_root_retirement_guards as legacy_guards,
)
from db import migration_provider_directory_terminal_root_retirement_v2 as retirement_v2


revision = "20260810120000_provider_directory_terminal_root_retirement_v2"
down_revision = (
    "20260810110000_provider_directory_reviewed_subset_direct_v4_disposition"
)
branch_labels = None
depends_on = None


_DATASET = "provider_directory_endpoint_dataset"
_IMPORT_RUN = "import_run"
_FROZEN_SCHEMA = "terminal_test"
_FROZEN_SQL_SHA256 = {
    "v1_relation": "3e82da8a9fffc3ef64f97ea91e0717c939ec886db43798f9eaeb3f68f030c4d6",
    "v1_evidence": "d99685d44848d3f8d5d00c2db3b50fd5d5ac5cd397a6e3a5a269e1ae5e8aeefa",
    "v1_eligible": "229f40083f01562d30332d7c222d668576ab297c6970e5ae4afc4ec673733696",
    "v1_marker": "3518d5af21e13d86c8956858b3e2b314f948acc30d1b8b60cc4850282849f769",
    "v1_valid": "a19a17804a39f93dc193676736bdf836c4204c6dc0cf20e2302da24cf856e1a6",
    "v1_run": "78a1cb7212328fee722c203ddbde50db210339ef741ac7653a3abe7773afdeac",
    "v1_parent": "0f104e7ec75583e392e6d742c30d1c3093ecc6856e9e0388d2148cf7fa24544f",
    "v1_child": "3c22a2c483fe2fc01f8e8189c972216c993b382c9f06bac431d5d67a282edbc7",
    "v1_import": "f4d8afa04f0ff261511083ac057cbf9abf63fcec8b6a2a49b7b4a556566a2e6b",
    "v2_evidence": "56cd513884c07a9105dbe87b98625f86fe16b9ffc501b0341e892ba239cb18fc",
    "v2_eligible": "b4a034810993a58b0c35c49af78fe4b750253276fa99472d3baa8143e391f34f",
    "v2_marker": "56c8d306c4140be9374c0b6ff3784987c7f8cf07941af60577a7102f7eaa307e",
    "v2_valid": "dbf5fc6f4e32c0f12c8c92e79e27d6457d7f306fdb6ed8f39cd7f35bad55018e",
    "v2_parent": "1eeb892a667156ca10229d344d73fea963cc731c2cd45405f9adc4281fc2d4d6",
}


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _function_body(rendered_sql: str) -> str:
    prefix = "AS $function$\n"
    suffix = "\n    $function$;"
    if rendered_sql.count(prefix) != 1 or rendered_sql.count(suffix) != 1:
        raise RuntimeError("terminal retirement v2 function body changed")
    return rendered_sql.split(prefix, 1)[1].rsplit(suffix, 1)[0]


def _body_sha256(rendered_sql: str) -> str:
    normalized_body = " ".join(_function_body(rendered_sql).split())
    return sha256(normalized_body.encode("utf-8")).hexdigest()


def _frozen_rendered_sql() -> dict[str, str]:
    schema = _FROZEN_SCHEMA
    return {
        "v1_relation": legacy_evidence.relation_evidence_function_sql(schema),
        "v1_evidence": legacy_evidence.evidence_function_sql(schema),
        "v1_eligible": legacy_guards.eligible_function_sql(schema),
        "v1_marker": legacy_guards.marker_function_sql(schema),
        "v1_valid": legacy_guards.valid_function_sql(schema),
        "v1_run": legacy_guards.run_retired_function_sql(schema),
        "v1_parent": legacy_guards.parent_guard_function_sql(schema),
        "v1_child": legacy_guards.child_guard_function_sql(schema),
        "v1_import": legacy_guards.import_run_guard_function_sql(schema),
        "v2_evidence": retirement_v2.evidence_function_sql(schema),
        "v2_eligible": retirement_v2.eligible_function_sql(schema),
        "v2_marker": retirement_v2.marker_function_sql(schema),
        "v2_valid": retirement_v2.valid_function_sql(schema),
        "v2_parent": retirement_v2.parent_guard_function_sql(schema),
    }


def _assert_frozen_generators() -> None:
    observed = {
        name: sha256(rendered_sql.encode("utf-8")).hexdigest()
        for name, rendered_sql in _frozen_rendered_sql().items()
    }
    if observed != _FROZEN_SQL_SHA256:
        raise RuntimeError("terminal retirement v2 frozen SQL changed")


def _signature(schema: str, name: str, arguments: str) -> str:
    return f"{legacy_evidence._qf(schema, name)}({arguments})"


def _shape_fence_sql(
    schema: str,
    *,
    name: str,
    arguments: str,
    argument_names: tuple[str, ...],
    rendered_sql: str,
    language: str,
    volatility: str,
    return_type: str,
    configuration: tuple[str, ...],
) -> str:
    signature = _signature(schema, name, arguments)
    expected_config = ", ".join(legacy_evidence._ql(value) for value in configuration)
    argument_count = 0 if not arguments else arguments.count(",") + 1
    expected_argument_names = (
        "NULL::text[]"
        if not argument_names
        else "ARRAY["
        + ", ".join(legacy_evidence._ql(value) for value in argument_names)
        + "]::text[]"
    )
    return f"""
    DO $migration$
    DECLARE matching_count bigint;
    BEGIN
        SELECT pg_catalog.count(*) INTO matching_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS language_row
            ON language_row.oid = function_row.prolang
         WHERE function_row.oid = pg_catalog.to_regprocedure(
                   {legacy_evidence._ql(signature)}
               )
           AND namespace_row.nspname = {legacy_evidence._ql(schema)}
           AND function_row.prokind = 'f'
           AND function_row.pronargs = {argument_count}
           AND function_row.pronargdefaults = 0
           AND function_row.proargdefaults IS NULL
           AND function_row.proallargtypes IS NULL
           AND function_row.proargmodes IS NULL
           AND function_row.proargnames IS NOT DISTINCT FROM
                {expected_argument_names}
           AND function_row.provariadic = 0
           AND function_row.prorettype =
                {legacy_evidence._ql(return_type)}::pg_catalog.regtype
           AND function_row.proretset IS FALSE
           AND language_row.lanname = {legacy_evidence._ql(language)}
           AND function_row.provolatile = {legacy_evidence._ql(volatility)}
           AND function_row.proisstrict IS FALSE
           AND function_row.proparallel = 'u'
           AND function_row.prosecdef IS TRUE
           AND function_row.proleakproof IS FALSE
           AND function_row.prosupport = 0
           AND function_row.protrftypes IS NULL
           AND function_row.probin IS NULL
           AND function_row.prosqlbody IS NULL
           AND function_row.procost = 100
           AND function_row.prorows = 0
           AND function_row.proowner = (
                SELECT relation_row.relowner
                  FROM pg_catalog.pg_class AS relation_row
                 WHERE relation_row.oid =
                       {legacy_evidence._ql(legacy_evidence._qf(schema, _DATASET))}
                       ::pg_catalog.regclass
           )
           AND function_row.proconfig IS NOT DISTINCT FROM
                ARRAY[{expected_config}]::text[]
           AND pg_catalog.encode(
                   pg_catalog.sha256(
                       pg_catalog.convert_to(
                           pg_catalog.btrim(
                               pg_catalog.regexp_replace(
                                   function_row.prosrc,
                                   '[[:space:]]+',
                                   ' ',
                                   'g'
                               )
                           ),
                           'UTF8'
                       )
                   ),
                   'hex'
               ) = {legacy_evidence._ql(_body_sha256(rendered_sql))}
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.aclexplode(
                    COALESCE(
                        function_row.proacl,
                        pg_catalog.acldefault('f', function_row.proowner)
                    )
                ) AS function_acl
                WHERE function_acl.privilege_type = 'EXECUTE'
                  AND (
                       function_acl.grantee <> function_row.proowner
                       OR function_acl.grantor <> function_row.proowner
                  )
           );
        IF matching_count <> 1 THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_v2_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _function_spec(
    name: str,
    arguments: str,
    rendered_sql: str,
    *,
    argument_names: tuple[str, ...],
    language: str = "sql",
    volatility: str = "s",
    return_type: str = "boolean",
    configuration: tuple[str, ...] = ("search_path=pg_catalog",),
) -> dict[str, object]:
    return {
        "name": name,
        "arguments": arguments,
        "argument_names": argument_names,
        "rendered_sql": rendered_sql,
        "language": language,
        "volatility": volatility,
        "return_type": return_type,
        "configuration": configuration,
    }


def _legacy_function_specs(schema: str) -> tuple[dict[str, object], ...]:
    utc_config = ("search_path=pg_catalog", "TimeZone=UTC")
    return (
        _function_spec(
            legacy_evidence.RELATION_EVIDENCE_FUNCTION,
            "text,text",
            legacy_evidence.relation_evidence_function_sql(schema),
            argument_names=("candidate_dataset_id", "candidate_relation"),
            language="plpgsql",
            return_type="pg_catalog.jsonb",
            configuration=utc_config,
        ),
        _function_spec(
            legacy_evidence.EVIDENCE_FUNCTION,
            "text",
            legacy_evidence.evidence_function_sql(schema),
            argument_names=("candidate_dataset_id",),
            return_type="pg_catalog.jsonb",
            configuration=utc_config,
        ),
        _function_spec(
            legacy_guards.ELIGIBLE_FUNCTION,
            "text,integer",
            legacy_guards.eligible_function_sql(schema),
            argument_names=("candidate_dataset_id", "minimum_age"),
        ),
        _function_spec(
            legacy_guards.MARKER_FUNCTION,
            "text,jsonb",
            legacy_guards.marker_function_sql(schema),
            argument_names=("candidate_dataset_id", "marker"),
        ),
        _function_spec(
            legacy_guards.VALID_FUNCTION,
            "text",
            legacy_guards.valid_function_sql(schema),
            argument_names=("candidate_dataset_id",),
        ),
        _function_spec(
            legacy_guards.RUN_RETIRED_FUNCTION,
            "text",
            legacy_guards.run_retired_function_sql(schema),
            argument_names=("candidate_run_id",),
        ),
        *(
            _function_spec(
                name,
                "",
                renderer(schema),
                argument_names=(),
                language="plpgsql",
                volatility="v",
                return_type="trigger",
            )
            for name, renderer in (
                (legacy_guards.PARENT_GUARD, legacy_guards.parent_guard_function_sql),
                (legacy_guards.CHILD_GUARD, legacy_guards.child_guard_function_sql),
                (
                    legacy_guards.IMPORT_RUN_GUARD,
                    legacy_guards.import_run_guard_function_sql,
                ),
            )
        ),
    )


def _v2_function_specs(schema: str) -> tuple[dict[str, object], ...]:
    return (
        _function_spec(
            retirement_v2.EVIDENCE_FUNCTION,
            "text",
            retirement_v2.evidence_function_sql(schema),
            argument_names=("candidate_dataset_id",),
            return_type="pg_catalog.jsonb",
            configuration=("search_path=pg_catalog", "TimeZone=UTC"),
        ),
        _function_spec(
            retirement_v2.ELIGIBLE_FUNCTION,
            "text,integer",
            retirement_v2.eligible_function_sql(schema),
            argument_names=("candidate_dataset_id", "minimum_age"),
        ),
        _function_spec(
            retirement_v2.MARKER_FUNCTION,
            "text,jsonb",
            retirement_v2.marker_function_sql(schema),
            argument_names=("candidate_dataset_id", "marker"),
        ),
        _function_spec(
            retirement_v2.VALID_FUNCTION,
            "text",
            retirement_v2.valid_function_sql(schema),
            argument_names=("candidate_dataset_id",),
        ),
    )


def _parent_guard_spec(schema: str, *, dual: bool) -> dict[str, object]:
    rendered_sql = (
        retirement_v2.parent_guard_function_sql(schema)
        if dual
        else legacy_guards.parent_guard_function_sql(schema)
    )
    return _function_spec(
        legacy_guards.PARENT_GUARD,
        "",
        rendered_sql,
        argument_names=(),
        language="plpgsql",
        volatility="v",
        return_type="trigger",
    )


def _function_topology_fence_sql(
    schema: str,
    function_specs: tuple[dict[str, object], ...],
) -> str:
    """Require exactly the named signatures with no same-name overloads."""

    function_names = tuple(str(spec["name"]) for spec in function_specs)
    if len(set(function_names)) != len(function_names):
        raise RuntimeError("terminal retirement v2 function names changed")
    signatures = ", ".join(
        legacy_evidence._ql(
            _signature(schema, str(spec["name"]), str(spec["arguments"]))
        )
        for spec in function_specs
    )
    names = ", ".join(legacy_evidence._ql(name) for name in function_names)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_catalog.unnest(
                ARRAY[{signatures}]::text[]
            ) AS expected(signature)
            WHERE pg_catalog.to_regprocedure(expected.signature) IS NULL
        ) OR (
            SELECT pg_catalog.count(*)
              FROM pg_catalog.pg_proc AS function_row
              JOIN pg_catalog.pg_namespace AS namespace_row
                ON namespace_row.oid = function_row.pronamespace
             WHERE namespace_row.nspname = {legacy_evidence._ql(schema)}
               AND function_row.proname::text = ANY(
                    ARRAY[{names}]::text[]
               )
        ) <> {len(function_specs)} THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_v2_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _trigger_specs(schema: str) -> tuple[tuple[str, str, int, str], ...]:
    child_specs = tuple(
        trigger_spec
        for table_name, suffix in legacy_guards.CHILD_TRIGGER_SUFFIXES.items()
        for trigger_spec in (
            (
                table_name,
                f"pd_trr_{suffix}_row",
                31,
                legacy_guards.CHILD_GUARD,
            ),
            (
                table_name,
                f"pd_trr_{suffix}_truncate",
                34,
                legacy_guards.CHILD_GUARD,
            ),
        )
    )
    return (
        (_DATASET, "pd_trr_dataset_row", 31, legacy_guards.PARENT_GUARD),
        (_DATASET, "pd_trr_dataset_truncate", 34, legacy_guards.PARENT_GUARD),
        (
            _IMPORT_RUN,
            "pd_trr_import_run_row",
            31,
            legacy_guards.IMPORT_RUN_GUARD,
        ),
        (
            _IMPORT_RUN,
            "pd_trr_import_run_truncate",
            34,
            legacy_guards.IMPORT_RUN_GUARD,
        ),
        *child_specs,
    )


def _trigger_topology_fence_sql(schema: str) -> str:
    trigger_specs = _trigger_specs(schema)
    expected_values = ",\n".join(
        "("
        + ", ".join(
            (
                f"{legacy_evidence._ql(legacy_evidence._qf(schema, table_name))}::regclass",
                legacy_evidence._ql(trigger_name),
                str(trigger_type),
                "pg_catalog.to_regprocedure("
                f"{legacy_evidence._ql(_signature(schema, function_name, ''))}"
                ")",
            )
        )
        + ")"
        for table_name, trigger_name, trigger_type, function_name in trigger_specs
    )
    return f"""
    DO $migration$
    DECLARE matching_count bigint; prefixed_count bigint;
    BEGIN
        SELECT pg_catalog.count(*) INTO matching_count
          FROM (VALUES {expected_values}) AS expected(
                relation_oid, trigger_name, trigger_type, function_oid
          )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = expected.relation_oid
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgtype = expected.trigger_type
           AND trigger_row.tgfoid = expected.function_oid
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgconstraint = 0
           AND trigger_row.tgparentid = 0
           AND trigger_row.tgconstrrelid = 0
           AND trigger_row.tgconstrindid = 0
           AND trigger_row.tgdeferrable IS FALSE
           AND trigger_row.tginitdeferred IS FALSE
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND pg_catalog.octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NULL
           AND trigger_row.tgnewtable IS NULL;
        SELECT pg_catalog.count(*) INTO prefixed_count
          FROM pg_catalog.pg_trigger AS trigger_row
          JOIN pg_catalog.pg_class AS relation_row
            ON relation_row.oid = trigger_row.tgrelid
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = relation_row.relnamespace
         WHERE namespace_row.nspname = {legacy_evidence._ql(schema)}
           AND pg_catalog.left(trigger_row.tgname, 7) = 'pd_trr_';
        IF matching_count <> {len(trigger_specs)}
           OR prefixed_count <> {len(trigger_specs)} THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_v2_trigger_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _v2_absent_fence_sql(schema: str) -> str:
    names = ", ".join(
        legacy_evidence._ql(str(function_spec["name"]))
        for function_spec in _v2_function_specs(schema)
    )
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc AS function_row
            JOIN pg_catalog.pg_namespace AS namespace_row
              ON namespace_row.oid = function_row.pronamespace
            WHERE namespace_row.nspname = {legacy_evidence._ql(schema)}
              AND function_row.proname::text = ANY(ARRAY[{names}]::text[])
        ) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_v2_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _adoption_fence_sql(schema: str) -> str:
    dataset = legacy_evidence._qf(schema, _DATASET)
    names = ", ".join(
        legacy_evidence._ql(str(function_spec["name"]))
        for function_spec in _v2_function_specs(schema)
    )
    legacy_valid = legacy_evidence._qf(schema, legacy_guards.VALID_FUNCTION)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {dataset} AS row
             WHERE COALESCE(
                    row.publication_metadata_json::jsonb,
                    '{{}}'::jsonb
                   ) ? {legacy_evidence._ql(retirement_v2.MARKER)}
                OR (
                    COALESCE(
                        row.publication_metadata_json::jsonb,
                        '{{}}'::jsonb
                    ) ? {legacy_evidence._ql(legacy_evidence.MARKER)}
                    AND row.status <> {legacy_evidence._ql(legacy_evidence.STATUS)}
                )
        ) OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc AS function_row
            JOIN pg_catalog.pg_namespace AS namespace_row
              ON namespace_row.oid = function_row.pronamespace
            WHERE namespace_row.nspname = {legacy_evidence._ql(schema)}
              AND function_row.proname::text = ANY(ARRAY[{names}]::text[])
        ) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_v2_adoption_blocked'
                USING ERRCODE = '55000';
        END IF;
        IF EXISTS (
            SELECT 1 FROM {dataset} AS row
             WHERE row.status = {legacy_evidence._ql(legacy_evidence.STATUS)}
               AND {legacy_valid}(row.dataset_id) IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_v1_state_invalid'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _v2_unused_fence_sql(schema: str, error_code: str) -> str:
    dataset = legacy_evidence._qf(schema, _DATASET)
    legacy_valid = legacy_evidence._qf(schema, legacy_guards.VALID_FUNCTION)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {dataset} AS row
             WHERE COALESCE(
                    row.publication_metadata_json::jsonb,
                    '{{}}'::jsonb
                   ) ? {legacy_evidence._ql(retirement_v2.MARKER)}
                OR (
                    COALESCE(
                        row.publication_metadata_json::jsonb,
                        '{{}}'::jsonb
                    ) ? {legacy_evidence._ql(legacy_evidence.MARKER)}
                    AND row.status <> {legacy_evidence._ql(legacy_evidence.STATUS)}
                )
                OR (
                    row.status = {legacy_evidence._ql(legacy_evidence.STATUS)}
                    AND {legacy_valid}(row.dataset_id) IS DISTINCT FROM TRUE
                )
        ) THEN
            RAISE EXCEPTION {legacy_evidence._ql(error_code)}
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _replacement_sql(rendered_sql: str) -> str:
    if rendered_sql.count("CREATE FUNCTION") != 1:
        raise RuntimeError("terminal retirement v2 replacement SQL changed")
    return rendered_sql.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)


def _revoke_sql(schema: str, name: str, arguments: str) -> str:
    return (
        "REVOKE ALL ON FUNCTION " f"{_signature(schema, name, arguments)} FROM PUBLIC;"
    )


def _legacy_dependency_specs(schema: str) -> tuple[dict[str, object], ...]:
    return tuple(
        function_spec
        for function_spec in _legacy_function_specs(schema)
        if function_spec["name"] != legacy_guards.PARENT_GUARD
    )


def _fence_function_specs(
    schema: str,
    function_specs: tuple[dict[str, object], ...],
) -> None:
    for function_spec in function_specs:
        op.execute(_shape_fence_sql(schema, **function_spec))


def upgrade() -> None:
    _assert_frozen_generators()
    schema = _schema()
    dataset = legacy_evidence._qf(schema, _DATASET)
    legacy_specs = _legacy_function_specs(schema)
    v2_specs = _v2_function_specs(schema)
    op.execute(f"LOCK TABLE {dataset} IN SHARE ROW EXCLUSIVE MODE;")
    op.execute(_function_topology_fence_sql(schema, legacy_specs))
    _fence_function_specs(schema, legacy_specs)
    op.execute(_trigger_topology_fence_sql(schema))
    op.execute(_adoption_fence_sql(schema))
    for function_spec in v2_specs:
        op.execute(str(function_spec["rendered_sql"]))
        op.execute(
            _revoke_sql(
                schema,
                str(function_spec["name"]),
                str(function_spec["arguments"]),
            )
        )
    parent_spec = _parent_guard_spec(schema, dual=True)
    op.execute(_replacement_sql(str(parent_spec["rendered_sql"])))
    op.execute(_revoke_sql(schema, legacy_guards.PARENT_GUARD, ""))
    installed_specs = (
        *_legacy_dependency_specs(schema),
        *v2_specs,
        parent_spec,
    )
    op.execute(_function_topology_fence_sql(schema, installed_specs))
    _fence_function_specs(schema, _legacy_dependency_specs(schema))
    _fence_function_specs(schema, v2_specs)
    op.execute(_shape_fence_sql(schema, **parent_spec))
    op.execute(_trigger_topology_fence_sql(schema))


def downgrade() -> None:
    _assert_frozen_generators()
    schema = _schema()
    dataset = legacy_evidence._qf(schema, _DATASET)
    v2_specs = _v2_function_specs(schema)
    dual_parent_spec = _parent_guard_spec(schema, dual=True)
    installed_specs = (
        *_legacy_dependency_specs(schema),
        *v2_specs,
        dual_parent_spec,
    )
    op.execute(f"LOCK TABLE {dataset} IN SHARE ROW EXCLUSIVE MODE;")
    op.execute(_function_topology_fence_sql(schema, installed_specs))
    _fence_function_specs(schema, _legacy_dependency_specs(schema))
    _fence_function_specs(schema, v2_specs)
    op.execute(_shape_fence_sql(schema, **dual_parent_spec))
    op.execute(_trigger_topology_fence_sql(schema))
    op.execute(
        _v2_unused_fence_sql(
            schema,
            "provider_directory_terminal_root_retirement_v2_downgrade_blocked",
        )
    )
    legacy_parent_spec = _parent_guard_spec(schema, dual=False)
    op.execute(_replacement_sql(str(legacy_parent_spec["rendered_sql"])))
    op.execute(_revoke_sql(schema, legacy_guards.PARENT_GUARD, ""))
    for function_spec in reversed(v2_specs):
        op.execute(
            "DROP FUNCTION "
            f"{_signature(schema, str(function_spec['name']), str(function_spec['arguments']))};"
        )
    legacy_specs = _legacy_function_specs(schema)
    op.execute(_function_topology_fence_sql(schema, legacy_specs))
    _fence_function_specs(schema, legacy_specs)
    op.execute(_trigger_topology_fence_sql(schema))
    op.execute(_v2_absent_fence_sql(schema))
