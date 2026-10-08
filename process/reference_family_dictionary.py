# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compiled terminal dictionary domains and native key-scoped effects.

COPY, admission, retention and publication remain in the shared family pipeline.
"""

from dataclasses import dataclass, replace

from sqlalchemy import Column, MetaData, Table, text
from sqlalchemy.dialects import postgresql

from db import models

_CLAIMS_DICTIONARY_SOURCE = "cms_physician_provider_service"


async def validate_claims_dictionary_closure(session, schema_name):
    """Require the complete current procedure/code slice through indexed set checks."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    schema = native._quoted(native._schema_name(schema_name))
    catalog = f"{schema}.claims_code_catalog"
    crosswalk = f"{schema}.claims_code_crosswalk"
    for model in _CLAIMS_SCOPED_MODELS:
        if await session.scalar(text(f"SELECT count(*)>100000 FROM {schema}.{native._quoted(model.__tablename__)}")):
            raise native.ReferenceFamilyArchiveError("claims dictionary publication bound is exceeded")
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {schema}.{native._quoted(model.__tablename__)} WHERE source IS DISTINCT FROM :source)"
            ),
            {"source": _CLAIMS_DICTIONARY_SOURCE},
        ):
            raise native.ReferenceFamilyArchiveError("claims dictionary contains a foreign source")
    for system, code in (("from_system", "from_code"), ("to_system", "to_code")):
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {crosswalk} edge WHERE NOT EXISTS(SELECT 1 FROM {catalog} code WHERE code.code_system=edge.{system} AND code.code=edge.{code}))"
            )
        ):
            raise native.ReferenceFamilyArchiveError("claims crosswalk endpoint is missing")
    await _validate_claims_procedures(session, schema, catalog, crosswalk)


async def _validate_claims_procedures(session, schema, catalog, crosswalk):
    """Preserve the producer's exact six procedure-edge forms independently of Rx closure."""
    from api.code_systems import INTERNAL_PROCEDURE_CODE_SYSTEM
    from process import reference_family_archive as native

    if await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {schema}.pricing_procedure procedure
        WHERE UPPER(BTRIM(procedure.reported_code)) ~ '^[A-Z0-9]{{5}}$'
          AND UPPER(BTRIM(procedure.reported_code)) ~ '[0-9]'
          AND NOT EXISTS(SELECT 1 FROM {catalog} code WHERE code.code_system=:internal
                         AND code.code=procedure.procedure_code::text))
    """),
        {"internal": INTERNAL_PROCEDURE_CODE_SYSTEM},
    ):
        raise native.ReferenceFamilyArchiveError("claims procedure catalog closure is missing")
    if await session.scalar(
        text(f"""
        WITH src AS (
          SELECT procedure_code::text internal_code,UPPER(BTRIM(reported_code)) code,
            CASE WHEN UPPER(BTRIM(reported_code)) ~ '^[0-9]{{5}}$' THEN 'CPT'
                 WHEN UPPER(BTRIM(reported_code)) ~ '^D[0-9]{{4}}$' THEN 'CDT' ELSE 'HCPCS' END system
          FROM {schema}.pricing_procedure
          WHERE UPPER(BTRIM(reported_code)) ~ '^[A-Z0-9]{{5}}$' AND UPPER(BTRIM(reported_code)) ~ '[0-9]'
        ), expected AS (
          SELECT system from_system,code from_code,:internal to_system,internal_code to_code FROM src
          UNION ALL SELECT :internal,internal_code,system,code FROM src
          UNION ALL SELECT system,code,'HCPCS',code FROM src WHERE system IN ('CPT','CDT')
          UNION ALL SELECT 'HCPCS',code,system,code FROM src WHERE system IN ('CPT','CDT')
          UNION ALL SELECT 'HCPCS',code,:internal,internal_code FROM src WHERE system IN ('CPT','CDT')
          UNION ALL SELECT :internal,internal_code,'HCPCS',code FROM src WHERE system IN ('CPT','CDT')
        ) SELECT EXISTS(SELECT 1 FROM expected e WHERE NOT EXISTS(
          SELECT 1 FROM {crosswalk} edge WHERE
            (edge.from_system,edge.from_code,edge.to_system,edge.to_code)=
            (e.from_system,e.from_code,e.to_system,e.to_code)))
    """),
        {"internal": INTERNAL_PROCEDURE_CODE_SYSTEM},
    ):
        raise native.ReferenceFamilyArchiveError("claims procedure crosswalk closure is missing")


async def replace_claims_dictionary_slice(session, *, incoming_schema, current_schema, destination_schema):
    """CAS an exact owned slice while preserving every unrelated key and payload."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    incoming_schema = native._schema_name(incoming_schema)
    destination_schema = native._schema_name(destination_schema)
    current_schema = None if current_schema is None else native._schema_name(current_schema)
    await _lock_reference_dictionary(session, destination_schema)
    await native.validate_claims_dictionary_closure(session, incoming_schema)
    for model in _CLAIMS_SCOPED_MODELS:
        live = f"{native._quoted(destination_schema)}.{native._quoted(model.__source_table__)}"
        incoming = f"{native._quoted(incoming_schema)}.{native._quoted(model.__tablename__)}"
        if current_schema is None:
            if await session.scalar(
                text(f"SELECT EXISTS(SELECT 1 FROM {live} WHERE source=:source)"), {"source": _CLAIMS_DICTIONARY_SOURCE}
            ):
                raise native.ReferenceFamilyArchiveError("unregistered claims dictionary predecessor")
        elif not await native._is_model_table_equal(
            session,
            model,
            left_schema=destination_schema,
            left_name=model.__source_table__,
            right_schema=current_schema,
            right_name=model.__tablename__,
            left_predicate=_source_model_predicate(model),
        ):
            raise native.ReferenceFamilyArchiveError("claims dictionary predecessor changed")
        join = " AND ".join(
            f"live.{native._quoted(column.name)}=incoming.{native._quoted(column.name)}"
            for column in model.__table__.primary_key.columns
        )
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {incoming} incoming JOIN {live} live ON {join} WHERE live.source IS DISTINCT FROM :source)"
            ),
            {"source": _CLAIMS_DICTIONARY_SOURCE},
        ):
            raise native.ReferenceFamilyArchiveError("claims dictionary key belongs to another source")
    # Validate both collision sets before the first shared mutation.
    for model in _CLAIMS_SCOPED_MODELS:
        live = f"{native._quoted(destination_schema)}.{native._quoted(model.__source_table__)}"
        incoming = f"{native._quoted(incoming_schema)}.{native._quoted(model.__tablename__)}"
        columns = ",".join(native._quoted(column.name) for column in model.__table__.columns)
        await session.execute(text(f"DELETE FROM {live} WHERE source=:source"), {"source": _CLAIMS_DICTIONARY_SOURCE})
        await session.execute(text(f"INSERT INTO {live} ({columns}) SELECT {columns} FROM {incoming}"))
        if not await native._is_model_table_equal(
            session,
            model,
            left_schema=destination_schema,
            left_name=model.__source_table__,
            right_schema=incoming_schema,
            right_name=model.__tablename__,
            left_predicate=_source_model_predicate(model),
        ):
            raise native.ReferenceFamilyArchiveError("claims dictionary publication differs")


class ClaimsCodeCatalog:
    """Immutable native copy of the exact claims-owned shared catalog slice."""

    __tablename__ = "claims_code_catalog"
    __source_table__ = "code_catalog"
    __table__ = models.CodeCatalog.__table__.to_metadata(MetaData(), name=__tablename__)
    __my_additional_indexes__ = models.CodeCatalog.__my_additional_indexes__


class ClaimsCodeCrosswalk:
    """Immutable native copy of the exact claims-owned shared crosswalk slice."""

    __tablename__ = "claims_code_crosswalk"
    __source_table__ = "code_crosswalk"
    __table__ = models.CodeCrosswalk.__table__.to_metadata(MetaData(), name=__tablename__)
    __my_additional_indexes__ = models.CodeCrosswalk.__my_additional_indexes__


_CLAIMS_SCOPED_MODELS = (ClaimsCodeCatalog, ClaimsCodeCrosswalk)


class DrugClaimsCodeCatalog:
    """Complete native payloads for the prescription family's referenced codes."""

    __tablename__ = "drug_claims_code_catalog"
    __source_table__ = "code_catalog"
    __table__ = models.CodeCatalog.__table__.to_metadata(MetaData(), name=__tablename__)
    __my_additional_indexes__ = models.CodeCatalog.__my_additional_indexes__


class DrugClaimsCodeCrosswalk:
    """Complete native edges touching the prescription family's internal keys."""

    __tablename__ = "drug_claims_code_crosswalk"
    __source_table__ = "code_crosswalk"
    __table__ = models.CodeCrosswalk.__table__.to_metadata(MetaData(), name=__tablename__)
    __my_additional_indexes__ = models.CodeCrosswalk.__my_additional_indexes__


def _dictionary_effect_model(name, model):
    """Compile native key/preimage heaps; no executable expressions or row hooks."""
    table = Table(
        name,
        MetaData(),
        *(Column(column.name, column.type, primary_key=True) for column in model.__table__.primary_key.columns),
        Column("baseline_image", postgresql.JSONB),
        Column("before_image", postgresql.JSONB),
        Column("after_image", postgresql.JSONB),
        Column("destination_oid", postgresql.OID, nullable=False),
    )
    return type(name.title().replace("_", ""), (), {"__tablename__": name, "__table__": table})


_DRUG_SCOPED_MODELS = (DrugClaimsCodeCatalog, DrugClaimsCodeCrosswalk)
_DRUG_EFFECT_MODELS = tuple(
    _dictionary_effect_model(name, model)
    for name, model in zip(
        ("drug_claims_catalog_effect", "drug_claims_crosswalk_effect"), _DRUG_SCOPED_MODELS, strict=True
    )
)


@dataclass(frozen=True)
class TerminalReferenceCapability:
    """Two genuine producer domains sharing the same protected native mechanics."""

    importers: tuple[str, ...]
    phase: str
    dictionary_models: tuple[type, ...]
    effect_models: tuple[type, ...] = ()


TERMINAL_CAPABILITIES = {
    "claims-pricing": TerminalReferenceCapability(
        ("claims-pricing", "claims-procedures"), "claims-pricing finalized", _CLAIMS_SCOPED_MODELS
    ),
    "drug-claims": TerminalReferenceCapability(
        ("drug-claims",), "drug-claims finalized", _DRUG_SCOPED_MODELS, _DRUG_EFFECT_MODELS
    ),
}


def _ownership_spec(ownership):
    from process import reference_family_archive as native

    spec = native.reference_family_spec(
        ownership.importer_id,
        canonical=ownership.importer_id == "mrf" and native.STAGE_TABLE in dict(ownership.relation_oids),
    )
    if ownership.importer_id == "drug-claims" and any(
        name in dict(ownership.relation_oids)
        for name in reference_family_receive_spec("drug-claims").table_names
        if name not in spec.table_names
    ):
        spec = reference_family_receive_spec("drug-claims")
    return spec


def reference_family_receive_spec(importer_id):
    """Receive keeps exact destination effects; portable SOURCE contains only genuine result models."""
    from process import reference_family_archive as native

    spec = native.reference_family_spec(importer_id)
    capability = TERMINAL_CAPABILITIES.get(importer_id)
    return replace(spec, model_types=(*spec.model_types, *capability.effect_models)) if capability else spec


def terminal_incumbent_presence(importer_id, pairs):
    """Allow complete ordinary serving heaps only while every scoped heap remains absent."""
    from process import reference_family_archive as native

    capability = TERMINAL_CAPABILITIES[importer_id]
    scoped_names = {model.__tablename__ for model in (*capability.dictionary_models, *capability.effect_models)}
    scoped_flags = [oid is not None for name, oid in pairs if name in scoped_names]
    if any(scoped_flags) and not all(scoped_flags):
        raise native.ReferenceFamilyArchiveError("terminal reference dictionary family is incomplete")
    return [oid is not None for name, oid in pairs if all(scoped_flags) or name not in scoped_names]


async def require_terminal_empty_incumbent(session, incumbent):
    """An unregistered ordinary predecessor must be native and entirely empty, not adopted data."""
    from process import reference_family_archive as native

    pairs = [(name, oid) for name, oid in incumbent.relation_oids if oid is not None]
    if not pairs:
        return
    await native.require_native_read_catalog(session, tuple(oid for _name, oid in pairs))
    expression = " OR ".join(
        f"EXISTS(SELECT 1 FROM {native._quoted(incumbent.schema_name)}.{native._quoted(name)})" for name, _oid in pairs
    )
    if await session.scalar(text("SELECT " + expression)) is not False:
        raise native.ReferenceFamilyArchiveError("unregistered terminal reference predecessor is not empty")


def _source_model_name(model):
    """Use only a compiled reviewed source name, never a request selector."""
    return getattr(model, "__source_table__", model.__tablename__)


def _source_model_predicate(model, schema_name="mrf"):
    """Select the exact fixed claims producer slice for native acquisition."""
    from process import reference_family_archive as native

    if model in _CLAIMS_SCOPED_MODELS:
        return f"WHERE canonical.source='{_CLAIMS_DICTIONARY_SOURCE}'"
    if model in _DRUG_SCOPED_MODELS:
        schema = native._quoted(native._schema_name(schema_name))
        keys = f"{schema}.pricing_prescription"

        def internal(system, code):
            """Match only a compiled internal prescription key."""
            return f"EXISTS(SELECT 1 FROM {keys} rx WHERE rx.rx_code_system={system} AND rx.rx_code={code})"

        if model is DrugClaimsCodeCrosswalk:
            return (
                "WHERE "
                + internal("canonical.from_system", "canonical.from_code")
                + " OR "
                + internal("canonical.to_system", "canonical.to_code")
            )
        crosswalk_ref = f"{schema}.code_crosswalk"
        return (
            "WHERE "
            + internal("canonical.code_system", "canonical.code")
            + f" OR EXISTS(SELECT 1 FROM {crosswalk_ref} edge WHERE ("
            + internal("edge.from_system", "edge.from_code")
            + " OR "
            + internal("edge.to_system", "edge.to_code")
            + ") AND ("
            + "(canonical.code_system,canonical.code)=(edge.from_system,edge.from_code) OR "
            + "(canonical.code_system,canonical.code)=(edge.to_system,edge.to_code)))"
        )
    return ""


async def validate_prescription_dictionary_closure(session, schema_name):
    """Require exact internal/endpoint closure and symmetric optional Rx mappings."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    schema = native._quoted(native._schema_name(schema_name))
    catalog = f"{schema}.{native._quoted(DrugClaimsCodeCatalog.__tablename__)}"
    crosswalk_ref = f"{schema}.{native._quoted(DrugClaimsCodeCrosswalk.__tablename__)}"
    prescription_ref = f"{schema}.pricing_prescription"
    for model in _DRUG_SCOPED_MODELS:
        if await session.scalar(text(f"SELECT count(*)>100000 FROM {schema}.{native._quoted(model.__tablename__)}")):
            raise native.ReferenceFamilyArchiveError("claims dictionary publication bound is exceeded")
    for system, code in (("from_system", "from_code"), ("to_system", "to_code")):
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {crosswalk_ref} edge WHERE NOT EXISTS(SELECT 1 FROM {catalog} code WHERE (code.code_system,code.code)=(edge.{system},edge.{code})))"
            )
        ):
            raise native.ReferenceFamilyArchiveError("prescription crosswalk endpoint is missing")
    if await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {prescription_ref} rx WHERE rx.rx_code_system<>'HP_RX_CODE'
          OR NOT EXISTS(SELECT 1 FROM {catalog} code WHERE (code.code_system,code.code)=(rx.rx_code_system,rx.rx_code))
          OR NOT EXISTS(SELECT 1 FROM {crosswalk_ref} edge WHERE
             (edge.from_system,edge.from_code,edge.to_system,edge.to_code)=
             (rx.rx_code_system,rx.rx_code,rx.rx_code_system,rx.rx_code)
             AND edge.match_type='exact' AND edge.confidence=1))
    """)
    ):
        raise native.ReferenceFamilyArchiveError("prescription internal identity closure is missing")
    for name in ("pricing_provider_prescription", "pricing_provider_rx_rollup"):
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {schema}.{name} provider WHERE NOT EXISTS(SELECT 1 FROM {prescription_ref} rx WHERE (rx.rx_code_system,rx.rx_code)=(provider.rx_code_system,provider.rx_code)))"
            )
        ):
            raise native.ReferenceFamilyArchiveError("prescription serving dimension closure is missing")
    await _validate_prescription_edges(session, prescription_ref, catalog, crosswalk_ref)
    await validate_prescription_rollup(session, schema_name)


async def _validate_prescription_edges(session, prescription_ref, catalog, crosswalk_ref):
    """Reject unrelated endpoints, asymmetric mappings and non-native external systems."""
    from process import reference_family_archive as native

    if await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {crosswalk_ref} edge WHERE NOT (
          EXISTS(SELECT 1 FROM {prescription_ref} rx WHERE
            (rx.rx_code_system,rx.rx_code)=(edge.from_system,edge.from_code)) OR
          EXISTS(SELECT 1 FROM {prescription_ref} rx WHERE
            (rx.rx_code_system,rx.rx_code)=(edge.to_system,edge.to_code))) OR
          NOT EXISTS(SELECT 1 FROM {crosswalk_ref} reverse WHERE
            (reverse.from_system,reverse.from_code,reverse.to_system,reverse.to_code)=
            (edge.to_system,edge.to_code,edge.from_system,edge.from_code)
            AND (reverse.match_type,reverse.confidence) IS NOT DISTINCT FROM (edge.match_type,edge.confidence)))
    """)
    ):
        raise native.ReferenceFamilyArchiveError("prescription symmetric crosswalk closure is missing")
    if await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {catalog} code WHERE NOT (
          EXISTS(SELECT 1 FROM {prescription_ref} rx WHERE (rx.rx_code_system,rx.rx_code)=(code.code_system,code.code)) OR
          EXISTS(SELECT 1 FROM {crosswalk_ref} edge WHERE (code.code_system,code.code)=(edge.from_system,edge.from_code)
             OR (code.code_system,code.code)=(edge.to_system,edge.to_code))))
    """)
    ):
        raise native.ReferenceFamilyArchiveError("prescription catalog contains an unrelated key")
    if await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {crosswalk_ref} edge WHERE NOT (
          (edge.from_system='HP_RX_CODE' AND edge.to_system IN ('NDC','RXNORM')) OR
          (edge.to_system='HP_RX_CODE' AND edge.from_system IN ('NDC','RXNORM')) OR
          (edge.from_system='HP_RX_CODE' AND edge.to_system='HP_RX_CODE' AND edge.from_code=edge.to_code))
          OR edge.match_type IS NULL OR edge.match_type NOT IN ('exact','normalized')
          OR edge.confidence IS NULL OR edge.confidence<0 OR edge.confidence>1)
    """)
    ):
        raise native.ReferenceFamilyArchiveError("prescription crosswalk domain is invalid")


async def validate_prescription_rollup(session, schema_name, *, require_binding=False):
    """Compare the actual producer aggregate, excluding only the portable OID binding."""
    from db.prescription_autocomplete_rollup_sql import prescription_autocomplete_rollup_insert_sql
    from process import reference_family_archive as native

    schema = native._schema_name(schema_name)
    model = models.PricingProviderPrescriptionAutocomplete
    columns = ",".join(
        native._quoted(column.name)
        for column in model.__table__.columns
        if column.name != "source_relation_fingerprint"
    )
    expected = prescription_autocomplete_rollup_insert_sql(
        schema=schema,
        rollup_table=model.__tablename__,
        provider_table="pricing_provider_prescription",
        select_only=True,
    )
    actual = f"SELECT {columns} FROM {native._quoted(schema)}.{native._quoted(model.__tablename__)}"
    if await session.scalar(
        text(f"""
        WITH expected AS ({expected}), differences AS (
          ({actual} EXCEPT ALL SELECT {columns} FROM expected) UNION ALL
          (SELECT {columns} FROM expected EXCEPT ALL {actual}))
        SELECT EXISTS(SELECT 1 FROM differences)
    """)
    ):
        raise native.ReferenceFamilyArchiveError("prescription autocomplete aggregate differs")
    if require_binding and await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {native._quoted(schema)}.{native._quoted(model.__tablename__)}
          WHERE source_relation_fingerprint IS DISTINCT FROM
          to_regclass(:provider)::oid::text)
    """),
        {"provider": f'{native._quoted(schema)}."pricing_provider_prescription"'},
    ):
        raise native.ReferenceFamilyArchiveError("prescription autocomplete binding differs")


def _dictionary_key_join(model, left, right):
    from process import reference_family_archive as native

    return " AND ".join(
        f"{left}.{native._quoted(column.name)}={right}.{native._quoted(column.name)}"
        for column in model.__table__.primary_key.columns
    )


async def _lock_reference_dictionary(session, schema_name):
    """Authenticate both native global heaps before any scoped reads or writes."""
    from process import reference_family_archive as native

    names = ("code_catalog", "code_crosswalk")
    oids = tuple([await native._relation_oid(session, schema_name, name) for name in names])
    if any(oid is None for oid in oids):
        raise native.ReferenceFamilyArchiveError("claims shared dictionary is unavailable")
    await native._lock_family(session, schema_name, names, "SHARE ROW EXCLUSIVE", nowait=True)
    if tuple([await native._relation_oid(session, schema_name, name) for name in names]) != oids:
        raise native.ReferenceFamilyArchiveError("claims shared dictionary identity changed")
    await native.require_native_read_catalog(session, oids)
    return oids


async def prepare_reference_dictionary_effects(session, ownership, *, destination_schema="mrf", max_bytes):
    """Capture bounded native preimages before receive writes close; never change shared rows."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    if ownership.importer_id != "drug-claims" or type(max_bytes) is not int or max_bytes <= 0:
        raise native.ReferenceFamilyArchiveError("reference dictionary effect scope is invalid")
    await native.verify_reference_family_stage_ownership(session, ownership)
    await validate_prescription_dictionary_closure(session, ownership.schema_name)
    await session.execute(
        text(f"""
        UPDATE {native._quoted(ownership.schema_name)}.pricing_provider_rx_rollup
        SET source_relation_fingerprint=to_regclass(:provider)::oid::text
    """),
        {"provider": f'{native._quoted(ownership.schema_name)}."pricing_provider_prescription"'},
    )
    current = await native.capture_reference_family_incumbent(
        session, importer_id="drug-claims", schema_name=destination_schema
    )
    scoped_names = {model.__tablename__ for model in (*_DRUG_SCOPED_MODELS, *_DRUG_EFFECT_MODELS)}
    current_present = any(oid is not None for name, oid in current.relation_oids if name in scoped_names)
    if current_present:
        await _require_terminal_current_binding(session, current)
    else:
        await require_terminal_empty_incumbent(session, current)
    oids = await _lock_reference_dictionary(session, destination_schema)
    for mirror, effect, oid in zip(_DRUG_SCOPED_MODELS, _DRUG_EFFECT_MODELS, oids, strict=True):
        await _capture_dictionary_effect_table(
            session, ownership.schema_name, destination_schema, mirror, effect, oid, current_present
        )
    size = await session.scalar(
        text(
            "SELECT "
            + " + ".join(
                f"COALESCE((SELECT sum(pg_column_size(row_value))::bigint FROM {native._quoted(ownership.schema_name)}.{native._quoted(model.__tablename__)} row_value),0)"
                for model in native.reference_family_receive_spec("drug-claims").model_types
            )
        )
    )
    if type(size) is not int or size > max_bytes:
        raise native.ReferenceFamilyArchiveError("reference dictionary effect byte bound is exceeded")


async def _require_terminal_current_binding(session, incumbent):
    """A fabricated mirror/effect heap is not an installed predecessor."""
    from process import reference_family_archive as native

    metadata = tuple(
        [
            await native._relation_oid(session, "hp_snapshot_retention", name)
            for name in ("current_generation", "relation")
        ]
    )
    await native.require_native_read_catalog(session, metadata)
    rows = (
        await session.execute(
            text("""
        SELECT c.generation_id::text,heap.relname::text AS relation_name,r.relation_oid::bigint
        FROM hp_snapshot_retention.current_generation c JOIN hp_snapshot_retention.relation r USING(generation_id)
        JOIN pg_catalog.pg_class heap ON heap.oid::bigint=r.relation_oid
        WHERE c.importer_id=:importer AND c.dataset_key=:importer ORDER BY heap.relname
    """),
            {"importer": incumbent.importer_id},
        )
    ).all()
    if len({row[0] for row in rows}) != 1 or tuple((row[1], row[2]) for row in rows) != tuple(
        sorted(incumbent.relation_oids)
    ):
        raise native.ReferenceFamilyArchiveError("reference dictionary predecessor is unregistered")


async def _capture_dictionary_effect_table(
    session, stage_schema, destination_schema, mirror, effect, oid, current_present
):
    from process import reference_family_archive as native

    stage, destination = native._quoted(stage_schema), native._quoted(destination_schema)
    effect_ref, incoming, live = (
        f"{stage}.{native._quoted(effect.__tablename__)}",
        f"{stage}.{native._quoted(mirror.__tablename__)}",
        f"{destination}.{native._quoted(mirror.__source_table__)}",
    )
    if await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {effect_ref})")):
        raise native.ReferenceFamilyArchiveError("reference dictionary effects already exist")
    keys = ",".join(native._quoted(column.name) for column in mirror.__table__.primary_key.columns)
    current_ref = f"{destination}.{native._quoted(effect.__tablename__)}"
    if current_present:
        key_query = f"SELECT {keys} FROM {incoming} UNION SELECT {keys} FROM {current_ref}"
        baseline = (
            "CASE WHEN current_row.destination_oid IS NULL THEN to_jsonb(live) ELSE current_row.baseline_image END"
        )
        current_join = f"LEFT JOIN {current_ref} current_row ON {_dictionary_key_join(mirror, 'keys', 'current_row')}"
    else:
        key_query, baseline, current_join = f"SELECT {keys} FROM {incoming}", "to_jsonb(live)", ""
    collision = (
        (
            "current_row.destination_oid IS NOT NULL AND (current_row.destination_oid<>:oid OR current_row.after_image IS DISTINCT FROM to_jsonb(live)) OR "
            "current_row.destination_oid IS NULL AND "
            if current_present
            else ""
        )
        + "to_jsonb(live) IS NOT NULL AND to_jsonb(incoming) IS NOT NULL AND to_jsonb(live) IS DISTINCT FROM to_jsonb(incoming)"
    )
    joins = f"FROM ({key_query}) keys LEFT JOIN {live} live ON {_dictionary_key_join(mirror, 'keys', 'live')} LEFT JOIN {incoming} incoming ON {_dictionary_key_join(mirror, 'keys', 'incoming')} {current_join}"
    if await session.scalar(text(f"SELECT EXISTS(SELECT 1 {joins} WHERE {collision})"), {"oid": oid}):
        raise native.ReferenceFamilyArchiveError("reference dictionary predecessor or foreign key changed")
    await session.execute(
        text(f"""
        INSERT INTO {effect_ref}({keys},baseline_image,before_image,after_image,destination_oid)
        SELECT {",".join("keys." + native._quoted(column.name) for column in mirror.__table__.primary_key.columns)},
          {baseline},to_jsonb(live),CASE WHEN to_jsonb(incoming) IS NULL THEN {baseline} ELSE to_jsonb(incoming) END,:oid
        {joins}
    """),
        {"oid": oid},
    )
    if await session.scalar(text(f"SELECT count(*)>100000 FROM {effect_ref}")):
        raise native.ReferenceFamilyArchiveError("claims dictionary publication bound is exceeded")


async def validate_reference_dictionary_effects(session, ownership):
    """Authenticate the closed local effect domain, including absent and removed keys."""
    from process import reference_family_archive as native

    if native._ownership_spec(ownership) != native.reference_family_receive_spec("drug-claims"):
        raise native.ReferenceFamilyArchiveError("reference dictionary effect inventory is incomplete")
    await validate_prescription_rollup(session, ownership.schema_name, require_binding=True)
    schema = native._quoted(ownership.schema_name)
    for mirror, effect in zip(_DRUG_SCOPED_MODELS, _DRUG_EFFECT_MODELS, strict=True):
        incoming, effects = (
            f"{schema}.{native._quoted(mirror.__tablename__)}",
            f"{schema}.{native._quoted(effect.__tablename__)}",
        )
        join = _dictionary_key_join(mirror, "incoming", "effect")
        images = " OR ".join(
            f"(effect.{image} IS NOT NULL AND (jsonb_typeof(effect.{image})<>'object' OR "
            + " OR ".join(
                f"effect.{image}->>'{column.name}' IS DISTINCT FROM effect.{native._quoted(column.name)}::text"
                for column in mirror.__table__.primary_key.columns
            )
            + f" OR to_jsonb(jsonb_populate_record(NULL::{incoming},effect.{image})) IS DISTINCT FROM effect.{image}))"
            for image in ("baseline_image", "before_image", "after_image")
        )
        if await session.scalar(
            text(f"""
            SELECT EXISTS(SELECT 1 FROM {incoming} incoming LEFT JOIN {effects} effect ON {join}
              WHERE effect.destination_oid IS NULL OR effect.after_image IS DISTINCT FROM to_jsonb(incoming))
              OR EXISTS(SELECT 1 FROM {effects} effect LEFT JOIN {incoming} incoming ON {join}
              WHERE effect.destination_oid IS NULL OR effect.destination_oid=0 OR {images}
              OR (to_jsonb(incoming) IS NULL AND effect.after_image IS DISTINCT FROM effect.baseline_image))
              OR (SELECT count(*)>100000 OR count(DISTINCT destination_oid)>1 FROM {effects})
        """)
        ):
            raise native.ReferenceFamilyArchiveError("reference dictionary effect closure differs")


async def apply_reference_dictionary_effects(
    session, *, incoming_schema, current_schema, destination_schema="mrf", rollback=False
):
    """CAS all exact key preimages, then publish or restore in the owning transaction."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    oids = await _lock_reference_dictionary(session, destination_schema)
    for mirror, effect, oid in zip(_DRUG_SCOPED_MODELS, _DRUG_EFFECT_MODELS, oids, strict=True):
        effects = f"{native._quoted(incoming_schema)}.{native._quoted(effect.__tablename__)}"
        live = f"{native._quoted(destination_schema)}.{native._quoted(mirror.__source_table__)}"
        fence = f"{native._quoted(current_schema)}.{native._quoted(effect.__tablename__)}" if rollback else effects
        image = "after_image" if rollback else "before_image"
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {fence} effect LEFT JOIN {live} live ON {_dictionary_key_join(mirror, 'effect', 'live')} WHERE effect.destination_oid<>:oid OR effect.{image} IS DISTINCT FROM to_jsonb(live))"
            ),
            {"oid": oid},
        ):
            raise native.ReferenceFamilyArchiveError("reference dictionary destination changed")
        if rollback and await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {effects} effect LEFT JOIN {live} live ON {_dictionary_key_join(mirror, 'effect', 'live')} WHERE effect.destination_oid<>:oid OR (NOT EXISTS(SELECT 1 FROM {fence} current_row WHERE {_dictionary_key_join(mirror, 'effect', 'current_row')}) AND effect.baseline_image IS DISTINCT FROM to_jsonb(live)))"
            ),
            {"oid": oid},
        ):
            raise native.ReferenceFamilyArchiveError("reference dictionary rollback key changed")
    for mirror, effect in zip(_DRUG_SCOPED_MODELS, _DRUG_EFFECT_MODELS, strict=True):
        await _write_dictionary_effects(
            session, mirror, effect, incoming_schema, current_schema, destination_schema, rollback
        )


async def _write_dictionary_effects(
    session, mirror, effect, incoming_schema, current_schema, destination_schema, rollback
):
    from process import reference_family_archive as native

    effects = f"{native._quoted(incoming_schema)}.{native._quoted(effect.__tablename__)}"
    live = f"{native._quoted(destination_schema)}.{native._quoted(mirror.__source_table__)}"
    if rollback:
        current = f"{native._quoted(current_schema)}.{native._quoted(effect.__tablename__)}"
        keys = ",".join(native._quoted(column.name) for column in mirror.__table__.primary_key.columns)
        projection = f"SELECT {keys},after_image image FROM {effects} UNION ALL SELECT {','.join('current.' + native._quoted(column.name) for column in mirror.__table__.primary_key.columns)},current.baseline_image FROM {current} current WHERE NOT EXISTS(SELECT 1 FROM {effects} target WHERE {_dictionary_key_join(mirror, 'current', 'target')})"
    else:
        projection = f"SELECT *,after_image image FROM {effects}"
    columns = ",".join(native._quoted(column.name) for column in mirror.__table__.columns)
    await session.execute(
        text(
            f"DELETE FROM {live} live USING ({projection}) effect WHERE {_dictionary_key_join(mirror, 'live', 'effect')} AND to_jsonb(live) IS DISTINCT FROM effect.image"
        )
    )
    await session.execute(
        text(
            f"INSERT INTO {live}({columns}) SELECT {','.join('row_value.' + native._quoted(column.name) for column in mirror.__table__.columns)} FROM ({projection}) effect CROSS JOIN LATERAL jsonb_populate_record(NULL::{live},effect.image) row_value WHERE effect.image IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {live} live WHERE {_dictionary_key_join(mirror, 'live', 'effect')})"
        )
    )
    if await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM ({projection}) effect LEFT JOIN {live} live ON {_dictionary_key_join(mirror, 'live', 'effect')} WHERE effect.image IS DISTINCT FROM to_jsonb(live))"
        )
    ):
        raise native.ReferenceFamilyArchiveError("reference dictionary publication differs")
