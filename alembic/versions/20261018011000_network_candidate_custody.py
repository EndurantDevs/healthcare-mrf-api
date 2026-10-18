# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain independent candidate custody outside ordinary publisher writes."""

from sqlalchemy import MetaData, text

from alembic import op
from db.models.network_serving import NetworkCandidateCustody, NetworkMembershipCandidate
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261018011000_network_candidate_custody"
down_revision = (
    "20261017040000_nppes_evidence_candidates",
    "20261017210000_provider_directory_readiness_indexes",
)
branch_labels = None
depends_on = None


def _table(schema):
    metadata = MetaData()
    for model in (NetworkMembershipCandidate, NetworkCandidateCustody):
        model.__table__.to_metadata(
            metadata,
            schema=schema,
            referred_schema_fn=lambda _table, _schema, _constraint, _referred: schema,
        )
    return metadata.tables[schema + ".network_candidate_custody"]


def upgrade():
    """Create one control ledger and preserve existing explicit candidate readers."""
    bind = op.get_bind()
    schema = registry_schema()
    namespace = _identifier(schema)
    owner = bind.execute(
        text(
            "SELECT role.rolname FROM pg_namespace n JOIN pg_roles role ON role.oid=n.nspowner WHERE n.nspname=:schema"
        ),
        {"schema": schema},
    ).scalar_one()
    readers = list(
        bind.execute(
            text(
                "SELECT DISTINCT role.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl "
                "JOIN pg_roles role ON role.oid=acl.grantee WHERE n.nspname=:schema "
                "AND c.relname='network_membership_candidate' AND acl.privilege_type='SELECT' "
                "AND acl.grantee<>c.relowner AND NOT acl.is_grantable ORDER BY role.rolname"
            ),
            {"schema": schema},
        ).scalars()
    )
    if len(readers) > 128:
        raise RuntimeError("Candidate custody reader inventory exceeds its bound")
    _table(schema).create(bind)
    relation = namespace + ".network_candidate_custody"
    bind.execute(text(f"ALTER TABLE {relation} OWNER TO {_identifier(owner)}"))
    bind.execute(text(f"REVOKE ALL ON TABLE {relation} FROM PUBLIC"))
    inherited = (
        bind.execute(
            text(
                "SELECT DISTINCT role.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "CROSS JOIN LATERAL aclexplode(c.relacl) acl JOIN pg_roles role ON role.oid=acl.grantee "
                "WHERE n.nspname=:schema AND c.relname='network_candidate_custody' "
                "AND acl.grantee<>c.relowner ORDER BY role.rolname"
            ),
            {"schema": schema},
        )
        .scalars()
        .all()
    )
    if len(inherited) > 128:
        raise RuntimeError("Candidate custody default privilege inventory exceeds its bound")
    for role in inherited:
        bind.execute(text(f"REVOKE ALL ON TABLE {relation} FROM {_identifier(role)}"))
    for role in readers:
        bind.execute(text(f"GRANT SELECT ON TABLE {relation} TO {_identifier(role)}"))


def downgrade():
    """Keep immutable custody evidence until an explicit retained-data migration."""
    raise RuntimeError("Candidate custody evidence requires an explicit retained-data migration")
