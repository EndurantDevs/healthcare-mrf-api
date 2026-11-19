# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import TEXT, Column, DateTime, PrimaryKeyConstraint, String

from db.connection import Base
from db.json_mixin import JSONOutputMixin


class CodeRelationship(Base, JSONOutputMixin):
    __tablename__ = "code_relationship"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("from_system", "from_code", "relationship", "to_system", "to_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["from_system", "from_code", "relationship", "to_system", "to_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("from_system", "from_code"), "name": "code_relationship_from_idx"},
        {"index_elements": ("to_system", "to_code"), "name": "code_relationship_to_idx"},
        {"index_elements": ("relationship",), "name": "code_relationship_type_idx"},
        {"index_elements": ("source",), "name": "code_relationship_source_idx"},
        {
            "index_elements": ("relationship",),
            "name": "code_relationship_public_type_idx",
            "where": "upper(from_system) <> 'SNOMEDCT_US' AND upper(to_system) <> 'SNOMEDCT_US'",
        },
    ]

    from_system = Column(String(32), nullable=False)
    from_code = Column(String(128), nullable=False)
    relationship = Column(String(64), nullable=False)
    to_system = Column(String(32), nullable=False)
    to_code = Column(String(128), nullable=False)
    source = Column(String(128), nullable=False)
    source_attribution = Column(TEXT)
    updated_at = Column(DateTime)
