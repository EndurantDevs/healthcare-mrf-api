# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-only address observations published with facility anchors."""

import os

from sqlalchemy import Column, PrimaryKeyConstraint, String
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base

__all__ = ("FacilityAddressContribution",)


class FacilityAddressContribution(Base):
    __tablename__ = "facility_address_contribution"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("kind", "address_key"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )

    kind = Column(String(16), nullable=False)
    address_key = Column(UUID(as_uuid=True), nullable=False)
    payload = Column(JSONB, nullable=False)
