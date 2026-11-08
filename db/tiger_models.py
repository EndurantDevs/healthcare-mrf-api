# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import JSON, BigInteger, Column, Double, Integer, PrimaryKeyConstraint, Sequence, String
from sqlalchemy.types import UserDefinedType

from db.connection import Base, db
from db.json_mixin import JSONOutputMixin


class TigerMultiPolygon(UserDefinedType):
    """The installed TIGER geometry typmod, preserved as native PostGIS data."""

    cache_ok = True

    def get_col_spec(self, **_kwargs):
        """Compile the native, fixed PostGIS type and spatial reference."""
        return "geometry(MultiPolygon,4269)"


class ZipState(Base, JSONOutputMixin):
    __tablename__ = "zip_state"
    __schema__ = "tiger"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("zip", "stusps"),
        {"schema": "tiger", "extend_existing": True},
    )

    zip = Column(String(5), nullable=False)
    stusps = Column(String(2), nullable=False)
    statefp = Column(String(2))


class State(Base, JSONOutputMixin):
    __tablename__ = "state"
    __schema__ = "tiger"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("statefp"),
        {"schema": "tiger", "extend_existing": True},
    )

    statefp = Column(Integer)
    stusps = Column(String(2))
    name = Column(String)
    lsad = Column(String(2))
    mtfcc = Column(String(5))
    funcstat = Column(String(1))
    aland = Column(BigInteger)
    awater = Column(BigInteger)
    intptlat = Column(String(11))
    intptlon = Column(String(12))
    geom = Column(JSON)


class Zip_zcta5(Base, JSONOutputMixin):
    __tablename__ = "zcta5"
    __schema__ = "tiger"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("zcta5ce", "statefp"),
        {"schema": "tiger", "extend_existing": True},
    )

    __my_additional_indexes__ = [{"index_elements": ("the_geom",), "using": "gist"}]

    gid_sequence = Sequence("zcta5_gid_seq", schema="tiger", data_type=Integer)
    gid = Column(Integer, gid_sequence, server_default=gid_sequence.next_value(), nullable=False)
    statefp = Column(String(2), nullable=False)
    zcta5ce = Column(String(5), nullable=False)
    classfp = Column(String(2))
    mtfcc = Column(String(5))
    funcstat = Column(String(1))
    aland = Column(Double)
    awater = Column(Double)
    intptlat = Column(String(11))
    intptlon = Column(String(12))
    partflg = Column(String(1))
    the_geom = Column(TigerMultiPolygon())
