import os
from sqlalchemy import DateTime, Numeric, DATE, Column,\
    String, Integer, Float, BigInteger, Boolean, ARRAY, JSON, TIMESTAMP, TEXT, SMALLINT

from db.connection import db
from db.json_mixin import JSONOutputMixin



class ZipState(db.Model, JSONOutputMixin):
    __tablename__ = 'zip_state'
    __schema__ = 'tiger'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': 'tiger', 'extend_existing': True},
    )

    zip = Column(String)
    stusps = Column(String(2))
    statefp = Column(Integer)


class State(db.Model, JSONOutputMixin):
    __tablename__ = 'state'
    __schema__ = 'tiger'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': 'tiger', 'extend_existing': True},
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


class Zip_zcta5(db.Model, JSONOutputMixin):
    __tablename__ = 'zcta5'
    __schema__ = 'tiger'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': 'tiger', 'extend_existing': True},
    )

    gid = Column(String(5))
    statefp = Column(String(2))
    zcta5ce = Column(String(5))
    classfp = Column(String(2))
    mtfcc = Column(String(5))
    funcstat = Column(String(1))
    aland = Column(BigInteger)
    awater = Column(BigInteger)
    intptlat = Column(String(11))
    intptlon = Column(String(12))
    partflg = Column(String(1))
    the_geom = Column(String)


