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
