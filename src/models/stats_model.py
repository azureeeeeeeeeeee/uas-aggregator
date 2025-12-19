from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime
from src.utils import Base

class Stats(Base):
    __tablename__ = "stats"

    id = Column(Integer, primary_key=True, index=True)
    received = Column(Integer, default=0)
    unique_processed = Column(Integer, default=0)
    duplicate_dropped = Column(Integer, default=0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
