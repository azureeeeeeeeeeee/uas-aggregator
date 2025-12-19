import logging
import os
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from pathlib import Path

def setup_logger(type: str | None = 'Aggregator'):
    logger = logging.getLogger(f"{type}")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"%(asctime)s [Client : {type}] [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger

# Default to SQLite if not set (for local testing without docker)
DB_PATH = Path(__file__).resolve().parent.parent / "src" / "db.sqlite"
DEFAULT_DB_URL = f"sqlite:///{DB_PATH}"

DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

# Retry logic for DB connection (helpful for docker-compose startup)
engine = None
connect_args = {}

if "sqlite" in DATABASE_URL:
    connect_args["check_same_thread"] = False
    execution_options = {}
else:
    # For Postgres, explicit isolation level
    execution_options = {"isolation_level": "READ COMMITTED"}

for i in range(5):
    try:
        engine = create_engine(
            DATABASE_URL, 
            echo=False, 
            connect_args=connect_args,
            execution_options=execution_options
        )
        # Test connection
        with engine.connect() as conn:
            pass
        break
    except Exception as e:
        print(f"Database connection failed, retrying {i+1}/5... {e}")
        time.sleep(2)

if not engine:
    # Final attempt or crash
    engine = create_engine(
        DATABASE_URL, 
        echo=False, 
        connect_args=connect_args,
        execution_options=execution_options
    )

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()
db = SessionLocal()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()