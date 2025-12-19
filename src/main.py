from fastapi import FastAPI, HTTPException, Depends, Request
from typing import List, Dict, Union
from src.utils import Base, engine, setup_logger, get_db
from src.models.dedup_model import DedupEvent
from src.models.stats_model import Stats
from src.services.processor import EventProcessor
from datetime import datetime, timedelta, timezone
from sqlalchemy import distinct, text
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager

# Create tables
Base.metadata.create_all(bind=engine)

logger = setup_logger()

# Lifespan context to initialize stats
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Ensure stats row exists
    with Session(engine) as db:
        stats = db.query(Stats).first()
        if not stats:
            # Atomic initial insert if needed
            try:
                db.add(Stats(id=1, received=0, unique_processed=0, duplicate_dropped=0))
                db.commit()
                logger.info("Initialized Stats table.")
            except Exception:
                # Might happen if another worker initializes it concurrently
                db.rollback()
    yield

app = FastAPI(lifespan=lifespan)

START_TIME = datetime.now(timezone.utc)

@app.get("/")
def main():
    return {
        "message": "Aggregator Service is Running",
        "docs": "/docs"
    }

@app.post("/publish")
async def publish_event(
    request: Request,
    db: Session = Depends(get_db)
):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if isinstance(data, dict):
        events_data = [data]
    elif isinstance(data, list):
        events_data = data
    else:
        raise HTTPException(status_code=400, detail="Request body must be a JSON object or array")

    processor = EventProcessor(db)
    result = processor.process_batch(events_data)
    
    return result


@app.get("/events")
def get_events(topic: str = None, limit: int = 100, db: Session = Depends(get_db)):
    query = db.query(DedupEvent)
    if topic:
        query = query.filter_by(topic=topic)
    
    # Order by timestamp desc
    events = query.order_by(DedupEvent.timestamp.desc()).limit(limit).all()

    if not events and topic:
        # User requirement says: "Endpoint GET /events?topic=...: daftar event unik yang telah diproses."
        # If no events for topic, returning empty list is often better than 404, but conforming to existing test expectation:
        # test_get_events_not_found expects 404
        raise HTTPException(status_code=404, detail="No events found")
        
    return [
        {
            "event_id": e.event_id,
            "topic": e.topic,
            "source": e.source,
            "timestamp": e.timestamp.isoformat() if e.timestamp else None,
            "payload": e.payload
        }
        for e in events
    ]


@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    # Retrieve persistent stats
    stats = db.query(Stats).filter(Stats.id == 1).first()
    
    # Calculate live topics count
    # Note: large scale this is slow, but for requirements it works.
    topics = [row[0] for row in db.query(distinct(DedupEvent.topic)).all()]
    
    uptime = datetime.now(timezone.utc) - START_TIME
    
    if not stats:
        return {"error": "Stats not initialized"}

    return {
        "received": stats.received,
        "unique_processed": stats.unique_processed,
        "duplicate_dropped": stats.duplicate_dropped,
        "topics": topics,
        "uptime": str(timedelta(seconds=int(uptime.total_seconds())))
    }