import pytest
import concurrent.futures
from src.utils import SessionLocal, Base, engine
from src.models.dedup_model import DedupEvent
from src.models.stats_model import Stats
from fastapi.testclient import TestClient
from src.main import app
import uuid
from datetime import datetime
import time

# Create a clean DB for concurrency test
# We might need to use the actual engine to ensure locking works, 
# SQLite might be tricky with concurrency but we will try.
# Ideally this test runs against Postgres in CI/Docker, but here we use the configured engine.

def make_event(event_id):
    return {
        "event_id": event_id,
        "topic": "concurrency-test",
        "source": "tester",
        "timestamp": datetime.now().isoformat(),
        "payload": {"val": 1}
    }

def send_batch(client, events):
    response = client.post("/publish", json=events)
    return response.json()

def test_concurrency_race_condition():
    # Setup
    Base.metadata.create_all(bind=engine)
    
    # We use TestClient, which is synchronous. 
    # To test concurrency, we need to simulate parallel requests.
    # Since TestClient calls app directly, it might be running in same thread?
    # FastAPI TestClient is usually single threaded.
    # To properly test DB concurrency, we might need to spawn threads that maintain their own clients 
    # or just rely on the fact that `EventProcessor` locks the DB row/table via transactions.
    
    # Actually, with TestClient, we are testing the endpoint logic.
    # If we want to test DB race conditions, we should probably call `process_batch` directly from threads 
    # OR use multiple instances of TestClient if possible?
    
    # Better: Use `concurrent.futures` to call a function that creates a NEW session and calls `EventProcessor`.
    # This simulates multiple workers hitting the DB.
    
    from src.services.processor import EventProcessor
    
    # Shared event ID
    target_event_id = str(uuid.uuid4())
    event = make_event(target_event_id)
    
    # Function to be run by threads
    def worker_process():
        # Each worker must have its own DB session
        session = SessionLocal()
        try:
            processor = EventProcessor(session)
            # Process a batch containing the SAME event
            return processor.process_batch([event])
        finally:
            session.close()

    # Run 5 workers in parallel
    workers = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker_process) for _ in range(workers)]
        results = [f.result() for f in futures]
        
    # Validation
    total_unique = sum(r["processed_count"] for r in results)
    total_dupes = sum(r["duplicates_skipped"] for r in results)
    
    print(f"Results: {[r['processed_count'] for r in results]}")
    
    # Only ONE should succeed in inserting unique
    assert total_unique == 1
    # The rest (workers-1) should fail/skip
    assert total_dupes == workers - 1
    
    # Verify DB
    session = SessionLocal()
    count = session.query(DedupEvent).filter_by(event_id=target_event_id).count()
    session.close()
    assert count == 1

