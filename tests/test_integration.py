import pytest
import threading
import uuid
import time
from src.models.stats_model import Stats
from src.main import app

# Tests use 'client' fixture from conftest.py

def test_atomic_deduplication(client):
    event_id = str(uuid.uuid4())
    payload = {
        "topic": "test-topic",
        "event_id": event_id,
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "test",
        "payload": {}
    }
    
    # Send First Time
    r1 = client.post("/publish", json=[payload])
    assert r1.status_code == 200
    data1 = r1.json()
    assert data1["processed_count"] == 1
    assert data1["duplicates_skipped"] == 0

    # Send Second Time (Duplicate)
    r2 = client.post("/publish", json=[payload])
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["processed_count"] == 0
    assert data2["duplicates_skipped"] == 1

def test_concurrency_dedup(client):
    """
    Simulate multiple threads trying to insert the SAME event at the same time.
    Only one should succeed.
    """
    event_id = str(uuid.uuid4())
    payload = {
        "topic": "race-topic",
        "event_id": event_id,
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "test-race",
        "payload": {}
    }
    
    results = []
    
    def send_request():
        r = client.post("/publish", json=[payload])
        results.append(r.json())

    threads = []
    for _ in range(10):
        t = threading.Thread(target=send_request)
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    # Check results
    total_processed = sum(r["processed_count"] for r in results)
    total_dupes = sum(r["duplicates_skipped"] for r in results)
    
    assert total_processed == 1
    assert total_dupes == 9

def test_stats_consistency(client):
    # Get current stats
    r = client.get("/stats")
    initial_stats = r.json()
    initial_received = initial_stats["received"]
    
    # Send a batch of 10 unique events
    batch = []
    for _ in range(10):
        batch.append({
            "topic": "stats-test",
            "event_id": str(uuid.uuid4()),
            "timestamp": "2024-01-01T00:00:00Z",
            "source": "stats",
            "payload": {}
        })
        
    client.post("/publish", json=batch)
    
    # Check stats again
    r = client.get("/stats")
    new_stats = r.json()
    
    assert new_stats["received"] == initial_received + 10
    assert new_stats["unique_processed"] >= 10 # Could include previous tests
