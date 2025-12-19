import pytest
from datetime import datetime, timezone
import time
from src.models.dedup_model import DedupEvent
from src.models.stats_model import Stats

# 'client' and 'db_session' fixtures are automatically available from conftest.py

def make_event(event_id: str, topic="sensor", source="node-1", payload=None):
    return {
        "event_id": event_id,
        "topic": topic,
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload or {"value": 42},
    }

# basic dedup
def test_deduplication_logic(client):
    e = make_event("dup1")
    client.post("/publish", json=e)
    client.post("/publish", json=e)
    stats = client.get("/stats").json()
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 1
    assert stats["received"] == 2

# persistence check - since we use rollback in tests, we just check if DB has it
def test_dedup_persistence(client, db_session):
    e = make_event("persist-1")
    client.post("/publish", json=e)
    
    # Check if in DB
    assert db_session.query(DedupEvent).filter_by(event_id="persist-1").first()
    
    # Check stats
    stats = client.get("/stats").json()
    # Stat 'unique_processed' should be 1 if this test logic runs isolated
    # But note: stats row is shared? No, rolled back.
    # However, 'db_session' fixture rolls back transaction.
    # But Stats initialization in 'lifespan' (main.py) might be lost if we rolled back everything?
    # conftest creates tables only once.
    # wait. 'db_session' begins a transaction.
    # 'setup_database' creates tables.
    # 'lifespan' insert logic triggers on app startup.
    # But app startup happens when TestClient is created.
    # TestClient is created inside 'client' fixture which uses 'db_session' dependency?
    # No, 'client' fixture creates TestClient.
    
    # We need to ensure stats row exists.
    # In `lifespan`, we check if stats exists.
    # If `db_session` rolls back, it might remove stats row if it was inserted in that session.
    # But stats row should probably be inserted in setup.
    
    # Let's ensure stats row exists in the test.
    if not db_session.query(Stats).first():
        db_session.add(Stats(id=1))
        db_session.commit() # This commit might be part of the transaction we rollback?
        
    client.post("/publish", json=e)
    # The second publish (if duplicated logic runs again)
    stats = client.get("/stats").json()
    assert stats["duplicate_dropped"] == 1

# test validasi
@pytest.mark.parametrize("invalid_event", [
    {"event_id": "no_topic"},
    {"topic": "missing_event_id"},
])
def test_invalid_event_fields(client, invalid_event):
    r = client.post("/publish", json=invalid_event)
    # New architecture skips invalid events gracefully instead of failing entire batch
    assert r.status_code == 200
    data = r.json()
    assert data["processed_count"] == 0

# test validasi
def test_invalid_timestamp(client):
    bad = make_event("badtime")
    bad["timestamp"] = "not_a_timestamp"
    r = client.post("/publish", json=bad)
    # Graceful failure
    assert r.status_code == 200
    data = r.json()
    assert data["processed_count"] == 0

# /events konsisten
def test_get_events_consistency(client):
    e1 = make_event("a1", topic="alpha")
    e2 = make_event("a2", topic="alpha")
    client.post("/publish", json=[e1, e2])
    resp = client.get("/events", params={"topic": "alpha"})
    data = resp.json()
    assert resp.status_code == 200
    assert len(data) == 2
    assert all("event_id" in e and "payload" in e for e in data)

# test /stats
def test_stats_topics(client):
    client.post("/publish", json=make_event("t1", topic="temp"))
    client.post("/publish", json=make_event("t2", topic="humidity"))
    stats = client.get("/stats").json()
    assert "temp" in stats["topics"]
    assert "humidity" in stats["topics"]

# Batch insert
def test_batch_insert(client):
    events = [make_event(f"b{i}") for i in range(10)]
    r = client.post("/publish", json=events)
    assert r.status_code == 200
    stats = client.get("/stats").json()
    assert stats["unique_processed"] == 10
    assert stats["received"] == 10

# Batch insert
def test_batch_with_duplicates(client):
    uniq = [make_event(f"x{i}") for i in range(5)]
    client.post("/publish", json=uniq)
    mixed = uniq + [make_event(f"x{i}") for i in range(5)]
    client.post("/publish", json=mixed)
    stats = client.get("/stats").json()
    assert stats["unique_processed"] == 5
    assert stats["duplicate_dropped"] >= 5

# Topik tidak ditemukan
def test_get_events_not_found(client):
    r = client.get("/events", params={"topic": "unknown"})
    assert r.status_code == 404

# Stress test
def test_small_stress(client):
    events = [make_event(f"s{i}") for i in range(300)]
    start = time.time()
    client.post("/publish", json=events)
    elapsed = time.time() - start
    stats = client.get("/stats").json()
    assert stats["unique_processed"] == 300
    assert elapsed < 5.0
