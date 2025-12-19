# Quickstart Guide

This guide will help you run the Aggregator Service and test it using the interactive API documentation (Swagger UI).

## Prerequisites
- Docker & Docker Compose
- Web Browser

## 1. Run the Application
Start the entire system (Aggregator, Publisher, Database, Redis) using Docker Compose:

```bash
docker-compose up --build
```
*Wait a few seconds for the database to initialize.*

## 2. Access the GUI API Client
FastAPI provides an automatic interactive API documentation interface.
Open your browser and navigate to:
**[http://localhost:8000/docs](http://localhost:8000/docs)**

## 3. How to Test Endpoints

### Check Stats (Real-time)
1. In Swagger UI, expand **`GET /stats`**.
2. Click **Try it out** -> **Execute**.
3. You will see the live counts of `received`, `unique_processed`, and `duplicate_dropped`.
   *Note: Since the `publisher` service is running in the background, these numbers should increase automatically.*

### Publish Events Manually
1. Expand **`POST /publish`**.
2. Click **Try it out**.
3. Paste a sample event batch in the Request body:
   ```json
   [
     {
       "topic": "test-gui",
       "event_id": "manual-1",
       "timestamp": "2024-01-01T12:00:00Z",
       "source": "swagger",
       "payload": {"hello": "world"}
     },
     {
       "topic": "test-gui",
       "event_id": "manual-1",
       "timestamp": "2024-01-01T12:00:00Z",
       "source": "swagger",
       "payload": {"hello": "world"}
     }
   ]
   ```
4. Click **Execute**.
5. **Response**: You should see `processed_count: 1` and `duplicates_skipped: 1` (since the two events have the same ID).

### View Events
1. Expand **`GET /events`**.
2. Click **Try it out**.
3. (Optional) Enter `test-gui` in the `topic` field.
4. Click **Execute** to see the entry you just added.

## 4. Run Automated Tests
If you want to run the python test suite manually:

```bash
# Install dependencies first (if locally)
pip install -r requirements.txt
pip install httpx

# Run tests
python -m pytest tests/
```
