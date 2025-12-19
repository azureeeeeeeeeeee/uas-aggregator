import requests
import uuid
import random
import time
import os
import logging
from datetime import datetime, timezone

# Setup simple logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Publisher")

AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8000/publish")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
DUPLICATION_RATE = float(os.getenv("DUPLICATION_RATE", "0.2"))
DELAY = float(os.getenv("DELAY", "1.0"))

TOPICS = ["sensor-temp", "sensor-humidity", "system-log", "user-activity"]
SOURCES = ["raspberry-pi", "iot-hub", "mobile-app"]

def generate_event(topic=None, source=None):
    return {
        "topic": topic or random.choice(TOPICS),
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source or random.choice(SOURCES),
        "payload": {
            "value": random.randint(0, 100),
            "status": random.choice(["ok", "warn", "error"])
        }
    }

def run_loop():
    logger.info(f"Starting publisher service. Target: {AGGREGATOR_URL}")
    while True:
        try:
            # Generate unique events
            events = [generate_event() for _ in range(BATCH_SIZE)]
            
            # Select some to be duplicates (re-use existing IDs from THIS batch or potential historical - here simplistically internal dupes)
            # To test REAL persistent dedup, we should probably re-send some OLD events.
            # But "publisher sends duplicates" usually means redundant transmission.
            
            # Let's create a "duplication" by picking a few events from the generated list and adding them again.
            num_dupes = int(BATCH_SIZE * DUPLICATION_RATE)
            if num_dupes > 0:
                duplicates = random.sample(events, num_dupes)
                events.extend(duplicates)
                random.shuffle(events)
            
            response = requests.post(AGGREGATOR_URL, json=events)
            response.raise_for_status()
            logger.info(f"Sent {len(events)} events (approx {num_dupes} dupes). Response: {response.status_code}")
            
            time.sleep(DELAY)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error: {e}. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    run_loop()
