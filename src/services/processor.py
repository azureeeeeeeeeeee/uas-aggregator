from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.models.dedup_model import DedupEvent
from src.models.stats_model import Stats
from src.models.schemas.dedup_schema import EventSchema
from datetime import datetime, timezone
import logging

logger = logging.getLogger("EventProcessor")

class EventProcessor:
    def __init__(self, db: Session):
        self.db = db

    def process_batch(self, events_data: list[dict]):
        processed_ids = []
        duplicates = 0
        unique_count = 0
        
        # We need to process sequentially to ensure atomic handling of each item
        # or we could try to bulk insert? 
        # Requirement: "Transactions: Apply transaction when insert/processing"
        
        for raw_event in events_data:
            try:
                event_schema = EventSchema(**raw_event)
            except Exception as e:
                logger.error(f"Invalid event data: {e}")
                continue 

            # Create model instance
            new_event = DedupEvent(
                event_id=event_schema.event_id,
                topic=event_schema.topic,
                source=event_schema.source,
                timestamp=event_schema.timestamp,
                payload=event_schema.payload
            )
            
            try:
                # Use subtransaction (SAVEPOINT) for each insert to handle duplicates gracefully
                with self.db.begin_nested(): 
                    self.db.add(new_event)
                    self.db.flush() # Check constraints immediately
                
                unique_count += 1
                processed_ids.append(event_schema.event_id)
                
            except IntegrityError:
                # Duplicate detected (topic + event_id collision)
                duplicates += 1
                # Subtransaction rolls back automatically
            except Exception as e:
                logger.error(f"Error processing event {event_schema.event_id}: {e}")

        # Atomic Stats Update
        if events_data:
            self._update_stats(len(events_data), unique_count, duplicates)

        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error(f"Commit failed: {e}")
            raise e

        logger.info(f"Batch Result: {unique_count} unique, {duplicates} duplicates.")
        
        return {
            "status": "ok",
            "processed_count": unique_count,
            "duplicates_skipped": duplicates,
            "total_received": len(events_data)
        }

    def _update_stats(self, received, unique, duplicates):
        # Atomic update logic
        try:
            self.db.query(Stats).filter(Stats.id == 1).update({
                Stats.received: Stats.received + received,
                Stats.unique_processed: Stats.unique_processed + unique,
                Stats.duplicate_dropped: Stats.duplicate_dropped + duplicates,
                Stats.last_updated: datetime.now(timezone.utc)
            })
            # Note: Commit happens in the parent method
        except Exception as e:
            logger.error(f"Stats update failed: {e}")
