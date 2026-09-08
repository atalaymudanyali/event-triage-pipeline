import json
import logging

from confluent_kafka import Consumer, KafkaError, Producer

from triage_pipeline.agent import triage_ticket
from triage_pipeline.config import settings
from triage_pipeline.db import save_triage_result
from triage_pipeline.models import SupportTicketEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _send_to_dlt(producer: Producer, event: SupportTicketEvent, error: str):
    value = json.dumps(
        {"event": event.model_dump(), "error": error},
        ensure_ascii=False,
    )
    producer.produce(
        topic=settings.kafka_dlt_topic,
        key=event.event_id,
        value=value.encode("utf-8"),
    )
    producer.flush(timeout=5)
    logger.warning("[%s] Sent to dead-letter topic: %s", event.event_id[:8], error)


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "triage-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.kafka_topic])

    dlt_producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    retry_counts: dict[str, int] = {}

    logger.info("Consuming from topic '%s'", settings.kafka_topic)
    logger.info("Broker: %s | Model: %s", settings.kafka_bootstrap_servers, settings.gemini_model)
    logger.info("Max retries before DLT: %d", settings.max_retries)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            raw = json.loads(msg.value().decode("utf-8"))
            event = SupportTicketEvent.model_validate(raw)

            logger.info(
                "[%s] Processing: %s — %s",
                event.event_id[:8], event.customer_name, event.subject,
            )

            try:
                result = triage_ticket(event)
            except Exception as e:
                retries = retry_counts.get(event.event_id, 0) + 1
                retry_counts[event.event_id] = retries

                if retries >= settings.max_retries:
                    _send_to_dlt(dlt_producer, event, str(e))
                    retry_counts.pop(event.event_id, None)
                    consumer.commit(message=msg)
                else:
                    logger.warning(
                        "[%s] LLM error (attempt %d/%d): %s",
                        event.event_id[:8], retries, settings.max_retries, e,
                    )
                continue

            retry_counts.pop(event.event_id, None)

            logger.info(
                "[%s] Result: %s | %s | %s",
                event.event_id[:8], result.category.value, result.urgency.value,
                result.suggested_action.value,
            )

            saved = save_triage_result(event, result)
            if saved:
                logger.info("[%s] Saved to database", event.event_id[:8])
            else:
                logger.info("[%s] Already processed (duplicate)", event.event_id[:8])

            consumer.commit(message=msg)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
