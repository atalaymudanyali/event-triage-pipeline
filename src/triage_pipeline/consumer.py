import json
import sys

from confluent_kafka import Consumer, KafkaError

from triage_pipeline.config import settings
from triage_pipeline.db import save_triage_result
from triage_pipeline.llm import triage_ticket
from triage_pipeline.models import SupportTicketEvent


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

    print(f"Consuming from topic '{settings.kafka_topic}'...")
    print(f"Broker: {settings.kafka_bootstrap_servers}")
    print(f"Model: {settings.gemini_model}")
    print()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            raw = json.loads(msg.value().decode("utf-8"))
            event = SupportTicketEvent.model_validate(raw)

            print(f"[{event.event_id[:8]}] Processing: {event.subject}")
            print(f"  Customer: {event.customer_name}")

            try:
                result = triage_ticket(event)
            except Exception as e:
                print(f"  LLM ERROR: {e}", file=sys.stderr)
                continue

            print(f"  Category: {result.category.value}")
            print(f"  Urgency:  {result.urgency.value}")
            print(f"  Action:   {result.suggested_action.value}")
            print(f"  Reason:   {result.reasoning}")

            saved = save_triage_result(event, result)
            if saved:
                print("  Saved to database.")
            else:
                print("  Already processed (duplicate).")

            consumer.commit(message=msg)
            print()
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
