import json
import random
import time

from confluent_kafka import Producer

from triage_pipeline.config import settings
from triage_pipeline.models import SupportTicketEvent

CUSTOMERS = [
    ("Ayşe Yılmaz", "ayse.yilmaz@email.com"),
    ("Mehmet Kaya", "mehmet.kaya@email.com"),
    ("Zeynep Demir", "zeynep.demir@email.com"),
    ("Ali Çelik", "ali.celik@email.com"),
    ("Fatma Şahin", "fatma.sahin@email.com"),
    ("Emre Özkan", "emre.ozkan@email.com"),
    ("Elif Arslan", "elif.arslan@email.com"),
    ("Burak Koç", "burak.koc@email.com"),
]

TICKETS = [
    (
        "Order not delivered",
        "I placed an order 10 days ago (order #{order_id}) and it still hasn't arrived. "
        "The tracking page says 'in transit' but hasn't updated in 5 days. "
        "I need this resolved urgently.",
    ),
    (
        "Wrong item received",
        "I ordered a blue jacket (size M) but received a red scarf instead. "
        "Order #{order_id}. I need the correct item sent and a return label for this one.",
    ),
    (
        "Payment charged twice",
        "I was charged twice for order #{order_id}. My bank statement shows two identical "
        "charges of {amount} TL. Please refund the duplicate charge immediately.",
    ),
    (
        "Damaged product",
        "The laptop I received (order #{order_id}) has a cracked screen. "
        "It was clearly damaged during shipping. The box was crushed. "
        "I want a replacement or full refund.",
    ),
    (
        "Can't log into my account",
        "I've been trying to log in for the past hour but keep getting 'invalid credentials'. "
        "I'm sure my password is correct. I also tried resetting it but never received "
        "the reset email. My email is {email}.",
    ),
    (
        "Refund not processed",
        "I returned my order #{order_id} two weeks ago and the tracking confirms it was "
        "delivered back to your warehouse. But I still haven't received my refund of "
        "{amount} TL. When will this be processed?",
    ),
    (
        "How do I track my order?",
        "Hi, I just placed order #{order_id} and I'm wondering how I can track it. "
        "Is there a tracking page? I couldn't find it in my account.",
    ),
    (
        "Request for invoice",
        "Could you please send me the invoice for order #{order_id}? "
        "I need it for my company's expense report. The order total was {amount} TL.",
    ),
    (
        "Product arrived late for event",
        "I ordered a gift (order #{order_id}) specifically for a birthday party yesterday "
        "and it arrived today — a day late. This ruined my plans. "
        "I want compensation for this failure.",
    ),
    (
        "App keeps crashing",
        "Your mobile app crashes every time I try to open my order history. "
        "I'm using an iPhone 14 with the latest app version. "
        "I've tried reinstalling but the problem persists.",
    ),
]


def generate_ticket() -> SupportTicketEvent:
    name, email = random.choice(CUSTOMERS)
    subject, message_template = random.choice(TICKETS)
    message = message_template.format(
        order_id=random.randint(100000, 999999),
        amount=random.choice([49, 99, 149, 249, 399, 599, 899, 1299]),
        email=email,
    )
    return SupportTicketEvent(
        customer_name=name,
        customer_email=email,
        subject=subject,
        message=message,
    )


def delivery_report(err, msg):
    if err:
        print(f"  FAILED: {err}")
    else:
        print(f"  Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    print(f"Producing tickets to topic '{settings.kafka_topic}'...")
    print(f"Broker: {settings.kafka_bootstrap_servers}")
    print()

    try:
        while True:
            ticket = generate_ticket()
            value = json.dumps(ticket.model_dump(), ensure_ascii=False)

            print(f"[{ticket.event_id[:8]}] {ticket.customer_name}: {ticket.subject}")
            producer.produce(
                topic=settings.kafka_topic,
                key=ticket.event_id,
                value=value.encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            delay = random.uniform(2, 6)
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush(timeout=5)


if __name__ == "__main__":
    main()
