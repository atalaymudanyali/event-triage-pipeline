from triage_pipeline.models import SupportTicketEvent
from triage_pipeline.producer import CUSTOMERS, TICKETS, generate_ticket


class TestGenerateTicket:
    def test_returns_valid_event(self):
        ticket = generate_ticket()
        assert isinstance(ticket, SupportTicketEvent)
        assert ticket.customer_name
        assert ticket.customer_email
        assert ticket.subject
        assert ticket.message
        assert ticket.event_id

    def test_uses_known_customers(self):
        names = {name for name, _ in CUSTOMERS}
        for _ in range(50):
            ticket = generate_ticket()
            assert ticket.customer_name in names

    def test_uses_known_subjects(self):
        subjects = {subject for subject, _ in TICKETS}
        for _ in range(50):
            ticket = generate_ticket()
            assert ticket.subject in subjects

    def test_templates_are_populated(self):
        for _ in range(50):
            ticket = generate_ticket()
            assert "{order_id}" not in ticket.message
            assert "{amount}" not in ticket.message
            assert "{email}" not in ticket.message

    def test_unique_event_ids(self):
        tickets = [generate_ticket() for _ in range(50)]
        ids = {t.event_id for t in tickets}
        assert len(ids) == 50
