import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main, messaging
from tests.fake_rabbit import FakeRabbit


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    main.Base.metadata.create_all(engine)
    with sessionmaker(bind=engine, expire_on_commit=False)() as session:
        yield session
    engine.dispose()


@pytest.fixture
def rabbit(monkeypatch):
    broker = FakeRabbit()
    monkeypatch.setattr(messaging, "_connection", broker.connection)
    monkeypatch.setattr(messaging, "MESSAGING_ENABLED", True)
    return broker


def make_order(db, saga_state="QUOTE_PENDING", status="QUOTE_PENDING"):
    order = main.Order(
        customer_id="c1",
        vehicle_id="v1",
        items=[
            {"kind": "PART", "sku": "oil", "name": "Oil", "quantity": 2, "unit_price": "10.00"},
            {"kind": "SERVICE", "sku": "inspection", "name": "Inspection", "quantity": 1, "unit_price": "5.00"},
        ],
        saga_state=saga_state,
        status=status,
    )
    db.add(order)
    db.commit()
    return order


def outbox_types(db, order):
    return [
        (message.event_type, message.status)
        for message in db.scalars(
            select(main.OutboxMessage)
            .where(main.OutboxMessage.aggregate_id == order.id)
            .order_by(main.OutboxMessage.created_at)
        )
    ]


def test_publish_and_consume_roundtrip(rabbit):
    queue = messaging.BILLING_COMMANDS_QUEUE
    messaging.publish(queue, "CreateQuote", {"order_id": "o1"}, correlation_id="corr-1")
    envelope = json.loads(rabbit.queues[queue][0])
    assert envelope == {
        "type": "CreateQuote",
        "payload": {"order_id": "o1"},
        "correlation_id": "corr-1",
    }
    received = []
    assert messaging.process_batch(queue, lambda kind, payload: received.append((kind, payload))) == 1
    assert received == [("CreateQuote", {"order_id": "o1"})]
    assert messaging.process_batch(queue, lambda *_: None) == 0


def test_failed_handler_moves_message_to_dead_letter_queue(rabbit):
    queue = messaging.BILLING_COMMANDS_QUEUE
    messaging.publish(queue, "CreateQuote", {"order_id": "o1"})

    def boom(*_):
        raise RuntimeError("handler failed")

    assert messaging.process_batch(queue, boom) == 0
    assert len(rabbit.queues[queue]) == 0
    assert len(rabbit.queues[queue + ".dlq"]) == 1


def test_relay_publishes_pending_commands(db, rabbit):
    order = make_order(db)
    main.record_outbox(db, order.id, "CreateQuote", {"order_id": order.id})
    db.commit()
    assert main.relay_outbox(db) == 1
    assert main.relay_outbox(db) == 0
    assert outbox_types(db, order) == [("CreateQuote", "PROCESSED")]
    received = []
    messaging.process_batch(messaging.BILLING_COMMANDS_QUEUE, lambda kind, _: received.append(kind))
    assert received == ["CreateQuote"]


def test_relay_keeps_message_pending_on_failure(db, rabbit, monkeypatch):
    order = make_order(db)
    main.record_outbox(db, order.id, "CreateQuote", {"order_id": order.id})
    db.commit()

    def broken(*args, **kwargs):
        raise RuntimeError("RabbitMQ unavailable")

    monkeypatch.setattr(main.messaging, "publish", broken)
    assert main.relay_outbox(db) == 0
    message = db.scalars(select(main.OutboxMessage)).one()
    assert message.status == "PENDING"
    assert "RabbitMQ unavailable" in message.error_message


def test_happy_path_saga(db):
    order = make_order(db)
    main.handle_saga_event(db, "QuoteCreated", {"order_id": order.id, "quote_id": "q1"})
    assert order.saga_state == "QUOTE_CREATED" and order.quote_id == "q1"
    main.handle_saga_event(db, "QuoteApproved", {"order_id": order.id})
    assert order.status == "AWAITING_PAYMENT"
    main.handle_saga_event(db, "PaymentConfirmed", {"order_id": order.id, "payment_id": "p1"})
    assert order.saga_state == "STOCK_RESERVING"
    reserve = db.scalars(select(main.OutboxMessage).where(main.OutboxMessage.event_type == "ReserveStock")).one()
    assert reserve.payload["items"] == [{"sku": "oil", "quantity": 2}]
    main.handle_saga_event(db, "StockReserved", {"order_id": order.id})
    assert order.status == "READY_FOR_SERVICE" and order.saga_state == "COMPLETED"


def test_duplicate_and_out_of_order_events_are_ignored(db):
    order = make_order(db)
    main.handle_saga_event(db, "StockReserved", {"order_id": order.id})
    assert order.saga_state == "QUOTE_PENDING"
    main.handle_saga_event(db, "QuoteCreated", {"order_id": order.id, "quote_id": "q1"})
    main.handle_saga_event(db, "QuoteCreated", {"order_id": order.id, "quote_id": "q2"})
    assert order.quote_id == "q1"
    main.handle_saga_event(db, "Unknown", {"order_id": "missing"})


def test_payment_events_tolerate_delayed_quote_events(db):
    order = make_order(db)
    main.handle_saga_event(
        db,
        "PaymentConfirmed",
        {"order_id": order.id, "quote_id": "q-late", "payment_id": "p-late"},
    )
    main.handle_saga_event(db, "QuoteApproved", {"order_id": order.id, "quote_id": "q-late"})
    main.handle_saga_event(
        db,
        "QuoteCreated",
        {"order_id": order.id, "quote_id": "q-late"},
    )
    assert order.quote_id == "q-late"
    assert order.payment_id == "p-late"
    assert order.saga_state == "STOCK_RESERVING"
    assert len(db.scalars(select(main.OutboxMessage).where(main.OutboxMessage.event_type == "ReserveStock")).all()) == 1


def test_quote_rejected_cancels_order(db):
    order = make_order(db, saga_state="QUOTE_CREATED")
    main.handle_saga_event(db, "QuoteRejected", {"order_id": order.id})
    assert order.status == "CANCELLED" and order.saga_state == "CANCELLED"


def test_payment_failed_cancels_order(db):
    order = make_order(db, saga_state="AWAITING_PAYMENT", status="AWAITING_PAYMENT")
    main.handle_saga_event(db, "PaymentFailed", {"order_id": order.id})
    assert order.status == "CANCELLED"


def test_stock_failure_triggers_refund_compensation(db):
    order = make_order(db, saga_state="STOCK_RESERVING", status="AWAITING_PAYMENT")
    order.payment_id = "p1"
    db.commit()
    main.handle_saga_event(db, "StockReservationFailed", {"order_id": order.id, "reason": "insufficient stock"})
    assert order.saga_state == "COMPENSATION_PENDING"
    assert ("RefundPayment", "PENDING") in outbox_types(db, order)
    main.handle_saga_event(db, "PaymentRefundRequested", {"order_id": order.id})
    assert order.saga_state == "COMPENSATED" and order.status == "CANCELLED"
    assert ("COMPENSATION_COMPLETED", "PROCESSED") in outbox_types(db, order)


def test_retry_compensation_releases_stock_before_requesting_refund(db, monkeypatch):
    monkeypatch.setattr(messaging, "MESSAGING_ENABLED", True)
    order = make_order(db, saga_state="COMPENSATION_PENDING", status="COMPENSATION_PENDING")
    order.payment_id = "p1"
    db.commit()

    main.retry_compensation(
        order.id,
        actor=main.Actor(is_admin=True, customer_id=None),
        db=db,
    )

    assert outbox_types(db, order)[-1] == ("ReleaseStock", "PENDING")
    assert ("RefundPayment", "PENDING") not in outbox_types(db, order)

    main.handle_saga_event(db, "StockReleased", {"order_id": order.id})

    assert outbox_types(db, order)[-1] == ("RefundPayment", "PENDING")
