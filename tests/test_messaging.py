import boto3
import pytest
from moto import mock_aws
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import main, messaging


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    main.Base.metadata.create_all(engine)
    with sessionmaker(bind=engine, expire_on_commit=False)() as session:
        yield session
    engine.dispose()


@pytest.fixture
def sqs(monkeypatch):
    with mock_aws():
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setattr(messaging, "_client", None)
        client = boto3.client("sqs", region_name="us-east-1")
        urls = {
            name: client.create_queue(
                QueueName=f"{name}.fifo",
                Attributes={"FifoQueue": "true"},
            )["QueueUrl"]
            for name in ("billing-commands", "inventory-commands", "os-events")
        }
        monkeypatch.setattr(main, "BILLING_COMMANDS_QUEUE_URL", urls["billing-commands"])
        monkeypatch.setattr(main, "INVENTORY_COMMANDS_QUEUE_URL", urls["inventory-commands"])
        monkeypatch.setattr(main, "OS_EVENTS_QUEUE_URL", urls["os-events"])
        yield urls
        monkeypatch.setattr(messaging, "_client", None)


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
        (m.event_type, m.status)
        for m in db.scalars(
            select(main.OutboxMessage).where(main.OutboxMessage.aggregate_id == order.id).order_by(main.OutboxMessage.created_at)
        )
    ]


def test_publish_and_consume_roundtrip(sqs):
    url = sqs["billing-commands"]
    messaging.publish(url, "CreateQuote", {"order_id": "o1"}, group_id="o1", dedup_id="d1")
    received = []
    count = messaging.process_batch(url, lambda t, p: received.append((t, p)), wait_seconds=0)
    assert count == 1
    assert received == [("CreateQuote", {"order_id": "o1"})]
    assert messaging.process_batch(url, lambda t, p: None, wait_seconds=0) == 0


def test_failed_handler_keeps_message(sqs):
    url = sqs["billing-commands"]
    messaging.publish(url, "CreateQuote", {"order_id": "o1"}, group_id="o1")

    def boom(*_):
        raise RuntimeError("fail")

    assert messaging.process_batch(url, boom, wait_seconds=0) == 0
    attrs = boto3.client("sqs", region_name="us-east-1").get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessages"]
    )["Attributes"]
    assert int(attrs["ApproximateNumberOfMessagesNotVisible"]) + int(attrs["ApproximateNumberOfMessages"]) == 1


def test_relay_publishes_pending_commands(db, sqs):
    order = make_order(db)
    main.record_outbox(db, order.id, "CreateQuote", {"order_id": order.id})
    db.commit()
    assert main.relay_outbox(db) == 1
    assert main.relay_outbox(db) == 0
    assert outbox_types(db, order) == [("CreateQuote", "PROCESSED")]
    received = []
    messaging.process_batch(sqs["billing-commands"], lambda t, p: received.append(t), wait_seconds=0)
    assert received == ["CreateQuote"]


def test_relay_keeps_message_pending_on_failure(db, monkeypatch):
    order = make_order(db)
    main.record_outbox(db, order.id, "CreateQuote", {"order_id": order.id})
    db.commit()

    def broken(*args, **kwargs):
        raise RuntimeError("sqs down")

    monkeypatch.setattr(main.messaging, "publish", broken)
    assert main.relay_outbox(db) == 0
    message = db.scalars(select(main.OutboxMessage)).one()
    assert message.status == "PENDING"
    assert "sqs down" in message.error_message


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
    main.handle_saga_event(db, "PaymentConfirmed", {"order_id": order.id, "payment_id": "p1"})
    assert order.saga_state == "QUOTE_PENDING"
    main.handle_saga_event(db, "QuoteCreated", {"order_id": order.id, "quote_id": "q1"})
    main.handle_saga_event(db, "QuoteCreated", {"order_id": order.id, "quote_id": "q2"})
    assert order.quote_id == "q1"
    main.handle_saga_event(db, "Unknown", {"order_id": "missing"})


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

