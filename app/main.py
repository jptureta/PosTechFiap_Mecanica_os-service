import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import uuid4

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, ForeignKey, Integer, JSON, Numeric, String, create_engine, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

from app import diagnostics, messaging, observability
from app.auth import Actor, get_actor

logger = logging.getLogger(__name__)
OS_EVENTS_QUEUE_URL = os.getenv("OS_EVENTS_QUEUE_URL", "")
BILLING_COMMANDS_QUEUE_URL = os.getenv("BILLING_COMMANDS_QUEUE_URL", "")
INVENTORY_COMMANDS_QUEUE_URL = os.getenv("INVENTORY_COMMANDS_QUEUE_URL", "")
OUTBOX_RELAY_INTERVAL_SECONDS = float(os.getenv("OUTBOX_RELAY_INTERVAL_SECONDS", "2"))
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./os-service.db")
BILLING_URL = os.getenv("BILLING_URL", "http://billing-service:8000").rstrip("/")
INVENTORY_URL = os.getenv("INVENTORY_URL", "http://inventory-service:8000").rstrip("/")
INTERNAL_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")
engine_options = {"connect_args": {"check_same_thread": False}} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, **engine_options)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False)
    vehicle_id: Mapped[str] = mapped_column(String(100), nullable=False)
    items: Mapped[list] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False, default="QUOTE_PENDING")
    saga_state: Mapped[str] = mapped_column(String(40), nullable=False, default="QUOTE_PENDING")
    quote_id: Mapped[str | None] = mapped_column(String(36), unique=True)
    payment_id: Mapped[str | None] = mapped_column(String(100), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    history: Mapped[list["OrderHistory"]] = relationship(back_populates="order", cascade="all, delete-orphan")


class OrderHistory(Base):
    __tablename__ = "order_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), index=True)
    previous_status: Mapped[str | None] = mapped_column(String(40))
    new_status: Mapped[str] = mapped_column(String(40), nullable=False)
    note: Mapped[str] = mapped_column(String(300), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    order: Mapped[Order] = relationship(back_populates="history")


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    aggregate_type: Mapped[str] = mapped_column(String(50), default="ORDER")
    aggregate_id: Mapped[str] = mapped_column(String(36), index=True)
    event_type: Mapped[str] = mapped_column(String(50))
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20), default="PENDING")
    error_message: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Execution(Base):
    __tablename__ = "executions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(40), nullable=False, default="QUEUED")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    notes: Mapped[str | None] = mapped_column(String(1000))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    history: Mapped[list["ExecutionHistory"]] = relationship(
        back_populates="execution", cascade="all, delete-orphan"
    )


class ExecutionHistory(Base):
    __tablename__ = "execution_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    execution_id: Mapped[str] = mapped_column(ForeignKey("executions.id"), index=True)
    previous_status: Mapped[str | None] = mapped_column(String(40))
    new_status: Mapped[str] = mapped_column(String(40), nullable=False)
    note: Mapped[str] = mapped_column(String(500), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    execution: Mapped[Execution] = relationship(back_populates="history")


class EnqueueExecution(BaseModel):
    order_id: str = Field(min_length=1, max_length=36)
    priority: int = Field(ge=0, le=10, default=0)


class ExecutionStatusUpdate(BaseModel):
    status: str = Field(min_length=1, max_length=40)
    note: str = Field(min_length=1, max_length=500)


class ExecutionCompletion(BaseModel):
    note: str = Field(min_length=1, max_length=500, default="Serviço concluído")


class CreateDiagnostic(BaseModel):
    mechanic: str = Field(min_length=1, max_length=120)
    summary: str = Field(min_length=1, max_length=500)
    obd_codes: list[str] = Field(default_factory=list)
    checklist: dict[str, Any] = Field(default_factory=dict)
    measurements: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = Field(default=None, max_length=1000)


class OrderItem(BaseModel):
    kind: Literal["PART", "SERVICE"] = "PART"
    sku: str
    quantity: int = Field(gt=0)


class CreateOrder(BaseModel):
    customer_id: str = Field(min_length=1, max_length=100)
    vehicle_id: str = Field(min_length=1, max_length=100)
    items: list[OrderItem] = Field(min_length=1)


class StatusChange(BaseModel):
    status: str = Field(min_length=1, max_length=40)
    note: str = Field(min_length=1, max_length=300)


class PaymentConfirmed(BaseModel):
    order_id: str
    payment_id: str


def get_db():
    with SessionLocal() as session:
        yield session


def require_internal_token(x_internal_token: Annotated[str | None, Header()] = None) -> None:
    if not INTERNAL_TOKEN or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid internal service token")


def record_status(db: Session, order: Order, new_status: str, note: str) -> None:
    if order.status == new_status:
        return
    order.history.append(
        OrderHistory(order_id=order.id, previous_status=order.status, new_status=new_status, note=note)
    )
    order.status = new_status


def record_outbox(
    db: Session,
    aggregate_id: str,
    event_type: str,
    payload: dict,
    status: str = "PENDING",
    error_message: str | None = None,
) -> OutboxMessage:
    msg = OutboxMessage(
        aggregate_type="ORDER",
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=payload,
        status=status,
        error_message=error_message,
        processed_at=datetime.now(timezone.utc) if status == "PROCESSED" else None,
    )
    db.add(msg)
    return msg


def ensure_order_access(order: Order, actor: Actor) -> None:
    if not actor.is_admin and actor.customer_id != order.customer_id:
        raise HTTPException(status_code=403, detail="order does not belong to the authenticated customer")


def order_document(order: Order) -> dict:
    return {
        "id": order.id,
        "customer_id": order.customer_id,
        "vehicle_id": order.vehicle_id,
        "items": order.items,
        "status": order.status,
        "saga_state": order.saga_state,
        "quote_id": order.quote_id,
        "created_at": order.created_at,
        "history": [
            {
                "previous_status": item.previous_status,
                "new_status": item.new_status,
                "note": item.note,
                "changed_at": item.changed_at,
            }
            for item in order.history
        ],
    }


COMMAND_QUEUES = {
    "CreateQuote": lambda: BILLING_COMMANDS_QUEUE_URL,
    "RefundPayment": lambda: BILLING_COMMANDS_QUEUE_URL,
    "ReserveStock": lambda: INVENTORY_COMMANDS_QUEUE_URL,
    "ReleaseStock": lambda: INVENTORY_COMMANDS_QUEUE_URL,
}


def relay_outbox(db: Session) -> int:
    """Publica no SQS os comandos pendentes da outbox (Transactional Outbox).

    Em caso de falha a mensagem permanece PENDING e será reenviada no próximo ciclo.
    """
    pending = db.scalars(
        select(OutboxMessage)
        .where(OutboxMessage.status == "PENDING", OutboxMessage.event_type.in_(COMMAND_QUEUES))
        .order_by(OutboxMessage.created_at.asc())
    ).all()
    published = 0
    for message in pending:
        try:
            messaging.publish(
                COMMAND_QUEUES[message.event_type](),
                message.event_type,
                message.payload,
                group_id=message.aggregate_id,
                dedup_id=message.id,
            )
        except Exception as exc:
            logger.exception("outbox publish failed for message %s", message.id)
            message.error_message = str(exc)[:500]
            db.commit()
            continue
        message.status = "PROCESSED"
        message.error_message = None
        message.processed_at = datetime.now(timezone.utc)
        db.commit()
        published += 1
    return published


def ensure_execution_queued(db: Session, order: Order, priority: int = 0) -> Execution:
    execution = db.scalar(select(Execution).where(Execution.order_id == order.id))
    if execution is None:
        execution = Execution(order_id=order.id, priority=priority)
        execution.history.append(
            ExecutionHistory(previous_status=None, new_status="QUEUED", note="OS adicionada à fila de execução")
        )
        db.add(execution)
    return execution


def record_execution_status(execution: Execution, new_status: str, note: str) -> None:
    if execution.status == new_status:
        return
    execution.history.append(
        ExecutionHistory(previous_status=execution.status, new_status=new_status, note=note)
    )
    execution.status = new_status
    execution.updated_at = datetime.now(timezone.utc)


def execution_document(execution: Execution) -> dict:
    return {
        "id": execution.id,
        "order_id": execution.order_id,
        "status": execution.status,
        "priority": execution.priority,
        "notes": execution.notes,
        "created_at": execution.created_at,
        "updated_at": execution.updated_at,
        "history": [
            {
                "previous_status": item.previous_status,
                "new_status": item.new_status,
                "note": item.note,
                "changed_at": item.changed_at,
            }
            for item in execution.history
        ],
    }


def cancel_order(db: Session, order: Order, note: str) -> None:
    order.saga_state = "CANCELLED"
    record_status(db, order, "CANCELLED", note)


def handle_saga_event(db: Session, event_type: str, payload: dict) -> None:
    """Máquina de estados da Saga orquestrada. Idempotente: eventos repetidos
    ou fora de ordem para o estado atual são ignorados."""
    order = db.get(Order, payload.get("order_id"))
    if order is None:
        logger.warning("saga event %s for unknown order %s", event_type, payload.get("order_id"))
        return
    state = order.saga_state
    if event_type == "QuoteCreated" and state == "QUOTE_PENDING":
        order.quote_id = payload["quote_id"]
        order.saga_state = "QUOTE_CREATED"
    elif event_type == "QuoteApproved" and state == "QUOTE_CREATED":
        order.saga_state = "AWAITING_PAYMENT"
        record_status(db, order, "AWAITING_PAYMENT", "Orçamento aprovado")
    elif event_type == "QuoteRejected" and state in {"QUOTE_PENDING", "QUOTE_CREATED"}:
        cancel_order(db, order, "Orçamento rejeitado pelo cliente")
    elif event_type == "PaymentConfirmed" and state == "AWAITING_PAYMENT":
        order.payment_id = payload["payment_id"]
        order.saga_state = "STOCK_RESERVING"
        record_outbox(
            db,
            order.id,
            "ReserveStock",
            {
                "order_id": order.id,
                "items": [
                    {"sku": item["sku"], "quantity": item["quantity"]}
                    for item in order.items
                    if item["kind"] == "PART"
                ],
            },
        )
    elif event_type == "PaymentFailed" and state in {"QUOTE_CREATED", "AWAITING_PAYMENT"}:
        cancel_order(db, order, "Pagamento recusado")
    elif event_type == "StockReserved" and state == "STOCK_RESERVING":
        order.saga_state = "COMPLETED"
        record_status(db, order, "READY_FOR_SERVICE", "Pagamento confirmado e estoque reservado")
        ensure_execution_queued(db, order)
        record_outbox(db, order.id, "SAGA_COMPLETED", {"payment_id": order.payment_id}, status="PROCESSED")
    elif event_type == "StockReleased" and state == "COMPENSATION_PENDING":
        if order.payment_id:
            record_outbox(db, order.id, "RefundPayment", {"order_id": order.id, "payment_id": order.payment_id})
        else:
            order.saga_state = "COMPENSATED"
            record_status(db, order, "CANCELLED", "Estoque liberado após cancelamento da execução")
            record_outbox(db, order.id, "COMPENSATION_COMPLETED", {"action": "STOCK_RELEASED"}, status="PROCESSED")
    elif event_type == "StockReservationFailed" and state == "STOCK_RESERVING":
        order.saga_state = "COMPENSATION_PENDING"
        record_status(db, order, "COMPENSATION_PENDING", "Reserva de estoque falhou após pagamento")
        record_outbox(
            db,
            order.id,
            "INVENTORY_RESERVATION_FAILED",
            {"payment_id": order.payment_id, "reason": payload.get("reason")},
            status="PROCESSED",
        )
        record_outbox(db, order.id, "RefundPayment", {"order_id": order.id, "payment_id": order.payment_id})
    elif event_type == "PaymentRefundRequested" and state == "COMPENSATION_PENDING":
        order.saga_state = "COMPENSATED"
        record_status(db, order, "CANCELLED", "Compensação solicitada ao Billing; confirmar reembolso no provedor")
        record_outbox(
            db,
            order.id,
            "COMPENSATION_COMPLETED",
            {"payment_id": order.payment_id, "action": "REFUND_REQUIRED"},
            status="PROCESSED",
        )
    else:
        logger.info("saga event %s ignored for order %s in state %s", event_type, order.id, state)
        return
    db.commit()


def consume_saga_event(event_type: str, payload: dict) -> None:
    with SessionLocal() as session:
        handle_saga_event(session, event_type, payload)


def relay_loop(stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            with SessionLocal() as session:
                relay_outbox(session)
        except Exception:
            logger.exception("outbox relay cycle failed")
        stop.wait(OUTBOX_RELAY_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    stops: list[threading.Event] = []
    if messaging.MESSAGING_ENABLED:
        stops.append(messaging.start_consumer(OS_EVENTS_QUEUE_URL, consume_saga_event))
        relay_stop = threading.Event()
        threading.Thread(target=relay_loop, args=(relay_stop,), name="outbox-relay", daemon=True).start()
        stops.append(relay_stop)
    yield
    for stop in stops:
        stop.set()


app = FastAPI(title="Ordem de Serviço", version="1.0.0", lifespan=lifespan)
observability.instrument_app(app, "os-service")


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        logger.exception("database readiness check failed")
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ok", "service": "os-service"}


@app.get("/health/live")
def liveness():
    return {"status": "ok", "service": "os-service"}


@app.post("/orders", status_code=status.HTTP_202_ACCEPTED)
def create_order(
    payload: CreateOrder,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin and actor.customer_id != payload.customer_id:
        raise HTTPException(status_code=403, detail="customer cannot open an order for another customer")
    items = []
    for requested_item in payload.items:
        catalog_path = "parts" if requested_item.kind == "PART" else "services"
        try:
            catalog_response = httpx.get(
                f"{INVENTORY_URL}/internal/{catalog_path}/{requested_item.sku}",
                headers={"X-Internal-Token": INTERNAL_TOKEN},
                timeout=5,
            )
            if catalog_response.status_code == 404:
                raise HTTPException(status_code=422, detail=f"unknown catalog item: {requested_item.sku}")
            catalog_response.raise_for_status()
            catalog_item = catalog_response.json()
        except httpx.HTTPError as exc:
            logger.exception("catalog lookup failed for item %s", requested_item.sku)
            raise HTTPException(status_code=503, detail="catalog unavailable; no order was created") from exc
        if not catalog_item.get("active", True):
            raise HTTPException(status_code=422, detail=f"inactive catalog item: {requested_item.sku}")
        items.append(
            {
                "kind": requested_item.kind,
                "sku": requested_item.sku,
                "name": catalog_item["name"],
                "quantity": requested_item.quantity,
                "unit_price": str(catalog_item["unit_price"]),
            }
        )
    order = Order(
        customer_id=payload.customer_id,
        vehicle_id=payload.vehicle_id,
        items=items,
    )
    order.history.append(OrderHistory(new_status="QUOTE_PENDING", note="OS aberta"))
    db.add(order)
    db.commit()
    db.refresh(order)
    request = {
        "order_id": order.id,
        "customer_id": order.customer_id,
        "items": order.items,
    }
    if messaging.MESSAGING_ENABLED:
        record_outbox(db, order.id, "CreateQuote", request)
        db.commit()
        return order_document(order)
    try:
        response = httpx.post(
            f"{BILLING_URL}/quotes",
            json=request,
            headers={"X-Internal-Token": INTERNAL_TOKEN},
            timeout=5,
        )
        response.raise_for_status()
        quote_id = response.json().get("id")
        if not isinstance(quote_id, str) or not quote_id:
            raise ValueError("billing response did not include a quote id")
        order.quote_id = quote_id
        order.saga_state = "QUOTE_CREATED"
        record_outbox(db, order.id, "QUOTE_REQUESTED", request, status="PROCESSED")
        db.commit()
    except (httpx.HTTPError, ValueError) as exc:
        logger.exception("billing quote creation failed for order %s", order.id)
        order.saga_state = "QUOTE_PENDING"
        record_outbox(db, order.id, "QUOTE_REQUESTED", request, status="FAILED", error_message=str(exc))
        db.commit()
    return order_document(order)


@app.post("/orders/{order_id}/retry-quote", status_code=status.HTTP_202_ACCEPTED)
def retry_quote(
    order_id: str,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can retry the saga")
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    if order.saga_state == "QUOTE_CREATED":
        return order_document(order)
    try:
        response = httpx.post(
            f"{BILLING_URL}/quotes",
            json={"order_id": order.id, "customer_id": order.customer_id, "items": order.items},
            headers={"X-Internal-Token": INTERNAL_TOKEN},
            timeout=5,
        )
        response.raise_for_status()
        quote_id = response.json().get("id")
        if not isinstance(quote_id, str) or not quote_id:
            raise ValueError("billing response did not include a quote id")
    except httpx.HTTPError as exc:
        logger.exception("billing quote retry failed for order %s", order.id)
        raise HTTPException(status_code=503, detail="quote remains pending; retry later") from exc
    order.quote_id = quote_id
    order.saga_state = "QUOTE_CREATED"
    db.commit()
    return order_document(order)


@app.get("/orders/{order_id}")
def get_order(order_id: str, actor: Actor = Depends(get_actor), db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    ensure_order_access(order, actor)
    return order_document(order)


@app.get("/orders/{order_id}/history")
def get_history(order_id: str, actor: Actor = Depends(get_actor), db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    ensure_order_access(order, actor)
    return order_document(order)["history"]


@app.patch("/orders/{order_id}/status")
def change_status(
    order_id: str,
    payload: StatusChange,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can change order status")
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    transitions = {
        "READY_FOR_SERVICE": "IN_DIAGNOSIS",
        "IN_DIAGNOSIS": "IN_PROGRESS",
        "IN_PROGRESS": "COMPLETED",
        "COMPLETED": "DELIVERED",
    }
    if transitions.get(order.status) != payload.status:
        raise HTTPException(status_code=409, detail=f"invalid transition from {order.status} to {payload.status}")
    record_status(db, order, payload.status, payload.note)
    db.commit()
    return order_document(order)


@app.patch("/internal/orders/{order_id}/status")
def internal_change_status(
    order_id: str,
    payload: StatusChange,
    db: Session = Depends(get_db),
    _: None = Depends(require_internal_token),
):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    allowed = {"IN_DIAGNOSIS", "IN_PROGRESS", "COMPLETED", "CANCELLED"}
    if payload.status not in allowed:
        raise HTTPException(status_code=409, detail=f"unsupported execution status: {payload.status}")
    record_status(db, order, payload.status, payload.note)
    db.commit()
    return order_document(order)


@app.post("/internal/orders/{order_id}/quote-approved")
def quote_approved(
    order_id: str,
    db: Session = Depends(get_db),
    _: None = Depends(require_internal_token),
):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    order.saga_state = "AWAITING_PAYMENT"
    record_status(db, order, "AWAITING_PAYMENT", "Orçamento aprovado")
    db.commit()
    return {"order_id": order.id, "saga_state": order.saga_state}


@app.post("/internal/payments/confirmed")
def payment_confirmed(
    payload: PaymentConfirmed,
    db: Session = Depends(get_db),
    _: None = Depends(require_internal_token),
):
    order = db.get(Order, payload.order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    if order.payment_id == payload.payment_id and order.status == "READY_FOR_SERVICE":
        return {"order_id": order.id, "status": order.status}
    if order.payment_id not in (None, payload.payment_id):
        raise HTTPException(status_code=409, detail="order already has a different payment")
    order.payment_id = payload.payment_id
    try:
        response = httpx.post(
            f"{INVENTORY_URL}/reservations",
            json={
                "order_id": order.id,
                "items": [
                    {"sku": item["sku"], "quantity": item["quantity"]}
                    for item in order.items
                    if item["kind"] == "PART"
                ],
            },
            headers={"X-Internal-Token": INTERNAL_TOKEN},
            timeout=5,
        )
        response.raise_for_status()
    except httpx.HTTPError as inventory_error:
        logger.exception("inventory reservation failed for order %s", order.id)
        order.saga_state = "COMPENSATION_PENDING"
        record_status(db, order, "COMPENSATION_PENDING", "Reserva de estoque falhou após pagamento")
        record_outbox(
            db,
            order.id,
            "INVENTORY_RESERVATION_FAILED",
            {"payment_id": payload.payment_id},
            status="FAILED",
            error_message=str(inventory_error),
        )
        db.commit()
        try:
            compensation = httpx.post(
                f"{BILLING_URL}/internal/payments/{payload.payment_id}/compensate",
                headers={"X-Internal-Token": INTERNAL_TOKEN},
                timeout=5,
            )
            compensation.raise_for_status()
        except httpx.HTTPError as compensation_error:
            logger.exception("payment compensation could not be recorded for order %s", order.id)
            record_outbox(
                db,
                order.id,
                "COMPENSATION_FAILED",
                {"payment_id": payload.payment_id},
                status="FAILED",
                error_message=str(compensation_error),
            )
            db.commit()
            raise HTTPException(
                status_code=503,
                detail="inventory failed; payment compensation remains pending",
            ) from compensation_error
        order.saga_state = "COMPENSATED"
        record_status(db, order, "CANCELLED", "Compensação solicitada ao Billing; confirmar reembolso no provedor")
        record_outbox(
            db,
            order.id,
            "COMPENSATION_COMPLETED",
            {"payment_id": payload.payment_id, "action": "REFUND_REQUIRED"},
            status="PROCESSED",
        )
        db.commit()
        return {
            "order_id": order.id,
            "status": order.status,
            "saga_state": order.saga_state,
            "compensation": "refund reconciliation required",
        }
    order.saga_state = "COMPLETED"
    record_status(db, order, "READY_FOR_SERVICE", "Pagamento confirmado e estoque reservado")
    ensure_execution_queued(db, order)
    record_outbox(
        db,
        order.id,
        "SAGA_COMPLETED",
        {"payment_id": payload.payment_id},
        status="PROCESSED",
    )
    db.commit()
    return {"order_id": order.id, "status": order.status, "saga_state": order.saga_state}


@app.post("/orders/{order_id}/retry-compensation", status_code=status.HTTP_202_ACCEPTED)
def retry_compensation(
    order_id: str,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can retry compensation")
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    if order.saga_state == "COMPENSATED":
        return order_document(order)
    if order.saga_state != "COMPENSATION_PENDING":
        raise HTTPException(status_code=409, detail=f"order is not awaiting compensation (saga_state={order.saga_state})")
    if not order.payment_id:
        raise HTTPException(status_code=422, detail="order has no payment to compensate")
    try:
        compensation = httpx.post(
            f"{BILLING_URL}/internal/payments/{order.payment_id}/compensate",
            headers={"X-Internal-Token": INTERNAL_TOKEN},
            timeout=5,
        )
        compensation.raise_for_status()
    except httpx.HTTPError as exc:
        logger.exception("retry compensation failed for order %s", order.id)
        record_outbox(
            db,
            order.id,
            "COMPENSATION_RETRY_FAILED",
            {"payment_id": order.payment_id},
            status="FAILED",
            error_message=str(exc),
        )
        db.commit()
        raise HTTPException(status_code=503, detail="compensation remains pending; retry later") from exc
    order.saga_state = "COMPENSATED"
    record_status(db, order, "CANCELLED", "Compensação solicitada ao Billing; confirmar reembolso no provedor")
    record_outbox(
        db,
        order.id,
        "COMPENSATION_COMPLETED",
        {"payment_id": order.payment_id, "action": "REFUND_REQUIRED"},
        status="PROCESSED",
    )
    db.commit()
    return order_document(order)


@app.get("/orders/{order_id}/outbox")
def get_order_outbox(
    order_id: str,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    ensure_order_access(order, actor)
    messages = db.scalars(
        select(OutboxMessage).where(OutboxMessage.aggregate_id == order_id).order_by(OutboxMessage.created_at.asc())
    ).all()
    return [
        {
            "id": m.id,
            "event_type": m.event_type,
            "payload": m.payload,
            "status": m.status,
            "error_message": m.error_message,
            "created_at": m.created_at,
            "processed_at": m.processed_at,
        }
        for m in messages
    ]


def compensate_execution_cancellation(db: Session, order: Order, note: str) -> None:
    has_parts = any(item.get("kind") == "PART" for item in order.items)
    if messaging.MESSAGING_ENABLED:
        order.saga_state = "COMPENSATION_PENDING"
        record_status(db, order, "COMPENSATION_PENDING", note)
        if has_parts:
            record_outbox(db, order.id, "ReleaseStock", {"order_id": order.id})
        elif order.payment_id:
            record_outbox(db, order.id, "RefundPayment", {"order_id": order.id, "payment_id": order.payment_id})
        else:
            order.saga_state = "COMPENSATED"
            record_status(db, order, "CANCELLED", note)
        return

    try:
        if has_parts:
            rel = httpx.delete(
                f"{INVENTORY_URL}/reservations/{order.id}",
                headers={"X-Internal-Token": INTERNAL_TOKEN},
                timeout=5,
            )
            rel.raise_for_status()
        if order.payment_id:
            comp = httpx.post(
                f"{BILLING_URL}/internal/payments/{order.payment_id}/compensate",
                headers={"X-Internal-Token": INTERNAL_TOKEN},
                timeout=5,
            )
            comp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.exception("execution cancellation compensation failed for order %s", order.id)
        order.saga_state = "COMPENSATION_PENDING"
        record_status(db, order, "COMPENSATION_PENDING", "Compensação de cancelamento pendente")
        record_outbox(
            db,
            order.id,
            "EXECUTION_COMPENSATION_FAILED",
            {"payment_id": order.payment_id},
            status="FAILED",
            error_message=str(exc),
        )
        db.commit()
        raise HTTPException(status_code=503, detail="execution cancellation compensation pending") from exc

    order.saga_state = "COMPENSATED"
    record_status(db, order, "CANCELLED", f"{note}; estoque liberado e estorno solicitado")
    record_outbox(
        db,
        order.id,
        "COMPENSATION_COMPLETED",
        {"payment_id": order.payment_id, "action": "STOCK_RELEASED_AND_REFUND_REQUIRED"},
        status="PROCESSED",
    )


@app.post("/executions", status_code=status.HTTP_201_CREATED)
def create_execution(
    payload: EnqueueExecution,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can enqueue an order")
    order = db.get(Order, payload.order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    existing = db.scalar(select(Execution).where(Execution.order_id == payload.order_id))
    if existing is not None:
        existing.priority = payload.priority
        db.commit()
        db.refresh(existing)
        return execution_document(existing)
    execution = ensure_execution_queued(db, order, priority=payload.priority)
    db.commit()
    db.refresh(execution)
    return execution_document(execution)


@app.get("/executions")
def list_executions(actor: Actor = Depends(get_actor), db: Session = Depends(get_db)):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can list the execution queue")
    executions = db.scalars(
        select(Execution).order_by(Execution.priority.desc(), Execution.created_at.asc())
    ).all()
    return [execution_document(item) for item in executions]


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str, actor: Actor = Depends(get_actor), db: Session = Depends(get_db)):
    execution = db.get(Execution, execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="execution not found")
    order = db.get(Order, execution.order_id)
    if order is not None:
        ensure_order_access(order, actor)
    return execution_document(execution)


@app.post("/executions/next")
def claim_next_execution(actor: Actor = Depends(get_actor), db: Session = Depends(get_db)):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can claim from the execution queue")
    execution = db.scalars(
        select(Execution)
        .where(Execution.status == "QUEUED")
        .order_by(Execution.priority.desc(), Execution.created_at.asc())
    ).first()
    if execution is None:
        return {"execution": None}
    return {"execution": execution_document(execution)}


@app.patch("/executions/{execution_id}/status")
def update_execution_status(
    execution_id: str,
    payload: ExecutionStatusUpdate,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can update execution status")
    execution = db.get(Execution, execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="execution not found")
    allowed = {
        "QUEUED": {"DIAGNOSING", "CANCELLED"},
        "DIAGNOSING": {"REPAIRING", "CANCELLED"},
        "REPAIRING": {"COMPLETED", "CANCELLED"},
    }
    if payload.status not in allowed.get(execution.status, set()) and payload.status != execution.status:
        raise HTTPException(status_code=409, detail=f"invalid transition from {execution.status} to {payload.status}")
    record_execution_status(execution, payload.status, payload.note)
    order = db.get(Order, execution.order_id)
    if order is not None:
        os_status_map = {
            "DIAGNOSING": "IN_DIAGNOSIS",
            "REPAIRING": "IN_PROGRESS",
            "COMPLETED": "COMPLETED",
        }
        if payload.status in os_status_map:
            record_status(db, order, os_status_map[payload.status], payload.note)
        elif payload.status == "CANCELLED":
            compensate_execution_cancellation(db, order, payload.note)
    db.commit()
    db.refresh(execution)
    return execution_document(execution)


@app.post("/executions/{execution_id}/complete")
def complete_execution(
    execution_id: str,
    payload: ExecutionCompletion,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can complete execution")
    execution = db.get(Execution, execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="execution not found")
    if execution.status not in {"DIAGNOSING", "REPAIRING"}:
        raise HTTPException(status_code=409, detail=f"cannot complete execution in {execution.status}")
    record_execution_status(execution, "COMPLETED", payload.note)
    order = db.get(Order, execution.order_id)
    if order is not None:
        record_status(db, order, "COMPLETED", payload.note)
    db.commit()
    db.refresh(execution)
    return execution_document(execution)


@app.post("/orders/{order_id}/diagnostics", status_code=status.HTTP_201_CREATED)
def add_order_diagnostic(
    order_id: str,
    payload: CreateDiagnostic,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if not actor.is_admin:
        raise HTTPException(status_code=403, detail="only an administrator can record diagnostics")
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    return diagnostics.save_diagnostic(
        order_id=order.id,
        mechanic=payload.mechanic,
        summary=payload.summary,
        obd_codes=payload.obd_codes,
        checklist=payload.checklist,
        measurements=payload.measurements,
        notes=payload.notes,
    )


@app.get("/orders/{order_id}/diagnostics")
def get_order_diagnostics(
    order_id: str,
    actor: Actor = Depends(get_actor),
    db: Session = Depends(get_db),
):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    ensure_order_access(order, actor)
    return diagnostics.list_diagnostics(order.id)
