import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from pytest_bdd import given, parsers, scenarios, then, when
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import diagnostics, main, messaging
from app.auth import Actor, get_actor

scenarios("features/order_saga.feature")


@pytest.fixture
def ctx(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    main.Base.metadata.create_all(engine)
    monkeypatch.setattr(main, "engine", engine)
    monkeypatch.setattr(main, "SessionLocal", factory)
    monkeypatch.setattr(messaging, "MESSAGING_ENABLED", True)

    def override_db():
        with factory() as session:
            yield session

    main.app.dependency_overrides[main.get_db] = override_db
    main.app.dependency_overrides[get_actor] = lambda: Actor(is_admin=True, customer_id="cli-10")

    catalog: dict[str, dict] = {}
    monkeypatch.setattr(
        main.httpx,
        "get",
        lambda url, **kw: type(
            "Resp",
            (),
            {
                "status_code": 200 if url.rsplit("/", 1)[-1] in catalog else 404,
                "json": lambda self: catalog[url.rsplit("/", 1)[-1]],
                "raise_for_status": lambda self: None,
            },
        )(),
    )

    with mock_aws():
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setattr(messaging, "_client", None)
        monkeypatch.setattr(diagnostics, "_resource", None)
        sqs = boto3.client("sqs", region_name="us-east-1")
        urls = {
            name: sqs.create_queue(QueueName=f"{name}.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
            for name in ("billing-commands", "inventory-commands", "os-events")
        }
        monkeypatch.setattr(main, "BILLING_COMMANDS_QUEUE_URL", urls["billing-commands"])
        monkeypatch.setattr(main, "INVENTORY_COMMANDS_QUEUE_URL", urls["inventory-commands"])
        monkeypatch.setattr(main, "OS_EVENTS_QUEUE_URL", urls["os-events"])

        ddb = boto3.client("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName=diagnostics.DIAGNOSTICS_TABLE_NAME,
            KeySchema=[
                {"AttributeName": "order_id", "KeyType": "HASH"},
                {"AttributeName": "diagnostic_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "order_id", "AttributeType": "S"},
                {"AttributeName": "diagnostic_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield {"client": TestClient(main.app), "factory": factory, "urls": urls, "catalog": catalog, "order_id": None}

    main.app.dependency_overrides.clear()
    engine.dispose()


@given(parsers.parse('o catálogo possui a peça "{part_sku}" ao preço "{part_price}" e o serviço "{svc_sku}" ao preço "{svc_price}"'))
def setup_catalog(ctx, part_sku, part_price, svc_sku, svc_price):
    ctx["catalog"][part_sku] = {"name": part_sku, "unit_price": part_price, "active": True}
    ctx["catalog"][svc_sku] = {"name": svc_sku, "unit_price": svc_price, "active": True}


@when(parsers.parse('o cliente "{customer_id}" abre uma OS para o veículo "{vehicle_id}" com 1 peça "{part_sku}" e 1 serviço "{svc_sku}"'))
def open_order_with_part_and_service(ctx, customer_id, vehicle_id, part_sku, svc_sku):
    res = ctx["client"].post(
        "/orders",
        json={
            "customer_id": customer_id,
            "vehicle_id": vehicle_id,
            "items": [
                {"kind": "PART", "sku": part_sku, "quantity": 1},
                {"kind": "SERVICE", "sku": svc_sku, "quantity": 1},
            ],
        },
        headers={"X-Correlation-ID": "bdd-happy-flow"},
    )
    assert res.status_code == 202
    assert res.headers["X-Correlation-ID"] == "bdd-happy-flow"
    ctx["order_id"] = res.json()["id"]


@when(parsers.parse('o cliente "{customer_id}" abre uma OS para o veículo "{vehicle_id}" com 2 peças "{part_sku}"'))
def open_order_with_parts(ctx, customer_id, vehicle_id, part_sku):
    res = ctx["client"].post(
        "/orders",
        json={
            "customer_id": customer_id,
            "vehicle_id": vehicle_id,
            "items": [{"kind": "PART", "sku": part_sku, "quantity": 2}],
        },
    )
    assert res.status_code == 202
    ctx["order_id"] = res.json()["id"]


@when("o relay da outbox publica os comandos pendentes no SQS")
def run_outbox_relay(ctx):
    with ctx["factory"]() as session:
        assert main.relay_outbox(session) >= 1


@when("o Billing processa a criação do orçamento e emite o evento QuoteCreated")
def billing_creates_quote(ctx):
    cmds = []
    messaging.process_batch(ctx["urls"]["billing-commands"], lambda t, p: cmds.append((t, p)), wait_seconds=0)
    assert cmds[0][0] == "CreateQuote"
    messaging.publish(
        ctx["urls"]["os-events"],
        "QuoteCreated",
        {"order_id": ctx["order_id"], "quote_id": "quote-bdd-1"},
        group_id=ctx["order_id"],
    )
    messaging.process_batch(ctx["urls"]["os-events"], main.consume_saga_event, wait_seconds=0)


@when("o cliente aprova o orçamento gerando o evento QuoteApproved")
def approve_quote_event(ctx):
    messaging.publish(ctx["urls"]["os-events"], "QuoteApproved", {"order_id": ctx["order_id"]}, group_id=ctx["order_id"])
    messaging.process_batch(ctx["urls"]["os-events"], main.consume_saga_event, wait_seconds=0)


@when(parsers.parse('o pagamento "{payment_id}" é confirmado gerando o evento PaymentConfirmed'))
def confirm_payment_event(ctx, payment_id):
    messaging.publish(
        ctx["urls"]["os-events"],
        "PaymentConfirmed",
        {"order_id": ctx["order_id"], "payment_id": payment_id},
        group_id=ctx["order_id"],
    )
    messaging.process_batch(ctx["urls"]["os-events"], main.consume_saga_event, wait_seconds=0)
    with ctx["factory"]() as session:
        assert main.relay_outbox(session) == 1


@when("o Inventory reserva as peças gerando o evento StockReserved")
def reserve_stock_event(ctx):
    cmds = []
    messaging.process_batch(ctx["urls"]["inventory-commands"], lambda t, p: cmds.append((t, p)), wait_seconds=0)
    assert cmds[0][0] == "ReserveStock"
    messaging.publish(ctx["urls"]["os-events"], "StockReserved", {"order_id": ctx["order_id"]}, group_id=ctx["order_id"])
    messaging.process_batch(ctx["urls"]["os-events"], main.consume_saga_event, wait_seconds=0)


@then(parsers.parse('a OS deve estar no status "{expected_status}" e na fila de execução'))
def assert_ready_and_queued(ctx, expected_status):
    order = ctx["client"].get(f"/orders/{ctx['order_id']}").json()
    assert order["status"] == expected_status
    queue = ctx["client"].get("/executions").json()
    assert any(item["order_id"] == ctx["order_id"] and item["status"] == "QUEUED" for item in queue)


@when(parsers.parse('o mecânico "{mechanic}" inicia o diagnóstico e registra o código OBD "{obd}" no DynamoDB'))
def mechanic_diagnoses(ctx, mechanic, obd):
    claimed = ctx["client"].post("/executions/next").json()["execution"]
    ctx["execution_id"] = claimed["id"]
    ctx["client"].patch(
        f"/executions/{claimed['id']}/status",
        json={"status": "DIAGNOSING", "note": "Leitura de scanner OBD"},
    )
    res = ctx["client"].post(
        f"/orders/{ctx['order_id']}/diagnostics",
        json={"mechanic": mechanic, "summary": "Catalisador abaixo da eficiência", "obd_codes": [obd]},
    )
    assert res.status_code == 201


@when("o mecânico avança para reparo, conclui a execução e realiza a entrega do veículo")
def mechanic_repairs_and_delivers(ctx):
    exec_id = ctx["execution_id"]
    ctx["client"].patch(f"/executions/{exec_id}/status", json={"status": "REPAIRING", "note": "Troca de óleo e filtro"})
    ctx["client"].post(f"/executions/{exec_id}/complete", json={"note": "Serviço finalizado"})
    ctx["client"].patch(
        f"/orders/{ctx['order_id']}/status",
        json={"status": "DELIVERED", "note": "Veículo entregue ao cliente"},
    )


@then(parsers.parse('a OS deve estar no status "{expected_status}" e possuir 1 laudo técnico no DynamoDB'))
def assert_delivered_with_diagnostic(ctx, expected_status):
    order = ctx["client"].get(f"/orders/{ctx['order_id']}").json()
    assert order["status"] == expected_status
    diags = ctx["client"].get(f"/orders/{ctx['order_id']}/diagnostics").json()
    assert len(diags) == 1
    assert diags[0]["obd_codes"] == ["P0420"]


@when(parsers.parse('o orçamento é criado, aprovado e o pagamento "{payment_id}" é confirmado'))
def fast_forward_to_payment(ctx, payment_id):
    with ctx["factory"]() as session:
        main.handle_saga_event(session, "QuoteCreated", {"order_id": ctx["order_id"], "quote_id": "quote-bdd-2"})
        main.handle_saga_event(session, "QuoteApproved", {"order_id": ctx["order_id"]})
        main.handle_saga_event(session, "PaymentConfirmed", {"order_id": ctx["order_id"], "payment_id": payment_id})
        main.relay_outbox(session)


@when("o Inventory informa falha de estoque gerando o evento StockReservationFailed")
def inventory_fails(ctx):
    with ctx["factory"]() as session:
        main.handle_saga_event(
            session,
            "StockReservationFailed",
            {"order_id": ctx["order_id"], "reason": "insufficient stock"},
        )
        assert main.relay_outbox(session) == 1


@when("o Billing processa a compensação gerando o evento PaymentRefundRequested")
def billing_compensates(ctx):
    cmds = []
    messaging.process_batch(ctx["urls"]["billing-commands"], lambda t, p: cmds.append((t, p)), wait_seconds=0)
    assert any(c[0] == "RefundPayment" for c in cmds)
    with ctx["factory"]() as session:
        main.handle_saga_event(session, "PaymentRefundRequested", {"order_id": ctx["order_id"]})


@then(parsers.parse('a OS deve estar no status "{expected_status}" com estado de Saga "{expected_saga}"'))
def assert_compensated(ctx, expected_status, expected_saga):
    order = ctx["client"].get(f"/orders/{ctx['order_id']}").json()
    assert order["status"] == expected_status
    assert order["saga_state"] == expected_saga

