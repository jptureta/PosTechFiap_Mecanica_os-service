import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import diagnostics, main
from app.auth import Actor, get_actor


@pytest.fixture
def env(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    main.Base.metadata.create_all(engine)
    monkeypatch.setattr(main, "engine", engine)
    main.INTERNAL_TOKEN = "test-internal"

    def override_db():
        with factory() as session:
            yield session

    main.app.dependency_overrides[main.get_db] = override_db
    main.app.dependency_overrides[get_actor] = lambda: Actor(is_admin=True, customer_id=None)

    monkeypatch.setattr(
        main.httpx,
        "get",
        lambda url, **kwargs: type(
            "Response",
            (),
            {
                "status_code": 200,
                "json": lambda self: {"name": "Oil", "unit_price": "10.00", "active": True},
                "raise_for_status": lambda self: None,
            },
        )(),
    )
    monkeypatch.setattr(
        main.httpx,
        "post",
        lambda *args, **kwargs: type(
            "Response",
            (),
            {"raise_for_status": lambda self: None, "json": lambda self: {"id": "quote-1"}},
        )(),
    )
    monkeypatch.setattr(
        main.httpx,
        "delete",
        lambda *args, **kwargs: type("Response", (), {"raise_for_status": lambda self: None})(),
    )

    with mock_aws():
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setattr(diagnostics, "_resource", None)
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
        yield TestClient(main.app)

    main.app.dependency_overrides.clear()
    engine.dispose()


def test_execution_flow_and_dynamodb_diagnostics(env):
    order = env.post(
        "/orders",
        json={"customer_id": "c1", "vehicle_id": "v1", "items": [{"sku": "oil", "quantity": 1}]},
    ).json()
    env.post(
        "/internal/payments/confirmed",
        json={"order_id": order["id"], "payment_id": "pay-1"},
        headers={"X-Internal-Token": "test-internal"},
    )

    claimed = env.post("/executions/next").json()["execution"]
    assert claimed["order_id"] == order["id"]
    exec_id = claimed["id"]

    diag_step = env.patch(f"/executions/{exec_id}/status", json={"status": "DIAGNOSING", "note": "Iniciando scanner"})
    assert diag_step.status_code == 200
    assert env.get(f"/orders/{order['id']}").json()["status"] == "IN_DIAGNOSIS"

    created_diag = env.post(
        f"/orders/{order['id']}/diagnostics",
        json={
            "mechanic": "Carlos",
            "summary": "Falha ignição cilindro 2",
            "obd_codes": ["P0302"],
            "checklist": {"velas": "trocar", "bobina": "ok"},
            "measurements": {"compressao_psi": 145, "pressao_combustivel_bar": 3.5},
            "notes": "Isolador trincado",
        },
    )
    assert created_diag.status_code == 201

    listed = env.get(f"/orders/{order['id']}/diagnostics")
    assert listed.status_code == 200
    assert len(listed.json()) == 1
    assert listed.json()[0]["obd_codes"] == ["P0302"]
    assert listed.json()[0]["checklist"]["velas"] == "trocar"

    env.patch(f"/executions/{exec_id}/status", json={"status": "REPAIRING", "note": "Substituindo vela"})
    assert env.get(f"/orders/{order['id']}").json()["status"] == "IN_PROGRESS"

    completed = env.post(f"/executions/{exec_id}/complete", json={"note": "Teste de rodagem ok"})
    assert completed.status_code == 200
    assert completed.json()["status"] == "COMPLETED"
    assert env.get(f"/orders/{order['id']}").json()["status"] == "COMPLETED"


def test_execution_cancellation_triggers_compensation(env):
    order = env.post(
        "/orders",
        json={"customer_id": "c1", "vehicle_id": "v1", "items": [{"sku": "oil", "quantity": 1}]},
    ).json()
    env.post(
        "/internal/payments/confirmed",
        json={"order_id": order["id"], "payment_id": "pay-cancel"},
        headers={"X-Internal-Token": "test-internal"},
    )
    exec_id = env.post("/executions/next").json()["execution"]["id"]
    res = env.patch(f"/executions/{exec_id}/status", json={"status": "CANCELLED", "note": "Chassi condenado"})
    assert res.status_code == 200
    updated_order = env.get(f"/orders/{order['id']}").json()
    assert updated_order["status"] == "CANCELLED"
    assert updated_order["saga_state"] == "COMPENSATED"

