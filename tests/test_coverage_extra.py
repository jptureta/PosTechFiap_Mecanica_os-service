from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import auth, main, messaging, observability
from app.auth import Actor, get_actor
from tests.test_api import client_for_test


def cred(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def test_auth_missing_secret_returns_503(monkeypatch):
    monkeypatch.delenv("JWT_SECRET_KEY", raising=False)
    with pytest.raises(HTTPException) as exc:
        auth.get_actor(cred("any"))
    assert exc.value.status_code == 503


def test_auth_invalid_jwt_returns_401(monkeypatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "secret-1")
    with pytest.raises(HTTPException) as exc:
        auth.get_actor(cred("not-a-jwt"))
    assert exc.value.status_code == 401


def test_auth_customer_and_admin_tokens(monkeypatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "secret-1")
    admin_tok = jwt.encode({"sub": "u1", "tipo": "admin"}, "secret-1", algorithm="HS256")
    assert auth.get_actor(cred(admin_tok)) == Actor(is_admin=True, customer_id=None)

    cust_tok = jwt.encode({"sub": "u2", "tipo": "cliente", "cliente_id": 42}, "secret-1", algorithm="HS256")
    assert auth.get_actor(cred(cust_tok)) == Actor(is_admin=False, customer_id="42")

    bad_cust = jwt.encode({"sub": "u2", "tipo": "cliente"}, "secret-1", algorithm="HS256")
    with pytest.raises(HTTPException) as exc:
        auth.get_actor(cred(bad_cust))
    assert exc.value.status_code == 401

    bad_type = jwt.encode({"sub": "u3", "tipo": "outro"}, "secret-1", algorithm="HS256")
    with pytest.raises(HTTPException) as exc:
        auth.get_actor(cred(bad_type))
    assert exc.value.status_code == 401


def test_health_live_metrics_and_internal_endpoints(monkeypatch):
    client, engine = client_for_test(monkeypatch)
    try:
        assert client.get("/health").status_code == 200
        assert client.get("/health/live").status_code == 200
        assert client.get("/metrics").json()["service"] == "os-service"

        order = client.post(
            "/orders",
            json={"customer_id": "customer-1", "vehicle_id": "v1", "items": [{"sku": "oil-1", "quantity": 1}]},
        ).json()
        oid = order["id"]

        assert client.post(f"/internal/orders/{oid}/quote-approved").status_code == 401
        ok = client.post(
            f"/internal/orders/{oid}/quote-approved",
            headers={"X-Internal-Token": "test-internal"},
        )
        assert ok.status_code == 200
        assert ok.json()["saga_state"] == "AWAITING_PAYMENT"

        assert (
            client.patch(
                f"/internal/orders/{oid}/status",
                json={"status": "IN_DIAGNOSIS", "note": "diag"},
                headers={"X-Internal-Token": "test-internal"},
            ).status_code
            == 200
        )
        assert (
            client.patch(
                f"/internal/orders/{oid}/status",
                json={"status": "INVALID", "note": "bad"},
                headers={"X-Internal-Token": "test-internal"},
            ).status_code
            == 409
        )
    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_order_access_controls_and_retry_quote(monkeypatch):
    client, engine = client_for_test(monkeypatch)
    try:
        def fail_post(url, **kw):
            req = main.httpx.Request("POST", url)
            raise main.httpx.ConnectError("down", request=req)

        monkeypatch.setattr(main.httpx, "post", fail_post)
        order = client.post(
            "/orders",
            json={"customer_id": "customer-1", "vehicle_id": "v1", "items": [{"sku": "oil-1", "quantity": 1}]},
        ).json()
        assert order["saga_state"] == "QUOTE_PENDING"
        assert client.post(f"/orders/{order['id']}/retry-quote").status_code == 503

        monkeypatch.setattr(
            main.httpx,
            "post",
            lambda *a, **kw: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"id": "q-retried"}),
        )
        retried = client.post(f"/orders/{order['id']}/retry-quote")
        assert retried.status_code == 202
        assert retried.json()["saga_state"] == "QUOTE_CREATED"
        assert client.post(f"/orders/{order['id']}/retry-quote").status_code == 202

        main.app.dependency_overrides[get_actor] = lambda: Actor(is_admin=False, customer_id="other")
        assert client.get(f"/orders/{order['id']}").status_code == 403
        assert (
            client.post(
                "/orders",
                json={"customer_id": "customer-1", "vehicle_id": "v1", "items": [{"sku": "oil-1", "quantity": 1}]},
            ).status_code
            == 403
        )
        assert client.post(f"/orders/{order['id']}/retry-quote").status_code == 403
        assert client.patch(f"/orders/{order['id']}/status", json={"status": "IN_DIAGNOSIS", "note": "x"}).status_code == 403
    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_catalog_validation_and_execution_edge_cases(monkeypatch):
    client, engine = client_for_test(monkeypatch)
    try:
        monkeypatch.setattr(
            main.httpx,
            "get",
            lambda url, **kw: SimpleNamespace(status_code=404, json=lambda: {}, raise_for_status=lambda: None),
        )
        assert (
            client.post(
                "/orders",
                json={"customer_id": "c1", "vehicle_id": "v1", "items": [{"sku": "missing", "quantity": 1}]},
            ).status_code
            == 422
        )

        monkeypatch.setattr(
            main.httpx,
            "get",
            lambda url, **kw: SimpleNamespace(
                status_code=200,
                json=lambda: {"name": "Inactive", "unit_price": "10.00", "active": False},
                raise_for_status=lambda: None,
            ),
        )
        assert (
            client.post(
                "/orders",
                json={"customer_id": "c1", "vehicle_id": "v1", "items": [{"sku": "old", "quantity": 1}]},
            ).status_code
            == 422
        )

        assert client.post("/executions/next").json() == {"execution": None}
        assert client.get("/executions/missing").status_code == 404
        assert client.patch("/executions/missing/status", json={"status": "DIAGNOSING", "note": "n"}).status_code == 404
        assert client.post("/executions/missing/complete", json={"note": "n"}).status_code == 404
    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_messaging_Execution_compensation_and_consumer_thread(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    main.Base.metadata.create_all(engine)
    monkeypatch.setattr(messaging, "MESSAGING_ENABLED", True)
    with sessionmaker(bind=engine, expire_on_commit=False)() as db:
        order = main.Order(
            customer_id="c1",
            vehicle_id="v1",
            items=[{"kind": "PART", "sku": "p1", "name": "P", "quantity": 1, "unit_price": "10.00"}],
            payment_id="pay-1",
            saga_state="COMPLETED",
            status="READY_FOR_SERVICE",
        )
        db.add(order)
        db.commit()
        main.compensate_execution_cancellation(db, order, "Cancelado na oficina")
        db.commit()
        assert order.saga_state == "COMPENSATION_PENDING"
        main.handle_saga_event(db, "StockReleased", {"order_id": order.id})
        main.handle_saga_event(db, "PaymentRefundRequested", {"order_id": order.id})
        assert order.saga_state == "COMPENSATED"
        assert order.status == "CANCELLED"

    calls = []
    monkeypatch.setattr(messaging, "process_batch", lambda url, h: calls.append(url) or 0)
    stop = messaging.start_consumer("http://localhost/q.fifo", lambda t, p: None)
    stop.set()
    observability.configure_logging("os-service").info("coverage_log_check")
    engine.dispose()

