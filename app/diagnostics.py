"""Persistência NoSQL (Amazon DynamoDB) para laudos e diagnósticos técnicos.

Diagnósticos veiculares possuem estrutura flexível (códigos de falha OBD-II,
leituras de sensores, checklist variável por montadora/modelo e observações),
adequada para modelo orientado a documentos no DynamoDB com chave de partição
``order_id`` e chave de ordenação ``diagnostic_id`` (timestamp ISO + sufixo).
"""

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Key

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMODB_ENDPOINT_URL = os.getenv("DYNAMODB_ENDPOINT_URL") or None
DIAGNOSTICS_TABLE_NAME = os.getenv("DIAGNOSTICS_TABLE_NAME", "oficina-os-diagnostics")

_resource = None


def get_table():
    global _resource
    if _resource is None:
        _resource = boto3.resource("dynamodb", region_name=AWS_REGION, endpoint_url=DYNAMODB_ENDPOINT_URL)
    return _resource.Table(DIAGNOSTICS_TABLE_NAME)


def _floats_to_decimals(obj: Any) -> Any:
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _floats_to_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_floats_to_decimals(v) for v in obj]
    return obj


def _decimals_to_numbers(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj) if "." in str(obj) else int(obj)
    if isinstance(obj, dict):
        return {k: _decimals_to_numbers(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimals_to_numbers(v) for v in obj]
    return obj


def save_diagnostic(
    order_id: str,
    mechanic: str,
    summary: str,
    obd_codes: list[str] | None = None,
    checklist: dict[str, Any] | None = None,
    measurements: dict[str, Any] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    item: dict[str, Any] = {
        "order_id": order_id,
        "diagnostic_id": f"{now}#{uuid4().hex[:8]}",
        "created_at": now,
        "mechanic": mechanic,
        "summary": summary,
        "obd_codes": obd_codes or [],
        "checklist": checklist or {},
        "measurements": measurements or {},
    }
    if notes:
        item["notes"] = notes
    item_for_db = _floats_to_decimals(item)
    get_table().put_item(Item=item_for_db)
    return _decimals_to_numbers(item_for_db)


def list_diagnostics(order_id: str) -> list[dict[str, Any]]:
    response = get_table().query(
        KeyConditionExpression=Key("order_id").eq(order_id),
        ScanIndexForward=True,
    )
    return [_decimals_to_numbers(item) for item in response.get("Items", [])]

