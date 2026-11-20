"""One JSON line per finished stream on the `app.muxws.exchanges` logger, built on `peer.on_exchange`."""

import json
import logging

from collections.abc import Callable
from typing import Any

from muxws import Exchange, Peer

exchanges_logger = logging.getLogger("app.muxws.exchanges")

#: Longest payload written, in UTF-8 bytes of its JSON text; a longer one is written trimmed, as a string.
TRIM_PAYLOAD_SIZE = 4096


def without_secrets(record: dict[str, Any]) -> dict[str, Any]:
    """Mask the `password` payload key and the `cookie` header on both sides of `record`."""
    for side in (record["request"], record["response"]):
        if side["headers"] and "cookie" in side["headers"]:
            side["headers"]["cookie"] = "***"
        side["payloads"] = [_masked(payload) for payload in side["payloads"]]
    return record


def _masked(payload: Any) -> Any:
    if isinstance(payload, dict) and "password" in payload:
        return {**payload, "password": "***"}
    # A payload over TRIM_PAYLOAD_SIZE arrives as its trimmed JSON text.
    if isinstance(payload, str) and '"password"' in payload:
        return "***"
    return payload


def log_exchanges(
    peer: Peer, redact: Callable[[dict[str, Any]], dict[str, Any]] | None = without_secrets
) -> Callable[[Exchange], None]:
    """Write every finished stream of `peer` to `app.muxws.exchanges` at `INFO`, `redact` applied first."""

    def write(exchange: Exchange) -> None:
        record = exchange.as_dict(trim_payload_size=TRIM_PAYLOAD_SIZE)
        if redact is not None:
            record = redact(record)
        exchanges_logger.info(json.dumps(record, separators=(",", ":")))

    return peer.on_exchange(write)
