import logging
import os
import time
from contextlib import asynccontextmanager

import asyncpg
from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

__all__ = ["query_router", "lifespan"]

logger = logging.getLogger("inventory_logger")

# ---------------------------------------------------------------------------
# Config & guardrails
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("API_KEY", "8c4f4fbe2a18300fcc13024720e29c7b828bc0e168867c8d5c")
DATABASE_URL = "postgresql://kunal:qamtlDTbBxPoxmZq@skp-inv-mgmt-prod-replica-0.c2i02uy25hsv.ap-south-1.rds.amazonaws.com:5432/prod_skp_inventory_management"

MAX_SERIALS_PER_REQUEST = 500          # Apps Script chunks at 500
STATEMENT_TIMEOUT_MS = 15_000          # no query may run longer than 15s
POOL_MIN, POOL_MAX = 0, 3              # tiny pool; idle connections close
RATE_LIMIT_PER_MINUTE = 60             # global cap across all callers

# Exactly four whitelisted queries. Nothing else can ever run.
QUERIES = {
    "sp_by_primary": """
        SELECT sp.serial_number, sp.secondary_serial_number, sp.transition_status_id,
               sp.holding_facility, sp.receiving_facility, sp.order_number,
               sm.code AS sku_model, sp.quality_status_id, sp.is_refurbished
        FROM public.serialized_product sp
        LEFT JOIN public.sku_model sm ON sp.sku_model_id = sm.id
        WHERE sp.serial_number = ANY($1::text[])
    """,
    "sp_by_secondary": """
        SELECT sp.serial_number, sp.secondary_serial_number, sp.transition_status_id,
               sp.holding_facility, sp.receiving_facility, sp.order_number,
               sm.code AS sku_model, sp.quality_status_id, sp.is_refurbished
        FROM public.serialized_product sp
        LEFT JOIN public.sku_model sm ON sp.sku_model_id = sm.id
        WHERE sp.secondary_serial_number = ANY($1::text[])
    """,
    "map_by_primary": """
        SELECT primary_serial_number, secondary_serial_number, is_consumed
        FROM public.secondary_serial_number_mapping
        WHERE primary_serial_number = ANY($1::text[])
    """,
    "map_by_secondary": """
        SELECT primary_serial_number, secondary_serial_number, is_consumed
        FROM public.secondary_serial_number_mapping
        WHERE secondary_serial_number = ANY($1::text[])
    """,
}

# ---------------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------------

pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    try:
        pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            max_inactive_connection_lifetime=30,  # nothing lingers in pg_stat_activity
            command_timeout=STATEMENT_TIMEOUT_MS / 1000,
            # server_settings={
            #     # "application_name": "inventory-status-checker",
            #     # "statement_timeout": str(STATEMENT_TIMEOUT_MS),
            # },
        )
    except Exception:
        logger.exception("failed to create db pool")
        raise
    logger.info("db pool created")
    yield
    await pool.close()
    logger.info("db pool closed")


# ---------------------------------------------------------------------------
# Simple global rate limit (fixed one-minute window)
# ---------------------------------------------------------------------------

_window_start = 0.0
_window_count = 0


def _rate_limited() -> bool:
    global _window_start, _window_count
    now = time.monotonic()
    if now - _window_start > 60:
        _window_start, _window_count = now, 0
    _window_count += 1
    return _window_count > RATE_LIMIT_PER_MINUTE


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

query_router = APIRouter()


class QueryRequest(BaseModel):
    # camelCase alias matches what Apps Script already sends
    query_id: str = Field(alias="queryId")
    serials: list[str] = Field(min_length=1, max_length=MAX_SERIALS_PER_REQUEST)

    model_config = {"populate_by_name": True}


@query_router.post("/inventory/query")
async def run_query(
    body: QueryRequest,
    request: Request,
    x_api_key: str = Header(default=""),
):
    client = request.client.host if request.client else "-"

    if not API_KEY or x_api_key != API_KEY:
        logger.warning(f"unauthorized query attempt client={client}")
        raise HTTPException(status_code=401, detail="unauthorized")
    if _rate_limited():
        logger.warning(f"rate limited client={client}")
        raise HTTPException(status_code=429, detail="rate limited")

    sql = QUERIES.get(body.query_id)
    if sql is None:
        logger.warning(f"unknown queryId={body.query_id!r} client={client}")
        raise HTTPException(status_code=400, detail="unknown queryId")

    serials = [s.strip() for s in body.serials if s and s.strip()]
    if not serials:
        logger.warning(f"no serials provided queryId={body.query_id} client={client}")
        raise HTTPException(status_code=400, detail="no serials provided")

    try:
        async with pool.acquire() as conn:
            records = await conn.fetch(sql, serials)
    except Exception as e:
        # Never leak connection strings / SQL details to the caller
        logger.exception(
            f"db error queryId={body.query_id} client={client} serial_count={len(serials)} "
            f"error_type={type(e).__name__} error={e}"
        )
        raise HTTPException(status_code=500, detail="db error")

    return {"rows": [dict(r) for r in records]}
