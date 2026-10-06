import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

__all__ = ["query_router", "lifespan"]

logger = logging.getLogger("inventory_logger")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("API_KEY", "8c4f4fbe2a18300fcc13024720e29c7b828bc0e168867c8d5c")

INVENTORY_DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://kunal:qamtlDTbBxPoxmZq@skp-inv-mgmt-prod-replica-0.c2i02uy25hsv.ap-south-1.rds.amazonaws.com:5432/prod_skp_inventory_management",
)

USER_MGMT_DATABASE_URL = os.environ.get(
    "USER_MGMT_DATABASE_URL",
    "postgresql://kunal:qamtlDTbBxPoxmZq@skp-user-mgmt-prod-replica-0.c2i02uy25hsv.ap-south-1.rds.amazonaws.com:5432/prod_skp_user_management",
)

# Loan DB — hosts order_detail and customer_address
# Set LOAN_DATABASE_URL in the server environment
LOAN_DATABASE_URL = os.environ.get("LOAN_DATABASE_URL", "")

MAX_SERIALS_PER_REQUEST = 500
STATEMENT_TIMEOUT_MS = 60_000
POOL_MIN, POOL_MAX = 0, 3
RATE_LIMIT_PER_MINUTE = 60

# ---------------------------------------------------------------------------
# Arg builders
# ---------------------------------------------------------------------------

def _serials_args(params: dict) -> list:
    serials = params.get("serials", [])
    if not serials:
        raise ValueError("serials list is required and must not be empty")
    return [[s.strip() for s in serials if s and s.strip()]]

def _sku_list_args(params: dict) -> list:
    return []

def _location_tree_args(params: dict) -> list:
    cid = params.get("country_id")
    if cid is None:
        raise ValueError("country_id is required")
    return [int(cid)]

def _facility_codes_by_type_args(params: dict) -> list:
    loc_ids = params.get("location_ids")
    ftype = params.get("facility_type_id")
    if not loc_ids:
        raise ValueError("location_ids is required and must not be empty")
    if ftype is None:
        raise ValueError("facility_type_id is required")
    return [[int(i) for i in loc_ids], int(ftype)]

def _stock_summary_args(params: dict) -> list:
    codes = params.get("location_codes")
    if not codes:
        raise ValueError("location_codes is required and must not be empty")
    return [list(codes)]

def _stock_detail_location_args(params: dict) -> list:
    code = params.get("location_code")
    if not code:
        raise ValueError("location_code is required")
    return [str(code)]

def _stock_detail_sku_args(params: dict) -> list:
    codes = params.get("location_codes")
    if not codes:
        raise ValueError("location_codes is required and must not be empty")
    return [list(codes)]

def _stock_detail_location_multi_args(params: dict) -> list:
    codes = params.get("facility_codes")
    if not codes:
        raise ValueError("facility_codes is required and must not be empty")
    return [list(codes)]

def _sold_orders_args(params: dict) -> list:
    country_code = params.get("country_code")
    date_from = params.get("date_from")
    date_to = params.get("date_to")
    if not country_code:
        raise ValueError("country_code is required")
    if not date_from or not date_to:
        raise ValueError("date_from and date_to are required")
    return [str(country_code), str(date_from), str(date_to)]

def _sold_summary_args(params: dict) -> list:
    order_numbers = params.get("order_numbers")
    if not order_numbers:
        raise ValueError("order_numbers is required and must not be empty")
    return [list(order_numbers)]

# ---------------------------------------------------------------------------
# Query registry
# ---------------------------------------------------------------------------

QUERIES: dict[str, tuple[str, str, Any]] = {

    # ── Original four ────────────────────────────────────────────────────────
    "sp_by_primary": (
        """
        SELECT sp.serial_number, sp.secondary_serial_number, sp.transition_status_id,
               sp.holding_facility, sp.receiving_facility, sp.order_number,
               sm.code AS sku_model, sp.quality_status_id, sp.is_refurbished
        FROM public.serialized_product sp
        LEFT JOIN public.sku_model sm ON sp.sku_model_id = sm.id
        WHERE sp.serial_number = ANY($1::text[])
        """,
        "inventory", _serials_args,
    ),
    "sp_by_secondary": (
        """
        SELECT sp.serial_number, sp.secondary_serial_number, sp.transition_status_id,
               sp.holding_facility, sp.receiving_facility, sp.order_number,
               sm.code AS sku_model, sp.quality_status_id, sp.is_refurbished
        FROM public.serialized_product sp
        LEFT JOIN public.sku_model sm ON sp.sku_model_id = sm.id
        WHERE sp.secondary_serial_number = ANY($1::text[])
        """,
        "inventory", _serials_args,
    ),
    "map_by_primary": (
        """
        SELECT primary_serial_number, secondary_serial_number, is_consumed
        FROM public.secondary_serial_number_mapping
        WHERE primary_serial_number = ANY($1::text[])
        """,
        "inventory", _serials_args,
    ),
    "map_by_secondary": (
        """
        SELECT primary_serial_number, secondary_serial_number, is_consumed
        FROM public.secondary_serial_number_mapping
        WHERE secondary_serial_number = ANY($1::text[])
        """,
        "inventory", _serials_args,
    ),

    # ── CSM Stock Report — inventory DB ──────────────────────────────────────
    "sku_list": (
        """
        SELECT id   AS sku_model_id,
               code AS sku_model_code,
               name AS sku_model_name
        FROM public.sku_model
        WHERE is_enable = true
        ORDER BY code
        """,
        "inventory", _sku_list_args,
    ),
    "stock_summary": (
        """
        SELECT holding_facility_location_code,
               holding_facility               AS holding_facility_code,
               holding_facility_name,
               sku_model_id,
               transition_status_id,
               COUNT(*)                       AS count
        FROM public.serialized_product
        WHERE holding_facility_location_code = ANY($1::text[])
          AND transition_status_id           IN (2, 3, 4, 6, 7)
        GROUP BY holding_facility_location_code,
                 holding_facility,
                 holding_facility_name,
                 sku_model_id,
                 transition_status_id
        """,
        "inventory", _stock_summary_args,
    ),
    "stock_detail_location": (
        """
        SELECT serial_number,
               secondary_serial_number,
               holding_facility_location_code,
               holding_facility               AS holding_facility_code,
               holding_facility_name,
               sku_model_id,
               transition_status_id,
               held_since
        FROM public.serialized_product
        WHERE holding_facility_location_code = $1
          AND transition_status_id           IN (2, 3, 4, 6, 7)
        """,
        "inventory", _stock_detail_location_args,
    ),
    "stock_detail_sku": (
        """
        SELECT serial_number,
               secondary_serial_number,
               holding_facility_location_code,
               holding_facility               AS holding_facility_code,
               holding_facility_name,
               sku_model_id,
               transition_status_id,
               held_since
        FROM public.serialized_product
        WHERE holding_facility_location_code = ANY($1::text[])
          AND transition_status_id           IN (2, 3, 4, 6, 7)
        """,
        "inventory", _stock_detail_sku_args,
    ),
    "stock_detail_location_multi": (
        """
        SELECT serial_number,
               secondary_serial_number,
               holding_facility_location_code,
               holding_facility               AS holding_facility_code,
               holding_facility_name,
               sku_model_id,
               transition_status_id,
               held_since
        FROM public.serialized_product
        WHERE holding_facility = ANY($1::text[])
          AND transition_status_id IN (2, 3, 4, 6, 7)
        """,
        "inventory", _stock_detail_location_multi_args,
    ),

    # ── CSM Stock Report — user management DB ────────────────────────────────
    "location_tree": (
        """
        WITH RECURSIVE location_tree AS (
            SELECT id, name, code, parent_location_id, location_type_id
            FROM location
            WHERE id = $1
            UNION ALL
            SELECT l.id, l.name, l.code, l.parent_location_id, l.location_type_id
            FROM location l
            JOIN location_tree lt ON l.parent_location_id = lt.id
        )
        SELECT id, name, code, location_type_id
        FROM location_tree
        ORDER BY location_type_id, name
        """,
        "user_mgmt", _location_tree_args,
    ),
    "facility_codes_by_type": (
        """
        SELECT code
        FROM public.facility
        WHERE location_id      = ANY($1::int[])
          AND facility_type_id = $2
        """,
        "user_mgmt", _facility_codes_by_type_args,
    ),

    # ── CSM Stock Report — loan DB ────────────────────────────────────────────
    "sold_orders": (
        """
        SELECT a.order_number, a.down_payment_date AS sold_date, b.area_code
        FROM order_detail a
        JOIN customer_address b ON a.customer_id = b.customer_id
        WHERE a.country_code = $1
          AND a.down_payment_date >= $2::timestamp
          AND a.down_payment_date <  $3::timestamp
        """,
        "loan", _sold_orders_args,
    ),

    # ── CSM Stock Report — inventory DB (sold summary) ────────────────────────
    "sold_summary": (
        """
        SELECT
            a.holding_facility_location_name,
            b.code  AS sku_code,
            d.name  AS sku_family,
            COUNT(*) AS count
        FROM public.serialized_product a
        JOIN public.sku_model b ON b.id = a.sku_model_id
        JOIN public.sku d       ON d.id = b.sku_id
        WHERE a.order_number = ANY($1::text[])
          AND a.transition_status_id = 5
        GROUP BY
            a.holding_facility_location_name,
            b.code,
            d.name
        ORDER BY
            a.holding_facility_location_name,
            b.code
        """,
        "inventory", _sold_summary_args,
    ),
}

# ---------------------------------------------------------------------------
# Pool lifecycle — three pools
# ---------------------------------------------------------------------------

inventory_pool: asyncpg.Pool | None = None
user_mgmt_pool: asyncpg.Pool | None = None
loan_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global inventory_pool, user_mgmt_pool, loan_pool

    try:
        inventory_pool = await asyncpg.create_pool(
            dsn=INVENTORY_DATABASE_URL,
            min_size=POOL_MIN, max_size=POOL_MAX,
            max_inactive_connection_lifetime=30,
            command_timeout=STATEMENT_TIMEOUT_MS / 1000,
        )
        logger.info("inventory db pool created")
    except Exception:
        logger.exception("failed to create inventory db pool")
        raise

    if USER_MGMT_DATABASE_URL:
        try:
            user_mgmt_pool = await asyncpg.create_pool(
                dsn=USER_MGMT_DATABASE_URL,
                min_size=POOL_MIN, max_size=POOL_MAX,
                max_inactive_connection_lifetime=30,
                command_timeout=STATEMENT_TIMEOUT_MS / 1000,
            )
            logger.info("user_mgmt db pool created")
        except Exception:
            logger.exception("failed to create user_mgmt db pool")
            raise
    else:
        logger.warning("USER_MGMT_DATABASE_URL not set")

    if LOAN_DATABASE_URL:
        try:
            loan_pool = await asyncpg.create_pool(
                dsn=LOAN_DATABASE_URL,
                min_size=POOL_MIN, max_size=POOL_MAX,
                max_inactive_connection_lifetime=30,
                command_timeout=STATEMENT_TIMEOUT_MS / 1000,
            )
            logger.info("loan db pool created")
        except Exception:
            logger.exception("failed to create loan db pool")
            raise
    else:
        logger.warning("LOAN_DATABASE_URL not set — sold_orders queries will fail at runtime")

    yield

    for p, name in [(inventory_pool, "inventory"), (user_mgmt_pool, "user_mgmt"), (loan_pool, "loan")]:
        if p:
            await p.close()
            logger.info(f"{name} db pool closed")


# ---------------------------------------------------------------------------
# Rate limiter
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
# Request model
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    query_id: str = Field(alias="queryId")
    serials: list[str] | None = Field(default=None, max_length=MAX_SERIALS_PER_REQUEST)
    params: dict[str, Any] | None = Field(default=None)
    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

query_router = APIRouter()


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

    entry = QUERIES.get(body.query_id)
    if entry is None:
        logger.warning(f"unknown queryId={body.query_id!r} client={client}")
        raise HTTPException(status_code=400, detail="unknown queryId")

    sql, db_name, arg_builder = entry

    merged_params = dict(body.params or {})
    if body.serials is not None:
        merged_params["serials"] = body.serials

    try:
        args = arg_builder(merged_params)
    except ValueError as e:
        logger.warning(f"bad params queryId={body.query_id} client={client} error={e}")
        raise HTTPException(status_code=400, detail=str(e))

    if db_name == "user_mgmt":
        if user_mgmt_pool is None:
            raise HTTPException(status_code=503, detail="user management database not configured")
        active_pool = user_mgmt_pool
    elif db_name == "loan":
        if loan_pool is None:
            raise HTTPException(status_code=503, detail="loan database not configured — set LOAN_DATABASE_URL on the server")
        active_pool = loan_pool
    else:
        active_pool = inventory_pool

    try:
        async with active_pool.acquire() as conn:
            if args:
                records = await conn.fetch(sql, *args)
            else:
                records = await conn.fetch(sql)
    except Exception as e:
        logger.exception(
            f"db error queryId={body.query_id} client={client} "
            f"error_type={type(e).__name__} error={e}"
        )
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

    # Original four queryIds keep the legacy {"rows": [...]} shape
    if body.query_id in ("sp_by_primary", "sp_by_secondary", "map_by_primary", "map_by_secondary"):
        return {"rows": [dict(r) for r in records]}

    return [dict(r) for r in records]
