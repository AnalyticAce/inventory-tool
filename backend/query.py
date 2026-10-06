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
# Config & guardrails
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("API_KEY", "8c4f4fbe2a18300fcc13024720e29c7b828bc0e168867c8d5c")

# Inventory DB (prod_skp_inventory_management) — existing connection
INVENTORY_DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://kunal:qamtlDTbBxPoxmZq@skp-inv-mgmt-prod-replica-0.c2i02uy25hsv.ap-south-1.rds.amazonaws.com:5432/prod_skp_inventory_management",
)

# User management DB (prod_skp_user_management) — new connection
# Set USER_MGMT_DATABASE_URL in the server environment (same host, different DB,
# or a different host entirely — Shalom to supply the correct DSN).
USER_MGMT_DATABASE_URL = os.environ.get(
    "USER_MGMT_DATABASE_URL",
    "postgresql://kunal:qamtlDTbBxPoxmZq@skp-user-mgmt-prod-replica-0.c2i02uy25hsv.ap-south-1.rds.amazonaws.com:5432/prod_skp_user_management",
)

MAX_SERIALS_PER_REQUEST = 500
STATEMENT_TIMEOUT_MS = 15_000
POOL_MIN, POOL_MAX = 0, 3
RATE_LIMIT_PER_MINUTE = 60

# ---------------------------------------------------------------------------
# Query registry
# Each entry: sql, which pool to use, and a validator that checks the params
# dict and returns the positional args list for asyncpg.
# ---------------------------------------------------------------------------

# ── Original four (inventory DB, serials param) ─────────────────────────────

def _serials_args(params: dict) -> list:
    serials = params.get("serials", [])
    if not serials:
        raise ValueError("serials list is required and must not be empty")
    return [[s.strip() for s in serials if s and s.strip()]]


# ── New CSM Stock Report queries ─────────────────────────────────────────────

def _sku_list_args(params: dict) -> list:
    return []   # no params


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


# Registry: queryId -> (sql, db, arg_builder)
# db is "inventory" or "user_mgmt"
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
        "inventory",
        _serials_args,
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
        "inventory",
        _serials_args,
    ),
    "map_by_primary": (
        """
        SELECT primary_serial_number, secondary_serial_number, is_consumed
        FROM public.secondary_serial_number_mapping
        WHERE primary_serial_number = ANY($1::text[])
        """,
        "inventory",
        _serials_args,
    ),
    "map_by_secondary": (
        """
        SELECT primary_serial_number, secondary_serial_number, is_consumed
        FROM public.secondary_serial_number_mapping
        WHERE secondary_serial_number = ANY($1::text[])
        """,
        "inventory",
        _serials_args,
    ),

    # ── CSM Stock Report — user management DB ────────────────────────────────
    "sku_list": (
        """
        SELECT id   AS sku_model_id,
               code AS sku_model_code,
               name AS sku_model_name
        FROM public.sku_model
        WHERE is_enable = true
        ORDER BY code
        """,
        "inventory",   # sku_model lives in the inventory DB
        _sku_list_args,
    ),
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
        "user_mgmt",
        _location_tree_args,
    ),
    "facility_codes_by_type": (
        """
        SELECT code
        FROM public.facility
        WHERE location_id      = ANY($1::int[])
          AND facility_type_id = $2
        """,
        "user_mgmt",
        _facility_codes_by_type_args,
    ),

    # ── CSM Stock Report — inventory DB ──────────────────────────────────────
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
        "inventory",
        _stock_summary_args,
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
        "inventory",
        _stock_detail_location_args,
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
        "inventory",
        _stock_detail_sku_args,
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
        "inventory",
        _stock_detail_location_multi_args,
    ),
}

# ---------------------------------------------------------------------------
# Pool lifecycle — two pools
# ---------------------------------------------------------------------------

inventory_pool: asyncpg.Pool | None = None
user_mgmt_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global inventory_pool, user_mgmt_pool

    # Inventory pool (always required)
    try:
        inventory_pool = await asyncpg.create_pool(
            dsn=INVENTORY_DATABASE_URL,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            max_inactive_connection_lifetime=30,
            command_timeout=STATEMENT_TIMEOUT_MS / 1000,
        )
        logger.info("inventory db pool created")
    except Exception:
        logger.exception("failed to create inventory db pool")
        raise

    # User management pool (required for location_tree and facility_codes_by_type)
    if USER_MGMT_DATABASE_URL:
        try:
            user_mgmt_pool = await asyncpg.create_pool(
                dsn=USER_MGMT_DATABASE_URL,
                min_size=POOL_MIN,
                max_size=POOL_MAX,
                max_inactive_connection_lifetime=30,
                command_timeout=STATEMENT_TIMEOUT_MS / 1000,
            )
            logger.info("user_mgmt db pool created")
        except Exception:
            logger.exception("failed to create user_mgmt db pool")
            raise
    else:
        logger.warning(
            "USER_MGMT_DATABASE_URL not set — location_tree and facility_codes_by_type "
            "queries will fail at runtime"
        )

    yield

    if inventory_pool:
        await inventory_pool.close()
        logger.info("inventory db pool closed")
    if user_mgmt_pool:
        await user_mgmt_pool.close()
        logger.info("user_mgmt db pool closed")


# ---------------------------------------------------------------------------
# Rate limiter (unchanged)
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
# Request model — backward-compatible
# Old callers send { queryId, serials: [...] }
# New callers send { queryId, params: { ... } }
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    query_id: str = Field(alias="queryId")
    # Original field — kept for the four existing queryIds
    serials: list[str] | None = Field(default=None, max_length=MAX_SERIALS_PER_REQUEST)
    # New field — used by all seven CSM Stock Report queryIds
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

    # Build positional args — merge serials into params for backward compat
    merged_params = dict(body.params or {})
    if body.serials is not None:
        merged_params["serials"] = body.serials

    try:
        args = arg_builder(merged_params)
    except ValueError as e:
        logger.warning(f"bad params queryId={body.query_id} client={client} error={e}")
        raise HTTPException(status_code=400, detail=str(e))

    # Route to correct pool
    if db_name == "user_mgmt":
        if user_mgmt_pool is None:
            logger.error(f"user_mgmt pool not available queryId={body.query_id}")
            raise HTTPException(
                status_code=503,
                detail="user management database not configured",
            )
        active_pool = user_mgmt_pool
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

    return [dict(r) for r in records]
