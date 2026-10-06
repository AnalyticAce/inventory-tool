from fastapi import FastAPI, HTTPException, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from typing import Any, Dict
import logging
import logging.handlers
import os
import time
from query import query_router, lifespan

# Logging setup
logger = logging.getLogger("inventory_logger")
logger.setLevel(logging.INFO)

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")

_LOG_DIR = os.getenv("LOG_DIR", "/tmp/inventory-logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(_LOG_DIR, "app.log"), maxBytes=10 * 1024 * 1024, backupCount=5
)
_file_handler.setFormatter(_fmt)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_fmt)
logger.addHandler(_file_handler)
logger.addHandler(_console_handler)

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(title="Inventory", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.include_router(query_router)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    client = request.client.host if request.client else "-"
    try:
        response = await call_next(request)
    except Exception:
        ms = (time.perf_counter() - start) * 1000
        logger.exception(
            f'method={request.method} path={request.url.path} '
            f'client={client} status=500 duration={ms:.1f}ms unhandled exception'
        )
        raise
    ms = (time.perf_counter() - start) * 1000
    logger.info(
        f'method={request.method} path={request.url.path} '
        f'client={client} status={response.status_code} duration={ms:.1f}ms'
    )
    return response



@app.get("/health")
async def health_check():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8020)
