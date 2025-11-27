from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from for_against import router as for_against_router
from strength import router as strength_router, _compute_strength as compute_strength_data
from schedule_strength import router as schedule_strength_router, _compute_schedule_strength as compute_schedule_data
from logger_config import setup_logging, generate_request_id, set_request_id, get_request_id
from cache import get_cache
import time
from datetime import datetime
import asyncio

# Setup logging
logger = setup_logging("INFO")

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """Pre-warm the cache on startup to avoid 429 errors from Google Sheets"""
    logger.info("=" * 80)
    logger.info("CACHE WARMUP: Starting cache pre-population...")
    logger.info("=" * 80)

    cache = get_cache()

    try:
        # Warm up strength cache
        logger.info("Warming up 'strength' cache...")
        strength_data = await cache.get_or_compute("strength", compute_strength_data)
        logger.info(f"✓ 'strength' cache warmed up successfully (teams: {len(strength_data.get('teams', []))})")

        # Warm up schedule_strength cache
        logger.info("Warming up 'schedule_strength' cache...")
        schedule_data = await cache.get_or_compute("schedule_strength", compute_schedule_data)
        logger.info(f"✓ 'schedule_strength' cache warmed up successfully (teams: {len(schedule_data.get('teams', []))})")

        # Note: for_against uses the same data as strength, so it's already cached
        logger.info("✓ 'for_against' will use the same cached data as 'strength'")

        logger.info("=" * 80)
        logger.info("CACHE WARMUP: Complete! All endpoints ready to serve cached data.")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"CACHE WARMUP FAILED: {str(e)}")
        logger.error("App will continue but may experience 429 errors on first requests")
        logger.error("=" * 80)

@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Middleware to add request ID and timing to all requests"""
    # Generate and set request ID
    request_id = generate_request_id()
    set_request_id(request_id)
    
    # Log request start
    start_time = datetime.now()
    logger.info(f"Request started: {request.method} {request.url.path}")
    
    # Process request
    response = await call_next(request)
    
    # Log request completion with timing
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Request completed: {request.method} {request.url.path} - "
               f"Status: {response.status_code} - Duration: {duration:.3f}s")
    
    # Add request ID to response headers for debugging
    response.headers["X-Request-ID"] = request_id
    
    return response

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the routers without prefix
app.include_router(for_against_router)
app.include_router(strength_router)
app.include_router(schedule_strength_router)

@app.get("/")
def read_root():
    logger.info("Root endpoint accessed")
    return {"Hello": "World"}

@app.get("/test")
def read_test():
    logger.info("Test endpoint accessed")
    return {"message": "Test endpoint"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
