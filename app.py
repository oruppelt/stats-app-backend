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
import json
from pathlib import Path

# Setup logging
logger = setup_logging("INFO")

app = FastAPI()

DATA_DIR = Path(__file__).parent / "data"

def load_static_data(filename: str):
    """Load data from JSON file"""
    filepath = DATA_DIR / filename
    if filepath.exists():
        with open(filepath, 'r') as f:
            return json.load(f)
    return None

async def background_refresh():
    """Background task to refresh cache from Google Sheets after startup"""
    await asyncio.sleep(5)  # Wait 5 seconds after startup

    logger.info("=" * 80)
    logger.info("BACKGROUND REFRESH: Starting data refresh from Google Sheets...")
    logger.info("=" * 80)

    cache = get_cache()

    try:
        # Refresh strength cache
        logger.info("Refreshing 'strength' cache from Google Sheets...")
        strength_data = await compute_strength_data()
        cache.set("strength", strength_data)
        logger.info(f"✓ 'strength' cache refreshed successfully (teams: {len(strength_data.get('teams', []))})")

        # Refresh schedule_strength cache
        logger.info("Refreshing 'schedule_strength' cache from Google Sheets...")
        schedule_data = await compute_schedule_data()
        cache.set("schedule_strength", schedule_data)
        logger.info(f"✓ 'schedule_strength' cache refreshed successfully (teams: {len(schedule_data.get('teams', []))})")

        logger.info("=" * 80)
        logger.info("BACKGROUND REFRESH: Complete! Cache updated with fresh data.")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"BACKGROUND REFRESH FAILED: {str(e)}")
        logger.error("Static data will continue to be served until next refresh")
        logger.error("=" * 80)

@app.on_event("startup")
async def startup_event():
    """Load static data on startup, then refresh in background"""
    logger.info("=" * 80)
    logger.info("STARTUP: Loading static data from JSON files...")
    logger.info("=" * 80)

    cache = get_cache()

    try:
        # Load strength data from JSON
        logger.info("Loading 'strength' data from JSON...")
        strength_data = load_static_data("strength.json")
        if strength_data:
            cache.set("strength", strength_data)
            logger.info(f"✓ 'strength' data loaded successfully (teams: {len(strength_data.get('teams', []))})")
        else:
            logger.warning("⚠ No static 'strength' data found, will fetch from Google Sheets")

        # Load schedule_strength data from JSON
        logger.info("Loading 'schedule_strength' data from JSON...")
        schedule_data = load_static_data("schedule_strength.json")
        if schedule_data:
            cache.set("schedule_strength", schedule_data)
            logger.info(f"✓ 'schedule_strength' data loaded successfully (teams: {len(schedule_data.get('teams', []))})")
        else:
            logger.warning("⚠ No static 'schedule_strength' data found, will fetch from Google Sheets")

        # Note: for_against uses the same data as strength
        logger.info("✓ 'for_against' will use the same cached data as 'strength'")

        logger.info("=" * 80)
        logger.info("STARTUP: Complete! All endpoints ready with static data.")
        logger.info("Background refresh will start in 5 seconds...")
        logger.info("=" * 80)

        # Start background refresh task
        asyncio.create_task(background_refresh())

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"STARTUP FAILED: {str(e)}")
        logger.error("App will continue but may need to fetch from Google Sheets")
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
