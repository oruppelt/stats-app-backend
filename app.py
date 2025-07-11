from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from for_against import router as for_against_router
from strength import router as strength_router
from schedule_strength import router as schedule_strength_router
from logger_config import setup_logging, generate_request_id, set_request_id, get_request_id
import time
from datetime import datetime

# Setup logging
logger = setup_logging("INFO")

app = FastAPI()

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
