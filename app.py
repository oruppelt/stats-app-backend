from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .for_against import router as for_against_router
from .strength import router as strength_router
from .schedule_strength import router as schedule_strength_router

app = FastAPI()

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
    return {"Hello": "World"}

@app.get("/test")
def read_test():
    return {"message": "Test endpoint"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
