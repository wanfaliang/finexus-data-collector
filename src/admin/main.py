"""
FastAPI Admin Application

Main entry point for the FinExus Data Collector Admin UI backend.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from src.admin import __version__
from src.admin.api.v1 import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    print("Admin API starting up...")
    yield
    # Shutdown
    print("Admin API shutting down...")


# Create FastAPI app
app = FastAPI(
    title="FinExus Data Collector Admin API",
    description="Admin interface for managing data collection from multiple sources",
    version=__version__,
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3001",  # React dev server (port 3001)
        "http://localhost:5173",  # Vite dev server
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "FinExus Data Collector Admin API",
        "version": __version__,
        "docs": "/api/docs",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": __version__}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.admin.main:app",
        host="0.0.0.0",
        port=8001,  # Changed from 8000 to avoid conflict
        reload=True,
        log_level="info",
    )
