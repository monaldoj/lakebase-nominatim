"""FastAPI application entry point for Nominatim geocoding service."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import router
from src.services.nominatim import nominatim_service
from src.config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Initializes Nominatim service on startup and cleans up on shutdown.
    """
    # Startup
    logger.info("Starting Nominatim API service...")
    try:
        nominatim_service.initialize()
        logger.info("Nominatim service initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Nominatim service: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down Nominatim API service...")
    nominatim_service.close()
    logger.info("Service shutdown complete")


# Create FastAPI application
app = FastAPI(
    title=settings.api_title,
    description=settings.api_description,
    version=settings.api_version,
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router)


@app.get("/ping")
async def ping():
    """Simple ping endpoint for basic health check."""
    return {"message": "pong"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
