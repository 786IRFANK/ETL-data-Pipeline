"""
FastAPI Application Entry Point
"""
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import pipeline
from app.api.routes.adhoc_pipeline import router as adhoc_router
from app.api.routes.adhoc_user import router as adhoc_user
from app.api.routes.connector_services import router as connector_router
from app.config.settings import settings

def create_app() -> FastAPI:
    """
    Application factory
    """
    app = FastAPI(
        title=settings.APP_NAME,
        description=settings.APP_DESCRIPTION,
        version=settings.APP_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    if settings.CORS_ORIGINS:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.CORS_ORIGINS,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type"],
        )

    app.include_router(
        pipeline.router,
        prefix="/api/v1/pipeline",
        tags=["Pipeline"],
    )

    app.include_router(
        adhoc_router,
        prefix="/api/v1/adhoc",
        tags=["Adhoc Upload Pipeline"],
    )

    app.include_router(
        connector_router,
        prefix="/api/v1/connector",
        tags=["Connector Service"],
    )

    app.include_router(
        adhoc_user,
        prefix="/api/v1/adhoc-user",
        tags=["Adhoc User"],
    )
    

    return app


app = create_app()

@app.get("/", tags=["Health"])
async def root():
    return {
        "message": "Axonal Data Pipeline API",
        "version": settings.APP_VERSION,
        "status": "running",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
    )
