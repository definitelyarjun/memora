"""FoundationIQ — FastAPI application entry point."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.routers.ingestion import router as tabular_router
from app.routers.quality import router as quality_router
from app.routers.benchmark import router as benchmark_router
from app.routers.automation import router as automation_router
from app.routers.financial import router as financial_router
from app.routers.retention import router as retention_router
from app.routers.verdict import router as verdict_router

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="AI Readiness & Automation Diagnostic Platform for Startups",
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(tabular_router)
app.include_router(quality_router)
app.include_router(benchmark_router)
app.include_router(automation_router)
app.include_router(financial_router)
app.include_router(retention_router)
app.include_router(verdict_router)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health", tags=["Health"])
async def health() -> dict:
    return {"status": "ok", "app": settings.app_name, "version": settings.app_version}
