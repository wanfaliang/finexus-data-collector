"""API v1 Router"""
from fastapi import APIRouter

from src.admin.api.v1 import freshness, quota, actions, cu_explorer, la_explorer, ce_explorer

# Create main API router
api_router = APIRouter()

# Include sub-routers
api_router.include_router(
    freshness.router,
    prefix="/freshness",
    tags=["freshness"],
)

api_router.include_router(
    quota.router,
    prefix="/quota",
    tags=["quota"],
)

api_router.include_router(
    actions.router,
    prefix="/actions",
    tags=["actions"],
)

# Survey Explorer routers
api_router.include_router(
    cu_explorer.router,
    prefix="/explorer",
    tags=["explorer"],
)

api_router.include_router(
    la_explorer.router,
    prefix="/explorer",
    tags=["explorer"],
)

api_router.include_router(
    ce_explorer.router,
    prefix="/explorer",
    tags=["explorer"],
)
