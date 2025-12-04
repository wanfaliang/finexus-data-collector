"""API v1 Router"""
from fastapi import APIRouter

from src.admin.api.v1 import freshness, quota, actions, cu_explorer, la_explorer, ce_explorer, ln_explorer
from src.admin.api.v1 import bea_dashboard, bea_explorer, bea_actions, bea_sentinel
from src.admin.api.v1 import treasury_dashboard, treasury_actions, treasury_explorer

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

api_router.include_router(
    ln_explorer.router,
    prefix="/explorer",
    tags=["explorer"],
)

# BEA routers
api_router.include_router(
    bea_dashboard.router,
    prefix="/bea",
    tags=["bea"],
)

api_router.include_router(
    bea_explorer.router,
    prefix="/bea/explorer",
    tags=["bea-explorer"],
)

api_router.include_router(
    bea_actions.router,
    prefix="/bea",
    tags=["bea-actions"],
)

api_router.include_router(
    bea_sentinel.router,
    prefix="/bea",
    tags=["bea-sentinel"],
)

# Treasury routers - actions MUST come first to avoid route masking
# (e.g., /backfill/auctions being matched by /auctions/{security_term}/history)
api_router.include_router(
    treasury_actions.router,
    prefix="/treasury",
    tags=["treasury-actions"],
)

api_router.include_router(
    treasury_dashboard.router,
    prefix="/treasury",
    tags=["treasury"],
)

api_router.include_router(
    treasury_explorer.router,
    prefix="/treasury/explorer",
    tags=["treasury-explorer"],
)
