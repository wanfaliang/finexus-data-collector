"""
BEA Sentinel API Endpoints

API endpoints for managing sentinel series used to detect new BEA data releases.

Author: FinExus Data Collector
Created: 2025-11-28
"""
from typing import Optional, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.bea.bea_client import BEAClient
from src.bea.bea_collector import SentinelManager
from src.database.connection import get_session
from src.config import settings

router = APIRouter()


# ==================== Request/Response Models ==================== #

class SelectSentinelsRequest(BaseModel):
    """Request to select sentinels for a dataset"""
    dataset: str = Field(..., description="Dataset name: NIPA, Regional, or GDPbyIndustry")
    frequency: str = Field("A", description="Frequency for NIPA/GDPbyIndustry: A, Q, M")


class CheckSentinelsRequest(BaseModel):
    """Request to check sentinels for new data"""
    dataset: str = Field(..., description="Dataset name: NIPA, Regional, or GDPbyIndustry")


class DeleteSentinelRequest(BaseModel):
    """Request to delete a specific sentinel"""
    dataset: str = Field(..., description="Dataset name")
    sentinel_id: str = Field(..., description="Sentinel ID to delete")


class SentinelResponse(BaseModel):
    """Generic response for sentinel operations"""
    success: bool
    message: str
    data: Optional[dict] = None


class SentinelStatsResponse(BaseModel):
    """Response for sentinel statistics"""
    total: int
    by_dataset: dict


class SentinelListResponse(BaseModel):
    """Response for listing sentinels"""
    dataset: str
    count: int
    sentinels: List[dict]


# ==================== Helper ==================== #

def get_sentinel_manager() -> SentinelManager:
    """Create a SentinelManager instance."""
    api_key = settings.api.bea_api_key
    if not api_key or len(api_key) != 36:
        raise HTTPException(status_code=500, detail="Invalid or missing BEA_API_KEY")

    client = BEAClient(api_key=api_key)
    session = get_session().__enter__()  # Get session from context manager
    return SentinelManager(client, session)


# ==================== Endpoints ==================== #

@router.get("/sentinel/stats", response_model=SentinelStatsResponse)
async def get_sentinel_stats():
    """
    Get sentinel statistics for all datasets.

    Returns count of sentinels, last check time, and change statistics.
    """
    with get_session() as session:
        api_key = settings.api.bea_api_key
        if not api_key:
            raise HTTPException(status_code=500, detail="BEA_API_KEY not configured")

        client = BEAClient(api_key=api_key)
        manager = SentinelManager(client, session)
        stats = manager.get_sentinel_stats()

        # Convert datetime objects to ISO strings for JSON serialization
        for dataset_stats in stats.get('by_dataset', {}).values():
            if dataset_stats.get('last_checked'):
                dataset_stats['last_checked'] = dataset_stats['last_checked'].isoformat()
            if dataset_stats.get('last_changed'):
                dataset_stats['last_changed'] = dataset_stats['last_changed'].isoformat()

        return SentinelStatsResponse(
            total=stats['total'],
            by_dataset=stats['by_dataset']
        )


@router.get("/sentinel/list/{dataset}", response_model=SentinelListResponse)
async def list_sentinels(dataset: str):
    """
    List all sentinels for a specific dataset.

    - **dataset**: NIPA, Regional, or GDPbyIndustry
    """
    if dataset not in ("NIPA", "Regional", "GDPbyIndustry"):
        raise HTTPException(status_code=400, detail="dataset must be NIPA, Regional, or GDPbyIndustry")

    with get_session() as session:
        api_key = settings.api.bea_api_key
        if not api_key:
            raise HTTPException(status_code=500, detail="BEA_API_KEY not configured")

        client = BEAClient(api_key=api_key)
        manager = SentinelManager(client, session)
        sentinels = manager.list_sentinels(dataset)

        return SentinelListResponse(
            dataset=dataset,
            count=len(sentinels),
            sentinels=sentinels
        )


@router.post("/sentinel/select", response_model=SentinelResponse)
async def select_sentinels(request: SelectSentinelsRequest):
    """
    Automatically select sentinel series for a dataset.

    Selects ~5-10% of series as sentinels based on:
    - Priority tables (GDP, Income headlines)
    - Even distribution across categories
    - Key aggregates (line 1, totals)

    This clears existing sentinels for the dataset/frequency and creates new ones.
    """
    if request.dataset not in ("NIPA", "Regional", "GDPbyIndustry"):
        raise HTTPException(status_code=400, detail="dataset must be NIPA, Regional, or GDPbyIndustry")

    if request.frequency not in ("A", "Q", "M"):
        raise HTTPException(status_code=400, detail="frequency must be A, Q, or M")

    with get_session() as session:
        api_key = settings.api.bea_api_key
        if not api_key:
            raise HTTPException(status_code=500, detail="BEA_API_KEY not configured")

        client = BEAClient(api_key=api_key)
        manager = SentinelManager(client, session)

        try:
            result = manager.select_sentinels(request.dataset, request.frequency)
            return SentinelResponse(
                success=True,
                message=result['message'],
                data=result
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/sentinel/check", response_model=SentinelResponse)
async def check_sentinels(request: CheckSentinelsRequest):
    """
    Check sentinel series for new data.

    Fetches current values from BEA API and compares against stored values.
    If changes are detected, updates the dataset's `needs_update` flag.

    This makes API calls to BEA, so use sparingly (respects rate limits).
    """
    if request.dataset not in ("NIPA", "Regional", "GDPbyIndustry"):
        raise HTTPException(status_code=400, detail="dataset must be NIPA, Regional, or GDPbyIndustry")

    with get_session() as session:
        api_key = settings.api.bea_api_key
        if not api_key:
            raise HTTPException(status_code=500, detail="BEA_API_KEY not configured")

        client = BEAClient(api_key=api_key)
        manager = SentinelManager(client, session)

        try:
            result = manager.check_sentinels(request.dataset)
            return SentinelResponse(
                success=True,
                message=result['message'],
                data=result
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/sentinel/check-all", response_model=SentinelResponse)
async def check_all_sentinels():
    """
    Check sentinels for all datasets.

    Checks NIPA, Regional, and GDPbyIndustry sentinels in sequence.
    """
    with get_session() as session:
        api_key = settings.api.bea_api_key
        if not api_key:
            raise HTTPException(status_code=500, detail="BEA_API_KEY not configured")

        client = BEAClient(api_key=api_key)
        manager = SentinelManager(client, session)

        results = {}
        total_checked = 0
        total_changed = 0

        for dataset in ["NIPA", "Regional", "GDPbyIndustry"]:
            try:
                result = manager.check_sentinels(dataset)
                results[dataset] = result
                total_checked += result['checked']
                total_changed += result['changed']
            except Exception as e:
                results[dataset] = {'error': str(e)}

        return SentinelResponse(
            success=True,
            message=f"Checked {total_checked} sentinels across all datasets, {total_changed} have new data",
            data={
                'total_checked': total_checked,
                'total_changed': total_changed,
                'new_data_detected': total_changed > 0,
                'by_dataset': results
            }
        )


@router.delete("/sentinel/{dataset}/{sentinel_id}", response_model=SentinelResponse)
async def delete_sentinel(dataset: str, sentinel_id: str):
    """
    Delete a specific sentinel.

    - **dataset**: NIPA, Regional, or GDPbyIndustry
    - **sentinel_id**: The sentinel ID to delete
    """
    if dataset not in ("NIPA", "Regional", "GDPbyIndustry"):
        raise HTTPException(status_code=400, detail="dataset must be NIPA, Regional, or GDPbyIndustry")

    with get_session() as session:
        api_key = settings.api.bea_api_key
        if not api_key:
            raise HTTPException(status_code=500, detail="BEA_API_KEY not configured")

        client = BEAClient(api_key=api_key)
        manager = SentinelManager(client, session)

        deleted = manager.delete_sentinel(dataset, sentinel_id)

        if deleted:
            return SentinelResponse(
                success=True,
                message=f"Deleted sentinel {sentinel_id} from {dataset}"
            )
        else:
            raise HTTPException(status_code=404, detail=f"Sentinel {sentinel_id} not found in {dataset}")


@router.delete("/sentinel/{dataset}", response_model=SentinelResponse)
async def clear_sentinels(dataset: str):
    """
    Clear all sentinels for a dataset.

    - **dataset**: NIPA, Regional, or GDPbyIndustry
    """
    if dataset not in ("NIPA", "Regional", "GDPbyIndustry"):
        raise HTTPException(status_code=400, detail="dataset must be NIPA, Regional, or GDPbyIndustry")

    with get_session() as session:
        api_key = settings.api.bea_api_key
        if not api_key:
            raise HTTPException(status_code=500, detail="BEA_API_KEY not configured")

        client = BEAClient(api_key=api_key)
        manager = SentinelManager(client, session)

        count = manager.clear_sentinels(dataset)

        return SentinelResponse(
            success=True,
            message=f"Cleared {count} sentinels from {dataset}"
        )
