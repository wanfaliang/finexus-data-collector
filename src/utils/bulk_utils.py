"""
Bulk Data Utilities
Helper functions for managing bulk CSV files
"""
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from src.config import settings

logger = logging.getLogger(__name__)


def get_bulk_data_path() -> Path:
    """Get the bulk data directory path"""
    return Path(settings.data_collection.bulk_data_path)


def ensure_bulk_data_folder() -> Path:
    """
    Ensure bulk data folder exists, create if it doesn't

    Returns:
        Path object to the bulk data folder
    """
    bulk_path = get_bulk_data_path()
    bulk_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Bulk data folder ready: {bulk_path.absolute()}")
    return bulk_path


def get_bulk_file_path(filename: str, subfolder: Optional[str] = None) -> Path:
    """
    Get full path to a bulk file

    Args:
        filename: Name of the bulk file
        subfolder: Optional subfolder (e.g., 'profiles', 'prices', 'financials')

    Returns:
        Full path to the file
    """
    bulk_path = get_bulk_data_path()

    if subfolder:
        bulk_path = bulk_path / subfolder
        bulk_path.mkdir(parents=True, exist_ok=True)

    return bulk_path / filename


def generate_bulk_filename(data_type: str, date: Optional[datetime] = None, extension: str = 'csv') -> str:
    """
    Generate a standardized bulk filename with timestamp

    Args:
        data_type: Type of data (e.g., 'profile', 'eod_prices', 'income_statement')
        date: Date for the data (defaults to today)
        extension: File extension (default: 'csv')

    Returns:
        Filename string like 'profile_bulk_20250115.csv'

    Examples:
        >>> generate_bulk_filename('profile')
        'profile_bulk_20250115.csv'
        >>> generate_bulk_filename('eod_prices', datetime(2025, 1, 10))
        'eod_prices_bulk_20250110.csv'
    """
    if date is None:
        date = datetime.now()

    date_str = date.strftime('%Y%m%d')
    return f"{data_type}_bulk_{date_str}.{extension}"


def list_bulk_files(data_type: Optional[str] = None, subfolder: Optional[str] = None) -> list[Path]:
    """
    List all bulk files in the bulk data folder

    Args:
        data_type: Filter by data type (e.g., 'profile', 'eod_prices')
        subfolder: Optional subfolder to search in

    Returns:
        List of Path objects
    """
    bulk_path = get_bulk_data_path()

    if subfolder:
        bulk_path = bulk_path / subfolder

    if not bulk_path.exists():
        return []

    if data_type:
        pattern = f"{data_type}_bulk_*.csv"
    else:
        pattern = "*_bulk_*.csv"

    files = sorted(bulk_path.glob(pattern), reverse=True)  # Most recent first
    return files


def archive_bulk_file(file_path: Path, archive_subfolder: str = 'archive') -> Path:
    """
    Move a bulk file to archive folder

    Args:
        file_path: Path to the file to archive
        archive_subfolder: Subfolder name for archives (default: 'archive')

    Returns:
        New path to the archived file
    """
    bulk_path = get_bulk_data_path()
    archive_path = bulk_path / archive_subfolder
    archive_path.mkdir(parents=True, exist_ok=True)

    new_path = archive_path / file_path.name
    file_path.rename(new_path)
    logger.info(f"Archived {file_path.name} to {archive_path}")

    return new_path
