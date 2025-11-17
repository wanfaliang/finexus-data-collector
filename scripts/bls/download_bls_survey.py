#!/usr/bin/env python3
"""
Download BLS flat files for a given survey from download.bls.gov

This script automatically downloads all flat files for a BLS survey,
saving you from manually clicking through the website.

BLS provides public domain data and encourages downloading for analysis.

Usage:
    # Download all PC (PPI Industry) files
    python scripts/bls/download_bls_survey.py pc

    # Download WP (PPI Commodities) files
    python scripts/bls/download_bls_survey.py wp

    # Download any other survey
    python scripts/bls/download_bls_survey.py <survey_code>

    # Dry run (show what would be downloaded)
    python scripts/bls/download_bls_survey.py pc --dry-run

    # Force re-download existing files
    python scripts/bls/download_bls_survey.py pc --force
"""
import argparse
import requests
from pathlib import Path
from bs4 import BeautifulSoup
import time
from typing import List, Tuple
import sys

def get_file_list(survey_code: str) -> List[Tuple[str, str]]:
    """
    Get list of files available for download from BLS

    Args:
        survey_code: BLS survey code (e.g., 'pc', 'wp', 'ce', 'la')

    Returns:
        List of (filename, url) tuples
    """
    base_url = f"https://download.bls.gov/pub/time.series/{survey_code.lower()}/"

    print(f"Fetching file list from: {base_url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(base_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching file list: {e}")
        sys.exit(1)

    # Parse HTML directory listing
    soup = BeautifulSoup(response.text, 'html.parser')

    files = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and not href.startswith('?') and href != '../':
            # Skip parent directory link
            if href.endswith('/'):
                continue

            # Extract just the filename from the href
            # Handle both relative paths (filename) and absolute paths (/pub/time.series/pc/filename)
            filename = href.split('/')[-1] if '/' in href else href

            # Build the full URL
            if href.startswith('http'):
                file_url = href
            elif href.startswith('/'):
                file_url = 'https://download.bls.gov' + href
            else:
                file_url = base_url + href

            files.append((filename, file_url))

    return files


def get_file_size(url: str) -> int:
    """Get file size without downloading the full file"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.head(url, headers=headers, timeout=10)
        return int(response.headers.get('content-length', 0))
    except:
        return 0


def download_file(url: str, dest_path: Path, show_progress: bool = True) -> bool:
    """
    Download a file with progress indication

    Args:
        url: URL to download from
        dest_path: Local path to save to
        show_progress: Whether to show download progress

    Returns:
        True if successful, False otherwise
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))

        with open(dest_path, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
                return True

            downloaded = 0
            chunk_size = 8192

            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    if show_progress and total_size > 0:
                        percent = (downloaded / total_size) * 100
                        size_mb = downloaded / (1024 * 1024)
                        total_mb = total_size / (1024 * 1024)
                        print(f"\r  Progress: {percent:5.1f}% ({size_mb:.1f}/{total_mb:.1f} MB)", end='', flush=True)

            if show_progress:
                print()  # New line after progress

        return True

    except requests.RequestException as e:
        print(f"\n  Error downloading: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download BLS flat files for a survey",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download PC (PPI Industry) survey files
  python scripts/bls/download_bls_survey.py pc

  # Download WP (PPI Commodities) survey files
  python scripts/bls/download_bls_survey.py wp

  # Dry run to see what would be downloaded
  python scripts/bls/download_bls_survey.py pc --dry-run

  # Force re-download existing files
  python scripts/bls/download_bls_survey.py pc --force

Available surveys: ap, cu, la, ce, pc, wp, pr, jt, and more
        """
    )
    parser.add_argument(
        'survey_code',
        help='BLS survey code (e.g., pc, wp, ce, la, cu, ap)'
    )
    parser.add_argument(
        '--data-dir',
        default='data/bls',
        help='Base directory for BLS data (default: data/bls)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be downloaded without actually downloading'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-download files even if they already exist'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay in seconds between downloads (default: 0.5)'
    )

    args = parser.parse_args()

    survey_code = args.survey_code.lower()

    print("=" * 80)
    print(f"BLS SURVEY FILE DOWNLOADER - {survey_code.upper()}")
    print("=" * 80)
    print()

    # Create destination directory
    dest_dir = Path(args.data_dir) / survey_code
    dest_dir.mkdir(parents=True, exist_ok=True)
    print(f"Destination: {dest_dir.absolute()}")
    print()

    # Get file list
    files = get_file_list(survey_code)

    if not files:
        print(f"No files found for survey: {survey_code}")
        print(f"Check that the survey code is correct at: https://download.bls.gov/pub/time.series/")
        sys.exit(1)

    print(f"Found {len(files)} files to download")
    print()

    if args.dry_run:
        print("DRY RUN - Files that would be downloaded:")
        print()
        for filename, url in files:
            dest_path = dest_dir / filename
            exists = dest_path.exists()
            size = get_file_size(url)
            size_mb = size / (1024 * 1024) if size > 0 else 0
            status = "EXISTS" if exists and not args.force else "DOWNLOAD"
            print(f"  [{status:8}] {filename:40} ({size_mb:6.1f} MB)")
        print()
        print(f"Total files: {len(files)}")
        return

    # Download files
    downloaded = 0
    skipped = 0
    failed = 0

    for i, (filename, url) in enumerate(files, 1):
        dest_path = dest_dir / filename

        # Check if file exists
        if dest_path.exists() and not args.force:
            print(f"[{i:3}/{len(files)}] SKIP (exists): {filename}")
            skipped += 1
            continue

        print(f"[{i:3}/{len(files)}] Downloading: {filename}")

        success = download_file(url, dest_path, show_progress=True)

        if success:
            file_size = dest_path.stat().st_size / (1024 * 1024)
            print(f"  ✓ Downloaded: {file_size:.1f} MB")
            downloaded += 1
        else:
            print(f"  ✗ Failed to download")
            failed += 1
            # Remove partial download
            if dest_path.exists():
                dest_path.unlink()

        # Respectful delay between downloads
        if i < len(files):
            time.sleep(args.delay)

    print()
    print("=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"  Downloaded: {downloaded} files")
    print(f"  Skipped:    {skipped} files (already exist)")
    print(f"  Failed:     {failed} files")
    print(f"  Total:      {len(files)} files")
    print()
    print(f"Files saved to: {dest_dir.absolute()}")

    if failed > 0:
        print()
        print("⚠️  Some files failed to download. You can re-run the script to retry.")
        sys.exit(1)


if __name__ == "__main__":
    main()
