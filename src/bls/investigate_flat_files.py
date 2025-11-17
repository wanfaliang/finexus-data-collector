# investigate_flat_files.py
"""
Investigate BLS flat file structure for bulk downloads.
BLS provides complete historical data in tab-delimited files.
"""
import requests
import io
import csv

BASE_URL = "https://download.bls.gov/pub/time.series"

def list_ap_files():
    """List available files in the AP directory"""
    # Common file structure for BLS surveys:
    # - {survey}.series - Series metadata/catalog
    # - {survey}.data.0.Current - Recent data (last ~3 years)
    # - {survey}.data.1.AllData - All historical data
    # - {survey}.txt - Documentation

    files = [
        "ap.series",           # Series catalog (already have this)
        "ap.data.0.Current",   # Recent data
        "ap.data.1.AllData",   # All historical data
        "ap.area",             # Area codes
        "ap.item",             # Item codes
        "ap.footnote",         # Footnote codes
        "ap.period",           # Period codes
        "ap.txt",              # Documentation
    ]

    print("Available AP survey files:")
    for f in files:
        url = f"{BASE_URL}/ap/{f}"
        print(f"  {url}")

    return files

def fetch_sample_data(limit=20):
    """Fetch and parse sample from ap.data.0.Current"""
    url = f"{BASE_URL}/ap/ap.data.0.Current"
    print(f"\nFetching sample data from: {url}")

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        # BLS data files are tab-delimited
        lines = resp.text.split('\n')
        print(f"Total lines in file: {len(lines):,}")

        print(f"\nFirst {limit} lines:")
        print("-" * 100)
        for i, line in enumerate(lines[:limit]):
            print(f"{i:3d}: {line}")

        # Try to parse as CSV
        print("\n" + "=" * 100)
        print("Parsing as tab-delimited CSV:")
        print("=" * 100)
        reader = csv.DictReader(io.StringIO(resp.text), delimiter='\t')

        # Get field names
        print(f"Columns: {reader.fieldnames}")

        # Show first few records
        print("\nFirst 10 data records:")
        for i, row in enumerate(reader):
            if i >= 10:
                break
            # Clean up whitespace
            row = {k.strip(): v.strip() for k, v in row.items()}
            print(f"\n  Record {i+1}:")
            for k, v in row.items():
                if v:  # Only show non-empty fields
                    print(f"    {k:20s}: {v}")

        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

def check_all_data_size():
    """Check the size of the full historical data file"""
    url = f"{BASE_URL}/ap/ap.data.1.AllData"
    print(f"\nChecking size of full historical data: {url}")

    try:
        resp = requests.head(url, timeout=10)
        size_bytes = int(resp.headers.get('Content-Length', 0))
        size_mb = size_bytes / (1024 * 1024)
        print(f"  File size: {size_mb:.2f} MB ({size_bytes:,} bytes)")

        # Estimate number of records (rough estimate)
        # Assuming ~80 bytes per line average
        estimated_lines = size_bytes // 80
        print(f"  Estimated lines: ~{estimated_lines:,}")

        return size_mb

    except Exception as e:
        print(f"Error: {e}")
        return None

def fetch_area_codes():
    """Fetch and show area code mapping"""
    url = f"{BASE_URL}/ap/ap.area"
    print(f"\nFetching area codes: {url}")

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()

        lines = resp.text.split('\n')
        print(f"First 15 lines:")
        for line in lines[:15]:
            print(f"  {line}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("=" * 100)
    print("BLS FLAT FILE INVESTIGATION - AP Survey (Average Prices)")
    print("=" * 100)

    # 1. List available files
    list_ap_files()

    # 2. Check full data file size
    check_all_data_size()

    # 3. Fetch sample data
    fetch_sample_data(limit=25)

    # 4. Area codes
    fetch_area_codes()

    print("\n" + "=" * 100)
    print("INVESTIGATION COMPLETE")
    print("=" * 100)
