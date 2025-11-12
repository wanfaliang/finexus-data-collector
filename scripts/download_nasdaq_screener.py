"""
Download stock list CSV from Nasdaq screener
"""
import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.nasdaq_screener_downloader import download_nasdaq_screener_csv
from src.config import settings

# Setup logging with UTF-8 encoding for Windows console
import io

# Create a UTF-8 wrapper for stdout to handle emojis on Windows
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/nasdaq_screener_download.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Download stock screener CSV from Nasdaq',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download with default settings (headless mode)
  python scripts/download_nasdaq_screener.py

  # Download with visible browser
  python scripts/download_nasdaq_screener.py --no-headless

  # Specify custom output filename
  python scripts/download_nasdaq_screener.py --output nasdaq_stocks.csv

  # Custom output directory
  python scripts/download_nasdaq_screener.py --output-dir data/downloads
        """
    )

    parser.add_argument(
        '--output',
        '-o',
        metavar='FILENAME',
        help='Output CSV filename (default: nasdaq_screener_YYYYMMDD_HHMMSS.csv)'
    )

    parser.add_argument(
        '--output-dir',
        metavar='DIR',
        help=f'Output directory (default: {settings.data_collection.nasdaq_screener_path})'
    )

    parser.add_argument(
        '--no-headless',
        action='store_true',
        help='Run browser in visible mode (default: headless)'
    )

    parser.add_argument(
        '--retries',
        type=int,
        default=3,
        metavar='N',
        help='Maximum number of retry attempts (default: 3)'
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("NASDAQ STOCK SCREENER CSV DOWNLOAD")
    logger.info("="*80)
    logger.info(f"Output directory: {args.output_dir or settings.data_collection.nasdaq_screener_path}")
    logger.info(f"Headless mode: {not args.no_headless}")
    logger.info(f"Max retries: {args.retries}")
    logger.info("")

    try:
        # Create logs directory if needed
        Path('logs').mkdir(exist_ok=True)

        # Download CSV
        csv_path = download_nasdaq_screener_csv(
            output_dir=args.output_dir,
            output_filename=args.output,
            headless=not args.no_headless,
            max_retries=args.retries
        )

        logger.info("="*80)
        logger.info("✓ DOWNLOAD SUCCESSFUL")
        logger.info("="*80)
        logger.info(f"CSV saved to: {csv_path}")
        logger.info("")

        # Show file info
        file_size = csv_path.stat().st_size
        logger.info(f"File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")

        return 0

    except Exception as e:
        logger.error("="*80)
        logger.error("✗ DOWNLOAD FAILED")
        logger.error("="*80)
        logger.error(f"Error: {e}")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("  1. Make sure Playwright browsers are installed:")
        logger.error("     python -m playwright install chromium")
        logger.error("  2. Check your internet connection")
        logger.error("  3. Try running with --no-headless to see browser")
        logger.error("  4. Check logs/nasdaq_screener_download.log for details")

        return 1


if __name__ == "__main__":
    sys.exit(main())
