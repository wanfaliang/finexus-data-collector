"""
Regenerate Monthly Prices from Daily Prices

Recreates prices_monthly table data from prices_daily
Useful for:
- Fixing monthly price gaps after bulk daily backfills
- Ensuring monthly data consistency
- One-time cleanup or periodic maintenance
"""
import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.database.models import PriceDaily
from src.collectors.price_collector import PriceCollector
from sqlalchemy import func

# Create logs directory if needed
Path('logs').mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/regenerate_monthly_prices.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_all_symbols(session):
    """Get all distinct symbols from prices_daily table"""
    results = session.query(PriceDaily.symbol).distinct().all()
    return [r[0] for r in results]


def regenerate_all(limit=None, symbols_list=None):
    """
    Regenerate monthly prices for all symbols

    Args:
        limit: Maximum number of symbols to process (for testing)
        symbols_list: Optional specific list of symbols to process

    Returns:
        Exit code (0 = success, 1 = partial failure)
    """
    logger.info("="*80)
    logger.info("REGENERATE MONTHLY PRICES FROM DAILY PRICES")
    logger.info("="*80)

    with get_session() as session:
        # Get symbols to process
        if symbols_list:
            symbols = symbols_list
            logger.info(f"Processing {len(symbols)} specified symbols")
        else:
            symbols = get_all_symbols(session)
            logger.info(f"Found {len(symbols):,} symbols in prices_daily")

        if limit and len(symbols) > limit:
            symbols = symbols[:limit]
            logger.info(f"Limited to {limit} symbols")

        if not symbols:
            logger.warning("No symbols to process!")
            return 0

        # Initialize collector
        collector = PriceCollector(session)

        # Process each symbol
        logger.info(f"\n{'='*80}")
        logger.info("PROCESSING SYMBOLS")
        logger.info(f"{'='*80}\n")

        success_count = 0
        failed_symbols = []

        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[{i}/{len(symbols)}] {symbol}")
                collector._generate_monthly_prices(symbol)
                success_count += 1

            except Exception as e:
                logger.error(f"   {symbol}: Failed - {e}")
                failed_symbols.append(symbol)
                session.rollback()

            # Progress indicator every 100 symbols
            if i % 100 == 0:
                logger.info(f"\n  Progress: {i}/{len(symbols)} ({i/len(symbols)*100:.1f}%)\n")

        # Summary
        logger.info(f"\n{'='*80}")
        logger.info("REGENERATION COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Total symbols: {len(symbols):,}")
        logger.info(f"Successfully processed: {success_count:,}")
        logger.info(f"Failed: {len(failed_symbols)}")

        if failed_symbols:
            logger.warning(f"\nFailed symbols:")
            for s in failed_symbols[:20]:  # Show first 20
                logger.warning(f"  - {s}")
            if len(failed_symbols) > 20:
                logger.warning(f"  ... and {len(failed_symbols) - 20} more")

        logger.info(f"{'='*80}")

        return 0 if not failed_symbols else 1


def main():
    parser = argparse.ArgumentParser(
        description='Regenerate monthly prices from daily prices',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Regenerate all monthly prices
  python scripts/regenerate_monthly_prices.py

  # Test with first 10 symbols
  python scripts/regenerate_monthly_prices.py --limit 10

  # Regenerate specific symbols
  python scripts/regenerate_monthly_prices.py --symbols AAPL,MSFT,GOOGL

  # Read symbols from file
  python scripts/regenerate_monthly_prices.py --symbols-file data/priority_lists/priority1_active_in_db.txt
        """
    )

    parser.add_argument(
        '--limit',
        type=int,
        metavar='N',
        help='Limit number of symbols to process (for testing)'
    )

    parser.add_argument(
        '--symbols',
        type=str,
        help='Comma-separated list of symbols to process (e.g., AAPL,MSFT,GOOGL)'
    )

    parser.add_argument(
        '--symbols-file',
        type=str,
        help='File containing symbols (one per line or CSV)'
    )

    args = parser.parse_args()

    # Parse symbols if provided
    symbols_list = None
    if args.symbols:
        symbols_list = [s.strip() for s in args.symbols.split(',')]
        logger.info(f"Processing specific symbols: {symbols_list}")

    elif args.symbols_file:
        from pathlib import Path
        symbols_file = Path(args.symbols_file)

        if not symbols_file.exists():
            logger.error(f"Symbols file not found: {args.symbols_file}")
            return 1

        if symbols_file.suffix == '.txt':
            with open(symbols_file, 'r') as f:
                symbols_list = [line.strip() for line in f if line.strip()]

        elif symbols_file.suffix == '.csv':
            import csv
            with open(symbols_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                symbols_list = []
                for row in reader:
                    symbol = row.get('symbol') or row.get('Symbol') or row.get('SYMBOL') or row.get('ticker') or row.get('Ticker')
                    if symbol:
                        symbols_list.append(symbol.strip())

        else:
            logger.error(f"Unsupported file format: {symbols_file.suffix}. Use .txt or .csv")
            return 1

        logger.info(f"Loaded {len(symbols_list)} symbols from {symbols_file.name}")

    return regenerate_all(args.limit, symbols_list)


if __name__ == "__main__":
    sys.exit(main())
