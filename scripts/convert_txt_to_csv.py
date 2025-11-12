"""
Command-line script to convert delimited text files to CSV
"""
import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.csv_reader import CSVReader
from src.config import settings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Convert delimited text files to CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert single tab-delimited file
  python scripts/convert_txt_to_csv.py data/input.txt

  # Convert pipe-delimited file
  python scripts/convert_txt_to_csv.py data/input.txt --delimiter "|"

  # Specify output filename
  python scripts/convert_txt_to_csv.py data/input.txt --output output.csv

  # Batch convert all .txt files in a directory
  python scripts/convert_txt_to_csv.py data/txt_files/ --batch

  # Auto-detect delimiter
  python scripts/convert_txt_to_csv.py data/input.txt --auto-detect
        """
    )

    parser.add_argument(
        'input_path',
        help='Path to input .txt file or directory (for batch mode)'
    )

    parser.add_argument(
        '--output',
        '-o',
        metavar='FILENAME',
        help='Output CSV filename (default: same as input with .csv extension)'
    )

    parser.add_argument(
        '--output-dir',
        metavar='DIR',
        help=f'Output directory (default: {settings.data_collection.bulk_data_path})'
    )

    parser.add_argument(
        '--delimiter',
        '-d',
        default='\t',
        help='Field delimiter (default: tab)'
    )

    parser.add_argument(
        '--encoding',
        '-e',
        default='utf-8',
        help='File encoding (default: utf-8)'
    )

    parser.add_argument(
        '--auto-detect',
        action='store_true',
        help='Auto-detect delimiter'
    )

    parser.add_argument(
        '--batch',
        action='store_true',
        help='Batch convert all .txt files in directory'
    )

    parser.add_argument(
        '--pattern',
        default='*.txt',
        help='File pattern for batch mode (default: *.txt)'
    )

    parser.add_argument(
        '--header',
        type=int,
        metavar='ROW',
        help='Row number to use as column names (default: 0)'
    )

    parser.add_argument(
        '--skiprows',
        type=int,
        metavar='N',
        help='Number of rows to skip at start'
    )

    args = parser.parse_args()

    # Create CSV reader
    reader = CSVReader(args.output_dir)

    try:
        if args.batch:
            # Batch mode
            logger.info(f"Batch converting files in: {args.input_path}")
            exported_files = reader.batch_convert(
                args.input_path,
                pattern=args.pattern,
                delimiter=args.delimiter,
                encoding=args.encoding,
                header=args.header,
                skiprows=args.skiprows
            )
            logger.info(f"Converted {len(exported_files)} files")
            for file in exported_files:
                print(f"  - {file}")

        elif args.auto_detect:
            # Auto-detect delimiter
            logger.info(f"Converting with auto-detect: {args.input_path}")
            output_path = reader.read_and_export_with_auto_detect(
                args.input_path,
                output_filename=args.output,
                encoding=args.encoding,
                header=args.header,
                skiprows=args.skiprows
            )
            logger.info(f"Success! Output: {output_path}")

        else:
            # Single file conversion
            logger.info(f"Converting: {args.input_path}")
            output_path = reader.read_and_export(
                args.input_path,
                output_filename=args.output,
                delimiter=args.delimiter,
                encoding=args.encoding,
                header=args.header,
                skiprows=args.skiprows
            )
            logger.info(f"Success! Output: {output_path}")

        return 0

    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
