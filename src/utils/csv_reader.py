"""
CSV Reader Utility - Read delimited .txt files and export as CSV
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, List

from src.config import settings

logger = logging.getLogger(__name__)


class CSVReader:
    """Utility to read delimited text files and export as CSV"""

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize CSV Reader

        Args:
            output_dir: Output directory path. If None, uses config setting.
        """
        self.output_dir = Path(output_dir or settings.data_collection.bulk_data_path)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"CSV Reader initialized. Output directory: {self.output_dir}")

    def read_and_export(
        self,
        input_path: str,
        output_filename: Optional[str] = None,
        delimiter: str = '\t',
        encoding: str = 'utf-8',
        header: Optional[int] = 0,
        skiprows: Optional[int] = None,
        usecols: Optional[List[str]] = None,
        **kwargs
    ) -> Path:
        """
        Read a delimited text file and export as CSV

        Args:
            input_path: Path to input .txt file
            output_filename: Output CSV filename. If None, derives from input filename.
            delimiter: Field delimiter (default: tab)
            encoding: File encoding (default: utf-8)
            header: Row number to use as column names (default: 0)
            skiprows: Number of rows to skip at start
            usecols: List of columns to use
            **kwargs: Additional pandas read_csv arguments

        Returns:
            Path to exported CSV file
        """
        input_file = Path(input_path)

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        logger.info(f"Reading delimited file: {input_file}")

        # Set default parameters for better handling
        read_params = {
            'delimiter': delimiter,
            'encoding': encoding,
            'header': header,
            'skiprows': skiprows,
            'usecols': usecols,
            'quotechar': '"',  # Handle quoted fields
            'on_bad_lines': 'warn',  # Warn on bad lines instead of failing
        }
        read_params.update(kwargs)

        # Read the delimited file
        df = pd.read_csv(input_file, **read_params)

        logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")

        # Determine output filename
        if output_filename is None:
            output_filename = input_file.stem + '.csv'

        if not output_filename.endswith('.csv'):
            output_filename += '.csv'

        output_path = self.output_dir / output_filename

        # Export to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Exported CSV to: {output_path}")

        return output_path

    def read_and_export_with_auto_detect(
        self,
        input_path: str,
        output_filename: Optional[str] = None,
        encoding: str = 'utf-8',
        **kwargs
    ) -> Path:
        """
        Read a delimited file with automatic delimiter detection and export as CSV

        Args:
            input_path: Path to input .txt file
            output_filename: Output CSV filename
            encoding: File encoding
            **kwargs: Additional pandas read_csv arguments

        Returns:
            Path to exported CSV file
        """
        input_file = Path(input_path)

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        logger.info(f"Reading file with auto-detect: {input_file}")

        # Set default parameters for better handling
        read_params = {
            'encoding': encoding,
            'sep': None,  # Auto-detect separator
            'engine': 'python',  # Required for auto-detect
            'quotechar': '"',  # Handle quoted fields
            'on_bad_lines': 'warn',  # Warn on bad lines instead of failing
        }
        read_params.update(kwargs)

        # Use pandas auto-detect (tries common delimiters)
        df = pd.read_csv(input_file, **read_params)

        logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")

        # Determine output filename
        if output_filename is None:
            output_filename = input_file.stem + '.csv'

        if not output_filename.endswith('.csv'):
            output_filename += '.csv'

        output_path = self.output_dir / output_filename

        # Export to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Exported CSV to: {output_path}")

        return output_path

    def batch_convert(
        self,
        input_dir: str,
        pattern: str = '*.txt',
        delimiter: str = '\t',
        encoding: str = 'utf-8',
        **kwargs
    ) -> List[Path]:
        """
        Batch convert multiple delimited text files to CSV

        Args:
            input_dir: Directory containing input files
            pattern: File pattern to match (default: *.txt)
            delimiter: Field delimiter
            encoding: File encoding
            **kwargs: Additional pandas read_csv arguments

        Returns:
            List of exported CSV file paths
        """
        input_path = Path(input_dir)

        if not input_path.exists():
            raise FileNotFoundError(f"Input directory not found: {input_dir}")

        input_files = list(input_path.glob(pattern))
        logger.info(f"Found {len(input_files)} files matching '{pattern}' in {input_dir}")

        exported_files = []

        for input_file in input_files:
            try:
                output_path = self.read_and_export(
                    str(input_file),
                    delimiter=delimiter,
                    encoding=encoding,
                    **kwargs
                )
                exported_files.append(output_path)
            except Exception as e:
                logger.error(f"Failed to convert {input_file.name}: {e}")

        logger.info(f"Successfully converted {len(exported_files)}/{len(input_files)} files")
        return exported_files


def convert_txt_to_csv(
    input_path: str,
    output_dir: Optional[str] = None,
    output_filename: Optional[str] = None,
    delimiter: str = '\t',
    encoding: str = 'utf-8',
    **kwargs
) -> Path:
    """
    Convenience function to convert a single delimited text file to CSV

    Args:
        input_path: Path to input .txt file
        output_dir: Output directory (uses config if None)
        output_filename: Output filename (derives from input if None)
        delimiter: Field delimiter
        encoding: File encoding
        **kwargs: Additional pandas read_csv arguments

    Returns:
        Path to exported CSV file

    Example:
        >>> output = convert_txt_to_csv('data/input.txt', delimiter='|')
        >>> print(f"Exported to: {output}")
    """
    reader = CSVReader(output_dir)
    return reader.read_and_export(
        input_path,
        output_filename=output_filename,
        delimiter=delimiter,
        encoding=encoding,
        **kwargs
    )
