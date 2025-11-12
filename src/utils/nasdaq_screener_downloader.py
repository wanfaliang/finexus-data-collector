"""
Nasdaq Stock Screener CSV Downloader using Playwright
Downloads the complete stock list CSV from Nasdaq screener
"""
import logging
import time
from pathlib import Path
from typing import Optional
from datetime import datetime

from playwright.sync_api import sync_playwright, Page, Download, TimeoutError as PlaywrightTimeoutError

from src.config import settings

logger = logging.getLogger(__name__)


class NasdaqScreenerDownloader:
    """Download CSV from Nasdaq stock screener using Playwright"""

    SCREENER_URL = "https://www.nasdaq.com/market-activity/stocks/screener"

    def __init__(self, download_dir: Optional[str] = None, headless: bool = True):
        """
        Initialize Nasdaq screener downloader

        Args:
            download_dir: Directory to save downloaded CSV. If None, uses nasdaq_screener_path.
            headless: Run browser in headless mode (default: True)
        """
        self.download_dir = Path(download_dir or settings.data_collection.nasdaq_screener_path)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless
        logger.info(f"Nasdaq Screener Downloader initialized. Download dir: {self.download_dir}")

    def download_csv(
        self,
        output_filename: Optional[str] = None,
        timeout: int = 60000,
        wait_after_click: int = 5
    ) -> Path:
        """
        Download the stock screener CSV from Nasdaq

        Args:
            output_filename: Custom filename for downloaded CSV. If None, uses timestamp.
            timeout: Page load timeout in milliseconds (default: 60s)
            wait_after_click: Seconds to wait after clicking download (default: 5s)

        Returns:
            Path to downloaded CSV file

        Raises:
            Exception: If download fails
        """
        logger.info(f"Starting CSV download from Nasdaq screener: {self.SCREENER_URL}")

        with sync_playwright() as p:
            # Launch browser with additional args to avoid detection
            logger.info(f"Launching browser (headless={self.headless})...")
            browser = p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',  # Hide automation
                ]
            )

            try:
                # Create context with download path and realistic user agent
                context = browser.new_context(
                    accept_downloads=True,
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    extra_http_headers={
                        'Accept-Language': 'en-US,en;q=0.9',
                    }
                )

                # Hide webdriver property
                page = context.new_page()
                page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                """)

                # Navigate to screener
                logger.info(f"Navigating to {self.SCREENER_URL}...")
                page.goto(self.SCREENER_URL, timeout=timeout, wait_until='load')

                # Wait for page to be ready (don't wait for networkidle - Nasdaq has continuous background requests)
                logger.info("Waiting for page to load...")
                time.sleep(5)  # Wait for dynamic content to render

                # Find and click the Download CSV button
                logger.info("Looking for Download CSV button...")

                # Try multiple selectors for the download button
                download_selectors = [
                    'button:has-text("Download CSV")',
                    'a:has-text("Download CSV")',
                    'button[class*="download"]',
                    'a[class*="download"]',
                    'button:has-text("Download")',
                    'text=Download CSV'
                ]

                download_button = None
                for selector in download_selectors:
                    try:
                        download_button = page.wait_for_selector(selector, timeout=10000)
                        if download_button:
                            logger.info(f"Found download button with selector: {selector}")
                            break
                    except PlaywrightTimeoutError:
                        continue

                if not download_button:
                    # Take screenshot for debugging
                    screenshot_path = self.download_dir / f"nasdaq_screener_debug_{datetime.now():%Y%m%d_%H%M%S}.png"
                    page.screenshot(path=str(screenshot_path))
                    logger.error(f"Could not find download button. Screenshot saved to: {screenshot_path}")
                    raise Exception("Download CSV button not found on page")

                # Set up download handler
                logger.info("Clicking Download CSV button...")
                with page.expect_download(timeout=timeout) as download_info:
                    download_button.click()
                    logger.info("Download button clicked, waiting for download...")

                download = download_info.value

                # Wait a bit for download to complete
                time.sleep(wait_after_click)

                # Determine output filename
                if output_filename is None:
                    # Use timestamp-based filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_filename = f"nasdaq_screener_{timestamp}.csv"
                elif not output_filename.endswith('.csv'):
                    output_filename += '.csv'

                output_path = self.download_dir / output_filename

                # Save the downloaded file
                download.save_as(str(output_path))
                logger.info(f"CSV downloaded successfully: {output_path}")

                # Get file size
                file_size = output_path.stat().st_size
                logger.info(f"Downloaded file size: {file_size:,} bytes")

                return output_path

            finally:
                browser.close()
                logger.info("Browser closed")

    def download_with_retry(
        self,
        output_filename: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: int = 5,
        **kwargs
    ) -> Path:
        """
        Download CSV with automatic retry on failure

        Args:
            output_filename: Custom filename for downloaded CSV
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Seconds to wait between retries (default: 5)
            **kwargs: Additional arguments passed to download_csv()

        Returns:
            Path to downloaded CSV file

        Raises:
            Exception: If all retry attempts fail
        """
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Download attempt {attempt}/{max_retries}")
                return self.download_csv(output_filename=output_filename, **kwargs)
            except Exception as e:
                last_error = e
                logger.error(f"Attempt {attempt} failed: {e}")

                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"All {max_retries} attempts failed")

        raise Exception(f"Failed to download after {max_retries} attempts") from last_error


def download_nasdaq_screener_csv(
    output_dir: Optional[str] = None,
    output_filename: Optional[str] = None,
    headless: bool = True,
    max_retries: int = 3
) -> Path:
    """
    Convenience function to download Nasdaq screener CSV

    Args:
        output_dir: Directory to save CSV (uses config if None)
        output_filename: Custom filename (uses timestamp if None)
        headless: Run browser in headless mode
        max_retries: Number of retry attempts

    Returns:
        Path to downloaded CSV file

    Example:
        >>> csv_path = download_nasdaq_screener_csv()
        >>> print(f"Downloaded to: {csv_path}")
    """
    downloader = NasdaqScreenerDownloader(download_dir=output_dir, headless=headless)
    return downloader.download_with_retry(
        output_filename=output_filename,
        max_retries=max_retries
    )
