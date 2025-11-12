"""
Nasdaq ETF Screener CSV Downloader using Selenium
Uses your actual Chrome browser for better compatibility
"""
import logging
import time
from pathlib import Path
from typing import Optional
from datetime import datetime
import shutil

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from src.config import settings

logger = logging.getLogger(__name__)


class NasdaqETFScreenerSelenium:
    """Download CSV from Nasdaq ETF screener using Selenium"""

    SCREENER_URL = "https://www.nasdaq.com/market-activity/etf/screener"

    def __init__(self, download_dir: Optional[str] = None, headless: bool = False):
        """
        Initialize Nasdaq ETF screener downloader

        Args:
            download_dir: Directory to save downloaded CSV. If None, uses nasdaq_etf_screener_path.
            headless: Run browser in headless mode (default: False for better compatibility)
        """
        self.download_dir = Path(download_dir or settings.data_collection.nasdaq_etf_screener_path)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless
        logger.info(f"Nasdaq ETF Screener Selenium Downloader initialized. Download dir: {self.download_dir}")

    def _get_chrome_options(self, temp_download_dir: Path) -> Options:
        """Configure Chrome options"""
        chrome_options = Options()

        if self.headless:
            chrome_options.add_argument('--headless=new')  # New headless mode

        # Download preferences - use absolute path
        prefs = {
            "download.default_directory": str(temp_download_dir.resolve()),  # Absolute path
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,  # Disable to avoid blocking downloads
            "safebrowsing.disable_download_protection": True,
            "profile.default_content_setting_values.automatic_downloads": 1,  # Allow automatic downloads
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Additional Chrome arguments for downloads
        chrome_options.add_argument('--safebrowsing-disable-download-protection')
        chrome_options.add_argument('--safebrowsing-disable-extension-blacklist')

        # Additional options for compatibility
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        return chrome_options

    def download_csv(
        self,
        output_filename: Optional[str] = None,
        timeout: int = 60,
        wait_after_click: int = 10
    ) -> Path:
        """
        Download the ETF screener CSV from Nasdaq

        Args:
            output_filename: Custom filename for downloaded CSV. If None, uses timestamp.
            timeout: Page load timeout in seconds (default: 60)
            wait_after_click: Seconds to wait after clicking download (default: 10)

        Returns:
            Path to downloaded CSV file

        Raises:
            Exception: If download fails
        """
        logger.info(f"Starting CSV download from Nasdaq ETF screener: {self.SCREENER_URL}")

        # Create temporary download directory
        temp_download_dir = self.download_dir / "temp_downloads"
        temp_download_dir.mkdir(parents=True, exist_ok=True)

        driver = None
        try:
            # Setup Chrome driver
            chrome_options = self._get_chrome_options(temp_download_dir)
            service = Service(ChromeDriverManager().install())

            logger.info(f"Launching Chrome (headless={self.headless})...")
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(timeout)

            # Navigate to screener
            logger.info(f"Navigating to {self.SCREENER_URL}...")
            driver.get(self.SCREENER_URL)

            # Wait for page to load
            logger.info("Waiting for page to load...")
            time.sleep(5)  # Initial wait for dynamic content

            # Try to accept cookies if banner appears
            try:
                logger.info("Looking for cookie consent banner...")
                accept_cookies_selectors = [
                    (By.XPATH, "//button[contains(text(), 'Accept All')]"),
                    (By.XPATH, "//button[contains(text(), 'Accept all')]"),
                    (By.XPATH, "//button[@id='onetrust-accept-btn-handler']"),
                    (By.CSS_SELECTOR, "button.ot-button-primary"),
                ]

                for by, selector in accept_cookies_selectors:
                    try:
                        accept_button = WebDriverWait(driver, 3).until(
                            EC.element_to_be_clickable((by, selector))
                        )
                        if accept_button:
                            accept_button.click()
                            logger.info("✓ Accepted cookies")
                            time.sleep(1)  # Wait for banner to disappear
                            break
                    except Exception:
                        continue
            except Exception as e:
                logger.debug(f"No cookie banner found or already accepted: {e}")

            # Find and click the Download CSV button
            logger.info("Looking for Download CSV button...")

            # Try multiple selectors
            download_selectors = [
                (By.XPATH, "//button[contains(text(), 'Download CSV')]"),
                (By.XPATH, "//a[contains(text(), 'Download CSV')]"),
                (By.CSS_SELECTOR, "button[class*='download']"),
                (By.XPATH, "//*[contains(text(), 'Download CSV')]"),
            ]

            download_button = None
            for by, selector in download_selectors:
                try:
                    download_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((by, selector))
                    )
                    if download_button:
                        logger.info(f"Found download button with selector: {selector}")
                        break
                except Exception:
                    continue

            if not download_button:
                # Take screenshot for debugging
                screenshot_path = self.download_dir / f"nasdaq_etf_screener_debug_{datetime.now():%Y%m%d_%H%M%S}.png"
                driver.save_screenshot(str(screenshot_path))
                logger.error(f"Could not find download button. Screenshot saved to: {screenshot_path}")
                raise Exception("Download CSV button not found on page")

            # Click the button
            logger.info("Clicking Download CSV button...")
            download_button.click()
            logger.info(f"Download button clicked, waiting for download to complete...")

            # Wait for download to complete - poll for the file
            max_wait = wait_after_click * 2  # Double the wait time
            poll_interval = 0.5
            elapsed = 0
            downloaded_files = []

            while elapsed < max_wait:
                downloaded_files = list(temp_download_dir.glob("*.csv"))
                # Also check for partial downloads
                partial_files = list(temp_download_dir.glob("*.crdownload"))

                if downloaded_files:
                    logger.info(f"Download complete after {elapsed:.1f}s")
                    break
                elif partial_files:
                    logger.debug(f"Download in progress... ({elapsed:.1f}s)")

                time.sleep(poll_interval)
                elapsed += poll_interval

            if not downloaded_files:
                # Log what we found in the directory
                all_files = list(temp_download_dir.glob("*"))
                logger.error(f"Files in temp directory: {[f.name for f in all_files]}")
                raise Exception(f"No CSV file found in download directory after {max_wait}s")

            # Get the most recent file
            downloaded_file = max(downloaded_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Downloaded file found: {downloaded_file.name}")

            # Determine output filename
            if output_filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"nasdaq_etf_screener_{timestamp}.csv"
            elif not output_filename.endswith('.csv'):
                output_filename += '.csv'

            output_path = self.download_dir / output_filename

            # Move the file to the final destination
            shutil.move(str(downloaded_file), str(output_path))
            logger.info(f"CSV saved to: {output_path}")

            # Get file size
            file_size = output_path.stat().st_size
            logger.info(f"Downloaded file size: {file_size:,} bytes")

            return output_path

        finally:
            if driver:
                driver.quit()
                logger.info("Chrome browser closed")

            # Clean up temp directory
            if temp_download_dir.exists():
                shutil.rmtree(temp_download_dir, ignore_errors=True)

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


def download_nasdaq_etf_screener_csv_selenium(
    output_dir: Optional[str] = None,
    output_filename: Optional[str] = None,
    headless: bool = False,
    max_retries: int = 3
) -> Path:
    """
    Convenience function to download Nasdaq ETF screener CSV using Selenium

    Args:
        output_dir: Directory to save CSV (uses config if None)
        output_filename: Custom filename (uses timestamp if None)
        headless: Run browser in headless mode
        max_retries: Number of retry attempts

    Returns:
        Path to downloaded CSV file

    Example:
        >>> csv_path = download_nasdaq_etf_screener_csv_selenium()
        >>> print(f"Downloaded to: {csv_path}")
    """
    downloader = NasdaqETFScreenerSelenium(download_dir=output_dir, headless=headless)
    return downloader.download_with_retry(
        output_filename=output_filename,
        max_retries=max_retries
    )
