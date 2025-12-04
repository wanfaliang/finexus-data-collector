"""
BEA Background Task Runner

Simple thread-based background task executor for BEA data collection.
Tracks running tasks and prevents duplicate runs.

Author: FinExus Data Collector
Created: 2025-11-27
"""
import threading
import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime, UTC
from enum import Enum

from sqlalchemy.orm import Session

from src.bea.bea_client import BEAClient
from src.bea.bea_collector import NIPACollector, RegionalCollector, GDPByIndustryCollector, ITACollector, FixedAssetsCollector, CollectionProgress, SentinelManager
from src.database.bea_tracking_models import BEACollectionRun, BEADatasetFreshness
from src.database.connection import get_session
from src.config import settings

log = logging.getLogger("BEATaskRunner")


class TaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BEATaskRunner:
    """
    Singleton task runner for BEA background operations.

    Ensures only one task runs at a time per dataset to prevent
    conflicts and rate limit issues.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._running_tasks: Dict[str, threading.Thread] = {}
        self._task_lock = threading.Lock()
        self._initialized = True

    def is_running(self, dataset_name: str) -> bool:
        """Check if a task is currently running for a dataset."""
        with self._task_lock:
            thread = self._running_tasks.get(dataset_name)
            if thread is None:
                return False
            if not thread.is_alive():
                del self._running_tasks[dataset_name]
                return False
            return True

    def get_running_tasks(self) -> Dict[str, bool]:
        """Get status of all datasets."""
        with self._task_lock:
            # Clean up dead threads
            dead = [k for k, v in self._running_tasks.items() if not v.is_alive()]
            for k in dead:
                del self._running_tasks[k]

            return {
                "NIPA": "NIPA" in self._running_tasks,
                "Regional": "Regional" in self._running_tasks,
                "GDPbyIndustry": "GDPbyIndustry" in self._running_tasks,
                "ITA": "ITA" in self._running_tasks,
                "FixedAssets": "FixedAssets" in self._running_tasks,
            }

    def start_nipa_backfill(
        self,
        frequency: str = "A",
        year: str = "ALL",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start NIPA backfill in background thread.

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("NIPA"):
            log.warning("NIPA task already running")
            return None

        # Create run record first
        run_id = self._create_run_record(
            "NIPA", "backfill",
            frequency=frequency,
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_nipa_backfill(run_id, frequency, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["NIPA"] = thread

        thread.start()
        log.info(f"Started NIPA backfill task (run_id={run_id})")
        return run_id

    def start_regional_backfill(
        self,
        geo_fips: str = "STATE",
        year: str = "ALL",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start Regional backfill in background thread.

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("Regional"):
            log.warning("Regional task already running")
            return None

        # Create run record first
        run_id = self._create_run_record(
            "Regional", "backfill",
            geo_scope=geo_fips,
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_regional_backfill(run_id, geo_fips, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["Regional"] = thread

        thread.start()
        log.info(f"Started Regional backfill task (run_id={run_id})")
        return run_id

    def start_gdpbyindustry_backfill(
        self,
        frequency: str = "A",
        year: str = "ALL",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start GDP by Industry backfill in background thread.

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("GDPbyIndustry"):
            log.warning("GDPbyIndustry task already running")
            return None

        # Create run record first
        run_id = self._create_run_record(
            "GDPbyIndustry", "backfill",
            frequency=frequency,
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_gdpbyindustry_backfill(run_id, frequency, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["GDPbyIndustry"] = thread

        thread.start()
        log.info(f"Started GDPbyIndustry backfill task (run_id={run_id})")
        return run_id

    def start_update(
        self,
        dataset: str = "all",
        year: str = "LAST5",
        force: bool = False,
    ) -> Optional[int]:
        """
        Start incremental update in background thread.

        Args:
            dataset: "NIPA", "Regional", "GDPbyIndustry", or "all"
            year: Year specification
            force: Force update even if recently updated

        Returns:
            run_id if started, None if already running
        """
        # Check if requested dataset(s) are already running
        if dataset == "all":
            if self.is_running("NIPA") or self.is_running("Regional") or self.is_running("GDPbyIndustry"):
                log.warning("Cannot start update - task already running")
                return None
        elif self.is_running(dataset):
            log.warning(f"{dataset} task already running")
            return None

        run_id = self._create_run_record(
            dataset, "update",
            year_spec=year
        )

        def task():
            self._run_update(run_id, dataset, year, force)

        thread = threading.Thread(target=task, daemon=True)

        # Mark all as running if "all"
        with self._task_lock:
            if dataset == "all":
                self._running_tasks["NIPA"] = thread
                self._running_tasks["Regional"] = thread
                self._running_tasks["GDPbyIndustry"] = thread
            else:
                self._running_tasks[dataset] = thread

        thread.start()
        log.info(f"Started update task for {dataset} (run_id={run_id})")
        return run_id

    def start_nipa_update(
        self,
        frequency: str = "A",
        year: str = "LAST5",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start NIPA update (not backfill) in background thread.

        Args:
            frequency: 'A', 'Q', or 'M'
            year: Year specification
            tables: Optional list of specific tables

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("NIPA"):
            log.warning("NIPA task already running")
            return None

        # Create run record with run_type='update'
        run_id = self._create_run_record(
            "NIPA", "update",
            frequency=frequency,
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_nipa_backfill(run_id, frequency, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["NIPA"] = thread

        thread.start()
        log.info(f"Started NIPA update task (run_id={run_id})")
        return run_id

    def start_regional_update(
        self,
        geo_fips: str = "STATE",
        year: str = "LAST5",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start Regional update (not backfill) in background thread.

        Args:
            geo_fips: Geographic scope ('STATE', 'COUNTY', 'MSA')
            year: Year specification
            tables: Optional list of specific tables

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("Regional"):
            log.warning("Regional task already running")
            return None

        # Create run record with run_type='update'
        run_id = self._create_run_record(
            "Regional", "update",
            geo_scope=geo_fips,
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_regional_backfill(run_id, geo_fips, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["Regional"] = thread

        thread.start()
        log.info(f"Started Regional update task (run_id={run_id})")
        return run_id

    def start_gdpbyindustry_update(
        self,
        frequency: str = "A",
        year: str = "LAST5",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start GDP by Industry update (not backfill) in background thread.

        Args:
            frequency: 'A' or 'Q'
            year: Year specification
            tables: Optional list of specific table IDs

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("GDPbyIndustry"):
            log.warning("GDPbyIndustry task already running")
            return None

        # Create run record with run_type='update'
        run_id = self._create_run_record(
            "GDPbyIndustry", "update",
            frequency=frequency,
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_gdpbyindustry_backfill(run_id, frequency, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["GDPbyIndustry"] = thread

        thread.start()
        log.info(f"Started GDPbyIndustry update task (run_id={run_id})")
        return run_id

    def start_ita_backfill(
        self,
        frequency: str = "A",
        year: str = "ALL",
        indicators: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start ITA (International Transactions) backfill in background thread.

        Args:
            frequency: 'A' (annual), 'QSA' (quarterly seasonally adjusted), 'QNSA' (quarterly not seasonally adjusted)
            year: Year specification ('ALL', 'LAST5', specific year)
            indicators: Optional list of specific indicator codes

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("ITA"):
            log.warning("ITA task already running")
            return None

        # Create run record first
        run_id = self._create_run_record(
            "ITA", "backfill",
            frequency=frequency,
            year_spec=year,
            tables_filter=indicators  # Using tables_filter for indicators
        )

        def task():
            self._run_ita_backfill(run_id, frequency, year, indicators)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["ITA"] = thread

        thread.start()
        log.info(f"Started ITA backfill task (run_id={run_id})")
        return run_id

    def start_ita_update(
        self,
        frequency: str = "A",
        year: str = "LAST5",
        indicators: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start ITA (International Transactions) update in background thread.

        Args:
            frequency: 'A' (annual), 'QSA' (quarterly seasonally adjusted), 'QNSA' (quarterly not seasonally adjusted)
            year: Year specification
            indicators: Optional list of specific indicator codes

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("ITA"):
            log.warning("ITA task already running")
            return None

        # Create run record with run_type='update'
        run_id = self._create_run_record(
            "ITA", "update",
            frequency=frequency,
            year_spec=year,
            tables_filter=indicators
        )

        def task():
            self._run_ita_backfill(run_id, frequency, year, indicators)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["ITA"] = thread

        thread.start()
        log.info(f"Started ITA update task (run_id={run_id})")
        return run_id

    def start_fixedassets_backfill(
        self,
        year: str = "ALL",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start Fixed Assets backfill in background thread.

        Args:
            year: Year specification ('ALL', 'LAST5', 'LAST10', specific year)
            tables: Optional list of specific table names

        Returns:
            run_id if started, None if already running

        Note:
            Fixed Assets only supports annual data.
        """
        if self.is_running("FixedAssets"):
            log.warning("FixedAssets task already running")
            return None

        # Create run record first
        run_id = self._create_run_record(
            "FixedAssets", "backfill",
            frequency="A",  # Fixed Assets is annual only
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_fixedassets_backfill(run_id, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["FixedAssets"] = thread

        thread.start()
        log.info(f"Started FixedAssets backfill task (run_id={run_id})")
        return run_id

    def start_fixedassets_update(
        self,
        year: str = "LAST5",
        tables: Optional[list] = None,
    ) -> Optional[int]:
        """
        Start Fixed Assets update in background thread.

        Args:
            year: Year specification
            tables: Optional list of specific table names

        Returns:
            run_id if started, None if already running
        """
        if self.is_running("FixedAssets"):
            log.warning("FixedAssets task already running")
            return None

        # Create run record with run_type='update'
        run_id = self._create_run_record(
            "FixedAssets", "update",
            frequency="A",
            year_spec=year,
            tables_filter=tables
        )

        def task():
            self._run_fixedassets_backfill(run_id, year, tables)

        thread = threading.Thread(target=task, daemon=True)

        with self._task_lock:
            self._running_tasks["FixedAssets"] = thread

        thread.start()
        log.info(f"Started FixedAssets update task (run_id={run_id})")
        return run_id

    # ==================== Internal Methods ==================== #

    def _create_run_record(
        self,
        dataset_name: str,
        run_type: str,
        frequency: Optional[str] = None,
        geo_scope: Optional[str] = None,
        year_spec: Optional[str] = None,
        tables_filter: Optional[list] = None,
    ) -> int:
        """Create a collection run record in the database."""
        import json

        with get_session() as session:
            run = BEACollectionRun(
                dataset_name=dataset_name,
                run_type=run_type,
                frequency=frequency,
                geo_scope=geo_scope,
                year_spec=year_spec,
                started_at=datetime.now(UTC),
                status=TaskStatus.QUEUED,
                tables_filter=json.dumps(tables_filter) if tables_filter else None,
            )
            session.add(run)
            session.commit()
            return run.run_id

    def _update_run_status(
        self,
        run_id: int,
        status: str,
        progress: Optional[CollectionProgress] = None,
        error_message: Optional[str] = None,
    ):
        """Update a collection run record."""
        with get_session() as session:
            run = session.query(BEACollectionRun).filter(
                BEACollectionRun.run_id == run_id
            ).first()

            if run:
                run.status = status

                if status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                    run.completed_at = datetime.now(UTC)

                if progress:
                    run.tables_processed = progress.tables_processed
                    run.series_processed = progress.series_processed
                    run.data_points_inserted = progress.data_points_inserted
                    run.data_points_updated = progress.data_points_updated
                    run.api_requests_made = progress.api_requests

                if error_message:
                    run.error_message = error_message

                session.commit()

    def _get_client(self) -> BEAClient:
        """Get a BEA API client."""
        api_key = settings.api.bea_api_key
        if not api_key or len(api_key) != 36:
            raise ValueError("Invalid or missing BEA_API_KEY")
        return BEAClient(api_key=api_key)

    def _run_nipa_backfill(
        self,
        run_id: int,
        frequency: str,
        year: str,
        tables: Optional[list],
    ):
        """Execute NIPA backfill (runs in background thread)."""
        try:
            self._update_run_status(run_id, TaskStatus.RUNNING)

            client = self._get_client()

            with get_session() as session:
                collector = NIPACollector(client, session)

                # Pass run_id to collector so it uses existing record (no duplicate)
                progress = collector.backfill_all_tables(
                    frequency=frequency,
                    year=year,
                    tables=tables,
                    progress_callback=lambda p: self._update_run_status(run_id, TaskStatus.RUNNING, p),
                    run_id=run_id
                )

                status = TaskStatus.COMPLETED if not progress.errors else TaskStatus.FAILED
                error_msg = "; ".join(progress.errors[:5]) if progress.errors else None
                self._update_run_status(run_id, status, progress, error_msg)

                # Sync sentinels after successful update
                if status == TaskStatus.COMPLETED:
                    sentinel_manager = SentinelManager(client, session)
                    sentinel_manager.sync_sentinels_from_data('NIPA')

                log.info(f"NIPA backfill completed: {progress.data_points_inserted} data points")

        except Exception as e:
            log.error(f"NIPA backfill failed: {e}", exc_info=True)
            self._update_run_status(run_id, TaskStatus.FAILED, error_message=str(e))

        finally:
            with self._task_lock:
                self._running_tasks.pop("NIPA", None)

    def _run_regional_backfill(
        self,
        run_id: int,
        geo_fips: str,
        year: str,
        tables: Optional[list],
    ):
        """Execute Regional backfill (runs in background thread)."""
        try:
            self._update_run_status(run_id, TaskStatus.RUNNING)

            client = self._get_client()

            with get_session() as session:
                collector = RegionalCollector(client, session)

                # Pass run_id to collector so it uses existing record (no duplicate)
                progress = collector.backfill_all_tables(
                    geo_fips=geo_fips,
                    year=year,
                    tables=tables,
                    progress_callback=lambda p: self._update_run_status(run_id, TaskStatus.RUNNING, p),
                    run_id=run_id
                )

                status = TaskStatus.COMPLETED if not progress.errors else TaskStatus.FAILED
                error_msg = "; ".join(progress.errors[:5]) if progress.errors else None
                self._update_run_status(run_id, status, progress, error_msg)

                # Sync sentinels after successful update
                if status == TaskStatus.COMPLETED:
                    sentinel_manager = SentinelManager(client, session)
                    sentinel_manager.sync_sentinels_from_data('Regional')

                log.info(f"Regional backfill completed: {progress.data_points_inserted} data points")

        except Exception as e:
            log.error(f"Regional backfill failed: {e}", exc_info=True)
            self._update_run_status(run_id, TaskStatus.FAILED, error_message=str(e))

        finally:
            with self._task_lock:
                self._running_tasks.pop("Regional", None)

    def _run_gdpbyindustry_backfill(
        self,
        run_id: int,
        frequency: str,
        year: str,
        tables: Optional[list],
    ):
        """Execute GDP by Industry backfill (runs in background thread)."""
        try:
            self._update_run_status(run_id, TaskStatus.RUNNING)

            client = self._get_client()

            with get_session() as session:
                collector = GDPByIndustryCollector(client, session)

                # Pass run_id to collector so it uses existing record (no duplicate)
                progress = collector.backfill_all_tables(
                    frequency=frequency,
                    year=year,
                    tables=tables,
                    progress_callback=lambda p: self._update_run_status(run_id, TaskStatus.RUNNING, p),
                    run_id=run_id
                )

                status = TaskStatus.COMPLETED if not progress.errors else TaskStatus.FAILED
                error_msg = "; ".join(progress.errors[:5]) if progress.errors else None
                self._update_run_status(run_id, status, progress, error_msg)

                # Sync sentinels after successful update
                if status == TaskStatus.COMPLETED:
                    sentinel_manager = SentinelManager(client, session)
                    sentinel_manager.sync_sentinels_from_data('GDPbyIndustry')

                log.info(f"GDPbyIndustry backfill completed: {progress.data_points_inserted} data points")

        except Exception as e:
            log.error(f"GDPbyIndustry backfill failed: {e}", exc_info=True)
            self._update_run_status(run_id, TaskStatus.FAILED, error_message=str(e))

        finally:
            with self._task_lock:
                self._running_tasks.pop("GDPbyIndustry", None)

    def _run_ita_backfill(
        self,
        run_id: int,
        frequency: str,
        year: str,
        indicators: Optional[list],
    ):
        """Execute ITA backfill (runs in background thread)."""
        try:
            self._update_run_status(run_id, TaskStatus.RUNNING)

            client = self._get_client()

            with get_session() as session:
                collector = ITACollector(client, session)

                # Pass run_id to collector so it uses existing record (no duplicate)
                progress = collector.backfill_all_indicators(
                    frequency=frequency,
                    year=year,
                    indicators=indicators,
                    progress_callback=lambda p: self._update_run_status(run_id, TaskStatus.RUNNING, p),
                    run_id=run_id
                )

                status = TaskStatus.COMPLETED if not progress.errors else TaskStatus.FAILED
                error_msg = "; ".join(progress.errors[:5]) if progress.errors else None
                self._update_run_status(run_id, status, progress, error_msg)

                # Sync sentinels after successful update
                if status == TaskStatus.COMPLETED:
                    sentinel_manager = SentinelManager(client, session)
                    sentinel_manager.sync_sentinels_from_data('ITA')

                log.info(f"ITA backfill completed: {progress.data_points_inserted} data points")

        except Exception as e:
            log.error(f"ITA backfill failed: {e}", exc_info=True)
            self._update_run_status(run_id, TaskStatus.FAILED, error_message=str(e))

        finally:
            with self._task_lock:
                self._running_tasks.pop("ITA", None)

    def _run_fixedassets_backfill(
        self,
        run_id: int,
        year: str,
        tables: Optional[list],
    ):
        """Execute Fixed Assets backfill (runs in background thread)."""
        try:
            self._update_run_status(run_id, TaskStatus.RUNNING)

            client = self._get_client()

            with get_session() as session:
                collector = FixedAssetsCollector(client, session)

                # Pass run_id to collector so it uses existing record (no duplicate)
                progress = collector.backfill_all_tables(
                    year=year,
                    tables=tables,
                    progress_callback=lambda p: self._update_run_status(run_id, TaskStatus.RUNNING, p),
                    run_id=run_id
                )

                status = TaskStatus.COMPLETED if not progress.errors else TaskStatus.FAILED
                error_msg = "; ".join(progress.errors[:5]) if progress.errors else None
                self._update_run_status(run_id, status, progress, error_msg)

                log.info(f"FixedAssets backfill completed: {progress.data_points_inserted} data points")

        except Exception as e:
            log.error(f"FixedAssets backfill failed: {e}", exc_info=True)
            self._update_run_status(run_id, TaskStatus.FAILED, error_message=str(e))

        finally:
            with self._task_lock:
                self._running_tasks.pop("FixedAssets", None)

    def _run_update(
        self,
        run_id: int,
        dataset: str,
        year: str,
        force: bool,
    ):
        """Execute incremental update (runs in background thread)."""
        try:
            self._update_run_status(run_id, TaskStatus.RUNNING)

            client = self._get_client()
            total_points = 0
            errors = []

            with get_session() as session:
                if dataset in ("NIPA", "all"):
                    try:
                        collector = NIPACollector(client, session)
                        # Pass run_id to collector so it uses existing record (no duplicate)
                        progress = collector.backfill_all_tables(
                            frequency="A",
                            year=year,
                            run_id=run_id
                        )
                        total_points += progress.data_points_inserted
                        errors.extend(progress.errors)
                    except Exception as e:
                        errors.append(f"NIPA: {str(e)}")

                if dataset in ("Regional", "all"):
                    try:
                        collector = RegionalCollector(client, session)
                        # Only update priority tables for incremental
                        # Pass run_id to collector so it uses existing record (no duplicate)
                        progress = collector.backfill_all_tables(
                            geo_fips="STATE",
                            year=year,
                            tables=["SAGDP1", "CAINC1", "SAINC1"],
                            run_id=run_id
                        )
                        total_points += progress.data_points_inserted
                        errors.extend(progress.errors)
                    except Exception as e:
                        errors.append(f"Regional: {str(e)}")

                if dataset in ("GDPbyIndustry", "all"):
                    try:
                        collector = GDPByIndustryCollector(client, session)
                        # Only update priority tables for incremental (Value Added, Contributions, Real Value Added)
                        # Pass run_id to collector so it uses existing record (no duplicate)
                        progress = collector.backfill_all_tables(
                            frequency="A",
                            year=year,
                            tables=[1, 5, 6],
                            run_id=run_id
                        )
                        total_points += progress.data_points_inserted
                        errors.extend(progress.errors)
                    except Exception as e:
                        errors.append(f"GDPbyIndustry: {str(e)}")

            # Create a summary progress
            summary = CollectionProgress(dataset, "update")
            summary.data_points_inserted = total_points
            summary.errors = errors

            status = TaskStatus.COMPLETED if not errors else TaskStatus.FAILED
            error_msg = "; ".join(errors[:5]) if errors else None
            self._update_run_status(run_id, status, summary, error_msg)

            # Sync sentinels after successful update
            if status == TaskStatus.COMPLETED:
                sentinel_manager = SentinelManager(client, session)
                if dataset in ("NIPA", "all"):
                    sentinel_manager.sync_sentinels_from_data('NIPA')
                if dataset in ("Regional", "all"):
                    sentinel_manager.sync_sentinels_from_data('Regional')
                if dataset in ("GDPbyIndustry", "all"):
                    sentinel_manager.sync_sentinels_from_data('GDPbyIndustry')

            log.info(f"Update completed: {total_points} data points")

        except Exception as e:
            log.error(f"Update failed: {e}", exc_info=True)
            self._update_run_status(run_id, TaskStatus.FAILED, error_message=str(e))

        finally:
            with self._task_lock:
                self._running_tasks.pop("NIPA", None)
                self._running_tasks.pop("Regional", None)
                self._running_tasks.pop("GDPbyIndustry", None)


# Global singleton instance
task_runner = BEATaskRunner()
