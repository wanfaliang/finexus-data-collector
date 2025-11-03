"""Main Job Orchestrator - Coordinates all data collection jobs"""
import logging
import argparse
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import settings
from src.database.connection import get_session
from src.collectors.financial_collector import FinancialCollector
from src.collectors.price_collector import PriceCollector
from src.collectors.economic_collector import EconomicCollector
from src.collectors.analyst_collector import AnalystCollector
from src.collectors.insider_collector import InsiderCollector
from src.collectors.company_collector import CompanyCollector

logger = logging.getLogger(__name__)


def update_financial_statements():
    """Job: Update financial statements"""
    logger.info("=== Starting Financial Statements Update ===")
    with get_session() as session:
        collector = FinancialCollector(session)
        results = collector.collect_for_all_symbols()
        logger.info(f"Financial statements update completed: {results}")
        return results


def update_daily_prices():
    """Job: Update daily prices"""
    logger.info("=== Starting Daily Prices Update ===")
    with get_session() as session:
        collector = PriceCollector(session)
        results = collector.collect_for_all_symbols()
        collector.collect_sp500()
        logger.info(f"Daily prices update completed: {results}")
        return results


def update_economic_indicators():
    """Job: Update economic indicators"""
    logger.info("=== Starting Economic Indicators Update ===")
    with get_session() as session:
        collector = EconomicCollector(session)
        results = collector.collect_all_indicators()
        logger.info(f"Economic indicators update completed: {results}")
        return results


def update_analyst_data():
    """Job: Update analyst estimates and price targets"""
    logger.info("=== Starting Analyst Data Update ===")
    with get_session() as session:
        collector = AnalystCollector(session)
        results = collector.collect_for_all_symbols()
        logger.info(f"Analyst data update completed: {results}")
        return results


def update_insider_data():
    """Job: Update insider trading and institutional ownership"""
    logger.info("=== Starting Insider/Institutional Data Update ===")
    with get_session() as session:
        collector = InsiderCollector(session)
        results = collector.collect_for_all_symbols()
        logger.info(f"Insider data update completed: {results}")
        return results


def update_company_profiles():
    """Job: Update company profiles"""
    logger.info("=== Starting Company Profiles Update ===")
    with get_session() as session:
        collector = CompanyCollector(session)
        results = collector.collect_for_all_symbols()
        logger.info(f"Company profiles update completed: {results}")
        return results


def run_all_jobs():
    """Run all jobs once"""
    logger.info("=== Running All Data Collection Jobs ===")
    
    jobs = [
        ("Company Profiles", update_company_profiles),
        ("Financial Statements", update_financial_statements),
        ("Daily Prices", update_daily_prices),
        ("Economic Indicators", update_economic_indicators),
        ("Analyst Data", update_analyst_data),
        ("Insider/Institutional Data", update_insider_data),
    ]
    
    results = {}
    for job_name, job_func in jobs:
        try:
            logger.info(f"\n>>> Starting {job_name}...")
            results[job_name] = job_func()
        except Exception as e:
            logger.error(f"Error in {job_name}: {e}", exc_info=True)
            results[job_name] = {'status': 'failed', 'error': str(e)}
    
    return results


def setup_scheduler():
    """Setup APScheduler with cron triggers"""
    scheduler = BlockingScheduler()
    
    # Company profiles (weekly on Sundays at 6 AM)
    scheduler.add_job(
        update_company_profiles,
        trigger=CronTrigger(day_of_week='sun', hour=6, minute=0),
        id='update_companies',
        name='Update Company Profiles',
        replace_existing=True
    )
    
    # Financial statements (daily at 7 PM)
    scheduler.add_job(
        update_financial_statements,
        trigger=CronTrigger.from_crontab(settings.schedule.schedule_financials),
        id='update_financials',
        name='Update Financial Statements',
        replace_existing=True
    )
    
    # Daily prices (weekdays at 6 PM)
    scheduler.add_job(
        update_daily_prices,
        trigger=CronTrigger.from_crontab(settings.schedule.schedule_daily_prices),
        id='update_prices',
        name='Update Daily Prices',
        replace_existing=True
    )
    
    # Economic indicators (daily at 8 AM)
    scheduler.add_job(
        update_economic_indicators,
        trigger=CronTrigger.from_crontab(settings.schedule.schedule_economic),
        id='update_economic',
        name='Update Economic Indicators',
        replace_existing=True
    )
    
    # Analyst data (daily at 10 AM)
    scheduler.add_job(
        update_analyst_data,
        trigger=CronTrigger.from_crontab(settings.schedule.schedule_analyst),
        id='update_analyst',
        name='Update Analyst Data',
        replace_existing=True
    )
    
    # Insider/Institutional data (daily at 11 AM)
    scheduler.add_job(
        update_insider_data,
        trigger=CronTrigger.from_crontab(settings.schedule.schedule_insider),
        id='update_insider',
        name='Update Insider Data',
        replace_existing=True
    )
    
    return scheduler


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='FinExus Data Collector')
    parser.add_argument('--run-once', action='store_true',
                        help='Run all jobs once and exit')
    parser.add_argument('--schedule', action='store_true',
                        help='Run scheduler (production mode)')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(settings.app.log_file_path)
        ]
    )
    
    if args.run_once:
        logger.info("Running all jobs once...")
        results = run_all_jobs()
        logger.info(f"\n=== All Jobs Completed ===\nResults: {results}")
        
    elif args.schedule:
        logger.info("Starting scheduler...")
        scheduler = setup_scheduler()
        
        logger.info("\nScheduled jobs:")
        for job in scheduler.get_jobs():
            logger.info(f"  - {job.name}: {job.trigger}")
        
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down scheduler...")
            scheduler.shutdown()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
