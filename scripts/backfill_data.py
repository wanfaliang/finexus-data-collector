"""Backfill Historical Data"""
import sys
import os
import argparse
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.connection import get_session
from src.collectors.company_collector import CompanyCollector
from src.collectors.financial_collector import FinancialCollector
from src.collectors.price_collector import PriceCollector
from src.collectors.economic_collector import EconomicCollector
from src.collectors.analyst_collector import AnalystCollector
from src.collectors.insider_collector import InsiderCollector
from src.collectors.employee_collector import EmployeeCollector
from src.collectors.enterprise_collector import EnterpriseCollector

def main():
    parser = argparse.ArgumentParser(description='Backfill historical data')
    parser.add_argument('--years', type=int, default=10,
                        help='Years of historical data to load')
    args = parser.parse_args()
    
    print(f"Starting backfill for {args.years} years of historical data...")
    
    with get_session() as session:
        # Company profiles (do this first)
        print("\n=== Company Profiles ===")
        cc = CompanyCollector(session)
        results = cc.collect_for_all_symbols()
        print(f"Results: {results}")
        
        # Financial data
        print("\n=== Financial Statements ===")
        fc = FinancialCollector(session)
        results = fc.collect_for_all_symbols()
        print(f"Results: {results}")
        
        # Prices
        print("\n=== Prices ===")
        pc = PriceCollector(session)
        results = pc.collect_for_all_symbols()
        print(f"Results: {results}")

        # Market Indices
        print("\n=== Market Indices ===")
        index_results = pc.collect_all_indices()
        print(f"Index Results: {index_results}")
        
        # Economic
        #print("\n=== Economic Indicators ===")
        #ec = EconomicCollector(session)
        #results = ec.collect_all_indicators()
        #print(f"Results: {results}")
        
        # Analyst data
        print("\n=== Analyst Data ===")
        ac = AnalystCollector(session)
        results = ac.collect_for_all_symbols()
        print(f"Results: {results}")
        
        # Insider/Institutional data
        print("\n=== Insider/Institutional Data ===")
        ic = InsiderCollector(session)
        results = ic.collect_for_all_symbols()
        print(f"Results: {results}")

        # Employee history
        print("\n=== Employee History ===")
        ec = EmployeeCollector(session)
        results = ec.collect_for_all_symbols()
        print(f"Results: {results}")

        # Enterprise values
        print("\n=== Enterprise Values ===")
        evc = EnterpriseCollector(session)
        results = evc.collect_for_all_symbols()
        print(f"Results: {results}")

    print("\n✓ Backfill complete!")

if __name__ == "__main__":
    main()
