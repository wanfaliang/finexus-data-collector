"""
Example: How to use data transformation utilities when inserting API data

This example shows how to:
1. Fetch data from FMP API
2. Transform camelCase to snake_case
3. Insert into database
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.utils.data_transform import (
    transform_income_statement,
    transform_balance_sheet,
    transform_cash_flow,
    transform_key_metrics,
    transform_financial_ratios,
    transform_price_data,
    transform_batch
)
from src.database.models import (
    IncomeStatement,
    BalanceSheet,
    CashFlow,
    KeyMetric,
    FinancialRatio,
    PriceDaily
)
from sqlalchemy.orm import Session


def insert_income_statements(session: Session, api_response_list: list):
    """
    Insert income statement data from API response

    Args:
        session: SQLAlchemy session
        api_response_list: List of income statement data from FMP API
    """
    # Transform all records at once
    transformed_data = transform_batch(api_response_list, transform_income_statement)

    # Create model instances
    for data in transformed_data:
        income_stmt = IncomeStatement(**data)
        session.merge(income_stmt)  # Use merge to update if exists

    session.commit()
    print(f"Inserted/Updated {len(transformed_data)} income statement records")


def insert_balance_sheets(session: Session, api_response_list: list):
    """Insert balance sheet data from API response"""
    transformed_data = transform_batch(api_response_list, transform_balance_sheet)

    for data in transformed_data:
        balance_sheet = BalanceSheet(**data)
        session.merge(balance_sheet)

    session.commit()
    print(f"Inserted/Updated {len(transformed_data)} balance sheet records")


def insert_cash_flows(session: Session, api_response_list: list):
    """Insert cash flow data from API response"""
    transformed_data = transform_batch(api_response_list, transform_cash_flow)

    for data in transformed_data:
        cash_flow = CashFlow(**data)
        session.merge(cash_flow)

    session.commit()
    print(f"Inserted/Updated {len(transformed_data)} cash flow records")


def insert_key_metrics(session: Session, api_response_list: list):
    """Insert key metrics data from API response"""
    transformed_data = transform_batch(api_response_list, transform_key_metrics)

    for data in transformed_data:
        key_metric = KeyMetric(**data)
        session.merge(key_metric)

    session.commit()
    print(f"Inserted/Updated {len(transformed_data)} key metric records")


def insert_financial_ratios(session: Session, api_response_list: list):
    """Insert financial ratios data from API response"""
    transformed_data = transform_batch(api_response_list, transform_financial_ratios)

    for data in transformed_data:
        ratio = FinancialRatio(**data)
        session.merge(ratio)

    session.commit()
    print(f"Inserted/Updated {len(transformed_data)} financial ratio records")


def insert_price_data(session: Session, api_response_list: list):
    """Insert daily price data from API response"""
    transformed_data = transform_batch(api_response_list, transform_price_data)

    for data in transformed_data:
        price = PriceDaily(**data)
        session.merge(price)

    session.commit()
    print(f"Inserted/Updated {len(transformed_data)} price records")


# Example usage with mock API data
if __name__ == "__main__":
    # Mock API response (this would come from your FMP API call)
    mock_api_response = [
        {
            "date": "2023-09-30",
            "symbol": "AAPL",
            "reportedCurrency": "USD",
            "cik": "0000320193",
            "fillingDate": "2023-11-03",
            "acceptedDate": "2023-11-02 18:08:27",
            "calendarYear": "2023",
            "period": "FY",
            "revenue": 383285000000,
            "costOfRevenue": 214137000000,
            "grossProfit": 169148000000,
            "grossProfitRatio": 0.441,
            "researchAndDevelopmentExpenses": 29915000000,
            "generalAndAdministrativeExpenses": 0,
            "sellingAndMarketingExpenses": 0,
            "sellingGeneralAndAdministrativeExpenses": 24932000000,
            "otherExpenses": 0,
            "operatingExpenses": 54847000000,
            "costAndExpenses": 268984000000,
            "interestIncome": 3750000000,
            "interestExpense": 3933000000,
            "depreciationAndAmortization": 11519000000,
            "ebitda": 125820000000,
            "ebitdaratio": 0.328,
            "operatingIncome": 114301000000,
            "operatingIncomeRatio": 0.298,
            "totalOtherIncomeExpensesNet": -565000000,
            "incomeBeforeTax": 113736000000,
            "incomeBeforeTaxRatio": 0.297,
            "incomeTaxExpense": 16741000000,
            "netIncome": 96995000000,
            "netIncomeRatio": 0.253,
            "eps": 6.16,
            "epsdiluted": 6.13,
            "weightedAverageShsOut": 15744231000,
            "weightedAverageShsOutDil": 15812547000
        }
    ]

    # Transform the data
    transformed = transform_batch(mock_api_response, transform_income_statement)

    print("Transformed data sample:")
    for key, value in list(transformed[0].items())[:10]:
        print(f"  {key}: {value}")

    # In your actual code, you would do:
    # from sqlalchemy import create_engine
    # from sqlalchemy.orm import sessionmaker
    #
    # engine = create_engine("postgresql://user:pass@localhost/db")
    # Session = sessionmaker(bind=engine)
    # session = Session()
    #
    # insert_income_statements(session, mock_api_response)
    # session.close()
