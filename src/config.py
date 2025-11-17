"""
Configuration Management for FinExus Data Collector
Uses Pydantic Settings for type-safe configuration with environment variable support
"""
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database configuration"""
    url: str = Field(..., alias='DATABASE_URL')
    pool_size: int = Field(20, alias='DB_POOL_SIZE')
    max_overflow: int = Field(10, alias='DB_MAX_OVERFLOW')
    echo: bool = Field(False, alias='DB_ECHO')
    pool_pre_ping: bool = True
    pool_recycle: int = 3600
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class APISettings(BaseSettings):
    """API configuration"""
    fmp_api_key: str = Field(..., alias='FMP_API_KEY')
    fred_api_key: str = Field(..., alias='FRED_API_KEY')
    bls_api_key: str = Field(..., alias='BLS_API_KEY')
    census_api_key: str = Field(..., alias='CENSUS_API_KEY')

    sleep_sec: float = Field(0.2, alias='API_SLEEP_SEC')
    timeout: int = Field(30, alias='API_TIMEOUT')
    retries: int = Field(3, alias='API_RETRIES')
    backoff: float = Field(0.7, alias='API_BACKOFF')

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class DataCollectionSettings(BaseSettings):
    """Data collection configuration"""
    default_years_history: int = Field(10, alias='DEFAULT_YEARS_HISTORY')
    batch_size: int = Field(100, alias='BATCH_SIZE')
    max_workers: int = Field(5, alias='MAX_WORKERS')

    include_analyst_data: bool = Field(True, alias='INCLUDE_ANALYST_DATA')
    include_institutional_data: bool = Field(True, alias='INCLUDE_INSTITUTIONAL_DATA')
    include_insider_data: bool = Field(True, alias='INCLUDE_INSIDER_DATA')
    include_sp500: bool = Field(True, alias='INCLUDE_SP500')
    include_economic_data: bool = Field(True, alias='INCLUDE_ECONOMIC_DATA')

    # Bulk data settings
    bulk_data_path: str = Field('data/bulk_csv', alias='BULK_DATA_PATH')
    nasdaq_screener_path: str = Field('data/nasdaq_screener', alias='NASDAQ_SCREENER_PATH')
    nasdaq_etf_screener_path: str = Field('data/nasdaq_etf_screener', alias='NASDAQ_ETF_SCREENER_PATH')

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class ScheduleSettings(BaseSettings):
    """Job scheduling configuration"""
    schedule_daily_prices: str = Field('0 18 * * 1-5', alias='SCHEDULE_DAILY_PRICES')
    schedule_financials: str = Field('0 19 * * *', alias='SCHEDULE_FINANCIALS')
    schedule_economic: str = Field('0 8 * * *', alias='SCHEDULE_ECONOMIC')
    schedule_analyst: str = Field('0 10 * * *', alias='SCHEDULE_ANALYST')
    schedule_insider: str = Field('0 11 * * *', alias='SCHEDULE_INSIDER')
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class ValidationSettings(BaseSettings):
    """Data validation configuration"""
    enable_data_validation: bool = Field(True, alias='ENABLE_DATA_VALIDATION')
    alert_on_stale_data_days: int = Field(7, alias='ALERT_ON_STALE_DATA_DAYS')
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class MonitoringSettings(BaseSettings):
    """Monitoring and metrics configuration"""
    enable_metrics: bool = Field(True, alias='ENABLE_METRICS')
    metrics_port: int = Field(9090, alias='METRICS_PORT')
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class AppSettings(BaseSettings):
    """Main application settings"""
    environment: str = Field('development', alias='ENVIRONMENT')
    log_level: str = Field('INFO', alias='LOG_LEVEL')
    log_file_path: str = Field('logs/finexus_collector.log', alias='LOG_FILE_PATH')
    
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        v = v.upper()
        if v not in valid_levels:
            raise ValueError(f'log_level must be one of {valid_levels}')
        return v
    
    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v: str) -> str:
        valid_envs = ['development', 'staging', 'production']
        v = v.lower()
        if v not in valid_envs:
            raise ValueError(f'environment must be one of {valid_envs}')
        return v
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')


class Settings:
    """Centralized settings manager"""
    _database: Optional[DatabaseSettings] = None
    _api: Optional[APISettings] = None
    _data_collection: Optional[DataCollectionSettings] = None
    _schedule: Optional[ScheduleSettings] = None
    _validation: Optional[ValidationSettings] = None
    _monitoring: Optional[MonitoringSettings] = None
    _app: Optional[AppSettings] = None
    
    @property
    def database(self) -> DatabaseSettings:
        if self._database is None:
            self._database = DatabaseSettings() # type: ignore
        return self._database
    
    @property
    def api(self) -> APISettings:
        if self._api is None:
            self._api = APISettings() # type: ignore
        return self._api
    
    @property
    def data_collection(self) -> DataCollectionSettings:
        if self._data_collection is None:
            self._data_collection = DataCollectionSettings() # type: ignore
        return self._data_collection
    
    @property
    def schedule(self) -> ScheduleSettings:
        if self._schedule is None:
            self._schedule = ScheduleSettings() # type: ignore
        return self._schedule
    
    @property
    def validation(self) -> ValidationSettings:
        if self._validation is None:
            self._validation = ValidationSettings() # type: ignore
        return self._validation
    
    @property
    def monitoring(self) -> MonitoringSettings:
        if self._monitoring is None:
            self._monitoring = MonitoringSettings() # type: ignore
        return self._monitoring
    
    @property
    def app(self) -> AppSettings:
        if self._app is None:
            self._app = AppSettings() # type: ignore
        return self._app


# Global settings instance
settings = Settings()

# API Endpoints
FMP_ENDPOINTS = {
    "profile": "https://financialmodelingprep.com/stable/profile",
    "income_statement": "https://financialmodelingprep.com/stable/income-statement",
    "balance_sheet": "https://financialmodelingprep.com/stable/balance-sheet-statement",
    "cash_flow": "https://financialmodelingprep.com/stable/cash-flow-statement",
    "ratios": "https://financialmodelingprep.com/stable/ratios",
    "key_metrics": "https://financialmodelingprep.com/stable/key-metrics",
    "enterprise_values": "https://financialmodelingprep.com/stable/enterprise-values",
    "employee_history": "https://financialmodelingprep.com/stable/historical-employee-count",
    "prices_full": "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted",
    "analyst_estimates": "https://financialmodelingprep.com/stable/analyst-estimates",
    "price_target_consensus": "https://financialmodelingprep.com/stable/price-target-consensus",
    "insider_trading_search": "https://financialmodelingprep.com/stable/insider-trading/search",
    "institutional_ownership_summary": "https://financialmodelingprep.com/stable/institutional-ownership/symbol-positions-summary",
    "institutional_13f_extract": "https://financialmodelingprep.com/stable/institutional-ownership/extract",
    "insider_trading_statistics": "https://financialmodelingprep.com/stable/insider-trading/statistics",
    "economic_calendar": "https://financialmodelingprep.com/stable/economic-calendar",
    "earnings_calendar": "https://financialmodelingprep.com/stable/earnings-calendar",
    "key_metrics_ttm_bulk": "https://financialmodelingprep.com/stable/key-metrics-ttm-bulk",
    "ratios_ttm_bulk": "https://financialmodelingprep.com/stable/ratios-ttm-bulk",
    "price_target_summary_bulk": "https://financialmodelingprep.com/stable/price-target-summary-bulk",
    "company_profile_bulk": "https://financialmodelingprep.com/stable/profile-bulk",
}
