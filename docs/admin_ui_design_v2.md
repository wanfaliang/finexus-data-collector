# FinExus Data Collector - Unified Admin UI Design v2

## Vision: Unified Multi-Source Data Collection Platform

A comprehensive admin interface for managing data collection from multiple sources with different characteristics, unified under a consistent UI.

## Data Sources & Their Characteristics

| Source | API Limit | Update Freq | Status Check | Current Implementation |
|--------|-----------|-------------|--------------|------------------------|
| **BLS** | 500 req/day | Varies (monthly/quarterly) | Sentinel system | ✅ Fully implemented |
| **Nasdaq** | Unknown | Daily | File availability | ✅ Screener collectors |
| **FRED** | 500 req/day | Varies by series | API check | ⏳ Needs integration |
| **FMP** | Varies by plan | Real-time/daily | API check | ✅ Financial data |
| **Census** | Varies | Annual/decennial | API check | ⏳ Planned |
| **Conference Board** | Manual | Monthly | Manual check | ⏳ Planned |
| **Shiller Data** | Manual | Quarterly | File check | ⏳ Planned |
| **UMich Surveys** | Manual | Monthly | Manual check | ⏳ Planned |
| **SEC EDGAR** | No limit | Real-time | Filing check | ⏳ Planned |

## Updated Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     React Frontend                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │Dashboard │  │  Sources │  │   Data   │  │   Jobs   │   │
│  │ (Home)   │  │(Collectors)│ │ Explorer │  │ Scheduler│   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Quota   │  │ Settings │  │ Pipelines│  │   Logs   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ REST API / WebSocket
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Backend                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Source  │  │   Job    │  │   Data   │  │Pipeline  │   │
│  │  Manager │  │Scheduler │  │  Query   │  │ Engine   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Source Adapters (Abstraction)              │
│  ┌────┐  ┌────┐  ┌────┐  ┌────┐  ┌────┐  ┌────┐  ┌────┐  │
│  │BLS │  │FRED│  │FMP │  │NSDQ│  │CENS│  │CONF│  │SEC │  │
│  └────┘  └────┘  └────┘  └────┘  └────┘  └────┘  └────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Core Abstraction: Data Source Interface

Each data source implements a common interface:

```python
class DataSourceInterface:
    """Abstract interface for all data sources"""

    # Metadata
    @property
    def source_id(self) -> str: ...
    @property
    def display_name(self) -> str: ...
    @property
    def icon(self) -> str: ...

    # Status & Health
    async def check_health(self) -> HealthStatus: ...
    async def get_quota_status(self) -> QuotaStatus: ...
    async def check_freshness(self) -> FreshnessStatus: ...

    # Collections
    async def list_collections(self) -> List[Collection]: ...
    async def get_collection_status(self, collection_id: str) -> CollectionStatus: ...

    # Operations
    async def trigger_collection(self, collection_id: str, params: dict) -> Job: ...
    async def cancel_job(self, job_id: str) -> bool: ...

    # Configuration
    async def get_config(self) -> dict: ...
    async def update_config(self, config: dict) -> bool: ...
```

## Implementation Phases (Prioritized)

### 🔥 Phase 1: Core BLS Admin (TOP PRIORITY - 2 weeks)

**Goal**: Complete BLS admin as designed in v1

**Backend**:
- FastAPI app with BLS endpoints
- Source adapter for BLS (implement interface)
- WebSocket for real-time updates

**Frontend**:
- Dashboard with BLS focus
- Surveys/Sources page (BLS only)
- Quota management
- Operations page
- Settings (BLS API key)

**Deliverable**: Fully functional BLS admin UI

### 📊 Phase 2: Multi-Source Support (2 weeks)

**Goal**: Add source abstraction and integrate existing collectors

**Backend**:
- Implement DataSourceInterface
- Create adapters for:
  - BLS (refactor existing)
  - Nasdaq (screeners)
  - FMP (financial data)
- Source registry/plugin system

**Frontend**:
- Multi-source dashboard
- Source selector/switcher
- Per-source quota displays
- Generic collection status

**Deliverable**: Unified UI supporting BLS, Nasdaq, FMP

### 🔍 Phase 3: Data Explorer Module (2 weeks)

**Goal**: Allow users to explore collected data without SQL

**Features**:
1. **Quick Stats Dashboard**:
   - Total records by source/table
   - Date ranges covered
   - Last updated timestamps
   - Data quality metrics

2. **Query Builder**:
   - Visual query builder (no SQL required)
   - Select source → Select dataset → Filter → Display
   - Example: "Show CPI for all areas in 2024"
   - Export to CSV/Excel

3. **Data Preview**:
   - Paginated table views
   - Column sorting/filtering
   - Quick statistics (min/max/avg)

4. **Basic Visualizations**:
   - Line charts for time series
   - Bar charts for comparisons
   - Geographic maps for area data
   - Interactive charts (zoom, pan)

5. **Saved Queries**:
   - Save frequently used queries
   - Share queries with team
   - Schedule query exports

**UI Mock**:
```
┌─────────────────────────────────────────────────────────────┐
│  Data Explorer                                  [Save Query] │
├─────────────────────────────────────────────────────────────┤
│  Source: [BLS ▼]  Dataset: [CU - CPI ▼]                    │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Filters:                                               │ │
│  │   Area: [All US ▼]  Item: [All items ▼]              │ │
│  │   Date Range: [2024-01-01] to [2024-12-31]           │ │
│  │   [+ Add Filter]                          [Apply]     │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Results: 12 records                         [Export CSV ▼] │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Date       │ Series    │ Value  │ Change │           │ │
│  ├────────────┼───────────┼────────┼────────┤           │ │
│  │ 2024-01-01 │ CUSR0000  │ 308.41 │ +0.3%  │           │ │
│  │ 2024-02-01 │ CUSR0000  │ 309.68 │ +0.4%  │           │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Visualization                              [Chart Type ▼]  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              CPI Over Time                             │ │
│  │   310 ┤                                           ╭─   │ │
│  │   309 ┤                                    ╭──────╯    │ │
│  │   308 ┤                             ╭──────╯           │ │
│  │       └────┴────┴────┴────┴────┴────┴────┴────        │ │
│  │       Jan  Feb  Mar  Apr  May  Jun  Jul  Aug          │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### ⏰ Phase 4: Job Scheduler & Pipelines (2 weeks)

**Goal**: Automated job execution and dependency management

**Features**:

1. **Job Scheduler**:
   - Cron-like scheduling
   - One-time jobs
   - Recurring jobs
   - Job dependencies
   - Retry policies

2. **Pipeline Builder**:
   - Visual pipeline editor
   - Drag-and-drop tasks
   - Define dependencies
   - Example pipeline:
     ```
     [Update Company List] → [Collect Financials] → [Calculate Ratios]
     ```

3. **Job Management**:
   - Job history and logs
   - Job status monitoring
   - Cancel/retry jobs
   - Job templates

**UI Mock**:
```
┌─────────────────────────────────────────────────────────────┐
│  Job Scheduler                         [+ New Job] [+ Pipeline]│
├─────────────────────────────────────────────────────────────┤
│  Active Jobs (3)            Scheduled (12)         History   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ [▶] Daily BLS Check                    Running 2m 15s  │ │
│  │     ├─ Check CU freshness              ✓ Complete      │ │
│  │     ├─ Check LA freshness              ⏳ Running      │ │
│  │     └─ Check CE freshness              ⏳ Pending      │ │
│  │                                                         │ │
│  │ [⏸] FMP Stock Prices                   Scheduled 9:00  │ │
│  │     Schedule: Daily at 09:00 EST                       │ │
│  │     Last run: 2h ago (Success)                         │ │
│  │                                                         │ │
│  │ [⚙] Update Company List → Financials  Scheduled 10:00 │ │
│  │     Pipeline (2 tasks)                                 │ │
│  │     Depends on: Market close                           │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Pipeline Editor                                            │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  ┌──────────────┐      ┌──────────────┐               │ │
│  │  │ Update       │  ──► │ Collect      │               │ │
│  │  │ Company List │      │ Financials   │               │ │
│  │  └──────────────┘      └──────────────┘               │ │
│  │         │                     │                         │ │
│  │         └──────────┬──────────┘                        │ │
│  │                    ▼                                    │ │
│  │            ┌──────────────┐                            │ │
│  │            │ Calculate    │                            │ │
│  │            │ Ratios       │                            │ │
│  │            └──────────────┘                            │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 🔧 Phase 5: Enhanced Configuration (1 week)

**Goal**: Flexible configuration for all sources

**Features**:

1. **Multiple API Keys**:
   - Support multiple keys per source
   - Key rotation/fallback
   - Per-key quota tracking
   - Key testing/validation

2. **Source-Specific Settings**:
   - BLS: Sentinel configuration, check frequency
   - FMP: Plan level, rate limits
   - Nasdaq: Download paths, file retention
   - FRED: Series selection, update frequency

3. **Global Settings**:
   - Database connection pooling
   - Logging levels
   - Notification preferences
   - Default year ranges

**UI Mock**:
```
┌─────────────────────────────────────────────────────────────┐
│  Settings                                                    │
├─────────────────────────────────────────────────────────────┤
│  ┌─ BLS Configuration ──────────────────────────────────┐   │
│  │  API Keys (Multiple)                 [+ Add Key]     │   │
│  │  ┌──────────────────────────────────────────────┐   │   │
│  │  │ Key 1: •••••••••••••••0c48  [Primary] [Test] │   │   │
│  │  │   Quota: 145/500 today                        │   │   │
│  │  │   Status: Active ✓                            │   │   │
│  │  │                                                │   │   │
│  │  │ Key 2: •••••••••••••••abc1  [Backup]  [Test] │   │   │
│  │  │   Quota: 0/500 today                          │   │   │
│  │  │   Status: Active ✓                            │   │   │
│  │  └──────────────────────────────────────────────┘   │   │
│  │                                                      │   │
│  │  Key Rotation: ● Round-robin  ○ Failover           │   │
│  │                                                      │   │
│  │  Sentinel Configuration:                            │   │
│  │    Sentinels per survey: [50]                       │   │
│  │    Check frequency: [6] hours                       │   │
│  │    Auto-update on detection: ○ Yes  ● No           │   │
│  └──────────────────────────────────────────────────┘   │
│                                                           │
│  ┌─ FMP Configuration ──────────────────────────────────┐  │
│  │  API Key: •••••••••••••••xyz9           [Test]      │  │
│  │  Plan: Professional                                  │  │
│  │  Rate Limit: 300 req/min                            │  │
│  │  Features: ✓ Real-time ✓ Historical ✓ Fundamentals │  │
│  └──────────────────────────────────────────────────┘   │
│                                                           │
│  ┌─ Global Settings ────────────────────────────────────┐  │
│  │  Database:                                           │  │
│  │    Pool Size: [20]  Max Overflow: [10]             │  │
│  │                                                      │  │
│  │  Logging:                                            │  │
│  │    Level: [INFO ▼]  Retain: [30] days              │  │
│  │                                                      │  │
│  │  Notifications: (Coming Soon)                        │  │
│  │    Email: □ Enable                                   │  │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 🔮 Phase 6: Future Enhancements (Planning)

To be implemented later:

1. **User Management**:
   - Multi-user support
   - Role-based access control (RBAC)
   - Activity audit logs
   - User preferences

2. **Developer Tools**:
   - API documentation (auto-generated)
   - API key management for external access
   - Webhook configuration
   - GraphQL endpoint (optional)

3. **Advanced Analytics**:
   - Data quality dashboards
   - Collection performance metrics
   - Cost/quota optimization suggestions
   - Anomaly detection

4. **Alerting System**:
   - Email/Slack/Discord notifications
   - Alert rules (quota thresholds, failures, freshness)
   - Alert history and management

## Updated Dashboard Design

The dashboard should provide a unified view across all sources:

```
┌─────────────────────────────────────────────────────────────┐
│  FinExus Data Collector                          🔄 Refresh │
├─────────────────────────────────────────────────────────────┤
│  System Overview                                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │ Sources  │ │   Jobs   │ │  Quota   │ │  Health  │      │
│  │   5/8    │ │  3 run   │ │ 65% used │ │   Good   │      │
│  │  active  │ │  12 sched│ │  today   │ │    ✓     │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
├─────────────────────────────────────────────────────────────┤
│  Data Sources Status                                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Source │ Status  │ Collections │ Last Run │ Quota  │   │
│  ├────────┼─────────┼─────────────┼──────────┼────────┤   │
│  │ BLS    │ ✓ Good  │ 12/18 curr  │ 2h ago   │145/500 │   │
│  │ FMP    │ ✓ Good  │ All current │ 30m ago  │45/300  │   │
│  │ Nasdaq │ ✓ Good  │ Daily done  │ 1h ago   │ N/A    │   │
│  │ FRED   │ ⚠ Warn  │ 5 need upd  │ 3d ago   │0/500   │   │
│  │ Census │ ○ Idle  │ Not sched   │ Never    │ N/A    │   │
│  └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  Active Jobs (3)                 Recent Activity            │
│  ┌──────────────────┐           ┌───────────────────────┐  │
│  │ BLS Check CU     │           │ 2h: BLS sentinel chk  │  │
│  │ ████████░░░ 85%  │           │ 3h: FMP prices update │  │
│  │                  │           │ 1d: Nasdaq screener   │  │
│  │ FMP Financials   │           │ 2d: BLS full update   │  │
│  │ ██░░░░░░░░  25%  │           └───────────────────────┘  │
│  └──────────────────┘                                       │
└─────────────────────────────────────────────────────────────┘
```

## API Structure (Multi-Source)

### Source Management
```python
GET  /api/v1/sources                     # List all sources
GET  /api/v1/sources/{source_id}         # Get source details
GET  /api/v1/sources/{source_id}/health  # Check source health
GET  /api/v1/sources/{source_id}/quota   # Get quota status

# Collections per source
GET  /api/v1/sources/{source_id}/collections
GET  /api/v1/sources/{source_id}/collections/{collection_id}
POST /api/v1/sources/{source_id}/collections/{collection_id}/trigger
```

### Data Explorer
```python
GET  /api/v1/data/sources                # List available data sources
GET  /api/v1/data/{source}/datasets      # List datasets in source
POST /api/v1/data/{source}/query         # Execute query
GET  /api/v1/data/queries/saved          # List saved queries
POST /api/v1/data/queries/save           # Save query
```

### Job Scheduler
```python
GET  /api/v1/jobs                        # List all jobs
POST /api/v1/jobs                        # Create job
GET  /api/v1/jobs/{job_id}               # Get job details
DELETE /api/v1/jobs/{job_id}             # Delete job
POST /api/v1/jobs/{job_id}/trigger       # Trigger job now
POST /api/v1/jobs/{job_id}/cancel        # Cancel running job

# Pipelines
GET  /api/v1/pipelines                   # List pipelines
POST /api/v1/pipelines                   # Create pipeline
GET  /api/v1/pipelines/{id}              # Get pipeline
POST /api/v1/pipelines/{id}/execute      # Execute pipeline
```

## Implementation Priority

### Immediate (Next 2 weeks)
✅ Phase 1: Core BLS Admin
- Your original v1 design
- Full BLS functionality
- Solid foundation

### Soon (Weeks 3-4)
📊 Phase 2: Multi-Source Support
- Source abstraction layer
- Integrate Nasdaq, FMP
- Unified dashboard

### Medium-term (Weeks 5-8)
🔍 Phase 3: Data Explorer
⏰ Phase 4: Job Scheduler

### Future
🔧 Phase 5: Enhanced Config
🔮 Phase 6: Advanced Features

## Technical Stack Confirmation

**Backend**:
- FastAPI (async support crucial for multi-source)
- SQLAlchemy (existing models)
- APScheduler (for job scheduling)
- Celery (optional, for complex pipelines)

**Frontend**:
- React + TypeScript
- Material-UI or Ant Design (consistent components)
- React Query (server state)
- Recharts (visualizations)
- React Router (navigation)

## Summary

This design:
1. ✅ **Prioritizes** your core BLS admin (Phase 1)
2. ✅ **Abstracts** for multi-source support
3. ✅ **Addresses** your concerns:
   - Multiple API keys ✓
   - Data exploration ✓
   - Job scheduling ✓
   - Pipeline dependencies ✓
   - Future extensibility ✓
4. ✅ **Phases** implementation logically
5. ✅ **Maintains** focus on immediate needs

**Next step**: Implement Phase 1 (Core BLS Admin) as designed in v1, with the source abstraction in mind for easy Phase 2 integration.

Ready to start building! 🚀
