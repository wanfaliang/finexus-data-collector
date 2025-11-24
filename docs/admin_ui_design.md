# BLS Data Collector - Admin UI Design

## Technology Stack

**Backend**: FastAPI (Python)
- Fast, modern API framework
- Automatic OpenAPI/Swagger documentation
- Type hints and validation with Pydantic
- Async support for efficient database queries

**Frontend**: React (TypeScript)
- Component-based UI
- TypeScript for type safety
- State management: React Query for server state
- UI Framework: Material-UI (MUI) or Ant Design
- Charts: Recharts or Apache ECharts

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     React Frontend                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │Dashboard │  │ Surveys  │  │  Quota   │  │ Settings │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ REST API / WebSocket
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Backend                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  API     │  │  BLS     │  │ Database │  │ WebSocket│   │
│  │ Routes   │  │ Service  │  │  Layer   │  │ Events   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  PostgreSQL Database                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ BLS Tracking │  │  BLS Data    │  │  Sentinels   │      │
│  │   Tables     │  │   Tables     │  │   Tables     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. Dashboard (Home)
**Purpose**: At-a-glance overview of system health and status

**Components**:
- **Status Cards**:
  - API Quota: Used/Remaining today (with progress bar)
  - Surveys: Total / Needs Update / Updating / Current
  - Last Check: Time since last freshness check
  - Data Freshness: Surveys with stale data

- **Freshness Overview Table**:
  - Survey code and name
  - Status badge (Current / Needs Update / Updating)
  - Last BLS update detected
  - Last sentinel check
  - Quick actions (Check Now / Update / View Details)

- **Recent Activity Timeline**:
  - Last 10 operations (checks, updates, errors)
  - Timestamp, survey, action, result

- **API Usage Chart**:
  - Daily quota usage over last 7 days
  - Line chart with daily limit reference line

**Mock Layout**:
```
┌─────────────────────────────────────────────────────────┐
│  BLS Data Collector Admin                    🔄 Refresh │
├─────────────────────────────────────────────────────────┤
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐      │
│  │ API     │ │ Surveys │ │  Last   │ │  Stale  │      │
│  │ Quota   │ │ Status  │ │  Check  │ │  Data   │      │
│  │ 145/500 │ │ 12/18 ✓ │ │ 2h ago  │ │   3     │      │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘      │
├─────────────────────────────────────────────────────────┤
│  Survey Freshness Status                                │
│  ┌─────┬──────────┬─────────┬──────────┬─────────┐    │
│  │Code │ Status   │ BLS Upd │ Checked  │ Actions │    │
│  ├─────┼──────────┼─────────┼──────────┼─────────┤    │
│  │ CU  │ ✓ Current│ 2d ago  │ 2h ago   │ ⚙️ 🔍   │    │
│  │ LA  │ ! Update │ 1d ago  │ 30m ago  │ ⚙️ 🔍   │    │
│  │ CE  │ ⏳ 45%   │ 3d ago  │ 1h ago   │ ⚙️ 🔍   │    │
│  └─────┴──────────┴─────────┴──────────┴─────────┘    │
├─────────────────────────────────────────────────────────┤
│  API Usage (Last 7 Days)        Recent Activity        │
│  ┌──────────────────┐           ┌───────────────────┐  │
│  │      📊          │           │ 2h ago: Check CU  │  │
│  │   Usage Chart    │           │ 3h ago: Update LA │  │
│  │                  │           │ 5h ago: Check All │  │
│  └──────────────────┘           └───────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 2. Surveys Page
**Purpose**: Detailed view and management of individual surveys

**Components**:
- **Survey List** (left sidebar):
  - Filterable/searchable
  - Status indicators
  - Click to view details

- **Survey Detail Panel** (main area):
  - Survey metadata (name, code, series count)
  - Freshness status:
    - Last BLS update detected
    - Last sentinel check
    - Sentinels changed (last check)
    - Update frequency statistics
  - Update status:
    - Last full update (start/end times)
    - Progress if in-progress
    - Series updated count
  - Sentinel list (expandable):
    - Series ID
    - Last value, year, period
    - Check count / change count
    - Last changed timestamp
  - Action buttons:
    - Check Freshness Now
    - Trigger Full Update
    - Configure Sentinels (re-select)
    - View Update History

**Mock Layout**:
```
┌─────────────────────────────────────────────────────────┐
│  Surveys                                                 │
├──────────┬──────────────────────────────────────────────┤
│ Filters  │  Survey: CU - Consumer Price Index           │
│ ────     │  ┌────────────────────────────────────────┐  │
│ □ Current│  │ Freshness Status                       │  │
│ ☑ Update │  │  Last BLS Update: Jan 15, 2025 (2d)   │  │
│ □ Active │  │  Last Checked: Jan 17 14:30 (2h ago)   │  │
│ ──────── │  │  Sentinels Changed: 0/50               │  │
│ Search   │  │  Update Frequency: ~7.2 days           │  │
│ [......] │  └────────────────────────────────────────┘  │
│ ──────── │  ┌────────────────────────────────────────┐  │
│ CU  !    │  │ Update Status                          │  │
│ LA  ✓    │  │  Last Update: Jan 15, 2025             │  │
│ CE  ✓    │  │  Series Updated: 6,840 / 6,840         │  │
│ AP  ✓    │  │  Duration: 2.5 hours                   │  │
│ PC  !    │  └────────────────────────────────────────┘  │
│ WP  ✓    │  ┌────────────────────────────────────────┐  │
│ ...      │  │ Sentinels (50)              ▼ Expand   │  │
│          │  │  CUSR0000SA0 - Checks: 45, Changes: 12 │  │
│          │  │  CUSR0000SAF - Checks: 45, Changes: 8  │  │
│          │  │  ...                                    │  │
│          │  └────────────────────────────────────────┘  │
│          │  [Check Now] [Update] [Configure]          │
└──────────┴──────────────────────────────────────────────┘
```

### 3. Quota Management Page
**Purpose**: Monitor and manage API quota usage

**Components**:
- **Daily Quota Card**:
  - Used / Remaining
  - Progress bar
  - Estimated series capacity remaining

- **Usage Breakdown** (Today):
  - By survey (pie chart or bar chart)
  - By script (check_freshness, universal_update, etc.)
  - Timeline chart (hourly usage)

- **Historical Usage**:
  - Last 30 days chart
  - Average daily usage
  - Peak usage days
  - Quota exhaustion events

- **Usage Log Table**:
  - Timestamp
  - Survey
  - Script name
  - Requests used
  - Series count
  - Filterable by date range, survey, script

- **Quota Settings** (future):
  - Daily limit override
  - Alert thresholds
  - Auto-pause settings

**Mock Layout**:
```
┌─────────────────────────────────────────────────────────┐
│  API Quota Management                    Today: Jan 17  │
├─────────────────────────────────────────────────────────┤
│  Daily Quota                                            │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Used: 145 / 500 (29%)                           │  │
│  │  ████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  │  │
│  │  Remaining: 355 requests (~17,750 series)        │  │
│  └──────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────┤
│  Today's Usage Breakdown                                │
│  ┌──────────────────┐  ┌──────────────────────────┐   │
│  │   By Survey      │  │   By Script              │   │
│  │   ┌────────┐     │  │   ┌────────────────┐     │   │
│  │   │ 📊 Pie │     │  │   │ check_freshness│     │   │
│  │   │ Chart  │     │  │   │ universal_upd  │     │   │
│  │   └────────┘     │  │   └────────────────┘     │   │
│  └──────────────────┘  └──────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  Usage History (Last 30 Days)                           │
│  ┌──────────────────────────────────────────────────┐  │
│  │              📈 Line Chart                        │  │
│  │                                                   │  │
│  └──────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────┤
│  Usage Log                          [Export CSV]        │
│  ┌────────┬────────┬────────┬──────┬────────────┐      │
│  │ Time   │ Survey │ Script │ Reqs │ Series     │      │
│  ├────────┼────────┼────────┼──────┼────────────┤      │
│  │ 14:30  │ CU     │ check  │  1   │ 50         │      │
│  │ 12:00  │ LA     │ update │ 45   │ 2,250      │      │
│  └────────┴────────┴────────┴──────┴────────────┘      │
└─────────────────────────────────────────────────────────┘
```

### 4. Operations Page
**Purpose**: Trigger and monitor data collection operations

**Components**:
- **Quick Actions Panel**:
  - Check Freshness (all surveys or selected)
  - Update Fresh Surveys (--fresh-only)
  - Force Update (selected surveys)
  - Select Sentinels (all or selected)

- **Operation Configuration**:
  - Survey selection (checkboxes)
  - Year range
  - Options (force, limit, etc.)
  - Estimated API requests

- **Active Operations** (live updates via WebSocket):
  - Currently running operations
  - Progress bars
  - Cancel button
  - Real-time log output

- **Operation History**:
  - Recent operations (last 50)
  - Status (completed, failed, cancelled)
  - Duration, series updated, observations
  - Error details (expandable)

**Mock Layout**:
```
┌─────────────────────────────────────────────────────────┐
│  Operations                                              │
├─────────────────────────────────────────────────────────┤
│  Quick Actions                                           │
│  [Check Freshness] [Update Fresh] [Force Update]        │
├─────────────────────────────────────────────────────────┤
│  Operation Configuration                                 │
│  Surveys: ☑ CU  □ LA  ☑ CE  □ AP  ... [Select All]     │
│  Year Range: [2024] to [2025]                           │
│  Options: □ Force  □ Verbose  Limit: [____]            │
│  Estimated: 28 requests (~1,400 series)                 │
│                                    [Run Operation ▶]    │
├─────────────────────────────────────────────────────────┤
│  Active Operations                                       │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Update CU Survey                        [Cancel] │  │
│  │ Progress: 3,250 / 6,840 series (47%)             │  │
│  │ ███████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  │  │
│  │ Batch 65/137: Fetching series 3,251-3,300...     │  │
│  │   Saved 1,240 observations                       │  │
│  └──────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────┤
│  Recent Operations                                       │
│  ┌────────┬────────┬────────┬────────┬──────────┐      │
│  │ Time   │ Type   │ Status │ Series │ Duration │      │
│  ├────────┼────────┼────────┼────────┼──────────┤      │
│  │ 14:30  │ Check  │ ✓ Done │ 900    │ 2m 15s   │      │
│  │ 12:00  │ Update │ ✓ Done │ 2,250  │ 45m 30s  │      │
│  │ 08:00  │ Check  │ ✗ Fail │ 0      │ 5s       │      │
│  └────────┴────────┴────────┴────────┴──────────┘      │
└─────────────────────────────────────────────────────────┘
```

### 5. Settings Page
**Purpose**: Configure system behavior and preferences

**Components**:
- **API Configuration**:
  - BLS API key (masked)
  - Daily quota limit
  - Request timeout
  - Test connection button

- **Sentinel Configuration**:
  - Sentinels per survey (default: 50)
  - Auto-selection strategy
  - Check frequency (default: 6 hours)

- **Update Configuration**:
  - Default year range
  - Auto-update on freshness detection (on/off)
  - Batch size (default: 50)

- **Notification Settings** (future):
  - Email alerts on quota exhaustion
  - Slack/Discord webhooks
  - Alert thresholds

- **Database Connection**:
  - Connection string (masked)
  - Connection pool size
  - Test connection button

**Mock Layout**:
```
┌─────────────────────────────────────────────────────────┐
│  Settings                                    [Save]      │
├─────────────────────────────────────────────────────────┤
│  API Configuration                                       │
│  API Key: [••••••••••••••••••••••••0c48] [Test]        │
│  Daily Limit: [500] requests                            │
│  Timeout: [30] seconds                                  │
├─────────────────────────────────────────────────────────┤
│  Sentinel Configuration                                  │
│  Sentinels per Survey: [50]                             │
│  Check Frequency: [6] hours                             │
│  Selection Strategy:                                     │
│    □ 20 National/Aggregate                              │
│    □ 20 Geographic Diversity                            │
│    □ 10 Random Sampling                                 │
├─────────────────────────────────────────────────────────┤
│  Update Configuration                                    │
│  Default Year Range: Last [2] years                     │
│  Batch Size: [50] series                                │
│  Auto-update on Detection: ○ Yes  ● No                  │
├─────────────────────────────────────────────────────────┤
│  Notifications (Coming Soon)                            │
│  Email Alerts: □ Enable                                 │
│  Webhook URL: [_______________________________]         │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints Design

### Freshness API
```python
GET  /api/v1/surveys                     # List all surveys with freshness status
GET  /api/v1/surveys/{code}              # Get survey details
GET  /api/v1/surveys/{code}/sentinels    # Get sentinel list
POST /api/v1/surveys/{code}/check        # Trigger freshness check
POST /api/v1/surveys/{code}/update       # Trigger update
POST /api/v1/surveys/{code}/select-sentinels  # Re-select sentinels

GET  /api/v1/freshness/overview          # Dashboard overview data
POST /api/v1/freshness/check-all         # Check all surveys
POST /api/v1/freshness/update-fresh      # Update fresh surveys (--fresh-only)
```

### Quota API
```python
GET  /api/v1/quota/today                 # Today's quota usage
GET  /api/v1/quota/history               # Historical usage
GET  /api/v1/quota/breakdown             # Usage breakdown (by survey/script)
GET  /api/v1/quota/logs                  # Usage log entries
```

### Operations API
```python
POST /api/v1/operations/check            # Trigger check operation
POST /api/v1/operations/update           # Trigger update operation
GET  /api/v1/operations/active           # Get active operations
GET  /api/v1/operations/history          # Get operation history
POST /api/v1/operations/{id}/cancel      # Cancel operation
```

### Settings API
```python
GET  /api/v1/settings                    # Get all settings
PUT  /api/v1/settings                    # Update settings
POST /api/v1/settings/test-connection    # Test BLS API connection
```

### WebSocket API
```python
WS   /api/v1/ws/operations               # Real-time operation updates
WS   /api/v1/ws/freshness                # Real-time freshness changes
```

## Data Models (Pydantic)

### Response Models
```python
class SurveyFreshnessResponse(BaseModel):
    survey_code: str
    survey_name: str
    status: str  # "current" | "needs_update" | "updating"
    last_bls_update: Optional[datetime]
    last_check: Optional[datetime]
    sentinels_changed: int
    sentinels_total: int
    update_frequency_days: Optional[float]
    update_progress: Optional[float]  # 0.0 to 1.0
    series_updated: int
    series_total: int

class QuotaUsageResponse(BaseModel):
    date: date
    used: int
    limit: int
    remaining: int
    by_survey: Dict[str, int]
    by_script: Dict[str, int]

class SentinelResponse(BaseModel):
    series_id: str
    sentinel_order: int
    last_value: Optional[Decimal]
    last_year: Optional[int]
    last_period: Optional[str]
    check_count: int
    change_count: int
    last_changed: Optional[datetime]

class OperationResponse(BaseModel):
    id: str
    type: str  # "check" | "update" | "select"
    status: str  # "running" | "completed" | "failed"
    surveys: List[str]
    started_at: datetime
    completed_at: Optional[datetime]
    series_updated: int
    observations: int
    error: Optional[str]
```

### Request Models
```python
class CheckFreshnessRequest(BaseModel):
    surveys: Optional[List[str]] = None  # None = all
    verbose: bool = False

class UpdateRequest(BaseModel):
    surveys: List[str]
    start_year: Optional[int]
    end_year: Optional[int]
    force: bool = False

class SelectSentinelsRequest(BaseModel):
    surveys: List[str]
    force: bool = False
```

## Technology Choices

### Frontend Libraries
```json
{
  "dependencies": {
    "react": "^18.2.0",
    "react-router-dom": "^6.20.0",
    "@mui/material": "^5.15.0",
    "@mui/icons-material": "^5.15.0",
    "@tanstack/react-query": "^5.15.0",
    "recharts": "^2.10.0",
    "axios": "^1.6.0",
    "date-fns": "^3.0.0",
    "react-hook-form": "^7.49.0"
  }
}
```

### Backend Libraries
```python
# requirements-admin.txt
fastapi==0.109.0
uvicorn[standard]==0.27.0
pydantic==2.5.0
sqlalchemy==2.0.25
asyncpg==0.29.0  # async PostgreSQL driver
websockets==12.0
python-multipart==0.0.6
pydantic-settings==2.1.0
```

## Development Phases

### Phase 1: Backend Foundation (Week 1)
- [ ] Set up FastAPI project structure
- [ ] Create API routes for freshness endpoints
- [ ] Implement quota endpoints
- [ ] Add CORS configuration
- [ ] Set up async database queries
- [ ] Test with Swagger UI

### Phase 2: Frontend Foundation (Week 1-2)
- [ ] Set up React project with Vite
- [ ] Configure routing
- [ ] Set up React Query
- [ ] Create layout and navigation
- [ ] Implement Dashboard page (basic)
- [ ] Test API integration

### Phase 3: Core Features (Week 2-3)
- [ ] Complete Dashboard with all cards
- [ ] Implement Surveys page
- [ ] Implement Quota page
- [ ] Add charts and visualizations
- [ ] Polish UI/UX

### Phase 4: Operations & Real-time (Week 3-4)
- [ ] Implement Operations page
- [ ] Add WebSocket support
- [ ] Real-time operation monitoring
- [ ] Operation history and logs

### Phase 5: Settings & Polish (Week 4)
- [ ] Settings page
- [ ] Configuration management
- [ ] Error handling and validation
- [ ] Loading states and skeletons
- [ ] Responsive design
- [ ] Documentation

## Deployment

### Development
```bash
# Backend
cd admin-api
uvicorn app.main:app --reload --port 8000

# Frontend
cd admin-ui
npm run dev
```

### Production
```bash
# Backend (systemd service)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

# Frontend (build and serve with nginx)
npm run build
# Serve dist/ with nginx
```

### Docker Compose (Recommended)
```yaml
version: '3.8'
services:
  admin-api:
    build: ./admin-api
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://...
      - BLS_API_KEY=...

  admin-ui:
    build: ./admin-ui
    ports:
      - "3000:80"
    depends_on:
      - admin-api
```

## Security Considerations

1. **Authentication** (Phase 2+):
   - JWT-based auth
   - Login page
   - Protected routes

2. **API Security**:
   - Rate limiting
   - CORS configuration
   - Input validation
   - SQL injection prevention (via SQLAlchemy)

3. **Secrets Management**:
   - Environment variables
   - No API keys in frontend
   - Secure settings storage

## Next Steps

1. **Immediate**: Set up FastAPI project structure
2. **Next**: Create core API endpoints
3. **Then**: Set up React project
4. **Finally**: Implement Dashboard

Ready to start implementation! 🚀
