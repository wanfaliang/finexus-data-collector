
## Design Overview- 
Backend API: http://localhost:8001
  - Frontend (when we build it): http://localhost:3001 or http://localhost:5173

  🎉 Phase 1 Backend Complete!

  Here's what we've built:

  Backend Structure

  src/admin/
  ├── __init__.py
  ├── main.py                    # FastAPI app (port 8001)
  ├── core/
  │   ├── __init__.py
  │   └── database.py            # DB dependencies (reuses existing)
  ├── schemas/
  │   ├── __init__.py
  │   ├── freshness.py           # Pydantic response models
  │   └── quota.py               # Quota response models
  └── api/
      ├── __init__.py
      └── v1/
          ├── __init__.py        # Main router
          ├── freshness.py       # BLS freshness endpoints
          └── quota.py           # Quota tracking endpoints

  Available Endpoints

  Freshness:
  - GET /api/v1/freshness/overview - All surveys status
  - GET /api/v1/freshness/surveys/{code} - Single survey status
  - GET /api/v1/freshness/surveys/{code}/sentinels - Sentinel list
  - GET /api/v1/freshness/surveys/needs-update - Surveys needing update

  Quota:
  - GET /api/v1/quota/today - Today's usage
  - GET /api/v1/quota/history?days=7 - Historical usage
  - GET /api/v1/quota/breakdown - By survey/script
  - GET /api/v1/quota/logs - Usage log entries

  Test the Backend

  # Run the backend
  python -m src.admin.main

  # Or directly
  python src/admin/main.py

  Then visit:
  - API Docs: http://localhost:8001/api/docs
  - Health Check: http://localhost:8001/health

  Ready to test? 🚀

## Completed All the BLS Monitoring Endpoints

Excellent! 🎉 The Phase 1 backend API is now fully functional. All the core BLS monitoring endpoints are
  working:

  Completed:
  - ✅ /freshness/overview - Survey freshness dashboard data
  - ✅ /freshness/surveys/needs-update - Which surveys need updates (route order fixed!)
  - ✅ /freshness/surveys/{survey_code} - Individual survey details
  - ✅ /freshness/surveys/{survey_code}/sentinels - Sentinel series data
  - ✅ /quota/today - Today's quota usage
  - ✅ /quota/history - Multi-day trends
  - ✅ /quota/breakdown - Usage by survey and script
  - ✅ /quota/logs - Detailed usage logs

## Frontend Complete! 🎉

The React+TypeScript admin dashboard is now live at http://localhost:3001

**Completed Features:**
- ✅ Single-page compact dashboard combining survey freshness + API quota
- ✅ 6-column stats grid showing total surveys, current, need update, API used, remaining, usage %
- ✅ Survey table with separate Code and Name columns (efficient horizontal space usage)
- ✅ Real-time status indicators (Current/Needs Update/Updating) with color-coded chips
- ✅ Series counts displayed with proper formatting (e.g., "6,840" instead of "6840")
- ✅ Progress tracking for updating surveys (e.g., "100 / 6840 (50%)")
- ✅ Action buttons with tooltips ("Start full update")
- ✅ 7-Day API usage chart (full-width bar chart)
- ✅ Auto-refresh every 10 seconds
- ✅ Compact spacing (reduced vertical space, maximized horizontal space)
- ✅ Removed redundant navigation (single page design)

**Tech Stack:**
- React 18 + TypeScript + Vite
- Material-UI v7 (with custom compact theme)
- TanStack Query for data fetching
- Recharts for visualization
- React Router for navigation

**Fixed Issues:**
- ✅ Fixed series_total_count not populating in select_sentinels.py
- ✅ Removed MUI Grid API (deprecated) - using native CSS Grid
- ✅ Compact table design with py: 0.75 instead of stacked rows
- ✅ Combined Dashboard and Quota into single page

**Running the Full Stack:**
```bash
# Terminal 1 - Backend
python -m src.admin.main

# Terminal 2 - Frontend
cd frontend
npm run dev
```

Then visit http://localhost:3001 for the dashboard!

