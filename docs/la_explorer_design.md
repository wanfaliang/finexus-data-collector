# LA Explorer Design

## Data Understanding

### LA Survey Overview
- **Coverage**: ~8,300 geographic areas including states, metros, counties, cities
- **Source**: Local Area Unemployment Statistics from Current Population Survey
- **Geographic Levels**: States (50+), Metro areas (~400), Counties (~3,100), Cities

### Measures Available
- `03`: Unemployment Rate (%)
- `04`: Unemployment (count in thousands)
- `05`: Employment (count in thousands)
- `06`: Labor Force (count in thousands)
- `07`: Employment-Population Ratio (%)
- `08`: Labor Force Participation Rate (%)
- `09`: Civilian Noninstitutional Population (thousands) - states only

### Area Types
- `A`: Statewide (50 states + DC + territories)
- `B`: Metropolitan Statistical Areas
- `C`: Metropolitan Divisions
- `D`: Micropolitan Areas
- `E`: Combined Areas
- `F`: Counties and Equivalents
- `G`: Cities over 25,000 population

### Seasonal Adjustment
- `S`: Seasonally Adjusted (available for states and major metros)
- `U`: Not Seasonally Adjusted (all areas)

## Explorer Sections

### 1. Overview Section
**Purpose**: National unemployment trends and key metrics

**API Endpoints**:
- `GET /api/v1/explorer/la/overview` - Latest national unemployment metrics
- `GET /api/v1/explorer/la/overview/timeline` - Historical national trends

**Data**:
- US National unemployment rate (series: LASST000000000000003 or similar)
- US National labor force
- US National employment
- Timeline with M/M and Y/Y changes

**Visualization**:
- Key metric cards (unemployment rate, labor force, employment)
- Interactive timeline chart with clickable month dots (CU/LN pattern)
- Time range selector (12/24/60 months)

### 2. State Analysis Section
**Purpose**: Compare unemployment across all 50 states

**API Endpoints**:
- `GET /api/v1/explorer/la/states` - Latest data for all states
- `GET /api/v1/explorer/la/states/timeline` - Historical state data
- `GET /api/v1/explorer/la/states/{state_code}` - Detailed state data

**Data**:
- All state unemployment rates (area_type_code = 'A')
- State rankings (highest/lowest unemployment)
- Use seasonally adjusted data where available (seasonal_code = 'S')

**Visualization**:
- **Choropleth Map**: US map colored by unemployment rate
  - Color scale: green (low) → yellow → red (high)
  - Clickable states to drill down
  - Tooltip showing state name and rate
- **Data Table**: Sortable state list with unemployment, labor force, employment
- **Timeline Chart**: Multi-line chart showing top/bottom states over time
- Interactive timeline selector (CU/LN pattern)

### 3. Metro Areas Section
**Purpose**: Analyze unemployment in metropolitan areas

**API Endpoints**:
- `GET /api/v1/explorer/la/metros` - Latest data for metro areas
- `GET /api/v1/explorer/la/metros/timeline` - Historical metro data
- `GET /api/v1/explorer/la/metros/{metro_code}` - Detailed metro data

**Data**:
- Major metropolitan areas (area_type_code = 'B')
- Filter: top 100 metros by population or all metros
- Use not seasonally adjusted (seasonal_code = 'U') as not all have seasonal adjustment

**Visualization**:
- **Point/Marker Map**: US map with metro areas as points
  - Point size: scaled by labor force size
  - Point color: unemployment rate (green → yellow → red)
  - Tooltip: metro name, unemployment rate, labor force
  - Clickable for details
- **Data Table**: Sortable metro list
- **Timeline Chart**: Selected metros comparison over time
- Interactive timeline selector

### 4. Series Detail Explorer
**Purpose**: Browse and visualize individual LA series

**API Endpoints**:
- `GET /api/v1/explorer/la/series` - Search/filter series catalog
- `GET /api/v1/explorer/la/series/{series_id}` - Individual series data
- `GET /api/v1/explorer/la/series/{series_id}/timeline` - Series historical data

**Filters**:
- Area Type (state, metro, county, city)
- Measure (unemployment rate, labor force, employment, etc.)
- Seasonal Adjustment (S/U)
- Geographic search (by state, metro name)

**Visualization**:
- Series browser with filters
- Multi-series comparison charts
- Individual series detail view

## Backend API Design

### Schema Models (`src/admin/schemas/la_explorer.py`)

```python
class UnemploymentMetric(BaseModel):
    area_code: str
    area_name: str
    area_type: str
    unemployment_rate: Optional[float]
    unemployment_level: Optional[int]
    employment_level: Optional[int]
    labor_force: Optional[int]
    latest_date: str
    month_over_month: Optional[float]  # percentage points
    year_over_year: Optional[float]

class LAOverviewResponse(BaseModel):
    survey_code: str = "LA"
    national_unemployment: UnemploymentMetric
    last_updated: str

class LAStateAnalysisResponse(BaseModel):
    survey_code: str = "LA"
    states: List[UnemploymentMetric]
    rankings: dict  # highest/lowest states

class LAMetroAnalysisResponse(BaseModel):
    survey_code: str = "LA"
    metros: List[UnemploymentMetric]

class TimelinePoint(BaseModel):
    year: int
    period: str
    period_name: str
    value: float

class LATimelineResponse(BaseModel):
    survey_code: str = "LA"
    area_name: str
    timeline: List[TimelinePoint]
```

### Endpoint Implementation

**Key Series IDs to identify**:
- US National unemployment rate (not seasonally adjusted)
- All 50 states unemployment rates (seasonally adjusted preferred)
- Major metro areas unemployment rates

**Data Retrieval Pattern**:
```python
# Get latest data
latest_data = db.query(LAData).\
    join(LASeries).\
    filter(LASeries.measure_code == '03').\  # unemployment rate
    filter(LASeries.area_type_code == 'A').\  # states
    filter(LASeries.seasonal_code == 'S').\   # seasonally adjusted
    order_by(LAData.year.desc(), LAData.period.desc()).\
    first()

# Calculate M/M and Y/Y changes
# Similar to LN Explorer pattern
```

## Frontend Design

### Map Component
- Use Recharts or simple SVG-based US map
- For metro points: overlay markers on US map
- Simpler than CU's choropleth (just state boundaries with colors + metro dots)

### Timeline Pattern
- Reuse TimelineSelector component from LN Explorer
- Clickable month dots
- Selected period updates maps and tables
- M/M and Y/Y only shown for latest period

### Layout Structure
```
LA Explorer
├── Overview Section
│   ├── Key Metrics Cards
│   ├── Timeline Chart
│   └── Timeline Selector
├── State Analysis
│   ├── US Choropleth Map
│   ├── State Rankings Table
│   ├── Timeline Chart
│   └── Timeline Selector
├── Metro Areas
│   ├── US Point Map
│   ├── Metro Table
│   ├── Timeline Chart
│   └── Timeline Selector
└── Series Detail Explorer
    ├── Filter Panel
    ├── Series List
    └── Chart Viewer
```

## Implementation Notes

1. **Seasonal Adjustment Strategy**:
   - Use seasonally adjusted (S) for states when available
   - Use not seasonally adjusted (U) for metros and counties (most don't have SA)
   - Clearly indicate which series is displayed

2. **Map Simplification**:
   - State map: Simple choropleth (no need for county-level detail initially)
   - Metro map: Points/markers only (not complex boundaries)
   - Use US state GeoJSON for boundaries
   - Use metro lat/lon for point placement

3. **Data Volume Management**:
   - Don't load all 8,300 areas at once
   - State analysis: 50 states (manageable)
   - Metro analysis: Top 100-200 metros (or paginate)
   - Series explorer: Lazy load with search/filter

4. **Reusable Components**:
   - TimelineSelector from LN Explorer
   - Data table with sorting from LN Explorer
   - Chart components from existing explorers
