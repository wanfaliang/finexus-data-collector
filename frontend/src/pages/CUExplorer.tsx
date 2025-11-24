import { useState } from 'react';
import { useQuery, useQueries } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  CircularProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Chip,
  ToggleButton,
  ToggleButtonGroup,
} from '@mui/material';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  BarChart,
  Bar,
  Cell,
} from 'recharts';
import { TrendingUp, TrendingDown, ArrowBack } from '@mui/icons-material';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import { cuExplorerAPI, type CUSeriesInfo } from '../api/client';
import { getAreaCoordinates } from '../utils/areaCoordinates';

// Helper function to format BLS period code to month name
const formatPeriod = (periodCode?: string): string => {
  if (!periodCode) return '';
  const monthMap: Record<string, string> = {
    'M01': 'Jan', 'M02': 'Feb', 'M03': 'Mar', 'M04': 'Apr',
    'M05': 'May', 'M06': 'Jun', 'M07': 'Jul', 'M08': 'Aug',
    'M09': 'Sep', 'M10': 'Oct', 'M11': 'Nov', 'M12': 'Dec',
  };
  return monthMap[periodCode] || periodCode;
};

// Custom clickable dot component (not used anymore but keeping for reference)
const ClickableDot = (props: any) => {
  const { cx, cy, payload, onClick, fill, stroke, ...rest } = props;

  // Filter out non-SVG props to avoid warnings
  const svgProps = {
    cx,
    cy,
    r: 5,
    fill: fill || '#1976d2',
    stroke: stroke || fill || '#1976d2',
    strokeWidth: 2,
  };

  return (
    <circle
      {...svgProps}
      style={{ cursor: 'pointer' }}
      onClick={(e) => {
        e.stopPropagation();
        if (onClick) {
          onClick(payload);
        }
      }}
      onMouseEnter={(e) => {
        e.currentTarget.setAttribute('r', '7');
      }}
      onMouseLeave={(e) => {
        e.currentTarget.setAttribute('r', '5');
      }}
    />
  );
};

export default function CUExplorer() {
  const [selectedAreaOverview, setSelectedAreaOverview] = useState<string>('0000'); // US City Average
  const [selectedAreaCategory, setSelectedAreaCategory] = useState<string>('0000');
  const [selectedItem, setSelectedItem] = useState<string>('SA0'); // All items
  const [selectedAreaDetail, setSelectedAreaDetail] = useState<string>(''); // All areas
  const [selectedItemDetail, setSelectedItemDetail] = useState<string>(''); // All items for detail
  const [selectedSeasonal, setSelectedSeasonal] = useState<string>('');
  const [startYear, setStartYear] = useState<string>('');
  const [startMonth, setStartMonth] = useState<string>('');
  const [endYear, setEndYear] = useState<string>('');
  const [endMonth, setEndMonth] = useState<string>('');
  const [aggregation, setAggregation] = useState<string>('individual');
  const [selectedSeries, setSelectedSeries] = useState<string[]>([]); // Array for multiple charts

  // Timeline state
  const [overviewTimeRange, setOverviewTimeRange] = useState<number>(12); // months
  const [selectedOverviewPeriod, setSelectedOverviewPeriod] = useState<{year: number, period: string} | null>(null);
  const [categoryTimeRange, setCategoryTimeRange] = useState<number>(12);
  const [selectedCategoryPeriod, setSelectedCategoryPeriod] = useState<{year: number, period: string} | null>(null);
  const [areaTimeRange, setAreaTimeRange] = useState<number>(12);
  const [selectedAreaPeriod, setSelectedAreaPeriod] = useState<{year: number, period: string} | null>(null);

  // Map view state
  const [mapMetric, setMapMetric] = useState<'yoy' | 'mom'>('yoy');
  const [mapItem, setMapItem] = useState<string>('SA0'); // All items
  const [mapSelectedSeries, setMapSelectedSeries] = useState<string[]>([]); // Series selected from map
  const [mapTimeRange, setMapTimeRange] = useState<number>(12); // months back for map data

  // Fetch dimensions
  const { data: dimensions, isLoading: loadingDimensions } = useQuery({
    queryKey: ['cu', 'dimensions'],
    queryFn: cuExplorerAPI.getDimensions,
  });

  // Fetch overview data
  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['cu', 'overview', selectedAreaOverview],
    queryFn: () => cuExplorerAPI.getOverview(selectedAreaOverview),
  });

  // Fetch category analysis
  const { data: categories, isLoading: loadingCategories } = useQuery({
    queryKey: ['cu', 'categories', selectedAreaCategory],
    queryFn: () => cuExplorerAPI.getCategoryAnalysis(selectedAreaCategory),
  });

  // Fetch area comparison
  const { data: areaComparison, isLoading: loadingAreas, error: areasError } = useQuery({
    queryKey: ['cu', 'areas', selectedItem],
    queryFn: async () => {
      console.log('Fetching area comparison for item:', selectedItem);
      const result = await cuExplorerAPI.compareAreas(selectedItem);
      console.log('Area comparison result:', result);
      return result;
    },
  });

  // Fetch map data (latest snapshot for markers)
  const { data: mapData, isLoading: loadingMap } = useQuery({
    queryKey: ['cu', 'map', mapItem],
    queryFn: () => cuExplorerAPI.compareAreas(mapItem),
  });

  // Fetch map timeline data (for historical view if needed)
  const { data: mapTimelineData } = useQuery({
    queryKey: ['cu', 'map', 'timeline', mapItem, mapTimeRange],
    queryFn: () => cuExplorerAPI.getAreaComparisonTimeline(mapItem, mapTimeRange),
  });

  // Fetch timeline data
  const { data: overviewTimeline } = useQuery({
    queryKey: ['cu', 'overview', 'timeline', selectedAreaOverview, overviewTimeRange],
    queryFn: () => cuExplorerAPI.getOverviewTimeline(selectedAreaOverview, overviewTimeRange),
  });

  const { data: categoryTimeline } = useQuery({
    queryKey: ['cu', 'categories', 'timeline', selectedAreaCategory, categoryTimeRange],
    queryFn: () => cuExplorerAPI.getCategoryTimeline(selectedAreaCategory, categoryTimeRange),
  });

  const { data: areaTimeline } = useQuery({
    queryKey: ['cu', 'areas', 'timeline', selectedItem, areaTimeRange],
    queryFn: () => cuExplorerAPI.getAreaComparisonTimeline(selectedItem, areaTimeRange),
  });

  // Fetch data for map-selected series
  const mapChartQueries = useQueries({
    queries: mapSelectedSeries.map(seriesId => ({
      queryKey: ['cu', 'data', seriesId, startYear, endYear, startMonth, endMonth],
      queryFn: () => cuExplorerAPI.getSeriesData(seriesId, {
        start_year: startYear ? parseInt(startYear) : undefined,
        end_year: endYear ? parseInt(endYear) : undefined,
        start_period: startMonth || undefined,
        end_period: endMonth || undefined,
      }),
    })),
  });

  // Fetch series list for detail explorer
  const { data: seriesData, isLoading: loadingSeries } = useQuery({
    queryKey: ['cu', 'series', selectedAreaDetail, selectedItemDetail, selectedSeasonal, startYear, endYear],
    queryFn: () =>
      cuExplorerAPI.getSeries({
        area_code: selectedAreaDetail || undefined,
        item_code: selectedItemDetail || undefined,
        seasonal_code: selectedSeasonal || undefined,
        begin_year: startYear ? parseInt(startYear) : undefined,
        end_year: endYear ? parseInt(endYear) : undefined,
        active_only: true,
        limit: 100,
      }),
  });

  // Fetch data for all selected series
  const chartQueries = useQueries({
    queries: selectedSeries.map(seriesId => ({
      queryKey: ['cu', 'data', seriesId, startYear, endYear, startMonth, endMonth],
      queryFn: () => cuExplorerAPI.getSeriesData(seriesId, {
        start_year: startYear ? parseInt(startYear) : undefined,
        end_year: endYear ? parseInt(endYear) : undefined,
        start_period: startMonth || undefined,
        end_period: endMonth || undefined,
      }),
    })),
  });

  if (loadingDimensions) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <CircularProgress size={40} />
      </Box>
    );
  }

  // Deduplicate areas by area_code to avoid duplicate key warnings
  const selectableAreas = dimensions?.areas
    .filter(a => a.selectable)
    .filter((area, index, self) =>
      index === self.findIndex(a => a.area_code === area.area_code)
    ) || [];

  const formatPercent = (value?: number) => {
    if (value === undefined || value === null) return 'N/A';
    const sign = value >= 0 ? '+' : '';
    return `${sign}${value.toFixed(2)}%`;
  };

  return (
    <Box sx={{ width: '100%' }}>
      {/* Header */}
      <Box sx={{ mb: 3 }}>
        <Button
          startIcon={<ArrowBack />}
          onClick={() => window.history.back()}
          sx={{ mb: 2 }}
          size="small"
        >
          Back to Dashboard
        </Button>
        <Typography variant="h5" fontWeight="700" sx={{ color: 'text.primary', mb: 0.5 }}>
          CU - Consumer Price Index Explorer
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Analyze CPI trends, categories, and comparisons
        </Typography>
      </Box>

      {/* Section 1: Overview */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'primary.main',
          bgcolor: 'primary.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'primary.main' }}>
            Overview
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Box>
            {/* Filters */}
            <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
              <FormControl size="small" sx={{ minWidth: 250 }}>
                <InputLabel>Area</InputLabel>
                <Select
                  value={selectedAreaOverview}
                  label="Area"
                  onChange={(e) => {
                    setSelectedAreaOverview(e.target.value);
                    setSelectedOverviewPeriod(null); // Reset selected period
                  }}
                >
                  {selectableAreas.map((area) => (
                    <MenuItem key={area.area_code} value={area.area_code}>
                      {area.area_name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              <FormControl size="small" sx={{ minWidth: 200 }}>
                <InputLabel>Time Range</InputLabel>
                <Select
                  value={overviewTimeRange}
                  label="Time Range"
                  onChange={(e) => {
                    setOverviewTimeRange(e.target.value as number);
                    setSelectedOverviewPeriod(null); // Reset selected period
                  }}
                >
                  <MenuItem value={12}>Last 12 months</MenuItem>
                  <MenuItem value={24}>Last 2 years</MenuItem>
                  <MenuItem value={60}>Last 5 years</MenuItem>
                </Select>
              </FormControl>
            </Box>

            {/* Timeline Chart */}
            {overviewTimeline && overviewTimeline.timeline.length > 0 && (
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                  Year-over-Year % Change
                </Typography>
                <Box sx={{ width: '100%', height: 250 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={overviewTimeline.timeline}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="period_name"
                      tick={{ fontSize: 11 }}
                      angle={-45}
                      textAnchor="end"
                      height={80}
                    />
                    <YAxis label={{ value: '% Change', angle: -90, position: 'insideLeft' }} />
                    <Tooltip cursor={{ stroke: '#1976d2', strokeWidth: 2 }} />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="headline_yoy"
                      stroke="#1976d2"
                      name="Headline YoY"
                      strokeWidth={2}
                      dot={{ r: 3 }}
                    />
                    <Line
                      type="monotone"
                      dataKey="core_yoy"
                      stroke="#9c27b0"
                      name="Core YoY"
                      strokeWidth={2}
                      dot={{ r: 3 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
                </Box>

                {/* Timeline Selector */}
                <Box sx={{ mt: 3, mb: 1, px: 2 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                    Select Month (click any point):
                  </Typography>
                  <Box sx={{ position: 'relative', height: 60 }}>
                    {/* Timeline line */}
                    <Box
                      sx={{
                        position: 'absolute',
                        top: 20,
                        left: 0,
                        right: 0,
                        height: 2,
                        bgcolor: 'grey.300',
                      }}
                    />
                    {/* Timeline dots */}
                    <Box sx={{ position: 'relative', display: 'flex', justifyContent: 'space-between' }}>
                      {overviewTimeline.timeline.map((point, index) => {
                        const isSelected = selectedOverviewPeriod?.year === point.year && selectedOverviewPeriod?.period === point.period;
                        const isLatest = index === overviewTimeline.timeline.length - 1;
                        const shouldShowLabel = index % Math.max(1, Math.floor(overviewTimeline.timeline.length / 8)) === 0 || index === overviewTimeline.timeline.length - 1;

                        return (
                          <Box
                            key={`${point.year}-${point.period}`}
                            sx={{
                              position: 'relative',
                              display: 'flex',
                              flexDirection: 'column',
                              alignItems: 'center',
                              cursor: 'pointer',
                              '&:hover .dot': {
                                transform: 'scale(1.5)',
                              },
                            }}
                            onClick={() => setSelectedOverviewPeriod({ year: point.year, period: point.period })}
                          >
                            {/* Dot */}
                            <Box
                              className="dot"
                              sx={{
                                width: isSelected ? 14 : 10,
                                height: isSelected ? 14 : 10,
                                borderRadius: '50%',
                                bgcolor: isSelected ? 'primary.main' : (isLatest && !selectedOverviewPeriod) ? 'primary.light' : 'grey.400',
                                border: isSelected ? '3px solid' : '2px solid',
                                borderColor: isSelected ? 'primary.dark' : 'white',
                                transition: 'all 0.2s ease',
                                zIndex: isSelected ? 3 : 1,
                                boxShadow: isSelected ? '0 2px 8px rgba(25, 118, 210, 0.4)' : 'none',
                              }}
                            />
                            {/* Label - show every nth month or if selected */}
                            {(shouldShowLabel || isSelected) && (
                              <Typography
                                variant="caption"
                                sx={{
                                  position: 'absolute',
                                  top: 28,
                                  fontSize: isSelected ? '0.7rem' : '0.65rem',
                                  fontWeight: isSelected ? 600 : 400,
                                  color: isSelected ? 'primary.main' : 'text.secondary',
                                  whiteSpace: 'nowrap',
                                  transform: 'rotate(-45deg)',
                                  transformOrigin: 'top left',
                                }}
                              >
                                {point.period_name}
                              </Typography>
                            )}
                          </Box>
                        );
                      })}
                    </Box>
                  </Box>
                </Box>
              </Box>
            )}

            {loadingOverview ? (
              <Box display="flex" justifyContent="center" p={4}>
                <CircularProgress />
              </Box>
            ) : (() => {
              // Get selected or latest data point
              const selectedData = selectedOverviewPeriod && overviewTimeline
                ? overviewTimeline.timeline.find(p => p.year === selectedOverviewPeriod.year && p.period === selectedOverviewPeriod.period)
                : overviewTimeline?.timeline[overviewTimeline.timeline.length - 1]; // Latest

              const headlineValue = selectedData?.headline_value ?? overview?.headline_cpi?.latest_value;
              const headlineMom = selectedData?.headline_mom ?? overview?.headline_cpi?.month_over_month;
              const headlineYoy = selectedData?.headline_yoy ?? overview?.headline_cpi?.year_over_year;
              const headlineDate = selectedData?.period_name || overview?.headline_cpi?.latest_date;

              const coreValue = selectedData?.core_value ?? overview?.core_cpi?.latest_value;
              const coreMom = selectedData?.core_mom ?? overview?.core_cpi?.month_over_month;
              const coreYoy = selectedData?.core_yoy ?? overview?.core_cpi?.year_over_year;
              const coreDate = selectedData?.period_name || overview?.core_cpi?.latest_date;

              return (
                <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 3 }}>
                  {/* Headline CPI Card */}
                  <Card sx={{ bgcolor: 'primary.50', border: '1px solid', borderColor: 'primary.200' }}>
                    <CardContent>
                      <Typography variant="overline" color="text.secondary">
                        Headline CPI
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 1 }}>
                        All items - {headlineDate}
                      </Typography>
                      <Typography variant="h6" fontWeight="600" sx={{ mb: 2, color: 'text.secondary' }}>
                        Index: {headlineValue?.toFixed(2) || 'N/A'}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 3 }}>
                        <Box>
                          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 0.5 }}>
                            Month/Month
                          </Typography>
                          <Typography
                            variant="h4"
                            fontWeight="700"
                            color={(headlineMom ?? 0) >= 0 ? 'error.main' : 'success.main'}
                          >
                            {formatPercent(headlineMom)}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 0.5 }}>
                            Year/Year
                          </Typography>
                          <Typography
                            variant="h4"
                            fontWeight="700"
                            color={(headlineYoy ?? 0) >= 0 ? 'error.main' : 'success.main'}
                          >
                            {formatPercent(headlineYoy)}
                          </Typography>
                        </Box>
                      </Box>
                    </CardContent>
                  </Card>

                  {/* Core CPI Card */}
                  <Card sx={{ bgcolor: 'secondary.50', border: '1px solid', borderColor: 'secondary.200' }}>
                    <CardContent>
                      <Typography variant="overline" color="text.secondary">
                        Core CPI
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 1 }}>
                        All items less food and energy - {coreDate}
                      </Typography>
                      <Typography variant="h6" fontWeight="600" sx={{ mb: 2, color: 'text.secondary' }}>
                        Index: {coreValue?.toFixed(2) || 'N/A'}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 3 }}>
                        <Box>
                          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 0.5 }}>
                            Month/Month
                          </Typography>
                          <Typography
                            variant="h4"
                            fontWeight="700"
                            color={(coreMom ?? 0) >= 0 ? 'error.main' : 'success.main'}
                          >
                            {formatPercent(coreMom)}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 0.5 }}>
                            Year/Year
                          </Typography>
                          <Typography
                            variant="h4"
                            fontWeight="700"
                            color={(coreYoy ?? 0) >= 0 ? 'error.main' : 'success.main'}
                          >
                            {formatPercent(coreYoy)}
                          </Typography>
                        </Box>
                      </Box>
                    </CardContent>
                  </Card>
                </Box>
              );
            })()}
          </Box>
        </CardContent>
      </Card>

      {/* Section 2: Category Analysis */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'secondary.main',
          bgcolor: 'secondary.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'secondary.main' }}>
            Category Analysis
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* Filters */}
          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <FormControl size="small" sx={{ minWidth: 250 }}>
              <InputLabel>Area</InputLabel>
              <Select
                value={selectedAreaCategory}
                label="Area"
                onChange={(e) => {
                  setSelectedAreaCategory(e.target.value);
                  setSelectedCategoryPeriod(null); // Reset selected period
                }}
              >
                {selectableAreas.map((area) => (
                  <MenuItem key={area.area_code} value={area.area_code}>
                    {area.area_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Time Range</InputLabel>
              <Select
                value={categoryTimeRange}
                label="Time Range"
                onChange={(e) => {
                  setCategoryTimeRange(e.target.value as number);
                  setSelectedCategoryPeriod(null); // Reset selected period
                }}
              >
                <MenuItem value={12}>Last 12 months</MenuItem>
                <MenuItem value={24}>Last 2 years</MenuItem>
                <MenuItem value={60}>Last 5 years</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {/* Timeline Chart */}
          {categoryTimeline && categoryTimeline.timeline.length > 0 && (
            <Box sx={{ mb: 3 }}>
              <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                Year-over-Year % Change by Category
              </Typography>
              <Box sx={{ width: '100%', height: 300 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={categoryTimeline.timeline.map(point => ({
                    period_name: point.period_name,
                    year: point.year,
                    period: point.period,
                    ...Object.fromEntries(
                      point.categories.map(cat => [cat.category_name, cat.year_over_year])
                    )
                  }))}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="period_name"
                      tick={{ fontSize: 11 }}
                      angle={-45}
                      textAnchor="end"
                      height={80}
                    />
                    <YAxis label={{ value: '% Change', angle: -90, position: 'insideLeft' }} />
                    <Tooltip cursor={{ stroke: '#9c27b0', strokeWidth: 2 }} />
                    <Legend wrapperStyle={{ fontSize: '11px' }} />
                    {categoryTimeline.timeline[0]?.categories.map((cat, idx) => {
                      const colors = ['#1976d2', '#d32f2f', '#388e3c', '#f57c00', '#7b1fa2', '#0288d1', '#c2185b', '#5d4037'];
                      return (
                        <Line
                          key={cat.category_code}
                          type="monotone"
                          dataKey={cat.category_name}
                          stroke={colors[idx % colors.length]}
                          strokeWidth={2}
                          dot={{ r: 2 }}
                        />
                      );
                    })}
                  </LineChart>
                </ResponsiveContainer>
              </Box>

              {/* Timeline Selector */}
              <Box sx={{ mt: 3, mb: 1, px: 2 }}>
                <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                  Select Month (click any point):
                </Typography>
                <Box sx={{ position: 'relative', height: 60 }}>
                  {/* Timeline line */}
                  <Box
                    sx={{
                      position: 'absolute',
                      top: 20,
                      left: 0,
                      right: 0,
                      height: 2,
                      bgcolor: 'grey.300',
                    }}
                  />
                  {/* Timeline dots */}
                  <Box sx={{ position: 'relative', display: 'flex', justifyContent: 'space-between' }}>
                    {categoryTimeline.timeline.map((point, index) => {
                      const isSelected = selectedCategoryPeriod?.year === point.year && selectedCategoryPeriod?.period === point.period;
                      const isLatest = index === categoryTimeline.timeline.length - 1;
                      const shouldShowLabel = index % Math.max(1, Math.floor(categoryTimeline.timeline.length / 8)) === 0 || index === categoryTimeline.timeline.length - 1;

                      return (
                        <Box
                          key={`${point.year}-${point.period}`}
                          sx={{
                            position: 'relative',
                            display: 'flex',
                            flexDirection: 'column',
                            alignItems: 'center',
                            cursor: 'pointer',
                            '&:hover .dot': {
                              transform: 'scale(1.5)',
                            },
                          }}
                          onClick={() => setSelectedCategoryPeriod({ year: point.year, period: point.period })}
                        >
                          {/* Dot */}
                          <Box
                            className="dot"
                            sx={{
                              width: isSelected ? 14 : 10,
                              height: isSelected ? 14 : 10,
                              borderRadius: '50%',
                              bgcolor: isSelected ? 'secondary.main' : (isLatest && !selectedCategoryPeriod) ? 'secondary.light' : 'grey.400',
                              border: isSelected ? '3px solid' : '2px solid',
                              borderColor: isSelected ? 'secondary.dark' : 'white',
                              transition: 'all 0.2s ease',
                              zIndex: isSelected ? 3 : 1,
                              boxShadow: isSelected ? '0 2px 8px rgba(156, 39, 176, 0.4)' : 'none',
                            }}
                          />
                          {/* Label - show every nth month or if selected */}
                          {(shouldShowLabel || isSelected) && (
                            <Typography
                              variant="caption"
                              sx={{
                                position: 'absolute',
                                top: 28,
                                fontSize: isSelected ? '0.7rem' : '0.65rem',
                                fontWeight: isSelected ? 600 : 400,
                                color: isSelected ? 'secondary.main' : 'text.secondary',
                                whiteSpace: 'nowrap',
                                transform: 'rotate(-45deg)',
                                transformOrigin: 'top left',
                              }}
                            >
                              {point.period_name}
                            </Typography>
                          )}
                        </Box>
                      );
                    })}
                  </Box>
                </Box>
              </Box>
            </Box>
          )}

          {loadingCategories ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : (() => {
            // Get selected or latest data point
            const selectedData = selectedCategoryPeriod && categoryTimeline
              ? categoryTimeline.timeline.find(p => p.year === selectedCategoryPeriod.year && p.period === selectedCategoryPeriod.period)
              : categoryTimeline?.timeline[categoryTimeline.timeline.length - 1]; // Latest

            const displayCategories = selectedData?.categories ?? categories?.categories ?? [];
            const displayDate = selectedData?.period_name || categories?.categories[0]?.latest_date;

            return (
              <>
                <Typography variant="subtitle1" fontWeight="600" sx={{ mb: 0.5 }}>
                  {categories?.area_name || categoryTimeline?.area_name}
                </Typography>
                {selectedData && (
                  <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                    Showing data for: {displayDate}
                  </Typography>
                )}

                <TableContainer>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ fontWeight: 600 }}>Category</TableCell>
                        <TableCell align="right" sx={{ fontWeight: 600 }}>
                          Latest Value
                        </TableCell>
                        <TableCell align="right" sx={{ fontWeight: 600 }}>
                          M/M Change
                        </TableCell>
                        <TableCell align="right" sx={{ fontWeight: 600 }}>
                          Y/Y Change
                        </TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {displayCategories.map((cat) => (
                        <TableRow key={cat.category_code}>
                          <TableCell>{cat.category_name}</TableCell>
                          <TableCell align="right">
                            {cat.latest_value?.toFixed(2) || 'N/A'}
                          </TableCell>
                          <TableCell align="right">
                            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                              {cat.month_over_month !== undefined &&
                                cat.month_over_month !== null &&
                                (cat.month_over_month >= 0 ? (
                                  <TrendingUp fontSize="small" color="error" />
                                ) : (
                                  <TrendingDown fontSize="small" color="success" />
                                ))}
                              <Typography
                                variant="body2"
                                color={
                                  (cat.month_over_month ?? 0) >= 0 ? 'error.main' : 'success.main'
                                }
                                fontWeight="600"
                              >
                                {formatPercent(cat.month_over_month)}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell align="right">
                            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                              {cat.year_over_year !== undefined &&
                                cat.year_over_year !== null &&
                                (cat.year_over_year >= 0 ? (
                                  <TrendingUp fontSize="small" color="error" />
                                ) : (
                                  <TrendingDown fontSize="small" color="success" />
                                ))}
                              <Typography
                                variant="body2"
                                color={
                                  (cat.year_over_year ?? 0) >= 0 ? 'error.main' : 'success.main'
                                }
                                fontWeight="600"
                              >
                                {formatPercent(cat.year_over_year)}
                              </Typography>
                            </Box>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>

                {/* Bar chart for Y/Y changes */}
                <Box sx={{ mt: 3, height: 300 }}>
                  <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
                    Year-over-Year Changes by Category
                  </Typography>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={displayCategories}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis
                        dataKey="category_name"
                        tick={{ fontSize: 10 }}
                        angle={-45}
                        textAnchor="end"
                        height={100}
                      />
                      <YAxis label={{ value: '% Change', angle: -90, position: 'insideLeft' }} />
                      <Tooltip />
                      <Bar dataKey="year_over_year">
                        {displayCategories.map((_, index) => {
                          const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1', '#388e3c', '#c2185b', '#5d4037', '#00796b'];
                          return <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />;
                        })}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </Box>
              </>
            );
          })()}
        </CardContent>
      </Card>

      {/* Section 3: Area Comparison */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'success.main',
          bgcolor: 'success.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'success.main' }}>
            Area Comparison
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* Filters */}
          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <FormControl size="small" sx={{ minWidth: 250 }}>
              <InputLabel>CPI Category</InputLabel>
              <Select
                value={selectedItem}
                label="CPI Category"
                onChange={(e) => {
                  setSelectedItem(e.target.value);
                  setSelectedAreaPeriod(null); // Reset selected period
                }}
              >
                <MenuItem value="SA0">All items</MenuItem>
                <MenuItem value="SAF">Food and beverages</MenuItem>
                <MenuItem value="SAH">Housing</MenuItem>
                <MenuItem value="SAA">Apparel</MenuItem>
                <MenuItem value="SAT">Transportation</MenuItem>
                <MenuItem value="SAM">Medical care</MenuItem>
                <MenuItem value="SAR">Recreation</MenuItem>
                <MenuItem value="SAE">Education and communication</MenuItem>
                <MenuItem value="SAG">Other goods and services</MenuItem>
              </Select>
            </FormControl>

            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Time Range</InputLabel>
              <Select
                value={areaTimeRange}
                label="Time Range"
                onChange={(e) => {
                  setAreaTimeRange(e.target.value as number);
                  setSelectedAreaPeriod(null); // Reset selected period
                }}
              >
                <MenuItem value={12}>Last 12 months</MenuItem>
                <MenuItem value={24}>Last 2 years</MenuItem>
                <MenuItem value={60}>Last 5 years</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {/* Timeline Chart */}
          {areaTimeline && areaTimeline.timeline.length > 0 && (
            <Box sx={{ mb: 3 }}>
              <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                Year-over-Year % Change by Area
              </Typography>
              <Box sx={{ width: '100%', height: 300 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={areaTimeline.timeline.map(point => ({
                    period_name: point.period_name,
                    year: point.year,
                    period: point.period,
                    ...Object.fromEntries(
                      point.areas.map(area => [area.area_name, area.year_over_year])
                    )
                  }))}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="period_name"
                      tick={{ fontSize: 11 }}
                      angle={-45}
                      textAnchor="end"
                      height={80}
                    />
                    <YAxis label={{ value: '% Change', angle: -90, position: 'insideLeft' }} />
                    <Tooltip cursor={{ stroke: '#2e7d32', strokeWidth: 2 }} />
                    <Legend wrapperStyle={{ fontSize: '10px' }} />
                    {areaTimeline.timeline[0]?.areas.slice(0, 10).map((area, idx) => {
                      const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1', '#388e3c', '#c2185b', '#5d4037', '#00796b'];
                      return (
                        <Line
                          key={area.area_code}
                          type="monotone"
                          dataKey={area.area_name}
                          stroke={colors[idx % colors.length]}
                          strokeWidth={2}
                          dot={{ r: 2 }}
                        />
                      );
                    })}
                  </LineChart>
                </ResponsiveContainer>
              </Box>

              {/* Timeline Selector */}
              <Box sx={{ mt: 3, mb: 1, px: 2 }}>
                <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                  Select Month (click any point):
                </Typography>
                <Box sx={{ position: 'relative', height: 60 }}>
                  {/* Timeline line */}
                  <Box
                    sx={{
                      position: 'absolute',
                      top: 20,
                      left: 0,
                      right: 0,
                      height: 2,
                      bgcolor: 'grey.300',
                    }}
                  />
                  {/* Timeline dots */}
                  <Box sx={{ position: 'relative', display: 'flex', justifyContent: 'space-between' }}>
                    {areaTimeline.timeline.map((point, index) => {
                      const isSelected = selectedAreaPeriod?.year === point.year && selectedAreaPeriod?.period === point.period;
                      const isLatest = index === areaTimeline.timeline.length - 1;
                      const shouldShowLabel = index % Math.max(1, Math.floor(areaTimeline.timeline.length / 8)) === 0 || index === areaTimeline.timeline.length - 1;

                      return (
                        <Box
                          key={`${point.year}-${point.period}`}
                          sx={{
                            position: 'relative',
                            display: 'flex',
                            flexDirection: 'column',
                            alignItems: 'center',
                            cursor: 'pointer',
                            '&:hover .dot': {
                              transform: 'scale(1.5)',
                            },
                          }}
                          onClick={() => setSelectedAreaPeriod({ year: point.year, period: point.period })}
                        >
                          {/* Dot */}
                          <Box
                            className="dot"
                            sx={{
                              width: isSelected ? 14 : 10,
                              height: isSelected ? 14 : 10,
                              borderRadius: '50%',
                              bgcolor: isSelected ? 'success.main' : (isLatest && !selectedAreaPeriod) ? 'success.light' : 'grey.400',
                              border: isSelected ? '3px solid' : '2px solid',
                              borderColor: isSelected ? 'success.dark' : 'white',
                              transition: 'all 0.2s ease',
                              zIndex: isSelected ? 3 : 1,
                              boxShadow: isSelected ? '0 2px 8px rgba(46, 125, 50, 0.4)' : 'none',
                            }}
                          />
                          {/* Label - show every nth month or if selected */}
                          {(shouldShowLabel || isSelected) && (
                            <Typography
                              variant="caption"
                              sx={{
                                position: 'absolute',
                                top: 28,
                                fontSize: isSelected ? '0.7rem' : '0.65rem',
                                fontWeight: isSelected ? 600 : 400,
                                color: isSelected ? 'success.main' : 'text.secondary',
                                whiteSpace: 'nowrap',
                                transform: 'rotate(-45deg)',
                                transformOrigin: 'top left',
                              }}
                            >
                              {point.period_name}
                            </Typography>
                          )}
                        </Box>
                      );
                    })}
                  </Box>
                </Box>
              </Box>
            </Box>
          )}

          {loadingAreas ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : areasError ? (
            <Box p={4}>
              <Typography color="error">Error loading area comparison: {String(areasError)}</Typography>
            </Box>
          ) : (() => {
            // Get selected or latest data point
            // Use timeline data only when a specific period is selected, otherwise use snapshot for completeness
            const selectedData = selectedAreaPeriod && areaTimeline
              ? areaTimeline.timeline.find(p => p.year === selectedAreaPeriod.year && p.period === selectedAreaPeriod.period)
              : null; // Use null instead of latest timeline to force fallback to snapshot

            const displayAreas = selectedData?.areas ?? areaComparison?.areas ?? [];
            const displayDate = selectedData?.period_name || areaComparison?.areas[0]?.latest_date;

            if (displayAreas.length === 0) {
              return (
                <Box p={4}>
                  <Typography color="text.secondary">No area comparison data available</Typography>
                </Box>
              );
            }

            return (
              <>
                <Typography variant="subtitle1" fontWeight="600" sx={{ mb: 0.5 }}>
                  {areaComparison?.item_name || areaTimeline?.item_name} - Comparison Across Metro Areas ({displayAreas.length} areas)
                </Typography>
                {selectedData && (
                  <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                    Showing data for: {displayDate}
                  </Typography>
                )}

                <TableContainer>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ fontWeight: 600 }}>Area</TableCell>
                        <TableCell align="right" sx={{ fontWeight: 600 }}>
                          Latest Value
                        </TableCell>
                        <TableCell align="right" sx={{ fontWeight: 600 }}>
                          Y/Y Change
                        </TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {displayAreas.map((area) => (
                        <TableRow key={area.area_code}>
                          <TableCell>{area.area_name}</TableCell>
                          <TableCell align="right">
                            {area.latest_value?.toFixed(2) || 'N/A'}
                          </TableCell>
                          <TableCell align="right">
                            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                              {area.year_over_year !== undefined &&
                                area.year_over_year !== null &&
                                (area.year_over_year >= 0 ? (
                                  <TrendingUp fontSize="small" color="error" />
                                ) : (
                                  <TrendingDown fontSize="small" color="success" />
                                ))}
                              <Typography
                                variant="body2"
                                color={(area.year_over_year ?? 0) >= 0 ? 'error.main' : 'success.main'}
                                fontWeight="600"
                              >
                                {formatPercent(area.year_over_year)}
                              </Typography>
                            </Box>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>

                {/* Bar chart for area comparison */}
                <Box sx={{ mt: 3, height: 300 }}>
                  <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
                    Year-over-Year Changes by Area
                  </Typography>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={displayAreas}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis
                        dataKey="area_name"
                        tick={{ fontSize: 10 }}
                        angle={-45}
                        textAnchor="end"
                        height={120}
                      />
                      <YAxis label={{ value: '% Change', angle: -90, position: 'insideLeft' }} />
                      <Tooltip />
                      <Bar dataKey="year_over_year">
                        {displayAreas.map((_, index) => {
                          const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1', '#388e3c', '#c2185b', '#5d4037', '#00796b', '#c62828', '#ad1457', '#6a1b9a', '#4527a0', '#283593'];
                          return <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />;
                        })}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </Box>
              </>
            );
          })()}
        </CardContent>
      </Card>

      {/* Section 3.5: Geographic View */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'warning.main',
          bgcolor: 'warning.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'warning.main' }}>
            Geographic View
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* Controls */}
          <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center' }}>
            <FormControl size="small" sx={{ minWidth: 250 }}>
              <InputLabel>CPI Category</InputLabel>
              <Select
                value={mapItem}
                label="CPI Category"
                onChange={(e) => setMapItem(e.target.value)}
              >
                <MenuItem value="SA0">All items</MenuItem>
                <MenuItem value="SAF">Food and beverages</MenuItem>
                <MenuItem value="SAH">Housing</MenuItem>
                <MenuItem value="SAA">Apparel</MenuItem>
                <MenuItem value="SAT">Transportation</MenuItem>
                <MenuItem value="SAM">Medical care</MenuItem>
                <MenuItem value="SAR">Recreation</MenuItem>
                <MenuItem value="SAE">Education and communication</MenuItem>
                <MenuItem value="SAG">Other goods and services</MenuItem>
              </Select>
            </FormControl>

            <ToggleButtonGroup
              value={mapMetric}
              exclusive
              onChange={(_, newValue) => newValue && setMapMetric(newValue)}
              size="small"
            >
              <ToggleButton value="yoy">
                Year-over-Year %
              </ToggleButton>
              <ToggleButton value="mom">
                Month-over-Month %
              </ToggleButton>
            </ToggleButtonGroup>

            <FormControl size="small" sx={{ minWidth: 150 }}>
              <InputLabel>Time Range</InputLabel>
              <Select
                value={mapTimeRange}
                label="Time Range"
                onChange={(e) => setMapTimeRange(e.target.value as number)}
              >
                <MenuItem value={12}>Last 12 months</MenuItem>
                <MenuItem value={24}>Last 2 years</MenuItem>
                <MenuItem value={60}>Last 5 years</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {/* Map */}
          {loadingMap ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : (() => {
            // Helper function to get color based on inflation rate
            const getColor = (value: number | null | undefined): string => {
              if (value === null || value === undefined) return '#9e9e9e'; // grey for missing data

              // Color scale: green (negative/low) → yellow → red (high)
              if (value < 0) return '#2e7d32'; // green for deflation
              if (value < 1) return '#66bb6a'; // light green
              if (value < 2) return '#9ccc65'; // lime
              if (value < 3) return '#ffeb3b'; // yellow
              if (value < 4) return '#ffa726'; // orange
              if (value < 5) return '#ff7043'; // deep orange
              return '#f44336'; // red for high inflation
            };

            // Prepare map data with coordinates
            const mapAreas = mapData?.areas
              .map(area => {
                const coords = getAreaCoordinates(area.area_code);
                if (!coords) return null;

                const value = mapMetric === 'yoy' ? area.year_over_year : area.month_over_month;

                return {
                  ...area,
                  lat: coords.lat,
                  lng: coords.lng,
                  displayValue: value,
                  color: getColor(value),
                };
              })
              .filter(area => area !== null);

            return (
              <>
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
                  Click on a metro area to add it to the comparison chart below
                </Typography>

                {/* Color legend */}
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2, flexWrap: 'wrap' }}>
                  <Typography variant="caption" fontWeight="600">Legend:</Typography>
                  {[
                    { label: '< 0% (Deflation)', color: '#2e7d32' },
                    { label: '0-1%', color: '#66bb6a' },
                    { label: '1-2%', color: '#9ccc65' },
                    { label: '2-3%', color: '#ffeb3b' },
                    { label: '3-4%', color: '#ffa726' },
                    { label: '4-5%', color: '#ff7043' },
                    { label: '> 5%', color: '#f44336' },
                  ].map(item => (
                    <Box key={item.label} sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      <Box sx={{ width: 16, height: 16, borderRadius: '50%', bgcolor: item.color, border: '1px solid #fff' }} />
                      <Typography variant="caption">{item.label}</Typography>
                    </Box>
                  ))}
                </Box>

                <Box sx={{ height: 600, border: '1px solid', borderColor: 'divider', borderRadius: 2, overflow: 'hidden' }}>
                  <MapContainer
                    center={[39.8283, -98.5795]}
                    zoom={4}
                    style={{ height: '100%', width: '100%' }}
                  >
                    <TileLayer
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    {mapAreas?.map(area => {
                      const isUSAverage = area.area_code === '0000';
                      return (
                        <CircleMarker
                          key={area.area_code}
                          center={[area.lat, area.lng]}
                          radius={isUSAverage ? 15 : 10}
                          fillColor={area.color}
                          color={isUSAverage ? '#ffd700' : '#fff'}
                          weight={isUSAverage ? 3 : 2}
                          fillOpacity={0.8}
                          eventHandlers={{
                            click: () => {
                              // Find the series ID for this area
                              const seasonal_code = area.area_code === '0000' ? 'S' : 'U';
                              const series_id = `CU${seasonal_code}R${area.area_code}${mapItem}`;

                              // Add to map-selected series for the chart comparison
                              if (!mapSelectedSeries.includes(series_id)) {
                                setMapSelectedSeries([...mapSelectedSeries, series_id]);
                              }
                            },
                          }}
                        >
                          <Popup>
                            <Box sx={{ p: 1 }}>
                              <Typography variant="subtitle2" fontWeight="600">
                                {area.area_name}
                                {isUSAverage && ' ⭐'}
                              </Typography>
                              <Typography variant="caption" display="block">
                                {mapData?.item_name}
                              </Typography>
                              <Typography variant="body2" sx={{ mt: 1 }}>
                                <strong>{mapMetric === 'yoy' ? 'Y/Y' : 'M/M'}:</strong> {formatPercent(area.displayValue)}
                              </Typography>
                              <Typography variant="body2">
                                <strong>CPI Value:</strong> {area.latest_value?.toFixed(2) || 'N/A'}
                              </Typography>
                              <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1 }}>
                                Click marker to add to chart comparison
                              </Typography>
                            </Box>
                          </Popup>
                        </CircleMarker>
                      );
                    })}
                  </MapContainer>
                </Box>
              </>
            );
          })()}

          {/* Map Selection Chart */}
          {mapSelectedSeries.length > 0 && (
            <Box sx={{ mt: 4, pt: 3, borderTop: '2px solid', borderColor: 'divider' }}>
              <Typography variant="h6" fontWeight="600" sx={{ mb: 2 }}>
                Selected Areas Comparison
              </Typography>

              {/* Series chips */}
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 3 }}>
                {mapSelectedSeries.map((seriesId, idx) => {
                  const query = mapChartQueries[idx];
                  const seriesName = query.data?.series[0]?.area_name || seriesId;
                  const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1', '#388e3c', '#c2185b', '#5d4037', '#00796b', '#c62828', '#ad1457', '#6a1b9a', '#4527a0', '#283593'];

                  return (
                    <Chip
                      key={seriesId}
                      label={seriesName}
                      onDelete={() => setMapSelectedSeries(mapSelectedSeries.filter(s => s !== seriesId))}
                      sx={{
                        bgcolor: `${colors[idx % colors.length]}20`,
                        borderLeft: `4px solid ${colors[idx % colors.length]}`,
                        fontWeight: 500,
                      }}
                    />
                  );
                })}
                <Button
                  size="small"
                  onClick={() => setMapSelectedSeries([])}
                  sx={{ ml: 1 }}
                >
                  Clear All
                </Button>
              </Box>

              {/* Combined chart */}
              {mapChartQueries.some(q => q.isSuccess) && (() => {
                const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1', '#388e3c', '#c2185b', '#5d4037', '#00796b', '#c62828', '#ad1457', '#6a1b9a', '#4527a0', '#283593'];
                const periodMap = new Map();
                const combinedData: any[] = [];

                mapChartQueries.forEach((query, idx) => {
                  if (!query.data?.series[0]) return;
                  const data = query.data.series[0];
                  const seriesId = mapSelectedSeries[idx];

                  data.data_points.forEach(point => {
                    const periodKey = `${point.year}-${point.period}`;
                    if (!periodMap.has(periodKey)) {
                      periodMap.set(periodKey, {
                        period_name: point.period_name,
                        year: point.year,
                        period: point.period,
                        sortKey: point.year * 100 + parseInt(point.period.substring(1))
                      });
                    }
                    const periodData = periodMap.get(periodKey)!;
                    periodData[seriesId] = point.value;
                  });
                });

                periodMap.forEach(value => combinedData.push(value));
                combinedData.sort((a, b) => a.sortKey - b.sortKey);

                return (
                  <Box sx={{ width: '100%', height: 400 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={combinedData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                          dataKey="period_name"
                          tick={{ fontSize: 11 }}
                          angle={-45}
                          textAnchor="end"
                          height={80}
                        />
                        <YAxis label={{ value: 'CPI Value', angle: -90, position: 'insideLeft' }} />
                        <Tooltip cursor={{ stroke: '#2e7d32', strokeWidth: 2 }} />
                        <Legend wrapperStyle={{ fontSize: '12px' }} />
                        {mapChartQueries.map((query, idx) => {
                          if (!query.data?.series[0]) return null;
                          const seriesId = mapSelectedSeries[idx];
                          const seriesName = query.data.series[0].area_name;
                          return (
                            <Line
                              key={seriesId}
                              type="monotone"
                              dataKey={seriesId}
                              name={seriesName}
                              stroke={colors[idx % colors.length]}
                              strokeWidth={2}
                              dot={{ r: 3 }}
                            />
                          );
                        })}
                      </LineChart>
                    </ResponsiveContainer>
                  </Box>
                );
              })()}
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Section 4: Series Detail Explorer */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'info.main',
          bgcolor: 'info.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'info.main' }}>
            Series Detail Explorer
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* First row of filters */}
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 2, mb: 2 }}>
            <FormControl size="small" fullWidth>
              <InputLabel>Area</InputLabel>
              <Select
                value={selectedAreaDetail}
                label="Area"
                onChange={(e) => setSelectedAreaDetail(e.target.value)}
              >
                <MenuItem value="">All Areas</MenuItem>
                {selectableAreas.map((area) => (
                  <MenuItem key={area.area_code} value={area.area_code}>
                    {area.area_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Item/Category</InputLabel>
              <Select
                value={selectedItemDetail}
                label="Item/Category"
                onChange={(e) => setSelectedItemDetail(e.target.value)}
              >
                <MenuItem value="">All Items</MenuItem>
                {dimensions?.items.filter(i => i.selectable).map((item) => (
                  <MenuItem key={item.item_code} value={item.item_code}>
                    {item.item_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Seasonal</InputLabel>
              <Select
                value={selectedSeasonal}
                label="Seasonal"
                onChange={(e) => setSelectedSeasonal(e.target.value)}
              >
                <MenuItem value="">All</MenuItem>
                <MenuItem value="S">Adjusted</MenuItem>
                <MenuItem value="U">Not Adjusted</MenuItem>
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Aggregation</InputLabel>
              <Select
                value={aggregation}
                label="Aggregation"
                onChange={(e) => setAggregation(e.target.value)}
              >
                <MenuItem value="individual">Individual (Monthly)</MenuItem>
                <MenuItem value="sum">Sum (Annual)</MenuItem>
                <MenuItem value="avg">Average (Annual)</MenuItem>
                <MenuItem value="max">Max (Annual)</MenuItem>
                <MenuItem value="min">Min (Annual)</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {/* Second row for date range */}
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 2, mb: 3 }}>
            <FormControl size="small" fullWidth>
              <InputLabel>Start Year</InputLabel>
              <Select
                value={startYear}
                label="Start Year"
                onChange={(e) => setStartYear(e.target.value)}
              >
                <MenuItem value="">Any</MenuItem>
                {Array.from({ length: 30 }, (_, i) => new Date().getFullYear() - i).map(year => (
                  <MenuItem key={year} value={year.toString()}>{year}</MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Start Month</InputLabel>
              <Select
                value={startMonth}
                label="Start Month"
                onChange={(e) => setStartMonth(e.target.value)}
              >
                <MenuItem value="">Any</MenuItem>
                <MenuItem value="01">January</MenuItem>
                <MenuItem value="02">February</MenuItem>
                <MenuItem value="03">March</MenuItem>
                <MenuItem value="04">April</MenuItem>
                <MenuItem value="05">May</MenuItem>
                <MenuItem value="06">June</MenuItem>
                <MenuItem value="07">July</MenuItem>
                <MenuItem value="08">August</MenuItem>
                <MenuItem value="09">September</MenuItem>
                <MenuItem value="10">October</MenuItem>
                <MenuItem value="11">November</MenuItem>
                <MenuItem value="12">December</MenuItem>
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>End Year</InputLabel>
              <Select
                value={endYear}
                label="End Year"
                onChange={(e) => setEndYear(e.target.value)}
              >
                <MenuItem value="">Any</MenuItem>
                {Array.from({ length: 30 }, (_, i) => new Date().getFullYear() - i).map(year => (
                  <MenuItem key={year} value={year.toString()}>{year}</MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>End Month</InputLabel>
              <Select
                value={endMonth}
                label="End Month"
                onChange={(e) => setEndMonth(e.target.value)}
              >
                <MenuItem value="">Any</MenuItem>
                <MenuItem value="01">January</MenuItem>
                <MenuItem value="02">February</MenuItem>
                <MenuItem value="03">March</MenuItem>
                <MenuItem value="04">April</MenuItem>
                <MenuItem value="05">May</MenuItem>
                <MenuItem value="06">June</MenuItem>
                <MenuItem value="07">July</MenuItem>
                <MenuItem value="08">August</MenuItem>
                <MenuItem value="09">September</MenuItem>
                <MenuItem value="10">October</MenuItem>
                <MenuItem value="11">November</MenuItem>
                <MenuItem value="12">December</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {/* Combined Chart Area - Displays all selected series on one chart */}
          {selectedSeries.length > 0 && (() => {
            // Define colors for different series
            const seriesColors = [
              '#1976d2', '#d32f2f', '#388e3c', '#f57c00', '#7b1fa2',
              '#0288d1', '#c2185b', '#5d4037', '#00796b', '#0097a7',
              '#303f9f', '#689f38', '#fbc02d', '#e64a19', '#512da8'
            ];

            // Check if any data is still loading
            const anyLoading = chartQueries.some(q => q.isLoading);

            // Combine all series data into a single dataset
            const combinedData: Record<string, any>[] = [];
            const periodMap = new Map<string, Record<string, any>>();

            chartQueries.forEach((query, index) => {
              const seriesId = selectedSeries[index];
              const data = query.data;

              if (data?.series[0]) {
                data.series[0].data_points.forEach(point => {
                  // Create a unique key using year and period for proper sorting
                  const periodKey = `${point.year}-${point.period}`;

                  if (!periodMap.has(periodKey)) {
                    periodMap.set(periodKey, {
                      period_name: point.period_name,
                      year: point.year,
                      period: point.period,
                      sortKey: point.year * 100 + parseInt(point.period.substring(1)) // e.g., 202401, 202402
                    });
                  }
                  const periodData = periodMap.get(periodKey)!;
                  periodData[seriesId] = point.value;
                });
              }
            });

            // Convert map to array and sort by year and period chronologically
            periodMap.forEach(value => combinedData.push(value));
            combinedData.sort((a, b) => a.sortKey - b.sortKey);

            return (
              <Box sx={{ mb: 3 }}>
                <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 1 }}>
                  <Box
                    sx={{
                      px: 2,
                      py: 1.5,
                      borderBottom: '1px solid',
                      borderColor: 'divider',
                    }}
                  >
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                      <Typography variant="subtitle1" fontWeight="600">
                        Series Comparison ({selectedSeries.length} series)
                      </Typography>
                      <Button
                        size="small"
                        onClick={() => setSelectedSeries([])}
                        variant="outlined"
                        color="error"
                      >
                        Clear All
                      </Button>
                    </Box>
                    {/* List of selected series with remove buttons */}
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 2 }}>
                      {selectedSeries.map((seriesId, index) => {
                        const chartData = chartQueries[index].data;
                        const seriesInfo = chartData?.series[0];
                        return (
                          <Chip
                            key={seriesId}
                            label={
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                <Box
                                  sx={{
                                    width: 12,
                                    height: 12,
                                    borderRadius: '50%',
                                    bgcolor: seriesColors[index % seriesColors.length],
                                  }}
                                />
                                <Typography variant="caption" sx={{ fontFamily: 'monospace' }}>
                                  {seriesId}
                                </Typography>
                                {seriesInfo && (
                                  <Typography variant="caption" color="text.secondary">
                                    - {seriesInfo.item_name}
                                  </Typography>
                                )}
                              </Box>
                            }
                            onDelete={() => setSelectedSeries(selectedSeries.filter(id => id !== seriesId))}
                            size="small"
                            sx={{ maxWidth: '100%' }}
                          />
                        );
                      })}
                    </Box>
                  </Box>
                  <Box sx={{ p: 2, height: 450 }}>
                    {anyLoading ? (
                      <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                        <CircularProgress />
                      </Box>
                    ) : combinedData.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={combinedData}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                          <XAxis
                            dataKey="period_name"
                            tick={{ fontSize: 10 }}
                            angle={-45}
                            textAnchor="end"
                            height={80}
                          />
                          <YAxis tick={{ fontSize: 11 }} label={{ value: 'CPI Value', angle: -90, position: 'insideLeft' }} />
                          <Tooltip />
                          <Legend wrapperStyle={{ fontSize: '11px' }} />
                          {selectedSeries.map((seriesId, index) => (
                            <Line
                              key={seriesId}
                              type="monotone"
                              dataKey={seriesId}
                              stroke={seriesColors[index % seriesColors.length]}
                              strokeWidth={2}
                              dot={{ r: 2 }}
                              name={seriesId}
                              connectNulls
                            />
                          ))}
                        </LineChart>
                      </ResponsiveContainer>
                    ) : (
                      <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                        <Typography variant="body2" color="text.secondary">
                          No data available
                        </Typography>
                      </Box>
                    )}
                  </Box>
                </Card>
              </Box>
            );
          })()}

          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
            <Box
              sx={{
                px: 2,
                py: 1.5,
                borderBottom: '1px solid',
                borderColor: 'divider',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <Typography variant="subtitle2" fontWeight="600">
                Series ({seriesData?.total || 0})
              </Typography>
              {selectedSeries.length > 0 && (
                <Typography variant="caption" color="text.secondary">
                  {selectedSeries.length} chart{selectedSeries.length !== 1 ? 's' : ''} displayed above
                </Typography>
              )}
            </Box>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Series ID</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Area</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Item</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Seasonal</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Period</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Action</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {loadingSeries ? (
                    <TableRow>
                      <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                        <CircularProgress size={24} />
                      </TableCell>
                    </TableRow>
                  ) : seriesData?.series.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                        <Typography variant="body2" color="text.secondary">
                          No series found. Try adjusting your filters.
                        </Typography>
                      </TableCell>
                    </TableRow>
                  ) : (
                    seriesData?.series.map((series: CUSeriesInfo) => (
                      <TableRow
                        key={series.series_id}
                        sx={{
                          cursor: 'pointer',
                          bgcolor: selectedSeries.includes(series.series_id) ? 'action.selected' : 'inherit',
                          '&:hover': { bgcolor: 'action.hover' },
                        }}
                        onClick={() => {
                          if (!selectedSeries.includes(series.series_id)) {
                            setSelectedSeries([...selectedSeries, series.series_id]);
                          }
                        }}
                      >
                        <TableCell sx={{ fontSize: '0.8rem', fontFamily: 'monospace' }}>
                          {series.series_id}
                        </TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }}>{series.area_name}</TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }}>{series.item_name}</TableCell>
                        <TableCell>
                          <Chip
                            label={series.seasonal_code === 'S' ? 'Adjusted' : 'Unadjusted'}
                            size="small"
                            sx={{ fontSize: '0.65rem', height: 20 }}
                          />
                        </TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }}>
                          {formatPeriod(series.begin_period)} {series.begin_year} - {series.end_year ? `${formatPeriod(series.end_period)} ${series.end_year}` : 'Present'}
                        </TableCell>
                        <TableCell>
                          <Button
                            size="small"
                            startIcon={<TrendingUp />}
                            onClick={(e) => {
                              e.stopPropagation();
                              if (!selectedSeries.includes(series.series_id)) {
                                setSelectedSeries([...selectedSeries, series.series_id]);
                              }
                            }}
                            sx={{ fontSize: '0.7rem', py: 0.25 }}
                            disabled={selectedSeries.includes(series.series_id)}
                          >
                            {selectedSeries.includes(series.series_id) ? 'Added' : 'View'}
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </Card>
        </CardContent>
      </Card>
    </Box>
  );
}
