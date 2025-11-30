import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
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
  CircularProgress,
  Chip,
  Button,
  Grid,
  ToggleButtonGroup,
  ToggleButton,
  Paper,
  TextField,
  Checkbox,
  FormControlLabel,
  TablePagination,
} from '@mui/material';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import {
  TrendingUp,
  TrendingDown,
  ArrowBack,
  TableChart,
  ShowChart,
  Map as MapIcon,
} from '@mui/icons-material';
import { MapContainer, TileLayer, CircleMarker, Popup, Marker, Pane } from 'react-leaflet';
import { Icon, DivIcon } from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { laExplorerAPI, lnExplorerAPI } from '../api/client';
import type { LASeriesInfo, UnemploymentMetric } from '../api/client';
import { getLAAreaCoordinates, US_BOUNDS } from '../utils/laAreaCoordinates';

export default function LAExplorer() {

  // Overview state
  const [overviewTimeRange, setOverviewTimeRange] = useState<number>(12);

  // States state
  const [stateTimeRange, setStateTimeRange] = useState<number>(24);
  const [selectedStatePeriod, setSelectedStatePeriod] = useState<{year: number, period: string} | null>(null);
  const [stateView, setStateView] = useState<'table' | 'chart'>('table');
  const [selectedStatesForTimeline, setSelectedStatesForTimeline] = useState<string[]>([]);

  // Metros state
  const [metroTimeRange, setMetroTimeRange] = useState<number>(24);
  const [selectedMetroPeriod, setSelectedMetroPeriod] = useState<{year: number, period: string} | null>(null);
  const [metroView, setMetroView] = useState<'table' | 'chart'>('table');
  const [selectedMetrosForTimeline, setSelectedMetrosForTimeline] = useState<string[]>([]);
  const [metroPage, setMetroPage] = useState(0);
  const [metroRowsPerPage, setMetroRowsPerPage] = useState(25);

  // Series Detail state
  const [selectedArea, setSelectedArea] = useState<string>('');
  const [selectedMeasure, setSelectedMeasure] = useState<string>('');
  const [selectedSeasonal, setSelectedSeasonal] = useState<string>('');
  const [selectedSeriesIds, setSelectedSeriesIds] = useState<string[]>([]);

  // Helper functions
  const formatRate = (val?: number | null) => (val != null ? `${val.toFixed(1)}%` : 'N/A');
  const formatNumber = (val?: number | null) => (val != null ? val.toLocaleString(undefined, { maximumFractionDigits: 0 }) : 'N/A');
  const formatChange = (val?: number | null) => {
    if (val == null) return 'N/A';
    const sign = val >= 0 ? '+' : '';
    return `${sign}${val.toFixed(1)}pp`;
  };
  const formatPeriod = (period: string) => {
    const monthMap: Record<string, string> = {
      'M01': 'Jan', 'M02': 'Feb', 'M03': 'Mar', 'M04': 'Apr',
      'M05': 'May', 'M06': 'Jun', 'M07': 'Jul', 'M08': 'Aug',
      'M09': 'Sep', 'M10': 'Oct', 'M11': 'Nov', 'M12': 'Dec',
    };
    return monthMap[period] || period;
  };

  // Data queries
  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['la', 'overview'],
    queryFn: laExplorerAPI.getOverview,
  });

  const { data: states, isLoading: loadingStates } = useQuery({
    queryKey: ['la', 'states'],
    queryFn: laExplorerAPI.getStates,
  });

  const { data: stateTimeline, isLoading: loadingStateTimeline } = useQuery({
    queryKey: ['la', 'states', 'timeline', stateTimeRange, selectedStatesForTimeline],
    queryFn: () => laExplorerAPI.getStatesTimeline(
      stateTimeRange,
      selectedStatesForTimeline.length > 0 ? selectedStatesForTimeline.join(',') : undefined
    ),
  });

  const { data: metros, isLoading: loadingMetros } = useQuery({
    queryKey: ['la', 'metros'],
    queryFn: () => laExplorerAPI.getMetros(500),
  });

  // Build metro codes from the loaded metros data for timeline query
  const topMetroCodes = metros?.metros.slice(0, 10).map(m => m.area_code) || [];

  const { data: metroTimeline, isLoading: loadingMetroTimeline } = useQuery({
    queryKey: ['la', 'metros', 'timeline', metroTimeRange, selectedMetrosForTimeline, topMetroCodes],
    queryFn: () => laExplorerAPI.getMetrosTimeline(
      metroTimeRange,
      selectedMetrosForTimeline.length > 0
        ? selectedMetrosForTimeline.join(',')
        : topMetroCodes.length > 0
          ? topMetroCodes.join(',')
          : undefined,
      10
    ),
    enabled: !!metros, // Only run after metros data is loaded
  });

  // National unemployment timeline from LN survey (for comparison baseline)
  const { data: nationalTimeline } = useQuery({
    queryKey: ['ln', 'overview', 'timeline', Math.max(stateTimeRange, metroTimeRange)],
    queryFn: () => lnExplorerAPI.getOverviewTimeline(Math.max(stateTimeRange, metroTimeRange)),
  });

  // Series Detail queries
  const { data: dimensions, isLoading: loadingDimensions } = useQuery({
    queryKey: ['la', 'dimensions'],
    queryFn: laExplorerAPI.getDimensions,
  });

  const { data: seriesData, isLoading: loadingSeries } = useQuery({
    queryKey: ['la', 'series', selectedArea, selectedMeasure, selectedSeasonal],
    queryFn: () =>
      laExplorerAPI.getSeries({
        area_code: selectedArea || undefined,
        measure_code: selectedMeasure || undefined,
        seasonal_code: selectedSeasonal || undefined,
        active_only: true,
        limit: 100,
      }),
  });

  const seriesDataQueries = selectedSeriesIds.map(seriesId =>
    useQuery({
      queryKey: ['la', 'data', seriesId],
      queryFn: () => laExplorerAPI.getSeriesData(seriesId),
      enabled: selectedSeriesIds.includes(seriesId),
    })
  );

  // Timeline Selector Component (reusable)
  const TimelineSelector = ({
    timeline,
    selectedPeriod,
    onSelectPeriod
  }: {
    timeline: any[],
    selectedPeriod: {year: number, period: string} | null,
    onSelectPeriod: (period: {year: number, period: string}) => void
  }) => (
    <Box sx={{ mt: 3, mb: 1, px: 2 }}>
      <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
        Select Month (click any point):
      </Typography>
      <Box sx={{ position: 'relative', height: 60 }}>
        <Box sx={{ position: 'absolute', top: 20, left: 0, right: 0, height: 2, bgcolor: 'grey.300' }} />
        <Box sx={{ position: 'relative', display: 'flex', justifyContent: 'space-between' }}>
          {timeline.map((point, index) => {
            const isSelected = selectedPeriod?.year === point.year && selectedPeriod?.period === point.period;
            const isLatest = index === timeline.length - 1;
            const shouldShowLabel = index % Math.max(1, Math.floor(timeline.length / 8)) === 0 || index === timeline.length - 1;

            return (
              <Box
                key={`${point.year}-${point.period}`}
                sx={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  cursor: 'pointer',
                  flex: 1,
                }}
                onClick={() => onSelectPeriod({ year: point.year, period: point.period })}
              >
                <Box
                  className="dot"
                  sx={{
                    width: isSelected ? 14 : 10,
                    height: isSelected ? 14 : 10,
                    borderRadius: '50%',
                    bgcolor: isSelected ? 'primary.main' : (isLatest && !selectedPeriod) ? 'primary.light' : 'grey.400',
                    mb: 1,
                    transition: 'all 0.2s',
                    boxShadow: isSelected ? 2 : 0,
                    '&:hover': {
                      transform: 'scale(1.2)',
                      bgcolor: isSelected ? 'primary.main' : 'primary.light',
                    },
                  }}
                />
                {(shouldShowLabel || isSelected) && (
                  <Typography
                    variant="caption"
                    sx={{
                      fontSize: '0.65rem',
                      color: isSelected ? 'primary.main' : 'text.secondary',
                      fontWeight: isSelected ? 600 : 400,
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {formatPeriod(point.period)} {point.year}
                  </Typography>
                )}
              </Box>
            );
          })}
        </Box>
      </Box>
    </Box>
  );

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h5" fontWeight="700">
          LA - Local Area Unemployment Statistics Explorer
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Explore unemployment data across states and metropolitan areas
        </Typography>
      </Box>

      {/* Overview Section */}
      <Card sx={{ mb: 4, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '3px solid', borderColor: 'error.main', bgcolor: 'error.50' }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'error.main' }}>
            National Overview
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Typography variant="caption" color="text.secondary" sx={{ mb: 3, display: 'block' }}>
            Note: Data aggregated from state-level statistics. For official national unemployment, see LN Survey.
          </Typography>

          {loadingOverview ? (
            <Box display="flex" justifyContent="center" py={8}>
              <CircularProgress />
            </Box>
          ) : overview ? (
            <Grid container spacing={3}>
              <Grid item xs={12} md={4}>
                <Card sx={{ bgcolor: 'error.50', border: '2px solid', borderColor: 'error.main' }}>
                  <CardContent>
                    <Typography variant="overline" color="error.dark">Unemployment Rate</Typography>
                    <Typography variant="caption" display="block" color="text.secondary">
                      {overview.national_unemployment.latest_date}
                    </Typography>
                    <Typography variant="h3" fontWeight="bold" color="error.dark" sx={{ my: 1 }}>
                      {formatRate(overview.national_unemployment.unemployment_rate)}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={4}>
                <Card sx={{ bgcolor: 'success.50', border: '2px solid', borderColor: 'success.main' }}>
                  <CardContent>
                    <Typography variant="overline" color="success.dark">Employment</Typography>
                    <Typography variant="caption" display="block" color="text.secondary">
                      {overview.national_unemployment.latest_date}
                    </Typography>
                    <Typography variant="h4" fontWeight="bold" color="success.dark" sx={{ my: 1 }}>
                      {formatNumber(overview.national_unemployment.employment_level)}K
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={4}>
                <Card sx={{ bgcolor: 'info.50', border: '2px solid', borderColor: 'info.main' }}>
                  <CardContent>
                    <Typography variant="overline" color="info.dark">Labor Force</Typography>
                    <Typography variant="caption" display="block" color="text.secondary">
                      {overview.national_unemployment.latest_date}
                    </Typography>
                    <Typography variant="h4" fontWeight="bold" color="info.dark" sx={{ my: 1 }}>
                      {formatNumber(overview.national_unemployment.labor_force)}K
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </Grid>
          ) : null}
        </CardContent>
      </Card>

      {/* Geographic View Section */}
      <Card sx={{ mb: 4, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '3px solid', borderColor: 'warning.main', bgcolor: 'warning.50' }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'warning.main' }}>
            Geographic View
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
            View unemployment rates across states and metropolitan areas. Click on any marker to add it to the comparison charts above.
          </Typography>

          {/* Color Legend */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2, flexWrap: 'wrap' }}>
            <Typography variant="caption" fontWeight="600">Unemployment Rate:</Typography>
            {[
              { label: '< 3%', color: '#2e7d32' },
              { label: '3-4%', color: '#66bb6a' },
              { label: '4-5%', color: '#ffeb3b' },
              { label: '5-6%', color: '#ffa726' },
              { label: '6-7%', color: '#ff7043' },
              { label: '> 7%', color: '#d32f2f' },
            ].map(item => (
              <Box key={item.label} sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                <Box sx={{ width: 16, height: 16, borderRadius: '50%', bgcolor: item.color, border: '1px solid #ccc' }} />
                <Typography variant="caption">{item.label}</Typography>
              </Box>
            ))}
          </Box>

          {/* Shape Legend */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 3 }}>
            <Typography variant="caption" fontWeight="600">Markers:</Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 18, height: 18, borderRadius: '50%', bgcolor: '#ffa726', border: '3px solid #ffd700' }} />
              <Typography variant="caption">National</Typography>
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 14, height: 14, bgcolor: '#ffa726', border: '2px solid #1976d2' }} />
              <Typography variant="caption">State (square)</Typography>
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 12, height: 12, borderRadius: '50%', bgcolor: '#ffa726', border: '2px solid #7b1fa2' }} />
              <Typography variant="caption">Metro (circle)</Typography>
            </Box>
          </Box>

          {/* Map */}
          {loadingStates || loadingMetros ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : (() => {
            // Pre-process metros with coordinates
            const metrosWithCoords = (metros?.metros || [])
              .map(metro => {
                const coords = getLAAreaCoordinates(metro.area_code, metro.area_name);
                if (!coords) return null;
                const rate = metro.unemployment_rate || 0;
                const color = rate < 3 ? '#2e7d32' : rate < 4 ? '#66bb6a' : rate < 5 ? '#ffeb3b' : rate < 6 ? '#ffa726' : rate < 7 ? '#ff7043' : '#d32f2f';
                return { ...metro, lat: coords.lat, lng: coords.lng, color };
              })
              .filter(m => m !== null) || [];

            // Pre-process states with coordinates
            const statesWithCoords = states?.states
              .map(state => {
                const coords = getLAAreaCoordinates(state.area_code, state.area_name);
                if (!coords) return null;
                const rate = state.unemployment_rate || 0;
                const color = rate < 3 ? '#2e7d32' : rate < 4 ? '#66bb6a' : rate < 5 ? '#ffeb3b' : rate < 6 ? '#ffa726' : rate < 7 ? '#ff7043' : '#d32f2f';
                return { ...state, lat: coords.lat, lng: coords.lng, color };
              })
              .filter(s => s !== null) || [];

            return (
            <Box sx={{ height: 500, border: '1px solid', borderColor: 'divider', borderRadius: 2, overflow: 'hidden' }}>
              <MapContainer
                center={[39.8283, -98.5795]}
                zoom={4}
                minZoom={3}
                maxZoom={10}
                maxBounds={US_BOUNDS}
                maxBoundsViscosity={1.0}
                style={{ height: '100%', width: '100%', background: '#f5f5f5' }}
                scrollWheelZoom={true}
              >
                {/* Simple light map style - CartoDB Positron (no terrain, just borders) */}
                <TileLayer
                  attribution='&copy; <a href="https://carto.com/">CARTO</a>'
                  url="https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png"
                />
                {/* Add labels layer on top */}
                <TileLayer
                  url="https://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}{r}.png"
                />

                {/* National marker - special star/prominent marker */}
                {overview && (
                  <CircleMarker
                    center={[39.8283, -98.5795]}
                    radius={18}
                    fillColor={(() => {
                      const rate = overview.national_unemployment.unemployment_rate;
                      if (!rate) return '#9e9e9e';
                      if (rate < 3) return '#2e7d32';
                      if (rate < 4) return '#66bb6a';
                      if (rate < 5) return '#ffeb3b';
                      if (rate < 6) return '#ffa726';
                      if (rate < 7) return '#ff7043';
                      return '#d32f2f';
                    })()}
                    color="#ffd700"
                    weight={4}
                    fillOpacity={0.9}
                    eventHandlers={{
                      mouseover: (e) => e.target.openPopup(),
                      mouseout: (e) => e.target.closePopup(),
                    }}
                  >
                    <Popup>
                      <Box sx={{ p: 1, minWidth: 150 }}>
                        <Typography variant="subtitle2" fontWeight="700">
                          United States (National)
                        </Typography>
                        <Typography variant="body2" sx={{ mt: 1 }}>
                          <strong>Unemployment Rate:</strong> {formatRate(overview.national_unemployment.unemployment_rate)}
                        </Typography>
                        <Typography variant="body2">
                          <strong>Labor Force:</strong> {formatNumber(overview.national_unemployment.labor_force)}K
                        </Typography>
                      </Box>
                    </Popup>
                  </CircleMarker>
                )}

                {/* Metro markers - circles */}
                {metrosWithCoords.map((metro) => (
                  <CircleMarker
                    key={metro.area_code}
                    center={[metro.lat, metro.lng]}
                    radius={8}
                    fillColor={metro.color}
                    color="#7b1fa2"
                    weight={2}
                    fillOpacity={0.9}
                    eventHandlers={{
                      mouseover: (e) => e.target.openPopup(),
                      mouseout: (e) => e.target.closePopup(),
                      click: () => {
                        if (!selectedMetrosForTimeline.includes(metro.area_code)) {
                          setSelectedMetrosForTimeline([...selectedMetrosForTimeline.slice(0, 9), metro.area_code]);
                        }
                      },
                    }}
                  >
                    <Popup>
                      <Box sx={{ p: 1, minWidth: 180 }}>
                        <Typography variant="subtitle2" fontWeight="600" sx={{ fontSize: '0.85rem' }}>
                          {metro.area_name}
                        </Typography>
                        <Typography variant="body2" sx={{ mt: 1 }}>
                          <strong>Unemployment:</strong> {formatRate(metro.unemployment_rate)}
                        </Typography>
                        <Typography variant="body2">
                          <strong>Labor Force:</strong> {formatNumber(metro.labor_force)}K
                        </Typography>
                      </Box>
                    </Popup>
                  </CircleMarker>
                ))}

                {/* State markers - SQUARES using DivIcon */}
                {statesWithCoords.map(state => {
                  const squareIcon = new DivIcon({
                    className: '',
                    html: `<div style="width:14px;height:14px;background:${state.color};border:2px solid #1976d2;transform:translate(-50%,-50%);cursor:pointer;"></div>`,
                    iconSize: [14, 14],
                    iconAnchor: [7, 7],
                  });

                  return (
                    <Marker
                      key={state.area_code}
                      position={[state.lat, state.lng]}
                      icon={squareIcon}
                      eventHandlers={{
                        mouseover: (e) => e.target.openPopup(),
                        mouseout: (e) => e.target.closePopup(),
                        click: () => {
                          if (!selectedStatesForTimeline.includes(state.area_code)) {
                            setSelectedStatesForTimeline([...selectedStatesForTimeline.slice(0, 9), state.area_code]);
                          }
                        },
                      }}
                    >
                      <Popup>
                        <Box sx={{ p: 1, minWidth: 150 }}>
                          <Typography variant="subtitle2" fontWeight="600">
                            {state.area_name}
                          </Typography>
                          <Typography variant="body2" sx={{ mt: 1 }}>
                            <strong>Unemployment:</strong> {formatRate(state.unemployment_rate)}
                          </Typography>
                          <Typography variant="body2">
                            <strong>Labor Force:</strong> {formatNumber(state.labor_force)}K
                          </Typography>
                        </Box>
                      </Popup>
                    </Marker>
                  );
                })}
              </MapContainer>
            </Box>
            );
          })()}
        </CardContent>
      </Card>

      {/* State Analysis Section */}
      <Card sx={{ mb: 4, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '3px solid', borderColor: 'primary.main', bgcolor: 'primary.50' }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'primary.main' }}>
            State Analysis
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', mb: 2, gap: 2 }}>
              <FormControl size="small" sx={{ minWidth: 150 }}>
                <InputLabel>Time Range</InputLabel>
                <Select
                  value={stateTimeRange}
                  label="Time Range"
                  onChange={(e) => {
                    setStateTimeRange(e.target.value as number);
                    setSelectedStatePeriod(null);
                  }}
                >
                  <MenuItem value={12}>Last 12 months</MenuItem>
                  <MenuItem value={24}>Last 2 years</MenuItem>
                  <MenuItem value={60}>Last 5 years</MenuItem>
                </Select>
              </FormControl>
              <ToggleButtonGroup
                value={stateView}
                exclusive
                onChange={(_, val) => val && setStateView(val)}
                size="small"
              >
                <ToggleButton value="table">
                  <TableChart fontSize="small" />
                </ToggleButton>
                <ToggleButton value="chart">
                  <ShowChart fontSize="small" />
                </ToggleButton>
              </ToggleButtonGroup>
          </Box>

          {loadingStates ? (
            <Box display="flex" justifyContent="center" py={8}>
              <CircularProgress />
            </Box>
          ) : states ? (
            <>
              {/* State Rankings */}
              <Grid container spacing={2} sx={{ mb: 3 }}>
                <Grid item xs={12} md={6}>
                  <Card>
                    <CardContent>
                      <Typography variant="subtitle2" fontWeight="600" color="error.main" sx={{ mb: 1 }}>
                        Highest Unemployment
                      </Typography>
                      {states.rankings.highest.map((name, idx) => (
                        <Typography key={idx} variant="body2" color="text.secondary">
                          {idx + 1}. {name}
                        </Typography>
                      ))}
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Card>
                    <CardContent>
                      <Typography variant="subtitle2" fontWeight="600" color="success.main" sx={{ mb: 1 }}>
                        Lowest Unemployment
                      </Typography>
                      {states.rankings.lowest.map((name, idx) => (
                        <Typography key={idx} variant="body2" color="text.secondary">
                          {idx + 1}. {name}
                        </Typography>
                      ))}
                    </CardContent>
                  </Card>
                </Grid>
              </Grid>

              {/* State Timeline - Always show National + selected states */}
              {nationalTimeline && nationalTimeline.timeline.length > 0 && (
                <Card sx={{ mb: 3 }}>
                  <CardContent>
                    <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
                      State Unemployment Trends vs National
                    </Typography>
                    <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                      Click states in the table below to add them to the chart
                    </Typography>
                    <Box sx={{ height: 300, mb: 2 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={(() => {
                          // Use national timeline as base to ensure continuous national line
                          // Then add state data where available
                          const stateMap = new Map<string, Record<string, number>>();
                          if (stateTimeline && stateTimeline.timeline.length > 0) {
                            stateTimeline.timeline.forEach((p: any) => {
                              stateMap.set(`${p.year}-${p.period}`, p.states || {});
                            });
                          }
                          return nationalTimeline.timeline.map((p: any) => ({
                            year: p.year,
                            period: p.period,
                            period_name: p.period_name,
                            national: p.headline_value,
                            states: stateMap.get(`${p.year}-${p.period}`) || {}
                          }));
                        })()}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis
                            dataKey="period_name"
                            tick={{ fontSize: 10 }}
                            angle={-45}
                            textAnchor="end"
                            height={80}
                            interval={Math.floor(nationalTimeline.timeline.length / 12)}
                          />
                          <YAxis tick={{ fontSize: 11 }} label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }} />
                          <Tooltip />
                          <Legend wrapperStyle={{ fontSize: '11px' }} />
                          {/* Always show National line */}
                          <Line
                            type="monotone"
                            dataKey="national"
                            stroke="#000000"
                            strokeWidth={3}
                            dot={false}
                            name="National (US)"
                          />
                          {/* Show selected states */}
                          {selectedStatesForTimeline.slice(0, 10).map((areaCode, idx) => {
                            const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1', '#c62828', '#5e35b1', '#00796b', '#f9a825'];
                            return (
                              <Line
                                key={areaCode}
                                type="monotone"
                                dataKey={(point: any) => point.states?.[areaCode]}
                                stroke={colors[idx % colors.length]}
                                strokeWidth={2}
                                dot={false}
                                name={stateTimeline?.state_names?.[areaCode] || areaCode}
                              />
                            );
                          })}
                        </LineChart>
                      </ResponsiveContainer>
                    </Box>
                    {stateTimeline && stateTimeline.timeline.length > 0 && (
                      <TimelineSelector
                        timeline={stateTimeline.timeline}
                        selectedPeriod={selectedStatePeriod}
                        onSelectPeriod={setSelectedStatePeriod}
                      />
                    )}
                  </CardContent>
                </Card>
              )}

              {/* State Data Table/Chart */}
              <Card>
                <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                  <Typography variant="subtitle2" fontWeight="600">
                    State Unemployment Data ({states.states.length} states)
                  </Typography>
                  {selectedStatePeriod && (
                    <Typography variant="caption" color="primary.main" sx={{ display: 'block', mt: 0.5 }}>
                      Showing data for: {formatPeriod(selectedStatePeriod.period)} {selectedStatePeriod.year}
                    </Typography>
                  )}
                </Box>
                {stateView === 'table' ? (
                  <TableContainer>
                    <Table size="small" stickyHeader>
                      <TableHead>
                        <TableRow>
                          <TableCell padding="checkbox">
                            <Checkbox size="small" disabled />
                          </TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>State</TableCell>
                          <TableCell align="right" sx={{ fontWeight: 600 }}>Unemp. Rate</TableCell>
                          <TableCell align="right" sx={{ fontWeight: 600 }}>Labor Force (K)</TableCell>
                          <TableCell align="right" sx={{ fontWeight: 600 }}>Employment (K)</TableCell>
                          {!selectedStatePeriod && (
                            <>
                              <TableCell align="right" sx={{ fontWeight: 600 }}>M/M</TableCell>
                              <TableCell align="right" sx={{ fontWeight: 600 }}>Y/Y</TableCell>
                            </>
                          )}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {(() => {
                          let displayStates = states.states;

                          // If period selected, get data from timeline
                          if (selectedStatePeriod && stateTimeline) {
                            const timelinePoint = stateTimeline.timeline.find(
                              p => p.year === selectedStatePeriod.year && p.period === selectedStatePeriod.period
                            );

                            if (timelinePoint) {
                              displayStates = states.states.map(state => ({
                                ...state,
                                unemployment_rate: timelinePoint.states[state.area_code] ?? state.unemployment_rate,
                              })).sort((a, b) => (b.unemployment_rate || 0) - (a.unemployment_rate || 0));
                            }
                          }

                          return displayStates.map((state) => (
                            <TableRow
                              key={state.area_code}
                              sx={{
                                cursor: 'pointer',
                                '&:hover': { bgcolor: 'action.hover' },
                                bgcolor: selectedStatesForTimeline.includes(state.area_code) ? 'action.selected' : 'inherit',
                              }}
                              onClick={() => {
                                if (selectedStatesForTimeline.includes(state.area_code)) {
                                  setSelectedStatesForTimeline(selectedStatesForTimeline.filter(c => c !== state.area_code));
                                } else if (selectedStatesForTimeline.length < 10) {
                                  setSelectedStatesForTimeline([...selectedStatesForTimeline, state.area_code]);
                                }
                              }}
                            >
                              <TableCell padding="checkbox">
                                <Checkbox
                                  size="small"
                                  checked={selectedStatesForTimeline.includes(state.area_code)}
                                  disabled={!selectedStatesForTimeline.includes(state.area_code) && selectedStatesForTimeline.length >= 10}
                                />
                              </TableCell>
                              <TableCell>{state.area_name}</TableCell>
                              <TableCell align="right">
                                <Chip
                                  label={formatRate(state.unemployment_rate)}
                                  size="small"
                                  sx={{
                                    bgcolor: (state.unemployment_rate || 0) > 5 ? 'error.100' : (state.unemployment_rate || 0) > 4 ? 'warning.100' : 'success.100',
                                    fontWeight: 600,
                                  }}
                                />
                              </TableCell>
                              <TableCell align="right">{formatNumber(state.labor_force)}</TableCell>
                              <TableCell align="right">{formatNumber(state.employment_level)}</TableCell>
                              {!selectedStatePeriod && (
                                <>
                                  <TableCell align="right">
                                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                      {state.month_over_month && (state.month_over_month >= 0 ? <TrendingUp fontSize="small" color="error" /> : <TrendingDown fontSize="small" color="success" />)}
                                      {formatChange(state.month_over_month)}
                                    </Box>
                                  </TableCell>
                                  <TableCell align="right">
                                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                      {state.year_over_year && (state.year_over_year >= 0 ? <TrendingUp fontSize="small" color="error" /> : <TrendingDown fontSize="small" color="success" />)}
                                      {formatChange(state.year_over_year)}
                                    </Box>
                                  </TableCell>
                                </>
                              )}
                            </TableRow>
                          ));
                        })()}
                      </TableBody>
                    </Table>
                  </TableContainer>
                ) : (
                  <Box sx={{ p: 2, height: 500 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={(() => {
                          let displayStates = states.states.slice(0, 20);
                          if (selectedStatePeriod && stateTimeline) {
                            const timelinePoint = stateTimeline.timeline.find(
                              p => p.year === selectedStatePeriod.year && p.period === selectedStatePeriod.period
                            );
                            if (timelinePoint) {
                              displayStates = states.states.map(state => ({
                                ...state,
                                unemployment_rate: timelinePoint.states[state.area_code] ?? state.unemployment_rate,
                              })).sort((a, b) => (b.unemployment_rate || 0) - (a.unemployment_rate || 0)).slice(0, 20);
                            }
                          }
                          return displayStates;
                        })()}
                        layout="vertical"
                        margin={{ left: 120 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" tick={{ fontSize: 11 }} label={{ value: 'Unemployment Rate (%)', position: 'insideBottom', offset: -5, style: { fontSize: 11 } }} />
                        <YAxis type="category" dataKey="area_name" tick={{ fontSize: 10 }} width={110} />
                        <Tooltip />
                        <Bar dataKey="unemployment_rate" fill="#1976d2" name="Unemployment Rate (%)" />
                      </BarChart>
                    </ResponsiveContainer>
                  </Box>
                )}
              </Card>
            </>
          ) : null}
        </CardContent>
      </Card>

      {/* Metro Areas Section */}
      <Card sx={{ mb: 4, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '3px solid', borderColor: 'success.main', bgcolor: 'success.50' }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'success.main' }}>
            Metropolitan Areas
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', mb: 2, gap: 2 }}>
              <FormControl size="small" sx={{ minWidth: 150 }}>
                <InputLabel>Time Range</InputLabel>
                <Select
                  value={metroTimeRange}
                  label="Time Range"
                  onChange={(e) => {
                    setMetroTimeRange(e.target.value as number);
                    setSelectedMetroPeriod(null);
                  }}
                >
                  <MenuItem value={12}>Last 12 months</MenuItem>
                  <MenuItem value={24}>Last 2 years</MenuItem>
                  <MenuItem value={60}>Last 5 years</MenuItem>
                </Select>
              </FormControl>
              <ToggleButtonGroup
                value={metroView}
                exclusive
                onChange={(_, val) => val && setMetroView(val)}
                size="small"
              >
                <ToggleButton value="table">
                  <TableChart fontSize="small" />
                </ToggleButton>
                <ToggleButton value="chart">
                  <ShowChart fontSize="small" />
                </ToggleButton>
              </ToggleButtonGroup>
          </Box>

          {loadingMetros ? (
            <Box display="flex" justifyContent="center" py={8}>
              <CircularProgress />
            </Box>
          ) : metros ? (
            <>
              {/* Metro Timeline - Always show National + selected metros */}
              {nationalTimeline && nationalTimeline.timeline.length > 0 && (
                <Card sx={{ mb: 3 }}>
                  <CardContent>
                    <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
                      Metro Area Unemployment Trends vs National
                    </Typography>
                    <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                      Click metro areas in the table below to add them to the chart
                    </Typography>
                    <Box sx={{ height: 300, mb: 2 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={(() => {
                          // Use national timeline as base to ensure continuous national line
                          // Then add metro data where available
                          const metroMap = new Map<string, Record<string, number>>();
                          if (metroTimeline && metroTimeline.timeline.length > 0) {
                            metroTimeline.timeline.forEach((p: any) => {
                              metroMap.set(`${p.year}-${p.period}`, p.metros || {});
                            });
                          }
                          return nationalTimeline.timeline.map((p: any) => ({
                            year: p.year,
                            period: p.period,
                            period_name: p.period_name,
                            national: p.headline_value,
                            metros: metroMap.get(`${p.year}-${p.period}`) || {}
                          }));
                        })()}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis
                            dataKey="period_name"
                            tick={{ fontSize: 10 }}
                            angle={-45}
                            textAnchor="end"
                            height={80}
                            interval={Math.floor(nationalTimeline.timeline.length / 12)}
                          />
                          <YAxis tick={{ fontSize: 11 }} label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }} />
                          <Tooltip />
                          <Legend wrapperStyle={{ fontSize: '10px' }} />
                          {/* Always show National line */}
                          <Line
                            type="monotone"
                            dataKey="national"
                            stroke="#000000"
                            strokeWidth={3}
                            dot={false}
                            name="National (US)"
                          />
                          {/* Show selected metros */}
                          {selectedMetrosForTimeline.slice(0, 10).map((areaCode, idx) => {
                            const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1', '#c62828', '#5e35b1', '#00796b', '#f9a825'];
                            return (
                              <Line
                                key={areaCode}
                                type="monotone"
                                dataKey={(point: any) => point.metros?.[areaCode]}
                                stroke={colors[idx % colors.length]}
                                strokeWidth={2}
                                dot={false}
                                name={metroTimeline?.metro_names?.[areaCode]?.substring(0, 30) || areaCode}
                              />
                            );
                          })}
                        </LineChart>
                      </ResponsiveContainer>
                    </Box>
                    {metroTimeline && metroTimeline.timeline.length > 0 && (
                      <TimelineSelector
                        timeline={metroTimeline.timeline}
                        selectedPeriod={selectedMetroPeriod}
                        onSelectPeriod={setSelectedMetroPeriod}
                      />
                    )}
                  </CardContent>
                </Card>
              )}

              {/* Metro Data Table/Chart */}
              <Card>
                <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                  <Typography variant="subtitle2" fontWeight="600">
                    Metropolitan Area Data ({metros.total_count} metros)
                  </Typography>
                  {selectedMetroPeriod && (
                    <Typography variant="caption" color="primary.main" sx={{ display: 'block', mt: 0.5 }}>
                      Showing data for: {formatPeriod(selectedMetroPeriod.period)} {selectedMetroPeriod.year}
                    </Typography>
                  )}
                </Box>
                {metroView === 'table' ? (
                  <>
                  <TableContainer>
                    <Table size="small" stickyHeader>
                      <TableHead>
                        <TableRow>
                          <TableCell padding="checkbox">
                            <Checkbox size="small" disabled />
                          </TableCell>
                          <TableCell sx={{ fontWeight: 600 }}>Metro Area</TableCell>
                          <TableCell align="right" sx={{ fontWeight: 600 }}>Unemp. Rate</TableCell>
                          <TableCell align="right" sx={{ fontWeight: 600 }}>Labor Force (K)</TableCell>
                          {!selectedMetroPeriod && (
                            <>
                              <TableCell align="right" sx={{ fontWeight: 600 }}>M/M</TableCell>
                              <TableCell align="right" sx={{ fontWeight: 600 }}>Y/Y</TableCell>
                            </>
                          )}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {(() => {
                          let displayMetros = metros.metros;

                          if (selectedMetroPeriod && metroTimeline) {
                            const timelinePoint = metroTimeline.timeline.find(
                              p => p.year === selectedMetroPeriod.year && p.period === selectedMetroPeriod.period
                            );

                            if (timelinePoint) {
                              displayMetros = metros.metros.map(metro => ({
                                ...metro,
                                unemployment_rate: timelinePoint.metros[metro.area_code] ?? metro.unemployment_rate,
                              })).sort((a, b) => (b.unemployment_rate || 0) - (a.unemployment_rate || 0));
                            }
                          }

                          return displayMetros
                            .slice(metroPage * metroRowsPerPage, metroPage * metroRowsPerPage + metroRowsPerPage)
                            .map((metro) => (
                            <TableRow
                              key={metro.area_code}
                              sx={{
                                cursor: 'pointer',
                                '&:hover': { bgcolor: 'action.hover' },
                                bgcolor: selectedMetrosForTimeline.includes(metro.area_code) ? 'action.selected' : 'inherit',
                              }}
                              onClick={() => {
                                if (selectedMetrosForTimeline.includes(metro.area_code)) {
                                  setSelectedMetrosForTimeline(selectedMetrosForTimeline.filter(c => c !== metro.area_code));
                                } else if (selectedMetrosForTimeline.length < 10) {
                                  setSelectedMetrosForTimeline([...selectedMetrosForTimeline, metro.area_code]);
                                }
                              }}
                            >
                              <TableCell padding="checkbox">
                                <Checkbox
                                  size="small"
                                  checked={selectedMetrosForTimeline.includes(metro.area_code)}
                                  disabled={!selectedMetrosForTimeline.includes(metro.area_code) && selectedMetrosForTimeline.length >= 10}
                                />
                              </TableCell>
                              <TableCell sx={{ fontSize: '0.85rem' }}>{metro.area_name}</TableCell>
                              <TableCell align="right">
                                <Chip
                                  label={formatRate(metro.unemployment_rate)}
                                  size="small"
                                  sx={{
                                    bgcolor: (metro.unemployment_rate || 0) > 5 ? 'error.100' : (metro.unemployment_rate || 0) > 4 ? 'warning.100' : 'success.100',
                                    fontWeight: 600,
                                  }}
                                />
                              </TableCell>
                              <TableCell align="right">{formatNumber(metro.labor_force)}</TableCell>
                              {!selectedMetroPeriod && (
                                <>
                                  <TableCell align="right">
                                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                      {metro.month_over_month && (metro.month_over_month >= 0 ? <TrendingUp fontSize="small" color="error" /> : <TrendingDown fontSize="small" color="success" />)}
                                      {formatChange(metro.month_over_month)}
                                    </Box>
                                  </TableCell>
                                  <TableCell align="right">
                                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                      {metro.year_over_year && (metro.year_over_year >= 0 ? <TrendingUp fontSize="small" color="error" /> : <TrendingDown fontSize="small" color="success" />)}
                                      {formatChange(metro.year_over_year)}
                                    </Box>
                                  </TableCell>
                                </>
                              )}
                            </TableRow>
                          ));
                        })()}
                      </TableBody>
                    </Table>
                  </TableContainer>
                  <TablePagination
                    component="div"
                    count={metros.metros.length}
                    page={metroPage}
                    onPageChange={(_, newPage) => setMetroPage(newPage)}
                    rowsPerPage={metroRowsPerPage}
                    onRowsPerPageChange={(e) => {
                      setMetroRowsPerPage(parseInt(e.target.value, 10));
                      setMetroPage(0);
                    }}
                    rowsPerPageOptions={[25, 50, 100]}
                  />
                  </>
                ) : (
                  <Box sx={{ p: 2, height: 500 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={(() => {
                          let displayMetros = metros.metros.slice(0, 20);
                          if (selectedMetroPeriod && metroTimeline) {
                            const timelinePoint = metroTimeline.timeline.find(
                              p => p.year === selectedMetroPeriod.year && p.period === selectedMetroPeriod.period
                            );
                            if (timelinePoint) {
                              displayMetros = metros.metros.map(metro => ({
                                ...metro,
                                unemployment_rate: timelinePoint.metros[metro.area_code] ?? metro.unemployment_rate,
                              })).sort((a, b) => (b.unemployment_rate || 0) - (a.unemployment_rate || 0)).slice(0, 20);
                            }
                          }
                          return displayMetros.map(m => ({
                            ...m,
                            short_name: m.area_name.length > 30 ? m.area_name.substring(0, 27) + '...' : m.area_name
                          }));
                        })()}
                        layout="vertical"
                        margin={{ left: 180 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" tick={{ fontSize: 11 }} label={{ value: 'Unemployment Rate (%)', position: 'insideBottom', offset: -5, style: { fontSize: 11 } }} />
                        <YAxis type="category" dataKey="short_name" tick={{ fontSize: 9 }} width={170} />
                        <Tooltip />
                        <Bar dataKey="unemployment_rate" fill="#2e7d32" name="Unemployment Rate (%)" />
                      </BarChart>
                    </ResponsiveContainer>
                  </Box>
                )}
              </Card>
            </>
          ) : null}
        </CardContent>
      </Card>

      {/* Series Detail Explorer Section */}
      <Card sx={{ mb: 4, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '3px solid', borderColor: 'info.main', bgcolor: 'info.50' }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'info.main' }}>
            Series Detail Explorer
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* Filters */}
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2, mb: 3 }}>
            <FormControl size="small" fullWidth>
              <InputLabel>Area</InputLabel>
              <Select
                value={selectedArea}
                label="Area"
                onChange={(e) => setSelectedArea(e.target.value)}
              >
                <MenuItem value="">All Areas</MenuItem>
                {dimensions?.areas.slice(0, 100).map((area) => (
                  <MenuItem key={area.area_code} value={area.area_code}>
                    {area.area_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Measure</InputLabel>
              <Select
                value={selectedMeasure}
                label="Measure"
                onChange={(e) => setSelectedMeasure(e.target.value)}
              >
                <MenuItem value="">All Measures</MenuItem>
                {dimensions?.measures.map((measure) => (
                  <MenuItem key={measure.measure_code} value={measure.measure_code}>
                    {measure.measure_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Seasonal Adjustment</InputLabel>
              <Select
                value={selectedSeasonal}
                label="Seasonal Adjustment"
                onChange={(e) => setSelectedSeasonal(e.target.value)}
              >
                <MenuItem value="">All</MenuItem>
                <MenuItem value="S">Seasonally Adjusted</MenuItem>
                <MenuItem value="U">Not Adjusted</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {/* Series List */}
          <Card variant="outlined" sx={{ mb: 3 }}>
            <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
              <Typography variant="subtitle2" fontWeight="600">
                Available Series ({seriesData?.total || 0})
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Select series to visualize (max 5)
              </Typography>
            </Box>
            <TableContainer sx={{ maxHeight: 400 }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell padding="checkbox">
                      <Checkbox size="small" disabled />
                    </TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Area</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Measure</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Seasonal</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Period</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {loadingSeries ? (
                    <TableRow>
                      <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                        <CircularProgress size={24} />
                      </TableCell>
                    </TableRow>
                  ) : seriesData?.series.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                        <Typography variant="body2" color="text.secondary">
                          No series found
                        </Typography>
                      </TableCell>
                    </TableRow>
                  ) : (
                    seriesData?.series.map((series: LASeriesInfo) => (
                      <TableRow
                        key={series.series_id}
                        sx={{
                          cursor: 'pointer',
                          bgcolor: selectedSeriesIds.includes(series.series_id) ? 'action.selected' : 'inherit',
                          '&:hover': { bgcolor: 'action.hover' },
                        }}
                        onClick={() => {
                          if (selectedSeriesIds.includes(series.series_id)) {
                            setSelectedSeriesIds(selectedSeriesIds.filter(id => id !== series.series_id));
                          } else if (selectedSeriesIds.length < 5) {
                            setSelectedSeriesIds([...selectedSeriesIds, series.series_id]);
                          }
                        }}
                      >
                        <TableCell padding="checkbox">
                          <Checkbox
                            size="small"
                            checked={selectedSeriesIds.includes(series.series_id)}
                            disabled={!selectedSeriesIds.includes(series.series_id) && selectedSeriesIds.length >= 5}
                          />
                        </TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }}>{series.area_name}</TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }}>{series.measure_name}</TableCell>
                        <TableCell>
                          <Chip
                            label={series.seasonal_code === 'S' ? 'SA' : 'NSA'}
                            size="small"
                            sx={{ fontSize: '0.65rem', height: 20 }}
                          />
                        </TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }}>
                          {series.begin_year} - {series.end_year || 'Present'}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </Card>

          {/* Charts for Selected Series */}
          {selectedSeriesIds.length > 0 && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              {selectedSeriesIds.map((seriesId, idx) => {
                const queryResult = seriesDataQueries[idx];
                const chartData = queryResult?.data;

                return (
                  <Card key={seriesId} variant="outlined">
                    <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <Box>
                        <Typography variant="subtitle2" fontWeight="600">
                          {chartData?.series[0]?.area_name || seriesId}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {chartData?.series[0]?.measure_name}
                        </Typography>
                      </Box>
                      <Button
                        size="small"
                        onClick={() => setSelectedSeriesIds(selectedSeriesIds.filter(id => id !== seriesId))}
                      >
                        Remove
                      </Button>
                    </Box>
                    <Box sx={{ p: 2, height: 300 }}>
                      {queryResult?.isLoading ? (
                        <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                          <CircularProgress />
                        </Box>
                      ) : chartData?.series[0] ? (
                        <ResponsiveContainer width="100%" height="100%">
                          <LineChart data={chartData.series[0].data_points}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                            <XAxis
                              dataKey="period_name"
                              tick={{ fontSize: 10 }}
                              angle={-45}
                              textAnchor="end"
                              height={80}
                              interval={Math.floor(chartData.series[0].data_points.length / 12)}
                            />
                            <YAxis tick={{ fontSize: 11 }} />
                            <Tooltip />
                            <Legend />
                            <Line
                              type="monotone"
                              dataKey="value"
                              stroke="#1976d2"
                              strokeWidth={2}
                              dot={{ r: 2 }}
                              name="Value"
                            />
                          </LineChart>
                        </ResponsiveContainer>
                      ) : null}
                    </Box>
                  </Card>
                );
              })}
            </Box>
          )}
        </CardContent>
      </Card>
    </Box>
  );
}
