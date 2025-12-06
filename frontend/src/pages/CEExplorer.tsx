import { useState } from 'react';
import { useQuery, useQueries } from '@tanstack/react-query';
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
  ToggleButtonGroup,
  ToggleButton,
  Checkbox,
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
import { TrendingUp, TrendingDown, Remove, AttachMoney, ShowChart, TableChart } from '@mui/icons-material';
import { ceExplorerAPI } from '../api/client';
import type { CESeriesInfo, CESupersectorMetric, CEIndustryMetric, CEEarningsMetric } from '../api/client';

// Color palette for charts
const COLORS = ['#1976d2', '#2e7d32', '#ed6c02', '#9c27b0', '#d32f2f', '#0288d1', '#689f38', '#f57c00'];

// Helper to format large numbers
const formatNumber = (value: number | undefined | null): string => {
  if (value === undefined || value === null) return 'N/A';
  if (value >= 1000) {
    return `${(value / 1000).toFixed(1)}M`;
  }
  return `${value.toLocaleString()}K`;
};

// Helper to format change values
const formatChange = (value: number | undefined | null, suffix = ''): string => {
  if (value === undefined || value === null) return 'N/A';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(1)}${suffix}`;
};

// Change indicator component
const ChangeIndicator = ({ value, suffix = '' }: { value: number | undefined | null; suffix?: string }) => {
  if (value === undefined || value === null) return <Typography variant="body2" color="text.secondary">N/A</Typography>;

  const isPositive = value > 0;
  const isNegative = value < 0;
  const color = isPositive ? 'success.main' : isNegative ? 'error.main' : 'text.secondary';
  const Icon = isPositive ? TrendingUp : isNegative ? TrendingDown : Remove;

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
      <Icon sx={{ fontSize: 16, color }} />
      <Typography variant="body2" sx={{ color, fontWeight: 600 }}>
        {formatChange(value, suffix)}
      </Typography>
    </Box>
  );
};

// Helper to format period
const formatPeriod = (period: string) => {
  const monthMap: Record<string, string> = {
    'M01': 'Jan', 'M02': 'Feb', 'M03': 'Mar', 'M04': 'Apr',
    'M05': 'May', 'M06': 'Jun', 'M07': 'Jul', 'M08': 'Aug',
    'M09': 'Sep', 'M10': 'Oct', 'M11': 'Nov', 'M12': 'Dec',
  };
  return monthMap[period] || period;
};

// Timeline Selector Component (reusable)
const TimelineSelector = ({
  timeline,
  selectedPeriod,
  onSelectPeriod
}: {
  timeline: Array<{ year: number; period: string; period_name?: string }>,
  selectedPeriod: { year: number; period: string } | null,
  onSelectPeriod: (period: { year: number; period: string }) => void
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

export default function CEExplorer() {
  // Overview state
  const [overviewTimeRange, setOverviewTimeRange] = useState<number>(24);
  const [selectedOverviewPeriod, setSelectedOverviewPeriod] = useState<{ year: number; period: string } | null>(null);

  // Supersector state
  const [supersectorTimeRange, setSupersectorTimeRange] = useState<number>(24);
  const [selectedSupersectors, setSelectedSupersectors] = useState<string[]>(['30', '20', '40']); // Manufacturing, Construction, Trade
  const [selectedSupersectorPeriod, setSelectedSupersectorPeriod] = useState<{ year: number; period: string } | null>(null);

  // Industry state
  const [industryDisplayLevel, setIndustryDisplayLevel] = useState<number | ''>('');
  const [industrySupersector, setIndustrySupersector] = useState<string>('');
  const [industryLimit, setIndustryLimit] = useState<number>(20);

  // Earnings state
  const [earningsSupersector, setEarningsSupersector] = useState<string>('');
  const [earningsLimit, setEarningsLimit] = useState<number>(20);
  const [selectedEarningsIndustry, setSelectedEarningsIndustry] = useState<string | null>(null);
  const [earningsTimeRange, setEarningsTimeRange] = useState<number>(24);
  const [selectedEarningsPeriod, setSelectedEarningsPeriod] = useState<{ year: number; period: string } | null>(null);

  // Series explorer state
  const [selectedIndustry, setSelectedIndustry] = useState<string>('');
  const [selectedSupersectorFilter, setSelectedSupersectorFilter] = useState<string>('');
  const [selectedDataTypeFilter, setSelectedDataTypeFilter] = useState<string>('');
  const [selectedSeasonal, setSelectedSeasonal] = useState<string>('');
  const [selectedSeriesIds, setSelectedSeriesIds] = useState<string[]>([]);
  const [seriesTimeRange, setSeriesTimeRange] = useState<number>(24);
  const [seriesView, setSeriesView] = useState<'chart' | 'table'>('chart');

  // Fetch dimensions
  const { data: dimensions, isLoading: loadingDimensions, error: dimensionsError } = useQuery({
    queryKey: ['ce', 'dimensions'],
    queryFn: ceExplorerAPI.getDimensions,
    staleTime: 0,  // Always refetch on mount
  });

  // Fetch overview data
  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['ce', 'overview'],
    queryFn: ceExplorerAPI.getOverview,
  });

  // Fetch overview timeline
  const { data: overviewTimeline } = useQuery({
    queryKey: ['ce', 'overview', 'timeline', overviewTimeRange],
    queryFn: () => ceExplorerAPI.getOverviewTimeline(overviewTimeRange),
  });

  // Fetch supersector analysis
  const { data: supersectors, isLoading: loadingSupersectors } = useQuery({
    queryKey: ['ce', 'supersectors'],
    queryFn: ceExplorerAPI.getSupersectors,
  });

  // Fetch supersector timeline
  const { data: supersectorTimeline } = useQuery({
    queryKey: ['ce', 'supersectors', 'timeline', selectedSupersectors.join(','), supersectorTimeRange],
    queryFn: () => ceExplorerAPI.getSupersectorsTimeline({
      supersector_codes: selectedSupersectors.join(','),
      months_back: supersectorTimeRange,
    }),
    enabled: selectedSupersectors.length > 0,
  });

  // Fetch industry analysis
  const { data: industries, isLoading: loadingIndustries } = useQuery({
    queryKey: ['ce', 'industries', industryDisplayLevel, industrySupersector, industryLimit],
    queryFn: () => ceExplorerAPI.getIndustries({
      display_level: industryDisplayLevel === '' ? undefined : industryDisplayLevel,
      supersector_code: industrySupersector || undefined,
      limit: industryLimit,
    }),
  });

  // Fetch earnings analysis
  const { data: earnings, isLoading: loadingEarnings } = useQuery({
    queryKey: ['ce', 'earnings', earningsSupersector, earningsLimit],
    queryFn: () => ceExplorerAPI.getEarnings({
      supersector_code: earningsSupersector || undefined,
      limit: earningsLimit,
    }),
  });

  // Fetch earnings timeline for selected industry
  const { data: earningsTimeline } = useQuery({
    queryKey: ['ce', 'earnings', 'timeline', selectedEarningsIndustry, earningsTimeRange],
    queryFn: () => ceExplorerAPI.getEarningsTimeline(selectedEarningsIndustry!, earningsTimeRange),
    enabled: !!selectedEarningsIndustry,
  });

  // Fetch series list
  const { data: seriesData, isLoading: loadingSeries } = useQuery({
    queryKey: ['ce', 'series', selectedIndustry, selectedSupersectorFilter, selectedDataTypeFilter, selectedSeasonal],
    queryFn: () =>
      ceExplorerAPI.getSeries({
        industry_code: selectedIndustry || undefined,
        supersector_code: selectedSupersectorFilter || undefined,
        data_type_code: selectedDataTypeFilter || undefined,
        seasonal_code: selectedSeasonal || undefined,
        active_only: true,
        limit: 50,
      }),
  });

  // Fetch data for selected series (multiple)
  const seriesDataQueries = useQueries({
    queries: selectedSeriesIds.map(seriesId => ({
      queryKey: ['ce', 'data', seriesId],
      queryFn: () => ceExplorerAPI.getSeriesData(seriesId),
      enabled: true,
    })),
  });

  if (loadingDimensions || !dimensions) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <CircularProgress size={40} />
      </Box>
    );
  }

  if (dimensionsError) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <Typography color="error">Failed to load CE dimensions. Please try again.</Typography>
      </Box>
    );
  }

  const selectableIndustries = dimensions.industries.filter(i => i.selectable);

  // Toggle supersector selection for timeline
  const toggleSupersector = (code: string) => {
    setSelectedSupersectors(prev =>
      prev.includes(code)
        ? prev.filter(c => c !== code)
        : prev.length < 6 ? [...prev, code] : prev
    );
  };

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h5" fontWeight="700" sx={{ color: 'text.primary', mb: 0.5 }}>
          CE - Current Employment Statistics Explorer
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Explore employment data by industry and supersector
        </Typography>
      </Box>

      {/* Section 1: Overview */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2,
          borderBottom: '3px solid',
          borderColor: 'primary.main',
          bgcolor: 'primary.50',
        }}>
          <Typography variant="h6" fontWeight="700" sx={{ color: 'primary.main' }}>
            Employment Overview
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {loadingOverview ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : (
            <>
              {/* Headline Metrics Cards */}
              <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 2, mb: 3 }}>
                {[
                  { key: 'total_nonfarm', label: 'Total Nonfarm', data: overview?.total_nonfarm, color: '#1976d2' },
                  { key: 'total_private', label: 'Total Private', data: overview?.total_private, color: '#2e7d32' },
                  { key: 'goods_producing', label: 'Goods-Producing', data: overview?.goods_producing, color: '#ed6c02' },
                  { key: 'service_providing', label: 'Service-Providing', data: overview?.service_providing, color: '#9c27b0' },
                  { key: 'government', label: 'Government', data: overview?.government, color: '#0288d1' },
                ].map(({ key, label, data, color }) => (
                  <Card key={key} sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
                    <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                      <Typography variant="caption" color="text.secondary" fontWeight={500} sx={{ display: 'block', mb: 0.5 }}>
                        {label}
                      </Typography>
                      <Typography variant="h5" fontWeight="700" sx={{ color, mb: 1 }}>
                        {formatNumber(data?.latest_value)}
                      </Typography>
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <Typography variant="caption" color="text.secondary">MoM</Typography>
                          <ChangeIndicator value={data?.month_over_month} suffix="K" />
                        </Box>
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <Typography variant="caption" color="text.secondary">YoY</Typography>
                          <ChangeIndicator value={data?.year_over_year} suffix="K" />
                        </Box>
                      </Box>
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1, fontSize: '0.65rem' }}>
                        {data?.latest_date}
                      </Typography>
                    </CardContent>
                  </Card>
                ))}
              </Box>

              {/* Timeline Chart */}
              <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Typography variant="subtitle2" fontWeight="600">
                  Employment Trends (Thousands)
                </Typography>
                <FormControl size="small" sx={{ minWidth: 150 }}>
                  <InputLabel>Time Range</InputLabel>
                  <Select
                    value={overviewTimeRange}
                    label="Time Range"
                    onChange={(e) => setOverviewTimeRange(e.target.value as number)}
                  >
                    <MenuItem value={12}>Last 12 months</MenuItem>
                    <MenuItem value={24}>Last 2 years</MenuItem>
                    <MenuItem value={60}>Last 5 years</MenuItem>
                    <MenuItem value={120}>Last 10 years</MenuItem>
                    <MenuItem value={240}>Last 20 years</MenuItem>
                    <MenuItem value={0}>All Time</MenuItem>
                  </Select>
                </FormControl>
              </Box>

              {overviewTimeline && overviewTimeline.timeline.length > 0 && (
                <>
                  <Box sx={{ width: '100%', height: 300 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={overviewTimeline.timeline}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                        <XAxis
                          dataKey="period_name"
                          tick={{ fontSize: 10 }}
                          angle={-45}
                          textAnchor="end"
                          height={70}
                        />
                        <YAxis
                          tick={{ fontSize: 11 }}
                          tickFormatter={(v) => `${(v / 1000).toFixed(0)}M`}
                          domain={['dataMin - 1000', 'dataMax + 1000']}
                        />
                        <Tooltip
                          formatter={(value: number) => [`${value.toLocaleString()}K`, '']}
                          labelFormatter={(label) => label}
                        />
                        <Legend />
                        <Line type="monotone" dataKey="total_nonfarm" stroke="#1976d2" name="Total Nonfarm" strokeWidth={2} dot={false} />
                        <Line type="monotone" dataKey="total_private" stroke="#2e7d32" name="Total Private" strokeWidth={2} dot={false} />
                        <Line type="monotone" dataKey="goods_producing" stroke="#ed6c02" name="Goods-Producing" strokeWidth={1.5} dot={false} />
                        <Line type="monotone" dataKey="service_providing" stroke="#9c27b0" name="Service-Providing" strokeWidth={1.5} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </Box>

                  {/* Timeline Selector */}
                  <TimelineSelector
                    timeline={overviewTimeline.timeline}
                    selectedPeriod={selectedOverviewPeriod}
                    onSelectPeriod={setSelectedOverviewPeriod}
                  />

                  {/* Selected Period Details */}
                  {selectedOverviewPeriod && (() => {
                    const point = overviewTimeline.timeline.find(
                      p => p.year === selectedOverviewPeriod.year && p.period === selectedOverviewPeriod.period
                    );
                    if (!point) return null;
                    return (
                      <Card variant="outlined" sx={{ mt: 2, p: 2, bgcolor: 'primary.50' }}>
                        <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
                          Snapshot: {point.period_name}
                        </Typography>
                        <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 2 }}>
                          <Box>
                            <Typography variant="caption" color="text.secondary">Total Nonfarm</Typography>
                            <Typography variant="body1" fontWeight="600">{formatNumber(point.total_nonfarm)}</Typography>
                          </Box>
                          <Box>
                            <Typography variant="caption" color="text.secondary">Total Private</Typography>
                            <Typography variant="body1" fontWeight="600">{formatNumber(point.total_private)}</Typography>
                          </Box>
                          <Box>
                            <Typography variant="caption" color="text.secondary">Goods-Producing</Typography>
                            <Typography variant="body1" fontWeight="600">{formatNumber(point.goods_producing)}</Typography>
                          </Box>
                          <Box>
                            <Typography variant="caption" color="text.secondary">Service-Providing</Typography>
                            <Typography variant="body1" fontWeight="600">{formatNumber(point.service_providing)}</Typography>
                          </Box>
                        </Box>
                      </Card>
                    );
                  })()}
                </>
              )}
            </>
          )}
        </CardContent>
      </Card>

      {/* Section 2: Supersector Analysis */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2,
          borderBottom: '3px solid',
          borderColor: 'success.main',
          bgcolor: 'success.50',
        }}>
          <Typography variant="h6" fontWeight="700" sx={{ color: 'success.main' }}>
            Supersector Analysis
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {loadingSupersectors ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : (
            <>
              {/* Supersector Bar Chart */}
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                  Employment by Supersector (Thousands)
                </Typography>
                <Box sx={{ width: '100%', height: 300 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={[...(supersectors?.supersectors || [])].sort((a, b) => (b.latest_value || 0) - (a.latest_value || 0))}
                      layout="vertical"
                      margin={{ left: 150 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                      <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v.toLocaleString()}`} />
                      <YAxis
                        type="category"
                        dataKey="supersector_name"
                        tick={{ fontSize: 10 }}
                        width={140}
                      />
                      <Tooltip formatter={(value: number) => [`${value.toLocaleString()}K`, 'Employment']} />
                      <Bar dataKey="latest_value" fill="#2e7d32">
                        {supersectors?.supersectors.map((_, index) => (
                          <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </Box>
              </Box>

              {/* Supersector Table */}
              <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                Supersector Details
              </Typography>
              <TableContainer>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Supersector</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Employment</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>MoM Change</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>MoM %</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>YoY Change</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>YoY %</TableCell>
                      <TableCell align="center" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Compare</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {supersectors?.supersectors.map((ss: CESupersectorMetric) => (
                      <TableRow key={ss.supersector_code} hover>
                        <TableCell sx={{ fontSize: '0.8rem' }}>{ss.supersector_name}</TableCell>
                        <TableCell align="right" sx={{ fontSize: '0.8rem', fontWeight: 600 }}>
                          {ss.latest_value?.toLocaleString()}K
                        </TableCell>
                        <TableCell align="right">
                          <ChangeIndicator value={ss.month_over_month} suffix="K" />
                        </TableCell>
                        <TableCell align="right">
                          <ChangeIndicator value={ss.month_over_month_pct} suffix="%" />
                        </TableCell>
                        <TableCell align="right">
                          <ChangeIndicator value={ss.year_over_year} suffix="K" />
                        </TableCell>
                        <TableCell align="right">
                          <ChangeIndicator value={ss.year_over_year_pct} suffix="%" />
                        </TableCell>
                        <TableCell align="center">
                          <Chip
                            label={selectedSupersectors.includes(ss.supersector_code) ? 'Selected' : 'Add'}
                            size="small"
                            color={selectedSupersectors.includes(ss.supersector_code) ? 'primary' : 'default'}
                            onClick={() => toggleSupersector(ss.supersector_code)}
                            sx={{ fontSize: '0.65rem', height: 22 }}
                          />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>

              {/* Supersector Timeline Comparison */}
              {selectedSupersectors.length > 0 && supersectorTimeline && supersectorTimeline.timeline.length > 0 && (
                <Box sx={{ mt: 3 }}>
                  <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Typography variant="subtitle2" fontWeight="600">
                      Selected Supersectors Timeline
                    </Typography>
                    <FormControl size="small" sx={{ minWidth: 150 }}>
                      <InputLabel>Time Range</InputLabel>
                      <Select
                        value={supersectorTimeRange}
                        label="Time Range"
                        onChange={(e) => setSupersectorTimeRange(e.target.value as number)}
                      >
                        <MenuItem value={12}>Last 12 months</MenuItem>
                        <MenuItem value={24}>Last 2 years</MenuItem>
                        <MenuItem value={60}>Last 5 years</MenuItem>
                        <MenuItem value={120}>Last 10 years</MenuItem>
                        <MenuItem value={240}>Last 20 years</MenuItem>
                        <MenuItem value={0}>All Time</MenuItem>
                      </Select>
                    </FormControl>
                  </Box>
                  <Box sx={{ width: '100%', height: 300 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={supersectorTimeline.timeline}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                        <XAxis
                          dataKey="period_name"
                          tick={{ fontSize: 10 }}
                          angle={-45}
                          textAnchor="end"
                          height={70}
                        />
                        <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `${v.toLocaleString()}`} />
                        <Tooltip formatter={(value: number) => [`${value?.toLocaleString() || 'N/A'}K`, '']} />
                        <Legend />
                        {selectedSupersectors.map((code, index) => (
                          <Line
                            key={code}
                            type="monotone"
                            dataKey={`supersectors.${code}`}
                            stroke={COLORS[index % COLORS.length]}
                            name={supersectorTimeline.supersector_names[code] || code}
                            strokeWidth={2}
                            dot={false}
                          />
                        ))}
                      </LineChart>
                    </ResponsiveContainer>
                  </Box>

                  {/* Timeline Selector */}
                  <TimelineSelector
                    timeline={supersectorTimeline.timeline}
                    selectedPeriod={selectedSupersectorPeriod}
                    onSelectPeriod={setSelectedSupersectorPeriod}
                  />

                  {/* Selected Period Details */}
                  {selectedSupersectorPeriod && (() => {
                    const point = supersectorTimeline.timeline.find(
                      (p: any) => p.year === selectedSupersectorPeriod.year && p.period === selectedSupersectorPeriod.period
                    );
                    if (!point) return null;
                    return (
                      <Card variant="outlined" sx={{ mt: 2, p: 2, bgcolor: 'success.50' }}>
                        <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
                          Snapshot: {point.period_name}
                        </Typography>
                        <Box sx={{ display: 'grid', gridTemplateColumns: `repeat(${Math.min(selectedSupersectors.length, 4)}, 1fr)`, gap: 2 }}>
                          {selectedSupersectors.map((code) => (
                            <Box key={code}>
                              <Typography variant="caption" color="text.secondary">
                                {supersectorTimeline.supersector_names[code] || code}
                              </Typography>
                              <Typography variant="body1" fontWeight="600">
                                {point.supersectors?.[code] != null
                                  ? `${point.supersectors[code].toLocaleString()}K`
                                  : 'N/A'}
                              </Typography>
                            </Box>
                          ))}
                        </Box>
                      </Card>
                    );
                  })()}
                </Box>
              )}
            </>
          )}
        </CardContent>
      </Card>

      {/* Section 3: Industry Analysis */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2,
          borderBottom: '3px solid',
          borderColor: 'warning.main',
          bgcolor: 'warning.50',
        }}>
          <Typography variant="h6" fontWeight="700" sx={{ color: 'warning.main' }}>
            Industry Analysis
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* Filters */}
          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <FormControl size="small" sx={{ minWidth: 150 }}>
              <InputLabel>Display Level</InputLabel>
              <Select
                value={industryDisplayLevel}
                label="Display Level"
                onChange={(e) => setIndustryDisplayLevel(e.target.value as number | '')}
              >
                <MenuItem value="">All Levels</MenuItem>
                <MenuItem value={1}>Level 1 (Broad)</MenuItem>
                <MenuItem value={2}>Level 2</MenuItem>
                <MenuItem value={3}>Level 3</MenuItem>
                <MenuItem value={4}>Level 4</MenuItem>
                <MenuItem value={5}>Level 5 (Detail)</MenuItem>
              </Select>
            </FormControl>

            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Supersector</InputLabel>
              <Select
                value={industrySupersector}
                label="Supersector"
                onChange={(e) => setIndustrySupersector(e.target.value)}
              >
                <MenuItem value="">All Supersectors</MenuItem>
                {dimensions?.supersectors.map((ss) => (
                  <MenuItem key={ss.supersector_code} value={ss.supersector_code}>
                    {ss.supersector_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Show</InputLabel>
              <Select
                value={industryLimit}
                label="Show"
                onChange={(e) => setIndustryLimit(e.target.value as number)}
              >
                <MenuItem value={20}>Top 20</MenuItem>
                <MenuItem value={50}>Top 50</MenuItem>
                <MenuItem value={100}>Top 100</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {loadingIndustries ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : (
            <TableContainer sx={{ maxHeight: 500 }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Industry</TableCell>
                    <TableCell align="center" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Level</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Employment</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>MoM Change</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>MoM %</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>YoY Change</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>YoY %</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {industries?.industries.map((ind: CEIndustryMetric) => (
                    <TableRow key={ind.industry_code} hover>
                      <TableCell sx={{ fontSize: '0.8rem' }}>
                        <Box sx={{ pl: (ind.display_level - 1) * 2 }}>
                          {ind.industry_name}
                        </Box>
                      </TableCell>
                      <TableCell align="center">
                        <Chip label={ind.display_level} size="small" sx={{ fontSize: '0.65rem', height: 20 }} />
                      </TableCell>
                      <TableCell align="right" sx={{ fontSize: '0.8rem', fontWeight: 600 }}>
                        {ind.latest_value?.toLocaleString()}K
                      </TableCell>
                      <TableCell align="right">
                        <ChangeIndicator value={ind.month_over_month} suffix="K" />
                      </TableCell>
                      <TableCell align="right">
                        <ChangeIndicator value={ind.month_over_month_pct} suffix="%" />
                      </TableCell>
                      <TableCell align="right">
                        <ChangeIndicator value={ind.year_over_year} suffix="K" />
                      </TableCell>
                      <TableCell align="right">
                        <ChangeIndicator value={ind.year_over_year_pct} suffix="%" />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}

          {industries && (
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2 }}>
              Showing {industries.industries.length} of {industries.total_count} industries • Data as of {industries.last_updated}
            </Typography>
          )}
        </CardContent>
      </Card>

      {/* Section 4: Earnings Analysis */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2,
          borderBottom: '3px solid',
          borderColor: 'secondary.main',
          bgcolor: 'secondary.50',
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <AttachMoney sx={{ color: 'secondary.main' }} />
            <Typography variant="h6" fontWeight="700" sx={{ color: 'secondary.main' }}>
              Earnings & Hours Analysis
            </Typography>
          </Box>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* Filters */}
          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Supersector</InputLabel>
              <Select
                value={earningsSupersector}
                label="Supersector"
                onChange={(e) => setEarningsSupersector(e.target.value)}
              >
                <MenuItem value="">All Supersectors</MenuItem>
                {dimensions?.supersectors.map((ss) => (
                  <MenuItem key={ss.supersector_code} value={ss.supersector_code}>
                    {ss.supersector_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Show</InputLabel>
              <Select
                value={earningsLimit}
                label="Show"
                onChange={(e) => setEarningsLimit(e.target.value as number)}
              >
                <MenuItem value={20}>Top 20</MenuItem>
                <MenuItem value={50}>Top 50</MenuItem>
                <MenuItem value={100}>Top 100</MenuItem>
              </Select>
            </FormControl>
          </Box>

          {loadingEarnings ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : (
            <>
              <TableContainer sx={{ maxHeight: 400 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Industry</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Hourly ($)</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Weekly ($)</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Hours/Week</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Hourly MoM</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Hourly YoY</TableCell>
                      <TableCell align="center" sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Trend</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {earnings?.earnings.map((e: CEEarningsMetric) => (
                      <TableRow
                        key={e.industry_code}
                        hover
                        sx={{
                          cursor: 'pointer',
                          bgcolor: selectedEarningsIndustry === e.industry_code ? 'action.selected' : 'inherit',
                        }}
                        onClick={() => setSelectedEarningsIndustry(e.industry_code)}
                      >
                        <TableCell sx={{ fontSize: '0.8rem' }}>{e.industry_name}</TableCell>
                        <TableCell align="right" sx={{ fontSize: '0.8rem', fontWeight: 600 }}>
                          ${e.avg_hourly_earnings?.toFixed(2) || 'N/A'}
                        </TableCell>
                        <TableCell align="right" sx={{ fontSize: '0.8rem' }}>
                          ${e.avg_weekly_earnings?.toFixed(0) || 'N/A'}
                        </TableCell>
                        <TableCell align="right" sx={{ fontSize: '0.8rem' }}>
                          {e.avg_weekly_hours?.toFixed(1) || 'N/A'}
                        </TableCell>
                        <TableCell align="right">
                          <ChangeIndicator value={e.hourly_mom_pct} suffix="%" />
                        </TableCell>
                        <TableCell align="right">
                          <ChangeIndicator value={e.hourly_yoy_pct} suffix="%" />
                        </TableCell>
                        <TableCell align="center">
                          <Chip
                            label={selectedEarningsIndustry === e.industry_code ? 'Selected' : 'View'}
                            size="small"
                            color={selectedEarningsIndustry === e.industry_code ? 'secondary' : 'default'}
                            sx={{ fontSize: '0.65rem', height: 22 }}
                          />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>

              {earnings && (
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2 }}>
                  Showing {earnings.earnings.length} of {earnings.total_count} industries • Data as of {earnings.last_updated}
                </Typography>
              )}

              {/* Earnings Timeline Chart */}
              {selectedEarningsIndustry && earningsTimeline && earningsTimeline.timeline.length > 0 && (
                <Box sx={{ mt: 3 }}>
                  <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Typography variant="subtitle2" fontWeight="600">
                      Earnings Trend: {earningsTimeline.industry_name}
                    </Typography>
                    <FormControl size="small" sx={{ minWidth: 150 }}>
                      <InputLabel>Time Range</InputLabel>
                      <Select
                        value={earningsTimeRange}
                        label="Time Range"
                        onChange={(e) => setEarningsTimeRange(e.target.value as number)}
                      >
                        <MenuItem value={12}>Last 12 months</MenuItem>
                        <MenuItem value={24}>Last 2 years</MenuItem>
                        <MenuItem value={60}>Last 5 years</MenuItem>
                        <MenuItem value={120}>Last 10 years</MenuItem>
                        <MenuItem value={240}>Last 20 years</MenuItem>
                        <MenuItem value={0}>All Time</MenuItem>
                      </Select>
                    </FormControl>
                  </Box>
                  <Box sx={{ width: '100%', height: 300 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={earningsTimeline.timeline}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                        <XAxis
                          dataKey="period_name"
                          tick={{ fontSize: 10 }}
                          angle={-45}
                          textAnchor="end"
                          height={70}
                        />
                        <YAxis
                          yAxisId="left"
                          tick={{ fontSize: 11 }}
                          tickFormatter={(v) => `$${v}`}
                          domain={['auto', 'auto']}
                        />
                        <YAxis
                          yAxisId="right"
                          orientation="right"
                          tick={{ fontSize: 11 }}
                          domain={['auto', 'auto']}
                        />
                        <Tooltip
                          formatter={(value: number, name: string) => {
                            if (name === 'Hours/Week') return [value?.toFixed(1), name];
                            return [`$${value?.toFixed(2)}`, name];
                          }}
                        />
                        <Legend />
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey="avg_hourly_earnings"
                          stroke="#9c27b0"
                          name="Hourly Earnings"
                          strokeWidth={2}
                          dot={false}
                        />
                        <Line
                          yAxisId="right"
                          type="monotone"
                          dataKey="avg_weekly_hours"
                          stroke="#2e7d32"
                          name="Hours/Week"
                          strokeWidth={2}
                          dot={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </Box>

                  {/* Timeline Selector */}
                  <TimelineSelector
                    timeline={earningsTimeline.timeline}
                    selectedPeriod={selectedEarningsPeriod}
                    onSelectPeriod={setSelectedEarningsPeriod}
                  />

                  {/* Selected Period Details */}
                  {selectedEarningsPeriod && (() => {
                    const point = earningsTimeline.timeline.find(
                      (p: any) => p.year === selectedEarningsPeriod.year && p.period === selectedEarningsPeriod.period
                    );
                    if (!point) return null;
                    return (
                      <Card variant="outlined" sx={{ mt: 2, p: 2, bgcolor: 'secondary.50' }}>
                        <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
                          {earningsTimeline.industry_name} - {point.period_name}
                        </Typography>
                        <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2 }}>
                          <Box>
                            <Typography variant="caption" color="text.secondary">Hourly Earnings</Typography>
                            <Typography variant="body1" fontWeight="600">
                              ${point.avg_hourly_earnings?.toFixed(2) || 'N/A'}
                            </Typography>
                          </Box>
                          <Box>
                            <Typography variant="caption" color="text.secondary">Weekly Earnings</Typography>
                            <Typography variant="body1" fontWeight="600">
                              ${point.avg_weekly_earnings?.toFixed(0) || 'N/A'}
                            </Typography>
                          </Box>
                          <Box>
                            <Typography variant="caption" color="text.secondary">Hours/Week</Typography>
                            <Typography variant="body1" fontWeight="600">
                              {point.avg_weekly_hours?.toFixed(1) || 'N/A'}
                            </Typography>
                          </Box>
                        </Box>
                      </Card>
                    );
                  })()}
                </Box>
              )}
            </>
          )}
        </CardContent>
      </Card>

      {/* Section 5: Series Explorer */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2,
          borderBottom: '3px solid',
          borderColor: 'info.main',
          bgcolor: 'info.50',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}>
          <Typography variant="h6" fontWeight="700" sx={{ color: 'info.main' }}>
            Series Explorer
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
            <FormControl size="small" sx={{ minWidth: 150 }}>
              <InputLabel>Time Range</InputLabel>
              <Select
                value={seriesTimeRange}
                label="Time Range"
                onChange={(e) => setSeriesTimeRange(e.target.value as number)}
              >
                <MenuItem value={12}>Last 12 months</MenuItem>
                <MenuItem value={24}>Last 2 years</MenuItem>
                <MenuItem value={60}>Last 5 years</MenuItem>
                <MenuItem value={120}>Last 10 years</MenuItem>
                <MenuItem value={240}>Last 20 years</MenuItem>
                <MenuItem value={0}>All Time</MenuItem>
              </Select>
            </FormControl>
            <ToggleButtonGroup
              value={seriesView}
              exclusive
              onChange={(_, val) => val && setSeriesView(val)}
              size="small"
            >
              <ToggleButton value="chart">
                <ShowChart fontSize="small" />
              </ToggleButton>
              <ToggleButton value="table">
                <TableChart fontSize="small" />
              </ToggleButton>
            </ToggleButtonGroup>
          </Box>
        </Box>
        <CardContent sx={{ p: 3 }}>
          {/* Filters */}
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 2, mb: 3 }}>
            <FormControl size="small" fullWidth>
              <InputLabel>Industry</InputLabel>
              <Select
                value={selectedIndustry}
                label="Industry"
                onChange={(e) => setSelectedIndustry(e.target.value)}
              >
                <MenuItem value="">All Industries</MenuItem>
                {selectableIndustries.map((industry) => (
                  <MenuItem key={industry.industry_code} value={industry.industry_code}>
                    {industry.industry_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Supersector</InputLabel>
              <Select
                value={selectedSupersectorFilter}
                label="Supersector"
                onChange={(e) => setSelectedSupersectorFilter(e.target.value)}
              >
                <MenuItem value="">All Supersectors</MenuItem>
                {dimensions?.supersectors.map((supersector) => (
                  <MenuItem key={supersector.supersector_code} value={supersector.supersector_code}>
                    {supersector.supersector_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl size="small" fullWidth>
              <InputLabel>Data Type</InputLabel>
              <Select
                value={selectedDataTypeFilter}
                label="Data Type"
                onChange={(e) => setSelectedDataTypeFilter(e.target.value)}
              >
                <MenuItem value="">All Data Types</MenuItem>
                {dimensions?.data_types.map((dt) => (
                  <MenuItem key={dt.data_type_code} value={dt.data_type_code}>
                    {dt.data_type_text.length > 40 ? dt.data_type_text.substring(0, 40) + '...' : dt.data_type_text}
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
            <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Box>
                <Typography variant="subtitle2" fontWeight="600">
                  Available Series ({seriesData?.total || 0})
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  Select series to compare (max 5)
                </Typography>
              </Box>
              {selectedSeriesIds.length > 0 && (
                <Button size="small" onClick={() => setSelectedSeriesIds([])} variant="outlined">
                  Clear All ({selectedSeriesIds.length})
                </Button>
              )}
            </Box>
            <TableContainer sx={{ maxHeight: 300 }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell padding="checkbox" sx={{ bgcolor: 'background.paper' }}>
                      <Checkbox size="small" disabled />
                    </TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Series ID</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Industry</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Data Type</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Seasonal</TableCell>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper' }}>Period</TableCell>
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
                    seriesData?.series.map((series: CESeriesInfo) => (
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
                        <TableCell sx={{ fontSize: '0.8rem', fontFamily: 'monospace' }}>
                          {series.series_id}
                        </TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }}>{series.industry_name}</TableCell>
                        <TableCell sx={{ fontSize: '0.7rem' }}>
                          {series.data_type_text
                            ? series.data_type_text.length > 30
                              ? series.data_type_text.substring(0, 30) + '...'
                              : series.data_type_text
                            : 'N/A'}
                        </TableCell>
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

          {/* Combined Chart or 2D Data Table for Selected Series */}
          {selectedSeriesIds.length > 0 && (
            <Card variant="outlined">
              <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                <Typography variant="subtitle2" fontWeight="600">
                  {seriesView === 'chart' ? 'Series Comparison Chart' : 'Series Data Table'}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {selectedSeriesIds.length} series selected
                </Typography>
              </Box>

              {seriesView === 'chart' ? (
                /* Single Chart with Multiple Lines */
                <Box sx={{ p: 2, height: 400 }}>
                  {seriesDataQueries.some(q => q.isLoading) ? (
                    <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                      <CircularProgress />
                    </Box>
                  ) : (
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                        <XAxis
                          dataKey="period_name"
                          type="category"
                          allowDuplicatedCategory={false}
                          tick={{ fontSize: 10 }}
                          angle={-45}
                          textAnchor="end"
                          height={80}
                        />
                        <YAxis tick={{ fontSize: 11 }} />
                        <Tooltip />
                        <Legend wrapperStyle={{ fontSize: '11px' }} />
                        {seriesDataQueries.map((queryResult, idx) => {
                          const chartData = queryResult?.data;
                          if (!chartData?.series[0]) return null;
                          const seriesInfo = seriesData?.series.find(s => s.series_id === selectedSeriesIds[idx]);
                          const label = seriesInfo
                            ? `${seriesInfo.industry_name} - ${seriesInfo.data_type_text?.substring(0, 20) || 'N/A'}`
                            : selectedSeriesIds[idx];
                          // Filter data by time range (0 = all time)
                          const filteredData = seriesTimeRange === 0
                            ? chartData.series[0].data_points
                            : chartData.series[0].data_points.slice(-seriesTimeRange);
                          return (
                            <Line
                              key={selectedSeriesIds[idx]}
                              data={filteredData}
                              type="monotone"
                              dataKey="value"
                              stroke={COLORS[idx % COLORS.length]}
                              strokeWidth={2}
                              dot={false}
                              name={label.length > 40 ? label.substring(0, 37) + '...' : label}
                            />
                          );
                        })}
                      </LineChart>
                    </ResponsiveContainer>
                  )}
                </Box>
              ) : (
                /* 2D Data Table - Rows = Time Periods, Columns = Series */
                <TableContainer sx={{ maxHeight: 500 }}>
                  {seriesDataQueries.some(q => q.isLoading) ? (
                    <Box display="flex" justifyContent="center" alignItems="center" p={4}>
                      <CircularProgress />
                    </Box>
                  ) : (() => {
                    // Build 2D data structure
                    const allPeriods = new Map<string, { year: number; period: string; period_name: string; values: Record<string, number | null> }>();

                    seriesDataQueries.forEach((queryResult, idx) => {
                      const chartData = queryResult?.data;
                      if (!chartData?.series[0]) return;
                      // Filter by time range (0 = all time)
                      const filteredData = seriesTimeRange === 0
                        ? chartData.series[0].data_points
                        : chartData.series[0].data_points.slice(-seriesTimeRange);
                      filteredData.forEach((dp: any) => {
                        const key = `${dp.year}-${dp.period}`;
                        if (!allPeriods.has(key)) {
                          allPeriods.set(key, {
                            year: dp.year,
                            period: dp.period,
                            period_name: dp.period_name,
                            values: {}
                          });
                        }
                        allPeriods.get(key)!.values[selectedSeriesIds[idx]] = dp.value;
                      });
                    });

                    // Sort by year and period descending (most recent first)
                    const sortedPeriods = Array.from(allPeriods.values()).sort((a, b) => {
                      if (b.year !== a.year) return b.year - a.year;
                      return b.period.localeCompare(a.period);
                    });

                    return (
                      <Table size="small" stickyHeader>
                        <TableHead>
                          <TableRow>
                            <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: 'background.paper', position: 'sticky', left: 0, zIndex: 3 }}>
                              Period
                            </TableCell>
                            {selectedSeriesIds.map((seriesId, idx) => {
                              const seriesInfo = seriesData?.series.find(s => s.series_id === seriesId);
                              return (
                                <TableCell
                                  key={seriesId}
                                  align="right"
                                  sx={{ fontWeight: 600, fontSize: '0.7rem', bgcolor: 'background.paper', minWidth: 120 }}
                                >
                                  {seriesInfo ? (
                                    <>
                                      <Box>{seriesInfo.industry_name?.substring(0, 25)}</Box>
                                      <Box sx={{ fontWeight: 400, color: 'text.secondary' }}>{seriesInfo.data_type_text?.substring(0, 20) || 'N/A'}</Box>
                                    </>
                                  ) : seriesId}
                                </TableCell>
                              );
                            })}
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {sortedPeriods.map(period => (
                            <TableRow key={`${period.year}-${period.period}`} hover>
                              <TableCell sx={{ fontSize: '0.75rem', fontWeight: 500, position: 'sticky', left: 0, bgcolor: 'background.paper' }}>
                                {period.period_name}
                              </TableCell>
                              {selectedSeriesIds.map(seriesId => (
                                <TableCell key={seriesId} align="right" sx={{ fontSize: '0.8rem' }}>
                                  {period.values[seriesId] != null ? period.values[seriesId]?.toLocaleString(undefined, { maximumFractionDigits: 1 }) : '-'}
                                </TableCell>
                              ))}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    );
                  })()}
                </TableContainer>
              )}
            </Card>
          )}
        </CardContent>
      </Card>
    </Box>
  );
}
