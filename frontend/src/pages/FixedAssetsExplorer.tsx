import React, { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  Grid,
  Typography,
  CircularProgress,
  Alert,
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
  Divider,
  Autocomplete,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Tabs,
  Tab,
  IconButton,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  TrendingFlat,
  ArrowBack,
} from '@mui/icons-material';
import { Link } from 'react-router-dom';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Area,
  ComposedChart,
  Treemap,
} from 'recharts';
import { beaExplorerAPI } from '../api/client';
import type { FixedAssetsTable, FixedAssetsSeries, FixedAssetsTimeSeries, FixedAssetsHeadlineMetric } from '../api/client';

// Format value based on unit and multiplier
const formatValue = (value: number | null, unit?: string | null, unitMult?: number | null): string => {
  if (value === null || value === undefined) return 'N/A';

  if (unit?.toLowerCase().includes('percent')) {
    return `${value.toFixed(1)}%`;
  }

  if (unit?.toLowerCase().includes('index')) {
    return value.toFixed(1);
  }

  const multiplier = unitMult ? Math.pow(10, unitMult) : 1;
  const actualValue = value * multiplier;

  if (Math.abs(actualValue) >= 1e12) return `$${(actualValue / 1e12).toFixed(2)}T`;
  if (Math.abs(actualValue) >= 1e9) return `$${(actualValue / 1e9).toFixed(2)}B`;
  if (Math.abs(actualValue) >= 1e6) return `$${(actualValue / 1e6).toFixed(1)}M`;
  if (Math.abs(actualValue) >= 1e3) return `$${(actualValue / 1e3).toFixed(1)}K`;

  return `$${actualValue.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
};

// Border colors for headline cards
const CARD_COLORS = [
  '#667eea', // Purple-blue
  '#f5576c', // Pink-red
  '#4facfe', // Blue
  '#43e97b', // Green
  '#fa709a', // Pink
  '#a18cd1', // Lavender
];

// Treemap colors
const TREEMAP_COLORS = [
  '#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe', '#00f2fe',
  '#43e97b', '#38f9d7', '#fa709a', '#fee140', '#a18cd1', '#fbc2eb',
];

// Headline Card Component
function HeadlineCard({
  metric,
  isLoading,
  colorIndex = 0,
}: {
  metric?: FixedAssetsHeadlineMetric;
  isLoading: boolean;
  colorIndex?: number;
}) {
  const borderColor = CARD_COLORS[colorIndex % CARD_COLORS.length];

  return (
    <Card
      sx={{
        height: '100%',
        borderTop: `3px solid ${borderColor}`,
        boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
        transition: 'transform 0.2s ease, box-shadow 0.2s ease',
        '&:hover': {
          transform: 'translateY(-2px)',
          boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
        },
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Typography variant="body2" color="text.secondary" fontWeight={500} noWrap>
          {metric?.name || 'Loading...'}
        </Typography>

        {isLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 2 }}>
            <CircularProgress size={20} />
          </Box>
        ) : (
          <>
            <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 1, mt: 1 }}>
              <Typography variant="h5" fontWeight="bold" color="text.primary">
                {formatValue(metric?.value ?? null, metric?.unit, metric?.unit_mult)}
              </Typography>
              {metric?.time_period && (
                <Typography variant="caption" color="text.secondary">
                  ({metric.time_period})
                </Typography>
              )}
            </Box>

            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
              {metric?.description}
            </Typography>
          </>
        )}
      </CardContent>
    </Card>
  );
}

// Period options for data display
const PERIOD_OPTIONS = [
  { label: '10Y', value: 10 },
  { label: '20Y', value: 20 },
  { label: '30Y', value: 30 },
  { label: 'All', value: 0 },
];

// Custom treemap content
const createTreemapContent = (unit: string | null, unitMult: number | null) => {
  return function TreemapContent(props: any) {
    const { x, y, width, height, name, value, index } = props;

    if (width < 40 || height < 30) return null;

    const bgColor = TREEMAP_COLORS[index % TREEMAP_COLORS.length];
    const canShowText = width > 60 && height > 40;
    const canShowValue = width > 80 && height > 55;
    const maxChars = Math.floor(width / 8);

    return (
      <g>
        <rect
          x={x}
          y={y}
          width={width}
          height={height}
          style={{
            fill: bgColor,
            stroke: '#fff',
            strokeWidth: 2,
            strokeOpacity: 1,
          }}
        />
        {canShowText && (
          <>
            <text
              x={x + width / 2}
              y={y + height / 2 - (canShowValue ? 8 : 0)}
              textAnchor="middle"
              fill="#fff"
              fontSize={Math.min(13, Math.max(10, width / 12))}
              fontWeight="500"
            >
              {name?.length > maxChars ? name.substring(0, maxChars) + '...' : name}
            </text>
            {canShowValue && (
              <text
                x={x + width / 2}
                y={y + height / 2 + 12}
                textAnchor="middle"
                fill="rgba(255,255,255,0.9)"
                fontSize={Math.min(11, Math.max(9, width / 14))}
              >
                {formatValue(value, unit, unitMult)}
              </text>
            )}
          </>
        )}
      </g>
    );
  };
};

export default function FixedAssetsExplorer() {
  const [selectedTable, setSelectedTable] = useState<string>('FAAt101');
  const [selectedSeries, setSelectedSeries] = useState<FixedAssetsSeries | null>(null);
  const [periodYears, setPeriodYears] = useState<number>(20);
  const [snapshotViewMode, setSnapshotViewMode] = useState<'treemap' | 'bar'>('treemap');

  // Fetch tables
  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ['fixedassets-tables'],
    queryFn: () => beaExplorerAPI.getFixedAssetsTables(),
  });

  // Fetch headline metrics
  const { data: headlineData, isLoading: headlineLoading } = useQuery({
    queryKey: ['fixedassets-headline'],
    queryFn: () => beaExplorerAPI.getFixedAssetsHeadline(),
  });

  // Fetch series for selected table
  const { data: seriesList, isLoading: seriesLoading } = useQuery({
    queryKey: ['fixedassets-series', selectedTable],
    queryFn: () => beaExplorerAPI.getFixedAssetsTableSeries(selectedTable),
    enabled: !!selectedTable,
  });

  // Fetch snapshot data
  const { data: snapshotData, isLoading: snapshotLoading } = useQuery({
    queryKey: ['fixedassets-snapshot', selectedTable],
    queryFn: () => beaExplorerAPI.getFixedAssetsSnapshot({ table_name: selectedTable }),
    enabled: !!selectedTable,
  });

  // Fetch time series data for selected series
  const { data: seriesData, isLoading: dataLoading } = useQuery({
    queryKey: ['fixedassets-data', selectedSeries?.series_code],
    queryFn: () => beaExplorerAPI.getFixedAssetsSeriesData(selectedSeries!.series_code),
    enabled: !!selectedSeries,
  });

  // Auto-select first series when series list loads
  React.useEffect(() => {
    if (seriesList && seriesList.length > 0 && !selectedSeries) {
      setSelectedSeries(seriesList[0]);
    }
  }, [seriesList, selectedSeries]);

  // Reset selected series when table changes
  React.useEffect(() => {
    setSelectedSeries(null);
  }, [selectedTable]);

  // Filter data by period years
  const filterByPeriod = (data: FixedAssetsTimeSeries['data']) => {
    if (periodYears === 0) return data;

    const currentYear = new Date().getFullYear();
    const cutoffYear = currentYear - periodYears;

    return data.filter(d => {
      const year = parseInt(d.time_period);
      return year >= cutoffYear;
    });
  };

  // Chart data
  const chartData = useMemo(() => {
    if (!seriesData?.data) return [];
    return filterByPeriod(seriesData.data).map(d => ({
      period: d.time_period,
      value: d.value,
    }));
  }, [seriesData, periodYears]);

  // Treemap data from snapshot
  const treemapData = useMemo(() => {
    if (!snapshotData?.data) return [];

    return snapshotData.data
      .filter(d => d.value > 0)
      .slice(0, 15)
      .map(d => ({
        name: d.line_description.length > 30
          ? d.line_description.substring(0, 30) + '...'
          : d.line_description,
        value: d.value,
        fullName: d.line_description,
        lineNumber: d.line_number,
      }));
  }, [snapshotData]);

  // Latest value and change
  const { latestValue, change, direction } = useMemo(() => {
    if (!seriesData?.data?.length) return { latestValue: null, change: null, direction: 'flat' as const };

    const validData = seriesData.data.filter(d => d.value !== null);
    if (validData.length < 2) return { latestValue: validData[0] || null, change: null, direction: 'flat' as const };

    const latest = validData[validData.length - 1];
    const previous = validData[validData.length - 2];

    if (!previous.value || previous.value === 0) return { latestValue: latest, change: null, direction: 'flat' as const };

    const pctChange = ((latest.value! - previous.value) / Math.abs(previous.value)) * 100;
    const dir = pctChange > 0.5 ? 'up' : pctChange < -0.5 ? 'down' : 'flat';

    return { latestValue: latest, change: pctChange, direction: dir };
  }, [seriesData]);

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;
  const trendColor = direction === 'up' ? 'success.main' : direction === 'down' ? 'error.main' : 'text.secondary';

  const selectedTableInfo = tables?.find(t => t.table_name === selectedTable);

  return (
    <Box sx={{ p: 3 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
        <IconButton
          component={Link}
          to="/bea"
          size="small"
          sx={{
            bgcolor: 'grey.100',
            '&:hover': { bgcolor: 'grey.200' },
          }}
        >
          <ArrowBack />
        </IconButton>
        <Typography variant="h4" fontWeight="bold">
          Fixed Assets Explorer
        </Typography>
      </Box>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3, ml: 6 }}>
        Current-Cost Net Stock of Fixed Assets and Consumer Durable Goods
      </Typography>

      {/* Headline Metrics */}
      <Typography
        variant="h6"
        fontWeight="600"
        sx={{
          mb: 2,
          color: 'primary.main',
          borderBottom: '2px solid',
          borderColor: 'primary.main',
          pb: 1,
          display: 'inline-block',
        }}
      >
        Asset Overview
      </Typography>
      <Grid container spacing={2} sx={{ mb: 4 }}>
        {headlineLoading ? (
          Array.from({ length: 6 }).map((_, idx) => (
            <Grid item xs={6} sm={4} md={2} key={idx}>
              <HeadlineCard isLoading={true} colorIndex={idx} />
            </Grid>
          ))
        ) : (
          headlineData?.data?.map((metric, idx) => (
            <Grid item xs={6} sm={4} md={2} key={metric.series_code}>
              <HeadlineCard metric={metric} isLoading={false} colorIndex={idx} />
            </Grid>
          ))
        )}
      </Grid>

      {/* Asset Composition - Treemap/Bar */}
      <Box sx={{ mb: 4 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2, flexWrap: 'wrap', gap: 2 }}>
          <Typography
            variant="h6"
            fontWeight="600"
            sx={{
              color: 'primary.main',
              borderBottom: '2px solid',
              borderColor: 'primary.main',
              pb: 1,
            }}
          >
            Asset Composition ({snapshotData?.period || 'Latest'})
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Table</InputLabel>
              <Select
                value={selectedTable}
                label="Table"
                onChange={(e) => setSelectedTable(e.target.value)}
              >
                {tables?.map(t => (
                  <MenuItem key={t.table_name} value={t.table_name}>
                    {t.table_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Tabs
              value={snapshotViewMode}
              onChange={(_, v) => setSnapshotViewMode(v)}
              sx={{ minHeight: 36 }}
            >
              <Tab label="Treemap" value="treemap" sx={{ minHeight: 36, py: 0 }} />
              <Tab label="Bar Chart" value="bar" sx={{ minHeight: 36, py: 0 }} />
            </Tabs>
          </Box>
        </Box>

        <Card sx={{ border: '1px solid', borderColor: 'divider' }}>
          <CardContent>
            {snapshotLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
                <CircularProgress />
              </Box>
            ) : treemapData.length === 0 ? (
              <Alert severity="info">No snapshot data available for this table</Alert>
            ) : snapshotViewMode === 'treemap' ? (
              <Box sx={{ height: 400 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <Treemap
                    data={treemapData}
                    dataKey="value"
                    aspectRatio={4 / 3}
                    stroke="#fff"
                    content={createTreemapContent(snapshotData?.unit || null, snapshotData?.unit_mult || null)}
                  />
                </ResponsiveContainer>
              </Box>
            ) : (
              <Box sx={{ height: 400 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart
                    data={treemapData.slice(0, 12)}
                    layout="vertical"
                    margin={{ top: 10, right: 30, left: 180, bottom: 10 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} />
                    <XAxis
                      type="number"
                      tick={{ fontSize: 11 }}
                      tickFormatter={(v) => formatValue(v, snapshotData?.unit, snapshotData?.unit_mult)}
                    />
                    <YAxis
                      type="category"
                      dataKey="name"
                      tick={{ fontSize: 11 }}
                      width={170}
                    />
                    <Tooltip
                      formatter={(value: number) => [formatValue(value, snapshotData?.unit, snapshotData?.unit_mult), 'Value']}
                      labelFormatter={(label, payload) => payload[0]?.payload?.fullName || label}
                    />
                    <defs>
                      <linearGradient id="faBarGradient" x1="0" y1="0" x2="1" y2="0">
                        <stop offset="0%" stopColor="#667eea" />
                        <stop offset="100%" stopColor="#764ba2" />
                      </linearGradient>
                    </defs>
                    <Area
                      type="monotone"
                      dataKey="value"
                      fill="url(#faBarGradient)"
                      stroke="#667eea"
                    />
                  </ComposedChart>
                </ResponsiveContainer>
              </Box>
            )}
            {snapshotData?.table_description && (
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2 }}>
                Source: {snapshotData.table_description}
              </Typography>
            )}
          </CardContent>
        </Card>
      </Box>

      <Divider sx={{ mb: 3 }} />

      {/* Data Explorer Section */}
      <Typography
        variant="h6"
        fontWeight="600"
        sx={{
          mb: 2,
          color: 'primary.main',
          borderBottom: '2px solid',
          borderColor: 'primary.main',
          pb: 1,
          display: 'inline-block',
        }}
      >
        Data Explorer
      </Typography>

      {/* Selection Row */}
      <Box sx={{ display: 'flex', gap: 2, mb: 3, flexWrap: 'wrap' }}>
        <Autocomplete
          options={tables || []}
          getOptionLabel={(t) => `${t.table_name} - ${t.table_description}`}
          value={tables?.find(t => t.table_name === selectedTable) || null}
          onChange={(_, newValue) => {
            if (newValue) setSelectedTable(newValue.table_name);
          }}
          loading={tablesLoading}
          sx={{ minWidth: 400, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField {...params} label="Select Table" size="small" />
          )}
        />

        <Autocomplete
          options={seriesList || []}
          getOptionLabel={(s) => `Line ${s.line_number}: ${s.line_description}`}
          value={selectedSeries}
          onChange={(_, newValue) => setSelectedSeries(newValue)}
          loading={seriesLoading}
          sx={{ minWidth: 400, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField {...params} label="Select Series" size="small" />
          )}
        />

        <ToggleButtonGroup
          value={periodYears}
          exclusive
          onChange={(_, value) => value !== null && setPeriodYears(value)}
          size="small"
          sx={{
            '& .MuiToggleButton-root': {
              px: 2,
              '&.Mui-selected': {
                bgcolor: 'primary.main',
                color: 'white',
                '&:hover': {
                  bgcolor: 'primary.dark',
                },
              },
            },
          }}
        >
          {PERIOD_OPTIONS.map(opt => (
            <ToggleButton key={opt.value} value={opt.value}>
              {opt.label}
            </ToggleButton>
          ))}
        </ToggleButtonGroup>
      </Box>

      {/* Data Display */}
      {!selectedSeries ? (
        <Card>
          <CardContent sx={{ py: 6, textAlign: 'center' }}>
            <Typography color="text.secondary">Select a table and series to view data</Typography>
          </CardContent>
        </Card>
      ) : dataLoading ? (
        <Card>
          <CardContent sx={{ py: 6, textAlign: 'center' }}>
            <CircularProgress />
          </CardContent>
        </Card>
      ) : chartData.length === 0 ? (
        <Alert severity="warning">No data available for this series</Alert>
      ) : (
        <>
          {/* Series Info & Stats */}
          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                <Box>
                  <Typography variant="h6" fontWeight="bold">
                    {selectedSeries?.line_description}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Table: {selectedTableInfo?.table_description || selectedTable}
                  </Typography>
                </Box>
              </Box>

              <Box
                sx={{
                  display: 'flex',
                  gap: 6,
                  p: 3,
                  background: 'linear-gradient(135deg, #f5f7fa 0%, #e4e8ec 100%)',
                  borderRadius: 2,
                  flexWrap: 'wrap',
                }}
              >
                <Box>
                  <Typography variant="caption" color="text.secondary" fontWeight="medium">
                    Latest Value
                  </Typography>
                  <Typography variant="h4" fontWeight="bold" color="primary.main">
                    {formatValue(latestValue?.value ?? null, seriesData?.unit, seriesData?.unit_mult)}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">{latestValue?.time_period}</Typography>
                </Box>
                {change !== null && (
                  <Box>
                    <Typography variant="caption" color="text.secondary" fontWeight="medium">
                      Change
                    </Typography>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      <TrendIcon sx={{ color: trendColor, fontSize: 28 }} />
                      <Typography variant="h5" fontWeight="bold" sx={{ color: trendColor }}>
                        {change > 0 ? '+' : ''}{change.toFixed(1)}%
                      </Typography>
                    </Box>
                    <Typography variant="body2" color="text.secondary">vs prior year</Typography>
                  </Box>
                )}
                <Box>
                  <Typography variant="caption" color="text.secondary" fontWeight="medium">
                    Metric
                  </Typography>
                  <Typography variant="body1" fontWeight="medium">{seriesData?.metric_name || 'N/A'}</Typography>
                </Box>
                <Box>
                  <Typography variant="caption" color="text.secondary" fontWeight="medium">
                    Data Points
                  </Typography>
                  <Typography variant="body1" fontWeight="medium">{chartData.length}</Typography>
                </Box>
              </Box>
            </CardContent>
          </Card>

          {/* Chart */}
          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 2, color: 'primary.main' }}>
                Time Series Chart
              </Typography>
              <Box sx={{ height: 400 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={chartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                    <defs>
                      <linearGradient id="colorFA" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#667eea" stopOpacity={0.2}/>
                        <stop offset="95%" stopColor="#667eea" stopOpacity={0.02}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e8e8e8" vertical={false} />
                    <XAxis
                      dataKey="period"
                      tick={{ fontSize: 11, fill: '#666' }}
                      tickLine={{ stroke: '#e0e0e0' }}
                      axisLine={{ stroke: '#e0e0e0' }}
                      interval="preserveStartEnd"
                    />
                    <YAxis
                      tick={{ fontSize: 11, fill: '#666' }}
                      tickLine={{ stroke: '#e0e0e0' }}
                      axisLine={{ stroke: '#e0e0e0' }}
                      tickFormatter={(v) => formatValue(v, seriesData?.unit, seriesData?.unit_mult)}
                      width={90}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'rgba(255,255,255,0.96)',
                        border: 'none',
                        borderRadius: 12,
                        boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
                        padding: '12px 16px',
                      }}
                      labelStyle={{ fontWeight: 'bold', marginBottom: 8 }}
                      formatter={(value: number) => [
                        formatValue(value, seriesData?.unit, seriesData?.unit_mult),
                        selectedSeries?.line_description?.substring(0, 30) || 'Value'
                      ]}
                    />
                    <Area
                      type="monotone"
                      dataKey="value"
                      stroke="transparent"
                      fill="url(#colorFA)"
                    />
                    <Line
                      type="monotone"
                      dataKey="value"
                      stroke="#667eea"
                      strokeWidth={2.5}
                      dot={false}
                      activeDot={{
                        r: 6,
                        fill: '#667eea',
                        stroke: '#fff',
                        strokeWidth: 3,
                      }}
                    />
                  </ComposedChart>
                </ResponsiveContainer>
              </Box>
            </CardContent>
          </Card>

          {/* Data Table */}
          <Card sx={{ border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="subtitle1" fontWeight="bold" color="primary.main">
                  Data Table
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {chartData.length} records
                </Typography>
              </Box>
              <TableContainer sx={{ maxHeight: 400 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Year</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Value</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {chartData.slice().reverse().map((row, idx) => (
                      <TableRow
                        key={row.period}
                        hover
                        sx={{ bgcolor: idx % 2 === 0 ? 'transparent' : 'grey.50' }}
                      >
                        <TableCell sx={{ fontWeight: 'medium' }}>{row.period}</TableCell>
                        <TableCell align="right" sx={{ fontFamily: 'monospace' }}>
                          {formatValue(row.value, seriesData?.unit, seriesData?.unit_mult)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </>
      )}
    </Box>
  );
}
