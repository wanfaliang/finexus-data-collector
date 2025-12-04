import React, { useState, useMemo } from 'react';
import { useQuery, useQueries } from '@tanstack/react-query';
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
  Paper,
  Divider,
  Autocomplete,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Chip,
  IconButton,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  TrendingFlat,
  Close as CloseIcon,
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
  Legend,
} from 'recharts';
import { beaExplorerAPI } from '../api/client';
import type { NIPASeries, NIPATimeSeries } from '../api/client';

// Key NIPA series for the Economic Snapshot
const HEADLINE_SERIES = [
  { tableCode: 'T10101', lineNumber: 1, name: 'Real GDP Growth', description: 'Percent change from preceding period', frequencies: ['Q', 'A'] },
  { tableCode: 'T10105', lineNumber: 1, name: 'GDP (Nominal)', description: 'Gross domestic product', frequencies: ['Q', 'A'] },
  { tableCode: 'T10101', lineNumber: 2, name: 'PCE Growth', description: 'Personal consumption expenditures', frequencies: ['Q', 'A'] },
  { tableCode: 'T10101', lineNumber: 7, name: 'Investment', description: 'Gross private domestic investment', frequencies: ['Q', 'A'] },
  { tableCode: 'T10101', lineNumber: 16, name: 'Exports', description: 'Exports of goods and services', frequencies: ['Q', 'A'] },
  { tableCode: 'T10101', lineNumber: 21, name: 'Government', description: 'Government consumption & investment', frequencies: ['Q', 'A'] },
];

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

// Calculate change between last two values
// For percent data (like growth rates), use simple difference (percentage points)
// For level data (like GDP in dollars), use percent change
const calculateChange = (
  data: NIPATimeSeries['data'],
  unit?: string | null
): { change: number | null; direction: 'up' | 'down' | 'flat'; isPercentagePoints: boolean } => {
  if (!data || data.length < 2) return { change: null, direction: 'flat', isPercentagePoints: false };

  const validData = data.filter(d => d.value !== null);
  if (validData.length < 2) return { change: null, direction: 'flat', isPercentagePoints: false };

  const latest = validData[validData.length - 1].value!;
  const previous = validData[validData.length - 2].value!;

  // For percent/index data, use simple difference (percentage points)
  const isPercentOrIndex = unit?.toLowerCase().includes('percent') || unit?.toLowerCase().includes('index');

  let change: number;
  if (isPercentOrIndex) {
    // Simple difference for percent data
    change = latest - previous;
  } else {
    // Percent change for level data
    if (previous === 0) return { change: null, direction: 'flat', isPercentagePoints: false };
    change = ((latest - previous) / Math.abs(previous)) * 100;
  }

  const threshold = isPercentOrIndex ? 0.1 : 0.5;
  const direction = change > threshold ? 'up' : change < -threshold ? 'down' : 'flat';

  return { change, direction, isPercentagePoints: !!isPercentOrIndex };
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

// Headline Card Component
function HeadlineCard({
  title,
  description,
  seriesData,
  isLoading,
  colorIndex = 0,
}: {
  title: string;
  description: string;
  seriesData?: NIPATimeSeries;
  isLoading: boolean;
  colorIndex?: number;
}) {
  const borderColor = CARD_COLORS[colorIndex % CARD_COLORS.length];

  const latestValue = useMemo(() => {
    if (!seriesData?.data?.length) return null;
    const validData = seriesData.data.filter(d => d.value !== null);
    return validData.length > 0 ? validData[validData.length - 1] : null;
  }, [seriesData]);

  const { change, direction, isPercentagePoints } = useMemo(() => {
    if (!seriesData?.data) return { change: null, direction: 'flat' as const, isPercentagePoints: false };
    return calculateChange(seriesData.data, seriesData?.unit);
  }, [seriesData]);

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;

  // Sparkline data
  const sparklineData = useMemo(() => {
    if (!seriesData?.data) return [];
    return seriesData.data
      .filter(d => d.value !== null)
      .slice(-12)
      .map(d => ({ period: d.time_period, value: d.value }));
  }, [seriesData]);

  const trendColor = direction === 'up' ? 'success.main' : direction === 'down' ? 'error.main' : 'text.secondary';

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
          {title}
        </Typography>

        {isLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 2 }}>
            <CircularProgress size={20} />
          </Box>
        ) : (
          <>
            <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 1, mt: 1 }}>
              <Typography variant="h5" fontWeight="bold" color="text.primary">
                {formatValue(latestValue?.value ?? null, seriesData?.unit, seriesData?.unit_mult)}
              </Typography>
              {latestValue && (
                <Typography variant="caption" color="text.secondary">
                  ({latestValue.time_period})
                </Typography>
              )}
            </Box>

            {change !== null && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
                <TrendIcon sx={{ fontSize: 16, color: trendColor }} />
                <Typography variant="body2" fontWeight="medium" sx={{ color: trendColor }}>
                  {change > 0 ? '+' : ''}{change.toFixed(1)}{isPercentagePoints ? ' pp' : '%'}
                </Typography>
              </Box>
            )}

            {sparklineData.length > 0 && (
              <Box sx={{ height: 40, mt: 1 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={sparklineData}>
                    <Line type="monotone" dataKey="value" stroke={borderColor} strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </Box>
            )}

            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
              {description}
            </Typography>
          </>
        )}
      </CardContent>
    </Card>
  );
}

// Period options for data display
const PERIOD_OPTIONS = [
  { label: '1Y', value: 1 },
  { label: '5Y', value: 5 },
  { label: '10Y', value: 10 },
  { label: '20Y', value: 20 },
  { label: 'All', value: 0 },
];

// Colors for multiple series
const SERIES_COLORS = [
  '#667eea', // Purple-blue
  '#f5576c', // Pink-red
  '#4facfe', // Blue
  '#43e97b', // Green
  '#fa709a', // Pink
  '#fee140', // Yellow
  '#a18cd1', // Lavender
  '#00f2fe', // Cyan
];

export default function NIPAExplorer() {
  const [selectedTable, setSelectedTable] = useState<string>('T10101');
  const [selectedSeriesList, setSelectedSeriesList] = useState<NIPASeries[]>([]);
  const [frequency, setFrequency] = useState<string>('Q');
  const [periodYears, setPeriodYears] = useState<number>(10); // Default to 10 years

  // Fetch tables
  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ['nipa-tables'],
    queryFn: () => beaExplorerAPI.getNIPATables(),
  });

  // Fetch series for selected table
  const { data: seriesList, isLoading: seriesLoading } = useQuery({
    queryKey: ['nipa-series', selectedTable],
    queryFn: () => beaExplorerAPI.getNIPATableSeries(selectedTable),
    enabled: !!selectedTable,
  });

  // Fetch headline series data
  const headlineQueries = HEADLINE_SERIES.map(hs => {
    // eslint-disable-next-line react-hooks/rules-of-hooks
    return useQuery({
      queryKey: ['nipa-headline', hs.tableCode, hs.lineNumber],
      queryFn: async () => {
        const series = await beaExplorerAPI.getNIPATableSeries(hs.tableCode);
        const targetSeries = series.find(s => s.line_number === hs.lineNumber);
        if (!targetSeries) return null;

        for (const freq of hs.frequencies) {
          try {
            const data = await beaExplorerAPI.getNIPASeriesData(targetSeries.series_code, { frequency: freq });
            if (data?.data?.length > 0) {
              return data;
            }
          } catch {
            // Try next frequency
          }
        }
        return null;
      },
      staleTime: 5 * 60 * 1000,
    });
  });

  // Auto-select first series when seriesList loads
  React.useEffect(() => {
    if (seriesList && seriesList.length > 0 && selectedSeriesList.length === 0) {
      setSelectedSeriesList([seriesList[0]]);
    }
  }, [seriesList, selectedSeriesList.length]);

  // Clear series selection when table changes
  React.useEffect(() => {
    setSelectedSeriesList([]);
  }, [selectedTable]);

  // Fetch data for all selected series
  const seriesQueries = useQueries({
    queries: selectedSeriesList.map(series => ({
      queryKey: ['nipa-series-data', series.series_code, frequency],
      queryFn: () => beaExplorerAPI.getNIPASeriesData(series.series_code, { frequency }),
      enabled: !!series,
      staleTime: 5 * 60 * 1000,
    })),
  });

  const dataLoading = seriesQueries.some(q => q.isLoading);
  const dataError = seriesQueries.find(q => q.error)?.error;

  // Combine series data with their metadata
  const allSeriesData = useMemo(() => {
    return selectedSeriesList.map((series, idx) => ({
      series,
      data: seriesQueries[idx]?.data,
      color: SERIES_COLORS[idx % SERIES_COLORS.length],
    }));
  }, [selectedSeriesList, seriesQueries]);

  // Helper to filter data by period years
  const filterByPeriod = (data: NIPATimeSeries['data']) => {
    if (periodYears === 0) return data; // All data

    const currentYear = new Date().getFullYear();
    const cutoffYear = currentYear - periodYears;

    return data.filter(d => {
      const yearMatch = d.time_period.match(/^(\d{4})/);
      if (!yearMatch) return true;
      return parseInt(yearMatch[1]) >= cutoffYear;
    });
  };

  // Combined chart data for all series
  const chartData = useMemo(() => {
    if (allSeriesData.length === 0) return [];

    // Get all unique periods across all series
    const allPeriods = new Set<string>();
    allSeriesData.forEach(({ data }) => {
      if (data?.data) {
        filterByPeriod(data.data).forEach(d => allPeriods.add(d.time_period));
      }
    });

    // Sort periods
    const sortedPeriods = Array.from(allPeriods).sort();

    // Build combined data
    return sortedPeriods.map(period => {
      const point: Record<string, any> = { period };
      allSeriesData.forEach(({ series, data }) => {
        const dataPoint = data?.data?.find(d => d.time_period === period);
        point[series.series_code] = dataPoint?.value ?? null;
      });
      return point;
    });
  }, [allSeriesData, periodYears]);

  // For the first selected series (for stats display)
  const primarySeriesData = allSeriesData[0]?.data;
  const primarySeries = selectedSeriesList[0];

  // Latest value and change for primary series
  const { latestValue, change, direction, isPercentagePoints } = useMemo(() => {
    if (!primarySeriesData?.data?.length) return { latestValue: null, change: null, direction: 'flat' as const, isPercentagePoints: false };
    const validData = primarySeriesData.data.filter(d => d.value !== null);
    const latest = validData.length > 0 ? validData[validData.length - 1] : null;
    const { change, direction, isPercentagePoints } = calculateChange(primarySeriesData.data, primarySeriesData?.unit);
    return { latestValue: latest, change, direction, isPercentagePoints };
  }, [primarySeriesData]);

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;
  const trendColor = direction === 'up' ? 'success.main' : direction === 'down' ? 'error.main' : 'text.secondary';

  // Add/remove series
  const handleAddSeries = (series: NIPASeries | null) => {
    if (!series) return;
    if (selectedSeriesList.find(s => s.series_code === series.series_code)) return;
    if (selectedSeriesList.length >= 8) return; // Max 8 series
    setSelectedSeriesList([...selectedSeriesList, series]);
  };

  const handleRemoveSeries = (seriesCode: string) => {
    setSelectedSeriesList(selectedSeriesList.filter(s => s.series_code !== seriesCode));
  };

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
          NIPA Explorer
        </Typography>
      </Box>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3, ml: 6 }}>
        National Income and Product Accounts - GDP, Personal Income, Government, and more
      </Typography>

      {/* Economic Snapshot - Headline Metrics */}
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
        Economic Snapshot
      </Typography>
      <Grid container spacing={2} sx={{ mb: 4 }}>
        {HEADLINE_SERIES.map((hs, idx) => (
          <Grid item xs={6} sm={4} md={2} key={`${hs.tableCode}-${hs.lineNumber}`}>
            <HeadlineCard
              title={hs.name}
              description={hs.description}
              seriesData={headlineQueries[idx].data ?? undefined}
              isLoading={headlineQueries[idx].isLoading}
              colorIndex={idx}
            />
          </Grid>
        ))}
      </Grid>

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

      {/* Selection Row - Full Width */}
      <Box sx={{ display: 'flex', gap: 2, mb: 3, flexWrap: 'wrap' }}>
        <Autocomplete
          options={tables || []}
          getOptionLabel={(t) => `${t.table_name} - ${t.table_description}`}
          value={tables?.find(t => t.table_name === selectedTable) || null}
          onChange={(_, newValue) => {
            setSelectedTable(newValue?.table_name || '');
            setSelectedSeries(null);
          }}
          loading={tablesLoading}
          sx={{ minWidth: 400, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField {...params} label="Select Table" size="small" />
          )}
        />

        <Autocomplete
          options={seriesList?.filter(s => !selectedSeriesList.find(sel => sel.series_code === s.series_code)) || []}
          getOptionLabel={(s) => `${s.line_number}. ${s.line_description}`}
          value={null}
          onChange={(_, newValue) => handleAddSeries(newValue)}
          loading={seriesLoading}
          disabled={!selectedTable || selectedSeriesList.length >= 8}
          sx={{ minWidth: 400, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField
              {...params}
              label={selectedSeriesList.length >= 8 ? "Max 8 series" : "Add Series"}
              size="small"
              placeholder="Click to add more series..."
            />
          )}
        />

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Frequency</InputLabel>
          <Select
            value={frequency}
            label="Frequency"
            onChange={(e) => setFrequency(e.target.value)}
          >
            <MenuItem value="A">Annual</MenuItem>
            <MenuItem value="Q">Quarterly</MenuItem>
            <MenuItem value="M">Monthly</MenuItem>
          </Select>
        </FormControl>

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

      {/* Selected Series Chips */}
      {selectedSeriesList.length > 0 && (
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 3 }}>
          {selectedSeriesList.map((series, idx) => (
            <Chip
              key={series.series_code}
              label={`${series.line_number}. ${series.line_description.substring(0, 30)}...`}
              onDelete={() => handleRemoveSeries(series.series_code)}
              sx={{
                bgcolor: SERIES_COLORS[idx % SERIES_COLORS.length],
                color: 'white',
                fontWeight: 500,
                '& .MuiChip-deleteIcon': {
                  color: 'rgba(255,255,255,0.7)',
                  '&:hover': {
                    color: 'white',
                  },
                },
              }}
            />
          ))}
        </Box>
      )}

      {/* Data Display - Full Width */}
      {selectedSeriesList.length === 0 ? (
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
      ) : dataError ? (
        <Alert severity="error">Error loading data</Alert>
      ) : chartData.length === 0 ? (
        <Alert severity="warning">No data for frequency "{frequency}". Try Annual or Quarterly.</Alert>
      ) : (
        <>
          {/* Series Info & Stats - Show for primary series */}
          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                <Box>
                  <Typography variant="h6" fontWeight="bold">
                    {selectedSeriesList.length === 1
                      ? primarySeries?.line_description
                      : `${selectedSeriesList.length} Series Selected`}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Table: {selectedTable}
                    {selectedSeriesList.length === 1 && ` | Series: ${primarySeries?.series_code}`}
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
                }}
              >
                <Box>
                  <Typography variant="caption" color="text.secondary" fontWeight="medium">
                    Latest Value {selectedSeriesList.length > 1 && '(Primary)'}
                  </Typography>
                  <Typography variant="h4" fontWeight="bold" color="primary.main">
                    {formatValue(latestValue?.value ?? null, primarySeriesData?.unit, primarySeriesData?.unit_mult)}
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
                        {change > 0 ? '+' : ''}{change.toFixed(1)}{isPercentagePoints ? ' pp' : '%'}
                      </Typography>
                    </Box>
                    <Typography variant="body2" color="text.secondary">vs prior period</Typography>
                  </Box>
                )}
                <Box>
                  <Typography variant="caption" color="text.secondary" fontWeight="medium">
                    Unit
                  </Typography>
                  <Typography variant="body1" fontWeight="medium">{primarySeriesData?.unit || 'N/A'}</Typography>
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

          {/* Chart - Full Width */}
          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 2, color: 'primary.main' }}>
                Time Series Chart
              </Typography>
              <Box sx={{ height: 450 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={chartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                    <defs>
                      {allSeriesData.map(({ series, color }, idx) => (
                        <linearGradient key={series.series_code} id={`color-${idx}`} x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor={color} stopOpacity={0.2}/>
                          <stop offset="95%" stopColor={color} stopOpacity={0.02}/>
                        </linearGradient>
                      ))}
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
                      tickFormatter={(v) => formatValue(v, primarySeriesData?.unit, primarySeriesData?.unit_mult)}
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
                      formatter={(value: number, name: string) => {
                        const seriesInfo = allSeriesData.find(s => s.series.series_code === name);
                        return [
                          formatValue(value, seriesInfo?.data?.unit, seriesInfo?.data?.unit_mult),
                          seriesInfo?.series.line_description?.substring(0, 35) || name
                        ];
                      }}
                    />
                    {/* Render area fill only for single series */}
                    {selectedSeriesList.length === 1 && (
                      <Area
                        type="monotone"
                        dataKey={selectedSeriesList[0].series_code}
                        stroke="transparent"
                        fill={`url(#color-0)`}
                      />
                    )}
                    {/* Render lines for each series */}
                    {allSeriesData.map(({ series, color }, idx) => (
                      <Line
                        key={series.series_code}
                        type="monotone"
                        dataKey={series.series_code}
                        name={series.line_description.substring(0, 30)}
                        stroke={color}
                        strokeWidth={2.5}
                        dot={false}
                        activeDot={{
                          r: 6,
                          fill: color,
                          stroke: '#fff',
                          strokeWidth: 3,
                        }}
                      />
                    ))}
                    {/* Show legend when multiple series */}
                    {selectedSeriesList.length > 1 && (
                      <Legend
                        verticalAlign="top"
                        height={36}
                        wrapperStyle={{ paddingBottom: 10 }}
                      />
                    )}
                  </ComposedChart>
                </ResponsiveContainer>
              </Box>
            </CardContent>
          </Card>

          {/* Data Table - Full Width */}
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
              <TableContainer sx={{ maxHeight: 500 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'bold', bgcolor: 'grey.100', position: 'sticky', left: 0, zIndex: 3 }}>
                        Period
                      </TableCell>
                      {allSeriesData.map(({ series, color }, idx) => (
                        <TableCell
                          key={series.series_code}
                          align="right"
                          sx={{
                            fontWeight: 'bold',
                            bgcolor: 'grey.100',
                            borderLeft: '1px solid',
                            borderColor: 'divider',
                          }}
                        >
                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 1 }}>
                            <Box sx={{ width: 12, height: 12, borderRadius: '50%', bgcolor: color }} />
                            <span style={{ maxWidth: 150, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                              {series.line_description.substring(0, 20)}...
                            </span>
                          </Box>
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {chartData.slice().reverse().map((row, idx) => (
                      <TableRow
                        key={row.period}
                        hover
                        sx={{ bgcolor: idx % 2 === 0 ? 'transparent' : 'grey.50' }}
                      >
                        <TableCell sx={{ fontWeight: 'medium', position: 'sticky', left: 0, bgcolor: idx % 2 === 0 ? 'white' : 'grey.50' }}>
                          {row.period}
                        </TableCell>
                        {allSeriesData.map(({ series, data }) => (
                          <TableCell
                            key={series.series_code}
                            align="right"
                            sx={{ fontFamily: 'monospace', borderLeft: '1px solid', borderColor: 'divider' }}
                          >
                            {formatValue(row[series.series_code], data?.unit, data?.unit_mult)}
                          </TableCell>
                        ))}
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
