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
  Divider,
  Autocomplete,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Chip,
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
  Legend,
  BarChart,
  Bar,
  Cell,
} from 'recharts';
import { beaExplorerAPI } from '../api/client';
import type { ITAIndicator, ITAArea, ITATimeSeries, ITAHeadlineMetric, ITASnapshot, ITAHeadlineResponse } from '../api/client';

// Key indicators for the headline snapshot
const HEADLINE_INDICATORS = [
  { code: 'BalGds', name: 'Goods Balance', description: 'Trade balance in goods' },
  { code: 'BalServ', name: 'Services Balance', description: 'Trade balance in services' },
  { code: 'BalGdsServ', name: 'Goods & Services', description: 'Combined trade balance' },
  { code: 'BalCurrAcct', name: 'Current Account', description: 'Current account balance' },
  { code: 'BalPrimInc', name: 'Primary Income', description: 'Primary income balance' },
  { code: 'BalSecInc', name: 'Secondary Income', description: 'Secondary income balance' },
];

// Format value for trade data (in millions/billions)
const formatValue = (value: number | null, unit?: string | null, unitMult?: number | null): string => {
  if (value === null || value === undefined) return 'N/A';

  const multiplier = unitMult ? Math.pow(10, unitMult) : 1;
  const actualValue = value * multiplier;

  const isNegative = actualValue < 0;
  const absValue = Math.abs(actualValue);

  let formatted: string;
  if (absValue >= 1e12) formatted = `$${(absValue / 1e12).toFixed(2)}T`;
  else if (absValue >= 1e9) formatted = `$${(absValue / 1e9).toFixed(2)}B`;
  else if (absValue >= 1e6) formatted = `$${(absValue / 1e6).toFixed(1)}M`;
  else if (absValue >= 1e3) formatted = `$${(absValue / 1e3).toFixed(1)}K`;
  else formatted = `$${absValue.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

  return isNegative ? `-${formatted}` : formatted;
};

// Calculate change between periods
const calculateChange = (
  data: ITATimeSeries['data'],
): { change: number | null; direction: 'up' | 'down' | 'flat' } => {
  if (!data || data.length < 2) return { change: null, direction: 'flat' };

  const validData = data.filter(d => d.value !== null);
  if (validData.length < 2) return { change: null, direction: 'flat' };

  const latest = validData[validData.length - 1].value!;
  const previous = validData[validData.length - 2].value!;

  if (previous === 0) return { change: null, direction: 'flat' };
  const change = ((latest - previous) / Math.abs(previous)) * 100;

  const direction = change > 0.5 ? 'up' : change < -0.5 ? 'down' : 'flat';
  return { change, direction };
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
  metric,
  isLoading,
  colorIndex = 0,
}: {
  title: string;
  description: string;
  metric?: ITAHeadlineMetric;
  isLoading: boolean;
  colorIndex?: number;
}) {
  const borderColor = CARD_COLORS[colorIndex % CARD_COLORS.length];

  // Determine if value is positive (surplus) or negative (deficit)
  const direction = useMemo(() => {
    if (!metric?.value) return 'flat';
    return metric.value > 0 ? 'up' : metric.value < 0 ? 'down' : 'flat';
  }, [metric]);

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;
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
                {formatValue(metric?.value ?? null, metric?.unit, metric?.unit_mult)}
              </Typography>
              {metric?.time_period && (
                <Typography variant="caption" color="text.secondary">
                  ({metric.time_period})
                </Typography>
              )}
            </Box>

            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
              <TrendIcon sx={{ fontSize: 16, color: trendColor }} />
              <Typography variant="body2" fontWeight="medium" sx={{ color: trendColor }}>
                {direction === 'up' ? 'Surplus' : direction === 'down' ? 'Deficit' : 'Balanced'}
              </Typography>
            </Box>

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

// Bar colors for positive/negative values
const getBarColor = (value: number) => {
  return value >= 0 ? '#43e97b' : '#f5576c';
};

export default function ITAExplorer() {
  const [selectedIndicator, setSelectedIndicator] = useState<ITAIndicator | null>(null);
  const [selectedAreaList, setSelectedAreaList] = useState<ITAArea[]>([]);
  const [frequency, setFrequency] = useState<string>('A');
  const [periodYears, setPeriodYears] = useState<number>(10);
  const [snapshotIndicator, setSnapshotIndicator] = useState<string>('BalGdsServ');
  const [snapshotViewMode, setSnapshotViewMode] = useState<'bar' | 'table'>('bar');

  // Fetch indicators
  const { data: indicators, isLoading: indicatorsLoading } = useQuery({
    queryKey: ['ita-indicators'],
    queryFn: () => beaExplorerAPI.getITAIndicators(),
  });

  // Fetch areas/countries
  const { data: areas, isLoading: areasLoading } = useQuery({
    queryKey: ['ita-areas'],
    queryFn: () => beaExplorerAPI.getITAAreas(),
  });

  // Fetch headline metrics
  const { data: headlineMetrics, isLoading: headlineLoading } = useQuery({
    queryKey: ['ita-headline', frequency],
    queryFn: () => beaExplorerAPI.getITAHeadline({ frequency }),
  });

  // Fetch snapshot data
  const { data: snapshotData, isLoading: snapshotLoading } = useQuery({
    queryKey: ['ita-snapshot', snapshotIndicator, frequency],
    queryFn: () => beaExplorerAPI.getITASnapshot({ indicator_code: snapshotIndicator, frequency }),
  });

  // Auto-select first indicator when loaded
  React.useEffect(() => {
    if (indicators && indicators.length > 0 && !selectedIndicator) {
      const balGdsServ = indicators.find(i => i.indicator_code === 'BalGdsServ');
      setSelectedIndicator(balGdsServ || indicators[0]);
    }
  }, [indicators, selectedIndicator]);

  // Auto-select "AllCountries" when areas load
  React.useEffect(() => {
    if (areas && areas.length > 0 && selectedAreaList.length === 0) {
      const allCountries = areas.find(a => a.area_code === 'AllCountries');
      if (allCountries) {
        setSelectedAreaList([allCountries]);
      } else if (areas.length > 0) {
        setSelectedAreaList([areas[0]]);
      }
    }
  }, [areas, selectedAreaList.length]);

  // Fetch data for all selected areas
  const areaQueries = useQueries({
    queries: selectedAreaList.map(area => ({
      queryKey: ['ita-data', selectedIndicator?.indicator_code, area.area_code, frequency],
      queryFn: () => beaExplorerAPI.getITAData({
        indicator_code: selectedIndicator?.indicator_code || '',
        area_code: area.area_code,
        frequency,
      }),
      enabled: !!selectedIndicator && !!area,
      staleTime: 5 * 60 * 1000,
    })),
  });

  const dataLoading = areaQueries.some(q => q.isLoading);
  const dataError = areaQueries.find(q => q.error)?.error;

  // Combine area data with metadata
  const allAreaData = useMemo(() => {
    return selectedAreaList.map((area, idx) => ({
      area,
      data: areaQueries[idx]?.data,
      color: SERIES_COLORS[idx % SERIES_COLORS.length],
    }));
  }, [selectedAreaList, areaQueries]);

  // Helper to filter data by period years
  const filterByPeriod = (data: ITATimeSeries['data']) => {
    if (periodYears === 0) return data;

    const currentYear = new Date().getFullYear();
    const cutoffYear = currentYear - periodYears;

    return data.filter(d => {
      const yearMatch = d.time_period.match(/^(\d{4})/);
      if (!yearMatch) return true;
      return parseInt(yearMatch[1]) >= cutoffYear;
    });
  };

  // Combined chart data for all areas
  const chartData = useMemo(() => {
    if (allAreaData.length === 0) return [];

    const allPeriods = new Set<string>();
    allAreaData.forEach(({ data }) => {
      if (data?.data) {
        filterByPeriod(data.data).forEach(d => allPeriods.add(d.time_period));
      }
    });

    const sortedPeriods = Array.from(allPeriods).sort();

    return sortedPeriods.map(period => {
      const point: Record<string, any> = { period };
      allAreaData.forEach(({ area, data }) => {
        const dataPoint = data?.data?.find(d => d.time_period === period);
        point[area.area_code] = dataPoint?.value ?? null;
      });
      return point;
    });
  }, [allAreaData, periodYears]);

  // Snapshot bar data (top trading partners)
  const snapshotBarData = useMemo(() => {
    if (!snapshotData?.data) return [];

    // Filter out aggregate codes and sort by absolute value
    return snapshotData.data
      .filter(d => d.value !== null && d.area_code !== 'AllCountries')
      .sort((a, b) => Math.abs(b.value!) - Math.abs(a.value!))
      .slice(0, 15)
      .map(d => ({
        name: d.area_name?.length > 20 ? d.area_name.substring(0, 20) + '...' : d.area_name,
        fullName: d.area_name,
        value: d.value,
        code: d.area_code,
      }));
  }, [snapshotData]);

  // For primary selected area (stats display)
  const primaryAreaData = allAreaData[0]?.data;
  const primaryArea = selectedAreaList[0];

  // Latest value and change for primary area
  const { latestValue, change, direction } = useMemo(() => {
    if (!primaryAreaData?.data?.length) return { latestValue: null, change: null, direction: 'flat' as const };
    const validData = primaryAreaData.data.filter(d => d.value !== null);
    const latest = validData.length > 0 ? validData[validData.length - 1] : null;
    const { change, direction } = calculateChange(primaryAreaData.data);
    return { latestValue: latest, change, direction };
  }, [primaryAreaData]);

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;
  const trendColor = direction === 'up' ? 'success.main' : direction === 'down' ? 'error.main' : 'text.secondary';

  // Get headline metric by indicator code
  const getHeadlineMetric = (code: string): ITAHeadlineMetric | undefined => {
    return headlineMetrics?.data?.find(m => m.indicator_code === code);
  };

  // Add/remove area
  const handleAddArea = (area: ITAArea | null) => {
    if (!area) return;
    if (selectedAreaList.find(a => a.area_code === area.area_code)) return;
    if (selectedAreaList.length >= 8) return;
    setSelectedAreaList([...selectedAreaList, area]);
  };

  const handleRemoveArea = (areaCode: string) => {
    setSelectedAreaList(selectedAreaList.filter(a => a.area_code !== areaCode));
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
          International Trade Explorer
        </Typography>
      </Box>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3, ml: 6 }}>
        U.S. International Transactions - Trade in Goods, Services, and Income
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
        Trade Balance Overview
      </Typography>
      <Grid container spacing={2} sx={{ mb: 4 }}>
        {HEADLINE_INDICATORS.map((ind, idx) => (
          <Grid item xs={6} sm={4} md={2} key={ind.code}>
            <HeadlineCard
              title={ind.name}
              description={ind.description}
              metric={getHeadlineMetric(ind.code)}
              isLoading={headlineLoading}
              colorIndex={idx}
            />
          </Grid>
        ))}
      </Grid>

      {/* Trading Partners Snapshot */}
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
            Top Trading Partners ({snapshotData?.period || 'Latest'})
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
            <FormControl size="small" sx={{ minWidth: 180 }}>
              <InputLabel>Indicator</InputLabel>
              <Select
                value={snapshotIndicator}
                label="Indicator"
                onChange={(e) => setSnapshotIndicator(e.target.value)}
              >
                {HEADLINE_INDICATORS.map(ind => (
                  <MenuItem key={ind.code} value={ind.code}>{ind.name}</MenuItem>
                ))}
              </Select>
            </FormControl>
            <Tabs
              value={snapshotViewMode}
              onChange={(_, v) => setSnapshotViewMode(v)}
              sx={{ minHeight: 36 }}
            >
              <Tab label="Chart" value="bar" sx={{ minHeight: 36, py: 0 }} />
              <Tab label="Table" value="table" sx={{ minHeight: 36, py: 0 }} />
            </Tabs>
          </Box>
        </Box>

        <Card sx={{ border: '1px solid', borderColor: 'divider' }}>
          <CardContent>
            {snapshotLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
                <CircularProgress />
              </Box>
            ) : snapshotBarData.length === 0 ? (
              <Alert severity="info">No snapshot data available</Alert>
            ) : snapshotViewMode === 'bar' ? (
              <Box sx={{ height: 450 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={snapshotBarData}
                    layout="vertical"
                    margin={{ top: 10, right: 30, left: 120, bottom: 10 }}
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
                      width={110}
                    />
                    <Tooltip
                      formatter={(value: number) => [formatValue(value, snapshotData?.unit, snapshotData?.unit_mult), snapshotData?.indicator_description || 'Value']}
                      labelFormatter={(label, payload) => payload[0]?.payload?.fullName || label}
                    />
                    <Bar dataKey="value" name="Balance">
                      {snapshotBarData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={getBarColor(entry.value || 0)} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
                <Box sx={{ display: 'flex', justifyContent: 'center', gap: 4, mt: 2 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 16, height: 16, bgcolor: '#43e97b', borderRadius: 1 }} />
                    <Typography variant="caption">Surplus</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 16, height: 16, bgcolor: '#f5576c', borderRadius: 1 }} />
                    <Typography variant="caption">Deficit</Typography>
                  </Box>
                </Box>
              </Box>
            ) : (
              <TableContainer sx={{ maxHeight: 450 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Country/Region</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Balance</TableCell>
                      <TableCell align="center" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {snapshotBarData.map((row, idx) => (
                      <TableRow key={row.code} hover sx={{ bgcolor: idx % 2 === 0 ? 'transparent' : 'grey.50' }}>
                        <TableCell>{row.fullName}</TableCell>
                        <TableCell align="right" sx={{ fontFamily: 'monospace', fontWeight: 'medium' }}>
                          {formatValue(row.value, snapshotData?.unit, snapshotData?.unit_mult)}
                        </TableCell>
                        <TableCell align="center">
                          <Chip
                            label={row.value && row.value >= 0 ? 'Surplus' : 'Deficit'}
                            size="small"
                            sx={{
                              bgcolor: row.value && row.value >= 0 ? 'success.light' : 'error.light',
                              color: row.value && row.value >= 0 ? 'success.dark' : 'error.dark',
                              fontWeight: 500,
                            }}
                          />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
            {snapshotData?.indicator_description && (
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2 }}>
                Indicator: {snapshotData.indicator_description}
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
          options={indicators || []}
          getOptionLabel={(i) => `${i.indicator_code} - ${i.indicator_description}`}
          value={selectedIndicator}
          onChange={(_, newValue) => setSelectedIndicator(newValue)}
          loading={indicatorsLoading}
          sx={{ minWidth: 400, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField {...params} label="Select Indicator" size="small" />
          )}
        />

        <Autocomplete
          options={areas?.filter(a => !selectedAreaList.find(sel => sel.area_code === a.area_code)) || []}
          getOptionLabel={(a) => `${a.area_code} - ${a.area_name}`}
          value={null}
          onChange={(_, newValue) => handleAddArea(newValue)}
          loading={areasLoading}
          disabled={selectedAreaList.length >= 8}
          sx={{ minWidth: 300, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField
              {...params}
              label={selectedAreaList.length >= 8 ? "Max 8 countries" : "Add Country/Region"}
              size="small"
              placeholder="Click to add more..."
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
            <MenuItem value="QSA">Quarterly (SA)</MenuItem>
            <MenuItem value="QNSA">Quarterly (NSA)</MenuItem>
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

      {/* Selected Areas Chips */}
      {selectedAreaList.length > 0 && (
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 3 }}>
          {selectedAreaList.map((area, idx) => (
            <Chip
              key={area.area_code}
              label={area.area_name?.substring(0, 30) || area.area_code}
              title={area.area_name}
              onDelete={() => handleRemoveArea(area.area_code)}
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

      {/* Data Display */}
      {!selectedIndicator || selectedAreaList.length === 0 ? (
        <Card>
          <CardContent sx={{ py: 6, textAlign: 'center' }}>
            <Typography color="text.secondary">Select an indicator and country/region to view data</Typography>
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
        <Alert severity="warning">No data available for this selection. Try a different indicator or frequency.</Alert>
      ) : (
        <>
          {/* Info & Stats */}
          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                <Box>
                  <Typography variant="h6" fontWeight="bold">
                    {selectedIndicator?.indicator_description}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    {selectedAreaList.length === 1
                      ? primaryArea?.area_name
                      : `${selectedAreaList.length} Countries/Regions Selected`}
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
                    Latest Value {selectedAreaList.length > 1 && '(Primary)'}
                  </Typography>
                  <Typography variant="h4" fontWeight="bold" color="primary.main">
                    {formatValue(latestValue?.value ?? null, primaryAreaData?.unit, primaryAreaData?.unit_mult)}
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
                    <Typography variant="body2" color="text.secondary">vs prior period</Typography>
                  </Box>
                )}
                <Box>
                  <Typography variant="caption" color="text.secondary" fontWeight="medium">
                    Frequency
                  </Typography>
                  <Typography variant="body1" fontWeight="medium">
                    {frequency === 'A' ? 'Annual' : frequency === 'QSA' ? 'Quarterly (SA)' : 'Quarterly (NSA)'}
                  </Typography>
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
              <Box sx={{ height: 450 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={chartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                    <defs>
                      {allAreaData.map(({ area, color }, idx) => (
                        <linearGradient key={area.area_code} id={`color-area-${idx}`} x1="0" y1="0" x2="0" y2="1">
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
                      tickFormatter={(v) => formatValue(v, primaryAreaData?.unit, primaryAreaData?.unit_mult)}
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
                        const areaInfo = allAreaData.find(a => a.area.area_code === name);
                        return [
                          formatValue(value, areaInfo?.data?.unit, areaInfo?.data?.unit_mult),
                          areaInfo?.area.area_name?.substring(0, 30) || name
                        ];
                      }}
                    />
                    {/* Render area fill only for single area */}
                    {selectedAreaList.length === 1 && (
                      <Area
                        type="monotone"
                        dataKey={selectedAreaList[0].area_code}
                        stroke="transparent"
                        fill={`url(#color-area-0)`}
                      />
                    )}
                    {/* Render lines for each area */}
                    {allAreaData.map(({ area, color }) => (
                      <Line
                        key={area.area_code}
                        type="monotone"
                        dataKey={area.area_code}
                        name={area.area_name?.substring(0, 25) || area.area_code}
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
                    {/* Show legend when multiple areas */}
                    {selectedAreaList.length > 1 && (
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
              <TableContainer sx={{ maxHeight: 500 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'bold', bgcolor: 'grey.100', position: 'sticky', left: 0, zIndex: 3 }}>
                        Period
                      </TableCell>
                      {allAreaData.map(({ area, color }) => (
                        <TableCell
                          key={area.area_code}
                          align="right"
                          sx={{
                            fontWeight: 'bold',
                            bgcolor: 'grey.100',
                            borderLeft: '1px solid',
                            borderColor: 'divider',
                          }}
                        >
                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 1 }}>
                            <Box sx={{ width: 12, height: 12, borderRadius: '50%', bgcolor: color, flexShrink: 0 }} />
                            <span style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={area.area_name}>
                              {area.area_name?.substring(0, 20) || area.area_code}
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
                        {allAreaData.map(({ area, data }) => (
                          <TableCell
                            key={area.area_code}
                            align="right"
                            sx={{ fontFamily: 'monospace', borderLeft: '1px solid', borderColor: 'divider' }}
                          >
                            {formatValue(row[area.area_code], data?.unit, data?.unit_mult)}
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
