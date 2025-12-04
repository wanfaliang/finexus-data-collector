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
  Treemap,
} from 'recharts';
import { beaExplorerAPI } from '../api/client';
import type { GDPByIndustryIndustry, GDPByIndustryTimeSeries, GDPByIndustryTable } from '../api/client';

// Key industries for the snapshot (using actual database codes)
const HEADLINE_INDUSTRIES = [
  { code: 'GDP', name: 'GDP Total', description: 'Gross Domestic Product' },
  { code: 'FIRE', name: 'Finance/Insurance/RE', description: 'Finance, insurance, real estate' },
  { code: 'PROF', name: 'Professional Services', description: 'Professional and business services' },
  { code: '31G', name: 'Manufacturing', description: 'Durable and non-durable goods' },
  { code: 'G', name: 'Government', description: 'Federal, state, and local' },
  { code: '62', name: 'Healthcare/Social', description: 'Healthcare and social assistance' },
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
const calculateChange = (
  data: GDPByIndustryTimeSeries['data'],
  unit?: string | null
): { change: number | null; direction: 'up' | 'down' | 'flat'; isPercentagePoints: boolean } => {
  if (!data || data.length < 2) return { change: null, direction: 'flat', isPercentagePoints: false };

  const validData = data.filter(d => d.value !== null);
  if (validData.length < 2) return { change: null, direction: 'flat', isPercentagePoints: false };

  const latest = validData[validData.length - 1].value!;
  const previous = validData[validData.length - 2].value!;

  const isPercentOrIndex = unit?.toLowerCase().includes('percent') || unit?.toLowerCase().includes('index');

  let change: number;
  if (isPercentOrIndex) {
    change = latest - previous;
  } else {
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
  seriesData?: GDPByIndustryTimeSeries;
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
  const trendColor = direction === 'up' ? 'success.main' : direction === 'down' ? 'error.main' : 'text.secondary';

  // Sparkline data
  const sparklineData = useMemo(() => {
    if (!seriesData?.data) return [];
    return seriesData.data
      .filter(d => d.value !== null)
      .slice(-12)
      .map(d => ({ period: d.time_period, value: d.value }));
  }, [seriesData]);

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

// Treemap colors for industries
const TREEMAP_COLORS = [
  '#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe', '#00f2fe',
  '#43e97b', '#38f9d7', '#fa709a', '#fee140', '#a18cd1', '#fbc2eb',
  '#5ee7df', '#b490ca', '#667db6', '#0082c8', '#00d4a1', '#ff6b6b',
];

// Custom treemap content - shows industry name and value
const createTreemapContent = (unit: string | null, unitMult: number | null) => {
  return function TreemapContent(props: any) {
    const { x, y, width, height, name, value, index } = props;

    // Skip very small cells
    if (width < 40 || height < 30) return null;

    const bgColor = TREEMAP_COLORS[index % TREEMAP_COLORS.length];
    const canShowText = width > 60 && height > 40;
    const canShowValue = width > 80 && height > 55;

    // Calculate max characters based on width
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

export default function GDPbyIndustryExplorer() {
  const [selectedTable, setSelectedTable] = useState<number>(1);
  const [selectedIndustryList, setSelectedIndustryList] = useState<GDPByIndustryIndustry[]>([]);
  const [frequency, setFrequency] = useState<string>('A');
  const [periodYears, setPeriodYears] = useState<number>(10);
  const [snapshotViewMode, setSnapshotViewMode] = useState<'treemap' | 'bar'>('treemap');

  // Fetch tables
  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ['gdpbyindustry-tables'],
    queryFn: () => beaExplorerAPI.getGDPByIndustryTables(),
  });

  // Fetch industries
  const { data: industries, isLoading: industriesLoading } = useQuery({
    queryKey: ['gdpbyindustry-industries'],
    queryFn: () => beaExplorerAPI.getGDPByIndustryIndustries(),
  });

  // Fetch snapshot data for treemap
  const { data: snapshotData, isLoading: snapshotLoading } = useQuery({
    queryKey: ['gdpbyindustry-snapshot', selectedTable, frequency],
    queryFn: () => beaExplorerAPI.getGDPByIndustrySnapshot({ table_id: selectedTable, frequency }),
  });

  // Fetch headline series data
  const headlineQueries = useQueries({
    queries: HEADLINE_INDUSTRIES.map(ind => ({
      queryKey: ['gdpbyindustry-headline', ind.code],
      queryFn: async () => {
        try {
          const data = await beaExplorerAPI.getGDPByIndustryData({
            table_id: 1, // Value Added by Industry
            industry_code: ind.code,
            frequency: 'A',
          });
          return data;
        } catch {
          return null;
        }
      },
      staleTime: 5 * 60 * 1000,
    })),
  });

  // Auto-select first industry when industries load
  React.useEffect(() => {
    if (industries && industries.length > 0 && selectedIndustryList.length === 0) {
      const allIndustry = industries.find(i => i.industry_code === 'ALL');
      if (allIndustry) {
        setSelectedIndustryList([allIndustry]);
      } else {
        setSelectedIndustryList([industries[0]]);
      }
    }
  }, [industries, selectedIndustryList.length]);

  // Fetch data for all selected industries
  const industryQueries = useQueries({
    queries: selectedIndustryList.map(industry => ({
      queryKey: ['gdpbyindustry-data', selectedTable, industry.industry_code, frequency],
      queryFn: () => beaExplorerAPI.getGDPByIndustryData({
        table_id: selectedTable,
        industry_code: industry.industry_code,
        frequency,
      }),
      enabled: !!industry,
      staleTime: 5 * 60 * 1000,
    })),
  });

  const dataLoading = industryQueries.some(q => q.isLoading);
  const dataError = industryQueries.find(q => q.error)?.error;

  // Combine industry data with their metadata
  const allIndustryData = useMemo(() => {
    return selectedIndustryList.map((industry, idx) => ({
      industry,
      data: industryQueries[idx]?.data,
      color: SERIES_COLORS[idx % SERIES_COLORS.length],
    }));
  }, [selectedIndustryList, industryQueries]);

  // Helper to filter data by period years
  const filterByPeriod = (data: GDPByIndustryTimeSeries['data']) => {
    if (periodYears === 0) return data;

    const currentYear = new Date().getFullYear();
    const cutoffYear = currentYear - periodYears;

    return data.filter(d => {
      const yearMatch = d.time_period.match(/^(\d{4})/);
      if (!yearMatch) return true;
      return parseInt(yearMatch[1]) >= cutoffYear;
    });
  };

  // Combined chart data for all industries
  const chartData = useMemo(() => {
    if (allIndustryData.length === 0) return [];

    const allPeriods = new Set<string>();
    allIndustryData.forEach(({ data }) => {
      if (data?.data) {
        filterByPeriod(data.data).forEach(d => allPeriods.add(d.time_period));
      }
    });

    const sortedPeriods = Array.from(allPeriods).sort();

    return sortedPeriods.map(period => {
      const point: Record<string, any> = { period };
      allIndustryData.forEach(({ industry, data }) => {
        const dataPoint = data?.data?.find(d => d.time_period === period);
        point[industry.industry_code] = dataPoint?.value ?? null;
      });
      return point;
    });
  }, [allIndustryData, periodYears]);

  // Treemap data for snapshot
  const treemapData = useMemo(() => {
    if (!snapshotData?.data) return [];

    // Filter to show only major sectors (level 1-2) and exclude 'ALL'
    const filtered = snapshotData.data
      .filter(d => d.industry_code !== 'ALL' && d.value > 0)
      .slice(0, 20); // Top 20 industries

    return filtered.map((d, idx) => ({
      name: d.industry_description.length > 25
        ? d.industry_description.substring(0, 25) + '...'
        : d.industry_description,
      value: d.value,
      fullName: d.industry_description,
      code: d.industry_code,
    }));
  }, [snapshotData]);

  // For the primary selected industry (for stats display)
  const primaryIndustryData = allIndustryData[0]?.data;
  const primaryIndustry = selectedIndustryList[0];

  // Latest value and change for primary industry
  const { latestValue, change, direction, isPercentagePoints } = useMemo(() => {
    if (!primaryIndustryData?.data?.length) return { latestValue: null, change: null, direction: 'flat' as const, isPercentagePoints: false };
    const validData = primaryIndustryData.data.filter(d => d.value !== null);
    const latest = validData.length > 0 ? validData[validData.length - 1] : null;
    const { change, direction, isPercentagePoints } = calculateChange(primaryIndustryData.data, primaryIndustryData?.unit);
    return { latestValue: latest, change, direction, isPercentagePoints };
  }, [primaryIndustryData]);

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;
  const trendColor = direction === 'up' ? 'success.main' : direction === 'down' ? 'error.main' : 'text.secondary';

  // Add/remove industry
  const handleAddIndustry = (industry: GDPByIndustryIndustry | null) => {
    if (!industry) return;
    if (selectedIndustryList.find(i => i.industry_code === industry.industry_code)) return;
    if (selectedIndustryList.length >= 8) return;
    setSelectedIndustryList([...selectedIndustryList, industry]);
  };

  const handleRemoveIndustry = (industryCode: string) => {
    setSelectedIndustryList(selectedIndustryList.filter(i => i.industry_code !== industryCode));
  };

  const selectedTableInfo = tables?.find(t => t.table_id === selectedTable);

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
          GDP by Industry Explorer
        </Typography>
      </Box>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3, ml: 6 }}>
        Value Added, Gross Output, and Components by Industry Sector
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
        Industry Snapshot
      </Typography>
      <Grid container spacing={2} sx={{ mb: 4 }}>
        {HEADLINE_INDUSTRIES.map((ind, idx) => (
          <Grid item xs={6} sm={4} md={2} key={ind.code}>
            <HeadlineCard
              title={ind.name}
              description={ind.description}
              seriesData={headlineQueries[idx].data ?? undefined}
              isLoading={headlineQueries[idx].isLoading}
              colorIndex={idx}
            />
          </Grid>
        ))}
      </Grid>

      {/* Industry Overview - Treemap */}
      <Box sx={{ mb: 4 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
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
            Industry Overview ({snapshotData?.period || 'Latest'})
          </Typography>
          <Tabs
            value={snapshotViewMode}
            onChange={(_, v) => setSnapshotViewMode(v)}
            sx={{ minHeight: 36 }}
          >
            <Tab label="Treemap" value="treemap" sx={{ minHeight: 36, py: 0 }} />
            <Tab label="Bar Chart" value="bar" sx={{ minHeight: 36, py: 0 }} />
          </Tabs>
        </Box>

        <Card sx={{ border: '1px solid', borderColor: 'divider' }}>
          <CardContent>
            {snapshotLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
                <CircularProgress />
              </Box>
            ) : treemapData.length === 0 ? (
              <Alert severity="info">No snapshot data available</Alert>
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
                    data={treemapData.slice(0, 15)}
                    layout="vertical"
                    margin={{ top: 10, right: 30, left: 150, bottom: 10 }}
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
                      width={140}
                    />
                    <Tooltip
                      formatter={(value: number) => [formatValue(value, snapshotData?.unit, snapshotData?.unit_mult), 'Value']}
                    />
                    <defs>
                      <linearGradient id="barGradient" x1="0" y1="0" x2="1" y2="0">
                        <stop offset="0%" stopColor="#667eea" />
                        <stop offset="100%" stopColor="#764ba2" />
                      </linearGradient>
                    </defs>
                    <Area
                      type="monotone"
                      dataKey="value"
                      fill="url(#barGradient)"
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
          getOptionLabel={(t) => `${t.table_id}. ${t.table_description}`}
          value={tables?.find(t => t.table_id === selectedTable) || null}
          onChange={(_, newValue) => {
            setSelectedTable(newValue?.table_id || 1);
          }}
          loading={tablesLoading}
          sx={{ minWidth: 400, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField {...params} label="Select Table" size="small" />
          )}
        />

        <Autocomplete
          options={industries?.filter(i => !selectedIndustryList.find(sel => sel.industry_code === i.industry_code)) || []}
          getOptionLabel={(i) => `${i.industry_code} - ${i.industry_description}`}
          value={null}
          onChange={(_, newValue) => handleAddIndustry(newValue)}
          loading={industriesLoading}
          disabled={selectedIndustryList.length >= 8}
          sx={{ minWidth: 350, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField
              {...params}
              label={selectedIndustryList.length >= 8 ? "Max 8 industries" : "Add Industry"}
              size="small"
              placeholder="Click to add more industries..."
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

      {/* Selected Industries Chips */}
      {selectedIndustryList.length > 0 && (
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 3 }}>
          {selectedIndustryList.map((industry, idx) => (
            <Chip
              key={industry.industry_code}
              label={industry.industry_description?.substring(0, 35) || industry.industry_code}
              title={industry.industry_description}
              onDelete={() => handleRemoveIndustry(industry.industry_code)}
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
      {selectedIndustryList.length === 0 ? (
        <Card>
          <CardContent sx={{ py: 6, textAlign: 'center' }}>
            <Typography color="text.secondary">Select a table and industry to view data</Typography>
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
        <Alert severity="warning">No data for frequency "{frequency}". Try Annual.</Alert>
      ) : (
        <>
          {/* Industry Info & Stats */}
          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                <Box>
                  <Typography variant="h6" fontWeight="bold">
                    {selectedIndustryList.length === 1
                      ? primaryIndustry?.industry_description
                      : `${selectedIndustryList.length} Industries Selected`}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Table: {selectedTableInfo?.table_description || selectedTable}
                    {selectedIndustryList.length === 1 && ` | Industry: ${primaryIndustry?.industry_code}`}
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
                    Latest Value {selectedIndustryList.length > 1 && '(Primary)'}
                  </Typography>
                  <Typography variant="h4" fontWeight="bold" color="primary.main">
                    {formatValue(latestValue?.value ?? null, primaryIndustryData?.unit, primaryIndustryData?.unit_mult)}
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
                  <Typography variant="body1" fontWeight="medium">{primaryIndustryData?.unit || 'N/A'}</Typography>
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
                      {allIndustryData.map(({ industry, color }, idx) => (
                        <linearGradient key={industry.industry_code} id={`color-industry-${idx}`} x1="0" y1="0" x2="0" y2="1">
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
                      tickFormatter={(v) => formatValue(v, primaryIndustryData?.unit, primaryIndustryData?.unit_mult)}
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
                        const industryInfo = allIndustryData.find(i => i.industry.industry_code === name);
                        return [
                          formatValue(value, industryInfo?.data?.unit, industryInfo?.data?.unit_mult),
                          industryInfo?.industry.industry_description?.substring(0, 35) || name
                        ];
                      }}
                    />
                    {/* Render area fill only for single industry */}
                    {selectedIndustryList.length === 1 && (
                      <Area
                        type="monotone"
                        dataKey={selectedIndustryList[0].industry_code}
                        stroke="transparent"
                        fill={`url(#color-industry-0)`}
                      />
                    )}
                    {/* Render lines for each industry */}
                    {allIndustryData.map(({ industry, color }, idx) => (
                      <Line
                        key={industry.industry_code}
                        type="monotone"
                        dataKey={industry.industry_code}
                        name={industry.industry_description.substring(0, 30)}
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
                    {/* Show legend when multiple industries */}
                    {selectedIndustryList.length > 1 && (
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
                      {allIndustryData.map(({ industry, color }, idx) => (
                        <TableCell
                          key={industry.industry_code}
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
                            <span style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={industry.industry_description}>
                              {industry.industry_description?.substring(0, 25) || industry.industry_code}
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
                        {allIndustryData.map(({ industry, data }) => (
                          <TableCell
                            key={industry.industry_code}
                            align="right"
                            sx={{ fontFamily: 'monospace', borderLeft: '1px solid', borderColor: 'divider' }}
                          >
                            {formatValue(row[industry.industry_code], data?.unit, data?.unit_mult)}
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
