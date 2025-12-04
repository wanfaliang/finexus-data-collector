import React, { useState, useMemo, useEffect } from 'react';
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
  Tabs,
  Tab,
  IconButton,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  TrendingFlat,
  Map as MapIcon,
  ViewModule as TreemapIcon,
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
import ReactECharts from 'echarts-for-react';
import * as echarts from 'echarts';
import * as topojson from 'topojson-client';
import { beaExplorerAPI } from '../api/client';
import type { RegionalTable, RegionalLineCode, RegionalGeo, RegionalTimeSeries } from '../api/client';

// US States GeoJSON for ECharts (simplified inline version with state FIPS codes)
// Maps state FIPS codes to state names for ECharts
const STATE_FIPS_TO_NAME: Record<string, string> = {
  '01000': 'Alabama', '02000': 'Alaska', '04000': 'Arizona', '05000': 'Arkansas',
  '06000': 'California', '08000': 'Colorado', '09000': 'Connecticut', '10000': 'Delaware',
  '11000': 'District of Columbia', '12000': 'Florida', '13000': 'Georgia', '15000': 'Hawaii',
  '16000': 'Idaho', '17000': 'Illinois', '18000': 'Indiana', '19000': 'Iowa',
  '20000': 'Kansas', '21000': 'Kentucky', '22000': 'Louisiana', '23000': 'Maine',
  '24000': 'Maryland', '25000': 'Massachusetts', '26000': 'Michigan', '27000': 'Minnesota',
  '28000': 'Mississippi', '29000': 'Missouri', '30000': 'Montana', '31000': 'Nebraska',
  '32000': 'Nevada', '33000': 'New Hampshire', '34000': 'New Jersey', '35000': 'New Mexico',
  '36000': 'New York', '37000': 'North Carolina', '38000': 'North Dakota', '39000': 'Ohio',
  '40000': 'Oklahoma', '41000': 'Oregon', '42000': 'Pennsylvania', '44000': 'Rhode Island',
  '45000': 'South Carolina', '46000': 'South Dakota', '47000': 'Tennessee', '48000': 'Texas',
  '49000': 'Utah', '50000': 'Vermont', '51000': 'Virginia', '53000': 'Washington',
  '54000': 'West Virginia', '55000': 'Wisconsin', '56000': 'Wyoming',
};

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

  return actualValue.toLocaleString(undefined, { maximumFractionDigits: 0 });
};

// Calculate change between last two values
const calculateChange = (
  data: RegionalTimeSeries['data'],
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

// Treemap color scale - gradient from light to dark based on value
const getTreemapColor = (value: number, maxValue: number, minValue: number) => {
  const ratio = maxValue > minValue ? (value - minValue) / (maxValue - minValue) : 0.5;
  // Color gradient from light blue to deep purple
  const colors = [
    { r: 79, g: 172, b: 254 },   // Light blue
    { r: 102, g: 126, b: 234 },  // Purple-blue
    { r: 118, g: 75, b: 162 },   // Purple
  ];

  let color;
  if (ratio < 0.5) {
    const t = ratio * 2;
    color = {
      r: Math.round(colors[0].r + (colors[1].r - colors[0].r) * t),
      g: Math.round(colors[0].g + (colors[1].g - colors[0].g) * t),
      b: Math.round(colors[0].b + (colors[1].b - colors[0].b) * t),
    };
  } else {
    const t = (ratio - 0.5) * 2;
    color = {
      r: Math.round(colors[1].r + (colors[2].r - colors[1].r) * t),
      g: Math.round(colors[1].g + (colors[2].g - colors[1].g) * t),
      b: Math.round(colors[1].b + (colors[2].b - colors[1].b) * t),
    };
  }
  return `rgb(${color.r}, ${color.g}, ${color.b})`;
};

// Custom Treemap content renderer - factory function to capture unit/unitMult
const createTreemapContent = (unit: string | null, unitMult: number | null, maxValue: number, minValue: number) => {
  return (props: any) => {
    const { x, y, width, height, name, value } = props;

    if (width < 30 || height < 20) return null;

    const color = getTreemapColor(value, maxValue, minValue);
    const formattedValue = formatValue(value, unit, unitMult);
    const showValue = width > 60 && height > 35;
    const showName = width > 40 && height > 25;

    // Extract state abbreviation or short name
    const shortName = name.length > 12 ? name.substring(0, 10) + '...' : name;

    return (
      <g>
        <rect
          x={x}
          y={y}
          width={width}
          height={height}
          style={{
            fill: color,
            stroke: '#fff',
            strokeWidth: 2,
            strokeOpacity: 0.8,
          }}
        />
        {showName && (
          <text
            x={x + width / 2}
            y={y + height / 2 - (showValue ? 6 : 0)}
            textAnchor="middle"
            dominantBaseline="middle"
            style={{
              fontSize: Math.min(12, width / 8),
              fill: '#fff',
              fontWeight: 500,
              textShadow: '0 1px 2px rgba(0,0,0,0.3)',
            }}
          >
            {shortName}
          </text>
        )}
        {showValue && (
          <text
            x={x + width / 2}
            y={y + height / 2 + 10}
            textAnchor="middle"
            dominantBaseline="middle"
            style={{
              fontSize: Math.min(10, width / 10),
              fill: 'rgba(255,255,255,0.9)',
              fontWeight: 500,
            }}
          >
            {formattedValue}
          </text>
        )}
      </g>
    );
  };
};

export default function RegionalExplorer() {
  const [selectedTable, setSelectedTable] = useState<string>('SAGDP1');
  const [selectedLineCode, setSelectedLineCode] = useState<RegionalLineCode | null>(null);
  const [selectedGeos, setSelectedGeos] = useState<RegionalGeo[]>([]);
  const [geoSearch, setGeoSearch] = useState<string>('');
  const [geoType, setGeoType] = useState<string>('State');
  const [periodYears, setPeriodYears] = useState<number>(10);
  const [snapshotViewMode, setSnapshotViewMode] = useState<'map' | 'treemap'>('map');
  const [usaMapLoaded, setUsaMapLoaded] = useState(false);

  // Load USA GeoJSON map data
  useEffect(() => {
    if (!usaMapLoaded) {
      fetch('https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json')
        .then(res => res.json())
        .then((geoJson) => {
          echarts.registerMap('USA', geoJson as any);
          setUsaMapLoaded(true);
        })
        .catch(err => {
          console.error('Failed to load USA map:', err);
        });
    }
  }, [usaMapLoaded]);

  // Fetch tables
  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ['regional-tables'],
    queryFn: () => beaExplorerAPI.getRegionalTables(),
  });

  // Fetch line codes for selected table
  const { data: lineCodes, isLoading: lineCodesLoading } = useQuery({
    queryKey: ['regional-linecodes', selectedTable],
    queryFn: () => beaExplorerAPI.getRegionalTableLineCodes(selectedTable),
    enabled: !!selectedTable,
  });

  // Fetch geographies
  const { data: geoOptions, isLoading: geosLoading } = useQuery({
    queryKey: ['regional-geos', geoType, geoSearch],
    queryFn: () => beaExplorerAPI.getRegionalGeographies({
      geo_type: geoType,
      search: geoSearch || undefined,
      limit: 100,
    }),
  });

  // Fetch snapshot data for treemap (all states GDP)
  const { data: snapshotData, isLoading: snapshotLoading } = useQuery({
    queryKey: ['regional-snapshot'],
    queryFn: () => beaExplorerAPI.getRegionalSnapshot({
      table_name: 'SAGDP1',
      line_code: 1, // Real GDP
      geo_type: 'State',
    }),
    staleTime: 5 * 60 * 1000,
  });

  // Prepare treemap data
  const { treemapData, treemapContentRenderer } = useMemo(() => {
    if (!snapshotData?.data?.length) return { treemapData: [], treemapContentRenderer: null };
    const maxValue = Math.max(...snapshotData.data.map(d => d.value));
    const minValue = Math.min(...snapshotData.data.map(d => d.value));
    const data = snapshotData.data.map(d => ({
      name: d.geo_name,
      value: d.value,
      geo_fips: d.geo_fips,
    }));
    const renderer = createTreemapContent(snapshotData.unit, snapshotData.unit_mult, maxValue, minValue);
    return { treemapData: data, treemapContentRenderer: renderer };
  }, [snapshotData]);

  // Prepare ECharts map options
  const mapChartOption = useMemo(() => {
    if (!snapshotData?.data?.length || !usaMapLoaded) return null;

    // Convert snapshot data to ECharts format (by state name)
    // Use STATE_FIPS_TO_NAME for mapping, or fallback to geo_name from API
    const mapData = snapshotData.data
      .filter(d => STATE_FIPS_TO_NAME[d.geo_fips]) // Only include US states (exclude territories)
      .map(d => ({
        name: STATE_FIPS_TO_NAME[d.geo_fips] || d.geo_name,
        value: d.value,
        geo_fips: d.geo_fips,
      }));

    if (mapData.length === 0) return null;

    const values = mapData.map(d => d.value);
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    const unit = snapshotData.unit;
    const unitMult = snapshotData.unit_mult;

    return {
      backgroundColor: '#6b7a8a', // Ocean/background color
      tooltip: {
        trigger: 'item',
        formatter: (params: any) => {
          if (!params.data?.value) return `${params.name}: No data`;
          const formatted = formatValue(params.data.value, unit, unitMult);
          return `<strong>${params.name}</strong><br/>Real GDP: ${formatted}`;
        },
      },
      visualMap: {
        min: minVal,
        max: maxVal,
        left: 'left',
        top: 'bottom',
        text: ['High', 'Low'],
        textStyle: {
          color: '#fff',
        },
        calculable: true,
        inRange: {
          color: ['#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#313695'],
        },
        formatter: (value: number) => {
          return formatValue(value, unit, unitMult);
        },
      },
      series: [
        {
          name: 'State GDP',
          type: 'map',
          map: 'USA',
          roam: true,
          emphasis: {
            label: {
              show: true,
              fontSize: 12,
              fontWeight: 'bold',
            },
            itemStyle: {
              areaColor: '#ffd700',
              shadowBlur: 20,
              shadowColor: 'rgba(0, 0, 0, 0.3)',
            },
          },
          select: {
            label: {
              show: true,
            },
            itemStyle: {
              areaColor: '#ff6b6b',
            },
          },
          data: mapData,
          label: {
            show: false,
          },
          itemStyle: {
            borderColor: '#fff',
            borderWidth: 1,
          },
        },
      ],
    };
  }, [snapshotData, usaMapLoaded]);

  // Auto-select first line code when table changes
  React.useEffect(() => {
    if (lineCodes && lineCodes.length > 0 && !selectedLineCode) {
      setSelectedLineCode(lineCodes[0]);
    }
  }, [lineCodes, selectedLineCode]);

  // Clear line code selection when table changes
  React.useEffect(() => {
    setSelectedLineCode(null);
  }, [selectedTable]);

  // Auto-select a default geography when line code is selected and no geos selected
  React.useEffect(() => {
    if (selectedLineCode && selectedGeos.length === 0 && geoOptions && geoOptions.length > 0) {
      // Find California or first state
      const california = geoOptions.find(g => g.geo_fips === '06000');
      if (california) {
        setSelectedGeos([california]);
      } else {
        setSelectedGeos([geoOptions[0]]);
      }
    }
  }, [selectedLineCode, geoOptions, selectedGeos.length]);

  // Fetch data for all selected geographies
  const geoQueries = useQueries({
    queries: selectedGeos.map(geo => ({
      queryKey: ['regional-data', selectedTable, selectedLineCode?.line_code, geo.geo_fips],
      queryFn: () => beaExplorerAPI.getRegionalData({
        table_name: selectedTable,
        line_code: selectedLineCode!.line_code,
        geo_fips: geo.geo_fips,
      }),
      enabled: !!selectedLineCode && !!geo,
      staleTime: 5 * 60 * 1000,
    })),
  });

  const dataLoading = geoQueries.some(q => q.isLoading);
  const dataError = geoQueries.find(q => q.error)?.error;

  // Combine geo data with their metadata
  const allGeoData = useMemo(() => {
    return selectedGeos.map((geo, idx) => ({
      geo,
      data: geoQueries[idx]?.data,
      color: SERIES_COLORS[idx % SERIES_COLORS.length],
    }));
  }, [selectedGeos, geoQueries]);

  // Filter data by period years
  const filterByPeriod = (data: RegionalTimeSeries['data']) => {
    if (periodYears === 0) return data;
    const currentYear = new Date().getFullYear();
    const cutoffYear = currentYear - periodYears;
    return data.filter(d => {
      const yearMatch = d.time_period.match(/^(\d{4})/);
      if (!yearMatch) return true;
      return parseInt(yearMatch[1]) >= cutoffYear;
    });
  };

  // Combined chart data for all geographies
  const chartData = useMemo(() => {
    if (allGeoData.length === 0) return [];

    const allPeriods = new Set<string>();
    allGeoData.forEach(({ data }) => {
      if (data?.data) {
        filterByPeriod(data.data).forEach(d => allPeriods.add(d.time_period));
      }
    });

    const sortedPeriods = Array.from(allPeriods).sort();

    return sortedPeriods.map(period => {
      const point: Record<string, any> = { period };
      allGeoData.forEach(({ geo, data }) => {
        const dataPoint = data?.data?.find(d => d.time_period === period);
        point[geo.geo_fips] = dataPoint?.value ?? null;
      });
      return point;
    });
  }, [allGeoData, periodYears]);

  // For the first selected geo (for stats display)
  const primaryGeoData = allGeoData[0]?.data;
  const primaryGeo = selectedGeos[0];

  // Latest value and change for primary geo
  const { latestValue, change, direction, isPercentagePoints } = useMemo(() => {
    if (!primaryGeoData?.data?.length) return { latestValue: null, change: null, direction: 'flat' as const, isPercentagePoints: false };
    const validData = primaryGeoData.data.filter(d => d.value !== null);
    const latest = validData.length > 0 ? validData[validData.length - 1] : null;
    const { change, direction, isPercentagePoints } = calculateChange(primaryGeoData.data, primaryGeoData?.unit);
    return { latestValue: latest, change, direction, isPercentagePoints };
  }, [primaryGeoData]);

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;
  const trendColor = direction === 'up' ? 'success.main' : direction === 'down' ? 'error.main' : 'text.secondary';

  // Add/remove geography
  const handleAddGeo = (geo: RegionalGeo | null) => {
    if (!geo) return;
    if (selectedGeos.find(g => g.geo_fips === geo.geo_fips)) return;
    if (selectedGeos.length >= 8) return;
    setSelectedGeos([...selectedGeos, geo]);
  };

  const handleRemoveGeo = (fips: string) => {
    setSelectedGeos(selectedGeos.filter(g => g.geo_fips !== fips));
  };

  // Table categories
  const tableCategories = useMemo(() => {
    if (!tables) return {};
    const categories: Record<string, typeof tables> = {
      'State GDP (SAGDP)': [],
      'County GDP (CAGDP)': [],
      'State Income (SAINC)': [],
      'County Income (CAINC)': [],
      'Other': [],
    };

    tables.forEach(t => {
      if (t.table_name.startsWith('SAGDP')) {
        categories['State GDP (SAGDP)'].push(t);
      } else if (t.table_name.startsWith('CAGDP')) {
        categories['County GDP (CAGDP)'].push(t);
      } else if (t.table_name.startsWith('SAINC')) {
        categories['State Income (SAINC)'].push(t);
      } else if (t.table_name.startsWith('CAINC')) {
        categories['County Income (CAINC)'].push(t);
      } else {
        categories['Other'].push(t);
      }
    });

    return categories;
  }, [tables]);

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
          Regional Explorer
        </Typography>
      </Box>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3, ml: 6 }}>
        State and County GDP, Personal Income, and Economic Profiles
      </Typography>

      {/* Economic Snapshot - State GDP Map/Treemap */}
      <Card
        sx={{
          mb: 4,
          background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
          boxShadow: '0 4px 20px rgba(0,0,0,0.08)',
        }}
      >
        <CardContent>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Box>
              <Typography
                variant="h6"
                fontWeight="600"
                sx={{
                  color: 'primary.main',
                  borderBottom: '2px solid',
                  borderColor: 'primary.main',
                  pb: 0.5,
                  display: 'inline-block',
                }}
              >
                State GDP Overview
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Real GDP by State {snapshotData?.year ? `(${snapshotData.year})` : ''} - {snapshotViewMode === 'map' ? 'Geographic distribution' : 'Size represents GDP value'}
              </Typography>
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              {snapshotData && (
                <Chip
                  label={`${snapshotData.data?.length || 0} States`}
                  size="small"
                  sx={{ background: 'rgba(102, 126, 234, 0.1)', color: '#667eea', fontWeight: 500 }}
                />
              )}
              <Tabs
                value={snapshotViewMode}
                onChange={(_, newValue) => setSnapshotViewMode(newValue)}
                sx={{
                  minHeight: 36,
                  '& .MuiTab-root': {
                    minHeight: 36,
                    py: 0.5,
                    px: 2,
                    textTransform: 'none',
                    fontWeight: 500,
                  },
                }}
              >
                <Tab icon={<MapIcon sx={{ fontSize: 18 }} />} iconPosition="start" label="Map" value="map" />
                <Tab icon={<TreemapIcon sx={{ fontSize: 18 }} />} iconPosition="start" label="Treemap" value="treemap" />
              </Tabs>
            </Box>
          </Box>

          {snapshotLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
              <CircularProgress />
            </Box>
          ) : snapshotViewMode === 'map' ? (
            // US Map View
            mapChartOption ? (
              <Box sx={{ height: 450 }}>
                <ReactECharts
                  option={mapChartOption}
                  style={{ height: '100%', width: '100%' }}
                  opts={{ renderer: 'svg' }}
                />
              </Box>
            ) : (
              <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', py: 6 }}>
                <CircularProgress size={24} sx={{ mr: 2 }} />
                <Typography color="text.secondary">Loading map...</Typography>
              </Box>
            )
          ) : treemapData.length > 0 && treemapContentRenderer ? (
            // Treemap View
            <Box sx={{ height: 350 }}>
              <ResponsiveContainer width="100%" height="100%">
                <Treemap
                  data={treemapData}
                  dataKey="value"
                  aspectRatio={4 / 3}
                  stroke="#fff"
                  content={treemapContentRenderer}
                >
                  <Tooltip
                    content={({ payload }) => {
                      if (!payload || !payload.length) return null;
                      const data = payload[0].payload;
                      return (
                        <Box
                          sx={{
                            background: 'rgba(255,255,255,0.95)',
                            border: '1px solid #e0e0e0',
                            borderRadius: 1,
                            p: 1.5,
                            boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
                          }}
                        >
                          <Typography variant="subtitle2" fontWeight="bold">
                            {data.name}
                          </Typography>
                          <Typography variant="body2" color="text.secondary">
                            Real GDP: {formatValue(data.value, snapshotData?.unit, snapshotData?.unit_mult)}
                          </Typography>
                        </Box>
                      );
                    }}
                  />
                </Treemap>
              </ResponsiveContainer>
            </Box>
          ) : (
            <Alert severity="info">No data available for state GDP snapshot</Alert>
          )}
        </CardContent>
      </Card>

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
      <Box sx={{ display: 'flex', gap: 2, mb: 2, flexWrap: 'wrap' }}>
        <Autocomplete
          options={tables || []}
          groupBy={(t) => {
            if (t.table_name.startsWith('SAGDP')) return 'State GDP';
            if (t.table_name.startsWith('CAGDP')) return 'County GDP';
            if (t.table_name.startsWith('SAINC')) return 'State Income';
            if (t.table_name.startsWith('CAINC')) return 'County Income';
            return 'Other';
          }}
          getOptionLabel={(t) => `${t.table_name} - ${t.table_description.substring(0, 50)}...`}
          value={tables?.find(t => t.table_name === selectedTable) || null}
          onChange={(_, newValue) => {
            setSelectedTable(newValue?.table_name || '');
            setSelectedLineCode(null);
          }}
          loading={tablesLoading}
          sx={{ minWidth: 350, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField {...params} label="Select Table" size="small" />
          )}
        />

        <Autocomplete
          options={lineCodes || []}
          getOptionLabel={(lc) => `${lc.line_code}. ${lc.line_description}`}
          value={selectedLineCode}
          onChange={(_, newValue) => setSelectedLineCode(newValue)}
          loading={lineCodesLoading}
          disabled={!selectedTable}
          sx={{ minWidth: 350, flexGrow: 1 }}
          renderInput={(params) => (
            <TextField {...params} label="Select Metric" size="small" />
          )}
        />

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Geo Type</InputLabel>
          <Select
            value={geoType}
            label="Geo Type"
            onChange={(e) => {
              setGeoType(e.target.value);
              setSelectedGeos([]);
            }}
          >
            <MenuItem value="State">States</MenuItem>
            <MenuItem value="County">Counties</MenuItem>
            <MenuItem value="Nation">Nation</MenuItem>
          </Select>
        </FormControl>

        <Autocomplete
          options={geoOptions?.filter(g => !selectedGeos.find(sg => sg.geo_fips === g.geo_fips)) || []}
          getOptionLabel={(g) => g.geo_name}
          value={null}
          onChange={(_, newValue) => handleAddGeo(newValue)}
          onInputChange={(_, value) => setGeoSearch(value)}
          loading={geosLoading}
          disabled={!selectedLineCode || selectedGeos.length >= 8}
          sx={{ minWidth: 250 }}
          renderInput={(params) => (
            <TextField
              {...params}
              label={selectedGeos.length >= 8 ? "Max 8 geographies" : "Add Geography"}
              size="small"
              placeholder="Search..."
            />
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
                '&:hover': { bgcolor: 'primary.dark' },
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

      {/* Selected Geographies Chips */}
      {selectedGeos.length > 0 && (
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 3 }}>
          {selectedGeos.map((geo, idx) => (
            <Chip
              key={geo.geo_fips}
              label={geo.geo_name}
              onDelete={() => handleRemoveGeo(geo.geo_fips)}
              sx={{
                bgcolor: SERIES_COLORS[idx % SERIES_COLORS.length],
                color: 'white',
                fontWeight: 500,
                '& .MuiChip-deleteIcon': {
                  color: 'rgba(255,255,255,0.7)',
                  '&:hover': { color: 'white' },
                },
              }}
            />
          ))}
        </Box>
      )}

      {/* Data Display - Full Width */}
      {selectedGeos.length === 0 ? (
        <Card>
          <CardContent sx={{ py: 6, textAlign: 'center' }}>
            <Typography color="text.secondary">Select a table, metric, and geography to view data</Typography>
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
        <Alert severity="warning">No data available for the selected options.</Alert>
      ) : (
        <>
          {/* Stats Card */}
          <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                <Box>
                  <Typography variant="h6" fontWeight="bold">
                    {selectedGeos.length === 1
                      ? primaryGeo?.geo_name
                      : `${selectedGeos.length} Geographies Selected`}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    {selectedTable} | {selectedLineCode?.line_description}
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
                    Latest Value {selectedGeos.length > 1 && '(Primary)'}
                  </Typography>
                  <Typography variant="h4" fontWeight="bold" color="primary.main">
                    {formatValue(latestValue?.value ?? null, primaryGeoData?.unit, primaryGeoData?.unit_mult)}
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
                  <Typography variant="body1" fontWeight="medium">{primaryGeoData?.unit || 'N/A'}</Typography>
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
                      {allGeoData.map(({ geo, color }, idx) => (
                        <linearGradient key={geo.geo_fips} id={`regional-color-${idx}`} x1="0" y1="0" x2="0" y2="1">
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
                      tickFormatter={(v) => formatValue(v, primaryGeoData?.unit, primaryGeoData?.unit_mult)}
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
                        const geoInfo = allGeoData.find(g => g.geo.geo_fips === name);
                        return [
                          formatValue(value, geoInfo?.data?.unit, geoInfo?.data?.unit_mult),
                          geoInfo?.geo.geo_name || name
                        ];
                      }}
                    />
                    {selectedGeos.length === 1 && (
                      <Area
                        type="monotone"
                        dataKey={selectedGeos[0].geo_fips}
                        stroke="transparent"
                        fill="url(#regional-color-0)"
                      />
                    )}
                    {allGeoData.map(({ geo, color }, idx) => (
                      <Line
                        key={geo.geo_fips}
                        type="monotone"
                        dataKey={geo.geo_fips}
                        name={geo.geo_fips}
                        stroke={color}
                        strokeWidth={2.5}
                        dot={false}
                        activeDot={{ r: 6, fill: color, stroke: '#fff', strokeWidth: 3 }}
                      />
                    ))}
                    {selectedGeos.length > 1 && (
                      <Legend
                        verticalAlign="top"
                        height={36}
                        wrapperStyle={{ paddingBottom: 10 }}
                        formatter={(value) => {
                          const geo = selectedGeos.find(g => g.geo_fips === value);
                          return geo?.geo_name || value;
                        }}
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
                        Year
                      </TableCell>
                      {allGeoData.map(({ geo, color }) => (
                        <TableCell
                          key={geo.geo_fips}
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
                              {geo.geo_name.substring(0, 20)}
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
                        {allGeoData.map(({ geo, data }) => (
                          <TableCell
                            key={geo.geo_fips}
                            align="right"
                            sx={{ fontFamily: 'monospace', borderLeft: '1px solid', borderColor: 'divider' }}
                          >
                            {formatValue(row[geo.geo_fips], data?.unit, data?.unit_mult)}
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
