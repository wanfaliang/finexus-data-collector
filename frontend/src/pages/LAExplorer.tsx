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
} from '@mui/material';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { TrendingUp } from '@mui/icons-material';
import { laExplorerAPI } from '../api/client';
import type { LASeriesInfo } from '../api/client';

export default function LAExplorer() {
  const [selectedArea, setSelectedArea] = useState<string>('');
  const [selectedMeasure, setSelectedMeasure] = useState<string>('');
  const [selectedSeasonal, setSelectedSeasonal] = useState<string>('');
  const [selectedSeries, setSelectedSeries] = useState<string | null>(null);

  // Fetch dimensions
  const { data: dimensions, isLoading: loadingDimensions } = useQuery({
    queryKey: ['la', 'dimensions'],
    queryFn: laExplorerAPI.getDimensions,
  });

  // Fetch series list
  const { data: seriesData, isLoading: loadingSeries } = useQuery({
    queryKey: ['la', 'series', selectedArea, selectedMeasure, selectedSeasonal],
    queryFn: () =>
      laExplorerAPI.getSeries({
        area_code: selectedArea || undefined,
        measure_code: selectedMeasure || undefined,
        seasonal_code: selectedSeasonal || undefined,
        active_only: true,
        limit: 50,
      }),
  });

  // Fetch data for selected series
  const { data: chartData, isLoading: loadingData } = useQuery({
    queryKey: ['la', 'data', selectedSeries],
    queryFn: () => laExplorerAPI.getSeriesData(selectedSeries!),
    enabled: !!selectedSeries,
  });

  if (loadingDimensions) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <CircularProgress size={40} />
      </Box>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h5" fontWeight="700" sx={{ color: 'text.primary', mb: 0.5 }}>
          LA - Local Area Unemployment Statistics Explorer
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Explore unemployment data by area and measure type
        </Typography>
      </Box>

      {/* Filters */}
      <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
        <CardContent sx={{ p: 2 }}>
          <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
            Filter Series
          </Typography>
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2 }}>
            <FormControl size="small" fullWidth>
              <InputLabel>Area</InputLabel>
              <Select
                value={selectedArea}
                label="Area"
                onChange={(e) => setSelectedArea(e.target.value)}
              >
                <MenuItem value="">All Areas</MenuItem>
                {dimensions?.areas.map((area) => (
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
        </CardContent>
      </Card>

      {/* Series List */}
      <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
        <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="subtitle2" fontWeight="600">
            Series ({seriesData?.total || 0})
          </Typography>
          {selectedSeries && (
            <Button size="small" onClick={() => setSelectedSeries(null)} variant="outlined">
              Clear Selection
            </Button>
          )}
        </Box>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Series ID</TableCell>
                <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Area</TableCell>
                <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Measure</TableCell>
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
                seriesData?.series.map((series: LASeriesInfo) => (
                  <TableRow
                    key={series.series_id}
                    sx={{
                      cursor: 'pointer',
                      bgcolor: selectedSeries === series.series_id ? 'action.selected' : 'inherit',
                      '&:hover': { bgcolor: 'action.hover' },
                    }}
                    onClick={() => setSelectedSeries(series.series_id)}
                  >
                    <TableCell sx={{ fontSize: '0.8rem', fontFamily: 'monospace' }}>
                      {series.series_id}
                    </TableCell>
                    <TableCell sx={{ fontSize: '0.75rem' }}>{series.area_name}</TableCell>
                    <TableCell sx={{ fontSize: '0.75rem' }}>{series.measure_name}</TableCell>
                    <TableCell>
                      <Chip
                        label={series.seasonal_code === 'S' ? 'Adjusted' : 'Unadjusted'}
                        size="small"
                        sx={{ fontSize: '0.65rem', height: 20 }}
                      />
                    </TableCell>
                    <TableCell sx={{ fontSize: '0.75rem' }}>
                      {series.begin_year} - {series.end_year || 'Present'}
                    </TableCell>
                    <TableCell>
                      <Button
                        size="small"
                        startIcon={<TrendingUp />}
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedSeries(series.series_id);
                        }}
                        sx={{ fontSize: '0.7rem', py: 0.25 }}
                      >
                        View
                      </Button>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Card>

      {/* Chart */}
      {selectedSeries && (
        <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
          <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Typography variant="subtitle2" fontWeight="600">
              Time Series Data: {selectedSeries}
            </Typography>
            {chartData?.series[0] && (
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
                {chartData.series[0].area_name} - {chartData.series[0].measure_name}
              </Typography>
            )}
          </Box>
          <Box sx={{ p: 2, height: 400 }}>
            {loadingData ? (
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
            ) : (
              <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                <Typography variant="body2" color="text.secondary">
                  No data available
                </Typography>
              </Box>
            )}
          </Box>
        </Card>
      )}
    </Box>
  );
}
