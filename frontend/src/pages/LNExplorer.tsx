import { useState } from 'react';
import { useQuery, useQueries } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  ToggleButton,
  ToggleButtonGroup,
  Chip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
} from '@mui/material';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';
import { TrendingUp, TrendingDown, ArrowBack } from '@mui/icons-material';
import { lnExplorerAPI, type LNSeriesInfo } from '../api/client';

// Helper functions
const formatRate = (value?: number) => {
  if (value === undefined || value === null) return 'N/A';
  return `${value.toFixed(1)}%`;
};

const formatChange = (value?: number) => {
  if (value === undefined || value === null) return 'N/A';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(1)}pp`;
};

const formatPeriod = (periodCode?: string): string => {
  if (!periodCode) return '';
  const monthMap: Record<string, string> = {
    'M01': 'Jan', 'M02': 'Feb', 'M03': 'Mar', 'M04': 'Apr',
    'M05': 'May', 'M06': 'Jun', 'M07': 'Jul', 'M08': 'Aug',
    'M09': 'Sep', 'M10': 'Oct', 'M11': 'Nov', 'M12': 'Dec',
  };
  return monthMap[periodCode] || periodCode;
};

export default function LNExplorer() {
  // Timeline state - for controlling time range and selected periods
  const [overviewTimeRange, setOverviewTimeRange] = useState<number>(12);
  const [selectedOverviewPeriod, setSelectedOverviewPeriod] = useState<{year: number, period: string} | null>(null);
  const [demographicTimeRange, setDemographicTimeRange] = useState<number>(12);
  const [selectedDemographicPeriod, setSelectedDemographicPeriod] = useState<{year: number, period: string} | null>(null);
  const [occupationTimeRange, setOccupationTimeRange] = useState<number>(12);
  const [selectedOccupationPeriod, setSelectedOccupationPeriod] = useState<{year: number, period: string} | null>(null);
  const [industryTimeRange, setIndustryTimeRange] = useState<number>(12);
  const [selectedIndustryPeriod, setSelectedIndustryPeriod] = useState<{year: number, period: string} | null>(null);

  // State for view toggles
  const [demographicView, setDemographicView] = useState<'table' | 'chart'>('table');
  const [occupationView, setOccupationView] = useState<'table' | 'chart'>('table');
  const [industryView, setIndustryView] = useState<'table' | 'chart'>('table');

  // State for Series Detail Explorer
  const [showSeriesExplorer, setShowSeriesExplorer] = useState<boolean>(false);
  const [selectedLfstCode, setSelectedLfstCode] = useState<string>('');
  const [selectedAgesCode, setSelectedAgesCode] = useState<string>('');
  const [selectedSexsCode, setSelectedSexsCode] = useState<string>('');
  const [selectedRaceCode, setSelectedRaceCode] = useState<string>('');
  const [selectedSeasonal, setSelectedSeasonal] = useState<string>('S');
  const [selectedSeries, setSelectedSeries] = useState<string[]>([]);

  // Fetch overview data (snapshot - latest or fallback)
  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['ln', 'overview'],
    queryFn: () => lnExplorerAPI.getOverview(),
  });

  // Fetch overview timeline
  const { data: overviewTimeline, isLoading: loadingOverviewTimeline } = useQuery({
    queryKey: ['ln', 'overview', 'timeline', overviewTimeRange],
    queryFn: () => lnExplorerAPI.getOverviewTimeline(overviewTimeRange),
  });

  // Fetch demographic analysis (snapshot)
  const { data: demographics, isLoading: loadingDemographics } = useQuery({
    queryKey: ['ln', 'demographics'],
    queryFn: () => lnExplorerAPI.getDemographicAnalysis(),
  });

  // Fetch demographic timelines for each dimension
  const demographicTimelineQueries = useQueries({
    queries: ['age', 'sex', 'race', 'education'].map(dimension_type => ({
      queryKey: ['ln', 'demographics', 'timeline', dimension_type, demographicTimeRange],
      queryFn: () => lnExplorerAPI.getDemographicTimeline(dimension_type, demographicTimeRange),
    })),
  });

  // Fetch occupation analysis (snapshot)
  const { data: occupations, isLoading: loadingOccupations } = useQuery({
    queryKey: ['ln', 'occupations'],
    queryFn: () => lnExplorerAPI.getOccupationAnalysis(),
  });

  // Fetch occupation timeline
  const { data: occupationTimeline, isLoading: loadingOccupationTimeline } = useQuery({
    queryKey: ['ln', 'occupations', 'timeline', occupationTimeRange],
    queryFn: () => lnExplorerAPI.getOccupationTimeline(occupationTimeRange),
  });

  // Fetch industry analysis (snapshot)
  const { data: industries, isLoading: loadingIndustries } = useQuery({
    queryKey: ['ln', 'industries'],
    queryFn: () => lnExplorerAPI.getIndustryAnalysis(),
  });

  // Fetch industry timeline
  const { data: industryTimeline, isLoading: loadingIndustryTimeline } = useQuery({
    queryKey: ['ln', 'industries', 'timeline', industryTimeRange],
    queryFn: () => lnExplorerAPI.getIndustryTimeline(industryTimeRange),
  });

  // Fetch dimensions for series explorer
  const { data: dimensions } = useQuery({
    queryKey: ['ln', 'dimensions'],
    queryFn: () => lnExplorerAPI.getDimensions(),
    enabled: showSeriesExplorer,
  });

  // Fetch series list for explorer
  const { data: seriesList } = useQuery({
    queryKey: ['ln', 'series', selectedLfstCode, selectedAgesCode, selectedSexsCode, selectedRaceCode, selectedSeasonal],
    queryFn: () => lnExplorerAPI.getSeries({
      lfst_code: selectedLfstCode || undefined,
      ages_code: selectedAgesCode || undefined,
      sexs_code: selectedSexsCode || undefined,
      race_code: selectedRaceCode || undefined,
      seasonal: selectedSeasonal || undefined,
      limit: 100,
    }),
    enabled: showSeriesExplorer,
  });

  // Fetch data for selected series
  const seriesDataQueries = useQueries({
    queries: selectedSeries.map(seriesId => ({
      queryKey: ['ln', 'seriesData', seriesId],
      queryFn: () => lnExplorerAPI.getSeriesData(seriesId),
    })),
  });

  const handleAddSeries = (series: LNSeriesInfo) => {
    if (!selectedSeries.includes(series.series_id)) {
      setSelectedSeries([...selectedSeries, series.series_id]);
    }
  };

  const handleRemoveSeries = (seriesId: string) => {
    setSelectedSeries(selectedSeries.filter(id => id !== seriesId));
  };

  // Timeline Selector Component
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
          {timeline.map((point, index) => {
            const isSelected = selectedPeriod?.year === point.year && selectedPeriod?.period === point.period;
            const isLatest = index === timeline.length - 1;
            const shouldShowLabel = index % Math.max(1, Math.floor(timeline.length / 8)) === 0 || index === timeline.length - 1;

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
                onClick={() => onSelectPeriod({ year: point.year, period: point.period })}
              >
                {/* Dot */}
                <Box
                  className="dot"
                  sx={{
                    width: isSelected ? 14 : 10,
                    height: isSelected ? 14 : 10,
                    borderRadius: '50%',
                    bgcolor: isSelected ? 'primary.main' : (isLatest && !selectedPeriod) ? 'primary.light' : 'grey.400',
                    border: isSelected ? '3px solid' : '2px solid',
                    borderColor: isSelected ? 'primary.dark' : 'white',
                    transition: 'all 0.2s ease',
                    zIndex: isSelected ? 3 : 1,
                    boxShadow: isSelected ? '0 2px 8px rgba(25, 118, 210, 0.4)' : 'none',
                  }}
                />
                {/* Label */}
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
  );

  // Series Explorer View
  if (showSeriesExplorer) {
    return (
      <Box sx={{ p: 0 }}>
        <Box sx={{ mb: 3, display: 'flex', alignItems: 'center', gap: 2 }}>
          <Button
            startIcon={<ArrowBack />}
            onClick={() => setShowSeriesExplorer(false)}
            variant="outlined"
          >
            Back to Overview
          </Button>
          <Typography variant="h4" fontWeight="bold">
            LN - Series Detail Explorer
          </Typography>
        </Box>

        {/* Filters */}
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Filter Series
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={3}>
                <FormControl fullWidth size="small">
                  <InputLabel>Labor Force Status</InputLabel>
                  <Select
                    value={selectedLfstCode}
                    onChange={(e) => setSelectedLfstCode(e.target.value)}
                    label="Labor Force Status"
                  >
                    <MenuItem value="">All</MenuItem>
                    {dimensions?.labor_force_statuses.map(item => (
                      <MenuItem key={item.code} value={item.code}>{item.text}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={3}>
                <FormControl fullWidth size="small">
                  <InputLabel>Age</InputLabel>
                  <Select
                    value={selectedAgesCode}
                    onChange={(e) => setSelectedAgesCode(e.target.value)}
                    label="Age"
                  >
                    <MenuItem value="">All</MenuItem>
                    {dimensions?.ages.map(item => (
                      <MenuItem key={item.code} value={item.code}>{item.text}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={3}>
                <FormControl fullWidth size="small">
                  <InputLabel>Sex</InputLabel>
                  <Select
                    value={selectedSexsCode}
                    onChange={(e) => setSelectedSexsCode(e.target.value)}
                    label="Sex"
                  >
                    <MenuItem value="">All</MenuItem>
                    {dimensions?.sexes.map(item => (
                      <MenuItem key={item.code} value={item.code}>{item.text}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={3}>
                <FormControl fullWidth size="small">
                  <InputLabel>Seasonal Adjustment</InputLabel>
                  <Select
                    value={selectedSeasonal}
                    onChange={(e) => setSelectedSeasonal(e.target.value)}
                    label="Seasonal Adjustment"
                  >
                    <MenuItem value="">All</MenuItem>
                    <MenuItem value="S">Seasonally Adjusted</MenuItem>
                    <MenuItem value="U">Not Seasonally Adjusted</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </CardContent>
        </Card>

        {/* Series List */}
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Available Series ({seriesList?.total || 0})
            </Typography>
            <TableContainer sx={{ maxHeight: 400 }}>
              <Table stickyHeader size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Series ID</TableCell>
                    <TableCell>Title</TableCell>
                    <TableCell>Seasonal</TableCell>
                    <TableCell align="center">Action</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {seriesList?.series.map((series) => (
                    <TableRow key={series.series_id}>
                      <TableCell><Typography variant="body2" fontFamily="monospace">{series.series_id}</Typography></TableCell>
                      <TableCell><Typography variant="body2">{series.series_title}</Typography></TableCell>
                      <TableCell><Chip label={series.seasonal === 'S' ? 'SA' : 'NSA'} size="small" /></TableCell>
                      <TableCell align="center">
                        <Button
                          size="small"
                          variant="outlined"
                          onClick={() => handleAddSeries(series)}
                          disabled={selectedSeries.includes(series.series_id)}
                        >
                          {selectedSeries.includes(series.series_id) ? 'Added' : 'Add'}
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </CardContent>
        </Card>

        {/* Selected Series Charts */}
        {selectedSeries.length > 0 && (
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Selected Series Charts
              </Typography>
              <Box sx={{ mb: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                {selectedSeries.map(seriesId => (
                  <Chip
                    key={seriesId}
                    label={seriesId}
                    onDelete={() => handleRemoveSeries(seriesId)}
                  />
                ))}
              </Box>
              {seriesDataQueries.map((query, index) => {
                if (query.isLoading) return <CircularProgress key={index} />;
                if (!query.data) return null;

                const seriesData = query.data.series[0];
                const chartData = seriesData.data_points.map(dp => ({
                  period_name: `${formatPeriod(dp.period)} ${dp.year}`,
                  value: dp.value,
                }));

                return (
                  <Box key={selectedSeries[index]} sx={{ mb: 4 }}>
                    <Typography variant="subtitle1" fontWeight="600" gutterBottom>
                      {seriesData.series_title}
                    </Typography>
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                          dataKey="period_name"
                          tick={{ fontSize: 11 }}
                          interval={Math.floor(chartData.length / 12)}
                        />
                        <YAxis />
                        <Tooltip />
                        <Line type="monotone" dataKey="value" stroke="#1976d2" strokeWidth={2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </Box>
                );
              })}
            </CardContent>
          </Card>
        )}
      </Box>
    );
  }

  // Main Dashboard View
  return (
    <Box sx={{ p: 0 }}>
      <Typography variant="h4" gutterBottom fontWeight="bold" sx={{ mb: 3 }}>
        LN - Labor Force Statistics Explorer
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 4 }}>
        Current Population Survey (CPS) • Unemployment & Labor Force Data
      </Typography>

      <Box sx={{ mb: 3 }}>
        <Button
          variant="outlined"
          onClick={() => setShowSeriesExplorer(true)}
          sx={{ mr: 2 }}
        >
          Series Detail Explorer
        </Button>
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
            Overview - Key Labor Market Indicators
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Box>
            {/* Time Range Filter */}
            <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
              <FormControl size="small" sx={{ minWidth: 200 }}>
                <InputLabel>Time Range</InputLabel>
                <Select
                  value={overviewTimeRange}
                  label="Time Range"
                  onChange={(e) => {
                    setOverviewTimeRange(e.target.value as number);
                    setSelectedOverviewPeriod(null);
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
                  Labor Market Rates Timeline
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
                      <YAxis label={{ value: 'Rate (%)', angle: -90, position: 'insideLeft' }} />
                      <Tooltip cursor={{ stroke: '#1976d2', strokeWidth: 2 }} />
                      <Legend />
                      <Line
                        type="monotone"
                        dataKey="headline_value"
                        stroke="#dc2626"
                        name="Unemployment"
                        strokeWidth={2}
                        dot={{ r: 3 }}
                      />
                      <Line
                        type="monotone"
                        dataKey="lfpr_value"
                        stroke="#0891b2"
                        name="Labor Force Participation"
                        strokeWidth={2}
                        dot={{ r: 3 }}
                      />
                      <Line
                        type="monotone"
                        dataKey="epop_value"
                        stroke="#059669"
                        name="Employment-Pop Ratio"
                        strokeWidth={2}
                        dot={{ r: 3 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </Box>

                {/* Timeline Selector */}
                <TimelineSelector
                  timeline={overviewTimeline.timeline}
                  selectedPeriod={selectedOverviewPeriod}
                  onSelectPeriod={setSelectedOverviewPeriod}
                />
              </Box>
            )}

            {loadingOverview || loadingOverviewTimeline ? (
              <Box display="flex" justifyContent="center" p={4}>
                <CircularProgress />
              </Box>
            ) : (() => {
              // Get selected or latest data point
              const selectedData = selectedOverviewPeriod && overviewTimeline
                ? overviewTimeline.timeline.find(p => p.year === selectedOverviewPeriod.year && p.period === selectedOverviewPeriod.period)
                : overviewTimeline?.timeline[overviewTimeline.timeline.length - 1];

              const headlineValue = selectedData?.headline_value ?? overview?.headline_unemployment?.latest_value;
              const headlineDate = selectedData ? `${formatPeriod(selectedData.period)} ${selectedData.year}` : overview?.headline_unemployment?.latest_date;

              const lfprValue = selectedData?.lfpr_value ?? overview?.labor_force_participation?.latest_value;
              const lfprDate = selectedData ? `${formatPeriod(selectedData.period)} ${selectedData.year}` : overview?.labor_force_participation?.latest_date;

              const epopValue = selectedData?.epop_value ?? overview?.employment_population_ratio?.latest_value;
              const epopDate = selectedData ? `${formatPeriod(selectedData.period)} ${selectedData.year}` : overview?.employment_population_ratio?.latest_date;

              // For M/M and Y/Y, use the overview API data (only available for latest)
              const headlineMom = overview?.headline_unemployment?.month_over_month;
              const headlineYoy = overview?.headline_unemployment?.year_over_year;
              const lfprMom = overview?.labor_force_participation?.month_over_month;
              const lfprYoy = overview?.labor_force_participation?.year_over_year;
              const epopMom = overview?.employment_population_ratio?.month_over_month;
              const epopYoy = overview?.employment_population_ratio?.year_over_year;

              return (
                <Grid container spacing={3}>
                  {/* Headline Unemployment Rate */}
                  <Grid item xs={12} md={4}>
                    <Card sx={{ bgcolor: 'error.50', border: '2px solid', borderColor: 'error.main' }}>
                      <CardContent>
                        <Typography variant="overline" color="text.secondary" fontWeight="600">
                          Unemployment Rate
                        </Typography>
                        <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 1 }}>
                          {headlineDate}
                        </Typography>
                        <Typography variant="h3" fontWeight="bold" sx={{ color: 'error.main', my: 1 }}>
                          {formatRate(headlineValue)}
                        </Typography>
                        {!selectedOverviewPeriod && (
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 2 }}>
                            {headlineMom !== undefined && (
                              <Chip
                                size="small"
                                icon={headlineMom >= 0 ? <TrendingUp /> : <TrendingDown />}
                                label={`M/M: ${formatChange(headlineMom)}`}
                                color={headlineMom >= 0 ? 'error' : 'success'}
                                sx={{ fontWeight: 600 }}
                              />
                            )}
                            {headlineYoy !== undefined && (
                              <Chip
                                size="small"
                                icon={headlineYoy >= 0 ? <TrendingUp /> : <TrendingDown />}
                                label={`Y/Y: ${formatChange(headlineYoy)}`}
                                color={headlineYoy >= 0 ? 'error' : 'success'}
                                sx={{ fontWeight: 600 }}
                              />
                            )}
                          </Box>
                        )}
                      </CardContent>
                    </Card>
                  </Grid>

                  {/* Labor Force Participation Rate */}
                  <Grid item xs={12} md={4}>
                    <Card sx={{ bgcolor: 'info.50', border: '2px solid', borderColor: 'info.main' }}>
                      <CardContent>
                        <Typography variant="overline" color="text.secondary" fontWeight="600">
                          Labor Force Participation
                        </Typography>
                        <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 1 }}>
                          {lfprDate}
                        </Typography>
                        <Typography variant="h3" fontWeight="bold" sx={{ color: 'info.main', my: 1 }}>
                          {formatRate(lfprValue)}
                        </Typography>
                        {!selectedOverviewPeriod && (
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 2 }}>
                            {lfprMom !== undefined && (
                              <Chip
                                size="small"
                                icon={lfprMom >= 0 ? <TrendingUp /> : <TrendingDown />}
                                label={`M/M: ${formatChange(lfprMom)}`}
                                color={lfprMom >= 0 ? 'success' : 'error'}
                                sx={{ fontWeight: 600 }}
                              />
                            )}
                            {lfprYoy !== undefined && (
                              <Chip
                                size="small"
                                icon={lfprYoy >= 0 ? <TrendingUp /> : <TrendingDown />}
                                label={`Y/Y: ${formatChange(lfprYoy)}`}
                                color={lfprYoy >= 0 ? 'success' : 'error'}
                                sx={{ fontWeight: 600 }}
                              />
                            )}
                          </Box>
                        )}
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2 }}>
                          % of population in labor force
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>

                  {/* Employment-Population Ratio */}
                  <Grid item xs={12} md={4}>
                    <Card sx={{ bgcolor: 'success.50', border: '2px solid', borderColor: 'success.main' }}>
                      <CardContent>
                        <Typography variant="overline" color="text.secondary" fontWeight="600">
                          Employment-Pop Ratio
                        </Typography>
                        <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 1 }}>
                          {epopDate}
                        </Typography>
                        <Typography variant="h3" fontWeight="bold" sx={{ color: 'success.main', my: 1 }}>
                          {formatRate(epopValue)}
                        </Typography>
                        {!selectedOverviewPeriod && (
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 2 }}>
                            {epopMom !== undefined && (
                              <Chip
                                size="small"
                                icon={epopMom >= 0 ? <TrendingUp /> : <TrendingDown />}
                                label={`M/M: ${formatChange(epopMom)}`}
                                color={epopMom >= 0 ? 'success' : 'error'}
                                sx={{ fontWeight: 600 }}
                              />
                            )}
                            {epopYoy !== undefined && (
                              <Chip
                                size="small"
                                icon={epopYoy >= 0 ? <TrendingUp /> : <TrendingDown />}
                                label={`Y/Y: ${formatChange(epopYoy)}`}
                                color={epopYoy >= 0 ? 'success' : 'error'}
                                sx={{ fontWeight: 600 }}
                              />
                            )}
                          </Box>
                        )}
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2 }}>
                          % of population employed
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                </Grid>
              );
            })()}
          </Box>
        </CardContent>
      </Card>

      {/* Section 2: Demographic Analysis */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'secondary.main',
          bgcolor: 'secondary.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'secondary.main' }}>
            Demographic Analysis - Unemployment by Group
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Box>
            {/* Time Range Filter and View Toggle */}
            <Box sx={{ display: 'flex', gap: 2, mb: 3, justifyContent: 'space-between' }}>
              <FormControl size="small" sx={{ minWidth: 200 }}>
                <InputLabel>Time Range</InputLabel>
                <Select
                  value={demographicTimeRange}
                  label="Time Range"
                  onChange={(e) => {
                    setDemographicTimeRange(e.target.value as number);
                    setSelectedDemographicPeriod(null);
                  }}
                >
                  <MenuItem value={12}>Last 12 months</MenuItem>
                  <MenuItem value={24}>Last 2 years</MenuItem>
                  <MenuItem value={60}>Last 5 years</MenuItem>
                </Select>
              </FormControl>
              <ToggleButtonGroup
                value={demographicView}
                exclusive
                onChange={(_, newValue) => newValue && setDemographicView(newValue)}
                size="small"
              >
                <ToggleButton value="table">Table</ToggleButton>
                <ToggleButton value="chart">Chart</ToggleButton>
              </ToggleButtonGroup>
            </Box>

            {loadingDemographics ? (
              <Box display="flex" justifyContent="center" p={4}>
                <CircularProgress />
              </Box>
            ) : demographics?.breakdowns?.map((breakdown, idx) => {
              const timelineData = demographicTimelineQueries[idx]?.data;

              return (
                <Box key={idx} sx={{ mb: 4 }}>
                  <Typography variant="h6" fontWeight="600" sx={{ mb: 2 }}>
                    {breakdown.dimension_name}
                  </Typography>

                  {/* Timeline Chart */}
                  {timelineData && timelineData.timeline.length > 0 && (
                    <Box sx={{ mb: 3 }}>
                      <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                        Historical Trend
                      </Typography>
                      <Box sx={{ width: '100%', height: 250 }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <LineChart data={timelineData.timeline}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis
                              dataKey="period_name"
                              tick={{ fontSize: 11 }}
                              angle={-45}
                              textAnchor="end"
                              height={80}
                            />
                            <YAxis label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft' }} />
                            <Tooltip cursor={{ stroke: '#1976d2', strokeWidth: 2 }} />
                            <Legend />
                            {breakdown.metrics.map((metric, i) => {
                              const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1'];
                              return (
                                <Line
                                  key={metric.dimension_name}
                                  type="monotone"
                                  dataKey={(dataPoint: any) => {
                                    const m = dataPoint.metrics.find((m: any) => m.dimension_name === metric.dimension_name);
                                    return m?.latest_value;
                                  }}
                                  stroke={colors[i % colors.length]}
                                  strokeWidth={2}
                                  name={metric.dimension_name}
                                  dot={{ r: 3 }}
                                />
                              );
                            })}
                          </LineChart>
                        </ResponsiveContainer>
                      </Box>

                      {/* Timeline Selector */}
                      <TimelineSelector
                        timeline={timelineData.timeline}
                        selectedPeriod={selectedDemographicPeriod}
                        onSelectPeriod={setSelectedDemographicPeriod}
                      />
                    </Box>
                  )}

                  {(() => {
                    // Get selected or latest data
                    const selectedTimelinePoint = selectedDemographicPeriod && timelineData
                      ? timelineData.timeline.find(p => p.year === selectedDemographicPeriod.year && p.period === selectedDemographicPeriod.period)
                      : timelineData?.timeline[timelineData.timeline.length - 1];

                    const displayMetrics = selectedTimelinePoint ? selectedTimelinePoint.metrics : breakdown.metrics;
                    const displayDate = selectedTimelinePoint ? `${formatPeriod(selectedTimelinePoint.period)} ${selectedTimelinePoint.year}` : null;

                    return (
                      <>
                        {displayDate && (
                          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
                            Showing data for: {displayDate}
                          </Typography>
                        )}

                        {demographicView === 'table' ? (
                          <TableContainer>
                            <Table size="small">
                              <TableHead>
                                <TableRow>
                                  <TableCell sx={{ fontWeight: 600 }}>Group</TableCell>
                                  <TableCell align="right" sx={{ fontWeight: 600 }}>Unemployment Rate</TableCell>
                                  {!selectedDemographicPeriod && (
                                    <>
                                      <TableCell align="right" sx={{ fontWeight: 600 }}>M/M Change</TableCell>
                                      <TableCell align="right" sx={{ fontWeight: 600 }}>Y/Y Change</TableCell>
                                    </>
                                  )}
                                </TableRow>
                              </TableHead>
                              <TableBody>
                                {displayMetrics.map((metric: any, metricIdx: number) => (
                                  <TableRow key={metricIdx}>
                                    <TableCell>{metric.dimension_name}</TableCell>
                                    <TableCell align="right">
                                      <Typography variant="body2" fontWeight="600" color="error.main">
                                        {formatRate(metric.latest_value)}
                                      </Typography>
                                    </TableCell>
                                    {!selectedDemographicPeriod && (
                                      <>
                                        <TableCell align="right">
                                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                            {metric.month_over_month !== undefined && metric.month_over_month !== null && (
                                              <>
                                                {metric.month_over_month >= 0 ? (
                                                  <TrendingUp fontSize="small" color="error" />
                                                ) : (
                                                  <TrendingDown fontSize="small" color="success" />
                                                )}
                                                <Typography
                                                  variant="body2"
                                                  color={metric.month_over_month >= 0 ? 'error.main' : 'success.main'}
                                                  fontWeight="600"
                                                >
                                                  {formatChange(metric.month_over_month)}
                                                </Typography>
                                              </>
                                            )}
                                          </Box>
                                        </TableCell>
                                        <TableCell align="right">
                                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                            {metric.year_over_year !== undefined && metric.year_over_year !== null && (
                                              <>
                                                {metric.year_over_year >= 0 ? (
                                                  <TrendingUp fontSize="small" color="error" />
                                                ) : (
                                                  <TrendingDown fontSize="small" color="success" />
                                                )}
                                                <Typography
                                                  variant="body2"
                                                  color={metric.year_over_year >= 0 ? 'error.main' : 'success.main'}
                                                  fontWeight="600"
                                                >
                                                  {formatChange(metric.year_over_year)}
                                                </Typography>
                                              </>
                                            )}
                                          </Box>
                                        </TableCell>
                                      </>
                                    )}
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </TableContainer>
                        ) : (
                          <Box sx={{ height: 300 }}>
                            <ResponsiveContainer width="100%" height="100%">
                              <BarChart data={displayMetrics.map((m: any) => ({
                                name: m.dimension_name,
                                rate: m.latest_value || 0,
                              }))}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis
                                  dataKey="name"
                                  tick={{ fontSize: 11 }}
                                  angle={-45}
                                  textAnchor="end"
                                  height={100}
                                />
                                <YAxis label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft' }} />
                                <Tooltip />
                                <Bar dataKey="rate">
                                  {displayMetrics.map((_: any, index: number) => {
                                    const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2', '#0288d1'];
                                    return <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />;
                                  })}
                                </Bar>
                              </BarChart>
                            </ResponsiveContainer>
                          </Box>
                        )}
                      </>
                    );
                  })()}
                </Box>
              );
            })}
          </Box>
        </CardContent>
      </Card>

      {/* Section 3: Occupation Analysis */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'warning.main',
          bgcolor: 'warning.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'warning.main' }}>
            Occupation Analysis - Unemployment by Occupation
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Box>
            {/* Time Range Filter and View Toggle */}
            <Box sx={{ display: 'flex', gap: 2, mb: 3, justifyContent: 'space-between' }}>
              <FormControl size="small" sx={{ minWidth: 200 }}>
                <InputLabel>Time Range</InputLabel>
                <Select
                  value={occupationTimeRange}
                  label="Time Range"
                  onChange={(e) => {
                    setOccupationTimeRange(e.target.value as number);
                    setSelectedOccupationPeriod(null);
                  }}
                >
                  <MenuItem value={12}>Last 12 months</MenuItem>
                  <MenuItem value={24}>Last 2 years</MenuItem>
                  <MenuItem value={60}>Last 5 years</MenuItem>
                </Select>
              </FormControl>
              <ToggleButtonGroup
                value={occupationView}
                exclusive
                onChange={(_, newValue) => newValue && setOccupationView(newValue)}
                size="small"
              >
                <ToggleButton value="table">Table</ToggleButton>
                <ToggleButton value="chart">Chart</ToggleButton>
              </ToggleButtonGroup>
            </Box>

            {loadingOccupations || loadingOccupationTimeline ? (
              <Box display="flex" justifyContent="center" p={4}>
                <CircularProgress />
              </Box>
            ) : (
              <>
                {/* Timeline Chart */}
                {occupationTimeline && occupationTimeline.timeline.length > 0 && (
                  <Box sx={{ mb: 3 }}>
                    <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                      Historical Trend
                    </Typography>
                    <Box sx={{ width: '100%', height: 250 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={occupationTimeline.timeline}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis
                            dataKey="period_name"
                            tick={{ fontSize: 11 }}
                            angle={-45}
                            textAnchor="end"
                            height={80}
                          />
                          <YAxis label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft' }} />
                          <Tooltip cursor={{ stroke: '#1976d2', strokeWidth: 2 }} />
                          <Legend />
                          {occupations?.occupations.map((metric, i) => {
                            const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2'];
                            return (
                              <Line
                                key={metric.dimension_name}
                                type="monotone"
                                dataKey={(dataPoint: any) => {
                                  const m = dataPoint.metrics.find((m: any) => m.dimension_name === metric.dimension_name);
                                  return m?.latest_value;
                                }}
                                stroke={colors[i % colors.length]}
                                strokeWidth={2}
                                name={metric.dimension_name}
                                dot={{ r: 3 }}
                              />
                            );
                          })}
                        </LineChart>
                      </ResponsiveContainer>
                    </Box>

                    {/* Timeline Selector */}
                    <TimelineSelector
                      timeline={occupationTimeline.timeline}
                      selectedPeriod={selectedOccupationPeriod}
                      onSelectPeriod={setSelectedOccupationPeriod}
                    />
                  </Box>
                )}

                {(() => {
                  // Get selected or latest data
                  const selectedTimelinePoint = selectedOccupationPeriod && occupationTimeline
                    ? occupationTimeline.timeline.find(p => p.year === selectedOccupationPeriod.year && p.period === selectedOccupationPeriod.period)
                    : occupationTimeline?.timeline[occupationTimeline.timeline.length - 1];

                  const displayMetrics = selectedTimelinePoint ? selectedTimelinePoint.metrics : occupations?.occupations || [];
                  const displayDate = selectedTimelinePoint ? `${formatPeriod(selectedTimelinePoint.period)} ${selectedTimelinePoint.year}` : null;

                  return (
                    <>
                      {displayDate && (
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
                          Showing data for: {displayDate}
                        </Typography>
                      )}

                      {occupationView === 'table' ? (
                        <TableContainer>
                          <Table size="small">
                            <TableHead>
                              <TableRow>
                                <TableCell sx={{ fontWeight: 600 }}>Occupation</TableCell>
                                <TableCell align="right" sx={{ fontWeight: 600 }}>Unemployment Rate</TableCell>
                                {!selectedOccupationPeriod && (
                                  <>
                                    <TableCell align="right" sx={{ fontWeight: 600 }}>M/M Change</TableCell>
                                    <TableCell align="right" sx={{ fontWeight: 600 }}>Y/Y Change</TableCell>
                                  </>
                                )}
                              </TableRow>
                            </TableHead>
                            <TableBody>
                              {displayMetrics.map((metric: any, idx: number) => (
                                <TableRow key={idx}>
                                  <TableCell>{metric.dimension_name}</TableCell>
                                  <TableCell align="right">
                                    <Typography variant="body2" fontWeight="600" color="error.main">
                                      {formatRate(metric.latest_value)}
                                    </Typography>
                                  </TableCell>
                                  {!selectedOccupationPeriod && (
                                    <>
                                      <TableCell align="right">
                                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                          {metric.month_over_month !== undefined && metric.month_over_month !== null && (
                                            <>
                                              {metric.month_over_month >= 0 ? (
                                                <TrendingUp fontSize="small" color="error" />
                                              ) : (
                                                <TrendingDown fontSize="small" color="success" />
                                              )}
                                              <Typography
                                                variant="body2"
                                                color={metric.month_over_month >= 0 ? 'error.main' : 'success.main'}
                                                fontWeight="600"
                                              >
                                                {formatChange(metric.month_over_month)}
                                              </Typography>
                                            </>
                                          )}
                                        </Box>
                                      </TableCell>
                                      <TableCell align="right">
                                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                          {metric.year_over_year !== undefined && metric.year_over_year !== null && (
                                            <>
                                              {metric.year_over_year >= 0 ? (
                                                <TrendingUp fontSize="small" color="error" />
                                              ) : (
                                                <TrendingDown fontSize="small" color="success" />
                                              )}
                                              <Typography
                                                variant="body2"
                                                color={metric.year_over_year >= 0 ? 'error.main' : 'success.main'}
                                                fontWeight="600"
                                              >
                                                {formatChange(metric.year_over_year)}
                                              </Typography>
                                            </>
                                          )}
                                        </Box>
                                      </TableCell>
                                    </>
                                  )}
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </TableContainer>
                      ) : (
                        <Box sx={{ height: 300 }}>
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={displayMetrics.map((m: any) => ({
                              name: m.dimension_name,
                              rate: m.latest_value || 0,
                            }))}>
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis
                                dataKey="name"
                                tick={{ fontSize: 11 }}
                                angle={-45}
                                textAnchor="end"
                                height={120}
                              />
                              <YAxis label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft' }} />
                              <Tooltip />
                              <Bar dataKey="rate">
                                {displayMetrics.map((_: any, index: number) => {
                                  const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2'];
                                  return <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />;
                                })}
                              </Bar>
                            </BarChart>
                          </ResponsiveContainer>
                        </Box>
                      )}
                    </>
                  );
                })()}
              </>
            )}
          </Box>
        </CardContent>
      </Card>

      {/* Section 4: Industry Analysis */}
      <Card sx={{ mb: 3, border: '2px solid', borderColor: 'divider', boxShadow: 2 }}>
        <Box sx={{
          px: 3,
          py: 2.5,
          borderBottom: '3px solid',
          borderColor: 'success.main',
          bgcolor: 'success.50',
        }}>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'success.main' }}>
            Industry Analysis - Unemployment by Industry
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Box>
            {/* Time Range Filter and View Toggle */}
            <Box sx={{ display: 'flex', gap: 2, mb: 3, justifyContent: 'space-between' }}>
              <FormControl size="small" sx={{ minWidth: 200 }}>
                <InputLabel>Time Range</InputLabel>
                <Select
                  value={industryTimeRange}
                  label="Time Range"
                  onChange={(e) => {
                    setIndustryTimeRange(e.target.value as number);
                    setSelectedIndustryPeriod(null);
                  }}
                >
                  <MenuItem value={12}>Last 12 months</MenuItem>
                  <MenuItem value={24}>Last 2 years</MenuItem>
                  <MenuItem value={60}>Last 5 years</MenuItem>
                </Select>
              </FormControl>
              <ToggleButtonGroup
                value={industryView}
                exclusive
                onChange={(_, newValue) => newValue && setIndustryView(newValue)}
                size="small"
              >
                <ToggleButton value="table">Table</ToggleButton>
                <ToggleButton value="chart">Chart</ToggleButton>
              </ToggleButtonGroup>
            </Box>

            {loadingIndustries || loadingIndustryTimeline ? (
              <Box display="flex" justifyContent="center" p={4}>
                <CircularProgress />
              </Box>
            ) : (
              <>
                {/* Timeline Chart */}
                {industryTimeline && industryTimeline.timeline.length > 0 && (
                  <Box sx={{ mb: 3 }}>
                    <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
                      Historical Trend (Top 5 Industries)
                    </Typography>
                    <Box sx={{ width: '100%', height: 250 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={industryTimeline.timeline}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis
                            dataKey="period_name"
                            tick={{ fontSize: 11 }}
                            angle={-45}
                            textAnchor="end"
                            height={80}
                          />
                          <YAxis label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft' }} />
                          <Tooltip cursor={{ stroke: '#1976d2', strokeWidth: 2 }} />
                          <Legend />
                          {industries?.industries.slice(0, 5).map((metric, i) => {
                            const colors = ['#1976d2', '#2e7d32', '#d32f2f', '#f57c00', '#7b1fa2'];
                            return (
                              <Line
                                key={metric.dimension_name}
                                type="monotone"
                                dataKey={(dataPoint: any) => {
                                  const m = dataPoint.metrics.find((m: any) => m.dimension_name === metric.dimension_name);
                                  return m?.latest_value;
                                }}
                                stroke={colors[i % colors.length]}
                                strokeWidth={2}
                                name={metric.dimension_name}
                                dot={{ r: 3 }}
                              />
                            );
                          })}
                        </LineChart>
                      </ResponsiveContainer>
                    </Box>

                    {/* Timeline Selector */}
                    <TimelineSelector
                      timeline={industryTimeline.timeline}
                      selectedPeriod={selectedIndustryPeriod}
                      onSelectPeriod={setSelectedIndustryPeriod}
                    />
                  </Box>
                )}

                {(() => {
                  // Get selected or latest data
                  const selectedTimelinePoint = selectedIndustryPeriod && industryTimeline
                    ? industryTimeline.timeline.find(p => p.year === selectedIndustryPeriod.year && p.period === selectedIndustryPeriod.period)
                    : industryTimeline?.timeline[industryTimeline.timeline.length - 1];

                  const displayMetrics = selectedTimelinePoint ? selectedTimelinePoint.metrics : industries?.industries || [];
                  const displayDate = selectedTimelinePoint ? `${formatPeriod(selectedTimelinePoint.period)} ${selectedTimelinePoint.year}` : null;

                  return (
                    <>
                      {displayDate && (
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
                          Showing data for: {displayDate}
                        </Typography>
                      )}

                      {industryView === 'table' ? (
                        <TableContainer>
                          <Table size="small">
                            <TableHead>
                              <TableRow>
                                <TableCell sx={{ fontWeight: 600 }}>Industry</TableCell>
                                <TableCell align="right" sx={{ fontWeight: 600 }}>Unemployment Rate</TableCell>
                                {!selectedIndustryPeriod && (
                                  <>
                                    <TableCell align="right" sx={{ fontWeight: 600 }}>M/M Change</TableCell>
                                    <TableCell align="right" sx={{ fontWeight: 600 }}>Y/Y Change</TableCell>
                                  </>
                                )}
                              </TableRow>
                            </TableHead>
                            <TableBody>
                              {displayMetrics.map((metric: any, idx: number) => (
                                <TableRow key={idx}>
                                  <TableCell>{metric.dimension_name}</TableCell>
                                  <TableCell align="right">
                                    <Typography variant="body2" fontWeight="600" color="error.main">
                                      {formatRate(metric.latest_value)}
                                    </Typography>
                                  </TableCell>
                                  {!selectedIndustryPeriod && (
                                    <>
                                      <TableCell align="right">
                                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                          {metric.month_over_month !== undefined && metric.month_over_month !== null && (
                                            <>
                                              {metric.month_over_month >= 0 ? (
                                                <TrendingUp fontSize="small" color="error" />
                                              ) : (
                                                <TrendingDown fontSize="small" color="success" />
                                              )}
                                              <Typography
                                                variant="body2"
                                                color={metric.month_over_month >= 0 ? 'error.main' : 'success.main'}
                                                fontWeight="600"
                                              >
                                                {formatChange(metric.month_over_month)}
                                              </Typography>
                                            </>
                                          )}
                                        </Box>
                                      </TableCell>
                                      <TableCell align="right">
                                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5 }}>
                                          {metric.year_over_year !== undefined && metric.year_over_year !== null && (
                                            <>
                                              {metric.year_over_year >= 0 ? (
                                                <TrendingUp fontSize="small" color="error" />
                                              ) : (
                                                <TrendingDown fontSize="small" color="success" />
                                              )}
                                              <Typography
                                                variant="body2"
                                                color={metric.year_over_year >= 0 ? 'error.main' : 'success.main'}
                                                fontWeight="600"
                                              >
                                                {formatChange(metric.year_over_year)}
                                              </Typography>
                                            </>
                                          )}
                                        </Box>
                                      </TableCell>
                                    </>
                                  )}
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </TableContainer>
                      ) : (
                        <Box sx={{ height: 350 }}>
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={displayMetrics.map((m: any) => ({
                              name: m.dimension_name,
                              rate: m.latest_value || 0,
                            }))}>
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis
                                dataKey="name"
                                tick={{ fontSize: 10 }}
                                angle={-45}
                                textAnchor="end"
                                height={150}
                              />
                              <YAxis label={{ value: 'Unemployment Rate (%)', angle: -90, position: 'insideLeft' }} />
                              <Tooltip />
                              <Bar dataKey="rate" fill="#1976d2" />
                            </BarChart>
                          </ResponsiveContainer>
                        </Box>
                      )}
                    </>
                  );
                })()}
              </>
            )}
          </Box>
        </CardContent>
      </Card>

      {/* Info box */}
      <Card sx={{ bgcolor: 'info.50', border: '1px solid', borderColor: 'info.main' }}>
        <CardContent>
          <Typography variant="body2" color="text.secondary">
            <strong>Note:</strong> All data is seasonally adjusted unless otherwise noted.
            Unemployment rate = (Unemployed / Labor Force) × 100.
            Labor Force Participation Rate = (Labor Force / Civilian Population) × 100.
            Click on timeline points to view data for specific months. M/M and Y/Y changes are only available for the latest period.
            For detailed series data, use the Series Detail Explorer button above.
          </Typography>
        </CardContent>
      </Card>
    </Box>
  );
}
