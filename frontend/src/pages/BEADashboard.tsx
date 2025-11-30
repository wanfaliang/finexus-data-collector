import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Grid,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  LinearProgress,
  Alert,
  Button,
  Menu,
  MenuItem,
  Divider,
  Snackbar,
} from '@mui/material';
import {
  CheckCircle,
  Warning,
  Sync,
  Storage,
  TableChart,
  Timeline,
  PlayArrow,
  KeyboardArrowDown,
  Refresh,
  Sensors,
  NewReleases,
} from '@mui/icons-material';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { beaDashboardAPI, beaActionsAPI, beaSentinelAPI } from '../api/client';
import type { BEADatasetFreshness, BEACollectionRun, BEASentinelStats } from '../api/client';

// Helper function to format timestamps as relative time
const formatRelativeTime = (dateStr: string | null | undefined): string => {
  if (!dateStr) return 'Never';

  const date = new Date(dateStr);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);

  if (diffMins < 1) return 'Just now';
  if (diffMins < 60) return `${diffMins}m ago`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;

  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) return `${diffDays}d ago`;

  return date.toLocaleDateString();
};

// Format large numbers
const formatNumber = (num: number): string => {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
};

// Format duration
const formatDuration = (seconds: number | null): string => {
  if (!seconds) return '-';
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
};

// Dataset Card Component
function DatasetCard({ dataset }: { dataset: BEADatasetFreshness }) {
  const getStatusConfig = () => {
    if (dataset.update_in_progress) {
      return { icon: Sync, label: 'Updating', color: 'info.main', bgColor: 'info.light' };
    }
    if (dataset.needs_update) {
      return { icon: Warning, label: 'Needs Update', color: 'warning.main', bgColor: 'warning.light' };
    }
    return { icon: CheckCircle, label: 'Current', color: 'success.main', bgColor: 'success.light' };
  };

  const statusConfig = getStatusConfig();
  const StatusIcon = statusConfig.icon;

  const datasetDescriptions: Record<string, string> = {
    'NIPA': 'National Income and Product Accounts - GDP, income, consumption',
    'Regional': 'Regional Economic Accounts - State/county GDP, personal income',
    'GDPbyIndustry': 'GDP by Industry - Value added, contributions by industry sector',
  };

  return (
    <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
      <CardContent>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
          <Box>
            <Typography variant="h6" fontWeight="bold">
              {dataset.dataset_name}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {datasetDescriptions[dataset.dataset_name] || dataset.dataset_name}
            </Typography>
          </Box>
          <Chip
            icon={<StatusIcon sx={{ fontSize: 16 }} />}
            label={statusConfig.label}
            size="small"
            sx={{
              bgcolor: statusConfig.bgColor,
              color: statusConfig.color,
              fontWeight: 'medium',
            }}
          />
        </Box>

        <Grid container spacing={2}>
          <Grid item xs={4}>
            <Box sx={{ textAlign: 'center', p: 1, bgcolor: 'grey.50', borderRadius: 1 }}>
              <TableChart sx={{ color: 'primary.main', mb: 0.5 }} />
              <Typography variant="h6" fontWeight="bold">
                {dataset.tables_count}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Tables
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={4}>
            <Box sx={{ textAlign: 'center', p: 1, bgcolor: 'grey.50', borderRadius: 1 }}>
              <Timeline sx={{ color: 'secondary.main', mb: 0.5 }} />
              <Typography variant="h6" fontWeight="bold">
                {formatNumber(dataset.series_count)}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Series
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={4}>
            <Box sx={{ textAlign: 'center', p: 1, bgcolor: 'grey.50', borderRadius: 1 }}>
              <Storage sx={{ color: 'info.main', mb: 0.5 }} />
              <Typography variant="h6" fontWeight="bold">
                {formatNumber(dataset.data_points_count)}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Data Points
              </Typography>
            </Box>
          </Grid>
        </Grid>

        <Box sx={{ mt: 2, pt: 2, borderTop: '1px solid', borderColor: 'divider' }}>
          <Typography variant="body2" color="text.secondary">
            Latest Data: {dataset.latest_data_year || 'N/A'}
            {dataset.latest_data_period && ` ${dataset.latest_data_period}`}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Last Updated: {formatRelativeTime(dataset.last_update_completed)}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );
}

// Collection Run Row Component
function CollectionRunRow({ run }: { run: BEACollectionRun }) {
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'success';
      case 'running': return 'info';
      case 'queued': return 'default';
      case 'failed': return 'error';
      case 'partial': return 'warning';
      default: return 'default';
    }
  };

  // Format frequency/geo_scope for display
  const formatFrequency = (freq: string | null) => {
    if (!freq) return '';
    switch (freq) {
      case 'A': return 'Annual';
      case 'Q': return 'Quarterly';
      case 'M': return 'Monthly';
      default: return freq;
    }
  };

  // Build parameters display string
  const getParamsDisplay = () => {
    const parts: string[] = [];
    if (run.frequency) parts.push(formatFrequency(run.frequency));
    if (run.geo_scope) parts.push(run.geo_scope);
    if (run.year_spec && run.year_spec !== 'ALL') parts.push(run.year_spec);
    return parts.length > 0 ? parts.join(', ') : '-';
  };

  return (
    <TableRow>
      <TableCell>{run.dataset_name}</TableCell>
      <TableCell>{run.run_type}</TableCell>
      <TableCell>{getParamsDisplay()}</TableCell>
      <TableCell>
        <Chip
          label={run.status}
          size="small"
          color={getStatusColor(run.status)}
          icon={run.status === 'running' ? <Sync sx={{ animation: 'spin 1s linear infinite' }} /> : undefined}
        />
      </TableCell>
      <TableCell>{formatRelativeTime(run.started_at)}</TableCell>
      <TableCell align="right">{run.tables_processed}</TableCell>
      <TableCell align="right">{formatNumber(run.data_points_inserted)}</TableCell>
      <TableCell align="right">{formatDuration(run.duration_seconds)}</TableCell>
    </TableRow>
  );
}

// Action Buttons Component
function ActionButtons() {
  const queryClient = useQueryClient();
  // Backfill menu anchors
  const [nipaAnchor, setNipaAnchor] = useState<null | HTMLElement>(null);
  const [regionalAnchor, setRegionalAnchor] = useState<null | HTMLElement>(null);
  const [gdpbyindustryAnchor, setGdpbyindustryAnchor] = useState<null | HTMLElement>(null);
  // Update menu anchors
  const [nipaUpdateAnchor, setNipaUpdateAnchor] = useState<null | HTMLElement>(null);
  const [regionalUpdateAnchor, setRegionalUpdateAnchor] = useState<null | HTMLElement>(null);
  const [gdpbyindustryUpdateAnchor, setGdpbyindustryUpdateAnchor] = useState<null | HTMLElement>(null);
  const [snackbar, setSnackbar] = useState<{ open: boolean; message: string; severity: 'success' | 'error' }>({
    open: false,
    message: '',
    severity: 'success',
  });

  // Task status query - poll only when a task is running
  const { data: taskStatus } = useQuery({
    queryKey: ['bea-task-status'],
    queryFn: beaActionsAPI.getTaskStatus,
    refetchInterval: (query) => {
      const data = query.state.data;
      // Poll every 3s when task running, don't poll otherwise
      const isRunning = data?.nipa_running || data?.regional_running || data?.gdpbyindustry_running;
      return isRunning ? 3000 : false;
    },
  });

  // Generic success/error handler for mutations
  const handleMutationResult = (data: { success: boolean; message: string }) => {
    setSnackbar({
      open: true,
      message: data.success ? data.message : `Failed: ${data.message}`,
      severity: data.success ? 'success' : 'error',
    });
    queryClient.invalidateQueries({ queryKey: ['bea-task-status'] });
    queryClient.invalidateQueries({ queryKey: ['bea-recent-runs'] });
  };

  const handleMutationError = (error: Error) => {
    setSnackbar({ open: true, message: `Error: ${error.message}`, severity: 'error' });
  };

  // Backfill Mutations
  const nipaBackfillMutation = useMutation({
    mutationFn: beaActionsAPI.backfillNIPA,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  const regionalBackfillMutation = useMutation({
    mutationFn: beaActionsAPI.backfillRegional,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  const gdpbyindustryBackfillMutation = useMutation({
    mutationFn: beaActionsAPI.backfillGDPbyIndustry,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  // Update Mutations
  const nipaUpdateMutation = useMutation({
    mutationFn: beaActionsAPI.updateNIPA,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  const regionalUpdateMutation = useMutation({
    mutationFn: beaActionsAPI.updateRegional,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  const gdpbyindustryUpdateMutation = useMutation({
    mutationFn: beaActionsAPI.updateGDPbyIndustry,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  // Backfill handlers
  const handleNipaBackfill = (frequency: 'A' | 'Q' | 'M', year: string) => {
    setNipaAnchor(null);
    nipaBackfillMutation.mutate({ frequency, year });
  };

  const handleRegionalBackfill = (geo: 'STATE' | 'COUNTY' | 'MSA', year: string) => {
    setRegionalAnchor(null);
    regionalBackfillMutation.mutate({ geo, year });
  };

  const handleGDPbyIndustryBackfill = (frequency: 'A' | 'Q', year: string) => {
    setGdpbyindustryAnchor(null);
    gdpbyindustryBackfillMutation.mutate({ frequency, year });
  };

  // Update handlers
  const handleNipaUpdate = (section: 'priority' | 'gdp' | 'income' | 'govt' | 'trade' | 'investment' | 'all', frequency: 'A' | 'Q' = 'A') => {
    setNipaUpdateAnchor(null);
    nipaUpdateMutation.mutate({ section, frequency, year: 'LAST5' });
  };

  const handleRegionalUpdate = (category: 'priority' | 'state_gdp' | 'state_income' | 'county' | 'quarterly' | 'all') => {
    setRegionalUpdateAnchor(null);
    regionalUpdateMutation.mutate({ category, year: 'LAST5' });
  };

  const handleGDPbyIndustryUpdate = (category: 'priority' | 'value_added' | 'gross_output' | 'inputs' | 'all', frequency: 'A' | 'Q' = 'A') => {
    setGdpbyindustryUpdateAnchor(null);
    gdpbyindustryUpdateMutation.mutate({ category, frequency, year: 'LAST5' });
  };

  const isAnyTaskRunning = taskStatus?.nipa_running || taskStatus?.regional_running || taskStatus?.gdpbyindustry_running;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Backfill Buttons Row */}
      <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap', alignItems: 'center' }}>
        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'bold', minWidth: 60 }}>
          Backfill:
        </Typography>
        {/* NIPA Backfill Button */}
        <Button
          variant="outlined"
          startIcon={<PlayArrow />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setNipaAnchor(e.currentTarget)}
          disabled={taskStatus?.nipa_running}
          size="small"
        >
          {taskStatus?.nipa_running ? 'NIPA Running...' : 'NIPA'}
        </Button>
        <Menu
          anchorEl={nipaAnchor}
          open={Boolean(nipaAnchor)}
          onClose={() => setNipaAnchor(null)}
        >
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Annual (A)
          </MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('A', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('A', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('A', 'LAST5')}>Last 5 Years</MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Quarterly (Q)
          </MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('Q', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('Q', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('Q', 'LAST5')}>Last 5 Years</MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Monthly (M)
          </MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('M', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('M', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleNipaBackfill('M', 'LAST5')}>Last 5 Years</MenuItem>
        </Menu>

        {/* Regional Backfill Button */}
        <Button
          variant="outlined"
          startIcon={<PlayArrow />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setRegionalAnchor(e.currentTarget)}
          disabled={taskStatus?.regional_running}
          size="small"
        >
          {taskStatus?.regional_running ? 'Running...' : 'Regional'}
        </Button>
        <Menu
          anchorEl={regionalAnchor}
          open={Boolean(regionalAnchor)}
          onClose={() => setRegionalAnchor(null)}
        >
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            State Level
          </MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('STATE', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('STATE', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('STATE', 'LAST5')}>Last 5 Years</MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            County Level
          </MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('COUNTY', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('COUNTY', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('COUNTY', 'LAST5')}>Last 5 Years</MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            MSA Level
          </MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('MSA', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('MSA', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleRegionalBackfill('MSA', 'LAST5')}>Last 5 Years</MenuItem>
        </Menu>

        {/* GDP by Industry Backfill Button */}
        <Button
          variant="outlined"
          startIcon={<PlayArrow />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setGdpbyindustryAnchor(e.currentTarget)}
          disabled={taskStatus?.gdpbyindustry_running}
          size="small"
        >
          {taskStatus?.gdpbyindustry_running ? 'Running...' : 'Industry'}
        </Button>
        <Menu
          anchorEl={gdpbyindustryAnchor}
          open={Boolean(gdpbyindustryAnchor)}
          onClose={() => setGdpbyindustryAnchor(null)}
        >
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Annual (A)
          </MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryBackfill('A', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryBackfill('A', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryBackfill('A', 'LAST5')}>Last 5 Years</MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Quarterly (Q)
          </MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryBackfill('Q', 'ALL')}>All Years (from 2005)</MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryBackfill('Q', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryBackfill('Q', 'LAST5')}>Last 5 Years</MenuItem>
        </Menu>
      </Box>

      {/* Update Buttons Row */}
      <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap', alignItems: 'center' }}>
        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'bold', minWidth: 60 }}>
          Update:
        </Typography>
        {/* NIPA Update Button */}
        <Button
          variant="contained"
          color="primary"
          startIcon={<Refresh />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setNipaUpdateAnchor(e.currentTarget)}
          disabled={taskStatus?.nipa_running}
          size="small"
        >
          {taskStatus?.nipa_running ? 'Updating...' : 'NIPA'}
        </Button>
        <Menu
          anchorEl={nipaUpdateAnchor}
          open={Boolean(nipaUpdateAnchor)}
          onClose={() => setNipaUpdateAnchor(null)}
        >
          <MenuItem onClick={() => handleNipaUpdate('priority')}>
            <strong>Priority Tables</strong>&nbsp;(GDP & Income headlines)
          </MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            By Section (Annual)
          </MenuItem>
          <MenuItem onClick={() => handleNipaUpdate('gdp')}>GDP & Output (T1xxxx)</MenuItem>
          <MenuItem onClick={() => handleNipaUpdate('income')}>Personal Income (T2xxxx)</MenuItem>
          <MenuItem onClick={() => handleNipaUpdate('govt')}>Government (T3xxxx)</MenuItem>
          <MenuItem onClick={() => handleNipaUpdate('trade')}>Foreign Trade (T4xxxx)</MenuItem>
          <MenuItem onClick={() => handleNipaUpdate('investment')}>Saving & Investment (T5xxxx)</MenuItem>
          <Divider />
          <MenuItem onClick={() => handleNipaUpdate('all')}>All Tables (Annual)</MenuItem>
          <MenuItem onClick={() => handleNipaUpdate('all', 'Q')}>All Tables (Quarterly)</MenuItem>
        </Menu>

        {/* Regional Update Button */}
        <Button
          variant="contained"
          color="primary"
          startIcon={<Refresh />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setRegionalUpdateAnchor(e.currentTarget)}
          disabled={taskStatus?.regional_running}
          size="small"
        >
          {taskStatus?.regional_running ? 'Updating...' : 'Regional'}
        </Button>
        <Menu
          anchorEl={regionalUpdateAnchor}
          open={Boolean(regionalUpdateAnchor)}
          onClose={() => setRegionalUpdateAnchor(null)}
        >
          <MenuItem onClick={() => handleRegionalUpdate('priority')}>
            <strong>Priority Tables</strong>&nbsp;(State GDP & Income)
          </MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            By Category
          </MenuItem>
          <MenuItem onClick={() => handleRegionalUpdate('state_gdp')}>State GDP (SAGDP*)</MenuItem>
          <MenuItem onClick={() => handleRegionalUpdate('state_income')}>State Income (SAINC*)</MenuItem>
          <MenuItem onClick={() => handleRegionalUpdate('county')}>County Data (CA*)</MenuItem>
          <MenuItem onClick={() => handleRegionalUpdate('quarterly')}>Quarterly State (SQ*)</MenuItem>
          <Divider />
          <MenuItem onClick={() => handleRegionalUpdate('all')}>All Tables</MenuItem>
        </Menu>

        {/* GDP by Industry Update Button */}
        <Button
          variant="contained"
          color="primary"
          startIcon={<Refresh />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setGdpbyindustryUpdateAnchor(e.currentTarget)}
          disabled={taskStatus?.gdpbyindustry_running}
          size="small"
        >
          {taskStatus?.gdpbyindustry_running ? 'Updating...' : 'Industry'}
        </Button>
        <Menu
          anchorEl={gdpbyindustryUpdateAnchor}
          open={Boolean(gdpbyindustryUpdateAnchor)}
          onClose={() => setGdpbyindustryUpdateAnchor(null)}
        >
          <MenuItem onClick={() => handleGDPbyIndustryUpdate('priority')}>
            <strong>Priority Tables</strong>&nbsp;(Value Added, Contributions)
          </MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            By Category (Annual)
          </MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryUpdate('value_added')}>Value Added (Tables 1-14)</MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryUpdate('gross_output')}>Gross Output (Tables 15-19)</MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryUpdate('inputs')}>Input Analysis (Tables 20-42)</MenuItem>
          <Divider />
          <MenuItem onClick={() => handleGDPbyIndustryUpdate('all')}>All Tables (Annual)</MenuItem>
          <MenuItem onClick={() => handleGDPbyIndustryUpdate('all', 'Q')}>All Tables (Quarterly)</MenuItem>
        </Menu>
      </Box>

      {/* Snackbar for notifications */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          severity={snackbar.severity}
          sx={{ width: '100%' }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  );
}

// Sentinel Panel Component
function SentinelPanel() {
  const queryClient = useQueryClient();
  const [snackbar, setSnackbar] = useState<{ open: boolean; message: string; severity: 'success' | 'error' | 'info' }>({
    open: false,
    message: '',
    severity: 'success',
  });

  // Fetch sentinel stats - no polling needed, data only changes on manual operations
  const { data: sentinelStats, isLoading: statsLoading } = useQuery({
    queryKey: ['bea-sentinel-stats'],
    queryFn: beaSentinelAPI.getStats,
  });

  // Select sentinels mutation
  const selectMutation = useMutation({
    mutationFn: ({ dataset, frequency }: { dataset: string; frequency: string }) =>
      beaSentinelAPI.selectSentinels(dataset, frequency),
    onSuccess: (data) => {
      setSnackbar({ open: true, message: data.message, severity: 'success' });
      queryClient.invalidateQueries({ queryKey: ['bea-sentinel-stats'] });
    },
    onError: (error: Error) => {
      setSnackbar({ open: true, message: `Error: ${error.message}`, severity: 'error' });
    },
  });

  // Check sentinels mutation
  const checkMutation = useMutation({
    mutationFn: (dataset: string) => beaSentinelAPI.checkSentinels(dataset),
    onSuccess: (data) => {
      const severity = data.data?.new_data_detected ? 'info' : 'success';
      setSnackbar({ open: true, message: data.message, severity });
      queryClient.invalidateQueries({ queryKey: ['bea-sentinel-stats'] });
      queryClient.invalidateQueries({ queryKey: ['bea-freshness'] });
    },
    onError: (error: Error) => {
      setSnackbar({ open: true, message: `Error: ${error.message}`, severity: 'error' });
    },
  });

  // Check all sentinels mutation
  const checkAllMutation = useMutation({
    mutationFn: beaSentinelAPI.checkAllSentinels,
    onSuccess: (data) => {
      const severity = data.data?.new_data_detected ? 'info' : 'success';
      setSnackbar({ open: true, message: data.message, severity });
      queryClient.invalidateQueries({ queryKey: ['bea-sentinel-stats'] });
      queryClient.invalidateQueries({ queryKey: ['bea-freshness'] });
    },
    onError: (error: Error) => {
      setSnackbar({ open: true, message: `Error: ${error.message}`, severity: 'error' });
    },
  });

  const handleSelectAll = () => {
    // Select sentinels for all datasets
    selectMutation.mutate({ dataset: 'NIPA', frequency: 'A' });
    selectMutation.mutate({ dataset: 'Regional', frequency: 'A' });
    selectMutation.mutate({ dataset: 'GDPbyIndustry', frequency: 'A' });
  };

  const datasets = ['NIPA', 'Regional', 'GDPbyIndustry'];
  const isChecking = checkMutation.isPending || checkAllMutation.isPending;
  const isSelecting = selectMutation.isPending;

  return (
    <Card sx={{ mb: 3 }}>
      <CardContent>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Sensors color="primary" />
            <Typography variant="h6" fontWeight="bold">
              Sentinel Monitoring
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Button
              variant="outlined"
              size="small"
              onClick={handleSelectAll}
              disabled={isSelecting}
              startIcon={isSelecting ? <CircularProgress size={16} /> : <Sensors />}
            >
              {isSelecting ? 'Selecting...' : 'Select Sentinels'}
            </Button>
            <Button
              variant="contained"
              size="small"
              onClick={() => checkAllMutation.mutate()}
              disabled={isChecking || (sentinelStats?.total || 0) === 0}
              startIcon={isChecking ? <CircularProgress size={16} color="inherit" /> : <Refresh />}
            >
              {isChecking ? 'Checking...' : 'Check for Updates'}
            </Button>
          </Box>
        </Box>

        {statsLoading ? (
          <LinearProgress />
        ) : (sentinelStats?.total || 0) === 0 ? (
          <Alert severity="info" sx={{ mt: 1 }}>
            No sentinels configured. Click "Select Sentinels" to automatically choose representative series for monitoring.
          </Alert>
        ) : (
          <Grid container spacing={2}>
            {datasets.map((dataset) => {
              const datasetStats = sentinelStats?.by_dataset?.[dataset];
              if (!datasetStats) return null;

              return (
                <Grid item xs={12} md={4} key={dataset}>
                  <Paper variant="outlined" sx={{ p: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                      <Typography variant="subtitle2" fontWeight="bold">
                        {dataset}
                      </Typography>
                      <Chip
                        label={`${datasetStats.count} sentinels`}
                        size="small"
                        color="primary"
                        variant="outlined"
                      />
                    </Box>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                      <Typography variant="body2" color="text.secondary">
                        Last Checked: {datasetStats.last_checked ? formatRelativeTime(datasetStats.last_checked) : 'Never'}
                      </Typography>
                      {datasetStats.changes_detected > 0 && (
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
                          <NewReleases color="warning" sx={{ fontSize: 16 }} />
                          <Typography variant="body2" color="warning.main">
                            {datasetStats.changes_detected} changes detected
                          </Typography>
                        </Box>
                      )}
                    </Box>
                    <Box sx={{ mt: 1.5, display: 'flex', gap: 1 }}>
                      <Button
                        size="small"
                        variant="text"
                        onClick={() => selectMutation.mutate({ dataset, frequency: 'A' })}
                        disabled={isSelecting}
                      >
                        Reselect
                      </Button>
                      <Button
                        size="small"
                        variant="text"
                        onClick={() => checkMutation.mutate(dataset)}
                        disabled={isChecking}
                      >
                        Check
                      </Button>
                    </Box>
                  </Paper>
                </Grid>
              );
            })}
          </Grid>
        )}

        <Snackbar
          open={snackbar.open}
          autoHideDuration={6000}
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        >
          <Alert
            onClose={() => setSnackbar({ ...snackbar, open: false })}
            severity={snackbar.severity}
            sx={{ width: '100%' }}
          >
            {snackbar.message}
          </Alert>
        </Snackbar>
      </CardContent>
    </Card>
  );
}

// Main Dashboard Component
export default function BEADashboard() {
  const queryClient = useQueryClient();

  // Fetch data - no polling, only on load or after task completion
  const { data: freshnessData, isLoading: freshnessLoading, error: freshnessError } = useQuery({
    queryKey: ['bea-freshness'],
    queryFn: beaDashboardAPI.getFreshnessOverview,
  });

  const { data: usageData } = useQuery({
    queryKey: ['bea-usage-today'],
    queryFn: beaDashboardAPI.getUsageToday,
    refetchInterval: 60000,
  });

  const { data: usageHistory } = useQuery({
    queryKey: ['bea-usage-history'],
    queryFn: () => beaDashboardAPI.getUsageHistory(7),
  });

  const { data: recentRuns, isLoading: runsLoading } = useQuery({
    queryKey: ['bea-recent-runs'],
    queryFn: () => beaDashboardAPI.getRecentRuns(10),
    refetchInterval: () => {
      // Only poll when a task is running, otherwise don't poll
      // Get task status from query cache
      const status = queryClient.getQueryData<{ nipa_running: boolean; regional_running: boolean; gdpbyindustry_running: boolean }>(['bea-task-status']);
      const isRunning = status?.nipa_running || status?.regional_running || status?.gdpbyindustry_running;
      return isRunning ? 5000 : false;
    },
  });

  const { data: stats } = useQuery({
    queryKey: ['bea-stats'],
    queryFn: beaDashboardAPI.getStatsSummary,
  });

  if (freshnessLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (freshnessError) {
    return (
      <Alert severity="error" sx={{ m: 2 }}>
        Failed to load BEA dashboard data. Make sure the API is running.
      </Alert>
    );
  }

  return (
    <Box sx={{ p: 3 }}>
      {/* Header */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h4" fontWeight="bold" gutterBottom>
          BEA Data Dashboard
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Bureau of Economic Analysis - NIPA, Regional, and GDP by Industry Data
        </Typography>
      </Box>

      {/* Summary Cards */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} md={3}>
          <Card sx={{ bgcolor: 'primary.light', color: 'primary.contrastText' }}>
            <CardContent>
              <Typography variant="h3" fontWeight="bold">
                {formatNumber(stats?.total_data_points || freshnessData?.total_data_points || 0)}
              </Typography>
              <Typography variant="body2">Total Data Points</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography variant="h3" fontWeight="bold" color="success.main">
                {freshnessData?.datasets_current || 0}
              </Typography>
              <Typography variant="body2" color="text.secondary">Datasets Current</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography variant="h3" fontWeight="bold" color="warning.main">
                {freshnessData?.datasets_need_update || 0}
              </Typography>
              <Typography variant="body2" color="text.secondary">Need Update</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography variant="h3" fontWeight="bold" color="info.main">
                {usageData?.requests_remaining || 100}/100
              </Typography>
              <Typography variant="body2" color="text.secondary">API Requests Left (this min)</Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Action Buttons */}
      <Card sx={{ mb: 3, p: 2 }}>
        <ActionButtons />
      </Card>

      {/* Dataset Cards */}
      <Typography variant="h6" fontWeight="bold" sx={{ mb: 2 }}>
        Datasets
      </Typography>
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {freshnessData?.datasets.map((dataset) => (
          <Grid item xs={12} md={6} key={dataset.dataset_name}>
            <DatasetCard dataset={dataset} />
          </Grid>
        ))}
      </Grid>

      {/* Sentinel Monitoring Panel */}
      <SentinelPanel />

      {/* API Usage Chart */}
      {usageHistory && usageHistory.length > 0 && (
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" fontWeight="bold" gutterBottom>
              API Usage (Last 7 Days)
            </Typography>
            <Box sx={{ height: 200 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={[...usageHistory].reverse()}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="date"
                    tickFormatter={(d) => new Date(d).toLocaleDateString('en-US', { weekday: 'short' })}
                  />
                  <YAxis />
                  <Tooltip
                    labelFormatter={(d) => new Date(d).toLocaleDateString()}
                    formatter={(value: number) => [value, 'Requests']}
                  />
                  <Bar dataKey="total_requests" fill="#1976d2" name="Requests" />
                </BarChart>
              </ResponsiveContainer>
            </Box>
          </CardContent>
        </Card>
      )}

      {/* Recent Collection Runs */}
      <Card>
        <CardContent>
          <Typography variant="h6" fontWeight="bold" gutterBottom>
            Recent Collection Runs
          </Typography>
          {runsLoading ? (
            <LinearProgress />
          ) : recentRuns && recentRuns.length > 0 ? (
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Dataset</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Parameters</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Started</TableCell>
                    <TableCell align="right">Tables</TableCell>
                    <TableCell align="right">Data Points</TableCell>
                    <TableCell align="right">Duration</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {recentRuns.map((run) => (
                    <CollectionRunRow key={run.run_id} run={run} />
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          ) : (
            <Typography variant="body2" color="text.secondary">
              No collection runs yet. Use the action buttons above to start a backfill.
            </Typography>
          )}
        </CardContent>
      </Card>

      {/* CSS for spinning animation */}
      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </Box>
  );
}
