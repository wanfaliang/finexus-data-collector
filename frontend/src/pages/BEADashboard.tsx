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
  OpenInNew,
} from '@mui/icons-material';
import { Link } from 'react-router-dom';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { beaDashboardAPI, beaActionsAPI } from '../api/client';
import type { BEADatasetFreshness, BEACollectionRun } from '../api/client';

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

// Dataset info with descriptions and update strategies
const datasetInfo: Record<string, {
  description: string;
  frequencies: string;
  updateSchedule: string;
  keyTables: string;
  explorerPath: string;
}> = {
  'NIPA': {
    description: 'National Income and Product Accounts - GDP, income, consumption',
    frequencies: 'A, Q, M',
    updateSchedule: 'Monthly (M), Quarterly (Q), Annually (A)',
    keyTables: 'T10101 (GDP), T20100 (Personal Income)',
    explorerPath: '/bea/nipa',
  },
  'Regional': {
    description: 'Regional Economic Accounts - State/county GDP, personal income',
    frequencies: 'A, Q',
    updateSchedule: 'Annually or after BEA revisions',
    keyTables: 'SAGDP1 (State GDP), SAINC1 (State Income)',
    explorerPath: '/bea/regional',
  },
  'GDPbyIndustry': {
    description: 'GDP by Industry - Value added, contributions by industry sector',
    frequencies: 'A, Q',
    updateSchedule: 'Quarterly (late Jan/Apr/Jul/Oct)',
    keyTables: 'Table 1 (Value Added), Table 5 (Contributions)',
    explorerPath: '/bea/gdpbyindustry',
  },
  'ITA': {
    description: 'International Transactions - Trade balance, exports, imports',
    frequencies: 'A, QSA, QNSA',
    updateSchedule: 'Quarterly (SA/NSA)',
    keyTables: 'BalGds, BalServ, BalCAcc (Trade Balances)',
    explorerPath: '/bea/ita',
  },
  'FixedAssets': {
    description: 'Fixed Assets - Current-cost stocks, depreciation, investment',
    frequencies: 'A only',
    updateSchedule: 'Annually or after BEA revisions',
    keyTables: 'FAAt101 (Net Stock), FAAt103 (Depreciation)',
    explorerPath: '/bea/fixedassets',
  },
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
  const info = datasetInfo[dataset.dataset_name];

  return (
    <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
      <CardContent sx={{ pb: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1.5 }}>
          <Box>
            <Typography variant="h6" fontWeight="bold">
              {dataset.dataset_name}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.8rem' }}>
              {info?.description || dataset.dataset_name}
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

        <Grid container spacing={1.5} sx={{ mb: 1.5 }}>
          <Grid item xs={4}>
            <Box sx={{ textAlign: 'center', p: 0.75, bgcolor: 'grey.50', borderRadius: 1 }}>
              <TableChart sx={{ color: 'primary.main', fontSize: 20 }} />
              <Typography variant="subtitle2" fontWeight="bold">
                {dataset.tables_count}
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.65rem' }}>
                Tables
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={4}>
            <Box sx={{ textAlign: 'center', p: 0.75, bgcolor: 'grey.50', borderRadius: 1 }}>
              <Timeline sx={{ color: 'secondary.main', fontSize: 20 }} />
              <Typography variant="subtitle2" fontWeight="bold">
                {formatNumber(dataset.series_count)}
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.65rem' }}>
                Series
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={4}>
            <Box sx={{ textAlign: 'center', p: 0.75, bgcolor: 'grey.50', borderRadius: 1 }}>
              <Storage sx={{ color: 'info.main', fontSize: 20 }} />
              <Typography variant="subtitle2" fontWeight="bold">
                {formatNumber(dataset.data_points_count)}
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.65rem' }}>
                Data Points
              </Typography>
            </Box>
          </Grid>
        </Grid>

        <Box sx={{ pt: 1.5, borderTop: '1px solid', borderColor: 'divider' }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
            <Typography variant="caption" color="text.secondary">Latest Data:</Typography>
            <Typography variant="caption" fontWeight="medium">
              {dataset.latest_data_year || 'N/A'}{dataset.latest_data_period && ` ${dataset.latest_data_period}`}
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
            <Typography variant="caption" color="text.secondary">Last Updated:</Typography>
            <Typography variant="caption" fontWeight="medium">
              {formatRelativeTime(dataset.last_update_completed)}
            </Typography>
          </Box>
        </Box>

        {info && (
          <Box sx={{ mt: 1.5, pt: 1.5, borderTop: '1px dashed', borderColor: 'divider', bgcolor: 'action.hover', mx: -2, px: 2, pb: 0.5, mb: -1 }}>
            <Typography variant="caption" fontWeight="bold" color="primary.main" sx={{ display: 'block', mb: 0.5 }}>
              Update Strategy
            </Typography>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.25 }}>
              <Typography variant="caption" color="text.secondary">Frequencies:</Typography>
              <Typography variant="caption" fontWeight="medium">{info.frequencies}</Typography>
            </Box>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.25 }}>
              <Typography variant="caption" color="text.secondary">Schedule:</Typography>
              <Typography variant="caption" fontWeight="medium" sx={{ textAlign: 'right', maxWidth: '60%' }}>{info.updateSchedule}</Typography>
            </Box>
            <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography variant="caption" color="text.secondary">Key Tables:</Typography>
              <Typography variant="caption" fontWeight="medium" sx={{ textAlign: 'right', maxWidth: '65%', fontSize: '0.65rem' }}>{info.keyTables}</Typography>
            </Box>
          </Box>
        )}

        {/* Explore Button */}
        {info?.explorerPath && (
          <Box sx={{ mt: 2, pt: 1.5, borderTop: '1px solid', borderColor: 'divider' }}>
            <Button
              component={Link}
              to={info.explorerPath}
              variant="outlined"
              size="small"
              endIcon={<OpenInNew sx={{ fontSize: 16 }} />}
              fullWidth
              sx={{
                textTransform: 'none',
                fontWeight: 600,
                borderColor: 'primary.main',
                color: 'primary.main',
                '&:hover': {
                  bgcolor: 'primary.main',
                  color: 'white',
                  borderColor: 'primary.main',
                },
              }}
            >
              Explore Data
            </Button>
          </Box>
        )}
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
      case 'QSA': return 'Qtr (SA)';
      case 'QNSA': return 'Qtr (NSA)';
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
  const [itaAnchor, setItaAnchor] = useState<null | HTMLElement>(null);
  const [fixedassetsAnchor, setFixedassetsAnchor] = useState<null | HTMLElement>(null);
  // Update menu anchors
  const [nipaUpdateAnchor, setNipaUpdateAnchor] = useState<null | HTMLElement>(null);
  const [regionalUpdateAnchor, setRegionalUpdateAnchor] = useState<null | HTMLElement>(null);
  const [gdpbyindustryUpdateAnchor, setGdpbyindustryUpdateAnchor] = useState<null | HTMLElement>(null);
  const [itaUpdateAnchor, setItaUpdateAnchor] = useState<null | HTMLElement>(null);
  const [fixedassetsUpdateAnchor, setFixedassetsUpdateAnchor] = useState<null | HTMLElement>(null);
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
      const isRunning = data?.nipa_running || data?.regional_running || data?.gdpbyindustry_running || data?.ita_running || data?.fixedassets_running;
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

  // ITA Mutations
  const itaBackfillMutation = useMutation({
    mutationFn: beaActionsAPI.backfillITA,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  const itaUpdateMutation = useMutation({
    mutationFn: beaActionsAPI.updateITA,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  // FixedAssets Mutations
  const fixedassetsBackfillMutation = useMutation({
    mutationFn: beaActionsAPI.backfillFixedAssets,
    onSuccess: handleMutationResult,
    onError: handleMutationError,
  });

  const fixedassetsUpdateMutation = useMutation({
    mutationFn: beaActionsAPI.updateFixedAssets,
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

  // ITA handlers
  const handleItaBackfill = (frequency: 'A' | 'QSA' | 'QNSA', year: string) => {
    setItaAnchor(null);
    itaBackfillMutation.mutate({ frequency, year });
  };

  const handleItaUpdate = (category: 'priority' | 'goods' | 'services' | 'income' | 'current_account' | 'all', frequency: 'A' | 'QSA' | 'QNSA' = 'A') => {
    setItaUpdateAnchor(null);
    itaUpdateMutation.mutate({ category, frequency, year: 'LAST5' });
  };

  // FixedAssets handlers
  const handleFixedassetsBackfill = (year: string) => {
    setFixedassetsAnchor(null);
    fixedassetsBackfillMutation.mutate({ year });
  };

  const handleFixedassetsUpdate = (category: 'priority' | 'all', year: string = 'LAST5') => {
    setFixedassetsUpdateAnchor(null);
    fixedassetsUpdateMutation.mutate({ year });
  };

  const isAnyTaskRunning = taskStatus?.nipa_running || taskStatus?.regional_running || taskStatus?.gdpbyindustry_running || taskStatus?.ita_running || taskStatus?.fixedassets_running;

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

        {/* ITA Backfill Button */}
        <Button
          variant="outlined"
          startIcon={<PlayArrow />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setItaAnchor(e.currentTarget)}
          disabled={taskStatus?.ita_running}
          size="small"
        >
          {taskStatus?.ita_running ? 'Running...' : 'Int\'l Trade'}
        </Button>
        <Menu
          anchorEl={itaAnchor}
          open={Boolean(itaAnchor)}
          onClose={() => setItaAnchor(null)}
        >
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Annual (A)
          </MenuItem>
          <MenuItem onClick={() => handleItaBackfill('A', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleItaBackfill('A', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleItaBackfill('A', 'LAST5')}>Last 5 Years</MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Quarterly (SA)
          </MenuItem>
          <MenuItem onClick={() => handleItaBackfill('QSA', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleItaBackfill('QSA', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleItaBackfill('QSA', 'LAST5')}>Last 5 Years</MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Quarterly (NSA)
          </MenuItem>
          <MenuItem onClick={() => handleItaBackfill('QNSA', 'ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleItaBackfill('QNSA', 'LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleItaBackfill('QNSA', 'LAST5')}>Last 5 Years</MenuItem>
        </Menu>

        {/* FixedAssets Backfill Button */}
        <Button
          variant="outlined"
          startIcon={<PlayArrow />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setFixedassetsAnchor(e.currentTarget)}
          disabled={taskStatus?.fixedassets_running}
          size="small"
        >
          {taskStatus?.fixedassets_running ? 'Running...' : 'Fixed Assets'}
        </Button>
        <Menu
          anchorEl={fixedassetsAnchor}
          open={Boolean(fixedassetsAnchor)}
          onClose={() => setFixedassetsAnchor(null)}
        >
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            Annual Only (A)
          </MenuItem>
          <MenuItem onClick={() => handleFixedassetsBackfill('ALL')}>All Years</MenuItem>
          <MenuItem onClick={() => handleFixedassetsBackfill('LAST10')}>Last 10 Years</MenuItem>
          <MenuItem onClick={() => handleFixedassetsBackfill('LAST5')}>Last 5 Years</MenuItem>
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
          <MenuItem onClick={() => handleNipaUpdate('all', 'M')}>All Tables (Monthly)</MenuItem>
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

        {/* ITA Update Button */}
        <Button
          variant="contained"
          color="primary"
          startIcon={<Refresh />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setItaUpdateAnchor(e.currentTarget)}
          disabled={taskStatus?.ita_running}
          size="small"
        >
          {taskStatus?.ita_running ? 'Updating...' : 'Int\'l Trade'}
        </Button>
        <Menu
          anchorEl={itaUpdateAnchor}
          open={Boolean(itaUpdateAnchor)}
          onClose={() => setItaUpdateAnchor(null)}
        >
          <MenuItem onClick={() => handleItaUpdate('priority')}>
            <strong>Priority</strong>&nbsp;(Trade Balances)
          </MenuItem>
          <Divider />
          <MenuItem disabled sx={{ opacity: 1, fontWeight: 'bold', fontSize: '0.75rem' }}>
            By Category (Annual)
          </MenuItem>
          <MenuItem onClick={() => handleItaUpdate('goods')}>Goods Trade</MenuItem>
          <MenuItem onClick={() => handleItaUpdate('services')}>Services Trade</MenuItem>
          <MenuItem onClick={() => handleItaUpdate('income')}>Investment Income</MenuItem>
          <MenuItem onClick={() => handleItaUpdate('current_account')}>Current Account</MenuItem>
          <Divider />
          <MenuItem onClick={() => handleItaUpdate('all')}>All Indicators (Annual)</MenuItem>
          <MenuItem onClick={() => handleItaUpdate('all', 'QSA')}>All Indicators (Quarterly SA)</MenuItem>
        </Menu>

        {/* FixedAssets Update Button */}
        <Button
          variant="contained"
          color="primary"
          startIcon={<Refresh />}
          endIcon={<KeyboardArrowDown />}
          onClick={(e) => setFixedassetsUpdateAnchor(e.currentTarget)}
          disabled={taskStatus?.fixedassets_running}
          size="small"
        >
          {taskStatus?.fixedassets_running ? 'Updating...' : 'Fixed Assets'}
        </Button>
        <Menu
          anchorEl={fixedassetsUpdateAnchor}
          open={Boolean(fixedassetsUpdateAnchor)}
          onClose={() => setFixedassetsUpdateAnchor(null)}
        >
          <MenuItem onClick={() => handleFixedassetsUpdate('all', 'LAST5')}>
            <strong>All Tables</strong>&nbsp;(Last 5 Years)
          </MenuItem>
          <Divider />
          <MenuItem onClick={() => handleFixedassetsUpdate('all', 'LAST10')}>All Tables (Last 10 Years)</MenuItem>
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
    queryFn: () => beaDashboardAPI.getRecentRuns(50),
    refetchInterval: () => {
      // Only poll when a task is running, otherwise don't poll
      // Get task status from query cache
      const status = queryClient.getQueryData<{ nipa_running: boolean; regional_running: boolean; gdpbyindustry_running: boolean; ita_running: boolean; fixedassets_running: boolean }>(['bea-task-status']);
      const isRunning = status?.nipa_running || status?.regional_running || status?.gdpbyindustry_running || status?.ita_running || status?.fixedassets_running;
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
          Bureau of Economic Analysis - NIPA, Regional, GDP by Industry, International Transactions, and Fixed Assets
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
        Datasets
      </Typography>
      <Box
        sx={{
          display: 'grid',
          gridTemplateColumns: {
            xs: '1fr',
            sm: 'repeat(2, 1fr)',
            md: 'repeat(3, 1fr)',
            lg: 'repeat(5, 1fr)',
          },
          gap: 2,
          mb: 3,
        }}
      >
        {freshnessData?.datasets.map((dataset) => (
          <DatasetCard key={dataset.dataset_name} dataset={dataset} />
        ))}
      </Box>

      {/* API Usage Chart */}
      {usageHistory && usageHistory.length > 0 && (
        <>
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
          API Usage (Last 7 Days)
        </Typography>
        <Card sx={{ mb: 3 }}>
          <CardContent>
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
        </>
      )}

      {/* Recent Collection Runs */}
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
        Recent Collection Runs
      </Typography>
      <Card>
        <CardContent>
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
