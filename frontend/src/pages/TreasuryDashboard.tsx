import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Alert,
  Snackbar,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Grid,
  Divider,
} from '@mui/material';
import {
  PlayArrow,
  Refresh,
  CheckCircle,
  Explore,
  AccountBalance,
  EventNote,
  Storage,
  Timeline,
} from '@mui/icons-material';
import { Link } from 'react-router-dom';
import { treasuryAPI } from '../api/client';

// Stat card component
function StatCard({
  icon: Icon,
  title,
  value,
  subtitle,
  color = '#667eea',
}: {
  icon: React.ElementType;
  title: string;
  value: string | number;
  subtitle?: string;
  color?: string;
}) {
  return (
    <Card
      sx={{
        height: '100%',
        borderTop: `4px solid ${color}`,
        transition: 'transform 0.2s ease, box-shadow 0.2s ease',
        '&:hover': {
          transform: 'translateY(-2px)',
          boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
        },
      }}
    >
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
          <Icon sx={{ color, fontSize: 24 }} />
          <Typography variant="body2" color="text.secondary" fontWeight={500}>
            {title}
          </Typography>
        </Box>
        <Typography variant="h4" fontWeight="bold" sx={{ color }}>
          {value}
        </Typography>
        {subtitle && (
          <Typography variant="caption" color="text.secondary">
            {subtitle}
          </Typography>
        )}
      </CardContent>
    </Card>
  );
}

export default function TreasuryDashboard() {
  const queryClient = useQueryClient();
  const [snackbar, setSnackbar] = useState<{ open: boolean; message: string; severity: 'success' | 'error' }>({
    open: false,
    message: '',
    severity: 'success',
  });

  // Track if any task is running
  const [isTaskRunning, setIsTaskRunning] = useState(false);

  const { data: stats, isLoading: statsLoading } = useQuery({
    queryKey: ['treasury-stats'],
    queryFn: treasuryAPI.getStats,
  });

  const { data: recentRuns, isLoading: runsLoading } = useQuery({
    queryKey: ['treasury-recent-runs'],
    queryFn: () => treasuryAPI.getRecentRuns(10),
    refetchInterval: isTaskRunning ? 3000 : false,
  });

  // Check if any runs are in "running" status and update polling state
  useEffect(() => {
    const hasRunning = recentRuns?.some((run) => run.status === 'running') ?? false;
    setIsTaskRunning(hasRunning);
  }, [recentRuns]);

  const { data: upcomingAuctions } = useQuery({
    queryKey: ['treasury-upcoming'],
    queryFn: () => treasuryAPI.getUpcomingAuctions(),
  });

  // Mutations
  const backfillMutation = useMutation({
    mutationFn: async (years: number) => {
      const result = await treasuryAPI.backfillAuctions(years);
      return result;
    },
    onSuccess: (data) => {
      setSnackbar({ open: true, message: data.message, severity: data.success ? 'success' : 'error' });
      queryClient.invalidateQueries({ queryKey: ['treasury-recent-runs'] });
    },
    onError: (error: Error) => {
      setSnackbar({ open: true, message: `Error: ${error.message}`, severity: 'error' });
    },
  });

  const refreshUpcomingMutation = useMutation({
    mutationFn: () => treasuryAPI.refreshUpcoming(),
    onSuccess: (data) => {
      setSnackbar({ open: true, message: data.message, severity: data.success ? 'success' : 'error' });
      queryClient.invalidateQueries({ queryKey: ['treasury-upcoming'] });
      queryClient.invalidateQueries({ queryKey: ['treasury-recent-runs'] });
    },
    onError: (error: Error) => {
      setSnackbar({ open: true, message: `Error: ${error.message}`, severity: 'error' });
    },
  });

  const handleBackfillAll = () => {
    backfillMutation.mutate(20);
    refreshUpcomingMutation.mutate();
  };

  if (statsLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  // Format date range
  const dateRange = stats?.earliest_auction && stats?.latest_auction
    ? `${stats.earliest_auction} - ${stats.latest_auction}`
    : 'No data yet';

  return (
    <Box sx={{ p: 3 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4" fontWeight="bold">
            Treasury Auctions Dashboard
          </Typography>
          <Typography variant="body1" color="text.secondary">
            U.S. Treasury Notes & Bonds auction data collection
          </Typography>
        </Box>
        <Button
          component={Link}
          to="/treasury/explorer"
          variant="contained"
          startIcon={<Explore />}
          sx={{
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            '&:hover': {
              background: 'linear-gradient(135deg, #5a6fd6 0%, #6a4190 100%)',
            },
          }}
        >
          Open Explorer
        </Button>
      </Box>

      {/* Stats Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            icon={AccountBalance}
            title="Total Auctions"
            value={stats?.total_auctions?.toLocaleString() || 0}
            subtitle="Historical auction records"
            color="#667eea"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            icon={EventNote}
            title="Upcoming Auctions"
            value={stats?.total_upcoming_auctions || 0}
            subtitle="Scheduled auctions"
            color="#f5576c"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            icon={Timeline}
            title="Date Range"
            value={stats?.total_auctions ? `${Math.round((new Date(stats.latest_auction!).getTime() - new Date(stats.earliest_auction!).getTime()) / (365.25 * 24 * 60 * 60 * 1000))} yrs` : '0 yrs'}
            subtitle={dateRange}
            color="#43e97b"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <StatCard
            icon={Storage}
            title="Daily Rates"
            value={stats?.total_daily_rates || 0}
            subtitle="Yield curve records"
            color="#4facfe"
          />
        </Grid>
      </Grid>

      {/* Action Buttons */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" fontWeight="bold" gutterBottom>
            Data Collection Actions
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
            <Button
              variant="contained"
              color="success"
              startIcon={<PlayArrow />}
              onClick={handleBackfillAll}
              disabled={backfillMutation.isPending || refreshUpcomingMutation.isPending || isTaskRunning}
              sx={{ px: 3 }}
            >
              {backfillMutation.isPending || isTaskRunning ? 'Running...' : 'Backfill All Data (20 years)'}
            </Button>

            <Button
              variant="contained"
              color="primary"
              startIcon={<Refresh />}
              onClick={() => refreshUpcomingMutation.mutate()}
              disabled={refreshUpcomingMutation.isPending || isTaskRunning}
            >
              {refreshUpcomingMutation.isPending ? 'Refreshing...' : 'Refresh Upcoming'}
            </Button>
          </Box>
        </CardContent>
      </Card>

      {/* Upcoming Auctions */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" fontWeight="bold" gutterBottom>
            Upcoming Auctions
          </Typography>
          {upcomingAuctions && upcomingAuctions.length > 0 ? (
            <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap' }}>
              {upcomingAuctions.map((auction, idx) => (
                <Chip
                  key={idx}
                  label={`${auction.security_term} - ${auction.auction_date}`}
                  sx={{
                    fontWeight: 500,
                    bgcolor: auction.security_term.includes('10') ? '#f5576c' :
                             auction.security_term.includes('30') ? '#a18cd1' :
                             auction.security_term.includes('2') ? '#667eea' :
                             auction.security_term.includes('5') ? '#4facfe' :
                             auction.security_term.includes('7') ? '#43e97b' :
                             auction.security_term.includes('20') ? '#fa709a' : '#667eea',
                    color: 'white',
                    '& .MuiChip-label': { px: 1.5 },
                  }}
                />
              ))}
            </Box>
          ) : (
            <Typography color="text.secondary">No upcoming auctions scheduled</Typography>
          )}
        </CardContent>
      </Card>

      {/* Recent Runs */}
      <Card>
        <CardContent>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography variant="h6" fontWeight="bold">
              Recent Collection Runs
            </Typography>
            {isTaskRunning && (
              <Chip
                label="Task Running"
                color="warning"
                size="small"
                icon={<CircularProgress size={14} color="inherit" />}
              />
            )}
          </Box>
          {runsLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress size={24} />
            </Box>
          ) : recentRuns && recentRuns.length > 0 ? (
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ bgcolor: 'grey.50' }}>
                    <TableCell sx={{ fontWeight: 'bold' }}>Type</TableCell>
                    <TableCell sx={{ fontWeight: 'bold' }}>Status</TableCell>
                    <TableCell sx={{ fontWeight: 'bold' }}>Started</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 'bold' }}>Records</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 'bold' }}>Duration</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {recentRuns.map((run, idx) => (
                    <TableRow
                      key={run.run_id}
                      sx={{ bgcolor: idx % 2 === 0 ? 'transparent' : 'grey.50' }}
                    >
                      <TableCell>
                        <Typography variant="body2" fontWeight="medium">
                          {run.collection_type}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {run.run_type}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={run.status}
                          size="small"
                          color={run.status === 'completed' ? 'success' : run.status === 'running' ? 'warning' : 'error'}
                          icon={run.status === 'completed' ? <CheckCircle /> : undefined}
                          sx={{ fontWeight: 500 }}
                        />
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2">
                          {new Date(run.started_at).toLocaleDateString()}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {new Date(run.started_at).toLocaleTimeString()}
                        </Typography>
                      </TableCell>
                      <TableCell align="right">
                        <Typography variant="body2" fontWeight="medium">
                          {run.records_inserted || 0}
                        </Typography>
                      </TableCell>
                      <TableCell align="right">
                        <Typography variant="body2" color="text.secondary">
                          {run.duration_seconds ? `${run.duration_seconds.toFixed(1)}s` : '-'}
                        </Typography>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          ) : (
            <Box sx={{ py: 4, textAlign: 'center' }}>
              <Typography color="text.secondary">No collection runs yet</Typography>
              <Typography variant="caption" color="text.secondary">
                Click "Backfill All Data" to start collecting auction data
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Snackbar */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          severity={snackbar.severity}
          variant="filled"
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  );
}
