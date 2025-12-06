import React from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  TextField,
  Typography,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  Alert,
} from '@mui/material';
import {
  CheckCircle,
  Warning,
  Sync,
  Refresh,
  PlayArrow,
  Explore,
} from '@mui/icons-material';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, ResponsiveContainer } from 'recharts';
import { Link } from 'react-router-dom';
import { freshnessAPI, actionsAPI, quotaAPI } from '../api/client';
import type { SurveyFreshness } from '../api/client';

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

  const diffWeeks = Math.floor(diffDays / 7);
  if (diffWeeks < 4) return `${diffWeeks}w ago`;

  return date.toLocaleDateString();
};

// Survey Card Component
interface SurveyCardProps {
  survey: SurveyFreshness;
  onUpdate: () => void;
  onForceUpdate: () => void;
  onCheckFreshness: () => void;
  isUpdating: boolean;
  isCheckingFreshness: boolean;
}

function SurveyCard({ survey, onUpdate, onForceUpdate, onCheckFreshness, isUpdating, isCheckingFreshness }: SurveyCardProps) {
  const getStatusConfig = (status: string) => {
    switch (status) {
      case 'current':
        return { icon: CheckCircle, label: 'Current', color: 'success.main', bgColor: 'success.light' };
      case 'needs_update':
        return { icon: Warning, label: 'Needs Update', color: 'warning.main', bgColor: 'warning.light' };
      case 'updating':
        return { icon: Sync, label: 'Updating', color: 'info.main', bgColor: 'info.light' };
      default:
        return { icon: CheckCircle, label: 'Unknown', color: 'text.secondary', bgColor: 'grey.200' };
    }
  };

  const statusConfig = getStatusConfig(survey.status);
  const StatusIcon = statusConfig.icon;
  const isUpdatingNow = survey.status === 'updating';

  return (
    <Card
      sx={{
        border: '1px solid',
        borderColor: isUpdatingNow ? statusConfig.color : 'divider',
        boxShadow: 'none',
        height: '100%',
        transition: 'all 0.2s',
        '&:hover': {
          borderColor: statusConfig.color,
          boxShadow: 2,
        }
      }}
    >
      <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
        {/* Header */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1.5 }}>
          <Box sx={{ flex: 1 }}>
            <Link
              to={`/surveys/${survey.survey_code}/explorer`}
              style={{ textDecoration: 'none' }}
            >
              <Typography variant="h6" fontWeight="700" sx={{ color: 'primary.main', mb: 0.5 }}>
                {survey.survey_code}
              </Typography>
            </Link>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', lineHeight: 1.3 }}>
              {survey.survey_name}
            </Typography>
          </Box>
          <Chip
            icon={<StatusIcon sx={{ fontSize: 16 }} />}
            label={statusConfig.label}
            size="small"
            sx={{
              height: 24,
              fontSize: '0.7rem',
              bgcolor: statusConfig.bgColor,
              color: statusConfig.color,
              fontWeight: 600,
              '& .MuiChip-icon': { color: statusConfig.color }
            }}
          />
        </Box>

        {/* Stats Grid */}
        <Box sx={{ mb: 1.5 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
            <Typography variant="caption" color="text.secondary" fontSize="0.7rem">
              Series
            </Typography>
            <Typography variant="body2" fontWeight="600" fontSize="0.85rem">
              {survey.series_total?.toLocaleString() || 0}
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
            <Typography variant="caption" color="text.secondary" fontSize="0.7rem">
              Updated
            </Typography>
            <Typography variant="body2" fontWeight="600" fontSize="0.85rem">
              {survey.series_updated?.toLocaleString() || 0}
            </Typography>
          </Box>
          {(isUpdatingNow || survey.update_progress !== null) && survey.update_progress !== undefined && survey.update_progress > 0 && (
            <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography variant="caption" color="text.secondary" fontSize="0.7rem">
                Progress
              </Typography>
              <Typography variant="body2" fontWeight="600" fontSize="0.85rem" color="info.main">
                {(survey.update_progress * 100).toFixed(0)}%
              </Typography>
            </Box>
          )}
        </Box>

        {/* Timestamps */}
        <Box sx={{ mb: 1.5, pt: 1.5, borderTop: '1px solid', borderColor: 'divider' }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
            <Typography variant="caption" color="text.secondary" fontSize="0.65rem">
              Last Update
            </Typography>
            <Typography variant="caption" fontSize="0.65rem">
              {formatRelativeTime(survey.last_full_update_completed)}
            </Typography>
          </Box>
        </Box>

        {/* Action Buttons */}
        <Box sx={{ display: 'flex', gap: 0.5 }}>
          <Tooltip title="Check if BLS has new data">
            <span style={{ flex: 1 }}>
              <Button
                fullWidth
                size="small"
                variant="outlined"
                color="info"
                startIcon={isCheckingFreshness ? <CircularProgress size={14} color="inherit" /> : <Sync />}
                onClick={onCheckFreshness}
                disabled={isCheckingFreshness || isUpdatingNow}
                sx={{
                  fontSize: '0.7rem',
                  py: 0.5,
                  fontWeight: 500,
                }}
              >
                Check
              </Button>
            </span>
          </Tooltip>
          <Tooltip title="Resume update cycle (skip already-updated series)">
            <span style={{ flex: 1 }}>
              <Button
                fullWidth
                size="small"
                variant="outlined"
                startIcon={isUpdatingNow ? <CircularProgress size={14} color="inherit" /> : <PlayArrow />}
                onClick={onUpdate}
                disabled={isUpdatingNow || isUpdating}
                sx={{
                  fontSize: '0.7rem',
                  py: 0.5,
                  fontWeight: 500,
                }}
              >
                {isUpdatingNow ? 'Updating...' : 'Resume'}
              </Button>
            </span>
          </Tooltip>
          <Tooltip title="Start new update cycle (reset and update all series)">
            <span style={{ flex: 1 }}>
              <Button
                fullWidth
                size="small"
                variant="outlined"
                color="warning"
                startIcon={<Refresh />}
                onClick={onForceUpdate}
                disabled={isUpdatingNow || isUpdating}
                sx={{
                  fontSize: '0.7rem',
                  py: 0.5,
                  fontWeight: 500,
                }}
              >
                Start
              </Button>
            </span>
          </Tooltip>
        </Box>

        {/* Explorer Button */}
        <Button
          component={Link}
          to={`/surveys/${survey.survey_code}/explorer`}
          fullWidth
          size="small"
          variant="contained"
          color="primary"
          startIcon={<Explore />}
          sx={{
            mt: 1.5,
            fontSize: '0.75rem',
            py: 0.75,
            fontWeight: 600,
          }}
        >
          Explore {survey.survey_code} Data
        </Button>
      </CardContent>
    </Card>
  );
}

export default function Dashboard() {
  const queryClient = useQueryClient();
  const [confirmUpdateDialog, setConfirmUpdateDialog] = React.useState<{
    open: boolean;
    surveyCode: string;
    surveyName: string;
    seriesCount: number;
    seriesRemaining: number;
    isForce: boolean;
  }>({
    open: false,
    surveyCode: '',
    surveyName: '',
    seriesCount: 0,
    seriesRemaining: 0,
    isForce: false,
  });
  const [requestsInput, setRequestsInput] = React.useState<string>('');
  const [apiKeyInput, setApiKeyInput] = React.useState<string>('');
  const [userAgentInput, setUserAgentInput] = React.useState<string>('');

  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['freshness', 'overview'],
    queryFn: freshnessAPI.getOverview,
    // Only poll every 10s if there's an active update, otherwise no auto-refresh
    refetchInterval: (query) => {
      const data = query.state.data;
      const hasUpdating = data?.surveys?.some(s => s.status === 'updating') ?? false;
      return hasUpdating ? 10000 : false;
    },
  });

  const { data: quota, isLoading: loadingQuota } = useQuery({
    queryKey: ['quota', 'today'],
    queryFn: () => quotaAPI.getToday(),
  });

  // Fetch remaining quota from actions API (tracks freshness checks too)
  const { data: quotaStatus, refetch: refetchQuotaStatus } = useQuery({
    queryKey: ['actions', 'quota'],
    queryFn: () => actionsAPI.getQuota(),
  });

  const { data: quotaHistory, isLoading: loadingHistory } = useQuery({
    queryKey: ['quota', 'history'],
    queryFn: () => quotaAPI.getHistory(7),
  });

  const checkFreshnessMutation = useMutation({
    mutationFn: (surveyCodes?: string[]) => actionsAPI.checkFreshness(surveyCodes),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['freshness'] });
    },
  });

  const [checkingSurvey, setCheckingSurvey] = React.useState<string | null>(null);

  const checkSingleFreshnessMutation = useMutation({
    mutationFn: (surveyCode: string) => actionsAPI.checkFreshness([surveyCode]),
    onMutate: (surveyCode) => {
      setCheckingSurvey(surveyCode);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['freshness'] });
    },
    onSettled: () => {
      setCheckingSurvey(null);
    },
  });

  const executeUpdateMutation = useMutation({
    mutationFn: ({ surveyCode, force, maxRequests, apiKey, userAgent }: { surveyCode: string; force: boolean; maxRequests: number; apiKey?: string; userAgent?: string }) =>
      actionsAPI.executeUpdate(surveyCode, force, maxRequests, apiKey, userAgent),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['freshness'] });
      queryClient.invalidateQueries({ queryKey: ['actions', 'quota'] });
      setConfirmUpdateDialog({ open: false, surveyCode: '', surveyName: '', seriesCount: 0, seriesRemaining: 0, isForce: false });
      setRequestsInput('');
      setApiKeyInput('');
    },
  });

  const handleUpdateClick = async (survey: SurveyFreshness, isForce: boolean = false) => {
    // Refresh quota status before opening dialog
    const { data: freshQuota } = await refetchQuotaStatus();
    const remaining = freshQuota?.remaining || 0;
    const seriesRemaining = isForce
      ? survey.series_total || 0
      : (survey.series_total || 0) - (survey.series_updated || 0);

    setConfirmUpdateDialog({
      open: true,
      surveyCode: survey.survey_code,
      surveyName: survey.survey_name,
      seriesCount: survey.series_total || 0,
      seriesRemaining,
      isForce,
    });
    // Default to remaining quota or less
    setRequestsInput(String(Math.min(remaining, 500)));
  };

  const handleConfirmUpdate = () => {
    const maxRequests = parseInt(requestsInput, 10);
    if (isNaN(maxRequests) || maxRequests <= 0) {
      return; // Invalid input
    }
    executeUpdateMutation.mutate({
      surveyCode: confirmUpdateDialog.surveyCode,
      force: confirmUpdateDialog.isForce,
      maxRequests,
      apiKey: apiKeyInput.trim() || undefined,
      userAgent: userAgentInput.trim() || undefined,
    });
  };

  const handleCancelUpdate = () => {
    setConfirmUpdateDialog({ open: false, surveyCode: '', surveyName: '', seriesCount: 0, seriesRemaining: 0, isForce: false });
    setRequestsInput('');
    setApiKeyInput('');
  };

  if (loadingOverview || loadingQuota || loadingHistory) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <CircularProgress size={40} />
      </Box>
    );
  }

  const usagePercentage = quota?.percentage_used || 0;
  const remaining = quota?.remaining || 0;

  return (
    <Box sx={{ width: '100%' }}>
      {/* Header */}
      <Box sx={{ mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <Typography variant="h5" fontWeight="700" sx={{ color: 'text.primary', mb: 0.5 }}>
            BLS Data Dashboard
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Survey freshness monitoring and API quota tracking
          </Typography>
        </div>
        <Button
          variant="outlined"
          size="small"
          startIcon={checkFreshnessMutation.isPending ? <CircularProgress size={14} color="inherit" /> : <Refresh />}
          onClick={() => checkFreshnessMutation.mutate()}
          disabled={checkFreshnessMutation.isPending}
          sx={{ borderColor: 'divider', fontSize: '0.875rem', py: 0.75, px: 2 }}
        >
          Check All
        </Button>
      </Box>

      {/* Compact Stats Grid - 6 columns */}
      <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: 2, mb: 3 }}>
        <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
          <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
            <Typography variant="caption" color="text.secondary" fontWeight={500} sx={{ display: 'block', mb: 0.5 }}>
              Total Surveys
            </Typography>
            <Typography variant="h5" fontWeight="700">
              {overview?.total_surveys}
            </Typography>
          </CardContent>
        </Card>

        <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
          <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
            <Typography variant="caption" color="text.secondary" fontWeight={500} sx={{ display: 'block', mb: 0.5 }}>
              Current
            </Typography>
            <Typography variant="h5" fontWeight="700" color="success.main">
              {overview?.surveys_current}
            </Typography>
          </CardContent>
        </Card>

        <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
          <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
            <Typography variant="caption" color="text.secondary" fontWeight={500} sx={{ display: 'block', mb: 0.5 }}>
              Need Update
            </Typography>
            <Typography variant="h5" fontWeight="700" color="warning.main">
              {overview?.surveys_need_update}
            </Typography>
          </CardContent>
        </Card>

        <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
          <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
            <Typography variant="caption" color="text.secondary" fontWeight={500} sx={{ display: 'block', mb: 0.5 }}>
              API Used
            </Typography>
            <Typography variant="h5" fontWeight="700">
              {quota?.used}
            </Typography>
          </CardContent>
        </Card>

        <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
          <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
            <Typography variant="caption" color="text.secondary" fontWeight={500} sx={{ display: 'block', mb: 0.5 }}>
              Remaining
            </Typography>
            <Typography variant="h5" fontWeight="700" color={remaining > 100 ? 'success.main' : 'error.main'}>
              {remaining}
            </Typography>
          </CardContent>
        </Card>

        <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
          <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
            <Typography variant="caption" color="text.secondary" fontWeight={500} sx={{ display: 'block', mb: 0.5 }}>
              Usage %
            </Typography>
            <Typography variant="h5" fontWeight="700" color="warning.main">
              {usagePercentage.toFixed(0)}%
            </Typography>
          </CardContent>
        </Card>
      </Box>

      {/* Survey Cards Grid */}
      <Box sx={{ mb: 3 }}>
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
          BLS Surveys
        </Typography>
        <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2 }}>
          {overview?.surveys.map((survey: SurveyFreshness) => (
            <SurveyCard
              key={survey.survey_code}
              survey={survey}
              onUpdate={() => handleUpdateClick(survey, false)}
              onForceUpdate={() => handleUpdateClick(survey, true)}
              onCheckFreshness={() => checkSingleFreshnessMutation.mutate(survey.survey_code)}
              isUpdating={executeUpdateMutation.isPending}
              isCheckingFreshness={checkingSurvey === survey.survey_code}
            />
          ))}
        </Box>
      </Box>

      {/* 7-Day API Usage Chart - Full Width */}
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
        7-Day API Usage
      </Typography>
      <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
        <Box sx={{ px: 2, py: 1.5 }}>
          <Typography variant="subtitle2" fontWeight="600" color="text.secondary">
            Daily request volume
          </Typography>
        </Box>
        <Box sx={{ p: 2, height: 250 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={quotaHistory}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
              <XAxis dataKey="date" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <RechartsTooltip />
              <Bar dataKey="used" fill="#1976d2" name="Requests" />
            </BarChart>
          </ResponsiveContainer>
        </Box>
      </Card>

      {/* Confirmation Dialog */}
      <Dialog
        open={confirmUpdateDialog.open}
        onClose={handleCancelUpdate}
        aria-labelledby="confirm-update-title"
        aria-describedby="confirm-update-description"
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle id="confirm-update-title">
          {confirmUpdateDialog.isForce ? 'Start New Update Cycle' : 'Resume Update Cycle'}
        </DialogTitle>
        <DialogContent>
          <DialogContentText id="confirm-update-description">
            {confirmUpdateDialog.isForce
              ? 'You are about to start a new update cycle for:'
              : 'You are about to resume the update cycle for:'}
          </DialogContentText>
          <Box sx={{ mt: 2, p: 2, bgcolor: confirmUpdateDialog.isForce ? 'warning.light' : 'grey.50', borderRadius: 1 }}>
            <Typography variant="body2" fontWeight="600" gutterBottom>
              {confirmUpdateDialog.surveyCode} - {confirmUpdateDialog.surveyName}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {confirmUpdateDialog.isForce
                ? <>This will <strong>create a new cycle</strong> and update all <strong>{confirmUpdateDialog.seriesCount.toLocaleString()}</strong> series from scratch</>
                : <>Series remaining: <strong>{confirmUpdateDialog.seriesRemaining.toLocaleString()}</strong> of {confirmUpdateDialog.seriesCount.toLocaleString()}</>}
            </Typography>
          </Box>

          {/* Quota Input Section */}
          <Box sx={{ mt: 3 }}>
            <Alert severity="info" sx={{ mb: 2 }}>
              <Typography variant="body2">
                <strong>Daily quota remaining: {quotaStatus?.remaining ?? '...'}</strong> requests (system key)
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Each request updates up to 50 series. Enter the number of requests for this session.
              </Typography>
            </Alert>

            <TextField
              fullWidth
              label="Requests for this session"
              type="number"
              value={requestsInput}
              onChange={(e) => setRequestsInput(e.target.value)}
              inputProps={{
                min: 1,
                max: apiKeyInput.trim() ? 500 : (quotaStatus?.remaining ?? 500),
              }}
              helperText={(() => {
                const requests = parseInt(requestsInput, 10) || 0;
                const estimatedSeries = requests * 50;
                const remaining = quotaStatus?.remaining ?? 0;
                const usingCustomKey = !!apiKeyInput.trim();
                if (!usingCustomKey && requests > remaining) {
                  return `Exceeds remaining quota (${remaining})`;
                }
                return `Estimated series: ~${estimatedSeries.toLocaleString()} (${requests} requests × 50 series/request)`;
              })()}
              error={!apiKeyInput.trim() && parseInt(requestsInput, 10) > (quotaStatus?.remaining ?? 500)}
              sx={{ mt: 1 }}
            />

            {/* Custom API Key Input */}
            <TextField
              fullWidth
              label="Custom BLS API Key (optional)"
              type="password"
              value={apiKeyInput}
              onChange={(e) => setApiKeyInput(e.target.value)}
              helperText={apiKeyInput.trim()
                ? "Using custom key - quota validation skipped, usage not logged"
                : "Leave empty to use system key with quota tracking"
              }
              sx={{ mt: 2 }}
              size="small"
            />

            {/* Custom User-Agent Input - only show when custom API key is provided */}
            {apiKeyInput.trim() && (
              <TextField
                fullWidth
                label="User-Agent (recommended with custom key)"
                value={userAgentInput}
                onChange={(e) => setUserAgentInput(e.target.value)}
                placeholder="YourApp/1.0 (+contact: your@email.com)"
                helperText="Identifies your application to BLS. Should match your API key registration."
                sx={{ mt: 2 }}
                size="small"
              />
            )}
          </Box>

          <DialogContentText sx={{ mt: 2 }}>
            {confirmUpdateDialog.isForce
              ? <><strong>Warning:</strong> Starting a new cycle will reset progress. Use this only when BLS has published new data.</>
              : <><strong>Note:</strong> Already-updated series in the current cycle will be skipped.</>}
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleCancelUpdate} variant="outlined" color="inherit">
            Cancel
          </Button>
          <Button
            onClick={handleConfirmUpdate}
            variant="contained"
            color={confirmUpdateDialog.isForce ? 'warning' : 'primary'}
            disabled={
              !requestsInput ||
              parseInt(requestsInput, 10) <= 0 ||
              (!apiKeyInput.trim() && parseInt(requestsInput, 10) > (quotaStatus?.remaining ?? 0))
            }
            autoFocus
          >
            {confirmUpdateDialog.isForce ? 'Start New Cycle' : 'Resume Update'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
