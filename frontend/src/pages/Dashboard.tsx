import React from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  CircularProgress,

  Typography,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
} from '@mui/material';
import {
  CheckCircle,
  Warning,
  Sync,
  Refresh,
  PlayArrow,
  
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
  isUpdating: boolean;
}

function SurveyCard({ survey, onUpdate, isUpdating }: SurveyCardProps) {
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
              Sentinels
            </Typography>
            <Typography
              variant="body2"
              fontWeight="600"
              fontSize="0.85rem"
              color={survey.sentinels_changed > 0 ? 'error' : 'text.primary'}
            >
              {survey.sentinels_changed} / {survey.sentinels_total}
            </Typography>
          </Box>
          {isUpdatingNow && survey.update_progress !== null && (
            <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography variant="caption" color="text.secondary" fontSize="0.7rem">
                Progress
              </Typography>
              <Typography variant="body2" fontWeight="600" fontSize="0.85rem" color="info.main">
                {survey.series_updated} / {survey.series_total} ({(survey.update_progress * 100).toFixed(0)}%)
              </Typography>
            </Box>
          )}
        </Box>

        {/* Timestamps */}
        <Box sx={{ mb: 1.5, pt: 1.5, borderTop: '1px solid', borderColor: 'divider' }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
            <Typography variant="caption" color="text.secondary" fontSize="0.65rem">
              Last Check
            </Typography>
            <Typography variant="caption" fontSize="0.65rem">
              {formatRelativeTime(survey.last_check)}
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
            <Typography variant="caption" color="text.secondary" fontSize="0.65rem">
              BLS Update
            </Typography>
            <Typography variant="caption" fontSize="0.65rem">
              {formatRelativeTime(survey.last_bls_update)}
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
            <Typography variant="caption" color="text.secondary" fontSize="0.65rem">
              Full Update
            </Typography>
            <Typography variant="caption" fontSize="0.65rem">
              {formatRelativeTime(survey.last_full_update_completed)}
            </Typography>
          </Box>
        </Box>

        {/* Action Button */}
        <Tooltip title="Start full update for this survey">
          <span>
            <Button
              fullWidth
              size="small"
              variant="outlined"
              startIcon={isUpdatingNow ? <CircularProgress size={14} color="inherit" /> : <PlayArrow />}
              onClick={onUpdate}
              disabled={isUpdatingNow || isUpdating}
              sx={{
                fontSize: '0.75rem',
                py: 0.5,
                fontWeight: 500,
              }}
            >
              {isUpdatingNow ? 'Updating...' : 'Update'}
            </Button>
          </span>
        </Tooltip>
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
  }>({
    open: false,
    surveyCode: '',
    surveyName: '',
    seriesCount: 0,
  });

  const { data: overview, isLoading: loadingOverview } = useQuery({
    queryKey: ['freshness', 'overview'],
    queryFn: freshnessAPI.getOverview,
    refetchInterval: 10000,
  });

  const { data: quota, isLoading: loadingQuota } = useQuery({
    queryKey: ['quota', 'today'],
    queryFn: () => quotaAPI.getToday(),
  });

  const { data: quotaHistory, isLoading: loadingHistory } = useQuery({
    queryKey: ['quota', 'history'],
    queryFn: () => quotaAPI.getHistory(7),
  });

  const checkFreshnessMutation = useMutation({
    mutationFn: () => actionsAPI.checkFreshness(),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['freshness'] });
    },
  });

  const executeUpdateMutation = useMutation({
    mutationFn: (surveyCode: string) => actionsAPI.executeUpdate(surveyCode, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['freshness'] });
      setConfirmUpdateDialog({ open: false, surveyCode: '', surveyName: '', seriesCount: 0 });
    },
  });

  const handleUpdateClick = (survey: SurveyFreshness) => {
    setConfirmUpdateDialog({
      open: true,
      surveyCode: survey.survey_code,
      surveyName: survey.survey_name,
      seriesCount: survey.series_total || 0,
    });
  };

  const handleConfirmUpdate = () => {
    executeUpdateMutation.mutate(confirmUpdateDialog.surveyCode);
  };

  const handleCancelUpdate = () => {
    setConfirmUpdateDialog({ open: false, surveyCode: '', surveyName: '', seriesCount: 0 });
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
        <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 2 }}>
          BLS Surveys
        </Typography>
        <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2 }}>
          {overview?.surveys.map((survey: SurveyFreshness) => (
            <SurveyCard
              key={survey.survey_code}
              survey={survey}
              onUpdate={() => handleUpdateClick(survey)}
              isUpdating={executeUpdateMutation.isPending}
            />
          ))}
        </Box>
      </Box>

      {/* 7-Day API Usage Chart - Full Width */}
      <Card sx={{ border: '1px solid', borderColor: 'divider', boxShadow: 'none' }}>
        <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
          <Typography variant="subtitle2" fontWeight="600">
            7-Day API Usage
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
      >
        <DialogTitle id="confirm-update-title">
          Confirm Full Survey Update
        </DialogTitle>
        <DialogContent>
          <DialogContentText id="confirm-update-description">
            You are about to trigger a full data update for:
          </DialogContentText>
          <Box sx={{ mt: 2, p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
            <Typography variant="body2" fontWeight="600" gutterBottom>
              {confirmUpdateDialog.surveyCode} - {confirmUpdateDialog.surveyName}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              This will update approximately <strong>{confirmUpdateDialog.seriesCount.toLocaleString()}</strong> series
            </Typography>
          </Box>
          <DialogContentText sx={{ mt: 2 }}>
            <strong>Warning:</strong> This operation will consume API quota and may take several minutes to complete.
            Are you sure you want to proceed?
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleCancelUpdate} variant="outlined" color="inherit">
            Cancel
          </Button>
          <Button
            onClick={handleConfirmUpdate}
            variant="contained"
            color="warning"
            autoFocus
          >
            Yes, Update Now
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
