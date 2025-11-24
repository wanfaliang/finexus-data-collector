import { useParams } from 'react-router-dom';
import type { ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Typography,
  useTheme,
  useMediaQuery,
} from '@mui/material';
import {
  CheckCircle,
  Warning,
  Sync,
  
} from '@mui/icons-material';
import { freshnessAPI } from '../api/client';

interface DetailItemProps {
  label: string;
  value: string | number | null | undefined;
  icon?: ReactNode;
}

function DetailItem({ label, value, icon }: DetailItemProps) {
  return (
    <Box sx={{ mb: 2 }}>
      <Box display="flex" alignItems="center" mb={0.5}>
        {icon && <Box sx={{ mr: 1, display: 'flex', alignItems: 'center', color: 'text.secondary' }}>{icon}</Box>}
        <Typography variant="caption" color="text.secondary" fontWeight={600}>
          {label}
        </Typography>
      </Box>
      <Typography variant="body1" fontWeight={500}>
        {value ?? 'N/A'}
      </Typography>
    </Box>
  );
}

export default function SurveyDetail() {
  const { code } = useParams<{ code: string }>();
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  const { data: survey, isLoading } = useQuery({
    queryKey: ['freshness', 'survey', code],
    queryFn: () => freshnessAPI.getSurvey(code!),
    enabled: !!code,
  });

  if (isLoading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <CircularProgress size={60} />
      </Box>
    );
  }

  if (!survey) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <Typography variant="h6" color="text.secondary">
          Survey not found
        </Typography>
      </Box>
    );
  }

  const getStatusChip = (status: string) => {
    switch (status) {
      case 'current':
        return <Chip icon={<CheckCircle />} label="Current" color="success" />;
      case 'needs_update':
        return <Chip icon={<Warning />} label="Needs Update" color="warning" />;
      case 'updating':
        return <Chip icon={<Sync />} label="Updating" color="info" />;
      default:
        return <Chip label="Unknown" />;
    }
  };

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 4 }}>
        <Box display="flex" alignItems="center" gap={2} mb={0.5}>
          <Typography variant={isMobile ? 'h4' : 'h3'} fontWeight="600" sx={{ color: 'text.primary' }}>
            {survey.survey_code}
          </Typography>
          {getStatusChip(survey.status)}
        </Box>
        <Typography variant="body2" color="text.secondary">
          {survey.survey_name}
        </Typography>
      </Box>

      {/* Stats Cards */}
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(4, 1fr)' }, gap: 3, mb: 4 }}>
          <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
            <CardContent sx={{ p: 2.5 }}>
              <Typography color="text.secondary" variant="body2" fontWeight={500} sx={{ mb: 1.5 }}>
                Total Series
              </Typography>
              <Typography variant="h3" fontWeight="600">
                {survey.series_total || 0}
              </Typography>
            </CardContent>
          </Card>
        <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
            <CardContent sx={{ p: 2.5 }}>
              <Typography color="text.secondary" variant="body2" fontWeight={500} sx={{ mb: 1.5 }}>
                Series Updated
              </Typography>
              <Typography variant="h3" fontWeight="600" color="success.main">
                {survey.series_updated || 0}
              </Typography>
            </CardContent>
          </Card>
        <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
            <CardContent sx={{ p: 2.5 }}>
              <Typography color="text.secondary" variant="body2" fontWeight={500} sx={{ mb: 1.5 }}>
                Sentinels Changed
              </Typography>
              <Typography variant="h3" fontWeight="600" color="warning.main">
                {survey.sentinels_changed || 0}
              </Typography>
            </CardContent>
          </Card>
        <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
            <CardContent sx={{ p: 2.5 }}>
              <Typography color="text.secondary" variant="body2" fontWeight={500} sx={{ mb: 1.5 }}>
                Total Sentinels
              </Typography>
              <Typography variant="h3" fontWeight="600" color="info.main">
                {survey.sentinels_total || 0}
              </Typography>
            </CardContent>
          </Card>
      </Box>

      {/* Survey Details */}
      <Card sx={{ border: '1px solid', borderColor: 'divider' }}>
        <CardContent sx={{ p: 0 }}>
          <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Typography variant="h6" fontWeight="600">
              Details
            </Typography>
          </Box>
          <Box sx={{ p: 3 }}>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' }, gap: 3, mt: 1 }}>
              <DetailItem
                label="Survey Code"
                value={survey.survey_code}
              />
            <DetailItem
                label="Last Sentinel Check"
                value={survey.last_check ? new Date(survey.last_check).toLocaleString() : 'Never'}
              />
            <DetailItem
                label="Last BLS Update Detected"
                value={survey.last_bls_update ? new Date(survey.last_bls_update).toLocaleString() : 'Never'}
              />
            <DetailItem
                label="Last Full Update Completed"
                value={survey.last_full_update_completed ? new Date(survey.last_full_update_completed).toLocaleString() : 'Never'}
              />
            <DetailItem
                label="Update Frequency"
                value={survey.update_frequency_days ? `${survey.update_frequency_days} days` : 'Unknown'}
              />
            <DetailItem
                label="Status"
                value={survey.status}
              />
          </Box>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
