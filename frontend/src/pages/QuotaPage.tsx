import { useQuery } from '@tanstack/react-query';
import {
  Box,
  Card,
  CardContent,
  CircularProgress,
  
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  useTheme,
  useMediaQuery,
} from '@mui/material';
import {
  TrendingUp,
  HourglassEmpty,
  Percent,
  Storage,
} from '@mui/icons-material';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { quotaAPI } from '../api/client';

export default function QuotaPage() {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  const { data: today, isLoading: loadingToday } = useQuery({
    queryKey: ['quota', 'today'],
    queryFn: () => quotaAPI.getToday(),
  });

  const { data: history, isLoading: loadingHistory } = useQuery({
    queryKey: ['quota', 'history'],
    queryFn: () => quotaAPI.getHistory(7),
  });

  const { data: breakdown, isLoading: loadingBreakdown } = useQuery({
    queryKey: ['quota', 'breakdown'],
    queryFn: () => quotaAPI.getBreakdown(),
  });

  if (loadingToday || loadingHistory || loadingBreakdown) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <CircularProgress size={60} />
      </Box>
    );
  }

  const usagePercentage = today?.percentage_used || 0;
  const remaining = today?.remaining || 0;

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 4 }}>
        <Typography variant={isMobile ? 'h4' : 'h3'} fontWeight="600" sx={{ mb: 0.5, color: 'text.primary' }}>
          Quota Usage
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Monitor BLS API quota consumption and usage patterns
        </Typography>
      </Box>

      {/* Stats Cards */}
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(4, 1fr)' }, gap: 3, mb: 4 }}>
          <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
            <CardContent sx={{ p: 2.5 }}>
              <Box display="flex" alignItems="center" mb={1.5}>
                <TrendingUp sx={{ color: 'primary.main', mr: 1 }} />
                <Typography color="text.secondary" variant="body2">
                  Used Today
                </Typography>
              </Box>
              <Typography variant="h4" fontWeight="bold">
                {today?.used}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                of {today?.limit} limit
              </Typography>
            </CardContent>
          </Card>
        <Card sx={{ height: '100%' }}>
            <CardContent sx={{ p: 3 }}>
              <Box display="flex" alignItems="center" mb={1}>
                <HourglassEmpty sx={{ color: remaining > 100 ? 'success.main' : 'error.main', mr: 1 }} />
                <Typography color="text.secondary" variant="body2">
                  Remaining
                </Typography>
              </Box>
              <Typography
                variant="h4"
                fontWeight="bold"
                color={remaining > 100 ? 'success.main' : 'error.main'}
              >
                {remaining}
              </Typography>
            </CardContent>
          </Card>
        <Card sx={{ height: '100%' }}>
            <CardContent sx={{ p: 3 }}>
              <Box display="flex" alignItems="center" mb={1}>
                <Percent sx={{ color: 'warning.main', mr: 1 }} />
                <Typography color="text.secondary" variant="body2">
                  Usage %
                </Typography>
              </Box>
              <Typography variant="h4" fontWeight="bold" color="warning.main">
                {usagePercentage.toFixed(1)}%
              </Typography>
            </CardContent>
          </Card>
        <Card sx={{ height: '100%' }}>
            <CardContent sx={{ p: 3 }}>
              <Box display="flex" alignItems="center" mb={1}>
                <Storage sx={{ color: 'info.main', mr: 1 }} />
                <Typography color="text.secondary" variant="body2">
                  Total Series Today
                </Typography>
              </Box>
              <Typography variant="h4" fontWeight="bold" color="info.main">
                {breakdown?.total_series || 0}
              </Typography>
            </CardContent>
          </Card>
      </Box>

      {/* 7-Day History Chart */}
      <Card sx={{ mb: 4, border: '1px solid', borderColor: 'divider' }}>
        <CardContent sx={{ p: 0 }}>
          <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Typography variant="h6" fontWeight="600">
              7-Day Usage History
            </Typography>
          </Box>
          <Box sx={{ p: 3, pt: 2 }}>
            <Box sx={{ height: 300 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={history}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="used" fill="#1976d2" name="Requests Used" />
              </BarChart>
            </ResponsiveContainer>
            </Box>
          </Box>
        </CardContent>
      </Card>

      {/* Usage Breakdown Tables */}
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(2, 1fr)' }, gap: 3 }}>
          <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
            <CardContent sx={{ p: 0 }}>
              <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                <Typography variant="h6" fontWeight="600">
                  Usage by Survey
                </Typography>
              </Box>
              <TableContainer>
                <Table size={isMobile ? 'small' : 'medium'}>
                  <TableHead>
                    <TableRow sx={{ bgcolor: 'grey.50' }}>
                      <TableCell><strong>Survey</strong></TableCell>
                      <TableCell align="right"><strong>Requests</strong></TableCell>
                      <TableCell align="right"><strong>Series</strong></TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {breakdown?.by_survey?.map((item, index) => (
                      <TableRow
                        key={index}
                        hover
                        sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                      >
                        <TableCell>{item.label}</TableCell>
                        <TableCell align="right">{item.requests}</TableCell>
                        <TableCell align="right">{item.series}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        <Card sx={{ height: '100%', border: '1px solid', borderColor: 'divider' }}>
            <CardContent sx={{ p: 0 }}>
              <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                <Typography variant="h6" fontWeight="600">
                  Usage by Script
                </Typography>
              </Box>
              <TableContainer>
                <Table size={isMobile ? 'small' : 'medium'}>
                  <TableHead>
                    <TableRow sx={{ bgcolor: 'grey.50' }}>
                      <TableCell><strong>Script</strong></TableCell>
                      <TableCell align="right"><strong>Requests</strong></TableCell>
                      <TableCell align="right"><strong>Series</strong></TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {breakdown?.by_script?.map((item, index) => (
                      <TableRow
                        key={index}
                        hover
                        sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                      >
                        <TableCell>{item.label}</TableCell>
                        <TableCell align="right">{item.requests}</TableCell>
                        <TableCell align="right">{item.series}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
      </Box>
    </Box>
  );
}
