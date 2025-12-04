import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
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
  ToggleButton,
  ToggleButtonGroup,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  TrendingFlat,
  ArrowBack,
  Close as CloseIcon,
  Info as InfoIcon,
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
  BarChart,
  Bar,
} from 'recharts';
import { treasuryExplorerAPI } from '../api/client';

// Term colors
const TERM_COLORS: Record<string, string> = {
  '2-Year': '#667eea',
  '5-Year': '#4facfe',
  '7-Year': '#43e97b',
  '10-Year': '#f5576c',
  '20-Year': '#fa709a',
  '30-Year': '#a18cd1',
};

// Period options
const PERIOD_OPTIONS = [
  { label: '1Y', value: 1 },
  { label: '3Y', value: 3 },
  { label: '5Y', value: 5 },
  { label: '10Y', value: 10 },
  { label: '20Y', value: 20 },
];

// Format yield
const formatYield = (value: number | null): string => {
  if (value === null || value === undefined) return 'N/A';
  return `${value.toFixed(3)}%`;
};

// Format amount in billions
const formatAmount = (value: number | null): string => {
  if (value === null || value === undefined) return 'N/A';
  return `$${(value / 1e9).toFixed(1)}B`;
};

// Term Summary Card
function TermCard({
  term,
  data,
  onClick,
  isSelected,
}: {
  term: string;
  data: any;
  onClick: () => void;
  isSelected: boolean;
}) {
  const color = TERM_COLORS[term] || '#667eea';
  const yieldChange = data?.yield_change;
  const direction = yieldChange === null || yieldChange === undefined
    ? 'flat'
    : yieldChange > 0.001
      ? 'up'
      : yieldChange < -0.001
        ? 'down'
        : 'flat';

  const TrendIcon = direction === 'up' ? TrendingUp : direction === 'down' ? TrendingDown : TrendingFlat;
  const trendColor = direction === 'up' ? 'error.main' : direction === 'down' ? 'success.main' : 'text.secondary';

  return (
    <Card
      onClick={onClick}
      sx={{
        cursor: 'pointer',
        height: '100%',
        borderTop: `4px solid ${color}`,
        boxShadow: isSelected ? `0 0 0 2px ${color}` : '0 2px 8px rgba(0,0,0,0.08)',
        transition: 'all 0.2s ease',
        '&:hover': {
          transform: 'translateY(-2px)',
          boxShadow: `0 4px 16px rgba(0,0,0,0.12)`,
        },
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Typography variant="h6" fontWeight="bold" sx={{ color }}>
          {term}
        </Typography>

        {data ? (
          <>
            <Box sx={{ mt: 1 }}>
              <Typography variant="caption" color="text.secondary">
                Latest Yield
              </Typography>
              <Typography variant="h5" fontWeight="bold">
                {formatYield(data.high_yield)}
              </Typography>
            </Box>

            {yieldChange !== null && yieldChange !== undefined && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
                <TrendIcon sx={{ fontSize: 16, color: trendColor }} />
                <Typography variant="body2" sx={{ color: trendColor }}>
                  {yieldChange > 0 ? '+' : ''}{(yieldChange * 100).toFixed(1)} bps
                </Typography>
              </Box>
            )}

            <Box sx={{ mt: 1, display: 'flex', gap: 2 }}>
              <Box>
                <Typography variant="caption" color="text.secondary">Bid-to-Cover</Typography>
                <Typography variant="body2" fontWeight="medium">
                  {data.bid_to_cover_ratio?.toFixed(2) || 'N/A'}
                </Typography>
              </Box>
              <Box>
                <Typography variant="caption" color="text.secondary">Date</Typography>
                <Typography variant="body2" fontWeight="medium">
                  {data.auction_date || 'N/A'}
                </Typography>
              </Box>
            </Box>
          </>
        ) : (
          <Typography color="text.secondary" sx={{ mt: 2 }}>No data</Typography>
        )}
      </CardContent>
    </Card>
  );
}

export default function TreasuryExplorer() {
  const [selectedTerm, setSelectedTerm] = useState<string>('10-Year');
  const [periodYears, setPeriodYears] = useState<number>(5);
  const [detailDialogOpen, setDetailDialogOpen] = useState(false);
  const [selectedAuction, setSelectedAuction] = useState<any>(null);

  // Fetch snapshot data for cards
  const { data: snapshot, isLoading: snapshotLoading } = useQuery({
    queryKey: ['treasury-snapshot'],
    queryFn: () => treasuryExplorerAPI.getSnapshot(),
  });

  // Fetch yield history for selected term
  const { data: yieldHistory, isLoading: historyLoading } = useQuery({
    queryKey: ['treasury-history', selectedTerm, periodYears],
    queryFn: () => treasuryExplorerAPI.getYieldHistory(selectedTerm, periodYears),
    enabled: !!selectedTerm,
  });

  // Fetch recent auctions for selected term
  const { data: recentAuctions, isLoading: auctionsLoading } = useQuery({
    queryKey: ['treasury-auctions', selectedTerm],
    queryFn: () => treasuryExplorerAPI.getAuctions({ security_term: selectedTerm, limit: 20 }),
    enabled: !!selectedTerm,
  });

  // Fetch upcoming auctions
  const { data: upcomingAuctions } = useQuery({
    queryKey: ['treasury-upcoming-explorer'],
    queryFn: () => treasuryExplorerAPI.getUpcoming(),
  });

  // Fetch auction detail
  const { data: auctionDetail, isLoading: detailLoading } = useQuery({
    queryKey: ['treasury-auction-detail', selectedAuction?.auction_id],
    queryFn: () => treasuryExplorerAPI.getAuctionDetail(selectedAuction.auction_id),
    enabled: !!selectedAuction?.auction_id,
  });

  // Get snapshot data as map
  const snapshotMap = useMemo(() => {
    if (!snapshot?.data) return {};
    return Object.fromEntries(snapshot.data.map((d: any) => [d.security_term, d]));
  }, [snapshot]);

  // Chart data
  const chartData = useMemo(() => {
    if (!yieldHistory?.data) return [];
    return yieldHistory.data.map((d: any) => ({
      date: d.auction_date,
      yield: d.high_yield,
      bidToCover: d.bid_to_cover_ratio,
      amount: d.offering_amount ? d.offering_amount / 1e9 : null,
    }));
  }, [yieldHistory]);

  const color = TERM_COLORS[selectedTerm] || '#667eea';

  const handleAuctionClick = (auction: any) => {
    setSelectedAuction(auction);
    setDetailDialogOpen(true);
  };

  return (
    <Box sx={{ p: 3 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
        <IconButton
          component={Link}
          to="/treasury"
          size="small"
          sx={{
            bgcolor: 'grey.100',
            '&:hover': { bgcolor: 'grey.200' },
          }}
        >
          <ArrowBack />
        </IconButton>
        <Typography variant="h4" fontWeight="bold">
          Treasury Auction Explorer
        </Typography>
      </Box>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3, ml: 6 }}>
        Explore U.S. Treasury Notes & Bonds auction history and yields
      </Typography>

      {/* Term Summary Cards */}
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
        Latest Auctions by Term
      </Typography>

      {snapshotLoading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
          <CircularProgress />
        </Box>
      ) : (
        <Grid container spacing={2} sx={{ mb: 4 }}>
          {['2-Year', '5-Year', '7-Year', '10-Year', '20-Year', '30-Year'].map((term) => (
            <Grid item xs={6} sm={4} md={2} key={term}>
              <TermCard
                term={term}
                data={snapshotMap[term]}
                onClick={() => setSelectedTerm(term)}
                isSelected={selectedTerm === term}
              />
            </Grid>
          ))}
        </Grid>
      )}

      <Divider sx={{ mb: 3 }} />

      {/* Yield History Section */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography
          variant="h6"
          fontWeight="600"
          sx={{
            color: 'primary.main',
            borderBottom: '2px solid',
            borderColor: 'primary.main',
            pb: 1,
            display: 'inline-block',
          }}
        >
          {selectedTerm} Yield History
        </Typography>

        <ToggleButtonGroup
          value={periodYears}
          exclusive
          onChange={(_, value) => value !== null && setPeriodYears(value)}
          size="small"
          sx={{
            '& .MuiToggleButton-root': {
              px: 2,
              '&.Mui-selected': {
                bgcolor: color,
                color: 'white',
                '&:hover': {
                  bgcolor: color,
                },
              },
            },
          }}
        >
          {PERIOD_OPTIONS.map((opt) => (
            <ToggleButton key={opt.value} value={opt.value}>
              {opt.label}
            </ToggleButton>
          ))}
        </ToggleButtonGroup>
      </Box>

      {historyLoading ? (
        <Card sx={{ mb: 3 }}>
          <CardContent sx={{ py: 6, textAlign: 'center' }}>
            <CircularProgress />
          </CardContent>
        </Card>
      ) : chartData.length === 0 ? (
        <Alert severity="info" sx={{ mb: 3 }}>No yield history data available for {selectedTerm}</Alert>
      ) : (
        <Card sx={{ mb: 3, border: '1px solid', borderColor: 'divider' }}>
          <CardContent>
            <Box sx={{ height: 400 }}>
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={chartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                  <defs>
                    <linearGradient id="yieldGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={color} stopOpacity={0.2} />
                      <stop offset="95%" stopColor={color} stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e8e8e8" vertical={false} />
                  <XAxis
                    dataKey="date"
                    tick={{ fontSize: 11, fill: '#666' }}
                    tickLine={{ stroke: '#e0e0e0' }}
                    axisLine={{ stroke: '#e0e0e0' }}
                    interval="preserveStartEnd"
                  />
                  <YAxis
                    yAxisId="yield"
                    tick={{ fontSize: 11, fill: '#666' }}
                    tickLine={{ stroke: '#e0e0e0' }}
                    axisLine={{ stroke: '#e0e0e0' }}
                    tickFormatter={(v) => `${v.toFixed(2)}%`}
                    domain={['auto', 'auto']}
                  />
                  <YAxis
                    yAxisId="btc"
                    orientation="right"
                    tick={{ fontSize: 11, fill: '#666' }}
                    tickLine={{ stroke: '#e0e0e0' }}
                    axisLine={{ stroke: '#e0e0e0' }}
                    domain={[0, 'auto']}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(255,255,255,0.96)',
                      border: 'none',
                      borderRadius: 12,
                      boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
                      padding: '12px 16px',
                    }}
                    formatter={(value: number, name: string) => {
                      if (name === 'yield') return [`${value?.toFixed(3)}%`, 'Yield'];
                      if (name === 'bidToCover') return [value?.toFixed(2), 'Bid-to-Cover'];
                      return [value, name];
                    }}
                  />
                  <Legend />
                  <Area
                    yAxisId="yield"
                    type="monotone"
                    dataKey="yield"
                    stroke="transparent"
                    fill="url(#yieldGradient)"
                  />
                  <Line
                    yAxisId="yield"
                    type="monotone"
                    dataKey="yield"
                    name="Yield"
                    stroke={color}
                    strokeWidth={2.5}
                    dot={false}
                    activeDot={{ r: 6, fill: color, stroke: '#fff', strokeWidth: 3 }}
                  />
                  <Line
                    yAxisId="btc"
                    type="monotone"
                    dataKey="bidToCover"
                    name="Bid-to-Cover"
                    stroke="#888"
                    strokeWidth={1.5}
                    strokeDasharray="5 5"
                    dot={false}
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </Box>
          </CardContent>
        </Card>
      )}

      {/* Upcoming Auctions */}
      {upcomingAuctions && upcomingAuctions.length > 0 && (
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
            Upcoming Auctions
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', mb: 3 }}>
            {upcomingAuctions.map((auction: any) => (
              <Chip
                key={auction.upcoming_id}
                label={`${auction.security_term} - ${auction.auction_date}`}
                sx={{
                  bgcolor: TERM_COLORS[auction.security_term] || '#667eea',
                  color: 'white',
                  fontWeight: 500,
                }}
              />
            ))}
          </Box>
          <Divider sx={{ mb: 3 }} />
        </>
      )}

      {/* Recent Auctions Table */}
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
        Recent {selectedTerm} Auctions
      </Typography>

      {auctionsLoading ? (
        <Card>
          <CardContent sx={{ py: 4, textAlign: 'center' }}>
            <CircularProgress />
          </CardContent>
        </Card>
      ) : !recentAuctions || recentAuctions.length === 0 ? (
        <Alert severity="info">No auction data available for {selectedTerm}</Alert>
      ) : (
        <Card sx={{ border: '1px solid', borderColor: 'divider' }}>
          <CardContent>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Date</TableCell>
                    <TableCell sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>CUSIP</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>High Yield</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Coupon</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Bid-to-Cover</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Offering</TableCell>
                    <TableCell align="center" sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>Details</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {recentAuctions.map((auction: any, idx: number) => (
                    <TableRow
                      key={auction.auction_id}
                      hover
                      sx={{ bgcolor: idx % 2 === 0 ? 'transparent' : 'grey.50' }}
                    >
                      <TableCell>{auction.auction_date}</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{auction.cusip}</TableCell>
                      <TableCell align="right" sx={{ fontFamily: 'monospace' }}>
                        {formatYield(auction.high_yield)}
                      </TableCell>
                      <TableCell align="right" sx={{ fontFamily: 'monospace' }}>
                        {formatYield(auction.coupon_rate)}
                      </TableCell>
                      <TableCell align="right" sx={{ fontFamily: 'monospace' }}>
                        {auction.bid_to_cover_ratio?.toFixed(2) || 'N/A'}
                      </TableCell>
                      <TableCell align="right" sx={{ fontFamily: 'monospace' }}>
                        {formatAmount(auction.offering_amount)}
                      </TableCell>
                      <TableCell align="center">
                        <IconButton
                          size="small"
                          onClick={() => handleAuctionClick(auction)}
                        >
                          <InfoIcon fontSize="small" />
                        </IconButton>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </CardContent>
        </Card>
      )}

      {/* Auction Detail Dialog */}
      <Dialog
        open={detailDialogOpen}
        onClose={() => setDetailDialogOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">
            Auction Details - {selectedAuction?.security_term} ({selectedAuction?.auction_date})
          </Typography>
          <IconButton onClick={() => setDetailDialogOpen(false)}>
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent dividers>
          {detailLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : auctionDetail ? (
            <Grid container spacing={3}>
              {/* Basic Info */}
              <Grid item xs={12} md={6}>
                <Typography variant="subtitle2" color="primary" fontWeight="bold" gutterBottom>
                  Basic Information
                </Typography>
                <Table size="small">
                  <TableBody>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>CUSIP</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{auctionDetail.cusip}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Security Type</TableCell>
                      <TableCell>{auctionDetail.security_type}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Term</TableCell>
                      <TableCell>{auctionDetail.security_term}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Issue Date</TableCell>
                      <TableCell>{auctionDetail.issue_date || 'N/A'}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Maturity Date</TableCell>
                      <TableCell>{auctionDetail.maturity_date || 'N/A'}</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Grid>

              {/* Yield Info */}
              <Grid item xs={12} md={6}>
                <Typography variant="subtitle2" color="primary" fontWeight="bold" gutterBottom>
                  Yield Information
                </Typography>
                <Table size="small">
                  <TableBody>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>High Yield</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatYield(auctionDetail.high_yield)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Low Yield</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatYield(auctionDetail.low_yield)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Median Yield</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatYield(auctionDetail.median_yield)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Coupon Rate</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatYield(auctionDetail.coupon_rate)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Price per $100</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>${auctionDetail.price_per_100?.toFixed(4) || 'N/A'}</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Grid>

              {/* Demand Info */}
              <Grid item xs={12} md={6}>
                <Typography variant="subtitle2" color="primary" fontWeight="bold" gutterBottom>
                  Demand Metrics
                </Typography>
                <Table size="small">
                  <TableBody>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Offering Amount</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatAmount(auctionDetail.offering_amount)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Total Tendered</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatAmount(auctionDetail.total_tendered)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Total Accepted</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatAmount(auctionDetail.total_accepted)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Bid-to-Cover</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace', fontWeight: 'bold', color: 'primary.main' }}>
                        {auctionDetail.bid_to_cover_ratio?.toFixed(2) || 'N/A'}
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Grid>

              {/* Bidder Breakdown */}
              <Grid item xs={12} md={6}>
                <Typography variant="subtitle2" color="primary" fontWeight="bold" gutterBottom>
                  Bidder Breakdown
                </Typography>
                <Table size="small">
                  <TableBody>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Primary Dealer Accepted</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatAmount(auctionDetail.primary_dealer_accepted)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Direct Bidder Accepted</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatAmount(auctionDetail.direct_bidder_accepted)}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 'medium' }}>Indirect Bidder Accepted</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace' }}>{formatAmount(auctionDetail.indirect_bidder_accepted)}</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Grid>
            </Grid>
          ) : (
            <Alert severity="error">Failed to load auction details</Alert>
          )}
        </DialogContent>
      </Dialog>
    </Box>
  );
}
