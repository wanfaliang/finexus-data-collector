import type { ReactNode } from 'react';
import { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
  AppBar,
  Box,
  Toolbar,
  Typography,
  Button,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Divider,
} from '@mui/material';
import {
  Storage as StorageIcon,
  Dashboard as DashboardIcon,
  AccountBalance as BEAIcon,
  Gavel as TreasuryIcon,
  KeyboardArrowDown,
  Settings as SettingsIcon,
  TrendingUp,
  Map,
  Factory,
  Public,
  Business,
} from '@mui/icons-material';

interface AppLayoutProps {
  children: ReactNode;
}

export default function AppLayout({ children }: AppLayoutProps) {
  const location = useLocation();
  const [beaAnchor, setBeaAnchor] = useState<null | HTMLElement>(null);

  const beaSubItems = [
    { label: 'Dashboard', path: '/bea', icon: SettingsIcon, description: 'Manage BEA data collection' },
    { divider: true },
    { label: 'NIPA Explorer', path: '/bea/nipa', icon: TrendingUp, description: 'GDP, Income, Consumption' },
    { label: 'Regional Explorer', path: '/bea/regional', icon: Map, description: 'State & County data' },
    { label: 'GDP by Industry', path: '/bea/gdpbyindustry', icon: Factory, description: 'Industry sectors' },
    { label: 'Int\'l Trade (ITA)', path: '/bea/ita', icon: Public, description: 'Trade balances, exports' },
    { label: 'Fixed Assets', path: '/bea/fixedassets', icon: Business, description: 'Asset stocks, depreciation' },
  ];

  const isBEAPage = location.pathname.startsWith('/bea');

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <AppBar
        position="fixed"
        sx={{
          boxShadow: '0 1px 3px rgba(0,0,0,0.12)',
        }}
      >
        <Toolbar>
          <StorageIcon sx={{ mr: 2 }} />
          <Typography variant="h6" noWrap component="div" fontWeight="bold" sx={{ mr: 4 }}>
            FinExus Admin
          </Typography>
          <Box sx={{ display: 'flex', gap: 1 }}>
            {/* BLS Dashboard */}
            <Button
              component={Link}
              to="/dashboard"
              startIcon={<DashboardIcon />}
              sx={{
                color: 'white',
                bgcolor: location.pathname === '/dashboard' ? 'rgba(255,255,255,0.15)' : 'transparent',
                '&:hover': { bgcolor: 'rgba(255,255,255,0.25)' },
              }}
            >
              BLS Dashboard
            </Button>

            {/* BEA Dropdown */}
            <Button
              startIcon={<BEAIcon />}
              endIcon={<KeyboardArrowDown />}
              onClick={(e) => setBeaAnchor(e.currentTarget)}
              sx={{
                color: 'white',
                bgcolor: isBEAPage ? 'rgba(255,255,255,0.15)' : 'transparent',
                '&:hover': { bgcolor: 'rgba(255,255,255,0.25)' },
              }}
            >
              BEA Data
            </Button>

            {/* Treasury Dashboard */}
            <Button
              component={Link}
              to="/treasury"
              startIcon={<TreasuryIcon />}
              sx={{
                color: 'white',
                bgcolor: location.pathname === '/treasury' ? 'rgba(255,255,255,0.15)' : 'transparent',
                '&:hover': { bgcolor: 'rgba(255,255,255,0.25)' },
              }}
            >
              Treasury
            </Button>
            <Menu
              anchorEl={beaAnchor}
              open={Boolean(beaAnchor)}
              onClose={() => setBeaAnchor(null)}
              PaperProps={{
                sx: { minWidth: 240, mt: 1 }
              }}
            >
              {beaSubItems.map((item, idx) =>
                item.divider ? (
                  <Divider key={idx} sx={{ my: 0.5 }} />
                ) : (
                  <MenuItem
                    key={item.path}
                    component={Link}
                    to={item.path!}
                    onClick={() => setBeaAnchor(null)}
                    selected={location.pathname === item.path}
                  >
                    <ListItemIcon>
                      <item.icon fontSize="small" />
                    </ListItemIcon>
                    <ListItemText
                      primary={item.label}
                      secondary={item.description}
                      secondaryTypographyProps={{ variant: 'caption' }}
                    />
                  </MenuItem>
                )
              )}
            </Menu>
          </Box>
        </Toolbar>
      </AppBar>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          width: '100%',
          backgroundColor: 'background.default',
          minHeight: '100vh',
        }}
      >
        <Toolbar />
        <Box
          sx={{
            py: 4,
            px: { xs: 3, sm: 4, md: 6 },
          }}
        >
          {children}
        </Box>
      </Box>
    </Box>
  );
}
