import type { ReactNode } from 'react';
import {
  AppBar,
  Box,
  Toolbar,
  Typography,
} from '@mui/material';
import {
  Storage as StorageIcon,
} from '@mui/icons-material';

interface AppLayoutProps {
  children: ReactNode;
}

export default function AppLayout({ children }: AppLayoutProps) {
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
          <Typography variant="h6" noWrap component="div" fontWeight="bold">
            FinExus Admin
          </Typography>
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
