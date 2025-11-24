import React, { useEffect, useState, useMemo, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {Plus,  RefreshCw,  Share2,  Download,  ExternalLink,  FolderOpen,  Search,  Grid3x3,  List,  Clock,  Database,  TrendingUp,  CheckCircle2,  XCircle,  Loader2,  MoreVertical,

  Eye,

} from "lucide-react";
import { datasetsAPI, formatError } from "../services/api";

/**
 * Datasets.jsx - Professional Dataset Management Interface
 * 
 * Features:
 * - Beautiful grid/list view toggle
 * - Status-based filtering and actions
 * - Search and advanced filtering
 * - Rich status indicators with progress
 * - Inline actions (share, export, delete)
 * - Responsive design
 * - Empty states with helpful guidance
 */

function cls(...classes) {
  return classes.filter(Boolean).join(" ");
}

// ============================================================================
// STATUS CONFIGURATION
// ============================================================================

const statusConfig = {
  created: {
    label: "Created",
    color: "slate",
    icon: Clock,
    bgClass: "bg-slate-100",
    textClass: "text-slate-700",
    borderClass: "border-slate-200",
    description: "Ready to collect data",
  },
  collecting: {
    label: "Collecting",
    color: "amber",
    icon: Loader2,
    bgClass: "bg-amber-100",
    textClass: "text-amber-800",
    borderClass: "border-amber-200",
    description: "Gathering financial data",
    animate: true,
  },
  ready: {
    label: "Ready",
    color: "emerald",
    icon: CheckCircle2,
    bgClass: "bg-emerald-100",
    textClass: "text-emerald-800",
    borderClass: "border-emerald-200",
    description: "Available for exploration",
  },
  failed: {
    label: "Failed",
    color: "rose",
    icon: XCircle,
    bgClass: "bg-rose-100",
    textClass: "text-rose-800",
    borderClass: "border-rose-200",
    description: "Collection error occurred",
  },
};

// ============================================================================
// STATUS CHIP COMPONENT
// ============================================================================

function StatusChip({ status, progress }) {
  const config = statusConfig[status] || statusConfig.created;
  const Icon = config.icon;

  return (
    <div className={cls(
      "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border",
      config.bgClass,
      config.textClass,
      config.borderClass
    )}>
      <Icon className={cls("w-3.5 h-3.5", config.animate && "animate-spin")} />
      <span>{config.label}</span>
      {status === "collecting" && progress !== undefined && (
        <span className="opacity-75">({progress}%)</span>
      )}
    </div>
  );
}

// ============================================================================
// EMPTY STATE COMPONENT
// ============================================================================

function EmptyState({ onCreate, hasFilters }) {
  if (hasFilters) {
    return (
      <div className="text-center py-16">
        <div className="mx-auto w-16 h-16 rounded-2xl bg-slate-100 flex items-center justify-center">
          <Search className="w-8 h-8 text-slate-400" />
        </div>
        <h3 className="mt-4 text-lg font-medium text-slate-900">No datasets found</h3>
        <p className="mt-2 text-sm text-slate-700">Try adjusting your filters or search query</p>
      </div>
    );
  }

  return (
    <div className="text-center py-20">
      <div className="mx-auto w-20 h-20 rounded-3xl bg-gradient-to-br from-slate-100 to-slate-50 flex items-center justify-center shadow-sm">
        <FolderOpen className="w-10 h-10 text-slate-400" />
      </div>
      <h2 className="mt-6 text-2xl font-semibold text-slate-900">No datasets yet</h2>
      <p className="mt-3 text-slate-700 max-w-md mx-auto">
        Create your first dataset to explore financial data with flexible filtering,
        custom dashboards, and powerful visualizations.
      </p>
      <button
        onClick={onCreate}
        className="mt-8 inline-flex items-center gap-2 px-5 py-2.5 rounded-xl bg-black text-white hover:bg-black/90 transition-colors shadow-sm"
      >
        <Plus className="w-4 h-4" />
        Create Your First Dataset
      </button>
      <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4 max-w-2xl mx-auto text-left">
        <div className="p-4 rounded-xl border border-slate-200 bg-white">
          <Database className="w-5 h-5 text-slate-700 mb-2" />
          <div className="text-sm font-medium text-slate-900">Flexible Data</div>
          <div className="text-xs text-slate-700 mt-1">
            Access 16+ data sources with custom filtering
          </div>
        </div>
        <div className="p-4 rounded-xl border border-slate-200 bg-white">
          <TrendingUp className="w-5 h-5 text-slate-700 mb-2" />
          <div className="text-sm font-medium text-slate-900">Custom Dashboards</div>
          <div className="text-xs text-slate-700 mt-1">
            Build your own visualizations and insights
          </div>
        </div>
        <div className="p-4 rounded-xl border border-slate-200 bg-white">
          <Share2 className="w-5 h-5 text-slate-700 mb-2" />
          <div className="text-sm font-medium text-slate-900">Team Collaboration</div>
          <div className="text-xs text-slate-700 mt-1">
            Share datasets and dashboards with your team
          </div>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// DATASET CARD COMPONENT (Grid View)
// ============================================================================

function DatasetCard({ dataset, onOpen, onAction }) {
  
  const canExplore = dataset.status === "ready";

  return (
    <article className="group rounded-2xl border border-slate-200 bg-white p-5 hover:shadow-lg hover:border-slate-300 transition-all duration-200">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <h3 className="font-semibold text-slate-900 truncate group-hover:text-black transition-colors">
            {dataset.name || `Dataset ${dataset.id}`}
          </h3>
          <p className="text-xs text-slate-500 mt-0.5 line-clamp-2">
            {dataset.description || "No description"}
          </p>
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); onAction('menu', dataset); }}
          className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 hover:text-slate-700 transition-colors"
        >
          <MoreVertical className="w-4 h-4" />
        </button>
      </div>

      {/* Status & Progress */}
      <div className="mt-4">
        <StatusChip status={dataset.status} progress={dataset.progress} />
      </div>

      {/* Stats */}
      <div className="mt-4 grid grid-cols-2 gap-3 text-xs">
        <div className="flex items-center gap-1.5 text-slate-700">
          <Database className="w-3.5 h-3.5" />
          <span>{dataset.companies?.length || 0} companies</span>
        </div>
        <div className="flex items-center gap-1.5 text-slate-700">
          <Clock className="w-3.5 h-3.5" />
          <span>{formatRelative(dataset.updated_at)}</span>
        </div>
      </div>

      {dataset.status === "ready" && (
        <div className="mt-3 pt-3 border-t border-slate-100 flex items-center gap-2 text-xs text-slate-700">
          <span>{formatBytes(dataset.data_size_mb * 1024 * 1024)}</span>
          <span>•</span>
          <span>{formatNumber(dataset.row_count)} rows</span>
        </div>
      )}

      {/* Actions */}
      <div className="mt-4 flex items-center gap-2">
        {canExplore ? (
          <button
            onClick={() => onOpen(dataset.id)}
            className="flex-1 inline-flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg bg-black text-white hover:bg-black/90 transition-colors text-sm font-medium"
          >
            <ExternalLink className="w-3.5 h-3.5" />
            Explore
          </button>
        ) : dataset.status === "created" ? (
          <button
            onClick={() => onAction('start', dataset)}
            className="flex-1 inline-flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg bg-slate-900 text-white hover:bg-black transition-colors text-sm font-medium"
          >
            <Database className="w-3.5 h-3.5" />
            Start Collection
          </button>
        ) : dataset.status === "failed" ? (
          <button
            onClick={() => onAction('retry', dataset)}
            className="flex-1 inline-flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 transition-colors text-sm font-medium"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Retry
          </button>
        ) : (
          <button
            disabled
            className="flex-1 inline-flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg bg-slate-100 text-slate-400 cursor-not-allowed text-sm font-medium"
          >
            <Loader2 className="w-3.5 h-3.5 animate-spin" />
            Collecting...
          </button>
        )}
        
        {canExplore && (
          <>
            <button
              onClick={(e) => { e.stopPropagation(); onAction('share', dataset); }}
              className="p-2 rounded-lg border border-slate-200 hover:bg-slate-50 text-slate-700 hover:text-slate-900 transition-colors"
              title="Share"
            >
              <Share2 className="w-3.5 h-3.5" />
            </button>
            <button
              onClick={(e) => { e.stopPropagation(); onAction('export', dataset); }}
              className="p-2 rounded-lg border border-slate-200 hover:bg-slate-50 text-slate-700 hover:text-slate-900 transition-colors"
              title="Export"
            >
              <Download className="w-3.5 h-3.5" />
            </button>
          </>
        )}
      </div>

      {/* Visibility indicator */}
      {dataset.visibility === "public" && (
        <div className="mt-3 flex items-center gap-1.5 text-xs text-slate-500">
          <Eye className="w-3 h-3" />
          <span>Public link active</span>
        </div>
      )}
    </article>
  );
}

// ============================================================================
// DATASET ROW COMPONENT (List View)
// ============================================================================

function DatasetRow({ dataset, onOpen, onAction }) {
  const config = statusConfig[dataset.status] || statusConfig.created;
  const canExplore = dataset.status === "ready";

  return (
    <tr className="border-t border-slate-100 hover:bg-slate-50/50 transition-colors">
      <td className="px-4 py-4">
        <div className="flex items-start gap-3">
          <div className={cls(
            "w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0",
            config.bgClass
          )}>
            <Database className={cls("w-5 h-5", config.textClass)} />
          </div>
          <div className="min-w-0 flex-1">
            <div className="font-medium text-slate-900 truncate">
              {dataset.name || `Dataset ${dataset.id}`}
            </div>
            {dataset.description && (
              <div className="text-sm text-slate-500 truncate mt-0.5">
                {dataset.description}
              </div>
            )}
          </div>
        </div>
      </td>
      
      <td className="px-4 py-4">
        <StatusChip status={dataset.status} progress={dataset.progress} />
      </td>
      
      <td className="px-4 py-4 text-sm text-slate-700">
        {dataset.companies?.length || 0} companies
      </td>
      
      <td className="px-4 py-4 text-sm text-slate-700">
        {dataset.status === "ready" ? (
          <div className="space-y-0.5">
            <div>{formatBytes(dataset.data_size_mb * 1024 * 1024)}</div>
            <div className="text-xs text-slate-500">{formatNumber(dataset.row_count)} rows</div>
          </div>
        ) : (
          "—"
        )}
      </td>
      
      <td className="px-4 py-4 text-sm text-slate-700">
        {formatDate(dataset.updated_at)}
      </td>
      
      <td className="px-4 py-4">
        <div className="flex items-center justify-end gap-2">
          {canExplore ? (
            <>
              <button
                onClick={() => onOpen(dataset.id)}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-black text-white hover:bg-black/90 transition-colors text-sm"
              >
                <ExternalLink className="w-3.5 h-3.5" />
                Explore
              </button>
              <button
                onClick={() => onAction('share', dataset)}
                className="p-1.5 rounded-lg border border-slate-200 hover:bg-slate-50 text-slate-700"
                title="Share"
              >
                <Share2 className="w-3.5 h-3.5" />
              </button>
              <button
                onClick={() => onAction('export', dataset)}
                className="p-1.5 rounded-lg border border-slate-200 hover:bg-slate-50 text-slate-700"
                title="Export"
              >
                <Download className="w-3.5 h-3.5" />
              </button>
            </>
          ) : dataset.status === "created" ? (
            <button
              onClick={() => onAction('start', dataset)}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-900 text-white hover:bg-black transition-colors text-sm"
            >
              <Database className="w-3.5 h-3.5" />
              Start Collection
            </button>
          ) : dataset.status === "failed" ? (
            <button
              onClick={() => onAction('retry', dataset)}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-rose-600 text-white hover:bg-rose-700 transition-colors text-sm"
            >
              <RefreshCw className="w-3.5 h-3.5" />
              Retry
            </button>
          ) : (
            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm text-slate-500">
              <Loader2 className="w-3.5 h-3.5 animate-spin" />
              Collecting...
            </span>
          )}
          <button
            onClick={() => onAction('menu', dataset)}
            className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 hover:text-slate-700"
          >
            <MoreVertical className="w-3.5 h-3.5" />
          </button>
        </div>
      </td>
    </tr>
  );
}

// ============================================================================
// MAIN COMPONENT
// ============================================================================

export default function Datasets() {
  const navigate = useNavigate();
  
  // State
  const [datasets, setDatasets] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);
  
  // UI State
  const [viewMode, setViewMode] = useState('grid'); // 'grid' | 'list'
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('all'); // 'all' | 'created' | 'collecting' | 'ready' | 'failed'
  const [visibilityFilter, setVisibilityFilter] = useState('all'); // 'all' | 'private' | 'public'

  // Load datasets
  const loadDatasets = useCallback(async (silent = false) => {
    if (!silent) setRefreshing(true);
    setError(null);
    
    try {
      const params = {};
      if (statusFilter !== 'all') params.status = statusFilter;
      if (visibilityFilter !== 'all') params.visibility = visibilityFilter;
      
      const res = await datasetsAPI.list(params);
      const rawDatasets = res.data;
      
      // Normalize dataset_id to id
      const normalized = rawDatasets.map(d => ({
        ...d,
        id: d.dataset_id || d.id,
      }));
      
      setDatasets(normalized);
    } catch (e) {
      setError(formatError(e));
      setDatasets([]);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [statusFilter, visibilityFilter]);

  useEffect(() => {
    loadDatasets();
  }, [loadDatasets]);

  // Auto-refresh collecting datasets every 5 seconds
  useEffect(() => {
    if (!datasets) return;
    
    const collectingDatasets = datasets.filter(d => d.status === 'collecting');
    if (collectingDatasets.length === 0) return;
    
    const interval = setInterval(() => {
      loadDatasets(true); // Silent refresh
    }, 5000);
    
    return () => clearInterval(interval);
  }, [datasets, loadDatasets]);

  // Filtered datasets
  const filteredDatasets = useMemo(() => {
    if (!datasets) return [];
    
    let filtered = datasets;
    
    // Search filter
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter(d => 
        d.name?.toLowerCase().includes(query) ||
        d.description?.toLowerCase().includes(query) ||
        d.companies?.some(c => c.ticker?.toLowerCase().includes(query))
      );
    }
    
    return filtered;
  }, [datasets, searchQuery]);

  // Group by status for summary
  const summary = useMemo(() => {
    if (!datasets) return {};
    
    return {
      total: datasets.length,
      created: datasets.filter(d => d.status === 'created').length,
      collecting: datasets.filter(d => d.status === 'collecting').length,
      ready: datasets.filter(d => d.status === 'ready').length,
      failed: datasets.filter(d => d.status === 'failed').length,
    };
  }, [datasets]);

  // Actions
  const handleOpen = (id) => {
    navigate(`/datasets/${id}`);
  };

  const handleAction = async (action, dataset) => {
    try {
      switch (action) {
        case 'start':
          await datasetsAPI.startCollection(dataset.id);
          loadDatasets(true);
          break;
        case 'retry':
          await datasetsAPI.reset(dataset.id);
          await datasetsAPI.startCollection(dataset.id);
          loadDatasets(true);
          break;
        case 'share':
          // TODO: Open share modal
          console.log('Share:', dataset);
          break;
        case 'export':
          // TODO: Open export modal
          try {
            const response = await datasetsAPI.downloadRawData(dataset.id);
            const blob = response.data;
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${dataset.name || 'dataset'}.xlsx`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
          } catch (e) {
            alert('Export failed: ' + formatError(e));
          }
          break;
        case 'menu':
          // TODO: Open context menu
          console.log('Menu:', dataset);
          break;
        default:
          break;
      }
    } catch (e) {
      alert(formatError(e));
    }
  };

  // Loading state
  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 className="w-8 h-8 text-slate-400 animate-spin" />
      </div>
    );
  }

  const hasFilters = searchQuery.trim() || statusFilter !== 'all' || visibilityFilter !== 'all';
  const showEmpty = filteredDatasets.length === 0;

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between gap-4 mb-8">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900">Datasets</h1>
          <p className="text-slate-700 mt-2">
            Create, manage, and explore your financial datasets
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => loadDatasets()}
            disabled={refreshing}
            className={cls(
              "inline-flex items-center gap-2 px-4 py-2 rounded-xl border border-slate-200 bg-white text-slate-900 hover:bg-slate-50 transition-colors",
              refreshing && "opacity-70"
            )}
          >
            <RefreshCw className={cls("w-4 h-4", refreshing && "animate-spin")} />
            Refresh
          </button>
          <button
            onClick={() => navigate("/datasets/new")}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-black text-white hover:bg-black/90 transition-colors shadow-sm"
          >
            <Plus className="w-4 h-4" />
            New Dataset
          </button>
        </div>
      </div>

      {/* Error banner */}
      {error && (
        <div className="mb-6 p-4 rounded-xl border border-rose-200 bg-rose-50 text-rose-800 flex items-start gap-3">
          <XCircle className="w-5 h-5 flex-shrink-0 mt-0.5" />
          <div className="flex-1">{error}</div>
        </div>
      )}

      {/* Summary cards */}
      {!showEmpty && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
          <div className="p-4 rounded-xl border border-slate-200 bg-white">
            <div className="text-2xl font-bold text-slate-900">{summary.total}</div>
            <div className="text-sm text-slate-700 mt-1">Total Datasets</div>
          </div>
          <div className="p-4 rounded-xl border border-slate-200 bg-white">
            <div className="text-2xl font-bold text-emerald-600">{summary.ready}</div>
            <div className="text-sm text-slate-700 mt-1">Ready</div>
          </div>
          <div className="p-4 rounded-xl border border-slate-200 bg-white">
            <div className="text-2xl font-bold text-amber-600">{summary.collecting}</div>
            <div className="text-sm text-slate-700 mt-1">Collecting</div>
          </div>
          <div className="p-4 rounded-xl border border-slate-200 bg-white">
            <div className="text-2xl font-bold text-slate-700">{summary.created}</div>
            <div className="text-sm text-slate-700 mt-1">Created</div>
          </div>
          <div className="p-4 rounded-xl border border-slate-200 bg-white">
            <div className="text-2xl font-bold text-rose-600">{summary.failed}</div>
            <div className="text-sm text-slate-700 mt-1">Failed</div>
          </div>
        </div>
      )}

      {/* Filters & View Toggle */}
      {!showEmpty && (
        <div className="flex flex-col md:flex-row gap-4 mb-6">
          {/* Search */}
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <input
              type="text"
              placeholder="Search datasets..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2 rounded-xl border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-black/5"
            />
          </div>

          {/* Status filter */}
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="px-4 py-2 rounded-xl border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-black/5"
          >
            <option value="all">All Status</option>
            <option value="ready">Ready</option>
            <option value="collecting">Collecting</option>
            <option value="created">Created</option>
            <option value="failed">Failed</option>
          </select>

          {/* Visibility filter */}
          <select
            value={visibilityFilter}
            onChange={(e) => setVisibilityFilter(e.target.value)}
            className="px-4 py-2 rounded-xl border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-black/5"
          >
            <option value="all">All Visibility</option>
            <option value="private">Private</option>
            <option value="public">Public</option>
          </select>

          {/* View toggle */}
          <div className="flex items-center gap-1 p-1 rounded-xl border border-slate-200 bg-white">
            <button
              onClick={() => setViewMode('grid')}
              className={cls(
                "p-2 rounded-lg transition-colors",
                viewMode === 'grid' ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-50"
              )}
            >
              <Grid3x3 className="w-4 h-4" />
            </button>
            <button
              onClick={() => setViewMode('list')}
              className={cls(
                "p-2 rounded-lg transition-colors",
                viewMode === 'list' ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-50"
              )}
            >
              <List className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}

      {/* Empty state */}
      {showEmpty ? (
        <EmptyState
          onCreate={() => navigate("/datasets/new")}
          hasFilters={hasFilters}
        />
      ) : viewMode === 'grid' ? (
        /* Grid view */
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {filteredDatasets.map((dataset) => (
            <DatasetCard
              key={dataset.id}
              dataset={dataset}
              onOpen={handleOpen}
              onAction={handleAction}
            />
          ))}
        </div>
      ) : (
        /* List view */
        <div className="rounded-2xl border border-slate-200 bg-white overflow-hidden">
          <table className="w-full">
            <thead className="bg-slate-50 border-b border-slate-200">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-700 uppercase tracking-wider">
                  Dataset
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-700 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-700 uppercase tracking-wider">
                  Companies
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-700 uppercase tracking-wider">
                  Size
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-700 uppercase tracking-wider">
                  Updated
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-slate-700 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {filteredDatasets.map((dataset) => (
                <DatasetRow
                  key={dataset.id}
                  dataset={dataset}
                  onOpen={handleOpen}
                  onAction={handleAction}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function formatDate(dateStr) {
  if (!dateStr) return "—";
  try {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  } catch {
    return "—";
  }
}

function formatRelative(dateStr) {
  if (!dateStr) return "just now";
  
  try {
    const date = new Date(dateStr);
    const now = Date.now();
    const diff = now - date.getTime();
    
    const minutes = Math.floor(diff / 60000);
    if (minutes < 1) return "just now";
    if (minutes < 60) return `${minutes}m ago`;
    
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    
    const days = Math.floor(hours / 24);
    if (days < 7) return `${days}d ago`;
    
    if (days < 30) return `${Math.floor(days / 7)}w ago`;
    
    return formatDate(dateStr);
  } catch {
    return "—";
  }
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function formatNumber(num) {
  if (!num) return '0';
  return num.toLocaleString();
}
