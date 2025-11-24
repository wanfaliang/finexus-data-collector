import axios from 'axios';

// Create axios instance with base configuration
export const apiClient = axios.create({
  baseURL: '/api/v1',
  headers: {
    'Content-Type': 'application/json',
  },
});

// API response types
export interface SurveyFreshness {
  survey_code: string;
  survey_name: string;
  status: 'current' | 'needs_update' | 'updating' | 'unknown';
  last_bls_update: string | null;
  last_check: string | null;
  sentinels_changed: number;
  sentinels_total: number;
  update_frequency_days: number | null;
  update_progress: number | null;
  series_updated: number;
  series_total: number;
  last_full_update_completed: string | null;
}

export interface FreshnessOverview {
  total_surveys: number;
  surveys_current: number;
  surveys_need_update: number;
  surveys_updating: number;
  surveys: SurveyFreshness[];
}

export interface QuotaUsage {
  date: string;
  used: number;
  limit: number;
  remaining: number;
  percentage_used: number;
}

export interface QuotaBreakdownItem {
  label: string;
  requests: number;
  series: number;
}

export interface QuotaBreakdown {
  date: string;
  total_requests: number;
  total_series: number;
  by_survey: QuotaBreakdownItem[];
  by_script: QuotaBreakdownItem[];
}

export interface FreshnessCheckResult {
  survey_code: string;
  sentinels_checked: number;
  sentinels_changed: number;
  needs_update: boolean;
  error: string | null;
}

export interface FreshnessCheckResponse {
  check_time: string;
  surveys_checked: number;
  surveys_needing_update: number;
  results: FreshnessCheckResult[];
}

export interface UpdateTriggerResponse {
  survey_code: string;
  status: string;
  message: string;
  series_count: number | null;
  estimated_requests: number | null;
}

// API functions
export const freshnessAPI = {
  getOverview: () =>
    apiClient.get<FreshnessOverview>('/freshness/overview').then(r => r.data),

  getSurvey: (code: string) =>
    apiClient.get<SurveyFreshness>(`/freshness/surveys/${code}`).then(r => r.data),

  getSurveysNeedingUpdate: () =>
    apiClient.get<string[]>('/freshness/surveys/needs-update').then(r => r.data),
};

export const quotaAPI = {
  getToday: (dailyLimit = 500) =>
    apiClient.get<QuotaUsage>('/quota/today', { params: { daily_limit: dailyLimit } }).then(r => r.data),

  getHistory: (days = 7, dailyLimit = 500) =>
    apiClient.get<QuotaUsage[]>('/quota/history', { params: { days, daily_limit: dailyLimit } }).then(r => r.data),

  getBreakdown: (usageDate?: string) =>
    apiClient.get<QuotaBreakdown>('/quota/breakdown', { params: { usage_date: usageDate } }).then(r => r.data),
};

export const actionsAPI = {
  checkFreshness: (surveyCodes?: string[]) =>
    apiClient.post<FreshnessCheckResponse>('/actions/freshness/check', { survey_codes: surveyCodes }).then(r => r.data),

  executeUpdate: (surveyCode: string, force = false) =>
    apiClient.post<UpdateTriggerResponse>(`/actions/freshness/execute/${surveyCode}`, { force }).then(r => r.data),

  resetFreshness: (surveyCode: string) =>
    apiClient.post(`/actions/freshness/reset/${surveyCode}`, {
      clear_update_flag: true,
      reset_sentinels: false,
    }).then(r => r.data),

  listSurveys: () =>
    apiClient.get<string[]>('/actions/surveys').then(r => r.data),
};

// ==================== Explorer Types ====================

// CU Explorer
export interface CUAreaItem {
  area_code: string;
  area_name: string;
  display_level: number;
  selectable: boolean;
  sort_sequence: number;
}

export interface CUItemItem {
  item_code: string;
  item_name: string;
  display_level: number;
  selectable: boolean;
  sort_sequence: number;
}

export interface CUDimensions {
  areas: CUAreaItem[];
  items: CUItemItem[];
}

export interface CUSeriesInfo {
  series_id: string;
  series_title: string;
  area_code: string;
  area_name: string;
  item_code: string;
  item_name: string;
  seasonal_code: string;
  periodicity_code?: string;
  base_period?: string;
  begin_year?: number;
  begin_period?: string;
  end_year?: number;
  end_period?: string;
  is_active: boolean;
}

export interface CUSeriesListResponse {
  survey_code: string;
  total: number;
  limit: number;
  offset: number;
  series: CUSeriesInfo[];
}

export interface DataPoint {
  year: number;
  period: string;
  period_name: string;
  value?: number;
  footnote_codes?: string;
}

export interface CUSeriesData {
  series_id: string;
  series_title: string;
  area_name: string;
  item_name: string;
  data_points: DataPoint[];
}

export interface CUDataResponse {
  survey_code: string;
  series: CUSeriesData[];
}

// CU Analytics
export interface InflationMetric {
  series_id: string;
  item_name: string;
  latest_value?: number;
  latest_date?: string;
  month_over_month?: number;
  year_over_year?: number;
}

export interface CUOverviewResponse {
  survey_code: string;
  headline_cpi?: InflationMetric;
  core_cpi?: InflationMetric;
  last_updated?: string;
}

export interface CategoryMetric {
  category_code: string;
  category_name: string;
  latest_value?: number;
  latest_date?: string;
  month_over_month?: number;
  year_over_year?: number;
  series_id: string;
}

export interface CUCategoryAnalysisResponse {
  survey_code: string;
  area_code: string;
  area_name: string;
  categories: CategoryMetric[];
}

export interface AreaComparisonMetric {
  area_code: string;
  area_name: string;
  series_id: string;
  latest_value?: number;
  latest_date?: string;
  month_over_month?: number;
  year_over_year?: number;
}

export interface CUAreaComparisonResponse {
  survey_code: string;
  item_code: string;
  item_name: string;
  areas: AreaComparisonMetric[];
}

// LA Explorer
export interface LAAreaItem {
  area_code: string;
  area_name: string;
  area_type?: string;
}

export interface LAMeasureItem {
  measure_code: string;
  measure_name: string;
}

export interface LADimensions {
  areas: LAAreaItem[];
  measures: LAMeasureItem[];
}

export interface LASeriesInfo {
  series_id: string;
  series_title: string;
  area_code: string;
  area_name: string;
  measure_code: string;
  measure_name: string;
  seasonal_code?: string;
  begin_year?: number;
  begin_period?: string;
  end_year?: number;
  end_period?: string;
  is_active: boolean;
}

export interface LASeriesListResponse {
  survey_code: string;
  total: number;
  limit: number;
  offset: number;
  series: LASeriesInfo[];
}

export interface LASeriesData {
  series_id: string;
  series_title: string;
  area_name: string;
  measure_name: string;
  data_points: DataPoint[];
}

export interface LADataResponse {
  survey_code: string;
  series: LASeriesData[];
}

// CE Explorer
export interface CEIndustryItem {
  industry_code: string;
  industry_name: string;
  display_level: number;
  selectable: boolean;
  sort_sequence: number;
  supersector_code?: string;
}

export interface CESupersectorItem {
  supersector_code: string;
  supersector_name: string;
}

export interface CEDimensions {
  industries: CEIndustryItem[];
  supersectors: CESupersectorItem[];
}

export interface CESeriesInfo {
  series_id: string;
  series_title: string;
  industry_code: string;
  industry_name: string;
  supersector_code?: string;
  supersector_name?: string;
  seasonal_code?: string;
  begin_year?: number;
  begin_period?: string;
  end_year?: number;
  end_period?: string;
  is_active: boolean;
}

export interface CESeriesListResponse {
  survey_code: string;
  total: number;
  limit: number;
  offset: number;
  series: CESeriesInfo[];
}

export interface CESeriesData {
  series_id: string;
  series_title: string;
  industry_name: string;
  data_points: DataPoint[];
}

export interface CEDataResponse {
  survey_code: string;
  series: CESeriesData[];
}

// ==================== Timeline Types ====================

export interface TimelineDataPoint {
  year: number;
  period: string;
  period_name: string;
  headline_value?: number;
  headline_yoy?: number;
  headline_mom?: number;
  core_value?: number;
  core_yoy?: number;
  core_mom?: number;
}

export interface CUOverviewTimelineResponse {
  survey_code: string;
  area_code: string;
  area_name: string;
  timeline: TimelineDataPoint[];
}

export interface CategoryTimelinePoint {
  year: number;
  period: string;
  period_name: string;
  categories: CategoryMetric[];
}

export interface CUCategoryTimelineResponse {
  survey_code: string;
  area_code: string;
  area_name: string;
  timeline: CategoryTimelinePoint[];
}

export interface AreaTimelinePoint {
  year: number;
  period: string;
  period_name: string;
  areas: AreaComparisonMetric[];
}

export interface CUAreaComparisonTimelineResponse {
  survey_code: string;
  item_code: string;
  item_name: string;
  timeline: AreaTimelinePoint[];
}

// ==================== Explorer API Functions ====================

export const cuExplorerAPI = {
  getDimensions: () =>
    apiClient.get<CUDimensions>('/explorer/cu/dimensions').then(r => r.data),

  getSeries: (params?: {
    area_code?: string;
    item_code?: string;
    seasonal_code?: string;
    begin_year?: number;
    end_year?: number;
    active_only?: boolean;
    limit?: number;
    offset?: number;
  }) =>
    apiClient.get<CUSeriesListResponse>('/explorer/cu/series', { params }).then(r => r.data),

  getSeriesData: (seriesId: string, params?: {
    start_year?: number;
    end_year?: number;
    start_period?: string;
    end_period?: string;
  }) =>
    apiClient.get<CUDataResponse>(`/explorer/cu/series/${seriesId}/data`, { params }).then(r => r.data),

  getOverview: (area_code?: string) =>
    apiClient.get<CUOverviewResponse>('/explorer/cu/overview', { params: { area_code } }).then(r => r.data),

  getCategoryAnalysis: (area_code?: string) =>
    apiClient.get<CUCategoryAnalysisResponse>('/explorer/cu/categories', { params: { area_code } }).then(r => r.data),

  compareAreas: (item_code?: string) =>
    apiClient.get<CUAreaComparisonResponse>('/explorer/cu/areas/compare', { params: { item_code } }).then(r => r.data),

  // Timeline endpoints
  getOverviewTimeline: (area_code?: string, months_back?: number) =>
    apiClient.get<CUOverviewTimelineResponse>('/explorer/cu/overview/timeline', {
      params: { area_code, months_back }
    }).then(r => r.data),

  getCategoryTimeline: (area_code?: string, months_back?: number) =>
    apiClient.get<CUCategoryTimelineResponse>('/explorer/cu/categories/timeline', {
      params: { area_code, months_back }
    }).then(r => r.data),

  getAreaComparisonTimeline: (item_code?: string, months_back?: number) =>
    apiClient.get<CUAreaComparisonTimelineResponse>('/explorer/cu/areas/compare/timeline', {
      params: { item_code, months_back }
    }).then(r => r.data),
};

export const laExplorerAPI = {
  getDimensions: () =>
    apiClient.get<LADimensions>('/explorer/la/dimensions').then(r => r.data),

  getSeries: (params?: {
    area_code?: string;
    measure_code?: string;
    seasonal_code?: string;
    active_only?: boolean;
    limit?: number;
    offset?: number;
  }) =>
    apiClient.get<LASeriesListResponse>('/explorer/la/series', { params }).then(r => r.data),

  getSeriesData: (seriesId: string, params?: { start_year?: number; end_year?: number }) =>
    apiClient.get<LADataResponse>(`/explorer/la/series/${seriesId}/data`, { params }).then(r => r.data),
};

export const ceExplorerAPI = {
  getDimensions: () =>
    apiClient.get<CEDimensions>('/explorer/ce/dimensions').then(r => r.data),

  getSeries: (params?: {
    industry_code?: string;
    supersector_code?: string;
    seasonal_code?: string;
    active_only?: boolean;
    limit?: number;
    offset?: number;
  }) =>
    apiClient.get<CESeriesListResponse>('/explorer/ce/series', { params }).then(r => r.data),

  getSeriesData: (seriesId: string, params?: { start_year?: number; end_year?: number }) =>
    apiClient.get<CEDataResponse>(`/explorer/ce/series/${seriesId}/data`, { params }).then(r => r.data),
};
