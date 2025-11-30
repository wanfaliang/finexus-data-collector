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

// LA Explorer types
export interface UnemploymentMetric {
  series_id: string;
  area_code: string;
  area_name: string;
  area_type: string;
  unemployment_rate?: number;
  unemployment_level?: number;
  employment_level?: number;
  labor_force?: number;
  latest_date: string;
  month_over_month?: number;
  year_over_year?: number;
}

export interface LAOverviewResponse {
  survey_code: string;
  national_unemployment: UnemploymentMetric;
  last_updated: string;
}

export interface LAStateAnalysisResponse {
  survey_code: string;
  states: UnemploymentMetric[];
  rankings: {
    highest: string[];
    lowest: string[];
  };
}

export interface LAMetroAnalysisResponse {
  survey_code: string;
  metros: UnemploymentMetric[];
  total_count: number;
}

export interface OverviewTimelinePoint {
  year: number;
  period: string;
  period_name: string;
  unemployment_rate?: number;
  unemployment_level?: number;
  employment_level?: number;
  labor_force?: number;
}

export interface LAOverviewTimelineResponse {
  survey_code: string;
  area_name: string;
  timeline: OverviewTimelinePoint[];
}

export interface StateTimelinePoint {
  year: number;
  period: string;
  period_name: string;
  states: Record<string, number>;
}

export interface LAStateTimelineResponse {
  survey_code: string;
  timeline: StateTimelinePoint[];
  state_names: Record<string, string>;
}

export interface MetroTimelinePoint {
  year: number;
  period: string;
  period_name: string;
  metros: Record<string, number>;
}

export interface LAMetroTimelineResponse {
  survey_code: string;
  timeline: MetroTimelinePoint[];
  metro_names: Record<string, string>;
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

  // Explorer endpoints
  getOverview: () =>
    apiClient.get<LAOverviewResponse>('/explorer/la/overview').then(r => r.data),

  getOverviewTimeline: (months_back?: number) =>
    apiClient.get<LAOverviewTimelineResponse>('/explorer/la/overview/timeline', {
      params: { months_back }
    }).then(r => r.data),

  getStates: () =>
    apiClient.get<LAStateAnalysisResponse>('/explorer/la/states').then(r => r.data),

  getStatesTimeline: (months_back?: number, state_codes?: string) =>
    apiClient.get<LAStateTimelineResponse>('/explorer/la/states/timeline', {
      params: { months_back, state_codes }
    }).then(r => r.data),

  getMetros: (limit?: number) =>
    apiClient.get<LAMetroAnalysisResponse>('/explorer/la/metros', {
      params: { limit }
    }).then(r => r.data),

  getMetrosTimeline: (months_back?: number, metro_codes?: string, limit?: number) =>
    apiClient.get<LAMetroTimelineResponse>('/explorer/la/metros/timeline', {
      params: { months_back, metro_codes, limit }
    }).then(r => r.data),
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

// ==================== LN Explorer Types ====================

export interface LNDimensionItem {
  code: string;
  text: string;
}

export interface LNDimensions {
  labor_force_statuses: LNDimensionItem[];
  ages: LNDimensionItem[];
  sexes: LNDimensionItem[];
  races: LNDimensionItem[];
  educations: LNDimensionItem[];
  occupations: LNDimensionItem[];
  industries: LNDimensionItem[];
  marital_statuses: LNDimensionItem[];
  veteran_statuses: LNDimensionItem[];
  disability_statuses: LNDimensionItem[];
  telework_statuses: LNDimensionItem[];
}

export interface LNSeriesInfo {
  series_id: string;
  series_title: string;
  seasonal: string;
  lfst_code?: string;
  ages_code?: string;
  sexs_code?: string;
  race_code?: string;
  education_code?: string;
  occupation_code?: string;
  indy_code?: string;
  mari_code?: string;
  vets_code?: string;
  disa_code?: string;
  tlwk_code?: string;
  begin_year?: number;
  begin_period?: string;
  end_year?: number;
  end_period?: string;
  is_active: boolean;
}

export interface LNSeriesListResponse {
  survey_code: string;
  total: number;
  limit: number;
  offset: number;
  series: LNSeriesInfo[];
}

export interface LNSeriesData {
  series_id: string;
  series_title: string;
  data_points: DataPoint[];
}

export interface LNDataResponse {
  survey_code: string;
  series: LNSeriesData[];
}

export interface UnemploymentMetric {
  series_id: string;
  dimension_name: string;
  latest_value?: number;
  latest_date?: string;
  month_over_month?: number;
  year_over_year?: number;
}

export interface LNOverviewResponse {
  survey_code: string;
  headline_unemployment?: UnemploymentMetric;
  labor_force_participation?: UnemploymentMetric;
  employment_population_ratio?: UnemploymentMetric;
  last_updated?: string;
}

export interface DemographicBreakdown {
  dimension_type: string;
  dimension_name: string;
  metrics: UnemploymentMetric[];
}

export interface LNDemographicAnalysisResponse {
  survey_code: string;
  breakdowns: DemographicBreakdown[];
}

export interface OverviewTimelinePoint {
  year: number;
  period: string;
  period_name: string;
  headline_value?: number;
  lfpr_value?: number;
  epop_value?: number;
}

export interface LNOverviewTimelineResponse {
  survey_code: string;
  timeline: OverviewTimelinePoint[];
}

export interface DemographicTimelinePoint {
  year: number;
  period: string;
  period_name: string;
  metrics: UnemploymentMetric[];
}

export interface LNDemographicTimelineResponse {
  survey_code: string;
  dimension_type: string;
  dimension_name: string;
  timeline: DemographicTimelinePoint[];
}

export interface LNOccupationAnalysisResponse {
  survey_code: string;
  occupations: UnemploymentMetric[];
}

export interface LNOccupationTimelineResponse {
  survey_code: string;
  timeline: DemographicTimelinePoint[];
}

export interface LNIndustryAnalysisResponse {
  survey_code: string;
  industries: UnemploymentMetric[];
}

export interface LNIndustryTimelineResponse {
  survey_code: string;
  timeline: DemographicTimelinePoint[];
}

// ==================== LN Explorer API ====================

export const lnExplorerAPI = {
  getDimensions: () =>
    apiClient.get<LNDimensions>('/explorer/ln/dimensions').then(r => r.data),

  getSeries: (params?: {
    lfst_code?: string;
    ages_code?: string;
    sexs_code?: string;
    race_code?: string;
    education_code?: string;
    occupation_code?: string;
    indy_code?: string;
    mari_code?: string;
    vets_code?: string;
    disa_code?: string;
    tlwk_code?: string;
    seasonal?: string;
    active_only?: boolean;
    limit?: number;
    offset?: number;
  }) =>
    apiClient.get<LNSeriesListResponse>('/explorer/ln/series', { params }).then(r => r.data),

  getSeriesData: (seriesId: string, params?: {
    start_year?: number;
    end_year?: number;
    start_period?: string;
    end_period?: string;
  }) =>
    apiClient.get<LNDataResponse>(`/explorer/ln/series/${seriesId}/data`, { params }).then(r => r.data),

  getOverview: () =>
    apiClient.get<LNOverviewResponse>('/explorer/ln/overview').then(r => r.data),

  getDemographicAnalysis: () =>
    apiClient.get<LNDemographicAnalysisResponse>('/explorer/ln/demographics').then(r => r.data),

  // Timeline endpoints
  getOverviewTimeline: (months_back?: number) =>
    apiClient.get<LNOverviewTimelineResponse>('/explorer/ln/overview/timeline', {
      params: { months_back }
    }).then(r => r.data),

  getDemographicTimeline: (dimension_type: string, months_back?: number) =>
    apiClient.get<LNDemographicTimelineResponse>('/explorer/ln/demographics/timeline', {
      params: { dimension_type, months_back }
    }).then(r => r.data),

  // Occupation analysis
  getOccupationAnalysis: () =>
    apiClient.get<LNOccupationAnalysisResponse>('/explorer/ln/occupations').then(r => r.data),

  getOccupationTimeline: (months_back?: number) =>
    apiClient.get<LNOccupationTimelineResponse>('/explorer/ln/occupations/timeline', {
      params: { months_back }
    }).then(r => r.data),

  // Industry analysis
  getIndustryAnalysis: () =>
    apiClient.get<LNIndustryAnalysisResponse>('/explorer/ln/industries').then(r => r.data),

  getIndustryTimeline: (months_back?: number) =>
    apiClient.get<LNIndustryTimelineResponse>('/explorer/ln/industries/timeline', {
      params: { months_back }
    }).then(r => r.data),
};

// ==================== BEA Types ====================

export interface BEADatasetFreshness {
  dataset_name: string;
  latest_data_year: number | null;
  latest_data_period: string | null;
  last_checked_at: string | null;
  last_bea_update_detected: string | null;
  needs_update: boolean;
  update_in_progress: boolean;
  last_update_completed: string | null;
  tables_count: number;
  series_count: number;
  data_points_count: number;
  total_checks: number;
  total_updates_detected: number;
}

export interface BEAFreshnessOverview {
  total_datasets: number;
  datasets_current: number;
  datasets_need_update: number;
  datasets_updating: number;
  total_data_points: number;
  datasets: BEADatasetFreshness[];
}

export interface BEAAPIUsage {
  date: string;
  total_requests: number;
  total_data_mb: number;
  total_errors: number;
  requests_remaining: number;
  data_mb_remaining: number;
}

export interface BEACollectionRun {
  run_id: number;
  dataset_name: string;
  run_type: string;
  frequency: string | null;  // 'A', 'Q', 'M' for NIPA/GDPbyIndustry
  geo_scope: string | null;  // 'STATE', 'COUNTY', 'MSA' for Regional
  year_spec: string | null;  // 'ALL', 'LAST5', etc.
  started_at: string;
  completed_at: string | null;
  status: string;
  error_message: string | null;
  tables_processed: number;
  series_processed: number;
  data_points_inserted: number;
  data_points_updated: number;
  api_requests_made: number;
  start_year: number | null;
  end_year: number | null;
  duration_seconds: number | null;
}

export interface BEAStatsSummary {
  nipa: {
    tables: number;
    series: number;
    data_points: number;
  };
  regional: {
    tables: number;
    line_codes: number;
    data_points: number;
  };
  gdpbyindustry: {
    tables: number;
    industries: number;
    data_points: number;
  };
  total_data_points: number;
}

// NIPA Explorer types
export interface NIPATable {
  table_name: string;
  table_description: string;
  has_annual: boolean;
  has_quarterly: boolean;
  has_monthly: boolean;
  first_year: number | null;
  last_year: number | null;
  series_count: number;
  is_active: boolean;
}

export interface NIPASeries {
  series_code: string;
  table_name: string;
  line_number: number;
  line_description: string;
  metric_name: string | null;
  cl_unit: string | null;
  unit_mult: number | null;
  data_points_count: number;
}

export interface NIPATimeSeries {
  series_code: string;
  line_description: string;
  metric_name: string | null;
  unit: string | null;
  data: Array<{
    time_period: string;
    value: number | null;
    note_ref: string | null;
  }>;
}

// Regional Explorer types
export interface RegionalTable {
  table_name: string;
  table_description: string;
  geo_scope: string | null;
  first_year: number | null;
  last_year: number | null;
  line_codes_count: number;
  is_active: boolean;
}

export interface RegionalLineCode {
  table_name: string;
  line_code: number;
  line_description: string;
  cl_unit: string | null;
  unit_mult: number | null;
}

export interface RegionalGeo {
  geo_fips: string;
  geo_name: string;
  geo_type: string | null;
  parent_fips: string | null;
}

export interface RegionalTimeSeries {
  table_name: string;
  line_code: number;
  line_description: string;
  geo_fips: string;
  geo_name: string;
  unit: string | null;
  data: Array<{
    time_period: string;
    value: number | null;
    note_ref: string | null;
  }>;
}

// ==================== BEA API Functions ====================

// BEA Action types
export interface BEATaskStatus {
  nipa_running: boolean;
  regional_running: boolean;
  gdpbyindustry_running: boolean;
}

export interface BEATaskResponse {
  success: boolean;
  message: string;
  run_id: number | null;
}

export interface NIPABackfillRequest {
  frequency: 'A' | 'Q' | 'M';
  year: string;
  tables?: string[];
}

export interface RegionalBackfillRequest {
  geo: 'STATE' | 'COUNTY' | 'MSA';
  year: string;
  tables?: string[];
}

export interface GDPByIndustryBackfillRequest {
  frequency: 'A' | 'Q';
  year: string;
  tables?: number[];
}

export interface UpdateRequest {
  dataset: 'NIPA' | 'Regional' | 'GDPbyIndustry' | 'all';
  year: string;
  force: boolean;
}

export interface NIPAUpdateRequest {
  section: 'priority' | 'gdp' | 'income' | 'govt' | 'trade' | 'investment' | 'industry' | 'supplemental' | 'all';
  frequency: 'A' | 'Q' | 'M';
  year: string;
}

export interface RegionalUpdateRequest {
  category: 'priority' | 'state_gdp' | 'state_income' | 'county' | 'quarterly' | 'all';
  year: string;
}

export interface GDPByIndustryUpdateRequest {
  category: 'priority' | 'value_added' | 'gross_output' | 'inputs' | 'all';
  frequency: 'A' | 'Q';
  year: string;
}

export const beaDashboardAPI = {
  getFreshnessOverview: () =>
    apiClient.get<BEAFreshnessOverview>('/bea/freshness/overview').then(r => r.data),

  getDatasetFreshness: (datasetName: string) =>
    apiClient.get<BEADatasetFreshness>(`/bea/freshness/${datasetName}`).then(r => r.data),

  getUsageToday: () =>
    apiClient.get<BEAAPIUsage>('/bea/usage/today').then(r => r.data),

  getUsageHistory: (days = 7) =>
    apiClient.get<BEAAPIUsage[]>('/bea/usage/history', { params: { days } }).then(r => r.data),

  getRecentRuns: (limit = 10, dataset?: string) =>
    apiClient.get<BEACollectionRun[]>('/bea/runs/recent', { params: { limit, dataset } }).then(r => r.data),

  getCollectionRun: (runId: number) =>
    apiClient.get<BEACollectionRun>(`/bea/runs/${runId}`).then(r => r.data),

  getStatsSummary: () =>
    apiClient.get<BEAStatsSummary>('/bea/stats/summary').then(r => r.data),
};

export const beaActionsAPI = {
  getTaskStatus: () =>
    apiClient.get<BEATaskStatus>('/bea/actions/status').then(r => r.data),

  backfillNIPA: (request: NIPABackfillRequest) =>
    apiClient.post<BEATaskResponse>('/bea/actions/backfill/nipa', request).then(r => r.data),

  backfillRegional: (request: RegionalBackfillRequest) =>
    apiClient.post<BEATaskResponse>('/bea/actions/backfill/regional', request).then(r => r.data),

  backfillGDPbyIndustry: (request: GDPByIndustryBackfillRequest) =>
    apiClient.post<BEATaskResponse>('/bea/actions/backfill/gdpbyindustry', request).then(r => r.data),

  update: (request: UpdateRequest) =>
    apiClient.post<BEATaskResponse>('/bea/actions/update', request).then(r => r.data),

  // Granular update endpoints
  updateNIPA: (request: NIPAUpdateRequest) =>
    apiClient.post<BEATaskResponse>('/bea/actions/update/nipa', request).then(r => r.data),

  updateRegional: (request: RegionalUpdateRequest) =>
    apiClient.post<BEATaskResponse>('/bea/actions/update/regional', request).then(r => r.data),

  updateGDPbyIndustry: (request: GDPByIndustryUpdateRequest) =>
    apiClient.post<BEATaskResponse>('/bea/actions/update/gdpbyindustry', request).then(r => r.data),
};

// ==================== BEA Sentinel Types ====================

export interface BEASentinelStats {
  total: number;
  by_dataset: {
    [key: string]: {
      count: number;
      last_checked: string | null;
      changes_detected: number;
    };
  };
}

export interface BEASentinel {
  sentinel_id: string;
  table_name: string;
  series_code: string | null;
  line_code: number | null;
  geo_fips: string | null;
  industry_code: string | null;
  frequency: string | null;
  selection_reason: string | null;
  last_value: number | null;
  last_year: number | null;
  last_period: string | null;
  last_checked_at: string | null;
  last_changed_at: string | null;
  check_count: number;
  change_count: number;
}

export interface BEASentinelListResponse {
  dataset: string;
  count: number;
  sentinels: BEASentinel[];
}

export interface BEASentinelResponse {
  success: boolean;
  message: string;
  data?: {
    selected?: number;
    total_series?: number;
    percentage?: number;
    checked?: number;
    changed?: number;
    new_data_detected?: boolean;
    changes?: Array<{
      sentinel_id: string;
      table: string;
      old_value: number | null;
      new_value: number | null;
      old_period: string;
      new_period: string;
    }>;
    total_checked?: number;
    total_changed?: number;
    by_dataset?: { [key: string]: any };
  };
}

export const beaSentinelAPI = {
  getStats: () =>
    apiClient.get<BEASentinelStats>('/bea/sentinel/stats').then(r => r.data),

  listSentinels: (dataset: string) =>
    apiClient.get<BEASentinelListResponse>(`/bea/sentinel/list/${dataset}`).then(r => r.data),

  selectSentinels: (dataset: string, frequency: string = 'A') =>
    apiClient.post<BEASentinelResponse>('/bea/sentinel/select', { dataset, frequency }).then(r => r.data),

  checkSentinels: (dataset: string) =>
    apiClient.post<BEASentinelResponse>('/bea/sentinel/check', { dataset }).then(r => r.data),

  checkAllSentinels: () =>
    apiClient.post<BEASentinelResponse>('/bea/sentinel/check-all').then(r => r.data),

  deleteSentinel: (dataset: string, sentinelId: string) =>
    apiClient.delete<BEASentinelResponse>(`/bea/sentinel/${dataset}/${sentinelId}`).then(r => r.data),

  clearSentinels: (dataset: string) =>
    apiClient.delete<BEASentinelResponse>(`/bea/sentinel/${dataset}`).then(r => r.data),
};

export const beaExplorerAPI = {
  // NIPA endpoints
  getNIPATables: (activeOnly = true) =>
    apiClient.get<NIPATable[]>('/bea/explorer/nipa/tables', { params: { active_only: activeOnly } }).then(r => r.data),

  getNIPATable: (tableName: string) =>
    apiClient.get<NIPATable>(`/bea/explorer/nipa/tables/${tableName}`).then(r => r.data),

  getNIPATableSeries: (tableName: string) =>
    apiClient.get<NIPASeries[]>(`/bea/explorer/nipa/tables/${tableName}/series`).then(r => r.data),

  getNIPASeriesData: (seriesCode: string, params?: {
    start_year?: number;
    end_year?: number;
    frequency?: string;
  }) =>
    apiClient.get<NIPATimeSeries>(`/bea/explorer/nipa/series/${seriesCode}/data`, { params }).then(r => r.data),

  // Regional endpoints
  getRegionalTables: (activeOnly = true) =>
    apiClient.get<RegionalTable[]>('/bea/explorer/regional/tables', { params: { active_only: activeOnly } }).then(r => r.data),

  getRegionalTable: (tableName: string) =>
    apiClient.get<RegionalTable>(`/bea/explorer/regional/tables/${tableName}`).then(r => r.data),

  getRegionalTableLineCodes: (tableName: string) =>
    apiClient.get<RegionalLineCode[]>(`/bea/explorer/regional/tables/${tableName}/linecodes`).then(r => r.data),

  getRegionalGeographies: (params?: {
    geo_type?: string;
    search?: string;
    limit?: number;
  }) =>
    apiClient.get<RegionalGeo[]>('/bea/explorer/regional/geographies', { params }).then(r => r.data),

  getRegionalData: (params: {
    table_name: string;
    line_code: number;
    geo_fips: string;
    start_year?: number;
    end_year?: number;
  }) =>
    apiClient.get<RegionalTimeSeries>('/bea/explorer/regional/data', { params }).then(r => r.data),

  compareRegionalData: (params: {
    table_name: string;
    line_code: number;
    geo_fips_list: string;
    year?: string;
  }) =>
    apiClient.get('/bea/explorer/regional/compare', { params }).then(r => r.data),
};
