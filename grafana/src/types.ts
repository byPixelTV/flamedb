import { DataQuery, DataSourceJsonData } from '@grafana/data';
export interface Filter { key: string; value: string }
export interface FlameQuery extends DataQuery {
  metrics: string[];
  filters: Filter[];
  mode: 'timeseries' | 'latest' | 'table';
  aggregation: 'AVG' | 'SUM' | 'COUNT';
  interval: string;
  groupBy: string[];
  legend: string;
}
export interface FlameOptions extends DataSourceJsonData {
  host?: string; port?: number; timeoutSeconds?: number; maxScanEvents?: number;
}
export interface FlameSecureOptions { apiKey?: string }
export const defaults: Omit<FlameQuery, 'refId'> = {
  metrics: [], filters: [], mode: 'timeseries', aggregation: 'AVG', interval: 'auto', groupBy: [], legend: '',
};
