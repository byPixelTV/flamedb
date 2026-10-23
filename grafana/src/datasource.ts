import { DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { FlameQuery, FlameOptions, defaults } from './types';

export class DataSource extends DataSourceWithBackend<FlameQuery, FlameOptions> {
  constructor(settings: DataSourceInstanceSettings<FlameOptions>) { super(settings); }
  getDefaultQuery(): Partial<FlameQuery> { return defaults; }
  applyTemplateVariables(query: FlameQuery, scopedVars: ScopedVars): FlameQuery {
    const replace = (v: string) => getTemplateSrv().replace(v, scopedVars, 'raw');
    return { ...defaults, ...query,
      metrics: (query.metrics ?? []).map(replace),
      filters: (query.filters ?? []).map(f => ({ key: f.key, value: replace(f.value) })),
      legend: replace(query.legend ?? ''),
    };
  }
  filterQuery(query: FlameQuery): boolean { return !query.hide && query.metrics?.length > 0; }
  async metrics(): Promise<string[]> { return this.getResource('metrics'); }
  async tags(metric: string): Promise<Record<string, string[]>> { return this.getResource('tags', { metric }); }
}

