import React, { useEffect, useState } from 'react';
import { QueryEditorProps } from '@grafana/data';
import { Alert, Button, InlineField, InlineFieldRow, Input, MultiSelect, Select } from '@grafana/ui';
import { DataSource } from './datasource';
import { FlameOptions, FlameQuery, defaults } from './types';

type Props = QueryEditorProps<DataSource, FlameQuery, FlameOptions>;
const options = (values: string[]) => values.map(value => ({ label: value, value }));

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const q = { ...defaults, ...query };
  const [metrics, setMetrics] = useState<string[]>([]);
  const [tags, setTags] = useState<Record<string, string[]>>({});
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let active = true;
    datasource.metrics().then(v => { if (active) { setMetrics(v); setError(''); } }).catch(e => { if (active) { setError(e.message ?? 'Metric discovery failed'); } });
    return () => { active = false; };
  }, [datasource, revision]);
  const primary = q.metrics[0];
  useEffect(() => {
    let active = true; setTags({});
    if (primary) { datasource.tags(primary).then(v => { if (active) { setTags(v); } }).catch(e => { if (active) { setError(e.message ?? 'Tag discovery failed'); } }); }
    return () => { active = false; };
  }, [datasource, primary, revision]);
  const update = (patch: Partial<FlameQuery>, run = true) => { onChange({ ...q, ...patch }); if (run) { onRunQuery(); } };
  const filter = (index: number, patch: Partial<FlameQuery['filters'][number]>) => update({ filters: q.filters.map((f, i) => i === index ? { ...f, ...patch } : f) }, false);
  return <div>
    {error && <Alert title="Discovery unavailable" severity="warning">{error}. You can enter metric and tag names manually.</Alert>}
    <InlineFieldRow>
      <InlineField label="Metrics" labelWidth={16} grow>
        <MultiSelect aria-label="Metrics" options={options(metrics)} value={q.metrics} allowCustomValue placeholder="Choose metrics" onChange={v => update({ metrics: v.map(x => x.value!).filter(Boolean) })} />
      </InlineField>
      <Button variant="secondary" icon="sync" aria-label="Refresh metric discovery" onClick={() => setRevision(v => v + 1)}>Refresh</Button>
    </InlineFieldRow>
    {q.filters.map((f, index) => <InlineFieldRow key={index}>
      <InlineField label={index === 0 ? 'Filters' : 'AND'} labelWidth={16}>
        <Select aria-label={`Filter ${index + 1} tag`} width={25} options={options(Object.keys(tags))} value={f.key} allowCustomValue onChange={v => filter(index, { key: v.value ?? '', value: '' })} />
      </InlineField>
      <InlineField label="="><Select aria-label={`Filter ${index + 1} value`} width={32} options={options(tags[f.key] ?? [])} value={f.value} allowCustomValue onChange={v => filter(index, { value: v.value ?? '' })} /></InlineField>
      <Button variant="secondary" icon="trash-alt" aria-label={`Remove filter ${index + 1}`} onClick={() => update({ filters: q.filters.filter((_, i) => i !== index) })} />
    </InlineFieldRow>)}
    <InlineFieldRow><InlineField label="" labelWidth={16}><Button variant="secondary" icon="plus" onClick={() => update({ filters: [...q.filters, { key: '', value: '' }] }, false)}>Add filter</Button></InlineField></InlineFieldRow>
    <InlineFieldRow>
      <InlineField label="Output" labelWidth={16}><Select aria-label="Output" width={32} value={q.mode} options={[
        { label: 'Time series', value: 'timeseries' }, { label: 'Latest value per group', value: 'latest' }, { label: 'Table: latest values per group', value: 'table' },
      ]} onChange={v => update({ mode: v.value as FlameQuery['mode'] })} /></InlineField>
      {q.mode === 'timeseries' && <>
        <InlineField label="Calculation"><Select aria-label="Calculation" width={22} value={q.aggregation} options={[
          { label: 'Average', value: 'AVG' }, { label: 'Sum', value: 'SUM' }, { label: 'Count events', value: 'COUNT' },
        ]} onChange={v => update({ aggregation: v.value as FlameQuery['aggregation'] })} /></InlineField>
        <InlineField label="Interval"><Select aria-label="Interval" width={20} value={q.interval} allowCustomValue options={options(['auto', '1s', '5s', '10s', '30s', '1m', '5m', '1h'])} onChange={v => update({ interval: v.value ?? 'auto' })} /></InlineField>
      </>}
    </InlineFieldRow>
    <InlineFieldRow><InlineField label="Group by tags" labelWidth={16} grow tooltip="Create one series or table row for each combination of tag values.">
      <MultiSelect aria-label="Group by tags" options={options(Object.keys(tags))} value={q.groupBy} allowCustomValue onChange={v => update({ groupBy: v.map(x => x.value!).filter(Boolean) })} />
    </InlineField></InlineFieldRow>
    {q.mode !== 'table' && <InlineFieldRow><InlineField label="Legend" labelWidth={16} grow tooltip="Use {{metric}} and tag placeholders such as {{server}}.">
      <Input aria-label="Legend" placeholder="{{metric}} · {{server}}" value={q.legend} onChange={e => update({ legend: e.currentTarget.value }, false)} onBlur={onRunQuery} />
    </InlineField></InlineFieldRow>}
    <div style={{ color: 'var(--text-secondary)', margin: '8px 0' }}>Tag suggestions use the latest 1,000 events of the first metric. Custom names and values are supported. Latest values respect the dashboard time range.</div>
    <Button onClick={onRunQuery}>Run query</Button>
  </div>;
}

