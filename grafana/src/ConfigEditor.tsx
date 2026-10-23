import React from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { FlameOptions, FlameSecureOptions } from './types';

type Props = DataSourcePluginOptionsEditorProps<FlameOptions, FlameSecureOptions>;
export function ConfigEditor({ options, onOptionsChange }: Props) {
  const change = (patch: Partial<FlameOptions>) => onOptionsChange({ ...options, jsonData: { ...options.jsonData, ...patch } });
  return <div>
    <InlineField label="Host" labelWidth={20} tooltip="Hostname reachable from the Grafana server, not from your browser."><Input width={48} value={options.jsonData.host ?? '127.0.0.1'} onChange={e => change({ host: e.currentTarget.value })} /></InlineField>
    <InlineField label="TCP port" labelWidth={20}><Input width={16} type="number" min={1} max={65535} value={options.jsonData.port ?? 7777} onChange={e => change({ port: Number(e.currentTarget.value) })} /></InlineField>
    <InlineField label="Read-only API key" labelWidth={20}><SecretInput width={48} value={options.secureJsonData?.apiKey ?? ''} isConfigured={Boolean(options.secureJsonFields?.apiKey)} onChange={e => onOptionsChange({ ...options, secureJsonData: { ...options.secureJsonData, apiKey: e.currentTarget.value } })} onReset={() => onOptionsChange({ ...options, secureJsonFields: { ...options.secureJsonFields, apiKey: false }, secureJsonData: { ...options.secureJsonData, apiKey: '' } })} /></InlineField>
    <InlineField label="Timeout (seconds)" labelWidth={20}><Input width={16} type="number" min={1} max={120} value={options.jsonData.timeoutSeconds ?? 10} onChange={e => change({ timeoutSeconds: Number(e.currentTarget.value) })} /></InlineField>
    <InlineField label="Scan limit per metric" labelWidth={20} tooltip="Bounds grouped queries and tables. Queries exceeding this limit return an error instead of partial data."><Input width={20} type="number" min={100} max={200000} value={options.jsonData.maxScanEvents ?? 50000} onChange={e => change({ maxScanEvents: Number(e.currentTarget.value) })} /></InlineField>
  </div>;
}
