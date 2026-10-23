import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { QueryEditor } from './QueryEditor';
import { ConfigEditor } from './ConfigEditor';
import { FlameOptions, FlameQuery } from './types';
export const plugin = new DataSourcePlugin<DataSource, FlameQuery, FlameOptions>(DataSource)
  .setConfigEditor(ConfigEditor).setQueryEditor(QueryEditor);
