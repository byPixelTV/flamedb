"""Build the supplied v2 dashboard without changing the datasource identity."""
import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
source = json.loads((ROOT / 'source-dashboard.json').read_text(encoding='utf-8-sig'))
dashboard = copy.deepcopy(source)
spec = dashboard['spec']
DS = source['spec']['elements']['panel-1']['spec']['data']['spec']['queries'][0]['spec']['query']['datasource']['name']
SERVER = ['group', 'server']
WORLD = SERVER + ['world']
REGION = WORLD + ['region']
GC = SERVER + ['gc']
AREA = SERVER + ['area']
LABEL = '{{group}} / {{server}}'
elements = {}
tabs = []
next_id = 8


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def query(metric, ref='A', groups=None, mode='timeseries', aggregation='AVG', legend=None, filters=None):
    groups = SERVER if groups is None else groups
    if legend is None and mode == 'latest' and groups:
        legend = ' / '.join('{{' + k + '}}' for k in groups)
    if legend is None:
        legend = metric + ' · ' + ' / '.join('{{' + k + '}}' for k in groups) if groups else metric
    return {'kind': 'PanelQuery', 'spec': {'refId': ref, 'hidden': False, 'query': {
        'kind': 'DataQuery', 'group': 'flamedb-flamedb-datasource', 'version': 'v0', 'datasource': {'name': DS},
        'spec': {'metrics': [metric], 'filters': filters or [], 'groupBy': groups,
                 'mode': mode, 'aggregation': aggregation, 'interval': '10s', 'legend': legend}}}}


def cumulative(field, alias=None):
    return {'kind': 'Transformation', 'group': 'calculateField', 'spec': {'options': {
        'mode': 'cumulativeFunctions', 'cumulative': {'field': field, 'reducer': 'sum'},
        'alias': alias or field, 'replaceFields': True}}}


def unit_for(metric):
    if metric in ['system:util', 'system:cpu:system', 'system:ram:usage', 'system:regions:util']:
        return 'percent'
    if metric == 'system:region:util': return 'percentunit'
    if metric.endswith(':mspt') or metric == 'system:mspt': return 'ms'
    if ':ram:' in metric or ':jvm:memory:' in metric or metric.endswith('freed_bytes'): return 'bytes'
    if metric.endswith('_seconds') or metric == 'system:cpu:seconds': return 's'
    return 'short'


def thresholds(metric):
    if metric.endswith(':tps') or metric == 'system:tps':
        return {'mode': 'absolute', 'steps': [{'color': 'red', 'value': 0}, {'color': 'orange', 'value': 17}, {'color': 'green', 'value': 19}]}
    if metric.endswith(':mspt') or metric == 'system:mspt':
        return {'mode': 'absolute', 'steps': [{'color': 'green', 'value': 0}, {'color': 'orange', 'value': 40}, {'color': 'red', 'value': 50}]}
    return {'mode': 'absolute', 'steps': [{'color': 'green', 'value': 0}]}


class Tab:
    def __init__(self, title):
        self.items = []; self.x = 0; self.y = 0; self.row_height = 0
        tabs.append({'kind': 'TabsLayoutTab', 'spec': {'title': title, 'layout': {'kind': 'GridLayout', 'spec': {'items': self.items}}}})

    def place(self, panel, width=12, height=8):
        if self.x + width > 24:
            self.y += self.row_height; self.x = 0; self.row_height = 0
        name = 'panel-' + str(panel['spec']['id']); elements[name] = panel
        self.items.append({'kind': 'GridLayoutItem', 'spec': {
            'element': {'kind': 'ElementReference', 'name': name},
            'x': self.x, 'y': self.y, 'width': width, 'height': height}})
        self.x += width; self.row_height = max(self.row_height, height)
        if self.x == 24:
            self.y += self.row_height; self.x = 0; self.row_height = 0
        return panel


def panel(tab, title, metrics, viz='timeseries', unit=None, groups=None, description='', width=12, height=8, filters=None, mode=None, transforms=None):
    global next_id
    metrics = [metrics] if isinstance(metrics, str) else metrics
    mode = mode or ('table' if viz == 'table' else 'latest' if viz == 'stat' else 'timeseries')
    qs = [query(m, chr(65 + i), groups=groups, mode=mode, filters=filters) for i, m in enumerate(metrics)]
    if mode == 'table':
        qs = [query(metrics[0], groups=groups, mode=mode, filters=filters)]
        qs[0]['spec']['query']['spec']['metrics'] = metrics
    defaults = {'unit': unit or unit_for(metrics[0]), 'color': {'mode': 'palette-classic'}, 'noValue': '—'}
    if defaults['unit'] == 'short': defaults['decimals'] = 2 if any(m.endswith(':tps') or 'damage-' in m for m in metrics) else 0
    elif defaults['unit'] in ['ms', 'percent', 'percentunit']: defaults['decimals'] = 2
    overrides = []
    if viz == 'timeseries':
        defaults['min'] = 0
        defaults['custom'] = {'drawStyle': 'line', 'lineInterpolation': 'linear', 'lineWidth': 2,
                              'fillOpacity': 8, 'showPoints': 'never', 'spanNulls': False,
                              'axisPlacement': 'auto', 'stacking': {'mode': 'none', 'group': 'A'}}
        options = {'legend': {'displayMode': 'table', 'placement': 'bottom', 'calcs': ['lastNotNull']}, 'tooltip': {'mode': 'multi', 'sort': 'desc'}}
    elif viz == 'stat':
        defaults.update({'color': {'mode': 'thresholds'}, 'thresholds': thresholds(metrics[0])})
        options = {'colorMode': 'background', 'graphMode': 'none', 'justifyMode': 'auto', 'orientation': 'auto', 'textMode': 'auto',
                   'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}, 'wideLayout': True}
    else:
        defaults = {'custom': {'align': 'auto'}, 'noValue': '—'}
        options = {'showHeader': True, 'cellHeight': 'sm', 'footer': {'show': False}}
        overrides.append({'matcher': {'id': 'byRegexp', 'options': '.* time$'}, 'properties': [{'id': 'custom.hideFrom.viz', 'value': True}]})
        for tag, column_width in [('server', 120), ('world', 110), ('region', 65)]:
            overrides.append({'matcher': {'id': 'byName', 'options': tag}, 'properties': [{'id': 'custom.width', 'value': column_width}]})
        for metric in metrics:
            properties = [{'id': 'unit', 'value': unit_for(metric)}, {'id': 'displayName', 'value': metric.replace('system:region:', '').replace('system:world:', '').replace('system:', '').replace(':', ' / ')}]
            if metric.endswith(':tps') or metric.endswith(':mspt') or metric in ['system:tps', 'system:mspt']:
                properties += [{'id': 'decimals', 'value': 2}, {'id': 'custom.cellOptions', 'value': {'type': 'color-background'}}, {'id': 'thresholds', 'value': thresholds(metric)}]
            overrides.append({'matcher': {'id': 'byName', 'options': metric}, 'properties': properties})
    p = {'kind': 'Panel', 'spec': {'id': next_id, 'title': title,
        'description': description + ' Uses the selected dashboard time range.', 'links': [],
        'data': {'kind': 'QueryGroup', 'spec': {'queries': qs, 'transformations': transforms or [], 'queryOptions': {'maxDataPoints': 1000, 'interval': '10s'}}},
        'vizConfig': {'kind': 'VizConfig', 'group': viz, 'version': '13.2.4', 'spec': {'fieldConfig': {'defaults': defaults, 'overrides': overrides}, 'options': options}}}}
    next_id += 1
    return tab.place(p, width, height)


server = Tab('Server stats')
for title, metric in [('Players · network', 'system:global-playercount'), ('Players · servers', 'system:players'),
                      ('TPS · servers', 'system:tps'), ('MSPT · servers', 'system:mspt'),
                      ('Process CPU', 'system:util'), ('Heap used', 'system:ram:used'),
                      ('Uptime', 'system:process:uptime_seconds'), ('Folia regions', 'system:regions')]:
    panel(server, title, metric, 'stat', groups=[] if metric == 'system:global-playercount' else SERVER, width=6, height=4,
          description='Latest observation per server; network player count is already global and is never summed across servers.')
for title, metrics, unit in [
    ('TPS · Folia minimum / Paper server', 'system:tps', 'short'),
    ('MSPT · Folia maximum / Paper server', 'system:mspt', 'ms'),
    ('Players and slots · per server', ['system:players', 'system:players:max'], 'short'),
    ('CPU · process and host', ['system:util', 'system:cpu:system'], 'percent'),
    ('Chunks and entities · per server', ['system:chunks', 'system:entities'], 'short'),
    ('Players · whole network', 'system:global-playercount', 'short')]:
    panel(server, title, metrics, unit=unit, groups=[] if metrics == 'system:global-playercount' else SERVER,
          description='Sampled gauges averaged only within time buckets, with separate series per server. Folia TPS is the minimum region TPS; Folia MSPT the maximum region MSPT.')
panel(server, 'Server inventory · latest', ['system:players', 'system:players:max', 'system:plugins', 'system:cpu:cores',
      'system:chunks', 'system:entities', 'system:regions', 'system:process:uptime_seconds'], 'table', groups=SERVER, width=24, height=9)
panel(server, 'Process CPU time · since JVM start', 'system:cpu:seconds', unit='s', description='Already cumulative CPU seconds; may reset after a JVM restart. Never sum repeated samples.')
start_field = 'system:process:start_time_seconds · ' + LABEL
start_transform = {'kind': 'Transformation', 'group': 'calculateField', 'spec': {'options': {
    'mode': 'binary', 'binary': {'left': {'matcher': {'id': 'byType', 'options': 'number'}}, 'operator': '*', 'right': {'fixed': '1000'}}, 'alias': 'Process start', 'replaceFields': True}}}
panel(server, 'Process start time', 'system:process:start_time_seconds', 'stat', unit='dateTimeAsIso', width=12, height=8,
      transforms=[start_transform], description='Collector stores Unix seconds. Multiplied by 1000 for Grafana datetime milliseconds.')

players = Tab('Player Stats')
game_metrics = [
    ('craftattack:deaths', 'Deaths', 1), ('craftattack:blocks-broken', 'Blocks Broken', 2),
    ('craftattack:kills', 'Kills', 3), ('craftattack:blocks-placed', 'Blocks Placed', 4),
    ('craftattack:damage-dealt', 'Damage Dealt', 5), ('craftattack:damage-taken', 'Damage Taken', 6),
    ('craftattack:mob-kills', 'Mob Kills', 7), ('craftattack:clans:joins', 'Clan Joins', None),
    ('playtime:sessions', 'Session Events', None)]
for metric, title, original in game_metrics:
    q = query(metric, groups=[], aggregation='COUNT' if metric == 'playtime:sessions' else 'SUM', legend=title)
    q['spec']['query']['spec']['interval'] = '1m'
    description = 'Cumulative total inside the selected dashboard range, not an all-time baseline. '
    description += 'Counts records, not playtime seconds; the provided system collector does not define the session payload.' if metric == 'playtime:sessions' else 'Assumes writes contain event increments (damage metrics contain damage amounts).'
    if original:
        p = copy.deepcopy(source['spec']['elements']['panel-' + str(original)])
        p['spec']['description'] = description
        p['spec']['data']['spec'].update({'queries': [q], 'transformations': [cumulative(title)], 'queryOptions': {'interval': '1m', 'maxDataPoints': 1000}})
        p['spec']['vizConfig']['spec']['fieldConfig']['defaults']['unit'] = 'short'
        if 'damage-' in metric: p['spec']['vizConfig']['spec']['fieldConfig']['defaults']['decimals'] = 2
        players.place(p, 6, 5)
    else:
        p = panel(players, title, metric, 'stat', groups=[], width=6, height=5, mode='timeseries')
        p['spec']['description'] = description
        p['spec']['data']['spec'] = {'queries': [q], 'transformations': [cumulative(title)], 'queryOptions': {'interval': '1m', 'maxDataPoints': 1000}}
        p['spec']['vizConfig']['spec']['options']['graphMode'] = 'area'
if players.x:
    players.y += players.row_height; players.x = 0; players.row_height = 0
for metric, title, _ in game_metrics:
    p = panel(players, title + ' · cumulative', metric, groups=[], width=12, height=8)
    p['spec']['description'] = 'Running total from the selected dashboard range start. Does not include earlier history. Session Events counts records; other game metrics sum event increments.'
    q = query(metric, groups=[], aggregation='COUNT' if metric == 'playtime:sessions' else 'SUM', legend=title)
    q['spec']['query']['spec']['interval'] = '1m'
    p['spec']['data']['spec'] = {'queries': [q], 'transformations': [cumulative(title)], 'queryOptions': {'interval': '1m', 'maxDataPoints': 1000}}

jvm = Tab('JVM & Memory')
for title, metrics, unit, filters in [
    ('Heap · used / allocated / max', ['system:ram:used', 'system:ram:allocated', 'system:ram:max'], 'bytes', None),
    ('Heap usage', 'system:ram:usage', 'percent', None),
    ('JVM heap memory', ['system:jvm:memory:used', 'system:jvm:memory:committed', 'system:jvm:memory:max', 'system:jvm:memory:init'], 'bytes', [{'key': 'area', 'value': 'heap'}]),
    ('JVM non-heap memory', ['system:jvm:memory:used', 'system:jvm:memory:committed', 'system:jvm:memory:max', 'system:jvm:memory:init'], 'bytes', [{'key': 'area', 'value': 'nonheap'}]),
    ('Threads · current / daemon / peak', ['system:threads:current', 'system:threads:daemon', 'system:threads:peak'], 'short', None),
    ('Threads started · since JVM start', 'system:threads:started', 'short', None)]:
    panel(jvm, title, metrics, unit=unit, filters=filters,
          description='Periodic JVM samples, not event increments. Started/peak counters may reset after JVM restart. Undefined non-heap max/init values are omitted by the collector.')
panel(jvm, 'JVM memory areas · latest', ['system:jvm:memory:used', 'system:jvm:memory:committed', 'system:jvm:memory:max', 'system:jvm:memory:init'], 'table', groups=AREA, width=24, height=8)
panel(jvm, 'Threads · latest', ['system:threads:current', 'system:threads:daemon', 'system:threads:peak', 'system:threads:started'], 'table', groups=SERVER, width=24, height=8)

GCtab = Tab('Garbage collection')
for title, metric, unit in [
    ('GC collections · JVM counters', 'system:gc:collections', 'short'),
    ('GC duration · JVM counters', 'system:gc:duration_seconds', 's'),
    ('GC freed bytes · notification tracker', 'system:gc:freed_bytes', 'bytes'),
    ('GC notifications · count', 'system:gc:notification:count', 'short'),
    ('GC notification duration · total', 'system:gc:notification:duration_seconds', 's'),
    ('GC notification freed bytes · total', 'system:gc:notification:freed_bytes', 'bytes')]:
    panel(GCtab, title, metric, unit=unit, groups=GC,
          description='Already cumulative per JVM/collector lifetime; resets are possible. Separate server and gc series. An empty gc tag is the collector-emitted total; never add it to the individual collectors.')
for title, metric, meaning in [
    ('GC duration buckets · raw cumulative counts', 'system:gc:duration_bucket', 'Number of observed GC notifications with duration <= le seconds.'),
    ('GC freed-byte buckets · raw cumulative counts', 'system:gc:freed_bucket', 'Number of observed GC notifications with freed bytes <= le bytes.')]:
    panel(GCtab, title, metric, unit='short', groups=GC + ['le'],
          description=meaning + ' Buckets already overlap cumulatively. Do not sum le buckets or cumulatively sum periodic snapshots. These are raw counters, not calculated percentiles.')
panel(GCtab, 'GC JVM counters · latest', ['system:gc:collections', 'system:gc:duration_seconds', 'system:gc:freed_bytes'], 'table', groups=GC, width=24, height=8)
panel(GCtab, 'GC notification counters · latest', ['system:gc:notification:count', 'system:gc:notification:duration_seconds', 'system:gc:notification:freed_bytes'], 'table', groups=GC, width=24, height=8)

worlds = Tab('Worlds & Regions')
panel(worlds, 'Regions · latest observations', ['system:region:tps', 'system:region:mspt', 'system:region:online_players', 'system:region:chunks', 'system:region:entities', 'system:region:util'], 'table', groups=REGION, width=24, height=12,
      description='One row per server, world and region. Last value per metric, not a sum of repeated samples. Missing metrics remain null. Timestamp columns can be shown in field overrides.')
for title, metric in [('Region TPS', 'system:region:tps'), ('Region MSPT', 'system:region:mspt'), ('Region utilization', 'system:region:util')]:
    panel(worlds, title, metric, groups=REGION,
          description='Individual Folia region series. Utilization is a 0–1 fraction and uses Grafana percentunit.')
panel(worlds, 'Folia region count · per server', 'system:regions')
panel(worlds, 'Folia region utilization · sum (%)', 'system:regions:util',
      description='The collector sums region utilization and multiplies by 100. This can exceed 100%; it is not normalized CPU usage.')
panel(worlds, 'Folia regions · per world', ['system:regions:world', 'system:regions:world_nether', 'system:regions:world_the_end'], groups=WORLD,
      description='Explicit world metric names from the supplied list. For custom worlds, add their system:regions:<world-name> metric in the builder.')
panel(worlds, 'Worlds · latest observations', ['system:world:players', 'system:world:chunks', 'system:world:entities'], 'table', groups=WORLD, width=24, height=9)
for title, metric in [('World players', 'system:world:players'), ('World chunks', 'system:world:chunks'), ('World entities', 'system:world:entities')]:
    panel(worlds, title, metric, groups=WORLD)

events = Tab('Server events')
for title, name in [('Logins', 'login'), ('Joins', 'join'), ('Quits', 'quit'), ('Chat messages', 'chat'), ('Server-list pings', 'ping')]:
    panel(events, title + ' · since collector start', 'system:events:' + name,
          description='The collector periodically records LongAdder.sum(): already cumulative, not an increment. Display unchanged, separated by server. Resets after MetricsSession restart are expected.')
panel(events, 'Event counters · latest', ['system:events:' + x for x in ['login', 'join', 'quit', 'chat', 'ping']], 'table', groups=SERVER, width=24, height=9)

paper = Tab('Paper ticks')
for title, metric, unit, groups in [('Ticks · since collector start', 'system:tick:count', 'short', SERVER),
                                   ('Tick duration · cumulative seconds', 'system:tick:duration_seconds', 's', SERVER),
                                   ('Tick duration buckets · raw cumulative counts', 'system:tick:duration_bucket', 'short', SERVER + ['le'])]:
    panel(paper, title, metric, unit=unit, groups=groups,
          description='Optional metric emitted by PaperTickHistogram only on non-Folia Paper. Empty on Folia. Already cumulative snapshots. Bucket le bounds are seconds; bucket values count ticks. No percentiles are inferred.')

spec['elements'] = elements
spec['layout'] = {'kind': 'TabsLayout', 'spec': {'tabs': tabs}}
spec['tags'] = sorted(set(spec.get('tags', []) + ['flamedb', 'betterattack', 'moonlight']))
spec['description'] = 'Moonlight server, JVM, GC, Folia/Paper and gameplay metrics. Game totals are cumulative within the selected range. System counters retain collector lifetime semantics. All panels follow the selected dashboard time range without panel-specific time overrides.'
spec['timeSettings']['autoRefresh'] = '30s'
write_json(ROOT / 'betterattack-dashboard.json', dashboard)
print(f'Generated {len(elements)} panels across {len(tabs)} tabs; datasource {DS}')
