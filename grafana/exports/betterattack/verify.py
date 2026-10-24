"""Independent structure, coverage and collector-semantics checks."""
import json
from pathlib import Path

root = Path(__file__).resolve().parent
load = lambda name: json.loads((root / name).read_text(encoding='utf-8-sig'))
d = load('betterattack-dashboard.json')
def assert_no_null(value, path='root'):
    assert value is not None, f'Null value rejected by the dashboard editor: {path}'
    if isinstance(value, dict):
        for key, item in value.items(): assert_no_null(item, path + '/' + key)
    elif isinstance(value, list):
        for index, item in enumerate(value): assert_no_null(item, path + '/' + str(index))
assert_no_null(d)
original = load('source-dashboard.json')
expected = set(load('supplied-metrics.json')) - {'smp:kills'}
optional = {'system:tick:count', 'system:tick:duration_seconds', 'system:tick:duration_bucket'}
assert d['apiVersion'] == 'dashboard.grafana.app/v2'
assert d['metadata']['name'] == original['metadata']['name']
assert d['spec']['title'] == original['spec']['title']
assert d['spec']['timeSettings']['from'] == original['spec']['timeSettings']['from']
assert d['spec']['preload'] is False
panels = d['spec']['elements']
refs = []
for tab in d['spec']['layout']['spec']['tabs']:
    rectangles = []
    for item in tab['spec']['layout']['spec']['items']:
        v = item['spec']; name = v['element']['name']; refs.append(name)
        assert name in panels
        assert 0 <= v['x'] < 24 and v['x'] + v['width'] <= 24 and v['y'] >= 0
        x, y, w, h = [v[k] for k in ['x', 'y', 'width', 'height']]
        assert w > 0 and h > 0
        for xx, yy, ww, hh in rectangles:
            assert x + w <= xx or xx + ww <= x or y + h <= yy or yy + hh <= y, (tab['spec']['title'], name, 'overlap')
        rectangles.append((x, y, w, h))
assert len(refs) == len(set(refs)) == len(panels) and set(refs) == set(panels)
assert len({p['spec']['id'] for p in panels.values()}) == len(panels)
covered = set()
for name, panel in panels.items():
    sp = panel['spec']; data = sp['data']['spec']; transforms = data['transformations']
    assert sp['title'] and sp['description']
    assert not {'timeFrom', 'timeTo', 'timeShift', 'timeCompare'} & set(data['queryOptions']), (name, 'panel must inherit dashboard time')
    metrics = []
    for pq in data['queries']:
        query = pq['spec']['query']; q = query['spec']; metrics += q['metrics']; covered.update(q['metrics'])
        assert not any(m.startswith('smp:') for m in q['metrics']), (name, 'SMP statistics were removed')
        assert query['group'] == 'flamedb-flamedb-datasource'
        assert query['datasource']['name'] == 'cg24sd0kvre9sd'
        assert len(q['metrics']) <= 16
        if any(m.startswith('system:') for m in q['metrics']):
            assert q['aggregation'] == 'AVG', (name, 'sampled system values must not be summed over time')
            assert not any(t['spec']['options'].get('mode') == 'cumulativeFunctions' for t in transforms)
            if 'system:global-playercount' in q['metrics']:
                assert q['groupBy'] == []
            else:
                assert 'server' in q['groupBy']
            if any(m.startswith('system:gc:') for m in q['metrics']): assert 'gc' in q['groupBy']
            if any(m.endswith('_bucket') for m in q['metrics']): assert 'le' in q['groupBy']
            if any(m.startswith('system:region:') for m in q['metrics']):
                assert {'server', 'world', 'region'} <= set(q['groupBy'])
        else:
            assert q['groupBy'] == []
            assert q['aggregation'] == ('COUNT' if 'playtime:sessions' in q['metrics'] else 'SUM')
            options = transforms[0]['spec']['options']
            assert options['mode'] == 'cumulativeFunctions'
            assert options['cumulative'] == {'field': q['legend'], 'reducer': 'sum'}
            assert options['replaceFields'] is True
    defaults = sp['vizConfig']['spec']['fieldConfig']['defaults']
    if sp['vizConfig']['group'] == 'table':
        overrides = sp['vizConfig']['spec']['fieldConfig']['overrides']
        assert any(o['matcher'].get('options') == '.* time$' and {'id': 'custom.hideFrom.viz', 'value': True} in o['properties'] for o in overrides)
    if sp['vizConfig']['group'] != 'table':
        if any(m.endswith(':tps') or 'damage-' in m for m in metrics): assert defaults.get('decimals') == 2
        if 'system:region:util' in metrics: assert defaults['unit'] == 'percentunit'
        if any(m in ['system:ram:usage', 'system:util', 'system:cpu:system', 'system:regions:util'] for m in metrics): assert defaults['unit'] == 'percent'
        if 'system:process:start_time_seconds' in metrics:
            binary = transforms[0]['spec']['options']['binary']
            assert binary == {'left': {'matcher': {'id': 'byType', 'options': 'number'}}, 'operator': '*', 'right': {'fixed': '1000'}}
            assert defaults['unit'] == 'dateTimeAsIso'
assert expected <= covered, sorted(expected - covered)
assert covered - expected == optional, sorted(covered - expected)
for i in range(1, 8):
    name = 'panel-' + str(i)
    assert panels[name]['spec']['title'] == original['spec']['elements'][name]['spec']['title']
    assert panels[name]['spec']['id'] == i
print(f'Coverage: {len(expected)}/{len(expected)} supplied metrics, {len(optional)} optional Paper metrics, {len(panels)} panels, {len(d["spec"]["layout"]["spec"]["tabs"])} tabs')
print('Dashboard structure and metric semantics passed')
