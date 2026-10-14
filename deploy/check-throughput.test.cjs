const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const root = fs.realpathSync(os.tmpdir());
const fixture = fs.mkdtempSync(path.join(root, 'flamedb-throughput-controls-'));
try {
  const actual = fs.realpathSync(fixture);
  if (!actual.startsWith(root + path.sep)) throw new Error('Unexpected temporary fixture path.');
  const latest = path.join(actual, 'latest.json');
  fs.writeFileSync(latest, JSON.stringify({run_path: actual}));
  fs.writeFileSync(path.join(actual, 'environment.json'), JSON.stringify({addresses:'api:7777,api-2:7777,api-3:7777'}));
  const base = {
    mode:'mixed',elapsed_seconds_including_drain:100,correctness_pass:true,all_entity_scores_verified:true,
    errors:0,events_ambiguous:0,events_confirmed:1600000000,events_submitted:1600000000,
    reads_confirmed:4000000,readback_events:1600000000,requests_confirmed:20000000,
    requests_submitted:20000000,confirmed_requests_per_second:200000,confirmed_events_per_second:16000000
  };
  const cases = [
    {name:'synthetic-positive-control',changes:{},pass:true},
    // Correctly measured 200k events/s is only 2,000 requests/s at batch 100.
    {name:'events-are-not-requests',changes:{requests_confirmed:200000,requests_submitted:200000,confirmed_requests_per_second:2000,confirmed_events_per_second:200000},pass:false},
    {name:'invented-request-rate',changes:{requests_confirmed:200000,requests_submitted:200000},pass:false},
    {name:'unconfirmed-submissions',changes:{requests_submitted:20000001},pass:false},
    {name:'unknown-writes',changes:{events_ambiguous:1},pass:false}
  ];
  for (const c of cases) {
    const mixed = {...base,...c.changes};
    fs.writeFileSync(path.join(actual,'summary.json'),JSON.stringify({mixed,recovery:{recovery_verified:true,readback_events:mixed.events_confirmed}}));
    const r = spawnSync(process.execPath,[path.join(__dirname,'check-throughput.cjs'),latest],{encoding:'utf8'});
    if (r.error || (r.status === 0) !== c.pass || (c.pass && !r.stdout.includes('REQUEST_THROUGHPUT_TARGET_VERIFIED'))) {
      throw new Error(`${c.name}: unexpected outcome ${r.status}: ${r.stdout} ${r.stderr}`);
    }
  }
  console.log('THROUGHPUT_ORACLE_CONTROLS_VERIFIED');
} finally {
  const actual = fs.realpathSync(fixture);
  if (!actual.startsWith(root + path.sep) || path.basename(actual).indexOf('flamedb-throughput-controls-') !== 0) throw new Error('Unsafe fixture cleanup path.');
  fs.rmSync(actual,{recursive:true,force:true});
}
