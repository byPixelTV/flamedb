// The target remains a failing acceptance gate until an actual sustained mixed
// workload supplies enough confirmed requests. Batch events are never requests.
const fs = require('node:fs');
const path = require('node:path');
const latest = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const summary = JSON.parse(fs.readFileSync(path.join(latest.run_path, 'summary.json'), 'utf8'));
const env = JSON.parse(fs.readFileSync(path.join(latest.run_path, 'environment.json'), 'utf8'));
const r = summary.mixed;
if (!r || r.mode !== 'mixed' || env.addresses.split(',').length !== 3 || r.elapsed_seconds_including_drain < 60 ||
    !r.correctness_pass || !r.all_entity_scores_verified || r.errors !== 0 || r.events_ambiguous !== 0 ||
    r.events_confirmed <= 0 || r.reads_confirmed <= 0 || r.events_confirmed !== r.readback_events ||
    r.events_confirmed !== summary.recovery.readback_events || !summary.recovery.recovery_verified ||
    r.requests_confirmed !== r.requests_submitted || r.events_confirmed !== r.events_submitted) {
  throw new Error('Missing independently verified sustained three-API mixed workload and restart recovery.');
}
const rate = r.requests_confirmed / r.elapsed_seconds_including_drain;
if (!Number.isFinite(rate) || Math.abs(rate-r.confirmed_requests_per_second) > .000001) throw new Error('Inconsistent request-rate measurement.');
console.log(`Observed ${rate.toFixed(2)} confirmed requests/s; ${r.confirmed_events_per_second.toFixed(2)} events/s; target 200000 requests/s.`);
if (rate < 200000) { console.error('Request throughput target remains unmet.'); process.exit(1); }
console.log('REQUEST_THROUGHPUT_TARGET_VERIFIED');
