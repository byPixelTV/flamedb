[CmdletBinding()]
param(
  [string]$Container = 'flamedb-nextgen-postgres-1',
  [string]$OutputDirectory = '.compat/profile-ingestion-before',
  [ValidateRange(1,20)][int]$Repeats = 5,
  [ValidateRange(100,10000)][int]$Rows = 10000,
  [switch]$LegacyTriggerDispatch,
  [switch]$ExistingScoresAndTags,
  [switch]$ExplicitTagConflict
)
$ErrorActionPreference = 'Stop'
$docker = (Get-Command docker.exe -CommandType Application).Source
$info = (& $docker inspect $Container | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0 -or -not $info.State.Running -or
    $info.Config.Labels.'com.docker.compose.project' -ne 'flamedb-nextgen' -or
    $info.Config.Labels.'com.docker.compose.service' -ne 'postgres') {
  throw 'Profiling requires the running, owned development PostgreSQL fixture.'
}
$output = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $output -Force | Out-Null
$prefix = 'profile_' + [guid]::NewGuid().ToString('N')
$engineSource = Get-Content -LiteralPath 'internal/analytics/engine.go' -Raw
$schemaMatch = [regex]::Match($engineSource, 'const schema = `([\s\S]*?)`')
if (-not $schemaMatch.Success -or $prefix -notmatch '^profile_[a-f0-9]{32}$') { throw 'Missing schema source or invalid private fixture name.' }
$identitySchema = Get-Content -LiteralPath 'internal/analytics/large_keys.sql' -Raw
if ($LegacyTriggerDispatch) {
  $guards = [regex]::Matches($identitySchema, ' WHEN .*? EXECUTE FUNCTION')
  if ($guards.Count -ne 5) { throw 'Expected exactly five current trigger guards for the isolated baseline.' }
  $identitySchema = [regex]::Replace($identitySchema, ' WHEN .*? EXECUTE FUNCTION', ' EXECUTE FUNCTION')
}
$initialization = @"
CREATE SCHEMA "$prefix";
SET search_path TO "$prefix",public;
$($schemaMatch.Groups[1].Value)
$identitySchema
SELECT create_hypertable('flame_events','ts',chunk_time_interval=>86400000000000::bigint);
"@
# Only this private schema receives data. Never profile mutations in public.
try {
$initialization | & $docker exec -i $info.Id psql -U flamedb -d flamedb -X -q -t -A -v ON_ERROR_STOP=1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not initialize private profiling schema.' }
$statements = @{
  events = @'
INSERT INTO flame_events(metric,ts,value,tags)
SELECT metric,ts,value,tags FROM (SELECT DISTINCT ON(metric,ts) metric,ts,value,tags
FROM flame_ingest WHERE octet_length(metric)<=2000 ORDER BY metric,ts,ord DESC) chosen
ON CONFLICT(metric,ts) WHERE octet_length(metric)<=2000
DO UPDATE SET value=excluded.value,tags=excluded.tags
'@
  tags = @'
INSERT INTO flame_tag_values(metric,tag,value)
SELECT DISTINCT metric,t.key,t.value FROM flame_ingest CROSS JOIN LATERAL jsonb_each_text(tags) t
ORDER BY metric,t.key,t.value ON CONFLICT DO NOTHING
'@
  scores = @'
INSERT INTO flame_scores(metric,board,entity,value)
SELECT metric,board,entity,value FROM flame_score_ingest
WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000
ON CONFLICT(metric,board,entity) WHERE octet_length(metric)+octet_length(board)+octet_length(entity)<=2000
DO UPDATE SET value=excluded.value
'@
}
if ($ExplicitTagConflict) {
  $statements.tags = $statements.tags.Replace('ORDER BY metric,t.key,t.value','WHERE octet_length(metric)+octet_length(t.key)+octet_length(t.value)<=2000 ORDER BY metric,t.key,t.value').Replace('ON CONFLICT DO NOTHING','ON CONFLICT(metric,tag,value) WHERE octet_length(metric)+octet_length(tag)+octet_length(value)<=2000 DO NOTHING')
  $statements['tags-large'] = @'
INSERT INTO flame_tag_values(metric,tag,value)
SELECT DISTINCT metric,t.key,t.value FROM flame_ingest CROSS JOIN LATERAL jsonb_each_text(tags) t
WHERE octet_length(metric)+octet_length(t.key)+octet_length(t.value)>2000
ORDER BY metric,t.key,t.value ON CONFLICT(row_id) DO NOTHING
'@
}
if ($ExistingScoresAndTags) {
  $seed = @"
SET search_path TO "$prefix",public;
CREATE TEMP TABLE flame_ingest(ord bigint,metric text,ts bigint,value double precision,tags jsonb);
INSERT INTO flame_ingest SELECT i,'${prefix}_'||(i%16),1791849600000000000+i,1,
jsonb_build_object('player','entity'||(i%1000),'source','profile') FROM generate_series(1,$Rows) i;
CREATE TEMP TABLE flame_score_ingest(metric text,board text,entity text,value double precision);
INSERT INTO flame_score_ingest SELECT metric,'',tags->>'player',sum(value) FROM flame_ingest GROUP BY metric,tags->>'player';
$($statements.tags);
$($statements.scores);
"@
  $seed | & $docker exec -i $info.Id psql -U flamedb -d flamedb -X -q -t -A -v ON_ERROR_STOP=1 | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'Private steady-state fixture initialization failed.' }
}
$observations = @()
$phases = @('events','tags','scores')
if ($ExplicitTagConflict) { $phases = @('events','tags','tags-large','scores') }
foreach ($phase in $phases) {
  for ($iteration = 0; $iteration -le $Repeats; $iteration++) {
    # Each statement uses a private schema with the actual application schema
    # and Timescale chunk/index/trigger definitions. Iteration zero warms it.
    # Generated values are fixed numeric constants and a private UUID prefix.
    $sql = @"
BEGIN;
SET LOCAL search_path TO "$prefix",public;
SET LOCAL statement_timeout='30s';
SET LOCAL synchronous_commit=on;
CREATE TEMP TABLE flame_ingest(ord bigint,metric text,ts bigint,value double precision,tags jsonb);
INSERT INTO flame_ingest SELECT i,'${prefix}_'||(i%16),1791849600000000000+i,1,
jsonb_build_object('player','entity'||(i%1000),'source','profile') FROM generate_series(1,$Rows) i;
CREATE TEMP TABLE flame_score_ingest(metric text,board text,entity text,value double precision);
INSERT INTO flame_score_ingest SELECT metric,'',tags->>'player',sum(value) FROM flame_ingest GROUP BY metric,tags->>'player';
EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) $($statements[$phase]);
ROLLBACK;
"@
    $sql | Set-Content -LiteralPath (Join-Path $output "$phase-$iteration.sql")
    $raw = $sql | & $docker exec -i $info.Id psql -U flamedb -d flamedb -X -q -t -A -v ON_ERROR_STOP=1
    if ($LASTEXITCODE -ne 0) { throw "PostgreSQL execution plan failed: $phase/$iteration" }
    $plan = ($raw -join "`n") | ConvertFrom-Json
    if ($plan.Count -ne 1 -or $null -eq $plan[0].'Execution Time') { throw 'Missing measured PostgreSQL plan.' }
    $plan | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath (Join-Path $output "$phase-$iteration.json")
    $observations += @{phase=$phase;iteration=$iteration;warmup=($iteration -eq 0);execution_ms=$plan[0].'Execution Time';planning_ms=$plan[0].'Planning Time';triggers=$plan[0].Triggers;shared_hit_blocks=$plan[0].Plan.'Shared Hit Blocks';shared_read_blocks=$plan[0].Plan.'Shared Read Blocks';shared_dirtied_blocks=$plan[0].Plan.'Shared Dirtied Blocks'}
  }
}
} finally {
  if ($prefix -notmatch '^profile_[a-f0-9]{32}$') { throw 'Invalid private schema cleanup target.' }
  $cleanup = "DROP SCHEMA IF EXISTS `"$prefix`" CASCADE;"
  $cleanup | & $docker exec -i $info.Id psql -U flamedb -d flamedb -X -q -t -A -v ON_ERROR_STOP=1 | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'Private profiling schema cleanup failed.' }
}
$source = @(Get-ChildItem -LiteralPath 'internal/analytics' -File | Where-Object {$_.Extension -in '.go','.sql'} | Sort-Object FullName | ForEach-Object { @{path=$_.Name;sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash} })
@{container_id=$info.Id;image=$info.Image;rows=$Rows;repeats=$Repeats;prefix=$prefix;source=$source;legacy_trigger_dispatch=[bool]$LegacyTriggerDispatch;existing_scores_and_tags=[bool]$ExistingScoresAndTags;explicit_tag_conflict=[bool]$ExplicitTagConflict;isolated_schema=$true;schema_removed=$true;commit_measured=$false;observations=$observations} | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $output 'summary.json')
foreach ($phase in $phases) {
  $ordered = @($observations | Where-Object {$_.phase -eq $phase -and -not $_.warmup} | Sort-Object execution_ms)
  Write-Output "$phase median execution: $($ordered[[int][math]::Floor($ordered.Count/2)].execution_ms) ms"
}
Write-Output 'POSTGRES_INGESTION_PLANS_RECORDED'
