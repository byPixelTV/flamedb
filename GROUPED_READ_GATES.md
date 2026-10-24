# Gates: Server-side grouped reads for Grafana

OWNS: internal/query/**, internal/storage/scan.go, internal/analytics/grouped.go, internal/analytics/read.go, internal/server/grouped_test.go, grafana/pkg/plugin/**, grafana/scripts/**, grafana/README.md, grafana/patches/grouped-history.patch, GROUPED_READ_GATES.md, scripts/check-grouped-query.cjs

Scope: Stream grouped histories and latest-per-group snapshots on FlameDB, bound output rather than scanned raw events, use compact results in Grafana, preserve global time ranges, and verify histories beyond the old scan limit.

- [x] G1: Query, storage, server and analytics tests pass, including large streaming aggregation and compact protocol checks
  CHECK: node scripts/check-grouped-query.cjs
  EXPECT: Server grouped-query tests passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=89707b1ebbdc00f73ec8d296cf5f9293e5876ed13f63ea2de8dab9cfbe6909f0; exit=0; EXPECT=matched; output-sha256=74656a8b499acf0199e476faaab75a5fd347241a86718b8968192dab766324f3; output-bytes=265; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb; path=e76b00ac42dd/62 entries

- [x] G2: Plugin tests verify native grouped conversion, upgrade fallback and bounded output
  CHECK: node scripts/test.mjs
  EXPECT: FlameDB backend tests passed
  CWD: grafana
  EVIDENCE: automatic-evidence=v1; definition-sha256=c987169c87943071f7f025802dd79829446ffbd674c7ec2e12151c8fe41ed0ed; exit=0; EXPECT=matched; output-sha256=ae52cd815032a4ef27f72fc2698d5d3dce860631f338c9c80f9ceee440be83a5; output-bytes=151; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=e76b00ac42dd/62 entries

- [x] G3: Plugin binaries build for Windows and Linux
  CHECK: npm.cmd run build
  EXPECT: FlameDB plugin build passed
  CWD: grafana
  EVIDENCE: automatic-evidence=v1; definition-sha256=f161b59ab8caf534c5c4d39ee01e24a14c73b1c6d459c03aa1e67b5549296e4d; exit=0; EXPECT=matched; output-sha256=df3403ba48f50886b231e787c3c03c90842533c9ba0dc3677aacdf1f2375d04a; output-bytes=1218; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=e76b00ac42dd/62 entries

- [x] G4: Real Grafana queries all 60000 history records with a raw scan limit of 100 and obtains latest-per-region rows
  CHECK: node scripts/integration.mjs
  EXPECT: Native grouping beyond raw scan limit passed
  CWD: grafana
  EVIDENCE: automatic-evidence=v1; definition-sha256=feff93fc038731444ddc835378576364195d26047318ad9733b222a693683aea; exit=0; EXPECT=matched; output-sha256=ad2295778385fa5bd73eddbe55f8680d80ee7bcea02861f7d935df8151783f7f; output-bytes=198; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=e76b00ac42dd/62 entries

