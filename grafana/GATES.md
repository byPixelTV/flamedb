# Gates: FlameDB Grafana datasource

OWNS: grafana/**

Scope: Standalone Grafana datasource with visual queries, time series, latest-by-tag tables, secure configuration, Windows/Linux builds and an example dashboard.

- [x] G1: Backend tests exercise TCP auth, query validation, cancellation and frame conversion
  CHECK: node scripts/test.mjs
  EXPECT: FlameDB backend tests passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=362f759921d2bf35247b14defab0b09a29fa54f0403f57c888763685a38b55e4; exit=0; EXPECT=matched; output-sha256=b3e50f8335647f62e4fbe5fa84bd29b6d072a5bdce7fdd245e6f42372fd82afd; output-bytes=151; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=c942a1fe9be0/62 entries

- [x] G2: Frontend typechecks and produces a Grafana AMD bundle and platform binaries
  CHECK: npm.cmd run build
  EXPECT: FlameDB plugin build passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=d328e4e61c6ad42274193041e45611ed016aac41c96854b43bd37cb15e29bd7c; exit=0; EXPECT=matched; output-sha256=4ac3e49992ecb69d4eac99d45c61507d164054f466c4a83b12f1b96183bb2a73; output-bytes=1300; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=c942a1fe9be0/62 entries

- [x] G3: Grafana loads the plugin and queries a temporary FlameDB instance
  CHECK: node scripts/integration.mjs
  EXPECT: Grafana integration passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=07f813b3fc0ccde5001dbdf628300d615699cf8117ac4d25945349ba88ef78c4; exit=0; EXPECT=matched; output-sha256=cc7d42446302edb6d75fcc06d558123871e7af190872cc48e09581a7194b39b6; output-bytes=153; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=c942a1fe9be0/62 entries

- [x] G4: Query builder and example dashboard reviewed in Grafana
  EVIDENCE: Grafana 13.2.4 rendered the overview table and Stat panels. Metric and tag dropdowns populated; selecting region=1 and Run query changed TPS from 18.43 to 19.34 without raw syntax. Screenshot: .runtime/query-builder.png.



