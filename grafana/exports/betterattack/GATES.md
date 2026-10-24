# Gates: BetterAttack complete metrics dashboard

OWNS: grafana/exports/betterattack/**

Scope: Extend the supplied Grafana v2 dashboard with every supplied metric, preserve its datasource, distinguish events from sampled counters, and validate the import and visible layout in Grafana.

- [x] G1: Every supplied metric is reachable in the layout and units/counter calculations match the collector
  CHECK: python verify.py
  EXPECT: Dashboard structure and metric semantics passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=874b365114e629cbf85b10d4d386634e0d43e6833c7a955c1dd81461e65f5af6; exit=0; EXPECT=matched; output-sha256=ba5e5c8d27a7d09ce195f41e581c70880ca5cb9c0ac7f5b447793a4b018be079; output-bytes=128; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana\exports\betterattack; path=c942a1fe9be0/62 entries

- [x] G2: Grafana accepts the v2 dashboard and every panel query returns seeded data
  CHECK: node integration.mjs
  EXPECT: BetterAttack Grafana integration passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=7766e62f4c8f00af636a3cfd268ea2979a64bcfb305168d72a0b364fafd38cd3; exit=0; EXPECT=matched; output-sha256=bf95535a70bd7411287ca1561e8e44bb464ee013c6a585bab42db6463a6dd575; output-bytes=324; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana\exports\betterattack; path=c942a1fe9be0/62 entries

- [x] G3: All seven dashboard tabs visually reviewed
  EVIDENCE: Grafana 13.2.4 browser review confirmed 12-event cumulative player totals, unchanged sampled event counters (22), heap/nonheap units, GC and Paper tick series, and region rows. Final revision shows TPS 17.20/19.80, region utilization 30%/40%, no timestamp columns in the region table, and process start dates in 2026 after seconds-to-milliseconds conversion. Screenshots: server-preview.png and regions-preview.png; synthetic local data only.

