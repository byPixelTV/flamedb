# Gates: Grafana large responses and sampling gaps

OWNS: grafana/pkg/plugin/**, grafana/scripts/**, grafana/README.md, grafana/LARGE_RESPONSE_GATES.md

Scope: Read grouped results in bounded time windows, report framing limits clearly, preserve scan limits and global ranges, tolerate one empty bucket without hiding longer gaps, and build deployable plugin binaries.

- [x] G1: Backend regressions verify oversized frames, complete windowed reads, scan limits, cancellation and gap handling
  CHECK: node scripts/test.mjs
  EXPECT: FlameDB backend tests passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=362f759921d2bf35247b14defab0b09a29fa54f0403f57c888763685a38b55e4; exit=0; EXPECT=matched; output-sha256=0ea7aae83fa6c3f7ca645108fedc232a305396ee9123c677eccd541d47e4e821; output-bytes=151; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=e76b00ac42dd/62 entries

- [x] G2: Frontend and Windows/Linux backend binaries build successfully
  CHECK: npm.cmd run build
  EXPECT: FlameDB plugin build passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=d328e4e61c6ad42274193041e45611ed016aac41c96854b43bd37cb15e29bd7c; exit=0; EXPECT=matched; output-sha256=3e2a7e3c2b990cfd40a2e18e46711c4b8b30a6e1e13ccff4c30193ab40585535; output-bytes=1218; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=e76b00ac42dd/62 entries

- [x] G3: Real FlameDB/Grafana integration still returns correct filtered series and latest tables
  CHECK: node scripts/integration.mjs
  EXPECT: Grafana integration passed
  EVIDENCE: automatic-evidence=v1; definition-sha256=07f813b3fc0ccde5001dbdf628300d615699cf8117ac4d25945349ba88ef78c4; exit=0; EXPECT=matched; output-sha256=8267b9ae52e6dc8411b601d09f16ea6609e8736118025cda6d15b6f1abaf2b8d; output-bytes=152; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\grafana; path=e76b00ac42dd/62 entries
