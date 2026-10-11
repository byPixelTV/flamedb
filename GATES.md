# Gates: windowed leaderboard and sustained memory performance

Scope: Make time-window leaderboards scan only relevant primary events, avoid per-event decoding allocations, and eliminate cardinality bookkeeping allocation churn without changing durability or query semantics.

- [x] G1: Windowed queries preserve exact ranges, pagination, legacy values, overwrites, deletes, and concurrent-read consistency
  CHECK: go test ./internal/storage ./internal/query -count=1
  EXPECT: /^ok\s+github\.com\/byPixelTV\/flamedb\/internal\/query\b/m
  EVIDENCE: automatic-evidence=v1; definition-sha256=171dbfc6de7eeaacc5314e71c97baa2e4f574010cdae1d5c9573c87838a8677e; exit=0; EXPECT=matched; output-sha256=ef6bfe08683fe66a32f3fdbe691ef9105d5ef0e46d76193199b254c8e2365666; output-bytes=114; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb; path=6139ba940532/61 entries

- [x] G2: Repeated cardinality flushes and leaderboard scans have measured allocation reductions and bounded retained bookkeeping
  CHECK: go test ./internal/storage -run "Test(CardinalityMemory|WindowedAllocations|WindowedSteadyStateMemory)" -count=1 -v > performance-memory.log 2>&1 && type performance-memory.log
  EXPECT: /^ok\s+github\.com\/byPixelTV\/flamedb\/internal\/storage\b/m
  EVIDENCE: automatic-evidence=v1; definition-sha256=0ba5cd84a4aa8b48667554bad07ed0f6bb6bd645b2048e2b6af7c4f36d3d4f23; exit=0; EXPECT=matched; output-sha256=4efa43adad57f06544aff31dffba40c27f5c312e8721761e75592e9bc5536899; output-bytes=660; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb; path=6139ba940532/61 entries

- [x] G5: Concurrent ingestion and windowed reads have no detected data races
  CHECK: go test -race ./internal/storage ./internal/query -run "Test(Windowed|Cardinality|ClanLeaderboard)" -count=1 -timeout=120s
  EXPECT: /^ok\s+github\.com\/byPixelTV\/flamedb\/internal\/storage\b/m
  EVIDENCE: automatic-evidence=v1; definition-sha256=aafd53fa6b0e4f8c696c19cea2c2672493ab9fe810f1e46773239646780ebf2f; exit=0; EXPECT=matched; output-sha256=18030323638e92c278771d12433ed2ef116c88a8a505a1ece92740ae126417e6; output-bytes=115; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb; path=6139ba940532/61 entries

- [x] G3: The project passes its regression suite and static checks
  CHECK: go test ./... -count=1 && go vet ./...
  EXPECT: /^ok\s+github\.com\/byPixelTV\/flamedb\/internal\/storage\b/m
  EVIDENCE: automatic-evidence=v1; definition-sha256=db461a91515445a8585d95fe9fb443f796d54bd54633cae5a3e212ceaad69618; exit=0; EXPECT=matched; output-sha256=d7a31604e72c417e18c0523f9447238a80e23af4a721057fbc773111767b0ec5; output-bytes=787; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb; path=6139ba940532/61 entries

- [x] G4: Reproducible benchmarks measure query latency and allocations against the original algorithm
  CHECK: go test ./internal/storage -run "^$" -bench "Benchmark(WindowedLeaderboard|CardinalityChurn)" -benchmem -benchtime=3x -count=3 > performance-benchmark.log 2>&1 && type performance-benchmark.log
  EXPECT: /^ok\s+github\.com\/byPixelTV\/flamedb\/internal\/storage\b/m
  EVIDENCE: automatic-evidence=v1; definition-sha256=12870ea83d612cc064607e5f6c3b70b3c0adcfc374c79d57a98b09bd373a9bd5; exit=0; EXPECT=matched; output-sha256=e67a2afbf1f320edab2ef233f9f768ea2585a41dda8ddcc981d2049583258be1; output-bytes=2660; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb; path=6139ba940532/61 entries

