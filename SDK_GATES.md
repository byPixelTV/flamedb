# Gates: SDK concurrency and request deadlines

Scope: Kotlin, Go and TypeScript use configurable reserved read capacity, total request deadlines including admission/connect/auth, safe response ownership, separate queue/request timings, and preserve recovery without replaying ambiguous writes.

- [x] K1: Kotlin reads progress during blocked writes, deadlines and cancellation cover queued/active calls, and recovery/close preserve response ownership
  CHECK: gradlew.bat test --no-daemon --console=plain
  EXPECT: BUILD SUCCESSFUL
  CWD: pkg/kotlin
  EVIDENCE: automatic-evidence=v1; definition-sha256=603a812663190349c28c1651f66cebf9b11f4144a14d46b743b2854cde92b749; exit=0; EXPECT=matched; output-sha256=2f4ca9b391c4832eb0219071be70f7364102485b6910748eafe29d51fd9fb614; output-bytes=1226; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb\pkg\kotlin; path=6139ba940532/61 entries

- [x] G1: Go pooled calls preserve deadlines, read isolation, response ownership and recovery with no data races
  CHECK: go test -race ./... -count=1 -timeout=120s && go vet ./...
  EXPECT: /^ok\s+github\.com\/byPixelTV\/flamedb\/pkg\/go\/flamedb\b/m
  CWD: pkg/go
  EVIDENCE: automatic-evidence=v1; definition-sha256=a2d206962f64a287c6bfb8aa5fd567457ee723e58d35f5d5093eaa14e4d6e4c9; exit=0; EXPECT=matched; output-sha256=99bc89dfa1a9ffb50e736963e81f69d5763fad1c7d048dcd3bd595fe16eef1f3; output-bytes=56; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb\pkg\go; path=6139ba940532/61 entries

- [x] T1: TypeScript builds and read isolation, queue deadlines, pipelining, response ownership and recovery pass
  CHECK: npm.cmd test
  EXPECT: /(?:# fail 0|ℹ fail 0)/
  CWD: pkg/typescript
  EVIDENCE: automatic-evidence=v1; definition-sha256=df5a3d0d5aab1d2e44fccbaf3dac76579ac1f1d443e3a5adc1ce9155a93c669d; exit=0; EXPECT=matched; output-sha256=23b361e86ab22057922590cfc62e2560192f881890cf6fcba529c96d058dabd5; output-bytes=1576; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb\pkg\typescript; path=6139ba940532/61 entries

- [x] D1: SDK usage and ordering/deadline/timing contracts are documented consistently
  CHECK: node scripts/check-sdk-docs.cjs
  EXPECT: SDK documentation contracts verified
  EVIDENCE: automatic-evidence=v1; definition-sha256=32197bf46625113183fae1a811d9140d2196987e541dd57265a05ab506ef47e9; exit=0; EXPECT=matched; output-sha256=8e17e846310aab03bd808b46320a61b760ae03bf13fb3eec5b7574a8a5186991; output-bytes=37; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb; path=6139ba940532/61 entries
