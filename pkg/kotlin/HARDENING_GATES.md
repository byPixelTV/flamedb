# Gates: bounded Kotlin SDK admission and responses

Scope: retain reserved read/write pools and total deadlines; bound waiting calls and wire responses; expose structured, compatible failures and mutation uncertainty; prove recovery, no replay, and known counterexamples with local TCP fixtures. No automatic batching or publication.

- [x] K1: Existing and hardening TCP tests pass, including bounded admission/cancellation/close, strict response framing/UTF-8/size, error status, read isolation, restart/auth recovery, response ownership/no replay, and known unsafe counterexamples
  CHECK: gradlew.bat test --no-daemon --console=plain
  EXPECT: BUILD SUCCESSFUL
  EVIDENCE: automatic-evidence=v1; definition-sha256=fc8fbdd74796e7e1f72aa0545b12363d9fad9cd5b1b80b257157fa545c0068e6; exit=0; EXPECT=matched; output-sha256=37a75a970d3a985a658c4d8879e91ceac2d7cf4d41fd8cc3ceb0ce844890bc43; output-bytes=1201; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb\pkg\kotlin; path=6139ba940532/61 entries

- [x] K2: Local TCP stress completes 100,000 commands without misassignment/replay and rejects overload with 10,000 simultaneous callers while respecting configured capacity
  CHECK: gradlew.bat sdkStressTest --no-daemon --console=plain
  EXPECT: /SDK_STRESS_VERIFIED requests=100000 callers=10000/
  EVIDENCE: automatic-evidence=v1; definition-sha256=6e8c38a3ebb9e47d467a69ea121a27c366347cdd999b330c0f1d4e0e169a0889; exit=0; EXPECT=matched; output-sha256=71a52e8bb794c4e3ad8567fe0a0b1682320bf090b73d501056aebc822d38de23; output-bytes=1605; shell=C:\WINDOWS\system32\cmd.exe; cwd=D:\Dev\Go\flamedb\pkg\kotlin; path=6139ba940532/61 entries

- [x] K3: Configuration defaults, error/outcome semantics, cancellation, byte framing, compatibility and measured local-only results are documented and reviewed
  EVIDENCE: Manually reviewed README.md and HARDENING_RESULTS.md against final source and JUnit XML: 32 passing regression tests and one passing stress test; 100000 requests in 3027ms; 64 queued/9936 rejected out of 10000 callers; heap snapshots 7/26/8MiB. Verified original FlameDBException JVM constructors with javap. Data-class ABI/recompile, strict framing/default limits, shared cancellation diagnostics and local-only measurement limitations are explicit. All changed paths are under pkg/kotlin; no publication command or automatic batch queue.
