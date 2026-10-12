# Gates: SDK-managed buffered ingestion

Scope: provide one SDK entry point with safe defaults for native batching, bounded heap/disk admission, controlled failures, crash recovery and shutdown; preserve confirmed direct write semantics, reserved pools and total request deadlines. Do not publish or alter EraMC changes.

- [x] B1: Existing transport tests and buffered TCP regressions pass, including disk limit, recovery, partial/unknown no replay, cancellation/shutdown and default API compatibility
  CHECK: gradlew.bat test --no-daemon --console=plain
  EXPECT: BUILD SUCCESSFUL
  EVIDENCE: automatic-evidence=v1; definition-sha256=fc8fbdd74796e7e1f72aa0545b12363d9fad9cd5b1b80b257157fa545c0068e6; exit=0; EXPECT=matched; output-sha256=44b9920f4e2d55868bc29c53df8f413326502269400618096c34673492e510ea; output-bytes=1854; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\pkg\kotlin; path=b8514920e88f/61 entries

- [x] B2: Existing SDK stress and buffered 100000-event burst prove bounded admission, response ownership, read isolation and network batching; the per-event coroutine counterexample fails as expected
  CHECK: gradlew.bat sdkStressTest --no-daemon --console=plain
  EXPECT: BUFFERED_SDK_STRESS_VERIFIED
  EVIDENCE: automatic-evidence=v1; definition-sha256=4f5e375a53a85f7d0bdecaa6f7ccd62f00b708a395ba3fd225db38135cc2a7d1; exit=0; EXPECT=matched; output-sha256=3abb1a5a038c3848d20fa7e5620533fe90ee8af5abd58af2ad65947e3ba31003; output-bytes=1940; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\pkg\kotlin; path=b8514920e88f/61 entries

- [x] B3: Minimal usage, acknowledgement/durability/capacity/recovery/shutdown semantics and measured limitations match the final API; direct-client compatibility is preserved; no release or external project changes
  EVIDENCE: Manual review 2026-10-12: README minimal factory matches compiled default-usage test; BufferedWriteConfig defaults/capacity match source; 55 passing regressions and two passing stresses confirmed in JUnit XML; local-vs-remote ACK, OS-cache/fsync/metadata limits, safe retry predicate, bounded cold read admission, unknown/partial audit and shutdown documented in BUFFERED_WRITES.md; actual numbers and negative control saved in BUFFERED_RESULTS.md; direct transport/configuration semantics unchanged, all changes confined to pkg/kotlin, git diff --check clean and no publishing commands.
