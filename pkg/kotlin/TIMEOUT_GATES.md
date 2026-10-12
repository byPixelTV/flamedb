# Gates: diagnose deadline failures without unnecessary reconnects

- [x] T1: Regression tests prove expired/cancelled IO work never sends or discards an untouched healthy socket, with correct timeout status, and preserve existing pooling/response/deadline/recovery invariants
  CHECK: gradlew.bat test --no-daemon --console=plain
  EXPECT: BUILD SUCCESSFUL
  EVIDENCE: automatic-evidence=v1; definition-sha256=fc8fbdd74796e7e1f72aa0545b12363d9fad9cd5b1b80b257157fa545c0068e6; exit=0; EXPECT=matched; output-sha256=37a75a970d3a985a658c4d8879e91ceac2d7cf4d41fd8cc3ceb0ce844890bc43; output-bytes=1201; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\pkg\kotlin; path=b8514920e88f/61 entries

- [x] T2: Mixed-request and overload stress retain bounded admission, read isolation, response assignment and no replay
  CHECK: gradlew.bat sdkStressTest --no-daemon --console=plain
  EXPECT: /SDK_STRESS_VERIFIED requests=100000 callers=10000/
  EVIDENCE: automatic-evidence=v1; definition-sha256=6e8c38a3ebb9e47d467a69ea121a27c366347cdd999b330c0f1d4e0e169a0889; exit=0; EXPECT=matched; output-sha256=cb3c756b2f3ad967d8b821253ce3f2ed025499ca3a1f06c87496678931b7be5d; output-bytes=1605; shell=C:\WINDOWS\system32\cmd.exe; cwd=C:\Dev\Go\flamedb\pkg\kotlin; path=b8514920e88f/61 entries

- [x] T3: Document confirmed config/log observations, reproduced SDK flaw, diagnostic logging, caller handling and production-cause limitations
  EVIDENCE: Manually reviewed TIMEOUT_DIAGNOSIS.md/README.md against supplied initialization, timeout causes, source and JUnit XML. Both counterexamples failed on prior code with expected 1 versus observed 2 sockets; final 34 regression tests plus 100000-request/10000-caller stress pass. Verified 3216ms, 64 queued/9936 rejected, heap 7/10/8MiB snapshots. No blanket claim about production cause, false API-key rejection, retry policy or database performance; sampled diagnostic and launch error handling explained. Existing public signatures and queue/response/pool limits retained, no publication.
