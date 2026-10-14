package compat

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestFrozenLegacySDKsAgainstNextgen compiles unchanged SDK source from the
// frozen reference and executes real consumers against PostgreSQL/Redis-backed
// FlameDB TCP. It is deliberately separate from local fake-server SDK unit tests.
func TestFrozenLegacySDKsAgainstNextgen(t *testing.T) {
	nextgen := prerequisite(t, "FLAMEDB_NEXTGEN_BINARY")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	frozen := filepath.Join(root, ".compat", "reference")
	verifyFrozenSDKSource(t, root, frozen)
	addr := startServer(t, nextgen, true)
	prefix := fmt.Sprintf("frozensdk%d", time.Now().UnixNano())
	sdk := filepath.Join(root, "internal", "compat", "sdk")
	mode := "all"
	if os.Getenv("FLAMEDB_TEST_LEGACY_SKIP_RAW") == "1" {
		mode = "skip-raw"
		t.Log("LIMITATION: raw-event Kotlin/TypeScript decoding excluded explicitly; frozen server emits uppercase event keys rejected by those frozen SDKs")
	}
	t.Run("Go", func(t *testing.T) {
		runConsumer(t, filepath.Join(sdk, "go"), "FROZEN_GO_SDK_OK", "go", "run", ".", addr, prefix+"go")
	})
	t.Run("TypeScript", func(t *testing.T) {
		output := t.TempDir()
		compiler := filepath.Join(root, "pkg", "typescript", "node_modules", "typescript", "lib", "tsc.js")
		source := filepath.Join(frozen, "pkg", "typescript", "src", "index.ts")
		runConsumer(t, output, "", "node", compiler, "--target", "ES2020", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--types", "node", "--typeRoots", filepath.Join(root, "pkg", "typescript", "node_modules", "@types"), "--skipLibCheck", "--outDir", output, source)
		runConsumer(t, sdk, "FROZEN_TYPESCRIPT_SDK_OK", "node", filepath.Join(sdk, "typescript.cjs"), filepath.Join(output, "index.js"), addr, prefix+"ts", mode)
		runConsumer(t, sdk, "FROZEN_TYPESCRIPT_SDK_OK", "bun", filepath.Join(sdk, "typescript.cjs"), filepath.Join(output, "index.js"), addr, prefix+"bun", mode)
	})
	t.Run("Kotlin", func(t *testing.T) {
		wrapper := filepath.Join(root, "pkg", "kotlin", "gradlew")
		if os.PathSeparator == '\\' {
			wrapper += ".bat"
		}
		runConsumer(t, filepath.Join(root, "pkg", "kotlin"), "FROZEN_KOTLIN_SDK_OK", wrapper, "-p", filepath.Join(sdk, "kotlin"), "runLegacy", "--no-daemon", "--console=plain", "-PsdkAddr="+addr, "-PsdkPrefix="+prefix+"kotlin", "-PsdkJournal="+t.TempDir(), "-PsdkMode="+mode)
	})
}

// This negative control demonstrates the pre-existing discrepancy using the
// actual frozen server and unchanged SDKs; a passing result confirms the defect,
// not successful raw-event SDK compatibility.
func TestFrozenRawEventSDKBaselineLimitation(t *testing.T) {
	reference := prerequisite(t, "FLAMEDB_REFERENCE_BINARY")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	frozen := filepath.Join(root, ".compat", "reference")
	verifyFrozenSDKSource(t, root, frozen)
	addr := startServer(t, reference, false)
	sdk := filepath.Join(root, "internal", "compat", "sdk")
	prefix := fmt.Sprintf("rawbaseline%d", time.Now().UnixNano())
	w := connect(t, addr, "compat-key")
	w.send(t, "WRITE "+prefix+".proof 1 ts=1704067200000000001")
	result := w.send(t, "GET "+prefix+".proof").(map[string]any)
	row := result["events"].([]any)[0].(map[string]any)
	if row["Timestamp"] == nil || row["timestamp"] != nil {
		t.Fatalf("baseline uppercase-only event shape changed: %v", row)
	}
	t.Logf("frozen server raw event: %v", row)
	t.Run("TypeScript", func(t *testing.T) {
		output := t.TempDir()
		runConsumer(t, output, "", "node", filepath.Join(root, "pkg", "typescript", "node_modules", "typescript", "lib", "tsc.js"), "--target", "ES2020", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--types", "node", "--typeRoots", filepath.Join(root, "pkg", "typescript", "node_modules", "@types"), "--skipLibCheck", "--outDir", output, filepath.Join(frozen, "pkg", "typescript", "src", "index.ts"))
		expectConsumerFailure(t, sdk, "undefined", "node", filepath.Join(sdk, "typescript.cjs"), filepath.Join(output, "index.js"), addr, prefix+"ts", "raw-only")
	})
	t.Run("Kotlin", func(t *testing.T) {
		wrapper := filepath.Join(root, "pkg", "kotlin", "gradlew")
		if os.PathSeparator == '\\' {
			wrapper += ".bat"
		}
		expectConsumerFailure(t, filepath.Join(root, "pkg", "kotlin"), "Fields [timestamp, metric, value] are required", wrapper, "-p", filepath.Join(sdk, "kotlin"), "runLegacy", "--no-daemon", "--console=plain", "-PsdkAddr="+addr, "-PsdkPrefix="+prefix+"kotlin", "-PsdkJournal="+t.TempDir(), "-PsdkMode=raw-only")
	})
}

func expectConsumerFailure(t *testing.T, dir, failure, executable string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), failure) {
		t.Fatalf("expected pre-existing baseline failure %q; got %v\n%s", failure, err, output)
	}
	t.Logf("BASELINE_RAW_SDK_LIMITATION_CONFIRMED %s: %s", executable, failure)
}

func runConsumer(t *testing.T, dir, success, executable string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("consumer %s: %v\n%s", executable, err, output)
	}
	if success != "" && !strings.Contains(string(output), success) {
		t.Fatalf("missing success marker %s\n%s", success, output)
	}
	t.Logf("%s\n%s", executable, output)
}

func TestAdditiveRankingSDKsAgainstNextgen(t *testing.T) {
	nextgen := prerequisite(t, "FLAMEDB_NEXTGEN_BINARY")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	addr := startServer(t, nextgen, true)
	prefix := fmt.Sprintf("rankingsdk%d", time.Now().UnixNano())
	sdk := filepath.Join(root, "internal", "compat", "sdk")
	t.Run("Go", func(t *testing.T) {
		runConsumer(t, filepath.Join(sdk, "ranking-go"), "RANKING_GO_SDK_OK", "go", "run", ".", addr, prefix+"go")
	})
	t.Run("TypeScript", func(t *testing.T) {
		npm := "npm"
		if os.PathSeparator == '\\' {
			npm += ".cmd"
		}
		runConsumer(t, filepath.Join(root, "pkg", "typescript"), "", npm, "run", "build")
		runConsumer(t, sdk, "RANKING_TYPESCRIPT_SDK_OK", "node", filepath.Join(sdk, "ranking-typescript.cjs"), filepath.Join(root, "pkg", "typescript", "dist", "index.js"), addr, prefix+"ts")
		runConsumer(t, sdk, "RANKING_TYPESCRIPT_SDK_OK", "bun", filepath.Join(sdk, "ranking-typescript.cjs"), filepath.Join(root, "pkg", "typescript", "dist", "index.js"), addr, prefix+"bun")
	})
	t.Run("Kotlin", func(t *testing.T) {
		wrapper := filepath.Join(root, "pkg", "kotlin", "gradlew")
		if os.PathSeparator == '\\' {
			wrapper += ".bat"
		}
		runConsumer(t, filepath.Join(root, "pkg", "kotlin"), "RANKING_KOTLIN_SDK_OK", wrapper, "-p", filepath.Join(sdk, "ranking-kotlin"), "runRanking", "--no-daemon", "--console=plain", "-PsdkAddr="+addr, "-PsdkPrefix="+prefix+"kotlin")
	})
}

func TestFrozenMissingMetricResultShapesAgainstNextgen(t *testing.T) {
	reference := prerequisite(t, "FLAMEDB_REFERENCE_BINARY")
	nextgen := prerequisite(t, "FLAMEDB_NEXTGEN_BINARY")
	old := connect(t, startServer(t, reference, false), "compat-key")
	current := connect(t, startServer(t, nextgen, true), "compat-key")
	metric := fmt.Sprintf("missingmetric%d", time.Now().UnixNano())
	for _, w := range []*wire{old, current} {
		w.send(t, "WRITE "+metric+" 1 ts=1704067200000000001")
	}
	for _, q := range []struct{ suffix, field string }{{"", "metrics"}, {" SUM", "aggregates"}, {" GROUP BY 1d", "series_by_metric"}} {
		a := old.send(t, "GET "+metric+","+metric+".absent"+q.suffix).(map[string]any)
		b := current.send(t, "GET "+metric+","+metric+".absent"+q.suffix).(map[string]any)
		oldMap, ok := a[q.field].(map[string]any)
		if !ok {
			t.Fatalf("missing baseline map %s: %v", q.field, a)
		}
		newMap, ok := b[q.field].(map[string]any)
		if !ok {
			t.Fatalf("missing nextgen map %s: %v", q.field, b)
		}
		if !reflect.DeepEqual(oldMap[metric+".absent"], newMap[metric+".absent"]) {
			t.Fatalf("%s absent shape mismatch frozen=%v nextgen=%v", q.field, oldMap[metric+".absent"], newMap[metric+".absent"])
		}
		if q.field == "aggregates" {
			if !reflect.DeepEqual(oldMap, newMap) {
				t.Fatalf("aggregate mismatch: %v != %v", oldMap, newMap)
			}
		} else {
			if _, ok := oldMap[metric+".absent"].([]any); !ok {
				t.Fatalf("frozen %s absent is not []: %v", q.field, oldMap)
			}
			if len(oldMap[metric].([]any)) != 1 || len(newMap[metric].([]any)) != 1 {
				t.Fatal("present metric row count mismatch")
			}
		}
	}
}

func verifyFrozenSDKSource(t *testing.T, root, frozen string) {
	t.Helper()
	for _, rel := range []string{"pkg/go/flamedb", "pkg/typescript/src", "pkg/kotlin/src/main/kotlin"} {
		checked := 0
		err := filepath.WalkDir(filepath.Join(frozen, filepath.FromSlash(rel)), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			ext := filepath.Ext(path)
			if ext != ".go" && ext != ".ts" && ext != ".kt" {
				return nil
			}
			file, err := filepath.Rel(frozen, path)
			if err != nil {
				return err
			}
			command := exec.Command("git", "show", ReferenceCommit+":"+filepath.ToSlash(file))
			command.Dir = root
			expected, err := command.Output()
			if err != nil {
				return err
			}
			actual, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// The frozen checkout was materialized with Windows autocrlf. Ignore
			// only CRLF versus LF; retain and compare every source character.
			if !bytes.Equal(bytes.ReplaceAll(actual, []byte("\r\n"), []byte("\n")), bytes.ReplaceAll(expected, []byte("\r\n"), []byte("\n"))) {
				return fmt.Errorf("frozen SDK differs from %s: %s", ReferenceCommit, file)
			}
			checked++
			return nil
		})
		if err != nil || checked == 0 {
			t.Fatalf("verify frozen %s (%d files): %v", rel, checked, err)
		}
		t.Logf("verified unchanged %s: %d files", rel, checked)
	}
}
