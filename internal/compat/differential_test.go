// Package compat verifies the public TCP protocol against the frozen reference.
// Run with FLAMEDB_INTEGRATION=1 to require real binaries and databases.
package compat

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const ReferenceCommit = "a7bb892b881e59d207f79ae7258ceecdd4c63ac7"

type wire struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (w *wire) read(t *testing.T) any {
	t.Helper()
	_ = w.conn.SetDeadline(time.Now().Add(30 * time.Second))
	line, err := w.reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("TCP read: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var result any
	if err := dec.Decode(&result); err != nil {
		t.Fatalf("JSON %q: %v", line, err)
	}
	return result
}
func (w *wire) send(t *testing.T, command string) any {
	t.Helper()
	_ = w.conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := fmt.Fprintln(w.conn, command); err != nil {
		t.Fatal(err)
	}
	return w.read(t)
}
func connect(t *testing.T, addr, key string) *wire {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	w := &wire{conn, bufio.NewReader(conn)}
	if got := w.read(t); !reflect.DeepEqual(got, map[string]any{"auth": "required"}) {
		t.Fatalf("greeting: %v", got)
	}
	auth := w.send(t, "AUTH "+key)
	if key != "invalid" {
		if auth.(map[string]any)["auth"] != "ok" {
			t.Fatalf("auth: %v", auth)
		}
	}
	return w
}

func prerequisite(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		if os.Getenv("FLAMEDB_INTEGRATION") == "1" {
			t.Fatalf("explicit integration requires %s", key)
		}
		t.Skipf("set FLAMEDB_INTEGRATION=1 and %s for real differential verification", key)
	}
	return value
}
func startServer(t *testing.T, binary string, external bool) string {
	addr, _ := startServerControlled(t, binary, external)
	return addr
}
func startServerControlled(t *testing.T, binary string, external bool) (string, func()) {
	t.Helper()
	absolute, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dir := t.TempDir()
	cfg := fmt.Sprintf("auth:\n  keys:\n    - name: compat\n      key: compat-key\n      permissions: [read, write]\n    - name: reader\n      key: read-key\n      permissions: [read]\n    - name: writer\n      key: write-key\n      permissions: [write]\nserver:\n  host: 127.0.0.1\n  port: %d\n  node_id: compat\n  advertise_addr: %s\n  data_path: %q\ncluster:\n  seeds: []\n  replication_factor: 1\n", port, addr, filepath.ToSlash(filepath.Join(dir, "data")))
	if external {
		cfg += externalConfig(t)
	}
	path := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(path, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(dir, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(absolute, path)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var once sync.Once
	kill := func() {
		once.Do(func() {
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
			logFile.Close()
		})
	}
	t.Cleanup(kill)
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		select {
		case err := <-done:
			data, _ := os.ReadFile(logFile.Name())
			t.Fatalf("server exited: %v\n%s", err, data)
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return addr, kill
		}
		time.Sleep(50 * time.Millisecond)
	}
	data, _ := os.ReadFile(logFile.Name())
	t.Fatalf("server startup timeout\n%s", data)
	return "", kill
}

func externalConfig(t *testing.T) string {
	return fmt.Sprintf("storage:\n  engine: postgres\n  postgres:\n    dsn: %q\n  redis:\n    mode: standalone\n    address: %q\n", prerequisite(t, "FLAMEDB_TEST_POSTGRES"), prerequisite(t, "FLAMEDB_TEST_REDIS"))
}

func TestFrozenReferenceDifferentialTCP(t *testing.T) {
	reference := prerequisite(t, "FLAMEDB_REFERENCE_BINARY")
	nextgen := prerequisite(t, "FLAMEDB_NEXTGEN_BINARY")
	oldAddr := startServer(t, reference, false)
	newAddr := startServer(t, nextgen, true)
	old := connect(t, oldAddr, "compat-key")
	next := connect(t, newAddr, "compat-key")
	prefix := fmt.Sprintf("compat%d", time.Now().UnixNano())
	metric := prefix + ".kills"
	other := prefix + ".money"
	compare := func(t *testing.T, command string) {
		t.Helper()
		a := old.send(t, command)
		b := next.send(t, command)
		validateApprovedAliases(t, b)
		if !reflect.DeepEqual(a, b) {
			t.Errorf("%s\nreference: %v\nnextgen:   %v", command, a, b)
		}
	}
	// Explicit nanosecond timestamps make responses comparable without rounding or field removal.
	cases := []string{
		"GET " + metric, "LEADERBOARD " + metric,
		fmt.Sprintf(`WRITE %s 3 lb="alice" lb.reason="fall" player="alice" region="eu" suite="%s" ts=1704067200000000001`, metric, prefix),
		fmt.Sprintf(`WRITE %s 4 lb="bob" lb.reason="fire" player="bob" region="us" suite="%s" ts=1704070800000000002`, metric, prefix),
		fmt.Sprintf(`WRITE %s 1 lb="alice" lb.reason="fall" player="alice" region="eu" suite="%s" ts=1704153600000000003`, metric, prefix),
		fmt.Sprintf(`WRITE %s -1.25 player="alice" suite="%s" ts=1704067200000000001`, other, prefix),
		"GET " + metric + " ORDER ASC LIMIT 100",
		"GET " + metric + " ORDER DESC LIMIT 1 OFFSET 1",
		"GET " + metric + ` WHERE region="eu" AND player="alice" ORDER ASC`,
		"GET " + metric + " FROM 2024-01-01 TO 2024-01-02 ORDER ASC",
		"GET " + metric + " FROM 2024-01-02 TO 2024-01-03 ORDER DESC",
		"GET " + metric + "," + other + " ORDER ASC",
		"GET " + metric + " COUNT", "GET " + metric + " SUM", "GET " + metric + " AVG",
		"GET " + metric + "," + other + " SUM",
		"GET " + metric + " SUM GROUP BY 1h", "GET " + metric + " COUNT GROUP BY 1d",
		"GET " + metric + "," + other + " AVG GROUP BY 1mo",
		"LEADERBOARD " + metric + " LIMIT 100", "LEADERBOARD " + metric + " LIMIT 1 OFFSET 1",
		"LEADERBOARD " + metric + " BOARD reason",
		"LEADERBOARD " + metric + " FROM 2024-01-01 TO 2024-01-02 ENTITY player",
		"GROUP_LEADERBOARD " + metric + ` GROUP "red:alice,bob" GROUP "blue:missing"`,
		"GROUP_LEADERBOARD " + metric + ` GROUP "red:alice" GROUP "blue:bob" FROM 2024-01-01 TO 2024-01-02 ENTITY player`,
		"GROUP_LEADERBOARD " + metric + ` BOARD reason GROUP "heat:fire,fall"`,
		"METRICS WHERE suite=" + fmt.Sprintf("%q", prefix),
		"SET " + metric + ` 8 lb="alice"`, "SET " + metric + ` 9 lb.reason="fire"`,
		"LEADERBOARD " + metric, "LEADERBOARD " + metric + " BOARD reason",
		"WRITE_BATCH\nWRITE " + metric + " 2 player=\"alice\" ts=1704240000000000004\nWRITE " + other + " 5 ts=1704240000000000004\nEND",
		"WRITE_BATCH\nWRITE " + metric + " 2 ts=1704326400000000005\nGET " + metric + "\nWRITE invalid nope\nEND",
		"WRITE_BATCH\nEND", "GET " + metric + " COUNT",
		"PREVIEW_DELETE " + metric + ` WHERE region="eu"`,
		"DELETE " + metric + ` lb="alice"`, "LEADERBOARD " + metric,
		"DELETE " + metric + ` FROM 2024-01-01 TO 2024-01-02 WHERE region="us"`,
		"GET " + metric + " ORDER ASC LIMIT 100", "LEADERBOARD " + metric + " BOARD reason",
		"BOGUS", "WRITE", "WRITE x nope", "GET x LIMIT -1", "GET x ORDER sideways",
		"LEADERBOARD x FROM 2024-01-01", "REPL_BATCH WRITE x 1",
		"WRITE x 1 __replica", "METRICS __local",
	}
	for i, command := range cases {
		t.Run(fmt.Sprintf("%02d_%s", i, strings.Fields(command)[0]), func(t *testing.T) { compare(t, command) })
	}
	edge := prefix + ":namespace.edge"
	for _, command := range []string{
		`SET ` + edge + ` -1 lb="a"`, `SET ` + edge + ` -1 lb="aa"`, `SET ` + edge + ` -1 lb="ä"`,
		`SET ` + edge + ` -1.0000000000000002 lb="b"`, `SET ` + edge + ` 0 lb="zero"`,
		`SET ` + edge + ` 9007199254740992 lb="large_a"`, `SET ` + edge + ` 9007199254740994 lb="large_b"`,
		`LEADERBOARD ` + edge + ` LIMIT 100`, `LEADERBOARD ` + edge + ` LIMIT 2 OFFSET 4`,
		`WRITE ` + edge + ` 1 lb="overwrite" label="old" ts=1704067200000000027`,
		`WRITE ` + edge + ` 4 lb="overwrite" label="new" ts=1704067200000000027`,
		`GET ` + edge + ` ORDER ASC LIMIT 100`, `GET ` + edge + ` WHERE label="old"`, `GET ` + edge + ` WHERE label="new"`,
		`LEADERBOARD ` + edge + ` LIMIT 100`, `DELETE ` + edge, `GET ` + edge, `LEADERBOARD ` + edge + ` LIMIT 100`,
		`WRITE ` + edge + ` 5 lb="nul\u0000" special="\xff" player="nul\u0000" ts=1704067200000000031`,
		`WRITE ` + edge + ` 5 lb="nul\u0000suffix" special="\xfe" player="nul\u0000suffix" ts=1704067200000000032`,
		`SET ` + edge + ` -2 lb="neg\u0000"`, `SET ` + edge + ` -2 lb="neg\u0000suffix"`,
		`GET ` + edge + ` WHERE special="\xff" ORDER ASC`,
		`LEADERBOARD ` + edge + ` LIMIT 100`,
		`LEADERBOARD ` + edge + ` FROM 2024-01-01 TO 2024-01-02 ENTITY player`,
		`WRITE ` + edge + ` 1 ts=9223372036854775806`, `WRITE ` + edge + ` 2 ts=9223372036854775807`,
		`GET ` + edge + ` ORDER DESC LIMIT 5`, `GET ` + edge + ` COUNT`,
	} {
		compare(t, command)
	}
	// Finite stored values can overflow when combined; preserve the legacy wire error.
	overflow := prefix + ".overflow"
	for _, command := range []string{
		`WRITE ` + overflow + ` 1.7976931348623157e308 player="same" ts=1704067200000000001`,
		`WRITE ` + overflow + ` 1.7976931348623157e308 player="same" ts=1704067200000000002`,
		`GET ` + overflow + ` SUM`, `GET ` + overflow + ` AVG`, `GET ` + overflow + ` COUNT`,
		`GET ` + overflow + ` SUM GROUP BY 1h`, `GET ` + overflow + ` AVG GROUP BY 1h`, `GET ` + overflow + ` COUNT GROUP BY 1h`,
		`LEADERBOARD ` + overflow + ` FROM 2024-01-01 TO 2024-01-02 ENTITY player`,
		`SET ` + overflow + ` 1.7976931348623157e308 lb="a"`, `SET ` + overflow + ` 1.7976931348623157e308 lb="b"`,
		`LEADERBOARD ` + overflow,
		`GROUP_LEADERBOARD ` + overflow + ` GROUP "sum:a,b"`,
		`GROUP_LEADERBOARD ` + overflow + ` GROUP "sum:same" FROM 2024-01-01 TO 2024-01-02 ENTITY player`,
	} {
		compare(t, command)
	}
	// Valid protocol text can exceed PostgreSQL's text-index tuple size. Use
	// incompressible strings so TOAST/compression cannot hide a truncated model.
	randomText := func(bytes int) string {
		data := make([]byte, bytes/2)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(data)
	}
	wideMetric := prefix + "." + randomText(4096)
	wideEntity, wideBoard, wideTag := randomText(4096), randomText(4096), randomText(4096)
	wideValue := randomText(65536)
	for _, command := range []string{
		fmt.Sprintf(`WRITE %s 2 lb="%s" lb.%s="named" %s="old" ts=1704067200000000091`, wideMetric, wideEntity, wideBoard, wideTag),
		fmt.Sprintf(`WRITE %s 3 lb="%s" lb.%s="named" %s="%s" ts=1704067200000000091`, wideMetric, wideEntity, wideBoard, wideTag, wideValue),
		`GET ` + wideMetric + ` ORDER ASC`, `GET ` + wideMetric + ` COUNT`, `GET ` + wideMetric + ` SUM`,
		`LEADERBOARD ` + wideMetric, `LEADERBOARD ` + wideMetric + ` BOARD ` + wideBoard,
		`LEADERBOARD ` + wideMetric + ` FROM 2024-01-01 TO 2024-01-02 ENTITY ` + wideTag,
		fmt.Sprintf(`WRITE_BATCH\nWRITE %s 1 lb="%s" ts=1704067200000000092\nWRITE %s -1 lb.%s="named" ts=1704067200000000093\nEND`, wideMetric, wideEntity, wideMetric, wideBoard),
		fmt.Sprintf(`SET %s -3 lb="%s"`, wideMetric, wideEntity),
		fmt.Sprintf(`DELETE %s lb="%s" WHERE %s="%s"`, wideMetric, wideEntity, wideTag, wideValue),
		`GET ` + wideMetric + ` ORDER ASC`, `LEADERBOARD ` + wideMetric + ` BOARD ` + wideBoard,
	} {
		command = strings.ReplaceAll(command, `\n`, "\n")
		compare(t, command)
	}
	// Unfiltered METRICS includes unrelated persistent database workloads. Limit only
	// this one response to the test-owned prefix; all other JSON fields remain exact.
	a := old.send(t, "METRICS").(map[string]any)
	b := next.send(t, "METRICS").(map[string]any)
	filter := func(m map[string]any) {
		var scoped []any
		for _, key := range m["metric_keys"].([]any) {
			if strings.HasPrefix(key.(string), prefix) {
				scoped = append(scoped, key)
			}
		}
		m["metric_keys"] = scoped
	}
	filter(a)
	filter(b)
	if !reflect.DeepEqual(a, b) {
		t.Errorf("scoped unfiltered METRICS reference=%v nextgen=%v", a, b)
	}
	// Reference cardinality is asynchronous: wait until its index settles, then compare exactly.
	statsCmd := "STATS " + metric + " TAGS player region missing"
	deadline := time.Now().Add(5 * time.Second)
	for {
		a := old.send(t, statsCmd)
		b := next.send(t, statsCmd)
		if reflect.DeepEqual(a, b) {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("%s reference=%v nextgen=%v", statsCmd, a, b)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, key := range []string{"read-key", "write-key"} {
		a := connect(t, oldAddr, key)
		b := connect(t, newAddr, key)
		for _, command := range []string{"WRITE " + prefix + ".permissions 1 ts=1704067200000000099", "GET " + prefix + ".permissions", "LEADERBOARD " + prefix + ".permissions", "STATS " + prefix + ".permissions TAGS player"} {
			ar := a.send(t, command)
			br := b.send(t, command)
			validateApprovedAliases(t, br)
			if !reflect.DeepEqual(ar, br) {
				t.Errorf("%s %s: %v != %v", key, command, ar, br)
			}
		}
	}
	// QUORUM must fail before the database mutation in a topology without a proven synchronous majority.
	rejected := next.send(t, "WRITE "+prefix+".quorum 1 QUORUM")
	if m, ok := rejected.(map[string]any); !ok || m["error"] == nil {
		t.Errorf("unsupported QUORUM acknowledged: %v", rejected)
	}
	got := next.send(t, "GET "+prefix+".quorum COUNT").(map[string]any)
	if a, ok := got["aggregate"].(map[string]any); !ok || a["count"] != json.Number("0") {
		t.Errorf("rejected QUORUM mutated: %v", got)
	}
}

// The user explicitly approved additive lowercase aliases for legacy SDKs that
// could not decode the frozen uppercase Event fields. Validate their values,
// then compare every frozen field exactly. No other additions are discarded.
func validateApprovedAliases(t *testing.T, result any) {
	t.Helper()
	m, ok := result.(map[string]any)
	if !ok {
		return
	}
	validate := func(events any) {
		list, ok := events.([]any)
		if !ok {
			return
		}
		for _, value := range list {
			event, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("non-object event: %v", value)
			}
			for _, pair := range [][2]string{{"timestamp", "Timestamp"}, {"metric", "Metric"}, {"value", "Value"}, {"tags", "Tags"}} {
				alias, present := event[pair[0]]
				legacy, oldPresent := event[pair[1]]
				if !present || !oldPresent {
					t.Errorf("missing event alias/legacy field %v in %v", pair, event)
				}
				equal := reflect.DeepEqual(alias, legacy)
				if pair[0] == "tags" && legacy == nil {
					empty, ok := alias.(map[string]any)
					equal = ok && len(empty) == 0
				}
				if !equal {
					t.Errorf("event alias mismatch %s=%v %s=%v", pair[0], alias, pair[1], legacy)
				}
				delete(event, pair[0])
			}
		}
	}
	if events, ok := m["events"]; ok {
		validate(events)
	}
	if metrics, ok := m["metrics"].(map[string]any); ok {
		for _, events := range metrics {
			validate(events)
		}
	}
}
