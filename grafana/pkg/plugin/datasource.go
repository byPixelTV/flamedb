package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

type settings struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
	MaxScanEvents  int    `json:"maxScanEvents"`
}
type filter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type queryModel struct {
	Metrics     []string `json:"metrics"`
	Filters     []filter `json:"filters"`
	Mode        string   `json:"mode"`
	Aggregation string   `json:"aggregation"`
	Interval    string   `json:"interval"`
	GroupBy     []string `json:"groupBy"`
	Legend      string   `json:"legend"`
}
type Datasource struct {
	client  *wireClient
	maxScan int
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9_.:/-]+$`)

func New(_ context.Context, s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var cfg settings
	if err := json.Unmarshal(s.JSONData, &cfg); err != nil {
		return nil, err
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 7777
	}
	if cfg.TimeoutSeconds == 0 {
		cfg.TimeoutSeconds = 10
	}
	if cfg.MaxScanEvents == 0 {
		cfg.MaxScanEvents = 50000
	}
	if cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutSeconds < 1 || cfg.TimeoutSeconds > 120 || cfg.MaxScanEvents < 100 || cfg.MaxScanEvents > 200000 {
		return nil, errors.New("invalid port, timeout (1–120s), or scan limit (100–200000)")
	}
	key := s.DecryptedSecureJSONData["apiKey"]
	if key == "" || strings.ContainsAny(key, " \t\r\n\x00\x1f") {
		return nil, errors.New("a valid FlameDB API key is required")
	}
	return &Datasource{client: &wireClient{address: net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)), apiKey: key, timeout: time.Duration(cfg.TimeoutSeconds) * time.Second, slots: make(chan struct{}, 8)}, maxScan: cfg.MaxScanEvents}, nil
}

func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	_, err := d.client.query(ctx, "METRICS")
	if err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: "Connected to FlameDB; metric discovery succeeded."}, nil
}

func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	out := backend.NewQueryDataResponse()
	for _, q := range req.Queries {
		var model queryModel
		if err := json.Unmarshal(q.JSON, &model); err != nil {
			out.Responses[q.RefID] = backend.ErrDataResponse(backend.StatusBadRequest, "invalid query model")
			continue
		}
		frames, err := d.execute(ctx, q, model)
		if err != nil {
			out.Responses[q.RefID] = backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
			continue
		}
		for _, f := range frames {
			f.RefID = q.RefID
			if model.Legend != "" && model.Mode != "table" && len(f.Fields) > 1 {
				f.Fields[1].Config = &data.FieldConfig{DisplayNameFromDS: f.Fields[1].Name}
			}
		}
		out.Responses[q.RefID] = backend.DataResponse{Frames: frames}
	}
	return out, nil
}

func validate(m *queryModel) error {
	if len(m.Metrics) == 0 || len(m.Metrics) > 16 {
		return errors.New("select between 1 and 16 metrics")
	}
	for _, v := range append(append([]string{}, m.Metrics...), m.GroupBy...) {
		if !identifier.MatchString(v) {
			return errors.New("invalid metric or grouping tag")
		}
	}
	if len(m.Filters) > 32 || len(m.GroupBy) > 8 {
		return errors.New("too many filters or grouping tags")
	}
	seen := map[string]bool{}
	for _, f := range m.Filters {
		if !identifier.MatchString(f.Key) || seen[f.Key] {
			return errors.New("invalid or duplicate filter key")
		}
		seen[f.Key] = true
	}
	if m.Mode == "" {
		m.Mode = "timeseries"
	}
	if m.Mode != "timeseries" && m.Mode != "latest" && m.Mode != "table" {
		return errors.New("invalid output mode")
	}
	if m.Aggregation == "" {
		m.Aggregation = "AVG"
	}
	if m.Aggregation != "AVG" && m.Aggregation != "SUM" && m.Aggregation != "COUNT" {
		return errors.New("invalid aggregation")
	}
	return nil
}

func where(filters []filter) string {
	clauses := make([]string, 0, len(filters))
	for _, f := range filters {
		clauses = append(clauses, f.Key+"="+strconv.Quote(f.Value))
	}
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}

func bucketInterval(q backend.DataQuery, m queryModel) (time.Duration, error) {
	interval := q.Interval
	if m.Interval != "" && m.Interval != "auto" {
		var err error
		interval, err = time.ParseDuration(m.Interval)
		if err != nil {
			return 0, errors.New("interval must be a duration, for example 10s or 1m")
		}
	}
	if m.Interval != "" && m.Interval != "auto" && interval <= 0 {
		return 0, errors.New("interval must be positive")
	}
	if interval < time.Second {
		interval = time.Second
	}
	maxPoints := q.MaxDataPoints
	if maxPoints <= 0 || maxPoints > 10000 {
		maxPoints = 10000
	}
	minimum := q.TimeRange.To.Sub(q.TimeRange.From) / time.Duration(maxPoints)
	if minimum > interval {
		interval = (minimum/time.Second + 1) * time.Second
	}
	return interval, nil
}

func (d *Datasource) execute(ctx context.Context, q backend.DataQuery, m queryModel) (data.Frames, error) {
	if err := validate(&m); err != nil {
		return nil, err
	}
	if !q.TimeRange.To.After(q.TimeRange.From) {
		return nil, errors.New("invalid time range")
	}
	interval, err := bucketInterval(q, m)
	if err != nil {
		return nil, err
	}
	suffix := where(m.Filters) + " FROM " + q.TimeRange.From.UTC().Format(time.RFC3339Nano) + " TO " + q.TimeRange.To.UTC().Format(time.RFC3339Nano)
	var frames data.Frames
	all := map[string][]event{}
	for _, metric := range m.Metrics {
		command := "GET " + metric + suffix
		native := m.Mode == "timeseries" && len(m.GroupBy) == 0
		if native {
			command += " GROUP BY " + interval.String() + " " + m.Aggregation
		} else if m.Mode == "latest" && len(m.GroupBy) == 0 {
			command += " LIMIT 1 ORDER DESC"
		} else {
			command += " LIMIT " + strconv.Itoa(d.maxScan+1) + " ORDER ASC"
		}
		r, err := d.client.query(ctx, command)
		if err != nil {
			return nil, err
		}
		if native {
			sort.Slice(r.Series, func(i, j int) bool { return r.Series[i].TS < r.Series[j].TS })
			times := []time.Time{}
			values := []*float64{}
			for _, p := range r.Series {
				times = append(times, time.Unix(0, p.TS).UTC())
				if p.Count == 0 {
					values = append(values, nil)
				} else {
					v := p.Value
					values = append(values, &v)
				}
			}
			frame := data.NewFrame(metric, data.NewField("Time", nil, times), data.NewField(seriesName(metric, nil, m.Legend), nil, values))
			frame.Meta = &data.FrameMeta{Type: data.FrameTypeTimeSeriesMulti}
			frames = append(frames, frame)
		} else {
			if len(r.Events) > d.maxScan {
				return nil, fmt.Errorf("%s exceeds the scan limit (%d events); narrow the time range or add filters", metric, d.maxScan)
			}
			all[metric] = r.Events
		}
	}
	if len(all) > 0 {
		groups := map[string]bool{}
		for _, events := range all {
			for _, e := range events {
				key, _ := groupKey(e.Tags, m.GroupBy)
				groups[key] = true
			}
		}
		limit := 1000
		if m.Mode == "table" {
			limit = 10000
		}
		if len(groups) > limit {
			return nil, fmt.Errorf("query exceeds %d groups; add filters", limit)
		}
		if m.Mode == "timeseries" {
			frames = groupedSeries(all, m, interval)
		} else {
			frames = latestFrames(all, m)
		}
	}
	return frames, nil
}

func seriesName(metric string, labels map[string]string, legend string) string {
	if legend == "" {
		return metric
	}
	name := strings.ReplaceAll(legend, "{{metric}}", metric)
	for k, v := range labels {
		name = strings.ReplaceAll(name, "{{"+k+"}}", v)
	}
	return name
}

func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	send := func(status int, v any) error {
		body, _ := json.Marshal(v)
		return sender.Send(&backend.CallResourceResponse{Status: status, Headers: map[string][]string{"Content-Type": {"application/json"}}, Body: body})
	}
	if req.Method != http.MethodGet {
		return send(405, map[string]string{"error": "GET required"})
	}
	switch strings.Trim(req.Path, "/") {
	case "metrics":
		r, err := d.client.query(ctx, "METRICS")
		if err != nil {
			return send(502, map[string]string{"error": err.Error()})
		}
		if r.MetricKeys == nil {
			r.MetricKeys = []string{}
		}
		sort.Strings(r.MetricKeys)
		return send(200, r.MetricKeys)
	case "tags":
		// Metadata is deliberately bounded. Custom entries in the editor cover tags
		// absent from this recent sample; no full database scan for a dropdown.
		request, err := http.NewRequest(http.MethodGet, req.URL, nil)
		if err != nil {
			return send(400, map[string]string{"error": "invalid URL"})
		}
		metric := request.URL.Query().Get("metric")
		if !identifier.MatchString(metric) {
			return send(400, map[string]string{"error": "invalid metric"})
		}
		r, err := d.client.query(ctx, "GET "+metric+" LIMIT 1000 ORDER DESC")
		if err != nil {
			return send(502, map[string]string{"error": err.Error()})
		}
		sets := map[string]map[string]bool{}
		for _, e := range r.Events {
			for k, v := range e.Tags {
				if sets[k] == nil {
					sets[k] = map[string]bool{}
				}
				sets[k][v] = true
			}
		}
		tags := map[string][]string{}
		for k, vs := range sets {
			for v := range vs {
				tags[k] = append(tags[k], v)
			}
			sort.Strings(tags[k])
		}
		return send(200, tags)
	default:
		return send(404, map[string]string{"error": "unknown resource"})
	}
}
