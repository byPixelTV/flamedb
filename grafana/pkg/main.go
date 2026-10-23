package main

import (
	"github.com/byPixelTV/flamedb/grafana/pkg/plugin"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"os"
)

func main() {
	if err := datasource.Manage("flamedb-flamedb-datasource", plugin.New, datasource.ManageOpts{}); err != nil {
		os.Exit(1)
	}
}
