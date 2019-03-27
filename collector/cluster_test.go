package collector

import (
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/IBM/yarnbeat/config"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)


func TestClusterCollector_StartStop(t *testing.T) {
	var testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(clusterMetricsPayload))
	}))

	event := make(chan *beat.Event)
	c := ClusterCollector{
		Config: &config.Config{
			ResourceManagers: []string{testServer.URL},
			Period: 1,
		},
		Event:  event,
		Logger: logp.NewLogger("test"),
	}

	go c.Start()

	select {
	case <- event:
	case <- time.After(time.Duration(1500) * time.Millisecond):
		t.Error("collector failed to generate event")
	}

	stopped := make(chan struct{})
	wg := sync.WaitGroup{}
	c.Stop(wg)
	go func() {
		defer close(stopped)
		wg.Wait()
	}()
	select {
	case <- stopped:
	case <- time.After(time.Duration(1500) * time.Millisecond):
		t.Error("collector failed to stop")
	}
}

var clusterMetricsPayload = `{
  "clusterMetrics": {
    "appsSubmitted": 2994,
    "appsCompleted": 2993,
    "appsPending": 0,
    "appsRunning": 1,
    "appsFailed": 0,
    "appsKilled": 0,
    "reservedMB": 0,
    "availableMB": 1180672,
    "allocatedMB": 11264,
    "reservedVirtualCores": 0,
    "availableVirtualCores": 284,
    "allocatedVirtualCores": 4,
    "containersAllocated": 4,
    "containersReserved": 0,
    "containersPending": 0,
    "totalMB": 1191936,
    "totalVirtualCores": 288,
    "totalNodes": 12,
    "lostNodes": 0,
    "unhealthyNodes": 0,
    "decommissioningNodes": 0,
    "decommissionedNodes": 0,
    "rebootedNodes": 0,
    "activeNodes": 12,
    "shutdownNodes": 0
  }
}`