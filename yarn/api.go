package yarn

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type Application struct {
	Id                      string  `field:"yarn.app.id"`
	Name                    string  `field:"yarn.app.name"`
	ApplicationTags         string  `field:",yarn.app.tags"`
	ApplicationType         string  `field:"yarn.app.type"`
	User                    string  `field:"yarn.app.user"`
	State                   string  `field:"yarn.app.state"`
	Queue                   string  `field:"yarn.app.queue"`
	Priority                int     `field:"yarn.app.priority"`
	Progress                float64 `field:"yarn.app.progress"`
	AmContainerLogs         string  `field:"yarn.app.container_logs"`
	AllocatedMB             int     `field:"yarn.app.allocated.mb"`
	AllocatedVCores         int     `field:"yarn.app.allocated.vcores"`
	RunningContainers       int     `field:"yarn.app.allocated.containers"`
	TrackingUrl             string  `field:"yarn.app.tracking_url"`
	StartedTime             uint64  `field:"yarn.app.start"`
	FinishedTime            uint64  `field:"yarn.app.finish"`
	ElapsedTime             uint64  `field:"yarn.app.elapsed.ms"`
	FinalStatus             string  `field:"yarn.app.final_status"`
	VcoreSeconds            uint64  `field:"yarn.app.vcore_sec"`
	MemorySeconds           uint64  `field:"yarn.app.memory_sec"`
	NumAMContainerPreempted int     `field:"yarn.app.preempted.containers"`
	PreemptedResourceMB     uint64  `field:"yarn.app.preempted.mb"`
	PreemptedResourceVCores uint64  `field:"yarn.app.preempted.vcores"`
}

type ClusterMetrics struct {
	AppsSubmitted         int    `field:"yarn.cluster.apps.submitted"`
	AppsCompleted         int    `field:"yarn.cluster.apps.completed"`
	AppsPending           int    `field:"yarn.cluster.apps.pending"`
	AppsRunning           int    `field:"yarn.cluster.apps.running"`
	AppsFailed            int    `field:"yarn.cluster.apps.failed"`
	AppsKilled            int    `field:"yarn.cluster.apps.killed"`
	ReservedMb            uint64 `field:"yarn.cluster.memory.reserved.mb"`
	AvailableMb           uint64 `field:"yarn.cluster.memory.available.mb"`
	AllocatedMb           uint64 `field:"yarn.cluster.memory.allocated.mb"`
	TotalMb               uint64 `field:"yarn.cluster.memory.total.mb"`
	ReservedVirtualCores  uint64 `field:"yarn.cluster.vcores.reserved"`
	AvailableVirtualCores uint64 `field:"yarn.cluster.vcores.available"`
	AllocatedVirtualCores uint64 `field:"yarn.cluster.vcores.allocated"`
	TotalVirtualCores     uint64 `field:"yarn.cluster.vcores.total"`
	ContainersAllocated   int    `field:"yarn.cluster.containers.allocated"`
	ContainersReserved    int    `field:"yarn.cluster.containers.reserved"`
	ContainersPending     int    `field:"yarn.cluster.containers.pending"`
	TotalNodes            int    `field:"yarn.cluster.nodes.total"`
	ActiveNodes           int    `field:"yarn.cluster.nodes.active"`
	LostNodes             int    `field:"yarn.cluster.nodes.lost"`
	UnhealthyNodes        int    `field:"yarn.cluster.nodes.unhealthy"`
	DecommissioningNodes  int    `field:"yarn.cluster.nodes.decommissioning"`
	DecommissionedNodes   int    `field:"yarn.cluster.nodes.decommissioned"`
	RebootedNodes         int    `field:"yarn.cluster.nodes.rebooted"`
	ShutdownNodes         int    `field:"yarn.cluster.nodes.shutdown"`
}

type appWrapper struct {
	App []Application
}

type appsWrapper struct {
	Apps appWrapper
}

type clusterMetricsWrapper struct {
	ClusterMetrics ClusterMetrics
}

func GetRunningApps(resourceManagers []string, client http.Client) ([]Application, error) {
	rawQuery := "states=RUNNING"
	return getApplications(resourceManagers, client, rawQuery)
}

func GetFinishedApps(resourceManagers []string, client http.Client, since time.Time) ([]Application, error) {
	rawQuery := fmt.Sprint("states=FINISHED&finishedTimeBegin=", since.Unix()*1000)
	return getApplications(resourceManagers, client, rawQuery)
}

func GetClusterMetrics(resourceManagers []string, client http.Client) (*ClusterMetrics, error) {
	data, err := yarnGet(resourceManagers, client, "ws/v1/cluster/metrics", "")

	var payload clusterMetricsWrapper
	err = json.Unmarshal(data, &payload)
	if err != nil {
		return nil, err
	}
	return &payload.ClusterMetrics, nil
}

func getApplications(resourceManagers []string, client http.Client, rawQuery string) ([]Application, error) {
	data, err := yarnGet(resourceManagers, client, "/ws/v1/cluster/apps", rawQuery)
	if err != nil {
		return nil, err
	}

	var payload appsWrapper
	err = json.Unmarshal(data, &payload)
	if err != nil {
		return nil, err
	}
	return payload.Apps.App, nil
}

func yarnGet(resourceManagers []string, client http.Client, path string, rawQuery string) ([]byte, error) {
	var lastErr error
	for _, rm := range resourceManagers {
		u, err := url.Parse(rm)
		if err != nil {
			lastErr = err
			continue
		}
		u.Path = path
		u.RawQuery = rawQuery
		urlStr := u.String()
		resp, err := client.Get(urlStr)
		if err != nil {
			lastErr = err
		} else {
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				lastErr = err
				continue
			}

			if resp.StatusCode != 200 {
				lastErr = errors.New(fmt.Sprintf("error encountered while polling %s: [%d] %s",
					urlStr, resp.StatusCode, string(data)))
				continue
			}

			return data, nil
		}
	}
	return nil, lastErr
}