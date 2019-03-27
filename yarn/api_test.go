package yarn

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func Test_GetRunningApps(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/ws/v1/cluster/apps"
		if r.URL.Path !=  expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		expectedQuery := "states=RUNNING"
		if r.URL.RawQuery != expectedQuery {
			t.Errorf("Expected query %s, got %s", expectedQuery, r.URL.RawQuery)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(runningAppsPayload))
	}))

	client := http.Client{}
	apps, err := GetRunningApps([]string{testServer.URL}, client)
	if err != nil {
		t.Fatal("Failed to get applications: ", err.Error())
	}

	if len(apps) != 2 {
		t.Fatal("Expected 2 applications, got ", len(apps))
	}

	if apps[0].Id != "application_1542655614241_1094" {
		t.Error("Failed to parse application ID")
	}

	if apps[0].VcoreSeconds != 47753402 {
		t.Error("Failed to parse vcore seconds")
	}
}

func Test_GetFinishedApps(t *testing.T) {
	now := time.Now()
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/ws/v1/cluster/apps"
		if r.URL.Path !=  expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		expectedQuery := fmt.Sprint("states=FINISHED&finishedTimeBegin=", now.Unix()*1000)
		if r.URL.RawQuery != expectedQuery {
			t.Errorf("Expected query %s, got %s", expectedQuery, r.URL.RawQuery)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(finishedAppPayload))
	}))

	client := http.Client{}
	apps, err := GetFinishedApps([]string{testServer.URL}, client, now)
	if err != nil {
		t.Fatal("Failed to get applications: ", err.Error())
	}

	if len(apps) != 1 {
		t.Fatal("Expected 1 applications, got ", len(apps))
	}

	if apps[0].Id != "application_1542655614241_1092" {
		t.Error("Failed to parse application ID")
	}

	if apps[0].VcoreSeconds != 47753402 {
		t.Error("Failed to parse vcore seconds")
	}
}

func Test_getApplications(t *testing.T) {
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	client := http.Client{}
	rawQuery := "states=RUNNING"
	_, err := getApplications([]string{failServer.URL}, client, rawQuery)
	if err == nil {
		t.Error("expected an error on non-200 response")
	}

	failServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<p>foo bar</p>`))
	}))
	rawQuery = "states=RUNNING"
	_, err = getApplications([]string{failServer.URL}, client, rawQuery)
	if err == nil {
		t.Error("expected an error when response was not JSON")
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(runningAppsPayload))
	}))
	failServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`<p>Server encountered an error</p>`))
	}))
	rawQuery = "states=RUNNING"
	_, err = getApplications([]string{failServer.URL, testServer.URL}, client, rawQuery)
	if err != nil {
		t.Error("getApplications failed to try all given resource managers: ", err)
	}
}

func Test_GetClusterMetrics(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/ws/v1/cluster/metrics"
		if r.URL.Path !=  expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(clusterMetricsPayload))
	}))

	client := http.Client{}
	cm, err := GetClusterMetrics([]string{testServer.URL}, client)
	if err != nil {
		t.Fatal("Failed to get cluster metrics: ", err.Error())
	}

	if cm.AppsCompleted != 2993 {
		t.Error("Failed to parse appsCompleted")
	}

	if cm.AvailableMb != 1180672 {
		t.Error("Failed to parse availableMb")
	}
}

var runningAppsPayload = `
{
  "apps": {
    "app": [
      {
        "id": "application_1542655614241_1094",
        "user": "test-user",
        "name": "test-application",
        "queue": "default",
        "state": "RUNNING",
        "finalStatus": "UNDEFINED",
        "progress": 100.0,
        "trackingUI": "ApplicationMaster",
        "trackingUrl": "http://localhost:8088/proxy/application_1542655614241_1094/",
        "clusterId": 1548431160089,
        "applicationType": "MAPREDUCE",
        "applicationTags": "tag1,tag2",
        "priority": 0,
        "startedTime": 1543533091923,
        "finishedTime": 0,
        "elapsedTime": 8960870187,
        "amContainerLogs": "http://localhost:8042/node/containerlogs/container_e63_1542655614241_1094_02_000001/test-user",
        "allocatedMB": 11264,
        "allocatedVCores": 4,
        "runningContainers": 4,
        "memorySeconds": 141587997542,
        "vcoreSeconds": 47753402,
        "preemptedResourceMB": 0,
        "preemptedResourceVCores": 0,
        "numAMContainerPreempted": 0,
        "preemptedMemorySeconds": 85371591338,
        "preemptedVcoreSeconds": 27790212
      },
      {
        "id": "application_1542655614241_1095",
        "user": "test-user",
        "name": "test-application",
        "queue": "default",
        "state": "RUNNING",
        "finalStatus": "UNDEFINED",
        "progress": 100.0,
        "trackingUI": "ApplicationMaster",
        "trackingUrl": "http://localhost:8088/proxy/application_1542655614241_1095/",
        "clusterId": 1548431160089,
        "applicationType": "MAPREDUCE",
        "applicationTags": "tag3,tag4",
        "priority": 0,
        "startedTime": 1543533091923,
        "finishedTime": 0,
        "elapsedTime": 8960870187,
        "amContainerLogs": "http://localhost:8042/node/containerlogs/container_e63_1542655614241_1095_02_000001/test-user",
        "allocatedMB": 11264,
        "allocatedVCores": 4,
        "runningContainers": 4,
        "memorySeconds": 141587997542,
        "vcoreSeconds": 47753402,
        "preemptedResourceMB": 0,
        "preemptedResourceVCores": 0,
        "numAMContainerPreempted": 0,
        "preemptedMemorySeconds": 85371591338,
        "preemptedVcoreSeconds": 27790212
      }
    ]
  }
}`

var finishedAppPayload = `
{
  "apps": {
    "app": [
      {
        "id": "application_1542655614241_1092",
        "user": "test-user",
        "name": "test-application",
        "queue": "default",
        "state": "FINISHED",
        "finalStatus": "SUCCESS",
        "progress": 100.0,
        "trackingUI": "ApplicationMaster",
        "trackingUrl": "http://localhost:8088/proxy/application_1542655614241_1094/",
        "clusterId": 1548431160089,
        "applicationType": "MAPREDUCE",
        "applicationTags": "tag1,tag2",
        "priority": 0,
        "startedTime": 1543533091923,
        "finishedTime": 0,
        "elapsedTime": 8960870187,
        "amContainerLogs": "http://localhost:8042/node/containerlogs/container_e63_1542655614241_1094_02_000001/test-user",
        "allocatedMB": 11264,
        "allocatedVCores": 4,
        "runningContainers": 4,
        "memorySeconds": 141587997542,
        "vcoreSeconds": 47753402,
        "preemptedResourceMB": 0,
        "preemptedResourceVCores": 0,
        "numAMContainerPreempted": 0,
        "preemptedMemorySeconds": 85371591338,
        "preemptedVcoreSeconds": 27790212
      }
    ]
  }
}`

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