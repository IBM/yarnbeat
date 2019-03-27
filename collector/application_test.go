package collector

import (
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/IBM/yarnbeat/config"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAppCollector_StartStop(t *testing.T) {
	rmQueries := 0
	hsQueries := 0
	testResourceManager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rmQueries += 1
		params := r.URL.Query()
		if params.Get("states") == "RUNNING" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(runningAppPayload))
		} else if params.Get("states") == "FINISHED" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(finishedAppPayload))
		} else {
			w.WriteHeader(http.StatusExpectationFailed)
			w.Write([]byte(fmt.Sprint("Expected query param states= RUNNING or FINISHED, got ", params.Get("states"))))
		}
	}))

	testHistoryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hsQueries += 1

		if r.URL.Path == "/ws/v1/history/mapreduce/jobs/job_1542655614241_1092" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(testMrJobPayload))
		} else if r.URL.Path == "/ws/v1/history/mapreduce/jobs/job_1542655614241_1092/counters" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(testMrCountersPayload))
		} else {
			t.Error("History server query contained unexpected path: ", r.URL.Path)
		}

	}))

	event := make(chan *beat.Event)
	c := AppCollector{
		Config: &config.Config{
			ResourceManagers: []string{testResourceManager.URL},
			Period: 1,
			EnableMapReduce: true,
			HistoryServer: testHistoryServer.URL,
			MapReduceCounters: []config.MapReduceCounterConfig{{GroupName: "org.test", Counters: map[string]string{"counterA": "counter_a"}}},
		},
		Event:  event,
		Logger: logp.NewLogger("test"),
	}

	startTime := time.Now()
	stopped := make(chan struct{})
	go c.Start()

	for i := 0; i < 4; i++ {
		select {
		case <-stopped:
			if i != 3 {
				t.Errorf("collector stopped before returning all events (i=%d)", i)
			}
		case <-event:
			if i == 2 {
				wg := sync.WaitGroup{}
				c.Stop(wg)
				go func() {
					defer close(stopped)
					wg.Wait()
				}()
			}
			if i == 3 {
				t.Error("collector generated too many events")
			}
		case <-time.After(time.Duration(1500) * time.Millisecond):
			if i == 3 {
				t.Error("collector failed to stop")
			} else {
				t.Fatal("collector did not receive events")
			}
		}
	}

	if !c.lastRun.After(startTime) {
		t.Error("expected lastRun to be after the start time")
	}

	if rmQueries != 2 {
		t.Error( "expected 2 resource manager queries, got ", rmQueries)
	}
	if hsQueries != 2 {
		t.Error( "expected 2 resource manager query, got ", hsQueries)
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

var runningAppPayload = `
{
  "apps": {
    "app": [
      {
        "id": "application_1542655614241_1094",
        "user": "test-user",
        "name": "test job",
        "queue": "default",
        "state": "RUNNING",
        "finalStatus": "UNDEFINED",
        "progress": 100.0,
        "trackingUI": "ApplicationMaster",
        "trackingUrl": "http://localhost:8088/proxy/application_1542655614241_1094/",
        "clusterId": 1548431160089,
        "applicationType": "SPARK",
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
        "name": "test job",
        "queue": "default",
        "state": "RUNNING",
        "finalStatus": "UNDEFINED",
        "progress": 100.0,
        "trackingUI": "ApplicationMaster",
        "trackingUrl": "http://localhost:8088/proxy/application_1542655614241_1095/",
        "clusterId": 1548431160089,
        "applicationType": "SPARK",
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
        "name": "test job",
        "queue": "default",
        "state": "FINISHED",
        "finalStatus": "SUCCESS",
        "progress": 100.0,
        "trackingUI": "ApplicationMaster",
        "trackingUrl": "http://localhost:8088/proxy/application_1542655614241_1092/",
        "clusterId": 1548431160089,
        "applicationType": "MAPREDUCE",
        "applicationTags": "tag1,tag2",
        "priority": 0,
        "startedTime": 1543533091923,
        "finishedTime": 0,
        "elapsedTime": 8960870187,
        "amContainerLogs": "http://localhost:8042/node/containerlogs/container_e63_1542655614241_1092_02_000001/test-user",
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

var testMrJobPayload = `
{
  "job": {
    "submitTime": 1553092851187,
    "startTime": 1553092860935,
    "finishTime": 1553092897998,
    "id": "job_1542655614241_1092",
    "name": "ProviderRollupProcess-379",
    "queue": "datagrid",
    "user": "jenkins",
    "state": "SUCCEEDED",
    "mapsTotal": 1,
    "mapsCompleted": 1,
    "reducesTotal": 50,
    "reducesCompleted": 50,
    "uberized": false,
    "avgMapTime": 9176,
    "avgReduceTime": 1172,
    "avgShuffleTime": 6715,
    "avgMergeTime": 46,
    "failedReduceAttempts": 0,
    "killedReduceAttempts": 0,
    "successfulReduceAttempts": 50,
    "failedMapAttempts": 0,
    "killedMapAttempts": 0,
    "successfulMapAttempts": 1
  }
}`

var testMrCountersPayload = `
{
  "jobCounters": {
    "id": "job_1542655614241_1092",
    "counterGroup": [
      {
        "counterGroupName": "org.test",
        "counter": [
          {
            "name": "counterA",
            "totalCounterValue": 1234,
            "mapCounterValue": 0,
            "reduceCounterValue": 1234
          }
        ]
      }
    ]
  }
}`