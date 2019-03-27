package mapreduce

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func Test_GetJobSummary(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/ws/v1/history/mapreduce/jobs/job_1542655614241_1092"
		if r.URL.Path !=  expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(testJobPayload))
	}))

	job, err := GetJobSummary(testServer.URL, http.Client{}, "job_1542655614241_1092")
	if err != nil {
		t.Fatal("Failed to get job summary: ", err)
	}

	if job.Queue != "default" {
		t.Error("Failed to parse job queue")
	}

	if job.Name != "Test job" {
		t.Error("Failed to parse job name")
	}

	if job.ReducesTotal != 50 {
		t.Error("Failed to parse job reducesTotal")
	}
}

func Test_GetJobCounters(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/ws/v1/history/mapreduce/jobs/job_1542655614241_1092/counters"
		if r.URL.Path !=  expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(testCountersPayload))
	}))

	counters, err := GetJobCounters(testServer.URL, http.Client{}, "job_1542655614241_1092")
	if err != nil {
		t.Fatal("failed to get job summary: ", err)
	}

	if len(counters) != 1 {
		t.Fatal("expected 1 counter group, got ", len(counters))
	}

	if counters[0].CounterGroupName != "org.test" {
		t.Errorf("failed to parse counter group name, expected \"org.test\", got \"%s\"", counters[0].CounterGroupName)
	}

	if len(counters[0].Counters) != 1 {
		t.Fatal("expected 1 counter, got ", len(counters[0].Counters))
	}

	if counters[0].Counters[0].Name != "counterA" {
		t.Errorf("failed to parse counter name, expected \"counterA\", got \"%s\"", counters[0].Counters[0].Name)
	}

	if counters[0].Counters[0].TotalCounterValue != 1234 {
		t.Error("failed to parse counter name, expected 1234, got ", counters[0].Counters[0].TotalCounterValue)
	}
}

var testJobPayload = `
{
  "job": {
    "submitTime": 1553092851187,
    "startTime": 1553092860935,
    "finishTime": 1553092897998,
    "id": "job_1542655614241_1092",
    "name": "Test job",
    "queue": "default",
    "user": "test-user",
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

var testCountersPayload = `
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