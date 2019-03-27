package mapreduce

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

type Job struct {
	Id                        string `field:"mapreduce.job.id"`
	Name                      string `field:"mapreduce.job.name"`
	Queue                     string `field:"mapreduce.job.queue"`
	User                      string `field:"mapreduce.job.user"`
	State                     string `field:"mapreduce.job.state"`
	SubmitTime                uint64 `field:"mapreduce.job.submit_time"`
	StartTime                 uint64 `field:"mapreduce.job.start_time"`
	FinishTime                uint64 `field:"mapreduce.job.finish_time"`
	MapsTotal                 int    `field:"mapreduce.job.maps.total"`
	MapsCompleted             int    `field:"mapreduce.job.maps.completed"`
	AvgMapTime                uint64 `field:"mapreduce.job.maps.avg_time"`
	FailedMapAttempts         int    `field:"mapreduce.job.maps.attempts.failed"`
	KilledMapAttempts         int    `field:"mapreduce.job.maps.attempts.killed"`
	SuccessfullMapAttempts    int    `field:"mapreduce.job.maps.attempts.successful"`
	ReducesTotal              int    `field:"mapreduce.job.reduces.total"`
	ReducesCompleted          int    `field:"mapreduce.job.reduces.completed"`
	AvgReduceTime             uint64 `field:"mapreduce.job.reduces.avg_time"`
	FailedReduceAttempts      int    `field:"mapreduce.job.reduces.attempts.failed"`
	KilledReduceAttempts      int    `field:"mapreduce.job.reduces.attempts.killed"`
	SuccessfullReduceAttempts int    `field:"mapreduce.job.reduces.attempts.successful"`
	AvgShuffleTime            uint64 `field:"mapreduce.job.shuffles.avg_time"`
	AvgMergeTime              uint64 `field:"mapreduce.job.merges.avg_time"`
	Uberized                  bool   `field:"mapreduce.job.uberized"`
}

type Counter struct {
	Name               string
	ReduceCounterValue uint64
	MapCounterValue    uint64
	TotalCounterValue  uint64
}

type CounterGroup struct {
	CounterGroupName string
	Counters         []Counter `json:"counter"`
}

type jobCounters struct {
	Id            string
	CounterGroups []CounterGroup `json:"counterGroup"`
}

type jobCountersWrapper struct {
	JobCounters jobCounters
}

type jobWrapper struct {
	Job Job
}

func GetJobSummary(historyServer string, client http.Client, jobId string) (*Job, error) {
	u, err := url.Parse(historyServer)
	if err != nil {
		return nil, err
	}

	u.Path = fmt.Sprintf("ws/v1/history/mapreduce/jobs/%s", jobId)
	urlStr := u.String()
	resp, err := client.Get(urlStr)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("error encountered while polling %s: [%d] %s",
			urlStr, resp.StatusCode, string(data)))
		return nil, err
	}

	var payload jobWrapper
	err = json.Unmarshal(data, &payload)
	if err != nil {
		return nil, err
	}
	return &payload.Job, nil
}

func GetJobCounters(historyServer string, client http.Client, jobId string) ([]CounterGroup, error) {
	u, err := url.Parse(historyServer)
	if err != nil {
		return nil, err
	}

	u.Path = fmt.Sprintf("ws/v1/history/mapreduce/jobs/%s/counters", jobId)
	urlStr := u.String()
	resp, err := client.Get(urlStr)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("error encountered while polling %s: [%d] %s",
			urlStr, resp.StatusCode, string(data)))
		return nil, err
	}

	var payload jobCountersWrapper
	err = json.Unmarshal(data, &payload)
	if err != nil {
		return nil, err
	}

	return payload.JobCounters.CounterGroups, nil
}
