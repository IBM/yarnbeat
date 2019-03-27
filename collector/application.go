package collector

import (
	"errors"
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/IBM/yarnbeat/config"
	"github.com/IBM/yarnbeat/helper"
	"github.com/IBM/yarnbeat/mapreduce"
	"github.com/IBM/yarnbeat/yarn"
	"net/http"
	"strings"
	"sync"
	"time"
)

type AppCollector struct {
	Event     chan<- *beat.Event
	Logger    *logp.Logger
	Config    *config.Config
	done      chan bool
	client    http.Client
	lastRun   time.Time
}

func (c *AppCollector) Start() {
	ticker := time.NewTicker(time.Duration(c.Config.Period) * time.Second)
	c.client = http.Client{}
	c.lastRun = time.Now()
	c.done = make(chan bool)
	for {
		select {
		case <-c.done:
			close(c.done)
			return
		case <-ticker.C:
			c.poll()
		}

	}
}

func (c *AppCollector) Stop(wg sync.WaitGroup) {
	wg.Add(1)
	c.done<-true
	wg.Done()
}

func (c *AppCollector) poll() {
	c.Logger.Debug("Polling YARN API for applications")
	runningApps, err := yarn.GetRunningApps(c.Config.ResourceManagers, c.client)
	if err != nil {
		c.Logger.Error("Failed to collect running YARN applications: ", err)
		runningApps = []yarn.Application{}
	}

	finishedApps, err := yarn.GetFinishedApps(c.Config.ResourceManagers, c.client, c.lastRun)
	if err != nil {
		c.Logger.Error("Failed to collect finished YARN applications: ", err)
		finishedApps = []yarn.Application{}
	}

	applications := append(runningApps, finishedApps...)

	now := time.Now()
	c.lastRun = now

	for i := range applications {
		event := beat.Event{Timestamp: now, Fields: common.MapStr{}}
		event.Fields.Put("type", "application")
		application := &applications[i]
		err = helper.SetEventFields(&event, *application)
		if err != nil {
			c.Logger.Errorf("Failed to create event for application %s: %s", application.Id, err)
			continue
		}

		if c.Config.EnableMapReduce &&
			application.ApplicationType == "MAPREDUCE" &&
			application.State == "FINISHED" {
			err = c.addMapReduceFields(application, event)
			if err != nil {
				c.Logger.Warnf("Failed to add mapreduce fields for application %s: %s", application.Id, err)
			}
		}

		c.Event <- &event
	}
}

func (c *AppCollector) addMapReduceFields(application *yarn.Application, event beat.Event) error {
	jobId := strings.Replace(application.Id, "application", "job", 1)
	job, err := mapreduce.GetJobSummary(c.Config.HistoryServer, c.client, jobId)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to retrieve mapreduce job summary for job %s: %s", jobId, err))
	}

	var counters []mapreduce.CounterGroup
	if len(c.Config.MapReduceCounters) > 0 {
		counters, err = mapreduce.GetJobCounters(c.Config.HistoryServer, c.client, jobId)
		if err != nil {
			c.Logger.Warnf("Failed to retrieve mapreduce counters for job %s: %s", jobId, err)
			counters = make([]mapreduce.CounterGroup, 0)
		}
	} else {
		counters = make([]mapreduce.CounterGroup, 0)
	}

	err = helper.SetMapReduceEventFields(&event, c.Config.MapReduceCounters, job, counters)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to set event fields for job %s: %s", jobId, err))
	}

	return nil
}
