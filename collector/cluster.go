package collector

import (
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/IBM/yarnbeat/config"
	"github.com/IBM/yarnbeat/helper"
	"github.com/IBM/yarnbeat/yarn"
	"net/http"
	"sync"
	"time"
)

type ClusterCollector struct {
	Config *config.Config
	Event  chan<- *beat.Event
	Logger *logp.Logger
	done   chan bool
	client http.Client
}

func (c *ClusterCollector) Start() {
	ticker := time.NewTicker(time.Duration(c.Config.Period) * time.Second)
	c.client = http.Client{}
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

func (c *ClusterCollector) Stop(wg sync.WaitGroup) {
	wg.Add(1)
	c.done<-true
	wg.Done()
}

func (c *ClusterCollector) poll() {
	c.Logger.Debug("Polling YARN API for applications")
	metrics, err := yarn.GetClusterMetrics(c.Config.ResourceManagers, c.client)
	if err != nil {
		c.Logger.Error("Failed to retrieve YARN cluster metrics: ", err)
		return
	}

	event := beat.Event{Timestamp: time.Now(), Fields: common.MapStr{}}
	event.Fields.Put("type", "cluster")
	err = helper.SetEventFields(&event, *metrics)
	if err != nil {
		c.Logger.Error("Failed to create event for cluster metrics: ", err)
		return
	}

	c.Logger.Debug("Reporting cluster metrics")
	c.Event <- &event
}
