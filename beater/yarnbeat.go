package beater

import (
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/IBM/yarnbeat/collector"
	"net/url"
	"sync"

	"github.com/IBM/yarnbeat/config"
)

var Name = "yarnbeat"
var Version = "1.1.0"

type Yarnbeat struct {
	config           config.Config
	client           beat.Client
	log              *logp.Logger
	appCollector     *collector.AppCollector
	clusterCollector *collector.ClusterCollector
	waitGroup        *sync.WaitGroup
	event            chan *beat.Event
}

func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	b.Info.Beat = Name
	b.Info.Version = Version

	bt := &Yarnbeat{
		config:    c,
		log:       logp.NewLogger(Name),
		waitGroup: &sync.WaitGroup{},
	}
	return bt, nil
}

func (bt *Yarnbeat) Run(b *beat.Beat) error {
	bt.log.Infof("%s-%s is running! Hit CTRL-C to stop it.", Name, Version)

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}
	defer bt.client.Close()

	urls := make([]url.URL, len(bt.config.ResourceManagers))
	for i := range bt.config.ResourceManagers {
		u, err := url.Parse(bt.config.ResourceManagers[i])
		if err != nil {
			return err
		}
		urls[i] = *u
	}

	bt.event = make(chan *beat.Event)
	bt.appCollector = &collector.AppCollector{
		Config: &bt.config,
		Event:  bt.event,
		Logger: bt.log,
	}

	bt.clusterCollector = &collector.ClusterCollector{
		Config: &bt.config,
		Event:  bt.event,
		Logger: bt.log,
	}

	go bt.appCollector.Start()
	go bt.clusterCollector.Start()

	for {
		e, open := <-bt.event
		if e != nil {
			bt.client.Publish(*e)
		}
		if !open {
			break
		}
	}
	return nil
}

func (bt *Yarnbeat) Stop() {
	wg := sync.WaitGroup{}
	bt.appCollector.Stop(wg)
	bt.clusterCollector.Stop(wg)
	wg.Wait()
	close(bt.event)
}
