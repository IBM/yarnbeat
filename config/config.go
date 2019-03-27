// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

type Config struct {
	Period            int                      `config:"period"`
	ResourceManagers  []string                 `config:"resource_manager_urls"`
	EnableMapReduce   bool                     `config:"enable_mr"`
	HistoryServer     string                   `config:"history_server_url"`
	MapReduceCounters []MapReduceCounterConfig `config:"mr_counters"`
}

type MapReduceCounterConfig struct {
	GroupName string            `config:"group_name"`
	Counters  map[string]string `config:"counters"`
}

var DefaultConfig = Config{
	Period:            60,
	ResourceManagers:  []string{"localhost:8088"},
	EnableMapReduce:   false,
	HistoryServer:     "localhost:9888",
	MapReduceCounters: []MapReduceCounterConfig{},
}
