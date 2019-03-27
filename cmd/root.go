package cmd

import (
	"github.com/elastic/beats/libbeat/cmd/instance"
	"github.com/IBM/yarnbeat/beater"

	"github.com/elastic/beats/libbeat/cmd"
)

var Settings = instance.Settings{
	Name:        beater.Name,
	IndexPrefix: beater.Name,
	Version:     beater.Version,
}

// RootCmd to handle beats cli
var RootCmd = cmd.GenRootCmdWithSettings(beater.New, Settings)
