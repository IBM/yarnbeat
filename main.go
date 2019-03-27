package main

import (
	"os"

	"github.com/IBM/yarnbeat/cmd"

	_ "github.com/IBM/yarnbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
