package main

import (
	"fmt"
	"myredis/config"
	"myredis/lib/logger"
	"myredis/lib/utils"
	MydisServer "myredis/myredis/server"
	"myredis/tcp"
	"os"
)

var banner = `
		    _______________________________________________________
                   |                                                       |
            /|     |███╗   ███╗██╗   ██╗██████╗ ███████╗██████╗ ██╗███████╗|
            ||     |████╗ ████║╚██╗ ██╔╝██╔══██╗██╔════╝██╔══██╗██║██╔════╝|
       .----|-----,|██╔████╔██║ ╚████╔╝ ██████╔╝█████╗  ██║  ██║██║███████╗|
       ||  ||   ==||██║╚██╔╝██║  ╚██╔╝  ██╔══██╗██╔══╝  ██║  ██║██║╚════██║|
  .-----'--'|   ==||██║ ╚═╝ ██║   ██║   ██║  ██║███████╗██████╔╝██║███████║|
  |)-      ~|     ||╚═╝     ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝╚══════╝|
  | ___     |     |____...==..._  >\_______________________________________|
 [_/.-.\"--"-------- //.-.  .-.\\/   |/   \\ .-.  .-. //    \\ .-.  .-. //     
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func main() {
	print(banner)
	// 配置日志模块
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "mydis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	configFileName := os.Getenv("CONFIG")
	if configFileName == "" {
		if fileExists("mydis.conf") {
			config.SetupConfig("mydis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFileName)
	}
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, MydisServer.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
