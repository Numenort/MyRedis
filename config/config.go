package config

type ServerProperties struct {
	Dir               string `cfg:"dir"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	Databases         int    `cfg:"databases"`
}

var Properties ServerProperties

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
