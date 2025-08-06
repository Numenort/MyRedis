package config

type ServerProperties struct {
	Dir               string `cfg:"dir"`
	Databases         int    `cfg:"databases"`
	AppendOnly        bool   `cfg:"appendonly"`
	AppendFilename    string `cfg:"appendfilename"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	AppendFsync       string `cfg:"appendfsync"`
	RequirePass       string `cfg:"requirepass"`
	RDBFilename       string `cfg:"rdbfilename"`
}

var Properties *ServerProperties

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
