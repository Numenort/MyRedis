package config

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	src := "bind 0.0.0.0\n" +
		"port 6399\n" +
		"databases 16\n" +
		"maxclients 1000\n" +
		"appendonly yes\n" +
		"appendfilename appendonly.aof\n" +
		"appendfsync everysec\n" +
		"dbfilename node1.rdb\n" +
		"dir ./data/node1\n" +
		"cluster-enable yes\n" +
		"cluster-as-seed yes\n" +
		"raft-listen-address 0.0.0.0:16666\n" +
		"raft-advertise-address 127.0.0.1:16666\n"

	p := parse(strings.NewReader(src))
	if p == nil {
		t.Error("cannot get result")
		return
	}
	if p.Bind != "0.0.0.0" {
		t.Error("string parse failed")
	}
	if p.Port != 6399 {
		t.Error("int parse failed")
	}
	if p.Databases != 16 {
		t.Error("int parse failed")
	}
	if !p.AppendOnly {
		t.Error("bool parse failed")
	}
}
