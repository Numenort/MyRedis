package server

import (
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/myredis/parser"
	"myredis/protocol/assert"
	"myredis/pubsub"
	"testing"
)

func TestPublish(t *testing.T) {
	hub := pubsub.MakeHub()
	channel := utils.RandString(5)
	msg := utils.RandString(5)
	conn := connection.NewSimpleConn()
	pubsub.Subscribe(hub, conn, utils.ToCmdLine(channel))
	conn.Clean()
	pubsub.Publish(hub, utils.ToCmdLine(channel, msg))
	data := conn.Bytes()
	ret, err := parser.ParseOne(data)
	if err != nil {
		t.Error(err)
		return
	}
	assert.AssertMultiBulkReply(t, ret, []string{
		"message",
		channel,
		msg,
	})

	// unsubscribe
	pubsub.UnSubscribe(hub, conn, utils.ToCmdLine(channel))
	conn.Clean()
	pubsub.Publish(hub, utils.ToCmdLine(channel, msg))
	data = conn.Bytes()
	print(string(data))
	if len(data) > 0 {
		t.Error("expect no msg")
	}

	// unsubscribe all
	pubsub.Subscribe(hub, conn, utils.ToCmdLine(channel))
	pubsub.UnSubscribe(hub, conn, utils.ToCmdLine())
	conn.Clean()
	pubsub.Publish(hub, utils.ToCmdLine(channel, msg))
	data = conn.Bytes()
	if len(data) > 0 {
		t.Error("expect no msg")
	}

}
