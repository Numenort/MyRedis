package pubsub

import (
	"myredis/datastruct/list"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
	"strconv"
)

var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

// 返回 subscribe/unsubscribe 的确认消息
//
// 格式：*3\r\n$len(t)\r\n<t>\r\n$len(channel)\r\n<channel>\r\n<count>\r\n
//
// 参数：
//   - t: 消息类型，如 "subscribe"
//   - channel: 频道名称
//   - code: 当前客户端订阅的总频道数
//
// 返回值：
//   - []byte: 序列化的 RESP 协议字节流
func makeMessge(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + protocol.CRLF + t + protocol.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
		":" + strconv.FormatInt(code, 10) + protocol.CRLF)
}

/* 将客户端加入指定频道的订阅者列表，若频道不存在，则创建新的订阅者链表
* 参数：
 *   - hub: Pub/Sub 中心
 *   - channel: 要订阅的频道名
 *   - client: 客户端连接对象
 *
 * 返回值：
 *   - bool: 是否为“新订阅”（true 表示之前未订阅）
*/
func subscribe0(hub *Hub, channel string, client myredis.Connection) bool {
	// 客户端订阅该频道
	client.Subscribe(channel)
	// 获取该频道对应的订阅者列表
	raw, ok := hub.subs.Get(channel)
	var subscribes *list.LinkedList
	if ok {
		subscribes, _ = raw.(*list.LinkedList)
	} else {
		// 新建订阅者列表
		subscribes = list.Make()
		hub.subs.Put(channel, subscribes)
	}
	// 检查客户端是否已经存在
	if subscribes.Contains(func(a interface{}) bool { return a == client }) {
		return false
	}
	subscribes.Add(client)
	return true
}

/*	从指定频道的订阅者列表中移除客户端
* 参数：
 *   - hub: Pub/Sub 中心
 *   - channel: 要取消订阅的频道
 *   - client: 客户端连接
 *
 * 返回值：
 *   - bool: 是否真正执行了解除订阅
*/
func unsubscribe0(hub *Hub, channel string, client myredis.Connection) bool {
	// 客户端取消订阅该频道
	client.UnSubscribe(channel)
	raw, ok := hub.subs.Get(channel)
	if !ok {
		return false
	}
	subscribes, _ := raw.(*list.LinkedList)
	removed := subscribes.RemoveAllByVal(func(a interface{}) bool { return utils.Equals(a, client) })
	if subscribes.Len() == 0 {
		hub.subs.Remove(channel)
	}
	return removed > 0
}

/*	让客户端订阅一个或多个频道
* 参数：
 *   - hub: Pub/Sub 中心
 *   - c: 客户端连接
 * 	 - args: 频道名称列表
*/
func Subscribe(hub *Hub, c myredis.Connection, args [][]byte) myredis.Reply {
	// 获取频道列表
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	// 对每个频道加锁
	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		if subscribe0(hub, channel, c) {
			_, _ = c.Write(makeMessge(_subscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

/* 让客户端退订其所有正在监听的频道
* 参数：
 *   - hub: Pub/Sub 中心
 *   - c: 客户端连接
*/
func UnsubscribeAll(hub *Hub, c myredis.Connection) {
	channels := c.GetChannels()

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		unsubscribe0(hub, channel, c)
	}
}

/* 让客户端取消订阅一个或多个指定频道，如果未指定频道，则退订其所有频道
* 参数：
 *   - hub: Pub/Sub 中心
 *   - c: 客户端连接
 * 	 - args: 频道名称列表
*/
func Unsubscribe(hub *Hub, c myredis.Connection, args [][]byte) myredis.Reply {
	var channels []string

	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, arg := range channels {
			channels[i] = string(arg)
		}
	} else {
		channels = c.GetChannels()
	}

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)
	// 没有频道可退订
	if len(channels) == 0 {
		_, _ = c.Write(unSubscribeNothing)
		return &protocol.NoReply{}
	}
	// 逐个退订
	for _, channel := range channels {
		if unsubscribe0(hub, channel, c) {
			// 发送退订确认
			_, _ = c.Write(makeMessge(_unsubscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

/* 向指定频道发布一条消息，所有订阅者都会收到
* 参数：
 *   - hub: Pub/Sub 中心
 * 	 - args: 频道名称列表
* 返回值：
 *   - myredis.Reply: 返回接收到消息的订阅者数量
*/
func Publish(hub *Hub, args [][]byte) myredis.Reply {
	if len(args) != 2 {
		return &protocol.ArgNumErrReply{Cmd: "publish"}
	}

	channel := string(args[0])
	message := args[1]

	hub.subsLocker.Lock(channel)
	defer hub.subsLocker.UnLock(channel)

	raw, ok := hub.subs.Get(channel)
	if !ok {
		// 没有订阅者
		return protocol.MakeIntReply(0)
	}
	subscribes, _ := raw.(*list.LinkedList)
	// 构造消息：*3\r\n$message\r\n$channel\r\n$message\r\n
	replyArgs := make([][]byte, 3)
	replyArgs[0] = messageBytes
	replyArgs[1] = []byte(channel)
	replyArgs[2] = message
	// 给每个订阅者（client）发送消息
	subscribes.ForEach(func(i int, v interface{}) bool {
		client, _ := v.(myredis.Connection)
		_, _ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})

	return protocol.MakeIntReply(int64(subscribes.Len()))
}
