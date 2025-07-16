package database

import (
	"myredis/myredis"
	"myredis/protocol"
	"strings"
)

var cmdTable = make(map[string]*command)

type command struct {
	name     string
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc

	arity int
	flags int
	extra *commandExtra
}

/*
signs: 	   描述命令的特性

firstKey:  命令中第一个键参数的位置

lastKey:   命令中最后一个键参数的位置

keyStep:   键参数之间的步长

for example:

	MSET key1 val1 key2 val2
*/
type commandExtra struct {
	signs    []string
	firstKey int
	lastKey  int
	keyStep  int
}

const flagWrite = 0

const (
	flagReadOnly = 1 << iota
	flagSpecial  // command invoked in Exec
)

func registerCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int, flags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:     name,
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
		flags:    flags,
	}
	cmdTable[name] = cmd
	return cmd
}

// 注册特殊命令，例如 publish, select, keys, flushAll
func registerSpecialCommand(name string, arity int, flags int) *command {
	name = strings.ToLower(name)
	flags |= flagSpecial
	cmd := &command{
		name:  name,
		arity: arity,
		flags: flags,
	}
	cmdTable[name] = cmd
	return cmd
}

func isReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd := cmdTable[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&flagReadOnly > 0
}

func (cmd *command) toDescReply() myredis.Reply {
	args := make([]myredis.Reply, 0)
	args = append(args,
		protocol.MakeBulkReply([]byte(cmd.name)),
		protocol.MakeIntReply(int64(cmd.arity)))
	if cmd.extra != nil {
		signs := make([][]byte, len(cmd.extra.signs))
		for i, v := range cmd.extra.signs {
			signs[i] = []byte(v)
		}
		args = append(args,
			protocol.MakeMultiBulkReply(signs),
			protocol.MakeIntReply(int64(cmd.extra.firstKey)),
			protocol.MakeIntReply(int64(cmd.extra.lastKey)),
			protocol.MakeIntReply(int64(cmd.extra.keyStep)),
		)
	}
	return protocol.MakeMultiRawReply(args)
}

func (cmd *command) attachCommandExtra(signs []string, firstKey int, lastKey int, keyStep int) {
	cmd.extra = &commandExtra{
		signs:    signs,
		firstKey: firstKey,
		lastKey:  lastKey,
		keyStep:  keyStep,
	}
}
