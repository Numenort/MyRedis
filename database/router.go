package database

import (
	"myredis/interface/myredis"
	"myredis/protocol"
	"strings"
)

// 存储所有注册的 Redis 命令，键为命令名（小写）
var cmdTable = make(map[string]*command)

type command struct {
	name     string   // 命令名称
	executor ExecFunc // 执行函数：实际处理命令逻辑
	prepare  PreFunc  // 准备函数：在执行前获取涉及的 key（用于锁、AOF 重写等）
	undo     UndoFunc // 撤销函数：用于事务回滚时生成反向操作

	arity int // 参数个数要求：
	//   > 0: 必须恰好有 N 个参数
	//   < 0: 至少有 |N| 个参数
	//   = 0: 任意数量参数
	flags int           // 命令标志位，如只读、写操作等
	extra *commandExtra // 额外元数据：键位置、特性标志等
}

/*
commandExtra 包含命令的高级元信息，主要用于集群、AOF、事务等场景。

字段说明：
  - signs:      命令特性标签，如 "write", "readonly", "movablekeys" 等
  - firstKey:   第一个 key 参数在参数列表中的索引（从 1 开始计数）
  - lastKey:    最后一个 key 参数的索引
  - keyStep:    key 参数之间的步长

示例：MSET key1 val1 key2 val2

	参数顺序：[key1, val1, key2, val2]
	firstKey = 1, lastKey = -1（表示最后一个 key 是倒数第一个），keyStep = 2
	表示每隔 2 个参数有一个 key。
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

// registerCommand 注册一个通用 Redis 命令到全局命令表中。
//
// 参数：
//   - name: 命令名称（大小写不敏感，内部转为小写）
//   - executor: 命令执行函数，处理客户端请求
//   - prepare: 准备函数，用于提前提取命令中涉及的 key（用于加锁、AOF 重写等）
//   - rollback: 撤销函数，用于事务中生成回滚操作（如删除刚添加的 key）
//   - arity: 参数个数限制（正数=精确，负数=最小个数）
//   - flags: 命令标志（如 flagReadOnly）
//
// 返回值：
//
//	返回 *command 对象，可用于链式调用（如 attachCommandExtra）
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

// 判断指定命令是否为只读命令。
func isReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd := cmdTable[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&flagReadOnly > 0
}

// toDescReply 将 command 对象转换为 Redis 的 COMMAND 命令返回格式。
// 用于实现 `COMMAND`, `COMMAND INFO` 等功能。
//
// 返回值格式（数组）：
//  1. 命令名（Bulk String）
//  2. 参数个数（Integer）
//  3. 特性标签（Array of Bulk String）——仅当有 extra 时存在
//  4. 第一个 key 的索引（Integer）
//  5. 最后一个 key 的索引（Integer）
//  6. key 步长（Integer）
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
