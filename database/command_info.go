package database

const (
	// 写操作命令，会修改数据
	redisFlagWrite = "write"
	// 只读命令，不会修改数据
	redisFlagReadonly = "readonly"
	// 命令结果具有随机性
	redisFlagRandom = "random"

	// 内存不足时拒绝执行该命令
	redisFlagDenyOOM = "denyoom"
	// 快速执行的命令
	redisFlagFast = "fast"
	// 脚本执行环境中需要确定性排序的命令
	redisFlagSortForScript = "sortforscript"
)
