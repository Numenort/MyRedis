package myredis

type Connection interface {
	Write([]byte) (int, error)
	Close() error
	RemoteAddr() string

	SetPassword(string)
	GetPassword() string

	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string

	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32

	GetTxErrors() []error
	AddTxError(err error)

	GetDBIndex() int
	SelectDB(dbNum int)

	SetSlave()
	IsSlave() bool

	SetMaster()
	IsMaster() bool

	Name() string
}
