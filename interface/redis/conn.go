package redis

type Connection interface {
	Write([]byte) (int, error)
	Close() error
	RemoteAddr() string

	InMultiState() bool
	SetMultiState(bool)

	GetTxErrors() []error

	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()

	GetWatching() map[string]uint32
	AddTxError(err error)
}
