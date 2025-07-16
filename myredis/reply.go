package myredis

type Reply interface {
	ToBytes() []byte
}
