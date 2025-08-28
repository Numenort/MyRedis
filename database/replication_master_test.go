package database

import "sync/atomic"

func mockServer() *Server {
	server := &Server{}
	server.dbSet = make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	server.slaveStatus = initReplSlaveStatus()
	server.initMasterStatus()
	return server
}
