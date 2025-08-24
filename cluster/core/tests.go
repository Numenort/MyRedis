package core

import (
	dbimpl "myredis/database"
	"strconv"
)

func MakeTestCluster(ids []string) map[string]*Cluster {
	nodes := make(map[string]*Cluster)
	connections := NewInMemConnectionFactory()
	connections.nodes = nodes
	for _, id := range ids {
		db := dbimpl.NewStandaloneServer()
		cluster := &Cluster{
			db:              db,
			config:          &Config{},
			connections:     connections,
			slotsManager:    newSlotsManager(),
			rebalanceManger: newRebalanceManager(),
			transactions:    newTransactionManager(),
			id_:             id,
		}
		cluster.pickNodeImpl = func(slotID uint32) string {
			index := int(slotID) % len(ids)
			return ids[index]
		}
		cluster.getSlotImpl = func(key string) uint32 {
			i, err := strconv.Atoi(key)
			if err == nil && i < SlotCount {
				return uint32(i)
			}
			return defaultGetSlotImpl(cluster, key)
		}
		cluster.injectDeleteCallback()
		cluster.injectInsertCallback()
		nodes[id] = cluster
	}
	return nodes
}
