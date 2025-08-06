package raft

import (
	"errors"
	"sort"
)

// 给指定节点添加一批 slot
//
//	参数：
//	nodeID: 节点 ID
//	slots:  要添加的 slot ID 列表
func (fsm *FSM) addSlots(nodeID string, slots []uint32) {
	// 搜索对应槽位
	for _, slotID := range slots {
		index := sort.Search(len(fsm.Node2Slot[nodeID]), func(i int) bool {
			return fsm.Node2Slot[nodeID][i] >= slotID
		})
		// 如果没有找到对应的槽位，插入该槽位
		if !(index < len(fsm.Node2Slot[nodeID]) && fsm.Node2Slot[nodeID][index] == slotID) {
			fsm.Node2Slot[nodeID] = append(
				fsm.Node2Slot[nodeID][:index],
				append(
					[]uint32{slotID}, fsm.Node2Slot[nodeID][index:]...,
				)...,
			)
		}
		// 更新该槽位对应的节点
		fsm.Slot2Node[slotID] = nodeID
	}
}

// 从指定节点移除一批 slot
//
//	参数：
//	nodeID: 节点 ID
//	slots:  要移除的 slot ID 列表
func (fsm *FSM) removeSlots(nodeID string, slots []uint32) {
	for _, slotID := range slots {
		index := sort.Search(len(fsm.Node2Slot[nodeID]), func(i int) bool {
			return fsm.Node2Slot[nodeID][i] >= slotID
		})
		// 从 node 的槽位中移除对应的槽位
		for index < len(fsm.Node2Slot[nodeID]) && fsm.Node2Slot[nodeID][index] == slotID {
			fsm.Node2Slot[nodeID] = append(fsm.Node2Slot[nodeID][:index], fsm.Node2Slot[nodeID][index+1:]...)
		}
		// 移除该槽位对应的节点
		if fsm.Slot2Node[slotID] == nodeID {
			delete(fsm.Slot2Node, slotID)
		}
	}
}

// addNode 将一个新节点加入集群，并设置其主从关系
//
//	参数：
//	id:       新节点 ID
//	masterId: 主节点 ID；如果是主节点，则传 ""
func (fsm *FSM) addNode(id, masterID string) error {
	// 未设置主节点，自己是主节点
	if masterID == "" {
		fsm.MasterSlaves[id] = &MasterSlave{
			MasterID: id,
		}
	} else {
		// 寻找主节点
		master := fsm.MasterSlaves[masterID]
		if master == nil {
			return errors.New("master not found")
		}
		// 记录当前节点是否为主节点的从节点
		exists := false
		for _, slave := range master.Slaves {
			if slave == id {
				exists = true
				break
			}
		}
		// 加入主节点的从节点列表
		if !exists {
			master.Slaves = append(master.Slaves, id)
		}
		fsm.SlaveMasters[id] = masterID
	}
	return nil
}

// failover 将 oldMaster 的主从关系转移到 newMaster
//
//	参数：
//	oldMasterId: 原主节点 ID
//	newMasterId: 新主节点 ID（通常是原主的一个从节点）
func (fsm *FSM) failover(OldMasterID, NewMasterID string) {
	// 获取旧的主节点的从节点
	oldSlaves := fsm.MasterSlaves[OldMasterID].Slaves
	newSlaves := make([]string, 0, len(oldSlaves))

	for _, slave := range oldSlaves {
		// 防止自身是自身的主节点
		if slave != NewMasterID {
			fsm.SlaveMasters[slave] = NewMasterID
			newSlaves = append(newSlaves, slave)
		}
	}
	// 修改旧的主节点的相关关系
	delete(fsm.MasterSlaves, OldMasterID)
	fsm.SlaveMasters[OldMasterID] = NewMasterID
	newSlaves = append(newSlaves, OldMasterID)
	// 修改新的主节点的关系
	delete(fsm.SlaveMasters, NewMasterID)
	fsm.MasterSlaves[NewMasterID] = &MasterSlave{
		MasterID: NewMasterID,
		Slaves:   newSlaves,
	}
}
