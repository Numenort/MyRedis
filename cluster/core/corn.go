package core

import (
	"myredis/cluster/raft"
	"sync/atomic"
	"time"
)

// 集群节点的后台周期性任务协程
func (cluster *Cluster) clusterCron() {
	if cluster.config.noCron {
		return
	}

	ticker := time.NewTicker(time.Second)
	var running int32 // 原子标志位： 0=空闲，1=正在执行任务

	for {
		select {
		case <-ticker.C:
			// Leader 执行的操作
			if cluster.raftNode.State() == raft.Leader {
				if atomic.CompareAndSwapInt32(&running, 0, 1) {
					go func() {
						// 检查是否需要触发故障转移
						cluster.doFailoverCheck()
						// 检查是否需要进行 slot 再平衡
						cluster.doRebalance()
						// 任务结束，释放运行锁
						atomic.StoreInt32(&running, 0)
					}()
				}
			} else {
				// Follower 或 Candidate 节点：发送心跳响应
				cluster.sendHearbeat()
			}
		case <-cluster.closeChan:
			ticker.Stop()
			return
		}
	}

}
